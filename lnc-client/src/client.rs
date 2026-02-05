use crate::error::{ClientError, Result};
use crate::tls::TlsClientConfig;
use bytes::Bytes;
use lnc_network::{encode_frame, parse_frame, ControlCommand, Frame, FrameType, TlsConnector, LWP_HEADER_SIZE};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tracing::{debug, trace, warn};

/// Wrapper enum for TCP and TLS streams to avoid dynamic dispatch
#[allow(clippy::large_enum_variant)]
pub enum ClientStream {
    /// Plain TCP connection
    Tcp(TcpStream),
    /// TLS-encrypted connection
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for ClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            ClientStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            ClientStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            ClientStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            ClientStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}


/// Helper to extract error message from a frame payload
fn extract_error_message(frame: &Frame) -> String {
    frame
        .payload
        .as_ref()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .unwrap_or_else(|| "Unknown error".to_string())
}

/// Helper to validate frame type and extract error if present
#[allow(dead_code)] // Reserved for future protocol extensions
fn expect_frame_type(
    frame: Frame,
    expected: ControlCommand,
    expected_name: &str,
) -> Result<Frame> {
    match frame.frame_type {
        FrameType::Control(cmd) if cmd == expected => Ok(frame),
        FrameType::Control(ControlCommand::ErrorResponse) => {
            Err(ClientError::ServerError(extract_error_message(&frame)))
        }
        other => Err(ClientError::InvalidResponse(format!(
            "Expected {}, got {:?}",
            expected_name, other
        ))),
    }
}

/// Helper to validate frame is a success response (TopicResponse or similar)
fn expect_success_response(frame: Frame) -> Result<()> {
    match frame.frame_type {
        FrameType::Control(ControlCommand::TopicResponse) => Ok(()),
        FrameType::Control(ControlCommand::ErrorResponse) => {
            Err(ClientError::ServerError(extract_error_message(&frame)))
        }
        other => Err(ClientError::InvalidResponse(format!(
            "Expected TopicResponse, got {:?}",
            other
        ))),
    }
}


#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub mtls_enabled: bool,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

/// Retention configuration for a topic
#[derive(Debug, Clone, Default)]
pub struct RetentionInfo {
    /// Maximum age of messages in seconds (0 = no limit)
    pub max_age_secs: u64,
    /// Maximum size of topic data in bytes (0 = no limit)
    pub max_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    /// Retention policy configuration (None = no retention policy set)
    pub retention: Option<RetentionInfo>,
}

/// Result of a fetch operation
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub data: Bytes,
    pub next_offset: u64,
    pub bytes_returned: u32,
    pub record_count: u32,
}

/// Result of a subscribe operation
#[derive(Debug, Clone)]
pub struct SubscribeResult {
    pub consumer_id: u64,
    pub start_offset: u64,
}

/// Result of a commit offset operation
#[derive(Debug, Clone)]
pub struct CommitResult {
    pub consumer_id: u64,
    pub committed_offset: u64,
}

/// Cluster status information
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub node_id: u16,
    pub is_leader: bool,
    pub leader_id: Option<u16>,
    pub current_term: u64,
    pub node_count: usize,
    pub healthy_nodes: usize,
    pub quorum_available: bool,
    pub peer_states: std::collections::HashMap<u16, String>,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub addr: SocketAddr,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub keepalive_interval: Duration,
    /// Optional TLS configuration for encrypted connections
    pub tls: Option<TlsClientConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:1992"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 1992))),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(10),
            tls: None,
        }
    }
}

impl ClientConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            ..Default::default()
        }
    }

    /// Enable TLS with the provided configuration
    pub fn with_tls(mut self, tls_config: TlsClientConfig) -> Self {
        self.tls = Some(tls_config);
        self
    }

    /// Check if TLS is enabled
    pub fn is_tls_enabled(&self) -> bool {
        self.tls.is_some()
    }
}

pub struct LanceClient {
    stream: ClientStream,
    config: ClientConfig,
    batch_id: AtomicU64,
    read_buffer: Vec<u8>,
    read_offset: usize,
}

impl LanceClient {
    /// Connect to LANCE server, automatically using TLS if configured
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        // If TLS is configured in ClientConfig, use TLS connection
        if let Some(ref tls_config) = config.tls {
            return Self::connect_tls(config.clone(), tls_config.clone()).await;
        }

        debug!(addr = %config.addr, "Connecting to LANCE server");

        let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(config.addr))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(ClientError::ConnectionFailed)?;

        stream.set_nodelay(true)?;

        debug!(addr = %config.addr, "Connected to LANCE server");

        Ok(Self {
            stream: ClientStream::Tcp(stream),
            config,
            batch_id: AtomicU64::new(0),
            read_buffer: vec![0u8; 64 * 1024],
            read_offset: 0,
        })
    }

    /// Connect to LANCE server with TLS encryption
    ///
    /// # Arguments
    /// * `config` - Client configuration with server address
    /// * `tls_config` - TLS configuration including certificates
    ///
    /// # Example
    /// ```rust,ignore
    /// use lnc_client::{ClientConfig, TlsClientConfig, LanceClient};
    ///
    /// let config = ClientConfig::new("127.0.0.1:1992".parse().unwrap());
    /// let tls = TlsClientConfig::new()
    ///     .with_ca_cert("/path/to/ca.pem");
    ///
    /// let client = LanceClient::connect_tls(config, tls).await?;
    /// ```
    pub async fn connect_tls(config: ClientConfig, tls_config: TlsClientConfig) -> Result<Self> {
        debug!(addr = %config.addr, "Connecting to LANCE server with TLS");

        // First establish TCP connection
        let tcp_stream = tokio::time::timeout(
            config.connect_timeout,
            TcpStream::connect(config.addr),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(ClientError::ConnectionFailed)?;

        tcp_stream.set_nodelay(true)?;

        // Create TLS connector
        let network_config = tls_config.to_network_config();
        let connector = TlsConnector::new(network_config)
            .map_err(|e| ClientError::TlsError(e.to_string()))?;

        // Determine server name for SNI
        let server_name = tls_config
            .server_name
            .unwrap_or_else(|| config.addr.ip().to_string());

        // Perform TLS handshake
        let tls_stream = connector
            .connect(&server_name, tcp_stream)
            .await
            .map_err(|e| ClientError::TlsError(e.to_string()))?;

        debug!(addr = %config.addr, "TLS connection established");

        Ok(Self {
            stream: ClientStream::Tls(tls_stream),
            config,
            batch_id: AtomicU64::new(0),
            read_buffer: vec![0u8; 64 * 1024],
            read_offset: 0,
        })
    }

    pub async fn connect_to(addr: &str) -> Result<Self> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| ClientError::ProtocolError(format!("Invalid address: {}", e)))?;
        Self::connect(ClientConfig::new(socket_addr)).await
    }

    /// Connect to LANCE server with TLS using address string
    pub async fn connect_tls_to(addr: &str, tls_config: TlsClientConfig) -> Result<Self> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| ClientError::ProtocolError(format!("Invalid address: {}", e)))?;
        Self::connect_tls(ClientConfig::new(socket_addr), tls_config).await
    }

    fn next_batch_id(&self) -> u64 {
        self.batch_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub async fn send_ingest(&mut self, payload: Bytes, record_count: u32) -> Result<u64> {
        self.send_ingest_to_topic(0, payload, record_count, None)
            .await
    }

    pub async fn send_ingest_to_topic(
        &mut self,
        topic_id: u32,
        payload: Bytes,
        record_count: u32,
        _auth_config: Option<&AuthConfig>,
    ) -> Result<u64> {
        let batch_id = self.next_batch_id();
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let frame =
            Frame::new_ingest_with_topic(batch_id, timestamp_ns, record_count, payload, topic_id);
        let frame_bytes = encode_frame(&frame);

        trace!(
            batch_id,
            topic_id,
            payload_len = frame.payload_length(),
            "Sending ingest frame"
        );

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        Ok(batch_id)
    }

    pub async fn send_ingest_sync(&mut self, payload: Bytes, record_count: u32) -> Result<u64> {
        self.send_ingest_to_topic_sync(0, payload, record_count, None)
            .await
    }

    pub async fn send_ingest_to_topic_sync(
        &mut self,
        topic_id: u32,
        payload: Bytes,
        record_count: u32,
        auth_config: Option<&AuthConfig>,
    ) -> Result<u64> {
        let batch_id = self
            .send_ingest_to_topic(topic_id, payload, record_count, auth_config)
            .await?;
        self.wait_for_ack(batch_id).await
    }

    async fn wait_for_ack(&mut self, expected_batch_id: u64) -> Result<u64> {
        let frame = self.recv_frame().await?;

        match frame.frame_type {
            FrameType::Ack => {
                let acked_id = frame.batch_id();
                if acked_id != expected_batch_id {
                    return Err(ClientError::InvalidResponse(format!(
                        "Ack batch_id mismatch: sent {}, received {}",
                        expected_batch_id, acked_id
                    )));
                }
                trace!(batch_id = acked_id, "Received ack");
                Ok(acked_id)
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            FrameType::Backpressure => {
                warn!("Server signaled backpressure");
                Err(ClientError::ServerBackpressure)
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected Ack, got {:?}",
                other
            ))),
        }
    }

    pub async fn recv_ack(&mut self) -> Result<u64> {
        let frame = self.recv_frame().await?;

        match frame.frame_type {
            FrameType::Ack => {
                trace!(batch_id = frame.batch_id(), "Received ack");
                Ok(frame.batch_id())
            },
            FrameType::Backpressure => {
                warn!("Server signaled backpressure");
                Err(ClientError::ServerBackpressure)
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected Ack, got {:?}",
                other
            ))),
        }
    }

    pub async fn send_keepalive(&mut self) -> Result<()> {
        let frame = Frame::new_keepalive();
        let frame_bytes = encode_frame(&frame);

        trace!("Sending keepalive");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        Ok(())
    }

    pub async fn recv_keepalive(&mut self) -> Result<()> {
        let frame = self.recv_frame().await?;

        match frame.frame_type {
            FrameType::Keepalive => {
                trace!("Received keepalive response");
                Ok(())
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected Keepalive, got {:?}",
                other
            ))),
        }
    }

    pub async fn ping(&mut self) -> Result<Duration> {
        let start = std::time::Instant::now();
        self.send_keepalive().await?;
        self.recv_keepalive().await?;
        Ok(start.elapsed())
    }

    pub async fn create_topic(&mut self, name: &str) -> Result<TopicInfo> {
        let frame = Frame::new_create_topic(name);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_name = %name, "Creating topic");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_topic_response(response)
    }

    pub async fn list_topics(&mut self) -> Result<Vec<TopicInfo>> {
        let frame = Frame::new_list_topics();
        let frame_bytes = encode_frame(&frame);

        trace!("Listing topics");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_topic_list_response(response)
    }

    pub async fn get_topic(&mut self, topic_id: u32) -> Result<TopicInfo> {
        let frame = Frame::new_get_topic(topic_id);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, "Getting topic");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_topic_response(response)
    }

    pub async fn delete_topic(&mut self, topic_id: u32) -> Result<()> {
        let frame = Frame::new_delete_topic(topic_id);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, "Deleting topic");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_delete_response(response)
    }

    /// Set retention policy for an existing topic
    ///
    /// # Arguments
    /// * `topic_id` - Topic identifier
    /// * `max_age_secs` - Maximum age in seconds (0 = no limit)
    /// * `max_bytes` - Maximum size in bytes (0 = no limit)
    pub async fn set_retention(
        &mut self,
        topic_id: u32,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<()> {
        let frame = Frame::new_set_retention(topic_id, max_age_secs, max_bytes);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, max_age_secs, max_bytes, "Setting retention policy");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_retention_response(response)
    }

    /// Create a topic with retention policy in a single operation
    ///
    /// # Arguments
    /// * `name` - Topic name
    /// * `max_age_secs` - Maximum age in seconds (0 = no limit)
    /// * `max_bytes` - Maximum size in bytes (0 = no limit)
    pub async fn create_topic_with_retention(
        &mut self,
        name: &str,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<TopicInfo> {
        let frame = Frame::new_create_topic_with_retention(name, max_age_secs, max_bytes);
        let frame_bytes = encode_frame(&frame);

        trace!(name, max_age_secs, max_bytes, "Creating topic with retention");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_topic_response(response)
    }


    /// Get cluster status and health information
    pub async fn get_cluster_status(&mut self) -> Result<ClusterStatus> {
        let frame = Frame::new_get_cluster_status();
        let frame_bytes = encode_frame(&frame);

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_cluster_status_response(response)
    }

    fn parse_cluster_status_response(&self, frame: Frame) -> Result<ClusterStatus> {
        match frame.frame_type {
            FrameType::Control(ControlCommand::ClusterStatusResponse) => {
                let payload = frame.payload.ok_or_else(|| {
                    ClientError::InvalidResponse("Empty cluster status response".to_string())
                })?;
                let json: serde_json::Value = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::ProtocolError(format!("Invalid JSON: {}", e)))?;

                let peer_states: std::collections::HashMap<u16, String> = json["peer_states"]
                    .as_object()
                    .map(|obj| {
                        obj.iter()
                            .filter_map(|(k, v)| {
                                k.parse::<u16>().ok().map(|id| (id, v.as_str().unwrap_or("unknown").to_string()))
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(ClusterStatus {
                    node_id: json["node_id"].as_u64().unwrap_or(0) as u16,
                    is_leader: json["is_leader"].as_bool().unwrap_or(false),
                    leader_id: json["leader_id"].as_u64().map(|id| id as u16),
                    current_term: json["current_term"].as_u64().unwrap_or(0),
                    node_count: json["node_count"].as_u64().unwrap_or(1) as usize,
                    healthy_nodes: json["healthy_nodes"].as_u64().unwrap_or(1) as usize,
                    quorum_available: json["quorum_available"].as_bool().unwrap_or(true),
                    peer_states,
                })
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected ClusterStatusResponse, got {:?}",
                other
            ))),
        }
    }


    /// Fetch data from a topic starting at the given offset
    /// Returns (data, next_offset, record_count)
    pub async fn fetch(
        &mut self,
        topic_id: u32,
        start_offset: u64,
        max_bytes: u32,
    ) -> Result<FetchResult> {
        let frame = Frame::new_fetch(topic_id, start_offset, max_bytes);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, start_offset, max_bytes, "Fetching data");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_fetch_response(response)
    }

    /// Subscribe to a topic for streaming data
    /// Returns the consumer ID and starting offset
    pub async fn subscribe(
        &mut self,
        topic_id: u32,
        start_offset: u64,
        max_batch_bytes: u32,
        consumer_id: u64,
    ) -> Result<SubscribeResult> {
        let frame = Frame::new_subscribe(topic_id, start_offset, max_batch_bytes, consumer_id);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, start_offset, consumer_id, "Subscribing to topic");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_subscribe_response(response)
    }

    /// Unsubscribe from a topic
    pub async fn unsubscribe(&mut self, topic_id: u32, consumer_id: u64) -> Result<()> {
        let frame = Frame::new_unsubscribe(topic_id, consumer_id);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, consumer_id, "Unsubscribing from topic");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        // Wait for ack or error
        let response = self.recv_frame().await?;
        match response.frame_type {
            FrameType::Ack => Ok(()),
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = response
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected Ack, got {:?}",
                other
            ))),
        }
    }

    /// Commit consumer offset for checkpointing
    pub async fn commit_offset(
        &mut self,
        topic_id: u32,
        consumer_id: u64,
        offset: u64,
    ) -> Result<CommitResult> {
        let frame = Frame::new_commit_offset(topic_id, consumer_id, offset);
        let frame_bytes = encode_frame(&frame);

        trace!(topic_id, consumer_id, offset, "Committing offset");

        tokio::time::timeout(
            self.config.write_timeout,
            self.stream.write_all(&frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;

        let response = self.recv_frame().await?;
        self.parse_commit_response(response)
    }

    fn parse_subscribe_response(&self, frame: Frame) -> Result<SubscribeResult> {
        match frame.frame_type {
            FrameType::Control(ControlCommand::SubscribeAck) => {
                let payload = frame.payload.ok_or_else(|| {
                    ClientError::InvalidResponse("Empty subscribe response".to_string())
                })?;

                if payload.len() < 16 {
                    return Err(ClientError::ProtocolError(
                        "Subscribe response too small".to_string(),
                    ));
                }

                let consumer_id = u64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ]);
                let start_offset = u64::from_le_bytes([
                    payload[8],
                    payload[9],
                    payload[10],
                    payload[11],
                    payload[12],
                    payload[13],
                    payload[14],
                    payload[15],
                ]);

                Ok(SubscribeResult {
                    consumer_id,
                    start_offset,
                })
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected SubscribeAck, got {:?}",
                other
            ))),
        }
    }

    fn parse_commit_response(&self, frame: Frame) -> Result<CommitResult> {
        match frame.frame_type {
            FrameType::Control(ControlCommand::CommitAck) => {
                let payload = frame.payload.ok_or_else(|| {
                    ClientError::InvalidResponse("Empty commit response".to_string())
                })?;

                if payload.len() < 16 {
                    return Err(ClientError::ProtocolError(
                        "Commit response too small".to_string(),
                    ));
                }

                let consumer_id = u64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ]);
                let committed_offset = u64::from_le_bytes([
                    payload[8],
                    payload[9],
                    payload[10],
                    payload[11],
                    payload[12],
                    payload[13],
                    payload[14],
                    payload[15],
                ]);

                Ok(CommitResult {
                    consumer_id,
                    committed_offset,
                })
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected CommitAck, got {:?}",
                other
            ))),
        }
    }

    fn parse_fetch_response(&self, frame: Frame) -> Result<FetchResult> {
        match frame.frame_type {
            FrameType::Control(ControlCommand::FetchResponse) => {
                let payload = frame.payload.ok_or_else(|| {
                    ClientError::InvalidResponse("Empty fetch response".to_string())
                })?;

                if payload.len() < 16 {
                    return Err(ClientError::ProtocolError(
                        "Fetch response too small".to_string(),
                    ));
                }

                let next_offset = u64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ]);
                let bytes_returned =
                    u32::from_le_bytes([payload[8], payload[9], payload[10], payload[11]]);
                let record_count =
                    u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);
                let data = payload.slice(16..);

                Ok(FetchResult {
                    data,
                    next_offset,
                    bytes_returned,
                    record_count,
                })
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected FetchResponse, got {:?}",
                other
            ))),
        }
    }

    fn parse_delete_response(&self, frame: Frame) -> Result<()> {
        expect_success_response(frame)
    }

    fn parse_retention_response(&self, frame: Frame) -> Result<()> {
        expect_success_response(frame)
    }

    fn parse_topic_response(&self, frame: Frame) -> Result<TopicInfo> {
        match frame.frame_type {
            FrameType::Control(ControlCommand::TopicResponse) => {
                let payload = frame.payload.ok_or_else(|| {
                    ClientError::InvalidResponse("Empty topic response".to_string())
                })?;
                let json: serde_json::Value = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::ProtocolError(format!("Invalid JSON: {}", e)))?;

                let retention = if json.get("retention").is_some() {
                    Some(RetentionInfo {
                        max_age_secs: json["retention"]["max_age_secs"].as_u64().unwrap_or(0),
                        max_bytes: json["retention"]["max_bytes"].as_u64().unwrap_or(0),
                    })
                } else {
                    None
                };

                Ok(TopicInfo {
                    id: json["id"].as_u64().unwrap_or(0) as u32,
                    name: json["name"].as_str().unwrap_or("").to_string(),
                    created_at: json["created_at"].as_u64().unwrap_or(0),
                    retention,
                })
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected TopicResponse, got {:?}",
                other
            ))),
        }
    }

    fn parse_topic_list_response(&self, frame: Frame) -> Result<Vec<TopicInfo>> {
        match frame.frame_type {
            FrameType::Control(ControlCommand::TopicResponse) => {
                let payload = frame.payload.ok_or_else(|| {
                    ClientError::InvalidResponse("Empty topic list response".to_string())
                })?;
                let json: serde_json::Value = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::ProtocolError(format!("Invalid JSON: {}", e)))?;

                let topics = json["topics"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .map(|t| {
                                let retention = if t.get("retention").is_some() {
                                    Some(RetentionInfo {
                                        max_age_secs: t["retention"]["max_age_secs"].as_u64().unwrap_or(0),
                                        max_bytes: t["retention"]["max_bytes"].as_u64().unwrap_or(0),
                                    })
                                } else {
                                    None
                                };
                                TopicInfo {
                                    id: t["id"].as_u64().unwrap_or(0) as u32,
                                    name: t["name"].as_str().unwrap_or("").to_string(),
                                    created_at: t["created_at"].as_u64().unwrap_or(0),
                                    retention,
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(topics)
            },
            FrameType::Control(ControlCommand::ErrorResponse) => {
                let error_msg = frame
                    .payload
                    .map(|p| String::from_utf8_lossy(&p).to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(ClientError::ServerError(error_msg))
            },
            other => Err(ClientError::InvalidResponse(format!(
                "Expected TopicResponse, got {:?}",
                other
            ))),
        }
    }

    async fn recv_frame(&mut self) -> Result<Frame> {
        loop {
            if self.read_offset >= LWP_HEADER_SIZE {
                if let Some((frame, consumed)) = parse_frame(&self.read_buffer[..self.read_offset])?
                {
                    self.read_buffer.copy_within(consumed..self.read_offset, 0);
                    self.read_offset -= consumed;
                    return Ok(frame);
                }
            }

            let n = tokio::time::timeout(
                self.config.read_timeout,
                self.stream.read(&mut self.read_buffer[self.read_offset..]),
            )
            .await
            .map_err(|_| ClientError::Timeout)??;

            if n == 0 {
                return Err(ClientError::ConnectionClosed);
            }

            self.read_offset += n;
        }
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub async fn close(mut self) -> Result<()> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

impl std::fmt::Debug for LanceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceClient")
            .field("addr", &self.config.addr)
            .field("batch_id", &self.batch_id.load(Ordering::SeqCst))
            .finish()
    }
}
