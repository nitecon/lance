use crate::error::{ClientError, Result};
use crate::tls::TlsClientConfig;
use bytes::Bytes;
use lnc_network::{
    ControlCommand, Frame, FrameType, LWP_HEADER_SIZE, TlsConnector, encode_frame, parse_frame,
};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::lookup_host;
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
    /// Delegates read readiness directly to the concrete transport so
    /// buffered frames stay on the Architecture §15 zero-copy path without
    /// layering additional indirection.
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
    /// Delegates write readiness to the underlying transport without
    /// introducing dynamic dispatch, which keeps buffered writes on the
    /// Section 14 thread-pinned path compliant with Architecture §15's
    /// loaner-buffer rules.
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

    /// Flushes bytes on whichever transport is active so that ingestion
    /// frames honor the write-buffering guarantees described in Architecture
    /// §22 without duplicating logic across TCP/TLS paths.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            ClientStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    /// Propagates orderly shutdown to the concrete stream implementation,
    /// enabling graceful disconnects per Architecture §14's pinning strategy
    /// whether the session is plain TCP or TLS.
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
fn expect_frame_type(frame: Frame, expected: ControlCommand, expected_name: &str) -> Result<Frame> {
    match frame.frame_type {
        FrameType::Control(cmd) if cmd == expected => Ok(frame),
        FrameType::Control(ControlCommand::ErrorResponse) => {
            Err(ClientError::ServerError(extract_error_message(&frame)))
        },
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
        },
        other => Err(ClientError::InvalidResponse(format!(
            "Expected TopicResponse, got {:?}",
            other
        ))),
    }
}

/// Authentication configuration for client connections
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Whether mutual TLS (mTLS) authentication is enabled
    pub mtls_enabled: bool,
    /// Path to the client certificate file for mTLS
    pub client_cert_path: Option<String>,
    /// Path to the client private key file for mTLS
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

/// Information about a topic
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Unique topic identifier
    pub id: u32,
    /// Topic name
    pub name: String,
    /// Unix timestamp when the topic was created
    pub created_at: u64,
    /// Topic identity epoch for stale-id detection
    pub topic_epoch: u64,
    /// Retention policy configuration (None = no retention policy set)
    pub retention: Option<RetentionInfo>,
}

/// Result of a fetch operation
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// Raw data fetched from the topic
    pub data: Bytes,
    /// Offset to use for the next fetch operation
    pub next_offset: u64,
    /// Number of bytes returned in this fetch
    pub bytes_returned: u32,
    /// Number of records in the fetched data
    pub record_count: u32,
}

/// Result of a subscribe operation
#[derive(Debug, Clone)]
pub struct SubscribeResult {
    /// Assigned consumer identifier
    pub consumer_id: u64,
    /// Starting offset for consumption
    pub start_offset: u64,
}

/// Result of a commit offset operation
#[derive(Debug, Clone)]
pub struct CommitResult {
    /// Consumer identifier that committed the offset
    pub consumer_id: u64,
    /// The offset that was successfully committed
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

/// Configuration for the LANCE client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server address to connect to (supports both IP:port and hostname:port)
    pub addr: String,
    /// Timeout for establishing connections
    pub connect_timeout: Duration,
    /// Timeout for read operations
    pub read_timeout: Duration,
    /// Timeout for write operations
    pub write_timeout: Duration,
    /// Interval between keepalive messages
    pub keepalive_interval: Duration,
    /// Optional TLS configuration for encrypted connections
    pub tls: Option<TlsClientConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:1992".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(10),
            tls: None,
        }
    }
}

impl ClientConfig {
    /// Create a new client configuration with the specified server address
    ///
    /// The address can be either an IP:port (e.g., "127.0.0.1:1992") or
    /// a hostname:port (e.g., "lance.example.com:1992"). DNS resolution
    /// is performed asynchronously during connection.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            ..Default::default()
        }
    }

    /// Enables TLS for this configuration so clients can satisfy
    /// Architecture §14's production security guidance when traversing
    /// untrusted networks.
    ///
    /// # Arguments
    /// * `tls_config` - Certificates and trust roots passed through to
    ///   `lnc-network`'s TLS connector.
    ///
    /// # Returns
    /// * `Self` - Updated config allowing fluent builder-style chaining.
    pub fn with_tls(mut self, tls_config: TlsClientConfig) -> Self {
        self.tls = Some(tls_config);
        self
    }

    /// Check if TLS is enabled
    pub fn is_tls_enabled(&self) -> bool {
        self.tls.is_some()
    }
}

/// LANCE protocol client for communicating with LANCE servers
///
/// Provides methods for ingesting data, managing topics, and consuming records.
pub struct LanceClient {
    stream: ClientStream,
    config: ClientConfig,
    batch_id: AtomicU64,
    read_buffer: Vec<u8>,
    read_offset: usize,
}

impl LanceClient {
    /// Resolves an address string (hostname:port or IP:port) to a `SocketAddr`
    /// so clients can honor Architecture §10.1's Docker-first deployment model
    /// where hostnames are common.
    ///
    /// # Arguments
    /// * `addr` - Address string such as `"10.0.0.5:1992"` or
    ///   `"broker.lance:1992"`.
    ///
    /// # Returns
    /// * `Result<SocketAddr>` - Parsed IP:port pair ready for `TcpStream`.
    ///
    /// # Errors
    /// * [`ClientError::ProtocolError`] when DNS resolution fails or produces
    ///   no usable endpoints.
    async fn resolve_address(addr: &str) -> Result<SocketAddr> {
        // First, try parsing as a SocketAddr directly (for IP:port format)
        if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
            return Ok(socket_addr);
        }

        // If direct parsing fails, perform DNS resolution (for hostname:port format)
        let mut addrs = lookup_host(addr).await.map_err(|e| {
            ClientError::ProtocolError(format!("DNS resolution failed for '{}': {}", addr, e))
        })?;

        addrs
            .next()
            .ok_or_else(|| ClientError::ProtocolError(format!("No addresses found for '{}'", addr)))
    }

    /// Connect to LANCE server, automatically using TLS if configured
    ///
    /// The address in the config can be either an IP:port or hostname:port.
    /// DNS resolution is performed automatically for hostnames.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        // If TLS is configured in ClientConfig, use TLS connection
        if let Some(ref tls_config) = config.tls {
            return Self::connect_tls(config.clone(), tls_config.clone()).await;
        }

        debug!(addr = %config.addr, "Connecting to LANCE server");

        // Resolve address (handles both IP:port and hostname:port)
        let socket_addr = Self::resolve_address(&config.addr).await?;
        debug!(resolved_addr = %socket_addr, "Resolved server address");

        let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(socket_addr))
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
    /// * `config` - Client configuration with server address (IP:port or hostname:port)
    /// * `tls_config` - TLS configuration including certificates
    ///
    /// # Example
    /// ```rust,ignore
    /// use lnc_client::{ClientConfig, TlsClientConfig, LanceClient};
    ///
    /// let config = ClientConfig::new("lance.example.com:1992");
    /// let tls = TlsClientConfig::new()
    ///     .with_ca_cert("/path/to/ca.pem");
    ///
    /// let client = LanceClient::connect_tls(config, tls).await?;
    /// ```
    pub async fn connect_tls(config: ClientConfig, tls_config: TlsClientConfig) -> Result<Self> {
        debug!(addr = %config.addr, "Connecting to LANCE server with TLS");

        // Resolve address (handles both IP:port and hostname:port)
        let socket_addr = Self::resolve_address(&config.addr).await?;
        debug!(resolved_addr = %socket_addr, "Resolved server address");

        // First establish TCP connection
        let tcp_stream =
            tokio::time::timeout(config.connect_timeout, TcpStream::connect(socket_addr))
                .await
                .map_err(|_| ClientError::Timeout)?
                .map_err(ClientError::ConnectionFailed)?;

        tcp_stream.set_nodelay(true)?;

        // Create TLS connector
        let network_config = tls_config.to_network_config();
        let connector =
            TlsConnector::new(network_config).map_err(|e| ClientError::TlsError(e.to_string()))?;

        // Determine server name for SNI - prefer configured name, then extract hostname from address
        let server_name = tls_config.server_name.unwrap_or_else(|| {
            // Extract hostname from address (remove port if present)
            config
                .addr
                .rsplit_once(':')
                .map(|(host, _)| host.to_string())
                .unwrap_or_else(|| socket_addr.ip().to_string())
        });

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

    /// Connect to a LANCE server using an address string
    ///
    /// The address can be either an IP:port (e.g., "127.0.0.1:1992") or
    /// a hostname:port (e.g., "lance.example.com:1992").
    pub async fn connect_to(addr: &str) -> Result<Self> {
        Self::connect(ClientConfig::new(addr)).await
    }

    /// Connect to LANCE server with TLS using address string
    ///
    /// The address can be either an IP:port (e.g., "127.0.0.1:1992") or
    /// a hostname:port (e.g., "lance.example.com:1992").
    pub async fn connect_tls_to(addr: &str, tls_config: TlsClientConfig) -> Result<Self> {
        Self::connect_tls(ClientConfig::new(addr), tls_config).await
    }

    fn next_batch_id(&self) -> u64 {
        self.batch_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Sends an ingest frame to the default topic (ID 0) while preserving the
    /// Architecture §22 write-buffering guarantees.
    ///
    /// # Arguments
    /// * `payload` - Record batch encoded using the zero-copy LWP format.
    /// * `record_count` - Logical record total encoded in the frame header.
    ///
    /// # Returns
    /// * `Result<u64>` - Server-assigned batch identifier for the frame.
    ///
    /// # Errors
    /// Propagates [`ClientError::Timeout`] when the write exceeds the configured
    /// deadline or any framing/connection error surfaced by Tokio.
    pub async fn send_ingest(&mut self, payload: Bytes, record_count: u32) -> Result<u64> {
        self.send_ingest_to_topic(0, payload, record_count, None)
            .await
    }

    /// Sends an ingest frame to a specific topic while attaching metadata
    /// required by Architecture §22's deferred flush/ack scheme.
    ///
    /// # Arguments
    /// * `topic_id` - Destination topic identifier.
    /// * `payload` - Zero-copy encoded batch.
    /// * `record_count` - Logical records contained in `payload`.
    /// * `_auth_config` - Optional future hook for per-request auth context.
    ///
    /// # Returns
    /// * `Result<u64>` - Batch identifier allocated by the client monotonic
    ///   counter and echoed back by the server.
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

    /// Sends an ingest request to the default topic and waits for the
    /// corresponding acknowledgment, mirroring Architecture §22.3 sync gates.
    ///
    /// # Arguments
    /// * `payload` - Ingest batch to transmit.
    /// * `record_count` - Logical record total for metrics validation.
    ///
    /// # Returns
    /// * `Result<u64>` - The acked batch identifier if the server confirms the
    ///   write succeeded.
    pub async fn send_ingest_sync(&mut self, payload: Bytes, record_count: u32) -> Result<u64> {
        self.send_ingest_to_topic_sync(0, payload, record_count, None)
            .await
    }

    /// Sends an ingest request to a topic and blocks for server acknowledgment
    /// so callers can enforce durability or backpressure decisions inline.
    ///
    /// # Arguments
    /// * `topic_id` - Destination topic.
    /// * `payload` - Zero-copy loaner buffer to transmit.
    /// * `record_count` - Logical records contained in the batch.
    /// * `auth_config` - Optional per-request authentication context.
    ///
    /// # Returns
    /// * `Result<u64>` - The acked batch identifier, ensuring sequencing with
    ///   downstream consumers.
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

    /// Waits for a specific acknowledgment frame, enforcing Architecture §22's
    /// deferred flush contract between ingestion and persistence stages.
    ///
    /// # Arguments
    /// * `expected_batch_id` - Identifier produced by the paired send path.
    ///
    /// # Returns
    /// * `Result<u64>` - The acked batch identifier (matching expectation).
    ///
    /// # Errors
    /// Surfaces protocol mismatches, server backpressure, or error frames so
    /// callers can react immediately.
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

    /// Receives the next acknowledgment frame and translates server feedback
    /// (ack, backpressure, or error) into structured [`ClientError`] variants.
    ///
    /// # Returns
    /// * `Result<u64>` - Acked batch identifier if the server confirmed success.
    ///
    /// # Errors
    /// Surfaces [`ClientError::ServerBackpressure`] or
    /// [`ClientError::InvalidResponse`] when the frame type deviates from the
    /// Architecture §22 control flow expectations.
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

    /// Sends a keepalive frame so long-lived clients satisfy Architecture §9.4
    /// drain/force-exit requirements and keep connection state fresh.
    ///
    /// # Returns
    /// * `Result<()>` - Ok when the frame is flushed before the configured
    ///   write timeout expires.
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

    /// Waits for a keepalive response, guaranteeing the control-plane path is
    /// still healthy per Architecture §9.4 monitoring requirements.
    ///
    /// # Returns
    /// * `Result<()>` - Ok when the server replies with `FrameType::Keepalive`.
    ///
    /// # Errors
    /// Returns [`ClientError::InvalidResponse`] when any other frame type
    /// arrives, signaling connection drift.
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

    /// Ping the server and measure round-trip latency
    pub async fn ping(&mut self) -> Result<Duration> {
        let start = std::time::Instant::now();
        self.send_keepalive().await?;
        self.recv_keepalive().await?;
        Ok(start.elapsed())
    }

    /// Create a new topic with the given name
    pub async fn create_topic(&mut self, name: &str) -> Result<TopicInfo> {
        const DEFAULT_CREATE_TOPIC_ATTEMPTS: usize = 20;
        const DEFAULT_CREATE_TOPIC_BACKOFF_MS: u64 = 500;
        self.ensure_topic(
            name,
            DEFAULT_CREATE_TOPIC_ATTEMPTS,
            DEFAULT_CREATE_TOPIC_BACKOFF_MS,
        )
        .await
    }

    async fn create_topic_once(&mut self, name: &str) -> Result<TopicInfo> {
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

    /// Ensure a topic exists and return its metadata.
    ///
    /// This helper encapsulates common create/list convergence retry behavior so
    /// application callers (bench/chaos) can stay simple.
    pub async fn ensure_topic(
        &mut self,
        name: &str,
        max_attempts: usize,
        base_backoff_ms: u64,
    ) -> Result<TopicInfo> {
        let attempts = max_attempts.max(1);
        let mut last_error: Option<ClientError> = None;
        let mut saw_retryable_error = false;

        for attempt in 1..=attempts {
            let mut retryable_this_attempt = false;

            match self.create_topic_once(name).await {
                Ok(info) => {
                    trace!(
                        topic_id = info.id,
                        topic_name = %info.name,
                        attempt,
                        max_attempts = attempts,
                        "Topic ensured via create_topic"
                    );
                    return Ok(info);
                },
                Err(create_err) => {
                    if create_err.is_retryable() {
                        retryable_this_attempt = true;
                        saw_retryable_error = true;
                    }
                    last_error = Some(ClientError::ServerError(create_err.to_string()));
                    warn!(
                        topic_name = %name,
                        attempt,
                        max_attempts = attempts,
                        error = %create_err,
                        "create_topic failed during ensure_topic; retrying with list fallback"
                    );
                },
            }

            match self.list_topics().await {
                Ok(topics) => {
                    if let Some(topic) = topics.into_iter().find(|t| t.name == name) {
                        trace!(
                            topic_id = topic.id,
                            topic_name = %topic.name,
                            attempt,
                            max_attempts = attempts,
                            "Topic ensured via list_topics fallback"
                        );
                        return Ok(topic);
                    }
                },
                Err(list_err) => {
                    if list_err.is_retryable() {
                        retryable_this_attempt = true;
                        saw_retryable_error = true;
                    }
                    last_error = Some(ClientError::ServerError(list_err.to_string()));
                    warn!(
                        topic_name = %name,
                        attempt,
                        max_attempts = attempts,
                        error = %list_err,
                        "list_topics failed during ensure_topic"
                    );
                },
            }

            if attempt < attempts {
                let backoff_ms = if retryable_this_attempt {
                    base_backoff_ms.saturating_mul(attempt as u64).max(1)
                } else {
                    // Non-retryable errors are unlikely to heal with long sleeps.
                    base_backoff_ms.max(1)
                };
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                // Refresh connection to improve odds of landing on current leader
                // after elections and readiness transitions.
                let reconnect_config = self.config.clone();
                match Self::connect(reconnect_config).await {
                    Ok(new_client) => {
                        *self = new_client;
                    },
                    Err(reconnect_err) => {
                        warn!(
                            topic_name = %name,
                            attempt,
                            max_attempts = attempts,
                            error = %reconnect_err,
                            "ensure_topic reconnect attempt failed"
                        );
                        last_error = Some(reconnect_err);
                    },
                }
            }
        }

        if let Some(err) = last_error {
            return Err(ClientError::ServerError(format!(
                "ensure_topic('{}') failed after {} attempts: {}",
                name, attempts, err
            )));
        }

        if saw_retryable_error {
            return Err(ClientError::ServerError(format!(
                "ensure_topic('{}') exhausted {} retryable attempts",
                name, attempts
            )));
        }

        Err(ClientError::ServerError(format!(
            "topic '{}' not found after {} ensure_topic attempts",
            name, attempts
        )))
    }

    /// Ensure topic with standard retry profile suitable for benchmark/chaos tools.
    pub async fn ensure_topic_default(&mut self, name: &str) -> Result<TopicInfo> {
        const DEFAULT_ENSURE_TOPIC_ATTEMPTS: usize = 20;
        const DEFAULT_ENSURE_TOPIC_BACKOFF_MS: u64 = 500;
        self.ensure_topic(
            name,
            DEFAULT_ENSURE_TOPIC_ATTEMPTS,
            DEFAULT_ENSURE_TOPIC_BACKOFF_MS,
        )
        .await
    }

    /// List all topics on the server
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

    /// Get information about a specific topic
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

    /// Delete a topic by its ID
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

    /// Sets the retention policy for an existing topic, mirroring the
    /// configuration model documented in Architecture §12.7.
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

        trace!(
            topic_id,
            max_age_secs, max_bytes, "Setting retention policy"
        );

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

        trace!(
            name,
            max_age_secs, max_bytes, "Creating topic with retention"
        );

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
                                k.parse::<u16>()
                                    .ok()
                                    .map(|id| (id, v.as_str().unwrap_or("unknown").to_string()))
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

    /// Subscribes to a topic for streaming consumption, honoring the consumer
    /// coordination model described in Architecture §20.
    ///
    /// # Arguments
    /// * `topic_id` - Topic to follow.
    /// * `start_offset` - First logical offset to deliver.
    /// * `max_batch_bytes` - Maximum payload size per delivery window.
    /// * `consumer_id` - Stable consumer identifier for server-side tracking.
    ///
    /// # Returns
    /// * `Result<SubscribeResult>` - Contains the confirmed consumer_id and
    ///   start offset granted by the server.
    ///
    /// # Errors
    /// Surfaces timeouts, protocol violations, or server error frames (e.g.,
    /// `ControlCommand::ErrorResponse`).
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

    /// Unsubscribes a consumer from a topic, ensuring server resources are
    /// reclaimed per Architecture §20's consumer lifecycle.
    ///
    /// # Arguments
    /// * `topic_id` - Topic to leave.
    /// * `consumer_id` - Consumer identifier provided during subscribe.
    ///
    /// # Returns
    /// * `Result<()>` - Ok when the server acknowledges the unsubscribe.
    ///
    /// # Errors
    /// Propagates [`ClientError::ServerError`] if the server rejects the
    /// request or [`ClientError::InvalidResponse`] if a non-ack frame arrives.
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
            FrameType::Control(ControlCommand::CatchingUp) => {
                let server_offset = frame
                    .payload
                    .as_ref()
                    .filter(|p| p.len() >= 8)
                    .map(|p| u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]))
                    .unwrap_or(0);
                Err(ClientError::ServerCatchingUp { server_offset })
            },
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
                    topic_epoch: json["topic_epoch"].as_u64().unwrap_or(1),
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
                                        max_age_secs: t["retention"]["max_age_secs"]
                                            .as_u64()
                                            .unwrap_or(0),
                                        max_bytes: t["retention"]["max_bytes"]
                                            .as_u64()
                                            .unwrap_or(0),
                                    })
                                } else {
                                    None
                                };
                                TopicInfo {
                                    id: t["id"].as_u64().unwrap_or(0) as u32,
                                    name: t["name"].as_str().unwrap_or("").to_string(),
                                    created_at: t["created_at"].as_u64().unwrap_or(0),
                                    topic_epoch: t["topic_epoch"].as_u64().unwrap_or(1),
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

    /// Reads the next wire frame into the preallocated buffer, preserving the
    /// Architecture §15 zero-copy guarantees while translating parse failures
    /// into structured [`ClientError`] values.
    ///
    /// # Returns
    /// * `Result<Frame>` - Fully parsed LWP frame ready for higher-level
    ///   ingestion/consumer handlers.
    ///
    /// # Errors
    /// Propagates timeouts, malformed headers, or connection closures so
    /// callers can fail fast when the transport becomes unhealthy.
    async fn recv_frame(&mut self) -> Result<Frame> {
        // Max frame size cap to prevent OOM from malformed headers (16 MB)
        const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

        loop {
            if self.read_offset >= LWP_HEADER_SIZE {
                // Grow buffer if the header indicates a payload larger than current capacity
                let payload_len = u32::from_le_bytes([
                    self.read_buffer[32],
                    self.read_buffer[33],
                    self.read_buffer[34],
                    self.read_buffer[35],
                ]) as usize;
                let total_frame_size = LWP_HEADER_SIZE + payload_len;
                if total_frame_size > MAX_FRAME_SIZE {
                    return Err(ClientError::ServerError(format!(
                        "Frame too large: {} bytes",
                        total_frame_size
                    )));
                }
                if total_frame_size > self.read_buffer.len() {
                    self.read_buffer.resize(total_frame_size, 0);
                }

                if let Some((frame, consumed)) = parse_frame(&self.read_buffer[..self.read_offset])?
                {
                    self.read_buffer.copy_within(consumed..self.read_offset, 0);
                    self.read_offset -= consumed;
                    // Shrink buffer back to default if it was grown for a large frame
                    if self.read_buffer.len() > 64 * 1024 && self.read_offset < 64 * 1024 {
                        self.read_buffer.resize(64 * 1024, 0);
                    }
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

    /// Get a reference to the client configuration
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Close the client connection
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
