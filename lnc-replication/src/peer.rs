//! Peer connection management for replication networking (CONTROL PLANE).
//!
//! This module is part of the CONTROL PLANE. It handles TCP connections
//! to peer nodes for Raft consensus operations only. These connections
//! are used for control plane operations (leader election, AppendEntries,
//! etc.), NOT for actual data replication.
//!
//! # Architecture Note: Control Plane vs Data Plane
//!
//! LANCE strictly separates control plane from data plane:
//!
//! - **Control Plane Connections (this module)**: Used for Raft consensus.
//!   These handle leader election, AppendEntries RPCs for metadata, and
//!   cluster state synchronization. Do NOT use these for data replication.
//!
//! - **Data Plane**: Uses independent replication mechanism completely separate
//!   from these control plane connections. The data plane replicates actual
//!   write data from leader to followers and operates independently.
//!
//! # Key Point
//!
//! The `PeerManager` in this module is for Raft control plane only.
//! Data plane replication must use a separate mechanism - do not send
//! actual data payloads through these connections.
//!
//! For data plane implementation, see `lance/src/server/mod.rs`.
//!
//! See `docs/Architecture.md` section "Control Plane vs Data Plane Architecture"
//! for detailed explanation.
//!
//! Supports optional TLS encryption when the `tls` feature is enabled.

use crate::codec::{AppendEntriesRequest, AppendEntriesResponse};
use crate::codec::{PreVoteRequest, PreVoteResponse, VoteRequest, VoteResponse};
use crate::codec::{ReplicationCodec, ReplicationMessage};
use crate::follower::{FollowerHealth, FollowerStatus};
use bytes::{BufMut, Bytes};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

#[cfg(feature = "tls")]
use rustls::pki_types::ServerName;
#[cfg(feature = "tls")]
use tokio_rustls::client::TlsStream;

/// Abstraction over TCP and TLS streams for peer connections
pub enum PeerStream {
    /// Plain TCP connection
    Tcp(TcpStream),
    /// TLS-encrypted connection (when tls feature is enabled)
    #[cfg(feature = "tls")]
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for PeerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            PeerStream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            PeerStream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            PeerStream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            PeerStream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Configuration for peer connections
#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub reconnect_delay: Duration,
    pub max_reconnect_attempts: u32,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            // 1s connect timeout is too aggressive during election/restart churn.
            // Allow a slightly wider budget before forcing disconnect/reconnect loops.
            connect_timeout: Duration::from_secs(3),
            // 2s was too aggressive under benchmark/chaos load and caused
            // repeated disconnect/reconnect churn during legitimate follower stalls.
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(2),
            reconnect_delay: Duration::from_millis(250),
            max_reconnect_attempts: 10,
        }
    }
}

/// State of a peer connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    Disconnected,
    Connecting,
    Connected,
    Failed,
}

type PeerConnectionHandle = Arc<Mutex<PeerConnection>>;
type PeerConnectionMap = Arc<RwLock<HashMap<u16, PeerConnectionHandle>>>;

/// Represents a connection to a peer node
pub struct PeerConnection {
    pub node_id: u16,
    pub addr: SocketAddr,
    pub state: PeerState,
    stream: Option<PeerStream>,
    config: PeerConfig,
    reconnect_attempts: u32,
    /// Optional TLS connector for secure peer connections
    #[cfg(feature = "tls")]
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
}

impl PeerConnection {
    /// Create a new peer connection (plain TCP)
    pub fn new(node_id: u16, addr: SocketAddr, config: PeerConfig) -> Self {
        Self {
            node_id,
            addr,
            state: PeerState::Disconnected,
            stream: None,
            config,
            reconnect_attempts: 0,
            #[cfg(feature = "tls")]
            tls_connector: None,
        }
    }

    /// Create a new peer connection with TLS support
    #[cfg(feature = "tls")]
    pub fn new_with_tls(
        node_id: u16,
        addr: SocketAddr,
        config: PeerConfig,
        tls_connector: Arc<tokio_rustls::TlsConnector>,
    ) -> Self {
        Self {
            node_id,
            addr,
            state: PeerState::Disconnected,
            stream: None,
            config,
            reconnect_attempts: 0,
            tls_connector: Some(tls_connector),
        }
    }

    pub async fn connect(&mut self) -> Result<(), std::io::Error> {
        self.state = PeerState::Connecting;

        match tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(self.addr)).await
        {
            Ok(Ok(tcp_stream)) => {
                tcp_stream.set_nodelay(true)?;

                // Wrap with TLS if connector is configured
                #[cfg(feature = "tls")]
                let stream = if let Some(ref connector) = self.tls_connector {
                    let server_name = ServerName::try_from(self.addr.ip().to_string())
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                    let tls_stream = connector
                        .connect(server_name.to_owned(), tcp_stream)
                        .await
                        .map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                        })?;
                    PeerStream::Tls(Box::new(tls_stream))
                } else {
                    PeerStream::Tcp(tcp_stream)
                };

                #[cfg(not(feature = "tls"))]
                let stream = PeerStream::Tcp(tcp_stream);

                self.stream = Some(stream);
                self.state = PeerState::Connected;
                self.reconnect_attempts = 0;
                info!(
                    target: "lance::replication",
                    node_id = self.node_id,
                    addr = %self.addr,
                    tls = cfg!(feature = "tls") && self.has_tls(),
                    "Connected to peer"
                );
                Ok(())
            },
            Ok(Err(e)) => {
                self.state = PeerState::Failed;
                self.reconnect_attempts += 1;
                warn!(
                    target: "lance::replication",
                    node_id = self.node_id,
                    addr = %self.addr,
                    error = %e,
                    attempts = self.reconnect_attempts,
                    "Failed to connect to peer"
                );
                Err(e)
            },
            Err(_) => {
                self.state = PeerState::Failed;
                self.reconnect_attempts += 1;
                warn!(
                    target: "lance::replication",
                    node_id = self.node_id,
                    addr = %self.addr,
                    attempts = self.reconnect_attempts,
                    "Connection timeout"
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Connection timeout",
                ))
            },
        }
    }

    /// Check if TLS is enabled for this connection
    #[cfg(feature = "tls")]
    pub fn has_tls(&self) -> bool {
        self.tls_connector.is_some()
    }

    #[cfg(not(feature = "tls"))]
    pub fn has_tls(&self) -> bool {
        false
    }

    /// Minimum message size to apply LZ4 compression (4KB).
    ///
    /// 1KB data-plane frames are latency-sensitive and often incompressible,
    /// so attempting compression on every append adds avoidable CPU overhead.
    const COMPRESSION_THRESHOLD: usize = 4 * 1024;
    /// High bit of the length prefix signals LZ4 compression.
    const COMPRESSED_FLAG: u32 = 0x8000_0000;

    pub async fn send(&mut self, msg: &ReplicationMessage) -> Result<(), std::io::Error> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
        })?;

        let mut codec = ReplicationCodec::new();
        let encoded = codec
            .encode(msg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        // LZ4 compress large messages to reduce replication bandwidth.
        // Use BytesMut throughout to avoid separate Vec heap allocations
        // for the frame envelope — the length prefix and payload are written
        // into a single contiguous buffer via BufMut.
        let frame = if encoded.len() >= Self::COMPRESSION_THRESHOLD {
            let compressor = lnc_network::Compressor::lz4();
            match compressor.compress_raw(&encoded) {
                Ok(compressed) if compressed.len() < encoded.len() => {
                    // Compressed is smaller — use it. Prepend original length for decompression.
                    let payload_len = 4 + compressed.len();
                    let frame_len = payload_len as u32 | Self::COMPRESSED_FLAG;
                    let mut frame = bytes::BytesMut::with_capacity(4 + payload_len);
                    frame.put_u32_le(frame_len);
                    frame.put_u32_le(encoded.len() as u32);
                    frame.put_slice(&compressed);
                    frame
                },
                _ => {
                    // Compression didn't help or failed — send uncompressed
                    let mut frame = bytes::BytesMut::with_capacity(4 + encoded.len());
                    frame.put_u32_le(encoded.len() as u32);
                    frame.put_slice(&encoded);
                    frame
                },
            }
        } else {
            let mut frame = bytes::BytesMut::with_capacity(4 + encoded.len());
            frame.put_u32_le(encoded.len() as u32);
            frame.put_slice(&encoded);
            frame
        };

        match tokio::time::timeout(self.config.write_timeout, stream.write_all(&frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.disconnect();
                Err(e)
            },
            Err(_) => {
                self.disconnect();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Write timeout",
                ))
            },
        }
    }

    pub async fn recv(&mut self) -> Result<ReplicationMessage, std::io::Error> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
        })?;

        // Read length prefix (4 bytes). High bit = LZ4 compressed.
        let mut len_buf = [0u8; 4];
        match tokio::time::timeout(self.config.read_timeout, stream.read_exact(&mut len_buf)).await
        {
            Ok(Ok(_)) => {},
            Ok(Err(e)) => {
                self.disconnect();
                return Err(e);
            },
            Err(_) => {
                self.disconnect();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Read timeout",
                ));
            },
        }

        let raw_len = u32::from_le_bytes(len_buf);
        let is_compressed = raw_len & Self::COMPRESSED_FLAG != 0;
        let length = (raw_len & !Self::COMPRESSED_FLAG) as usize;

        // Read payload
        let mut payload = vec![0u8; length];
        if length > 0 {
            match tokio::time::timeout(self.config.read_timeout, stream.read_exact(&mut payload))
                .await
            {
                Ok(Ok(_)) => {},
                Ok(Err(e)) => {
                    self.disconnect();
                    return Err(e);
                },
                Err(_) => {
                    self.disconnect();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Read timeout",
                    ));
                },
            }
        }

        // Decompress if the high bit was set
        let decode_buf = if is_compressed && payload.len() >= 4 {
            let original_len =
                u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
            let compressor = lnc_network::Compressor::lz4();
            let decompressed = compressor
                .decompress_raw(
                    lnc_network::CompressionAlgorithm::Lz4,
                    &payload[4..],
                    original_len,
                )
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("LZ4 decompress failed: {e}"),
                    )
                })?;
            decompressed.to_vec()
        } else {
            payload
        };

        // Bytes::from(Vec) is zero-copy ownership transfer (no memcpy)
        ReplicationCodec::decode(Bytes::from(decode_buf))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Send pre-encoded bytes with LZ4 compression and BytesMut framing.
    ///
    /// This is the broadcast-optimized path: the caller has already serialized
    /// the `ReplicationMessage` via `ReplicationCodec::encode()` once, and we
    /// only need to frame + optionally compress + write. Avoids re-encoding
    /// the same message N times for N peers.
    pub async fn send_raw_encoded(&mut self, encoded: &[u8]) -> Result<(), std::io::Error> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
        })?;

        let frame = if encoded.len() >= Self::COMPRESSION_THRESHOLD {
            let compressor = lnc_network::Compressor::lz4();
            match compressor.compress_raw(encoded) {
                Ok(compressed) if compressed.len() < encoded.len() => {
                    let payload_len = 4 + compressed.len();
                    let frame_len = payload_len as u32 | Self::COMPRESSED_FLAG;
                    let mut frame = bytes::BytesMut::with_capacity(4 + payload_len);
                    frame.put_u32_le(frame_len);
                    frame.put_u32_le(encoded.len() as u32);
                    frame.put_slice(&compressed);
                    frame
                },
                _ => {
                    let mut frame = bytes::BytesMut::with_capacity(4 + encoded.len());
                    frame.put_u32_le(encoded.len() as u32);
                    frame.put_slice(encoded);
                    frame
                },
            }
        } else {
            let mut frame = bytes::BytesMut::with_capacity(4 + encoded.len());
            frame.put_u32_le(encoded.len() as u32);
            frame.put_slice(encoded);
            frame
        };

        match tokio::time::timeout(self.config.write_timeout, stream.write_all(&frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.disconnect();
                Err(e)
            },
            Err(_) => {
                self.disconnect();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Write timeout",
                ))
            },
        }
    }

    /// Write a fully-prepared wire frame directly to the stream.
    ///
    /// **Zero per-peer work**: the caller has already performed serialization,
    /// LZ4 compression, and length-prefix framing via
    /// [`PeerManager::prepare_wire_frame`]. This method only does the
    /// `write_all` syscall — no allocation, no compression, no encoding.
    ///
    /// This is the "Final Wire Format" path described in §10.2:
    /// compress once in the actor, then fan-out the identical bytes to N peers.
    pub async fn send_wire_frame(&mut self, wire_frame: &[u8]) -> Result<(), std::io::Error> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
        })?;

        match tokio::time::timeout(self.config.write_timeout, stream.write_all(wire_frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.disconnect();
                Err(e)
            },
            Err(_) => {
                self.disconnect();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Write timeout",
                ))
            },
        }
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
        self.state = PeerState::Disconnected;
        debug!(
            target: "lance::replication",
            node_id = self.node_id,
            "Disconnected from peer"
        );
    }

    pub fn is_connected(&self) -> bool {
        self.state == PeerState::Connected && self.stream.is_some()
    }

    pub fn should_reconnect(&self) -> bool {
        self.state != PeerState::Connected
            && self.reconnect_attempts < self.config.max_reconnect_attempts
    }
}

/// Manages connections to all peer nodes with health tracking
pub struct PeerManager {
    peers: PeerConnectionMap,
    control_peers: PeerConnectionMap,
    election_peers: PeerConnectionMap,
    health: Arc<RwLock<HashMap<u16, FollowerHealth>>>,
    config: PeerConfig,
}

impl PeerManager {
    /// Synthetic latency sample applied when an AppendEntries RPC fails before a
    /// valid response is decoded. This drives health eviction for persistently
    /// failing peers so fanout can focus on healthy quorum members.
    const RPC_FAILURE_PENALTY_LATENCY: Duration = Duration::from_secs(1);

    pub fn new(_node_id: u16, config: PeerConfig) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            control_peers: Arc::new(RwLock::new(HashMap::new())),
            election_peers: Arc::new(RwLock::new(HashMap::new())),
            health: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub async fn add_peer(&self, peer_id: u16, addr: SocketAddr) {
        let mut inserted = false;
        let (conn, control_conn, election_conn) = {
            let mut peers = self.peers.write().await;
            let mut control_peers = self.control_peers.write().await;
            let mut election_peers = self.election_peers.write().await;

            if let (Some(existing), Some(existing_control), Some(existing_election)) = (
                peers.get(&peer_id),
                control_peers.get(&peer_id),
                election_peers.get(&peer_id),
            ) {
                (
                    Arc::clone(existing),
                    Arc::clone(existing_control),
                    Arc::clone(existing_election),
                )
            } else {
                let conn = Arc::new(Mutex::new(PeerConnection::new(
                    peer_id,
                    addr,
                    self.config.clone(),
                )));
                let control_conn = Arc::new(Mutex::new(PeerConnection::new(
                    peer_id,
                    addr,
                    self.config.clone(),
                )));
                let election_conn = Arc::new(Mutex::new(PeerConnection::new(
                    peer_id,
                    addr,
                    self.config.clone(),
                )));
                peers.insert(peer_id, Arc::clone(&conn));
                control_peers.insert(peer_id, Arc::clone(&control_conn));
                election_peers.insert(peer_id, Arc::clone(&election_conn));
                inserted = true;
                (conn, control_conn, election_conn)
            }
        };

        if inserted {
            // Initialize health tracking for this peer
            let mut health = self.health.write().await;
            health.insert(peer_id, FollowerHealth::new(peer_id));

            info!(
                target: "lance::replication",
                peer_id,
                addr = %addr,
                "Added peer with health tracking"
            );
            return;
        }

        let mut existing = conn.lock().await;
        let mut existing_control = control_conn.lock().await;
        let mut existing_election = election_conn.lock().await;
        if existing.addr != addr {
            info!(
                target: "lance::replication",
                peer_id,
                old_addr = %existing.addr,
                new_addr = %addr,
                "Peer address changed, updating connection"
            );
            *existing = PeerConnection::new(peer_id, addr, self.config.clone());
            *existing_control = PeerConnection::new(peer_id, addr, self.config.clone());
            *existing_election = PeerConnection::new(peer_id, addr, self.config.clone());
        }
    }

    pub async fn remove_peer(&self, peer_id: u16) {
        let removed = {
            let mut peers = self.peers.write().await;
            peers.remove(&peer_id)
        };
        let removed_control = {
            let mut peers = self.control_peers.write().await;
            peers.remove(&peer_id)
        };
        let removed_election = {
            let mut peers = self.election_peers.write().await;
            peers.remove(&peer_id)
        };

        if let Some(conn) = removed {
            let mut conn = conn.lock().await;
            conn.disconnect();
            drop(conn);

            // Remove health tracking
            let mut health = self.health.write().await;
            health.remove(&peer_id);

            info!(
                target: "lance::replication",
                peer_id,
                "Removed peer"
            );
        }

        if let Some(conn) = removed_control {
            let mut conn = conn.lock().await;
            conn.disconnect();
        }

        if let Some(conn) = removed_election {
            let mut conn = conn.lock().await;
            conn.disconnect();
        }
    }

    pub async fn connect_all(&self) {
        let peers: Vec<(u16, Arc<Mutex<PeerConnection>>)> = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .map(|(peer_id, conn)| (*peer_id, Arc::clone(conn)))
                .collect()
        };

        for (peer_id, conn) in peers {
            let mut conn = conn.lock().await;
            if !conn.is_connected() && conn.should_reconnect() {
                if let Err(e) = conn.connect().await {
                    warn!(
                        target: "lance::replication",
                        peer_id,
                        error = %e,
                        "Failed to connect to peer"
                    );
                }
            }
        }
    }

    pub async fn disconnect_all(&self) {
        let peers: Vec<(u16, Arc<Mutex<PeerConnection>>)> = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .map(|(peer_id, conn)| (*peer_id, Arc::clone(conn)))
                .collect()
        };

        for (peer_id, conn) in peers {
            let mut conn = conn.lock().await;
            if conn.is_connected() {
                conn.disconnect();
                info!(
                    target: "lance::replication",
                    peer_id,
                    "Disconnected peer during shutdown"
                );
            }
        }

        let control_peers: Vec<(u16, Arc<Mutex<PeerConnection>>)> = {
            let peers = self.control_peers.read().await;
            peers
                .iter()
                .map(|(peer_id, conn)| (*peer_id, Arc::clone(conn)))
                .collect()
        };

        for (_peer_id, conn) in control_peers {
            let mut conn = conn.lock().await;
            if conn.is_connected() {
                conn.disconnect();
            }
        }

        let election_peers: Vec<(u16, Arc<Mutex<PeerConnection>>)> = {
            let peers = self.election_peers.read().await;
            peers
                .iter()
                .map(|(peer_id, conn)| (*peer_id, Arc::clone(conn)))
                .collect()
        };

        for (_peer_id, conn) in election_peers {
            let mut conn = conn.lock().await;
            if conn.is_connected() {
                conn.disconnect();
            }
        }
    }

    async fn get_peer_connection(
        &self,
        peer_id: u16,
    ) -> Result<PeerConnectionHandle, std::io::Error> {
        let peers = self.peers.read().await;
        peers
            .get(&peer_id)
            .cloned()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))
    }

    async fn get_election_peer_connection(
        &self,
        peer_id: u16,
    ) -> Result<PeerConnectionHandle, std::io::Error> {
        let peers = self.election_peers.read().await;
        peers
            .get(&peer_id)
            .cloned()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))
    }

    async fn get_control_peer_connection(
        &self,
        peer_id: u16,
    ) -> Result<PeerConnectionHandle, std::io::Error> {
        let peers = self.control_peers.read().await;
        peers
            .get(&peer_id)
            .cloned()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))
    }

    pub async fn send_to_peer(
        &self,
        peer_id: u16,
        msg: &ReplicationMessage,
    ) -> Result<(), std::io::Error> {
        let conn = self.get_peer_connection(peer_id).await?;
        let mut conn = conn.lock().await;

        if !conn.is_connected() {
            conn.connect().await?;
        }

        conn.send(msg).await
    }

    pub async fn send_append_entries(
        &self,
        peer_id: u16,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, std::io::Error> {
        let msg = ReplicationMessage::AppendEntriesRequest(request);
        let start = Instant::now();

        let conn = self.get_peer_connection(peer_id).await?;
        let mut conn = conn.lock().await;
        let outcome = async {
            if !conn.is_connected() {
                conn.connect().await?;
            }
            conn.send(&msg).await?;
            conn.recv().await
        }
        .await;
        let latency = start.elapsed();
        drop(conn);

        match outcome {
            Ok(ReplicationMessage::AppendEntriesResponse(resp)) => {
                self.record_peer_latency(peer_id, latency).await;
                Ok(resp)
            },
            Ok(_) => {
                self.record_peer_latency(peer_id, Self::RPC_FAILURE_PENALTY_LATENCY)
                    .await;
                let conn = self.get_peer_connection(peer_id).await?;
                conn.lock().await.disconnect();
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected AppendEntriesResponse",
                ))
            },
            Err(e) => {
                self.record_peer_latency(peer_id, Self::RPC_FAILURE_PENALTY_LATENCY)
                    .await;
                Err(e)
            },
        }
    }

    /// Send AppendEntries over the control-plane connection.
    ///
    /// This isolates heartbeats from data replication so a busy data stream does
    /// not starve liveness RPCs on the same peer mutex/stream.
    pub async fn send_append_entries_control(
        &self,
        peer_id: u16,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, std::io::Error> {
        let msg = ReplicationMessage::AppendEntriesRequest(request);
        let start = Instant::now();
        let conn = self.get_control_peer_connection(peer_id).await?;
        if conn.try_lock().is_err() {
            lnc_metrics::increment_control_rpc_starvation();
        }
        lnc_metrics::increment_control_rpc_in_flight();

        let mut conn = conn.lock().await;
        let result = async {
            if !conn.is_connected() {
                conn.connect().await?;
            }
            conn.send(&msg).await?;
            let response_msg = conn.recv().await?;

            match response_msg {
                ReplicationMessage::AppendEntriesResponse(resp) => Ok(resp),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected AppendEntriesResponse",
                )),
            }
        }
        .await;

        lnc_metrics::decrement_control_rpc_in_flight();
        lnc_metrics::record_raft_vote_rpc_latency_ms(start.elapsed().as_millis() as u64);

        match result {
            Ok(resp) => Ok(resp),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                let conn = self.get_control_peer_connection(peer_id).await?;
                conn.lock().await.disconnect();
                Err(e)
            },
            Err(e) => Err(e),
        }
    }

    /// Send a PreVoteRequest to a peer and wait for the PreVoteResponse.
    pub async fn send_pre_vote_request(
        &self,
        peer_id: u16,
        request: PreVoteRequest,
    ) -> Result<PreVoteResponse, std::io::Error> {
        let msg = ReplicationMessage::PreVoteRequest(request);
        let start = Instant::now();
        let conn = self.get_election_peer_connection(peer_id).await?;
        if conn.try_lock().is_err() {
            lnc_metrics::increment_control_rpc_starvation();
        }
        lnc_metrics::increment_control_rpc_in_flight();

        let mut conn = conn.lock().await;
        let result = async {
            if !conn.is_connected() {
                conn.connect().await?;
            }
            conn.send(&msg).await?;
            let response_msg = conn.recv().await?;

            match response_msg {
                ReplicationMessage::PreVoteResponse(resp) => Ok(resp),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected PreVoteResponse",
                )),
            }
        }
        .await;

        lnc_metrics::decrement_control_rpc_in_flight();
        lnc_metrics::record_raft_pre_vote_rpc_latency_ms(start.elapsed().as_millis() as u64);

        match result {
            Ok(resp) => Ok(resp),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                let conn = self.get_election_peer_connection(peer_id).await?;
                conn.lock().await.disconnect();
                Err(e)
            },
            Err(e) => Err(e),
        }
    }

    /// Send a VoteRequest to a peer and wait for the VoteResponse.
    pub async fn send_vote_request(
        &self,
        peer_id: u16,
        request: VoteRequest,
    ) -> Result<VoteResponse, std::io::Error> {
        let msg = ReplicationMessage::VoteRequest(request);
        let start = Instant::now();
        let conn = self.get_election_peer_connection(peer_id).await?;
        if conn.try_lock().is_err() {
            lnc_metrics::increment_control_rpc_starvation();
        }
        lnc_metrics::increment_control_rpc_in_flight();

        let mut conn = conn.lock().await;
        let result = async {
            if !conn.is_connected() {
                conn.connect().await?;
            }
            conn.send(&msg).await?;
            let response_msg = conn.recv().await?;

            match response_msg {
                ReplicationMessage::VoteResponse(resp) => Ok(resp),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected VoteResponse",
                )),
            }
        }
        .await;

        lnc_metrics::decrement_control_rpc_in_flight();
        lnc_metrics::record_raft_vote_rpc_latency_ms(start.elapsed().as_millis() as u64);

        match result {
            Ok(resp) => Ok(resp),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                let conn = self.get_peer_connection(peer_id).await?;
                conn.lock().await.disconnect();
                Err(e)
            },
            Err(e) => Err(e),
        }
    }

    /// Record a latency measurement for a peer and update health status
    pub async fn record_peer_latency(
        &self,
        peer_id: u16,
        latency: Duration,
    ) -> Option<FollowerStatus> {
        let mut health = self.health.write().await;
        if let Some(follower_health) = health.get_mut(&peer_id) {
            follower_health.record_latency(latency)
        } else {
            None
        }
    }

    /// Get the health status of a specific peer
    pub async fn get_peer_health(&self, peer_id: u16) -> Option<FollowerStatus> {
        let health = self.health.read().await;
        health.get(&peer_id).map(|h| h.status)
    }

    /// Get all healthy peer IDs (not evicted from quorum)
    pub async fn healthy_peer_ids(&self) -> Vec<u16> {
        let health = self.health.read().await;
        health
            .iter()
            .filter(|(_, h)| !h.is_evicted())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get count of healthy peers (for quorum calculation)
    pub async fn healthy_peer_count(&self) -> usize {
        let health = self.health.read().await;
        health.values().filter(|h| !h.is_evicted()).count()
    }

    /// Get all peer health statuses
    pub async fn peer_health_statuses(&self) -> HashMap<u16, FollowerStatus> {
        let health = self.health.read().await;
        health.iter().map(|(id, h)| (*id, h.status)).collect()
    }

    /// Prepare a fully-framed wire envelope from a `ReplicationMessage`.
    ///
    /// Performs serialization, LZ4 compression (if beneficial), and
    /// length-prefix framing in a **single pass**. The returned `Arc<[u8]>`
    /// is the exact bytes to write to each peer's TCP stream — no further
    /// processing needed per-peer.
    ///
    /// This eliminates the N× compression overhead identified in the 10X
    /// audit (§10.2): with 5 followers, the old path spent 5× CPU cycles
    /// compressing identical bytes. Now we compress once and fan-out.
    pub fn prepare_wire_frame(msg: &ReplicationMessage) -> Result<Arc<[u8]>, std::io::Error> {
        let mut codec = ReplicationCodec::new();
        let encoded = codec
            .encode(msg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        let frame = if encoded.len() >= PeerConnection::COMPRESSION_THRESHOLD {
            let compressor = lnc_network::Compressor::lz4();
            match compressor.compress_raw(&encoded) {
                Ok(compressed) if compressed.len() < encoded.len() => {
                    let payload_len = 4 + compressed.len();
                    let frame_len = payload_len as u32 | PeerConnection::COMPRESSED_FLAG;
                    let mut frame = bytes::BytesMut::with_capacity(4 + payload_len);
                    frame.put_u32_le(frame_len);
                    frame.put_u32_le(encoded.len() as u32);
                    frame.put_slice(&compressed);
                    frame
                },
                _ => {
                    let mut frame = bytes::BytesMut::with_capacity(4 + encoded.len());
                    frame.put_u32_le(encoded.len() as u32);
                    frame.put_slice(&encoded);
                    frame
                },
            }
        } else {
            let mut frame = bytes::BytesMut::with_capacity(4 + encoded.len());
            frame.put_u32_le(encoded.len() as u32);
            frame.put_slice(&encoded);
            frame
        };

        Ok(Arc::from(frame.as_ref()))
    }

    /// Broadcast a message to all peers concurrently using JoinSet.
    ///
    /// Unlike the previous sequential implementation where latency = ∑(RTT),
    /// this fires all sends in parallel so latency = max(RTT). Critical for
    /// replication at 100Gbps where even 3 sequential RTTs compound.
    ///
    /// **Single-pass compression**: the message is serialized, compressed,
    /// and framed exactly once via [`Self::prepare_wire_frame`]. Each spawned
    /// task receives the identical `Arc<[u8]>` wire bytes and calls
    /// `send_wire_frame` which does only a `write_all` syscall.
    pub async fn broadcast(
        &self,
        msg: &ReplicationMessage,
    ) -> Vec<(u16, Result<(), std::io::Error>)> {
        let peer_ids: Vec<u16> = {
            let peers = self.peers.read().await;
            peers.keys().copied().collect()
        };

        // Encode + compress + frame ONCE
        let wire_frame = match Self::prepare_wire_frame(msg) {
            Ok(frame) => frame,
            Err(e) => {
                return peer_ids
                    .into_iter()
                    .map(|id| {
                        (
                            id,
                            Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e.to_string(),
                            )),
                        )
                    })
                    .collect();
            },
        };

        let mut join_set = JoinSet::new();
        let peers = self.peers.clone();

        for peer_id in peer_ids {
            let peers_ref = peers.clone();
            let frame = wire_frame.clone();
            join_set.spawn(async move {
                let result = Self::send_wire_frame_to_peer(&peers_ref, peer_id, &frame).await;
                (peer_id, result)
            });
        }

        let mut results = Vec::new();
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok((peer_id, result)) => results.push((peer_id, result)),
                Err(e) => {
                    warn!(
                        target: "lance::replication",
                        error = %e,
                        "Broadcast task panicked"
                    );
                },
            }
        }
        results
    }

    /// Send a pre-framed wire envelope directly to a specific peer.
    ///
    /// The `wire_frame` has already been through encode + compress + frame
    /// via [`Self::prepare_wire_frame`], so this only does connect-if-needed +
    /// `write_all`. Zero per-peer CPU work.
    async fn send_wire_frame_to_peer(
        peers: &PeerConnectionMap,
        peer_id: u16,
        wire_frame: &[u8],
    ) -> Result<(), std::io::Error> {
        let conn = {
            let peers_guard = peers.read().await;
            peers_guard.get(&peer_id).cloned().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found")
            })?
        };
        let mut conn = conn.lock().await;

        if !conn.is_connected() {
            conn.connect().await?;
        }

        conn.send_wire_frame(wire_frame).await
    }

    /// Send pre-framed wire bytes directly to a peer (for actor JoinSet path).
    ///
    /// This is the public API for the `ReplicationActor` to use when it has
    /// already prepared the wire frame via [`Self::prepare_wire_frame`]. Each
    /// follower task clones the `Arc<[u8]>` and calls this method — zero
    /// compression, zero encoding, just a `write_all`.
    pub async fn send_wire_bytes_directly(
        &self,
        peer_id: u16,
        wire_frame: Arc<[u8]>,
    ) -> Result<(), std::io::Error> {
        Self::send_wire_frame_to_peer(&self.peers, peer_id, &wire_frame).await
    }

    /// Send an AppendEntriesRequest to a peer using pre-shared data (for concurrent replication).
    ///
    /// This is the JoinSet-friendly variant: it takes `Arc<[u8]>` data that can be
    /// cheaply cloned across spawned tasks without re-encoding per follower.
    /// Returns the response and the measured latency.
    pub async fn send_append_entries_raw(
        &self,
        peer_id: u16,
        request: AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, Duration), std::io::Error> {
        let msg = ReplicationMessage::AppendEntriesRequest(request);
        let start = Instant::now();

        let conn = self.get_peer_connection(peer_id).await?;
        let mut conn = conn.lock().await;
        let outcome = async {
            if !conn.is_connected() {
                conn.connect().await?;
            }
            conn.send(&msg).await?;
            conn.recv().await
        }
        .await;
        let latency = start.elapsed();
        drop(conn);

        match outcome {
            Ok(ReplicationMessage::AppendEntriesResponse(resp)) => {
                self.record_peer_latency(peer_id, latency).await;
                Ok((resp, latency))
            },
            Ok(_) => {
                self.record_peer_latency(peer_id, Self::RPC_FAILURE_PENALTY_LATENCY)
                    .await;
                let conn = self.get_peer_connection(peer_id).await?;
                conn.lock().await.disconnect();
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected AppendEntriesResponse",
                ))
            },
            Err(e) => {
                self.record_peer_latency(peer_id, Self::RPC_FAILURE_PENALTY_LATENCY)
                    .await;
                Err(e)
            },
        }
    }

    /// Send data replication bytes directly to a peer (fire-and-forget, no response expected).
    ///
    /// Used for L3 quorum data replication where the forwarder tracks successful peers
    /// via the return value, not via a response message.
    pub async fn send_data_replication(
        &self,
        peer_id: u16,
        data: bytes::Bytes,
    ) -> Result<(), std::io::Error> {
        let conn = self.get_peer_connection(peer_id).await?;
        let mut conn = conn.lock().await;

        if !conn.is_connected() {
            conn.connect().await?;
        }

        // Write the length prefix + data directly
        // Write the length prefix + data directly
        let len_bytes = (data.len() as u32).to_le_bytes();
        let stream = conn.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Stream not connected")
        })?;
        stream.write_all(&len_bytes).await?;
        stream.write_all(&data).await?;
        stream.flush().await?;

        Ok(())
    }

    pub async fn connected_peer_count(&self) -> usize {
        let peers: Vec<Arc<Mutex<PeerConnection>>> = {
            let peers = self.peers.read().await;
            peers.values().cloned().collect()
        };

        let mut connected = 0usize;
        for conn in peers {
            if conn.lock().await.is_connected() {
                connected += 1;
            }
        }
        connected
    }

    pub async fn peer_states(&self) -> HashMap<u16, PeerState> {
        let peers: Vec<(u16, Arc<Mutex<PeerConnection>>)> = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .map(|(peer_id, conn)| (*peer_id, Arc::clone(conn)))
                .collect()
        };

        let mut states = HashMap::with_capacity(peers.len());
        for (peer_id, conn) in peers {
            states.insert(peer_id, conn.lock().await.state);
        }
        states
    }

    /// Get the address of a specific peer by node ID
    pub async fn get_peer_addr(&self, peer_id: u16) -> Option<SocketAddr> {
        let conn = {
            let peers = self.peers.read().await;
            peers.get(&peer_id).cloned()
        }?;
        Some(conn.lock().await.addr)
    }

    /// Get all known peer IDs
    pub async fn peer_ids(&self) -> Vec<u16> {
        let peers = self.peers.read().await;
        let health = self.health.read().await;

        let mut ids = Vec::with_capacity(peers.len());
        for peer_id in peers.keys() {
            let evicted = health
                .get(peer_id)
                .map(|follower_health| follower_health.is_evicted())
                .unwrap_or(false);
            if !evicted {
                ids.push(*peer_id);
            }
        }

        // Safety fallback: if all peers are currently evicted, return the full set
        // so replication can still probe for recovery.
        if ids.is_empty() {
            return peers.keys().copied().collect();
        }

        ids
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_config_default() {
        let config = PeerConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(3));
        assert_eq!(config.read_timeout, Duration::from_secs(10));
        assert_eq!(config.write_timeout, Duration::from_secs(2));
        assert_eq!(config.reconnect_delay, Duration::from_millis(250));
        assert_eq!(config.max_reconnect_attempts, 10);
    }

    #[test]
    fn test_peer_connection_new() {
        let addr: SocketAddr = "127.0.0.1:1992".parse().unwrap();
        let conn = PeerConnection::new(1, addr, PeerConfig::default());
        assert_eq!(conn.node_id, 1);
        assert_eq!(conn.state, PeerState::Disconnected);
        assert!(!conn.is_connected());
    }

    #[tokio::test]
    async fn test_peer_manager_add_remove() {
        let manager = PeerManager::new(0, PeerConfig::default());
        let addr: SocketAddr = "127.0.0.1:1993".parse().unwrap();

        manager.add_peer(1, addr).await;
        assert_eq!(manager.peers.read().await.len(), 1);

        manager.remove_peer(1).await;
        assert_eq!(manager.peers.read().await.len(), 0);
    }

    #[tokio::test]
    async fn test_peer_manager_health_tracking() {
        let manager = PeerManager::new(0, PeerConfig::default());
        let addr: SocketAddr = "127.0.0.1:1994".parse().unwrap();

        manager.add_peer(1, addr).await;

        // Health tracking should be initialized
        assert_eq!(manager.health.read().await.len(), 1);

        // Initial status should be healthy
        let status = manager.get_peer_health(1).await;
        assert_eq!(status, Some(FollowerStatus::Healthy));

        // Record fast latencies - should stay healthy
        for _ in 0..5 {
            manager
                .record_peer_latency(1, Duration::from_millis(5))
                .await;
        }
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Healthy)
        );

        // Record slow latencies - should become degraded then evicted
        manager
            .record_peer_latency(1, Duration::from_millis(300))
            .await;
        manager
            .record_peer_latency(1, Duration::from_millis(300))
            .await;
        // After 2 slow samples, should be degraded (not yet evicted)
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Degraded)
        );

        for _ in 0..3 {
            manager
                .record_peer_latency(1, Duration::from_millis(300))
                .await;
        }
        // After 5 slow samples, should be evicted
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Evicted)
        );
    }

    #[tokio::test]
    async fn test_peer_manager_healthy_peer_count() {
        let manager = PeerManager::new(0, PeerConfig::default());

        manager.add_peer(1, "127.0.0.1:1995".parse().unwrap()).await;
        manager.add_peer(2, "127.0.0.1:1996".parse().unwrap()).await;
        manager.add_peer(3, "127.0.0.1:1997".parse().unwrap()).await;

        // All should be healthy initially
        assert_eq!(manager.healthy_peer_count().await, 3);

        // Evict peer 1
        for _ in 0..5 {
            manager
                .record_peer_latency(1, Duration::from_millis(300))
                .await;
        }

        // Now only 2 healthy
        assert_eq!(manager.healthy_peer_count().await, 2);

        let healthy_ids = manager.healthy_peer_ids().await;
        assert!(healthy_ids.contains(&2));
        assert!(healthy_ids.contains(&3));
        assert!(!healthy_ids.contains(&1));
    }

    #[tokio::test]
    async fn test_peer_manager_health_recovery() {
        let manager = PeerManager::new(0, PeerConfig::default());
        manager.add_peer(1, "127.0.0.1:1998".parse().unwrap()).await;

        // Evict peer
        for _ in 0..5 {
            manager
                .record_peer_latency(1, Duration::from_millis(300))
                .await;
        }
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Evicted)
        );

        // Recover with fast latencies (need recovery_window consecutive fast responses)
        for _ in 0..5 {
            manager
                .record_peer_latency(1, Duration::from_millis(5))
                .await;
        }
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Healthy)
        );
    }
}
