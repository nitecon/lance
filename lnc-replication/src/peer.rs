//! Peer connection management for replication networking.
//!
//! Handles TCP connections to peer nodes for Raft consensus and data replication.
//! Supports optional TLS encryption when the `tls` feature is enabled.

use crate::codec::{AppendEntriesRequest, AppendEntriesResponse};
use crate::codec::{PreVoteRequest, PreVoteResponse, VoteRequest, VoteResponse};
use crate::codec::{ReplicationCodec, ReplicationMessage};
use crate::follower::{FollowerHealth, FollowerStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
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
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            reconnect_delay: Duration::from_secs(1),
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

    pub async fn send(&mut self, msg: &ReplicationMessage) -> Result<(), std::io::Error> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
        })?;

        let mut codec = ReplicationCodec::new();
        let encoded = codec
            .encode(msg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        // Write length prefix + message
        let len = encoded.len() as u32;
        let mut frame = Vec::with_capacity(4 + encoded.len());
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&encoded);

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

        // Read length prefix (4 bytes)
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

        let length = u32::from_le_bytes(len_buf) as usize;

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

        ReplicationCodec::decode(&payload)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
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
    peers: Arc<RwLock<HashMap<u16, PeerConnection>>>,
    health: Arc<RwLock<HashMap<u16, FollowerHealth>>>,
    config: PeerConfig,
}

impl PeerManager {
    pub fn new(_node_id: u16, config: PeerConfig) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            health: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub async fn add_peer(&self, peer_id: u16, addr: SocketAddr) {
        let mut peers = self.peers.write().await;
        match peers.entry(peer_id) {
            std::collections::hash_map::Entry::Vacant(e) => {
                let conn = PeerConnection::new(peer_id, addr, self.config.clone());
                e.insert(conn);

                // Initialize health tracking for this peer
                let mut health = self.health.write().await;
                health.insert(peer_id, FollowerHealth::new(peer_id));

                info!(
                    target: "lance::replication",
                    peer_id,
                    addr = %addr,
                    "Added peer with health tracking"
                );
            },
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let existing = e.get();
                if existing.addr != addr {
                    info!(
                        target: "lance::replication",
                        peer_id,
                        old_addr = %existing.addr,
                        new_addr = %addr,
                        "Peer address changed, updating connection"
                    );
                    let mut conn = PeerConnection::new(peer_id, addr, self.config.clone());
                    // Preserve disconnect state so reconnect logic kicks in
                    std::mem::swap(e.get_mut(), &mut conn);
                    // Old connection is dropped here
                }
            },
        }
    }

    pub async fn remove_peer(&self, peer_id: u16) {
        let mut peers = self.peers.write().await;
        if let Some(mut conn) = peers.remove(&peer_id) {
            conn.disconnect();

            // Remove health tracking
            let mut health = self.health.write().await;
            health.remove(&peer_id);

            info!(
                target: "lance::replication",
                peer_id,
                "Removed peer"
            );
        }
    }

    pub async fn connect_all(&self) {
        let mut peers = self.peers.write().await;
        for (peer_id, conn) in peers.iter_mut() {
            if !conn.is_connected() && conn.should_reconnect() {
                if let Err(e) = conn.connect().await {
                    warn!(
                        target: "lance::replication",
                        peer_id = *peer_id,
                        error = %e,
                        "Failed to connect to peer"
                    );
                }
            }
        }
    }

    pub async fn disconnect_all(&self) {
        let mut peers = self.peers.write().await;
        for (peer_id, conn) in peers.iter_mut() {
            if conn.is_connected() {
                conn.disconnect();
                info!(
                    target: "lance::replication",
                    peer_id = *peer_id,
                    "Disconnected peer during shutdown"
                );
            }
        }
    }

    pub async fn send_to_peer(
        &self,
        peer_id: u16,
        msg: &ReplicationMessage,
    ) -> Result<(), std::io::Error> {
        let mut peers = self.peers.write().await;
        let conn = peers
            .get_mut(&peer_id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))?;

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

        self.send_to_peer(peer_id, &msg).await?;

        // Receive response
        let mut peers = self.peers.write().await;
        let conn = peers
            .get_mut(&peer_id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))?;

        let response_msg = conn.recv().await?;
        let latency = start.elapsed();

        // Record latency for health tracking
        drop(peers); // Release peers lock before acquiring health lock
        self.record_peer_latency(peer_id, latency).await;

        match response_msg {
            ReplicationMessage::AppendEntriesResponse(resp) => Ok(resp),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected AppendEntriesResponse",
            )),
        }
    }

    /// Send a PreVoteRequest to a peer and wait for the PreVoteResponse.
    pub async fn send_pre_vote_request(
        &self,
        peer_id: u16,
        request: PreVoteRequest,
    ) -> Result<PreVoteResponse, std::io::Error> {
        let msg = ReplicationMessage::PreVoteRequest(request);
        self.send_to_peer(peer_id, &msg).await?;

        let mut peers = self.peers.write().await;
        let conn = peers
            .get_mut(&peer_id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))?;

        let response_msg = conn.recv().await?;

        match response_msg {
            ReplicationMessage::PreVoteResponse(resp) => Ok(resp),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected PreVoteResponse",
            )),
        }
    }

    /// Send a VoteRequest to a peer and wait for the VoteResponse.
    pub async fn send_vote_request(
        &self,
        peer_id: u16,
        request: VoteRequest,
    ) -> Result<VoteResponse, std::io::Error> {
        let msg = ReplicationMessage::VoteRequest(request);
        self.send_to_peer(peer_id, &msg).await?;

        let mut peers = self.peers.write().await;
        let conn = peers
            .get_mut(&peer_id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found"))?;

        let response_msg = conn.recv().await?;

        match response_msg {
            ReplicationMessage::VoteResponse(resp) => Ok(resp),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected VoteResponse",
            )),
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

    pub async fn broadcast(
        &self,
        msg: &ReplicationMessage,
    ) -> Vec<(u16, Result<(), std::io::Error>)> {
        let peer_ids: Vec<u16> = {
            let peers = self.peers.read().await;
            peers.keys().copied().collect()
        };

        let mut results = Vec::new();
        for peer_id in peer_ids {
            let result = self.send_to_peer(peer_id, msg).await;
            results.push((peer_id, result));
        }
        results
    }

    pub async fn connected_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        peers.values().filter(|c| c.is_connected()).count()
    }

    pub async fn peer_states(&self) -> HashMap<u16, PeerState> {
        let peers = self.peers.read().await;
        peers.iter().map(|(id, c)| (*id, c.state)).collect()
    }

    /// Get the address of a specific peer by node ID
    pub async fn get_peer_addr(&self, peer_id: u16) -> Option<SocketAddr> {
        let peers = self.peers.read().await;
        peers.get(&peer_id).map(|c| c.addr)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_config_default() {
        let config = PeerConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
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
            .record_peer_latency(1, Duration::from_millis(20))
            .await;
        manager
            .record_peer_latency(1, Duration::from_millis(20))
            .await;
        // After 2 slow, should be degraded
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Degraded)
        );

        manager
            .record_peer_latency(1, Duration::from_millis(20))
            .await;
        // After 3 slow, should be evicted
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
        for _ in 0..3 {
            manager
                .record_peer_latency(1, Duration::from_millis(20))
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
        for _ in 0..3 {
            manager
                .record_peer_latency(1, Duration::from_millis(20))
                .await;
        }
        assert_eq!(
            manager.get_peer_health(1).await,
            Some(FollowerStatus::Evicted)
        );

        // Recover with fast latencies (need recovery_window consecutive fast responses)
        for _ in 0..10 {
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
