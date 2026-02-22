//! Data Plane Replication Module
//!
//! This module implements the **data plane** replication layer, completely
//! separate from the control plane (Raft). The data plane handles actual
//! data replication from leader to followers with quorum tracking.
//!
//! # Architecture
//!
//! The data plane is designed to function independently from the control plane:
//! - Data plane connections are separate from Raft peer connections
//! - Data replication does not block on Raft consensus
//! - Works behind load balancers and Kubernetes services
//! - Provides follower-to-leader ACK path for quorum durability
//!
//! # Key Components
//!
//! - `DataPlaneManager`: Manages data plane connections to followers
//! - `DataReplicationStream`: Handles replication to a single follower
//! - `FollowerAckHandler`: Processes ACKs from followers back to leader
//!
//! See `docs/Architecture.md` section "Control Plane vs Data Plane Architecture"
//! for detailed explanation.

use crate::codec::DataReplicationEntry;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

/// Configuration for data plane connections
#[derive(Debug, Clone)]
pub struct DataPlaneConfig {
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub ack_timeout: Duration,
    pub reconnect_delay: Duration,
    pub max_reconnect_attempts: u32,
}

impl Default for DataPlaneConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            write_timeout: Duration::from_secs(5),
            ack_timeout: Duration::from_secs(5),
            reconnect_delay: Duration::from_millis(250),
            max_reconnect_attempts: 10,
        }
    }
}

/// State of a data plane connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPlaneState {
    Disconnected,
    Connecting,
    Connected,
    Failed,
}

/// Connection to a follower for data plane replication
pub struct DataPlaneConnection {
    pub node_id: u16,
    pub addr: SocketAddr,
    pub state: DataPlaneState,
    stream: Option<TcpStream>,
    config: DataPlaneConfig,
    reconnect_attempts: u32,
}

impl DataPlaneConnection {
    pub fn new(node_id: u16, addr: SocketAddr, config: DataPlaneConfig) -> Self {
        Self {
            node_id,
            addr,
            state: DataPlaneState::Disconnected,
            stream: None,
            config,
            reconnect_attempts: 0,
        }
    }

    pub async fn connect(&mut self) -> Result<(), std::io::Error> {
        self.state = DataPlaneState::Connecting;

        match tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(self.addr)).await
        {
            Ok(Ok(stream)) => {
                stream.set_nodelay(true)?;
                self.stream = Some(stream);
                self.state = DataPlaneState::Connected;
                self.reconnect_attempts = 0;
                info!(
                    target: "lance::data_plane",
                    node_id = self.node_id,
                    addr = %self.addr,
                    "Data plane connected to follower"
                );
                Ok(())
            },
            Ok(Err(e)) => {
                self.state = DataPlaneState::Failed;
                self.reconnect_attempts += 1;
                warn!(
                    target: "lance::data_plane",
                    node_id = self.node_id,
                    addr = %self.addr,
                    error = %e,
                    attempts = self.reconnect_attempts,
                    "Failed to connect data plane to follower"
                );
                Err(e)
            },
            Err(_) => {
                self.state = DataPlaneState::Failed;
                self.reconnect_attempts += 1;
                warn!(
                    target: "lance::data_plane",
                    node_id = self.node_id,
                    addr = %self.addr,
                    attempts = self.reconnect_attempts,
                    "Data plane connection timeout"
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Data plane connection timeout",
                ))
            },
        }
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, DataPlaneState::Connected)
    }

    pub async fn disconnect(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.shutdown().await;
        }
        self.state = DataPlaneState::Disconnected;
    }

    /// Send data replication entry and wait for ACK
    pub async fn replicate_with_ack(
        &mut self,
        entry: &DataReplicationEntry,
    ) -> Result<(), std::io::Error> {
        info!(target: "lance::data_plane", node_id = self.node_id, "Starting replicate_with_ack");
        if !self.is_connected() {
            info!(target: "lance::data_plane", node_id = self.node_id, "Not connected, calling connect()");
            self.connect().await?;
        }

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Stream not connected")
        })?;

        // Encode entry
        let data = entry.encode();
        let len_bytes = (data.len() as u32).to_le_bytes();
        info!(target: "lance::data_plane", node_id = self.node_id, data_len = data.len(), "Sending data to follower");

        // Send length + data with timeout
        tokio::time::timeout(self.config.write_timeout, async {
            stream.write_all(&len_bytes).await?;
            stream.write_all(&data).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Write timeout"))??;
        info!(target: "lance::data_plane", node_id = self.node_id, "Data sent, waiting for ACK");

        // Wait for ACK (1 byte: 0 = success, 1 = failure)
        let mut ack_buf = [0u8; 1];
        let read_result =
            tokio::time::timeout(self.config.ack_timeout, stream.read_exact(&mut ack_buf))
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "ACK timeout"))?;

        match read_result {
            Ok(_) => {
                info!(target: "lance::data_plane", node_id = self.node_id, ack_byte = ack_buf[0], "ACK received");
                if ack_buf[0] == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Follower rejected data",
                    ))
                }
            },
            Err(e) => {
                warn!(target: "lance::data_plane", node_id = self.node_id, error = %e, "Failed to read ACK");
                // Connection broken, mark as disconnected
                self.state = DataPlaneState::Disconnected;
                Err(e)
            },
        }
    }
}

/// Manages data plane connections to all followers
pub struct DataPlaneManager {
    #[allow(dead_code)]
    node_id: u16,
    connections: Arc<RwLock<HashMap<u16, Arc<Mutex<DataPlaneConnection>>>>>,
    config: DataPlaneConfig,
}

impl DataPlaneManager {
    pub fn new(node_id: u16, config: DataPlaneConfig) -> Self {
        Self {
            node_id,
            connections: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Add a follower to the data plane
    pub async fn add_follower(&self, node_id: u16, addr: SocketAddr) {
        let mut connections = self.connections.write().await;
        let conn = DataPlaneConnection::new(node_id, addr, self.config.clone());
        connections.insert(node_id, Arc::new(Mutex::new(conn)));
        info!(
            target: "lance::data_plane",
            node_id,
            addr = %addr,
            "Added follower to data plane"
        );
    }

    /// Remove a follower from the data plane
    pub async fn remove_follower(&self, node_id: u16) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.remove(&node_id) {
            conn.lock().await.disconnect().await;
            info!(
                target: "lance::data_plane",
                node_id,
                "Removed follower from data plane"
            );
        }
    }

    /// Replicate data to a specific follower
    pub async fn replicate_to_follower(
        &self,
        node_id: u16,
        entry: &DataReplicationEntry,
    ) -> Result<(), std::io::Error> {
        info!(target: "lance::data_plane", node_id, "Starting replication to follower");
        let conn = {
            let connections = self.connections.read().await;
            connections.get(&node_id).cloned().ok_or_else(|| {
                warn!(target: "lance::data_plane", node_id, "Follower not found in data plane");
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Follower {} not in data plane", node_id),
                )
            })?
        };

        let mut conn = conn.lock().await;
        let result = conn.replicate_with_ack(entry).await;
        match &result {
            Ok(_) => {
                info!(target: "lance::data_plane", node_id, "Replication to follower succeeded")
            },
            Err(e) => {
                warn!(target: "lance::data_plane", node_id, error = %e, "Replication to follower failed")
            },
        }
        result
    }

    /// Get all follower node IDs
    pub async fn follower_ids(&self) -> Vec<u16> {
        let connections = self.connections.read().await;
        connections.keys().copied().collect()
    }

    /// Get connection count
    pub async fn connection_count(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }
}

/// Handler for follower-to-leader ACKs on the server side
/// (follower receives data and sends ACK back)
pub struct FollowerAckHandler;

impl FollowerAckHandler {
    /// Handle incoming data replication request as a follower
    /// Returns true if successfully written locally
    pub async fn handle_replication_request(
        stream: &mut TcpStream,
        write_callback: impl FnOnce(&DataReplicationEntry) -> Result<(), std::io::Error>,
    ) -> Result<(), std::io::Error> {
        info!(target: "lance::data_plane", "Waiting to read replication request length");

        // Read length
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let data_len = u32::from_le_bytes(len_buf) as usize;
        info!(target: "lance::data_plane", data_len = data_len, "Reading replication data");

        // Read data
        let mut data_buf = vec![0u8; data_len];
        stream.read_exact(&mut data_buf).await?;
        info!(target: "lance::data_plane", bytes_read = data_len, "Replication data received");

        // Decode entry
        let entry = DataReplicationEntry::decode(&data_buf).ok_or_else(|| {
            error!(target: "lance::data_plane", "Failed to decode DataReplicationEntry");
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to decode entry")
        })?;

        let topic_id = entry.topic_id;
        let global_offset = entry.global_offset;
        info!(target: "lance::data_plane", topic_id = topic_id, global_offset = global_offset, "Decoded replication entry");

        // Write locally
        info!(target: "lance::data_plane", topic_id = topic_id, global_offset = global_offset, "Calling write callback");
        match write_callback(&entry) {
            Ok(()) => {
                info!(target: "lance::data_plane", topic_id = topic_id, global_offset = global_offset, "Write succeeded, sending ACK");
                // Send success ACK
                stream.write_all(&[0u8]).await?;
                stream.flush().await?;
                info!(target: "lance::data_plane", topic_id = topic_id, global_offset = global_offset, "ACK sent successfully");
                Ok(())
            },
            Err(e) => {
                error!(target: "lance::data_plane", topic_id = topic_id, global_offset = global_offset, error = %e, "Write failed, sending NACK");
                // Send failure ACK
                let _ = stream.write_all(&[1u8]).await;
                let _ = stream.flush().await;
                Err(e)
            },
        }
    }
}
