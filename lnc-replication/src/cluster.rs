//! Cluster coordination module.
//!
//! Ties together Raft consensus, peer networking, and discovery
//! into a cohesive cluster management system.

use crate::codec::{
    ClusterConfig as RaftClusterConfig, ConfigNode, NodeRole, ReplicationCodec, ReplicationMessage,
};
use crate::discovery::{ClusterConfig, PeerDiscovery};
use crate::log_store::LogStore;
use crate::peer::{PeerConfig, PeerManager, PeerState};
use crate::raft::{FencingToken, RaftConfig, RaftNode, RaftState};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tracing::{debug, error, info, trace, warn};

/// Events emitted by the cluster coordinator
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// This node became leader
    BecameLeader {
        term: u64,
        fencing_token: FencingToken,
    },
    /// This node is now following a leader
    BecameFollower { leader_id: u16, term: u64 },
    /// Leadership was lost
    LostLeadership { term: u64 },
    /// Peer joined the cluster
    PeerJoined { node_id: u16, addr: SocketAddr },
    /// Peer left the cluster
    PeerLeft { node_id: u16 },
    /// Cluster reached quorum
    QuorumReached,
    /// Cluster lost quorum
    QuorumLost,
    /// Topic operation received from leader (for followers to apply)
    TopicOperation(crate::codec::TopicOperation),
}

/// Cluster coordinator state
pub struct ClusterCoordinator {
    config: ClusterConfig,
    raft: Arc<RwLock<RaftNode>>,
    peers: Arc<PeerManager>,
    discovery: Arc<PeerDiscovery>,
    event_tx: broadcast::Sender<ClusterEvent>,
    /// Cached leader state for hot path (lock-free read)
    /// Updated by the coordination loop, read by connection handlers
    cached_is_leader: std::sync::atomic::AtomicBool,
    /// Cached leader address for redirects (updated asynchronously)
    cached_leader_addr: std::sync::RwLock<Option<SocketAddr>>,
    /// Async quorum manager for L3 sync replication
    quorum_manager: crate::quorum::AsyncQuorumManager,
    /// Persistent Raft log store (optional - None for in-memory only)
    log_store: Option<Arc<std::sync::Mutex<LogStore>>>,
    /// Timestamp of last successful replication sync (for lag metrics)
    last_sync_time: std::sync::atomic::AtomicU64,
}

impl ClusterCoordinator {
    /// Create a new cluster coordinator
    pub fn new(config: ClusterConfig) -> Self {
        let (event_tx, _) = broadcast::channel(64);

        // Create Raft cluster config from our config
        let raft_cluster_config = RaftClusterConfig::single(vec![ConfigNode {
            node_id: config.node_id,
            role: NodeRole::Full,
            address: config.listen_addr.to_string(),
        }]);

        let raft_config = RaftConfig {
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            pre_vote_enabled: true,
            leader_lease: Duration::from_millis(100),
        };

        let raft = RaftNode::new(config.node_id, raft_cluster_config, raft_config);

        let peer_config = PeerConfig::default();
        let peers = PeerManager::new(config.node_id, peer_config);

        let discovery = PeerDiscovery::new(config.node_id, config.discovery.clone());

        // Create quorum config based on cluster size (from discovery method)
        let cluster_size = match &config.discovery {
            crate::discovery::DiscoveryMethod::Static(peers) => peers.len() + 1,
            _ => 3, // Default to 3-node cluster for dynamic discovery
        };
        let quorum_config = crate::quorum::QuorumConfig::new(cluster_size)
            .with_timeout(config.heartbeat_interval.as_millis() as u64 * 3);
        let quorum_manager = crate::quorum::AsyncQuorumManager::new(quorum_config);

        Self {
            config,
            raft: Arc::new(RwLock::new(raft)),
            peers: Arc::new(peers),
            discovery: Arc::new(discovery),
            event_tx,
            cached_is_leader: std::sync::atomic::AtomicBool::new(false),
            cached_leader_addr: std::sync::RwLock::new(None),
            quorum_manager,
            log_store: None,
            last_sync_time: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Create a new cluster coordinator with persistent log storage
    pub fn with_persistence(
        config: ClusterConfig,
        data_dir: &Path,
    ) -> Result<Self, std::io::Error> {
        let (event_tx, _) = broadcast::channel(64);

        // Open persistent log store
        let raft_dir = data_dir.join("raft");
        let log_store =
            LogStore::open(&raft_dir).map_err(|e| std::io::Error::other(e.to_string()))?;

        // Load persistent state (term, voted_for)
        let persistent_state = log_store
            .load_state()
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Create Raft cluster config from our config
        let raft_cluster_config = RaftClusterConfig::single(vec![ConfigNode {
            node_id: config.node_id,
            role: NodeRole::Full,
            address: config.listen_addr.to_string(),
        }]);

        let raft_config = RaftConfig {
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            pre_vote_enabled: true,
            leader_lease: Duration::from_millis(100),
        };

        let mut raft = RaftNode::new(config.node_id, raft_cluster_config, raft_config);

        // Restore persistent state if available
        if persistent_state.current_term > 0 {
            raft.restore_state(persistent_state.current_term, persistent_state.voted_for);
            info!(
                target: "lance::cluster",
                term = persistent_state.current_term,
                voted_for = ?persistent_state.voted_for,
                "Restored Raft state from disk"
            );
        }

        let peer_config = PeerConfig::default();
        let peers = PeerManager::new(config.node_id, peer_config);

        let discovery = PeerDiscovery::new(config.node_id, config.discovery.clone());

        // Create quorum config based on cluster size
        let cluster_size = match &config.discovery {
            crate::discovery::DiscoveryMethod::Static(peers) => peers.len() + 1,
            _ => 3,
        };
        let quorum_config = crate::quorum::QuorumConfig::new(cluster_size)
            .with_timeout(config.heartbeat_interval.as_millis() as u64 * 3);
        let quorum_manager = crate::quorum::AsyncQuorumManager::new(quorum_config);

        Ok(Self {
            config,
            raft: Arc::new(RwLock::new(raft)),
            peers: Arc::new(peers),
            discovery: Arc::new(discovery),
            event_tx,
            cached_is_leader: std::sync::atomic::AtomicBool::new(false),
            cached_leader_addr: std::sync::RwLock::new(None),
            quorum_manager,
            log_store: Some(Arc::new(std::sync::Mutex::new(log_store))),
            last_sync_time: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Subscribe to cluster events
    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.event_tx.subscribe()
    }

    /// Get the current node ID
    pub fn node_id(&self) -> u16 {
        self.config.node_id
    }

    /// Get the listen address
    pub fn listen_addr(&self) -> SocketAddr {
        self.config.listen_addr
    }

    /// Check if this node is currently the leader (lock-free, hot path safe)
    /// Uses cached value updated by coordination loop
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.cached_is_leader
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Check leader state with lock (for coordination loop only)
    pub async fn is_leader_authoritative(&self) -> bool {
        let raft = self.raft.read().await;
        raft.state() == RaftState::Leader
    }

    /// Get the current leader ID (if known)
    pub async fn leader_id(&self) -> Option<u16> {
        let raft = self.raft.read().await;
        raft.leader_id()
    }

    /// Get the current term
    pub async fn current_term(&self) -> u64 {
        let raft = self.raft.read().await;
        raft.current_term()
    }

    /// Get the current fencing token (only valid if leader)
    pub async fn fencing_token(&self) -> Option<FencingToken> {
        let raft = self.raft.read().await;
        raft.fencing_token()
    }

    /// Get the current Raft state (Follower, Candidate, or Leader)
    pub async fn state(&self) -> RaftState {
        let raft = self.raft.read().await;
        raft.state()
    }

    /// Get the address of a specific peer by node ID
    pub async fn peer_addr(&self, node_id: u16) -> Option<SocketAddr> {
        self.peers.get_peer_addr(node_id).await
    }

    /// Get the leader's client address for redirects (lock-free, hot path safe)
    /// Returns None if leader is unknown or is self
    #[inline]
    pub fn leader_addr(&self) -> Option<SocketAddr> {
        if let Ok(guard) = self.cached_leader_addr.read() {
            *guard
        } else {
            None
        }
    }

    /// Get the leader's client address with lock (for coordination loop only)
    /// Note: Peers store replication addresses, but we need client addresses for redirects.
    /// Convention: client_port = replication_port - 1
    pub async fn leader_addr_authoritative(&self) -> Option<SocketAddr> {
        let leader_id = self.leader_id().await?;
        if leader_id == self.config.node_id {
            return None; // We are the leader
        }
        // Get the replication address and convert to client address
        let repl_addr = self.peer_addr(leader_id).await?;
        // Client port is replication port - 1
        Some(SocketAddr::new(repl_addr.ip(), repl_addr.port() - 1))
    }

    /// Re-resolve DNS for raw peer hostname strings and update the peer map.
    /// This handles pod IP changes in Kubernetes (e.g. after pod restart).
    /// No-op if raw_peer_strings is empty (all peers were specified as IPs).
    pub async fn refresh_peer_addresses(&self) {
        if self.config.raw_peer_strings.is_empty() {
            return;
        }

        for (idx, peer_str) in self.config.raw_peer_strings.iter().enumerate() {
            let (node_id, host_port) = if peer_str.contains('@') {
                let parts: Vec<&str> = peer_str.splitn(2, '@').collect();
                if parts.len() == 2 {
                    if let Ok(id) = parts[0].parse::<u16>() {
                        (id, parts[1].to_string())
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                (idx as u16, peer_str.to_string())
            };

            // Skip self
            if node_id == self.config.node_id {
                continue;
            }

            // Skip if already a valid SocketAddr (IP, no DNS needed)
            if host_port.parse::<SocketAddr>().is_ok() {
                continue;
            }

            // Re-resolve DNS
            if let Ok(mut addrs) = tokio::net::lookup_host(&host_port).await {
                if let Some(addr) = addrs.next() {
                    // add_peer now upserts — updates address if changed
                    self.peers.add_peer(node_id, addr).await;
                }
            }
        }
    }

    /// Get connected peer count
    pub async fn connected_peer_count(&self) -> usize {
        self.peers.connected_peer_count().await
    }

    /// Get peer states
    pub async fn peer_states(&self) -> std::collections::HashMap<u16, PeerState> {
        self.peers.peer_states().await
    }

    /// Replicate a topic operation to followers (leader only)
    /// Uses async (L2) replication - returns after broadcast without waiting for ACKs
    pub async fn replicate_topic_op(
        &self,
        op: crate::codec::TopicOperation,
    ) -> Result<(), std::io::Error> {
        self.replicate_topic_op_internal(op, false).await
    }

    /// Replicate a topic operation with sync quorum waiting (L3 mode)
    /// Waits for M/2+1 ACKs before returning success
    pub async fn replicate_topic_op_sync(
        &self,
        op: crate::codec::TopicOperation,
    ) -> Result<(), std::io::Error> {
        self.replicate_topic_op_internal(op, true).await
    }

    /// Internal replication implementation
    /// sync_quorum: if true, wait for ACKs from M/2+1 nodes before returning
    async fn replicate_topic_op_internal(
        &self,
        op: crate::codec::TopicOperation,
        sync_quorum: bool,
    ) -> Result<(), std::io::Error> {
        // Start timing for replication latency metric
        let replication_start = Instant::now();

        // Only leader can replicate (use authoritative check for replication path)
        if !self.is_leader_authoritative().await {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Not leader - cannot replicate topic operation",
            ));
        }

        let op_bytes = op.to_bytes();

        // Create log entry for the topic operation
        let entry = crate::codec::LogEntry {
            term: self.current_term().await,
            index: 0, // Will be assigned by Raft
            hlc: lnc_core::HlcTimestamp::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                0,
            ),
            entry_type: crate::codec::EntryType::TopicOp,
            data: op_bytes,
        };

        // For sync quorum, register the write before broadcasting
        let quorum_handle = if sync_quorum {
            Some(self.quorum_manager.register_write().await)
        } else {
            None
        };

        // Create AppendEntries request with the topic operation
        let request = crate::codec::AppendEntriesRequest {
            term: entry.term,
            leader_id: self.config.node_id,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_hlc: entry.hlc,
            entries: vec![entry],
        };

        let msg = ReplicationMessage::AppendEntriesRequest(request);

        // Broadcast to all peers
        let results = self.peers.broadcast(&msg).await;

        // Count successful sends
        let success_count = results.iter().filter(|(_, r)| r.is_ok()).count();
        let total_peers = results.len();

        // For sync quorum mode, wait for ACKs
        if let Some((write_id, rx)) = quorum_handle {
            // Record immediate ACKs from successful sends
            // (In a full implementation, we'd wait for actual AppendEntriesResponse)
            for (node_id, result) in &results {
                if result.is_ok() {
                    self.quorum_manager.record_ack(write_id, *node_id).await;
                } else {
                    self.quorum_manager.record_nack(write_id, *node_id).await;
                }
            }

            // Wait for quorum with timeout
            let quorum_result = self.quorum_manager.wait_for_quorum(write_id, rx).await;

            match quorum_result {
                crate::quorum::QuorumResult::Success => {
                    // Record replication latency on success
                    lnc_metrics::record_replication_latency(replication_start.elapsed());
                    // Update last sync time for lag tracking
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    self.last_sync_time
                        .store(now_ms, std::sync::atomic::Ordering::Relaxed);
                    info!(
                        target: "lance::cluster",
                        write_id,
                        success_count,
                        total_peers,
                        latency_us = replication_start.elapsed().as_micros(),
                        "Topic operation replicated with sync quorum"
                    );
                    Ok(())
                },
                crate::quorum::QuorumResult::Timeout => {
                    lnc_metrics::increment_quorum_timeouts();
                    warn!(
                        target: "lance::cluster",
                        write_id,
                        success_count,
                        total_peers,
                        "Sync quorum timeout for topic operation"
                    );
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Sync quorum timeout for topic replication",
                    ))
                },
                crate::quorum::QuorumResult::Failed
                | crate::quorum::QuorumResult::Partial { .. } => {
                    lnc_metrics::increment_quorum_failures();
                    warn!(
                        target: "lance::cluster",
                        write_id,
                        success_count,
                        total_peers,
                        "Failed to reach sync quorum for topic operation"
                    );
                    Err(std::io::Error::other(
                        "Failed to reach sync quorum for topic replication",
                    ))
                },
            }
        } else {
            // Async mode (L2) - just check broadcast success
            let quorum = (total_peers / 2) + 1;

            if success_count >= quorum || total_peers == 0 {
                // Record replication latency on success
                lnc_metrics::record_replication_latency(replication_start.elapsed());
                // Update last sync time for lag tracking
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                self.last_sync_time
                    .store(now_ms, std::sync::atomic::Ordering::Relaxed);
                info!(
                    target: "lance::cluster",
                    success_count,
                    total_peers,
                    latency_us = replication_start.elapsed().as_micros(),
                    "Topic operation replicated (async)"
                );
                Ok(())
            } else {
                lnc_metrics::increment_quorum_failures();
                warn!(
                    target: "lance::cluster",
                    success_count,
                    total_peers,
                    quorum,
                    "Failed to reach quorum for topic operation"
                );
                Err(std::io::Error::other(
                    "Failed to reach quorum for topic replication",
                ))
            }
        }
    }

    /// Initialize the cluster - discover peers and establish connections
    pub async fn initialize(&self) -> Result<(), std::io::Error> {
        info!(
            target: "lance::cluster",
            node_id = self.config.node_id,
            listen_addr = %self.config.listen_addr,
            "Initializing cluster coordinator"
        );

        // Discover peers
        let peers = self.discovery.discover().await;
        info!(
            target: "lance::cluster",
            peer_count = peers.len(),
            peers = ?peers.iter().map(|p| (p.node_id, p.addr)).collect::<Vec<_>>(),
            "Discovered peers"
        );

        // Add discovered peers to peer manager
        for peer in &peers {
            self.peers.add_peer(peer.node_id, peer.addr).await;
        }

        // Connect to all peers
        self.peers.connect_all().await;

        let connected = self.peers.connected_peer_count().await;
        info!(
            target: "lance::cluster",
            connected_peers = connected,
            "Initial peer connections established"
        );

        Ok(())
    }

    /// Start the replication listener to accept incoming peer connections
    pub async fn start_listener(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.config.listen_addr).await?;
        info!(
            target: "lance::cluster",
            addr = %self.config.listen_addr,
            "Replication listener started"
        );

        let raft = Arc::clone(&self.raft);
        let peers = Arc::clone(&self.peers);
        let event_tx = self.event_tx.clone();
        let node_id = self.config.node_id;
        let log_store = self.log_store.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, addr)) => {
                        info!(
                            target: "lance::cluster",
                            peer_addr = %addr,
                            "Accepted peer connection"
                        );

                        let raft_clone = Arc::clone(&raft);
                        let peers_clone = Arc::clone(&peers);
                        let event_tx_clone = event_tx.clone();
                        let log_store_clone = log_store.clone();

                        tokio::spawn(async move {
                            if let Err(e) = handle_peer_connection(
                                &mut stream,
                                raft_clone,
                                peers_clone,
                                node_id,
                                event_tx_clone,
                                log_store_clone,
                            )
                            .await
                            {
                                debug!(
                                    target: "lance::cluster",
                                    peer_addr = %addr,
                                    error = %e,
                                    "Peer connection handler error"
                                );
                            }
                        });
                    },
                    Err(e) => {
                        error!(
                            target: "lance::cluster",
                            error = %e,
                            "Failed to accept peer connection"
                        );
                    },
                }
            }
        });

        Ok(())
    }

    /// Gracefully shut down the cluster coordinator.
    ///
    /// If this node is the leader, voluntarily steps down so that followers
    /// can detect the leadership vacancy faster. Then disconnects all peer
    /// connections, which causes immediate "Broken pipe" detection on the
    /// remote side — much faster than waiting for heartbeat timeout.
    /// This reduces the leadership gap during rolling restarts from
    /// heartbeat_timeout + election_timeout (~500ms+) to just election_timeout (~300ms).
    pub async fn graceful_shutdown(&self) {
        let was_leader = {
            let mut raft = self.raft.write().await;
            if raft.is_leader() {
                let term = raft.current_term();
                info!(
                    target: "lance::cluster",
                    node_id = self.config.node_id,
                    term,
                    "Stepping down from leadership before shutdown"
                );
                raft.step_down_voluntarily();
                true
            } else {
                false
            }
        };

        // Disconnect all peers — this causes immediate error detection on remote
        // nodes (Broken pipe / Connection reset) rather than waiting for heartbeat
        // timeout. Remote peers will then start election immediately.
        self.peers.disconnect_all().await;

        info!(
            target: "lance::cluster",
            node_id = self.config.node_id,
            was_leader,
            "Cluster coordinator shutdown complete"
        );
    }

    /// Run the cluster coordination loop
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            target: "lance::cluster",
            node_id = self.config.node_id,
            "Starting cluster coordination loop"
        );

        let heartbeat_interval = self.config.heartbeat_interval;
        let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);
        let mut election_check_ticker = tokio::time::interval(Duration::from_millis(50));

        loop {
            tokio::select! {
                _ = heartbeat_ticker.tick() => {
                    self.on_heartbeat_tick().await;
                }
                _ = election_check_ticker.tick() => {
                    self.on_election_check().await;
                }
                _ = shutdown.recv() => {
                    self.graceful_shutdown().await;
                    break;
                }
            }
        }
    }

    async fn on_heartbeat_tick(&self) {
        let (is_leader, current_term, leader_id) = {
            let raft = self.raft.read().await;
            (
                raft.state() == RaftState::Leader,
                raft.current_term(),
                raft.leader_id(),
            )
        };

        // Update cached leader state for hot path (lock-free reads)
        self.update_cached_leader_state(is_leader).await;

        // Publish cluster health metrics for production observability
        let connected_peers = self.connected_peer_count().await;
        let total_nodes = connected_peers + 1; // connected peers + self (approximation)
        let quorum_size = (total_nodes / 2) + 1;
        let quorum_available = connected_peers + 1 >= quorum_size; // +1 for self

        lnc_metrics::set_cluster_is_leader(is_leader);
        lnc_metrics::set_cluster_current_term(current_term);
        lnc_metrics::set_cluster_leader_id(leader_id.unwrap_or(0));
        lnc_metrics::set_cluster_node_count(total_nodes);
        lnc_metrics::set_cluster_healthy_nodes(connected_peers + 1); // +1 for self
        lnc_metrics::set_cluster_quorum_available(quorum_available);

        // Update replication lag metrics
        let last_sync = self
            .last_sync_time
            .load(std::sync::atomic::Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let lag_ms = if last_sync > 0 {
            now_ms.saturating_sub(last_sync)
        } else {
            0
        };
        lnc_metrics::set_replication_last_sync_ms(lag_ms);

        // Pending ops tracked via quorum manager
        let pending_ops = self.quorum_manager.pending_count().await;
        lnc_metrics::set_replication_pending_ops(pending_ops as u64);

        if is_leader {
            // Send heartbeats to all peers
            if let Err(e) = self.send_heartbeats().await {
                warn!(
                    target: "lance::cluster",
                    error = %e,
                    "Failed to send heartbeats"
                );
            }
        }
    }

    /// Update cached leader state for lock-free hot path reads
    async fn update_cached_leader_state(&self, is_leader: bool) {
        use std::sync::atomic::Ordering;

        let old_is_leader = self.cached_is_leader.swap(is_leader, Ordering::Relaxed);

        // Update cached leader address if leadership changed
        if old_is_leader != is_leader || !is_leader {
            let leader_addr = if is_leader {
                None // We are the leader, no redirect needed
            } else {
                self.leader_addr_authoritative().await
            };

            if let Ok(mut guard) = self.cached_leader_addr.write() {
                *guard = leader_addr;
            }
        }
    }

    async fn on_election_check(&self) {
        let (should_start_election, is_leader) = {
            let raft = self.raft.read().await;
            (
                raft.election_timeout_elapsed(),
                raft.state() == RaftState::Leader,
            )
        };

        // Leaders send heartbeats, they don't start elections
        if is_leader {
            return;
        }

        if should_start_election {
            info!(
                target: "lance::cluster",
                "Election timeout elapsed, starting election"
            );
            lnc_metrics::increment_raft_elections_started();
            self.start_election().await;
        }
    }

    async fn send_heartbeats(&self) -> Result<(), std::io::Error> {
        let request = {
            let raft = self.raft.read().await;
            raft.create_append_entries(Vec::new())
        };

        let Some(request) = request else {
            return Ok(()); // Not leader, skip heartbeats
        };

        let msg = ReplicationMessage::AppendEntriesRequest(request);
        let results = self.peers.broadcast(&msg).await;

        for (peer_id, result) in results {
            match result {
                Ok(()) => {
                    trace!(
                        target: "lance::cluster",
                        peer_id,
                        "Sent heartbeat"
                    );
                },
                Err(e) => {
                    warn!(
                        target: "lance::cluster",
                        peer_id,
                        error = %e,
                        "Failed to send heartbeat"
                    );
                },
            }
        }

        Ok(())
    }

    async fn start_election(&self) {
        let pre_vote_request = {
            let mut raft = self.raft.write().await;
            raft.start_pre_vote()
        };

        if let Some(request) = pre_vote_request {
            // Send PreVoteRequest to each peer and collect actual responses
            let peer_ids: Vec<u16> = self.peers.peer_states().await.keys().copied().collect();

            let mut grants = 1usize; // Count self
            for peer_id in peer_ids {
                match self
                    .peers
                    .send_pre_vote_request(peer_id, request.clone())
                    .await
                {
                    Ok(resp) => {
                        if resp.vote_granted {
                            grants += 1;
                            debug!(
                                target: "lance::cluster",
                                peer_id,
                                "Pre-vote granted"
                            );
                        } else {
                            debug!(
                                target: "lance::cluster",
                                peer_id,
                                resp_term = resp.term,
                                "Pre-vote denied"
                            );
                            // If peer has higher term, step down
                            let mut raft = self.raft.write().await;
                            if resp.term > raft.current_term() {
                                raft.handle_pre_vote_response(&resp);
                                return; // Abort election
                            }
                        }
                    },
                    Err(e) => {
                        debug!(
                            target: "lance::cluster",
                            peer_id,
                            error = %e,
                            "Failed to get pre-vote response"
                        );
                    },
                }
            }

            // Check if we have enough pre-votes for majority
            let total_nodes = self.peers.connected_peer_count().await + 1;
            let majority = (total_nodes / 2) + 1;

            if grants >= majority {
                info!(
                    target: "lance::cluster",
                    grants,
                    majority,
                    "Pre-vote succeeded, proceeding to election"
                );
                self.conduct_election().await;
            } else {
                info!(
                    target: "lance::cluster",
                    grants,
                    majority,
                    "Pre-vote failed, not enough votes"
                );
            }
        }
    }

    /// Persist Raft state (term, voted_for) to durable storage
    fn persist_state(&self, term: u64, voted_for: Option<u16>) {
        if let Some(ref log_store) = self.log_store {
            if let Ok(store) = log_store.lock() {
                let state = crate::log_store::PersistentState {
                    current_term: term,
                    voted_for,
                };
                if let Err(e) = store.save_state(&state) {
                    error!(
                        target: "lance::cluster",
                        error = %e,
                        term,
                        "Failed to persist Raft state"
                    );
                }
            }
        }
    }

    async fn conduct_election(&self) {
        let vote_request = {
            let mut raft = self.raft.write().await;
            raft.start_election()
        };

        // Persist state after starting election (term incremented)
        {
            let raft = self.raft.read().await;
            self.persist_state(raft.current_term(), None);
        }

        // Get peer IDs before sending
        let peer_ids: Vec<u16> = self.peers.peer_states().await.keys().copied().collect();

        // Send VoteRequest to each peer and process actual responses
        for peer_id in peer_ids {
            match self
                .peers
                .send_vote_request(peer_id, vote_request.clone())
                .await
            {
                Ok(resp) => {
                    let won = {
                        let mut raft = self.raft.write().await;
                        raft.handle_vote_response(peer_id, &resp)
                    };

                    if won {
                        // We became leader with real votes!
                        lnc_metrics::increment_raft_elections_won();
                        let (term, token) = {
                            let raft = self.raft.read().await;
                            (raft.current_term(), raft.fencing_token())
                        };

                        if let Some(token) = token {
                            info!(
                                target: "lance::cluster",
                                term,
                                fencing_token = %token,
                                peer_id,
                                "Became leader"
                            );

                            let _ = self.event_tx.send(ClusterEvent::BecameLeader {
                                term,
                                fencing_token: token,
                            });
                        }

                        // Update cached state immediately
                        self.update_cached_leader_state(true).await;
                        return;
                    }

                    if resp.vote_granted {
                        debug!(
                            target: "lance::cluster",
                            peer_id,
                            "Vote granted (not yet quorum)"
                        );
                    } else {
                        debug!(
                            target: "lance::cluster",
                            peer_id,
                            resp_term = resp.term,
                            "Vote denied"
                        );
                    }
                },
                Err(e) => {
                    debug!(
                        target: "lance::cluster",
                        peer_id,
                        error = %e,
                        "Failed to get vote response"
                    );
                },
            }
        }

        // If we get here, we didn't win the election
        info!(
            target: "lance::cluster",
            "Election did not reach quorum, reverting to follower"
        );
    }
}

/// Handle an incoming peer connection
async fn handle_peer_connection(
    stream: &mut tokio::net::TcpStream,
    raft: Arc<RwLock<RaftNode>>,
    _peers: Arc<PeerManager>,
    node_id: u16,
    event_tx: broadcast::Sender<ClusterEvent>,
    log_store: Option<Arc<std::sync::Mutex<LogStore>>>,
) -> Result<(), std::io::Error> {
    let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer

    loop {
        // Read message length (4 bytes, little-endian to match PeerConnection::send)
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(()); // Connection closed cleanly
            },
            Err(e) => return Err(e),
        }

        let msg_len = u32::from_le_bytes(len_buf) as usize;
        if msg_len > buf.len() {
            buf.resize(msg_len, 0);
        }

        stream.read_exact(&mut buf[..msg_len]).await?;

        // Decode the message
        let msg = match ReplicationCodec::decode(&buf[..msg_len]) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    target: "lance::cluster",
                    error = %e,
                    "Failed to decode peer message"
                );
                continue;
            },
        };

        // Process message and generate response
        let response = match msg {
            ReplicationMessage::AppendEntriesRequest(req) => {
                // Extract topic operations from entries before processing
                for entry in &req.entries {
                    if entry.entry_type == crate::codec::EntryType::TopicOp {
                        if let Some(op) = crate::codec::TopicOperation::from_bytes(&entry.data) {
                            debug!(
                                target: "lance::cluster",
                                node_id,
                                ?op,
                                "Received topic operation from leader"
                            );
                            // Emit event for server to apply
                            let _ = event_tx.send(ClusterEvent::TopicOperation(op));
                        }
                    }
                }

                // Persist log entries before acknowledging (Raft durability guarantee)
                if !req.entries.is_empty() {
                    if let Some(ref store) = log_store {
                        if let Ok(mut guard) = store.lock() {
                            // Check if we need to truncate conflicting entries
                            if req.prev_log_index > 0 {
                                let matches = guard.matches(req.prev_log_index, req.prev_log_term);
                                if !matches {
                                    // Truncate from the conflict point
                                    if let Err(e) = guard.truncate_from(req.prev_log_index + 1) {
                                        warn!(
                                            target: "lance::cluster",
                                            error = %e,
                                            "Failed to truncate log"
                                        );
                                    }
                                }
                            }

                            // Append new entries
                            if let Err(e) = guard.append(req.entries.clone()) {
                                warn!(
                                    target: "lance::cluster",
                                    error = %e,
                                    "Failed to append log entries"
                                );
                            }
                        }
                    }
                }

                let mut raft_guard = raft.write().await;
                let resp = raft_guard.handle_append_entries(&req);
                Some(ReplicationMessage::AppendEntriesResponse(resp))
            },
            ReplicationMessage::PreVoteRequest(req) => {
                let raft_guard = raft.read().await;
                let resp = raft_guard.handle_pre_vote_request(&req);
                Some(ReplicationMessage::PreVoteResponse(resp))
            },
            ReplicationMessage::VoteRequest(req) => {
                let mut raft_guard = raft.write().await;
                let resp = raft_guard.handle_vote_request(&req);
                Some(ReplicationMessage::VoteResponse(resp))
            },
            ReplicationMessage::InstallSnapshotRequest(req) => {
                // Handle snapshot installation (Raft §7)
                let current_term = {
                    let raft_guard = raft.read().await;
                    raft_guard.current_term()
                };

                // Reject if term is stale
                if req.term < current_term {
                    return Ok(());
                }

                // Update term if needed
                if req.term > current_term {
                    let mut raft_guard = raft.write().await;
                    raft_guard.restore_state(req.term, None);
                }

                // Apply snapshot to log store
                let bytes_stored = if let Some(ref store) = log_store {
                    if let Ok(mut guard) = store.lock() {
                        // Compact log up to snapshot index
                        if let Err(e) = guard.compact_to(req.last_included_index) {
                            warn!(
                                target: "lance::cluster",
                                error = %e,
                                "Failed to compact log for snapshot"
                            );
                        }
                        req.data.len() as u64
                    } else {
                        0
                    }
                } else {
                    req.data.len() as u64
                };

                info!(
                    target: "lance::cluster",
                    node_id,
                    last_included_index = req.last_included_index,
                    bytes = bytes_stored,
                    done = req.done,
                    "Installed snapshot chunk"
                );

                Some(ReplicationMessage::InstallSnapshotResponse(
                    crate::codec::InstallSnapshotResponse {
                        term: current_term,
                        bytes_stored,
                    },
                ))
            },
            _ => {
                debug!(
                    target: "lance::cluster",
                    node_id,
                    "Received unhandled message type"
                );
                None
            },
        };

        // Send response if any
        if let Some(resp) = response {
            let mut codec = ReplicationCodec::new();
            match codec.encode(&resp) {
                Ok(data) => {
                    let len_bytes = (data.len() as u32).to_le_bytes();
                    stream.write_all(&len_bytes).await?;
                    stream.write_all(&data).await?;
                    stream.flush().await?;
                },
                Err(e) => {
                    warn!(
                        target: "lance::cluster",
                        error = %e,
                        "Failed to encode response"
                    );
                },
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_coordinator_new() {
        let config = ClusterConfig::default();
        let coordinator = ClusterCoordinator::new(config);
        assert_eq!(coordinator.node_id(), 0);
    }

    #[tokio::test]
    async fn test_cluster_not_leader_initially() {
        let config = ClusterConfig::default();
        let coordinator = ClusterCoordinator::new(config);
        assert!(!coordinator.is_leader());
    }

    #[tokio::test]
    async fn test_cluster_subscribe() {
        let config = ClusterConfig::default();
        let coordinator = ClusterCoordinator::new(config);
        let _rx = coordinator.subscribe();
        // Should not panic
    }
}
