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
use tokio::sync::{Notify, RwLock, broadcast};
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
    /// Data received from leader for local segment write (follower replication, legacy format)
    DataReceived {
        topic_id: u32,
        payload: bytes::Bytes,
    },
    /// Enriched data received from leader with segment metadata (L3 filesystem-consistent mode)
    DataReceivedEnriched(crate::codec::DataReplicationEntry),
}

/// Persist follower AppendEntries state transition without blocking async workers.
async fn persist_append_entries_blocking(
    log_store: Option<Arc<std::sync::Mutex<LogStore>>>,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<crate::codec::LogEntry>,
) -> Result<(), std::io::Error> {
    let Some(store) = log_store else {
        return Ok(());
    };

    tokio::task::spawn_blocking(move || {
        let mut guard = store
            .lock()
            .map_err(|e| std::io::Error::other(format!("Raft log store lock poisoned: {e}")))?;

        if prev_log_index > 0 {
            let matches = guard.matches(prev_log_index, prev_log_term);
            if !matches {
                guard
                    .truncate_from(prev_log_index + 1)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
        }

        guard
            .append(entries)
            .map_err(|e| std::io::Error::other(e.to_string()))
    })
    .await
    .map_err(|e| std::io::Error::other(format!("Append persistence task failed: {e}")))?
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
    /// Notify signal fired whenever commit_index advances.
    ///
    /// The Apply Loop awaits this to achieve sub-millisecond time-to-visibility
    /// instead of polling on heartbeat ticks. Fired by:
    /// - `handle_peer_connection` after follower processes AppendEntries
    /// - `replicate_data_enriched` after leader's commit_index advances via quorum
    commit_notify: Arc<Notify>,
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
            commit_notify: Arc::new(Notify::new()),
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

        // Restore persistent state if available, including log position from LogStore
        if persistent_state.current_term > 0 {
            let last_log_index = log_store.last_index();
            let last_log_term = log_store.last_term();
            raft.restore_state(
                persistent_state.current_term,
                persistent_state.voted_for,
                last_log_index,
                last_log_term,
            );
            info!(
                target: "lance::cluster",
                term = persistent_state.current_term,
                voted_for = ?persistent_state.voted_for,
                last_log_index,
                last_log_term,
                "Restored Raft state from disk with log position"
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
            commit_notify: Arc::new(Notify::new()),
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
    ///
    /// Uses Acquire ordering to ensure visibility of leader-only data structures
    /// that are initialized when becoming leader (e.g., next_index, match_index).
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.cached_is_leader
            .load(std::sync::atomic::Ordering::Acquire)
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

    /// Persist a single log entry without blocking async executor workers.
    async fn persist_log_entry_blocking(
        &self,
        entry: crate::codec::LogEntry,
    ) -> Result<(), std::io::Error> {
        let Some(store) = self.log_store.clone() else {
            return Ok(());
        };

        tokio::task::spawn_blocking(move || {
            let mut guard = store
                .lock()
                .map_err(|e| std::io::Error::other(format!("Raft log store lock poisoned: {e}")))?;
            guard
                .append(vec![entry])
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
        .await
        .map_err(|e| std::io::Error::other(format!("Raft log persistence task failed: {e}")))?
    }

    /// Replicate a topic operation to followers (leader only)
    /// Uses L3 quorum replication - waits for quorum ACKs before returning
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

    /// Internal replication implementation.
    /// `sync_quorum`: when true, require ACKs from all currently connected followers
    /// before returning success to client-facing metadata operations.
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

        // Advance leader log first so prev_log_index/term are coherent for followers.
        let (term, prev_log_index, prev_log_term, new_log_index) = {
            let mut raft = self.raft.write().await;
            let prev_idx = raft.last_log_index();
            let prev_term = raft.current_term();
            raft.advance_log(1);
            (
                raft.current_term(),
                prev_idx,
                prev_term,
                raft.last_log_index(),
            )
        };

        let log_entry = crate::codec::LogEntry {
            term,
            index: new_log_index,
            hlc: lnc_core::HlcTimestamp::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0),
                0,
            ),
            entry_type: crate::codec::EntryType::TopicOp,
            data: op_bytes,
        };

        // Leader durability before replication.
        if let Err(e) = self.persist_log_entry_blocking(log_entry.clone()).await {
            error!(
                target: "lance::cluster",
                error = %e,
                topic_op = ?op,
                "Leader failed to persist topic operation"
            );
            return Err(std::io::Error::other(format!(
                "Leader log persistence failed: {}",
                e
            )));
        }

        let leader_commit = {
            let raft = self.raft.read().await;
            raft.commit_index()
        };

        let request = crate::codec::AppendEntriesRequest {
            term,
            leader_id: self.config.node_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            leader_hlc: log_entry.hlc,
            entries: vec![log_entry],
        };

        let peer_ids = self.peers.peer_ids().await;
        let expected_follower_count = peer_ids.len();
        let connected_follower_count = self
            .peers
            .peer_states()
            .await
            .values()
            .filter(|state| **state == PeerState::Connected)
            .count();
        let mut success_count: usize = 0;
        let mut fail_count: usize = 0;
        let mut pending_peers = peer_ids;
        let max_attempts = 3usize;

        for attempt in 1..=max_attempts {
            if pending_peers.is_empty() {
                break;
            }

            let mut join_set = tokio::task::JoinSet::new();
            let current_batch = std::mem::take(&mut pending_peers);

            for peer_id in current_batch {
                let peers = Arc::clone(&self.peers);
                let req = request.clone();
                join_set.spawn(async move {
                    let result = match tokio::time::timeout(
                        Duration::from_secs(3),
                        peers.send_append_entries(peer_id, req),
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(_) => Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Topic operation AppendEntries RPC timed out",
                        )),
                    };
                    (peer_id, result)
                });
            }

            while let Some(join_result) = join_set.join_next().await {
                if let Ok((peer_id, result)) = join_result {
                    match result {
                        Ok(resp) if resp.success => {
                            success_count += 1;
                            let mut raft = self.raft.write().await;
                            let _ = raft.update_match_index(peer_id, resp.match_index);
                        },
                        Ok(_) | Err(_) => {
                            pending_peers.push(peer_id);
                        },
                    }
                } else {
                    fail_count += 1;
                }
            }

            let committed = {
                let raft = self.raft.read().await;
                raft.commit_index() >= new_log_index
            };
            if committed && (!sync_quorum || success_count >= expected_follower_count) {
                break;
            }

            if !pending_peers.is_empty() {
                warn!(
                    target: "lance::cluster",
                    log_index = new_log_index,
                    attempt,
                    pending = pending_peers.len(),
                    "Retrying topic operation replication for pending peers"
                );
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }

        fail_count += pending_peers.len();

        let committed = {
            let raft = self.raft.read().await;
            raft.commit_index() >= new_log_index
        };

        if !committed {
            lnc_metrics::increment_quorum_failures();
            warn!(
                target: "lance::cluster",
                log_index = new_log_index,
                success_count,
                fail_count,
                sync_quorum,
                "Topic operation replicated but not committed (quorum not reached)"
            );
            return Err(std::io::Error::other(
                "Topic operation replication quorum not reached",
            ));
        }

        // For client-facing topic metadata operations, require full convergence to
        // all configured followers (not just currently connected). Treating
        // disconnected peers as optional can return false success and create
        // divergent topic registries behind a load-balanced endpoint.
        if sync_quorum && success_count < expected_follower_count {
            return Err(std::io::Error::other(format!(
                "Topic operation not replicated to all followers (acked={success_count}, expected={expected_follower_count}, connected_now={connected_follower_count})"
            )));
        }

        self.commit_notify.notify_one();

        // Best-effort commit-index heartbeat so followers that appended the entry can
        // apply it immediately instead of waiting for the next periodic heartbeat.
        // IMPORTANT: use request/response API (not write-only broadcast) so
        // AppendEntries responses are drained and cannot poison subsequent RPCs.
        if let Some(commit_req) = {
            let raft = self.raft.read().await;
            raft.create_append_entries(Vec::new())
        } {
            let peers = Arc::clone(&self.peers);
            tokio::spawn(async move {
                let peer_ids = peers.peer_ids().await;
                let mut join_set = tokio::task::JoinSet::new();
                for peer_id in peer_ids {
                    let peers = Arc::clone(&peers);
                    let req = commit_req.clone();
                    join_set.spawn(async move {
                        let _ = peers.send_append_entries(peer_id, req).await;
                    });
                }
                while join_set.join_next().await.is_some() {}
            });
        }

        // Record replication latency on success
        lnc_metrics::record_replication_latency(replication_start.elapsed());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_sync_time
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);

        info!(
            target: "lance::cluster",
            log_index = new_log_index,
            success_count,
            fail_count,
            sync_quorum,
            latency_us = replication_start.elapsed().as_micros(),
            "Topic operation replicated and committed"
        );
        Ok(())
    }

    /// Replicate ingested data to followers using the enriched wire format (L3 quorum).
    ///
    /// Per Architecture.md §4.1.1 and LWP-Specification.md §18.2:
    /// Encodes a `DataReplicationEntry` into a `LogEntry` with `EntryType::Data` and
    /// broadcasts via AppendEntries to all connected peers.
    pub async fn replicate_data_enriched(
        &self,
        entry: crate::codec::DataReplicationEntry,
    ) -> Result<Vec<u16>, std::io::Error> {
        // Only leader replicates data
        if !self.is_leader() {
            return Ok(vec![]);
        }

        let encoded = entry.encode();

        // Advance leader's log index BEFORE replicating so the AppendEntries
        // carries the correct prev_log_index. This ensures elections (§5.4.1)
        // pick the candidate with the most up-to-date log.
        let (term, prev_log_index, prev_log_term, new_log_index) = {
            let mut raft = self.raft.write().await;
            let prev_idx = raft.last_log_index();
            let prev_term = raft.current_term();
            raft.advance_log(1);
            (
                raft.current_term(),
                prev_idx,
                prev_term,
                raft.last_log_index(),
            )
        };

        let log_entry = crate::codec::LogEntry {
            term,
            index: new_log_index,
            hlc: lnc_core::HlcTimestamp::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0),
                0,
            ),
            entry_type: crate::codec::EntryType::Data,
            data: encoded,
        };

        // Persist to leader's own log store BEFORE sending to followers (Raft durability)
        if let Err(e) = self.persist_log_entry_blocking(log_entry.clone()).await {
            error!(
                target: "lance::cluster",
                topic_id = entry.topic_id,
                error = %e,
                "Leader failed to persist log entry"
            );
            return Err(std::io::Error::other(format!(
                "Leader log persistence failed: {}",
                e
            )));
        }

        // Get current commit_index to send to followers
        let leader_commit = {
            let raft = self.raft.read().await;
            raft.commit_index()
        };

        let request = crate::codec::AppendEntriesRequest {
            term,
            leader_id: self.config.node_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            leader_hlc: log_entry.hlc,
            entries: vec![log_entry],
        };

        // Send to all peers CONCURRENTLY and wait for responses (true sync replication).
        // Each peer gets its own spawned task so network RTTs overlap instead of adding.
        let peer_ids = self.peers.peer_ids().await;
        let start = std::time::Instant::now();
        let mut join_set = tokio::task::JoinSet::new();

        for peer_id in peer_ids {
            let peers = Arc::clone(&self.peers);
            let req = request.clone();
            join_set.spawn(async move {
                let peer_start = std::time::Instant::now();
                let result = peers.send_append_entries(peer_id, req).await;
                (peer_id, result, peer_start.elapsed())
            });
        }

        let mut successful_peers: Vec<u16> = Vec::new();
        let mut fail_count: usize = 0;

        // Process responses and update match_index for each peer
        // Optimization: Return early once quorum is reached (avoid livelock on slow/dead nodes)
        while let Some(join_result) = join_set.join_next().await {
            if let Ok((peer_id, result, peer_elapsed)) = join_result {
                lnc_metrics::record_peer_replication_latency(peer_id, peer_elapsed);
                match result {
                    Ok(resp) if resp.success => {
                        successful_peers.push(peer_id);

                        // Update match_index and check if commit_index advanced
                        let mut raft = self.raft.write().await;
                        if let Some(new_commit) = raft.update_match_index(peer_id, resp.match_index)
                        {
                            debug!(
                                target: "lance::cluster",
                                peer_id,
                                match_index = resp.match_index,
                                new_commit,
                                "Peer replicated, commit_index advanced"
                            );
                        }

                        // Early return optimization: If quorum reached, no need to wait for stragglers
                        if raft.commit_index() >= new_log_index {
                            debug!(
                                target: "lance::cluster",
                                log_index = new_log_index,
                                success_count = successful_peers.len(),
                                "Quorum reached, returning early"
                            );
                            // Abort remaining tasks. Note: PeerManager must handle dropped
                            // futures gracefully (e.g., keep connections in a pool rather than
                            // closing on drop) to avoid connection flapping.
                            join_set.abort_all();
                            break;
                        }
                    },
                    Ok(_) | Err(_) => {
                        fail_count += 1;
                    },
                }
            } else {
                fail_count += 1;
            }
        }

        // Final check if we reached quorum for this entry
        let committed = {
            let raft = self.raft.read().await;
            raft.commit_index() >= new_log_index
        };

        if !committed {
            warn!(
                target: "lance::cluster",
                topic_id = entry.topic_id,
                log_index = new_log_index,
                success_count = successful_peers.len(),
                fail_count,
                "Data replicated but not yet committed (quorum not reached)"
            );
            return Err(std::io::Error::other("Replication quorum not reached"));
        }

        // Signal the Apply Loop to drain newly committed entries.
        // The dedicated Applier task wakes immediately (sub-ms latency).
        self.commit_notify.notify_one();

        if fail_count > 0 {
            debug!(
                target: "lance::cluster",
                topic_id = entry.topic_id,
                global_offset = entry.global_offset,
                segment = %entry.segment_name,
                success_count = successful_peers.len(),
                fail_count,
                "Enriched data replication (partial failure but committed)"
            );
        }

        // Record aggregate replication latency
        lnc_metrics::record_replication_latency(start.elapsed());

        // Update last sync time
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_sync_time
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);

        Ok(successful_peers)
    }

    /// Decode a data replication payload.
    ///
    /// Handles both enriched and legacy wire formats:
    /// - **Enriched** (≥34 bytes): Returns full `DataReplicationEntry`
    /// - **Legacy** (<34 bytes): `[topic_id: 4 bytes LE][payload bytes]`
    ///
    /// Returns `(topic_id, payload)` for backward compatibility.
    /// Use `decode_data_entry_enriched()` to get the full enriched entry.
    pub fn decode_data_entry(data: &[u8]) -> Option<(u32, bytes::Bytes)> {
        if data.len() < 4 {
            return None;
        }

        // Try enriched format first
        if let Some(entry) = crate::codec::DataReplicationEntry::decode(data) {
            return Some((entry.topic_id, entry.payload));
        }

        // Fall back to legacy format
        let topic_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let payload = bytes::Bytes::copy_from_slice(&data[4..]);

        Some((topic_id, payload))
    }

    /// Decode a data entry from the log (enriched format with segment metadata).
    fn decode_data_entry_enriched(data: &[u8]) -> Option<crate::codec::DataReplicationEntry> {
        crate::codec::DataReplicationEntry::decode(data)
    }

    /// Apply committed entries to the state machine (the "Apply Loop").
    ///
    /// **Linearizability Invariant**: Nothing reaches the state machine until it
    /// is committed in the LogStore. This is the *only* place that emits
    /// `ClusterEvent::TopicOperation` / `DataReceivedEnriched` / `DataReceived`.
    ///
    /// **Crash Safety**: `last_applied` advances entry-by-entry. On restart,
    /// unapplied entries are re-emitted. Subscribers must be idempotent
    /// (ignore entries with index ≤ their own high-water mark).
    ///
    /// **Zero-Copy**: `LogStore::get_range` returns `Bytes::slice()` views —
    /// no heap allocation per entry.
    ///
    /// **Backpressure**: If the broadcast channel has no receivers, the loop
    /// stops to avoid unbounded work. Subscribers that lag will receive
    /// `RecvError::Lagged` and must re-sync from `last_applied`.
    async fn apply_committed_entries(&self) {
        let (last_applied, commit_index) = {
            let raft = self.raft.read().await;
            (raft.last_applied(), raft.commit_index())
        };

        if last_applied >= commit_index {
            return;
        }

        // Without a LogStore we cannot drain entries — fall back to
        // optimistic advance (in-memory-only mode, e.g. unit tests).
        let Some(ref store) = self.log_store else {
            let mut raft = self.raft.write().await;
            raft.advance_last_applied(commit_index);
            return;
        };

        // Read the committed-but-unapplied range from the LogStore.
        // Zero-copy: Bytes::slice() views into mmap'd / BufReader segments.
        let entries = {
            let guard = match store.lock() {
                Ok(g) => g,
                Err(e) => {
                    warn!(
                        target: "lance::apply",
                        error = %e,
                        "LogStore lock poisoned, skipping apply cycle"
                    );
                    return;
                },
            };
            guard.get_range(last_applied + 1, commit_index)
        };

        if entries.is_empty() {
            // Entries may have been compacted — advance to prevent stall
            let mut raft = self.raft.write().await;
            raft.advance_last_applied(commit_index);
            trace!(
                target: "lance::apply",
                last_applied = commit_index,
                "No entries in range (compacted), advanced last_applied"
            );
            return;
        }

        let mut applied_count: u64 = 0;

        for entry in &entries {
            // Map LogEntry → ClusterEvent based on entry type
            let event = match entry.entry_type {
                crate::codec::EntryType::TopicOp => {
                    match crate::codec::TopicOperation::from_bytes(&entry.data) {
                        Some(op) => Some(ClusterEvent::TopicOperation(op)),
                        None => {
                            warn!(
                                target: "lance::apply",
                                index = entry.index,
                                "Failed to decode TopicOperation, skipping"
                            );
                            None
                        },
                    }
                },
                crate::codec::EntryType::Data => {
                    // Try enriched format first, fall back to legacy
                    if let Some(enriched) = Self::decode_data_entry_enriched(&entry.data) {
                        Some(ClusterEvent::DataReceivedEnriched(enriched))
                    } else if let Some((topic_id, payload)) = Self::decode_data_entry(&entry.data) {
                        Some(ClusterEvent::DataReceived { topic_id, payload })
                    } else {
                        warn!(
                            target: "lance::apply",
                            index = entry.index,
                            "Failed to decode Data entry, skipping"
                        );
                        None
                    }
                },
                // No-ops and ConfigChanges don't produce state machine events
                _ => None,
            };

            // Emit to broadcast channel if we have an event
            if let Some(evt) = event {
                if self.event_tx.send(evt).is_err() {
                    // No active receivers — stop applying to avoid unbounded work.
                    // The Applier will retry on the next commit_notify signal.
                    trace!(
                        target: "lance::apply",
                        index = entry.index,
                        "No active event subscribers, pausing apply loop"
                    );
                    break;
                }
            }

            // Advance last_applied entry-by-entry for crash safety.
            // If we crash here, we re-emit from this index on restart.
            {
                let mut raft = self.raft.write().await;
                raft.advance_last_applied(entry.index);
            }
            applied_count += 1;
        }

        if applied_count > 0 {
            debug!(
                target: "lance::apply",
                applied_count,
                new_last_applied = last_applied + applied_count,
                commit_index,
                "Applied committed entries to state machine"
            );
        }
    }

    /// Replicate a No-Op entry immediately after becoming leader (Raft §5.4.2).
    ///
    /// A new leader cannot commit entries from previous terms until it commits
    /// at least one entry from its own term. This prevents commit_index from
    /// stalling after an election.
    ///
    /// Industry best practice: replicate a No-Op entry immediately upon election.
    async fn replicate_noop_entry(&self) {
        let noop_entry = {
            let mut raft = self.raft.write().await;
            raft.create_noop_entry()
        };

        let Some(entry) = noop_entry else {
            return; // Not leader
        };

        info!(
            target: "lance::cluster",
            term = entry.term,
            index = entry.index,
            "Replicating No-Op entry to seal log"
        );

        // Persist to leader's log store
        if let Err(e) = self.persist_log_entry_blocking(entry.clone()).await {
            error!(
                target: "lance::cluster",
                error = %e,
                "Failed to persist No-Op entry"
            );
            return;
        }

        // Replicate to followers
        let request = {
            let raft = self.raft.read().await;
            raft.create_append_entries(vec![entry])
        };

        if let Some(req) = request {
            let peer_ids = self.peers.peer_ids().await;
            let mut join_set = tokio::task::JoinSet::new();
            for peer_id in peer_ids {
                let peers = Arc::clone(&self.peers);
                let req = req.clone();
                join_set.spawn(async move {
                    let _ = peers.send_append_entries(peer_id, req).await;
                });
            }
            while join_set.join_next().await.is_some() {}

            debug!(
                target: "lance::cluster",
                "No-Op entry replicated to followers"
            );
        }
    }

    /// Initialize the cluster coordinator.
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
        let commit_notify = Arc::clone(&self.commit_notify);

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
                        let commit_notify_clone = Arc::clone(&commit_notify);

                        tokio::spawn(async move {
                            if let Err(e) = handle_peer_connection(
                                &mut stream,
                                raft_clone,
                                peers_clone,
                                node_id,
                                event_tx_clone,
                                log_store_clone,
                                commit_notify_clone,
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

    /// Run the cluster coordination loop with a dedicated Applier branch.
    ///
    /// The `commit_notify` branch wakes the Apply Loop with sub-millisecond
    /// latency whenever `commit_index` advances (leader quorum or follower
    /// AppendEntries). The heartbeat tick still calls `apply_committed_entries`
    /// as a safety net in case a notification is missed.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            target: "lance::cluster",
            node_id = self.config.node_id,
            "Starting cluster coordination loop (with Notify-driven Applier)"
        );

        let heartbeat_interval = self.config.heartbeat_interval;
        let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);
        let mut election_check_ticker = tokio::time::interval(Duration::from_millis(50));
        let commit_notify = Arc::clone(&self.commit_notify);

        loop {
            tokio::select! {
                // Immediate wake on commit_index advance (sub-ms visibility)
                _ = commit_notify.notified() => {
                    self.apply_committed_entries().await;
                }
                _ = heartbeat_ticker.tick() => {
                    self.on_heartbeat_tick().await;
                }
                _ = election_check_ticker.tick() => {
                    self.on_election_check().await;
                }
                _ = shutdown.recv() => {
                    // Drain any remaining committed entries before shutdown
                    self.apply_committed_entries().await;
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

        // Apply committed entries to state machine (bridge commit_index to last_applied)
        self.apply_committed_entries().await;

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

        // Use Release ordering to ensure all leader-only data structure writes
        // (next_index, match_index, etc.) are visible to threads reading is_leader
        let old_is_leader = self.cached_is_leader.swap(is_leader, Ordering::Release);

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
        let (should_start_election, is_leader, pre_vote_in_progress) = {
            let raft = self.raft.read().await;
            (
                raft.election_timeout_elapsed(),
                raft.state() == RaftState::Leader,
                raft.pre_vote_in_progress(),
            )
        };

        // Leaders send heartbeats, they don't start elections.
        // Also avoid kicking off overlapping election rounds while pre-vote is active.
        if is_leader || pre_vote_in_progress {
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

        let peer_ids = self.peers.peer_ids().await;
        for peer_id in peer_ids {
            let peers = Arc::clone(&self.peers);
            let req = request.clone();
            tokio::spawn(async move {
                match peers.send_append_entries(peer_id, req).await {
                    Ok(resp) if resp.success => {
                        trace!(
                            target: "lance::cluster",
                            peer_id,
                            "Sent heartbeat"
                        );
                    },
                    Ok(resp) => {
                        warn!(
                            target: "lance::cluster",
                            peer_id,
                            term = resp.term,
                            match_index = resp.match_index,
                            "Heartbeat rejected by follower"
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
            });
        }

        Ok(())
    }

    async fn start_election(&self) {
        let pre_vote_request = {
            let mut raft = self.raft.write().await;
            raft.start_pre_vote()
        };

        if let Some(request) = pre_vote_request {
            // Send PreVoteRequest to all peers CONCURRENTLY
            let peer_ids: Vec<u16> = self.peers.peer_states().await.keys().copied().collect();
            let mut join_set = tokio::task::JoinSet::new();

            for peer_id in peer_ids {
                let peers = Arc::clone(&self.peers);
                let req = request.clone();
                join_set.spawn(async move {
                    let result = peers.send_pre_vote_request(peer_id, req).await;
                    (peer_id, result)
                });
            }

            // Process responses sequentially for term checks
            while let Some(join_result) = join_set.join_next().await {
                let Ok((peer_id, result)) = join_result else {
                    continue;
                };
                match result {
                    Ok(resp) => {
                        let should_proceed = {
                            let mut raft = self.raft.write().await;
                            let proceed = raft.handle_pre_vote_response(peer_id, &resp);

                            if resp.vote_granted {
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
                            }

                            // If we discovered higher term, abort
                            if resp.term > raft.current_term() {
                                return;
                            }

                            proceed
                        };

                        // If we have quorum, proceed to real election
                        if should_proceed {
                            info!(
                                target: "lance::cluster",
                                "Pre-vote succeeded, proceeding to election"
                            );
                            self.conduct_election().await;
                            return;
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

            // All responses processed, check final result
            let has_quorum = {
                let raft = self.raft.read().await;
                raft.votes_received.len() >= raft.quorum_size()
            };

            if !has_quorum {
                {
                    let mut raft = self.raft.write().await;
                    raft.finish_failed_pre_vote_round();
                }
                info!(
                    target: "lance::cluster",
                    "Pre-vote failed, not enough votes"
                );
            }
        }
    }

    /// Persist Raft state (term, voted_for) to durable storage without blocking
    /// async executor workers.
    async fn persist_state_blocking(&self, term: u64, voted_for: Option<u16>) {
        let Some(log_store) = self.log_store.clone() else {
            return;
        };

        let result = tokio::task::spawn_blocking(move || {
            let store = log_store
                .lock()
                .map_err(|e| std::io::Error::other(format!("Raft log store lock poisoned: {e}")))?;
            let state = crate::log_store::PersistentState {
                current_term: term,
                voted_for,
            };
            store
                .save_state(&state)
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
        .await;

        match result {
            Ok(Ok(())) => {},
            Ok(Err(e)) => {
                error!(
                    target: "lance::cluster",
                    error = %e,
                    term,
                    "Failed to persist Raft state"
                );
            },
            Err(e) => {
                error!(
                    target: "lance::cluster",
                    error = %e,
                    term,
                    "Raft state persistence task failed"
                );
            },
        }
    }

    async fn conduct_election(&self) {
        // CRITICAL: Persist state BEFORE sending vote requests to prevent split-brain
        // If we crash after sending requests but before persisting, we could reboot
        // and vote for a different candidate in the same term (Raft safety violation)
        let (vote_request, term, voted_for) = {
            let mut raft = self.raft.write().await;
            let req = raft.start_election();
            let term = raft.current_term();
            let voted_for = raft.voted_for;
            (req, term, voted_for)
        };

        // Persist immediately before sending vote requests.
        self.persist_state_blocking(term, voted_for).await;

        // Send VoteRequest to all peers CONCURRENTLY
        let peer_ids: Vec<u16> = self.peers.peer_states().await.keys().copied().collect();
        let mut join_set = tokio::task::JoinSet::new();

        for peer_id in peer_ids {
            let peers = Arc::clone(&self.peers);
            let req = vote_request.clone();
            join_set.spawn(async move {
                let result = peers.send_vote_request(peer_id, req).await;
                (peer_id, result)
            });
        }

        // Process responses sequentially for Raft state machine updates
        while let Some(join_result) = join_set.join_next().await {
            let Ok((peer_id, result)) = join_result else {
                continue;
            };
            match result {
                Ok(resp) => {
                    let won = {
                        let mut raft = self.raft.write().await;
                        raft.handle_vote_response(peer_id, &resp)
                    };

                    if won {
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

                        // Replicate No-Op entry to seal the log (Raft §5.4.2)
                        // This ensures the new leader can commit entries from its own term
                        // and prevents stalls when previous-term entries exist.
                        self.replicate_noop_entry().await;

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

/// Handle an incoming peer connection (Replication Plane only).
///
/// **Linearizability Invariant**: This function is part of the Replication Plane.
/// Its only job is to persist entries to the LogStore and update RaftNode state.
/// It does NOT emit `ClusterEvent`s — that is the exclusive responsibility of
/// `apply_committed_entries` (the Apply Loop / Application Plane).
///
/// After processing AppendEntries, it fires `commit_notify` so the Apply Loop
/// wakes immediately to drain newly committed entries.
async fn handle_peer_connection(
    stream: &mut tokio::net::TcpStream,
    raft: Arc<RwLock<RaftNode>>,
    _peers: Arc<PeerManager>,
    node_id: u16,
    _event_tx: broadcast::Sender<ClusterEvent>,
    log_store: Option<Arc<std::sync::Mutex<LogStore>>>,
    commit_notify: Arc<Notify>,
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

        // Decode the message (one copy at the boundary — zero-copy inside decoder)
        let msg = match ReplicationCodec::decode(bytes::Bytes::copy_from_slice(&buf[..msg_len])) {
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
                // Persist log entries before acknowledging (Raft durability guarantee).
                // NO event emission here — the Apply Loop handles that after commit.
                if !req.entries.is_empty() {
                    if let Err(e) = persist_append_entries_blocking(
                        log_store.clone(),
                        req.prev_log_index,
                        req.prev_log_term,
                        req.entries.clone(),
                    )
                    .await
                    {
                        warn!(
                            target: "lance::cluster",
                            error = %e,
                            "Failed to persist append entries"
                        );
                    }
                }

                // Update Raft state (term, commit_index, last_log_index).
                // Capture commit_index before and after under a single write lock
                // to avoid TOCTOU race.
                let (resp, commit_advanced) = {
                    let mut raft_guard = raft.write().await;
                    let old_commit = raft_guard.commit_index();
                    let r = raft_guard.handle_append_entries(&req);
                    (r, raft_guard.commit_index() > old_commit)
                };

                // If commit_index advanced, wake the Apply Loop immediately
                if resp.success && commit_advanced {
                    commit_notify.notify_one();
                }

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
                    // Use snapshot's last_included_index/term as the log position
                    raft_guard.restore_state(
                        req.term,
                        None,
                        req.last_included_index,
                        req.last_included_term,
                    );
                }

                // Apply snapshot to log store without blocking async workers.
                let bytes_stored = if let Some(store) = log_store.clone() {
                    let snapshot_index = req.last_included_index;
                    let snapshot_bytes = req.data.len() as u64;
                    match tokio::task::spawn_blocking(move || {
                        let mut guard = store.lock().map_err(|e| {
                            std::io::Error::other(format!("Raft log store lock poisoned: {e}"))
                        })?;
                        guard
                            .compact_to(snapshot_index)
                            .map_err(|e| std::io::Error::other(e.to_string()))?;
                        Ok::<u64, std::io::Error>(snapshot_bytes)
                    })
                    .await
                    {
                        Ok(Ok(bytes)) => bytes,
                        Ok(Err(e)) => {
                            warn!(
                                target: "lance::cluster",
                                error = %e,
                                "Failed to compact log for snapshot"
                            );
                            0
                        },
                        Err(e) => {
                            warn!(
                                target: "lance::cluster",
                                error = %e,
                                "Snapshot compaction task failed"
                            );
                            0
                        },
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
    use lnc_core::HlcTimestamp;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::tempdir;

    fn make_log_entry(term: u64, index: u64, payload: &[u8]) -> crate::codec::LogEntry {
        crate::codec::LogEntry {
            term,
            index,
            hlc: HlcTimestamp::new(1_000, 0),
            entry_type: crate::codec::EntryType::Data,
            data: bytes::Bytes::copy_from_slice(payload),
        }
    }

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

    #[tokio::test]
    async fn test_persist_append_entries_blocking_persists_new_entries() {
        let dir = tempdir().unwrap();
        let mut store = LogStore::open(dir.path()).unwrap();
        store
            .append(vec![make_log_entry(1, 1, b"a"), make_log_entry(1, 2, b"b")])
            .unwrap();

        let log_store = Arc::new(std::sync::Mutex::new(store));

        // Matching prev_log_index/term should append next entry durably.
        persist_append_entries_blocking(
            Some(Arc::clone(&log_store)),
            2,
            1,
            vec![make_log_entry(2, 3, b"c")],
        )
        .await
        .unwrap();

        let guard = log_store.lock().unwrap();
        assert!(guard.matches(2, 1));
        assert!(guard.matches(3, 2));
        assert_eq!(guard.last_index(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_persist_append_entries_blocking_does_not_block_async_runtime() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path()).unwrap();
        let log_store = Arc::new(std::sync::Mutex::new(store));

        // Hold lock from a plain thread to force persistence helper to wait.
        let held_store = Arc::clone(&log_store);
        let lock_holder = std::thread::spawn(move || {
            let _guard = held_store.lock().unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });

        // Give lock_holder a chance to acquire the lock first.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Heartbeat task should keep ticking while persistence awaits lock release.
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticks_clone = Arc::clone(&ticks);
        let heartbeat = tokio::spawn(async move {
            for _ in 0..50 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                ticks_clone.fetch_add(1, Ordering::Relaxed);
            }
        });

        persist_append_entries_blocking(
            Some(Arc::clone(&log_store)),
            0,
            0,
            vec![make_log_entry(1, 1, b"x")],
        )
        .await
        .unwrap();

        lock_holder.join().unwrap();
        heartbeat.await.unwrap();

        assert!(
            ticks.load(Ordering::Relaxed) > 0,
            "heartbeat task should make progress while append persistence waits"
        );
    }
}
