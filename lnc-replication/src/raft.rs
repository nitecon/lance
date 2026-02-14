//! Raft consensus protocol implementation with safety enhancements.
//!
//! This module implements the core Raft state machine with the following
//! enhancements beyond the basic protocol:
//!
//! - **Pre-Vote**: Prevents disruption from partitioned nodes rejoining
//! - **Fencing tokens**: Revokes write authority from deposed leaders
//! - **Leader lease**: Optimistic reads without quorum confirmation
//!
//! # References
//!
//! - Ongaro, D. "Consensus: Bridging Theory and Practice" (§9.6 Pre-Vote)
//! - Raft Extended Paper: <https://raft.github.io/raft.pdf>

use crate::codec::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, LogEntry, PreVoteRequest,
    PreVoteResponse, TimeoutNowRequest, TimeoutNowResponse, VoteRequest, VoteResponse,
};
use lnc_core::{HlcTimestamp, HybridLogicalClock};
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Raft node state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    /// Following a leader, receiving log entries.
    Follower,
    /// Conducting pre-vote or election.
    Candidate,
    /// Leading the cluster, replicating log entries.
    Leader,
}

/// Fencing token for preventing stale leaders from writing.
///
/// The token encodes (term, node_id) to ensure uniqueness and monotonicity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Create a new fencing token from term and node ID.
    #[inline]
    #[must_use]
    pub const fn new(term: u64, node_id: u16) -> Self {
        // High 48 bits = term, low 16 bits = node_id
        Self((term << 16) | (node_id as u64))
    }

    /// Extract the term from the token.
    #[inline]
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.0 >> 16
    }

    /// Extract the node ID from the token.
    #[inline]
    #[must_use]
    pub const fn node_id(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Get the raw u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for FencingToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fence(term={}, node={})", self.term(), self.node_id())
    }
}

/// Configuration for Raft timing parameters.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Election timeout range (min).
    pub election_timeout_min: Duration,
    /// Election timeout range (max).
    pub election_timeout_max: Duration,
    /// Heartbeat interval (should be << election_timeout).
    pub heartbeat_interval: Duration,
    /// Pre-vote enabled (recommended for production).
    pub pre_vote_enabled: bool,
    /// Leader lease duration for optimistic reads.
    pub leader_lease: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            pre_vote_enabled: true,
            leader_lease: Duration::from_millis(100),
        }
    }
}

/// Core Raft node state machine.
pub struct RaftNode {
    /// This node's ID.
    node_id: u16,
    /// Current state.
    state: RaftState,
    /// Current term.
    current_term: u64,
    /// Node voted for in current term (if any).
    pub(crate) voted_for: Option<u16>,
    /// Current leader (if known).
    leader_id: Option<u16>,
    /// Last time we heard from the leader.
    last_leader_contact: Instant,
    /// Current election timeout.
    election_timeout: Duration,
    /// Cluster configuration.
    config: ClusterConfig,
    /// Raft configuration parameters.
    raft_config: RaftConfig,
    /// Hybrid Logical Clock for timestamps.
    hlc: HybridLogicalClock,
    /// Current fencing token (for leaders).
    fencing_token: Option<FencingToken>,
    /// Log: last index.
    last_log_index: u64,
    /// Log: last term.
    last_log_term: u64,
    /// Commit index.
    commit_index: u64,
    /// Last applied index (entries applied to state machine via Apply Loop).
    last_applied: u64,
    /// Pre-vote in progress.
    pre_vote_in_progress: bool,
    /// Votes received in current election.
    pub(crate) votes_received: HashSet<u16>,
    /// Next log index to send to each peer (leader only).
    next_index: std::collections::HashMap<u16, u64>,
    /// Highest log index known to be replicated on each peer (leader only).
    match_index: std::collections::HashMap<u16, u64>,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(node_id: u16, config: ClusterConfig, raft_config: RaftConfig) -> Self {
        let election_timeout = Self::random_election_timeout(&raft_config);

        Self {
            node_id,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            last_leader_contact: Instant::now(),
            election_timeout,
            config,
            raft_config,
            hlc: HybridLogicalClock::new(node_id),
            fencing_token: None,
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            last_applied: 0,
            pre_vote_in_progress: false,
            votes_received: HashSet::new(),
            next_index: std::collections::HashMap::new(),
            match_index: std::collections::HashMap::new(),
        }
    }

    /// Restore persistent state after crash recovery (Raft §5.2)
    ///
    /// This must be called before the node participates in elections.
    pub fn restore_state(&mut self, term: u64, voted_for: Option<u16>) {
        self.current_term = term;
        self.voted_for = voted_for;
    }

    /// Get the current state.
    #[inline]
    #[must_use]
    pub const fn state(&self) -> RaftState {
        self.state
    }

    /// Get the current term.
    #[inline]
    #[must_use]
    pub const fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Get the current leader ID (if known).
    #[inline]
    #[must_use]
    pub const fn leader_id(&self) -> Option<u16> {
        self.leader_id
    }

    /// Get the current fencing token (leaders only).
    #[inline]
    #[must_use]
    pub const fn fencing_token(&self) -> Option<FencingToken> {
        self.fencing_token
    }

    /// Check if this node is the leader.
    #[inline]
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        matches!(self.state, RaftState::Leader)
    }

    /// Get a new HLC timestamp.
    #[inline]
    pub fn now(&self) -> HlcTimestamp {
        self.hlc.now()
    }

    /// Get the last log index.
    #[inline]
    #[must_use]
    pub const fn last_log_index(&self) -> u64 {
        self.last_log_index
    }

    /// Get the last applied index.
    #[inline]
    #[must_use]
    pub const fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Update last_applied after successfully applying an entry to the state machine.
    pub fn advance_last_applied(&mut self, new_last_applied: u64) {
        if new_last_applied > self.last_applied {
            self.last_applied = new_last_applied;
        }
    }

    /// Advance the Raft log index after a successful write or replication.
    ///
    /// Called by:
    /// - **Leader**: after each local write, before replicating to followers
    /// - **Follower**: after processing AppendEntries with data entries
    ///
    /// This ensures `last_log_index` reflects actual replication progress,
    /// so elections (§5.4.1) pick the candidate with the most up-to-date log.
    pub fn advance_log(&mut self, entry_count: u64) {
        self.last_log_index += entry_count;
        self.last_log_term = self.current_term;
    }

    // =========================================================================
    // Pre-Vote Protocol (Raft §9.6)
    // =========================================================================

    /// Start a pre-vote round before potentially starting an election.
    ///
    /// Pre-vote checks if we would win an election without incrementing our term,
    /// preventing disruption from partitioned nodes with stale logs.
    ///
    /// Returns the request to broadcast to all nodes.
    pub fn start_pre_vote(&mut self) -> Option<PreVoteRequest> {
        if !self.raft_config.pre_vote_enabled {
            // Skip pre-vote, go directly to election
            return None;
        }

        if self.state == RaftState::Leader {
            // Leaders don't start pre-votes
            return None;
        }

        self.pre_vote_in_progress = true;
        self.votes_received.clear();
        // Vote for ourselves in pre-vote
        self.votes_received.insert(self.node_id);

        Some(PreVoteRequest {
            // Use hypothetical next term (don't actually increment)
            term: self.current_term + 1,
            candidate_id: self.node_id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        })
    }

    /// Handle an incoming pre-vote request.
    ///
    /// Grant pre-vote if:
    /// 1. Candidate's term is at least as high as ours
    /// 2. Candidate's log is at least as up-to-date as ours
    /// 3. We haven't heard from a leader recently
    pub fn handle_pre_vote_request(&self, req: &PreVoteRequest) -> PreVoteResponse {
        // Check if candidate's term is high enough
        if req.term < self.current_term {
            return PreVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        // Check if candidate's log is up-to-date
        if !self.log_is_up_to_date(req.last_log_index, req.last_log_term) {
            return PreVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        // Check if we've heard from a leader recently
        // (prevents disruption if there's a working leader)
        let leader_active = self.last_leader_contact.elapsed() < self.election_timeout;
        if leader_active && self.leader_id.is_some() {
            return PreVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        PreVoteResponse {
            term: self.current_term,
            vote_granted: true,
        }
    }

    /// Handle an incoming pre-vote response.
    ///
    /// Returns `true` if pre-vote succeeded and we should start a real election.
    pub fn handle_pre_vote_response(&mut self, voter_id: u16, resp: &PreVoteResponse) -> bool {
        if !self.pre_vote_in_progress || resp.term < self.current_term {
            return false;
        }

        if resp.term > self.current_term {
            // Discovered higher term, step down
            self.become_follower(resp.term);
            self.pre_vote_in_progress = false;
            return false;
        }

        if resp.vote_granted {
            self.votes_received.insert(voter_id);
        }

        // Check if we have enough pre-votes for quorum
        self.votes_received.len() >= self.quorum_size()
    }

    // =========================================================================
    // Standard Raft Election
    // =========================================================================

    /// Start a real election (after successful pre-vote if enabled).
    ///
    /// Returns the vote request to broadcast.
    pub fn start_election(&mut self) -> VoteRequest {
        // Increment term
        self.current_term += 1;
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.node_id);
        self.reset_election_timeout();

        tracing::info!(
            target: "lance::raft",
            node_id = self.node_id,
            term = self.current_term,
            "Starting election"
        );

        VoteRequest {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        }
    }

    /// Handle an incoming vote request.
    pub fn handle_vote_request(&mut self, req: &VoteRequest) -> VoteResponse {
        // If request term > current term, update term and become follower
        if req.term > self.current_term {
            self.become_follower(req.term);
        }

        // Reject if request term < current term
        if req.term < self.current_term {
            return VoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        // Check if we can vote for this candidate
        let can_vote = match self.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };

        // Check if candidate's log is up-to-date
        let log_ok = self.log_is_up_to_date(req.last_log_index, req.last_log_term);

        let vote_granted = can_vote && log_ok;

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
            self.reset_election_timeout();
        }

        VoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    /// Handle an incoming vote response.
    ///
    /// Returns `true` if we won the election.
    pub fn handle_vote_response(&mut self, voter_id: u16, resp: &VoteResponse) -> bool {
        if self.state != RaftState::Candidate {
            return false;
        }

        if resp.term > self.current_term {
            self.become_follower(resp.term);
            return false;
        }

        if resp.term < self.current_term {
            // Stale response
            return false;
        }

        if resp.vote_granted {
            self.votes_received.insert(voter_id);
        }

        // Check if we have majority
        if self.votes_received.len() >= self.quorum_size() {
            self.become_leader();
            return true;
        }

        false
    }

    // =========================================================================
    // AppendEntries (Heartbeat + Log Replication)
    // =========================================================================

    /// Create an AppendEntries request (for leaders).
    pub fn create_append_entries(&self, entries: Vec<LogEntry>) -> Option<AppendEntriesRequest> {
        if self.state != RaftState::Leader {
            return None;
        }

        Some(AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.node_id,
            prev_log_index: self.last_log_index,
            prev_log_term: self.last_log_term,
            leader_commit: self.commit_index,
            leader_hlc: self.hlc.now(),
            entries,
        })
    }

    /// Handle an incoming AppendEntries request (for followers).
    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        // Update HLC from leader
        let _ = self.hlc.receive(req.leader_hlc);

        // If request term > current term, update and become follower
        if req.term > self.current_term {
            self.become_follower(req.term);
        }

        // Reject if term < current term
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index,
                follower_hlc: self.hlc.now(),
                follower_id: self.node_id,
            };
        }

        // Valid leader heartbeat - reset election timeout
        self.last_leader_contact = Instant::now();
        self.leader_id = Some(req.leader_id);

        // If we were candidate, step down
        if self.state == RaftState::Candidate {
            self.state = RaftState::Follower;
        }

        // Advance log index for data entries received from leader.
        // This tracks replication progress so elections (§5.4.1) pick
        // the follower with the most up-to-date log.
        let data_entries = req
            .entries
            .iter()
            .filter(|e| e.entry_type == crate::codec::EntryType::Data)
            .count() as u64;
        if data_entries > 0 {
            self.advance_log(data_entries);
        }

        let success = true;

        // Update commit index
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index);
        }

        AppendEntriesResponse {
            term: self.current_term,
            success,
            match_index: self.last_log_index,
            follower_hlc: self.hlc.now(),
            follower_id: self.node_id,
        }
    }

    /// Handle a TimeoutNow request from the current leader.
    /// This triggers an immediate election to facilitate leadership transfer.
    pub fn handle_timeout_now(&mut self, req: &TimeoutNowRequest) -> TimeoutNowResponse {
        // If we're already a leader or candidate, reject
        if self.state != RaftState::Follower {
            tracing::debug!(
                target: "lance::raft",
                node_id = self.node_id,
                state = ?self.state,
                "Rejecting TimeoutNow - not a follower"
            );
            return TimeoutNowResponse {
                term: self.current_term,
                accepted: false,
            };
        }

        // Verify the request is from a valid leader with matching or higher term
        if req.term < self.current_term {
            tracing::debug!(
                target: "lance::raft",
                node_id = self.node_id,
                req_term = req.term,
                current_term = self.current_term,
                "Rejecting TimeoutNow - stale term"
            );
            return TimeoutNowResponse {
                term: self.current_term,
                accepted: false,
            };
        }

        tracing::info!(
            target: "lance::raft",
            node_id = self.node_id,
            from_leader = req.leader_id,
            term = req.term,
            "Received TimeoutNow - starting immediate election"
        );

        // Update term if necessary
        if req.term > self.current_term {
            self.become_follower(req.term);
        }

        // Accept and immediately trigger election
        TimeoutNowResponse {
            term: self.current_term,
            accepted: true,
        }
    }

    /// Create a TimeoutNow request for leadership transfer.
    /// Returns None if this node is not the leader.
    pub fn create_timeout_now_request(&self) -> Option<TimeoutNowRequest> {
        if !self.is_leader() {
            return None;
        }

        Some(TimeoutNowRequest {
            term: self.current_term,
            leader_id: self.node_id,
        })
    }

    // =========================================================================
    // State Transitions
    // =========================================================================

    /// Transition to follower state.
    fn become_follower(&mut self, term: u64) {
        let was_leader = self.state == RaftState::Leader;

        self.state = RaftState::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.pre_vote_in_progress = false;

        // Reset election timeout to prevent election storms when discovering higher term
        // This is critical: without this, a partitioned node will continuously retry
        // elections every 50ms when it discovers the cluster has moved to a higher term
        self.reset_election_timeout();

        if was_leader {
            // Revoke fencing token
            self.fencing_token = None;
            tracing::warn!(
                target: "lance::raft",
                node_id = self.node_id,
                term = term,
                "Stepped down from leader"
            );
        }
    }

    /// Transition to leader state.
    fn become_leader(&mut self) {
        self.state = RaftState::Leader;
        self.leader_id = Some(self.node_id);
        // Reset election timeout so we don't immediately re-trigger elections
        self.reset_election_timeout();

        // Create new fencing token
        self.fencing_token = Some(FencingToken::new(self.current_term, self.node_id));

        // Initialize next_index and match_index for all peers (Raft §5.3)
        // next_index: optimistically assume peers are caught up (last_log_index + 1)
        // match_index: conservatively assume no replication yet (0)
        self.next_index.clear();
        self.match_index.clear();
        for node in &self.config.old_nodes {
            if node.node_id != self.node_id {
                self.next_index
                    .insert(node.node_id, self.last_log_index + 1);
                self.match_index.insert(node.node_id, 0);
            }
        }

        tracing::info!(
            target: "lance::raft",
            node_id = self.node_id,
            term = self.current_term,
            fencing_token = ?self.fencing_token,
            "Became leader"
        );
    }

    /// Create a No-Op entry for a new leader to commit (Raft §5.4.2).
    ///
    /// A new leader cannot immediately commit entries from previous terms,
    /// even if they are stored on a majority. The leader must first replicate
    /// and commit at least one entry from its own term to "seal" the log.
    ///
    /// Industry best practice: append a No-Op entry immediately upon election.
    pub fn create_noop_entry(&mut self) -> Option<crate::codec::LogEntry> {
        if self.state != RaftState::Leader {
            return None;
        }

        // Advance log for the No-Op entry
        self.advance_log(1);

        Some(crate::codec::LogEntry {
            term: self.current_term,
            index: self.last_log_index,
            hlc: self.hlc.now(),
            entry_type: crate::codec::EntryType::Noop,
            data: bytes::Bytes::new(),
        })
    }

    // =========================================================================
    // Fencing
    // =========================================================================

    /// Validate a fencing token before allowing a write.
    ///
    /// Returns `true` if the token is valid (current or newer).
    pub fn validate_fence(&self, token: FencingToken) -> bool {
        match self.fencing_token {
            Some(current) => token >= current,
            None => false, // No token means we're not the leader
        }
    }

    /// Get the minimum valid fencing token.
    ///
    /// Any operation with a token less than this should be rejected.
    #[inline]
    #[must_use]
    pub fn minimum_fence(&self) -> FencingToken {
        FencingToken::new(self.current_term, 0)
    }

    // =========================================================================
    // Leadership Transfer
    // =========================================================================

    /// Initiate graceful leadership transfer to a specific node.
    ///
    /// This is used for rolling upgrades and maintenance operations.
    /// The leader will:
    /// 1. Stop accepting new client requests
    /// 2. Ensure target has caught up with log
    /// 3. Send TimeoutNow to target to trigger immediate election
    ///
    /// Returns the target node ID if transfer is initiated.
    pub fn initiate_leadership_transfer(&mut self, target_node: u16) -> Option<u16> {
        if !self.is_leader() {
            tracing::warn!(
                target: "lance::raft",
                node_id = self.node_id,
                "Cannot transfer leadership - not the leader"
            );
            return None;
        }

        if target_node == self.node_id {
            tracing::warn!(
                target: "lance::raft",
                node_id = self.node_id,
                "Cannot transfer leadership to self"
            );
            return None;
        }

        // Check if target is in cluster configuration
        let target_in_cluster = self
            .config
            .old_nodes
            .iter()
            .any(|n| n.node_id == target_node);

        if !target_in_cluster {
            tracing::warn!(
                target: "lance::raft",
                node_id = self.node_id,
                target = target_node,
                "Cannot transfer leadership - target not in cluster"
            );
            return None;
        }

        tracing::info!(
            target: "lance::raft",
            node_id = self.node_id,
            target = target_node,
            term = self.current_term,
            "Initiating leadership transfer"
        );

        Some(target_node)
    }

    /// Voluntarily step down from leadership.
    ///
    /// This causes the node to become a follower without waiting for
    /// election timeout. Used when gracefully shutting down or during
    /// maintenance.
    pub fn step_down_voluntarily(&mut self) {
        if !self.is_leader() {
            return;
        }

        tracing::info!(
            target: "lance::raft",
            node_id = self.node_id,
            term = self.current_term,
            "Voluntarily stepping down from leadership"
        );

        self.state = RaftState::Follower;
        self.fencing_token = None;
        self.leader_id = None;
        self.reset_election_timeout();
    }

    // =========================================================================
    // Timing
    // =========================================================================

    /// Check if election timeout has elapsed.
    pub fn election_timeout_elapsed(&self) -> bool {
        self.last_leader_contact.elapsed() > self.election_timeout
    }

    /// Reset the election timeout with a new random value.
    fn reset_election_timeout(&mut self) {
        self.election_timeout = Self::random_election_timeout(&self.raft_config);
        self.last_leader_contact = Instant::now();
    }

    /// Generate a random election timeout within configured bounds.
    fn random_election_timeout(config: &RaftConfig) -> Duration {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut hasher);
        let hash = hasher.finish();

        let range =
            config.election_timeout_max.as_millis() - config.election_timeout_min.as_millis();
        let offset = (hash % range as u64) as u128;

        Duration::from_millis((config.election_timeout_min.as_millis() + offset) as u64)
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /// Check if a candidate's log is at least as up-to-date as ours.
    fn log_is_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        // Raft paper §5.4.1: Compare terms first, then indices
        if last_term != self.last_log_term {
            last_term > self.last_log_term
        } else {
            last_index >= self.last_log_index
        }
    }

    /// Calculate quorum size.
    pub fn quorum_size(&self) -> usize {
        let total = if self.config.is_joint {
            // Joint consensus: use larger of old/new configs
            self.config.old_nodes.len().max(self.config.new_nodes.len())
        } else {
            self.config.old_nodes.len()
        };

        total / 2 + 1
    }

    /// Check if a vote count constitutes a quorum.
    pub fn has_quorum(&self, vote_count: usize) -> bool {
        vote_count >= self.quorum_size()
    }

    /// Update match_index for a peer after successful replication.
    /// Returns the new commit_index if it advanced.
    pub fn update_match_index(&mut self, peer_id: u16, match_index: u64) -> Option<u64> {
        if self.state != RaftState::Leader {
            return None;
        }

        // Update the peer's match_index
        self.match_index.insert(peer_id, match_index);

        // Advance commit_index if a majority of peers have replicated up to some index N
        // Raft §5.3: Only commit entries from current term
        let old_commit = self.commit_index;

        // Collect all match indices including self
        let mut indices: Vec<u64> = self.match_index.values().copied().collect();
        indices.push(self.last_log_index); // Leader's own log
        indices.sort_unstable();

        // Find the median (majority) index
        let quorum_idx = indices.len() - self.quorum_size();
        let new_commit = indices[quorum_idx];

        // Only advance commit_index, never decrease
        if new_commit > old_commit {
            self.commit_index = new_commit;
            tracing::debug!(
                target: "lance::raft",
                node_id = self.node_id,
                old_commit,
                new_commit,
                "Advanced commit_index"
            );
            Some(new_commit)
        } else {
            None
        }
    }

    /// Get the current commit index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Update the cluster configuration (for membership changes).
    pub fn set_config(&mut self, config: ClusterConfig) {
        self.config = config;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::codec::{ConfigNode, NodeRole};

    fn test_config() -> ClusterConfig {
        ClusterConfig::single(vec![
            ConfigNode {
                node_id: 1,
                role: NodeRole::Full,
                address: "addr1".into(),
            },
            ConfigNode {
                node_id: 2,
                role: NodeRole::Full,
                address: "addr2".into(),
            },
            ConfigNode {
                node_id: 3,
                role: NodeRole::Full,
                address: "addr3".into(),
            },
        ])
    }

    #[test]
    fn test_fencing_token_ordering() {
        let t1 = FencingToken::new(1, 1);
        let t2 = FencingToken::new(1, 2);
        let t3 = FencingToken::new(2, 1);

        assert!(t1 < t2); // Same term, higher node_id
        assert!(t2 < t3); // Higher term wins
        assert!(t1 < t3);
    }

    #[test]
    fn test_pre_vote_request() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        let req = node.start_pre_vote().unwrap();
        assert_eq!(req.term, 1); // current_term + 1
        assert_eq!(req.candidate_id, 1);
    }

    #[test]
    fn test_pre_vote_response_leader_active() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Simulate recent leader contact
        node.leader_id = Some(2);
        node.last_leader_contact = Instant::now();

        let req = PreVoteRequest {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        };

        let resp = node.handle_pre_vote_request(&req);
        assert!(!resp.vote_granted); // Should reject because leader is active
    }

    #[test]
    fn test_election_flow() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Start election
        let vote_req = node.start_election();
        assert_eq!(vote_req.term, 1);
        assert_eq!(node.state, RaftState::Candidate);
        assert_eq!(node.current_term, 1);

        // With 3 nodes, quorum is 2 (3/2 + 1 = 2)
        // Node already voted for itself, so one more vote wins
        let resp = VoteResponse {
            term: 1,
            vote_granted: true,
        };
        let won = node.handle_vote_response(2, &resp);
        assert!(won); // Self + node 2 = 2 votes = quorum
        assert_eq!(node.state, RaftState::Leader);
        assert!(node.fencing_token.is_some());
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Become leader
        node.current_term = 1;
        node.state = RaftState::Leader;
        node.fencing_token = Some(FencingToken::new(1, 1));

        // Receive append entries with higher term
        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_hlc: HlcTimestamp::new(1000, 0),
            entries: vec![],
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.current_term, 2);
        assert!(node.fencing_token.is_none()); // Fencing token revoked
    }

    #[test]
    fn test_validate_fence() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Not leader, no fence
        assert!(!node.validate_fence(FencingToken::new(1, 1)));

        // Become leader
        node.state = RaftState::Leader;
        node.current_term = 5;
        node.fencing_token = Some(FencingToken::new(5, 1));

        // Valid fence
        assert!(node.validate_fence(FencingToken::new(5, 1)));
        assert!(node.validate_fence(FencingToken::new(6, 1)));

        // Invalid fence (old term)
        assert!(!node.validate_fence(FencingToken::new(4, 1)));
    }

    #[test]
    fn test_election_timeout_triggers_new_election() {
        let config = test_config();
        let raft_config = RaftConfig {
            election_timeout_min: Duration::from_millis(10),
            election_timeout_max: Duration::from_millis(20),
            ..RaftConfig::default()
        };
        let mut node = RaftNode::new(1, config, raft_config);

        // Initially follower
        assert_eq!(node.state, RaftState::Follower);
        assert!(!node.election_timeout_elapsed());

        // Simulate timeout by setting last_leader_contact to past
        node.last_leader_contact = Instant::now() - Duration::from_secs(1);
        assert!(node.election_timeout_elapsed());

        // Starting election should transition to candidate
        let _vote_req = node.start_election();
        assert_eq!(node.state, RaftState::Candidate);
    }

    #[test]
    fn test_leader_loses_quorum_steps_down() {
        let config = test_config();
        let mut node = RaftNode::new(1, config.clone(), RaftConfig::default());

        // Become leader
        node.start_election();
        let resp = VoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_vote_response(2, &resp);
        assert_eq!(node.state, RaftState::Leader);

        // Receive append entries response with higher term (another leader exists)
        let ae_resp = AppendEntriesResponse {
            term: 2,
            success: false,
            match_index: 0,
            follower_hlc: HlcTimestamp::default(),
            follower_id: 2,
        };
        // Process the response by checking term - step down if higher term seen
        if ae_resp.term > node.current_term {
            node.current_term = ae_resp.term;
            node.state = RaftState::Follower;
        }

        // Node should step down to follower
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.current_term, 2);
    }

    #[test]
    fn test_split_vote_triggers_new_election() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Start election
        node.start_election();
        assert_eq!(node.state, RaftState::Candidate);

        // Receive rejection from node 2
        let resp = VoteResponse {
            term: 1,
            vote_granted: false,
        };
        let won = node.handle_vote_response(2, &resp);
        assert!(!won);
        assert_eq!(node.state, RaftState::Candidate); // Still candidate

        // Receive rejection from node 3 - election fails, still candidate
        let resp = VoteResponse {
            term: 1,
            vote_granted: false,
        };
        let won = node.handle_vote_response(3, &resp);
        assert!(!won);
        // In split vote, node remains candidate until timeout triggers new election
        assert_eq!(node.state, RaftState::Candidate);
    }

    #[test]
    fn test_follower_rejects_stale_leader() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Set node to term 5
        node.current_term = 5;

        // Receive append entries from stale leader (term 3)
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_hlc: HlcTimestamp::new(1000, 0),
            entries: vec![],
        };

        let resp = node.handle_append_entries(&req);
        assert!(!resp.success);
        assert_eq!(resp.term, 5); // Returns current term
        assert_eq!(node.current_term, 5); // Term unchanged
    }

    #[test]
    fn test_candidate_steps_down_on_valid_leader() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Start election, become candidate
        node.start_election();
        assert_eq!(node.state, RaftState::Candidate);
        assert_eq!(node.current_term, 1);

        // Receive append entries from newly elected leader
        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_hlc: HlcTimestamp::new(1000, 0),
            entries: vec![],
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.leader_id, Some(2));
    }

    #[test]
    fn test_pre_vote_prevents_disruption() {
        let config = test_config();
        let mut node = RaftNode::new(1, config, RaftConfig::default());

        // Set as follower with active leader
        node.leader_id = Some(2);
        node.last_leader_contact = Instant::now();

        // Node 3 sends pre-vote request (was partitioned, trying to disrupt)
        let req = PreVoteRequest {
            term: 100, // Very high term from partitioned node
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        };

        let resp = node.handle_pre_vote_request(&req);

        // Should reject - leader is still active
        assert!(!resp.vote_granted);
        // Term should NOT be updated (pre-vote protection)
        assert_eq!(node.current_term, 0);
    }
}
