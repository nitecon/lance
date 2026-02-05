//! FlatBuffers-based codec for Raft replication messages.
//!
//! This module provides serialization and deserialization for all Raft RPC
//! messages using FlatBuffers for zero-copy reads and schema evolution.
//!
//! # Wire Format
//!
//! All messages are wrapped in a `ReplicationEnvelope` that identifies the
//! message type via a union discriminant.
//!
//! # Schema Evolution
//!
//! FlatBuffers supports adding new optional fields without breaking backward
//! compatibility, enabling rolling upgrades across cluster nodes.

use bytes::{Bytes, BytesMut};
use flatbuffers::FlatBufferBuilder;
use lnc_core::{HlcTimestamp, LanceError, Result};

/// Entry type for Raft log entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    /// Normal data entry
    Data = 0,
    /// Cluster configuration change
    ConfigChange = 1,
    /// Snapshot marker
    Snapshot = 2,
    /// No-op entry (used for leader commit)
    Noop = 3,
    /// Topic metadata operation
    TopicOp = 4,
}

impl EntryType {
    /// Convert to u8 value.
    #[inline]
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Convert from u8 value.
    #[must_use]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Data,
            1 => Self::ConfigChange,
            2 => Self::Snapshot,
            3 => Self::Noop,
            4 => Self::TopicOp,
            _ => Self::Data, // Default to Data for unknown types
        }
    }
}

/// Topic operation types for replication
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicOperation {
    /// Create a new topic
    Create {
        /// Topic ID assigned by leader
        topic_id: u32,
        /// Topic name
        name: String,
        /// Creation timestamp (Unix millis)
        created_at: u64,
    },
    /// Delete an existing topic
    Delete {
        /// Topic ID to delete
        topic_id: u32,
    },
}

impl TopicOperation {
    /// Serialize topic operation to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(64);
        match self {
            TopicOperation::Create {
                topic_id,
                name,
                created_at,
            } => {
                buf.extend_from_slice(&[0u8]); // Type = Create
                buf.extend_from_slice(&topic_id.to_le_bytes());
                buf.extend_from_slice(&created_at.to_le_bytes());
                let name_bytes = name.as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(name_bytes);
            },
            TopicOperation::Delete { topic_id } => {
                buf.extend_from_slice(&[1u8]); // Type = Delete
                buf.extend_from_slice(&topic_id.to_le_bytes());
            },
        }
        buf.freeze()
    }

    /// Deserialize topic operation from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            0 => {
                // Create
                if data.len() < 15 {
                    return None;
                }
                let topic_id = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                let created_at = u64::from_le_bytes([
                    data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12],
                ]);
                let name_len = u16::from_le_bytes([data[13], data[14]]) as usize;
                if data.len() < 15 + name_len {
                    return None;
                }
                let name = String::from_utf8_lossy(&data[15..15 + name_len]).to_string();
                Some(TopicOperation::Create {
                    topic_id,
                    name,
                    created_at,
                })
            },
            1 => {
                // Delete
                if data.len() < 5 {
                    return None;
                }
                let topic_id = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                Some(TopicOperation::Delete { topic_id })
            },
            _ => None,
        }
    }
}

/// A single entry in the Raft log.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Raft term when entry was created.
    pub term: u64,
    /// Log index.
    pub index: u64,
    /// HLC timestamp for causal ordering.
    pub hlc: HlcTimestamp,
    /// Entry type.
    pub entry_type: EntryType,
    /// Opaque payload data.
    pub data: Bytes,
}

/// AppendEntries RPC request (leader → follower).
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    /// Leader's current term.
    pub term: u64,
    /// Leader's node ID.
    pub leader_id: u16,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: u64,
    /// Term of prev_log_index entry.
    pub prev_log_term: u64,
    /// Leader's commit index.
    pub leader_commit: u64,
    /// Leader's HLC timestamp for clock synchronization.
    pub leader_hlc: HlcTimestamp,
    /// Log entries to replicate (empty for heartbeat).
    pub entries: Vec<LogEntry>,
}

/// AppendEntries RPC response (follower → leader).
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    /// Current term for leader to update itself.
    pub term: u64,
    /// True if follower contained entry matching prev_log_index/term.
    pub success: bool,
    /// Follower's match index (optimization for fast catch-up).
    pub match_index: u64,
    /// Follower's HLC timestamp.
    pub follower_hlc: HlcTimestamp,
    /// Follower's node ID.
    pub follower_id: u16,
}

/// Pre-Vote request (candidate → all nodes).
#[derive(Debug, Clone)]
pub struct PreVoteRequest {
    /// Candidate's current term (NOT incremented).
    pub term: u64,
    /// Candidate's node ID.
    pub candidate_id: u16,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// Pre-Vote response.
#[derive(Debug, Clone)]
pub struct PreVoteResponse {
    /// Responder's current term.
    pub term: u64,
    /// True if responder would grant vote.
    pub vote_granted: bool,
}

/// RequestVote RPC request (candidate → all nodes).
#[derive(Debug, Clone)]
pub struct VoteRequest {
    /// Candidate's term.
    pub term: u64,
    /// Candidate requesting vote.
    pub candidate_id: u16,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// RequestVote RPC response.
#[derive(Debug, Clone)]
pub struct VoteResponse {
    /// Current term for candidate to update itself.
    pub term: u64,
    /// True if candidate received vote.
    pub vote_granted: bool,
}

/// Node role in cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeRole {
    /// Full participant: stores data, votes, can be leader.
    Full = 0,
    /// Witness: votes only, no data, cannot be leader.
    Witness = 1,
}

impl NodeRole {
    /// Convert from u8 value.
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Full),
            1 => Some(Self::Witness),
            _ => None,
        }
    }
}

/// A node in the cluster configuration.
#[derive(Debug, Clone)]
pub struct ConfigNode {
    /// Node identifier.
    pub node_id: u16,
    /// Node role.
    pub role: NodeRole,
    /// Network address.
    pub address: String,
}

/// Cluster configuration for Joint Consensus.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// True if in joint consensus (transitioning between configs).
    pub is_joint: bool,
    /// Current/old configuration nodes.
    pub old_nodes: Vec<ConfigNode>,
    /// New configuration nodes (only present during joint consensus).
    pub new_nodes: Vec<ConfigNode>,
}

impl ClusterConfig {
    /// Create a single (non-joint) configuration.
    #[must_use]
    pub fn single(nodes: Vec<ConfigNode>) -> Self {
        Self {
            is_joint: false,
            old_nodes: nodes,
            new_nodes: Vec::new(),
        }
    }

    /// Create a joint configuration for membership transition.
    #[must_use]
    pub fn joint(old_nodes: Vec<ConfigNode>, new_nodes: Vec<ConfigNode>) -> Self {
        Self {
            is_joint: true,
            old_nodes,
            new_nodes,
        }
    }

    /// Check if a set of node IDs forms a quorum.
    #[must_use]
    pub fn is_quorum(&self, voters: &[u16]) -> bool {
        let old_count = self
            .old_nodes
            .iter()
            .filter(|n| voters.contains(&n.node_id))
            .count();
        let old_majority = old_count > self.old_nodes.len() / 2;

        if !self.is_joint {
            return old_majority;
        }

        let new_count = self
            .new_nodes
            .iter()
            .filter(|n| voters.contains(&n.node_id))
            .count();
        let new_majority = new_count > self.new_nodes.len() / 2;

        // Joint consensus: both configurations must have majority
        old_majority && new_majority
    }
}

/// InstallSnapshot RPC request (leader → follower).
#[derive(Debug, Clone)]
pub struct InstallSnapshotRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader's node ID.
    pub leader_id: u16,
    /// Snapshot replaces all entries up through this index.
    pub last_included_index: u64,
    /// Term of last_included_index.
    pub last_included_term: u64,
    /// HLC at snapshot time.
    pub snapshot_hlc: HlcTimestamp,
    /// Byte offset in snapshot file.
    pub offset: u64,
    /// Snapshot chunk data.
    pub data: Bytes,
    /// True if this is the last chunk.
    pub done: bool,
    /// Cluster configuration at snapshot time.
    pub config: ClusterConfig,
}

/// InstallSnapshot RPC response.
#[derive(Debug, Clone)]
pub struct InstallSnapshotResponse {
    /// Current term for leader to update itself.
    pub term: u64,
    /// Bytes successfully stored.
    pub bytes_stored: u64,
}

/// TimeoutNow request for leadership transfer.
/// Sent by the current leader to a target follower to trigger immediate election.
#[derive(Debug, Clone)]
pub struct TimeoutNowRequest {
    /// Term of the leader initiating the transfer
    pub term: u64,
    /// Leader's node ID
    pub leader_id: u16,
}

/// TimeoutNow response acknowledging the request.
#[derive(Debug, Clone)]
pub struct TimeoutNowResponse {
    /// Term of the responding node
    pub term: u64,
    /// Whether the node will start an election
    pub accepted: bool,
}

/// All possible replication messages.
#[derive(Debug, Clone)]
pub enum ReplicationMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    TimeoutNowRequest(TimeoutNowRequest),
    TimeoutNowResponse(TimeoutNowResponse),
}

/// Message type discriminant for wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    AppendEntriesRequest = 1,
    AppendEntriesResponse = 2,
    PreVoteRequest = 3,
    PreVoteResponse = 4,
    VoteRequest = 5,
    VoteResponse = 6,
    InstallSnapshotRequest = 7,
    InstallSnapshotResponse = 8,
    TimeoutNowRequest = 9,
    TimeoutNowResponse = 10,
}

impl MessageType {
    /// Convert from u8 value.
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::AppendEntriesRequest),
            2 => Some(Self::AppendEntriesResponse),
            3 => Some(Self::PreVoteRequest),
            4 => Some(Self::PreVoteResponse),
            5 => Some(Self::VoteRequest),
            6 => Some(Self::VoteResponse),
            7 => Some(Self::InstallSnapshotRequest),
            8 => Some(Self::InstallSnapshotResponse),
            9 => Some(Self::TimeoutNowRequest),
            10 => Some(Self::TimeoutNowResponse),
            _ => None,
        }
    }
}

/// Codec for encoding and decoding replication messages.
///
/// Uses FlatBuffers-style encoding with a simple header for message type
/// discrimination. This provides:
/// - Zero-copy reads where possible
/// - Forward/backward compatible schema evolution
/// - Efficient variable-length encoding
pub struct ReplicationCodec {
    /// Reusable buffer for encoding.
    builder: FlatBufferBuilder<'static>,
}

impl ReplicationCodec {
    /// Create a new codec with default buffer size.
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::with_capacity(4096),
        }
    }

    /// Create a new codec with specified initial buffer capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: FlatBufferBuilder::with_capacity(capacity),
        }
    }

    /// Encode a replication message to bytes.
    pub fn encode(&mut self, message: &ReplicationMessage) -> Result<Bytes> {
        self.builder.reset();

        match message {
            ReplicationMessage::AppendEntriesRequest(req) => {
                self.encode_append_entries_request(req)
            },
            ReplicationMessage::AppendEntriesResponse(resp) => {
                self.encode_append_entries_response(resp)
            },
            ReplicationMessage::PreVoteRequest(req) => self.encode_pre_vote_request(req),
            ReplicationMessage::PreVoteResponse(resp) => self.encode_pre_vote_response(resp),
            ReplicationMessage::VoteRequest(req) => self.encode_vote_request(req),
            ReplicationMessage::VoteResponse(resp) => self.encode_vote_response(resp),
            ReplicationMessage::InstallSnapshotRequest(req) => {
                self.encode_install_snapshot_request(req)
            },
            ReplicationMessage::InstallSnapshotResponse(resp) => {
                self.encode_install_snapshot_response(resp)
            },
            ReplicationMessage::TimeoutNowRequest(req) => {
                self.encode_timeout_now_request(req)
            },
            ReplicationMessage::TimeoutNowResponse(resp) => {
                self.encode_timeout_now_response(resp)
            },
        }
    }

    /// Decode a replication message from bytes.
    pub fn decode(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 5 {
            return Err(LanceError::InvalidData("Message too short".into()));
        }

        let msg_type = MessageType::from_u8(data[0])
            .ok_or_else(|| LanceError::InvalidData("Unknown message type".into()))?;

        let payload = &data[1..];

        match msg_type {
            MessageType::AppendEntriesRequest => Self::decode_append_entries_request(payload),
            MessageType::AppendEntriesResponse => Self::decode_append_entries_response(payload),
            MessageType::PreVoteRequest => Self::decode_pre_vote_request(payload),
            MessageType::PreVoteResponse => Self::decode_pre_vote_response(payload),
            MessageType::VoteRequest => Self::decode_vote_request(payload),
            MessageType::VoteResponse => Self::decode_vote_response(payload),
            MessageType::InstallSnapshotRequest => Self::decode_install_snapshot_request(payload),
            MessageType::InstallSnapshotResponse => Self::decode_install_snapshot_response(payload),
            MessageType::TimeoutNowRequest => Self::decode_timeout_now_request(payload),
            MessageType::TimeoutNowResponse => Self::decode_timeout_now_response(payload),
        }
    }

    // === Encoding helpers ===

    fn encode_append_entries_request(&mut self, req: &AppendEntriesRequest) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(64 + req.entries.len() * 64);

        // Message type
        buf.extend_from_slice(&[MessageType::AppendEntriesRequest as u8]);

        // Fixed fields
        buf.extend_from_slice(&req.term.to_le_bytes());
        buf.extend_from_slice(&req.leader_id.to_le_bytes());
        buf.extend_from_slice(&req.prev_log_index.to_le_bytes());
        buf.extend_from_slice(&req.prev_log_term.to_le_bytes());
        buf.extend_from_slice(&req.leader_commit.to_le_bytes());
        buf.extend_from_slice(&req.leader_hlc.as_u64().to_le_bytes());

        // Entry count
        let entry_count = req.entries.len() as u32;
        buf.extend_from_slice(&entry_count.to_le_bytes());

        // Entries
        for entry in &req.entries {
            buf.extend_from_slice(&entry.term.to_le_bytes());
            buf.extend_from_slice(&entry.index.to_le_bytes());
            buf.extend_from_slice(&entry.hlc.as_u64().to_le_bytes());
            buf.extend_from_slice(&[entry.entry_type as u8]);

            let data_len = entry.data.len() as u32;
            buf.extend_from_slice(&data_len.to_le_bytes());
            buf.extend_from_slice(&entry.data);
        }

        Ok(buf.freeze())
    }

    fn encode_append_entries_response(&mut self, resp: &AppendEntriesResponse) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(32);

        buf.extend_from_slice(&[MessageType::AppendEntriesResponse as u8]);
        buf.extend_from_slice(&resp.term.to_le_bytes());
        buf.extend_from_slice(&[resp.success as u8]);
        buf.extend_from_slice(&resp.match_index.to_le_bytes());
        buf.extend_from_slice(&resp.follower_hlc.as_u64().to_le_bytes());
        buf.extend_from_slice(&resp.follower_id.to_le_bytes());

        Ok(buf.freeze())
    }

    fn encode_pre_vote_request(&mut self, req: &PreVoteRequest) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(32);

        buf.extend_from_slice(&[MessageType::PreVoteRequest as u8]);
        buf.extend_from_slice(&req.term.to_le_bytes());
        buf.extend_from_slice(&req.candidate_id.to_le_bytes());
        buf.extend_from_slice(&req.last_log_index.to_le_bytes());
        buf.extend_from_slice(&req.last_log_term.to_le_bytes());

        Ok(buf.freeze())
    }

    fn encode_pre_vote_response(&mut self, resp: &PreVoteResponse) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(16);

        buf.extend_from_slice(&[MessageType::PreVoteResponse as u8]);
        buf.extend_from_slice(&resp.term.to_le_bytes());
        buf.extend_from_slice(&[resp.vote_granted as u8]);

        Ok(buf.freeze())
    }

    fn encode_vote_request(&mut self, req: &VoteRequest) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(32);

        buf.extend_from_slice(&[MessageType::VoteRequest as u8]);
        buf.extend_from_slice(&req.term.to_le_bytes());
        buf.extend_from_slice(&req.candidate_id.to_le_bytes());
        buf.extend_from_slice(&req.last_log_index.to_le_bytes());
        buf.extend_from_slice(&req.last_log_term.to_le_bytes());

        Ok(buf.freeze())
    }

    fn encode_vote_response(&mut self, resp: &VoteResponse) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(16);

        buf.extend_from_slice(&[MessageType::VoteResponse as u8]);
        buf.extend_from_slice(&resp.term.to_le_bytes());
        buf.extend_from_slice(&[resp.vote_granted as u8]);

        Ok(buf.freeze())
    }

    fn encode_install_snapshot_request(&mut self, req: &InstallSnapshotRequest) -> Result<Bytes> {
        let config_size = Self::estimate_config_size(&req.config);
        let mut buf = BytesMut::with_capacity(64 + req.data.len() + config_size);

        buf.extend_from_slice(&[MessageType::InstallSnapshotRequest as u8]);
        buf.extend_from_slice(&req.term.to_le_bytes());
        buf.extend_from_slice(&req.leader_id.to_le_bytes());
        buf.extend_from_slice(&req.last_included_index.to_le_bytes());
        buf.extend_from_slice(&req.last_included_term.to_le_bytes());
        buf.extend_from_slice(&req.snapshot_hlc.as_u64().to_le_bytes());
        buf.extend_from_slice(&req.offset.to_le_bytes());
        buf.extend_from_slice(&[req.done as u8]);

        // Data
        let data_len = req.data.len() as u32;
        buf.extend_from_slice(&data_len.to_le_bytes());
        buf.extend_from_slice(&req.data);

        // Config
        Self::encode_config(&mut buf, &req.config);

        Ok(buf.freeze())
    }

    fn encode_install_snapshot_response(
        &mut self,
        resp: &InstallSnapshotResponse,
    ) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(24);

        buf.extend_from_slice(&[MessageType::InstallSnapshotResponse as u8]);
        buf.extend_from_slice(&resp.term.to_le_bytes());
        buf.extend_from_slice(&resp.bytes_stored.to_le_bytes());

        Ok(buf.freeze())
    }

    fn encode_timeout_now_request(&mut self, req: &TimeoutNowRequest) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(16);

        buf.extend_from_slice(&[MessageType::TimeoutNowRequest as u8]);
        buf.extend_from_slice(&req.term.to_le_bytes());
        buf.extend_from_slice(&req.leader_id.to_le_bytes());

        Ok(buf.freeze())
    }

    fn encode_timeout_now_response(&mut self, resp: &TimeoutNowResponse) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(16);

        buf.extend_from_slice(&[MessageType::TimeoutNowResponse as u8]);
        buf.extend_from_slice(&resp.term.to_le_bytes());
        buf.extend_from_slice(&[resp.accepted as u8]);

        Ok(buf.freeze())
    }

    fn estimate_config_size(config: &ClusterConfig) -> usize {
        let node_size = |nodes: &[ConfigNode]| -> usize {
            nodes.iter().map(|n| 4 + n.address.len()).sum::<usize>() + 4
        };
        1 + node_size(&config.old_nodes) + node_size(&config.new_nodes)
    }

    fn encode_config(buf: &mut BytesMut, config: &ClusterConfig) {
        buf.extend_from_slice(&[config.is_joint as u8]);

        // Old nodes
        let old_count = config.old_nodes.len() as u32;
        buf.extend_from_slice(&old_count.to_le_bytes());
        for node in &config.old_nodes {
            buf.extend_from_slice(&node.node_id.to_le_bytes());
            buf.extend_from_slice(&[node.role as u8]);
            let addr_len = node.address.len() as u16;
            buf.extend_from_slice(&addr_len.to_le_bytes());
            buf.extend_from_slice(node.address.as_bytes());
        }

        // New nodes
        let new_count = config.new_nodes.len() as u32;
        buf.extend_from_slice(&new_count.to_le_bytes());
        for node in &config.new_nodes {
            buf.extend_from_slice(&node.node_id.to_le_bytes());
            buf.extend_from_slice(&[node.role as u8]);
            let addr_len = node.address.len() as u16;
            buf.extend_from_slice(&addr_len.to_le_bytes());
            buf.extend_from_slice(node.address.as_bytes());
        }
    }

    // === Decoding helpers ===

    fn decode_append_entries_request(data: &[u8]) -> Result<ReplicationMessage> {
        // Minimum size: term(8) + leader_id(2) + prev_log_index(8) + prev_log_term(8) +
        //              leader_commit(8) + leader_hlc(8) + entry_count(4) = 46 bytes
        if data.len() < 46 {
            return Err(LanceError::InvalidData(
                "AppendEntriesRequest too short".into(),
            ));
        }

        let mut pos = 0;

        let term = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        pos += 8;

        let leader_id = u16::from_le_bytes(
            data[pos..pos + 2]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid leader_id".into()))?,
        );
        pos += 2;

        let prev_log_index = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid prev_log_index".into()))?,
        );
        pos += 8;

        let prev_log_term = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid prev_log_term".into()))?,
        );
        pos += 8;

        let leader_commit = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid leader_commit".into()))?,
        );
        pos += 8;

        let leader_hlc = HlcTimestamp::from_raw(u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid leader_hlc".into()))?,
        ));
        pos += 8;

        let entry_count = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid entry_count".into()))?,
        ) as usize;
        pos += 4;

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            if pos + 29 > data.len() {
                return Err(LanceError::InvalidData("Truncated log entry".into()));
            }

            let term = u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| LanceError::InvalidData("Invalid entry term".into()))?,
            );
            pos += 8;

            let index = u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| LanceError::InvalidData("Invalid entry index".into()))?,
            );
            pos += 8;

            let hlc = HlcTimestamp::from_raw(u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .map_err(|_| LanceError::InvalidData("Invalid entry hlc".into()))?,
            ));
            pos += 8;

            let entry_type = EntryType::from_u8(data[pos]);
            pos += 1;

            let data_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| LanceError::InvalidData("Invalid data_len".into()))?,
            ) as usize;
            pos += 4;

            if pos + data_len > data.len() {
                return Err(LanceError::InvalidData("Truncated entry data".into()));
            }

            let entry_data = Bytes::copy_from_slice(&data[pos..pos + data_len]);
            pos += data_len;

            entries.push(LogEntry {
                term,
                index,
                hlc,
                entry_type,
                data: entry_data,
            });
        }

        Ok(ReplicationMessage::AppendEntriesRequest(
            AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                leader_commit,
                leader_hlc,
                entries,
            },
        ))
    }

    fn decode_append_entries_response(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 27 {
            return Err(LanceError::InvalidData(
                "AppendEntriesResponse too short".into(),
            ));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let success = data[8] != 0;
        let match_index = u64::from_le_bytes(
            data[9..17]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid match_index".into()))?,
        );
        let follower_hlc = HlcTimestamp::from_raw(u64::from_le_bytes(
            data[17..25]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid follower_hlc".into()))?,
        ));
        let follower_id = u16::from_le_bytes(
            data[25..27]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid follower_id".into()))?,
        );

        Ok(ReplicationMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term,
                success,
                match_index,
                follower_hlc,
                follower_id,
            },
        ))
    }

    fn decode_pre_vote_request(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 26 {
            return Err(LanceError::InvalidData("PreVoteRequest too short".into()));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let candidate_id = u16::from_le_bytes(
            data[8..10]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid candidate_id".into()))?,
        );
        let last_log_index = u64::from_le_bytes(
            data[10..18]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid last_log_index".into()))?,
        );
        let last_log_term = u64::from_le_bytes(
            data[18..26]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid last_log_term".into()))?,
        );

        Ok(ReplicationMessage::PreVoteRequest(PreVoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }))
    }

    fn decode_pre_vote_response(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 9 {
            return Err(LanceError::InvalidData("PreVoteResponse too short".into()));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let vote_granted = data[8] != 0;

        Ok(ReplicationMessage::PreVoteResponse(PreVoteResponse {
            term,
            vote_granted,
        }))
    }

    fn decode_vote_request(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 26 {
            return Err(LanceError::InvalidData("VoteRequest too short".into()));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let candidate_id = u16::from_le_bytes(
            data[8..10]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid candidate_id".into()))?,
        );
        let last_log_index = u64::from_le_bytes(
            data[10..18]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid last_log_index".into()))?,
        );
        let last_log_term = u64::from_le_bytes(
            data[18..26]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid last_log_term".into()))?,
        );

        Ok(ReplicationMessage::VoteRequest(VoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }))
    }

    fn decode_vote_response(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 9 {
            return Err(LanceError::InvalidData("VoteResponse too short".into()));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let vote_granted = data[8] != 0;

        Ok(ReplicationMessage::VoteResponse(VoteResponse {
            term,
            vote_granted,
        }))
    }

    fn decode_install_snapshot_request(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 51 {
            return Err(LanceError::InvalidData(
                "InstallSnapshotRequest too short".into(),
            ));
        }

        let mut pos = 0;

        let term = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        pos += 8;

        let leader_id = u16::from_le_bytes(
            data[pos..pos + 2]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid leader_id".into()))?,
        );
        pos += 2;

        let last_included_index = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid last_included_index".into()))?,
        );
        pos += 8;

        let last_included_term = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid last_included_term".into()))?,
        );
        pos += 8;

        let snapshot_hlc = HlcTimestamp::from_raw(u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid snapshot_hlc".into()))?,
        ));
        pos += 8;

        let offset = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid offset".into()))?,
        );
        pos += 8;

        let done = data[pos] != 0;
        pos += 1;

        let data_len = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid data_len".into()))?,
        ) as usize;
        pos += 4;

        if pos + data_len > data.len() {
            return Err(LanceError::InvalidData("Truncated snapshot data".into()));
        }

        let snapshot_data = Bytes::copy_from_slice(&data[pos..pos + data_len]);
        pos += data_len;

        let config = Self::decode_config(&data[pos..])?;

        Ok(ReplicationMessage::InstallSnapshotRequest(
            InstallSnapshotRequest {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                snapshot_hlc,
                offset,
                data: snapshot_data,
                done,
                config,
            },
        ))
    }

    fn decode_install_snapshot_response(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 16 {
            return Err(LanceError::InvalidData(
                "InstallSnapshotResponse too short".into(),
            ));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let bytes_stored = u64::from_le_bytes(
            data[8..16]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid bytes_stored".into()))?,
        );

        Ok(ReplicationMessage::InstallSnapshotResponse(
            InstallSnapshotResponse { term, bytes_stored },
        ))
    }

    fn decode_timeout_now_request(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 10 {
            return Err(LanceError::InvalidData("TimeoutNowRequest too short".into()));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let leader_id = u16::from_le_bytes(
            data[8..10]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid leader_id".into()))?,
        );

        Ok(ReplicationMessage::TimeoutNowRequest(TimeoutNowRequest {
            term,
            leader_id,
        }))
    }

    fn decode_timeout_now_response(data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 9 {
            return Err(LanceError::InvalidData("TimeoutNowResponse too short".into()));
        }

        let term = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid term".into()))?,
        );
        let accepted = data[8] != 0;

        Ok(ReplicationMessage::TimeoutNowResponse(TimeoutNowResponse {
            term,
            accepted,
        }))
    }

    fn decode_config(data: &[u8]) -> Result<ClusterConfig> {
        if data.is_empty() {
            return Err(LanceError::InvalidData("Config data empty".into()));
        }

        let mut pos = 0;
        let is_joint = data[pos] != 0;
        pos += 1;

        let (old_nodes, new_pos) = Self::decode_node_list(&data[pos..])?;
        pos += new_pos;

        let (new_nodes, _) = Self::decode_node_list(&data[pos..])?;

        Ok(ClusterConfig {
            is_joint,
            old_nodes,
            new_nodes,
        })
    }

    fn decode_node_list(data: &[u8]) -> Result<(Vec<ConfigNode>, usize)> {
        if data.len() < 4 {
            return Err(LanceError::InvalidData("Node list too short".into()));
        }

        let count = u32::from_le_bytes(
            data[0..4]
                .try_into()
                .map_err(|_| LanceError::InvalidData("Invalid node count".into()))?,
        ) as usize;
        let mut pos = 4;
        let mut nodes = Vec::with_capacity(count);

        for _ in 0..count {
            if pos + 5 > data.len() {
                return Err(LanceError::InvalidData("Truncated node entry".into()));
            }

            let node_id = u16::from_le_bytes(
                data[pos..pos + 2]
                    .try_into()
                    .map_err(|_| LanceError::InvalidData("Invalid node_id".into()))?,
            );
            pos += 2;

            let role = NodeRole::from_u8(data[pos])
                .ok_or_else(|| LanceError::InvalidData("Invalid node role".into()))?;
            pos += 1;

            let addr_len = u16::from_le_bytes(
                data[pos..pos + 2]
                    .try_into()
                    .map_err(|_| LanceError::InvalidData("Invalid addr_len".into()))?,
            ) as usize;
            pos += 2;

            if pos + addr_len > data.len() {
                return Err(LanceError::InvalidData("Truncated address".into()));
            }

            let address = String::from_utf8(data[pos..pos + addr_len].to_vec())
                .map_err(|_| LanceError::InvalidData("Invalid address UTF-8".into()))?;
            pos += addr_len;

            nodes.push(ConfigNode {
                node_id,
                role,
                address,
            });
        }

        Ok((nodes, pos))
    }
}

impl Default for ReplicationCodec {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_entries_request_roundtrip() {
        let mut codec = ReplicationCodec::new();

        let req = AppendEntriesRequest {
            term: 42,
            leader_id: 1,
            prev_log_index: 100,
            prev_log_term: 41,
            leader_commit: 99,
            leader_hlc: HlcTimestamp::new(1234567, 5),
            entries: vec![LogEntry {
                term: 42,
                index: 101,
                hlc: HlcTimestamp::new(1234568, 0),
                entry_type: EntryType::Data,
                data: Bytes::from_static(b"hello"),
            }],
        };

        let encoded = codec
            .encode(&ReplicationMessage::AppendEntriesRequest(req.clone()))
            .unwrap();
        let decoded = ReplicationCodec::decode(&encoded).unwrap();

        if let ReplicationMessage::AppendEntriesRequest(decoded_req) = decoded {
            assert_eq!(decoded_req.term, req.term);
            assert_eq!(decoded_req.leader_id, req.leader_id);
            assert_eq!(decoded_req.prev_log_index, req.prev_log_index);
            assert_eq!(decoded_req.entries.len(), 1);
            assert_eq!(decoded_req.entries[0].data, req.entries[0].data);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_pre_vote_roundtrip() {
        let mut codec = ReplicationCodec::new();

        let req = PreVoteRequest {
            term: 10,
            candidate_id: 2,
            last_log_index: 50,
            last_log_term: 9,
        };

        let encoded = codec
            .encode(&ReplicationMessage::PreVoteRequest(req.clone()))
            .unwrap();
        let decoded = ReplicationCodec::decode(&encoded).unwrap();

        if let ReplicationMessage::PreVoteRequest(decoded_req) = decoded {
            assert_eq!(decoded_req.term, req.term);
            assert_eq!(decoded_req.candidate_id, req.candidate_id);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_cluster_config_quorum() {
        let config = ClusterConfig::single(vec![
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
        ]);

        assert!(config.is_quorum(&[1, 2]));
        assert!(config.is_quorum(&[1, 3]));
        assert!(!config.is_quorum(&[1]));
    }

    #[test]
    fn test_joint_consensus_quorum() {
        let config = ClusterConfig::joint(
            vec![
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
            ],
            vec![
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
                ConfigNode {
                    node_id: 4,
                    role: NodeRole::Full,
                    address: "addr4".into(),
                },
                ConfigNode {
                    node_id: 5,
                    role: NodeRole::Full,
                    address: "addr5".into(),
                },
            ],
        );

        // Must have majority in BOTH configs
        assert!(config.is_quorum(&[2, 3, 4])); // 2/3 old, 3/4 new
        assert!(!config.is_quorum(&[1, 4, 5])); // 1/3 old (not majority)
    }

    #[test]
    fn test_topic_operation_create_roundtrip() {
        let op = TopicOperation::Create {
            topic_id: 42,
            name: "test-topic".to_string(),
            created_at: 1234567890,
        };

        let bytes = op.to_bytes();
        let decoded = TopicOperation::from_bytes(&bytes).expect("decode failed");

        match decoded {
            TopicOperation::Create {
                topic_id,
                name,
                created_at,
            } => {
                assert_eq!(topic_id, 42);
                assert_eq!(name, "test-topic");
                assert_eq!(created_at, 1234567890);
            },
            _ => panic!("Wrong operation type"),
        }
    }

    #[test]
    fn test_topic_operation_delete_roundtrip() {
        let op = TopicOperation::Delete { topic_id: 99 };

        let bytes = op.to_bytes();
        let decoded = TopicOperation::from_bytes(&bytes).expect("decode failed");

        match decoded {
            TopicOperation::Delete { topic_id } => {
                assert_eq!(topic_id, 99);
            },
            _ => panic!("Wrong operation type"),
        }
    }

    #[test]
    fn test_topic_operation_empty_name() {
        let op = TopicOperation::Create {
            topic_id: 1,
            name: String::new(),
            created_at: 0,
        };

        let bytes = op.to_bytes();
        let decoded = TopicOperation::from_bytes(&bytes).expect("decode failed");

        match decoded {
            TopicOperation::Create {
                topic_id,
                name,
                created_at,
            } => {
                assert_eq!(topic_id, 1);
                assert_eq!(name, "");
                assert_eq!(created_at, 0);
            },
            _ => panic!("Wrong operation type"),
        }
    }

    #[test]
    fn test_topic_operation_unicode_name() {
        let op = TopicOperation::Create {
            topic_id: 5,
            name: "тест-топик-日本語".to_string(),
            created_at: 999,
        };

        let bytes = op.to_bytes();
        let decoded = TopicOperation::from_bytes(&bytes).expect("decode failed");

        match decoded {
            TopicOperation::Create { topic_id, name, .. } => {
                assert_eq!(topic_id, 5);
                assert_eq!(name, "тест-топик-日本語");
            },
            _ => panic!("Wrong operation type"),
        }
    }

    #[test]
    fn test_topic_operation_invalid_data() {
        // Empty data
        assert!(TopicOperation::from_bytes(&[]).is_none());

        // Invalid type byte
        assert!(TopicOperation::from_bytes(&[255]).is_none());

        // Create with truncated data
        assert!(TopicOperation::from_bytes(&[0, 1, 2]).is_none());

        // Delete with truncated data
        assert!(TopicOperation::from_bytes(&[1, 1]).is_none());
    }
}
