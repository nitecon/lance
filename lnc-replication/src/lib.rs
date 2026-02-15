#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod actor;
mod audit;
mod cluster;
mod codec;
mod discovery;
mod follower;
mod forward;
#[cfg(target_os = "linux")]
mod ingestion;
mod log_store;
mod mode;
mod peer;
mod quorum;
mod raft;
mod resync;
mod segment;

pub mod schema;

pub use actor::{ReplicationActor, create_replication_channel};
pub use audit::{AuditConfig, AuditEntryHeader, AuditError, AuditLogWriter, AuditOperation};
pub use cluster::{ClusterCoordinator, ClusterEvent};
pub use codec::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, ConfigNode, DataReplicationEntry,
    EntryType, InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, MessageType, NodeRole,
    PreVoteRequest, PreVoteResponse, ReplicationAck, ReplicationAckStatus, ReplicationCodec,
    ReplicationFlags, ReplicationMessage, TopicOperation, VoteRequest, VoteResponse,
};
pub use discovery::{
    ClusterConfig as DiscoveryClusterConfig, DiscoveryMethod, PeerDiscovery, PeerInfo,
    parse_node_id_from_hostname, parse_peer_node_id, resolve_node_id, validate_node_id_consistency,
};
pub use follower::{FollowerHealth, FollowerStatus};
pub use forward::{
    ForwardBufferPool, ForwardConfig, ForwardError, LeaderConnectionPool, LocalWriteError,
    LocalWriteProcessor, NoOpLocalProcessor, PooledForwardBuffer, TeeForwardingStatus,
    check_tee_support, create_leader_pool,
};
#[cfg(target_os = "linux")]
pub use ingestion::IngestionHandler;
pub use log_store::{LogStore, PersistentState};
pub use mode::ReplicationMode;
pub use peer::{PeerConfig, PeerConnection, PeerManager, PeerState};
pub use quorum::{AsyncQuorumManager, QuorumConfig, QuorumResult};
pub use raft::{FencingToken, RaftConfig, RaftNode, RaftState};
pub use resync::{
    GapAnalysis, ResyncActor, ResyncConfig, ResyncMessageType, ResyncProgress, ResyncServer,
    ResyncState, SegmentChunk, SegmentDescriptor, SegmentManifest, detect_gap,
};
