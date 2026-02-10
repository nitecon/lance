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
mod log_store;
mod mode;
mod peer;
mod quorum;
mod raft;

pub mod schema;

pub use actor::{ReplicationActor, create_replication_channel};
pub use audit::{AuditConfig, AuditEntryHeader, AuditError, AuditLogWriter, AuditOperation};
pub use cluster::{ClusterCoordinator, ClusterEvent, MAX_WARTIME_TERMS, WARTIME_TTL, WartimeEntry};
pub use codec::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, ConfigNode, DataReplicationEntry,
    EntryType, InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, MessageType, NodeRole,
    PreVoteRequest, PreVoteResponse, ReplicationAck, ReplicationAckStatus, ReplicationCodec,
    ReplicationFlags, ReplicationMessage, ResyncBegin, ResyncComplete, SegmentFetchRequest,
    SegmentFetchResponse, SegmentInfo, SegmentManifestRequest, SegmentManifestResponse,
    TopicManifest, TopicOperation, VoteRequest, VoteResponse,
};
pub use discovery::{
    ClusterConfig as DiscoveryClusterConfig, DiscoveryMethod, PeerDiscovery, PeerInfo,
};
pub use follower::{FollowerHealth, FollowerStatus};
pub use forward::{
    ForwardConfig, ForwardError, LeaderConnectionPool, LocalWriteError, LocalWriteProcessor,
    NoOpLocalProcessor, TeeForwardingStatus, check_tee_support, create_leader_pool,
};
pub use log_store::{LogStore, PersistentState};
pub use mode::ReplicationMode;
pub use peer::{PeerConfig, PeerConnection, PeerManager, PeerState};
pub use quorum::{AsyncQuorumManager, QuorumConfig, QuorumResult};
pub use raft::{FencingToken, RaftConfig, RaftNode, RaftState};
