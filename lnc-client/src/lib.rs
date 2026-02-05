#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod client;
pub mod connection;
mod consumer;
mod error;
pub mod grouped;
pub mod offset;
pub mod producer;
pub mod record;
pub mod standalone;
pub mod tls;

pub use client::{
    AuthConfig, ClientConfig, ClientStream, CommitResult, FetchResult, LanceClient, SubscribeResult, TopicInfo,
};
pub use consumer::{
    Consumer, ConsumerConfig, PollResult, SeekPosition, StreamingConsumer, StreamingConsumerConfig,
};
pub use error::{parse_not_leader_error, ClientError};
pub use grouped::{
    AssignmentStrategy, GroupConfig, GroupCoordinator, GroupedConsumer, WorkerConfig,
};
pub use offset::{LockFileOffsetStore, MemoryOffsetStore, OffsetStore, CommitInfo, PostCommitHook, HookedOffsetStore, LoggingCommitHook, CollectingCommitHook};
pub use producer::{Producer, ProducerConfig, ProducerMetrics, MetricsSnapshot, SendAck};
pub use standalone::{StandaloneConfig, StandaloneConsumer, StandaloneConsumerBuilder};
pub use tls::TlsClientConfig;
pub use connection::{ConnectionPool, ConnectionPoolConfig, PoolStats, PooledClient, ReconnectingClient, ClusterClient};
pub use record::{Record, RecordType, RecordIterator, RecordParserConfig, RecordParseError, parse_records, parse_record, encode_record, TLV_HEADER_SIZE};

#[cfg(test)]
mod tests;
