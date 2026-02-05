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
    AuthConfig, ClientConfig, ClientStream, CommitResult, FetchResult, LanceClient,
    SubscribeResult, TopicInfo,
};
pub use connection::{
    ClusterClient, ConnectionPool, ConnectionPoolConfig, PoolStats, PooledClient,
    ReconnectingClient,
};
pub use consumer::{
    Consumer, ConsumerConfig, PollResult, SeekPosition, StreamingConsumer, StreamingConsumerConfig,
};
pub use error::{ClientError, parse_not_leader_error};
pub use grouped::{
    AssignmentStrategy, GroupConfig, GroupCoordinator, GroupedConsumer, WorkerConfig,
};
pub use offset::{
    CollectingCommitHook, CommitInfo, HookedOffsetStore, LockFileOffsetStore, LoggingCommitHook,
    MemoryOffsetStore, OffsetStore, PostCommitHook,
};
pub use producer::{MetricsSnapshot, Producer, ProducerConfig, ProducerMetrics, SendAck};
pub use record::{
    Record, RecordIterator, RecordParseError, RecordParserConfig, RecordType, TLV_HEADER_SIZE,
    encode_record, parse_record, parse_records,
};
pub use standalone::{StandaloneConfig, StandaloneConsumer, StandaloneConsumerBuilder};
pub use tls::TlsClientConfig;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
