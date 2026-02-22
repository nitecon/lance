//! # lnc-client
//!
//! Async Rust client library for the [LANCE](https://github.com/nitecon/lance) streaming platform.
//!
//! LANCE is a high-performance, non-blocking stream engine designed for 100Gbps sustained
//! ingestion with sub-microsecond P99 latency.
//!
//! ## Features
//!
//! - **Async/await** - Built on Tokio for high-performance async I/O
//! - **Producer** - Batch records with configurable batching and backpressure
//! - **Consumer** - Standalone ([`standalone`]) and grouped ([`grouped`]) consumer modes
//! - **Connection pooling** - Automatic reconnection and cluster-aware routing
//! - **TLS support** - Secure connections with rustls
//! - **Zero-copy** - Efficient record parsing with minimal allocations
//!
//! ## Quick Start
//!
//! ### Producer
//!
//! ```rust,no_run
//! use lnc_client::{Producer, ProducerConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let producer = Producer::connect("127.0.0.1:1992", ProducerConfig::new()).await?;
//!     
//!     producer.send(1, b"Hello, LANCE!").await?;
//!     producer.flush().await?;
//!     producer.close().await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Consumer (Standalone)
//!
//! For independent consumption with manual offset control:
//!
//! ```rust,no_run
//! use lnc_client::{StandaloneConsumer, StandaloneConfig};
//! use std::path::Path;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut consumer = StandaloneConsumer::connect(
//!         "127.0.0.1:1992",
//!         StandaloneConfig::new("my-consumer", 1)
//!             .with_offset_dir(Path::new("/var/lib/lance/offsets")),
//!     ).await?;
//!     
//!     loop {
//!         if let Some(records) = consumer.next_batch().await? {
//!             // Process records
//!             for chunk in records.data.chunks(256) {
//!                 println!("Received {} bytes", chunk.len());
//!             }
//!             consumer.commit().await?;
//!         }
//!     }
//! }
//! ```
//!
//! ### Consumer (Grouped)
//!
//! ```rust,no_run
//! use lnc_client::{AssignmentStrategy, GroupCoordinator, GroupConfig, GroupedConsumer, WorkerConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Start coordinator (typically one per consumer group)
//!     let coordinator = GroupCoordinator::start(
//!         "127.0.0.1:1992",
//!         GroupConfig::new("my-group")
//!             .with_topics(vec![1, 2, 3])
//!             .with_assignment_strategy(AssignmentStrategy::RoundRobin),
//!     ).await?;
//!
//!     // Workers join the group
//!     let mut worker = GroupedConsumer::join(
//!         "127.0.0.1:1992",
//!         coordinator.join_address(),
//!         WorkerConfig::new("worker-1"),
//!     ).await?;
//!
//!     // Worker processes assigned topics
//!     loop {
//!         let topics: Vec<u32> = worker.assignments().to_vec();
//!         for topic_id in topics {
//!             if let Some(_records) = worker.next_batch(topic_id).await? {
//!                 worker.commit(topic_id).await?;
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## Modules
//!
//! - [`producer`] - High-level producer with batching and async send
//! - [`standalone`] - Standalone consumer for direct offset control
//! - [`grouped`] - Grouped consumer with automatic partition assignment
//! - [`connection`] - Connection pooling and cluster-aware routing
//! - [`offset`] - Offset storage backends (memory, file-based)
//! - [`record`] - Record encoding/decoding utilities
//! - [`tls`] - TLS configuration
//!
//! ## Platform Support
//!
//! The client library supports all major platforms:
//! - Linux (x86_64, aarch64)
//! - macOS (x86_64, aarch64)
//! - Windows (x86_64)
//!
//! > **Note**: The LANCE server requires Linux with io_uring support, but the client works everywhere.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![warn(missing_docs)]

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
