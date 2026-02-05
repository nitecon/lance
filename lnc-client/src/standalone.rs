//! Standalone Consumer Mode
//!
//! Provides a simplified, Kafka-like consumer API for independent consumption.
//! In standalone mode, each consumer operates independently without coordination
//! with other consumers. This is the simplest consumption pattern.
//!
//! # Example
//!
//! ```rust,no_run
//! use lnc_client::{StandaloneConsumer, StandaloneConfig};
//! use std::path::Path;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect and start consuming
//!     let mut consumer = StandaloneConsumer::connect(
//!         "127.0.0.1:1992",
//!         StandaloneConfig::new("my-topic-id", 1)
//!             .with_consumer_id("my-consumer")
//!             .with_offset_dir(Path::new("/var/lib/lance/offsets")),
//!     ).await?;
//!
//!     // Poll for records
//!     loop {
//!         if let Some(records) = consumer.poll().await? {
//!             for record in records.data.chunks(256) {
//!                 // Process record
//!             }
//!             consumer.commit().await?;
//!         }
//!     }
//! }
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::{ClientConfig, LanceClient};
use crate::consumer::{PollResult, SeekPosition, StreamingConsumer, StreamingConsumerConfig};
use crate::error::Result;
use crate::offset::{LockFileOffsetStore, MemoryOffsetStore, OffsetStore};

/// Configuration for standalone consumer mode
#[derive(Debug, Clone)]
pub struct StandaloneConfig {
    /// Identifier for this consumer (used for offset storage)
    pub consumer_id: String,
    /// Topic ID to consume from
    pub topic_id: u32,
    /// Maximum bytes to fetch per poll
    pub max_fetch_bytes: u32,
    /// Starting position if no stored offset exists
    pub start_position: SeekPosition,
    /// Directory for offset persistence (None = in-memory only)
    pub offset_dir: Option<PathBuf>,
    /// Auto-commit interval (None = manual commit only)
    pub auto_commit_interval: Option<Duration>,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Poll timeout for blocking operations
    pub poll_timeout: Duration,
}

impl StandaloneConfig {
    /// Create a new standalone config for the given topic
    pub fn new(consumer_id: impl Into<String>, topic_id: u32) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            topic_id,
            max_fetch_bytes: 1_048_576, // 1MB default
            start_position: SeekPosition::Beginning,
            offset_dir: None,
            auto_commit_interval: Some(Duration::from_secs(5)),
            connect_timeout: Duration::from_secs(30),
            poll_timeout: Duration::from_millis(100),
        }
    }

    /// Set the consumer ID for offset tracking
    pub fn with_consumer_id(mut self, id: impl Into<String>) -> Self {
        self.consumer_id = id.into();
        self
    }

    /// Set the maximum bytes to fetch per poll
    pub fn with_max_fetch_bytes(mut self, bytes: u32) -> Self {
        self.max_fetch_bytes = bytes;
        self
    }

    /// Set the starting position when no stored offset exists
    pub fn with_start_position(mut self, position: SeekPosition) -> Self {
        self.start_position = position;
        self
    }

    /// Set the directory for offset persistence
    /// If not set, offsets are stored in memory only (lost on restart)
    pub fn with_offset_dir(mut self, dir: &Path) -> Self {
        self.offset_dir = Some(dir.to_path_buf());
        self
    }

    /// Set auto-commit interval (None to disable auto-commit)
    pub fn with_auto_commit_interval(mut self, interval: Option<Duration>) -> Self {
        self.auto_commit_interval = interval;
        self
    }

    /// Disable auto-commit (manual commit only)
    pub fn with_manual_commit(mut self) -> Self {
        self.auto_commit_interval = None;
        self
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set poll timeout
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }
}

/// Standalone consumer for independent consumption
///
/// This consumer operates independently without coordination with other consumers.
/// It manages its own offset tracking and persistence.
///
/// # Kafka Equivalence
///
/// This is similar to a Kafka consumer with `enable.auto.commit=true` and no
/// consumer group coordination. Each StandaloneConsumer instance tracks its
/// own progress independently.
pub struct StandaloneConsumer {
    inner: StreamingConsumer,
    config: StandaloneConfig,
    offset_store: Arc<dyn OffsetStore>,
    last_commit_time: Instant,
    pending_offset: u64,
    committed_offset: u64,
}

impl StandaloneConsumer {
    /// Connect to a LANCE server and start consuming
    ///
    /// This will:
    /// 1. Establish connection to the server
    /// 2. Load any previously committed offset from storage
    /// 3. Subscribe to the topic starting from the stored offset (or start_position)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to the server fails
    /// - Offset store cannot be initialized
    /// - Subscription fails
    pub async fn connect(addr: &str, config: StandaloneConfig) -> Result<Self> {
        let socket_addr = addr.parse().map_err(|e| {
            crate::error::ClientError::ProtocolError(format!("Invalid address: {}", e))
        })?;
        let mut client_config = ClientConfig::new(socket_addr);
        client_config.connect_timeout = config.connect_timeout;

        let client = LanceClient::connect(client_config).await?;
        Self::from_client(client, config).await
    }

    /// Create a standalone consumer from an existing client connection
    pub async fn from_client(client: LanceClient, config: StandaloneConfig) -> Result<Self> {
        // Generate numeric consumer ID from string name
        let numeric_consumer_id = Self::hash_consumer_id(&config.consumer_id);

        // Initialize offset store
        let offset_store: Arc<dyn OffsetStore> = if let Some(ref dir) = config.offset_dir {
            Arc::new(LockFileOffsetStore::open(dir, &config.consumer_id)?)
        } else {
            Arc::new(MemoryOffsetStore::new())
        };

        // Load stored offset
        let stored_offset = offset_store
            .load(config.topic_id, numeric_consumer_id)
            .ok()
            .flatten();

        // Determine starting position
        let start_position = if let Some(offset) = stored_offset {
            SeekPosition::Offset(offset)
        } else {
            config.start_position
        };

        // Create streaming consumer config
        let streaming_config = StreamingConsumerConfig::new(config.topic_id)
            .with_max_batch_bytes(config.max_fetch_bytes)
            .with_start_position(start_position)
            .with_auto_commit_interval(0); // We handle auto-commit ourselves

        let mut inner = StreamingConsumer::new(client, streaming_config);

        // Start the subscription
        inner.start().await?;

        let current_offset = inner.current_offset();

        Ok(Self {
            inner,
            config,
            offset_store,
            last_commit_time: Instant::now(),
            pending_offset: current_offset,
            committed_offset: stored_offset.unwrap_or(0),
        })
    }

    /// Poll for new records
    ///
    /// Returns `Ok(Some(PollResult))` if records are available,
    /// `Ok(None)` if no records are ready (non-blocking behavior).
    ///
    /// This method may trigger auto-commit if configured.
    pub async fn poll(&mut self) -> Result<Option<PollResult>> {
        // Check for auto-commit
        self.maybe_auto_commit().await?;

        // Poll the inner consumer
        let result = self.inner.poll().await?;

        if let Some(ref poll_result) = result {
            self.pending_offset = poll_result.current_offset;
        }

        Ok(result)
    }

    /// Poll for records, blocking until data is available or timeout
    ///
    /// This is a convenience method that retries polling until records
    /// are available or the configured poll_timeout is reached.
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<Option<PollResult>> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if let Some(result) = self.poll().await? {
                return Ok(Some(result));
            }
            // Small sleep to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(None)
    }

    /// Commit the current offset
    ///
    /// This persists the current consumption progress to the offset store.
    /// If the consumer crashes and restarts, it will resume from this offset.
    pub async fn commit(&mut self) -> Result<()> {
        self.commit_offset(self.pending_offset).await
    }

    /// Commit a specific offset
    ///
    /// Use this for fine-grained control over exactly which offset is committed.
    pub async fn commit_offset(&mut self, offset: u64) -> Result<()> {
        // Generate numeric consumer ID
        let numeric_consumer_id = Self::hash_consumer_id(&self.config.consumer_id);

        // Persist to offset store
        self.offset_store
            .save(self.config.topic_id, numeric_consumer_id, offset)?;

        self.committed_offset = offset;
        self.last_commit_time = Instant::now();

        // Also commit to server for visibility (optional, for monitoring)
        let _ = self.inner.commit().await;

        Ok(())
    }

    /// Seek to a specific position
    ///
    /// This changes the consumption position. The next poll will start
    /// from the new position.
    pub async fn seek(&mut self, position: SeekPosition) -> Result<u64> {
        let offset = self.inner.seek(position).await?;
        self.pending_offset = offset;
        Ok(offset)
    }

    /// Get the current consumption offset (may not be committed yet)
    pub fn current_offset(&self) -> u64 {
        self.pending_offset
    }

    /// Get the last committed offset
    pub fn committed_offset(&self) -> u64 {
        self.committed_offset
    }

    /// Get the consumer ID
    pub fn consumer_id(&self) -> &str {
        &self.config.consumer_id
    }

    /// Get the topic ID being consumed
    pub fn topic_id(&self) -> u32 {
        self.config.topic_id
    }

    /// Check if the consumer is actively subscribed
    pub fn is_subscribed(&self) -> bool {
        self.inner.is_subscribed()
    }

    /// Get a reference to the underlying client
    pub fn client(&self) -> &LanceClient {
        self.inner.client()
    }

    /// Stop the consumer and release resources
    ///
    /// This will:
    /// 1. Commit any pending offset (if auto-commit is enabled)
    /// 2. Unsubscribe from the topic
    /// 3. Close the connection
    pub async fn close(mut self) -> Result<LanceClient> {
        // Final commit if we have pending changes
        if self.pending_offset > self.committed_offset {
            let _ = self.commit().await;
        }

        self.inner.into_client().await
    }

    /// Hash a string consumer ID to a numeric ID
    fn hash_consumer_id(consumer_id: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        consumer_id.hash(&mut hasher);
        hasher.finish()
    }

    /// Check and perform auto-commit if interval has elapsed
    async fn maybe_auto_commit(&mut self) -> Result<()> {
        if let Some(interval) = self.config.auto_commit_interval {
            if self.last_commit_time.elapsed() >= interval {
                if self.pending_offset > self.committed_offset {
                    self.commit().await?;
                } else {
                    // Update time even if no commit needed
                    self.last_commit_time = Instant::now();
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for StandaloneConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandaloneConsumer")
            .field("consumer_id", &self.config.consumer_id)
            .field("topic_id", &self.config.topic_id)
            .field("pending_offset", &self.pending_offset)
            .field("committed_offset", &self.committed_offset)
            .field("is_subscribed", &self.inner.is_subscribed())
            .finish()
    }
}

/// Builder for creating multiple standalone consumers with shared configuration
pub struct StandaloneConsumerBuilder {
    addr: String,
    base_config: StandaloneConfig,
}

impl StandaloneConsumerBuilder {
    /// Create a new builder
    pub fn new(addr: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            base_config: StandaloneConfig::new(consumer_id, 0),
        }
    }

    /// Set the offset storage directory
    pub fn with_offset_dir(mut self, dir: &Path) -> Self {
        self.base_config = self.base_config.with_offset_dir(dir);
        self
    }

    /// Set the max fetch bytes
    pub fn with_max_fetch_bytes(mut self, bytes: u32) -> Self {
        self.base_config = self.base_config.with_max_fetch_bytes(bytes);
        self
    }

    /// Set the start position for new consumers
    pub fn with_start_position(mut self, position: SeekPosition) -> Self {
        self.base_config = self.base_config.with_start_position(position);
        self
    }

    /// Set auto-commit interval
    pub fn with_auto_commit_interval(mut self, interval: Option<Duration>) -> Self {
        self.base_config = self.base_config.with_auto_commit_interval(interval);
        self
    }

    /// Build a consumer for a specific topic
    pub async fn build_for_topic(&self, topic_id: u32) -> Result<StandaloneConsumer> {
        let mut config = self.base_config.clone();
        config.topic_id = topic_id;
        StandaloneConsumer::connect(&self.addr, config).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_standalone_config_defaults() {
        let config = StandaloneConfig::new("test-consumer", 1);

        assert_eq!(config.consumer_id, "test-consumer");
        assert_eq!(config.topic_id, 1);
        assert_eq!(config.max_fetch_bytes, 1_048_576);
        assert!(config.offset_dir.is_none());
        assert!(config.auto_commit_interval.is_some());
    }

    #[test]
    fn test_standalone_config_builder() {
        let config = StandaloneConfig::new("test", 1)
            .with_max_fetch_bytes(512 * 1024)
            .with_offset_dir(Path::new("/tmp/offsets"))
            .with_manual_commit()
            .with_start_position(SeekPosition::End);

        assert_eq!(config.max_fetch_bytes, 512 * 1024);
        assert!(config.offset_dir.is_some());
        assert!(config.auto_commit_interval.is_none());
    }

    #[test]
    fn test_standalone_config_with_auto_commit() {
        let config = StandaloneConfig::new("test", 1)
            .with_auto_commit_interval(Some(Duration::from_secs(10)));

        assert_eq!(config.auto_commit_interval, Some(Duration::from_secs(10)));
    }
}
