//! Standalone Consumer Mode
//!
//! Provides a simplified, Kafka-like consumer API for independent consumption.
//! In standalone mode, each consumer operates independently without coordination
//! with other consumers. This is the simplest consumption pattern.
//!
//! # Example — name-based (recommended)
//!
//! ```rust,no_run
//! use lnc_client::{StandaloneConsumer, StandaloneConfig};
//! use std::path::Path;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Pass a topic *name*; the client resolves it to a numeric ID automatically.
//!     let mut consumer = StandaloneConsumer::connect(
//!         "127.0.0.1:1992",
//!         StandaloneConfig::with_topic_name("my-consumer", "my-topic")
//!             .with_offset_dir(Path::new("/var/lib/lance/offsets")),
//!     ).await?;
//!
//!     // Receive records
//!     loop {
//!         if let Some(records) = consumer.next_batch().await? {
//!             for record in records.data.chunks(256) {
//!                 // Process record
//!             }
//!             consumer.commit().await?;
//!         }
//!     }
//! }
//! ```
//!
//! # Example — ID-based (legacy / backward-compatible)
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
use crate::error::{Result, validate_topic_name};
use crate::offset::{LockFileOffsetStore, MemoryOffsetStore, OffsetStore};

/// Configuration for standalone consumer mode
///
/// There are three ways to specify which topic to consume:
///
/// 1. **Name-based (recommended)** — pass only the topic name via
///    [`StandaloneConfig::with_topic_name`].  The consumer resolves the name
///    to a numeric ID automatically by calling `create_topic` (which is
///    idempotent) on connect.
///
/// 2. **ID + name** — use [`StandaloneConfig::new_with_id`] when you already
///    hold a resolved `TopicInfo` and want to skip the extra round-trip.
///
/// 3. **ID-only (legacy)** — use [`StandaloneConfig::new`] with a numeric ID
///    directly.  This preserves backward compatibility with existing callers.
#[derive(Debug, Clone)]
pub struct StandaloneConfig {
    /// Identifier for this consumer (used for offset storage)
    pub consumer_id: String,
    /// Topic ID to consume from.
    ///
    /// When this is `0` and [`topic_name`] is `Some`, the consumer will
    /// resolve the name to an ID on the first call to
    /// [`StandaloneConsumer::connect`].
    pub topic_id: u32,
    /// Human-readable topic name.
    ///
    /// When set and `topic_id == 0`, `StandaloneConsumer::connect` resolves
    /// the name to an ID via `create_topic` (idempotent).
    pub topic_name: Option<String>,
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
    /// Create a standalone config using a numeric topic ID.
    ///
    /// This is the **legacy constructor** and is kept for backward
    /// compatibility. Prefer [`StandaloneConfig::with_topic_name`] for new
    /// code — it lets the client resolve the name automatically and avoids
    /// coupling call sites to numeric IDs.
    pub fn new(consumer_id: impl Into<String>, topic_id: u32) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            topic_id,
            topic_name: None,
            max_fetch_bytes: 1_048_576, // 1MB default
            start_position: SeekPosition::Beginning,
            offset_dir: None,
            auto_commit_interval: Some(Duration::from_secs(5)),
            connect_timeout: Duration::from_secs(30),
            poll_timeout: Duration::from_millis(100),
        }
    }

    /// Create a standalone config using only the topic **name**.
    ///
    /// When this config is passed to [`StandaloneConsumer::connect`], the
    /// consumer calls `create_topic` (idempotent) to resolve the name to a
    /// numeric ID before subscribing.  No extra setup code is required on the
    /// call site.
    ///
    /// The name must match `[a-zA-Z0-9-]+`; an error is returned at connect
    /// time if validation fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use lnc_client::{StandaloneConsumer, StandaloneConfig};
    ///
    /// # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut consumer = StandaloneConsumer::connect(
    ///     "127.0.0.1:1992",
    ///     StandaloneConfig::with_topic_name("my-consumer", "rithmic-actions"),
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub fn with_topic_name(consumer_id: impl Into<String>, topic_name: impl Into<String>) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            topic_id: 0,
            topic_name: Some(topic_name.into()),
            max_fetch_bytes: 1_048_576,
            start_position: SeekPosition::Beginning,
            offset_dir: None,
            auto_commit_interval: Some(Duration::from_secs(5)),
            connect_timeout: Duration::from_secs(30),
            poll_timeout: Duration::from_millis(100),
        }
    }

    /// Create a standalone config with both a topic name and a pre-resolved
    /// numeric ID.
    ///
    /// Use this when you already have a [`TopicInfo`] from a prior
    /// `create_topic` call and want to avoid the extra round-trip that
    /// [`StandaloneConfig::with_topic_name`] performs on connect.
    ///
    /// [`TopicInfo`]: crate::TopicInfo
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use lnc_client::{ClientConfig, LanceClient, StandaloneConsumer, StandaloneConfig};
    ///
    /// # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut mgmt = LanceClient::connect(ClientConfig::new("127.0.0.1:1992")).await?;
    /// let info = mgmt.create_topic("rithmic-actions").await?;
    ///
    /// let mut consumer = StandaloneConsumer::connect(
    ///     "127.0.0.1:1992",
    ///     StandaloneConfig::new_with_id("my-consumer", &info.name, info.id),
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub fn new_with_id(
        consumer_id: impl Into<String>,
        topic_name: impl Into<String>,
        topic_id: u32,
    ) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            topic_id,
            topic_name: Some(topic_name.into()),
            max_fetch_bytes: 1_048_576,
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
    /// Connect to a LANCE server and start consuming.
    ///
    /// This will:
    /// 1. Validate the topic name if one is supplied (must match `[a-zA-Z0-9-]+`)
    /// 2. Resolve the topic name to a numeric ID via `create_topic` (idempotent)
    ///    when the config was created with [`StandaloneConfig::with_topic_name`]
    /// 3. Establish the consumer connection
    /// 4. Load any previously committed offset from storage
    /// 5. Subscribe to the topic starting from the stored offset (or `start_position`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The topic name is invalid (contains characters outside `[a-zA-Z0-9-]`)
    /// - Connection to the server fails
    /// - Topic name resolution fails
    /// - Offset store cannot be initialized
    /// - Subscription fails
    pub async fn connect(addr: &str, mut config: StandaloneConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new(addr);
        client_config.connect_timeout = config.connect_timeout;

        let mut client = LanceClient::connect(client_config).await?;

        // Resolve topic name to ID when the caller used the name-based constructor.
        if config.topic_id == 0 {
            if let Some(ref name) = config.topic_name.clone() {
                validate_topic_name(name)?;
                let topic_info = client.create_topic(name).await?;
                config.topic_id = topic_info.id;
            }
        }

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

    /// Receive the next batch for an active subscription.
    ///
    /// Returns `Ok(Some(PollResult))` if records are available,
    /// `Ok(None)` if no records are ready (non-blocking behavior).
    ///
    /// This method may trigger auto-commit if configured.
    pub async fn next_batch(&mut self) -> Result<Option<PollResult>> {
        // Check for auto-commit
        self.maybe_auto_commit().await?;

        // Pull from the inner streaming consumer
        let result = self.inner.next_batch().await?;

        if let Some(ref poll_result) = result {
            self.pending_offset = poll_result.current_offset;
        }

        Ok(result)
    }

    /// Primary consume interface alias.
    #[inline]
    pub async fn consume(&mut self) -> Result<Option<PollResult>> {
        self.next_batch().await
    }

    /// Compatibility wrapper for callers still using polling terminology.
    #[inline]
    pub async fn poll(&mut self) -> Result<Option<PollResult>> {
        self.next_batch().await
    }

    /// Poll for records, blocking until data is available or timeout
    ///
    /// This is a convenience method that retries polling until records
    /// are available or the configured poll_timeout is reached.
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<Option<PollResult>> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if let Some(result) = self.next_batch().await? {
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

    /// Build a consumer for a specific topic ID (legacy).
    pub async fn build_for_topic(&self, topic_id: u32) -> Result<StandaloneConsumer> {
        let mut config = self.base_config.clone();
        config.topic_id = topic_id;
        StandaloneConsumer::connect(&self.addr, config).await
    }

    /// Build a consumer for a topic identified by **name**.
    ///
    /// The name is validated and then resolved to a numeric ID via
    /// `create_topic` (idempotent) before the consumer connects.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::InvalidTopicName`] if `topic_name` contains
    /// characters outside `[a-zA-Z0-9-]` or is empty.
    pub async fn build_for_topic_name(
        &self,
        topic_name: impl Into<String>,
    ) -> Result<StandaloneConsumer> {
        let name = topic_name.into();
        validate_topic_name(&name)?;
        let mut config = self.base_config.clone();
        config.topic_id = 0;
        config.topic_name = Some(name);
        StandaloneConsumer::connect(&self.addr, config).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::error::validate_topic_name;

    #[test]
    fn test_standalone_config_defaults() {
        let config = StandaloneConfig::new("test-consumer", 1);

        assert_eq!(config.consumer_id, "test-consumer");
        assert_eq!(config.topic_id, 1);
        assert_eq!(config.max_fetch_bytes, 1_048_576);
        assert!(config.offset_dir.is_none());
        assert!(config.auto_commit_interval.is_some());
        assert!(config.topic_name.is_none());
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

    // -------------------------------------------------------------------------
    // Name-based constructor tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_with_topic_name_sets_name_and_zero_id() {
        let config = StandaloneConfig::with_topic_name("my-consumer", "rithmic-actions");

        assert_eq!(config.consumer_id, "my-consumer");
        assert_eq!(config.topic_name.as_deref(), Some("rithmic-actions"));
        // topic_id starts at 0 — resolved on connect
        assert_eq!(config.topic_id, 0);
    }

    #[test]
    fn test_new_with_id_sets_both_name_and_id() {
        let config = StandaloneConfig::new_with_id("my-consumer", "rithmic-actions", 42);

        assert_eq!(config.consumer_id, "my-consumer");
        assert_eq!(config.topic_name.as_deref(), Some("rithmic-actions"));
        assert_eq!(config.topic_id, 42);
    }

    #[test]
    fn test_with_topic_name_builder_chain() {
        let config = StandaloneConfig::with_topic_name("consumer-1", "data-stream")
            .with_max_fetch_bytes(256 * 1024)
            .with_manual_commit()
            .with_start_position(SeekPosition::End);

        assert_eq!(config.topic_name.as_deref(), Some("data-stream"));
        assert_eq!(config.topic_id, 0);
        assert_eq!(config.max_fetch_bytes, 256 * 1024);
        assert!(config.auto_commit_interval.is_none());
    }

    // -------------------------------------------------------------------------
    // validate_topic_name tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_topic_name_accepts_valid_names() {
        let valid = &[
            "rithmic-actions",
            "data",
            "topic-123",
            "MyTopic",
            "ABC",
            "a-b-c-1-2-3",
        ];
        for name in valid {
            assert!(
                validate_topic_name(name).is_ok(),
                "Expected {:?} to be valid",
                name
            );
        }
    }

    #[test]
    fn test_validate_topic_name_rejects_empty() {
        let result = validate_topic_name("");
        assert!(
            matches!(result, Err(crate::ClientError::InvalidTopicName(_))),
            "Empty name should be invalid"
        );
    }

    #[test]
    fn test_validate_topic_name_rejects_spaces() {
        let result = validate_topic_name("bad topic");
        assert!(
            matches!(result, Err(crate::ClientError::InvalidTopicName(_))),
            "Name with space should be invalid"
        );
    }

    #[test]
    fn test_validate_topic_name_rejects_special_chars() {
        let invalid = &[
            "topic!",
            "topic/sub",
            "topic.name",
            "topic_name",
            "topic@v2",
        ];
        for name in invalid {
            assert!(
                matches!(
                    validate_topic_name(name),
                    Err(crate::ClientError::InvalidTopicName(_))
                ),
                "Expected {:?} to be invalid",
                name
            );
        }
    }

    #[test]
    fn test_validate_topic_name_rejects_unicode() {
        let result = validate_topic_name("tópico");
        assert!(
            matches!(result, Err(crate::ClientError::InvalidTopicName(_))),
            "Name with unicode should be invalid"
        );
    }
}
