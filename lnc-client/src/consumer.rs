//! Consumer abstraction for reading from LANCE streams
//!
//! Provides a high-level API for consuming records from topics with support for:
//! - Offset tracking
//! - Seek/rewind to specific offsets
//! - Continuous polling

use std::sync::Arc;
use std::time::Duration;

use crate::client::LanceClient;
use crate::connection::ReconnectingClient;
use crate::error::{ClientError, Result};
use crate::offset::OffsetStore;

/// Position specifier for seeking within a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekPosition {
    /// Seek to the beginning of the stream (offset 0)
    Beginning,
    /// Seek to the end of the stream (latest data)
    End,
    /// Seek to a specific byte offset
    Offset(u64),
}

/// Configuration for a consumer
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Topic ID to consume from
    pub topic_id: u32,
    /// Maximum bytes to fetch per poll (default: 64KB)
    pub max_fetch_bytes: u32,
    /// Starting position (default: Beginning)
    pub start_position: SeekPosition,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            topic_id: 0,
            max_fetch_bytes: 64 * 1024, // 64KB
            start_position: SeekPosition::Beginning,
        }
    }
}

impl ConsumerConfig {
    /// Create a new consumer configuration for the specified topic
    pub fn new(topic_id: u32) -> Self {
        Self {
            topic_id,
            ..Default::default()
        }
    }

    /// Set the maximum bytes to fetch per poll operation
    pub fn with_max_fetch_bytes(mut self, bytes: u32) -> Self {
        self.max_fetch_bytes = bytes;
        self
    }

    /// Set the starting position for consumption
    pub fn with_start_position(mut self, position: SeekPosition) -> Self {
        self.start_position = position;
        self
    }
}

/// Result of a poll operation
#[derive(Debug, Clone)]
pub struct PollResult {
    /// Raw data fetched from the stream (zero-copy Bytes)
    pub data: bytes::Bytes,
    /// Current offset after this fetch
    pub current_offset: u64,
    /// Number of records in this batch (estimate)
    pub record_count: u32,
    /// Whether the end of available data was reached
    pub end_of_stream: bool,
}

impl PollResult {
    /// Returns true if no data was returned
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// A consumer for reading records from a LANCE topic stream.
///
/// The consumer automatically reconnects on transient failures (connection
/// drops, timeouts, server errors) with exponential backoff and DNS
/// re-resolution. The current offset is preserved across reconnections
/// so no data is lost.
///
/// # Example
///
/// ```text
/// let config = ConsumerConfig::new(topic_id);
/// let mut consumer = Consumer::connect("lance.example.com:1992", config).await?;
///
/// // Poll for records — auto-reconnects on failure
/// while let Some(result) = consumer.poll().await? {
///     process_data(&result.data);
///     if result.end_of_stream {
///         break;
///     }
/// }
/// ```
pub struct Consumer {
    client: ReconnectingClient,
    config: ConsumerConfig,
    current_offset: u64,
    /// Cached end offset for SeekPosition::End
    cached_end_offset: Option<u64>,
    /// Optional offset store for client-side offset persistence
    offset_store: Option<Arc<dyn OffsetStore>>,
    /// Consumer ID for offset store operations
    consumer_id: u64,
}

impl Consumer {
    /// Connect to a LANCE server and create a consumer with auto-reconnect.
    ///
    /// The address can be either an IP:port or hostname:port. DNS is
    /// re-resolved on each reconnect for load-balanced endpoints.
    pub async fn connect(addr: &str, config: ConsumerConfig) -> Result<Self> {
        let rc = ReconnectingClient::connect(addr)
            .await?
            .with_unlimited_retries()
            .with_base_delay(Duration::from_millis(500))
            .with_max_delay(Duration::from_secs(30));

        Ok(Self::from_reconnecting_client(rc, config, 0))
    }

    /// Create a new consumer with the given client and configuration.
    ///
    /// The `addr` is stored for DNS re-resolution on reconnect.
    pub fn new(client: LanceClient, addr: &str, config: ConsumerConfig) -> Self {
        Self::with_consumer_id(client, addr, config, 0)
    }

    /// Create a new consumer with a specific consumer ID
    pub fn with_consumer_id(
        client: LanceClient,
        addr: &str,
        config: ConsumerConfig,
        consumer_id: u64,
    ) -> Self {
        let rc = ReconnectingClient::from_existing(client, addr);
        Self::from_reconnecting_client(rc, config, consumer_id)
    }

    /// Internal constructor from a ReconnectingClient
    fn from_reconnecting_client(
        client: ReconnectingClient,
        config: ConsumerConfig,
        consumer_id: u64,
    ) -> Self {
        let initial_offset = match config.start_position {
            SeekPosition::Beginning => 0,
            SeekPosition::Offset(offset) => offset,
            SeekPosition::End => u64::MAX, // Will be resolved on first poll
        };

        Self {
            client,
            config,
            current_offset: initial_offset,
            cached_end_offset: None,
            offset_store: None,
            consumer_id,
        }
    }

    /// Create a consumer with an offset store for client-side offset persistence
    ///
    /// The consumer will automatically load its starting offset from the store
    /// if one exists, otherwise it uses the configured start position.
    ///
    /// # Arguments
    /// * `client` - The LANCE client connection
    /// * `addr` - Server address for reconnection
    /// * `config` - Consumer configuration
    /// * `consumer_id` - Unique identifier for this consumer instance
    /// * `offset_store` - The offset store for persistence
    ///
    /// # Example
    /// ```ignore
    /// let store = Arc::new(LockFileOffsetStore::open(path, "my-consumer")?);
    /// let consumer = Consumer::with_offset_store(client, "lance:1992", config, 12345, store)?;
    /// ```
    pub fn with_offset_store(
        client: LanceClient,
        addr: &str,
        config: ConsumerConfig,
        consumer_id: u64,
        offset_store: Arc<dyn OffsetStore>,
    ) -> Result<Self> {
        // Try to load existing offset from store
        let stored_offset = offset_store.load(config.topic_id, consumer_id)?;

        let initial_offset = if let Some(offset) = stored_offset {
            // Resume from stored offset
            offset
        } else {
            // No stored offset, use configured start position
            match config.start_position {
                SeekPosition::Beginning => 0,
                SeekPosition::Offset(offset) => offset,
                SeekPosition::End => u64::MAX, // Will be resolved on first poll
            }
        };

        let rc = ReconnectingClient::from_existing(client, addr);
        Ok(Self {
            client: rc,
            config,
            current_offset: initial_offset,
            cached_end_offset: None,
            offset_store: Some(offset_store),
            consumer_id,
        })
    }

    /// Create a consumer starting from the beginning of the stream
    pub fn from_beginning(client: LanceClient, addr: &str, topic_id: u32) -> Self {
        Self::new(client, addr, ConsumerConfig::new(topic_id))
    }

    /// Create a consumer starting from a specific offset
    pub fn from_offset(client: LanceClient, addr: &str, topic_id: u32, offset: u64) -> Self {
        let config =
            ConsumerConfig::new(topic_id).with_start_position(SeekPosition::Offset(offset));
        Self::new(client, addr, config)
    }

    /// Get the current offset position
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Get the topic ID being consumed
    pub fn topic_id(&self) -> u32 {
        self.config.topic_id
    }

    /// Seek to a specific position in the stream
    ///
    /// This allows rewinding to replay historical data or fast-forwarding
    /// to skip ahead.
    ///
    /// # Arguments
    /// * `position` - The position to seek to
    ///
    /// # Examples
    ///
    /// ```text
    /// // Rewind to the beginning
    /// consumer.seek(SeekPosition::Beginning).await?;
    ///
    /// // Seek to a specific offset
    /// consumer.seek(SeekPosition::Offset(1000)).await?;
    ///
    /// // Seek to the end (latest data)
    /// consumer.seek(SeekPosition::End).await?;
    /// ```
    pub async fn seek(&mut self, position: SeekPosition) -> Result<u64> {
        match position {
            SeekPosition::Beginning => {
                self.current_offset = 0;
                Ok(0)
            },
            SeekPosition::Offset(offset) => {
                self.current_offset = offset;
                Ok(offset)
            },
            SeekPosition::End => {
                let end_offset = self.discover_end_offset().await?;
                self.current_offset = end_offset;
                Ok(end_offset)
            },
        }
    }

    /// Rewind to the beginning of the stream (offset 0)
    ///
    /// Convenience method equivalent to `seek(SeekPosition::Beginning)`
    pub async fn rewind(&mut self) -> Result<()> {
        self.seek(SeekPosition::Beginning).await?;
        Ok(())
    }

    /// Seek to a specific byte offset in the stream
    ///
    /// Convenience method equivalent to `seek(SeekPosition::Offset(offset))`
    pub async fn seek_to_offset(&mut self, offset: u64) -> Result<()> {
        self.seek(SeekPosition::Offset(offset)).await?;
        Ok(())
    }

    /// Seek to the end of the stream (latest available data)
    ///
    /// Convenience method equivalent to `seek(SeekPosition::End)`
    pub async fn seek_to_end(&mut self) -> Result<u64> {
        self.seek(SeekPosition::End).await
    }

    /// Poll for the next batch of records
    ///
    /// Returns `Ok(Some(result))` if data was fetched, or `Ok(None)` if
    /// there was no new data available (end of stream reached).
    ///
    /// The consumer's offset is automatically advanced after each successful
    /// poll. On transient errors, the consumer auto-reconnects and retries
    /// from the same offset.
    pub async fn poll(&mut self) -> Result<Option<PollResult>> {
        // Handle SeekPosition::End that hasn't been resolved yet
        if self.current_offset == u64::MAX {
            let end_offset = self.discover_end_offset().await?;
            self.current_offset = end_offset;
        }

        let fetch_result = self.fetch_with_retry().await?;

        let end_of_stream =
            fetch_result.data.is_empty() || fetch_result.next_offset == self.current_offset;

        let result = PollResult {
            data: fetch_result.data,
            current_offset: fetch_result.next_offset,
            record_count: fetch_result.record_count,
            end_of_stream,
        };

        // Advance offset
        self.current_offset = fetch_result.next_offset;
        self.cached_end_offset = Some(fetch_result.next_offset);

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// Poll for records, blocking until data is available or timeout
    ///
    /// Unlike `poll()`, this will return empty results instead of None,
    /// allowing the caller to continue waiting.
    pub async fn poll_blocking(&mut self) -> Result<PollResult> {
        // Handle SeekPosition::End that hasn't been resolved yet
        if self.current_offset == u64::MAX {
            let end_offset = self.discover_end_offset().await?;
            self.current_offset = end_offset;
        }

        let fetch_result = self.fetch_with_retry().await?;

        let end_of_stream =
            fetch_result.data.is_empty() || fetch_result.next_offset == self.current_offset;

        let result = PollResult {
            data: fetch_result.data,
            current_offset: fetch_result.next_offset,
            record_count: fetch_result.record_count,
            end_of_stream,
        };

        // Advance offset only if we got data
        if !result.is_empty() {
            self.current_offset = fetch_result.next_offset;
        }
        self.cached_end_offset = Some(fetch_result.next_offset);

        Ok(result)
    }

    /// Fetch with automatic retry on transient errors.
    /// Reconnects and retries from the same offset, preserving position.
    ///
    /// On `ServerCatchingUp`, backs off for 5 seconds. If the server offset
    /// remains stagnant across repeated attempts, the client forces a
    /// reconnect so reads can re-route to a healthier/leader path while still
    /// preserving the requested offset.
    ///
    /// If the server offset does not advance after several attempts we emit a
    /// warning but preserve the client-requested offset. This keeps read
    /// semantics monotonic and avoids implicit offset rewinds that can produce
    /// duplicate replay.
    async fn fetch_with_retry(&mut self) -> Result<crate::client::FetchResult> {
        const MAX_RETRIES: u32 = 30;
        const CATCHING_UP_BACKOFF: Duration = Duration::from_secs(5);
        /// Alert after this many consecutive CATCHING_UP responses with an
        /// unchanged server_offset.
        const STALE_ALERT_THRESHOLD: u32 = 3;
        let mut attempt = 0u32;
        let mut backoff = Duration::from_millis(500);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);

        let mut last_server_offset: Option<u64> = None;
        let mut stale_count: u32 = 0;

        loop {
            let result = match self.client.client().await {
                Ok(c) => {
                    c.fetch(
                        self.config.topic_id,
                        self.current_offset,
                        self.config.max_fetch_bytes,
                    )
                    .await
                },
                Err(e) => Err(e),
            };

            match &result {
                Ok(_) => return result,
                // Server is healthy but behind on replication — fixed 5s backoff,
                // do NOT mark connection as failed (no reconnect needed).
                Err(ClientError::ServerCatchingUp { server_offset }) => {
                    attempt += 1;

                    // Track whether the server is making progress.
                    if last_server_offset == Some(*server_offset) {
                        stale_count += 1;
                    } else {
                        stale_count = 1;
                        last_server_offset = Some(*server_offset);
                    }

                    if stale_count == STALE_ALERT_THRESHOLD {
                        tracing::warn!(
                            topic_id = self.config.topic_id,
                            requested_offset = self.current_offset,
                            server_offset,
                            "Server offset stagnant while catching up; preserving consumer offset to avoid duplicate replay"
                        );

                        // Proactively reconnect after repeated stagnant catch-up
                        // responses so we can re-resolve leader routing.
                        self.client.mark_failed();

                        // Surface to caller quickly instead of sleeping through
                        // an extended catch-up loop on a stale route.
                        return result;
                    }

                    if attempt >= MAX_RETRIES {
                        return result;
                    }
                    tracing::info!(
                        topic_id = self.config.topic_id,
                        requested_offset = self.current_offset,
                        server_offset,
                        attempt,
                        "Server catching up, backing off {}s",
                        CATCHING_UP_BACKOFF.as_secs()
                    );
                    tokio::time::sleep(CATCHING_UP_BACKOFF).await;
                },
                Err(e) if e.is_retryable() && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.client.mark_failed();
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                },
                _ => return result,
            }
        }
    }

    /// Discover the current end offset by fetching from a very high offset
    async fn discover_end_offset(&mut self) -> Result<u64> {
        // Use cached value if available
        if let Some(end) = self.cached_end_offset {
            return Ok(end);
        }

        // Fetch from a very high offset so the server reports current end
        // offset without returning partial record bytes from the beginning.
        let c = self.client.client().await?;
        let fetch_result = c
            .fetch(
                self.config.topic_id,
                u64::MAX,
                1, // Minimal fetch just to get stream info
            )
            .await?;

        // The next_offset tells us where data ends
        // For empty streams, next_offset will be 0
        let end_offset = fetch_result.next_offset;
        self.cached_end_offset = Some(end_offset);
        Ok(end_offset)
    }

    /// Commit the current offset to the offset store
    ///
    /// If an offset store is configured, this persists the current offset
    /// so the consumer can resume from this position on restart.
    ///
    /// # Example
    /// ```ignore
    /// let records = consumer.poll().await?;
    /// process(records);
    /// consumer.commit().await?;  // Persist offset for crash recovery
    /// ```
    pub async fn commit(&mut self) -> Result<()> {
        if let Some(ref store) = self.offset_store {
            store.save(self.config.topic_id, self.consumer_id, self.current_offset)?;
        }
        Ok(())
    }

    /// Get the consumer ID
    pub fn consumer_id(&self) -> u64 {
        self.consumer_id
    }

    /// Check if this consumer has an offset store configured
    pub fn has_offset_store(&self) -> bool {
        self.offset_store.is_some()
    }

    /// Get mutable access to the underlying reconnecting client
    pub fn reconnecting_client(&mut self) -> &mut ReconnectingClient {
        &mut self.client
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("topic_id", &self.config.topic_id)
            .field("current_offset", &self.current_offset)
            .field("max_fetch_bytes", &self.config.max_fetch_bytes)
            .finish()
    }
}

/// Configuration for a streaming consumer
#[derive(Debug, Clone)]
pub struct StreamingConsumerConfig {
    /// Topic ID to consume from
    pub topic_id: u32,
    /// Maximum bytes per batch (default: 64KB)
    pub max_batch_bytes: u32,
    /// Starting position (default: Beginning)
    pub start_position: SeekPosition,
    /// Consumer group ID (for offset tracking)
    pub consumer_group: Option<String>,
    /// Auto-commit interval in milliseconds (0 = manual commit only)
    pub auto_commit_interval_ms: u64,
}

impl Default for StreamingConsumerConfig {
    fn default() -> Self {
        Self {
            topic_id: 0,
            max_batch_bytes: 64 * 1024,
            start_position: SeekPosition::Beginning,
            consumer_group: None,
            auto_commit_interval_ms: 5000, // 5 seconds
        }
    }
}

impl StreamingConsumerConfig {
    /// Create a new streaming consumer configuration for the specified topic
    pub fn new(topic_id: u32) -> Self {
        Self {
            topic_id,
            ..Default::default()
        }
    }

    /// Set the maximum bytes per batch
    pub fn with_max_batch_bytes(mut self, bytes: u32) -> Self {
        self.max_batch_bytes = bytes;
        self
    }

    /// Set the starting position for consumption
    pub fn with_start_position(mut self, position: SeekPosition) -> Self {
        self.start_position = position;
        self
    }

    /// Set the consumer group for coordinated consumption
    pub fn with_consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    /// Set the auto-commit interval in milliseconds (0 = disabled)
    pub fn with_auto_commit_interval(mut self, interval_ms: u64) -> Self {
        self.auto_commit_interval_ms = interval_ms;
        self
    }
}

/// A streaming consumer that uses subscribe/unsubscribe signals
///
/// Unlike the poll-based `Consumer`, `StreamingConsumer` explicitly signals
/// to the server when it starts and stops consuming, and reports position
/// updates for consumer group tracking.
///
/// # Lifecycle
///
/// 1. **Subscribe** - Call `start()` to signal the server to begin streaming
/// 2. **Consume** - Call `poll()` to receive batches of data
/// 3. **Commit** - Call `commit()` to checkpoint progress (or use auto-commit)
/// 4. **Unsubscribe** - Call `stop()` or drop the consumer to signal completion
///
/// # Example
///
/// ```text
/// let client = LanceClient::connect_to("127.0.0.1:1992").await?;
/// let config = StreamingConsumerConfig::new(topic_id)
///     .with_consumer_group("my-group");
/// let mut consumer = StreamingConsumer::new(client, config);
///
/// // Start streaming
/// consumer.start().await?;
///
/// // Process records
/// while let Some(result) = consumer.poll().await? {
///     process_data(&result.data);
///     consumer.commit().await?; // Checkpoint progress
/// }
///
/// // Stop streaming
/// consumer.stop().await?;
/// ```
pub struct StreamingConsumer {
    client: LanceClient,
    config: StreamingConsumerConfig,
    consumer_id: u64,
    current_offset: u64,
    committed_offset: u64,
    is_subscribed: bool,
    last_commit_time: std::time::Instant,
}

impl StreamingConsumer {
    /// Create a new streaming consumer
    pub fn new(client: LanceClient, config: StreamingConsumerConfig) -> Self {
        let consumer_id = Self::generate_consumer_id();
        let initial_offset = match config.start_position {
            SeekPosition::Beginning => 0,
            SeekPosition::Offset(offset) => offset,
            SeekPosition::End => u64::MAX,
        };

        Self {
            client,
            config,
            consumer_id,
            current_offset: initial_offset,
            committed_offset: 0,
            is_subscribed: false,
            last_commit_time: std::time::Instant::now(),
        }
    }

    /// Generate a unique consumer ID
    fn generate_consumer_id() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        // Mix in thread ID for uniqueness
        let thread_id = std::thread::current().id();
        timestamp ^ (format!("{:?}", thread_id).len() as u64).wrapping_mul(0x517cc1b727220a95)
    }

    /// Start streaming from the topic
    ///
    /// Sends a Subscribe signal to the server indicating this consumer
    /// wants to start receiving data from the configured position.
    pub async fn start(&mut self) -> Result<()> {
        if self.is_subscribed {
            return Ok(());
        }

        let result = self
            .client
            .subscribe(
                self.config.topic_id,
                self.current_offset,
                self.config.max_batch_bytes,
                self.consumer_id,
            )
            .await?;

        self.current_offset = result.start_offset;
        self.is_subscribed = true;
        self.last_commit_time = std::time::Instant::now();

        Ok(())
    }

    /// Stop streaming from the topic
    ///
    /// Sends an Unsubscribe signal to the server. This should be called
    /// when the consumer is done processing to free server resources.
    pub async fn stop(&mut self) -> Result<()> {
        if !self.is_subscribed {
            return Ok(());
        }

        // Commit any uncommitted offset before stopping
        if self.current_offset > self.committed_offset {
            let _ = self.commit().await;
        }

        self.client
            .unsubscribe(self.config.topic_id, self.consumer_id)
            .await?;
        self.is_subscribed = false;

        Ok(())
    }

    /// Poll for the next batch of records
    ///
    /// Returns `Ok(Some(result))` if data was fetched, or `Ok(None)` if
    /// no new data is available.
    pub async fn poll(&mut self) -> Result<Option<PollResult>> {
        if !self.is_subscribed {
            return Err(ClientError::ProtocolError(
                "Consumer not subscribed - call start() first".to_string(),
            ));
        }

        // Check if auto-commit is due
        self.maybe_auto_commit().await?;

        let fetch_result = self
            .client
            .fetch(
                self.config.topic_id,
                self.current_offset,
                self.config.max_batch_bytes,
            )
            .await?;

        let end_of_stream =
            fetch_result.data.is_empty() || fetch_result.next_offset == self.current_offset;

        let result = PollResult {
            data: fetch_result.data,
            current_offset: fetch_result.next_offset,
            record_count: fetch_result.record_count,
            end_of_stream,
        };

        // Advance offset
        self.current_offset = fetch_result.next_offset;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// Commit the current offset to the server
    ///
    /// This checkpoints the consumer's position so that if it restarts,
    /// it can resume from this point.
    pub async fn commit(&mut self) -> Result<()> {
        if self.current_offset <= self.committed_offset {
            return Ok(());
        }

        let result = self
            .client
            .commit_offset(self.config.topic_id, self.consumer_id, self.current_offset)
            .await?;

        self.committed_offset = result.committed_offset;
        self.last_commit_time = std::time::Instant::now();

        Ok(())
    }

    /// Auto-commit if interval has elapsed
    async fn maybe_auto_commit(&mut self) -> Result<()> {
        if self.config.auto_commit_interval_ms == 0 {
            return Ok(()); // Auto-commit disabled
        }

        let elapsed = self.last_commit_time.elapsed().as_millis() as u64;
        if elapsed >= self.config.auto_commit_interval_ms {
            self.commit().await?;
        }

        Ok(())
    }

    /// Seek to a new position
    ///
    /// Note: This will commit the current offset and restart the subscription
    /// at the new position.
    pub async fn seek(&mut self, position: SeekPosition) -> Result<u64> {
        let was_subscribed = self.is_subscribed;

        if was_subscribed {
            self.stop().await?;
        }

        let new_offset = match position {
            SeekPosition::Beginning => 0,
            SeekPosition::Offset(offset) => offset,
            SeekPosition::End => u64::MAX,
        };

        self.current_offset = new_offset;

        if was_subscribed {
            self.start().await?;
        }

        Ok(self.current_offset)
    }

    /// Get the current offset
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Get the last committed offset
    pub fn committed_offset(&self) -> u64 {
        self.committed_offset
    }

    /// Get the consumer ID
    pub fn consumer_id(&self) -> u64 {
        self.consumer_id
    }

    /// Check if currently subscribed
    pub fn is_subscribed(&self) -> bool {
        self.is_subscribed
    }

    /// Get access to the underlying client
    pub fn client(&self) -> &LanceClient {
        &self.client
    }

    /// Consume this consumer and return the underlying client
    pub async fn into_client(mut self) -> Result<LanceClient> {
        self.stop().await?;
        Ok(self.client)
    }
}

impl std::fmt::Debug for StreamingConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingConsumer")
            .field("topic_id", &self.config.topic_id)
            .field("consumer_id", &self.consumer_id)
            .field("current_offset", &self.current_offset)
            .field("committed_offset", &self.committed_offset)
            .field("is_subscribed", &self.is_subscribed)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_config_default() {
        let config = ConsumerConfig::default();
        assert_eq!(config.topic_id, 0);
        assert_eq!(config.max_fetch_bytes, 64 * 1024);
        assert_eq!(config.start_position, SeekPosition::Beginning);
    }

    #[test]
    fn test_consumer_config_builder() {
        let config = ConsumerConfig::new(42)
            .with_max_fetch_bytes(128 * 1024)
            .with_start_position(SeekPosition::Offset(1000));

        assert_eq!(config.topic_id, 42);
        assert_eq!(config.max_fetch_bytes, 128 * 1024);
        assert_eq!(config.start_position, SeekPosition::Offset(1000));
    }

    #[test]
    fn test_poll_result_is_empty() {
        let empty = PollResult {
            data: bytes::Bytes::new(),
            current_offset: 0,
            record_count: 0,
            end_of_stream: true,
        };
        assert!(empty.is_empty());

        let non_empty = PollResult {
            data: bytes::Bytes::from_static(&[1, 2, 3]),
            current_offset: 3,
            record_count: 1,
            end_of_stream: false,
        };
        assert!(!non_empty.is_empty());
    }

    // =========================================================================
    // StreamingConsumerConfig Tests
    // =========================================================================

    #[test]
    fn test_streaming_consumer_config_default() {
        let config = StreamingConsumerConfig::default();
        assert_eq!(config.topic_id, 0);
        assert_eq!(config.max_batch_bytes, 64 * 1024);
        assert_eq!(config.start_position, SeekPosition::Beginning);
        assert!(config.consumer_group.is_none());
        assert_eq!(config.auto_commit_interval_ms, 5000);
    }

    #[test]
    fn test_streaming_consumer_config_builder() {
        let config = StreamingConsumerConfig::new(42)
            .with_max_batch_bytes(128 * 1024)
            .with_start_position(SeekPosition::Offset(5000))
            .with_consumer_group("my-group")
            .with_auto_commit_interval(10000);

        assert_eq!(config.topic_id, 42);
        assert_eq!(config.max_batch_bytes, 128 * 1024);
        assert_eq!(config.start_position, SeekPosition::Offset(5000));
        assert_eq!(config.consumer_group, Some("my-group".to_string()));
        assert_eq!(config.auto_commit_interval_ms, 10000);
    }

    #[test]
    fn test_streaming_consumer_config_disable_auto_commit() {
        let config = StreamingConsumerConfig::new(1).with_auto_commit_interval(0);

        assert_eq!(config.auto_commit_interval_ms, 0);
    }

    #[test]
    fn test_streaming_consumer_config_seek_positions() {
        let beginning =
            StreamingConsumerConfig::new(1).with_start_position(SeekPosition::Beginning);
        assert_eq!(beginning.start_position, SeekPosition::Beginning);

        let end = StreamingConsumerConfig::new(1).with_start_position(SeekPosition::End);
        assert_eq!(end.start_position, SeekPosition::End);

        let offset =
            StreamingConsumerConfig::new(1).with_start_position(SeekPosition::Offset(12345));
        assert_eq!(offset.start_position, SeekPosition::Offset(12345));
    }

    // =========================================================================
    // SeekPosition Tests
    // =========================================================================

    #[test]
    fn test_seek_position_equality() {
        assert_eq!(SeekPosition::Beginning, SeekPosition::Beginning);
        assert_eq!(SeekPosition::End, SeekPosition::End);
        assert_eq!(SeekPosition::Offset(100), SeekPosition::Offset(100));

        assert_ne!(SeekPosition::Beginning, SeekPosition::End);
        assert_ne!(SeekPosition::Offset(100), SeekPosition::Offset(200));
        assert_ne!(SeekPosition::Beginning, SeekPosition::Offset(0));
    }

    #[test]
    fn test_seek_position_clone() {
        let pos = SeekPosition::Offset(42);
        let cloned = pos;
        assert_eq!(pos, cloned);
    }
}
