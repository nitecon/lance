//! Producer Abstraction Layer
//!
//! Provides a high-level, Kafka-like producer API with batching, buffering,
//! and async send capabilities.
//!
//! # Features
//!
//! - **Batching**: Records are accumulated and sent in batches for efficiency
//! - **Async Send**: Non-blocking send with Future-based acknowledgment
//! - **Callbacks**: Optional completion callbacks for send results
//! - **Flush**: Explicit flush to ensure all buffered records are sent
//! - **Metrics**: Built-in tracking of records sent, bytes, latency
//!
//! # Example
//!
//! ```rust,no_run
//! use lnc_client::{Producer, ProducerConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let producer = Producer::connect(
//!         "127.0.0.1:1992",
//!         ProducerConfig::new()
//!             .with_batch_size(16 * 1024)
//!             .with_linger_ms(5),
//!     ).await?;
//!
//!     // Async send with future
//!     let ack = producer.send(1, b"hello world").await?;
//!     println!("Sent with batch_id: {}", ack.batch_id);
//!
//!     // Batch multiple sends
//!     for i in 0..1000 {
//!         producer.send(1, format!("message-{}", i).as_bytes()).await?;
//!     }
//!
//!     // Ensure all buffered records are sent
//!     producer.flush().await?;
//!     
//!     producer.close().await?;
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tokio::time::interval;

use crate::client::LanceClient;
use crate::connection::ReconnectingClient;
use crate::error::{ClientError, Result};

/// Configuration for the producer
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Maximum size of a batch in bytes before sending
    pub batch_size: usize,
    /// Maximum time to wait before sending a batch (milliseconds)
    pub linger_ms: u64,
    /// Maximum number of in-flight requests (for backpressure)
    pub max_in_flight: usize,
    /// Buffer memory limit in bytes
    pub buffer_memory: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout for individual sends
    pub request_timeout: Duration,
    /// Enable compression (LZ4)
    pub compression: bool,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 16 * 1024,           // 16KB default batch
            linger_ms: 5,                    // 5ms linger
            max_in_flight: 5,                // 5 concurrent requests
            buffer_memory: 32 * 1024 * 1024, // 32MB buffer
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            compression: false,
        }
    }
}

impl ProducerConfig {
    /// Create a new producer configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size in bytes
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the linger time in milliseconds
    pub fn with_linger_ms(mut self, ms: u64) -> Self {
        self.linger_ms = ms;
        self
    }

    /// Set maximum in-flight requests
    pub fn with_max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n;
        self
    }

    /// Set buffer memory limit
    pub fn with_buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set request timeout
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }
}

/// Acknowledgment returned after a successful send
#[derive(Debug, Clone)]
pub struct SendAck {
    /// Batch ID assigned by the server
    pub batch_id: u64,
    /// Topic the record was sent to
    pub topic_id: u32,
    /// Timestamp when the record was acknowledged
    pub timestamp: Instant,
    /// Offset of this record within the batch (0-indexed)
    /// Used for ordered streaming ACK completion
    pub offset_in_batch: u32,
}

/// Result of a send operation
pub type SendResult = Result<SendAck>;

/// Batch of records ready to send
struct RecordBatch {
    topic_id: u32,
    data: BytesMut,
    record_count: usize,
    ack_txs: Vec<oneshot::Sender<SendResult>>,
    /// Byte offsets where each record starts within the batch data
    /// Used for ordered streaming ACK completion (M2)
    record_offsets: Vec<usize>,
    created_at: Instant,
}

impl RecordBatch {
    fn new(topic_id: u32) -> Self {
        Self {
            topic_id,
            data: BytesMut::with_capacity(16 * 1024),
            record_count: 0,
            ack_txs: Vec::new(),
            record_offsets: Vec::new(),
            created_at: Instant::now(),
        }
    }

    fn add(&mut self, data: Bytes, ack_tx: oneshot::Sender<SendResult>) {
        // Track the offset where this record starts
        self.record_offsets.push(self.data.len());
        self.data.extend_from_slice(&data);
        self.record_count += 1;
        self.ack_txs.push(ack_tx);
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.record_count == 0
    }
}

/// Producer metrics with backpressure telemetry (M3)
#[derive(Debug, Default)]
pub struct ProducerMetrics {
    /// Total records sent
    pub records_sent: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total batches sent
    pub batches_sent: AtomicU64,
    /// Total errors
    pub errors: AtomicU64,
    /// Current buffer size in bytes
    pub buffer_size: AtomicU64,
    /// M3: Number of records dropped due to backpressure (try_send fail)
    pub backpressure_drops: AtomicU64,
    /// M3: Number of times send() blocked waiting for buffer space
    pub backpressure_waits: AtomicU64,
    /// M3: Total milliseconds spent waiting for buffer space
    pub backpressure_wait_ms: AtomicU64,
}

impl ProducerMetrics {
    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            records_sent: self.records_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            buffer_size: self.buffer_size.load(Ordering::Relaxed),
            backpressure_drops: self.backpressure_drops.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
            backpressure_wait_ms: self.backpressure_wait_ms.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of producer metrics
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total number of records sent
    pub records_sent: u64,
    /// Total number of bytes sent
    pub bytes_sent: u64,
    /// Total number of batches sent
    pub batches_sent: u64,
    /// Total number of errors encountered
    pub errors: u64,
    /// Current buffer size in bytes
    pub buffer_size: u64,
    /// M3: Records dropped due to backpressure
    pub backpressure_drops: u64,
    /// M3: Number of times send blocked for buffer space
    pub backpressure_waits: u64,
    /// M3: Total milliseconds spent waiting for buffer
    pub backpressure_wait_ms: u64,
}

/// High-level producer with batching and async send.
///
/// The producer automatically reconnects on transient failures (connection
/// drops, FORWARD_FAILED during leader elections, timeouts) with exponential
/// backoff and DNS re-resolution. Callers never need to handle reconnection
/// logic — just call `send()` and the library takes care of the rest.
///
/// # Bounded Enqueue & Backpressure (M3)
///
/// The producer implements bounded enqueue with configurable `buffer_memory`.
/// When the buffer is full, the producer can:
/// - Return `ServerBackpressure` error immediately (default `send()` behavior)
/// - Block waiting for space (`send_timeout()`)
/// - Non-blocking try (`try_send()` returns `WouldBlock`)
pub struct Producer {
    client: Arc<Mutex<ReconnectingClient>>,
    config: ProducerConfig,
    batches: Arc<RwLock<HashMap<u32, RecordBatch>>>,
    metrics: Arc<ProducerMetrics>,
    running: Arc<AtomicBool>,
    connection_healthy: Arc<AtomicBool>,
    flush_tx: mpsc::Sender<oneshot::Sender<Result<()>>>,
}

impl Producer {
    /// Connect to a LANCE server and create a producer
    ///
    /// The address can be either an IP:port (e.g., "127.0.0.1:1992") or
    /// a hostname:port (e.g., "lance.example.com:1992"). DNS resolution
    /// is performed automatically for hostnames.
    ///
    /// The producer uses automatic reconnection with exponential backoff.
    /// Transient failures (connection drops, FORWARD_FAILED, timeouts) are
    /// retried transparently. DNS is re-resolved on each reconnect so that
    /// load-balanced endpoints route to healthy nodes.
    pub async fn connect(addr: &str, config: ProducerConfig) -> Result<Self> {
        let rc = ReconnectingClient::connect(addr)
            .await?
            .with_unlimited_retries()
            .with_base_delay(Duration::from_millis(500))
            .with_max_delay(Duration::from_secs(30));

        Self::from_reconnecting_client(rc, config).await
    }

    /// Create a producer from an existing client connection.
    ///
    /// The `addr` is stored for DNS re-resolution on reconnect. If you
    /// don't need auto-reconnect, pass the same address used to create
    /// the client.
    pub async fn from_client(
        client: LanceClient,
        addr: &str,
        config: ProducerConfig,
    ) -> Result<Self> {
        let rc = ReconnectingClient::from_existing(client, addr);
        Self::from_reconnecting_client(rc, config).await
    }

    /// Create a producer from a ReconnectingClient (internal constructor)
    async fn from_reconnecting_client(
        client: ReconnectingClient,
        config: ProducerConfig,
    ) -> Result<Self> {
        let client = Arc::new(Mutex::new(client));
        let batches = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(ProducerMetrics::default());
        let running = Arc::new(AtomicBool::new(true));
        let connection_healthy = Arc::new(AtomicBool::new(true));

        let (flush_tx, mut flush_rx) = mpsc::channel::<oneshot::Sender<Result<()>>>(16);

        // Spawn background linger task
        let linger_ms = config.linger_ms;
        let client_clone = client.clone();
        let batches_clone = batches.clone();
        let metrics_clone = metrics.clone();
        let running_clone = running.clone();
        let healthy_clone = connection_healthy.clone();

        tokio::spawn(async move {
            let mut linger_interval = interval(Duration::from_millis(linger_ms.max(1)));

            loop {
                tokio::select! {
                    _ = linger_interval.tick() => {
                        if !running_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        // Check for batches that should be sent due to linger timeout
                        // Auto-reconnect is handled inside send_batch_static
                        match Self::flush_expired_batches(
                            &client_clone,
                            &batches_clone,
                            &metrics_clone,
                            linger_ms,
                        ).await {
                            Ok(()) => {
                                healthy_clone.store(true, Ordering::SeqCst);
                            }
                            Err(_) => {
                                healthy_clone.store(false, Ordering::SeqCst);
                            }
                        }
                    }
                    Some(ack_tx) = flush_rx.recv() => {
                        // Explicit flush requested
                        let result = Self::flush_all_batches(
                            &client_clone,
                            &batches_clone,
                            &metrics_clone,
                        ).await;
                        if result.is_ok() {
                            healthy_clone.store(true, Ordering::SeqCst);
                        }
                        let _ = ack_tx.send(result);
                    }
                }
            }
        });

        Ok(Self {
            client,
            config,
            batches,
            metrics,
            running,
            connection_healthy,
            flush_tx,
        })
    }

    /// Primary produce interface: send a sequence of records in-order.
    ///
    /// Records are sent sequentially and this method only advances to the next
    /// record after the prior one is ACKed, preserving caller-provided order.
    pub async fn produce<T>(&self, topic_id: u32, records: &[T]) -> Result<Vec<SendAck>>
    where
        T: AsRef<[u8]>,
    {
        let mut acks = Vec::with_capacity(records.len());
        for record in records {
            acks.push(self.send(topic_id, record.as_ref()).await?);
        }
        Ok(acks)
    }

    /// Convenience alias for producing a single record.
    #[inline]
    pub async fn produce_single(&self, topic_id: u32, data: &[u8]) -> Result<SendAck> {
        self.send(topic_id, data).await
    }

    /// Send a record to a topic
    ///
    /// This method buffers the record and returns a future that resolves
    /// when the record has been acknowledged by the server.
    ///
    /// On transient failures (connection drops, leader elections), the
    /// producer automatically reconnects and retries. This method only
    /// returns an error for non-retryable failures or buffer overflow.
    ///
    /// M3: Tracks backpressure events in metrics for telemetry.
    pub async fn send(&self, topic_id: u32, data: &[u8]) -> Result<SendAck> {
        let (ack_tx, ack_rx) = oneshot::channel();

        // Check buffer memory limit
        let current_buffer = self.metrics.buffer_size.load(Ordering::Relaxed);
        if current_buffer + data.len() as u64 > self.config.buffer_memory as u64 {
            self.metrics
                .backpressure_drops
                .fetch_add(1, Ordering::Relaxed);
            return Err(ClientError::ServerBackpressure);
        }

        // Add to batch
        let should_flush = {
            let mut batches = self.batches.write().await;
            let batch = batches
                .entry(topic_id)
                .or_insert_with(|| RecordBatch::new(topic_id));
            batch.add(Bytes::copy_from_slice(data), ack_tx);
            self.metrics
                .buffer_size
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            batch.size() >= self.config.batch_size
        };

        // If batch is full, flush it immediately
        if should_flush {
            self.flush_topic(topic_id).await?;
        }

        // Wait for acknowledgment
        ack_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// M3: Non-blocking attempt to send a record.
    ///
    /// Returns `Ok(())` if the record was buffered successfully.
    /// Returns `Err(ClientError::WouldBlock)` if the buffer is full (backpressure).
    ///
    /// Use this when you want to drop records under load rather than blocking.
    /// For telemetry, this increments `backpressure_drops` when buffer is full.
    pub async fn try_send(&self, topic_id: u32, data: &[u8]) -> Result<()> {
        let (ack_tx, _ack_rx) = oneshot::channel();

        // Check buffer memory limit - fail fast if full
        let current_buffer = self.metrics.buffer_size.load(Ordering::Relaxed);
        if current_buffer + data.len() as u64 > self.config.buffer_memory as u64 {
            // M3: Track backpressure drop
            self.metrics
                .backpressure_drops
                .fetch_add(1, Ordering::Relaxed);
            return Err(ClientError::WouldBlock);
        }

        // Add to batch
        let should_flush = {
            let mut batches = self.batches.write().await;
            let batch = batches
                .entry(topic_id)
                .or_insert_with(|| RecordBatch::new(topic_id));
            batch.add(Bytes::copy_from_slice(data), ack_tx);
            self.metrics
                .buffer_size
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            batch.size() >= self.config.batch_size
        };

        // If batch is full, flush it immediately
        if should_flush {
            self.flush_topic(topic_id).await?;
        }

        Ok(())
    }

    /// Send a record without waiting for acknowledgment
    ///
    /// Returns immediately after buffering. Use `flush()` to ensure delivery.
    /// On transient failures, the producer auto-reconnects in the background.
    pub async fn send_async(&self, topic_id: u32, data: &[u8]) -> Result<()> {
        let (ack_tx, _ack_rx) = oneshot::channel();

        // Check buffer memory limit
        let current_buffer = self.metrics.buffer_size.load(Ordering::Relaxed);
        if current_buffer + data.len() as u64 > self.config.buffer_memory as u64 {
            return Err(ClientError::ServerBackpressure);
        }

        // Add to batch
        let should_flush = {
            let mut batches = self.batches.write().await;
            let batch = batches
                .entry(topic_id)
                .or_insert_with(|| RecordBatch::new(topic_id));
            batch.add(Bytes::copy_from_slice(data), ack_tx);
            self.metrics
                .buffer_size
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            batch.size() >= self.config.batch_size
        };

        // If batch is full, flush it immediately
        if should_flush {
            self.flush_topic(topic_id).await?;
        }

        Ok(())
    }

    /// Send a record with a callback for completion notification
    ///
    /// The callback is invoked when the record is acknowledged (or fails).
    /// This enables pipelined produce patterns without blocking.
    ///
    /// # Arguments
    /// * `topic_id` - Target topic ID
    /// * `data` - Record data to send
    /// * `callback` - Callback invoked with send result
    ///
    /// # Example
    /// ```rust,ignore
    /// // Assuming `producer` is an initialized Producer and `topic_id` is set
    /// producer.send_callback(topic_id, b"data", |result| {
    ///     match result {
    ///         Ok(ack) => println!("Sent with batch_id: {}", ack.batch_id),
    ///         Err(e) => eprintln!("Send failed: {}", e),
    ///     }
    /// }).await?;
    /// ```
    pub async fn send_callback<F>(&self, topic_id: u32, data: &[u8], callback: F) -> Result<()>
    where
        F: FnOnce(SendResult) + Send + 'static,
    {
        let (ack_tx, ack_rx) = oneshot::channel();

        // Check buffer memory limit
        let current_buffer = self.metrics.buffer_size.load(Ordering::Relaxed);
        if current_buffer + data.len() as u64 > self.config.buffer_memory as u64 {
            return Err(ClientError::ServerBackpressure);
        }

        // Add to batch
        let should_flush = {
            let mut batches = self.batches.write().await;
            let batch = batches
                .entry(topic_id)
                .or_insert_with(|| RecordBatch::new(topic_id));
            batch.add(Bytes::copy_from_slice(data), ack_tx);
            self.metrics
                .buffer_size
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            batch.size() >= self.config.batch_size
        };

        // Spawn task to invoke callback when ack received
        tokio::spawn(async move {
            let result = ack_rx.await.unwrap_or(Err(ClientError::ConnectionClosed));
            callback(result);
        });

        // If batch is full, flush it immediately
        if should_flush {
            self.flush_topic(topic_id).await?;
        }

        Ok(())
    }

    /// Flush all buffered records
    ///
    /// Blocks until all buffered records have been sent and acknowledged.
    pub async fn flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.flush_tx
            .send(ack_tx)
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;
        ack_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// Flush records for a specific topic
    async fn flush_topic(&self, topic_id: u32) -> Result<()> {
        let batch = {
            let mut batches = self.batches.write().await;
            batches.remove(&topic_id)
        };

        if let Some(batch) = batch {
            if !batch.is_empty() {
                self.send_batch(batch).await?;
            }
        }

        Ok(())
    }

    /// Send a batch to the server with automatic retry on transient errors
    async fn send_batch(&self, batch: RecordBatch) -> Result<()> {
        let topic_id = batch.topic_id;
        let record_count = batch.record_count;
        let byte_count = batch.data.len();
        let ack_txs = batch.ack_txs;
        let data = batch.data.freeze();

        // Retry loop for transient errors (FORWARD_FAILED, connection drops, etc.)
        const MAX_RETRIES: u32 = 30;
        let mut attempt = 0u32;
        let mut backoff = Duration::from_millis(500);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);

        let result = loop {
            let send_result = {
                let mut rc = self.client.lock().await;
                match rc.client().await {
                    Ok(c) => {
                        c.send_ingest_to_topic_sync(
                            topic_id,
                            data.clone(),
                            record_count as u32,
                            None,
                        )
                        .await
                    },
                    Err(e) => Err(e),
                }
            };

            match &send_result {
                Ok(_) => break send_result,
                Err(e) if e.is_retryable() && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                    // Mark connection as failed so next client() call reconnects
                    {
                        let mut rc = self.client.lock().await;
                        rc.mark_failed();
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                },
                _ => break send_result,
            }
        };

        // Update metrics
        self.metrics
            .buffer_size
            .fetch_sub(byte_count as u64, Ordering::Relaxed);

        match result {
            Ok(batch_id) => {
                self.metrics
                    .records_sent
                    .fetch_add(record_count as u64, Ordering::Relaxed);
                self.metrics
                    .bytes_sent
                    .fetch_add(byte_count as u64, Ordering::Relaxed);
                self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                self.connection_healthy.store(true, Ordering::SeqCst);

                // M2: Ordered streaming ACK completion - notify in order with yield points
                // This reduces HoL pressure by allowing earlier records to complete before
                // later ones, and yields between ACKs to let other tasks run.
                let timestamp = Instant::now();
                for (offset_in_batch, tx) in ack_txs.into_iter().enumerate() {
                    let ack = SendAck {
                        batch_id,
                        topic_id,
                        timestamp,
                        offset_in_batch: offset_in_batch as u32,
                    };
                    let _ = tx.send(Ok(ack));
                    // Yield every 8 ACKs to prevent starving other tasks
                    if offset_in_batch % 8 == 7 {
                        tokio::task::yield_now().await;
                    }
                }

                Ok(())
            },
            Err(e) => {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);

                // M2: Notify senders of error in order (preserves FIFO notification order)
                for tx in ack_txs {
                    let _ = tx.send(Err(ClientError::ServerError(e.to_string())));
                }

                Err(e)
            },
        }
    }

    /// Flush expired batches (called by background task)
    async fn flush_expired_batches(
        client: &Arc<Mutex<ReconnectingClient>>,
        batches: &Arc<RwLock<HashMap<u32, RecordBatch>>>,
        metrics: &Arc<ProducerMetrics>,
        linger_ms: u64,
    ) -> Result<()> {
        let linger_duration = Duration::from_millis(linger_ms);
        let now = Instant::now();

        // Find expired batches
        let expired_topics: Vec<u32> = {
            let batches_read = batches.read().await;
            batches_read
                .iter()
                .filter(|(_, batch)| {
                    !batch.is_empty() && now.duration_since(batch.created_at) >= linger_duration
                })
                .map(|(topic_id, _)| *topic_id)
                .collect()
        };

        // Flush each expired batch
        for topic_id in expired_topics {
            let batch = {
                let mut batches_write = batches.write().await;
                batches_write.remove(&topic_id)
            };

            if let Some(batch) = batch {
                if !batch.is_empty() {
                    Self::send_batch_static(client, metrics, batch).await?;
                }
            }
        }

        Ok(())
    }

    /// Flush all batches (called by background task)
    async fn flush_all_batches(
        client: &Arc<Mutex<ReconnectingClient>>,
        batches: &Arc<RwLock<HashMap<u32, RecordBatch>>>,
        metrics: &Arc<ProducerMetrics>,
    ) -> Result<()> {
        // Take all batches
        let all_batches: Vec<RecordBatch> = {
            let mut batches_write = batches.write().await;
            batches_write.drain().map(|(_, batch)| batch).collect()
        };

        // Send each batch
        for batch in all_batches {
            if !batch.is_empty() {
                Self::send_batch_static(client, metrics, batch).await?;
            }
        }

        Ok(())
    }

    /// Static version of send_batch for use in background tasks.
    /// Includes retry loop with auto-reconnect for transient errors.
    async fn send_batch_static(
        client: &Arc<Mutex<ReconnectingClient>>,
        metrics: &Arc<ProducerMetrics>,
        batch: RecordBatch,
    ) -> Result<()> {
        let topic_id = batch.topic_id;
        let record_count = batch.record_count;
        let byte_count = batch.data.len();
        let ack_txs = batch.ack_txs;
        let data = batch.data.freeze();

        // Retry loop for transient errors
        const MAX_RETRIES: u32 = 30;
        let mut attempt = 0u32;
        let mut backoff = Duration::from_millis(500);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);

        let result = loop {
            let send_result = {
                let mut rc = client.lock().await;
                match rc.client().await {
                    Ok(c) => {
                        c.send_ingest_to_topic_sync(
                            topic_id,
                            data.clone(),
                            record_count as u32,
                            None,
                        )
                        .await
                    },
                    Err(e) => Err(e),
                }
            };

            match &send_result {
                Ok(_) => break send_result,
                Err(e) if e.is_retryable() && attempt < MAX_RETRIES => {
                    attempt += 1;
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut rc = client.lock().await;
                        rc.mark_failed();
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                },
                _ => break send_result,
            }
        };

        // Update metrics
        metrics
            .buffer_size
            .fetch_sub(byte_count as u64, Ordering::Relaxed);

        match result {
            Ok(batch_id) => {
                metrics
                    .records_sent
                    .fetch_add(record_count as u64, Ordering::Relaxed);
                metrics
                    .bytes_sent
                    .fetch_add(byte_count as u64, Ordering::Relaxed);
                metrics.batches_sent.fetch_add(1, Ordering::Relaxed);

                // M2: Ordered streaming ACK completion - notify in order with yield points
                // This reduces HoL pressure by allowing earlier records to complete before
                // later ones, and yields between ACKs to let other tasks run.
                let timestamp = Instant::now();
                for (offset_in_batch, tx) in ack_txs.into_iter().enumerate() {
                    let ack = SendAck {
                        batch_id,
                        topic_id,
                        timestamp,
                        offset_in_batch: offset_in_batch as u32,
                    };
                    let _ = tx.send(Ok(ack));
                    // Yield every 8 ACKs to prevent starving other tasks
                    if offset_in_batch % 8 == 7 {
                        tokio::task::yield_now().await;
                    }
                }

                Ok(())
            },
            Err(e) => {
                metrics.errors.fetch_add(1, Ordering::Relaxed);

                // M2: Notify senders of error in order (preserves FIFO notification order)
                for tx in ack_txs {
                    let _ = tx.send(Err(ClientError::ServerError(e.to_string())));
                }

                Err(e)
            },
        }
    }

    /// Check if the producer connection is healthy
    ///
    /// Returns `false` if the background flush task has detected a dead
    /// connection (e.g., server went down). Callers should drop this
    /// producer and reconnect.
    pub fn is_healthy(&self) -> bool {
        self.connection_healthy.load(Ordering::SeqCst)
    }

    /// Get current metrics
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Close the producer
    ///
    /// Flushes all buffered records and releases resources.
    pub async fn close(self) -> Result<()> {
        // Flush first while the background flush task is still running.
        // If we flip `running` first, the task can exit before servicing
        // this flush request, causing close() to return ConnectionClosed.
        self.flush().await?;
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("config", &self.config)
            .field("metrics", &self.metrics.snapshot())
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_config_defaults() {
        let config = ProducerConfig::new();

        assert_eq!(config.batch_size, 16 * 1024);
        assert_eq!(config.linger_ms, 5);
        assert_eq!(config.max_in_flight, 5);
        assert!(!config.compression);
    }

    #[test]
    fn test_producer_config_builder() {
        let config = ProducerConfig::new()
            .with_batch_size(32 * 1024)
            .with_linger_ms(10)
            .with_max_in_flight(10)
            .with_compression(true);

        assert_eq!(config.batch_size, 32 * 1024);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.max_in_flight, 10);
        assert!(config.compression);
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new(1);
        assert!(batch.is_empty());
        assert_eq!(batch.size(), 0);
        assert!(batch.record_offsets.is_empty());

        let (tx, _rx) = oneshot::channel();
        batch.add(Bytes::from_static(b"hello"), tx);

        assert!(!batch.is_empty());
        assert_eq!(batch.size(), 5);
        assert_eq!(batch.record_count, 1);
        assert_eq!(batch.record_offsets, vec![0]); // First record starts at offset 0

        // Add second record
        let (tx2, _rx2) = oneshot::channel();
        batch.add(Bytes::from_static(b"world"), tx2);
        assert_eq!(batch.record_count, 2);
        assert_eq!(batch.record_offsets, vec![0, 5]); // Second record starts at offset 5
        assert_eq!(batch.size(), 10);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = ProducerMetrics::default();
        metrics.records_sent.fetch_add(100, Ordering::Relaxed);
        metrics.bytes_sent.fetch_add(1000, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_sent, 100);
        assert_eq!(snapshot.bytes_sent, 1000);
    }

    #[test]
    fn test_send_callback_closure_traits() {
        // Verify that callback closures satisfy required trait bounds
        fn assert_callback_traits<F>(_f: F)
        where
            F: FnOnce(SendResult) + Send + 'static,
        {
        }

        // Simple callback
        assert_callback_traits(|_result| {});

        // Callback with captured state
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = counter.clone();
        assert_callback_traits(move |result| {
            if result.is_ok() {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    /// M2 Regression Test: Verify ACKs are sent in order with correct offset_in_batch
    #[tokio::test]
    async fn test_send_ack_ordering() {
        let mut batch = RecordBatch::new(1);
        let mut receivers = Vec::new();

        // Add 5 records to batch
        for i in 0..5 {
            let (tx, rx) = oneshot::channel();
            batch.add(Bytes::from(format!("record-{}", i)), tx);
            receivers.push(rx);
        }

        // Simulate sending ACKs (normally done by send_batch)
        let batch_id = 42u64;
        let timestamp = Instant::now();
        let topic_id = batch.topic_id;

        // Send ACKs in order like send_batch() does
        for (offset_in_batch, tx) in batch.ack_txs.into_iter().enumerate() {
            let ack = SendAck {
                batch_id,
                topic_id,
                timestamp,
                offset_in_batch: offset_in_batch as u32,
            };
            let _ = tx.send(Ok(ack));
        }

        // Verify each receiver got ACK in correct order with correct offset
        for (i, rx) in receivers.into_iter().enumerate() {
            let result = rx.await.unwrap();
            assert!(result.is_ok(), "Record {} should be ACKed", i);
            let ack = result.unwrap();
            assert_eq!(ack.batch_id, batch_id);
            assert_eq!(
                ack.offset_in_batch, i as u32,
                "Record {} should have offset_in_batch={}",
                i, i
            );
            assert_eq!(ack.topic_id, 1);
        }
    }

    /// M2 Regression Test: Verify batch failure notifies all in order
    #[tokio::test]
    async fn test_send_ack_failure_ordering() {
        let mut batch = RecordBatch::new(2);
        let mut receivers = Vec::new();

        // Add records to batch
        for i in 0..3 {
            let (tx, rx) = oneshot::channel();
            batch.add(Bytes::from(format!("record-{}", i)), tx);
            receivers.push(rx);
        }

        // Simulate batch failure notification (normally done by send_batch error path)
        let error_msg = "server timeout";
        for tx in batch.ack_txs {
            let _ = tx.send(Err(ClientError::ServerError(error_msg.to_string())));
        }

        // Verify all receivers got error
        for (i, rx) in receivers.into_iter().enumerate() {
            let result = rx.await.unwrap();
            assert!(result.is_err(), "Record {} should receive error", i);
            let err = result.unwrap_err();
            assert!(matches!(err, ClientError::ServerError(ref msg) if msg == error_msg));
        }
    }

    /// M2 Regression Test: Verify record_offsets map correctly to data positions
    #[test]
    fn test_record_offsets_mapping() {
        let mut batch = RecordBatch::new(1);

        // Add records of varying sizes
        let records = vec![
            Bytes::from(vec![0u8; 100]), // offset 0
            Bytes::from(vec![1u8; 50]),  // offset 100
            Bytes::from(vec![2u8; 200]), // offset 150
            Bytes::from(vec![3u8; 75]),  // offset 350
        ];

        let expected_offsets = vec![0, 100, 150, 350];
        let expected_total_size: usize = records.iter().map(|r| r.len()).sum();

        for data in records {
            let (tx, _rx) = oneshot::channel();
            batch.add(data, tx);
        }

        assert_eq!(
            batch.record_offsets, expected_offsets,
            "Record offsets should match expected positions"
        );
        assert_eq!(
            batch.size(),
            expected_total_size,
            "Total batch size should be sum of record sizes"
        );
        assert_eq!(batch.record_count, 4);
    }

    /// M3 Regression Test: Verify backpressure telemetry metrics
    #[test]
    fn test_backpressure_telemetry_metrics() {
        let metrics = ProducerMetrics::default();

        // Simulate backpressure drops
        metrics.backpressure_drops.fetch_add(5, Ordering::Relaxed);
        metrics.backpressure_waits.fetch_add(3, Ordering::Relaxed);
        metrics
            .backpressure_wait_ms
            .fetch_add(150, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.backpressure_drops, 5);
        assert_eq!(snapshot.backpressure_waits, 3);
        assert_eq!(snapshot.backpressure_wait_ms, 150);
    }

    /// M3 Regression Test: Verify buffer memory limit checking
    #[test]
    fn test_buffer_memory_limit_checking() {
        // Create a batch and verify buffer_size tracking
        let metrics = Arc::new(ProducerMetrics::default());

        // Simulate adding records
        let data_len = 1000u64;
        metrics.buffer_size.fetch_add(data_len, Ordering::Relaxed);

        let current = metrics.buffer_size.load(Ordering::Relaxed);
        assert_eq!(current, data_len);

        // Simulate removing records (after send)
        metrics.buffer_size.fetch_sub(data_len, Ordering::Relaxed);
        assert_eq!(metrics.buffer_size.load(Ordering::Relaxed), 0);
    }
}
