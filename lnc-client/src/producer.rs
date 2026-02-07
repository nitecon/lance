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

use crate::client::{ClientConfig, LanceClient};
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
}

/// Result of a send operation
pub type SendResult = Result<SendAck>;

/// Batch of records ready to send
struct RecordBatch {
    topic_id: u32,
    data: BytesMut,
    record_count: usize,
    ack_txs: Vec<oneshot::Sender<SendResult>>,
    created_at: Instant,
}

impl RecordBatch {
    fn new(topic_id: u32) -> Self {
        Self {
            topic_id,
            data: BytesMut::with_capacity(16 * 1024),
            record_count: 0,
            ack_txs: Vec::new(),
            created_at: Instant::now(),
        }
    }

    fn add(&mut self, data: Bytes, ack_tx: oneshot::Sender<SendResult>) {
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

/// Producer metrics
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
}

/// High-level producer with batching and async send
pub struct Producer {
    client: Arc<Mutex<LanceClient>>,
    config: ProducerConfig,
    batches: Arc<RwLock<HashMap<u32, RecordBatch>>>,
    metrics: Arc<ProducerMetrics>,
    running: Arc<AtomicBool>,
    flush_tx: mpsc::Sender<oneshot::Sender<Result<()>>>,
}

impl Producer {
    /// Connect to a LANCE server and create a producer
    ///
    /// The address can be either an IP:port (e.g., "127.0.0.1:1992") or
    /// a hostname:port (e.g., "lance.example.com:1992"). DNS resolution
    /// is performed automatically for hostnames.
    pub async fn connect(addr: &str, config: ProducerConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new(addr);
        client_config.connect_timeout = config.connect_timeout;

        let client = LanceClient::connect(client_config).await?;
        Self::from_client(client, config).await
    }

    /// Create a producer from an existing client connection
    pub async fn from_client(client: LanceClient, config: ProducerConfig) -> Result<Self> {
        let client = Arc::new(Mutex::new(client));
        let batches = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(ProducerMetrics::default());
        let running = Arc::new(AtomicBool::new(true));

        let (flush_tx, mut flush_rx) = mpsc::channel::<oneshot::Sender<Result<()>>>(16);

        // Spawn background linger task
        let linger_ms = config.linger_ms;
        let client_clone = client.clone();
        let batches_clone = batches.clone();
        let metrics_clone = metrics.clone();
        let running_clone = running.clone();

        tokio::spawn(async move {
            let mut linger_interval = interval(Duration::from_millis(linger_ms.max(1)));

            loop {
                tokio::select! {
                    _ = linger_interval.tick() => {
                        if !running_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        // Check for batches that should be sent due to linger timeout
                        let _ = Self::flush_expired_batches(
                            &client_clone,
                            &batches_clone,
                            &metrics_clone,
                            linger_ms,
                        ).await;
                    }
                    Some(ack_tx) = flush_rx.recv() => {
                        // Explicit flush requested
                        let result = Self::flush_all_batches(
                            &client_clone,
                            &batches_clone,
                            &metrics_clone,
                        ).await;
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
            flush_tx,
        })
    }

    /// Send a record to a topic
    ///
    /// This method buffers the record and returns a future that resolves
    /// when the record has been acknowledged by the server.
    pub async fn send(&self, topic_id: u32, data: &[u8]) -> Result<SendAck> {
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

        // If batch is full, flush it immediately
        if should_flush {
            self.flush_topic(topic_id).await?;
        }

        // Wait for acknowledgment
        ack_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// Send a record without waiting for acknowledgment
    ///
    /// Returns immediately after buffering. Use `flush()` to ensure delivery.
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

    /// Send a batch to the server
    async fn send_batch(&self, batch: RecordBatch) -> Result<()> {
        let topic_id = batch.topic_id;
        let record_count = batch.record_count;
        let byte_count = batch.data.len();
        let ack_txs = batch.ack_txs;
        let data = batch.data.freeze();

        // Send to server
        let result = {
            let mut client = self.client.lock().await;
            client
                .send_ingest_to_topic_sync(topic_id, data.clone(), record_count as u32, None)
                .await
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

                // Notify all senders
                let ack = SendAck {
                    batch_id,
                    topic_id,
                    timestamp: Instant::now(),
                };

                for tx in ack_txs {
                    let _ = tx.send(Ok(ack.clone()));
                }

                Ok(())
            },
            Err(e) => {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);

                // Notify all senders of error
                for tx in ack_txs {
                    let _ = tx.send(Err(ClientError::ServerError(e.to_string())));
                }

                Err(e)
            },
        }
    }

    /// Flush expired batches (called by background task)
    async fn flush_expired_batches(
        client: &Arc<Mutex<LanceClient>>,
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
        client: &Arc<Mutex<LanceClient>>,
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

    /// Static version of send_batch for use in background tasks
    async fn send_batch_static(
        client: &Arc<Mutex<LanceClient>>,
        metrics: &Arc<ProducerMetrics>,
        batch: RecordBatch,
    ) -> Result<()> {
        let topic_id = batch.topic_id;
        let record_count = batch.record_count;
        let byte_count = batch.data.len();
        let ack_txs = batch.ack_txs;
        let data = batch.data.freeze();

        // Send to server
        let result = {
            let mut client_guard = client.lock().await;
            client_guard
                .send_ingest_to_topic_sync(topic_id, data.clone(), record_count as u32, None)
                .await
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

                // Notify all senders
                let ack = SendAck {
                    batch_id,
                    topic_id,
                    timestamp: Instant::now(),
                };

                for tx in ack_txs {
                    let _ = tx.send(Ok(ack.clone()));
                }

                Ok(())
            },
            Err(e) => {
                metrics.errors.fetch_add(1, Ordering::Relaxed);

                // Notify all senders of error
                for tx in ack_txs {
                    let _ = tx.send(Err(ClientError::ServerError(e.to_string())));
                }

                Err(e)
            },
        }
    }

    /// Get current metrics
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Close the producer
    ///
    /// Flushes all buffered records and releases resources.
    pub async fn close(self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.flush().await?;
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

        let (tx, _rx) = oneshot::channel();
        batch.add(Bytes::from_static(b"hello"), tx);

        assert!(!batch.is_empty());
        assert_eq!(batch.size(), 5);
        assert_eq!(batch.record_count, 1);
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
}
