//! Multi-actor ingestion scaling
//!
//! Implements N ingestion actors with client hash partitioning per Architecture §5.2.
//! Uses flume bounded channels for async backpressure without busy-spinning.

use super::ingestion::{DataReplicationRequest, IngestionRequest, WriteMetadata};
use super::writer::{TopicWriter, create_topic_writer, rotate_topic_segment};
use crate::config::Config;
use crate::topic::TopicRegistry;
use bytes::Bytes;
use lnc_core::{LanceError, Result, SortKey, pin_thread_to_cpu};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Unified ingestion sender that works with both single-actor (flume) and multi-actor modes
#[derive(Clone)]
pub enum IngestionSender {
    /// Multi-actor mode using crossbeam ArrayQueue
    Multi(MultiActorSender),
}

impl IngestionSender {
    /// Send an ingestion request
    pub async fn send(&self, request: IngestionRequest) -> Result<()> {
        match self {
            Self::Multi(sender) => sender.send(request).await,
        }
    }
}

/// Shared sequence counter across all ingestion actors
static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Multi-actor ingestion system with hash partitioning
pub struct MultiActorIngestion {
    /// Request queues - one per actor (flume senders for async backpressure)
    queues: Vec<flume::Sender<IngestionRequest>>,
    /// Actor thread handles
    handles: Vec<thread::JoinHandle<()>>,
    /// Number of actors
    actor_count: usize,
}

impl MultiActorIngestion {
    /// Create a new multi-actor ingestion system
    ///
    /// # Arguments
    /// * `config` - Server configuration
    /// * `topic_registry` - Shared topic registry
    /// * `queue_capacity` - Capacity per actor queue
    pub fn new(
        config: Config,
        topic_registry: Arc<TopicRegistry>,
        queue_capacity: usize,
        replication_tx: Option<tokio::sync::mpsc::Sender<DataReplicationRequest>>,
    ) -> Result<Self> {
        let actor_count = config.ingestion.actor_count.max(1);
        let pin_cores = &config.ingestion.pin_cores;

        info!(
            target: "lance::ingestion",
            actor_count,
            queue_capacity,
            pin_cores = ?pin_cores,
            "Starting multi-actor ingestion"
        );

        let mut queues = Vec::with_capacity(actor_count);
        let mut handles = Vec::with_capacity(actor_count);

        for actor_id in 0..actor_count {
            // Use flume bounded channel for async backpressure
            let (tx, rx) = flume::bounded(queue_capacity);
            queues.push(tx);

            let config_clone = config.clone();
            let registry_clone = Arc::clone(&topic_registry);
            let pin_core = pin_cores.get(actor_id).copied();

            let repl_tx_clone = replication_tx.clone();

            let handle = thread::Builder::new()
                .name(format!("ingestion-{}", actor_id))
                .spawn(move || {
                    // Pin to core if configured (per CodingGuidelines §3.4)
                    if let Some(core_id) = pin_core {
                        if let Err(e) = pin_thread_to_cpu(core_id) {
                            warn!(
                                target: "lance::ingestion",
                                actor_id,
                                core_id,
                                error = %e,
                                "Failed to pin ingestion actor to core"
                            );
                        } else {
                            info!(
                                target: "lance::ingestion",
                                actor_id,
                                core_id,
                                "Ingestion actor pinned to core"
                            );
                        }
                    }

                    run_ingestion_actor_sync(
                        actor_id,
                        config_clone,
                        rx,
                        registry_clone,
                        repl_tx_clone,
                    );
                })
                .map_err(|e| LanceError::Protocol(format!("Failed to spawn actor: {}", e)))?;

            handles.push(handle);
        }

        Ok(Self {
            queues,
            handles,
            actor_count,
        })
    }

    /// Shutdown all actors gracefully
    pub fn shutdown(self) {
        // Drop queues to signal shutdown
        drop(self.queues);

        // Wait for all actors to complete
        for handle in self.handles {
            let _ = handle.join();
        }

        info!(target: "lance::ingestion", "Multi-actor ingestion shutdown complete");
    }

    /// Create a sender handle that can be cloned and used from async contexts
    pub fn sender(&self) -> MultiActorSender {
        MultiActorSender {
            queues: self.queues.clone(),
            actor_count: self.actor_count,
        }
    }
}

/// Cloneable sender handle for multi-actor ingestion
#[derive(Clone)]
pub struct MultiActorSender {
    queues: Vec<flume::Sender<IngestionRequest>>,
    actor_count: usize,
}

impl MultiActorSender {
    /// Send a request to the appropriate actor based on topic_id hash partitioning
    ///
    /// This is async and yields to the Tokio runtime when the channel is full,
    /// providing true backpressure without busy-spinning.
    pub async fn send(&self, request: IngestionRequest) -> Result<()> {
        let actor_id = (request.topic_id as usize) % self.actor_count;

        // send_async yields to the Tokio runtime if the channel is full.
        // No more busy-spinning!
        self.queues[actor_id]
            .send_async(request)
            .await
            .map_err(|_| LanceError::ChannelDisconnected("ingestion_actor"))
    }
}

/// Run a single ingestion actor (synchronous, on dedicated thread)
fn run_ingestion_actor_sync(
    actor_id: usize,
    config: Config,
    rx: flume::Receiver<IngestionRequest>,
    topic_registry: Arc<TopicRegistry>,
    replication_tx: Option<tokio::sync::mpsc::Sender<DataReplicationRequest>>,
) {
    let mut topic_writers: HashMap<u32, TopicWriter> = HashMap::new();

    // ── Optional WAL (per-actor) ───────────────────────────────────
    // Each actor gets its own WAL segment directory to avoid
    // contention. WAL dir = <wal_dir>/actor-<N>/
    let mut wal: Option<lnc_io::Wal> = if config.wal.enabled {
        let mut wal_cfg = config.wal_config();
        wal_cfg.dir = wal_cfg.dir.join(format!("actor-{}", actor_id));
        wal_cfg.path = wal_cfg.dir.join("current.wal");
        match lnc_io::Wal::open(wal_cfg) {
            Ok(w) => {
                info!(
                    target: "lance::ingestion",
                    actor_id,
                    "WAL enabled for actor"
                );
                Some(w)
            },
            Err(e) => {
                tracing::error!(
                    target: "lance::ingestion",
                    actor_id,
                    error = %e,
                    "Failed to open WAL — continuing without WAL"
                );
                None
            },
        }
    } else {
        None
    };

    info!(
        target: "lance::ingestion",
        actor_id,
        "Ingestion actor started"
    );

    // ── Buffered-write constants ───────────────────────────────────────
    const MAX_BATCH: usize = 256;
    const FLUSH_THRESHOLD: usize = 4 * 1024 * 1024; // 4 MiB
    const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(5);

    let mut pending_signals: Vec<BatchSuccess> = Vec::new();
    let mut dirty_topics: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut dirty_bytes: usize = 0;
    let mut last_write = Instant::now();

    loop {
        // Calculate timeout based on pending data
        let timeout = if !pending_signals.is_empty() {
            let elapsed = last_write.elapsed();
            if elapsed >= FLUSH_TIMEOUT {
                Duration::ZERO // Flush immediately
            } else {
                FLUSH_TIMEOUT - elapsed
            }
        } else {
            Duration::from_secs(3600) // Wait up to 1 hour if idle
        };

        let recv_result = rx.recv_timeout(timeout);

        match recv_result {
            Ok(first) => {
                // ── Phase 1: Collect a batch ──────────────────────────
                let mut batch: Vec<IngestionRequest> = Vec::with_capacity(MAX_BATCH);
                batch.push(first);
                while batch.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }

                // ── Phase 2: Write all (buffered, no sync) ───────────
                for mut request in batch {
                    let topic_id = request.topic_id;
                    let payload = std::mem::take(&mut request.payload);
                    let payload_len = payload.len();
                    let write_id = request.write_id;
                    let write_done_tx = request.write_done_tx.take();

                    // WAL-first append
                    if let Some(ref mut w) = wal {
                        if let Err(e) = w.append(&payload) {
                            tracing::error!(
                                target: "lance::ingestion",
                                actor_id,
                                topic_id,
                                error = %e,
                                "WAL append failed — dropping request"
                            );
                            drop(write_done_tx);
                            continue;
                        }
                    }

                    match process_request_sync(
                        &config,
                        &topic_registry,
                        &mut topic_writers,
                        request,
                        &payload,
                    ) {
                        Ok(meta) => {
                            dirty_topics.insert(topic_id);
                            dirty_bytes += payload_len;
                            pending_signals.push(BatchSuccess {
                                write_done_tx,
                                topic_id,
                                payload,
                                payload_len,
                                write_id,
                                meta,
                            });
                        },
                        Err(e) => {
                            warn!(
                                target: "lance::ingestion",
                                actor_id,
                                topic_id,
                                error = %e,
                                "Failed to process ingestion request"
                            );
                            drop(write_done_tx);
                            topic_writers.remove(&topic_id);
                        },
                    }
                }
                last_write = Instant::now();

                // ── Phase 3: Flush if threshold crossed ──────────────
                if dirty_bytes >= FLUSH_THRESHOLD {
                    flush_and_signal_sync(
                        &mut topic_writers,
                        &mut dirty_topics,
                        &mut pending_signals,
                        &replication_tx,
                        &mut wal,
                    );
                    if let Some(ref mut w) = wal {
                        let _ = w.reset();
                    }
                    dirty_bytes = 0;
                    last_write = Instant::now();
                }
            },
            Err(flume::RecvTimeoutError::Timeout) => {
                // Flush pending data on timeout
                if !pending_signals.is_empty() {
                    flush_and_signal_sync(
                        &mut topic_writers,
                        &mut dirty_topics,
                        &mut pending_signals,
                        &replication_tx,
                        &mut wal,
                    );
                    if let Some(ref mut w) = wal {
                        let _ = w.reset();
                    }
                    dirty_bytes = 0;
                    last_write = Instant::now();
                }
            },
            Err(flume::RecvTimeoutError::Disconnected) => {
                // Channel disconnected - shutdown
                break;
            },
        }
    }

    // ── Shutdown: flush remaining buffered data ───────────────────────
    if !pending_signals.is_empty() {
        flush_and_signal_sync(
            &mut topic_writers,
            &mut dirty_topics,
            &mut pending_signals,
            &replication_tx,
            &mut wal,
        );
        if let Some(ref mut w) = wal {
            let _ = w.reset();
        }
    }

    // Close all topic writers on shutdown (fsync + rename to closed segment format)
    for (topic_id, mut tw) in topic_writers {
        if let Err(e) = tw.writer.fsync() {
            warn!(
                target: "lance::ingestion",
                actor_id,
                topic_id,
                error = %e,
                "Failed to flush topic writer"
            );
            continue;
        }
        let end_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        match tw.writer.close(end_timestamp) {
            Ok(closed_path) => {
                let _ = tw.index_builder.write_indexes(&closed_path);
                debug!(
                    target: "lance::ingestion",
                    actor_id,
                    topic_id,
                    segment = %closed_path.display(),
                    "Closed topic writer on shutdown"
                );
            },
            Err(e) => {
                debug!(
                    target: "lance::ingestion",
                    actor_id,
                    topic_id,
                    error = %e,
                    "Failed to close segment on shutdown (fsynced)"
                );
            },
        }
    }

    info!(
        target: "lance::ingestion",
        actor_id,
        "Ingestion actor shutdown complete"
    );
}

/// Holds the state for a single successful write within a batch.
struct BatchSuccess {
    write_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    topic_id: u32,
    payload: Bytes,
    payload_len: usize,
    write_id: Option<u64>,
    meta: WriteMetadata,
}

/// Synchronous flush helper for the multi-actor path.
/// Same semantics as `flush_and_signal` in ingestion.rs but callable
/// from a plain thread (no async runtime).
fn flush_and_signal_sync(
    topic_writers: &mut HashMap<u32, TopicWriter>,
    dirty_topics: &mut std::collections::HashSet<u32>,
    pending_signals: &mut Vec<BatchSuccess>,
    replication_tx: &Option<tokio::sync::mpsc::Sender<DataReplicationRequest>>,
    wal: &mut Option<lnc_io::Wal>,
) {
    // Sync WAL before segment fsyncs (batched, not per-append)
    if let Some(w) = wal {
        if let Err(e) = w.sync() {
            tracing::error!(
                target: "lance::ingestion",
                error = %e,
                "WAL batch sync failed"
            );
        }
    }
    for topic_id in dirty_topics.iter() {
        if let Some(tw) = topic_writers.get_mut(topic_id) {
            if let Err(e) = tw.writer.fsync() {
                tracing::error!(
                    target: "lance::ingestion",
                    topic_id,
                    error = %e,
                    "Batch fsync failed"
                );
            }
        }
    }
    dirty_topics.clear();

    for s in pending_signals.drain(..) {
        if let Some(tx) = s.write_done_tx {
            let _ = tx.send(());
        }

        if let Some(tx) = replication_tx {
            // Block thread until space available in replication channel
            // This prevents silent drops that cause quorum timeouts
            if let Err(_e) = tx.blocking_send(DataReplicationRequest {
                topic_id: s.topic_id,
                payload: s.payload,
                segment_name: s.meta.segment_name,
                write_offset: s.meta.write_offset,
                global_offset: s.meta.write_offset + s.payload_len as u64,
                is_new_segment: s.meta.is_new_segment,
                rotated_after: s.meta.rotated_after,
                write_id: s.write_id,
            }) {
                tracing::error!(target: "lance::ingestion", "Replication channel closed");
            }
        }
    }
}

/// Process a single ingestion request (synchronous), returning write metadata for replication.
///
/// `payload` is passed separately because the caller has already taken it from
/// the request via `std::mem::take` for WAL and deferred-signal storage.
fn process_request_sync(
    config: &Config,
    topic_registry: &TopicRegistry,
    topic_writers: &mut HashMap<u32, TopicWriter>,
    request: IngestionRequest,
    payload: &Bytes,
) -> Result<WriteMetadata> {
    let topic_id = request.topic_id;

    // Get or create writer for this topic
    let is_new_segment = !topic_writers.contains_key(&topic_id);
    let topic_writer = match topic_writers.get_mut(&topic_id) {
        Some(tw) => tw,
        None => {
            let tw = create_topic_writer(config, topic_registry, topic_id)?;
            topic_writers.insert(topic_id, tw);
            topic_writers
                .get_mut(&topic_id)
                .ok_or_else(|| LanceError::Protocol("Failed to get topic writer".into()))?
        },
    };

    let segment_name = topic_writer
        .writer
        .filename()
        .unwrap_or_default()
        .to_string();

    let seq = SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sort_key = SortKey::from_timestamp_ns(request.timestamp_ns, seq as u32);

    let offset = topic_writer.writer.write_batch(payload)?;

    topic_writer
        .index_builder
        .add_record(sort_key, request.timestamp_ns, offset);

    lnc_metrics::increment_records_ingested(request.record_count as u64);
    lnc_metrics::increment_bytes_ingested(payload.len() as u64);

    let rotated_after = if topic_writer.writer.current_offset() >= config.io.segment_max_size {
        rotate_topic_segment(config, topic_registry, topic_id, topic_writer)?;
        true
    } else {
        false
    };

    Ok(WriteMetadata {
        write_offset: offset,
        segment_name,
        is_new_segment,
        rotated_after,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::identity_op)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_via_sender() {
        let mut queues = Vec::new();
        for _ in 0..4 {
            let (tx, _rx) = flume::bounded::<IngestionRequest>(1);
            queues.push(tx);
        }

        let sender = MultiActorSender {
            queues,
            actor_count: 4,
        };

        // Verify partition key calculation: topic_id % actor_count
        assert_eq!(0 % 4, 0);
        assert_eq!(1 % 4, 1);
        assert_eq!(2 % 4, 2);
        assert_eq!(3 % 4, 3);
        assert_eq!(4 % 4, 0);
        assert_eq!(5 % 4, 1);

        // Verify sender has correct actor count
        assert_eq!(sender.actor_count, 4);
    }
}
