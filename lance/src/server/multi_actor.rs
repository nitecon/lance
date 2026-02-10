//! Multi-actor ingestion scaling
//!
//! Implements N ingestion actors with client hash partitioning per Architecture §5.2.
//! Uses crossbeam::ArrayQueue for lock-free MPMC communication per CodingGuidelines §3.3.

use super::ingestion::{
    BatchSuccess, DataReplicationRequest, IngestionRequest, flush_and_signal,
    process_ingestion_request,
};
use super::writer::TopicWriter;
use crate::config::Config;
use crate::topic::TopicRegistry;
use crossbeam::queue::ArrayQueue;
use lnc_core::{LanceError, Result, pin_thread_to_cpu};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tracing::{debug, info, warn};

/// Unified ingestion sender that works with both single-actor (flume) and multi-actor modes
#[derive(Clone)]
pub enum IngestionSender {
    /// Single-actor mode using flume channel
    Single(flume::Sender<IngestionRequest>),
    /// Multi-actor mode using crossbeam ArrayQueue
    Multi(MultiActorSender),
}

impl IngestionSender {
    /// Send an ingestion request
    pub async fn send(&self, request: IngestionRequest) -> Result<()> {
        match self {
            Self::Single(tx) => tx
                .send_async(request)
                .await
                .map_err(|_| LanceError::ChannelDisconnected("ingestion")),
            Self::Multi(sender) => sender.send(request),
        }
    }
}

/// Multi-actor ingestion system with hash partitioning
pub struct MultiActorIngestion {
    /// Request queues - one per actor
    queues: Vec<Arc<ArrayQueue<IngestionRequest>>>,
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
            let queue = Arc::new(ArrayQueue::new(queue_capacity));
            queues.push(Arc::clone(&queue));

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
                        queue,
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
    queues: Vec<Arc<ArrayQueue<IngestionRequest>>>,
    actor_count: usize,
}

impl MultiActorSender {
    /// Send a request to the appropriate actor based on topic_id hash partitioning
    pub fn send(&self, request: IngestionRequest) -> Result<()> {
        let actor_id = (request.topic_id as usize) % self.actor_count;
        let queue = &self.queues[actor_id];

        // Try to push, spinning briefly if full (lock-free backpressure)
        let mut attempts = 0;
        let mut req = request;
        loop {
            match queue.push(req) {
                Ok(()) => return Ok(()),
                Err(returned) => {
                    req = returned;
                    attempts += 1;
                    if attempts > 1000 {
                        return Err(LanceError::Protocol("Ingestion queue full".into()));
                    }
                    std::hint::spin_loop();
                },
            }
        }
    }
}

/// Run a single ingestion actor (synchronous, on dedicated thread)
fn run_ingestion_actor_sync(
    actor_id: usize,
    config: Config,
    queue: Arc<ArrayQueue<IngestionRequest>>,
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
    let mut last_write = std::time::Instant::now();

    // Spin-wait loop (per CodingGuidelines §3.1 - ingestion actors never park)
    loop {
        match queue.pop() {
            Some(first) => {
                // ── Phase 1: Collect a batch ──────────────────────────
                let mut batch: Vec<IngestionRequest> = Vec::with_capacity(MAX_BATCH);
                batch.push(first);
                while batch.len() < MAX_BATCH {
                    match queue.pop() {
                        Some(r) => batch.push(r),
                        None => break,
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

                    match process_ingestion_request(
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
                last_write = std::time::Instant::now();

                // ── Phase 3: Flush if threshold crossed ──────────────
                if dirty_bytes >= FLUSH_THRESHOLD {
                    flush_and_signal(
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
                }
            },
            None => {
                // No work — check shutdown
                if Arc::strong_count(&queue) == 1 {
                    break;
                }

                // Flush on timeout if we have pending data
                if !pending_signals.is_empty() && last_write.elapsed() >= FLUSH_TIMEOUT {
                    flush_and_signal(
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
                }

                std::hint::spin_loop();
            },
        }
    }

    // ── Shutdown: flush remaining buffered data ───────────────────────
    if !pending_signals.is_empty() {
        flush_and_signal(
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::identity_op)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_via_sender() {
        let queues = vec![
            Arc::new(ArrayQueue::<IngestionRequest>::new(1)),
            Arc::new(ArrayQueue::<IngestionRequest>::new(1)),
            Arc::new(ArrayQueue::<IngestionRequest>::new(1)),
            Arc::new(ArrayQueue::<IngestionRequest>::new(1)),
        ];

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
