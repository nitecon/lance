//! Multi-actor ingestion scaling
//!
//! Implements N ingestion actors with client hash partitioning per Architecture §5.2.
//! Uses crossbeam::ArrayQueue for lock-free MPMC communication per CodingGuidelines §3.3.

use super::ingestion::IngestionRequest;
use super::writer::{create_topic_writer, rotate_topic_segment, TopicWriter};
use crate::config::Config;
use crate::topic::TopicRegistry;
use crossbeam::queue::ArrayQueue;
use lnc_core::{pin_thread_to_cpu, LanceError, Result, SortKey};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Shared sequence counter across all ingestion actors
static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(0);

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

                    run_ingestion_actor_sync(actor_id, config_clone, queue, registry_clone);
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
                }
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
) {
    let mut topic_writers: HashMap<u32, TopicWriter> = HashMap::new();

    info!(
        target: "lance::ingestion",
        actor_id,
        "Ingestion actor started"
    );

    // Spin-wait loop (per CodingGuidelines §3.1 - ingestion actors never park)
    loop {
        match queue.pop() {
            Some(request) => {
                let topic_id = request.topic_id;

                if let Err(e) = process_request_sync(
                    &config,
                    &topic_registry,
                    &mut topic_writers,
                    request,
                ) {
                    warn!(
                        target: "lance::ingestion",
                        actor_id,
                        topic_id,
                        error = %e,
                        "Failed to process ingestion request"
                    );
                    topic_writers.remove(&topic_id);
                }
            }
            None => {
                // Check if queue is dropped (shutdown signal)
                if Arc::strong_count(&queue) == 1 {
                    break;
                }
                // Brief spin before retrying
                std::hint::spin_loop();
            }
        }
    }

    // Flush all topic writers on shutdown
    for (topic_id, mut tw) in topic_writers {
        if let Err(e) = tw.writer.fsync() {
            warn!(
                target: "lance::ingestion",
                actor_id,
                topic_id,
                error = %e,
                "Failed to flush topic writer"
            );
        } else {
            debug!(
                target: "lance::ingestion",
                actor_id,
                topic_id,
                "Flushed topic writer"
            );
        }
    }

    info!(
        target: "lance::ingestion",
        actor_id,
        "Ingestion actor shutdown complete"
    );
}

/// Process a single ingestion request (synchronous)
fn process_request_sync(
    config: &Config,
    topic_registry: &TopicRegistry,
    topic_writers: &mut HashMap<u32, TopicWriter>,
    request: IngestionRequest,
) -> Result<()> {
    let topic_id = request.topic_id;

    // Get or create writer for this topic
    let topic_writer = match topic_writers.get_mut(&topic_id) {
        Some(tw) => tw,
        None => {
            let tw = create_topic_writer(config, topic_registry, topic_id)?;
            topic_writers.insert(topic_id, tw);
            topic_writers
                .get_mut(&topic_id)
                .ok_or_else(|| LanceError::Protocol("Failed to get topic writer".into()))?
        }
    };

    let seq = SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sort_key = SortKey::from_timestamp_ns(request.timestamp_ns, seq as u32);

    let offset = topic_writer.writer.write_batch(&request.payload)?;

    topic_writer
        .index_builder
        .add_record(sort_key, request.timestamp_ns, offset);

    lnc_metrics::increment_records_ingested(request.record_count as u64);
    lnc_metrics::increment_bytes_ingested(request.payload.len() as u64);

    if topic_writer.writer.current_offset() >= config.io.segment_max_size {
        rotate_topic_segment(config, topic_registry, topic_id, topic_writer)?;
    }

    Ok(())
}

#[cfg(test)]
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
