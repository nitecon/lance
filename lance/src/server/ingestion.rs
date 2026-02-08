//! Ingestion actor - batch processing and record ingestion

use super::writer::{TopicWriter, create_topic_writer, rotate_topic_segment};
use crate::config::Config;
use crate::topic::TopicRegistry;
use bytes::Bytes;
use lnc_core::{BatchPool, LanceError, Result, SortKey};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info};

static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Request to ingest data into a topic
pub struct IngestionRequest {
    pub topic_id: u32,
    pub timestamp_ns: u64,
    pub record_count: u32,
    pub payload: Bytes,
}

/// Request to replicate data to followers (sent after local write)
pub struct DataReplicationRequest {
    pub topic_id: u32,
    pub payload: Bytes,
}

/// Run the ingestion actor that processes incoming batches
pub async fn run_ingestion_actor(
    config: Config,
    rx: flume::Receiver<IngestionRequest>,
    _batch_pool: Arc<BatchPool>,
    topic_registry: Arc<TopicRegistry>,
    replication_tx: Option<tokio::sync::mpsc::Sender<DataReplicationRequest>>,
) -> Result<()> {
    let mut topic_writers: HashMap<u32, TopicWriter> = HashMap::new();

    info!(
        target: "lance::ingestion",
        "Ingestion actor started"
    );

    while let Ok(request) = rx.recv_async().await {
        let topic_id = request.topic_id;
        let payload = request.payload.clone();

        // Process request, handling errors gracefully without crashing the actor
        match process_ingestion_request(&config, &topic_registry, &mut topic_writers, request) {
            Ok(()) => {
                // After successful local write, queue for replication
                if let Some(ref tx) = replication_tx {
                    let _ = tx.try_send(DataReplicationRequest { topic_id, payload });
                }
            },
            Err(e) => {
                tracing::warn!(
                    target: "lance::ingestion",
                    topic_id,
                    error = %e,
                    "Failed to process ingestion request"
                );
                // Remove the topic writer if it exists (may be corrupted)
                topic_writers.remove(&topic_id);
            },
        }
    }

    // Flush all topic writers on shutdown
    for (topic_id, mut tw) in topic_writers {
        tw.writer.fsync()?;
        debug!(
            target: "lance::ingestion",
            topic_id,
            "Flushed topic writer"
        );
    }

    info!(target: "lance::ingestion", "Ingestion actor shutdown complete");

    Ok(())
}

/// Write replicated data received from the leader to a local topic segment.
/// Used by followers to apply data replication entries.
pub fn write_replicated_data(
    config: &Config,
    topic_registry: &TopicRegistry,
    topic_writers: &mut HashMap<u32, TopicWriter>,
    topic_id: u32,
    payload: &[u8],
) -> Result<()> {
    // Get or create writer for this topic
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

    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sort_key = SortKey::from_timestamp_ns(timestamp_ns, seq as u32);

    let offset = topic_writer.writer.write_batch(payload)?;

    topic_writer
        .index_builder
        .add_record(sort_key, timestamp_ns, offset);

    lnc_metrics::increment_bytes_ingested(payload.len() as u64);

    if topic_writer.writer.current_offset() >= config.io.segment_max_size {
        rotate_topic_segment(config, topic_registry, topic_id, topic_writer)?;
    }

    Ok(())
}

/// Process a single ingestion request
fn process_ingestion_request(
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
        },
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
