//! Ingestion actor - batch processing and record ingestion

use super::writer::{TopicWriter, create_topic_writer, rotate_topic_segment};
use crate::config::Config;
use crate::topic::TopicRegistry;
use bytes::Bytes;
use lnc_core::{BatchPool, LanceError, Result, SortKey};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
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
    /// Quorum write ID (set in L3 mode so the forwarder can record ACKs)
    pub write_id: Option<u64>,
    /// Write completion signal — the connection handler awaits this before ACKing
    /// the client, guaranteeing the data is on disk before the ACK is sent.
    pub write_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Metadata returned from a successful write, used to build enriched replication entries
pub struct WriteMetadata {
    pub write_offset: u64,
    pub segment_name: String,
    pub is_new_segment: bool,
    pub rotated_after: bool,
}

/// Request to replicate data to followers (sent after local write)
pub struct DataReplicationRequest {
    pub topic_id: u32,
    pub payload: Bytes,
    /// Enriched replication metadata (present when leader has segment info)
    pub segment_name: String,
    pub write_offset: u64,
    pub global_offset: u64,
    pub is_new_segment: bool,
    pub rotated_after: bool,
    /// Quorum write ID for ACK tracking (L3 mode)
    pub write_id: Option<u64>,
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

    // ── Optional WAL ────────────────────────────────────────────────
    // When enabled, each payload is appended (length-framed) to the
    // WAL *before* the buffered segment write.  The WAL is fsync'd on
    // every append (sync_on_write=true), so it is the durability
    // backstop for L1 mode.  After segments are fsynced the WAL is
    // recycled (reset to byte 0).
    let mut wal: Option<lnc_io::Wal> = if config.wal.enabled {
        match lnc_io::Wal::open(config.wal_config()) {
            Ok(w) => {
                info!(
                    target: "lance::ingestion",
                    path = %config.wal_config().dir.display(),
                    size = config.wal.size,
                    "WAL enabled"
                );
                Some(w)
            },
            Err(e) => {
                tracing::error!(
                    target: "lance::ingestion",
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
        "Ingestion actor started"
    );

    // ── Buffered-write constants ───────────────────────────────────────
    // Writes are accumulated in SegmentWriter's 4 MiB BufWriter and in
    // `pending_signals` below.  We flush + fsync when EITHER:
    //   • accumulated dirty bytes >= FLUSH_THRESHOLD  (throughput path)
    //   • no new message arrives within FLUSH_TIMEOUT  (latency path)
    // This reduces expensive Ceph fdatasync calls from thousands/s to a
    // handful/s under load while keeping tail-latency bounded.
    const MAX_BATCH: usize = 256;
    const FLUSH_THRESHOLD: usize = 4 * 1024 * 1024; // 4 MiB
    const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(5);

    // State that persists across batch iterations so we can defer flush.
    let mut pending_signals: Vec<BatchSuccess> = Vec::new();
    let mut dirty_topics: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut dirty_bytes: usize = 0;

    loop {
        // ── Wait for work ─────────────────────────────────────────────
        // If we have pending (un-flushed) data, use a timeout so we
        // flush within FLUSH_TIMEOUT even at low throughput.
        // If nothing is pending, block indefinitely for the next message.
        let recv_result = if pending_signals.is_empty() {
            rx.recv_async().await.ok()
        } else {
            match tokio::time::timeout(FLUSH_TIMEOUT, rx.recv_async()).await {
                Ok(Ok(r)) => Some(r),
                Ok(Err(_)) => None, // channel closed
                Err(_) => {
                    // Timeout — flush everything we have and continue.
                    flush_and_signal(
                        &mut topic_writers,
                        &mut dirty_topics,
                        &mut pending_signals,
                        &replication_tx,
                        &mut wal,
                    );
                    if let Some(ref mut w) = wal {
                        if let Err(e) = w.reset() {
                            tracing::error!(
                                target: "lance::ingestion",
                                error = %e,
                                "WAL reset failed after timeout flush"
                            );
                        }
                    }
                    dirty_bytes = 0;
                    continue;
                },
            }
        };

        let first = match recv_result {
            Some(r) => r,
            None => break, // channel closed — will flush in shutdown below
        };

        // ── Phase 1: Collect a batch ──────────────────────────────────
        let mut batch: Vec<IngestionRequest> = Vec::with_capacity(MAX_BATCH);
        batch.push(first);
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }

        // ── Phase 2: Write all requests (buffered, no sync yet) ───────
        for mut request in batch {
            let topic_id = request.topic_id;
            let payload = std::mem::take(&mut request.payload);
            let payload_len = payload.len();
            let write_id = request.write_id;
            let write_done_tx = request.write_done_tx.take();

            // WAL-first: append the payload to the WAL before segment write.
            // The WAL fsync guarantees durability even if we crash before
            // the segment flush.
            if let Some(ref mut w) = wal {
                if let Err(e) = w.append(&payload) {
                    tracing::error!(
                        target: "lance::ingestion",
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
                    tracing::warn!(
                        target: "lance::ingestion",
                        topic_id,
                        error = %e,
                        "Failed to process ingestion request"
                    );
                    drop(write_done_tx);
                    topic_writers.remove(&topic_id);
                },
            }
        }

        // ── Phase 3: Flush if we crossed the size threshold ───────────
        if dirty_bytes >= FLUSH_THRESHOLD {
            flush_and_signal(
                &mut topic_writers,
                &mut dirty_topics,
                &mut pending_signals,
                &replication_tx,
                &mut wal,
            );
            // Recycle WAL after segments are durable
            if let Some(ref mut w) = wal {
                if let Err(e) = w.reset() {
                    tracing::error!(
                        target: "lance::ingestion",
                        error = %e,
                        "WAL reset failed after flush"
                    );
                }
            }
            dirty_bytes = 0;
        }
    }

    // ── Shutdown: flush any remaining buffered data ────────────────────
    if !pending_signals.is_empty() {
        flush_and_signal(
            &mut topic_writers,
            &mut dirty_topics,
            &mut pending_signals,
            &replication_tx,
            &mut wal,
        );
        if let Some(ref mut w) = wal {
            if let Err(e) = w.reset() {
                tracing::error!(
                    target: "lance::ingestion",
                    error = %e,
                    "WAL reset failed on shutdown"
                );
            }
        }
    }

    // Close all topic writers on shutdown (fsync + rename to closed segment format)
    for (topic_id, mut tw) in topic_writers {
        tw.writer.fsync()?;
        let end_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        match tw.writer.close(end_timestamp) {
            Ok(closed_path) => {
                let _ = tw.index_builder.write_indexes(&closed_path);
                debug!(
                    target: "lance::ingestion",
                    topic_id,
                    segment = %closed_path.display(),
                    "Closed topic writer on shutdown"
                );
            },
            Err(e) => {
                debug!(
                    target: "lance::ingestion",
                    topic_id,
                    error = %e,
                    "Failed to close segment on shutdown (fsynced)"
                );
            },
        }
    }

    info!(target: "lance::ingestion", "Ingestion actor shutdown complete");

    Ok(())
}

/// Write replicated data received from the leader to a local topic segment.
/// Used by followers to apply data replication entries (legacy format).
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

    let offset = topic_writer.writer.save(payload)?;

    topic_writer
        .index_builder
        .add_record(sort_key, timestamp_ns, offset);

    lnc_metrics::increment_bytes_ingested(payload.len() as u64);

    if topic_writer.writer.current_offset() >= config.io.segment_max_size {
        rotate_topic_segment(config, topic_registry, topic_id, topic_writer)?;
    }

    Ok(())
}

/// Write replicated data using the enriched wire format (L3 filesystem-consistent mode).
///
/// Per Architecture.md §4.1.1 and LWP-Specification.md §18:
/// Followers use the exact segment name, write offset, and flags from the leader
/// to maintain byte-identical segment files across all cluster nodes.
///
/// # Segment Lifecycle
/// - `NEW_SEGMENT` flag: create a new segment with the leader-dictated name
/// - Normal write: write payload at the specified offset in the current segment
/// - `ROTATE_AFTER` flag: seal the current segment after writing
///
/// # Invariants
/// - Write offset must match the follower's current segment position
/// - Segment names are dictated by the leader — followers never create segments independently
pub fn write_replicated_data_enriched(
    config: &Config,
    topic_registry: &TopicRegistry,
    topic_writers: &mut HashMap<u32, TopicWriter>,
    entry: &lnc_replication::DataReplicationEntry,
) -> Result<()> {
    let topic_id = entry.topic_id;
    let topic_dir = if topic_id == 0 {
        config.data_dir.join("segments").join("0")
    } else {
        topic_registry.get_topic_dir(topic_id)
    };
    std::fs::create_dir_all(&topic_dir)?;

    // Handle NEW_SEGMENT flag: create the segment with the leader-dictated name
    if entry.flags.new_segment() {
        tracing::debug!(
            target: "lance::ingestion",
            topic_id,
            segment = %entry.segment_name,
            "Creating leader-dictated segment (follower)"
        );

        let writer = lnc_io::SegmentWriter::create_named(&topic_dir, &entry.segment_name)?;
        let index_builder = lnc_index::IndexBuilder::with_defaults();
        topic_writers.insert(
            topic_id,
            TopicWriter {
                writer,
                index_builder,
            },
        );
    }

    // Use Entry API to handle initialization atomically without double-lookup
    let topic_writer = match topic_writers.entry(topic_id) {
        Entry::Occupied(o) => o.into_mut(),
        Entry::Vacant(v) => {
            tracing::warn!(
                target: "lance::ingestion",
                topic_id,
                segment = %entry.segment_name,
                "No active writer for topic, opening or creating segment from leader name"
            );

            let segment_path = topic_dir.join(&entry.segment_name);
            let mut writer = if segment_path.exists() {
                tracing::debug!(
                    target: "lance::ingestion",
                    topic_id,
                    segment = %entry.segment_name,
                    "Opening existing segment file"
                );
                lnc_io::SegmentWriter::open(&segment_path)?
            } else {
                tracing::debug!(
                    target: "lance::ingestion",
                    topic_id,
                    segment = %entry.segment_name,
                    "Creating new segment file"
                );
                lnc_io::SegmentWriter::create_named(&topic_dir, &entry.segment_name)?
            };

            // ── CRITICAL FIX ──────────────────────────────────────────────
            // Truncate BEFORE insertion to prevent the "append-error-retry" loop.
            // This ensures the writer is in a valid state before being cached.
            let current_offset = writer.write_offset();
            if !entry.payload.is_empty() && entry.write_offset < current_offset {
                tracing::warn!(
                    target: "lance::ingestion",
                    topic_id,
                    segment = %entry.segment_name,
                    current_offset,
                    leader_offset = entry.write_offset,
                    "Raft conflict detected on segment open, rewinding to match Raft log"
                );
                writer.truncate_to_offset(entry.write_offset)?;
            }
            // ──────────────────────────────────────────────────────────────

            let index_builder = lnc_index::IndexBuilder::with_defaults();
            v.insert(TopicWriter {
                writer,
                index_builder,
            })
        },
    };

    // Verify the segment name matches what the leader expects
    if let Some(current_name) = topic_writer.writer.filename() {
        if current_name != entry.segment_name && !entry.flags.new_segment() {
            tracing::warn!(
                target: "lance::ingestion",
                topic_id,
                expected = %entry.segment_name,
                actual = %current_name,
                "Segment name mismatch — follower may need resync"
            );
        }
    }

    // Check for Raft conflict even with cached writer (leader may have rolled back)
    if !entry.payload.is_empty() {
        let current_offset = topic_writer.writer.write_offset();
        if entry.write_offset < current_offset {
            tracing::warn!(
                target: "lance::ingestion",
                topic_id,
                segment = %entry.segment_name,
                current_offset,
                leader_offset = entry.write_offset,
                "Raft conflict detected on cached writer, rewinding to match Raft log"
            );
            // Truncate segment file to match leader's offset
            topic_writer.writer.truncate_to_offset(entry.write_offset)?;

            // SAFETY: Clear in-memory index to prevent corruption from ghost entries
            // The index builder may contain entries for offsets we just truncated.
            // Clearing ensures the .idx file won't have invalid pointers on rotation.
            topic_writer.index_builder.clear();
        }
    }

    // Write at the exact offset the leader specifies
    if !entry.payload.is_empty() {
        topic_writer
            .writer
            .save_at_offset(entry.write_offset, &entry.payload)?;

        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let seq = SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let sort_key = SortKey::from_timestamp_ns(timestamp_ns, seq as u32);

        topic_writer
            .index_builder
            .add_record(sort_key, timestamp_ns, entry.write_offset);

        lnc_metrics::increment_bytes_ingested(entry.payload.len() as u64);
    }

    // Handle ROTATE_AFTER flag: seal the current segment
    if entry.flags.rotate_after() {
        tracing::debug!(
            target: "lance::ingestion",
            topic_id,
            segment = %entry.segment_name,
            "Sealing segment after leader-dictated rotation (follower)"
        );

        topic_writer.writer.fsync()?;

        let end_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let closed_path = topic_writer.writer.close(end_timestamp)?;
        topic_writer.index_builder.write_indexes(&closed_path)?;
        topic_writer.index_builder.clear();

        // Remove the writer — next write with NEW_SEGMENT flag will create a new one
        topic_writers.remove(&topic_id);
    }

    Ok(())
}

/// Holds the state for a single successful write within a batch.
/// Used to defer completion signalling and replication queuing
/// until after the batch fsync.
struct BatchSuccess {
    write_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    topic_id: u32,
    payload: Bytes,
    payload_len: usize,
    write_id: Option<u64>,
    meta: WriteMetadata,
}

/// Flush all dirty topic writers to stable storage, signal pending
/// write completions, and queue replication requests.
///
/// This is the single point where data becomes durable and ACKs are
/// released.  Called when the flush threshold is reached, the timeout
/// fires, or the actor is shutting down.
fn flush_and_signal(
    topic_writers: &mut HashMap<u32, TopicWriter>,
    dirty_topics: &mut std::collections::HashSet<u32>,
    pending_signals: &mut Vec<BatchSuccess>,
    replication_tx: &Option<tokio::sync::mpsc::Sender<DataReplicationRequest>>,
    wal: &mut Option<lnc_io::Wal>,
) {
    // Phase A: fsync WAL + every dirty topic writer.
    // WAL sync is done here (not per-append) to batch fsyncs for throughput.
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

    // Phase B: signal completions + queue replication.
    for s in pending_signals.drain(..) {
        if let Some(tx) = s.write_done_tx {
            let _ = tx.send(());
        }

        if let Some(tx) = replication_tx {
            let _ = tx.try_send(DataReplicationRequest {
                topic_id: s.topic_id,
                payload: s.payload,
                segment_name: s.meta.segment_name,
                write_offset: s.meta.write_offset,
                global_offset: s.meta.write_offset + s.payload_len as u64,
                is_new_segment: s.meta.is_new_segment,
                rotated_after: s.meta.rotated_after,
                write_id: s.write_id,
            });
        }
    }
}

/// Process a single ingestion request, returning write metadata for replication.
///
/// `payload` is passed separately because the caller has already taken it from
/// the request via `std::mem::take` for WAL and deferred-signal storage.
fn process_ingestion_request(
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
