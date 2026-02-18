//! Ingestion actor - batch processing and record ingestion

use super::writer::{TopicWriter, create_topic_writer, rotate_topic_segment};
use crate::config::Config;
use crate::topic::TopicRegistry;
use bytes::Bytes;
use lnc_core::{LanceError, Result, SortKey};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Request to ingest data into a topic
pub struct IngestionRequest {
    pub topic_id: u32,
    /// Connection-scoped routing key used to shard hot topics across actors
    /// while preserving per-connection ordering.
    pub routing_key: u64,
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
    topic_writers: &mut HashMap<(u32, String), TopicWriter>,
    entry: &lnc_replication::DataReplicationEntry,
) -> Result<()> {
    let topic_id = entry.topic_id;
    let writer_key = (topic_id, entry.segment_name.clone());
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
            writer_key.clone(),
            TopicWriter {
                writer,
                index_builder,
            },
        );
    }

    // Use Entry API to handle initialization atomically without double-lookup
    let topic_writer = match topic_writers.entry(writer_key.clone()) {
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
        topic_writers.remove(&writer_key);
    }

    Ok(())
}
