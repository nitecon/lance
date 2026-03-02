//! Topic writer - segment writing and rotation

use crate::config::Config;
use crate::topic::TopicRegistry;
use lnc_core::Result;
use lnc_io::SegmentWriter;
use tracing::info;

/// Writer for a single topic's segments
pub struct TopicWriter {
    pub writer: SegmentWriter,
    pub index_builder: lnc_index::IndexBuilder,
}

/// Create a new topic writer for the given topic
pub fn create_topic_writer(
    _config: &Config,
    topic_registry: &TopicRegistry,
    topic_id: u32,
) -> Result<TopicWriter> {
    // Name-based storage: all topics resolve through the registry.
    // topic_id==0 is the legacy/default topic and also goes through
    // the registry lookup (which falls back to "0" if not registered).
    let topic_dir = topic_registry.get_topic_dir(topic_id);

    std::fs::create_dir_all(&topic_dir)?;

    let start_index = find_next_segment_index(&topic_dir)?;
    let start_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let segment_path = topic_dir.join(format!("{}_{}.lnc", start_index, start_timestamp));
    let writer = SegmentWriter::create(&segment_path)?;
    let index_builder = lnc_index::IndexBuilder::with_defaults();

    info!(
        target: "lance::ingestion",
        topic_id,
        segment = %writer.path().display(),
        "Created topic writer"
    );

    Ok(TopicWriter {
        writer,
        index_builder,
    })
}

/// Rotate to a new segment when the current one reaches max size
pub fn rotate_topic_segment(
    _config: &Config,
    topic_registry: &TopicRegistry,
    topic_id: u32,
    topic_writer: &mut TopicWriter,
) -> Result<()> {
    topic_writer.writer.fsync()?;

    let end_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let closed_path = topic_writer.writer.close(end_timestamp)?;
    topic_writer.index_builder.write_indexes(&closed_path)?;

    let topic_dir = topic_registry.get_topic_dir(topic_id);

    let new_start_index = topic_writer.writer.start_index() + topic_writer.writer.record_count();
    let new_segment_path = topic_dir.join(format!("{}_{}.lnc", new_start_index, end_timestamp));
    topic_writer.writer = SegmentWriter::create(&new_segment_path)?;
    topic_writer.index_builder.clear();

    info!(
        target: "lance::ingestion",
        topic_id,
        old_segment = %closed_path.display(),
        new_segment = %topic_writer.writer.path().display(),
        "Segment rotated"
    );

    Ok(())
}

/// Find the next available segment index for a topic directory.
///
/// Returns 0 for an empty directory, or `max_existing_index + 1` when
/// segments already exist.  This prevents multiple writers (e.g. with
/// `actor_count > 1`) from creating segments with overlapping start
/// indices, which would corrupt the cumulative byte-offset addressing
/// used by `read_from_offset`.
pub fn find_next_segment_index(segments_dir: &std::path::Path) -> Result<u64> {
    let mut max_index: Option<u64> = None;

    if segments_dir.exists() {
        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "lnc") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(index_str) = filename.split('_').next() {
                        if let Ok(index) = index_str.parse::<u64>() {
                            max_index = Some(max_index.map_or(index, |cur: u64| cur.max(index)));
                        }
                    }
                }
            }
        }
    }

    Ok(max_index.map_or(0, |idx| idx + 1))
}
