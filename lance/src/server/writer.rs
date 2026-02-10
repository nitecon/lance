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

/// Resolve the directory for a given topic.
///
/// Topic 0 is the legacy/default topic stored under `segments/0/`.
/// All other topics use the registry's configured directory.
pub fn resolve_topic_dir(
    config: &Config,
    topic_registry: &TopicRegistry,
    topic_id: u32,
) -> std::path::PathBuf {
    if topic_id == 0 {
        config.data_dir.join("segments").join("0")
    } else {
        topic_registry.get_topic_dir(topic_id)
    }
}

/// Create a new topic writer for the given topic
pub fn create_topic_writer(
    config: &Config,
    topic_registry: &TopicRegistry,
    topic_id: u32,
) -> Result<TopicWriter> {
    let topic_dir = resolve_topic_dir(config, topic_registry, topic_id);

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
    config: &Config,
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

    let topic_dir = resolve_topic_dir(config, topic_registry, topic_id);

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

/// Find the next segment index for a topic directory
pub fn find_next_segment_index(segments_dir: &std::path::Path) -> Result<u64> {
    let mut max_index = 0u64;

    if segments_dir.exists() {
        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "lnc") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(index_str) = filename.split('_').next() {
                        if let Ok(index) = index_str.parse::<u64>() {
                            max_index = max_index.max(index);
                        }
                    }
                }
            }
        }
    }

    Ok(max_index)
}
