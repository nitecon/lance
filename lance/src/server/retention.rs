//! Retention policy enforcement - background segment cleanup
//!
//! Enforces TTL-based and size-based retention policies per topic by
//! periodically scanning and deleting expired segments.

use crate::topic::{RetentionConfig, TopicRegistry};
use lnc_core::Result;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tracing::{info, trace, warn};

/// Segment file metadata for retention decisions
#[derive(Debug, Clone)]
struct SegmentInfo {
    path: std::path::PathBuf,
    start_index: u64,
    start_timestamp_ns: u64,
    end_timestamp_ns: Option<u64>,
    size_bytes: u64,
    is_active: bool,
}

impl SegmentInfo {
    /// Parse segment info from filename and file metadata
    ///
    /// Filename formats:
    /// - Active: `{start_index}_{start_timestamp_ns}.lnc`
    /// - Closed: `{start_index}_{start_timestamp_ns}-{end_timestamp_ns}.lnc`
    fn from_path(path: &Path) -> Option<Self> {
        let metadata = std::fs::metadata(path).ok()?;
        let size_bytes = metadata.len();

        let filename = path.file_stem()?.to_str()?;

        // Check if it's a closed segment (has end timestamp)
        let (start_part, end_timestamp_ns) = if filename.contains('-') {
            let parts: Vec<&str> = filename.split('-').collect();
            if parts.len() != 2 {
                return None;
            }
            (parts[0], parts[1].parse::<u64>().ok())
        } else {
            (filename, None)
        };

        // Parse start_index_timestamp
        let start_parts: Vec<&str> = start_part.split('_').collect();
        if start_parts.len() != 2 {
            return None;
        }

        let start_index = start_parts[0].parse::<u64>().ok()?;
        let start_timestamp_ns = start_parts[1].parse::<u64>().ok()?;

        Some(Self {
            path: path.to_path_buf(),
            start_index,
            start_timestamp_ns,
            end_timestamp_ns,
            size_bytes,
            is_active: end_timestamp_ns.is_none(),
        })
    }

    /// Check if segment has expired based on TTL
    fn is_expired(&self, max_age_secs: u64, now_ns: u64) -> bool {
        // Never delete active segments
        if self.is_active {
            return false;
        }

        // Use end timestamp if available, otherwise start timestamp
        let segment_age_ns = if let Some(end_ts) = self.end_timestamp_ns {
            now_ns.saturating_sub(end_ts)
        } else {
            now_ns.saturating_sub(self.start_timestamp_ns)
        };

        let max_age_ns = max_age_secs * 1_000_000_000;
        segment_age_ns > max_age_ns
    }
}

/// Configuration for the retention service
#[derive(Debug, Clone)]
pub struct RetentionServiceConfig {
    /// How often to run retention cleanup (default: 60 seconds)
    pub cleanup_interval: Duration,
    /// Whether to actually delete files (false = dry run for testing)
    pub dry_run: bool,
}

impl Default for RetentionServiceConfig {
    fn default() -> Self {
        Self {
            cleanup_interval: Duration::from_secs(60),
            dry_run: false,
        }
    }
}

/// Run the retention service as a background task
pub async fn run_retention_service(
    config: RetentionServiceConfig,
    topic_registry: Arc<TopicRegistry>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    info!(
        target: "lance::retention",
        interval_secs = config.cleanup_interval.as_secs(),
        dry_run = config.dry_run,
        "Retention service started"
    );

    let mut interval = tokio::time::interval(config.cleanup_interval);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = run_retention_cycle(&config, &topic_registry).await {
                    warn!(
                        target: "lance::retention",
                        error = %e,
                        "Retention cycle failed"
                    );
                }
            }
            _ = shutdown_rx.recv() => {
                info!(target: "lance::retention", "Retention service shutting down");
                break;
            }
        }
    }

    Ok(())
}

/// Run a single retention cleanup cycle for all topics
async fn run_retention_cycle(
    config: &RetentionServiceConfig,
    topic_registry: &TopicRegistry,
) -> Result<()> {
    let topics = topic_registry.list_topics();
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let mut total_deleted = 0u64;
    let mut total_bytes_freed = 0u64;

    for topic in topics {
        if let Some(ref retention) = topic.retention {
            let (deleted, bytes) =
                enforce_topic_retention(config, topic_registry, topic.id, retention, now_ns)?;
            total_deleted += deleted;
            total_bytes_freed += bytes;
        }
    }

    if total_deleted > 0 {
        info!(
            target: "lance::retention",
            segments_deleted = total_deleted,
            bytes_freed = total_bytes_freed,
            dry_run = config.dry_run,
            "Retention cycle complete"
        );
    } else {
        trace!(
            target: "lance::retention",
            "Retention cycle complete, no segments expired"
        );
    }

    Ok(())
}

/// Enforce retention policy for a single topic
fn enforce_topic_retention(
    config: &RetentionServiceConfig,
    topic_registry: &TopicRegistry,
    topic_id: u32,
    retention: &RetentionConfig,
    now_ns: u64,
) -> Result<(u64, u64)> {
    let topic_dir = topic_registry.get_topic_dir(topic_id);

    if !topic_dir.exists() {
        return Ok((0, 0));
    }

    // Collect all segments
    let mut segments = collect_segments(&topic_dir)?;

    // Sort by start_index (oldest first)
    segments.sort_by_key(|s| s.start_index);

    let mut deleted_count = 0u64;
    let mut bytes_freed = 0u64;
    let mut segments_to_delete: Vec<SegmentInfo> = Vec::new();

    // Phase 1: Mark TTL-expired segments for deletion
    if let Some(max_age_secs) = retention.max_age_secs {
        for segment in &segments {
            if segment.is_expired(max_age_secs, now_ns) {
                segments_to_delete.push(segment.clone());
            }
        }
    }

    // Phase 2: Mark size-exceeded segments for deletion (oldest first)
    if let Some(max_bytes) = retention.max_bytes {
        // Calculate current total size (excluding already-marked segments)
        let marked_paths: std::collections::HashSet<_> =
            segments_to_delete.iter().map(|s| &s.path).collect();

        let remaining_segments: Vec<_> = segments
            .iter()
            .filter(|s| !marked_paths.contains(&s.path))
            .collect();

        let total_size: u64 = remaining_segments.iter().map(|s| s.size_bytes).sum();

        if total_size > max_bytes {
            let mut excess = total_size - max_bytes;

            // Delete oldest closed segments first until under limit
            for segment in remaining_segments {
                if excess == 0 {
                    break;
                }
                if segment.is_active {
                    continue; // Never delete active segment
                }

                segments_to_delete.push(segment.clone());
                excess = excess.saturating_sub(segment.size_bytes);
            }
        }
    }

    // Phase 3: Delete marked segments
    for segment in segments_to_delete {
        if config.dry_run {
            info!(
                target: "lance::retention",
                topic_id,
                segment = %segment.path.display(),
                size = segment.size_bytes,
                "[DRY RUN] Would delete segment"
            );
        } else {
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {
                    info!(
                        target: "lance::retention",
                        topic_id,
                        segment = %segment.path.display(),
                        size = segment.size_bytes,
                        "Deleted expired segment"
                    );

                    // Also delete associated index files if they exist
                    let sparse_idx = segment.path.with_extension("sparse.idx");
                    let secondary_idx = segment.path.with_extension("secondary.idx");
                    let _ = std::fs::remove_file(sparse_idx);
                    let _ = std::fs::remove_file(secondary_idx);
                },
                Err(e) => {
                    warn!(
                        target: "lance::retention",
                        topic_id,
                        segment = %segment.path.display(),
                        error = %e,
                        "Failed to delete segment"
                    );
                    continue;
                },
            }
        }

        deleted_count += 1;
        bytes_freed += segment.size_bytes;
    }

    Ok((deleted_count, bytes_freed))
}

/// Collect all segment files in a topic directory
fn collect_segments(topic_dir: &Path) -> Result<Vec<SegmentInfo>> {
    let mut segments = Vec::new();

    for entry in std::fs::read_dir(topic_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "lnc") {
            if let Some(info) = SegmentInfo::from_path(&path) {
                segments.push(info);
            }
        }
    }

    Ok(segments)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_segment_info_from_active_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("0_1234567890.lnc");
        File::create(&path).unwrap().write_all(b"test").unwrap();

        let info = SegmentInfo::from_path(&path).unwrap();
        assert_eq!(info.start_index, 0);
        assert_eq!(info.start_timestamp_ns, 1234567890);
        assert!(info.end_timestamp_ns.is_none());
        assert!(info.is_active);
    }

    #[test]
    fn test_segment_info_from_closed_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("100_1234567890-9876543210.lnc");
        File::create(&path).unwrap().write_all(b"test").unwrap();

        let info = SegmentInfo::from_path(&path).unwrap();
        assert_eq!(info.start_index, 100);
        assert_eq!(info.start_timestamp_ns, 1234567890);
        assert_eq!(info.end_timestamp_ns, Some(9876543210));
        assert!(!info.is_active);
    }

    #[test]
    fn test_segment_expiry_ttl() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("0_1000000000000000000-1000000001000000000.lnc");
        File::create(&path).unwrap().write_all(b"test").unwrap();

        let info = SegmentInfo::from_path(&path).unwrap();

        // Segment ended at 1000000001000000000 ns
        // With 60 second TTL, it should be expired after 1000000061000000000 ns
        let now_before_expiry = 1000000060000000000u64; // 59 seconds after
        let now_after_expiry = 1000000062000000000u64; // 61 seconds after

        assert!(!info.is_expired(60, now_before_expiry));
        assert!(info.is_expired(60, now_after_expiry));
    }

    #[test]
    fn test_active_segment_never_expires() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("0_1000000000000000000.lnc");
        File::create(&path).unwrap().write_all(b"test").unwrap();

        let info = SegmentInfo::from_path(&path).unwrap();
        assert!(info.is_active);

        // Even with very old timestamp, active segment should not expire
        let far_future = 9999999999999999999u64;
        assert!(!info.is_expired(1, far_future));
    }
}
