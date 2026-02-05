//! Recovery logic - startup recovery and segment cleanup

use crate::config::Config;
use lnc_core::Result;
use lnc_io::close_unclosed_segments;
use lnc_recovery::{find_segments_needing_recovery, IndexRebuilder, SegmentRecovery, WalReplay};
use tracing::{debug, info};

/// Clean up empty segment files from previous aborted sessions
pub fn cleanup_empty_segments(data_dir: &std::path::Path) -> Result<()> {
    let segments_dir = data_dir.join("segments");

    if !segments_dir.exists() {
        return Ok(());
    }

    let entries = match std::fs::read_dir(&segments_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    let mut removed_count = 0;

    for entry in entries.flatten() {
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "lnc") {
            if let Ok(metadata) = std::fs::metadata(&path) {
                if metadata.len() == 0 {
                    if let Err(e) = std::fs::remove_file(&path) {
                        debug!(
                            target: "lance::server",
                            path = %path.display(),
                            error = %e,
                            "Failed to remove empty segment"
                        );
                    } else {
                        removed_count += 1;
                        debug!(
                            target: "lance::server",
                            path = %path.display(),
                            "Removed empty segment file"
                        );
                    }
                }
            }
        }
    }

    if removed_count > 0 {
        info!(
            target: "lance::server",
            count = removed_count,
            "Cleaned up empty segment files"
        );
    }

    Ok(())
}

/// Perform startup recovery - segment validation, index rebuilding, and WAL replay
pub fn perform_startup_recovery(config: &Config) -> Result<()> {
    info!(target: "lance::server", "Performing startup recovery checks");

    // Clean up empty segment files from previous aborted sessions
    cleanup_empty_segments(&config.data_dir)?;

    // Close any unclosed segments from previous crash/shutdown
    let segments_dir = config.data_dir.join("segments");
    if segments_dir.exists() {
        let closed = close_unclosed_segments(&segments_dir)?;
        if !closed.is_empty() {
            info!(
                target: "lance::server",
                count = closed.len(),
                "Closed unclosed segments from previous session"
            );
        }
    }

    let segments = find_segments_needing_recovery(&config.data_dir)?;

    for segment_path in segments {
        info!(
            target: "lance::server",
            segment = %segment_path.display(),
            "Recovering segment"
        );

        let recovery = SegmentRecovery::new(&segment_path);
        let result = recovery.truncate_to_valid()?;

        info!(
            target: "lance::server",
            segment = %segment_path.display(),
            valid_records = result.valid_records,
            truncated_bytes = result.truncated_bytes,
            "Segment recovery complete"
        );

        let rebuilder = IndexRebuilder::with_defaults();
        if rebuilder.needs_rebuild(&segment_path) {
            info!(
                target: "lance::server",
                segment = %segment_path.display(),
                "Rebuilding indexes"
            );
            rebuilder.rebuild(&segment_path)?;
        }

        recovery.clear_dirty_marker()?;
    }

    if config.wal.enabled {
        let wal_replay = WalReplay::new(config.wal_config());
        if wal_replay.should_replay() {
            info!(target: "lance::server", "WAL replay needed");
        }
    }

    info!(target: "lance::server", "Startup recovery complete");

    Ok(())
}
