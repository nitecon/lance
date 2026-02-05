//! Segment repair and index rebuild commands.
//!
//! This module provides CLI commands for repairing corrupted segments
//! and rebuilding indices.

use lnc_core::Result;
use lnc_recovery::{IndexRebuilder, SegmentRecovery};
use std::path::Path;

/// Repair a corrupted segment by truncating to the last valid record.
pub fn repair_segment(path: &Path) -> Result<()> {
    let recovery = SegmentRecovery::new(path);
    let result = recovery.truncate_to_valid()?;

    println!("Segment repaired: {}", path.display());
    println!("  Final size: {} bytes", result.valid_bytes);
    println!("  Records preserved: {}", result.valid_records);

    if result.truncated_bytes > 0 {
        println!("  Truncated: {} bytes", result.truncated_bytes);
    }

    Ok(())
}

/// Rebuild index files for a segment.
pub fn rebuild_index(segment_path: &Path) -> Result<()> {
    let rebuilder = IndexRebuilder::with_defaults();
    let result = rebuilder.rebuild(segment_path)?;

    println!("Index rebuilt for: {}", segment_path.display());
    println!("  Records indexed: {}", result.records_indexed);
    println!("  Sparse entries: {}", result.sparse_entries);
    println!("  Secondary entries: {}", result.secondary_entries);
    println!("  Sparse index: {}", result.sparse_path.display());
    println!("  Secondary index: {}", result.secondary_path.display());

    Ok(())
}
