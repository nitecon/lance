//! Segment and index inspection commands.
//!
//! This module provides CLI commands for inspecting segments and indices
//! without modifying them.

use lnc_core::{parse_header, Result};
use lnc_index::{SecondaryIndex, SparseIndex};
use lnc_recovery::{find_segments_needing_recovery, SegmentRecovery};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Scan a segment file and report its status.
pub fn scan_segment(path: &Path) -> Result<()> {
    let recovery = SegmentRecovery::new(path);
    let result = recovery.scan()?;

    println!("Segment: {}", path.display());
    println!("  Valid bytes: {}", result.valid_bytes);
    println!("  Valid records: {}", result.valid_records);
    println!("  Truncated bytes: {}", result.truncated_bytes);

    if let Some(offset) = result.corrupted_at {
        println!("  Corruption detected at offset: {}", offset);
    }

    Ok(())
}

/// Dump records from a segment file up to the specified limit.
pub fn dump_segment_records(segment_path: &Path, limit: usize) -> Result<()> {
    let mut file = File::open(segment_path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let mut offset = 0usize;
    let mut count = 0usize;

    println!("Records in: {}", segment_path.display());

    while offset < data.len() && count < limit {
        let remaining = &data[offset..];

        if remaining.len() < 5 {
            break;
        }

        let header = match parse_header(remaining) {
            Ok(h) => h,
            Err(_) => break,
        };

        let record_size = header.total_size();

        println!(
            "  [{}] offset={}, size={}, payload={}",
            count, offset, record_size, header.length
        );

        offset += record_size;
        count += 1;
    }

    println!("Displayed {} records", count);

    Ok(())
}

/// Inspect an index file (sparse or secondary).
pub fn inspect_index(index_path: &Path) -> Result<()> {
    let ext = index_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match ext {
        "idx" => {
            let index = SparseIndex::open(index_path)?;
            println!("Sparse Index: {}", index_path.display());
            println!("  Entries: {}", index.entry_count());

            if let Some(first) = index.first() {
                println!("  First entry:");
                println!("    Timestamp: {} ns", first.sort_key.timestamp_ns());
                println!("    Sequence: {}", first.sort_key.sequence());
                println!("    Offset: {}", first.byte_offset);
            }

            if let Some(last) = index.last() {
                println!("  Last entry:");
                println!("    Timestamp: {} ns", last.sort_key.timestamp_ns());
                println!("    Sequence: {}", last.sort_key.sequence());
                println!("    Offset: {}", last.byte_offset);
            }
        }
        "tidx" => {
            let index = SecondaryIndex::open(index_path)?;
            println!("Secondary Index: {}", index_path.display());
            println!("  Entries: {}", index.entry_count());

            if let Some(first) = index.first() {
                println!("  First timestamp: {} ns", first.timestamp_ns);
                println!("  First offset: {}", first.byte_offset);
            }

            if let Some(last) = index.last() {
                println!("  Last timestamp: {} ns", last.timestamp_ns);
                println!("  Last offset: {}", last.byte_offset);
            }
        }
        _ => {
            println!("Unknown index type: {}", ext);
        }
    }

    Ok(())
}

/// Find segments that need recovery in the data directory.
pub fn find_recovery_candidates(data_dir: &Path) -> Result<()> {
    let segments = find_segments_needing_recovery(data_dir)?;

    if segments.is_empty() {
        println!("No segments need recovery in: {}", data_dir.display());
    } else {
        println!("Segments needing recovery in: {}", data_dir.display());
        for segment in &segments {
            println!("  {}", segment.display());
        }
    }

    Ok(())
}
