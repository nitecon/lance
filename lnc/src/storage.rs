//! Storage management commands.
//!
//! This module provides CLI commands for managing storage, including
//! cleaning orphaned directories and listing topics.

use lnc_core::Result;
use std::path::Path;

/// Clean storage by removing orphaned topic directories.
///
/// Orphaned directories are topic folders that either:
/// - Have no metadata.json (incomplete creation)
/// - Have a deleted flag in metadata
/// - Are empty directories
pub fn clean_storage(data_dir: &Path, dry_run: bool) -> Result<()> {
    let segments_dir = data_dir.join("segments");

    if !segments_dir.exists() {
        println!(
            "Segments directory does not exist: {}",
            segments_dir.display()
        );
        return Ok(());
    }

    let mut orphaned_dirs = Vec::new();
    let mut bytes_to_free = 0u64;

    for entry in std::fs::read_dir(&segments_dir)? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        let metadata_path = path.join("metadata.json");
        let is_orphaned = if metadata_path.exists() {
            // Check if metadata marks topic as deleted
            match std::fs::read_to_string(&metadata_path) {
                Ok(content) => content.contains("\"deleted\":true"),
                Err(_) => true, // Can't read metadata, consider orphaned
            }
        } else {
            // No metadata.json = orphaned directory
            true
        };

        if is_orphaned {
            // Calculate directory size
            let dir_size = calculate_dir_size(&path);
            bytes_to_free += dir_size;
            orphaned_dirs.push((path, dir_size));
        }
    }

    if orphaned_dirs.is_empty() {
        println!(
            "No orphaned topic directories found in: {}",
            segments_dir.display()
        );
        return Ok(());
    }

    println!("Found {} orphaned topic directories:", orphaned_dirs.len());
    for (path, size) in &orphaned_dirs {
        let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
        println!("  {} ({} bytes)", dir_name, size);
    }
    println!(
        "Total space to free: {} bytes ({:.2} MB)",
        bytes_to_free,
        bytes_to_free as f64 / (1024.0 * 1024.0)
    );

    if dry_run {
        println!("\nDry run - no files were deleted.");
        println!("Run without --dry-run to actually delete.");
    } else {
        for (path, _) in &orphaned_dirs {
            match std::fs::remove_dir_all(path) {
                Ok(()) => {
                    let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
                    println!("Deleted: {}", dir_name);
                }
                Err(e) => {
                    eprintln!("Failed to delete {}: {}", path.display(), e);
                }
            }
        }
        println!(
            "\nCleaned {} directories, freed {} bytes.",
            orphaned_dirs.len(),
            bytes_to_free
        );
    }

    Ok(())
}

/// Calculate the total size of a directory recursively.
fn calculate_dir_size(path: &Path) -> u64 {
    let mut total = 0u64;

    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_file() {
                if let Ok(metadata) = entry_path.metadata() {
                    total += metadata.len();
                }
            } else if entry_path.is_dir() {
                total += calculate_dir_size(&entry_path);
            }
        }
    }

    total
}

// =============================================================================
// Topic Listing
// =============================================================================

/// Information about a topic directory
#[derive(Debug)]
struct TopicDirInfo {
    /// Directory ID (from folder name)
    dir_id: String,
    /// Topic name from metadata
    name: String,
    /// Status (active, deleted, orphaned, error)
    status: &'static str,
    /// Size in bytes
    size: u64,
}

/// Scan a topic directory and extract its info
fn scan_topic_dir(path: &std::path::Path) -> TopicDirInfo {
    let dir_id = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    let metadata_path = path.join("metadata.json");
    let (name, status) = parse_topic_metadata(&metadata_path, &dir_id);
    let size = calculate_dir_size(path);

    TopicDirInfo {
        dir_id,
        name,
        status,
        size,
    }
}

/// Parse topic metadata and determine status
fn parse_topic_metadata(metadata_path: &std::path::Path, fallback_name: &str) -> (String, &'static str) {
    if !metadata_path.exists() {
        return (fallback_name.to_string(), "orphaned");
    }

    match std::fs::read_to_string(metadata_path) {
        Ok(content) => {
            let name = extract_json_field(&content, "name")
                .unwrap_or_else(|| fallback_name.to_string());
            let status = if content.contains("\"deleted\":true") {
                "deleted"
            } else {
                "active"
            };
            (name, status)
        }
        Err(_) => (fallback_name.to_string(), "error"),
    }
}

/// Format a size in bytes as a human-readable string
fn format_size(bytes: u64) -> String {
    if bytes > 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes > 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Print the topic list header
fn print_topic_list_header(segments_dir: &Path) {
    println!("Topics in: {}", segments_dir.display());
    println!("{:<6} {:<30} {:<12} {:<15}", "ID", "Name", "Status", "Size");
    println!("{}", "-".repeat(65));
}

/// Print a single topic info row
fn print_topic_row(info: &TopicDirInfo) {
    let size_str = format_size(info.size);
    println!(
        "{:<6} {:<30} {:<12} {:<15}",
        info.dir_id, info.name, info.status, size_str
    );
}

/// List all topics in the data directory with their status.
pub fn list_topics(data_dir: &Path) -> Result<()> {
    let segments_dir = data_dir.join("segments");

    if !segments_dir.exists() {
        println!(
            "Segments directory does not exist: {}",
            segments_dir.display()
        );
        return Ok(());
    }

    // Collect and sort topic directories
    let mut entries: Vec<_> = std::fs::read_dir(&segments_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    entries.sort_by_key(|e| e.file_name());

    // Print header
    print_topic_list_header(&segments_dir);

    // Scan and print each topic
    for entry in entries {
        let info = scan_topic_dir(&entry.path());
        print_topic_row(&info);
    }

    Ok(())
}

/// Extract a string field from JSON content (simple parser).
fn extract_json_field(json: &str, field: &str) -> Option<String> {
    let pattern = format!("\"{}\":\"", field);
    let start = json.find(&pattern)? + pattern.len();
    let rest = &json[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}
