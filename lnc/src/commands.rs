use lnc_client::ClientError;
use lnc_core::{LanceError, Result};
use lnc_index::{SecondaryIndex, SparseIndex};
use lnc_recovery::{IndexRebuilder, SegmentRecovery, find_segments_needing_recovery};
use std::path::Path;

fn client_err(e: ClientError) -> LanceError {
    LanceError::Client(e.to_string())
}

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
        },
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
        },
        _ => {
            println!("Unknown index type: {}", ext);
        },
    }

    Ok(())
}

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

pub fn dump_segment_records(segment_path: &Path, limit: usize) -> Result<()> {
    use lnc_core::parse_header;
    use std::fs::File;
    use std::io::Read;

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

/// Clean storage by removing orphaned topic directories.
///
/// Orphaned directories are topic folders that either:
/// - Have no metadata.json (incomplete creation)
/// - Have a deleted flag in metadata
/// - Are empty directories
#[allow(dead_code)]
fn _clean_storage_placeholder() {}

/// Orphaned directory info for cleanup
struct OrphanedDir {
    path: std::path::PathBuf,
    size: u64,
}

/// Scan segments directory for orphaned topic directories
fn scan_orphaned_dirs(segments_dir: &Path) -> Result<Vec<OrphanedDir>> {
    let mut orphaned = Vec::new();

    for entry in std::fs::read_dir(segments_dir)? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        if is_orphaned_directory(&path) {
            let size = calculate_dir_size(&path);
            orphaned.push(OrphanedDir { path, size });
        }
    }

    Ok(orphaned)
}

/// Check if a directory is orphaned (no valid metadata or marked deleted)
fn is_orphaned_directory(path: &Path) -> bool {
    let metadata_path = path.join("metadata.json");

    if metadata_path.exists() {
        // Check if metadata marks topic as deleted
        match std::fs::read_to_string(&metadata_path) {
            Ok(content) => content.contains("\"deleted\":true"),
            Err(_) => true, // Can't read metadata, consider orphaned
        }
    } else {
        // No metadata.json = orphaned directory
        true
    }
}

/// Display orphaned directories and compute total size
fn display_orphaned_summary(orphaned: &[OrphanedDir]) -> u64 {
    let total_bytes: u64 = orphaned.iter().map(|d| d.size).sum();

    println!("Found {} orphaned topic directories:", orphaned.len());
    for dir in orphaned {
        let dir_name = dir
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        println!("  {} ({} bytes)", dir_name, dir.size);
    }
    println!(
        "Total space to free: {} bytes ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / (1024.0 * 1024.0)
    );

    total_bytes
}

/// Execute cleanup by removing orphaned directories
fn execute_cleanup(orphaned: &[OrphanedDir], total_bytes: u64) {
    for dir in orphaned {
        match std::fs::remove_dir_all(&dir.path) {
            Ok(()) => {
                let dir_name = dir
                    .path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                println!("Deleted: {}", dir_name);
            },
            Err(e) => {
                eprintln!("Failed to delete {}: {}", dir.path.display(), e);
            },
        }
    }
    println!(
        "\nCleaned {} directories, freed {} bytes.",
        orphaned.len(),
        total_bytes
    );
}

/// Clean storage by removing orphaned topic directories.
///
/// Refactored into phases: scan → display → cleanup
pub fn clean_storage(data_dir: &Path, dry_run: bool) -> Result<()> {
    let segments_dir = data_dir.join("segments");

    // Phase 1: Validate directory exists
    if !segments_dir.exists() {
        println!(
            "Segments directory does not exist: {}",
            segments_dir.display()
        );
        return Ok(());
    }

    // Phase 2: Scan for orphaned directories
    let orphaned = scan_orphaned_dirs(&segments_dir)?;

    if orphaned.is_empty() {
        println!(
            "No orphaned topic directories found in: {}",
            segments_dir.display()
        );
        return Ok(());
    }

    // Phase 3: Display summary
    let total_bytes = display_orphaned_summary(&orphaned);

    // Phase 4: Execute cleanup or show dry-run message
    if dry_run {
        println!("\nDry run - no files were deleted.");
        println!("Run without --dry-run to actually delete.");
    } else {
        execute_cleanup(&orphaned, total_bytes);
    }

    Ok(())
}

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

    println!("Topics in: {}", segments_dir.display());
    println!("{:<6} {:<30} {:<12} {:<15}", "ID", "Name", "Status", "Size");
    println!("{}", "-".repeat(65));

    let mut entries: Vec<_> = std::fs::read_dir(&segments_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let dir_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let metadata_path = path.join("metadata.json");
        let (name, status) = if metadata_path.exists() {
            match std::fs::read_to_string(&metadata_path) {
                Ok(content) => {
                    let name = extract_json_field(&content, "name")
                        .unwrap_or_else(|| dir_name.to_string());
                    let status = if content.contains("\"deleted\":true") {
                        "deleted"
                    } else {
                        "active"
                    };
                    (name, status)
                },
                Err(_) => (dir_name.to_string(), "error"),
            }
        } else {
            (dir_name.to_string(), "orphaned")
        };

        let size = calculate_dir_size(&path);
        let size_str = if size > 1024 * 1024 {
            format!("{:.2} MB", size as f64 / (1024.0 * 1024.0))
        } else if size > 1024 {
            format!("{:.2} KB", size as f64 / 1024.0)
        } else {
            format!("{} B", size)
        };

        println!(
            "{:<6} {:<30} {:<12} {:<15}",
            dir_name, name, status, size_str
        );
    }

    Ok(())
}

fn extract_json_field(json: &str, field: &str) -> Option<String> {
    let pattern = format!("\"{}\":\"", field);
    let start = json.find(&pattern)? + pattern.len();
    let rest = &json[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Fetch data from a topic at a specific offset.
///
/// This is the CLI interface to the Consumer API for fetching/replaying data.
pub async fn fetch_topic(
    server_addr: &str,
    topic_id: u32,
    offset: u64,
    max_bytes: u32,
    output_hex: bool,
) -> Result<()> {
    use lnc_client::{Consumer, ConsumerConfig, LanceClient, SeekPosition};

    let client = LanceClient::connect_to(server_addr).await.map_err(|e| {
        lnc_core::LanceError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;

    let config = ConsumerConfig::new(topic_id)
        .with_max_fetch_bytes(max_bytes)
        .with_start_position(SeekPosition::Offset(offset));

    let mut consumer = Consumer::new(client, config);

    println!("Fetching from topic {} at offset {}", topic_id, offset);
    println!("Max bytes: {}", max_bytes);
    println!();

    match consumer.poll().await {
        Ok(Some(result)) => {
            println!("Bytes returned: {}", result.data.len());
            println!("Next offset: {}", result.current_offset);
            println!("Record count: {}", result.record_count);
            println!("End of stream: {}", result.end_of_stream);
            println!();

            if !result.data.is_empty() {
                if output_hex {
                    println!("Data (hex):");
                    print_hex(&result.data);
                } else {
                    println!("Data (UTF-8 lossy):");
                    println!("{}", String::from_utf8_lossy(&result.data));
                }
            }
        },
        Ok(None) => {
            println!("No data available at offset {}", offset);
        },
        Err(e) => {
            return Err(lnc_core::LanceError::Io(std::io::Error::other(
                e.to_string(),
            )));
        },
    }

    Ok(())
}

/// Statistics tracked during consumption
struct ConsumeStats {
    total_bytes: u64,
    total_records: u64,
    fetch_count: usize,
    final_offset: u64,
}

impl ConsumeStats {
    fn new() -> Self {
        Self {
            total_bytes: 0,
            total_records: 0,
            fetch_count: 0,
            final_offset: 0,
        }
    }
}

/// Determine the starting position for consumption
fn resolve_start_position(
    start_offset: Option<u64>,
    from_beginning: bool,
    from_end: bool,
) -> lnc_client::SeekPosition {
    use lnc_client::SeekPosition;

    if from_beginning {
        SeekPosition::Beginning
    } else if from_end {
        SeekPosition::End
    } else if let Some(offset) = start_offset {
        SeekPosition::Offset(offset)
    } else {
        SeekPosition::Beginning
    }
}

/// Format the start position for display
fn format_position(position: &lnc_client::SeekPosition) -> String {
    use lnc_client::SeekPosition;

    match position {
        SeekPosition::Beginning => "beginning".to_string(),
        SeekPosition::End => "end (tail)".to_string(),
        SeekPosition::Offset(o) => format!("offset {}", *o),
    }
}

/// Print consumption summary
fn print_consume_summary(stats: &ConsumeStats) {
    println!();
    println!("Summary:");
    println!("  Total fetches: {}", stats.fetch_count);
    println!("  Total bytes: {}", stats.total_bytes);
    println!("  Total records: {}", stats.total_records);
    println!("  Final offset: {}", stats.final_offset);
}

/// Process a single fetch result
fn process_fetch_result(result: &lnc_client::PollResult, fetch_num: usize, output_hex: bool) {
    println!(
        "--- Fetch {} (offset: {}, bytes: {}, records: {}) ---",
        fetch_num,
        result.current_offset,
        result.data.len(),
        result.record_count
    );

    if !result.data.is_empty() {
        if output_hex {
            print_hex(&result.data);
        } else {
            println!("{}", String::from_utf8_lossy(&result.data));
        }
    }
    println!();
}

/// Consume data continuously from a topic, starting at a specific offset.
///
/// Supports rewinding to replay historical data.
/// Refactored into components: setup → poll_loop → summary
pub async fn consume_topic(
    server_addr: &str,
    topic_id: u32,
    start_offset: Option<u64>,
    from_beginning: bool,
    from_end: bool,
    max_records: Option<usize>,
    output_hex: bool,
) -> Result<()> {
    use lnc_client::{Consumer, ConsumerConfig, LanceClient};

    // Phase 1: Setup consumer
    let client = LanceClient::connect_to(server_addr).await.map_err(|e| {
        lnc_core::LanceError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;

    let start_position = resolve_start_position(start_offset, from_beginning, from_end);
    let config = ConsumerConfig::new(topic_id)
        .with_max_fetch_bytes(64 * 1024)
        .with_start_position(start_position);

    let mut consumer = Consumer::new(client, config);

    // Phase 2: Display startup info
    println!(
        "Consuming from topic {} starting at {}",
        topic_id,
        format_position(&start_position)
    );
    if let Some(max) = max_records {
        println!("Max records: {}", max);
    }
    println!();

    // Phase 3: Poll loop
    let mut stats = ConsumeStats::new();
    let max_fetches = max_records.unwrap_or(usize::MAX);

    loop {
        if stats.fetch_count >= max_fetches {
            break;
        }

        match consumer.poll().await {
            Ok(Some(result)) => {
                stats.fetch_count += 1;
                stats.total_bytes += result.data.len() as u64;
                stats.total_records += result.record_count as u64;

                process_fetch_result(&result, stats.fetch_count, output_hex);

                if result.end_of_stream {
                    println!("End of stream reached.");
                    break;
                }
            },
            Ok(None) => {
                println!("No more data available.");
                break;
            },
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            },
        }
    }

    // Phase 4: Print summary
    stats.final_offset = consumer.current_offset();
    print_consume_summary(&stats);

    Ok(())
}

fn print_hex(data: &[u8]) {
    for (i, chunk) in data.chunks(16).enumerate() {
        print!("{:08x}  ", i * 16);

        for (j, byte) in chunk.iter().enumerate() {
            print!("{:02x} ", byte);
            if j == 7 {
                print!(" ");
            }
        }

        // Padding for incomplete rows
        if chunk.len() < 16 {
            for j in chunk.len()..16 {
                print!("   ");
                if j == 7 {
                    print!(" ");
                }
            }
        }

        print!(" |");
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }
}

/// Set retention policy for a topic
pub async fn set_retention(
    server_addr: &str,
    topic_id: u32,
    max_age_secs: u64,
    max_bytes: u64,
) -> Result<()> {
    use lnc_client::{ClientConfig, LanceClient};

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    client
        .set_retention(topic_id, max_age_secs, max_bytes)
        .await
        .map_err(client_err)?;

    println!("Retention policy set for topic {}", topic_id);
    println!(
        "  Max age: {} seconds",
        if max_age_secs == 0 {
            "unlimited".to_string()
        } else {
            max_age_secs.to_string()
        }
    );
    println!(
        "  Max bytes: {}",
        if max_bytes == 0 {
            "unlimited".to_string()
        } else {
            format_bytes(max_bytes)
        }
    );

    Ok(())
}

/// Create a topic with optional retention policy
pub async fn create_topic(
    server_addr: &str,
    name: &str,
    max_age_secs: Option<u64>,
    max_bytes: Option<u64>,
) -> Result<()> {
    use lnc_client::{ClientConfig, LanceClient};

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    let topic = if max_age_secs.is_some() || max_bytes.is_some() {
        client
            .create_topic_with_retention(name, max_age_secs.unwrap_or(0), max_bytes.unwrap_or(0))
            .await
            .map_err(client_err)?
    } else {
        client.create_topic(name).await.map_err(client_err)?
    };

    println!("Topic created successfully:");
    println!("  ID: {}", topic.id);
    println!("  Name: {}", topic.name);
    if let Some(age) = max_age_secs {
        println!(
            "  Max age: {} seconds",
            if age == 0 {
                "unlimited".to_string()
            } else {
                age.to_string()
            }
        );
    }
    if let Some(bytes) = max_bytes {
        println!(
            "  Max bytes: {}",
            if bytes == 0 {
                "unlimited".to_string()
            } else {
                format_bytes(bytes)
            }
        );
    }

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// List all topics from a remote server
pub async fn list_topics_remote(server_addr: &str) -> Result<()> {
    use lnc_client::{ClientConfig, LanceClient};

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    let topics = client.list_topics().await.map_err(client_err)?;

    if topics.is_empty() {
        println!("No topics found on server {}", server_addr);
        return Ok(());
    }

    println!("Topics on {}:", server_addr);
    println!(
        "{:<8} {:<32} {:<20} {:>12} {:>12}",
        "ID", "NAME", "CREATED", "MAX AGE", "MAX BYTES"
    );
    println!("{}", "-".repeat(88));

    for topic in topics {
        let created = chrono::DateTime::from_timestamp(topic.created_at as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let max_age = topic
            .retention
            .as_ref()
            .map(|r| {
                if r.max_age_secs == 0 {
                    "unlimited".to_string()
                } else {
                    format!("{}s", r.max_age_secs)
                }
            })
            .unwrap_or_else(|| "-".to_string());

        let max_bytes = topic
            .retention
            .as_ref()
            .map(|r| {
                if r.max_bytes == 0 {
                    "unlimited".to_string()
                } else {
                    format_bytes(r.max_bytes)
                }
            })
            .unwrap_or_else(|| "-".to_string());

        println!(
            "{:<8} {:<32} {:<20} {:>12} {:>12}",
            topic.id, topic.name, created, max_age, max_bytes
        );
    }

    Ok(())
}

/// Get details of a specific topic from a remote server
pub async fn get_topic_remote(server_addr: &str, topic_id: u32) -> Result<()> {
    use lnc_client::{ClientConfig, LanceClient};

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    let topic = client.get_topic(topic_id).await.map_err(client_err)?;

    println!("Topic Details:");
    println!("  ID:         {}", topic.id);
    println!("  Name:       {}", topic.name);

    let created = chrono::DateTime::from_timestamp(topic.created_at as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("{} (timestamp)", topic.created_at));
    println!("  Created:    {}", created);

    if let Some(retention) = topic.retention {
        println!("  Retention:");
        if retention.max_age_secs == 0 {
            println!("    Max Age:   unlimited");
        } else {
            println!("    Max Age:   {} seconds", retention.max_age_secs);
        }
        if retention.max_bytes == 0 {
            println!("    Max Bytes: unlimited");
        } else {
            println!("    Max Bytes: {}", format_bytes(retention.max_bytes));
        }
    } else {
        println!("  Retention:  not configured");
    }

    Ok(())
}

/// Delete a topic from a remote server
pub async fn delete_topic_remote(server_addr: &str, topic_id: u32, force: bool) -> Result<()> {
    use lnc_client::{ClientConfig, LanceClient};
    use std::io::{self, Write};

    if !force {
        print!(
            "Are you sure you want to delete topic {} on {}? This cannot be undone. [y/N]: ",
            topic_id, server_addr
        );
        io::stdout().flush().ok();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            return Err(lnc_core::LanceError::Io(io::Error::other(
                "Failed to read confirmation",
            )));
        }

        let input = input.trim().to_lowercase();
        if input != "y" && input != "yes" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    client.delete_topic(topic_id).await.map_err(client_err)?;

    println!("Topic {} deleted successfully.", topic_id);

    Ok(())
}

/// Show cluster status and health
pub async fn cluster_status(server_addr: &str) -> Result<()> {
    use lnc_client::{ClientConfig, LanceClient};

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    let status = client.get_cluster_status().await.map_err(client_err)?;

    println!("Cluster Status:");
    println!("  Node ID:        {}", status.node_id);
    println!(
        "  Is Leader:      {}",
        if status.is_leader { "yes" } else { "no" }
    );
    println!(
        "  Leader ID:      {}",
        status
            .leader_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!("  Current Term:   {}", status.current_term);
    println!("  Node Count:     {}", status.node_count);
    println!("  Healthy Nodes:  {}", status.healthy_nodes);
    println!(
        "  Quorum:         {}",
        if status.quorum_available {
            "available"
        } else {
            "NOT AVAILABLE"
        }
    );

    if !status.peer_states.is_empty() {
        println!("\nPeer States:");
        for (node_id, state) in &status.peer_states {
            println!("  Node {}: {}", node_id, state);
        }
    }

    Ok(())
}

/// Ingest data to a topic from file or stdin
pub async fn ingest_data(
    server_addr: &str,
    topic_id: u32,
    file_path: Option<&str>,
    line_mode: bool,
    batch_size: usize,
) -> Result<()> {
    use bytes::Bytes;
    use lnc_client::{ClientConfig, LanceClient};
    use std::io::{self, BufRead, Read};

    let config = ClientConfig::new(server_addr);
    let mut client = LanceClient::connect(config).await.map_err(client_err)?;

    // Open input source
    let input: Box<dyn Read> = match file_path {
        Some("-") | None => Box::new(io::stdin()),
        Some(path) => Box::new(std::fs::File::open(path).map_err(lnc_core::LanceError::Io)?),
    };

    let mut total_records = 0u64;
    let mut total_bytes = 0u64;
    let start_time = std::time::Instant::now();

    if line_mode {
        // Line-delimited mode: batch lines together
        let reader = io::BufReader::new(input);
        let mut batch = Vec::with_capacity(batch_size);

        for line_result in reader.lines() {
            let line = line_result.map_err(lnc_core::LanceError::Io)?;
            if line.is_empty() {
                continue;
            }

            batch.push(line);

            if batch.len() >= batch_size {
                let payload = Bytes::from(batch.join("\n"));
                let record_count = batch.len() as u32;
                total_bytes += payload.len() as u64;
                total_records += record_count as u64;

                client
                    .send_ingest_to_topic_sync(topic_id, payload, record_count, None)
                    .await
                    .map_err(client_err)?;

                batch.clear();
            }
        }

        // Send remaining batch
        if !batch.is_empty() {
            let payload = Bytes::from(batch.join("\n"));
            let record_count = batch.len() as u32;
            total_bytes += payload.len() as u64;
            total_records += record_count as u64;

            client
                .send_ingest_to_topic_sync(topic_id, payload, record_count, None)
                .await
                .map_err(client_err)?;
        }
    } else {
        // Binary mode: read entire file as single record
        let mut reader = io::BufReader::new(input);
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .map_err(lnc_core::LanceError::Io)?;

        if !buffer.is_empty() {
            let payload = Bytes::from(buffer);
            total_bytes = payload.len() as u64;
            total_records = 1;

            client
                .send_ingest_to_topic_sync(topic_id, payload, 1, None)
                .await
                .map_err(client_err)?;
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        (total_bytes as f64 / elapsed.as_secs_f64()) / 1024.0 / 1024.0
    } else {
        0.0
    };

    println!("Ingest complete:");
    println!("  Topic:       {}", topic_id);
    println!("  Records:     {}", total_records);
    println!("  Bytes:       {}", format_bytes(total_bytes));
    println!("  Duration:    {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput:  {:.2} MB/s", throughput);

    Ok(())
}

/// Verify data integrity for a topic by checking CRC values
pub fn verify_topic_data(data_path: &Path, topic_id: u32, fail_fast: bool) -> Result<()> {
    use std::fs;

    let topic_path = data_path.join(format!("topic_{}", topic_id));
    if !topic_path.exists() {
        return Err(lnc_core::LanceError::NotFound(format!(
            "Topic {} not found at {:?}",
            topic_id, data_path
        )));
    }

    println!("Verifying topic {} at {:?}", topic_id, topic_path);

    let mut total_segments = 0u64;
    let mut verified_segments = 0u64;
    let mut total_records = 0u64;
    let mut errors = Vec::new();

    // Scan all segment files
    let entries = fs::read_dir(&topic_path).map_err(lnc_core::LanceError::Io)?;

    for entry in entries {
        let entry = entry.map_err(lnc_core::LanceError::Io)?;
        let path = entry.path();

        if path.extension().map(|e| e == "seg").unwrap_or(false) {
            total_segments += 1;

            match verify_segment_crc(&path) {
                Ok(record_count) => {
                    verified_segments += 1;
                    total_records += record_count;
                    println!(
                        "  ✓ {:?}: {} records",
                        path.file_name().unwrap_or_default(),
                        record_count
                    );
                },
                Err(e) => {
                    let error_msg = format!("{:?}: {}", path.file_name().unwrap_or_default(), e);
                    println!("  ✗ {}", error_msg);
                    errors.push(error_msg);

                    if fail_fast {
                        return Err(lnc_core::LanceError::DataCorruption(
                            "Verification failed - stopping on first error".to_string(),
                        ));
                    }
                },
            }
        }
    }

    println!("\nVerification Summary:");
    println!(
        "  Segments:  {}/{} verified",
        verified_segments, total_segments
    );
    println!("  Records:   {}", total_records);
    println!("  Errors:    {}", errors.len());

    if errors.is_empty() {
        println!("\n✓ All data verified successfully");
        Ok(())
    } else {
        println!("\n✗ Verification failed with {} errors", errors.len());
        Err(lnc_core::LanceError::DataCorruption(format!(
            "{} segment(s) failed verification",
            errors.len()
        )))
    }
}

fn verify_segment_crc(segment_path: &Path) -> Result<u64> {
    use std::fs::File;
    use std::io::{BufReader, Read};

    let file = File::open(segment_path).map_err(lnc_core::LanceError::Io)?;
    let mut reader = BufReader::new(file);
    let mut record_count = 0u64;

    // Read and verify each record's CRC
    let mut header_buf = [0u8; 5]; // TLV header: type(1) + length(4)

    loop {
        match reader.read_exact(&mut header_buf) {
            Ok(()) => {
                let length = u32::from_le_bytes([
                    header_buf[1],
                    header_buf[2],
                    header_buf[3],
                    header_buf[4],
                ]) as usize;

                // Skip the value bytes
                let mut value_buf = vec![0u8; length];
                reader
                    .read_exact(&mut value_buf)
                    .map_err(lnc_core::LanceError::Io)?;

                record_count += 1;
            },
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(lnc_core::LanceError::Io(e)),
        }
    }

    Ok(record_count)
}

/// Export topic data to a file for backup
pub fn export_topic(
    data_path: &Path,
    topic_id: u32,
    output: &Path,
    with_metadata: bool,
) -> Result<()> {
    use std::fs::{self, File};
    use std::io::{BufReader, BufWriter, Read, Write};

    let topic_path = data_path.join(format!("topic_{}", topic_id));
    if !topic_path.exists() {
        return Err(lnc_core::LanceError::NotFound(format!(
            "Topic {} not found at {:?}",
            topic_id, data_path
        )));
    }

    println!("Exporting topic {} to {:?}", topic_id, output);

    let out_file = File::create(output).map_err(lnc_core::LanceError::Io)?;
    let mut writer = BufWriter::new(out_file);

    // Write export header
    let header = serde_json::json!({
        "version": 1,
        "topic_id": topic_id,
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "with_metadata": with_metadata
    });
    let header_bytes =
        serde_json::to_vec(&header).map_err(|e| lnc_core::LanceError::Config(e.to_string()))?;

    writer
        .write_all(&(header_bytes.len() as u32).to_le_bytes())
        .map_err(lnc_core::LanceError::Io)?;
    writer
        .write_all(&header_bytes)
        .map_err(lnc_core::LanceError::Io)?;

    let mut total_bytes = 0u64;
    let mut segment_count = 0u64;

    // Export all segment files
    let mut entries: Vec<_> = fs::read_dir(&topic_path)
        .map_err(lnc_core::LanceError::Io)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "seg")
                .unwrap_or(false)
        })
        .collect();

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        let file = File::open(&path).map_err(lnc_core::LanceError::Io)?;
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        reader
            .read_to_end(&mut data)
            .map_err(lnc_core::LanceError::Io)?;

        // Write segment marker and data
        writer
            .write_all(&(data.len() as u64).to_le_bytes())
            .map_err(lnc_core::LanceError::Io)?;
        writer.write_all(&data).map_err(lnc_core::LanceError::Io)?;

        total_bytes += data.len() as u64;
        segment_count += 1;

        println!(
            "  Exported {:?} ({} bytes)",
            path.file_name().unwrap_or_default(),
            data.len()
        );
    }

    // Write end marker
    writer
        .write_all(&0u64.to_le_bytes())
        .map_err(lnc_core::LanceError::Io)?;
    writer.flush().map_err(lnc_core::LanceError::Io)?;

    println!("\nExport Summary:");
    println!("  Segments:  {}", segment_count);
    println!("  Total:     {}", format_bytes(total_bytes));
    println!("\n✓ Export completed successfully");

    Ok(())
}

/// Import topic data from a backup file
pub fn import_topic(data_path: &Path, topic_name: &str, input: &Path) -> Result<()> {
    use std::fs::{self, File};
    use std::io::{BufReader, BufWriter, Read, Write};

    if !input.exists() {
        return Err(lnc_core::LanceError::NotFound(format!(
            "Input file not found: {:?}",
            input
        )));
    }

    println!("Importing topic '{}' from {:?}", topic_name, input);

    let in_file = File::open(input).map_err(lnc_core::LanceError::Io)?;
    let mut reader = BufReader::new(in_file);

    // Read and parse header
    let mut header_len_buf = [0u8; 4];
    reader
        .read_exact(&mut header_len_buf)
        .map_err(lnc_core::LanceError::Io)?;
    let header_len = u32::from_le_bytes(header_len_buf) as usize;

    let mut header_buf = vec![0u8; header_len];
    reader
        .read_exact(&mut header_buf)
        .map_err(lnc_core::LanceError::Io)?;

    let header: serde_json::Value = serde_json::from_slice(&header_buf)
        .map_err(|e| lnc_core::LanceError::Config(format!("Invalid export header: {}", e)))?;

    println!("  Export version: {}", header["version"]);
    println!("  Original topic: {}", header["topic_id"]);
    println!("  Exported at: {}", header["exported_at"]);

    // Generate new topic ID (use timestamp-based for uniqueness)
    let new_topic_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_millis() % 1_000_000) as u32)
        .unwrap_or(1);

    let topic_path = data_path.join(format!("topic_{}", new_topic_id));
    fs::create_dir_all(&topic_path).map_err(lnc_core::LanceError::Io)?;

    let mut segment_count = 0u64;
    let mut total_bytes = 0u64;

    // Read and write segment data
    loop {
        let mut len_buf = [0u8; 8];
        reader
            .read_exact(&mut len_buf)
            .map_err(lnc_core::LanceError::Io)?;
        let segment_len = u64::from_le_bytes(len_buf) as usize;

        if segment_len == 0 {
            break; // End marker
        }

        let mut segment_data = vec![0u8; segment_len];
        reader
            .read_exact(&mut segment_data)
            .map_err(lnc_core::LanceError::Io)?;

        let segment_path = topic_path.join(format!("{:016}.seg", segment_count));
        let out_file = File::create(&segment_path).map_err(lnc_core::LanceError::Io)?;
        let mut writer = BufWriter::new(out_file);
        writer
            .write_all(&segment_data)
            .map_err(lnc_core::LanceError::Io)?;
        writer.flush().map_err(lnc_core::LanceError::Io)?;

        total_bytes += segment_len as u64;
        segment_count += 1;

        println!(
            "  Imported segment {} ({} bytes)",
            segment_count, segment_len
        );
    }

    // Write topic metadata
    let metadata = serde_json::json!({
        "id": new_topic_id,
        "name": topic_name,
        "created_at": chrono::Utc::now().timestamp(),
        "imported_from": header["topic_id"],
        "imported_at": chrono::Utc::now().to_rfc3339()
    });
    let metadata_path = topic_path.join("metadata.json");
    let metadata_file = File::create(&metadata_path).map_err(lnc_core::LanceError::Io)?;
    serde_json::to_writer_pretty(metadata_file, &metadata)
        .map_err(|e| lnc_core::LanceError::Config(e.to_string()))?;

    println!("\nImport Summary:");
    println!("  New Topic ID: {}", new_topic_id);
    println!("  Segments:     {}", segment_count);
    println!("  Total:        {}", format_bytes(total_bytes));
    println!("\n✓ Import completed successfully");

    Ok(())
}
