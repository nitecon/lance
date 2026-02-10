//! Follower startup resync protocol implementation.
//!
//! Per RaftQuorumWartime.md §5.2:
//! When a follower starts up and discovers it may be behind, it initiates
//! a resync protocol to fetch missing segment files from the leader.
//!
//! This module provides:
//! - Leader-side: manifest building and segment chunk serving
//! - Follower-side: manifest auditing, segment fetching, and resync state machine

use crate::health::HealthState;
use crate::topic::TopicRegistry;
use lnc_core::Result;
use lnc_replication::{
    ClusterCoordinator, ReplicationMessage, ResyncBegin, ResyncComplete, SegmentFetchRequest,
    SegmentFetchResponse, SegmentInfo, SegmentManifestRequest, SegmentManifestResponse,
    TopicManifest,
};
use std::io::{Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Default chunk size for segment fetch responses (256KB).
/// Used by the follower resync state machine when requesting chunks.
#[allow(dead_code)]
pub const DEFAULT_CHUNK_SIZE: u32 = 256 * 1024;

/// Build a segment manifest for all topics on this node.
///
/// Per RaftQuorumWartime.md §7.1:
/// The leader enumerates all topics and their segment files,
/// computing CRC32C checksums for closed (immutable) segments.
pub fn build_segment_manifest(registry: &TopicRegistry) -> Result<SegmentManifestResponse> {
    let topics = registry.list_topics();
    let mut manifests = Vec::with_capacity(topics.len());

    for topic in &topics {
        let topic_dir = registry.get_topic_dir(topic.id);
        if !topic_dir.exists() {
            continue;
        }

        let (closed, active) = enumerate_segments(&topic_dir)?;

        manifests.push(TopicManifest {
            topic_id: topic.id,
            topic_name: topic.name.clone(),
            active_segment: active.unwrap_or_default(),
            closed_segments: closed,
        });
    }

    info!(
        target: "lance::resync",
        topic_count = manifests.len(),
        total_closed = manifests.iter().map(|m| m.closed_segments.len()).sum::<usize>(),
        "Built segment manifest"
    );

    Ok(SegmentManifestResponse { topics: manifests })
}

/// Enumerate segment files in a topic directory.
///
/// Returns (closed_segments, active_segment_name).
/// Closed segments have a dash in their stem: `{start}_{ts1}-{ts2}.lnc`
/// Active segments have no dash: `{start}_{ts}.lnc`
fn enumerate_segments(topic_dir: &Path) -> Result<(Vec<SegmentInfo>, Option<String>)> {
    let mut closed = Vec::new();
    let mut active: Option<String> = None;

    let entries = std::fs::read_dir(topic_dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("lnc") {
            continue;
        }

        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

        // Closed segments contain a dash separating start and end timestamps
        if stem.contains('-') {
            let metadata = entry.metadata()?;
            let size_bytes = metadata.len();
            let crc32c = compute_file_crc32c(&path)?;

            closed.push(SegmentInfo {
                name: file_name,
                crc32c,
                size_bytes,
            });
        } else {
            // Active segment (only one per topic)
            active = Some(file_name);
        }
    }

    // Sort closed segments by name for deterministic ordering
    closed.sort_by(|a, b| a.name.cmp(&b.name));

    Ok((closed, active))
}

/// Compute CRC32C checksum of an entire file.
///
/// Reads the full file into memory. This is acceptable because:
/// - Only called during resync (not hot path)
/// - Closed segments are bounded in size
fn compute_file_crc32c(path: &Path) -> Result<u32> {
    let data = std::fs::read(path)?;
    Ok(lnc_core::crc32c(&data))
}

/// Read a chunk of a segment file for serving to a follower.
///
/// Per RaftQuorumWartime.md §7.2:
/// Returns up to `max_chunk_size` bytes starting at `offset`.
pub fn read_segment_chunk(
    registry: &TopicRegistry,
    topic_id: u32,
    segment_name: &str,
    offset: u64,
    max_chunk_size: u32,
) -> Result<(bytes::Bytes, u64, bool)> {
    let topic_dir = registry.get_topic_dir(topic_id);
    let segment_path = topic_dir.join(segment_name);

    if !segment_path.exists() {
        return Err(lnc_core::LanceError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Segment not found: {}", segment_path.display()),
        )));
    }

    let metadata = std::fs::metadata(&segment_path)?;
    let total_size = metadata.len();

    if offset >= total_size {
        // Already past end — return empty final chunk
        return Ok((bytes::Bytes::new(), total_size, true));
    }

    let remaining = total_size - offset;
    let chunk_size = std::cmp::min(remaining, max_chunk_size as u64) as usize;

    let mut file = std::fs::File::open(&segment_path)?;
    file.seek(std::io::SeekFrom::Start(offset))?;

    let mut buf = vec![0u8; chunk_size];
    file.read_exact(&mut buf)?;

    let done = offset + chunk_size as u64 >= total_size;

    debug!(
        target: "lance::resync",
        topic_id,
        segment = segment_name,
        offset,
        chunk_size,
        total_size,
        done,
        "Serving segment chunk"
    );

    Ok((bytes::Bytes::from(buf), total_size, done))
}

// =============================================================================
// Follower-side resync state machine
// =============================================================================

/// Action that the follower resync audit determines for a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum SegmentAction {
    /// Segment exists locally and CRC matches — no action needed.
    Ok,
    /// Segment is missing locally — fetch from leader.
    Fetch,
    /// Segment exists but CRC mismatches — delete and re-fetch.
    CrcMismatch { local_crc: u32, manifest_crc: u32 },
}

/// Result of auditing one topic's segments against the leader manifest.
#[derive(Debug)]
pub struct TopicAuditResult {
    pub topic_id: u32,
    pub topic_name: String,
    /// Leader's current active segment name (for creating fresh writer after resync).
    #[allow(dead_code)]
    pub active_segment: String,
    /// Segments that need fetching (missing or CRC mismatch).
    pub segments_to_fetch: Vec<SegmentInfo>,
    /// Segments that are OK (already in sync).
    pub segments_ok: usize,
    /// Extra local segments not in manifest (stale).
    pub extra_local: Vec<String>,
}

/// Audit local segments against the leader's manifest for a single topic.
///
/// Per RaftQuorumWartime.md §5.2 Step 7:
/// Compare local closed segments vs manifest, determine fetch/skip/delete actions.
pub fn audit_topic_segments(
    registry: &TopicRegistry,
    manifest: &TopicManifest,
) -> Result<TopicAuditResult> {
    let topic_dir = registry.get_topic_dir(manifest.topic_id);

    // Get local segments (if directory exists)
    let (local_closed, _local_active) = if topic_dir.exists() {
        enumerate_segments(&topic_dir)?
    } else {
        (Vec::new(), None)
    };

    // Build lookup of local segments by name
    let local_map: std::collections::HashMap<&str, &SegmentInfo> =
        local_closed.iter().map(|s| (s.name.as_str(), s)).collect();

    // Build lookup of manifest segments by name
    let manifest_map: std::collections::HashMap<&str, &SegmentInfo> = manifest
        .closed_segments
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    let mut segments_to_fetch = Vec::new();
    let mut segments_ok = 0;

    // Check each manifest segment against local state
    for manifest_seg in &manifest.closed_segments {
        match local_map.get(manifest_seg.name.as_str()) {
            Some(local_seg) => {
                if local_seg.crc32c == manifest_seg.crc32c
                    && local_seg.size_bytes == manifest_seg.size_bytes
                {
                    segments_ok += 1;
                } else {
                    // CRC or size mismatch — need to re-fetch
                    warn!(
                        target: "lance::resync",
                        topic_id = manifest.topic_id,
                        segment = %manifest_seg.name,
                        local_crc = local_seg.crc32c,
                        manifest_crc = manifest_seg.crc32c,
                        local_size = local_seg.size_bytes,
                        manifest_size = manifest_seg.size_bytes,
                        "Segment CRC/size mismatch — will re-fetch"
                    );
                    // Delete the local mismatched file
                    let local_path = topic_dir.join(&manifest_seg.name);
                    if local_path.exists() {
                        if let Err(e) = std::fs::remove_file(&local_path) {
                            warn!(
                                target: "lance::resync",
                                path = %local_path.display(),
                                error = %e,
                                "Failed to delete mismatched segment"
                            );
                        }
                    }
                    segments_to_fetch.push(manifest_seg.clone());
                }
            },
            None => {
                // Missing locally — need to fetch
                segments_to_fetch.push(manifest_seg.clone());
            },
        }
    }

    // Find extra local segments not in manifest (stale)
    let extra_local: Vec<String> = local_closed
        .iter()
        .filter(|s| !manifest_map.contains_key(s.name.as_str()))
        .map(|s| s.name.clone())
        .collect();

    if !extra_local.is_empty() {
        warn!(
            target: "lance::resync",
            topic_id = manifest.topic_id,
            extra_count = extra_local.len(),
            "Found extra local segments not in leader manifest (stale from previous term)"
        );
    }

    Ok(TopicAuditResult {
        topic_id: manifest.topic_id,
        topic_name: manifest.topic_name.clone(),
        active_segment: manifest.active_segment.clone(),
        segments_to_fetch,
        segments_ok,
        extra_local,
    })
}

/// Write a fetched segment chunk to disk.
///
/// Creates the file on first chunk (offset=0), appends on subsequent chunks.
pub fn write_segment_chunk(
    registry: &TopicRegistry,
    topic_id: u32,
    segment_name: &str,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    let topic_dir = registry.get_topic_dir(topic_id);
    std::fs::create_dir_all(&topic_dir)?;
    let segment_path = topic_dir.join(segment_name);

    if offset == 0 {
        // First chunk — create/truncate file
        let mut file = std::fs::File::create(&segment_path)?;
        file.write_all(data)?;
        file.sync_all()?;
    } else {
        // Subsequent chunk — append
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&segment_path)?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_all()?;
    }

    Ok(())
}

/// Verify a fully-fetched segment's CRC32C against the expected value.
pub fn verify_segment_crc(
    registry: &TopicRegistry,
    topic_id: u32,
    segment_name: &str,
    expected_crc: u32,
) -> Result<bool> {
    let topic_dir = registry.get_topic_dir(topic_id);
    let segment_path = topic_dir.join(segment_name);
    let actual_crc = compute_file_crc32c(&segment_path)?;
    Ok(actual_crc == expected_crc)
}

/// Run the follower resync state machine.
///
/// Per RaftQuorumWartime.md §5.2:
/// This drives the full resync protocol from the follower's perspective.
///
/// The `resync_rx` channel receives `SegmentManifestResponse` and
/// `SegmentFetchResponse` messages forwarded from the cluster event loop.
pub async fn run_follower_resync(
    node_id: u16,
    coordinator: Arc<ClusterCoordinator>,
    registry: Arc<TopicRegistry>,
    health: Arc<HealthState>,
    mut resync_rx: tokio::sync::mpsc::Receiver<ResyncEvent>,
) {
    info!(
        target: "lance::resync",
        node_id,
        "Starting follower resync state machine"
    );

    // Step 2: Set NOT_READY
    health.set_catching_up(true);

    // Step 3: ACK mode → CatchingUp (handled by the cluster coordinator)

    // Step 4: Send ResyncBegin to leader
    let leader_id = match coordinator.leader_id().await {
        Some(id) => id,
        None => {
            error!(
                target: "lance::resync",
                "No leader known — cannot start resync"
            );
            health.set_catching_up(false);
            health.set_ready(true);
            return;
        },
    };

    let term = coordinator.current_term().await;
    let resync_begin = ReplicationMessage::ResyncBegin(ResyncBegin { node_id, term });
    if let Err(e) = coordinator.send_to_peer(leader_id, &resync_begin).await {
        error!(
            target: "lance::resync",
            leader_id,
            error = %e,
            "Failed to send ResyncBegin to leader"
        );
        health.set_catching_up(false);
        health.set_ready(true);
        return;
    }
    info!(
        target: "lance::resync",
        leader_id,
        term,
        "Sent ResyncBegin to leader"
    );

    // Step 5: Send SegmentManifestRequest
    let manifest_req =
        ReplicationMessage::SegmentManifestRequest(SegmentManifestRequest { node_id });
    if let Err(e) = coordinator.send_to_peer(leader_id, &manifest_req).await {
        error!(
            target: "lance::resync",
            leader_id,
            error = %e,
            "Failed to send SegmentManifestRequest to leader"
        );
        health.set_catching_up(false);
        health.set_ready(true);
        return;
    }

    // Step 6: Wait for SegmentManifestResponse
    let manifest = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        wait_for_manifest(&mut resync_rx),
    )
    .await
    {
        Ok(Some(m)) => m,
        Ok(None) => {
            error!(
                target: "lance::resync",
                "Resync channel closed before receiving manifest"
            );
            health.set_catching_up(false);
            health.set_ready(true);
            return;
        },
        Err(_) => {
            error!(
                target: "lance::resync",
                "Timed out waiting for segment manifest from leader"
            );
            health.set_catching_up(false);
            health.set_ready(true);
            return;
        },
    };

    info!(
        target: "lance::resync",
        topic_count = manifest.topics.len(),
        total_closed = manifest.topics.iter().map(|t| t.closed_segments.len()).sum::<usize>(),
        "Received segment manifest from leader"
    );

    // Step 7: Audit & Fetch
    let mut total_fetched = 0u64;
    let mut total_ok = 0usize;

    for topic_manifest in &manifest.topics {
        let audit = match audit_topic_segments(&registry, topic_manifest) {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    target: "lance::resync",
                    topic_id = topic_manifest.topic_id,
                    error = %e,
                    "Failed to audit topic segments — skipping"
                );
                continue;
            },
        };

        total_ok += audit.segments_ok;

        info!(
            target: "lance::resync",
            topic_id = audit.topic_id,
            topic_name = %audit.topic_name,
            ok = audit.segments_ok,
            to_fetch = audit.segments_to_fetch.len(),
            extra = audit.extra_local.len(),
            "Topic audit complete"
        );

        // Fetch missing/mismatched segments
        for seg in &audit.segments_to_fetch {
            match fetch_segment(
                node_id,
                leader_id,
                &coordinator,
                &registry,
                audit.topic_id,
                seg,
                &mut resync_rx,
            )
            .await
            {
                Ok(bytes) => {
                    total_fetched += bytes;
                    info!(
                        target: "lance::resync",
                        topic_id = audit.topic_id,
                        segment = %seg.name,
                        bytes,
                        "Fetched segment successfully"
                    );
                },
                Err(e) => {
                    error!(
                        target: "lance::resync",
                        topic_id = audit.topic_id,
                        segment = %seg.name,
                        error = %e,
                        "Failed to fetch segment — resync may be incomplete"
                    );
                },
            }
        }
    }

    info!(
        target: "lance::resync",
        total_ok,
        total_fetched_bytes = total_fetched,
        "Segment audit and fetch complete"
    );

    // Step 10: Send ResyncComplete
    let term = coordinator.current_term().await;
    let resync_complete = ReplicationMessage::ResyncComplete(ResyncComplete { node_id, term });
    if let Err(e) = coordinator.send_to_peer(leader_id, &resync_complete).await {
        warn!(
            target: "lance::resync",
            leader_id,
            error = %e,
            "Failed to send ResyncComplete to leader"
        );
    }

    // Step 11-12: ACK mode → Ok, health → READY
    health.set_catching_up(false);
    health.set_ready(true);

    info!(
        target: "lance::resync",
        node_id,
        total_ok,
        total_fetched_bytes = total_fetched,
        "Follower resync complete — now ready"
    );
}

/// Events forwarded from the cluster event loop to the resync state machine.
#[derive(Debug)]
pub enum ResyncEvent {
    /// Leader sent segment manifest response.
    ManifestReceived(SegmentManifestResponse),
    /// Leader sent segment fetch response.
    FetchReceived(SegmentFetchResponse),
}

/// Wait for a manifest response from the resync event channel.
async fn wait_for_manifest(
    rx: &mut tokio::sync::mpsc::Receiver<ResyncEvent>,
) -> Option<SegmentManifestResponse> {
    while let Some(event) = rx.recv().await {
        match event {
            ResyncEvent::ManifestReceived(m) => return Some(m),
            ResyncEvent::FetchReceived(_) => {
                debug!(
                    target: "lance::resync",
                    "Ignoring FetchReceived while waiting for manifest"
                );
            },
        }
    }
    None
}

/// Fetch a single segment from the leader using chunked transfer.
///
/// Per RaftQuorumWartime.md §7.2:
/// Requests 256KB chunks until the entire segment is received,
/// then verifies CRC32C.
async fn fetch_segment(
    node_id: u16,
    leader_id: u16,
    coordinator: &Arc<ClusterCoordinator>,
    registry: &TopicRegistry,
    topic_id: u32,
    seg_info: &SegmentInfo,
    resync_rx: &mut tokio::sync::mpsc::Receiver<ResyncEvent>,
) -> Result<u64> {
    let mut offset: u64 = 0;
    let mut total_received: u64 = 0;

    loop {
        // Send fetch request
        let fetch_req = ReplicationMessage::SegmentFetchRequest(SegmentFetchRequest {
            node_id,
            topic_id,
            segment_name: seg_info.name.clone(),
            offset,
            max_chunk_size: DEFAULT_CHUNK_SIZE,
        });

        if let Err(e) = coordinator.send_to_peer(leader_id, &fetch_req).await {
            return Err(lnc_core::LanceError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("Failed to send SegmentFetchRequest: {e}"),
            )));
        }

        // Wait for fetch response
        let resp = match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            wait_for_fetch(resync_rx),
        )
        .await
        {
            Ok(Some(r)) => r,
            Ok(None) => {
                return Err(lnc_core::LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Resync channel closed during segment fetch",
                )));
            },
            Err(_) => {
                return Err(lnc_core::LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timed out waiting for segment fetch response",
                )));
            },
        };

        // Verify this is the response we expected
        if resp.topic_id != topic_id || resp.segment_name != seg_info.name {
            warn!(
                target: "lance::resync",
                expected_topic = topic_id,
                got_topic = resp.topic_id,
                expected_seg = %seg_info.name,
                got_seg = %resp.segment_name,
                "Received unexpected fetch response — skipping"
            );
            continue;
        }

        // Write chunk to disk
        write_segment_chunk(registry, topic_id, &seg_info.name, resp.offset, &resp.data)?;
        total_received += resp.data.len() as u64;
        offset = resp.offset + resp.data.len() as u64;

        debug!(
            target: "lance::resync",
            topic_id,
            segment = %seg_info.name,
            offset,
            total_received,
            total_size = resp.total_size,
            done = resp.done,
            "Received segment chunk"
        );

        if resp.done {
            break;
        }
    }

    // Verify CRC32C
    match verify_segment_crc(registry, topic_id, &seg_info.name, seg_info.crc32c) {
        Ok(true) => {
            debug!(
                target: "lance::resync",
                topic_id,
                segment = %seg_info.name,
                crc = seg_info.crc32c,
                "Segment CRC verified"
            );
        },
        Ok(false) => {
            error!(
                target: "lance::resync",
                topic_id,
                segment = %seg_info.name,
                expected_crc = seg_info.crc32c,
                "Segment CRC verification FAILED after fetch"
            );
            return Err(lnc_core::LanceError::InvalidData(format!(
                "CRC mismatch after fetching segment {}",
                seg_info.name
            )));
        },
        Err(e) => {
            warn!(
                target: "lance::resync",
                topic_id,
                segment = %seg_info.name,
                error = %e,
                "Failed to verify segment CRC"
            );
        },
    }

    Ok(total_received)
}

/// Wait for a fetch response from the resync event channel.
async fn wait_for_fetch(
    rx: &mut tokio::sync::mpsc::Receiver<ResyncEvent>,
) -> Option<SegmentFetchResponse> {
    while let Some(event) = rx.recv().await {
        match event {
            ResyncEvent::FetchReceived(r) => return Some(r),
            ResyncEvent::ManifestReceived(_) => {
                debug!(
                    target: "lance::resync",
                    "Ignoring ManifestReceived while waiting for fetch response"
                );
            },
        }
    }
    None
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_enumerate_segments_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let (closed, active) = enumerate_segments(dir.path()).unwrap();
        assert!(closed.is_empty());
        assert!(active.is_none());
    }

    #[test]
    fn test_enumerate_segments_mixed() {
        let dir = tempfile::tempdir().unwrap();

        // Create a closed segment
        let closed_path = dir.path().join("0_1000-2000.lnc");
        std::fs::write(&closed_path, b"closed segment data").unwrap();

        // Create an active segment
        let active_path = dir.path().join("50_3000.lnc");
        std::fs::write(&active_path, b"active segment data").unwrap();

        // Create a non-segment file (should be ignored)
        let other_path = dir.path().join("metadata.json");
        std::fs::write(&other_path, b"{}").unwrap();

        let (closed, active) = enumerate_segments(dir.path()).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].name, "0_1000-2000.lnc");
        assert_eq!(closed[0].size_bytes, 19); // "closed segment data".len()
        assert!(closed[0].crc32c != 0);
        assert_eq!(active, Some("50_3000.lnc".to_string()));
    }

    #[test]
    fn test_compute_file_crc32c() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.lnc");
        let data = b"hello world";
        std::fs::write(&path, data).unwrap();

        let crc = compute_file_crc32c(&path).unwrap();
        assert_eq!(crc, lnc_core::crc32c(data));
    }

    #[test]
    fn test_read_segment_chunk_basic() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        // TopicRegistry::get_topic_dir returns data_dir/segments/{topic_id}
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        let seg_path = topic_dir.join("0_1000-2000.lnc");
        let data = vec![0xABu8; 1024];
        std::fs::write(&seg_path, &data).unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();

        // Read first chunk
        let (chunk, total, done) =
            read_segment_chunk(&registry, 1, "0_1000-2000.lnc", 0, 512).unwrap();
        assert_eq!(chunk.len(), 512);
        assert_eq!(total, 1024);
        assert!(!done);

        // Read second chunk
        let (chunk, total, done) =
            read_segment_chunk(&registry, 1, "0_1000-2000.lnc", 512, 512).unwrap();
        assert_eq!(chunk.len(), 512);
        assert_eq!(total, 1024);
        assert!(done);
    }

    #[test]
    fn test_read_segment_chunk_past_end() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        let seg_path = topic_dir.join("0_1000-2000.lnc");
        std::fs::write(&seg_path, b"small").unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();
        let (chunk, total, done) =
            read_segment_chunk(&registry, 1, "0_1000-2000.lnc", 100, 512).unwrap();
        assert!(chunk.is_empty());
        assert_eq!(total, 5);
        assert!(done);
    }

    #[test]
    fn test_audit_topic_segments_all_match() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        let seg_data = b"segment data here";
        let seg_path = topic_dir.join("0_1000-2000.lnc");
        std::fs::write(&seg_path, seg_data).unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();
        let manifest = TopicManifest {
            topic_id: 1,
            topic_name: "test".to_string(),
            active_segment: "50_3000.lnc".to_string(),
            closed_segments: vec![SegmentInfo {
                name: "0_1000-2000.lnc".to_string(),
                crc32c: lnc_core::crc32c(seg_data),
                size_bytes: seg_data.len() as u64,
            }],
        };

        let result = audit_topic_segments(&registry, &manifest).unwrap();
        assert_eq!(result.segments_ok, 1);
        assert!(result.segments_to_fetch.is_empty());
        assert!(result.extra_local.is_empty());
    }

    #[test]
    fn test_audit_topic_segments_missing() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();
        let manifest = TopicManifest {
            topic_id: 1,
            topic_name: "test".to_string(),
            active_segment: "50_3000.lnc".to_string(),
            closed_segments: vec![SegmentInfo {
                name: "0_1000-2000.lnc".to_string(),
                crc32c: 12345,
                size_bytes: 100,
            }],
        };

        let result = audit_topic_segments(&registry, &manifest).unwrap();
        assert_eq!(result.segments_ok, 0);
        assert_eq!(result.segments_to_fetch.len(), 1);
        assert_eq!(result.segments_to_fetch[0].name, "0_1000-2000.lnc");
    }

    #[test]
    fn test_audit_topic_segments_crc_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        let seg_data = b"local data";
        let seg_path = topic_dir.join("0_1000-2000.lnc");
        std::fs::write(&seg_path, seg_data).unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();
        let manifest = TopicManifest {
            topic_id: 1,
            topic_name: "test".to_string(),
            active_segment: "50_3000.lnc".to_string(),
            closed_segments: vec![SegmentInfo {
                name: "0_1000-2000.lnc".to_string(),
                crc32c: 99999, // Different from actual CRC
                size_bytes: seg_data.len() as u64,
            }],
        };

        let result = audit_topic_segments(&registry, &manifest).unwrap();
        assert_eq!(result.segments_ok, 0);
        assert_eq!(result.segments_to_fetch.len(), 1);
        // The mismatched local file should have been deleted
        assert!(!seg_path.exists());
    }

    #[test]
    fn test_audit_topic_segments_extra_local() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        // Create a local segment not in the manifest
        let seg_path = topic_dir.join("0_500-900.lnc");
        std::fs::write(&seg_path, b"stale data").unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();
        let manifest = TopicManifest {
            topic_id: 1,
            topic_name: "test".to_string(),
            active_segment: "50_3000.lnc".to_string(),
            closed_segments: vec![],
        };

        let result = audit_topic_segments(&registry, &manifest).unwrap();
        assert_eq!(result.segments_ok, 0);
        assert!(result.segments_to_fetch.is_empty());
        assert_eq!(result.extra_local.len(), 1);
        assert_eq!(result.extra_local[0], "0_500-900.lnc");
    }

    #[test]
    fn test_write_segment_chunk_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let registry = TopicRegistry::new(data_dir.clone()).unwrap();

        let chunk1 = vec![0xAAu8; 512];
        write_segment_chunk(&registry, 1, "0_1000-2000.lnc", 0, &chunk1).unwrap();

        let chunk2 = vec![0xBBu8; 512];
        write_segment_chunk(&registry, 1, "0_1000-2000.lnc", 512, &chunk2).unwrap();

        // Verify file contents
        let seg_path = data_dir.join("segments").join("1").join("0_1000-2000.lnc");
        let contents = std::fs::read(&seg_path).unwrap();
        assert_eq!(contents.len(), 1024);
        assert!(contents[..512].iter().all(|&b| b == 0xAA));
        assert!(contents[512..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn test_verify_segment_crc_match() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let topic_dir = data_dir.join("segments").join("1");
        std::fs::create_dir_all(&topic_dir).unwrap();

        let data = b"test data for crc";
        let seg_path = topic_dir.join("0_1000-2000.lnc");
        std::fs::write(&seg_path, data).unwrap();

        let registry = TopicRegistry::new(data_dir).unwrap();
        let expected_crc = lnc_core::crc32c(data);

        assert!(verify_segment_crc(&registry, 1, "0_1000-2000.lnc", expected_crc).unwrap());
        assert!(!verify_segment_crc(&registry, 1, "0_1000-2000.lnc", 99999).unwrap());
    }
}
