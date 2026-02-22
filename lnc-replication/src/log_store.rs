//! Persistent Raft log storage
//!
//! Provides durable storage for Raft log entries with the following guarantees:
//! - Entries are persisted before acknowledgment
//! - Log can be truncated for compaction
//! - Crash recovery restores committed state

use crate::codec::{EntryType, LogEntry};
use crate::segment::{SegmentConfig, SegmentManager};
use bytes::Bytes;
use crc32fast::Hasher;
use lnc_core::{HlcTimestamp, LanceError, Result};
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Persistent state that must survive restarts (Raft §5.2)
#[derive(Debug, Clone, Default)]
pub struct PersistentState {
    /// Latest term server has seen
    pub current_term: u64,
    /// Candidate ID that received vote in current term
    pub voted_for: Option<u16>,
}

/// Snapshot metadata for log compaction
#[derive(Debug, Clone, Copy, Default)]
pub struct SnapshotMeta {
    /// Index of last entry included in snapshot
    pub last_included_index: u64,
    /// Term of last entry included in snapshot
    pub last_included_term: u64,
}

/// Configuration for automatic log compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of entries before triggering compaction
    pub max_entries: usize,
    /// Minimum entries to keep after compaction
    pub min_entries_retained: usize,
    /// Whether automatic compaction is enabled
    pub enabled: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            min_entries_retained: 1_000,
            enabled: true,
        }
    }
}

/// On-disk format for a log entry (with dual CRC32 checksums for corruption detection)
/// [term: u64][index: u64][hlc_raw: u64][entry_type: u8][data_len: u32][header_crc: u32][data: bytes][data_crc: u32]
///
/// **Dual Checksum Design**: Validates header (including data_len) before reading payload.
/// This prevents allocation attacks where a corrupted data_len field could cause OOM.
const ENTRY_HEADER_SIZE: usize = 8 + 8 + 8 + 1 + 4; // 29 bytes (excluding checksums)
const HEADER_CRC_SIZE: usize = 4; // CRC32 for header
const DATA_CRC_SIZE: usize = 4; // CRC32 for data

/// Persistent Raft log store with segmented architecture
///
/// **Segmented Architecture**: Uses SegmentManager to split log across multiple
/// {index}_{ts}.lnc files. This enables O(1) compaction via file deletion instead
/// of O(N) rewrite.
///
/// **Memory Optimization**: Each segment maintains an in-memory index, keeping
/// memory usage constant regardless of log size.
///
/// **Performance**: Compaction = fs::remove_file() (O(1)), not rewrite_log() (O(N)).
pub struct LogStore {
    /// Directory for log files
    dir: PathBuf,
    /// Segment manager for O(1) compaction
    segments: SegmentManager,
    /// Persistent state file path
    state_path: PathBuf,
    /// Snapshot metadata (last compacted entry)
    snapshot_meta: SnapshotMeta,
    /// Compaction configuration
    compaction_config: CompactionConfig,
}

impl LogStore {
    /// Create or open a log store at the given directory
    ///
    /// **Bootstrap Logic**: Scans directory for .lnc segment files, builds indices,
    /// and handles partial segment recovery after crashes.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let state_path = dir.join("raft.state");
        let meta_path = dir.join("raft.meta");

        // Load snapshot metadata
        let snapshot_meta = Self::load_snapshot_meta(&meta_path)?;

        // Initialize segment manager with configuration
        let segment_config = SegmentConfig {
            max_entries_per_segment: 10_000,
        };

        let mut segments = SegmentManager::open(dir, segment_config)?;

        // Build indices for all segments (handles torn writes)
        Self::bootstrap_segments(&mut segments)?;

        let store = Self {
            dir: dir.to_path_buf(),
            segments,
            state_path,
            snapshot_meta,
            compaction_config: CompactionConfig::default(),
        };

        info!(
            target: "lance::raft::log",
            dir = %dir.display(),
            segments = store.segments.segment_count(),
            entries = store.segments.total_entry_count(),
            first_index = store.segments.first_index().unwrap_or(1),
            "Log store opened with segmented architecture"
        );

        Ok(store)
    }

    /// Bootstrap all segments by building their indices
    ///
    /// **Operational Readiness**: Source of truth for SREs who rsync files and start nodes.
    /// Reconstructs indices from segment files and validates the last entry in the active
    /// segment to detect torn writes from interrupted rsync operations.
    ///
    /// **Torn Write Recovery**: If the active segment has a corrupted last entry,
    /// automatically truncates it before the node joins the Raft cluster.
    ///
    /// **10X Parallel Bootstrap**: Sealed segments build indices in parallel via rayon.
    /// Each segment's build_index is independent (separate file, separate BufReader),
    /// so on NVMe drives with hundreds of sealed segments this reduces startup from
    /// seconds to milliseconds by saturating IOPS.
    fn bootstrap_segments(segments: &mut SegmentManager) -> Result<()> {
        let segment_count = segments.segment_count();
        if segment_count == 0 {
            info!(
                target: "lance::raft::log",
                "Bootstrap complete - no segments found"
            );
            return Ok(());
        }

        let all_segments = segments.segments_mut();

        // Split: sealed segments (all but last) get parallel build_index,
        // active segment (last) gets sequential build_index + validate_last_entry
        let (sealed, active_slice) = all_segments.split_at_mut(segment_count.saturating_sub(1));

        // Phase 1: Parallel index build for sealed segments (rayon par_iter_mut)
        sealed
            .par_iter_mut()
            .try_for_each(|segment| -> Result<()> {
                segment.build_index()?;
                debug!(
                    target: "lance::raft::log",
                    segment = %segment.path().display(),
                    entries = segment.entry_count(),
                    is_active = false,
                    "Bootstrapped sealed segment (parallel)"
                );
                Ok(())
            })?;

        // Phase 2: Sequential build_index + validate_last_entry for active segment
        if let Some(active) = active_slice.first_mut() {
            active.build_index()?;
            if active.is_active() {
                active.validate_last_entry()?;
            }
            debug!(
                target: "lance::raft::log",
                segment = %active.path().display(),
                entries = active.entry_count(),
                is_active = active.is_active(),
                "Bootstrapped active segment"
            );
        }

        let total_entries: usize = segments.total_entry_count();

        info!(
            target: "lance::raft::log",
            total_entries,
            segment_count,
            sealed_parallel = segment_count.saturating_sub(1),
            "Bootstrap complete - node ready for Raft cluster"
        );

        Ok(())
    }

    /// Load persistent state (term, voted_for)
    pub fn load_state(&self) -> Result<PersistentState> {
        if !self.state_path.exists() {
            return Ok(PersistentState::default());
        }

        let content = fs::read(&self.state_path)?;
        if content.len() < 16 {
            return Ok(PersistentState::default());
        }

        // SAFETY: Length check above guarantees these slices are exactly 8 bytes
        let term_bytes: [u8; 8] = content[0..8]
            .try_into()
            .map_err(|_| LanceError::InvalidData("Invalid state file format".into()))?;
        let voted_bytes: [u8; 8] = content[8..16]
            .try_into()
            .map_err(|_| LanceError::InvalidData("Invalid state file format".into()))?;

        let current_term = u64::from_le_bytes(term_bytes);
        let voted_for_raw = u64::from_le_bytes(voted_bytes);
        let voted_for = if voted_for_raw == u64::MAX {
            None
        } else {
            Some(voted_for_raw as u16)
        };

        Ok(PersistentState {
            current_term,
            voted_for,
        })
    }

    /// Save persistent state (must be called before responding to RPCs)
    pub fn save_state(&self, state: &PersistentState) -> Result<()> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&state.current_term.to_le_bytes());
        buf.extend_from_slice(
            &state
                .voted_for
                .map(|v| v as u64)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );

        // Write atomically via temp file
        let temp_path = self.state_path.with_extension("tmp");
        fs::write(&temp_path, &buf)?;
        fs::rename(&temp_path, &self.state_path)?;

        Ok(())
    }

    /// Get the last log index
    #[inline]
    pub fn last_index(&self) -> u64 {
        self.segments.last_index().unwrap_or(0)
    }

    /// Get the last log term
    #[inline]
    pub fn last_term(&self) -> u64 {
        if let Some(last_idx) = self.segments.last_index() {
            self.term_at(last_idx).unwrap_or(0)
        } else {
            0
        }
    }

    /// Get the first log index
    #[inline]
    pub fn first_index(&self) -> u64 {
        self.segments.first_index().unwrap_or(1)
    }

    /// Get a single entry by index (delegates to SegmentManager)
    ///
    /// **Segmented Architecture**: SegmentManager finds the correct segment and reads via pread.
    pub fn get(&self, index: u64) -> Option<LogEntry> {
        let entry_bytes = self.segments.get(index).ok()?;
        Self::decode_entry_from_bytes(Bytes::from(entry_bytes)).ok()
    }

    /// Decode a log entry from raw bytes with dual checksum validation
    ///
    /// **Security**: Validates header checksum before trusting data_len to prevent allocation attacks.
    ///
    /// **10X Zero-Copy**: Accepts `Bytes` and uses `Bytes::slice()` for the data field.
    /// This increments a reference count instead of copying, eliminating per-entry allocation
    /// when decoding from contiguous range reads.
    fn decode_entry_from_bytes(buf: Bytes) -> Result<LogEntry> {
        if buf.len() < ENTRY_HEADER_SIZE + HEADER_CRC_SIZE {
            return Err(LanceError::Protocol(
                "Buffer too small for entry header".into(),
            ));
        }

        // Parse header fields
        let term = u64::from_le_bytes(
            buf[0..8]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid term slice".into()))?,
        );
        let index = u64::from_le_bytes(
            buf[8..16]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid index slice".into()))?,
        );
        let hlc_raw = u64::from_le_bytes(
            buf[16..24]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid hlc slice".into()))?,
        );
        let entry_type_byte = buf[24];
        let data_len = u32::from_le_bytes(
            buf[25..29]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid data_len slice".into()))?,
        ) as usize;

        // Validate header checksum BEFORE trusting data_len
        let stored_header_crc = u32::from_le_bytes(
            buf[29..33]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid header CRC slice".into()))?,
        );
        let mut hasher = Hasher::new();
        hasher.update(&buf[0..29]); // Hash header only
        let calculated_header_crc = hasher.finalize();

        if calculated_header_crc != stored_header_crc {
            return Err(LanceError::Protocol(format!(
                "Header corruption detected at index {}: checksum mismatch (expected {:#x}, got {:#x})",
                index, stored_header_crc, calculated_header_crc
            )));
        }

        // Now we can trust data_len - validate buffer size
        let expected_len = ENTRY_HEADER_SIZE + HEADER_CRC_SIZE + data_len + DATA_CRC_SIZE;
        if buf.len() != expected_len {
            return Err(LanceError::Protocol(format!(
                "Buffer size mismatch: expected {}, got {}",
                expected_len,
                buf.len()
            )));
        }

        // Extract data and validate data checksum
        let data_start = ENTRY_HEADER_SIZE + HEADER_CRC_SIZE;
        let data_end = data_start + data_len;

        let stored_data_crc = u32::from_le_bytes(
            buf[data_end..data_end + 4]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid data CRC slice".into()))?,
        );
        let mut hasher = Hasher::new();
        hasher.update(&buf[data_start..data_end]);
        let calculated_data_crc = hasher.finalize();

        if calculated_data_crc != stored_data_crc {
            return Err(LanceError::Protocol(format!(
                "Data corruption detected at index {}: checksum mismatch (expected {:#x}, got {:#x})",
                index, stored_data_crc, calculated_data_crc
            )));
        }

        let hlc = HlcTimestamp::from_raw(hlc_raw);
        let entry_type = EntryType::from_u8(entry_type_byte);

        // Zero-copy: Bytes::slice() increments ref-count, NO allocation or copy
        let data = buf.slice(data_start..data_end);

        Ok(LogEntry {
            term,
            index,
            hlc,
            entry_type,
            data,
        })
    }

    /// Get term at index (if exists)
    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.get(index).map(|e| e.term)
    }

    /// Get entries from start_index to end (inclusive)
    ///
    /// **Segmented Architecture**: Delegates to SegmentManager which handles
    /// contiguous range optimization automatically.
    ///
    /// **10X Zero-Copy**: Uses Bytes::slice() to create entry views into the
    /// contiguous read buffer. For 1000 entries, this eliminates 1000 data copies.
    pub fn get_range(&self, start_index: u64, end_index: u64) -> Vec<LogEntry> {
        let range_data = match self.segments.get_range(start_index, end_index) {
            Ok(data) => data,
            Err(_) => return Vec::new(),
        };

        let mut entries = Vec::new();
        for (bytes, slices) in range_data {
            for (offset, len) in slices {
                // Zero-copy: slice into the shared Bytes buffer (ref-count increment, no copy)
                let entry_buf = bytes.slice(offset..offset + len);
                if let Ok(entry) = Self::decode_entry_from_bytes(entry_buf) {
                    entries.push(entry);
                }
            }
        }
        entries
    }

    /// Append entries to the log (batched with single fdatasync)
    ///
    /// **Batching**: Writes all entries to the active segment, then calls persist()
    /// once for the entire batch. This amortizes fdatasync cost across N entries.
    ///
    /// **Rotation**: Automatically rotates to new segment when current is full.
    pub fn append(&mut self, entries: Vec<LogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        for entry in &entries {
            // Check for rotation before each append
            self.segments.maybe_rotate(entry.index)?;

            // Get active segment
            let active = self.segments.get_or_create_active_segment(entry.index)?;

            // Serialize entry with dual checksums
            let entry_bytes = Self::serialize_entry(entry)?;

            // Append to segment (buffered, no sync yet)
            active.append(entry, &entry_bytes)?;
        }

        // Single fdatasync for entire batch (Raft durability requirement)
        self.segments.persist()?;

        debug!(
            target: "lance::raft::log",
            last_index = self.last_index(),
            entries_count = entries.len(),
            "Appended batch to segmented log"
        );

        Ok(())
    }

    /// Serialize a log entry with dual checksums
    fn serialize_entry(entry: &LogEntry) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(
            ENTRY_HEADER_SIZE + HEADER_CRC_SIZE + entry.data.len() + DATA_CRC_SIZE,
        );

        // Write header fields
        buf.extend_from_slice(&entry.term.to_le_bytes());
        buf.extend_from_slice(&entry.index.to_le_bytes());
        buf.extend_from_slice(&entry.hlc.as_u64().to_le_bytes());
        buf.push(entry.entry_type.as_u8());
        buf.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());

        // Calculate and write header CRC
        let mut hasher = Hasher::new();
        hasher.update(&buf);
        let header_crc = hasher.finalize();
        buf.extend_from_slice(&header_crc.to_le_bytes());

        // Write data
        buf.extend_from_slice(&entry.data);

        // Calculate and write data CRC
        let mut hasher = Hasher::new();
        hasher.update(&entry.data);
        let data_crc = hasher.finalize();
        buf.extend_from_slice(&data_crc.to_le_bytes());

        Ok(buf)
    }

    /// Truncate log from index onwards (for conflict resolution)
    ///
    /// **O(1) Segmented Architecture**: Deletes segments instead of rewriting.
    /// No IO spikes, no election timeouts.
    #[track_caller]
    pub fn truncate_from(&mut self, from_index: u64) -> Result<()> {
        let first_index = self.first_index();
        if from_index < first_index {
            return Err(LanceError::Protocol(format!(
                "Cannot truncate before first_index {}",
                first_index
            )));
        }

        // O(1) segment deletion
        self.segments.truncate_from(from_index)?;

        warn!(
            target: "lance::raft::log",
            from_index,
            new_last_index = self.last_index(),
            caller = %std::panic::Location::caller(),
            "Truncated log via O(1) segment deletion"
        );

        Ok(())
    }

    /// Compact log up to index (for snapshotting)
    ///
    /// **O(1) Segmented Architecture**: Deletes old segment files via fs::remove_file().
    /// 1GB log compaction = single unlink() syscall, not 1GB rewrite.
    pub fn compact_to(&mut self, to_index: u64) -> Result<()> {
        let first_index = self.first_index();
        if to_index < first_index {
            return Ok(()); // Already compacted
        }

        // Save snapshot metadata before compaction
        if let Some(entry) = self.get(to_index) {
            self.snapshot_meta = SnapshotMeta {
                last_included_index: to_index,
                last_included_term: entry.term,
            };

            // Persist snapshot metadata to prevent first_index drift on restart
            let meta_path = self.dir.join("raft.meta");
            Self::save_snapshot_meta(&meta_path, &self.snapshot_meta)?;
        }

        // O(1) compaction via segment deletion
        self.segments.compact_to(to_index + 1)?;

        info!(
            target: "lance::raft::log",
            new_first_index = self.first_index(),
            snapshot_index = self.snapshot_meta.last_included_index,
            snapshot_term = self.snapshot_meta.last_included_term,
            segments_remaining = self.segments.segment_count(),
            "Compacted log via O(1) segment deletion"
        );

        Ok(())
    }

    /// Check if automatic compaction should be triggered
    pub fn should_compact(&self) -> bool {
        self.compaction_config.enabled
            && self.segments.total_entry_count() > self.compaction_config.max_entries
    }

    /// Get the recommended compaction index based on config
    pub fn recommended_compact_index(&self) -> Option<u64> {
        if !self.should_compact() {
            return None;
        }

        let total_entries = self.segments.total_entry_count();
        let entries_to_remove =
            total_entries.saturating_sub(self.compaction_config.min_entries_retained);
        if entries_to_remove == 0 {
            return None;
        }

        Some(self.first_index() + entries_to_remove as u64 - 1)
    }

    /// Perform automatic compaction if needed
    pub fn maybe_compact(&mut self) -> Result<bool> {
        if let Some(compact_index) = self.recommended_compact_index() {
            self.compact_to(compact_index)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get snapshot metadata
    pub fn snapshot_meta(&self) -> &SnapshotMeta {
        &self.snapshot_meta
    }

    /// Set snapshot metadata (used when installing snapshot from leader)
    pub fn set_snapshot_meta(&mut self, meta: SnapshotMeta) {
        self.snapshot_meta = meta;

        // Compact segments up to snapshot index
        let first_index = self.first_index();
        if meta.last_included_index >= first_index {
            let _ = self.segments.compact_to(meta.last_included_index + 1);
        }

        // Persist metadata to disk
        let meta_path = self.dir.join("raft.meta");
        let _ = Self::save_snapshot_meta(&meta_path, &meta);
    }

    /// Load snapshot metadata from disk
    fn load_snapshot_meta(path: &Path) -> Result<SnapshotMeta> {
        if !path.exists() {
            return Ok(SnapshotMeta::default());
        }

        let mut file = File::open(path)?;
        let mut buf = [0u8; 16]; // 2 x u64
        file.read_exact(&mut buf)?;

        let last_included_index = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let last_included_term = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);

        Ok(SnapshotMeta {
            last_included_index,
            last_included_term,
        })
    }

    /// Save snapshot metadata to disk
    fn save_snapshot_meta(path: &Path, meta: &SnapshotMeta) -> Result<()> {
        let mut file = File::create(path)?;
        file.write_all(&meta.last_included_index.to_le_bytes())?;
        file.write_all(&meta.last_included_term.to_le_bytes())?;
        file.sync_data()?;
        Ok(())
    }

    /// Get compaction configuration
    pub fn compaction_config(&self) -> &CompactionConfig {
        &self.compaction_config
    }

    /// Set compaction configuration
    pub fn set_compaction_config(&mut self, config: CompactionConfig) {
        self.compaction_config = config;
    }

    /// Get the number of entries in the log
    pub fn entry_count(&self) -> usize {
        self.segments.total_entry_count()
    }

    /// Check if log contains entry at index with given term
    pub fn matches(&self, index: u64, term: u64) -> bool {
        if index == 0 {
            return true; // Empty log always matches
        }
        self.term_at(index).map(|t| t == term).unwrap_or(false)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_entry(term: u64, index: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            term,
            index,
            hlc: HlcTimestamp::new(1000, 0),
            entry_type: EntryType::TopicOp,
            data: Bytes::copy_from_slice(data),
        }
    }

    #[test]
    fn test_log_store_open_empty() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path()).unwrap();

        assert_eq!(store.last_index(), 0);
        assert_eq!(store.last_term(), 0);
        assert_eq!(store.first_index(), 1);
    }

    #[test]
    fn test_log_store_append_and_get() {
        let dir = tempdir().unwrap();
        let mut store = LogStore::open(dir.path()).unwrap();

        let entries = vec![
            make_entry(1, 1, b"entry1"),
            make_entry(1, 2, b"entry2"),
            make_entry(2, 3, b"entry3"),
        ];

        store.append(entries).unwrap();

        assert_eq!(store.last_index(), 3);
        assert_eq!(store.last_term(), 2);
        assert_eq!(store.get(1).unwrap().data.as_ref(), b"entry1");
        assert_eq!(store.get(2).unwrap().data.as_ref(), b"entry2");
        assert_eq!(store.get(3).unwrap().data.as_ref(), b"entry3");
        assert!(store.get(4).is_none());
    }

    #[test]
    fn test_log_store_persistence() {
        let dir = tempdir().unwrap();

        // Write entries
        {
            let mut store = LogStore::open(dir.path()).unwrap();
            store
                .append(vec![
                    make_entry(1, 1, b"persistent1"),
                    make_entry(1, 2, b"persistent2"),
                ])
                .unwrap();
        }

        // Reopen and verify
        {
            let store = LogStore::open(dir.path()).unwrap();
            assert_eq!(store.last_index(), 2);
            assert_eq!(store.get(1).unwrap().data.as_ref(), b"persistent1");
            assert_eq!(store.get(2).unwrap().data.as_ref(), b"persistent2");
        }
    }

    #[test]
    fn test_log_store_truncate() {
        let dir = tempdir().unwrap();
        let mut store = LogStore::open(dir.path()).unwrap();

        store
            .append(vec![
                make_entry(1, 1, b"a"),
                make_entry(1, 2, b"b"),
                make_entry(1, 3, b"c"),
            ])
            .unwrap();

        store.truncate_from(2).unwrap();

        assert_eq!(store.last_index(), 1);
        assert!(store.get(2).is_none());
    }

    #[test]
    fn test_log_store_matches() {
        let dir = tempdir().unwrap();
        let mut store = LogStore::open(dir.path()).unwrap();

        store
            .append(vec![make_entry(1, 1, b"a"), make_entry(2, 2, b"b")])
            .unwrap();

        assert!(store.matches(0, 0)); // Empty matches
        assert!(store.matches(1, 1)); // Correct term
        assert!(!store.matches(1, 2)); // Wrong term
        assert!(store.matches(2, 2)); // Correct term
        assert!(!store.matches(3, 1)); // Index doesn't exist
    }

    #[test]
    fn test_persistent_state() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path()).unwrap();

        let state = PersistentState {
            current_term: 5,
            voted_for: Some(2),
        };

        store.save_state(&state).unwrap();

        let loaded = store.load_state().unwrap();
        assert_eq!(loaded.current_term, 5);
        assert_eq!(loaded.voted_for, Some(2));
    }

    #[test]
    fn test_persistent_state_none_voted() {
        let dir = tempdir().unwrap();
        let store = LogStore::open(dir.path()).unwrap();

        let state = PersistentState {
            current_term: 3,
            voted_for: None,
        };

        store.save_state(&state).unwrap();

        let loaded = store.load_state().unwrap();
        assert_eq!(loaded.current_term, 3);
        assert_eq!(loaded.voted_for, None);
    }
}
