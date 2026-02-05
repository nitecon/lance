//! Persistent Raft log storage
//!
//! Provides durable storage for Raft log entries with the following guarantees:
//! - Entries are persisted before acknowledgment
//! - Log can be truncated for compaction
//! - Crash recovery restores committed state

use crate::codec::{EntryType, LogEntry};
use bytes::Bytes;
use lnc_core::{HlcTimestamp, LanceError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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

/// Header for the log file
const LOG_MAGIC: &[u8; 4] = b"RAFT";
const LOG_VERSION: u32 = 1;

/// On-disk format for a log entry
/// [term: u64][index: u64][hlc_raw: u64][entry_type: u8][data_len: u32][data: bytes]
const ENTRY_HEADER_SIZE: usize = 8 + 8 + 8 + 1 + 4; // 29 bytes

/// Persistent Raft log store
pub struct LogStore {
    /// Directory for log files
    dir: PathBuf,
    /// Current log file
    log_file: Option<File>,
    /// In-memory log entries (loaded on startup)
    entries: Vec<LogEntry>,
    /// First index in log (after compaction)
    first_index: u64,
    /// Persistent state file path
    state_path: PathBuf,
    /// Snapshot metadata (last compacted entry)
    snapshot_meta: SnapshotMeta,
    /// Compaction configuration
    compaction_config: CompactionConfig,
}

impl LogStore {
    /// Create or open a log store at the given directory
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let log_path = dir.join("raft.log");
        let state_path = dir.join("raft.state");

        let mut store = Self {
            dir: dir.to_path_buf(),
            log_file: None,
            entries: Vec::new(),
            first_index: 1,
            state_path,
            snapshot_meta: SnapshotMeta::default(),
            compaction_config: CompactionConfig::default(),
        };

        // Load existing log if present
        if log_path.exists() {
            store.load_log(&log_path)?;
        }

        // Open log file for appending
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&log_path)?;
        store.log_file = Some(file);

        // Write header if new file
        if store.entries.is_empty() {
            store.write_header()?;
        }

        info!(
            target: "lance::raft::log",
            dir = %dir.display(),
            entries = store.entries.len(),
            first_index = store.first_index,
            "Log store opened"
        );

        Ok(store)
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
        if self.entries.is_empty() {
            0
        } else {
            self.first_index + self.entries.len() as u64 - 1
        }
    }

    /// Get the last log term
    #[inline]
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Get the first log index
    #[inline]
    pub fn first_index(&self) -> u64 {
        self.first_index
    }

    /// Get entry at index (if exists)
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index < self.first_index {
            return None;
        }
        let offset = (index - self.first_index) as usize;
        self.entries.get(offset)
    }

    /// Get term at index (if exists)
    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.get(index).map(|e| e.term)
    }

    /// Get entries from start_index to end (inclusive)
    pub fn get_range(&self, start_index: u64, end_index: u64) -> Vec<LogEntry> {
        if start_index < self.first_index || start_index > end_index {
            return Vec::new();
        }

        let start_offset = (start_index - self.first_index) as usize;
        let end_offset = ((end_index - self.first_index) as usize).min(self.entries.len() - 1);

        self.entries[start_offset..=end_offset].to_vec()
    }

    /// Append entries to the log (persists immediately)
    pub fn append(&mut self, entries: Vec<LogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let file = self
            .log_file
            .as_mut()
            .ok_or_else(|| LanceError::Protocol("Log file not open".into()))?;

        // Seek to end
        file.seek(SeekFrom::End(0))?;

        let mut writer = BufWriter::new(file);

        for entry in &entries {
            Self::write_entry(&mut writer, entry)?;
        }

        writer.flush()?;

        // Sync to disk for durability
        let file = writer.into_inner().map_err(|e| {
            LanceError::Io(std::io::Error::other(format!(
                "Failed to flush writer: {}",
                e
            )))
        })?;
        file.sync_all()?;

        // Update in-memory state
        self.entries.extend(entries);

        debug!(
            target: "lance::raft::log",
            last_index = self.last_index(),
            "Appended entries to log"
        );

        Ok(())
    }

    /// Truncate log from index onwards (for conflict resolution)
    pub fn truncate_from(&mut self, from_index: u64) -> Result<()> {
        if from_index < self.first_index {
            return Err(LanceError::Protocol(format!(
                "Cannot truncate before first_index {}",
                self.first_index
            )));
        }

        let offset = (from_index - self.first_index) as usize;
        if offset >= self.entries.len() {
            return Ok(()); // Nothing to truncate
        }

        self.entries.truncate(offset);

        // Rewrite log file (simple approach - could be optimized)
        self.rewrite_log()?;

        warn!(
            target: "lance::raft::log",
            from_index,
            new_last_index = self.last_index(),
            "Truncated log"
        );

        Ok(())
    }

    /// Compact log up to index (for snapshotting)
    pub fn compact_to(&mut self, to_index: u64) -> Result<()> {
        if to_index < self.first_index {
            return Ok(()); // Already compacted
        }

        let offset = (to_index - self.first_index) as usize;
        if offset >= self.entries.len() {
            return Err(LanceError::Protocol(
                "Cannot compact beyond last entry".into(),
            ));
        }

        // Save snapshot metadata before compaction
        if let Some(entry) = self.get(to_index) {
            self.snapshot_meta = SnapshotMeta {
                last_included_index: to_index,
                last_included_term: entry.term,
            };
        }

        // Remove compacted entries
        self.entries.drain(0..=offset);
        self.first_index = to_index + 1;

        // Rewrite log file
        self.rewrite_log()?;

        info!(
            target: "lance::raft::log",
            new_first_index = self.first_index,
            snapshot_index = self.snapshot_meta.last_included_index,
            snapshot_term = self.snapshot_meta.last_included_term,
            "Compacted log"
        );

        Ok(())
    }

    /// Check if automatic compaction should be triggered
    pub fn should_compact(&self) -> bool {
        self.compaction_config.enabled && self.entries.len() > self.compaction_config.max_entries
    }

    /// Get the recommended compaction index based on config
    pub fn recommended_compact_index(&self) -> Option<u64> {
        if !self.should_compact() {
            return None;
        }

        let entries_to_remove = self.entries.len() - self.compaction_config.min_entries_retained;
        if entries_to_remove == 0 {
            return None;
        }

        Some(self.first_index + entries_to_remove as u64 - 1)
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
        // Update first_index if snapshot is newer
        if meta.last_included_index >= self.first_index {
            self.first_index = meta.last_included_index + 1;
            // Clear entries that are covered by the snapshot
            self.entries.retain(|e| e.index > meta.last_included_index);
        }
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
        self.entries.len()
    }

    /// Check if log contains entry at index with given term
    pub fn matches(&self, index: u64, term: u64) -> bool {
        if index == 0 {
            return true; // Empty log always matches
        }
        self.term_at(index).map(|t| t == term).unwrap_or(false)
    }

    /// Write log file header
    fn write_header(&mut self) -> Result<()> {
        let file = self
            .log_file
            .as_mut()
            .ok_or_else(|| LanceError::Protocol("Log file not open".into()))?;

        file.seek(SeekFrom::Start(0))?;
        file.write_all(LOG_MAGIC)?;
        file.write_all(&LOG_VERSION.to_le_bytes())?;
        file.write_all(&0u64.to_le_bytes())?; // entry count placeholder
        file.sync_all()?;

        Ok(())
    }

    /// Write a single entry to writer
    fn write_entry<W: Write>(writer: &mut W, entry: &LogEntry) -> Result<()> {
        writer.write_all(&entry.term.to_le_bytes())?;
        writer.write_all(&entry.index.to_le_bytes())?;
        writer.write_all(&entry.hlc.as_u64().to_le_bytes())?;
        writer.write_all(&[entry.entry_type.as_u8()])?;
        writer.write_all(&(entry.data.len() as u32).to_le_bytes())?;
        writer.write_all(&entry.data)?;
        Ok(())
    }

    /// Read a single entry from reader
    fn read_entry<R: Read>(reader: &mut R) -> Result<Option<LogEntry>> {
        let mut header = [0u8; ENTRY_HEADER_SIZE];
        match reader.read_exact(&mut header) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        // Parse header fields - array slices are guaranteed by ENTRY_HEADER_SIZE
        let term = u64::from_le_bytes([
            header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7],
        ]);
        let index = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]);
        let hlc_raw = u64::from_le_bytes([
            header[16], header[17], header[18], header[19], header[20], header[21], header[22],
            header[23],
        ]);
        let entry_type_byte = header[24];
        let data_len =
            u32::from_le_bytes([header[25], header[26], header[27], header[28]]) as usize;

        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        let hlc = HlcTimestamp::from_raw(hlc_raw);
        let entry_type = EntryType::from_u8(entry_type_byte);

        Ok(Some(LogEntry {
            term,
            index,
            hlc,
            entry_type,
            data: Bytes::from(data),
        }))
    }

    /// Load log from file
    fn load_log(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and validate header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != LOG_MAGIC {
            return Err(LanceError::Protocol("Invalid log file magic".into()));
        }

        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf)?;
        let version = u32::from_le_bytes(version_buf);
        if version != LOG_VERSION {
            return Err(LanceError::Protocol(format!(
                "Unsupported log version: {}",
                version
            )));
        }

        // Skip entry count (we'll count as we read)
        reader.seek(SeekFrom::Current(8))?;

        // Read entries
        while let Some(entry) = Self::read_entry(&mut reader)? {
            if self.entries.is_empty() {
                self.first_index = entry.index;
            }
            self.entries.push(entry);
        }

        Ok(())
    }

    /// Rewrite the entire log file (after truncation/compaction)
    fn rewrite_log(&mut self) -> Result<()> {
        let log_path = self.dir.join("raft.log");
        let temp_path = self.dir.join("raft.log.tmp");

        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            // Write header
            writer.write_all(LOG_MAGIC)?;
            writer.write_all(&LOG_VERSION.to_le_bytes())?;
            writer.write_all(&(self.entries.len() as u64).to_le_bytes())?;

            // Write entries
            for entry in &self.entries {
                Self::write_entry(&mut writer, entry)?;
            }

            writer.flush()?;
            writer
                .into_inner()
                .map_err(|e| {
                    LanceError::Io(std::io::Error::other(format!(
                        "Failed to flush writer: {}",
                        e
                    )))
                })?
                .sync_all()?;
        }

        // Atomic rename
        fs::rename(&temp_path, &log_path)?;

        // Reopen log file
        let file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        self.log_file = Some(file);

        Ok(())
    }
}

#[cfg(test)]
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
