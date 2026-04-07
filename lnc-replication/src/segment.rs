//! Segmented log storage for O(1) compaction
//!
//! Implements the segmented log architecture described in Architecture.md §2.
//! Log entries are split across multiple segment files named `{start_index}_{start_ts}.lnc`,
//! enabling O(1) compaction via file deletion instead of O(N) rewrite.

use crate::codec::LogEntry;
use bytes::Bytes;
use lnc_core::{LanceError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Zero-copy range result: a contiguous `Bytes` buffer paired with `(offset, len)` slices.
pub type RangeReadResult = (Bytes, Vec<(usize, usize)>);

/// Metadata for a log entry within a segment
#[derive(Debug, Clone, Copy)]
pub struct EntryIndex {
    /// Entry term
    #[allow(dead_code)]
    pub term: u64,
    /// File offset where entry starts (relative to segment file)
    pub offset: u64,
    /// Entry length in bytes (including checksums)
    pub len: u32,
}

/// A single segment file containing a contiguous range of log entries
#[derive(Debug)]
pub struct Segment {
    /// Buffered writer for appends (reused across all writes)
    writer: BufWriter<File>,
    /// Separate read-only handle for pread (no seeking contention)
    read_handle: File,
    /// Path to segment file
    path: PathBuf,
    /// First log index in this segment
    start_index: u64,
    /// Timestamp when segment was created (for file naming)
    start_ts: u64,
    /// In-memory index for entries in this segment
    index: Vec<EntryIndex>,
    /// Whether this segment is active (writable)
    is_active: bool,
    /// Current file size (tracked locally to avoid syscalls)
    file_size: u64,
}

impl Segment {
    /// Create a new active segment
    pub fn create(dir: &Path, start_index: u64) -> Result<Self> {
        let start_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| LanceError::Protocol(format!("System time error: {}", e)))?
            .as_millis() as u64;
        let filename = format!("{:06}_{}.lnc", start_index, start_ts);
        let path = dir.join(&filename);

        let write_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        // Separate read handle to avoid seeking contention
        let read_handle = OpenOptions::new().read(true).open(&path)?;

        info!(
            target: "lance::raft::segment",
            path = %path.display(),
            start_index,
            "Created new segment"
        );

        Ok(Self {
            writer: BufWriter::new(write_file),
            read_handle,
            path,
            start_index,
            start_ts,
            index: Vec::new(),
            is_active: true,
            file_size: 0,
        })
    }

    /// Open an existing segment file
    pub fn open(path: PathBuf) -> Result<Self> {
        // Parse filename: {start_index}_{start_ts}.lnc
        let filename = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| LanceError::Protocol("Invalid segment filename".into()))?;

        let parts: Vec<&str> = filename.split('_').collect();
        if parts.len() != 2 {
            return Err(LanceError::Protocol(format!(
                "Invalid segment filename format: {}",
                filename
            )));
        }

        let start_index = parts[0]
            .parse::<u64>()
            .map_err(|_| LanceError::Protocol("Invalid start_index in filename".into()))?;
        let start_ts = parts[1]
            .parse::<u64>()
            .map_err(|_| LanceError::Protocol("Invalid start_ts in filename".into()))?;

        let write_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let read_handle = OpenOptions::new().read(true).open(&path)?;

        let file_size = write_file.metadata()?.len();

        debug!(
            target: "lance::raft::segment",
            path = %path.display(),
            start_index,
            file_size,
            "Opened existing segment"
        );

        Ok(Self {
            writer: BufWriter::new(write_file),
            read_handle,
            path,
            start_index,
            start_ts,
            index: Vec::new(),
            is_active: false,
            file_size,
        })
    }

    /// Get the first index in this segment
    #[inline]
    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    /// Get the last index in this segment (or None if empty)
    #[inline]
    pub fn end_index(&self) -> Option<u64> {
        if self.index.is_empty() {
            None
        } else {
            Some(self.start_index + self.index.len() as u64 - 1)
        }
    }

    /// Get the number of entries in this segment
    #[inline]
    pub fn entry_count(&self) -> usize {
        self.index.len()
    }

    /// Check if this segment is active (writable)
    #[inline]
    pub fn is_active(&self) -> bool {
        self.is_active
    }

    /// Mark this segment as sealed (read-only) and rename with end timestamp
    ///
    /// **Rsync-Safe**: Renames from `{start_index}_{start_ts}.lnc` to
    /// `{start_index}_{start_ts}-{end_ts}.lnc`. Files with `-` are immutable
    /// and safe to rsync while node is running.
    pub fn seal(&mut self) -> Result<()> {
        if self.is_active {
            // Ensure all data is persisted before sealing
            self.persist()?;
            self.is_active = false;

            // Rename file with end timestamp for rsync-safety
            let end_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| LanceError::Protocol(format!("System time error: {}", e)))?
                .as_millis() as u64;

            let old_path = self.path.clone();
            let filename = format!("{:06}_{}-{}.lnc", self.start_index, self.start_ts, end_ts);
            let new_path = old_path
                .parent()
                .ok_or_else(|| LanceError::Protocol("Invalid segment path".into()))?
                .join(filename);

            fs::rename(&old_path, &new_path)?;
            self.path = new_path;

            debug!(
                target: "lance::raft::segment",
                path = %self.path.display(),
                entries = self.index.len(),
                "Sealed segment with end timestamp"
            );
        }
        Ok(())
    }

    /// Get the segment file path
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append an entry to this segment (must be active)
    ///
    /// **Performance**: Reuses BufWriter across all appends (no per-append allocation).
    /// **Durability**: Does NOT sync to disk. Call persist() to ensure durability.
    pub fn append(&mut self, entry: &LogEntry, entry_bytes: &[u8]) -> Result<()> {
        if !self.is_active {
            return Err(LanceError::Protocol(
                "Cannot append to sealed segment".into(),
            ));
        }

        let offset = self.file_size;

        // Write to buffered writer (no syscall yet)
        self.writer.write_all(entry_bytes)?;

        // Update index and file size
        self.index.push(EntryIndex {
            term: entry.term,
            offset,
            len: entry_bytes.len() as u32,
        });
        self.file_size += entry_bytes.len() as u64;

        Ok(())
    }

    /// Persist buffered writes to disk with fdatasync (Raft durability requirement)
    ///
    /// **Critical**: Must be called before acknowledging writes to Raft clients.
    /// flush() only moves data to OS page cache - power loss would lose data.
    /// sync_data() performs fdatasync to ensure physical write to disk.
    pub fn persist(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Read an entry by local offset within this segment
    #[cfg(unix)]
    pub fn read_entry(&self, local_offset: usize) -> Result<Vec<u8>> {
        let meta = self
            .index
            .get(local_offset)
            .ok_or_else(|| LanceError::Protocol("Entry not found in segment".into()))?;

        let mut buf = vec![0u8; meta.len as usize];
        self.read_handle.read_exact_at(&mut buf, meta.offset)?;
        Ok(buf)
    }

    #[cfg(windows)]
    pub fn read_entry(&self, local_offset: usize) -> Result<Vec<u8>> {
        let meta = self
            .index
            .get(local_offset)
            .ok_or_else(|| LanceError::Protocol("Entry not found in segment".into()))?;

        let mut buf = vec![0u8; meta.len as usize];
        self.read_handle.seek_read(&mut buf, meta.offset)?;
        Ok(buf)
    }

    /// Read a contiguous range of entries (returns single buffer with slice offsets)
    ///
    /// **Zero-Copy Optimization**: Returns Bytes instead of Vec<Vec<u8>> to avoid allocation storm.
    /// For 1000 entries, this eliminates 1000 heap allocations.
    #[cfg(unix)]
    pub fn read_range(&self, start_offset: usize, end_offset: usize) -> Result<RangeReadResult> {
        if start_offset > end_offset || end_offset >= self.index.len() {
            return Err(LanceError::Protocol("Invalid range".into()));
        }

        let first_meta = &self.index[start_offset];
        let last_meta = &self.index[end_offset];

        let file_offset = first_meta.offset;
        let total_len = (last_meta.offset + last_meta.len as u64 - first_meta.offset) as usize;

        // Single large pread into Bytes
        let mut buf = vec![0u8; total_len];
        self.read_handle.read_exact_at(&mut buf, file_offset)?;
        let bytes = Bytes::from(buf);

        // Return slice offsets instead of copying
        let mut slices = Vec::with_capacity(end_offset - start_offset + 1);
        let mut buf_offset = 0usize;

        for i in start_offset..=end_offset {
            let entry_len = self.index[i].len as usize;
            slices.push((buf_offset, entry_len));
            buf_offset += entry_len;
        }

        Ok((bytes, slices))
    }

    #[cfg(windows)]
    pub fn read_range(&self, start_offset: usize, end_offset: usize) -> Result<RangeReadResult> {
        if start_offset > end_offset || end_offset >= self.index.len() {
            return Err(LanceError::Protocol("Invalid range".into()));
        }

        let first_meta = &self.index[start_offset];
        let last_meta = &self.index[end_offset];

        let file_offset = first_meta.offset;
        let total_len = (last_meta.offset + last_meta.len as u64 - first_meta.offset) as usize;

        let mut buf = vec![0u8; total_len];
        self.read_handle.seek_read(&mut buf, file_offset)?;
        let bytes = Bytes::from(buf);

        let mut slices = Vec::with_capacity(end_offset - start_offset + 1);
        let mut buf_offset = 0usize;

        for i in start_offset..=end_offset {
            let entry_len = self.index[i].len as usize;
            slices.push((buf_offset, entry_len));
            buf_offset += entry_len;
        }

        Ok((bytes, slices))
    }

    /// Delete this segment file
    pub fn delete(self) -> Result<()> {
        let path = self.path.clone();
        drop(self.writer); // Close write handle
        drop(self.read_handle); // Close read handle
        fs::remove_file(&path)?;
        info!(
            target: "lance::raft::segment",
            path = %path.display(),
            "Deleted segment"
        );
        Ok(())
    }

    /// Validate the very last entry in an active segment
    ///
    /// **Critical for rsync**: Nodes seeded via rsync may have torn writes.
    /// This validates the data CRC of the final entry to detect corruption.
    pub fn validate_last_entry(&mut self) -> Result<()> {
        if !self.is_active || self.index.is_empty() {
            return Ok(());
        }

        let last_idx = self.index.len() - 1;
        let entry_bytes = self.read_entry(last_idx)?;

        // Attempt to decode - this validates both header and data CRCs
        match Self::decode_entry_from_bytes(&entry_bytes) {
            Ok(_) => Ok(()),
            Err(e) => {
                // Torn write detected - truncate at this entry
                warn!(
                    target: "lance::raft::segment",
                    path = %self.path.display(),
                    error = %e,
                    "Torn write detected in last entry - truncating"
                );

                let truncate_offset = if last_idx > 0 {
                    let prev_meta = &self.index[last_idx - 1];
                    prev_meta.offset + prev_meta.len as u64
                } else {
                    0
                };

                self.truncate_at(truncate_offset)?;
                self.index.pop(); // Remove corrupted entry from index
                Ok(())
            },
        }
    }

    /// Decode entry from bytes (used for validation)
    fn decode_entry_from_bytes(buf: &[u8]) -> Result<()> {
        use crc32fast::Hasher;

        const ENTRY_HEADER_SIZE: usize = 29;
        const HEADER_CRC_SIZE: usize = 4;
        const DATA_CRC_SIZE: usize = 4;

        if buf.len() < ENTRY_HEADER_SIZE + HEADER_CRC_SIZE {
            return Err(LanceError::Protocol("Buffer too small".into()));
        }

        // Validate header CRC
        let stored_header_crc = u32::from_le_bytes(
            buf[29..33]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid header CRC slice".into()))?,
        );
        let mut hasher = Hasher::new();
        hasher.update(&buf[0..29]);
        let calculated_header_crc = hasher.finalize();

        if calculated_header_crc != stored_header_crc {
            return Err(LanceError::Protocol("Header CRC mismatch".into()));
        }

        // Parse data_len from validated header
        let data_len = u32::from_le_bytes(
            buf[25..29]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid data_len slice".into()))?,
        ) as usize;
        let expected_len = ENTRY_HEADER_SIZE + HEADER_CRC_SIZE + data_len + DATA_CRC_SIZE;

        if buf.len() != expected_len {
            return Err(LanceError::Protocol("Buffer size mismatch".into()));
        }

        // Validate data CRC
        let data_start = ENTRY_HEADER_SIZE + HEADER_CRC_SIZE;
        let data_end = data_start + data_len;
        let data = &buf[data_start..data_end];

        let stored_data_crc = u32::from_le_bytes(
            buf[data_end..data_end + 4]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid data CRC slice".into()))?,
        );
        let mut hasher = Hasher::new();
        hasher.update(data);
        let calculated_data_crc = hasher.finalize();

        if calculated_data_crc != stored_data_crc {
            return Err(LanceError::Protocol("Data CRC mismatch".into()));
        }

        Ok(())
    }

    /// Build index by scanning segment file (cold startup recovery)
    ///
    /// **Torn Write Handling**: If a CRC mismatch is detected during scan,
    /// truncates the segment at that point. This handles crashes mid-append.
    ///
    /// **Performance**: Only reads headers (33 bytes) to reconstruct index,
    /// not full payloads.
    pub fn build_index(&mut self) -> Result<()> {
        use crc32fast::Hasher;

        self.index.clear();
        let mut current_offset = 0u64;
        let file_len = self.read_handle.metadata()?.len();
        let mut reader = BufReader::new(&self.read_handle);

        loop {
            // Read entry header (29 bytes)
            let mut header = [0u8; 29]; // ENTRY_HEADER_SIZE
            match reader.read_exact(&mut header) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file - normal termination
                    break;
                },
                Err(e) => return Err(e.into()),
            }

            // Parse header to get data_len
            let data_len =
                u32::from_le_bytes([header[25], header[26], header[27], header[28]]) as usize;

            // Read header CRC
            let mut header_crc_bytes = [0u8; 4];
            match reader.read_exact(&mut header_crc_bytes) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Torn write detected - truncate here
                    warn!(
                        target: "lance::raft::segment",
                        path = %self.path.display(),
                        offset = current_offset,
                        "Torn write detected during header CRC read - truncating segment"
                    );
                    self.truncate_at(current_offset)?;
                    break;
                },
                Err(e) => return Err(e.into()),
            }

            let stored_header_crc = u32::from_le_bytes(header_crc_bytes);

            // Validate header CRC
            let mut hasher = Hasher::new();
            hasher.update(&header);
            let calculated_header_crc = hasher.finalize();

            if calculated_header_crc != stored_header_crc {
                // Corruption detected - truncate segment at this point
                warn!(
                    target: "lance::raft::segment",
                    path = %self.path.display(),
                    offset = current_offset,
                    expected_crc = stored_header_crc,
                    actual_crc = calculated_header_crc,
                    "Header CRC mismatch - truncating segment"
                );
                self.truncate_at(current_offset)?;
                break;
            }

            // Extract term from header (first 8 bytes)
            let term = u64::from_le_bytes([
                header[0], header[1], header[2], header[3], header[4], header[5], header[6],
                header[7],
            ]);

            // Detect torn payload/data CRC region before indexing this entry.
            let entry_len = 29 + 4 + data_len + 4; // header + header_crc + data + data_crc
            if current_offset + entry_len as u64 > file_len {
                warn!(
                    target: "lance::raft::segment",
                    path = %self.path.display(),
                    offset = current_offset,
                    expected_entry_len = entry_len,
                    file_len,
                    "Torn write detected during data payload read - truncating segment"
                );
                self.truncate_at(current_offset)?;
                break;
            }

            // Skip data + data CRC (we don't need to validate data during index rebuild)
            let skip_len = data_len + 4; // data + data_crc
            reader.seek(SeekFrom::Current(skip_len as i64))?;

            // Add to index
            self.index.push(EntryIndex {
                term,
                offset: current_offset,
                len: entry_len as u32,
            });

            current_offset += entry_len as u64;
        }

        self.file_size = current_offset;

        info!(
            target: "lance::raft::segment",
            path = %self.path.display(),
            entries = self.index.len(),
            file_size = self.file_size,
            "Built index from segment file"
        );

        Ok(())
    }

    /// Truncate segment file at the given offset (handles torn writes)
    fn truncate_at(&mut self, offset: u64) -> Result<()> {
        // Flush any pending writes
        self.writer.flush()?;

        // Truncate the underlying file
        self.writer.get_ref().set_len(offset)?;
        self.writer.get_ref().sync_data()?;
        // After truncation, reset the file cursor to the new EOF so subsequent
        // appends continue from the truncated boundary instead of a stale offset.
        self.writer.get_mut().seek(SeekFrom::Start(offset))?;

        self.file_size = offset;

        info!(
            target: "lance::raft::segment",
            path = %self.path.display(),
            offset,
            "Truncated segment due to corruption"
        );

        Ok(())
    }
}

/// Configuration for segment management
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Maximum entries per segment before creating a new one
    pub max_entries_per_segment: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_entries_per_segment: 10_000,
        }
    }
}

/// Manages multiple log segments for O(1) compaction and truncation
#[derive(Debug)]
pub struct SegmentManager {
    /// Directory containing segment files
    dir: PathBuf,
    /// All segments (sealed + active), sorted by start_index
    segments: Vec<Segment>,
    /// Index of the active (writable) segment
    active_segment_idx: Option<usize>,
    /// Configuration
    config: SegmentConfig,
}

impl SegmentManager {
    /// Create a new SegmentManager and scan directory for existing segments
    pub fn open(dir: &Path, config: SegmentConfig) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let mut segments = Vec::new();

        // Scan directory for existing segment files
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("lnc") {
                    match Segment::open(path) {
                        Ok(segment) => segments.push(segment),
                        Err(e) => {
                            tracing::warn!(
                                target: "lance::raft::segment",
                                error = %e,
                                "Failed to open segment file"
                            );
                        },
                    }
                }
            }
        }

        // Sort segments by start_index
        segments.sort_by_key(|s| s.start_index());

        // Find active segment (last one if it exists)
        let active_segment_idx = if segments.is_empty() {
            None
        } else {
            let last_idx = segments.len() - 1;
            segments[last_idx].is_active = true;
            Some(last_idx)
        };

        info!(
            target: "lance::raft::segment",
            dir = %dir.display(),
            segment_count = segments.len(),
            "Opened segment manager"
        );

        Ok(Self {
            dir: dir.to_path_buf(),
            segments,
            active_segment_idx,
            config,
        })
    }

    /// Get or create the active segment
    pub fn get_or_create_active_segment(&mut self, next_index: u64) -> Result<&mut Segment> {
        // If no active segment, create one
        if self.active_segment_idx.is_none() {
            let segment = Segment::create(&self.dir, next_index)?;
            self.segments.push(segment);
            self.active_segment_idx = Some(self.segments.len() - 1);
        }

        let idx = self
            .active_segment_idx
            .ok_or_else(|| LanceError::Protocol("No active segment index".into()))?;
        Ok(&mut self.segments[idx])
    }

    /// Rotate to a new segment if the current one is full
    pub fn maybe_rotate(&mut self, next_index: u64) -> Result<bool> {
        if let Some(idx) = self.active_segment_idx {
            let entry_count = self.segments[idx].entry_count();

            // Check if segment should be rotated
            let should_rotate = entry_count >= self.config.max_entries_per_segment;

            if should_rotate {
                // Seal current segment (persists and marks read-only)
                self.segments[idx].seal()?;

                // Create new segment
                let new_segment = Segment::create(&self.dir, next_index)?;
                self.segments.push(new_segment);
                self.active_segment_idx = Some(self.segments.len() - 1);

                info!(
                    target: "lance::raft::segment",
                    old_segment_entries = entry_count,
                    new_segment_start = next_index,
                    "Rotated to new segment"
                );

                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Find segment containing the given index
    pub fn find_segment(&self, index: u64) -> Option<(usize, usize)> {
        for (seg_idx, segment) in self.segments.iter().enumerate() {
            if index >= segment.start_index() {
                if let Some(end_idx) = segment.end_index() {
                    if index <= end_idx {
                        let local_offset = (index - segment.start_index()) as usize;
                        return Some((seg_idx, local_offset));
                    }
                }
            }
        }
        None
    }

    /// Get entry by index
    pub fn get(&self, index: u64) -> Result<Vec<u8>> {
        let (seg_idx, local_offset) = self
            .find_segment(index)
            .ok_or_else(|| LanceError::Protocol("Entry not found".into()))?;

        self.segments[seg_idx].read_entry(local_offset)
    }

    /// Get range of entries (returns single buffer with slice offsets)
    ///
    /// **Zero-Copy**: Returns Bytes buffer with slice offsets to avoid allocation storm.
    pub fn get_range(&self, start_index: u64, end_index: u64) -> Result<Vec<RangeReadResult>> {
        if start_index > end_index {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();

        // Find segments that overlap with the range
        for segment in &self.segments {
            let seg_start = segment.start_index();
            let seg_end = segment.end_index().unwrap_or(seg_start);

            // Check if segment overlaps with requested range
            if seg_end >= start_index && seg_start <= end_index {
                let range_start = start_index.max(seg_start);
                let range_end = end_index.min(seg_end);

                let local_start = (range_start - seg_start) as usize;
                let local_end = (range_end - seg_start) as usize;

                let range_data = segment.read_range(local_start, local_end)?;
                result.push(range_data);
            }
        }

        Ok(result)
    }

    /// Persist all buffered writes in the active segment (Raft durability)
    pub fn persist(&mut self) -> Result<()> {
        if let Some(idx) = self.active_segment_idx {
            self.segments[idx].persist()?;
        }
        Ok(())
    }

    /// Compact segments up to the given index (O(1) via file deletion)
    pub fn compact_to(&mut self, to_index: u64) -> Result<()> {
        let mut segments_to_remove = Vec::new();

        // Find segments that are entirely before to_index
        for (idx, segment) in self.segments.iter().enumerate() {
            if let Some(end_idx) = segment.end_index() {
                if end_idx < to_index {
                    segments_to_remove.push(idx);
                }
            }
        }

        // Delete old segments (O(1) per segment)
        for idx in segments_to_remove.iter().rev() {
            let segment = self.segments.remove(*idx);
            segment.delete()?;

            // Adjust active segment index if needed
            if let Some(active_idx) = self.active_segment_idx {
                if active_idx > *idx {
                    self.active_segment_idx = Some(active_idx - 1);
                } else if active_idx == *idx {
                    self.active_segment_idx = None;
                }
            }
        }

        info!(
            target: "lance::raft::segment",
            segments_deleted = segments_to_remove.len(),
            to_index,
            "Compacted segments"
        );

        Ok(())
    }

    /// Truncate log from index onwards (precision truncation)
    ///
    /// **10X Refinement**: Uses precision truncation within segment containing
    /// the conflict, then deletes subsequent segments. This preserves as much
    /// data as possible instead of deleting entire segments.
    pub fn truncate_from(&mut self, from_index: u64) -> Result<()> {
        // Find segment containing from_index
        let mut truncate_segment_idx = None;
        let mut delete_from_idx = None;

        for (idx, segment) in self.segments.iter().enumerate() {
            let seg_start = segment.start_index();
            let seg_end = segment.end_index().unwrap_or(seg_start);

            if from_index >= seg_start && from_index <= seg_end {
                // Conflict is within this segment - use precision truncation
                truncate_segment_idx = Some(idx);
                delete_from_idx = Some(idx + 1);
                break;
            } else if seg_start >= from_index {
                // This segment starts after conflict - delete it and all subsequent
                delete_from_idx = Some(idx);
                break;
            }
        }

        // Precision truncate the segment containing the conflict
        if let Some(idx) = truncate_segment_idx {
            let segment = &mut self.segments[idx];
            let local_offset = (from_index - segment.start_index()) as usize;

            if local_offset < segment.entry_count() {
                // Calculate file offset for truncation
                let truncate_offset = if local_offset > 0 {
                    let meta = &segment.index[local_offset - 1];
                    meta.offset + meta.len as u64
                } else {
                    0
                };

                segment.truncate_at(truncate_offset)?;
                segment.index.truncate(local_offset);

                info!(
                    target: "lance::raft::segment",
                    segment_path = %segment.path.display(),
                    from_index,
                    entries_removed = segment.entry_count() - local_offset,
                    "Precision truncated segment"
                );
            }
        }

        // Delete all subsequent segments
        if let Some(start_idx) = delete_from_idx {
            let mut segments_to_remove = Vec::new();
            for idx in start_idx..self.segments.len() {
                segments_to_remove.push(idx);
            }

            for idx in segments_to_remove.into_iter().rev() {
                let segment = self.segments.remove(idx);
                segment.delete()?;

                // Update active segment index if we removed it
                if Some(idx) == self.active_segment_idx {
                    self.active_segment_idx = None;
                } else if let Some(active_idx) = self.active_segment_idx {
                    if idx < active_idx {
                        self.active_segment_idx = Some(active_idx - 1);
                    }
                }
            }
        }

        info!(
            target: "lance::raft::segment",
            from_index,
            remaining_segments = self.segments.len(),
            "Truncated log with precision"
        );

        Ok(())
    }

    /// Get the first index across all segments
    pub fn first_index(&self) -> Option<u64> {
        self.segments.first().map(|s| s.start_index())
    }

    /// Get the last index across all segments
    pub fn last_index(&self) -> Option<u64> {
        self.segments.last().and_then(|s| s.end_index())
    }

    /// Get total entry count across all segments
    pub fn total_entry_count(&self) -> usize {
        self.segments.iter().map(|s| s.entry_count()).sum()
    }

    /// Get the number of segments
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Get mutable iterator over segments (for bootstrap)
    pub fn segments_mut(&mut self) -> &mut [Segment] {
        &mut self.segments
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segment_filename_parsing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001_1234567890.lnc");
        std::fs::write(&path, b"test").unwrap();

        let segment = Segment::open(path).unwrap();
        assert_eq!(segment.start_index(), 1);
        assert_eq!(segment.start_ts, 1234567890);
    }

    #[test]
    fn test_segment_create_and_seal() {
        let dir = tempdir().unwrap();
        let mut segment = Segment::create(dir.path(), 1).unwrap();

        assert!(segment.is_active());
        assert_eq!(segment.start_index(), 1);
        assert_eq!(segment.entry_count(), 0);

        segment.seal().unwrap();
        assert!(!segment.is_active());
    }

    #[test]
    fn test_segment_manager_open_empty() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();
        let manager = SegmentManager::open(dir.path(), config).unwrap();

        assert_eq!(manager.segment_count(), 0);
        assert_eq!(manager.first_index(), None);
        assert_eq!(manager.last_index(), None);
        assert_eq!(manager.total_entry_count(), 0);
    }

    #[test]
    fn test_segment_manager_create_active_segment() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();
        let mut manager = SegmentManager::open(dir.path(), config).unwrap();

        let segment = manager.get_or_create_active_segment(1).unwrap();
        assert_eq!(segment.start_index(), 1);
        assert!(segment.is_active());
        assert_eq!(manager.segment_count(), 1);
    }

    #[test]
    fn test_segment_manager_rotation() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig {
            max_entries_per_segment: 5,
        };
        let mut manager = SegmentManager::open(dir.path(), config).unwrap();

        // Create first segment
        manager.get_or_create_active_segment(1).unwrap();

        // Should not rotate yet (0 entries < 5)
        let rotated = manager.maybe_rotate(1).unwrap();
        assert!(!rotated);
        assert_eq!(manager.segment_count(), 1);

        // Simulate adding 5 entries by manually updating the index
        if let Some(idx) = manager.active_segment_idx {
            for i in 0..5 {
                manager.segments[idx].index.push(EntryIndex {
                    term: 0,
                    offset: i * 100,
                    len: 100,
                });
            }
        }

        // Now should rotate
        let rotated = manager.maybe_rotate(6).unwrap();
        assert!(rotated);
        assert_eq!(manager.segment_count(), 2);

        // First segment should be sealed
        assert!(!manager.segments[0].is_active());
        // Second segment should be active
        assert!(manager.segments[1].is_active());
        assert_eq!(manager.segments[1].start_index(), 6);
    }

    #[test]
    fn test_segment_manager_compact_to() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();
        let mut manager = SegmentManager::open(dir.path(), config).unwrap();

        // Create three segments manually
        let seg1 = Segment::create(dir.path(), 1).unwrap();
        let seg2 = Segment::create(dir.path(), 100).unwrap();
        let seg3 = Segment::create(dir.path(), 200).unwrap();

        // Add mock entries to each segment
        manager.segments.push(seg1);
        manager.segments.push(seg2);
        manager.segments.push(seg3);

        // Add mock index entries
        for i in 0..10 {
            manager.segments[0].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }
        for i in 0..10 {
            manager.segments[1].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }
        for i in 0..10 {
            manager.segments[2].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }

        manager.active_segment_idx = Some(2);

        assert_eq!(manager.segment_count(), 3);

        // Compact up to index 15 (should delete first segment only, which ends at index 10)
        manager.compact_to(15).unwrap();

        assert_eq!(manager.segment_count(), 2);
        assert_eq!(manager.segments[0].start_index(), 100);
        assert_eq!(manager.segments[1].start_index(), 200);
    }

    #[test]
    fn test_segment_manager_truncate_from() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();
        let mut manager = SegmentManager::open(dir.path(), config).unwrap();

        // Create three segments
        let seg1 = Segment::create(dir.path(), 1).unwrap();
        let seg2 = Segment::create(dir.path(), 100).unwrap();
        let seg3 = Segment::create(dir.path(), 200).unwrap();

        manager.segments.push(seg1);
        manager.segments.push(seg2);
        manager.segments.push(seg3);

        // Add mock entries
        for i in 0..10 {
            manager.segments[0].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }
        for i in 0..10 {
            manager.segments[1].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }
        for i in 0..10 {
            manager.segments[2].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }

        manager.active_segment_idx = Some(2);

        assert_eq!(manager.segment_count(), 3);

        // Truncate from index 105 (precision truncation: truncates segment 2 at index 105, deletes segment 3)
        manager.truncate_from(105).unwrap();

        assert_eq!(manager.segment_count(), 2);
        assert_eq!(manager.segments[0].start_index(), 1);
        assert_eq!(manager.segments[1].start_index(), 100);
        // Segment 2 should have entries 100-104 (5 entries)
        assert_eq!(manager.segments[1].entry_count(), 5);
    }

    #[test]
    fn test_segment_manager_find_segment() {
        let dir = tempdir().unwrap();
        let config = SegmentConfig::default();
        let mut manager = SegmentManager::open(dir.path(), config).unwrap();

        // Create two segments
        let seg1 = Segment::create(dir.path(), 1).unwrap();
        let seg2 = Segment::create(dir.path(), 100).unwrap();

        manager.segments.push(seg1);
        manager.segments.push(seg2);

        // Add entries to first segment (indices 1-10)
        for i in 0..10 {
            manager.segments[0].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }

        // Add entries to second segment (indices 100-109)
        for i in 0..10 {
            manager.segments[1].index.push(EntryIndex {
                term: 0,
                offset: i * 100,
                len: 100,
            });
        }

        // Find entry in first segment
        let result = manager.find_segment(5);
        assert!(result.is_some());
        let (seg_idx, local_offset) = result.unwrap();
        assert_eq!(seg_idx, 0);
        assert_eq!(local_offset, 4); // index 5 - start_index 1 = offset 4

        // Find entry in second segment
        let result = manager.find_segment(105);
        assert!(result.is_some());
        let (seg_idx, local_offset) = result.unwrap();
        assert_eq!(seg_idx, 1);
        assert_eq!(local_offset, 5); // index 105 - start_index 100 = offset 5

        // Entry not found
        let result = manager.find_segment(500);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_index_truncates_partial_payload_entry() {
        use crc32fast::Hasher;

        let dir = tempdir().unwrap();
        let path = dir.path().join("000001_1234567890.lnc");

        // Build a single entry header that claims payload bytes, but omit payload+data_crc.
        let mut header = [0u8; 29];
        header[0..8].copy_from_slice(&1u64.to_le_bytes()); // term
        header[8..16].copy_from_slice(&1u64.to_le_bytes()); // index
        header[16..24].copy_from_slice(&0u64.to_le_bytes()); // hlc_raw
        header[24] = 1; // entry type
        header[25..29].copy_from_slice(&8u32.to_le_bytes()); // data_len (payload not present)

        let mut hasher = Hasher::new();
        hasher.update(&header);
        let header_crc = hasher.finalize();

        let mut torn_bytes = Vec::new();
        torn_bytes.extend_from_slice(&header);
        torn_bytes.extend_from_slice(&header_crc.to_le_bytes());
        std::fs::write(&path, torn_bytes).unwrap();

        let mut segment = Segment::open(path.clone()).unwrap();
        segment.build_index().unwrap();

        assert_eq!(segment.entry_count(), 0);
        assert_eq!(segment.file_size, 0);
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    }
}
