use crate::backend::IoBackend;
use crate::fallback::Pwritev2Backend;
use bytes::Bytes;
use lnc_core::{LanceError, Result};
use lnc_metrics::time_io_sampled;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Default write-buffer capacity (4 MiB).
///
/// Writes are accumulated in this buffer before being flushed to the
/// underlying file, dramatically reducing the number of `write(2)` syscalls
/// on network-attached storage such as Ceph RBD.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;

#[cfg(target_os = "linux")]
use crate::uring::IoUringBackend;
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Open,
    Sealed,
    Archived,
}

/// Concrete direct I/O backend — monomorphized to avoid dynamic dispatch.
///
/// Per Engineering Standards §1: No dynamic dispatch on data plane.
/// This enum wraps the two known `IoBackend` implementations with static
/// dispatch via match arms instead of a vtable.
pub enum DirectBackend {
    #[cfg(target_os = "linux")]
    IoUring(Box<IoUringBackend>),
    Pwritev2(Pwritev2Backend),
}

impl DirectBackend {
    #[inline]
    fn write(&mut self, data: &[u8], offset: u64) -> Result<usize> {
        match self {
            #[cfg(target_os = "linux")]
            Self::IoUring(b) => b.write(data, offset),
            Self::Pwritev2(b) => b.write(data, offset),
        }
    }

    #[inline]
    fn fsync(&mut self) -> Result<()> {
        match self {
            #[cfg(target_os = "linux")]
            Self::IoUring(b) => b.fsync(),
            Self::Pwritev2(b) => b.fsync(),
        }
    }
}

/// I/O backend for `SegmentWriter`.
///
/// - **Buffered** (default): `BufWriter<File>` — cross-platform, sequential
///   writes with a 4 MiB user-space buffer.  Best for Ceph RBD and general
///   use.
/// - **Direct**: `DirectBackend` (`IoUringBackend` with `O_DIRECT`, or
///   `Pwritev2Backend`).  Uses positional writes and bypasses the page cache.
///   Best for local NVMe with io_uring.
enum WriteBackend {
    Buffered(BufWriter<File>),
    Direct(Box<DirectBackend>),
}

pub struct SegmentWriter {
    backend: WriteBackend,
    path: PathBuf,
    write_offset: u64,
    state: SegmentState,
    start_index: u64,
    record_count: u64,
}

impl SegmentWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            backend: WriteBackend::Buffered(BufWriter::with_capacity(
                DEFAULT_WRITE_BUFFER_SIZE,
                file,
            )),
            path: path.to_path_buf(),
            write_offset: 0,
            state: SegmentState::Open,
            start_index: 0,
            record_count: 0,
        })
    }

    pub fn create_with_index(path: &Path, start_index: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            backend: WriteBackend::Buffered(BufWriter::with_capacity(
                DEFAULT_WRITE_BUFFER_SIZE,
                file,
            )),
            path: path.to_path_buf(),
            write_offset: 0,
            state: SegmentState::Open,
            start_index,
            record_count: 0,
        })
    }

    /// Create a segment writer backed by a direct I/O backend (io_uring or pwritev2).
    ///
    /// The backend handles its own file opening (with `O_DIRECT` if
    /// appropriate), so we only need the path for metadata.
    pub fn create_with_backend(path: &Path, backend: DirectBackend) -> Result<Self> {
        Ok(Self {
            backend: WriteBackend::Direct(Box::new(backend)),
            path: path.to_path_buf(),
            write_offset: 0,
            state: SegmentState::Open,
            start_index: 0,
            record_count: 0,
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).open(path)?;

        let write_offset = file.seek(SeekFrom::End(0))?;

        Ok(Self {
            backend: WriteBackend::Buffered(BufWriter::with_capacity(
                DEFAULT_WRITE_BUFFER_SIZE,
                file,
            )),
            path: path.to_path_buf(),
            write_offset,
            state: SegmentState::Open,
            start_index: 0,
            record_count: 0,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<u64> {
        if self.state != SegmentState::Open {
            return Err(LanceError::Io(std::io::Error::other(
                "Segment is not open for writing",
            )));
        }

        let offset = self.write_offset;
        match &mut self.backend {
            WriteBackend::Buffered(bw) => {
                bw.write_all(data)?;
            },
            WriteBackend::Direct(backend) => {
                backend.write(data, offset)?;
            },
        }
        self.write_offset += data.len() as u64;
        Ok(offset)
    }

    pub fn write_batch(&mut self, data: &[u8]) -> Result<u64> {
        // Sampled I/O latency tracking - only ~1% overhead on hot path
        let _latency_timer = time_io_sampled();

        let offset = self.append(data)?;
        self.record_count += 1;
        Ok(offset)
    }

    /// Durable save: write data and flush to stable storage.
    ///
    /// This is the canonical write function that **must** be used by both L1
    /// and L3 ingestion paths.  Unlike `write_batch` (which only calls
    /// `write_all`), `save` follows the write with `sync_data` so the data
    /// is guaranteed to survive a process crash or pod kill.
    ///
    /// Returns the byte offset at which the batch was written.
    pub fn save(&mut self, data: &[u8]) -> Result<u64> {
        let _latency_timer = time_io_sampled();

        let offset = self.append(data)?;
        self.record_count += 1;
        match &mut self.backend {
            WriteBackend::Buffered(bw) => {
                bw.flush()?;
                bw.get_mut().sync_data()?;
            },
            WriteBackend::Direct(backend) => {
                backend.fsync()?;
            },
        }
        Ok(offset)
    }

    pub fn fsync(&mut self) -> Result<()> {
        self.sync()
    }

    pub fn sync(&mut self) -> Result<()> {
        match &mut self.backend {
            WriteBackend::Buffered(bw) => {
                bw.flush()?;
                bw.get_mut().sync_all()?;
            },
            WriteBackend::Direct(backend) => {
                backend.fsync()?;
            },
        }
        Ok(())
    }

    /// Flush the internal write buffer to the OS page cache without
    /// syncing to stable storage.  Useful when the caller will batch
    /// multiple flushes before a single `fsync`.
    ///
    /// For `Direct` backends this is a no-op (writes go straight to disk).
    pub fn flush_buffer(&mut self) -> Result<()> {
        if let WriteBackend::Buffered(bw) = &mut self.backend {
            bw.flush()?;
        }
        Ok(())
    }

    pub fn seal(&mut self) -> Result<()> {
        self.sync()?;
        self.state = SegmentState::Sealed;
        Ok(())
    }

    pub fn close(&mut self, end_timestamp: u64) -> Result<PathBuf> {
        self.seal()?;

        // Rename to closed segment format: {start_index}_{start_timestamp}-{end_timestamp}.lnc
        let new_path = rename_to_closed_segment(&self.path, end_timestamp)?;
        self.path = new_path.clone();

        Ok(new_path)
    }

    #[inline]
    #[must_use]
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    #[inline]
    #[must_use]
    pub fn current_offset(&self) -> u64 {
        self.write_offset
    }

    #[inline]
    #[must_use]
    pub fn state(&self) -> SegmentState {
        self.state
    }

    #[inline]
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    #[must_use]
    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    #[inline]
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Create a segment with a specific filename (leader-dictated).
    ///
    /// Used by followers in L3 mode to create segments with the exact name
    /// dictated by the leader, ensuring byte-identical segment layouts.
    pub fn create_named(dir: &Path, segment_name: &str) -> Result<Self> {
        let path = dir.join(segment_name);
        Self::create(&path)
    }

    /// Write data at a specific offset within the segment.
    ///
    /// Used by followers in L3 mode to write at the exact offset dictated
    /// by the leader. Validates that the offset matches the current write
    /// position to detect desynchronization.
    ///
    /// Returns `Ok(offset)` on success, or an error if the offset doesn't
    /// match the current write position or the segment is not open.
    pub fn write_at_offset(&mut self, offset: u64, data: &[u8]) -> Result<u64> {
        if self.state != SegmentState::Open {
            return Err(LanceError::Io(std::io::Error::other(
                "Segment is not open for writing",
            )));
        }

        if offset != self.write_offset {
            return Err(LanceError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Write offset mismatch: expected {}, got {} (segment: {})",
                    self.write_offset,
                    offset,
                    self.path.display()
                ),
            )));
        }

        match &mut self.backend {
            WriteBackend::Buffered(bw) => {
                bw.write_all(data)?;
            },
            WriteBackend::Direct(backend) => {
                backend.write(data, offset)?;
            },
        }
        self.write_offset += data.len() as u64;
        self.record_count += 1;
        Ok(offset)
    }

    /// Durable save at a specific offset: write + sync_data.
    ///
    /// Follower counterpart of [`Self::save`].  Same durability guarantee —
    /// data survives process crash — but validates the offset matches
    /// the leader-dictated position first.
    pub fn save_at_offset(&mut self, offset: u64, data: &[u8]) -> Result<u64> {
        let result = self.write_at_offset(offset, data)?;
        match &mut self.backend {
            WriteBackend::Buffered(bw) => {
                bw.flush()?;
                bw.get_mut().sync_data()?;
            },
            WriteBackend::Direct(backend) => {
                backend.fsync()?;
            },
        }
        Ok(result)
    }

    /// Get the segment filename (without directory path).
    #[inline]
    #[must_use]
    pub fn filename(&self) -> Option<&str> {
        self.path.file_name().and_then(|f| f.to_str())
    }
}

pub struct SegmentReader {
    file: File,
    path: PathBuf,
    size: u64,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();

        Ok(Self {
            file,
            path: path.to_path_buf(),
            size,
        })
    }

    pub fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.file.seek(SeekFrom::Start(offset))?;
        let bytes_read = self.file.read(buf)?;
        Ok(bytes_read)
    }

    pub fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Zero-copy segment reader using memory mapping (Linux only with mmap)
/// Falls back to regular reads on other platforms
pub struct ZeroCopyReader {
    #[cfg(target_os = "linux")]
    mmap: Option<Arc<MmapHandle>>,
    #[cfg(not(target_os = "linux"))]
    file: File,
    path: PathBuf,
    size: u64,
}

/// Internal mmap handle - wrapped in Arc for sharing
#[cfg(target_os = "linux")]
pub(crate) struct MmapHandle {
    ptr: *mut u8,
    len: usize,
}

/// A zero-copy slice backed by Arc<MmapHandle>
/// This allows creating Bytes without copying by sharing the mmap reference
#[cfg(target_os = "linux")]
pub struct SharedMmapSlice {
    mmap: Arc<MmapHandle>,
    offset: usize,
    len: usize,
}

#[cfg(target_os = "linux")]
impl SharedMmapSlice {
    fn new(mmap: Arc<MmapHandle>, offset: usize, len: usize) -> Self {
        Self { mmap, offset, len }
    }
}

#[cfg(target_os = "linux")]
impl AsRef<[u8]> for SharedMmapSlice {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: offset and len are validated at construction time
        unsafe { std::slice::from_raw_parts(self.mmap.ptr.add(self.offset), self.len) }
    }
}

#[cfg(target_os = "linux")]
impl MmapHandle {
    fn new(file: &File, size: usize) -> Result<Self> {
        use std::os::unix::io::AsRawFd;

        // SAFETY: mmap with valid file descriptor creates read-only private mapping
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(LanceError::Io(std::io::Error::last_os_error()));
        }

        // SAFETY: ptr is valid from successful mmap, madvise is advisory only
        unsafe {
            libc::madvise(ptr, size, libc::MADV_SEQUENTIAL);
        }

        Ok(Self {
            ptr: ptr as *mut u8,
            len: size,
        })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid from mmap, len matches mapped size
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[cfg(target_os = "linux")]
impl Drop for MmapHandle {
    fn drop(&mut self) {
        // SAFETY: ptr and len are from the original mmap call
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

#[cfg(target_os = "linux")]
unsafe impl Send for MmapHandle {}
#[cfg(target_os = "linux")]
unsafe impl Sync for MmapHandle {}

impl ZeroCopyReader {
    /// Open a segment for zero-copy reading
    #[cfg(target_os = "linux")]
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();

        let mmap = if size > 0 {
            Some(Arc::new(MmapHandle::new(&file, size as usize)?))
        } else {
            None
        };

        Ok(Self {
            mmap,
            path: path.to_path_buf(),
            size,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();

        Ok(Self {
            file,
            path: path.to_path_buf(),
            size,
        })
    }

    /// Get a zero-copy slice of the segment data
    /// Returns None if offset/length is out of bounds
    #[cfg(target_os = "linux")]
    pub fn slice(&self, offset: u64, length: usize) -> Option<&[u8]> {
        let mmap = self.mmap.as_ref()?;
        let start = offset as usize;
        let end = start.checked_add(length)?;

        if end > mmap.len() {
            return None;
        }

        Some(&mmap.as_slice()[start..end])
    }

    #[cfg(not(target_os = "linux"))]
    pub fn slice(&self, _offset: u64, _length: usize) -> Option<&[u8]> {
        // Not available without mmap
        None
    }

    /// Get a zero-copy Bytes backed by the mmap
    /// This is the true zero-copy path - no data is copied, only Arc refcount incremented
    #[cfg(target_os = "linux")]
    pub fn slice_bytes(&self, offset: u64, length: usize) -> Option<Bytes> {
        let mmap = self.mmap.as_ref()?;
        let start = offset as usize;
        let end = start.checked_add(length)?;

        if end > mmap.len() {
            return None;
        }

        // Create SharedMmapSlice which holds Arc<MmapHandle>
        // Bytes::from_owner takes ownership and keeps the mmap alive
        let shared = SharedMmapSlice::new(Arc::clone(mmap), start, length);
        Some(Bytes::from_owner(shared))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn slice_bytes(&self, _offset: u64, _length: usize) -> Option<Bytes> {
        // Not available without mmap
        None
    }

    /// Read into a buffer (fallback for non-mmap platforms)
    #[cfg(target_os = "linux")]
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if let Some(slice) = self.slice(offset, buf.len()) {
            buf.copy_from_slice(slice);
            Ok(buf.len())
        } else {
            Err(LanceError::IndexOutOfBounds(format!(
                "offset {} + len {} exceeds size {}",
                offset,
                buf.len(),
                self.size
            )))
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.file.seek(SeekFrom::Start(offset))?;
        let bytes_read = self.file.read(buf)?;
        Ok(bytes_read)
    }

    /// Check if zero-copy reads are available
    #[cfg(target_os = "linux")]
    pub fn supports_zero_copy(&self) -> bool {
        self.mmap.is_some()
    }

    #[cfg(not(target_os = "linux"))]
    pub fn supports_zero_copy(&self) -> bool {
        false
    }

    #[inline]
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A borrowed slice from a zero-copy read
/// This is a view into memory-mapped data
pub struct BorrowedSlice<'a> {
    data: &'a [u8],
    offset: u64,
}

impl<'a> BorrowedSlice<'a> {
    pub fn new(data: &'a [u8], offset: u64) -> Self {
        Self { data, offset }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.data
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_write_read() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_segment.lnc");

        {
            let mut writer = SegmentWriter::create(&path).unwrap();
            let offset1 = writer.append(b"hello").unwrap();
            let offset2 = writer.append(b"world").unwrap();

            assert_eq!(offset1, 0);
            assert_eq!(offset2, 5);
            assert_eq!(writer.write_offset(), 10);

            writer.seal().unwrap();
        }

        {
            let mut reader = SegmentReader::open(&path).unwrap();
            assert_eq!(reader.size(), 10);

            let mut buf = [0u8; 5];
            reader.read_exact_at(0, &mut buf).unwrap();
            assert_eq!(&buf, b"hello");

            reader.read_exact_at(5, &mut buf).unwrap();
            assert_eq!(&buf, b"world");
        }

        std::fs::remove_file(&path).ok();
    }
}

// =============================================================================
// Segment Compaction
// =============================================================================

/// Configuration for segment compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of segments to trigger compaction
    pub min_segments: usize,
    /// Maximum size of a single compacted segment (bytes)
    pub max_segment_size: u64,
    /// Minimum segment size to consider for compaction (bytes)
    pub min_segment_size: u64,
    /// Whether to delete source segments after compaction
    pub delete_sources: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_segments: 4,
            max_segment_size: 1024 * 1024 * 1024, // 1GB
            min_segment_size: 1024 * 1024,        // 1MB
            delete_sources: true,
        }
    }
}

/// Metadata about a segment for compaction decisions
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    /// Path to the segment file
    pub path: PathBuf,
    /// Size in bytes
    pub size: u64,
    /// Start index of records in this segment
    pub start_index: u64,
    /// End index of records (exclusive)
    pub end_index: u64,
    /// Timestamp when segment was created
    pub created_at: u64,
}

/// Result of a compaction operation
#[derive(Debug)]
pub struct CompactionResult {
    /// Path to the new compacted segment
    pub output_path: PathBuf,
    /// Source segments that were merged
    pub source_segments: Vec<PathBuf>,
    /// Total bytes in the compacted segment
    pub total_bytes: u64,
    /// Number of records in the compacted segment
    pub record_count: u64,
}

/// Segment compactor for merging small segments
pub struct SegmentCompactor {
    config: CompactionConfig,
}

impl SegmentCompactor {
    /// Create a new segment compactor with the given configuration
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    /// Create a compactor with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CompactionConfig::default())
    }

    /// Find segments that are candidates for compaction
    pub fn find_candidates(&self, segments_dir: &Path) -> Result<Vec<SegmentMetadata>> {
        let mut candidates = Vec::new();

        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let path = entry.path();

            // Only consider .lnc files
            if path.extension().and_then(|e| e.to_str()) != Some("lnc") {
                continue;
            }

            let metadata = entry.metadata()?;
            let size = metadata.len();

            // Skip segments that are too large (already compacted)
            if size > self.config.max_segment_size {
                continue;
            }

            // Parse segment name for index info
            // Format: {start_index}_{start_timestamp_ns}.lnc or
            //         {start_index}_{start_timestamp_ns}-{end_timestamp_ns}.lnc
            let file_name = path.file_stem().and_then(|n| n.to_str()).unwrap_or("");

            let (start_index, created_at) = parse_segment_name(file_name);

            candidates.push(SegmentMetadata {
                path,
                size,
                start_index,
                end_index: start_index, // Will be updated during compaction
                created_at,
            });
        }

        // Sort by start_index for sequential merging
        candidates.sort_by_key(|s| s.start_index);

        Ok(candidates)
    }

    /// Check if compaction should be triggered
    pub fn should_compact(&self, candidates: &[SegmentMetadata]) -> bool {
        // Need at least min_segments small segments
        let small_segments = candidates
            .iter()
            .filter(|s| s.size < self.config.min_segment_size)
            .count();

        small_segments >= self.config.min_segments
    }

    /// Select segments to compact into a single segment
    pub fn select_segments_for_compaction<'a>(
        &self,
        candidates: &'a [SegmentMetadata],
    ) -> Vec<&'a SegmentMetadata> {
        let mut selected: Vec<&'a SegmentMetadata> = Vec::new();
        let mut total_size = 0u64;

        for segment in candidates {
            // Don't exceed max segment size
            if total_size + segment.size > self.config.max_segment_size {
                break;
            }

            // Prefer contiguous segments
            if let Some(last) = selected.last() {
                // Skip if there's a gap (non-contiguous)
                if segment.start_index > last.end_index + 1 {
                    continue;
                }
            }

            selected.push(segment);
            total_size += segment.size;

            // Stop if we have enough segments
            if selected.len() >= 16 {
                break;
            }
        }

        selected
    }

    /// Perform compaction of selected segments
    pub fn compact(
        &self,
        segments: &[&SegmentMetadata],
        output_dir: &Path,
    ) -> Result<CompactionResult> {
        if segments.is_empty() {
            return Err(LanceError::InvalidData("No segments to compact".into()));
        }

        // Determine output segment name
        // Safety: segments is non-empty (checked above)
        let first = &segments[0];
        let output_name = format!(
            "{}_{}-{}.lnc",
            first.start_index,
            first.created_at,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let output_path = output_dir.join(&output_name);

        // Create output segment
        let mut writer = SegmentWriter::create(&output_path)?;
        let mut total_bytes = 0u64;
        let mut record_count = 0u64;
        let mut source_segments = Vec::new();

        // Copy data from each source segment
        for segment in segments {
            let mut reader = SegmentReader::open(&segment.path)?;
            let segment_size = reader.size();

            // Read in chunks to avoid large allocations
            const CHUNK_SIZE: usize = 64 * 1024; // 64KB
            let mut offset = 0u64;
            let mut buf = vec![0u8; CHUNK_SIZE];

            while offset < segment_size {
                let to_read = std::cmp::min(CHUNK_SIZE, (segment_size - offset) as usize);
                let bytes_read = reader.read_at(offset, &mut buf[..to_read])?;

                if bytes_read == 0 {
                    break;
                }

                writer.append(&buf[..bytes_read])?;
                total_bytes += bytes_read as u64;
                offset += bytes_read as u64;
            }

            record_count += segment.end_index.saturating_sub(segment.start_index);
            source_segments.push(segment.path.clone());
        }

        // Seal the compacted segment
        writer.seal()?;

        // Delete source segments if configured
        if self.config.delete_sources {
            for path in &source_segments {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!(
                        target: "lance::compaction",
                        path = %path.display(),
                        error = %e,
                        "Failed to delete source segment"
                    );
                }
            }
        }

        tracing::info!(
            target: "lance::compaction",
            output = %output_path.display(),
            source_count = source_segments.len(),
            total_bytes,
            "Compaction completed"
        );

        Ok(CompactionResult {
            output_path,
            source_segments,
            total_bytes,
            record_count,
        })
    }

    /// Run automatic compaction if needed
    pub fn maybe_compact(&self, segments_dir: &Path) -> Result<Option<CompactionResult>> {
        let candidates = self.find_candidates(segments_dir)?;

        if !self.should_compact(&candidates) {
            return Ok(None);
        }

        let selected = self.select_segments_for_compaction(&candidates);

        if selected.len() < 2 {
            return Ok(None);
        }

        let result = self.compact(&selected, segments_dir)?;
        Ok(Some(result))
    }
}

/// Rename an active segment to closed segment format
/// Active: {start_index}_{start_timestamp}.lnc
/// Closed: {start_index}_{start_timestamp}-{end_timestamp}.lnc
fn rename_to_closed_segment(path: &Path, end_timestamp: u64) -> Result<PathBuf> {
    let filename = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| LanceError::Io(std::io::Error::other("Invalid segment filename")))?;

    // Check if already in closed format (contains '-')
    if filename.contains('-') {
        return Ok(path.to_path_buf());
    }

    // Build new filename: {original}-{end_timestamp}.lnc
    let new_filename = format!("{}-{}.lnc", filename, end_timestamp);
    let new_path = path.with_file_name(new_filename);

    std::fs::rename(path, &new_path)?;

    Ok(new_path)
}

/// Close any unclosed segments found during startup.
///
/// Recurses into subdirectories so that topic-partitioned layouts
/// (`segments/0/`, `segments/1/`, …) are handled correctly.
pub fn close_unclosed_segments(segments_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut closed = Vec::new();

    if !segments_dir.exists() {
        return Ok(closed);
    }

    for entry in std::fs::read_dir(segments_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Recurse into topic subdirectories
        if path.is_dir() {
            closed.extend(close_unclosed_segments(&path)?);
            continue;
        }

        if path.extension().is_some_and(|ext| ext == "lnc") {
            if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                // Active segments don't have '-' in the name
                if !filename.contains('-') {
                    // Use current timestamp as end timestamp for unclosed segments
                    let end_timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);

                    let new_path = rename_to_closed_segment(&path, end_timestamp)?;
                    closed.push(new_path);
                }
            }
        }
    }

    Ok(closed)
}

/// Parse segment name to extract start_index and timestamp
fn parse_segment_name(name: &str) -> (u64, u64) {
    // Format: {start_index}_{start_timestamp_ns} or
    //         {start_index}_{start_timestamp_ns}-{end_timestamp_ns}
    let parts: Vec<&str> = name.split('_').collect();
    if parts.len() >= 2 {
        let start_index = parts[0].parse().unwrap_or(0);
        let timestamp_part = parts[1].split('-').next().unwrap_or("0");
        let timestamp = timestamp_part.parse().unwrap_or(0);
        (start_index, timestamp)
    } else {
        (0, 0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod compaction_tests {
    use super::*;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.min_segments, 4);
        assert_eq!(config.max_segment_size, 1024 * 1024 * 1024);
        assert!(config.delete_sources);
    }

    #[test]
    fn test_parse_segment_name() {
        let (index, ts) = parse_segment_name("0_1234567890");
        assert_eq!(index, 0);
        assert_eq!(ts, 1234567890);

        let (index, ts) = parse_segment_name("100_1234567890-9876543210");
        assert_eq!(index, 100);
        assert_eq!(ts, 1234567890);
    }

    #[test]
    fn test_should_compact() {
        let config = CompactionConfig {
            min_segments: 2,
            min_segment_size: 1024 * 1024,
            ..Default::default()
        };
        let compactor = SegmentCompactor::new(config);

        let candidates = vec![
            SegmentMetadata {
                path: PathBuf::from("a.lnc"),
                size: 1000,
                start_index: 0,
                end_index: 10,
                created_at: 0,
            },
            SegmentMetadata {
                path: PathBuf::from("b.lnc"),
                size: 2000,
                start_index: 10,
                end_index: 20,
                created_at: 0,
            },
        ];

        assert!(compactor.should_compact(&candidates));
    }
}
