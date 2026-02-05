use lnc_core::{Result, SortKey};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;
use zerocopy::{FromBytes, IntoBytes};

#[repr(C)]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes)]
pub struct IndexEntry {
    pub sort_key: SortKey,
    pub byte_offset: u64,
}

impl IndexEntry {
    #[inline]
    #[must_use]
    pub const fn new(sort_key: SortKey, byte_offset: u64) -> Self {
        Self {
            sort_key,
            byte_offset,
        }
    }

    #[inline]
    #[must_use]
    pub const fn size() -> usize {
        std::mem::size_of::<Self>()
    }
}

pub struct SparseIndex {
    mmap: Mmap,
    entry_count: usize,
}

impl SparseIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let _metadata = file.metadata()?;

        // SAFETY: We're memory-mapping a file that contains IndexEntry data
        // written by LANCE with correct layout. The file is opened read-only.
        let mmap = unsafe { Mmap::map(&file)? };

        let entry_count = mmap.len() / IndexEntry::size();

        Ok(Self { mmap, entry_count })
    }

    pub fn entries(&self) -> &[IndexEntry] {
        if self.mmap.is_empty() {
            return &[];
        }

        // SAFETY: IndexEntry is #[repr(C)] with known size/alignment.
        // The file is written by LANCE with correct layout.
        // The lifetime is tied to the Mmap which outlives this slice.
        unsafe {
            std::slice::from_raw_parts(self.mmap.as_ptr() as *const IndexEntry, self.entry_count)
        }
    }

    pub fn binary_search_by_sort_key(
        &self,
        sort_key: &SortKey,
    ) -> std::result::Result<usize, usize> {
        self.entries()
            .binary_search_by(|entry| entry.sort_key.cmp(sort_key))
    }

    pub fn binary_search_by_timestamp(
        &self,
        timestamp_ns: u64,
    ) -> std::result::Result<usize, usize> {
        self.entries()
            .binary_search_by(|entry| entry.sort_key.timestamp_ns().cmp(&timestamp_ns))
    }

    pub fn find_offset_for_record(&self, record_index: u64, interval: u64) -> Option<u64> {
        let index_position = (record_index / interval) as usize;

        if index_position < self.entry_count {
            Some(self.entries()[index_position].byte_offset)
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn first(&self) -> Option<&IndexEntry> {
        self.entries().first()
    }

    pub fn last(&self) -> Option<&IndexEntry> {
        self.entries().last()
    }
}

pub struct SparseIndexWriter {
    entries: Vec<IndexEntry>,
    interval: u64,
    record_count: u64,
}

impl SparseIndexWriter {
    pub fn new(interval: u64) -> Self {
        Self {
            entries: Vec::new(),
            interval,
            record_count: 0,
        }
    }

    pub fn maybe_add_entry(&mut self, sort_key: SortKey, byte_offset: u64) -> bool {
        if self.record_count % self.interval == 0 {
            self.entries.push(IndexEntry::new(sort_key, byte_offset));
            self.record_count += 1;
            true
        } else {
            self.record_count += 1;
            false
        }
    }

    pub fn add_entry(&mut self, sort_key: SortKey, byte_offset: u64) {
        self.entries.push(IndexEntry::new(sort_key, byte_offset));
    }

    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        use std::io::Write;

        let mut file = File::create(path)?;
        // Safety: IndexEntry is repr(C) and IntoBytes, so this cast is valid
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                self.entries.as_ptr() as *const u8,
                self.entries.len() * std::mem::size_of::<IndexEntry>(),
            )
        };
        file.write_all(bytes)?;
        file.sync_all()?;

        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.record_count = 0;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sparse_index_write_and_read() {
        use lnc_core::HlcTimestamp;

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.idx");

        // Use HlcTimestamp directly for precise control
        let mut writer = SparseIndexWriter::new(1);
        writer.add_entry(SortKey::new(HlcTimestamp::new(1000, 0), 0, 0), 0);
        writer.add_entry(SortKey::new(HlcTimestamp::new(2000, 0), 0, 1), 100);
        writer.add_entry(SortKey::new(HlcTimestamp::new(3000, 0), 0, 2), 200);
        writer.write_to_file(&path).unwrap();

        let index = SparseIndex::open(&path).unwrap();
        assert_eq!(index.entry_count(), 3);

        let entries = index.entries();
        // timestamp_ns() returns physical_ms * 1_000_000
        assert_eq!(entries[0].sort_key.hlc().physical_ms(), 1000);
        assert_eq!(entries[0].byte_offset, 0);
        assert_eq!(entries[2].sort_key.hlc().physical_ms(), 3000);
        assert_eq!(entries[2].byte_offset, 200);
    }

    #[test]
    fn test_binary_search() {
        use lnc_core::HlcTimestamp;

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.idx");

        let mut writer = SparseIndexWriter::new(1);
        for i in 0..100u64 {
            // Use milliseconds directly via HlcTimestamp
            // physical_ms = i * 1000, so to_nanos_approx = i * 1000 * 1_000_000
            writer.add_entry(SortKey::new(HlcTimestamp::new(i * 1000, 0), 0, i), i * 100);
        }
        writer.write_to_file(&path).unwrap();

        let index = SparseIndex::open(&path).unwrap();

        // Search by nanoseconds: i=50 means physical_ms=50_000, nanos=50_000_000_000
        let result = index.binary_search_by_timestamp(50_000_000_000);
        assert_eq!(result, Ok(50));

        // Non-exact match (between i=50 and i=51)
        let result = index.binary_search_by_timestamp(50_500_000_000);
        assert_eq!(result, Err(51));
    }

    #[test]
    fn test_interval_based_entries() {
        use lnc_core::HlcTimestamp;

        let mut writer = SparseIndexWriter::new(4);

        for i in 0..16u64 {
            writer.maybe_add_entry(SortKey::new(HlcTimestamp::new(i * 1000, 0), 0, i), i * 100);
        }

        assert_eq!(writer.entry_count(), 4);
    }
}
