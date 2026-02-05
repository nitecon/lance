use lnc_core::{Result, TIMESTAMP_JITTER_WINDOW_NS};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;
use zerocopy::{FromBytes, IntoBytes};

/// Configuration for jitter window queries
#[derive(Debug, Clone, Copy)]
pub struct JitterWindowConfig {
    /// Window size in nanoseconds
    pub window_ns: u64,
    /// Whether to expand search range by jitter window
    pub expand_range: bool,
}

impl Default for JitterWindowConfig {
    fn default() -> Self {
        Self {
            window_ns: TIMESTAMP_JITTER_WINDOW_NS,
            expand_range: true,
        }
    }
}

impl JitterWindowConfig {
    /// Create a new jitter window config with custom window size
    pub fn with_window(window_ns: u64) -> Self {
        Self {
            window_ns,
            expand_range: true,
        }
    }

    /// Create a config with no jitter window expansion
    pub fn no_expansion() -> Self {
        Self {
            window_ns: 0,
            expand_range: false,
        }
    }

    /// Expand a timestamp range by the jitter window
    #[inline]
    pub fn expand_range_start(&self, ts: u64) -> u64 {
        if self.expand_range {
            ts.saturating_sub(self.window_ns)
        } else {
            ts
        }
    }

    /// Expand a timestamp range end by the jitter window
    #[inline]
    pub fn expand_range_end(&self, ts: u64) -> u64 {
        if self.expand_range {
            ts.saturating_add(self.window_ns)
        } else {
            ts
        }
    }
}

/// Result of a point-in-time query with jitter tolerance
#[derive(Debug, Clone)]
pub struct JitterQueryResult<'a> {
    /// Entries within the jitter window
    pub entries: Vec<&'a TimestampEntry>,
    /// The exact match entry (if any)
    pub exact_match: Option<&'a TimestampEntry>,
    /// Closest entry before target (if no exact match)
    pub closest_before: Option<&'a TimestampEntry>,
    /// Closest entry after target (if no exact match)
    pub closest_after: Option<&'a TimestampEntry>,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes)]
pub struct TimestampEntry {
    pub timestamp_ns: u64,
    pub byte_offset: u64,
}

impl TimestampEntry {
    #[inline]
    #[must_use]
    pub const fn new(timestamp_ns: u64, byte_offset: u64) -> Self {
        Self {
            timestamp_ns,
            byte_offset,
        }
    }

    #[inline]
    #[must_use]
    pub const fn size() -> usize {
        std::mem::size_of::<Self>()
    }
}

pub struct SecondaryIndex {
    mmap: Mmap,
    entry_count: usize,
}

impl SecondaryIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;

        // SAFETY: We're memory-mapping a file that contains TimestampEntry data
        // written by LANCE with correct layout. The file is opened read-only.
        let mmap = unsafe { Mmap::map(&file)? };

        let entry_count = mmap.len() / TimestampEntry::size();

        Ok(Self { mmap, entry_count })
    }

    pub fn entries(&self) -> &[TimestampEntry] {
        if self.mmap.is_empty() {
            return &[];
        }

        // SAFETY: TimestampEntry is #[repr(C)] with known size/alignment.
        // The file is written by LANCE with correct layout.
        // The lifetime is tied to the Mmap which outlives this slice.
        unsafe {
            std::slice::from_raw_parts(
                self.mmap.as_ptr() as *const TimestampEntry,
                self.entry_count,
            )
        }
    }

    pub fn binary_search(&self, timestamp_ns: u64) -> std::result::Result<usize, usize> {
        self.entries()
            .binary_search_by(|entry| entry.timestamp_ns.cmp(&timestamp_ns))
    }

    pub fn find_range_start(&self, start_ts: u64) -> usize {
        let search_ts = start_ts.saturating_sub(TIMESTAMP_JITTER_WINDOW_NS);
        match self.binary_search(search_ts) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }

    pub fn find_range_end(&self, end_ts: u64) -> usize {
        let search_ts = end_ts.saturating_add(TIMESTAMP_JITTER_WINDOW_NS);
        match self.binary_search(search_ts) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    }

    pub fn query_time_range(&self, start_ts: u64, end_ts: u64) -> TimeRangeIterator<'_> {
        let start_idx = self.find_range_start(start_ts);
        let end_idx = self.find_range_end(end_ts).min(self.entry_count);

        TimeRangeIterator {
            entries: self.entries(),
            current_idx: start_idx,
            end_idx,
            filter_start: start_ts,
            filter_end: end_ts,
        }
    }

    /// Query a time range with custom jitter window configuration
    pub fn query_time_range_with_jitter(
        &self,
        start_ts: u64,
        end_ts: u64,
        config: JitterWindowConfig,
    ) -> TimeRangeIterator<'_> {
        let search_start = config.expand_range_start(start_ts);
        let search_end = config.expand_range_end(end_ts);

        let start_idx = match self.binary_search(search_start) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        let end_idx = match self.binary_search(search_end) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
        .min(self.entry_count);

        TimeRangeIterator {
            entries: self.entries(),
            current_idx: start_idx,
            end_idx,
            filter_start: start_ts,
            filter_end: end_ts,
        }
    }

    /// Point-in-time query with jitter tolerance
    /// Returns all entries within the jitter window of the target timestamp
    pub fn query_point_in_time(&self, target_ts: u64) -> JitterQueryResult<'_> {
        self.query_point_in_time_with_config(target_ts, JitterWindowConfig::default())
    }

    /// Point-in-time query with custom jitter window configuration
    pub fn query_point_in_time_with_config(
        &self,
        target_ts: u64,
        config: JitterWindowConfig,
    ) -> JitterQueryResult<'_> {
        let entries = self.entries();
        let mut result = JitterQueryResult {
            entries: Vec::new(),
            exact_match: None,
            closest_before: None,
            closest_after: None,
        };

        if entries.is_empty() {
            return result;
        }

        // Find the search position
        let search_result = self.binary_search(target_ts);

        // Determine the index to start searching from
        let center_idx = match search_result {
            Ok(idx) => {
                result.exact_match = Some(&entries[idx]);
                idx
            },
            Err(idx) => idx,
        };

        // Expand search window
        let window_start = config.expand_range_start(target_ts);
        let window_end = config.expand_range_end(target_ts);

        // Find entries within the jitter window
        // Search backwards from center
        let mut idx = center_idx;
        while idx > 0 {
            idx -= 1;
            let entry = &entries[idx];
            if entry.timestamp_ns < window_start {
                // Found closest before if no exact match
                if result.exact_match.is_none() && result.closest_before.is_none() {
                    result.closest_before = Some(entry);
                }
                break;
            }
            result.entries.push(entry);
        }

        // Search forwards from center (including center if it's within window)
        for entry in entries.iter().skip(center_idx) {
            if entry.timestamp_ns > window_end {
                // Found closest after if no exact match
                if result.exact_match.is_none() && result.closest_after.is_none() {
                    result.closest_after = Some(entry);
                }
                break;
            }
            if entry.timestamp_ns >= window_start {
                result.entries.push(entry);
            }
        }

        // Sort entries by timestamp for consistent ordering
        result.entries.sort_by_key(|e| e.timestamp_ns);

        result
    }

    /// Find the closest entry to a target timestamp
    pub fn find_closest(&self, target_ts: u64) -> Option<&TimestampEntry> {
        let entries = self.entries();
        if entries.is_empty() {
            return None;
        }

        match self.binary_search(target_ts) {
            Ok(idx) => Some(&entries[idx]),
            Err(idx) => {
                if idx == 0 {
                    Some(&entries[0])
                } else if idx >= entries.len() {
                    entries.last()
                } else {
                    // Compare distances to entries before and after
                    let before = &entries[idx - 1];
                    let after = &entries[idx];
                    let dist_before = target_ts.saturating_sub(before.timestamp_ns);
                    let dist_after = after.timestamp_ns.saturating_sub(target_ts);
                    if dist_before <= dist_after {
                        Some(before)
                    } else {
                        Some(after)
                    }
                }
            },
        }
    }

    /// Check if a timestamp falls within the jitter window of any indexed entry
    pub fn has_entry_within_jitter(&self, target_ts: u64) -> bool {
        self.has_entry_within_window(target_ts, TIMESTAMP_JITTER_WINDOW_NS)
    }

    /// Check if a timestamp falls within a custom window of any indexed entry
    pub fn has_entry_within_window(&self, target_ts: u64, window_ns: u64) -> bool {
        let entries = self.entries();
        if entries.is_empty() {
            return false;
        }

        let window_start = target_ts.saturating_sub(window_ns);
        let window_end = target_ts.saturating_add(window_ns);

        // Binary search to find approximate position
        let idx = match self.binary_search(target_ts) {
            Ok(_) => return true, // Exact match
            Err(idx) => idx,
        };

        // Check entry before
        if idx > 0 {
            let before = &entries[idx - 1];
            if before.timestamp_ns >= window_start {
                return true;
            }
        }

        // Check entry at/after
        if idx < entries.len() {
            let after = &entries[idx];
            if after.timestamp_ns <= window_end {
                return true;
            }
        }

        false
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

    pub fn first(&self) -> Option<&TimestampEntry> {
        self.entries().first()
    }

    pub fn last(&self) -> Option<&TimestampEntry> {
        self.entries().last()
    }
}

pub struct TimeRangeIterator<'a> {
    entries: &'a [TimestampEntry],
    current_idx: usize,
    end_idx: usize,
    filter_start: u64,
    filter_end: u64,
}

impl<'a> Iterator for TimeRangeIterator<'a> {
    type Item = &'a TimestampEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_idx < self.end_idx {
            let entry = &self.entries[self.current_idx];
            self.current_idx += 1;

            if entry.timestamp_ns >= self.filter_start && entry.timestamp_ns <= self.filter_end {
                return Some(entry);
            }
        }
        None
    }
}

pub struct SecondaryIndexWriter {
    entries: Vec<TimestampEntry>,
    bucket_interval_ns: u64,
    last_bucket: u64,
}

impl SecondaryIndexWriter {
    pub fn new(bucket_interval_ns: u64) -> Self {
        Self {
            entries: Vec::new(),
            bucket_interval_ns,
            last_bucket: 0,
        }
    }

    pub fn with_default_interval() -> Self {
        Self::new(10_000_000)
    }

    pub fn maybe_add_entry(&mut self, timestamp_ns: u64, byte_offset: u64) -> bool {
        let bucket = timestamp_ns / self.bucket_interval_ns;

        if bucket != self.last_bucket || self.entries.is_empty() {
            self.entries
                .push(TimestampEntry::new(timestamp_ns, byte_offset));
            self.last_bucket = bucket;
            true
        } else {
            false
        }
    }

    pub fn add_entry(&mut self, timestamp_ns: u64, byte_offset: u64) {
        self.entries
            .push(TimestampEntry::new(timestamp_ns, byte_offset));
    }

    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        use std::io::Write;

        let mut file = File::create(path)?;
        // Safety: TimestampEntry is repr(C) and IntoBytes, so this cast is valid
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                self.entries.as_ptr() as *const u8,
                self.entries.len() * std::mem::size_of::<TimestampEntry>(),
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
        self.last_bucket = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_secondary_index_write_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tidx");

        let mut writer = SecondaryIndexWriter::new(1);
        writer.add_entry(1_000_000, 0);
        writer.add_entry(2_000_000, 100);
        writer.add_entry(3_000_000, 200);
        writer.write_to_file(&path).unwrap();

        let index = SecondaryIndex::open(&path).unwrap();
        assert_eq!(index.entry_count(), 3);

        let entries = index.entries();
        assert_eq!(entries[0].timestamp_ns, 1_000_000);
        assert_eq!(entries[2].byte_offset, 200);
    }

    #[test]
    fn test_time_range_query() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tidx");

        let mut writer = SecondaryIndexWriter::new(1);
        for i in 0..100 {
            writer.add_entry(i * 1_000_000, i * 100);
        }
        writer.write_to_file(&path).unwrap();

        let index = SecondaryIndex::open(&path).unwrap();

        let results: Vec<_> = index.query_time_range(20_000_000, 30_000_000).collect();

        assert_eq!(results.len(), 11);
        assert_eq!(results[0].timestamp_ns, 20_000_000);
        assert_eq!(results[10].timestamp_ns, 30_000_000);
    }

    #[test]
    fn test_bucket_based_entries() {
        let mut writer = SecondaryIndexWriter::new(10_000_000);

        writer.maybe_add_entry(5_000_000, 0);
        writer.maybe_add_entry(8_000_000, 100);
        writer.maybe_add_entry(15_000_000, 200);
        writer.maybe_add_entry(18_000_000, 300);
        writer.maybe_add_entry(25_000_000, 400);

        assert_eq!(writer.entry_count(), 3);
    }
}
