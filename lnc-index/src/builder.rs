use crate::secondary::SecondaryIndexWriter;
use crate::sparse::SparseIndexWriter;
use lnc_core::{Result, SortKey, DEFAULT_SPARSE_INDEX_INTERVAL};
use std::path::Path;

pub struct IndexBuilder {
    sparse_writer: SparseIndexWriter,
    secondary_writer: SecondaryIndexWriter,
    record_count: u64,
}

impl IndexBuilder {
    pub fn new(sparse_interval: u64, timestamp_bucket_ns: u64) -> Self {
        Self {
            sparse_writer: SparseIndexWriter::new(sparse_interval),
            secondary_writer: SecondaryIndexWriter::new(timestamp_bucket_ns),
            record_count: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_SPARSE_INDEX_INTERVAL, 10_000_000)
    }

    pub fn add_record(&mut self, sort_key: SortKey, original_ts: u64, byte_offset: u64) {
        self.sparse_writer.maybe_add_entry(sort_key, byte_offset);
        self.secondary_writer
            .maybe_add_entry(original_ts, byte_offset);
        self.record_count += 1;
    }

    pub fn write_indexes(
        &self,
        segment_path: &Path,
    ) -> Result<(std::path::PathBuf, std::path::PathBuf)> {
        let sparse_path = segment_path.with_extension(crate::INDEX_FILE_EXTENSION);
        let secondary_path = segment_path.with_extension(crate::SECONDARY_INDEX_EXTENSION);

        self.sparse_writer.write_to_file(&sparse_path)?;
        self.secondary_writer.write_to_file(&secondary_path)?;

        Ok((sparse_path, secondary_path))
    }

    #[inline]
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    #[inline]
    #[must_use]
    pub fn sparse_entry_count(&self) -> usize {
        self.sparse_writer.entry_count()
    }

    #[inline]
    #[must_use]
    pub fn secondary_entry_count(&self) -> usize {
        self.secondary_writer.entry_count()
    }

    pub fn clear(&mut self) {
        self.sparse_writer.clear();
        self.secondary_writer.clear();
        self.record_count = 0;
    }
}

pub fn rebuild_indexes_from_segment<F>(
    segment_path: &Path,
    sparse_interval: u64,
    timestamp_bucket_ns: u64,
    mut record_parser: F,
) -> Result<IndexBuilder>
where
    F: FnMut(&[u8], u64) -> Result<(SortKey, u64)>,
{
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(segment_path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let mut builder = IndexBuilder::new(sparse_interval, timestamp_bucket_ns);
    let mut offset = 0u64;

    while (offset as usize) < data.len() {
        let remaining = &data[offset as usize..];
        if remaining.len() < 5 {
            break;
        }

        match record_parser(remaining, offset) {
            Ok((sort_key, original_ts)) => {
                let header = lnc_core::parse_header(remaining)?;
                let record_size = header.total_size();

                builder.add_record(sort_key, original_ts, offset);
                offset += record_size as u64;
            },
            Err(e) => {
                tracing::warn!(
                    target: "lance::index",
                    offset = offset,
                    error = %e,
                    "Failed to parse record, stopping index rebuild"
                );
                break;
            },
        }
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_index_builder() {
        let mut builder = IndexBuilder::with_defaults();

        for i in 0..10000 {
            let sort_key = SortKey::from_timestamp_ns(i * 1000, i as u32);
            builder.add_record(sort_key, i * 1000, i * 100);
        }

        assert_eq!(builder.record_count(), 10000);
        assert!(builder.sparse_entry_count() > 0);
        assert!(builder.secondary_entry_count() > 0);
    }

    #[test]
    fn test_index_builder_write() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("test.lnc");

        let mut builder = IndexBuilder::new(10, 1_000_000);

        for i in 0..100 {
            let sort_key = SortKey::from_timestamp_ns(i * 1_000_000, i as u32);
            builder.add_record(sort_key, i * 1_000_000, i * 100);
        }

        let (sparse_path, secondary_path) = builder.write_indexes(&segment_path).unwrap();

        assert!(sparse_path.exists());
        assert!(secondary_path.exists());
    }

    #[test]
    fn test_index_builder_empty() {
        let builder = IndexBuilder::with_defaults();
        
        assert_eq!(builder.record_count(), 0);
        assert_eq!(builder.sparse_entry_count(), 0);
        assert_eq!(builder.secondary_entry_count(), 0);
    }

    #[test]
    fn test_index_builder_clear() {
        let mut builder = IndexBuilder::with_defaults();
        
        // Add some records
        for i in 0..100 {
            let sort_key = SortKey::from_timestamp_ns(i * 1000, i as u32);
            builder.add_record(sort_key, i * 1000, i * 100);
        }
        
        assert!(builder.record_count() > 0);
        
        // Clear and verify
        builder.clear();
        
        assert_eq!(builder.record_count(), 0);
        assert_eq!(builder.sparse_entry_count(), 0);
        assert_eq!(builder.secondary_entry_count(), 0);
    }

    #[test]
    fn test_index_builder_sparse_interval() {
        // With sparse_interval=10, we get 1 sparse entry per 10 records
        let mut builder = IndexBuilder::new(10, 1_000_000_000);
        
        for i in 0..100 {
            let sort_key = SortKey::from_timestamp_ns(i * 1_000_000, i as u32);
            builder.add_record(sort_key, i * 1_000_000, i * 100);
        }
        
        // 100 records / 10 interval = 10 sparse entries
        assert_eq!(builder.record_count(), 100);
        assert_eq!(builder.sparse_entry_count(), 10);
    }

    #[test]
    fn test_index_builder_timestamp_buckets() {
        // With 1ms buckets (1_000_000 ns), records in same bucket share secondary entry
        let mut builder = IndexBuilder::new(1000, 1_000_000);
        
        // Add 10 records in same 1ms bucket
        for i in 0..10 {
            let sort_key = SortKey::from_timestamp_ns(i * 100, i as u32);  // < 1ms apart
            builder.add_record(sort_key, i * 100, i * 100);
        }
        
        // All 10 records should be in 1 secondary bucket
        assert_eq!(builder.record_count(), 10);
        assert_eq!(builder.secondary_entry_count(), 1);
        
        // Add record in next bucket
        let sort_key = SortKey::from_timestamp_ns(2_000_000, 10);
        builder.add_record(sort_key, 2_000_000, 1000);
        
        // Now 2 secondary buckets
        assert_eq!(builder.record_count(), 11);
        assert_eq!(builder.secondary_entry_count(), 2);
    }

    #[test]
    fn test_index_builder_write_empty() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("empty.lnc");
        
        let builder = IndexBuilder::with_defaults();
        
        // Writing empty builder should still create valid (empty) index files
        let (sparse_path, secondary_path) = builder.write_indexes(&segment_path).unwrap();
        
        assert!(sparse_path.exists());
        assert!(secondary_path.exists());
        
        // Files should be empty or minimal
        let sparse_size = std::fs::metadata(&sparse_path).unwrap().len();
        let secondary_size = std::fs::metadata(&secondary_path).unwrap().len();
        
        assert_eq!(sparse_size, 0);
        assert_eq!(secondary_size, 0);
    }

    #[test]
    fn test_index_builder_large_dataset() {
        let mut builder = IndexBuilder::new(1000, 1_000_000_000);
        
        // Add 100k records to test scaling
        for i in 0u64..100_000 {
            let sort_key = SortKey::from_timestamp_ns(i * 1_000_000, (i % 1000) as u32);
            builder.add_record(sort_key, i * 1_000_000, i * 100);
        }
        
        assert_eq!(builder.record_count(), 100_000);
        // 100k records / 1000 interval = 100 sparse entries
        assert_eq!(builder.sparse_entry_count(), 100);
    }
}
