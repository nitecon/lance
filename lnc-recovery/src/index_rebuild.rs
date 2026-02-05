use lnc_core::{parse_header, Result, SortKey, DEFAULT_SPARSE_INDEX_INTERVAL};
use lnc_index::{IndexBuilder, INDEX_FILE_EXTENSION, SECONDARY_INDEX_EXTENSION};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{info, warn};

pub struct IndexRebuilder {
    sparse_interval: u64,
    timestamp_bucket_ns: u64,
}

impl IndexRebuilder {
    pub fn new(sparse_interval: u64, timestamp_bucket_ns: u64) -> Self {
        Self {
            sparse_interval,
            timestamp_bucket_ns,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_SPARSE_INDEX_INTERVAL, 10_000_000)
    }

    pub fn rebuild(&self, segment_path: &Path) -> Result<RebuildResult> {
        let mut file = File::open(segment_path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

        let mut data = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut data)?;

        let mut builder = IndexBuilder::new(self.sparse_interval, self.timestamp_bucket_ns);
        let mut offset = 0u64;
        let mut records_indexed = 0u64;

        while (offset as usize) < data.len() {
            let remaining = &data[offset as usize..];

            if remaining.len() < 5 {
                break;
            }

            let header = match parse_header(remaining) {
                Ok(h) => h,
                Err(e) => {
                    warn!(
                        target: "lance::recovery",
                        offset = offset,
                        error = %e,
                        "Failed to parse TLV header during index rebuild"
                    );
                    break;
                },
            };

            let record_size = header.total_size();

            if remaining.len() < record_size {
                warn!(
                    target: "lance::recovery",
                    offset = offset,
                    expected = record_size,
                    available = remaining.len(),
                    "Truncated record during index rebuild"
                );
                break;
            }

            let timestamp_ns = extract_timestamp_from_payload(
                remaining,
                header.header_size,
                header.length as usize,
            );
            let sort_key = SortKey::from_timestamp_ns(timestamp_ns, records_indexed as u32);

            builder.add_record(sort_key, timestamp_ns, offset);

            records_indexed += 1;
            offset += record_size as u64;
        }

        let (sparse_path, secondary_path) = builder.write_indexes(segment_path)?;

        info!(
            target: "lance::recovery",
            segment = %segment_path.display(),
            records = records_indexed,
            sparse_entries = builder.sparse_entry_count(),
            secondary_entries = builder.secondary_entry_count(),
            "Index rebuild complete"
        );

        Ok(RebuildResult {
            records_indexed,
            sparse_entries: builder.sparse_entry_count(),
            secondary_entries: builder.secondary_entry_count(),
            sparse_path,
            secondary_path,
        })
    }

    pub fn needs_rebuild(&self, segment_path: &Path) -> bool {
        let sparse_path = segment_path.with_extension(INDEX_FILE_EXTENSION);
        let secondary_path = segment_path.with_extension(SECONDARY_INDEX_EXTENSION);

        !sparse_path.exists() || !secondary_path.exists()
    }
}

fn extract_timestamp_from_payload(data: &[u8], header_size: usize, payload_len: usize) -> u64 {
    let payload = &data[header_size..header_size + payload_len];

    if payload.len() >= 8 {
        u64::from_le_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ])
    } else {
        0
    }
}

#[derive(Debug)]
pub struct RebuildResult {
    pub records_indexed: u64,
    pub sparse_entries: usize,
    pub secondary_entries: usize,
    pub sparse_path: std::path::PathBuf,
    pub secondary_path: std::path::PathBuf,
}

pub fn rebuild_missing_indexes(data_dir: &Path) -> Result<Vec<RebuildResult>> {
    let rebuilder = IndexRebuilder::with_defaults();
    let mut results = Vec::new();

    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "lnc") && rebuilder.needs_rebuild(&path) {
            info!(
                target: "lance::recovery",
                segment = %path.display(),
                "Rebuilding missing indexes"
            );

            let result = rebuilder.rebuild(&path)?;
            results.push(result);
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn encode_test_tlv(tag: u8, payload: &[u8], out: &mut Vec<u8>) {
        out.push(tag);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
    }

    #[test]
    fn test_index_rebuild() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("0_1000.lnc");

        let mut file = File::create(&segment_path).unwrap();

        for i in 0..100u64 {
            let timestamp = (i * 1_000_000u64).to_le_bytes();
            let mut payload = Vec::new();
            payload.extend_from_slice(&timestamp);
            payload.extend_from_slice(b"test data");

            let mut buf = Vec::new();
            encode_test_tlv(0x10, &payload, &mut buf);
            file.write_all(&buf).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        let rebuilder = IndexRebuilder::new(10, 1_000_000);
        assert!(rebuilder.needs_rebuild(&segment_path));

        let result = rebuilder.rebuild(&segment_path).unwrap();

        assert_eq!(result.records_indexed, 100);
        assert!(result.sparse_entries > 0);
        assert!(result.sparse_path.exists());
        assert!(result.secondary_path.exists());

        assert!(!rebuilder.needs_rebuild(&segment_path));
    }
}
