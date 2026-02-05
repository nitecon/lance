use lnc_core::{Result, TLV_HEADER_SIZE, parse_header};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct RecoveryResult {
    pub valid_bytes: u64,
    pub valid_records: u64,
    pub truncated_bytes: u64,
    pub corrupted_at: Option<u64>,
}

pub struct SegmentRecovery {
    path: std::path::PathBuf,
}

impl SegmentRecovery {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    pub fn scan(&self) -> Result<RecoveryResult> {
        let mut file = File::open(&self.path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

        let mut offset = 0u64;
        let mut valid_records = 0u64;
        let mut corrupted_at = None;

        let mut header_buf = [0u8; 16];

        while offset < file_size {
            file.seek(SeekFrom::Start(offset))?;

            let bytes_read = file.read(&mut header_buf)?;
            if bytes_read < TLV_HEADER_SIZE {
                debug!(
                    target: "lance::recovery",
                    offset = offset,
                    bytes_read = bytes_read,
                    "Incomplete header at end of segment"
                );
                break;
            }

            let header = match parse_header(&header_buf) {
                Ok(h) => h,
                Err(e) => {
                    warn!(
                        target: "lance::recovery",
                        offset = offset,
                        error = %e,
                        "Failed to parse TLV header"
                    );
                    corrupted_at = Some(offset);
                    break;
                },
            };

            let record_size = header.total_size();
            let remaining = file_size - offset;

            if remaining < record_size as u64 {
                debug!(
                    target: "lance::recovery",
                    offset = offset,
                    expected = record_size,
                    remaining = remaining,
                    "Truncated record at end of segment"
                );
                corrupted_at = Some(offset);
                break;
            }

            valid_records += 1;
            offset += record_size as u64;
        }

        let truncated_bytes = file_size - offset;

        info!(
            target: "lance::recovery",
            path = %self.path.display(),
            valid_bytes = offset,
            valid_records = valid_records,
            truncated_bytes = truncated_bytes,
            "Segment scan complete"
        );

        Ok(RecoveryResult {
            valid_bytes: offset,
            valid_records,
            truncated_bytes,
            corrupted_at,
        })
    }

    pub fn truncate_to_valid(&self) -> Result<RecoveryResult> {
        let result = self.scan()?;

        if result.truncated_bytes > 0 {
            let file = std::fs::OpenOptions::new().write(true).open(&self.path)?;

            file.set_len(result.valid_bytes)?;
            file.sync_all()?;

            info!(
                target: "lance::recovery",
                path = %self.path.display(),
                truncated_bytes = result.truncated_bytes,
                new_size = result.valid_bytes,
                "Segment truncated to last valid record"
            );
        }

        Ok(result)
    }

    pub fn mark_as_dirty(&self) -> Result<()> {
        let dirty_marker_path = self.path.with_extension("dirty");
        File::create(&dirty_marker_path)?;

        info!(
            target: "lance::recovery",
            path = %self.path.display(),
            "Segment marked as dirty"
        );

        Ok(())
    }

    pub fn clear_dirty_marker(&self) -> Result<()> {
        let dirty_marker_path = self.path.with_extension("dirty");
        if dirty_marker_path.exists() {
            std::fs::remove_file(&dirty_marker_path)?;

            info!(
                target: "lance::recovery",
                path = %self.path.display(),
                "Dirty marker cleared"
            );
        }

        Ok(())
    }

    pub fn is_dirty(&self) -> bool {
        self.path.with_extension("dirty").exists()
    }
}

pub fn find_segments_needing_recovery(data_dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut segments = Vec::new();

    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "lnc") {
            let filename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

            let is_active = !filename.contains('-');
            let has_dirty_marker = path.with_extension("dirty").exists();

            if is_active || has_dirty_marker {
                segments.push(path);
            }
        }
    }

    Ok(segments)
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
    fn test_segment_scan_valid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.lnc");

        let mut file = File::create(&path).unwrap();

        let payload = b"test payload";
        let mut buf = Vec::new();
        encode_test_tlv(0x10, payload, &mut buf);
        file.write_all(&buf).unwrap();

        buf.clear();
        encode_test_tlv(0x10, payload, &mut buf);
        file.write_all(&buf).unwrap();

        file.sync_all().unwrap();
        drop(file);

        let recovery = SegmentRecovery::new(&path);
        let result = recovery.scan().unwrap();

        assert_eq!(result.valid_records, 2);
        assert_eq!(result.truncated_bytes, 0);
        assert!(result.corrupted_at.is_none());
    }

    #[test]
    fn test_segment_scan_truncated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.lnc");

        let mut file = File::create(&path).unwrap();

        let payload = b"test payload";
        let mut buf = Vec::new();
        encode_test_tlv(0x10, payload, &mut buf);
        file.write_all(&buf).unwrap();

        file.write_all(&[0x10, 0x00, 0x10]).unwrap();

        file.sync_all().unwrap();
        drop(file);

        let recovery = SegmentRecovery::new(&path);
        let result = recovery.scan().unwrap();

        assert_eq!(result.valid_records, 1);
        assert!(result.truncated_bytes > 0);
    }

    #[test]
    fn test_segment_truncate_to_valid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_truncate.lnc");

        let mut file = File::create(&path).unwrap();

        // Write valid record
        let payload = b"valid record";
        let mut buf = Vec::new();
        encode_test_tlv(0x10, payload, &mut buf);
        file.write_all(&buf).unwrap();

        // Write incomplete/truncated record
        file.write_all(&[0x10, 0xFF, 0x00, 0x00, 0x00]).unwrap();

        file.sync_all().unwrap();
        drop(file);

        let recovery = SegmentRecovery::new(&path);
        let result = recovery.truncate_to_valid().unwrap();

        assert_eq!(result.valid_records, 1);
        assert!(result.truncated_bytes > 0);

        // Verify file was actually truncated
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), result.valid_bytes);
    }

    #[test]
    fn test_dirty_marker() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_dirty.lnc");

        // Create empty segment file
        File::create(&path).unwrap();

        let recovery = SegmentRecovery::new(&path);

        // Initially not dirty
        assert!(!recovery.is_dirty());

        // Mark as dirty
        recovery.mark_as_dirty().unwrap();
        assert!(recovery.is_dirty());

        // Clear dirty marker
        recovery.clear_dirty_marker().unwrap();
        assert!(!recovery.is_dirty());
    }

    #[test]
    fn test_segment_scan_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_empty.lnc");

        // Create empty file
        File::create(&path).unwrap();

        let recovery = SegmentRecovery::new(&path);
        let result = recovery.scan().unwrap();

        assert_eq!(result.valid_records, 0);
        assert_eq!(result.valid_bytes, 0);
        assert_eq!(result.truncated_bytes, 0);
        assert!(result.corrupted_at.is_none());
    }

    #[test]
    fn test_segment_scan_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_multi.lnc");

        let mut file = File::create(&path).unwrap();

        // Write multiple valid records
        for i in 0..10 {
            let payload = format!("record_{}", i);
            let mut buf = Vec::new();
            encode_test_tlv(0x10, payload.as_bytes(), &mut buf);
            file.write_all(&buf).unwrap();
        }

        file.sync_all().unwrap();
        drop(file);

        let recovery = SegmentRecovery::new(&path);
        let result = recovery.scan().unwrap();

        assert_eq!(result.valid_records, 10);
        assert_eq!(result.truncated_bytes, 0);
        assert!(result.corrupted_at.is_none());
    }

    #[test]
    fn test_find_segments_needing_recovery() {
        let dir = tempdir().unwrap();

        // Create a segment file directly in data_dir (function doesn't recurse)
        let segment_path = dir.path().join("segment_0.lnc");
        File::create(&segment_path).unwrap();

        // Mark as dirty - dirty marker uses .dirty extension replacing .lnc
        let dirty_marker = dir.path().join("segment_0.dirty");
        File::create(&dirty_marker).unwrap();

        let segments = find_segments_needing_recovery(dir.path()).unwrap();

        assert_eq!(segments.len(), 1);
        assert!(segments[0].ends_with("segment_0.lnc"));
    }
}
