use lnc_core::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub dir: PathBuf,
    pub size: u64,
    pub max_segment_size: u64,
    pub sync_on_write: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: PathBuf::from("wal/current.wal"),
            dir: PathBuf::from("wal"),
            size: 64 * 1024 * 1024,
            max_segment_size: 64 * 1024 * 1024, // 64 MiB
            sync_on_write: true,
        }
    }
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            path: dir.join("current.wal"),
            dir,
            ..Default::default()
        }
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    pub fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_on_write = sync;
        self
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

pub struct Wal {
    config: WalConfig,
    current_segment: Option<File>,
    current_segment_id: u64,
    write_offset: u64,
}

impl Wal {
    pub fn open(config: WalConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.dir)?;

        let mut wal = Self {
            config,
            current_segment: None,
            current_segment_id: 0,
            write_offset: 0,
        };

        wal.recover()?;
        Ok(wal)
    }

    fn recover(&mut self) -> Result<()> {
        let mut max_segment_id = 0u64;

        if let Ok(entries) = std::fs::read_dir(&self.config.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(id_str) = name.strip_suffix(".wal") {
                        if let Ok(id) = id_str.parse::<u64>() {
                            max_segment_id = max_segment_id.max(id);
                        }
                    }
                }
            }
        }

        self.current_segment_id = max_segment_id;
        self.open_or_create_segment()?;

        Ok(())
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.config.dir.join(format!("{:016}.wal", segment_id))
    }

    fn open_or_create_segment(&mut self) -> Result<()> {
        let path = self.segment_path(self.current_segment_id);

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        self.write_offset = file.seek(SeekFrom::End(0))?;
        self.current_segment = Some(file);

        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<()> {
        if let Some(ref mut file) = self.current_segment {
            file.sync_all()?;
        }

        self.current_segment_id += 1;
        self.write_offset = 0;
        self.open_or_create_segment()?;

        Ok(())
    }

    /// Append a framed entry to the WAL.
    ///
    /// Wire format: `[len: u32 LE][data: len bytes]`
    ///
    /// This framing allows [`Self::replay`] to iterate entries without external
    /// metadata.  Returns `(segment_id, byte_offset)` of the entry.
    pub fn append(&mut self, data: &[u8]) -> Result<(u64, u64)> {
        let frame_len = 4 + data.len() as u64;
        if self.write_offset + frame_len > self.config.max_segment_size {
            self.rotate_segment()?;
        }

        let segment_id = self.current_segment_id;
        let offset = self.write_offset;

        if let Some(ref mut file) = self.current_segment {
            let len_bytes = (data.len() as u32).to_le_bytes();
            file.write_all(&len_bytes)?;
            file.write_all(data)?;

            if self.config.sync_on_write {
                file.sync_all()?;
            }

            self.write_offset += frame_len;
        }

        Ok((segment_id, offset))
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut file) = self.current_segment {
            file.sync_all()?;
        }
        Ok(())
    }

    pub fn read_at(&mut self, segment_id: u64, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let path = self.segment_path(segment_id);
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        let bytes_read = file.read(buf)?;
        Ok(bytes_read)
    }

    #[inline]
    #[must_use]
    pub fn current_segment_id(&self) -> u64 {
        self.current_segment_id
    }

    #[inline]
    #[must_use]
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    #[inline]
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.config.path.exists() || self.write_offset > 0
    }

    pub fn replay<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], u64) -> Result<()>,
    {
        for seg_id in 0..=self.current_segment_id {
            let path = self.segment_path(seg_id);
            if !path.exists() {
                continue;
            }

            let mut file = File::open(&path)?;
            let size = file.metadata()?.len();
            let mut data = vec![0u8; size as usize];
            file.read_exact(&mut data)?;

            let mut offset = 0u64;
            while (offset as usize) < data.len() {
                let remaining = &data[offset as usize..];
                if remaining.len() < 4 {
                    break;
                }

                let len =
                    u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]])
                        as usize;
                if remaining.len() < 4 + len {
                    break;
                }

                let entry_data = &remaining[4..4 + len];
                callback(entry_data, offset)?;
                offset += (4 + len) as u64;
            }
        }

        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        for seg_id in 0..=self.current_segment_id {
            let path = self.segment_path(seg_id);
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
        }

        self.current_segment_id = 0;
        self.write_offset = 0;
        self.current_segment = None;
        self.open_or_create_segment()?;

        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn config(&self) -> &WalConfig {
        &self.config
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_basic() {
        let dir = std::env::temp_dir().join("test_wal_basic");
        let _ = std::fs::remove_dir_all(&dir);

        let config = WalConfig::new(&dir).with_sync_on_write(false);
        let mut wal = Wal::open(config).unwrap();

        // Each append writes: [len:4 bytes LE][data:N bytes]
        let (seg1, off1) = wal.append(b"record1").unwrap();
        let (seg2, off2) = wal.append(b"record2").unwrap();

        assert_eq!(seg1, seg2);
        assert_eq!(off1, 0);
        // Second entry starts at 4 (len) + 7 (data) = 11
        assert_eq!(off2, 11);

        // read_at reads raw bytes — skip the 4-byte length header
        let mut buf = [0u8; 7];
        wal.read_at(seg1, off1 + 4, &mut buf).unwrap();
        assert_eq!(&buf, b"record1");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_rotation() {
        let dir = std::env::temp_dir().join("test_wal_rotation");
        let _ = std::fs::remove_dir_all(&dir);

        // Each 10-byte payload = 14 bytes framed (4 + 10).
        // max_segment_size=30 fits 2 entries (28 bytes), third rotates.
        let config = WalConfig::new(&dir)
            .with_max_segment_size(30)
            .with_sync_on_write(false);

        let mut wal = Wal::open(config).unwrap();

        let (seg1, _) = wal.append(b"0123456789").unwrap();
        let (seg2, _) = wal.append(b"0123456789").unwrap();
        let (seg3, _) = wal.append(b"0123456789").unwrap();

        assert_eq!(seg1, 0);
        assert_eq!(seg2, 0);
        assert_eq!(seg3, 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_replay_roundtrip() {
        let dir = std::env::temp_dir().join("test_wal_replay_roundtrip");
        let _ = std::fs::remove_dir_all(&dir);

        let config = WalConfig::new(&dir).with_sync_on_write(false);
        let mut wal = Wal::open(config.clone()).unwrap();

        wal.append(b"alpha").unwrap();
        wal.append(b"beta").unwrap();
        wal.append(b"gamma").unwrap();
        drop(wal);

        // Re-open and replay
        let mut wal2 = Wal::open(config).unwrap();
        let mut replayed: Vec<Vec<u8>> = Vec::new();
        wal2.replay(|data, _offset| {
            replayed.push(data.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0], b"alpha");
        assert_eq!(replayed[1], b"beta");
        assert_eq!(replayed[2], b"gamma");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
