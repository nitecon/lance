use lnc_core::Result;
use lnc_io::{SegmentWriter, Wal, WalConfig};
use std::path::Path;
use tracing::{debug, info, warn};

pub struct WalReplay {
    config: WalConfig,
}

impl WalReplay {
    pub fn new(config: WalConfig) -> Self {
        Self { config }
    }

    pub fn replay_to_segment(&self, segment_writer: &mut SegmentWriter) -> Result<u64> {
        if !self.config.enabled {
            return Ok(0);
        }

        if !self.config.path.exists() {
            info!(
                target: "lance::recovery",
                path = %self.config.path.display(),
                "No WAL file found, skipping replay"
            );
            return Ok(0);
        }

        let mut wal = Wal::open(self.config.clone())?;

        if !wal.is_valid() {
            warn!(
                target: "lance::recovery",
                path = %self.config.path.display(),
                "WAL is invalid, skipping replay"
            );
            return Ok(0);
        }

        let mut entries_replayed = 0u64;
        let mut bytes_replayed = 0u64;

        wal.replay(|data, offset| {
            segment_writer.write_batch(data)?;
            entries_replayed += 1;
            bytes_replayed += data.len() as u64;

            debug!(
                target: "lance::recovery",
                offset = offset,
                size = data.len(),
                "Replayed WAL entry"
            );

            Ok(())
        })?;

        segment_writer.fsync()?;

        info!(
            target: "lance::recovery",
            entries = entries_replayed,
            bytes = bytes_replayed,
            "WAL replay complete"
        );

        Ok(entries_replayed)
    }

    pub fn reset_wal(&self) -> Result<()> {
        if !self.config.enabled || !self.config.path.exists() {
            return Ok(());
        }

        let mut wal = Wal::open(self.config.clone())?;
        wal.reset()?;

        info!(
            target: "lance::recovery",
            path = %self.config.path.display(),
            "WAL reset after successful replay"
        );

        Ok(())
    }

    pub fn should_replay(&self) -> bool {
        self.config.enabled && self.config.path.exists()
    }
}

pub fn perform_wal_recovery(
    wal_config: WalConfig,
    data_dir: &Path,
    start_index: u64,
    start_timestamp_ns: u64,
) -> Result<Option<std::path::PathBuf>> {
    let replay = WalReplay::new(wal_config.clone());

    if !replay.should_replay() {
        return Ok(None);
    }

    let segment_path = data_dir.join(format!("{}_{}.lnc", start_index, start_timestamp_ns));
    let mut segment_writer = SegmentWriter::create(&segment_path)?;

    let entries = replay.replay_to_segment(&mut segment_writer)?;

    if entries > 0 {
        segment_writer.fsync()?;
        replay.reset_wal()?;

        info!(
            target: "lance::recovery",
            entries = entries,
            segment = %segment_writer.path().display(),
            "WAL recovery complete"
        );

        Ok(Some(segment_writer.path().to_path_buf()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_replay_no_wal() {
        let dir = tempdir().unwrap();
        let config = WalConfig::new(dir.path()).with_enabled(true);

        let replay = WalReplay::new(config);
        assert!(!replay.should_replay());
    }

    #[test]
    fn test_wal_replay_disabled() {
        let config = WalConfig::default().with_enabled(false);

        let replay = WalReplay::new(config);
        assert!(!replay.should_replay());
    }
}
