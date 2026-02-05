#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod index_rebuild;
mod segment_recovery;
mod wal_replay;

pub use index_rebuild::{IndexRebuilder, RebuildResult, rebuild_missing_indexes};
pub use segment_recovery::{RecoveryResult, SegmentRecovery, find_segments_needing_recovery};
pub use wal_replay::{WalReplay, perform_wal_recovery};
