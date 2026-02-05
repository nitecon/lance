#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod index_rebuild;
mod segment_recovery;
mod wal_replay;

pub use index_rebuild::{rebuild_missing_indexes, IndexRebuilder, RebuildResult};
pub use segment_recovery::{find_segments_needing_recovery, RecoveryResult, SegmentRecovery};
pub use wal_replay::{perform_wal_recovery, WalReplay};
