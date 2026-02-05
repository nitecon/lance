#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod backend;
mod priority;
mod segment;
mod tier;
mod wal;

pub use backend::{probe_io_uring, IoBackend, IoBackendType};
pub use priority::{IoPriority, IoPriorityQueue, IoPriorityStats, PrioritizedIoRequest};
pub use segment::{
    close_unclosed_segments, BorrowedSlice, CompactionConfig, CompactionResult, SegmentCompactor,
    SegmentMetadata, SegmentReader, SegmentState, SegmentWriter, ZeroCopyReader,
};
pub use tier::{
    StorageTier, TierConfig, TierMoveResult, TierStats, TierableSegment, TieredStorageConfig,
    TieredStorageManager,
};
pub use wal::{Wal, WalConfig};

#[cfg(target_os = "linux")]
pub mod uring;

#[cfg(target_os = "linux")]
pub use uring::{
    probe_send_zc, probe_splice, probe_tee, SendZcSupport, SpliceForwarder, SplicePipe,
    SpliceSupport, TeeForwarder, TeeSupport, ZeroCopySender,
};

pub mod fallback;

pub const O_DIRECT_ALIGNMENT: usize = 4096;
