#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod backend;
mod priority;
mod segment;
mod tier;
mod wal;

pub use backend::{IoBackend, IoBackendType, probe_io_uring};
pub use priority::{IoPriority, IoPriorityQueue, IoPriorityStats, PrioritizedIoRequest};
pub use segment::{
    BorrowedSlice, CompactionConfig, CompactionResult, DirectBackend, SegmentCompactor,
    SegmentMetadata, SegmentReader, SegmentState, SegmentWriter, ZeroCopyReader,
    close_unclosed_segments,
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
    SendZcSupport, SpliceForwarder, SplicePipe, SpliceSupport, TeeForwarder, TeeSupport,
    ZeroCopySender, probe_send_zc, probe_splice, probe_tee,
};

pub mod fallback;

pub const O_DIRECT_ALIGNMENT: usize = 4096;
