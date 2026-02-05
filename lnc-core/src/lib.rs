#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod backpressure;
mod batch;
mod buffer;
mod chaos;
mod checksum;
mod error;
mod hlc;
mod numa;
mod sort_key;
mod tlv;

pub use backpressure::{
    BackpressureConfig, BackpressureGuard, BackpressureLevel, BackpressureMonitor,
    BackpressureSnapshot,
};
pub use batch::{BatchPool, IngestionBatch, LoanableBatch};
pub use buffer::{AlignedBuffer, NumaAlignedBuffer};
pub use chaos::{
    ChaosConfig, ChaosInjector, ChaosScenario, FaultType, SharedChaosInjector, shared_injector,
};
pub use checksum::crc32c;
pub use error::{LanceError, Result};
pub use hlc::{ClockHealth, HlcTimestamp, HybridLogicalClock};
pub use numa::{
    NumaAllocator, NumaThreadPoolConfig, NumaTopology, get_current_numa_node, pin_thread_to_cpu,
    pin_thread_to_numa_node,
};
pub use sort_key::SortKey;
pub use tlv::{Header, parse_header};

pub const LANCE_MAGIC: [u8; 4] = [0x4C, 0x41, 0x4E, 0x43]; // 'LANC'
pub const TLV_HEADER_SIZE: usize = 5;
pub const EXTENDED_TLV_HEADER_SIZE: usize = 7;
pub const EXTENDED_TYPE_MARKER: u8 = 0xFF;

pub const DEFAULT_BATCH_SIZE: usize = 64 * 1024; // 64 KiB
pub const DEFAULT_SPARSE_INDEX_INTERVAL: u64 = 4096;
pub const TIMESTAMP_JITTER_WINDOW_NS: u64 = 10_000_000; // 10ms
