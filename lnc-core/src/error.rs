use thiserror::Error;

#[derive(Error, Debug)]
pub enum LanceError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Buffer pool exhausted - no free batches available")]
    BufferPoolExhausted,

    #[error("Buffer too small: required {required} bytes, available {available}")]
    BufferTooSmall { required: usize, available: usize },

    #[error("Invalid TLV header: {0}")]
    InvalidTlvHeader(String),

    #[error("Invalid magic bytes - expected LANC")]
    InvalidMagic,

    #[error("CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("Unsupported kernel feature: {0}")]
    UnsupportedKernel(&'static str),

    #[error("Thread pinning failed for core {0}")]
    PinFailed(usize),

    #[error("NUMA allocation failed for node {0}")]
    NumaAllocFailed(usize),

    #[error("Memory lock (mlock) failed: {0}")]
    MlockFailed(String),

    #[error("Segment corrupted at offset {offset}: {reason}")]
    SegmentCorrupted { offset: u64, reason: String },

    #[error("Drain timeout exceeded")]
    DrainTimeout,

    #[error("Quorum not reached - required {required} ACKs, got {received}")]
    QuorumNotReached { required: usize, received: usize },

    #[error("Channel disconnected: {0}")]
    ChannelDisconnected(&'static str),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Index out of bounds: {0}")]
    IndexOutOfBounds(String),

    #[error("WAL replay failed: {0}")]
    WalReplayFailed(String),

    #[error("Segment not found: {0}")]
    SegmentNotFound(String),

    #[error("Payload too large: {0} bytes exceeds maximum")]
    PayloadTooLarge(usize),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Fenced off: attempted token {attempted:?}, current {current:?}")]
    FencedOff { attempted: u64, current: u64 },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Data corruption: {0}")]
    DataCorruption(String),

    #[error("Client error: {0}")]
    Client(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, LanceError>;
