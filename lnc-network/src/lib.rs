#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

pub mod compression;
mod connection;
mod frame;
mod protocol;
pub mod tls;

pub use compression::{CompressionAlgorithm, CompressionError, Compressor};
pub use connection::ConnectionBuffer;
pub use frame::{
    Frame, FrameType, encode_ack_bytes, encode_backpressure_bytes, encode_frame, parse_frame,
};
pub use protocol::{ControlCommand, IngestHeader, LwpFlags, LwpHeader, PROTOCOL_VERSION};
pub use tls::{TlsAcceptor, TlsConfig, TlsConnector, TlsError, TlsResult};

pub const LWP_HEADER_SIZE: usize = 44;
pub const KEEPALIVE_TIMEOUT_SECS: u64 = 30;
