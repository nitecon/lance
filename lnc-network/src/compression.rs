//! Compression support for LANCE payloads
//!
//! Provides LZ4 and ZSTD compression for network payloads.
//! Compression is indicated by the `Compressed` flag in the LWP header.
//!
//! # Compression Format
//!
//! Compressed payloads have a 5-byte header:
//! - Byte 0: Compression algorithm (0x01 = LZ4, 0x02 = ZSTD)
//! - Bytes 1-4: Original uncompressed size (little-endian u32)
//!
//! # Usage
//!
//! ```rust,ignore
//! use lnc_network::compression::{Compressor, CompressionAlgorithm};
//!
//! let compressor = Compressor::new(CompressionAlgorithm::Lz4, 1);
//! let compressed = compressor.compress(&data)?;
//! let decompressed = compressor.decompress(&compressed)?;
//! ```

use bytes::{Bytes, BytesMut};

/// Compression algorithm identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    /// No compression
    None = 0x00,
    /// LZ4 fast compression
    Lz4 = 0x01,
    /// ZSTD compression (better ratio)
    Zstd = 0x02,
}

impl From<u8> for CompressionAlgorithm {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Lz4,
            0x02 => Self::Zstd,
            _ => Self::None,
        }
    }
}

impl CompressionAlgorithm {
    /// Check if this algorithm is available
    pub fn is_available(&self) -> bool {
        match self {
            Self::None => true,
            Self::Lz4 => cfg!(feature = "lz4"),
            Self::Zstd => cfg!(feature = "zstd"),
        }
    }
}

/// Compression error types
#[derive(Debug, Clone, PartialEq)]
pub enum CompressionError {
    /// Compression failed
    CompressionFailed(String),
    /// Decompression failed
    DecompressionFailed(String),
    /// Invalid compressed data header
    InvalidHeader,
    /// Unsupported algorithm
    UnsupportedAlgorithm(u8),
    /// Decompressed size exceeds limit
    SizeExceeded { actual: usize, limit: usize },
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CompressionFailed(msg) => write!(f, "Compression failed: {}", msg),
            Self::DecompressionFailed(msg) => write!(f, "Decompression failed: {}", msg),
            Self::InvalidHeader => write!(f, "Invalid compression header"),
            Self::UnsupportedAlgorithm(alg) => write!(f, "Unsupported algorithm: 0x{:02x}", alg),
            Self::SizeExceeded { actual, limit } => {
                write!(f, "Decompressed size {} exceeds limit {}", actual, limit)
            },
        }
    }
}

impl std::error::Error for CompressionError {}

/// Result type for compression operations
pub type CompressionResult<T> = std::result::Result<T, CompressionError>;

/// Compression header size (algorithm byte + original size)
pub const COMPRESSION_HEADER_SIZE: usize = 5;

/// Maximum decompressed size (16 MB default)
pub const MAX_DECOMPRESSED_SIZE: usize = 16 * 1024 * 1024;

/// Compressor for LANCE payloads
///
/// NOTE: Full implementation requires lz4 and zstd crate dependencies.
/// This provides the API structure and header format.
#[derive(Debug, Clone)]
pub struct Compressor {
    algorithm: CompressionAlgorithm,
    /// Compression level (used by ZSTD, ignored by LZ4)
    #[cfg(feature = "zstd")]
    level: i32,
    max_decompressed_size: usize,
}

impl Compressor {
    /// Create a new compressor with the specified algorithm and level
    #[cfg(feature = "zstd")]
    pub fn new(algorithm: CompressionAlgorithm, level: i32) -> Self {
        Self {
            algorithm,
            level,
            max_decompressed_size: MAX_DECOMPRESSED_SIZE,
        }
    }

    /// Create a new compressor with the specified algorithm
    #[cfg(not(feature = "zstd"))]
    pub fn new(algorithm: CompressionAlgorithm, _level: i32) -> Self {
        Self {
            algorithm,
            max_decompressed_size: MAX_DECOMPRESSED_SIZE,
        }
    }

    /// Create an LZ4 compressor (fast, moderate compression)
    pub fn lz4() -> Self {
        Self::new(CompressionAlgorithm::Lz4, 1)
    }

    /// Create a ZSTD compressor (slower, better compression)
    #[cfg(feature = "zstd")]
    pub fn zstd(level: i32) -> Self {
        Self::new(CompressionAlgorithm::Zstd, level.clamp(1, 22))
    }

    /// Create a ZSTD compressor - requires zstd feature
    #[cfg(not(feature = "zstd"))]
    pub fn zstd(_level: i32) -> Self {
        Self::new(CompressionAlgorithm::Zstd, 0)
    }

    /// Set the maximum allowed decompressed size
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_decompressed_size = max_size;
        self
    }

    /// Get the compression algorithm
    pub fn algorithm(&self) -> CompressionAlgorithm {
        self.algorithm
    }

    /// Compress data and prepend compression header
    pub fn compress(&self, data: &[u8]) -> CompressionResult<Bytes> {
        if self.algorithm == CompressionAlgorithm::None {
            return Ok(Bytes::copy_from_slice(data));
        }

        // For now, return a placeholder since we don't have lz4/zstd deps
        // Real implementation would call the compression library
        let compressed = self.compress_raw(data)?;

        // Prepend header: [algorithm, original_size_le32]
        let mut result = BytesMut::with_capacity(COMPRESSION_HEADER_SIZE + compressed.len());
        result.extend_from_slice(&[self.algorithm as u8]);
        result.extend_from_slice(&(data.len() as u32).to_le_bytes());
        result.extend_from_slice(&compressed);

        Ok(result.freeze())
    }

    /// Decompress data with header
    pub fn decompress(&self, data: &[u8]) -> CompressionResult<Bytes> {
        if data.len() < COMPRESSION_HEADER_SIZE {
            return Err(CompressionError::InvalidHeader);
        }

        let algorithm = CompressionAlgorithm::from(data[0]);
        if algorithm == CompressionAlgorithm::None {
            return Ok(Bytes::copy_from_slice(&data[COMPRESSION_HEADER_SIZE..]));
        }

        let original_size = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;

        if original_size > self.max_decompressed_size {
            return Err(CompressionError::SizeExceeded {
                actual: original_size,
                limit: self.max_decompressed_size,
            });
        }

        let compressed_data = &data[COMPRESSION_HEADER_SIZE..];
        self.decompress_raw(algorithm, compressed_data, original_size)
    }

    /// Raw compression without header.
    pub fn compress_raw(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.into()),
            CompressionAlgorithm::Lz4 => self.compress_lz4(data),
            CompressionAlgorithm::Zstd => self.compress_zstd(data),
        }
    }

    #[cfg(feature = "lz4")]
    fn compress_lz4(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    #[cfg(not(feature = "lz4"))]
    fn compress_lz4(&self, _data: &[u8]) -> CompressionResult<Vec<u8>> {
        Err(CompressionError::UnsupportedAlgorithm(self.algorithm as u8))
    }

    #[cfg(feature = "zstd")]
    fn compress_zstd(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        zstd::encode_all(std::io::Cursor::new(data), self.level)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))
    }

    #[cfg(not(feature = "zstd"))]
    fn compress_zstd(&self, _data: &[u8]) -> CompressionResult<Vec<u8>> {
        Err(CompressionError::UnsupportedAlgorithm(self.algorithm as u8))
    }

    /// Raw decompression without header.
    pub fn decompress_raw(
        &self,
        algorithm: CompressionAlgorithm,
        data: &[u8],
        _original_size: usize,
    ) -> CompressionResult<Bytes> {
        match algorithm {
            CompressionAlgorithm::None => Ok(Bytes::copy_from_slice(data)),
            CompressionAlgorithm::Lz4 => Self::decompress_lz4(data),
            CompressionAlgorithm::Zstd => Self::decompress_zstd(data),
        }
    }

    #[cfg(feature = "lz4")]
    fn decompress_lz4(data: &[u8]) -> CompressionResult<Bytes> {
        lz4_flex::decompress_size_prepended(data)
            .map(Bytes::from)
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
    }

    #[cfg(not(feature = "lz4"))]
    fn decompress_lz4(_data: &[u8]) -> CompressionResult<Bytes> {
        Err(CompressionError::UnsupportedAlgorithm(
            CompressionAlgorithm::Lz4 as u8,
        ))
    }

    #[cfg(feature = "zstd")]
    fn decompress_zstd(data: &[u8]) -> CompressionResult<Bytes> {
        zstd::decode_all(std::io::Cursor::new(data))
            .map(Bytes::from)
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
    }

    #[cfg(not(feature = "zstd"))]
    fn decompress_zstd(_data: &[u8]) -> CompressionResult<Bytes> {
        Err(CompressionError::UnsupportedAlgorithm(
            CompressionAlgorithm::Zstd as u8,
        ))
    }

    /// Check if compression would be beneficial for this data size
    pub fn should_compress(&self, data_len: usize) -> bool {
        // Don't compress small payloads (overhead > benefit)
        data_len >= 256
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::None, 0)
    }
}

/// Parse compression header from data
pub fn parse_compression_header(data: &[u8]) -> Option<(CompressionAlgorithm, usize)> {
    if data.len() < COMPRESSION_HEADER_SIZE {
        return None;
    }

    let algorithm = CompressionAlgorithm::from(data[0]);
    let original_size = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;

    Some((algorithm, original_size))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::len_zero)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_from_u8() {
        assert_eq!(CompressionAlgorithm::from(0x00), CompressionAlgorithm::None);
        assert_eq!(CompressionAlgorithm::from(0x01), CompressionAlgorithm::Lz4);
        assert_eq!(CompressionAlgorithm::from(0x02), CompressionAlgorithm::Zstd);
        assert_eq!(CompressionAlgorithm::from(0xFF), CompressionAlgorithm::None);
    }

    #[test]
    fn test_compressor_creation() {
        let lz4 = Compressor::lz4();
        assert_eq!(lz4.algorithm(), CompressionAlgorithm::Lz4);

        let zstd = Compressor::zstd(3);
        assert_eq!(zstd.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_should_compress() {
        let compressor = Compressor::lz4();
        assert!(!compressor.should_compress(100));
        assert!(compressor.should_compress(1000));
    }

    #[test]
    fn test_none_compressor_passthrough() {
        let compressor = Compressor::default();
        let data = b"test data";
        let result = compressor.compress(data);
        assert!(result.is_ok());
        assert_eq!(result.as_ref().map(|b| b.as_ref()), Ok(data.as_slice()));
    }

    #[test]
    fn test_parse_header() {
        let header = [0x01, 0x00, 0x10, 0x00, 0x00]; // LZ4, 4096 bytes
        let (alg, size) = parse_compression_header(&header).unwrap();
        assert_eq!(alg, CompressionAlgorithm::Lz4);
        assert_eq!(size, 4096);
    }

    #[test]
    fn test_invalid_header() {
        let compressor = Compressor::lz4();
        let result = compressor.decompress(&[0x01, 0x02]); // Too short
        assert!(matches!(result, Err(CompressionError::InvalidHeader)));
    }

    #[test]
    fn test_size_exceeded() {
        let compressor = Compressor::lz4().with_max_size(100);
        let mut data = vec![0x01]; // LZ4
        data.extend_from_slice(&1000u32.to_le_bytes()); // 1000 bytes original
        data.extend_from_slice(&[0u8; 10]); // Some "compressed" data

        let result = compressor.decompress(&data);
        assert!(matches!(
            result,
            Err(CompressionError::SizeExceeded {
                actual: 1000,
                limit: 100
            })
        ));
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_lz4_compress_decompress_roundtrip() {
        let compressor = Compressor::lz4();
        let original = b"Hello, this is a test payload for LZ4 compression roundtrip testing!";

        let compressed = compressor.compress(original).expect("compression failed");
        assert!(compressed.len() > 0, "compressed data should not be empty");

        let decompressed = compressor
            .decompress(&compressed)
            .expect("decompression failed");
        assert_eq!(
            decompressed.as_ref(),
            original,
            "roundtrip should preserve data"
        );
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_compress_decompress_roundtrip() {
        let compressor = Compressor::zstd(3);
        let original = b"Hello, this is a test payload for ZSTD compression roundtrip testing!";

        let compressed = compressor.compress(original).expect("compression failed");
        assert!(compressed.len() > 0, "compressed data should not be empty");

        let decompressed = compressor
            .decompress(&compressed)
            .expect("decompression failed");
        assert_eq!(
            decompressed.as_ref(),
            original,
            "roundtrip should preserve data"
        );
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_lz4_large_payload() {
        let compressor = Compressor::lz4();
        // Create a 64KB payload with repeating pattern (compresses well)
        let original: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();

        let compressed = compressor.compress(&original).expect("compression failed");
        // LZ4 should achieve significant compression on repetitive data
        assert!(
            compressed.len() < original.len(),
            "LZ4 should compress repetitive data"
        );

        let decompressed = compressor
            .decompress(&compressed)
            .expect("decompression failed");
        assert_eq!(decompressed.as_ref(), original.as_slice());
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_large_payload() {
        let compressor = Compressor::zstd(3);
        // Create a 64KB payload with repeating pattern (compresses well)
        let original: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();

        let compressed = compressor.compress(&original).expect("compression failed");
        // ZSTD should achieve significant compression on repetitive data
        assert!(
            compressed.len() < original.len(),
            "ZSTD should compress repetitive data"
        );

        let decompressed = compressor
            .decompress(&compressed)
            .expect("decompression failed");
        assert_eq!(decompressed.as_ref(), original.as_slice());
    }

    /// Benchmark: Measure allocation overhead for CompressionAlgorithm::None
    ///
    /// This test measures the cost of the data copy in compress_raw()
    /// when compression is disabled. This is important for understanding the
    /// performance impact on the hot path when compression is not used.
    #[test]
    fn benchmark_allocation_overhead_none_compression() {
        use std::time::Instant;

        let compressor = Compressor::default(); // None algorithm

        // Test various payload sizes typical in LANCE
        let sizes = [1024, 4096, 16384, 65536, 262144]; // 1KB, 4KB, 16KB, 64KB, 256KB
        let iterations = 1000;

        println!("\n=== Allocation Overhead Benchmark (CompressionAlgorithm::None) ===");
        println!(
            "{:<12} {:>12} {:>12} {:>12}",
            "Size", "Total (µs)", "Per-op (ns)", "Throughput"
        );
        println!("{}", "-".repeat(52));

        for size in sizes {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

            let start = Instant::now();
            for _ in 0..iterations {
                let _ = compressor.compress(&data);
            }
            let elapsed = start.elapsed();

            let total_us = elapsed.as_micros();
            let per_op_ns = elapsed.as_nanos() / iterations as u128;
            let throughput_mbps =
                (size as f64 * iterations as f64) / elapsed.as_secs_f64() / 1_000_000.0;

            println!(
                "{:<12} {:>12} {:>12} {:>10.2} MB/s",
                format!("{}B", size),
                total_us,
                per_op_ns,
                throughput_mbps
            );

            // Verify the copy happened correctly
            let result = compressor.compress(&data).unwrap();
            assert_eq!(result.len(), data.len());
        }

        println!("\nNote: data copy creates a full allocation. Consider Bytes::copy_from_slice()");
        println!("or Cow<[u8]> if zero-copy passthrough is needed on hot path.\n");
    }
}
