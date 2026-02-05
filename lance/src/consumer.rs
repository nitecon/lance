//! Consumer read path implementation
//!
//! Provides rate-limited reading from segments for consumers.
//! Based on Architecture Section 17: Consumer Read Path (Zero-Copy Out).
//!
//! Key design principles:
//! - Zero-copy reads via mmap'd segments (per Architecture §17.3)
//! - Token bucket rate limiting (per Architecture §17.5)
//! - Read path metrics (per Architecture §17.7)

use bytes::Bytes;
use lnc_core::{LanceError, Result};
use lnc_io::ZeroCopyReader;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Rate limiter using token bucket algorithm
/// Prevents any single consumer from monopolizing resources.
pub struct ConsumerRateLimiter {
    /// Bytes per second limit
    rate_limit: u64,
    /// Token bucket state
    tokens: AtomicU64,
    /// Last refill timestamp in milliseconds
    last_refill: AtomicU64,
}

impl ConsumerRateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            rate_limit: bytes_per_second,
            tokens: AtomicU64::new(bytes_per_second), // Start with 1 second of tokens
            last_refill: AtomicU64::new(current_time_ms()),
        }
    }

    /// Try to consume tokens; returns how many bytes allowed
    pub fn try_consume(&self, requested: u64) -> u64 {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            let allowed = std::cmp::min(current, requested);

            if allowed == 0 {
                return 0;
            }

            if self
                .tokens
                .compare_exchange_weak(
                    current,
                    current - allowed,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return allowed;
            }
        }
    }

    /// Check if any tokens are available without consuming
    pub fn has_capacity(&self) -> bool {
        self.refill();
        self.tokens.load(Ordering::Relaxed) > 0
    }

    fn refill(&self) {
        let now = current_time_ms();
        let last = self.last_refill.load(Ordering::Relaxed);
        let elapsed_ms = now.saturating_sub(last);

        if elapsed_ms > 0 {
            let refill_amount = (self.rate_limit * elapsed_ms) / 1000;
            if refill_amount > 0 {
                self.tokens.fetch_add(refill_amount, Ordering::Relaxed);
                self.last_refill.store(now, Ordering::Relaxed);

                // Cap at 1 second worth of tokens (burst limit)
                let current = self.tokens.load(Ordering::Relaxed);
                if current > self.rate_limit {
                    self.tokens.store(self.rate_limit, Ordering::Relaxed);
                }
            }
        }
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Fetch request parameters
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub topic_id: u32,
    pub start_offset: u64,
    pub max_bytes: u32,
}

impl FetchRequest {
    pub fn parse(payload: &[u8]) -> Result<Self> {
        if payload.len() < 16 {
            return Err(LanceError::Protocol(
                "Fetch request payload too small".into(),
            ));
        }

        let topic_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let start_offset = u64::from_le_bytes([
            payload[4],
            payload[5],
            payload[6],
            payload[7],
            payload[8],
            payload[9],
            payload[10],
            payload[11],
        ]);
        let max_bytes = u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);

        Ok(Self {
            topic_id,
            start_offset,
            max_bytes,
        })
    }
}

/// Fetch response header
/// Format: next_offset(8) + bytes_returned(4) + record_count(4) + data...
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FetchResponse {
    pub next_offset: u64,
    pub bytes_returned: u32,
    pub record_count: u32,
    /// Zero-copy data via Bytes (refcount increment, no copy)
    pub data: Bytes,
}

impl FetchResponse {
    /// Create a new fetch response with zero-copy data
    pub fn new(next_offset: u64, record_count: u32, data: Bytes) -> Self {
        Self {
            next_offset,
            bytes_returned: data.len() as u32,
            record_count,
            data,
        }
    }

    pub fn empty(current_offset: u64) -> Self {
        Self {
            next_offset: current_offset,
            bytes_returned: 0,
            record_count: 0,
            data: Bytes::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.data.len());
        buf.extend_from_slice(&self.next_offset.to_le_bytes());
        buf.extend_from_slice(&self.bytes_returned.to_le_bytes());
        buf.extend_from_slice(&self.record_count.to_le_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }
}

/// Zero-copy read from a segment file using mmap'd memory
/// Returns Bytes (refcount increment, no copy on slice)
/// Per Architecture §17.3: Zero-Copy Send with io_uring
#[allow(dead_code)]
pub fn read_segment_zero_copy(
    segment_path: &Path,
    offset: u64,
    max_bytes: u32,
) -> Result<(Bytes, u64)> {
    let reader = ZeroCopyReader::open(segment_path)?;
    let segment_size = reader.size();

    if offset >= segment_size {
        return Ok((Bytes::new(), offset));
    }

    let available = segment_size - offset;
    let to_read = std::cmp::min(available, max_bytes as u64) as usize;

    // Try zero-copy path first (mmap on Linux)
    // Uses Arc-based mmap sharing - no data copy, only refcount increment
    #[cfg(target_os = "linux")]
    if reader.supports_zero_copy() {
        if let Some(data) = reader.slice_bytes(offset, to_read) {
            let next_offset = offset + to_read as u64;
            lnc_metrics::increment_zero_copy_sends();
            return Ok((data, next_offset));
        }
    }

    // Fallback: read into buffer
    let mut buf = vec![0u8; to_read];
    #[cfg(target_os = "linux")]
    let bytes_read = reader.read_at(offset, &mut buf)?;
    #[cfg(not(target_os = "linux"))]
    let bytes_read = {
        let mut reader = reader;
        reader.read_at(offset, &mut buf)?
    };
    buf.truncate(bytes_read);

    let next_offset = offset + bytes_read as u64;
    Ok((Bytes::from(buf), next_offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_basic() {
        let limiter = ConsumerRateLimiter::new(1000); // 1KB/s

        // Should be able to consume up to rate limit
        let consumed = limiter.try_consume(500);
        assert_eq!(consumed, 500);

        let consumed = limiter.try_consume(500);
        assert_eq!(consumed, 500);

        // Should be exhausted now (before refill)
        let consumed = limiter.try_consume(100);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn test_fetch_request_parse() {
        // Build a payload manually
        let mut payload = Vec::new();
        payload.extend_from_slice(&42u32.to_le_bytes());
        payload.extend_from_slice(&12345u64.to_le_bytes());
        payload.extend_from_slice(&65536u32.to_le_bytes());

        let parsed = FetchRequest::parse(&payload).unwrap();

        assert_eq!(parsed.topic_id, 42);
        assert_eq!(parsed.start_offset, 12345);
        assert_eq!(parsed.max_bytes, 65536);
    }

    #[test]
    fn test_fetch_response_encode() {
        let data = Bytes::from_static(b"test data payload");
        let resp = FetchResponse::new(1000, 5, data.clone());

        let encoded = resp.encode();

        // Verify header fields
        let next_offset = u64::from_le_bytes([
            encoded[0], encoded[1], encoded[2], encoded[3], encoded[4], encoded[5], encoded[6],
            encoded[7],
        ]);
        assert_eq!(next_offset, 1000);

        let bytes_returned = u32::from_le_bytes([encoded[8], encoded[9], encoded[10], encoded[11]]);
        assert_eq!(bytes_returned, data.len() as u32);

        assert_eq!(&encoded[16..], &data[..]);
    }

    #[test]
    fn test_fetch_response_zero_copy() {
        let data = Bytes::from_static(b"zero copy test");
        let resp = FetchResponse::new(2000, 1, data.clone());

        assert_eq!(resp.next_offset, 2000);
        assert_eq!(resp.record_count, 1);
        assert_eq!(resp.bytes_returned, 14);
        assert_eq!(resp.data, data);
    }
}
