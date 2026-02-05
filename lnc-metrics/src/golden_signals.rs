//! # 4 Golden Signals Metrics
//!
//! Implementation of Google SRE's 4 Golden Signals for monitoring:
//!
//! 1. **Latency** - Request duration with percentile tracking
//! 2. **Traffic** - Request rate and throughput
//! 3. **Errors** - Error counts and rates by type
//! 4. **Saturation** - Resource utilization gauges
//!
//! All metrics use lock-free atomic operations for zero-overhead collection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// =============================================================================
// LATENCY METRICS
// =============================================================================

/// Latency histogram bucket boundaries in microseconds.
/// Buckets: 100µs, 500µs, 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 5s, 10s, +Inf
const LATENCY_BUCKET_BOUNDS_US: [u64; 13] = [
    100,        // 100µs
    500,        // 500µs
    1_000,      // 1ms
    5_000,      // 5ms
    10_000,     // 10ms
    25_000,     // 25ms
    50_000,     // 50ms
    100_000,    // 100ms
    250_000,    // 250ms
    500_000,    // 500ms
    1_000_000,  // 1s
    5_000_000,  // 5s
    10_000_000, // 10s
];

/// Histogram for latency tracking with atomic buckets.
/// Provides lock-free percentile approximation.
pub struct LatencyHistogram {
    /// Counts per bucket (13 bounded + 1 overflow)
    buckets: [AtomicU64; 14],
    /// Total observations
    count: AtomicU64,
    /// Sum of all observations in microseconds
    sum_us: AtomicU64,
    /// Minimum observed value in microseconds
    min_us: AtomicU64,
    /// Maximum observed value in microseconds
    max_us: AtomicU64,
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            min_us: AtomicU64::new(u64::MAX),
            max_us: AtomicU64::new(0),
        }
    }

    /// Record a latency observation.
    #[inline]
    pub fn record(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.record_us(us);
    }

    /// Record a latency observation in microseconds.
    #[inline]
    pub fn record_us(&self, us: u64) {
        // Find bucket
        let bucket_idx = LATENCY_BUCKET_BOUNDS_US
            .iter()
            .position(|&bound| us <= bound)
            .unwrap_or(13); // overflow bucket

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);

        // Update min (using compare-exchange loop)
        let mut current_min = self.min_us.load(Ordering::Relaxed);
        while us < current_min {
            match self.min_us.compare_exchange_weak(
                current_min,
                us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }

        // Update max
        let mut current_max = self.max_us.load(Ordering::Relaxed);
        while us > current_max {
            match self.max_us.compare_exchange_weak(
                current_max,
                us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Get histogram snapshot for export.
    #[must_use]
    pub fn snapshot(&self) -> LatencySnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum_us = self.sum_us.load(Ordering::Relaxed);
        let min_us = self.min_us.load(Ordering::Relaxed);
        let max_us = self.max_us.load(Ordering::Relaxed);

        let mut buckets = [0u64; 14];
        for (i, bucket) in self.buckets.iter().enumerate() {
            buckets[i] = bucket.load(Ordering::Relaxed);
        }

        LatencySnapshot {
            count,
            sum_us,
            min_us: if min_us == u64::MAX { 0 } else { min_us },
            max_us,
            buckets,
        }
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of latency histogram for export.
#[derive(Debug, Clone)]
pub struct LatencySnapshot {
    pub count: u64,
    pub sum_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub buckets: [u64; 14],
}

impl LatencySnapshot {
    /// Calculate average latency in microseconds.
    #[must_use]
    pub fn avg_us(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_us as f64 / self.count as f64
        }
    }

    /// Estimate percentile (approximate due to bucketing).
    /// Returns value in microseconds.
    #[must_use]
    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 || !(0.0..=100.0).contains(&p) {
            return 0;
        }

        let target = ((p / 100.0) * self.count as f64).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, &bucket_count) in self.buckets.iter().enumerate() {
            cumulative += bucket_count;
            if cumulative >= target {
                // Return upper bound of this bucket
                return if i < 13 {
                    LATENCY_BUCKET_BOUNDS_US[i]
                } else {
                    self.max_us // overflow bucket returns max
                };
            }
        }

        self.max_us
    }

    /// Get p50 (median) in microseconds.
    #[must_use]
    pub fn p50(&self) -> u64 {
        self.percentile(50.0)
    }

    /// Get p95 in microseconds.
    #[must_use]
    pub fn p95(&self) -> u64 {
        self.percentile(95.0)
    }

    /// Get p99 in microseconds.
    #[must_use]
    pub fn p99(&self) -> u64 {
        self.percentile(99.0)
    }

    /// Get p999 in microseconds.
    #[must_use]
    pub fn p999(&self) -> u64 {
        self.percentile(99.9)
    }
}

/// RAII guard for timing an operation.
pub struct LatencyTimer<'a> {
    histogram: &'a LatencyHistogram,
    start: Instant,
}

impl<'a> LatencyTimer<'a> {
    pub fn new(histogram: &'a LatencyHistogram) -> Self {
        Self {
            histogram,
            start: Instant::now(),
        }
    }

    /// Manually stop the timer and record the duration.
    pub fn stop(self) {
        self.histogram.record(self.start.elapsed());
    }
}

impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        self.histogram.record(self.start.elapsed());
    }
}

// =============================================================================
// GLOBAL LATENCY HISTOGRAMS
// =============================================================================

/// Ingest request latency (client → ack)
pub static LATENCY_INGEST: LatencyHistogram = LatencyHistogram::new();

/// Fetch/read request latency  
pub static LATENCY_FETCH: LatencyHistogram = LatencyHistogram::new();

/// I/O operation latency (disk)
pub static LATENCY_IO: LatencyHistogram = LatencyHistogram::new();

/// Replication latency (leader → follower ack)
pub static LATENCY_REPLICATION: LatencyHistogram = LatencyHistogram::new();

/// Network round-trip latency
pub static LATENCY_NETWORK: LatencyHistogram = LatencyHistogram::new();

// =============================================================================
// TRAFFIC METRICS (already have counters, add rate tracking)
// =============================================================================

/// Rate calculator for tracking requests/bytes per second.
pub struct RateTracker {
    /// Last sample timestamp (milliseconds since epoch, approximated)
    last_sample_ms: AtomicU64,
    /// Counter value at last sample
    last_value: AtomicU64,
    /// Calculated rate (scaled by 1000 for precision)
    rate_scaled: AtomicU64,
}

impl RateTracker {
    pub const fn new() -> Self {
        Self {
            last_sample_ms: AtomicU64::new(0),
            last_value: AtomicU64::new(0),
            rate_scaled: AtomicU64::new(0),
        }
    }

    /// Update rate calculation based on current counter value.
    /// Call this periodically (e.g., every second).
    pub fn update(&self, current_value: u64, current_time_ms: u64) {
        let last_ms = self.last_sample_ms.load(Ordering::Relaxed);
        let last_val = self.last_value.load(Ordering::Relaxed);

        let elapsed_ms = current_time_ms.saturating_sub(last_ms);
        if elapsed_ms > 0 {
            let delta = current_value.saturating_sub(last_val);
            // Rate = delta / elapsed_seconds = (delta * 1000) / elapsed_ms
            // Scale by 1000 for precision
            let rate = (delta * 1_000_000) / elapsed_ms;
            self.rate_scaled.store(rate, Ordering::Relaxed);
        }

        self.last_sample_ms
            .store(current_time_ms, Ordering::Relaxed);
        self.last_value.store(current_value, Ordering::Relaxed);
    }

    /// Get current rate per second.
    #[must_use]
    pub fn rate_per_second(&self) -> f64 {
        self.rate_scaled.load(Ordering::Relaxed) as f64 / 1000.0
    }
}

impl Default for RateTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Request rate trackers
pub static RATE_INGEST_OPS: RateTracker = RateTracker::new();
pub static RATE_INGEST_BYTES: RateTracker = RateTracker::new();
pub static RATE_READ_OPS: RateTracker = RateTracker::new();
pub static RATE_READ_BYTES: RateTracker = RateTracker::new();

// =============================================================================
// ERROR METRICS
// =============================================================================

/// Error types for categorization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorType {
    /// Protocol/parsing errors
    Protocol = 0,
    /// I/O errors (disk, network)
    Io = 1,
    /// Timeout errors
    Timeout = 2,
    /// CRC/checksum failures
    Checksum = 3,
    /// Replication/quorum errors
    Replication = 4,
    /// Backpressure rejections
    Backpressure = 5,
    /// Authentication/authorization errors
    Auth = 6,
    /// Internal/unexpected errors
    Internal = 7,
}

impl ErrorType {
    pub const COUNT: usize = 8;

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Protocol => "protocol",
            Self::Io => "io",
            Self::Timeout => "timeout",
            Self::Checksum => "checksum",
            Self::Replication => "replication",
            Self::Backpressure => "backpressure",
            Self::Auth => "auth",
            Self::Internal => "internal",
        }
    }
}

/// Error counters by type
pub static ERRORS_BY_TYPE: [AtomicU64; ErrorType::COUNT] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Total errors counter
pub static ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Increment error counter by type.
#[inline]
pub fn record_error(error_type: ErrorType) {
    ERRORS_BY_TYPE[error_type as usize].fetch_add(1, Ordering::Relaxed);
    ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Get error count for specific type.
#[inline]
#[must_use]
pub fn get_error_count(error_type: ErrorType) -> u64 {
    ERRORS_BY_TYPE[error_type as usize].load(Ordering::Relaxed)
}

/// Get total error count.
#[inline]
#[must_use]
pub fn get_total_errors() -> u64 {
    ERRORS_TOTAL.load(Ordering::Relaxed)
}

// =============================================================================
// SATURATION METRICS
// =============================================================================

/// Resource saturation gauges
pub static SATURATION_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_QUEUE_CAPACITY: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_PENDING_IO: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_MEMORY_USED: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_MEMORY_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_BUFFER_POOL_USED: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_BUFFER_POOL_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_CONNECTIONS_USED: AtomicU64 = AtomicU64::new(0);
pub static SATURATION_CONNECTIONS_MAX: AtomicU64 = AtomicU64::new(0);

/// Set queue depth gauge.
#[inline]
pub fn set_queue_depth(current: u64, capacity: u64) {
    SATURATION_QUEUE_DEPTH.store(current, Ordering::Relaxed);
    SATURATION_QUEUE_CAPACITY.store(capacity, Ordering::Relaxed);
}

/// Set pending I/O operations gauge.
#[inline]
pub fn set_pending_io(count: u64) {
    SATURATION_PENDING_IO.store(count, Ordering::Relaxed);
}

/// Set memory usage gauge.
#[inline]
pub fn set_memory_usage(used: u64, total: u64) {
    SATURATION_MEMORY_USED.store(used, Ordering::Relaxed);
    SATURATION_MEMORY_TOTAL.store(total, Ordering::Relaxed);
}

/// Set buffer pool usage gauge.
#[inline]
pub fn set_buffer_pool_usage(used: u64, total: u64) {
    SATURATION_BUFFER_POOL_USED.store(used, Ordering::Relaxed);
    SATURATION_BUFFER_POOL_TOTAL.store(total, Ordering::Relaxed);
}

/// Set connection pool usage gauge.
#[inline]
pub fn set_connection_usage(used: u64, max: u64) {
    SATURATION_CONNECTIONS_USED.store(used, Ordering::Relaxed);
    SATURATION_CONNECTIONS_MAX.store(max, Ordering::Relaxed);
}

/// Calculate saturation ratio (0.0 - 1.0) for a resource.
#[inline]
#[must_use]
pub fn saturation_ratio(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 / total as f64
    }
}

/// Get queue saturation ratio.
#[inline]
#[must_use]
pub fn queue_saturation() -> f64 {
    saturation_ratio(
        SATURATION_QUEUE_DEPTH.load(Ordering::Relaxed),
        SATURATION_QUEUE_CAPACITY.load(Ordering::Relaxed),
    )
}

/// Get memory saturation ratio.
#[inline]
#[must_use]
pub fn memory_saturation() -> f64 {
    saturation_ratio(
        SATURATION_MEMORY_USED.load(Ordering::Relaxed),
        SATURATION_MEMORY_TOTAL.load(Ordering::Relaxed),
    )
}

/// Get buffer pool saturation ratio.
#[inline]
#[must_use]
pub fn buffer_pool_saturation() -> f64 {
    saturation_ratio(
        SATURATION_BUFFER_POOL_USED.load(Ordering::Relaxed),
        SATURATION_BUFFER_POOL_TOTAL.load(Ordering::Relaxed),
    )
}

// =============================================================================
// GOLDEN SIGNALS SNAPSHOT
// =============================================================================

/// Complete snapshot of all 4 Golden Signals.
#[derive(Debug, Clone)]
pub struct GoldenSignalsSnapshot {
    // Latency
    pub latency_ingest: LatencySnapshot,
    pub latency_fetch: LatencySnapshot,
    pub latency_io: LatencySnapshot,
    pub latency_replication: LatencySnapshot,
    pub latency_network: LatencySnapshot,

    // Traffic (rates per second)
    pub traffic_ingest_ops_per_sec: f64,
    pub traffic_ingest_bytes_per_sec: f64,
    pub traffic_read_ops_per_sec: f64,
    pub traffic_read_bytes_per_sec: f64,

    // Errors
    pub errors_total: u64,
    pub errors_by_type: [u64; ErrorType::COUNT],

    // Saturation (ratios 0.0-1.0)
    pub saturation_queue: f64,
    pub saturation_memory: f64,
    pub saturation_buffer_pool: f64,
    pub saturation_connections: f64,
    pub saturation_pending_io: u64,
}

impl GoldenSignalsSnapshot {
    /// Capture current state of all Golden Signals.
    #[must_use]
    pub fn capture() -> Self {
        let mut errors_by_type = [0u64; ErrorType::COUNT];
        for (i, counter) in ERRORS_BY_TYPE.iter().enumerate() {
            errors_by_type[i] = counter.load(Ordering::Relaxed);
        }

        Self {
            // Latency
            latency_ingest: LATENCY_INGEST.snapshot(),
            latency_fetch: LATENCY_FETCH.snapshot(),
            latency_io: LATENCY_IO.snapshot(),
            latency_replication: LATENCY_REPLICATION.snapshot(),
            latency_network: LATENCY_NETWORK.snapshot(),

            // Traffic
            traffic_ingest_ops_per_sec: RATE_INGEST_OPS.rate_per_second(),
            traffic_ingest_bytes_per_sec: RATE_INGEST_BYTES.rate_per_second(),
            traffic_read_ops_per_sec: RATE_READ_OPS.rate_per_second(),
            traffic_read_bytes_per_sec: RATE_READ_BYTES.rate_per_second(),

            // Errors
            errors_total: ERRORS_TOTAL.load(Ordering::Relaxed),
            errors_by_type,

            // Saturation
            saturation_queue: queue_saturation(),
            saturation_memory: memory_saturation(),
            saturation_buffer_pool: buffer_pool_saturation(),
            saturation_connections: saturation_ratio(
                SATURATION_CONNECTIONS_USED.load(Ordering::Relaxed),
                SATURATION_CONNECTIONS_MAX.load(Ordering::Relaxed),
            ),
            saturation_pending_io: SATURATION_PENDING_IO.load(Ordering::Relaxed),
        }
    }

    /// Check if any signal is in warning state.
    #[must_use]
    pub fn has_warnings(&self) -> bool {
        // Latency warning: p99 > 100ms
        self.latency_ingest.p99() > 100_000 ||
        self.latency_fetch.p99() > 100_000 ||
        // Saturation warning: > 70%
        self.saturation_queue > 0.7 ||
        self.saturation_memory > 0.7 ||
        self.saturation_buffer_pool > 0.7 ||
        self.saturation_connections > 0.7
    }

    /// Check if any signal is in critical state.
    #[must_use]
    pub fn has_critical(&self) -> bool {
        // Latency critical: p99 > 1s
        self.latency_ingest.p99() > 1_000_000 ||
        self.latency_fetch.p99() > 1_000_000 ||
        // Saturation critical: > 90%
        self.saturation_queue > 0.9 ||
        self.saturation_memory > 0.9 ||
        self.saturation_buffer_pool > 0.9 ||
        self.saturation_connections > 0.9
    }
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

/// Record ingest latency.
#[inline]
pub fn record_ingest_latency(duration: Duration) {
    LATENCY_INGEST.record(duration);
}

/// Record fetch latency.
#[inline]
pub fn record_fetch_latency(duration: Duration) {
    LATENCY_FETCH.record(duration);
}

/// Record I/O latency.
#[inline]
pub fn record_io_latency(duration: Duration) {
    LATENCY_IO.record(duration);
}

/// Record replication latency.
#[inline]
pub fn record_replication_latency(duration: Duration) {
    LATENCY_REPLICATION.record(duration);
}

/// Record network latency.
#[inline]
pub fn record_network_latency(duration: Duration) {
    LATENCY_NETWORK.record(duration);
}

/// Start timing an ingest operation.
#[inline]
#[must_use]
pub fn time_ingest() -> LatencyTimer<'static> {
    LatencyTimer::new(&LATENCY_INGEST)
}

/// Start timing a fetch operation.
#[inline]
#[must_use]
pub fn time_fetch() -> LatencyTimer<'static> {
    LatencyTimer::new(&LATENCY_FETCH)
}

/// Start timing an I/O operation.
#[inline]
#[must_use]
pub fn time_io() -> LatencyTimer<'static> {
    LatencyTimer::new(&LATENCY_IO)
}

// =============================================================================
// SAMPLING FOR HOT PATH (USE THESE ON INGEST/FETCH HOT PATHS)
// =============================================================================

/// Sample rate for hot-path latency tracking (1 in N).
/// Set to 0 to disable sampling (record every observation).
pub static LATENCY_SAMPLE_RATE: AtomicU64 = AtomicU64::new(100);

// Counter for sampling - uses thread-local to avoid contention.
thread_local! {
    static SAMPLE_COUNTER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Check if this observation should be sampled (for hot paths).
/// Returns true approximately 1 in N times based on LATENCY_SAMPLE_RATE.
#[inline]
pub fn should_sample() -> bool {
    let rate = LATENCY_SAMPLE_RATE.load(Ordering::Relaxed);
    if rate == 0 {
        return true; // Sampling disabled, record everything
    }

    SAMPLE_COUNTER.with(|counter| {
        let current = counter.get();
        counter.set(current.wrapping_add(1));
        current % rate == 0
    })
}

/// Set the sampling rate for latency tracking.
/// - 0 = record every observation (high overhead on hot path)
/// - 100 = record 1 in 100 (default, ~1% overhead)
/// - 1000 = record 1 in 1000 (~0.1% overhead)
#[inline]
pub fn set_latency_sample_rate(rate: u64) {
    LATENCY_SAMPLE_RATE.store(rate, Ordering::Relaxed);
}

/// Record latency with sampling (safe for hot path).
/// Only records if sampled, avoiding overhead on most calls.
#[inline]
pub fn record_ingest_latency_sampled(duration: Duration) {
    if should_sample() {
        LATENCY_INGEST.record(duration);
    }
}

/// Record fetch latency with sampling (safe for hot path).
#[inline]
pub fn record_fetch_latency_sampled(duration: Duration) {
    if should_sample() {
        LATENCY_FETCH.record(duration);
    }
}

/// Record I/O latency with sampling (safe for hot path).
#[inline]
pub fn record_io_latency_sampled(duration: Duration) {
    if should_sample() {
        LATENCY_IO.record(duration);
    }
}

/// Sampled timer that only measures if sampled.
/// Avoids Instant::now() syscall overhead when not sampling.
pub struct SampledTimer {
    start: Option<Instant>,
    histogram: &'static LatencyHistogram,
}

impl SampledTimer {
    /// Create a sampled timer - only starts timing if sampled.
    #[inline]
    pub fn new(histogram: &'static LatencyHistogram) -> Self {
        let start = if should_sample() {
            Some(Instant::now())
        } else {
            None
        };
        Self { start, histogram }
    }

    /// Check if this timer is actually measuring.
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.start.is_some()
    }
}

impl Drop for SampledTimer {
    #[inline]
    fn drop(&mut self) {
        if let Some(start) = self.start {
            self.histogram.record(start.elapsed());
        }
    }
}

/// Start a sampled ingest timer (SAFE FOR HOT PATH).
/// Only incurs Instant::now() overhead ~1% of the time (default rate=100).
#[inline]
#[must_use]
pub fn time_ingest_sampled() -> SampledTimer {
    SampledTimer::new(&LATENCY_INGEST)
}

/// Start a sampled fetch timer (SAFE FOR HOT PATH).
#[inline]
#[must_use]
pub fn time_fetch_sampled() -> SampledTimer {
    SampledTimer::new(&LATENCY_FETCH)
}

/// Start a sampled I/O timer (SAFE FOR HOT PATH).
#[inline]
#[must_use]
pub fn time_io_sampled() -> SampledTimer {
    SampledTimer::new(&LATENCY_IO)
}

/// Update rate trackers - call periodically (e.g., every second).
pub fn update_rates(
    ingest_ops: u64,
    ingest_bytes: u64,
    read_ops: u64,
    read_bytes: u64,
    current_time_ms: u64,
) {
    RATE_INGEST_OPS.update(ingest_ops, current_time_ms);
    RATE_INGEST_BYTES.update(ingest_bytes, current_time_ms);
    RATE_READ_OPS.update(read_ops, current_time_ms);
    RATE_READ_BYTES.update(read_bytes, current_time_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_histogram() {
        let hist = LatencyHistogram::new();

        // Record various latencies
        hist.record_us(50); // 100µs bucket
        hist.record_us(200); // 500µs bucket
        hist.record_us(1500); // 5ms bucket
        hist.record_us(50000); // 50ms bucket
        hist.record_us(150000); // 250ms bucket

        let snap = hist.snapshot();
        assert_eq!(snap.count, 5);
        assert_eq!(snap.min_us, 50);
        assert_eq!(snap.max_us, 150000);
    }

    #[test]
    fn test_percentile_calculation() {
        let hist = LatencyHistogram::new();

        // Add 100 samples in first bucket
        for _ in 0..100 {
            hist.record_us(50);
        }

        let snap = hist.snapshot();
        assert_eq!(snap.p50(), 100); // Upper bound of first bucket
        assert_eq!(snap.p99(), 100);
    }

    #[test]
    fn test_error_tracking() {
        record_error(ErrorType::Io);
        record_error(ErrorType::Io);
        record_error(ErrorType::Timeout);

        assert_eq!(get_error_count(ErrorType::Io), 2);
        assert_eq!(get_error_count(ErrorType::Timeout), 1);
        assert_eq!(get_total_errors(), 3);
    }

    #[test]
    fn test_saturation_ratio() {
        assert!((saturation_ratio(50, 100) - 0.5).abs() < f64::EPSILON);
        assert!((saturation_ratio(0, 100) - 0.0).abs() < f64::EPSILON);
        assert!((saturation_ratio(100, 100) - 1.0).abs() < f64::EPSILON);
        assert!((saturation_ratio(0, 0) - 0.0).abs() < f64::EPSILON);
    }
}
