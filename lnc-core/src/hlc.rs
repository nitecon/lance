//! Hybrid Logical Clock (HLC) implementation for LANCE.
//!
//! HLC provides causally consistent timestamps that remain close to physical wall-clock
//! time while handling clock drift, NTP kinks, and distributed ordering.
//!
//! # Structure
//!
//! An HLC timestamp is a 64-bit value packed as:
//! - **44 bits**: Physical time in milliseconds since Unix epoch (~557 years)
//! - **20 bits**: Logical counter (1M events per millisecond per node)
//!
//! # Guarantees
//!
//! - **Monotonicity**: Local timestamps always increase
//! - **Causality**: If A happened-before B, then HLC(A) < HLC(B)
//! - **Physical closeness**: HLC stays within bounded drift of wall-clock time

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use zerocopy::{FromBytes, IntoBytes};

/// Hybrid Logical Clock timestamp (64-bit packed).
///
/// Layout: `[physical_ms: 44 bits][logical: 20 bits]`
///
/// - 44 bits physical = ~557 years from epoch (sufficient until 2527)
/// - 20 bits logical = 1M events per millisecond per node
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, IntoBytes, FromBytes,
)]
#[repr(transparent)]
pub struct HlcTimestamp(u64);

impl HlcTimestamp {
    /// Number of bits allocated to the logical counter.
    const LOGICAL_BITS: u32 = 20;

    /// Mask for extracting the logical counter.
    const LOGICAL_MASK: u64 = (1 << Self::LOGICAL_BITS) - 1;

    /// Maximum value for the logical counter.
    pub const MAX_LOGICAL: u32 = (1 << Self::LOGICAL_BITS) - 1;

    /// Create a new HLC timestamp from physical time (ms) and logical counter.
    ///
    /// # Panics
    ///
    /// Debug builds will panic if `logical >= 2^20`.
    #[inline]
    #[must_use]
    pub const fn new(physical_ms: u64, logical: u32) -> Self {
        debug_assert!(logical < (1 << Self::LOGICAL_BITS));
        Self((physical_ms << Self::LOGICAL_BITS) | (logical as u64))
    }

    /// Create an HLC timestamp from a raw u64 value.
    #[inline]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Create a zero (minimum) HLC timestamp.
    #[inline]
    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    /// Create a maximum HLC timestamp.
    #[inline]
    #[must_use]
    pub const fn max() -> Self {
        Self(u64::MAX)
    }

    /// Extract the physical time component in milliseconds.
    #[inline]
    #[must_use]
    pub const fn physical_ms(&self) -> u64 {
        self.0 >> Self::LOGICAL_BITS
    }

    /// Extract the logical counter component.
    #[inline]
    #[must_use]
    pub const fn logical(&self) -> u32 {
        (self.0 & Self::LOGICAL_MASK) as u32
    }

    /// Get the raw u64 representation.
    #[inline]
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    /// Convert to nanoseconds (approximate, for display/logging only).
    ///
    /// Note: This loses precision since HLC uses milliseconds internally.
    #[inline]
    #[must_use]
    pub const fn to_nanos_approx(&self) -> u64 {
        self.physical_ms() * 1_000_000
    }
}

impl std::fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HLC({}.{})", self.physical_ms(), self.logical())
    }
}

/// Hybrid Logical Clock for generating causally consistent timestamps.
///
/// Thread-safe via atomic operations. Each node should have one HLC instance.
///
/// # Example
///
/// ```
/// use lnc_core::HybridLogicalClock;
///
/// let hlc = HybridLogicalClock::new(1);
/// let ts1 = hlc.now();
/// let ts2 = hlc.now();
/// assert!(ts1 < ts2);
/// ```
pub struct HybridLogicalClock {
    /// Last known HLC timestamp (atomic for thread safety).
    last: AtomicU64,
    /// Node ID for identification (not part of HLC, used externally).
    node_id: u16,
}

impl HybridLogicalClock {
    /// Maximum drift allowed before warning (milliseconds).
    pub const MAX_DRIFT_WARNING_MS: u64 = 100;

    /// Maximum drift allowed before critical alert (milliseconds).
    pub const MAX_DRIFT_CRITICAL_MS: u64 = 1000;

    /// Create a new HLC instance for a given node.
    #[must_use]
    pub fn new(node_id: u16) -> Self {
        Self {
            last: AtomicU64::new(0),
            node_id,
        }
    }

    /// Get the node ID associated with this clock.
    #[inline]
    #[must_use]
    pub const fn node_id(&self) -> u16 {
        self.node_id
    }

    /// Generate a new HLC timestamp for a local event.
    ///
    /// This method is lock-free and thread-safe via CAS operations.
    ///
    /// # Algorithm
    ///
    /// 1. If wall clock > last physical: use wall clock, reset logical to 0
    /// 2. Otherwise: keep physical, increment logical
    /// 3. CAS to ensure monotonicity under concurrent access
    pub fn now(&self) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();

        loop {
            let last = HlcTimestamp(self.last.load(Ordering::Acquire));
            let last_physical = last.physical_ms();
            let last_logical = last.logical();

            let (new_physical, new_logical) = if wall_ms > last_physical {
                // Wall clock advanced: reset logical counter
                (wall_ms, 0)
            } else {
                // Wall clock unchanged or behind: increment logical
                if last_logical >= HlcTimestamp::MAX_LOGICAL {
                    // Logical counter exhausted: spin until wall clock advances
                    // This should be extremely rare (1M events/ms)
                    std::hint::spin_loop();
                    continue;
                }
                (last_physical, last_logical + 1)
            };

            let new_ts = HlcTimestamp::new(new_physical, new_logical);

            // CAS to ensure monotonicity under concurrency
            if self
                .last
                .compare_exchange_weak(last.0, new_ts.0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_ts;
            }
            // CAS failed: another thread updated; retry
        }
    }

    /// Update HLC upon receiving a message with a remote timestamp.
    ///
    /// Returns a new timestamp that is greater than both the local clock
    /// and the remote timestamp, ensuring causal ordering.
    ///
    /// # Algorithm
    ///
    /// 1. If wall clock > max(local, remote): use wall clock, reset logical
    /// 2. If local > remote: use local physical, increment local logical
    /// 3. If remote > local: use remote physical, increment remote logical
    /// 4. If equal physical: use that physical, increment max logical
    pub fn receive(&self, remote_ts: HlcTimestamp) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();

        loop {
            let last = HlcTimestamp(self.last.load(Ordering::Acquire));

            let (new_physical, new_logical) =
                if wall_ms > last.physical_ms() && wall_ms > remote_ts.physical_ms() {
                    // Wall clock is ahead of both: use wall clock, reset logical
                    (wall_ms, 0)
                } else if last.physical_ms() > remote_ts.physical_ms() {
                    // Local is ahead: increment local logical
                    (last.physical_ms(), last.logical().saturating_add(1))
                } else if remote_ts.physical_ms() > last.physical_ms() {
                    // Remote is ahead: adopt remote physical, increment remote logical
                    (
                        remote_ts.physical_ms(),
                        remote_ts.logical().saturating_add(1),
                    )
                } else {
                    // Same physical time: use max logical + 1
                    let max_logical = last.logical().max(remote_ts.logical());
                    (last.physical_ms(), max_logical.saturating_add(1))
                };

            // Clamp logical to max value (shouldn't happen in practice)
            let new_logical = new_logical.min(HlcTimestamp::MAX_LOGICAL);
            let new_ts = HlcTimestamp::new(new_physical, new_logical);

            if self
                .last
                .compare_exchange_weak(last.0, new_ts.0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_ts;
            }
        }
    }

    /// Get the current HLC timestamp without advancing it.
    ///
    /// Useful for reading the last known time without generating a new event.
    #[inline]
    #[must_use]
    pub fn current(&self) -> HlcTimestamp {
        HlcTimestamp(self.last.load(Ordering::Acquire))
    }

    /// Calculate the drift between HLC physical time and wall clock.
    ///
    /// Returns positive value if HLC is ahead of wall clock.
    #[must_use]
    pub fn drift_ms(&self) -> i64 {
        let wall_ms = Self::wall_clock_ms();
        let hlc_physical = self.current().physical_ms();
        #[allow(clippy::cast_possible_wrap)]
        let result = hlc_physical as i64 - wall_ms as i64;
        result
    }

    /// Check if the clock drift is within acceptable bounds.
    #[must_use]
    pub fn health(&self) -> ClockHealth {
        let drift = self.drift_ms().unsigned_abs();

        if drift > Self::MAX_DRIFT_CRITICAL_MS {
            ClockHealth::Critical { drift_ms: drift }
        } else if drift > Self::MAX_DRIFT_WARNING_MS {
            ClockHealth::Degraded { drift_ms: drift }
        } else {
            ClockHealth::Healthy
        }
    }

    /// Get current wall clock time in milliseconds since Unix epoch.
    #[inline]
    fn wall_clock_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| {
                #[allow(clippy::cast_possible_truncation)]
                let ms = d.as_millis() as u64;
                ms
            })
            .unwrap_or(0)
    }
}

impl std::fmt::Debug for HybridLogicalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridLogicalClock")
            .field("node_id", &self.node_id)
            .field("current", &self.current())
            .field("drift_ms", &self.drift_ms())
            .finish_non_exhaustive()
    }
}

/// Health status of the HLC clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockHealth {
    /// Clock drift is within normal bounds (<100ms).
    Healthy,
    /// Clock drift is elevated but acceptable (100ms - 1s).
    Degraded { drift_ms: u64 },
    /// Clock drift is critical (>1s) - indicates NTP failure or partition.
    Critical { drift_ms: u64 },
}

impl ClockHealth {
    /// Check if the clock is in a healthy state.
    #[inline]
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Check if the clock is in a critical state.
    #[inline]
    #[must_use]
    pub const fn is_critical(&self) -> bool {
        matches!(self, Self::Critical { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_timestamp_packing() {
        let ts = HlcTimestamp::new(1234567890, 42);
        assert_eq!(ts.physical_ms(), 1234567890);
        assert_eq!(ts.logical(), 42);
    }

    #[test]
    fn test_hlc_timestamp_ordering() {
        let ts1 = HlcTimestamp::new(100, 0);
        let ts2 = HlcTimestamp::new(100, 1);
        let ts3 = HlcTimestamp::new(101, 0);

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
        assert!(ts1 < ts3);
    }

    #[test]
    fn test_hlc_monotonicity() {
        let hlc = HybridLogicalClock::new(1);
        let mut prev = hlc.now();

        for _ in 0..1000 {
            let curr = hlc.now();
            assert!(curr > prev, "HLC must be strictly monotonic");
            prev = curr;
        }
    }

    #[test]
    fn test_hlc_receive_advances() {
        let hlc = HybridLogicalClock::new(1);
        let local = hlc.now();

        // Simulate receiving a message with a future timestamp
        let remote = HlcTimestamp::new(local.physical_ms() + 1000, 0);
        let after_receive = hlc.receive(remote);

        assert!(after_receive > local);
        assert!(after_receive > remote);
    }

    #[test]
    fn test_hlc_receive_same_physical() {
        let hlc = HybridLogicalClock::new(1);

        // Force the HLC to a known state
        let ts1 = hlc.now();

        // Receive a message with same physical but higher logical
        let remote = HlcTimestamp::new(ts1.physical_ms(), ts1.logical() + 10);
        let after = hlc.receive(remote);

        assert!(after > remote);
        assert!(after > ts1);
    }

    #[test]
    fn test_hlc_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let hlc = Arc::new(HybridLogicalClock::new(1));
        let mut handles = vec![];

        for _ in 0..4 {
            let hlc_clone = Arc::clone(&hlc);
            handles.push(thread::spawn(move || {
                let mut timestamps = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    timestamps.push(hlc_clone.now());
                }
                timestamps
            }));
        }

        let mut all_timestamps: Vec<HlcTimestamp> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("thread panicked"))
            .collect();

        // All timestamps should be unique (no duplicates)
        let original_len = all_timestamps.len();
        all_timestamps.sort();
        all_timestamps.dedup();
        assert_eq!(
            all_timestamps.len(),
            original_len,
            "HLC produced duplicate timestamps"
        );
    }

    #[test]
    fn test_hlc_size() {
        assert_eq!(std::mem::size_of::<HlcTimestamp>(), 8);
    }

    #[test]
    fn test_clock_health() {
        let hlc = HybridLogicalClock::new(1);
        // Generate a timestamp to sync with wall clock
        let _ = hlc.now();
        // After generating a timestamp, clock should be healthy
        assert!(hlc.health().is_healthy());
    }
}
