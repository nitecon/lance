//! Chaos testing utilities for fault injection and resilience testing.
//!
//! Provides controlled failure injection to test system behavior under:
//! - Network partitions and latency
//! - Disk I/O failures
//! - Clock skew
//! - Resource exhaustion

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Chaos injection configuration
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of I/O failure (0-100)
    pub io_failure_pct: u8,
    /// Probability of network delay (0-100)
    pub network_delay_pct: u8,
    /// Network delay range
    pub network_delay_range: (Duration, Duration),
    /// Probability of clock skew injection (0-100)
    pub clock_skew_pct: u8,
    /// Clock skew range in milliseconds
    pub clock_skew_range_ms: (i64, i64),
    /// Probability of memory allocation failure (0-100)
    pub alloc_failure_pct: u8,
    /// Enable chaos injection (master switch)
    pub enabled: bool,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            io_failure_pct: 0,
            network_delay_pct: 0,
            network_delay_range: (Duration::from_millis(10), Duration::from_millis(100)),
            clock_skew_pct: 0,
            clock_skew_range_ms: (-100, 100),
            alloc_failure_pct: 0,
            enabled: false,
        }
    }
}

impl ChaosConfig {
    /// Create a mild chaos config for basic testing
    #[must_use]
    pub fn mild() -> Self {
        Self {
            io_failure_pct: 1,
            network_delay_pct: 5,
            network_delay_range: (Duration::from_millis(1), Duration::from_millis(10)),
            clock_skew_pct: 2,
            clock_skew_range_ms: (-10, 10),
            alloc_failure_pct: 0,
            enabled: true,
        }
    }

    /// Create an aggressive chaos config for stress testing
    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            io_failure_pct: 5,
            network_delay_pct: 20,
            network_delay_range: (Duration::from_millis(50), Duration::from_millis(500)),
            clock_skew_pct: 10,
            clock_skew_range_ms: (-500, 500),
            alloc_failure_pct: 1,
            enabled: true,
        }
    }
}

/// Chaos injection controller
pub struct ChaosInjector {
    config: ChaosConfig,
    injection_count: AtomicU32,
    paused: AtomicBool,
    seed: AtomicU32,
}

impl ChaosInjector {
    #[must_use]
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            config,
            injection_count: AtomicU32::new(0),
            paused: AtomicBool::new(false),
            seed: AtomicU32::new(12345),
        }
    }

    /// Create a disabled injector (no-op)
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(ChaosConfig::default())
    }

    /// Simple PRNG for deterministic chaos
    fn next_rand(&self) -> u32 {
        let mut seed = self.seed.load(Ordering::Relaxed);
        seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12345);
        self.seed.store(seed, Ordering::Relaxed);
        (seed >> 16) & 0x7fff
    }

    fn should_inject(&self, probability_pct: u8) -> bool {
        if !self.config.enabled || self.paused.load(Ordering::Relaxed) {
            return false;
        }
        if probability_pct == 0 {
            return false;
        }
        let rand = self.next_rand() % 100;
        rand < u32::from(probability_pct)
    }

    /// Check if an I/O failure should be injected
    pub fn should_fail_io(&self) -> bool {
        let result = self.should_inject(self.config.io_failure_pct);
        if result {
            self.injection_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Check if network delay should be injected
    pub fn should_delay_network(&self) -> Option<Duration> {
        if !self.should_inject(self.config.network_delay_pct) {
            return None;
        }

        self.injection_count.fetch_add(1, Ordering::Relaxed);

        let (min, max) = self.config.network_delay_range;
        #[allow(clippy::cast_possible_truncation)]
        let min_ms = min.as_millis().min(u128::from(u32::MAX)) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let max_ms = max.as_millis().min(u128::from(u32::MAX)) as u32;
        let range = max_ms.saturating_sub(min_ms);
        let delay_ms = if range > 0 {
            min_ms + (self.next_rand() % range)
        } else {
            min_ms
        };

        Some(Duration::from_millis(u64::from(delay_ms)))
    }

    /// Get clock skew to inject (in milliseconds)
    pub fn get_clock_skew_ms(&self) -> Option<i64> {
        if !self.should_inject(self.config.clock_skew_pct) {
            return None;
        }

        self.injection_count.fetch_add(1, Ordering::Relaxed);

        let (min, max) = self.config.clock_skew_range_ms;
        #[allow(clippy::cast_possible_truncation)]
        let range = (max - min).unsigned_abs() as u32;
        let skew = if range > 0 {
            min + i64::from(self.next_rand() % range)
        } else {
            min
        };

        Some(skew)
    }

    /// Check if allocation should fail
    pub fn should_fail_alloc(&self) -> bool {
        let result = self.should_inject(self.config.alloc_failure_pct);
        if result {
            self.injection_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Pause chaos injection
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume chaos injection
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    /// Get total injection count
    pub fn injection_count(&self) -> u32 {
        self.injection_count.load(Ordering::Relaxed)
    }

    /// Reset injection count
    pub fn reset_count(&self) {
        self.injection_count.store(0, Ordering::Relaxed);
    }

    /// Check if chaos is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled && !self.paused.load(Ordering::Relaxed)
    }
}

/// Shared chaos injector for use across threads
pub type SharedChaosInjector = Arc<ChaosInjector>;

/// Create a shared chaos injector
#[must_use]
pub fn shared_injector(config: ChaosConfig) -> SharedChaosInjector {
    Arc::new(ChaosInjector::new(config))
}

/// Fault types for scenario testing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultType {
    /// Disk write failure
    DiskWriteFailure,
    /// Disk read failure
    DiskReadFailure,
    /// Network partition (complete)
    NetworkPartition,
    /// Network delay/latency
    NetworkLatency,
    /// Leader crash simulation
    LeaderCrash,
    /// Follower crash simulation
    FollowerCrash,
    /// Clock jump forward
    ClockJumpForward,
    /// Clock jump backward
    ClockJumpBackward,
    /// Memory pressure
    MemoryPressure,
    /// CPU stall
    CpuStall,
}

impl FaultType {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            FaultType::DiskWriteFailure => "disk_write_failure",
            FaultType::DiskReadFailure => "disk_read_failure",
            FaultType::NetworkPartition => "network_partition",
            FaultType::NetworkLatency => "network_latency",
            FaultType::LeaderCrash => "leader_crash",
            FaultType::FollowerCrash => "follower_crash",
            FaultType::ClockJumpForward => "clock_jump_forward",
            FaultType::ClockJumpBackward => "clock_jump_backward",
            FaultType::MemoryPressure => "memory_pressure",
            FaultType::CpuStall => "cpu_stall",
        }
    }
}

/// Scenario for chaos testing
#[derive(Debug, Clone)]
pub struct ChaosScenario {
    pub name: String,
    pub faults: Vec<(FaultType, Duration)>,
    pub duration: Duration,
}

impl ChaosScenario {
    /// Create a new scenario
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            faults: Vec::new(),
            duration: Duration::from_secs(60),
        }
    }

    /// Add a fault at a specific time offset
    #[must_use]
    pub fn with_fault(mut self, fault: FaultType, at: Duration) -> Self {
        self.faults.push((fault, at));
        self
    }

    /// Set scenario duration
    #[must_use]
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Leader election scenario
    #[must_use]
    pub fn leader_election() -> Self {
        Self::new("leader_election")
            .with_fault(FaultType::LeaderCrash, Duration::from_secs(5))
            .with_duration(Duration::from_secs(30))
    }

    /// Network partition scenario
    #[must_use]
    pub fn network_partition() -> Self {
        Self::new("network_partition")
            .with_fault(FaultType::NetworkPartition, Duration::from_secs(2))
            .with_duration(Duration::from_secs(30))
    }

    /// Disk failure scenario
    #[must_use]
    pub fn disk_failure() -> Self {
        Self::new("disk_failure")
            .with_fault(FaultType::DiskWriteFailure, Duration::from_secs(3))
            .with_fault(FaultType::DiskReadFailure, Duration::from_secs(10))
            .with_duration(Duration::from_secs(30))
    }

    /// Clock skew scenario
    #[must_use]
    pub fn clock_skew() -> Self {
        Self::new("clock_skew")
            .with_fault(FaultType::ClockJumpForward, Duration::from_secs(5))
            .with_fault(FaultType::ClockJumpBackward, Duration::from_secs(15))
            .with_duration(Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_config_default_disabled() {
        let config = ChaosConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.io_failure_pct, 0);
    }

    #[test]
    fn test_chaos_injector_disabled() {
        let injector = ChaosInjector::disabled();

        // Should never inject when disabled
        for _ in 0..100 {
            assert!(!injector.should_fail_io());
            assert!(injector.should_delay_network().is_none());
        }
    }

    #[test]
    fn test_chaos_injector_aggressive() {
        let injector = ChaosInjector::new(ChaosConfig::aggressive());

        // With 5% I/O failure, we should see some failures in 1000 iterations
        let mut failures = 0;
        for _ in 0..1000 {
            if injector.should_fail_io() {
                failures += 1;
            }
        }

        // Should have roughly 5% failures (with some variance)
        assert!(failures > 0);
        assert!(failures < 200); // Upper bound with variance
    }

    #[test]
    fn test_chaos_injector_pause_resume() {
        let config = ChaosConfig {
            io_failure_pct: 100, // Always fail
            enabled: true,
            ..Default::default()
        };
        let injector = ChaosInjector::new(config);

        // Should fail when enabled
        assert!(injector.should_fail_io());

        // Pause and verify no failures
        injector.pause();
        assert!(!injector.should_fail_io());

        // Resume and verify failures again
        injector.resume();
        assert!(injector.should_fail_io());
    }

    #[test]
    fn test_chaos_scenario() {
        let scenario = ChaosScenario::leader_election();
        assert_eq!(scenario.name, "leader_election");
        assert_eq!(scenario.faults.len(), 1);
        assert_eq!(scenario.faults[0].0, FaultType::LeaderCrash);
    }

    #[test]
    fn test_fault_type_as_str() {
        assert_eq!(FaultType::DiskWriteFailure.as_str(), "disk_write_failure");
        assert_eq!(FaultType::NetworkPartition.as_str(), "network_partition");
    }
}
