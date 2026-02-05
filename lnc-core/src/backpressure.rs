//! Backpressure monitoring and flow control.
//!
//! Tracks system resource utilization and provides signals for adaptive
//! rate limiting to prevent overload and maintain stable performance.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Thresholds for backpressure activation
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Queue depth threshold to start applying backpressure
    pub queue_depth_warning: usize,
    /// Queue depth threshold for critical backpressure
    pub queue_depth_critical: usize,
    /// Memory usage percentage to start backpressure (0-100)
    pub memory_warning_pct: u8,
    /// Memory usage percentage for critical backpressure (0-100)
    pub memory_critical_pct: u8,
    /// Pending I/O operations threshold
    pub pending_io_warning: usize,
    /// Pending I/O operations critical threshold
    pub pending_io_critical: usize,
    /// Sampling interval for metrics
    pub sample_interval: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            queue_depth_warning: 1000,
            queue_depth_critical: 5000,
            memory_warning_pct: 70,
            memory_critical_pct: 90,
            pending_io_warning: 100,
            pending_io_critical: 500,
            sample_interval: Duration::from_millis(100),
        }
    }
}

/// Current backpressure level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    /// No backpressure - operate normally
    None,
    /// Light backpressure - slow down non-critical work
    Light,
    /// Moderate backpressure - reduce throughput
    Moderate,
    /// Heavy backpressure - only critical operations
    Heavy,
    /// Critical - reject new work
    Critical,
}

impl BackpressureLevel {
    /// Get a delay multiplier for rate limiting (1.0 = no delay)
    #[must_use]
    pub fn delay_multiplier(&self) -> f64 {
        match self {
            BackpressureLevel::None => 1.0,
            BackpressureLevel::Light => 1.5,
            BackpressureLevel::Moderate => 2.0,
            BackpressureLevel::Heavy => 4.0,
            BackpressureLevel::Critical => 10.0,
        }
    }

    /// Check if new work should be accepted
    #[must_use]
    pub fn accepts_work(&self) -> bool {
        !matches!(self, BackpressureLevel::Critical)
    }

    /// Check if background work should proceed
    #[must_use]
    pub fn allows_background_work(&self) -> bool {
        matches!(self, BackpressureLevel::None | BackpressureLevel::Light)
    }
}

/// Backpressure monitor tracking system resource utilization
pub struct BackpressureMonitor {
    config: BackpressureConfig,
    /// Current queue depth
    queue_depth: AtomicU64,
    /// Pending I/O operations
    pending_io: AtomicU64,
    /// Memory bytes in use
    memory_bytes: AtomicU64,
    /// Total memory available
    memory_total: AtomicU64,
    /// Whether backpressure is currently active
    active: AtomicBool,
    /// Last sample time (stored as epoch nanoseconds for lock-free access)
    last_sample_ns: AtomicU64,
}

impl BackpressureMonitor {
    #[must_use]
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            queue_depth: AtomicU64::new(0),
            pending_io: AtomicU64::new(0),
            memory_bytes: AtomicU64::new(0),
            memory_total: AtomicU64::new(Self::detect_total_memory()),
            active: AtomicBool::new(false),
            last_sample_ns: AtomicU64::new(Self::now_ns()),
        }
    }

    fn detect_total_memory() -> u64 {
        // Try to detect system memory
        #[cfg(target_os = "linux")]
        {
            if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
                for line in content.lines() {
                    if line.starts_with("MemTotal:") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            if let Ok(kb) = parts[1].parse::<u64>() {
                                return kb * 1024;
                            }
                        }
                    }
                }
            }
        }
        // Default: 8GB
        8 * 1024 * 1024 * 1024
    }

    /// Update queue depth metric
    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth as u64, Ordering::Relaxed);
    }

    /// Increment queue depth
    pub fn increment_queue_depth(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement queue depth
    pub fn decrement_queue_depth(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Update pending I/O count
    pub fn set_pending_io(&self, count: usize) {
        self.pending_io.store(count as u64, Ordering::Relaxed);
    }

    /// Update memory usage
    pub fn set_memory_usage(&self, bytes: u64) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Get current backpressure level
    #[must_use]
    pub fn level(&self) -> BackpressureLevel {
        #[allow(clippy::cast_possible_truncation)]
        let queue = self.queue_depth.load(Ordering::Relaxed) as usize;
        #[allow(clippy::cast_possible_truncation)]
        let pending = self.pending_io.load(Ordering::Relaxed) as usize;
        let mem_bytes = self.memory_bytes.load(Ordering::Relaxed);
        let mem_total = self.memory_total.load(Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        let mem_pct = if mem_total > 0 {
            ((mem_bytes * 100) / mem_total) as u8
        } else {
            0
        };

        // Check critical thresholds first
        if queue >= self.config.queue_depth_critical
            || pending >= self.config.pending_io_critical
            || mem_pct >= self.config.memory_critical_pct
        {
            return BackpressureLevel::Critical;
        }

        // Calculate severity score
        let mut score = 0u8;

        if queue >= self.config.queue_depth_warning {
            score += 2;
            if queue >= self.config.queue_depth_critical / 2 {
                score += 1;
            }
        }

        if pending >= self.config.pending_io_warning {
            score += 2;
            if pending >= self.config.pending_io_critical / 2 {
                score += 1;
            }
        }

        if mem_pct >= self.config.memory_warning_pct {
            score += 2;
            if mem_pct
                >= u8::midpoint(
                    self.config.memory_warning_pct,
                    self.config.memory_critical_pct,
                )
            {
                score += 1;
            }
        }

        match score {
            0 => BackpressureLevel::None,
            1..=2 => BackpressureLevel::Light,
            3..=4 => BackpressureLevel::Moderate,
            5..=6 => BackpressureLevel::Heavy,
            _ => BackpressureLevel::Critical,
        }
    }

    /// Check if backpressure is currently active
    pub fn is_active(&self) -> bool {
        let level = self.level();
        let active = !matches!(level, BackpressureLevel::None);
        self.active.store(active, Ordering::Relaxed);
        active
    }

    /// Get current metrics snapshot
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn snapshot(&self) -> BackpressureSnapshot {
        BackpressureSnapshot {
            queue_depth: self.queue_depth.load(Ordering::Relaxed) as usize,
            pending_io: self.pending_io.load(Ordering::Relaxed) as usize,
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            memory_total: self.memory_total.load(Ordering::Relaxed),
            level: self.level(),
        }
    }

    /// Get current time as nanoseconds since an arbitrary epoch
    #[allow(clippy::cast_possible_truncation)]
    fn now_ns() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    /// Should we sample metrics now? (lock-free implementation)
    #[allow(clippy::cast_possible_truncation)]
    pub fn should_sample(&self) -> bool {
        let now = Self::now_ns();
        let last = self.last_sample_ns.load(Ordering::Relaxed);
        let interval_ns = self.config.sample_interval.as_nanos() as u64;

        if now.saturating_sub(last) >= interval_ns {
            // Try to atomically update the last sample time
            // If another thread beat us, that's fine - we skip this sample
            let _ = self.last_sample_ns.compare_exchange(
                last,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            return true;
        }
        false
    }
}

impl Default for BackpressureMonitor {
    fn default() -> Self {
        Self::new(BackpressureConfig::default())
    }
}

/// Snapshot of backpressure metrics
#[derive(Debug, Clone)]
pub struct BackpressureSnapshot {
    pub queue_depth: usize,
    pub pending_io: usize,
    pub memory_bytes: u64,
    pub memory_total: u64,
    pub level: BackpressureLevel,
}

impl BackpressureSnapshot {
    /// Memory usage percentage
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn memory_pct(&self) -> u8 {
        if self.memory_total > 0 {
            ((self.memory_bytes * 100) / self.memory_total) as u8
        } else {
            0
        }
    }
}

/// Guard that tracks an operation for backpressure
pub struct BackpressureGuard<'a> {
    monitor: &'a BackpressureMonitor,
}

impl<'a> BackpressureGuard<'a> {
    pub fn new(monitor: &'a BackpressureMonitor) -> Self {
        monitor.increment_queue_depth();
        Self { monitor }
    }
}

impl Drop for BackpressureGuard<'_> {
    fn drop(&mut self) {
        self.monitor.decrement_queue_depth();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_level_none() {
        let monitor = BackpressureMonitor::default();
        assert_eq!(monitor.level(), BackpressureLevel::None);
        assert!(!monitor.is_active());
    }

    #[test]
    fn test_backpressure_queue_warning() {
        let config = BackpressureConfig {
            queue_depth_warning: 10,
            queue_depth_critical: 100,
            ..Default::default()
        };
        let monitor = BackpressureMonitor::new(config);

        monitor.set_queue_depth(15);
        assert!(monitor.is_active());
        assert!(matches!(
            monitor.level(),
            BackpressureLevel::Light | BackpressureLevel::Moderate
        ));
    }

    #[test]
    fn test_backpressure_critical() {
        let config = BackpressureConfig {
            queue_depth_warning: 10,
            queue_depth_critical: 100,
            ..Default::default()
        };
        let monitor = BackpressureMonitor::new(config);

        monitor.set_queue_depth(100);
        assert_eq!(monitor.level(), BackpressureLevel::Critical);
        assert!(!monitor.level().accepts_work());
    }

    #[test]
    fn test_backpressure_guard() {
        let monitor = BackpressureMonitor::default();
        assert_eq!(monitor.snapshot().queue_depth, 0);

        {
            let _guard = BackpressureGuard::new(&monitor);
            assert_eq!(monitor.snapshot().queue_depth, 1);
        }

        assert_eq!(monitor.snapshot().queue_depth, 0);
    }

    #[test]
    fn test_delay_multiplier() {
        assert_eq!(BackpressureLevel::None.delay_multiplier(), 1.0);
        assert!(BackpressureLevel::Critical.delay_multiplier() > 1.0);
    }
}
