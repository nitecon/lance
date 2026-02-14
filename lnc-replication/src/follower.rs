use std::collections::VecDeque;
use std::time::Duration;

const LATENCY_WINDOW_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FollowerStatus {
    #[default]
    Healthy,
    Degraded,
    Evicted,
}

pub struct FollowerHealth {
    pub node_id: u16,
    latencies: VecDeque<Duration>,
    consecutive_slow: u32,
    pub status: FollowerStatus,
    p99_threshold: Duration,
    eviction_threshold: u32,
    recovery_window: u32,
}

impl FollowerHealth {
    pub fn new(node_id: u16) -> Self {
        Self {
            node_id,
            latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
            consecutive_slow: 0,
            status: FollowerStatus::Healthy,
            p99_threshold: Duration::from_millis(10),
            eviction_threshold: 3,
            recovery_window: 10,
        }
    }

    pub fn with_thresholds(
        node_id: u16,
        p99_threshold: Duration,
        eviction_threshold: u32,
        recovery_window: u32,
    ) -> Self {
        Self {
            node_id,
            latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
            consecutive_slow: 0,
            status: FollowerStatus::Healthy,
            p99_threshold,
            eviction_threshold,
            recovery_window,
        }
    }

    pub fn record_latency(&mut self, latency: Duration) -> Option<FollowerStatus> {
        self.latencies.push_back(latency);
        if self.latencies.len() > LATENCY_WINDOW_SIZE {
            self.latencies.pop_front();
        }

        let previous_status = self.status;

        if latency > self.p99_threshold {
            self.consecutive_slow += 1;

            if self.consecutive_slow >= self.eviction_threshold
                && self.status != FollowerStatus::Evicted
            {
                self.status = FollowerStatus::Evicted;
                lnc_metrics::increment_follower_evictions();
                tracing::warn!(
                    target: "lance::replication",
                    node_id = self.node_id,
                    latency_ms = latency.as_millis(),
                    "Follower evicted from quorum due to latency"
                );
            } else if self.status == FollowerStatus::Healthy {
                self.status = FollowerStatus::Degraded;
            }
        } else if self.status == FollowerStatus::Evicted {
            let recent_fast = self
                .latencies
                .iter()
                .rev()
                .take(self.recovery_window as usize)
                .filter(|&l| *l <= self.p99_threshold)
                .count();

            if recent_fast >= self.recovery_window as usize {
                self.status = FollowerStatus::Healthy;
                self.consecutive_slow = 0;
                lnc_metrics::increment_follower_recoveries();
                tracing::info!(
                    target: "lance::replication",
                    node_id = self.node_id,
                    "Follower recovered, rejoining quorum"
                );
            }
        } else {
            self.consecutive_slow = 0;
            self.status = FollowerStatus::Healthy;
        }

        if self.status != previous_status {
            Some(self.status)
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.status == FollowerStatus::Healthy
    }

    #[inline]
    #[must_use]
    pub fn is_evicted(&self) -> bool {
        self.status == FollowerStatus::Evicted
    }

    pub fn average_latency(&self) -> Option<Duration> {
        if self.latencies.is_empty() {
            return None;
        }

        let total: Duration = self.latencies.iter().sum();
        Some(total / self.latencies.len() as u32)
    }

    /// Compute P99 latency using O(N) partial selection on a stack-allocated buffer.
    ///
    /// **Zero-Allocation (§18.1)**: Uses a fixed-size `[Duration; LATENCY_WINDOW_SIZE]`
    /// on the stack instead of `Vec::collect()` on the heap. If monitoring scrapes this
    /// every second, there is zero allocator pressure — no garbage for the global
    /// allocator to contend over.
    ///
    /// **O(N)**: `select_nth_unstable` uses introselect (quickselect + heapselect).
    pub fn p99_latency(&self) -> Option<Duration> {
        if self.latencies.is_empty() {
            return None;
        }

        // Stack allocation — no heap, no allocator lock contention
        let mut buf = [Duration::ZERO; LATENCY_WINDOW_SIZE];
        let len = self.latencies.len();
        for (i, lat) in self.latencies.iter().enumerate() {
            buf[i] = *lat;
        }

        let idx = (len as f64 * 0.99).ceil() as usize - 1;
        let idx = idx.min(len - 1);

        // O(N) selection on the stack slice
        let (_left, median, _right) = buf[..len].select_nth_unstable(idx);
        Some(*median)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_follower_health_eviction() {
        let mut health = FollowerHealth::with_thresholds(1, Duration::from_millis(10), 3, 5);

        health.record_latency(Duration::from_millis(5));
        assert_eq!(health.status, FollowerStatus::Healthy);

        health.record_latency(Duration::from_millis(20));
        health.record_latency(Duration::from_millis(20));
        assert_eq!(health.status, FollowerStatus::Degraded);

        health.record_latency(Duration::from_millis(20));
        assert_eq!(health.status, FollowerStatus::Evicted);
    }

    #[test]
    fn test_follower_health_recovery() {
        let mut health = FollowerHealth::with_thresholds(1, Duration::from_millis(10), 3, 3);

        for _ in 0..3 {
            health.record_latency(Duration::from_millis(20));
        }
        assert_eq!(health.status, FollowerStatus::Evicted);

        for _ in 0..3 {
            health.record_latency(Duration::from_millis(5));
        }
        assert_eq!(health.status, FollowerStatus::Healthy);
    }

    #[test]
    fn test_p99_latency_stack_based() {
        let mut health = FollowerHealth::new(1);

        // Fill window with 1..=100 ms
        for i in 1..=100 {
            health.record_latency(Duration::from_millis(i));
        }

        let p99 = health.p99_latency().unwrap();
        // P99 of 1..=100 is the 99th value = 99ms
        // ceil(100 * 0.99) = 99, idx = 98, value at rank 99 = 99ms
        assert_eq!(p99, Duration::from_millis(99));
    }

    #[test]
    fn test_p99_latency_empty() {
        let health = FollowerHealth::new(1);
        assert!(health.p99_latency().is_none());
    }

    #[test]
    fn test_p99_latency_single_element() {
        let mut health = FollowerHealth::new(1);
        health.record_latency(Duration::from_millis(42));
        assert_eq!(health.p99_latency().unwrap(), Duration::from_millis(42));
    }

    #[test]
    fn test_average_latency() {
        let mut health = FollowerHealth::new(1);
        health.record_latency(Duration::from_millis(10));
        health.record_latency(Duration::from_millis(20));
        health.record_latency(Duration::from_millis(30));
        assert_eq!(health.average_latency().unwrap(), Duration::from_millis(20));
    }
}
