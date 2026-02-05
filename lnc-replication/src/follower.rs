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

    pub fn p99_latency(&self) -> Option<Duration> {
        if self.latencies.is_empty() {
            return None;
        }

        let mut sorted: Vec<Duration> = self.latencies.iter().copied().collect();
        sorted.sort();

        let idx = (sorted.len() as f64 * 0.99).ceil() as usize - 1;
        Some(sorted[idx.min(sorted.len() - 1)])
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
}
