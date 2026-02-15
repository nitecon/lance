use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};

#[derive(Debug, Clone)]
pub struct QuorumConfig {
    pub total_nodes: usize,
    pub required_acks: usize,
    pub timeout_ms: u64,
}

impl QuorumConfig {
    pub fn new(total_nodes: usize) -> Self {
        let required_acks = (total_nodes / 2) + 1;
        Self {
            total_nodes,
            required_acks,
            // Increased from 100ms to 1000ms for production stability.
            // 100ms was causing OOMKilled cascades under benchmark load due to
            // quorum timeouts triggering retries and memory pressure.
            timeout_ms: 1000,
        }
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Recalculate quorum based on current healthy node count.
    ///
    /// **Safety-first invariant** (Architecture §4.1.1): The required ACKs
    /// can never drop below the static majority of `total_nodes`. This
    /// prevents a scenario where a 5-node cluster with 4 degraded nodes
    /// accepts writes with quorum=1, which would violate Raft's safety
    /// guarantee and risk split-brain during network partitions.
    ///
    /// The floor is `(total_nodes / 2) + 1` — the same majority required
    /// when all nodes are healthy. Adaptive eviction improves *latency*
    /// (by not waiting for slow followers) but never weakens *durability*.
    pub fn recalculate(&mut self, healthy_nodes: usize) {
        let adaptive_quorum = (healthy_nodes / 2) + 1;
        let static_floor = (self.total_nodes / 2) + 1;

        self.required_acks = adaptive_quorum.max(static_floor).max(1);

        tracing::info!(
            target: "lance::replication",
            healthy_nodes,
            adaptive_quorum,
            static_floor,
            required_acks = self.required_acks,
            "Quorum recalculated (floor enforced)"
        );
    }

    #[inline]
    #[must_use]
    pub fn is_quorum_reached(&self, acks: usize) -> bool {
        acks >= self.required_acks
    }

    #[inline]
    #[must_use]
    pub fn can_tolerate_failures(&self) -> usize {
        self.total_nodes.saturating_sub(self.required_acks)
    }
}

impl Default for QuorumConfig {
    fn default() -> Self {
        Self::new(3)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumResult {
    Success,
    Failed,
    Timeout,
    Partial { received: usize, required: usize },
}

impl QuorumResult {
    #[inline]
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    #[inline]
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        !self.is_success()
    }
}

pub struct QuorumTracker {
    config: QuorumConfig,
    acks_received: usize,
    nacks_received: usize,
}

impl QuorumTracker {
    pub fn new(config: QuorumConfig) -> Self {
        Self {
            config,
            acks_received: 0,
            nacks_received: 0,
        }
    }

    pub fn record_ack(&mut self) -> Option<QuorumResult> {
        self.acks_received += 1;

        if self.config.is_quorum_reached(self.acks_received) {
            Some(QuorumResult::Success)
        } else {
            None
        }
    }

    pub fn record_nack(&mut self) -> Option<QuorumResult> {
        self.nacks_received += 1;

        let remaining = self
            .config
            .total_nodes
            .saturating_sub(self.acks_received)
            .saturating_sub(self.nacks_received);

        if self.acks_received + remaining < self.config.required_acks {
            lnc_metrics::increment_quorum_failures();
            Some(QuorumResult::Failed)
        } else {
            None
        }
    }

    pub fn finalize(&self) -> QuorumResult {
        if self.config.is_quorum_reached(self.acks_received) {
            QuorumResult::Success
        } else {
            QuorumResult::Partial {
                received: self.acks_received,
                required: self.config.required_acks,
            }
        }
    }
}

/// Tracks a pending write waiting for quorum ACKs
struct PendingWrite {
    tracker: QuorumTracker,
    result_tx: oneshot::Sender<QuorumResult>,
    created_at: std::time::Instant,
}

/// Number of shards for the pending write map.
///
/// At 100Gbps with 10,000+ in-flight writes, a single `RwLock<HashMap>` becomes
/// a massive synchronization point that flushes CPU caches and stalls ingestion.
/// Sharding by `write_id % NUM_SHARDS` reduces lock contention probability by 64x.
const NUM_SHARDS: usize = 64;

/// Sharded pending-write map that eliminates global lock contention.
///
/// Instead of one `RwLock<HashMap>`, we maintain `NUM_SHARDS` independent
/// `RwLock<HashMap>` slots. Each ACK/NACK only locks the shard that owns
/// the target `write_id`, so concurrent operations on different shards
/// never contend.
struct ShardedPendingMap {
    shards: Box<[RwLock<HashMap<u64, PendingWrite>>]>,
}

impl ShardedPendingMap {
    fn new() -> Self {
        let shards: Vec<RwLock<HashMap<u64, PendingWrite>>> = (0..NUM_SHARDS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    #[inline]
    fn shard_for(&self, write_id: u64) -> &RwLock<HashMap<u64, PendingWrite>> {
        &self.shards[(write_id as usize) % NUM_SHARDS]
    }

    async fn insert(&self, write_id: u64, write: PendingWrite) {
        let mut shard = self.shard_for(write_id).write().await;
        shard.insert(write_id, write);
    }

    async fn remove(&self, write_id: u64) -> Option<PendingWrite> {
        let mut shard = self.shard_for(write_id).write().await;
        shard.remove(&write_id)
    }

    /// Total pending writes across all shards (for metrics).
    async fn len(&self) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            total += shard.read().await.len();
        }
        total
    }

    /// Clean up stale writes across all shards.
    async fn cleanup_stale(&self, max_age: Duration) {
        let now = std::time::Instant::now();
        for shard in self.shards.iter() {
            let mut map = shard.write().await;
            let stale_ids: Vec<u64> = map
                .iter()
                .filter(|(_, w)| now.duration_since(w.created_at) > max_age)
                .map(|(id, _)| *id)
                .collect();

            for write_id in stale_ids {
                if let Some(write) = map.remove(&write_id) {
                    tracing::warn!(
                        target: "lance::replication",
                        write_id,
                        "Cleaning up stale pending write"
                    );
                    let _ = write.result_tx.send(QuorumResult::Timeout);
                }
            }
        }
    }
}

/// Manages async quorum waiting for L3 quorum replication
///
/// When a write is submitted, it returns a channel that will receive
/// the quorum result once enough ACKs are received or timeout occurs.
///
/// Uses a sharded pending-write map (`NUM_SHARDS` = 64) so that
/// concurrent ACK/NACK processing on different write IDs never
/// contends on the same lock.
pub struct AsyncQuorumManager {
    config: QuorumConfig,
    pending: Arc<ShardedPendingMap>,
    next_write_id: std::sync::atomic::AtomicU64,
}

impl AsyncQuorumManager {
    pub fn new(config: QuorumConfig) -> Self {
        Self {
            config,
            pending: Arc::new(ShardedPendingMap::new()),
            next_write_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Register a new write and get a channel to await quorum
    /// Returns (write_id, receiver for quorum result)
    pub async fn register_write(&self) -> (u64, oneshot::Receiver<QuorumResult>) {
        let write_id = self
            .next_write_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        let pending_write = PendingWrite {
            tracker: QuorumTracker::new(self.config.clone()),
            result_tx: tx,
            created_at: std::time::Instant::now(),
        };

        self.pending.insert(write_id, pending_write).await;

        tracing::debug!(
            target: "lance::replication",
            write_id,
            required_acks = self.config.required_acks,
            "Registered pending write"
        );

        (write_id, rx)
    }

    /// Record an ACK from a follower for a specific write
    pub async fn record_ack(&self, write_id: u64, node_id: u16) {
        let mut shard = self.pending.shard_for(write_id).write().await;

        if let Some(write) = shard.get_mut(&write_id) {
            if let Some(result) = write.tracker.record_ack() {
                tracing::info!(
                    target: "lance::replication",
                    write_id,
                    node_id,
                    "Quorum reached"
                );

                // Remove and notify - take ownership to send
                if let Some(write) = shard.remove(&write_id) {
                    let _ = write.result_tx.send(result);
                }
            } else {
                tracing::debug!(
                    target: "lance::replication",
                    write_id,
                    node_id,
                    acks = write.tracker.acks_received,
                    required = write.tracker.config.required_acks,
                    "ACK received, waiting for quorum"
                );
            }
        }
    }

    /// Record a NACK (failure) from a follower for a specific write
    pub async fn record_nack(&self, write_id: u64, node_id: u16) {
        let mut shard = self.pending.shard_for(write_id).write().await;

        if let Some(write) = shard.get_mut(&write_id) {
            if let Some(result) = write.tracker.record_nack() {
                tracing::warn!(
                    target: "lance::replication",
                    write_id,
                    node_id,
                    "Quorum failed - too many NACKs"
                );

                if let Some(write) = shard.remove(&write_id) {
                    let _ = write.result_tx.send(result);
                }
            }
        }
    }

    /// Wait for quorum with timeout
    /// This is the main API for L3 quorum replication
    pub async fn wait_for_quorum(
        &self,
        write_id: u64,
        rx: oneshot::Receiver<QuorumResult>,
    ) -> QuorumResult {
        let timeout = Duration::from_millis(self.config.timeout_ms);

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                // Channel closed without result
                tracing::warn!(
                    target: "lance::replication",
                    write_id,
                    "Quorum channel closed unexpectedly"
                );
                self.cleanup_write(write_id).await;
                QuorumResult::Failed
            },
            Err(_) => {
                // Timeout
                tracing::warn!(
                    target: "lance::replication",
                    write_id,
                    timeout_ms = self.config.timeout_ms,
                    "Quorum timeout"
                );
                lnc_metrics::increment_quorum_timeouts();
                self.cleanup_write(write_id).await;
                QuorumResult::Timeout
            },
        }
    }

    /// Clean up a timed-out or failed write
    async fn cleanup_write(&self, write_id: u64) {
        if let Some(write) = self.pending.remove(write_id).await {
            let result = write.tracker.finalize();
            let _ = write.result_tx.send(result);
        }
    }

    /// Get current pending write count (for metrics)
    pub async fn pending_count(&self) -> usize {
        self.pending.len().await
    }

    /// Clean up stale pending writes (call periodically)
    pub async fn cleanup_stale(&self, max_age: Duration) {
        self.pending.cleanup_stale(max_age).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_config_calculation() {
        let config = QuorumConfig::new(3);
        assert_eq!(config.required_acks, 2);
        assert_eq!(config.can_tolerate_failures(), 1);

        let config = QuorumConfig::new(5);
        assert_eq!(config.required_acks, 3);
        assert_eq!(config.can_tolerate_failures(), 2);
    }

    #[test]
    fn test_quorum_tracker() {
        let config = QuorumConfig::new(3);
        let mut tracker = QuorumTracker::new(config);

        assert!(tracker.record_ack().is_none());
        assert!(tracker.record_ack().is_some());
        assert_eq!(tracker.finalize(), QuorumResult::Success);
    }

    #[test]
    fn test_quorum_failure() {
        let config = QuorumConfig::new(3);
        let mut tracker = QuorumTracker::new(config);

        tracker.record_ack();
        tracker.record_nack();
        let result = tracker.record_nack();

        assert!(result.is_some());
        assert_eq!(result.unwrap(), QuorumResult::Failed);
    }

    #[tokio::test]
    async fn test_async_quorum_manager_success() {
        let config = QuorumConfig::new(3).with_timeout(1000);
        let manager = AsyncQuorumManager::new(config);

        let (write_id, rx) = manager.register_write().await;
        assert_eq!(manager.pending_count().await, 1);

        // Simulate ACKs from followers (need 2 for quorum of 3)
        manager.record_ack(write_id, 1).await;
        manager.record_ack(write_id, 2).await;

        // Should complete immediately since quorum reached
        let result = manager.wait_for_quorum(write_id, rx).await;
        assert_eq!(result, QuorumResult::Success);
        assert_eq!(manager.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_async_quorum_manager_timeout() {
        let config = QuorumConfig::new(3).with_timeout(50); // 50ms timeout
        let manager = AsyncQuorumManager::new(config);

        let (write_id, rx) = manager.register_write().await;

        // Only one ACK - not enough for quorum
        manager.record_ack(write_id, 1).await;

        // Should timeout
        let result = manager.wait_for_quorum(write_id, rx).await;
        assert_eq!(result, QuorumResult::Timeout);
    }

    #[tokio::test]
    async fn test_async_quorum_manager_nack_failure() {
        let config = QuorumConfig::new(3).with_timeout(1000);
        let manager = AsyncQuorumManager::new(config);

        let (write_id, rx) = manager.register_write().await;

        // One ACK, two NACKs - quorum impossible
        manager.record_ack(write_id, 1).await;
        manager.record_nack(write_id, 2).await;
        manager.record_nack(write_id, 3).await;

        let result = manager.wait_for_quorum(write_id, rx).await;
        assert_eq!(result, QuorumResult::Failed);
    }

    #[tokio::test]
    async fn test_async_quorum_manager_cleanup_stale() {
        let config = QuorumConfig::new(3).with_timeout(1000);
        let manager = AsyncQuorumManager::new(config);

        let (_write_id, _rx) = manager.register_write().await;
        assert_eq!(manager.pending_count().await, 1);

        // Clean up writes older than 0ms (all of them)
        manager.cleanup_stale(Duration::from_millis(0)).await;
        assert_eq!(manager.pending_count().await, 0);
    }

    #[test]
    fn test_quorum_config_single_node() {
        let config = QuorumConfig::new(1);
        assert_eq!(config.required_acks, 1);
        assert_eq!(config.can_tolerate_failures(), 0);
    }

    #[test]
    fn test_quorum_config_five_nodes() {
        let config = QuorumConfig::new(5);
        assert_eq!(config.required_acks, 3);
        assert_eq!(config.can_tolerate_failures(), 2);
    }
}
