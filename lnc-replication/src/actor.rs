use crate::follower::{FollowerHealth, FollowerStatus};
use crate::mode::ReplicationMode;
use crate::quorum::{QuorumConfig, QuorumResult, QuorumTracker};
use bytes::Bytes;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

pub struct ReplicationRequest {
    pub batch_id: u64,
    pub offset: u64,
    /// Payload data using `Bytes` for zero-copy reference counting.
    ///
    /// Unlike `Vec<u8>`, cloning a `Bytes` is an `Arc` refcount increment —
    /// no heap allocation, no memcpy. This is critical on the hot path where
    /// every write produces a `ReplicationRequest`.
    pub data: Bytes,
    pub quorum_tx: oneshot::Sender<QuorumResult>,
}

pub struct ReplicationActor {
    mode: ReplicationMode,
    node_id: u16,
    followers: HashMap<u16, FollowerHealth>,
    quorum_config: QuorumConfig,
    request_rx: mpsc::Receiver<ReplicationRequest>,
}

impl ReplicationActor {
    pub fn new(
        mode: ReplicationMode,
        node_id: u16,
        peer_ids: Vec<u16>,
        request_rx: mpsc::Receiver<ReplicationRequest>,
    ) -> Self {
        let mut followers = HashMap::new();
        for peer_id in &peer_ids {
            followers.insert(*peer_id, FollowerHealth::new(*peer_id));
        }

        let total_nodes = peer_ids.len() + 1;
        let quorum_config = QuorumConfig::new(total_nodes);

        Self {
            mode,
            node_id,
            followers,
            quorum_config,
            request_rx,
        }
    }

    pub async fn run(&mut self) {
        tracing::info!(
            target: "lance::replication",
            mode = %self.mode,
            node_id = self.node_id,
            followers = self.followers.len(),
            "Replication actor started"
        );

        while let Some(request) = self.request_rx.recv().await {
            let result = self.handle_request(request.batch_id, &request.data).await;
            let _ = request.quorum_tx.send(result);
        }

        tracing::info!(
            target: "lance::replication",
            "Replication actor shutting down"
        );
    }

    /// Handle a replication request using concurrent broadcast via JoinSet.
    ///
    /// All follower replication requests fire simultaneously so P99 latency
    /// is determined by the fastest quorum (max of quorum-size RTTs), not
    /// the sum of all follower RTTs. As results arrive, the tracker records
    /// ACKs/NACKs and returns as soon as quorum is reached.
    async fn handle_request(&mut self, batch_id: u64, _data: &[u8]) -> QuorumResult {
        if !self.mode.requires_quorum() {
            return QuorumResult::Success;
        }

        let healthy_followers: Vec<u16> = self
            .followers
            .iter()
            .filter(|(_, f)| !f.is_evicted())
            .map(|(id, _)| *id)
            .collect();

        if healthy_followers.is_empty() {
            tracing::warn!(
                target: "lance::replication",
                batch_id = batch_id,
                "No healthy followers available"
            );
            return QuorumResult::Failed;
        }

        let mut tracker = QuorumTracker::new(self.quorum_config.clone());

        // Local write is already done (per architecture) — count as first ACK
        tracker.record_ack();

        // Fire all follower replication requests concurrently
        let mut join_set = JoinSet::new();
        let timeout_ms = self.quorum_config.timeout_ms;

        for follower_id in healthy_followers {
            join_set.spawn(async move {
                let latency = Self::simulate_replication_static(follower_id).await;
                (follower_id, latency)
            });
        }

        // Collect results as they arrive — return as soon as quorum is reached
        while let Some(res) = join_set.join_next().await {
            let (follower_id, latency) = match res {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        target: "lance::replication",
                        error = %e,
                        "Replication task panicked"
                    );
                    if let Some(result) = tracker.record_nack() {
                        join_set.abort_all();
                        return result;
                    }
                    continue;
                },
            };

            // Update follower health in the actor's local state
            self.update_follower_health(follower_id, latency);

            if latency < Duration::from_millis(timeout_ms) {
                if let Some(result) = tracker.record_ack() {
                    // Quorum reached — abort remaining tasks, no need to wait
                    join_set.abort_all();
                    return result;
                }
            } else if let Some(result) = tracker.record_nack() {
                join_set.abort_all();
                return result;
            }
        }

        tracker.finalize()
    }

    /// Static replication simulation (Send + 'static compatible for JoinSet::spawn).
    async fn simulate_replication_static(_follower_id: u16) -> Duration {
        Duration::from_millis(2)
    }

    fn recalculate_quorum(&mut self) {
        let healthy_count = self.followers.values().filter(|f| !f.is_evicted()).count();

        self.quorum_config.recalculate(healthy_count + 1);
    }

    pub fn update_follower_health(&mut self, node_id: u16, latency: Duration) {
        if let Some(follower) = self.followers.get_mut(&node_id) {
            if let Some(new_status) = follower.record_latency(latency) {
                if new_status == FollowerStatus::Evicted || new_status == FollowerStatus::Healthy {
                    self.recalculate_quorum();
                }
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }

    #[inline]
    #[must_use]
    pub fn node_id(&self) -> u16 {
        self.node_id
    }

    #[inline]
    #[must_use]
    pub fn follower_count(&self) -> usize {
        self.followers.len()
    }

    #[inline]
    #[must_use]
    pub fn healthy_follower_count(&self) -> usize {
        self.followers.values().filter(|f| !f.is_evicted()).count()
    }

    #[inline]
    #[must_use]
    pub fn required_acks(&self) -> usize {
        self.quorum_config.required_acks
    }
}

pub fn create_replication_channel(
    buffer_size: usize,
) -> (
    mpsc::Sender<ReplicationRequest>,
    mpsc::Receiver<ReplicationRequest>,
) {
    mpsc::channel(buffer_size)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_actor_l1() {
        let (tx, rx) = create_replication_channel(16);
        let mut actor = ReplicationActor::new(ReplicationMode::L1, 0, vec![1, 2], rx);

        let (quorum_tx, quorum_rx) = oneshot::channel();
        tx.send(ReplicationRequest {
            batch_id: 1,
            offset: 0,
            data: Bytes::from_static(&[1, 2, 3]),
            quorum_tx,
        })
        .await
        .unwrap();

        drop(tx);

        tokio::spawn(async move {
            actor.run().await;
        });

        let result = quorum_rx.await.unwrap();
        assert_eq!(result, QuorumResult::Success);
    }
}
