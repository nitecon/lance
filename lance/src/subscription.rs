//! Subscription management for streaming consumers
//!
//! Tracks active subscriptions and committed offsets for consumer groups.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// Information about an active subscription
struct SubscriptionInfo {
    /// Current streaming position
    current_offset: u64,
    /// Last activity timestamp
    last_activity: u64,
}

impl SubscriptionInfo {
    fn new(start_offset: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            current_offset: start_offset,
            last_activity: now,
        }
    }

    fn touch(&mut self) {
        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
    }
}

/// Committed offset position
struct CommittedOffset {
    offset: u64,
}

impl CommittedOffset {
    fn new(offset: u64) -> Self {
        Self { offset }
    }
}

/// Key for subscription lookup: (topic_id, consumer_id)
type SubscriptionKey = (u32, u64);

/// Manages active subscriptions and committed offsets
pub struct SubscriptionManager {
    /// Active subscriptions by (topic_id, consumer_id)
    subscriptions: RwLock<HashMap<SubscriptionKey, SubscriptionInfo>>,
    /// Committed offsets by (topic_id, consumer_id)
    committed_offsets: RwLock<HashMap<SubscriptionKey, CommittedOffset>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            committed_offsets: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe a consumer to a topic
    ///
    /// Returns the actual starting offset (may differ from requested if
    /// there's a previously committed offset for this consumer).
    pub fn subscribe(
        &self,
        consumer_id: u64,
        topic_id: u32,
        requested_offset: u64,
        _max_batch_bytes: u32,
    ) -> u64 {
        let key = (topic_id, consumer_id);

        // Check for previously committed offset
        let start_offset = if requested_offset == u64::MAX {
            // SeekPosition::End - use latest
            requested_offset
        } else if requested_offset == 0 {
            // SeekPosition::Beginning - check for committed offset first
            self.get_committed_offset(topic_id, consumer_id)
                .unwrap_or(0)
        } else {
            // Explicit offset requested
            requested_offset
        };

        let info = SubscriptionInfo::new(start_offset);

        if let Ok(mut subs) = self.subscriptions.write() {
            subs.insert(key, info);
        }

        info!(
            target: "lance::subscription",
            consumer_id,
            topic_id,
            start_offset,
            "Consumer subscribed"
        );

        start_offset
    }

    /// Unsubscribe a consumer from a topic
    pub fn unsubscribe(&self, consumer_id: u64, topic_id: u32) -> bool {
        let key = (topic_id, consumer_id);

        let removed = if let Ok(mut subs) = self.subscriptions.write() {
            subs.remove(&key).is_some()
        } else {
            false
        };

        if removed {
            info!(
                target: "lance::subscription",
                consumer_id,
                topic_id,
                "Consumer unsubscribed"
            );
        }

        removed
    }

    /// Commit an offset for a consumer
    pub fn commit_offset(&self, consumer_id: u64, topic_id: u32, offset: u64) -> u64 {
        let key = (topic_id, consumer_id);

        let committed = CommittedOffset::new(offset);
        let committed_offset = committed.offset;

        if let Ok(mut offsets) = self.committed_offsets.write() {
            offsets.insert(key, committed);
        }

        // Also update the subscription's current offset if it exists
        if let Ok(mut subs) = self.subscriptions.write() {
            if let Some(info) = subs.get_mut(&key) {
                if offset > info.current_offset {
                    info.current_offset = offset;
                }
                info.touch();
            }
        }

        debug!(
            target: "lance::subscription",
            consumer_id,
            topic_id,
            offset,
            "Offset committed"
        );

        committed_offset
    }

    /// Get the last committed offset for a consumer
    pub fn get_committed_offset(&self, topic_id: u32, consumer_id: u64) -> Option<u64> {
        let key = (topic_id, consumer_id);
        self.committed_offsets
            .read()
            .ok()?
            .get(&key)
            .map(|c| c.offset)
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_unsubscribe() {
        let manager = SubscriptionManager::new();

        // Subscribe returns the starting offset
        let offset = manager.subscribe(1001, 42, 0, 65536);
        assert_eq!(offset, 0);

        // Unsubscribe returns true when subscription existed
        assert!(manager.unsubscribe(1001, 42));

        // Unsubscribe again returns false
        assert!(!manager.unsubscribe(1001, 42));
    }

    #[test]
    fn test_commit_offset() {
        let manager = SubscriptionManager::new();

        // Subscribe first
        manager.subscribe(1001, 42, 0, 65536);

        // Commit offset
        let committed = manager.commit_offset(1001, 42, 1000);
        assert_eq!(committed, 1000);

        // Verify committed offset can be retrieved
        assert_eq!(manager.get_committed_offset(42, 1001), Some(1000));
    }

    #[test]
    fn test_resume_from_committed_offset() {
        let manager = SubscriptionManager::new();

        // First subscription
        manager.subscribe(1001, 42, 0, 65536);
        manager.commit_offset(1001, 42, 5000);
        manager.unsubscribe(1001, 42);

        // Re-subscribe with offset 0 (Beginning) should resume from committed
        let offset = manager.subscribe(1001, 42, 0, 65536);
        assert_eq!(offset, 5000);
    }

    #[test]
    fn test_explicit_offset_overrides_committed() {
        let manager = SubscriptionManager::new();

        // Commit an offset
        manager.commit_offset(1001, 42, 5000);

        // Subscribe with explicit offset should use that offset
        let offset = manager.subscribe(1001, 42, 100, 65536);
        assert_eq!(offset, 100);
    }

    #[test]
    fn test_committed_offset_persists_after_unsubscribe() {
        let manager = SubscriptionManager::new();

        manager.subscribe(1001, 42, 0, 65536);
        manager.commit_offset(1001, 42, 1000);
        manager.unsubscribe(1001, 42);

        // Committed offset should still be available
        assert_eq!(manager.get_committed_offset(42, 1001), Some(1000));
    }
}
