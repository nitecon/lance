//! I/O priority classes for differentiated workload handling.
//!
//! Implements a priority queue system for I/O operations that ensures:
//! - Critical operations (WAL sync, replication) get lowest latency
//! - Consumer reads don't starve ingestion
//! - Background tasks (compaction) yield to foreground work

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

/// I/O priority classes from highest to lowest priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum IoPriority {
    /// Critical path: WAL sync, replication acks
    /// Must complete with minimal latency
    Critical = 0,
    /// High priority: Producer ingestion writes
    High = 1,
    /// Normal priority: Consumer reads
    #[default]
    Normal = 2,
    /// Low priority: Index updates, metadata
    Low = 3,
    /// Background: Compaction, garbage collection
    Background = 4,
}

impl IoPriority {
    /// Get the priority level as a number (lower = higher priority)
    #[inline]
    pub fn level(&self) -> u8 {
        *self as u8
    }

    /// Check if this priority should preempt another
    #[inline]
    pub fn preempts(&self, other: IoPriority) -> bool {
        (*self as u8) < (other as u8)
    }

    /// Get the weight for weighted fair queuing (higher = more slots)
    #[inline]
    pub fn weight(&self) -> u32 {
        match self {
            IoPriority::Critical => 16,
            IoPriority::High => 8,
            IoPriority::Normal => 4,
            IoPriority::Low => 2,
            IoPriority::Background => 1,
        }
    }
}

/// An I/O request with priority
#[derive(Debug)]
pub struct PrioritizedIoRequest<T> {
    pub priority: IoPriority,
    pub request: T,
    pub enqueue_time_ns: u64,
}

impl<T> PrioritizedIoRequest<T> {
    pub fn new(priority: IoPriority, request: T) -> Self {
        Self {
            priority,
            request,
            enqueue_time_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        }
    }

    /// Get queue wait time in nanoseconds
    pub fn wait_time_ns(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        now.saturating_sub(self.enqueue_time_ns)
    }
}

/// Priority queue for I/O requests
///
/// Uses separate queues per priority level with weighted fair queuing
/// to prevent starvation while maintaining priority ordering.
pub struct IoPriorityQueue<T> {
    queues: [VecDeque<PrioritizedIoRequest<T>>; 5],
    total_enqueued: AtomicU64,
    total_dequeued: AtomicU64,
    /// Tokens per priority for weighted fair queuing
    tokens: [u32; 5],
}

impl<T> IoPriorityQueue<T> {
    pub fn new() -> Self {
        Self {
            queues: [
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
            ],
            total_enqueued: AtomicU64::new(0),
            total_dequeued: AtomicU64::new(0),
            tokens: [16, 8, 4, 2, 1], // Initial tokens matching weights
        }
    }

    /// Enqueue a request at the given priority
    pub fn enqueue(&mut self, priority: IoPriority, request: T) {
        let idx = priority.level() as usize;
        self.queues[idx].push_back(PrioritizedIoRequest::new(priority, request));
        self.total_enqueued.fetch_add(1, Ordering::Relaxed);
    }

    /// Dequeue the next request using weighted fair queuing
    ///
    /// Returns requests in priority order, but uses tokens to prevent
    /// complete starvation of lower priority queues.
    pub fn dequeue(&mut self) -> Option<PrioritizedIoRequest<T>> {
        // First, try to find a non-empty queue with tokens
        for (idx, queue) in self.queues.iter_mut().enumerate() {
            if !queue.is_empty() && self.tokens[idx] > 0 {
                self.tokens[idx] -= 1;
                self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                return queue.pop_front();
            }
        }

        // If all tokens exhausted, reset and try again
        self.reset_tokens();

        for (idx, queue) in self.queues.iter_mut().enumerate() {
            if !queue.is_empty() {
                self.tokens[idx] = self.tokens[idx].saturating_sub(1);
                self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                return queue.pop_front();
            }
        }

        None
    }

    /// Dequeue strictly by priority (no fairness, for critical paths)
    pub fn dequeue_strict(&mut self) -> Option<PrioritizedIoRequest<T>> {
        for queue in self.queues.iter_mut() {
            if let Some(req) = queue.pop_front() {
                self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                return Some(req);
            }
        }
        None
    }

    fn reset_tokens(&mut self) {
        self.tokens = [
            IoPriority::Critical.weight(),
            IoPriority::High.weight(),
            IoPriority::Normal.weight(),
            IoPriority::Low.weight(),
            IoPriority::Background.weight(),
        ];
    }

    /// Get total pending requests across all priorities
    pub fn len(&self) -> usize {
        self.queues.iter().map(|q| q.len()).sum()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }

    /// Get pending count for a specific priority
    pub fn pending_at_priority(&self, priority: IoPriority) -> usize {
        self.queues[priority.level() as usize].len()
    }

    /// Get queue statistics
    pub fn stats(&self) -> IoPriorityStats {
        IoPriorityStats {
            pending_critical: self.queues[0].len(),
            pending_high: self.queues[1].len(),
            pending_normal: self.queues[2].len(),
            pending_low: self.queues[3].len(),
            pending_background: self.queues[4].len(),
            total_enqueued: self.total_enqueued.load(Ordering::Relaxed),
            total_dequeued: self.total_dequeued.load(Ordering::Relaxed),
        }
    }
}

impl<T> Default for IoPriorityQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for the priority queue
#[derive(Debug, Clone)]
pub struct IoPriorityStats {
    pub pending_critical: usize,
    pub pending_high: usize,
    pub pending_normal: usize,
    pub pending_low: usize,
    pub pending_background: usize,
    pub total_enqueued: u64,
    pub total_dequeued: u64,
}

impl IoPriorityStats {
    /// Total pending requests
    pub fn total_pending(&self) -> usize {
        self.pending_critical
            + self.pending_high
            + self.pending_normal
            + self.pending_low
            + self.pending_background
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_ordering() {
        assert!(IoPriority::Critical < IoPriority::High);
        assert!(IoPriority::High < IoPriority::Normal);
        assert!(IoPriority::Normal < IoPriority::Low);
        assert!(IoPriority::Low < IoPriority::Background);
    }

    #[test]
    fn test_priority_preempts() {
        assert!(IoPriority::Critical.preempts(IoPriority::High));
        assert!(IoPriority::High.preempts(IoPriority::Normal));
        assert!(!IoPriority::Normal.preempts(IoPriority::High));
    }

    #[test]
    fn test_priority_queue_strict() {
        let mut queue = IoPriorityQueue::<u32>::new();

        queue.enqueue(IoPriority::Low, 1);
        queue.enqueue(IoPriority::Critical, 2);
        queue.enqueue(IoPriority::Normal, 3);

        // Strict dequeue should return Critical first
        let req = queue.dequeue_strict().unwrap();
        assert_eq!(req.priority, IoPriority::Critical);
        assert_eq!(req.request, 2);

        let req = queue.dequeue_strict().unwrap();
        assert_eq!(req.priority, IoPriority::Normal);
        assert_eq!(req.request, 3);

        let req = queue.dequeue_strict().unwrap();
        assert_eq!(req.priority, IoPriority::Low);
        assert_eq!(req.request, 1);
    }

    #[test]
    fn test_priority_queue_weighted() {
        let mut queue = IoPriorityQueue::<u32>::new();

        // Add many requests at different priorities
        for i in 0..10 {
            queue.enqueue(IoPriority::High, i);
            queue.enqueue(IoPriority::Low, i + 100);
        }

        // With weighted dequeue, high priority gets more but low isn't starved
        let mut high_count = 0;
        let mut low_count = 0;

        while let Some(req) = queue.dequeue() {
            match req.priority {
                IoPriority::High => high_count += 1,
                IoPriority::Low => low_count += 1,
                _ => {},
            }
        }

        assert_eq!(high_count, 10);
        assert_eq!(low_count, 10);
    }

    #[test]
    fn test_priority_stats() {
        let mut queue = IoPriorityQueue::<u32>::new();

        queue.enqueue(IoPriority::Critical, 1);
        queue.enqueue(IoPriority::High, 2);
        queue.enqueue(IoPriority::Normal, 3);

        let stats = queue.stats();
        assert_eq!(stats.pending_critical, 1);
        assert_eq!(stats.pending_high, 1);
        assert_eq!(stats.pending_normal, 1);
        assert_eq!(stats.total_pending(), 3);
    }
}
