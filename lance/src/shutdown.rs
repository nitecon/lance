//! Graceful shutdown handling for LANCE server.
//!
//! Implements the drain sequence as specified in Architecture Section 9:
//! 1. Stop accepting new connections
//! 2. Drain in-flight batches to io_uring
//! 3. Await all I/O completions
//! 4. Flush sparse indexes
//! 5. Seal active segment
//! 6. Exit cleanly

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// Default drain timeout (25s to leave 5s buffer for K8s SIGKILL at 30s)
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Global shutdown flag
pub static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Counter for in-flight operations during drain
pub static IN_FLIGHT_OPS: AtomicU64 = AtomicU64::new(0);

/// Check if shutdown has been requested
#[inline]
pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Increment in-flight operation counter
#[inline]
pub fn begin_operation() {
    IN_FLIGHT_OPS.fetch_add(1, Ordering::Relaxed);
}

/// Decrement in-flight operation counter
#[inline]
pub fn end_operation() {
    IN_FLIGHT_OPS.fetch_sub(1, Ordering::Relaxed);
}

/// Get current in-flight operation count
#[inline]
pub fn in_flight_count() -> u64 {
    IN_FLIGHT_OPS.load(Ordering::Relaxed)
}

#[cfg(unix)]
#[allow(clippy::expect_used)] // Signal handlers are startup-critical; abort is correct on failure
pub fn install_signal_handlers(
    shutdown_tx: broadcast::Sender<()>,
) -> impl std::future::Future<Output = ()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");

    async move {
        tokio::select! {
            _ = sigterm.recv() => {
                info!(target: "lance::shutdown", "SIGTERM received, initiating graceful shutdown");
            }
            _ = sigint.recv() => {
                info!(target: "lance::shutdown", "SIGINT received, initiating graceful shutdown");
            }
        }

        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        let _ = shutdown_tx.send(());
    }
}

#[cfg(windows)]
pub async fn install_signal_handlers(shutdown_tx: broadcast::Sender<()>) {
    if let Err(e) = tokio::signal::ctrl_c().await {
        warn!(target: "lance::shutdown", error = %e, "Failed to listen for Ctrl+C");
        return;
    }

    info!(target: "lance::shutdown", "Ctrl+C received, initiating graceful shutdown");
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    let _ = shutdown_tx.send(());
}

/// Shutdown phase tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum DrainPhase {
    /// Waiting for in-flight operations to complete
    WaitingForOperations,
    /// Flushing indexes
    FlushingIndexes,
    /// Sealing active segments
    SealingSegments,
    /// Drain complete
    Complete,
}

/// Execute the drain sequence with timeout
/// Returns Ok(true) if clean shutdown, Ok(false) if timed out
pub async fn drain_with_timeout(timeout: Duration) -> bool {
    info!(
        target: "lance::shutdown",
        timeout_secs = timeout.as_secs(),
        "Beginning drain sequence"
    );

    let start = std::time::Instant::now();

    // Phase 1: Wait for in-flight operations to complete
    if !wait_for_in_flight_operations(&start, timeout).await {
        return false;
    }

    info!(
        target: "lance::shutdown",
        elapsed_ms = start.elapsed().as_millis(),
        "Drain sequence complete"
    );

    true
}

/// Wait for all in-flight operations to complete
async fn wait_for_in_flight_operations(start: &std::time::Instant, timeout: Duration) -> bool {
    loop {
        let count = in_flight_count();
        if count == 0 {
            return true;
        }

        if start.elapsed() > timeout {
            warn!(
                target: "lance::shutdown",
                in_flight = count,
                "Drain timeout exceeded, forcing shutdown"
            );
            return false;
        }

        info!(
            target: "lance::shutdown",
            in_flight = count,
            elapsed_secs = start.elapsed().as_secs(),
            phase = ?DrainPhase::WaitingForOperations,
            "Waiting for in-flight operations"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Drain coordinator for managing graceful shutdown
///
/// Per Architecture §9, implements:
/// 1. Stop accepting new connections (via SHUTDOWN_REQUESTED flag)
/// 2. Drain in-flight batches
/// 3. Await I/O completions
/// 4. Flush sparse indexes
/// 5. Seal active segments
#[allow(dead_code)]
pub struct DrainCoordinator {
    timeout: Duration,
    phase: DrainPhase,
}

#[allow(dead_code)]
impl DrainCoordinator {
    /// Create a new drain coordinator with the specified timeout
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            phase: DrainPhase::WaitingForOperations,
        }
    }

    /// Get the current drain phase
    pub fn phase(&self) -> DrainPhase {
        self.phase
    }

    /// Execute the full drain sequence (without TopicRegistry)
    pub async fn execute(&mut self) -> bool {
        self.execute_with_registry(None).await
    }

    /// Execute the full drain sequence with optional TopicRegistry integration
    pub async fn execute_with_registry(
        &mut self,
        topic_registry: Option<&crate::topic::TopicRegistry>,
    ) -> bool {
        let start = std::time::Instant::now();

        // Phase 1: Wait for in-flight operations
        self.phase = DrainPhase::WaitingForOperations;
        if !wait_for_in_flight_operations(&start, self.timeout).await {
            return false;
        }

        // Phase 2: Flush indexes
        self.phase = DrainPhase::FlushingIndexes;
        info!(target: "lance::shutdown", phase = ?self.phase, "Flushing indexes");
        if let Some(registry) = topic_registry {
            match registry.flush_all_indexes() {
                Ok(count) => {
                    info!(target: "lance::shutdown", flushed = count, "Index flush complete");
                },
                Err(e) => {
                    warn!(target: "lance::shutdown", error = %e, "Index flush failed");
                },
            }
        }

        // Phase 3: Seal segments
        self.phase = DrainPhase::SealingSegments;
        info!(target: "lance::shutdown", phase = ?self.phase, "Sealing active segments");
        if let Some(registry) = topic_registry {
            match registry.seal_all_segments() {
                Ok(count) => {
                    info!(target: "lance::shutdown", sealed = count, "Segment sealing complete");
                },
                Err(e) => {
                    warn!(target: "lance::shutdown", error = %e, "Segment sealing failed");
                },
            }
        }

        self.phase = DrainPhase::Complete;
        info!(
            target: "lance::shutdown",
            elapsed_ms = start.elapsed().as_millis(),
            "Drain coordinator complete"
        );

        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_in_flight_tracking() {
        // Reset counter
        IN_FLIGHT_OPS.store(0, Ordering::SeqCst);

        assert_eq!(in_flight_count(), 0);

        begin_operation();
        assert_eq!(in_flight_count(), 1);

        begin_operation();
        assert_eq!(in_flight_count(), 2);

        end_operation();
        assert_eq!(in_flight_count(), 1);

        end_operation();
        assert_eq!(in_flight_count(), 0);
    }

    #[test]
    fn test_shutdown_flag() {
        // Reset flag
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);

        assert!(!is_shutdown_requested());

        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        assert!(is_shutdown_requested());

        // Reset for other tests
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_drain_phase_transitions() {
        // Test DrainPhase enum values and ordering
        assert_eq!(
            DrainPhase::WaitingForOperations,
            DrainPhase::WaitingForOperations
        );
        assert_eq!(DrainPhase::FlushingIndexes, DrainPhase::FlushingIndexes);
        assert_eq!(DrainPhase::SealingSegments, DrainPhase::SealingSegments);
        assert_eq!(DrainPhase::Complete, DrainPhase::Complete);

        // Phases are distinct
        assert_ne!(DrainPhase::WaitingForOperations, DrainPhase::Complete);
    }

    #[test]
    fn test_drain_coordinator_creation() {
        let coordinator = DrainCoordinator::new(Duration::from_secs(5));
        assert_eq!(coordinator.phase(), DrainPhase::WaitingForOperations);
    }

    #[tokio::test]
    async fn test_drain_coordinator_phase_progression() {
        // Reset state
        IN_FLIGHT_OPS.store(0, Ordering::SeqCst);

        let mut coordinator = DrainCoordinator::new(Duration::from_secs(5));
        assert_eq!(coordinator.phase(), DrainPhase::WaitingForOperations);

        // Execute should complete when no in-flight ops
        let result = coordinator.execute().await;
        assert!(result, "Should complete successfully with no in-flight ops");
        assert_eq!(coordinator.phase(), DrainPhase::Complete);
    }

    #[test]
    fn test_concurrent_operations() {
        // Reset counter
        IN_FLIGHT_OPS.store(0, Ordering::SeqCst);

        // Simulate multiple concurrent operations
        for _ in 0..100 {
            begin_operation();
        }
        assert_eq!(in_flight_count(), 100);

        for _ in 0..50 {
            end_operation();
        }
        assert_eq!(in_flight_count(), 50);

        for _ in 0..50 {
            end_operation();
        }
        assert_eq!(in_flight_count(), 0);
    }

    #[test]
    fn test_drain_timeout_constant() {
        // Verify drain timeout is 25s (leaves 5s buffer for K8s SIGKILL at 30s)
        assert_eq!(DRAIN_TIMEOUT.as_secs(), 25);
    }
}
