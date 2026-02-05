//! Circuit Breaker Pattern Implementation
//!
//! Prevents cascade failures by tracking failure rates and temporarily
//! rejecting requests when a downstream dependency is unhealthy.
//!
//! # States
//!
//! - **Closed**: Normal operation, requests flow through
//! - **Open**: Circuit is tripped, requests are rejected immediately
//! - **HalfOpen**: Testing if dependency has recovered
//!
//! # Usage
//!
//! ```rust,ignore
//! let breaker = CircuitBreaker::new(CircuitBreakerConfig::default());
//!
//! match breaker.check() {
//!     CircuitState::Closed => {
//!         match do_operation().await {
//!             Ok(_) => breaker.record_success(),
//!             Err(_) => breaker.record_failure(),
//!         }
//!     }
//!     CircuitState::Open => return Err("Service unavailable"),
//!     CircuitState::HalfOpen => { /* Limited requests allowed */ }
//! }
//! ```

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum CircuitState {
    /// Normal operation - requests flow through
    Closed,
    /// Circuit tripped - requests rejected immediately
    Open,
    /// Testing recovery - limited requests allowed
    HalfOpen,
}

/// Configuration for circuit breaker behavior
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CircuitBreakerConfig {
    /// Number of failures before tripping the circuit
    pub failure_threshold: u32,
    /// Number of successes in half-open to close the circuit
    pub success_threshold: u32,
    /// Time to wait before transitioning from open to half-open
    pub reset_timeout: Duration,
    /// Time window for counting failures
    pub failure_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            reset_timeout: Duration::from_secs(30),
            failure_window: Duration::from_secs(60),
        }
    }
}

#[allow(dead_code)]
impl CircuitBreakerConfig {
    /// Create a config for aggressive failure detection
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 2,
            reset_timeout: Duration::from_secs(10),
            failure_window: Duration::from_secs(30),
        }
    }

    /// Create a config for tolerant failure detection
    pub fn tolerant() -> Self {
        Self {
            failure_threshold: 10,
            success_threshold: 5,
            reset_timeout: Duration::from_secs(60),
            failure_window: Duration::from_secs(120),
        }
    }
}

/// Circuit breaker for preventing cascade failures
///
/// Thread-safe implementation using atomic operations.
#[allow(dead_code)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    /// Current failure count
    failure_count: AtomicU32,
    /// Current success count (used in half-open state)
    success_count: AtomicU32,
    /// Timestamp when circuit was opened (millis since epoch)
    opened_at: AtomicU64,
    /// Current state (0=Closed, 1=Open, 2=HalfOpen)
    state: AtomicU32,
    /// Last failure timestamp for window tracking
    last_failure_at: AtomicU64,
}

#[allow(dead_code)]
impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            opened_at: AtomicU64::new(0),
            state: AtomicU32::new(0), // Closed
            last_failure_at: AtomicU64::new(0),
        }
    }

    /// Check the current circuit state
    ///
    /// This also handles automatic state transitions based on timeouts.
    pub fn check(&self) -> CircuitState {
        let state_val = self.state.load(Ordering::Acquire);
        
        match state_val {
            0 => CircuitState::Closed,
            1 => {
                // Check if reset timeout has elapsed
                let opened_at = self.opened_at.load(Ordering::Relaxed);
                let now = Self::current_time_millis();
                
                if now.saturating_sub(opened_at) >= self.config.reset_timeout.as_millis() as u64 {
                    // Transition to half-open
                    if self.state.compare_exchange(1, 2, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        self.success_count.store(0, Ordering::Relaxed);
                        tracing::info!(target: "lance::circuit_breaker", "Circuit transitioning to half-open");
                    }
                    CircuitState::HalfOpen
                } else {
                    CircuitState::Open
                }
            }
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Fallback
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let state = self.state.load(Ordering::Acquire);
        
        match state {
            0 => {
                // In closed state, reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            2 => {
                // In half-open state, count successes
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                
                if count >= self.config.success_threshold {
                    // Enough successes, close the circuit
                    if self.state.compare_exchange(2, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        self.failure_count.store(0, Ordering::Relaxed);
                        self.success_count.store(0, Ordering::Relaxed);
                        tracing::info!(target: "lance::circuit_breaker", "Circuit closed after recovery");
                    }
                }
            }
            _ => {}
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let state = self.state.load(Ordering::Acquire);
        let now = Self::current_time_millis();
        
        match state {
            0 => {
                // In closed state, check failure window
                let last_failure = self.last_failure_at.load(Ordering::Relaxed);
                
                if now.saturating_sub(last_failure) > self.config.failure_window.as_millis() as u64 {
                    // Outside failure window, reset count
                    self.failure_count.store(1, Ordering::Relaxed);
                } else {
                    let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                    
                    if count >= self.config.failure_threshold {
                        // Trip the circuit
                        if self.state.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                            self.opened_at.store(now, Ordering::Relaxed);
                            tracing::warn!(
                                target: "lance::circuit_breaker",
                                failures = count,
                                "Circuit opened due to failures"
                            );
                        }
                    }
                }
                
                self.last_failure_at.store(now, Ordering::Relaxed);
            }
            2 => {
                // In half-open state, any failure reopens the circuit
                if self.state.compare_exchange(2, 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                    self.opened_at.store(now, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                    tracing::warn!(target: "lance::circuit_breaker", "Circuit reopened after half-open failure");
                }
            }
            _ => {}
        }
    }

    /// Force the circuit to open
    pub fn trip(&self) {
        let now = Self::current_time_millis();
        self.state.store(1, Ordering::Release);
        self.opened_at.store(now, Ordering::Relaxed);
        tracing::warn!(target: "lance::circuit_breaker", "Circuit manually tripped");
    }

    /// Force the circuit to close
    pub fn reset(&self) {
        self.state.store(0, Ordering::Release);
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
        tracing::info!(target: "lance::circuit_breaker", "Circuit manually reset");
    }

    /// Get current state without side effects
    pub fn state(&self) -> CircuitState {
        match self.state.load(Ordering::Relaxed) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Relaxed)
    }

    fn current_time_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_starts_closed() {
        let breaker = CircuitBreaker::default();
        assert_eq!(breaker.check(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_opens_after_threshold() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::new(config);

        // Record failures
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);
        
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);
        
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_success_resets_failure_count() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::new(config);

        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.failure_count(), 2);

        breaker.record_success();
        assert_eq!(breaker.failure_count(), 0);
    }

    #[test]
    fn test_manual_trip_and_reset() {
        let breaker = CircuitBreaker::default();
        
        assert_eq!(breaker.state(), CircuitState::Closed);
        
        breaker.trip();
        assert_eq!(breaker.state(), CircuitState::Open);
        
        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_config_presets() {
        let aggressive = CircuitBreakerConfig::aggressive();
        assert_eq!(aggressive.failure_threshold, 3);
        
        let tolerant = CircuitBreakerConfig::tolerant();
        assert_eq!(tolerant.failure_threshold, 10);
    }
}
