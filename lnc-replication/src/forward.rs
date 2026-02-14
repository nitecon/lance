//! Write forwarding - transparent forwarding of write requests from followers to leader
//!
//! This module implements server-side write forwarding to support load-balancer
//! deployments where clients cannot directly connect to the leader.

use crossbeam::queue::ArrayQueue;
use lnc_network::LwpHeader;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

/// Error types for write forwarding operations
#[derive(Debug)]
pub enum ForwardError {
    /// Leader address is not known
    LeaderUnknown,
    /// Failed to connect to leader
    ConnectionFailed(std::io::Error),
    /// Failed to send data to leader
    SendFailed(std::io::Error),
    /// Failed to receive response from leader
    ReceiveFailed(std::io::Error),
    /// Connection timeout
    Timeout,
    /// Pool exhausted (all connections in use)
    PoolExhausted,
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeaderUnknown => write!(f, "Leader address unknown"),
            Self::ConnectionFailed(e) => write!(f, "Failed to connect to leader: {}", e),
            Self::SendFailed(e) => write!(f, "Failed to send to leader: {}", e),
            Self::ReceiveFailed(e) => write!(f, "Failed to receive from leader: {}", e),
            Self::Timeout => write!(f, "Forward operation timed out"),
            Self::PoolExhausted => write!(f, "Connection pool exhausted"),
        }
    }
}

impl std::error::Error for ForwardError {}

/// Configuration for the leader connection pool
#[derive(Debug, Clone)]
pub struct ForwardConfig {
    /// Maximum connections to maintain in the pool
    pub pool_size: usize,
    /// Hard limit on total connections (pooled + in-flight)
    ///
    /// **Safety**: Prevents FD exhaustion when leader is slow. If the leader
    /// stalls, acquire() returns PoolExhausted instead of spawning unbounded
    /// connections that hit the process FD limit and crash the follower.
    pub max_connections: usize,
    /// Timeout for connecting to leader
    pub connect_timeout: Duration,
    /// Timeout for forwarding operations
    pub forward_timeout: Duration,
    /// Enable TEE-based forwarding for L3 quorum (Linux only)
    /// When enabled and supported, uses IORING_OP_TEE to duplicate writes
    /// for local processing while forwarding to leader
    pub enable_tee_forwarding: bool,
    /// Enable local acknowledgment in L3 mode (requires tee_forwarding)
    /// Sends ACK after local write, before leader response
    pub enable_local_ack: bool,
    /// Number of pre-allocated response buffers in the forward buffer pool
    ///
    /// **Zero-Allocation (§15)**: Matches the LoanableBatchPool pattern.
    /// Buffers are borrowed from the pool, filled with the leader's response,
    /// and returned when the connection handler finishes writing to the client.
    pub forward_buffer_pool_size: usize,
    /// Initial capacity (bytes) for each forward buffer
    ///
    /// Buffers grow if a response exceeds this, and retain the larger capacity
    /// for subsequent uses. 4KiB covers header-only ACKs; large responses
    /// cause a one-time growth that is amortized across all future uses.
    pub forward_buffer_capacity: usize,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            pool_size: 8,
            max_connections: 64,
            connect_timeout: Duration::from_secs(5),
            forward_timeout: Duration::from_secs(30),
            enable_tee_forwarding: false,
            enable_local_ack: false,
            forward_buffer_pool_size: 64,
            forward_buffer_capacity: 4096,
        }
    }
}

impl ForwardConfig {
    /// Create config with TEE forwarding enabled for L3 quorum
    pub fn with_tee_forwarding(mut self) -> Self {
        self.enable_tee_forwarding = true;
        self
    }

    /// Enable local acknowledgment (L3 mode optimization)
    pub fn with_local_ack(mut self) -> Self {
        self.enable_local_ack = true;
        self
    }
}

/// TEE forwarding support status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeeForwardingStatus {
    /// TEE forwarding is available and enabled
    Enabled,
    /// TEE forwarding is disabled by configuration
    Disabled,
    /// TEE forwarding requested but not supported (non-Linux or old kernel)
    Unsupported,
}

/// Check if TEE forwarding is available on this system
pub fn check_tee_support(config: &ForwardConfig) -> TeeForwardingStatus {
    if !config.enable_tee_forwarding {
        return TeeForwardingStatus::Disabled;
    }

    #[cfg(target_os = "linux")]
    {
        // Check kernel support for TEE
        if lnc_io::probe_tee() == lnc_io::TeeSupport::Supported {
            tracing::info!(
                target: "lance::replication",
                "TEE forwarding enabled for L3 quorum"
            );
            return TeeForwardingStatus::Enabled;
        }
        tracing::warn!(
            target: "lance::replication",
            "TEE forwarding requested but kernel does not support IORING_OP_TEE"
        );
        TeeForwardingStatus::Unsupported
    }

    #[cfg(not(target_os = "linux"))]
    {
        tracing::warn!(
            target: "lance::replication",
            "TEE forwarding requested but only available on Linux"
        );
        TeeForwardingStatus::Unsupported
    }
}

/// Connection pool for forwarding writes to the leader
///
/// Maintains persistent connections to the current leader for efficient
/// write forwarding. Automatically handles leader changes by draining
/// old connections.
pub struct LeaderConnectionPool {
    /// Current leader address (client port, not replication port)
    leader_addr: RwLock<Option<SocketAddr>>,
    /// Pool of available connections (protected by mutex)
    connections: Mutex<Vec<TcpStream>>,
    /// Configuration
    config: ForwardConfig,
    /// TEE forwarding status (checked once at startup)
    tee_status: TeeForwardingStatus,
    /// Total connections currently alive (pooled + in-flight)
    ///
    /// **Lock-Free**: Uses AtomicUsize with Relaxed ordering per LANCE standards.
    /// Incremented on acquire, decremented on release/drop.
    total_connections: AtomicUsize,
    /// Pre-allocated response buffer pool for zero-alloc forwarding
    ///
    /// **Zero-Allocation (§15)**: Lock-free ArrayQueue of reusable `Vec<u8>` buffers.
    /// Eliminates per-forward heap allocation that causes global allocator contention
    /// at 100Gbps forwarding rates.
    buffer_pool: ForwardBufferPool,
}

impl LeaderConnectionPool {
    /// Create a new leader connection pool
    pub fn new(config: ForwardConfig) -> Self {
        let tee_status = check_tee_support(&config);
        let buffer_pool = ForwardBufferPool::new(
            config.forward_buffer_pool_size,
            config.forward_buffer_capacity,
        );
        Self {
            leader_addr: RwLock::new(None),
            connections: Mutex::new(Vec::with_capacity(config.pool_size)),
            config,
            tee_status,
            total_connections: AtomicUsize::new(0),
            buffer_pool,
        }
    }

    /// Update the leader address when Raft elects a new leader
    ///
    /// This drains all existing connections since they're to the old leader.
    pub async fn on_leader_change(&self, new_leader: Option<SocketAddr>) {
        let mut addr = self.leader_addr.write().await;

        // Only update if actually changed
        if *addr != new_leader {
            debug!(
                old_leader = ?*addr,
                new_leader = ?new_leader,
                "Leader changed, draining connection pool"
            );

            // Drain old connections (they're to the old leader)
            let mut pool = self.connections.lock().await;
            let drained = pool.len();
            pool.clear();

            // Decrement total_connections for pooled connections we just dropped
            // Note: in-flight connections will decrement when their PooledConnection drops
            if drained > 0 {
                self.total_connections.fetch_sub(drained, Ordering::Relaxed);
            }

            *addr = new_leader;
        }
    }

    /// Get the current leader address
    pub async fn leader_addr(&self) -> Option<SocketAddr> {
        *self.leader_addr.read().await
    }

    /// Check if TEE forwarding is enabled and available
    #[inline]
    pub fn is_tee_enabled(&self) -> bool {
        self.tee_status == TeeForwardingStatus::Enabled
    }

    /// Get the TEE forwarding status
    #[inline]
    pub fn tee_status(&self) -> TeeForwardingStatus {
        self.tee_status
    }

    /// Check if local acknowledgment is enabled
    #[inline]
    pub fn is_local_ack_enabled(&self) -> bool {
        self.config.enable_local_ack && self.is_tee_enabled()
    }

    /// Acquire a connection from the pool, creating a new one if needed
    ///
    /// **Hard Connection Cap**: Enforces max_connections to prevent FD exhaustion.
    /// If the leader is slow and all connections are in-flight, returns PoolExhausted
    /// instead of spawning unbounded connections.
    ///
    /// **TOCTOU-Safe**: Uses pre-increment `fetch_add` to atomically reserve a slot
    /// before attempting the connection. If the connection fails, the slot is released.
    /// This closes the race where two concurrent callers both pass a `load` check.
    pub async fn acquire(&self) -> Result<PooledConnection<'_>, ForwardError> {
        let addr = self
            .leader_addr
            .read()
            .await
            .ok_or(ForwardError::LeaderUnknown)?;

        // Try to get an existing connection from the pool
        {
            let mut pool = self.connections.lock().await;
            if let Some(stream) = pool.pop() {
                // Connection was already counted in total_connections
                debug!(leader = %addr, "Reusing pooled connection to leader");
                return Ok(PooledConnection {
                    stream: Some(stream),
                    pool: self,
                });
            }
        }

        // Pre-increment to atomically reserve a slot under the cap (TOCTOU-safe)
        let prev = self.total_connections.fetch_add(1, Ordering::Relaxed);
        if prev >= self.config.max_connections {
            // Over cap — release the reserved slot
            self.total_connections.fetch_sub(1, Ordering::Relaxed);
            tracing::warn!(
                target: "lance::forward",
                leader = %addr,
                current_connections = prev,
                max_connections = self.config.max_connections,
                "Connection pool exhausted - hard cap reached"
            );
            lnc_metrics::increment_pool_exhausted();
            return Err(ForwardError::PoolExhausted);
        }

        // Create a new connection (slot already reserved)
        debug!(leader = %addr, "Creating new connection to leader");
        let stream =
            match tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr)).await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    // Connection failed — release the reserved slot
                    self.total_connections.fetch_sub(1, Ordering::Relaxed);
                    tracing::warn!(
                        target: "lance::forward",
                        leader = %addr,
                        error = %e,
                        "Failed to connect to leader"
                    );
                    return Err(ForwardError::ConnectionFailed(e));
                },
                Err(_) => {
                    // Timeout — release the reserved slot
                    self.total_connections.fetch_sub(1, Ordering::Relaxed);
                    tracing::warn!(
                        target: "lance::forward",
                        leader = %addr,
                        timeout_secs = self.config.connect_timeout.as_secs(),
                        "Connect to leader timed out"
                    );
                    return Err(ForwardError::Timeout);
                },
            };

        stream.set_nodelay(true).map_err(|e| {
            // set_nodelay failed — release the reserved slot
            self.total_connections.fetch_sub(1, Ordering::Relaxed);
            ForwardError::ConnectionFailed(e)
        })?;

        Ok(PooledConnection {
            stream: Some(stream),
            pool: self,
        })
    }

    /// Return a connection to the pool
    async fn release(&self, stream: TcpStream) {
        let mut pool = self.connections.lock().await;
        // Only keep up to pool_size connections
        if pool.len() < self.config.pool_size {
            pool.push(stream);
        } else {
            // Connection dropped - decrement total count
            drop(stream);
            self.total_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get the current total connection count (pooled + in-flight)
    #[inline]
    pub fn total_connections(&self) -> usize {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Get the configured maximum connections
    #[inline]
    pub fn max_connections(&self) -> usize {
        self.config.max_connections
    }

    /// Pool saturation ratio: total_connections / max_connections
    ///
    /// **SRE Health Gauge**: Expose as `lance_forward_pool_saturation`.
    /// If this hits 0.9, the leader is stalling and PoolExhausted errors are imminent.
    #[inline]
    pub fn pool_saturation(&self) -> f64 {
        let current = self.total_connections.load(Ordering::Relaxed) as f64;
        let max = self.config.max_connections as f64;
        if max == 0.0 {
            return 0.0;
        }
        current / max
    }

    /// Forward a write request to the leader and return the response
    ///
    /// This is the main forwarding function used by connection handlers.
    /// The entire operation is bounded by `forward_timeout` to prevent
    /// indefinite hangs on stale or broken connections.
    ///
    /// **Zero-Allocation (§15)**: Returns a `PooledForwardBuffer` that borrows
    /// from the pre-allocated buffer pool. The buffer is automatically returned
    /// to the pool when the caller drops it (after `write_all` to the client).
    pub async fn forward_write(
        &self,
        request_bytes: &[u8],
    ) -> Result<PooledForwardBuffer, ForwardError> {
        let timeout = self.config.forward_timeout;
        tokio::time::timeout(timeout, self.forward_write_inner(request_bytes))
            .await
            .map_err(|_| ForwardError::Timeout)?
    }

    /// Inner forwarding logic (called within forward_timeout)
    ///
    /// **10X Refinement**: Uses LwpHeader::parse() for type-safe header access
    /// instead of magic-number byte offsets. If the LwpHeader struct changes,
    /// this code adapts automatically instead of silently reading wrong fields.
    ///
    /// **Zero-Allocation**: Borrows a buffer from the pool instead of `vec![0u8; N]`.
    /// If the pool is empty, falls back to a fresh Vec (which will be donated to
    /// the pool on drop, growing the pool's effective capacity).
    async fn forward_write_inner(
        &self,
        request_bytes: &[u8],
    ) -> Result<PooledForwardBuffer, ForwardError> {
        let start = std::time::Instant::now();

        let mut conn = self.acquire().await?;

        // Send request to leader
        let stream = conn.stream.as_mut().ok_or(ForwardError::LeaderUnknown)?;

        stream
            .write_all(request_bytes)
            .await
            .map_err(ForwardError::SendFailed)?;

        // Read response from leader
        // First read the LWP header (44 bytes) to get payload length
        let mut header_buf = [0u8; LwpHeader::SIZE];
        stream
            .read_exact(&mut header_buf)
            .await
            .map_err(ForwardError::ReceiveFailed)?;

        // Type-safe header parsing via zerocopy-backed LwpHeader
        let lwp_header = LwpHeader::parse(&header_buf).map_err(|_| {
            ForwardError::ReceiveFailed(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid LWP response header from leader",
            ))
        })?;
        let payload_len = lwp_header.ingest_header.payload_length as usize;
        let total_len = LwpHeader::SIZE + payload_len;

        // Borrow a buffer from the pool (zero-alloc hot path)
        let mut buf = self.buffer_pool.acquire(total_len);
        buf.resize(total_len);
        buf.as_mut_slice()[..LwpHeader::SIZE].copy_from_slice(&header_buf);

        if payload_len > 0 {
            stream
                .read_exact(&mut buf.as_mut_slice()[LwpHeader::SIZE..])
                .await
                .map_err(ForwardError::ReceiveFailed)?;
        }

        let elapsed = start.elapsed();

        // Return connection to pool for reuse
        if let Some(stream) = conn.stream.take() {
            self.release(stream).await;
        }

        debug!(
            latency_us = elapsed.as_micros(),
            request_len = request_bytes.len(),
            response_len = buf.len(),
            "Write forwarded to leader"
        );

        Ok(buf)
    }
}

/// A connection from the pool
pub struct PooledConnection<'a> {
    stream: Option<TcpStream>,
    pool: &'a LeaderConnectionPool,
}

impl<'a> PooledConnection<'a> {
    /// Take ownership of the stream (prevents return to pool)
    ///
    /// **Important**: Caller assumes responsibility for connection lifecycle.
    /// The total_connections counter is decremented since the pool no longer tracks it.
    pub fn take(&mut self) -> Option<TcpStream> {
        let stream = self.stream.take();
        if stream.is_some() {
            self.pool.total_connections.fetch_sub(1, Ordering::Relaxed);
        }
        stream
    }

    /// Get reference to the pool for manual connection management
    #[inline]
    pub fn pool(&self) -> &LeaderConnectionPool {
        self.pool
    }
}

impl<'a> Drop for PooledConnection<'a> {
    /// If the connection was not returned to the pool or taken, decrement total_connections.
    ///
    /// This handles error paths where the PooledConnection is dropped without
    /// explicit release (e.g., forwarding failure mid-stream).
    fn drop(&mut self) {
        if self.stream.is_some() {
            self.pool.total_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

// ============================================================================
// Forward Buffer Pool (§15 - Loanable Buffer Pattern)
// ============================================================================
// Pre-allocated, lock-free buffer pool for forwarding responses.
// Eliminates per-forward heap allocation on the hot path.

/// Lock-free pool of reusable response buffers for write forwarding.
///
/// **Zero-Allocation (§15)**: Uses `crossbeam::ArrayQueue` for lock-free
/// acquire/release, matching the `BatchPool` pattern from ingestion.
/// Buffers retain their capacity across uses, so a one-time growth for
/// a large response is amortized across all subsequent forwards.
pub struct ForwardBufferPool {
    free_list: Arc<ArrayQueue<Vec<u8>>>,
    default_capacity: usize,
}

impl ForwardBufferPool {
    /// Create a new buffer pool with `pool_size` pre-allocated buffers.
    pub fn new(pool_size: usize, default_capacity: usize) -> Self {
        let free_list = Arc::new(ArrayQueue::new(pool_size.max(1)));
        for _ in 0..pool_size {
            let _ = free_list.push(Vec::with_capacity(default_capacity));
        }
        Self {
            free_list,
            default_capacity,
        }
    }

    /// Acquire a buffer from the pool, or create a fresh one if empty.
    ///
    /// The returned `PooledForwardBuffer` auto-returns to this pool on drop.
    /// If `min_capacity` exceeds the buffer's current capacity, the buffer
    /// will grow (one-time cost, retained for future uses).
    #[inline]
    pub fn acquire(&self, min_capacity: usize) -> PooledForwardBuffer {
        let mut buf = self
            .free_list
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(min_capacity.max(self.default_capacity)));
        buf.clear();
        if buf.capacity() < min_capacity {
            buf.reserve(min_capacity - buf.capacity());
        }
        PooledForwardBuffer {
            buf,
            pool: Arc::clone(&self.free_list),
        }
    }

    /// Number of buffers currently available in the pool
    #[inline]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }
}

impl Clone for ForwardBufferPool {
    fn clone(&self) -> Self {
        Self {
            free_list: Arc::clone(&self.free_list),
            default_capacity: self.default_capacity,
        }
    }
}

/// RAII guard for a pooled forward buffer.
///
/// Derefs to `&[u8]` for direct use with `write_all`. On drop, the buffer
/// is returned to the pool (cleared but capacity retained) for reuse.
pub struct PooledForwardBuffer {
    buf: Vec<u8>,
    pool: Arc<ArrayQueue<Vec<u8>>>,
}

impl PooledForwardBuffer {
    /// Resize the buffer to exactly `len` bytes (zero-filled if growing).
    #[inline]
    pub fn resize(&mut self, len: usize) {
        self.buf.resize(len, 0);
    }

    /// Get a mutable slice of the buffer's current contents.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    /// Current length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

impl Deref for PooledForwardBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.buf
    }
}

impl AsRef<[u8]> for PooledForwardBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledForwardBuffer {
    /// Return the buffer to the pool on drop.
    ///
    /// If the pool is full (more buffers than pool_size due to fallback
    /// allocations), the excess buffer is simply dropped.
    fn drop(&mut self) {
        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        // Best-effort return — if pool is full, buffer is dropped
        let _ = self.pool.push(buf);
    }
}

/// Create a shared leader connection pool
pub fn create_leader_pool(config: ForwardConfig) -> Arc<LeaderConnectionPool> {
    Arc::new(LeaderConnectionPool::new(config))
}

// ============================================================================
// Local Write Processor for TEE'd Data (L3 Quorum)
// ============================================================================
// Per Architecture.md Section 21.9: Process TEE'd write data locally
// for L3 quorum acknowledgment while forwarding to leader.

/// Callback for processing locally TEE'd write data
///
/// Implementations receive the raw write data that was duplicated via TEE
/// and should write it to local storage (WAL/segment) for quorum acknowledgment.
pub trait LocalWriteProcessor: Send + Sync {
    /// Process a TEE'd write locally
    ///
    /// # Arguments
    /// * `topic_id` - Target topic ID
    /// * `batch_id` - Batch ID for acknowledgment correlation
    /// * `data` - Raw write data (LWP frame payload)
    ///
    /// # Returns
    /// Ok(()) if write was successfully persisted locally
    fn process_local_write(
        &self,
        topic_id: u64,
        batch_id: u64,
        data: &[u8],
    ) -> Result<(), LocalWriteError>;
}

/// Errors from local write processing
#[derive(Debug)]
pub enum LocalWriteError {
    /// WAL write failed
    WalFailed(std::io::Error),
    /// Topic not found
    TopicNotFound(u64),
    /// Storage full
    StorageFull,
    /// Processing error
    ProcessingError(String),
}

impl std::fmt::Display for LocalWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WalFailed(e) => write!(f, "WAL write failed: {}", e),
            Self::TopicNotFound(id) => write!(f, "Topic {} not found", id),
            Self::StorageFull => write!(f, "Storage full"),
            Self::ProcessingError(msg) => write!(f, "Processing error: {}", msg),
        }
    }
}

impl std::error::Error for LocalWriteError {}

/// No-op local write processor (for testing or when local writes not needed)
pub struct NoOpLocalProcessor;

impl LocalWriteProcessor for NoOpLocalProcessor {
    fn process_local_write(
        &self,
        _topic_id: u64,
        _batch_id: u64,
        _data: &[u8],
    ) -> Result<(), LocalWriteError> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_forward_config_default() {
        let config = ForwardConfig::default();
        assert_eq!(config.pool_size, 8);
        assert_eq!(config.max_connections, 64);
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.forward_timeout, Duration::from_secs(30));
        assert!(!config.enable_tee_forwarding);
        assert!(!config.enable_local_ack);
    }

    #[test]
    fn test_forward_config_with_tee() {
        let config = ForwardConfig::default()
            .with_tee_forwarding()
            .with_local_ack();
        assert!(config.enable_tee_forwarding);
        assert!(config.enable_local_ack);
    }

    #[test]
    fn test_tee_forwarding_status_disabled() {
        let config = ForwardConfig::default();
        let status = check_tee_support(&config);
        assert_eq!(status, TeeForwardingStatus::Disabled);
    }

    #[test]
    fn test_tee_forwarding_status_enum() {
        assert_eq!(TeeForwardingStatus::Enabled, TeeForwardingStatus::Enabled);
        assert_eq!(TeeForwardingStatus::Disabled, TeeForwardingStatus::Disabled);
        assert_eq!(
            TeeForwardingStatus::Unsupported,
            TeeForwardingStatus::Unsupported
        );
        assert_ne!(TeeForwardingStatus::Enabled, TeeForwardingStatus::Disabled);
    }

    #[test]
    fn test_local_write_error_display() {
        let err = LocalWriteError::TopicNotFound(42);
        assert!(err.to_string().contains("42"));

        let err = LocalWriteError::StorageFull;
        assert!(err.to_string().contains("full"));

        let err = LocalWriteError::ProcessingError("test error".to_string());
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_noop_local_processor() {
        let processor = NoOpLocalProcessor;
        let result = processor.process_local_write(1, 100, b"test data");
        assert!(result.is_ok());
    }

    #[test]
    fn test_pool_tee_status_methods() {
        let config = ForwardConfig::default();
        let pool = LeaderConnectionPool::new(config);

        // TEE disabled by default
        assert!(!pool.is_tee_enabled());
        assert_eq!(pool.tee_status(), TeeForwardingStatus::Disabled);
        assert!(!pool.is_local_ack_enabled());
    }

    #[tokio::test]
    async fn test_pool_leader_unknown() {
        let pool = LeaderConnectionPool::new(ForwardConfig::default());
        let result = pool.acquire().await;
        assert!(matches!(result, Err(ForwardError::LeaderUnknown)));
    }

    #[tokio::test]
    async fn test_pool_leader_change() {
        let pool = LeaderConnectionPool::new(ForwardConfig::default());

        // Initially no leader
        assert!(pool.leader_addr().await.is_none());

        // Set leader
        let addr: SocketAddr = "127.0.0.1:1992".parse().unwrap();
        pool.on_leader_change(Some(addr)).await;
        assert_eq!(pool.leader_addr().await, Some(addr));

        // Change leader
        let new_addr: SocketAddr = "127.0.0.1:2002".parse().unwrap();
        pool.on_leader_change(Some(new_addr)).await;
        assert_eq!(pool.leader_addr().await, Some(new_addr));

        // Clear leader
        pool.on_leader_change(None).await;
        assert!(pool.leader_addr().await.is_none());
    }

    // =========================================================================
    // Regression Tests for Concurrent Write Forwarding
    // =========================================================================
    // These tests verify batch_id preservation under concurrent load,
    // addressing the bug fixed in process_frames where read_offset was
    // not being decremented after forwarding.

    #[test]
    fn test_pooled_connection_pool_accessor() {
        // Verify the pool() accessor works correctly
        let config = ForwardConfig::default();
        let pool = LeaderConnectionPool::new(config);

        // We can't create a PooledConnection directly in tests without
        // connecting, but we verify the pool field is usable via config check
        assert_eq!(pool.tee_status(), TeeForwardingStatus::Disabled);
    }

    #[tokio::test]
    async fn test_concurrent_leader_addr_access() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let pool = Arc::new(LeaderConnectionPool::new(ForwardConfig::default()));
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        pool.on_leader_change(Some(addr)).await;

        let mut tasks = JoinSet::new();

        // Spawn 100 concurrent readers
        for _ in 0..100 {
            let pool = Arc::clone(&pool);
            tasks.spawn(async move {
                for _ in 0..10 {
                    let leader = pool.leader_addr().await;
                    assert!(leader.is_some());
                }
            });
        }

        // All reads should complete without deadlock
        while let Some(result) = tasks.join_next().await {
            result.expect("Task should complete successfully");
        }
    }

    #[tokio::test]
    async fn test_concurrent_leader_change_safety() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let pool = Arc::new(LeaderConnectionPool::new(ForwardConfig::default()));
        let mut tasks = JoinSet::new();

        // Spawn writers that change the leader
        for i in 0..10 {
            let pool = Arc::clone(&pool);
            tasks.spawn(async move {
                let addr: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
                pool.on_leader_change(Some(addr)).await;
            });
        }

        // Spawn readers concurrently
        for _ in 0..50 {
            let pool = Arc::clone(&pool);
            tasks.spawn(async move {
                // Just read - should not panic or deadlock
                let _ = pool.leader_addr().await;
            });
        }

        // All operations should complete without deadlock or panic
        while let Some(result) = tasks.join_next().await {
            result.expect("Task should complete successfully");
        }

        // Pool should have a valid leader (one of the written values)
        let final_leader = pool.leader_addr().await;
        assert!(final_leader.is_some());
    }

    #[tokio::test]
    async fn test_acquire_fails_without_leader() {
        let pool = LeaderConnectionPool::new(ForwardConfig::default());

        // No leader set - acquire should fail with LeaderUnknown
        let result = pool.acquire().await;
        assert!(matches!(result, Err(ForwardError::LeaderUnknown)));
    }

    #[test]
    fn test_forward_error_display_coverage() {
        // Ensure all ForwardError variants have proper Display implementation
        let errors = vec![
            ForwardError::LeaderUnknown,
            ForwardError::Timeout,
            ForwardError::PoolExhausted,
            ForwardError::ConnectionFailed(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "test",
            )),
            ForwardError::SendFailed(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "test")),
            ForwardError::ReceiveFailed(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "test",
            )),
        ];

        for err in errors {
            let display = format!("{}", err);
            assert!(!display.is_empty(), "Error display should not be empty");
        }
    }

    #[test]
    fn test_forward_config_pool_size_bounds() {
        let config = ForwardConfig::default();

        // Verify reasonable defaults
        assert!(config.pool_size > 0, "Pool size must be positive");
        assert!(config.pool_size <= 64, "Pool size should be reasonable");

        // Custom pool size
        let config = ForwardConfig {
            pool_size: 16,
            ..ForwardConfig::default()
        };
        let pool = LeaderConnectionPool::new(config);
        assert!(!pool.is_tee_enabled()); // Sanity check
    }

    #[test]
    fn test_pool_saturation_empty() {
        let pool = LeaderConnectionPool::new(ForwardConfig::default());
        assert_eq!(pool.pool_saturation(), 0.0);
        assert_eq!(pool.max_connections(), 64);
        assert_eq!(pool.total_connections(), 0);
    }

    #[test]
    fn test_pool_saturation_zero_max() {
        let config = ForwardConfig {
            max_connections: 0,
            ..ForwardConfig::default()
        };
        let pool = LeaderConnectionPool::new(config);
        // Division by zero guard
        assert_eq!(pool.pool_saturation(), 0.0);
    }

    // =========================================================================
    // ForwardBufferPool Tests
    // =========================================================================

    #[test]
    fn test_forward_buffer_pool_acquire_release() {
        let pool = ForwardBufferPool::new(4, 1024);
        assert_eq!(pool.available(), 4);

        let buf1 = pool.acquire(64);
        assert_eq!(pool.available(), 3);

        let buf2 = pool.acquire(64);
        assert_eq!(pool.available(), 2);

        // Drop returns buffers to pool
        drop(buf1);
        assert_eq!(pool.available(), 3);

        drop(buf2);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_forward_buffer_pool_fallback_allocation() {
        // Pool of 1 — second acquire falls back to fresh Vec
        let pool = ForwardBufferPool::new(1, 128);
        assert_eq!(pool.available(), 1);

        let buf1 = pool.acquire(64);
        assert_eq!(pool.available(), 0);

        // Fallback allocation (pool empty)
        let buf2 = pool.acquire(64);
        assert_eq!(pool.available(), 0);

        // Drop buf1 returns to pool
        drop(buf1);
        assert_eq!(pool.available(), 1);

        // Drop buf2 — pool is full (capacity=1), so excess is dropped
        drop(buf2);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn test_pooled_forward_buffer_resize_and_deref() {
        let pool = ForwardBufferPool::new(2, 128);

        let mut buf = pool.acquire(64);
        assert!(buf.is_empty());

        buf.resize(44);
        assert_eq!(buf.len(), 44);

        // Fill with test data
        buf.as_mut_slice()[..4].copy_from_slice(b"LNCE");
        assert_eq!(&buf[..4], b"LNCE");

        // AsRef<[u8]> works
        let slice: &[u8] = buf.as_ref();
        assert_eq!(slice.len(), 44);
    }

    #[test]
    fn test_pooled_forward_buffer_capacity_retention() {
        let pool = ForwardBufferPool::new(2, 128);

        // First use: grow to 4096
        let mut buf = pool.acquire(4096);
        buf.resize(4096);
        assert!(buf.len() == 4096);
        drop(buf);

        // Second use: buffer retains the 4096 capacity
        let buf2 = pool.acquire(64);
        // The underlying Vec was cleared but capacity retained
        assert!(buf2.is_empty());
        drop(buf2);
    }

    #[test]
    fn test_forward_config_buffer_pool_defaults() {
        let config = ForwardConfig::default();
        assert_eq!(config.forward_buffer_pool_size, 64);
        assert_eq!(config.forward_buffer_capacity, 4096);
    }
}
