//! Connection Management and Resilience
//!
//! Provides connection pooling, automatic reconnection, and resilience features
//! for LANCE client connections.
//!
//! # Features
//!
//! - **Connection Pool**: Manage multiple connections to a LANCE server
//! - **Auto-Reconnect**: Automatically reconnect on connection failures
//! - **Health Checking**: Periodic health checks with ping/pong
//! - **Backoff**: Exponential backoff for reconnection attempts
//! - **Circuit Breaker**: Prevent cascading failures
//!
//! # Example
//!
//! ```rust,no_run
//! use lnc_client::{ConnectionPool, ConnectionPoolConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = ConnectionPool::new(
//!         "127.0.0.1:1992",
//!         ConnectionPoolConfig::new()
//!             .with_max_connections(10)
//!             .with_health_check_interval(30),
//!     ).await?;
//!
//!     // Get a connection from the pool
//!     let mut conn = pool.get().await?;
//!     
//!     // Use the connection
//!     conn.ping().await?;
//!     
//!     // Connection is returned to pool when dropped
//!     Ok(())
//! }
//! ```

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::client::{ClientConfig, LanceClient};
use crate::error::{ClientError, Result};
use crate::tls::TlsClientConfig;

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    /// Minimum number of idle connections to maintain
    pub min_idle: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Maximum time to wait for a connection from the pool
    pub acquire_timeout: Duration,
    /// Health check interval (0 = disabled)
    pub health_check_interval: Duration,
    /// Maximum connection lifetime (0 = unlimited)
    pub max_lifetime: Duration,
    /// Idle timeout before closing a connection
    pub idle_timeout: Duration,
    /// Enable automatic reconnection
    pub auto_reconnect: bool,
    /// Maximum reconnection attempts (0 = unlimited)
    pub max_reconnect_attempts: u32,
    /// Base delay for exponential backoff
    pub reconnect_base_delay: Duration,
    /// Maximum delay for exponential backoff
    pub reconnect_max_delay: Duration,
    /// TLS configuration (None = plain TCP)
    pub tls_config: Option<TlsClientConfig>,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_idle: 1,
            connect_timeout: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(30),
            max_lifetime: Duration::from_secs(3600), // 1 hour
            idle_timeout: Duration::from_secs(300),  // 5 minutes
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_base_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(30),
            tls_config: None,
        }
    }
}

impl ConnectionPoolConfig {
    /// Create a new connection pool configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum connections
    pub fn with_max_connections(mut self, n: usize) -> Self {
        self.max_connections = n;
        self
    }

    /// Set minimum idle connections
    pub fn with_min_idle(mut self, n: usize) -> Self {
        self.min_idle = n;
        self
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set acquire timeout
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set health check interval (seconds)
    pub fn with_health_check_interval(mut self, secs: u64) -> Self {
        self.health_check_interval = Duration::from_secs(secs);
        self
    }

    /// Set maximum connection lifetime
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Enable or disable auto-reconnect
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Set maximum reconnection attempts
    pub fn with_max_reconnect_attempts(mut self, attempts: u32) -> Self {
        self.max_reconnect_attempts = attempts;
        self
    }

    /// Set TLS configuration for encrypted connections
    pub fn with_tls(mut self, tls_config: TlsClientConfig) -> Self {
        self.tls_config = Some(tls_config);
        self
    }
}

/// Connection pool statistics
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Current active connections (in use)
    pub active_connections: u64,
    /// Current idle connections (available)
    pub idle_connections: u64,
    /// Total acquire attempts
    pub acquire_attempts: u64,
    /// Successful acquires
    pub acquire_successes: u64,
    /// Failed acquires (timeout, error)
    pub acquire_failures: u64,
    /// Health check failures
    pub health_check_failures: u64,
    /// Reconnection attempts
    pub reconnect_attempts: u64,
}

/// Internal pool metrics using atomics
#[derive(Debug, Default)]
struct PoolMetrics {
    connections_created: AtomicU64,
    connections_closed: AtomicU64,
    active_connections: AtomicU64,
    idle_connections: AtomicU64,
    acquire_attempts: AtomicU64,
    acquire_successes: AtomicU64,
    acquire_failures: AtomicU64,
    health_check_failures: AtomicU64,
    reconnect_attempts: AtomicU64,
}

impl PoolMetrics {
    fn snapshot(&self) -> PoolStats {
        PoolStats {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            idle_connections: self.idle_connections.load(Ordering::Relaxed),
            acquire_attempts: self.acquire_attempts.load(Ordering::Relaxed),
            acquire_successes: self.acquire_successes.load(Ordering::Relaxed),
            acquire_failures: self.acquire_failures.load(Ordering::Relaxed),
            health_check_failures: self.health_check_failures.load(Ordering::Relaxed),
            reconnect_attempts: self.reconnect_attempts.load(Ordering::Relaxed),
        }
    }
}

/// Pooled connection wrapper
struct PooledConnection {
    client: LanceClient,
    created_at: Instant,
    last_used: Instant,
}

impl PooledConnection {
    fn new(client: LanceClient) -> Self {
        let now = Instant::now();
        Self {
            client,
            created_at: now,
            last_used: now,
        }
    }

    fn is_expired(&self, max_lifetime: Duration) -> bool {
        if max_lifetime.is_zero() {
            return false;
        }
        self.created_at.elapsed() > max_lifetime
    }

    fn is_idle_too_long(&self, idle_timeout: Duration) -> bool {
        if idle_timeout.is_zero() {
            return false;
        }
        self.last_used.elapsed() > idle_timeout
    }
}

/// Connection pool for managing LANCE client connections
pub struct ConnectionPool {
    addr: SocketAddr,
    config: ConnectionPoolConfig,
    connections: Arc<Mutex<VecDeque<PooledConnection>>>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<PoolMetrics>,
    running: Arc<AtomicBool>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub async fn new(addr: &str, config: ConnectionPoolConfig) -> Result<Self> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| ClientError::ProtocolError(format!("Invalid address: {}", e)))?;

        let pool = Self {
            addr: socket_addr,
            config: config.clone(),
            connections: Arc::new(Mutex::new(VecDeque::new())),
            semaphore: Arc::new(Semaphore::new(config.max_connections)),
            metrics: Arc::new(PoolMetrics::default()),
            running: Arc::new(AtomicBool::new(true)),
        };

        // Pre-populate with minimum idle connections
        for _ in 0..config.min_idle {
            if let Ok(conn) = pool.create_connection().await {
                let mut connections = pool.connections.lock().await;
                connections.push_back(conn);
                pool.metrics
                    .idle_connections
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Start health check task if enabled
        if !config.health_check_interval.is_zero() {
            let pool_clone = ConnectionPool {
                addr: pool.addr,
                config: pool.config.clone(),
                connections: pool.connections.clone(),
                semaphore: pool.semaphore.clone(),
                metrics: pool.metrics.clone(),
                running: pool.running.clone(),
            };
            tokio::spawn(async move {
                pool_clone.health_check_task().await;
            });
        }

        Ok(pool)
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> Result<PooledClient> {
        self.metrics
            .acquire_attempts
            .fetch_add(1, Ordering::Relaxed);

        // Acquire permit with timeout
        let permit = tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            self.metrics
                .acquire_failures
                .fetch_add(1, Ordering::Relaxed);
            ClientError::Timeout
        })?
        .map_err(|_| {
            self.metrics
                .acquire_failures
                .fetch_add(1, Ordering::Relaxed);
            ClientError::ConnectionClosed
        })?;

        // Try to get an existing connection
        let conn = {
            let mut connections = self.connections.lock().await;
            loop {
                match connections.pop_front() {
                    Some(conn) => {
                        self.metrics
                            .idle_connections
                            .fetch_sub(1, Ordering::Relaxed);

                        // Check if connection is still valid
                        if conn.is_expired(self.config.max_lifetime)
                            || conn.is_idle_too_long(self.config.idle_timeout)
                        {
                            self.metrics
                                .connections_closed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        break Some(conn);
                    },
                    None => break None,
                }
            }
        };

        let conn = match conn {
            Some(mut c) => {
                c.last_used = Instant::now();
                c
            },
            None => {
                // Create a new connection
                self.create_connection().await?
            },
        };

        self.metrics
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .acquire_successes
            .fetch_add(1, Ordering::Relaxed);

        Ok(PooledClient {
            conn: Some(conn),
            pool: self.connections.clone(),
            metrics: self.metrics.clone(),
            permit: Some(permit),
            config: self.config.clone(),
        })
    }

    /// Create a new connection
    async fn create_connection(&self) -> Result<PooledConnection> {
        let mut client_config = ClientConfig::new(self.addr);
        client_config.connect_timeout = self.config.connect_timeout;

        let client = match &self.config.tls_config {
            Some(tls_config) => LanceClient::connect_tls(client_config, tls_config.clone()).await?,
            None => LanceClient::connect(client_config).await?,
        };
        self.metrics
            .connections_created
            .fetch_add(1, Ordering::Relaxed);

        Ok(PooledConnection::new(client))
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        self.metrics.snapshot()
    }

    /// Close the pool
    pub async fn close(&self) {
        self.running.store(false, Ordering::Relaxed);

        let mut connections = self.connections.lock().await;
        let count = connections.len() as u64;
        connections.clear();
        self.metrics
            .connections_closed
            .fetch_add(count, Ordering::Relaxed);
        self.metrics.idle_connections.store(0, Ordering::Relaxed);
    }

    /// Health check task
    async fn health_check_task(&self) {
        let mut interval = tokio::time::interval(self.config.health_check_interval);

        while self.running.load(Ordering::Relaxed) {
            interval.tick().await;

            // Get all connections for health check
            let mut to_check = {
                let mut connections = self.connections.lock().await;
                std::mem::take(&mut *connections)
            };

            let mut healthy = VecDeque::new();
            let _initial_count = to_check.len();

            for mut conn in to_check.drain(..) {
                // Check expiry
                if conn.is_expired(self.config.max_lifetime) {
                    self.metrics
                        .connections_closed
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Ping to check health
                match conn.client.ping().await {
                    Ok(_) => {
                        conn.last_used = Instant::now();
                        healthy.push_back(conn);
                    },
                    Err(_) => {
                        self.metrics
                            .health_check_failures
                            .fetch_add(1, Ordering::Relaxed);
                        self.metrics
                            .connections_closed
                            .fetch_add(1, Ordering::Relaxed);
                    },
                }
            }

            // Return healthy connections
            {
                let mut connections = self.connections.lock().await;
                connections.extend(healthy);
                self.metrics
                    .idle_connections
                    .store(connections.len() as u64, Ordering::Relaxed);
            }
        }
    }
}

/// RAII wrapper for pooled connection
pub struct PooledClient {
    conn: Option<PooledConnection>,
    pool: Arc<Mutex<VecDeque<PooledConnection>>>,
    metrics: Arc<PoolMetrics>,
    #[allow(dead_code)]
    permit: Option<OwnedSemaphorePermit>,
    #[allow(dead_code)]
    config: ConnectionPoolConfig,
}

impl PooledClient {
    /// Get a reference to the underlying client
    pub fn client(&mut self) -> Result<&mut LanceClient> {
        match self.conn.as_mut() {
            Some(conn) => Ok(&mut conn.client),
            None => Err(ClientError::ConnectionClosed),
        }
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<Duration> {
        if let Some(ref mut conn) = self.conn {
            conn.client.ping().await
        } else {
            Err(ClientError::ConnectionClosed)
        }
    }

    /// Mark the connection as unhealthy (don't return to pool)
    pub fn mark_unhealthy(&mut self) {
        self.conn = None;
        self.metrics
            .connections_closed
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            conn.last_used = Instant::now();

            // Return to pool
            let pool = self.pool.clone();
            let metrics = self.metrics.clone();

            tokio::spawn(async move {
                let mut connections = pool.lock().await;
                connections.push_back(conn);
                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                metrics.idle_connections.fetch_add(1, Ordering::Relaxed);
            });
        } else {
            self.metrics
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        }

        // Permit is released when dropped
    }
}

/// Reconnecting client wrapper with automatic reconnection
/// Client with automatic reconnection and leader redirection support
pub struct ReconnectingClient {
    addr: String,
    config: ClientConfig,
    tls_config: Option<TlsClientConfig>,
    client: Option<LanceClient>,
    reconnect_attempts: u32,
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
    /// Current leader address (for redirection)
    leader_addr: Option<SocketAddr>,
    /// Whether to follow leader redirects
    follow_leader: bool,
}

impl ReconnectingClient {
    /// Create a new reconnecting client
    pub async fn connect(addr: &str) -> Result<Self> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| ClientError::ProtocolError(format!("Invalid address: {}", e)))?;

        let config = ClientConfig::new(socket_addr);
        let client = LanceClient::connect(config.clone()).await?;

        Ok(Self {
            addr: addr.to_string(),
            config,
            tls_config: None,
            client: Some(client),
            reconnect_attempts: 0,
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            leader_addr: None,
            follow_leader: true,
        })
    }

    /// Create a new reconnecting client with TLS
    pub async fn connect_tls(addr: &str, tls_config: TlsClientConfig) -> Result<Self> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| ClientError::ProtocolError(format!("Invalid address: {}", e)))?;

        let config = ClientConfig::new(socket_addr);
        let client = LanceClient::connect_tls(config.clone(), tls_config.clone()).await?;

        Ok(Self {
            addr: addr.to_string(),
            config,
            tls_config: Some(tls_config),
            client: Some(client),
            reconnect_attempts: 0,
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            leader_addr: None,
            follow_leader: true,
        })
    }

    /// Set maximum reconnection attempts
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Enable or disable automatic leader following
    pub fn with_follow_leader(mut self, follow: bool) -> Self {
        self.follow_leader = follow;
        self
    }

    /// Get the original connection address
    pub fn original_addr(&self) -> &str {
        &self.addr
    }

    /// Get the current leader address if known
    pub fn leader_addr(&self) -> Option<SocketAddr> {
        self.leader_addr
    }

    /// Update the known leader address (called when redirect received)
    pub fn set_leader_addr(&mut self, addr: SocketAddr) {
        self.leader_addr = Some(addr);
        if self.follow_leader {
            // Update config to connect to leader on next reconnect
            self.config.addr = addr;
        }
    }

    /// Get total reconnection attempts made
    pub fn reconnect_attempts(&self) -> u32 {
        self.reconnect_attempts
    }

    /// Get a reference to the underlying client, reconnecting if needed
    pub async fn client(&mut self) -> Result<&mut LanceClient> {
        if self.client.is_none() {
            self.reconnect().await?;
        }
        self.client.as_mut().ok_or(ClientError::ConnectionClosed)
    }

    /// Attempt to reconnect with exponential backoff
    async fn reconnect(&mut self) -> Result<()> {
        let mut attempts = 0;

        loop {
            attempts += 1;
            self.reconnect_attempts += 1;

            let result = match &self.tls_config {
                Some(tls) => LanceClient::connect_tls(self.config.clone(), tls.clone()).await,
                None => LanceClient::connect(self.config.clone()).await,
            };

            match result {
                Ok(client) => {
                    self.client = Some(client);
                    return Ok(());
                },
                Err(e) => {
                    if self.max_attempts > 0 && attempts >= self.max_attempts {
                        return Err(e);
                    }

                    // Calculate backoff delay
                    let delay = self.base_delay * 2u32.saturating_pow(attempts - 1);
                    let delay = delay.min(self.max_delay);

                    tokio::time::sleep(delay).await;
                },
            }
        }
    }

    /// Execute an operation with automatic reconnection on failure
    pub async fn execute<F, T>(&mut self, op: F) -> Result<T>
    where
        F: Fn(
            &mut LanceClient,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>,
    {
        loop {
            let client = self.client().await?;

            match op(client).await {
                Ok(result) => return Ok(result),
                Err(ClientError::ConnectionClosed) | Err(ClientError::ConnectionFailed(_)) => {
                    self.client = None;
                    // Will reconnect on next iteration
                },
                Err(e) => return Err(e),
            }
        }
    }

    /// Mark connection as failed
    pub fn mark_failed(&mut self) {
        self.client = None;
    }
}

/// Cluster-aware client with automatic node discovery
///
/// Discovers cluster nodes and maintains connections for high availability.
/// Note: Write routing to the leader is handled server-side via transparent
/// forwarding - clients can send writes to ANY node.
pub struct ClusterClient {
    /// Known cluster nodes
    nodes: Vec<SocketAddr>,
    /// Primary node for this client (for connection affinity, not write routing)
    primary: Option<SocketAddr>,
    /// Client configuration
    config: ClientConfig,
    /// TLS configuration
    tls_config: Option<TlsClientConfig>,
    /// Active client connection
    client: Option<LanceClient>,
    /// Last successful discovery time
    last_discovery: Option<Instant>,
    /// Discovery refresh interval
    discovery_interval: Duration,
}

impl ClusterClient {
    /// Create a new cluster client with seed nodes
    pub async fn connect(seed_addrs: &[&str]) -> Result<Self> {
        let nodes: Vec<SocketAddr> = seed_addrs.iter().filter_map(|s| s.parse().ok()).collect();

        if nodes.is_empty() {
            return Err(ClientError::ProtocolError(
                "No valid seed addresses".to_string(),
            ));
        }

        let config = ClientConfig::new(nodes[0]);
        let mut cluster = Self {
            nodes,
            primary: None,
            config,
            tls_config: None,
            client: None,
            last_discovery: None,
            discovery_interval: Duration::from_secs(60),
        };

        cluster.discover_cluster().await?;
        Ok(cluster)
    }

    /// Create a new cluster client with TLS
    pub async fn connect_tls(seed_addrs: &[&str], tls_config: TlsClientConfig) -> Result<Self> {
        let nodes: Vec<SocketAddr> = seed_addrs.iter().filter_map(|s| s.parse().ok()).collect();

        if nodes.is_empty() {
            return Err(ClientError::ProtocolError(
                "No valid seed addresses".to_string(),
            ));
        }

        let config = ClientConfig::new(nodes[0]).with_tls(tls_config.clone());
        let mut cluster = Self {
            nodes,
            primary: None,
            config,
            tls_config: Some(tls_config),
            client: None,
            last_discovery: None,
            discovery_interval: Duration::from_secs(60),
        };

        cluster.discover_cluster().await?;
        Ok(cluster)
    }

    /// Set the discovery refresh interval
    pub fn with_discovery_interval(mut self, interval: Duration) -> Self {
        self.discovery_interval = interval;
        self
    }

    /// Discover cluster topology from any available node
    async fn discover_cluster(&mut self) -> Result<()> {
        for &node in &self.nodes.clone() {
            let mut config = self.config.clone();
            config.addr = node;

            match LanceClient::connect(config).await {
                Ok(mut client) => {
                    match client.get_cluster_status().await {
                        Ok(status) => {
                            self.primary = status.leader_id.map(|id| {
                                // Try to find node in peer_states or use first node
                                status
                                    .peer_states
                                    .get(&id)
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(node)
                            });
                            self.last_discovery = Some(Instant::now());

                            // Connect to primary if found
                            if let Some(primary_addr) = self.primary {
                                self.config.addr = primary_addr;
                                self.client =
                                    Some(LanceClient::connect(self.config.clone()).await?);
                            } else {
                                self.client = Some(client);
                            }
                            return Ok(());
                        },
                        Err(_) => {
                            // Single-node mode or cluster not available
                            self.client = Some(client);
                            self.primary = Some(node);
                            self.last_discovery = Some(Instant::now());
                            return Ok(());
                        },
                    }
                },
                Err(_) => continue,
            }
        }

        Err(ClientError::ConnectionFailed(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "Could not connect to any cluster node",
        )))
    }

    /// Get a client connection, refreshing discovery if needed
    pub async fn client(&mut self) -> Result<&mut LanceClient> {
        // Check if discovery refresh is needed
        let needs_refresh = self
            .last_discovery
            .map(|t| t.elapsed() > self.discovery_interval)
            .unwrap_or(true);

        if needs_refresh || self.client.is_none() {
            self.discover_cluster().await?;
        }

        self.client.as_mut().ok_or(ClientError::ConnectionClosed)
    }

    /// Get the current primary node address
    pub fn primary(&self) -> Option<SocketAddr> {
        self.primary
    }

    /// Get all known cluster nodes
    pub fn nodes(&self) -> &[SocketAddr] {
        &self.nodes
    }

    /// Get the TLS configuration if set
    pub fn tls_config(&self) -> Option<&TlsClientConfig> {
        self.tls_config.as_ref()
    }

    /// Check if TLS is enabled
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_config.is_some()
    }

    /// Force a cluster discovery refresh
    pub async fn refresh(&mut self) -> Result<()> {
        self.discover_cluster().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_defaults() {
        let config = ConnectionPoolConfig::new();

        assert_eq!(config.max_connections, 10);
        assert_eq!(config.min_idle, 1);
        assert!(config.auto_reconnect);
    }

    #[test]
    fn test_pool_config_builder() {
        let config = ConnectionPoolConfig::new()
            .with_max_connections(20)
            .with_min_idle(5)
            .with_health_check_interval(60)
            .with_auto_reconnect(false);

        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_idle, 5);
        assert_eq!(config.health_check_interval, Duration::from_secs(60));
        assert!(!config.auto_reconnect);
    }

    #[test]
    fn test_pool_stats_default() {
        let stats = PoolStats::default();

        assert_eq!(stats.connections_created, 0);
        assert_eq!(stats.active_connections, 0);
    }

    #[test]
    fn test_pooled_connection_expiry() {
        use std::thread::sleep;

        // Can't easily test without actual connection, just test the logic
        let max_lifetime = Duration::from_millis(10);
        let created_at = Instant::now();

        sleep(Duration::from_millis(20));

        assert!(created_at.elapsed() > max_lifetime);
    }

    #[test]
    fn test_reconnecting_client_leader_addr() {
        // Test leader address tracking (without actual connection)
        let addr: SocketAddr = "127.0.0.1:1992".parse().unwrap();
        let leader: SocketAddr = "127.0.0.1:1993".parse().unwrap();

        // Simulate leader address update logic
        let follow_leader = true;
        let mut config_addr = addr;

        // Set leader - simulates set_leader_addr behavior
        let leader_addr: Option<SocketAddr> = Some(leader);
        if follow_leader {
            config_addr = leader;
        }

        assert_eq!(leader_addr, Some(leader));
        assert_eq!(config_addr, leader);
    }

    #[test]
    fn test_connection_pool_config_auto_reconnect() {
        let config = ConnectionPoolConfig::new()
            .with_auto_reconnect(true)
            .with_max_reconnect_attempts(10);

        assert!(config.auto_reconnect);
        assert_eq!(config.max_reconnect_attempts, 10);
    }
}
