//! Peer discovery for cluster membership.
//!
//! Supports multiple discovery mechanisms:
//! - Static: Fixed list of peer addresses from configuration
//! - DNS: DNS-based discovery for Kubernetes/cloud deployments

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Discovery mechanism for finding cluster peers
#[derive(Debug, Clone)]
pub enum DiscoveryMethod {
    /// Static list of peer addresses
    Static(Vec<PeerInfo>),
    /// DNS-based discovery (hostname resolves to multiple IPs)
    Dns {
        hostname: String,
        port: u16,
        refresh_interval: Duration,
    },
    /// StatefulSet-style DNS discovery with stable node IDs.
    ///
    /// Each entry in `peer_hostnames` is a hostname like `lance-1.lance-headless`
    /// where the node ID is parsed from the first component's numeric suffix
    /// (e.g., `lance-1` → node_id 1). This avoids the non-deterministic
    /// `idx as u16` trap where DNS round-robin reordering corrupts Raft state.
    DnsStateful {
        peer_hostnames: Vec<String>,
        port: u16,
        refresh_interval: Duration,
    },
}

impl Default for DiscoveryMethod {
    fn default() -> Self {
        Self::Static(Vec::new())
    }
}

/// Information about a discovered peer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerInfo {
    pub node_id: u16,
    pub addr: SocketAddr,
}

impl PeerInfo {
    pub fn new(node_id: u16, addr: SocketAddr) -> Self {
        Self { node_id, addr }
    }
}

/// Peer discovery service
pub struct PeerDiscovery {
    node_id: u16,
    method: DiscoveryMethod,
    known_peers: Arc<RwLock<HashSet<PeerInfo>>>,
}

impl PeerDiscovery {
    pub fn new(node_id: u16, method: DiscoveryMethod) -> Self {
        Self {
            node_id,
            method,
            known_peers: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Create a static discovery with the given peers
    pub fn static_peers(node_id: u16, peers: Vec<PeerInfo>) -> Self {
        Self::new(node_id, DiscoveryMethod::Static(peers))
    }

    /// Create a DNS-based discovery
    pub fn dns(node_id: u16, hostname: String, port: u16) -> Self {
        Self::new(
            node_id,
            DiscoveryMethod::Dns {
                hostname,
                port,
                refresh_interval: Duration::from_secs(30),
            },
        )
    }

    /// Discover peers and return the current set
    pub async fn discover(&self) -> Vec<PeerInfo> {
        match &self.method {
            DiscoveryMethod::Static(peers) => {
                // Filter out self
                peers
                    .iter()
                    .filter(|p| p.node_id != self.node_id)
                    .cloned()
                    .collect()
            },
            DiscoveryMethod::Dns { hostname, port, .. } => self.discover_dns(hostname, *port).await,
            DiscoveryMethod::DnsStateful {
                peer_hostnames,
                port,
                ..
            } => self.discover_dns_stateful(peer_hostnames, *port).await,
        }
    }

    /// DNS discovery with hostname-based stable node ID resolution.
    ///
    /// Per Architecture §10.7, node IDs are parsed from the hostname's numeric
    /// suffix (e.g., `lance-3.lance-headless` → node_id 3). This is the
    /// **required** path for Kubernetes StatefulSet deployments where DNS
    /// round-robin ordering is non-deterministic.
    async fn discover_dns_stateful(&self, peer_hostnames: &[String], port: u16) -> Vec<PeerInfo> {
        let mut peers = Vec::new();

        for hostname in peer_hostnames {
            // Parse node ID from hostname (e.g., "lance-2.lance-headless" → 2)
            let node_id = match parse_node_id_from_hostname(hostname) {
                Some(id) => id,
                None => {
                    warn!(
                        target: "lance::discovery",
                        hostname,
                        "Cannot parse node ID from hostname, skipping peer"
                    );
                    continue;
                },
            };

            // Skip self
            if node_id == self.node_id {
                continue;
            }

            // Resolve this specific peer's address
            match tokio::net::lookup_host(format!("{}:{}", hostname, port)).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        peers.push(PeerInfo::new(node_id, addr));
                    }
                },
                Err(e) => {
                    warn!(
                        target: "lance::discovery",
                        hostname,
                        node_id,
                        error = %e,
                        "DNS resolution failed for peer"
                    );
                },
            }
        }

        if !peers.is_empty() {
            info!(
                target: "lance::discovery",
                peer_count = peers.len(),
                "Discovered peers via StatefulSet DNS"
            );
        }

        peers
    }

    /// Legacy DNS discovery (headless service resolving to multiple IPs).
    ///
    /// **WARNING**: This method assigns node IDs based on DNS enumeration order
    /// which is non-deterministic. Use `DnsStateful` for production deployments.
    /// Retained for backward compatibility with single-hostname discovery.
    async fn discover_dns(&self, hostname: &str, port: u16) -> Vec<PeerInfo> {
        // Use tokio's DNS resolver
        match tokio::net::lookup_host(format!("{}:{}", hostname, port)).await {
            Ok(addrs) => {
                let mut peers = Vec::new();
                // Sort by IP address for deterministic ordering as a best-effort
                // mitigation. For true stability, use DnsStateful instead.
                let mut addr_list: Vec<std::net::SocketAddr> = addrs.collect();
                addr_list.sort();

                for (idx, addr) in addr_list.into_iter().enumerate() {
                    let node_id = idx as u16;
                    if node_id != self.node_id {
                        peers.push(PeerInfo::new(node_id, addr));
                    }
                }

                if !peers.is_empty() {
                    info!(
                        target: "lance::discovery",
                        hostname,
                        peer_count = peers.len(),
                        "Discovered peers via DNS (legacy mode - consider DnsStateful)"
                    );
                }

                peers
            },
            Err(e) => {
                warn!(
                    target: "lance::discovery",
                    hostname,
                    error = %e,
                    "DNS discovery failed"
                );
                Vec::new()
            },
        }
    }

    /// Update the known peers set
    pub async fn update_peers(&self, peers: Vec<PeerInfo>) {
        let mut known = self.known_peers.write().await;
        known.clear();
        for peer in peers {
            if peer.node_id != self.node_id {
                known.insert(peer);
            }
        }
    }

    /// Get current known peers
    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let known = self.known_peers.read().await;
        known.iter().cloned().collect()
    }

    /// Add a peer manually
    pub async fn add_peer(&self, peer: PeerInfo) {
        if peer.node_id != self.node_id {
            let node_id = peer.node_id;
            let addr = peer.addr;
            let mut known = self.known_peers.write().await;
            known.insert(peer);
            debug!(
                target: "lance::discovery",
                node_id,
                addr = %addr,
                "Added peer"
            );
        }
    }

    /// Remove a peer
    pub async fn remove_peer(&self, node_id: u16) {
        let mut known = self.known_peers.write().await;
        known.retain(|p| p.node_id != node_id);
        debug!(
            target: "lance::discovery",
            node_id,
            "Removed peer"
        );
    }

    /// Run background discovery loop (for DNS-based discovery)
    pub async fn run_discovery_loop(&self, shutdown: tokio::sync::broadcast::Receiver<()>) {
        let interval = match &self.method {
            DiscoveryMethod::Dns {
                refresh_interval, ..
            } => *refresh_interval,
            DiscoveryMethod::DnsStateful {
                refresh_interval, ..
            } => *refresh_interval,
            DiscoveryMethod::Static(_) => {
                // Static discovery doesn't need a loop
                return;
            },
        };

        let mut shutdown = shutdown;
        let mut ticker = tokio::time::interval(interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let peers = self.discover().await;
                    self.update_peers(peers).await;
                }
                _ = shutdown.recv() => {
                    info!(
                        target: "lance::discovery",
                        "Discovery loop shutting down"
                    );
                    break;
                }
            }
        }
    }
}

/// Cluster membership configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: u16,
    pub listen_addr: SocketAddr,
    pub discovery: DiscoveryMethod,
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    /// Raw peer strings (e.g. "lance-0.lance-headless:1993") for periodic DNS re-resolution.
    /// Empty if peers were specified as IP addresses.
    pub raw_peer_strings: Vec<String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            listen_addr: "0.0.0.0:1993"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 1993))),
            discovery: DiscoveryMethod::default(),
            raw_peer_strings: Vec::new(),
            heartbeat_interval: Duration::from_millis(150),
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(500),
        }
    }
}

impl ClusterConfig {
    pub fn new(node_id: u16, listen_addr: SocketAddr) -> Self {
        Self {
            node_id,
            listen_addr,
            ..Default::default()
        }
    }

    pub fn with_static_peers(mut self, peers: Vec<PeerInfo>) -> Self {
        self.discovery = DiscoveryMethod::Static(peers);
        self
    }

    pub fn with_dns_discovery(mut self, hostname: String, port: u16) -> Self {
        self.discovery = DiscoveryMethod::Dns {
            hostname,
            port,
            refresh_interval: Duration::from_secs(30),
        };
        self
    }

    /// Configure StatefulSet-style DNS discovery with stable node IDs.
    ///
    /// Each hostname should follow the pattern `{name}-{id}.{service}` where
    /// the numeric suffix of the first component is the node ID.
    pub fn with_stateful_dns_discovery(mut self, peer_hostnames: Vec<String>, port: u16) -> Self {
        self.discovery = DiscoveryMethod::DnsStateful {
            peer_hostnames,
            port,
            refresh_interval: Duration::from_secs(30),
        };
        self
    }
}

/// Parse a stable node ID from a hostname following the StatefulSet pattern.
///
/// Per Architecture §10.7, hostnames like `lance-3`, `lance-3.lance-headless`,
/// or `lance-3.lance-headless.default.svc.cluster.local` all resolve to node_id 3.
///
/// The algorithm:
/// 1. Take the first DNS label (before the first `.`)
/// 2. Extract the numeric suffix after the last `-`
/// 3. Parse as u16
///
/// Returns `None` if the hostname doesn't match the expected pattern.
pub fn parse_node_id_from_hostname(hostname: &str) -> Option<u16> {
    // Take the first DNS label (e.g., "lance-3" from "lance-3.lance-headless")
    let first_label = hostname.split('.').next()?;
    // Extract numeric suffix after last '-' (e.g., "3" from "lance-3")
    let suffix = first_label.rsplit('-').next()?;
    suffix.parse::<u16>().ok()
}

/// Extract a node ID from a peer string, centralizing the ID-extraction logic
/// used by both synchronous (`peer_ids()`) and async (`parse_peers_async()`) paths.
///
/// Supports three formats:
/// - `"node_id@host:port"` → explicit node ID
/// - `"lance-N.lance-headless:port"` → hostname-based ID via [`parse_node_id_from_hostname`]
/// - `"192.168.1.1:port"` → raw IP, falls back to `fallback_idx`
///
/// The `fallback_idx` is used when no ID can be derived (raw IPs without explicit IDs).
pub fn parse_peer_node_id(peer_str: &str, fallback_idx: usize) -> u16 {
    if peer_str.contains('@') {
        // "node_id@host:port" format
        peer_str
            .split('@')
            .next()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(fallback_idx as u16)
    } else {
        let host = peer_str.split(':').next().unwrap_or(peer_str);
        // Raw IP addresses don't carry node IDs
        if host.parse::<std::net::IpAddr>().is_ok() {
            fallback_idx as u16
        } else {
            parse_node_id_from_hostname(host).unwrap_or(fallback_idx as u16)
        }
    }
}

/// Resolve the node ID using the Architecture §10.7 priority chain.
///
/// 1. **`LANCE_NODE_ID` env-var** — explicit override (Docker, bare metal)
/// 2. **`HOSTNAME` env-var** — parse numeric suffix (Kubernetes StatefulSet)
/// 3. **`None`** — caller must handle (fail startup or use CLI arg)
///
/// This is the authoritative source of truth for node identity. The
/// `LANCE_NODE_ID` env-var always wins, allowing SREs to override the
/// hostname-derived ID in edge cases.
pub fn resolve_node_id() -> Option<u16> {
    // Priority 1: Explicit environment variable
    if let Ok(id_str) = std::env::var("LANCE_NODE_ID") {
        if let Ok(id) = id_str.parse::<u16>() {
            tracing::info!(
                target: "lance::discovery",
                node_id = id,
                "Node ID resolved from LANCE_NODE_ID env-var"
            );
            return Some(id);
        }
        tracing::warn!(
            target: "lance::discovery",
            value = %id_str,
            "LANCE_NODE_ID env-var is not a valid u16, falling through to hostname"
        );
    }

    // Priority 2: Parse from hostname (StatefulSet pattern: lance-0, lance-1)
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        if let Some(id) = parse_node_id_from_hostname(&hostname) {
            tracing::info!(
                target: "lance::discovery",
                node_id = id,
                hostname = %hostname,
                "Node ID resolved from HOSTNAME env-var"
            );
            return Some(id);
        }
    }

    // Priority 3: Cannot determine
    None
}

/// Validate that the configured `node_id` is consistent with the hostname.
///
/// If the `HOSTNAME` env-var is set and contains a parseable node ID suffix,
/// this function checks that it matches `configured_node_id`. A mismatch
/// indicates a StatefulSet misconfiguration (e.g., pod `lance-2` started
/// with `--node-id 1`) which would corrupt Raft persistent state.
///
/// Returns `Ok(())` if consistent or if hostname is not parseable.
/// Returns `Err(message)` if there is a mismatch — the node should refuse to start.
pub fn validate_node_id_consistency(configured_node_id: u16) -> Result<(), String> {
    // Check LANCE_NODE_ID env-var first — if set, it's the authoritative source
    // and we trust the operator's explicit override.
    if std::env::var("LANCE_NODE_ID").is_ok() {
        return Ok(());
    }

    // Check hostname-derived ID
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        if let Some(hostname_id) = parse_node_id_from_hostname(&hostname) {
            if hostname_id != configured_node_id {
                return Err(format!(
                    "Node ID mismatch: configured node_id={} but hostname '{}' implies node_id={}. \
                     This will corrupt Raft persistent state. Either fix the StatefulSet ordinal \
                     or set LANCE_NODE_ID={} to override.",
                    configured_node_id, hostname, hostname_id, configured_node_id
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_info() {
        let addr: SocketAddr = "127.0.0.1:1993".parse().unwrap();
        let peer = PeerInfo::new(1, addr);
        assert_eq!(peer.node_id, 1);
        assert_eq!(peer.addr, addr);
    }

    #[tokio::test]
    async fn test_static_discovery() {
        let addr1: SocketAddr = "127.0.0.1:1993".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.2:1993".parse().unwrap();

        let peers = vec![PeerInfo::new(0, addr1), PeerInfo::new(1, addr2)];

        let discovery = PeerDiscovery::static_peers(0, peers);
        let discovered = discovery.discover().await;

        // Should filter out self (node_id 0)
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].node_id, 1);
    }

    #[tokio::test]
    async fn test_add_remove_peer() {
        let discovery = PeerDiscovery::new(0, DiscoveryMethod::default());

        let addr: SocketAddr = "127.0.0.1:1993".parse().unwrap();
        let peer = PeerInfo::new(1, addr);

        discovery.add_peer(peer.clone()).await;
        let peers = discovery.get_peers().await;
        assert_eq!(peers.len(), 1);

        discovery.remove_peer(1).await;
        let peers = discovery.get_peers().await;
        assert_eq!(peers.len(), 0);
    }

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.node_id, 0);
        assert_eq!(config.heartbeat_interval, Duration::from_millis(150));
    }

    #[test]
    fn test_parse_node_id_simple() {
        assert_eq!(parse_node_id_from_hostname("lance-0"), Some(0));
        assert_eq!(parse_node_id_from_hostname("lance-1"), Some(1));
        assert_eq!(parse_node_id_from_hostname("lance-3"), Some(3));
        assert_eq!(parse_node_id_from_hostname("lance-255"), Some(255));
    }

    #[test]
    fn test_parse_node_id_statefulset_fqdn() {
        assert_eq!(
            parse_node_id_from_hostname("lance-0.lance-headless"),
            Some(0)
        );
        assert_eq!(
            parse_node_id_from_hostname("lance-2.lance-headless.default.svc.cluster.local"),
            Some(2)
        );
    }

    #[test]
    fn test_parse_node_id_custom_prefix() {
        assert_eq!(parse_node_id_from_hostname("myapp-5"), Some(5));
        assert_eq!(
            parse_node_id_from_hostname("my-custom-app-10.svc"),
            Some(10)
        );
    }

    #[test]
    fn test_parse_node_id_invalid() {
        assert_eq!(parse_node_id_from_hostname(""), None);
        assert_eq!(parse_node_id_from_hostname("lance"), None);
        assert_eq!(parse_node_id_from_hostname("lance-abc"), None);
        // u16 overflow
        assert_eq!(parse_node_id_from_hostname("lance-99999"), None);
    }

    #[test]
    fn test_parse_peer_node_id_hostname() {
        assert_eq!(parse_peer_node_id("lance-0.lance-headless:1993", 0), 0);
        assert_eq!(parse_peer_node_id("lance-1.lance-headless:1993", 1), 1);
        assert_eq!(parse_peer_node_id("lance-2.lance-headless:1993", 2), 2);
        // Hostname ID takes precedence over fallback index
        assert_eq!(parse_peer_node_id("lance-2.lance-headless:1993", 99), 2);
    }

    #[test]
    fn test_parse_peer_node_id_explicit() {
        assert_eq!(parse_peer_node_id("0@127.0.0.1:1993", 99), 0);
        assert_eq!(parse_peer_node_id("1@127.0.0.1:1994", 99), 1);
        assert_eq!(parse_peer_node_id("5@10.0.0.1:1993", 0), 5);
    }

    #[test]
    fn test_parse_peer_node_id_raw_ip_fallback() {
        // Raw IPs can't carry node IDs, so fallback_idx is used
        assert_eq!(parse_peer_node_id("127.0.0.1:1992", 0), 0);
        assert_eq!(parse_peer_node_id("127.0.0.1:1993", 1), 1);
        assert_eq!(parse_peer_node_id("10.244.1.48:1993", 3), 3);
    }

    #[test]
    fn test_cluster_config_stateful_dns() {
        let config = ClusterConfig::new(0, "0.0.0.0:1993".parse().unwrap())
            .with_stateful_dns_discovery(
                vec![
                    "lance-1.lance-headless".to_string(),
                    "lance-2.lance-headless".to_string(),
                ],
                1993,
            );

        match &config.discovery {
            DiscoveryMethod::DnsStateful {
                peer_hostnames,
                port,
                ..
            } => {
                assert_eq!(peer_hostnames.len(), 2);
                assert_eq!(*port, 1993);
            },
            _ => panic!("Expected DnsStateful"),
        }
    }

    // NOTE: These env-var tests mutate process-global state and are not
    // parallel-safe. Run with `--test-threads=1` if flaky.

    #[test]
    #[ignore] // Manipulates global env vars, can interfere with other tests
    fn test_validate_node_id_consistency_no_hostname() {
        // When HOSTNAME is not set, validation should pass for any node_id
        unsafe {
            std::env::remove_var("HOSTNAME");
            std::env::remove_var("LANCE_NODE_ID");
        }
        assert!(validate_node_id_consistency(42).is_ok());
    }

    #[test]
    #[ignore] // Manipulates global env vars, can interfere with other tests
    fn test_validate_node_id_consistency_matching() {
        unsafe {
            std::env::remove_var("LANCE_NODE_ID");
            std::env::set_var("HOSTNAME", "lance-3");
        }
        assert!(validate_node_id_consistency(3).is_ok());
        unsafe {
            std::env::remove_var("HOSTNAME");
        }
    }

    #[test]
    #[ignore] // Manipulates global env vars, can interfere with other tests
    fn test_validate_node_id_consistency_mismatch() {
        unsafe {
            std::env::remove_var("LANCE_NODE_ID");
            std::env::set_var("HOSTNAME", "lance-3");
        }
        let result = validate_node_id_consistency(1);
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("Node ID mismatch"));
        assert!(msg.contains("node_id=1"));
        assert!(msg.contains("node_id=3"));
        unsafe {
            std::env::remove_var("HOSTNAME");
        }
    }

    #[test]
    #[ignore] // Manipulates global env vars, can interfere with other tests
    fn test_validate_node_id_env_override_bypasses_check() {
        // When LANCE_NODE_ID is set, hostname mismatch is allowed
        // (operator explicitly overriding)
        unsafe {
            std::env::set_var("LANCE_NODE_ID", "1");
            std::env::set_var("HOSTNAME", "lance-3");
        }
        assert!(validate_node_id_consistency(1).is_ok());
        unsafe {
            std::env::remove_var("LANCE_NODE_ID");
            std::env::remove_var("HOSTNAME");
        }
    }
}
