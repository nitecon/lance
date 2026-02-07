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
        }
    }

    async fn discover_dns(&self, hostname: &str, port: u16) -> Vec<PeerInfo> {
        // Use tokio's DNS resolver
        match tokio::net::lookup_host(format!("{}:{}", hostname, port)).await {
            Ok(addrs) => {
                let mut peers = Vec::new();
                for (idx, addr) in addrs.enumerate() {
                    // Assign node IDs based on discovery order
                    // In production, this should use a more stable mechanism
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
                        "Discovered peers via DNS"
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
}
