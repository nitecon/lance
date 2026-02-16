use lnc_core::{LanceError, Result};
use lnc_io::WalConfig;
use lnc_replication::{
    DiscoveryClusterConfig, DiscoveryMethod, PeerInfo, ReplicationMode,
    parse_node_id_from_hostname, parse_peer_node_id,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_id: u16,
    pub listen_addr: SocketAddr,
    pub replication_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub health_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub replication_mode: String,
    pub peers: Vec<String>,
    pub wal: WalSettings,
    pub ingestion: IngestionSettings,
    pub io: IoSettings,
    #[serde(default)]
    pub monitoring: MonitoringSettings,
    /// Retention cleanup interval in seconds (default: 60)
    #[serde(default = "default_retention_cleanup_interval")]
    pub retention_cleanup_interval_secs: u64,
    /// Authentication settings
    #[serde(default)]
    pub auth: AuthSettings,
    /// TLS settings for secure connections
    #[serde(default)]
    pub tls: TlsSettings,
    /// Quorum timeout in milliseconds for L3 replication (default: 100ms)
    #[serde(default)]
    pub replication_quorum_timeout_ms: Option<u64>,
    /// Per-connection consumer read rate limit in bytes per second.
    /// Disabled by default (unlimited throughput, zero overhead on fetch path).
    /// Set to cap how fast any single consumer can read, e.g. `104857600` for 100 MB/s.
    #[serde(default)]
    pub consumer_rate_limit_bytes_per_sec: Option<u64>,
    /// Capacity of the data replication forwarder channel.
    /// Higher values absorb network micro-bursts without backpressuring ingestion.
    #[serde(default = "default_data_replication_channel_capacity")]
    pub data_replication_channel_capacity: usize,
    /// Maximum number of concurrent replication batches in flight.
    /// `1` preserves strict sequential forwarding.
    /// Values `>1` enable bounded pipelining for higher throughput.
    #[serde(default = "default_replication_max_inflight")]
    pub replication_max_inflight: usize,
    /// Pin the dedicated coordinator runtime thread to a specific CPU core (optional).
    #[serde(default)]
    pub coordinator_pin_core: Option<usize>,
    /// Pin the dedicated forwarder runtime thread to a specific CPU core (optional).
    #[serde(default)]
    pub forwarder_pin_core: Option<usize>,
}

/// Authentication configuration for the server
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthSettings {
    /// Enable token-based authentication
    pub enabled: bool,
    /// Static tokens for authentication (simple mode)
    #[serde(default)]
    pub tokens: Vec<String>,
    /// Path to token file (one token per line)
    pub token_file: Option<PathBuf>,
    /// Require authentication for write operations only
    pub write_only: bool,
}

/// TLS configuration for secure connections
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Enable TLS for client connections
    pub enabled: bool,
    /// Path to server certificate file (PEM format)
    pub cert_path: Option<PathBuf>,
    /// Path to server private key file (PEM format)
    pub key_path: Option<PathBuf>,
    /// Path to CA certificate for client verification (mTLS)
    pub ca_path: Option<PathBuf>,
    /// Require client certificates (mTLS mode)
    pub require_client_cert: bool,
}

fn default_retention_cleanup_interval() -> u64 {
    60
}

fn default_data_replication_channel_capacity() -> usize {
    65_536
}

fn default_replication_max_inflight() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSettings {
    pub enabled: bool,
    pub size: u64,
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionSettings {
    pub batch_pool_size: usize,
    pub batch_capacity: usize,
    pub channel_capacity: usize,
    pub max_payload_size: usize,
    /// Number of ingestion actors (default: 1, set higher for multi-core scaling)
    #[serde(default = "default_actor_count")]
    pub actor_count: usize,
    /// Pin ingestion actors to specific CPU cores (optional)
    #[serde(default)]
    pub pin_cores: Vec<usize>,
}

fn default_actor_count() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoSettings {
    pub ring_size: u32,
    pub sparse_index_interval: u64,
    pub segment_max_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringSettings {
    /// Enable metrics collection and Prometheus export
    pub enabled: bool,
    /// Latency sampling rate: 0 = all requests, 100 = 1%, 1000 = 0.1%
    /// Higher values reduce hot-path overhead at the cost of precision
    pub latency_sample_rate: u64,
    /// Enable detailed per-operation latency tracking
    pub latency_tracking: bool,
    /// Enable error categorization by type
    pub error_tracking: bool,
    /// Enable saturation/resource utilization gauges
    pub saturation_tracking: bool,
    /// Saturation warning threshold (0.0-1.0)
    pub saturation_warning_threshold: f64,
    /// Saturation critical threshold (0.0-1.0)
    pub saturation_critical_threshold: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 0,
            listen_addr: "0.0.0.0:1992"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 1992))),
            replication_addr: "0.0.0.0:1993"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 1993))),
            metrics_addr: "0.0.0.0:9090"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 9090))),
            health_addr: "0.0.0.0:8080"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 8080))),
            data_dir: PathBuf::from("./data"),
            replication_mode: "l1".into(),
            peers: Vec::new(),
            wal: WalSettings::default(),
            ingestion: IngestionSettings::default(),
            io: IoSettings::default(),
            monitoring: MonitoringSettings::default(),
            retention_cleanup_interval_secs: default_retention_cleanup_interval(),
            auth: AuthSettings::default(),
            tls: TlsSettings::default(),
            replication_quorum_timeout_ms: None,
            consumer_rate_limit_bytes_per_sec: None,
            data_replication_channel_capacity: default_data_replication_channel_capacity(),
            replication_max_inflight: default_replication_max_inflight(),
            coordinator_pin_core: None,
            forwarder_pin_core: None,
        }
    }
}

impl Default for WalSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            size: 64 * 1024 * 1024,
            path: None,
        }
    }
}

impl Default for IngestionSettings {
    fn default() -> Self {
        Self {
            batch_pool_size: 64,
            batch_capacity: 64 * 1024,
            channel_capacity: 16384,
            max_payload_size: 16 * 1024 * 1024, // 16 MB
            actor_count: 1,
            pin_cores: Vec::new(),
        }
    }
}

impl Default for IoSettings {
    fn default() -> Self {
        Self {
            ring_size: 256,
            sparse_index_interval: 4096,
            segment_max_size: 1024 * 1024 * 1024,
        }
    }
}

impl Default for MonitoringSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            latency_sample_rate: 100, // 1% sampling - balanced precision/overhead
            latency_tracking: true,
            error_tracking: true,
            saturation_tracking: true,
            saturation_warning_threshold: 0.7,
            saturation_critical_threshold: 0.9,
        }
    }
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        match ext {
            "toml" => toml::from_str(&content)
                .map_err(|e| LanceError::Config(format!("TOML parse error: {}", e))),
            "json" => serde_json::from_str(&content)
                .map_err(|e| LanceError::Config(format!("JSON parse error: {}", e))),
            _ => Err(LanceError::Config(format!(
                "Unknown config file extension: {}",
                ext
            ))),
        }
    }

    pub fn from_args(args: &super::Args) -> Self {
        Self {
            node_id: args.node_id,
            listen_addr: args.listen,
            replication_addr: args.replication,
            metrics_addr: args.metrics,
            health_addr: args.health,
            data_dir: args.data_dir.clone(),
            replication_mode: args.replication_mode.clone(),
            peers: args.peers.clone(),
            wal: WalSettings {
                enabled: args.wal_enabled,
                size: args.wal_size,
                path: Some(args.data_dir.join("wal")),
            },
            consumer_rate_limit_bytes_per_sec: args.consumer_rate_limit,
            ..Default::default()
        }
    }

    pub fn wal_config(&self) -> WalConfig {
        let wal_dir = self
            .wal
            .path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("wal"));
        WalConfig {
            enabled: self.wal.enabled,
            size: self.wal.size,
            path: wal_dir.join("current.wal"),
            dir: wal_dir,
            max_segment_size: self.wal.size,
            sync_on_write: false,
        }
    }

    pub fn replication_mode(&self) -> ReplicationMode {
        self.replication_mode.parse().unwrap_or(ReplicationMode::L1)
    }

    pub fn peer_ids(&self) -> Vec<u16> {
        self.peers
            .iter()
            .enumerate()
            .map(|(idx, peer_str)| parse_peer_node_id(peer_str, idx))
            .filter(|&id| id != self.node_id)
            .collect()
    }

    /// Parse peer addresses from config strings like "host:port" or "node_id@host:port"
    /// This version resolves hostnames to IP addresses asynchronously with retry logic
    /// for Kubernetes environments where peers may not be immediately available.
    /// Default: 30 retries with 2 second intervals (60 seconds total) to handle
    /// StatefulSet sequential pod creation delays.
    pub async fn parse_peers_async(&self) -> Vec<PeerInfo> {
        self.parse_peers_with_retry(30, Duration::from_secs(2))
            .await
    }

    /// Parse peers with configurable retry count and interval.
    /// Retries DNS resolution for unresolved peers until all are found or retries exhausted.
    pub async fn parse_peers_with_retry(
        &self,
        max_retries: u32,
        retry_interval: Duration,
    ) -> Vec<PeerInfo> {
        let mut peers = Vec::new();
        let mut unresolved: Vec<(u16, String)> = Vec::new();

        // First pass: collect all peer info and try initial resolution
        for (idx, peer_str) in self.peers.iter().enumerate() {
            let node_id = parse_peer_node_id(peer_str, idx);
            let host_port = if peer_str.contains('@') {
                match peer_str.split_once('@').map(|x| x.1) {
                    Some(hp) => hp.to_string(),
                    None => continue,
                }
            } else {
                peer_str.to_string()
            };

            // Skip self
            if node_id == self.node_id {
                continue;
            }

            // Try direct SocketAddr parse first (for IP addresses)
            if let Ok(addr) = host_port.parse::<SocketAddr>() {
                peers.push(PeerInfo::new(node_id, addr));
                continue;
            }

            // Mark for DNS resolution
            unresolved.push((node_id, host_port));
        }

        // Calculate expected peer count (excluding self)
        let expected_peers = self
            .peers
            .iter()
            .enumerate()
            .filter(|(idx, p)| parse_peer_node_id(p, *idx) != self.node_id)
            .count();

        // Retry loop for DNS resolution
        for attempt in 0..=max_retries {
            if unresolved.is_empty() {
                break;
            }

            let mut still_unresolved = Vec::new();

            for (node_id, host_port) in unresolved {
                // Resolve and immediately extract first address to avoid borrow issues
                let resolved_addr: Option<SocketAddr> = tokio::net::lookup_host(host_port.as_str())
                    .await
                    .ok()
                    .and_then(|mut addrs| addrs.next());

                match resolved_addr {
                    Some(addr) => {
                        tracing::debug!(
                            target: "lance::config",
                            peer = %host_port,
                            resolved = %addr,
                            node_id,
                            attempt,
                            "Resolved peer hostname"
                        );
                        peers.push(PeerInfo::new(node_id, addr));
                    },
                    None => {
                        if attempt < max_retries {
                            tracing::debug!(
                                target: "lance::config",
                                peer = %host_port,
                                attempt,
                                max_retries,
                                "Peer not yet resolvable, will retry"
                            );
                        } else {
                            tracing::warn!(
                                target: "lance::config",
                                peer = %host_port,
                                "Failed to resolve peer hostname after all retries"
                            );
                        }
                        still_unresolved.push((node_id, host_port));
                    },
                }
            }

            unresolved = still_unresolved;

            // If we have all expected peers, no need to retry
            if peers.len() >= expected_peers {
                break;
            }

            // Wait before next retry if there are still unresolved peers
            if !unresolved.is_empty() && attempt < max_retries {
                tracing::info!(
                    target: "lance::config",
                    resolved = peers.len(),
                    pending = unresolved.len(),
                    attempt = attempt + 1,
                    max_retries,
                    "Waiting for peer DNS resolution"
                );
                tokio::time::sleep(retry_interval).await;
            }
        }

        peers
    }

    /// Create cluster configuration for replication (with async peer resolution)
    ///
    /// Automatically selects `DnsStateful` discovery when peer strings contain
    /// hostnames with parseable node IDs (e.g., `lance-1.lance-headless:1993`).
    /// Falls back to `Static` discovery for raw IP addresses.
    pub async fn cluster_config_async(&self) -> DiscoveryClusterConfig {
        let peers = self.parse_peers_async().await;
        tracing::info!(
            target: "lance::config",
            node_id = self.node_id,
            peer_count = peers.len(),
            "Parsed cluster peers"
        );

        // Detect if peers use hostname patterns suitable for DnsStateful discovery.
        // If any peer string contains a hostname with a parseable node ID suffix,
        // use DnsStateful for stable node ID resolution on DNS refresh.
        let has_hostname_peers = self.peers.iter().any(|p| {
            let host = if p.contains('@') {
                // node_id@host:port format — already has explicit ID
                return false;
            } else {
                p.rsplit(':').next_back().unwrap_or(p)
            };
            // Check if it's NOT a raw IP and HAS a parseable hostname ID
            host.parse::<std::net::IpAddr>().is_err() && parse_node_id_from_hostname(host).is_some()
        });

        let discovery = if has_hostname_peers {
            // Extract hostnames (without port) for DnsStateful discovery
            let peer_hostnames: Vec<String> = self
                .peers
                .iter()
                .filter_map(|p| {
                    if p.contains('@') {
                        return None;
                    }
                    // Split off port to get hostname
                    if let Some(colon_pos) = p.rfind(':') {
                        let host = &p[..colon_pos];
                        if host.parse::<std::net::IpAddr>().is_err() {
                            return Some(host.to_string());
                        }
                    }
                    None
                })
                .collect();

            let port = self
                .peers
                .first()
                .and_then(|p| p.rsplit(':').next())
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(1993);

            if peer_hostnames.is_empty() {
                DiscoveryMethod::Static(peers)
            } else {
                tracing::info!(
                    target: "lance::config",
                    hostnames = ?peer_hostnames,
                    port,
                    "Using DnsStateful discovery for stable node IDs"
                );
                DiscoveryMethod::DnsStateful {
                    peer_hostnames,
                    port,
                    refresh_interval: Duration::from_secs(30),
                }
            }
        } else {
            DiscoveryMethod::Static(peers)
        };

        DiscoveryClusterConfig {
            node_id: self.node_id,
            listen_addr: self.replication_addr,
            discovery,
            // Keep these aligned with RaftConfig production defaults.
            // The old 150/300-500ms timings are too aggressive under real I/O + network jitter
            // and cause frequent leader churn during benchmark/chaos runs.
            heartbeat_interval: Duration::from_millis(250),
            election_timeout_min: Duration::from_millis(1000),
            election_timeout_max: Duration::from_millis(2000),
            raw_peer_strings: self.peers.clone(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.node_id, 0);
        assert_eq!(config.listen_addr.port(), 1992);
        assert_eq!(config.metrics_addr.port(), 9090);
        assert_eq!(config.health_addr.port(), 8080);
        assert_eq!(config.replication_mode, "l1");
        assert!(config.peers.is_empty());
        assert_eq!(config.data_replication_channel_capacity, 65_536);
        assert_eq!(config.replication_max_inflight, 1);
        assert_eq!(config.coordinator_pin_core, None);
        assert_eq!(config.forwarder_pin_core, None);
    }

    #[test]
    fn test_wal_settings_default() {
        let wal = WalSettings::default();
        assert!(!wal.enabled);
        assert_eq!(wal.size, 64 * 1024 * 1024);
        assert!(wal.path.is_none());
    }

    #[test]
    fn test_ingestion_settings_default() {
        let settings = IngestionSettings::default();
        assert_eq!(settings.batch_pool_size, 64);
        assert_eq!(settings.batch_capacity, 64 * 1024);
        assert_eq!(settings.channel_capacity, 16384);
        assert_eq!(settings.max_payload_size, 16 * 1024 * 1024);
    }

    #[test]
    fn test_io_settings_default() {
        let settings = IoSettings::default();
        assert_eq!(settings.ring_size, 256);
        assert_eq!(settings.sparse_index_interval, 4096);
        assert_eq!(settings.segment_max_size, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_config_from_toml_file() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.toml");

        let toml_content = r#"
node_id = 1
listen_addr = "127.0.0.1:1993"
replication_addr = "127.0.0.1:1992"
metrics_addr = "127.0.0.1:9091"
health_addr = "127.0.0.1:8081"
data_dir = "/tmp/lance-test"
replication_mode = "l3"
peers = ["127.0.0.1:1994", "127.0.0.1:1995"]
data_replication_channel_capacity = 32768
replication_max_inflight = 8
coordinator_pin_core = 6
forwarder_pin_core = 7

[wal]
enabled = true
size = 128000000

[ingestion]
batch_pool_size = 128
batch_capacity = 131072
channel_capacity = 32768
max_payload_size = 33554432

[io]
ring_size = 512
sparse_index_interval = 8192
segment_max_size = 2147483648
"#;

        std::fs::write(&config_path, toml_content).unwrap();

        let config = Config::from_file(&config_path).unwrap();
        assert_eq!(config.node_id, 1);
        assert_eq!(config.listen_addr.port(), 1993);
        assert_eq!(config.replication_mode, "l3");
        assert_eq!(config.peers.len(), 2);
        assert!(config.wal.enabled);
        assert_eq!(config.ingestion.batch_pool_size, 128);
        assert_eq!(config.io.ring_size, 512);
        assert_eq!(config.data_replication_channel_capacity, 32768);
        assert_eq!(config.replication_max_inflight, 8);
        assert_eq!(config.coordinator_pin_core, Some(6));
        assert_eq!(config.forwarder_pin_core, Some(7));
    }

    #[test]
    fn test_config_from_json_file() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.json");

        let json_content = r#"{
            "node_id": 2,
            "listen_addr": "127.0.0.1:1996",
            "replication_addr": "127.0.0.1:1992",
            "metrics_addr": "127.0.0.1:9092",
            "health_addr": "127.0.0.1:8082",
            "data_dir": "/tmp/lance-json",
            "replication_mode": "l1",
            "peers": [],
            "wal": {"enabled": false, "size": 67108864, "path": null},
            "ingestion": {"batch_pool_size": 64, "batch_capacity": 65536, "channel_capacity": 16384, "max_payload_size": 16777216},
            "io": {"ring_size": 256, "sparse_index_interval": 4096, "segment_max_size": 1073741824}
        }"#;

        std::fs::write(&config_path, json_content).unwrap();

        let config = Config::from_file(&config_path).unwrap();
        assert_eq!(config.node_id, 2);
        assert_eq!(config.listen_addr.port(), 1996);
    }

    #[test]
    fn test_config_unknown_extension() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        std::fs::write(&config_path, "node_id: 1").unwrap();

        let result = Config::from_file(&config_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_config() {
        let mut config = Config::default();
        config.wal.enabled = true;
        config.wal.size = 128 * 1024 * 1024;
        config.data_dir = PathBuf::from("/data/lance");

        let wal_config = config.wal_config();
        assert!(wal_config.enabled);
        assert_eq!(wal_config.size, 128 * 1024 * 1024);
        assert!(!wal_config.sync_on_write);
    }

    #[test]
    fn test_replication_mode_parsing() {
        let mut config = Config::default();

        config.replication_mode = "l1".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L1);

        config.replication_mode = "L1".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L1);

        config.replication_mode = "invalid".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L1); // fallback
    }

    #[test]
    fn test_peer_ids_ip_addresses() {
        let mut config = Config::default();
        config.node_id = 1;
        config.peers = vec![
            "127.0.0.1:1992".into(),
            "127.0.0.1:1993".into(),
            "127.0.0.1:1994".into(),
        ];

        let peer_ids = config.peer_ids();
        // IP addresses fall back to idx-based IDs: [0, 1, 2], filter out self (1)
        assert!(!peer_ids.contains(&1));
        assert!(peer_ids.contains(&0));
        assert!(peer_ids.contains(&2));
        assert_eq!(peer_ids.len(), 2);
    }

    #[test]
    fn test_peer_ids_hostname_node0() {
        // Regression: node_id=0 must correctly filter itself from hostname-based peers
        let mut config = Config::default();
        config.node_id = 0;
        config.peers = vec![
            "lance-0.lance-headless:1993".into(),
            "lance-1.lance-headless:1993".into(),
            "lance-2.lance-headless:1993".into(),
        ];

        let peer_ids = config.peer_ids();
        // Should extract IDs [0, 1, 2] from hostnames, filter out self (0)
        assert!(!peer_ids.contains(&0));
        assert!(peer_ids.contains(&1));
        assert!(peer_ids.contains(&2));
        assert_eq!(peer_ids.len(), 2);
    }

    #[test]
    fn test_peer_ids_hostname_node1() {
        let mut config = Config::default();
        config.node_id = 1;
        config.peers = vec![
            "lance-0.lance-headless:1993".into(),
            "lance-1.lance-headless:1993".into(),
            "lance-2.lance-headless:1993".into(),
        ];

        let peer_ids = config.peer_ids();
        assert!(!peer_ids.contains(&1));
        assert!(peer_ids.contains(&0));
        assert!(peer_ids.contains(&2));
        assert_eq!(peer_ids.len(), 2);
    }

    #[test]
    fn test_peer_ids_explicit_format() {
        let mut config = Config::default();
        config.node_id = 0;
        config.peers = vec![
            "0@127.0.0.1:1993".into(),
            "1@127.0.0.1:1994".into(),
            "2@127.0.0.1:1995".into(),
        ];

        let peer_ids = config.peer_ids();
        assert!(!peer_ids.contains(&0));
        assert!(peer_ids.contains(&1));
        assert!(peer_ids.contains(&2));
        assert_eq!(peer_ids.len(), 2);
    }
}
