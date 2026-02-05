use lnc_core::{LanceError, Result};
use lnc_io::WalConfig;
use lnc_replication::{DiscoveryClusterConfig, DiscoveryMethod, PeerInfo, ReplicationMode};
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
            sync_on_write: true,
        }
    }

    pub fn replication_mode(&self) -> ReplicationMode {
        self.replication_mode.parse().unwrap_or(ReplicationMode::L1)
    }

    pub fn peer_ids(&self) -> Vec<u16> {
        self.peers
            .iter()
            .enumerate()
            .map(|(i, _)| (i + 1) as u16)
            .filter(|&id| id != self.node_id)
            .collect()
    }

    /// Parse peer addresses from config strings like "host:port" or "node_id@host:port"
    /// This version resolves hostnames to IP addresses asynchronously
    pub async fn parse_peers_async(&self) -> Vec<PeerInfo> {
        let mut peers = Vec::new();

        for (idx, peer_str) in self.peers.iter().enumerate() {
            // Support both "host:port" and "node_id@host:port" formats
            let (node_id, host_port) = if peer_str.contains('@') {
                let parts: Vec<&str> = peer_str.splitn(2, '@').collect();
                if parts.len() == 2 {
                    if let Ok(id) = parts[0].parse::<u16>() {
                        (id, parts[1])
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                (idx as u16, peer_str.as_str())
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

            // Resolve hostname using DNS lookup
            match tokio::net::lookup_host(host_port).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        tracing::debug!(
                            target: "lance::config",
                            peer = peer_str,
                            resolved = %addr,
                            node_id,
                            "Resolved peer hostname"
                        );
                        peers.push(PeerInfo::new(node_id, addr));
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        target: "lance::config",
                        peer = peer_str,
                        error = %e,
                        "Failed to resolve peer hostname"
                    );
                },
            }
        }

        peers
    }

    /// Create cluster configuration for replication (with async peer resolution)
    pub async fn cluster_config_async(&self) -> DiscoveryClusterConfig {
        let peers = self.parse_peers_async().await;
        tracing::info!(
            target: "lance::config",
            node_id = self.node_id,
            peer_count = peers.len(),
            "Parsed cluster peers"
        );
        DiscoveryClusterConfig {
            node_id: self.node_id,
            listen_addr: self.replication_addr,
            discovery: DiscoveryMethod::Static(peers),
            heartbeat_interval: Duration::from_millis(150),
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(500),
        }
    }
}

#[cfg(test)]
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
replication_mode = "l2"
peers = ["127.0.0.1:1994", "127.0.0.1:1995"]

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
        assert_eq!(config.replication_mode, "l2");
        assert_eq!(config.peers.len(), 2);
        assert!(config.wal.enabled);
        assert_eq!(config.ingestion.batch_pool_size, 128);
        assert_eq!(config.io.ring_size, 512);
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
        assert!(wal_config.sync_on_write);
    }

    #[test]
    fn test_replication_mode_parsing() {
        let mut config = Config::default();

        config.replication_mode = "l1".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L1);

        config.replication_mode = "L1".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L1);

        config.replication_mode = "l2".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L2);

        config.replication_mode = "invalid".into();
        assert_eq!(config.replication_mode(), ReplicationMode::L1); // fallback
    }

    #[test]
    fn test_peer_ids() {
        let mut config = Config::default();
        config.node_id = 1;
        config.peers = vec![
            "127.0.0.1:1992".into(),
            "127.0.0.1:1993".into(),
            "127.0.0.1:1994".into(),
        ];

        let peer_ids = config.peer_ids();
        // Should exclude own node_id (1)
        assert!(!peer_ids.contains(&1));
        assert!(peer_ids.contains(&2));
        assert!(peer_ids.contains(&3));
    }
}
