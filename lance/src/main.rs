#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod auth;
mod config;
mod consumer;
mod health;
mod server;
mod shutdown;
mod subscription;
mod topic;

use clap::Parser;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "lance")]
#[command(about = "LANCE - Log-based Asynchronous Networked Compute Engine")]
#[command(version)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:1992")]
    listen: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:1993")]
    replication: SocketAddr,

    #[arg(short, long, default_value = "0.0.0.0:9090")]
    metrics: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:8080")]
    health: SocketAddr,

    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    #[arg(short, long, default_value = "0")]
    node_id: u16,

    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,

    #[arg(long, default_value = "l1")]
    replication_mode: String,

    #[arg(long)]
    wal_enabled: bool,

    #[arg(long, default_value = "67108864")]
    wal_size: u64,

    #[arg(short, long)]
    config: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    gen_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Handle --gen-config before initializing tracing
    if let Some(path) = &args.gen_config {
        if let Err(e) = generate_config(path) {
            eprintln!("Failed to generate config: {}", e);
            std::process::exit(1);
        }
        println!("Generated default config at: {}", path.display());
        return;
    }

    init_tracing();

    info!(
        target: "lance",
        node_id = args.node_id,
        listen = %args.listen,
        metrics = %args.metrics,
        health = %args.health,
        data_dir = %args.data_dir.display(),
        "Starting LANCE"
    );

    log_startup_info();

    if let Err(e) = std::fs::create_dir_all(&args.data_dir) {
        error!(
            target: "lance",
            error = %e,
            "Failed to create data directory"
        );
        std::process::exit(1);
    }

    let config = match args.config {
        Some(path) => match config::Config::from_file(&path) {
            Ok(c) => c,
            Err(e) => {
                error!(
                    target: "lance",
                    error = %e,
                    path = %path.display(),
                    "Failed to load config file"
                );
                std::process::exit(1);
            },
        },
        None => config::Config::from_args(&args),
    };

    if let Err(e) = lnc_metrics::init_prometheus_exporter(args.metrics) {
        error!(
            target: "lance",
            error = %e,
            "Failed to initialize metrics exporter"
        );
        std::process::exit(1);
    }

    info!(
        target: "lance",
        addr = %args.metrics,
        "Prometheus metrics exporter started"
    );

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    let shutdown_signal = shutdown::install_signal_handlers(shutdown_tx.clone());

    // Start health server
    let health_state = std::sync::Arc::new(health::HealthState::new());
    let _health_handle = tokio::spawn(health::run_health_server(
        config.health_addr,
        std::sync::Arc::clone(&health_state),
        shutdown_tx.subscribe(),
    ));

    // Start periodic metrics export task
    let metrics_shutdown_rx = shutdown_tx.subscribe();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut shutdown_rx = metrics_shutdown_rx;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    lnc_metrics::export_to_prometheus();
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    });

    let server_handle = tokio::spawn(server::run(
        config,
        shutdown_tx.subscribe(),
        std::sync::Arc::clone(&health_state),
    ));

    tokio::select! {
        _ = shutdown_signal => {
            info!(target: "lance", "Shutdown signal received");
        }
        result = server_handle => {
            match result {
                Ok(Ok(())) => info!(target: "lance", "Server exited cleanly"),
                Ok(Err(e)) => error!(target: "lance", error = %e, "Server error"),
                Err(e) => error!(target: "lance", error = %e, "Server task panicked"),
            }
        }
    }

    // Execute graceful drain sequence
    let clean = shutdown::drain_with_timeout(shutdown::DRAIN_TIMEOUT).await;
    if clean {
        info!(target: "lance", "Graceful drain complete");
    } else {
        tracing::warn!(target: "lance", "Drain timed out, some operations may be incomplete");
    }

    info!(target: "lance", "LANCE shutdown complete");
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,lance=debug"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(filter)
        .init();
}

fn generate_config(path: &Path) -> std::io::Result<()> {
    let config = config::Config::default();

    let content = format!(
        r#"# LANCE Configuration File
# Generated by: lance --gen-config {}
#
# All values shown are defaults. Uncomment and modify as needed.

# =============================================================================
# Node Identity
# =============================================================================

# Unique node identifier (0-65535)
node_id = {}

# =============================================================================
# Network Settings
# =============================================================================

# Address and port for client connections (LWP protocol)
listen_addr = "{}"

# Address and port for Prometheus metrics endpoint
metrics_addr = "{}"

# Address and port for health check endpoints
health_addr = "{}"

# Peer nodes for replication (comma-separated host:port)
# Example: ["node1:1992", "node2:1992"]
peers = []

# =============================================================================
# Replication Settings
# =============================================================================

# Replication mode:
#   "l1" - Single node, no replication (default)
#   "l2" - Async replication to followers
#   "l3" - Sync replication with quorum
replication_mode = "{}"

# =============================================================================
# Storage Settings
# =============================================================================

# Directory for data files (segments, indexes)
data_dir = "{}"

# =============================================================================
# WAL (Write-Ahead Log) Settings
# =============================================================================

[wal]
# Enable WAL for durability (recommended for production)
enabled = {}

# WAL size in bytes (default: 64 MB)
size = {}

# WAL directory (defaults to data_dir/wal if not specified)
# path = "/path/to/wal"

# =============================================================================
# Ingestion Settings
# =============================================================================

[ingestion]
# Number of batch buffers in the pool
batch_pool_size = {}

# Capacity of each batch buffer in bytes
batch_capacity = {}

# Internal channel capacity for ingestion pipeline
channel_capacity = {}

# Maximum payload size per frame in bytes (default: 16 MB)
# Clients sending larger payloads will be disconnected
max_payload_size = {}

# =============================================================================
# I/O Settings
# =============================================================================

[io]
# io_uring ring size (Linux only, power of 2)
ring_size = {}

# Sparse index entry interval (records between index entries)
sparse_index_interval = {}

# Maximum segment file size in bytes (default: 1 GB)
segment_max_size = {}
"#,
        path.display(),
        config.node_id,
        config.listen_addr,
        config.metrics_addr,
        config.health_addr,
        config.replication_mode,
        config.data_dir.display(),
        config.wal.enabled,
        config.wal.size,
        config.ingestion.batch_pool_size,
        config.ingestion.batch_capacity,
        config.ingestion.channel_capacity,
        config.ingestion.max_payload_size,
        config.io.ring_size,
        config.io.sparse_index_interval,
        config.io.segment_max_size,
    );

    std::fs::write(path, content)
}

/// Log required startup information per Engineering Standards §7.2
/// Must log: io_uring status, NUMA topology, thread pinning, IRQ affinity, SIMD features
fn log_startup_info() {
    info!(target: "lance", "LANCE v{}", env!("CARGO_PKG_VERSION"));

    // io_uring status (per Standards §7.2)
    #[cfg(target_os = "linux")]
    {
        if lnc_io::probe_io_uring() {
            info!(target: "lance", "io_uring: enabled (kernel 5.6+)");
        } else {
            tracing::warn!(target: "lance", "io_uring: FALLBACK to pwritev2");
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        tracing::warn!(target: "lance", "io_uring: not available (non-Linux platform)");
    }

    // NUMA topology (per Standards §7.2)
    #[cfg(target_os = "linux")]
    {
        // Log NUMA node count if available
        let numa_nodes = std::fs::read_dir("/sys/devices/system/node")
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().starts_with("node"))
                    .count()
            })
            .unwrap_or(1);
        info!(target: "lance", "NUMA: {} node(s) detected", numa_nodes);
    }

    #[cfg(not(target_os = "linux"))]
    {
        info!(target: "lance", "NUMA: topology detection not available");
    }

    // Thread pinning info (per Standards §7.2)
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    info!(target: "lance", "CPU cores: {} available", num_cpus);

    // SIMD features (per Standards §7.2)
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            info!(target: "lance", "CRC32C: SSE4.2 accelerated");
        } else {
            info!(target: "lance", "CRC32C: software fallback");
        }

        if is_x86_feature_detected!("avx2") {
            info!(target: "lance", "SIMD: AVX2 available");
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        info!(target: "lance", "CRC32C: ARM CRC extension");
    }
}
