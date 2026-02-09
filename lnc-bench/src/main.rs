//! lnc-bench — Throughput and latency benchmark for LANCE
//!
//! Measures peak ingestion throughput (msg/s, MB/s) and ACK latency
//! percentiles (p50, p95, p99, p999, max) using the lnc-client Producer API.
//!
//! # Usage
//!
//! ```text
//! lnc-bench --endpoint 127.0.0.1:1992 --topic 1 --duration 30 --msg-size 1024 --connections 4
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use tokio::sync::{Barrier, Semaphore, mpsc};
use tracing::{error, info, warn};

/// LANCE throughput and latency benchmark
#[derive(Parser, Debug, Clone)]
#[command(
    name = "lnc-bench",
    about = "Throughput and latency benchmark for LANCE"
)]
struct Args {
    /// LANCE server endpoint (host:port)
    #[arg(long, short = 'e')]
    endpoint: String,

    /// Topic name (created if it doesn't exist)
    #[arg(long, short = 't', default_value = "bench-topic")]
    topic_name: String,

    /// Duration of the benchmark in seconds
    #[arg(long, short = 'd', default_value = "30")]
    duration: u64,

    /// Message payload size in bytes
    #[arg(long, short = 's', default_value = "1024")]
    msg_size: usize,

    /// Number of concurrent producer connections
    #[arg(long, short = 'c', default_value = "1")]
    connections: usize,

    /// Producer batch size in bytes (0 = send individual messages)
    #[arg(long, short = 'b', default_value = "65536")]
    batch_size: usize,

    /// Producer linger time in milliseconds
    #[arg(long, short = 'l', default_value = "1")]
    linger_ms: u64,

    /// Warmup period before measuring (seconds)
    #[arg(long, short = 'w', default_value = "3")]
    warmup: u64,

    /// Report interval in seconds
    #[arg(long, default_value = "5")]
    report_interval: u64,

    /// Number of in-flight sends per connection (pipeline depth)
    #[arg(long, short = 'p', default_value = "64")]
    pipeline: usize,

    /// Collect per-message latencies (adds overhead, enables percentile reporting)
    #[arg(long, default_value = "true")]
    latency: bool,
}

/// Shared counters across all producer tasks
struct SharedMetrics {
    total_messages: AtomicU64,
    total_bytes: AtomicU64,
    total_errors: AtomicU64,
    running: AtomicBool,
}

/// Compute percentiles from sorted samples
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

fn format_bytes(bytes: f64) -> String {
    if bytes < 1024.0 {
        format!("{bytes:.0} B/s")
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1} KB/s", bytes / 1024.0)
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB/s", bytes / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB/s", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Setup: create or find the topic
    let topic_id = match setup_topic(&args.endpoint, &args.topic_name).await {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to setup topic '{}': {}", args.topic_name, e);
            std::process::exit(1);
        },
    };

    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║                    LANCE BENCHMARK                         ║");
    info!("╠══════════════════════════════════════════════════════════════╣");
    info!("║  endpoint:    {:<45}║", args.endpoint);
    info!(
        "║  topic:       {:<45}║",
        format!("'{}' (id={})", args.topic_name, topic_id)
    );
    info!("║  connections: {:<45}║", args.connections);
    info!(
        "║  msg_size:    {:<45}║",
        format!("{} bytes", args.msg_size)
    );
    info!(
        "║  batch_size:  {:<45}║",
        format!("{} bytes", args.batch_size)
    );
    info!("║  linger_ms:   {:<45}║", args.linger_ms);
    info!(
        "║  pipeline:    {:<45}║",
        format!("{} inflight/conn", args.pipeline)
    );
    info!(
        "║  duration:    {:<45}║",
        format!("{}s (+{}s warmup)", args.duration, args.warmup)
    );
    info!("║  latency:     {:<45}║", args.latency);
    info!("╚══════════════════════════════════════════════════════════════╝");

    // Pre-generate payload (avoid allocation in hot loop)
    let payload = generate_payload(args.msg_size);

    let metrics = Arc::new(SharedMetrics {
        total_messages: AtomicU64::new(0),
        total_bytes: AtomicU64::new(0),
        total_errors: AtomicU64::new(0),
        running: AtomicBool::new(true),
    });

    // Barrier to synchronize all producers starting together
    let barrier = Arc::new(Barrier::new(args.connections + 1));

    // Spawn producer tasks
    let mut handles = Vec::with_capacity(args.connections);
    for conn_id in 0..args.connections {
        let args = args.clone();
        let metrics = Arc::clone(&metrics);
        let barrier = Arc::clone(&barrier);
        let payload = payload.clone();

        handles.push(tokio::spawn(async move {
            producer_task(conn_id, topic_id, args, metrics, barrier, payload).await
        }));
    }

    // Wait for all producers to connect
    barrier.wait().await;
    let bench_start = Instant::now();

    info!(
        "All {} producers connected — starting benchmark",
        args.connections
    );

    // Warmup phase
    if args.warmup > 0 {
        info!("Warmup: {}s ...", args.warmup);
        tokio::time::sleep(Duration::from_secs(args.warmup)).await;
        // Reset counters after warmup
        metrics.total_messages.store(0, Ordering::SeqCst);
        metrics.total_bytes.store(0, Ordering::SeqCst);
        metrics.total_errors.store(0, Ordering::SeqCst);
        info!(
            "Warmup complete — counters reset, measuring for {}s",
            args.duration
        );
    }

    let measure_start = Instant::now();

    // Periodic reporting
    let report_metrics = Arc::clone(&metrics);
    let report_interval = args.report_interval;
    let report_msg_size = args.msg_size;
    let report_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(report_interval));
        interval.tick().await; // skip first immediate tick
        let mut prev_msgs: u64 = 0;
        let mut prev_bytes: u64 = 0;
        let mut tick: u64 = 0;

        loop {
            interval.tick().await;
            if !report_metrics.running.load(Ordering::Relaxed) {
                break;
            }
            tick += 1;

            let msgs = report_metrics.total_messages.load(Ordering::Relaxed);
            let bytes = report_metrics.total_bytes.load(Ordering::Relaxed);
            let errors = report_metrics.total_errors.load(Ordering::Relaxed);

            let delta_msgs = msgs - prev_msgs;
            let delta_bytes = bytes - prev_bytes;
            let msgs_per_sec = delta_msgs as f64 / report_interval as f64;
            let bytes_per_sec = delta_bytes as f64 / report_interval as f64;

            info!(
                "  [{:>3}s]  {:.0} msg/s  {}  total={} err={}  ({} bytes/msg)",
                tick * report_interval,
                msgs_per_sec,
                format_bytes(bytes_per_sec),
                msgs,
                errors,
                report_msg_size,
            );

            prev_msgs = msgs;
            prev_bytes = bytes;
        }
    });

    // Wait for measurement duration
    tokio::time::sleep(Duration::from_secs(args.duration)).await;

    // Stop all producers
    metrics.running.store(false, Ordering::SeqCst);
    let measure_elapsed = measure_start.elapsed();

    // Collect results
    let mut all_latencies: Vec<u64> = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(latencies) => {
                all_latencies.extend(latencies);
            },
            Err(e) => {
                error!("Producer task panicked: {e}");
            },
        }
    }

    report_handle.abort();

    let total_msgs = metrics.total_messages.load(Ordering::Relaxed);
    let total_bytes = metrics.total_bytes.load(Ordering::Relaxed);
    let total_errors = metrics.total_errors.load(Ordering::Relaxed);
    let elapsed_secs = measure_elapsed.as_secs_f64();
    let total_elapsed = bench_start.elapsed();

    let msgs_per_sec = total_msgs as f64 / elapsed_secs;
    let bytes_per_sec = total_bytes as f64 / elapsed_secs;

    // Sort latencies for percentile computation
    all_latencies.sort_unstable();

    info!("");
    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║                  BENCHMARK RESULTS                         ║");
    info!("╠══════════════════════════════════════════════════════════════╣");
    info!("║                                                            ║");
    info!(
        "║  Duration:     {:<43}║",
        format!("{:.1}s (measured)", elapsed_secs)
    );
    info!(
        "║  Total time:   {:<43}║",
        format!(
            "{:.1}s (incl. warmup + connect)",
            total_elapsed.as_secs_f64()
        )
    );
    info!("║                                                            ║");
    info!("║  ── Throughput ────────────────────────────────────────     ║");
    info!("║  Messages:     {:<43}║", format!("{total_msgs}"));
    info!("║  Msg/sec:      {:<43}║", format!("{msgs_per_sec:.0}"));
    info!("║  Bandwidth:    {:<43}║", format_bytes(bytes_per_sec));
    info!("║  Errors:       {:<43}║", format!("{total_errors}"));
    info!("║                                                            ║");

    if !all_latencies.is_empty() {
        let p50 = percentile(&all_latencies, 50.0);
        let p95 = percentile(&all_latencies, 95.0);
        let p99 = percentile(&all_latencies, 99.0);
        let p999 = percentile(&all_latencies, 99.9);
        let max = *all_latencies.last().unwrap_or(&0);
        let min = *all_latencies.first().unwrap_or(&0);
        let avg = if all_latencies.is_empty() {
            0
        } else {
            all_latencies.iter().sum::<u64>() / all_latencies.len() as u64
        };

        info!("║  ── Latency (send → ACK) ─────────────────────────────     ║");
        info!(
            "║  samples:      {:<43}║",
            format!("{}", all_latencies.len())
        );
        info!("║  min:          {:<43}║", format_ns(min));
        info!("║  avg:          {:<43}║", format_ns(avg));
        info!("║  p50:          {:<43}║", format_ns(p50));
        info!("║  p95:          {:<43}║", format_ns(p95));
        info!("║  p99:          {:<43}║", format_ns(p99));
        info!("║  p99.9:        {:<43}║", format_ns(p999));
        info!("║  max:          {:<43}║", format_ns(max));
        info!("║                                                            ║");
    }

    info!("╚══════════════════════════════════════════════════════════════╝");
}

/// Setup topic: create if it doesn't exist, return topic_id
async fn setup_topic(endpoint: &str, topic_name: &str) -> Result<u32, String> {
    let mut client = lnc_client::LanceClient::connect_to(endpoint)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    // Try to create topic (returns existing info if already exists)
    match client.create_topic(topic_name).await {
        Ok(info) => {
            info!(topic_id = info.id, name = %info.name, "Topic ready");
            let _ = client.close().await;
            return Ok(info.id);
        },
        Err(e) => {
            warn!("create_topic returned error (may already exist): {e}");
        },
    }

    // Fallback: list topics and find by name
    match client.list_topics().await {
        Ok(topics) => {
            let _ = client.close().await;
            for t in &topics {
                if t.name == topic_name {
                    info!(topic_id = t.id, name = %t.name, "Found existing topic");
                    return Ok(t.id);
                }
            }
            Err(format!(
                "topic '{}' not found after create attempt",
                topic_name
            ))
        },
        Err(e) => {
            let _ = client.close().await;
            Err(format!("list_topics failed: {e}"))
        },
    }
}

/// Generate a deterministic payload of the given size
fn generate_payload(size: usize) -> Bytes {
    let mut data = vec![0u8; size];
    // Fill with a repeating pattern so it's compressible but not all zeros
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 251) as u8; // prime modulus for varied data
    }
    Bytes::from(data)
}

/// Single producer connection task
///
/// Uses a semaphore to maintain `pipeline` concurrent in-flight sends.
/// Each send is spawned as a lightweight task; the semaphore bounds
/// concurrency so the producer's internal batching stays effective.
async fn producer_task(
    conn_id: usize,
    topic_id: u32,
    args: Args,
    metrics: Arc<SharedMetrics>,
    barrier: Arc<Barrier>,
    payload: Bytes,
) -> Vec<u64> {
    let config = lnc_client::ProducerConfig::new()
        .with_batch_size(args.batch_size)
        .with_linger_ms(args.linger_ms)
        .with_buffer_memory(256 * 1024 * 1024); // 256MB buffer per connection

    let producer = match lnc_client::Producer::connect(&args.endpoint, config).await {
        Ok(p) => p,
        Err(e) => {
            error!(conn_id, "Failed to connect: {e}");
            barrier.wait().await;
            return Vec::new();
        },
    };

    // Signal ready
    barrier.wait().await;

    let producer = Arc::new(producer);
    let track_latency = args.latency;
    let msg_size = args.msg_size as u64;
    let pipeline = args.pipeline;

    // Semaphore bounds in-flight sends
    let sem = Arc::new(Semaphore::new(pipeline));

    // Channel for latency samples from spawned tasks
    let (lat_tx, mut lat_rx) = if track_latency {
        let (tx, rx) = mpsc::unbounded_channel::<u64>();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Hot loop — submit sends as fast as the semaphore allows
    while metrics.running.load(Ordering::Relaxed) {
        // Acquire permit (blocks when pipeline is full)
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break, // semaphore closed
        };

        let prod = Arc::clone(&producer);
        let m = Arc::clone(&metrics);
        let pl = payload.clone(); // Bytes clone is Arc increment
        let lat = lat_tx.clone();

        tokio::spawn(async move {
            let start = Instant::now();
            match prod.send(topic_id, &pl).await {
                Ok(_ack) => {
                    m.total_messages.fetch_add(1, Ordering::Relaxed);
                    m.total_bytes.fetch_add(msg_size, Ordering::Relaxed);
                    if let Some(tx) = lat {
                        let _ = tx.send(start.elapsed().as_nanos() as u64);
                    }
                },
                Err(e) => {
                    let prev = m.total_errors.fetch_add(1, Ordering::Relaxed);
                    // Log first error and every 10000th to avoid spam
                    if prev == 0 || prev % 10000 == 0 {
                        warn!("send error (count={}): {}", prev + 1, e);
                    }
                    if matches!(e, lnc_client::ClientError::ServerBackpressure) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                },
            }
            drop(permit); // release semaphore slot
        });
    }

    // Wait for all in-flight sends to drain
    let _ = sem.acquire_many(pipeline as u32).await;

    // Flush + close
    // Arc::into_inner only works if we're the last holder; try_unwrap for safety
    match Arc::try_unwrap(producer) {
        Ok(p) => {
            if let Err(e) = p.flush().await {
                error!(conn_id, "Flush failed: {e}");
            }
            if let Err(e) = p.close().await {
                error!(conn_id, "Close failed: {e}");
            }
        },
        Err(_) => {
            error!(
                conn_id,
                "Could not unwrap producer for close (in-flight tasks still hold refs)"
            );
        },
    }

    // Collect latency samples
    drop(lat_tx); // close sender so receiver drains
    let mut samples = Vec::new();
    if let Some(ref mut rx) = lat_rx {
        rx.close();
        while let Some(ns) = rx.recv().await {
            samples.push(ns);
        }
    }
    samples
}
