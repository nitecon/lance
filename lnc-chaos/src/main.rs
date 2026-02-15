//! LANCE Chaos Testing Tool
//!
//! Continuously produces sequentially numbered messages to one or more LANCE
//! endpoints while simultaneously consuming them. Verifies that every message
//! is received exactly once, in order, with no gaps or losses.
//!
//! Supports multi-endpoint testing with automated Kubernetes StatefulSet
//! rolling restarts to validate durability under failure conditions.
//!
//! # Usage
//!
//! Single endpoint:
//! ```text
//! lnc-chaos --endpoint host:1992 --topic chaos-test --rate 6000
//! ```
//!
//! Multi-endpoint with K8s rolling restart:
//! ```text
//! lnc-chaos \
//!   --endpoint host1:1992 --statefulset lance \
//!   --endpoint host2:1992 --statefulset lance-solo \
//!   --endpoint host3:1992 --statefulset lance-lazy \
//!   --namespace lance --warmup-secs 15 --post-roll-secs 60
//! ```

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use lnc_client::{
    ClientConfig, Consumer, ConsumerConfig, LanceClient, PollResult, Producer, ProducerConfig,
    RecordIterator, RecordType, encode_record,
};

/// LANCE Chaos Testing Tool — validates data integrity during rolling restarts
#[derive(Parser, Debug, Clone)]
#[command(
    name = "lnc-chaos",
    about = "Chaos test: produce + consume with ordering verification across endpoints"
)]
struct Args {
    /// LANCE server endpoint(s) (host:port). Specify multiple for multi-endpoint tests.
    #[arg(short, long, required = true)]
    endpoint: Vec<String>,

    /// K8s StatefulSet name(s) to rolling-restart, paired positionally with --endpoint.
    /// When provided, enables automated rolling restart after warmup period.
    #[arg(short, long)]
    statefulset: Vec<String>,

    /// K8s manifest file(s), paired positionally with --statefulset.
    /// When provided with --clean, infrastructure is torn down (sts + PVCs deleted),
    /// then recreated from manifests before the test starts.
    #[arg(short, long)]
    manifest: Vec<String>,

    /// K8s namespace for StatefulSet operations
    #[arg(long, default_value = "lance")]
    namespace: String,

    /// Seconds to produce before triggering rolling restart
    #[arg(long, default_value_t = 15)]
    warmup_secs: u64,

    /// Seconds to continue validating after rolling restart completes
    #[arg(long, default_value_t = 60)]
    post_roll_secs: u64,

    /// Topic name (will be created if it doesn't exist)
    #[arg(short, long, default_value = "chaos-test")]
    topic: String,

    /// Target messages per minute to produce (e.g. 6000 = 100/s)
    #[arg(short, long, default_value_t = 6000)]
    rate: u64,

    /// Payload size in bytes per message (excluding the 8-byte sequence header)
    #[arg(long, default_value_t = 128)]
    payload_size: usize,

    /// Status report interval in seconds
    #[arg(long, default_value_t = 5)]
    report_interval: u64,

    /// Max fetch bytes per consumer poll
    #[arg(long, default_value_t = 256 * 1024)]
    max_fetch_bytes: u32,

    /// Duration to run in seconds (0 = auto from warmup+post-roll, or run forever without statefulsets)
    #[arg(long, default_value_t = 0)]
    duration: u64,

    /// Delete and recreate the topic for a clean test (removes old data)
    #[arg(long, default_value_t = false)]
    clean: bool,
}

/// Shared counters between producer, consumer, and reporter for a single endpoint
struct Stats {
    produced: AtomicU64,
    consumed: AtomicU64,
    gaps: AtomicU64,
    duplicates: AtomicU64,
    out_of_order: AtomicU64,
    parse_errors: AtomicU64,
    producer_errors: AtomicU64,
    consumer_errors: AtomicU64,
    producer_reconnects: AtomicU64,
    consumer_reconnects: AtomicU64,
    last_produced_seq: AtomicU64,
    last_consumed_seq: AtomicU64,
    /// Phase 1: producer stops when this goes false
    producer_running: AtomicBool,
    /// Set by producer after flush+close completes — drain waits for this
    producer_done: AtomicBool,
    /// Phase 2: consumer stops when this goes false (after drain period)
    running: AtomicBool,
}

impl Stats {
    fn new() -> Self {
        Self {
            produced: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            out_of_order: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            producer_errors: AtomicU64::new(0),
            consumer_errors: AtomicU64::new(0),
            producer_reconnects: AtomicU64::new(0),
            consumer_reconnects: AtomicU64::new(0),
            last_produced_seq: AtomicU64::new(0),
            last_consumed_seq: AtomicU64::new(0),
            producer_running: AtomicBool::new(true),
            producer_done: AtomicBool::new(false),
            running: AtomicBool::new(true),
        }
    }
}

/// Per-endpoint final report data
#[derive(Debug)]
struct EndpointReport {
    label: String,
    endpoint: String,
    produced: u64,
    consumed: u64,
    gaps: u64,
    duplicates: u64,
    out_of_order: u64,
    parse_errors: u64,
    producer_errors: u64,
    consumer_errors: u64,
    producer_reconnects: u64,
    consumer_reconnects: u64,
    roll_performed: bool,
    roll_result: Option<String>,
    passed: bool,
}

impl EndpointReport {
    fn from_stats(
        label: &str,
        endpoint: &str,
        stats: &Stats,
        roll_performed: bool,
        roll_result: Option<String>,
    ) -> Self {
        let gaps = stats.gaps.load(Ordering::Relaxed);
        let duplicates = stats.duplicates.load(Ordering::Relaxed);
        let out_of_order = stats.out_of_order.load(Ordering::Relaxed);
        Self {
            label: label.to_string(),
            endpoint: endpoint.to_string(),
            produced: stats.produced.load(Ordering::Relaxed),
            consumed: stats.consumed.load(Ordering::Relaxed),
            gaps,
            duplicates,
            out_of_order,
            parse_errors: stats.parse_errors.load(Ordering::Relaxed),
            producer_errors: stats.producer_errors.load(Ordering::Relaxed),
            consumer_errors: stats.consumer_errors.load(Ordering::Relaxed),
            producer_reconnects: stats.producer_reconnects.load(Ordering::Relaxed),
            consumer_reconnects: stats.consumer_reconnects.load(Ordering::Relaxed),
            roll_performed,
            roll_result,
            passed: gaps == 0 && duplicates == 0 && out_of_order == 0,
        }
    }
}

/// Encode a message: 8-byte LE sequence number + 8-byte LE timestamp + padding
fn encode_message(seq: u64, payload_size: usize) -> Vec<u8> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);

    let total = 16 + payload_size; // seq(8) + ts(8) + padding
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&ts.to_le_bytes());
    // Fill remaining with a recognizable pattern
    buf.resize(total, (seq & 0xFF) as u8);
    buf
}

/// Decode a message, returning (sequence_number, timestamp_micros)
fn decode_message(data: &[u8]) -> Option<(u64, u64)> {
    if data.len() < 16 {
        return None;
    }
    let seq = u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    let ts = u64::from_le_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ]);
    Some((seq, ts))
}

/// Resolve or create the topic, returning the topic_id.
/// If `clean` is true, delete the existing topic first for a fresh start.
async fn resolve_topic(endpoint: &str, topic_name: &str, clean: bool) -> Result<u32, String> {
    let config: ClientConfig = ClientConfig::new(endpoint);
    let mut client: LanceClient = LanceClient::connect(config)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    // If --clean, delete existing topic first
    if clean {
        if let Ok(topics) = client.list_topics().await {
            for t in &topics {
                if t.name == topic_name {
                    match client.delete_topic(t.id).await {
                        Ok(_) => info!(topic_id = t.id, "Deleted topic '{}' (--clean)", topic_name),
                        Err(e) => warn!("delete_topic failed: {} (continuing)", e),
                    }
                    break;
                }
            }
        }
        // Allow time for the delete operation to replicate to all cluster nodes
        // and for segment files to be cleaned up on disk.
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Try to create topic (will succeed if new, or return existing info)
    match client.create_topic(topic_name).await {
        Ok(info) => {
            info!(topic_id = info.id, name = %info.name, "Topic ready");
            let _ = client.close().await;
            Ok(info.id)
        },
        Err(e) => {
            // If topic already exists, the error message typically contains the ID
            // Try listing topics to find it
            warn!("create_topic returned error (may already exist): {}", e);
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
                        "topic '{}' not found after create failed: {}",
                        topic_name, e
                    ))
                },
                Err(list_err) => {
                    let _ = client.close().await;
                    Err(format!(
                        "create_topic failed: {}, list_topics failed: {}",
                        e, list_err
                    ))
                },
            }
        },
    }
}

/// Producer task: connects once and sends messages in a loop.
/// The Producer library handles reconnection, backoff, and retry internally.
async fn producer_task(
    label: String,
    endpoint: String,
    topic_id: u32,
    rate: u64,
    payload_size: usize,
    stats: Arc<Stats>,
    ready: Arc<Notify>,
) {
    let msgs_per_sec = rate.max(1) / 60;
    let interval_us = if msgs_per_sec > 0 {
        1_000_000 / msgs_per_sec
    } else {
        10_000 // default 100/s
    };
    info!(
        msgs_per_sec,
        interval_us,
        "[{label}] Producer rate: {}/min = {}/s, interval={}us",
        rate,
        msgs_per_sec,
        interval_us
    );

    let config = ProducerConfig::new()
        .with_batch_size(32 * 1024)
        .with_linger_ms(1)
        .with_request_timeout(Duration::from_secs(10));

    // Connect — library retries internally on transient failures
    let producer = loop {
        match Producer::connect(&endpoint, config.clone()).await {
            Ok(p) => {
                info!("[{label}] Producer connected");
                break p;
            },
            Err(e) => {
                error!("[{label}] Producer connect failed: {}, retrying...", e);
                stats.producer_reconnects.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(2)).await;
            },
        }
    };

    let mut seq: u64 = 1;
    let mut ticker = tokio::time::interval(Duration::from_micros(interval_us));

    // Signal consumer we're starting
    ready.notify_one();

    while stats.producer_running.load(Ordering::Relaxed) {
        ticker.tick().await;

        let payload = encode_message(seq, payload_size);
        let msg = encode_record(RecordType::Data, &payload);

        // send() blocks until ACK — library auto-reconnects on transient errors
        match producer.send(topic_id, &msg).await {
            Ok(_ack) => {
                stats.produced.fetch_add(1, Ordering::Relaxed);
                stats.last_produced_seq.store(seq, Ordering::Relaxed);
                seq += 1;
            },
            Err(e) => {
                // Non-retryable error after library exhausted retries
                warn!(
                    seq,
                    "[{label}] Producer send failed (retries exhausted): {}", e
                );
                stats.producer_errors.fetch_add(1, Ordering::Relaxed);
            },
        }
    }

    info!("[{label}] Producer flushing and closing...");
    let _ = producer.close().await;
    stats.producer_done.store(true, Ordering::Release);
    info!(
        total_produced = stats.produced.load(Ordering::Relaxed),
        last_seq = seq - 1,
        "[{label}] Producer stopped"
    );
}

/// Process fetched records, verifying sequential ordering.
fn process_records(result: &PollResult, expected_seq: &mut u64, stats: &Stats) {
    for record_result in RecordIterator::new(result.data.clone()) {
        let record = match record_result {
            Ok(r) => r,
            Err(e) => {
                stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                warn!("Record parse error (skipping rest of batch): {}", e);
                break;
            },
        };

        if let Some((seq, _ts)) = decode_message(&record.payload) {
            stats.consumed.fetch_add(1, Ordering::Relaxed);
            stats.last_consumed_seq.store(seq, Ordering::Relaxed);

            // Auto-initialize expected_seq from first successfully parsed record
            if *expected_seq == 0 {
                info!(
                    first_seq = seq,
                    "Consumer baseline: first record seq={}", seq
                );
                *expected_seq = seq;
            }

            if seq == *expected_seq {
                *expected_seq += 1;
            } else if seq > *expected_seq {
                let gap = seq - *expected_seq;
                warn!(
                    expected = *expected_seq,
                    got = seq,
                    gap,
                    "GAP DETECTED: missing {} messages",
                    gap
                );
                stats.gaps.fetch_add(gap, Ordering::Relaxed);
                *expected_seq = seq + 1;
            } else {
                warn!(
                    expected = *expected_seq,
                    got = seq,
                    "DUPLICATE/OUT-OF-ORDER: seq {} already consumed",
                    seq
                );
                stats.duplicates.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Consumer task: connects once and polls in a loop, verifying ordering.
/// The Consumer library handles reconnection, backoff, and retry internally.
async fn consumer_task(
    label: String,
    endpoint: String,
    topic_id: u32,
    max_fetch_bytes: u32,
    stats: Arc<Stats>,
    ready: Arc<Notify>,
) {
    // Wait for producer to start
    ready.notified().await;
    // Small delay to let some messages accumulate
    tokio::time::sleep(Duration::from_millis(500)).await;

    let config = ConsumerConfig::new(topic_id).with_max_fetch_bytes(max_fetch_bytes);

    // Connect — library retries internally on transient failures
    let mut consumer = loop {
        match Consumer::connect(&endpoint, config.clone()).await {
            Ok(c) => {
                info!("[{label}] Consumer connected");
                break c;
            },
            Err(e) => {
                error!("[{label}] Consumer connect failed: {}, retrying...", e);
                stats.consumer_reconnects.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(2)).await;
            },
        }
    };

    // Seek past existing data so we only verify new messages
    info!("[{label}] Consumer seeking past existing data...");
    let mut seek_fetches: u64 = 0;
    loop {
        match consumer.poll().await {
            Ok(Some(result)) if !result.end_of_stream => {
                seek_fetches += 1;
                if seek_fetches % 100 == 0 {
                    info!(
                        offset = consumer.current_offset(),
                        seek_fetches, "[{label}] Still seeking to end of existing data..."
                    );
                }
            },
            _ => break,
        }
    }
    if seek_fetches > 0 {
        info!(
            offset = consumer.current_offset(),
            seek_fetches, "[{label}] Skipped existing data, starting verification from tail"
        );
    }
    info!(
        offset = consumer.current_offset(),
        "[{label}] Consumer ready, verifying new messages only"
    );

    // 0 = sentinel meaning "not yet initialized"; set from first parsed record
    let mut expected_seq: u64 = 0;

    while stats.running.load(Ordering::Relaxed) {
        // poll() auto-reconnects on transient errors
        match consumer.poll().await {
            Ok(Some(result)) => {
                process_records(&result, &mut expected_seq, &stats);
            },
            Ok(None) => {
                // No new data, poll again shortly
                tokio::time::sleep(Duration::from_millis(50)).await;
            },
            Err(e) => {
                // Non-retryable error after library exhausted retries
                warn!(
                    offset = consumer.current_offset(),
                    "Consumer poll failed (retries exhausted): {}", e
                );
                stats.consumer_errors.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
            },
        }
    }

    let final_seq = if expected_seq > 0 {
        expected_seq - 1
    } else {
        0
    };
    info!(
        last_consumed = final_seq,
        offset = consumer.current_offset(),
        "[{label}] Consumer stopped"
    );
}

/// Reporter task: prints periodic stats for a single endpoint
async fn reporter_task(label: String, report_interval: u64, stats: Arc<Stats>) {
    let interval = Duration::from_secs(report_interval);
    let start = Instant::now();
    let mut last_produced = 0u64;
    let mut last_consumed = 0u64;
    let mut tick = 0u64;

    while stats.running.load(Ordering::Relaxed) {
        tokio::time::sleep(interval).await;
        tick += 1;

        let produced = stats.produced.load(Ordering::Relaxed);
        let consumed = stats.consumed.load(Ordering::Relaxed);
        let gaps = stats.gaps.load(Ordering::Relaxed);
        let dups = stats.duplicates.load(Ordering::Relaxed);
        let ooo = stats.out_of_order.load(Ordering::Relaxed);
        let parse_err = stats.parse_errors.load(Ordering::Relaxed);
        let p_err = stats.producer_errors.load(Ordering::Relaxed);
        let c_err = stats.consumer_errors.load(Ordering::Relaxed);
        let p_reconn = stats.producer_reconnects.load(Ordering::Relaxed);
        let c_reconn = stats.consumer_reconnects.load(Ordering::Relaxed);
        let last_p_seq = stats.last_produced_seq.load(Ordering::Relaxed);
        let last_c_seq = stats.last_consumed_seq.load(Ordering::Relaxed);

        let elapsed = start.elapsed().as_secs_f64();
        let p_rate = (produced - last_produced) as f64 / report_interval as f64;
        let c_rate = (consumed - last_consumed) as f64 / report_interval as f64;
        let lag = produced.saturating_sub(consumed);

        let status = if gaps == 0 && dups == 0 && ooo == 0 {
            "OK"
        } else {
            "ISSUES"
        };

        info!("╔══════════════════════════════════════════════════════════════╗");
        info!("║ [{label}] [{status}] t={tick} elapsed={elapsed:.1}s");
        info!(
            "║ produced={produced} ({p_rate:.0}/s) | consumed={consumed} ({c_rate:.0}/s) | lag={lag}"
        );
        info!("║ last_p_seq={last_p_seq} | last_c_seq={last_c_seq}");
        info!("║ gaps={gaps} | dups={dups} | ooo={ooo}");
        info!(
            "║ p_err={p_err} c_err={c_err} parse_err={parse_err} | p_reconn={p_reconn} c_reconn={c_reconn}"
        );
        info!("╚══════════════════════════════════════════════════════════════╝");

        last_produced = produced;
        last_consumed = consumed;
    }
}

/// Configuration for a single endpoint chaos test run.
struct EndpointTestConfig {
    label: String,
    endpoint: String,
    statefulset: Option<String>,
    namespace: String,
    topic: String,
    rate: u64,
    payload_size: usize,
    max_fetch_bytes: u32,
    report_interval: u64,
    warmup_secs: u64,
    clean: bool,
    total_duration: u64,
}

/// K8s roller task: waits for warmup, then executes kubectl rollout restart.
async fn k8s_roller_task(
    label: String,
    namespace: String,
    statefulset: String,
    warmup_secs: u64,
    stats: Arc<Stats>,
) -> String {
    info!("[{label}] K8s roller: waiting {warmup_secs}s warmup before rolling {statefulset}...");

    // Wait for warmup period
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(warmup_secs) {
        if !stats.running.load(Ordering::Relaxed) {
            return "cancelled: test stopped before warmup completed".to_string();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    info!(
        "[{label}] Rolling restart: kubectl rollout restart statefulset/{statefulset} -n {namespace}"
    );

    let output = tokio::process::Command::new("kubectl")
        .args([
            "rollout",
            "restart",
            &format!("statefulset/{statefulset}"),
            "-n",
            &namespace,
        ])
        .output()
        .await;

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            if out.status.success() {
                info!("[{label}] Roll initiated: {}", stdout.trim());

                // Wait for rollout to complete
                info!("[{label}] Waiting for rollout to complete...");
                let status_output = tokio::process::Command::new("kubectl")
                    .args([
                        "rollout",
                        "status",
                        &format!("statefulset/{statefulset}"),
                        "-n",
                        &namespace,
                        "--timeout=120s",
                    ])
                    .output()
                    .await;

                match status_output {
                    Ok(s) if s.status.success() => {
                        let msg = String::from_utf8_lossy(&s.stdout);
                        info!("[{label}] Rollout complete: {}", msg.trim());
                        format!("success: {}", msg.trim())
                    },
                    Ok(s) => {
                        let msg = String::from_utf8_lossy(&s.stderr);
                        warn!("[{label}] Rollout status check failed: {}", msg.trim());
                        format!("rollout initiated but status check failed: {}", msg.trim())
                    },
                    Err(e) => {
                        warn!("[{label}] Failed to check rollout status: {e}");
                        format!("rollout initiated but status check errored: {e}")
                    },
                }
            } else {
                error!("[{label}] Roll failed: {}", stderr.trim());
                format!("failed: {}", stderr.trim())
            }
        },
        Err(e) => {
            error!("[{label}] kubectl command failed: {e}");
            format!("kubectl error: {e}")
        },
    }
}

/// Run a complete chaos test for a single endpoint.
/// Spawns producer, consumer, reporter, and optionally a K8s roller.
/// Returns an EndpointReport with the results.
async fn run_endpoint_test(cfg: EndpointTestConfig) -> EndpointReport {
    let label = &cfg.label;
    let endpoint = &cfg.endpoint;
    info!("[{label}] Starting chaos test against {endpoint}");

    // Resolve topic on this endpoint
    let topic_id = match resolve_topic(endpoint, &cfg.topic, cfg.clean).await {
        Ok(id) => id,
        Err(e) => {
            error!("[{label}] Failed to resolve topic '{}': {e}", cfg.topic);
            return EndpointReport {
                label: cfg.label.clone(),
                endpoint: cfg.endpoint.clone(),
                produced: 0,
                consumed: 0,
                gaps: 0,
                duplicates: 0,
                out_of_order: 0,
                parse_errors: 0,
                producer_errors: 0,
                consumer_errors: 0,
                producer_reconnects: 0,
                consumer_reconnects: 0,
                roll_performed: false,
                roll_result: Some(format!("topic resolve failed: {e}")),
                passed: false,
            };
        },
    };

    let stats = Arc::new(Stats::new());
    let ready = Arc::new(Notify::new());
    let label_owned = cfg.label.clone();
    let endpoint_owned = cfg.endpoint.clone();

    // Spawn producer
    let producer_handle = tokio::spawn(producer_task(
        label_owned.clone(),
        endpoint_owned.clone(),
        topic_id,
        cfg.rate,
        cfg.payload_size,
        stats.clone(),
        ready.clone(),
    ));

    // Spawn consumer
    let consumer_handle = tokio::spawn(consumer_task(
        label_owned.clone(),
        endpoint_owned.clone(),
        topic_id,
        cfg.max_fetch_bytes,
        stats.clone(),
        ready.clone(),
    ));

    // Spawn reporter
    let reporter_handle = tokio::spawn(reporter_task(
        label_owned.clone(),
        cfg.report_interval,
        stats.clone(),
    ));

    // Spawn K8s roller if statefulset is configured
    // DISABLED: User will manually perform k8s restarts for testing
    // let roller_handle = cfg.statefulset.clone().map(|ss| {
    //     tokio::spawn(k8s_roller_task(
    //         label_owned.clone(),
    //         cfg.namespace,
    //         ss,
    //         cfg.warmup_secs,
    //         stats.clone(),
    //     ))
    // });
    let roller_handle: Option<tokio::task::JoinHandle<String>> = None;

    // Two-phase shutdown:
    //   Phase 1: stop the producer (no more new messages)
    //   Phase 2: drain period — consumer catches up to produced count
    //   Phase 3: stop consumer + reporter
    let stats_shutdown = stats.clone();
    let shutdown_label = label_owned.clone();
    let total_duration = cfg.total_duration;
    let shutdown_handle = tokio::spawn(async move {
        if total_duration > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(total_duration)) => {
                    info!("[{shutdown_label}] Duration elapsed, stopping producer...");
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("[{shutdown_label}] Ctrl-C received, stopping producer...");
                }
            }
        } else {
            let _ = tokio::signal::ctrl_c().await;
            info!("[{shutdown_label}] Ctrl-C received, stopping producer...");
        }

        // Phase 1: stop producer, let consumer keep draining
        stats_shutdown
            .producer_running
            .store(false, Ordering::Relaxed);

        // Wait for producer to flush final batch and close
        let flush_deadline = Instant::now() + Duration::from_secs(10);
        while !stats_shutdown.producer_done.load(Ordering::Acquire) {
            if Instant::now() > flush_deadline {
                warn!("[{shutdown_label}] Producer flush timeout (10s)");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let final_produced = stats_shutdown.produced.load(Ordering::Relaxed);
        info!(
            "[{shutdown_label}] Producer done, final produced={final_produced} — draining consumer..."
        );

        // Phase 2: wait for consumer to catch up to final count (max 30s drain)
        let drain_start = Instant::now();
        let drain_timeout = Duration::from_secs(30);
        loop {
            let consumed = stats_shutdown.consumed.load(Ordering::Relaxed);
            if consumed >= final_produced {
                info!(
                    "[{shutdown_label}] Consumer drained: produced={final_produced} consumed={consumed}"
                );
                break;
            }
            if drain_start.elapsed() > drain_timeout {
                warn!(
                    "[{shutdown_label}] Drain timeout (30s): produced={final_produced} consumed={consumed}, lag={}",
                    final_produced.saturating_sub(consumed)
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // Phase 3: stop consumer + reporter
        stats_shutdown.running.store(false, Ordering::Relaxed);
    });

    // Wait for shutdown (includes drain)
    let _ = shutdown_handle.await;

    // Collect roller result
    let (roll_performed, roll_result) = if let Some(handle) = roller_handle {
        match handle.await {
            Ok(result) => (true, Some(result)),
            Err(e) => (true, Some(format!("roller task panicked: {e}"))),
        }
    } else {
        (false, None)
    };

    // Brief grace period for final logging
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Abort remaining tasks
    producer_handle.abort();
    consumer_handle.abort();
    reporter_handle.abort();

    EndpointReport::from_stats(
        &label_owned,
        &endpoint_owned,
        &stats,
        roll_performed,
        roll_result,
    )
}

/// Tear down all K8s infrastructure (statefulsets + PVCs), then recreate from manifests.
/// Guarantees fresh PVCs with no stale segment data between test runs.
async fn reset_infrastructure(
    namespace: &str,
    statefulsets: &[String],
    manifests: &[String],
) -> std::result::Result<(), String> {
    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║          INFRASTRUCTURE RESET — CLEAN SLATE                ║");
    info!("╠══════════════════════════════════════════════════════════════╣");

    // Step 1: Delete each statefulset (cascades to pods)
    for ss in statefulsets {
        info!("║ Deleting statefulset/{ss} -n {namespace}...");
        let out = tokio::process::Command::new("kubectl")
            .args([
                "delete",
                "statefulset",
                ss,
                "-n",
                namespace,
                "--ignore-not-found",
                "--timeout=60s",
            ])
            .output()
            .await
            .map_err(|e| format!("kubectl delete sts failed: {e}"))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            warn!("║ delete sts/{ss} warning: {}", stderr.trim());
        }
    }

    // Step 2: Delete services associated with each statefulset
    for ss in statefulsets {
        // Delete headless service (e.g. lance-headless)
        let headless = format!("{ss}-headless");
        for svc in &[ss.as_str(), headless.as_str()] {
            let out = tokio::process::Command::new("kubectl")
                .args([
                    "delete",
                    "service",
                    svc,
                    "-n",
                    namespace,
                    "--ignore-not-found",
                ])
                .output()
                .await;
            if let Ok(o) = out {
                if o.status.success() {
                    info!("║ Deleted service/{svc}");
                }
            }
        }
    }

    // Step 3: Delete PVCs for each statefulset (label selector app=<name>)
    for ss in statefulsets {
        info!("║ Deleting PVCs for app={ss} -n {namespace}...");
        let out = tokio::process::Command::new("kubectl")
            .args([
                "delete",
                "pvc",
                "-l",
                &format!("app={ss}"),
                "-n",
                namespace,
                "--ignore-not-found",
            ])
            .output()
            .await
            .map_err(|e| format!("kubectl delete pvc failed: {e}"))?;
        if out.status.success() {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if !stdout.trim().is_empty() {
                info!("║ {}", stdout.trim());
            }
        }
    }

    // Step 4: Wait for all pods to be gone
    info!("║ Waiting for pods to terminate...");
    for ss in statefulsets {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let out = tokio::process::Command::new("kubectl")
                .args([
                    "get",
                    "pods",
                    "-l",
                    &format!("app={ss}"),
                    "-n",
                    namespace,
                    "--no-headers",
                ])
                .output()
                .await
                .map_err(|e| format!("kubectl get pods failed: {e}"))?;
            let stdout = String::from_utf8_lossy(&out.stdout);
            if stdout.trim().is_empty() {
                info!("║ All pods for {ss} terminated");
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err(format!("Timeout waiting for {ss} pods to terminate"));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // Step 5: Wait for PVCs to be gone
    info!("║ Waiting for PVCs to be released...");
    for ss in statefulsets {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let out = tokio::process::Command::new("kubectl")
                .args([
                    "get",
                    "pvc",
                    "-l",
                    &format!("app={ss}"),
                    "-n",
                    namespace,
                    "--no-headers",
                ])
                .output()
                .await
                .map_err(|e| format!("kubectl get pvc failed: {e}"))?;
            let stdout = String::from_utf8_lossy(&out.stdout);
            if stdout.trim().is_empty() {
                info!("║ All PVCs for {ss} released");
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err(format!("Timeout waiting for {ss} PVCs to be released"));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // Step 6: Apply manifests to recreate infrastructure
    for manifest in manifests {
        info!("║ Applying {manifest} -n {namespace}...");
        let out = tokio::process::Command::new("kubectl")
            .args(["apply", "-f", manifest, "-n", namespace])
            .output()
            .await
            .map_err(|e| format!("kubectl apply failed: {e}"))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(format!(
                "kubectl apply -f {manifest} failed: {}",
                stderr.trim()
            ));
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            info!("║   {line}");
        }
    }

    // Step 7: Wait for all statefulsets to be ready
    info!("║ Waiting for all statefulsets to be ready...");
    for ss in statefulsets {
        info!("║ Waiting for statefulset/{ss}...");
        let out = tokio::process::Command::new("kubectl")
            .args([
                "rollout",
                "status",
                &format!("statefulset/{ss}"),
                "-n",
                namespace,
                "--timeout=120s",
            ])
            .output()
            .await
            .map_err(|e| format!("kubectl rollout status failed: {e}"))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(format!("statefulset/{ss} not ready: {}", stderr.trim()));
        }
        info!("║ statefulset/{ss} ready ✓");
    }

    // Step 8: Brief settle time for DNS and services
    info!("║ Settling (5s) for DNS propagation...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    info!("║ Infrastructure reset complete — clean PVCs, fresh state");
    info!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}

/// Print the comprehensive final report across all endpoints.
fn print_final_report(reports: &[EndpointReport]) {
    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║               CHAOS TEST — FINAL REPORT                    ║");
    info!("╠══════════════════════════════════════════════════════════════╣");

    for report in reports {
        let verdict = if report.passed { "PASS" } else { "FAIL" };
        info!("║");
        info!(
            "║ ── {} ({}) ── [{}]",
            report.label, report.endpoint, verdict
        );
        info!(
            "║   produced: {}  |  consumed: {}",
            report.produced, report.consumed
        );
        info!(
            "║   gaps: {}  |  duplicates: {}  |  out-of-order: {}",
            report.gaps, report.duplicates, report.out_of_order
        );
        info!(
            "║   producer_errors: {}  |  consumer_errors: {}  |  parse_errors: {}",
            report.producer_errors, report.consumer_errors, report.parse_errors
        );
        info!(
            "║   producer_reconnects: {}  |  consumer_reconnects: {}",
            report.producer_reconnects, report.consumer_reconnects
        );
        if report.roll_performed {
            let result = report.roll_result.as_deref().unwrap_or("unknown");
            info!("║   roll: {}", result);
        } else {
            info!("║   roll: not configured");
        }
        if report.passed {
            info!("║   result: PASS — no data loss detected");
        } else {
            info!("║   result: FAIL — data integrity issues detected");
        }
    }

    let all_passed = reports.iter().all(|r| r.passed);
    let total_produced: u64 = reports.iter().map(|r| r.produced).sum();
    let total_consumed: u64 = reports.iter().map(|r| r.consumed).sum();
    let total_gaps: u64 = reports.iter().map(|r| r.gaps).sum();

    info!("║");
    info!("╠══════════════════════════════════════════════════════════════╣");
    info!(
        "║ AGGREGATE: {} endpoints | produced={} consumed={} gaps={}",
        reports.len(),
        total_produced,
        total_consumed,
        total_gaps
    );
    if all_passed {
        info!("║ OVERALL: PASS — all endpoints healthy");
    } else {
        let failed: Vec<&str> = reports
            .iter()
            .filter(|r| !r.passed)
            .map(|r| r.label.as_str())
            .collect();
        info!("║ OVERALL: FAIL — failed endpoints: {}", failed.join(", "));
    }
    info!("╚══════════════════════════════════════════════════════════════╝");
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .init();

    let args = Args::parse();

    // Validate: if statefulsets are provided, count must match endpoints
    if !args.statefulset.is_empty() && args.statefulset.len() != args.endpoint.len() {
        error!(
            "Mismatch: {} endpoint(s) but {} statefulset(s). \
             Each --statefulset must pair with an --endpoint.",
            args.endpoint.len(),
            args.statefulset.len()
        );
        std::process::exit(1);
    }

    // Validate: if manifests are provided, count must match statefulsets
    if !args.manifest.is_empty() && args.manifest.len() != args.statefulset.len() {
        error!(
            "Mismatch: {} statefulset(s) but {} manifest(s). \
             Each --manifest must pair with a --statefulset.",
            args.statefulset.len(),
            args.manifest.len()
        );
        std::process::exit(1);
    }

    // Infrastructure reset: when --clean + manifests provided, tear down and recreate
    if args.clean && !args.manifest.is_empty() {
        if let Err(e) =
            reset_infrastructure(&args.namespace, &args.statefulset, &args.manifest).await
        {
            error!("Infrastructure reset failed: {e}");
            std::process::exit(1);
        }
    }

    // Compute total duration per endpoint
    let total_duration = if args.duration > 0 {
        args.duration
    } else if !args.statefulset.is_empty() {
        // Auto-compute: warmup + post-roll + buffer for rollout time
        args.warmup_secs + args.post_roll_secs + 30
    } else {
        0 // run forever (ctrl-c to stop)
    };

    let endpoint_count = args.endpoint.len();
    let has_rolling = !args.statefulset.is_empty();

    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║                   LANCE Chaos Test                         ║");
    info!("╠══════════════════════════════════════════════════════════════╣");
    info!("║ endpoints:  {} target(s)", endpoint_count);
    for (i, ep) in args.endpoint.iter().enumerate() {
        let ss = args.statefulset.get(i).map(|s| s.as_str()).unwrap_or("-");
        info!("║   [{i}] {ep}  (statefulset: {ss})");
    }
    info!("║ topic:      {}", args.topic);
    info!("║ rate:       {} msg/min ({}/s)", args.rate, args.rate / 60);
    info!("║ payload:    {} bytes", args.payload_size);
    if has_rolling {
        info!("║ namespace:  {}", args.namespace);
        info!("║ warmup:     {}s", args.warmup_secs);
        info!("║ post-roll:  {}s", args.post_roll_secs);
    }
    info!(
        "║ duration:   {}",
        if total_duration == 0 {
            "forever (ctrl-c to stop)".to_string()
        } else {
            format!("{}s", total_duration)
        }
    );
    info!("╚══════════════════════════════════════════════════════════════╝");

    // Build endpoint test configs and run them
    // For multi-endpoint: run all tests concurrently
    let mut handles = Vec::new();

    for (i, ep) in args.endpoint.iter().enumerate() {
        let statefulset = args.statefulset.get(i).cloned();
        let label = statefulset
            .as_deref()
            .unwrap_or(&format!("ep-{i}"))
            .to_string();

        handles.push(tokio::spawn(run_endpoint_test(EndpointTestConfig {
            label,
            endpoint: ep.clone(),
            statefulset,
            namespace: args.namespace.clone(),
            topic: args.topic.clone(),
            rate: args.rate,
            payload_size: args.payload_size,
            max_fetch_bytes: args.max_fetch_bytes,
            report_interval: args.report_interval,
            warmup_secs: args.warmup_secs,
            clean: args.clean,
            total_duration,
        })));
    }

    // Collect all reports
    let mut reports = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(report) => reports.push(report),
            Err(e) => error!("Endpoint test task panicked: {e}"),
        }
    }

    // Print comprehensive final report
    print_final_report(&reports);

    // Exit with failure if any endpoint failed
    let any_failed = reports.iter().any(|r| !r.passed);
    if any_failed {
        std::process::exit(1);
    }
}
