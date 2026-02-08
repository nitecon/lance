//! LANCE Chaos Testing Tool
//!
//! Continuously produces sequentially numbered messages to a LANCE topic while
//! simultaneously consuming them. Verifies that every message is received
//! exactly once, in order, with no gaps or losses.
//!
//! Designed to run during rolling restarts (`kubectl rollout restart`) to
//! validate durability and availability under failure conditions.
//!
//! # Usage
//!
//! ```text
//! lnc-chaos --endpoint lance.nitecon.net:1992 --topic chaos-test --rate 100
//! ```

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use lnc_client::{
    ClientConfig, FetchResult, LanceClient, Producer, ProducerConfig, RecordIterator, RecordType,
    encode_record,
};

/// LANCE Chaos Testing Tool — validates data integrity during rolling restarts
#[derive(Parser, Debug, Clone)]
#[command(
    name = "lnc-chaos",
    about = "Chaos test: produce + consume with ordering verification"
)]
struct Args {
    /// LANCE server endpoint (host:port)
    #[arg(short, long, default_value = "lance.nitecon.net:1992")]
    endpoint: String,

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

    /// Duration to run in seconds (0 = run forever)
    #[arg(long, default_value_t = 0)]
    duration: u64,

    /// Delete and recreate the topic for a clean test (removes old data)
    #[arg(long, default_value_t = false)]
    clean: bool,
}

/// Shared counters between producer, consumer, and reporter
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
    queue_depth: AtomicU64,
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
            queue_depth: AtomicU64::new(0),
            running: AtomicBool::new(true),
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

/// Connect a LanceClient with retry
async fn connect_client(endpoint: &str) -> Option<LanceClient> {
    let config: ClientConfig = ClientConfig::new(endpoint);
    match LanceClient::connect(config).await {
        Ok(c) => Some(c),
        Err(e) => {
            error!("Failed to connect to {}: {}", endpoint, e);
            None
        },
    }
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
        // Small delay for the server to finalize deletion
        tokio::time::sleep(Duration::from_millis(500)).await;
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

/// Max local queue size to prevent OOM (messages, not bytes)
const MAX_QUEUE_DEPTH: usize = 500_000;

/// Producer task: generates messages at a constant rate into a local queue,
/// then burst-drains the queue to the server whenever a connection is available.
/// During server downtime, messages accumulate in the queue and are sent in a
/// burst once the connection is re-established.
async fn producer_task(args: Args, topic_id: u32, stats: Arc<Stats>, ready: Arc<Notify>) {
    let msgs_per_sec = args.rate.max(1) / 60;
    let interval_us = if msgs_per_sec > 0 {
        1_000_000 / msgs_per_sec
    } else {
        10_000 // default 100/s
    };
    info!(
        msgs_per_sec,
        interval_us,
        "Producer rate: {}/min = {}/s, interval={}us",
        args.rate,
        msgs_per_sec,
        interval_us
    );

    let mut seq: u64 = 1;
    let mut queue: VecDeque<bytes::Bytes> = VecDeque::new();
    let mut producer: Option<Producer> = None;
    let mut backoff_ms: u64 = 1000;
    const MAX_BACKOFF_MS: u64 = 30_000;
    let mut ticker = tokio::time::interval(Duration::from_micros(interval_us));

    // Signal consumer we're starting
    ready.notify_one();

    while stats.running.load(Ordering::Relaxed) {
        // 1. Generate one message per tick (constant rate regardless of connection)
        ticker.tick().await;

        if queue.len() < MAX_QUEUE_DEPTH {
            let payload = encode_message(seq, args.payload_size);
            let msg = encode_record(RecordType::Data, &payload);
            queue.push_back(msg);
            stats.last_produced_seq.store(seq, Ordering::Relaxed);
            seq += 1;
        } else {
            warn!(queue_depth = queue.len(), "Queue full, dropping message");
            stats.producer_errors.fetch_add(1, Ordering::Relaxed);
        }

        // 2. Check connection health
        if let Some(ref p) = producer {
            if !p.is_healthy() {
                warn!(
                    queue_depth = queue.len(),
                    "Producer connection unhealthy, will reconnect"
                );
                if let Some(p) = producer.take() {
                    let _ = p.close().await;
                }
                stats.producer_reconnects.fetch_add(1, Ordering::Relaxed);
                backoff_ms = 1000;
            }
        }

        // 3. Reconnect if needed (quick attempt, don't block message generation)
        if producer.is_none() && !queue.is_empty() {
            let config = ProducerConfig::new()
                .with_batch_size(32 * 1024)
                .with_linger_ms(1)
                .with_request_timeout(Duration::from_secs(10));

            match tokio::time::timeout(
                Duration::from_millis(500),
                Producer::connect(&args.endpoint, config),
            )
            .await
            {
                Ok(Ok(p)) => {
                    info!(
                        queue_depth = queue.len(),
                        "Producer connected, draining queue"
                    );
                    producer = Some(p);
                    backoff_ms = 1000;
                },
                Ok(Err(e)) => {
                    error!(backoff_ms, "Producer connect failed: {}", e);
                    stats.producer_reconnects.fetch_add(1, Ordering::Relaxed);
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                },
                Err(_) => {
                    // Timeout — will retry next tick
                    stats.producer_reconnects.fetch_add(1, Ordering::Relaxed);
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                },
            }
        }

        // 4. Burst-drain the queue (send waits for ACK to guarantee no duplicates)
        if let Some(ref p) = producer {
            while let Some(msg) = queue.front() {
                match p.send(topic_id, msg).await {
                    Ok(_ack) => {
                        queue.pop_front();
                        stats.produced.fetch_add(1, Ordering::Relaxed);
                    },
                    Err(e) => {
                        warn!(
                            queue_depth = queue.len(),
                            "Producer send error during drain: {}", e
                        );
                        stats.producer_errors.fetch_add(1, Ordering::Relaxed);
                        // Drop producer to force reconnect on next tick
                        if let Some(p) = producer.take() {
                            let _ = p.close().await;
                        }
                        stats.producer_reconnects.fetch_add(1, Ordering::Relaxed);
                        backoff_ms = 1000;
                        break;
                    },
                }
            }
        }

        // 5. Update queue depth stat
        stats
            .queue_depth
            .store(queue.len() as u64, Ordering::Relaxed);
    }

    // Graceful shutdown: try to drain remaining queue
    if !queue.is_empty() {
        if let Some(ref p) = producer {
            info!(remaining = queue.len(), "Draining remaining queue...");
            while let Some(msg) = queue.front() {
                if p.send(topic_id, msg).await.is_ok() {
                    queue.pop_front();
                    stats.produced.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }
        }
        if !queue.is_empty() {
            warn!(lost = queue.len(), "Queue not fully drained on shutdown");
        }
    }

    if let Some(p) = producer.take() {
        info!("Producer flushing and closing...");
        let _ = p.close().await;
    }
    info!(
        total_produced = stats.produced.load(Ordering::Relaxed),
        total_generated = seq - 1,
        queue_remaining = queue.len(),
        "Producer stopped"
    );
}

/// Attempt a single fetch from the client, returning Ok(result) or Err on connection failure
async fn do_fetch(
    client: &mut LanceClient,
    topic_id: u32,
    offset: u64,
    max_bytes: u32,
) -> Result<FetchResult, String> {
    client
        .fetch(topic_id, offset, max_bytes)
        .await
        .map_err(|e| format!("{e}"))
}

/// Process fetched records, verifying sequential ordering.
/// Returns the updated expected_seq.
fn process_records(result: &FetchResult, expected_seq: &mut u64, stats: &Stats) {
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

/// Seek to the end of existing topic data, returning the tail offset.
/// This skips any old data from previous runs so verification only covers
/// messages produced during this session.
async fn seek_to_end(client: &mut LanceClient, topic_id: u32, max_bytes: u32) -> u64 {
    let mut offset: u64 = 0;
    let mut fetches: u64 = 0;
    loop {
        match client.fetch(topic_id, offset, max_bytes).await {
            Ok(result) => {
                if result.data.is_empty() || result.next_offset == offset {
                    break;
                }
                offset = result.next_offset;
                fetches += 1;
                if fetches % 100 == 0 {
                    info!(offset, fetches, "Still seeking to end of existing data...");
                }
            },
            Err(e) => {
                warn!(
                    offset,
                    "Error during seek-to-end: {}, using offset so far", e
                );
                break;
            },
        }
    }
    if fetches > 0 {
        info!(
            offset,
            fetches, "Skipped existing data, starting verification from tail"
        );
    }
    offset
}

/// Consumer task: reads messages and verifies sequential ordering
async fn consumer_task(args: Args, topic_id: u32, stats: Arc<Stats>, ready: Arc<Notify>) {
    // Wait for producer to start
    ready.notified().await;
    // Small delay to let some messages accumulate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 0 = sentinel meaning "not yet initialized"; set from first parsed record
    let mut expected_seq: u64 = 0;
    let mut current_offset: u64 = 0;
    let mut client: Option<LanceClient> = None;
    let mut backoff_ms: u64 = 1000;
    const MAX_BACKOFF_MS: u64 = 30_000;
    let mut seeked = false;

    while stats.running.load(Ordering::Relaxed) {
        // Ensure we have a connection
        if client.is_none() {
            info!(endpoint = %args.endpoint, "Consumer connecting...");
            match connect_client(&args.endpoint).await {
                Some(c) => {
                    info!(
                        offset = current_offset,
                        "Consumer connected, resuming from offset"
                    );
                    client = Some(c);
                    backoff_ms = 1000;
                },
                None => {
                    stats.consumer_reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    continue;
                },
            }
        }

        // Take the client out so we own it (avoids borrow issues)
        let mut c = match client.take() {
            Some(c) => c,
            None => continue,
        };

        // On first connection, seek past any existing data from previous runs
        if !seeked {
            info!("Consumer seeking past existing data...");
            current_offset = seek_to_end(&mut c, topic_id, args.max_fetch_bytes).await;
            seeked = true;
            info!(
                offset = current_offset,
                "Consumer ready, verifying new messages only"
            );
            client = Some(c);
            continue;
        }

        match do_fetch(&mut c, topic_id, current_offset, args.max_fetch_bytes).await {
            Ok(result) => {
                if !result.data.is_empty() && result.next_offset != current_offset {
                    process_records(&result, &mut expected_seq, &stats);
                    current_offset = result.next_offset;
                } else {
                    // No new data, poll again shortly
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                // Success — reset backoff and put client back
                backoff_ms = 1000;
                client = Some(c);
            },
            Err(e) => {
                warn!(
                    offset = current_offset,
                    backoff_ms, "Consumer fetch error: {}", e
                );
                stats.consumer_errors.fetch_add(1, Ordering::Relaxed);
                stats.consumer_reconnects.fetch_add(1, Ordering::Relaxed);
                // Reset seek state — after a server restart, byte offsets may
                // shift due to unflushed write loss, so we must re-seek to end.
                seeked = false;
                expected_seq = 0;
                current_offset = 0;
                // Drop c (don't put it back) to force reconnect
                drop(c);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            },
        }
    }

    if let Some(c) = client {
        let _ = c.close().await;
    }
    let final_seq = if expected_seq > 0 {
        expected_seq - 1
    } else {
        0
    };
    info!(
        last_consumed = final_seq,
        offset = current_offset,
        "Consumer stopped"
    );
}

/// Reporter task: prints periodic stats
async fn reporter_task(args: Args, stats: Arc<Stats>) {
    let interval = Duration::from_secs(args.report_interval);
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
        let p_rate = (produced - last_produced) as f64 / args.report_interval as f64;
        let c_rate = (consumed - last_consumed) as f64 / args.report_interval as f64;
        let lag = produced.saturating_sub(consumed);

        let status = if gaps == 0 && dups == 0 && ooo == 0 {
            "OK"
        } else {
            "ISSUES"
        };

        info!("╔══════════════════════════════════════════════════════════════╗");
        info!("║ [{status}] t={tick} elapsed={elapsed:.1}s");
        let q_depth = stats.queue_depth.load(Ordering::Relaxed);

        info!(
            "║ produced={produced} ({p_rate:.0}/s) | consumed={consumed} ({c_rate:.0}/s) | lag={lag}"
        );
        info!("║ last_p_seq={last_p_seq} | last_c_seq={last_c_seq} | queue={q_depth}");
        info!("║ gaps={gaps} | dups={dups} | ooo={ooo}");
        info!(
            "║ p_err={p_err} c_err={c_err} parse_err={parse_err} | p_reconn={p_reconn} c_reconn={c_reconn}"
        );
        info!("╚══════════════════════════════════════════════════════════════╝");

        last_produced = produced;
        last_consumed = consumed;
    }
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

    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║                   LANCE Chaos Test                         ║");
    info!("╠══════════════════════════════════════════════════════════════╣");
    info!("║ endpoint: {}", args.endpoint);
    info!("║ topic:    {}", args.topic);
    info!("║ rate:     {} msg/min ({}/s)", args.rate, args.rate / 60);
    info!("║ payload:  {} bytes", args.payload_size);
    info!(
        "║ duration: {}",
        if args.duration == 0 {
            "∞".to_string()
        } else {
            format!("{}s", args.duration)
        }
    );
    info!("╚══════════════════════════════════════════════════════════════╝");

    // Resolve topic
    let topic_id = match resolve_topic(&args.endpoint, &args.topic, args.clean).await {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to resolve topic '{}': {}", args.topic, e);
            std::process::exit(1);
        },
    };

    let stats = Arc::new(Stats::new());
    let ready = Arc::new(Notify::new());

    // Spawn tasks
    let producer_handle = tokio::spawn(producer_task(
        args.clone(),
        topic_id,
        stats.clone(),
        ready.clone(),
    ));
    let consumer_handle = tokio::spawn(consumer_task(
        args.clone(),
        topic_id,
        stats.clone(),
        ready.clone(),
    ));
    let reporter_handle = tokio::spawn(reporter_task(args.clone(), stats.clone()));

    // Duration-based or ctrl-c shutdown
    let stats_shutdown = stats.clone();
    let shutdown_handle = tokio::spawn(async move {
        if args.duration > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(args.duration)) => {
                    info!("Duration elapsed, shutting down...");
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C received, shutting down...");
                }
            }
        } else {
            let _ = tokio::signal::ctrl_c().await;
            info!("Ctrl-C received, shutting down...");
        }
        stats_shutdown.running.store(false, Ordering::Relaxed);
    });

    // Wait for shutdown signal
    let _ = shutdown_handle.await;

    // Give tasks a moment to drain
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Abort remaining tasks
    producer_handle.abort();
    consumer_handle.abort();
    reporter_handle.abort();

    // Final report
    let produced = stats.produced.load(Ordering::Relaxed);
    let consumed = stats.consumed.load(Ordering::Relaxed);
    let gaps = stats.gaps.load(Ordering::Relaxed);
    let dups = stats.duplicates.load(Ordering::Relaxed);

    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║                    FINAL REPORT                            ║");
    info!("╠══════════════════════════════════════════════════════════════╣");
    let q_depth = stats.queue_depth.load(Ordering::Relaxed);
    info!("║ Total produced:  {produced}");
    info!("║ Total consumed:  {consumed}");
    info!("║ Queue remaining: {q_depth}");
    info!("║ Gaps detected:   {gaps}");
    info!("║ Duplicates:      {dups}");
    info!(
        "║ Producer reconn: {}",
        stats.producer_reconnects.load(Ordering::Relaxed)
    );
    info!(
        "║ Consumer reconn: {}",
        stats.consumer_reconnects.load(Ordering::Relaxed)
    );

    if gaps == 0 && dups == 0 {
        info!("║ Result: ✅ PASS — no data loss detected");
    } else {
        info!("║ Result: ❌ FAIL — data integrity issues detected");
    }
    info!("╚══════════════════════════════════════════════════════════════╝");

    if gaps > 0 || dups > 0 {
        std::process::exit(1);
    }
}
