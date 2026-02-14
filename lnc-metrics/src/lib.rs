#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

pub mod golden_signals;
pub mod otlp;
mod tracing_export;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

pub use golden_signals::{
    ERRORS_BY_TYPE,
    ERRORS_TOTAL,
    // Errors
    ErrorType,
    // Snapshot
    GoldenSignalsSnapshot,
    LATENCY_FETCH,
    LATENCY_INGEST,
    LATENCY_IO,
    LATENCY_NETWORK,
    LATENCY_REPLICATION,
    LATENCY_SAMPLE_RATE,
    // Latency
    LatencyHistogram,
    LatencySnapshot,
    LatencyTimer,
    RATE_INGEST_BYTES,
    RATE_INGEST_OPS,
    RATE_READ_BYTES,
    RATE_READ_OPS,
    // Traffic
    RateTracker,
    SATURATION_BUFFER_POOL_TOTAL,
    SATURATION_BUFFER_POOL_USED,
    SATURATION_CONNECTIONS_MAX,
    SATURATION_CONNECTIONS_USED,
    SATURATION_MEMORY_TOTAL,
    SATURATION_MEMORY_USED,
    SATURATION_PENDING_IO,
    SATURATION_QUEUE_CAPACITY,
    SATURATION_QUEUE_DEPTH,
    // Latency - SAMPLED (use these on hot paths)
    SampledTimer,
    buffer_pool_saturation,
    get_error_count,
    get_total_errors,
    memory_saturation,
    queue_saturation,
    record_error,
    record_fetch_latency,
    record_fetch_latency_sampled,
    record_ingest_latency,
    record_ingest_latency_sampled,
    record_io_latency,
    record_io_latency_sampled,
    record_network_latency,
    record_peer_replication_latency,
    record_replication_latency,
    saturation_ratio,
    set_buffer_pool_usage,
    set_connection_usage,
    set_latency_sample_rate,
    set_memory_usage,
    set_pending_io,
    // Saturation
    set_queue_depth,
    should_sample,
    time_fetch,
    time_fetch_sampled,
    time_ingest,
    time_ingest_sampled,
    time_io,
    time_io_sampled,
    update_rates,
};
pub use tracing_export::{LocalSpan, OperationType, SpanContext, TracingConfig, init_tracing};

pub static RECORDS_INGESTED: AtomicU64 = AtomicU64::new(0);
pub static BYTES_INGESTED: AtomicU64 = AtomicU64::new(0);
pub static BATCHES_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static IO_OPS_SUBMITTED: AtomicU64 = AtomicU64::new(0);
pub static IO_OPS_COMPLETED: AtomicU64 = AtomicU64::new(0);
pub static CONNECTIONS_ACTIVE: AtomicU64 = AtomicU64::new(0);
pub static CONNECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static CRC_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static BACKPRESSURE_EVENTS: AtomicU64 = AtomicU64::new(0);
pub static POOL_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static NUMA_MISALIGNED: AtomicU64 = AtomicU64::new(0);
pub static FOLLOWER_EVICTIONS: AtomicU64 = AtomicU64::new(0);
pub static FOLLOWER_RECOVERIES: AtomicU64 = AtomicU64::new(0);
pub static QUORUM_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static QUORUM_TIMEOUTS: AtomicU64 = AtomicU64::new(0);

// HLC (Hybrid Logical Clock) metrics
pub static HLC_DRIFT_MS: AtomicU64 = AtomicU64::new(0);
pub static HLC_DRIFT_WARNINGS: AtomicU64 = AtomicU64::new(0);
pub static HLC_DRIFT_CRITICAL: AtomicU64 = AtomicU64::new(0);
pub static HLC_LOGICAL_EXHAUSTED: AtomicU64 = AtomicU64::new(0);

// Raft consensus metrics
pub static RAFT_ELECTIONS_STARTED: AtomicU64 = AtomicU64::new(0);
pub static RAFT_ELECTIONS_WON: AtomicU64 = AtomicU64::new(0);
pub static RAFT_PRE_VOTES_REJECTED: AtomicU64 = AtomicU64::new(0);
pub static RAFT_LEADER_STEPDOWNS: AtomicU64 = AtomicU64::new(0);
pub static RAFT_FENCING_REJECTIONS: AtomicU64 = AtomicU64::new(0);

// Consumer read path metrics (per Architecture §17.7)
pub static READS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static READ_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static CONSUMER_THROTTLED: AtomicU64 = AtomicU64::new(0);
pub static ZERO_COPY_SENDS: AtomicU64 = AtomicU64::new(0);

// Cluster health metrics (for production observability)
pub static CLUSTER_LEADER_ID: AtomicU64 = AtomicU64::new(0);
pub static CLUSTER_CURRENT_TERM: AtomicU64 = AtomicU64::new(0);
pub static CLUSTER_NODE_COUNT: AtomicU64 = AtomicU64::new(0);
pub static CLUSTER_HEALTHY_NODES: AtomicU64 = AtomicU64::new(0);
pub static CLUSTER_IS_LEADER: AtomicU64 = AtomicU64::new(0);
pub static CLUSTER_QUORUM_AVAILABLE: AtomicU64 = AtomicU64::new(0);

// Replication lag metrics (bytes behind leader, time since last sync)
pub static REPLICATION_LAG_BYTES: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_LAST_SYNC_MS: AtomicU64 = AtomicU64::new(0);
pub static REPLICATION_PENDING_OPS: AtomicU64 = AtomicU64::new(0);

// Resync protocol metrics (§18.8 Follower Resync)
pub static RESYNC_STARTED: AtomicU64 = AtomicU64::new(0);
pub static RESYNC_COMPLETED: AtomicU64 = AtomicU64::new(0);
pub static RESYNC_FAILED: AtomicU64 = AtomicU64::new(0);
pub static RESYNC_SEGMENTS_TRANSFERRED: AtomicU64 = AtomicU64::new(0);
pub static RESYNC_BYTES_TRANSFERRED: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn increment_records_ingested(count: u64) {
    RECORDS_INGESTED.fetch_add(count, Ordering::Relaxed);
}

#[inline]
pub fn increment_bytes_ingested(bytes: u64) {
    BYTES_INGESTED.fetch_add(bytes, Ordering::Relaxed);
}

#[inline]
pub fn increment_batches_written() {
    BATCHES_WRITTEN.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_bytes_written(bytes: u64) {
    BYTES_WRITTEN.fetch_add(bytes, Ordering::Relaxed);
}

#[inline]
pub fn increment_io_submitted() {
    IO_OPS_SUBMITTED.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_io_completed() {
    IO_OPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_connections() {
    CONNECTIONS_ACTIVE.fetch_add(1, Ordering::Relaxed);
    CONNECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn decrement_connections() {
    CONNECTIONS_ACTIVE.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_crc_failures() {
    CRC_FAILURES.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_backpressure() {
    BACKPRESSURE_EVENTS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_pool_exhausted() {
    POOL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_numa_misaligned() {
    NUMA_MISALIGNED.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_follower_evictions() {
    FOLLOWER_EVICTIONS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_follower_recoveries() {
    FOLLOWER_RECOVERIES.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_quorum_failures() {
    QUORUM_FAILURES.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn increment_quorum_timeouts() {
    QUORUM_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
}

// HLC metrics functions

/// Update the current HLC drift in milliseconds.
#[inline]
pub fn set_hlc_drift_ms(drift_ms: u64) {
    HLC_DRIFT_MS.store(drift_ms, Ordering::Relaxed);
}

/// Increment HLC drift warning counter (drift > 100ms).
#[inline]
pub fn increment_hlc_drift_warnings() {
    HLC_DRIFT_WARNINGS.fetch_add(1, Ordering::Relaxed);
}

/// Increment HLC drift critical counter (drift > 1s).
#[inline]
pub fn increment_hlc_drift_critical() {
    HLC_DRIFT_CRITICAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment HLC logical counter exhausted events.
#[inline]
pub fn increment_hlc_logical_exhausted() {
    HLC_LOGICAL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
}

// Raft metrics functions

/// Increment Raft elections started counter.
#[inline]
pub fn increment_raft_elections_started() {
    RAFT_ELECTIONS_STARTED.fetch_add(1, Ordering::Relaxed);
}

/// Increment Raft elections won counter.
#[inline]
pub fn increment_raft_elections_won() {
    RAFT_ELECTIONS_WON.fetch_add(1, Ordering::Relaxed);
}

/// Increment Raft pre-votes rejected counter.
#[inline]
pub fn increment_raft_pre_votes_rejected() {
    RAFT_PRE_VOTES_REJECTED.fetch_add(1, Ordering::Relaxed);
}

/// Increment Raft leader stepdown counter.
#[inline]
pub fn increment_raft_leader_stepdowns() {
    RAFT_LEADER_STEPDOWNS.fetch_add(1, Ordering::Relaxed);
}

/// Increment Raft fencing rejection counter.
#[inline]
pub fn increment_raft_fencing_rejections() {
    RAFT_FENCING_REJECTIONS.fetch_add(1, Ordering::Relaxed);
}

// Consumer read path functions

/// Increment total read operations counter.
#[inline]
pub fn increment_reads() {
    READS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment total bytes read counter.
#[inline]
pub fn increment_read_bytes(bytes: u64) {
    READ_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
}

/// Increment consumer throttled events counter.
#[inline]
pub fn increment_consumer_throttled() {
    CONSUMER_THROTTLED.fetch_add(1, Ordering::Relaxed);
}

/// Increment zero-copy sends counter (per Architecture §17.7)
#[inline]
pub fn increment_zero_copy_sends() {
    ZERO_COPY_SENDS.fetch_add(1, Ordering::Relaxed);
}

// Cluster health metric functions

/// Set the current cluster leader ID.
#[inline]
pub fn set_cluster_leader_id(leader_id: u16) {
    CLUSTER_LEADER_ID.store(leader_id as u64, Ordering::Relaxed);
}

/// Set the current Raft term.
#[inline]
pub fn set_cluster_current_term(term: u64) {
    CLUSTER_CURRENT_TERM.store(term, Ordering::Relaxed);
}

/// Set the total number of nodes in the cluster.
#[inline]
pub fn set_cluster_node_count(count: usize) {
    CLUSTER_NODE_COUNT.store(count as u64, Ordering::Relaxed);
}

/// Set the number of healthy (connected) nodes.
#[inline]
pub fn set_cluster_healthy_nodes(count: usize) {
    CLUSTER_HEALTHY_NODES.store(count as u64, Ordering::Relaxed);
}

/// Set whether this node is the leader (1 = leader, 0 = follower).
#[inline]
pub fn set_cluster_is_leader(is_leader: bool) {
    CLUSTER_IS_LEADER.store(if is_leader { 1 } else { 0 }, Ordering::Relaxed);
}

/// Set whether quorum is available (1 = yes, 0 = no).
#[inline]
pub fn set_cluster_quorum_available(available: bool) {
    CLUSTER_QUORUM_AVAILABLE.store(if available { 1 } else { 0 }, Ordering::Relaxed);
}

// Replication lag metric functions

/// Set the replication lag in bytes (how far behind the leader).
#[inline]
pub fn set_replication_lag_bytes(bytes: u64) {
    REPLICATION_LAG_BYTES.store(bytes, Ordering::Relaxed);
}

/// Set the time since last successful sync in milliseconds.
#[inline]
pub fn set_replication_last_sync_ms(ms: u64) {
    REPLICATION_LAST_SYNC_MS.store(ms, Ordering::Relaxed);
}

/// Set the number of pending replication operations.
#[inline]
pub fn set_replication_pending_ops(count: u64) {
    REPLICATION_PENDING_OPS.store(count, Ordering::Relaxed);
}

/// Increment pending replication operations.
#[inline]
pub fn increment_replication_pending_ops() {
    REPLICATION_PENDING_OPS.fetch_add(1, Ordering::Relaxed);
}

/// Decrement pending replication operations.
#[inline]
pub fn decrement_replication_pending_ops() {
    REPLICATION_PENDING_OPS.fetch_sub(1, Ordering::Relaxed);
}

// Resync protocol metric functions (§18.8)

/// Increment resync sessions started counter.
#[inline]
pub fn increment_resync_started() {
    RESYNC_STARTED.fetch_add(1, Ordering::Relaxed);
}

/// Increment resync sessions completed successfully.
#[inline]
pub fn increment_resync_completed() {
    RESYNC_COMPLETED.fetch_add(1, Ordering::Relaxed);
}

/// Increment resync sessions that failed.
#[inline]
pub fn increment_resync_failed() {
    RESYNC_FAILED.fetch_add(1, Ordering::Relaxed);
}

/// Increment segments transferred during resync.
#[inline]
pub fn increment_resync_segments_transferred(count: u64) {
    RESYNC_SEGMENTS_TRANSFERRED.fetch_add(count, Ordering::Relaxed);
}

/// Increment bytes transferred during resync.
#[inline]
pub fn increment_resync_bytes_transferred(bytes: u64) {
    RESYNC_BYTES_TRANSFERRED.fetch_add(bytes, Ordering::Relaxed);
}

pub struct MetricsSnapshot {
    pub records_ingested: u64,
    pub bytes_ingested: u64,
    pub batches_written: u64,
    pub bytes_written: u64,
    pub io_ops_submitted: u64,
    pub io_ops_completed: u64,
    pub connections_active: u64,
    pub connections_total: u64,
    pub crc_failures: u64,
    pub backpressure_events: u64,
    pub pool_exhausted: u64,
    pub numa_misaligned: u64,
    pub follower_evictions: u64,
    pub follower_recoveries: u64,
    pub quorum_failures: u64,
    // HLC metrics
    pub hlc_drift_ms: u64,
    pub hlc_drift_warnings: u64,
    pub hlc_drift_critical: u64,
    pub hlc_logical_exhausted: u64,
    // Raft metrics
    pub raft_elections_started: u64,
    pub raft_elections_won: u64,
    pub raft_pre_votes_rejected: u64,
    pub raft_leader_stepdowns: u64,
    pub raft_fencing_rejections: u64,
    // Consumer read path metrics
    pub reads_total: u64,
    pub read_bytes_total: u64,
    pub consumer_throttled: u64,
    pub zero_copy_sends: u64,
    // Cluster health metrics
    pub cluster_leader_id: u64,
    pub cluster_current_term: u64,
    pub cluster_node_count: u64,
    pub cluster_healthy_nodes: u64,
    pub cluster_is_leader: u64,
    pub cluster_quorum_available: u64,
    // Replication lag metrics
    pub replication_lag_bytes: u64,
    pub replication_last_sync_ms: u64,
    pub replication_pending_ops: u64,
    // Resync protocol metrics
    pub resync_started: u64,
    pub resync_completed: u64,
    pub resync_failed: u64,
    pub resync_segments_transferred: u64,
    pub resync_bytes_transferred: u64,
}

impl MetricsSnapshot {
    #[must_use]
    pub fn capture() -> Self {
        Self {
            records_ingested: RECORDS_INGESTED.load(Ordering::Relaxed),
            bytes_ingested: BYTES_INGESTED.load(Ordering::Relaxed),
            batches_written: BATCHES_WRITTEN.load(Ordering::Relaxed),
            bytes_written: BYTES_WRITTEN.load(Ordering::Relaxed),
            io_ops_submitted: IO_OPS_SUBMITTED.load(Ordering::Relaxed),
            io_ops_completed: IO_OPS_COMPLETED.load(Ordering::Relaxed),
            connections_active: CONNECTIONS_ACTIVE.load(Ordering::Relaxed),
            connections_total: CONNECTIONS_TOTAL.load(Ordering::Relaxed),
            crc_failures: CRC_FAILURES.load(Ordering::Relaxed),
            backpressure_events: BACKPRESSURE_EVENTS.load(Ordering::Relaxed),
            pool_exhausted: POOL_EXHAUSTED.load(Ordering::Relaxed),
            numa_misaligned: NUMA_MISALIGNED.load(Ordering::Relaxed),
            follower_evictions: FOLLOWER_EVICTIONS.load(Ordering::Relaxed),
            follower_recoveries: FOLLOWER_RECOVERIES.load(Ordering::Relaxed),
            quorum_failures: QUORUM_FAILURES.load(Ordering::Relaxed),
            // HLC metrics
            hlc_drift_ms: HLC_DRIFT_MS.load(Ordering::Relaxed),
            hlc_drift_warnings: HLC_DRIFT_WARNINGS.load(Ordering::Relaxed),
            hlc_drift_critical: HLC_DRIFT_CRITICAL.load(Ordering::Relaxed),
            hlc_logical_exhausted: HLC_LOGICAL_EXHAUSTED.load(Ordering::Relaxed),
            // Raft metrics
            raft_elections_started: RAFT_ELECTIONS_STARTED.load(Ordering::Relaxed),
            raft_elections_won: RAFT_ELECTIONS_WON.load(Ordering::Relaxed),
            raft_pre_votes_rejected: RAFT_PRE_VOTES_REJECTED.load(Ordering::Relaxed),
            raft_leader_stepdowns: RAFT_LEADER_STEPDOWNS.load(Ordering::Relaxed),
            raft_fencing_rejections: RAFT_FENCING_REJECTIONS.load(Ordering::Relaxed),
            // Consumer read path metrics
            reads_total: READS_TOTAL.load(Ordering::Relaxed),
            read_bytes_total: READ_BYTES_TOTAL.load(Ordering::Relaxed),
            consumer_throttled: CONSUMER_THROTTLED.load(Ordering::Relaxed),
            zero_copy_sends: ZERO_COPY_SENDS.load(Ordering::Relaxed),
            // Cluster health metrics
            cluster_leader_id: CLUSTER_LEADER_ID.load(Ordering::Relaxed),
            cluster_current_term: CLUSTER_CURRENT_TERM.load(Ordering::Relaxed),
            cluster_node_count: CLUSTER_NODE_COUNT.load(Ordering::Relaxed),
            cluster_healthy_nodes: CLUSTER_HEALTHY_NODES.load(Ordering::Relaxed),
            cluster_is_leader: CLUSTER_IS_LEADER.load(Ordering::Relaxed),
            cluster_quorum_available: CLUSTER_QUORUM_AVAILABLE.load(Ordering::Relaxed),
            // Replication lag metrics
            replication_lag_bytes: REPLICATION_LAG_BYTES.load(Ordering::Relaxed),
            replication_last_sync_ms: REPLICATION_LAST_SYNC_MS.load(Ordering::Relaxed),
            replication_pending_ops: REPLICATION_PENDING_OPS.load(Ordering::Relaxed),
            // Resync protocol metrics
            resync_started: RESYNC_STARTED.load(Ordering::Relaxed),
            resync_completed: RESYNC_COMPLETED.load(Ordering::Relaxed),
            resync_failed: RESYNC_FAILED.load(Ordering::Relaxed),
            resync_segments_transferred: RESYNC_SEGMENTS_TRANSFERRED.load(Ordering::Relaxed),
            resync_bytes_transferred: RESYNC_BYTES_TRANSFERRED.load(Ordering::Relaxed),
        }
    }
}

pub fn init_prometheus_exporter(
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder.with_http_listener(addr).install()?;

    metrics::describe_counter!(
        "lance_records_ingested_total",
        "Total number of records ingested"
    );
    metrics::describe_counter!("lance_bytes_ingested_total", "Total bytes ingested");
    metrics::describe_counter!(
        "lance_batches_written_total",
        "Total batches written to disk"
    );
    metrics::describe_counter!("lance_bytes_written_total", "Total bytes written to disk");
    metrics::describe_counter!(
        "lance_io_ops_submitted_total",
        "Total I/O operations submitted"
    );
    metrics::describe_counter!(
        "lance_io_ops_completed_total",
        "Total I/O operations completed"
    );
    metrics::describe_gauge!(
        "lance_connections_active",
        "Current number of active connections"
    );
    metrics::describe_counter!(
        "lance_connections_total",
        "Total connections ever established"
    );
    metrics::describe_counter!("lance_crc_failures_total", "Total CRC validation failures");
    metrics::describe_counter!(
        "lance_backpressure_events_total",
        "Total backpressure events"
    );
    metrics::describe_counter!(
        "lance_pool_exhausted_total",
        "Buffer pool exhaustion events"
    );
    metrics::describe_counter!("lance_numa_misaligned_total", "NUMA misalignment warnings");
    metrics::describe_counter!(
        "lance_follower_evictions_total",
        "Followers evicted from quorum"
    );
    metrics::describe_counter!(
        "lance_follower_recoveries_total",
        "Followers recovered to quorum"
    );
    metrics::describe_counter!("lance_quorum_failures_total", "Quorum not reached events");

    // HLC metrics
    metrics::describe_gauge!(
        "lance_hlc_drift_ms",
        "Current HLC drift from wall clock in milliseconds"
    );
    metrics::describe_counter!(
        "lance_hlc_drift_warnings_total",
        "HLC drift warning events (drift > 100ms)"
    );
    metrics::describe_counter!(
        "lance_hlc_drift_critical_total",
        "HLC drift critical events (drift > 1s)"
    );
    metrics::describe_counter!(
        "lance_hlc_logical_exhausted_total",
        "HLC logical counter exhaustion events"
    );

    // Raft consensus metrics
    metrics::describe_counter!(
        "lance_raft_elections_started_total",
        "Raft elections started"
    );
    metrics::describe_counter!(
        "lance_raft_elections_won_total",
        "Raft elections won (became leader)"
    );
    metrics::describe_counter!(
        "lance_raft_pre_votes_rejected_total",
        "Raft pre-votes rejected"
    );
    metrics::describe_counter!("lance_raft_leader_stepdowns_total", "Raft leader stepdowns");
    metrics::describe_counter!(
        "lance_raft_fencing_rejections_total",
        "Raft fencing token rejections"
    );

    // Consumer read path metrics
    metrics::describe_counter!("lance_reads_total", "Total read operations");
    metrics::describe_counter!("lance_read_bytes_total", "Total bytes read");
    metrics::describe_counter!(
        "lance_consumer_throttled_total",
        "Consumer rate limit events"
    );
    metrics::describe_counter!("lance_zero_copy_sends_total", "Zero-copy sends completed");

    // Cluster health metrics
    metrics::describe_gauge!("lance_cluster_leader_id", "Current cluster leader node ID");
    metrics::describe_gauge!("lance_cluster_current_term", "Current Raft term");
    metrics::describe_gauge!("lance_cluster_node_count", "Total nodes in cluster");
    metrics::describe_gauge!(
        "lance_cluster_healthy_nodes",
        "Number of healthy (connected) nodes"
    );
    metrics::describe_gauge!(
        "lance_cluster_is_leader",
        "Whether this node is the leader (1=yes, 0=no)"
    );
    metrics::describe_gauge!(
        "lance_cluster_quorum_available",
        "Whether quorum is available (1=yes, 0=no)"
    );

    // Replication lag metrics
    metrics::describe_gauge!(
        "lance_replication_lag_bytes",
        "Replication lag in bytes behind leader"
    );
    metrics::describe_gauge!(
        "lance_replication_last_sync_ms",
        "Time since last successful replication sync in milliseconds"
    );
    metrics::describe_gauge!(
        "lance_replication_pending_ops",
        "Number of pending replication operations"
    );

    // ==========================================================================
    // 4 GOLDEN SIGNALS METRICS
    // ==========================================================================

    // Latency histograms (Prometheus histogram buckets)
    metrics::describe_histogram!(
        "lance_ingest_latency_seconds",
        metrics::Unit::Seconds,
        "Ingest request latency"
    );
    metrics::describe_histogram!(
        "lance_fetch_latency_seconds",
        metrics::Unit::Seconds,
        "Fetch request latency"
    );
    metrics::describe_histogram!(
        "lance_io_latency_seconds",
        metrics::Unit::Seconds,
        "I/O operation latency"
    );
    metrics::describe_histogram!(
        "lance_replication_latency_seconds",
        metrics::Unit::Seconds,
        "Replication latency"
    );
    metrics::describe_histogram!(
        "lance_network_latency_seconds",
        metrics::Unit::Seconds,
        "Network round-trip latency"
    );

    // Traffic rates
    metrics::describe_gauge!(
        "lance_ingest_ops_per_second",
        "Ingest operations per second"
    );
    metrics::describe_gauge!(
        "lance_ingest_bytes_per_second",
        "Ingest throughput in bytes per second"
    );
    metrics::describe_gauge!("lance_read_ops_per_second", "Read operations per second");
    metrics::describe_gauge!(
        "lance_read_bytes_per_second",
        "Read throughput in bytes per second"
    );

    // Errors by type
    metrics::describe_counter!("lance_errors_total", "Total errors by type");

    // Saturation gauges
    metrics::describe_gauge!(
        "lance_queue_saturation_ratio",
        "Queue depth saturation (0.0-1.0)"
    );
    metrics::describe_gauge!(
        "lance_memory_saturation_ratio",
        "Memory saturation (0.0-1.0)"
    );
    metrics::describe_gauge!(
        "lance_buffer_pool_saturation_ratio",
        "Buffer pool saturation (0.0-1.0)"
    );
    metrics::describe_gauge!(
        "lance_connection_saturation_ratio",
        "Connection pool saturation (0.0-1.0)"
    );
    metrics::describe_gauge!("lance_pending_io_count", "Number of pending I/O operations");

    Ok(())
}

pub fn export_to_prometheus() {
    let snapshot = MetricsSnapshot::capture();

    metrics::counter!("lance_records_ingested_total").absolute(snapshot.records_ingested);
    metrics::counter!("lance_bytes_ingested_total").absolute(snapshot.bytes_ingested);
    metrics::counter!("lance_batches_written_total").absolute(snapshot.batches_written);
    metrics::counter!("lance_bytes_written_total").absolute(snapshot.bytes_written);
    metrics::counter!("lance_io_ops_submitted_total").absolute(snapshot.io_ops_submitted);
    metrics::counter!("lance_io_ops_completed_total").absolute(snapshot.io_ops_completed);
    metrics::gauge!("lance_connections_active").set(snapshot.connections_active as f64);
    metrics::counter!("lance_connections_total").absolute(snapshot.connections_total);
    metrics::counter!("lance_crc_failures_total").absolute(snapshot.crc_failures);
    metrics::counter!("lance_backpressure_events_total").absolute(snapshot.backpressure_events);
    metrics::counter!("lance_pool_exhausted_total").absolute(snapshot.pool_exhausted);
    metrics::counter!("lance_numa_misaligned_total").absolute(snapshot.numa_misaligned);
    metrics::counter!("lance_follower_evictions_total").absolute(snapshot.follower_evictions);
    metrics::counter!("lance_follower_recoveries_total").absolute(snapshot.follower_recoveries);
    metrics::counter!("lance_quorum_failures_total").absolute(snapshot.quorum_failures);

    // HLC metrics
    metrics::gauge!("lance_hlc_drift_ms").set(snapshot.hlc_drift_ms as f64);
    metrics::counter!("lance_hlc_drift_warnings_total").absolute(snapshot.hlc_drift_warnings);
    metrics::counter!("lance_hlc_drift_critical_total").absolute(snapshot.hlc_drift_critical);
    metrics::counter!("lance_hlc_logical_exhausted_total").absolute(snapshot.hlc_logical_exhausted);

    // Raft metrics
    metrics::counter!("lance_raft_elections_started_total")
        .absolute(snapshot.raft_elections_started);
    metrics::counter!("lance_raft_elections_won_total").absolute(snapshot.raft_elections_won);
    metrics::counter!("lance_raft_pre_votes_rejected_total")
        .absolute(snapshot.raft_pre_votes_rejected);
    metrics::counter!("lance_raft_leader_stepdowns_total").absolute(snapshot.raft_leader_stepdowns);
    metrics::counter!("lance_raft_fencing_rejections_total")
        .absolute(snapshot.raft_fencing_rejections);

    // Consumer read path metrics
    metrics::counter!("lance_reads_total").absolute(snapshot.reads_total);
    metrics::counter!("lance_read_bytes_total").absolute(snapshot.read_bytes_total);
    metrics::counter!("lance_consumer_throttled_total").absolute(snapshot.consumer_throttled);
    metrics::counter!("lance_zero_copy_sends_total").absolute(snapshot.zero_copy_sends);

    // Cluster health metrics
    metrics::gauge!("lance_cluster_leader_id").set(snapshot.cluster_leader_id as f64);
    metrics::gauge!("lance_cluster_current_term").set(snapshot.cluster_current_term as f64);
    metrics::gauge!("lance_cluster_node_count").set(snapshot.cluster_node_count as f64);
    metrics::gauge!("lance_cluster_healthy_nodes").set(snapshot.cluster_healthy_nodes as f64);
    metrics::gauge!("lance_cluster_is_leader").set(snapshot.cluster_is_leader as f64);
    metrics::gauge!("lance_cluster_quorum_available").set(snapshot.cluster_quorum_available as f64);

    // Replication lag metrics
    metrics::gauge!("lance_replication_lag_bytes").set(snapshot.replication_lag_bytes as f64);
    metrics::gauge!("lance_replication_last_sync_ms").set(snapshot.replication_last_sync_ms as f64);
    metrics::gauge!("lance_replication_pending_ops").set(snapshot.replication_pending_ops as f64);

    // Resync protocol metrics
    metrics::counter!("lance_resync_started_total").absolute(snapshot.resync_started);
    metrics::counter!("lance_resync_completed_total").absolute(snapshot.resync_completed);
    metrics::counter!("lance_resync_failed_total").absolute(snapshot.resync_failed);
    metrics::counter!("lance_resync_segments_transferred_total")
        .absolute(snapshot.resync_segments_transferred);
    metrics::counter!("lance_resync_bytes_transferred_total")
        .absolute(snapshot.resync_bytes_transferred);

    // ==========================================================================
    // 4 GOLDEN SIGNALS EXPORT
    // ==========================================================================

    let gs = GoldenSignalsSnapshot::capture();

    // Latency - export p50, p95, p99, p999 as gauges (histograms require different approach)
    export_latency_percentiles("lance_ingest_latency", &gs.latency_ingest);
    export_latency_percentiles("lance_fetch_latency", &gs.latency_fetch);
    export_latency_percentiles("lance_io_latency", &gs.latency_io);
    export_latency_percentiles("lance_replication_latency", &gs.latency_replication);
    export_latency_percentiles("lance_network_latency", &gs.latency_network);

    // Traffic rates
    metrics::gauge!("lance_ingest_ops_per_second").set(gs.traffic_ingest_ops_per_sec);
    metrics::gauge!("lance_ingest_bytes_per_second").set(gs.traffic_ingest_bytes_per_sec);
    metrics::gauge!("lance_read_ops_per_second").set(gs.traffic_read_ops_per_sec);
    metrics::gauge!("lance_read_bytes_per_second").set(gs.traffic_read_bytes_per_sec);

    // Errors by type
    for (i, &count) in gs.errors_by_type.iter().enumerate() {
        let error_type = match i {
            0 => "protocol",
            1 => "io",
            2 => "timeout",
            3 => "checksum",
            4 => "replication",
            5 => "backpressure",
            6 => "auth",
            7 => "internal",
            _ => "unknown",
        };
        metrics::counter!("lance_errors_total", "type" => error_type).absolute(count);
    }

    // Saturation
    metrics::gauge!("lance_queue_saturation_ratio").set(gs.saturation_queue);
    metrics::gauge!("lance_memory_saturation_ratio").set(gs.saturation_memory);
    metrics::gauge!("lance_buffer_pool_saturation_ratio").set(gs.saturation_buffer_pool);
    metrics::gauge!("lance_connection_saturation_ratio").set(gs.saturation_connections);
    metrics::gauge!("lance_pending_io_count").set(gs.saturation_pending_io as f64);
}

fn export_latency_percentiles(prefix: &str, snap: &LatencySnapshot) {
    let p50_sec = snap.p50() as f64 / 1_000_000.0;
    let p95_sec = snap.p95() as f64 / 1_000_000.0;
    let p99_sec = snap.p99() as f64 / 1_000_000.0;
    let p999_sec = snap.p999() as f64 / 1_000_000.0;
    let avg_sec = snap.avg_us() / 1_000_000.0;

    metrics::gauge!(format!("{}_p50_seconds", prefix)).set(p50_sec);
    metrics::gauge!(format!("{}_p95_seconds", prefix)).set(p95_sec);
    metrics::gauge!(format!("{}_p99_seconds", prefix)).set(p99_sec);
    metrics::gauge!(format!("{}_p999_seconds", prefix)).set(p999_sec);
    metrics::gauge!(format!("{}_avg_seconds", prefix)).set(avg_sec);
    metrics::gauge!(format!("{}_count", prefix)).set(snap.count as f64);
}
