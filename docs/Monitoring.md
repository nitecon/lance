[← Back to Docs Index](./README.md)

# Monitoring and Observability

## Table of Contents

- [Overview](#overview)
- [Configuration](#configuration)
- [The 4 Golden Signals](#the-4-golden-signals)
- [Performance Impact](#performance-impact)
- [Prometheus Integration](#prometheus-integration)
- [Grafana Dashboards](#grafana-dashboards)
- [Alerting Best Practices](#alerting-best-practices)
- [Summary](#summary)

LANCE implements comprehensive monitoring based on Google SRE's **4 Golden Signals** framework, providing deep visibility into system health while maintaining strict performance requirements.

## Overview

The observability stack is built on three pillars:

1. **Metrics** - Atomic counters and gauges exported to Prometheus
2. **Tracing** - Distributed tracing via OpenTelemetry
3. **Logging** - Structured logging with tracing integration

All telemetry collection is designed to be **non-blocking** and **lock-free**, ensuring minimal impact on hot-path performance.

## Configuration

Monitoring is configured via the `[monitoring]` section in your configuration file.

### TOML Configuration

```toml
[monitoring]
# Enable metrics collection and Prometheus export
enabled = true

# Latency sampling rate for hot-path operations
# 0 = sample all requests (highest precision, highest overhead)
# 100 = sample 1% of requests (default, balanced)
# 1000 = sample 0.1% of requests (lowest overhead)
latency_sample_rate = 100

# Enable detailed per-operation latency tracking
latency_tracking = true

# Enable error categorization by type
error_tracking = true

# Enable saturation/resource utilization gauges
saturation_tracking = true

# Alert thresholds for resource saturation (0.0-1.0)
saturation_warning_threshold = 0.7
saturation_critical_threshold = 0.9
```

### JSON Configuration

```json
{
  "monitoring": {
    "enabled": true,
    "latency_sample_rate": 100,
    "latency_tracking": true,
    "error_tracking": true,
    "saturation_tracking": true,
    "saturation_warning_threshold": 0.7,
    "saturation_critical_threshold": 0.9
  }
}
```

### Default Values

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | `true` | Enable/disable all monitoring |
| `latency_sample_rate` | `100` | Sample 1% of requests for latency |
| `latency_tracking` | `true` | Track operation latencies |
| `error_tracking` | `true` | Categorize errors by type |
| `saturation_tracking` | `true` | Track resource utilization |
| `saturation_warning_threshold` | `0.7` | 70% utilization triggers warning |
| `saturation_critical_threshold` | `0.9` | 90% utilization triggers critical |

## The 4 Golden Signals

### 1. Latency

Request duration tracking with percentile distribution (p50, p95, p99, p999).

| Metric | Description |
|--------|-------------|
| `lance_ingest_latency_p*_seconds` | End-to-end ingest request latency |
| `lance_fetch_latency_p*_seconds` | Consumer fetch request latency |
| `lance_io_latency_p*_seconds` | Disk I/O operation latency |
| `lance_replication_latency_p*_seconds` | Leader → follower replication latency |
| `lance_network_latency_p*_seconds` | Network round-trip latency |

**Recommended Alert Thresholds:**
- Warning: p99 > 100ms
- Critical: p99 > 1s

### 2. Traffic

Request rate and throughput metrics.

| Metric | Description |
|--------|-------------|
| `lance_ingest_ops_per_second` | Ingest operations per second |
| `lance_ingest_bytes_per_second` | Ingest throughput (bytes/sec) |
| `lance_read_ops_per_second` | Read/fetch operations per second |
| `lance_read_bytes_per_second` | Read throughput (bytes/sec) |
| `lance_connections_active` | Current active connections |

### 3. Errors

Error counts categorized by type for targeted debugging.

| Metric | Description |
|--------|-------------|
| `lance_errors_total{type="protocol"}` | Protocol/parsing errors |
| `lance_errors_total{type="io"}` | Disk/network I/O errors |
| `lance_errors_total{type="timeout"}` | Operation timeouts |
| `lance_errors_total{type="checksum"}` | CRC/checksum failures |
| `lance_errors_total{type="replication"}` | Replication/quorum failures |
| `lance_errors_total{type="backpressure"}` | Backpressure rejections |
| `lance_errors_total{type="auth"}` | Authentication failures |
| `lance_errors_total{type="internal"}` | Internal/unexpected errors |

### 4. Saturation

Resource utilization gauges indicating system capacity.

| Metric | Description | Warning | Critical |
|--------|-------------|---------|----------|
| `lance_queue_saturation_ratio` | Ingest queue depth | > 70% | > 90% |
| `lance_memory_saturation_ratio` | Memory utilization | > 70% | > 90% |
| `lance_buffer_pool_saturation_ratio` | Buffer pool usage | > 70% | > 90% |
| `lance_connection_saturation_ratio` | Connection pool usage | > 70% | > 90% |
| `lance_pending_io_count` | Pending I/O operations | N/A | N/A |

## Performance Impact

### Why Sampling?

LANCE is designed for high-throughput, low-latency data ingestion. Adding observability to hot paths (ingest, fetch, I/O) risks degrading the very performance we're trying to monitor.

Key concerns addressed:
- **System call overhead** - Each timing measurement requires a system call
- **Atomic contention** - Histogram updates under high concurrency
- **Memory allocation** - Must avoid allocations on critical paths

### How Sampling Works

Instead of measuring every request, LANCE uses statistical sampling:

1. A thread-local counter tracks request count (no contention)
2. Every Nth request (configured by `latency_sample_rate`) triggers actual timing
3. Non-sampled requests incur negligible overhead (~1 CPU cycle)
4. Sampled requests perform full timing and histogram update

### Overhead Analysis

| Scenario | Overhead | Notes |
|----------|----------|-------|
| Non-sampled (99%) | ~1 CPU cycle | Thread-local counter check only |
| Sampled (1%) | ~200-500ns | Full timing + histogram update |
| **Amortized** | **~3-6ns/request** | Negligible at high throughput |

**At 1 million requests/second:**
- Without sampling: 200-500µs overhead per second (unacceptable)
- With 1% sampling: 2-5µs overhead per second (negligible)

### Why This Trade-off Is Worthwhile

The benefits of latency tracking far outweigh the microscopic performance cost:

1. **Early anomaly detection** - Latency spikes often precede failures
2. **Capacity planning** - p99 trends inform scaling decisions
3. **SLO compliance** - Contractual latency guarantees require measurement
4. **Root cause analysis** - Percentile breakdown isolates bottlenecks
5. **Performance regression detection** - CI/CD latency gates

**The math:**
- 3-6ns amortized overhead on a 50µs p99 ingest = **0.006-0.012%** impact
- Value of catching a 10x latency regression before production = **invaluable**

### Tuning the Sample Rate

Adjust `latency_sample_rate` based on your throughput and precision needs:

| Throughput | Recommended Rate | Sampling % | Use Case |
|------------|------------------|------------|----------|
| < 10K req/s | `0` | 100% | Development, debugging |
| 10K-50K req/s | `100` | 1% | Default production |
| 50K-500K req/s | `500` | 0.2% | High throughput |
| > 500K req/s | `1000` | 0.1% | Ultra-high throughput |

**Example configurations:**

Development (full precision):
```toml
[monitoring]
latency_sample_rate = 0
```

High-throughput production:
```toml
[monitoring]
latency_sample_rate = 500
```

## Prometheus Integration

### Enabling the Exporter

The Prometheus exporter is enabled by default and listens on the address configured by `metrics_addr` (default: `0.0.0.0:9090`).

```toml
# Main configuration
metrics_addr = "0.0.0.0:9090"

[monitoring]
enabled = true
```

### Scrape Configuration

Add LANCE to your Prometheus scrape configuration:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'lance'
    static_configs:
      - targets: ['lance-node-1:9090', 'lance-node-2:9090']
    scrape_interval: 15s
```

### Example Prometheus Queries

**Ingest throughput:**
```promql
rate(lance_bytes_ingested_total[5m])
```

**p99 ingest latency:**
```promql
lance_ingest_latency_p99_seconds
```

**Error rate by type:**
```promql
sum by (type) (rate(lance_errors_total[5m]))
```

**Queue saturation alert:**
```promql
lance_queue_saturation_ratio > 0.8
```

**Combined health check:**
```promql
(lance_queue_saturation_ratio > 0.9) or (lance_ingest_latency_p99_seconds > 1)
```

## Grafana Dashboards

We recommend organizing dashboards by signal:

1. **Overview** - All 4 signals at a glance
2. **Latency Deep Dive** - Percentile trends, heatmaps
3. **Throughput** - Traffic patterns, rate limiting
4. **Errors** - Error breakdown, correlation with events
5. **Saturation** - Resource utilization, capacity planning

## Alerting Best Practices

1. **Alert on symptoms, not causes** - Alert on p99 latency, not CPU usage
2. **Use rate() for counters** - Always apply `rate()` to `_total` metrics
3. **Set meaningful thresholds** - Base alerts on SLO requirements
4. **Correlate signals** - High latency + high saturation = capacity issue
5. **Trend, don't snapshot** - Look at 5m/15m/1h trends, not instant values

### Example Alert Rules

```yaml
# alertmanager rules
groups:
  - name: lance
    rules:
      - alert: LanceHighLatency
        expr: lance_ingest_latency_p99_seconds > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LANCE p99 latency elevated"
          
      - alert: LanceQueueSaturation
        expr: lance_queue_saturation_ratio > 0.9
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "LANCE ingest queue near capacity"
          
      - alert: LanceErrorRate
        expr: rate(lance_errors_total[5m]) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "LANCE error rate elevated"
```

## Summary

LANCE's monitoring implementation demonstrates that **observability and performance are not mutually exclusive**. Through careful engineering—lock-free atomics, statistical sampling, and thread-local counters—we achieve comprehensive visibility with negligible overhead.

The ~3-6ns amortized cost per request is a small price for the operational confidence that comes from knowing your system's latency, throughput, errors, and saturation at all times. The ability to detect issues before they impact users, plan capacity based on real data, and debug production problems quickly makes this trade-off clearly worthwhile.

---

[↑ Back to Top](#monitoring-and-observability) | [← Back to Docs Index](./README.md)
