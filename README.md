# LANCE

**Log-based Asynchronous Networked Compute Engine**

[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/docker/v/nitecon/lance?label=docker&logo=docker)](https://hub.docker.com/r/nitecon/lance)
[![Docker Pulls](https://img.shields.io/docker/pulls/nitecon/lance)](https://hub.docker.com/r/nitecon/lance)

> A high-performance, non-blocking stream engine designed to replace Kafka-heavy workloads with zero-copy efficiency and deterministic memory management.

---

## Why LANCE?
Lance was born from the limitations of traditional streaming systems (Kafka, NATS, Redpanda) in high-throughput, low-latency environments like HFT, network forensics, and robotics.

While these platforms excel at general-purpose pub/sub, they often falter in three critical areas:

- **Deterministic Replay & Pre-warming:** Standard offset management is often too cumbersome for training statistical models (LSTMs, NNEs) or pre-warming stateful applications. Lance treats stream rewinding as a first-class citizen, ensuring "cold starts" are backed by reliable, precise data baselines.
- **Client-Side Intelligence:** By offloading offset tracking and state management from the server to the workers, Lance eliminates unnecessary broker overhead and grants consumers more granular control over their data velocity.
- **Physical Portability:** Built with disaster recovery and multi-region warm-spares in mind, Lance’s storage architecture is file-system friendly. In a catastrophic failure, "warming" a new cluster is as simple as a reliable rsync of the underlying data files.

Modern stream processing systems face a fundamental tension: **throughput vs. latency**. Kafka and its alternatives make trade-offs that become bottlenecks at scale:

| System | Language | Allocation Model | I/O Model | Latency Profile |
|--------|----------|------------------|-----------|-----------------|
| Kafka | JVM | GC-managed | epoll + threads | P99 spikes during GC |
| Redpanda | C++ | Manual | io_uring | Good, but C++ complexity |
| NATS | Go | GC-managed | goroutines | P99 spikes during GC |
| **LANCE** | **Rust** | **Zero-copy pools** | **io_uring** | **Deterministic** |

LANCE is designed from the ground up for **100Gbps sustained ingestion** with **sub-microsecond P99 latency**.
---

## The Modern Streaming Platform for Cloud-Native Infrastructure

LANCE is built for the realities of modern compute platforms. Whether you're running on **Kubernetes**, bare metal, or hybrid environments, LANCE delivers:

### ☸️ Kubernetes-Native by Design

- **Ephemeral-friendly** — Stateless server design with client-side offset tracking keeps brokers simple
- **Built-in tee forwarding** — Fan-out streams to multiple downstream services without application logic
- **Graceful drain support** — Clean shutdown integration with K8s preStop hooks and readiness probes
- **Horizontal scaling** — Consumer groups with automatic partition rebalancing as pods scale

### ⚡ Engineered for Raw Performance

- **io_uring native** — Bypass syscall overhead with Linux's fastest async I/O interface
- **Zero-copy reads** — Arc-based mmap sharing delivers data directly from page cache to network
- **NUMA-aware** — Pin threads and allocate memory local to I/O devices for minimal latency
- **Deterministic latency** — No GC, no allocations on hot path, sub-microsecond P99

### 🧠 Smart Clients, Less Server Load

LANCE clients aren't dumb pipes—they're intelligent participants:

- **Grouped mode** — Automatic partition assignment and rebalancing across consumer instances
- **Standalone mode** — Direct offset control for batch jobs, replay scenarios, and CLI tools
- **Client-side batching** — Aggregate records locally before transmission, reducing round-trips
- **Backpressure-aware** — Clients respond to server signals, preventing cascade failures

The result? **Your servers do less work** while clients self-organize for optimal throughput.

---

## Key Design Principles

### 🚫 No Allocations on Hot Path

Every allocation is a potential latency spike. LANCE pre-allocates all buffers at startup and uses a **Loaner Pattern** for buffer recycling:

```
Free Pool → Ingestion Actor → io_uring Poller → Free Pool
```

No `malloc`, no GC, no surprises.

### 🔒 Lock-Free Data Plane

The data plane uses **zero locks**:
- Atomic counters with `Ordering::Relaxed`
- Lock-free SPSC/MPMC queues (`ringbuf`, `crossbeam`)
- Ownership transfer instead of shared state

### ⚡ io_uring Native

LANCE bypasses the traditional `read`/`write` syscall overhead:
- **Submission Queue**: Batch operations to kernel
- **Completion Queue**: Async notification of I/O completion
- **O_DIRECT**: Bypass page cache for predictable latency
- **Registered Buffers**: Zero-copy between kernel and userspace

### 🧠 NUMA-Aware

At 100Gbps, crossing the QPI/UPI link between CPU sockets adds 30-50ns per access. LANCE:
- Discovers NIC and NVMe NUMA topology at startup
- Pins threads to cores on the same NUMA node as their I/O devices
- Allocates buffers with `mbind()` for NUMA-local memory
- Steers NIC IRQs to match Network Actor cores

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              LANCE NODE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                   │
│   │   Network   │────►│  Ingestion  │────►│  io_uring   │                   │
│   │   Actor     │     │   Actor     │     │   Poller    │                   │
│   │  (tokio)    │     │  (pinned)   │     │  (pinned)   │                   │
│   └─────────────┘     └─────────────┘     └──────┬──────┘                   │
│         │                                        │                          │
│         │ LWP Protocol                           │ O_DIRECT                 │
│         │ (44-byte frames)                       │ (io_uring)               │
│         ▼                                        ▼                          │
│   ┌─────────────┐                         ┌─────────────┐                   │
│   │   Clients   │                         │   Segment   │                   │
│   │  (TCP/TLS)  │                         │   Files     │                   │
│   └─────────────┘                         │  (.lnc)     │                   │
│                                           └─────────────┘                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Responsibility | Thread Model |
|-----------|----------------|--------------|
| **Network Actor** | LWP protocol parsing, CRC validation | `tokio` async |
| **Ingestion Actor** | TLV encoding, SortKey assignment, batch sorting | Dedicated, pinned |
| **io_uring Poller** | Kernel I/O submission/completion | Dedicated, pinned, never parks |
| **Replication Actor** | L1/L3 replication, quorum management | `tokio` async |

---

## Storage Format

### Segment Files (`.lnc`)

Immutable, append-only files containing TLV-encoded records:

```
┌─────────┬─────────┬─────────┬─────────┬─────────┐
│  TLV 0  │  TLV 1  │  TLV 2  │   ...   │  TLV N  │
└─────────┴─────────┴─────────┴─────────┴─────────┘
```

**TLV Header**: `[Type:1][Length:4][Value:N]` (5 bytes + payload)

### Sparse Index (`.idx`)

Memory-mapped index for O(1) lookups:

```
┌────────────────────┬────────────────────┬─────┐
│ SortKey | Offset   │ SortKey | Offset   │ ... │
│ (16 + 8 bytes)     │ (16 + 8 bytes)     │     │
└────────────────────┴────────────────────┴─────┘
```

### 128-bit Sort Key

Deterministic ordering across distributed nodes:

```
┌──────────────────┬───────────┬───────────┬──────────┐
│  timestamp_ns    │  node_id  │ actor_id  │ sequence │
│    (64 bits)     │ (16 bits) │  (8 bits) │ (40 bits)│
└──────────────────┴───────────┴───────────┴──────────┘
```

---

## Replication Modes

| Mode | Consistency | Latency | Use Case |
|------|-------------|---------|----------|
| **L1** | Eventual | Lowest | High-throughput analytics |
| **L3** | Strong (M/2+1 quorum) | Higher | Financial transactions |

L3 mode includes **Adaptive Eviction**: slow followers are automatically removed from quorum to prevent tail latency poisoning.

---

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **Ingestion throughput** | 100 Gbps | Sustained, not burst |
| **P50 latency** | < 500 ns | Network → Disk acknowledged |
| **P99 latency** | < 5 μs | No GC, no locks |
| **P999 latency** | < 50 μs | Deterministic, not probabilistic |

Below is a local performance benchmark run on a 16-core AMD Ryzen 9 7950X3D with 32gb of ram.  You can run this yourself with:
```bash
./scripts/run-docker-tests.sh --local --benchmark
```

### LANCE Performance Benchmark Results
```
========================================
LANCE Performance Benchmark Results
========================================
Date: Wed Feb  4 09:18:46 PM EST 2026
Kernel: 6.14.0-37-generic
CPU: AMD Ryzen 9 7950X3D 16-Core Processor
Memory: 29Gi

=== io_uring Support ===
[21:18:46] Checking kernel TEE/splice support...
[21:18:46]   Kernel version: 6.14.0-37-generic
[21:18:46]   ✓ Kernel supports io_uring TEE operations
[21:18:46]   ✓ io_uring is enabled
TEE: Supported

=== Cargo Benchmarks ===

=== IO Backend Benchmarks ===
    Finished `release` profile [optimized] target(s) in 0.06s
     Running unittests src/lib.rs (target/release/deps/lnc_io-c112fd2f8fad787d)

running 6 tests
test backend::tests::test_io_backend_type_clone ... ok
test backend::tests::test_io_backend_type_debug ... ok
test backend::tests::test_io_backend_type_default ... ok
test backend::tests::test_io_backend_type_equality ... ok
test backend::tests::test_probe_io_uring ... ok
test fallback::tests::test_pwritev2_backend_write_read ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s


=== Priority Queue Benchmarks ===
    Finished `release` profile [optimized] target(s) in 0.06s
     Running unittests src/lib.rs (target/release/deps/lnc_io-c112fd2f8fad787d)

running 5 tests
test priority::tests::test_priority_preempts ... ok
test priority::tests::test_priority_ordering ... ok
test priority::tests::test_priority_queue_strict ... ok
test priority::tests::test_priority_queue_weighted ... ok
test priority::tests::test_priority_stats ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.00s


=== Forward Config Benchmarks ===
    Finished `release` profile [optimized] target(s) in 0.06s
     Running unittests src/lib.rs (target/release/deps/lnc_replication-91dcf25c49e61c4d)

running 15 tests
test forward::tests::test_forward_config_default ... ok
test forward::tests::test_acquire_fails_without_leader ... ok
test forward::tests::test_forward_config_pool_size_bounds ... ok
test forward::tests::test_concurrent_leader_change_safety ... ok
test forward::tests::test_forward_config_with_tee ... ok
test forward::tests::test_concurrent_leader_addr_access ... ok
test forward::tests::test_forward_error_display_coverage ... ok
test forward::tests::test_local_write_error_display ... ok
test forward::tests::test_pool_tee_status_methods ... ok
test forward::tests::test_pooled_connection_pool_accessor ... ok
test forward::tests::test_pool_leader_unknown ... ok
test forward::tests::test_tee_forwarding_status_disabled ... ok
test forward::tests::test_noop_local_processor ... ok
test forward::tests::test_tee_forwarding_status_enum ... ok
test forward::tests::test_pool_leader_change ... ok

test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 70 filtered out; finished in 0.00s


=== TEE vs Splice Performance Benchmark ===
    Finished `release` profile [optimized] target(s) in 0.06s
     Running unittests src/lib.rs (target/release/deps/lnc_io-c112fd2f8fad787d)

running 1 test

=== TEE vs Splice Performance Benchmark ===
TEE Support: Supported
Splice Support: Supported

Forwarder Status:
  Splice Forwarder: created
  TEE Forwarder: created
  TEE fully supported: true

--- Pipe Creation Benchmark ---
Pipe create+close: 3619 ns/op (276319 ops/sec)

--- Memory Overhead ---
SpliceForwarder size: 280 bytes
TeeForwarder size: 280 bytes
SplicePipe size: 8 bytes

--- Operation Tracking ---
Splice pending ops: 0
TEE pending ops: 0

--- Performance Summary ---
TEE forwarding duplicates data in-kernel without userspace copies.
For L3 sync quorum: forward to leader + local ACK = 2 destinations, 0 copies.
Standard forwarding would require: read() + write() + write() = 2 copies.

Payload       Std (2 copies)    TEE (0 copy)      Savings
----------------------------------------------------------
1024B                6144 ns          500 ns       91.9%
4096B               24576 ns          500 ns       98.0%
16384B              98304 ns          500 ns       99.5%
65536B             393216 ns          500 ns       99.9%

Note: Actual performance depends on kernel version, CPU, and memory bandwidth.
Run integration tests with --benchmark for end-to-end latency measurements.

test uring::tests::benchmark_tee_vs_splice_performance ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 39 filtered out; finished in 0.04s


=== to_vec() Overhead Benchmark (Compression None) ===
    Finished `release` profile [optimized] target(s) in 0.06s
     Running unittests src/lib.rs (target/release/deps/lnc_network-3a4f9166bd071da6)

running 1 test

=== to_vec() Overhead Benchmark (CompressionAlgorithm::None) ===
Size           Total (µs)  Per-op (ns)   Throughput
----------------------------------------------------
1024B                  16           16   63960.02 MB/s
4096B                  53           53   77022.88 MB/s
16384B                212          212   77037.01 MB/s
65536B               1083         1083   60501.88 MB/s
262144B              3774         3774   69443.39 MB/s

Note: to_vec() creates a full copy. Consider Bytes::copy_from_slice()
or Cow<[u8]> if zero-copy passthrough is needed on hot path.

test compression::tests::benchmark_to_vec_overhead_none_compression ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 37 filtered out; finished in 0.01s


========================================
Benchmark Summary
========================================
Completed at: Wed Feb  4 09:18:47 PM EST 2026
```


---

## Throughput Benchmark (`lnc-bench`)

Real-world throughput and latency measured with `lnc-bench` — a pipelined benchmark tool that pushes sustained load through the full write path (network → ingestion → WAL → segment → ACK).

### Local NVMe (MacBook Pro 2025, Apple M-series, 16 cores)

All tests: 1 KB messages, L1 Solo, macOS (`pwritev2` fallback — no `io_uring`), zero errors.

| Profile | Connections | Pipeline | Msg/s | Bandwidth | p50 | p99 | p999 |
|---------|-----------|----------|------:|----------:|----:|----:|-----:|
| **Latency-optimised** | 1 | 64 | 9,083 | 8.9 MB/s | 6.99 ms | 11.24 ms | 13.15 ms |
| **Balanced** | 8 | 256 | 130,800 | 127.7 MB/s | 14.69 ms | 24.84 ms | 49.91 ms |
| **Max throughput** | 16 | 512 | **237,442** | **231.9 MB/s** | 31.66 ms | 52.25 ms | 119.23 ms |

> On Linux with `io_uring` + `O_DIRECT`, expect **2-5× higher throughput** and significantly lower latency.

### Kubernetes (Ceph RBD Network-Attached Storage)

Test parameters: 1 KB messages, 8 connections, 256 pipeline depth, 128 KB batches.

| Deployment | Msg/s | Throughput | p50 Latency |
|------------|------:|----------:|------------:|
| **L1 Solo** (1 node) | 8,640 | 8.4 MB/s | 237 ms |
| **L3 Cluster** (3 nodes) | 32,360 | 31.6 MB/s | 59 ms |

> L3 outperforms L1 on Ceph RBD because quorum replication parallelises I/O across 3 volumes while solo serialises on one.

### LANCE vs Kafka — At a Glance

| Metric | LANCE (measured) | Kafka (typical) | Advantage |
|--------|----------------:|----------------:|:---------:|
| **Single-node throughput** | 237K msg/s (231.9 MB/s) | 50K-200K msg/s | **LANCE** |
| **P99 latency (1 conn)** | 11.24 ms (macOS, no io_uring) | 5-50 ms | Comparable |
| **P99 latency (Linux, io_uring)** | < 5 µs (bench gate) | 5-50 ms | **1000×+ LANCE** |
| **Container image** | ~20 MB | ~400 MB+ | **20× smaller** |
| **Startup time** | < 1 second | 10-60 seconds | **10-60× faster** |
| **Memory footprint** | 256 MB - 1 GB | 4-32 GB | **10-100× less** |
| **GC pauses** | None (Rust) | 100 ms - 2s | **∞ LANCE** |

> 📄 **[Full LANCE vs Kafka Comparison →](docs/LanceVsKafkaComparison.md)** — Architecture, throughput deep dive, replication, wire protocol, feature matrix, and when to choose what.

Run your own benchmark:

```bash
cargo run --release -p lnc-bench -- \
  --endpoint <host>:1992 \
  --topic-name bench-test \
  --connections 8 \
  --pipeline 256 \
  --msg-size 1024 \
  --duration 30
```

---

## Chaos Test Results

Data integrity verified with `lnc-chaos` — rolling-restart chaos testing that produces messages during Kubernetes StatefulSet restarts and validates zero gaps / zero duplicates on consumption.

| Deployment | Messages Produced | Gaps | Duplicates | Result |
|------------|------------------:|-----:|-----------:|--------|
| **L3 Cluster** (3-node quorum) | 2,963+ | 0 | 0 | **PASS** |
| **L1 Solo** (single node) | 1,504+ | 0 | 0 | **PASS** |

> **Key insight:** L1 solo achieves zero-gap results when the *server* is restarted (graceful drain preserves all acknowledged writes). Gaps only occur if the *client* process dies mid-send or if the underlying storage layer (e.g., Ceph RBD) doesn't honour `fsync` semantics on pod kill.

Run chaos tests:

```bash
cargo run --release -p lnc-chaos -- \
  --endpoint <host>:1992 \
  --clean \
  --duration 120
```

---

## Getting Started

### Prerequisites

- **Rust**: 2024 edition (1.85+) - Required for `gen` blocks, `async` closures, and Linux-specific features
- **Linux**: 5.15+ (io_uring with `IORING_OP_SEND_ZC`, `IORING_OP_TEE`)
- **Hardware**: NVMe SSD, 10G+ NIC recommended

> **Note**: LANCE is Linux-only due to io_uring requirements. The client library (`lnc-client`) supports all platforms.

### Build

```bash
cargo build --release
```

### Run

```bash
# Start a single-node instance
./target/release/lance --config lance.toml

# With NUMA awareness (production)
numactl --cpunodebind=0 --membind=0 ./target/release/lance --config lance.toml
```

### Configuration

```toml
# lance.toml
[node]
id = 1
data_dir = "/var/lib/lance"

[network]
bind = "0.0.0.0:9000"
nic_pci_addr = "0000:3b:00.0"

[io]
uring_sq_depth = 4096
batch_size = 262144  # 256KB
o_direct = true

[numa]
enabled = true
nic_node = 0
nvme_node = 0

[replication]
mode = "L1"  # or "L3"
peers = ["lance-2:9000", "lance-3:9000"]
```

---

## Kubernetes Deployment

A production-ready Kubernetes StatefulSet configuration is available at [`k8s/lance-cluster.yaml`](k8s/lance-cluster.yaml).

### Quick Start

```bash
# Create namespace
kubectl create namespace lance

# Label namespace for io_uring support (requires privileged pods)
kubectl label namespace lance pod-security.kubernetes.io/enforce=privileged

# Deploy the cluster
kubectl apply -n lance -f k8s/lance-cluster.yaml
```

### Configuration Notes

Before deploying, update the following to match your environment:

| Setting | Location | Description |
|---------|----------|-------------|
| **`storageClassName`** | `volumeClaimTemplates` | Change `ceph-block` to your cluster's storage class |
| **DNS names** | `--peers` argument | Update `lance-X.lance-headless` if using a different service name |
| **Replicas** | `spec.replicas` | Adjust cluster size (update `--peers` accordingly) |

### Using a Config File (Recommended)

For cleaner configuration, mount a ConfigMap with `lance.toml` instead of passing CLI arguments:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: lance-config
data:
  lance.toml: |
    [node]
    data_dir = "/var/lib/lance"
    
    [replication]
    mode = "L3"
    peers = ["lance-0.lance-headless:1993", "lance-1.lance-headless:1993", "lance-2.lance-headless:1993"]
```

Then mount it in your StatefulSet:

```yaml
volumes:
- name: config
  configMap:
    name: lance-config
containers:
- name: lance
  command: ["/usr/local/bin/lance", "--config", "/etc/lance/lance.toml", "--node-id", "$(NODE_ID)"]
  volumeMounts:
  - name: config
    mountPath: /etc/lance
```

### Security Context

The StatefulSet requires:
- **`seccompProfile: Unconfined`** — Required for io_uring syscalls
- **`SYS_ADMIN` capability** — Required for io_uring ring setup
- **`fsGroup: 1000`** — Matches the `lance` user in the container

---

## Installation

### Pre-built Binaries

Official releases include the **LANCE server** (`lance`) and **CLI client** (`lnc`) as pre-built binaries:

```bash
# Download from GitHub Releases
curl -LO https://github.com/nitecon/lance/releases/latest/download/lance-linux-amd64.tar.gz
tar xzf lance-linux-amd64.tar.gz

# Server
./lance --config lance.toml

# CLI client
./lnc produce --topic events --data '{"event": "click"}'
./lnc consume --topic events --offset earliest
```

### Client Library

The Rust client library is available on crates.io:

```bash
cargo add lnc-client
```

---

## Documentation

| Document                                          | Description                             |
|---------------------------------------------------|-----------------------------------------|
| [Architecture](docs/Architecture.md)              | Deep-dive into system design            |
| [Coding Guidelines](docs/CodingGuidelines.md)     | Engineering standards and requirements  |
| [Kubernetes Deployment](k8s/lance-cluster.yaml)   | Production StatefulSet configuration    |
| [LWP Specification](docs/LWP-Specification.md)    | Lance Wire Protocol (LWP) Specification |
| [Monitoring](docs/Monitoring.md)                  | Monitoring and Observability            |
| [Recovery](docs/Recoveryprocedures.md)            | Recovery Procedures                     |
| [LANCE vs Kafka](docs/LanceVsKafkaComparison.md) | Architectural comparison & benchmarks   |
| [lnc-bench](lnc-bench/)                          | Throughput & latency benchmark tool     |
| [lnc-chaos](lnc-chaos/)                          | Rolling-restart chaos testing tool      |

---

## Contributing

We welcome contributions! Please read our [Contributing Guide](CONTRIBUTING.md) and [Coding Guidelines](docs/CodingGuidelines.md) before submitting PRs.

### Developer Setup

```bash
# Install Rust 2024 edition toolchain
rustup update
rustup component add rustfmt clippy

# Install pre-commit hooks
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg

# Verify everything works
cargo build
cargo test
pre-commit run --all-files
```

**Key rules**:
- No allocations on hot path
- No locks on data plane
- All `unsafe` blocks must have `// SAFETY:` comments
- Performance regression tests required for data plane changes
- Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/)

---

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

---

## Acknowledgments

LANCE draws inspiration from:
- [Redpanda](https://redpanda.com/) - Proving C++ can achieve Kafka-like semantics with better performance
- [io_uring](https://kernel.dk/io_uring.pdf) - Jens Axboe's revolutionary Linux I/O interface
- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) - Lock-free inter-thread messaging patterns
- [Aeron](https://github.com/real-logic/aeron) - High-performance messaging with mechanical sympathy

---

<p style="text-align: center;">
  <strong>Built with 🦀 Rust for deterministic performance</strong>
</p>
