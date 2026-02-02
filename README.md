# LANCE

**Log-based Asynchronous Networked Compute Engine**

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> A high-performance, non-blocking stream engine designed to replace Kafka-heavy workloads with zero-copy efficiency and deterministic memory management.

---

## Why LANCE?

Modern stream processing systems face a fundamental tension: **throughput vs. latency**. Kafka and its alternatives make trade-offs that become bottlenecks at scale:

| System | Language | Allocation Model | I/O Model | Latency Profile |
|--------|----------|------------------|-----------|-----------------|
| Kafka | JVM | GC-managed | epoll + threads | P99 spikes during GC |
| Redpanda | C++ | Manual | io_uring | Good, but C++ complexity |
| NATS | Go | GC-managed | goroutines | P99 spikes during GC |
| **LANCE** | **Rust** | **Zero-copy pools** | **io_uring** | **Deterministic** |

LANCE is designed from the ground up for **100Gbps sustained ingestion** with **sub-microsecond P99 latency**.

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
| **Replication Actor** | L1/L2 replication, quorum management | `tokio` async |

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
| **L2** | Strong (M/2+1 quorum) | Higher | Financial transactions |

L2 mode includes **Adaptive Eviction**: slow followers are automatically removed from quorum to prevent tail latency poisoning.

---

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **Ingestion throughput** | 100 Gbps | Sustained, not burst |
| **P50 latency** | < 500 ns | Network → Disk acknowledged |
| **P99 latency** | < 5 μs | No GC, no locks |
| **P999 latency** | < 50 μs | Deterministic, not probabilistic |

---

## Getting Started

### Prerequisites

- **Rust**: 1.75+ (nightly for `likely`/`unlikely` intrinsics)
- **Linux**: 5.15+ (io_uring with `IORING_OP_SEND_ZC`)
- **Hardware**: NVMe SSD, 10G+ NIC recommended

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
mode = "L1"  # or "L2"
peers = ["lance-2:9000", "lance-3:9000"]
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/Architecture.md) | Deep-dive into system design |
| [Coding Guidelines](docs/CodingGuidelines.md) | Engineering standards and patterns |
| [Operations](docs/Operations.md) | Deployment and monitoring guide |

---

## Project Status

🚧 **Active Development** 🚧

LANCE is under active development. The architecture is stable, but the implementation is in progress.

### Roadmap

- [x] Architecture design
- [x] Engineering standards
- [ ] Core TLV encoding (`lnc-core`)
- [ ] io_uring integration (`lnc-io`)
- [ ] LWP protocol (`lnc-network`)
- [ ] Replication (`lnc-replication`)
- [ ] Observability (`lnc-metrics`)
- [ ] CLI tools (`lnc-cli`)

---

## Contributing

We welcome contributions! Please read our [Contributing Guide](CONTRIBUTING.md) and [Coding Guidelines](docs/CodingGuidelines.md) before submitting PRs.

### Developer Setup

```bash
# Install Rust toolchain
rustup update stable
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
