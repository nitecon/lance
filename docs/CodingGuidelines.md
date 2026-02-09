[← Back to Docs Index](./README.md)

# LANCE Engineering Standards

## Table of Contents

- [1. The Golden Rules](#1-the-golden-rules)
- [2. Memory & Ownership](#2-memory-ownership)
- [3. Concurrency & Threading](#3-concurrency-threading)
- [4. I/O & Storage](#4-io-storage)
- [5. Network & Protocol](#5-network-protocol)
- [6. Safety & Error Handling](#6-safety-error-handling)
- [7. Observability](#7-observability)
- [8. Code Style & Idioms](#8-code-style-idioms)
- [9. Crate Architecture](#9-crate-architecture)
- [10. CI/CD & Review Gates](#10-cicd-review-gates)
- [11. Performance Regression Testing](#11-performance-regression-testing)
- [Quick Reference Card](#quick-reference-card)

**Revision**: 2026.02 | **Status**: Mandatory | **Scope**: All Contributors

> *"At 100Gbps, every microsecond counts. A single allocation on the hot path is not a performance bug—it's a design failure."*

---

## 1. The Golden Rules

These rules are **non-negotiable**. Violations block merge.

| Rule | Violation | Detection |
|------|-----------|-----------|
| **No Mutex on Hot Path** | `std::sync::Mutex`, `parking_lot::Mutex` in `lnc-io`, `lnc-core`, `lnc-network` | `grep -r "Mutex" src/` |
| **No Clone on Payloads** | `.clone()` on `Vec<u8>`, `Bytes`, `IngestionBatch`, or buffer types | Clippy + manual review |
| **No Allocation in Poller** | `Vec::new()`, `Box::new()`, `String::from()` in io_uring loop | Custom lint |
| **No Dynamic Dispatch** | `Box<dyn Trait>`, `&dyn Trait` on data plane | `grep -r "dyn " src/` |
| **No Unwrap in Production** | `.unwrap()`, `.expect()` outside `#[cfg(test)]` | `#![deny(clippy::unwrap_used)]` |
| **No `to_vec()` on Network Path** | `payload.to_vec()` in `NetworkActor` | Explicit review gate |

---

## 2. Memory & Ownership

### 2.1 The No-Copy Mandate

In LANCE, **an allocation on the hot path is a bug**.

```rust
// ❌ FORBIDDEN: Copies payload data
fn handle_frame(payload: &[u8]) {
    let owned = payload.to_vec();  // ALLOCATION + COPY
    queue.send(owned);
}

// ✅ REQUIRED: Zero-copy via Bytes
fn handle_frame(buffer: &ConnectionBuffer, start: usize, len: usize) {
    let payload: Bytes = buffer.slice(start, len);  // REFCOUNT INCREMENT ONLY
    queue.send(payload);
}
```

### 2.2 The Loaner Pattern

Buffers are **loaned**, not shared. Ownership transfers cleanly between actors.

```
Free Pool → Ingestion Actor → Handoff Queue → io_uring Poller → Free Pool
     ↑                                                              │
     └──────────────────────────────────────────────────────────────┘
```

**Rules**:
- Use `LoanableBatch` for all payload buffers
- Transfer via `ArrayQueue` (lock-free MPMC) or `ringbuf` (SPSC)
- Never wrap in `Arc<Mutex<_>>`—if you need shared access, your design is wrong

### 2.3 Alignment Requirements

| Buffer Type | Alignment | Reason |
|-------------|-----------|--------|
| I/O buffers | 4KB (`align(4096)`) | `O_DIRECT` requirement |
| Hot structs | 8-byte (`align(8)`) | CPU word alignment |
| Cache-critical | 64-byte (`align(64)`) | Avoid false sharing |

```rust
// ✅ CORRECT: Explicit alignment
#[repr(C, align(4096))]
pub struct AlignedBuffer {
    data: [u8; BATCH_SIZE],
}

// ❌ WRONG: Packed (causes unaligned access)
#[repr(C, packed)]  // NEVER USE ON HOT PATH
pub struct BadStruct { /* ... */ }
```

### 2.4 NUMA Locality

Memory must be allocated on the **same NUMA node** as the processing core.

```rust
// Allocate on specific NUMA node
let buffer = NumaAlignedBuffer::new(size, numa_node)?;

// Verify at runtime
if current_numa_node() != expected_node {
    metrics::increment!("lance_numa_misaligned");
    warn!("NUMA misalignment detected - performance degraded");
}
```

**Crossing the QPI/UPI link is a code review failure.**

### 2.5 Approved Crates for Memory

| Use Case | Crate | Why |
|----------|-------|-----|
| Zero-copy slicing | `bytes` | Reference-counted, no allocation on slice |
| Zero-copy structs | `zerocopy` | Safe transmute for `#[repr(C)]` types |
| Aligned allocation | `std::alloc` + manual | Full control over alignment |

---

## 3. Concurrency & Threading

### 3.1 Thread Architecture

| Thread Type | Runtime | Pinning | Parking |
|-------------|---------|---------|---------|
| **io_uring Poller** | Dedicated OS thread | Required (`isolcpus`) | **Never** (spin-wait) |
| **Ingestion Actor** | Dedicated OS thread | Required | **Never** |
| **Network Actor** | `tokio` async | Recommended | Allowed |
| **Replication Actor** | `tokio` async | Optional | Allowed |

### 3.2 Lock-Free or Bust

```rust
// ❌ FORBIDDEN on data plane
let guard = mutex.lock().unwrap();

// ✅ REQUIRED: Lock-free primitives
static COUNTER: AtomicU64 = AtomicU64::new(0);
COUNTER.fetch_add(1, Ordering::Relaxed);
```

### 3.3 Channel Selection

| Channel Type | Scale | Crate | Use Case |
|--------------|-------|-------|----------|
| SPSC | Single producer/consumer | `ringbuf` | Ingestion → io_uring (single-core) |
| MPSC | Multi-producer, single consumer | `flume` | Network → Ingestion |
| MPMC | Multi-producer, multi-consumer | `crossbeam::ArrayQueue` | Multi-actor → multi-poller |

### 3.4 Thread Pinning

```rust
use core_affinity::{CoreId, set_for_current};

pub fn pin_to_core(core_id: usize) -> Result<()> {
    set_for_current(CoreId { id: core_id })
        .then_some(())
        .ok_or(LanceError::PinFailed(core_id))
}
```

**Production kernel config**:
```bash
GRUB_CMDLINE_LINUX="isolcpus=2-7,10-15 nohz_full=2-7,10-15 rcu_nocbs=2-7,10-15"
```

### 3.5 IRQ Affinity

Thread pinning is incomplete without IRQ steering:

```bash
# NIC IRQs must match Network Actor cores
echo $CORE_MASK > /proc/irq/$IRQ/smp_affinity

# Disable irqbalance (conflicts with manual affinity)
systemctl disable irqbalance
```

**If NIC IRQs land on different cores than Network Actors, every packet crosses the L3 cache boundary.**

---

## 4. I/O & Storage

### 4.1 io_uring Rules

| Rule | Implementation |
|------|----------------|
| **Probe at startup** | Verify `io_uring_setup` succeeds; fall back to `pwritev2` if not |
| **O_DIRECT always** | All segment writes bypass page cache |
| **Registered buffers** | Pre-register with kernel for zero-copy |
| **Priority classes** | Writes: `IOPRIO_CLASS_RT`, Reads: `IOPRIO_CLASS_IDLE` |

### 4.2 Coherency Barrier

```rust
// ✅ CORRECT: Active segment uses io_uring
impl SegmentWriter {
    pub fn write(&mut self, batch: &Batch) -> Result<()> {
        self.iouring.submit_write(batch)?;  // Direct I/O
    }
}

// ✅ CORRECT: Closed segment uses mmap
impl SegmentReader {
    pub fn open_closed(path: &Path) -> Result<Self> {
        let mmap = unsafe { Mmap::map(&file)? };  // Safe: immutable
        Ok(Self::Mapped(mmap))
    }
}

// ❌ FORBIDDEN: mmap on active segment
// This causes cache incoherence with io_uring writes!
```

### 4.3 Batching

Never submit single records to io_uring:

```rust
// ❌ WRONG: One syscall per record
for record in records {
    iouring.submit_write(&record)?;
}

// ✅ CORRECT: Batch into IngestionBatch, single writev
let batch = collect_into_batch(records);
iouring.submit_writev(&batch)?;
```

---

## 5. Network & Protocol

### 5.1 LANCE Wire Protocol (LWP)

| Field | Size | Purpose |
|-------|------|---------|
| Magic | 4 bytes | `0x4C 0x41 0x4E 0x43` ('LANC') |
| Version | 1 byte | Protocol version |
| Flags | 1 byte | Compression, encryption, batch mode |
| Reserved | 2 bytes | Future use |
| Header CRC | 4 bytes | CRC32C of header |
| IngestHeader | 32 bytes | Payload metadata |
| **Total** | **44 bytes** | Fixed-size framing |

### 5.2 Zero-Copy Network Path

```rust
// ✅ CORRECT: Bytes slice (refcount increment)
let payload: Bytes = connection_buffer.slice(start, len);
ingestion_tx.send(IngestRequest { header, payload })?;

// ❌ FORBIDDEN: Vec allocation
let payload: Vec<u8> = buffer[start..end].to_vec();  // BLOCKS MERGE
```

### 5.3 Connection Buffer

Use `bytes::BytesMut` for per-connection buffers:

```rust
pub struct ConnectionBuffer {
    buffer: BytesMut,  // Reference-counted
}

impl ConnectionBuffer {
    pub fn slice(&self, start: usize, len: usize) -> Bytes {
        self.buffer.clone().freeze().slice(start..start + len)
    }
}
```

---

## 6. Safety & Error Handling

### 6.1 Documented Unsafe

Every `unsafe` block **must** have a `// SAFETY:` comment:

```rust
// SAFETY: IndexEntry is #[repr(C)] with known size/alignment.
// The mmap contains valid data written by LANCE with correct layout.
// The lifetime is tied to the Mmap which outlives this slice.
unsafe {
    std::slice::from_raw_parts(
        self.mmap.as_ptr() as *const IndexEntry,
        self.mmap.len() / std::mem::size_of::<IndexEntry>(),
    )
}
```

### 6.2 Error Handling

```rust
// ✅ CORRECT: Result with typed error
pub fn process(&mut self) -> Result<(), LanceError> {
    let batch = self.pool.try_borrow()
        .ok_or(LanceError::PoolExhausted)?;
    // ...
}

// ❌ FORBIDDEN in data plane
pub fn process(&mut self) {
    let batch = self.pool.borrow().unwrap();  // PANIC RISK
}
```

### 6.3 Checksum Validation

- **Network Actor** computes CRC32C using `crc32fast` (SIMD-accelerated)
- **Ingestion Actor** trusts pre-validated payloads
- Invalid checksums = immediate connection close

---

## 7. Observability

### 7.1 Non-Blocking Telemetry

```rust
// ✅ CORRECT: Atomic counter, relaxed ordering
static RECORDS_INGESTED: AtomicU64 = AtomicU64::new(0);
RECORDS_INGESTED.fetch_add(batch.len() as u64, Ordering::Relaxed);

// ❌ FORBIDDEN: Mutex-protected counter
static COUNTER: Mutex<u64> = Mutex::new(0);
*COUNTER.lock().unwrap() += 1;  // LOCK ON HOT PATH
```

### 7.2 Required Startup Logs

Every LANCE instance must log at startup:

| Item | Example |
|------|---------|
| io_uring status | `io_uring enabled (kernel 5.15+)` or `FALLBACK: pwritev2` |
| NUMA topology | `NIC on node 0, NVMe on node 1` |
| Thread pinning | `io_uring poller pinned to core 4` |
| IRQ affinity | `NIC IRQs steered to cores 2-5` |
| SIMD features | `CRC32C: SSE4.2 accelerated` |

### 7.3 Metrics Naming

```
lance_{subsystem}_{metric}_{unit}

Examples:
- lance_ingestion_records_total
- lance_io_write_latency_ns
- lance_network_bytes_received_total
- lance_pool_exhausted_total
```

---

## 8. Code Style & Idioms

### 8.1 Performance Idioms

| Idiom | Standard | Rationale |
|-------|----------|-----------|
| **Dispatch** | Static (`T: Trait`) | Virtual calls kill inlining |
| **Bit-packing** | Manual shifts | Compiler generates optimal masks |
| **Branching** | `likely`/`unlikely` | CPU branch prediction hints |
| **Serialization** | `zerocopy` | If calling `serialize()`, you're wrong |
| **Iteration** | `for` loops | Avoid iterator adapter overhead on hot path |

### 8.2 Branch Prediction

```rust
use std::intrinsics::{likely, unlikely};

// 99%+ of frames use standard header
if likely(type_byte != 0xFF) {
    // Fast path: standard header
} else {
    // Slow path: extended header
}
```

### 8.3 Bit-Packing

```rust
// Extract node_id from packed u64
const NODE_ID_SHIFT: u32 = 48;
const NODE_ID_MASK: u64 = 0xFFFF;

#[inline(always)]
pub fn extract_node_id(packed: u64) -> u16 {
    ((packed >> NODE_ID_SHIFT) & NODE_ID_MASK) as u16
}
```

---

## 9. Crate Architecture

```
lance/
├── lnc-core/           # TLV, SortKey, Bit-packing, LoanableBatch, Bytes integration
├── lnc-io/             # io_uring, NUMA pinning, pwritev2 fallback, O_DIRECT
├── lnc-network/        # LWP Protocol, ConnectionBuffer, Zero-Copy Send, IRQ affinity
├── lnc-replication/    # L1/L3 modes, Adaptive Eviction, Quorum management, Filesystem-consistent replication
├── lnc-index/          # Sparse Index, Secondary Index, Jitter Window queries
├── lnc-metrics/        # Non-blocking OTEL, Atomic counters, Prometheus export
├── lnc-recovery/       # WAL replay, Segment recovery, Index rebuild
├── lnc-cli/            # Triage tools, Index repair, Benchmarks
└── Cargo.toml
```

### 9.1 Dependency Rules

| Crate | May Depend On | Must NOT Depend On |
|-------|---------------|-------------------|
| `lnc-core` | `bytes`, `zerocopy`, `crossbeam` | `tokio`, `lnc-network` |
| `lnc-io` | `lnc-core`, `io-uring` | `tokio` |
| `lnc-network` | `lnc-core`, `tokio`, `bytes` | `lnc-io` (no circular) |

---

## 10. CI/CD & Review Gates

### 10.1 Required Pre-Completion Checks

All PRs **must pass** the following checks before merge:

| Check | Command | Purpose |
|-------|---------|---------|
| **Format** | `cargo fmt --all -- --check` | Enforce consistent code style |
| **Lint** | `cargo clippy --workspace --all-targets -- -D warnings` | Catch common mistakes and enforce idioms |
| **Compile** | `cargo check --all-targets --workspace` | Verify all code compiles (including tests, benches) |
| **Dependencies** | `cargo deny check` | Audit licenses, security advisories, and banned crates |

Run all checks locally before pushing:

```bash
# Quick validation script
cargo fmt --all -- --check && \
cargo clippy --workspace --all-targets -- -D warnings && \
cargo check --all-targets --workspace && \
cargo deny check
```

### 10.2 Automated CI Checks

```yaml
# .github/workflows/mechanical-integrity.yml
jobs:
  audit:
    steps:
      - name: No Mutex on hot path
        run: |
          if grep -rE "Mutex|RwLock" lnc-core lnc-io; then
            echo "❌ BLOCKED: Lock found on hot path"
            exit 1
          fi
          
      - name: No dynamic dispatch
        run: |
          if grep -rE "dyn\s+" lnc-core lnc-io lnc-network; then
            echo "❌ BLOCKED: Dynamic dispatch on data plane"
            exit 1
          fi
          
      - name: No to_vec on network path
        run: |
          if grep -rE "\.to_vec\(\)" lnc-network; then
            echo "❌ BLOCKED: to_vec() in network code"
            exit 1
          fi
          
      - name: Clippy strict
        run: cargo clippy -- -D clippy::unwrap_used -D clippy::expect_used
```

### 10.3 Human Review Checklist

Before approving any PR touching `lnc-core`, `lnc-io`, or `lnc-network`:

- [ ] **No new allocations** in ingestion loop, io_uring poller, or network parser
- [ ] **No new locks** anywhere in data plane crates
- [ ] **Ownership flows linearly** (no `Arc<Mutex<_>>` patterns)
- [ ] **New structs are aligned** (`#[repr(C, align(N))]`)
- [ ] **New metrics use atomics** with `Ordering::Relaxed`
- [ ] **Unsafe blocks have SAFETY comments**
- [ ] **Tests include latency assertions**

---

## 11. Performance Regression Testing

### 11.1 Latency Gates

```rust
#[test]
fn ingestion_latency_regression() {
    let result = benchmark_ingestion(1_000_000);  // 1M records
    
    assert!(result.p50_ns < 500, 
        "P50 regression: {} ns (limit: 500 ns)", result.p50_ns);
    assert!(result.p99_ns < 5_000, 
        "P99 regression: {} ns (limit: 5 μs)", result.p99_ns);
    assert!(result.p999_ns < 50_000, 
        "P999 regression: {} ns (limit: 50 μs)", result.p999_ns);
}
```

### 11.2 Allocation Gates

```rust
#[test]
fn zero_allocation_hot_path() {
    let stats = allocation_counter::count(|| {
        for _ in 0..10_000 {
            process_batch(&mut batch);
        }
    });
    
    assert_eq!(stats.allocations, 0, 
        "Hot path allocated {} times", stats.allocations);
}
```

### 11.3 Throughput Gates

| Benchmark | Minimum | Target |
|-----------|---------|--------|
| Ingestion (256B records) | 5M rec/s | 10M rec/s |
| Ingestion (4KB records) | 1M rec/s | 2M rec/s |
| Index lookup | 1M ops/s | 5M ops/s |
| Network parse (LWP) | 10M frames/s | 20M frames/s |

---

## Quick Reference Card

### Forbidden Patterns

```rust
// ❌ payload.to_vec()
// ❌ data.clone()
// ❌ mutex.lock()
// ❌ .unwrap() / .expect()
// ❌ Box<dyn Trait>
// ❌ Vec::new() in loop
// ❌ #[repr(C, packed)]
```

### Required Patterns

```rust
// ✅ buffer.slice() -> Bytes
// ✅ ownership.move()
// ✅ AtomicU64::fetch_add(_, Relaxed)
// ✅ Result<T, LanceError>
// ✅ T: Trait (static dispatch)
// ✅ pre-allocated pool.borrow()
// ✅ #[repr(C, align(N))]
```

---

**Remember**: At 100Gbps, a 1μs lock acquisition per batch at 10M batches/second is 10 seconds of cumulative blocking per second. The system collapses. These rules exist because they have been violated before.
---

[↑ Back to Top](#lance-engineering-standards) | [← Back to Docs Index](./README.md)
