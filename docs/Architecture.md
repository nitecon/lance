[← Back to Docs Index](./README.md)

# Technical Design: Project LANCE

## Table of Contents

- [1. System Overview](#1-system-overview)
- [2. Storage Layer Architecture](#2-storage-layer-architecture)
- [3. Memory & I/O Management](#3-memory-io-management)
- [4. Replication & State Machine](#4-replication-state-machine)
- [5. Concurrency Model](#5-concurrency-model)
- [6. Backpressure & Flow Control](#6-backpressure-flow-control)
- [7. Crash Recovery & Durability](#7-crash-recovery-durability)
- [8. io_uring Feature Baseline](#8-io_uring-feature-baseline)
- [9. Graceful Shutdown & Signal Handling](#9-graceful-shutdown-signal-handling)
- [10. Container Deployment & Ephemeral Integrity](#10-container-deployment-ephemeral-integrity)
- [11. Chaos Testing & Failure Mode Design](#11-chaos-testing-failure-mode-design)
- [12. Observability & Telemetry](#12-observability-telemetry)
- [13. Memory Layout & Zero-Copy Primitives](#13-memory-layout-zero-copy-primitives)
- [14. Thread Pinning & NUMA Awareness](#14-thread-pinning-numa-awareness)
- [15. Buffer Ownership & Lifecycle (The Loaner Pattern)](#15-buffer-ownership-lifecycle-the-loaner-pattern)
- [16. The LANCE Wire Protocol (LWP)](#16-the-lance-wire-protocol-lwp)
- [17. Consumer Read Path (Zero-Copy Out)](#17-consumer-read-path-zero-copy-out)
- [18. Mechanical Integrity Checklist (Engineering Audit)](#18-mechanical-integrity-checklist-engineering-audit)
- [19. Client-Side Offset Management](#19-client-side-offset-management)
- [20. Consumer Client Modes](#20-consumer-client-modes)
- [21. Write Forwarding (Cluster Mode)](#21-write-forwarding-cluster-mode)
- [22. Write Buffering & Deferred Sync Architecture](#22-write-buffering-deferred-sync-architecture)
- [23. CATCHING_UP Protocol](#23-catching_up-protocol)
- [24. Recursive Segment Recovery](#24-recursive-segment-recovery)

---

This design paper outlines the architecture for LANCE (Log-based Asynchronous Networked Compute Engine), a high-performance, non-blocking stream engine designed to replace Kafka-heavy workloads with zero-copy efficiency and deterministic memory management.

## 1. System Overview

LANCE is a distributed, file-based stream engine optimized for sub-microsecond ingestion and dual-index retrieval (Sequence & Timestamp). Unlike the JVM-based Kafka or Go-based alternatives, LANCE leverages Rust's memory safety and io_uring to achieve near-hardware-limit I/O throughput while maintaining strict schema enforcement via TLV (Type-Length-Value) encoding.

## 2. Storage Layer Architecture

The storage engine persists data in immutable segments using a custom `.lnc` format.

### 2.1 File Naming & Segmenting

Segments are named to provide O(1) metadata discovery from the filesystem layer:

- **Active Segment**: `{start_index}_{start_timestamp_ns}.lnc`
- **Closed Segment**: `{start_index}_{start_timestamp_ns}-{end_timestamp_ns}.lnc`

### 2.2 Encoding: Structured TLV

To optimize space and ensure structural integrity, LANCE replaces raw JSON with a binary TLV format.

- **[Type: 1-byte]**: Identifies the record schema.
- **[Length: 4-bytes little-endian]**: Total payload size.
- **[Value: N-bytes]**: The encoded record data.

**Implementation**: See `lnc-core/src/tlv.rs` for core TLV encoding/decoding and `lnc-client/src/record.rs` for client-side record parsing.

### 2.3 Dual Indexing Strategy

Since TLV records are variable-width, LANCE implements a Sparse Index sidecar file.

- **Primary Index (Sequence)**: Maps every Nth record to a byte offset in the `.lnc` file.
- **Secondary Index (Timestamp)**: A binary-searchable list of timestamps mapped to record offsets.

### 2.4 Sparse Index Configuration

The sparse index balances read amplification against index file size.

**Implementation**: See `lnc-index/src/sparse.rs` for `SparseIndex` and `SparseIndexWriter` with `memmap2` zero-copy access.

#### Index Granularity

| Parameter | Default | Configurable |
|-----------|---------|--------------|
| **Primary Index (N)** | 4,096 records | Yes |
| **Secondary Index** | 1 entry per 10ms wall-clock bucket | Yes |

**Primary Index**: Every Nth record's byte offset is stored. For a seek to record `R`, LANCE reads index entry `floor(R / N)`, then scans forward up to `N-1` records.

**Trade-offs**:
- **Smaller N** → Faster seeks, larger index file
- **Larger N** → Slower seeks, smaller index file

**Recommendation**: For records averaging 256 bytes, N=4096 yields ~64 KiB of scan per seek (acceptable for SSD) and keeps index overhead under 0.1% of segment size.

#### Index File Format

```
Primary:   [{record_index: u64, byte_offset: u64}, ...]  (16 bytes per entry)
Secondary: [{timestamp_ns: u64, byte_offset: u64}, ...]  (16 bytes per entry)
```

Both indexes are memory-mapped at segment open time for O(1) lookup.

#### ⚠️ Cache Coherency: Active vs Closed Segments

**Critical constraint**: `mmap` and `io_uring` with `O_DIRECT` do **not** share cache coherency. The kernel page cache (used by mmap) and direct I/O paths are independent.

**Implementation**: See `lnc-io/src/segment.rs` for `SegmentWriter`, `SegmentReader`, `ZeroCopyReader` (mmap-based), and segment naming conventions (`rename_to_closed_segment`, `parse_segment_name`).

| Segment State | Read Method | Write Method | Why |
|---------------|-------------|--------------|-----|
| **Active** | `io_uring` read | `io_uring` write | Both use direct I/O; coherent |
| **Closed** | `mmap` | N/A (immutable) | Safe; no concurrent writes |

**Rule**: Never `mmap` the active segment while `io_uring` is appending to it. Only `mmap` closed (immutable) segments.

```rust
impl SegmentReader {
    pub fn open(path: &Path, is_closed: bool) -> Result<Self> {
        if is_closed {
            // Safe: immutable segment, use mmap for zero-copy reads
            let mmap = unsafe { Mmap::map(&File::open(path)?)? };
            Ok(Self::Mapped(mmap))
        } else {
            // Active segment: use io_uring for reads to stay coherent with writer
            Ok(Self::Direct(IoUringReader::new(path)?))
        }
    }
}
```

#### Rust Implementation: Zero-Copy Index Access (Closed Segments)

Using `memmap2` and `#[repr(C)]`, we achieve zero-parsing overhead for **closed segment indexes**:

```rust
use memmap2::Mmap;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct IndexEntry {
    pub record_index: u64,
    pub byte_offset: u64,
}

impl SparseIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { mmap })
    }

    /// Returns the index entries as a slice with zero parsing overhead.
    /// 
    /// # Safety
    /// The mmap must contain valid IndexEntry data aligned to 8 bytes.
    pub fn entries(&self) -> &[IndexEntry] {
        // SAFETY: IndexEntry is #[repr(C)] with known size/alignment.
        // The file is written by LANCE with correct layout.
        unsafe {
            std::slice::from_raw_parts(
                self.mmap.as_ptr() as *const IndexEntry,
                self.mmap.len() / std::mem::size_of::<IndexEntry>(),
            )
        }
    }
}
```

**Why this matters**: Casting a memory-mapped byte slice directly to `&[IndexEntry]` avoids deserialization. Binary search over millions of entries incurs zero allocation.

### 2.5 TLV Schema Constraints

The TLV header uses a fixed-width format optimized for parsing speed.

#### Field Sizes

| Field | Size | Range |
|-------|------|-------|
| **Type** | 1 byte | 0–255 (256 schema types) |
| **Length** | 4 bytes | 0–4 GiB per record |
| **Value** | Variable | Up to `Length` bytes |

#### Type Field Rationale

The 1-byte type field is an intentional constraint:

1. **Cache-line efficiency**: The 5-byte header fits entirely within a single cache line alongside payload data.
2. **Schema governance**: 256 types encourages schema discipline. If you need 300+ distinct message types, you likely need schema composition (nested TLV) rather than flat enumeration.
3. **Reserved ranges**: Types 0x00–0x0F are reserved for LANCE internal use (heartbeats, control frames). Types 0x10–0xFF are available for application schemas.

#### Escape Hatch

For deployments requiring more schema flexibility, the **Type = 0xFF** value signals an **Extended Header**:

```
[Type: 0xFF] [Extended Type: 2 bytes] [Length: 4 bytes] [Value: N bytes]
```

This allows up to 65,536 types at the cost of 2 additional header bytes per record.

#### Parser Optimization: Branch Prediction

Since 99%+ of traffic uses standard headers (Type ≠ 0xFF), the parser must hint this to the CPU:

```rust
#[inline(always)]
pub fn parse_header(buf: &[u8]) -> Result<Header> {
    let type_byte = buf[0];
    
    // Hint: extended headers are rare
    if std::intrinsics::likely(type_byte != 0xFF) {
        // Fast path: standard 5-byte header
        let length = u32::from_le_bytes(buf[1..5].try_into()?);
        Ok(Header::Standard { type_id: type_byte, length })
    } else {
        // Slow path: extended 7-byte header
        let ext_type = u16::from_le_bytes(buf[1..3].try_into()?);
        let length = u32::from_le_bytes(buf[3..7].try_into()?);
        Ok(Header::Extended { type_id: ext_type, length })
    }
}
```

**Note**: `std::intrinsics::likely` requires `#![feature(core_intrinsics)]` on nightly. For stable Rust, use the `likely_stable` crate or rely on PGO (Profile-Guided Optimization) to achieve similar results.

## 3. Memory & I/O Management

### 3.1 Real-time Heap Tracking

LANCE utilizes a custom Global Allocator wrapper. This provides the system with a deterministic "Pressure Signal" to trigger flushes to disk before the OS invokes the OOM killer or forces synchronous paging.

### 3.2 Non-Blocking I/O (io_uring)

To bypass the overhead of standard syscalls and Go-style context switching:

- **Submission Queue (SQ)**: The write thread pushes batches of TLV records directly to the kernel.
- **Completion Queue (CQ)**: A background poller handles ACKs, updating the "High Water Mark" for replicators.

## 4. Replication & State Machine

### 4.1 Consistency Model

LANCE supports two replication modes:

- **Standalone (L1)**: Single-node, no replication. Optimized for maximum throughput with local durability only. Ideal for development, testing, or workloads where replication is handled externally.
- **Quorum (L3)**: Filesystem-consistent replication with quorum-based durability. Requires M/2+1 nodes to confirm the write before the producer is ACKed. All nodes maintain **byte-identical** segment files on disk.

**Implementation**: See `lnc-replication/src/mode.rs` for the `ReplicationMode` enum and `lnc-replication/src/quorum.rs` for quorum management.

#### 4.1.1 Filesystem-Consistent Replication (L3)

In L3 mode, all cluster nodes maintain **byte-identical** `.lnc` segment files. The leader dictates all segment operations (creation, writing, rotation), and followers mirror the leader's exact file layout.

**Design goals**:
- Files can be `rsync`'d between nodes for bootstrapping or migration (solo → cluster)
- "Hot copy" of closed segments to pre-seed new cluster nodes (only catch up on active segment)
- Troubleshooting via `diff` across nodes — files must be identical
- Consumer byte offsets are globally consistent because all nodes share the same segment layout

**Enriched replication wire format**:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    DATA REPLICATION MESSAGE (Leader → Followers)              │
├──────────┬──────────────┬──────────────┬───────────────┬─────────┬──────────┤
│ topic_id │ global_offset│ segment_name │ write_offset  │  flags  │ payload  │
│ (4 bytes)│   (8 bytes)  │ (2B len+name)│   (8 bytes)   │ (1 byte)│ (N bytes)│
└──────────┴──────────────┴──────────────┴───────────────┴─────────┴──────────┘

Flags byte:
  Bit 0: ROTATE_AFTER  — follower must rotate segment after this write
  Bit 1: NEW_SEGMENT   — follower must create this exact segment file
```

**Replication invariant**: After every write, followers validate:
`local_cumulative_offset == leader_global_offset`

Any mismatch is a fatal inconsistency that triggers an immediate alert and follower resync.

#### 4.1.2 Global Offset

The **global offset** is a monotonically-increasing byte counter per topic, maintained by the leader. Because all nodes have byte-identical segment files, the cumulative byte position IS the global offset — it's the same on every node.

Making the offset explicit (rather than just an emergent property of identical files) provides:
- **Validation**: Followers verify their local byte position matches the leader's claimed global offset
- **Protocol clarity**: Consumers fetch by "global offset", not "node-local byte position"
- **Safety net**: If anything ever breaks filesystem consistency, the offset layer catches it immediately
- **Future flexibility**: Offset-based retention/compaction without breaking consumers

#### 4.1.3 Leader-Dictated Segment Management

In L3 mode, followers **never** independently create or rotate segments. All segment lifecycle is driven by the leader:

1. **Leader creates segment** → sends `NEW_SEGMENT` flag with exact filename to followers
2. **Leader writes data** → sends payload with `(segment_name, write_offset, global_offset)` to followers
3. **Leader rotates segment** → sends `ROTATE_AFTER` flag; followers seal and rotate to the leader-specified new segment
4. **Followers validate** → after each write, assert `local_cumulative_offset == global_offset`

This ensures all nodes have identical segment files at all times.

#### ⚠️ The "Slow Follower" Poison (L3 Tail Latency Trap)

**The Problem**: In L3 mode with M/2+1 quorum, if one follower is experiencing a "Stop the World" event (disk failure, kernel panic, GC pause) and another is slightly laggy, your ingestion latency spikes to the timeout limit of the slowest required member.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    L3 QUORUM WITH SLOW FOLLOWER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Leader (writes locally)                                                    │
│      │                                                                       │
│      ├──► Follower A: ACK in 1ms   ✓                                        │
│      ├──► Follower B: ACK in 2ms   ✓  ← Quorum reached (2/3)                │
│      └──► Follower C: ACK in 850ms ✗  ← Disk thrashing, but we don't wait  │
│                                                                              │
│   BUT if Follower A is down:                                                 │
│      │                                                                       │
│      ├──► Follower A: TIMEOUT (down)                                        │
│      ├──► Follower B: ACK in 2ms   ✓                                        │
│      └──► Follower C: ACK in 850ms ✓  ← Must wait! Quorum requires 2/3     │
│                                                                              │
│   Ingestion latency: 850ms (P99 disaster)                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Adaptive Eviction Policy

To prevent slow followers from poisoning ingestion latency, LANCE implements **dynamic quorum reconfiguration**:

```rust
pub struct FollowerHealth {
    node_id: u16,
    /// Rolling window of ACK latencies (last 100 batches)
    latencies: VecDeque<Duration>,
    /// Consecutive batches exceeding P99 threshold
    consecutive_slow: u32,
    /// Current status
    status: FollowerStatus,
}

#[derive(Clone, Copy, PartialEq)]
pub enum FollowerStatus {
    Healthy,
    Degraded,   // Exceeding P99 but still in quorum
    Evicted,    // Removed from quorum calculation
}

impl ReplicationActor {
    const P99_THRESHOLD: Duration = Duration::from_millis(10);
    const EVICTION_THRESHOLD: u32 = 3;  // 3 consecutive slow batches
    const RECOVERY_WINDOW: u32 = 10;    // 10 fast batches to rejoin
    
    pub fn update_follower_health(&mut self, node_id: u16, latency: Duration) {
        let follower = self.followers.get_mut(&node_id).unwrap();
        follower.latencies.push_back(latency);
        if follower.latencies.len() > 100 {
            follower.latencies.pop_front();
        }
        
        if latency > Self::P99_THRESHOLD {
            follower.consecutive_slow += 1;
            
            if follower.consecutive_slow >= Self::EVICTION_THRESHOLD 
               && follower.status != FollowerStatus::Evicted 
            {
                // Evict from quorum
                follower.status = FollowerStatus::Evicted;
                self.recalculate_quorum();
                
                warn!(
                    target: "lance::replication",
                    node_id = node_id,
                    latency_ms = latency.as_millis(),
                    "Follower evicted from quorum due to latency"
                );
                metrics::increment!("lance_follower_evictions");
            }
        } else {
            // Fast response - check for recovery
            if follower.status == FollowerStatus::Evicted {
                follower.consecutive_slow = 0;
                // Count consecutive fast responses
                let recent_fast = follower.latencies.iter()
                    .rev()
                    .take(Self::RECOVERY_WINDOW as usize)
                    .filter(|&l| *l <= Self::P99_THRESHOLD)
                    .count();
                
                if recent_fast >= Self::RECOVERY_WINDOW as usize {
                    follower.status = FollowerStatus::Healthy;
                    self.recalculate_quorum();
                    
                    info!(
                        target: "lance::replication",
                        node_id = node_id,
                        "Follower recovered, rejoining quorum"
                    );
                    metrics::increment!("lance_follower_recoveries");
                }
            } else {
                follower.consecutive_slow = 0;
            }
        }
    }
    
    fn recalculate_quorum(&mut self) {
        let healthy_count = self.followers.values()
            .filter(|f| f.status != FollowerStatus::Evicted)
            .count();
        
        // Quorum is M/2+1 of HEALTHY nodes, not total nodes
        self.required_acks = (healthy_count / 2) + 1;
        
        // Safety: Never allow quorum < 1 (always require at least local + 1 ACK)
        self.required_acks = self.required_acks.max(1);
        
        info!(
            target: "lance::replication",
            healthy_nodes = healthy_count,
            required_acks = self.required_acks,
            "Quorum recalculated"
        );
    }
}
```

#### Asynchronous ACK Release

**Critical**: The io_uring poller must NEVER block waiting for network ACKs. The write path is decoupled:

```rust
/// io_uring Poller writes to local segment immediately
/// Replication Actor handles quorum asynchronously
pub struct WriteResult {
    batch_id: u64,
    local_offset: u64,
    /// Channel to signal when quorum is reached
    quorum_tx: oneshot::Sender<QuorumResult>,
}

impl IoUringPoller {
    pub fn handle_write_completion(&mut self, batch: LoanableBatch) -> WriteResult {
        // Local write is DONE - return batch to pool immediately
        let offset = self.current_offset;
        self.pool.return_batch(batch);
        
        // Create channel for async quorum notification
        let (tx, rx) = oneshot::channel();
        
        // Send to Replication Actor (non-blocking)
        self.replication_tx.try_send(ReplicationRequest {
            batch_id: batch.batch_id,
            offset,
            quorum_tx: tx,
        }).ok();  // Drop if channel full (backpressure)
        
        WriteResult { batch_id: batch.batch_id, local_offset: offset, quorum_tx: tx }
    }
}

impl ReplicationActor {
    pub async fn handle_replication(&mut self, req: ReplicationRequest) {
        // Send to followers in parallel
        let futures: Vec<_> = self.healthy_followers()
            .map(|f| self.send_to_follower(f, &req))
            .collect();
        
        // Wait for quorum (with timeout)
        let timeout = Duration::from_millis(100);
        let mut acks = 0;
        
        for result in futures::future::join_all(futures).await {
            match result {
                Ok(latency) => {
                    acks += 1;
                    self.update_follower_health(result.node_id, latency);
                    
                    if acks >= self.required_acks {
                        // Quorum reached - notify client
                        let _ = req.quorum_tx.send(QuorumResult::Success);
                        return;
                    }
                }
                Err(e) => {
                    self.update_follower_health(result.node_id, timeout);
                }
            }
        }
        
        // Quorum not reached
        let _ = req.quorum_tx.send(QuorumResult::Failed);
    }
}
```

#### L3 Replication Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `lance_replication_ack_latency_ns` | Histogram | Per-follower ACK latency |
| `lance_follower_evictions` | Counter | Followers removed from quorum |
| `lance_follower_recoveries` | Counter | Followers rejoining quorum |
| `lance_quorum_size` | Gauge | Current required ACKs |
| `lance_healthy_followers` | Gauge | Followers in healthy state |
| `lance_global_offset` | Gauge | Current global offset per topic |
| `lance_replication_offset_mismatch` | Counter | Follower offset validation failures |

### 4.2 Distributed Tie-Breaking

In a multi-node write scenario, the Node ID is embedded in the record metadata.

- **Conflict Resolution**: If two nodes produce an event at the exact same nanosecond, the node with the lower ID (e.g., lance-1) wins the index assignment.
- **Triage**: The Node ID in the log allows for immediate identification of the origin during post-mortem analysis.

### 4.3 Timestamp Collision Handling

When two nodes produce events at the exact same nanosecond, LANCE uses a **128-bit Composite Sort Key** for deterministic ordering without modifying the original timestamp.

#### Sort Key Structure

Instead of re-timestamping (which risks ordering violations with legitimately later events), we use a composite key:

```rust
#[repr(C)]
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SortKey {
    pub timestamp_ns: u64,   // 64 bits: nanosecond timestamp
    pub node_id: u16,        // 16 bits: node identifier (lower wins)
    pub sequence: u48,       // 48 bits: per-node monotonic counter
}

// Packed as 128 bits for atomic compare-and-swap
impl SortKey {
    pub fn to_u128(&self) -> u128 {
        ((self.timestamp_ns as u128) << 64)
            | ((self.node_id as u128) << 48)
            | (self.sequence as u128)
    }
}
```

#### Resolution Algorithm

1. **Ingestion Actor receives batch**: Each record is assigned a `SortKey` using the creation timestamp, node ID, and a per-node monotonic sequence counter.
2. **In-memory sort**: Before io_uring submission, the batch is sorted by `SortKey`. This is an in-place sort with no reallocation.
3. **File is source of truth**: The `.lnc` segment stores records in `SortKey` order. The original `timestamp_ns` is preserved in the payload for consumers.

#### Why Monotonic Sequence?

In a 100Gbps environment, nanosecond collisions are common (multiple events within the same ns). The 48-bit sequence counter:

- **Never exhausts**: 2^48 = 281 trillion events per nanosecond per node
- **No re-submission**: Events are never re-queued, eliminating the cascade risk
- **Preserves causality**: Events from the same node maintain arrival order

#### Storage Strategy: SortKey in Index, Not Record

**Problem**: A 16-byte SortKey + 5-byte TLV header = 21 bytes overhead. For small records (64-byte telemetry), this is ~33% metadata bloat.

**Solution**: Store SortKey in the **Sparse Index only**, not in every TLV record on disk.

| Location | What's Stored | Size | Purpose |
|----------|---------------|------|--------|
| **TLV Record** | `[Type:1][Length:4][Value:N]` | 5 + N bytes | Minimal on-disk footprint |
| **Sparse Index** | `[SortKey:16][ByteOffset:8]` | 24 bytes per entry | Ordering + seek |
| **Record Payload** | `original_ts` (optional) | 8 bytes | Consumer timestamp queries |

**On-disk layout**:

```
Segment (.lnc):        [TLV][TLV][TLV][TLV]...     ← Records in SortKey order, no SortKey stored
Sparse Index (.idx):   [SortKey|Offset][SortKey|Offset]...  ← Every Nth record
```

**Trade-off**: If you need per-record audit trails (which node wrote this exact record), embed `node_id` (2 bytes) in the TLV payload. The full SortKey is only needed for ordering, which the index provides.

```rust
// TLV payload for audit-required records
#[derive(Serialize, Deserialize)]
pub struct AuditableRecord {
    pub original_ts: u64,   // 8 bytes: creation timestamp
    pub node_id: u16,       // 2 bytes: origin node
    pub payload: Vec<u8>,   // N bytes: actual data
}
// Total overhead: 10 bytes (vs 16 for full SortKey)
```

#### Index Implications

The Sparse Index stores **SortKey** for ordering, but the Secondary Index (timestamp-based) uses `original_ts` from the payload:

- **Primary Index**: `[SortKey:16][ByteOffset:8]` — for sequential access and ordering
- **Secondary Index**: `[original_ts:8][ByteOffset:8]` — for time-range queries

This means:

- **Queries by time** return events in creation-time order (expected by consumers)
- **Internal ordering** uses `SortKey` for total ordering across nodes
- **Disk I/O savings**: ~6 bytes per record for small payloads (~10% reduction)

#### ⚠️ The Secondary Index "Local Overlap" Problem

**The Problem**: Since batches are sorted by `SortKey` (not `original_ts`), the on-disk `original_ts` values are **mostly monotonic but with local jitter**. A binary search in the Secondary Index may land on a page where adjacent records have non-monotonic timestamps due to SortKey tie-breaking.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│               SECONDARY INDEX LOCAL OVERLAP SCENARIO                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   On-disk order (sorted by SortKey):                                        │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Record 0: original_ts=1000, SortKey=(1000, node=1, seq=0)           │   │
│   │ Record 1: original_ts=1000, SortKey=(1000, node=2, seq=0)  ← same ts│   │
│   │ Record 2: original_ts=999,  SortKey=(999,  node=3, seq=5)  ← LOWER! │   │
│   │ Record 3: original_ts=1001, SortKey=(1001, node=1, seq=1)           │   │
│   │ Record 4: original_ts=1000, SortKey=(1000, node=3, seq=6)  ← jitter │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│   Query: "Find all records where original_ts >= 1000"                       │
│   Binary search lands on Record 1 (ts=1000)                                 │
│   Naive forward scan misses Record 4 (ts=1000) which comes AFTER Record 2   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Solution: Peek Window for Time-Range Queries

The `SegmentReader` must "peek" beyond the binary search result to catch records displaced by SortKey tie-breaking:

```rust
/// Maximum timestamp jitter window (nanoseconds)
/// This should be >= max clock skew between nodes + max batch latency
const TIMESTAMP_JITTER_WINDOW_NS: u64 = 10_000_000; // 10ms

impl SegmentReader {
    /// Find all records in [start_ts, end_ts] range
    /// Handles local overlap due to SortKey ordering
    pub fn query_time_range(
        &self,
        start_ts: u64,
        end_ts: u64,
    ) -> impl Iterator<Item = Record> + '_ {
        // Binary search for approximate start position
        // Search for (start_ts - JITTER_WINDOW) to catch displaced records
        let search_ts = start_ts.saturating_sub(TIMESTAMP_JITTER_WINDOW_NS);
        let start_idx = self.secondary_index.binary_search_by(|entry| {
            entry.timestamp_ns.cmp(&search_ts)
        }).unwrap_or_else(|i| i);
        
        // Scan forward, collecting records in range
        // Continue past end_ts by JITTER_WINDOW to catch displaced records
        let scan_end_ts = end_ts.saturating_add(TIMESTAMP_JITTER_WINDOW_NS);
        
        self.records_from(start_idx)
            .take_while(move |r| r.original_ts <= scan_end_ts)
            .filter(move |r| r.original_ts >= start_ts && r.original_ts <= end_ts)
    }
    
    /// Point query: find record(s) at exact timestamp
    pub fn query_exact_timestamp(&self, ts: u64) -> Vec<Record> {
        // Must scan window around target timestamp
        self.query_time_range(ts, ts).collect()
    }
}
```

#### Calculating the Jitter Window

The jitter window must account for:

| Factor | Typical Value | Description |
|--------|---------------|-------------|
| **Clock skew** | 1-5ms | NTP-synchronized nodes |
| **Batch latency** | 1-10ms | Time records sit in batch before flush |
| **Network jitter** | 0-2ms | Variance in replication latency |

**Conservative default**: `TIMESTAMP_JITTER_WINDOW_NS = 10_000_000` (10ms)

For tighter windows, deploy with PTP (Precision Time Protocol) and reduce batch timeouts.

#### Secondary Index Ordering Guarantee

**Important**: The Secondary Index itself is NOT sorted by `original_ts`. It's in the same order as the Primary Index (SortKey order). The binary search finds an approximate position, then linear scan with filtering is required.

If sorted Secondary Index is required for faster queries, build it as a **separate B-tree** during segment close:

```rust
/// Optional: Build sorted timestamp index for closed segments
pub fn build_sorted_timestamp_index(segment: &ClosedSegment) -> BTreeMap<u64, Vec<u64>> {
    let mut index: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    
    for (offset, record) in segment.records() {
        index.entry(record.original_ts)
            .or_default()
            .push(offset);
    }
    
    index
}
```

This adds memory overhead but eliminates the jitter window scan for read-heavy workloads.

### 4.4 Cluster Topology Requirements

LANCE's replication modes have specific cluster size requirements.

#### Minimum Cluster Sizes

| Replication Mode | Minimum Nodes | Quorum Formula | Notes |
|------------------|---------------|----------------|-------|
| **L1 (Standalone)** | 1 | N/A (no quorum) | Single-node, no replication |
| **L3 (Quorum)** | 3 | `floor(M/2) + 1` | Filesystem-consistent replication; tolerates 1 node failure |

#### Two-Node Deployments

A 2-node L3 cluster is **not recommended** because:

- Quorum = `floor(2/2) + 1 = 2` → Both nodes must ACK every write
- A single node failure halts all writes

For 2-node HA, use external replication (e.g., `rsync` of closed segments from an L1 node to a standby) with manual failover.

#### Recommended Topologies

| Use Case | Nodes | Mode | Fault Tolerance |
|----------|-------|------|-----------------|
| Dev/Test | 1 | L1 | None |
| Production (durability) | 3 | L3 | 1 node |
| Mission-critical | 5 | L3 | 2 nodes |

## 5. Concurrency Model

LANCE adopts an **Actor-per-Resource** concurrency model to avoid shared mutable state on the hot path.

### 5.1 Thread Architecture

| Thread | Responsibility | Communication | Crate |
|--------|----------------|---------------|-------|
| **Network Acceptor** | Accepts TCP connections, spawns per-client handlers | `tokio` async runtime | `tokio` |
| **Ingestion Actor(s)** | Owns partition of write buffer; TLV encode + SortKey | Bounded MPSC channel | `flume` |
| **io_uring Poller(s)** | Pinned OS thread; submits SQEs, harvests CQEs | Lock-free queue | `crossbeam` or `ringbuf` |
| **Replication Actor** | Manages L3 quorum replication state machine | `tokio` async runtime | `tokio` |

#### ⚠️ Scaling Consideration: Single vs Multi-Core Ingestion

At 100Gbps, a single Ingestion Actor becomes a bottleneck (SortKey assignment + TLV encoding saturates one core).

| Deployment Scale | Ingestion Actors | Handoff Queue | io_uring Pollers |
|------------------|------------------|---------------|------------------|
| **< 10Gbps** | 1 | SPSC (`ringbuf`) | 1 |
| **10-50Gbps** | N (CPU cores / 2) | MPSC (`flume`) | 1 |
| **> 50Gbps** | N (CPU cores / 2) | MPMC (`crossbeam::ArrayQueue`) | M (1 per NVMe) |

**Multi-Actor Design**:

```rust
// Network handler partitions by hash(client_id) to preserve per-client ordering
let actor_idx = hash(client_id) % ingestion_actors.len();
ingestion_actors[actor_idx].send(record).await?;

// Each Ingestion Actor has its own MonotonicSequence counter
// SortKey includes actor_id to maintain global uniqueness:
// [timestamp:64][node_id:16][actor_id:8][sequence:40]
```

**MPMC Handoff** (for multi-actor → multi-poller):

```rust
use crossbeam::queue::ArrayQueue;

// Bounded MPMC queue: multiple ingestion actors, multiple io_uring pollers
let handoff: Arc<ArrayQueue<Batch>> = Arc::new(ArrayQueue::new(8192));

// Producer (Ingestion Actor)
match handoff.push(batch) {
    Ok(()) => { /* success */ }
    Err(batch) => {
        // Queue full: backpressure
        metrics::increment!("backpressure.handoff_full");
    }
}

// Consumer (io_uring Poller) - spins, never parks
loop {
    if let Some(batch) = handoff.pop() {
        submit_to_iouring(&batch)?;
    } else {
        std::hint::spin_loop();
    }
}
```

#### Crate Selection Rationale

| Channel Type | Scale | Crate | Why |
|--------------|-------|-------|-----|
| **Network → Ingestion** | All | `flume` | Async-compatible MPSC with backpressure |
| **Ingestion → io_uring** | Single-core | `ringbuf` | SPSC; poller never parks |
| **Ingestion → io_uring** | Multi-core | `crossbeam::ArrayQueue` | MPMC; bounded, lock-free |
| **Replication** | All | `tokio::sync::mpsc` | Already in async context |

### 5.2 Data Flow

**Single-core mode**:
```
Client → [TCP] → Network Handler → [flume] → Ingestion Actor → [ringbuf] → io_uring Poller → Kernel
```

**Multi-core mode**:
```
                                    ┌→ Ingestion Actor 0 →┐
Client → [TCP] → Network Handler →┼→ Ingestion Actor 1 →┼→ [ArrayQueue] →┼→ io_uring Poller 0 → NVMe 0
                                    └→ Ingestion Actor N →┘                  └→ io_uring Poller 1 → NVMe 1
```

Each arrow represents a **handoff**, not shared memory. In multi-core mode, the ArrayQueue enables work-stealing across pollers.

### 5.3 Why Not Pure Async?

`io_uring` completion polling doesn't integrate cleanly with `tokio`'s reactor. Mixing them introduces latency spikes from runtime contention. By isolating the io_uring loop on a dedicated, pinned thread, we achieve deterministic I/O latency.

## 6. Backpressure & Flow Control

LANCE implements a **multi-stage backpressure** system to prevent unbounded memory growth and provide graceful degradation under load.

**Implementation**: See `lnc-core/src/backpressure.rs` for `BackpressureMonitor`, `BackpressureConfig`, `BackpressureLevel`, and `BackpressureGuard`.

### 6.1 Pressure Signals

| Signal | Threshold | Action |
|--------|-----------|--------|
| **Heap Pressure** | 80% of configured limit | Begin async flush of oldest batches |
| **Heap Critical** | 95% of configured limit | Block new writes until flush completes |
| **Queue Depth** | Ingestion channel > 10,000 entries | Yield to producer (busy-wait backoff) |
| **Disk Latency** | io_uring CQE latency > 10ms (P99) | Reduce batch size, signal slow-consumer |

### 6.2 Producer Notification

Producers receive backpressure via two mechanisms:

1. **Synchronous (L3 mode)**: The ACK is delayed until the write is durably committed to a quorum. Slow ACKs naturally throttle the producer.

2. **Asynchronous (L1 mode)**: A dedicated `Backpressure` frame is sent to the client when `Queue Depth` or `Heap Critical` thresholds are breached. Clients should implement exponential backoff upon receiving this signal.

### 6.3 Bounded Queues

All internal channels are **bounded**:

```
Ingestion Channel: capacity = 16,384 entries  (flume::bounded)
io_uring SQ:       capacity = 4,096 entries   (kernel-side)
io_uring Handoff:  capacity = 8,192 entries   (ringbuf::HeapRb)
Replication Queue: capacity = 8,192 entries   (tokio::sync::mpsc)
```

#### Handoff Strategy

**Ingestion → io_uring** uses `ringbuf::HeapRb` (lock-free SPSC):

```rust
use ringbuf::{HeapRb, traits::{Producer, Consumer}};

// Ingestion Actor side
let rb = HeapRb::<BatchRef>::new(8192);
let (mut producer, consumer) = rb.split();

// Push batch (never blocks, returns Err if full)
match producer.try_push(batch) {
    Ok(()) => { /* success */ }
    Err(batch) => {
        // Ring full: apply backpressure to upstream
        metrics::increment!("backpressure.ringbuf_full");
        std::hint::spin_loop();  // Yield CPU briefly
    }
}
```

**Why `ringbuf` for io_uring handoff?**

- The io_uring poller is a **dedicated, pinned OS thread**
- It must never park (waiting on a mutex) or allocate (triggering global allocator contention)
- `ringbuf` provides true lock-free SPSC semantics with zero allocation after init

## 7. Crash Recovery & Durability

LANCE provides **segment-level durability** with explicit trade-offs for the active segment.

**Implementation**: See `lnc-recovery/src/segment_recovery.rs` for segment scanning and truncation, and `lnc-recovery/src/wal_replay.rs` for WAL replay.

### 7.1 Durability Guarantees

| Segment State | Durability | Recovery Behavior |
|---------------|------------|-------------------|
| **Closed Segment** | Fully durable | Immutable; verified via trailing checksum |
| **Active Segment (L3)** | Durable after quorum ACK | Recoverable up to last ACKed write |
| **Active Segment (L1)** | Best-effort | Data between last `fsync` and crash is lost |

### 7.2 Active Segment Recovery

On startup, LANCE performs the following recovery sequence:

1. **Scan Active Segment**: Read from byte 0 until a TLV parse failure or EOF.
2. **Checksum Validation**: Each record includes a trailing CRC-32C. Invalid checksums indicate a torn write.
3. **Truncation**: The segment is truncated to the last valid record boundary.
4. **Index Rebuild**: The sparse index sidecar is regenerated from the recovered segment.

### 7.3 Write-Ahead Logging (Optional)

For deployments requiring stronger L1 durability, an optional **micro-WAL** can be enabled:

```
Config: wal_mode = "enabled" | "disabled" (default: disabled)
```

When enabled, each batch is first written to a small, pre-allocated WAL file (default: 64 MiB). The WAL is `fsync`'d before the batch is considered "submitted." On crash recovery, the WAL is replayed into the active segment.

**Trade-off**: Enabling WAL adds ~1 `fsync` per batch, increasing latency but guaranteeing no data loss for acknowledged L1 writes.

#### Pre-allocation Optimization

A common source of latency spikes in write-heavy systems is **metadata allocation**—the filesystem must find and reserve blocks during the write. We eliminate this by pre-allocating the WAL at startup:

```rust
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;

const WAL_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB

pub fn create_wal(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    
    // Pre-allocate the full WAL size
    // This forces the filesystem to allocate blocks NOW, not during hot-path writes
    file.set_len(WAL_SIZE)?;
    
    // Optional: advise the kernel we'll write sequentially
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, WAL_SIZE as i64, libc::POSIX_FADV_SEQUENTIAL);
        }
    }
    
    Ok(file)
}
```

**Why this matters**:

- `set_len()` pre-allocates disk blocks, eliminating allocation latency during writes
- `posix_fadvise(SEQUENTIAL)` hints the kernel to optimize readahead/writeback
- Combined, this reduces P99 write latency by 10-50% compared to grow-on-demand

#### WAL Recycling

Instead of deleting and recreating the WAL after each flush, we **recycle**:

1. Reset the write cursor to byte 0
2. Write a "WAL invalidated" marker at the header
3. Continue writing from byte 0

This avoids the overhead of `unlink()` + `create()` + `set_len()` on every segment rotation.

## 8. io_uring Feature Baseline

LANCE targets a specific io_uring feature set to balance performance with deployment compatibility.

**Implementation**: See `lnc-io/src/uring.rs` for `IoUringBackend`, `IoUringPoller`, `RegisteredBufferPool`, and zero-copy operations (`ZeroCopySender`, `SpliceForwarder`, `TeeForwarder`).

### 8.1 Required Kernel Features

| Feature | Minimum Kernel | Usage in LANCE |
|---------|----------------|----------------|
| `io_uring` base | Linux 5.1 | Core async I/O |
| `IORING_OP_WRITE` | Linux 5.1 | Segment writes |
| `IORING_OP_READ` | Linux 5.1 | Segment reads |
| `IORING_OP_FSYNC` | Linux 5.1 | Durability flush |
| `IORING_FEAT_FAST_POLL` | Linux 5.7 | Reduced syscall overhead |
| `IORING_SETUP_COOP_TASKRUN` | Linux 5.19 | Avoid IPI interrupts (optional) |

### 8.2 Runtime Capability Detection

At startup, LANCE probes the kernel for supported features:

```rust
let features = io_uring::probe()?;
if !features.is_supported(IORING_OP_WRITE) {
    return Err(LanceError::UnsupportedKernel("io_uring WRITE not available"));
}
```

### 8.3 Fallback Strategy

If io_uring is unavailable (e.g., containerized environments with restricted syscalls), LANCE falls back to **`pwritev2`** with `RWF_DSYNC`. This path is significantly slower but maintains correctness.

```
Config: io_backend = "io_uring" | "pwritev2" | "auto" (default: auto)
```

### 8.4 Recommended Deployment

| Environment | Recommendation |
|-------------|----------------|
| Bare metal / VM (Linux 5.10+) | `io_uring` (full feature set) |
| Container (Linux 5.1–5.6) | `io_uring` (reduced features) |
| Container (seccomp-restricted) | `pwritev2` fallback |
| Non-Linux | Not supported |
## 9. Graceful Shutdown & Signal Handling

LANCE must handle process termination gracefully to prevent data loss and ensure cluster stability during rolling restarts, scaling events, or crashes.

### 9.1 Signal Handling Strategy

| Signal | Source | LANCE Behavior |
|--------|--------|----------------|
| `SIGTERM` | Kubernetes preStop, `kill` | Begin graceful drain (default: 30s timeout) |
| `SIGINT` | Ctrl+C, interactive shutdown | Same as SIGTERM |
| `SIGQUIT` | Core dump request | Flush active segment, then dump |
| `SIGKILL` | Force kill (untrappable) | No handling possible; rely on crash recovery |

### 9.2 Drain Sequence

Upon receiving `SIGTERM`/`SIGINT`, LANCE executes a deterministic shutdown:

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. STOP ACCEPTING     │ Close TCP listener, reject new connections │
├───────────────────────┼─────────────────────────────────────────────┤
│ 2. DRAIN INGESTION    │ Flush in-flight batches to io_uring SQ      │
├───────────────────────┼─────────────────────────────────────────────┤
│ 3. AWAIT CQEs         │ Wait for all io_uring completions (bounded) │
├───────────────────────┼─────────────────────────────────────────────┤
│ 4. FSYNC ACTIVE       │ fsync active segment + index sidecars       │
├───────────────────────┼─────────────────────────────────────────────┤
│ 5. NOTIFY CLUSTER     │ Broadcast "leaving" to replication peers    │
├───────────────────────┼─────────────────────────────────────────────┤
│ 6. CLOSE FILES        │ Drop file handles (RAII triggers cleanup)   │
├───────────────────────┼─────────────────────────────────────────────┤
│ 7. EXIT 0             │ Clean exit code for orchestrator            │
└─────────────────────────────────────────────────────────────────┘
```

### 9.3 Implementation

**Implementation**: See `lance/src/shutdown.rs` for the complete graceful shutdown implementation including:
- `ShutdownSignal` for cross-platform signal handling (Unix SIGTERM/SIGINT, Windows Ctrl+C)
- `DrainCoordinator` for orchestrating the drain sequence
- `DRAIN_TIMEOUT` constant (25 seconds) for Kubernetes compatibility
- In-flight operation tracking via `InFlightOps`

### 9.4 Drain Timeout & Force Exit

If the drain sequence exceeds the configured timeout (default: 25s, leaving 5s buffer for K8s SIGKILL):

1. Log warning with in-flight batch count
2. Abort pending io_uring SQEs
3. Mark active segment as "dirty" (triggers recovery scan on restart)
4. Exit with code 1 (signals unclean shutdown to orchestrator)

**Implementation**: The `DrainCoordinator::drain_with_timeout()` method in `lance/src/shutdown.rs` implements this behavior with the `DRAIN_TIMEOUT` constant.

### 9.5 Replication Impact

When a node leaves the cluster:

| Replication Mode | Behavior |
|------------------|----------|
| **L1 (Standalone)** | Single-node; no cluster impact |
| **L3 (Quorum)** | Quorum recalculates; if below M/2+1, writes block until node returns or is replaced |

The "leaving" broadcast (Step 5) allows peers to immediately update their view rather than waiting for heartbeat timeout (faster failover).
## 10. Container Deployment & Ephemeral Integrity

LANCE is distributed as a Docker container, designed to run on any container runtime. Kubernetes is a supported orchestration platform but not a requirement.

### 10.1 Docker-First Design

The official LANCE image is the primary distribution mechanism:

```dockerfile
FROM rust:1.75-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y liburing2 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/lance /usr/local/bin/lance

# Non-root user for security
RUN useradd -r -u 1000 lance
USER lance

EXPOSE 1992 1993 9090 8080
ENTRYPOINT ["/usr/local/bin/lance"]
```

#### Network Ports

| Port | Protocol | Purpose | Default |
|------|----------|---------|---------|
| **1992** | TCP | LWP client connections (producers/consumers) | `listen_addr` |
| **1993** | TCP | Replication (inter-node Raft/log sync) | `replication_addr` |
| **9090** | HTTP | Prometheus metrics scrape endpoint | `metrics_addr` |
| **8080** | HTTP | Health checks (`/health/live`, `/health/ready`) | `health_addr` |
| **4317** | gRPC | OTLP telemetry export (outbound to collector) | `otel_endpoint` |

**Note**: Port 4317 is an *outbound* connection to your OTEL collector, not a listening port on LANCE.

#### Running with Docker

```bash
# Single node (development)
docker run -d \
  --name lance-0 \
  -e LANCE_NODE_ID=0 \
  -e LANCE_DATA_DIR=/data \
  -v lance-data:/data \
  -p 1992:1992 \
  -p 9090:9090 \
  -p 8080:8080 \
  ghcr.io/lance/lance:latest

# Multi-node cluster (Docker Compose)
docker compose up -d
```

#### Docker Compose (3-Node Cluster)

```yaml
version: '3.8'
services:
  lance-0:
    image: ghcr.io/lance/lance:latest
    hostname: lance-0
    environment:
      LANCE_NODE_ID: 0
      LANCE_PEERS: "lance-1:1992,lance-2:1992"
      LANCE_DATA_DIR: /data
    volumes:
      - lance-0-data:/data
    ports:
      - "1992:1992"   # LWP client port
      - "9090:9090"   # Prometheus metrics
      - "8080:8080"   # Health checks
    stop_grace_period: 30s  # Match DRAIN_TIMEOUT
    
  lance-1:
    image: ghcr.io/lance/lance:latest
    hostname: lance-1
    environment:
      LANCE_NODE_ID: 1
      LANCE_PEERS: "lance-0:1992,lance-2:1992"
      LANCE_DATA_DIR: /data
    volumes:
      - lance-1-data:/data
    stop_grace_period: 30s
    
  lance-2:
    image: ghcr.io/lance/lance:latest
    hostname: lance-2
    environment:
      LANCE_NODE_ID: 2
      LANCE_PEERS: "lance-0:1992,lance-1:1992"
      LANCE_DATA_DIR: /data
    volumes:
      - lance-2-data:/data
    stop_grace_period: 30s

volumes:
  lance-0-data:
  lance-1-data:
  lance-2-data:
```

### 10.2 Container Lifecycle Integration

LANCE responds correctly to standard container signals regardless of orchestrator:

| Event | Signal | LANCE Response |
|-------|--------|----------------|
| **Container start** | N/A | Replay WAL (if enabled), rebuild sparse indexes if dirty |
| **Graceful stop** | SIGTERM | Drain sequence (Section 9.2) |
| **Force stop** | SIGKILL | Recovery scan on restart |
| **Health check** | HTTP GET | `/health/live`, `/health/ready` endpoints |

### 10.3 Storage Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   LANCE Container                            │
├─────────────────────────────────────────────────────────────┤
│  /data/segments/   ← Volume mount (durable)                 │
│  /data/wal/        ← Volume mount (depends on wal_mode)     │
│  /tmp/lance/       ← tmpfs or ephemeral (scratch, locks)    │
└─────────────────────────────────────────────────────────────┘
```

#### Storage Requirements by Environment

| Environment | Segments Mount | WAL Mount | Scratch |
|-------------|----------------|-----------|--------|
| **Docker** | Named volume or bind mount | Same as segments | Container-local |
| **Docker Compose** | Named volumes per node | Same as segments | Container-local |
| **Kubernetes** | PVC (SSD-backed) | PVC or emptyDir | emptyDir |
| **Bare metal** | Local SSD path | Local SSD path | `/tmp` |

### 10.4 Ephemeral Integrity Contract

LANCE guarantees the following when running on ephemeral infrastructure:

| Guarantee | Mechanism |
|-----------|-----------|
| **No silent data loss** | All acknowledged writes are on PVC before ACK |
| **Crash-consistent segments** | CRC-32C per record; truncate to last valid on recovery |
| **Index rebuildable** | Sparse indexes can be regenerated from segment data |
| **WAL replay idempotent** | Replaying the same WAL twice produces identical state |

### 10.5 Kubernetes Orchestration (Optional)

When deploying to Kubernetes, use a StatefulSet (not Deployment) to ensure:

- Stable network identity (`lance-0`, `lance-1`, etc.)
- Ordered startup/shutdown for quorum management
- PVC retention across pod reschedules

#### ⚠️ Critical: Seccomp Profile for io_uring

**The Problem**: Most Kubernetes distributions (GKE, EKS, AKS, OpenShift) use a default Seccomp profile that **blocks io_uring syscalls** due to historical kernel security vulnerabilities (CVE-2021-41073, CVE-2022-29582, etc.).

**Without a custom Seccomp profile**, LANCE will fall back to `pwritev2`, and performance will degrade by 40-60%.

**Required syscalls**:

| Syscall | Number | Purpose |
|---------|--------|--------|
| `io_uring_setup` | 425 | Create io_uring instance |
| `io_uring_enter` | 426 | Submit/complete I/O operations |
| `io_uring_register` | 427 | Register buffers/files |

**Solution 1: Custom Seccomp Profile**

Create a Seccomp profile that allows io_uring:

```json
{
  "defaultAction": "SCMP_ACT_ERRNO",
  "architectures": ["SCMP_ARCH_X86_64"],
  "syscalls": [
    {
      "names": [
        "io_uring_setup",
        "io_uring_enter", 
        "io_uring_register"
      ],
      "action": "SCMP_ACT_ALLOW"
    },
    {
      "comment": "Include all syscalls from RuntimeDefault profile",
      "names": ["..."],
      "action": "SCMP_ACT_ALLOW"
    }
  ]
}
```

Deploy the profile to nodes at `/var/lib/kubelet/seccomp/lance-iouring.json`, then reference it:

```yaml
spec:
  securityContext:
    seccompProfile:
      type: Localhost
      localhostProfile: lance-iouring.json
```

**Solution 2: Unconfined (Not Recommended for Production)**

```yaml
spec:
  securityContext:
    seccompProfile:
      type: Unconfined  # Disables seccomp entirely
```

**Solution 3: Use RuntimeDefault + Accept Fallback**

If io_uring is blocked, LANCE detects this at startup and falls back to `pwritev2`. Monitor the metric `lance_io_backend{type="pwritev2"}` to detect this.

#### StatefulSet Configuration

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: lance
spec:
  serviceName: lance-headless
  replicas: 3
  podManagementPolicy: Parallel  # Fast scaling
  updateStrategy:
    type: RollingUpdate
  template:
    spec:
      terminationGracePeriodSeconds: 30  # Must exceed DRAIN_TIMEOUT
      securityContext:
        seccompProfile:
          type: Localhost
          localhostProfile: lance-iouring.json  # Custom profile for io_uring
      containers:
      - name: lance
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          runAsUser: 1000
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "kill -TERM 1 && sleep 25"]
        livenessProbe:
          httpGet:
            path: /health/live
            port: 9090
          initialDelaySeconds: 10
          periodSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 9090
          initialDelaySeconds: 5
          periodSeconds: 2
          failureThreshold: 2
        volumeMounts:
        - name: data
          mountPath: /data
        - name: tmp
          mountPath: /tmp/lance
      volumes:
      - name: tmp
        emptyDir: {}
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi
```

#### Managed Kubernetes Compatibility

| Platform | io_uring Support | Notes |
|----------|------------------|-------|
| **GKE** | Requires custom Seccomp | Use GKE Sandbox (gVisor) alternatives don't support io_uring |
| **EKS** | Requires custom Seccomp | AL2 nodes have io_uring; Bottlerocket does not |
| **AKS** | Requires custom Seccomp | Ubuntu nodes support io_uring |
| **OpenShift** | SCC required | Create custom SecurityContextConstraints |
| **k3s/RKE2** | Often works by default | Less restrictive default policies |

### 10.6 Health Endpoints

| Endpoint | Purpose | Returns 200 When |
|----------|---------|------------------|
| `/health/live` | Liveness probe | Process is running, not deadlocked |
| `/health/ready` | Readiness probe | Node is accepting writes, replication caught up |
| `/health/startup` | Startup probe | WAL replay complete, indexes loaded |

#### Readiness Conditions

```rust
pub fn is_ready(&self) -> bool {
    self.wal_replay_complete
        && self.segment_writer.is_some()
        && self.replication_lag_ms < MAX_REPLICATION_LAG_MS
        && !SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}
```

### 10.7 Node Identity

Node ID assignment varies by deployment method:

```rust
fn resolve_node_id() -> u16 {
    // Priority 1: Explicit environment variable
    if let Ok(id) = std::env::var("LANCE_NODE_ID") {
        return id.parse().expect("LANCE_NODE_ID must be numeric");
    }
    
    // Priority 2: Parse from hostname (StatefulSet pattern: lance-0, lance-1)
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        if let Some(suffix) = hostname.rsplit('-').next() {
            if let Ok(id) = suffix.parse::<u16>() {
                return id;
            }
        }
    }
    
    // Priority 3: Fail explicitly
    panic!("Cannot determine node ID: set LANCE_NODE_ID or use hostname pattern 'name-N'");
}
```

| Environment | Node ID Source |
|-------------|----------------|
| **Docker** | `LANCE_NODE_ID` env var (required) |
| **Docker Compose** | `LANCE_NODE_ID` env var per service |
| **Kubernetes StatefulSet** | Hostname suffix (automatic) |
| **Bare metal** | `LANCE_NODE_ID` env var or config file |
## 11. Chaos Testing & Failure Mode Design

LANCE is designed with chaos engineering principles as a first-class concern. Every component must have a defined failure mode and recovery path.

### 11.1 Design Philosophy

> **"If we haven't tested the failure, we don't know if it works."**

The architecture explicitly defines:
1. **What can fail** (exhaustive enumeration)
2. **How it fails** (graceful degradation vs. hard stop)
3. **How we detect it** (metrics, logs, probes)
4. **How we recover** (automatic vs. manual intervention)

### 11.2 Failure Mode Catalog

| Component | Failure Mode | Detection | Recovery | Data Impact |
|-----------|--------------|-----------|----------|-------------|
| **Network Acceptor** | Port bind failure | Startup probe fails | Pod restart | None |
| **Ingestion Channel** | Queue full (backpressure) | `backpressure.channel_full` metric | Upstream yield | None (flow control) |
| **io_uring Poller** | Kernel returns -EIO | CQE error code | Mark segment dirty, rotate | Last batch may be lost |
| **io_uring Poller** | Thread panic | Liveness probe timeout | Pod restart + recovery | WAL replay recovers |
| **Active Segment** | Disk full | Write returns -ENOSPC | Block writes, alert | None (no silent drop) |
| **Sparse Index** | Corruption detected | CRC mismatch on load | Rebuild from segment | None (index is derived) |
| **Replication** | Leader unreachable | Heartbeat timeout (5s) | Follower promotion | None (L3 quorum-ACK'd); possible gap (L1 standalone) |
| **Replication** | Split-brain | Quorum check on write | Reject writes from minority | None (writes fail-safe) |
| **WAL** | Corruption on replay | CRC mismatch | Truncate to last valid | Batch loss (logged) |
| **Node** | OOM killed | Kubernetes event | Pod restart + recovery | WAL replay recovers |
| **Node** | SIGKILL (force) | Exit code 137 | Recovery scan on restart | Possible batch loss |
| **PVC** | Volume unmount | I/O errors | Pod eviction | None (PVC persists) |

### 11.3 Chaos Test Scenarios

These scenarios **must pass** before any release:

#### 11.3.1 Pod Kill During Write

```bash
# Scenario: Kill pod while writes are in-flight
kubectl delete pod lance-0 --grace-period=0 --force

# Expected:
# - lance-0 restarts, runs recovery
# - No acknowledged writes are lost
# - Replication peers detect and update quorum
# - Metric: recovery.segments_scanned increments
```

#### 11.3.2 Network Partition (Split-Brain)

```bash
# Scenario: Isolate lance-0 from lance-1, lance-2
kubectl exec lance-0 -- iptables -A INPUT -s lance-1.lance-headless -j DROP
kubectl exec lance-0 -- iptables -A INPUT -s lance-2.lance-headless -j DROP

# Expected (L3 mode):
# - lance-0 cannot achieve quorum, rejects writes
# - lance-1/lance-2 form new quorum, continue serving
# - lance-0 logs: "Quorum lost, entering read-only mode"
# - After partition heals: lance-0 syncs from peers (filesystem-consistent resync)
```

#### 11.3.3 Disk Latency Injection

```bash
# Scenario: Inject 500ms latency on disk writes
kubectl exec lance-0 -- tc qdisc add dev sda root netem delay 500ms

# Expected:
# - io_uring CQE latency exceeds threshold
# - Backpressure signal sent to producers
# - Metric: disk.latency_p99_ms spikes
# - No data loss, just throughput degradation
```

#### 11.3.4 Memory Pressure

```bash
# Scenario: Reduce memory limit, trigger heap pressure
kubectl patch statefulset lance -p '{"spec":{"template":{"spec":{"containers":[{"name":"lance","resources":{"limits":{"memory":"512Mi"}}}]}}}}'

# Expected:
# - Global allocator pressure signal at 80%
# - Proactive flush to disk
# - If critical (95%), block new writes
# - No OOM kill (we beat the kernel to it)
```

#### 11.3.5 Rolling Restart Under Load

```bash
# Scenario: Rolling restart while sustaining 100k msg/sec
kubectl rollout restart statefulset lance

# Expected:
# - Pods drain in order (grace period honored)
# - No write failures (clients retry to other nodes)
# - Total restart time < 5 minutes for 3-node cluster
# - Zero acknowledged message loss
```

### 11.4 Chaos Testing Infrastructure

#### Required Tools

| Tool | Purpose |
|------|---------|
| **Chaos Mesh** | Pod kill, network partition, I/O fault injection |
| **Litmus** | Kubernetes-native chaos experiments |
| **tc/netem** | Network latency/loss injection |
| **stress-ng** | CPU/memory pressure simulation |

#### CI Integration

```yaml
# .github/workflows/chaos.yml
chaos-tests:
  runs-on: self-hosted-k8s
  steps:
    - name: Deploy LANCE cluster
      run: kubectl apply -f deploy/statefulset.yaml
    
    - name: Run load generator
      run: ./bin/lance-bench --rate 100000 --duration 5m &
    
    - name: Inject chaos
      run: |
        kubectl apply -f chaos/pod-kill.yaml
        sleep 30
        kubectl apply -f chaos/network-partition.yaml
        sleep 60
    
    - name: Validate invariants
      run: |
        ./bin/lance-verify --check-no-ack-loss
        ./bin/lance-verify --check-index-integrity
        ./bin/lance-verify --check-replication-sync
```

### 11.5 Observability for Chaos

#### Critical Metrics

| Metric | Alert Threshold | Indicates |
|--------|-----------------|-----------|
| `lance_recovery_scans_total` | > 0 in last hour | Unclean shutdowns occurred |
| `lance_wal_replay_records` | > 0 on startup | WAL was needed (not a clean shutdown) |
| `lance_quorum_lost_seconds` | > 0 | Split-brain or node loss |
| `lance_backpressure_events_total` | Spike | Upstream overload or disk slowdown |
| `lance_segment_dirty_flag` | = 1 | Crash recovery pending |

#### Structured Logging

All failure events emit structured logs for post-mortem:

```rust
tracing::error!(
    event = "io_uring_error",
    errno = %e,
    sqe_id = sqe.user_data(),
    segment = %self.active_segment.path().display(),
    batch_size = batch.len(),
    "io_uring write failed, marking segment dirty"
);
```

### 11.6 Recovery Guarantees

| Scenario | Guarantee |
|----------|-----------|
| **Clean shutdown** | Zero data loss, zero recovery scan |
| **SIGKILL / OOM** | Acknowledged writes recovered via WAL (if enabled) or CRC scan |
| **Disk failure** | Replicas hold data; rebuild from peers |
| **Split-brain healed** | Minority node syncs from majority; no divergent writes |
| **Index corruption** | Rebuild from segment; no data loss |

### 11.7 Blast Radius Containment

To prevent cascading failures:

1. **Bulkheads**: Each segment has independent I/O; one segment's failure doesn't block others
2. **Circuit breakers**: If replication peer fails 3x consecutively, stop retrying for 30s
3. **Load shedding**: Under extreme pressure, reject new connections (503) rather than degrade existing ones
4. **Timeouts everywhere**: No unbounded waits; every I/O and RPC has a deadline
## 12. Observability & Telemetry

LANCE exports metrics, traces, and logs in OpenTelemetry (OTEL) format for seamless integration with modern observability stacks. **All telemetry collection is non-blocking** to ensure zero impact on the hot path.

**Implementation**: See `lnc-metrics/src/lib.rs` for the Prometheus exporter and all metric definitions (ingestion, I/O, replication, backpressure, recovery).

### 12.1 Telemetry Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            LANCE Process                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  Hot Path (Ingestion)                                                    │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │
│  │ Network RX  │───▶│  Ingestion  │───▶│  io_uring   │                  │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                  │
│         │                  │                  │                          │
│         ▼                  ▼                  ▼                          │
│  [Atomic Counter]   [Atomic Counter]   [Atomic Counter]  ◀─ Lock-free   │
│         │                  │                  │                          │
├─────────┼──────────────────┼──────────────────┼──────────────────────────┤
│         └──────────────────┴──────────────────┘                          │
│                            │                                             │
│                    ┌───────▼───────┐                                     │
│                    │ Metrics       │  ◀─ Dedicated thread, periodic      │
│                    │ Aggregator    │     scrape of atomics               │
│                    └───────┬───────┘                                     │
│                            │                                             │
│                    ┌───────▼───────┐                                     │
│                    │ OTEL Exporter │  ◀─ Async, batched, non-blocking    │
│                    └───────┬───────┘                                     │
└────────────────────────────┼────────────────────────────────────────────┘
                             │
                             ▼
                    OTEL Collector / Prometheus / Jaeger
```

### 12.2 Non-Blocking Guarantee

The hot path **never blocks** on telemetry:

| Mechanism | Implementation |
|-----------|----------------|
| **Metric increment** | `AtomicU64::fetch_add(1, Ordering::Relaxed)` — single CPU instruction |
| **Histogram update** | Lock-free HDR histogram (`hdrhistogram` crate) |
| **Span creation** | Thread-local span buffer, no allocation on hot path |
| **Export** | Dedicated background thread with bounded channel; drops on overflow |

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    pub records_ingested: AtomicU64,
    pub bytes_written: AtomicU64,
    pub io_uring_submissions: AtomicU64,
    pub io_uring_completions: AtomicU64,
    pub backpressure_events: AtomicU64,
}

impl Metrics {
    #[inline(always)]
    pub fn record_ingested(&self, bytes: u64) {
        self.records_ingested.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
}
```

### 12.3 OTEL Integration

LANCE uses the `opentelemetry` and `opentelemetry-otlp` crates for standards-compliant export.

#### Metrics Export

```rust
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub fn init_metrics(endpoint: &str) -> Result<SdkMeterProvider> {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint)
        .build_metrics_exporter()?;
    
    let provider = SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(15))  // Export every 15s
                .build()
        )
        .build();
    
    Ok(provider)
}
```

#### Tracing Export

```rust
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;

pub fn init_tracing(endpoint: &str) -> Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
        )
        .with_trace_config(
            opentelemetry_sdk::trace::config()
                .with_sampler(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(0.01))  // 1% sampling
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;
    
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    
    tracing_subscriber::registry()
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    Ok(())
}
```

### 12.4 Metric Categories

#### Ingestion Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_records_ingested_total` | Counter | `node_id` | Total records ingested |
| `lance_bytes_written_total` | Counter | `node_id`, `segment` | Total bytes written to segments |
| `lance_batch_size` | Histogram | `node_id` | Records per batch |
| `lance_ingestion_latency_ns` | Histogram | `node_id` | Time from receive to io_uring submit |

#### io_uring Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_iouring_sq_depth` | Gauge | `node_id` | Current SQ utilization |
| `lance_iouring_cq_latency_ns` | Histogram | `node_id`, `op` | Time from SQE submit to CQE harvest |
| `lance_iouring_errors_total` | Counter | `node_id`, `errno` | io_uring operation failures |

#### Replication Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_replication_lag_bytes` | Gauge | `node_id` | Bytes behind leader |
| `lance_replication_lag_records` | Gauge | `node_id`, `peer` | Records behind leader |
| `lance_replication_last_sync_ms` | Gauge | `node_id` | Time since last successful sync |
| `lance_replication_pending_ops` | Gauge | `node_id` | Pending replication operations |
| `lance_replication_latency_seconds` | Histogram | `node_id`, `peer` | Replication operation latency |
| `lance_quorum_size` | Gauge | `node_id` | Current quorum membership |
| `lance_cluster_healthy_nodes` | Gauge | `node_id` | Number of healthy cluster nodes |
| `lance_cluster_is_leader` | Gauge | `node_id` | 1 if this node is leader, 0 otherwise |
| `lance_cluster_quorum_available` | Gauge | `node_id` | 1 if quorum is available, 0 otherwise |

#### Backpressure Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_backpressure_events_total` | Counter | `node_id`, `reason` | Backpressure triggers |
| `lance_channel_depth` | Gauge | `node_id`, `channel` | Current channel utilization |
| `lance_heap_usage_bytes` | Gauge | `node_id` | Current heap allocation |

#### Recovery Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_recovery_scans_total` | Counter | `node_id` | Segment recovery scans performed |
| `lance_wal_replay_records_total` | Counter | `node_id` | Records replayed from WAL |
| `lance_segment_dirty` | Gauge | `node_id`, `segment` | 1 if segment needs recovery |

### 12.5 Distributed Tracing

LANCE propagates trace context through the system:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Trace: write_record                                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ ├─ Span: network.receive          (2μs)                                 │
│ ├─ Span: tlv.encode               (500ns)                               │
│ ├─ Span: ingestion.enqueue        (100ns)                               │
│ ├─ Span: ingestion.batch          (50μs)  ◀─ Batching window            │
│ ├─ Span: iouring.submit           (1μs)                                 │
│ ├─ Span: iouring.complete         (200μs) ◀─ Disk I/O                   │
│ └─ Span: replication.ack (L3)     (5ms)   ◀─ Network to peers           │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Trace Context Propagation

For L3 replication, trace context is embedded in the replication protocol:

```rust
#[derive(Serialize, Deserialize)]
pub struct ReplicationMessage {
    pub batch: Vec<Record>,
    pub trace_context: Option<TraceContext>,  // W3C Trace Context format
}
```

### 12.6 Logging Strategy

LANCE uses structured logging via `tracing`:

| Level | Usage |
|-------|-------|
| `ERROR` | Unrecoverable failures, data loss risk |
| `WARN` | Recoverable issues, backpressure, retries |
| `INFO` | Lifecycle events (startup, shutdown, segment rotation) |
| `DEBUG` | Per-batch details (disabled in production) |
| `TRACE` | Per-record details (never in production) |

#### Log Format

```json
{
  "timestamp": "2026-02-02T22:13:45.123456Z",
  "level": "INFO",
  "target": "lance::segment",
  "message": "Segment rotated",
  "node_id": 0,
  "old_segment": "1000000_1706912000000000000.lnc",
  "new_segment": "2000000_1706912100000000000.lnc",
  "records_in_old": 1000000,
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7"
}
```

### 12.7 Configuration

```toml
[telemetry]
# OTEL exporter endpoint (gRPC)
otel_endpoint = "http://otel-collector:4317"

# Export interval
metrics_interval_secs = 15

# Trace sampling rate (0.0 - 1.0)
trace_sample_rate = 0.01

# Log level
log_level = "info"

# Log format: "json" or "pretty"
log_format = "json"

# Metrics export on overflow behavior: "drop" or "block"
# MUST be "drop" for production to maintain non-blocking guarantee
overflow_behavior = "drop"
```

### 12.8 Prometheus Compatibility

For environments without OTEL collectors, LANCE also exposes a Prometheus scrape endpoint:

```
GET /metrics HTTP/1.1
Host: lance-0:9090

# HELP lance_records_ingested_total Total records ingested
# TYPE lance_records_ingested_total counter
lance_records_ingested_total{node_id="0"} 1234567890

# HELP lance_iouring_cq_latency_ns CQE latency histogram
# TYPE lance_iouring_cq_latency_ns histogram
lance_iouring_cq_latency_ns_bucket{node_id="0",op="write",le="1000"} 500
lance_iouring_cq_latency_ns_bucket{node_id="0",op="write",le="10000"} 9500
...
```

This is served by a lightweight HTTP server on a dedicated thread, ensuring scrapes don't impact the hot path.
## 5.4 Revised Write Path: End-to-End Flow

This section provides a detailed view of the write path, showing where each component assigns metadata to minimize latency.

### Write Path Timeline

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              WRITE PATH TIMELINE                                         │
├──────────────────┬──────────────────────────────────────────────────────────────────────┤
│ NETWORK HANDLER  │  1. Receive TCP packet                                               │
│ (Async/Tokio)    │  2. Attach: SystemTime::now() → original_ts                         │
│                  │  3. Attach: node_id (from config)                                    │
│                  │  4. Hash(client_id) → select Ingestion Actor                         │
│                  │  5. Send to Ingestion Actor via flume channel                        │
├──────────────────┼──────────────────────────────────────────────────────────────────────┤
│ INGESTION ACTOR  │  6. Receive record from channel                                      │
│ (Pinned Thread)  │  7. Assign: sequence = self.counter.fetch_add(1)                    │
│                  │  8. Build: SortKey { original_ts, node_id, actor_id, sequence }     │
│                  │  9. TLV encode: [Type][Length][Payload]                              │
│                  │ 10. Accumulate batch until: size >= 64KB OR timeout >= 1ms          │
│                  │ 11. In-place sort batch by SortKey (CPU cache friendly)             │
│                  │ 12. Push batch to handoff queue (ringbuf or ArrayQueue)             │
├──────────────────┼──────────────────────────────────────────────────────────────────────┤
│ io_uring POLLER  │ 13. Pop batch from handoff queue (spin, never park)                 │
│ (Pinned Thread)  │ 14. Build SQE: pwrite(fd, batch_ptr, batch_len, file_offset)        │
│                  │ 15. Submit SQE to kernel                                             │
│                  │ 16. Poll CQE (completion)                                            │
│                  │ 17. On success: update High Water Mark                               │
│                  │ 18. Notify Replication Actor (if L3 mode)                            │
├──────────────────┼──────────────────────────────────────────────────────────────────────┤
│ REPLICATION      │ 19. (L3 only) Wait for M/2+1 peer ACKs                               │
│ (Async/Tokio)    │ 20. Send ACK to original client                                      │
└──────────────────┴──────────────────────────────────────────────────────────────────────┘
```

### Latency Breakdown (Target)

| Stage | Target Latency | Notes |
|-------|----------------|-------|
| Network → Ingestion | < 10μs | Async channel, no syscall |
| TLV Encode | < 500ns | Zero-copy, stack allocation |
| Batch Sort | < 50μs | In-place, 64KB batch |
| Ingestion → io_uring | < 1μs | Lock-free queue |
| io_uring Submit | < 2μs | Single syscall batched |
| Disk Write (NVMe) | 50-200μs | Hardware-dependent |
| **Total (L1)** | **< 300μs P99** | Without replication wait |
| Replication ACK (L3) | 1-10ms | Network RTT dependent |
| **Total (L3)** | **< 15ms P99** | With quorum ACK |

### Critical Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Timestamp at Network Handler** | Captures true arrival time before any queuing delay |
| **Sequence at Ingestion Actor** | Per-actor counter avoids atomic contention across threads |
| **Sort before io_uring** | Ensures on-disk order matches SortKey order |
| **Batch accumulation** | Amortizes syscall overhead; 64KB aligns with NVMe page size |
| **Spin on handoff queue** | io_uring thread must never park (deterministic latency) |

### Memory Allocation Strategy

| Stage | Allocation | Why |
|-------|------------|-----|
| **Network Handler** | Arena per connection | Avoid global allocator contention |
| **Ingestion Actor** | Pre-allocated batch buffers | Reuse across batches |
| **io_uring Poller** | Zero allocation | Uses pre-registered buffers |
| **Replication** | Pool of message buffers | Bounded memory for in-flight RPCs |

```rust
// Pre-registered io_uring buffers (allocated once at startup)
pub struct IoUringBuffers {
    buffers: Vec<AlignedBuffer>,  // 4KB aligned for O_DIRECT
    free_list: ArrayQueue<usize>, // Lock-free recycling
}

impl IoUringBuffers {
    pub fn acquire(&self) -> Option<&mut AlignedBuffer> {
        self.free_list.pop().map(|idx| &mut self.buffers[idx])
    }
    
    pub fn release(&self, idx: usize) {
        let _ = self.free_list.push(idx);
    }
}
```
## 13. Memory Layout & Zero-Copy Primitives

To achieve sub-microsecond ingestion, our memory layout must be cache-line aligned and zero-copy. We use the `zerocopy` crate to safely cast raw bytes from network/disk directly into structs without deserialization.

### 13.1 Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Cache-line alignment** | 64-byte aligned allocations; hot fields in first 64 bytes |
| **Zero-copy ingestion** | `zerocopy::FromBytes` for network → struct casting |
| **No per-record allocation** | Contiguous slab buffer with pointer indirection |
| **Sort without moving data** | Sort `RecordPointer` (16 bytes), not payloads (KB) |

### 13.2 On-Disk Record Format

Following the "SortKey in index, not record" strategy from Section 4.3:

```
┌─────────────────────────────────────────────────────────┐
│                    TLV RECORD (On-Disk)                 │
├──────────────┬──────────────┬───────────────┬───────────┤
│ schema_type  │ payload_len  │    payload    │ checksum  │
│   (1 byte)   │  (4 bytes)   │  (N bytes)    │ (4 bytes) │
└──────────────┴──────────────┴───────────────┴───────────┘
Total overhead: 9 bytes per record
```

**Why not embed SortKey?** The SortKey (16 bytes) is only needed for ordering, which the sparse index provides. Embedding it in every record wastes ~64% more space for small payloads.

### 13.3 In-Memory Header (Ingestion Path)

During ingestion, we need the full SortKey in memory for sorting. This is a **transient, in-memory-only** structure:

```rust
use zerocopy::{AsBytes, FromBytes, FromZeroes};

/// In-memory record header during ingestion (NOT written to disk)
/// Size: 32 bytes (fits 2 per cache line)
#[repr(C, align(8))]  // NOT packed - aligned for performance
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Copy, Clone)]
pub struct IngestHeader {
    // --- Sort Key (16 bytes) ---
    pub timestamp_ns: u64,      // 8 bytes: Nanoseconds since epoch
    pub node_actor_seq: u64,    // 8 bytes: [node_id:16][actor_id:8][sequence:40]
    
    // --- Record Metadata (16 bytes) ---
    pub schema_type: u8,        // 1 byte: 0-254 (0xFF = extended)
    pub _pad1: [u8; 3],         // 3 bytes: alignment padding
    pub payload_len: u32,       // 4 bytes: up to 4GiB
    pub payload_offset: u32,    // 4 bytes: offset in slab buffer
    pub checksum: u32,          // 4 bytes: CRC32C of payload
}

impl IngestHeader {
    #[inline]
    pub fn sort_key(&self) -> u128 {
        ((self.timestamp_ns as u128) << 64) | (self.node_actor_seq as u128)
    }
}
```

**Why `align(8)` instead of `packed`?**

| Attribute | Alignment | Access Speed | `zerocopy` Compatible |
|-----------|-----------|--------------|----------------------|
| `packed` | None (1-byte) | Slow (unaligned loads) | ❌ No |
| `align(8)` | 8-byte | Fast (aligned loads) | ✅ Yes |

### 13.4 The Ingestion Batch (Contiguous Slab)

```rust
/// Pre-allocated buffer for zero-copy record accumulation
pub struct IngestionBatch {
    /// Single contiguous allocation for all payloads
    buffer: AlignedBuffer,
    
    /// Metadata for sorting without moving data
    headers: Vec<IngestHeader>,
    
    /// Current write offset in buffer
    write_offset: usize,
    
    /// Batch capacity
    capacity: usize,
}

/// 4KB-aligned buffer for O_DIRECT compatibility
#[repr(C, align(4096))]
pub struct AlignedBuffer {
    data: [u8; BATCH_SIZE],  // Default: 256KB
}

impl IngestionBatch {
    /// Append a record to the batch (zero-copy from network buffer)
    #[inline]
    pub fn push(&mut self, header: IngestHeader, payload: &[u8]) -> Result<()> {
        let end = self.write_offset + payload.len();
        if end > self.capacity {
            return Err(LanceError::BatchFull);
        }
        
        // Zero-copy: memcpy directly into slab
        self.buffer.data[self.write_offset..end].copy_from_slice(payload);
        
        // Store header with offset
        let mut header = header;
        header.payload_offset = self.write_offset as u32;
        self.headers.push(header);
        
        self.write_offset = end;
        Ok(())
    }
    
    /// Sort by SortKey without moving payload data
    #[inline]
    pub fn sort_in_place(&mut self) {
        // Sort 32-byte headers, not KB-sized payloads
        self.headers.sort_unstable_by_key(|h| h.sort_key());
    }
    
    /// Build io_uring iovec array for writev
    pub fn build_iovecs(&self) -> Vec<libc::iovec> {
        self.headers.iter().map(|h| {
            libc::iovec {
                iov_base: self.buffer.data[h.payload_offset as usize..].as_ptr() as *mut _,
                iov_len: h.payload_len as usize + 9, // payload + TLV header
            }
        }).collect()
    }
}
```

### 13.5 Sparse Index Entry (On-Disk)

```rust
/// Sparse index entry - written to .idx sidecar file
/// Size: 24 bytes (aligned)
#[repr(C, align(8))]
#[derive(AsBytes, FromBytes, FromZeroes, Copy, Clone)]
pub struct SparseIndexEntry {
    pub sort_key: u128,     // 16 bytes: Total ordering key
    pub file_offset: u64,   // 8 bytes: Byte position in .lnc file
}
```

Memory-mapped for O(1) lookup:

```rust
impl SparseIndex {
    pub fn lookup(&self, target_key: u128) -> Option<u64> {
        let entries = self.as_slice();  // Zero-copy from mmap
        
        // Binary search over mmap'd entries
        match entries.binary_search_by_key(&target_key, |e| e.sort_key) {
            Ok(idx) => Some(entries[idx].file_offset),
            Err(idx) if idx > 0 => Some(entries[idx - 1].file_offset),
            Err(_) => None,
        }
    }
}
```

### 13.6 Checksum Strategy

**Problem**: CRC32 per record on the ingestion thread is CPU-intensive.

**Solution**: Offload to Network Actor using SIMD-accelerated `crc32fast`:

```rust
use crc32fast::Hasher;

impl NetworkHandler {
    async fn receive_record(&mut self, buf: &[u8]) -> Result<IngestHeader> {
        // CRC computed BEFORE sending to Ingestion Actor
        let mut hasher = Hasher::new();
        hasher.update(&buf[5..]);  // Skip TLV header, hash payload only
        let checksum = hasher.finalize();
        
        let header = IngestHeader {
            timestamp_ns: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64,
            node_actor_seq: self.build_node_actor_seq(),
            schema_type: buf[0],
            payload_len: u32::from_le_bytes(buf[1..5].try_into()?),
            payload_offset: 0,  // Set by IngestionBatch::push()
            checksum,
            _pad1: [0; 3],
        };
        
        Ok(header)
    }
}
```

**Performance**: `crc32fast` uses SSE4.2/AVX2/ARM NEON intrinsics. On modern CPUs:
- ~20 GB/s throughput (single core)
- Negligible impact at 100Gbps with multiple Network Actors

### 13.7 Cache Line Analysis

| Structure | Size | Per Cache Line (64B) | Hot Path |
|-----------|------|----------------------|----------|
| `IngestHeader` | 32 bytes | 2 headers | ✅ Ingestion |
| `SparseIndexEntry` | 24 bytes | 2.6 entries | ✅ Lookup |
| `RecordPointer` | 16 bytes | 4 pointers | ✅ Sort |
| TLV overhead | 9 bytes | N/A | ✅ Disk I/O |

### 13.8 Memory Alignment Summary

| Buffer | Alignment | Reason |
|--------|-----------|--------|
| `IngestionBatch.buffer` | 4096 bytes | O_DIRECT requires page-aligned I/O |
| `IngestHeader` | 8 bytes | Fast field access, `zerocopy` compatible |
| `SparseIndexEntry` | 8 bytes | Clean mmap casting |
| Stack allocations | 64 bytes | Cache-line aligned for hot-path locals |

### 13.9 Alternative: Packed Layout with Byte Arrays

If exact 24-byte headers are required (e.g., wire protocol compatibility), use byte-array wrappers:

```rust
/// Exactly 24 bytes, no padding, but requires accessor methods
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Copy, Clone)]
pub struct PackedHeader {
    pub timestamp_ns: [u8; 8],    // u64 as bytes
    pub node_id: [u8; 2],         // u16 as bytes
    pub sequence: [u8; 6],        // u48 as bytes
    pub schema_type: u8,
    pub payload_len: [u8; 4],     // u32 as bytes
    pub checksum: [u8; 3],        // u24 as bytes
}

impl PackedHeader {
    #[inline]
    pub fn timestamp_ns(&self) -> u64 {
        u64::from_le_bytes(self.timestamp_ns)
    }
    
    #[inline]
    pub fn sequence(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf[..6].copy_from_slice(&self.sequence);
        u64::from_le_bytes(buf)
    }
}
```

**Trade-off**: Exact size control vs. accessor overhead. For hot-path ingestion, prefer aligned structs. For wire format, use packed with accessors.
## 13.10 Bit-Packing: node_actor_seq Implementation

The `node_actor_seq` field packs three values into 64 bits for atomic operations and efficient comparison:

```
┌────────────────────────────────────────────────────────────────┐
│                    node_actor_seq (64 bits)                    │
├──────────────────┬────────────────┬────────────────────────────┤
│    node_id       │   actor_id     │        sequence            │
│   (16 bits)      │   (8 bits)     │       (40 bits)            │
│   bits 63-48     │   bits 47-40   │       bits 39-0            │
└──────────────────┴────────────────┴────────────────────────────┘
```

### Manual Bit-Shifting (Recommended for Hot Path)

```rust
/// Bit-pack node_id, actor_id, and sequence into u64
#[inline(always)]
pub const fn pack_node_actor_seq(node_id: u16, actor_id: u8, sequence: u64) -> u64 {
    debug_assert!(sequence < (1 << 40), "sequence exceeds 40 bits");
    ((node_id as u64) << 48) | ((actor_id as u64) << 40) | (sequence & 0xFF_FFFF_FFFF)
}

/// Extract components from packed u64
#[inline(always)]
pub const fn unpack_node_id(packed: u64) -> u16 {
    (packed >> 48) as u16
}

#[inline(always)]
pub const fn unpack_actor_id(packed: u64) -> u8 {
    ((packed >> 40) & 0xFF) as u8
}

#[inline(always)]
pub const fn unpack_sequence(packed: u64) -> u64 {
    packed & 0xFF_FFFF_FFFF  // Lower 40 bits
}
```

### Alternative: `bitfield` Crate

For self-documenting code (slight runtime cost):

```rust
use bitfield::bitfield;

bitfield! {
    pub struct NodeActorSeq(u64);
    impl Debug;
    pub u16, node_id, set_node_id: 63, 48;
    pub u8, actor_id, set_actor_id: 47, 40;
    pub u64, sequence, set_sequence: 39, 0;
}
```

### Usage in IngestHeader

```rust
impl IngestHeader {
    #[inline]
    pub fn new(timestamp_ns: u64, node_id: u16, actor_id: u8, sequence: u64) -> Self {
        Self {
            timestamp_ns,
            node_actor_seq: pack_node_actor_seq(node_id, actor_id, sequence),
            // ... other fields
        }
    }
    
    /// 128-bit sort key for total ordering
    #[inline(always)]
    pub fn sort_key(&self) -> u128 {
        ((self.timestamp_ns as u128) << 64) | (self.node_actor_seq as u128)
    }
}
```

### Sequence Counter Per Actor

Each Ingestion Actor maintains its own monotonic counter:

```rust
pub struct IngestionActor {
    node_id: u16,
    actor_id: u8,
    sequence: AtomicU64,  // Per-actor, no contention
}

impl IngestionActor {
    #[inline]
    fn next_sequence(&self) -> u64 {
        // Relaxed ordering: only this actor increments
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }
    
    #[inline]
    fn build_node_actor_seq(&self) -> u64 {
        pack_node_actor_seq(self.node_id, self.actor_id, self.next_sequence())
    }
}
```

**Capacity**: 40-bit sequence = 1.1 trillion records per actor before wrap. At 10M records/sec, that's ~3.5 years.
## 13.11 io_uring Probe & Fallback Strategy

The io_uring availability check **must** be the first operation in `main()`. If blocked (Seccomp, old kernel), LANCE falls back to `pwritev2` with a prominent warning.

### Startup Probe Sequence

```rust
use io_uring::{IoUring, opcode, types};
use tracing::{error, warn, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    IoUring,
    Pwritev2,
}

/// MUST be called first in main() before any async runtime starts
pub fn probe_io_backend() -> IoBackend {
    match probe_io_uring() {
        Ok(()) => {
            info!(target: "lance::init", "io_uring available, using high-performance backend");
            IoBackend::IoUring
        }
        Err(e) => {
            // HIGH PRIORITY WARNING - this will tank performance
            error!(
                target: "lance::init",
                error = %e,
                "io_uring UNAVAILABLE - falling back to pwritev2"
            );
            warn!(
                target: "lance::init",
                "Performance will degrade by 40-60%. \
                 If running in Kubernetes, check Seccomp profile. \
                 Required syscalls: io_uring_setup (425), io_uring_enter (426), io_uring_register (427)"
            );
            
            // Emit metric for alerting
            metrics::gauge!("lance_io_backend", 0.0, "type" => "pwritev2");
            
            IoBackend::Pwritev2
        }
    }
}

fn probe_io_uring() -> Result<(), Box<dyn std::error::Error>> {
    // Attempt to create a minimal io_uring instance
    let ring = IoUring::builder()
        .dontfork()
        .build(8)?;  // Minimal 8-entry ring
    
    // Probe for required operations
    let probe = ring.submitter().register_probe()?;
    
    let required_ops = [
        (opcode::Write::CODE, "IORING_OP_WRITE"),
        (opcode::Writev::CODE, "IORING_OP_WRITEV"),
        (opcode::Read::CODE, "IORING_OP_READ"),
        (opcode::Fsync::CODE, "IORING_OP_FSYNC"),
    ];
    
    for (op, name) in required_ops {
        if !probe.is_supported(op) {
            return Err(format!("{} not supported", name).into());
        }
    }
    
    Ok(())
}
```

### Integration in main()

```rust
fn main() -> Result<()> {
    // ┌─────────────────────────────────────────────────────────┐
    // │  STEP 1: IO Backend probe (BEFORE anything else)       │
    // └─────────────────────────────────────────────────────────┘
    let io_backend = probe_io_backend();
    
    // Initialize tracing after probe (probe logs go to stderr initially)
    init_tracing()?;
    
    // ┌─────────────────────────────────────────────────────────┐
    // │  STEP 2: Log startup configuration                      │
    // └─────────────────────────────────────────────────────────┘
    info!(
        target: "lance::init",
        io_backend = ?io_backend,
        node_id = config.node_id,
        data_dir = %config.data_dir.display(),
        "LANCE starting"
    );
    
    // Emit backend metric for dashboards
    match io_backend {
        IoBackend::IoUring => metrics::gauge!("lance_io_backend", 1.0, "type" => "io_uring"),
        IoBackend::Pwritev2 => metrics::gauge!("lance_io_backend", 1.0, "type" => "pwritev2"),
    }
    
    // ┌─────────────────────────────────────────────────────────┐
    // │  STEP 3: Build runtime with selected backend            │
    // └─────────────────────────────────────────────────────────┘
    let runtime = match io_backend {
        IoBackend::IoUring => build_iouring_runtime(config)?,
        IoBackend::Pwritev2 => build_pwritev2_runtime(config)?,
    };
    
    runtime.run()
}
```

### Fallback Writer Implementation

```rust
pub trait SegmentWriter: Send + Sync {
    fn write_batch(&self, batch: &IngestionBatch) -> Result<u64>;
    fn fsync(&self) -> Result<()>;
}

/// High-performance io_uring writer
pub struct IoUringWriter {
    ring: IoUring,
    fd: RawFd,
    // ...
}

/// Fallback pwritev2 writer (still respects RWF_DSYNC)
pub struct Pwritev2Writer {
    file: File,
}

impl SegmentWriter for Pwritev2Writer {
    fn write_batch(&self, batch: &IngestionBatch) -> Result<u64> {
        let iovecs = batch.build_iovecs();
        let offset = self.file.stream_position()?;
        
        // pwritev2 with RWF_DSYNC for durability without full fsync
        let written = unsafe {
            libc::pwritev2(
                self.file.as_raw_fd(),
                iovecs.as_ptr(),
                iovecs.len() as i32,
                offset as i64,
                libc::RWF_DSYNC,  // Sync data (not metadata) on write
            )
        };
        
        if written < 0 {
            return Err(io::Error::last_os_error().into());
        }
        
        Ok(written as u64)
    }
    
    fn fsync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }
}
```

### Alerting on Fallback

Configure your monitoring to alert on `lance_io_backend{type="pwritev2"} == 1`:

```yaml
# Prometheus alert rule
groups:
  - name: lance
    rules:
      - alert: LanceIoUringUnavailable
        expr: lance_io_backend{type="pwritev2"} == 1
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "LANCE running without io_uring"
          description: "Node {{ $labels.instance }} is using pwritev2 fallback. Check Seccomp profile."
```
## 13.12 SIMD-Accelerated Checksumming

The `crc32fast` crate auto-detects CPU features and uses the fastest available implementation:

| CPU Feature | Throughput | Notes |
|-------------|------------|-------|
| **SSE4.2** (x86) | ~8 GB/s | Hardware CRC32C instruction |
| **AVX2** (x86) | ~20 GB/s | SIMD parallel hashing |
| **ARMv8 CRC** | ~10 GB/s | Hardware instruction |
| **Fallback** | ~500 MB/s | Software implementation |

### Cargo.toml Configuration

```toml
[dependencies]
crc32fast = "1.3"

[profile.release]
lto = "thin"           # Enable link-time optimization
codegen-units = 1      # Better SIMD codegen
```

### Network Actor Implementation

```rust
use crc32fast::Hasher;
use std::arch::x86_64::*;

pub struct NetworkActor {
    // Pre-allocated hasher (reusable, avoids allocation)
    hasher: Hasher,
}

impl NetworkActor {
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
        }
    }
    
    /// Process incoming record with SIMD checksum
    #[inline]
    pub async fn receive_record(&mut self, buf: &[u8]) -> Result<(IngestHeader, &[u8])> {
        // Parse TLV header (5 bytes)
        let schema_type = buf[0];
        let payload_len = u32::from_le_bytes(buf[1..5].try_into()?) as usize;
        let payload = &buf[5..5 + payload_len];
        
        // ┌─────────────────────────────────────────────────────────┐
        // │  SIMD CRC32 - runs at ~memcpy speed on AVX2            │
        // └─────────────────────────────────────────────────────────┘
        self.hasher.reset();
        self.hasher.update(payload);
        let checksum = self.hasher.finalize();
        
        // Build header with checksum already computed
        let header = IngestHeader {
            timestamp_ns: current_timestamp_ns(),
            node_actor_seq: 0,  // Set by Ingestion Actor
            schema_type,
            _pad1: [0; 3],
            payload_len: payload_len as u32,
            payload_offset: 0,  // Set by IngestionBatch::push()
            checksum,
        };
        
        Ok((header, payload))
    }
}

/// High-resolution timestamp (cached per batch for speed)
#[inline(always)]
fn current_timestamp_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos() as u64
}
```

### Verification: Is SIMD Active?

At startup, log the detected implementation:

```rust
pub fn log_crc32_implementation() {
    // crc32fast automatically selects the best implementation
    // We can verify by checking CPU features
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            info!(target: "lance::init", crc32_impl = "AVX2", "SIMD checksum: ~20 GB/s");
        } else if is_x86_feature_detected!("sse4.2") {
            info!(target: "lance::init", crc32_impl = "SSE4.2", "SIMD checksum: ~8 GB/s");
        } else {
            warn!(target: "lance::init", crc32_impl = "software", "SIMD checksum: ~500 MB/s (fallback)");
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    {
        // ARMv8 CRC extension is standard on modern ARM
        info!(target: "lance::init", crc32_impl = "ARMv8", "SIMD checksum: ~10 GB/s");
    }
}
```

### Batched Checksumming (Optional Optimization)

For very small records, batch multiple payloads into a single CRC pass:

```rust
impl IngestionBatch {
    /// Compute checksums for all records in batch (single SIMD pass)
    pub fn compute_checksums_batched(&mut self) {
        let mut hasher = Hasher::new();
        
        for header in &mut self.headers {
            let start = header.payload_offset as usize;
            let end = start + header.payload_len as usize;
            let payload = &self.buffer.data[start..end];
            
            hasher.reset();
            hasher.update(payload);
            header.checksum = hasher.finalize();
        }
    }
}
```

### Checksum Validation on Read

```rust
impl SegmentReader {
    pub fn validate_record(&self, offset: u64) -> Result<bool> {
        let header = self.read_tlv_header(offset)?;
        let payload = self.read_payload(offset + 5, header.payload_len)?;
        let stored_checksum = self.read_checksum(offset + 5 + header.payload_len)?;
        
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let computed = hasher.finalize();
        
        if computed != stored_checksum {
            error!(
                target: "lance::integrity",
                offset = offset,
                stored = stored_checksum,
                computed = computed,
                "Checksum mismatch - record corrupted"
            );
            return Ok(false);
        }
        
        Ok(true)
    }
}
```

### Performance Budget

At 100Gbps ingestion:
- Raw throughput: 12.5 GB/s
- AVX2 CRC32: ~20 GB/s
- **Headroom**: ~60% (checksum is NOT the bottleneck)
## 14. Thread Pinning & NUMA Awareness

At 100Gbps, if your NIC is on NUMA Node 0 and your io_uring poller is on NUMA Node 1, QPI/UPI cross-socket traffic introduces **30-50% latency penalties**. This section mandates NUMA-local execution.

**Implementation**: See `lnc-core/src/numa.rs` for `NumaTopology`, `NumaAllocator`, and thread pinning utilities.

### 14.1 The NUMA Problem

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DUAL-SOCKET SERVER                                 │
├─────────────────────────────────┬───────────────────────────────────────────┤
│         NUMA NODE 0             │           NUMA NODE 1                     │
│  ┌─────────┐  ┌─────────┐       │     ┌─────────┐  ┌─────────┐              │
│  │ CPU 0-15│  │ DDR4    │       │     │ CPU 16-31│ │ DDR4    │              │
│  └────┬────┘  └────┬────┘       │     └────┬────┘  └────┬────┘              │
│       │            │            │          │            │                   │
│  ┌────┴────────────┴────┐       │     ┌────┴────────────┴────┐              │
│  │    Memory Controller │◄──────┼─────│    Memory Controller │              │
│  └──────────┬───────────┘  QPI  │     └──────────┬───────────┘              │
│             │              ~~~  │                │                          │
│      ┌──────┴──────┐       30-50│         ┌──────┴──────┐                   │
│      │  PCIe Root  │       ns   │         │  PCIe Root  │                   │
│      └──────┬──────┘            │         └──────┬──────┘                   │
│             │                   │                │                          │
│      ┌──────┴──────┐            │         ┌──────┴──────┐                   │
│      │  100G NIC   │            │         │   NVMe x4   │                   │
│      └─────────────┘            │         └─────────────┘                   │
└─────────────────────────────────┴───────────────────────────────────────────┘

❌ BAD: Network Actor on Node 0, io_uring Poller on Node 1
   → Every batch crosses QPI, adding 30-50ns per access

✅ GOOD: All actors and their buffers on Node 0 (where NIC lives)
   → Local memory access: ~80ns vs ~130ns remote
```

### 14.2 NUMA Topology Discovery

At startup, LANCE discovers the hardware topology:

```rust
use hwloc2::{Topology, ObjectType};

pub struct NumaTopology {
    pub nic_node: usize,
    pub nvme_nodes: Vec<usize>,
    pub cores_per_node: Vec<Vec<usize>>,
}

impl NumaTopology {
    pub fn discover() -> Result<Self> {
        let topo = Topology::new()?;
        
        // Find NIC's NUMA node by PCI address
        let nic_node = find_device_numa_node(&topo, &config.nic_pci_addr)?;
        
        // Find NVMe devices' NUMA nodes
        let nvme_nodes: Vec<usize> = config.nvme_pci_addrs
            .iter()
            .map(|addr| find_device_numa_node(&topo, addr))
            .collect::<Result<_>>()?;
        
        // Map cores to NUMA nodes
        let cores_per_node = enumerate_cores_by_node(&topo)?;
        
        info!(
            target: "lance::numa",
            nic_node = nic_node,
            nvme_nodes = ?nvme_nodes,
            "NUMA topology discovered"
        );
        
        Ok(Self { nic_node, nvme_nodes, cores_per_node })
    }
}

fn find_device_numa_node(topo: &Topology, pci_addr: &str) -> Result<usize> {
    // Parse PCI address (e.g., "0000:3b:00.0")
    let pci_obj = topo.objects_with_type(ObjectType::PCIDevice)?
        .find(|obj| obj.name() == Some(pci_addr))
        .ok_or(LanceError::DeviceNotFound)?;
    
    // Walk up to find NUMA node
    let numa_node = pci_obj.ancestor_by_type(ObjectType::NUMANode)
        .map(|n| n.os_index())
        .unwrap_or(0);  // Default to node 0 if not found
    
    Ok(numa_node as usize)
}
```

### 14.3 Thread Pinning Strategy

| Thread | Pin To | Rationale |
|--------|--------|-----------|
| **Network Actors** | NIC's NUMA node | Minimize DMA → CPU latency |
| **Ingestion Actors** | NIC's NUMA node | Process data where it lands |
| **io_uring Pollers** | NVMe's NUMA node | Minimize CPU → NVMe latency |
| **Replication Actor** | NIC's NUMA node | Network-bound |

```rust
use core_affinity::{CoreId, set_for_current};

pub fn pin_thread(thread_name: &str, core_id: usize) -> Result<()> {
    let core = CoreId { id: core_id };
    
    if !set_for_current(core) {
        return Err(LanceError::PinFailed(core_id));
    }
    
    info!(
        target: "lance::affinity",
        thread = thread_name,
        core = core_id,
        "Thread pinned"
    );
    
    Ok(())
}

// Example: Spawn io_uring poller on NVMe-local cores
pub fn spawn_iouring_poller(topo: &NumaTopology, nvme_idx: usize) -> JoinHandle<()> {
    let nvme_node = topo.nvme_nodes[nvme_idx];
    let core = topo.cores_per_node[nvme_node]
        .get(nvme_idx % topo.cores_per_node[nvme_node].len())
        .copied()
        .expect("No cores available on NVMe NUMA node");
    
    std::thread::Builder::new()
        .name(format!("iouring-{}", nvme_idx))
        .spawn(move || {
            pin_thread(&format!("iouring-{}", nvme_idx), core)
                .expect("Failed to pin io_uring thread");
            
            iouring_poll_loop();
        })
        .expect("Failed to spawn io_uring thread")
}
```

### 14.4 NUMA-Local Memory Allocation

**Critical rule**: Every `IngestionBatch` must be allocated on the same NUMA node as the thread that processes it.

```rust
use libc::{mbind, MPOL_BIND};
use std::alloc::{alloc, Layout};

/// Allocate memory on a specific NUMA node
pub unsafe fn alloc_numa_local(size: usize, align: usize, node: usize) -> *mut u8 {
    let layout = Layout::from_size_align(size, align).expect("Invalid layout");
    let ptr = alloc(layout);
    
    if ptr.is_null() {
        return ptr;
    }
    
    // Bind memory to specific NUMA node
    let mut nodemask: libc::c_ulong = 1 << node;
    let result = mbind(
        ptr as *mut libc::c_void,
        size,
        MPOL_BIND,
        &mut nodemask,
        std::mem::size_of::<libc::c_ulong>() * 8,
        0,
    );
    
    if result != 0 {
        warn!(
            target: "lance::numa",
            node = node,
            error = %std::io::Error::last_os_error(),
            "mbind failed, memory may not be NUMA-local"
        );
    }
    
    ptr
}

/// NUMA-aware aligned buffer for IngestionBatch
pub struct NumaAlignedBuffer {
    ptr: *mut u8,
    size: usize,
    node: usize,
}

impl NumaAlignedBuffer {
    pub fn new(size: usize, node: usize) -> Result<Self> {
        let ptr = unsafe { alloc_numa_local(size, 4096, node) };
        if ptr.is_null() {
            return Err(LanceError::AllocationFailed);
        }
        Ok(Self { ptr, size, node })
    }
    
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for NumaAlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.size, 4096);
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}
```

### 14.5 Verification at Startup

```rust
pub fn verify_numa_placement(topo: &NumaTopology) -> Result<()> {
    // Verify NIC and ingestion actors are on same node
    let current_node = unsafe { libc::numa_node_of_cpu(libc::sched_getcpu()) };
    
    if current_node as usize != topo.nic_node {
        error!(
            target: "lance::numa",
            expected = topo.nic_node,
            actual = current_node,
            "Ingestion actor NOT on NIC's NUMA node - performance will suffer"
        );
        metrics::gauge!("lance_numa_misaligned", 1.0);
    }
    
    Ok(())
}
```

### 14.6 Production Kernel Configuration

For maximum isolation, production deployments should use `isolcpus`:

```bash
# /etc/default/grub
GRUB_CMDLINE_LINUX="isolcpus=2-7,10-15 nohz_full=2-7,10-15 rcu_nocbs=2-7,10-15"
```

| Parameter | Purpose |
|-----------|---------|
| `isolcpus` | Remove cores from general scheduler; LANCE pins explicitly |
| `nohz_full` | Disable timer ticks on isolated cores (reduces jitter) |
| `rcu_nocbs` | Offload RCU callbacks from isolated cores |

**Verification**:
```bash
# Check isolated cores
cat /sys/devices/system/cpu/isolated
# Expected: 2-7,10-15

# Verify LANCE threads are on isolated cores
ps -eo pid,comm,psr | grep lance
```

### 14.7 IRQ Affinity & NIC Queue Steering

**⚠️ The Last Mile Problem**: You've pinned threads to cores, but if the kernel delivers NIC interrupts to a different core, every packet crosses the L3 cache boundary. This is **Jitter Poison**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     IRQ AFFINITY MISMATCH (BAD)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Core 0 (NIC IRQ)          L3 Cache Boundary         Core 2 (Network Actor)│
│   ┌─────────────┐                  │                  ┌─────────────┐        │
│   │  IRQ Handler │ ═══════════════►│═══════════════► │ NetworkActor │        │
│   │  (softirq)   │    Cache Miss   │   Cache Miss    │  (pinned)    │        │
│   └─────────────┘     ~40-80ns     │    ~40-80ns     └─────────────┘        │
│                                    │                                         │
│   Total added latency: 80-160ns per packet × 10M packets/sec = DISASTER     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### NIC Queue → Core Mapping

Modern NICs have multiple RX/TX queues. Each queue should be steered to the same core as its Network Actor:

```bash
#!/bin/bash
# configure-irq-affinity.sh
# Run after LANCE startup, after thread pinning is established

NIC="eth0"  # Replace with your 100G NIC (e.g., enp3s0f0)

# Get NIC IRQ numbers
IRQ_LIST=$(grep "$NIC" /proc/interrupts | awk '{print $1}' | tr -d ':')

# Map each NIC queue IRQ to a specific core
# Assumes Network Actors are pinned to cores 2, 3, 4, 5
CORE_LIST=(2 3 4 5)
QUEUE_IDX=0

for IRQ in $IRQ_LIST; do
    CORE=${CORE_LIST[$((QUEUE_IDX % ${#CORE_LIST[@]}))]}
    
    # Set IRQ affinity (convert core to bitmask)
    MASK=$(printf '%x' $((1 << CORE)))
    echo $MASK > /proc/irq/$IRQ/smp_affinity
    
    echo "IRQ $IRQ -> Core $CORE (mask: 0x$MASK)"
    QUEUE_IDX=$((QUEUE_IDX + 1))
done

# Configure RSS (Receive Side Scaling) to match
ethtool -X $NIC equal ${#CORE_LIST[@]}
```

#### Verification

```bash
# Check current IRQ affinity
for IRQ in $(grep eth0 /proc/interrupts | awk '{print $1}' | tr -d ':'); do
    echo -n "IRQ $IRQ: "
    cat /proc/irq/$IRQ/smp_affinity_list
done

# Expected output (for 4 Network Actors on cores 2-5):
# IRQ 48: 2
# IRQ 49: 3
# IRQ 50: 4
# IRQ 51: 5

# Verify RSS indirection table
ethtool -x eth0
```

#### Rust Startup Integration

```rust
use std::process::Command;

pub fn configure_irq_affinity(nic: &str, cores: &[usize]) -> Result<()> {
    // Find IRQs for this NIC
    let irqs = find_nic_irqs(nic)?;
    
    for (idx, irq) in irqs.iter().enumerate() {
        let core = cores[idx % cores.len()];
        let mask = 1u64 << core;
        
        // Write to /proc/irq/{irq}/smp_affinity
        let path = format!("/proc/irq/{}/smp_affinity", irq);
        std::fs::write(&path, format!("{:x}", mask))?;
        
        info!(
            target: "lance::irq",
            irq = irq,
            core = core,
            "IRQ affinity configured"
        );
    }
    
    // Configure RSS
    Command::new("ethtool")
        .args(["-X", nic, "equal", &cores.len().to_string()])
        .status()?;
    
    Ok(())
}

fn find_nic_irqs(nic: &str) -> Result<Vec<u32>> {
    let interrupts = std::fs::read_to_string("/proc/interrupts")?;
    let mut irqs = Vec::new();
    
    for line in interrupts.lines() {
        if line.contains(nic) {
            if let Some(irq_str) = line.split(':').next() {
                if let Ok(irq) = irq_str.trim().parse() {
                    irqs.push(irq);
                }
            }
        }
    }
    
    Ok(irqs)
}
```

#### Production Requirements

| Requirement | Implementation |
|-------------|----------------|
| **IRQ Steering** | `/proc/irq/{N}/smp_affinity` matches Network Actor cores |
| **RSS Configuration** | `ethtool -X` distributes queues across Network Actor cores |
| **irqbalance Disabled** | `systemctl disable irqbalance` (conflicts with manual affinity) |
| **Startup Ordering** | Configure IRQ affinity AFTER thread pinning, BEFORE accepting connections |

#### Metrics

```rust
// Track IRQ → core mapping at startup
for (irq, core) in irq_core_mapping.iter() {
    metrics::gauge!("lance_irq_affinity", 1.0, 
        "irq" => irq.to_string(),
        "core" => core.to_string()
    );
}
```

**Warning**: If `irqbalance` is running, it will override your affinity settings. Disable it:

```bash
systemctl stop irqbalance
systemctl disable irqbalance
```
## 15. Buffer Ownership & Lifecycle (The Loaner Pattern)

To prevent engineers from using `Mutex` or `Clone` on the hot path, we mandate a strict **Ownership Relay** pattern for buffer management.

**Implementation**: See `lnc-core/src/buffer.rs` for `AlignedBuffer` and `NumaAlignedBuffer` with 4KB alignment for O_DIRECT, `mlock()` support, and NUMA-local allocation.

### 15.1 The Problem: Lock Contention

**What engineers might do**:
```rust
// ❌ BAD: Shared mutable state with lock
struct SharedBatch {
    buffer: Arc<Mutex<Vec<u8>>>,  // Lock contention!
}

// ❌ BAD: Cloning payload data
fn handoff(batch: &IngestionBatch) {
    let cloned = batch.buffer.clone();  // Expensive copy!
    queue.push(cloned);
}
```

**Why it's bad**: At 100Gbps, even a 1μs lock acquisition per batch destroys throughput. Cloning a 256KB buffer takes ~50μs.

### 15.2 The Loaner Pattern

Buffers are **loaned**, not shared. Ownership transfers cleanly between actors.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BUFFER OWNERSHIP LIFECYCLE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────┐     pop()      ┌──────────────────┐                      │
│   │              │ ◄───────────── │                  │                      │
│   │  FREE POOL   │                │ INGESTION ACTOR  │                      │
│   │  (ArrayQueue)│ ───────────► │  (exclusive owner)│                      │
│   │              │    push()      │                  │                      │
│   └──────────────┘   (recycle)    └────────┬─────────┘                      │
│         ▲                                  │                                │
│         │                                  │ push() (transfer ownership)    │
│         │                                  ▼                                │
│   ┌─────┴────────┐                ┌──────────────────┐                      │
│   │              │ ◄───────────── │                  │                      │
│   │ io_uring     │    CQE done    │  HANDOFF QUEUE   │                      │
│   │ POLLER       │ ───────────► │  (ArrayQueue)    │                      │
│   │              │    pop()       │                  │                      │
│   └──────────────┘                └──────────────────┘                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 15.3 Implementation

```rust
use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

/// A batch that can be safely loaned between actors
pub struct LoanableBatch {
    buffer: NumaAlignedBuffer,
    headers: Vec<IngestHeader>,
    write_offset: usize,
    batch_id: u64,  // For debugging/tracing
}

impl LoanableBatch {
    pub fn new(capacity: usize, numa_node: usize, batch_id: u64) -> Result<Self> {
        Ok(Self {
            buffer: NumaAlignedBuffer::new(capacity, numa_node)?,
            headers: Vec::with_capacity(8192),
            write_offset: 0,
            batch_id,
        })
    }
    
    /// Reset for reuse (called by io_uring poller before returning to pool)
    pub fn reset(&mut self) {
        self.headers.clear();
        self.write_offset = 0;
        // Note: buffer memory is NOT zeroed (unnecessary overhead)
    }
}

/// The Free Pool: pre-allocated batches ready for use
pub struct BatchPool {
    free: ArrayQueue<LoanableBatch>,
    total_batches: usize,
}

impl BatchPool {
    pub fn new(num_batches: usize, batch_size: usize, numa_node: usize) -> Result<Self> {
        let free = ArrayQueue::new(num_batches);
        
        for i in 0..num_batches {
            let batch = LoanableBatch::new(batch_size, numa_node, i as u64)?;
            free.push(batch).expect("Pool should not be full during init");
        }
        
        info!(
            target: "lance::pool",
            num_batches = num_batches,
            batch_size = batch_size,
            numa_node = numa_node,
            "Batch pool initialized"
        );
        
        Ok(Self { free, total_batches: num_batches })
    }
    
    /// Borrow a batch (blocks if none available via spin-wait)
    #[inline]
    pub fn borrow(&self) -> LoanableBatch {
        loop {
            if let Some(batch) = self.free.pop() {
                return batch;
            }
            // Backpressure: pool exhausted, spin briefly
            metrics::increment!("lance_pool_exhausted");
            std::hint::spin_loop();
        }
    }
    
    /// Return a batch to the pool
    #[inline]
    pub fn return_batch(&self, mut batch: LoanableBatch) {
        batch.reset();
        
        // This should never fail if we sized the pool correctly
        if self.free.push(batch).is_err() {
            error!(target: "lance::pool", "Pool overflow - batch leaked");
            metrics::increment!("lance_pool_overflow");
        }
    }
    
    pub fn available(&self) -> usize {
        self.free.len()
    }
}
```

### 15.4 Ownership Transfer: Ingestion → io_uring

```rust
/// Handoff queue: transfers ownership from Ingestion Actor to io_uring Poller
pub struct HandoffQueue {
    queue: ArrayQueue<LoanableBatch>,
}

impl HandoffQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
        }
    }
    
    /// Transfer ownership to the queue (Ingestion Actor side)
    #[inline]
    pub fn send(&self, batch: LoanableBatch) -> Result<(), LoanableBatch> {
        self.queue.push(batch).map_err(|b| {
            metrics::increment!("lance_handoff_full");
            b
        })
    }
    
    /// Take ownership from the queue (io_uring Poller side)
    #[inline]
    pub fn recv(&self) -> Option<LoanableBatch> {
        self.queue.pop()
    }
}
```

### 15.5 The Ingestion Loop (Complete)

```rust
pub fn ingestion_loop(
    actor: &mut IngestionActor,
    pool: Arc<BatchPool>,
    handoff: Arc<HandoffQueue>,
    mut rx: flume::Receiver<IngestRequest>,
) {
    loop {
        // STEP 1: Borrow a batch from the pool
        // At this point, we have EXCLUSIVE ownership
        let mut batch = pool.borrow();
        
        // STEP 2: Fill the batch
        while !batch.is_full() {
            match rx.recv_timeout(Duration::from_millis(1)) {
                Ok(request) => {
                    actor.append_to_batch(&mut batch, request);
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    // Batch timeout: flush partial batch
                    if !batch.is_empty() {
                        break;
                    }
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    // Shutdown signal
                    return;
                }
            }
        }
        
        // STEP 3: Sort in place (we still own the batch)
        batch.sort_in_place();
        
        // STEP 4: Transfer ownership to io_uring poller
        // After this line, we CANNOT access `batch` anymore
        if let Err(returned_batch) = handoff.send(batch) {
            // Backpressure: handoff queue full
            // Return batch to pool and apply backpressure upstream
            pool.return_batch(returned_batch);
            actor.apply_backpressure();
        }
        
        // Note: `batch` is now MOVED - Rust prevents us from using it
    }
}
```

### 15.6 The io_uring Poller Loop (Complete)

```rust
pub fn iouring_poll_loop(
    ring: &mut IoUring,
    handoff: Arc<HandoffQueue>,
    pool: Arc<BatchPool>,
) {
    // Track in-flight batches (keyed by SQE user_data)
    let mut in_flight: HashMap<u64, LoanableBatch> = HashMap::new();
    let mut next_id: u64 = 0;
    
    loop {
        // STEP 1: Receive batches from handoff queue
        while let Some(batch) = handoff.recv() {
            let id = next_id;
            next_id += 1;
            
            // Build SQE with batch ID as user_data
            let sqe = build_write_sqe(&batch, id);
            
            // Submit to io_uring
            unsafe { ring.submission().push(&sqe).expect("SQ full") };
            
            // Track ownership: batch is now "in flight"
            in_flight.insert(id, batch);
        }
        
        // STEP 2: Submit pending SQEs
        ring.submit().expect("io_uring submit failed");
        
        // STEP 3: Harvest completions
        for cqe in ring.completion() {
            let id = cqe.user_data();
            let result = cqe.result();
            
            // Reclaim ownership of the batch
            if let Some(batch) = in_flight.remove(&id) {
                if result < 0 {
                    error!(
                        target: "lance::io",
                        batch_id = batch.batch_id,
                        error = result,
                        "Write failed"
                    );
                    metrics::increment!("lance_write_errors");
                }
                
                // STEP 4: Return batch to pool for reuse
                // Ownership transfers back to the pool
                pool.return_batch(batch);
            }
        }
        
        // Yield if no work
        if in_flight.is_empty() {
            std::hint::spin_loop();
        }
    }
}
```

### 15.7 Safety Guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| **No data races** | Rust ownership: only one actor can access batch at a time |
| **No use-after-free** | Batch returned to pool only after CQE confirms write complete |
| **No double-free** | `ArrayQueue` is bounded; `push` fails if pool is full |
| **No locks** | `ArrayQueue` is lock-free; ownership transfer is atomic |

### 15.8 Debugging: Ownership Verification

In debug builds, verify exclusive ownership before mutation:

```rust
impl LoanableBatch {
    #[cfg(debug_assertions)]
    pub fn assert_exclusive_ownership(&self) {
        // If this batch is wrapped in Arc, verify refcount == 1
        // This catches bugs where someone kept a reference
    }
}

// Usage in ingestion loop
#[cfg(debug_assertions)]
batch.assert_exclusive_ownership();
```
## 16. The LANCE Wire Protocol (LWP)

To prevent "stitching" fragments in memory (a common Go bottleneck), the LWP is designed for **Fixed-Length Framing** with zero-allocation parsing.

**Implementation**: See `lnc-network/src/protocol.rs` for `LwpHeader`, `LwpFlags`, `ControlCommand`, and `IngestHeader`. Frame handling is in `lnc-network/src/frame.rs`.

### 16.1 Frame Layout

Every incoming stream request begins with an LWP Frame Header:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                         LWP FRAME (Network → LANCE)                           │
├───────────┬─────────┬───────┬──────────┬────────────┬─────────────────────────┤
│   magic   │ version │ flags │ reserved │ header_crc │     ingest_header       │
│ (4 bytes) │(1 byte) │(1 byte)│(2 bytes)│ (4 bytes)  │      (32 bytes)         │
├───────────┴─────────┴───────┴──────────┴────────────┴─────────────────────────┤
│                              payload (N bytes)                                │
└───────────────────────────────────────────────────────────────────────────────┘
                    Total Frame Header: 44 bytes (fixed)
```

```rust
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const LWP_MAGIC: [u8; 4] = [0x4C, 0x41, 0x4E, 0x43]; // 'LANC'
pub const LWP_VERSION: u8 = 1;

/// LWP Frame Header - exactly 44 bytes, cache-line friendly
#[repr(C, align(8))]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Copy, Clone)]
pub struct LwpFrameHeader {
    pub magic: [u8; 4],           // 0x4C 0x41 0x4E 0x43 ('LANC')
    pub version: u8,              // Protocol version (currently 1)
    pub flags: LwpFlags,          // Feature flags
    pub reserved: u16,            // Future use (must be 0)
    pub header_crc: u32,          // CRC32C of bytes 0-39 (everything except this field)
    pub ingest_header: IngestHeader, // 32 bytes (from Section 13.3)
} // Total: 44 bytes

#[repr(transparent)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Copy, Clone)]
pub struct LwpFlags(u8);

impl LwpFlags {
    pub const NONE: Self = Self(0);
    pub const COMPRESSED: Self = Self(1 << 0);    // Bit 0: Payload is LZ4 compressed
    pub const ENCRYPTED: Self = Self(1 << 1);     // Bit 1: Payload is encrypted
    pub const KEEPALIVE: Self = Self(1 << 2);     // Bit 2: Keepalive frame (no payload)
    pub const BATCH: Self = Self(1 << 3);         // Bit 3: Multiple records in payload
    
    pub fn is_compressed(&self) -> bool { self.0 & Self::COMPRESSED.0 != 0 }
    pub fn is_encrypted(&self) -> bool { self.0 & Self::ENCRYPTED.0 != 0 }
    pub fn is_keepalive(&self) -> bool { self.0 & Self::KEEPALIVE.0 != 0 }
    pub fn is_batch(&self) -> bool { self.0 & Self::BATCH.0 != 0 }
}
```

### 16.2 Zero-Allocation Framing

The Network Actor uses a **per-connection Ring Buffer** to avoid fragmentation.

```rust
/// Per-connection receive buffer (ring buffer)
pub struct ConnectionBuffer {
    buffer: Box<[u8; CONNECTION_BUFFER_SIZE]>,  // 64KB default
    read_pos: usize,
    write_pos: usize,
}

impl ConnectionBuffer {
    pub fn new() -> Self {
        Self {
            buffer: Box::new([0u8; CONNECTION_BUFFER_SIZE]),
            read_pos: 0,
            write_pos: 0,
        }
    }
    
    /// Available space for writing
    #[inline]
    pub fn writable(&mut self) -> &mut [u8] {
        &mut self.buffer[self.write_pos..]
    }
    
    /// Advance write position after recv()
    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.write_pos += n;
    }
    
    /// Available data for reading
    #[inline]
    pub fn readable(&self) -> &[u8] {
        &self.buffer[self.read_pos..self.write_pos]
    }
    
    /// Advance read position after consuming data
    #[inline]
    pub fn advance_read(&mut self, n: usize) {
        self.read_pos += n;
        
        // Compact buffer when read catches up to write
        if self.read_pos == self.write_pos {
            self.read_pos = 0;
            self.write_pos = 0;
        }
    }
    
    /// Handle wrap-around: copy remaining data to start
    pub fn compact(&mut self) {
        if self.read_pos > 0 {
            let remaining = self.write_pos - self.read_pos;
            self.buffer.copy_within(self.read_pos..self.write_pos, 0);
            self.read_pos = 0;
            self.write_pos = remaining;
        }
    }
}
```

### 16.3 Frame Parsing State Machine

```rust
#[derive(Debug)]
enum FrameState {
    /// Waiting for 44-byte header
    AwaitingHeader,
    /// Header received, waiting for payload
    AwaitingPayload { header: LwpFrameHeader, payload_len: usize },
}

pub struct FrameParser {
    state: FrameState,
    buffer: ConnectionBuffer,
}

impl FrameParser {
    pub fn new() -> Self {
        Self {
            state: FrameState::AwaitingHeader,
            buffer: ConnectionBuffer::new(),
        }
    }
    
    /// Attempt to parse a complete frame from the buffer
    pub fn try_parse(&mut self) -> Result<Option<ParsedFrame>> {
        loop {
            match &self.state {
                FrameState::AwaitingHeader => {
                    let readable = self.buffer.readable();
                    
                    if readable.len() < LWP_HEADER_SIZE {
                        return Ok(None); // Need more data
                    }
                    
                    // Zero-copy header parsing
                    let header = LwpFrameHeader::read_from_prefix(readable)
                        .ok_or(LanceError::InvalidFrame)?;
                    
                    // Validate magic
                    if header.magic != LWP_MAGIC {
                        return Err(LanceError::InvalidMagic);
                    }
                    
                    // Validate header CRC (covers bytes 0-39)
                    let expected_crc = crc32fast::hash(&readable[..40]);
                    if header.header_crc != expected_crc {
                        return Err(LanceError::HeaderCrcMismatch);
                    }
                    
                    // Handle keepalive (no payload)
                    if header.flags.is_keepalive() {
                        self.buffer.advance_read(LWP_HEADER_SIZE);
                        return Ok(Some(ParsedFrame::Keepalive));
                    }
                    
                    let payload_len = header.ingest_header.payload_len as usize;
                    self.buffer.advance_read(LWP_HEADER_SIZE);
                    self.state = FrameState::AwaitingPayload { header, payload_len };
                }
                
                FrameState::AwaitingPayload { header, payload_len } => {
                    let readable = self.buffer.readable();
                    
                    if readable.len() < *payload_len {
                        return Ok(None); // Need more data
                    }
                    
                    // Payload is now contiguous in the buffer
                    let payload_slice = &readable[..*payload_len];
                    
                    // Validate payload checksum (from IngestHeader)
                    let expected_checksum = header.ingest_header.checksum;
                    let actual_checksum = crc32fast::hash(payload_slice);
                    if expected_checksum != actual_checksum {
                        return Err(LanceError::PayloadCrcMismatch);
                    }
                    
                    let frame = ParsedFrame::Record {
                        header: *header,
                        payload_start: self.buffer.read_pos,
                        payload_len: *payload_len,
                    };
                    
                    self.buffer.advance_read(*payload_len);
                    self.state = FrameState::AwaitingHeader;
                    
                    return Ok(Some(frame));
                }
            }
        }
    }
}

pub enum ParsedFrame {
    Keepalive,
    Record {
        header: LwpFrameHeader,
        payload_start: usize,  // Offset in ConnectionBuffer
        payload_len: usize,
    },
}
```

### 16.4 Handling Wrap-Around

When the payload spans the ring buffer wrap-around point:

```rust
impl FrameParser {
    /// Handle case where payload would wrap around buffer end
    fn handle_wraparound(&mut self, payload_len: usize) -> Result<()> {
        let space_to_end = self.buffer.buffer.len() - self.buffer.read_pos;
        
        if payload_len > space_to_end {
            // Payload would wrap - compact buffer first
            self.buffer.compact();
            
            // If still not enough space, buffer is too small
            if payload_len > self.buffer.buffer.len() - self.buffer.write_pos {
                return Err(LanceError::PayloadTooLarge);
            }
        }
        
        Ok(())
    }
}
```

### 16.5 Network Actor Integration (Zero-Copy)

**⚠️ Critical**: The naive implementation would use `payload.to_vec()` here—this is a **Review Blocker**. At 10M records/second, `to_vec()` hammers the global allocator with lock contention, violating Section 18.1.

#### The Sliced Buffer Loan Pattern

Instead of copying, we use the `bytes` crate for reference-counted buffer slicing:

```rust
use bytes::{Bytes, BytesMut};

/// Per-connection buffer using Bytes for zero-copy slicing
pub struct ConnectionBuffer {
    buffer: BytesMut,
    capacity: usize,
}

impl ConnectionBuffer {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = BytesMut::with_capacity(capacity);
        buffer.resize(capacity, 0);
        Self { buffer, capacity }
    }
    
    /// Get a zero-copy slice of the payload
    /// Returns Bytes (reference-counted, no allocation)
    #[inline]
    pub fn slice(&self, start: usize, len: usize) -> Bytes {
        self.buffer.clone().freeze().slice(start..start + len)
    }
    
    /// Writable region for recv()
    #[inline]
    pub fn writable(&mut self) -> &mut [u8] {
        &mut self.buffer[..]
    }
}

/// IngestRequest carries a Bytes view, NOT a Vec<u8>
pub struct IngestRequest {
    pub header: IngestHeader,
    pub payload: Bytes,  // ← Zero-copy: reference into ConnectionBuffer
}

impl NetworkActor {
    pub async fn handle_connection(&mut self, mut socket: TcpStream) -> Result<()> {
        // Pre-allocate buffer once per connection
        let mut buffer = ConnectionBuffer::new(CONNECTION_BUFFER_SIZE);
        let mut parser = FrameParser::new();
        
        loop {
            // Read into buffer
            let n = socket.read(buffer.writable()).await?;
            if n == 0 {
                return Ok(()); // Connection closed
            }
            parser.advance_write(n);
            
            // Parse all complete frames
            while let Some(frame) = parser.try_parse()? {
                match frame {
                    ParsedFrame::Keepalive => {
                        self.send_keepalive_ack(&mut socket).await?;
                    }
                    ParsedFrame::Record { header, payload_start, payload_len } => {
                        // ✅ ZERO-COPY: Bytes::slice() increments refcount, no allocation
                        let payload = buffer.slice(payload_start, payload_len);
                        
                        // Send to Ingestion Actor (ownership of Bytes view transfers)
                        self.ingestion_tx.send(IngestRequest {
                            header: header.ingest_header,
                            payload,  // No .to_vec()!
                        }).await?;
                    }
                }
            }
            
            // Compact buffer after processing (Bytes slices remain valid due to refcount)
            parser.compact_if_needed(&mut buffer);
        }
    }
}
```

#### Why `Bytes` Works

| Operation | `Vec<u8>` | `Bytes` |
|-----------|-----------|---------|
| **Slice** | `to_vec()` = allocation + copy | `slice()` = refcount increment |
| **Send to channel** | Move (or clone = copy) | Move (cheap, just pointer + refcount) |
| **Drop** | Dealloc | Decrement refcount; dealloc when zero |

**Memory model**: The `ConnectionBuffer` owns the backing `BytesMut`. Each `IngestRequest.payload` is a `Bytes` view into that buffer. When the Ingestion Actor finishes processing and drops the `Bytes`, the refcount decrements. The `ConnectionBuffer` can only be reused/compacted when all outstanding `Bytes` slices are dropped.

#### Cargo.toml

```toml
[dependencies]
bytes = "1.5"  # Zero-copy buffer slicing
```

#### Backpressure Integration

If the Ingestion Actor is slow, `Bytes` slices accumulate and the `ConnectionBuffer` cannot compact. This naturally applies backpressure:

```rust
impl ConnectionBuffer {
    /// Check if buffer can be compacted (no outstanding slices)
    pub fn can_compact(&self) -> bool {
        // BytesMut tracks if any Bytes slices reference it
        self.buffer.is_unique()
    }
    
    pub fn compact_or_backpressure(&mut self) -> Result<()> {
        if !self.can_compact() {
            // Outstanding Bytes slices prevent compaction
            // This means Ingestion Actor is falling behind
            metrics::increment!("lance_network_backpressure");
            return Err(LanceError::BackpressureRequired);
        }
        // Safe to compact...
        Ok(())
    }
}
```

### 16.6 Protocol Extensions

| Flag | Behavior |
|------|----------|
| `COMPRESSED` | Payload is LZ4-compressed; decompress before checksum validation |
| `ENCRYPTED` | Payload encrypted with session key; decrypt before processing |
| `BATCH` | Payload contains multiple TLV records (for high-throughput clients) |

### 16.7 Wire Protocol Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `lance_lwp_frames_received` | Counter | Total frames received |
| `lance_lwp_invalid_magic` | Counter | Frames with bad magic (protocol error) |
| `lance_lwp_header_crc_fail` | Counter | Header CRC mismatches |
| `lance_lwp_payload_crc_fail` | Counter | Payload CRC mismatches |
| `lance_lwp_keepalives` | Counter | Keepalive frames processed |
## 17. Consumer Read Path (Zero-Copy Out)

Reading data from a stream engine shouldn't involve the CPU. We want to move bytes from the NVMe controller directly to the NIC, bypassing user-space memory.

### 17.1 The Cold Storage Trap

**The Risk**: A consumer requesting a 10GB replay might:
- Hog an io_uring poller, starving ingestion writes
- Saturate the NIC, causing head-of-line blocking
- Consume all available read buffers

**The Solution**: Separate I/O paths with QoS prioritization.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         READ PATH ISOLATION                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  INGESTION (High Priority)              CONSUMER (Low Priority)             │
│  ┌─────────────────────┐                ┌─────────────────────┐             │
│  │   io_uring Ring 0   │                │   io_uring Ring 1   │             │
│  │   (WRITE only)      │                │   (READ + SEND)     │             │
│  │   SQ depth: 4096    │                │   SQ depth: 1024    │             │
│  │   IOPRIO: REALTIME  │                │   IOPRIO: IDLE      │             │
│  └──────────┬──────────┘                └──────────┬──────────┘             │
│             │                                      │                        │
│             ▼                                      ▼                        │
│       ┌─────────┐                           ┌─────────┐                     │
│       │  NVMe 0 │                           │  NVMe 0 │ (same device,       │
│       └─────────┘                           └─────────┘  different queue)   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 17.2 I/O Prioritization (Quality of Service)

Linux io_uring supports I/O priority classes via `ioprio`:

```rust
use io_uring::{opcode, types::Fd};

/// I/O priority classes (Linux-specific)
pub mod ioprio {
    pub const CLASS_SHIFT: u16 = 13;
    pub const CLASS_RT: u16 = 1;      // Real-time (highest)
    pub const CLASS_BE: u16 = 2;      // Best-effort (default)
    pub const CLASS_IDLE: u16 = 3;    // Idle (lowest)
    
    /// Build ioprio value: class in high bits, data in low bits
    pub const fn build(class: u16, data: u16) -> u16 {
        (class << CLASS_SHIFT) | (data & 0x1FFF)
    }
}

/// Submit a high-priority write (ingestion path)
pub fn submit_write_high_priority(
    ring: &mut IoUring,
    fd: Fd,
    buf: &[u8],
    offset: u64,
) -> Result<()> {
    let sqe = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
        .offset(offset)
        .build()
        .ioprio(ioprio::build(ioprio::CLASS_RT, 0));  // Real-time priority
    
    unsafe { ring.submission().push(&sqe)? };
    Ok(())
}

/// Submit a low-priority read (consumer path)
pub fn submit_read_low_priority(
    ring: &mut IoUring,
    fd: Fd,
    buf: &mut [u8],
    offset: u64,
) -> Result<()> {
    let sqe = opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
        .offset(offset)
        .build()
        .ioprio(ioprio::build(ioprio::CLASS_IDLE, 7));  // Idle priority, level 7
    
    unsafe { ring.submission().push(&sqe)? };
    Ok(())
}
```

**Important**: I/O priority is only honored by certain schedulers:
- `mq-deadline`: Supports priority (recommended for NVMe)
- `kyber`: Supports priority  
- `none`/`noop`: Ignores priority

```bash
# Verify and set scheduler
cat /sys/block/nvme0n1/queue/scheduler
# [mq-deadline] kyber bfq none

echo mq-deadline > /sys/block/nvme0n1/queue/scheduler
```

### 17.3 Zero-Copy Send with io_uring

For large replays, use `IORING_OP_SEND_ZC` to avoid copying data through user-space:

```rust
/// Zero-copy send: NVMe → Kernel Page Cache → NIC
/// No user-space memory touched for the payload
pub struct ZeroCopySender {
    ring: IoUring,
    socket_fd: RawFd,
    segment_fd: RawFd,
    /// Pre-registered buffer pool for DMA
    buffer_pool: RegisteredBufferPool,
}

impl ZeroCopySender {
    /// Stream a segment range to a consumer socket
    pub async fn stream_segment(
        &mut self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<u64> {
        let mut sent: u64 = 0;
        let mut offset = start_offset;
        
        while offset < end_offset {
            let chunk_size = std::cmp::min(
                end_offset - offset,
                SEND_CHUNK_SIZE as u64,  // 64KB chunks
            ) as u32;
            
            // Step 1: Read from segment into registered buffer
            let buf_idx = self.buffer_pool.acquire()?;
            let read_sqe = opcode::Read::new(
                types::Fd(self.segment_fd),
                self.buffer_pool.ptr(buf_idx),
                chunk_size,
            )
            .offset(offset)
            .build()
            .ioprio(ioprio::build(ioprio::CLASS_IDLE, 7))
            .user_data(buf_idx as u64 | READ_TAG);
            
            // Step 2: Zero-copy send to socket (linked operation)
            let send_sqe = opcode::SendZc::new(
                types::Fd(self.socket_fd),
                self.buffer_pool.ptr(buf_idx),
                chunk_size,
            )
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK)  // Chain to read
            .user_data(buf_idx as u64 | SEND_TAG);
            
            unsafe {
                self.ring.submission().push(&read_sqe)?;
                self.ring.submission().push(&send_sqe)?;
            }
            
            self.ring.submit_and_wait(2)?;
            
            // Process completions
            for cqe in self.ring.completion() {
                if cqe.result() < 0 {
                    return Err(io::Error::from_raw_os_error(-cqe.result()).into());
                }
                
                let tag = cqe.user_data();
                if tag & SEND_TAG != 0 {
                    sent += cqe.result() as u64;
                }
                
                // Release buffer back to pool
                let buf_idx = (tag & !TAG_MASK) as usize;
                self.buffer_pool.release(buf_idx);
            }
            
            offset += chunk_size as u64;
        }
        
        Ok(sent)
    }
}
```

### 17.4 Registered Buffer Pool

Pre-register buffers with the kernel to avoid per-operation page pinning:

```rust
pub struct RegisteredBufferPool {
    buffers: Vec<AlignedBuffer>,
    free_list: ArrayQueue<usize>,
    ring_fd: RawFd,
}

impl RegisteredBufferPool {
    pub fn new(ring: &mut IoUring, num_buffers: usize, buffer_size: usize) -> Result<Self> {
        let mut buffers: Vec<AlignedBuffer> = Vec::with_capacity(num_buffers);
        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(num_buffers);
        
        for _ in 0..num_buffers {
            let buf = AlignedBuffer::new(buffer_size)?;
            iovecs.push(libc::iovec {
                iov_base: buf.as_ptr() as *mut _,
                iov_len: buffer_size,
            });
            buffers.push(buf);
        }
        
        // Register buffers with io_uring for zero-copy
        ring.submitter().register_buffers(&iovecs)?;
        
        let free_list = ArrayQueue::new(num_buffers);
        for i in 0..num_buffers {
            free_list.push(i).unwrap();
        }
        
        Ok(Self {
            buffers,
            free_list,
            ring_fd: ring.as_raw_fd(),
        })
    }
    
    #[inline]
    pub fn acquire(&self) -> Result<usize> {
        self.free_list.pop().ok_or(LanceError::BufferPoolExhausted)
    }
    
    #[inline]
    pub fn release(&self, idx: usize) {
        let _ = self.free_list.push(idx);
    }
    
    #[inline]
    pub fn ptr(&self, idx: usize) -> *mut u8 {
        self.buffers[idx].as_ptr() as *mut u8
    }
}
```

### 17.5 Consumer Rate Limiting

Prevent any single consumer from monopolizing resources:

```rust
pub struct ConsumerRateLimiter {
    /// Bytes per second limit
    rate_limit: u64,
    /// Token bucket state
    tokens: AtomicU64,
    last_refill: AtomicU64,
}

impl ConsumerRateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            rate_limit: bytes_per_second,
            tokens: AtomicU64::new(bytes_per_second),  // Start with 1 second of tokens
            last_refill: AtomicU64::new(current_time_ms()),
        }
    }
    
    /// Try to consume tokens; returns how many bytes allowed
    pub fn try_consume(&self, requested: u64) -> u64 {
        self.refill();
        
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            let allowed = std::cmp::min(current, requested);
            
            if allowed == 0 {
                return 0;
            }
            
            if self.tokens.compare_exchange_weak(
                current,
                current - allowed,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ).is_ok() {
                return allowed;
            }
        }
    }
    
    fn refill(&self) {
        let now = current_time_ms();
        let last = self.last_refill.load(Ordering::Relaxed);
        let elapsed_ms = now.saturating_sub(last);
        
        if elapsed_ms > 0 {
            let refill_amount = (self.rate_limit * elapsed_ms) / 1000;
            self.tokens.fetch_add(refill_amount, Ordering::Relaxed);
            self.last_refill.store(now, Ordering::Relaxed);
            
            // Cap at 1 second worth of tokens (burst limit)
            let current = self.tokens.load(Ordering::Relaxed);
            if current > self.rate_limit {
                self.tokens.store(self.rate_limit, Ordering::Relaxed);
            }
        }
    }
}
```

### 17.6 Read Path Summary

| Step | Mechanism | Priority |
|------|-----------|----------|
| 1. Request | Sparse Index Lookup | N/A |
| 2. Seek | Binary search in mmap'd index | N/A |
| 3. Read | `IORING_OP_READ` with `IOPRIO_CLASS_IDLE` | Low |
| 4. Send | `IORING_OP_SEND_ZC` (zero-copy) | Low |
| 5. Rate Limit | Token bucket per consumer | Configurable |

### 17.7 Read Path Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `lance_reads_total` | Counter | Total read operations |
| `lance_read_bytes_total` | Counter | Total bytes read |
| `lance_read_latency_ns` | Histogram | Read operation latency |
| `lance_consumer_throttled_total` | Counter | Rate limit events |
| `lance_zero_copy_sends_total` | Counter | Zero-copy sends completed |
## 18. Mechanical Integrity Checklist (Engineering Audit)

This checklist serves as a **mandatory code review gate** for all LANCE contributions. Any violation blocks merge.

### 18.1 Hot Path Rules (Zero Tolerance)

| Rule | Violation | Detection |
|------|-----------|-----------|
| **No Mutex on Hot Path** | `std::sync::Mutex`, `parking_lot::Mutex` in ingestion/IO code | `grep -r "Mutex" src/ingestion src/io` |
| **No Clone on Payloads** | `.clone()` on `Vec<u8>`, `IngestionBatch`, or payload buffers | Clippy lint + manual review |
| **No Allocation in Poller** | `Vec::new()`, `Box::new()`, `String::from()` in io_uring loop | Custom lint rule |
| **No Dynamic Dispatch** | `Box<dyn Trait>`, `&dyn Trait` on hot path | `grep -r "dyn " src/ingestion src/io` |
| **No Unwrap in Production** | `.unwrap()`, `.expect()` outside tests | `#![deny(clippy::unwrap_used)]` |

### 18.2 Mandatory Code Patterns

#### ✅ Ownership Transfer (Not Sharing)

```rust
// ✅ CORRECT: Ownership moves through the pipeline
fn process(batch: LoanableBatch) {  // Takes ownership
    handoff.send(batch);             // Transfers ownership
    // batch is now inaccessible - Rust enforces this
}

// ❌ WRONG: Shared mutable state
fn process(batch: Arc<Mutex<LoanableBatch>>) {
    let guard = batch.lock().unwrap();  // LOCK CONTENTION
    // ...
}
```

#### ✅ Static Dispatch (Not Dynamic)

```rust
// ✅ CORRECT: Compiler monomorphizes, enables inlining
fn encode<E: Encoder>(encoder: &E, data: &[u8]) -> Vec<u8> {
    encoder.encode(data)  // Inlined at compile time
}

// ❌ WRONG: Virtual call overhead
fn encode(encoder: &dyn Encoder, data: &[u8]) -> Vec<u8> {
    encoder.encode(data)  // vtable lookup at runtime
}
```

#### ✅ Pre-allocated Buffers (Not Grow-on-Demand)

```rust
// ✅ CORRECT: Pre-allocate at startup
let pool = BatchPool::new(64, BATCH_SIZE, numa_node)?;

// ❌ WRONG: Allocate per request
fn handle_request(data: &[u8]) {
    let buffer = Vec::with_capacity(data.len());  // ALLOCATION
}
```

#### ✅ Atomic Counters (Not Locked Counters)

```rust
// ✅ CORRECT: Lock-free increment
static RECORDS_INGESTED: AtomicU64 = AtomicU64::new(0);
RECORDS_INGESTED.fetch_add(1, Ordering::Relaxed);

// ❌ WRONG: Mutex-protected counter
static RECORDS_INGESTED: Mutex<u64> = Mutex::new(0);
*RECORDS_INGESTED.lock().unwrap() += 1;  // LOCK
```

### 18.3 Memory Rules

| Rule | Requirement |
|------|-------------|
| **NUMA Locality** | Every `IngestionBatch` allocated on same NUMA node as processing thread |
| **Alignment** | All I/O buffers 4KB-aligned for O_DIRECT |
| **Cache Lines** | Hot structures ≤64 bytes or padded to avoid false sharing |
| **No Page Faults** | All buffers pre-faulted at startup (`mlock` or touch pages) |

### 18.4 Thread Rules

| Rule | Requirement |
|------|-------------|
| **Pinning** | io_uring pollers pinned to isolated cores |
| **No Parking** | Poller threads spin, never call `thread::park()` or `Condvar::wait()` |
| **Core Isolation** | Production: `isolcpus` + `nohz_full` for LANCE cores |
| **Affinity Verification** | Startup logs must show thread → core mapping |

### 18.5 I/O Rules

| Rule | Requirement |
|------|-------------|
| **O_DIRECT** | All segment writes use direct I/O (bypass page cache) |
| **io_uring Probe** | Must probe io_uring at startup, log fallback prominently |
| **Priority Classes** | Writes: `IOPRIO_CLASS_RT`, Reads: `IOPRIO_CLASS_IDLE` |
| **Registered Buffers** | Consumer read path uses pre-registered buffer pool |

### 18.6 Protocol Rules

| Rule | Requirement |
|------|-------------|
| **CRC Validation** | All frames validated before processing |
| **Magic Check** | Invalid magic = immediate connection close |
| **Fixed Framing** | LWP header is exactly 44 bytes, always |
| **Keepalive** | Connections timeout after 30s without keepalive |

### 18.7 Observability Rules

| Rule | Requirement |
|------|-------------|
| **Non-Blocking Export** | Metrics use `Ordering::Relaxed`, never block |
| **Overflow = Drop** | Telemetry export drops on overflow, never blocks |
| **Startup Logging** | Log: io_uring status, NUMA topology, SIMD features, thread pinning |
| **Fallback Alerting** | `pwritev2` fallback triggers high-priority alert |

### 18.8 CI/CD Integration

```yaml
# .github/workflows/mechanical-integrity.yml
name: Mechanical Integrity

on: [pull_request]

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Check for Mutex on hot path
        run: |
          if grep -rE "Mutex|RwLock" src/ingestion src/io src/network; then
            echo "❌ FAIL: Mutex/RwLock found on hot path"
            exit 1
          fi
          
      - name: Check for dynamic dispatch
        run: |
          if grep -rE "dyn\s+" src/ingestion src/io; then
            echo "❌ FAIL: Dynamic dispatch found on hot path"
            exit 1
          fi
          
      - name: Check for unwrap in non-test code
        run: |
          cargo clippy -- -D clippy::unwrap_used -D clippy::expect_used
          
      - name: Check for clone on large types
        run: |
          # Custom script to detect .clone() on buffer types
          ./scripts/check-clone-abuse.sh
          
      - name: Verify alignment attributes
        run: |
          # Ensure all I/O structs have #[repr(C, align(...))]
          grep -rE "struct.*Batch|struct.*Buffer" src/ | \
            xargs -I{} sh -c 'grep -B2 "{}" | grep -q "repr.*align" || echo "Missing alignment: {}"'
```

### 18.9 Pre-Merge Checklist (Human Review)

Before approving any PR touching the hot path, reviewers must verify:

- [ ] **No new allocations** in ingestion loop, io_uring poller, or network parser
- [ ] **No new locks** anywhere in `src/ingestion`, `src/io`, `src/network`
- [ ] **Ownership flows linearly** (no `Arc<Mutex<_>>` patterns)
- [ ] **New structs are aligned** (`#[repr(C, align(8))]` minimum)
- [ ] **New metrics use atomics** with `Ordering::Relaxed`
- [ ] **Tests include latency assertions** (P99 < threshold)
- [ ] **NUMA-sensitive code uses `hwloc`** for topology discovery

### 18.10 Performance Regression Gate

```rust
#[test]
fn ingestion_latency_regression() {
    let result = benchmark_ingestion(1_000_000);  // 1M records
    
    assert!(result.p50_ns < 500, "P50 latency regression: {} ns", result.p50_ns);
    assert!(result.p99_ns < 5_000, "P99 latency regression: {} ns", result.p99_ns);
    assert!(result.p999_ns < 50_000, "P999 latency regression: {} ns", result.p999_ns);
    assert!(result.allocations == 0, "Unexpected allocations: {}", result.allocations);
}
```

---

**This checklist is not optional.** Any engineer who bypasses these rules introduces latency that compounds at 100Gbps. A 1μs lock acquisition per batch at 10M batches/second is 10 seconds of cumulative blocking per second—the system collapses.

The rules exist because they have been violated before. Learn from those mistakes.

---

## 19. Client-Side Offset Management

LANCE fundamentally differs from Kafka in its approach to offset management. Rather than server-side offset tracking with consumer groups managed by the broker, LANCE employs **client-side offset management** where consumers are fully responsible for tracking their position in the stream.

**Implementation**: See `lnc-client/src/offset.rs` for `OffsetStore` trait, `MemoryOffsetStore`, `LockFileOffsetStore`, and `HookedOffsetStore` with `PostCommitHook` support.

### 19.1 Design Philosophy

| Aspect | Kafka | LANCE |
|--------|-------|-------|
| Offset Storage | Server-side (broker) | Client-side (local/external) |
| Consumer Groups | Broker-managed | Client-coordinated or external |
| Rebalancing | Automatic (broker-driven) | Explicit (client-driven) |
| Server Resources | O(consumers × partitions) | O(1) per connection |
| Complexity Location | Server | Client library |

**Rationale**: By moving offset management to the client, LANCE servers remain stateless with respect to consumer progress. This enables:
- Horizontal scaling without consumer group coordination overhead
- Deterministic replay from any offset without server round-trips
- Zero server-side state per consumer (reduced memory, simplified failover)
- Client-controlled checkpointing strategies

### 19.2 Offset Storage Strategies

The client library supports pluggable offset storage via the `OffsetStore` trait:

```rust
pub trait OffsetStore: Send + Sync {
    fn load(&self, topic_id: u32, consumer_id: u64) -> Result<Option<u64>>;
    fn save(&self, topic_id: u32, consumer_id: u64, offset: u64) -> Result<()>;
}
```

#### 19.2.1 Lock File Store (Default)

File-based offset persistence with advisory locking:

```
/var/lance/offsets/
└── my-consumer/
    ├── topic_1_consumer_12345.offset
    └── topic_2_consumer_12345.offset
```

**Characteristics**:
- Single-consumer guarantee via `flock()`
- Atomic updates via write-rename
- In-memory cache with periodic flush
- Suitable for single-node deployments

#### 19.2.2 External Store (Production)

For distributed deployments, offsets should be stored in:
- **Redis**: Low-latency, suitable for high-frequency commits
- **PostgreSQL/MySQL**: Transactional guarantees, audit trail
- **etcd/Consul**: Consistent with service discovery

### 19.3 Seek and Fetch Model

Unlike Kafka's `poll()` model where the broker tracks position, LANCE uses explicit seek/fetch:

```rust
// Explicit positioning
consumer.seek(SeekPosition::Beginning).await?;
consumer.seek(SeekPosition::Offset(saved_offset)).await?;
consumer.seek(SeekPosition::End).await?;

// Fetch returns data + new offset
let result = consumer.poll().await?;
// result.current_offset is the offset AFTER this fetch

// Client decides when to commit
offset_store.save(topic_id, consumer_id, result.current_offset)?;
```

### 19.4 Replay and Rewind

Full replay capability without server coordination:

```rust
// Replay entire stream
consumer.rewind().await?;
while let Some(batch) = consumer.poll().await? {
    reprocess(batch);
}

// Seek to specific point
consumer.seek_to_offset(checkpoint_offset).await?;
```

### 19.5 At-Least-Once vs Exactly-Once

| Guarantee | Implementation |
|-----------|----------------|
| At-least-once | Commit offset AFTER processing |
| At-most-once | Commit offset BEFORE processing |
| Exactly-once | External transaction (process + commit atomically) |

```rust
// At-least-once pattern
loop {
    let batch = consumer.poll().await?;
    process(&batch)?;  // May fail
    consumer.commit().await?;  // Only if process succeeded
}
```

---

## 20. Consumer Client Modes

LANCE provides two consumer client modes designed to balance simplicity, scalability, and resource efficiency.

**Implementation**: See `lnc-client/src/consumer.rs` for `Consumer`, `StreamingConsumer`, `ConsumerConfig`, `SeekPosition`, and `PollResult`.

### 20.1 Mode Comparison

| Feature | Standalone Mode | Grouped Mode |
|---------|-----------------|--------------|
| Server Connections | 1 per consumer | 1 per group (shared) |
| Offset Management | Per-consumer | Per-group, partitioned |
| Coordination | None | External coordinator |
| Scalability | Linear (N connections) | Sublinear (1 connection) |
| Use Case | Simple consumers, replay | High-fan-out, microservices |

### 20.2 Standalone Consumer

Each consumer operates independently with its own connection and offset tracking.

```
┌─────────────────┐     ┌─────────────────┐
│   Consumer A    │     │   Consumer B    │
│  (connection 1) │     │  (connection 2) │
│  offset: 1000   │     │  offset: 500    │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│              LANCE Server               │
│         (no consumer state)             │
└─────────────────────────────────────────┘
```

**Use Cases**:
- Independent workers processing same stream
- Replay/reprocessing jobs
- Development and testing
- Simple single-consumer applications

```rust
use lnc_client::{StandaloneConsumer, ConsumerConfig, SeekPosition};

let config = ConsumerConfig::new(topic_id)
    .with_start_position(SeekPosition::Beginning)
    .with_poll_timeout(Duration::from_millis(100));

let mut consumer = StandaloneConsumer::new(client, config).await?;

while let Some(result) = consumer.poll().await? {
    process(&result.data);
    consumer.commit().await?;
}
```

### 20.3 Grouped Consumer

Multiple workers share a single connection through a coordinator, dramatically reducing server-side connection overhead.

```
┌─────────────────────────────────────────┐
│           Group Coordinator             │
│  (partition assignment, heartbeats)     │
└────────────────┬────────────────────────┘
                 │ single connection
                 ▼
┌─────────────────────────────────────────┐
│              LANCE Server               │
└─────────────────────────────────────────┘
                 ▲
                 │ assignments
    ┌────────────┼────────────┐
    │            │            │
┌───┴───┐   ┌────┴───┐   ┌────┴───┐
│Worker1│   │Worker2 │   │Worker3 │
│ t:1,3 │   │ t:2,4  │   │ t:5,6  │
└───────┘   └────────┘   └────────┘
```

**Benefits**:
- **Connection Efficiency**: 100 workers = 1 connection (vs 100 in Kafka)
- **Reduced Server Load**: No per-consumer state on server
- **Coordinated Rebalancing**: Workers get assigned topic partitions
- **Shared Offset Commits**: Single commit stream per group

```rust
use lnc_client::{GroupCoordinator, GroupConfig, GroupedConsumer, WorkerConfig};

// Start coordinator (one per group)
let coordinator = GroupCoordinator::new(GroupConfig {
    group_id: "my-group".into(),
    topics: vec![topic_id],
    assignment_strategy: AssignmentStrategy::RoundRobin,
    ..Default::default()
}).await?;

// Workers join the group
let worker = GroupedConsumer::join(
    coordinator.join_address(),
    client,
    WorkerConfig::default(),
).await?;

// Process assigned partitions
for topic_id in worker.assignments() {
    while let Some(result) = worker.poll(topic_id).await? {
        process(&result.data);
        worker.commit(topic_id, result.current_offset).await?;
    }
}
```

### 20.4 Assignment Strategies

| Strategy | Description |
|----------|-------------|
| RoundRobin | Distribute topics evenly across workers |
| Sticky | Minimize reassignment during rebalance |
| Range | Assign contiguous topic ranges |
| Custom | User-defined assignment function |

### 20.5 Coordinator Protocol

The coordinator handles:
1. **Worker Registration**: Workers announce availability
2. **Heartbeats**: Detect worker failures (configurable timeout)
3. **Assignment**: Distribute topics based on strategy
4. **Rebalancing**: Reassign on worker join/leave
5. **Offset Aggregation**: Collect and batch commit offsets

```
Worker                     Coordinator
   │                            │
   │──── JoinGroup ───────────>│
   │<─── Assignment ───────────│
   │                            │
   │──── Heartbeat ───────────>│
   │<─── HeartbeatAck ─────────│
   │                            │
   │──── CommitOffset ────────>│
   │<─── CommitAck ────────────│
   │                            │
   │──── LeaveGroup ──────────>│
   │                            │
```

### 20.6 Scaling Considerations

| Consumers | Standalone Connections | Grouped Connections |
|-----------|------------------------|---------------------|
| 10 | 10 | 1 |
| 100 | 100 | 1 |
| 1,000 | 1,000 | 1-10 (sharded groups) |
| 10,000 | 10,000 | 10-100 (sharded groups) |

**Recommendation**: Use Grouped mode when:
- Consumer count > 10 per topic
- Connection overhead is a concern
- Coordinated processing is required

### 20.7 Failure Handling

| Failure | Standalone Behavior | Grouped Behavior |
|---------|---------------------|------------------|
| Worker crash | Other workers unaffected | Coordinator reassigns topics |
| Coordinator crash | N/A | Workers reconnect, state in offset store |
| Network partition | Reconnect with backoff | Heartbeat timeout, rejoin |

## 21. Write Forwarding (Cluster Mode)

In clustered deployments, LANCE implements transparent write forwarding to support load-balancer-friendly operations where clients cannot directly connect to the leader.

**Implementation**: See `lnc-replication/src/forward.rs` for `LeaderConnectionPool`, `ForwardConfig`, `TeeForwardingStatus`, and `LocalWriteProcessor`. Zero-copy primitives are in `lnc-io/src/uring.rs` (`SpliceForwarder`, `TeeForwarder`).

### 21.1 Problem Statement

The traditional NOT_LEADER redirect approach is incompatible with production deployments because:

1. **Load Balancer Incompatibility**: Clients connect through L4/L7 load balancers that distribute connections based on least-sessions or round-robin. Clients cannot "choose" to connect to the leader.
2. **Sticky Sessions**: Once a client establishes a connection to a node, that session is sticky. The client cannot easily migrate to another node.
3. **Network Topology**: In many deployments, clients may not have direct network access to all cluster nodes—only the load balancer VIP is exposed.

### 21.2 Architecture

**Core Principle**: Clients connect to ANY node. Reads are served locally. Writes are transparently forwarded to the leader.

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Client    │────▶│ Load Balancer│────▶│  Any Node       │
│  (Producer/ │◀────│   (L4/L7)    │◀────│  (Follower/     │
│   Consumer) │     └──────────────┘     │   Leader)       │
└─────────────┘                          └────────┬────────┘
                                                  │
                                    ┌─────────────┴─────────────┐
                                    ▼                           ▼
                            [If Read]                    [If Write]
                            Serve locally                Forward to Leader
                            from replica                 via internal mesh
                                                               │
                                                               ▼
                                                    ┌─────────────────┐
                                                    │     Leader      │
                                                    │  (Raft commit)  │
                                                    └─────────────────┘
```

### 21.3 Request Classification

| Operation | Type | Handling |
|-----------|------|----------|
| `Ingest` | **Write** | Forward to leader |
| `CreateTopic` | **Write** | Forward to leader |
| `DeleteTopic` | **Write** | Forward to leader |
| `SetRetention` | **Write** | Forward to leader |
| `Fetch` | Read | Serve locally |
| `ListTopics` | Read | Serve locally |
| `GetTopic` | Read | Serve locally |
| `Subscribe` | Session | Handle locally |
| `Unsubscribe` | Session | Handle locally |
| `Keepalive` | Control | Handle locally |

### 21.4 Leader Connection Pool

Followers maintain a connection pool to the current leader for efficient write forwarding:

```rust
pub struct LeaderConnectionPool {
    /// Current leader address (client port)
    leader_addr: RwLock<Option<SocketAddr>>,
    /// Pool of available connections
    connections: Mutex<Vec<TcpStream>>,
    /// Configuration
    config: ForwardConfig,
}

pub struct ForwardConfig {
    pub pool_size: usize,           // Default: 8
    pub connect_timeout: Duration,   // Default: 5s
    pub forward_timeout: Duration,   // Default: 30s
}
```

**Key behaviors**:
- **On leader change**: Pool is drained (old connections are to the old leader)
- **Connection reuse**: Connections are returned to pool after successful forward
- **On-demand creation**: New connections created if pool is empty

### 21.5 Forwarding Flow

```
Client Request                    Follower                         Leader
     │                               │                               │
     │──── Ingest(batch_id=N) ──────▶│                               │
     │                               │                               │
     │                               │── forward_write(raw_bytes) ──▶│
     │                               │                               │
     │                               │◀──── ACK(batch_id=N) ─────────│
     │                               │                               │
     │◀──── ACK(batch_id=N) ─────────│                               │
     │                               │                               │
```

The follower forwards the **raw request bytes** to the leader (no re-encoding), and the leader's response is forwarded back to the client. The `batch_id` is preserved end-to-end.

### 21.6 Failure Handling

| Scenario | Detection | Behavior |
|----------|-----------|----------|
| Leader unreachable | Connection timeout/error | Return error to client, client retries |
| Leader changes mid-forward | Connection reset | Return error, client retries (hits new leader) |
| Follower crashes | Client TCP lost | Client reconnects via LB to another node |
| Network partition | Raft election timeout | New leader elected, forwarding resumes |

### 21.7 Backpressure Propagation

Write forwarding naturally propagates backpressure through TCP flow control:

```
┌──────────┐    ┌──────────┐    ┌──────────┐
│  Client  │    │ Follower │    │  Leader  │
│          │    │          │    │          │
│ [buffer] │◀──▶│ [forward]│◀──▶│ [buffer] │
└──────────┘    └──────────┘    └──────────┘
     ▲               │               │
     │               │               ▼
     │          If leader buffer     Leader under
     │          full, write blocks   pressure
     │               │
     └───────────────┘
   TCP window shrinks,
   client throttled
```

No explicit backpressure signaling is required—TCP handles it automatically.

### 21.8 Zero-Copy Splice Forwarding (Linux)

On Linux with kernel 5.6+, LANCE uses `IORING_OP_SPLICE` for zero-copy write forwarding, eliminating userspace memory copies for 100Gbps line rates.

```rust
// lnc-io exports splice primitives
pub use uring::{
    probe_splice, SpliceForwarder, SplicePipe, SpliceSupport,
};

/// Zero-copy splice forwarder using io_uring
pub struct SpliceForwarder {
    ring: IoUring,
    pending_ops: u32,
    splice_supported: bool,
}

/// Data flow: client_socket → pipe → leader_socket
/// No userspace memory copies involved
impl SpliceForwarder {
    pub fn submit_forward_splice(
        &mut self,
        source_fd: RawFd,
        dest_fd: RawFd,
        pipe: &SplicePipe,
        len: u32,
        user_data: u64,
    ) -> Result<()>;
}
```

**Characteristics**:
- **Zero userspace copies**: Data moves kernel-to-kernel via pipe buffer
- **Linked operations**: Source→pipe and pipe→dest are atomically linked
- **Feature detection**: `probe_splice()` checks kernel support at startup
- **Graceful fallback**: Falls back to direct buffer copy on unsupported kernels

### 21.9 Zero-Copy Tee for L3 Quorum (Phase 3 - Implemented)

For L3 quorum scenarios where writes need both forwarding AND local processing, LANCE uses `IORING_OP_TEE` for in-kernel data duplication:

```
Data flow with TEE:
  client_socket → pipe1 → leader_socket (forwarding)
                       ↘ pipe2 → local_processor (teeing)
```

**Use cases**:
- **L3 quorum**: Forward to leader while locally acknowledging for quorum
- **Audit logging**: Copy writes to audit trail without performance impact
- **Real-time analytics**: Stream writes to analytics pipeline

```rust
// lnc-io exports TEE primitives
pub use uring::{probe_tee, TeeForwarder, TeeSupport};

// lnc-replication exports L3 quorum configuration
pub use forward::{
    check_tee_support, ForwardConfig, TeeForwardingStatus,
    LocalWriteProcessor, AuditConfig, AuditLogWriter,
};

/// Tee forwarder for L3 quorum zero-copy forwarding
pub struct TeeForwarder {
    ring: IoUring,
    tee_supported: bool,
    splice_supported: bool,
}

impl TeeForwarder {
    /// Submit a tee operation to duplicate data in-kernel
    pub fn submit_forward_with_tee(
        &mut self,
        source_fd: RawFd,
        leader_fd: RawFd,
        local_fd: RawFd,
        pipe1: &SplicePipe,
        pipe2: &SplicePipe,
        len: u32,
        user_data: u64,
    ) -> Result<()>;
}
```

**Implementation status**: ✅ Implemented
- `TeeForwarder` and `probe_tee()` in `lnc-io/src/uring.rs`
- `TeeForwardingStatus` and `check_tee_support()` in `lnc-replication/src/forward.rs`
- `LocalWriteProcessor` trait for processing TEE'd data locally
- `AuditLogWriter` for audit logging pipeline in `lnc-replication/src/audit.rs`
- Configuration via `ForwardConfig::with_tee_forwarding()` and `with_local_ack()`

### 21.10 Industry Alignment

This architecture aligns with established patterns:

- **Kafka KIP-392**: Allows consumers to fetch from closest replica
- **Amazon Aurora**: Separate writer and reader endpoints with internal forwarding
- **CockroachDB**: Follower reads with write forwarding to leaseholder

---

## 22. Write Buffering & Deferred Sync Architecture

LANCE's current I/O backend uses **buffered file writes** with **deferred `fsync`** to amortize expensive storage-layer sync calls. This is the production-ready write path that replaces per-write `sync_data` calls with batch-oriented durability, dramatically improving throughput on network-attached storage (e.g., Ceph RBD).

> **Relationship to §3/§8**: The io_uring backend described in Sections 3 and 8 is the target architecture for bare-metal NVMe deployments. The write buffering architecture described here is the **current production implementation** that works on all Linux filesystems and storage backends without kernel feature dependencies.

**Implementation**:
- `lnc-io/src/segment.rs` — `SegmentWriter` with `BufWriter<File>`
- `lance/src/server/ingestion.rs` — Single-actor deferred flush loop
- `lance/src/server/multi_actor.rs` — Multi-actor deferred flush loop
- `lance/src/server/connection.rs` — ACK-after-durability via `oneshot` channel

### 22.1 Design Motivation

On network-attached storage (Ceph RBD, EBS, etc.), each `fdatasync()` / `sync_all()` call incurs a full round-trip to the storage backend — typically 5–50ms depending on network conditions and OSD load. With per-write sync, this limits throughput to ~20–200 writes/second regardless of payload size.

**Solution**: Accumulate writes in a userspace buffer and defer `fsync` until either a **size threshold** or **time deadline** is reached, then sync once for the entire batch.

### 22.2 SegmentWriter: BufWriter Integration

`SegmentWriter` wraps its `File` in a `BufWriter` with a 4 MiB capacity:

```rust
const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

pub struct SegmentWriter {
    file: BufWriter<File>,
    path: PathBuf,
    write_offset: u64,
    state: SegmentState,
    start_index: u64,
    record_count: u64,
}
```

All constructors (`create`, `create_with_index`, `open`) use `BufWriter::with_capacity(DEFAULT_WRITE_BUFFER_SIZE, file)`.

#### Write Methods

| Method | Behavior | Use Case |
|--------|----------|----------|
| `append(data)` | Write to buffer only, no sync | Hot-path ingestion (deferred flush) |
| `write_batch(data)` | `append` + increment `record_count` | Hot-path ingestion |
| `save(data)` | `append` + `flush()` + `sync_data()` | Per-write durability (fallback) |
| `save_at_offset(offset, data)` | `write_at_offset` + `flush()` + `sync_data()` | L3 follower path |
| `flush_buffer()` | `BufWriter::flush()` only (no sync) | Pre-sync buffer drain |
| `sync()` / `fsync()` | `flush()` + `sync_all()` | Durability checkpoint |

**Critical invariant**: Every method that calls `sync_data()` or `sync_all()` **must** call `BufWriter::flush()` first, otherwise buffered data would not reach the kernel before sync.

### 22.3 Deferred Flush in Ingestion Actors

The ingestion actor loops (both single-actor and multi-actor paths) implement a **dual-threshold deferred flush** strategy:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DEFERRED FLUSH STRATEGY                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Writes arrive on ingestion channel                                        │
│       │                                                                     │
│       ▼                                                                     │
│   ┌─────────────────────┐                                                   │
│   │  Collect batch       │  (up to MAX_BATCH=256 messages per iteration)    │
│   │  from channel        │                                                  │
│   └──────────┬──────────┘                                                   │
│              │                                                              │
│              ▼                                                              │
│   ┌─────────────────────┐                                                   │
│   │  write_batch()       │  Writes to BufWriter (no syscall if buffered)    │
│   │  (buffered, no sync) │                                                  │
│   └──────────┬──────────┘                                                   │
│              │                                                              │
│              ▼                                                              │
│   ┌─────────────────────────────────────────────┐                           │
│   │  dirty_bytes >= 4 MiB?  OR  5ms timeout?    │                           │
│   └──────────┬──────────────────────┬───────────┘                           │
│         YES  │                      │  NO                                   │
│              ▼                      ▼                                       │
│   ┌─────────────────────┐   ┌─────────────────┐                             │
│   │  flush_and_signal()  │   │  Continue loop   │                            │
│   │  • fsync all dirty   │   │  (wait for more  │                            │
│   │    topic writers     │   │   messages)      │                            │
│   │  • signal write_done │   └─────────────────┘                             │
│   │  • queue replication │                                                   │
│   └─────────────────────┘                                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Constants

| Constant | Value | Rationale |
|----------|-------|-----------|
| `MAX_BATCH` | 256 | Messages collected per channel drain iteration |
| `FLUSH_THRESHOLD` | 4 MiB | Matches `BufWriter` capacity; triggers size-based flush |
| `FLUSH_TIMEOUT` | 5 ms | Bounds tail latency for single-producer workloads |

#### Deferred State

Across batch iterations, the actor maintains:

```rust
struct BatchSuccess {
    write_done_tx: Option<oneshot::Sender<()>>,  // ACK signal
    topic_id: u32,
    payload: Bytes,
    payload_len: usize,
    write_id: u64,
    meta: WriteMeta,
}

let mut pending_signals: Vec<BatchSuccess> = Vec::new();
let mut dirty_topics: HashSet<u32> = HashSet::new();
let mut dirty_bytes: usize = 0;
```

### 22.4 flush_and_signal: The Durability Gate

`flush_and_signal` is the **single point** where data becomes durable and clients are notified:

```rust
fn flush_and_signal(
    topic_writers: &mut HashMap<u32, TopicWriter>,
    dirty_topics: &mut HashSet<u32>,
    pending_signals: &mut Vec<BatchSuccess>,
    replication_tx: &Option<mpsc::Sender<DataReplicationRequest>>,
) {
    // 1. fsync every dirty topic writer
    for &topic_id in dirty_topics.iter() {
        if let Some(tw) = topic_writers.get_mut(&topic_id) {
            tw.writer.fsync();  // flush BufWriter + sync_all
        }
    }
    dirty_topics.clear();

    // 2. Signal all pending write completions (ACK gate)
    // 3. Queue replication entries
    for success in pending_signals.drain(..) {
        if let Some(tx) = success.write_done_tx {
            let _ = tx.send(());  // Connection handler sends ACK to client
        }
        if let Some(rep_tx) = replication_tx {
            let _ = rep_tx.try_send(/* replication entry */);
        }
    }
}
```

**Ordering guarantee**: The `write_done_tx` signal is sent **after** `fsync` completes, ensuring the client ACK reflects actual storage durability.

### 22.5 ACK-After-Durability (Connection Handler)

The connection handler creates a `oneshot` channel per write request and embeds the sender in the `IngestionRequest`:

```
Client                  Connection Handler             Ingestion Actor
  │                            │                              │
  │── Ingest(payload) ────────▶│                              │
  │                            │── IngestionRequest ─────────▶│
  │                            │   { write_done_tx: Some(tx) }│
  │                            │                              │
  │                            │   (writes accumulate...)     │
  │                            │                              │
  │                            │                 fsync() ─────│
  │                            │◀──── write_done_rx ──────────│
  │                            │                              │
  │◀──── ACK ─────────────────│                              │
  │                            │                              │
```

**Key property**: The ACK to the client is delayed until the ingestion actor has fsynced the data to stable storage. This provides the same durability guarantee as per-write sync, but with amortized I/O cost.

### 22.6 Multi-Actor Path

The multi-actor ingestion path (`run_ingestion_actor_sync`) uses the same dual-threshold strategy but with `std::time::Instant` for timing (since it runs on a plain OS thread, not tokio):

```rust
let mut last_write = std::time::Instant::now();

// In the spin-wait loop:
if last_write.elapsed() >= FLUSH_TIMEOUT && !pending_signals.is_empty() {
    flush_and_signal_sync(...);
    dirty_bytes = 0;
    last_write = std::time::Instant::now();
}
```

### 22.7 Shutdown Flush

On graceful shutdown (SIGTERM/SIGINT), the ingestion actor loop exits and flushes any remaining buffered data:

```rust
// After loop exits:
if !pending_signals.is_empty() {
    flush_and_signal(&mut topic_writers, &mut dirty_topics, &mut pending_signals, &replication_tx);
}

// Close all topic writers (fsync + seal + rename)
for (_, mut tw) in topic_writers.drain() {
    tw.writer.fsync();
}
```

This ensures zero data loss for acknowledged writes during rolling restarts.

### 22.8 Performance Characteristics

| Metric | Per-Write Sync | Write Buffering (5ms) | Improvement |
|--------|---------------|----------------------|-------------|
| **fsync calls/sec** | ~200 (1 per write) | ~5–10 (batched) | **20–40×** |
| **L1 throughput** | ~8 msg/s | ~34 msg/s | **4.25×** |
| **L3 throughput** | ~32 msg/s | ~35 msg/s | ~1× (network-bound) |
| **Tail latency (P99)** | ~50ms (Ceph sync) | ~55ms (+5ms batch) | Negligible |

> **Note**: L3 throughput is dominated by quorum replication latency, not local I/O, so write buffering has minimal impact on L3. The primary beneficiary is L1 (standalone) mode on network-attached storage.

### 22.9 Durability Trade-offs

| Scenario | Behavior | Data Impact |
|----------|----------|-------------|
| **Graceful shutdown** | Buffer flushed + fsynced | Zero loss |
| **SIGKILL / OOM** | Up to 5ms of un-fsynced writes lost | Bounded by `FLUSH_TIMEOUT` |
| **Ceph RBD pod kill** | `sync_all` may not persist (storage-layer issue) | Requires O_DIRECT or L3 |
| **Power loss** | Same as SIGKILL | Bounded by `FLUSH_TIMEOUT` |

**Important**: The 5ms `FLUSH_TIMEOUT` bounds the worst-case data loss window. Unlike the aspirational io_uring + O_DIRECT path (§3/§8), the current `BufWriter` path relies on the kernel page cache and `sync_all()`, which may not guarantee persistence on all storage backends (notably Ceph RBD under pod kill).

### 22.10 Future: Migration to io_uring

The write buffering architecture is designed to be replaced by the io_uring backend (§3/§8) without changing the ingestion actor interface:

1. Replace `BufWriter<File>` with `IoUringWriter` in `SegmentWriter`
2. Replace `flush_buffer()` + `sync_all()` with `IORING_OP_FSYNC` submission
3. Replace `write_all()` with `IORING_OP_WRITE` with pre-registered buffers
4. Add `O_DIRECT` for cache-bypass durability

The `flush_and_signal` / ACK-after-durability pattern remains unchanged — only the underlying I/O submission mechanism changes.

---

## 23. CATCHING_UP Protocol

When a consumer requests data at an offset beyond what a server node currently holds, LANCE returns a dedicated **CATCHING_UP** frame rather than an empty fetch response or an error. This allows the consumer to distinguish "no new data yet" from "this node is behind on replication" and apply an appropriate backoff strategy.

**Implementation**:
- `lnc-network/src/protocol.rs` — `ControlCommand::CatchingUp` (`0x12`)
- `lnc-network/src/frame.rs` — `Frame::new_catching_up(current_max_offset)`
- `lance/src/server/command_handlers.rs` — Server-side detection and response
- `lnc-client/src/client.rs` — `parse_fetch_response` maps `0x12` to `ClientError::ServerCatchingUp`
- `lnc-client/src/consumer.rs` — `fetch_with_retry` backoff and stale-offset reset
- `lnc-client/src/error.rs` — `ClientError::ServerCatchingUp { server_offset }`

### 23.1 Wire Format

```
┌────────────────────────────────────────────────────────────────┐
│                  CATCHING_UP Frame (0x12)                       │
├────────────────────────┬───────────────────────────────────────┤
│  Control Command 0x12  │  Payload: current_max_offset (8B LE)  │
└────────────────────────┴───────────────────────────────────────┘
```

The 8-byte little-endian payload carries the server's current maximum data offset for the requested topic, enabling the client to track replication progress.

### 23.2 Server-Side Detection

In `handle_fetch_request`, the server checks whether the consumer's requested offset exceeds the topic's total data size:

```rust
if data.is_empty() && req.start_offset > 0 {
    let total = topic_registry.total_data_size(req.topic_id);
    if req.start_offset > total {
        return Frame::new_catching_up(total);
    }
}
```

This fires when:
- A follower has not yet replicated to the consumer's position
- The consumer was previously connected to a more-current node
- Crash recovery truncated data that the consumer had already read

### 23.3 Client-Side Handling

The consumer's `fetch_with_retry` handles `ServerCatchingUp` with a **fixed 5-second backoff** (not exponential) because the server is healthy — it's just behind on replication:

```
Consumer                          Server (Follower)
  │                                    │
  │── Fetch(offset=5000) ─────────────▶│
  │                                    │  total_data = 3000
  │◀── CATCHING_UP(3000) ─────────────│
  │                                    │
  │    (sleep 5s, no reconnect)        │
  │                                    │
  │── Fetch(offset=5000) ─────────────▶│
  │                                    │  total_data = 5500 (caught up)
  │◀── FetchResponse(data) ───────────│
  │                                    │
```

### 23.4 Stale Offset Reset

If the server's `current_max_offset` does not advance after **3 consecutive** CATCHING_UP responses, the consumer concludes the gap is permanent (crash-recovery truncation) and resets its offset to the server's position:

```rust
if stale_count >= STALE_RESET_THRESHOLD {
    tracing::warn!(
        old_offset = self.current_offset,
        new_offset = *server_offset,
        lost_bytes = self.current_offset.saturating_sub(*server_offset),
        "Data permanently unreachable — resetting consumer offset"
    );
    self.current_offset = *server_offset;
}
```

This prevents the consumer from blocking indefinitely on data that was lost during an unclean shutdown.

### 23.5 Interaction with Replication Modes

| Mode | When CATCHING_UP Fires | Expected Resolution |
|------|----------------------|---------------------|
| **L1** | After crash recovery truncates active segment | Stale reset after 3 attempts |
| **L3** | Consumer hits follower before replication catches up | Server catches up within seconds |
| **L3 (leader failover)** | New leader may be slightly behind old leader | Resolves after leader election |

---

## 24. Recursive Segment Recovery

LANCE stores segments in a topic-partitioned directory layout (`segments/0/`, `segments/1/`, …). Both startup recovery and segment closing must **recursively traverse** these subdirectories to find all segments requiring attention.

**Implementation**:
- `lnc-io/src/segment.rs` — `close_unclosed_segments(segments_dir)` (recursive)
- `lnc-recovery/src/segment_recovery.rs` — `find_segments_needing_recovery(data_dir)` (recursive)
- `lance/src/server/recovery.rs` — `perform_startup_recovery` orchestrator

### 24.1 Directory Layout

```
/var/lib/lance/
├── segments/
│   ├── 0/                              ← default/system topic
│   │   ├── 0_1706000000.lnc            ← active segment (needs recovery)
│   │   └── 0_1705000000-1705999999.lnc ← closed segment (safe)
│   ├── 42/                             ← user topic ID 42
│   │   ├── 100_1706100000.lnc          ← active segment
│   │   └── 0_1706000000-1706099999.lnc ← closed segment
│   └── 99/                             ← user topic ID 99
│       └── 0_1706200000.lnc            ← active segment
└── wal/                                ← future WAL directory
```

### 24.2 Segment Naming Convention

| Format | State | Example |
|--------|-------|---------|
| `{start_index}_{start_ts}.lnc` | **Active** (open for writes) | `0_1706000000.lnc` |
| `{start_index}_{start_ts}-{end_ts}.lnc` | **Closed** (immutable) | `0_1706000000-1706099999.lnc` |

The presence of a `-` in the filename stem distinguishes closed from active segments.

### 24.3 Recovery Sequence

`perform_startup_recovery` runs at server start before accepting connections:

```
┌─────────────────────────────────────────────────────────────────┐
│                    STARTUP RECOVERY SEQUENCE                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. cleanup_empty_segments(data_dir)                             │
│     └── Remove 0-byte .lnc files from aborted creates           │
│                                                                  │
│  2. find_segments_needing_recovery(data_dir)   [RECURSIVE]      │
│     └── Walk all topic subdirectories                            │
│     └── Collect active segments (no '-' in name)                 │
│     └── Collect segments with .dirty marker files                │
│                                                                  │
│  3. For each segment needing recovery:                           │
│     a. SegmentRecovery::truncate_to_valid()                      │
│        └── Parse TLV records from byte 0                         │
│        └── Truncate at last valid record boundary                │
│     b. IndexRebuilder::rebuild()                                 │
│        └── Regenerate sparse index sidecar                       │
│     c. recovery.clear_dirty_marker()                             │
│                                                                  │
│  4. close_unclosed_segments(segments_dir)       [RECURSIVE]     │
│     └── Walk all topic subdirectories                            │
│     └── Rename active segments to closed format                  │
│     └── end_timestamp = current time                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Critical ordering**: Truncation (step 3) runs **before** closing (step 4). This ensures corrupt trailing bytes are removed before the segment is marked as closed and becomes immutable.

### 24.4 Recursive Traversal

Both `close_unclosed_segments` and `find_segments_needing_recovery` use the same recursive pattern:

```rust
pub fn close_unclosed_segments(segments_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut closed = Vec::new();
    if !segments_dir.exists() { return Ok(closed); }

    for entry in std::fs::read_dir(segments_dir)? {
        let path = entry?.path();

        // Recurse into topic subdirectories
        if path.is_dir() {
            closed.extend(close_unclosed_segments(&path)?);
            continue;
        }

        if path.extension().is_some_and(|ext| ext == "lnc") {
            if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                if !filename.contains('-') {  // Active segment
                    let new_path = rename_to_closed_segment(&path, now_ns())?;
                    closed.push(new_path);
                }
            }
        }
    }
    Ok(closed)
}
```

### 24.5 Dirty Markers

When a segment is being written to, an optional `.dirty` marker file can be created alongside it (e.g., `0_1706000000.dirty`). This provides an additional signal during recovery:

| Marker Present | Segment State | Recovery Action |
|---------------|---------------|----------------|
| No `.dirty` + active name | Normal active segment | Truncate + close |
| `.dirty` + active name | Write was in progress | Truncate + close + remove marker |
| `.dirty` + closed name | Close was interrupted | Re-validate + remove marker |

---

## 25. Segment Compaction

Over time, LANCE accumulates many small closed segments — especially under low-throughput workloads or frequent rolling restarts. The **Segment Compactor** merges contiguous small segments into fewer, larger segments to reduce file descriptor pressure and improve sequential read performance.

**Implementation**: `lnc-io/src/segment.rs` — `SegmentCompactor`, `CompactionConfig`, `CompactionResult`, `SegmentMetadata`.

### 25.1 Compaction Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     COMPACTION DECISION FLOW                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   find_candidates(segments_dir)                                             │
│       │                                                                     │
│       ▼                                                                     │
│   ┌─────────────────────┐                                                   │
│   │ should_compact()?    │  ← min_segments small files below min_segment_size│
│   └──────────┬──────────┘                                                   │
│         YES  │                                                              │
│              ▼                                                              │
│   ┌─────────────────────┐                                                   │
│   │ select_segments()    │  ← contiguous, ≤ max_segment_size, ≤ 16 segments │
│   └──────────┬──────────┘                                                   │
│              ▼                                                              │
│   ┌─────────────────────┐                                                   │
│   │ compact()            │  ← sequential copy in 64 KB chunks               │
│   │  • create output     │     seal output                                  │
│   │  • delete sources    │     (if delete_sources=true)                     │
│   └─────────────────────┘                                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 25.2 Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_segments` | 4 | Minimum small segments before triggering compaction |
| `max_segment_size` | 1 GiB | Upper bound on compacted output size |
| `min_segment_size` | 1 MiB | Segments below this size are compaction candidates |
| `delete_sources` | `true` | Remove source segments after successful merge |

### 25.3 Compaction Process

1. **Candidate discovery**: Scan `segments_dir` for `.lnc` files smaller than `max_segment_size`, sorted by `start_index`.
2. **Trigger check**: At least `min_segments` files must be below `min_segment_size`.
3. **Selection**: Choose up to 16 contiguous segments whose combined size ≤ `max_segment_size`.
4. **Merge**: Create a new closed segment (`{start_index}_{start_ts}-{end_ts}.lnc`), copy data in 64 KB chunks via `SegmentReader` → `SegmentWriter::append`.
5. **Cleanup**: Delete source segments and their associated `.sparse.idx` / `.secondary.idx` sidecar files.
6. **Invocation**: `maybe_compact(segments_dir)` combines all steps — returns `None` if compaction is unnecessary.

### 25.4 Safety Invariants

- **Only closed segments** are compacted — active segments are never touched.
- **Sequential copy** preserves TLV record ordering.
- **Source deletion** happens only after the output is sealed, ensuring crash-safety.
- The compacted segment is named with the first source's `start_index` and `created_at`, maintaining sort ordering.

---

## 26. Circuit Breaker

LANCE implements the **Circuit Breaker** pattern to prevent cascade failures when a downstream dependency (e.g., replication peer, storage backend) becomes unhealthy. The implementation is fully lock-free using atomic operations.

**Implementation**: `lance/src/server/circuit_breaker.rs` — `CircuitBreaker`, `CircuitBreakerConfig`, `CircuitState`.

### 26.1 State Machine

```
┌──────────┐    failure_threshold     ┌──────────┐
│          │ ─────────────────────► │          │
│  CLOSED  │                         │   OPEN   │
│          │ ◄─────────────────────  │          │
└──────────┘   success_threshold     └────┬─────┘
     ▲          (from HalfOpen)           │
     │                                    │ reset_timeout elapsed
     │                                    ▼
     │                              ┌──────────┐
     └────── success_threshold ────│ HALF-OPEN│
              successes             │          │
                                    └──────────┘
                                          │
                    any failure ───────────┘ (reopens)
```

### 26.2 Configuration Presets

| Preset | `failure_threshold` | `success_threshold` | `reset_timeout` | `failure_window` |
|--------|--------------------:|--------------------:|----------------:|-----------------:|
| **Default** | 5 | 3 | 30 s | 60 s |
| **Aggressive** | 3 | 2 | 10 s | 30 s |
| **Tolerant** | 10 | 5 | 60 s | 120 s |

### 26.3 Lock-Free Implementation

All state is tracked via atomics:

| Field | Type | Purpose |
|-------|------|---------|
| `state` | `AtomicU32` | 0=Closed, 1=Open, 2=HalfOpen |
| `failure_count` | `AtomicU32` | Consecutive failures in current window |
| `success_count` | `AtomicU32` | Successes in half-open state |
| `opened_at` | `AtomicU64` | Timestamp (ms) when circuit tripped |
| `last_failure_at` | `AtomicU64` | Window tracking for failure rate |

State transitions use `compare_exchange` with `Ordering::AcqRel` to prevent lost updates under concurrency.

### 26.4 Key Behaviours

- **Failure window**: Failures outside `failure_window` reset the counter (prevents old failures from tripping the circuit).
- **Half-open probing**: After `reset_timeout`, a limited number of requests are allowed through. Any failure immediately reopens the circuit.
- **Manual control**: `trip()` and `reset()` allow operator intervention via admin commands.
- **Success reset**: A single success in Closed state resets the failure counter to zero.

---

## 27. Retention Service

LANCE enforces per-topic data retention policies via a background service that periodically scans and deletes expired segments. The service supports both **TTL-based** (time) and **size-based** (bytes) retention.

**Implementation**: `lance/src/server/retention.rs` — `run_retention_service`, `RetentionServiceConfig`, `enforce_topic_retention`.

### 27.1 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     RETENTION SERVICE                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   tokio::interval(cleanup_interval)                                         │
│       │                                                                     │
│       ▼                                                                     │
│   run_retention_cycle()                                                     │
│       │                                                                     │
│       ├── for each topic with retention config:                             │
│       │   │                                                                 │
│       │   ├── Phase 1: Mark TTL-expired closed segments                     │
│       │   ├── Phase 2: Mark size-exceeded segments (oldest first)           │
│       │   └── Phase 3: Delete marked segments + sidecar indexes             │
│       │                                                                     │
│       └── Log summary: segments_deleted, bytes_freed                        │
│                                                                              │
│   shutdown_rx ──► graceful exit                                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 27.2 Retention Policies

| Policy | Configuration | Behaviour |
|--------|---------------|-----------|
| **TTL** | `max_age_secs` | Delete closed segments whose end timestamp is older than `now - max_age` |
| **Size** | `max_bytes` | Delete oldest closed segments until total topic size ≤ `max_bytes` |

Both policies can be combined — TTL is applied first, then size-based cleanup removes additional segments if needed.

### 27.3 Safety Rules

- **Active segments are never deleted** — only closed (immutable) segments are candidates.
- **Index sidecar cleanup** — `.sparse.idx` and `.secondary.idx` files are deleted alongside the segment.
- **Dry-run mode** — set `dry_run: true` for testing; logs what *would* be deleted without touching files.
- **Configurable interval** — default 60-second cycle, configurable via `retention_cleanup_interval` in `Config`.

### 27.4 Metrics

| Event | Log Target | Level |
|-------|------------|-------|
| Segment deleted | `lance::retention` | `info` |
| Delete failed | `lance::retention` | `warn` |
| No segments expired | `lance::retention` | `debug` |
| Cycle summary | `lance::retention` | `info` |

---

## 28. Token Authentication

LANCE supports optional **bearer-token authentication** for securing client connections. Authentication can be applied to all operations or limited to write-only enforcement (reads remain open).

**Implementation**: `lance/src/auth.rs` — `TokenValidator`, `AuthResult`, `AuthError`, `AuthSettings`.

### 28.1 Configuration

Authentication is configured via the `[auth]` section in `lance.toml`:

```toml
[auth]
enabled = true
write_only = false          # true = only writes require auth
tokens = ["token1", "token2"]
token_file = "/etc/lance/tokens"  # one token per line
```

### 28.2 Authentication Flow

```
Client                   Connection Handler              TokenValidator
  │                            │                              │
  │── Frame(AUTH token) ──────▶│                              │
  │                            │── validate_for_operation() ─▶│
  │                            │                              │
  │                            │◀── AuthResult ───────────────│
  │                            │                              │
  │◀── ACK/REJECT ────────────│                              │
  │                            │                              │
```

### 28.3 AuthResult Variants

| Result | Meaning |
|--------|---------|
| `Allowed` | Token valid, operation permitted |
| `Denied` | Token invalid or missing |
| `NotRequired` | Auth disabled or operation exempt (read in write-only mode) |

### 28.4 Key Properties

- **Token storage**: Tokens are loaded at startup from config or file, stored in a `HashSet` for O(1) lookup.
- **Write-only mode**: When `write_only = true`, read operations (`Fetch`, `ListTopics`, `Subscribe`) bypass authentication. Write operations (`Ingest`, `CreateTopic`, `DeleteTopic`) always require a valid token.
- **Disabled by default**: When `enabled = false`, all operations are permitted without authentication.
- **Connection-level validation**: Authentication is checked per-frame in the connection handler, before the frame is dispatched to ingestion or command handlers.

---

## 29. Hybrid Logical Clock (HLC)

LANCE uses a **Hybrid Logical Clock** for causally consistent timestamps across distributed nodes. The HLC combines physical wall-clock time with a logical counter, ensuring monotonicity even when clocks drift or events arrive out of order.

**Implementation**: `lnc-core/src/hlc.rs` — `HlcTimestamp`, `HybridLogicalClock`, `ClockHealth`.

### 29.1 Timestamp Layout

```
┌──────────────────────────────────────────────────────────┐
│                    HLC Timestamp (64 bits)                  │
├────────────────────────────────────┬───────────────────────┤
│        physical_ms (44 bits)        │   logical (20 bits)   │
│  milliseconds since Unix epoch     │  counter (0–1,048,575) │
└────────────────────────────────────┴───────────────────────┘
```

| Component | Bits | Range | Notes |
|-----------|------|-------|-------|
| `physical_ms` | 44 | ~557 years (until 2527) | Wall-clock milliseconds |
| `logical` | 20 | 0–1,048,575 | Events per millisecond per node |

### 29.2 Algorithm

**Local event** (`now()`):
1. Read wall clock → `wall_ms`
2. If `wall_ms > last.physical`: use `wall_ms`, reset logical to 0
3. Else: keep physical, increment logical
4. CAS (`compare_exchange_weak`) to ensure monotonicity under concurrency

**Remote receive** (`receive(remote_ts)`):
1. Read wall clock → `wall_ms`
2. If `wall_ms > max(local, remote)`: use wall clock, reset logical
3. If `local > remote`: increment local logical
4. If `remote > local`: adopt remote physical, increment remote logical
5. If equal physical: increment `max(local_logical, remote_logical)`
6. CAS to commit

### 29.3 Clock Health Monitoring

| Health | Drift | Action |
|--------|-------|--------|
| `Healthy` | < 100 ms | Normal operation |
| `Degraded` | 100 ms – 1 s | Warning logged, check NTP |
| `Critical` | > 1 s | Alert, potential partition or NTP failure |

### 29.4 Thread Safety

The HLC uses `AtomicU64` with `compare_exchange_weak` (AcqRel/Acquire ordering). The CAS loop naturally handles contention — failed CAS retries with the updated value. The only spin case is logical counter exhaustion (>1M events/ms), which is practically unreachable.

### 29.5 Usage in LANCE

The HLC is used by the replication layer to establish causal ordering of writes across nodes. The `receive()` method is called when processing `AppendEntries` from the leader, ensuring that follower timestamps respect the happened-before relationship with leader events.

---

## 30. Subscription Management

LANCE tracks active consumer subscriptions and committed offsets server-side to support `Subscribe`/`Unsubscribe`/`CommitOffset` control commands. This enables server-assisted resume-from-committed-offset semantics.

**Implementation**: `lance/src/subscription.rs` — `SubscriptionManager`.

### 30.1 Data Model

```
SubscriptionManager
├── subscriptions: HashMap<(topic_id, consumer_id), SubscriptionInfo>
│   ├── current_offset: u64
│   └── last_activity: u64  (millis since epoch)
│
└── committed_offsets: HashMap<(topic_id, consumer_id), CommittedOffset>
    └── offset: u64
```

### 30.2 Operations

| Operation | Behaviour |
|-----------|-----------|
| `subscribe(consumer_id, topic_id, offset, batch_size)` | Creates subscription; resumes from committed offset if `offset=0` and a prior commit exists |
| `unsubscribe(consumer_id, topic_id)` | Removes subscription; committed offsets **persist** |
| `commit_offset(consumer_id, topic_id, offset)` | Saves durable offset; updates subscription position if active |
| `get_committed_offset(topic_id, consumer_id)` | Returns last committed offset (survives unsubscribe) |

### 30.3 Offset Resume Logic

When a consumer subscribes with `offset=0` (SeekPosition::Beginning):
1. Check for a previously committed offset for `(topic_id, consumer_id)`
2. If found, resume from the committed offset (not from the beginning)
3. If not found, start from offset 0

Explicit offsets always override committed offsets.

### 30.4 Interaction with Client-Side Offsets (§19)

The `SubscriptionManager` provides **server-assisted** offset tracking that complements the client-side `OffsetStore` (§19). The server tracks committed offsets in memory for the duration of the process; the client-side store provides durable, cross-restart persistence. In typical deployments:

- **Client `OffsetStore`** is authoritative for long-term offset persistence
- **Server `SubscriptionManager`** provides within-session resume and activity tracking

### 30.5 Concurrency

The `SubscriptionManager` uses `RwLock<HashMap<...>>` for both maps. This is acceptable because subscription operations are infrequent control-plane events (not hot-path data-plane). The `RwLock` allows concurrent reads (e.g., multiple `get_committed_offset` calls) while serialising writes.

---

[↑ Back to Top](#technical-design-project-lance) | [← Back to Docs Index](./README.md)
