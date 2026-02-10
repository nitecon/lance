[← Back to Docs Index](./README.md)

# Raft Consensus, Quorum Management & Wartime Protocol

## Table of Contents

- [1. Overview](#1-overview)
- [2. Raft Consensus in LANCE](#2-raft-consensus-in-lance)
- [3. Quorum Management](#3-quorum-management)
- [4. Follower Lifecycle & ACK Semantics](#4-follower-lifecycle--ack-semantics)
- [5. Follower Startup Resync Protocol](#5-follower-startup-resync-protocol)
- [6. Wartime Term Extension](#6-wartime-term-extension)
- [7. Segment Manifest & Fetch Protocol](#7-segment-manifest--fetch-protocol)
- [8. Wire Protocol Additions](#8-wire-protocol-additions)
- [9. Readiness Probe Integration](#9-readiness-probe-integration)
- [10. Failure Modes & Recovery](#10-failure-modes--recovery)
- [11. Metrics & Observability](#11-metrics--observability)
- [12. Configuration](#12-configuration)
- [13. Implementation Reference](#13-implementation-reference)

This document specifies the Raft consensus layer, quorum management, follower resync protocol, and the "wartime" term extension mechanism used by LANCE's L3 (quorum) replication mode. It is a companion to [Architecture.md §4](./Architecture.md#4-replication-state-machine) and [LWP-Specification.md §18](./LWP-Specification.md).

---

## 1. Overview

LANCE's L3 replication mode provides **filesystem-consistent** replication across cluster nodes. All nodes maintain byte-identical `.lnc` segment files on disk. The leader dictates all segment operations (creation, writing, rotation), and followers mirror the leader's exact file layout.

This design introduces a critical challenge: **what happens when a follower restarts and its local segment files are stale, empty, or mismatched with the leader's current state?**

The naive approach — resume writing at the follower's local offset — fails because the follower's offset diverges from the leader's. This causes a tight error loop that accumulates memory and leads to OOM kills.

This document specifies the **Follower Startup Resync Protocol** and the **Wartime Term Extension** mechanism that together provide a deterministic, convergent recovery path for followers rejoining the cluster.

### 1.1 Design Goals

| Goal | Mechanism |
|------|-----------|
| **Deterministic recovery** | Follower audits local state against leader's manifest, fetches only what's missing |
| **No data loss** | Closed segments are validated by CRC32C; mismatches are re-fetched from leader |
| **No OOM** | Chunked segment transfer (256KB); offset-mismatch writes are skipped gracefully |
| **No disruptive elections** | Wartime term extension prevents elections during follower catch-up |
| **No false durability** | Catching-up followers are excluded from write-commit quorum |
| **Bounded catch-up** | Wartime has a per-follower TTL and global max-term limit |
| **k8s-aware** | Readiness probe returns NOT_READY during resync, preventing premature rolling |

### 1.2 Relationship to Existing Architecture

This protocol extends the existing replication layer described in:

- **Architecture.md §4.1.1** — Filesystem-consistent replication (L3)
- **Architecture.md §4.1.3** — Leader-dictated segment management
- **Architecture.md §10.6** — Health endpoints (readiness probe)
- **LWP-Specification.md §18** — Replication wire format

The existing `DataReplicationEntry`, `ReplicationFlags`, `ReplicationAck`, and `ClusterEvent` types are preserved. This document adds new message types and state machines on top of the existing protocol.

---

## 2. Raft Consensus in LANCE

### 2.1 Raft Implementation

LANCE implements a modified Raft consensus protocol (Ongaro & Ousterhout, 2014) with the following extensions:

| Feature | Standard Raft | LANCE Extension |
|---------|---------------|-----------------|
| **Pre-Vote** | §9.6 (optional) | Enabled by default — prevents term inflation from partitioned nodes |
| **Leader Lease** | Not in spec | Configurable lease duration for read optimization |
| **Wartime Extension** | Not in spec | Term extension during follower catch-up (§6) |
| **Fencing Tokens** | Not in spec | Monotonic token issued on leader election for stale-write rejection |

**Implementation**: `lnc-replication/src/raft.rs` (`RaftNode`, `RaftState`, `RaftConfig`)

### 2.2 State Machine

```
                    ┌──────────────────────────────────────────────┐
                    │                                              │
                    ▼                                              │
              ┌──────────┐    election timeout    ┌───────────┐   │
              │ Follower │ ──────────────────────▶ │ Candidate │   │
              └──────────┘                         └───────────┘   │
                    ▲                                    │         │
                    │          discovers leader          │ wins    │
                    │◄──────────────────────────────────│ election│
                    │                                    │         │
                    │                                    ▼         │
                    │                              ┌──────────┐   │
                    │◄──────── higher term ────────│  Leader  │───┘
                    │                              └──────────┘
                    │                                    │
                    │         wartime active              │
                    │         (no step-down for           │
                    │          catching-up nodes)         │
```

### 2.3 Election Timing

| Parameter | Default | Configurable | Notes |
|-----------|---------|--------------|-------|
| `election_timeout_min` | 150ms | Yes | Lower bound of randomized timeout |
| `election_timeout_max` | 300ms | Yes | Upper bound of randomized timeout |
| `heartbeat_interval` | 50ms | Yes | Leader sends heartbeats at this interval |
| `leader_lease` | 100ms | Yes | Read optimization lease |
| `pre_vote_enabled` | `true` | Yes | Pre-vote prevents term inflation |

### 2.4 Log Replication

LANCE uses Raft's log replication for two purposes:

1. **Metadata replication** — Topic create/delete operations (`EntryType::TopicOp`)
2. **Data replication** — Enriched segment write entries (`EntryType::Data` containing `DataReplicationEntry`)

The `AppendEntries` RPC carries both metadata and data entries. Followers process them in order, applying topic operations and writing segment data as directed by the leader.

---

## 3. Quorum Management

### 3.1 Quorum Formula

For a cluster of `M` nodes, quorum requires `floor(M/2) + 1` ACKs:

| Cluster Size | Quorum | Fault Tolerance |
|-------------|--------|-----------------|
| 1 (L1) | N/A | 0 |
| 3 (L3) | 2 | 1 node |
| 5 (L3) | 3 | 2 nodes |

**Implementation**: `lnc-replication/src/quorum.rs` (`QuorumConfig`, `QuorumTracker`, `AsyncQuorumManager`)

### 3.2 Dual Quorum Concept

LANCE distinguishes between two types of quorum:

| Quorum Type | Purpose | Includes Catching-Up Nodes? |
|-------------|---------|----------------------------|
| **Election Quorum** | Leader election, pre-vote | **Yes** — catching-up nodes vote normally |
| **Write-Commit Quorum** | Durability guarantee for producer ACKs | **No** — only fully-synced nodes count |

This distinction is critical. A catching-up follower is **alive** (participates in elections, prevents unnecessary leader changes) but **not durable** (its segment files may be incomplete, so writes cannot be considered committed on that node).

### 3.3 Adaptive Eviction

The existing `FollowerHealth` system (Architecture.md §4.1) tracks per-follower ACK latency and evicts slow followers from write-commit quorum:

| Status | Write-Commit Quorum | Election Quorum | Trigger |
|--------|---------------------|-----------------|---------|
| `Healthy` | ✅ Included | ✅ Included | Default state |
| `Degraded` | ✅ Included | ✅ Included | Exceeding P99 threshold |
| `Evicted` | ❌ Excluded | ✅ Included | 3+ consecutive slow batches |
| `CatchingUp` | ❌ Excluded | ✅ Included | Follower in resync (§5) |

**Implementation**: `lnc-replication/src/follower.rs` (`FollowerHealth`, `FollowerStatus`)

### 3.4 Quorum Recalculation

When a follower's status changes, the leader recalculates write-commit quorum:

```
healthy_count = count(followers where status ∈ {Healthy, Degraded})
required_acks = floor(healthy_count / 2) + 1
required_acks = max(required_acks, 1)  // Safety: always require at least 1 ACK
```

**Important**: The leader always counts itself as a healthy node for write-commit quorum (its local write is always durable).

---

## 4. Follower Lifecycle & ACK Semantics

### 4.1 ACK Status Codes

Followers respond to data replication entries with a `ReplicationAck` containing a status code:

| Status | Code | Meaning | Leader Behavior |
|--------|------|---------|-----------------|
| `Ok` | `0x00` | Write completed, data durable | Count toward write-commit quorum |
| `Error` | `0x01` | Write failed (I/O error, etc.) | Log warning, do not count for quorum |
| `ResyncNeeded` | `0x02` | Follower is out of sync | Trigger resync protocol |
| `CatchingUp` | `0x03` | Follower is alive but resyncing | Count for election quorum only |

### 4.2 CatchingUp ACK Behavior

During the resync protocol (§5), the follower:

1. **Continues responding to Raft heartbeats** (`AppendEntries` with empty entries) — this keeps the follower visible to the leader and prevents unnecessary elections
2. **ACKs data replication entries with `CatchingUp` status** — the leader knows the follower is alive but cannot count it for write-commit quorum
3. **Does NOT process data payloads** — incoming `DataReplicationEntry` payloads are acknowledged but not written to disk (the offset-mismatch-skip logic handles this gracefully)
4. **Buffers `NEW_SEGMENT` notifications** — these are recorded for the convergence loop in §5

### 4.3 Transition: CatchingUp → Healthy

Once the resync protocol completes:

1. Follower sends `ResyncComplete` to leader
2. Follower switches ACK mode from `CatchingUp` to `Ok`
3. Leader removes wartime status for this follower
4. Leader recalculates write-commit quorum to include this follower
5. Follower sets readiness probe to READY

This transition is atomic from the leader's perspective — the follower is either catching up or fully synced, never in between.

---

## 5. Follower Startup Resync Protocol

### 5.1 Protocol Overview

When a follower starts (or restarts after a crash), it must synchronize its local segment files with the leader before participating in write-commit quorum. The protocol is:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FOLLOWER STARTUP RESYNC PROTOCOL                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. JOIN RAFT          Join cluster, discover leader via heartbeat       │
│         │                                                                │
│  2. SET NOT_READY      k8s readiness probe → false                      │
│         │                                                                │
│  3. SET ACK MODE       ACK mode → CatchingUp (still ACKs heartbeats)   │
│         │                                                                │
│  4. RESYNC_BEGIN       Send ResyncBegin to leader                       │
│         │              Leader enters wartime for this follower           │
│         │              Leader excludes follower from write-commit quorum │
│         │                                                                │
│  5. MANIFEST_REQ       Send SegmentManifestRequest to leader            │
│         │                                                                │
│  6. MANIFEST_RESP      Receive per-topic segment inventory:             │
│         │              • Closed segments: name, CRC32C, size            │
│         │              • Active segment: name (no CRC — unstable)       │
│         │                                                                │
│  7. AUDIT & FETCH      For each topic:                                  │
│         │              a. Compare local closed segments vs manifest      │
│         │              b. Missing → fetch from leader (chunked, 256KB)  │
│         │              c. CRC mismatch → delete local, fetch from leader│
│         │              d. Extra local → log warning (stale data)        │
│         │              Meanwhile: buffer NEW_SEGMENT notifications       │
│         │                                                                │
│  8. CONVERGENCE        Check buffered NEW_SEGMENT notifications:        │
│         │              • If active segment rotated → old is now closed  │
│         │                → fetch it, loop back to check                 │
│         │              • If no new rotations → proceed                  │
│         │                                                                │
│  9. CREATE ACTIVE      Create fresh active segment per topic            │
│         │              (leader's current segment name, offset 0)        │
│         │                                                                │
│ 10. RESYNC_COMPLETE    Send ResyncComplete to leader                    │
│         │              Leader exits wartime for this follower            │
│         │              Leader includes follower in write-commit quorum   │
│         │                                                                │
│ 11. SET ACK MODE       ACK mode → Ok (normal operation)                 │
│         │                                                                │
│ 12. SET READY          k8s readiness probe → true                       │
│         │                                                                │
│ 13. NORMAL REPLICATION Offset-mismatch-skip as final safety net         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Step-by-Step Detail

#### Step 1: Join Raft

The follower starts normally, joins the Raft cluster, and discovers the leader via `AppendEntries` heartbeats. The existing `ClusterCoordinator::start()` handles this. The follower receives a `ClusterEvent::BecameFollower { leader_id, term }` event.

#### Step 2: Set NOT_READY

The follower sets its health status to not-ready. The k8s readiness probe (`/health/ready`) returns HTTP 503. This prevents:

- Kubernetes from routing client traffic to this pod
- Kubernetes from proceeding with rolling updates (it waits for readiness)

#### Step 3: Set ACK Mode to CatchingUp

The follower's replication ACK handler switches to `CatchingUp` mode. All `ReplicationAck` responses use status `0x03` (`CatchingUp`). The follower still responds to Raft heartbeats normally.

#### Step 4: Send ResyncBegin

The follower sends a `ResyncBegin` message to the leader over the replication TCP connection (port 1993). The leader:

1. Records this follower as "in wartime" with a TTL (default: 60s)
2. Excludes this follower from write-commit quorum calculation
3. Continues counting this follower for election quorum (it's alive)
4. Logs: `"Follower {node_id} entering wartime resync (TTL={ttl}s)"`

#### Step 5: Send SegmentManifestRequest

The follower sends a `SegmentManifestRequest` to the leader. This is a lightweight request with no payload — just the message type header.

#### Step 6: Receive SegmentManifestResponse

The leader responds with a `SegmentManifestResponse` containing, for each topic:

```
TopicManifest {
    topic_id: u32,
    topic_name: String,
    active_segment: String,           // e.g., "0_1706918400000000000.lnc"
    closed_segments: Vec<SegmentInfo> // name, crc32c, size_bytes
}

SegmentInfo {
    name: String,                     // e.g., "0_1706918400000000000-1706918500000000000.lnc"
    crc32c: u32,                      // CRC32C of entire file contents
    size_bytes: u64,                  // File size in bytes
}
```

**Note**: The active segment is excluded from CRC validation because the leader is continuously writing to it — its checksum is never stable.

#### Step 7: Audit & Fetch

For each topic, the follower compares its local segment files against the manifest:

| Local State | Manifest State | Action |
|-------------|---------------|--------|
| File exists, CRC matches | In manifest | ✅ No action |
| File exists, CRC mismatch | In manifest | ❌ Delete local, fetch from leader |
| File missing | In manifest | ❌ Fetch from leader |
| File exists | NOT in manifest | ⚠️ Log warning (stale from previous term) |

Fetching uses the `SegmentFetchRequest`/`SegmentFetchResponse` protocol (§7.2), which transfers segment data in 256KB chunks to prevent memory spikes.

**Concurrently**: The follower's event loop continues running. Any `NEW_SEGMENT` flags in incoming `DataReplicationEntry` messages are buffered in a `Vec<(topic_id, new_segment_name)>`.

#### Step 8: Convergence Loop

After all closed segments are validated/fetched, the follower checks its buffered `NEW_SEGMENT` notifications:

- If the leader rotated a segment during the fetch phase, the previously-active segment is now closed. The follower must fetch it.
- The follower re-requests the manifest (or uses the buffered notification) to get the CRC of the newly-closed segment, fetches it, and checks again.
- This loop converges because the leader can only rotate **forward** — each iteration only adds newly-closed segments, never removes ones already validated.

**Convergence guarantee**: The loop terminates when no new `NEW_SEGMENT` notifications have been received since the last check. In practice, this is 1-2 iterations at most (segment rotation is infrequent relative to the fetch time).

#### Step 9: Create Fresh Active Segment

For each topic, the follower creates a new, empty segment file with the leader's current active segment name. The writer starts at offset 0.

When the next `DataReplicationEntry` arrives from the leader with `write_offset > 0`, the offset-mismatch-skip logic (§5.4) gracefully drops it. The follower waits for the leader to rotate to a new segment (`NEW_SEGMENT` flag), at which point the follower creates the new segment and writes from offset 0 — perfectly in sync.

**Alternative**: If the leader's active segment is very new (small `write_offset`), the follower may be able to request the active segment's current content via `SegmentFetchRequest` and resume from the leader's current offset. This is an optimization, not a requirement — the convergence guarantee holds either way.

#### Step 10-12: Complete Resync

The follower sends `ResyncComplete`, switches to normal ACK mode, and sets the readiness probe to READY. The leader exits wartime for this follower and includes it in write-commit quorum.

### 5.3 Convergence Proof

The resync protocol converges because:

1. **Closed segments are immutable** — once a segment is closed, its CRC never changes
2. **The leader can only rotate forward** — new segments are created, old ones are sealed
3. **Each iteration of the convergence loop processes all newly-closed segments** — the set of unprocessed segments monotonically decreases
4. **The loop terminates when no new rotations are detected** — this is guaranteed to happen because segment rotation is bounded by write throughput and segment size limits

In the worst case (continuous high-throughput writes causing frequent rotation), the follower may need 2-3 iterations. Each iteration fetches at most one newly-closed segment (the one that was active when the previous iteration started).

### 5.4 Offset-Mismatch-Skip (Safety Net)

During normal replication (after resync completes), the follower validates that the leader's `write_offset` matches the local writer's current offset before writing. If they don't match:

```rust
if entry.write_offset != topic_writer.writer.current_offset() {
    tracing::debug!(
        target: "lance::ingestion",
        topic_id,
        segment = %entry.segment_name,
        expected = topic_writer.writer.current_offset(),
        leader_offset = entry.write_offset,
        "Skipping write — offset mismatch, awaiting segment rotation"
    );
    return Ok(());
}
```

This is a **safety net**, not the primary recovery mechanism. It handles edge cases where the resync protocol couldn't perfectly align the active segment (e.g., the leader wrote data between the follower's segment creation and the first replication message).

**Implementation**: `lance/src/server/ingestion.rs` (`write_replicated_data_enriched`)

---

## 6. Wartime Term Extension

### 6.1 Concept

The "wartime" mechanism is analogous to emergency powers in governance — the leader extends its term during a critical operation (follower catch-up) to prevent disruptive elections that would interrupt the recovery process.

Without wartime, a catching-up follower that can't ACK data writes might cause the leader to believe it has lost quorum, triggering an unnecessary election. This is especially problematic in a 3-node cluster where losing one follower to resync leaves only 2 nodes — the leader needs both remaining nodes to maintain quorum.

### 6.2 Wartime Rules

| Rule | Value | Rationale |
|------|-------|-----------|
| **Per-follower TTL** | 60s (configurable) | Bounds catch-up time; prevents indefinite wartime |
| **Max consecutive wartime terms** | 3 (configurable) | Prevents a pinned leader if followers keep crashing |
| **Scope** | Per-follower | One follower's wartime doesn't affect other followers |
| **Election quorum** | Unaffected | Catching-up nodes still vote in elections |
| **Write-commit quorum** | Excludes catching-up nodes | No false durability guarantees |
| **Healthy follower elections** | Unaffected | A healthy follower can still start an election if it hasn't received heartbeats |

### 6.3 Wartime State Machine

```
                    ResyncBegin received
                           │
                           ▼
                    ┌──────────────┐
                    │   WARTIME    │
                    │  (per-node)  │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ResyncComplete  TTL expires  Max terms
              │            │         exceeded
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  NORMAL  │ │  EXPIRED │ │ REFUSED  │
        │ (healthy)│ │ (retry)  │ │ (wait)   │
        └──────────┘ └──────────┘ └──────────┘
```

### 6.4 Leader-Side Wartime Tracking

The leader maintains a per-follower wartime state:

```rust
struct WartimeState {
    /// When wartime was entered for this follower
    started_at: Instant,
    /// Maximum duration before automatic expiry
    ttl: Duration,
    /// How many consecutive wartime terms this follower has used
    consecutive_terms: u32,
}
```

On each heartbeat tick, the leader checks:

1. **TTL expired?** — If `now - started_at > ttl`, the leader drops this follower from wartime. The follower must restart the resync protocol.
2. **Max terms exceeded?** — If `consecutive_terms > max_wartime_terms`, the leader refuses the next `ResyncBegin` from this follower. The follower must wait for the next Raft term (i.e., a new leader election) before retrying.

### 6.5 Wartime Does NOT Suppress Elections

Wartime only prevents the leader from stepping down due to missing write-commit ACKs from the catching-up follower. It does **not**:

- Prevent other followers from starting elections (if they haven't received heartbeats)
- Prevent the catching-up follower from voting in elections
- Extend the Raft term number itself (the term is unchanged)
- Block any Raft state transitions other than the leader's self-assessment of quorum health

### 6.6 Wartime Abuse Prevention

| Scenario | Prevention |
|----------|------------|
| Follower crashes repeatedly, keeps entering wartime | `max_wartime_terms` limit (default: 3) |
| Follower enters wartime but never completes | Per-follower TTL (default: 60s) |
| Multiple followers in wartime simultaneously | Each follower has independent wartime state; write-commit quorum adjusts |
| All followers in wartime (leader alone) | Leader can still write locally; producers see degraded durability |
| Malicious follower sends ResyncBegin repeatedly | Rate-limit ResyncBegin per node (1 per TTL window) |

---

## 7. Segment Manifest & Fetch Protocol

### 7.1 Segment Manifest

The manifest is the leader's authoritative inventory of segment files per topic.

#### Request

`SegmentManifestRequest` — empty payload, just the message type header. Sent by follower to leader.

#### Response

`SegmentManifestResponse` — contains the full segment inventory:

```
┌──────────────────────────────────────────────────────────────────────┐
│                    SEGMENT MANIFEST RESPONSE                          │
├──────────────────────────────────────────────────────────────────────┤
│ topic_count: u32                                                      │
│ ┌──────────────────────────────────────────────────────────────────┐ │
│ │ TopicManifest (repeated topic_count times)                       │ │
│ │ ┌──────────────────────────────────────────────────────────────┐ │ │
│ │ │ topic_id: u32                                                │ │ │
│ │ │ topic_name_len: u16, topic_name: [u8; N]                    │ │ │
│ │ │ active_segment_len: u16, active_segment: [u8; N]            │ │ │
│ │ │ closed_segment_count: u32                                    │ │ │
│ │ │ ┌──────────────────────────────────────────────────────────┐ │ │ │
│ │ │ │ SegmentInfo (repeated closed_segment_count times)        │ │ │ │
│ │ │ │ ┌──────────────────────────────────────────────────────┐ │ │ │ │
│ │ │ │ │ name_len: u16, name: [u8; N]                        │ │ │ │ │
│ │ │ │ │ crc32c: u32                                          │ │ │ │ │
│ │ │ │ │ size_bytes: u64                                      │ │ │ │ │
│ │ │ │ └──────────────────────────────────────────────────────┘ │ │ │ │
│ │ │ └──────────────────────────────────────────────────────────┘ │ │ │
│ │ └──────────────────────────────────────────────────────────────┘ │ │
│ └──────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

#### CRC32C Computation

The leader computes CRC32C over the **entire file contents** of each closed segment. This is done lazily (on manifest request) or eagerly (on segment close) depending on configuration.

**Important**: The active segment is **never** checksummed because the leader is continuously writing to it. Only closed (immutable) segments have stable CRCs.

### 7.2 Segment Fetch

When a follower needs to fetch a segment (missing or CRC mismatch), it uses chunked transfer to prevent memory spikes.

#### Request

```
SegmentFetchRequest {
    topic_id: u32,
    segment_name: String,
    offset: u64,          // Byte offset to start reading from (for resumption)
    max_chunk_size: u32,  // Requested chunk size (default: 256KB)
}
```

#### Response

```
SegmentFetchResponse {
    topic_id: u32,
    segment_name: String,
    offset: u64,          // Byte offset of this chunk
    data: Bytes,          // Chunk data (up to max_chunk_size)
    total_size: u64,      // Total segment file size
    done: bool,           // True if this is the last chunk
}
```

#### Transfer Flow

```
Follower                                    Leader
   │                                           │
   │── SegmentFetchRequest(offset=0) ────────▶│
   │                                           │── Read 256KB from file
   │◀── SegmentFetchResponse(offset=0, ───────│
   │    data=[256KB], done=false)              │
   │                                           │
   │── SegmentFetchRequest(offset=262144) ───▶│
   │                                           │── Read next 256KB
   │◀── SegmentFetchResponse(offset=262144, ──│
   │    data=[256KB], done=false)              │
   │                                           │
   │   ... (repeat until done=true) ...        │
   │                                           │
   │── SegmentFetchRequest(offset=N) ────────▶│
   │                                           │── Read remaining bytes
   │◀── SegmentFetchResponse(offset=N, ───────│
   │    data=[remaining], done=true)           │
   │                                           │
   │── Verify CRC32C locally ─────────────────│
   │                                           │
```

This is similar to Raft's `InstallSnapshot` chunked protocol (§7 of the Raft paper), which already exists in the codebase (`InstallSnapshotRequest`/`InstallSnapshotResponse`).

### 7.3 Fetch Resumption

If the connection drops during a fetch, the follower can resume by sending a `SegmentFetchRequest` with the offset of the last successfully received byte. The leader reads from that offset and continues.

The follower validates the final CRC32C after all chunks are received. If the CRC doesn't match (e.g., due to a concurrent segment close race), the follower deletes the local file and retries the entire fetch.

---

## 8. Wire Protocol Additions

### 8.1 New Message Types

The following message types are added to the `MessageType` enum:

| Message Type | Discriminant | Direction | Purpose |
|-------------|-------------|-----------|---------|
| `ResyncBegin` | `11` | follower → leader | Enter wartime for this follower |
| `ResyncComplete` | `12` | follower → leader | Exit wartime, follower is caught up |
| `SegmentManifestRequest` | `13` | follower → leader | Request segment inventory |
| `SegmentManifestResponse` | `14` | leader → follower | Segment inventory response |
| `SegmentFetchRequest` | `15` | follower → leader | Request segment data chunk |
| `SegmentFetchResponse` | `16` | leader → follower | Segment data chunk response |

### 8.2 New ACK Status

The `ReplicationAckStatus` enum is extended:

| Status | Code | Meaning |
|--------|------|---------|
| `Ok` | `0x00` | Write completed successfully |
| `Error` | `0x01` | Error occurred during write |
| `ResyncNeeded` | `0x02` | Follower is out of sync |
| `CatchingUp` | `0x03` | Follower is alive but resyncing |

### 8.3 Wire Format: ResyncBegin

```
[message_type: 1 byte = 11]
[node_id: 2 bytes LE]
[term: 8 bytes LE]
```

### 8.4 Wire Format: ResyncComplete

```
[message_type: 1 byte = 12]
[node_id: 2 bytes LE]
[term: 8 bytes LE]
```

### 8.5 Wire Format: SegmentManifestRequest

```
[message_type: 1 byte = 13]
[node_id: 2 bytes LE]
```

### 8.6 Wire Format: SegmentManifestResponse

```
[message_type: 1 byte = 14]
[topic_count: 4 bytes LE]
  (repeated topic_count times):
    [topic_id: 4 bytes LE]
    [topic_name_len: 2 bytes LE][topic_name: N bytes]
    [active_segment_len: 2 bytes LE][active_segment: N bytes]
    [closed_segment_count: 4 bytes LE]
      (repeated closed_segment_count times):
        [name_len: 2 bytes LE][name: N bytes]
        [crc32c: 4 bytes LE]
        [size_bytes: 8 bytes LE]
```

### 8.7 Wire Format: SegmentFetchRequest

```
[message_type: 1 byte = 15]
[topic_id: 4 bytes LE]
[segment_name_len: 2 bytes LE][segment_name: N bytes]
[offset: 8 bytes LE]
[max_chunk_size: 4 bytes LE]
```

### 8.8 Wire Format: SegmentFetchResponse

```
[message_type: 1 byte = 16]
[topic_id: 4 bytes LE]
[segment_name_len: 2 bytes LE][segment_name: N bytes]
[offset: 8 bytes LE]
[total_size: 8 bytes LE]
[done: 1 byte (0 or 1)]
[data_len: 4 bytes LE]
[data: data_len bytes]
```

### 8.9 Backward Compatibility

All new message types use discriminants > 10, which are currently unused. Older nodes that receive these messages will log "Received unhandled message type" and ignore them (existing behavior in `handle_peer_connection`).

The new `CatchingUp` ACK status (`0x03`) is also backward-compatible — older leaders that don't recognize it will treat it as `Error` (the default fallback in `ReplicationAckStatus::from_u8`).

---

## 9. Readiness Probe Integration

### 9.1 Health Endpoint Behavior

The existing `/health/ready` endpoint (Architecture.md §10.6) is extended to account for resync state:

```rust
pub fn is_ready(&self) -> bool {
    self.wal_replay_complete
        && self.segment_writer.is_some()
        && self.replication_lag_ms < MAX_REPLICATION_LAG_MS
        && !SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
        && !self.resync_in_progress.load(Ordering::SeqCst)  // NEW
}
```

### 9.2 k8s Rolling Update Behavior

During a rolling update of the Lance StatefulSet:

1. k8s terminates pod N (sends SIGTERM)
2. Pod N drains (Architecture.md §9.2)
3. k8s starts new pod N
4. New pod N starts resync → readiness probe returns 503
5. k8s waits for readiness before proceeding to pod N+1
6. Resync completes → readiness probe returns 200
7. k8s proceeds to pod N+1

This prevents the scenario where k8s rolls all pods simultaneously, leaving no healthy followers for the leader.

### 9.3 Liveness vs Readiness During Resync

| Probe | During Resync | Rationale |
|-------|---------------|-----------|
| `/health/live` | 200 (alive) | Process is running, not deadlocked |
| `/health/ready` | 503 (not ready) | Node is catching up, don't route traffic |
| `/health/startup` | 200 (started) | WAL replay complete, resync is a separate phase |

---

## 10. Failure Modes & Recovery

### 10.1 Failure During Resync

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Leader crashes during follower resync | TCP connection drops | Follower detects disconnect, restarts resync against new leader |
| Follower OOM during fetch | k8s OOMKilled event | Pod restarts, begins fresh resync |
| Network partition during fetch | TCP timeout | Follower retries fetch from last offset; if persistent, restarts resync |
| CRC mismatch after fetch | Local CRC validation | Delete local file, retry fetch |
| Wartime TTL expires | Leader timer | Leader drops follower from wartime; follower must restart resync |
| Leader election during resync | Raft term change | Follower detects new leader, restarts resync |

### 10.2 Failure During Normal Operation (Post-Resync)

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Offset mismatch on write | `write_offset != current_offset` | Skip write, wait for `NEW_SEGMENT` (safety net) |
| Segment name mismatch | `entry.segment_name != writer.segment_name` | Log warning, wait for `NEW_SEGMENT` |
| Follower falls behind | Increasing replication lag metric | Adaptive eviction (§3.3) |
| All followers catching up | Write-commit quorum = leader only | Producers see degraded durability (leader-only ACK) |

### 10.3 Cascading Failure Prevention

| Scenario | Prevention |
|----------|------------|
| Resync causes leader overload (serving segment data) | Chunked transfer with configurable chunk size; leader rate-limits fetch responses |
| Multiple followers resync simultaneously | Each follower has independent wartime; leader serves fetches sequentially per connection |
| Resync loop (follower keeps failing) | `max_wartime_terms` limit; after N failures, follower must wait for new term |

---

## 11. Metrics & Observability

### 11.1 New Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_resync_in_progress` | Gauge | `node_id` | 1 if this node is currently resyncing |
| `lance_resync_duration_seconds` | Histogram | `node_id` | Time to complete resync |
| `lance_resync_segments_fetched` | Counter | `node_id` | Segments fetched during resync |
| `lance_resync_bytes_fetched` | Counter | `node_id` | Bytes transferred during resync |
| `lance_resync_crc_mismatches` | Counter | `node_id` | Segments with CRC mismatch (re-fetched) |
| `lance_wartime_active` | Gauge | `node_id`, `follower_id` | 1 if wartime is active for a follower |
| `lance_wartime_ttl_expires` | Counter | `node_id` | Wartime TTL expirations |
| `lance_wartime_terms_refused` | Counter | `node_id` | ResyncBegin refused due to max terms |
| `lance_ack_catching_up` | Counter | `node_id` | CatchingUp ACKs sent |
| `lance_offset_mismatch_skips` | Counter | `node_id`, `topic_id` | Writes skipped due to offset mismatch |

### 11.2 Structured Log Events

| Event | Level | Target | When |
|-------|-------|--------|------|
| `resync_started` | INFO | `lance::resync` | Follower begins resync |
| `resync_manifest_received` | INFO | `lance::resync` | Manifest received from leader |
| `resync_segment_fetched` | INFO | `lance::resync` | Segment successfully fetched and validated |
| `resync_crc_mismatch` | ERROR | `lance::resync` | Local segment CRC doesn't match leader |
| `resync_completed` | INFO | `lance::resync` | Resync finished, transitioning to normal |
| `wartime_entered` | WARN | `lance::cluster` | Leader enters wartime for a follower |
| `wartime_exited` | INFO | `lance::cluster` | Leader exits wartime (normal completion) |
| `wartime_expired` | WARN | `lance::cluster` | Wartime TTL expired for a follower |
| `wartime_refused` | ERROR | `lance::cluster` | ResyncBegin refused (max terms exceeded) |

---

## 12. Configuration

### 12.1 Resync Configuration

```toml
[replication.resync]
# Maximum time a follower can spend in resync before wartime expires
wartime_ttl_secs = 60

# Maximum consecutive wartime terms per follower before refusing ResyncBegin
max_wartime_terms = 3

# Chunk size for segment fetch transfers
fetch_chunk_size_bytes = 262144  # 256KB

# Whether to eagerly compute CRC32C on segment close (vs lazily on manifest request)
eager_crc = true
```

### 12.2 Defaults

| Parameter | Default | Min | Max | Notes |
|-----------|---------|-----|-----|-------|
| `wartime_ttl_secs` | 60 | 10 | 300 | Lower = faster failure detection; higher = more time for large datasets |
| `max_wartime_terms` | 3 | 1 | 10 | Prevents indefinite pinned leader |
| `fetch_chunk_size_bytes` | 262144 | 4096 | 4194304 | 256KB balances throughput vs memory |
| `eager_crc` | true | — | — | Eager avoids latency spike on manifest request |

---

## 13. Implementation Reference

### 13.1 Crate Mapping

| Component | Crate | File(s) |
|-----------|-------|---------|
| Wire protocol types | `lnc-replication` | `src/codec.rs` |
| Message encode/decode | `lnc-replication` | `src/codec.rs` |
| Leader-side handlers | `lnc-replication` | `src/cluster.rs` |
| Wartime state machine | `lnc-replication` | `src/cluster.rs` |
| Follower resync logic | `lance` | `src/server/mod.rs` |
| Segment CRC computation | `lnc-io` | `src/segment.rs` |
| Readiness probe | `lance` | `src/server/mod.rs`, `src/health.rs` |
| Offset-mismatch-skip | `lance` | `src/server/ingestion.rs` |
| ACK status extension | `lnc-replication` | `src/codec.rs` |

### 13.2 Existing Code References

| Symbol | Location | Relevance |
|--------|----------|-----------|
| `ClusterCoordinator` | `lnc-replication/src/cluster.rs` | Leader-side cluster management |
| `ClusterEvent` | `lnc-replication/src/cluster.rs` | Event enum for follower notifications |
| `DataReplicationEntry` | `lnc-replication/src/codec.rs` | Existing enriched replication message |
| `ReplicationFlags` | `lnc-replication/src/codec.rs` | NEW_SEGMENT, ROTATE_AFTER flags |
| `ReplicationAck` | `lnc-replication/src/codec.rs` | Follower ACK with status |
| `ReplicationAckStatus` | `lnc-replication/src/codec.rs` | ACK status codes (extended with CatchingUp) |
| `MessageType` | `lnc-replication/src/codec.rs` | Message discriminant enum (extended) |
| `ReplicationMessage` | `lnc-replication/src/codec.rs` | Message union (extended) |
| `handle_peer_connection` | `lnc-replication/src/cluster.rs` | Incoming peer message handler |
| `FollowerHealth` | `lnc-replication/src/follower.rs` | Per-follower health tracking |
| `FollowerStatus` | `lnc-replication/src/follower.rs` | Health status enum (extended with CatchingUp) |
| `QuorumConfig` | `lnc-replication/src/quorum.rs` | Quorum calculation |
| `AsyncQuorumManager` | `lnc-replication/src/quorum.rs` | Async quorum waiting |
| `write_replicated_data_enriched` | `lance/src/server/ingestion.rs` | Follower write path (offset-mismatch-skip) |
| `cluster_event_loop` | `lance/src/server/mod.rs` | Follower event processing loop |
| `SegmentWriter` | `lnc-io/src/segment.rs` | Segment file writer |

### 13.3 Industry Pattern Comparison

| Pattern | System | LANCE Equivalent |
|---------|--------|-----------------|
| ISR (In-Sync Replicas) | Apache Kafka | Healthy followers = ISR; CatchingUp = OSR |
| InstallSnapshot (chunked) | Raft §7 / etcd | SegmentFetch chunked transfer |
| SNAP+DIFF | ZooKeeper | Closed segments (SNAP) + active segment replication (DIFF) |
| Wartime / Emergency Powers | Governance theory | Leader term extension during catch-up |
| Fencing Tokens | Distributed systems | Existing in LANCE Raft (stale-write rejection) |

---

[↑ Back to Top](#raft-consensus-quorum-management--wartime-protocol) | [← Back to Docs Index](./README.md)
