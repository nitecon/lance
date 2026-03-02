# Bugs Fixed

## Ack batch_id Mismatch in Forwarding Path (2026-03-02)

**Symptom**: `Ack batch_id mismatch: sent N, received N-1` under high throughput (~1% error rate, starting ~35s into 60s benchmark with 64 connections, 256 pipeline depth).

**Root Cause**: In `lance/src/server/connection.rs`, `process_frames` used cursor-based parsing but passed the full buffer to `dispatch_frame`. When cursor > 0 (multiple frames in one TCP read), `&buffer[..consumed]` in forwarding functions sent the WRONG frame bytes (from buffer offset 0, not cursor position). Leader processed the wrong frame and acked the wrong batch_id.

**Secondary Issue**: Forwarding functions (`try_forward_to_leader`, `try_forward_control_to_leader`) did `buffer.copy_within(consumed..read_offset, 0)` per frame, conflicting with the cursor-based single-shift-at-end design in `process_frames`.

**Fix (3 parts)**:
1. `process_frames` now passes `&mut buffer[cursor..]` and `effective_read_offset = *read_offset - cursor` to `dispatch_frame` so forwarding sends correct frame bytes.
2. Removed all `buffer.copy_within` calls from forwarding functions -- the single shift at end of `process_frames` loop handles this.
3. Defensive `wait_for_ack` in client: drains stale acks (batch_id < expected) instead of immediately failing, preventing cascade failures.

**Cleanup**: Changed `next_batch_id` from `fetch_add(1, SeqCst) + 1` with `AtomicU64::new(0)` to `fetch_add(1, SeqCst)` with `AtomicU64::new(1)` -- same result, clearer intent.

**Cascade failure mechanism**: `InvalidResponse` is NOT retryable, so after first mismatch, connection stays open with stale ack in read buffer, causing every subsequent send to fail "off by 1".

**Files changed**:
- `lance/src/server/connection.rs` -- cursor-adjusted sub-slice, removed copy_within from forwarding
- `lnc-client/src/client.rs` -- next_batch_id cleanup, defensive wait_for_ack stale ack draining

## Large Payload Data Corruption — Stale DirEntry Metadata + Short Reads + Segment Index Collision (2026-03-02)

**Symptom**: lnc-chaos with `--payload-size 4096` fails with gaps from start (seq 7->9, 11->13), 132 gaps, 24 dups, 151 parse errors showing `0xFCFCFC0C` (4244438268) as TLV length. Consumer gets stuck, reconnects 7 times. With `--payload-size 512`, passes perfectly (531/531, 0 gaps, 0 dups).

**Root Cause (3 interrelated issues)**:

1. **Stale `DirEntry::metadata()` on Windows**: `read_from_offset()` and `total_data_size()` used `entry.metadata().len()` from `std::fs::read_dir`, which on Windows comes from `FindFirstFile` and caches the file size at directory-scan time. When the `BufWriter` auto-flushes (at 4 MiB boundary), the file size jumps in the page cache but the `DirEntry` metadata may not reflect the new size. This can cause the reader to see a stale-larger size (from a previous `DirEntry` cache) and read beyond actually-committed data, getting zero-filled or uninitialised bytes that corrupt the TLV stream. With 4096-byte payloads, the BufWriter auto-flush boundary alignment is more likely to intersect record boundaries compared to 512-byte payloads.

2. **Single `file.read()` without retry**: `read_from_offset()` used a single `file.read()` call which can return short reads on any OS. While rare for regular files, this can happen under concurrent write + read. A short read that splits a TLV record causes the consumer to see a truncated record, and the `next_offset` computation advances correctly but the data is incomplete at the byte boundary.

3. **`find_next_segment_index` returns `max_index` not `max_index + 1`**: When `actor_count > 1` (production uses 8), multiple actors create segments with the same `start_index=0` but different timestamps. `read_from_offset` sorts these by timestamp and treats them as sequential, but they're written to in parallel. Data written to segment A changes the cumulative offset for segment B, corrupting the byte-offset addressing scheme. This is a MAJOR production bug that would cause data loss/corruption in the k8s deployment (`--ingestion-actor-count 8`).

**Fix (3 parts)**:
1. `read_from_offset()` now opens the file and uses `file.metadata().len()` for the authoritative size, taking `min(dir_entry_size, open_file_size)` as `effective_size` to never read beyond committed data.
2. `read_from_offset()` now uses a `read_exact`-style loop (retry on short reads, handle `EINTR`) instead of a single `file.read()`.
3. `find_next_segment_index()` returns `max_index + 1` (via `Option<u64>`) when segments exist, preventing index collisions across actors.
4. `total_data_size()` opens each file to get authoritative size instead of using `DirEntry::metadata()`.

**Additional cleanup**: Fixed 3 pre-existing clippy warnings in `topic.rs` (redundant closures, redundant local binding).

**Files changed**:
- `lance/src/topic.rs` -- `read_from_offset()`, `total_data_size()`, clippy fixes
- `lance/src/server/writer.rs` -- `find_next_segment_index()` returns `max_index + 1`

## Multi-Actor Ordering Bug — LeaderConnectionPool Routing Key Scatter (2026-03-02)

**Symptom**: lnc-chaos with `--payload-size 4096` and `--ingestion-actor-count 8` in K8s passes all 465 messages (no data loss) but shows 237 gaps and 35 dups — messages arrive OUT OF ORDER. With `--payload-size 512` passes cleanly (same rate). 0 parse errors, 0 reconnects.

**Root Cause**: `MultiActorSender::send()` computed actor dispatch as:
```
mixed = (topic_id * GOLDEN_RATIO) ^ routing_key
actor_id = mixed % actor_count
```
The `routing_key` is a per-TCP-connection counter assigned in `handle_connection`. When the client connects to a **follower** node, the follower's `LeaderConnectionPool` (default `pool_size: 8`) maintains up to 8 TCP connections to the leader. Each pooled connection gets a different `routing_key` on the leader side. This caused XOR to produce different `mixed` values for the same `topic_id`, scattering a single topic's messages across multiple ingestion actors. Each actor writes to its own segment file. When the consumer reads via `read_from_offset`, segments are assembled by `start_index` order which does NOT match produce order, causing the ordering violations.

**Why 512-byte payloads passed**: The forwarding pool reuses connections. With small 512-byte payloads, messages are forwarded quickly over a single reused connection (low latency = low pool concurrency). With 4096-byte payloads, higher latency per forward increases pool concurrency, making it more likely that different messages use different pooled connections (and thus different routing_keys).

**Fix**: Changed `MultiActorSender::send()` to hash on `topic_id` ONLY:
```
hashed = (topic_id * GOLDEN_RATIO)
actor_id = hashed % actor_count
```
This guarantees all messages for the same topic always go to the same actor regardless of which connection they arrive on. Multi-actor parallelism is preserved across different topics.

**Files changed**:
- `lance/src/server/multi_actor.rs` -- topic-only hash dispatch, updated tests
- `lance/src/server/ingestion.rs` -- updated `routing_key` doc comment, `#[allow(dead_code)]`
- `lance/src/server/connection.rs` -- updated `routing_key` doc comment
