# Lance Lead Dev Memory

## Key Architecture Notes
- **batch_id lifecycle**: Client generates monotonic batch_id via `next_batch_id()` (AtomicU64, starts at 1), encodes in LWP header, server echoes in ack frame, client validates match.
- **LWP Header**: 44 bytes total (12-byte preamble + 32-byte IngestHeader). batch_id at offset 0-7 of IngestHeader. All little-endian.
- **Producer Mutex serialization**: `Arc<Mutex<ReconnectingClient>>` ensures only one send+ack cycle in flight at a time per connection.
- **Cursor-based parsing**: `process_frames` in `connection.rs` uses cursor to avoid O(N^2) copy_within per frame. Single shift at end of loop.
- **Forwarding path**: Non-leader nodes forward raw frame bytes to leader via `try_forward_to_leader`. Leader acks are relayed back to client.
- **Error retryability**: `InvalidResponse` is NOT retryable (connection stays open), `ServerBackpressure`/`Timeout`/`ConnectionClosed` are retryable.
- **Multi-actor ingestion**: `actor_count` routes requests via topic_id hash. Each actor has its own segment writers. Production uses `actor_count=8`.
- **BufWriter 4 MiB buffer**: SegmentWriter uses BufWriter with 4 MiB capacity. Auto-flush can make file visible to readers before explicit fsync.
- **DirEntry metadata staleness (Windows)**: `DirEntry::metadata()` from `read_dir` caches file size at scan time. On Windows, this can be stale vs `File::metadata()`.

## Known Bugs Fixed
- See [bugs-fixed.md](bugs-fixed.md) for details on the batch_id mismatch forwarding bug.
- See [bugs-fixed.md](bugs-fixed.md) for the large payload corruption, stale metadata, and segment index collision bugs.

## File Locations
- Server connection handler: `lance/src/server/connection.rs`
- Client core: `lnc-client/src/client.rs`
- Producer batching: `lnc-client/src/producer.rs`
- Frame encoding: `lnc-network/src/frame.rs`
- Protocol wire format: `lnc-network/src/protocol.rs`
- Reconnection logic: `lnc-client/src/connection.rs`
- Error types: `lnc-client/src/error.rs`
- Benchmark: `lnc-bench/src/main.rs`
- Topic registry/read path: `lance/src/topic.rs`
- Segment writer: `lance/src/server/writer.rs`
- Multi-actor ingestion: `lance/src/server/multi_actor.rs`

## Test Infrastructure
- Unit tests: `cargo test -p lance`, `cargo test -p lnc-client`, `cargo test -p lnc-network`
- Integration tests in lnc-client require running LANCE server (ignored by default)
- Cluster tests require 3-node cluster (ignored by default)
- Integration test scripts: `scripts/run_integration_tests.ps1` (Windows), `scripts/run_integration_tests.sh` (Linux/Mac)
- K8s production deployment: `--ingestion-actor-count 8` in `k8s/lance.yaml`
- Solo deployment: default `actor_count=1`, no `--ingestion-actor-count` flag
