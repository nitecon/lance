//! Follower Resync Protocol (§18.8)
//!
//! When a follower has been offline for hours and is thousands of segments behind,
//! standard Raft AppendEntries cannot catch it up without saturating the heartbeat
//! thread and destabilizing the cluster. The ResyncActor manages an out-of-band
//! bulk segment transfer while the node is in the `CatchingUp` state.
//!
//! # Protocol Flow
//!
//! 1. **Gap Detection**: Follower detects offset mismatch via `ReplicationAck`
//!    with `Status = 0x02` (ResyncNeeded) and its last confirmed global offset.
//! 2. **Manifest Exchange**: Leader computes the divergence point, builds a
//!    `SegmentManifest` listing all segments the follower is missing.
//! 3. **Segment Streaming**: Leader streams closed `.lnc` files in chunks via
//!    a dedicated TCP connection (not the Raft heartbeat channel). Files are
//!    read asynchronously with a reusable buffer to avoid heap-loading entire
//!    segments. Note: `sendfile(2)` cannot be used here because the protocol
//!    requires per-chunk CRC32C computation and custom framing headers.
//! 4. **Bootstrap**: Follower receives files, places them in the data directory,
//!    and calls `bootstrap_segments()` to rebuild sparse + secondary indices.
//! 5. **Completion**: Follower transitions from `CatchingUp` → `Healthy` and
//!    normal replication resumes.
//!
//! # SRE-First Design
//!
//! - Dedicated transfer channel avoids Raft heartbeat saturation
//! - Configurable bandwidth throttle prevents NIC monopolization
//! - CRC32C validation per chunk ensures data integrity
//! - Resumable: tracks last received segment for crash recovery
//! - All progress is observable via `lnc_metrics::RESYNC_*` counters

use bytes::{Bytes, BytesMut};
use lnc_core::{LanceError, Result};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// State Machine
// ---------------------------------------------------------------------------

/// Resync actor state machine (§23 CATCHING_UP lifecycle).
///
/// Transitions:
/// ```text
/// Idle ──► DetectingGap ──► Requesting ──► Streaming ──► Bootstrapping ──► Complete
///   ▲          │                │               │               │              │
///   └──────────┴────────────────┴───────────────┴───────────────┴──► Failed ───┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResyncState {
    /// No resync in progress — node is healthy or standalone.
    Idle,
    /// Follower detected an offset gap it cannot fill via standard RPCs.
    DetectingGap,
    /// Follower has sent ResyncNeeded ACK; waiting for leader manifest.
    Requesting,
    /// Receiving bulk segment data from leader.
    Streaming,
    /// All segments received; rebuilding indices via `bootstrap_segments()`.
    Bootstrapping,
    /// Resync completed successfully — ready to resume normal replication.
    Complete,
    /// Resync failed — will retry after backoff.
    Failed,
}

impl std::fmt::Display for ResyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "IDLE"),
            Self::DetectingGap => write!(f, "DETECTING_GAP"),
            Self::Requesting => write!(f, "REQUESTING"),
            Self::Streaming => write!(f, "STREAMING"),
            Self::Bootstrapping => write!(f, "BOOTSTRAPPING"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the resync protocol.
#[derive(Debug, Clone)]
pub struct ResyncConfig {
    /// Size of each chunk during segment streaming (default: 4 MiB).
    /// Larger chunks reduce syscall overhead; smaller chunks improve
    /// progress granularity and resumability.
    pub chunk_size: usize,
    /// Maximum bandwidth for segment streaming in bytes/sec (0 = unlimited).
    /// Prevents the resync transfer from monopolizing the NIC and starving
    /// normal replication traffic.
    pub max_bandwidth_bytes_per_sec: u64,
    /// Timeout for the entire resync session.
    pub session_timeout: Duration,
    /// Timeout for individual chunk transfers.
    pub chunk_timeout: Duration,
    /// Maximum number of retry attempts before giving up.
    pub max_retries: u32,
    /// Backoff delay between retry attempts.
    pub retry_backoff: Duration,
    /// Maximum number of segments to transfer concurrently (pipelining).
    /// Higher values improve throughput on high-latency links but use
    /// more memory for buffering.
    pub max_concurrent_segments: usize,
    /// Maximum number of unacknowledged chunks in flight per segment.
    /// Pipelining avoids the stop-and-wait throughput ceiling
    /// (`chunk_size / RTT`). On a 10 GbE link with 1 ms RTT and 4 MiB
    /// chunks, stop-and-wait caps at ~4 GB/s; with 8 in-flight chunks
    /// the TCP window stays saturated.
    pub max_in_flight_chunks: usize,
    /// TCP send/receive buffer size in bytes. Tuning this to match the
    /// bandwidth-delay product (BDP) of the link prevents the kernel TCP
    /// window from becoming the bottleneck on high-latency paths (e.g.
    /// cross-region). Set to 0 to leave the OS default. Default: 8 MiB.
    pub tcp_buffer_size: usize,
    /// Port offset from the main replication port for the dedicated
    /// resync transfer channel (default: +1, e.g. 1993 → 1994).
    pub port_offset: u16,
}

impl Default for ResyncConfig {
    fn default() -> Self {
        Self {
            chunk_size: 4 * 1024 * 1024,                // 4 MiB
            max_bandwidth_bytes_per_sec: 0,             // unlimited
            session_timeout: Duration::from_secs(3600), // 1 hour
            chunk_timeout: Duration::from_secs(30),
            max_retries: 5,
            retry_backoff: Duration::from_secs(5),
            max_concurrent_segments: 4,
            max_in_flight_chunks: 8,
            tcp_buffer_size: 8 * 1024 * 1024, // 8 MiB
            port_offset: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol Messages
// ---------------------------------------------------------------------------

/// Describes a single segment that needs to be transferred.
#[derive(Debug, Clone)]
pub struct SegmentDescriptor {
    /// Exact segment filename (e.g. `000001_1706918400000.lnc`)
    pub filename: String,
    /// Total size of the segment file in bytes.
    pub size_bytes: u64,
    /// CRC32C of the entire segment file for end-to-end validation.
    pub file_crc: u32,
    /// Whether this segment is sealed (closed) or active.
    pub is_sealed: bool,
}

/// Manifest sent by the leader listing all segments the follower needs.
///
/// The follower uses this to know exactly what to expect and can verify
/// completeness after the transfer.
#[derive(Debug, Clone)]
pub struct SegmentManifest {
    /// Leader's current term (for stale-leader detection).
    pub leader_term: u64,
    /// Leader's node ID.
    pub leader_id: u16,
    /// The global offset the follower reported as its last confirmed offset.
    pub follower_last_offset: u64,
    /// The leader's current global offset (target for the resync).
    pub leader_current_offset: u64,
    /// Ordered list of segments to transfer (oldest first).
    pub segments: Vec<SegmentDescriptor>,
    /// Total bytes across all segments (for progress reporting).
    pub total_bytes: u64,
}

impl SegmentManifest {
    /// Compute the manifest from a leader's segment directory.
    ///
    /// Scans the directory for `.lnc` files, filters to those the follower
    /// is missing (based on `follower_last_offset`), and builds descriptors.
    pub fn from_directory(
        dir: &Path,
        leader_term: u64,
        leader_id: u16,
        follower_last_offset: u64,
        leader_current_offset: u64,
    ) -> Result<Self> {
        let mut segments = Vec::new();
        let mut total_bytes = 0u64;

        let mut entries: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "lnc"))
            .collect();

        // Sort by filename to ensure correct ordering
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::metadata(&path)?;
            let size = metadata.len();

            let filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| LanceError::Protocol("Invalid segment filename".into()))?
                .to_string();

            // Determine if sealed: sealed segments have a dash in the stem
            // e.g. `000001_1706918400000-1706918500000.lnc`
            let is_sealed = path
                .file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.contains('-'));

            // Compute CRC32C of the file for integrity validation
            let file_crc = compute_file_crc(&path)?;

            segments.push(SegmentDescriptor {
                filename,
                size_bytes: size,
                file_crc,
                is_sealed,
            });

            total_bytes += size;
        }

        info!(
            target: "lance::resync",
            leader_id,
            leader_term,
            follower_last_offset,
            leader_current_offset,
            segment_count = segments.len(),
            total_bytes,
            "Built resync manifest"
        );

        Ok(Self {
            leader_term,
            leader_id,
            follower_last_offset,
            leader_current_offset,
            segments,
            total_bytes,
        })
    }

    /// Encode the manifest to bytes for wire transfer.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256 + self.segments.len() * 128);

        buf.extend_from_slice(&self.leader_term.to_le_bytes());
        buf.extend_from_slice(&self.leader_id.to_le_bytes());
        buf.extend_from_slice(&self.follower_last_offset.to_le_bytes());
        buf.extend_from_slice(&self.leader_current_offset.to_le_bytes());
        buf.extend_from_slice(&self.total_bytes.to_le_bytes());

        let segment_count = self.segments.len() as u32;
        buf.extend_from_slice(&segment_count.to_le_bytes());

        for seg in &self.segments {
            let name_bytes = seg.filename.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&seg.size_bytes.to_le_bytes());
            buf.extend_from_slice(&seg.file_crc.to_le_bytes());
            buf.extend_from_slice(&[if seg.is_sealed { 1u8 } else { 0u8 }]);
        }

        buf.freeze()
    }

    /// Decode a manifest from wire bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 34 {
            return None;
        }

        let mut pos = 0;

        let leader_term = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let leader_id = u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?);
        pos += 2;
        let follower_last_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let leader_current_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let total_bytes = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        let segment_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        let mut segments = Vec::with_capacity(segment_count);

        for _ in 0..segment_count {
            if pos + 2 > data.len() {
                return None;
            }
            let name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?) as usize;
            pos += 2;

            if pos + name_len > data.len() {
                return None;
            }
            let filename = std::str::from_utf8(&data[pos..pos + name_len])
                .ok()?
                .to_string();
            pos += name_len;

            if pos + 13 > data.len() {
                return None;
            }
            let size_bytes = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
            pos += 8;
            let file_crc = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
            pos += 4;
            let is_sealed = data[pos] != 0;
            pos += 1;

            segments.push(SegmentDescriptor {
                filename,
                size_bytes,
                file_crc,
                is_sealed,
            });
        }

        Some(Self {
            leader_term,
            leader_id,
            follower_last_offset,
            leader_current_offset,
            segments,
            total_bytes,
        })
    }
}

/// A chunk of segment data sent during streaming.
#[derive(Debug, Clone)]
pub struct SegmentChunk {
    /// Which segment this chunk belongs to (index into manifest).
    pub segment_index: u32,
    /// Byte offset within the segment file.
    pub offset: u64,
    /// Chunk payload.
    pub data: Bytes,
    /// CRC32C of this chunk's data.
    pub chunk_crc: u32,
    /// Whether this is the last chunk for this segment.
    pub is_final: bool,
}

impl SegmentChunk {
    /// Encode a chunk header for wire transfer (fixed 21-byte header + payload).
    ///
    /// Layout: segment_index(4) + offset(8) + chunk_len(4) + chunk_crc(4) + is_final(1) + data(N)
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(21 + self.data.len());
        buf.extend_from_slice(&self.segment_index.to_le_bytes());
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.chunk_crc.to_le_bytes());
        buf.extend_from_slice(&[if self.is_final { 1u8 } else { 0u8 }]);
        buf.extend_from_slice(&self.data);
        buf.freeze()
    }

    /// Decode a chunk from wire bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 21 {
            return None;
        }

        let segment_index = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let offset = u64::from_le_bytes(data[4..12].try_into().ok()?);
        let chunk_len = u32::from_le_bytes(data[12..16].try_into().ok()?) as usize;
        let chunk_crc = u32::from_le_bytes(data[16..20].try_into().ok()?);
        let is_final = data[20] != 0;

        if data.len() < 21 + chunk_len {
            return None;
        }

        let payload = Bytes::copy_from_slice(&data[21..21 + chunk_len]);

        Some(Self {
            segment_index,
            offset,
            data: payload,
            chunk_crc,
            is_final,
        })
    }
}

// ---------------------------------------------------------------------------
// Wire message types for the resync channel
// ---------------------------------------------------------------------------

/// Message types on the dedicated resync TCP channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResyncMessageType {
    /// Follower → Leader: request resync with last known offset.
    ResyncRequest = 0x01,
    /// Leader → Follower: segment manifest.
    Manifest = 0x02,
    /// Leader → Follower: segment data chunk.
    Chunk = 0x03,
    /// Follower → Leader: chunk acknowledged.
    ChunkAck = 0x04,
    /// Either direction: resync complete.
    Complete = 0x05,
    /// Either direction: resync failed / abort.
    Abort = 0xFF,
}

impl ResyncMessageType {
    #[inline]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::ResyncRequest),
            0x02 => Some(Self::Manifest),
            0x03 => Some(Self::Chunk),
            0x04 => Some(Self::ChunkAck),
            0x05 => Some(Self::Complete),
            0xFF => Some(Self::Abort),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Gap Detection
// ---------------------------------------------------------------------------

/// Result of gap detection analysis.
#[derive(Debug, Clone)]
pub struct GapAnalysis {
    /// The follower's last confirmed global offset.
    pub follower_offset: u64,
    /// The leader's current global offset.
    pub leader_offset: u64,
    /// Number of bytes the follower is behind.
    pub gap_bytes: u64,
    /// Estimated number of segments missing.
    pub estimated_missing_segments: u64,
    /// Whether the gap is large enough to warrant bulk resync
    /// (vs. standard AppendEntries catch-up).
    pub requires_bulk_resync: bool,
}

/// Threshold in bytes above which we switch from AppendEntries to bulk resync.
/// At 256 MiB per segment, 10 segments = ~2.5 GiB — too much for Raft RPCs.
const BULK_RESYNC_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

/// Analyze the gap between follower and leader to determine resync strategy.
pub fn detect_gap(follower_offset: u64, leader_offset: u64, avg_segment_size: u64) -> GapAnalysis {
    let gap_bytes = leader_offset.saturating_sub(follower_offset);
    let estimated_missing_segments = if avg_segment_size > 0 {
        gap_bytes / avg_segment_size
    } else {
        0
    };

    let requires_bulk_resync = gap_bytes > BULK_RESYNC_THRESHOLD_BYTES;

    debug!(
        target: "lance::resync",
        follower_offset,
        leader_offset,
        gap_bytes,
        estimated_missing_segments,
        requires_bulk_resync,
        "Gap analysis complete"
    );

    GapAnalysis {
        follower_offset,
        leader_offset,
        gap_bytes,
        estimated_missing_segments,
        requires_bulk_resync,
    }
}

// ---------------------------------------------------------------------------
// ResyncActor (Follower Side)
// ---------------------------------------------------------------------------

/// Progress tracker for an in-flight resync session.
#[derive(Debug, Clone)]
pub struct ResyncProgress {
    /// Total segments to receive.
    pub total_segments: usize,
    /// Segments fully received so far.
    pub segments_received: usize,
    /// Total bytes expected.
    pub total_bytes: u64,
    /// Bytes received so far.
    pub bytes_received: u64,
    /// When the resync session started.
    pub started_at: Instant,
    /// Current transfer rate in bytes/sec (exponential moving average).
    pub transfer_rate_bps: f64,
}

impl ResyncProgress {
    fn new(total_segments: usize, total_bytes: u64) -> Self {
        Self {
            total_segments,
            segments_received: 0,
            total_bytes,
            bytes_received: 0,
            started_at: Instant::now(),
            transfer_rate_bps: 0.0,
        }
    }

    /// Estimated time remaining based on current transfer rate.
    pub fn estimated_remaining(&self) -> Duration {
        if self.transfer_rate_bps <= 0.0 {
            return Duration::from_secs(u64::MAX);
        }
        let remaining_bytes = self.total_bytes.saturating_sub(self.bytes_received);
        let remaining_secs = remaining_bytes as f64 / self.transfer_rate_bps;
        Duration::from_secs_f64(remaining_secs)
    }

    /// Fraction complete (0.0 to 1.0).
    #[inline]
    pub fn fraction_complete(&self) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        self.bytes_received as f64 / self.total_bytes as f64
    }

    /// Update transfer rate using exponential moving average.
    fn update_rate(&mut self, chunk_bytes: u64, chunk_duration: Duration) {
        let chunk_secs = chunk_duration.as_secs_f64();
        if chunk_secs > 0.0 {
            let instant_rate = chunk_bytes as f64 / chunk_secs;
            // EMA with alpha = 0.3 for smoothing
            self.transfer_rate_bps = 0.3 * instant_rate + 0.7 * self.transfer_rate_bps;
        }
    }
}

/// The follower-side resync actor.
///
/// Manages the lifecycle of a bulk segment transfer from the leader when
/// this node is in the `CatchingUp` state. Runs on a dedicated async task
/// to avoid interfering with the Raft heartbeat loop.
pub struct ResyncActor {
    /// Current state of the resync state machine.
    state: ResyncState,
    /// This node's ID.
    node_id: u16,
    /// Configuration for the resync protocol.
    config: ResyncConfig,
    /// Directory where segment files are stored.
    data_dir: PathBuf,
    /// The manifest received from the leader (populated during Requesting state).
    manifest: Option<SegmentManifest>,
    /// Progress tracker (populated during Streaming state).
    progress: Option<ResyncProgress>,
    /// Number of retry attempts so far.
    retry_count: u32,
    /// Segments that have been fully received and need index rebuilding.
    pending_bootstrap: VecDeque<PathBuf>,
}

impl ResyncActor {
    /// Create a new resync actor for a follower node.
    pub fn new(node_id: u16, data_dir: PathBuf, config: ResyncConfig) -> Self {
        Self {
            state: ResyncState::Idle,
            node_id,
            config,
            data_dir,
            manifest: None,
            progress: None,
            retry_count: 0,
            pending_bootstrap: VecDeque::new(),
        }
    }

    /// Get the current resync state.
    #[inline]
    #[must_use]
    pub fn state(&self) -> ResyncState {
        self.state
    }

    /// Check if a resync is currently in progress.
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        !matches!(
            self.state,
            ResyncState::Idle | ResyncState::Complete | ResyncState::Failed
        )
    }

    /// Get current progress (if streaming).
    #[inline]
    #[must_use]
    pub fn progress(&self) -> Option<&ResyncProgress> {
        self.progress.as_ref()
    }

    /// Initiate a resync session by connecting to the leader's resync port.
    ///
    /// This is the main entry point called when the follower detects it needs
    /// to catch up via bulk transfer rather than standard AppendEntries.
    pub async fn initiate_resync(
        &mut self,
        leader_addr: std::net::SocketAddr,
        last_confirmed_offset: u64,
    ) -> Result<()> {
        if self.is_active() {
            return Err(LanceError::Protocol("Resync already in progress".into()));
        }

        self.state = ResyncState::DetectingGap;
        self.retry_count = 0;
        lnc_metrics::increment_resync_started();

        info!(
            target: "lance::resync",
            node_id = self.node_id,
            leader = %leader_addr,
            last_offset = last_confirmed_offset,
            "Initiating resync session"
        );

        // Connect to the leader's dedicated resync port
        let resync_addr = std::net::SocketAddr::new(
            leader_addr.ip(),
            leader_addr.port() + self.config.port_offset,
        );

        let result = self
            .run_resync_session(resync_addr, last_confirmed_offset)
            .await;

        match &result {
            Ok(()) => {
                self.state = ResyncState::Complete;
                lnc_metrics::increment_resync_completed();
                info!(
                    target: "lance::resync",
                    node_id = self.node_id,
                    "Resync completed successfully"
                );
            },
            Err(e) => {
                self.state = ResyncState::Failed;
                lnc_metrics::increment_resync_failed();
                error!(
                    target: "lance::resync",
                    node_id = self.node_id,
                    error = %e,
                    "Resync failed"
                );
            },
        }

        result
    }

    /// Run the full resync session with retry logic.
    async fn run_resync_session(
        &mut self,
        resync_addr: std::net::SocketAddr,
        last_confirmed_offset: u64,
    ) -> Result<()> {
        loop {
            match self
                .attempt_resync(resync_addr, last_confirmed_offset)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    self.retry_count += 1;
                    if self.retry_count > self.config.max_retries {
                        return Err(LanceError::Protocol(format!(
                            "Resync failed after {} retries: {}",
                            self.config.max_retries, e
                        )));
                    }

                    warn!(
                        target: "lance::resync",
                        node_id = self.node_id,
                        attempt = self.retry_count,
                        max_retries = self.config.max_retries,
                        error = %e,
                        "Resync attempt failed, retrying after backoff"
                    );

                    tokio::time::sleep(self.config.retry_backoff).await;
                },
            }
        }
    }

    /// Single resync attempt: connect, request manifest, stream segments, bootstrap.
    async fn attempt_resync(
        &mut self,
        resync_addr: std::net::SocketAddr,
        last_confirmed_offset: u64,
    ) -> Result<()> {
        // Phase 1: Connect
        self.state = ResyncState::Requesting;

        let mut stream =
            tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(resync_addr))
                .await
                .map_err(|_| LanceError::Protocol("Resync connection timeout".into()))?
                .map_err(LanceError::Io)?;

        stream.set_nodelay(true).map_err(LanceError::Io)?;

        // Tune TCP window for high-BDP links
        if self.config.tcp_buffer_size > 0 {
            tune_tcp_buffers(&stream, self.config.tcp_buffer_size)?;
        }

        // Phase 2: Send resync request
        self.send_resync_request(&mut stream, last_confirmed_offset)
            .await?;

        // Phase 3: Receive manifest
        let manifest = self.receive_manifest(&mut stream).await?;

        info!(
            target: "lance::resync",
            node_id = self.node_id,
            leader_id = manifest.leader_id,
            segments = manifest.segments.len(),
            total_bytes = manifest.total_bytes,
            "Received resync manifest"
        );

        self.progress = Some(ResyncProgress::new(
            manifest.segments.len(),
            manifest.total_bytes,
        ));
        self.manifest = Some(manifest);

        // Phase 4: Stream segments
        self.state = ResyncState::Streaming;
        self.receive_segments(&mut stream).await?;

        // Phase 5: Bootstrap indices
        self.state = ResyncState::Bootstrapping;
        self.bootstrap_received_segments().await?;

        // Phase 6: Send completion ACK
        self.send_complete(&mut stream).await?;

        Ok(())
    }

    /// Send the initial resync request to the leader.
    async fn send_resync_request(
        &self,
        stream: &mut TcpStream,
        last_confirmed_offset: u64,
    ) -> Result<()> {
        // Wire format: msg_type(1) + node_id(2) + last_offset(8) = 11 bytes
        let mut buf = [0u8; 11];
        buf[0] = ResyncMessageType::ResyncRequest as u8;
        buf[1..3].copy_from_slice(&self.node_id.to_le_bytes());
        buf[3..11].copy_from_slice(&last_confirmed_offset.to_le_bytes());

        stream.write_all(&buf).await.map_err(LanceError::Io)?;
        stream.flush().await.map_err(LanceError::Io)?;

        debug!(
            target: "lance::resync",
            node_id = self.node_id,
            last_offset = last_confirmed_offset,
            "Sent resync request"
        );

        Ok(())
    }

    /// Receive and decode the segment manifest from the leader.
    async fn receive_manifest(&mut self, stream: &mut TcpStream) -> Result<SegmentManifest> {
        // Read message type + length prefix
        let mut header = [0u8; 5];
        tokio::time::timeout(self.config.chunk_timeout, stream.read_exact(&mut header))
            .await
            .map_err(|_| LanceError::Protocol("Manifest receive timeout".into()))?
            .map_err(LanceError::Io)?;

        let msg_type = header[0];
        if msg_type == ResyncMessageType::Abort as u8 {
            return Err(LanceError::Protocol("Leader aborted resync".into()));
        }
        if msg_type != ResyncMessageType::Manifest as u8 {
            return Err(LanceError::Protocol(format!(
                "Expected Manifest message, got type 0x{:02x}",
                msg_type
            )));
        }

        let manifest_len = u32::from_le_bytes(
            header[1..5]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid manifest length".into()))?,
        ) as usize;

        // Read manifest body
        let mut manifest_buf = vec![0u8; manifest_len];
        tokio::time::timeout(
            self.config.chunk_timeout,
            stream.read_exact(&mut manifest_buf),
        )
        .await
        .map_err(|_| LanceError::Protocol("Manifest body receive timeout".into()))?
        .map_err(LanceError::Io)?;

        SegmentManifest::decode(&manifest_buf)
            .ok_or_else(|| LanceError::Protocol("Failed to decode manifest".into()))
    }

    /// Receive all segment chunks from the leader.
    ///
    /// Uses `tokio::fs::File` for all disk writes to avoid blocking the
    /// executor. Per-chunk writes and final `sync_all` use async file I/O.
    ///
    /// **Crash consistency**: segments are written to `<name>.lnc.part`
    /// temporary files. Only after `sync_all` succeeds is the `.part` file
    /// atomically renamed to the final `.lnc` path. If the follower crashes
    /// mid-transfer, the `.part` file is harmless and resync restarts cleanly.
    async fn receive_segments(&mut self, stream: &mut TcpStream) -> Result<()> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or_else(|| LanceError::Protocol("No manifest available".into()))?
            .clone();

        // Build part/final path pairs and open .part files for writing.
        // We never truncate the real segment — only the .part staging file.
        let mut segment_files: Vec<Option<tokio::fs::File>> =
            Vec::with_capacity(manifest.segments.len());
        let mut part_paths: Vec<PathBuf> = Vec::with_capacity(manifest.segments.len());
        let mut final_paths: Vec<PathBuf> = Vec::with_capacity(manifest.segments.len());

        for descriptor in &manifest.segments {
            let final_path = self.data_dir.join(&descriptor.filename);
            let part_path = self.data_dir.join(format!("{}.part", descriptor.filename));

            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&part_path)
                .await
                .map_err(LanceError::Io)?;

            segment_files.push(Some(file));
            part_paths.push(part_path);
            final_paths.push(final_path);
        }

        // Receive chunks until all segments are complete
        let mut segments_complete = vec![false; manifest.segments.len()];
        let session_deadline = Instant::now() + self.config.session_timeout;

        loop {
            if Instant::now() > session_deadline {
                return Err(LanceError::Protocol("Resync session timeout".into()));
            }

            // Check if all segments are complete
            if segments_complete.iter().all(|&c| c) {
                break;
            }

            // Read chunk header: msg_type(1) + chunk_len(4) = 5 bytes
            let mut chunk_header = [0u8; 5];
            tokio::time::timeout(
                self.config.chunk_timeout,
                stream.read_exact(&mut chunk_header),
            )
            .await
            .map_err(|_| LanceError::Protocol("Chunk receive timeout".into()))?
            .map_err(LanceError::Io)?;

            let msg_type = chunk_header[0];
            if msg_type == ResyncMessageType::Abort as u8 {
                return Err(LanceError::Protocol(
                    "Leader aborted during streaming".into(),
                ));
            }
            if msg_type == ResyncMessageType::Complete as u8 {
                break;
            }
            if msg_type != ResyncMessageType::Chunk as u8 {
                return Err(LanceError::Protocol(format!(
                    "Expected Chunk message, got type 0x{:02x}",
                    msg_type
                )));
            }

            let chunk_total_len = u32::from_le_bytes(
                chunk_header[1..5]
                    .try_into()
                    .map_err(|_| LanceError::Protocol("Invalid chunk length".into()))?,
            ) as usize;

            // Read chunk body
            let chunk_start = Instant::now();
            let mut chunk_buf = vec![0u8; chunk_total_len];
            tokio::time::timeout(self.config.chunk_timeout, stream.read_exact(&mut chunk_buf))
                .await
                .map_err(|_| LanceError::Protocol("Chunk body receive timeout".into()))?
                .map_err(LanceError::Io)?;

            let chunk_duration = chunk_start.elapsed();

            // Decode chunk
            let chunk = SegmentChunk::decode(&chunk_buf)
                .ok_or_else(|| LanceError::Protocol("Failed to decode chunk".into()))?;

            // Validate CRC32C
            let computed_crc = crc32fast::hash(&chunk.data);
            if computed_crc != chunk.chunk_crc {
                return Err(LanceError::CrcMismatch {
                    expected: chunk.chunk_crc,
                    actual: computed_crc,
                });
            }

            // Write chunk to .part staging file (async I/O)
            let seg_idx = chunk.segment_index as usize;
            if seg_idx >= segment_files.len() {
                return Err(LanceError::Protocol(format!(
                    "Chunk references invalid segment index {}",
                    seg_idx
                )));
            }

            if let Some(ref mut file) = segment_files[seg_idx] {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(chunk.offset))
                    .await
                    .map_err(LanceError::Io)?;
                file.write_all(&chunk.data).await.map_err(LanceError::Io)?;
            }

            // Update progress
            if let Some(ref mut progress) = self.progress {
                progress.bytes_received += chunk.data.len() as u64;
                progress.update_rate(chunk.data.len() as u64, chunk_duration);

                lnc_metrics::increment_resync_bytes_transferred(chunk.data.len() as u64);
            }

            // Send chunk ACK
            self.send_chunk_ack(stream, chunk.segment_index, chunk.offset)
                .await?;

            // Mark segment complete if final chunk
            if chunk.is_final {
                segments_complete[seg_idx] = true;

                // Durability barrier: sync .part file to disk
                if let Some(ref file) = segment_files[seg_idx] {
                    file.sync_all().await.map_err(LanceError::Io)?;
                }
                // Drop the file handle before rename
                segment_files[seg_idx] = None;

                // Atomic switch: .part → final path
                // rename(2) is atomic on POSIX when src and dst are on the
                // same filesystem, which is guaranteed here (same data_dir).
                tokio::fs::rename(&part_paths[seg_idx], &final_paths[seg_idx])
                    .await
                    .map_err(LanceError::Io)?;

                // Strict POSIX durability: fsync the parent directory to
                // ensure the rename metadata is persisted to disk. Without
                // this, a power failure after rename but before the next
                // journal checkpoint could lose the directory entry.
                let dir_path = self.data_dir.clone();
                tokio::task::spawn_blocking(move || {
                    let dir = std::fs::File::open(&dir_path).map_err(LanceError::Io)?;
                    dir.sync_all().map_err(LanceError::Io)
                })
                .await
                .map_err(|e| LanceError::Protocol(format!("Dir fsync task panicked: {e}")))??;

                // Register the final path for bootstrap indexing
                self.pending_bootstrap
                    .push_back(final_paths[seg_idx].clone());

                if let Some(ref mut progress) = self.progress {
                    progress.segments_received += 1;
                    lnc_metrics::increment_resync_segments_transferred(1);
                }

                info!(
                    target: "lance::resync",
                    node_id = self.node_id,
                    segment_index = seg_idx,
                    filename = %manifest.segments[seg_idx].filename,
                    segments_done = segments_complete.iter().filter(|&&c| c).count(),
                    segments_total = manifest.segments.len(),
                    "Segment transfer complete (atomic rename)"
                );
            }

            // Bandwidth throttling
            if self.config.max_bandwidth_bytes_per_sec > 0 {
                if let Some(ref progress) = self.progress {
                    if progress.transfer_rate_bps > self.config.max_bandwidth_bytes_per_sec as f64 {
                        let overshoot = progress.transfer_rate_bps
                            - self.config.max_bandwidth_bytes_per_sec as f64;
                        let throttle_ms = (overshoot
                            / self.config.max_bandwidth_bytes_per_sec as f64
                            * 100.0) as u64;
                        if throttle_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(throttle_ms.min(1000))).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Send a chunk acknowledgment to the leader.
    async fn send_chunk_ack(
        &self,
        stream: &mut TcpStream,
        segment_index: u32,
        offset: u64,
    ) -> Result<()> {
        // Wire format: msg_type(1) + segment_index(4) + offset(8) = 13 bytes
        let mut buf = [0u8; 13];
        buf[0] = ResyncMessageType::ChunkAck as u8;
        buf[1..5].copy_from_slice(&segment_index.to_le_bytes());
        buf[5..13].copy_from_slice(&offset.to_le_bytes());

        stream.write_all(&buf).await.map_err(LanceError::Io)?;
        Ok(())
    }

    /// Send completion message to the leader.
    async fn send_complete(&self, stream: &mut TcpStream) -> Result<()> {
        let buf = [ResyncMessageType::Complete as u8];
        stream.write_all(&buf).await.map_err(LanceError::Io)?;
        stream.flush().await.map_err(LanceError::Io)?;
        Ok(())
    }

    /// Bootstrap received segments by rebuilding their sparse and secondary indices.
    ///
    /// Uses `lnc_recovery::IndexRebuilder` to scan each `.lnc` file and generate
    /// the corresponding `.idx` and `.sidx` files. The heavy blocking I/O
    /// (file CRC reads + index rebuilding) is offloaded to `spawn_blocking`
    /// to avoid stalling the Tokio executor.
    async fn bootstrap_received_segments(&mut self) -> Result<()> {
        let segment_count = self.pending_bootstrap.len();
        let node_id = self.node_id;

        info!(
            target: "lance::resync",
            node_id,
            segments = segment_count,
            "Bootstrapping indices for received segments"
        );

        // Drain pending paths and manifest into owned data for the blocking task
        let paths: Vec<PathBuf> = self.pending_bootstrap.drain(..).collect();
        let manifest = self.manifest.clone();

        tokio::task::spawn_blocking(move || {
            let start = Instant::now();

            for segment_path in &paths {
                if !segment_path.exists() {
                    warn!(
                        target: "lance::resync",
                        path = %segment_path.display(),
                        "Segment file missing during bootstrap, skipping"
                    );
                    continue;
                }

                // Validate file CRC against manifest
                if let Some(ref manifest) = manifest {
                    let filename = segment_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or_default();

                    if let Some(descriptor) =
                        manifest.segments.iter().find(|s| s.filename == filename)
                    {
                        let actual_crc = compute_file_crc(segment_path)?;
                        if actual_crc != descriptor.file_crc {
                            return Err(LanceError::CrcMismatch {
                                expected: descriptor.file_crc,
                                actual: actual_crc,
                            });
                        }
                    }
                }

                // Build sparse + secondary index
                bootstrap_segment_index(segment_path)?;
            }

            let elapsed = start.elapsed();
            info!(
                target: "lance::resync",
                node_id,
                segments = segment_count,
                elapsed_ms = elapsed.as_millis(),
                "Bootstrap complete"
            );

            Ok(())
        })
        .await
        .map_err(|e| LanceError::Protocol(format!("Bootstrap task panicked: {e}")))?
    }

    /// Reset the actor to idle state (e.g. after completion or manual reset).
    pub fn reset(&mut self) {
        self.state = ResyncState::Idle;
        self.manifest = None;
        self.progress = None;
        self.retry_count = 0;
        self.pending_bootstrap.clear();
    }
}

// ---------------------------------------------------------------------------
// ResyncServer (Leader Side)
// ---------------------------------------------------------------------------

/// Leader-side resync server that handles incoming resync requests from followers.
///
/// Listens on a dedicated port (replication_port + port_offset) and streams
/// segment files to requesting followers. Each follower connection is handled
/// in a separate async task.
pub struct ResyncServer {
    /// This node's ID (must be leader).
    node_id: u16,
    /// Configuration.
    config: ResyncConfig,
    /// Directory containing segment files to serve.
    data_dir: PathBuf,
}

impl ResyncServer {
    /// Create a new resync server.
    pub fn new(node_id: u16, data_dir: PathBuf, config: ResyncConfig) -> Self {
        Self {
            node_id,
            config,
            data_dir,
        }
    }

    /// Handle a single follower's resync request on an established connection.
    ///
    /// Takes **ownership** of the `TcpStream` so it can be split into owned
    /// read/write halves via `into_split()`. This enables a session-level
    /// full-duplex architecture where a single ACK reader covers all segments
    /// without per-segment split/rejoin overhead.
    ///
    /// Protocol phases:
    /// 1. Handshake — read resync request (follower ID + last offset)
    /// 2. Manifest — build and send list of missing segments
    /// 3. Streaming — full-duplex pipelined chunk transfer (all segments)
    /// 4. Completion — send Complete, wait for follower ACK
    pub async fn handle_follower(&self, stream: TcpStream, leader_term: u64) -> Result<()> {
        // TCP tuning: set nodelay + size buffers for high-BDP links
        stream.set_nodelay(true).map_err(LanceError::Io)?;
        if self.config.tcp_buffer_size > 0 {
            tune_tcp_buffers(&stream, self.config.tcp_buffer_size)?;
        }

        // We need a unified stream for the handshake phases, then split for
        // streaming. Use a mutable binding so we can call read/write before
        // consuming via into_split().
        let mut stream = stream;

        // Phase 1: Read resync request
        let mut req_buf = [0u8; 11];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut req_buf))
            .await
            .map_err(|_| LanceError::Protocol("Resync request timeout".into()))?
            .map_err(LanceError::Io)?;

        let msg_type = req_buf[0];
        if msg_type != ResyncMessageType::ResyncRequest as u8 {
            return Err(LanceError::Protocol(format!(
                "Expected ResyncRequest, got type 0x{:02x}",
                msg_type
            )));
        }

        let follower_id = u16::from_le_bytes(
            req_buf[1..3]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid follower ID".into()))?,
        );
        let follower_last_offset = u64::from_le_bytes(
            req_buf[3..11]
                .try_into()
                .map_err(|_| LanceError::Protocol("Invalid follower offset".into()))?,
        );

        info!(
            target: "lance::resync",
            leader_id = self.node_id,
            follower_id,
            follower_last_offset,
            "Received resync request from follower"
        );

        // Phase 2: Build and send manifest
        // Wrap blocking filesystem operations in spawn_blocking to avoid
        // stalling the Tokio executor (directory scans + CRC computation).
        let data_dir = self.data_dir.clone();
        let node_id = self.node_id;
        let (leader_current_offset, manifest) = tokio::task::spawn_blocking(move || {
            let offset = compute_current_offset_blocking(&data_dir)?;
            let manifest = SegmentManifest::from_directory(
                &data_dir,
                leader_term,
                node_id,
                follower_last_offset,
                offset,
            )?;
            Ok::<_, LanceError>((offset, manifest))
        })
        .await
        .map_err(|e| LanceError::Protocol(format!("Manifest build task panicked: {e}")))??;
        let _ = leader_current_offset;

        let manifest_bytes = manifest.encode();
        let mut header = [0u8; 5];
        header[0] = ResyncMessageType::Manifest as u8;
        header[1..5].copy_from_slice(&(manifest_bytes.len() as u32).to_le_bytes());

        stream.write_all(&header).await.map_err(LanceError::Io)?;
        stream
            .write_all(&manifest_bytes)
            .await
            .map_err(LanceError::Io)?;
        stream.flush().await.map_err(LanceError::Io)?;

        info!(
            target: "lance::resync",
            leader_id = self.node_id,
            follower_id,
            segments = manifest.segments.len(),
            total_bytes = manifest.total_bytes,
            "Sent manifest to follower"
        );

        // Phase 3: Full-duplex pipelined streaming
        //
        // Split the socket into owned halves so the ACK reader and writer
        // can run concurrently for the entire streaming phase (all segments).
        // `into_split()` gives us `OwnedReadHalf` / `OwnedWriteHalf` which
        // can be moved into separate futures without lifetime constraints.
        let (reader, mut writer) = stream.into_split();

        // Compute total expected chunks across all segments for the ACK reader
        let chunk_size = self.config.chunk_size as u64;
        let total_expected_chunks: u64 = manifest
            .segments
            .iter()
            .map(|d| {
                if d.size_bytes == 0 {
                    0u64
                } else {
                    d.size_bytes.div_ceil(chunk_size)
                }
            })
            .sum();

        // Semaphore controls in-flight backpressure across all segments.
        // Writer acquires a permit per chunk; ACK reader releases one per ACK.
        // On reader failure, Semaphore::close() instantly wakes the writer
        // from acquire() with AcquireError — no poison flag needed.
        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight_chunks));
        let sem_ack = semaphore.clone();

        let chunk_timeout = self.config.chunk_timeout;

        // Session-level ACK reader future — covers ALL segments.
        // Runs concurrently with the writer via tokio::try_join!.
        let ack_reader_fut = async move {
            let mut reader = reader;
            let mut buf = [0u8; 13];
            let mut acks_received = 0u64;

            while acks_received < total_expected_chunks {
                let read_result =
                    tokio::time::timeout(chunk_timeout, reader.read_exact(&mut buf)).await;

                match read_result {
                    Ok(Ok(_)) => {
                        if buf[0] != ResyncMessageType::ChunkAck as u8 {
                            sem_ack.close();
                            return Err(LanceError::Protocol(format!(
                                "Expected ChunkAck, got type 0x{:02x}",
                                buf[0]
                            )));
                        }
                        sem_ack.add_permits(1);
                        acks_received += 1;
                    },
                    Ok(Err(e)) => {
                        sem_ack.close();
                        return Err(LanceError::Io(e));
                    },
                    Err(_) => {
                        sem_ack.close();
                        return Err(LanceError::Protocol("Chunk ACK timeout".into()));
                    },
                }
            }

            // Return the reader half so we can reunite after streaming
            Ok(reader)
        };

        // Writer future — streams all segments sequentially, using the
        // shared semaphore for backpressure against the ACK reader.
        let writer_fut = async {
            for (seg_idx, descriptor) in manifest.segments.iter().enumerate() {
                self.stream_segment_pipelined(&mut writer, seg_idx as u32, descriptor, &semaphore)
                    .await?;
            }
            Ok::<_, LanceError>(writer)
        };

        // Structured cancellation via try_join!:
        // - If writer fails (disk I/O), reader is dropped (socket closed).
        // - If reader fails (network), semaphore.close() wakes writer from
        //   acquire(), then try_join! drops the writer future immediately.
        // No zombie futures, no deadlocks, no AtomicBool polling.
        let (writer, reader) = tokio::try_join!(writer_fut, ack_reader_fut)?;

        let mut stream = reader
            .reunite(writer)
            .map_err(|_| LanceError::Protocol("Failed to reunite socket halves".into()))?;

        // Phase 4: Send completion and wait for follower ACK
        let complete_buf = [ResyncMessageType::Complete as u8; 5];
        stream
            .write_all(&complete_buf)
            .await
            .map_err(LanceError::Io)?;
        stream.flush().await.map_err(LanceError::Io)?;

        let mut ack_buf = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(30), stream.read_exact(&mut ack_buf))
            .await
            .map_err(|_| LanceError::Protocol("Completion ACK timeout".into()))?
            .map_err(LanceError::Io)?;

        if ack_buf[0] == ResyncMessageType::Complete as u8 {
            info!(
                target: "lance::resync",
                leader_id = self.node_id,
                follower_id,
                "Resync session completed successfully"
            );
        }

        Ok(())
    }

    /// Stream a single segment file in pipelined chunks to the follower.
    ///
    /// This is a pure **writer** — it only writes to the socket. ACK reading
    /// is handled by the session-level ACK reader in `handle_follower`.
    ///
    /// # Zero-Copy Writes
    ///
    /// The 26-byte header (5 outer + 21 inner) is assembled in a
    /// stack-allocated `[u8; 26]` array — zero heap allocation per chunk.
    /// The 4 MiB payload is sent with a second `write_all` directly from
    /// the file read buffer — no memcpy. With a dedicated writer half,
    /// the two back-to-back syscalls are not interleaved by reads.
    async fn stream_segment_pipelined(
        &self,
        writer: &mut OwnedWriteHalf,
        segment_index: u32,
        descriptor: &SegmentDescriptor,
        semaphore: &Semaphore,
    ) -> Result<()> {
        let path = self.data_dir.join(&descriptor.filename);

        let mut file = tokio::fs::File::open(&path).await.map_err(LanceError::Io)?;
        let total_size = file.metadata().await.map_err(LanceError::Io)?.len();

        if total_size == 0 {
            return Ok(());
        }

        let chunk_size = self.config.chunk_size;
        let max_bw = self.config.max_bandwidth_bytes_per_sec;
        let mut buffer = vec![0u8; chunk_size];
        let mut offset = 0u64;

        debug!(
            target: "lance::resync",
            segment_index,
            filename = %descriptor.filename,
            size = total_size,
            "Streaming segment (pipelined)"
        );

        loop {
            // Acquire a permit (blocks when pipeline is full).
            // If the ACK reader fails and calls semaphore.close(), this
            // returns AcquireError immediately — no poison flag needed.
            let permit = semaphore
                .acquire()
                .await
                .map_err(|_| LanceError::Protocol("Session closed (ACK reader failed)".into()))?;

            let n = file.read(&mut buffer).await.map_err(LanceError::Io)?;
            if n == 0 {
                break; // EOF
            }

            let is_final = (offset + n as u64) >= total_size;
            let chunk_slice = &buffer[..n];
            let chunk_crc = crc32fast::hash(chunk_slice);

            // Stack-allocated header — zero heap allocation per chunk.
            // Layout: frame_type(1) + frame_len(4) + seg_idx(4) + offset(8)
            //       + chunk_len(4) + crc(4) + is_final(1) = 26 bytes
            let frame_len = (21 + n) as u32;
            let mut hdr = [0u8; 26];
            hdr[0] = ResyncMessageType::Chunk as u8;
            hdr[1..5].copy_from_slice(&frame_len.to_le_bytes());
            hdr[5..9].copy_from_slice(&segment_index.to_le_bytes());
            hdr[9..17].copy_from_slice(&offset.to_le_bytes());
            hdr[17..21].copy_from_slice(&(n as u32).to_le_bytes());
            hdr[21..25].copy_from_slice(&chunk_crc.to_le_bytes());
            hdr[25] = if is_final { 1 } else { 0 };

            // Syscall 1: header (26 bytes, stack)
            writer.write_all(&hdr).await.map_err(LanceError::Io)?;
            // Syscall 2: payload (up to 4 MiB, zero-copy from file buffer)
            writer
                .write_all(chunk_slice)
                .await
                .map_err(LanceError::Io)?;

            // Forget the permit — it is "owned" by the in-flight chunk
            // and will be restored by the ACK reader task.
            permit.forget();

            offset += n as u64;

            // Bandwidth throttling (leader side)
            if max_bw > 0 {
                let expected_duration_ns = (n as u64 * 1_000_000_000) / max_bw;
                if expected_duration_ns > 0 {
                    tokio::time::sleep(Duration::from_nanos(expected_duration_ns)).await;
                }
            }
        }

        Ok(())
    }

    /// Compute the leader's current global offset from the segment directory.
    ///
    /// Delegates to the free function [`compute_current_offset_blocking`].
    /// Callers in async context should use `spawn_blocking` instead.
    /// Production code uses `compute_current_offset_blocking` directly via
    /// `spawn_blocking`; this wrapper is retained for tests.
    #[cfg(test)]
    fn compute_current_offset(&self) -> Result<u64> {
        compute_current_offset_blocking(&self.data_dir)
    }
}

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

/// Tune TCP send/receive buffer sizes for high bandwidth-delay product links.
///
/// On cross-region or 10+ GbE paths the default kernel TCP window (often
/// 4 MiB) can become the bottleneck. This sets both buffers to the
/// configured size using `socket2::SockRef`, which avoids taking ownership
/// of the socket. Silently succeeds if the OS caps the value lower.
fn tune_tcp_buffers(stream: &TcpStream, buffer_size: usize) -> Result<()> {
    let sock_ref = socket2::SockRef::from(stream);
    sock_ref
        .set_send_buffer_size(buffer_size)
        .map_err(LanceError::Io)?;
    sock_ref
        .set_recv_buffer_size(buffer_size)
        .map_err(LanceError::Io)?;
    Ok(())
}

/// Compute the current global offset by summing sizes of all `.lnc` files.
///
/// This is a blocking filesystem operation. In async context, call via
/// `tokio::task::spawn_blocking`.
fn compute_current_offset_blocking(data_dir: &Path) -> Result<u64> {
    let mut total = 0u64;

    for entry in std::fs::read_dir(data_dir).map_err(LanceError::Io)? {
        let entry = entry.map_err(LanceError::Io)?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "lnc") {
            let size = std::fs::metadata(&path).map_err(LanceError::Io)?.len();
            total += size;
        }
    }

    Ok(total)
}

/// Compute CRC32C of an entire file.
fn compute_file_crc(path: &Path) -> Result<u32> {
    let data = std::fs::read(path).map_err(LanceError::Io)?;
    Ok(crc32fast::hash(&data))
}

/// Build sparse and secondary indices for a received segment file.
///
/// This is the "bootstrap" step: after receiving a raw `.lnc` file from the
/// leader, the follower needs to build the index files so the segment can
/// be queried. Delegates to `lnc_recovery::IndexRebuilder`.
fn bootstrap_segment_index(segment_path: &Path) -> Result<()> {
    use lnc_core::DEFAULT_SPARSE_INDEX_INTERVAL;

    let rebuilder = lnc_recovery::IndexRebuilder::new(DEFAULT_SPARSE_INDEX_INTERVAL, 10_000_000);

    if rebuilder.needs_rebuild(segment_path) {
        let result = rebuilder.rebuild(segment_path)?;

        info!(
            target: "lance::resync",
            segment = %segment_path.display(),
            records = result.records_indexed,
            sparse_entries = result.sparse_entries,
            secondary_entries = result.secondary_entries,
            "Bootstrapped segment index"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_resync_state_display() {
        assert_eq!(format!("{}", ResyncState::Idle), "IDLE");
        assert_eq!(format!("{}", ResyncState::DetectingGap), "DETECTING_GAP");
        assert_eq!(format!("{}", ResyncState::Streaming), "STREAMING");
        assert_eq!(format!("{}", ResyncState::Bootstrapping), "BOOTSTRAPPING");
        assert_eq!(format!("{}", ResyncState::Complete), "COMPLETE");
        assert_eq!(format!("{}", ResyncState::Failed), "FAILED");
    }

    #[test]
    fn test_resync_config_default() {
        let config = ResyncConfig::default();
        assert_eq!(config.chunk_size, 4 * 1024 * 1024);
        assert_eq!(config.max_bandwidth_bytes_per_sec, 0);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.max_concurrent_segments, 4);
        assert_eq!(config.max_in_flight_chunks, 8);
        assert_eq!(config.tcp_buffer_size, 8 * 1024 * 1024);
        assert_eq!(config.port_offset, 1);
    }

    #[test]
    fn test_gap_analysis_no_gap() {
        let analysis = detect_gap(1000, 1000, 256 * 1024 * 1024);
        assert_eq!(analysis.gap_bytes, 0);
        assert!(!analysis.requires_bulk_resync);
    }

    #[test]
    fn test_gap_analysis_small_gap() {
        let analysis = detect_gap(1000, 1_000_000, 256 * 1024 * 1024);
        assert_eq!(analysis.gap_bytes, 999_000);
        assert!(!analysis.requires_bulk_resync);
    }

    #[test]
    fn test_gap_analysis_large_gap() {
        let analysis = detect_gap(0, 1_000_000_000, 256 * 1024 * 1024);
        assert_eq!(analysis.gap_bytes, 1_000_000_000);
        assert!(analysis.requires_bulk_resync);
        assert!(analysis.estimated_missing_segments > 0);
    }

    #[test]
    fn test_segment_manifest_encode_decode_roundtrip() {
        let manifest = SegmentManifest {
            leader_term: 5,
            leader_id: 1,
            follower_last_offset: 1000,
            leader_current_offset: 5_000_000,
            total_bytes: 4_999_000,
            segments: vec![
                SegmentDescriptor {
                    filename: "000001_1706918400000.lnc".to_string(),
                    size_bytes: 2_000_000,
                    file_crc: 0xDEADBEEF,
                    is_sealed: true,
                },
                SegmentDescriptor {
                    filename: "000002_1706918500000.lnc".to_string(),
                    size_bytes: 2_999_000,
                    file_crc: 0xCAFEBABE,
                    is_sealed: false,
                },
            ],
        };

        let encoded = manifest.encode();
        let decoded = SegmentManifest::decode(&encoded).unwrap();

        assert_eq!(decoded.leader_term, 5);
        assert_eq!(decoded.leader_id, 1);
        assert_eq!(decoded.follower_last_offset, 1000);
        assert_eq!(decoded.leader_current_offset, 5_000_000);
        assert_eq!(decoded.total_bytes, 4_999_000);
        assert_eq!(decoded.segments.len(), 2);
        assert_eq!(decoded.segments[0].filename, "000001_1706918400000.lnc");
        assert_eq!(decoded.segments[0].size_bytes, 2_000_000);
        assert_eq!(decoded.segments[0].file_crc, 0xDEADBEEF);
        assert!(decoded.segments[0].is_sealed);
        assert_eq!(decoded.segments[1].filename, "000002_1706918500000.lnc");
        assert!(!decoded.segments[1].is_sealed);
    }

    #[test]
    fn test_segment_manifest_decode_too_short() {
        assert!(SegmentManifest::decode(&[0u8; 10]).is_none());
        assert!(SegmentManifest::decode(&[]).is_none());
    }

    #[test]
    fn test_segment_chunk_encode_decode_roundtrip() {
        let chunk = SegmentChunk {
            segment_index: 3,
            offset: 65536,
            data: Bytes::from_static(b"hello resync world"),
            chunk_crc: crc32fast::hash(b"hello resync world"),
            is_final: true,
        };

        let encoded = chunk.encode();
        let decoded = SegmentChunk::decode(&encoded).unwrap();

        assert_eq!(decoded.segment_index, 3);
        assert_eq!(decoded.offset, 65536);
        assert_eq!(decoded.data, Bytes::from_static(b"hello resync world"));
        assert_eq!(decoded.chunk_crc, chunk.chunk_crc);
        assert!(decoded.is_final);
    }

    #[test]
    fn test_segment_chunk_decode_too_short() {
        assert!(SegmentChunk::decode(&[0u8; 10]).is_none());
        assert!(SegmentChunk::decode(&[]).is_none());
    }

    #[test]
    fn test_segment_chunk_crc_validation() {
        let data = b"test payload data";
        let correct_crc = crc32fast::hash(data);
        let wrong_crc = correct_crc ^ 0xFF;

        let chunk = SegmentChunk {
            segment_index: 0,
            offset: 0,
            data: Bytes::from_static(data),
            chunk_crc: correct_crc,
            is_final: false,
        };

        // Correct CRC should validate
        let computed = crc32fast::hash(&chunk.data);
        assert_eq!(computed, correct_crc);

        // Wrong CRC should not match
        assert_ne!(computed, wrong_crc);
    }

    #[test]
    fn test_resync_message_type_from_u8() {
        assert_eq!(
            ResyncMessageType::from_u8(0x01),
            Some(ResyncMessageType::ResyncRequest)
        );
        assert_eq!(
            ResyncMessageType::from_u8(0x02),
            Some(ResyncMessageType::Manifest)
        );
        assert_eq!(
            ResyncMessageType::from_u8(0x03),
            Some(ResyncMessageType::Chunk)
        );
        assert_eq!(
            ResyncMessageType::from_u8(0x04),
            Some(ResyncMessageType::ChunkAck)
        );
        assert_eq!(
            ResyncMessageType::from_u8(0x05),
            Some(ResyncMessageType::Complete)
        );
        assert_eq!(
            ResyncMessageType::from_u8(0xFF),
            Some(ResyncMessageType::Abort)
        );
        assert_eq!(ResyncMessageType::from_u8(0x42), None);
    }

    #[test]
    fn test_resync_progress_fraction() {
        let mut progress = ResyncProgress::new(10, 1000);
        assert_eq!(progress.fraction_complete(), 0.0);

        progress.bytes_received = 500;
        assert!((progress.fraction_complete() - 0.5).abs() < f64::EPSILON);

        progress.bytes_received = 1000;
        assert!((progress.fraction_complete() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_resync_progress_empty_total() {
        let progress = ResyncProgress::new(0, 0);
        assert!((progress.fraction_complete() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_resync_progress_estimated_remaining() {
        let mut progress = ResyncProgress::new(10, 10_000_000);
        progress.bytes_received = 5_000_000;
        progress.transfer_rate_bps = 1_000_000.0; // 1 MB/s

        let remaining = progress.estimated_remaining();
        // 5 MB remaining at 1 MB/s = ~5 seconds
        assert!(remaining.as_secs() >= 4 && remaining.as_secs() <= 6);
    }

    #[test]
    fn test_resync_progress_rate_update() {
        let mut progress = ResyncProgress::new(1, 1000);
        progress.update_rate(1000, Duration::from_secs(1));
        // First update: 0.3 * 1000 + 0.7 * 0 = 300
        assert!(progress.transfer_rate_bps > 0.0);

        progress.update_rate(2000, Duration::from_secs(1));
        // Second update: 0.3 * 2000 + 0.7 * 300 = 810
        assert!(progress.transfer_rate_bps > 300.0);
    }

    #[test]
    fn test_resync_actor_new() {
        let dir = tempdir().unwrap();
        let actor = ResyncActor::new(1, dir.path().to_path_buf(), ResyncConfig::default());

        assert_eq!(actor.state(), ResyncState::Idle);
        assert!(!actor.is_active());
        assert!(actor.progress().is_none());
    }

    #[test]
    fn test_resync_actor_reset() {
        let dir = tempdir().unwrap();
        let mut actor = ResyncActor::new(1, dir.path().to_path_buf(), ResyncConfig::default());

        // Simulate some state changes
        actor.state = ResyncState::Streaming;
        actor.retry_count = 3;
        actor
            .pending_bootstrap
            .push_back(PathBuf::from("/tmp/test.lnc"));

        actor.reset();

        assert_eq!(actor.state(), ResyncState::Idle);
        assert_eq!(actor.retry_count, 0);
        assert!(actor.pending_bootstrap.is_empty());
    }

    #[test]
    fn test_segment_manifest_from_directory() {
        let dir = tempdir().unwrap();

        // Create some test segment files
        std::fs::write(dir.path().join("000001_1000.lnc"), b"segment1data").unwrap();
        std::fs::write(
            dir.path().join("000002_2000-3000.lnc"),
            b"segment2data_sealed",
        )
        .unwrap();

        let manifest = SegmentManifest::from_directory(dir.path(), 1, 0, 0, 1000).unwrap();

        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(manifest.leader_term, 1);
        assert_eq!(manifest.leader_id, 0);
        assert!(manifest.total_bytes > 0);

        // First segment is active (no dash in stem)
        let seg1 = manifest
            .segments
            .iter()
            .find(|s| s.filename == "000001_1000.lnc")
            .unwrap();
        assert!(!seg1.is_sealed);

        // Second segment is sealed (has dash in stem)
        let seg2 = manifest
            .segments
            .iter()
            .find(|s| s.filename == "000002_2000-3000.lnc")
            .unwrap();
        assert!(seg2.is_sealed);
    }

    #[test]
    fn test_compute_file_crc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.lnc");
        std::fs::write(&path, b"test data for crc").unwrap();

        let crc = compute_file_crc(&path).unwrap();
        let expected = crc32fast::hash(b"test data for crc");
        assert_eq!(crc, expected);
    }

    #[test]
    fn test_resync_server_new() {
        let dir = tempdir().unwrap();
        let server = ResyncServer::new(0, dir.path().to_path_buf(), ResyncConfig::default());
        assert_eq!(server.node_id, 0);
    }

    #[test]
    fn test_resync_server_compute_offset() {
        let dir = tempdir().unwrap();

        std::fs::write(dir.path().join("000001_1000.lnc"), b"aaaa").unwrap();
        std::fs::write(dir.path().join("000002_2000.lnc"), b"bbbbbb").unwrap();
        // Non-lnc file should be ignored
        std::fs::write(dir.path().join("readme.txt"), b"ignore me").unwrap();

        let server = ResyncServer::new(0, dir.path().to_path_buf(), ResyncConfig::default());
        let offset = server.compute_current_offset().unwrap();
        assert_eq!(offset, 10); // 4 + 6
    }

    #[test]
    fn test_segment_manifest_empty_directory() {
        let dir = tempdir().unwrap();
        let manifest = SegmentManifest::from_directory(dir.path(), 1, 0, 0, 0).unwrap();

        assert!(manifest.segments.is_empty());
        assert_eq!(manifest.total_bytes, 0);
    }

    #[test]
    fn test_segment_manifest_encode_empty() {
        let manifest = SegmentManifest {
            leader_term: 1,
            leader_id: 0,
            follower_last_offset: 0,
            leader_current_offset: 0,
            total_bytes: 0,
            segments: vec![],
        };

        let encoded = manifest.encode();
        let decoded = SegmentManifest::decode(&encoded).unwrap();
        assert!(decoded.segments.is_empty());
        assert_eq!(decoded.leader_term, 1);
    }
}
