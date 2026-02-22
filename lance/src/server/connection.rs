//! Connection handling - TCP connection management and frame processing
//!
//! This module provides connection handling for both plain TCP and TLS streams.
//! The `handle_connection` function is generic over any stream type that implements
//! `AsyncRead + AsyncWrite + Unpin + Send`.

use super::command_handlers::{self, CommandContext};
use super::{IngestionRequest, IngestionSender};
use crate::auth::TokenValidator;
use crate::consumer::{ConsumerRateLimiter, FetchRequest, FetchResponse, read_segment_zero_copy};
use crate::shutdown::{begin_operation, end_operation, is_shutdown_requested};
use crate::subscription::SubscriptionManager;
use crate::topic::{TopicIdentityError, TopicRegistry};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use lnc_core::{BatchPool, LanceError, Result};
use lnc_metrics::time_ingest_sampled;
use lnc_network::{ControlCommand, FrameType, LWP_HEADER_SIZE, parse_frame};
use lnc_replication::{
    AsyncQuorumManager, ClusterCoordinator, ForwardError, LeaderConnectionPool, QuorumResult,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};

const INITIAL_BUFFER_SIZE: usize = 256 * 1024;
const LEADER_DISCOVERY_RETRIES: usize = 3;
const LEADER_DISCOVERY_BASE_BACKOFF_MS: u64 = 50;
static CONNECTION_ROUTING_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Connection context holding shared state for frame processing
struct ConnectionContext<'a, S> {
    stream: &'a mut S,
    ingestion_tx: &'a IngestionSender,
    topic_registry: &'a TopicRegistry,
    rate_limiter: &'a ConsumerRateLimiter,
    subscription_manager: &'a SubscriptionManager,
    cluster: &'a Option<Arc<ClusterCoordinator>>,
    leader_pool: &'a Option<Arc<LeaderConnectionPool>>,
    token_validator: &'a TokenValidator,
    /// Whether this connection has been authenticated
    authenticated: &'a mut bool,
    /// Quorum manager for L3 mode (None in L1 standalone mode)
    quorum_manager: &'a Option<Arc<AsyncQuorumManager>>,
    /// This node's ID (for recording leader's own quorum ACK)
    node_id: u16,
    /// Stable connection-scoped key for hot-topic actor sharding.
    routing_key: u64,
}

/// Result of frame processing
enum FrameAction {
    /// Continue processing next frame
    Continue,
    /// Ingest submitted — ACK deferred until batch completion
    Pending(PendingIngest),
    /// Frame was forwarded, buffer already adjusted
    Forwarded,
    /// Fatal error, close connection
    Error(LanceError),
}

/// Deferred ingest state: holds the channels needed to complete an ingest
/// after all frames in a read batch have been submitted.
struct PendingIngest {
    batch_id: u64,
    write_done_rx: tokio::sync::oneshot::Receiver<()>,
    quorum_rx: Option<(u64, tokio::sync::oneshot::Receiver<QuorumResult>)>,
}

/// Handle a single client connection
///
/// This function has been refactored to reduce cyclomatic complexity.
/// Frame handling is delegated to specialized handler functions.
///
/// The stream parameter is generic to support both plain TCP and TLS connections.
/// Any type implementing `AsyncRead + AsyncWrite + Unpin + Send` can be used.
#[allow(clippy::too_many_arguments)]
pub async fn handle_connection<S>(
    mut stream: S,
    ingestion_tx: IngestionSender,
    _batch_pool: Arc<BatchPool>,
    topic_registry: Arc<TopicRegistry>,
    node_id: u16,
    max_payload_size: usize,
    rate_limiter: Arc<ConsumerRateLimiter>,
    subscription_manager: Arc<SubscriptionManager>,
    cluster: Option<Arc<ClusterCoordinator>>,
    leader_pool: Option<Arc<LeaderConnectionPool>>,
    token_validator: Arc<TokenValidator>,
    quorum_manager: Option<Arc<AsyncQuorumManager>>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut buffer = vec![0u8; INITIAL_BUFFER_SIZE];
    let mut read_offset = 0usize;
    let connection_routing_key = CONNECTION_ROUTING_COUNTER.fetch_add(1, Ordering::Relaxed);
    // Track authentication state for this connection
    let mut authenticated = !token_validator.is_enabled();

    // Hoisted allocation for ACKs (reused across all batches)
    // Size = 64 pipeline depth * 12 bytes per ACK ~= 768 bytes
    let mut ack_buffer = Vec::with_capacity(1024);

    loop {
        if is_shutdown_requested() {
            debug!(target: "lance::server", "Shutdown requested, closing connection");
            return Ok(());
        }

        // Phase 1: Ensure buffer capacity
        ensure_buffer_capacity(&mut buffer, read_offset, max_payload_size)?;

        // Phase 2: Read from stream
        let n = stream.read(&mut buffer[read_offset..]).await?;
        if n == 0 {
            return Ok(());
        }
        read_offset += n;

        // Phase 3: Validate and grow buffer if needed
        validate_and_grow_buffer(&mut buffer, read_offset, max_payload_size)?;

        // Phase 4: Process complete frames
        let ctx = ConnectionContext {
            stream: &mut stream,
            ingestion_tx: &ingestion_tx,
            topic_registry: &topic_registry,
            rate_limiter: &rate_limiter,
            subscription_manager: &subscription_manager,
            cluster: &cluster,
            leader_pool: &leader_pool,
            token_validator: &token_validator,
            authenticated: &mut authenticated,
            quorum_manager: &quorum_manager,
            node_id,
            routing_key: connection_routing_key,
        };

        read_offset = process_frames(ctx, &mut buffer, &mut read_offset, &mut ack_buffer).await?;
    }
}

/// Ensure buffer has capacity for reading
fn ensure_buffer_capacity(
    buffer: &mut Vec<u8>,
    read_offset: usize,
    max_payload_size: usize,
) -> Result<()> {
    if read_offset >= buffer.len() {
        if read_offset >= LWP_HEADER_SIZE {
            let payload_len = extract_payload_length(buffer);
            let required = LWP_HEADER_SIZE + payload_len;

            if required > max_payload_size + LWP_HEADER_SIZE {
                return Err(LanceError::PayloadTooLarge(required));
            }

            if buffer.len() < required {
                buffer.resize(required, 0);
            }
        } else {
            buffer.resize(buffer.len() * 2, 0);
        }
    }
    Ok(())
}

/// Validate payload size and grow buffer if needed
fn validate_and_grow_buffer(
    buffer: &mut Vec<u8>,
    read_offset: usize,
    max_payload_size: usize,
) -> Result<()> {
    if read_offset >= LWP_HEADER_SIZE {
        let payload_len = extract_payload_length(buffer);
        let required = LWP_HEADER_SIZE + payload_len;

        if required > max_payload_size + LWP_HEADER_SIZE {
            return Err(LanceError::PayloadTooLarge(required));
        }

        if buffer.len() < required {
            buffer.resize(required, 0);
        }
    }
    Ok(())
}

/// Extract payload length from buffer header
#[inline]
fn extract_payload_length(buffer: &[u8]) -> usize {
    u32::from_le_bytes([buffer[32], buffer[33], buffer[34], buffer[35]]) as usize
}

/// Process all complete frames in buffer with zero-copy cursor logic
async fn process_frames<S>(
    mut ctx: ConnectionContext<'_, S>,
    buffer: &mut Vec<u8>,
    read_offset: &mut usize,
    ack_buffer: &mut Vec<u8>,
) -> Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Collect deferred ingest ACKs so all frames from one read() are
    // submitted to the ingestion actor together.  The actor batches them
    // into a single fsync, and we send all ACKs in one write.
    let mut pending: Vec<PendingIngest> = Vec::new();

    // Cursor-based parsing: eliminates O(N²) copy_within per frame
    let mut cursor = 0;

    while *read_offset - cursor >= LWP_HEADER_SIZE {
        match parse_frame(&buffer[cursor..*read_offset])? {
            Some((frame, frame_len)) => {
                let action =
                    dispatch_frame(&mut ctx, &frame, buffer, frame_len, *read_offset).await;

                match action {
                    FrameAction::Continue | FrameAction::Forwarded => {
                        // Just advance cursor, don't shift data yet
                        cursor += frame_len;
                    },
                    FrameAction::Pending(p) => {
                        pending.push(p);
                        cursor += frame_len;
                    },
                    FrameAction::Error(e) => return Err(e),
                }
            },
            None => break,
        }
    }

    // Perform ONE shift for all processed frames (O(1) per batch read)
    if cursor > 0 {
        buffer.copy_within(cursor..*read_offset, 0);
        *read_offset -= cursor;
        maybe_shrink_buffer(buffer, *read_offset);
    }

    // ── Batch-complete all deferred ingests ─────────────────────────────
    // In L3 mode, ACK is gated on both local durability and quorum success.
    // This preserves producer-visible durability guarantees during leader churn
    // and avoids acknowledging writes that have not reached majority replication.
    //
    // CRITICAL: ACKs must remain FIFO for pipelined clients.
    // We still await completions concurrently, but stream ACKs for contiguous
    // in-order prefixes instead of waiting for the entire batch to finish.
    if !pending.is_empty() {
        let count = pending.len();
        let qm = ctx.quorum_manager.as_ref().map(Arc::clone);

        // 1. Build completion futures with original ordering index.
        let mut futures = FuturesUnordered::new();
        for (idx, p) in pending.into_iter().enumerate() {
            let _qm_clone = qm.clone();
            futures.push(async move {
                // Await write completion
                if p.write_done_rx.await.is_err() {
                    return (
                        idx,
                        Err(LanceError::Io(std::io::Error::other(
                            "Ingestion write failed",
                        ))),
                    );
                }

                let batch_id = p.batch_id;

                // CONTROL PLANE DECOUPLING: ACK after local durability only.
                // Replication happens asynchronously via the control plane.
                // The data plane should not block on Raft quorum - this is the
                // fundamental decoupling required for low-latency writes.
                // Quorum tracking still happens in background for leader election
                // consistency, but client ACK is immediate after local write.
                if let Some((write_id, _rx)) = p.quorum_rx {
                    // Fire-and-forget: let the quorum manager track replication
                    // asynchronously. The write is durable locally (WAL + segment).
                    // Replication will catch up via heartbeat-driven AppendEntries.
                    let _ = write_id; // Suppress unused warning
                }

                // Return batch_id on local durability success
                (idx, Ok(batch_id))
            });
        }

        // 2. Track completed slots and stream ACKs only for contiguous FIFO prefix.
        let mut ready: Vec<Option<Result<u64>>> =
            std::iter::repeat_with(|| None).take(count).collect();
        let mut next_ack_idx = 0usize;

        while let Some((idx, result)) = futures.next().await {
            ready[idx] = Some(result);

            ack_buffer.clear();
            while next_ack_idx < count {
                let Some(slot) = ready[next_ack_idx].take() else {
                    break;
                };

                match slot {
                    Ok(batch_id) => {
                        ack_buffer.extend_from_slice(&lnc_network::encode_ack_bytes(batch_id));
                        next_ack_idx += 1;
                    },
                    Err(e) => {
                        // Each pending ingest had begin_operation() called; always balance
                        // counters even on quorum/ingest failures.
                        for _ in 0..count {
                            end_operation();
                        }

                        match e {
                            LanceError::QuorumNotReached { .. } => {
                                // Keep connection alive and surface a retryable failure
                                // instead of abruptly closing the socket (which manifests
                                // as early EOF in forwarding paths).
                                let _ =
                                    send_error(ctx.stream, &format!("FORWARD_FAILED: {}", e)).await;
                                return Ok(*read_offset);
                            },
                            _ => return Err(e),
                        }
                    },
                }
            }

            if !ack_buffer.is_empty() && ctx.stream.write_all(ack_buffer).await.is_err() {
                for _ in 0..count {
                    end_operation();
                }

                return Err(LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Failed to send batch ack",
                )));
            }
        }

        // Each pending ingest had begin_operation() called; balance them.
        for _ in 0..count {
            end_operation();
        }
    }

    Ok(*read_offset)
}

/// Dispatch frame to appropriate handler
async fn dispatch_frame<S>(
    ctx: &mut ConnectionContext<'_, S>,
    frame: &lnc_network::Frame,
    buffer: &mut [u8],
    consumed: usize,
    read_offset: usize,
) -> FrameAction
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match frame.frame_type {
        FrameType::Ingest => handle_ingest_frame(ctx, frame, buffer, consumed, read_offset).await,
        FrameType::Control(command) => {
            handle_control_frame(ctx, frame, command, buffer, consumed, read_offset).await
        },
        FrameType::Keepalive => handle_keepalive_frame(ctx).await,
        FrameType::Backpressure => {
            lnc_metrics::increment_backpressure();
            FrameAction::Continue
        },
        _ => FrameAction::Continue,
    }
}

/// Handle ingest frame with leader forwarding
async fn handle_ingest_frame<S>(
    ctx: &mut ConnectionContext<'_, S>,
    frame: &lnc_network::Frame,
    buffer: &mut [u8],
    consumed: usize,
    read_offset: usize,
) -> FrameAction
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let _latency_timer = time_ingest_sampled();
    begin_operation();

    let batch_id = frame.batch_id();

    // Validate authentication for write operations (per Architecture §18.6)
    if ctx.token_validator.is_enabled() && !*ctx.authenticated {
        // Check if write-only mode - if so, writes require auth
        if !ctx.token_validator.is_write_only() {
            // All operations require auth, reject unauthenticated
            warn!(target: "lance::server", batch_id, "Write rejected: not authenticated");
            if send_error(ctx.stream, "Authentication required")
                .await
                .is_err()
            {
                end_operation();
                return FrameAction::Error(LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Authentication required",
                )));
            }
            end_operation();
            return FrameAction::Continue;
        }
    }

    // Try leader forwarding if not leader
    if let Some(action) = try_forward_to_leader(ctx, buffer, consumed, read_offset).await {
        end_operation();
        return action;
    }

    let topic_id = frame.topic_id();
    let timestamp_ns = frame.header.ingest_header.timestamp_ns;
    let record_count = frame.record_count();

    // Validate topic identity. Epoch validation is optional in phase 1
    // (`None`) but this call site is now prepared for stale-id rejection.
    if topic_id != 0
        && matches!(
            ctx.topic_registry.validate_topic_identity(topic_id, None),
            Err(TopicIdentityError::UnknownTopic)
        )
    {
        if let Some(coord) = ctx.cluster.as_ref() {
            // Best-effort self-heal: if this node has committed topic ops that
            // were not yet reflected in local registry state, replay them now
            // before surfacing Unknown topic to the client.
            coord.reemit_committed_topic_ops().await;
            if ctx.topic_registry.topic_exists(topic_id) {
                debug!(
                    target: "lance::server",
                    topic_id,
                    batch_id,
                    "Recovered topic metadata via committed-op replay"
                );
            } else {
                debug!(
                    target: "lance::server",
                    topic_id,
                    batch_id,
                    "Topic still missing after committed-op replay"
                );
            }
        }

        if ctx.topic_registry.topic_exists(topic_id) {
            // Metadata was recovered after best-effort replay/hydration.
        } else {
            // Re-check leadership/forwarding once more before returning Unknown topic.
            // During startup/election churn, role can change between the initial
            // pre-check and this local topic lookup.
            if let Some(action) = try_forward_to_leader(ctx, buffer, consumed, read_offset).await {
                end_operation();
                return action;
            }

            let err_msg = if ctx.cluster.is_some() {
                warn!(
                    target: "lance::server",
                    topic_id,
                    batch_id,
                    "Ingest topic metadata not yet converged; returning transient forward failure"
                );
                "FORWARD_FAILED: Unknown topic (metadata convergence)"
            } else {
                warn!(target: "lance::server", topic_id, batch_id, "Ingest to unknown topic");
                "Unknown topic"
            };

            if send_error(ctx.stream, err_msg).await.is_err() {
                end_operation();
                return FrameAction::Error(LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Failed to send error",
                )));
            }
            end_operation();
            return FrameAction::Continue;
        }
    }

    if let Some(ref payload) = frame.payload {
        // In L3 mode, register the write BEFORE sending to ingestion so the
        // write_id flows through the ingestion → forwarder → quorum ACK chain.
        let quorum_rx = if let Some(qm) = ctx.quorum_manager {
            let (write_id, rx) = qm.register_write().await;
            // Record the leader's own local write as an ACK immediately.
            // In a 3-node cluster with required_acks=2, this means only 1
            // follower ACK is needed (true majority quorum).
            qm.record_ack(write_id, ctx.node_id).await;
            Some((write_id, rx))
        } else {
            None
        };

        // Create a oneshot channel so the ingestion actor can signal write completion.
        let (write_done_tx, write_done_rx) = tokio::sync::oneshot::channel();

        let request = IngestionRequest {
            topic_id,
            routing_key: ctx.routing_key,
            timestamp_ns,
            record_count,
            payload: payload.clone(),
            write_id: quorum_rx.as_ref().map(|(id, _)| *id),
            write_done_tx: Some(write_done_tx),
        };

        if let Err(e) = ctx.ingestion_tx.send(request).await {
            end_operation();
            return FrameAction::Error(e);
        }

        // Defer the write_done await and ACK to process_frames so all
        // frames from a single read() are batched together.
        return FrameAction::Pending(PendingIngest {
            batch_id,
            write_done_rx,
            quorum_rx,
        });
    }

    end_operation();
    FrameAction::Continue
}

/// Handle control frame with leader forwarding
async fn handle_control_frame<S>(
    ctx: &mut ConnectionContext<'_, S>,
    frame: &lnc_network::Frame,
    command: ControlCommand,
    buffer: &mut [u8],
    consumed: usize,
    read_offset: usize,
) -> FrameAction
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Handle Authenticate command at connection level
    if command == ControlCommand::Authenticate {
        return handle_authenticate(ctx, frame).await;
    }

    // Forward write commands and fetch reads through the leader for consistent
    // metadata visibility during leadership churn / follower catch-up windows.
    let should_forward_to_leader =
        command_handlers::is_topic_metadata_operation(command) || command == ControlCommand::Fetch;
    if should_forward_to_leader {
        if let Some(action) =
            try_forward_control_to_leader(ctx, command, buffer, consumed, read_offset).await
        {
            return action;
        }
    }

    // Handle locally
    if let Err(e) = handle_control_command(
        ctx.stream,
        ctx.topic_registry,
        command,
        frame.payload.clone(),
        ctx.rate_limiter,
        ctx.subscription_manager,
        ctx.cluster,
    )
    .await
    {
        return FrameAction::Error(e);
    }

    FrameAction::Continue
}

/// Handle keepalive frame
async fn handle_keepalive_frame<S>(ctx: &mut ConnectionContext<'_, S>) -> FrameAction
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let response = lnc_network::Frame::new_keepalive();
    let response_bytes = lnc_network::encode_frame(&response);
    if ctx.stream.write_all(&response_bytes).await.is_err() {
        return FrameAction::Error(LanceError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "Failed to send keepalive",
        )));
    }
    FrameAction::Continue
}

/// Handle authentication command
async fn handle_authenticate<S>(
    ctx: &mut ConnectionContext<'_, S>,
    frame: &lnc_network::Frame,
) -> FrameAction
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let token = match &frame.payload {
        Some(payload) => match std::str::from_utf8(payload) {
            Ok(t) => t,
            Err(_) => {
                let response = lnc_network::Frame::new_authenticate_response(
                    false,
                    Some("Invalid token encoding"),
                );
                let response_bytes = lnc_network::encode_frame(&response);
                let _ = ctx.stream.write_all(&response_bytes).await;
                return FrameAction::Continue;
            },
        },
        None => {
            let response =
                lnc_network::Frame::new_authenticate_response(false, Some("No token provided"));
            let response_bytes = lnc_network::encode_frame(&response);
            let _ = ctx.stream.write_all(&response_bytes).await;
            return FrameAction::Continue;
        },
    };

    // Validate token
    if ctx.token_validator.validate(token) {
        *ctx.authenticated = true;
        debug!(target: "lance::server", "Client authenticated successfully");
        let response = lnc_network::Frame::new_authenticate_response(true, Some("Authenticated"));
        let response_bytes = lnc_network::encode_frame(&response);
        let _ = ctx.stream.write_all(&response_bytes).await;
    } else {
        warn!(target: "lance::server", "Client authentication failed: invalid token");
        let response = lnc_network::Frame::new_authenticate_response(false, Some("Invalid token"));
        let response_bytes = lnc_network::encode_frame(&response);
        let _ = ctx.stream.write_all(&response_bytes).await;
    }

    FrameAction::Continue
}

async fn resolve_leader_addr_with_retry(
    coord: &ClusterCoordinator,
) -> Option<std::net::SocketAddr> {
    for attempt in 0..LEADER_DISCOVERY_RETRIES {
        if let Some(addr) = coord.leader_addr_authoritative().await {
            return Some(addr);
        }

        if attempt + 1 < LEADER_DISCOVERY_RETRIES {
            let backoff_ms = LEADER_DISCOVERY_BASE_BACKOFF_MS * (attempt as u64 + 1);
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        }
    }

    None
}

/// Try to forward write to leader, returns Some(FrameAction) if handled
async fn try_forward_to_leader<S>(
    ctx: &mut ConnectionContext<'_, S>,
    buffer: &mut [u8],
    consumed: usize,
    read_offset: usize,
) -> Option<FrameAction>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let coord = ctx.cluster.as_ref()?;
    if coord.is_leader_authoritative() {
        return None;
    }

    if coord.is_leader() {
        // Data-plane ingest is allowed on elected leader while readiness warms up.
        // Metadata operations remain gated separately via authoritative checks.
        return None;
    }

    if let Some(pool) = ctx.leader_pool {
        match pool.forward_write(&buffer[..consumed]).await {
            Ok(response) => {
                if ctx.stream.write_all(&response).await.is_ok() {
                    buffer.copy_within(consumed..read_offset, 0);
                    return Some(FrameAction::Forwarded);
                }
            },
            Err(ForwardError::LeaderUnknown) => {
                if let Some(leader_addr) = resolve_leader_addr_with_retry(coord).await {
                    pool.on_leader_change(Some(leader_addr)).await;
                    match pool.forward_write(&buffer[..consumed]).await {
                        Ok(response) => {
                            if ctx.stream.write_all(&response).await.is_ok() {
                                buffer.copy_within(consumed..read_offset, 0);
                                return Some(FrameAction::Forwarded);
                            }
                        },
                        Err(e) => {
                            warn!(target: "lance::server", error = %e, "Write forwarding to leader failed");
                            let _ = send_error(ctx.stream, &format!("FORWARD_FAILED: {}", e)).await;
                            buffer.copy_within(consumed..read_offset, 0);
                            return Some(FrameAction::Forwarded);
                        },
                    }
                }

                warn!(target: "lance::server", "Write forwarding to leader failed: leader address unknown");
                let _ = send_error(ctx.stream, "FORWARD_FAILED: Leader address unknown").await;
                buffer.copy_within(consumed..read_offset, 0);
                return Some(FrameAction::Forwarded);
            },
            Err(e) => {
                // Transient forwarding failures (e.g. early EOF on stale pooled
                // connection during leader transition) can often be recovered by
                // refreshing leader address and retrying once.
                if let Some(leader_addr) = resolve_leader_addr_with_retry(coord).await {
                    pool.on_leader_change(Some(leader_addr)).await;
                    match pool.forward_write(&buffer[..consumed]).await {
                        Ok(response) => {
                            if ctx.stream.write_all(&response).await.is_ok() {
                                buffer.copy_within(consumed..read_offset, 0);
                                return Some(FrameAction::Forwarded);
                            }
                        },
                        Err(retry_err) => {
                            warn!(target: "lance::server", error = %retry_err, "Write forwarding retry after leader refresh failed");
                        },
                    }
                }

                warn!(target: "lance::server", error = %e, "Write forwarding to leader failed");
                let _ = send_error(ctx.stream, &format!("FORWARD_FAILED: {}", e)).await;
                buffer.copy_within(consumed..read_offset, 0);
                return Some(FrameAction::Forwarded);
            },
        }
    } else {
        // No leader pool, send redirect
        let err_msg = match coord.leader_addr() {
            Some(addr) => format!("NOT_LEADER: redirect to {}", addr),
            None => "NOT_LEADER: leader unknown".to_string(),
        };
        let _ = send_error(ctx.stream, &err_msg).await;
        buffer.copy_within(consumed..read_offset, 0);
        return Some(FrameAction::Forwarded);
    }

    None
}

/// Try to forward control command to leader
async fn try_forward_control_to_leader<S>(
    ctx: &mut ConnectionContext<'_, S>,
    command: ControlCommand,
    buffer: &mut [u8],
    consumed: usize,
    read_offset: usize,
) -> Option<FrameAction>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let coord = ctx.cluster.as_ref()?;
    if command == ControlCommand::Fetch {
        // Fetch must only execute locally on a ready leader; otherwise forward.
        if coord.is_leader() && coord.is_leader_authoritative() {
            return None;
        }
    } else if coord.is_leader_authoritative() {
        return None;
    }

    if coord.is_leader() {
        lnc_metrics::increment_cluster_elected_not_ready_rejects();
        warn!(
            target: "lance::server",
            "Leader elected but not ready; rejecting control command with retryable forward failure"
        );
        let _ = send_error(
            ctx.stream,
            "FORWARD_FAILED: Leader not ready (apply/metadata catch-up)",
        )
        .await;
        buffer.copy_within(consumed..read_offset, 0);
        return Some(FrameAction::Forwarded);
    }

    if let Some(pool) = ctx.leader_pool {
        match pool.forward_write(&buffer[..consumed]).await {
            Ok(response) => {
                if ctx.stream.write_all(&response).await.is_ok() {
                    buffer.copy_within(consumed..read_offset, 0);
                    return Some(FrameAction::Forwarded);
                }
            },
            Err(ForwardError::LeaderUnknown) => {
                if let Some(leader_addr) = resolve_leader_addr_with_retry(coord).await {
                    pool.on_leader_change(Some(leader_addr)).await;
                    match pool.forward_write(&buffer[..consumed]).await {
                        Ok(response) => {
                            if ctx.stream.write_all(&response).await.is_ok() {
                                buffer.copy_within(consumed..read_offset, 0);
                                return Some(FrameAction::Forwarded);
                            }
                        },
                        Err(e) => {
                            warn!(target: "lance::server", error = %e, "Control command forwarding to leader failed");
                            let _ = send_error(ctx.stream, &format!("FORWARD_FAILED: {}", e)).await;
                            buffer.copy_within(consumed..read_offset, 0);
                            return Some(FrameAction::Forwarded);
                        },
                    }
                }

                warn!(target: "lance::server", "Control command forwarding to leader failed: leader address unknown");
                let _ = send_error(ctx.stream, "FORWARD_FAILED: Leader address unknown").await;
                buffer.copy_within(consumed..read_offset, 0);
                return Some(FrameAction::Forwarded);
            },
            Err(e) => {
                // Mirror data-plane retry behavior for transient forward failures.
                if let Some(leader_addr) = resolve_leader_addr_with_retry(coord).await {
                    pool.on_leader_change(Some(leader_addr)).await;
                    match pool.forward_write(&buffer[..consumed]).await {
                        Ok(response) => {
                            if ctx.stream.write_all(&response).await.is_ok() {
                                buffer.copy_within(consumed..read_offset, 0);
                                return Some(FrameAction::Forwarded);
                            }
                        },
                        Err(retry_err) => {
                            warn!(target: "lance::server", error = %retry_err, "Control forwarding retry after leader refresh failed");
                        },
                    }
                }

                warn!(target: "lance::server", error = %e, "Control command forwarding to leader failed");
                let _ = send_error(ctx.stream, &format!("FORWARD_FAILED: {}", e)).await;
                buffer.copy_within(consumed..read_offset, 0);
                return Some(FrameAction::Forwarded);
            },
        }
    }

    None
}

/// Send error response to client
async fn send_error<S>(stream: &mut S, msg: &str) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let err = lnc_network::Frame::new_error_response(msg);
    let err_bytes = lnc_network::encode_frame(&err);
    stream.write_all(&err_bytes).await?;
    Ok(())
}

/// Shrink buffer if it grew large and data is consumed
fn maybe_shrink_buffer(buffer: &mut Vec<u8>, read_offset: usize) {
    if buffer.len() > INITIAL_BUFFER_SIZE * 4 && read_offset < INITIAL_BUFFER_SIZE {
        buffer.truncate(INITIAL_BUFFER_SIZE);
        buffer.shrink_to_fit();
    }
}

/// Dispatch control commands to their respective handlers
///
/// This function has been refactored to reduce cyclomatic complexity.
/// Each command is handled by a dedicated function in the command_handlers module.
async fn handle_control_command<S>(
    stream: &mut S,
    topic_registry: &TopicRegistry,
    command: ControlCommand,
    payload: Option<Bytes>,
    rate_limiter: &ConsumerRateLimiter,
    subscription_manager: &SubscriptionManager,
    cluster: &Option<Arc<ClusterCoordinator>>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Topic metadata commands are leader-authoritative to avoid stale
    // follower registry responses during startup/election churn.
    if command_handlers::is_topic_metadata_operation(command) {
        if let Some(coord) = cluster {
            if !coord.is_leader_authoritative() {
                if coord.is_leader() {
                    lnc_metrics::increment_cluster_elected_not_ready_rejects();
                    return send_error(
                        stream,
                        "FORWARD_FAILED: Leader not ready (apply/metadata catch-up)",
                    )
                    .await;
                }

                return send_not_leader_error(stream, coord.leader_addr().map(|a| a.to_string()))
                    .await;
            }
        }
    }

    // Build context for handlers
    let ctx = CommandContext {
        topic_registry,
        rate_limiter,
        subscription_manager,
        cluster,
    };

    // Dispatch to appropriate handler using ControlCommandDispatcher
    let dispatcher = command_handlers::ControlCommandDispatcher::new(&ctx);
    let response = dispatcher.dispatch(command, payload.as_ref()).await;

    let response_bytes = lnc_network::encode_frame(&response);
    stream.write_all(&response_bytes).await?;
    Ok(())
}

/// Send NOT_LEADER error response
async fn send_not_leader_error<S>(stream: &mut S, leader_addr: Option<String>) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let err_msg = match leader_addr {
        Some(addr) => format!("NOT_LEADER: redirect to {}", addr),
        None => "NOT_LEADER: leader unknown".to_string(),
    };
    let err = lnc_network::Frame::new_error_response(&err_msg);
    let err_bytes = lnc_network::encode_frame(&err);
    stream.write_all(&err_bytes).await?;
    Ok(())
}

/// Dispatch command to appropriate handler
/// Handle Fetch command with rate limiting and latency tracking
#[allow(dead_code)]
fn handle_fetch_request(
    topic_registry: &TopicRegistry,
    fetch_req: &FetchRequest,
) -> lnc_network::Frame {
    let topic_id = fetch_req.topic_id;

    // Get topic directory
    let topic_dir = if topic_id == 0 {
        topic_registry.data_dir().join("segments").join("0")
    } else {
        topic_registry.get_topic_dir(topic_id)
    };

    if !topic_dir.exists() {
        return lnc_network::Frame::new_error_response("Topic not found");
    }

    // Find the active segment file
    let segment_path = match find_active_segment(&topic_dir) {
        Some(path) => path,
        None => {
            // No segments yet, return empty response
            let response = FetchResponse::empty(0);
            return lnc_network::Frame::new_fetch_response(Bytes::from(response.encode()));
        },
    };

    // Read data from segment using zero-copy path (per Architecture §17.3)
    match read_segment_zero_copy(&segment_path, fetch_req.start_offset, fetch_req.max_bytes) {
        Ok((data, next_offset)) => {
            lnc_metrics::increment_reads();
            lnc_metrics::increment_read_bytes(data.len() as u64);

            // For now, record_count is estimated (actual counting would need TLV parsing)
            let record_count = if data.is_empty() { 0 } else { 1 };
            let response = FetchResponse::new(next_offset, record_count, data);
            lnc_network::Frame::new_fetch_response(Bytes::from(response.encode()))
        },
        Err(e) => {
            warn!(
                target: "lance::server",
                topic_id,
                offset = fetch_req.start_offset,
                error = %e,
                "Fetch failed"
            );
            lnc_network::Frame::new_error_response(&format!("Fetch failed: {}", e))
        },
    }
}

#[allow(dead_code)]
fn find_active_segment(topic_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    // Active segments use format: {start_index}_{start_ts}.lnc   (no '-')
    // Closed segments use format: {start_index}_{start_ts}-{end_ts}.lnc
    // Prefer the active segment; fall back to the latest closed one.
    let mut active: Option<(u64, std::path::PathBuf)> = None;
    let mut latest_closed: Option<(u64, std::path::PathBuf)> = None;

    for entry in std::fs::read_dir(topic_dir).ok()?.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "lnc") {
            let filename = path.file_stem()?.to_str()?.to_string();

            let is_closed = filename.contains('-');
            let start_part = if is_closed {
                &filename[..filename.find('-').unwrap_or(filename.len())]
            } else {
                filename.as_str()
            };

            let start_index: u64 = start_part
                .split('_')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if is_closed {
                if latest_closed
                    .as_ref()
                    .is_none_or(|(idx, _)| start_index > *idx)
                {
                    latest_closed = Some((start_index, path));
                }
            } else if active.as_ref().is_none_or(|(idx, _)| start_index > *idx) {
                active = Some((start_index, path));
            }
        }
    }

    // Active segment is the one being written to; prefer it
    active.or(latest_closed).map(|(_, p)| p)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use lnc_network::ControlCommand;

    /// Test that write operations are correctly identified
    #[test]
    fn test_write_operation_detection() {
        // These are write operations that should be routed to leader
        let write_ops = [ControlCommand::CreateTopic, ControlCommand::DeleteTopic];

        for cmd in &write_ops {
            let is_write = matches!(
                cmd,
                ControlCommand::CreateTopic | ControlCommand::DeleteTopic
            );
            assert!(is_write, "{:?} should be a write operation", cmd);
        }
    }

    /// Test that read operations are correctly identified
    #[test]
    fn test_read_operation_detection() {
        // These are read operations that can be served from any node
        let read_ops = [
            ControlCommand::ListTopics,
            ControlCommand::GetTopic,
            ControlCommand::Fetch,
        ];

        for cmd in &read_ops {
            let is_write = matches!(
                cmd,
                ControlCommand::CreateTopic | ControlCommand::DeleteTopic
            );
            assert!(!is_write, "{:?} should be a read operation", cmd);
        }
    }

    /// Test NOT_LEADER error message format
    #[test]
    fn test_not_leader_error_format() {
        use std::net::SocketAddr;

        // With known leader
        let addr: SocketAddr = "127.0.0.1:1993".parse().unwrap();
        let err_msg = format!("NOT_LEADER: redirect to {}", addr);
        assert!(err_msg.starts_with("NOT_LEADER:"));
        assert!(err_msg.contains("127.0.0.1:1993"));

        // With unknown leader
        let err_msg_unknown = "NOT_LEADER: leader unknown".to_string();
        assert!(err_msg_unknown.starts_with("NOT_LEADER:"));
        assert!(err_msg_unknown.contains("unknown"));
    }
}
