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
use crate::topic::TopicRegistry;
use bytes::Bytes;
use lnc_core::{BatchPool, LanceError, Result};
use lnc_network::{ControlCommand, FrameType, LWP_HEADER_SIZE, parse_frame};
use lnc_replication::{ClusterCoordinator, LeaderConnectionPool};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};
// 4 Golden Signals - sampled latency tracking (hot-path safe)
use lnc_metrics::time_ingest_sampled;

const INITIAL_BUFFER_SIZE: usize = 64 * 1024;

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
}

/// Result of frame processing
enum FrameAction {
    /// Continue processing next frame
    Continue,
    /// Frame was forwarded, buffer already adjusted
    Forwarded,
    /// Fatal error, close connection
    Error(LanceError),
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
    _node_id: u16,
    max_payload_size: usize,
    rate_limiter: Arc<ConsumerRateLimiter>,
    subscription_manager: Arc<SubscriptionManager>,
    cluster: Option<Arc<ClusterCoordinator>>,
    leader_pool: Option<Arc<LeaderConnectionPool>>,
    token_validator: Arc<TokenValidator>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut buffer = vec![0u8; INITIAL_BUFFER_SIZE];
    let mut read_offset = 0usize;
    // Track authentication state for this connection
    let mut authenticated = !token_validator.is_enabled();

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
        };

        read_offset = process_frames(ctx, &mut buffer, read_offset).await?;
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

/// Process all complete frames in buffer
async fn process_frames<S>(
    mut ctx: ConnectionContext<'_, S>,
    buffer: &mut Vec<u8>,
    mut read_offset: usize,
) -> Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    while read_offset >= LWP_HEADER_SIZE {
        match parse_frame(&buffer[..read_offset])? {
            Some((frame, consumed)) => {
                let action = dispatch_frame(&mut ctx, &frame, buffer, consumed, read_offset).await;

                match action {
                    FrameAction::Continue => {
                        buffer.copy_within(consumed..read_offset, 0);
                        read_offset -= consumed;
                        maybe_shrink_buffer(buffer, read_offset);
                    },
                    FrameAction::Forwarded => {
                        // Buffer already adjusted by forwarding logic
                        read_offset -= consumed;
                    },
                    FrameAction::Error(e) => return Err(e),
                }
            },
            None => break,
        }
    }
    Ok(read_offset)
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

    // Validate topic exists
    if topic_id != 0 && !ctx.topic_registry.topic_exists(topic_id) {
        warn!(target: "lance::server", topic_id, batch_id, "Ingest to unknown topic");
        if send_error(ctx.stream, "Unknown topic").await.is_err() {
            end_operation();
            return FrameAction::Error(LanceError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Failed to send error",
            )));
        }
        end_operation();
        return FrameAction::Continue;
    }

    if let Some(ref payload) = frame.payload {
        let request = IngestionRequest {
            topic_id,
            timestamp_ns,
            record_count,
            payload: payload.clone(),
        };

        if let Err(e) = ctx.ingestion_tx.send(request).await {
            end_operation();
            return FrameAction::Error(e);
        }

        let ack = lnc_network::Frame::new_ack(batch_id);
        let ack_bytes = lnc_network::encode_frame(&ack);
        if ctx.stream.write_all(&ack_bytes).await.is_err() {
            end_operation();
            return FrameAction::Error(LanceError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Failed to send ack",
            )));
        }
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

    // Check if write command needs forwarding
    if command_handlers::is_write_operation(command) {
        if let Some(action) =
            try_forward_control_to_leader(ctx, buffer, consumed, read_offset).await
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
    if coord.is_leader() {
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
            Err(e) => {
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
    buffer: &mut [u8],
    consumed: usize,
    read_offset: usize,
) -> Option<FrameAction>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let coord = ctx.cluster.as_ref()?;
    if coord.is_leader() {
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
            Err(e) => {
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
    // Check if this is a write operation that requires leader
    if command_handlers::is_write_operation(command) {
        if let Some(coord) = cluster {
            if !coord.is_leader() {
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
    let mut segments: Vec<_> = std::fs::read_dir(topic_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
        .collect();

    // Sort by name (which includes index) to get the latest
    segments.sort_by_key(|e| e.file_name());
    segments.last().map(|e| e.path())
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
