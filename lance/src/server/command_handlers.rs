//! Control command handlers - modular handlers for each ControlCommand
//!
//! This module provides individual handler functions for each control command,
//! reducing cyclomatic complexity in the main connection handler.
//!
//! ## Architecture
//!
//! The `ControlCommandDispatcher` struct encapsulates command routing logic,
//! delegating to specific handler functions based on the command type.
//! This follows the Command pattern to reduce cyclomatic complexity.

use crate::consumer::{ConsumerRateLimiter, FetchRequest};
use crate::subscription::SubscriptionManager;
use crate::topic::{RetentionConfig, TopicListResponse, TopicRegistry};
use bytes::Bytes;
use lnc_network::{ControlCommand, Frame};
use lnc_replication::{ClusterCoordinator, TopicOperation};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Context passed to all command handlers
pub struct CommandContext<'a> {
    pub topic_registry: &'a TopicRegistry,
    pub rate_limiter: &'a ConsumerRateLimiter,
    pub subscription_manager: &'a SubscriptionManager,
    pub cluster: &'a Option<Arc<ClusterCoordinator>>,
}

/// Command dispatcher that routes control commands to their handlers
///
/// This struct encapsulates the command dispatch logic, reducing cyclomatic
/// complexity in the connection handler. Each command type is routed to
/// a dedicated handler function.
///
/// # Example
///
/// ```ignore
/// let dispatcher = ControlCommandDispatcher::new(&ctx);
/// let response = dispatcher.dispatch(command, payload).await;
/// ```
pub struct ControlCommandDispatcher<'a> {
    ctx: &'a CommandContext<'a>,
}

impl<'a> ControlCommandDispatcher<'a> {
    /// Create a new dispatcher with the given context
    pub fn new(ctx: &'a CommandContext<'a>) -> Self {
        Self { ctx }
    }

    /// Dispatch a command to its appropriate handler
    ///
    /// Routes each `ControlCommand` variant to a dedicated handler function.
    /// Write operations are handled by async handlers, read operations by sync handlers.
    pub async fn dispatch(&self, command: ControlCommand, payload: Option<&Bytes>) -> Frame {
        match command {
            // Topic management commands
            ControlCommand::CreateTopic => handle_create_topic(self.ctx, payload).await,
            ControlCommand::CreateTopicWithRetention => {
                handle_create_topic_with_retention(self.ctx, payload).await
            },
            ControlCommand::ListTopics => handle_list_topics(self.ctx),
            ControlCommand::GetTopic => handle_get_topic(self.ctx, payload),
            ControlCommand::DeleteTopic => handle_delete_topic(self.ctx, payload).await,

            // Subscription commands
            ControlCommand::Subscribe => handle_subscribe(self.ctx, payload),
            ControlCommand::Unsubscribe => handle_unsubscribe(self.ctx, payload),
            ControlCommand::CommitOffset => handle_commit_offset(self.ctx, payload),

            // Retention commands
            ControlCommand::SetRetention => handle_set_retention(self.ctx, payload),

            // Cluster management commands
            ControlCommand::GetClusterStatus => handle_get_cluster_status(self.ctx).await,

            // Authentication commands - handled separately in connection.rs
            // as they need access to connection-level state
            ControlCommand::Authenticate | ControlCommand::AuthenticateResponse => {
                // These are handled at connection level, not via dispatcher
                Frame::new_error_response("Authentication handled at connection level")
            },

            // Fetch is handled separately with rate limiting
            ControlCommand::Fetch => self.handle_fetch(payload),

            // Server-only frames - invalid from clients
            ControlCommand::SubscribeAck
            | ControlCommand::CommitAck
            | ControlCommand::FetchResponse
            | ControlCommand::TopicResponse
            | ControlCommand::ClusterStatusResponse
            | ControlCommand::ErrorResponse => handle_invalid_client_command(),
        }
    }

    /// Handle Fetch command with rate limiting
    fn handle_fetch(&self, payload: Option<&Bytes>) -> Frame {
        match payload.map(|p| FetchRequest::parse(p)) {
            Some(Ok(fetch_req)) => {
                let allowed = self
                    .ctx
                    .rate_limiter
                    .try_consume(fetch_req.max_bytes as u64);
                if allowed == 0 && !self.ctx.rate_limiter.has_capacity() {
                    Frame::new_error_response("Rate limit exceeded")
                } else {
                    self.handle_fetch_request(&fetch_req)
                }
            },
            Some(Err(e)) => Frame::new_error_response(&e.to_string()),
            None => Frame::new_error_response("Fetch request payload required"),
        }
    }

    /// Execute fetch request against topic registry
    fn handle_fetch_request(&self, req: &FetchRequest) -> Frame {
        if self.ctx.topic_registry.get_topic(req.topic_id).is_none() {
            return Frame::new_error_response(&format!("Topic {} not found", req.topic_id));
        }

        match self.ctx.topic_registry.read_from_offset(
            req.topic_id,
            req.start_offset,
            req.max_bytes,
        ) {
            Ok(data) => Frame::new_fetch_response(data),
            Err(e) => Frame::new_error_response(&e.to_string()),
        }
    }
}

/// Check if a command is a write operation requiring leader
pub fn is_write_operation(command: ControlCommand) -> bool {
    matches!(
        command,
        ControlCommand::CreateTopic
            | ControlCommand::DeleteTopic
            | ControlCommand::SetRetention
            | ControlCommand::CreateTopicWithRetention
    )
}

/// Handle CreateTopic command
pub async fn handle_create_topic(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    let topic_name = payload
        .map(|p| String::from_utf8_lossy(p).to_string())
        .unwrap_or_default();

    if topic_name.is_empty() {
        return Frame::new_error_response("Topic name required");
    }

    match ctx.topic_registry.create_topic(&topic_name) {
        Ok(metadata) => {
            // Replicate topic creation to followers (if in cluster mode)
            if let Some(coord) = ctx.cluster {
                let op = TopicOperation::Create {
                    topic_id: metadata.id,
                    name: metadata.name.clone(),
                    created_at: metadata.created_at,
                };
                if let Err(e) = coord.replicate_topic_op(op).await {
                    warn!(
                        target: "lance::server",
                        topic_id = metadata.id,
                        error = %e,
                        "Failed to replicate topic creation (topic created locally)"
                    );
                }
            }

            let response_data = serde_json::json!({
                "id": metadata.id,
                "name": metadata.name,
                "created_at": metadata.created_at
            });
            Frame::new_topic_response(Bytes::from(response_data.to_string()))
        },
        Err(e) => Frame::new_error_response(&e.to_string()),
    }
}

/// Handle ListTopics command
pub fn handle_list_topics(ctx: &CommandContext<'_>) -> Frame {
    let topics = ctx.topic_registry.list_topics();
    let response = TopicListResponse::from_topics(&topics);
    Frame::new_topic_response(Bytes::from(response.to_bytes()))
}

/// Handle GetTopic command
pub fn handle_get_topic(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    let topic_id = payload
        .filter(|p| p.len() >= 4)
        .map(|p| u32::from_le_bytes([p[0], p[1], p[2], p[3]]))
        .unwrap_or(0);

    match ctx.topic_registry.get_topic_by_id(topic_id) {
        Some(metadata) => {
            let response_data = serde_json::json!({
                "id": metadata.id,
                "name": metadata.name,
                "created_at": metadata.created_at
            });
            Frame::new_topic_response(Bytes::from(response_data.to_string()))
        },
        None => Frame::new_error_response("Topic not found"),
    }
}

/// Handle DeleteTopic command
pub async fn handle_delete_topic(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    let topic_id = payload
        .filter(|p| p.len() >= 4)
        .map(|p| u32::from_le_bytes([p[0], p[1], p[2], p[3]]))
        .unwrap_or(0);

    match ctx.topic_registry.delete_topic(topic_id) {
        Ok(()) => {
            // Replicate topic deletion to followers (if in cluster mode)
            if let Some(coord) = ctx.cluster {
                let op = TopicOperation::Delete { topic_id };
                if let Err(e) = coord.replicate_topic_op(op).await {
                    warn!(
                        target: "lance::server",
                        topic_id,
                        error = %e,
                        "Failed to replicate topic deletion (topic deleted locally)"
                    );
                }
            }

            let response_data = serde_json::json!({"deleted": topic_id});
            Frame::new_topic_response(Bytes::from(response_data.to_string()))
        },
        Err(e) => Frame::new_error_response(&e.to_string()),
    }
}

/// Handle Subscribe command
pub fn handle_subscribe(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    // Payload: topic_id(4) + start_offset(8) + max_batch_bytes(4) + consumer_id(8) = 24 bytes
    match payload.filter(|p| p.len() >= 24) {
        Some(p) => {
            let topic_id = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
            let start_offset =
                u64::from_le_bytes([p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11]]);
            let max_batch_bytes = u32::from_le_bytes([p[12], p[13], p[14], p[15]]);
            let consumer_id =
                u64::from_le_bytes([p[16], p[17], p[18], p[19], p[20], p[21], p[22], p[23]]);

            // Validate topic exists (topic_id 0 is the default/legacy topic)
            if topic_id != 0 && !ctx.topic_registry.topic_exists(topic_id) {
                return Frame::new_error_response("Topic not found");
            }

            let actual_offset = ctx.subscription_manager.subscribe(
                consumer_id,
                topic_id,
                start_offset,
                max_batch_bytes,
            );

            info!(
                target: "lance::server",
                consumer_id,
                topic_id,
                start_offset = actual_offset,
                "Subscribe request processed"
            );

            Frame::new_subscribe_ack(consumer_id, actual_offset)
        },
        None => Frame::new_error_response("Invalid subscribe payload"),
    }
}

/// Handle Unsubscribe command
pub fn handle_unsubscribe(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    // Payload: topic_id(4) + consumer_id(8) = 12 bytes
    match payload.filter(|p| p.len() >= 12) {
        Some(p) => {
            let topic_id = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
            let consumer_id =
                u64::from_le_bytes([p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11]]);

            let was_subscribed = ctx.subscription_manager.unsubscribe(consumer_id, topic_id);

            info!(
                target: "lance::server",
                consumer_id,
                topic_id,
                was_subscribed,
                "Unsubscribe request processed"
            );

            // Return Ack frame for unsubscribe
            Frame::new_ack(consumer_id)
        },
        None => Frame::new_error_response("Invalid unsubscribe payload"),
    }
}

/// Handle CommitOffset command
pub fn handle_commit_offset(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    // Payload: topic_id(4) + consumer_id(8) + offset(8) = 20 bytes
    match payload.filter(|p| p.len() >= 20) {
        Some(p) => {
            let topic_id = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
            let consumer_id =
                u64::from_le_bytes([p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11]]);
            let offset =
                u64::from_le_bytes([p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19]]);

            let committed_offset =
                ctx.subscription_manager
                    .commit_offset(consumer_id, topic_id, offset);

            debug!(
                target: "lance::server",
                consumer_id,
                topic_id,
                committed_offset,
                "CommitOffset request processed"
            );

            Frame::new_commit_ack(consumer_id, committed_offset)
        },
        None => Frame::new_error_response("Invalid commit offset payload"),
    }
}

/// Handle SetRetention command
pub fn handle_set_retention(ctx: &CommandContext<'_>, payload: Option<&Bytes>) -> Frame {
    // Payload: topic_id(4) + max_age_secs(8) + max_bytes(8) = 20 bytes
    match payload.filter(|p| p.len() >= 20) {
        Some(p) => {
            let topic_id = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
            let max_age_secs =
                u64::from_le_bytes([p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11]]);
            let max_bytes =
                u64::from_le_bytes([p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19]]);

            match ctx
                .topic_registry
                .set_retention(topic_id, max_age_secs, max_bytes)
            {
                Ok(()) => {
                    info!(
                        target: "lance::server",
                        topic_id,
                        max_age_secs,
                        max_bytes,
                        "SetRetention request processed"
                    );
                    let response_data = serde_json::json!({
                        "topic_id": topic_id,
                        "max_age_secs": max_age_secs,
                        "max_bytes": max_bytes
                    });
                    Frame::new_topic_response(Bytes::from(response_data.to_string()))
                },
                Err(e) => Frame::new_error_response(&e.to_string()),
            }
        },
        None => Frame::new_error_response("Invalid set retention payload"),
    }
}

/// Handle GetClusterStatus command - returns cluster health and state information
pub async fn handle_get_cluster_status(ctx: &CommandContext<'_>) -> Frame {
    match &ctx.cluster {
        Some(cluster) => {
            let node_id = cluster.node_id();
            let is_leader = cluster.is_leader();
            let leader_id = cluster.leader_id().await;
            let current_term = cluster.current_term().await;
            let connected_peers = cluster.connected_peer_count().await;
            let peer_states = cluster.peer_states().await;

            // Calculate cluster health
            let total_nodes = peer_states.len() + 1; // peers + self
            let quorum_size = (total_nodes / 2) + 1;
            let healthy_nodes = connected_peers + 1; // connected peers + self
            let quorum_available = healthy_nodes >= quorum_size;

            // Build peer states string map
            let peer_states_map: std::collections::HashMap<u16, String> = peer_states
                .into_iter()
                .map(|(id, state)| (id, format!("{:?}", state)))
                .collect();

            let response_data = serde_json::json!({
                "node_id": node_id,
                "is_leader": is_leader,
                "leader_id": leader_id,
                "current_term": current_term,
                "node_count": total_nodes,
                "healthy_nodes": healthy_nodes,
                "quorum_available": quorum_available,
                "peer_states": peer_states_map
            });

            Frame::new_cluster_status_response(Bytes::from(response_data.to_string()))
        },
        None => {
            // Standalone mode - return minimal cluster info
            let response_data = serde_json::json!({
                "node_id": 0,
                "is_leader": true,
                "leader_id": 0,
                "current_term": 0,
                "node_count": 1,
                "healthy_nodes": 1,
                "quorum_available": true,
                "peer_states": {},
                "mode": "standalone"
            });

            Frame::new_cluster_status_response(Bytes::from(response_data.to_string()))
        },
    }
}

/// Handle CreateTopicWithRetention command
pub async fn handle_create_topic_with_retention(
    ctx: &CommandContext<'_>,
    payload: Option<&Bytes>,
) -> Frame {
    // Payload: name_len(2) + name(var) + max_age_secs(8) + max_bytes(8)
    match payload.filter(|p| p.len() >= 18) {
        Some(p) => {
            let name_len = u16::from_le_bytes([p[0], p[1]]) as usize;
            if p.len() < 2 + name_len + 16 {
                return Frame::new_error_response("Invalid payload length");
            }

            let topic_name = String::from_utf8_lossy(&p[2..2 + name_len]).to_string();
            let offset = 2 + name_len;
            let max_age_secs = u64::from_le_bytes([
                p[offset],
                p[offset + 1],
                p[offset + 2],
                p[offset + 3],
                p[offset + 4],
                p[offset + 5],
                p[offset + 6],
                p[offset + 7],
            ]);
            let max_bytes = u64::from_le_bytes([
                p[offset + 8],
                p[offset + 9],
                p[offset + 10],
                p[offset + 11],
                p[offset + 12],
                p[offset + 13],
                p[offset + 14],
                p[offset + 15],
            ]);

            let retention = Some(RetentionConfig {
                max_age_secs: Some(max_age_secs),
                max_bytes: Some(max_bytes),
            });

            match ctx
                .topic_registry
                .create_topic_with_retention(&topic_name, retention)
            {
                Ok(metadata) => {
                    // Replicate to followers if in cluster mode
                    if let Some(coord) = ctx.cluster {
                        let op = TopicOperation::Create {
                            topic_id: metadata.id,
                            name: metadata.name.clone(),
                            created_at: metadata.created_at,
                        };
                        if let Err(e) = coord.replicate_topic_op(op).await {
                            warn!(
                                target: "lance::server",
                                topic_id = metadata.id,
                                error = %e,
                                "Failed to replicate topic creation"
                            );
                        }
                    }

                    let response_data = serde_json::json!({
                        "id": metadata.id,
                        "name": metadata.name,
                        "created_at": metadata.created_at,
                        "max_age_secs": max_age_secs,
                        "max_bytes": max_bytes
                    });
                    Frame::new_topic_response(Bytes::from(response_data.to_string()))
                },
                Err(e) => Frame::new_error_response(&e.to_string()),
            }
        },
        None => Frame::new_error_response("Invalid create topic payload"),
    }
}

/// Handle invalid client commands (server-only frames)
pub fn handle_invalid_client_command() -> Frame {
    Frame::new_error_response("Invalid client command")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_write_operation() {
        assert!(is_write_operation(ControlCommand::CreateTopic));
        assert!(is_write_operation(ControlCommand::DeleteTopic));
        assert!(is_write_operation(ControlCommand::SetRetention));
        assert!(is_write_operation(ControlCommand::CreateTopicWithRetention));
        assert!(!is_write_operation(ControlCommand::ListTopics));
        assert!(!is_write_operation(ControlCommand::GetTopic));
        assert!(!is_write_operation(ControlCommand::Fetch));
        assert!(!is_write_operation(ControlCommand::Subscribe));
    }

    #[test]
    fn test_parse_set_retention_payload_valid() {
        let mut payload = Vec::with_capacity(20);
        payload.extend_from_slice(&1u32.to_le_bytes()); // topic_id
        payload.extend_from_slice(&86400u64.to_le_bytes()); // max_age_secs
        payload.extend_from_slice(&1073741824u64.to_le_bytes()); // max_bytes (1GB)
        let bytes = Bytes::from(payload);

        assert_eq!(bytes.len(), 20);
        let topic_id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let max_age_secs = u64::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
        ]);
        let max_bytes = u64::from_le_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ]);

        assert_eq!(topic_id, 1);
        assert_eq!(max_age_secs, 86400);
        assert_eq!(max_bytes, 1073741824);
    }

    #[test]
    fn test_parse_set_retention_payload_too_short() {
        let payload = Bytes::from(vec![0u8; 10]); // Too short (need 20 bytes)
        assert!(payload.len() < 20);
    }

    #[test]
    fn test_parse_create_topic_with_retention_payload() {
        let topic_name = "test-topic";
        let name_bytes = topic_name.as_bytes();
        let mut payload = Vec::with_capacity(2 + name_bytes.len() + 16);
        payload.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(name_bytes);
        payload.extend_from_slice(&86400u64.to_le_bytes()); // max_age_secs
        payload.extend_from_slice(&0u64.to_le_bytes()); // max_bytes (unlimited)
        let bytes = Bytes::from(payload);

        let name_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        assert_eq!(name_len, 10);

        let name = std::str::from_utf8(&bytes[2..2 + name_len]).unwrap();
        assert_eq!(name, "test-topic");

        let offset = 2 + name_len;
        let max_age_secs = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        assert_eq!(max_age_secs, 86400);

        let max_bytes = u64::from_le_bytes([
            bytes[offset + 8],
            bytes[offset + 9],
            bytes[offset + 10],
            bytes[offset + 11],
            bytes[offset + 12],
            bytes[offset + 13],
            bytes[offset + 14],
            bytes[offset + 15],
        ]);
        assert_eq!(max_bytes, 0);
    }

    #[test]
    fn test_handle_invalid_client_command() {
        let frame = handle_invalid_client_command();
        assert!(matches!(
            frame.frame_type,
            lnc_network::FrameType::Control(ControlCommand::ErrorResponse)
        ));
    }
}
