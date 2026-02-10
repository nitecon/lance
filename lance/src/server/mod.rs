//! Server module - TCP listener and orchestration
//!
//! This module contains the main server run loop and coordinates
//! between connection handling, ingestion, and recovery.

pub mod circuit_breaker;
mod command_handlers;
mod connection;
mod ingestion;
mod multi_actor;
mod recovery;
pub mod resync;
pub mod retention;
mod writer;

pub use connection::handle_connection;
pub use ingestion::{DataReplicationRequest, IngestionRequest, run_ingestion_actor};
pub use multi_actor::{IngestionSender, MultiActorIngestion};
pub use recovery::perform_startup_recovery;
pub use retention::{RetentionServiceConfig, run_retention_service};

use crate::auth::TokenValidator;
use crate::config::Config;
use crate::consumer::ConsumerRateLimiter;
use crate::health::HealthState;
use crate::subscription::SubscriptionManager;
use crate::topic::TopicRegistry;
use lnc_core::{BatchPool, NumaTopology, Result};
#[cfg(feature = "tls")]
use lnc_network::tls::{TlsAcceptor, TlsConfig};
use lnc_replication::{
    AsyncQuorumManager, ClusterCoordinator, ClusterEvent, ForwardConfig, LeaderConnectionPool,
    QuorumConfig, ReplicationActor, TopicOperation, create_leader_pool, create_replication_channel,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};

/// Main server entry point
pub async fn run(
    config: Config,
    mut shutdown_rx: broadcast::Receiver<()>,
    health_state: Arc<HealthState>,
) -> Result<()> {
    info!(
        target: "lance::server",
        node_id = config.node_id,
        listen = %config.listen_addr,
        mode = %config.replication_mode,
        "Server starting"
    );

    // Log NUMA topology at startup (per Architecture §14 and §18.7)
    let numa_topology = NumaTopology::get();
    info!(
        target: "lance::server",
        numa_nodes = numa_topology.node_count,
        cpu_count = numa_topology.cpu_count,
        cpus_per_node = ?numa_topology.cpus_per_node,
        "NUMA topology detected"
    );

    // Log thread pinning configuration if enabled
    if !config.ingestion.pin_cores.is_empty() {
        info!(
            target: "lance::server",
            pin_cores = ?config.ingestion.pin_cores,
            "Thread pinning enabled for ingestion actors"
        );
    }

    perform_startup_recovery(&config)?;

    let topic_registry = Arc::new(TopicRegistry::new(config.data_dir.clone())?);

    let batch_pool = Arc::new(BatchPool::new(
        config.ingestion.batch_pool_size,
        config.ingestion.batch_capacity,
    )?);

    // Create data replication channel (used to send post-write data to the
    // replication task which broadcasts to followers via ClusterCoordinator).
    // Only allocated when running in cluster mode; standalone gets None.
    // The receiver is consumed below after the cluster coordinator is created.
    let (data_repl_tx, data_repl_rx): (
        Option<tokio::sync::mpsc::Sender<DataReplicationRequest>>,
        Option<tokio::sync::mpsc::Receiver<DataReplicationRequest>>,
    ) = if config.replication_mode().is_replicated() {
        let (tx, rx) = tokio::sync::mpsc::channel::<DataReplicationRequest>(8192);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Create ingestion system - multi-actor when actor_count > 1, single-actor otherwise
    let (ingestion_sender, ingestion_rx, multi_actor_system) = if config.ingestion.actor_count > 1 {
        // Multi-actor mode with crossbeam ArrayQueue (per Architecture §5.2)
        let multi = MultiActorIngestion::new(
            config.clone(),
            Arc::clone(&topic_registry),
            config.ingestion.channel_capacity,
            data_repl_tx.clone(),
        )?;
        let sender = IngestionSender::Multi(multi.sender());
        (sender, None, Some(multi))
    } else {
        // Single-actor mode with flume channel
        let (tx, rx) = flume::bounded::<IngestionRequest>(config.ingestion.channel_capacity);
        let sender = IngestionSender::Single(tx);
        (sender, Some(rx), None)
    };

    let (_replication_tx, replication_rx) = create_replication_channel(8192);

    let mut replication_actor = ReplicationActor::new(
        config.replication_mode(),
        config.node_id,
        config.peer_ids(),
        replication_rx,
    );

    let replication_handle = tokio::spawn(async move {
        replication_actor.run().await;
    });

    // Start cluster coordinator for multi-node replication
    let cluster_coordinator = if config.replication_mode().is_replicated() {
        let cluster_config = config.cluster_config_async().await;
        let coordinator = Arc::new(ClusterCoordinator::new(cluster_config));

        // Start replication listener first (so peers can connect to us)
        if let Err(e) = coordinator.start_listener().await {
            warn!(
                target: "lance::server",
                error = %e,
                "Failed to start replication listener"
            );
        }

        // Initialize cluster - discover and connect to peers
        if let Err(e) = coordinator.initialize().await {
            warn!(
                target: "lance::server",
                error = %e,
                "Failed to initialize cluster coordinator, continuing in standalone mode"
            );
        }

        // Start cluster coordination loop
        let coord_clone = Arc::clone(&coordinator);
        let shutdown_for_cluster = shutdown_rx.resubscribe();
        tokio::spawn(async move {
            coord_clone.run(shutdown_for_cluster).await;
        });

        // Start cluster event handler for topic replication, data replication, and leader changes
        let mut event_rx = coordinator.subscribe();
        let event_registry = Arc::clone(&topic_registry);
        let event_config = config.clone();
        let event_coord = Arc::clone(&coordinator);
        let event_health = Arc::clone(&health_state);
        let event_node_id = config.node_id;
        tokio::spawn(async move {
            // Track last known leader for change detection
            let mut last_leader = event_coord.leader_addr();
            // Topic writers for follower data replication (only used by followers)
            let mut follower_writers: std::collections::HashMap<u32, writer::TopicWriter> =
                std::collections::HashMap::new();
            // Channel for forwarding resync events to the follower resync state machine.
            // Created when BecameFollower fires, dropped when resync completes.
            let mut resync_tx: Option<tokio::sync::mpsc::Sender<resync::ResyncEvent>> = None;

            loop {
                // Check for leader changes periodically
                let current_leader = event_coord.leader_addr();
                if current_leader != last_leader {
                    debug!(
                        target: "lance::server",
                        old_leader = ?last_leader,
                        new_leader = ?current_leader,
                        "Leader change detected"
                    );
                    last_leader = current_leader;
                }

                match event_rx.recv().await {
                    Ok(ClusterEvent::TopicOperation(op)) => match op {
                        TopicOperation::Create {
                            topic_id,
                            name,
                            created_at,
                        } => {
                            info!(
                                target: "lance::server",
                                topic_id,
                                topic_name = %name,
                                "Applying replicated topic creation"
                            );
                            if let Err(e) =
                                event_registry.create_topic_with_id(topic_id, &name, created_at)
                            {
                                warn!(
                                    target: "lance::server",
                                    topic_id,
                                    error = %e,
                                    "Failed to apply replicated topic creation"
                                );
                            }
                        },
                        TopicOperation::Delete { topic_id } => {
                            info!(
                                target: "lance::server",
                                topic_id,
                                "Applying replicated topic deletion"
                            );
                            if let Err(e) = event_registry.delete_topic(topic_id) {
                                warn!(
                                    target: "lance::server",
                                    topic_id,
                                    error = %e,
                                    "Failed to apply replicated topic deletion"
                                );
                            }
                            // Remove any cached writer for deleted topic
                            follower_writers.remove(&topic_id);
                        },
                    },
                    Ok(ClusterEvent::DataReceivedEnriched(entry)) => {
                        // Per RaftQuorumWartime.md §4.2:
                        // During resync, skip data writes — the resync state machine
                        // is fetching segments directly from the leader. Incoming data
                        // entries are acknowledged (at the Raft level) but not written.
                        // Check if resync is still active (receiver not dropped).
                        if let Some(ref tx) = resync_tx {
                            if tx.is_closed() {
                                // Resync task completed — clear the channel
                                resync_tx = None;
                            } else {
                                debug!(
                                    target: "lance::server",
                                    topic_id = entry.topic_id,
                                    segment = %entry.segment_name,
                                    "Skipping data write during resync (catching up)"
                                );
                                continue;
                            }
                        }
                        // Write replicated data using enriched format (L3 byte-identical segments)
                        let topic_id = entry.topic_id;
                        if let Err(e) = ingestion::write_replicated_data_enriched(
                            &event_config,
                            &event_registry,
                            &mut follower_writers,
                            &entry,
                        ) {
                            // Log but do NOT remove the writer on offset mismatch.
                            // Removing the writer causes a tight create/truncate/fail
                            // loop that leads to OOM. Instead, keep the writer alive
                            // so subsequent writes at the correct offset can succeed,
                            // or wait for a NEW_SEGMENT flag to start fresh.
                            warn!(
                                target: "lance::server",
                                topic_id,
                                segment = %entry.segment_name,
                                write_offset = entry.write_offset,
                                error = %e,
                                "Failed to write enriched replicated data (writer retained)"
                            );
                        }
                    },
                    Ok(ClusterEvent::DataReceived { topic_id, payload }) => {
                        // Skip legacy data writes during resync (same guard as enriched path)
                        if let Some(ref tx) = resync_tx {
                            if tx.is_closed() {
                                resync_tx = None;
                            } else {
                                debug!(
                                    target: "lance::server",
                                    topic_id,
                                    "Skipping legacy data write during resync (catching up)"
                                );
                                continue;
                            }
                        }
                        // Legacy fallback: write replicated data without segment metadata
                        if let Err(e) = ingestion::write_replicated_data(
                            &event_config,
                            &event_registry,
                            &mut follower_writers,
                            topic_id,
                            &payload,
                        ) {
                            warn!(
                                target: "lance::server",
                                topic_id,
                                payload_len = payload.len(),
                                error = %e,
                                "Failed to write replicated data (legacy)"
                            );
                            follower_writers.remove(&topic_id);
                        }
                    },
                    Ok(ClusterEvent::BecameLeader { term, .. }) => {
                        // Close all follower writer segments so the ingestion actor
                        // doesn't collide with stale active segments when it creates
                        // new writers for incoming leader writes.
                        info!(
                            target: "lance::server",
                            term,
                            writers = follower_writers.len(),
                            "BecameLeader — closing follower writer segments"
                        );
                        let end_ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_nanos() as u64)
                            .unwrap_or(0);
                        for (topic_id, mut tw) in follower_writers.drain() {
                            if let Err(e) = tw.writer.fsync() {
                                warn!(
                                    target: "lance::server",
                                    topic_id,
                                    error = %e,
                                    "Failed to fsync follower segment on leader transition"
                                );
                                continue;
                            }
                            match tw.writer.close(end_ts) {
                                Ok(closed_path) => {
                                    let _ = tw.index_builder.write_indexes(&closed_path);
                                    debug!(
                                        target: "lance::server",
                                        topic_id,
                                        segment = %closed_path.display(),
                                        "Closed follower segment on leader transition"
                                    );
                                },
                                Err(e) => {
                                    warn!(
                                        target: "lance::server",
                                        topic_id,
                                        error = %e,
                                        "Failed to close follower segment on leader transition"
                                    );
                                },
                            }
                        }
                    },
                    Ok(ClusterEvent::BecameFollower { leader_id, term }) => {
                        info!(
                            target: "lance::server",
                            term,
                            leader_id,
                            "BecameFollower — now following node {leader_id}"
                        );
                        // Per RaftQuorumWartime.md §5.2:
                        // Spawn the follower resync state machine to sync
                        // local segments with the leader before accepting traffic.
                        let (tx, rx) = tokio::sync::mpsc::channel::<resync::ResyncEvent>(64);
                        resync_tx = Some(tx);
                        let resync_coord = Arc::clone(&event_coord);
                        let resync_registry = Arc::clone(&event_registry);
                        let resync_health = Arc::clone(&event_health);
                        tokio::spawn(async move {
                            resync::run_follower_resync(
                                event_node_id,
                                resync_coord,
                                resync_registry,
                                resync_health,
                                rx,
                            )
                            .await;
                        });
                    },
                    Ok(ClusterEvent::LostLeadership { term }) => {
                        warn!(
                            target: "lance::server",
                            term,
                            "LostLeadership — no longer leader"
                        );
                    },
                    Ok(ClusterEvent::ResyncBeginReceived(req)) => {
                        // Leader: enter wartime for this follower
                        if event_coord.is_leader() {
                            if event_coord.enter_wartime(req.node_id) {
                                info!(
                                    target: "lance::server",
                                    follower_id = req.node_id,
                                    term = req.term,
                                    "Entered wartime for resyncing follower"
                                );
                            } else {
                                warn!(
                                    target: "lance::server",
                                    follower_id = req.node_id,
                                    "Wartime max terms exceeded for follower"
                                );
                            }
                        }
                    },
                    Ok(ClusterEvent::ResyncCompleteReceived(req)) => {
                        // Leader: exit wartime for this follower
                        if event_coord.is_leader() {
                            event_coord.exit_wartime(req.node_id);
                            info!(
                                target: "lance::server",
                                follower_id = req.node_id,
                                term = req.term,
                                "Follower resync complete, exited wartime"
                            );
                        }
                    },
                    Ok(ClusterEvent::SegmentManifestRequested { node_id, .. }) => {
                        // Leader: build and send manifest to requesting follower
                        if event_coord.is_leader() {
                            match resync::build_segment_manifest(&event_registry) {
                                Ok(manifest) => {
                                    let msg = lnc_replication::ReplicationMessage::SegmentManifestResponse(manifest);
                                    if let Err(e) = event_coord.send_to_peer(node_id, &msg).await {
                                        warn!(
                                            target: "lance::server",
                                            follower_id = node_id,
                                            error = %e,
                                            "Failed to send segment manifest to follower"
                                        );
                                    }
                                },
                                Err(e) => {
                                    warn!(
                                        target: "lance::server",
                                        follower_id = node_id,
                                        error = %e,
                                        "Failed to build segment manifest"
                                    );
                                },
                            }
                        }
                    },
                    Ok(ClusterEvent::SegmentFetchRequested(request)) => {
                        // Leader: read chunk and send to requesting follower
                        if event_coord.is_leader() {
                            let follower_id = request.node_id;
                            match resync::read_segment_chunk(
                                &event_registry,
                                request.topic_id,
                                &request.segment_name,
                                request.offset,
                                request.max_chunk_size,
                            ) {
                                Ok((data, total_size, done)) => {
                                    let resp = lnc_replication::SegmentFetchResponse {
                                        topic_id: request.topic_id,
                                        segment_name: request.segment_name,
                                        offset: request.offset,
                                        total_size,
                                        done,
                                        data,
                                    };
                                    let msg =
                                        lnc_replication::ReplicationMessage::SegmentFetchResponse(
                                            resp,
                                        );
                                    if let Err(e) =
                                        event_coord.send_to_peer(follower_id, &msg).await
                                    {
                                        warn!(
                                            target: "lance::server",
                                            follower_id,
                                            error = %e,
                                            "Failed to send segment chunk to follower"
                                        );
                                    }
                                },
                                Err(e) => {
                                    warn!(
                                        target: "lance::server",
                                        follower_id,
                                        topic_id = request.topic_id,
                                        segment = %request.segment_name,
                                        error = %e,
                                        "Failed to read segment chunk"
                                    );
                                },
                            }
                        }
                    },
                    Ok(ClusterEvent::SegmentManifestReceived(manifest)) => {
                        // Follower: forward manifest to resync state machine
                        if let Some(ref tx) = resync_tx {
                            if tx
                                .send(resync::ResyncEvent::ManifestReceived(manifest))
                                .await
                                .is_err()
                            {
                                debug!(
                                    target: "lance::server",
                                    "Resync channel closed — manifest dropped"
                                );
                                resync_tx = None;
                            }
                        } else {
                            debug!(
                                target: "lance::server",
                                "Received segment manifest but no resync active"
                            );
                        }
                    },
                    Ok(ClusterEvent::SegmentFetchReceived(resp)) => {
                        // Follower: forward fetch response to resync state machine
                        if let Some(ref tx) = resync_tx {
                            if tx
                                .send(resync::ResyncEvent::FetchReceived(resp))
                                .await
                                .is_err()
                            {
                                debug!(
                                    target: "lance::server",
                                    "Resync channel closed — fetch response dropped"
                                );
                                resync_tx = None;
                            }
                        } else {
                            debug!(
                                target: "lance::server",
                                "Received segment chunk but no resync active"
                            );
                        }
                    },
                    Ok(_) => {}, // PeerJoined, PeerLeft, etc.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            target: "lance::server",
                            skipped = n,
                            "Cluster event receiver lagged"
                        );
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!(target: "lance::server", "Cluster event channel closed");
                        break;
                    },
                }
            }
        });

        info!(
            target: "lance::server",
            node_id = config.node_id,
            replication_addr = %config.replication_addr,
            peers = ?config.peers,
            "Cluster coordinator started"
        );

        Some(coordinator)
    } else {
        info!(
            target: "lance::server",
            "Running in standalone mode (L1), cluster coordinator disabled"
        );
        None
    };

    let cluster = cluster_coordinator;

    // Create quorum manager for L3 mode (leader waits for M/2+1 ACKs before ACKing client)
    let quorum_manager: Option<Arc<AsyncQuorumManager>> =
        if config.replication_mode().requires_quorum() {
            let peer_count = config.peer_ids().len();
            // total_nodes = peers + self
            let total_nodes = peer_count + 1;
            let qm_config = QuorumConfig::new(total_nodes)
                .with_timeout(config.replication_quorum_timeout_ms.unwrap_or(100));
            info!(
                target: "lance::server",
                total_nodes,
                required_acks = qm_config.required_acks,
                timeout_ms = qm_config.timeout_ms,
                "L3 quorum manager initialized"
            );
            Some(Arc::new(AsyncQuorumManager::new(qm_config)))
        } else {
            None
        };

    // Start data replication forwarder task (leader sends data to followers).
    // Placed after quorum_manager creation so the forwarder can record ACKs.
    let forwarder_handle = if let (Some(coord), Some(mut repl_rx)) = (&cluster, data_repl_rx) {
        let repl_coord = Arc::clone(coord);
        let repl_qm = quorum_manager.clone();
        Some(tokio::spawn(async move {
            info!(
                target: "lance::server",
                "Data replication forwarder task started (enriched)"
            );
            while let Some(req) = repl_rx.recv().await {
                // Build enriched replication entry from the write metadata
                let mut flags = lnc_replication::ReplicationFlags::empty();
                if req.is_new_segment {
                    flags = flags.with_new_segment();
                }
                if req.rotated_after {
                    flags = flags.with_rotate_after();
                }

                let write_id = req.write_id;
                let topic_id = req.topic_id;
                let entry = lnc_replication::DataReplicationEntry {
                    topic_id,
                    global_offset: req.global_offset,
                    segment_name: req.segment_name,
                    write_offset: req.write_offset,
                    flags,
                    payload_crc: lnc_core::crc32c(&req.payload),
                    payload: req.payload,
                };

                match repl_coord.replicate_data_enriched(entry).await {
                    Ok(successful_peers) => {
                        // Record quorum ACKs for each peer that confirmed
                        if let (Some(wid), Some(qm)) = (write_id, &repl_qm) {
                            for peer_id in &successful_peers {
                                qm.record_ack(wid, *peer_id).await;
                            }
                        }
                    },
                    Err(e) => {
                        debug!(
                            target: "lance::server",
                            topic_id,
                            error = %e,
                            "Data replication broadcast error (non-fatal)"
                        );
                    },
                }
            }
            info!(
                target: "lance::server",
                "Data replication forwarder task stopped"
            );
        }))
    } else {
        None
    };

    // Create leader connection pool for write forwarding (only if in cluster mode)
    let leader_pool: Option<Arc<LeaderConnectionPool>> = if let Some(ref coord) = cluster {
        let pool = create_leader_pool(ForwardConfig::default());

        // Initialize pool with current leader address if known
        if let Some(leader_addr) = coord.leader_addr() {
            pool.on_leader_change(Some(leader_addr)).await;
        }

        // Start leader change watcher task with periodic DNS re-resolution
        let pool_for_watcher = Arc::clone(&pool);
        let coord_for_watcher = Arc::clone(coord);
        tokio::spawn(async move {
            let mut last_leader = coord_for_watcher.leader_addr();
            let mut dns_refresh_counter: u64 = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                // Re-resolve peer DNS every 30s (60 ticks × 500ms) to handle pod IP changes
                dns_refresh_counter += 1;
                if dns_refresh_counter % 60 == 0 {
                    coord_for_watcher.refresh_peer_addresses().await;
                }

                let current_leader = coord_for_watcher.leader_addr();
                if current_leader != last_leader {
                    info!(
                        target: "lance::server",
                        old_leader = ?last_leader,
                        new_leader = ?current_leader,
                        "Leader changed, updating forward pool"
                    );
                    pool_for_watcher.on_leader_change(current_leader).await;
                    last_leader = current_leader;
                }
            }
        });

        Some(pool)
    } else {
        None
    };

    // Start single-actor ingestion only if not using multi-actor mode
    let ingestion_handle = if let Some(rx) = ingestion_rx {
        let ingestion_config = config.clone();
        let ingestion_pool = Arc::clone(&batch_pool);
        let ingestion_registry = Arc::clone(&topic_registry);
        let ingestion_repl_tx = data_repl_tx.clone();
        Some(tokio::spawn(async move {
            run_ingestion_actor(
                ingestion_config,
                rx,
                ingestion_pool,
                ingestion_registry,
                ingestion_repl_tx,
            )
            .await
        }))
    } else {
        info!(
            target: "lance::server",
            actor_count = config.ingestion.actor_count,
            "Using multi-actor ingestion"
        );
        None
    };

    // Start retention service for background segment cleanup
    let retention_shutdown_rx = shutdown_rx.resubscribe();
    let retention_registry = Arc::clone(&topic_registry);
    let retention_config = RetentionServiceConfig {
        cleanup_interval: std::time::Duration::from_secs(config.retention_cleanup_interval_secs),
        dry_run: false,
    };
    let retention_handle = tokio::spawn(async move {
        if let Err(e) =
            run_retention_service(retention_config, retention_registry, retention_shutdown_rx).await
        {
            warn!(target: "lance::server", error = %e, "Retention service error");
        }
    });

    let listener = TcpListener::bind(&config.listen_addr).await?;

    // Create rate limiter for consumer reads (disabled by default for max throughput)
    let rate_limiter = Arc::new(match config.consumer_rate_limit_bytes_per_sec {
        Some(limit) => {
            info!(
                target: "lance::server",
                bytes_per_sec = limit,
                "Consumer rate limiter enabled"
            );
            ConsumerRateLimiter::new(limit)
        },
        None => {
            info!(
                target: "lance::server",
                "Consumer rate limiter disabled (unlimited throughput)"
            );
            ConsumerRateLimiter::disabled()
        },
    });

    // Create subscription manager for streaming consumers
    let subscription_manager = Arc::new(SubscriptionManager::new());

    // Create token validator for authentication (per Architecture §18.6)
    let token_validator = match TokenValidator::from_settings(&config.auth) {
        Ok(validator) => {
            if validator.is_enabled() {
                info!(
                    target: "lance::server",
                    token_count = validator.token_count(),
                    write_only = validator.is_write_only(),
                    "Authentication enabled"
                );
            }
            Arc::new(validator)
        },
        Err(e) => {
            warn!(
                target: "lance::server",
                error = %e,
                "Failed to initialize token validator, auth disabled"
            );
            Arc::new(TokenValidator::default())
        },
    };

    // Initialize TLS acceptor if configured
    #[cfg(feature = "tls")]
    let tls_acceptor: Option<Arc<TlsAcceptor>> = if config.tls.enabled {
        match (config.tls.cert_path.as_ref(), config.tls.key_path.as_ref()) {
            (Some(cert), Some(key)) => {
                let tls_config = TlsConfig::server(cert, key);
                match TlsAcceptor::new(tls_config) {
                    Ok(acceptor) => {
                        info!(
                            target: "lance::server",
                            cert = %cert.display(),
                            "TLS enabled for client connections"
                        );
                        Some(Arc::new(acceptor))
                    },
                    Err(e) => {
                        error!(
                            target: "lance::server",
                            error = %e,
                            "Failed to initialize TLS acceptor, continuing without TLS"
                        );
                        None
                    },
                }
            },
            _ => {
                warn!(
                    target: "lance::server",
                    "TLS enabled but cert_path or key_path not configured"
                );
                None
            },
        }
    } else {
        None
    };

    #[cfg(not(feature = "tls"))]
    let tls_acceptor: Option<()> = None;
    let _ = &tls_acceptor; // Suppress unused warning when TLS not enabled

    info!(
        target: "lance::server",
        addr = %config.listen_addr,
        tls = config.tls.enabled,
        "Listening for connections"
    );

    // Mark startup complete and ready
    health_state.set_startup_complete();
    health_state.set_ready(true);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, addr)) => {
                        // Disable Nagle's algorithm — ACKs and small control
                        // frames must go out immediately, not be batched.
                        let _ = stream.set_nodelay(true);
                        lnc_metrics::increment_connections();
                        trace!(target: "lance::server", peer = %addr, "Connection accepted");

                        let tx = ingestion_sender.clone();
                        let pool = Arc::clone(&batch_pool);
                        let registry = Arc::clone(&topic_registry);
                        let limiter = Arc::clone(&rate_limiter);
                        let subs = Arc::clone(&subscription_manager);
                        let cluster_state = cluster.clone();
                        let fwd_pool = leader_pool.clone();
                        let validator = Arc::clone(&token_validator);
                        let qm = quorum_manager.clone();
                        let node_id = config.node_id;
                        let max_payload = config.ingestion.max_payload_size;

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, tx, pool, registry, node_id, max_payload, limiter, subs, cluster_state, fwd_pool, validator, qm).await {
                                debug!(target: "lance::server", peer = %addr, error = %e, "Connection closed");
                            }
                            lnc_metrics::decrement_connections();
                        });
                    }
                    Err(e) => {
                        error!(target: "lance::server", error = %e, "Accept failed");
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!(target: "lance::server", "Shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    // Mark as not ready during shutdown
    health_state.set_ready(false);
    health_state.set_not_alive();

    // Drop sender to signal shutdown to ingestion actors
    drop(ingestion_sender);

    info!(target: "lance::server", "Waiting for actors to drain");

    // Wait for single-actor ingestion if active
    if let Some(handle) = ingestion_handle {
        let _ = handle.await;
    }

    // Shutdown multi-actor system if active
    if let Some(multi) = multi_actor_system {
        multi.shutdown();
    }

    // Drop replication channel sender so the forwarder sees channel closed.
    drop(data_repl_tx);
    // Abort the forwarder — we cannot drain it because send_append_entries()
    // blocks waiting for peer responses, and peers may be down during rolling
    // restart. Draining would delay shutdown and cause K8s to force-kill the pod.
    if let Some(fwd) = forwarder_handle {
        fwd.abort();
    }

    replication_handle.abort();
    retention_handle.abort();

    info!(target: "lance::server", "Server shutdown complete");

    Ok(())
}
