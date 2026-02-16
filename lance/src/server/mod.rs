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
pub mod retention;
mod writer;

pub use connection::handle_connection;
pub use ingestion::{DataReplicationRequest, IngestionRequest};
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
    QuorumConfig, ReplicationActor, ResyncActor, ResyncConfig, ResyncServer, TopicOperation,
    create_leader_pool, create_replication_channel,
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

    if let Some(core_id) = config.coordinator_pin_core {
        info!(
            target: "lance::server",
            core_id,
            "Thread pinning enabled for coordinator runtime"
        );
    }

    if let Some(core_id) = config.forwarder_pin_core {
        info!(
            target: "lance::server",
            core_id,
            "Thread pinning enabled for forwarder runtime"
        );
    }

    // BLOCKING RECOVERY (Required for data safety)
    // We cannot open TopicRegistry until we confirm segments are clean.
    // Race condition: Recovery truncates files while Registry reads them = corruption.
    perform_startup_recovery(&config)?;

    // Mark server as ready after recovery completes
    health_state.set_ready(true);
    info!(
        target: "lance::server",
        "Server marked ready after recovery"
    );

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
        let (tx, rx) = tokio::sync::mpsc::channel::<DataReplicationRequest>(
            config.data_replication_channel_capacity,
        );
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // ALWAYS use MultiActorIngestion (threaded) to prevent blocking fsync in async runtime.
    // Even with actor_count=1, we use the multi-actor path because it runs on dedicated
    // OS threads, ensuring fsync() never blocks the Tokio reactor.
    let actor_count = config.ingestion.actor_count.max(1);

    info!(
        target: "lance::server",
        actor_count,
        "Initializing threaded ingestion actors"
    );

    let multi = MultiActorIngestion::new(
        config.clone(),
        Arc::clone(&topic_registry),
        config.ingestion.channel_capacity,
        data_repl_tx.clone(),
    )?;

    let ingestion_sender = IngestionSender::Multi(multi.sender());
    let _ingestion_rx: Option<flume::Receiver<IngestionRequest>> = None;
    let multi_actor_system = Some(multi);

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

    // Start cluster coordinator for multi-node replication.
    // Step 2: run coordinator control-plane on a dedicated OS thread + Tokio runtime.
    let (cluster_coordinator, cluster_runtime_handle) = if config.replication_mode().is_replicated()
    {
        let cluster_config = config.cluster_config_async().await;

        // Use persistent log storage for durable Raft state (term, voted_for, log entries).
        // Falls back to in-memory-only mode if persistence fails (e.g. first run, permissions).
        let coordinator =
            match ClusterCoordinator::with_persistence(cluster_config.clone(), &config.data_dir) {
                Ok(c) => {
                    info!(
                        target: "lance::server",
                        data_dir = %config.data_dir.display(),
                        "Cluster coordinator initialized with persistent Raft log"
                    );
                    Arc::new(c)
                },
                Err(e) => {
                    warn!(
                        target: "lance::server",
                        error = %e,
                        "Failed to open persistent Raft log, falling back to in-memory mode"
                    );
                    Arc::new(ClusterCoordinator::new(cluster_config))
                },
            };

        // Bootstrap the control-plane runtime on its own dedicated OS thread.
        let runtime_coord = Arc::clone(&coordinator);
        let runtime_shutdown = shutdown_rx.resubscribe();
        let runtime_name = format!("lance-coordinator-{}", config.node_id);
        let coordinator_pin_core = config.coordinator_pin_core;
        let runtime_thread = match std::thread::Builder::new()
            .name(runtime_name.clone())
            .spawn(move || {
                if let Some(core_id) = coordinator_pin_core {
                    match lnc_core::pin_thread_to_cpu(core_id) {
                        Ok(()) => {
                            info!(
                                target: "lance::server",
                                core_id,
                                thread_name = %runtime_name,
                                "Pinned coordinator runtime thread to CPU core"
                            );
                        },
                        Err(e) => {
                            warn!(
                                target: "lance::server",
                                core_id,
                                thread_name = %runtime_name,
                                error = %e,
                                "Failed to pin coordinator runtime thread to CPU core"
                            );
                        },
                    }
                }

                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        error!(
                            target: "lance::server",
                            error = %e,
                            thread_name = %runtime_name,
                            "Failed to build dedicated coordinator runtime"
                        );
                        return;
                    },
                };

                runtime.block_on(async move {
                    // Start replication listener first (so peers can connect to us)
                    if let Err(e) = runtime_coord.start_listener().await {
                        warn!(
                            target: "lance::server",
                            error = %e,
                            "Failed to start replication listener on dedicated runtime"
                        );
                    }

                    // Initialize cluster - discover and connect to peers
                    if let Err(e) = runtime_coord.initialize().await {
                        warn!(
                            target: "lance::server",
                            error = %e,
                            "Failed to initialize cluster coordinator on dedicated runtime"
                        );
                    }

                    runtime_coord.run(runtime_shutdown).await;
                });
            }) {
            Ok(handle) => Some(handle),
            Err(e) => {
                warn!(
                    target: "lance::server",
                    error = %e,
                    "Failed to spawn dedicated coordinator runtime thread"
                );
                None
            },
        };

        // Start ResyncServer on leader side (dedicated port for bulk segment transfer).
        // The server listens on replication_port + port_offset (default: +1) and handles
        // incoming resync requests from followers that are too far behind for AppendEntries.
        let resync_config = ResyncConfig::default();
        let resync_server = Arc::new(ResyncServer::new(
            config.node_id,
            config.data_dir.clone(),
            resync_config.clone(),
        ));
        let resync_coord = Arc::clone(&coordinator);
        let resync_listen_port = config.replication_addr.port() + 1;
        let resync_listen_addr =
            std::net::SocketAddr::new(config.replication_addr.ip(), resync_listen_port);
        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(resync_listen_addr).await {
                Ok(l) => {
                    info!(
                        target: "lance::resync",
                        addr = %resync_listen_addr,
                        "Resync server listening"
                    );
                    l
                },
                Err(e) => {
                    warn!(
                        target: "lance::resync",
                        addr = %resync_listen_addr,
                        error = %e,
                        "Failed to start resync server listener"
                    );
                    return;
                },
            };

            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        // Only serve resync requests if we are the leader
                        if !resync_coord.is_leader() {
                            debug!(
                                target: "lance::resync",
                                peer = %addr,
                                "Rejecting resync request — not leader"
                            );
                            continue;
                        }

                        let server = Arc::clone(&resync_server);
                        let coord = Arc::clone(&resync_coord);
                        tokio::spawn(async move {
                            let term = coord.current_term().await;
                            if let Err(e) = server.handle_follower(stream, term).await {
                                warn!(
                                    target: "lance::resync",
                                    peer = %addr,
                                    error = %e,
                                    "Resync session failed"
                                );
                            }
                        });
                    },
                    Err(e) => {
                        warn!(
                            target: "lance::resync",
                            error = %e,
                            "Failed to accept resync connection"
                        );
                    },
                }
            }
        });

        // Subscribe to event channel and start handler BEFORE cluster loop to ensure receiver is active
        let mut event_rx = coordinator.subscribe();
        let event_registry = Arc::clone(&topic_registry);
        let event_config = config.clone();
        let event_coord = Arc::clone(&coordinator);
        let resync_data_dir = config.data_dir.clone();
        let resync_node_id = config.node_id;

        // Start cluster event handler for topic replication, data replication, and leader changes
        tokio::spawn(async move {
            // Track last known leader for change detection
            let mut last_leader = event_coord.leader_addr();
            // Topic writers for follower data replication (only used by followers)
            let mut follower_writers: std::collections::HashMap<u32, writer::TopicWriter> =
                std::collections::HashMap::new();
            // Resync actor for bulk segment transfer when follower is too far behind
            let mut resync_actor =
                ResyncActor::new(resync_node_id, resync_data_dir, ResyncConfig::default());

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
                        // Write replicated data using enriched format (L3 byte-identical segments)
                        let topic_id = entry.topic_id;
                        if let Err(e) = ingestion::write_replicated_data_enriched(
                            &event_config,
                            &event_registry,
                            &mut follower_writers,
                            &entry,
                        ) {
                            warn!(
                                target: "lance::server",
                                topic_id,
                                segment = %entry.segment_name,
                                error = %e,
                                "Failed to write enriched replicated data"
                            );
                            follower_writers.remove(&topic_id);
                        }
                    },
                    Ok(ClusterEvent::DataReceived { topic_id, payload }) => {
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

                        // Sync all local topics to followers to ensure cluster-wide consistency
                        // This is critical because each pod has its own persistent volume
                        let local_topics = event_registry.list_topics();
                        if !local_topics.is_empty() {
                            info!(
                                target: "lance::server",
                                topic_count = local_topics.len(),
                                "Syncing local topics to followers after becoming leader"
                            );
                            for topic in local_topics {
                                let op = TopicOperation::Create {
                                    topic_id: topic.id,
                                    name: topic.name.clone(),
                                    created_at: topic.created_at,
                                };
                                if let Err(e) = event_coord.replicate_topic_op(op).await {
                                    warn!(
                                        target: "lance::server",
                                        topic_id = topic.id,
                                        topic_name = %topic.name,
                                        error = %e,
                                        "Failed to sync topic to followers"
                                    );
                                }
                            }
                        }

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

                        // Check if we need a bulk resync (follower too far behind).
                        // Compute local data offset and compare against the threshold.
                        // The ResyncServer (leader side) determines the exact missing
                        // segments via manifest exchange — we only need to decide
                        // whether to attempt the resync connection at all.
                        if !resync_actor.is_active() {
                            if let Some(leader_repl_addr) = event_coord.peer_addr(leader_id).await {
                                // Compute local offset from segment files
                                let local_data_dir = event_config.data_dir.clone();
                                let local_offset = tokio::task::spawn_blocking(move || {
                                    let mut total = 0u64;
                                    if let Ok(entries) = std::fs::read_dir(&local_data_dir) {
                                        for entry in entries.flatten() {
                                            let path = entry.path();
                                            if path.extension().is_some_and(|ext| ext == "lnc") {
                                                if let Ok(meta) = std::fs::metadata(&path) {
                                                    total += meta.len();
                                                }
                                            }
                                        }
                                    }
                                    total
                                })
                                .await
                                .unwrap_or(0);

                                // Fresh node (no data) — always attempt resync to bootstrap.
                                // initiate_resync handles the full protocol: connect to
                                // leader's resync port (repl_port + port_offset), exchange
                                // manifest, stream missing segments, and rebuild indices.
                                if local_offset == 0 {
                                    info!(
                                        target: "lance::resync",
                                        leader_id,
                                        leader_addr = %leader_repl_addr,
                                        "Fresh node detected, initiating bulk resync"
                                    );
                                    if let Err(e) = resync_actor
                                        .initiate_resync(leader_repl_addr, local_offset)
                                        .await
                                    {
                                        warn!(
                                            target: "lance::resync",
                                            error = %e,
                                            "Bulk resync failed"
                                        );
                                        resync_actor.reset();
                                    }
                                }
                            }
                        }
                    },
                    Ok(ClusterEvent::LostLeadership { term }) => {
                        warn!(
                            target: "lance::server",
                            term,
                            "LostLeadership — no longer leader"
                        );
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
            dedicated_runtime = runtime_thread.is_some(),
            "Cluster coordinator started"
        );

        (Some(coordinator), runtime_thread)
    } else {
        info!(
            target: "lance::server",
            "Running in standalone mode (L1), cluster coordinator disabled"
        );
        (None, None)
    };

    let cluster = cluster_coordinator;

    // Create quorum manager for L3 mode (leader waits for M/2+1 ACKs before ACKing client)
    let quorum_manager: Option<Arc<AsyncQuorumManager>> =
        if config.replication_mode().requires_quorum() {
            let peer_count = config.peer_ids().len();
            // total_nodes = peers + self
            let total_nodes = peer_count + 1;
            let qm_config = QuorumConfig::new(total_nodes)
                .with_timeout(config.replication_quorum_timeout_ms.unwrap_or(1000));
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

    // Start data replication forwarder on a dedicated runtime thread.
    // Step 3: isolate data-forwarding execution from the main server runtime.
    let forwarder_runtime_handle = if let (Some(coord), Some(mut repl_rx)) =
        (&cluster, data_repl_rx)
    {
        let repl_coord = Arc::clone(coord);
        let repl_qm = quorum_manager.clone();
        let forwarder_shutdown = shutdown_rx.resubscribe();
        let runtime_name = format!("lance-forwarder-{}", config.node_id);
        let forwarder_pin_core = config.forwarder_pin_core;
        let max_inflight = config.replication_max_inflight.max(1);

        match std::thread::Builder::new()
                .name(runtime_name.clone())
                .spawn(move || {
                    if let Some(core_id) = forwarder_pin_core {
                        match lnc_core::pin_thread_to_cpu(core_id) {
                            Ok(()) => {
                                info!(
                                    target: "lance::server",
                                    core_id,
                                    thread_name = %runtime_name,
                                    "Pinned forwarder runtime thread to CPU core"
                                );
                            },
                            Err(e) => {
                                warn!(
                                    target: "lance::server",
                                    core_id,
                                    thread_name = %runtime_name,
                                    error = %e,
                                    "Failed to pin forwarder runtime thread to CPU core"
                                );
                            },
                        }
                    }

                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            error!(
                                target: "lance::server",
                                error = %e,
                                thread_name = %runtime_name,
                                "Failed to build dedicated forwarder runtime"
                            );
                            return;
                        },
                    };

                    runtime.block_on(async move {
                        let mut shutdown = forwarder_shutdown;
                        let mut channel_closed = false;
                        let mut inflight = tokio::task::JoinSet::new();
                        info!(
                            target: "lance::server",
                            max_inflight,
                            "Data replication forwarder started on dedicated runtime (enriched)"
                        );

                        loop {
                            tokio::select! {
                                maybe_req = repl_rx.recv(), if !channel_closed && inflight.len() < max_inflight => {
                                    match maybe_req {
                                        Some(req) => {
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

                                            let coord = Arc::clone(&repl_coord);
                                            inflight.spawn(async move {
                                                (write_id, topic_id, coord.replicate_data_enriched(entry).await)
                                            });
                                        }
                                        None => {
                                            channel_closed = true;
                                        }
                                    }
                                }
                                Some(join_result) = inflight.join_next(), if !inflight.is_empty() => {
                                    match join_result {
                                        Ok((write_id, topic_id, replication_result)) => {
                                            match replication_result {
                                                Ok(successful_peers) => {
                                                    if let (Some(wid), Some(qm)) = (write_id, &repl_qm) {
                                                        for peer_id in successful_peers {
                                                            qm.record_ack(wid, peer_id).await;
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    debug!(
                                                        target: "lance::server",
                                                        topic_id,
                                                        error = %e,
                                                        "Pipelined replication failed"
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                target: "lance::server",
                                            error = %e,
                                            "Forwarder in-flight task panicked"
                                        );
                                        }
                                    }
                                }
                                _ = shutdown.recv() => {
                                    break;
                                }
                            }

                            if channel_closed && inflight.is_empty() {
                                break;
                            }
                        }

                        // Best-effort drain already-started in-flight tasks during clean channel close.
                        while let Some(join_result) = inflight.join_next().await {
                            match join_result {
                                Ok((write_id, topic_id, replication_result)) => {
                                    match replication_result {
                                        Ok(successful_peers) => {
                                            if let (Some(wid), Some(qm)) = (write_id, &repl_qm) {
                                                for peer_id in successful_peers {
                                                    qm.record_ack(wid, peer_id).await;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            debug!(
                                                target: "lance::server",
                                                topic_id,
                                                error = %e,
                                                "Pipelined replication failed during drain"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        target: "lance::server",
                                        error = %e,
                                        "Forwarder in-flight task panicked during drain"
                                    );
                                }
                            }
                        }

                        info!(
                            target: "lance::server",
                            "Data replication forwarder stopped"
                        );
                    });
                }) {
                Ok(handle) => Some(handle),
                Err(e) => {
                    warn!(
                        target: "lance::server",
                        error = %e,
                        "Failed to spawn dedicated forwarder runtime thread"
                    );
                    None
                },
            }
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

    // Multi-actor ingestion is always used (managed by multi_actor_system)
    let ingestion_handle: Option<tokio::task::JoinHandle<()>> = None;

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

    // Join dedicated forwarder runtime thread after shutdown signal propagation.
    if let Some(handle) = forwarder_runtime_handle {
        match tokio::task::spawn_blocking(move || handle.join()).await {
            Ok(Ok(())) => {
                debug!(target: "lance::server", "Dedicated forwarder runtime thread joined");
            },
            Ok(Err(_)) => {
                warn!(
                    target: "lance::server",
                    "Dedicated forwarder runtime thread panicked during shutdown"
                );
            },
            Err(e) => {
                warn!(
                    target: "lance::server",
                    error = %e,
                    "Failed to join dedicated forwarder runtime thread"
                );
            },
        }
    }

    // Join dedicated coordinator runtime thread after shutdown signal propagation.
    if let Some(handle) = cluster_runtime_handle {
        match tokio::task::spawn_blocking(move || handle.join()).await {
            Ok(Ok(())) => {
                debug!(target: "lance::server", "Dedicated coordinator runtime thread joined");
            },
            Ok(Err(_)) => {
                warn!(
                    target: "lance::server",
                    "Dedicated coordinator runtime thread panicked during shutdown"
                );
            },
            Err(e) => {
                warn!(
                    target: "lance::server",
                    error = %e,
                    "Failed to join dedicated coordinator runtime thread"
                );
            },
        }
    }

    replication_handle.abort();
    retention_handle.abort();

    info!(target: "lance::server", "Server shutdown complete");

    Ok(())
}
