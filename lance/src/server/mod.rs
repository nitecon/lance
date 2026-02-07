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
pub use ingestion::{IngestionRequest, run_ingestion_actor};
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
    ClusterCoordinator, ClusterEvent, ForwardConfig, LeaderConnectionPool, ReplicationActor,
    TopicOperation, create_leader_pool, create_replication_channel,
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

    // Create ingestion system - multi-actor when actor_count > 1, single-actor otherwise
    let (ingestion_sender, ingestion_rx, multi_actor_system) = if config.ingestion.actor_count > 1 {
        // Multi-actor mode with crossbeam ArrayQueue (per Architecture §5.2)
        let multi = MultiActorIngestion::new(
            config.clone(),
            Arc::clone(&topic_registry),
            config.ingestion.channel_capacity,
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

        // Start cluster event handler for topic replication and leader changes
        let mut event_rx = coordinator.subscribe();
        let event_registry = Arc::clone(&topic_registry);
        let event_coord = Arc::clone(&coordinator);
        tokio::spawn(async move {
            // Track last known leader for change detection
            let mut last_leader = event_coord.leader_addr();

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
                    // Note: Leader pool is updated via forward module's on_leader_change
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
                        },
                    },
                    Ok(_) => {}, // Ignore other events
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
        Some(tokio::spawn(async move {
            run_ingestion_actor(ingestion_config, rx, ingestion_pool, ingestion_registry).await
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

    // Create rate limiter for consumer reads (100MB/s per connection)
    let rate_limiter = Arc::new(ConsumerRateLimiter::new(100 * 1024 * 1024));

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
                        let node_id = config.node_id;
                        let max_payload = config.ingestion.max_payload_size;

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, tx, pool, registry, node_id, max_payload, limiter, subs, cluster_state, fwd_pool, validator).await {
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

    replication_handle.abort();
    retention_handle.abort();

    info!(target: "lance::server", "Server shutdown complete");

    Ok(())
}
