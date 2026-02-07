//! Grouped Consumer Mode
//!
//! Provides Kafka-like consumer group coordination at the client level.
//! In grouped mode, a coordinator (main client) manages work distribution
//! among multiple worker consumers, enabling horizontal scaling.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                  GroupCoordinator                       │
//! │  - Discovers topics from server                         │
//! │  - Tracks workers via heartbeat                         │
//! │  - Assigns topics using configurable strategy           │
//! │  - Rebalances on worker join/leave                      │
//! └─────────────────────────────────────────────────────────┘
//!          │              │              │
//!          ▼              ▼              ▼
//!     ┌─────────┐   ┌─────────┐   ┌─────────┐
//!     │ Worker  │   │ Worker  │   │ Worker  │
//!     │   A     │   │   B     │   │   C     │
//!     │ [T1,T2] │   │ [T3,T4] │   │ [T5,T6] │
//!     └─────────┘   └─────────┘   └─────────┘
//! ```
//!
//! # Client-Side Offset Management
//!
//! Unlike Kafka where the broker manages consumer group offsets, LANCE
//! uses client-side offset management. This:
//! - Reduces server load (no offset commit replication)
//! - Enables exactly-once semantics with local transactions
//! - Allows stateless broker scaling
//!
//! # Example
//!
//! ```rust,no_run
//! use lnc_client::{AssignmentStrategy, GroupCoordinator, GroupConfig, GroupedConsumer, WorkerConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Start coordinator (typically one per consumer group)
//!     let coordinator = GroupCoordinator::start(
//!         "127.0.0.1:1992",
//!         GroupConfig::new("my-group")
//!             .with_topics(vec![1, 2, 3, 4])
//!             .with_assignment_strategy(AssignmentStrategy::RoundRobin),
//!     ).await?;
//!
//!     // Workers join the group (server_addr, coordinator_addr, config)
//!     let mut worker = GroupedConsumer::join(
//!         "127.0.0.1:1992",
//!         coordinator.join_address(),
//!         WorkerConfig::new("worker-1"),
//!     ).await?;
//!
//!     // Worker receives assignments and processes
//!     loop {
//!         // Copy assignments to avoid borrow issues
//!         let topics: Vec<u32> = worker.assignments().to_vec();
//!         for topic_id in topics {
//!             if let Some(_records) = worker.poll(topic_id).await? {
//!                 // Process records
//!                 worker.commit(topic_id).await?;
//!             }
//!         }
//!     }
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast};

use crate::client::{ClientConfig, LanceClient};
use crate::consumer::{PollResult, SeekPosition};
use crate::error::{ClientError, Result};
use crate::offset::{LockFileOffsetStore, MemoryOffsetStore, OffsetStore};
use crate::standalone::{StandaloneConfig, StandaloneConsumer};

/// Assignment strategy for distributing topics among workers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AssignmentStrategy {
    /// Distribute topics evenly across workers in round-robin fashion
    #[default]
    RoundRobin,
    /// Assign contiguous ranges of topics to each worker
    Range,
    /// Minimize reassignment when workers join/leave (sticky)
    Sticky,
}

/// Configuration for the group coordinator
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Unique identifier for this consumer group
    pub group_id: String,
    /// Topics to consume (if empty, discovers from server)
    pub topics: Vec<u32>,
    /// Assignment strategy
    pub assignment_strategy: AssignmentStrategy,
    /// How often workers must send heartbeat
    pub heartbeat_interval: Duration,
    /// How long before a worker is considered dead
    pub session_timeout: Duration,
    /// Address for the coordinator to listen on
    pub coordinator_addr: SocketAddr,
    /// LANCE server address for topic discovery
    pub server_addr: String,
    /// Offset storage directory
    pub offset_dir: Option<PathBuf>,
}

impl GroupConfig {
    /// Create a new group configuration
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Vec::new(),
            assignment_strategy: AssignmentStrategy::RoundRobin,
            heartbeat_interval: Duration::from_secs(3),
            session_timeout: Duration::from_secs(30),
            coordinator_addr: "127.0.0.1:19920"
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 19920))),
            server_addr: "127.0.0.1:1992".to_string(),
            offset_dir: None,
        }
    }

    /// Set topics to consume
    pub fn with_topics(mut self, topics: Vec<u32>) -> Self {
        self.topics = topics;
        self
    }

    /// Set the assignment strategy
    pub fn with_assignment_strategy(mut self, strategy: AssignmentStrategy) -> Self {
        self.assignment_strategy = strategy;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set session timeout
    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    /// Set coordinator listen address
    pub fn with_coordinator_addr(mut self, addr: SocketAddr) -> Self {
        self.coordinator_addr = addr;
        self
    }

    /// Set LANCE server address
    pub fn with_server_addr(mut self, addr: impl Into<String>) -> Self {
        self.server_addr = addr.into();
        self
    }

    /// Set offset storage directory
    pub fn with_offset_dir(mut self, dir: &Path) -> Self {
        self.offset_dir = Some(dir.to_path_buf());
        self
    }
}

/// Internal message types for coordinator-worker communication
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CoordinatorMessage {
    /// Worker joining the group
    Join { worker_id: String },
    /// Worker leaving the group
    Leave { worker_id: String },
    /// Heartbeat from worker
    Heartbeat { worker_id: String },
    /// Request current assignments
    GetAssignments { worker_id: String },
}

/// Internal response types
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CoordinatorResponse {
    /// Join accepted with initial assignments
    JoinAccepted {
        worker_id: String,
        generation: u64,
        assignments: Vec<u32>,
    },
    /// Leave acknowledged
    LeaveAcknowledged,
    /// Heartbeat acknowledged
    HeartbeatAck { generation: u64 },
    /// Current assignments
    Assignments {
        generation: u64,
        assignments: Vec<u32>,
    },
    /// Rebalance notification
    Rebalance {
        generation: u64,
        assignments: Vec<u32>,
    },
    /// Error response
    Error { message: String },
}

/// State of a worker in the group
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct WorkerState {
    worker_id: String,
    last_heartbeat: Instant,
    assignments: Vec<u32>,
    generation: u64,
}

/// Group coordinator that manages worker assignment
///
/// The coordinator:
/// - Listens for worker connections
/// - Tracks worker liveness via heartbeats
/// - Assigns topics to workers using the configured strategy
/// - Triggers rebalance when workers join/leave
pub struct GroupCoordinator {
    #[allow(dead_code)]
    config: GroupConfig,
    workers: Arc<RwLock<HashMap<String, WorkerState>>>,
    generation: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    join_addr: SocketAddr,
}

impl GroupCoordinator {
    /// Start the coordinator and begin listening for workers
    pub async fn start(server_addr: &str, mut config: GroupConfig) -> Result<Self> {
        config.server_addr = server_addr.to_string();

        // Discover topics if not specified
        if config.topics.is_empty() {
            let client_config = ClientConfig::new(&config.server_addr);
            let mut client = LanceClient::connect(client_config).await?;
            let topics = client.list_topics().await?;
            config.topics = topics.iter().map(|t| t.id).collect();
        }

        let workers = Arc::new(RwLock::new(HashMap::new()));
        let generation = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let (shutdown_tx, _) = broadcast::channel(1);

        let listener = TcpListener::bind(config.coordinator_addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        let join_addr = listener.local_addr().map_err(ClientError::IoError)?;

        let coordinator = Self {
            config: config.clone(),
            workers: workers.clone(),
            generation: generation.clone(),
            running: running.clone(),
            shutdown_tx: shutdown_tx.clone(),
            join_addr,
        };

        // Spawn coordinator tasks
        let workers_clone = workers.clone();
        let generation_clone = generation.clone();
        let _running_clone = running.clone();
        let config_clone = config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        // Accept worker connections
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                let workers = workers_clone.clone();
                                let generation = generation_clone.clone();
                                let config = config_clone.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_worker_connection(
                                        stream, addr, workers, generation, config
                                    ).await {
                                        tracing::warn!("Worker connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::error!("Accept error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Coordinator shutting down");
                        break;
                    }
                }
            }
        });

        // Spawn heartbeat checker
        let workers_checker = workers.clone();
        let generation_checker = generation.clone();
        let running_checker = running.clone();
        let session_timeout = config.session_timeout;
        let mut shutdown_rx2 = shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if !running_checker.load(Ordering::Relaxed) {
                            break;
                        }
                        Self::check_worker_health(
                            &workers_checker,
                            &generation_checker,
                            session_timeout,
                        ).await;
                    }
                    _ = shutdown_rx2.recv() => {
                        break;
                    }
                }
            }
        });

        Ok(coordinator)
    }

    /// Get the address workers should connect to
    pub fn join_address(&self) -> SocketAddr {
        self.join_addr
    }

    /// Get current generation (increments on rebalance)
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Get current worker count
    pub async fn worker_count(&self) -> usize {
        self.workers.read().await.len()
    }

    /// Get current assignments for all workers
    pub async fn get_assignments(&self) -> HashMap<String, Vec<u32>> {
        self.workers
            .read()
            .await
            .iter()
            .map(|(id, state)| (id.clone(), state.assignments.clone()))
            .collect()
    }

    /// Stop the coordinator
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        let _ = self.shutdown_tx.send(());
    }

    /// Handle a worker connection
    async fn handle_worker_connection(
        stream: TcpStream,
        _addr: SocketAddr,
        workers: Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: Arc<AtomicU64>,
        config: GroupConfig,
    ) -> Result<()> {
        // Simple protocol: length-prefixed JSON messages
        // In production, use proper framing like LWP
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut reader, mut writer) = stream.into_split();
        let mut buf = vec![0u8; 4096];

        loop {
            // Read message length
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break; // Connection closed
            }

            // Parse message (simplified - in production use proper serialization)
            let msg_str = std::str::from_utf8(&buf[..n])
                .map_err(|e| ClientError::ProtocolError(e.to_string()))?;

            let response = Self::process_message(msg_str, &workers, &generation, &config).await;

            // Send response
            let response_bytes = format!("{:?}", response);
            writer.write_all(response_bytes.as_bytes()).await?;
        }

        Ok(())
    }

    /// Process a coordinator message
    async fn process_message(
        msg: &str,
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: &Arc<AtomicU64>,
        config: &GroupConfig,
    ) -> CoordinatorResponse {
        // Parse message (simplified)
        if msg.starts_with("JOIN:") {
            let worker_id = msg
                .strip_prefix("JOIN:")
                .unwrap_or("unknown")
                .trim()
                .to_string();
            Self::handle_join(worker_id, workers, generation, config).await
        } else if msg.starts_with("LEAVE:") {
            let worker_id = msg
                .strip_prefix("LEAVE:")
                .unwrap_or("unknown")
                .trim()
                .to_string();
            Self::handle_leave(worker_id, workers, generation, config).await
        } else if msg.starts_with("HEARTBEAT:") {
            let worker_id = msg
                .strip_prefix("HEARTBEAT:")
                .unwrap_or("unknown")
                .trim()
                .to_string();
            Self::handle_heartbeat(worker_id, workers, generation).await
        } else if msg.starts_with("GET_ASSIGNMENTS:") {
            let worker_id = msg
                .strip_prefix("GET_ASSIGNMENTS:")
                .unwrap_or("unknown")
                .trim()
                .to_string();
            Self::handle_get_assignments(worker_id, workers, generation).await
        } else {
            CoordinatorResponse::Error {
                message: format!("Unknown message: {}", msg),
            }
        }
    }

    /// Handle worker join
    async fn handle_join(
        worker_id: String,
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: &Arc<AtomicU64>,
        config: &GroupConfig,
    ) -> CoordinatorResponse {
        let new_gen = generation.fetch_add(1, Ordering::SeqCst) + 1;

        {
            let mut workers_lock = workers.write().await;
            workers_lock.insert(
                worker_id.clone(),
                WorkerState {
                    worker_id: worker_id.clone(),
                    last_heartbeat: Instant::now(),
                    assignments: Vec::new(),
                    generation: new_gen,
                },
            );
        }

        // Rebalance assignments
        let _assignments = Self::rebalance(workers, config).await;

        // Get this worker's assignments
        let worker_assignments = {
            let workers_lock = workers.read().await;
            workers_lock
                .get(&worker_id)
                .map(|w| w.assignments.clone())
                .unwrap_or_default()
        };

        CoordinatorResponse::JoinAccepted {
            worker_id,
            generation: new_gen,
            assignments: worker_assignments,
        }
    }

    /// Handle worker leave
    async fn handle_leave(
        worker_id: String,
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: &Arc<AtomicU64>,
        config: &GroupConfig,
    ) -> CoordinatorResponse {
        {
            let mut workers_lock = workers.write().await;
            workers_lock.remove(&worker_id);
        }

        generation.fetch_add(1, Ordering::SeqCst);

        // Rebalance remaining workers
        Self::rebalance(workers, config).await;

        CoordinatorResponse::LeaveAcknowledged
    }

    /// Handle worker heartbeat
    async fn handle_heartbeat(
        worker_id: String,
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: &Arc<AtomicU64>,
    ) -> CoordinatorResponse {
        let current_gen = generation.load(Ordering::Relaxed);

        let mut workers_lock = workers.write().await;
        if let Some(worker) = workers_lock.get_mut(&worker_id) {
            worker.last_heartbeat = Instant::now();
            CoordinatorResponse::HeartbeatAck {
                generation: current_gen,
            }
        } else {
            CoordinatorResponse::Error {
                message: "Worker not found".to_string(),
            }
        }
    }

    /// Handle get assignments request
    async fn handle_get_assignments(
        worker_id: String,
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: &Arc<AtomicU64>,
    ) -> CoordinatorResponse {
        let current_gen = generation.load(Ordering::Relaxed);
        let workers_lock = workers.read().await;

        if let Some(worker) = workers_lock.get(&worker_id) {
            CoordinatorResponse::Assignments {
                generation: current_gen,
                assignments: worker.assignments.clone(),
            }
        } else {
            CoordinatorResponse::Error {
                message: "Worker not found".to_string(),
            }
        }
    }

    /// Check worker health and trigger rebalance if needed
    async fn check_worker_health(
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        generation: &Arc<AtomicU64>,
        session_timeout: Duration,
    ) {
        let now = Instant::now();
        let mut dead_workers = Vec::new();

        {
            let workers_lock = workers.read().await;
            for (id, state) in workers_lock.iter() {
                if now.duration_since(state.last_heartbeat) > session_timeout {
                    dead_workers.push(id.clone());
                }
            }
        }

        if !dead_workers.is_empty() {
            let mut workers_lock = workers.write().await;
            for id in dead_workers {
                tracing::warn!("Worker {} timed out, removing from group", id);
                workers_lock.remove(&id);
            }
            generation.fetch_add(1, Ordering::SeqCst);
            // Rebalance will happen on next assignment request
        }
    }

    /// Rebalance topic assignments among workers
    async fn rebalance(
        workers: &Arc<RwLock<HashMap<String, WorkerState>>>,
        config: &GroupConfig,
    ) -> HashMap<String, Vec<u32>> {
        let mut workers_lock = workers.write().await;
        let worker_ids: Vec<String> = workers_lock.keys().cloned().collect();

        if worker_ids.is_empty() {
            return HashMap::new();
        }

        let assignments = match config.assignment_strategy {
            AssignmentStrategy::RoundRobin => Self::assign_round_robin(&config.topics, &worker_ids),
            AssignmentStrategy::Range => Self::assign_range(&config.topics, &worker_ids),
            AssignmentStrategy::Sticky => {
                // For sticky, we try to maintain existing assignments
                let existing: HashMap<String, Vec<u32>> = workers_lock
                    .iter()
                    .map(|(id, state)| (id.clone(), state.assignments.clone()))
                    .collect();
                Self::assign_sticky(&config.topics, &worker_ids, &existing)
            },
        };

        // Update worker states
        for (worker_id, topics) in &assignments {
            if let Some(worker) = workers_lock.get_mut(worker_id) {
                worker.assignments = topics.clone();
            }
        }

        assignments
    }

    /// Round-robin assignment strategy
    fn assign_round_robin(topics: &[u32], workers: &[String]) -> HashMap<String, Vec<u32>> {
        let mut assignments: HashMap<String, Vec<u32>> =
            workers.iter().map(|w| (w.clone(), Vec::new())).collect();

        for (i, topic) in topics.iter().enumerate() {
            let worker = &workers[i % workers.len()];
            if let Some(topics) = assignments.get_mut(worker) {
                topics.push(*topic);
            }
        }

        assignments
    }

    /// Range assignment strategy
    fn assign_range(topics: &[u32], workers: &[String]) -> HashMap<String, Vec<u32>> {
        let mut assignments: HashMap<String, Vec<u32>> =
            workers.iter().map(|w| (w.clone(), Vec::new())).collect();

        let topics_per_worker = topics.len() / workers.len();
        let remainder = topics.len() % workers.len();

        let mut topic_idx = 0;
        for (worker_idx, worker) in workers.iter().enumerate() {
            let extra = if worker_idx < remainder { 1 } else { 0 };
            let count = topics_per_worker + extra;

            if let Some(worker_topics) = assignments.get_mut(worker) {
                for _ in 0..count {
                    if topic_idx < topics.len() {
                        worker_topics.push(topics[topic_idx]);
                        topic_idx += 1;
                    }
                }
            }
        }

        assignments
    }

    /// Sticky assignment strategy (minimizes movement)
    fn assign_sticky(
        topics: &[u32],
        workers: &[String],
        existing: &HashMap<String, Vec<u32>>,
    ) -> HashMap<String, Vec<u32>> {
        let mut assignments: HashMap<String, Vec<u32>> =
            workers.iter().map(|w| (w.clone(), Vec::new())).collect();

        let topic_set: HashSet<u32> = topics.iter().copied().collect();
        let mut assigned: HashSet<u32> = HashSet::new();

        // First, keep existing assignments that are still valid
        for (worker, old_topics) in existing {
            if assignments.contains_key(worker) {
                for topic in old_topics {
                    if topic_set.contains(topic) && !assigned.contains(topic) {
                        if let Some(worker_topics) = assignments.get_mut(worker) {
                            worker_topics.push(*topic);
                            assigned.insert(*topic);
                        }
                    }
                }
            }
        }

        // Then distribute unassigned topics
        let unassigned: Vec<u32> = topics
            .iter()
            .filter(|t| !assigned.contains(t))
            .copied()
            .collect();

        // Find worker with fewest assignments for each unassigned topic
        for topic in unassigned {
            let min_worker = assignments
                .iter()
                .min_by_key(|(_, topics)| topics.len())
                .map(|(w, _)| w.clone());

            if let Some(worker) = min_worker {
                if let Some(worker_topics) = assignments.get_mut(&worker) {
                    worker_topics.push(topic);
                }
            }
        }

        assignments
    }
}

impl Drop for GroupCoordinator {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Configuration for a grouped consumer (worker)
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique identifier for this worker
    pub worker_id: String,
    /// Maximum bytes to fetch per poll
    pub max_fetch_bytes: u32,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Offset storage directory
    pub offset_dir: Option<PathBuf>,
    /// Starting position for new topics
    pub start_position: SeekPosition,
}

impl WorkerConfig {
    /// Create a new worker configuration
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            max_fetch_bytes: 1_048_576,
            heartbeat_interval: Duration::from_secs(3),
            offset_dir: None,
            start_position: SeekPosition::Beginning,
        }
    }

    /// Set max fetch bytes
    pub fn with_max_fetch_bytes(mut self, bytes: u32) -> Self {
        self.max_fetch_bytes = bytes;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set offset storage directory
    pub fn with_offset_dir(mut self, dir: &Path) -> Self {
        self.offset_dir = Some(dir.to_path_buf());
        self
    }

    /// Set starting position
    pub fn with_start_position(mut self, position: SeekPosition) -> Self {
        self.start_position = position;
        self
    }
}

/// Grouped consumer (worker) that receives assignments from a coordinator
pub struct GroupedConsumer {
    config: WorkerConfig,
    server_addr: String,
    coordinator_addr: SocketAddr,
    assignments: Vec<u32>,
    generation: u64,
    consumers: HashMap<u32, StandaloneConsumer>,
    #[allow(dead_code)]
    offset_store: Arc<dyn OffsetStore>,
    running: bool,
}

impl GroupedConsumer {
    /// Join a consumer group by connecting to the coordinator
    pub async fn join(
        server_addr: &str,
        coordinator_addr: SocketAddr,
        config: WorkerConfig,
    ) -> Result<Self> {
        // Connect to coordinator
        let mut stream = TcpStream::connect(coordinator_addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Send join message
        let join_msg = format!("JOIN:{}", config.worker_id);
        stream.write_all(join_msg.as_bytes()).await?;

        // Receive response
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await?;
        let response = std::str::from_utf8(&buf[..n])
            .map_err(|e| ClientError::ProtocolError(e.to_string()))?;

        // Parse assignments from response (simplified)
        let (generation, assignments) = Self::parse_join_response(response)?;

        // Initialize offset store
        let offset_store: Arc<dyn OffsetStore> = if let Some(ref dir) = config.offset_dir {
            Arc::new(LockFileOffsetStore::open(dir, &config.worker_id)?)
        } else {
            Arc::new(MemoryOffsetStore::new())
        };

        // Create consumers for assigned topics
        let mut consumers = HashMap::new();
        for topic_id in &assignments {
            let standalone_config = StandaloneConfig::new(&config.worker_id, *topic_id)
                .with_max_fetch_bytes(config.max_fetch_bytes)
                .with_start_position(config.start_position);

            if let Some(ref dir) = config.offset_dir {
                let consumer = StandaloneConsumer::connect(
                    server_addr,
                    standalone_config.with_offset_dir(dir),
                )
                .await?;
                consumers.insert(*topic_id, consumer);
            } else {
                let consumer = StandaloneConsumer::connect(server_addr, standalone_config).await?;
                consumers.insert(*topic_id, consumer);
            }
        }

        Ok(Self {
            config,
            server_addr: server_addr.to_string(),
            coordinator_addr,
            assignments,
            generation,
            consumers,
            offset_store,
            running: true,
        })
    }

    /// Get current topic assignments
    pub fn assignments(&self) -> &[u32] {
        &self.assignments
    }

    /// Get current generation
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Poll a specific assigned topic
    pub async fn poll(&mut self, topic_id: u32) -> Result<Option<PollResult>> {
        if let Some(consumer) = self.consumers.get_mut(&topic_id) {
            consumer.poll().await
        } else {
            Err(ClientError::InvalidResponse(format!(
                "Topic {} not assigned to this worker",
                topic_id
            )))
        }
    }

    /// Commit offset for a specific topic
    pub async fn commit(&mut self, topic_id: u32) -> Result<()> {
        if let Some(consumer) = self.consumers.get_mut(&topic_id) {
            consumer.commit().await
        } else {
            Err(ClientError::InvalidResponse(format!(
                "Topic {} not assigned to this worker",
                topic_id
            )))
        }
    }

    /// Commit all topics
    pub async fn commit_all(&mut self) -> Result<()> {
        for consumer in self.consumers.values_mut() {
            consumer.commit().await?;
        }
        Ok(())
    }

    /// Send heartbeat to coordinator
    pub async fn heartbeat(&mut self) -> Result<u64> {
        let mut stream = TcpStream::connect(self.coordinator_addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let msg = format!("HEARTBEAT:{}", self.config.worker_id);
        stream.write_all(msg.as_bytes()).await?;

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let response = std::str::from_utf8(&buf[..n])
            .map_err(|e| ClientError::ProtocolError(e.to_string()))?;

        // Check if generation changed (rebalance needed)
        let new_gen = Self::parse_heartbeat_response(response)?;

        if new_gen > self.generation {
            // Fetch new assignments
            self.refresh_assignments().await?;
        }

        Ok(self.generation)
    }

    /// Refresh assignments from coordinator
    async fn refresh_assignments(&mut self) -> Result<()> {
        let mut stream = TcpStream::connect(self.coordinator_addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let msg = format!("GET_ASSIGNMENTS:{}", self.config.worker_id);
        stream.write_all(msg.as_bytes()).await?;

        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await?;
        let response = std::str::from_utf8(&buf[..n])
            .map_err(|e| ClientError::ProtocolError(e.to_string()))?;

        let (generation, new_assignments) = Self::parse_assignments_response(response)?;

        // Update assignments
        let old_set: HashSet<u32> = self.assignments.iter().copied().collect();
        let new_set: HashSet<u32> = new_assignments.iter().copied().collect();

        // Remove consumers for revoked topics
        for topic_id in old_set.difference(&new_set) {
            if let Some(consumer) = self.consumers.remove(topic_id) {
                let _ = consumer.close().await;
            }
        }

        // Add consumers for new topics
        for topic_id in new_set.difference(&old_set) {
            let standalone_config = StandaloneConfig::new(&self.config.worker_id, *topic_id)
                .with_max_fetch_bytes(self.config.max_fetch_bytes)
                .with_start_position(self.config.start_position);

            let consumer = if let Some(ref dir) = self.config.offset_dir {
                StandaloneConsumer::connect(
                    &self.server_addr,
                    standalone_config.with_offset_dir(dir),
                )
                .await?
            } else {
                StandaloneConsumer::connect(&self.server_addr, standalone_config).await?
            };

            self.consumers.insert(*topic_id, consumer);
        }

        self.assignments = new_assignments;
        self.generation = generation;

        Ok(())
    }

    /// Leave the consumer group
    pub async fn leave(mut self) -> Result<()> {
        // Commit all offsets
        self.commit_all().await?;

        // Notify coordinator
        let mut stream = TcpStream::connect(self.coordinator_addr)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        use tokio::io::AsyncWriteExt;

        let msg = format!("LEAVE:{}", self.config.worker_id);
        stream.write_all(msg.as_bytes()).await?;

        // Close all consumers
        for (_, consumer) in self.consumers.drain() {
            let _ = consumer.close().await;
        }

        self.running = false;
        Ok(())
    }

    /// Parse join response (simplified)
    fn parse_join_response(response: &str) -> Result<(u64, Vec<u32>)> {
        // Format: "JoinAccepted { worker_id: ..., generation: N, assignments: [1, 2, 3] }"
        // This is a simplified parser - production would use proper serialization

        let generation = response
            .find("generation: ")
            .and_then(|i| {
                let start = i + 12;
                let end = response[start..].find(',')?;
                response[start..start + end].parse().ok()
            })
            .unwrap_or(0);

        let assignments = response
            .find("assignments: [")
            .map(|i| {
                let start = i + 14;
                let end = response[start..].find(']').unwrap_or(0);
                response[start..start + end]
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect()
            })
            .unwrap_or_default();

        Ok((generation, assignments))
    }

    /// Parse heartbeat response
    fn parse_heartbeat_response(response: &str) -> Result<u64> {
        response
            .find("generation: ")
            .and_then(|i| {
                let start = i + 12;
                let end = response[start..]
                    .find([',', ' ', '}'])
                    .unwrap_or(response.len() - start);
                response[start..start + end].parse().ok()
            })
            .ok_or_else(|| ClientError::ProtocolError("Invalid heartbeat response".to_string()))
    }

    /// Parse assignments response
    fn parse_assignments_response(response: &str) -> Result<(u64, Vec<u32>)> {
        Self::parse_join_response(response)
    }
}

impl std::fmt::Debug for GroupedConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupedConsumer")
            .field("worker_id", &self.config.worker_id)
            .field("generation", &self.generation)
            .field("assignments", &self.assignments)
            .field("running", &self.running)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_group_config_defaults() {
        let config = GroupConfig::new("test-group");

        assert_eq!(config.group_id, "test-group");
        assert!(config.topics.is_empty());
        assert_eq!(config.assignment_strategy, AssignmentStrategy::RoundRobin);
    }

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::new("worker-1");

        assert_eq!(config.worker_id, "worker-1");
        assert_eq!(config.max_fetch_bytes, 1_048_576);
    }

    #[test]
    fn test_round_robin_assignment() {
        let topics = vec![1, 2, 3, 4, 5, 6];
        let workers = vec!["w1".to_string(), "w2".to_string(), "w3".to_string()];

        let assignments = GroupCoordinator::assign_round_robin(&topics, &workers);

        assert_eq!(assignments.get("w1"), Some(&vec![1, 4]));
        assert_eq!(assignments.get("w2"), Some(&vec![2, 5]));
        assert_eq!(assignments.get("w3"), Some(&vec![3, 6]));
    }

    #[test]
    fn test_range_assignment() {
        let topics = vec![1, 2, 3, 4, 5, 6, 7];
        let workers = vec!["w1".to_string(), "w2".to_string(), "w3".to_string()];

        let assignments = GroupCoordinator::assign_range(&topics, &workers);

        // 7 topics / 3 workers = 2 each + 1 remainder
        // w1 gets 3, w2 gets 2, w3 gets 2
        assert_eq!(assignments.get("w1").map(|v| v.len()), Some(3));
        assert_eq!(assignments.get("w2").map(|v| v.len()), Some(2));
        assert_eq!(assignments.get("w3").map(|v| v.len()), Some(2));
    }

    #[test]
    fn test_sticky_assignment_preserves_existing() {
        let topics = vec![1, 2, 3, 4];
        let workers = vec!["w1".to_string(), "w2".to_string()];

        let mut existing = HashMap::new();
        existing.insert("w1".to_string(), vec![1, 2]);
        existing.insert("w2".to_string(), vec![3, 4]);

        let assignments = GroupCoordinator::assign_sticky(&topics, &workers, &existing);

        // Should preserve existing assignments
        assert_eq!(assignments.get("w1"), Some(&vec![1, 2]));
        assert_eq!(assignments.get("w2"), Some(&vec![3, 4]));
    }

    #[test]
    fn test_assignment_strategy_default() {
        assert_eq!(
            AssignmentStrategy::default(),
            AssignmentStrategy::RoundRobin
        );
    }
}
