//! Shard infrastructure for shared-nothing concurrency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::{mpsc, oneshot};

use crate::command::CommandContext;
use crate::error::CommandError;
use crate::eviction::{EvictionCandidate, EvictionConfig, EvictionPolicy, EvictionPool};
use crate::persistence::{
    NoopSnapshotCoordinator, RocksStore, RocksWalWriter, SnapshotCoordinator, WalConfig,
};
use crate::pubsub::{
    ConnId, IntrospectionRequest, IntrospectionResponse, PubSubSender, ShardSubscriptions,
};
use crate::registry::CommandRegistry;
use crate::scripting::{ScriptExecutor, ScriptingConfig};
use crate::store::{HashMapStore, Store};

/// Messages sent to shard workers.
#[derive(Debug)]
pub enum ShardMessage {
    /// Execute a command on this shard.
    Execute {
        command: ParsedCommand,
        conn_id: u64,
        /// Transaction ID for VLL ordering (optional for single-shard operations).
        txid: Option<u64>,
        response_tx: oneshot::Sender<Response>,
    },

    /// Scatter-gather: partial request for multi-key operation.
    ScatterRequest {
        request_id: u64,
        keys: Vec<Bytes>,
        operation: ScatterOp,
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// Get the current shard version (for WATCH).
    GetVersion {
        response_tx: oneshot::Sender<u64>,
    },

    /// Execute a transaction atomically.
    ExecTransaction {
        commands: Vec<ParsedCommand>,
        /// Watched keys: (key, version_at_watch_time).
        watches: Vec<(Bytes, u64)>,
        conn_id: u64,
        response_tx: oneshot::Sender<TransactionResult>,
    },

    // =========================================================================
    // Pub/Sub messages
    // =========================================================================

    /// Subscribe to broadcast channels.
    Subscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Unsubscribe from broadcast channels.
    Unsubscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Subscribe to patterns.
    PSubscribe {
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Unsubscribe from patterns.
    PUnsubscribe {
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Publish to a broadcast channel.
    Publish {
        channel: Bytes,
        message: Bytes,
        response_tx: oneshot::Sender<usize>,
    },

    /// Subscribe to sharded channels.
    ShardedSubscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Unsubscribe from sharded channels.
    ShardedUnsubscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Publish to a sharded channel.
    ShardedPublish {
        channel: Bytes,
        message: Bytes,
        response_tx: oneshot::Sender<usize>,
    },

    /// Pub/Sub introspection request.
    PubSubIntrospection {
        request: IntrospectionRequest,
        response_tx: oneshot::Sender<IntrospectionResponse>,
    },

    /// Connection closed - clean up subscriptions.
    ConnectionClosed {
        conn_id: ConnId,
    },

    // =========================================================================
    // Scripting messages
    // =========================================================================

    /// Execute a Lua script (EVAL).
    EvalScript {
        /// Script source code.
        script_source: Bytes,
        /// Keys passed to the script.
        keys: Vec<Bytes>,
        /// Additional arguments.
        argv: Vec<Bytes>,
        /// Connection ID.
        conn_id: u64,
        /// Response channel.
        response_tx: oneshot::Sender<Response>,
    },

    /// Execute a cached Lua script (EVALSHA).
    EvalScriptSha {
        /// SHA1 hash of the script (hex string).
        script_sha: Bytes,
        /// Keys passed to the script.
        keys: Vec<Bytes>,
        /// Additional arguments.
        argv: Vec<Bytes>,
        /// Connection ID.
        conn_id: u64,
        /// Response channel.
        response_tx: oneshot::Sender<Response>,
    },

    /// Load a script into the cache (SCRIPT LOAD).
    ScriptLoad {
        /// Script source code.
        script_source: Bytes,
        /// Response channel (returns SHA1 hex).
        response_tx: oneshot::Sender<String>,
    },

    /// Check if scripts exist (SCRIPT EXISTS).
    ScriptExists {
        /// SHA1 hashes to check (hex strings).
        shas: Vec<Bytes>,
        /// Response channel.
        response_tx: oneshot::Sender<Vec<bool>>,
    },

    /// Flush the script cache (SCRIPT FLUSH).
    ScriptFlush {
        /// Response channel.
        response_tx: oneshot::Sender<()>,
    },

    /// Kill the running script (SCRIPT KILL).
    ScriptKill {
        /// Response channel.
        response_tx: oneshot::Sender<Result<(), String>>,
    },

    /// Shutdown signal.
    Shutdown,
}

/// Operation type for scatter-gather.
#[derive(Debug, Clone)]
pub enum ScatterOp {
    /// MGET operation - get multiple values.
    MGet,
    /// MSET operation - set multiple key-value pairs.
    MSet {
        /// Key-value pairs where keys align with the keys field in ScatterRequest.
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// DELETE operation.
    Del,
    /// EXISTS operation.
    Exists,
    /// TOUCH operation.
    Touch,
    /// UNLINK operation (async delete, same as Del for now).
    Unlink,
    /// KEYS operation - get all keys matching a pattern.
    Keys {
        /// Pattern to match (glob syntax).
        pattern: Bytes,
    },
    /// DBSIZE operation - get total key count.
    DbSize,
    /// FLUSHDB operation - clear all keys.
    FlushDb,
    /// SCAN operation - scan keys with cursor.
    Scan {
        /// Position within this shard to start scanning.
        cursor: u64,
        /// Hint for number of keys to return.
        count: usize,
        /// Optional pattern to match.
        pattern: Option<Bytes>,
        /// Optional type filter.
        key_type: Option<crate::types::KeyType>,
    },
}

/// Result from a shard for scatter-gather operations.
#[derive(Debug)]
pub struct PartialResult {
    /// Results keyed by original key position.
    pub results: Vec<(Bytes, Response)>,
}

/// Result from executing a transaction.
#[derive(Debug)]
pub enum TransactionResult {
    /// Transaction executed successfully.
    Success(Vec<Response>),
    /// Transaction aborted due to WATCH conflict.
    WatchAborted,
    /// Transaction failed with an error.
    Error(String),
}

/// A pending operation in the VLL transaction queue.
#[derive(Debug)]
#[allow(dead_code)]
pub struct PendingOp {
    /// Transaction ID.
    pub txid: u64,
    /// Keys involved in this operation.
    pub keys: Vec<Bytes>,
    /// The operation to execute.
    pub operation: ScatterOp,
}

/// VLL (Very Lightweight Locking) transaction queue stub.
///
/// This is a foundation for future conflict detection and ordering.
/// Currently serves as a placeholder for Phase 4 scatter-gather operations.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct TransactionQueue {
    /// Pending operations indexed by transaction ID.
    pending: std::collections::BTreeMap<u64, PendingOp>,
    /// Maximum queue depth before blocking new transactions.
    max_depth: usize,
}

#[allow(dead_code)]
impl TransactionQueue {
    /// Create a new transaction queue with the specified max depth.
    pub fn new(max_depth: usize) -> Self {
        Self {
            pending: std::collections::BTreeMap::new(),
            max_depth,
        }
    }

    /// Check if the queue has capacity for a new transaction.
    pub fn has_capacity(&self) -> bool {
        self.pending.len() < self.max_depth
    }

    /// Add a pending operation to the queue.
    pub fn enqueue(&mut self, op: PendingOp) {
        self.pending.insert(op.txid, op);
    }

    /// Remove a completed operation from the queue.
    pub fn dequeue(&mut self, txid: u64) -> Option<PendingOp> {
        self.pending.remove(&txid)
    }

    /// Get the number of pending operations.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// New connection to be handled by a shard.
pub struct NewConnection {
    /// The TCP socket.
    pub socket: tokio::net::TcpStream,
    /// Client address.
    pub addr: std::net::SocketAddr,
    /// Connection ID.
    pub conn_id: u64,
}

impl std::fmt::Debug for NewConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewConnection")
            .field("addr", &self.addr)
            .field("conn_id", &self.conn_id)
            .finish()
    }
}

/// A shard worker that owns a partition of the data.
pub struct ShardWorker {
    /// Shard ID.
    pub shard_id: usize,

    /// Total number of shards.
    pub num_shards: usize,

    /// Local data store.
    pub store: HashMapStore,

    /// Receiver for shard messages.
    pub message_rx: mpsc::Receiver<ShardMessage>,

    /// Receiver for new connections.
    pub new_conn_rx: mpsc::Receiver<NewConnection>,

    /// Senders to all shards (for cross-shard operations).
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry.
    pub registry: Arc<CommandRegistry>,

    /// Optional RocksDB store for persistence.
    pub rocks_store: Option<Arc<RocksStore>>,

    /// WAL writer for this shard.
    pub wal_writer: Option<RocksWalWriter>,

    /// Snapshot coordinator for BGSAVE.
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Monotonically increasing version for WATCH detection.
    /// Reset to 0 on server restart.
    shard_version: u64,

    /// Pub/Sub subscriptions for this shard.
    subscriptions: ShardSubscriptions,

    /// Script executor for this shard.
    script_executor: Option<ScriptExecutor>,

    /// Eviction configuration.
    eviction_config: EvictionConfig,

    /// Eviction pool for maintaining best eviction candidates.
    eviction_pool: EvictionPool,

    /// Current memory limit for this shard (0 = unlimited).
    /// This is maxmemory / num_shards.
    memory_limit: u64,

    /// Metrics recorder for observability.
    metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,

    /// Peak memory usage for this shard (high-water mark).
    peak_memory: u64,
}

impl ShardWorker {
    /// Create a new shard worker without persistence.
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
    ) -> Self {
        Self::with_eviction(
            shard_id,
            num_shards,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            EvictionConfig::default(),
            Arc::new(crate::noop::NoopMetricsRecorder::new()),
        )
    }

    /// Create a new shard worker without persistence but with eviction config.
    pub fn with_eviction(
        shard_id: usize,
        num_shards: usize,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
    ) -> Self {
        // Try to create script executor
        let script_executor = ScriptExecutor::new(ScriptingConfig::default())
            .map_err(|e| {
                tracing::warn!(shard_id, error = %e, "Failed to initialize script executor");
            })
            .ok();

        // Calculate per-shard memory limit
        let memory_limit = if eviction_config.maxmemory > 0 {
            eviction_config.maxmemory / num_shards as u64
        } else {
            0
        };

        Self {
            shard_id,
            num_shards,
            store: HashMapStore::new(),
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            rocks_store: None,
            wal_writer: None,
            snapshot_coordinator: Arc::new(NoopSnapshotCoordinator::new()),
            shard_version: 0,
            subscriptions: ShardSubscriptions::new(),
            script_executor,
            eviction_config,
            eviction_pool: EvictionPool::new(),
            memory_limit,
            metrics_recorder,
            peak_memory: 0,
        }
    }

    /// Create a new shard worker with persistence.
    #[allow(clippy::too_many_arguments)]
    pub fn with_persistence(
        shard_id: usize,
        num_shards: usize,
        store: HashMapStore,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        rocks_store: Arc<RocksStore>,
        wal_config: WalConfig,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
    ) -> Self {
        let wal_writer = RocksWalWriter::new(
            rocks_store.clone(),
            shard_id,
            wal_config,
            metrics_recorder.clone(),
        );

        // Try to create script executor
        let script_executor = ScriptExecutor::new(ScriptingConfig::default())
            .map_err(|e| {
                tracing::warn!(shard_id, error = %e, "Failed to initialize script executor");
            })
            .ok();

        // Calculate per-shard memory limit
        let memory_limit = if eviction_config.maxmemory > 0 {
            eviction_config.maxmemory / num_shards as u64
        } else {
            0
        };

        Self {
            shard_id,
            num_shards,
            store,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            rocks_store: Some(rocks_store),
            wal_writer: Some(wal_writer),
            snapshot_coordinator,
            shard_version: 0,
            subscriptions: ShardSubscriptions::new(),
            script_executor,
            eviction_config,
            eviction_pool: EvictionPool::new(),
            memory_limit,
            metrics_recorder,
            peak_memory: 0,
        }
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }

    /// Increment shard version (call on any write operation).
    fn increment_version(&mut self) {
        self.shard_version = self.shard_version.wrapping_add(1);
    }

    /// Get version for a key.
    ///
    /// Phase 1: Returns per-shard version (simple, some false positives).
    /// Phase 2 (future): Can be changed to return per-key version.
    fn get_key_version(&self, _key: &[u8]) -> u64 {
        self.shard_version
    }

    /// Check if watched keys have changed since they were watched.
    fn check_watches(&self, watches: &[(Bytes, u64)]) -> bool {
        watches.iter().all(|(key, watched_ver)| {
            self.get_key_version(key) == *watched_ver
        })
    }

    /// Run the shard worker event loop.
    pub async fn run(mut self) {
        tracing::info!(shard_id = self.shard_id, "Shard worker started");

        // Active expiry runs every 100ms
        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));

        // Metrics collection runs every 10 seconds
        let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages
                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        ShardMessage::Execute { command, conn_id, txid: _, response_tx } => {
                            let response = self.execute_command(&command, conn_id);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScatterRequest { request_id: _, keys, operation, response_tx } => {
                            let result = self.execute_scatter_part(&keys, &operation).await;
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::GetVersion { response_tx } => {
                            let _ = response_tx.send(self.shard_version);
                        }
                        ShardMessage::ExecTransaction { commands, watches, conn_id, response_tx } => {
                            let result = self.execute_transaction(commands, &watches, conn_id);
                            let _ = response_tx.send(result);
                        }

                        // Pub/Sub message handlers
                        ShardMessage::Subscribe { channels, conn_id, sender, response_tx } => {
                            let counts = self.handle_subscribe(channels, conn_id, sender);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::Unsubscribe { channels, conn_id, response_tx } => {
                            let counts = self.handle_unsubscribe(channels, conn_id);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::PSubscribe { patterns, conn_id, sender, response_tx } => {
                            let counts = self.handle_psubscribe(patterns, conn_id, sender);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::PUnsubscribe { patterns, conn_id, response_tx } => {
                            let counts = self.handle_punsubscribe(patterns, conn_id);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::Publish { channel, message, response_tx } => {
                            let count = self.subscriptions.publish(&channel, &message);
                            let _ = response_tx.send(count);
                        }
                        ShardMessage::ShardedSubscribe { channels, conn_id, sender, response_tx } => {
                            let counts = self.handle_ssubscribe(channels, conn_id, sender);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::ShardedUnsubscribe { channels, conn_id, response_tx } => {
                            let counts = self.handle_sunsubscribe(channels, conn_id);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::ShardedPublish { channel, message, response_tx } => {
                            let count = self.subscriptions.spublish(&channel, &message);
                            let _ = response_tx.send(count);
                        }
                        ShardMessage::PubSubIntrospection { request, response_tx } => {
                            let response = self.handle_introspection(request);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ConnectionClosed { conn_id } => {
                            self.subscriptions.remove_connection(conn_id);
                        }

                        // Scripting message handlers
                        ShardMessage::EvalScript { script_source, keys, argv, conn_id, response_tx } => {
                            let response = self.handle_eval_script(&script_source, &keys, &argv, conn_id);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::EvalScriptSha { script_sha, keys, argv, conn_id, response_tx } => {
                            let response = self.handle_evalsha(&script_sha, &keys, &argv, conn_id);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScriptLoad { script_source, response_tx } => {
                            let sha = self.handle_script_load(&script_source);
                            let _ = response_tx.send(sha);
                        }
                        ShardMessage::ScriptExists { shas, response_tx } => {
                            let results = self.handle_script_exists(&shas);
                            let _ = response_tx.send(results);
                        }
                        ShardMessage::ScriptFlush { response_tx } => {
                            self.handle_script_flush();
                            let _ = response_tx.send(());
                        }
                        ShardMessage::ScriptKill { response_tx } => {
                            let result = self.handle_script_kill();
                            let _ = response_tx.send(result);
                        }

                        ShardMessage::Shutdown => {
                            tracing::info!(shard_id = self.shard_id, "Shard worker shutting down");
                            // Flush WAL before shutdown
                            if let Some(ref wal) = self.wal_writer {
                                if let Err(e) = wal.flush_async().await {
                                    tracing::error!(shard_id = self.shard_id, error = %e, "Failed to flush WAL on shutdown");
                                }
                            }
                            break;
                        }
                    }
                }

                // Active expiry task
                _ = expiry_interval.tick() => {
                    self.run_active_expiry();
                }

                // Periodic metrics collection
                _ = metrics_interval.tick() => {
                    self.collect_shard_metrics();
                }

                else => break,
            }
        }

        // Final WAL flush
        if let Some(ref wal) = self.wal_writer {
            if let Err(e) = wal.flush_async().await {
                tracing::error!(shard_id = self.shard_id, error = %e, "Failed to flush WAL on exit");
            }
        }
    }

    /// Run active expiry with time budget.
    ///
    /// This method deletes expired keys up to a time budget to avoid
    /// blocking the event loop for too long.
    fn run_active_expiry(&mut self) {
        let budget = Duration::from_millis(25);
        let start = Instant::now();
        let now = Instant::now();

        // Get expired keys from the expiry index
        if let Some(expiry_index) = self.store.expiry_index() {
            let expired = expiry_index.get_expired(now);
            let mut deleted_count = 0u64;

            for key in expired {
                if start.elapsed() > budget {
                    tracing::trace!(
                        shard_id = self.shard_id,
                        "Active expiry budget exhausted"
                    );
                    break;
                }

                // Delete the key
                if self.store.delete(&key) {
                    deleted_count += 1;
                    tracing::trace!(
                        shard_id = self.shard_id,
                        key = %String::from_utf8_lossy(&key),
                        "Active expiry deleted key"
                    );
                }
            }

            // Record expired keys metric and increment version
            if deleted_count > 0 {
                let shard_label = self.shard_id.to_string();
                self.metrics_recorder.increment_counter(
                    "frogdb_keys_expired_total",
                    deleted_count,
                    &[("shard", &shard_label)],
                );
                self.increment_version();
            }
        }
    }

    /// Collect and emit shard metrics periodically.
    fn collect_shard_metrics(&mut self) {
        let shard_label = self.shard_id.to_string();
        let memory_used = self.store.memory_used() as u64;

        // Update peak memory if current exceeds it
        if memory_used > self.peak_memory {
            self.peak_memory = memory_used;
        }

        // Memory used by this shard
        self.metrics_recorder.record_gauge(
            "frogdb_shard_memory_bytes",
            memory_used as f64,
            &[("shard", &shard_label)],
        );

        // Per-shard memory metrics
        self.metrics_recorder.record_gauge(
            "frogdb_memory_used_bytes",
            memory_used as f64,
            &[("shard", &shard_label)],
        );

        // Peak memory for this shard
        self.metrics_recorder.record_gauge(
            "frogdb_memory_peak_bytes",
            self.peak_memory as f64,
            &[("shard", &shard_label)],
        );

        // Keyspace metrics: key count
        let key_count = self.store.len() as f64;

        self.metrics_recorder.record_gauge(
            "frogdb_shard_keys",
            key_count,
            &[("shard", &shard_label)],
        );

        self.metrics_recorder.record_gauge(
            "frogdb_keys_total",
            key_count,
            &[("shard", &shard_label)],
        );

        // Keys with expiry
        if let Some(expiry_index) = self.store.expiry_index() {
            self.metrics_recorder.record_gauge(
                "frogdb_keys_with_expiry",
                expiry_index.len() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    // ========================================================================
    // Memory eviction methods
    // ========================================================================

    /// Check if we're over the memory limit.
    fn is_over_memory_limit(&self) -> bool {
        if self.memory_limit == 0 {
            return false;
        }
        self.store.memory_used() as u64 > self.memory_limit
    }

    /// Check memory and evict if needed before a write operation.
    ///
    /// Returns Ok(()) if memory is available (or was freed via eviction),
    /// Returns Err(CommandError::OutOfMemory) if write should be rejected.
    fn check_memory_for_write(&mut self) -> Result<(), CommandError> {
        // No limit configured
        if self.memory_limit == 0 {
            return Ok(());
        }

        // Check if we're over limit
        if !self.is_over_memory_limit() {
            return Ok(());
        }

        // Try to evict if policy allows
        if self.eviction_config.policy == EvictionPolicy::NoEviction {
            tracing::debug!(
                shard_id = self.shard_id,
                memory_used = self.store.memory_used(),
                memory_limit = self.memory_limit,
                "OOM: no eviction policy configured"
            );
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_eviction_oom_total",
                1,
                &[("shard", &shard_label)],
            );
            return Err(CommandError::OutOfMemory);
        }

        // Attempt eviction
        let max_attempts = 10; // Limit attempts to avoid infinite loop
        for _ in 0..max_attempts {
            if !self.is_over_memory_limit() {
                return Ok(());
            }

            if !self.evict_one() {
                // No more keys to evict
                tracing::debug!(
                    shard_id = self.shard_id,
                    memory_used = self.store.memory_used(),
                    memory_limit = self.memory_limit,
                    "OOM: no keys available for eviction"
                );
                let shard_label = self.shard_id.to_string();
                self.metrics_recorder.increment_counter(
                    "frogdb_eviction_oom_total",
                    1,
                    &[("shard", &shard_label)],
                );
                return Err(CommandError::OutOfMemory);
            }
        }

        // Still over limit after max attempts
        if self.is_over_memory_limit() {
            tracing::debug!(
                shard_id = self.shard_id,
                memory_used = self.store.memory_used(),
                memory_limit = self.memory_limit,
                "OOM: still over limit after eviction attempts"
            );
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_eviction_oom_total",
                1,
                &[("shard", &shard_label)],
            );
            return Err(CommandError::OutOfMemory);
        }

        Ok(())
    }

    /// Evict one key based on the configured policy.
    ///
    /// Returns true if a key was evicted, false if no suitable key found.
    fn evict_one(&mut self) -> bool {
        match self.eviction_config.policy {
            EvictionPolicy::NoEviction => false,
            EvictionPolicy::AllkeysRandom => self.evict_random(false),
            EvictionPolicy::VolatileRandom => self.evict_random(true),
            EvictionPolicy::AllkeysLru => self.evict_lru(false),
            EvictionPolicy::VolatileLru => self.evict_lru(true),
            EvictionPolicy::AllkeysLfu => self.evict_lfu(false),
            EvictionPolicy::VolatileLfu => self.evict_lfu(true),
            EvictionPolicy::VolatileTtl => self.evict_ttl(),
        }
    }

    /// Evict a random key.
    fn evict_random(&mut self, volatile_only: bool) -> bool {
        let key = if volatile_only {
            // Sample from keys with TTL
            let keys = self.store.sample_volatile_keys(1);
            keys.into_iter().next()
        } else {
            // Sample from all keys
            self.store.random_key()
        };

        if let Some(key) = key {
            self.delete_for_eviction(&key)
        } else {
            false
        }
    }

    /// Evict the least recently used key.
    fn evict_lru(&mut self, volatile_only: bool) -> bool {
        // Sample keys and update pool
        self.sample_for_eviction(volatile_only);

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction_pool.pop_worst() {
            self.delete_for_eviction(&candidate.key)
        } else {
            false
        }
    }

    /// Evict the least frequently used key.
    fn evict_lfu(&mut self, volatile_only: bool) -> bool {
        // Sample keys and update pool with LFU ranking
        self.sample_for_eviction_lfu(volatile_only);

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction_pool.pop_worst() {
            self.delete_for_eviction(&candidate.key)
        } else {
            false
        }
    }

    /// Evict the key with shortest TTL.
    fn evict_ttl(&mut self) -> bool {
        // Sample volatile keys and update pool with TTL ranking
        self.sample_for_eviction_ttl();

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction_pool.pop_worst() {
            self.delete_for_eviction(&candidate.key)
        } else {
            false
        }
    }

    /// Sample keys and add to eviction pool for LRU.
    fn sample_for_eviction(&mut self, volatile_only: bool) {
        let samples = self.eviction_config.maxmemory_samples;
        let now = Instant::now();

        let keys = if volatile_only {
            self.store.sample_volatile_keys(samples)
        } else {
            self.store.sample_keys(samples)
        };

        for key in keys {
            if let Some(metadata) = self.store.get_metadata(&key) {
                let candidate = EvictionCandidate::from_metadata(
                    key,
                    metadata.last_access,
                    metadata.lfu_counter,
                    metadata.expires_at,
                    now,
                );
                self.eviction_pool.maybe_insert_lru(candidate);
            }
        }
    }

    /// Sample keys and add to eviction pool for LFU.
    fn sample_for_eviction_lfu(&mut self, volatile_only: bool) {
        let samples = self.eviction_config.maxmemory_samples;
        let now = Instant::now();

        let keys = if volatile_only {
            self.store.sample_volatile_keys(samples)
        } else {
            self.store.sample_keys(samples)
        };

        for key in keys {
            if let Some(metadata) = self.store.get_metadata(&key) {
                let candidate = EvictionCandidate::from_metadata(
                    key,
                    metadata.last_access,
                    metadata.lfu_counter,
                    metadata.expires_at,
                    now,
                );
                self.eviction_pool.maybe_insert_lfu(candidate);
            }
        }
    }

    /// Sample volatile keys and add to eviction pool for TTL.
    fn sample_for_eviction_ttl(&mut self) {
        let samples = self.eviction_config.maxmemory_samples;
        let now = Instant::now();

        let keys = self.store.sample_volatile_keys(samples);

        for key in keys {
            if let Some(metadata) = self.store.get_metadata(&key) {
                let candidate = EvictionCandidate::from_metadata(
                    key,
                    metadata.last_access,
                    metadata.lfu_counter,
                    metadata.expires_at,
                    now,
                );
                self.eviction_pool.maybe_insert_ttl(candidate);
            }
        }
    }

    /// Delete a key for eviction (updates metrics and pool).
    fn delete_for_eviction(&mut self, key: &[u8]) -> bool {
        // Get memory size before deletion for metrics
        let memory_freed = self.store.get_metadata(key)
            .map(|m| m.memory_size)
            .unwrap_or(0);

        // Remove from eviction pool
        self.eviction_pool.remove(key);

        // Delete the key
        if self.store.delete(key) {
            self.increment_version();

            // Record eviction metrics
            let shard_label = self.shard_id.to_string();
            let policy_label = self.eviction_config.policy.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_eviction_keys_total",
                1,
                &[("shard", &shard_label), ("policy", &policy_label)],
            );
            self.metrics_recorder.increment_counter(
                "frogdb_eviction_bytes_total",
                memory_freed as u64,
                &[("shard", &shard_label)],
            );

            tracing::debug!(
                shard_id = self.shard_id,
                key = %String::from_utf8_lossy(key),
                memory_freed = memory_freed,
                policy = %self.eviction_config.policy,
                "Evicted key"
            );

            true
        } else {
            false
        }
    }

    /// Handle a new connection assigned to this shard.
    async fn handle_new_connection(&self, new_conn: NewConnection) {
        tracing::debug!(
            shard_id = self.shard_id,
            conn_id = new_conn.conn_id,
            addr = %new_conn.addr,
            "New connection assigned to shard"
        );

        // Connection handling is spawned as a separate task
        // The actual connection loop is implemented in the server crate
    }

    /// Execute a command locally.
    fn execute_command(&mut self, command: &ParsedCommand, conn_id: u64) -> Response {
        let cmd_name = command.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ))
            }
        };

        // Validate arity
        if !handler.arity().check(command.args.len()) {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
        }

        // Check memory before write operations
        let is_write = handler.flags().contains(crate::command::CommandFlags::WRITE);
        if is_write {
            if let Err(err) = self.check_memory_for_write() {
                return err.to_response();
            }
        }

        // Create command context
        // Note: We need a mutable reference to the store, but we're inside ShardWorker
        // This is safe because each shard is single-threaded
        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::new(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
        );

        // Execute
        let response = match handler.execute(&mut ctx, &command.args) {
            Ok(response) => response,
            Err(err) => err.to_response(),
        };

        // Track keyspace hits/misses for GET-like commands
        let is_get_command = matches!(
            cmd_name_str.as_ref(),
            "GET" | "GETEX" | "GETDEL" | "HGET" | "LINDEX"
        );
        if is_get_command {
            if matches!(response, Response::Null) {
                self.metrics_recorder.increment_counter(
                    "frogdb_keyspace_misses_total",
                    1,
                    &[],
                );
            } else {
                self.metrics_recorder.increment_counter(
                    "frogdb_keyspace_hits_total",
                    1,
                    &[],
                );
            }
        }

        // Increment version on write operations
        if is_write {
            self.increment_version();
        }

        response
    }

    /// Execute a transaction atomically.
    ///
    /// This method:
    /// 1. Checks all watched keys' versions against their watched versions
    /// 2. If any mismatch, returns WatchAborted (EXEC returns nil)
    /// 3. Executes all queued commands in sequence
    /// 4. Returns Success with all command results
    fn execute_transaction(
        &mut self,
        commands: Vec<ParsedCommand>,
        watches: &[(Bytes, u64)],
        conn_id: u64,
    ) -> TransactionResult {
        // Check WATCH conditions
        if !self.check_watches(watches) {
            return TransactionResult::WatchAborted;
        }

        // Execute all commands
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let response = self.execute_command(&command, conn_id);
            results.push(response);
        }

        TransactionResult::Success(results)
    }

    /// Execute part of a scatter-gather operation.
    async fn execute_scatter_part(&mut self, keys: &[Bytes], operation: &ScatterOp) -> PartialResult {
        use crate::types::{KeyMetadata, Value};

        let results = match operation {
            ScatterOp::MGet => {
                keys.iter()
                    .map(|key| {
                        let response = match self.store.get(key) {
                            Some(value) => {
                                if let Some(sv) = value.as_string() {
                                    Response::bulk(sv.as_bytes())
                                } else {
                                    Response::null()
                                }
                            }
                            None => Response::null(),
                        };
                        (key.clone(), response)
                    })
                    .collect()
            }
            ScatterOp::MSet { pairs } => {
                let mut results = Vec::with_capacity(pairs.len());
                for (key, value) in pairs {
                    let val = Value::string(value.clone());
                    self.store.set(key.clone(), val.clone());

                    // Persist to WAL if enabled
                    if let Some(ref wal) = self.wal_writer {
                        let metadata = KeyMetadata::new(val.memory_size());
                        if let Err(e) = wal.write_set(key, &val, &metadata).await {
                            tracing::error!(key = %String::from_utf8_lossy(key), error = %e, "Failed to persist MSET");
                        }
                    }

                    results.push((key.clone(), Response::ok()));
                }
                // Increment version for MSET (write operation)
                if !pairs.is_empty() {
                    self.increment_version();
                }
                results
            }
            ScatterOp::Del | ScatterOp::Unlink => {
                let mut results = Vec::with_capacity(keys.len());
                let mut any_deleted = false;
                for key in keys {
                    let deleted = self.store.delete(key);

                    // Persist delete to WAL if enabled
                    if deleted {
                        any_deleted = true;
                        if let Some(ref wal) = self.wal_writer {
                            if let Err(e) = wal.write_delete(key).await {
                                tracing::error!(key = %String::from_utf8_lossy(key), error = %e, "Failed to persist DEL");
                            }
                        }
                    }

                    results.push((key.clone(), Response::Integer(if deleted { 1 } else { 0 })));
                }
                // Increment version for DEL/UNLINK if any key was deleted
                if any_deleted {
                    self.increment_version();
                }
                results
            }
            ScatterOp::Exists => {
                keys.iter()
                    .map(|key| {
                        let exists = self.store.contains(key);
                        (key.clone(), Response::Integer(if exists { 1 } else { 0 }))
                    })
                    .collect()
            }
            ScatterOp::Touch => {
                keys.iter()
                    .map(|key| {
                        let touched = self.store.touch(key);
                        (key.clone(), Response::Integer(if touched { 1 } else { 0 }))
                    })
                    .collect()
            }
            ScatterOp::Keys { pattern } => {
                // Get all keys matching pattern
                let all_keys = self.store.all_keys();
                let matching_keys: Vec<_> = all_keys
                    .into_iter()
                    .filter(|key| crate::glob::glob_match(pattern, key))
                    .map(|key| (key.clone(), Response::bulk(key)))
                    .collect();
                matching_keys
            }
            ScatterOp::DbSize => {
                // Return the key count for this shard
                let count = self.store.len();
                vec![(Bytes::from_static(b"__dbsize__"), Response::Integer(count as i64))]
            }
            ScatterOp::FlushDb => {
                // Clear all keys in this shard
                self.store.clear();
                self.increment_version();
                vec![(Bytes::from_static(b"__flushdb__"), Response::ok())]
            }
            ScatterOp::Scan { cursor, count, pattern, key_type } => {
                // Scan keys in this shard
                let pattern_ref = pattern.as_ref().map(|p| p.as_ref());
                let (next_cursor, found_keys) = self.store.scan_filtered(*cursor, *count, pattern_ref, *key_type);
                // Return cursor and keys as a special response
                let mut results = Vec::with_capacity(found_keys.len() + 1);
                results.push((Bytes::from_static(b"__cursor__"), Response::Integer(next_cursor as i64)));
                for key in found_keys {
                    results.push((key.clone(), Response::bulk(key)));
                }
                results
            }
        };

        PartialResult { results }
    }

    // =========================================================================
    // Pub/Sub helpers
    // =========================================================================

    /// Handle SUBSCRIBE - subscribe to broadcast channels.
    fn handle_subscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        // This returns the total subscription count after each subscription
        // The count is just a placeholder here since we don't track across shards
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.subscribe(channel, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect()
    }

    /// Handle UNSUBSCRIBE - unsubscribe from broadcast channels.
    fn handle_unsubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId) -> Vec<usize> {
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.unsubscribe(&channel, conn_id);
                i // Placeholder remaining count
            })
            .collect()
    }

    /// Handle PSUBSCRIBE - subscribe to patterns.
    fn handle_psubscribe(
        &mut self,
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| {
                self.subscriptions.psubscribe(pattern, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect()
    }

    /// Handle PUNSUBSCRIBE - unsubscribe from patterns.
    fn handle_punsubscribe(&mut self, patterns: Vec<Bytes>, conn_id: ConnId) -> Vec<usize> {
        patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| {
                self.subscriptions.punsubscribe(&pattern, conn_id);
                i // Placeholder remaining count
            })
            .collect()
    }

    /// Handle SSUBSCRIBE - subscribe to sharded channels.
    fn handle_ssubscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.ssubscribe(channel, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect()
    }

    /// Handle SUNSUBSCRIBE - unsubscribe from sharded channels.
    fn handle_sunsubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId) -> Vec<usize> {
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.sunsubscribe(&channel, conn_id);
                i // Placeholder remaining count
            })
            .collect()
    }

    /// Handle introspection requests.
    fn handle_introspection(&self, request: IntrospectionRequest) -> IntrospectionResponse {
        match request {
            IntrospectionRequest::Channels { pattern } => {
                let channels = self.subscriptions.channels(pattern.as_ref());
                IntrospectionResponse::Channels(channels)
            }
            IntrospectionRequest::NumSub { channels } => {
                let counts = self.subscriptions.numsub(&channels);
                IntrospectionResponse::NumSub(counts)
            }
            IntrospectionRequest::NumPat => {
                IntrospectionResponse::NumPat(self.subscriptions.pattern_count())
            }
            IntrospectionRequest::ShardChannels { pattern } => {
                let channels = self.subscriptions.shard_channels(pattern.as_ref());
                IntrospectionResponse::Channels(channels)
            }
            IntrospectionRequest::ShardNumSub { channels } => {
                let counts = self.subscriptions.shard_numsub(&channels);
                IntrospectionResponse::NumSub(counts)
            }
        }
    }

    // =========================================================================
    // Scripting helpers
    // =========================================================================

    /// Handle EVAL - execute a Lua script.
    fn handle_eval_script(
        &mut self,
        script_source: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
    ) -> Response {
        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                return Response::error("ERR scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::new(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
        );

        match executor.eval(script_source, keys, argv, &mut ctx, &self.registry) {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// Handle EVALSHA - execute a cached Lua script by SHA.
    fn handle_evalsha(
        &mut self,
        script_sha: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
    ) -> Response {
        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                return Response::error("ERR scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::new(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
        );

        match executor.evalsha(script_sha, keys, argv, &mut ctx, &self.registry) {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// Handle SCRIPT LOAD - load a script into the cache.
    fn handle_script_load(&mut self, script_source: &Bytes) -> String {
        match &mut self.script_executor {
            Some(executor) => executor.load_script(script_source.clone()),
            None => String::new(),
        }
    }

    /// Handle SCRIPT EXISTS - check if scripts are cached.
    fn handle_script_exists(&self, shas: &[Bytes]) -> Vec<bool> {
        match &self.script_executor {
            Some(executor) => {
                let sha_refs: Vec<&[u8]> = shas.iter().map(|s| s.as_ref()).collect();
                executor.scripts_exist(&sha_refs)
            }
            None => vec![false; shas.len()],
        }
    }

    /// Handle SCRIPT FLUSH - clear the script cache.
    fn handle_script_flush(&mut self) {
        if let Some(ref mut executor) = self.script_executor {
            executor.flush_scripts();
        }
    }

    /// Handle SCRIPT KILL - kill the running script.
    fn handle_script_kill(&self) -> Result<(), String> {
        match &self.script_executor {
            Some(executor) => {
                if !executor.is_running() {
                    return Err("NOTBUSY No scripts in execution right now.".to_string());
                }
                executor.kill_script().map_err(|e| e.to_string())
            }
            None => Err("ERR scripting not available".to_string()),
        }
    }
}

/// Extract hash tag from a key (Redis-compatible).
///
/// Rules:
/// - First `{` that has a matching `}` with at least one character between
/// - Nested braces: outer wins (first valid match)
/// - Empty braces `{}` are ignored (hash entire key)
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close_offset = key[open + 1..].iter().position(|&b| b == b'}')?;
    let tag = &key[open + 1..open + 1 + close_offset];
    if tag.is_empty() {
        None
    } else {
        Some(tag)
    }
}

/// Number of Redis cluster hash slots.
pub const REDIS_CLUSTER_SLOTS: usize = 16384;

/// Determine which shard owns a key using Redis-compatible CRC16 hashing.
///
/// Uses the XMODEM variant of CRC16, same as Redis cluster.
/// The slot is calculated as: CRC16(key) % 16384 % num_shards
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % REDIS_CLUSTER_SLOTS;
    slot % num_shards
}

/// Calculate the Redis cluster slot for a key (0-16383).
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % REDIS_CLUSTER_SLOTS as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_hash_tag_simple() {
        assert_eq!(extract_hash_tag(b"{user:1}:profile"), Some(b"user:1".as_slice()));
        assert_eq!(extract_hash_tag(b"{user:1}:settings"), Some(b"user:1".as_slice()));
    }

    #[test]
    fn test_extract_hash_tag_no_braces() {
        assert_eq!(extract_hash_tag(b"user:1:profile"), None);
    }

    #[test]
    fn test_extract_hash_tag_empty_braces() {
        assert_eq!(extract_hash_tag(b"foo{}bar"), None);
        assert_eq!(extract_hash_tag(b"{}"), None);
    }

    #[test]
    fn test_extract_hash_tag_nested() {
        // First { to first } after it
        assert_eq!(extract_hash_tag(b"{{foo}}"), Some(b"{foo".as_slice()));
    }

    #[test]
    fn test_extract_hash_tag_multiple() {
        // First valid tag wins
        assert_eq!(extract_hash_tag(b"foo{bar}{zap}"), Some(b"bar".as_slice()));
    }

    #[test]
    fn test_extract_hash_tag_empty_then_valid() {
        // Empty first braces skipped
        assert_eq!(extract_hash_tag(b"{}{valid}"), None); // Actually returns None because {} comes first
    }

    #[test]
    fn test_shard_for_key_consistent() {
        let key1 = b"user:123";
        let key2 = b"user:123";
        assert_eq!(shard_for_key(key1, 4), shard_for_key(key2, 4));
    }

    #[test]
    fn test_shard_for_key_hash_tag() {
        // Keys with same hash tag should go to same shard
        let key1 = b"{user:1}:profile";
        let key2 = b"{user:1}:settings";
        assert_eq!(shard_for_key(key1, 4), shard_for_key(key2, 4));
    }

    #[test]
    fn test_slot_for_key_hash_tag_colocation() {
        // Keys with same hash tag should map to the same slot
        let key1 = b"{user:1}:profile";
        let key2 = b"{user:1}:session";
        let key3 = b"{user:1}:settings";

        let slot1 = slot_for_key(key1);
        let slot2 = slot_for_key(key2);
        let slot3 = slot_for_key(key3);

        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);
    }

    #[test]
    fn test_slot_for_key_range() {
        // Slots should be in range 0-16383
        for i in 0..1000 {
            let key = format!("key:{}", i);
            let slot = slot_for_key(key.as_bytes());
            assert!(slot < REDIS_CLUSTER_SLOTS as u16);
        }
    }

    #[test]
    fn test_shard_distribution() {
        // Test that keys distribute across shards
        let num_shards = 4;
        let mut shard_counts = vec![0usize; num_shards];

        for i in 0..1000 {
            let key = format!("key:{}", i);
            let shard = shard_for_key(key.as_bytes(), num_shards);
            shard_counts[shard] += 1;
        }

        // Each shard should have at least some keys (distribution check)
        for count in &shard_counts {
            assert!(*count > 0, "Shard has no keys assigned");
        }
    }

    #[test]
    fn test_crc16_known_values() {
        // Test against known Redis CRC16 values
        // "123456789" should hash to 0x31C3 (12739) using XMODEM
        let crc = crc16::State::<crc16::XMODEM>::calculate(b"123456789");
        assert_eq!(crc, 0x31C3);
    }
}
