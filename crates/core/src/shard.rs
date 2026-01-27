//! Shard infrastructure for shared-nothing concurrency.
//!
//! This module provides the [`ShardWorker`] which manages a single data shard
//! in FrogDB's shared-nothing architecture. Each shard handles a portion of
//! the key space and processes commands independently.
//!
//! # Dependency Groups
//!
//! Dependencies can be organized into logical groups for cleaner construction:
//! - [`ShardCoreDeps`] - Essential dependencies for command execution
//! - [`ShardPersistenceDeps`] - Dependencies for persistence (optional)
//! - [`ShardClusterDeps`] - Dependencies for cluster mode (optional)
//!
//! # Builder Pattern
//!
//! Use [`ShardWorkerBuilder`] for a fluent construction API:
//!
//! ```rust,ignore
//! let worker = ShardWorkerBuilder::new(shard_id, num_shards, message_rx, new_conn_rx)
//!     .with_registry(registry)
//!     .with_shard_senders(shard_senders)
//!     .with_eviction(eviction_config)
//!     .with_persistence(rocks_store, wal_writer)
//!     .build();
//! ```

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::sync::RwLockExt;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::{mpsc, oneshot};

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::{CommandContext, QuorumChecker};
use crate::error::CommandError;
use crate::eviction::{EvictionCandidate, EvictionConfig, EvictionPolicy, EvictionPool};
use crate::functions::SharedFunctionRegistry;
use crate::persistence::{
    NoopSnapshotCoordinator, RocksStore, RocksWalWriter, SnapshotCoordinator, WalConfig,
};
use crate::pubsub::{
    ConnId, IntrospectionRequest, IntrospectionResponse, PubSubSender, ShardSubscriptions,
};
use crate::latency::{LatencyEvent, LatencyMonitor, LatencySample};
use crate::registry::CommandRegistry;
use crate::replication::{NoopBroadcaster, SharedBroadcaster};
use crate::scripting::{ScriptExecutor, ScriptingConfig};
use crate::slowlog::{SlowLog, SlowLogEntry};
use crate::store::{HashMapStore, Store};
use crate::vll::{ExecuteSignal, LockMode, ShardReadyResult};

// ============================================================================
// Dependency Groups for ShardWorker
// ============================================================================

/// Core dependencies required for shard operation.
#[derive(Clone)]
pub struct ShardCoreDeps {
    /// Senders to all shards for cross-shard operations.
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry for looking up command implementations.
    pub registry: Arc<CommandRegistry>,

    /// Metrics recorder for observability.
    pub metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,

    /// Slowlog next ID counter (shared across shards).
    pub slowlog_next_id: Arc<AtomicU64>,

    /// Replication broadcaster for propagating writes.
    pub replication_broadcaster: SharedBroadcaster,
}

/// Dependencies for persistence (optional).
#[derive(Clone, Default)]
pub struct ShardPersistenceDeps {
    /// RocksDB store for persistence.
    pub rocks_store: Option<Arc<RocksStore>>,

    /// Snapshot coordinator for BGSAVE operations.
    pub snapshot_coordinator: Option<Arc<dyn SnapshotCoordinator>>,
}

/// Dependencies for cluster mode (optional).
#[derive(Clone, Default)]
pub struct ShardClusterDeps {
    /// Cluster state with slot assignments.
    pub cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID in the cluster.
    pub node_id: Option<u64>,

    /// Raft instance for consensus operations.
    pub raft: Option<Arc<ClusterRaft>>,

    /// Network factory for cluster communications.
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Quorum checker for cluster health.
    pub quorum_checker: Option<Arc<dyn QuorumChecker>>,
}

impl ShardClusterDeps {
    /// Create empty cluster deps (standalone mode).
    pub fn standalone() -> Self {
        Self::default()
    }

    /// Check if cluster mode is enabled.
    pub fn is_cluster_mode(&self) -> bool {
        self.cluster_state.is_some()
    }
}

/// Configuration for shard behavior.
#[derive(Clone)]
pub struct ShardConfig {
    /// Eviction configuration.
    pub eviction: EvictionConfig,

    /// Scripting configuration.
    pub scripting: ScriptingConfig,

    /// Enable VLL (Virtual Lock Loom) for transaction coordination.
    pub enable_vll: bool,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            eviction: EvictionConfig::default(),
            scripting: ScriptingConfig::default(),
            enable_vll: false,
        }
    }
}

/// Number of 1-second buckets to keep for operation tracking (60 seconds).
const OPERATION_BUCKET_COUNT: usize = 60;

/// A single 1-second bucket of operation counts.
#[derive(Debug, Clone, Copy, Default)]
pub struct OperationBucket {
    /// Unix timestamp (seconds) for this bucket.
    pub timestamp: u64,
    /// Total operations in this second.
    pub total_ops: u64,
    /// Read operations in this second.
    pub read_ops: u64,
    /// Write operations in this second.
    pub write_ops: u64,
}

/// Windowed operation counters for hot shard detection.
/// Uses a ring buffer of 1-second buckets.
#[derive(Debug)]
pub struct OperationCounters {
    /// Ring buffer of 1-second buckets.
    buckets: [OperationBucket; OPERATION_BUCKET_COUNT],
    /// Current bucket index.
    current_index: usize,
    /// Timestamp of current bucket.
    current_second: u64,
}

impl Default for OperationCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationCounters {
    /// Create new operation counters.
    pub fn new() -> Self {
        Self {
            buckets: [OperationBucket::default(); OPERATION_BUCKET_COUNT],
            current_index: 0,
            current_second: 0,
        }
    }

    /// Get current unix timestamp in seconds.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Advance to current second, rolling over buckets as needed.
    fn advance_to_now(&mut self) {
        let now = Self::current_timestamp();

        if self.current_second == 0 {
            // First call - initialize
            self.current_second = now;
            self.buckets[self.current_index].timestamp = now;
            return;
        }

        // Advance buckets if time has passed
        while self.current_second < now {
            self.current_second += 1;
            self.current_index = (self.current_index + 1) % OPERATION_BUCKET_COUNT;
            // Clear the new bucket
            self.buckets[self.current_index] = OperationBucket {
                timestamp: self.current_second,
                ..Default::default()
            };
        }
    }

    /// Record an operation.
    pub fn record_op(&mut self, is_read: bool, is_write: bool) {
        self.advance_to_now();
        let bucket = &mut self.buckets[self.current_index];
        bucket.total_ops += 1;
        if is_read {
            bucket.read_ops += 1;
        }
        if is_write {
            bucket.write_ops += 1;
        }
    }

    /// Calculate ops/sec for the given period.
    /// Returns (total_ops_per_sec, read_ops_per_sec, write_ops_per_sec).
    pub fn calculate_ops_per_sec(&mut self, period_secs: u64) -> (f64, f64, f64) {
        self.advance_to_now();

        let period_secs = period_secs.min(OPERATION_BUCKET_COUNT as u64);
        if period_secs == 0 {
            return (0.0, 0.0, 0.0);
        }

        let now = self.current_second;
        let cutoff = now.saturating_sub(period_secs);

        let mut total_ops: u64 = 0;
        let mut read_ops: u64 = 0;
        let mut write_ops: u64 = 0;

        for bucket in &self.buckets {
            if bucket.timestamp > cutoff && bucket.timestamp <= now {
                total_ops += bucket.total_ops;
                read_ops += bucket.read_ops;
                write_ops += bucket.write_ops;
            }
        }

        let period = period_secs as f64;
        (
            total_ops as f64 / period,
            read_ops as f64 / period,
            write_ops as f64 / period,
        )
    }
}

/// Response for hot shard stats query.
#[derive(Debug, Clone)]
pub struct HotShardStatsResponse {
    /// Shard ID.
    pub shard_id: usize,
    /// Total operations per second.
    pub ops_per_sec: f64,
    /// Read operations per second.
    pub reads_per_sec: f64,
    /// Write operations per second.
    pub writes_per_sec: f64,
    /// Current queue depth.
    pub queue_depth: usize,
}

/// Messages sent to shard workers.
#[derive(Debug)]
pub enum ShardMessage {
    /// Execute a command on this shard.
    Execute {
        command: ParsedCommand,
        conn_id: u64,
        /// Transaction ID for VLL ordering (optional for single-shard operations).
        txid: Option<u64>,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        response_tx: oneshot::Sender<Response>,
    },

    /// Scatter-gather: partial request for multi-key operation.
    ScatterRequest {
        request_id: u64,
        keys: Vec<Bytes>,
        operation: ScatterOp,
        /// Connection ID for access control during continuation locks.
        conn_id: u64,
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
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
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
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
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
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
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

    // =========================================================================
    // Function messages
    // =========================================================================

    /// Execute a function (FCALL).
    FunctionCall {
        /// Function name.
        function_name: Bytes,
        /// Keys passed to the function.
        keys: Vec<Bytes>,
        /// Additional arguments.
        argv: Vec<Bytes>,
        /// Connection ID.
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        /// Whether this is a read-only call (FCALL_RO).
        read_only: bool,
        /// Response channel.
        response_tx: oneshot::Sender<Response>,
    },

    // =========================================================================
    // Blocking commands messages
    // =========================================================================

    /// Register a blocking wait for keys.
    BlockWait {
        /// Connection ID of the blocked client.
        conn_id: u64,
        /// Keys to wait on.
        keys: Vec<Bytes>,
        /// The blocking operation type.
        op: crate::types::BlockingOp,
        /// Channel to send the response when data is available.
        response_tx: oneshot::Sender<Response>,
        /// Deadline for the blocking operation (None = indefinite).
        deadline: Option<Instant>,
    },

    /// Cancel a blocking wait (timeout or disconnect).
    UnregisterWait {
        /// Connection ID to unregister.
        conn_id: u64,
    },

    // =========================================================================
    // Slowlog messages
    // =========================================================================

    /// Get slow query log entries from this shard.
    SlowlogGet {
        /// Maximum number of entries to return.
        count: usize,
        /// Response channel.
        response_tx: oneshot::Sender<Vec<SlowLogEntry>>,
    },

    /// Get the number of slowlog entries in this shard.
    SlowlogLen {
        /// Response channel.
        response_tx: oneshot::Sender<usize>,
    },

    /// Reset (clear) the slowlog for this shard.
    SlowlogReset {
        /// Response channel.
        response_tx: oneshot::Sender<()>,
    },

    /// Add a slow query entry to this shard's log.
    SlowlogAdd {
        /// Duration in microseconds.
        duration_us: u64,
        /// Command name and arguments.
        command: Vec<Bytes>,
        /// Client address.
        client_addr: String,
        /// Client name.
        client_name: String,
    },

    // =========================================================================
    // Memory messages
    // =========================================================================

    /// Get memory usage for a specific key.
    MemoryUsage {
        /// Key to check.
        key: Bytes,
        /// Number of nested samples for complex structures (optional).
        samples: Option<usize>,
        /// Response channel.
        response_tx: oneshot::Sender<Option<usize>>,
    },

    /// Get memory statistics from this shard.
    MemoryStats {
        /// Response channel.
        response_tx: oneshot::Sender<ShardMemoryStats>,
    },

    /// Get WAL lag statistics from this shard.
    WalLagStats {
        /// Response channel.
        response_tx: oneshot::Sender<WalLagStatsResponse>,
    },

    /// Scan for big keys (keys larger than threshold).
    ScanBigKeys {
        /// Minimum size in bytes to consider a key "big".
        threshold_bytes: usize,
        /// Maximum number of big keys to return.
        max_keys: usize,
        /// Response channel.
        response_tx: oneshot::Sender<BigKeysScanResponse>,
    },

    // =========================================================================
    // Latency messages
    // =========================================================================

    /// Get the latest latency sample for each event type.
    LatencyLatest {
        /// Response channel.
        response_tx: oneshot::Sender<Vec<(LatencyEvent, LatencySample)>>,
    },

    /// Get latency history for a specific event type.
    LatencyHistory {
        /// Event type to query.
        event: LatencyEvent,
        /// Response channel.
        response_tx: oneshot::Sender<Vec<LatencySample>>,
    },

    /// Reset latency data for specific events (or all if empty).
    LatencyReset {
        /// Events to reset (empty = all).
        events: Vec<LatencyEvent>,
        /// Response channel.
        response_tx: oneshot::Sender<()>,
    },

    // =========================================================================
    // Hot shard messages
    // =========================================================================

    /// Get hot shard statistics from this shard.
    HotShardStats {
        /// How many seconds of data to include (1-60).
        period_secs: u64,
        /// Response channel.
        response_tx: oneshot::Sender<HotShardStatsResponse>,
    },

    /// Update shard configuration at runtime.
    UpdateConfig {
        /// New eviction configuration (if changed).
        eviction_config: Option<EvictionConfig>,
        /// Response channel to acknowledge the update.
        response_tx: oneshot::Sender<()>,
    },

    // =========================================================================
    // VLL (Very Lightweight Locking) messages
    // =========================================================================

    /// VLL lock request - declare intents and acquire locks.
    VllLockRequest {
        /// Transaction ID for ordering.
        txid: u64,
        /// Keys to lock on this shard.
        keys: Vec<Bytes>,
        /// Lock mode (read or write).
        mode: LockMode,
        /// The operation to execute after locks are acquired.
        operation: ScatterOp,
        /// Channel to notify coordinator when ready.
        ready_tx: oneshot::Sender<ShardReadyResult>,
        /// Channel to receive execute signal from coordinator.
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    },

    /// VLL execute - execute a previously locked operation.
    VllExecute {
        /// Transaction ID.
        txid: u64,
        /// Response channel for the result.
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// VLL abort - release locks and cleanup for a failed operation.
    VllAbort {
        /// Transaction ID to abort.
        txid: u64,
    },

    /// VLL continuation lock - acquire full shard lock for MULTI/EXEC or Lua.
    VllContinuationLock {
        /// Transaction ID.
        txid: u64,
        /// Connection ID that owns this lock.
        conn_id: u64,
        /// Channel to notify coordinator when ready.
        ready_tx: oneshot::Sender<ShardReadyResult>,
        /// Channel to receive release signal.
        release_rx: oneshot::Receiver<()>,
    },

    // =========================================================================
    // Cluster / Raft messages
    // =========================================================================

    /// Execute a Raft command asynchronously.
    /// Used by cluster commands (CLUSTER MEET, CLUSTER FORGET, etc.) that need
    /// to call async Raft operations from synchronous command handlers.
    /// The shard worker uses its own Raft reference (set via set_raft()).
    RaftCommand {
        /// The Raft command to execute.
        cmd: crate::cluster::ClusterCommand,
        /// Response channel for the result.
        response_tx: oneshot::Sender<Result<(), String>>,
    },

    /// Get VLL queue information from this shard.
    GetVllQueueInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<VllQueueInfo>,
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
    /// COPY operation - retrieve value and expiry from source key for cross-shard copy.
    Copy {
        /// The source key to copy from.
        source_key: Bytes,
    },
    /// COPY set operation - write a value from cross-shard copy to destination key.
    CopySet {
        /// The destination key to write to.
        dest_key: Bytes,
        /// The value type (e.g., "string", "hash", "list", "set", "zset", "hll", "json").
        value_type: Bytes,
        /// The serialized value data.
        value_data: Bytes,
        /// TTL in milliseconds (None = no expiry).
        expiry_ms: Option<i64>,
        /// Whether to replace existing key.
        replace: bool,
    },
    /// RANDOMKEY operation - get a random key from the shard.
    RandomKey,
    /// DUMP operation for MIGRATE - serialize keys with full metadata.
    /// Returns serialized data compatible with Redis RESTORE command.
    Dump,
}

/// Result from a shard for scatter-gather operations.
#[derive(Debug)]
pub struct PartialResult {
    /// Results keyed by original key position.
    pub results: Vec<(Bytes, Response)>,
}

/// Memory statistics for a single shard.
#[derive(Debug, Clone, Default)]
pub struct ShardMemoryStats {
    /// Shard identifier.
    pub shard_id: usize,
    /// Total memory used by data (bytes).
    pub data_memory: usize,
    /// Number of keys in the shard.
    pub keys: usize,
    /// Peak memory usage (high-water mark).
    pub peak_memory: u64,
    /// Memory limit for this shard (0 = unlimited).
    pub memory_limit: u64,
    /// Overhead estimate (allocator, metadata, etc).
    pub overhead_estimate: usize,
}

/// Information about a large key.
#[derive(Debug, Clone)]
pub struct BigKeyInfo {
    /// The key name.
    pub key: Bytes,
    /// Type of the value (e.g., "string", "hash", "list").
    pub key_type: String,
    /// Memory usage in bytes.
    pub memory_bytes: usize,
}

/// Response from big key scanning.
#[derive(Debug, Clone, Default)]
pub struct BigKeysScanResponse {
    /// Shard identifier.
    pub shard_id: usize,
    /// List of big keys found.
    pub big_keys: Vec<BigKeyInfo>,
    /// Total number of keys scanned.
    pub keys_scanned: usize,
    /// Whether the scan was truncated due to max_keys limit.
    pub truncated: bool,
}

/// Response for WAL lag statistics query.
#[derive(Debug, Clone)]
pub struct WalLagStatsResponse {
    /// Shard identifier.
    pub shard_id: usize,
    /// Whether persistence is enabled for this shard.
    pub persistence_enabled: bool,
    /// Lag statistics (None if persistence is disabled).
    pub lag_stats: Option<crate::persistence::WalLagStats>,
}

impl Default for WalLagStatsResponse {
    fn default() -> Self {
        Self {
            shard_id: 0,
            persistence_enabled: false,
            lag_stats: None,
        }
    }
}

/// Response for VLL queue info query.
#[derive(Debug, Clone, Default)]
pub struct VllQueueInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Number of pending operations in the queue.
    pub queue_depth: usize,
    /// Transaction ID currently executing (if any).
    pub executing_txid: Option<u64>,
    /// Continuation lock info (if held).
    pub continuation_lock: Option<VllContinuationLockInfo>,
    /// Pending operations in the queue.
    pub pending_ops: Vec<VllPendingOpInfo>,
    /// Intent table state.
    pub intent_table: Vec<VllKeyIntentInfo>,
}

/// Information about a pending VLL operation.
#[derive(Debug, Clone)]
pub struct VllPendingOpInfo {
    /// Transaction ID.
    pub txid: u64,
    /// Operation type as string.
    pub operation: String,
    /// Number of keys involved.
    pub key_count: usize,
    /// Current state.
    pub state: String,
    /// Age in milliseconds.
    pub age_ms: u64,
}

/// Information about a continuation lock.
#[derive(Debug, Clone)]
pub struct VllContinuationLockInfo {
    /// Transaction ID holding the lock.
    pub txid: u64,
    /// Connection ID that owns the lock.
    pub conn_id: u64,
    /// Age in milliseconds.
    pub age_ms: u64,
}

/// Information about key intents.
#[derive(Debug, Clone)]
pub struct VllKeyIntentInfo {
    /// Key (may be truncated).
    pub key: String,
    /// Transaction IDs with intents on this key.
    pub txids: Vec<u64>,
    /// Lock state as string.
    pub lock_state: String,
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

// ============================================================================
// Blocking commands wait queue infrastructure
// ============================================================================

use crate::types::{BlockingOp, Direction};
use std::collections::{HashMap, VecDeque};

/// Entry in the wait queue for blocking commands.
pub struct WaitEntry {
    /// Connection ID of the blocked client.
    pub conn_id: u64,
    /// Keys the client is waiting on.
    pub keys: Vec<Bytes>,
    /// The blocking operation type.
    pub op: BlockingOp,
    /// Channel to send the response when data is available.
    pub response_tx: oneshot::Sender<Response>,
    /// Deadline for the blocking operation (None = indefinite).
    pub deadline: Option<Instant>,
}

impl std::fmt::Debug for WaitEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitEntry")
            .field("conn_id", &self.conn_id)
            .field("keys", &self.keys)
            .field("op", &self.op)
            .field("deadline", &self.deadline)
            .finish()
    }
}

/// Per-shard wait queue for blocked connections.
///
/// Maintains FIFO ordering per key - when a key gets data, the oldest
/// waiter for that key is satisfied first.
#[derive(Default)]
pub struct ShardWaitQueue {
    /// Waiters indexed by key. Each key maps to a list of entry indices (FIFO).
    waiters_by_key: HashMap<Bytes, VecDeque<usize>>,
    /// All wait entries indexed for O(1) access.
    entries: Vec<Option<WaitEntry>>,
    /// Free list of entry slot indices for reuse.
    free_slots: Vec<usize>,
    /// Index from conn_id to entry indices (for cleanup on disconnect).
    conn_entries: HashMap<u64, Vec<usize>>,
    /// Current number of active waiters.
    waiter_count: usize,
    /// Maximum waiters per key (0 = unlimited).
    max_waiters_per_key: usize,
    /// Maximum total blocked connections (0 = unlimited).
    max_blocked_connections: usize,
}

impl ShardWaitQueue {
    /// Create a new wait queue with default limits.
    pub fn new() -> Self {
        Self::with_limits(10000, 50000)
    }

    /// Create a new wait queue with specific limits.
    pub fn with_limits(max_waiters_per_key: usize, max_blocked_connections: usize) -> Self {
        Self {
            waiters_by_key: HashMap::new(),
            entries: Vec::new(),
            free_slots: Vec::new(),
            conn_entries: HashMap::new(),
            waiter_count: 0,
            max_waiters_per_key,
            max_blocked_connections,
        }
    }

    /// Register a new waiter.
    ///
    /// Returns Ok(()) if registered, Err with message if limits exceeded.
    pub fn register(&mut self, entry: WaitEntry) -> Result<(), String> {
        // Check global limit
        if self.max_blocked_connections > 0 && self.waiter_count >= self.max_blocked_connections {
            return Err("ERR max blocked connections limit reached".to_string());
        }

        // Check per-key limits
        if self.max_waiters_per_key > 0 {
            for key in &entry.keys {
                if let Some(waiters) = self.waiters_by_key.get(key) {
                    if waiters.len() >= self.max_waiters_per_key {
                        return Err("ERR max waiters per key limit reached".to_string());
                    }
                }
            }
        }

        let conn_id = entry.conn_id;
        let keys = entry.keys.clone();

        // Allocate a slot for the entry
        let slot_idx = if let Some(idx) = self.free_slots.pop() {
            self.entries[idx] = Some(entry);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            idx
        };

        // Index by each key
        for key in &keys {
            self.waiters_by_key
                .entry(key.clone())
                .or_default()
                .push_back(slot_idx);
        }

        // Index by connection ID
        self.conn_entries
            .entry(conn_id)
            .or_default()
            .push(slot_idx);

        self.waiter_count += 1;
        Ok(())
    }

    /// Unregister all waiters for a connection (called on disconnect or timeout).
    ///
    /// Returns the entries that were removed.
    pub fn unregister(&mut self, conn_id: u64) -> Vec<WaitEntry> {
        let entry_indices = match self.conn_entries.remove(&conn_id) {
            Some(indices) => indices,
            None => return vec![],
        };

        let mut removed = Vec::new();

        for idx in entry_indices {
            if let Some(entry) = self.entries[idx].take() {
                // Remove from key index
                for key in &entry.keys {
                    if let Some(waiters) = self.waiters_by_key.get_mut(key) {
                        waiters.retain(|&i| i != idx);
                        if waiters.is_empty() {
                            self.waiters_by_key.remove(key);
                        }
                    }
                }

                self.free_slots.push(idx);
                self.waiter_count -= 1;
                removed.push(entry);
            }
        }

        removed
    }

    /// Pop the oldest waiter for a key.
    ///
    /// Returns the WaitEntry if one exists, None otherwise.
    pub fn pop_oldest_waiter(&mut self, key: &Bytes) -> Option<WaitEntry> {
        // First, find and extract the entry without holding any borrows
        let (idx, entry) = {
            let waiters = self.waiters_by_key.get_mut(key)?;

            loop {
                let idx = waiters.pop_front()?;
                if let Some(entry) = self.entries[idx].take() {
                    break (idx, entry);
                }
                // Entry was already removed, continue to next
            }
        };

        // Collect other keys to clean up (excluding the current key)
        let other_keys: Vec<Bytes> = entry.keys.iter()
            .filter(|k| *k != key)
            .cloned()
            .collect();

        // Remove from all other key indices
        for k in &other_keys {
            if let Some(w) = self.waiters_by_key.get_mut(k) {
                w.retain(|&i| i != idx);
                if w.is_empty() {
                    self.waiters_by_key.remove(k);
                }
            }
        }

        // Remove from conn_entries
        if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
            conn_entries.retain(|&i| i != idx);
            if conn_entries.is_empty() {
                self.conn_entries.remove(&entry.conn_id);
            }
        }

        self.free_slots.push(idx);
        self.waiter_count -= 1;

        // Clean up empty key entry for the primary key
        if let Some(waiters) = self.waiters_by_key.get(key) {
            if waiters.is_empty() {
                self.waiters_by_key.remove(key);
            }
        }

        Some(entry)
    }

    /// Collect all expired waiters (deadline has passed).
    ///
    /// Returns the expired WaitEntry objects.
    pub fn collect_expired(&mut self, now: Instant) -> Vec<WaitEntry> {
        let mut expired_indices = Vec::new();

        // Find all expired entries
        for (idx, entry) in self.entries.iter().enumerate() {
            if let Some(ref e) = entry {
                if let Some(deadline) = e.deadline {
                    if deadline <= now {
                        expired_indices.push(idx);
                    }
                }
            }
        }

        let mut expired = Vec::new();

        // Remove expired entries
        for idx in expired_indices {
            if let Some(entry) = self.entries[idx].take() {
                // Remove from key index
                for key in &entry.keys {
                    if let Some(waiters) = self.waiters_by_key.get_mut(key) {
                        waiters.retain(|&i| i != idx);
                        if waiters.is_empty() {
                            self.waiters_by_key.remove(key);
                        }
                    }
                }

                // Remove from conn_entries
                if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
                    conn_entries.retain(|&i| i != idx);
                    if conn_entries.is_empty() {
                        self.conn_entries.remove(&entry.conn_id);
                    }
                }

                self.free_slots.push(idx);
                self.waiter_count -= 1;
                expired.push(entry);
            }
        }

        expired
    }

    /// Check if there are any waiters for a key.
    pub fn has_waiters(&self, key: &Bytes) -> bool {
        self.waiters_by_key
            .get(key)
            .map(|w| !w.is_empty())
            .unwrap_or(false)
    }

    /// Get the number of active waiters.
    pub fn waiter_count(&self) -> usize {
        self.waiter_count
    }

    /// Get the number of keys with waiters.
    pub fn blocked_keys_count(&self) -> usize {
        self.waiters_by_key.len()
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

    /// Function registry (shared across all shards).
    function_registry: Option<SharedFunctionRegistry>,

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

    /// Wait queue for blocking commands.
    wait_queue: ShardWaitQueue,

    /// Slow query log for this shard.
    slowlog: SlowLog,

    /// Latency monitor for this shard.
    latency_monitor: LatencyMonitor,

    /// VLL intent table for tracking pending key access.
    intent_table: Option<crate::vll::IntentTable>,

    /// VLL transaction queue for ordering operations.
    tx_queue: Option<crate::vll::TransactionQueue>,

    /// VLL continuation lock (for MULTI/EXEC and Lua scripts).
    continuation_lock: Option<crate::vll::ContinuationLock>,

    /// Pending release receiver for continuation lock.
    /// When Some, we poll this to detect when the lock should be released.
    pending_continuation_release: Option<oneshot::Receiver<()>>,

    /// Replication broadcaster for streaming writes to replicas.
    replication_broadcaster: SharedBroadcaster,

    /// Optional Raft instance for cluster command execution.
    raft: Option<Arc<ClusterRaft>>,

    /// Optional cluster state for cluster commands and routing.
    cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID (for cluster mode).
    node_id: Option<u64>,

    /// Operation counters for hot shard detection.
    operation_counters: OperationCounters,

    /// Current queue depth (messages waiting to be processed).
    /// Shared so it can be read from outside the worker.
    queue_depth: Arc<AtomicUsize>,

    /// Optional network factory for cluster node management.
    network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional quorum checker for local cluster health detection.
    quorum_checker: Option<Arc<dyn QuorumChecker>>,
}

// ============================================================================
// ShardWorker Builder
// ============================================================================

/// Error returned when building a [`ShardWorker`] fails due to missing required fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardBuilderError {
    /// A required field was not set.
    MissingField(&'static str),
}

impl std::fmt::Display for ShardBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardBuilderError::MissingField(field) => {
                write!(f, "missing required field: {}", field)
            }
        }
    }
}

impl std::error::Error for ShardBuilderError {}

/// Builder for creating [`ShardWorker`] instances with a fluent API.
///
/// This provides a cleaner way to construct shard workers with optional
/// features like persistence, VLL, and cluster support.
///
/// # Example
///
/// ```rust,ignore
/// let worker = ShardWorkerBuilder::new(shard_id, num_shards, message_rx, new_conn_rx)
///     .with_registry(registry)
///     .with_shard_senders(shard_senders)
///     .with_metrics(metrics_recorder)
///     .with_eviction(eviction_config)
///     .with_persistence(rocks_store, wal_config)
///     .enable_vll()
///     .build();
/// ```
pub struct ShardWorkerBuilder {
    shard_id: usize,
    num_shards: usize,
    message_rx: Option<mpsc::Receiver<ShardMessage>>,
    new_conn_rx: Option<mpsc::Receiver<NewConnection>>,
    shard_senders: Option<Arc<Vec<mpsc::Sender<ShardMessage>>>>,
    registry: Option<Arc<CommandRegistry>>,
    metrics_recorder: Option<Arc<dyn crate::noop::MetricsRecorder>>,
    slowlog_next_id: Option<Arc<AtomicU64>>,
    replication_broadcaster: Option<SharedBroadcaster>,
    eviction_config: EvictionConfig,
    scripting_config: ScriptingConfig,
    rocks_store: Option<Arc<RocksStore>>,
    wal_config: Option<WalConfig>,
    snapshot_coordinator: Option<Arc<dyn SnapshotCoordinator>>,
    function_registry: Option<SharedFunctionRegistry>,
    cluster_state: Option<Arc<ClusterState>>,
    node_id: Option<u64>,
    raft: Option<Arc<ClusterRaft>>,
    network_factory: Option<Arc<ClusterNetworkFactory>>,
    quorum_checker: Option<Arc<dyn QuorumChecker>>,
    enable_vll: bool,
    queue_depth: Option<Arc<AtomicUsize>>,
}

impl ShardWorkerBuilder {
    /// Create a new builder with required shard identity.
    pub fn new(shard_id: usize, num_shards: usize) -> Self {
        Self {
            shard_id,
            num_shards,
            message_rx: None,
            new_conn_rx: None,
            shard_senders: None,
            registry: None,
            metrics_recorder: None,
            slowlog_next_id: None,
            replication_broadcaster: None,
            eviction_config: EvictionConfig::default(),
            scripting_config: ScriptingConfig::default(),
            rocks_store: None,
            wal_config: None,
            snapshot_coordinator: None,
            function_registry: None,
            cluster_state: None,
            node_id: None,
            raft: None,
            network_factory: None,
            quorum_checker: None,
            enable_vll: false,
            queue_depth: None,
        }
    }

    /// Set the message receiver for shard commands.
    pub fn with_message_rx(mut self, rx: mpsc::Receiver<ShardMessage>) -> Self {
        self.message_rx = Some(rx);
        self
    }

    /// Set the new connection receiver.
    pub fn with_new_conn_rx(mut self, rx: mpsc::Receiver<NewConnection>) -> Self {
        self.new_conn_rx = Some(rx);
        self
    }

    /// Set shard senders for cross-shard operations.
    pub fn with_shard_senders(mut self, senders: Arc<Vec<mpsc::Sender<ShardMessage>>>) -> Self {
        self.shard_senders = Some(senders);
        self
    }

    /// Set the command registry.
    pub fn with_registry(mut self, registry: Arc<CommandRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the metrics recorder.
    pub fn with_metrics(mut self, recorder: Arc<dyn crate::noop::MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Set the slowlog next ID counter.
    pub fn with_slowlog_id(mut self, id: Arc<AtomicU64>) -> Self {
        self.slowlog_next_id = Some(id);
        self
    }

    /// Set the replication broadcaster.
    pub fn with_replication(mut self, broadcaster: SharedBroadcaster) -> Self {
        self.replication_broadcaster = Some(broadcaster);
        self
    }

    /// Set eviction configuration.
    pub fn with_eviction(mut self, config: EvictionConfig) -> Self {
        self.eviction_config = config;
        self
    }

    /// Set scripting configuration.
    pub fn with_scripting(mut self, config: ScriptingConfig) -> Self {
        self.scripting_config = config;
        self
    }

    /// Enable persistence with RocksDB.
    pub fn with_persistence(mut self, rocks_store: Arc<RocksStore>, wal_config: WalConfig) -> Self {
        self.rocks_store = Some(rocks_store);
        self.wal_config = Some(wal_config);
        self
    }

    /// Set the snapshot coordinator.
    pub fn with_snapshot_coordinator(mut self, coordinator: Arc<dyn SnapshotCoordinator>) -> Self {
        self.snapshot_coordinator = Some(coordinator);
        self
    }

    /// Set the function registry for FUNCTION/FCALL commands.
    pub fn with_function_registry(mut self, registry: SharedFunctionRegistry) -> Self {
        self.function_registry = Some(registry);
        self
    }

    /// Enable cluster mode with the given dependencies.
    pub fn with_cluster(
        mut self,
        cluster_state: Arc<ClusterState>,
        node_id: u64,
        raft: Arc<ClusterRaft>,
        network_factory: Arc<ClusterNetworkFactory>,
    ) -> Self {
        self.cluster_state = Some(cluster_state);
        self.node_id = Some(node_id);
        self.raft = Some(raft);
        self.network_factory = Some(network_factory);
        self
    }

    /// Set the quorum checker for cluster health detection.
    pub fn with_quorum_checker(mut self, checker: Arc<dyn QuorumChecker>) -> Self {
        self.quorum_checker = Some(checker);
        self
    }

    /// Enable VLL (Virtual Lock Loom) for transaction coordination.
    pub fn enable_vll(mut self) -> Self {
        self.enable_vll = true;
        self
    }

    /// Set the shared queue depth counter.
    pub fn with_queue_depth(mut self, depth: Arc<AtomicUsize>) -> Self {
        self.queue_depth = Some(depth);
        self
    }

    /// Set core dependencies from a bundle.
    pub fn with_core_deps(mut self, core: ShardCoreDeps) -> Self {
        self.shard_senders = Some(core.shard_senders);
        self.registry = Some(core.registry);
        self.metrics_recorder = Some(core.metrics_recorder);
        self.slowlog_next_id = Some(core.slowlog_next_id);
        self.replication_broadcaster = Some(core.replication_broadcaster);
        self
    }

    /// Set persistence dependencies from a bundle.
    pub fn with_persistence_deps(mut self, persistence: ShardPersistenceDeps) -> Self {
        self.rocks_store = persistence.rocks_store;
        self.snapshot_coordinator = persistence.snapshot_coordinator;
        self
    }

    /// Set cluster dependencies from a bundle.
    pub fn with_cluster_deps(mut self, cluster: ShardClusterDeps) -> Self {
        self.cluster_state = cluster.cluster_state;
        self.node_id = cluster.node_id;
        self.raft = cluster.raft;
        self.network_factory = cluster.network_factory;
        self.quorum_checker = cluster.quorum_checker;
        self
    }

    /// Try to build the ShardWorker, returning an error if required fields are missing.
    ///
    /// This is the fallible version of [`build()`](Self::build) that returns a
    /// `Result` instead of panicking on missing required fields.
    pub fn try_build(self) -> Result<ShardWorker, ShardBuilderError> {
        let message_rx = self
            .message_rx
            .ok_or(ShardBuilderError::MissingField("message_rx"))?;
        let new_conn_rx = self
            .new_conn_rx
            .ok_or(ShardBuilderError::MissingField("new_conn_rx"))?;
        let shard_senders = self
            .shard_senders
            .ok_or(ShardBuilderError::MissingField("shard_senders"))?;
        let registry = self
            .registry
            .ok_or(ShardBuilderError::MissingField("registry"))?;
        let metrics_recorder = self
            .metrics_recorder
            .unwrap_or_else(|| Arc::new(crate::noop::NoopMetricsRecorder::new()));
        let slowlog_next_id = self
            .slowlog_next_id
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        let replication_broadcaster = self
            .replication_broadcaster
            .unwrap_or_else(|| Arc::new(NoopBroadcaster));
        let snapshot_coordinator: Arc<dyn SnapshotCoordinator> = self
            .snapshot_coordinator
            .unwrap_or_else(|| Arc::new(NoopSnapshotCoordinator::new()));

        // Create the worker using the existing with_eviction constructor
        let mut worker = ShardWorker::with_eviction(
            self.shard_id,
            self.num_shards,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            self.eviction_config,
            metrics_recorder,
            slowlog_next_id,
            replication_broadcaster,
        );

        // Apply optional configurations
        if let Some(rocks_store) = self.rocks_store {
            worker.rocks_store = Some(rocks_store.clone());
            if let Some(wal_config) = self.wal_config {
                let wal_writer = RocksWalWriter::new(
                    rocks_store,
                    worker.shard_id,
                    wal_config,
                    worker.metrics_recorder.clone(),
                );
                worker.wal_writer = Some(wal_writer);
            }
        }

        worker.snapshot_coordinator = snapshot_coordinator;
        worker.function_registry = self.function_registry;
        worker.cluster_state = self.cluster_state;
        worker.node_id = self.node_id;
        worker.raft = self.raft;
        worker.network_factory = self.network_factory;
        worker.quorum_checker = self.quorum_checker;

        if let Some(queue_depth) = self.queue_depth {
            worker.queue_depth = queue_depth;
        }

        // VLL initialization is handled separately via enable_vll() method on ShardWorker
        // since it requires runtime configuration

        Ok(worker)
    }

    /// Build the ShardWorker.
    ///
    /// # Panics
    ///
    /// Panics if required dependencies are not set:
    /// - `message_rx`
    /// - `new_conn_rx`
    /// - `shard_senders`
    /// - `registry`
    pub fn build(self) -> ShardWorker {
        self.try_build()
            .expect("ShardWorkerBuilder: missing required fields")
    }
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
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    /// Create a new shard worker without persistence but with eviction config.
    #[allow(clippy::too_many_arguments)]
    pub fn with_eviction(
        shard_id: usize,
        num_shards: usize,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
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
            function_registry: None,
            eviction_config,
            eviction_pool: EvictionPool::new(),
            memory_limit,
            metrics_recorder,
            peak_memory: 0,
            wait_queue: ShardWaitQueue::new(),
            slowlog: SlowLog::new(
                crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                slowlog_next_id,
            ),
            latency_monitor: LatencyMonitor::default_monitor(),
            intent_table: None,
            tx_queue: None,
            continuation_lock: None,
            pending_continuation_release: None,
            replication_broadcaster,
            raft: None,
            cluster_state: None,
            node_id: None,
            operation_counters: OperationCounters::new(),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            network_factory: None,
            quorum_checker: None,
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
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
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
            function_registry: None,
            eviction_config,
            eviction_pool: EvictionPool::new(),
            memory_limit,
            metrics_recorder,
            peak_memory: 0,
            wait_queue: ShardWaitQueue::new(),
            slowlog: SlowLog::new(
                crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                slowlog_next_id,
            ),
            latency_monitor: LatencyMonitor::default_monitor(),
            intent_table: None,
            tx_queue: None,
            continuation_lock: None,
            pending_continuation_release: None,
            replication_broadcaster,
            raft: None,
            cluster_state: None,
            node_id: None,
            operation_counters: OperationCounters::new(),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            network_factory: None,
            quorum_checker: None,
        }
    }

    /// Set the function registry for this shard.
    pub fn set_function_registry(&mut self, registry: SharedFunctionRegistry) {
        self.function_registry = Some(registry);
    }

    /// Set the replication broadcaster for this shard.
    ///
    /// This allows setting the broadcaster after construction, which is useful
    /// when the primary replication handler is initialized separately from the shards.
    pub fn set_replication_broadcaster(&mut self, broadcaster: SharedBroadcaster) {
        self.replication_broadcaster = broadcaster;
    }

    /// Set the Raft instance for cluster commands.
    ///
    /// This allows setting the Raft instance after construction, which is needed
    /// because Raft initialization may happen after shard workers are created.
    pub fn set_raft(&mut self, raft: Arc<ClusterRaft>) {
        self.raft = Some(raft);
    }

    /// Set the cluster state for cluster commands.
    pub fn set_cluster_state(&mut self, cluster_state: Arc<ClusterState>) {
        self.cluster_state = Some(cluster_state);
    }

    /// Set this node's ID for cluster mode.
    pub fn set_node_id(&mut self, node_id: u64) {
        self.node_id = Some(node_id);
    }

    /// Set the network factory for cluster node management.
    pub fn set_network_factory(&mut self, network_factory: Arc<ClusterNetworkFactory>) {
        self.network_factory = Some(network_factory);
    }

    /// Set the quorum checker for local cluster health detection.
    pub fn set_quorum_checker(&mut self, quorum_checker: Arc<dyn QuorumChecker>) {
        self.quorum_checker = Some(quorum_checker);
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

    /// Check if this connection can execute during a continuation lock.
    ///
    /// When a continuation lock is held, only the lock owner can execute commands.
    /// Returns Ok(()) if execution is allowed, Err(Response) otherwise.
    fn can_execute_during_lock(&self, conn_id: u64) -> Result<(), Response> {
        if let Some(ref lock) = self.continuation_lock {
            if lock.conn_id != conn_id {
                return Err(Response::error("ERR shard busy with continuation lock"));
            }
        }
        Ok(())
    }

    /// Run the shard worker event loop.
    pub async fn run(mut self) {
        tracing::info!(shard_id = self.shard_id, "Shard worker started");

        // Active expiry runs every 100ms
        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));

        // Metrics collection runs every 10 seconds
        let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

        // Blocking waiter timeout check runs every 100ms
        let mut waiter_timeout_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages
                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        ShardMessage::Execute { command, conn_id, txid: _, protocol_version, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                let _ = response_tx.send(err);
                                continue;
                            }
                            let response = self.execute_command(&command, conn_id, protocol_version).await;
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScatterRequest { request_id: _, keys, operation, conn_id, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                // Return a PartialResult with the error for each key
                                let error_results: Vec<(Bytes, Response)> = keys.iter()
                                    .map(|k| (k.clone(), err.clone()))
                                    .collect();
                                let _ = response_tx.send(PartialResult { results: error_results });
                                continue;
                            }
                            let result = self.execute_scatter_part(&keys, &operation).await;
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::GetVersion { response_tx } => {
                            let _ = response_tx.send(self.shard_version);
                        }
                        ShardMessage::ExecTransaction { commands, watches, conn_id, protocol_version, response_tx } => {
                            let result = self.execute_transaction(commands, &watches, conn_id, protocol_version).await;
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
                        ShardMessage::EvalScript { script_source, keys, argv, conn_id, protocol_version, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                let _ = response_tx.send(err);
                                continue;
                            }
                            let response = self.handle_eval_script(&script_source, &keys, &argv, conn_id, protocol_version);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::EvalScriptSha { script_sha, keys, argv, conn_id, protocol_version, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                let _ = response_tx.send(err);
                                continue;
                            }
                            let response = self.handle_evalsha(&script_sha, &keys, &argv, conn_id, protocol_version);
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

                        // Function message handlers
                        ShardMessage::FunctionCall { function_name, keys, argv, conn_id, protocol_version, read_only, response_tx } => {
                            let response = self.handle_function_call(&function_name, &keys, &argv, conn_id, protocol_version, read_only);
                            let _ = response_tx.send(response);
                        }

                        // Blocking commands handlers
                        ShardMessage::BlockWait { conn_id, keys, op, response_tx, deadline } => {
                            self.handle_block_wait(conn_id, keys, op, response_tx, deadline);
                        }
                        ShardMessage::UnregisterWait { conn_id } => {
                            self.handle_unregister_wait(conn_id);
                        }

                        // Slowlog handlers
                        ShardMessage::SlowlogGet { count, response_tx } => {
                            let entries = self.slowlog.get(count);
                            let _ = response_tx.send(entries);
                        }
                        ShardMessage::SlowlogLen { response_tx } => {
                            let _ = response_tx.send(self.slowlog.len());
                        }
                        ShardMessage::SlowlogReset { response_tx } => {
                            self.slowlog.reset();
                            let _ = response_tx.send(());
                        }
                        ShardMessage::SlowlogAdd { duration_us, command, client_addr, client_name } => {
                            self.slowlog.add(duration_us, &command, client_addr, client_name);
                        }

                        // Memory handlers
                        ShardMessage::MemoryUsage { key, samples: _, response_tx } => {
                            let usage = self.calculate_key_memory_usage(&key);
                            let _ = response_tx.send(usage);
                        }
                        ShardMessage::MemoryStats { response_tx } => {
                            let stats = self.collect_memory_stats();
                            let _ = response_tx.send(stats);
                        }
                        ShardMessage::WalLagStats { response_tx } => {
                            let stats = self.collect_wal_lag_stats().await;
                            let _ = response_tx.send(stats);
                        }
                        ShardMessage::ScanBigKeys { threshold_bytes, max_keys, response_tx } => {
                            let result = self.scan_big_keys(threshold_bytes, max_keys);
                            let _ = response_tx.send(result);
                        }

                        // Latency handlers
                        ShardMessage::LatencyLatest { response_tx } => {
                            let latest = self.latency_monitor.latest();
                            let _ = response_tx.send(latest);
                        }
                        ShardMessage::LatencyHistory { event, response_tx } => {
                            let history = self.latency_monitor.history(event);
                            let _ = response_tx.send(history);
                        }
                        ShardMessage::LatencyReset { events, response_tx } => {
                            self.latency_monitor.reset(&events);
                            let _ = response_tx.send(());
                        }

                        ShardMessage::HotShardStats { period_secs, response_tx } => {
                            let stats = self.calculate_hot_shard_stats(period_secs);
                            let _ = response_tx.send(stats);
                        }

                        ShardMessage::UpdateConfig { eviction_config, response_tx } => {
                            if let Some(config) = eviction_config {
                                self.eviction_config = config;
                                // Recalculate per-shard memory limit
                                self.memory_limit = if self.eviction_config.maxmemory > 0 {
                                    self.eviction_config.maxmemory / self.num_shards as u64
                                } else {
                                    0
                                };
                                tracing::info!(
                                    shard_id = self.shard_id,
                                    "Shard config updated"
                                );
                            }
                            let _ = response_tx.send(());
                        }

                        // VLL message handlers
                        ShardMessage::VllLockRequest { txid, keys, mode, operation, ready_tx, execute_rx } => {
                            self.handle_vll_lock_request(txid, keys, mode, operation, ready_tx, execute_rx).await;
                        }
                        ShardMessage::VllExecute { txid, response_tx } => {
                            self.handle_vll_execute(txid, response_tx).await;
                        }
                        ShardMessage::VllAbort { txid } => {
                            self.handle_vll_abort(txid);
                        }
                        ShardMessage::VllContinuationLock { txid, conn_id, ready_tx, release_rx } => {
                            self.handle_vll_continuation_lock(txid, conn_id, ready_tx, release_rx).await;
                        }

                        // Cluster / Raft message handlers
                        ShardMessage::RaftCommand { cmd, response_tx } => {
                            let result = if let Some(ref raft) = self.raft {
                                raft.client_write(cmd).await
                                    .map(|_| ())
                                    .map_err(|e| e.to_string())
                            } else {
                                Err("Raft not initialized".to_string())
                            };
                            let _ = response_tx.send(result);
                        }

                        ShardMessage::GetVllQueueInfo { response_tx } => {
                            let info = self.collect_vll_queue_info();
                            let _ = response_tx.send(info);
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

                // Blocking waiter timeout check
                _ = waiter_timeout_interval.tick() => {
                    self.check_waiter_timeouts();
                }

                // Check for continuation lock release signal
                _ = async {
                    match &mut self.pending_continuation_release {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Release signal received - clear the continuation lock
                    self.continuation_lock = None;
                    self.pending_continuation_release = None;
                    tracing::debug!(shard_id = self.shard_id, "Continuation lock released");
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

        // Get expired keys using the cleaner abstraction
        let expired = self.store.get_expired_keys(now);
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

        // Keys with expiry (using cleaner abstraction)
        self.metrics_recorder.record_gauge(
            "frogdb_keys_with_expiry",
            self.store.keys_with_expiry_count() as f64,
            &[("shard", &shard_label)],
        );
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
            tracing::warn!(
                shard_id = self.shard_id,
                memory_used = self.store.memory_used(),
                memory_limit = self.memory_limit,
                "OOM rejected write"
            );
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_eviction_oom_total",
                1,
                &[("shard", &shard_label)],
            );
            return Err(CommandError::OutOfMemory);
        }

        tracing::debug!(
            shard_id = self.shard_id,
            memory_used = self.store.memory_used(),
            memory_limit = self.memory_limit,
            "Eviction triggered"
        );

        // Attempt eviction
        let max_attempts = 10; // Limit attempts to avoid infinite loop
        for _ in 0..max_attempts {
            if !self.is_over_memory_limit() {
                return Ok(());
            }

            if !self.evict_one() {
                // No more keys to evict
                tracing::warn!(
                    shard_id = self.shard_id,
                    policy = %self.eviction_config.policy,
                    "No volatile keys for eviction"
                );
                tracing::warn!(
                    shard_id = self.shard_id,
                    memory_used = self.store.memory_used(),
                    memory_limit = self.memory_limit,
                    "OOM rejected write"
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
            tracing::warn!(
                shard_id = self.shard_id,
                memory_used = self.store.memory_used(),
                memory_limit = self.memory_limit,
                "OOM rejected write"
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

    // =========================================================================
    // WAL persistence helpers
    // =========================================================================

    /// Persist a key's current state to WAL after a write operation.
    async fn persist_key_to_wal(&self, key: &[u8]) {
        if let Some(ref wal) = self.wal_writer {
            if let Some(value) = self.store.get(key) {
                let metadata = self.store.get_metadata(key)
                    .unwrap_or_else(|| crate::types::KeyMetadata::new(value.memory_size()));
                if let Err(e) = wal.write_set(key, &value, &metadata).await {
                    tracing::error!(
                        key = %String::from_utf8_lossy(key),
                        error = %e,
                        "Failed to persist key to WAL"
                    );
                }
            }
        }
    }

    /// Persist a deletion to WAL.
    async fn persist_delete_to_wal(&self, key: &[u8]) {
        if let Some(ref wal) = self.wal_writer {
            if let Err(e) = wal.write_delete(key).await {
                tracing::error!(
                    key = %String::from_utf8_lossy(key),
                    error = %e,
                    "Failed to persist delete to WAL"
                );
            }
        }
    }

    /// Persist command changes to WAL based on command type.
    async fn persist_command_to_wal(&self, cmd_name: &str, args: &[Bytes]) {
        if self.wal_writer.is_none() {
            return;
        }

        match cmd_name {
            // SET-like: persist current value
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "SETRANGE" | "APPEND"
            | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT"
            | "HSET" | "HSETNX" | "HMSET" | "HINCRBY" | "HINCRBYFLOAT"
            | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LSET" | "LINSERT"
            | "SADD" | "SMOVE"
            | "ZADD" | "ZINCRBY"
            | "PFADD" | "PFMERGE"
            | "GEOADD"
            | "BF.ADD" | "BF.MADD" | "BF.INSERT" | "BF.RESERVE"
            | "XADD" | "XTRIM"
            | "SETBIT" | "BITOP"
            | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "GETEX" => {
                // These commands have the key as the first argument
                if !args.is_empty() {
                    self.persist_key_to_wal(&args[0]).await;
                }
            }

            // BITOP has destination as first arg after operation type
            // Handled above with BITOP

            // DELETE-like: persist deletion only if key was deleted
            "DEL" | "UNLINK" | "GETDEL" => {
                for arg in args {
                    if !self.store.contains(arg) {
                        self.persist_delete_to_wal(arg).await;
                    }
                }
            }

            // POP/REMOVE: check if key still exists
            "LPOP" | "RPOP" | "LMPOP"
            | "SPOP" | "SREM"
            | "ZPOPMIN" | "ZPOPMAX" | "ZREM" | "ZMPOP"
            | "HDEL"
            | "LTRIM" | "LREM"
            | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZREMRANGEBYLEX" => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.persist_key_to_wal(key).await;
                    } else {
                        self.persist_delete_to_wal(key).await;
                    }
                }
            }

            // RENAME: delete old key, set new key
            "RENAME" | "RENAMENX" => {
                if args.len() >= 2 {
                    let old_key = &args[0];
                    let new_key = &args[1];
                    if !self.store.contains(old_key) {
                        self.persist_delete_to_wal(old_key).await;
                    }
                    self.persist_key_to_wal(new_key).await;
                }
            }

            // Store operations: persist destination
            "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE"
            | "ZINTERSTORE" | "ZUNIONSTORE" | "ZDIFFSTORE" | "ZRANGESTORE" => {
                // Destination is first argument
                if !args.is_empty() {
                    let dest = &args[0];
                    if self.store.contains(dest) {
                        self.persist_key_to_wal(dest).await;
                    }
                }
            }

            // LMOVE/COPY: destination is second argument
            "LMOVE" | "COPY" => {
                if args.len() >= 2 {
                    let dest = &args[1];
                    if self.store.contains(dest) {
                        self.persist_key_to_wal(dest).await;
                    }
                }
            }

            // FLUSHDB/FLUSHALL: handled by RocksDB clear, no WAL marker needed
            "FLUSHDB" | "FLUSHALL" => {
                // No-op: RocksDB column family is cleared directly
            }

            _ => {
                // Unknown write command: log and persist first key to be safe
                tracing::warn!(command = cmd_name, "Unknown write command for WAL persistence");
                if !args.is_empty() && self.store.contains(&args[0]) {
                    self.persist_key_to_wal(&args[0]).await;
                }
            }
        }
    }

    /// Execute a command locally.
    async fn execute_command(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> Response {
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
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
            protocol_version,
            None, // replication_tracker - not available in shard
            None, // replication_state - not available in shard
            self.cluster_state.as_ref(),
            self.node_id,
            self.raft.as_ref(),
            self.network_factory.as_ref(),
            self.quorum_checker.as_ref().map(|q| q.as_ref()),
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

            // Try to satisfy any blocking waiters after list/zset write operations
            let keys = handler.keys(&command.args);
            match cmd_name_str.as_ref() {
                // List push commands that may satisfy BLPOP/BRPOP/BLMOVE/BLMPOP waiters
                "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LINSERT" => {
                    for key in keys {
                        let key_bytes = Bytes::copy_from_slice(key);
                        self.try_satisfy_list_waiters(&key_bytes);
                    }
                }
                // Sorted set commands that may satisfy BZPOPMIN/BZPOPMAX/BZMPOP waiters
                "ZADD" => {
                    for key in keys {
                        let key_bytes = Bytes::copy_from_slice(key);
                        self.try_satisfy_zset_waiters(&key_bytes);
                    }
                }
                // Stream commands that may satisfy XREAD/XREADGROUP waiters
                "XADD" => {
                    for key in keys {
                        let key_bytes = Bytes::copy_from_slice(key);
                        self.try_satisfy_stream_waiters(&key_bytes);
                    }
                }
                _ => {}
            }

            // Persist to WAL for write operations
            self.persist_command_to_wal(&cmd_name_str, &command.args).await;

            // Broadcast to replicas (if running as primary with connected replicas)
            // Skip broadcast if this command came from replication (to avoid infinite loops)
            if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
                self.replication_broadcaster.broadcast_command(&cmd_name_str, &command.args);
            }
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
    async fn execute_transaction(
        &mut self,
        commands: Vec<ParsedCommand>,
        watches: &[(Bytes, u64)],
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> TransactionResult {
        // Check WATCH conditions
        if !self.check_watches(watches) {
            return TransactionResult::WatchAborted;
        }

        // Execute all commands
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let response = self.execute_command(&command, conn_id, protocol_version).await;
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
            ScatterOp::Copy { source_key } => {
                // Get the value and expiry from source key for cross-shard copy.
                // Returns an array with: [value_type, serialized_value, expiry_ms_or_nil]
                match self.store.get(source_key) {
                    Some(value) => {
                        // Get expiry if any
                        let expiry = self.store.get_expiry(source_key);
                        let expiry_ms = expiry.map(|exp| {
                            exp.duration_since(std::time::Instant::now())
                                .as_millis() as i64
                        });

                        // Serialize the value based on its type
                        let (type_str, serialized) = value.serialize_for_copy();

                        let expiry_resp = match expiry_ms {
                            Some(ms) if ms > 0 => Response::Integer(ms),
                            _ => Response::null(),
                        };

                        vec![(
                            source_key.clone(),
                            Response::Array(vec![
                                Response::bulk(Bytes::from(type_str)),
                                Response::bulk(serialized),
                                expiry_resp,
                            ]),
                        )]
                    }
                    None => {
                        // Source key doesn't exist
                        vec![(source_key.clone(), Response::null())]
                    }
                }
            }
            ScatterOp::CopySet {
                dest_key,
                value_type,
                value_data,
                expiry_ms,
                replace,
            } => {
                // Write a value from cross-shard copy to destination key.
                // Check if destination exists (when not using REPLACE)
                if !replace && self.store.contains(dest_key) {
                    return PartialResult {
                        results: vec![(dest_key.clone(), Response::Integer(0))],
                    };
                }

                // Deserialize the value
                match Value::deserialize_for_copy(value_type, value_data) {
                    Some(value) => {
                        // If REPLACE, delete existing first
                        if *replace {
                            self.store.delete(dest_key);
                        }

                        // Set the value
                        self.store.set(dest_key.clone(), value.clone());

                        // Set expiry if provided
                        if let Some(ms) = expiry_ms {
                            if *ms > 0 {
                                let expires_at =
                                    Instant::now() + Duration::from_millis(*ms as u64);
                                self.store.set_expiry(dest_key, expires_at);
                            }
                        }

                        // Persist to WAL if enabled
                        if let Some(ref wal) = self.wal_writer {
                            let metadata = KeyMetadata::new(value.memory_size());
                            if let Err(e) = wal.write_set(dest_key, &value, &metadata).await {
                                tracing::error!(
                                    key = %String::from_utf8_lossy(dest_key),
                                    error = %e,
                                    "Failed to persist COPY"
                                );
                            }
                        }

                        // Increment version
                        self.increment_version();

                        vec![(dest_key.clone(), Response::Integer(1))]
                    }
                    None => {
                        // Failed to deserialize value
                        vec![(
                            dest_key.clone(),
                            Response::error("ERR failed to deserialize value for COPY"),
                        )]
                    }
                }
            }
            ScatterOp::RandomKey => {
                // Return a random key from this shard
                match self.store.random_key() {
                    Some(key) => vec![(Bytes::from_static(b"__randomkey__"), Response::bulk(key))],
                    None => vec![(Bytes::from_static(b"__randomkey__"), Response::null())],
                }
            }
            ScatterOp::Dump => {
                // Serialize keys with full metadata for MIGRATE.
                // Returns serialized data in our internal format (compatible with RESTORE).
                use crate::persistence::serialize;

                keys.iter()
                    .map(|key| {
                        match self.store.get(key) {
                            Some(value) => {
                                // Get expiry if any
                                let expires_at = self.store.get_expiry(key);
                                let mut metadata = KeyMetadata::new(value.memory_size());
                                metadata.expires_at = expires_at;

                                // Serialize with full metadata
                                let serialized = serialize(&value, &metadata);
                                (key.clone(), Response::bulk(Bytes::from(serialized)))
                            }
                            None => {
                                // Key doesn't exist
                                (key.clone(), Response::null())
                            }
                        }
                    })
                    .collect()
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
    // VLL (Very Lightweight Locking) handlers
    // =========================================================================

    /// Handle VLL lock request - declare intents and try to acquire locks.
    async fn handle_vll_lock_request(
        &mut self,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: ScatterOp,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    ) {
        use crate::vll::{IntentTable, TransactionQueue, VllPendingOp, VllError};

        // Ensure we have the VLL infrastructure
        if self.intent_table.is_none() {
            self.intent_table = Some(IntentTable::new());
        }
        if self.tx_queue.is_none() {
            self.tx_queue = Some(TransactionQueue::new(10000));
        }

        let intent_table = self.intent_table.as_mut().unwrap();
        let tx_queue = self.tx_queue.as_mut().unwrap();

        // Check queue capacity - warn when queue depth is high
        let queue_depth = tx_queue.len();
        // Warn when queue has 8000+ transactions (80% of default 10000)
        if queue_depth >= 8000 {
            tracing::warn!(
                shard_id = self.shard_id,
                queue_depth,
                "Shard message queue depth high"
            );
        }

        if !tx_queue.has_capacity() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::QueueFull));
            return;
        }

        // Declare intents
        intent_table.declare_intents(&keys, txid, mode);

        // Create pending operation
        let pending_op = VllPendingOp::new(txid, keys.clone(), mode, operation, ready_tx, execute_rx);
        if let Err(e) = tx_queue.enqueue(pending_op) {
            intent_table.remove_all_intents(&keys, txid);
            tracing::warn!(shard_id = self.shard_id, txid, error = %e, "Failed to enqueue VLL operation");
            return;
        }

        // Try to acquire locks using SCA
        self.try_acquire_vll_locks(txid).await;
    }

    /// Try to acquire locks for a VLL operation using SCA.
    async fn try_acquire_vll_locks(&mut self, txid: u64) {
        let intent_table = match self.intent_table.as_mut() {
            Some(t) => t,
            None => return,
        };
        let tx_queue = match self.tx_queue.as_mut() {
            Some(q) => q,
            None => return,
        };

        let op = match tx_queue.get_mut(txid) {
            Some(op) => op,
            None => return,
        };

        // Check if we can proceed using SCA
        if !intent_table.can_proceed(&op.keys, txid, op.mode) {
            // Cannot proceed yet - will be retried when earlier operations complete
            return;
        }

        // Try to acquire locks
        if intent_table.try_acquire_locks(&op.keys, op.mode) {
            // Success! Notify coordinator
            if let Some(ready_tx) = op.mark_ready() {
                let _ = ready_tx.send(ShardReadyResult::Ready);
            }
        }
        // If lock acquisition fails, we'll retry later
    }

    /// Handle VLL execute - execute a ready operation.
    async fn handle_vll_execute(&mut self, txid: u64, response_tx: oneshot::Sender<PartialResult>) {
        let tx_queue = match self.tx_queue.as_mut() {
            Some(q) => q,
            None => {
                // No queue means no operation to execute
                let _ = response_tx.send(PartialResult { results: vec![] });
                return;
            }
        };

        // Get and remove the operation
        let op = match tx_queue.dequeue(txid) {
            Some(op) => op,
            None => {
                let _ = response_tx.send(PartialResult { results: vec![] });
                return;
            }
        };

        // Execute the operation
        let result = self.execute_scatter_part(&op.keys, &op.operation).await;

        // Release locks
        if let Some(intent_table) = self.intent_table.as_mut() {
            intent_table.release_locks(&op.keys, op.mode);
            intent_table.remove_all_intents(&op.keys, txid);
        }

        // Send result
        let _ = response_tx.send(result);

        // Try to unblock waiting operations
        self.process_waiting_vll_ops().await;
    }

    /// Handle VLL abort - cleanup a failed operation.
    fn handle_vll_abort(&mut self, txid: u64) {
        // Remove from queue
        if let Some(tx_queue) = self.tx_queue.as_mut() {
            if let Some(op) = tx_queue.dequeue(txid) {
                // Release any held locks and remove intents
                if let Some(intent_table) = self.intent_table.as_mut() {
                    if op.state == crate::vll::PendingOpState::Ready {
                        // Was holding locks
                        intent_table.release_locks(&op.keys, op.mode);
                    }
                    intent_table.remove_all_intents(&op.keys, txid);
                }
            }
        }
    }

    /// Handle VLL continuation lock - acquire full shard lock for MULTI/Lua.
    ///
    /// This is non-blocking: we store the release receiver and poll it in the main loop,
    /// allowing the shard to continue processing messages from the lock owner while locked.
    async fn handle_vll_continuation_lock(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) {
        use crate::vll::VllError;

        // Check if continuation lock is already held
        if self.continuation_lock.is_some() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
            return;
        }

        // Wait for queue to drain
        let drain_timeout = std::time::Duration::from_millis(2000);
        let start = std::time::Instant::now();

        while let Some(tx_queue) = self.tx_queue.as_ref() {
            if tx_queue.is_empty() {
                break;
            }
            if start.elapsed() > drain_timeout {
                let _ = ready_tx.send(ShardReadyResult::Failed(VllError::LockTimeout));
                return;
            }
            // Yield to allow pending ops to complete
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Acquire continuation lock
        self.continuation_lock = Some(crate::vll::ContinuationLock::new(txid, conn_id));

        // Store release receiver for polling in main loop (non-blocking!)
        self.pending_continuation_release = Some(release_rx);

        // Notify ready - shard continues processing messages from lock owner
        let _ = ready_tx.send(ShardReadyResult::Ready);
    }

    /// Process waiting VLL operations after one completes.
    async fn process_waiting_vll_ops(&mut self) {
        let tx_queue = match self.tx_queue.as_ref() {
            Some(q) => q,
            None => return,
        };

        // Get all pending txids
        let pending_txids: Vec<u64> = tx_queue
            .iter()
            .filter(|(_, op)| op.state == crate::vll::PendingOpState::Pending)
            .map(|(&txid, _)| txid)
            .collect();

        // Try to acquire locks for each
        for txid in pending_txids {
            self.try_acquire_vll_locks(txid).await;
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
        protocol_version: ProtocolVersion,
    ) -> Response {
        let start = Instant::now();
        let shard_label = self.shard_id.to_string();

        // EVAL always loads the script (cache miss)
        self.metrics_recorder.increment_counter(
            "frogdb_lua_scripts_cache_misses_total",
            1,
            &[("shard", &shard_label)],
        );

        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                self.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_errors_total",
                    1,
                    &[("shard", &shard_label), ("error", "not_available")],
                );
                return Response::error("ERR scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
            protocol_version,
            None,
            None,
            self.cluster_state.as_ref(),
            self.node_id,
            self.raft.as_ref(),
            self.network_factory.as_ref(),
            self.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        let result = executor.eval(script_source, keys, argv, &mut ctx, &self.registry);
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics
        self.metrics_recorder.increment_counter(
            "frogdb_lua_scripts_total",
            1,
            &[("shard", &shard_label), ("type", "eval")],
        );
        self.metrics_recorder.record_histogram(
            "frogdb_lua_scripts_duration_seconds",
            elapsed,
            &[("shard", &shard_label), ("type", "eval")],
        );

        match result {
            Ok(response) => response,
            Err(e) => {
                self.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_errors_total",
                    1,
                    &[("shard", &shard_label), ("error", "execution")],
                );
                Response::error(e.to_string())
            }
        }
    }

    /// Handle EVALSHA - execute a cached Lua script by SHA.
    fn handle_evalsha(
        &mut self,
        script_sha: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> Response {
        let start = Instant::now();
        let shard_label = self.shard_id.to_string();

        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                self.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_errors_total",
                    1,
                    &[("shard", &shard_label), ("error", "not_available")],
                );
                return Response::error("ERR scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
            protocol_version,
            None,
            None,
            self.cluster_state.as_ref(),
            self.node_id,
            self.raft.as_ref(),
            self.network_factory.as_ref(),
            self.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        let result = executor.evalsha(script_sha, keys, argv, &mut ctx, &self.registry);
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics based on result
        match &result {
            Ok(_) => {
                // EVALSHA success = cache hit
                self.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_cache_hits_total",
                    1,
                    &[("shard", &shard_label)],
                );
                self.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_total",
                    1,
                    &[("shard", &shard_label), ("type", "evalsha")],
                );
                self.metrics_recorder.record_histogram(
                    "frogdb_lua_scripts_duration_seconds",
                    elapsed,
                    &[("shard", &shard_label), ("type", "evalsha")],
                );
            }
            Err(e) => {
                // Check if it's a NOSCRIPT error (cache miss) or execution error
                let error_str = e.to_string();
                if error_str.contains("NOSCRIPT") {
                    self.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_cache_misses_total",
                        1,
                        &[("shard", &shard_label)],
                    );
                    self.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_errors_total",
                        1,
                        &[("shard", &shard_label), ("error", "noscript")],
                    );
                } else {
                    // Execution error after cache hit
                    self.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_cache_hits_total",
                        1,
                        &[("shard", &shard_label)],
                    );
                    self.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_errors_total",
                        1,
                        &[("shard", &shard_label), ("error", "execution")],
                    );
                }
            }
        }

        match result {
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

    // =========================================================================
    // Function execution helpers
    // =========================================================================

    /// Handle FCALL/FCALL_RO - execute a function.
    fn handle_function_call(
        &mut self,
        function_name: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
        read_only: bool,
    ) -> Response {
        // Get function registry
        let registry = match &self.function_registry {
            Some(r) => r,
            None => {
                return Response::error("ERR Functions not available");
            }
        };

        // Get function from registry
        let func_name = String::from_utf8_lossy(function_name);
        let (function, library_name) = {
            let registry_guard = match registry.try_read_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry_guard.get_function(&func_name) {
                Some((func, lib_name)) => (func.clone(), lib_name.to_string()),
                None => {
                    return Response::error(format!(
                        "ERR Function not found: {}",
                        func_name
                    ));
                }
            }
        };

        // Enforce read-only for FCALL_RO
        if read_only && !function.is_read_only() {
            return Response::error(format!(
                "ERR Can't execute a function with write flag using FCALL_RO: {}",
                func_name
            ));
        }

        // Get the library code
        let library_code = {
            let registry_guard = match registry.try_read_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry_guard.get_library(&library_name) {
                Some(lib) => lib.code.clone(),
                None => {
                    return Response::error(format!(
                        "ERR Library not found: {}",
                        library_name
                    ));
                }
            }
        };

        // Execute the function using the script executor
        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                return Response::error("ERR Scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
            protocol_version,
            None,
            None,
            self.cluster_state.as_ref(),
            self.node_id,
            self.raft.as_ref(),
            self.network_factory.as_ref(),
            self.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        match executor.execute_function(
            &func_name,
            &library_code,
            keys,
            argv,
            &mut ctx,
            &self.registry,
            read_only,
        ) {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }

    // =========================================================================
    // Blocking commands helpers
    // =========================================================================

    /// Handle a blocking wait request.
    fn handle_block_wait(
        &mut self,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: crate::types::BlockingOp,
        response_tx: oneshot::Sender<Response>,
        deadline: Option<Instant>,
    ) {
        let keys_count = keys.len();
        let entry = WaitEntry {
            conn_id,
            keys,
            op,
            response_tx,
            deadline,
        };

        if let Err(e) = self.wait_queue.register(entry) {
            tracing::warn!(
                shard_id = self.shard_id,
                conn_id = conn_id,
                error = %e,
                "Failed to register blocking wait"
            );
            // The response_tx was moved into entry, so we can't send an error back here.
            // The client will timeout.
        } else {
            tracing::debug!(
                shard_id = self.shard_id,
                conn_id,
                keys_count,
                "Client blocked on keys"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Handle unregistering a blocking wait (disconnect or explicit cancel).
    fn handle_unregister_wait(&mut self, conn_id: u64) {
        let removed = self.wait_queue.unregister(conn_id);
        if !removed.is_empty() {
            tracing::trace!(
                shard_id = self.shard_id,
                conn_id = conn_id,
                count = removed.len(),
                "Unregistered blocking waits on disconnect"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Check for expired blocking waits and send nil responses.
    fn check_waiter_timeouts(&mut self) {
        let now = Instant::now();
        let expired = self.wait_queue.collect_expired(now);

        if !expired.is_empty() {
            let shard_label = self.shard_id.to_string();

            for entry in expired {
                tracing::trace!(
                    shard_id = self.shard_id,
                    conn_id = entry.conn_id,
                    "Blocking wait timed out"
                );

                // Send nil response for timeout
                let _ = entry.response_tx.send(Response::Null);

                // Increment timeout counter
                self.metrics_recorder.increment_counter(
                    "frogdb_blocked_timeout_total",
                    1,
                    &[("shard", &shard_label)],
                );
            }

            // Update blocked clients gauge
            self.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Check if a list key has non-empty data.
    fn list_is_non_empty(&self, key: &Bytes) -> bool {
        use crate::store::Store;
        if let Some(value) = self.store.get(key) {
            if let Some(list) = value.as_list() {
                return !list.is_empty();
            }
        }
        false
    }

    /// Check if a sorted set key has non-empty data.
    fn zset_is_non_empty(&self, key: &Bytes) -> bool {
        use crate::store::Store;
        if let Some(value) = self.store.get(key) {
            if let Some(zset) = value.as_sorted_set() {
                return !zset.is_empty();
            }
        }
        false
    }

    /// Clean up an empty list key.
    fn cleanup_empty_list(&mut self, key: &Bytes) {
        use crate::store::Store;
        if let Some(value) = self.store.get(key) {
            if let Some(list) = value.as_list() {
                if list.is_empty() {
                    self.store.delete(key);
                }
            }
        }
    }

    /// Clean up an empty sorted set key.
    fn cleanup_empty_zset(&mut self, key: &Bytes) {
        use crate::store::Store;
        if let Some(value) = self.store.get(key) {
            if let Some(zset) = value.as_sorted_set() {
                if zset.is_empty() {
                    self.store.delete(key);
                }
            }
        }
    }

    /// Try to satisfy list waiters after a list write operation.
    ///
    /// Called after LPUSH, RPUSH, LPUSHX, RPUSHX operations.
    pub fn try_satisfy_list_waiters(&mut self, key: &Bytes) {
        use crate::store::Store;
        use crate::types::BlockingOp;

        while self.wait_queue.has_waiters(key) {
            // Check if the list has data
            let has_data = self.list_is_non_empty(key);

            if !has_data {
                break;
            }

            // Pop the oldest waiter
            let entry = match self.wait_queue.pop_oldest_waiter(key) {
                Some(e) => e,
                None => break,
            };

            // Execute the blocking operation
            let response = match &entry.op {
                BlockingOp::BLPop => {
                    // Pop from left and return [key, value]
                    if let Some(value) = self.store.get_mut(key).and_then(|v| v.as_list_mut()).and_then(|l| l.pop_front()) {
                        // Clean up empty list
                        self.cleanup_empty_list(key);
                        self.increment_version();
                        Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::bulk(value),
                        ])
                    } else {
                        continue; // List became empty, try next waiter
                    }
                }
                BlockingOp::BRPop => {
                    // Pop from right and return [key, value]
                    if let Some(value) = self.store.get_mut(key).and_then(|v| v.as_list_mut()).and_then(|l| l.pop_back()) {
                        // Clean up empty list
                        self.cleanup_empty_list(key);
                        self.increment_version();
                        Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::bulk(value),
                        ])
                    } else {
                        continue;
                    }
                }
                BlockingOp::BLMove { dest, src_dir, dest_dir } => {
                    // Pop from source direction
                    let value = match src_dir {
                        Direction::Left => self.store.get_mut(key).and_then(|v| v.as_list_mut()).and_then(|l| l.pop_front()),
                        Direction::Right => self.store.get_mut(key).and_then(|v| v.as_list_mut()).and_then(|l| l.pop_back()),
                    };

                    if let Some(value) = value {
                        // Clean up empty source list
                        self.cleanup_empty_list(key);

                        // Push to destination
                        // Get or create dest list
                        if self.store.get(dest).is_none() {
                            self.store.set(dest.clone(), crate::types::Value::list());
                        }

                        if let Some(dest_list) = self.store.get_mut(dest).and_then(|v| v.as_list_mut()) {
                            match dest_dir {
                                Direction::Left => dest_list.push_front(value.clone()),
                                Direction::Right => dest_list.push_back(value.clone()),
                            }
                        }

                        self.increment_version();
                        Response::bulk(value)
                    } else {
                        continue;
                    }
                }
                BlockingOp::BLMPop { direction, count } => {
                    let mut elements = Vec::new();
                    if let Some(list) = self.store.get_mut(key).and_then(|v| v.as_list_mut()) {
                        for _ in 0..*count {
                            let elem = match direction {
                                Direction::Left => list.pop_front(),
                                Direction::Right => list.pop_back(),
                            };
                            match elem {
                                Some(e) => elements.push(Response::bulk(e)),
                                None => break,
                            }
                        }
                    }

                    if elements.is_empty() {
                        continue;
                    }

                    // Clean up empty list
                    self.cleanup_empty_list(key);

                    self.increment_version();
                    Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::Array(elements),
                    ])
                }
                _ => continue, // Not a list operation
            };

            // Calculate wait duration (approximate since we don't track start time)
            tracing::debug!(
                shard_id = self.shard_id,
                conn_id = entry.conn_id,
                "Blocked client unblocked"
            );

            // Send response
            let _ = entry.response_tx.send(response);

            // Increment satisfied counter
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_blocked_satisfied_total",
                1,
                &[("shard", &shard_label)],
            );

            // Update blocked clients gauge
            self.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Try to satisfy sorted set waiters after a sorted set write operation.
    ///
    /// Called after ZADD operations.
    pub fn try_satisfy_zset_waiters(&mut self, key: &Bytes) {
        use crate::store::Store;
        use crate::types::BlockingOp;

        while self.wait_queue.has_waiters(key) {
            // Check if the zset has data
            let has_data = self.zset_is_non_empty(key);

            if !has_data {
                break;
            }

            // Pop the oldest waiter
            let entry = match self.wait_queue.pop_oldest_waiter(key) {
                Some(e) => e,
                None => break,
            };

            // Execute the blocking operation
            let response = match &entry.op {
                BlockingOp::BZPopMin => {
                    // Pop minimum element
                    if let Some(zset) = self.store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) {
                        let popped = zset.pop_min(1);
                        let is_empty = zset.is_empty();
                        if let Some((member, score)) = popped.into_iter().next() {
                            // Clean up empty zset
                            if is_empty {
                                self.store.delete(key);
                            }
                            self.increment_version();
                            Response::Array(vec![
                                Response::bulk(key.clone()),
                                Response::bulk(member),
                                Response::bulk(Bytes::from(score.to_string())),
                            ])
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                BlockingOp::BZPopMax => {
                    // Pop maximum element
                    if let Some(zset) = self.store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) {
                        let popped = zset.pop_max(1);
                        let is_empty = zset.is_empty();
                        if let Some((member, score)) = popped.into_iter().next() {
                            // Clean up empty zset
                            if is_empty {
                                self.store.delete(key);
                            }
                            self.increment_version();
                            Response::Array(vec![
                                Response::bulk(key.clone()),
                                Response::bulk(member),
                                Response::bulk(Bytes::from(score.to_string())),
                            ])
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                BlockingOp::BZMPop { min, count } => {
                    let mut elements = Vec::new();
                    if let Some(zset) = self.store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) {
                        let popped = if *min {
                            zset.pop_min(*count)
                        } else {
                            zset.pop_max(*count)
                        };
                        for (member, score) in popped {
                            elements.push(Response::Array(vec![
                                Response::bulk(member),
                                Response::bulk(Bytes::from(score.to_string())),
                            ]));
                        }
                    }

                    if elements.is_empty() {
                        continue;
                    }

                    // Clean up empty zset
                    self.cleanup_empty_zset(key);

                    self.increment_version();
                    Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::Array(elements),
                    ])
                }
                _ => continue, // Not a zset operation
            };

            tracing::debug!(
                shard_id = self.shard_id,
                conn_id = entry.conn_id,
                "Blocked client unblocked"
            );

            // Send response
            let _ = entry.response_tx.send(response);

            // Increment satisfied counter
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_blocked_satisfied_total",
                1,
                &[("shard", &shard_label)],
            );

            // Update blocked clients gauge
            self.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Try to satisfy stream waiters after a stream write operation.
    ///
    /// Called after XADD operations.
    pub fn try_satisfy_stream_waiters(&mut self, key: &Bytes) {
        use crate::store::Store;
        use crate::types::{BlockingOp, StreamEntry};

        while self.wait_queue.has_waiters(key) {
            // Check if the stream exists
            let stream_exists = self
                .store
                .get(key)
                .map(|v| v.as_stream().is_some())
                .unwrap_or(false);
            if !stream_exists {
                break;
            }

            // Pop the oldest waiter
            let entry = match self.wait_queue.pop_oldest_waiter(key) {
                Some(e) => e,
                None => break,
            };

            // Execute the blocking operation
            let response = match &entry.op {
                BlockingOp::XRead { after_ids, count } => {
                    // Find key index and read after that ID
                    let key_idx = entry.keys.iter().position(|k| k == key).unwrap_or(0);
                    let after_id = &after_ids[key_idx];

                    // Read entries from stream
                    let entries: Vec<StreamEntry> = match self.store.get(key) {
                        Some(value) => match value.as_stream() {
                            Some(stream) => stream.read_after(after_id, *count),
                            None => Vec::new(),
                        },
                        None => Vec::new(),
                    };

                    if entries.is_empty() {
                        // No new entries yet, continue to next waiter
                        continue;
                    }

                    // Format: [[key, [[id, [field, value, ...]], ...]]]
                    format_xread_response(key, &entries)
                }

                BlockingOp::XReadGroup {
                    group,
                    consumer,
                    noack,
                    count,
                } => {
                    // Read new entries and update PEL
                    let result: Option<Vec<crate::types::StreamEntry>> =
                        self.read_group_entries(key, group, consumer, *noack, *count);
                    match result {
                        Some(entries) if !entries.is_empty() => {
                            format_xread_response(key, &entries)
                        }
                        _ => continue,
                    }
                }

                _ => continue, // Not a stream operation
            };

            tracing::debug!(
                shard_id = self.shard_id,
                conn_id = entry.conn_id,
                "Blocked client unblocked"
            );

            // Send response
            let _ = entry.response_tx.send(response);

            // Increment satisfied counter
            let shard_label = self.shard_id.to_string();
            self.metrics_recorder.increment_counter(
                "frogdb_blocked_satisfied_total",
                1,
                &[("shard", &shard_label)],
            );

            // Update blocked clients gauge
            self.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Read entries for XREADGROUP and update group state.
    fn read_group_entries(
        &mut self,
        key: &Bytes,
        group_name: &Bytes,
        consumer_name: &Bytes,
        noack: bool,
        count: Option<usize>,
    ) -> Option<Vec<crate::types::StreamEntry>> {

        let stream = self.store.get_mut(key)?.as_stream_mut()?;
        let group = stream.get_group_mut(group_name)?;

        let last_delivered = group.last_delivered_id;
        let new_entries = stream.read_after(&last_delivered, count);

        if new_entries.is_empty() {
            return None;
        }

        // Update last_delivered_id and add to PEL
        if let Some(last) = new_entries.last() {
            let group = stream.get_group_mut(group_name)?;
            group.last_delivered_id = last.id;

            if !noack {
                for entry in &new_entries {
                    group.add_pending(entry.id, consumer_name.clone());
                }
            }
        }

        Some(new_entries)
    }

    /// Calculate memory usage for a specific key.
    ///
    /// Returns None if the key doesn't exist.
    fn calculate_key_memory_usage(&self, key: &[u8]) -> Option<usize> {
        let value = self.store.get(key)?;

        // Calculate approximate memory usage:
        // - Key size
        // - Value size (using memory_size method on Value)
        // - Overhead for metadata, expiry tracking, etc.
        let key_size = key.len();
        let value_size = value.memory_size();
        let overhead = std::mem::size_of::<crate::types::KeyMetadata>() + 64; // Rough estimate for hashmap entry overhead

        Some(key_size + value_size + overhead)
    }

    /// Collect memory statistics for this shard.
    fn collect_memory_stats(&self) -> ShardMemoryStats {
        let data_memory = self.store.memory_used();
        let keys = self.store.len();

        // Estimate overhead: hashmap overhead per key + shard-level structures
        let per_key_overhead = 64; // Rough estimate for HashMap entry overhead
        let overhead_estimate = keys * per_key_overhead + 1024; // Plus shard structures

        ShardMemoryStats {
            shard_id: self.shard_id,
            data_memory,
            keys,
            peak_memory: self.peak_memory,
            memory_limit: self.memory_limit,
            overhead_estimate,
        }
    }

    /// Collect WAL lag statistics for this shard.
    async fn collect_wal_lag_stats(&self) -> WalLagStatsResponse {
        if let Some(ref wal_writer) = self.wal_writer {
            let lag_stats = wal_writer.lag_stats().await;
            WalLagStatsResponse {
                shard_id: self.shard_id,
                persistence_enabled: true,
                lag_stats: Some(lag_stats),
            }
        } else {
            WalLagStatsResponse {
                shard_id: self.shard_id,
                persistence_enabled: false,
                lag_stats: None,
            }
        }
    }

    /// Scan for big keys (keys larger than threshold_bytes).
    fn scan_big_keys(&self, threshold_bytes: usize, max_keys: usize) -> BigKeysScanResponse {
        let all_keys = self.store.all_keys();
        let keys_scanned = all_keys.len();
        let mut big_keys = Vec::new();

        for key in all_keys {
            if let Some(memory) = self.calculate_key_memory_usage(&key) {
                if memory >= threshold_bytes {
                    if let Some(value) = self.store.get(&key) {
                        big_keys.push(BigKeyInfo {
                            key: key.clone(),
                            key_type: value.key_type().as_str().to_string(),
                            memory_bytes: memory,
                        });
                        if big_keys.len() >= max_keys {
                            break;
                        }
                    }
                }
            }
        }

        // Sort by memory usage descending
        big_keys.sort_by(|a, b| b.memory_bytes.cmp(&a.memory_bytes));

        // Calculate truncated before moving big_keys
        let truncated = big_keys.len() >= max_keys;

        BigKeysScanResponse {
            shard_id: self.shard_id,
            big_keys,
            keys_scanned,
            truncated,
        }
    }

    /// Calculate hot shard statistics for the given period.
    fn calculate_hot_shard_stats(&mut self, period_secs: u64) -> HotShardStatsResponse {
        let (ops_per_sec, reads_per_sec, writes_per_sec) =
            self.operation_counters.calculate_ops_per_sec(period_secs);

        HotShardStatsResponse {
            shard_id: self.shard_id,
            ops_per_sec,
            reads_per_sec,
            writes_per_sec,
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
        }
    }

    /// Collect VLL queue information for debugging.
    fn collect_vll_queue_info(&self) -> VllQueueInfo {
        let mut info = VllQueueInfo {
            shard_id: self.shard_id,
            ..Default::default()
        };

        // Collect queue depth and pending ops
        if let Some(ref tx_queue) = self.tx_queue {
            info.queue_depth = tx_queue.len();

            // Find executing txid (the one with Executing state)
            for (_, op) in tx_queue.iter() {
                if op.state == crate::vll::PendingOpState::Executing {
                    info.executing_txid = Some(op.txid);
                }

                info.pending_ops.push(VllPendingOpInfo {
                    txid: op.txid,
                    operation: Self::format_scatter_op(&op.operation),
                    key_count: op.keys.len(),
                    state: format!("{:?}", op.state),
                    age_ms: op.age().as_millis() as u64,
                });
            }
        }

        // Collect continuation lock info
        if let Some(ref lock) = self.continuation_lock {
            info.continuation_lock = Some(VllContinuationLockInfo {
                txid: lock.txid,
                conn_id: lock.conn_id,
                age_ms: lock.age().as_millis() as u64,
            });
        }

        // Collect intent table info
        if let Some(ref intent_table) = self.intent_table {
            for (key, txids) in intent_table.iter_keys() {
                let lock_state = intent_table.get_lock_state_string(key);
                info.intent_table.push(VllKeyIntentInfo {
                    key: Self::format_key_for_display(key),
                    txids,
                    lock_state,
                });
            }
        }

        info
    }

    /// Format a ScatterOp for display.
    fn format_scatter_op(op: &ScatterOp) -> String {
        match op {
            ScatterOp::MGet => "MGET".to_string(),
            ScatterOp::MSet { .. } => "MSET".to_string(),
            ScatterOp::Del => "DEL".to_string(),
            ScatterOp::Exists => "EXISTS".to_string(),
            ScatterOp::Touch => "TOUCH".to_string(),
            ScatterOp::Unlink => "UNLINK".to_string(),
            ScatterOp::Keys { .. } => "KEYS".to_string(),
            ScatterOp::DbSize => "DBSIZE".to_string(),
            ScatterOp::FlushDb => "FLUSHDB".to_string(),
            ScatterOp::Scan { .. } => "SCAN".to_string(),
            ScatterOp::Copy { .. } => "COPY".to_string(),
            ScatterOp::CopySet { .. } => "COPYSET".to_string(),
            ScatterOp::RandomKey => "RANDOMKEY".to_string(),
            ScatterOp::Dump => "DUMP".to_string(),
        }
    }

    /// Format a key for display, truncating if too long.
    fn format_key_for_display(key: &Bytes) -> String {
        const MAX_KEY_DISPLAY_LEN: usize = 64;
        match std::str::from_utf8(key) {
            Ok(s) => {
                if s.len() > MAX_KEY_DISPLAY_LEN {
                    format!("{}...", &s[..MAX_KEY_DISPLAY_LEN])
                } else {
                    s.to_string()
                }
            }
            Err(_) => {
                // Binary key - show hex
                let hex: String = key.iter().take(32).map(|b| format!("{:02x}", b)).collect();
                if key.len() > 32 {
                    format!("0x{}...", hex)
                } else {
                    format!("0x{}", hex)
                }
            }
        }
    }
}

/// Format XREAD response for a single stream.
fn format_xread_response(key: &Bytes, entries: &[crate::types::StreamEntry]) -> Response {
    let entry_responses: Vec<Response> = entries
        .iter()
        .map(|entry| {
            let id = Response::bulk(Bytes::from(entry.id.to_string()));
            let fields: Vec<Response> = entry
                .fields
                .iter()
                .flat_map(|(k, v)| vec![Response::bulk(k.clone()), Response::bulk(v.clone())])
                .collect();
            Response::Array(vec![id, Response::Array(fields)])
        })
        .collect();

    Response::Array(vec![Response::Array(vec![
        Response::bulk(key.clone()),
        Response::Array(entry_responses),
    ])])
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

/// Reserved connection ID for internally replicated commands.
///
/// Commands received via replication should use this connection ID to prevent
/// them from being re-broadcast back to replicas, which would cause infinite loops.
/// Connection IDs for real clients start at 1 (from NEXT_CONN_ID in server.rs).
pub const REPLICA_INTERNAL_CONN_ID: u64 = 0;

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
