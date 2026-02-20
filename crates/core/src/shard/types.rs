use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;
use frogdb_protocol::Response;
use tokio::sync::mpsc;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::EvictionConfig;
use crate::persistence::RocksStore;
use crate::persistence::SnapshotCoordinator;
use crate::registry::CommandRegistry;
use crate::replication::SharedBroadcaster;
use crate::scripting::ScriptingConfig;

use super::message::{ScatterOp, ShardMessage};

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

// ============================================================================
// Response / metadata types
// ============================================================================

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
