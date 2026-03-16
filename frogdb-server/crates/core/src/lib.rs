//! FrogDB Core
//!
//! Core data structures, command traits, and storage implementations for FrogDB.
//! This crate provides the foundational types used by the server.

// Re-export everything from frogdb-types so downstream crates continue working unchanged.
pub use frogdb_types::*;

// Re-export frogdb-cluster as the cluster module for backward compatibility.
pub use frogdb_cluster as cluster;

// Re-export frogdb-acl as the acl module for backward compatibility.
pub use frogdb_acl as acl;
pub mod client_registry;
pub mod command;
pub mod command_macro;
pub mod error;
pub mod eviction;
// Re-export frogdb-scripting as the functions module for backward compatibility.
pub use frogdb_scripting as functions;
pub mod latency;
pub mod metrics;
pub mod noop;
pub mod persistence;
pub mod probes;
pub mod pubsub;
pub mod registry;
// Re-export frogdb-replication as the replication module for backward compatibility.
pub use frogdb_replication as replication;
pub mod scripting;
pub mod shard;
pub mod slowlog;
pub mod store;

// Re-export frogdb-vll as the vll module for backward compatibility.
pub use frogdb_vll as vll;

pub use acl::{
    AclChecker, AclConfig, AclError, AclLog, AclManager, AllowAllChecker, AuthenticatedUser,
    CommandCategory, FullAclChecker, KeyAccessType, PermissionResult, User, UserPermissions,
    generate_password, hash_password,
};
pub use client_registry::{
    ClientFlags, ClientHandle, ClientInfo, ClientRegistry, ClientStats, ClientStatsDelta,
    CommandTypeStats, KillFilter, PauseMode, UnblockMode,
};
pub use cluster::{
    CLUSTER_SLOTS, ClusterCommand, ClusterConfig, ClusterError, ClusterNetwork,
    ClusterNetworkFactory, ClusterRaft, ClusterResponse, ClusterRpcRequest, ClusterRpcResponse,
    ClusterSnapshot, ClusterState, ClusterStateMachine, ClusterStorage, ConfigEpoch, DemotionEvent,
    NodeId, NodeInfo, NodeRole, SharedClusterRaft, SlotMigrationCompleteEvent, SlotRange,
    TypeConfig, handle_rpc_request, parse_rpc_message, send_rpc_response,
};
pub use command::{
    Arity, ClusterContextRef, Command, CommandContext, CommandContextCore, CommandFlags,
    CommandMetadata, ConnectionLevelOp, ExecutionStrategy, ListpackConfig, MergeStrategy,
    QuorumChecker, ReplicationContextRef, ServerWideOp, WaiterKind, WalStrategy, get_or_create,
};
pub use error::FrogDbError;
pub use eviction::{
    DEFAULT_LFU_DECAY_TIME, DEFAULT_LFU_LOG_FACTOR, DEFAULT_MAXMEMORY_SAMPLES, EVICTION_POOL_SIZE,
    EvictionCandidate, EvictionConfig, EvictionPolicy, EvictionPool, lfu_decay, lfu_log_incr,
};
pub use functions::{
    CapturedRegistration, FunctionError, FunctionFlags, FunctionLibrary, FunctionRegistry,
    FunctionStats, ParsedLibrary, RegisteredFunction, RestorePolicy, RunningFunctionInfo,
    SharedFunctionRegistry, ShebangInfo, dump_libraries, load_from_file, load_library,
    new_shared_registry, parse_shebang, restore_libraries, save_to_file, validate_library,
};
pub use latency::{
    CommandHistogram, DEFAULT_LATENCY_HISTORY_LEN, DEFAULT_LATENCY_THRESHOLD_MS, EventHistory,
    EventStats, LatencyEvent, LatencyMonitor, LatencySample, generate_latency_graph,
};
pub use metrics::{
    HotShardDetector, HotShardReport, MemoryDiagnosticsCollector, MemoryReport, NoopObservability,
    ObservabilityConfig,
};
pub use noop::ExpiryIndex;
pub use persistence::{
    CompressionType, DurabilityMode, HEADER_SIZE, NoopSnapshotCoordinator, OnWriteHook,
    RecoveryStats, RocksConfig, RocksSnapshotCoordinator, RocksStore, RocksWalWriter,
    SerializationError, SnapshotConfig, SnapshotCoordinator, SnapshotError, SnapshotHandle,
    SnapshotMetadata, SnapshotMetadataFile, WalConfig, WalLagStats, deserialize,
    recover_all_shards, recover_shard, serialize, spawn_periodic_sync,
};
pub use pubsub::{
    ConnId, GlobPattern, IntrospectionRequest, IntrospectionResponse,
    MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SUBSCRIPTIONS_PER_CONNECTION, PubSubMessage, PubSubSender, ShardSubscriptions,
};
pub use registry::{CommandEntry, CommandRegistry};
pub use replication::{
    FRAME_MAGIC, FRAME_VERSION, FullSyncState, NoopBroadcaster, PrimaryReplicationHandler,
    ReplicaConnection, ReplicaInfo, ReplicaReplicationHandler, ReplicationBroadcaster,
    ReplicationFrame, ReplicationFrameCodec, ReplicationState, ReplicationTrackerImpl,
    SharedBroadcaster, serialize_command_to_resp,
};
pub use scripting::{
    CachedScript, CrossShardRouter, LuaVm, ScriptCache, ScriptError, ScriptExecutor, ScriptRoute,
    ScriptRouter, ScriptingConfig, SingleShardRouter,
};
pub use shard::{
    BigKeyInfo, BigKeysScanResponse, HotShardStatsResponse, PartialResult, REDIS_CLUSTER_SLOTS,
    REPLICA_INTERNAL_CONN_ID, ScatterOp, ShardBuilderError, ShardClusterDeps, ShardConfig,
    ShardCoreDeps, ShardMemoryStats, ShardMessage, ShardPersistenceDeps, ShardWaitQueue,
    ShardWorker, ShardWorkerBuilder, TransactionResult, WaitEntry, WalLagStatsResponse,
    extract_hash_tag, shard_for_key, slot_for_key,
};
pub use slowlog::{
    DEFAULT_SLOWLOG_LOG_SLOWER_THAN, DEFAULT_SLOWLOG_MAX_ARG_LEN, DEFAULT_SLOWLOG_MAX_LEN, SlowLog,
    SlowLogEntry,
};
pub use store::ValueType;
pub use store::{HashMapStore, Store};
pub use vll::{
    ContinuationLock, ExecuteSignal, IntentTable, KeyLockState, LockMode, PendingOpState,
    ShardReadyResult, VllCommand, VllConfig, VllError, VllShardResult,
};

/// VllPendingOp specialized with ScatterOp as the operation type.
pub type VllPendingOp = vll::VllPendingOp<ScatterOp>;

/// TransactionQueue specialized with ScatterOp as the operation type.
pub type TransactionQueue = vll::TransactionQueue<ScatterOp>;
