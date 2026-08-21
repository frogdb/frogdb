//! FrogDB Core
//!
//! Core data structures, command traits, and storage implementations for FrogDB.
//! This crate provides the foundational types used by the server.

// Re-export everything from frogdb-types so downstream crates continue working unchanged.
pub use frogdb_types::*;

/// Reply sent to every client parked in a blocking command when this node stops
/// being a primary.
///
/// One string for both halves of the release: `WAIT` (which watches the role
/// fence itself, `specs/blocking.md` FM-BLOCKING-010) and the non-`WAIT` waiters
/// the demotion path drains off the shards (FM-BLOCKING-007). Redis reports the
/// same event from `disconnectAllBlockedClients`, and clients match on the
/// `UNBLOCKED` prefix, so the text is wire-compatible rather than merely
/// descriptive.
pub const ROLE_CHANGED_UNBLOCK_ERR: &str =
    "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)";

// Re-export frogdb-cluster as the cluster module for backward compatibility.
pub use frogdb_cluster as cluster;

// Re-export frogdb-acl as the acl module for backward compatibility.
pub use frogdb_acl as acl;
pub mod client_registry;
pub mod command;
pub mod command_spec;
pub mod conn_command;
pub mod error;
pub mod eviction;
pub mod hotkeys;
pub mod latency_histogram;
// Re-export frogdb-scripting as the functions module for backward compatibility.
pub use frogdb_scripting as functions;
pub mod keyspace_event;
pub mod keyspace_stats;
pub mod latency;
pub mod metrics;
pub mod noop;
pub mod observability;
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
pub mod tracking;
pub mod write_seam;

// Re-export frogdb-vll as the vll module for backward compatibility.
pub use frogdb_vll as vll;

pub use acl::{
    AclConfig, AclError, AclLog, AclManager, AuthenticatedUser, CommandCategory, FullAclChecker,
    KeyAccessType, PermissionResult, PermissionSet, RateLimitExceeded, RateLimitState, User,
    generate_password, hash_password,
};
pub use client_registry::{
    ClientEdge, ClientFlags, ClientHandle, ClientInfo, ClientMemoryUsage, ClientRegistry,
    ClientStats, ClientStatsDelta, CommandTypeStats, ErrorStats, KillFilter, PauseMode,
    PauseOverview, ServerCommandStats, UnblockMode, extract_error_prefix,
};
pub use cluster::{
    BusRpc, CLUSTER_SLOTS, ClusterBusStats, ClusterBusStatsSnapshot, ClusterCommand, ClusterConfig,
    ClusterError, ClusterNetwork, ClusterNetworkFactory, ClusterRaft, ClusterResponse,
    ClusterRpcRequest, ClusterRpcResponse, ClusterSnapshot, ClusterState, ClusterStateMachine,
    ClusterStorage, ConfigEpoch, DemotionEvent, FramedStream, HANDOFF_BARRIER_MS,
    HANDOFF_DRAIN_TIMEOUT_MS, HANDOFF_DRAIN_WAIT_MS, HANDOFF_LEASE_MS, HANDOFF_POLL_INTERVAL_MS,
    NodeId, NodeInfo, NodeRole, PromotionEvent, RaftRpc, RoleChangeEvent, RoleReconcile,
    SelfRoleReconciler, SharedClusterRaft, SlotHandoff, SlotHandoffEvent, SlotMigration,
    SlotMigrationCompleteEvent, SlotRange, TypeConfig, Violation, handle_rpc_request, new_framed,
    new_framed_tcp, parse_rpc_message, send_rpc_response,
};
/// The shard-local executor trait. Alias of [`command::Command`], whose
/// `execute(&mut CommandContext)` runs on the owning shard. Named `ShardCommand`
/// at the registry seam to contrast with [`conn_command::ConnectionCommand`]
/// (the connection-level executor) in the [`registry::CommandImpl`] union.
pub use command::Command as ShardCommand;
pub use command::{
    Arity, ClusterContextRef, Command, CommandContext, CommandContextCore, CommandFlags,
    ConnMutation, ConnectionLevelOp, ExecutionStrategy, KeyAccessFlag, ListpackConfig,
    QuorumChecker, ReplicationContextRef, ReplicationOverride, RoleController, ScatterGatherOp,
    ServerWideOp, SynthesizedCommand, WaiterKind, WaiterWake, WalAction, WalStrategy,
};
pub use command_spec::{
    AccessSpec, AdminSurface, CommandDocs, CommandSpec, EventSpec, IndexKind, KeySpec,
    LookupOutcome, LookupSpec, ReindexAction, ReindexSpec, SpecError, admin_surface,
    split_admin_surface_commands,
};
pub use conn_command::{
    BoxFuture, ClientTrackingProvider, ConfigProvider, ConnCtx, ConnStateMut, ConnectionCommand,
    CursorReadBatch, CursorRow, CursorStoreProvider, DebugProvider, HotkeyClusterProvider,
    InfoProvider, MemoryDiagProvider, MonitorProvider, NoopInfoProvider, NoopScriptingProvider,
    NoopStatusProvider, PubSubProvider, ResetOutcome, ScriptingProvider, StatusProvider,
    TrackingInfoView, TrackingModeView, TxnDiscardOutcome,
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
pub use hotkeys::{
    HotkeyEntry, HotkeyMetric, HotkeySession, HotkeySessionConfig, HotkeySessionState,
    SharedHotkeySession, new_shared_hotkey_session,
};
pub use keyspace_event::KeyspaceEventFlags;
pub use keyspace_stats::KeyspaceStats;
pub use latency::{
    CommandHistogram, DEFAULT_LATENCY_HISTORY_LEN, DEFAULT_LATENCY_THRESHOLD_MS, EventHistory,
    EventStats, LatencyEvent, LatencyMonitor, LatencySample, generate_latency_graph,
};
pub use latency_histogram::{CommandLatencyHistograms, LatencyHistogram};
pub use metrics::{
    HotShardDetector, HotShardSnapshot, MemoryDiagnosticsCollector, MemoryReport,
    NoopObservability, ObservabilityConfig, ShardLoad, ShardLoadClass, WriteFenceReporter,
};
pub use noop::ExpiryIndex;
pub use observability::{ShardWalLag, WalLagAggregate};
pub use persistence::{
    CompressionType, DurabilityMode, FULL_SYNC_HOLD, FlushHold, HEADER_SIZE,
    NoopSnapshotCoordinator, RecoveryStats, RocksConfig, RocksSnapshotCoordinator, RocksStore,
    RocksWalWriter, SerializationError, SnapshotConfig, SnapshotCoordinator, SnapshotError,
    SnapshotHandle, SnapshotMetadata, SnapshotMetadataFile, SnapshotRequest, SnapshotStats,
    WalConfig, WalFailurePolicy, WalLagStats, deserialize, recover_all_shards, recover_shard,
    serialize, spawn_periodic_sync,
};
pub use pubsub::{
    ConnId, DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT, Drained, GlobPattern, IntrospectionRequest,
    IntrospectionResponse, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION, PubSubClosed,
    PubSubConfirmation, PubSubMessage, PubSubReceiver, PubSubSender, ShardSubscriptions,
};
pub use registry::{CommandEntry, CommandImpl, CommandRegistry};
pub use replication::{
    FRAME_MAGIC, FRAME_VERSION, NoopBroadcaster, PrimaryReplicationHandler, ReplicaConnection,
    ReplicaInfo, ReplicaReplicationHandler, ReplicationBroadcaster, ReplicationFrame,
    ReplicationFrameCodec, ReplicationState, ReplicationTrackerImpl, SharedBroadcaster,
    StagedReplicationMetadata, StreamedFrame, ack_is_fresh, consume_staged_replication_metadata,
    read_staged_replication_metadata, serialize_command_to_resp,
};
pub use scripting::{
    CachedScript, LuaVm, ScriptCache, ScriptError, ScriptExecutor, ScriptingConfig,
};
#[cfg(any(test, feature = "shard-driver"))]
pub use shard::TickKind;
pub use shard::{
    BigKeyInfo, BigKeysScanResponse, BlockingMsg, ClusterMsg, CoreMsg, DebugIntrospectionMsg,
    Envelope, HotShardStatsResponse, IndexLifecycleManager, InfoShardSnapshot, LifecycleError,
    ObservabilityMsg, OperationCounters, PartialResult, PubSubMsg, REDIS_CLUSTER_SLOTS,
    REPLICA_INTERNAL_CONN_ID, RecoveryOutcome, RecoveryResult, ReplicationMsg, ScatterOp,
    ScriptingMsg, SearchMsg, ShardBuilderError, ShardClusterDeps, ShardConfig, ShardCoreDeps,
    ShardMemoryStats, ShardMessage, ShardPersistenceDeps, ShardReceiver, ShardSender,
    ShardWaitQueue, ShardWorker, ShardWorkerBuilder, SnapshotEntry, TieredCounts, TrackingMsg,
    TransactionResult, UnregisterAck, VllMsg, WaitEntry, WalDrainAck, WalLagStatsResponse,
    WatchEntry, extract_hash_tag, shard_for_key, slot_for_key,
};
pub use slowlog::{
    DEFAULT_SLOWLOG_LOG_SLOWER_THAN, DEFAULT_SLOWLOG_MAX_ARG_LEN, DEFAULT_SLOWLOG_MAX_LEN, SlowLog,
    SlowLogEntry,
};
pub use store::{DefaultValueType, ValueType};
pub use store::{
    ExpiryIndexAnomaly, ExpiryIndexAnomalyKind, HashMapStore, Store, StoreTypedExt,
    StoreTypedFamilyExt, TypedArc, WrongTypeError,
};
pub use tracking::{
    BroadcastTable, DEFAULT_TRACKING_TABLE_MAX_KEYS, InvalidationMessage, InvalidationRegistry,
    InvalidationSender, TrackedConnection, TrackingTable,
};
pub use vll::{LockMode, PendingOpState, ShardReadyResult, VllError};
