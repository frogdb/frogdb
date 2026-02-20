//! FrogDB Core
//!
//! Core data structures, command traits, and storage implementations for FrogDB.
//! This crate provides the foundational types used by the server.

pub mod acl;
pub mod args;
pub mod bitmap;
pub mod bloom;
pub mod client_registry;
pub mod cluster;
pub mod command;
pub mod command_macro;
pub mod error;
pub mod eviction;
pub mod functions;
pub mod geo;
pub mod glob;
pub mod hyperloglog;
pub mod json;
pub mod latency;
pub mod noop;
pub mod persistence;
pub mod pubsub;
pub mod registry;
pub mod replication;
pub mod scripting;
pub mod shard;
pub mod slowlog;
pub mod store;
pub mod sync;
pub use sync::{LockError, MutexExt, RwLockExt};
pub mod metrics;
pub mod timeseries;
pub mod traits;
pub mod types;
pub mod vll;

pub use acl::{
    generate_password, hash_password, AclChecker, AclConfig, AclError, AclLog, AclManager,
    AllowAllChecker, AuthenticatedUser, CommandCategory, FullAclChecker, KeyAccessType,
    PermissionResult, User, UserPermissions,
};
pub use args::{
    parse_f64, parse_from_bytes, parse_i64, parse_u64, parse_usize, ArgParser, CompareCondition,
    ExpiryOption, ScanOptions,
};
pub use bitmap::{
    bitcount, bitfield_get, bitfield_incrby, bitfield_set, bitop, bitpos, getbit, setbit, BitOp,
    BitfieldEncoding, BitfieldOffset, BitfieldSubCommand, OverflowMode,
};
pub use bloom::{BloomFilterValue, BloomLayer};
pub use client_registry::{
    ClientFlags, ClientHandle, ClientInfo, ClientRegistry, ClientStats, ClientStatsDelta,
    CommandTypeStats, KillFilter, PauseMode, UnblockMode,
};
pub use cluster::{
    handle_rpc_request, parse_rpc_message, send_rpc_response, ClusterCommand, ClusterConfig,
    ClusterError, ClusterNetwork, ClusterNetworkFactory, ClusterRaft, ClusterResponse,
    ClusterRpcRequest, ClusterRpcResponse, ClusterSnapshot, ClusterState, ClusterStateMachine,
    ClusterStorage, ConfigEpoch, NodeId, NodeInfo, NodeRole, SharedClusterRaft, SlotRange,
    TypeConfig, CLUSTER_SLOTS,
};
pub use command::{
    get_or_create, Arity, ClusterContextRef, Command, CommandContext, CommandContextCore,
    CommandFlags, CommandMetadata, ConnectionLevelOp, ExecutionStrategy, MergeStrategy,
    QuorumChecker, ReplicationContextRef, ServerWideOp,
};
pub use error::{CommandError, FrogDbError, RespError};
pub use eviction::{
    lfu_decay, lfu_log_incr, EvictionCandidate, EvictionConfig, EvictionPolicy, EvictionPool,
    DEFAULT_LFU_DECAY_TIME, DEFAULT_LFU_LOG_FACTOR, DEFAULT_MAXMEMORY_SAMPLES, EVICTION_POOL_SIZE,
};
pub use functions::{
    dump_libraries, load_from_file, load_library, new_shared_registry, parse_shebang,
    restore_libraries, save_to_file, validate_library, CapturedRegistration, FunctionError,
    FunctionFlags, FunctionLibrary, FunctionRegistry, FunctionStats, ParsedLibrary,
    RegisteredFunction, RestorePolicy, RunningFunctionInfo, SharedFunctionRegistry, ShebangInfo,
};
pub use geo::{
    geohash_decode, geohash_encode, geohash_range_for_bbox, geohash_to_score, geohash_to_string,
    haversine_distance, is_within_box, is_within_radius, score_to_geohash, BoundingBox,
    Coordinates, DistanceUnit, EARTH_RADIUS_M, GEOHASH_BITS, LAT_MAX, LAT_MIN, LON_MAX, LON_MIN,
};
pub use glob::glob_match;
pub use hyperloglog::{HyperLogLogValue, HLL_DENSE_SIZE, HLL_REGISTERS};
pub use json::{
    JsonError, JsonLimits, JsonType, JsonValue, DEFAULT_JSON_MAX_DEPTH, DEFAULT_JSON_MAX_SIZE,
};
pub use latency::{
    generate_latency_graph, CommandHistogram, EventHistory, EventStats, LatencyEvent,
    LatencyMonitor, LatencySample, DEFAULT_LATENCY_HISTORY_LEN, DEFAULT_LATENCY_THRESHOLD_MS,
};
pub use metrics::{
    HotShardDetector, HotShardReport, MemoryDiagnosticsCollector, MemoryReport, NoopObservability,
    ObservabilityConfig,
};
pub use noop::{
    ExpiryIndex, MetricsRecorder, NoopMetricsRecorder, NoopReplicationTracker, NoopTracer,
    NoopWalWriter, ReplicationConfig, ReplicationTracker, Tracer, WalOperation, WalWriter,
};
pub use persistence::{
    deserialize, recover_all_shards, recover_shard, serialize, spawn_periodic_sync,
    CompressionType, DurabilityMode, NoopSnapshotCoordinator, OnWriteHook, RecoveryStats,
    RocksConfig, RocksSnapshotCoordinator, RocksStore, RocksWalWriter, SerializationError,
    SnapshotConfig, SnapshotCoordinator, SnapshotError, SnapshotHandle, SnapshotMetadata,
    SnapshotMetadataFile, WalConfig, WalLagStats, HEADER_SIZE,
};
pub use pubsub::{
    ConnId, GlobPattern, IntrospectionRequest, IntrospectionResponse, PubSubMessage, PubSubSender,
    ShardSubscriptions, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
pub use registry::{CommandEntry, CommandRegistry};
pub use replication::{
    serialize_command_to_resp, NoopBroadcaster, ReplicaInfo, ReplicationBroadcaster,
    ReplicationFrame, ReplicationFrameCodec, ReplicationState, ReplicationTrackerImpl,
    SharedBroadcaster, FRAME_MAGIC, FRAME_VERSION,
};
pub use scripting::{
    CachedScript, CrossShardRouter, LuaVm, ScriptCache, ScriptError, ScriptExecutor, ScriptRoute,
    ScriptRouter, ScriptingConfig, SingleShardRouter,
};
pub use shard::{
    extract_hash_tag, shard_for_key, slot_for_key, BigKeyInfo, BigKeysScanResponse,
    HotShardStatsResponse, PartialResult, ScatterOp, ShardBuilderError, ShardClusterDeps,
    ShardConfig, ShardCoreDeps, ShardMemoryStats, ShardMessage, ShardPersistenceDeps,
    ShardWaitQueue, ShardWorker, ShardWorkerBuilder, TransactionResult, WaitEntry,
    WalLagStatsResponse, REDIS_CLUSTER_SLOTS, REPLICA_INTERNAL_CONN_ID,
};
pub use slowlog::{
    SlowLog, SlowLogEntry, DEFAULT_SLOWLOG_LOG_SLOWER_THAN, DEFAULT_SLOWLOG_MAX_ARG_LEN,
    DEFAULT_SLOWLOG_MAX_LEN,
};
pub use store::ValueType;
pub use store::{HashMapStore, Store};
pub use timeseries::{
    Aggregation, CompressedChunk, DownsampleError, DownsampleManager, DownsampleRule,
    DuplicatePolicy, LabelFilter, LabelIndex, NoopDownsampleManager, TimeSeriesValue,
};
pub use types::{
    BlockingOp, Consumer, ConsumerGroup, Direction, Expiry, HashValue, IncrementError, KeyMetadata,
    KeyType, LexBound, ListValue, PendingEntry, ScoreBound, SetCondition, SetOptions, SetResult,
    SetValue, SortedSetValue, StreamAddError, StreamEntry, StreamGroupError, StreamId,
    StreamIdParseError, StreamIdSpec, StreamRangeBound, StreamTrimMode, StreamTrimOptions,
    StreamTrimStrategy, StreamValue, StringValue, Value, ZAddResult,
};
pub use vll::{
    ExecuteSignal, IntentTable, KeyLockState, LockMode, PendingOpState, ShardReadyResult,
    TransactionQueue, VllCommand, VllConfig, VllError, VllPendingOp, VllShardResult,
};
