//! FrogDB Core
//!
//! Core data structures, command traits, and storage implementations for FrogDB.
//! This crate provides the foundational types used by the server.

pub mod acl;
pub mod args;
pub mod cluster;
pub mod bitmap;
pub mod command_macro;
pub mod bloom;
pub mod client_registry;
pub mod command;
pub mod error;
pub mod eviction;
pub mod geo;
pub mod glob;
pub mod hyperloglog;
pub mod json;
pub mod latency;
pub mod noop;
pub mod persistence;
pub mod replication;
pub mod slowlog;
pub mod pubsub;
pub mod registry;
pub mod scripting;
pub mod functions;
pub mod shard;
pub mod store;
pub mod sync;
pub use sync::{LockError, MutexExt, RwLockExt};
pub mod timeseries;
pub mod traits;
pub mod types;
pub mod vll;
pub mod metrics;

pub use command::{
    Arity, ClusterContextRef, Command, CommandContext, CommandContextCore, CommandFlags,
    CommandMetadata, ConnectionLevelOp, ExecutionStrategy, MergeStrategy, QuorumChecker,
    ReplicationContextRef, get_or_create,
};
pub use store::ValueType;
pub use error::{CommandError, FrogDbError, RespError};
pub use args::{
    ArgParser, CompareCondition, ExpiryOption, ScanOptions,
    parse_f64, parse_i64, parse_u64, parse_usize, parse_from_bytes,
};
pub use acl::{
    AclChecker, AclConfig, AclError, AclLog, AclManager, AllowAllChecker, AuthenticatedUser,
    CommandCategory, FullAclChecker, KeyAccessType, PermissionResult, User, UserPermissions,
    generate_password, hash_password,
};
pub use noop::{
    ExpiryIndex, MetricsRecorder, NoopMetricsRecorder,
    NoopReplicationTracker, NoopTracer, NoopWalWriter, ReplicationConfig, ReplicationTracker,
    Tracer, WalOperation, WalWriter,
};
pub use metrics::{
    HotShardDetector, HotShardReport, MemoryDiagnosticsCollector, MemoryReport,
    NoopObservability, ObservabilityConfig,
};
pub use persistence::{
    deserialize, recover_all_shards, recover_shard, serialize, spawn_periodic_sync, CompressionType,
    DurabilityMode, NoopSnapshotCoordinator, OnWriteHook, RecoveryStats, RocksConfig,
    RocksSnapshotCoordinator, RocksStore, RocksWalWriter, SerializationError, SnapshotConfig,
    SnapshotCoordinator, SnapshotError, SnapshotHandle, SnapshotMetadata, SnapshotMetadataFile,
    WalConfig, WalLagStats, HEADER_SIZE,
};
pub use registry::{CommandEntry, CommandRegistry};
pub use pubsub::{
    ConnId, GlobPattern, IntrospectionRequest, IntrospectionResponse, PubSubMessage, PubSubSender,
    ShardSubscriptions, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
pub use shard::{
    extract_hash_tag, shard_for_key, slot_for_key, BigKeyInfo, BigKeysScanResponse,
    HotShardStatsResponse, PartialResult, ScatterOp, ShardBuilderError, ShardClusterDeps,
    ShardConfig, ShardCoreDeps, ShardMemoryStats, ShardMessage, ShardPersistenceDeps, ShardWorker,
    ShardWorkerBuilder, ShardWaitQueue, TransactionResult, WaitEntry, WalLagStatsResponse,
    REDIS_CLUSTER_SLOTS, REPLICA_INTERNAL_CONN_ID,
};
pub use store::{HashMapStore, Store};
pub use types::{
    BlockingOp, Consumer, ConsumerGroup, Direction, Expiry, HashValue, IncrementError,
    KeyMetadata, KeyType, LexBound, ListValue, PendingEntry, ScoreBound, SetCondition,
    SetOptions, SetResult, SetValue, SortedSetValue, StreamAddError, StreamEntry,
    StreamGroupError, StreamId, StreamIdParseError, StreamIdSpec, StreamRangeBound,
    StreamTrimMode, StreamTrimOptions, StreamTrimStrategy, StreamValue, StringValue, Value,
    ZAddResult,
};
pub use scripting::{
    CachedScript, CrossShardRouter, ScriptCache, ScriptError, ScriptExecutor, ScriptingConfig,
    ScriptRoute, ScriptRouter, SingleShardRouter, LuaVm,
};
pub use functions::{
    dump_libraries, load_from_file, load_library, new_shared_registry, parse_shebang,
    restore_libraries, save_to_file, validate_library, CapturedRegistration, FunctionError,
    FunctionFlags, FunctionLibrary, FunctionRegistry, FunctionStats, ParsedLibrary,
    RegisteredFunction, RestorePolicy, RunningFunctionInfo, SharedFunctionRegistry, ShebangInfo,
};
pub use eviction::{
    EvictionConfig, EvictionPolicy, EvictionPool, EvictionCandidate,
    lfu_log_incr, lfu_decay, DEFAULT_MAXMEMORY_SAMPLES, DEFAULT_LFU_LOG_FACTOR,
    DEFAULT_LFU_DECAY_TIME, EVICTION_POOL_SIZE,
};
pub use glob::glob_match;
pub use client_registry::{
    ClientFlags, ClientHandle, ClientInfo, ClientRegistry, ClientStats, ClientStatsDelta,
    CommandTypeStats, KillFilter, PauseMode, UnblockMode,
};
pub use bitmap::{
    BitOp, BitfieldEncoding, BitfieldOffset, BitfieldSubCommand, OverflowMode,
    bitop, bitcount, bitfield_get, bitfield_incrby, bitfield_set, bitpos, getbit, setbit,
};
pub use geo::{
    BoundingBox, Coordinates, DistanceUnit, EARTH_RADIUS_M, GEOHASH_BITS, LAT_MAX, LAT_MIN,
    LON_MAX, LON_MIN, geohash_decode, geohash_encode, geohash_range_for_bbox, geohash_to_score,
    geohash_to_string, haversine_distance, is_within_box, is_within_radius, score_to_geohash,
};
pub use bloom::{BloomFilterValue, BloomLayer};
pub use hyperloglog::{HyperLogLogValue, HLL_DENSE_SIZE, HLL_REGISTERS};
pub use json::{JsonError, JsonLimits, JsonType, JsonValue, DEFAULT_JSON_MAX_DEPTH, DEFAULT_JSON_MAX_SIZE};
pub use slowlog::{
    SlowLog, SlowLogEntry, DEFAULT_SLOWLOG_LOG_SLOWER_THAN, DEFAULT_SLOWLOG_MAX_ARG_LEN,
    DEFAULT_SLOWLOG_MAX_LEN,
};
pub use latency::{
    CommandHistogram, EventHistory, EventStats, LatencyEvent, LatencyMonitor, LatencySample,
    generate_latency_graph, DEFAULT_LATENCY_HISTORY_LEN, DEFAULT_LATENCY_THRESHOLD_MS,
};
pub use timeseries::{
    Aggregation, CompressedChunk, DownsampleError, DownsampleManager, DownsampleRule,
    DuplicatePolicy, LabelFilter, LabelIndex, NoopDownsampleManager, TimeSeriesValue,
};
pub use vll::{
    ExecuteSignal, IntentTable, KeyLockState, LockMode, PendingOpState, ShardReadyResult,
    TransactionQueue, VllCommand, VllConfig, VllError, VllPendingOp, VllShardResult,
};
pub use replication::{
    NoopBroadcaster, ReplicaInfo, ReplicationBroadcaster, ReplicationFrame,
    ReplicationFrameCodec, ReplicationState, ReplicationTrackerImpl, SharedBroadcaster,
    FRAME_MAGIC, FRAME_VERSION, serialize_command_to_resp,
};
pub use cluster::{
    handle_rpc_request, parse_rpc_message, send_rpc_response, ClusterCommand, ClusterConfig,
    ClusterError, ClusterNetwork, ClusterNetworkFactory, ClusterRaft, ClusterResponse,
    ClusterRpcRequest, ClusterRpcResponse, ClusterSnapshot, ClusterState, ClusterStateMachine,
    ClusterStorage, ConfigEpoch, NodeId, NodeInfo, NodeRole, SharedClusterRaft, SlotRange,
    TypeConfig, CLUSTER_SLOTS,
};
