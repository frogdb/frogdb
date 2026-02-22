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
    AclChecker, AclConfig, AclError, AclLog, AclManager, AllowAllChecker, AuthenticatedUser,
    CommandCategory, FullAclChecker, KeyAccessType, PermissionResult, User, UserPermissions,
    generate_password, hash_password,
};
pub use args::{
    ArgParser, CompareCondition, ExpiryOption, ScanOptions, parse_f64, parse_from_bytes, parse_i64,
    parse_u64, parse_usize,
};
pub use bitmap::{
    BitOp, BitfieldEncoding, BitfieldOffset, BitfieldSubCommand, OverflowMode, bitcount,
    bitfield_get, bitfield_incrby, bitfield_set, bitop, bitpos, getbit, setbit,
};
pub use bloom::{BloomFilterValue, BloomLayer};
pub use client_registry::{
    ClientFlags, ClientHandle, ClientInfo, ClientRegistry, ClientStats, ClientStatsDelta,
    CommandTypeStats, KillFilter, PauseMode, UnblockMode,
};
pub use cluster::{
    CLUSTER_SLOTS, ClusterCommand, ClusterConfig, ClusterError, ClusterNetwork,
    ClusterNetworkFactory, ClusterRaft, ClusterResponse, ClusterRpcRequest, ClusterRpcResponse,
    ClusterSnapshot, ClusterState, ClusterStateMachine, ClusterStorage, ConfigEpoch, NodeId,
    NodeInfo, NodeRole, SharedClusterRaft, SlotRange, TypeConfig, handle_rpc_request,
    parse_rpc_message, send_rpc_response,
};
pub use command::{
    Arity, ClusterContextRef, Command, CommandContext, CommandContextCore, CommandFlags,
    CommandMetadata, ConnectionLevelOp, ExecutionStrategy, MergeStrategy, QuorumChecker,
    ReplicationContextRef, ServerWideOp, get_or_create,
};
pub use error::{CommandError, FrogDbError, RespError};
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
pub use geo::{
    BoundingBox, Coordinates, DistanceUnit, EARTH_RADIUS_M, GEOHASH_BITS, LAT_MAX, LAT_MIN,
    LON_MAX, LON_MIN, geohash_decode, geohash_encode, geohash_range_for_bbox, geohash_to_score,
    geohash_to_string, haversine_distance, is_within_box, is_within_radius, score_to_geohash,
};
pub use glob::glob_match;
pub use hyperloglog::{HLL_DENSE_SIZE, HLL_REGISTERS, HyperLogLogValue};
pub use json::{
    DEFAULT_JSON_MAX_DEPTH, DEFAULT_JSON_MAX_SIZE, JsonError, JsonLimits, JsonType, JsonValue,
};
pub use latency::{
    CommandHistogram, DEFAULT_LATENCY_HISTORY_LEN, DEFAULT_LATENCY_THRESHOLD_MS, EventHistory,
    EventStats, LatencyEvent, LatencyMonitor, LatencySample, generate_latency_graph,
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
    FRAME_MAGIC, FRAME_VERSION, NoopBroadcaster, ReplicaInfo, ReplicationBroadcaster,
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
