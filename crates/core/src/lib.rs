//! FrogDB Core
//!
//! Core data structures, command traits, and storage implementations for FrogDB.
//! This crate provides the foundational types used by the server.

pub mod acl;
pub mod bitmap;
pub mod bloom;
pub mod client_registry;
pub mod command;
pub mod error;
pub mod eviction;
pub mod geo;
pub mod glob;
pub mod noop;
pub mod persistence;
pub mod pubsub;
pub mod registry;
pub mod scripting;
pub mod shard;
pub mod store;
pub mod sync;
pub mod types;

pub use command::{Arity, Command, CommandContext, CommandFlags};
pub use error::CommandError;
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
pub use persistence::{
    deserialize, recover_all_shards, recover_shard, serialize, spawn_periodic_sync, CompressionType,
    DurabilityMode, NoopSnapshotCoordinator, OnWriteHook, RecoveryStats, RocksConfig, RocksStore,
    RocksWalWriter, SerializationError, SnapshotCoordinator, SnapshotHandle, SnapshotMetadata,
    WalConfig, HEADER_SIZE,
};
pub use registry::CommandRegistry;
pub use pubsub::{
    ConnId, GlobPattern, IntrospectionRequest, IntrospectionResponse, PubSubMessage, PubSubSender,
    ShardSubscriptions, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
pub use shard::{
    shard_for_key, slot_for_key, PartialResult, ScatterOp, ShardMessage, ShardWorker,
    ShardWaitQueue, TransactionResult, WaitEntry, REDIS_CLUSTER_SLOTS,
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
    CachedScript, ScriptCache, ScriptError, ScriptExecutor, ScriptingConfig, ScriptRoute,
    ScriptRouter, SingleShardRouter, LuaVm,
};
pub use eviction::{
    EvictionConfig, EvictionPolicy, EvictionPool, EvictionCandidate,
    lfu_log_incr, lfu_decay, DEFAULT_MAXMEMORY_SAMPLES, DEFAULT_LFU_LOG_FACTOR,
    DEFAULT_LFU_DECAY_TIME, EVICTION_POOL_SIZE,
};
pub use glob::glob_match;
pub use client_registry::{
    ClientFlags, ClientHandle, ClientInfo, ClientRegistry, KillFilter, PauseMode,
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
