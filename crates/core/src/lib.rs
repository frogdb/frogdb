//! FrogDB Core
//!
//! Core data structures, command traits, and storage implementations for FrogDB.
//! This crate provides the foundational types used by the server.

pub mod command;
pub mod error;
pub mod noop;
pub mod registry;
pub mod shard;
pub mod store;
pub mod sync;
pub mod types;

pub use command::{Arity, Command, CommandContext, CommandFlags};
pub use error::CommandError;
pub use noop::{
    AclChecker, AclResult, AlwaysAllowAcl, ExpiryIndex, MetricsRecorder, NoopMetricsRecorder,
    NoopReplicationTracker, NoopTracer, NoopWalWriter, ReplicationConfig, ReplicationTracker,
    Tracer, WalOperation, WalWriter,
};
pub use registry::CommandRegistry;
pub use shard::{
    shard_for_key, slot_for_key, PartialResult, ScatterOp, ShardMessage, ShardWorker,
    REDIS_CLUSTER_SLOTS,
};
pub use store::{HashMapStore, Store};
pub use types::{
    Expiry, IncrementError, KeyMetadata, KeyType, LexBound, ScoreBound, SetCondition, SetOptions,
    SetResult, SortedSetValue, StringValue, Value, ZAddResult,
};
