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

mod blocking;
mod builder;
mod connection;
mod counters;
mod diagnostics;
mod event_loop;
mod eviction;
mod execution;
mod functions;
mod helpers;
pub mod message;
mod persistence;
mod pubsub;
mod scripting;
pub mod types;
mod vll;
mod wait_queue;
mod worker;

#[cfg(test)]
mod tests;

pub use builder::{ShardBuilderError, ShardWorkerBuilder};
pub use connection::NewConnection;
pub use counters::{HotShardStatsResponse, OperationBucket, OperationCounters};
pub use helpers::{
    extract_hash_tag, shard_for_key, slot_for_key, REDIS_CLUSTER_SLOTS, REPLICA_INTERNAL_CONN_ID,
};
pub use message::{ScatterOp, ShardMessage};
pub use types::{
    BigKeyInfo, BigKeysScanResponse, PartialResult, PendingOp, ShardClusterDeps, ShardConfig,
    ShardCoreDeps, ShardMemoryStats, ShardPersistenceDeps, TransactionQueue, TransactionResult,
    VllContinuationLockInfo, VllKeyIntentInfo, VllPendingOpInfo, VllQueueInfo, WalLagStatsResponse,
};
pub use wait_queue::{ShardWaitQueue, WaitEntry};
pub use worker::ShardWorker;
