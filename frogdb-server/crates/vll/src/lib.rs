//! VLL (Very Lightweight Locking) module for atomic multi-shard operations.
//!
//! This module implements VLL for coordinating atomic
//! operations across internal shards. Key features:
//!
//! - Per-key lock table combining declared intents and granted locks
//! - SCA (Selective Contention Analysis) for out-of-order execution
//! - Transaction queue ordered by monotonic txid
//! - Continuation locks for MULTI/EXEC and Lua scripts

mod coordinator;
mod lock_table;
mod queue;
mod shard;
mod traits;
mod types;

pub use coordinator::{
    ContinuationError, ContinuationGuard, DEFAULT_LOCK_ACQUISITION_TIMEOUT, ScatterError,
    ScatterOutcome, ScatterParticipant, ScatterRequest, VllCoordinator,
};
pub use shard::{
    CONTINUATION_DRAIN_TIMEOUT, ContinuationLockSnapshot, DEFAULT_MAX_QUEUE_DEPTH, IntentSnapshot,
    PendingOpSnapshot, QUEUE_DEPTH_WARN_THRESHOLD, VllShardState,
};
pub use traits::{MetricsSink, NoopMetricsSink, ShardSink, ShardSinkError};
pub use types::{LockMode, PendingOpState, ShardReadyResult, VllConfig, VllError};
