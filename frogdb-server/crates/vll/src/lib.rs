//! VLL (Very Lightweight Locking) module for atomic multi-shard operations.
//!
//! This module implements VLL for coordinating atomic
//! operations across internal shards. Key features:
//!
//! - Intent-based locking (read/write intents per key)
//! - SCA (Selective Contention Analysis) for out-of-order execution
//! - Transaction queue ordered by monotonic txid
//! - Continuation locks for MULTI/EXEC and Lua scripts

mod intent_table;
mod lock_state;
mod queue;
mod types;

pub use intent_table::IntentTable;
pub use lock_state::KeyLockState;
pub use queue::{ContinuationLock, TransactionQueue, VllPendingOp};
pub use types::{
    ExecuteSignal, LockMode, PendingOpState, ShardReadyResult, VllCommand, VllConfig, VllError,
    VllShardResult,
};
