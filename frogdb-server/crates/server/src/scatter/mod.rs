//! Scatter-Gather abstraction for multi-shard operations.
//!
//! This module provides a trait-based abstraction for scatter-gather operations,
//! replacing the large match statements with composable strategy implementations.
//!
//! # Architecture
//!
//! - `ScatterGatherStrategy`: Trait defining how to partition keys across shards and merge results
//! - `ScatterGatherExecutor`: Coordinates VLL locking and execution across shards
//! - Strategy implementations: MGet, MSet, Del, Exists, Touch, Unlink, Keys, DbSize, FlushDb

mod executor;
mod strategies;

pub use executor::ScatterGatherExecutor;
pub use strategies::{
    DbSizeStrategy, DelStrategy, ExistsStrategy, FlushDbStrategy, KeysStrategy, MGetStrategy,
    MSetStrategy, TouchStrategy, UnlinkStrategy,
};

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use frogdb_core::{LockMode, ScatterOp};
use frogdb_protocol::Response;

/// Result of partitioning keys across shards.
#[derive(Debug, Clone)]
pub struct PartitionResult {
    /// Keys grouped by shard ID, sorted for deterministic ordering.
    pub shard_keys: BTreeMap<usize, Vec<Bytes>>,
    /// Original key order: (shard_id, key) for result reconstruction.
    pub key_order: Vec<(usize, Bytes)>,
    /// Per-shard operations (may differ from base operation, e.g., MSET with distributed pairs).
    pub shard_operations: BTreeMap<usize, ScatterOp>,
}

/// Strategy for scatter-gather operations.
///
/// Each strategy defines:
/// - How to partition keys across shards
/// - What lock mode to use (read/write)
/// - How to merge results from multiple shards
pub trait ScatterGatherStrategy: Send + Sync {
    /// Returns the command name for metrics.
    fn name(&self) -> &'static str;

    /// Returns the lock mode required for this operation.
    fn lock_mode(&self) -> LockMode;

    /// Partition the operation across shards.
    ///
    /// # Arguments
    /// * `args` - Command arguments (keys and values)
    /// * `num_shards` - Total number of shards
    ///
    /// # Returns
    /// Partition result with keys grouped by shard and original ordering preserved.
    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult;

    /// Merge results from all shards into a single response.
    ///
    /// # Arguments
    /// * `key_order` - Original (shard_id, key) ordering for result reconstruction
    /// * `shard_results` - Results from each shard: shard_id -> (key -> response)
    fn merge(
        &self,
        key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response;

    /// Returns the ScatterOp for this strategy (used for VLL requests).
    fn scatter_op(&self) -> ScatterOp;
}

/// Helper to get a strategy from a ScatterOp.
pub fn strategy_for_op(op: &ScatterOp) -> Option<Box<dyn ScatterGatherStrategy>> {
    match op {
        ScatterOp::MGet => Some(Box::new(MGetStrategy)),
        ScatterOp::MSet { pairs } => Some(Box::new(MSetStrategy::new(pairs.clone()))),
        ScatterOp::Del => Some(Box::new(DelStrategy)),
        ScatterOp::Exists => Some(Box::new(ExistsStrategy)),
        ScatterOp::Touch => Some(Box::new(TouchStrategy)),
        ScatterOp::Unlink => Some(Box::new(UnlinkStrategy)),
        ScatterOp::Keys { pattern } => Some(Box::new(KeysStrategy::new(pattern.clone()))),
        ScatterOp::DbSize => Some(Box::new(DbSizeStrategy)),
        ScatterOp::FlushDb => Some(Box::new(FlushDbStrategy)),
        // These are handled specially elsewhere
        ScatterOp::Scan { .. }
        | ScatterOp::Copy { .. }
        | ScatterOp::CopySet { .. }
        | ScatterOp::RandomKey
        | ScatterOp::Dump
        | ScatterOp::TsQueryIndex { .. }
        | ScatterOp::TsMget { .. }
        | ScatterOp::TsMrange { .. }
        | ScatterOp::FtCreate { .. }
        | ScatterOp::FtSearch { .. }
        | ScatterOp::FtDropIndex { .. }
        | ScatterOp::FtInfo { .. }
        | ScatterOp::FtList
        | ScatterOp::FtAlter { .. }
        | ScatterOp::FtSynupdate { .. }
        | ScatterOp::FtSyndump { .. }
        | ScatterOp::FtAggregate { .. } => None,
    }
}
