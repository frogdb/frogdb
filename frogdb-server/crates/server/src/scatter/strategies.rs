//! Scatter-gather strategy implementations.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use frogdb_core::{LockMode, ScatterOp, shard_for_key};
use frogdb_protocol::Response;

use super::{PartitionResult, ScatterGatherStrategy};

/// Helper to partition keys by shard.
fn partition_keys(keys: &[Bytes], num_shards: usize) -> PartitionResult {
    let mut shard_keys: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
    let mut key_order: Vec<(usize, Bytes)> = Vec::new();

    for key in keys {
        let shard_id = shard_for_key(key, num_shards);
        shard_keys.entry(shard_id).or_default().push(key.clone());
        key_order.push((shard_id, key.clone()));
    }

    PartitionResult {
        shard_keys,
        key_order,
        shard_operations: BTreeMap::new(), // Will be filled by caller
    }
}

// =============================================================================
// MGET Strategy - preserves key order
// =============================================================================

/// Strategy for MGET command.
#[derive(Debug, Clone, Default)]
pub struct MGetStrategy;

impl ScatterGatherStrategy for MGetStrategy {
    fn name(&self) -> &'static str {
        "MGET"
    }

    fn lock_mode(&self) -> LockMode {
        LockMode::Read
    }

    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult {
        let mut result = partition_keys(args, num_shards);
        // Same operation for all shards
        for &shard_id in result.shard_keys.keys() {
            result.shard_operations.insert(shard_id, ScatterOp::MGet);
        }
        result
    }

    fn merge(
        &self,
        key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response {
        // Return array in original key order
        let results: Vec<Response> = key_order
            .iter()
            .map(|(shard_id, key)| {
                shard_results
                    .get(shard_id)
                    .and_then(|m| m.get(key))
                    .cloned()
                    .unwrap_or(Response::null())
            })
            .collect();
        Response::Array(results)
    }

    fn scatter_op(&self) -> ScatterOp {
        ScatterOp::MGet
    }
}

// =============================================================================
// MSET Strategy - distributes pairs, returns OK
// =============================================================================

/// Strategy for MSET command.
#[derive(Debug, Clone)]
pub struct MSetStrategy {
    pairs: Vec<(Bytes, Bytes)>,
}

impl MSetStrategy {
    pub fn new(pairs: Vec<(Bytes, Bytes)>) -> Self {
        Self { pairs }
    }
}

impl ScatterGatherStrategy for MSetStrategy {
    fn name(&self) -> &'static str {
        "MSET"
    }

    fn lock_mode(&self) -> LockMode {
        LockMode::Write
    }

    fn partition(&self, _args: &[Bytes], num_shards: usize) -> PartitionResult {
        let mut shard_keys: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
        let mut shard_pairs: BTreeMap<usize, Vec<(Bytes, Bytes)>> = BTreeMap::new();
        let mut key_order: Vec<(usize, Bytes)> = Vec::new();

        for (key, value) in &self.pairs {
            let shard_id = shard_for_key(key, num_shards);
            shard_keys.entry(shard_id).or_default().push(key.clone());
            shard_pairs
                .entry(shard_id)
                .or_default()
                .push((key.clone(), value.clone()));
            key_order.push((shard_id, key.clone()));
        }

        // Build per-shard operations with the pairs for that shard
        let shard_operations: BTreeMap<usize, ScatterOp> = shard_pairs
            .into_iter()
            .map(|(shard_id, pairs)| (shard_id, ScatterOp::MSet { pairs }))
            .collect();

        PartitionResult {
            shard_keys,
            key_order,
            shard_operations,
        }
    }

    fn merge(
        &self,
        _key_order: &[(usize, Bytes)],
        _shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response {
        // MSET always returns OK
        Response::ok()
    }

    fn scatter_op(&self) -> ScatterOp {
        ScatterOp::MSet {
            pairs: self.pairs.clone(),
        }
    }
}

// =============================================================================
// Integer Sum Strategies - DEL, EXISTS, TOUCH, UNLINK, DBSIZE
// =============================================================================

/// Helper to merge results by summing integers.
fn merge_sum_integers(shard_results: &HashMap<usize, HashMap<Bytes, Response>>) -> Response {
    let total: i64 = shard_results
        .values()
        .flat_map(|m| m.values())
        .filter_map(|r| {
            if let Response::Integer(n) = r {
                Some(*n)
            } else {
                None
            }
        })
        .sum();
    Response::Integer(total)
}

/// Strategy for DEL command.
#[derive(Debug, Clone, Default)]
pub struct DelStrategy;

impl ScatterGatherStrategy for DelStrategy {
    fn name(&self) -> &'static str {
        "DEL"
    }

    fn lock_mode(&self) -> LockMode {
        LockMode::Write
    }

    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult {
        let mut result = partition_keys(args, num_shards);
        for &shard_id in result.shard_keys.keys() {
            result.shard_operations.insert(shard_id, ScatterOp::Del);
        }
        result
    }

    fn merge(
        &self,
        _key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response {
        merge_sum_integers(shard_results)
    }

    fn scatter_op(&self) -> ScatterOp {
        ScatterOp::Del
    }
}

/// Strategy for EXISTS command.
#[derive(Debug, Clone, Default)]
pub struct ExistsStrategy;

impl ScatterGatherStrategy for ExistsStrategy {
    fn name(&self) -> &'static str {
        "EXISTS"
    }

    fn lock_mode(&self) -> LockMode {
        LockMode::Read
    }

    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult {
        let mut result = partition_keys(args, num_shards);
        for &shard_id in result.shard_keys.keys() {
            result.shard_operations.insert(shard_id, ScatterOp::Exists);
        }
        result
    }

    fn merge(
        &self,
        _key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response {
        merge_sum_integers(shard_results)
    }

    fn scatter_op(&self) -> ScatterOp {
        ScatterOp::Exists
    }
}

/// Strategy for TOUCH command.
#[derive(Debug, Clone, Default)]
pub struct TouchStrategy;

impl ScatterGatherStrategy for TouchStrategy {
    fn name(&self) -> &'static str {
        "TOUCH"
    }

    fn lock_mode(&self) -> LockMode {
        LockMode::Write
    }

    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult {
        let mut result = partition_keys(args, num_shards);
        for &shard_id in result.shard_keys.keys() {
            result.shard_operations.insert(shard_id, ScatterOp::Touch);
        }
        result
    }

    fn merge(
        &self,
        _key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response {
        merge_sum_integers(shard_results)
    }

    fn scatter_op(&self) -> ScatterOp {
        ScatterOp::Touch
    }
}

/// Strategy for UNLINK command.
#[derive(Debug, Clone, Default)]
pub struct UnlinkStrategy;

impl ScatterGatherStrategy for UnlinkStrategy {
    fn name(&self) -> &'static str {
        "UNLINK"
    }

    fn lock_mode(&self) -> LockMode {
        LockMode::Write
    }

    fn partition(&self, args: &[Bytes], num_shards: usize) -> PartitionResult {
        let mut result = partition_keys(args, num_shards);
        for &shard_id in result.shard_keys.keys() {
            result.shard_operations.insert(shard_id, ScatterOp::Unlink);
        }
        result
    }

    fn merge(
        &self,
        _key_order: &[(usize, Bytes)],
        shard_results: &HashMap<usize, HashMap<Bytes, Response>>,
    ) -> Response {
        merge_sum_integers(shard_results)
    }

    fn scatter_op(&self) -> ScatterOp {
        ScatterOp::Unlink
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mget_partition() {
        let strategy = MGetStrategy;
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let result = strategy.partition(&keys, 4);

        // Should have preserved key order
        assert_eq!(result.key_order.len(), 3);

        // Should have operations for each shard with keys
        for &shard_id in result.shard_keys.keys() {
            assert!(result.shard_operations.contains_key(&shard_id));
        }
    }

    #[test]
    fn test_mget_merge_preserves_order() {
        let strategy = MGetStrategy;

        // Simulate results from 2 shards
        let key_order = vec![
            (0, Bytes::from("a")),
            (1, Bytes::from("b")),
            (0, Bytes::from("c")),
        ];

        let mut shard_results: HashMap<usize, HashMap<Bytes, Response>> = HashMap::new();
        let mut shard0: HashMap<Bytes, Response> = HashMap::new();
        shard0.insert(Bytes::from("a"), Response::bulk(Bytes::from("val_a")));
        shard0.insert(Bytes::from("c"), Response::bulk(Bytes::from("val_c")));
        shard_results.insert(0, shard0);

        let mut shard1: HashMap<Bytes, Response> = HashMap::new();
        shard1.insert(Bytes::from("b"), Response::bulk(Bytes::from("val_b")));
        shard_results.insert(1, shard1);

        let result = strategy.merge(&key_order, &shard_results);

        if let Response::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            // Check order is preserved: a, b, c
            assert!(matches!(&arr[0], Response::Bulk(Some(b)) if b == &Bytes::from("val_a")));
            assert!(matches!(&arr[1], Response::Bulk(Some(b)) if b == &Bytes::from("val_b")));
            assert!(matches!(&arr[2], Response::Bulk(Some(b)) if b == &Bytes::from("val_c")));
        } else {
            panic!("Expected Array response");
        }
    }

    #[test]
    fn test_del_merge_sums() {
        let strategy = DelStrategy;

        let mut shard_results: HashMap<usize, HashMap<Bytes, Response>> = HashMap::new();
        let mut shard0: HashMap<Bytes, Response> = HashMap::new();
        shard0.insert(Bytes::from("__count__"), Response::Integer(2));
        shard_results.insert(0, shard0);

        let mut shard1: HashMap<Bytes, Response> = HashMap::new();
        shard1.insert(Bytes::from("__count__"), Response::Integer(3));
        shard_results.insert(1, shard1);

        let result = strategy.merge(&[], &shard_results);
        assert!(matches!(result, Response::Integer(5)));
    }

    #[test]
    fn test_mset_partition_distributes_pairs() {
        let pairs = vec![
            (Bytes::from("k1"), Bytes::from("v1")),
            (Bytes::from("k2"), Bytes::from("v2")),
        ];
        let strategy = MSetStrategy::new(pairs);

        let result = strategy.partition(&[], 4);

        // Each shard operation should be MSet with only that shard's pairs
        for op in result.shard_operations.values() {
            assert!(matches!(op, ScatterOp::MSet { .. }));
        }
    }
}
