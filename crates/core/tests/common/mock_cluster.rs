use shuttle::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};

/// Simulated value type for cross-shard testing.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(Vec<u8>),
    #[allow(dead_code)]
    List(Vec<Vec<u8>>),
}

impl Value {
    pub fn string(data: Vec<u8>) -> Self {
        Value::String(data)
    }

    pub fn as_string(&self) -> Option<&Vec<u8>> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
}

/// Simulates multi-shard coordination for scatter-gather operations.
/// Each "shard" is a Mutex<HashMap> representing shard state.
///
/// This models the connection → multi-shard execution flow in FrogDB,
/// where cross-shard MSET/MGET operations use scatter-gather that is
/// NOT atomic across shards.
#[allow(clippy::type_complexity)]
pub struct TestCluster {
    shards: Vec<Arc<Mutex<HashMap<Vec<u8>, Value>>>>,
    num_shards: usize,
}

impl TestCluster {
    pub fn new(num_shards: usize) -> Self {
        Self {
            shards: (0..num_shards)
                .map(|_| Arc::new(Mutex::new(HashMap::new())))
                .collect(),
            num_shards,
        }
    }

    pub fn shard_for_key(&self, key: &[u8]) -> usize {
        // Simplified hash (real impl uses CRC16)
        key.iter().map(|&b| b as usize).sum::<usize>() % self.num_shards
    }

    /// Extract hash tag from key if present (e.g., "{tag}key" -> "tag").
    /// Keys with same hash tag are guaranteed to be on same shard.
    pub fn extract_hash_tag<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        let start = key.iter().position(|&b| b == b'{')?;
        let end = key[start..].iter().position(|&b| b == b'}')?;
        if end > 1 {
            Some(&key[start + 1..start + end])
        } else {
            None
        }
    }

    pub fn shard_for_key_with_tag(&self, key: &[u8]) -> usize {
        if let Some(tag) = self.extract_hash_tag(key) {
            tag.iter().map(|&b| b as usize).sum::<usize>() % self.num_shards
        } else {
            self.shard_for_key(key)
        }
    }

    /// MSET with scatter-gather semantics (non-atomic across shards).
    /// This simulates the real behavior where each shard commits independently.
    pub fn mset(&self, pairs: &[(Vec<u8>, Vec<u8>)]) {
        // Group by shard
        #[allow(clippy::type_complexity)]
        let mut by_shard: HashMap<usize, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();
        for (k, v) in pairs {
            let shard_id = self.shard_for_key_with_tag(k);
            by_shard
                .entry(shard_id)
                .or_default()
                .push((k.clone(), v.clone()));
        }

        // Execute on each shard (simulates parallel scatter)
        // In the real implementation, these run concurrently without coordination
        for (shard_id, shard_pairs) in by_shard {
            let mut shard = self.shards[shard_id].lock().unwrap();
            for (k, v) in shard_pairs {
                shard.insert(k, Value::string(v));
            }
            // Yield between shard operations to simulate non-atomicity
            drop(shard);
            shuttle::thread::yield_now();
        }
    }

    /// MSET atomic version - all keys in single shard lock.
    /// Used to test same-shard atomicity.
    pub fn mset_atomic(&self, pairs: &[(Vec<u8>, Vec<u8>)]) {
        // Verify all keys are on same shard (would be rejected with CROSSSLOT otherwise)
        let shard_ids: HashSet<_> = pairs
            .iter()
            .map(|(k, _)| self.shard_for_key_with_tag(k))
            .collect();
        assert_eq!(
            shard_ids.len(),
            1,
            "Atomic MSET requires all keys on same shard"
        );

        let shard_id = *shard_ids.iter().next().unwrap();
        let mut shard = self.shards[shard_id].lock().unwrap();
        for (k, v) in pairs {
            shard.insert(k.clone(), Value::string(v.clone()));
        }
        // Single lock release - atomic from observer's perspective
    }

    /// MGET with scatter-gather semantics.
    /// Results may reflect different points in time across shards.
    /// Within a single shard, reads are atomic (single lock acquisition).
    pub fn mget(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        // Group keys by shard (preserving order indices)
        let mut by_shard: HashMap<usize, Vec<(usize, &Vec<u8>)>> = HashMap::new();
        for (idx, k) in keys.iter().enumerate() {
            let shard_id = self.shard_for_key_with_tag(k);
            by_shard.entry(shard_id).or_default().push((idx, k));
        }

        let mut results = vec![None; keys.len()];

        // Read from each shard (atomically within shard, but yields between shards)
        for (shard_id, shard_keys) in by_shard {
            let shard = self.shards[shard_id].lock().unwrap();
            for (idx, k) in shard_keys {
                results[idx] = shard.get(k).and_then(|v| v.as_string().cloned());
            }
            drop(shard);
            // Yield between shard reads to simulate cross-shard non-isolation
            shuttle::thread::yield_now();
        }

        results
    }
}
