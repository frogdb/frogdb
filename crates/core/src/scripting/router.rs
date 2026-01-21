//! Script routing for shard distribution.

use bytes::Bytes;

use super::error::ScriptError;
use crate::shard::shard_for_key;

/// Result of routing a script's keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptRoute {
    /// All keys belong to a single shard.
    SingleShard(usize),
    // Future: CrossShard(Vec<usize>) for VLL-based cross-shard execution
}

/// Trait for routing scripts to shards.
pub trait ScriptRouter: Send + Sync {
    /// Route keys to target shard(s).
    ///
    /// Returns the shard routing decision or an error if keys span multiple shards.
    fn route(&self, keys: &[Bytes], num_shards: usize) -> Result<ScriptRoute, ScriptError>;
}

/// Single-shard router that returns CROSSSLOT error for multi-shard scripts.
#[derive(Debug, Default)]
pub struct SingleShardRouter;

impl SingleShardRouter {
    /// Create a new single-shard router.
    pub fn new() -> Self {
        Self
    }
}

impl ScriptRouter for SingleShardRouter {
    fn route(&self, keys: &[Bytes], num_shards: usize) -> Result<ScriptRoute, ScriptError> {
        if keys.is_empty() {
            // No keys -> route to shard 0 (arbitrary choice, consistent with Redis)
            return Ok(ScriptRoute::SingleShard(0));
        }

        let first_shard = shard_for_key(&keys[0], num_shards);

        for key in &keys[1..] {
            if shard_for_key(key, num_shards) != first_shard {
                return Err(ScriptError::CrossSlot);
            }
        }

        Ok(ScriptRoute::SingleShard(first_shard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_empty_keys() {
        let router = SingleShardRouter::new();
        let result = router.route(&[], 4).unwrap();
        assert_eq!(result, ScriptRoute::SingleShard(0));
    }

    #[test]
    fn test_router_single_key() {
        let router = SingleShardRouter::new();
        let keys = vec![Bytes::from_static(b"mykey")];
        let result = router.route(&keys, 4).unwrap();

        // Should route to some shard (deterministic based on key hash)
        match result {
            ScriptRoute::SingleShard(shard) => assert!(shard < 4),
        }
    }

    #[test]
    fn test_router_same_hash_tag() {
        let router = SingleShardRouter::new();
        let keys = vec![
            Bytes::from_static(b"{user:1}:name"),
            Bytes::from_static(b"{user:1}:email"),
            Bytes::from_static(b"{user:1}:age"),
        ];

        // All keys should route to the same shard due to hash tag
        let result = router.route(&keys, 4).unwrap();
        match result {
            ScriptRoute::SingleShard(shard) => assert!(shard < 4),
        }
    }

    #[test]
    fn test_router_cross_slot_error() {
        let router = SingleShardRouter::new();

        // Keys without hash tags will likely route to different shards
        // We need to find two keys that definitely go to different shards
        let mut key1 = None;
        let mut key2 = None;

        for i in 0u32..100 {
            let key = Bytes::from(format!("key{}", i));
            let shard = shard_for_key(&key, 4);

            match (&key1, &key2) {
                (None, _) => {
                    key1 = Some((key, shard));
                }
                (Some((_, s1)), None) if shard != *s1 => {
                    key2 = Some((key, shard));
                    break;
                }
                _ => {}
            }
        }

        if let (Some((k1, _)), Some((k2, _))) = (key1, key2) {
            let keys = vec![k1, k2];
            let result = router.route(&keys, 4);
            assert!(matches!(result, Err(ScriptError::CrossSlot)));
        }
    }
}
