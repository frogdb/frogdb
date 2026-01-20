//! Command routing utilities.

// Routing is primarily implemented in connection.rs
// This module provides additional routing utilities for future use.

use frogdb_core::shard_for_key;

/// Error returned when keys hash to different shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMismatchError;

/// Check if all keys hash to the same shard.
pub fn validate_same_shard(keys: &[&[u8]], num_shards: usize) -> Result<usize, ShardMismatchError> {
    if keys.is_empty() {
        return Ok(0); // No keys, any shard is fine
    }

    let first_shard = shard_for_key(keys[0], num_shards);

    for key in &keys[1..] {
        if shard_for_key(key, num_shards) != first_shard {
            return Err(ShardMismatchError);
        }
    }

    Ok(first_shard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_same_shard_empty() {
        assert_eq!(validate_same_shard(&[], 4), Ok(0));
    }

    #[test]
    fn test_validate_same_shard_single() {
        assert!(validate_same_shard(&[b"key1"], 4).is_ok());
    }

    #[test]
    fn test_validate_same_shard_with_hash_tag() {
        let keys: Vec<&[u8]> = vec![b"{user:1}:profile", b"{user:1}:settings"];
        assert!(validate_same_shard(&keys, 4).is_ok());
    }
}
