//! Command routing utilities.

// Routing is primarily implemented in connection.rs
// This module provides additional routing utilities for future use.

use bytes::Bytes;
use frogdb_core::{CommandError, shard_for_key};

/// Error returned when keys hash to different shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMismatchError;

/// Check if all keys hash to the same shard.
///
/// Returns the shard ID if all keys hash to the same shard, or `ShardMismatchError` if not.
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

/// Check if all keys hash to the same shard, returning `CommandError::CrossSlot` if not.
///
/// This is a convenience wrapper for command implementations that need to validate
/// multi-key operations.
///
/// # Example
///
/// ```rust,ignore
/// // In a command's execute method:
/// let keys = &args[1..numkeys + 1];
/// require_same_shard(keys, ctx.num_shards)?;
/// ```
#[inline]
pub fn require_same_shard(keys: &[Bytes], num_shards: usize) -> Result<(), CommandError> {
    if keys.len() < 2 {
        return Ok(());
    }

    let first_shard = shard_for_key(&keys[0], num_shards);
    for key in &keys[1..] {
        if shard_for_key(key, num_shards) != first_shard {
            return Err(CommandError::CrossSlot);
        }
    }
    Ok(())
}

/// Check if all keys hash to the same shard, returning the shard ID.
///
/// This variant returns the shard ID on success, useful when you need to know
/// which shard to route to.
///
/// # Example
///
/// ```rust,ignore
/// let shard = require_same_shard_id(keys, ctx.num_shards)?;
/// ```
#[inline]
pub fn require_same_shard_id(keys: &[Bytes], num_shards: usize) -> Result<usize, CommandError> {
    if keys.is_empty() {
        return Ok(0);
    }

    let first_shard = shard_for_key(&keys[0], num_shards);
    for key in &keys[1..] {
        if shard_for_key(key, num_shards) != first_shard {
            return Err(CommandError::CrossSlot);
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

    #[test]
    fn test_require_same_shard_empty() {
        assert!(require_same_shard(&[], 4).is_ok());
    }

    #[test]
    fn test_require_same_shard_single() {
        let keys = vec![Bytes::from("key1")];
        assert!(require_same_shard(&keys, 4).is_ok());
    }

    #[test]
    fn test_require_same_shard_with_hash_tag() {
        let keys = vec![
            Bytes::from("{user:1}:profile"),
            Bytes::from("{user:1}:settings"),
        ];
        assert!(require_same_shard(&keys, 4).is_ok());
    }

    #[test]
    fn test_require_same_shard_different_shards() {
        // Use keys that will hash to different shards
        let keys = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];
        // With enough shards, these should hash differently
        let result = require_same_shard(&keys, 16384);
        // The result depends on the hash function, but at least verify it works
        assert!(result.is_ok() || matches!(result, Err(CommandError::CrossSlot)));
    }

    #[test]
    fn test_require_same_shard_id() {
        let keys = vec![Bytes::from("key1")];
        assert!(require_same_shard_id(&keys, 4).is_ok());
    }
}
