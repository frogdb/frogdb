//! Recovery from persistent storage on startup.
//!
//! Loads key-value pairs from RocksDB and populates the in-memory store.
//! Expired keys are filtered out during recovery.

use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

use super::rocks::RocksStore;
use super::serialization::{SerializationError, deserialize};
use crate::noop::ExpiryIndex;
use crate::store::{HashMapStore, Store};

/// Statistics from recovery.
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Number of keys successfully loaded.
    pub keys_loaded: u64,

    /// Number of expired keys skipped.
    pub keys_expired_skipped: u64,

    /// Total bytes loaded (serialized size).
    pub bytes_loaded: u64,

    /// Duration of recovery in milliseconds.
    pub duration_ms: u64,

    /// Number of keys that failed to deserialize.
    pub keys_failed: u64,

    /// Number of warm keys recovered from tiered storage.
    pub warm_keys_loaded: u64,

    /// Number of stale warm keys skipped (hot copy exists).
    pub warm_keys_stale: u64,
}

/// Error during recovery.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] super::rocks::RocksError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),
}

/// Recover a shard's data from RocksDB.
///
/// Returns a populated store, expiry index, and recovery statistics.
pub fn recover_shard(
    rocks: &RocksStore,
    shard_id: usize,
) -> Result<(HashMapStore, ExpiryIndex, RecoveryStats), RecoveryError> {
    let start = Instant::now();
    let now = Instant::now();

    let mut store = HashMapStore::new();
    let mut expiry_index = ExpiryIndex::new();
    let mut stats = RecoveryStats::default();

    // Iterate over all key-value pairs in the shard
    for (key, value) in rocks.iter_cf(shard_id)? {
        stats.bytes_loaded += value.len() as u64;

        match deserialize(&value) {
            Ok((val, metadata)) => {
                // Check if key is expired
                if let Some(expires_at) = metadata.expires_at
                    && expires_at <= now
                {
                    stats.keys_expired_skipped += 1;
                    continue;
                }

                let key_bytes = Bytes::copy_from_slice(&key);

                // Add to expiry index if key has expiry
                if let Some(expires_at) = metadata.expires_at {
                    expiry_index.set(key_bytes.clone(), expires_at);
                }

                // Insert into store (bypassing normal path to set metadata directly)
                store.restore_entry(key_bytes, val, metadata);

                stats.keys_loaded += 1;
            }
            Err(e) => {
                tracing::warn!(
                    key = ?String::from_utf8_lossy(&key),
                    error = %e,
                    "Failed to deserialize key during recovery"
                );
                stats.keys_failed += 1;
            }
        }
    }

    stats.duration_ms = start.elapsed().as_millis() as u64;

    tracing::info!(
        shard_id = shard_id,
        keys_loaded = stats.keys_loaded,
        keys_expired = stats.keys_expired_skipped,
        keys_failed = stats.keys_failed,
        bytes = stats.bytes_loaded,
        duration_ms = stats.duration_ms,
        "Shard recovery complete"
    );

    Ok((store, expiry_index, stats))
}

/// Recover warm entries for a shard.
///
/// Scans the warm CF and inserts entries as `Warm` into the store.
/// Keys that already exist in the store (from hot recovery) are skipped.
fn recover_warm_shard(
    rocks: &RocksStore,
    shard_id: usize,
    store: &mut HashMapStore,
    stats: &mut RecoveryStats,
) -> Result<(), RecoveryError> {
    let now = Instant::now();

    for (key, value) in rocks.iter_warm_cf(shard_id)? {
        match deserialize(&value) {
            Ok((val, metadata)) => {
                // Skip expired keys
                if let Some(expires_at) = metadata.expires_at
                    && expires_at <= now
                {
                    stats.keys_expired_skipped += 1;
                    // Clean up stale warm entry
                    let _ = rocks.delete_warm(shard_id, &key);
                    continue;
                }

                let key_bytes = Bytes::copy_from_slice(&key);

                // Hot copy wins — restore_warm_entry checks for duplicates
                if store.contains(&key_bytes) {
                    stats.warm_keys_stale += 1;
                    // Delete stale warm copy
                    let _ = rocks.delete_warm(shard_id, &key);
                    continue;
                }

                let key_type = val.key_type();
                store.restore_warm_entry(key_bytes, metadata, key_type);
                stats.warm_keys_loaded += 1;
            }
            Err(e) => {
                tracing::warn!(
                    shard_id,
                    key = ?String::from_utf8_lossy(&key),
                    error = %e,
                    "Failed to deserialize warm key during recovery"
                );
                stats.keys_failed += 1;
            }
        }
    }

    Ok(())
}

/// Recover all shards from RocksDB.
///
/// Returns a vector of (store, expiry_index) pairs, one per shard.
pub fn recover_all_shards(
    rocks: &Arc<RocksStore>,
) -> Result<(Vec<(HashMapStore, ExpiryIndex)>, RecoveryStats), RecoveryError> {
    let start = Instant::now();
    let mut total_stats = RecoveryStats::default();
    let mut results = Vec::with_capacity(rocks.num_shards());

    for shard_id in 0..rocks.num_shards() {
        let (mut store, expiry_index, stats) = recover_shard(rocks, shard_id)?;

        total_stats.keys_loaded += stats.keys_loaded;
        total_stats.keys_expired_skipped += stats.keys_expired_skipped;
        total_stats.bytes_loaded += stats.bytes_loaded;
        total_stats.keys_failed += stats.keys_failed;

        // Recover warm entries if warm tier is enabled
        if rocks.warm_enabled() {
            recover_warm_shard(rocks, shard_id, &mut store, &mut total_stats)?;
        }

        results.push((store, expiry_index));
    }

    total_stats.duration_ms = start.elapsed().as_millis() as u64;

    tracing::info!(
        num_shards = rocks.num_shards(),
        total_keys = total_stats.keys_loaded,
        total_expired = total_stats.keys_expired_skipped,
        total_failed = total_stats.keys_failed,
        total_bytes = total_stats.bytes_loaded,
        warm_keys = total_stats.warm_keys_loaded,
        warm_stale = total_stats.warm_keys_stale,
        total_duration_ms = total_stats.duration_ms,
        "Full recovery complete"
    );

    Ok((results, total_stats))
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::persistence::serialization::serialize;
    use crate::store::Store;
    use crate::types::{KeyMetadata, SortedSetValue, Value};
    use std::time::Duration;
    use tempfile::TempDir;

    use super::super::rocks::RocksConfig;

    #[test]
    fn test_recover_empty_shard() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        let (mut store, expiry_index, stats) = recover_shard(&rocks, 0).unwrap();

        assert_eq!(store.len(), 0);
        assert!(expiry_index.is_empty());
        assert_eq!(stats.keys_loaded, 0);
    }

    #[test]
    fn test_recover_with_data() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        // Write some test data
        let value1 = Value::string("hello");
        let metadata1 = KeyMetadata::new(5);
        rocks
            .put(0, b"key1", &serialize(&value1, &metadata1))
            .unwrap();

        let value2 = Value::string("world");
        let metadata2 = KeyMetadata::new(5);
        rocks
            .put(0, b"key2", &serialize(&value2, &metadata2))
            .unwrap();

        // Recover
        let (mut store, expiry_index, stats) = recover_shard(&rocks, 0).unwrap();

        assert_eq!(store.len(), 2);
        assert!(expiry_index.is_empty()); // No expiry set
        assert_eq!(stats.keys_loaded, 2);
        assert_eq!(stats.keys_expired_skipped, 0);

        // Verify values
        let v1 = store.get(b"key1").unwrap();
        assert_eq!(v1.as_string().unwrap().as_bytes().as_ref(), b"hello");

        let v2 = store.get(b"key2").unwrap();
        assert_eq!(v2.as_string().unwrap().as_bytes().as_ref(), b"world");
    }

    #[test]
    fn test_recover_with_expiry() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        // Write key with future expiry
        let value = Value::string("expires_later");
        let mut metadata = KeyMetadata::new(13);
        metadata.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        rocks
            .put(0, b"future_key", &serialize(&value, &metadata))
            .unwrap();

        // Recover
        let (mut store, expiry_index, stats) = recover_shard(&rocks, 0).unwrap();

        assert_eq!(store.len(), 1);
        assert_eq!(expiry_index.len(), 1);
        assert_eq!(stats.keys_loaded, 1);
        assert!(expiry_index.get(b"future_key").is_some());
    }

    #[test]
    fn test_recover_skips_expired() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        // Write key with past expiry
        let value = Value::string("expired");
        let mut metadata = KeyMetadata::new(7);
        // Set expiry in the past
        metadata.expires_at = Some(Instant::now() - Duration::from_secs(1));
        rocks
            .put(0, b"expired_key", &serialize(&value, &metadata))
            .unwrap();

        // Write key without expiry
        let value2 = Value::string("valid");
        let metadata2 = KeyMetadata::new(5);
        rocks
            .put(0, b"valid_key", &serialize(&value2, &metadata2))
            .unwrap();

        // Recover
        let (mut store, expiry_index, stats) = recover_shard(&rocks, 0).unwrap();

        assert_eq!(store.len(), 1);
        assert!(expiry_index.is_empty());
        assert_eq!(stats.keys_loaded, 1);
        assert_eq!(stats.keys_expired_skipped, 1);

        // Only valid_key should be present
        assert!(store.get(b"valid_key").is_some());
        assert!(store.get(b"expired_key").is_none());
    }

    #[test]
    fn test_recover_sorted_set() {
        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        // Write a sorted set
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("member1"), 1.0);
        zset.add(Bytes::from("member2"), 2.0);

        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(100);
        rocks
            .put(0, b"myzset", &serialize(&value, &metadata))
            .unwrap();

        // Recover
        let (mut store, _, stats) = recover_shard(&rocks, 0).unwrap();

        assert_eq!(store.len(), 1);
        assert_eq!(stats.keys_loaded, 1);

        let recovered = store.get(b"myzset").unwrap();
        let zset = recovered.as_sorted_set().unwrap();
        assert_eq!(zset.len(), 2);
        assert_eq!(zset.get_score(b"member1"), Some(1.0));
        assert_eq!(zset.get_score(b"member2"), Some(2.0));
    }

    #[test]
    fn test_recover_all_shards() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 3, &RocksConfig::default()).unwrap());

        // Write data to different shards
        let value = Value::string("v");
        let metadata = KeyMetadata::new(1);

        rocks
            .put(0, b"shard0", &serialize(&value, &metadata))
            .unwrap();
        rocks
            .put(1, b"shard1a", &serialize(&value, &metadata))
            .unwrap();
        rocks
            .put(1, b"shard1b", &serialize(&value, &metadata))
            .unwrap();
        rocks
            .put(2, b"shard2", &serialize(&value, &metadata))
            .unwrap();

        // Recover all
        let (results, stats) = recover_all_shards(&rocks).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(stats.keys_loaded, 4);

        assert_eq!(results[0].0.len(), 1);
        assert_eq!(results[1].0.len(), 2);
        assert_eq!(results[2].0.len(), 1);
    }
}
