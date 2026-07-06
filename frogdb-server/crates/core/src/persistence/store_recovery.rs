//! Recovery from persistent storage on startup.
//!
//! The *format's* recovery protocol — RocksDB column-family iteration,
//! deserialization, expiry filtering, and warm-tier precedence — lives in the
//! `frogdb-persistence` crate ([`frogdb_persistence::recovery`]). This module
//! is the thin store-side adapter: it provides the [`RestoreSink`] that
//! persistence drives, deciding *how* a recovered entry lands in a
//! [`HashMapStore`] (and its expiry index), and re-assembles the per-shard
//! results the server expects.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use frogdb_persistence::{
    RecoveryError, RecoveryStats, RestoreSink, recover_shard_into, recover_warm_shard_into,
};
use frogdb_types::types::{KeyMetadata, KeyType, Value};

use super::rocks::RocksStore;
use crate::noop::ExpiryIndex;
use crate::store::{HashMapStore, Store};

/// Store-side [`RestoreSink`]: lands recovered entries into a [`HashMapStore`]
/// and mirrors their expiries into a standalone [`ExpiryIndex`].
///
/// Persistence owns the recovery *sequencing*; this adapter owns the single
/// concern of *how an entry is applied* to core's store.
#[derive(Default)]
struct StoreRestoreSink {
    store: HashMapStore,
    expiry_index: ExpiryIndex,
}

impl StoreRestoreSink {
    fn into_parts(self) -> (HashMapStore, ExpiryIndex) {
        (self.store, self.expiry_index)
    }
}

impl RestoreSink for StoreRestoreSink {
    fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
        // Mirror the expiry into the standalone index the server hands to the
        // shard worker; the store's own internal index is updated by
        // `restore_entry`.
        if let Some(expires_at) = metadata.expires_at {
            self.expiry_index.set(key.clone(), expires_at);
        }
        self.store.restore_entry(key, value, metadata);
    }

    fn restore_warm_entry(&mut self, key: Bytes, metadata: KeyMetadata, key_type: KeyType) {
        self.store.restore_warm_entry(key, metadata, key_type);
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.store.contains(key)
    }
}

/// Recover a shard's data from RocksDB.
///
/// Returns a populated store, expiry index, and recovery statistics. The format
/// protocol is driven by [`frogdb_persistence::recover_shard_into`]; this
/// function only supplies the sink and unpacks the result.
pub fn recover_shard(
    rocks: &RocksStore,
    shard_id: usize,
) -> Result<(HashMapStore, ExpiryIndex, RecoveryStats), RecoveryError> {
    let mut sink = StoreRestoreSink::default();
    let stats = recover_shard_into(rocks, shard_id, &mut sink)?;
    let (store, expiry_index) = sink.into_parts();
    Ok((store, expiry_index, stats))
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
        // Keep one sink alive across hot + warm recovery so warm-tier
        // precedence (`contains`) sees the hot keys restored this pass.
        let mut sink = StoreRestoreSink::default();
        let stats = recover_shard_into(rocks, shard_id, &mut sink)?;

        total_stats.keys_loaded += stats.keys_loaded;
        total_stats.keys_expired_skipped += stats.keys_expired_skipped;
        total_stats.bytes_loaded += stats.bytes_loaded;
        total_stats.keys_failed += stats.keys_failed;

        // Recover warm entries if warm tier is enabled
        if rocks.warm_enabled() {
            recover_warm_shard_into(rocks, shard_id, &mut sink, &mut total_stats)?;
        }

        results.push(sink.into_parts());
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

        let (store, expiry_index, stats) = recover_shard(&rocks, 0).unwrap();

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
        let (store, expiry_index, stats) = recover_shard(&rocks, 0).unwrap();

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
