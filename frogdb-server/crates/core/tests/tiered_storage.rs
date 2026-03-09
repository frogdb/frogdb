//! Integration tests for two-tier (hot/warm) storage.

use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use frogdb_core::store::{HashMapStore, Store};
use frogdb_core::types::{KeyMetadata, KeyType, Value};
use frogdb_persistence::{RocksConfig, RocksStore};

/// Create a store with warm tier enabled.
fn store_with_warm() -> (HashMapStore, Arc<RocksStore>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(RocksStore::open_with_warm(tmp.path(), 1, &RocksConfig::default(), true).unwrap());
    let mut store = HashMapStore::new();
    store.set_warm_store(rocks.clone(), 0);
    (store, rocks, tmp)
}

#[test]
fn test_demote_and_promote_cycle() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    // SET a key
    store.set(Bytes::from("key1"), Value::string("hello world"));
    assert_eq!(store.len(), 1);
    assert_eq!(store.hot_key_count(), 1);
    assert_eq!(store.warm_key_count(), 0);
    let mem_before = store.memory_used();
    assert!(mem_before > 0);

    // Demote the key
    let bytes_freed = store.demote_key(b"key1").unwrap();
    assert!(bytes_freed > 0);
    assert_eq!(store.len(), 1); // Key still exists (metadata in RAM)
    assert_eq!(store.hot_key_count(), 0);
    assert_eq!(store.warm_key_count(), 1);
    assert_eq!(store.demotion_count(), 1);
    assert!(store.memory_used() < mem_before); // Value bytes freed

    // GET promotes the key back
    let value = store.get(b"key1").unwrap();
    assert_eq!(
        value.as_string().unwrap().as_bytes().as_ref(),
        b"hello world"
    );
    assert_eq!(store.hot_key_count(), 1);
    assert_eq!(store.warm_key_count(), 0);
    assert_eq!(store.promotion_count(), 1);
}

#[test]
fn test_get_hot_does_not_promote() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));
    store.demote_key(b"key1").unwrap();

    // get_hot returns None for warm keys
    assert!(store.get_hot(b"key1").is_none());
    assert_eq!(store.warm_key_count(), 1);
    assert_eq!(store.promotion_count(), 0);
}

#[test]
fn test_set_overwrites_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("old"));
    store.demote_key(b"key1").unwrap();
    assert_eq!(store.warm_key_count(), 1);

    // SET on a warm key replaces it with a hot value
    store.set(Bytes::from("key1"), Value::string("new"));
    assert_eq!(store.hot_key_count(), 1);
    assert_eq!(store.warm_key_count(), 0);

    let value = store.get(b"key1").unwrap();
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"new");
}

#[test]
fn test_delete_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));
    store.demote_key(b"key1").unwrap();
    assert_eq!(store.len(), 1);
    assert_eq!(store.warm_key_count(), 1);

    // DELETE a warm key
    assert!(store.delete(b"key1"));
    assert_eq!(store.len(), 0);
    assert_eq!(store.warm_key_count(), 0);
}

#[test]
fn test_get_and_delete_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("getdel_value"));
    store.demote_key(b"key1").unwrap();

    // GETDEL on warm key reads from RocksDB and deletes
    let value = store.get_and_delete(b"key1").unwrap();
    assert_eq!(
        value.as_string().unwrap().as_bytes().as_ref(),
        b"getdel_value"
    );
    assert_eq!(store.len(), 0);
    assert_eq!(store.warm_key_count(), 0);
}

#[test]
fn test_contains_works_for_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));
    store.demote_key(b"key1").unwrap();

    // EXISTS should work without promotion
    assert!(store.contains(b"key1"));
    assert_eq!(store.promotion_count(), 0); // No promotion happened
}

#[test]
fn test_key_type_works_for_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("str"), Value::string("v"));
    store.set(Bytes::from("list"), Value::list());
    store.demote_key(b"str").unwrap();
    store.demote_key(b"list").unwrap();

    // TYPE should work without promotion
    assert_eq!(store.key_type(b"str"), KeyType::String);
    assert_eq!(store.key_type(b"list"), KeyType::List);
    assert_eq!(store.promotion_count(), 0);
}

#[test]
fn test_get_mut_promotes_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("initial"));
    store.demote_key(b"key1").unwrap();

    // get_mut should promote and return mutable reference
    let value = store.get_mut(b"key1").unwrap();
    // Verify it's a string
    assert!(value.as_string().is_some());
    assert_eq!(store.promotion_count(), 1);
    assert_eq!(store.warm_key_count(), 0);
}

#[test]
fn test_expired_warm_key_cleaned_on_promote() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));

    // Set expiry in the past
    store.set_expiry(b"key1", Instant::now() - Duration::from_secs(1));

    store.demote_key(b"key1").unwrap();
    assert_eq!(store.warm_key_count(), 1);

    // GET should find it expired and clean up
    assert!(store.get(b"key1").is_none());
    assert_eq!(store.warm_key_count(), 0);
    assert_eq!(store.expired_on_promote_count(), 1);
    assert_eq!(store.len(), 0);
}

#[test]
fn test_warm_keys_not_in_eviction_sample() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    // Add some keys
    for i in 0..20 {
        store.set(
            Bytes::from(format!("key{}", i)),
            Value::string(format!("value{}", i)),
        );
    }

    // Demote half
    for i in 0..10 {
        store.demote_key(format!("key{}", i).as_bytes()).unwrap();
    }

    assert_eq!(store.hot_key_count(), 10);
    assert_eq!(store.warm_key_count(), 10);

    // Sample keys should only return hot keys
    let sample = store.sample_keys(20);
    for key in &sample {
        let entry_hot = store.get_hot(key).is_some();
        assert!(entry_hot, "Sampled key {:?} should be hot", key);
    }
}

#[test]
fn test_clear_cleans_warm_tier() {
    let (mut store, rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("v1"));
    store.set(Bytes::from("key2"), Value::string("v2"));
    store.demote_key(b"key1").unwrap();

    // FLUSHDB
    store.clear();
    assert_eq!(store.len(), 0);
    assert_eq!(store.warm_key_count(), 0);
    assert_eq!(store.memory_used(), 0);

    // Verify warm CF is also cleaned
    assert!(rocks.get_warm(0, b"key1").unwrap().is_none());
}

#[test]
fn test_memory_accounting_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(
        Bytes::from("key1"),
        Value::string("a long value that uses memory"),
    );
    let mem_hot = store.memory_used();

    store.demote_key(b"key1").unwrap();
    let mem_warm = store.memory_used();

    // Warm entry should use less memory (no value bytes)
    assert!(
        mem_warm < mem_hot,
        "warm={} should be < hot={}",
        mem_warm,
        mem_hot
    );

    // Promote back
    store.get(b"key1");
    let mem_promoted = store.memory_used();

    // Should be back to roughly same memory
    assert_eq!(mem_promoted, mem_hot);
}

#[test]
fn test_demote_errors() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    // Key not found
    assert!(store.demote_key(b"nonexistent").is_err());

    // Already warm
    store.set(Bytes::from("key1"), Value::string("v"));
    store.demote_key(b"key1").unwrap();
    assert!(store.demote_key(b"key1").is_err());
}

#[test]
fn test_demote_no_warm_store() {
    let mut store = HashMapStore::new(); // No warm store configured

    store.set(Bytes::from("key1"), Value::string("v"));
    assert!(store.demote_key(b"key1").is_err());
}

#[test]
fn test_scan_includes_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("hot1"), Value::string("v"));
    store.set(Bytes::from("warm1"), Value::string("v"));
    store.demote_key(b"warm1").unwrap();

    // SCAN should include both hot and warm keys
    let (_, keys) = store.scan(0, 100, None);
    assert_eq!(keys.len(), 2);
}

#[test]
fn test_recovery_with_warm_entries() {
    use frogdb_core::persistence::recover_all_shards;
    use frogdb_persistence::serialize;

    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(RocksStore::open_with_warm(tmp.path(), 2, &RocksConfig::default(), true).unwrap());

    // Write a hot key to persistence CF
    let hot_value = Value::string("hot_data");
    let hot_meta = KeyMetadata::new(8);
    rocks
        .put(0, b"hot_key", &serialize(&hot_value, &hot_meta))
        .unwrap();

    // Write a warm key to warm CF
    let warm_value = Value::string("warm_data");
    let warm_meta = KeyMetadata::new(9);
    rocks
        .put_warm(0, b"warm_key", &serialize(&warm_value, &warm_meta))
        .unwrap();

    // Write a key that exists in both (hot copy should win)
    let dup_value = Value::string("hot_version");
    let dup_meta = KeyMetadata::new(11);
    rocks
        .put(0, b"dup_key", &serialize(&dup_value, &dup_meta))
        .unwrap();
    let dup_warm = Value::string("warm_version");
    let dup_warm_meta = KeyMetadata::new(12);
    rocks
        .put_warm(0, b"dup_key", &serialize(&dup_warm, &dup_warm_meta))
        .unwrap();

    // Recover
    let (results, stats) = recover_all_shards(&rocks).unwrap();

    let store = &results[0].0;
    assert_eq!(store.len(), 3); // hot_key + warm_key + dup_key
    assert_eq!(stats.keys_loaded, 2); // hot_key + dup_key from persistence
    assert_eq!(stats.warm_keys_loaded, 1); // warm_key from warm CF
    assert_eq!(stats.warm_keys_stale, 1); // dup_key stale in warm CF

    // Verify warm key count
    assert_eq!(store.warm_key_count(), 1); // warm_key only
}
