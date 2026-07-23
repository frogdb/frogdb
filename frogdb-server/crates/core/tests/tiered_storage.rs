//! Integration tests for two-tier (hot/warm) storage.

use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use frogdb_core::store::{HashMapStore, Store};
use frogdb_core::types::{KeyMetadata, KeyType, SortedSetValue, Value};
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
fn test_spill_and_unspill_cycle() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    // SET a key
    store.set(Bytes::from("key1"), Value::string("hello world"));
    assert_eq!(store.len(), 1);
    assert_eq!((store.len() - store.warm_tier().warm_keys()), 1);
    assert_eq!(store.warm_tier().warm_keys(), 0);
    let mem_before = store.memory_used();
    assert!(mem_before > 0);

    // Spill the key
    let bytes_freed = store.spill_key(b"key1").unwrap();
    assert!(bytes_freed > 0);
    assert_eq!(store.len(), 1); // Key still exists (metadata in RAM)
    assert_eq!((store.len() - store.warm_tier().warm_keys()), 0);
    assert_eq!(store.warm_tier().warm_keys(), 1);
    assert_eq!(store.warm_tier().spills(), 1);
    assert!(store.memory_used() < mem_before); // Value bytes freed

    // GET unspills the key back
    let value = store.get(b"key1").unwrap();
    assert_eq!(
        value.as_string().unwrap().as_bytes().as_ref(),
        b"hello world"
    );
    assert_eq!((store.len() - store.warm_tier().warm_keys()), 1);
    assert_eq!(store.warm_tier().warm_keys(), 0);
    assert_eq!(store.warm_tier().unspills(), 1);
}

#[test]
fn test_get_hot_does_not_unspill() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));
    store.spill_key(b"key1").unwrap();

    // get_hot returns None for warm keys
    assert!(store.get_hot(b"key1").is_none());
    assert_eq!(store.warm_tier().warm_keys(), 1);
    assert_eq!(store.warm_tier().unspills(), 0);
}

#[test]
fn test_set_overwrites_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("old"));
    store.spill_key(b"key1").unwrap();
    assert_eq!(store.warm_tier().warm_keys(), 1);

    // SET on a warm key replaces it with a hot value
    store.set(Bytes::from("key1"), Value::string("new"));
    assert_eq!((store.len() - store.warm_tier().warm_keys()), 1);
    assert_eq!(store.warm_tier().warm_keys(), 0);

    let value = store.get(b"key1").unwrap();
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"new");
}

#[test]
fn test_delete_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));
    store.spill_key(b"key1").unwrap();
    assert_eq!(store.len(), 1);
    assert_eq!(store.warm_tier().warm_keys(), 1);

    // DELETE a warm key
    assert!(store.delete(b"key1"));
    assert_eq!(store.len(), 0);
    assert_eq!(store.warm_tier().warm_keys(), 0);
}

#[test]
fn test_get_and_delete_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("getdel_value"));
    store.spill_key(b"key1").unwrap();

    // GETDEL on warm key reads from RocksDB and deletes
    let value = store.get_and_delete(b"key1").unwrap();
    assert_eq!(
        value.as_string().unwrap().as_bytes().as_ref(),
        b"getdel_value"
    );
    assert_eq!(store.len(), 0);
    assert_eq!(store.warm_tier().warm_keys(), 0);
}

#[test]
fn test_contains_works_for_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));
    store.spill_key(b"key1").unwrap();

    // EXISTS should work without unspilling
    assert!(store.contains(b"key1"));
    assert_eq!(store.warm_tier().unspills(), 0); // No unspill happened
}

#[test]
fn test_key_type_works_for_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("str"), Value::string("v"));
    store.set(Bytes::from("list"), Value::list());
    store.spill_key(b"str").unwrap();
    store.spill_key(b"list").unwrap();

    // TYPE should work without unspilling
    assert_eq!(store.key_type(b"str"), KeyType::String);
    assert_eq!(store.key_type(b"list"), KeyType::List);
    assert_eq!(store.warm_tier().unspills(), 0);
}

#[test]
fn test_get_mut_unspills_warm_key() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("initial"));
    store.spill_key(b"key1").unwrap();

    // get_mut should unspill and return mutable reference
    let value = store.get_mut(b"key1").unwrap();
    // Verify it's a string
    assert!(value.as_string().is_some());
    assert_eq!(store.warm_tier().unspills(), 1);
    assert_eq!(store.warm_tier().warm_keys(), 0);
}

#[test]
fn test_expired_warm_key_cleaned_on_unspill() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("key1"), Value::string("value"));

    // Set expiry in the past
    store.set_expiry(b"key1", Instant::now() - Duration::from_secs(1));

    store.spill_key(b"key1").unwrap();
    assert_eq!(store.warm_tier().warm_keys(), 1);

    // GET should find it expired and clean up
    assert!(store.get(b"key1").is_none());
    assert_eq!(store.warm_tier().warm_keys(), 0);
    assert_eq!(store.warm_tier().expired_on_unspill(), 1);
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

    // Spill half
    for i in 0..10 {
        store.spill_key(format!("key{}", i).as_bytes()).unwrap();
    }

    assert_eq!((store.len() - store.warm_tier().warm_keys()), 10);
    assert_eq!(store.warm_tier().warm_keys(), 10);

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

    // Two spilled keys land in the warm CF; a third stays hot.
    store.set(Bytes::from("key1"), Value::string("v1"));
    store.set(Bytes::from("key2"), Value::string("v2"));
    store.set(Bytes::from("key3"), Value::string("v3"));
    store.spill_key(b"key1").unwrap();
    store.spill_key(b"key2").unwrap();
    assert_eq!(store.warm_tier().warm_keys(), 2);

    // FLUSHDB clears the store and range-deletes the warm CF.
    store.clear();
    assert_eq!(store.len(), 0);
    assert_eq!(store.warm_tier().warm_keys(), 0);
    assert_eq!(store.memory_used(), 0);

    // Every spilled key is gone from the warm CF...
    assert!(rocks.get_warm(0, b"key1").unwrap().is_none());
    assert!(rocks.get_warm(0, b"key2").unwrap().is_none());
    // ...and the CF is entirely empty, so a recovery scan (which iterates the
    // warm CF) resurrects nothing after a restart.
    assert_eq!(
        rocks.iter_warm_cf(0).unwrap().count(),
        0,
        "the warm CF must be empty after clear"
    );
}

#[test]
fn test_memory_accounting_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(
        Bytes::from("key1"),
        Value::string("a long value that uses memory"),
    );
    let mem_hot = store.memory_used();

    store.spill_key(b"key1").unwrap();
    let mem_warm = store.memory_used();

    // Warm entry should use less memory (no value bytes)
    assert!(
        mem_warm < mem_hot,
        "warm={} should be < hot={}",
        mem_warm,
        mem_hot
    );

    // Unspill back
    store.get(b"key1");
    let mem_unspilled = store.memory_used();

    // Should be back to roughly same memory
    assert_eq!(mem_unspilled, mem_hot);
}

#[test]
fn test_spill_errors() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    // Key not found
    assert!(store.spill_key(b"nonexistent").is_err());

    // Already warm
    store.set(Bytes::from("key1"), Value::string("v"));
    store.spill_key(b"key1").unwrap();
    assert!(store.spill_key(b"key1").is_err());
}

#[test]
fn test_spill_no_warm_store() {
    let mut store = HashMapStore::new(); // No warm store configured

    store.set(Bytes::from("key1"), Value::string("v"));
    assert!(store.spill_key(b"key1").is_err());
}

#[test]
fn test_scan_includes_warm_keys() {
    let (mut store, _rocks, _tmp) = store_with_warm();

    store.set(Bytes::from("hot1"), Value::string("v"));
    store.set(Bytes::from("warm1"), Value::string("v"));
    store.spill_key(b"warm1").unwrap();

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
    assert_eq!(store.warm_tier().warm_keys(), 1); // warm_key only
}

#[test]
fn test_real_spill_recovers_across_reopen() {
    // Unlike `test_recovery_with_warm_entries` (which hand-writes the warm CF via
    // `put_warm`), this drives the *real* spill operation — `store.spill_key`, the
    // same call the `tiered-lru` eviction path invokes — so the warm CF holds
    // genuine spilled artifacts. It then drops the store (a "restart") and
    // recovers a fresh store from the same RocksDB, asserting every recovery
    // branch: warm-only keys restored *as warm*, a hot+warm dual-presence key
    // resolving to the hot copy (its real spilled warm copy pruned as stale), and
    // an expired spilled key that must not resurrect. This is the store-level
    // deterministic complement to the server-level memory-pressure restart tests
    // in `server/tests/integration_persistence.rs`.
    use frogdb_core::persistence::recover_all_shards;
    use frogdb_persistence::serialize;

    let (mut store, rocks, _tmp) = store_with_warm();

    // (1) Warm-only string: value lives ONLY in the warm CF, put there by a real
    //     spill. `store.set` is in-memory only (no WAL wired here), so nothing
    //     lands in the primary CF — recovery must restore it *as warm*.
    store.set(Bytes::from("warm_str"), Value::string("warm_string_value"));
    let freed = store.spill_key(b"warm_str").unwrap();
    assert!(freed > 0, "a real spill must free value bytes from RAM");

    // (2) Warm-only sorted set: a richer value shape, also via a real spill.
    let mut zset = SortedSetValue::new();
    zset.add(Bytes::from("m1"), 1.5);
    zset.add(Bytes::from("m2"), 2.5);
    store.set(Bytes::from("warm_zset"), Value::SortedSet(zset));
    store.spill_key(b"warm_zset").unwrap();

    // (3) Dual presence: a real spilled warm copy AND a hot copy in the primary CF
    //     (as the WAL flush would leave one). The primary/hot copy carries a
    //     distinct value so recovery precedence — hot wins — is observable, and
    //     the redundant real warm artifact must be pruned as stale.
    store.set(Bytes::from("dual"), Value::string("warm_side_v1"));
    store.spill_key(b"dual").unwrap(); // warm CF: "warm_side_v1" (real spill)
    rocks
        .put(
            0,
            b"dual",
            &serialize(&Value::string("hot_side_v2"), &KeyMetadata::new(11)),
        )
        .unwrap(); // primary CF: "hot_side_v2" (as a WAL flush would persist)

    // (4) Expired warm-only key: real spill of an already-past-TTL key. Recovery
    //     must filter and prune it — no resurrection.
    store.set(Bytes::from("warm_expired"), Value::string("gone"));
    store.set_expiry(b"warm_expired", Instant::now() - Duration::from_secs(1));
    store.spill_key(b"warm_expired").unwrap();

    // (5) Plain hot-only survivor in the primary CF (control: ordinary hot
    //     recovery must still work alongside the warm-tier paths).
    rocks
        .put(
            0,
            b"hot_only",
            &serialize(&Value::string("hot_only_v"), &KeyMetadata::new(10)),
        )
        .unwrap();

    // --- "Restart": drop the live store, recover a fresh one from the same DB ---
    drop(store);
    let (results, stats) = recover_all_shards(&rocks).unwrap();
    let mut store = results.into_iter().next().unwrap().0;
    // Re-wire the warm-tier handle, exactly as the server does after recovery
    // (`spawn_shard_workers` → `store.set_warm_store`), so unspill reads can
    // reach the warm CF. Recovery only reinstates the warm-key *bookkeeping*.
    store.set_warm_store(rocks.clone(), 0);

    // Warm-only keys restored as warm from real spilled artifacts.
    assert_eq!(
        stats.warm_keys_loaded, 2,
        "warm_str + warm_zset must restore as warm entries"
    );
    // Dual-presence resolved to the hot copy; its real warm artifact pruned.
    assert_eq!(
        stats.warm_keys_stale, 1,
        "the dual key's warm copy must be pruned as stale (hot wins)"
    );
    // Expired spilled key filtered during warm recovery.
    assert_eq!(
        stats.keys_expired_skipped, 1,
        "the past-TTL spilled key must be skipped on recovery"
    );

    // (1) Warm string recovered with correct value + type; still warm until read.
    assert_eq!(store.key_type(b"warm_str"), KeyType::String);
    assert_eq!(
        store
            .get(b"warm_str")
            .unwrap()
            .as_string()
            .unwrap()
            .as_bytes()
            .as_ref(),
        b"warm_string_value"
    );

    // (2) Warm sorted set recovered intact.
    assert_eq!(store.key_type(b"warm_zset"), KeyType::SortedSet);
    let z = store.get(b"warm_zset").unwrap();
    let z = z.as_sorted_set().unwrap();
    assert_eq!(z.len(), 2);
    assert_eq!(z.get_score(b"m1"), Some(1.5));
    assert_eq!(z.get_score(b"m2"), Some(2.5));

    // (3) Dual presence resolved to the HOT copy.
    assert_eq!(
        store
            .get(b"dual")
            .unwrap()
            .as_string()
            .unwrap()
            .as_bytes()
            .as_ref(),
        b"hot_side_v2",
        "hot copy must win over the spilled warm copy on recovery"
    );
    // The stale warm copy was pruned from the warm CF.
    assert!(rocks.get_warm(0, b"dual").unwrap().is_none());

    // (4) Expired spilled key did NOT resurrect, and its warm CF entry is pruned.
    assert!(!store.contains(b"warm_expired"));
    assert!(rocks.get_warm(0, b"warm_expired").unwrap().is_none());

    // (5) Ordinary hot key recovered.
    assert_eq!(
        store
            .get(b"hot_only")
            .unwrap()
            .as_string()
            .unwrap()
            .as_bytes()
            .as_ref(),
        b"hot_only_v"
    );

    // Exactly the four live keys are present (expired one gone).
    assert_eq!(store.len(), 4);
}
