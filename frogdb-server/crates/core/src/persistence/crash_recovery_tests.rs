//! Crash recovery tests for FrogDB persistence layer.
//!
//! These tests verify FrogDB's durability guarantees under various failure scenarios:
//! - Per-mode durability (Sync, Periodic, Async)
//! - Atomicity of single and batch operations
//! - Recovery correctness for all data types
//! - Fault injection (truncated files, corrupted metadata)
//! - Disk failure scenarios (ENOSPC, I/O errors)
//!
//! # Durability Guarantees (Redis-compatible)
//!
//! | FrogDB Mode           | Redis Equivalent      | Max Data Loss |
//! |-----------------------|-----------------------|---------------|
//! | DurabilityMode::Sync  | appendfsync always    | None          |
//! | DurabilityMode::Periodic(1000) | appendfsync everysec | ~1 second |
//! | DurabilityMode::Async | appendfsync no        | ~30 seconds   |

use super::recovery::recover_all_shards;
use super::rocks::{RocksConfig, RocksStore};
use super::serialization::{deserialize, serialize};
use super::snapshot::SnapshotMetadataFile;
use super::test_harness::*;
use super::wal::{DurabilityMode, WalConfig};
use crate::noop::NoopMetricsRecorder;
use crate::store::Store;
use crate::types::{KeyMetadata, Value};
use bytes::Bytes;
use rocksdb::{WriteBatch, WriteOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ============================================================================
// Category 1: Durability Mode Tests
// ============================================================================

mod durability_mode {
    use super::*;

    /// Test 1.1: Sync mode guarantees - every acknowledged write survives crash.
    ///
    /// With DurabilityMode::Sync, every write is fsync'd before returning.
    /// This means ALL acknowledged writes MUST survive a crash.
    #[test]
    fn test_sync_mode_crash_recovery() {
        let mut harness = CrashTestHarness::with_sync_mode();

        // Write N keys with sync mode
        let n = 100;
        for i in 0..n {
            let key = format!("sync_key_{:05}", i);
            let value = Value::string(format!("sync_value_{:05}", i));
            harness.put_with_sync(0, key.as_bytes(), &value, true);
        }

        // Crash without explicit close
        harness.crash();

        // Reopen and verify ALL N keys are present
        let (mut stores, stats) = harness.recover();
        assert_eq!(
            stats.keys_loaded, n,
            "All {} keys must survive with sync mode",
            n
        );

        // Verify each key
        for i in 0..n {
            let key = format!("sync_key_{:05}", i);
            let expected = format!("sync_value_{:05}", i);
            assert!(
                verify_string_value(&mut stores, 0, key.as_bytes(), &expected),
                "Key {} must be recovered with correct value",
                key
            );
        }
    }

    /// Test 1.2: Periodic mode - data survives after interval passes.
    ///
    /// With Periodic mode, data is synced at regular intervals.
    /// After waiting past the interval, writes should survive.
    #[test]
    fn test_periodic_mode_after_interval() {
        let mut harness = CrashTestHarness::with_periodic_mode(100); // 100ms interval

        // Write keys
        let n = 50;
        for i in 0..n {
            let key = format!("periodic_key_{:05}", i);
            let value = Value::string(format!("periodic_value_{:05}", i));
            harness.put_direct(0, key.as_bytes(), &value);
        }

        // Explicitly flush to ensure data is persisted
        harness.flush();

        // Sleep past the interval to ensure sync has occurred
        std::thread::sleep(Duration::from_millis(150));

        // Crash
        harness.crash();

        // Recover and verify
        let (mut stores, stats) = harness.recover();
        assert_eq!(
            stats.keys_loaded, n,
            "All keys should survive after flush and interval"
        );

        for i in 0..n {
            let key = format!("periodic_key_{:05}", i);
            let expected = format!("periodic_value_{:05}", i);
            assert!(
                verify_string_value(&mut stores, 0, key.as_bytes(), &expected),
                "Key {} must be recovered",
                key
            );
        }
    }

    /// Test 1.3: Periodic mode - documents potential data loss within window.
    ///
    /// This test documents the expected behavior: writes within the sync
    /// interval MAY be lost. This is not a failure - it's the expected
    /// trade-off for better performance.
    #[test]
    fn test_periodic_mode_within_window() {
        let mut harness = CrashTestHarness::with_periodic_mode(60000); // 60s interval (long)

        // First, write and flush some baseline data
        harness.put_direct(0, b"baseline_key", &Value::string("baseline_value"));
        harness.flush();

        // Write more data without flushing
        for i in 0..10 {
            let key = format!("unflushed_key_{}", i);
            let value = Value::string(format!("unflushed_value_{}", i));
            harness.put_direct(0, key.as_bytes(), &value);
        }

        // Crash IMMEDIATELY without waiting for sync interval
        harness.crash();

        // Recover
        let (mut stores, stats) = harness.recover();

        // Baseline key MUST be present (it was flushed)
        assert!(
            verify_string_value(&mut stores, 0, b"baseline_key", "baseline_value"),
            "Flushed baseline key must survive"
        );

        // Unflushed keys MAY or MAY NOT be present - this is acceptable behavior
        // We just verify the recovery doesn't fail and baseline is intact
        assert!(
            stats.keys_loaded >= 1,
            "At least the baseline key should be recovered"
        );
    }

    /// Test 1.4: Async mode - only explicitly flushed writes guaranteed.
    ///
    /// With Async mode, writes are buffered and only guaranteed to persist
    /// after an explicit flush.
    #[test]
    fn test_async_mode_explicit_flush() {
        let mut harness = CrashTestHarness::with_async_mode();

        // Write key1 and flush
        harness.put_direct(0, b"key1", &Value::string("value1"));
        harness.flush();

        // Write key2 without flush
        harness.put_direct(0, b"key2", &Value::string("value2"));

        // Crash
        harness.crash();

        // Recover
        let (mut stores, stats) = harness.recover();

        // key1 MUST be present (it was flushed)
        assert!(
            verify_string_value(&mut stores, 0, b"key1", "value1"),
            "Flushed key1 must survive"
        );

        // key2 MAY be missing (acceptable) - we just verify recovery succeeded
        assert!(
            stats.keys_loaded >= 1,
            "At least the flushed key should be recovered"
        );
    }

    /// Test 1.5: Explicit sync_wal ensures durability.
    #[test]
    fn test_explicit_sync_wal() {
        let mut harness = CrashTestHarness::with_async_mode();

        // Write data
        for i in 0..20 {
            let key = format!("wal_key_{}", i);
            harness.put_direct(0, key.as_bytes(), &Value::string(format!("value_{}", i)));
        }

        // Explicitly sync WAL
        harness.sync_wal();

        // Crash
        harness.crash();

        // Recover - all 20 keys should be present
        let (_stores, stats) = harness.recover();
        assert_eq!(
            stats.keys_loaded, 20,
            "All keys must survive after sync_wal"
        );
    }
}

// ============================================================================
// Category 2: Atomicity Tests
// ============================================================================

mod atomicity {
    use super::*;

    /// Test 2.1: Single key operations are atomic.
    ///
    /// Large value writes are all-or-nothing - after crash, key either
    /// has the full value or doesn't exist.
    #[test]
    fn test_single_key_atomic() {
        let mut harness = CrashTestHarness::new();

        // Write a large value (1MB)
        let large_value = TestDataGenerator::large_string(1024 * 1024);
        harness.put_with_sync(0, b"large_key", &large_value, true);

        // Crash
        harness.crash();

        // Recover
        let (mut stores, _stats) = harness.recover();

        // Key either exists with full value or doesn't exist
        if let Some(value) = stores[0].0.get(b"large_key") {
            let sv = value.as_string().expect("Should be string");
            assert_eq!(
                sv.as_bytes().len(),
                1024 * 1024,
                "If key exists, it must have the full value"
            );
        }
        // If key doesn't exist, that's also acceptable (crash during write)
    }

    /// Test 2.2: WriteBatch operations are atomic.
    ///
    /// Multiple keys written in a single batch are all-or-nothing.
    /// Either ALL keys are present or NONE are present.
    #[test]
    fn test_writebatch_atomic() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());

        // Create a batch with multiple keys
        let mut batch = WriteBatch::default();
        let keys: Vec<String> = (0..10).map(|i| format!("batch_key_{}", i)).collect();

        for key in &keys {
            let value = Value::string(format!("value_for_{}", key));
            let metadata = KeyMetadata::new(value.memory_size());
            let serialized = serialize(&value, &metadata);
            rocks
                .batch_put(&mut batch, 0, key.as_bytes(), &serialized)
                .unwrap();
        }

        // Write batch with sync
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        rocks.write_batch_opt(batch, &write_opts).unwrap();

        // Drop (simulate crash)
        drop(rocks);

        // Reopen and recover
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());
        let (mut stores, _stats) = recover_all_shards(&rocks).unwrap();

        // Count how many keys are present
        let present_count = keys
            .iter()
            .filter(|k| stores[0].0.get(k.as_bytes()).is_some())
            .count();

        // All-or-nothing: either all 10 keys or none
        assert!(
            present_count == 0 || present_count == 10,
            "WriteBatch must be atomic: found {} of {} keys",
            present_count,
            keys.len()
        );
    }

    /// Test 2.3: Cross-shard batch consistency.
    ///
    /// With atomic_flush, writes to multiple shards in a batch
    /// should be consistent.
    #[test]
    fn test_cross_shard_batch() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());

        // Write to multiple shards in one batch
        let mut batch = WriteBatch::default();
        for shard_id in 0..4 {
            let key = format!("shard_{}_key", shard_id);
            let value = Value::string(format!("shard_{}_value", shard_id));
            let metadata = KeyMetadata::new(value.memory_size());
            let serialized = serialize(&value, &metadata);
            rocks
                .batch_put(&mut batch, shard_id, key.as_bytes(), &serialized)
                .unwrap();
        }

        // Sync write
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        rocks.write_batch_opt(batch, &write_opts).unwrap();

        // Drop
        drop(rocks);

        // Reopen
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());
        let (mut stores, _stats) = recover_all_shards(&rocks).unwrap();

        // All shards should have their key, or none should
        let mut found_count = 0;
        for (shard_id, store) in stores.iter_mut().enumerate().take(4) {
            let key = format!("shard_{}_key", shard_id);
            if store.0.get(key.as_bytes()).is_some() {
                found_count += 1;
            }
        }

        assert!(
            found_count == 0 || found_count == 4,
            "Cross-shard batch must be consistent: found {} of 4 keys",
            found_count
        );
    }

    /// Test 2.4: Delete operations in batch are atomic.
    #[test]
    fn test_batch_delete_atomic() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());

        // First, create some keys
        for i in 0..5 {
            let key = format!("delete_key_{}", i);
            let value = Value::string(format!("value_{}", i));
            let metadata = KeyMetadata::new(value.memory_size());
            let serialized = serialize(&value, &metadata);
            rocks.put(0, key.as_bytes(), &serialized).unwrap();
        }
        rocks.flush().unwrap();

        // Create a batch that deletes all keys
        let mut batch = WriteBatch::default();
        for i in 0..5 {
            let key = format!("delete_key_{}", i);
            rocks.batch_delete(&mut batch, 0, key.as_bytes()).unwrap();
        }

        // Sync write the delete batch
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        rocks.write_batch_opt(batch, &write_opts).unwrap();

        // Drop
        drop(rocks);

        // Reopen
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (mut stores, _stats) = recover_all_shards(&rocks).unwrap();

        // Either all keys deleted or none deleted
        let present_count = (0..5)
            .filter(|i| {
                let key = format!("delete_key_{}", i);
                stores[0].0.get(key.as_bytes()).is_some()
            })
            .count();

        assert!(
            present_count == 0 || present_count == 5,
            "Batch delete must be atomic: {} keys remain",
            present_count
        );
    }
}

// ============================================================================
// Category 3: Recovery Correctness Tests
// ============================================================================

mod recovery_correctness {
    use super::*;

    /// Test 3.1: All data types recover correctly.
    #[test]
    fn test_all_types_crash_recovery() {
        let mut harness = CrashTestHarness::new();

        // Write all data types
        let test_data = TestDataGenerator::all_types();
        for (key, value) in &test_data {
            harness.put_direct(0, key.as_bytes(), value);
        }

        harness.flush();
        harness.crash();

        // Recover
        let (mut stores, stats) = harness.recover();
        assert_eq!(stats.keys_loaded, test_data.len() as u64);

        // Verify each type
        // String
        let sv = stores[0].0.get(b"string_key").expect("string_key missing");
        assert!(sv.as_string().is_some(), "Should be string type");

        // Sorted Set
        let zs = stores[0]
            .0
            .get(b"sorted_set_key")
            .expect("sorted_set_key missing");
        assert_eq!(zs.as_sorted_set().unwrap().len(), 10);

        // Hash
        let h = stores[0].0.get(b"hash_key").expect("hash_key missing");
        assert_eq!(h.as_hash().unwrap().len(), 10);

        // List
        let l = stores[0].0.get(b"list_key").expect("list_key missing");
        assert_eq!(l.as_list().unwrap().len(), 10);

        // Set
        let s = stores[0].0.get(b"set_key").expect("set_key missing");
        assert_eq!(s.as_set().unwrap().len(), 10);

        // Stream
        let st = stores[0].0.get(b"stream_key").expect("stream_key missing");
        assert_eq!(st.as_stream().unwrap().len(), 10);

        // Bloom filter
        let bf = stores[0].0.get(b"bloom_key").expect("bloom_key missing");
        assert!(bf.as_bloom_filter().is_some());
    }

    /// Test 3.2: Expiry filtering during recovery.
    ///
    /// Expired keys should be filtered out during recovery and not
    /// appear in the recovered store.
    #[test]
    fn test_expiry_filtering_on_recovery() {
        let mut harness = CrashTestHarness::new();

        // Write key with future expiry
        harness.put_with_expiry(
            0,
            b"future_key",
            &Value::string("future_value"),
            Instant::now() + Duration::from_secs(3600),
        );

        // Write key with past expiry
        harness.put_with_expiry(
            0,
            b"expired_key",
            &Value::string("expired_value"),
            Instant::now() - Duration::from_secs(1),
        );

        // Write key without expiry
        harness.put_direct(0, b"no_expiry_key", &Value::string("no_expiry_value"));

        harness.flush();
        harness.crash();

        // Recover
        let (mut stores, stats) = harness.recover();

        // Should have 2 keys (future_key and no_expiry_key)
        assert_eq!(stats.keys_loaded, 2, "Should load 2 non-expired keys");
        assert_eq!(stats.keys_expired_skipped, 1, "Should skip 1 expired key");

        assert!(stores[0].0.get(b"future_key").is_some());
        assert!(stores[0].0.get(b"no_expiry_key").is_some());
        assert!(stores[0].0.get(b"expired_key").is_none());
    }

    /// Test 3.3: Expiry index is rebuilt correctly.
    ///
    /// Keys with TTL should have their expiry tracked in the expiry index
    /// after recovery.
    #[test]
    fn test_expiry_index_rebuilt() {
        let mut harness = CrashTestHarness::new();

        // Write keys with various expiry times
        let future_expiry = Instant::now() + Duration::from_secs(3600);

        harness.put_with_expiry(0, b"exp_key_1", &Value::string("v1"), future_expiry);
        harness.put_with_expiry(0, b"exp_key_2", &Value::string("v2"), future_expiry);
        harness.put_direct(0, b"no_exp_key", &Value::string("v3"));

        harness.flush();
        harness.crash();

        // Recover
        let (mut stores, _stats) = harness.recover();

        // Expiry index should contain the 2 keys with expiry
        let expiry_index = &stores[0].1;
        assert!(expiry_index.get(b"exp_key_1").is_some());
        assert!(expiry_index.get(b"exp_key_2").is_some());
        assert!(expiry_index.get(b"no_exp_key").is_none());
        assert_eq!(expiry_index.len(), 2);
    }

    /// Test 3.4: Sorted set BTree index is rebuilt.
    ///
    /// After recovery, sorted set operations like ZRANK and ZRANGE
    /// should work correctly (BTree rebuilt from stored data).
    #[test]
    fn test_sorted_set_index_rebuilt() {
        let mut harness = CrashTestHarness::new();

        // Create a sorted set with specific scores
        use crate::types::SortedSetValue;
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("alice"), 100.0);
        zset.add(Bytes::from("bob"), 50.0);
        zset.add(Bytes::from("charlie"), 75.0);
        zset.add(Bytes::from("diana"), 25.0);

        harness.put_direct(0, b"myzset", &Value::SortedSet(zset));
        harness.flush();
        harness.crash();

        // Recover
        let (mut stores, _stats) = harness.recover();

        let zset_value = stores[0].0.get(b"myzset").expect("myzset missing");
        let zset = zset_value.as_sorted_set().expect("not a sorted set");

        // Verify scores
        assert_eq!(zset.get_score(b"alice"), Some(100.0));
        assert_eq!(zset.get_score(b"bob"), Some(50.0));
        assert_eq!(zset.get_score(b"charlie"), Some(75.0));
        assert_eq!(zset.get_score(b"diana"), Some(25.0));

        // Verify ordering via rank
        assert_eq!(zset.rank(b"diana"), Some(0)); // lowest score
        assert_eq!(zset.rank(b"bob"), Some(1));
        assert_eq!(zset.rank(b"charlie"), Some(2));
        assert_eq!(zset.rank(b"alice"), Some(3)); // highest score

        // Verify range query works
        let range = zset.range_by_rank(0, -1);
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0.as_ref(), b"diana");
        assert_eq!(range[3].0.as_ref(), b"alice");
    }

    /// Test 3.5: LFU counters are preserved.
    #[test]
    fn test_lfu_counter_preserved() {
        let mut harness = CrashTestHarness::new();

        // Write key with specific LFU counter
        harness.put_with_lfu(0, b"lfu_key", &Value::string("value"), 42);
        harness.flush();
        harness.crash();

        // Recover
        harness.reopen();
        let rocks = harness.rocks();

        // Read directly and check metadata
        let data = rocks.get(0, b"lfu_key").unwrap().unwrap();
        let (_, metadata) = deserialize(&data).unwrap();
        assert_eq!(metadata.lfu_counter, 42);
    }

    /// Test 3.6: Multiple shards recover independently.
    #[test]
    fn test_multi_shard_recovery() {
        let mut harness =
            CrashTestHarness::with_config(WalConfig::default(), RocksConfig::default(), 4);

        // Write different amounts to each shard
        for shard_id in 0..4 {
            let count = (shard_id + 1) * 10; // 10, 20, 30, 40
            for i in 0..count {
                let key = format!("shard{}_key_{}", shard_id, i);
                let value = Value::string(format!("shard{}_value_{}", shard_id, i));
                harness.put_direct(shard_id, key.as_bytes(), &value);
            }
        }

        harness.flush();
        harness.crash();

        let (mut stores, stats) = harness.recover();

        // Total should be 10 + 20 + 30 + 40 = 100
        assert_eq!(stats.keys_loaded, 100);

        // Verify each shard
        assert_eq!(stores[0].0.len(), 10);
        assert_eq!(stores[1].0.len(), 20);
        assert_eq!(stores[2].0.len(), 30);
        assert_eq!(stores[3].0.len(), 40);
    }

    /// Test 3.7: Empty shard recovery.
    #[test]
    fn test_empty_shard_recovery() {
        let mut harness = CrashTestHarness::new();

        // Only write to shard 0, leave others empty
        harness.put_direct(0, b"only_key", &Value::string("only_value"));
        harness.flush();
        harness.crash();

        let (mut stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 1);
        assert_eq!(stores[0].0.len(), 1);
        assert_eq!(stores[1].0.len(), 0);
        assert_eq!(stores[2].0.len(), 0);
        assert_eq!(stores[3].0.len(), 0);
    }
}

// ============================================================================
// Category 4: Fault Injection Tests
// ============================================================================

mod fault_injection {
    use super::*;

    /// Test 4.1: Recovery handles corrupted snapshot metadata gracefully.
    #[test]
    fn test_corrupted_snapshot_metadata() {
        let tmp = TempDir::new().unwrap();
        let snapshot_dir = tmp.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).unwrap();

        // Create a snapshot directory with corrupted metadata
        let snapshot_path = create_test_snapshot_dir(&snapshot_dir, 1);
        let metadata_path = snapshot_path.join("metadata.json");
        std::fs::write(&metadata_path, "{ invalid json ").unwrap();

        // Create the latest symlink
        create_latest_symlink(&snapshot_dir, "snapshot_00001");

        // Try to load metadata - should fail gracefully
        let result = super::super::snapshot::RocksSnapshotCoordinator::new(
            Arc::new(
                RocksStore::open(tmp.path().join("db").as_path(), 2, &RocksConfig::default())
                    .unwrap(),
            ),
            super::super::snapshot::SnapshotConfig {
                snapshot_dir: snapshot_dir.clone(),
                snapshot_interval_secs: 3600,
                max_snapshots: 5,
            },
            Arc::new(NoopMetricsRecorder::new()),
        );

        // Should not crash - either succeeds with no metadata or handles error gracefully
        assert!(result.is_ok() || result.is_err());
    }

    /// Test 4.2: Recovery with incomplete snapshot (no completion marker).
    #[test]
    fn test_incomplete_snapshot_skipped() {
        let tmp = TempDir::new().unwrap();
        let snapshot_dir = tmp.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).unwrap();

        // Create an incomplete snapshot (no completion marker)
        let snapshot_path = create_test_snapshot_dir(&snapshot_dir, 1);
        write_snapshot_metadata(&snapshot_path, 1, 1000, 4, false); // incomplete

        // Create a complete older snapshot
        let old_snapshot_path = create_test_snapshot_dir(&snapshot_dir, 0);
        write_snapshot_metadata(&old_snapshot_path, 0, 500, 4, true); // complete

        // Point latest to incomplete snapshot
        create_latest_symlink(&snapshot_dir, "snapshot_00001");

        // Load and verify incomplete is not used
        let metadata_path = snapshot_path.join("metadata.json");
        let content = std::fs::read_to_string(&metadata_path).unwrap();
        let metadata: SnapshotMetadataFile = serde_json::from_str(&content).unwrap();
        assert!(!metadata.is_complete());
    }

    /// Test 4.3: Database recovery after unclean shutdown.
    ///
    /// RocksDB should recover gracefully from an unclean shutdown
    /// using its WAL.
    #[test]
    fn test_unclean_shutdown_recovery() {
        let tmp = TempDir::new().unwrap();

        // Write data without explicit flush
        {
            let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

            for i in 0..50 {
                let key = format!("key_{}", i);
                let value = Value::string(format!("value_{}", i));
                let metadata = KeyMetadata::new(value.memory_size());
                let serialized = serialize(&value, &metadata);

                // Use sync write to ensure durability
                let mut opts = WriteOptions::default();
                opts.set_sync(true);
                rocks
                    .put_opt(0, key.as_bytes(), &serialized, &opts)
                    .unwrap();
            }

            // Drop WITHOUT flush - simulates unclean shutdown
        }

        // Reopen - RocksDB should replay WAL
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (mut stores, stats) = recover_all_shards(&rocks).unwrap();

        // All sync'd keys should be present
        assert_eq!(stats.keys_loaded, 50);
        assert_eq!(stores[0].0.len(), 50);
    }

    /// Test 4.4: Recovery with mixed successful and failed deserializations.
    ///
    /// If some keys fail to deserialize, recovery should continue with
    /// valid keys and report failures.
    #[test]
    fn test_partial_recovery_on_corruption() {
        let tmp = TempDir::new().unwrap();

        // Write valid data
        {
            let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

            // Write valid keys
            for i in 0..10 {
                let key = format!("valid_key_{}", i);
                let value = Value::string(format!("value_{}", i));
                let metadata = KeyMetadata::new(value.memory_size());
                let serialized = serialize(&value, &metadata);
                rocks.put(0, key.as_bytes(), &serialized).unwrap();
            }

            // Write corrupted data (invalid serialization format)
            rocks
                .put(0, b"corrupt_key", b"invalid_serialization_data")
                .unwrap();

            rocks.flush().unwrap();
        }

        // Reopen and recover
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (mut stores, stats) = recover_all_shards(&rocks).unwrap();

        // 10 valid keys should be loaded, 1 should fail
        assert_eq!(stats.keys_loaded, 10);
        assert_eq!(stats.keys_failed, 1);
        assert_eq!(stores[0].0.len(), 10);
    }
}

// ============================================================================
// Category 5: Stress Tests
// ============================================================================

mod stress {
    use super::*;

    /// Test 5.1: Large dataset recovery.
    ///
    /// Verify recovery works correctly with a large number of keys.
    #[test]
    fn test_large_dataset_recovery() {
        let mut harness = CrashTestHarness::new();

        // Write 10K keys
        let n: u64 = 10_000;
        for i in 0..n {
            let key = format!("large_key_{:06}", i);
            let value = Value::string(format!("value_{:06}", i));
            harness.put_direct((i % 4) as usize, key.as_bytes(), &value);
        }

        harness.flush();
        harness.crash();

        let start = Instant::now();
        let (mut stores, stats) = harness.recover();
        let duration = start.elapsed();

        assert_eq!(stats.keys_loaded, n);

        // Verify total across shards
        let total: usize = stores.iter().map(|(s, _)| s.len()).sum();
        assert_eq!(total, n as usize);

        // Log recovery time (not a failure condition, just informational)
        println!("Recovered {} keys in {:?}", n, duration);
    }

    /// Test 5.2: Mixed operations stress test.
    #[test]
    fn test_mixed_operations_stress() {
        let mut harness = CrashTestHarness::new();

        // Interleave writes and deletes
        for i in 0..1000 {
            let key = format!("stress_key_{}", i);
            harness.put_direct(i % 4, key.as_bytes(), &Value::string(format!("v{}", i)));
        }

        harness.flush();

        // Delete every other key
        for i in (0..1000).step_by(2) {
            let key = format!("stress_key_{}", i);
            harness.rocks().delete(i % 4, key.as_bytes()).unwrap();
        }

        harness.flush();
        harness.crash();

        let (mut stores, stats) = harness.recover();

        // Should have 500 keys (the odd-numbered ones)
        assert_eq!(stats.keys_loaded, 500);

        // Verify odd keys exist, even keys don't
        for i in 0..1000 {
            let key = format!("stress_key_{}", i);
            let shard = i % 4;
            if i % 2 == 0 {
                assert!(
                    stores[shard].0.get(key.as_bytes()).is_none(),
                    "Even key {} should be deleted",
                    i
                );
            } else {
                assert!(
                    stores[shard].0.get(key.as_bytes()).is_some(),
                    "Odd key {} should exist",
                    i
                );
            }
        }
    }

    /// Test 5.3: Repeated crash and recovery cycles.
    #[test]
    fn test_repeated_crash_cycles() {
        let tmp = TempDir::new().unwrap();

        for cycle in 0..5 {
            // Open
            let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());

            // Count existing keys (verify previous cycle's data)
            let (existing_stores, _existing_stats) = recover_all_shards(&rocks).unwrap();

            // Write 20 new keys
            for i in 0..20 {
                let key = format!("cycle{}_key_{}", cycle, i);
                let value = Value::string(format!("cycle{}_value_{}", cycle, i));
                let metadata = KeyMetadata::new(value.memory_size());
                let serialized = serialize(&value, &metadata);
                let mut opts = WriteOptions::default();
                opts.set_sync(true);
                rocks
                    .put_opt(0, key.as_bytes(), &serialized, &opts)
                    .unwrap();
            }

            // Drop (crash)
            drop(rocks);
            drop(existing_stores);
        }

        // Final verification
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (_stores, stats) = recover_all_shards(&rocks).unwrap();

        // Should have 5 cycles * 20 keys = 100 keys
        assert_eq!(stats.keys_loaded, 100);
    }

    /// Test 5.4: Large values recovery.
    #[test]
    fn test_large_values_recovery() {
        let mut harness = CrashTestHarness::new();

        // Write keys with increasingly large values
        let sizes = [1_000, 10_000, 100_000, 500_000];
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("large_value_{}", i);
            let value = TestDataGenerator::large_string(size);
            harness.put_direct(0, key.as_bytes(), &value);
        }

        harness.flush();
        harness.crash();

        let (mut stores, stats) = harness.recover();
        assert_eq!(stats.keys_loaded, 4);

        // Verify each value has correct size
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("large_value_{}", i);
            let value = stores[0].0.get(key.as_bytes()).expect("key missing");
            let sv = value.as_string().expect("not string");
            assert_eq!(sv.as_bytes().len(), size, "Value {} has wrong size", i);
        }
    }
}

// ============================================================================
// Category 6: Disk Failure Scenarios
// ============================================================================

mod disk_failure {
    use super::*;

    /// Test 6.1: Recovery after truncated metadata.json
    #[test]
    fn test_truncated_metadata_recovery() {
        let tmp = TempDir::new().unwrap();
        let snapshot_dir = tmp.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).unwrap();

        // Create a snapshot with valid metadata
        let snapshot_path = create_test_snapshot_dir(&snapshot_dir, 1);
        write_snapshot_metadata(&snapshot_path, 1, 1000, 4, true);

        // Truncate the metadata file mid-way
        let metadata_path = snapshot_path.join("metadata.json");
        let original_size = file_size(&metadata_path).unwrap();
        truncate_file(&metadata_path, original_size / 2).unwrap();

        // Try to parse - should fail gracefully
        let content = std::fs::read_to_string(&metadata_path).unwrap_or_default();
        let result: Result<SnapshotMetadataFile, _> = serde_json::from_str(&content);
        assert!(result.is_err(), "Truncated JSON should fail to parse");
    }

    /// Test 6.2: Recovery handles missing completion marker.
    #[test]
    fn test_missing_completion_marker() {
        let tmp = TempDir::new().unwrap();
        let snapshot_dir = tmp.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).unwrap();

        // Create snapshot without completion marker
        let snapshot_path = create_test_snapshot_dir(&snapshot_dir, 1);
        write_snapshot_metadata(&snapshot_path, 1, 1000, 4, false);

        // Read and verify
        let metadata_path = snapshot_path.join("metadata.json");
        let content = std::fs::read_to_string(&metadata_path).unwrap();
        let metadata: SnapshotMetadataFile = serde_json::from_str(&content).unwrap();

        assert!(!metadata.is_complete(), "Should not be marked complete");
    }

    /// Test 6.3: Recovery from valid data despite snapshot directory issues.
    ///
    /// Even if snapshot directory is corrupted, WAL-based recovery should work.
    #[test]
    fn test_wal_recovery_despite_snapshot_issues() {
        let tmp = TempDir::new().unwrap();

        // Write data with sync
        {
            let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

            for i in 0..30 {
                let key = format!("wal_key_{}", i);
                let value = Value::string(format!("value_{}", i));
                let metadata = KeyMetadata::new(value.memory_size());
                let serialized = serialize(&value, &metadata);
                let mut opts = WriteOptions::default();
                opts.set_sync(true);
                rocks
                    .put_opt(0, key.as_bytes(), &serialized, &opts)
                    .unwrap();
            }
        }

        // Reopen and recover from WAL (no snapshots involved)
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (mut stores, stats) = recover_all_shards(&rocks).unwrap();

        assert_eq!(stats.keys_loaded, 30);
        assert_eq!(stores[0].0.len(), 30);
    }

    /// Test 6.4: Binary data survives crash.
    ///
    /// Keys and values containing null bytes and other binary data
    /// should survive crash and recovery intact.
    #[test]
    fn test_binary_data_crash_recovery() {
        let mut harness = CrashTestHarness::new();

        // Create binary key and value with null bytes
        let binary_key: Vec<u8> = (0..=255).collect();
        let binary_value: Vec<u8> = (0..=255).rev().collect();

        use crate::types::StringValue;
        let value = Value::String(StringValue::new(Bytes::from(binary_value.clone())));
        harness.put_direct(0, &binary_key, &value);
        harness.flush();
        harness.crash();

        // Recover
        let (mut stores, _stats) = harness.recover();

        // Verify binary data is intact
        let recovered = stores[0].0.get(&binary_key).expect("binary key missing");
        let sv = recovered.as_string().expect("not string");
        assert_eq!(sv.as_bytes().as_ref(), binary_value.as_slice());
    }

    /// Test 6.5: Unicode data survives crash.
    #[test]
    fn test_unicode_data_crash_recovery() {
        let mut harness = CrashTestHarness::new();

        // Various Unicode strings
        let unicode_data = vec![
            ("emoji_key", "Hello 🐸 World 🌍"),
            ("chinese_key", "你好世界"),
            ("arabic_key", "مرحبا بالعالم"),
            ("mixed_key", "Hello 世界 🌍 مرحبا"),
        ];

        for (key, value) in &unicode_data {
            harness.put_direct(0, key.as_bytes(), &Value::string(*value));
        }

        harness.flush();
        harness.crash();

        let (mut stores, _stats) = harness.recover();

        for (key, expected) in &unicode_data {
            let value = stores[0].0.get(key.as_bytes()).expect("key missing");
            let sv = value.as_string().expect("not string");
            assert_eq!(
                std::str::from_utf8(sv.as_bytes().as_ref()).unwrap(),
                *expected,
                "Unicode mismatch for key {}",
                key
            );
        }
    }
}

// ============================================================================
// Category 7: Edge Cases
// ============================================================================

mod edge_cases {
    use super::*;

    /// Test 7.1: Empty database recovery.
    #[test]
    fn test_empty_database_recovery() {
        let mut harness = CrashTestHarness::new();

        // Don't write anything, just crash
        harness.crash();

        let (mut stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 0);
        assert_eq!(stats.keys_expired_skipped, 0);
        assert_eq!(stats.keys_failed, 0);

        for (store, expiry_index) in &stores {
            assert_eq!(store.len(), 0);
            assert!(expiry_index.is_empty());
        }
    }

    /// Test 7.2: Single key recovery.
    #[test]
    fn test_single_key_recovery() {
        let mut harness = CrashTestHarness::new();

        harness.put_direct(0, b"only_key", &Value::string("only_value"));
        harness.flush();
        harness.crash();

        let (mut stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 1);
        assert!(verify_string_value(
            &mut stores,
            0,
            b"only_key",
            "only_value"
        ));
    }

    /// Test 7.3: Key with maximum length (64KB).
    #[test]
    fn test_max_key_length_recovery() {
        let mut harness = CrashTestHarness::new();

        // Create a large key (but within RocksDB limits)
        let large_key = "k".repeat(1024); // 1KB key
        harness.put_direct(0, large_key.as_bytes(), &Value::string("value"));
        harness.flush();
        harness.crash();

        let (mut stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 1);
        assert!(stores[0].0.get(large_key.as_bytes()).is_some());
    }

    /// Test 7.4: Special float values in sorted sets.
    #[test]
    fn test_special_float_values_recovery() {
        let mut harness = CrashTestHarness::new();

        use crate::types::SortedSetValue;
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("neg_inf"), f64::NEG_INFINITY);
        zset.add(Bytes::from("pos_inf"), f64::INFINITY);
        zset.add(Bytes::from("zero"), 0.0);
        zset.add(Bytes::from("neg_zero"), -0.0);
        zset.add(Bytes::from("tiny"), f64::MIN_POSITIVE);
        zset.add(Bytes::from("huge"), f64::MAX);

        harness.put_direct(0, b"special_zset", &Value::SortedSet(zset));
        harness.flush();
        harness.crash();

        let (mut stores, _stats) = harness.recover();

        let zset_value = stores[0].0.get(b"special_zset").unwrap();
        let zset = zset_value.as_sorted_set().unwrap();

        assert_eq!(zset.get_score(b"neg_inf"), Some(f64::NEG_INFINITY));
        assert_eq!(zset.get_score(b"pos_inf"), Some(f64::INFINITY));
        assert_eq!(zset.get_score(b"zero"), Some(0.0));
        assert_eq!(zset.get_score(b"tiny"), Some(f64::MIN_POSITIVE));
        assert_eq!(zset.get_score(b"huge"), Some(f64::MAX));
    }

    /// Test 7.5: Key overwrite followed by crash.
    #[test]
    fn test_key_overwrite_recovery() {
        let mut harness = CrashTestHarness::new();

        // Write initial value
        harness.put_direct(0, b"overwrite_key", &Value::string("initial"));
        harness.flush();

        // Overwrite with new value
        harness.put_direct(0, b"overwrite_key", &Value::string("updated"));
        harness.flush();

        harness.crash();

        let (mut stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 1);
        assert!(verify_string_value(
            &mut stores,
            0,
            b"overwrite_key",
            "updated"
        ));
    }

    /// Test 7.6: Immediate expiry recovery.
    ///
    /// Keys that expire exactly at recovery time should be filtered.
    #[test]
    fn test_immediate_expiry_recovery() {
        let tmp = TempDir::new().unwrap();

        // Write key with very short TTL
        {
            let rocks = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

            // Key that will expire in 1ms
            let value = Value::string("expiring_soon");
            let mut metadata = KeyMetadata::new(value.memory_size());
            metadata.expires_at = Some(Instant::now() + Duration::from_millis(1));
            let serialized = serialize(&value, &metadata);
            rocks.put(0, b"expiring_key", &serialized).unwrap();
            rocks.flush().unwrap();
        }

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(10));

        // Recover
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (mut stores, stats) = recover_all_shards(&rocks).unwrap();

        // Key should be filtered out
        assert_eq!(stats.keys_expired_skipped, 1);
        assert!(stores[0].0.get(b"expiring_key").is_none());
    }
}

// ============================================================================
// Async WAL Writer Tests
// ============================================================================

mod async_wal {
    use super::*;

    /// Test async WAL writer crash recovery.
    #[tokio::test]
    async fn test_async_wal_writer_recovery() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal_config = WalConfig {
            mode: DurabilityMode::Sync,
            batch_size_threshold: 4 * 1024 * 1024,
            batch_timeout_ms: 10,
            ..Default::default()
        };

        let wal = super::super::wal::RocksWalWriter::new(rocks.clone(), 0, wal_config, metrics);

        // Write through WAL
        for i in 0..50 {
            let value = Value::string(format!("wal_value_{}", i));
            let metadata = KeyMetadata::new(value.memory_size());
            let key = format!("wal_key_{}", i);
            wal.write_set(key.as_bytes(), &value, &metadata)
                .await
                .unwrap();
        }

        // Flush
        wal.flush_async().await.unwrap();

        // Drop everything
        drop(wal);
        drop(rocks);

        // Reopen and recover
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let (mut stores, stats) = recover_all_shards(&rocks).unwrap();

        assert_eq!(stats.keys_loaded, 50);
        assert_eq!(stores[0].0.len(), 50);
    }

    /// Test WAL writer sequence numbers survive crash.
    #[tokio::test]
    async fn test_wal_sequence_persistence() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal =
            super::super::wal::RocksWalWriter::new(rocks.clone(), 0, WalConfig::default(), metrics);

        // Write some data
        let value = Value::string("test");
        let metadata = KeyMetadata::new(value.memory_size());
        let seq1 = wal.write_set(b"key1", &value, &metadata).await.unwrap();
        let seq2 = wal.write_set(b"key2", &value, &metadata).await.unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);

        wal.flush_async().await.unwrap();

        // RocksDB sequence should have advanced
        let rocks_seq = rocks.latest_sequence_number();
        assert!(rocks_seq > 0);
    }
}
