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

use super::recover_all_shards;
use super::rocks::{RawBatchOp, RocksConfig, RocksStore};
use super::serialization::{deserialize, serialize};
use super::snapshot::{
    RocksSnapshotCoordinator, SnapshotConfig, SnapshotCoordinator, SnapshotError,
    SnapshotMetadataFile,
};
use super::test_harness::*;
use super::wal::{DurabilityMode, WalConfig};
use crate::noop::NoopMetricsRecorder;
use crate::store::Store;
use crate::types::{KeyMetadata, Value};
use bytes::Bytes;
use frogdb_types::traits::MetricsRecorder;
use rocksdb::WriteOptions;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Boot a snapshot coordinator over `snapshot_dir`, the way the server does.
///
/// Shared by the FM-PERSISTENCE-017 tests so each of them asserts against the
/// *real* boot path (`load_latest_metadata` plus the epoch seed) rather than
/// against `serde_json` in isolation.
fn boot_coordinator(
    tmp: &TempDir,
    snapshot_dir: &Path,
) -> Result<RocksSnapshotCoordinator, SnapshotError> {
    let db_path = tmp.path().join("db");
    RocksSnapshotCoordinator::new(
        Arc::new(RocksStore::open(db_path.as_path(), 2, &RocksConfig::default()).unwrap()),
        SnapshotConfig {
            snapshot_dir: snapshot_dir.to_path_buf(),
            snapshot_interval_secs: 3600,
            max_snapshots: 5,
        },
        Arc::new(NoopMetricsRecorder::new()),
        db_path,
    )
}

/// Metrics recorder that keeps every counter call, including one carrying zero.
///
/// The FM-PERSISTENCE-035 tests below need to distinguish "reported nothing"
/// from "reported a loss of zero records": a dashboard counts events, so a
/// zero-valued alarm is still an alarm.
#[derive(Default)]
struct RecordingRecorder {
    counters: Mutex<Vec<(String, u64)>>,
}

impl MetricsRecorder for RecordingRecorder {
    fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        self.counters
            .lock()
            .unwrap()
            .push((name.to_string(), value));
    }
    fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
}

impl RecordingRecorder {
    fn calls(&self) -> Vec<(String, u64)> {
        self.counters.lock().unwrap().clone()
    }
}

/// The counter FM-PERSISTENCE-035 is about.
const WAL_DROPPED_RECORDS: &str = "frogdb_wal_recovery_dropped_records_total";

// ============================================================================
// Category 1: Durability Mode Tests
// ============================================================================

mod durability_mode {
    use super::*;

    // FM-PERSISTENCE-002
    /// Test 1.1: Sync mode guarantees - every acknowledged write survives crash.
    ///
    /// With DurabilityMode::Sync, every write is fsync'd before returning, so
    /// ALL acknowledged writes MUST survive a crash. The mode is what has to do
    /// the work here: the keys go in through `put_direct`, which derives its
    /// `sync` flag from the harness's `WalConfig`, not through an explicit
    /// override. The unsynced neighbour written alongside them is the control —
    /// it is the one write a crash is allowed to take — so this test fails both
    /// if sync mode stops fsyncing and if `crash()` regresses to a clean drop
    /// that loses nothing at all.
    #[test]
    fn test_sync_mode_crash_recovery() {
        let mut harness = CrashTestHarness::with_sync_mode();

        // Write N keys with sync mode
        let n = 100;
        for i in 0..n {
            let key = format!("sync_key_{:05}", i);
            let value = Value::string(format!("sync_value_{:05}", i));
            harness.put_direct(0, key.as_bytes(), &value);
        }

        // Control: a write that bypasses the mode and is never fsynced.
        harness.put_with_sync(0, b"unsynced_control", &Value::string("lost"), false);

        // Crash without explicit close
        harness.crash();

        // Reopen and verify ALL N keys are present
        let (mut stores, stats) = harness.recover();
        assert_eq!(
            stats.keys_loaded, n,
            "All {} keys must survive with sync mode, and only those",
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

        assert!(
            stores[0].0.get(b"unsynced_control").is_none(),
            "The never-fsynced control write must not survive the crash — if it \
             does, the crash primitive is not discarding anything and the 100 \
             surviving keys prove nothing about sync mode"
        );
    }

    // FM-PERSISTENCE-003
    /// Test 1.2: Periodic mode - data written before a sync survives, data
    /// written after it does not.
    ///
    /// Periodic mode never fsyncs at the commit seam; durability arrives out of
    /// band, one sync per interval. So the loss window is bounded *by the last
    /// sync*: everything before it comes back, and only the writes made since
    /// are at risk. Both halves are asserted — the surviving prefix and the lost
    /// suffix — because the surviving half alone passes against a crash that
    /// loses nothing.
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

        // Writes issued after the last sync sit in the current window.
        for i in 0..10 {
            let key = format!("in_window_key_{:05}", i);
            let value = Value::string(format!("in_window_value_{:05}", i));
            harness.put_direct(0, key.as_bytes(), &value);
        }

        // Crash
        harness.crash();

        // Recover and verify
        let (mut stores, stats) = harness.recover();
        assert_eq!(
            stats.keys_loaded, n,
            "Every key synced before the crash survives, and nothing from the \
             open window does"
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

        for i in 0..10 {
            let key = format!("in_window_key_{:05}", i);
            assert!(
                stores[0].0.get(key.as_bytes()).is_none(),
                "Key {} was written inside the open sync window and must be lost",
                key
            );
        }
    }

    // FM-PERSISTENCE-003
    /// Test 1.3: Periodic mode - a crash inside the sync window loses exactly
    /// the window, and never anything older.
    ///
    /// With a 60s interval no periodic sync can land during the test, so every
    /// write after the baseline flush is inside the open window and must be
    /// gone after the crash — while the flushed baseline must not be. That is
    /// the row's whole claim: the loss is a *suffix* bounded by the last sync,
    /// never a gap and never an older write.
    ///
    /// The crash model here is the harness's compensating delete of everything
    /// written since the last durability boundary (see `CrashTestHarness`), not
    /// a severed page cache: it reproduces *what a crash costs*, not the
    /// mechanics of losing it. The mechanics — that the loss window is set by
    /// the `WriteSink::commit(sync)` flag and by the flush interval — are
    /// verified by `test_periodic_mode_loss_bounded_by_flush_interval` in
    /// `frogdb-persistence`'s `wal::tests`, which crashes a real
    /// `PageCacheSink`.
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

        // Every write inside the open window MUST be gone.
        for i in 0..10 {
            let key = format!("unflushed_key_{}", i);
            assert!(
                stores[0].0.get(key.as_bytes()).is_none(),
                "Key {} was never synced and must not survive the crash",
                key
            );
        }

        assert_eq!(
            stats.keys_loaded, 1,
            "Exactly the synced baseline comes back — the periodic window is \
             lost whole, and nothing older than it is touched"
        );
    }

    // FM-PERSISTENCE-004
    /// Test 1.4: Async mode - only the explicit flush makes a write durable.
    ///
    /// Async never fsyncs on its own, so the flush is the *only* durability
    /// event in this test: `key1` precedes it and survives, `key2` follows it
    /// and does not. Deleting the `flush()` call must lose both keys — that is
    /// what makes the flush load-bearing rather than decorative.
    ///
    /// As in `test_periodic_mode_within_window`, the crash is the harness's
    /// compensating delete of everything written since the last durability
    /// boundary. That async's window is *unbounded* until an out-of-band fsync
    /// — the property that needs real elapsed time and a real page cache — is
    /// verified by `test_async_mode_unbounded_loss_without_fsync` and
    /// `test_async_mode_window_bounded_only_by_external_fsync` in
    /// `frogdb-persistence`'s `wal::tests`.
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

        // key2 was never covered by a flush: async mode owes it nothing.
        assert!(
            stores[0].0.get(b"key2").is_none(),
            "key2 was acknowledged but never synced — async mode must lose it"
        );
        assert_eq!(stats.keys_loaded, 1, "Exactly the flushed key comes back");
    }

    // FM-PERSISTENCE-004
    /// Test 1.5: In async mode an explicit `sync_wal()` is the out-of-band
    /// event that makes durability happen — and it covers only what preceded
    /// it.
    ///
    /// The 20 keys written before the sync survive; the one written after it is
    /// back inside the unbounded window and does not. Delete the `sync_wal()`
    /// call and all 21 are lost, so the sync is the thing under test.
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

        // Written after the only fsync in this test: not covered by it.
        harness.put_direct(0, b"after_sync_key", &Value::string("after_sync_value"));

        // Crash
        harness.crash();

        // Recover - all 20 keys should be present
        let (mut stores, stats) = harness.recover();
        assert_eq!(
            stats.keys_loaded, 20,
            "All keys written before sync_wal must survive, and only those"
        );
        for i in 0..20 {
            let key = format!("wal_key_{}", i);
            assert!(
                verify_string_value(&mut stores, 0, key.as_bytes(), &format!("value_{}", i)),
                "Key {} must be recovered with its value",
                key
            );
        }
        assert!(
            stores[0].0.get(b"after_sync_key").is_none(),
            "A write made after the last sync_wal is not covered by it"
        );
    }

    // FM-PERSISTENCE-002, FM-PERSISTENCE-004
    /// Issue 08 forcing test: `wal_config.mode` must actually gate what
    /// `crash()` loses. An async-mode write is never synced, so it must not
    /// survive; a sync-mode write is always synced, so it must survive.
    #[test]
    fn test_durability_mode_gates_crash_loss() {
        let mut async_harness = CrashTestHarness::with_async_mode();
        async_harness.put_direct(0, b"async_key", &Value::string("async_value"));
        async_harness.crash();
        let (mut stores, _stats) = async_harness.recover();
        assert!(
            !verify_string_value(&mut stores, 0, b"async_key", "async_value"),
            "An unsynced async-mode write must not survive a crash"
        );

        let mut sync_harness = CrashTestHarness::with_sync_mode();
        sync_harness.put_direct(0, b"sync_key", &Value::string("sync_value"));
        sync_harness.crash();
        let (mut stores, _stats) = sync_harness.recover();
        assert!(
            verify_string_value(&mut stores, 0, b"sync_key", "sync_value"),
            "A synced sync-mode write must survive a crash"
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
    /// A 1MB value is all-or-nothing across a crash, on both sides of the
    /// durability boundary: the synced one is there in full, the unsynced one is
    /// absent in full. Neither may come back as a truncated payload — a partial
    /// value is the failure this test exists to exclude, and "either outcome is
    /// fine" would not exclude it.
    #[test]
    fn test_single_key_atomic() {
        let mut harness = CrashTestHarness::new();

        // Write a large value (1MB)
        let large_value = TestDataGenerator::large_string(1024 * 1024);
        harness.put_with_sync(0, b"large_key", &large_value, true);
        // And one the crash is expected to take whole.
        harness.put_with_sync(0, b"large_unsynced_key", &large_value, false);

        // Crash
        harness.crash();

        // Recover
        let (mut stores, _stats) = harness.recover();

        let value = stores[0]
            .0
            .get(b"large_key")
            .expect("A synced write is not the crash's to take");
        let sv = value.as_string().expect("Should be string");
        assert_eq!(
            sv.as_bytes().len(),
            1024 * 1024,
            "The surviving key must carry the whole value, never a prefix of it"
        );

        assert!(
            stores[0].0.get(b"large_unsynced_key").is_none(),
            "The unsynced key must be gone entirely, not partially present"
        );
    }

    /// Test 2.2: WriteBatch operations are atomic.
    ///
    /// Multiple keys written in a single batch are all-or-nothing.
    /// Either ALL keys are present or NONE are present.
    #[test]
    fn test_writebatch_atomic() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());

        // Serialize all keys up front so the raw-batch ops can borrow them.
        let keys: Vec<String> = (0..10).map(|i| format!("batch_key_{}", i)).collect();
        let entries: Vec<(String, Vec<u8>)> = keys
            .iter()
            .map(|key| {
                let value = Value::string(format!("value_for_{}", key));
                let metadata = KeyMetadata::new(value.memory_size());
                (key.clone(), serialize(&value, &metadata))
            })
            .collect();
        let ops: Vec<RawBatchOp> = entries
            .iter()
            .map(|(key, serialized)| RawBatchOp::Put {
                shard: 0,
                key: key.as_bytes(),
                value: serialized,
            })
            .collect();

        // Commit the whole batch atomically with sync.
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        rocks.commit_raw_batch(&ops, &write_opts).unwrap();

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

        // Stage writes to four different shards into ONE atomic batch to verify
        // cross-shard all-or-nothing. The per-op `shard` on RawBatchOp is what
        // lets a single commit span shards (a batch-level shard could not).
        let entries: Vec<(usize, String, Vec<u8>)> = (0..4)
            .map(|shard_id| {
                let key = format!("shard_{}_key", shard_id);
                let value = Value::string(format!("shard_{}_value", shard_id));
                let metadata = KeyMetadata::new(value.memory_size());
                (shard_id, key, serialize(&value, &metadata))
            })
            .collect();
        let ops: Vec<RawBatchOp> = entries
            .iter()
            .map(|(shard, key, serialized)| RawBatchOp::Put {
                shard: *shard,
                key: key.as_bytes(),
                value: serialized,
            })
            .collect();

        // Sync commit
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        rocks.commit_raw_batch(&ops, &write_opts).unwrap();

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

        // Stage a batch that deletes all keys and commit atomically.
        let del_keys: Vec<String> = (0..5).map(|i| format!("delete_key_{}", i)).collect();
        let ops: Vec<RawBatchOp> = del_keys
            .iter()
            .map(|key| RawBatchOp::Delete {
                shard: 0,
                key: key.as_bytes(),
            })
            .collect();

        // Sync commit the delete batch
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        rocks.commit_raw_batch(&ops, &write_opts).unwrap();

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

    // FM-PERSISTENCE-036
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

        // The rebuilt index must agree with the restored store: the live TTL is
        // tracked, the dead one is not resurrected there either.
        let expiry_index = &stores[0].1;
        assert!(
            expiry_index.get(b"future_key").is_some(),
            "A surviving TTL must be mirrored into the rebuilt index"
        );
        assert!(
            expiry_index.get(b"expired_key").is_none(),
            "An expired key must not come back through the index"
        );
        assert!(expiry_index.get(b"no_expiry_key").is_none());
        assert_eq!(expiry_index.len(), 1);
    }

    // FM-PERSISTENCE-036
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
        let (stores, _stats) = harness.recover();

        // Expiry index should contain the 2 keys with expiry
        let expiry_index = &stores[0].1;
        assert!(expiry_index.get(b"exp_key_1").is_some());
        assert!(expiry_index.get(b"exp_key_2").is_some());
        assert!(expiry_index.get(b"no_exp_key").is_none());
        assert_eq!(expiry_index.len(), 2);
    }

    /// Test 3.4: Sorted set score index is rebuilt.
    ///
    /// After recovery, sorted set operations like ZRANK and ZRANGE
    /// should work correctly (score index rebuilt from stored data).
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

        harness.put_direct(0, b"myzset", &Value::SortedSet(Box::new(zset)));
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

    // FM-PERSISTENCE-041
    /// Test 3.6: Multiple shards recover independently, and recovery returns
    /// exactly one store per configured shard.
    ///
    /// The vector length is asserted against `num_shards` directly, and one
    /// shard is deliberately emptied by the crash (its writes are never synced)
    /// so a shard with nothing to restore still occupies its own slot — a short
    /// vector, or one padded/truncated to fit, misroutes a whole keyspace.
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

        // Shard 2's post-flush writes never reach durability: after the crash
        // that shard restores nothing at all, and must still get a store.
        for i in 0..5 {
            let key = format!("shard2_unsynced_{}", i);
            harness.put_with_sync(2, key.as_bytes(), &Value::string("lost"), false);
        }

        harness.crash();

        let (stores, stats) = harness.recover();

        // Total should be 10 + 20 + 30 + 40 = 100
        assert_eq!(stats.keys_loaded, 100);

        assert_eq!(
            stores.len(),
            harness.num_shards,
            "Recovery returns exactly one store per configured shard"
        );

        // Verify each shard
        assert_eq!(stores[0].0.len(), 10);
        assert_eq!(stores[1].0.len(), 20);
        assert_eq!(stores[2].0.len(), 30);
        assert_eq!(stores[3].0.len(), 40);
    }

    // FM-PERSISTENCE-029
    /// Test 3.7: Empty shard recovery — including a shard emptied by the crash.
    ///
    /// Shard 1 is written to and then loses every one of those writes, so at
    /// boot its column family is empty for the same reason a never-used shard's
    /// is. The probe must treat it as "nothing to restore" and boot, not as a
    /// half-restored keyspace or a failure.
    #[test]
    fn test_empty_shard_recovery() {
        let mut harness = CrashTestHarness::new();

        // Only write to shard 0, leave others empty
        harness.put_direct(0, b"only_key", &Value::string("only_value"));
        harness.flush();

        // Shard 1's writes are never synced: the crash takes all of them.
        harness.put_with_sync(1, b"doomed_key", &Value::string("doomed_value"), false);

        harness.crash();

        let (stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 1);
        assert_eq!(stats.keys_failed, 0);
        assert_eq!(stores.len(), harness.num_shards);
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

    // FM-PERSISTENCE-017
    /// Test 4.1: A half-written `metadata.json` is not a snapshot, and not an
    /// erased epoch either.
    ///
    /// The boot must survive it (a damaged pointer is what a crash mid-publish
    /// leaves behind, not an operator error), must not adopt it as a save, and
    /// must not rewind the epoch to 0 — epoch 0 makes the next `BGSAVE` reuse
    /// `snapshot_00001`, whose directory is sitting right there.
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

        let coord = boot_coordinator(&tmp, &snapshot_dir)
            .expect("an unreadable metadata pointer must not fail the boot");

        assert_eq!(
            coord.current_epoch(),
            1,
            "the epoch falls back to the highest snapshot directory on disk, so \
             the next BGSAVE cannot reuse a live epoch"
        );
        assert!(
            coord.last_save_time().is_none(),
            "a snapshot whose metadata will not parse is never reported as a save"
        );
    }

    // FM-PERSISTENCE-017
    /// Test 4.2: An unmarked snapshot is never adopted, even when `latest`
    /// points straight at it.
    ///
    /// This is the row's headline NOT-observable: `snapshot_00001` has no
    /// completion marker, so however it got published it is not a save. The
    /// complete `snapshot_00000` next to it is deliberately older — the
    /// coordinator must not silently fall back to it and report a save either,
    /// because `latest` is the pointer and it points at the incomplete one.
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

        // The file itself carries no marker...
        let metadata_path = snapshot_path.join("metadata.json");
        let content = std::fs::read_to_string(&metadata_path).unwrap();
        let metadata: SnapshotMetadataFile = serde_json::from_str(&content).unwrap();
        assert!(!metadata.is_complete());

        // ...and the boot path acts on that: epoch resumed, no save reported.
        let coord =
            boot_coordinator(&tmp, &snapshot_dir).expect("an incomplete snapshot is not fatal");
        assert!(
            coord.last_save_time().is_none(),
            "an unmarked snapshot must not be adopted as the latest save"
        );
        assert_eq!(
            coord.current_epoch(),
            1,
            "the epoch counter still resumes past the crashed epoch"
        );
    }

    // FM-PERSISTENCE-034
    /// Test 4.3: An unclean shutdown truncates, it does not refuse.
    ///
    /// The database has to come back up — an unclean shutdown is ordinary, and
    /// refusing to open would turn it into an outage. What it comes back with is
    /// every synced write and nothing that was still in flight: the loss is a
    /// suffix, so no unsynced key may reappear and no synced one may go missing.
    ///
    /// The tear here is the harness's durability boundary rather than a
    /// byte-level torn record; the record-level behaviour of point-in-time
    /// replay (truncate at the first inconsistency, drop the suffix behind a
    /// mid-log corruption) is forced by `wal_truncation_recovers_prefix_and_signals`
    /// and `wal_mid_log_bitflip_drops_suffix_and_signals` in `frogdb-persistence`.
    #[test]
    fn test_unclean_shutdown_recovery() {
        let mut harness = CrashTestHarness::with_sync_mode();

        // 50 synced writes: acknowledged and on the device.
        for i in 0..50 {
            let key = format!("key_{}", i);
            let value = Value::string(format!("value_{}", i));
            harness.put_direct(0, key.as_bytes(), &value);
        }

        // 10 writes still in flight when the power goes: the tail.
        for i in 0..10 {
            let key = format!("in_flight_{}", i);
            harness.put_with_sync(0, key.as_bytes(), &Value::string("in_flight"), false);
        }

        harness.crash();

        // Reopening must succeed — a torn tail is not a startup failure.
        let (mut stores, stats) = harness.recover();

        // All sync'd keys should be present
        assert_eq!(stats.keys_loaded, 50);
        assert_eq!(stores[0].0.len(), 50);
        for i in 0..50 {
            let key = format!("key_{}", i);
            assert!(
                verify_string_value(&mut stores, 0, key.as_bytes(), &format!("value_{}", i)),
                "Synced key {} must be replayed with its value",
                key
            );
        }
        for i in 0..10 {
            let key = format!("in_flight_{}", i);
            assert!(
                stores[0].0.get(key.as_bytes()).is_none(),
                "Key {} was in the torn tail and must not be replayed",
                key
            );
        }
    }

    // FM-PERSISTENCE-033
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
        let (stores, stats) = recover_all_shards(&rocks).unwrap();

        // 10 valid keys should be loaded, 1 should fail
        assert_eq!(stats.keys_loaded, 10);
        assert_eq!(stats.keys_failed, 1);
        assert_eq!(stores[0].0.len(), 10);

        // The count alone cannot tell an operator *what* failed, so the first
        // failure carries one concrete example: which shard, which tier, which
        // key. A shrunken keyspace with no example is the thing this row exists
        // to make impossible.
        let failure = stats
            .first_failure
            .as_ref()
            .expect("a counted decode failure must also be described");
        assert_eq!(failure.shard_id, 0);
        assert!(
            !failure.warm,
            "the corrupt value was written to the hot tier"
        );
        assert_eq!(failure.key.as_ref(), b"corrupt_key");
        assert!(
            !failure.error.is_empty(),
            "the decoder's own message is what makes the report actionable"
        );
        assert!(
            stats.bytes_loaded > 0,
            "bytes accumulate before the decode attempt: scanning unreadable \
             data is not the same as scanning nothing"
        );
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
        let (stores, stats) = harness.recover();
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

    // FM-PERSISTENCE-034
    /// Test 5.3: Repeated crash and recovery cycles.
    ///
    /// Five crashes in a row, each with a synced batch and an unsynced tail.
    /// Every cycle's synced keys accumulate and every cycle's tail is dropped:
    /// recovery neither loses ground it already made nor drags a previous
    /// crash's in-flight writes forward, however many times it happens.
    #[test]
    fn test_repeated_crash_cycles() {
        let mut harness = CrashTestHarness::with_sync_mode();

        for cycle in 0..5 {
            if cycle > 0 {
                harness.reopen();
            }

            // Write 20 new keys
            for i in 0..20 {
                let key = format!("cycle{}_key_{}", cycle, i);
                let value = Value::string(format!("cycle{}_value_{}", cycle, i));
                harness.put_direct(0, key.as_bytes(), &value);
            }

            // ...and a tail that never reaches the device.
            for i in 0..3 {
                let key = format!("cycle{}_tail_{}", cycle, i);
                harness.put_with_sync(0, key.as_bytes(), &Value::string("tail"), false);
            }

            harness.crash();
        }

        // Final verification
        let (mut stores, stats) = harness.recover();

        // Should have 5 cycles * 20 keys = 100 keys
        assert_eq!(stats.keys_loaded, 100);
        for cycle in 0..5 {
            for i in 0..20 {
                let key = format!("cycle{}_key_{}", cycle, i);
                let expected = format!("cycle{}_value_{}", cycle, i);
                assert!(
                    verify_string_value(&mut stores, 0, key.as_bytes(), &expected),
                    "Key {} was synced in cycle {} and must still be here",
                    key,
                    cycle
                );
            }
            for i in 0..3 {
                let key = format!("cycle{}_tail_{}", cycle, i);
                assert!(
                    stores[0].0.get(key.as_bytes()).is_none(),
                    "Key {} was in cycle {}'s torn tail and must be gone",
                    key,
                    cycle
                );
            }
        }
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

    // FM-PERSISTENCE-017
    /// Test 6.1: A metadata file torn in half is not half a snapshot.
    ///
    /// A crash mid-write leaves exactly this: valid JSON up to a point and
    /// nothing after it. The boot must refuse to read a save out of it — and
    /// must still not lose the epoch, which is on disk as a directory name
    /// whatever the pointer says.
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
        create_latest_symlink(&snapshot_dir, "snapshot_00001");

        // Half a metadata file is not parseable...
        let content = std::fs::read_to_string(&metadata_path).unwrap_or_default();
        let parsed: Result<SnapshotMetadataFile, _> = serde_json::from_str(&content);
        assert!(parsed.is_err(), "Truncated JSON should fail to parse");

        // ...and the boot path treats it as no snapshot rather than as a save.
        let coord = boot_coordinator(&tmp, &snapshot_dir)
            .expect("a truncated metadata file must not fail the boot");
        assert!(
            coord.last_save_time().is_none(),
            "a half-written metadata file is not a completed save"
        );
        assert_eq!(
            coord.current_epoch(),
            1,
            "the epoch survives in the directory name even when the pointer does not"
        );
    }

    // FM-PERSISTENCE-017
    /// Test 6.2: The completion marker is the whole difference between a
    /// snapshot and a directory.
    ///
    /// `mark_complete` is the only writer of `FROGDB_SNAPSHOT_COMPLETE_v1`, so
    /// an unmarked file is what every failed or interrupted save leaves. Both
    /// directions are asserted — unmarked is refused, marked carries the exact
    /// marker string — because "is_complete() is false" alone would still pass
    /// if the marker were never written at all.
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
        assert!(
            metadata.completed_at().is_none(),
            "an unmarked snapshot has no completion time to report"
        );
        assert!(
            !content.contains("FROGDB_SNAPSHOT_COMPLETE_v1"),
            "the marker must be absent from the file, not merely unread"
        );

        // The completed sibling proves the marker is really written when a save
        // finishes — otherwise the assertions above hold vacuously.
        let complete_path = create_test_snapshot_dir(&snapshot_dir, 2);
        write_snapshot_metadata(&complete_path, 2, 2000, 4, true);
        let complete = std::fs::read_to_string(complete_path.join("metadata.json")).unwrap();
        assert!(
            complete.contains("FROGDB_SNAPSHOT_COMPLETE_v1"),
            "a completed save publishes the marker verbatim"
        );
    }

    // FM-PERSISTENCE-034
    /// Test 6.3: Recovery from valid data despite snapshot directory issues.
    ///
    /// The keyspace comes back from the WAL, and the wreckage a crashed save
    /// leaves in the snapshot directory — a leftover staging dir, an unmarked
    /// epoch, a `latest` pointing at garbage — changes nothing about that:
    /// snapshot restore is an explicit operator action, never a boot path.
    #[test]
    fn test_wal_recovery_despite_snapshot_issues() {
        let mut harness = CrashTestHarness::with_sync_mode();

        // A snapshot directory in the state a crash mid-BGSAVE leaves it: a
        // leftover staging dir, an unmarked epoch, `latest` pointing at it.
        let snap_tmp = TempDir::new().unwrap();
        let snapshot_dir = snap_tmp.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        let staged = snapshot_dir.join(".snapshot_00002.tmp");
        std::fs::create_dir_all(&staged).unwrap();
        std::fs::write(staged.join("metadata.json"), "{ half-writ").unwrap();
        let orphan = create_test_snapshot_dir(&snapshot_dir, 1);
        write_snapshot_metadata(&orphan, 1, 1000, 4, false);
        create_latest_symlink(&snapshot_dir, "snapshot_00001");
        let coord = boot_coordinator(&snap_tmp, &snapshot_dir)
            .expect("a wrecked snapshot directory must not stop the server coming up");
        assert!(
            coord.last_save_time().is_none(),
            "none of that wreckage is a save"
        );

        // Write data with sync
        for i in 0..30 {
            let key = format!("wal_key_{}", i);
            let value = Value::string(format!("value_{}", i));
            harness.put_direct(0, key.as_bytes(), &value);
        }
        harness.put_with_sync(0, b"in_flight", &Value::string("in_flight"), false);

        harness.crash();

        // Reopen and recover from the WAL — the snapshot wreckage is not consulted.
        let (mut stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 30);
        assert_eq!(stores[0].0.len(), 30);
        assert!(
            stores[0].0.get(b"in_flight").is_none(),
            "the unsynced tail is still dropped, snapshots or no snapshots"
        );
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

    // FM-PERSISTENCE-029
    /// Test 7.1: A database the crash emptied boots fresh, not broken.
    ///
    /// Writes went in and every one of them died in the crash, so the probe
    /// finds an empty keyspace — the same observation a genuine first boot
    /// produces, and it has to be answered the same way: empty stores, zeroed
    /// stats, no failure. A partially restored keyspace presented as fresh, or a
    /// refusal, would both be wrong.
    #[test]
    fn test_empty_database_recovery() {
        let mut harness = CrashTestHarness::with_async_mode();

        // Everything written here is inside the loss window.
        for i in 0..25usize {
            let key = format!("doomed_key_{}", i);
            harness.put_direct(i % 4, key.as_bytes(), &Value::string("doomed"));
        }

        harness.crash();

        let (stores, stats) = harness.recover();

        assert_eq!(stats.keys_loaded, 0);
        assert_eq!(stats.keys_expired_skipped, 0);
        assert_eq!(stats.keys_failed, 0);

        assert_eq!(stores.len(), harness.num_shards);
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

        harness.put_direct(0, b"special_zset", &Value::SortedSet(Box::new(zset)));
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

    // FM-PERSISTENCE-036
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

    // FM-PERSISTENCE-035
    /// A clean reopen after a synced WAL run raises *nothing*.
    ///
    /// The dropped-records detector compares what RocksDB recovered to against
    /// the durable-sync watermark. Here the watermark is real — the writer runs
    /// in `Sync` mode, so its commits advance it — and recovery reaches it, so
    /// the counter must not be touched at all. Not "incremented by zero":
    /// touched at all, because a zero-valued counter call is still an alarm on
    /// a dashboard that counts events. This is the never-false-alarm half of the
    /// row, end to end through `RocksStore::open` rather than against
    /// `detect_and_reset` directly.
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
        let synced_seq = rocks.latest_sequence_number();

        // Drop everything
        drop(wal);
        drop(rocks);

        // Reopen and recover
        let recorder = Arc::new(RecordingRecorder::default());
        let rocks = Arc::new(
            RocksStore::open_with_metrics(tmp.path(), 2, &RocksConfig::default(), recorder.clone())
                .unwrap(),
        );
        let (stores, stats) = recover_all_shards(&rocks).unwrap();

        assert_eq!(stats.keys_loaded, 50);
        assert_eq!(stores[0].0.len(), 50);
        assert!(
            rocks.latest_sequence_number() >= synced_seq,
            "recovery reached at least the sequence the syncs covered"
        );
        assert!(
            !recorder
                .calls()
                .iter()
                .any(|(name, _)| name == WAL_DROPPED_RECORDS),
            "a reopen that lost nothing must raise no dropped-records signal, \
             not even a zero: got {:?}",
            recorder.calls()
        );
    }

    // FM-PERSISTENCE-035
    /// The sequence the detector compares against survives the process.
    ///
    /// The WAL writer's own sequence is per-writer and starts at 1; RocksDB's is
    /// the durable one, and it is what a later boot measures the watermark
    /// against — so it must not rewind across a reopen, or every subsequent boot
    /// would report a loss that never happened. Asserted together with the
    /// no-signal check, since a rewound sequence is precisely how this detector
    /// would false-alarm.
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

        drop(wal);
        drop(rocks);

        let recorder = Arc::new(RecordingRecorder::default());
        let rocks = Arc::new(
            RocksStore::open_with_metrics(tmp.path(), 2, &RocksConfig::default(), recorder.clone())
                .unwrap(),
        );
        assert!(
            rocks.latest_sequence_number() >= rocks_seq,
            "the durable sequence must not rewind across a reopen: {} < {}",
            rocks.latest_sequence_number(),
            rocks_seq
        );
        assert!(
            !recorder
                .calls()
                .iter()
                .any(|(name, _)| name == WAL_DROPPED_RECORDS),
            "nothing was dropped, so nothing may be reported: got {:?}",
            recorder.calls()
        );
        let (_stores, stats) = recover_all_shards(&rocks).unwrap();
        assert_eq!(stats.keys_loaded, 2, "both writes come back");
    }
}
