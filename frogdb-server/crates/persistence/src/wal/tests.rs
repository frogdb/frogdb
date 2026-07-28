//! Tests for WAL module.
use super::flush::{
    FlushEngine, FlushOutcomes, WalCommand, WalEntry, WalLagAtomics, WriteSink, flush_thread_loop,
};
use super::*;
use crate::rocks::RocksConfig;
use crate::serialization::serialize;
use bytes::Bytes;
use frogdb_types::traits::NoopMetricsRecorder;
use frogdb_types::types::{KeyMetadata, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[tokio::test]
async fn test_wal_write_and_flush() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 1024 * 1024,
            batch_timeout_ms: 1000,
            ..Default::default()
        },
        m,
    );
    let v = Value::string("test_value");
    let md = KeyMetadata::new(10);
    assert_eq!(wal.write_set(b"key1", &v, &md).await.unwrap(), 1);
    wal.flush_async().await.unwrap();
    assert!(rocks.get(0, b"key1").unwrap().is_some());
}
#[tokio::test]
async fn test_wal_delete() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let v = Value::string("test");
    let md = KeyMetadata::new(4);
    rocks.put(0, b"key", &serialize(&v, &md)).unwrap();
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(rocks.clone(), 0, WalConfig::default(), m);
    assert_eq!(wal.write_delete(b"key").await.unwrap(), 1);
    wal.flush_async().await.unwrap();
    assert!(rocks.get(0, b"key").unwrap().is_none());
}
#[tokio::test]
async fn test_wal_batch_threshold() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 100,
            batch_timeout_ms: 60000,
            ..Default::default()
        },
        m,
    );
    wal.write_set(
        b"bigkey",
        &Value::string("x".repeat(200)),
        &KeyMetadata::new(200),
    )
    .await
    .unwrap();
    wal.flush_async().await.unwrap();
    assert!(rocks.get(0, b"bigkey").unwrap().is_some());
}
#[tokio::test]
async fn test_wal_sequence() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(rocks, 0, WalConfig::default(), m);
    assert_eq!(wal.sequence(), 0);
    let v = Value::string("v");
    let md = KeyMetadata::new(1);
    assert_eq!(wal.write_set(b"k1", &v, &md).await.unwrap(), 1);
    assert_eq!(wal.write_set(b"k2", &v, &md).await.unwrap(), 2);
    assert_eq!(wal.write_delete(b"k1").await.unwrap(), 3);
    assert_eq!(wal.sequence(), 3);
}
#[tokio::test]
async fn test_wal_drop_flushes_pending() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    {
        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 60000,
                ..Default::default()
            },
            m,
        );
        wal.write_set(
            b"dropkey",
            &Value::string("drop_test"),
            &KeyMetadata::new(9),
        )
        .await
        .unwrap();
    }
    assert!(rocks.get(0, b"dropkey").unwrap().is_some());
}
#[tokio::test]
async fn test_wal_backpressure_no_data_loss() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 1024 * 1024,
            batch_timeout_ms: 60000,
            channel_capacity: 1,
        },
        m,
    );
    let v = Value::string("bp");
    let md = KeyMetadata::new(2);
    for i in 0..50 {
        wal.write_set(format!("bpkey{i}").as_bytes(), &v, &md)
            .await
            .unwrap();
    }
    wal.flush_async().await.unwrap();
    for i in 0..50 {
        assert!(
            rocks
                .get(0, format!("bpkey{i}").as_bytes())
                .unwrap()
                .is_some()
        );
    }
}
// ============================================================================
// FlushEngine / WriteSink tests — no RocksDB required.
//
// These drive the real flush thread loop against an in-memory sink with
// failure injection, pinning the durability-sink contract: every flush
// outcome (explicit, size-threshold, timeout, shutdown-drain) is recorded
// and surfaced; an acked write can never outrun a swallowed flush failure.
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum TestOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Merge(Vec<u8>, Vec<u8>),
    Clear,
}

/// In-memory [`WriteSink`] with commit/stage failure injection.
struct TestSink {
    staged: Vec<TestOp>,
    committed: Arc<Mutex<Vec<Vec<TestOp>>>>,
    /// Number of upcoming commits that fail.
    fail_commits: Arc<AtomicUsize>,
    /// Number of upcoming stage calls that fail.
    fail_stages: Arc<AtomicUsize>,
}

impl WriteSink for TestSink {
    fn stage_put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        if take_one(&self.fail_stages) {
            return Err(std::io::Error::other("injected stage failure"));
        }
        self.staged.push(TestOp::Put(key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn stage_delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        if take_one(&self.fail_stages) {
            return Err(std::io::Error::other("injected stage failure"));
        }
        self.staged.push(TestOp::Delete(key.to_vec()));
        Ok(())
    }

    fn stage_merge(&mut self, key: &[u8], operand: &[u8]) -> std::io::Result<()> {
        if take_one(&self.fail_stages) {
            return Err(std::io::Error::other("injected stage failure"));
        }
        self.staged
            .push(TestOp::Merge(key.to_vec(), operand.to_vec()));
        Ok(())
    }

    fn stage_clear(&mut self) -> std::io::Result<()> {
        if take_one(&self.fail_stages) {
            return Err(std::io::Error::other("injected stage failure"));
        }
        self.staged.push(TestOp::Clear);
        Ok(())
    }

    fn commit(&mut self, _sync: bool) -> std::io::Result<()> {
        let batch = std::mem::take(&mut self.staged);
        if take_one(&self.fail_commits) {
            return Err(std::io::Error::other("injected commit failure"));
        }
        self.committed.lock().unwrap().push(batch);
        Ok(())
    }

    fn staged_len(&self) -> usize {
        self.staged.len()
    }
}

fn take_one(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

/// A running flush thread over a [`TestSink`].
struct TestWal {
    tx: Option<flume::Sender<WalCommand>>,
    outcomes: Arc<FlushOutcomes>,
    lag: Arc<WalLagAtomics>,
    committed: Arc<Mutex<Vec<Vec<TestOp>>>>,
    fail_commits: Arc<AtomicUsize>,
    fail_stages: Arc<AtomicUsize>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl TestWal {
    fn spawn(batch_size_threshold: usize, batch_timeout: Duration) -> Self {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let fail_commits = Arc::new(AtomicUsize::new(0));
        let fail_stages = Arc::new(AtomicUsize::new(0));
        let sink = TestSink {
            staged: Vec::new(),
            committed: Arc::clone(&committed),
            fail_commits: Arc::clone(&fail_commits),
            fail_stages: Arc::clone(&fail_stages),
        };
        let lag = Arc::new(WalLagAtomics {
            pending_ops: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(0),
        });
        let outcomes = Arc::new(FlushOutcomes::new());
        let engine = FlushEngine::new(
            sink,
            0,
            &DurabilityMode::Async,
            Arc::clone(&lag),
            Arc::clone(&outcomes),
            Arc::new(NoopMetricsRecorder::new()),
        );
        let (tx, rx) = flume::bounded(64);
        let handle = std::thread::spawn(move || {
            flush_thread_loop(rx, engine, batch_size_threshold, batch_timeout);
        });
        Self {
            tx: Some(tx),
            outcomes,
            lag,
            committed,
            fail_commits,
            fail_stages,
            handle: Some(handle),
        }
    }

    fn put(&self, seq: u64, key: &[u8], size_estimate: usize) {
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Write(WalEntry::Put {
                seq,
                key: Bytes::copy_from_slice(key),
                value: b"v".to_vec(),
                size_estimate,
            }))
            .unwrap();
    }

    fn delete(&self, seq: u64, key: &[u8], size_estimate: usize) {
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Write(WalEntry::Delete {
                seq,
                key: Bytes::copy_from_slice(key),
                size_estimate,
            }))
            .unwrap();
    }

    fn merge(&self, seq: u64, key: &[u8], operand: &[u8], size_estimate: usize) {
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Write(WalEntry::Merge {
                seq,
                key: Bytes::copy_from_slice(key),
                operand: operand.to_vec(),
                size_estimate,
            }))
            .unwrap();
    }

    fn clear(&self, seq: u64, size_estimate: usize) {
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Write(WalEntry::Clear { seq, size_estimate }))
            .unwrap();
    }

    /// Explicit flush: this flush attempt's result only (writer's `flush_async`).
    fn flush(&self) -> std::io::Result<()> {
        let (done_tx, done_rx) = flume::bounded(1);
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Flush { done_tx })
            .unwrap();
        done_rx.recv().unwrap()
    }

    /// Durable-through confirmation (writer's `flush_through`).
    fn flush_through(&self, after_seq: u64, target_seq: u64) -> std::io::Result<()> {
        let result = self.flush();
        self.outcomes
            .confirm_durable_through(after_seq, target_seq, result)
    }

    fn committed_batches(&self) -> Vec<Vec<TestOp>> {
        self.committed.lock().unwrap().clone()
    }

    /// Disconnect the channel and join the flush thread (shutdown drain).
    fn shutdown(&mut self) {
        drop(self.tx.take());
        if let Some(h) = self.handle.take() {
            h.join().unwrap();
        }
    }

    fn wait_until(&self, what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if cond() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("timed out waiting for: {what}");
    }
}

impl Drop for TestWal {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[test]
fn test_sink_happy_path_batches_and_advances_durable_sequence() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.put(1, b"a", 10);
    wal.put(2, b"b", 10);
    wal.delete(3, b"a", 5);
    wal.flush_through(0, 3).unwrap();

    let batches = wal.committed_batches();
    assert_eq!(batches.len(), 1, "one explicit flush, one batch");
    assert_eq!(
        batches[0],
        vec![
            TestOp::Put(b"a".to_vec(), b"v".to_vec()),
            TestOp::Put(b"b".to_vec(), b"v".to_vec()),
            TestOp::Delete(b"a".to_vec()),
        ],
        "entries commit in order"
    );
    assert_eq!(wal.outcomes.durable_sequence(), 3);
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.flush_failures(), 0);
    assert_eq!(wal.outcomes.lost_ops(), 0);
    assert_eq!(wal.lag.pending_ops.load(Ordering::Acquire), 0);
    assert_eq!(wal.lag.pending_bytes.load(Ordering::Acquire), 0);
    wal.shutdown();
}

#[test]
fn test_sink_stages_merge_operand_in_order() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.put(1, b"h", 10);
    wal.merge(2, b"h", b"delta-op", 8);
    wal.flush_through(0, 2).unwrap();

    let batches = wal.committed_batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0],
        vec![
            TestOp::Put(b"h".to_vec(), b"v".to_vec()),
            TestOp::Merge(b"h".to_vec(), b"delta-op".to_vec()),
        ],
        "merge operand staged after the put, carrying its bytes"
    );
    assert_eq!(wal.outcomes.durable_sequence(), 2);
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.lag.pending_ops.load(Ordering::Acquire), 0);
    wal.shutdown();
}

/// A `Clear` entry is a flush barrier: it drains lower-seq entries into their
/// own committed batch, commits itself as a second batch, and lets higher-seq
/// entries land in a third — so a clear can neither drop a post-clear write nor
/// spare a pre-clear one. (proposal 43)
#[test]
fn test_sink_clear_is_a_flush_barrier_between_batches() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.put(1, b"a", 10);
    wal.put(2, b"b", 10);
    wal.clear(3, 64);
    wal.put(4, b"c", 10);
    wal.flush_through(0, 4).unwrap();

    let batches = wal.committed_batches();
    assert_eq!(
        batches,
        vec![
            vec![
                TestOp::Put(b"a".to_vec(), b"v".to_vec()),
                TestOp::Put(b"b".to_vec(), b"v".to_vec()),
            ],
            vec![TestOp::Clear],
            vec![TestOp::Put(b"c".to_vec(), b"v".to_vec())],
        ],
        "pre-clear puts flush first, the clear commits alone, post-clear puts follow"
    );
    assert_eq!(wal.outcomes.durable_sequence(), 4);
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.flush_failures(), 0);
    assert_eq!(wal.lag.pending_ops.load(Ordering::Acquire), 0);
    assert_eq!(wal.lag.pending_bytes.load(Ordering::Acquire), 0);
    wal.shutdown();
}

/// A standalone clear (no surrounding writes) advances the durable sequence
/// past itself, so a durable-through confirmation succeeds. (The RocksDB
/// empty-CF path — where the range delete stages nothing — is exercised by the
/// server integration tests.)
#[test]
fn test_sink_clear_advances_durable_sequence() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.clear(1, 64);
    wal.flush_through(0, 1).unwrap();
    assert_eq!(wal.outcomes.durable_sequence(), 1);
    assert!(wal.outcomes.last_flush_ok());
    wal.shutdown();
}

#[test]
fn test_sink_size_threshold_triggers_commit_without_explicit_flush() {
    let mut wal = TestWal::spawn(100, Duration::from_secs(60));
    wal.put(1, b"big", 200);
    wal.wait_until("size-threshold flush to commit", || {
        !wal.committed_batches().is_empty()
    });
    assert_eq!(wal.outcomes.durable_sequence(), 1);
    wal.shutdown();
}

/// Regression pin: an acked write cannot outrun a swallowed flush failure.
///
/// A size-threshold background flush fails and drops the batch; the follow-up
/// explicit flush finds an empty buffer and reports `Ok` — exactly the shape
/// that used to let rollback-mode `persist_and_confirm` ack a lost write. The
/// durable-through confirmation must fail.
#[test]
fn test_sink_size_threshold_flush_error_surfaces_on_confirm() {
    let mut wal = TestWal::spawn(100, Duration::from_secs(60));
    wal.fail_commits.store(1, Ordering::SeqCst);

    // Exceeds the threshold: background flush fires and fails.
    wal.put(1, b"big", 200);
    wal.wait_until("background flush failure to be recorded", || {
        wal.outcomes.flush_failures() == 1
    });

    // The buffer is now empty, so the explicit flush itself succeeds…
    let err = wal.flush_through(0, 1).unwrap_err();
    assert!(
        err.to_string().contains("injected commit failure"),
        "confirmation must carry the swallowed failure: {err}"
    );
    assert_eq!(wal.outcomes.lost_ops(), 1);
    assert_eq!(wal.outcomes.lost_bytes(), 200);
    assert!(!wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.durable_sequence(), 0);
    wal.shutdown();
}

#[test]
fn test_sink_timeout_flush_error_surfaces_on_confirm() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_millis(20));
    wal.fail_commits.store(1, Ordering::SeqCst);

    // Below the size threshold: only the timeout can flush it.
    wal.put(1, b"k", 10);
    wal.wait_until("timeout flush failure to be recorded", || {
        wal.outcomes.flush_failures() == 1
    });

    let err = wal.flush_through(0, 1).unwrap_err();
    assert!(err.to_string().contains("injected commit failure"), "{err}");
    assert_eq!(wal.outcomes.lost_ops(), 1);
    wal.shutdown();
}

#[test]
fn test_sink_disconnect_drain_error_recorded() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.fail_commits.store(1, Ordering::SeqCst);
    wal.put(1, b"k", 10);
    // Disconnect: the drain flush fails; the outcome must still be recorded.
    wal.shutdown();
    assert_eq!(wal.outcomes.flush_failures(), 1);
    assert_eq!(wal.outcomes.lost_ops(), 1);
    assert!(!wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.durable_sequence(), 0);
}

/// Failure attribution is precise: a failure that only covered *earlier*
/// sequences does not fail a later command's confirmation, while a
/// confirmation spanning the failed range does fail — and recovery is honest
/// (`last_flush_ok` returns to true, `lost_ops` never un-counts).
#[test]
fn test_sink_failure_attribution_and_recovery() {
    let mut wal = TestWal::spawn(100, Duration::from_secs(60));
    wal.fail_commits.store(1, Ordering::SeqCst);

    // Command A (seq 1): background size flush fails, batch dropped.
    wal.put(1, b"a", 200);
    wal.wait_until("failure to be recorded", || {
        wal.outcomes.flush_failures() == 1
    });

    // Command B (seq 2): commits fine now.
    wal.put(2, b"b", 10);

    // B's confirmation (entries after seq 1) succeeds: the failure covered
    // only sequences <= 1.
    wal.flush_through(1, 2)
        .expect("unrelated earlier failure must not fail B's confirmation");

    // A's confirmation (entries after seq 0) fails: seq 1 was lost.
    let err = wal.flush_through(0, 2).unwrap_err();
    assert!(err.to_string().contains("injected commit failure"), "{err}");

    // Recovery is truthfully reported: the last flush attempt succeeded and
    // the durable sequence advanced, but the loss remains counted forever.
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.durable_sequence(), 2);
    assert_eq!(wal.outcomes.flush_failures(), 1);
    assert_eq!(wal.outcomes.lost_ops(), 1);
    wal.shutdown();
}

#[test]
fn test_sink_stage_failure_recorded_and_surfaced() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.fail_stages.store(1, Ordering::SeqCst);

    wal.put(1, b"k", 10); // staging fails; entry dropped
    wal.put(2, b"k2", 10);
    let err = wal.flush_through(0, 2).unwrap_err();
    assert!(err.to_string().contains("injected stage failure"), "{err}");
    assert_eq!(wal.outcomes.lost_ops(), 1);

    // The second entry still committed.
    assert_eq!(
        wal.committed_batches(),
        vec![vec![TestOp::Put(b"k2".to_vec(), b"v".to_vec())]]
    );
    wal.shutdown();
}

#[tokio::test]
async fn test_wal_lag_stats() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(rocks, 0, WalConfig::default(), m);
    let s = wal.lag_stats();
    assert_eq!(s.shard_id, 0);
    assert_eq!(s.sequence, 0);
    assert_eq!(s.durable_sequence, 0);
    assert_eq!(s.flush_failures, 0);
    assert_eq!(s.lost_ops, 0);
    assert!(s.last_flush_ok);

    let v = Value::string("v");
    let md = KeyMetadata::new(1);
    wal.write_set(b"k1", &v, &md).await.unwrap();
    wal.write_set(b"k2", &v, &md).await.unwrap();
    wal.flush_async().await.unwrap();
    let s = wal.lag_stats();
    assert_eq!(s.sequence, 2);
    assert_eq!(s.durable_sequence, 2, "durable high-water tracks the flush");
    assert!(s.last_flush_ok);
    assert_eq!(s.lost_ops, 0);
}

/// Proposal 48 end-to-end: a FLUSHDB clear through the real WAL pipeline
/// (write_clear → flush barrier → tombstone commit) triggers the asynchronous
/// post-clear reclamation, post-clear writes survive it, and nothing is
/// resurrected across a restart.
#[tokio::test]
async fn test_wal_clear_reclamation_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let md = KeyMetadata::new(10);
    {
        let rocks = Arc::new(
            crate::rocks::RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap(),
        );
        let m = Arc::new(NoopMetricsRecorder::new());
        let wal = RocksWalWriter::new(rocks.clone(), 0, WalConfig::default(), m);

        let v = Value::string("pre-clear");
        for i in 0..100u32 {
            wal.write_set(format!("old{i:03}").as_bytes(), &v, &md)
                .await
                .unwrap();
        }
        wal.flush_async().await.unwrap();
        // Push the pre-clear data into SSTs so reclamation has files to drop.
        rocks.flush().unwrap();

        wal.write_clear().await.unwrap();
        // Writes accepted immediately after the clear (higher seq) must land
        // after the tombstone and survive the compaction that follows.
        let nv = Value::string("post-clear");
        for i in 0..10u32 {
            wal.write_set(format!("new{i:03}").as_bytes(), &nv, &md)
                .await
                .unwrap();
        }
        wal.flush_async().await.unwrap();

        // The tombstone commit spawned the reclamation thread; wait it out.
        let deadline = Instant::now() + Duration::from_secs(60);
        while rocks.reclaim_guard.in_flight_count() > 0 {
            assert!(Instant::now() < deadline, "reclamation did not finish");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(rocks.get(0, b"old000").unwrap().is_none());
        assert!(rocks.get(0, b"new009").unwrap().is_some());
        assert_eq!(rocks.iter_cf(0).unwrap().count(), 10);
    }
    // Restart: compaction must not have resurrected cleared keys.
    let rocks = crate::rocks::RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();
    assert_eq!(rocks.iter_cf(0).unwrap().count(), 10);
    assert!(rocks.get(0, b"old000").unwrap().is_none());
}

// ============================================================================
// Fsync-boundary (durability-mode) crash tests — issue 12.
//
// The prior per-mode "crash" tests (crash_recovery_tests.rs) dropped the
// `RocksStore` handle and reopened the *same* directory. That preserves the OS
// page cache: unsynced-but-acked WAL bytes were flushed by the kernel anyway,
// so `sync`/`periodic`/`async` were indistinguishable and their per-mode
// assertions collapsed to `keys_loaded >= 1`. Nothing exercised the fsync
// boundary the durability modes exist to control.
//
// These tests sever that boundary at the `WriteSink::commit(sync)` seam — the
// exact point the durability mode decides whether to fsync (`FlushEngine`'s
// `is_sync` -> `WriteOptions::set_sync`). A `PageCacheSink` models two storage
// tiers behind that seam:
//
//   * a **durable** log that survives a crash (data that has been fsynced), and
//   * a volatile **page cache** log that a crash discards (committed but not
//     yet fsynced).
//
// `commit(sync = true)` promotes the whole page cache — this batch and every
// earlier unsynced batch — into the durable tier, exactly as a single fsync of
// the shared WAL file makes all preceding writes durable. `commit(sync = false)`
// only appends to the page cache. An external `fsync()` models the out-of-band
// durability sources a mode may still have: the `periodic` mode's
// `spawn_periodic_sync` task calling `rocks.flush()`, or RocksDB flushing a
// memtable on its own. `crash()` clears the page cache, modelling a power / OS
// loss that drops precisely the unfsynced-but-acked writes.
//
// Driving the *real* flush-thread loop over this sink turns the previously
// vacuous per-mode claims into distinct, asserted loss windows:
//   * sync    — zero acked-write loss,
//   * periodic — loss bounded by the last periodic fsync (one flush interval),
//   * async   — unbounded until an out-of-band fsync lands.
// ============================================================================

/// One durability-log entry: a key set to a value, or deleted (tombstone).
#[derive(Debug, Clone, PartialEq)]
enum LogOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Storage state behind the fsync seam, shared between the flush thread (which
/// commits batches) and the test thread (which fsyncs / crashes / recovers).
#[derive(Default)]
struct PageCacheState {
    /// Fsynced data — survives a crash. Applied in commit order.
    durable: Vec<LogOp>,
    /// Committed-but-unsynced data — a crash discards it (the OS page cache).
    page_cache: Vec<LogOp>,
    /// The `sync` flag seen by each `commit`, in order. Lets a test pin the
    /// `DurabilityMode` -> `is_sync` -> `set_sync` wiring behaviourally.
    commit_syncs: Vec<bool>,
}

impl PageCacheState {
    /// fsync the WAL file: every unsynced byte becomes durable. Models both the
    /// `periodic` sync task's `rocks.flush()` and any RocksDB-internal flush.
    fn fsync(&mut self) {
        self.durable.append(&mut self.page_cache);
    }

    /// Power loss: the volatile page cache evaporates; durable data remains.
    fn crash(&mut self) {
        self.page_cache.clear();
    }

    /// Fold the durable log into the surviving key/value set.
    fn recovered(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut map = BTreeMap::new();
        for op in &self.durable {
            match op {
                LogOp::Put(k, v) => {
                    map.insert(k.clone(), v.clone());
                }
                LogOp::Delete(k) => {
                    map.remove(k);
                }
            }
        }
        map
    }
}

/// [`WriteSink`] whose commits land in a shared [`PageCacheState`]. `sync=true`
/// commits fsync (promote page cache to durable); `sync=false` commits stay in
/// the page cache until an external fsync or are lost on crash.
struct PageCacheSink {
    staged: Vec<LogOp>,
    state: Arc<Mutex<PageCacheState>>,
}

impl WriteSink for PageCacheSink {
    fn stage_put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.staged.push(LogOp::Put(key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn stage_delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.staged.push(LogOp::Delete(key.to_vec()));
        Ok(())
    }

    fn stage_merge(&mut self, _key: &[u8], _operand: &[u8]) -> std::io::Result<()> {
        unimplemented!("fsync-boundary tests exercise Put/Delete only")
    }

    fn stage_clear(&mut self) -> std::io::Result<()> {
        unimplemented!("fsync-boundary tests exercise Put/Delete only")
    }

    fn commit(&mut self, sync: bool) -> std::io::Result<()> {
        let batch = std::mem::take(&mut self.staged);
        let mut st = self.state.lock().unwrap();
        st.commit_syncs.push(sync);
        st.page_cache.extend(batch);
        if sync {
            // fsync semantics: this batch and every earlier unsynced write are
            // now durable.
            st.fsync();
        }
        Ok(())
    }

    fn staged_len(&self) -> usize {
        self.staged.len()
    }
}

/// A running flush thread over a [`PageCacheSink`], parameterised by mode.
struct PageCacheWal {
    tx: Option<flume::Sender<WalCommand>>,
    outcomes: Arc<FlushOutcomes>,
    state: Arc<Mutex<PageCacheState>>,
    handle: Option<std::thread::JoinHandle<()>>,
    last_seq: u64,
}

impl PageCacheWal {
    fn spawn(mode: DurabilityMode) -> Self {
        let state = Arc::new(Mutex::new(PageCacheState::default()));
        let sink = PageCacheSink {
            staged: Vec::new(),
            state: Arc::clone(&state),
        };
        let lag = Arc::new(WalLagAtomics {
            pending_ops: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(0),
        });
        let outcomes = Arc::new(FlushOutcomes::new());
        let engine = FlushEngine::new(
            sink,
            0,
            &mode,
            Arc::clone(&lag),
            Arc::clone(&outcomes),
            Arc::new(NoopMetricsRecorder::new()),
        );
        // Large threshold + long timeout: batches commit only on the explicit
        // flushes these tests issue, so the fsync boundary is deterministic.
        let (tx, rx) = flume::bounded(256);
        let handle = std::thread::spawn(move || {
            flush_thread_loop(rx, engine, 1024 * 1024, Duration::from_secs(60));
        });
        Self {
            tx: Some(tx),
            outcomes,
            state,
            handle: Some(handle),
            last_seq: 0,
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> u64 {
        self.last_seq += 1;
        let seq = self.last_seq;
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Write(WalEntry::Put {
                seq,
                key: Bytes::copy_from_slice(key),
                value: value.to_vec(),
                size_estimate: key.len() + value.len(),
            }))
            .unwrap();
        seq
    }

    /// Explicit flush: drain the staged batch through `commit`. In `sync` mode
    /// this fsyncs; in `periodic`/`async` it only reaches the page cache.
    fn flush(&self) {
        let (done_tx, done_rx) = flume::bounded(1);
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::Flush { done_tx })
            .unwrap();
        done_rx.recv().unwrap().unwrap();
    }

    /// Out-of-band fsync — the `periodic` sync task's `rocks.flush()`, or a
    /// RocksDB-internal memtable flush. Promotes the page cache to durable.
    fn periodic_fsync(&self) {
        self.state.lock().unwrap().fsync();
    }

    /// Crash after every sent write has reached the commit seam, then recover.
    ///
    /// We first wait until the flush thread has committed everything sent (so
    /// no entry is still staged), then stop it — its disconnect drain now sees
    /// an empty buffer and cannot "clean shutdown"-persist anything. Only then
    /// is the page cache severed, so the crash drops exactly the acked writes
    /// that were committed unsynced.
    fn crash_and_recover(mut self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let target = self.last_seq;
        spin_until("all sent writes committed to the sink", || {
            self.outcomes.durable_sequence() >= target
        });
        drop(self.tx.take());
        self.handle.take().unwrap().join().unwrap();
        let mut st = self.state.lock().unwrap();
        st.crash();
        st.recovered()
    }

    fn commit_syncs(&self) -> Vec<bool> {
        self.state.lock().unwrap().commit_syncs.clone()
    }
}

fn spin_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("timed out waiting for: {what}");
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

/// The seam wiring itself: `Sync` mode fsyncs on every commit, `Periodic` and
/// `Async` never do at the commit seam. This behaviourally pins
/// `FlushEngine::is_sync` -> `WriteOptions::set_sync` — previously only
/// statically verified, never asserted at the durability boundary.
#[test]
fn test_fsync_seam_sync_flag_matches_mode() {
    for (mode, expect_sync) in [
        (DurabilityMode::Sync, true),
        (DurabilityMode::Periodic { interval_ms: 1000 }, false),
        (DurabilityMode::Async, false),
    ] {
        let mut wal = PageCacheWal::spawn(mode.clone());
        wal.put(b"a", b"1");
        wal.put(b"b", b"2");
        wal.flush();
        let syncs = wal.commit_syncs();
        assert!(!syncs.is_empty(), "{mode:?}: a commit must have happened");
        assert!(
            syncs.iter().all(|&s| s == expect_sync),
            "{mode:?}: every commit sync flag must be {expect_sync}, got {syncs:?}"
        );
    }
}

/// Sync mode: **zero acked-write loss**. Every write is fsynced at commit, so a
/// page-cache-severing crash cannot drop any acknowledged write.
#[test]
fn test_sync_mode_zero_acked_write_loss() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Sync);
    let n = 200u32;
    for i in 0..n {
        wal.put(&key(i), format!("v{i}").as_bytes());
        // In sync mode a write is acked only once its commit fsyncs; flush after
        // each write models that per-write durability ack.
        wal.flush();
    }
    // The page cache is always empty in sync mode: every commit fsynced.
    assert!(
        wal.state.lock().unwrap().page_cache.is_empty(),
        "sync mode must leave nothing unsynced"
    );

    let recovered = wal.crash_and_recover();
    assert_eq!(
        recovered.len(),
        n as usize,
        "every acked write must survive the crash in sync mode"
    );
    for i in 0..n {
        assert_eq!(
            recovered.get(&key(i)).map(|v| v.as_slice()),
            Some(format!("v{i}").as_bytes()),
            "sync-mode key {i} lost or wrong after crash"
        );
    }
}

/// Periodic mode: the loss window is **bounded by the last periodic fsync**.
/// Writes committed before a periodic sync survive a crash; only writes since
/// that sync (the current flush interval) are at risk — never an unbounded
/// suffix reaching back before the sync.
#[test]
fn test_periodic_mode_loss_bounded_by_flush_interval() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Periodic { interval_ms: 1000 });

    // Interval 1: writes that a periodic sync makes durable ("before the last
    // tick").
    let durable_keys = 100u32;
    for i in 0..durable_keys {
        wal.put(&key(i), format!("v{i}").as_bytes());
    }
    wal.flush(); // commit to page cache (sync=false)...
    wal.periodic_fsync(); // ...then the periodic sync task fsyncs them.

    // Interval 2: writes accepted after the last tick, still in the page cache
    // when the crash hits ("within the window").
    let window_start = durable_keys;
    let window_end = durable_keys + 50;
    for i in window_start..window_end {
        wal.put(&key(i), format!("v{i}").as_bytes());
    }
    wal.flush(); // committed unsynced; NOT fsynced (interval not elapsed).

    let recovered = wal.crash_and_recover();

    // Bound, lower edge: every write that predates the periodic sync survived —
    // the loss window does not extend back past the last fsync.
    for i in 0..durable_keys {
        assert_eq!(
            recovered.get(&key(i)).map(|v| v.as_slice()),
            Some(format!("v{i}").as_bytes()),
            "periodic-mode key {i} predates the last fsync and must survive"
        );
    }
    // Bound, upper edge: the within-interval writes were the only ones lost.
    for i in window_start..window_end {
        assert!(
            !recovered.contains_key(&key(i)),
            "periodic-mode key {i} was written after the last fsync; a crash must drop it"
        );
    }
    assert_eq!(
        recovered.len(),
        durable_keys as usize,
        "exactly the pre-fsync writes survive — loss is bounded to one interval"
    );
}

/// Async mode, worst case: with no out-of-band fsync, the entire window of
/// acked-but-unsynced writes is lost. Nothing at the commit seam bounds it.
#[test]
fn test_async_mode_unbounded_loss_without_fsync() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Async);
    let n = 200u32;
    for i in 0..n {
        wal.put(&key(i), format!("v{i}").as_bytes());
    }
    wal.flush(); // committed to the page cache, never fsynced.

    let recovered = wal.crash_and_recover();
    assert!(
        recovered.is_empty(),
        "async mode fsyncs nothing on its own; every acked write is at risk, \
         yet {} survived",
        recovered.len()
    );
}

/// Async mode window is bounded only by the *next out-of-band fsync* (RocksDB
/// memtable flush, or a manual sync), and it can reach arbitrarily far back:
/// data fsynced by such a flush survives, everything written since is lost with
/// no interval bound.
#[test]
fn test_async_mode_window_bounded_only_by_external_fsync() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Async);

    // A batch that a background/manual fsync happens to catch.
    let synced = 40u32;
    for i in 0..synced {
        wal.put(&key(i), format!("v{i}").as_bytes());
    }
    wal.flush();
    wal.periodic_fsync(); // e.g. a RocksDB memtable flush fsyncs the WAL.

    // A long tail written afterwards with no further fsync — unbounded window.
    let tail = 300u32;
    for i in synced..(synced + tail) {
        wal.put(&key(i), format!("v{i}").as_bytes());
    }
    wal.flush();

    let recovered = wal.crash_and_recover();
    for i in 0..synced {
        assert!(
            recovered.contains_key(&key(i)),
            "async-mode key {i} was fsynced before the crash and must survive"
        );
    }
    assert_eq!(
        recovered.len(),
        synced as usize,
        "only the externally-fsynced prefix survives; the unbounded tail is lost"
    );
}
