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
            ..Default::default()
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
