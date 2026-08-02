//! Tests for WAL module.
use super::flush::{
    FlushEngine, FlushOutcomes, RocksSink, WalCommand, WalEntry, WalLagAtomics, WriteSink,
    current_timestamp_ms, flush_thread_loop,
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
// FM-PERSISTENCE-013
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
// FM-PERSISTENCE-013
/// Propagation truth: storing a new batch-size threshold on the live handle
/// retunes the flush thread's batching decision WITHOUT restarting it. A huge
/// initial threshold (and far-off timeout) leaves the first write staged; after
/// `set_batch_size_threshold` lowers it, the next write's batch decision
/// re-reads the atomic and flushes both entries.
#[tokio::test]
async fn test_wal_batch_threshold_live_retune() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            // Huge threshold + far-off timeout: nothing auto-flushes.
            batch_size_threshold: 100 * 1024 * 1024,
            batch_timeout_ms: 600_000,
            ..Default::default()
        },
        m,
    );
    let md = KeyMetadata::new(1);
    wal.write_set(b"k1", &Value::string("v1"), &md)
        .await
        .unwrap();
    // Not durable: staged well under the huge threshold, timeout is 10 minutes.
    // A brief settle window to let the flush thread process (and NOT flush).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rocks.get(0, b"k1").unwrap().is_none(),
        "entry should still be staged under the huge threshold"
    );
    assert_eq!(wal.batch_size_threshold(), 100 * 1024 * 1024);

    // Live retune: drop the threshold to 1 byte. No restart, no flush_async.
    wal.set_batch_size_threshold(1);
    assert_eq!(wal.batch_size_threshold(), 1);

    // The next write's batch decision re-reads the atomic and flushes the batch.
    wal.write_set(b"k2", &Value::string("v2"), &md)
        .await
        .unwrap();

    // Poll for durability of the FIRST (previously staged) entry, proving the
    // new threshold took effect mid-flight.
    let mut durable = false;
    for _ in 0..200 {
        if rocks.get(0, b"k1").unwrap().is_some() {
            durable = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        durable,
        "lowered batch-size threshold should have flushed the staged entry without restart"
    );
}
// FM-PERSISTENCE-035
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
// FM-PERSISTENCE-010
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
// FM-PERSISTENCE-013
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
        let batch_size_threshold = Arc::new(AtomicUsize::new(batch_size_threshold));
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

    /// Open a write group (writer's `begin_group`).
    fn begin_group(&self) {
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::GroupBegin)
            .unwrap();
    }

    /// Close the innermost write group (writer's `end_group`).
    fn end_group(&self) {
        self.tx
            .as_ref()
            .unwrap()
            .send(WalCommand::GroupEnd)
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

    /// Assert nothing commits for `window`.
    ///
    /// A negative assertion over a sleep, which is the safe direction: a loaded
    /// or slow machine oversleeps, giving the behaviour under test *more* time
    /// to appear, so the assertion only gets stronger — it cannot flake the way
    /// a positive deadline can.
    fn assert_no_commit_for(&self, what: &str, window: Duration) {
        std::thread::sleep(window);
        assert!(
            self.committed_batches().is_empty(),
            "expected no commit while {what}, got {:?}",
            self.committed_batches()
        );
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
fn test_sink_happy_path_batches_and_advances_committed_sequence() {
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
    assert_eq!(wal.outcomes.committed_sequence(), 3);
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.flush_failures(), 0);
    assert_eq!(wal.outcomes.lost_ops(), 0);
    assert_eq!(wal.lag.pending_ops.load(Ordering::Acquire), 0);
    assert_eq!(wal.lag.pending_bytes.load(Ordering::Acquire), 0);
    wal.shutdown();
}

// FM-PERSISTENCE-014
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
    assert_eq!(wal.outcomes.committed_sequence(), 2);
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.lag.pending_ops.load(Ordering::Acquire), 0);
    wal.shutdown();
}

// FM-PERSISTENCE-012
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
    assert_eq!(wal.outcomes.committed_sequence(), 4);
    assert!(wal.outcomes.last_flush_ok());
    assert_eq!(wal.outcomes.flush_failures(), 0);
    assert_eq!(wal.lag.pending_ops.load(Ordering::Acquire), 0);
    assert_eq!(wal.lag.pending_bytes.load(Ordering::Acquire), 0);
    wal.shutdown();
}

// FM-PERSISTENCE-012
/// A standalone clear (no surrounding writes) advances the durable sequence
/// past itself, so a durable-through confirmation succeeds. (The RocksDB
/// empty-CF path — where the range delete stages nothing — is exercised by the
/// server integration tests.)
#[test]
fn test_sink_clear_advances_committed_sequence() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.clear(1, 64);
    wal.flush_through(0, 1).unwrap();
    assert_eq!(wal.outcomes.committed_sequence(), 1);
    assert!(wal.outcomes.last_flush_ok());
    wal.shutdown();
}

/// The tear this whole mechanism exists to prevent, forced deterministically:
/// without a write group, a producer that stalls between two entries of the
/// *same* logical batch leaves a committed **prefix** in storage. A checkpoint
/// cut (BGSAVE) or a crash in that window observes half a transaction.
///
/// Under full-suite load the stall is just the shard task being descheduled;
/// here it is an explicit condition-wait for the timeout flush to land.
///
// FM-PERSISTENCE-001
#[test]
fn test_sink_ungrouped_entries_tear_across_batches() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_millis(20));
    wal.put(1, b"a", 10);
    // The producer stalls here — the flush thread's timeout fires underneath it.
    wal.wait_until("timeout flush of the partial batch", || {
        !wal.committed_batches().is_empty()
    });
    wal.put(2, b"b", 10);
    wal.put(3, b"c", 10);
    wal.flush_through(0, 3).unwrap();

    assert_eq!(
        wal.committed_batches(),
        vec![
            vec![TestOp::Put(b"a".to_vec(), b"v".to_vec())],
            vec![
                TestOp::Put(b"b".to_vec(), b"v".to_vec()),
                TestOp::Put(b"c".to_vec(), b"v".to_vec()),
            ],
        ],
        "ungrouped entries are cut by the batch timeout"
    );
    wal.shutdown();
}

/// The fix: the same stall inside a write group commits as one batch. The
/// timeout trigger is suppressed until the group closes, so no reader — a
/// checkpoint or a crash-recovery replay — can observe the prefix.
///
// FM-PERSISTENCE-001
#[test]
fn test_sink_write_group_survives_the_batch_timeout() {
    let timeout = Duration::from_millis(20);
    let mut wal = TestWal::spawn(1024 * 1024, timeout);
    wal.begin_group();
    wal.put(1, b"a", 10);
    // Stall well past several timeout periods: nothing may commit yet.
    wal.assert_no_commit_for("a write group is open", timeout * 5);
    wal.put(2, b"b", 10);
    wal.put(3, b"c", 10);
    wal.end_group();
    wal.flush_through(0, 3).unwrap();

    assert_eq!(
        wal.committed_batches(),
        vec![vec![
            TestOp::Put(b"a".to_vec(), b"v".to_vec()),
            TestOp::Put(b"b".to_vec(), b"v".to_vec()),
            TestOp::Put(b"c".to_vec(), b"v".to_vec()),
        ]],
        "the whole group commits as one batch"
    );
    assert_eq!(wal.outcomes.committed_sequence(), 3);
    wal.shutdown();
}

/// The size threshold is suppressed too: a group larger than the threshold
/// still commits atomically. Atomicity outranks batch sizing — a group cannot
/// be split without reintroducing the tear, so an oversized transaction buys
/// its atomicity with one oversized write batch.
///
// FM-PERSISTENCE-001
#[test]
fn test_sink_write_group_survives_the_size_threshold() {
    let mut wal = TestWal::spawn(100, Duration::from_secs(60));
    wal.begin_group();
    wal.put(1, b"a", 200);
    wal.put(2, b"b", 200);
    wal.assert_no_commit_for(
        "a write group exceeds the size threshold",
        Duration::from_millis(100),
    );
    wal.end_group();
    // Closing re-arms the suppressed trigger: the oversized batch commits on
    // its own, with no explicit flush.
    wal.wait_until("the closed group to commit", || {
        !wal.committed_batches().is_empty()
    });
    assert_eq!(
        wal.committed_batches(),
        vec![vec![
            TestOp::Put(b"a".to_vec(), b"v".to_vec()),
            TestOp::Put(b"b".to_vec(), b"v".to_vec()),
        ]],
        "one batch, threshold notwithstanding"
    );
    assert_eq!(wal.outcomes.committed_sequence(), 2);
    wal.shutdown();
}

// FM-PERSISTENCE-011
/// An explicit flush inside a group is still honoured. The producer is gone or
/// is asking for durability now; answering "after your group closes" would
/// deadlock it. The shard never does this — it flushes only after closing —
/// so this pins the defensive branch, not a reachable behaviour.
#[test]
fn test_sink_explicit_flush_inside_a_group_is_honoured() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.begin_group();
    wal.put(1, b"a", 10);
    wal.flush().unwrap();
    assert_eq!(
        wal.committed_batches(),
        vec![vec![TestOp::Put(b"a".to_vec(), b"v".to_vec())]],
        "an explicit flush commits even mid-group"
    );
    wal.end_group();
    wal.shutdown();
}

// FM-PERSISTENCE-010
/// A producer that dies mid-group does not strand its entries: the disconnect
/// drain commits what is staged. The group can never be closed, and losing the
/// writes outright would be strictly worse than committing a prefix nobody can
/// observe (the process is going away).
#[test]
fn test_sink_unclosed_group_commits_on_disconnect() {
    let mut wal = TestWal::spawn(1024 * 1024, Duration::from_secs(60));
    wal.begin_group();
    wal.put(1, b"a", 10);
    wal.put(2, b"b", 10);
    wal.shutdown();

    assert_eq!(
        wal.committed_batches(),
        vec![vec![
            TestOp::Put(b"a".to_vec(), b"v".to_vec()),
            TestOp::Put(b"b".to_vec(), b"v".to_vec()),
        ]],
        "the shutdown drain commits an unclosed group"
    );
    assert_eq!(wal.outcomes.committed_sequence(), 2);
}

// FM-PERSISTENCE-011
/// Groups nest: only the outermost close re-arms the flush triggers, so a
/// caller that wraps a batch cannot have its atomicity broken by an inner
/// group closing early.
#[test]
fn test_sink_nested_write_groups_commit_as_one_batch() {
    let timeout = Duration::from_millis(20);
    let mut wal = TestWal::spawn(1024 * 1024, timeout);
    wal.begin_group();
    wal.put(1, b"a", 10);
    wal.begin_group();
    wal.put(2, b"b", 10);
    wal.end_group(); // inner close: still inside the outer group
    wal.put(3, b"c", 10);
    wal.assert_no_commit_for("the outer group is still open", timeout * 5);
    wal.end_group();
    wal.flush_through(0, 3).unwrap();

    assert_eq!(
        wal.committed_batches(),
        vec![vec![
            TestOp::Put(b"a".to_vec(), b"v".to_vec()),
            TestOp::Put(b"b".to_vec(), b"v".to_vec()),
            TestOp::Put(b"c".to_vec(), b"v".to_vec()),
        ]],
        "the inner close does not cut the outer group"
    );
    wal.shutdown();
}

#[test]
fn test_sink_size_threshold_triggers_commit_without_explicit_flush() {
    let mut wal = TestWal::spawn(100, Duration::from_secs(60));
    wal.put(1, b"big", 200);
    wal.wait_until("size-threshold flush to commit", || {
        !wal.committed_batches().is_empty()
    });
    assert_eq!(wal.outcomes.committed_sequence(), 1);
    wal.shutdown();
}

// FM-PERSISTENCE-007
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
    assert_eq!(wal.outcomes.committed_sequence(), 0);
    wal.shutdown();
}

// FM-PERSISTENCE-007
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

// FM-PERSISTENCE-010
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
    assert_eq!(wal.outcomes.committed_sequence(), 0);
}

// FM-PERSISTENCE-005
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
    assert_eq!(wal.outcomes.committed_sequence(), 2);
    assert_eq!(wal.outcomes.flush_failures(), 1);
    assert_eq!(wal.outcomes.lost_ops(), 1);
    wal.shutdown();
}

// FM-PERSISTENCE-005
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

// FM-PERSISTENCE-013
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
    assert_eq!(s.committed_sequence, 0);
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
    assert_eq!(
        s.committed_sequence, 2,
        "committed high-water tracks the flush"
    );
    assert!(s.last_flush_ok);
    assert_eq!(s.lost_ops, 0);
}

// FM-PERSISTENCE-012
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
            flush_thread_loop(
                rx,
                engine,
                Arc::new(AtomicUsize::new(1024 * 1024)),
                Duration::from_secs(60),
            );
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
            self.outcomes.committed_sequence() >= target
        });
        drop(self.tx.take());
        self.handle.take().unwrap().join().unwrap();
        let mut st = self.state.lock().unwrap();
        st.crash();
        st.recovered()
    }

    /// The out-of-band sync exactly as production performs it
    /// ([`crate::rocks::RocksStore::durable_sync`]): snapshot what has been
    /// committed, fsync, then publish that sequence as durable. Snapshotting
    /// first is the part that must not be reordered — a write committed during
    /// the flush may not be covered by it.
    fn durable_sync(&self) {
        use crate::rocks::DurableSyncTarget;
        let covered = self.outcomes.committed_sequence();
        self.state.lock().unwrap().fsync();
        self.outcomes.publish_synced_through(covered);
    }

    /// Wait until every write sent so far has reached the commit seam.
    fn wait_committed(&self, target: u64) {
        spin_until("all sent writes committed to the sink", || {
            self.outcomes.committed_sequence() >= target
        });
    }

    /// Stop the flush thread cleanly (no crash), for tests that assert on the
    /// watermarks rather than on what survives.
    fn stop(mut self) {
        drop(self.tx.take());
        self.handle.take().unwrap().join().unwrap();
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

// FM-PERSISTENCE-002
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

// FM-PERSISTENCE-002
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

// FM-PERSISTENCE-003
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

// FM-PERSISTENCE-004
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

// FM-PERSISTENCE-004
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

// FM-PERSISTENCE-043
/// `Periodic`: committing is not syncing. N ops committed at the (never
/// fsyncing) commit seam leave the durable sequence at 0; only the periodic
/// sync task advances it, and then to exactly the prefix its flush covered.
/// Before the fix `durable_sequence()` was stored on every successful commit,
/// so it read `N` here with nothing on the device.
#[test]
fn durable_sequence_tracks_only_fsynced_commits_in_periodic_mode() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Periodic {
        interval_ms: 60_000,
    });
    let n = 8u64;
    for i in 0..n {
        wal.put(&key(i as u32), b"v");
    }
    wal.flush();
    wal.wait_committed(n);

    assert_eq!(
        wal.outcomes.committed_sequence(),
        n,
        "every op reached RocksDB"
    );
    assert!(
        wal.commit_syncs().iter().all(|&s| !s),
        "periodic mode never fsyncs at the commit seam: {:?}",
        wal.commit_syncs()
    );
    assert_eq!(
        wal.outcomes.durable_sequence(),
        0,
        "no fsync has happened, so nothing may be reported durable"
    );

    // The periodic tick: fsync, then publish what it covered.
    wal.durable_sync();
    assert_eq!(
        wal.outcomes.durable_sequence(),
        n,
        "the periodic sync makes exactly the committed prefix durable"
    );

    // The next interval's writes are committed but not yet synced.
    for i in n..(n + 4) {
        wal.put(&key(i as u32), b"v");
    }
    wal.flush();
    wal.wait_committed(n + 4);
    assert_eq!(wal.outcomes.committed_sequence(), n + 4);
    assert_eq!(
        wal.outcomes.durable_sequence(),
        n,
        "the new window is not durable until the next tick"
    );
    wal.stop();
}

// FM-PERSISTENCE-043
/// `Async`: same rule, no clock. The durable sequence stays at 0 through any
/// number of commits — including an explicit `WalCommand::Flush`, which in this
/// mode empties the buffer without fsyncing — and moves only when an
/// out-of-band sync publishes its coverage.
#[test]
fn durable_sequence_tracks_only_fsynced_commits_in_async_mode() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Async);
    let n = 6u64;
    for i in 0..n {
        wal.put(&key(i as u32), b"v");
        wal.flush(); // an explicit durability request, per write
    }
    wal.wait_committed(n);

    assert_eq!(wal.outcomes.committed_sequence(), n);
    assert_eq!(
        wal.outcomes.durable_sequence(),
        0,
        "async fsyncs nothing on its own, not even on an explicit flush"
    );

    wal.durable_sync();
    assert_eq!(
        wal.outcomes.durable_sequence(),
        n,
        "an out-of-band fsync is the only thing that advances it"
    );
    wal.stop();
}

// FM-PERSISTENCE-043
/// `Sync` mode is unchanged: every commit fsyncs, so the two watermarks are the
/// same number and the fix is invisible here — which is exactly why no existing
/// test noticed the defect.
#[test]
fn sync_mode_durable_sequence_equals_committed_sequence() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Sync);
    let n = 5u64;
    for i in 0..n {
        wal.put(&key(i as u32), b"v");
        wal.flush();
    }
    wal.wait_committed(n);
    assert_eq!(wal.outcomes.committed_sequence(), n);
    assert_eq!(
        wal.outcomes.durable_sequence(),
        n,
        "in sync mode committing is syncing"
    );
    wal.stop();
}

// FM-PERSISTENCE-043
/// A flush with nothing staged is a no-op on both watermarks: it must not
/// commit, must not re-report a stale sequence, and must not let the durable
/// watermark drift toward the committed one for free.
#[test]
fn empty_flush_advances_neither_sequence() {
    let mut wal = PageCacheWal::spawn(DurabilityMode::Async);
    wal.put(&key(0), b"v");
    wal.put(&key(1), b"v");
    wal.flush();
    wal.wait_committed(2);
    let commits = wal.commit_syncs().len();

    wal.flush();
    wal.flush();

    assert_eq!(
        wal.commit_syncs().len(),
        commits,
        "an empty stage must not reach the sink at all"
    );
    assert_eq!(
        wal.outcomes.committed_sequence(),
        2,
        "the committed sequence stays where the last real commit left it"
    );
    assert_eq!(
        wal.outcomes.durable_sequence(),
        0,
        "an empty flush is not an fsync"
    );
    wal.stop();
}

// FM-PERSISTENCE-013
/// The WAL writer must *adopt* the shared threshold cell handed to it, not copy
/// the number out of `WalConfig`. This is the seam CONFIG SET
/// `batch-size-threshold-kb` writes into: the manager owns the cell, every
/// writer built afterwards reads the same one, and a live set is visible to the
/// flush path without rebuilding the WAL.
#[tokio::test]
async fn test_wal_adopts_shared_batch_size_threshold_handle() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let shared = Arc::new(AtomicUsize::new(64 * 1024));
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            // Deliberately different from the handle: the handle wins.
            batch_size_threshold: 1024 * 1024,
            batch_size_threshold_handle: Some(shared.clone()),
            ..Default::default()
        },
        m.clone(),
    );
    assert_eq!(wal.batch_size_threshold(), 64 * 1024);

    // A later write through the shared cell is seen by the running writer.
    shared.store(8 * 1024, Ordering::Relaxed);
    assert_eq!(wal.batch_size_threshold(), 8 * 1024);

    // And the writer's own setter is visible through the shared cell, so the two
    // directions cannot drift apart.
    wal.set_batch_size_threshold(4 * 1024);
    assert_eq!(shared.load(Ordering::Relaxed), 4 * 1024);

    // Without a handle the writer falls back to the configured value.
    let solo = RocksWalWriter::new(
        rocks,
        1,
        WalConfig {
            batch_size_threshold: 2048,
            ..Default::default()
        },
        m,
    );
    assert_eq!(solo.batch_size_threshold(), 2048);
}

// ============================================================================
// RocksSink — the production WriteSink, driven directly
// ============================================================================
//
// The engine tests above substitute an in-memory sink, so the production seam's
// own contract (staging is visible, a commit consumes the batch, and only an
// fsync'd commit moves the durable-sync watermark) is asserted here against a
// real store.

/// Staging is observable through `staged_len`, and a commit consumes the batch
/// whatever its durability — the engine's `staged_len() == 0` early return is
/// what keeps an empty flush from touching RocksDB at all.
#[test]
fn rocks_sink_reports_its_staged_batch_and_a_commit_consumes_it() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
    let mut sink = RocksSink::new(rocks.clone(), 0);
    assert_eq!(sink.staged_len(), 0, "a fresh sink has nothing staged");
    sink.stage_put(b"a", b"1").unwrap();
    assert_eq!(sink.staged_len(), 1);
    sink.stage_delete(b"b").unwrap();
    assert_eq!(sink.staged_len(), 2, "every staged op counts, deletes too");
    sink.commit(false).unwrap();
    assert_eq!(
        sink.staged_len(),
        0,
        "the batch leaves the sink on commit, success or not"
    );
    assert_eq!(rocks.get(0, b"a").unwrap().as_deref(), Some(&b"1"[..]));
}

// FM-PERSISTENCE-035
/// Only a `sync = true` commit has fsync'd through its sequence, so only that
/// commit may advance the persisted durable-sync watermark. Recording it after
/// an async commit would claim durability the data does not have, and the next
/// open would read a watermark the WAL cannot reach.
#[test]
fn only_a_synced_rocks_sink_commit_records_the_durable_watermark() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
    let mut sink = RocksSink::new(rocks.clone(), 0);
    let baseline = crate::rocks::wal_watermark::read(rocks.path());
    assert_eq!(
        baseline,
        Some(0),
        "opening an empty store re-baselines the watermark at sequence 0"
    );

    sink.stage_put(b"async", b"1").unwrap();
    sink.commit(false).unwrap();
    assert!(
        rocks.latest_sequence_number() > 0,
        "the async commit did advance the RocksDB sequence"
    );
    assert_eq!(
        crate::rocks::wal_watermark::read(rocks.path()),
        baseline,
        "an async commit is not individually durable and must not advance the watermark"
    );

    sink.stage_put(b"synced", b"2").unwrap();
    sink.commit(true).unwrap();
    assert_eq!(
        crate::rocks::wal_watermark::read(rocks.path()),
        Some(rocks.latest_sequence_number()),
        "a synced commit records the sequence its fsync covered"
    );
}

/// Merges are staged as RocksDB merge operands, not puts: the folded value the
/// CF's merge operator produces is what a later read must see. A sink that
/// silently dropped merge operands would lose every HLL delta.
#[test]
fn rocks_sink_stages_merge_operands_that_the_merge_operator_folds() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
    let mut sink = RocksSink::new(rocks.clone(), 0);
    let md = KeyMetadata::new(0);
    let first = crate::serialization::serialize_hll_delta(&[(7, 3)], &md);
    let second = crate::serialization::serialize_hll_delta(&[(11, 5)], &md);
    sink.stage_merge(b"hll", &first).unwrap();
    sink.stage_merge(b"hll", &second).unwrap();
    sink.commit(true).unwrap();

    let stored = rocks
        .get(0, b"hll")
        .unwrap()
        .expect("the merged value must exist");
    assert_eq!(
        stored,
        crate::serialization::merge_hll_serialized(None, &[&first, &second]).unwrap(),
        "the stored value is both operands folded in order"
    );
}

/// `current_timestamp_ms` stamps `last_flush_timestamp_ms`, which operators read
/// as "when did this shard last flush". It must be wall-clock milliseconds since
/// the unix epoch — a constant would make every shard look permanently stale (or
/// permanently fresh) to the staleness math built on it.
#[test]
fn current_timestamp_ms_is_wall_clock_millis_since_the_epoch() {
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    };
    let before = now();
    let stamped = current_timestamp_ms();
    let after = now();
    assert!(
        before <= stamped && stamped <= after,
        "expected {before} <= {stamped} <= {after}"
    );
}

/// The batch timeout is a real elapsed-time trigger, not a formality: a partial
/// batch that has been waiting longer than the timeout must meet the trigger the
/// next time the engine evaluates it, even though it is nowhere near the size
/// threshold. Driven on the engine directly because the flush thread's own
/// `recv_timeout` arm would cut the batch anyway, hiding whether this trigger
/// works at all.
#[test]
fn a_partial_batch_older_than_the_timeout_meets_the_flush_trigger() {
    let mut engine = FlushEngine::new(
        TestSink {
            staged: Vec::new(),
            committed: Arc::new(Mutex::new(Vec::new())),
            fail_commits: Arc::new(AtomicUsize::new(0)),
            fail_stages: Arc::new(AtomicUsize::new(0)),
        },
        0,
        &DurabilityMode::Async,
        Arc::new(WalLagAtomics {
            pending_ops: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(0),
        }),
        Arc::new(FlushOutcomes::new()),
        Arc::new(NoopMetricsRecorder::new()),
    );
    engine.apply(WalEntry::Put {
        seq: 1,
        key: Bytes::from_static(b"k"),
        value: vec![0u8; 8],
        size_estimate: 8,
    });
    assert!(
        !engine.should_flush(1024 * 1024, Duration::from_secs(3600)),
        "a young batch far under the threshold must keep batching"
    );
    let waited = Duration::from_millis(30);
    std::thread::sleep(waited);
    assert!(
        engine.since_last_flush() >= waited,
        "elapsed time must be measured, not assumed"
    );
    assert!(
        engine.should_flush(1024 * 1024, Duration::from_millis(5)),
        "a batch older than the timeout is cut even though it is under the threshold"
    );
}

/// `confirm_durable_through`'s last line of defense: an `Ok` flush with no
/// overlapping recorded failure must still not confirm a sequence the engine
/// never committed. Getting the comparison backwards would turn the check into
/// its own opposite — silently confirming exactly the unconfirmable case.
#[test]
fn a_committed_sequence_below_the_target_is_refused_even_after_an_ok_flush() {
    let outcomes = FlushOutcomes::new();
    let err = outcomes
        .confirm_durable_through(0, 5, Ok(()))
        .expect_err("committed 0 cannot confirm target 5");
    assert!(
        err.to_string().contains("below confirmation target 5"),
        "unexpected error: {err}"
    );
    // Reaching the target confirms; overshooting it confirms too.
    outcomes.record_success(5, false);
    outcomes.confirm_durable_through(0, 5, Ok(())).unwrap();
    outcomes.confirm_durable_through(0, 4, Ok(())).unwrap();
}

/// `RocksStore::durable_sync` reads each registered target's committed sequence
/// and publishes it back as that target's synced sequence, so a
/// `DurableSyncTarget` that misreported its committed sequence would publish a
/// durability claim for the wrong point in the log.
#[test]
fn durable_sync_publishes_the_targets_own_committed_sequence() {
    let tmp = TempDir::new().unwrap();
    let rocks = crate::rocks::RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();
    let outcomes = Arc::new(FlushOutcomes::new());
    outcomes.record_success(7, false);
    assert_eq!(
        outcomes.durable_sequence(),
        0,
        "an async commit is committed, not synced"
    );
    let target: Arc<dyn crate::rocks::DurableSyncTarget> = outcomes.clone();
    rocks.register_sync_target(&target);
    rocks.durable_sync().unwrap();
    assert_eq!(
        outcomes.durable_sequence(),
        7,
        "the out-of-band sync publishes exactly what the target had committed"
    );
}

// ============================================================================
// RocksWalWriter — the enqueue half
// ============================================================================

/// Poll `cond` until it holds, or fail after `label` never came true. Batching
/// is handed to a separate thread, so the observable effects of an enqueue are
/// eventually-consistent with the call that produced them.
async fn wait_for(label: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {label}");
}

fn staging_writer(rocks: Arc<crate::rocks::RocksStore>, shard: usize) -> RocksWalWriter {
    RocksWalWriter::new(
        rocks,
        shard,
        WalConfig {
            mode: DurabilityMode::Async,
            // Huge threshold + far-off timeout: nothing auto-flushes, so what
            // the enqueue side accounted for stays observable.
            batch_size_threshold: 100 * 1024 * 1024,
            batch_timeout_ms: 600_000,
            ..Default::default()
        },
        Arc::new(NoopMetricsRecorder::new()),
    )
}

/// Every enqueue assigns the next sequence (1-based) and charges the pending
/// gauges the bytes its entry will occupy. `pending_bytes` is what the
/// size-threshold trigger and the `wal_pending_bytes` metric are computed from,
/// so an estimate that is off by a factor or a term retunes batching and
/// misreports lag at the same time.
#[tokio::test]
async fn each_enqueue_assigns_the_next_sequence_and_charges_its_own_size() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let wal = staging_writer(rocks, 0);
    let md = KeyMetadata::new(1);

    let value = Value::string("v1");
    assert_eq!(wal.write_set(b"k1", &value, &md).await.unwrap(), 1);
    let mut expected = b"k1".len() + serialize(&value, &md).len() + 32;
    wait_for("the set to be staged", || {
        wal.lag_stats().pending_bytes == expected
    })
    .await;

    assert_eq!(wal.write_delete(b"delete-me").await.unwrap(), 2);
    expected += b"delete-me".len() + 32;
    wait_for("the delete to be staged", || {
        wal.lag_stats().pending_bytes == expected
    })
    .await;

    assert_eq!(wal.write_merge(b"hll", &[(7, 3)], &md).await.unwrap(), 3);
    expected += b"hll".len() + crate::serialization::serialize_hll_delta(&[(7, 3)], &md).len() + 32;
    wait_for("the merge to be staged", || {
        wal.lag_stats().pending_bytes == expected
    })
    .await;

    let stats = wal.lag_stats();
    assert_eq!(stats.pending_ops, 3, "three entries are buffered");
    assert_eq!(stats.sequence, 3, "the writer assigned three sequences");

    // A clear takes the next sequence like any other entry, but it is a flush
    // barrier: it drains everything staged before it rather than joining the
    // batch, so the pending gauges fall to zero behind it.
    assert_eq!(wal.write_clear().await.unwrap(), 4);
    wait_for("the clear barrier to drain the buffer", || {
        wal.lag_stats().pending_ops == 0
    })
    .await;
    assert_eq!(wal.lag_stats().pending_bytes, 0);
}

/// A merge enqueued through the writer must reach the store as a merge operand
/// the CF's operator folds — the on-disk half of `PFADD`'s durability.
#[tokio::test]
async fn a_merge_enqueued_through_the_writer_lands_folded() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let wal = staging_writer(rocks.clone(), 0);
    let md = KeyMetadata::new(1);
    assert_eq!(wal.write_merge(b"hll", &[(7, 3)], &md).await.unwrap(), 1);
    wal.flush_async().await.unwrap();
    let stored = rocks
        .get(0, b"hll")
        .unwrap()
        .expect("the merged value must be on disk");
    assert_eq!(
        stored,
        crate::serialization::merge_hll_serialized(
            None,
            &[&crate::serialization::serialize_hll_delta(&[(7, 3)], &md)]
        )
        .unwrap()
    );
}

// FM-PERSISTENCE-001
/// The group markers are what make a multi-key command atomic in storage. With
/// the threshold at one byte every write would otherwise be cut into its own
/// batch; inside a group nothing may commit until the group closes.
#[tokio::test]
async fn a_writer_group_suppresses_the_size_trigger_until_it_closes() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            // One byte: every ungrouped write trips the size trigger at once.
            batch_size_threshold: 1,
            batch_timeout_ms: 600_000,
            ..Default::default()
        },
        Arc::new(NoopMetricsRecorder::new()),
    );
    let md = KeyMetadata::new(1);

    wal.begin_group().await.unwrap();
    wal.write_set(b"g1", &Value::string("a"), &md)
        .await
        .unwrap();
    wal.write_set(b"g2", &Value::string("b"), &md)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rocks.get(0, b"g1").unwrap().is_none() && rocks.get(0, b"g2").unwrap().is_none(),
        "an open group must hold both writes back despite the 1-byte threshold"
    );

    wal.end_group().await.unwrap();
    wait_for("the closed group to commit", || {
        rocks.get(0, b"g1").unwrap().is_some()
    })
    .await;
    assert!(
        rocks.get(0, b"g2").unwrap().is_some(),
        "the group commits as one batch, not one key at a time"
    );
}

/// `flush_through` is the acknowledge path: it must actually drain the buffer
/// before confirming, or a client would be told its write is durable while it
/// sits in the flush thread's queue.
#[tokio::test]
async fn flush_through_drains_the_buffer_before_it_confirms() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let wal = staging_writer(rocks.clone(), 0);
    let md = KeyMetadata::new(1);
    let after = wal.sequence();
    wal.write_set(b"acked", &Value::string("v"), &md)
        .await
        .unwrap();
    wal.flush_through(after).await.unwrap();
    assert!(
        rocks.get(0, b"acked").unwrap().is_some(),
        "the confirmed write must be in the store by the time flush_through returns"
    );
    assert_eq!(wal.lag_stats().pending_ops, 0, "the buffer was drained");
}

/// The writer's sequence accessors are the ones `INFO persistence` and the
/// replication acknowledgement path read. In `sync` mode a committed batch is
/// also an fsynced one, so both watermarks land on the last sequence written —
/// and each writer reports the shard it was built for.
#[tokio::test]
async fn sync_mode_accessors_report_the_writers_own_shard_and_watermarks() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());
    let wal = RocksWalWriter::new(
        rocks,
        2,
        WalConfig {
            mode: DurabilityMode::Sync,
            ..Default::default()
        },
        Arc::new(NoopMetricsRecorder::new()),
    );
    assert_eq!(wal.shard_id(), 2, "the writer reports its own shard");
    let md = KeyMetadata::new(1);
    wal.write_set(b"s1", &Value::string("a"), &md)
        .await
        .unwrap();
    wal.write_set(b"s2", &Value::string("b"), &md)
        .await
        .unwrap();
    wal.flush_async().await.unwrap();
    assert_eq!(wal.sequence(), 2);
    assert_eq!(
        wal.committed_sequence(),
        2,
        "both entries reached storage together"
    );
    assert_eq!(
        wal.durable_sequence(),
        2,
        "a sync-mode commit is fsynced, so the durable watermark follows it"
    );
}

// ============================================================================
// Flush-thread batching boundary and failure logging
// ============================================================================

/// Pre-load every command into the channel *before* the flush thread starts, so
/// the opportunistic drain loop sees a full queue and its stopping condition is
/// deterministic rather than a race with the producer.
fn run_preloaded(
    cmds: Vec<WalCommand>,
    batch_size_threshold: usize,
) -> (Vec<Vec<TestOp>>, Arc<FlushOutcomes>) {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let sink = TestSink {
        staged: Vec::new(),
        committed: Arc::clone(&committed),
        fail_commits: Arc::new(AtomicUsize::new(0)),
        fail_stages: Arc::new(AtomicUsize::new(0)),
    };
    let outcomes = Arc::new(FlushOutcomes::new());
    let engine = FlushEngine::new(
        sink,
        0,
        &DurabilityMode::Async,
        Arc::new(WalLagAtomics {
            pending_ops: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(0),
        }),
        Arc::clone(&outcomes),
        Arc::new(NoopMetricsRecorder::new()),
    );
    let (tx, rx) = flume::bounded(64);
    for cmd in cmds {
        tx.send(cmd).unwrap();
    }
    drop(tx); // the loop drains, then commits whatever is left, then returns
    let handle = std::thread::spawn(move || {
        flush_thread_loop(
            rx,
            engine,
            Arc::new(AtomicUsize::new(batch_size_threshold)),
            // Long enough that the timeout trigger never participates.
            Duration::from_secs(3600),
        );
    });
    handle.join().unwrap();
    let batches = committed.lock().unwrap().clone();
    (batches, outcomes)
}

// FM-PERSISTENCE-001
/// The drain loop packs queued entries into the current batch *until* the batch
/// reaches the size threshold — not one past it, and not one entry per batch.
/// The boundary is exact: with four equal entries and a threshold of three, the
/// first committed batch holds exactly three.
#[test]
fn the_drain_loop_stops_packing_exactly_at_the_size_threshold() {
    let entry = |seq: u64, key: &'static [u8]| {
        WalCommand::Write(WalEntry::Put {
            seq,
            key: Bytes::from_static(key),
            value: vec![0u8; 10],
            size_estimate: 10,
        })
    };
    let (batches, _) = run_preloaded(
        vec![
            entry(1, b"a"),
            entry(2, b"b"),
            entry(3, b"c"),
            entry(4, b"d"),
        ],
        30,
    );
    let sizes: Vec<usize> = batches.iter().map(|b| b.len()).collect();
    assert_eq!(
        sizes,
        vec![3, 1],
        "three entries fill the threshold exactly; the fourth waits for the next batch"
    );
}

// FM-PERSISTENCE-001
/// An entry that on its own overshoots the threshold *ends* its batch. The
/// drain condition packs while the batch is still under the limit, so flipping
/// it to "while over the limit" is not a harmless swap: it would turn every
/// oversized write into a magnet that pulls the whole queue into one batch.
#[test]
fn an_oversized_entry_closes_its_batch_instead_of_pulling_the_queue_in() {
    let entry = |seq: u64, key: &'static [u8]| {
        WalCommand::Write(WalEntry::Put {
            seq,
            key: Bytes::from_static(key),
            value: vec![0u8; 100],
            size_estimate: 100,
        })
    };
    let (batches, _) = run_preloaded(
        vec![
            entry(1, b"a"),
            entry(2, b"b"),
            entry(3, b"c"),
            entry(4, b"d"),
        ],
        30,
    );
    let sizes: Vec<usize> = batches.iter().map(|b| b.len()).collect();
    assert_eq!(
        sizes,
        vec![1, 1, 1, 1],
        "each entry is already over the threshold, so none of them drains the next in"
    );
}

/// Flush failures can recur every batch timeout, so only the first in each
/// interval is logged at full severity — but that first one must be logged, and
/// the ones it suppresses must still be visible at `DEBUG`. Driven on the engine
/// directly: the capturing subscriber is thread-local, and the flush thread is
/// not this thread.
#[test]
fn repeated_flush_failures_log_once_at_error_and_then_at_debug() {
    #[derive(Clone, Default)]
    struct LevelCapture(Arc<Mutex<Vec<tracing::Level>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LevelCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.lock().unwrap().push(*event.metadata().level());
        }
    }
    use tracing_subscriber::layer::SubscriberExt;

    let capture = LevelCapture::default();
    let fail_commits = Arc::new(AtomicUsize::new(2));
    let mut engine = FlushEngine::new(
        TestSink {
            staged: Vec::new(),
            committed: Arc::new(Mutex::new(Vec::new())),
            fail_commits: Arc::clone(&fail_commits),
            fail_stages: Arc::new(AtomicUsize::new(0)),
        },
        0,
        &DurabilityMode::Async,
        Arc::new(WalLagAtomics {
            pending_ops: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(0),
        }),
        Arc::new(FlushOutcomes::new()),
        Arc::new(NoopMetricsRecorder::new()),
    );
    let put = |seq: u64| WalEntry::Put {
        seq,
        key: Bytes::from_static(b"k"),
        value: vec![0u8; 4],
        size_estimate: 4,
    };
    {
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        engine.apply(put(1));
        assert!(engine.flush().is_err(), "the first commit was made to fail");
        engine.apply(put(2));
        assert!(engine.flush().is_err(), "and so was the second");
    }
    let levels = capture.0.lock().unwrap().clone();
    assert_eq!(
        levels
            .iter()
            .filter(|l| **l == tracing::Level::ERROR)
            .count(),
        1,
        "the first failure is logged loudly, the second is rate-limited: {levels:?}"
    );
    assert!(
        levels.contains(&tracing::Level::DEBUG),
        "the suppressed failure is still recorded at DEBUG: {levels:?}"
    );
}

/// Everything the shard task touches goes through `dyn WalSink`, so the trait
/// impl's delegation is itself load-bearing: a method that answered from thin
/// air instead of forwarding would be invisible to every test that holds the
/// concrete writer.
#[tokio::test]
async fn the_wal_sink_trait_object_forwards_every_method_to_the_writer() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap());
    let writer = Arc::new(RocksWalWriter::new(
        rocks.clone(),
        2,
        WalConfig {
            mode: DurabilityMode::Sync,
            batch_size_threshold: 100 * 1024 * 1024,
            batch_timeout_ms: 600_000,
            ..Default::default()
        },
        Arc::new(NoopMetricsRecorder::new()),
    ));
    let sink: Arc<dyn WalSink> = writer.clone();
    let md = KeyMetadata::new(1);

    assert_eq!(sink.shard_id(), 2);
    assert_eq!(sink.sequence(), 0);

    // A group's writes travel the same channel and commit together.
    sink.begin_group().await.unwrap();
    assert_eq!(
        sink.write_set(b"t1", &Value::string("a"), &md)
            .await
            .unwrap(),
        1
    );
    assert_eq!(sink.write_merge(b"hll", &[(7, 3)], &md).await.unwrap(), 2);
    assert_eq!(sink.write_delete(b"t1").await.unwrap(), 3);
    sink.end_group().await.unwrap();
    assert_eq!(sink.sequence(), 3, "three sequences were assigned");

    sink.flush_async().await.unwrap();
    assert!(
        rocks.get(2, b"t1").unwrap().is_none(),
        "the delete inside the group landed with it"
    );
    assert!(
        rocks.get(2, b"hll").unwrap().is_some(),
        "the merge inside the group landed too"
    );
    assert_eq!(
        sink.durable_sequence(),
        3,
        "sync mode fsyncs what it commits"
    );

    // `flush_through` confirms the range written after the captured sequence.
    let after = sink.sequence();
    assert_eq!(sink.write_clear().await.unwrap(), 4);
    sink.flush_through(after).await.unwrap();
    assert!(
        rocks.get(2, b"hll").unwrap().is_none(),
        "the clear that flush_through confirmed really ran"
    );
    let stats = sink.lag_stats();
    assert_eq!(stats.shard_id, 2);
    assert_eq!(stats.sequence, 4);
}

// FM-PERSISTENCE-001
/// The group markers only *do* anything if the trait impl forwards them, and a
/// trait method that answered `Ok(())` without forwarding is invisible under a
/// generous batch threshold — everything lands anyway at the closing flush.
/// Pinned at a one-byte threshold instead: the open group is the only thing
/// holding the two writes back, and the close is the only thing releasing them.
#[tokio::test]
async fn the_trait_objects_group_markers_reach_the_writer() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let writer = Arc::new(RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 1,
            batch_timeout_ms: 600_000,
            ..Default::default()
        },
        Arc::new(NoopMetricsRecorder::new()),
    ));
    let sink: Arc<dyn WalSink> = writer.clone();
    let md = KeyMetadata::new(1);

    sink.begin_group().await.unwrap();
    sink.write_set(b"tg1", &Value::string("a"), &md)
        .await
        .unwrap();
    sink.write_set(b"tg2", &Value::string("b"), &md)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rocks.get(0, b"tg1").unwrap().is_none() && rocks.get(0, b"tg2").unwrap().is_none(),
        "a group opened through the trait object must hold both writes back"
    );

    sink.end_group().await.unwrap();
    wait_for(
        "the group closed through the trait object to commit",
        || rocks.get(0, b"tg1").unwrap().is_some(),
    )
    .await;
    assert!(
        rocks.get(0, b"tg2").unwrap().is_some(),
        "the group commits as one batch, not one key at a time"
    );
}
