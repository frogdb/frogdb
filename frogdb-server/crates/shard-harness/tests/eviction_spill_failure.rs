//! Level-3 scenario for issue 41: a failed spill must not become a real delete.
//!
//! `spill_for_eviction`'s error arm used to fall through to
//! `delete_for_eviction` unconditionally, and `delete_for_eviction` routes a
//! *real* removal through `run_internal_removal_effects` — WAL delete,
//! replicated `DEL`, `evicted` keyspace notification. A transient RocksDB
//! failure on the warm CF therefore destroyed the value permanently, on the
//! primary and on every replica, while the client's write returned `+OK`.
//!
//! Why level 3: the subject is the *effect pipeline* — what replicates and what
//! notifies — which only the worker produces. A level-2 store test can see that
//! `spill_key` returned `Err` (and `core/tests/tiered_storage.rs` already does)
//! but not one thing that follows from it. A level-4 server test would add a
//! socket without adding signal.
//!
//! ## Forcing a real `SpillError::Rocks` without a fault-injection seam
//!
//! `WarmTier` holds a concrete `Arc<RocksStore>`, not a trait object, so there
//! is no `try_put` to stub. There is a better lever: open the store with
//! `warm_enabled = false` and then hand that handle to `set_warm_store`. The
//! tier is *configured* (so `spill_key` gets past its `NoWarmStore` guard and
//! commits to spilling) but the warm column families do not exist, so
//! `put_warm` returns a genuine `RocksError::ColumnFamilyNotFound` and
//! `spill_key` maps it to `SpillError::Rocks`. That is the same variant an
//! ENOSPC or EIO produces, reached through the same code path, with zero
//! production-code change.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::eviction::{EvictionConfig, EvictionPolicy};
use frogdb_core::noop::NoopMetricsRecorder;
use frogdb_core::store::{HashMapStore, Store};
use frogdb_core::{
    CommandRegistry, CoreMsg, RocksConfig, RocksStore, ShardReceiver, ShardSender, ShardWorker,
    ShardWorkerBuilder,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use frogdb_shard_harness::notify_capture::{NotificationCapture, all_keyevents_mask};
use frogdb_shard_harness::recording_broadcaster::RecordingBroadcaster;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};

/// How the warm tier behaves in a given fixture.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WarmTier {
    /// Warm CFs exist; spills succeed.
    Working,
    /// A handle is configured but its warm CFs do not exist, so every
    /// `put_warm` fails with a `RocksError` — the ENOSPC/EIO shape.
    Failing,
    /// No handle at all: `spill_key` reports `NoWarmStore` before any I/O.
    Absent,
}

/// Everything a test has to keep alive for the worker to stay usable.
struct Fixture {
    worker: ShardWorker,
    broadcaster: Arc<RecordingBroadcaster>,
    capture: NotificationCapture,
    _conn_tx: mpsc::Sender<frogdb_core::shard::NewConnection>,
    _tmp: Option<TempDir>,
}

/// A 1-shard worker with the real command set, a `TieredLru` policy at
/// `memory_limit` bytes, a recording broadcaster and a keyevent capture.
fn fixture(memory_limit: u64, warm: WarmTier) -> Fixture {
    let mut store = HashMapStore::new();
    let tmp = match warm {
        WarmTier::Absent => None,
        WarmTier::Working | WarmTier::Failing => {
            let tmp = TempDir::new().expect("tempdir");
            let rocks = Arc::new(
                RocksStore::open_with_warm(
                    tmp.path(),
                    1,
                    &RocksConfig::default(),
                    warm == WarmTier::Working,
                )
                .expect("open rocks"),
            );
            store.set_warm_store(rocks, 0);
            Some(tmp)
        }
    };

    let (msg_tx, msg_rx) = mpsc::channel(16);
    let (conn_tx, conn_rx) = mpsc::channel(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let broadcaster = Arc::new(RecordingBroadcaster::new());

    let mut worker = ShardWorkerBuilder::new(0, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
        .with_registry(Arc::new(registry))
        .with_metrics(Arc::new(NoopMetricsRecorder::new()))
        .with_store(store)
        .with_eviction(EvictionConfig::new(memory_limit, EvictionPolicy::TieredLru))
        .with_replication(broadcaster.clone())
        .build();

    let capture = NotificationCapture::new(worker.drive_capture_keyspace(
        vec![Bytes::from_static(b"__keyevent@0__:*")],
        9_041,
        all_keyevents_mask(),
    ));

    Fixture {
        worker,
        broadcaster,
        capture,
        _conn_tx: conn_tx,
        _tmp: tmp,
    }
}

impl Fixture {
    async fn run(&mut self, name: &'static [u8], args: &[&str]) -> Response {
        let (tx, rx) = oneshot::channel();
        let msg = CoreMsg::Execute {
            command: Arc::new(ParsedCommand::new(
                Bytes::from_static(name),
                args.iter()
                    .map(|a| Bytes::copy_from_slice(a.as_bytes()))
                    .collect(),
            )),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp2,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        self.worker.drive(msg).await;
        rx.await.expect("execute response")
    }

    /// Whether the key is still logically present, hot *or* warm. Deliberately
    /// not `GET`: a warm key answers `GET` by unspilling, which the failing
    /// fixture cannot do (the CF it would read from does not exist), and the
    /// question this issue asks is "does the key still exist", not "can it be
    /// read right now".
    fn key_exists(&mut self, key: &[u8]) -> bool {
        self.worker.store.contains(key)
    }
}

/// A value large enough that one of them blows the limit below on its own.
fn big() -> String {
    "x".repeat(4096)
}

/// The memory limit every fixture runs at.
///
/// Sized between the ~4 KiB a hot `big()` costs and the ~114 bytes a *spilled*
/// key's metadata-only footprint costs, so a successful spill actually gets the
/// shard back under the limit. A limit tighter than the metadata footprint
/// would make even a working warm tier unable to satisfy the check, which is a
/// different scenario from the one under test.
const LIMIT: u64 = 1024;

/// **Issue 41, the defect.** A warm-tier I/O failure must not delete the key,
/// must not replicate a `DEL`, and must not fire an `evicted` notification.
/// The write is refused with `-OOM` instead.
///
/// **Fails against the pre-fix code**: `spill_for_eviction`'s `Err(_)` arm fell
/// through to `delete_for_eviction`, so `k1` vanished, a `DEL k1` was
/// broadcast, an `evicted` keyevent fired, and `SET k2` returned `+OK`.
#[tokio::test]
async fn a_warm_tier_write_failure_refuses_the_write_instead_of_deleting_the_key() {
    let mut f = fixture(LIMIT, WarmTier::Failing);
    assert!(matches!(&f.run(b"SET", &["k1", &big()]).await, Response::Simple(s) if s == "OK"));

    // Ignore the setup write's own propagation and notification.
    f.broadcaster.clear();
    let _ = f.capture.drain();

    let reply = f.run(b"SET", &["k2", &big()]).await;

    match &reply {
        Response::Error(e) => assert!(
            e.starts_with(b"OOM"),
            "expected an OOM rejection, got {:?}",
            String::from_utf8_lossy(e),
        ),
        other => panic!("expected the write to be rejected, got {other:?}"),
    }

    assert!(
        f.key_exists(b"k1"),
        "the key was destroyed by a warm-tier write failure",
    );
    assert!(
        !f.key_exists(b"k2"),
        "the rejected write must not have landed",
    );

    let dels = f.broadcaster.frames_named("DEL");
    assert!(
        dels.is_empty(),
        "a warm-tier failure replicated a DEL, destroying the value on every replica: {dels:?}",
    );

    let evicted: Vec<_> = f
        .capture
        .drain_keyevents()
        .into_iter()
        .filter(|(ev, _)| ev == "evicted")
        .collect();
    assert!(
        evicted.is_empty(),
        "a warm-tier failure fired an `evicted` notification for a key that still exists: {evicted:?}",
    );
}

/// The structural failure that is *not* an I/O failure still degrades to a real
/// eviction. `NoWarmStore` means the tiered policy was configured without a
/// warm tier, and evicting is exactly what plain LRU — which `TieredLru` is a
/// superset of — would have done. Nothing is broken, so refusing writes would
/// be the wrong answer.
///
/// This test exists to pin the boundary: the fix above must not turn every
/// spill failure into an OOM.
#[tokio::test]
async fn a_missing_warm_tier_still_degrades_to_a_real_eviction() {
    let mut f = fixture(LIMIT, WarmTier::Absent);
    assert!(matches!(&f.run(b"SET", &["k1", &big()]).await, Response::Simple(s) if s == "OK"));

    f.broadcaster.clear();
    let _ = f.capture.drain();

    assert!(
        matches!(&f.run(b"SET", &["k2", &big()]).await, Response::Simple(s) if s == "OK"),
        "an eviction-backed write should still succeed without a warm tier",
    );

    assert!(!f.key_exists(b"k1"), "the LRU victim should be gone");
    assert!(f.key_exists(b"k2"), "the write should have landed");

    let dels = f.broadcaster.frames_named("DEL");
    assert_eq!(
        dels.len(),
        1,
        "a real eviction propagates one DEL: {dels:?}"
    );
    assert_eq!(dels[0].first_arg_lossy(), "k1");

    let evicted: Vec<_> = f
        .capture
        .drain_keyevents()
        .into_iter()
        .filter(|(ev, _)| ev == "evicted")
        .collect();
    assert_eq!(
        evicted.len(),
        1,
        "a real eviction fires one `evicted` keyevent: {evicted:?}",
    );
    assert_eq!(evicted[0].1, Bytes::from_static(b"k1"));
}

/// And the happy path is unchanged: a successful spill is tiering, not removal
/// — the key survives, nothing replicates, nothing notifies. Without this the
/// two tests above could both pass on a build that never spills at all.
#[tokio::test]
async fn a_successful_spill_is_invisible_to_replicas_and_subscribers() {
    let mut f = fixture(LIMIT, WarmTier::Working);
    assert!(matches!(&f.run(b"SET", &["k1", &big()]).await, Response::Simple(s) if s == "OK"));

    f.broadcaster.clear();
    let _ = f.capture.drain();

    assert!(
        matches!(&f.run(b"SET", &["k2", &big()]).await, Response::Simple(s) if s == "OK"),
        "a working warm tier should absorb the eviction pressure (spills={}, mem={})",
        f.worker.store.warm_tier().spills(),
        f.worker.store.memory_used(),
    );

    assert!(f.key_exists(b"k1"), "a spilled key is still present");
    assert_eq!(
        f.worker.store.warm_tier().spills(),
        1,
        "the eviction should have gone through the warm tier, not around it",
    );

    assert!(
        f.broadcaster.frames_named("DEL").is_empty(),
        "a spill is tiering, not removal — it must not replicate a DEL",
    );
    let evicted: Vec<_> = f
        .capture
        .drain_keyevents()
        .into_iter()
        .filter(|(ev, _)| ev == "evicted")
        .collect();
    assert!(
        evicted.is_empty(),
        "a spill must not fire `evicted` for a key that is still there: {evicted:?}",
    );
}
