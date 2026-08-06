//! Shard-driver integration harness smoke tests.
//!
//! Proves the seam architecture end to end: an out-of-crate consumer (this
//! crate, `frogdb-shard-harness`) builds a real [`frogdb_core::ShardWorker`]
//! via the public [`ShardWorkerBuilder`], populates its registry with the
//! *real* command set through `frogdb_commands::register_all`, and drives
//! commands + ticks through the feature-gated `drive*` seams. Those seams are
//! reachable here because this crate's own `[dependencies]` enable the
//! `shard-driver` and `fake-wal` features on `frogdb-core` (see the crate-level
//! doc on `frogdb_shard_harness` for why the harness lives in its own crate
//! rather than as a `frogdb-core` dev-dependency).
//!
//! The reusable harness (`ShardDriver`, sinks, notification capture, the
//! schedule generator) lives in this crate's `src/`; the scenario tests
//! (`scenario_s1`..`scenario_s8`, one per targeted concurrency scenario — S7 is
//! turmoil-level, in the server crate) are declared alongside this file in
//! `tests/main.rs`.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use frogdb_core::noop::NoopMetricsRecorder;
use frogdb_core::persistence::FakeFailure;
use frogdb_core::shard::{FakeWalRegistry, WalMode};
use frogdb_core::{
    CommandRegistry, CoreMsg, ShardMessage, ShardReceiver, ShardSender, ShardWorker,
    ShardWorkerBuilder,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

/// Build a 1-shard worker whose registry holds the real production command set.
fn real_worker() -> (ShardWorker, mpsc::Sender<frogdb_core::shard::NewConnection>) {
    let (msg_tx, msg_rx) = mpsc::channel(16);
    // The connection sender is returned so the caller keeps it alive; dropping
    // it would close the new-connection channel.
    let (conn_tx, conn_rx) = mpsc::channel(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let worker = ShardWorkerBuilder::new(0, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
        .with_registry(Arc::new(registry))
        .with_metrics(Arc::new(NoopMetricsRecorder::new()))
        .build();
    (worker, conn_tx)
}

/// Construct a `SET k v` / `GET k` execute message with its own response channel.
fn execute(name: &'static [u8], args: Vec<Bytes>) -> (ShardMessage, oneshot::Receiver<Response>) {
    let (tx, rx) = oneshot::channel();
    let msg = CoreMsg::Execute {
        command: Arc::new(ParsedCommand::new(Bytes::from_static(name), args)),
        conn_id: 1,
        txid: None,
        protocol_version: ProtocolVersion::Resp3,
        track_reads: false,
        no_touch: false,
        response_tx: tx,
    };
    (msg.into(), rx)
}

/// End-to-end smoke: drive a real `SET`+`GET` through the public seam and fire
/// both housekeeping ticks. Proves an integration test can reach the whole
/// dispatch path with the real registry — the property the in-crate unit
/// harness cannot have (frogdb-commands is unusable from a unit test).
#[tokio::test]
async fn smoke_real_registry_set_get_and_ticks() {
    let (mut worker, _conn_tx) = real_worker();

    let (set, set_rx) = execute(
        b"SET",
        vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
    );
    assert!(!worker.drive(set).await, "SET must not signal shutdown");
    assert!(matches!(set_rx.await.unwrap(), Response::Simple(_)));

    let (get, get_rx) = execute(b"GET", vec![Bytes::from_static(b"k")]);
    worker.drive(get).await;
    assert_eq!(
        get_rx.await.unwrap(),
        Response::Bulk(Some(Bytes::from_static(b"v"))),
        "GET must return the value written by the real SET command"
    );

    // The synchronous tick seams run without the event loop's timers.
    worker.drive_expiry_tick().await;
    worker.drive_waiter_timeout_tick();
}

/// F4 seam: a builder-injected fake-WAL failure at write-index 0 suppresses the
/// SET's recorded write. Drives a real `SET` through `WalMode::Fake` with
/// `with_fake_wal_failure(AtWriteIndex(0))` and asserts the fake log recorded no
/// write for the key — the persist-failure branch S6 exercises. Fails to compile
/// (`no method named with_fake_wal_failure`) until the builder seam lands.
#[tokio::test]
async fn fake_wal_failure_is_injected_at_index() {
    // Registry is a process-global keyed by shard id; isolate this run.
    FakeWalRegistry::clear();
    let (msg_tx, msg_rx) = mpsc::channel(16);
    let (_conn_tx, conn_rx) = mpsc::channel(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let mut worker = ShardWorkerBuilder::new(0, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
        .with_registry(Arc::new(registry))
        .with_metrics(Arc::new(NoopMetricsRecorder::new()))
        .with_wal_mode(WalMode::Fake)
        // Fail the FIRST write (index 0): a single SET must not persist.
        .with_fake_wal_failure(FakeFailure::AtWriteIndex(0))
        .build();

    // Drive one real SET through the dispatch path; the injected WAL failure
    // means the write is never recorded in the fake log.
    let (set, set_rx) = execute(
        b"SET",
        vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
    );
    worker.drive(set).await;
    let _ = set_rx.await;

    let log = FakeWalRegistry::log(0).expect("fake sink log registered");
    assert!(
        log.effects()
            .iter()
            .all(|e| e.key.as_deref() != Some(&b"k"[..])),
        "the injected index-0 WAL failure must suppress the SET's recorded write"
    );
}

/// Determinism R6: a `DriveTick(Expiry)` *message* runs the active-expiry sweep.
///
/// The turmoil simulation suppresses the event loop's 100 ms expiry timer branch
/// and delivers the sweep through the shard's ordinary message queue instead, so
/// the sweep is totally ordered against commands rather than racing them for a
/// `select!` branch. This pins the message → `drive_expiry_tick` route: the
/// physical key (and its expiry-index row) survives until the tick message is
/// dispatched, and is gone afterwards.
#[tokio::test]
async fn drive_tick_message_runs_the_expiry_sweep() {
    use frogdb_core::TickKind;
    use frogdb_core::store::BackdateExpiryResult;
    use frogdb_shard_harness::harness::ShardDriver;

    let mut d = ShardDriver::new(1);
    assert!(matches!(
        d.execute(0, "SET", &["k", "v"]).await,
        Response::Simple(_)
    ));
    assert_eq!(
        d.execute(0, "EXPIRE", &["k", "100"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        d.backdate_expiry(0, "k", 1_000).await,
        BackdateExpiryResult::Backdated
    );
    assert_eq!(
        d.expiry_index_check(0).await.total_entries,
        1,
        "the backdated key must still be physically present before the sweep"
    );

    assert!(
        !d.dispatch(0, ShardMessage::DriveTick(TickKind::Expiry))
            .await,
        "a drive tick must never signal shutdown"
    );

    assert_eq!(
        d.expiry_index_check(0).await.total_entries,
        0,
        "the queued DriveTick(Expiry) message must run the sweep that reaps the key"
    );
}

/// Determinism R6: a `DriveTick(WaiterTimeout)` *message* runs the blocking-waiter
/// timeout sweep — the queued counterpart of the suppressed 100 ms waiter branch.
#[tokio::test]
async fn drive_tick_message_runs_the_waiter_timeout_sweep() {
    use std::time::Duration;

    use frogdb_core::TickKind;
    use frogdb_core::types::BlockingOp;
    use frogdb_shard_harness::harness::ShardDriver;
    use tokio::time::Instant;

    let mut d = ShardDriver::new(1);
    let rx = d
        .block_wait(
            0,
            10,
            vec![Bytes::from_static(b"k")],
            BlockingOp::BLPop,
            // Already elapsed: the next sweep — and only a sweep — must time it out.
            Some(Instant::now() - Duration::from_millis(1)),
        )
        .await;
    assert_eq!(
        d.wait_queue_info(0).await.total_waiters,
        1,
        "the waiter must still be queued before the sweep"
    );

    assert!(
        !d.dispatch(0, ShardMessage::DriveTick(TickKind::WaiterTimeout))
            .await,
        "a drive tick must never signal shutdown"
    );

    assert_eq!(
        d.wait_queue_info(0).await.total_waiters,
        0,
        "the queued DriveTick(WaiterTimeout) message must run the timeout sweep"
    );
    assert!(
        matches!(rx.await, Ok(Response::NullArray)),
        "the timed-out BLPOP must be answered with its op-aware null-array reply"
    );
}
