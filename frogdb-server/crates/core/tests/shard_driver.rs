//! Shard-driver integration harness.
//!
//! Proves the seam architecture end to end: an out-of-crate consumer builds a
//! real [`frogdb_core::ShardWorker`] via the public [`ShardWorkerBuilder`],
//! populates its registry with the *real* command set through
//! `frogdb_commands::register_all` (a dev-dependency cycle that only resolves
//! for integration tests, which link the single normal build of `frogdb-core`
//! that `frogdb-commands` also links), and drives commands + ticks through the
//! feature-gated `drive*` seams. Those seams are reachable here because the
//! crate's own `[dev-dependencies]` self-dep enables the `shard-driver` and
//! `fake-wal` features (mirroring `frogdb-telemetry`'s `features = ["testing"]`
//! precedent).

mod generator;
mod harness;
mod notify_capture;
mod sink;

// Scenario submodules (one per targeted scenario; S7 is turmoil-level, server crate).
mod scenario_s1;
mod scenario_s2;
mod scenario_s3;
mod scenario_s4;
mod scenario_s5;
mod scenario_s6;
mod scenario_s8;

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
    worker.drive_expiry_tick();
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
