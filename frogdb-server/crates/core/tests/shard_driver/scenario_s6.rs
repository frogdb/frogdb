//! S6 — persist-failure mid-transaction rollback (needs the F4 fake-WAL
//! injection seam). On WAL failure the transaction rolls back in reverse order
//! (rollback.rs:85-116) and every command result is replaced with the
//! EXECABORT error (post-F2 spelling). Store == pre-transaction snapshot.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use frogdb_core::persistence::{FakeFailure, WalFailurePolicy};
use frogdb_core::shard::types::TransactionResult;
use frogdb_core::shard::{
    CoreMsg, Envelope, FakeWalRegistry, NewConnection, ShardReceiver, ShardSender,
    ShardWorkerBuilder, WalMode,
};
use frogdb_core::{CommandRegistry, ShardWorker};

/// Unique shard ids so the process-global FakeWalRegistry never collides (D6).
static NEXT_SHARD_ID: AtomicUsize = AtomicUsize::new(1);

fn cmd(name: &str, args: &[&str]) -> ParsedCommand {
    ParsedCommand::new(
        Bytes::from(name.to_string()),
        args.iter().map(|a| Bytes::from(a.to_string())).collect(),
    )
}

/// Build a single-shard worker with a fake WAL failing at `fail_index`, in
/// rollback mode, holding the msg/conn senders alive.
fn build_rollback_worker(
    fail_index: usize,
) -> (
    ShardWorker,
    mpsc::Sender<Envelope>,
    mpsc::Sender<NewConnection>,
    usize,
) {
    let shard_id = NEXT_SHARD_ID.fetch_add(1, Ordering::SeqCst);
    let (msg_tx, msg_rx) = mpsc::channel::<Envelope>(16);
    let (conn_tx, conn_rx) = mpsc::channel::<NewConnection>(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let mut worker = ShardWorkerBuilder::new(shard_id, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx.clone())]))
        .with_registry(Arc::new(registry))
        .with_wal_mode(WalMode::Fake)
        .with_fake_wal_failure(FakeFailure::AtWriteIndex(fail_index))
        .build();
    // Enable rollback mode: shared AtomicU8 whose value is the Rollback
    // discriminant (WalFailurePolicy::Rollback.as_u8() == 1, config.rs:17).
    worker.set_wal_failure_policy_flag(Arc::new(AtomicU8::new(WalFailurePolicy::Rollback.as_u8())));
    (worker, msg_tx, conn_tx, shard_id)
}

async fn exec_tx(worker: &mut ShardWorker, commands: Vec<ParsedCommand>) -> TransactionResult {
    let (tx, rx) = oneshot::channel();
    let msg = CoreMsg::ExecTransaction {
        commands,
        watches: vec![],
        conn_id: 1,
        protocol_version: ProtocolVersion::Resp3,
        response_tx: tx,
    };
    worker.drive(msg).await;
    rx.await.expect("transaction result")
}

async fn get(worker: &mut ShardWorker, key: &str) -> Response {
    let (tx, rx) = oneshot::channel();
    let msg = CoreMsg::Execute {
        command: Arc::new(cmd("GET", &[key])),
        conn_id: 1,
        txid: None,
        protocol_version: ProtocolVersion::Resp3,
        track_reads: false,
        no_touch: false,
        response_tx: tx,
    };
    worker.drive(msg).await;
    rx.await.unwrap()
}

/// Positive control (carried from Task 2's review): on the SUCCESS path (no
/// WAL failure ever trips), the WAL log CONTAINS the written key. Without
/// this, the failure-path "no post-failure appends for b/c" assertion below
/// could pass vacuously if the fake WAL never recorded anything at all.
#[tokio::test]
async fn s6_success_path_wal_log_contains_key() {
    FakeWalRegistry::clear();
    // Failure index far beyond anything this test writes, so it never trips.
    let (mut worker, _mtx, _ctx, shard_id) = build_rollback_worker(usize::MAX);

    let result = exec_tx(&mut worker, vec![cmd("SET", &["k", "v"])]).await;
    match result {
        TransactionResult::Success(results) => {
            assert_eq!(results.len(), 1);
            assert!(
                !matches!(&results[0], Response::Error(_)),
                "success-path command must not error, got {:?}",
                results[0]
            );
        }
        other => panic!("expected Success, got {other:?}"),
    }

    assert_eq!(
        get(&mut worker, "k").await,
        Response::Bulk(Some(Bytes::from_static(b"v")))
    );

    let log = FakeWalRegistry::log(shard_id).expect("fake log registered");
    assert!(
        log.effects()
            .iter()
            .any(|e| e.key.as_deref() == Some(&b"k"[..])),
        "positive control: WAL log must contain the written key on the success path"
    );
}

/// Fail at write index 2. The fake sink counts one index per WAL record
/// (write_set/write_merge/write_delete each pass the index check, fake.rs:147-155):
/// the seed SET consumes index 0; the failing transaction's three writes are
/// indices 1 (a, persists), 2 (b, FAILS), 3 (c). All results become EXECABORT;
/// the store is fully restored.
#[tokio::test]
async fn s6_rollback_all_results_execabort_and_store_restored() {
    FakeWalRegistry::clear();
    // Prior key states: a pre-existing, b and c absent.
    let (mut worker, _mtx, _ctx, shard_id) = build_rollback_worker(2);
    let _ = exec_tx(&mut worker, vec![cmd("SET", &["a", "orig-a"])]).await; // write index 0 (persists)

    // Failing transaction: 3 writes across a (overwrite), b (create), c (create).
    let result = exec_tx(
        &mut worker,
        vec![
            cmd("SET", &["a", "new-a"]), // write index 1 (persists)
            cmd("SET", &["b", "new-b"]), // write index 2 (FAILS -> whole tx rolls back)
            cmd("SET", &["c", "new-c"]),
        ],
    )
    .await;

    // All results replaced with the EXECABORT error (post-F2 spelling).
    match result {
        TransactionResult::Success(results) => {
            assert_eq!(results.len(), 3);
            for r in &results {
                match r {
                    Response::Error(e) => assert!(
                        e.starts_with(b"EXECABORT"),
                        "expected EXECABORT spelling, got {:?}",
                        String::from_utf8_lossy(e)
                    ),
                    other => panic!("expected EXECABORT error, got {other:?}"),
                }
            }
        }
        other => panic!("expected Success(vec of EXECABORT), got {other:?}"),
    }

    // Store == pre-transaction snapshot: a unchanged, b and c absent.
    assert_eq!(
        get(&mut worker, "a").await,
        Response::Bulk(Some(Bytes::from_static(b"orig-a")))
    );
    assert_eq!(get(&mut worker, "b").await, Response::Bulk(None));
    assert_eq!(get(&mut worker, "c").await, Response::Bulk(None));

    // FakeWalLog shows no post-failure appends for b/c.
    let log = FakeWalRegistry::log(shard_id).expect("fake log registered");
    assert!(
        log.effects()
            .iter()
            .all(|e| e.key.as_deref() != Some(&b"b"[..]) && e.key.as_deref() != Some(&b"c"[..])),
        "no WAL append should exist for rolled-back keys"
    );
}
