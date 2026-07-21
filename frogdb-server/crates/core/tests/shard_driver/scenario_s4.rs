//! S4 — continuation-lock holder panic. The guard's Drop (coordinator.rs:136-142)
//! fires every release_tx on unwind; the shard observes it at a
//! ContinuationReleasePump step (event_loop.rs:88-91). After the pump every
//! shard's continuation lock is cleared and another conn can execute.

use std::sync::Arc;
use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_vll::{NoopMetricsSink, VllCoordinator};

use super::harness::ShardDriver;
use super::sink::ChannelSink;

const SHARDS: [usize; 2] = [0, 1];

#[tokio::test]
async fn s4_holder_panic_releases_locks_and_shard_resumes() {
    let mut driver = ShardDriver::new(2);
    let coordinator = Arc::new(VllCoordinator::new(
        ChannelSink::new(driver.senders()),
        NoopMetricsSink,
    ));

    // Spawn the continuation holder; its run() panics AFTER lock acquisition.
    let coord = coordinator.clone();
    let holder = tokio::spawn(async move {
        coord
            .acquire_continuation_and_run(
                1,       // txid
                99,      // conn_id (the lock owner)
                &SHARDS, // ascending
                Duration::from_secs(5),
                || async {
                    panic!("continuation holder panics after acquiring the lock");
                    #[allow(unreachable_code)]
                    Ok::<(), frogdb_vll::ContinuationError>(())
                },
            )
            .await
    });

    // Phase 1: service both shards so each acquires its continuation lock and
    // replies Ready, letting the holder proceed into run() and panic.
    // Pump until the lock is visible on both shards.
    for _ in 0..64 {
        driver.pump_one(0).await;
        driver.pump_one(1).await;
        tokio::task::yield_now().await;
        let l0 = driver.lock_table_info(0).await;
        let l1 = driver.lock_table_info(1).await;
        if l0.continuation_lock.is_some() && l1.continuation_lock.is_some() {
            break;
        }
    }
    assert!(driver.lock_table_info(0).await.continuation_lock.is_some());
    assert!(driver.lock_table_info(1).await.continuation_lock.is_some());

    // The holder task panicked → its JoinError is observed (not swallowed).
    let join = holder.await;
    assert!(
        join.is_err(),
        "the panicking holder task must surface a JoinError"
    );

    // The guard's release signals have not been pumped yet, so both shards
    // still hold conn 99's continuation lock. A third connection's Execute
    // must hit the gate's positive branch (`can_execute_during_lock`,
    // worker.rs) and observe the shard-busy error, not run its command.
    for &sid in &SHARDS {
        let resp = driver.execute_conn(sid, 7, "SET", &["busy", "nope"]).await;
        match resp {
            Response::Error(msg) => assert!(
                msg.starts_with(&b"ERR shard busy"[..]),
                "shard {sid} returned an unexpected error while locked: {msg:?}"
            ),
            other => {
                panic!("shard {sid} let conn 7 execute while conn 99 holds the lock: {other:?}")
            }
        }
    }

    // Guard Drop (on unwind) fired the release signals; pump each shard's
    // continuation-release seam in a permuted order.
    driver.pump_continuation_release(1).await;
    driver.pump_continuation_release(0).await;

    // Every shard cleared its continuation lock.
    for &sid in &SHARDS {
        assert!(
            driver
                .lock_table_info(sid)
                .await
                .continuation_lock
                .is_none(),
            "shard {sid} still holds a continuation lock after release pump"
        );
    }

    // A different connection can now execute (no -ERR shard busy).
    for &sid in &SHARDS {
        let resp = driver.execute_conn(sid, 7, "SET", &["after", "ok"]).await;
        assert!(
            matches!(resp, Response::Simple(_)),
            "shard {sid} still refuses a post-release write: {resp:?}"
        );
    }
}
