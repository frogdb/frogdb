//! S3 — VLL phase-2/3 failure with sparse participants [2,5,7]. Real
//! VllCoordinator over a FaultSink; the driver's freedom is the per-shard
//! service schedule + sink-injected failures (C5 holds by construction). After
//! any abort every participant's lock table is clean and no partial write
//! landed on an aborted shard.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_vll::{LockMode, NoopMetricsSink, ScatterParticipant, ScatterRequest, VllCoordinator};

use super::harness::ShardDriver;
use super::sink::FaultSink;
use frogdb_core::shard::ScatterOp;

const PARTICIPANTS: [usize; 3] = [2, 5, 7];
const NUM_SHARDS: usize = 8;

fn mset_request(txid: u64) -> ScatterRequest<ScatterOp> {
    ScatterRequest {
        txid,
        mode: LockMode::Write,
        participants: PARTICIPANTS
            .iter()
            .map(|&sid| {
                let k = Bytes::from(format!("k{sid}"));
                ScatterParticipant {
                    shard_id: sid,
                    keys: vec![k.clone()],
                    operation: ScatterOp::MSet {
                        pairs: vec![(k, Bytes::from(format!("val{sid}")))],
                    },
                }
            })
            .collect(),
        timeout: Duration::from_millis(200),
        command: "MSET",
    }
}

/// Drive one scatter with the given fault sink to completion, servicing all
/// participant queues, then assert clean lock tables + no partial writes.
///
/// `aborted_shards` (meaningful only when `!expect_ok`) is the set of
/// participants the coordinator actually aborts — i.e. shards that never
/// completed `VllExecute`. Per `VllCoordinator::scatter`'s documented
/// contract (see `phase3_failure_aborts_remaining_holders_not_positions` in
/// `frogdb-vll/src/coordinator.rs`), a participant that already received
/// `VllExecute` before a later shard's dispatch failed is deliberately NOT
/// aborted — it releases its own locks and keeps its committed write. So
/// non-aborted participants are asserted to have their write COMMITTED,
/// not absent.
async fn run_and_assert_clean(
    mut driver: ShardDriver,
    fail_lock: HashSet<usize>,
    fail_execute: HashSet<usize>,
    withhold: HashSet<usize>,
    expect_ok: bool,
    aborted_shards: HashSet<usize>,
) {
    let sink = FaultSink::new(driver.senders(), fail_lock, fail_execute);
    let coordinator = Arc::new(VllCoordinator::new(sink, NoopMetricsSink));

    let coord = coordinator.clone();
    let handle = tokio::spawn(async move { coord.scatter(mset_request(1)).await });

    // Service every participant queue except withheld ones, until the
    // coordinator finishes. Withholding forces a LockTimeout on that shard.
    loop {
        let mut serviced = false;
        for &sid in &PARTICIPANTS {
            if withhold.contains(&sid) {
                continue;
            }
            serviced |= driver.pump_one(sid).await;
        }
        if handle.is_finished() && !serviced {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Drain any trailing abort messages the coordinator queued on the shards.
    for &sid in &PARTICIPANTS {
        driver.drain(sid).await;
    }

    let outcome = handle.await.unwrap();
    assert_eq!(
        outcome.is_ok(),
        expect_ok,
        "unexpected scatter outcome: {outcome:?}"
    );

    // Every participant's lock table is clean; no continuation lock.
    for &sid in &PARTICIPANTS {
        let lt = driver.lock_table_info(sid).await;
        assert!(
            lt.intents.is_empty(),
            "shard {sid} leaked lock intents after drain: {:?}",
            lt.intents
        );
        assert!(
            lt.continuation_lock.is_none(),
            "shard {sid} has a stray continuation lock"
        );
    }

    // On any abort: truly-aborted shards retain no partial VLL-EXEC write;
    // participants that already executed before the failure (never aborted,
    // per the coordinator's own contract) keep their committed write.
    if !expect_ok {
        for &sid in &PARTICIPANTS {
            let got = driver.execute(sid, "GET", &[&format!("k{sid}")]).await;
            if aborted_shards.contains(&sid) {
                assert_eq!(
                    got,
                    Response::Bulk(None),
                    "aborted scatter left a partial write on shard {sid}"
                );
            } else {
                assert_eq!(
                    got,
                    Response::Bulk(Some(Bytes::from(format!("val{sid}")))),
                    "shard {sid} already executed before the failure and must keep its committed write"
                );
            }
        }
    }

    // Non-participant shards are untouched.
    for sid in 0..NUM_SHARDS {
        if PARTICIPANTS.contains(&sid) {
            continue;
        }
        let lt = driver.lock_table_info(sid).await;
        assert!(lt.intents.is_empty(), "non-participant shard {sid} touched");
    }
}

#[tokio::test]
async fn s3_phase2_lock_failure_aborts_all_participants() {
    // Fail the lock dispatch on shard 5 → ShardUnavailable. Phase 3 (execute)
    // is never reached, so all real ids are aborted with no writes.
    run_and_assert_clean(
        ShardDriver::new(NUM_SHARDS),
        HashSet::from([5]),
        HashSet::new(),
        HashSet::new(),
        false,
        HashSet::from(PARTICIPANTS),
    )
    .await;
}

#[tokio::test]
async fn s3_phase3_execute_failure_aborts_remaining_participants() {
    // Fail the execute dispatch on shard 7 (last participant, dispatch order
    // [2,5,7]). Shards 2 and 5 already received VllExecute before 7's
    // dispatch failed, so per the coordinator's documented contract they are
    // NOT aborted — they release their own locks and keep their committed
    // writes. Only shard 7 (never executed) is genuinely aborted with no
    // write. See `phase3_failure_aborts_remaining_holders_not_positions` in
    // `frogdb-vll/src/coordinator.rs` for the same invariant pinned directly
    // on the coordinator.
    run_and_assert_clean(
        ShardDriver::new(NUM_SHARDS),
        HashSet::new(),
        HashSet::from([7]),
        HashSet::new(),
        false,
        HashSet::from([7]),
    )
    .await;
}

#[tokio::test]
async fn s3_withheld_service_forces_lock_timeout() {
    // Withhold service from shard 5: its ready_rx never resolves → LockTimeout
    // during phase 2, so phase 3 (execute) never runs for any participant;
    // all real ids aborted, tables clean, no writes.
    run_and_assert_clean(
        ShardDriver::new(NUM_SHARDS),
        HashSet::new(),
        HashSet::new(),
        HashSet::from([5]),
        false,
        HashSet::from(PARTICIPANTS),
    )
    .await;
}

#[tokio::test]
async fn s3_clean_run_commits_on_all_participants() {
    let mut driver = ShardDriver::new(NUM_SHARDS);
    let sink = super::sink::ChannelSink::new(driver.senders());
    let coordinator = Arc::new(VllCoordinator::new(sink, NoopMetricsSink));
    let coord = coordinator.clone();
    let handle = tokio::spawn(async move { coord.scatter(mset_request(1)).await });
    loop {
        let mut serviced = false;
        for &sid in &PARTICIPANTS {
            serviced |= driver.pump_one(sid).await;
        }
        if handle.is_finished() && !serviced {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(handle.await.unwrap().is_ok());
    for &sid in &PARTICIPANTS {
        assert_eq!(
            driver.execute(sid, "GET", &[&format!("k{sid}")]).await,
            Response::Bulk(Some(Bytes::from(format!("val{sid}"))))
        );
        assert!(driver.lock_table_info(sid).await.intents.is_empty());
    }
}
