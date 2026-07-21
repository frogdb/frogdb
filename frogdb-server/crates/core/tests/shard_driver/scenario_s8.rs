//! S8 — expiry sweep interleaved with EXEC (pin). The shard event loop is a
//! single task; message handling and expiry ticks are separate select arms,
//! each awaited to completion (research scenario 8). Interleaving is
//! message-granularity only: EXEC effects are always atomic and version bumps
//! are exactly-once (per committed EXEC + per non-empty sweep).

use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;
use proptest::prelude::*;

use super::generator::{Choice, Sender, Step, Tick, replay, schedule_strategy};
use super::harness::{ShardDriver, cmd};
use frogdb_core::shard::types::TransactionResult;

/// Deterministic pin: a multi-write EXEC is atomic even with an expiry tick and
/// an unrelated write permuted around it; the version bumps exactly once for
/// the committed EXEC.
#[tokio::test]
async fn s8_exec_atomic_and_version_bumped_once() {
    let mut d = ShardDriver::new(1);
    // Seed a soon-to-expire key and two transaction targets.
    let _ = d.execute(0, "SET", &["a", "0"]).await;
    let _ = d.execute(0, "SET", &["b", "0"]).await;
    let _ = d.execute(0, "SET", &["e", "x"]).await;
    let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;

    let v_before = d.get_version(0).await;

    // Permuted around the EXEC: an expiry tick (removes e) then EXEC (a,b).
    tokio::time::sleep(Duration::from_millis(3)).await;
    d.tick_expiry(0); // one non-empty sweep → +1

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["a", "1"]), cmd("SET", &["b", "1"])],
            vec![],
        )
        .await; // committed EXEC → +1

    assert!(matches!(result, TransactionResult::Success(_)));

    // Version read FIRST — before any GETs — so read-path effects (including
    // the post-F3 lazy-purge bump, which Task 9 lands before this task) cannot
    // contaminate the count. Bumped exactly twice: one non-empty sweep + one
    // committed EXEC.
    let v_after = d.get_version(0).await;
    assert_eq!(
        v_after,
        v_before + 2,
        "expected exactly one sweep bump + one EXEC bump"
    );

    // EXEC effects atomic: both keys reflect the committed values.
    assert_eq!(
        d.execute(0, "GET", &["a"]).await,
        Response::Bulk(Some(Bytes::from_static(b"1")))
    );
    assert_eq!(
        d.execute(0, "GET", &["b"]).await,
        Response::Bulk(Some(Bytes::from_static(b"1")))
    );
    // Expired key gone (already swept — this GET triggers no lazy purge).
    assert_eq!(d.execute(0, "GET", &["e"]).await, Response::Bulk(None));

    // Quiesce probes clean.
    let mem = d.memory_check(0).await;
    assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
    assert!(d.expiry_index_check(0).await.anomalies.is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// Permute [EXEC, Execute, ExpiryTick, WaiterTimeoutTick] via the shared
    /// scheduler. Whatever the order, at quiesce: the EXEC's effects are atomic
    /// (both a and b share the committed value, never one-updated), and the
    /// memory/expiry-index probes are clean.
    #[test]
    fn prop_s8_exec_atomic_under_permutation(schedule in schedule_strategy(2, 1, 10)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut d = ShardDriver::new(1);
            d.execute(0, "SET", &["a", "0"]).await;
            d.execute(0, "SET", &["b", "0"]).await;

            // Sender 0: a multi-write EXEC (a:=9, b:=9). Sender 1: an unrelated
            // single write on c.
            let mut senders = vec![
                Sender::new(vec![Step::ExecTransaction {
                    shard: 0,
                    conn_id: 1,
                    commands: vec![cmd("SET", &["a", "9"]), cmd("SET", &["b", "9"])],
                    watches: vec![],
                }]),
                Sender::new(vec![Step::Execute {
                    shard: 0,
                    conn_id: 2,
                    command: cmd("SET", &["c", "5"]),
                }]),
            ];
            // Keep only expiry/waiter ticks (ContinuationRelease not applicable).
            let sched: Vec<Choice> = schedule
                .into_iter()
                .filter(|c| !matches!(c, Choice::Tick { tick: Tick::ContinuationRelease, .. }))
                .collect();

            replay(&mut d, &mut senders, &sched, 1).await;

            // Atomicity: a and b share the committed transaction value.
            let a = d.execute(0, "GET", &["a"]).await;
            let b = d.execute(0, "GET", &["b"]).await;
            prop_assert_eq!(a, b, "EXEC not atomic: a and b diverged");

            let mem = d.memory_check(0).await;
            prop_assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
            prop_assert!(d.expiry_index_check(0).await.anomalies.is_empty());
            Ok(())
        }).unwrap();
    }
}
