//! S8 — expiry sweep interleaved with EXEC (pin). The shard event loop is a
//! single task; message handling and expiry ticks are separate select arms,
//! each awaited to completion (research scenario 8). Interleaving is
//! message-granularity only: EXEC effects are always atomic and WATCH version
//! bumps are slot-granular and exactly-once (each written/expired key's slot
//! bumps once — proposal 18).

use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;
use proptest::prelude::*;

use super::generator::{Choice, Sender, Step, Tick, replay, schedule_strategy};
use super::harness::{ShardDriver, cmd};
use super::notify_capture::{all_keyevents_mask, assert_keyevents_consistent};
use frogdb_core::shard::WatchEntry;
use frogdb_core::shard::types::TransactionResult;

/// Deterministic pin: a multi-write EXEC is atomic even with an expiry tick and
/// an unrelated write permuted around it. Under slot-granular WATCH (proposal
/// 18) the committed EXEC bumps each written key's slot (`a`, `b`) exactly once,
/// and the sweep bumps the expired key's slot (`e`) exactly once — a bump per
/// affected slot, not one shared shard counter.
#[tokio::test]
async fn s8_exec_atomic_and_version_bumped_once() {
    let mut d = ShardDriver::new(1);
    // Seed a soon-to-expire key and two transaction targets.
    let _ = d.execute(0, "SET", &["a", "0"]).await;
    let _ = d.execute(0, "SET", &["b", "0"]).await;
    let _ = d.execute(0, "SET", &["e", "x"]).await;
    let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;

    // Capture keyspace notifications AFTER seeding (so the seeding SETs, emitted
    // while notifications were still disabled, do not pollute the capture) and
    // BEFORE the schedule — so exactly the schedule's `expired`/`set` keyevents
    // are observed, in emission order.
    let mut capture = d.capture_keyspace(0, 9001, &["__keyevent@0__:*"], all_keyevents_mask());

    // Per-key WATCH-version baselines (slot-granular): each key's own slot stamp.
    let a_before = d.key_version(0, "a").await;
    let b_before = d.key_version(0, "b").await;
    let e_before = d.key_version(0, "e").await;

    // Permuted around the EXEC: an expiry tick (removes e) then EXEC (a,b).
    tokio::time::sleep(Duration::from_millis(3)).await;
    d.tick_expiry(0).await; // one non-empty sweep → +1 ; emits `expired` for e

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["a", "1"]), cmd("SET", &["b", "1"])],
            vec![],
        )
        .await; // committed EXEC → +1 ; emits `set` for a then b

    assert!(matches!(result, TransactionResult::Success(_)));

    // Notification order consistent with the chosen serialization order
    // (sweep BEFORE EXEC): `expired e` precedes `set a`, `set b`. This is the
    // "keyspace notifications consistent with the chosen order" half of S8,
    // previously unpinned for lack of a capture seam (design doc S8 note).
    let events = capture.drain_keyevents();
    assert_keyevents_consistent(&events, &[("expired", b"e"), ("set", b"a"), ("set", b"b")])
        .expect("sweep-before-EXEC keyevent order");

    // Slot-granular version pins (read via `key_version`: a direct,
    // non-destructive `get_key_version`, so no read-path effect can contaminate
    // the counts). The committed EXEC bumped the `a` and `b` slots exactly once
    // each (once per written key, not per command re-run), and the sweep bumped
    // the `e` slot exactly once. `e`'s stamp survives its key's removal (slots
    // are permanent).
    assert_eq!(
        d.key_version(0, "a").await,
        a_before + 1,
        "the committed EXEC bumps a's slot exactly once"
    );
    assert_eq!(
        d.key_version(0, "b").await,
        b_before + 1,
        "the committed EXEC bumps b's slot exactly once"
    );
    assert_eq!(
        d.key_version(0, "e").await,
        e_before + 1,
        "the non-empty sweep bumps e's slot exactly once"
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

/// S8 notification-order consistency: the *same* sweep-vs-EXEC events, run in
/// two different serialization orders, must produce keyevent streams that each
/// match their order — the capture seam makes the ordering observable, and the
/// checker asserts "notifications consistent with the chosen order" (the half
/// of S8 the design doc left unpinned for want of a capture seam). Two
/// independent drivers, one per order.
#[tokio::test]
async fn s8_notifications_consistent_with_serialization_order() {
    // --- Order A: sweep BEFORE EXEC → expired e, then set a, set b. ---
    {
        let mut d = ShardDriver::new(1);
        let _ = d.execute(0, "SET", &["a", "0"]).await;
        let _ = d.execute(0, "SET", &["b", "0"]).await;
        let _ = d.execute(0, "SET", &["e", "x"]).await;
        let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;
        let mut capture = d.capture_keyspace(0, 9101, &["__keyevent@0__:*"], all_keyevents_mask());
        tokio::time::sleep(Duration::from_millis(3)).await;

        d.tick_expiry(0).await; // sweep first
        let _ = d
            .exec_transaction(
                0,
                1,
                vec![cmd("SET", &["a", "1"]), cmd("SET", &["b", "1"])],
                vec![],
            )
            .await;

        let events = capture.drain_keyevents();
        assert_keyevents_consistent(&events, &[("expired", b"e"), ("set", b"a"), ("set", b"b")])
            .expect("order A (sweep→EXEC)");
    }

    // --- Order B: EXEC BEFORE sweep → set a, set b, then expired e. ---
    {
        let mut d = ShardDriver::new(1);
        let _ = d.execute(0, "SET", &["a", "0"]).await;
        let _ = d.execute(0, "SET", &["b", "0"]).await;
        let _ = d.execute(0, "SET", &["e", "x"]).await;
        let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;
        let mut capture = d.capture_keyspace(0, 9102, &["__keyevent@0__:*"], all_keyevents_mask());
        tokio::time::sleep(Duration::from_millis(3)).await;

        // EXEC first (it touches a/b only, so e is not lazily purged here)...
        let _ = d
            .exec_transaction(
                0,
                1,
                vec![cmd("SET", &["a", "1"]), cmd("SET", &["b", "1"])],
                vec![],
            )
            .await;
        // ...then the explicit sweep removes the now-expired e.
        d.tick_expiry(0).await;

        let events = capture.drain_keyevents();
        assert_keyevents_consistent(&events, &[("set", b"a"), ("set", b"b"), ("expired", b"e")])
            .expect("order B (EXEC→sweep)");
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// Permute [EXEC, Execute, ExpiryTick, WaiterTimeoutTick] via the shared
    /// scheduler, with a genuinely-TTL'd key `w` in play so `Tick::Expiry`
    /// does real sweep work in some interleavings (not the inert no-op it was
    /// before this fix).
    ///
    /// WATCH is now SLOT-granular (proposal 18): `check_watches`/`get_key_version`
    /// compare the watched key `a`'s own Hash Slot version. `w` (a distinct,
    /// different-slot key) does real sweep work, but its removal bumps only `w`'s
    /// slot — so a `Tick::Expiry` of `w` NO LONGER aborts a watch on `a` (the
    /// proposal-18 fix; contrast `scenario_s2.rs::ActiveExpiryUnrelated`). To keep
    /// a genuine schedule-dependent abort branch reachable, sender 1's racing
    /// write targets `{a}c` — a sibling colocated on `a`'s slot via the `{a}` hash
    /// tag — so it bumps `a`'s slot. The EXEC (watching `a` at the pre-sleep
    /// version) therefore commits iff its dispatch beats sender 1's write to
    /// `{a}c`; a sweep of the different-slot `w` never tips it. Whichever happens,
    /// atomicity holds: `a`/`b` are either both the EXEC's committed values or
    /// both untouched — never one-updated.
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

            // A key that will genuinely have expired by replay time, so a
            // live `Tick::Expiry` in the schedule does real sweep work
            // (removal + version bump) rather than finding nothing to do.
            d.execute(0, "SET", &["w", "w0"]).await;
            d.execute(0, "PEXPIRE", &["w", "1"]).await;

            // Watch snapshot happens before the sleep, mirroring a real
            // client's WATCH a issued while everything is still live. Snapshot
            // `a`'s own slot version (slot-granular WATCH).
            let watch_version = d.key_version(0, "a").await;

            tokio::time::sleep(Duration::from_millis(3)).await; // w now past TTL

            // Sender 0: a multi-write EXEC (a:=9, b:=9), watching `a` at the
            // pre-sleep version. Sender 1: an unrelated single write on c.
            let mut senders = vec![
                Sender::new(vec![Step::ExecTransaction {
                    shard: 0,
                    conn_id: 1,
                    commands: vec![cmd("SET", &["a", "9"]), cmd("SET", &["b", "9"])],
                    watches: vec![WatchEntry {
                        key: Bytes::from_static(b"a"),
                        version: watch_version,
                        live_at_watch: true,
                    }],
                }]),
                Sender::new(vec![Step::Execute {
                    shard: 0,
                    conn_id: 2,
                    // Same slot as `a` (hash tag `{a}`), so this racing write
                    // bumps a's slot and can abort the watch — keeping both the
                    // commit and abort branches reachable under slot-granular WATCH.
                    command: cmd("SET", &["{a}c", "5"]),
                }]),
            ];
            // Keep only expiry/waiter ticks (ContinuationRelease not applicable).
            let sched: Vec<Choice> = schedule
                .into_iter()
                .filter(|c| !matches!(c, Choice::Tick { tick: Tick::ContinuationRelease, .. }))
                .collect();

            replay(&mut d, &mut senders, &sched, 1).await;

            // Outcome-conditional atomicity: whichever way the WATCH raced
            // (sender 1's same-slot write to `{a}c`, an inert different-slot
            // sweep of `w`, or neither), a/b are never partially updated.
            let a = d.execute(0, "GET", &["a"]).await;
            let b = d.execute(0, "GET", &["b"]).await;
            let committed = a == Response::Bulk(Some(Bytes::from_static(b"9")));
            if committed {
                prop_assert_eq!(
                    b, Response::Bulk(Some(Bytes::from_static(b"9"))),
                    "EXEC committed on a but not on b"
                );
            } else {
                prop_assert_eq!(
                    a, Response::Bulk(Some(Bytes::from_static(b"0"))),
                    "EXEC aborted but a changed anyway"
                );
                prop_assert_eq!(
                    b, Response::Bulk(Some(Bytes::from_static(b"0"))),
                    "EXEC aborted but b changed anyway"
                );
            }

            let mem = d.memory_check(0).await;
            prop_assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
            prop_assert!(d.expiry_index_check(0).await.anomalies.is_empty());
            Ok(())
        }).unwrap();
    }
}
