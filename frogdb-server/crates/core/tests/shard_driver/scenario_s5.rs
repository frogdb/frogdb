//! S5 — blocked XREADGROUP + key death (TTL vs DEL). DEL drains XREADGROUP
//! waiters to NOGROUP (blocking.rs:319-331); TTL/active-expiry currently does
//! NOT (F1 gap). After the fix both arms converge to identical NOGROUP
//! outcomes; a plain XREAD waiter stays blocked in both.

use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;

use super::harness::ShardDriver;
use frogdb_core::types::BlockingOp;

/// Register a blocked XREADGROUP waiter on `st` for group `g`, consumer `c`.
async fn block_xreadgroup(
    d: &mut ShardDriver,
    conn_id: u64,
) -> tokio::sync::oneshot::Receiver<Response> {
    d.block_wait(
        0,
        conn_id,
        vec![Bytes::from_static(b"st")],
        BlockingOp::XReadGroup {
            group: Bytes::from_static(b"g"),
            consumer: Bytes::from_static(b"c"),
            noack: false,
            count: None,
        },
        None,
    )
    .await
}

fn is_nogroup(resp: &Response) -> bool {
    matches!(resp, Response::Error(e) if e.starts_with(b"NOGROUP"))
}

async fn setup_stream_group(d: &mut ShardDriver) {
    let _ = d.execute(0, "XADD", &["st", "1-1", "f", "v"]).await;
    let _ = d.execute(0, "XGROUP", &["CREATE", "st", "g", "0"]).await;
}

#[tokio::test]
async fn s5_del_arm_drains_xreadgroup_to_nogroup() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let group_rx = block_xreadgroup(&mut d, 10).await;

    // A plain XREAD waiter alongside — must stay blocked.
    let mut xread_rx = d
        .block_wait(
            0,
            11,
            vec![Bytes::from_static(b"st")],
            BlockingOp::XRead {
                after_ids: vec![],
                count: None,
            },
            None,
        )
        .await;

    // DEL the stream → NOGROUP for the group waiter.
    let _ = d.execute_conn(0, 20, "DEL", &["st"]).await;

    let group_resp = group_rx.await.expect("group waiter replied");
    assert!(
        is_nogroup(&group_resp),
        "DEL arm must drain group waiter to NOGROUP, got {group_resp:?}"
    );

    // XREAD waiter still blocked (its receiver is not yet resolved).
    assert!(
        xread_rx.try_recv().is_err(),
        "plain XREAD waiter must stay blocked"
    );

    // Only the XREAD waiter remains.
    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 1,
        "XREADGROUP waiter drained; XREAD retained"
    );
}

#[tokio::test]
async fn s5_ttl_arm_drains_xreadgroup_to_nogroup_after_f1_fix() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let group_rx = block_xreadgroup(&mut d, 10).await;

    // TTL path: seed a short TTL, wait past it, then run the active-expiry tick.
    let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    d.tick_expiry(0);

    // After F1: the group waiter receives the SAME NOGROUP the DEL arm produces.
    let group_resp = group_rx.await.expect("group waiter replied after expiry");
    assert!(
        is_nogroup(&group_resp),
        "TTL arm must converge to NOGROUP after F1 fix, got {group_resp:?}"
    );

    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 0,
        "no group waiter left after expiry drain"
    );
    // Expiry index clean at quiesce.
    assert!(d.expiry_index_check(0).await.anomalies.is_empty());
}

/// GAP 1 (lazy-expiry parity proposal, `.scratch/concurrency-testing/proposals/
/// lazy-expiry-parity.md`): the LAZY-expiry path does NOT drain a blocked
/// XREADGROUP waiter. This is the S5 "lazy arm" the proposal calls for.
///
/// BUG: a blocked XREADGROUP waiter on a TTL-expired stream is stranded when the
/// key is removed by a value-reading command whose lookup runs through
/// `Store::get_with_expiry_check` (e.g. `GET`, and any expiry-aware value read).
/// That lazy purge (`get_with_expiry_check` -> `HashMapStore::check_and_delete_expired`,
/// store/hashmap.rs:421) physically removes the key but is wait-queue-ignorant:
/// unlike the active sweep (F1, `apply_expiry_effects` ->
/// `drain_stream_waiters_with_error`, event_loop.rs:204) and unlike `DEL`
/// (blocking.rs), it never touches the wait queue. The waiter is left parked
/// until its own BLOCK deadline.
///
/// NOTE (finding): not every "read" purges. Metadata probes that read the
/// expiry non-destructively — `TYPE` (`Store::key_type`), `EXISTS`/`TOUCH`
/// (`Store::exists_unexpired`), and the `LookupSpec::FirstKey` keyspace-hit seam
/// (execution.rs:144, also `exists_unexpired`) — are `&self` and do NOT
/// physically remove the key; only a value-read through `get_with_expiry_check`
/// (a `&mut` path) does. So this repro uses `GET`, which reaches the physical
/// purge; a `TYPE`/`EXISTS` here would leave the key present for the sweep.
///
/// Reference behavior (Redis 7/8, Valkey): a read that lazily expires the stream
/// (`lookupKeyReadWithFlags` -> `expireIfNeeded` -> `deleteExpiredKeyAndPropagate`
/// -> `signalKeyAsReady`) serves the blocked consumer `NOGROUP` on the next serve
/// cycle — the same outcome the active sweep and `DEL` produce. All key-death
/// paths converge; FrogDB's lazy path diverges.
///
/// Documents the CURRENT (buggy) behavior: after a lazy read purges the expired
/// stream, the group waiter's receiver is still unresolved and the wait queue
/// still holds it. When the lazy-purge effects seam lands, remove `#[ignore]`
/// and flip the assertions to `is_nogroup(...)` / `total_waiters == 0` (mirroring
/// the DEL and TTL arms above).
///
/// Real-path (turmoil) note: unlike the DEL / active-expiry arms, this gap is not
/// cleanly expressible over a live connection. Proving "waiter NOT drained"
/// requires observing the waiter fall through to its BLOCK *timeout*, but the
/// BLOCK deadline is a real `std::time::Instant`
/// (server `connection/blocking.rs:44`) slept on via
/// `tokio::time::sleep_until(deadline.into())` (coordinator.rs:85) — a
/// real->virtual `Instant` conversion whose resolution under turmoil's virtual
/// clock is undefined. This is exactly the dual-clock trap the F1 real-path test
/// (`xreadgroup_ttl_no_nogroup_realpath`) sidestepped by relying on the drain —
/// which is the very thing broken here. The shard driver observes the non-drain
/// directly and deterministically.
#[tokio::test]
#[ignore = "GAP 1 repro: lazy read does not drain XREADGROUP waiter (documents current buggy behavior; unfixed)"]
async fn s5_gap1_lazy_read_does_not_drain_xreadgroup() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let mut group_rx = block_xreadgroup(&mut d, 10).await;

    // Short TTL; elapse it. Crucially do NOT tick active expiry — only a lazy
    // read may remove the key.
    let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    // A third connection issues a LAZY value read that physically purges the
    // expired stream from the store (GET -> get_string -> get_typed ->
    // get_with_expiry_check -> check_and_delete_expired). The stream is a
    // wrong type for GET, but the expiry purge fires BEFORE the type
    // projection, so the key is uninstalled and GET replies nil.
    let _ = d.execute_conn(0, 20, "GET", &["st"]).await;

    // The lazy purge physically removed the stream.
    assert!(
        d.expiry_index_check(0).await.anomalies.is_empty(),
        "GAP 1: expiry index should be clean after the lazy purge removed the key"
    );

    // BUG: the blocked XREADGROUP waiter was NOT drained. Its receiver is still
    // unresolved and the wait queue still holds it. Redis/Valkey serve NOGROUP.
    assert!(
        group_rx.try_recv().is_err(),
        "GAP 1: lazy read left the XREADGROUP waiter parked (no NOGROUP drain)"
    );
    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 1,
        "GAP 1: waiter still queued after a lazy purge — the active sweep (F1) \
         and DEL both drain it here, the lazy read path does not"
    );
}

/// GAP 2 (lazy-expiry parity proposal): a lazy read racing ahead of the active
/// sweep NULLIFIES the F1 drain.
///
/// BUG: the F1 fix drains blocked XREADGROUP waiters only for keys the sweep
/// itself removes (`apply_expiry_effects` iterates `result.deleted_keys`,
/// event_loop.rs:198). If a lazy read purges the expired stream FIRST, the key
/// is already gone from the store when the sweep runs, so it never appears in
/// `deleted_keys` and the drain never fires — the waiter is stranded despite
/// active expiry being enabled. A read racing one tick ahead of the sweep
/// silently disables the F1 fix.
///
/// Reference behavior: Redis has no such hole — expiry effects (including waking
/// blocked clients) are applied at the point of removal, by whichever path
/// removes the key, so a lazy expiry and an active cycle produce identical
/// externally-visible effects.
///
/// Documents the CURRENT (buggy) behavior: after a lazy purge FOLLOWED BY a full
/// active-expiry tick, the group waiter is STILL parked. When the lazy-purge
/// effects seam lands, remove `#[ignore]` and flip to `is_nogroup(...)`.
#[tokio::test]
#[ignore = "GAP 2 repro: racing lazy read nullifies the F1 active-expiry drain (documents current buggy behavior; unfixed)"]
async fn s5_gap2_lazy_read_nullifies_active_drain() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let mut group_rx = block_xreadgroup(&mut d, 10).await;

    let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    // Lazy value read purges the stream BEFORE the sweep can see it (GET reaches
    // the physical `get_with_expiry_check` purge; a TYPE/EXISTS probe would not).
    let _ = d.execute_conn(0, 20, "GET", &["st"]).await;

    // The active-expiry sweep now runs — but the key is already gone from the
    // store, so it is NOT in `deleted_keys` and the F1 drain has nothing to act
    // on.
    d.tick_expiry(0);

    // BUG: the waiter is stranded even though active expiry ran — the racing
    // lazy read removed the only evidence the sweep's drain keys off.
    assert!(
        group_rx.try_recv().is_err(),
        "GAP 2: F1 active-expiry drain could not fire — the lazy read already \
         removed the key from the store before the sweep"
    );
    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 1,
        "GAP 2: waiter stranded — a lazy read racing ahead of the sweep silently \
         disabled the F1 drain"
    );
}
