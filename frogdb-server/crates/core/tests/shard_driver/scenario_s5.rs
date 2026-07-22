//! S5 — blocked XREADGROUP + key death (DEL, active-expiry, and lazy read). All
//! key-death paths drain XREADGROUP waiters to NOGROUP: DEL (blocking.rs), the
//! active sweep (F1, `apply_expiry_effects`), and — now closed — the lazy
//! read-path purge (`apply_lazy_purge_effects`, gaps 1 & 2). A plain XREAD
//! waiter stays blocked in all of them. The two lazy arms
//! (`regression_gap1_lazy_read_drains_xreadgroup`,
//! `regression_gap2_lazy_read_drains_before_sweep`) pin that a lazy value read
//! drains the waiter at the point of removal, so it no longer depends on a
//! later active sweep seeing the key.

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

/// Regression pin (gap 1, lazy-expiry parity proposal, `.scratch/
/// concurrency-testing/proposals/lazy-expiry-parity.md`): a third-party LAZY
/// value read that physically purges a TTL-expired stream now drains the blocked
/// XREADGROUP waiter to NOGROUP — the same outcome as DEL and the active sweep.
/// This is the S5 "lazy arm" the proposal calls for, now closed.
///
/// A blocked XREADGROUP waiter on a TTL-expired stream used to be stranded when
/// the key was removed by a value-reading command whose lookup runs through
/// `Store::get_with_expiry_check` (e.g. `GET`, and any expiry-aware value read).
/// That lazy purge (`get_with_expiry_check` -> `HashMapStore::check_and_delete_expired`,
/// store/hashmap.rs) physically removes the key; it now reports the removal to
/// the worker, which applies the same effects the active sweep (F1,
/// `apply_expiry_effects` -> `drain_stream_waiters_with_error`, event_loop.rs)
/// and `DEL` (blocking.rs) apply — the waiter is drained to NOGROUP at the point
/// of removal.
///
/// NOTE (finding): not every "read" purges. Metadata probes that read the
/// expiry non-destructively — `TYPE` (`Store::key_type`), `EXISTS`/`TOUCH`
/// (`Store::exists_unexpired`), and the `LookupSpec::FirstKey` keyspace-hit seam
/// (also `exists_unexpired`) — are `&self` and do NOT physically remove the key,
/// so they do not report a purge; only a value-read through
/// `get_with_expiry_check` (a `&mut` path) does. So this pin uses `GET`, which
/// reaches the physical purge; a `TYPE`/`EXISTS` here would leave the key present.
///
/// Reference behavior (Redis 7/8, Valkey): a read that lazily expires the stream
/// (`lookupKeyReadWithFlags` -> `expireIfNeeded` -> `deleteExpiredKeyAndPropagate`
/// -> `signalKeyAsReady`) serves the blocked consumer `NOGROUP` on the next serve
/// cycle — the same outcome the active sweep and `DEL` produce. All key-death
/// paths converge.
///
/// Real-path (turmoil) note: unlike the DEL / active-expiry arms, this outcome is
/// not cleanly expressible over a live connection — proving the pre-fix non-drain
/// required observing the waiter fall through to its BLOCK *timeout*, whose
/// deadline is a real `std::time::Instant` slept on via
/// `tokio::time::sleep_until` — a real->virtual `Instant` conversion whose
/// resolution under turmoil's virtual clock is undefined. The shard driver
/// observes the drain directly and deterministically.
#[tokio::test]
async fn regression_gap1_lazy_read_drains_xreadgroup() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let group_rx = block_xreadgroup(&mut d, 10).await;

    // Short TTL; elapse it. Crucially do NOT tick active expiry — only a lazy
    // read may remove the key. (Unit-test tokio clock is real, so a 3ms sleep
    // genuinely elapses the real-`Instant` TTL.)
    let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    // A third connection issues a LAZY value read that physically purges the
    // expired stream from the store (GET -> get_string -> get_typed ->
    // get_with_expiry_check -> check_and_delete_expired). The stream is a
    // wrong type for GET, but the expiry purge fires BEFORE the type
    // projection, so the key is uninstalled and GET replies nil.
    let _ = d.execute_conn(0, 20, "GET", &["st"]).await;

    // The lazy read drains the group waiter to the SAME NOGROUP the DEL / TTL
    // arms produce.
    let group_resp = group_rx
        .await
        .expect("group waiter replied after lazy read");
    assert!(
        is_nogroup(&group_resp),
        "gap 1: lazy read must drain the XREADGROUP waiter to NOGROUP, got {group_resp:?}"
    );
    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 0,
        "gap 1: waiter drained by the lazy read — converges with the active sweep (F1) and DEL"
    );
    // The lazy purge physically removed the stream.
    assert!(
        d.expiry_index_check(0).await.anomalies.is_empty(),
        "gap 1: expiry index should be clean after the lazy purge removed the key"
    );
}

/// Regression pin (gap 2, lazy-expiry parity proposal): a lazy read racing ahead
/// of the active sweep no longer NULLIFIES the drain — the drain now fires at the
/// point of removal, so a later empty sweep is irrelevant.
///
/// The F1 fix drains blocked XREADGROUP waiters only for keys the sweep itself
/// removes (`apply_expiry_effects` iterates `result.deleted_keys`). A lazy read
/// that purges the expired stream FIRST leaves the key already gone when the
/// sweep runs, so it never appears in `deleted_keys` and the sweep's drain has
/// nothing to act on. With the lazy-purge effects seam, that no longer strands
/// the waiter: the lazy read itself drains it to NOGROUP at the moment it removes
/// the key, so the subsequent sweep is a no-op and the waiter is already gone.
///
/// Reference behavior: Redis has no such hole — expiry effects (including waking
/// blocked clients) are applied at the point of removal, by whichever path
/// removes the key, so a lazy expiry and an active cycle produce identical
/// externally-visible effects.
#[tokio::test]
async fn regression_gap2_lazy_read_drains_before_sweep() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let group_rx = block_xreadgroup(&mut d, 10).await;

    let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    // Lazy value read purges the stream BEFORE the sweep can see it (GET reaches
    // the physical `get_with_expiry_check` purge; a TYPE/EXISTS probe would not).
    // The purge now drains the waiter to NOGROUP at the point of removal.
    let _ = d.execute_conn(0, 20, "GET", &["st"]).await;

    // The waiter was drained by the lazy read, not the sweep — proving the drain
    // happens at the point of removal. The subsequent sweep finds nothing.
    let group_resp = group_rx
        .await
        .expect("group waiter replied after lazy read");
    assert!(
        is_nogroup(&group_resp),
        "gap 2: the lazy read must drain the waiter to NOGROUP at the point of removal, got {group_resp:?}"
    );

    // The active-expiry sweep now runs — the key is already gone, so it is NOT
    // in `deleted_keys`; nothing depends on that anymore because the lazy read
    // already drained the waiter. This is a no-op here.
    d.tick_expiry(0);

    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 0,
        "gap 2: waiter already drained by the lazy read racing ahead of the sweep — \
         the drain no longer depends on the sweep seeing the key"
    );
}
