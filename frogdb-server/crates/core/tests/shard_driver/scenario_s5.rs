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
