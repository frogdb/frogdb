//! S1 — dual-timeout race (pin). The lost-element race is closed in
//! `drive_satisfaction` (blocking.rs:243-261); 4b pins that closure under
//! permutation. Invariants: element conservation, empty wait queue at quiesce,
//! no delivery to a closed receiver.

use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::Response;
use proptest::prelude::*;

use super::harness::ShardDriver;
use frogdb_core::types::BlockingOp;

/// Run one permutation. `deadline_elapsed` selects A's deadline variant;
/// `drop_receiver` models A giving up (receiver-drop before UnregisterWait);
/// `push_first` selects whether B's LPUSH is serviced before or after the
/// waiter-timeout tick.
async fn run_case(deadline_elapsed: bool, drop_receiver: bool, push_first: bool) {
    let mut d = ShardDriver::new(1);

    let deadline = if deadline_elapsed {
        Some(Instant::now() - Duration::from_millis(1))
    } else {
        Some(Instant::now() + Duration::from_secs(30))
    };

    // Conn A blocks on BLPOP k.
    let rx_a = d
        .block_wait(
            0,
            10,
            vec![Bytes::from_static(b"k")],
            BlockingOp::BLPop,
            deadline,
        )
        .await;

    // Optionally model A giving up: drop the receiver so the shard sees a
    // closed channel (is_closed()).
    let mut rx_a = Some(rx_a);
    if drop_receiver {
        rx_a = None;
    }

    if push_first {
        // Conn B pushes the element, then a coarse waiter-timeout tick.
        let _ = d.execute_conn(0, 20, "LPUSH", &["k", "v"]).await;
        d.tick_waiter_timeout(0);
    } else {
        // Waiter-timeout tick first (GC pass), then the push.
        d.tick_waiter_timeout(0);
        let _ = d.execute_conn(0, 20, "LPUSH", &["k", "v"]).await;
    }

    // A optionally sends UnregisterWait (C3: only in Timeout/Unblocked
    // histories — modeled by the elapsed-deadline or dropped-receiver cases).
    if deadline_elapsed || drop_receiver {
        d.unregister_wait(0, 10).await;
    }

    // Determine whether A received the element.
    let a_got_element = match rx_a.take() {
        Some(rx) => matches!(rx.await, Ok(Response::Array(_))),
        None => false, // receiver dropped: cannot have observed a delivery
    };

    // Element conservation: v delivered to A exactly once XOR still in the list.
    let llen = d.execute(0, "LLEN", &["k"]).await;
    let list_has_v = matches!(llen, Response::Integer(n) if n == 1);
    assert!(
        a_got_element ^ list_has_v,
        "element conservation violated: a_got={a_got_element} list_has_v={list_has_v} \
         (elapsed={deadline_elapsed} drop={drop_receiver} push_first={push_first})"
    );

    // Wait queue empty at quiesce.
    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 0,
        "wait queue not drained at quiesce ({} waiters)",
        wq.total_waiters
    );
}

#[tokio::test]
async fn s1_all_deterministic_permutations() {
    for &elapsed in &[false, true] {
        for &drop_rx in &[false, true] {
            for &push_first in &[false, true] {
                run_case(elapsed, drop_rx, push_first).await;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    #[test]
    fn prop_s1_element_conserved(
        elapsed in any::<bool>(),
        drop_rx in any::<bool>(),
        push_first in any::<bool>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(run_case(elapsed, drop_rx, push_first));
    }
}
