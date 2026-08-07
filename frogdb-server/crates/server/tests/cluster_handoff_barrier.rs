//! Two-phase slot-handoff finalization (rework issue 02, phase 2a).
//!
//! `CLUSTER SETSLOT <slot> NODE <target>` no longer moves ownership in one
//! Raft entry. It prepares a handoff, the source fences the slot's writes and
//! drains its shard, the drain is confirmed through the log, and only then does
//! ownership move. These tests drive that path end to end against a real
//! three-node cluster.
//!
//! Two properties are worth the cost of a live cluster:
//!
//! * a write caught by the barrier is never acknowledged on the former owner —
//!   it parks, and when it wakes the slot already belongs to the target, so it
//!   is redirected; and
//! * a drain that never arrives aborts the finalization rather than proceeding.
//!   The migration record survives, the caller gets a retryable error, and
//!   `SETSLOT … STABLE` is still the way out.
//!
//! The second test also covers the awkward topology: the finalizer is neither
//! the migration source nor the Raft leader. The drain acknowledgement is a
//! replicated entry precisely so that node can see it.
#![cfg(not(feature = "turmoil"))]

use frogdb_protocol::Response;
use frogdb_test_harness::cluster_harness::ClusterTestHarness;
use frogdb_test_harness::cluster_helpers::{is_error, key_for_slot};
use std::time::Duration;

/// How long a probe waits before calling a command "parked".
const PARK_PROBE: Duration = Duration::from_millis(400);

/// How long a released command gets to produce its reply. Comfortably shorter
/// than the manual barrier the parking test arms, so a barrier that is never
/// released fails the test instead of quietly lapsing into a pass.
const RELEASE_WAIT: Duration = Duration::from_secs(5);

/// How long a manually armed barrier stays up. Long enough that only a real
/// release can end it inside [`RELEASE_WAIT`].
const MANUAL_BARRIER_MS: &str = "30000";

/// How long to wait for a replicated decision to become visible on a node.
const APPLY_WAIT: Duration = Duration::from_secs(5);

/// The first slot `owner` owns, according to its own applied topology.
fn slot_owned_by(harness: &ClusterTestHarness, owner: u64) -> u16 {
    let state = harness
        .node(owner)
        .expect("node")
        .cluster_state()
        .expect("cluster mode");
    (0..frogdb_core::CLUSTER_SLOTS)
        .find(|slot| state.get_slot_owner(*slot) == Some(owner))
        .expect("every node owns a slot after the bootstrap split")
}

/// Poll `node`'s applied state until `want` holds, or fail.
async fn wait_until(
    harness: &ClusterTestHarness,
    node: u64,
    what: &str,
    want: impl Fn(&frogdb_core::ClusterState) -> bool,
) {
    let state = harness
        .node(node)
        .expect("node")
        .cluster_state()
        .expect("cluster mode")
        .clone();
    let deadline = tokio::time::Instant::now() + APPLY_WAIT;
    while tokio::time::Instant::now() < deadline {
        if want(&state) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what} on node {node}");
}

// FM-CLUSTER-092
/// The write the barrier exists for. A `SET` on the migrating slot parks on the
/// source, the finalization runs to completion underneath it, and when the
/// write wakes the slot belongs to the target — so it is redirected instead of
/// being applied to a node that no longer owns it.
///
/// The barrier is pre-armed by hand (`DEBUG PAUSE-SLOT`) rather than by racing
/// the ~100 ms one the handoff arms: the property under test is what a parked
/// write observes on release, not whether a test can hit a 100 ms window.
/// Everything after the arm — prepare, drain, confirm, complete, release — is
/// the production path.
#[tokio::test]
async fn a_write_parked_by_the_barrier_wakes_up_redirected() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness.wait_for_leader(APPLY_WAIT).await.unwrap();

    let node_ids = harness.node_ids();
    let source_id = node_ids[0];
    let target_id = node_ids[1];
    let slot = slot_owned_by(&harness, source_id);
    let slot_arg = slot.to_string();
    let target_hex = harness.get_node_id_str(target_id).unwrap();
    let source_hex = harness.get_node_id_str(source_id).unwrap();

    let source = harness.node(source_id).unwrap();
    let target = harness.node(target_id).unwrap();

    assert!(
        !is_error(
            &source
                .send("CLUSTER", &["SETSLOT", &slot_arg, "MIGRATING", &target_hex])
                .await
        ),
        "SETSLOT MIGRATING should open the migration"
    );
    assert!(
        !is_error(
            &target
                .send("CLUSTER", &["SETSLOT", &slot_arg, "IMPORTING", &source_hex])
                .await
        ),
        "SETSLOT IMPORTING should be accepted on the target"
    );

    // Arm the barrier by hand and park a write behind it.
    assert_eq!(
        source
            .send("DEBUG", &["PAUSE-SLOT", &slot_arg, MANUAL_BARRIER_MS])
            .await,
        Response::ok()
    );
    let key = key_for_slot(slot);
    let mut probe = source.connect().await;
    probe.send_only(&["SET", &key, "v"]).await;
    assert!(
        probe.read_response(PARK_PROBE).await.is_none(),
        "the write should be parked by the barrier on its slot"
    );

    // Finalize underneath the parked write. `CLUSTER` is exempt from slot
    // pauses, so this is not parked by the barrier it will end.
    let finalize = tokio::time::timeout(
        RELEASE_WAIT,
        source.send("CLUSTER", &["SETSLOT", &slot_arg, "NODE", &target_hex]),
    )
    .await
    .expect("SETSLOT NODE was parked by the barrier it finalizes");
    assert_eq!(
        finalize,
        Response::ok(),
        "the two-phase finalization should succeed with a live source and shard"
    );

    // The parked write wakes because the handoff released the barrier — nothing
    // else did — and by then the slot has moved.
    let woken = probe
        .read_response(RELEASE_WAIT)
        .await
        .expect("the parked write never woke: the handoff did not release the barrier");
    match woken {
        Response::Error(msg) => {
            let text = String::from_utf8_lossy(&msg).to_string();
            assert!(
                text.starts_with("MOVED "),
                "the parked write should be redirected to the new owner, got: {text}"
            );
        }
        other => panic!("the parked write was acknowledged on the former owner: {other:?}"),
    }

    wait_until(&harness, source_id, "ownership to move to the target", |s| {
        s.get_slot_owner(slot) == Some(target_id)
    })
    .await;
    wait_until(&harness, source_id, "the migration record to clear", |s| {
        s.get_slot_migration(slot).is_none()
    })
    .await;

    harness.shutdown_all().await;
}

// FM-CLUSTER-091, FM-CLUSTER-087
/// A drain that never arrives aborts the finalization. The source is killed
/// while its migration is open, so nothing on that node can arm a barrier or
/// answer a drain; the finalizer's budget lapses, it aborts the handoff, and it
/// answers a retryable error. Ownership must not move and the migration record
/// must survive — `SETSLOT … STABLE` is still the operator's way out.
///
/// The finalizer here is deliberately neither the source nor the Raft leader,
/// which is the case the replicated drain acknowledgement exists for: a
/// point-to-point ack would be invisible to this node.
#[tokio::test]
async fn a_source_that_cannot_drain_aborts_the_finalization() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader = harness.wait_for_leader(APPLY_WAIT).await.unwrap();

    let node_ids = harness.node_ids();
    let others: Vec<u64> = node_ids.iter().copied().filter(|n| *n != leader).collect();
    let (source_id, finalizer_id) = (others[0], others[1]);
    let slot = slot_owned_by(&harness, source_id);
    let slot_arg = slot.to_string();
    let target_hex = harness.get_node_id_str(leader).unwrap();

    assert!(
        !is_error(
            &harness
                .node(source_id)
                .unwrap()
                .send("CLUSTER", &["SETSLOT", &slot_arg, "MIGRATING", &target_hex])
                .await
        ),
        "SETSLOT MIGRATING should open the migration"
    );
    wait_until(&harness, finalizer_id, "the migration to replicate", |s| {
        s.get_slot_migration(slot).is_some()
    })
    .await;

    // The source can no longer arm a barrier or answer a drain.
    harness.kill_node(source_id).await;

    let finalize = harness
        .node(finalizer_id)
        .unwrap()
        .send("CLUSTER", &["SETSLOT", &slot_arg, "NODE", &target_hex])
        .await;
    match &finalize {
        Response::Error(msg) => {
            let text = String::from_utf8_lossy(msg).to_string();
            assert!(
                text.starts_with("TRYAGAIN "),
                "an undrained handoff should answer a retryable error, got: {text}"
            );
        }
        other => panic!("finalization succeeded without a drain: {other:?}"),
    }

    // Nothing moved, and the abort left the migration intact for a retry.
    let state = harness
        .node(finalizer_id)
        .unwrap()
        .cluster_state()
        .unwrap()
        .clone();
    assert_eq!(
        state.get_slot_owner(slot),
        Some(source_id),
        "ownership must not move when the drain never arrived"
    );
    let migration = state
        .get_slot_migration(slot)
        .expect("the migration record must survive a failed finalization");
    wait_until(&harness, finalizer_id, "the prepared handoff to clear", |s| {
        s.get_slot_migration(slot)
            .is_some_and(|m| m.handoff.is_none())
    })
    .await;
    assert_eq!(migration.target_node, leader);

    // The operator's way out still works.
    assert!(
        !is_error(
            &harness
                .node(finalizer_id)
                .unwrap()
                .send("CLUSTER", &["SETSLOT", &slot_arg, "STABLE"])
                .await
        ),
        "SETSLOT STABLE should cancel a migration whose finalization failed"
    );
    wait_until(&harness, finalizer_id, "the migration to clear", |s| {
        s.get_slot_migration(slot).is_none()
    })
    .await;

    harness.shutdown_all().await;
}
