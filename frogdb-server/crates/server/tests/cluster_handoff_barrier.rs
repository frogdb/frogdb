//! Two-phase slot-handoff finalization (rework issue 02, phases 2a and 2b).
//!
//! `CLUSTER SETSLOT <slot> NODE <target>` no longer moves ownership in one
//! Raft entry. It prepares a handoff, the source fences the slot's writes and
//! drains its shard, the drain is confirmed through the log, and only then does
//! ownership move. These tests drive that path end to end against a real
//! three-node cluster.
//!
//! These properties are worth the cost of a live cluster:
//!
//! * a write caught by the barrier is never acknowledged on the former owner —
//!   it parks, and when it wakes the slot already belongs to the target, so it
//!   is redirected;
//! * the same holds for the two command shapes that reach execution by other
//!   routes — a `MULTI`/`EXEC` batch, which parks *after* its slot verdict was
//!   taken, and a Lua script, whose keys are declared rather than derived;
//! * a barrier on one slot does not park transactions that cannot reach it; and
//! * a drain that never arrives aborts the finalization rather than proceeding.
//!   The migration record survives, the caller gets a retryable error, and
//!   `SETSLOT … STABLE` is still the way out.
//!
//! The drain test also covers the awkward topology: the finalizer is neither
//! the migration source nor the Raft leader. The drain acknowledgement is a
//! replicated entry precisely so that node can see it.
//!
//! Every redirect assertion here is paired with a `DBSIZE` check on the former
//! owner. Telling the client `-MOVED` is only half the property; the other half
//! is that nothing landed on the node that stopped owning the slot.
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
///
/// The bootstrap node assigns the slot split through Raft *after* the cluster
/// elects a leader, so this polls rather than reading once.
async fn slot_owned_by(harness: &ClusterTestHarness, owner: u64) -> u16 {
    let state = harness
        .node(owner)
        .expect("node")
        .cluster_state()
        .expect("cluster mode")
        .clone();
    let deadline = tokio::time::Instant::now() + APPLY_WAIT;
    loop {
        if let Some(slot) =
            (0..frogdb_core::CLUSTER_SLOTS).find(|slot| state.get_slot_owner(*slot) == Some(owner))
        {
            return slot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {owner} never received a slot from the bootstrap split"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The first `count` slots `owner` owns, according to its own applied topology.
///
/// Two slots on one node is what the narrowing test needs: a barrier armed on
/// the first must not park a transaction that only touches the second.
async fn slots_owned_by(harness: &ClusterTestHarness, owner: u64, count: usize) -> Vec<u16> {
    let state = harness
        .node(owner)
        .expect("node")
        .cluster_state()
        .expect("cluster mode")
        .clone();
    let deadline = tokio::time::Instant::now() + APPLY_WAIT;
    loop {
        let slots: Vec<u16> = (0..frogdb_core::CLUSTER_SLOTS)
            .filter(|slot| state.get_slot_owner(*slot) == Some(owner))
            .take(count)
            .collect();
        if slots.len() == count {
            return slots;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {owner} never owned {count} slots from the bootstrap split"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// How many keys `node` holds locally, across every shard.
///
/// The acceptance question these tests ask is not "was the client told no?" but
/// "did anything land on the node that is about to stop owning the slot?".
/// `DBSIZE` is server-wide and keyless, so it answers without being routed,
/// redirected, or parked — and on a freshly bootstrapped cluster whose only
/// writes are the ones under test, a non-zero answer is exactly the orphaned
/// write the barrier exists to prevent.
async fn local_key_count(harness: &ClusterTestHarness, node: u64) -> i64 {
    match harness.node(node).unwrap().send("DBSIZE", &[]).await {
        Response::Integer(n) => n,
        other => panic!("DBSIZE should answer an integer, got: {other:?}"),
    }
}

/// Assert `reply` is a `-MOVED` redirect, with `what` naming the command for the
/// failure message.
fn assert_moved(reply: Response, what: &str) {
    match reply {
        Response::Error(msg) => {
            let text = String::from_utf8_lossy(&msg).to_string();
            assert!(
                text.starts_with("MOVED "),
                "{what} should be redirected to the new owner, got: {text}"
            );
        }
        other => panic!("{what} was acknowledged on the former owner: {other:?}"),
    }
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

/// Open a migration of `slot` from `source_id` to `target_id` and arm the
/// source's barrier by hand, returning the target's node-id string.
///
/// The hand-armed barrier (`DEBUG PAUSE-SLOT`) stands in for the ~100 ms one the
/// handoff arms: every test here is about what a *parked* command observes on
/// release, and racing a 100 ms window would test the test harness instead.
/// Everything after the arm — prepare, drain, confirm, complete, release — is
/// the production path.
async fn open_migration_and_arm_barrier(
    harness: &ClusterTestHarness,
    source_id: u64,
    target_id: u64,
    slot: u16,
) -> String {
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
    assert_eq!(
        source
            .send("DEBUG", &["PAUSE-SLOT", &slot_arg, MANUAL_BARRIER_MS])
            .await,
        Response::ok()
    );
    target_hex
}

/// Run the two-phase finalization of `slot` on `source_id`, asserting it
/// succeeds inside [`RELEASE_WAIT`].
///
/// `CLUSTER` is exempt from slot pauses, so this is not parked by the barrier it
/// will end — a timeout here means that exemption regressed.
async fn finalize_handoff(
    harness: &ClusterTestHarness,
    source_id: u64,
    slot: u16,
    target_hex: &str,
) {
    let slot_arg = slot.to_string();
    let finalize = tokio::time::timeout(
        RELEASE_WAIT,
        harness
            .node(source_id)
            .unwrap()
            .send("CLUSTER", &["SETSLOT", &slot_arg, "NODE", target_hex]),
    )
    .await
    .expect("SETSLOT NODE was parked by the barrier it finalizes");
    assert_eq!(
        finalize,
        Response::ok(),
        "the two-phase finalization should succeed with a live source and shard"
    );
}

// FM-CLUSTER-092
/// The write the barrier exists for. A `SET` on the migrating slot parks on the
/// source, the finalization runs to completion underneath it, and when the
/// write wakes the slot belongs to the target — so it is redirected instead of
/// being applied to a node that no longer owns it.
///
/// The barrier is pre-armed by hand — see
/// [`open_migration_and_arm_barrier`].
#[tokio::test]
async fn a_write_parked_by_the_barrier_wakes_up_redirected() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness.wait_for_leader(APPLY_WAIT).await.unwrap();

    let node_ids = harness.node_ids();
    let (source_id, target_id) = (node_ids[0], node_ids[1]);
    let slot = slot_owned_by(&harness, source_id).await;
    let target_hex = open_migration_and_arm_barrier(&harness, source_id, target_id, slot).await;

    let key = key_for_slot(slot);
    let mut probe = harness.node(source_id).unwrap().connect().await;
    probe.send_only(&["SET", &key, "v"]).await;
    assert!(
        probe.read_response(PARK_PROBE).await.is_none(),
        "the write should be parked by the barrier on its slot"
    );

    finalize_handoff(&harness, source_id, slot, &target_hex).await;

    // The parked write wakes because the handoff released the barrier — nothing
    // else did — and by then the slot has moved.
    let woken = probe
        .read_response(RELEASE_WAIT)
        .await
        .expect("the parked write never woke: the handoff did not release the barrier");
    assert_moved(woken, "the parked write");
    assert_eq!(
        local_key_count(&harness, source_id).await,
        0,
        "the write must leave nothing behind on the node that lost the slot"
    );

    wait_until(
        &harness,
        source_id,
        "ownership to move to the target",
        |s| s.get_slot_owner(slot) == Some(target_id),
    )
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
    let slot = slot_owned_by(&harness, source_id).await;
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
    wait_until(
        &harness,
        finalizer_id,
        "the prepared handoff to clear",
        |s| {
            s.get_slot_migration(slot)
                .is_some_and(|m| m.handoff.is_none())
        },
    )
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

// FM-CLUSTER-093 FM-CLUSTER-083
/// The same property as FM-CLUSTER-092, for a transaction rather than a single
/// write — and it exercises a different code path to get there.
///
/// A single write parks at the dispatch pipeline's `PauseGate`, *ahead* of slot
/// validation, so it re-enters validation on release for free. `EXEC` does not:
/// it is neither `WRITE`- nor `SCRIPT`-flagged, so `PauseGate` waves it through,
/// and the batch parks inside `handle_exec` — *after* the batch's slot verdict
/// has already been taken. Waking there and running the queued writes on the
/// verdict taken before the wait would acknowledge the whole transaction on a
/// node that no longer owns the slot. The EXEC algorithm re-validates instead,
/// and the redirect is the bare `-MOVED` rather than an array of per-command
/// replies, because the queue is gone by then (Redis's `discardTransaction` +
/// `clusterRedirectClient`).
#[tokio::test]
async fn an_exec_parked_by_the_barrier_wakes_up_redirected() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness.wait_for_leader(APPLY_WAIT).await.unwrap();

    let node_ids = harness.node_ids();
    let (source_id, target_id) = (node_ids[0], node_ids[1]);
    let slot = slot_owned_by(&harness, source_id).await;

    // Seed the key *before* the migration opens, for two reasons. It is what
    // lets the batch serve locally at all — once a migration is open the source
    // routes by key presence, and a batch naming a key that is not here is
    // `-ASK`ed at the target long before any barrier is consulted. And it makes
    // the transaction's effect observable from outside: the queued command is a
    // `DEL`, so `DBSIZE` on the former owner distinguishes "refused" from
    // "applied and then redirected" — which a `SET` would not.
    let key = key_for_slot(slot);
    let source = harness.node(source_id).unwrap();
    assert_eq!(source.send("SET", &[&key, "before"]).await, Response::ok());

    let target_hex = open_migration_and_arm_barrier(&harness, source_id, target_id, slot).await;

    // Queue the write. Queuing itself is not parked — the barrier gates
    // execution, not `MULTI`.
    let mut probe = source.connect().await;
    assert_eq!(probe.command(&["MULTI"]).await, Response::ok());
    assert_eq!(probe.command(&["DEL", &key]).await, Response::queued());

    probe.send_only(&["EXEC"]).await;
    assert!(
        probe.read_response(PARK_PROBE).await.is_none(),
        "a write transaction on the barriered slot should park"
    );

    finalize_handoff(&harness, source_id, slot, &target_hex).await;

    let woken = probe
        .read_response(RELEASE_WAIT)
        .await
        .expect("the parked EXEC never woke: the handoff did not release the barrier");
    assert_moved(woken, "the parked transaction");

    assert_eq!(
        local_key_count(&harness, source_id).await,
        1,
        "the transaction's DEL must not have been applied on the node that lost the slot"
    );

    harness.shutdown_all().await;
}

// FM-CLUSTER-094
/// The Lua acceptance criterion: a slot handoff that completes while a script is
/// in flight cannot leave that script's write on the former owner.
///
/// A script is the hardest case of the three because its keys are declared, not
/// derived — `EVAL <body> <numkeys> <key>…` — and because for a long time
/// nothing checked them at all: a bare `EVAL` ran to completion wherever it
/// landed, acknowledging writes the slot's real owner never saw
/// (FM-CLUSTER-030, fixed by hoisting `ClusterSlotValidation` ahead of
/// `ConnectionCommand`). This test is what keeps that fix honest across a
/// handoff: the script parks on the barrier, the finalization completes
/// underneath it, and on release it is redirected without ever having run.
///
/// The assertion that matters is the second one. Being told `-MOVED` is the
/// client's half; `DBSIZE == 0` on the former owner is the cluster's half, and
/// it is the half the barrier exists for.
#[tokio::test]
async fn a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness.wait_for_leader(APPLY_WAIT).await.unwrap();

    let node_ids = harness.node_ids();
    let (source_id, target_id) = (node_ids[0], node_ids[1]);
    let slot = slot_owned_by(&harness, source_id).await;
    let target_hex = open_migration_and_arm_barrier(&harness, source_id, target_id, slot).await;

    let key = key_for_slot(slot);
    let mut probe = harness.node(source_id).unwrap().connect().await;
    probe
        .send_only(&[
            "EVAL",
            "redis.call('SET', KEYS[1], 'from-the-script'); return 1",
            "1",
            &key,
        ])
        .await;
    assert!(
        probe.read_response(PARK_PROBE).await.is_none(),
        "a writing script on the barriered slot should park"
    );

    finalize_handoff(&harness, source_id, slot, &target_hex).await;

    let woken = probe
        .read_response(RELEASE_WAIT)
        .await
        .expect("the parked script never woke: the handoff did not release the barrier");
    assert_moved(woken, "the parked script");

    assert_eq!(
        local_key_count(&harness, source_id).await,
        0,
        "the script must not have written on the node that lost the slot"
    );

    harness.shutdown_all().await;
}

// FM-CLUSTER-096
/// A barrier on one slot must not park transactions that cannot reach it.
///
/// Phase 1 parked every write `EXEC` on every armed pause, because the
/// `TxnHost` seam handed the pause wait no queue: with no way to resolve the
/// batch to a slot, the only safe answer was "park". That is correct and
/// needlessly expensive — a finalization on one slot stalled every write
/// transaction on the node for the barrier's whole lifetime.
///
/// Here the barrier is armed on `barriered`, and the transaction touches only
/// `free`, a different slot on the same node. It must commit while the barrier
/// is still up. `PARK_PROBE` is the same budget the parking tests use to
/// conclude "parked", so a regression to node-wide parking fails here rather
/// than merely slowing the suite down.
#[tokio::test]
async fn an_exec_on_an_unbarriered_slot_runs_while_the_barrier_is_up() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness.wait_for_leader(APPLY_WAIT).await.unwrap();

    let node_ids = harness.node_ids();
    let source_id = node_ids[0];
    let slots = slots_owned_by(&harness, source_id, 2).await;
    let (barriered, free) = (slots[0], slots[1]);

    let source = harness.node(source_id).unwrap();
    assert_eq!(
        source
            .send(
                "DEBUG",
                &["PAUSE-SLOT", &barriered.to_string(), MANUAL_BARRIER_MS]
            )
            .await,
        Response::ok()
    );

    let key = key_for_slot(free);
    let mut probe = source.connect().await;
    assert_eq!(probe.command(&["MULTI"]).await, Response::ok());
    assert_eq!(probe.command(&["SET", &key, "v"]).await, Response::queued());

    probe.send_only(&["EXEC"]).await;
    let reply = probe
        .read_response(PARK_PROBE)
        .await
        .expect("a transaction touching no barriered slot must not park");
    assert_eq!(
        reply,
        Response::Array(vec![Response::ok()]),
        "the unbarriered transaction should have committed"
    );

    // And the barrier is still up, so this is a narrowing and not a disarm: a
    // transaction that *does* touch the barriered slot still parks.
    let barriered_key = key_for_slot(barriered);
    let mut blocked = source.connect().await;
    assert_eq!(blocked.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        blocked.command(&["SET", &barriered_key, "v"]).await,
        Response::queued()
    );
    blocked.send_only(&["EXEC"]).await;
    assert!(
        blocked.read_response(PARK_PROBE).await.is_none(),
        "a transaction touching the barriered slot must still park"
    );

    harness.shutdown_all().await;
}

/// Poll `node`'s `DBSIZE` until it reaches `want`, or fail naming `what`.
///
/// Asked of a replica as readily as of a primary: `DBSIZE` is keyless and
/// server-wide, so a replica answers it without a slot verdict, a redirect, or
/// a `READONLY` handshake — which makes it the one observable that reads "what
/// has this replica actually applied" without touching the routing layer under
/// test.
async fn wait_for_key_count(harness: &ClusterTestHarness, node: u64, want: i64, what: &str) {
    let deadline = tokio::time::Instant::now() + RELEASE_WAIT;
    loop {
        let seen = local_key_count(harness, node).await;
        if seen == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}: node {node} holds {seen} keys, wanted {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// FM-CLUSTER-097
/// The replication half of the barrier (rework issue 12).
///
/// The write barrier fences *acknowledgements*, not work: a write caught by it
/// can still apply to the source's keyspace before the fence refuses to answer
/// for it (FM-CLUSTER-095). Applied writes are broadcast, so without a hold on
/// the feed a replica of the losing node would receive, during the handover
/// window, writes the source's own clients were told to retry elsewhere. Redis
/// and Valkey close the same window by including `PAUSE_ACTION_REPLICA` in the
/// slot-migration pause — while it is up the primary stops flushing its replica
/// output buffers.
///
/// So: a replica attached across a loaded handoff must see *nothing* the source
/// applied while the barrier was armed, and must converge on all of it once the
/// handoff releases the barrier. The write used here is on a slot the barrier
/// does not cover, which is deliberate — the hold is node-wide (one monotonic
/// replication offset means a per-shard hold would put offsets on the wire out
/// of order), so an unbarriered slot's write is the sharpest probe of it: it
/// applies on the source with no parking at all, and the only thing that can
/// keep it off the wire is the hold.
#[tokio::test]
async fn the_barrier_holds_the_replica_feed_until_the_handoff_releases_it() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness.wait_for_leader(APPLY_WAIT).await.unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let (source_id, target_id) = (node_ids[0], node_ids[1]);
    let replica_id = harness.add_replica(source_id).await.unwrap();
    harness
        .wait_for_replication_link(source_id, 1, Duration::from_secs(20))
        .await
        .expect("the source never reported an attached replica");

    // Three slots on the source: one to barrier, and two to carry writes whose
    // *shipping* is the property under test.
    let slots = slots_owned_by(&harness, source_id, 3).await;
    let (barriered, before_slot, during_slot) = (slots[0], slots[1], slots[2]);
    let before_key = key_for_slot(before_slot);
    let during_key = key_for_slot(during_slot);

    // Control: with no barrier armed the link ships promptly. Without it the
    // later absence would be indistinguishable from a replica that never
    // attached, and the test would pass with the hold deleted.
    assert_eq!(
        harness
            .node(source_id)
            .unwrap()
            .send("SET", &[&before_key, "before"])
            .await,
        Response::ok()
    );
    wait_for_key_count(
        &harness,
        replica_id,
        1,
        "the pre-barrier write to replicate",
    )
    .await;

    let target_hex =
        open_migration_and_arm_barrier(&harness, source_id, target_id, barriered).await;

    // A write on an unbarriered slot: acknowledged and applied on the source...
    assert_eq!(
        harness
            .node(source_id)
            .unwrap()
            .send("SET", &[&during_key, "during"])
            .await,
        Response::ok(),
        "a write outside the barriered slot must not park"
    );
    assert_eq!(
        local_key_count(&harness, source_id).await,
        2,
        "the write should have applied on the source"
    );

    // ...and held off the wire for as long as the barrier is up. The control
    // above landed well inside this window, so an unchanged replica here is the
    // hold and not latency.
    let probe_deadline = tokio::time::Instant::now() + PARK_PROBE;
    while tokio::time::Instant::now() < probe_deadline {
        assert_eq!(
            local_key_count(&harness, replica_id).await,
            1,
            "the replica received a write the source applied under the handoff barrier"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The handoff completes, which releases the barrier — and with it the feed.
    finalize_handoff(&harness, source_id, barriered, &target_hex).await;

    // Nothing is dropped: writes held during the window ship once it ends.
    wait_for_key_count(
        &harness,
        replica_id,
        2,
        "the held write to ship after the barrier released",
    )
    .await;

    harness.shutdown_all().await;
}
