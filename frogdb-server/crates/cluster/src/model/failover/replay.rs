//! Counterexamples the failover model found, replayed deterministically
//! against the real state machine.
//!
//! Same discipline as [the handoff model's replays](crate::model::replay): a
//! model counterexample is only worth something if it survives translation, so
//! every discovery is re-stated here as a plain sequence of
//! [`ClusterState::apply_command`] calls over per-node states, with no
//! `stateright` in the loop. Both tests below assert the exposure is **still
//! present**; closing it turns them red, which is the prompt to flip the
//! assertion rather than let a dead test pass.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use super::{Failover, stranded_scope, unjustified_promotion_scope};
use crate::state::ClusterState;
use crate::types::{ClusterCommand, ClusterSnapshot, NodeId};

const PRIMARY: NodeId = 1;
const SUCCESSOR: NodeId = 2;
const WITNESS: NodeId = 3;
const SLOT: u16 = 0;

/// One node's replicated state, plus how far down the log it has applied.
struct Node {
    state: ClusterState,
    applied: usize,
}

impl Node {
    fn new(id: NodeId, base: &ClusterSnapshot) -> Self {
        Self {
            state: ClusterState::from_snapshot(base.clone(), Arc::new(AtomicU64::new(id))),
            applied: 0,
        }
    }

    /// Catch this node up to the end of `log`. Nodes that are *not* caught up
    /// are the whole point of both replays.
    fn catch_up(&mut self, log: &[ClusterCommand]) {
        while self.applied < log.len() {
            let _ = self.state.apply_command(log[self.applied].clone());
            self.applied += 1;
        }
    }

    fn view(&self) -> Arc<ClusterSnapshot> {
        self.state.snapshot()
    }

    /// The control flow of `FailureDetector::trigger_auto_failover`, reading
    /// this node's *own* snapshot: fail over only a primary, and only onto one
    /// of its replicas. Returns the command it would propose, or `None` where
    /// production returns early.
    fn plan_failover(&self, failed: NodeId) -> Option<ClusterCommand> {
        let view = self.view();
        if !view.nodes.get(&failed)?.is_primary() {
            return None;
        }
        let successor = view.get_replicas(failed).first()?.id;
        Some(ClusterCommand::Failover {
            old_primary_id: failed,
            new_primary_id: successor,
            force: true,
        })
    }
}

/// Append to the replicated log and apply it everywhere it is *not* being held
/// back. Standing in for Raft: the entry is committed, and every node in
/// `caught_up` applies it immediately.
fn commit(log: &mut Vec<ClusterCommand>, cmd: ClusterCommand, caught_up: &mut [&mut Node]) {
    log.push(cmd);
    for node in caught_up {
        node.catch_up(log);
    }
}

/// A failover the cluster misses is never retried, so the slots come to rest
/// on a node the cluster itself has flagged FAIL.
///
/// Discovered by
/// `model::failover::tests::a_slot_strands_on_a_primary_the_cluster_has_failed`.
/// Filed as
/// `.scratch/cluster-correctness/issues/open/18-a-missed-failover-is-never-retried.md`.
///
/// `reconcile_topology` is level-triggered, but only on the disagreement
/// between a local health verdict and the replicated `fail` flag; the
/// automatic failover hangs off the `MarkNodeFailed` write that resolves that
/// disagreement. Once the flag is set, the verdict and the flag agree forever
/// and no further pass writes anything — so a `trigger_auto_failover` that
/// returned early, for any reason, has thrown the failover away permanently.
/// Here the reason is the most ordinary one available: the proposer had not
/// yet applied the *previous* failover, so its own snapshot still called the
/// new primary a replica.
#[test]
fn a_missed_failover_leaves_the_slot_on_a_failed_primary() {
    let base = &*Failover::new(stranded_scope()).base();
    let mut log: Vec<ClusterCommand> = Vec::new();
    // Node 1 is the leader while it is a member; node 2 takes over when the
    // failover removes it. Node 3 is the detector, and lags.
    let mut leader = Node::new(PRIMARY, base);
    let mut successor = Node::new(SUCCESSOR, base);
    let mut detector = Node::new(WITNESS, base);

    // Node 3 crosses node 1's failure threshold and reconciles the verdict in.
    commit(
        &mut log,
        ClusterCommand::MarkNodeFailed { node_id: PRIMARY },
        &mut [&mut leader, &mut detector],
    );

    // The write came back Ok, so `mark_node_failed` triggers the failover.
    let failover = detector
        .plan_failover(PRIMARY)
        .expect("node 1 is a primary with replicas");
    commit(&mut log, failover, &mut [&mut leader, &mut successor]);
    assert_eq!(
        successor.view().slot_assignment.get(&SLOT),
        Some(&SUCCESSOR),
        "the slot moved to node 2, and node 1 was removed by the forced failover"
    );

    // Node 3 has not applied that entry — it is still catching up when node 2
    // crosses its failure threshold in turn, so it reconciles that verdict in
    // off a snapshot that predates the failover.
    assert!(
        detector.view().nodes[&SUCCESSOR].is_replica(),
        "node 3's snapshot still predates the failover"
    );
    commit(
        &mut log,
        ClusterCommand::MarkNodeFailed { node_id: SUCCESSOR },
        &mut [&mut successor],
    );

    // That write succeeded too, so `trigger_auto_failover` runs — and returns
    // early, because on *node 3's* snapshot node 2 is a replica, not a primary.
    assert!(
        detector.plan_failover(SUCCESSOR).is_none(),
        "the stale snapshot makes the proposer give up"
    );

    // Node 3 catches up. Every node now agrees, every verdict matches every
    // flag, and `reconcile_topology` has nothing left to write — this state is
    // final.
    detector.catch_up(&log);
    let view = detector.view();
    assert_eq!(view.slot_assignment.get(&SLOT), Some(&SUCCESSOR));
    assert!(
        view.nodes[&SUCCESSOR].flags.fail
            && view
                .get_replicas(SUCCESSOR)
                .iter()
                .any(|r| r.id == WITNESS && !r.flags.fail),
        "KNOWN EXPOSURE (issue 18): slot {SLOT} has come to rest on node {SUCCESSOR}, which the \
         cluster has flagged FAIL, while healthy node {WITNESS} replicates it and nothing will \
         ever retry the failover. If this assertion starts failing, the exposure is closed — \
         flip it to assert the slot moved, and close the issue."
    );
}

/// A forced failover naming a primary that has already left promotes its
/// successor anyway, out of a replication link that is still live.
///
/// Discovered by `model::failover::tests::a_promotion_can_move_nothing`. Filed
/// as
/// `.scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md`.
///
/// `force` exists to waive the check that the old primary is still a member —
/// that is the point of it, since the node being failed over is usually gone.
/// But the waiver is unconditional, so the command cannot tell "the old primary
/// is gone because it died" from "the old primary is gone because someone else
/// already failed it over". In the second case the successor inherits no slots
/// and is promoted for nothing: it detaches from the primary that is actually
/// feeding it, that primary loses its only replica, and the cluster's highest
/// config epoch lands on a node that owns no slots at all.
#[test]
fn a_forced_failover_promotes_a_node_that_inherits_nothing() {
    let base = &*Failover::new(unjustified_promotion_scope()).base();
    let mut log: Vec<ClusterCommand> = Vec::new();
    let mut leader = Node::new(PRIMARY, base);
    let mut detector = Node::new(SUCCESSOR, base);
    let mut lagging = Node::new(WITNESS, base);

    commit(
        &mut log,
        ClusterCommand::MarkNodeFailed { node_id: PRIMARY },
        &mut [&mut leader, &mut detector],
    );
    let failover = detector
        .plan_failover(PRIMARY)
        .expect("node 1 is a primary with replicas");
    commit(&mut log, failover, &mut [&mut leader, &mut detector]);

    // Node 3 is now node 2's replica — but has not applied any of it, so it
    // still believes node 1 is its primary.
    assert_eq!(
        detector.view().nodes[&WITNESS].primary_id,
        Some(SUCCESSOR),
        "the failover reparented node 3 onto node 2"
    );
    let stale_primary = lagging.view().nodes[&WITNESS]
        .primary_id
        .expect("node 3 is a replica");
    assert_eq!(stale_primary, PRIMARY, "node 3's snapshot is stale");

    // `CLUSTER FAILOVER FORCE` on node 3: the admin path reads the local
    // snapshot for the primary to take over from.
    commit(
        &mut log,
        ClusterCommand::Failover {
            old_primary_id: stale_primary,
            new_primary_id: WITNESS,
            force: true,
        },
        &mut [&mut leader, &mut detector, &mut lagging],
    );

    let view = detector.view();
    assert_eq!(
        view.slot_assignment.get(&SLOT),
        Some(&SUCCESSOR),
        "node 2 still owns the slot — the failover moved nothing"
    );
    assert!(
        view.nodes[&WITNESS].is_primary()
            && view.get_replicas(SUCCESSOR).is_empty()
            && view.nodes[&WITNESS].config_epoch > view.nodes[&SUCCESSOR].config_epoch,
        "KNOWN EXPOSURE (issue 19): node {WITNESS} was promoted out of a live replication link, \
         leaving node {SUCCESSOR} serving slot {SLOT} with no replica and the cluster's highest \
         config epoch on a node that owns nothing. If this assertion starts failing, the exposure \
         is closed — flip it to assert the command was refused, and close the issue."
    );
}
