//! Counterexamples the model found, replayed deterministically against the
//! real state machine.
//!
//! A model counterexample is only worth something if it survives translation:
//! the model layer is the part that could be wrong, so every discovery is
//! re-stated here as a plain sequence of [`ClusterState::apply_command`] calls
//! over per-node states, with no `stateright` in the loop. If a future change
//! closes the exposure, this test fails and says so rather than quietly
//! becoming vacuous.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use super::{Handoff, unbounded_lag_scope};
use crate::state::ClusterState;
use crate::types::{ClusterCommand, ClusterEvent, ClusterSnapshot, NodeId};

const SLOT: u16 = 1;
const SOURCE: NodeId = 2;
const TARGET: NodeId = 3;
const COORDINATOR: NodeId = 1;
const BARRIER_MS: u64 = 1;
const LEASE_MS: u64 = 2;

/// One node: its replicated state, and the local write barrier it holds — the
/// two halves whose disagreement is the whole point of the replay.
struct Node {
    id: NodeId,
    state: ClusterState,
    /// Local wall clock at which `pause_slot`'s hold on [`SLOT`] lapses.
    barrier_until_ms: Option<u64>,
}

impl Node {
    fn new(id: NodeId, base: &ClusterSnapshot) -> Self {
        Self {
            id,
            state: ClusterState::from_snapshot(base.clone(), Arc::new(AtomicU64::new(id.into()))),
            barrier_until_ms: None,
        }
    }

    /// Apply one committed entry at local time `now_ms`, then run the
    /// source-side reaction (`plan_handoff_action`) over the events it emitted.
    fn apply(&mut self, cmd: ClusterCommand, now_ms: u64) {
        let Ok((_, events)) = self.state.apply_command(cmd) else {
            return;
        };
        for event in events {
            match event {
                ClusterEvent::SlotHandoffPrepared {
                    slot,
                    source_node,
                    barrier_ms,
                    ..
                } if slot == SLOT && source_node == self.id => {
                    self.barrier_until_ms = Some(now_ms + barrier_ms);
                }
                ClusterEvent::SlotHandoffReleased {
                    slot, source_node, ..
                } if slot == SLOT && source_node == self.id => {
                    self.barrier_until_ms = None;
                }
                _ => {}
            }
        }
    }

    /// Would this node accept a write for [`SLOT`] at `now_ms`? Routing reads
    /// the node's *own* replicated view, and the pause is what holds writes off
    /// across the finalization instant.
    fn admits_write(&self, now_ms: u64) -> bool {
        self.state.snapshot().slot_assignment.get(&SLOT) == Some(&self.id)
            && !self.barrier_until_ms.is_some_and(|until| now_ms < until)
    }
}

/// A source that has not yet applied `CompleteSlotMigration` keeps serving the
/// slot once its own barrier lapses, while the target already owns it.
///
/// Discovered by `model::tests::stale_source_admits_writes_after_ownership_moves`
/// with the bounded-apply-lag assumption withdrawn. Filed as
/// `.scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md`.
///
/// The design is sound *given* that a node applies a committed entry within
/// `HANDOFF_BARRIER_MS` of applying the prepare — `handoff_barrier.rs` notes
/// only that the local pause outlasts the window in which `Complete` is
/// admissible, which is the safe direction for the *proposer* skew but says
/// nothing about the source's own apply lag. This test pins the residual: it
/// asserts what the system does **today**, so closing the exposure flips the
/// final assertion rather than leaving a silently-passing test behind.
#[test]
fn stale_source_keeps_serving_after_the_target_takes_the_slot() {
    let model = Handoff::new(unbounded_lag_scope());
    let base = &*model.base();
    let mut coordinator = Node::new(COORDINATOR, base);
    let mut source = Node::new(SOURCE, base);
    let mut target = Node::new(TARGET, base);

    // t = 1 — the coordinator prepares; every node applies promptly.
    let prepare = ClusterCommand::PrepareSlotHandoff {
        slot: SLOT,
        source_node: SOURCE,
        target_node: TARGET,
        barrier_ms: BARRIER_MS,
        lease_ms: LEASE_MS,
        proposed_at_ms: 1,
    };
    for node in [&mut coordinator, &mut source, &mut target] {
        node.apply(prepare.clone(), 1);
    }
    let seq = coordinator.state.snapshot().migrations[&SLOT]
        .handoff
        .as_ref()
        .expect("prepared")
        .seq;
    assert_eq!(
        source.barrier_until_ms,
        Some(2),
        "the source arms its pause for barrier_ms from *local apply* time"
    );

    // t = 1 — the source's shard drains and confirms.
    let confirm = ClusterCommand::ConfirmSlotHandoffDrained { slot: SLOT, seq };
    for node in [&mut coordinator, &mut source, &mut target] {
        node.apply(confirm.clone(), 1);
    }

    // t = 1 — ownership moves, inside the barrier window as FM-CLUSTER-084
    // requires. The source is the one node that does not apply it promptly.
    let complete = ClusterCommand::CompleteSlotMigration {
        slot: SLOT,
        source_node: SOURCE,
        target_node: TARGET,
        proposed_at_ms: 1,
    };
    coordinator.apply(complete.clone(), 1);
    target.apply(complete, 1);

    assert!(target.admits_write(1), "the target owns the slot now");
    assert!(
        !source.admits_write(1),
        "inside the barrier window the stale source is still fenced"
    );

    // t = 2 — the source's local pause lapses. It has still not applied
    // `Complete`, so its own view says it owns the slot.
    assert!(
        source.state.snapshot().slot_assignment.get(&SLOT) == Some(&SOURCE),
        "the source has not applied the ownership move"
    );
    assert!(
        target.admits_write(2) && source.admits_write(2),
        "KNOWN EXPOSURE (issue 17): two nodes admit writes for slot {SLOT} at t=2. \
         If this assertion starts failing, the exposure is closed — flip it to \
         assert a single writer and close the issue."
    );
}
