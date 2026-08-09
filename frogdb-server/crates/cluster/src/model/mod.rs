//! Explicit-state model of the two-phase slot handoff (`stateright`).
//!
//! # What this checks
//!
//! Slot ownership moves from a source to a target through four replicated
//! commands — `PrepareSlotHandoff`, `ConfirmSlotHandoffDrained`,
//! `AbortSlotHandoff`, `CompleteSlotMigration` — plus a source-local write
//! barrier armed off the `SlotHandoffPrepared` event. The handoff is correct
//! only if *no* interleaving of those commands with message loss, duplication,
//! apply lag and leader changes ever lets two nodes admit writes for the same
//! slot at the same instant. Unit tests can only assert the interleavings
//! someone thought to write down; this enumerates all of them inside a small
//! scope.
//!
//! # The transition function is production code
//!
//! The model never re-implements the state machine. Every step loads a node's
//! `ClusterSnapshot` back into a real [`ClusterState`] via
//! [`ClusterState::from_snapshot`], calls the production
//! [`ClusterState::apply_command`], and keeps the resulting snapshot. So the
//! thing being checked is `commands.rs` itself, and a change to an arm changes
//! the model's behaviour with no edit here. (It also runs every transition
//! through the snapshot restore seam, which is where FM-CLUSTER-100 — a
//! restore that drops `handoff_seq` re-mints a spent generation — would show
//! up; `handoff_seq_counter_survives_restore` is part of the seq property.)
//!
//! The model layer contributes only what Raft and the network contribute in
//! production:
//!
//! * a single replicated log, appended to by the current leader;
//! * per-node apply lag (a follower may sit arbitrarily far behind);
//! * leader changes (the new leader catches up before it may append);
//! * duplicated drain confirmations, lost/abandoned coordinator polls;
//! * a discrete wall clock that every node reads in common.
//!
//! The coordinator is a direct transcription of the *control flow* of
//! `SlotMigrationCoordinator::complete` (server crate) — prepare, poll for
//! *our own* prepare by proposer timestamp, poll for drained, complete;
//! abort-and-TRYAGAIN if the drain never lands; TRYAGAIN with **no** abort if
//! the prepare never becomes visible. The source-side barrier is a
//! transcription of `plan_handoff_action` (cluster-runtime): arm on a
//! `SlotHandoffPrepared` naming me as source, for `barrier_ms` measured from
//! *local apply time*; release on any `SlotHandoffReleased` naming me. Those
//! two are control flow, not state-machine logic, and they are what the model
//! exists to permute.
//!
//! # Scope
//!
//! 3 nodes, up to 2 slots, up to 3 attempts per slot, up to 2 duplicate drain
//! confirmations, up to 2 leader changes, `barrier_ms = 1` / `lease_ms = 2`
//! against a 2 ms horizon (per-config numbers in the budget table below).
//! Production uses 100 ms / 10000 ms; the state machine's predicates are pure
//! comparisons against `prepared_at_ms + k`, so only the ordering
//! `0 < barrier_ms < lease_ms` matters and the small values keep the clock from
//! multiplying the state space. Slot 0 migrates 1 -> 2 coordinated by its own
//! source; slot 1 migrates 2 -> 3 coordinated by node 1, a third party — so
//! node 2 is simultaneously the target of one handoff and the source of
//! another, and both a self-coordinated and a third-party-coordinated
//! finalization are covered (FM-CLUSTER-088, FM-CLUSTER-090).
//!
//! # Exploration budget (recorded per issue 10 / PRD §8 D1)
//!
//! Measured on a 10-core M-series laptop, BFS to exhaustion (every config
//! below terminates; none is depth- or time-truncated):
//!
//! | config | scope | unique states | depth | wall | where |
//! |---|---|---|---|---|---|
//! | `smoke_scope()` | 1 slot, 2 attempts, 1 dup ack, 1 leader change | 31 324 | 31 | 0.8 s (debug) | default suite |
//! | `unbounded_lag_scope()` | 1 slot, apply lag unbounded | 1 156 | 14 | 0.1 s (debug) | default suite |
//! | `cross_slot_scope()` | 2 slots, 1 attempt, 1 dup ack, 1 leader change | 1 306 692 | 30 | 8 s (release) | nightly, `just model-check` |
//! | `deep_scope()` | 1 slot, 3 attempts, 2 dup acks, 2 leader changes | 12 186 542 | 54 | 65 s (release) | nightly, `just model-check` |
//!
//! The two per-commit figures are debug numbers, because debug is what the
//! per-commit suite pays. The two full configs are `--release` only, and the
//! gap is not small: a full config left running unoptimized was still going
//! after ten minutes. The walls above are for an otherwise idle laptop; under
//! a parallel cargo build `deep_scope()` was observed at 147 s, so the nightly
//! job's budget is minutes, not seconds. Nothing is skipped by optimizing —
//! `debug_assert_clean` is `cfg(any(test, debug_assertions))`, so the
//! post-apply catalog check runs in the release *test* build too.
//!
//! **Where the small-scope hypothesis bends.** Breadth and depth do not
//! compose: two slots *with* retries blew past 25 s and 7 GB of resident state
//! without terminating, so the nightly runs the two configs side by side
//! instead of one product of them. Everything the model checks is covered by
//! one of the two; nothing is covered by both at once. `deep_scope()` alone
//! was holding ~3 GB resident halfway through its run, which is why the two
//! full runs are pinned to one at a time in `.config/nextest.toml`.
//!
//! The floors are asserted by the tests themselves (`MIN_*_STATES`), so a scope
//! change that quietly shrinks the explored space fails the build rather than
//! passing vacuously.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use stateright::{Model, Property};

use crate::state::ClusterState;
use crate::types::{ClusterCommand, ClusterEvent, ClusterSnapshot, NodeId, NodeInfo, SlotRange};

#[cfg(test)]
mod replay;
#[cfg(test)]
mod tests;

/// Nodes in the modelled cluster.
pub(crate) const NODES: usize = 3;

const NODE_IDS: [NodeId; NODES] = [1, 2, 3];

/// `(slot, source, target, coordinator)` for each modelled handoff.
///
/// Slot 0 is coordinated by its own source; slot 1 by a node that is neither
/// source nor target. Node 2 is the target of slot 0 and the source of slot 1.
pub(crate) const PLAN: [(u16, NodeId, NodeId, NodeId); 2] = [(0, 1, 2, 1), (1, 2, 3, 1)];

const fn idx(id: NodeId) -> usize {
    (id - 1) as usize
}

fn addr(id: NodeId, port_base: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port_base + id as u16))
}

/// How much of the space to explore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Scope {
    /// Which entries of [`PLAN`] are live, by index.
    pub plan: Vec<usize>,
    /// Source write-barrier window, in model milliseconds.
    pub barrier_ms: u64,
    /// Prepared-record lease, in model milliseconds. Must exceed `barrier_ms`.
    pub lease_ms: u64,
    /// The clock stops here, which is what makes the space finite.
    pub horizon_ms: u64,
    /// Finalization attempts per slot (an operator re-issuing `SETSLOT NODE`).
    pub max_attempts: u8,
    /// Extra drain confirmations per armed attempt beyond the first.
    pub dup_confirms: u8,
    /// Leader changes permitted along a path.
    pub max_leader_changes: u8,
    /// When true the clock may only advance once every node has applied the
    /// whole log — the design's unstated assumption that apply lag is far
    /// shorter than the barrier window. When false a follower may sit out an
    /// arbitrary number of milliseconds mid-handoff.
    pub bounded_apply_lag: bool,
}

/// A committed log entry, in the compact form the model hashes. Rendered to a
/// real [`ClusterCommand`] by [`Entry::command`] before every apply — the
/// enum is an encoding of *which* production command was proposed, never a
/// re-statement of what it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Entry {
    Prepare { slot: u16, at_ms: u64 },
    Confirm { slot: u16, seq: u64 },
    Abort { slot: u16, seq: u64 },
    Complete { slot: u16, at_ms: u64 },
}

impl Entry {
    fn plan(slot: u16) -> (u16, NodeId, NodeId, NodeId) {
        PLAN.iter()
            .copied()
            .find(|p| p.0 == slot)
            .expect("modelled slot")
    }

    pub(crate) fn command(self, scope: &Scope) -> ClusterCommand {
        match self {
            Entry::Prepare { slot, at_ms } => {
                let (_, source_node, target_node, _) = Self::plan(slot);
                ClusterCommand::PrepareSlotHandoff {
                    slot,
                    source_node,
                    target_node,
                    barrier_ms: scope.barrier_ms,
                    lease_ms: scope.lease_ms,
                    proposed_at_ms: at_ms,
                }
            }
            Entry::Confirm { slot, seq } => ClusterCommand::ConfirmSlotHandoffDrained { slot, seq },
            Entry::Abort { slot, seq } => ClusterCommand::AbortSlotHandoff { slot, seq },
            Entry::Complete { slot, at_ms } => {
                let (_, source_node, target_node, _) = Self::plan(slot);
                ClusterCommand::CompleteSlotMigration {
                    slot,
                    source_node,
                    target_node,
                    proposed_at_ms: at_ms,
                }
            }
        }
    }
}

/// The source's local write barrier for one slot, as `pause_slot` holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Armed {
    pub seq: u64,
    /// Local wall clock at which the pause lapses. Measured from *apply* time,
    /// which is necessarily at or after the proposer's `prepared_at_ms`.
    pub deadline_ms: u64,
    /// Drain confirmations already proposed for this arming.
    pub confirms: u8,
}

/// One node's runtime: how far it has applied, its resulting view, and the
/// barriers it currently holds.
#[derive(Clone, Debug)]
pub(crate) struct NodeRt {
    pub applied: usize,
    pub armed: BTreeMap<u16, Armed>,
    pub view: Arc<ClusterSnapshot>,
}

// `view` is excluded from the state's identity: it is a pure function of the
// log prefix `log[..applied]` applied to the (constant) initial snapshot, and
// both `log` and `applied` are part of the identity. Including it would only
// cost a deep hash of the whole cluster snapshot per node per state. The
// `apply_is_deterministic` property is what keeps that claim honest.
impl PartialEq for NodeRt {
    fn eq(&self, other: &Self) -> bool {
        self.applied == other.applied && self.armed == other.armed
    }
}
impl Eq for NodeRt {}
impl Hash for NodeRt {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.applied.hash(h);
        self.armed.hash(h);
    }
}

/// Where a finalization coordinator is in `SlotMigrationCoordinator::complete`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Coord {
    /// Ready to propose attempt `attempt`'s `PrepareSlotHandoff`.
    Idle { attempt: u8 },
    /// Prepare proposed; polling local state for *our* attempt by timestamp.
    AwaitPrepared { at_ms: u64, attempt: u8 },
    /// Our attempt is visible; polling for `drained`.
    AwaitDrained { seq: u64, attempt: u8 },
    /// `CompleteSlotMigration` proposed (accepted or refused).
    Done { attempt: u8 },
    /// Returned TRYAGAIN.
    GaveUp { attempt: u8 },
}

impl Coord {
    fn in_flight(self) -> bool {
        matches!(
            self,
            Coord::AwaitPrepared { .. } | Coord::AwaitDrained { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Action {
    /// Node `n` applies its next committed entry.
    Apply(usize),
    /// Slot `s`'s coordinator takes its next step.
    Coord(usize),
    /// Slot `s`'s coordinator exhausts its poll budget.
    CoordGivesUp(usize),
    /// Slot `s`'s source finishes its shard round trip and confirms the drain.
    Drain(usize),
    /// The operator re-issues finalization for slot `s`.
    Retry(usize),
    /// Node `n` becomes leader, catching up first.
    LeaderChange(usize),
    /// The shared wall clock advances one model millisecond.
    Tick,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Sys {
    pub log: Vec<Entry>,
    pub nodes: Vec<NodeRt>,
    pub coords: Vec<Coord>,
    pub now_ms: u64,
    pub leader: usize,
    pub leader_changes: u8,
    /// Handoff generations minted so far, in mint order.
    pub minted: Vec<u64>,
}

pub(crate) struct Handoff {
    pub scope: Scope,
    base: Arc<ClusterSnapshot>,
}

impl Handoff {
    pub(crate) fn new(scope: Scope) -> Self {
        assert!(scope.plan.iter().all(|&i| i < PLAN.len()));
        assert!(scope.barrier_ms < scope.lease_ms);
        assert!(
            scope.lease_ms <= scope.horizon_ms,
            "the horizon must outlive one lease or a prepared record can never lapse"
        );
        let base = Self::build_base(&scope);
        Self { scope, base }
    }

    /// The topology the model starts every node from. [`replay`] builds its
    /// deterministic re-runs from the same snapshot so a counterexample is
    /// replayed against the state the checker actually explored.
    pub(crate) fn base(&self) -> Arc<ClusterSnapshot> {
        Arc::clone(&self.base)
    }

    /// The topology every node starts from: three primaries, slot 0 on node 1,
    /// slot 1 on node 2, and a bulk-transfer migration open for each modelled
    /// slot. Built by the production state machine so the model can never
    /// start from a state the real one cannot reach.
    fn build_base(scope: &Scope) -> Arc<ClusterSnapshot> {
        let state = ClusterState::from_snapshot(
            ClusterSnapshot::default(),
            Arc::new(AtomicU64::new(NODE_IDS[0])),
        );
        for id in NODE_IDS {
            state
                .apply_command(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, addr(id, 7000), addr(id, 17000)),
                })
                .expect("AddNode");
        }
        for &(slot, source_node, _, _) in scope.plan.iter().map(|&i| &PLAN[i]) {
            state
                .apply_command(ClusterCommand::AssignSlots {
                    node_id: source_node,
                    slots: vec![SlotRange::new(slot, slot)],
                })
                .expect("AssignSlots");
        }
        for &(slot, source_node, target_node, _) in scope.plan.iter().map(|&i| &PLAN[i]) {
            state
                .apply_command(ClusterCommand::BeginSlotMigration {
                    slot,
                    source_node,
                    target_node,
                })
                .expect("BeginSlotMigration");
        }
        state.snapshot()
    }

    /// Apply node `n`'s next committed entry through the production state
    /// machine, then run the source-side barrier reaction over the events it
    /// emitted. Returns the minted generation if the entry was an accepted
    /// prepare.
    fn apply_next(&self, sys: &mut Sys, n: usize) -> Option<u64> {
        let now = sys.now_ms;
        let me = NODE_IDS[n];
        let entry = sys.log[sys.nodes[n].applied];
        let state =
            ClusterState::from_snapshot((*sys.nodes[n].view).clone(), Arc::new(AtomicU64::new(me)));
        let outcome = state.apply_command(entry.command(&self.scope));
        let node = &mut sys.nodes[n];
        node.view = state.snapshot();
        node.applied += 1;

        let mut minted = None;
        if let Ok((_, events)) = outcome {
            for ev in events {
                match ev {
                    // `plan_handoff_action` -> `HandoffAction::Arm`.
                    ClusterEvent::SlotHandoffPrepared {
                        slot,
                        source_node,
                        seq,
                        barrier_ms,
                        ..
                    } => {
                        minted = Some(seq);
                        if source_node != me {
                            continue;
                        }
                        // `pause_slot` merges with an existing pause and never
                        // shortens a live deadline (FM-CLUSTER-082).
                        let prev = node.armed.get(&slot).map_or(0, |a| a.deadline_ms);
                        node.armed.insert(
                            slot,
                            Armed {
                                seq,
                                deadline_ms: now.saturating_add(barrier_ms).max(prev),
                                confirms: 0,
                            },
                        );
                    }
                    // `plan_handoff_action` -> `HandoffAction::Release`.
                    ClusterEvent::SlotHandoffReleased {
                        slot, source_node, ..
                    } => {
                        if source_node == me {
                            node.armed.remove(&slot);
                        }
                    }
                    _ => {}
                }
            }
        }
        minted
    }

    /// The leader appends an entry and applies it before answering the
    /// proposer, which is what makes its view the most advanced one.
    fn commit(&self, sys: &mut Sys, entry: Entry) {
        sys.log.push(entry);
        let leader = sys.leader;
        debug_assert_eq!(sys.nodes[leader].applied + 1, sys.log.len());
        if let Some(seq) = self.apply_next(sys, leader) {
            sys.minted.push(seq);
        }
    }
}

/// `await_prepared_seq`: our own attempt, identified by the timestamp we
/// minted. No lease filter — `poll_handoff` reads `handoff` directly.
fn observed_prepare(view: &ClusterSnapshot, slot: u16, at_ms: u64) -> Option<u64> {
    view.migrations
        .get(&slot)?
        .handoff
        .as_ref()
        .filter(|h| h.prepared_at_ms == at_ms)
        .map(|h| h.seq)
}

/// `await_drained`.
fn observed_drained(view: &ClusterSnapshot, slot: u16, seq: u64) -> bool {
    view.migrations
        .get(&slot)
        .and_then(|m| m.handoff.as_ref())
        .is_some_and(|h| h.seq == seq && h.drained)
}

/// Fields of a node's view that two nodes at the same log position must agree
/// on, in an `Eq`-able shape (`ClusterSnapshot` is not `PartialEq`).
type Digest = (
    u64,
    Vec<(u16, NodeId)>,
    Vec<(u16, NodeId, NodeId, Option<(u64, u64, u64, u64, bool)>)>,
);

fn digest(view: &ClusterSnapshot) -> Digest {
    (
        view.handoff_seq,
        view.slot_assignment.iter().map(|(s, n)| (*s, *n)).collect(),
        view.migrations
            .values()
            .map(|m| {
                (
                    m.slot,
                    m.source_node,
                    m.target_node,
                    m.handoff
                        .as_ref()
                        .map(|h| (h.seq, h.prepared_at_ms, h.barrier_ms, h.lease_ms, h.drained)),
                )
            })
            .collect(),
    )
}

impl Sys {
    /// Nodes that would accept a write for `slot` right now: the node the slot
    /// is assigned to in *its own* view, unless its local write barrier is
    /// still live. This is the data-path predicate — routing consults the
    /// local snapshot, and `pause_slot` is what holds writes off during the
    /// handoff instant.
    pub(crate) fn writers(&self, slot: u16) -> Vec<NodeId> {
        (0..NODES)
            .filter(|&n| {
                let me = NODE_IDS[n];
                let node = &self.nodes[n];
                node.view.slot_assignment.get(&slot) == Some(&me)
                    && !node
                        .armed
                        .get(&slot)
                        .is_some_and(|a| self.now_ms < a.deadline_ms)
            })
            .map(|n| NODE_IDS[n])
            .collect()
    }
}

impl Model for Handoff {
    type State = Sys;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Sys {
            log: Vec::new(),
            nodes: (0..NODES)
                .map(|_| NodeRt {
                    applied: 0,
                    armed: BTreeMap::new(),
                    view: Arc::clone(&self.base),
                })
                .collect(),
            coords: vec![Coord::Idle { attempt: 0 }; self.scope.plan.len()],
            now_ms: 0,
            leader: 0,
            leader_changes: 0,
            minted: Vec::new(),
        }]
    }

    fn actions(&self, sys: &Self::State, out: &mut Vec<Self::Action>) {
        for n in 0..NODES {
            if sys.nodes[n].applied < sys.log.len() {
                out.push(Action::Apply(n));
            }
        }
        for s in 0..self.scope.plan.len() {
            let (slot, source, _, coordinator) = PLAN[self.scope.plan[s]];
            match sys.coords[s] {
                Coord::Idle { .. } => {
                    // Starting an attempt whose lease would outlive the clock
                    // horizon would strand a prepared record in every terminal
                    // state, turning the bound itself into a liveness
                    // counterexample. Refusing to start is the coordinator
                    // never being invoked, which is not a handoff bug.
                    if sys.now_ms + self.scope.lease_ms <= self.scope.horizon_ms {
                        out.push(Action::Coord(s));
                    }
                }
                Coord::AwaitPrepared { at_ms, .. } => {
                    if observed_prepare(&sys.nodes[idx(coordinator)].view, slot, at_ms).is_some() {
                        out.push(Action::Coord(s));
                    }
                    out.push(Action::CoordGivesUp(s));
                }
                Coord::AwaitDrained { seq, .. } => {
                    if observed_drained(&sys.nodes[idx(coordinator)].view, slot, seq) {
                        out.push(Action::Coord(s));
                    }
                    out.push(Action::CoordGivesUp(s));
                }
                Coord::Done { attempt } | Coord::GaveUp { attempt } => {
                    if attempt + 1 < self.scope.max_attempts {
                        out.push(Action::Retry(s));
                    }
                }
            }
            // The drain round trip has its own (longer) budget than the
            // barrier window, so a confirmation may still land after the
            // source has resumed serving.
            if sys.nodes[idx(source)]
                .armed
                .get(&slot)
                .is_some_and(|a| a.confirms <= self.scope.dup_confirms)
            {
                out.push(Action::Drain(s));
            }
        }
        if sys.leader_changes < self.scope.max_leader_changes {
            for n in 0..NODES {
                if n != sys.leader {
                    out.push(Action::LeaderChange(n));
                }
            }
        }
        let quiesced = sys.nodes.iter().all(|n| n.applied == sys.log.len());
        if sys.now_ms < self.scope.horizon_ms && (!self.scope.bounded_apply_lag || quiesced) {
            out.push(Action::Tick);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sys = last.clone();
        match action {
            Action::Apply(n) => {
                self.apply_next(&mut sys, n);
            }
            Action::Tick => sys.now_ms += 1,
            Action::LeaderChange(n) => {
                while sys.nodes[n].applied < sys.log.len() {
                    self.apply_next(&mut sys, n);
                }
                sys.leader = n;
                sys.leader_changes += 1;
            }
            Action::Drain(s) => {
                let (slot, source, _, _) = PLAN[self.scope.plan[s]];
                let armed = sys.nodes[idx(source)].armed.get_mut(&slot)?;
                let seq = armed.seq;
                armed.confirms += 1;
                self.commit(&mut sys, Entry::Confirm { slot, seq });
            }
            Action::Retry(s) => {
                let attempt = match sys.coords[s] {
                    Coord::Done { attempt } | Coord::GaveUp { attempt } => attempt,
                    _ => return None,
                };
                sys.coords[s] = Coord::Idle {
                    attempt: attempt + 1,
                };
            }
            Action::CoordGivesUp(s) => {
                let (slot, _, _, _) = PLAN[self.scope.plan[s]];
                sys.coords[s] = match sys.coords[s] {
                    // `await_prepared_seq` returned None: TRYAGAIN with *no*
                    // abort. Whatever record exists is left to its lease.
                    Coord::AwaitPrepared { attempt, .. } => Coord::GaveUp { attempt },
                    // `await_drained` returned false: best-effort abort, then
                    // TRYAGAIN. A lost abort proposal reduces to the branch
                    // above, so it is not modelled separately.
                    Coord::AwaitDrained { seq, attempt } => {
                        self.commit(&mut sys, Entry::Abort { slot, seq });
                        Coord::GaveUp { attempt }
                    }
                    _ => return None,
                };
            }
            Action::Coord(s) => {
                let (slot, _, _, coordinator) = PLAN[self.scope.plan[s]];
                let now = sys.now_ms;
                sys.coords[s] = match sys.coords[s] {
                    Coord::Idle { attempt } => {
                        let before = sys.minted.len();
                        self.commit(&mut sys, Entry::Prepare { slot, at_ms: now });
                        if sys.minted.len() > before {
                            Coord::AwaitPrepared {
                                at_ms: now,
                                attempt,
                            }
                        } else {
                            // The state machine refused the prepare (a live
                            // record already holds the slot, or the migration
                            // is gone). The command surfaces the error.
                            Coord::GaveUp { attempt }
                        }
                    }
                    Coord::AwaitPrepared { at_ms, attempt } => {
                        let seq = observed_prepare(&sys.nodes[idx(coordinator)].view, slot, at_ms)?;
                        Coord::AwaitDrained { seq, attempt }
                    }
                    Coord::AwaitDrained { attempt, .. } => {
                        self.commit(&mut sys, Entry::Complete { slot, at_ms: now });
                        Coord::Done { attempt }
                    }
                    _ => return None,
                };
            }
        }
        Some(sys)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            // (a) No interleaving admits writes on two nodes for one slot.
            Property::<Self>::always("single_writer_per_slot", |m, sys| {
                (0..m.scope.plan.len()).all(|s| sys.writers(PLAN[m.scope.plan[s]].0).len() <= 1)
            }),
            // (b) Handoff generations are never reused, and the counter that
            // mints them survives every snapshot restore (FM-CLUSTER-100).
            Property::<Self>::always("handoff_seqs_never_reused", |_, sys| {
                let strictly_increasing = sys.minted.windows(2).all(|w| w[0] < w[1]);
                let counter_survives_restore = sys
                    .nodes
                    .iter()
                    .all(|n| n.view.handoff_seq <= sys.minted.len() as u64)
                    && sys.nodes[sys.leader].view.handoff_seq == sys.minted.len() as u64;
                let armed_seqs_are_minted = sys
                    .nodes
                    .iter()
                    .all(|n| n.armed.values().all(|a| sys.minted.contains(&a.seq)));
                strictly_increasing && counter_survives_restore && armed_seqs_are_minted
            }),
            // Every explored state is judged by the invariant catalog. The
            // leader's view is the most advanced one and every follower view
            // is a prefix the leader also passed through, so checking the
            // leader covers every view any node ever holds.
            Property::<Self>::always("invariant_catalog_clean", |_, sys| {
                let view = (*sys.nodes[sys.leader].view).clone();
                let state = ClusterState::from_snapshot(view, Arc::new(AtomicU64::new(1)));
                state.check_invariants().is_empty()
            }),
            // The same log prefix yields the same state on every node — the
            // premise the `view`-free state identity rests on.
            Property::<Self>::always("apply_is_deterministic", |_, sys| {
                (0..NODES).all(|a| {
                    (a + 1..NODES).all(|b| {
                        sys.nodes[a].applied != sys.nodes[b].applied
                            || digest(&sys.nodes[a].view) == digest(&sys.nodes[b].view)
                    })
                })
            }),
            // Liveness: no coordinator is stuck mid-flight and no prepared
            // record outlives its resolution — every prepare ends Released
            // (abort / complete) or lapses with its lease.
            Property::<Self>::eventually("every_prepared_handoff_settles", |m, sys| {
                let coords_settled = sys.coords.iter().all(|c| !c.in_flight());
                let no_live_record = (0..m.scope.plan.len()).all(|s| {
                    let slot = PLAN[m.scope.plan[s]].0;
                    sys.nodes.iter().all(|n| {
                        n.view
                            .migrations
                            .get(&slot)
                            .and_then(|mig| mig.live_handoff_at(sys.now_ms))
                            .is_none()
                    })
                });
                coords_settled && no_live_record
            }),
            // Vacuity guards: the scope has to actually reach the interesting
            // outcomes, or the `always` properties above prove nothing.
            Property::<Self>::sometimes("a_handoff_completes", |m, sys| {
                (0..m.scope.plan.len()).any(|s| {
                    let (slot, _, target, _) = PLAN[m.scope.plan[s]];
                    sys.nodes[sys.leader].view.slot_assignment.get(&slot) == Some(&target)
                })
            }),
            Property::<Self>::sometimes("a_handoff_aborts", |_, sys| {
                sys.log.iter().any(|e| matches!(e, Entry::Abort { .. }))
            }),
        ];
        // Only a scope that allows a retry can be asked to exhibit one. Total
        // mints exceeding the slot count means *some* slot minted twice, since
        // a slot mints at most once per attempt.
        if self.scope.max_attempts >= 2 {
            properties.push(Property::<Self>::sometimes(
                "a_second_attempt_runs",
                |m, sys| sys.minted.len() > m.scope.plan.len(),
            ));
        }
        properties
    }
}

/// Bounded-depth configuration for the default test suite (< 10 s).
///
/// One handoff, third-party coordinated (slot 1: source 2, target 3,
/// coordinator 1), because that is the shape in which the source can lag the
/// leader at all — a source that *is* the leader applies at commit time and
/// never sits behind.
pub(crate) fn smoke_scope() -> Scope {
    Scope {
        plan: vec![1],
        barrier_ms: 1,
        lease_ms: 2,
        horizon_ms: 2,
        max_attempts: 2,
        dup_confirms: 1,
        max_leader_changes: 1,
        bounded_apply_lag: true,
    }
}

/// Full budget, breadth. Both handoffs concurrently, so node 2 is the target
/// of one and the source of the other (FM-CLUSTER-088).
///
/// Retries are *not* in this scope: a second attempt per slot on top of the
/// cross product did not finish in 25 s and passed 7 GB of resident state,
/// while the same budget buys a much deeper single-slot search in
/// [`deep_scope`]. Splitting breadth from depth is the concession the
/// small-scope hypothesis actually needs here; the two together cover every
/// mechanism, just not simultaneously.
pub(crate) fn cross_slot_scope() -> Scope {
    Scope {
        plan: vec![0, 1],
        barrier_ms: 1,
        lease_ms: 2,
        horizon_ms: 2,
        max_attempts: 1,
        dup_confirms: 1,
        max_leader_changes: 1,
        bounded_apply_lag: true,
    }
}

/// Full budget, depth. One handoff, three attempts, two duplicated drain
/// acknowledgements and two leader changes — the retry/lease machinery
/// (FM-CLUSTER-085/086) pushed as far as one slot can be pushed.
pub(crate) fn deep_scope() -> Scope {
    Scope {
        plan: vec![1],
        barrier_ms: 1,
        lease_ms: 2,
        horizon_ms: 2,
        max_attempts: 3,
        dup_confirms: 2,
        max_leader_changes: 2,
        bounded_apply_lag: true,
    }
}

/// The same handoff with the apply-lag assumption withdrawn: the source may sit
/// out arbitrarily many milliseconds mid-handoff. This scope is *expected* to
/// violate `single_writer_per_slot`; see
/// `model::tests::stale_source_admits_writes_after_ownership_moves` and
/// `.scratch/cluster-correctness/issues/open/16-stale-source-outlives-its-write-barrier.md`.
pub(crate) fn unbounded_lag_scope() -> Scope {
    Scope {
        plan: vec![1],
        barrier_ms: 1,
        lease_ms: 2,
        horizon_ms: 3,
        max_attempts: 1,
        dup_confirms: 0,
        max_leader_changes: 0,
        bounded_apply_lag: false,
    }
}
