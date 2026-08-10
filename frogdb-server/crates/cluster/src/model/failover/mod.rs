//! Explicit-state model of the failover composite (`stateright`), model 2.
//!
//! # What this checks
//!
//! A primary loses its slots to a successor through a single replicated
//! command, [`ClusterCommand::Failover`], proposed by whichever node currently
//! believes it is the Raft leader. Around it run the failure detector's
//! verdicts — `MarkNodeFailed` / `MarkNodeRecovered` — and the operator's
//! `CLUSTER FAILOVER`, and none of the three is ordered against the others.
//! The composite is correct only if no interleaving of detector verdicts,
//! automatic failovers proposed by *two* would-be leaders, manual takeovers,
//! apply lag and leader changes ever leaves two nodes acting as the primary
//! for one slot, or replays a config epoch across two promotions.
//!
//! # The transition function is production code
//!
//! Same discipline as [model 1](super): the model never re-implements the state
//! machine. Every step reloads a node's `ClusterSnapshot` into a real
//! [`ClusterState`] via [`ClusterState::from_snapshot`], calls the production
//! [`ClusterState::apply_command`], and keeps the resulting snapshot — so what
//! is being checked is the `Failover` / `MarkNodeFailed` / `MarkNodeRecovered`
//! arms of `commands.rs`, and an edit to an arm changes the model with no edit
//! here.
//!
//! The model layer contributes only what Raft, the network and the *callers*
//! contribute in production:
//!
//! * a single replicated log, appended to by the current leader;
//! * per-node apply lag (a follower, including a proposer, may sit behind);
//! * leader changes (the new leader catches up before it may append);
//! * the failure detector's control flow, per would-be leader;
//! * the `CLUSTER FAILOVER` admin path's control flow.
//!
//! The detector is a direct transcription of the *control flow* of
//! `FailureDetector::reconcile_topology` → `mark_node_failed` →
//! `trigger_auto_failover` (cluster-runtime): reconcile is level-triggered and
//! writes only where the local verdict disagrees with the replicated `fail`
//! flag; a successful `MarkNodeFailed` for a primary is followed by a
//! *separate* read of the proposer's own (possibly stale) snapshot to score a
//! candidate among `get_replicas`, and then by the `Failover { force: true }`
//! proposal. Splitting those into three model steps is what admits the
//! interleavings: everything can commit in the windows between them.
//! [`Action::Takeover`] transcribes `cluster::admin`'s replica path
//! (`CLUSTER FAILOVER [FORCE|TAKEOVER]` → `Failover { old: my primary, new: me
//! }`), which is the only producer of `force: false` in the tree.
//!
//! Detectors are *not* gated on being the real leader. `reconcile_topology` is
//! called behind `if detector.is_leader()`, and `is_leader()` reads openraft's
//! `server_state()`, which a deposed leader keeps answering `Leader` to for as
//! long as it takes to hear otherwise — while its `client_write` still commits,
//! by forwarding. `scope.detectors` is exactly the set of nodes in that window
//! at once, which is the "two would-be leaders" the model exists to permute.
//!
//! # Two exposures, pinned rather than scoped out
//!
//! [Model 1](super) could name its known defect as a scope knob — an assumption
//! the callers keep, withdrawn in one small scope to characterize the defect.
//! Neither exposure here is like that: both are reachable from the *faithful*
//! detector and admin control flow, inside the checked configurations, with no
//! caller doing anything wrong. Three attempts to fence them off as assumptions
//! (a leader-view successor-health gate, a caught-up-proposal gate, a fairness
//! knob that retried an abandoned attempt) each moved the counterexample rather
//! than removing it, which is the signal that the exposure is in the system and
//! not in the model. So they are pinned in place instead, as `sometimes`
//! properties that go **unwitnessed — and therefore red — the day the exposure
//! is closed**:
//!
//! * `a_slot_strands_on_a_failed_primary` — the automatic failover is
//!   edge-triggered on the `MarkNodeFailed` write while the loop that produces
//!   that write is level-triggered on the verdict/flag disagreement alone, so
//!   any early return in `trigger_auto_failover` discards the failover forever
//!   and a slot comes to rest on a FAIL-flagged primary beside a healthy
//!   replica. Minimal witness: [`stranded_scope`], issue 18.
//! * `a_promotion_moves_nothing` — `Failover { force: true }` waives the
//!   old-primary-exists check unconditionally, so a proposal built off a stale
//!   view promotes a successor that inherits no slots, detaching it from a live
//!   replication link and putting the highest config epoch on a slotless node.
//!   Minimal witness: [`unjustified_promotion_scope`], issue 19. Structurally
//!   unwitnessable under [`Topology::PeerPrimary`] (the node that could name a
//!   departed primary is already a primary there, so the failover absorbs and
//!   promotes nobody), so the property is only asserted for
//!   [`Topology::TwoReplicas`].
//!
//! Each is replayed against the real state machine, with no `stateright` in the
//! loop, in [`replay`]. The two safety properties the model exists for —
//! `single_primary_per_slot` and `epoch_grows_across_promotions` — hold in every
//! checked scope, as do `invariant_catalog_clean` and `apply_is_deterministic`.
//!
//! # Scope
//!
//! 3 nodes, one or two would-be leaders, one or two slots, bounded verdict
//! flips / committed entries / takeovers / leader changes. No migrations: slot
//! handoff is [model 1](super)'s subject, and keeping `migrations` empty here
//! keeps the two models' state spaces from multiplying (it also keeps issues 15
//! and 16, both migration-shaped, structurally out of scope rather than
//! muzzled).
//!
//! # Exploration budget (recorded per issue 10 / PRD §8 D1)
//!
//! Measured on a 10-core M-series laptop, BFS to exhaustion (every config
//! terminates; none is depth- or time-truncated):
//!
//! | config | scope | unique states | depth | wall | where |
//! |---|---|---|---|---|---|
//! | [`smoke_scope`] | 2 leaders, 1 slot, 4 entries, 2 flips | 535 528 | 20 | 4.5 s (debug) | default suite |
//! | [`stranded_scope`] | 1 detector, 3 entries, no takeovers (issue 18) | 1 075 | 15 | < 0.1 s (debug) | default suite |
//! | [`unjustified_promotion_scope`] | 1 detector, 3 entries, 1 takeover (issue 19) | 1 368 | 14 | < 0.1 s (debug) | default suite |
//! | [`two_leader_scope`] | 2 leaders, 1 slot, 5 entries, 3 flips, 2 leader changes | 23 090 707 | 26 | 70 s (release) | nightly, `just model-check` |
//! | [`absorb_scope`] | 2 slots (peer primary), 5 entries, 2 leader changes | 1 704 886 | 25 | 4.6 s (release) | nightly, `just model-check` |
//!
//! The per-commit figures are debug numbers, because debug is what the
//! per-commit suite pays; the two full configs are `--release` only. Breadth
//! and depth do not compose here either (the same finding model 1 recorded):
//! the deep config with a second takeover on top (6 entries, or 5 entries and 2
//! takeovers) had not terminated after four minutes and several GB of visited
//! states, so the nightly runs depth and breadth as two configs side by side
//! rather than one that is both.
//!
//! The floors are asserted by the tests themselves (`MIN_*_STATES`), so a scope
//! change that quietly shrinks the explored space fails the build rather than
//! passing vacuously.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use stateright::{Model, Property};

use super::{NODE_IDS, NODES, addr, idx};
use crate::state::ClusterState;
use crate::types::{
    ClusterCommand, ClusterEvent, ClusterSnapshot, NodeId, NodeInfo, NodeRole, SlotRange,
};

#[cfg(test)]
mod replay;
#[cfg(test)]
mod tests;

/// The slot node 1 owns at t0 — the one the failover has to move.
const SLOT: u16 = 0;

/// The slot the peer primary owns under [`Topology::PeerPrimary`].
const PEER_SLOT: u16 = 1;

/// The starting shape of the cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Topology {
    /// Node 1 is a primary owning [`SLOT`]; nodes 2 and 3 are its replicas.
    /// Two candidates for one failed primary is the shape in which two
    /// would-be leaders can score *different* successors.
    TwoReplicas,
    /// Node 1 is a primary owning [`SLOT`] with node 2 as its only replica;
    /// node 3 is an unrelated primary owning [`PEER_SLOT`]. Covers the absorb
    /// path (a failover whose successor is already a primary, so no promotion
    /// event fires) alongside the promotion path.
    PeerPrimary,
}

/// How much of the space to explore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Scope {
    pub topology: Topology,
    /// Node indices running the reconciliation loop concurrently — the
    /// would-be leaders. Each carries its own health verdicts and its own
    /// point in the detector's control flow.
    pub detectors: Vec<usize>,
    /// Health-verdict flips permitted along a path. Each one is a peer
    /// crossing (or un-crossing) the detector's failure threshold locally.
    pub max_flips: u8,
    /// Committed entries permitted along a path. This is the bound that makes
    /// the space finite.
    pub max_entries: usize,
    /// `CLUSTER FAILOVER [FORCE]` invocations permitted along a path.
    pub max_takeovers: u8,
    /// Leader changes permitted along a path.
    pub max_leader_changes: u8,
}

impl Scope {
    /// The slots this topology puts on the board.
    fn slots(&self) -> &'static [u16] {
        match self.topology {
            Topology::TwoReplicas => &[SLOT],
            Topology::PeerPrimary => &[SLOT, PEER_SLOT],
        }
    }
}

/// A committed log entry, in the compact form the model hashes. Rendered to a
/// real [`ClusterCommand`] by [`Entry::command`] before every apply — the enum
/// encodes *which* production command was proposed, never what it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Entry {
    MarkFailed {
        node: NodeId,
    },
    MarkRecovered {
        node: NodeId,
    },
    Failover {
        old: NodeId,
        new: NodeId,
        force: bool,
    },
}

impl Entry {
    pub(crate) fn command(self) -> ClusterCommand {
        match self {
            Entry::MarkFailed { node } => ClusterCommand::MarkNodeFailed { node_id: node },
            Entry::MarkRecovered { node } => ClusterCommand::MarkNodeRecovered { node_id: node },
            Entry::Failover { old, new, force } => ClusterCommand::Failover {
                old_primary_id: old,
                new_primary_id: new,
                force,
            },
        }
    }
}

/// Where a would-be leader's detector is in `reconcile_topology` →
/// `mark_node_failed` → `trigger_auto_failover`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Det {
    /// Between reconciliation passes.
    Idle,
    /// `MarkNodeFailed` committed for `failed`; `trigger_auto_failover` is
    /// about to read this node's own snapshot and score a candidate.
    Selecting { failed: NodeId },
    /// A candidate was scored off that (possibly stale) snapshot; the
    /// `Failover` proposal has not been made yet.
    Proposing { failed: NodeId, successor: NodeId },
}

/// One node's runtime: how far it has applied, and its resulting view.
#[derive(Clone, Debug)]
pub(crate) struct NodeRt {
    pub applied: usize,
    pub view: Arc<ClusterSnapshot>,
}

// `view` is excluded from the state's identity: it is a pure function of the
// log prefix `log[..applied]` applied to the (constant) initial snapshot, and
// both `log` and `applied` are part of the identity. The
// `apply_is_deterministic` property is what keeps that claim honest.
impl PartialEq for NodeRt {
    fn eq(&self, other: &Self) -> bool {
        self.applied == other.applied
    }
}
impl Eq for NodeRt {}
impl Hash for NodeRt {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.applied.hash(h);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Action {
    /// Node `n` applies its next committed entry.
    Apply(usize),
    /// Detector `d`'s local health verdict about node `p` crosses the
    /// threshold (either way).
    Flip(usize, usize),
    /// Detector `d` reconciles its verdict about node `p` into the replicated
    /// topology.
    Reconcile(usize, usize),
    /// Detector `d` scores node `c` as the successor for the primary it just
    /// flagged.
    Select(usize, usize),
    /// Detector `d` gives up: the failed node is not a primary, it has no
    /// candidate, or the `Failover` write never succeeded.
    Abandon(usize),
    /// Detector `d` proposes the `Failover` it scored.
    Propose(usize),
    /// `CLUSTER FAILOVER [FORCE]` is issued on node `n`.
    Takeover(usize, bool),
    /// Node `n` becomes leader, catching up first.
    LeaderChange(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Sys {
    pub log: Vec<Entry>,
    pub nodes: Vec<NodeRt>,
    /// Per would-be leader, in `scope.detectors` order.
    pub dets: Vec<Det>,
    /// Per would-be leader: "I believe node `p` is unreachable", by node index.
    /// A detector never judges itself (`reconcile_topology` skips `self`).
    pub verdicts: Vec<[bool; NODES]>,
    pub flips: u8,
    pub takeovers: u8,
    pub leader: usize,
    pub leader_changes: u8,
    /// The config epoch carried by each `NodePromoted`, in commit order.
    pub promotions: Vec<u64>,
    /// Set when an accepted `Failover` promoted a node that neither received a
    /// slot nor was a replica of the old primary — a promotion with nothing
    /// behind it. See `promotions_are_justified`.
    pub unjustified_promotion: bool,
}

pub(crate) struct Failover {
    pub scope: Scope,
    base: Arc<ClusterSnapshot>,
}

/// What one `apply_command` produced, in the two shapes the model needs.
struct Applied {
    accepted: bool,
    /// `(promoted node, epoch)` per `NodePromoted` event.
    promoted: Vec<(NodeId, u64)>,
}

impl Failover {
    pub(crate) fn new(scope: Scope) -> Self {
        assert!(
            !scope.detectors.is_empty(),
            "a model with no detector cannot fail anything over"
        );
        assert!(scope.detectors.iter().all(|&d| d < NODES));
        assert!(scope.max_entries > 0);
        let base = Self::build_base(&scope);
        Self { scope, base }
    }

    /// The topology the model starts every node from. [`replay`] builds its
    /// deterministic re-runs from the same snapshot so a counterexample is
    /// replayed against the state the checker actually explored.
    pub(crate) fn base(&self) -> Arc<ClusterSnapshot> {
        Arc::clone(&self.base)
    }

    /// Built by the production state machine, so the model can never start
    /// from a state the real one cannot reach.
    fn build_base(scope: &Scope) -> Arc<ClusterSnapshot> {
        let state = ClusterState::from_snapshot(
            ClusterSnapshot::default(),
            Arc::new(AtomicU64::new(NODE_IDS[0])),
        );
        let primary = |id: NodeId| ClusterCommand::AddNode {
            node: NodeInfo::new_primary(id, addr(id, 7000), addr(id, 17000)),
        };
        let replica = |id: NodeId, of: NodeId| ClusterCommand::AddNode {
            node: NodeInfo::new_replica(id, addr(id, 7000), addr(id, 17000), of),
        };
        let assign = |id: NodeId, slot: u16| ClusterCommand::AssignSlots {
            node_id: id,
            slots: vec![SlotRange::new(slot, slot)],
        };

        state.apply_command(primary(NODE_IDS[0])).expect("AddNode");
        state
            .apply_command(assign(NODE_IDS[0], SLOT))
            .expect("AssignSlots");
        state
            .apply_command(replica(NODE_IDS[1], NODE_IDS[0]))
            .expect("AddNode");
        match scope.topology {
            Topology::TwoReplicas => {
                state
                    .apply_command(replica(NODE_IDS[2], NODE_IDS[0]))
                    .expect("AddNode");
            }
            Topology::PeerPrimary => {
                state.apply_command(primary(NODE_IDS[2])).expect("AddNode");
                state
                    .apply_command(assign(NODE_IDS[2], PEER_SLOT))
                    .expect("AssignSlots");
            }
        }
        state.snapshot()
    }

    /// Apply node `n`'s next committed entry through the production state
    /// machine.
    fn apply_next(&self, sys: &mut Sys, n: usize) -> Applied {
        let me = NODE_IDS[n];
        let entry = sys.log[sys.nodes[n].applied];
        let state =
            ClusterState::from_snapshot((*sys.nodes[n].view).clone(), Arc::new(AtomicU64::new(me)));
        let outcome = state.apply_command(entry.command());
        let node = &mut sys.nodes[n];
        node.view = state.snapshot();
        node.applied += 1;

        match outcome {
            Ok((_, events)) => Applied {
                accepted: true,
                promoted: events
                    .iter()
                    .filter_map(|ev| match ev {
                        ClusterEvent::NodePromoted {
                            promoted_node_id,
                            epoch,
                        } => Some((*promoted_node_id, *epoch)),
                        _ => None,
                    })
                    .collect(),
            },
            Err(_) => Applied {
                accepted: false,
                promoted: Vec::new(),
            },
        }
    }

    /// The leader appends an entry and applies it before answering the
    /// proposer, which is what makes its view the most advanced one. Returns
    /// whether the state machine accepted the command — the proposer sees a
    /// `ClusterResponse::Error` when it did not, and production branches on it.
    fn commit(&self, sys: &mut Sys, entry: Entry) -> bool {
        let leader = sys.leader;
        debug_assert_eq!(sys.nodes[leader].applied, sys.log.len());
        let before = Arc::clone(&sys.nodes[leader].view);
        sys.log.push(entry);
        let applied = self.apply_next(sys, leader);

        for (node, epoch) in &applied.promoted {
            sys.promotions.push(*epoch);
            if let Entry::Failover { old, new, .. } = entry
                && *node == new
            {
                let inherited = before.slot_assignment.values().any(|owner| *owner == old);
                let was_child = before.nodes.get(&new).and_then(|n| n.primary_id) == Some(old);
                sys.unjustified_promotion |= !inherited && !was_child;
            }
        }
        // A `Failover { force: true }` removes the old primary, and a node
        // that is no longer a member cannot go on being the Raft leader. The
        // move is forced rather than modelled as a choice, and is not charged
        // against `max_leader_changes`: the alternative is a leader outside
        // the cluster proposing membership changes, which is not a state
        // production can be in.
        if !sys.nodes[sys.leader]
            .view
            .nodes
            .contains_key(&NODE_IDS[sys.leader])
            && let Some(next) =
                (0..NODES).find(|&n| sys.nodes[sys.leader].view.nodes.contains_key(&NODE_IDS[n]))
        {
            while sys.nodes[next].applied < sys.log.len() {
                self.apply_next(sys, next);
            }
            sys.leader = next;
        }
        applied.accepted
    }

    /// The node a detector runs on.
    fn det_node(&self, d: usize) -> usize {
        self.scope.detectors[d]
    }

    /// A detector on a node the cluster has dropped is a detector on a node
    /// that is out of the cluster — it stops mattering, and letting it keep
    /// reconciling would let a ghost rescue states the real cluster cannot.
    fn det_live(&self, sys: &Sys, d: usize) -> bool {
        let n = self.det_node(d);
        sys.nodes[n].view.nodes.contains_key(&NODE_IDS[n])
    }

    /// The peers detector `d` reconciles over: `get_all_nodes()` minus itself.
    fn det_peers(&self, sys: &Sys, d: usize) -> Vec<usize> {
        let n = self.det_node(d);
        (0..NODES)
            .filter(|&p| p != n && sys.nodes[n].view.nodes.contains_key(&NODE_IDS[p]))
            .collect()
    }
}

/// Fields of a node's view that two nodes at the same log position must agree
/// on, in an `Eq`-able shape (`ClusterSnapshot` is not `PartialEq`).
type Digest = (
    u64,
    Vec<(u16, NodeId)>,
    Vec<(NodeId, NodeRole, Option<NodeId>, u64, bool)>,
);

fn digest(view: &ClusterSnapshot) -> Digest {
    (
        view.config_epoch,
        view.slot_assignment.iter().map(|(s, n)| (*s, *n)).collect(),
        view.nodes
            .values()
            .map(|n| (n.id, n.role, n.primary_id, n.config_epoch, n.flags.fail))
            .collect(),
    )
}

impl Sys {
    /// Nodes that act as the primary for `slot` right now: the node the slot is
    /// assigned to in *its own* view, when that view also calls it a Primary.
    /// This is the data-path predicate — routing and role both come off the
    /// node's local snapshot.
    pub(crate) fn primaries_for(&self, slot: u16) -> Vec<NodeId> {
        (0..NODES)
            .filter(|&n| {
                let me = NODE_IDS[n];
                let view = &self.nodes[n].view;
                view.slot_assignment.get(&slot) == Some(&me)
                    && view.nodes.get(&me).is_some_and(|info| info.is_primary())
            })
            .map(|n| NODE_IDS[n])
            .collect()
    }

    fn quiesced(&self) -> bool {
        self.nodes.iter().all(|n| n.applied == self.log.len())
    }
}

impl Model for Failover {
    type State = Sys;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Sys {
            log: Vec::new(),
            nodes: (0..NODES)
                .map(|_| NodeRt {
                    applied: 0,
                    view: Arc::clone(&self.base),
                })
                .collect(),
            dets: vec![Det::Idle; self.scope.detectors.len()],
            verdicts: vec![[false; NODES]; self.scope.detectors.len()],
            flips: 0,
            takeovers: 0,
            leader: 0,
            leader_changes: 0,
            promotions: Vec::new(),
            unjustified_promotion: false,
        }]
    }

    fn actions(&self, sys: &Self::State, out: &mut Vec<Self::Action>) {
        for n in 0..NODES {
            if sys.nodes[n].applied < sys.log.len() {
                out.push(Action::Apply(n));
            }
        }

        let room = sys.log.len() < self.scope.max_entries;

        for d in 0..self.scope.detectors.len() {
            if !self.det_live(sys, d) {
                continue;
            }
            let dn = self.det_node(d);
            let view = Arc::clone(&sys.nodes[dn].view);

            if sys.flips < self.scope.max_flips {
                for p in self.det_peers(sys, d) {
                    out.push(Action::Flip(d, p));
                }
            }

            match sys.dets[d] {
                Det::Idle => {
                    if !room {
                        continue;
                    }
                    // `reconcile_topology`: one write per peer whose replicated
                    // flag disagrees with the local verdict, each on its own
                    // task — so any divergent peer may go first.
                    for p in self.det_peers(sys, d) {
                        let flagged = view.nodes[&NODE_IDS[p]].flags.fail;
                        if sys.verdicts[d][p] != flagged {
                            out.push(Action::Reconcile(d, p));
                        }
                    }
                }
                Det::Selecting { failed } => {
                    // `trigger_auto_failover` reads this node's own snapshot:
                    // it fails over only a primary, and only onto one of its
                    // replicas.
                    if view.nodes.get(&failed).is_some_and(|n| n.is_primary()) {
                        for r in view.get_replicas(failed) {
                            out.push(Action::Select(d, idx(r.id)));
                        }
                    }
                    // Not a primary, no replicas, or the write failed three
                    // times: `trigger_auto_failover` returns, and nothing
                    // re-arms it.
                    out.push(Action::Abandon(d));
                }
                Det::Proposing { .. } => {
                    if room {
                        out.push(Action::Propose(d));
                    }
                    // The Raft write timed out or was refused three times.
                    out.push(Action::Abandon(d));
                }
            }
        }

        if room && sys.takeovers < self.scope.max_takeovers {
            for (n, id) in NODE_IDS.iter().enumerate() {
                let view = &sys.nodes[n].view;
                let Some(me) = view.nodes.get(id) else {
                    continue;
                };
                // The admin path's replica leg. A primary's leg needs another
                // primary to absorb into and is the same command with the ids
                // swapped, so it adds no shape the detector path lacks.
                let Some(primary_id) = me.primary_id.filter(|_| me.is_replica()) else {
                    continue;
                };
                for force in [false, true] {
                    // Plain `CLUSTER FAILOVER` refuses a primary already
                    // flagged FAIL and tells the operator to use FORCE.
                    if !force && view.nodes[&primary_id].flags.fail {
                        continue;
                    }
                    out.push(Action::Takeover(n, force));
                }
            }
        }

        if sys.leader_changes < self.scope.max_leader_changes {
            for n in 0..NODES {
                if n != sys.leader {
                    out.push(Action::LeaderChange(n));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sys = last.clone();
        match action {
            Action::Apply(n) => {
                self.apply_next(&mut sys, n);
            }
            Action::LeaderChange(n) => {
                while sys.nodes[n].applied < sys.log.len() {
                    self.apply_next(&mut sys, n);
                }
                sys.leader = n;
                sys.leader_changes += 1;
            }
            Action::Flip(d, p) => {
                sys.verdicts[d][p] = !sys.verdicts[d][p];
                sys.flips += 1;
            }
            Action::Reconcile(d, p) => {
                let node = NODE_IDS[p];
                let failing = sys.verdicts[d][p];
                let accepted = self.commit(
                    &mut sys,
                    if failing {
                        Entry::MarkFailed { node }
                    } else {
                        Entry::MarkRecovered { node }
                    },
                );
                // `mark_node_failed` only reaches `trigger_auto_failover` when
                // the write came back Ok and carried no `ClusterResponse::Error`.
                sys.dets[d] = if failing && accepted {
                    Det::Selecting { failed: node }
                } else {
                    Det::Idle
                };
            }
            Action::Select(d, c) => {
                let Det::Selecting { failed } = sys.dets[d] else {
                    return None;
                };
                sys.dets[d] = Det::Proposing {
                    failed,
                    successor: NODE_IDS[c],
                };
            }
            Action::Abandon(d) => {
                if sys.dets[d] == Det::Idle {
                    return None;
                }
                sys.dets[d] = Det::Idle;
            }
            Action::Propose(d) => {
                let Det::Proposing { failed, successor } = sys.dets[d] else {
                    return None;
                };
                self.commit(
                    &mut sys,
                    Entry::Failover {
                        old: failed,
                        new: successor,
                        force: true,
                    },
                );
                sys.dets[d] = Det::Idle;
            }
            Action::Takeover(n, force) => {
                let me = NODE_IDS[n];
                let primary_id = sys.nodes[n].view.nodes.get(&me)?.primary_id?;
                self.commit(
                    &mut sys,
                    Entry::Failover {
                        old: primary_id,
                        new: me,
                        force,
                    },
                );
                sys.takeovers += 1;
            }
        }
        Some(sys)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // (a) Once the log has drained everywhere, exactly one node acts as
            // the primary for each slot. Before that a lagging old primary
            // still believes it owns its slots, which is the window the config
            // epoch and the client's MOVED handling exist to close; after it,
            // two claimants is split brain.
            Property::<Self>::always("single_primary_per_slot", |m, sys| {
                !sys.quiesced()
                    || m.scope
                        .slots()
                        .iter()
                        .all(|&slot| sys.primaries_for(slot).len() == 1)
            }),
            // (b) Every promotion claims a config epoch strictly above every
            // epoch claimed before it, so no two promotions are ever
            // indistinguishable by authority, and the claim never runs ahead of
            // the cluster counter (INV-EPOCH-1's other half).
            Property::<Self>::always("epoch_grows_across_promotions", |_, sys| {
                sys.promotions.windows(2).all(|w| w[0] < w[1])
                    && sys
                        .promotions
                        .last()
                        .is_none_or(|&last| last <= sys.nodes[sys.leader].view.config_epoch)
            }),
            // (c) KNOWN EXPOSURE (issue 18), pinned as a characterization
            // property rather than muzzled: the cluster *can* come to rest
            // with a slot owned by a node it has itself flagged FAIL while a
            // healthy replica sits beside it. `reconcile_topology` writes only
            // where a verdict disagrees with the replicated flag, and the
            // automatic failover hangs off that write, so a failover missed
            // once — because the proposer's snapshot was stale, because the
            // Raft write failed three times, because the successor was already
            // flagged — is never retried. Asserting reachability here (rather
            // than in a scope of its own) is deliberate: the exposure lives
            // inside the configurations that check the safety properties, and
            // when it is closed this property goes unwitnessed and the run
            // fails, which is the signal to flip it to
            // `always(!at_rest || unrescued_slot().is_none())`.
            Property::<Self>::sometimes("a_slot_strands_on_a_failed_primary", |m, sys| {
                m.at_rest(sys) && m.unrescued_slot(sys).is_some()
            }),
            // (e) Every explored state is judged by the invariant catalog. The
            // leader's view is the most advanced one and every follower view is
            // a prefix the leader also passed through.
            Property::<Self>::always("invariant_catalog_clean", |_, sys| {
                let view = (*sys.nodes[sys.leader].view).clone();
                let state = ClusterState::from_snapshot(view, Arc::new(AtomicU64::new(1)));
                state.check_invariants().is_empty()
            }),
            // (f) The same log prefix yields the same state on every node — the
            // premise the `view`-free state identity rests on.
            Property::<Self>::always("apply_is_deterministic", |_, sys| {
                (0..NODES).all(|a| {
                    (a + 1..NODES).all(|b| {
                        sys.nodes[a].applied != sys.nodes[b].applied
                            || digest(&sys.nodes[a].view) == digest(&sys.nodes[b].view)
                    })
                })
            }),
            // Vacuity guards: the scope has to reach the interesting outcomes,
            // or the `always` properties above prove nothing.
            Property::<Self>::sometimes("a_primary_is_flagged_failed", |_, sys| {
                sys.nodes[sys.leader]
                    .view
                    .nodes
                    .values()
                    .any(|n| n.is_primary() && n.flags.fail)
            }),
            Property::<Self>::sometimes("a_promotion_happens", |_, sys| !sys.promotions.is_empty()),
            Property::<Self>::sometimes("a_slot_changes_hands", |_, sys| {
                sys.nodes[sys.leader].view.slot_assignment.get(&SLOT) != Some(&NODE_IDS[0])
            }),
        ];

        // (d) KNOWN EXPOSURE (issue 19), pinned the same way as (c): a
        // promotion can move nothing at all. `Failover { force: true }` waives
        // the check that the old primary is still a member, so a proposal built
        // off a stale view — a detector that scored its candidate before an
        // earlier failover landed, or `CLUSTER FAILOVER FORCE` on a replica
        // that has not yet applied its own reparenting — still promotes its
        // successor: the successor detaches from the primary that is actually
        // feeding it, the live slot owner loses a replica, and the cluster's
        // highest config epoch lands on a node that owns nothing. Goes
        // unwitnessed when the exposure is closed, which is the signal to flip
        // it to `always(!unjustified_promotion)`.
        //
        // Only under [`Topology::TwoReplicas`]. The exposure needs a node that
        // is *promoted* while inheriting nothing, and under
        // [`Topology::PeerPrimary`] the node that can be left naming a departed
        // primary is already a Primary, so the failover absorbs into it and
        // emits no promotion at all — a `sometimes` property a topology
        // structurally cannot witness would fail that scope for the wrong
        // reason.
        if self.scope.topology == Topology::TwoReplicas {
            props.push(Property::<Self>::sometimes(
                "a_promotion_moves_nothing",
                |_, sys| sys.unjustified_promotion,
            ));
        }

        props
    }
}

impl Failover {
    /// The cluster is at rest: every node has applied every entry, no detector
    /// is mid-decision, and no detector's verdict disagrees with the replicated
    /// flag it reconciles against — so `reconcile_topology` would write
    /// nothing, forever. Operator actions are deliberately not part of this:
    /// convergence is the cluster's own job.
    ///
    /// A path that has spent its entry budget is excluded: being unable to
    /// append is an artifact of the bound, not a property of the composite.
    fn at_rest(&self, sys: &Sys) -> bool {
        sys.log.len() < self.scope.max_entries
            && sys.quiesced()
            && sys.dets.iter().all(|d| *d == Det::Idle)
            && (0..self.scope.detectors.len()).all(|d| {
                !self.det_live(sys, d)
                    || self.det_peers(sys, d).iter().all(|&p| {
                        let view = &sys.nodes[self.det_node(d)].view;
                        sys.verdicts[d][p] == view.nodes[&NODE_IDS[p]].flags.fail
                    })
            })
    }

    /// A modelled slot the cluster has come to rest on in a state it should
    /// have failed over: its owner is missing, is not a Primary, or is flagged
    /// FAIL while a *healthy* replica is available and another member's
    /// detector is watching. All three conditions matter — a slot whose only
    /// replicas are themselves flagged has nowhere to go, and a slot nobody is
    /// watching is a modelling artifact rather than a defect.
    fn unrescued_slot(&self, sys: &Sys) -> Option<u16> {
        let view = &sys.nodes[sys.leader].view;
        self.scope.slots().iter().copied().find(|slot| {
            let Some(&owner) = view.slot_assignment.get(slot) else {
                return true;
            };
            let Some(info) = view.nodes.get(&owner) else {
                return true;
            };
            if !info.is_primary() {
                return true;
            }
            let watched = self
                .scope
                .detectors
                .iter()
                .any(|&d| NODE_IDS[d] != owner && self.det_live(sys, self.detector_slot(d)));
            let rescuable = view.get_replicas(owner).iter().any(|r| !r.flags.fail);
            info.flags.fail && rescuable && watched
        })
    }

    /// Position of node index `n` in `scope.detectors`.
    fn detector_slot(&self, n: usize) -> usize {
        self.scope
            .detectors
            .iter()
            .position(|&d| d == n)
            .expect("called with a detector node index")
    }
}

/// Bounded configuration for the default test suite.
///
/// Two would-be leaders on the two replicas of the one primary that owns a
/// slot: the shape in which both can flag the primary and score *different*
/// successors for it.
pub(crate) fn smoke_scope() -> Scope {
    Scope {
        topology: Topology::TwoReplicas,
        detectors: vec![1, 2],
        max_flips: 2,
        max_entries: 4,
        max_takeovers: 1,
        max_leader_changes: 1,
    }
}

/// Full budget, depth: the same two-leader shape with room for a second round
/// of verdicts, another committed entry and another leader change. A *second*
/// takeover on top of that does not terminate in a laptop-runnable budget (see
/// the exploration budget in the [module docs](self)).
pub(crate) fn two_leader_scope() -> Scope {
    Scope {
        topology: Topology::TwoReplicas,
        detectors: vec![1, 2],
        max_flips: 3,
        max_entries: 5,
        max_takeovers: 1,
        max_leader_changes: 2,
    }
}

/// Full budget, breadth: two slots, and a successor that is already a primary
/// — the absorb path, where the failover moves slots without promoting anyone.
pub(crate) fn absorb_scope() -> Scope {
    Scope {
        topology: Topology::PeerPrimary,
        detectors: vec![1, 2],
        max_flips: 2,
        max_entries: 5,
        max_takeovers: 1,
        max_leader_changes: 2,
    }
}

/// The smallest scope that strands a slot (issue 18): one detector, one
/// failover, one later verdict. Small enough that the witness path is short
/// enough to transcribe into [`replay`] by hand.
pub(crate) fn stranded_scope() -> Scope {
    Scope {
        topology: Topology::TwoReplicas,
        detectors: vec![2],
        max_flips: 2,
        max_entries: 3,
        max_takeovers: 0,
        max_leader_changes: 0,
    }
}

/// The smallest witness for issue 19: one detector fails the primary over onto
/// itself, and the third node — which has not applied the failover and so
/// still believes the departed primary is its own — issues `CLUSTER FAILOVER
/// FORCE` against it. See `tests::a_promotion_can_move_nothing`.
pub(crate) fn unjustified_promotion_scope() -> Scope {
    Scope {
        topology: Topology::TwoReplicas,
        detectors: vec![1],
        max_flips: 1,
        max_entries: 3,
        max_takeovers: 1,
        max_leader_changes: 0,
    }
}
