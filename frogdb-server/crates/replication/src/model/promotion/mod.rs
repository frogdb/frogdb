//! Explicit-state model of the promotion / resume composite (`stateright`).
//!
//! # What this checks
//!
//! A replica is promoted to primary. It mints a replication id, freezes the id
//! it inherited into a failover window at the offset its applies settled at,
//! and reopens its resume window at that same offset. Around that run the
//! things that are *not* ordered against it: the old primary is still writing,
//! frames are still in flight to replicas that have not applied them, replicas
//! are reconnecting and asking to resume, and the persist that makes the
//! promotion durable can fail. The composite is correct only if no interleaving
//! of those ever lets a `+CONTINUE` stitch one node's history onto another's,
//! lets two replication ids claim the same offset range on one node, lets a
//! `+FULLRESYNC` adopt half its granted pair before the payload lands, or lets
//! a promotion that could not be persisted leave a trace of itself.
//!
//! # The transition function is production code
//!
//! The model never re-implements a decision. Every transition that decides
//! something calls the function production calls:
//!
//! * promotion — [`plan_primary_stint`] (`primary/promotion.rs`), the decision
//!   half of `PrimaryReplicationHandler::begin_primary_stint`, with the mint
//!   supplied by the model exactly as production supplies it from
//!   `generate_replication_id`;
//! * the resume decision — a real [`PartialSyncReplay`] is rebuilt from the
//!   node's window each step and asked
//!   [`PartialSyncReplay::handle_partial_sync_request`], and the *frames it
//!   grants* are what the model delivers, so a backlog that cannot cover the
//!   range it just promised shows up as a hole rather than as an assumption;
//! * the wire — the grant is rendered by [`HandshakeReply::line`], the same
//!   renderer the primary writes to the socket (`+CONTINUE <replid>` /
//!   `+FULLRESYNC <replid> <offset>`), and parsed back by the replica's
//!   [`select_psync_arm`], with the request line built by
//!   [`psync_request_args`], so the arm the replica takes is selected by
//!   production code over a production-rendered line;
//! * every state mutation on a node goes through a `ReplicationState` method
//!   ([`ReplicationState::shift_replication_id`],
//!   [`ReplicationState::apply_staged_metadata`]) — the model never assigns
//!   `replication_id` or `secondary_offset` by hand.
//!
//! An edit to any of those changes the model with no edit here.
//!
//! # What the model layer contributes
//!
//! Only what the network, the scheduler and the *callers* contribute in
//! production: which node is promoted and when, whether the persist behind it
//! succeeds, apply lag (a frame received is not a frame applied), connection
//! drops, reparenting, and demotions. Content is modelled as a tag per offset —
//! the stint that wrote it — because that is the only thing the safety
//! properties need to distinguish: two nodes' offset 7 either came from the
//! same write or it did not.
//!
//! ## Deliberate omissions, and why they cost nothing
//!
//! * **Acks.** There is no `Ack` action. An ack is a replica reporting the head
//!   it has applied, so the lag an ack exposes is exactly the lag already
//!   modelled by frames sitting in `inflight`; a separate action would add
//!   states that differ only in a number no property here reads. The WAIT/ack
//!   composite is checked by its own suites.
//! * **Backlog eviction.** The rebuilt backlog is sized far above any scope's
//!   write budget, so the only lower bound in play is the *armed floor* — which
//!   is the bound promotion sets and therefore the one this model exists to
//!   check. Eviction is FM-REPLICATION-014's own ground.
//! * **The mint's entropy.** Ids are `model_id(k)`, 40 hex digits keyed to a
//!   counter, because `plan_primary_stint` takes the minted id as a parameter.
//!   A failed promotion does not consume the counter: production burns entropy
//!   there, but a discarded id is unobservable, and not consuming it keeps the
//!   space finite in the same way.
//! * **The applied gate on a primary.** A promoted node's applied gate stays
//!   frozen in production, but nothing consumes it once the node has no inbound
//!   stream, so a successful promotion normalizes `frozen` to `false`. Carrying
//!   it would double the primary states with no observable difference.
//!
//! # One exposure, pinned rather than scoped out
//!
//! `a_failed_promotion_strands_the_node` is a **known exposure** (issue 16),
//! asserted `sometimes` inside the persist-failure scope rather than muzzled.
//! `RoleManager::promote` tears the replica stream down and clears the primary
//! target *before* `begin_primary_stint`, and `begin_primary_stint` freezes the
//! applied gate (`settle_at_applied`) before the persist it can fail. The
//! rollback restores the `ReplicationState` bit for bit — [`StintPlan`] does
//! exactly what it says — but nothing restores the stream, the target or the
//! gate, so the node comes to rest as a replica that follows nobody, applies
//! nothing, and cannot be recovered by anything it does itself. See
//! [`replay`] for the counterexample re-run against real state with no model
//! checker in the loop.
//!
//! When that is fixed this property goes unwitnessed and the run fails, which
//! is the signal to flip it to an `always` — see the note on the property.
//!
//! # Budget
//!
//! Two exhaustive configurations run side by side rather than one product of
//! both, because breadth and depth do not compose: the scope that permutes two
//! would-be primaries cannot also afford the write depth, and the deep scope
//! cannot afford the third node. Measured on an M-series laptop, release:
//!
//! | config | scope | unique states | depth | wall | where |
//! |---|---|---|---|---|---|
//! | `smoke` | 3 nodes, 4 writes, 1 promotion, 4 psyncs, 3 drops, 2 reparents | 124,265 | 23 | 0.2 s (1.1 s debug) | default suite |
//! | `strand` | 2 nodes, 2 writes, 2 promotions, 3 psyncs, 2 drops, 1 reparent, persist failures | 544 | 13 | <0.1 s | default suite |
//! | `full/deep` | 2 nodes, 7 writes, 3 promotions, 6 psyncs, 4 drops, 3 demotes, persist failures | 8,038,584 | 34 | 13.8 s | nightly / `just replication-model-check` |
//! | `full/two-primary` | 3 nodes, 7 writes, 2 promotions, 7 psyncs, 5 drops, 4 reparents | 6,172,626 | 41 | 11.9 s | nightly / `just replication-model-check` |
//!
//! The floors in [`tests`] are set just under those counts, so a scope edit that
//! collapses the space fails loudly instead of passing faster.
//!
//! [`plan_primary_stint`]: crate::primary::plan_primary_stint
//! [`StintPlan`]: crate::primary::StintPlan
//! [`PartialSyncReplay`]: crate::primary::PartialSyncReplay
//! [`select_psync_arm`]: crate::replica::psync::select_psync_arm
//! [`psync_request_args`]: crate::replica::psync::psync_request_args

use std::hash::{Hash, Hasher};

use bytes::Bytes;
use stateright::{Model, Property};

use crate::primary::{BacklogConfig, PartialSyncReplay, ReplayDecision, plan_primary_stint};
use crate::replica::psync::{PsyncArm, psync_request_args, select_psync_arm};
use crate::session_machine::HandshakeReply;
use crate::state::{ReplicationState, StagedReplicationMetadata};

pub(crate) mod replay;
#[cfg(test)]
mod tests;

/// A deterministic stand-in for `generate_replication_id`: 40 hex digits, one
/// per counter value, so the state space is finite. `plan_primary_stint` takes
/// the mint as a parameter precisely so this substitution is possible without
/// shadowing anything.
fn model_id(k: u8) -> String {
    format!("{k:040}")
}

/// What a node is doing, in the sense the composite cares about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Role {
    Primary,
    Replica { of: usize },
}

/// The inbound replication link's state. `Syncing` is the window FM-REPLICATION-001
/// is about: a `+FULLRESYNC` has been granted and the payload has not landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Link {
    Detached,
    Syncing,
    Streaming,
}

/// A granted `+FULLRESYNC` that has not installed yet.
///
/// The payload is captured *at grant time* because that is when production cuts
/// the checkpoint; whatever the source writes afterwards is replayed from its
/// backlog when the stream starts, which [`Action::Install`] does too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingSync {
    granted_id: String,
    granted_offset: u64,
    payload: Vec<u8>,
    /// The state the node was on when the arm was selected. The
    /// "adopts-nothing-early" property is an equality against this.
    pre: ReplicationState,
}

/// One node's runtime.
///
/// `history` is the node's data: one tag per offset, offset `i + 1` at index
/// `i`, the tag being the stint that wrote it. So the applied offset is
/// `history.len()` and two nodes disagreeing about offset 7 is a tag mismatch
/// rather than something the model has to be told about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRt {
    /// The real thing, mutated only through its own methods.
    pub(crate) state: ReplicationState,
    pub(crate) role: Role,
    pub(crate) link: Link,
    pub(crate) history: Vec<u8>,
    /// Received but not applied: `(offset, tag)`, in delivery order.
    pub(crate) inflight: Vec<(u64, u8)>,
    /// The armed backlog floor, `Some` exactly while the node is a primary.
    pub(crate) floor: Option<u64>,
    pub(crate) pending: Option<PendingSync>,
    /// The tag this node writes under — the index of the id it minted.
    pub(crate) stint: u8,
    /// Stands for the whole bundle a failed promotion leaves behind: the
    /// replica stream dropped, `primary_target` cleared, and the applied gate
    /// frozen by `settle_at_applied` with nothing to unfreeze it. A frozen node
    /// neither reconnects nor applies; only an external role action
    /// ([`Action::Demote`] / [`Action::Reparent`], both of which start a new
    /// replica stint) clears it.
    pub(crate) frozen: bool,
}

impl Hash for NodeRt {
    fn hash<H: Hasher>(&self, h: &mut H) {
        hash_repl_state(&self.state, h);
        self.role.hash(h);
        self.link.hash(h);
        self.history.hash(h);
        self.inflight.hash(h);
        self.floor.hash(h);
        match &self.pending {
            None => 0u8.hash(h),
            Some(p) => {
                1u8.hash(h);
                p.granted_id.hash(h);
                p.granted_offset.hash(h);
                p.payload.hash(h);
                hash_repl_state(&p.pre, h);
            }
        }
        self.stint.hash(h);
        self.frozen.hash(h);
    }
}

/// `ReplicationState` is deliberately not `Hash` (it is a persisted record, not
/// a key), so the model hashes the fields that carry replication identity.
/// `master_host`/`master_port`/`active_version` are runtime fields this model
/// never sets.
fn hash_repl_state<H: Hasher>(s: &ReplicationState, h: &mut H) {
    s.replication_id.hash(h);
    s.secondary_id.hash(h);
    s.offset_at_save.hash(h);
    s.secondary_offset.hash(h);
}

/// The whole system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Sys {
    pub(crate) nodes: Vec<NodeRt>,
    writes: u8,
    promotions: u8,
    psyncs: u8,
    drops: u8,
    demotes: u8,
    reparents: u8,
    /// Next mint index. Consumed only by a promotion that persists.
    mints: u8,
    /// A `+CONTINUE` was granted over a range the granter could not cover, or
    /// onto a prefix the two nodes disagree about.
    holed: bool,
    /// A failed promotion changed the node's `ReplicationState`.
    rollback_lost_state: bool,
    /// A rendered PSYNC reply did not select an arm.
    arm_undecidable: bool,
    /// Vacuity witness: a resume was actually granted somewhere.
    saw_continue: bool,
    /// Vacuity witness: a promotion landed with frames still in flight.
    saw_promotion_mid_flight: bool,
}

/// The bounds. Every knob is a budget on an action, so a scope is exactly "how
/// much of each thing may happen", and shrinking one is visible in the state
/// counts the tests floor.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Scope {
    pub nodes: usize,
    pub max_writes: u8,
    pub max_promotions: u8,
    pub max_psyncs: u8,
    pub max_drops: u8,
    pub max_demotes: u8,
    pub max_reparents: u8,
    /// Whether a promotion may fail to persist.
    pub persist_failures: bool,
}

/// Bounded smoke configuration: three nodes, one promotion, enough writes and
/// reconnects that a resume crosses the promotion. Default suite.
pub(crate) fn smoke_scope() -> Scope {
    Scope {
        nodes: 3,
        max_writes: 4,
        max_promotions: 1,
        max_psyncs: 4,
        max_drops: 3,
        max_demotes: 0,
        max_reparents: 2,
        persist_failures: false,
    }
}

/// The smallest scope that reaches the known exposure: two nodes, one write, a
/// promotion that may fail to persist. Default suite — it is the reachability
/// witness the replay test pins.
pub(crate) fn strand_scope() -> Scope {
    Scope {
        nodes: 2,
        max_writes: 2,
        max_promotions: 2,
        max_psyncs: 3,
        max_drops: 2,
        max_demotes: 0,
        max_reparents: 1,
        persist_failures: true,
    }
}

/// Full budget, depth: two nodes, two promotions, a demotion and enough write
/// depth that a second stint's window sits above the first's.
pub(crate) fn deep_scope() -> Scope {
    Scope {
        nodes: 2,
        max_writes: 7,
        max_promotions: 3,
        max_psyncs: 6,
        max_drops: 4,
        max_demotes: 3,
        max_reparents: 0,
        persist_failures: true,
    }
}

/// Full budget, breadth: three nodes and two promotions with no demotion, so
/// two would-be primaries serve resumes at once and a surviving replica can be
/// moved between them. This is the "two primaries feeding one replica" hunt.
pub(crate) fn two_primary_scope() -> Scope {
    Scope {
        nodes: 3,
        max_writes: 7,
        max_promotions: 2,
        max_psyncs: 7,
        max_drops: 5,
        max_demotes: 0,
        max_reparents: 4,
        persist_failures: false,
    }
}

/// The model.
#[derive(Debug, Clone)]
pub(crate) struct Promotion {
    pub(crate) scope: Scope,
}

impl Promotion {
    pub(crate) fn new(scope: Scope) -> Self {
        Self { scope }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    /// The primary takes a write.
    Write(usize),
    /// A replica applies the next frame it received.
    Deliver(usize),
    /// A replica is promoted; the flag is whether the persist behind it
    /// succeeds.
    Promote(usize, bool),
    /// A primary steps down toward another primary, taking its replicas with
    /// it (the topology's doing, not the node's).
    Demote(usize, usize),
    /// A detached replica reconnects and asks to resume.
    Psync(usize),
    /// A granted full resync's payload lands.
    Install(usize),
    /// The link drops.
    Drop(usize),
    /// An operator (or the role reconciler) points a replica at a different
    /// primary.
    Reparent(usize, usize),
}

/// Rebuild the node's real backlog from its window.
///
/// A primary's backlog holds exactly the frames it wrote since it armed its
/// floor: everything below the floor was inherited, never recorded, and is what
/// the floor exists to refuse. Sized so nothing is ever evicted — see the
/// module docs.
fn replay_for(node: &NodeRt) -> PartialSyncReplay {
    let replay = PartialSyncReplay::new(&BacklogConfig {
        enabled: true,
        max_entries: 4096,
        max_bytes: 1 << 20,
        ttl_secs: 0,
    });
    if let Some(floor) = node.floor {
        replay.arm_backlog_floor(floor);
        for offset in (floor + 1)..=(node.history.len() as u64) {
            replay.record(offset, 0, Bytes::from_static(b"x"));
        }
    }
    replay
}

impl Sys {
    fn replicas_of(&self, primary: usize) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&n| self.nodes[n].role == Role::Replica { of: primary })
            .collect()
    }
}

impl Role {
    /// The primary this node follows, if it follows one.
    fn primary_of(self) -> Option<usize> {
        match self {
            Role::Replica { of } => Some(of),
            Role::Primary => None,
        }
    }
}

impl Model for Promotion {
    type State = Sys;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let nodes = (0..self.scope.nodes)
            .map(|n| {
                let mut state = ReplicationState::new();
                // `ReplicationState::new` mints from entropy; the model pins the
                // boot ids so two runs explore the same space. A replica's boot
                // id sits well above any mint index so it can never collide
                // with one.
                state.replication_id = if n == 0 {
                    model_id(0)
                } else {
                    model_id(200 + n as u8)
                };
                NodeRt {
                    state,
                    role: if n == 0 {
                        Role::Primary
                    } else {
                        Role::Replica { of: 0 }
                    },
                    link: Link::Detached,
                    history: Vec::new(),
                    inflight: Vec::new(),
                    floor: if n == 0 { Some(0) } else { None },
                    pending: None,
                    stint: 0,
                    frozen: false,
                }
            })
            .collect();
        vec![Sys {
            nodes,
            writes: 0,
            promotions: 0,
            psyncs: 0,
            drops: 0,
            demotes: 0,
            reparents: 0,
            mints: 1,
            holed: false,
            rollback_lost_state: false,
            arm_undecidable: false,
            saw_continue: false,
            saw_promotion_mid_flight: false,
        }]
    }

    fn actions(&self, sys: &Self::State, out: &mut Vec<Self::Action>) {
        let n_nodes = sys.nodes.len();
        for n in 0..n_nodes {
            let node = &sys.nodes[n];
            match node.role {
                Role::Primary => {
                    if sys.writes < self.scope.max_writes {
                        out.push(Action::Write(n));
                    }
                    if sys.demotes < self.scope.max_demotes {
                        for t in 0..n_nodes {
                            if t != n && sys.nodes[t].role == Role::Primary {
                                out.push(Action::Demote(n, t));
                            }
                        }
                    }
                }
                Role::Replica { of } => {
                    // A frozen node has no stream and no target: it neither
                    // reconnects nor applies. That is the exposure, not a
                    // modelling shortcut — see `NodeRt::frozen`.
                    if !node.frozen {
                        if !node.inflight.is_empty() && node.link == Link::Streaming {
                            out.push(Action::Deliver(n));
                        }
                        if node.link == Link::Detached
                            && sys.psyncs < self.scope.max_psyncs
                            && sys.nodes[of].role == Role::Primary
                        {
                            out.push(Action::Psync(n));
                        }
                        if node.pending.is_some() {
                            out.push(Action::Install(n));
                        }
                    }
                    if sys.promotions < self.scope.max_promotions {
                        out.push(Action::Promote(n, true));
                        if self.scope.persist_failures {
                            out.push(Action::Promote(n, false));
                        }
                    }
                    if sys.reparents < self.scope.max_reparents {
                        for p in 0..n_nodes {
                            if p != n && p != of && sys.nodes[p].role == Role::Primary {
                                out.push(Action::Reparent(n, p));
                            }
                        }
                    }
                }
            }
            if node.link != Link::Detached && sys.drops < self.scope.max_drops {
                out.push(Action::Drop(n));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sys = last.clone();
        match action {
            Action::Write(p) => {
                let tag = sys.nodes[p].stint;
                sys.nodes[p].history.push(tag);
                let offset = sys.nodes[p].history.len() as u64;
                for r in sys.replicas_of(p) {
                    if sys.nodes[r].link == Link::Streaming {
                        sys.nodes[r].inflight.push((offset, tag));
                    }
                }
                sys.writes += 1;
            }

            Action::Deliver(r) => {
                let (offset, tag) = sys.nodes[r].inflight.remove(0);
                if offset == sys.nodes[r].history.len() as u64 + 1 {
                    sys.nodes[r].history.push(tag);
                } else {
                    // A frame that does not continue the node's own head is a
                    // hole in the stream it was granted.
                    sys.holed = true;
                }
            }

            Action::Promote(n, persist_ok) => {
                // `RoleManager::promote` drops the replica stream and clears the
                // primary target *before* it calls `begin_primary_stint`, and
                // `begin_primary_stint` discards the staged checkpoint before
                // it settles the heads. None of that is undone by the rollback.
                if !sys.nodes[n].inflight.is_empty() {
                    sys.saw_promotion_mid_flight = true;
                }
                sys.nodes[n].link = Link::Detached;
                sys.nodes[n].pending = None;
                // `settle_at_applied`: received-but-unapplied frames are
                // discarded and the applied head is frozen.
                sys.nodes[n].inflight.clear();
                sys.nodes[n].frozen = true;
                let boundary = sys.nodes[n].history.len() as u64;

                let before = sys.nodes[n].state.clone();
                let plan = plan_primary_stint(&before, model_id(sys.mints), boundary);
                if persist_ok {
                    sys.nodes[n].state = plan.minted;
                    sys.nodes[n].role = Role::Primary;
                    sys.nodes[n].stint = sys.mints;
                    sys.nodes[n].floor = Some(plan.backlog_floor);
                    // A primary has no inbound stream for the gate to hold up.
                    sys.nodes[n].frozen = false;
                    sys.mints += 1;
                } else {
                    sys.nodes[n].state = plan.rollback;
                    if sys.nodes[n].state != before {
                        sys.rollback_lost_state = true;
                    }
                }
                sys.promotions += 1;
            }

            Action::Demote(n, target) => {
                // `end_primary_stint`: the backlog window closes and every
                // replica is disconnected. The replicas follow the node's own
                // new primary, which is what the topology does with them.
                for r in sys.replicas_of(n) {
                    sys.nodes[r].link = Link::Detached;
                    sys.nodes[r].inflight.clear();
                    sys.nodes[r].pending = None;
                    sys.nodes[r].role = Role::Replica { of: target };
                }
                sys.nodes[n].role = Role::Replica { of: target };
                sys.nodes[n].floor = None;
                sys.nodes[n].link = Link::Detached;
                sys.nodes[n].inflight.clear();
                sys.nodes[n].pending = None;
                // Starting a replica stint is what unfreezes the applied gate.
                sys.nodes[n].frozen = false;
                sys.demotes += 1;
            }

            Action::Psync(r) => {
                let p = sys.nodes[r].role.primary_of()?;
                let head = sys.nodes[p].history.len() as u64;
                let (req_id, req_offset) = psync_request_args(
                    &sys.nodes[r].state.replication_id,
                    sys.nodes[r].history.len() as u64,
                );
                let decision = replay_for(&sys.nodes[p]).handle_partial_sync_request(
                    &sys.nodes[p].state,
                    &req_id,
                    req_offset.max(0) as u64,
                    head,
                );
                // Rendered by the production renderer the primary writes to
                // the socket, and selected by the replica's own parser — so the
                // model exercises the same two halves the wire does rather than
                // a transcription of them.
                let line = match &decision {
                    ReplayDecision::Continue(_) => HandshakeReply::Continue {
                        replication_id: sys.nodes[p].state.replication_id.clone(),
                    },
                    ReplayDecision::FullResync(_) => HandshakeReply::FullResync {
                        replication_id: sys.nodes[p].state.replication_id.clone(),
                        offset: head,
                    },
                }
                .line();
                match select_psync_arm(&line) {
                    Err(_) => sys.arm_undecidable = true,
                    Ok(PsyncArm::Continue { granted_id }) => {
                        let ReplayDecision::Continue(grant) = decision else {
                            // The two halves disagreed about which arm this is.
                            sys.arm_undecidable = true;
                            sys.psyncs += 1;
                            return Some(sys);
                        };
                        let resumed_at = sys.nodes[r].history.len() as u64;
                        // (a) the granter must actually hold the range it just
                        // promised, contiguously, all the way to the head it
                        // named…
                        let promised: Vec<u64> =
                            (grant.replay_from + 1..=grant.resume_offset).collect();
                        let delivered: Vec<u64> =
                            grant.frames.iter().map(|(off, _, _)| *off).collect();
                        if delivered != promised {
                            sys.holed = true;
                        }
                        // …and (b) the two nodes must agree about everything
                        // below the resume point, or the resume stitches one
                        // history onto another.
                        let below = grant.replay_from as usize;
                        if sys.nodes[r].history.len() < below
                            || sys.nodes[p].history.len() < below
                            || sys.nodes[r].history[..below] != sys.nodes[p].history[..below]
                        {
                            sys.holed = true;
                        }
                        if let Some(granted) = granted_id
                            && sys.nodes[r].state.replication_id != granted
                        {
                            sys.nodes[r].state.shift_replication_id(granted, resumed_at);
                        }
                        for offset in promised {
                            let tag = sys.nodes[p].history[offset as usize - 1];
                            sys.nodes[r].inflight.push((offset, tag));
                        }
                        sys.nodes[r].link = Link::Streaming;
                        sys.saw_continue = true;
                    }
                    Ok(PsyncArm::FullResync {
                        granted_id,
                        granted_offset,
                    }) => {
                        // Neither half of the granted pair is adopted here, and
                        // the *keyspace is not dropped either*: the payload is
                        // received into a staging area and swapped in only when
                        // it is complete (`ReplicaConnection::install_payload`,
                        // `replica/connection.rs:474` — a node with no installer
                        // wired "keeps serving its previous keyspace"). Clearing
                        // the history at the grant would model a flush FrogDB
                        // does not perform, and would make an interrupted full
                        // resync look like a node advertising a failover window
                        // over data it no longer holds.
                        let payload = sys.nodes[p].history[..granted_offset as usize].to_vec();
                        let pre = sys.nodes[r].state.clone();
                        sys.nodes[r].inflight.clear();
                        sys.nodes[r].pending = Some(PendingSync {
                            granted_id,
                            granted_offset,
                            payload,
                            pre,
                        });
                        sys.nodes[r].link = Link::Syncing;
                    }
                }
                sys.psyncs += 1;
            }

            Action::Install(r) => {
                let pending = sys.nodes[r].pending.take()?;
                let meta = StagedReplicationMetadata {
                    replication_id: pending.granted_id,
                    replication_offset: pending.granted_offset,
                    checksum: None,
                };
                sys.nodes[r].state.apply_staged_metadata(&meta);
                sys.nodes[r].history = pending.payload;
                sys.nodes[r].link = Link::Streaming;
                // What the source wrote between the cut and the install is
                // replayed from its backlog before the stream goes live.
                let p = sys.nodes[r].role.primary_of()?;
                let tail: Vec<(u64, u8)> = ((pending.granted_offset as usize)
                    ..sys.nodes[p].history.len())
                    .map(|i| (i as u64 + 1, sys.nodes[p].history[i]))
                    .collect();
                sys.nodes[r].inflight = tail;
            }

            Action::Drop(r) => {
                sys.nodes[r].link = Link::Detached;
                sys.nodes[r].inflight.clear();
                sys.nodes[r].pending = None;
                sys.drops += 1;
            }

            Action::Reparent(r, p) => {
                sys.nodes[r].role = Role::Replica { of: p };
                sys.nodes[r].link = Link::Detached;
                sys.nodes[r].inflight.clear();
                sys.nodes[r].pending = None;
                // A fresh `REPLICAOF` starts a replica stint, which unfreezes
                // the applied gate — the only way out of the exposure.
                sys.nodes[r].frozen = false;
                sys.reparents += 1;
            }
        }
        Some(sys)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = vec![
            // (a) A granted `+CONTINUE` is never served over a hole: the granter
            // holds every offset in the range it promised, and the two nodes
            // agree about everything below the resume point.
            Property::<Self>::always("continue_is_never_served_over_a_hole", |_, sys| !sys.holed),
            // (b) No two live replication ids claim the same offset range on one
            // node. The failover window claims `[0, secondary_offset]` under
            // `secondary_id`; the live stream claims `(floor, head]` under
            // `replication_id`. They must not overlap, and the window must not
            // claim data the node does not hold — which is exactly what a
            // boundary frozen at the *received* head instead of the applied one
            // would do (FM-REPLICATION-019).
            Property::<Self>::always("no_two_replids_claim_one_offset_range", |_, sys| {
                sys.nodes.iter().all(|n| {
                    if n.state.secondary_offset < 0 {
                        return true;
                    }
                    let window_end = n.state.secondary_offset as u64;
                    window_end <= n.history.len() as u64
                        && n.floor.is_none_or(|floor| floor >= window_end)
                })
            }),
            // (c) A `+FULLRESYNC` adopts neither half of the granted pair before
            // the payload installs: while the sync is open the node's state is
            // bit for bit what it was when the arm was selected — no id, no
            // offset, no window (FM-REPLICATION-001's ordering clause). The
            // keyspace it serves meanwhile is its *old* one, untouched, which is
            // why this clause is about the state and not about the data.
            Property::<Self>::always("full_resync_adopts_nothing_early", |_, sys| {
                sys.nodes.iter().all(|n| match &n.pending {
                    None => true,
                    Some(p) => n.state == p.pre,
                })
            }),
            // (d) A promotion that could not be written down leaves no trace of
            // itself *in the replication state* — the half [`StintPlan`] owns.
            // The halves it does not own are the exposure below.
            Property::<Self>::always("a_failed_promotion_restores_the_state_exactly", |_, sys| {
                !sys.rollback_lost_state
            }),
            // (e) Every rendered reply selects exactly one arm: production
            // renders it, production parses it, and "neither" is not a state the
            // replica can be in (FM-REPLICATION-013).
            Property::<Self>::always("every_psync_resolves_to_exactly_one_arm", |_, sys| {
                !sys.arm_undecidable
            }),
            // (f) Two nodes on one live link never disagree about their common
            // prefix, and nothing in flight contradicts the primary's own
            // history. This is the "two primaries feeding one replica" clause:
            // it holds across every interleaving in which both a stale primary
            // and a promoted one are serving resumes.
            Property::<Self>::always("a_live_link_never_forks_history", |_, sys| {
                sys.nodes.iter().all(|r| {
                    let Some(p) = r.role.primary_of() else {
                        return true;
                    };
                    if r.link != Link::Streaming {
                        return true;
                    }
                    let primary = &sys.nodes[p];
                    let common = r.history.len().min(primary.history.len());
                    r.history[..common] == primary.history[..common]
                        && r.inflight.iter().all(|&(offset, tag)| {
                            primary
                                .history
                                .get(offset as usize - 1)
                                .is_none_or(|&held| held == tag)
                        })
                })
            }),
            // (g) Every state a node reaches is a state the production validator
            // accepts. The invariant catalog W1 builds (issue 02) is not landed
            // yet; `ReplicationState::validate` is the production judgement that
            // exists, applied to every explored state.
            Property::<Self>::always("replication_state_stays_valid", |_, sys| {
                sys.nodes.iter().all(|n| n.state.validate())
            }),
            // Vacuity witnesses. A green run over a space where nothing
            // interesting happens proves nothing.
            Property::<Self>::sometimes("a_promotion_mints_and_freezes_a_window", |_, sys| {
                sys.nodes.iter().any(|n| n.state.secondary_id.is_some())
            }),
            Property::<Self>::sometimes("a_resume_is_granted", |_, sys| sys.saw_continue),
            Property::<Self>::sometimes("a_full_resync_is_granted", |_, sys| {
                sys.nodes.iter().any(|n| n.pending.is_some())
            }),
            Property::<Self>::sometimes("a_promotion_lands_with_frames_in_flight", |_, sys| {
                sys.saw_promotion_mid_flight
            }),
        ];

        // Only where the scope can produce it: a two-node scope with no
        // demotion has nobody left to follow the node it promoted.
        if self.scope.nodes >= 3 || self.scope.max_demotes >= 1 {
            props.push(Property::<Self>::sometimes(
                "a_replica_streams_from_a_promoted_node",
                |_, sys| {
                    sys.nodes.iter().any(|r| {
                        r.link == Link::Streaming
                            && r.role
                                .primary_of()
                                .is_some_and(|p| sys.nodes[p].state.secondary_id.is_some())
                    })
                },
            ));
        }

        if self.scope.persist_failures {
            // KNOWN EXPOSURE (issue 16), pinned as a characterization property
            // rather than muzzled. `RoleManager::promote` drops the replica
            // stream and clears `primary_target` before `begin_primary_stint`,
            // which freezes the applied gate in `settle_at_applied` before the
            // persist that can fail. The `StintPlan` rollback restores the
            // replication state exactly — property (d) proves that — but the
            // stream, the target and the gate are not part of the plan, so the
            // node comes to rest as a replica that follows nobody and applies
            // nothing until something outside it starts a new replica stint.
            //
            // When this is fixed the property goes unwitnessed and the run
            // fails, which is the signal to flip it to
            // `always(|_, sys| sys.nodes.iter().all(|n| !n.frozen))`.
            props.push(Property::<Self>::sometimes(
                "a_failed_promotion_strands_the_node",
                |_, sys| {
                    sys.nodes
                        .iter()
                        .any(|n| n.frozen && n.role.primary_of().is_some())
                },
            ));
        } else {
            // Liveness, and only where the exposure above is out of scope:
            // every replica ends up streaming, unless the reconnect budget ran
            // out first. Without the escape this would flag scope exhaustion
            // rather than a trap; with it, a node that can never reconnect
            // while budget remains is a discovery.
            props.push(Property::<Self>::eventually(
                "every_replica_ends_up_streaming",
                |m, sys| {
                    sys.psyncs >= m.scope.max_psyncs
                        || sys
                            .nodes
                            .iter()
                            .all(|n| n.role == Role::Primary || n.link == Link::Streaming)
                },
            ));
        }

        props
    }
}
