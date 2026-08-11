//! [`ReplicationView`]: the plain-data projection of this node's replication
//! state, assembled at check time from the components that actually own it.
//!
//! # Why a projection instead of a state machine
//!
//! `frogdb-cluster` can write its invariant catalog as pure functions over
//! `&ClusterStateInner` because one struct owns the whole replicated state and
//! one function transitions it. Replication has no such struct. Its state is a
//! federation: an `Arc<RwLock<ReplicationState>>` held by six components, a
//! live-offset `Arc<AtomicU64>` shared four ways, [`AppliedOffset`]'s two
//! atomics plus a gate mutex plus an epoch and a divergence latch, the
//! tracker's two `RwLock<HashMap>`s and an `AtomicU8` departure code, and a
//! ring buffer whose entries sit behind a `Mutex<VecDeque>` while its floor is
//! a separate `AtomicI64`.
//!
//! So the catalog is pure functions over *this* — a snapshot copied out of
//! those cells at one instant. No locks are held while it is checked, nothing
//! here is shared, and the same value is what a property test asserts on, what
//! a model checker keeps as its state, and what `DEBUG REPLICATION CHECK`
//! renders.
//!
//! # Every group is optional, and that is the design
//!
//! Three of the view's inputs are owned outside this crate —
//! `ReplicationQuorumChecker` (`frogdb-replication-runtime`), `RoleManager`
//! (the server crate) and the `ReplicaFeedGate`'s publisher (`frogdb-core`'s
//! client registry) — and the catalog has to live *here*, where the mutation
//! gate can see it (`.scratch/replication-correctness/PRD.md` §8 D7). Those
//! fields are therefore `Option`, filled by whichever caller can reach them.
//!
//! The same is true one level down, for a reason that is not about crate
//! boundaries at all: a seam builds the widest view *it* can reach.
//! [`crate::ReplicaSession::commit_phase`] owns one session and no offsets;
//! [`crate::replica::offset::AppliedOffset`] owns two counters and no registry.
//! Rather than invent zeroes for what a seam cannot see — which would report
//! violations that are artifacts of the capture — every group is optional and
//! every catalog entry declares the [`ViewField`]s it needs. An entry whose
//! inputs are absent is **skipped**, never failed.
//!
//! The honest cost, stated once here so nobody has to rediscover it:
//! `INV-FENCE-1` and `INV-ROLE-1` are checked less often than the rest, because
//! only a caller that can see the quorum checker or the role manager fills
//! their inputs.
//!
//! # Transition witnesses
//!
//! Three of the sixteen seed invariants state claims about a *transition*
//! rather than about a state: `INV-REPLID-2` (what a promotion must leave
//! behind), `INV-BACKLOG-2` (what a granted `+CONTINUE` may name) and
//! `INV-SESSION-1` (which way a session phase may move). A pure state snapshot
//! cannot express those, so the view carries three small witnesses —
//! [`PromotionWitness`], [`ContinueGrant`], [`PhaseChange`] — each filled only
//! by the seam that just performed the transition, and each `None` everywhere
//! else. They are still plain data: the catalog reads them, it does not call
//! back into anything.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use crate::replica_session::{Phase, ReplicaDeparture};
use crate::state::ReplicationState;

/// One capturable group of [`ReplicationView`]. A catalog entry declares the
/// groups it reads, and is skipped when the view in hand does not carry them.
///
/// The granularity is "what a seam can reach in one step", which is why `live`
/// is a field of its own rather than part of [`ViewField::Offsets`]: the
/// replica's applied/landed pair is owned by a type that does not hold the
/// received head at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ViewField {
    /// [`ReplicationView::state`] — replid, failover window, `offset_at_save`.
    State,
    /// [`ReplicationView::offsets`] — the applied/landed pair.
    Offsets,
    /// [`OffsetTriple::live`] — the received/write head. Separate from
    /// [`Self::Offsets`] because [`crate::replica::offset::AppliedOffset`] owns
    /// the pair and never the head.
    LiveOffset,
    /// [`ReplicationView::apply_gate`] — frozen/stint/epoch/diverged.
    ApplyGate,
    /// [`ReplicationView::backlog`] — the ring's window and totals.
    Backlog,
    /// [`ReplicationView::replicas`] — the session registry.
    /// [`ReplicationView::departure`] rides with it: both come from the
    /// tracker, so "no departure recorded" is only readable when this group is
    /// present.
    Replicas,
    /// [`ReplicationView::feed_gate`] — the slot-handoff hold.
    FeedGate,
    /// [`ReplicationView::fence`] — the self-fence quorum checker
    /// (`frogdb-replication-runtime`).
    Fence,
    /// [`ReplicationView::role`] — this node's role (the server crate).
    Role,
    /// [`ReplicationView::promotion`] — a promotion that just completed.
    Promotion,
    /// [`ReplicationView::grant`] — a `+CONTINUE` that was just granted.
    Grant,
    /// [`ReplicationView::phase_change`] — a session phase that just moved.
    PhaseChange,
}

/// The offset triple: what this node received, what it has applied, and what a
/// shard has actually landed.
///
/// `live` is optional because the applied/landed pair has an owner
/// ([`crate::replica::offset::AppliedOffset`]) that does not hold the received
/// head; every seam that owns the head fills it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetTriple {
    /// The live head: the primary's write position, or the replica's received
    /// position. `None` when the capturing seam cannot see it.
    pub live: Option<u64>,
    /// The offset of the data this node holds — everything claimed.
    pub applied: u64,
    /// The offset a shard has actually applied, and the only offset a replica
    /// ACKs.
    pub landed: u64,
}

/// The replica applier's admission gate, as
/// [`crate::replica::offset::AppliedOffset`] holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyGateView {
    /// Set by a promotion: no stint may claim past this point.
    pub frozen: bool,
    /// The current replica applying stint.
    pub stint: u64,
    /// The history epoch — bumped by every full resync.
    pub epoch: u64,
    /// The history epoch a replicated apply failed on, or `None` when no
    /// divergence is outstanding.
    pub diverged: Option<u64>,
}

/// The replication backlog's window and totals.
///
/// `BacklogGeometry` — what `INFO replication` renders — is *derived* from
/// these fields plus the live head (`active`/`first_byte_offset` from
/// [`Self::start_offset`], `histlen` from `live - start_offset`), so it is not
/// carried here as well: one source of truth, not two that can drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogView {
    /// Lowest offset a `+CONTINUE` may resume from (Redis `repl_backlog_off`),
    /// or `None` while the window is closed.
    pub start_offset: Option<u64>,
    /// The offset the oldest retained entry begins *at* — its end offset minus
    /// its own length — or `None` when the ring is empty. This, not
    /// [`Self::oldest_end`], is the lowest offset the retained data can
    /// actually serve.
    pub oldest_begin: Option<u64>,
    /// **End** offset of the oldest retained entry. Sits one entry above
    /// [`Self::oldest_begin`]: an entry spanning `(a, b]` is replayable from
    /// `a`, and `start_offset` is legal anywhere in `[a, b]`.
    pub oldest_end: Option<u64>,
    /// End offset of the newest retained entry, i.e. the head of the buffered
    /// range.
    pub newest_offset: Option<u64>,
    /// Entries retained right now.
    pub entries: usize,
    /// Bytes of RESP payload retained right now — the sum of the retained
    /// entries' lengths, which is also the length of the offset range they
    /// cover when the ring is contiguous.
    pub bytes: usize,
    /// The configured entry cap.
    pub max_entries: usize,
    /// The configured byte cap.
    pub max_bytes: usize,
}

/// One registered replica session, as the catalog needs to see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaView {
    /// The tracker's session id. Fresh on every reconnect.
    pub id: u64,
    /// The peer address the session was accepted from.
    pub addr: SocketAddr,
    /// The identity the replica announced over `REPLCONF listening-port`:
    /// `(peer ip, announced port)`. `None` when it announced nothing, which is
    /// *unknown* rather than "port 0" — two unannounced sessions are not
    /// evidence of the same replica.
    pub announced_id: Option<(IpAddr, u16)>,
    /// The session's lifecycle phase.
    pub phase: Phase,
    /// What the replica acknowledged **on the wire**. `0` means "has not acked
    /// yet", never "resumed at 0".
    pub acked: u64,
    /// Where the primary started forwarding from for this session — its PSYNC
    /// offset, or a full sync's snapshot offset. Sender-side bookkeeping, never
    /// a durability claim.
    pub resume_floor: u64,
    /// How long ago the last ACK (or resume) landed, sampled at capture time so
    /// the catalog itself reads no clock.
    pub last_ack_age: Duration,
}

/// The slot-handoff replica-feed hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedGateView {
    /// What the gate answers when a session asks whether it must wait —
    /// sampled from the same accessor the feed loop uses.
    pub is_held: bool,
    /// How much of the published hold is left at capture time, or `None` when
    /// nothing is held: either nothing was published, or the published
    /// deadline has already passed and the gate has expired itself.
    ///
    /// Sampled against the clock here rather than in the catalog, so the
    /// catalog stays a pure function. Captured *independently* of
    /// [`Self::is_held`] so the catalog can hold the two answers against each
    /// other — a gate that says "released" while still holding is exactly the
    /// shape of the revert this claim exists to catch.
    pub hold_remaining: Option<Duration>,
    /// The longest a slot-handoff barrier may hold the feed, when the capturing
    /// caller knows it. `None` at the gate itself, which is published to and
    /// never told the budget.
    pub barrier_budget: Option<Duration>,
}

/// The self-fence quorum checker, owned by `frogdb-replication-runtime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceView {
    /// Live `replication.self-fence-on-replica-loss`.
    pub self_fence_enabled: bool,
    /// Whether the checker has latched "a replica has streamed at least once".
    pub armed: bool,
    /// Live `replication.replica-freshness-timeout-ms`.
    pub freshness_window: Duration,
}

/// This node's replication role, owned by the server crate's `RoleManager` —
/// except on the replica connection, which knows it is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleView {
    /// Heads its own history.
    Primary,
    /// Follows `upstream` (`None` when the address is not reachable from the
    /// capturing seam).
    Replica { upstream: Option<SocketAddr> },
}

/// A promotion that just completed, captured by
/// [`crate::PrimaryReplicationHandler::begin_primary_stint`].
///
/// The identity half of a promotion is a *transition* claim — "the id you used
/// to head is now the failover window, frozen where you stopped" — so it needs
/// the id the node held before the mint, which no snapshot of the state after
/// the mint still carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionWitness {
    /// The replication id this node headed immediately before the mint.
    pub previous_id: String,
    /// The boundary the promotion froze at —
    /// [`crate::OffsetCoordinator::settle_at_applied`]'s answer.
    pub boundary: u64,
}

/// A `+CONTINUE` that was just granted, captured where the decision is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinueGrant {
    /// The offset the replica resumes *from* — its PSYNC offset.
    pub replay_from: u64,
    /// The offset the replay carries it *to* — the live head at grant time.
    pub resume_offset: u64,
}

/// A session phase that just moved, captured by
/// [`crate::ReplicaSession::commit_phase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseChange {
    /// The session whose phase moved.
    pub replica_id: u64,
    /// Where it was.
    pub from: Phase,
    /// Where it is now.
    pub to: Phase,
}

/// The plain-data projection the invariant catalog is written against.
///
/// Build one with [`ReplicationView::empty`] and the `with_*` combinators; each
/// component's owner has a capture method that fills the groups it can reach
/// (e.g. [`crate::PrimaryReplicationHandler::view`], which fills nearly all of
/// them).
// Deliberately not `Default`: [`ReplicationView::empty`] is the one way to start
// a view, so "carries nothing" is written once, in a place whose doc comment says
// what it means.
#[derive(Debug, Clone)]
pub struct ReplicationView {
    /// The persisted identity: replid, failover window, `offset_at_save`.
    pub state: Option<ReplicationState>,
    /// The offset triple.
    pub offsets: Option<OffsetTriple>,
    /// The replica applier's gate.
    pub apply_gate: Option<ApplyGateView>,
    /// The backlog window and totals.
    pub backlog: Option<BacklogView>,
    /// The session registry, in tracker order made deterministic by session id.
    pub replicas: Option<Vec<ReplicaView>>,
    /// How the most recent *streaming* replica left. Meaningful only when
    /// [`Self::replicas`] is present — both come from the tracker — where
    /// `None` reads as "no streaming replica has departed".
    pub departure: Option<ReplicaDeparture>,
    /// The slot-handoff feed hold.
    pub feed_gate: Option<FeedGateView>,
    /// The self-fence quorum checker.
    pub fence: Option<FenceView>,
    /// This node's role.
    pub role: Option<RoleView>,
    /// A promotion that just completed.
    pub promotion: Option<PromotionWitness>,
    /// A `+CONTINUE` that was just granted.
    pub grant: Option<ContinueGrant>,
    /// A session phase that just moved.
    pub phase_change: Option<PhaseChange>,
}

impl ReplicationView {
    /// A view that carries nothing. Every catalog entry is skipped against it,
    /// which is the correct reading: an empty capture is evidence of nothing.
    pub fn empty() -> Self {
        Self {
            state: None,
            offsets: None,
            apply_gate: None,
            backlog: None,
            replicas: None,
            departure: None,
            feed_gate: None,
            fence: None,
            role: None,
            promotion: None,
            grant: None,
            phase_change: None,
        }
    }

    /// Whether this view carries `field`.
    pub fn has(&self, field: ViewField) -> bool {
        match field {
            ViewField::State => self.state.is_some(),
            ViewField::Offsets => self.offsets.is_some(),
            ViewField::LiveOffset => self.offsets.is_some_and(|o| o.live.is_some()),
            ViewField::ApplyGate => self.apply_gate.is_some(),
            ViewField::Backlog => self.backlog.is_some(),
            ViewField::Replicas => self.replicas.is_some(),
            ViewField::FeedGate => self.feed_gate.is_some(),
            ViewField::Fence => self.fence.is_some(),
            ViewField::Role => self.role.is_some(),
            ViewField::Promotion => self.promotion.is_some(),
            ViewField::Grant => self.grant.is_some(),
            ViewField::PhaseChange => self.phase_change.is_some(),
        }
    }

    /// The live head, when this view carries one.
    pub fn live(&self) -> Option<u64> {
        self.offsets.and_then(|o| o.live)
    }

    /// Attach the persisted identity.
    #[must_use]
    pub fn with_state(mut self, state: ReplicationState) -> Self {
        self.state = Some(state);
        self
    }

    /// Attach the full offset triple.
    #[must_use]
    pub fn with_offsets(mut self, live: u64, applied: u64, landed: u64) -> Self {
        self.offsets = Some(OffsetTriple {
            live: Some(live),
            applied,
            landed,
        });
        self
    }

    /// Attach the applied/landed pair from a seam that cannot see the received
    /// head.
    #[must_use]
    pub fn with_applied_pair(mut self, applied: u64, landed: u64) -> Self {
        self.offsets = Some(OffsetTriple {
            live: None,
            applied,
            landed,
        });
        self
    }

    /// Attach the applier's gate.
    #[must_use]
    pub fn with_apply_gate(mut self, gate: ApplyGateView) -> Self {
        self.apply_gate = Some(gate);
        self
    }

    /// Attach the backlog window.
    #[must_use]
    pub fn with_backlog(mut self, backlog: BacklogView) -> Self {
        self.backlog = Some(backlog);
        self
    }

    /// Attach the session registry and the departure latch that rides with it.
    #[must_use]
    pub fn with_replicas(
        mut self,
        replicas: Vec<ReplicaView>,
        departure: Option<ReplicaDeparture>,
    ) -> Self {
        self.replicas = Some(replicas);
        self.departure = departure;
        self
    }

    /// Attach the feed hold.
    #[must_use]
    pub fn with_feed_gate(mut self, feed_gate: FeedGateView) -> Self {
        self.feed_gate = Some(feed_gate);
        self
    }

    /// Attach the self-fence checker's state.
    #[must_use]
    pub fn with_fence(mut self, fence: FenceView) -> Self {
        self.fence = Some(fence);
        self
    }

    /// Attach this node's role.
    #[must_use]
    pub fn with_role(mut self, role: RoleView) -> Self {
        self.role = Some(role);
        self
    }

    /// Attach the promotion witness.
    #[must_use]
    pub fn with_promotion(mut self, promotion: PromotionWitness) -> Self {
        self.promotion = Some(promotion);
        self
    }

    /// Attach the `+CONTINUE` grant witness.
    #[must_use]
    pub fn with_grant(mut self, grant: ContinueGrant) -> Self {
        self.grant = Some(grant);
        self
    }

    /// Attach the phase-change witness.
    #[must_use]
    pub fn with_phase_change(mut self, change: PhaseChange) -> Self {
        self.phase_change = Some(change);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_view_carries_no_field() {
        let view = ReplicationView::empty();
        for field in [
            ViewField::State,
            ViewField::Offsets,
            ViewField::LiveOffset,
            ViewField::ApplyGate,
            ViewField::Backlog,
            ViewField::Replicas,
            ViewField::FeedGate,
            ViewField::Fence,
            ViewField::Role,
            ViewField::Promotion,
            ViewField::Grant,
            ViewField::PhaseChange,
        ] {
            assert!(
                !view.has(field),
                "{field:?} must be absent from an empty view"
            );
        }
    }

    /// The applied/landed pair is a *different* claim from the full triple: a
    /// seam that cannot see the received head must not be read as having
    /// reported one.
    #[test]
    fn an_applied_pair_carries_offsets_but_not_the_live_head() {
        let view = ReplicationView::empty().with_applied_pair(7, 5);
        assert!(view.has(ViewField::Offsets));
        assert!(!view.has(ViewField::LiveOffset));
        assert_eq!(view.live(), None);

        let full = ReplicationView::empty().with_offsets(9, 7, 5);
        assert!(full.has(ViewField::Offsets));
        assert!(full.has(ViewField::LiveOffset));
        assert_eq!(full.live(), Some(9));
    }

    /// The departure latch is only readable through the registry it belongs to,
    /// so attaching replicas is what makes it present.
    #[test]
    fn the_departure_latch_rides_with_the_registry() {
        let view = ReplicationView::empty().with_replicas(Vec::new(), None);
        assert!(view.has(ViewField::Replicas));
        assert_eq!(view.departure, None);

        let lost = ReplicationView::empty().with_replicas(Vec::new(), Some(ReplicaDeparture::Lost));
        assert_eq!(lost.departure, Some(ReplicaDeparture::Lost));
    }
}
