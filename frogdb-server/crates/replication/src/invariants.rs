//! The replication invariant catalog: what is true of this node's replication
//! state after every transition, stated once, checked everywhere.
//!
//! The catalog is pure functions over [`ReplicationView`] — the plain-data
//! projection described in [`crate::view`]. Read that module first; in
//! particular, every group of the view is optional, and **an entry whose
//! required [`ViewField`]s are absent is skipped, not failed**. A narrow seam
//! that can only see one session must not be read as claiming the offsets are
//! zero.
//!
//! # Two tiers, no third
//!
//! [`Tier`], [`Citation`] and [`Violation`] come from `frogdb-types` (PRD §8
//! D6), shared with `frogdb-cluster`'s catalog so a citation means the same
//! thing in both. [`Tier::Hard`] is the default and the presumption: a
//! violation is a defect. [`Tier::DocumentedException`] carries the ruling that
//! makes a reachable-but-dirty state legitimate, and because the citation is a
//! variant field built by an asserting `const fn`, an exception without one is
//! a build error rather than a review comment.
//!
//! # The seed sixteen
//!
//! Every entry names the defect class it would have caught, so none of them is
//! decorative. Three — `INV-REPLID-2`, `INV-BACKLOG-2`, `INV-SESSION-1` — are
//! claims about a *transition* and read one of the view's witnesses; see
//! [`crate::view`].
//!
//! # Where this is checked
//!
//! [`debug_assert_view_clean`] is installed at the seams named in
//! `.scratch/replication-correctness/PRD.md` §3 W1, under
//! `#[cfg(any(test, debug_assertions))]`. Every hooked seam has a single exit,
//! so no early `return` can slip past the hook — the same discipline
//! `frogdb-cluster` bought by splitting its transition into a private
//! `apply_to`. That turns the crate's own tests and the server's
//! `integration_replication.rs` suite into invariant tests at no authoring
//! cost.

use std::collections::BTreeMap;

pub use frogdb_types::{Citation, Tier, Violation};

use crate::replica_session::{Phase, ReplicaDeparture};
use crate::state::is_valid_replication_id;
use crate::view::{ReplicationView, RoleView, ViewField};

/// One catalog entry: a claim about [`ReplicationView`], the fields it needs to
/// evaluate that claim, and how seriously a violation is taken.
pub struct Invariant {
    /// Stable id, e.g. `"INV-REPLID-1"`. Quoted by specs, issues and
    /// `DEBUG REPLICATION CHECK`; stable across refactors.
    pub id: &'static str,
    /// The claim, in one sentence, in the terms an operator would use.
    pub claim: &'static str,
    /// How seriously a violation is taken.
    pub tier: Tier,
    /// The view groups this entry reads. When the view in hand is missing any
    /// of them the entry is **skipped**: absence is not evidence.
    pub requires: &'static [ViewField],
    /// The check itself. Called only when every [`Self::requires`] field is
    /// present, so it may unwrap those groups.
    check: fn(&ReplicationView) -> Vec<Violation>,
}

impl Invariant {
    /// Whether a violation of this entry is a defect by definition.
    pub fn is_hard(&self) -> bool {
        matches!(self.tier, Tier::Hard)
    }

    /// Whether `view` carries everything this entry needs to be evaluated.
    pub fn is_checkable(&self, view: &ReplicationView) -> bool {
        self.requires.iter().all(|field| view.has(*field))
    }
}

/// The seed catalog: PRD §3 W1, in the order that document lists it.
pub static CATALOG: &[Invariant] = &[
    Invariant {
        id: "INV-REPLID-1",
        claim: "the failover window is whole: a secondary replication id exists exactly when a \
                secondary offset does",
        tier: Tier::Hard,
        requires: &[ViewField::State],
        check: inv_replid_1,
    },
    Invariant {
        id: "INV-REPLID-2",
        claim: "a promotion leaves the id it used to head as the secondary id, frozen at the \
                offset it stopped heading",
        tier: Tier::Hard,
        requires: &[ViewField::State, ViewField::Promotion],
        check: inv_replid_2,
    },
    Invariant {
        id: "INV-REPLID-3",
        claim: "both replication ids are well-formed and distinct",
        tier: Tier::Hard,
        requires: &[ViewField::State],
        check: inv_replid_3,
    },
    Invariant {
        id: "INV-OFFSET-1",
        claim: "landed never runs ahead of applied, and applied never runs ahead of live",
        tier: Tier::Hard,
        requires: &[ViewField::Offsets],
        check: inv_offset_1,
    },
    Invariant {
        id: "INV-OFFSET-2",
        claim: "the offset written to the state file never claims more than the live offset",
        // Both reconcile paths bump `offset_at_save` with a `max`, and nothing
        // ever lowers it: a node that ran to X, then followed a history whose
        // head is below X (a `+FULLRESYNC` to a lower offset), keeps X on disk
        // while holding data only up to the new head. Reachable today, and the
        // ruling on which of the two behaviours is right is issue 17 — until it
        // lands, a hook that panicked here would fire on shipped behaviour.
        tier: Tier::DocumentedException(Citation::issue(
            ".scratch/replication-correctness/issues/17",
        )),
        requires: &[ViewField::State, ViewField::LiveOffset],
        check: inv_offset_2,
    },
    Invariant {
        id: "INV-OFFSET-3",
        claim: "no replica is credited past the live offset, and none is credited at all before \
                it can have acked on the wire",
        tier: Tier::Hard,
        requires: &[ViewField::LiveOffset, ViewField::Replicas],
        check: inv_offset_3,
    },
    Invariant {
        id: "INV-OFFSET-4",
        claim: "the frozen failover offset never exceeds the live offset, outside an in-flight \
                full resync",
        tier: Tier::Hard,
        requires: &[ViewField::State, ViewField::LiveOffset],
        check: inv_offset_4,
    },
    Invariant {
        id: "INV-BACKLOG-1",
        claim: "the backlog floor sits inside the oldest retained entry, and the retained entries \
                cover their range without a hole",
        tier: Tier::Hard,
        requires: &[ViewField::Backlog],
        check: inv_backlog_1,
    },
    Invariant {
        id: "INV-BACKLOG-2",
        claim: "a granted +CONTINUE resumes at or above the backlog floor and at or below the \
                offset it replays to",
        tier: Tier::Hard,
        requires: &[ViewField::Backlog, ViewField::Grant],
        check: inv_backlog_2,
    },
    Invariant {
        id: "INV-BACKLOG-3",
        claim: "the backlog holds within both configured caps",
        tier: Tier::Hard,
        requires: &[ViewField::Backlog],
        check: inv_backlog_3,
    },
    Invariant {
        id: "INV-SESSION-1",
        claim: "a session phase only moves forward, and nothing leaves Disconnecting",
        tier: Tier::Hard,
        requires: &[ViewField::PhaseChange],
        check: inv_session_1,
    },
    Invariant {
        id: "INV-SESSION-2",
        claim: "at most one session per announced replica identity is streaming",
        tier: Tier::Hard,
        requires: &[ViewField::Replicas],
        check: inv_session_2,
    },
    Invariant {
        id: "INV-SESSION-3",
        claim: "a recorded streaming departure implies a session actually reached Streaming",
        tier: Tier::Hard,
        requires: &[ViewField::Replicas, ViewField::Fence],
        check: inv_session_3,
    },
    Invariant {
        id: "INV-GATE-1",
        claim: "the feed gate's held answer and its remaining hold agree, and a hold stays inside \
                the barrier budget",
        tier: Tier::Hard,
        requires: &[ViewField::FeedGate],
        check: inv_gate_1,
    },
    Invariant {
        id: "INV-FENCE-1",
        claim: "the self-fence checker is armed whenever a session is streaming, and only a \
                graceful departure disarms it",
        tier: Tier::Hard,
        requires: &[ViewField::Replicas, ViewField::Fence],
        check: inv_fence_1,
    },
    Invariant {
        id: "INV-ROLE-1",
        claim: "a node that follows an upstream serves no downstream streaming session",
        // Chained replication (replica-of-replica) is not a supported topology
        // and is not refused either: a replica will happily accept a PSYNC and
        // stream its own tail. Ruled a known non-guarantee rather than a defect,
        // which is exactly what this tier is for.
        tier: Tier::DocumentedException(Citation::issue(
            ".scratch/testing-improvements/issues/done/48-chained-replication-contract.md",
        )),
        requires: &[ViewField::Role, ViewField::Replicas],
        check: inv_role_1,
    },
];

// ---- the checks --------------------------------------------------------
//
// Each is called only when its entry's `requires` fields are present, so the
// `expect`s below are structural, not hopeful.

fn state(view: &ReplicationView) -> &crate::state::ReplicationState {
    view.state.as_ref().expect("entry requires State")
}

fn live(view: &ReplicationView) -> u64 {
    view.live().expect("entry requires LiveOffset")
}

fn replicas(view: &ReplicationView) -> &[crate::view::ReplicaView] {
    view.replicas.as_deref().expect("entry requires Replicas")
}

fn backlog(view: &ReplicationView) -> crate::view::BacklogView {
    view.backlog.expect("entry requires Backlog")
}

/// The half-cleared failover window: an id with no offset (or an offset with no
/// id) makes `window_contains` answer about a history that was never recorded,
/// so a replica gets a `+CONTINUE` into a stream that does not exist.
///
/// Generalizes FM-REPLICATION-013 (both `+CONTINUE` arms are `window_contains`),
/// FM-REPLICATION-019 and FM-REPLICATION-020 (a promotion writes both halves or
/// rolls both back), FM-REPLICATION-021 (what a reload may reconstitute),
/// FM-REPLICATION-022 (`clear_secondary_window` clears both halves in one call)
/// and FM-REPLICATION-023 (the `None`/`-1` pair INFO refuses to render as a
/// window) — plus FM-REPLICATION-001, whose granted resync adopts neither half.
fn inv_replid_1(view: &ReplicationView) -> Vec<Violation> {
    let state = state(view);
    let has_id = state.secondary_id.is_some();
    let has_offset = state.secondary_offset >= 0;
    if has_id == has_offset {
        return Vec::new();
    }
    vec![Violation::new(
        "INV-REPLID-1",
        format!(
            "half-cleared failover window: secondary_id={:?} but secondary_offset={}",
            state.secondary_id, state.secondary_offset
        ),
    )]
}

/// Revert (b) `f6484219`: a promotion that mints a new id without recording the
/// old one loses the only evidence that the replicas still following the old id
/// are followers of *this* history, so every one of them full-resyncs.
///
/// Generalizes FM-REPLICATION-019 (the mint freezes the inherited history at the
/// applied boundary) and FM-REPLICATION-020 (a promotion that cannot persist
/// restores the pair it was on).
fn inv_replid_2(view: &ReplicationView) -> Vec<Violation> {
    let state = state(view);
    let promotion = view.promotion.as_ref().expect("entry requires Promotion");
    let mut violations = Vec::new();
    if state.secondary_id.as_deref() != Some(promotion.previous_id.as_str()) {
        violations.push(Violation::new(
            "INV-REPLID-2",
            format!(
                "promotion did not inherit its previous history: headed {} before the mint, \
                 secondary_id is {:?}",
                promotion.previous_id, state.secondary_id
            ),
        ));
    }
    if state.secondary_offset != promotion.boundary as i64 {
        violations.push(Violation::new(
            "INV-REPLID-2",
            format!(
                "promotion froze at {} but secondary_offset is {}",
                promotion.boundary, state.secondary_offset
            ),
        ));
    }
    violations
}

/// A malformed id is unmatchable, so every PSYNC against it full-resyncs; an id
/// equal to its own secondary claims the same history twice and makes
/// `window_contains` accept offsets from before the failover as if they were
/// current.
///
/// Generalizes FM-REPLICATION-013 (an unknown replid always full-resyncs, which
/// needs the two ids to be distinguishable), FM-REPLICATION-019 (the mint is
/// distinct from the id it froze), FM-REPLICATION-021 (a reload's ids are
/// well-formed) and FM-REPLICATION-023 (one identity per node, at every point
/// in its life).
fn inv_replid_3(view: &ReplicationView) -> Vec<Violation> {
    let state = state(view);
    let mut violations = Vec::new();
    if !is_valid_replication_id(&state.replication_id) {
        violations.push(Violation::new(
            "INV-REPLID-3",
            format!("malformed replication_id {:?}", state.replication_id),
        ));
    }
    if let Some(secondary) = &state.secondary_id {
        if !is_valid_replication_id(secondary) {
            violations.push(Violation::new(
                "INV-REPLID-3",
                format!("malformed secondary_id {secondary:?}"),
            ));
        }
        if *secondary == state.replication_id {
            violations.push(Violation::new(
                "INV-REPLID-3",
                format!("replication_id and secondary_id are both {secondary}"),
            ));
        }
    }
    violations
}

/// The durability chain. `landed` is the only offset a replica may ACK and the
/// only one `WAIT` may count; `applied` is what this node claims to hold; `live`
/// is what it has received. Any inversion means something acknowledged data it
/// does not have.
///
/// The `applied <= live` clause is checked only where the capturing seam could
/// see the received head — [`crate::replica::offset::AppliedOffset`] owns the
/// pair and never the head, so at those seams the chain is the pair alone. That
/// is a narrower check, not a skipped one: the pair inversion is the half that
/// is reachable from there.
///
/// Generalizes FM-REPLICATION-008: an ACK is a durability claim, and the
/// ordering of the three heads is what makes `WAIT` count applies rather than
/// receipts.
fn inv_offset_1(view: &ReplicationView) -> Vec<Violation> {
    let offsets = view.offsets.expect("entry requires Offsets");
    let mut violations = Vec::new();
    if offsets.landed > offsets.applied {
        violations.push(Violation::new(
            "INV-OFFSET-1",
            format!(
                "landed {} runs ahead of applied {}",
                offsets.landed, offsets.applied
            ),
        ));
    }
    if let Some(live) = offsets.live
        && offsets.applied > live
    {
        violations.push(Violation::new(
            "INV-OFFSET-1",
            format!("applied {} runs ahead of live {live}", offsets.applied),
        ));
    }
    violations
}

/// A state file that claims an offset the node never reached makes recovery
/// resume above its own data: the gap is never replayed and never noticed.
///
/// Cited by FM-REPLICATION-021 as the exception it is, not as a guarantee: that
/// row's raise-only `offset_at_save` is the path that reaches this state, and
/// the ruling on which behaviour is right is the entry's own citation.
fn inv_offset_2(view: &ReplicationView) -> Vec<Violation> {
    let state = state(view);
    let live = live(view);
    if state.offset_at_save <= live {
        return Vec::new();
    }
    vec![Violation::new(
        "INV-OFFSET-2",
        format!(
            "offset_at_save {} claims more than live {live}",
            state.offset_at_save
        ),
    )]
}

/// Revert (c) `90fefaf7`: seeding a replica's acked offset at registration made
/// `WAIT` count a replica that had never acknowledged anything, so a write
/// "confirmed by two replicas" could exist on one node.
///
/// Generalizes FM-REPLICATION-037 (`WAIT` never invents a number),
/// FM-REPLICATION-039 (the count is a set of streaming replicas at or past the
/// target), FM-REPLICATION-043 (the one registry both renderers project) and
/// FM-REPLICATION-015 (seeding the tracker at the resume point credits nothing
/// beyond the live head).
fn inv_offset_3(view: &ReplicationView) -> Vec<Violation> {
    let live = live(view);
    let mut violations = Vec::new();
    for replica in replicas(view) {
        if replica.acked > live {
            violations.push(Violation::new(
                "INV-OFFSET-3",
                format!(
                    "replica {} acked {} past live {live}",
                    replica.id, replica.acked
                ),
            ));
        }
        if replica.resume_floor > live {
            violations.push(Violation::new(
                "INV-OFFSET-3",
                format!(
                    "replica {} resumes from {} past live {live}",
                    replica.id, replica.resume_floor
                ),
            ));
        }
        // A session below Streaming has not been sent a byte of the live
        // stream, so it cannot have acked one. `Disconnecting` is excluded
        // because it is *past* Streaming: a departing replica keeps whatever it
        // legitimately acked.
        let before_the_wire = matches!(
            replica.phase,
            Phase::Connecting | Phase::PreparingCheckpoint | Phase::StreamingCheckpoint
        );
        if before_the_wire && replica.acked != 0 {
            violations.push(Violation::new(
                "INV-OFFSET-3",
                format!(
                    "replica {} is credited with ack {} while still in {}",
                    replica.id, replica.acked, replica.phase
                ),
            ));
        }
    }
    violations
}

/// A frozen failover offset above the live offset makes `window_contains`
/// accept a resume request for a range this node never wrote.
///
/// Skipped at a live head of 0, which is not "this node wrote nothing" but "a
/// full resync is in flight": taking a `+FULLRESYNC` grant rewinds the live head
/// to 0 *before* the dataset lands, and the window deliberately keeps standing
/// over the keyspace this node is still holding until the payload is installed
/// (FM-REPLICATION-001, round-2 issue 51 — see
/// `a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone`).
/// The narrowing costs nothing: a window over a genuinely empty stream is
/// unreachable, because a window is only ever frozen from an offset the node
/// reached.
///
/// Generalizes FM-REPLICATION-019 (a promotion freezes at the applied boundary,
/// which is at or below the live head); the skip above is FM-REPLICATION-001's
/// in-flight resync, which is why that row cites this entry too.
fn inv_offset_4(view: &ReplicationView) -> Vec<Violation> {
    let state = state(view);
    let live = live(view);
    // `secondary_offset < 0` is the "no window" sentinel. Its boundary is
    // unobservable — a window frozen at exactly 0 is caught by the clause after
    // it, since a live head of 0 already returned above — so no test can tell
    // `< 0` from `<= 0` here, and `cargo mutants` reports that flip as missed.
    if live == 0 || state.secondary_offset < 0 || state.secondary_offset as u64 <= live {
        return Vec::new();
    }
    vec![Violation::new(
        "INV-OFFSET-4",
        format!(
            "secondary_offset {} exceeds live {live}",
            state.secondary_offset
        ),
    )]
}

/// FM-REPLICATION-016. The floor is what `+CONTINUE` is granted against; the
/// entries are what the grant is served from. A floor below the data — or a
/// hole between entries — means a grant this node cannot honor, and the replica
/// resumes into a stream missing the commands in the gap.
///
/// Also generalizes FM-REPLICATION-009 (freeing an idle window disarms the floor
/// with the entries), FM-REPLICATION-012 (the extraction re-reads the floor
/// under the entries lock), FM-REPLICATION-014 (the armed floor is the only
/// lower bound) and FM-REPLICATION-059 (the geometry both renderers report).
fn inv_backlog_1(view: &ReplicationView) -> Vec<Violation> {
    let backlog = backlog(view);
    let mut violations = Vec::new();
    let (Some(start), Some(oldest_begin), Some(oldest_end), Some(newest)) = (
        backlog.start_offset,
        backlog.oldest_begin,
        backlog.oldest_end,
        backlog.newest_offset,
    ) else {
        // An unarmed or empty ring claims no history, which is always honest.
        return violations;
    };
    if start < oldest_begin || start > oldest_end {
        violations.push(Violation::new(
            "INV-BACKLOG-1",
            format!(
                "backlog floor {start} sits outside the oldest entry ({oldest_begin}, {oldest_end}]"
            ),
        ));
    }
    // Every buffered entry spans exactly its own payload length, and the live
    // offset advances by exactly that much per command, so a contiguous ring
    // covers `newest - oldest_begin` bytes and a holed one covers fewer.
    let span = newest.saturating_sub(oldest_begin);
    if span != backlog.bytes as u64 {
        violations.push(Violation::new(
            "INV-BACKLOG-1",
            format!(
                "backlog covers ({oldest_begin}, {newest}] = {span} bytes but retains {} bytes \
                 across {} entries",
                backlog.bytes, backlog.entries
            ),
        ));
    }
    violations
}

/// FM-REPLICATION-014. A grant below the floor names data that has been
/// evicted; a grant above the offset it replays to names data that does not
/// exist yet. Either way the replica is told it is caught up when it is not.
///
/// Also generalizes FM-REPLICATION-012 (a resume evicted after the grant is
/// abandoned, not truncated) and FM-REPLICATION-015 (a grant replays exactly
/// `(replay_from, current]`).
fn inv_backlog_2(view: &ReplicationView) -> Vec<Violation> {
    let backlog = backlog(view);
    let grant = view.grant.expect("entry requires Grant");
    let mut violations = Vec::new();
    match backlog.start_offset {
        Some(start) if grant.replay_from < start => violations.push(Violation::new(
            "INV-BACKLOG-2",
            format!(
                "granted +CONTINUE from {} below the backlog floor {start}",
                grant.replay_from
            ),
        )),
        None => violations.push(Violation::new(
            "INV-BACKLOG-2",
            format!(
                "granted +CONTINUE from {} against a closed backlog window",
                grant.replay_from
            ),
        )),
        Some(_) => {}
    }
    if grant.replay_from > grant.resume_offset {
        violations.push(Violation::new(
            "INV-BACKLOG-2",
            format!(
                "granted +CONTINUE from {} above the offset it replays to {}",
                grant.replay_from, grant.resume_offset
            ),
        ));
    }
    violations
}

/// FM-REPLICATION-016/047. The caps are the whole memory bound on a primary
/// with a slow replica; a ring that outgrows them turns a lagging follower into
/// an OOM.
///
/// Also generalizes FM-REPLICATION-059: the geometry both renderers publish is
/// this ring's, so a report that could not describe a real buffer is a
/// violation here before it is a rendering bug there.
fn inv_backlog_3(view: &ReplicationView) -> Vec<Violation> {
    let backlog = backlog(view);
    let mut violations = Vec::new();
    // Both caps bind from the second entry on. The newest command is always
    // retained, whatever the caps say: the eviction loop stops on an empty
    // deque, because a loop that drains the ring cannot terminate and would
    // wedge every later write behind the entries lock (FM-REPLICATION-047). So
    // a degenerate `max_entries = 0`, or a single command larger than the whole
    // byte cap, legitimately leaves exactly one entry standing.
    if backlog.entries > backlog.max_entries.max(1) {
        violations.push(Violation::new(
            "INV-BACKLOG-3",
            format!(
                "backlog holds {} entries over the {} cap",
                backlog.entries, backlog.max_entries
            ),
        ));
    }
    if backlog.bytes > backlog.max_bytes && backlog.entries > 1 {
        violations.push(Violation::new(
            "INV-BACKLOG-3",
            format!(
                "backlog holds {} bytes over the {} cap across {} entries",
                backlog.bytes, backlog.max_bytes, backlog.entries
            ),
        ));
    }
    violations
}

/// A phase that moves backwards re-enters a stage whose side effects already
/// ran — a second checkpoint for a session already streaming, a second
/// registration for one already counted. `Disconnecting` is terminal because
/// cleanup has run: anything after it operates on a session that is gone.
///
/// Generalizes FM-REPLICATION-060: the phases INFO renders are the ones a
/// session actually passed through, in order, so a rendered state is never a
/// second pass through one.
fn inv_session_1(view: &ReplicationView) -> Vec<Violation> {
    let change = view.phase_change.expect("entry requires PhaseChange");
    let mut violations = Vec::new();
    if phase_rank(change.to) < phase_rank(change.from) {
        violations.push(Violation::new(
            "INV-SESSION-1",
            format!(
                "replica {} moved backwards from {} to {}",
                change.replica_id, change.from, change.to
            ),
        ));
    }
    if change.from == Phase::Disconnecting && change.to != Phase::Disconnecting {
        violations.push(Violation::new(
            "INV-SESSION-1",
            format!(
                "replica {} left the terminal Disconnecting phase for {}",
                change.replica_id, change.to
            ),
        ));
    }
    violations
}

/// Spec GAP-5. Two live sessions for one replica means `WAIT` counts one
/// follower twice, so a write acknowledged by a single node reports the quorum
/// the operator asked for.
///
/// Generalizes FM-REPLICATION-039 (the count is a set of *distinct* replicas),
/// FM-REPLICATION-043 (one registry, one entry per live session) and
/// FM-REPLICATION-049 (the announced identity compared here is recorded at the
/// handshake).
fn inv_session_2(view: &ReplicationView) -> Vec<Violation> {
    let mut streaming: BTreeMap<(std::net::IpAddr, u16), Vec<u64>> = BTreeMap::new();
    for replica in replicas(view) {
        // An unannounced session is *unknown*, not "port 0": two of them are no
        // evidence of the same replica, so they are not compared.
        let Some(identity) = replica.announced_id else {
            continue;
        };
        if replica.phase == Phase::Streaming {
            streaming.entry(identity).or_default().push(replica.id);
        }
    }
    streaming
        .into_iter()
        .filter(|(_, ids)| ids.len() > 1)
        .map(|((ip, port), ids)| {
            Violation::new(
                "INV-SESSION-2",
                format!("replica identity {ip}:{port} is streaming on sessions {ids:?}"),
            )
        })
        .collect()
}

/// FM-REPLICATION-062. The departure record is what a promotion and the
/// self-fence checker read to decide whether this node ever had a follower. A
/// record written for a session that never streamed makes both act on a
/// generation that did not exist; the arming latch is the independent witness
/// that one did, and only a graceful departure is allowed to clear it.
///
/// Also generalizes FM-REPLICATION-041, the other end of the same pair: the
/// fence arms on a session that streamed and disarms only on a graceful
/// departure.
fn inv_session_3(view: &ReplicationView) -> Vec<Violation> {
    let fence = view.fence.expect("entry requires Fence");
    if view.departure != Some(ReplicaDeparture::Lost) || fence.armed {
        return Vec::new();
    }
    vec![Violation::new(
        "INV-SESSION-3",
        "a Lost streaming departure is recorded but the self-fence checker was never armed, so \
         no session ever reached Streaming"
            .to_string(),
    )]
}

/// Revert (d) `8d55cc4f` / FM-CLUSTER-097. The gate is the only thing holding
/// the replica feed across a slot handoff. A gate that answers "released" while
/// a hold is still standing lets the feed run during the barrier — the exact
/// window the barrier exists to close — and one that holds past the budget
/// stalls every replica on a finalizer that already died.
///
/// The one entry no replication row cites: the gate lives in this crate, but
/// the transition it guards is a cluster one, and the vocabulary check is
/// per-area — a `Catalog` field on FM-CLUSTER-097 naming `INV-GATE-1` would be
/// a lint error, so the cross-reference lives here instead. See the `Catalog`
/// section of `specs/replication.md`.
fn inv_gate_1(view: &ReplicationView) -> Vec<Violation> {
    let gate = view.feed_gate.expect("entry requires FeedGate");
    let mut violations = Vec::new();
    if gate.is_held != gate.hold_remaining.is_some() {
        violations.push(Violation::new(
            "INV-GATE-1",
            format!(
                "feed gate reports held={} but its remaining hold is {:?}",
                gate.is_held, gate.hold_remaining
            ),
        ));
    }
    if let (Some(remaining), Some(budget)) = (gate.hold_remaining, gate.barrier_budget)
        && remaining > budget
    {
        violations.push(Violation::new(
            "INV-GATE-1",
            format!("feed gate holds for {remaining:?}, past the {budget:?} barrier budget"),
        ));
    }
    violations
}

/// FM-REPLICATION-041/062. The checker is what turns "my last replica left" into
/// a refusal to serve stale reads. Unarmed while a replica is streaming is the
/// dead-detector shape: the fence is configured, looks enabled, and can never
/// fire. Disarmed by a `Lost` departure is the same failure from the other end —
/// the one departure that *should* fence is the one that switched it off.
fn inv_fence_1(view: &ReplicationView) -> Vec<Violation> {
    let fence = view.fence.expect("entry requires Fence");
    let mut violations = Vec::new();
    let streaming: Vec<u64> = replicas(view)
        .iter()
        .filter(|replica| replica.phase == Phase::Streaming)
        .map(|replica| replica.id)
        .collect();
    if !fence.armed && !streaming.is_empty() {
        violations.push(Violation::new(
            "INV-FENCE-1",
            format!("sessions {streaming:?} are streaming but the self-fence checker is unarmed"),
        ));
    }
    if !fence.armed && view.departure == Some(ReplicaDeparture::Lost) {
        violations.push(Violation::new(
            "INV-FENCE-1",
            "the self-fence checker was disarmed by a Lost departure; only a Graceful one may \
             disarm it"
                .to_string(),
        ));
    }
    violations
}

/// Chained replication. A replica that serves its own followers relays a stream
/// it does not head: its offsets are its upstream's, so a `WAIT` against it
/// counts acks for data the sub-replica may never receive if the middle node is
/// itself behind. Ruled a documented non-guarantee — see the entry's citation.
///
/// Cited by FM-REPLICATION-022, which states the same claim point-wise for a
/// demotion: this entry is what would state it universally, if it were a
/// guarantee.
fn inv_role_1(view: &ReplicationView) -> Vec<Violation> {
    let role = view.role.as_ref().expect("entry requires Role");
    let RoleView::Replica { upstream } = role else {
        return Vec::new();
    };
    replicas(view)
        .iter()
        .filter(|replica| replica.phase == Phase::Streaming)
        .map(|replica| {
            Violation::new(
                "INV-ROLE-1",
                format!(
                    "node follows {upstream:?} yet streams to downstream session {} at {}",
                    replica.id, replica.addr
                ),
            )
        })
        .collect()
}

/// Declared phase order. Kept here rather than as an `Ord` impl on [`Phase`]
/// because "later in the lifecycle" is a claim this catalog makes about the
/// enum, not a general ordering of it.
fn phase_rank(phase: Phase) -> u8 {
    match phase {
        Phase::Connecting => 0,
        Phase::PreparingCheckpoint => 1,
        Phase::StreamingCheckpoint => 2,
        Phase::Streaming => 3,
        Phase::Disconnecting => 4,
    }
}

// ---- running the catalog -----------------------------------------------

fn check_catalog(view: &ReplicationView, hard_only: bool) -> Vec<Violation> {
    CATALOG
        .iter()
        .filter(|invariant| !hard_only || invariant.is_hard())
        .filter(|invariant| invariant.is_checkable(view))
        .flat_map(|invariant| (invariant.check)(view))
        .collect()
}

/// Every violation `view` exhibits, both tiers — what `DEBUG REPLICATION CHECK`
/// reports and what a triage pass reads.
pub fn check_all(view: &ReplicationView) -> Vec<Violation> {
    check_catalog(view, false)
}

/// Only the violations that are defects by definition — what the seam hooks
/// assert on. Documented exceptions are excluded by construction, so a hook can
/// never panic on a state something already ruled legitimate.
pub fn check_hard(view: &ReplicationView) -> Vec<Violation> {
    check_catalog(view, true)
}

/// Render violations one per line, for a panic body or a `DEBUG` reply.
pub fn render(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The seam hook: panic if `view` violates a HARD entry after `seam` ran.
///
/// Debug and test builds only, so a release binary pays nothing. `#[track_caller]`
/// so the panic names the seam's call site rather than this function.
#[cfg(any(test, debug_assertions))]
#[track_caller]
pub(crate) fn debug_assert_view_clean(view: &ReplicationView, seam: &str) {
    let violations = check_hard(view);
    assert!(
        violations.is_empty(),
        "replication invariants violated after {seam}:\n{}",
        render(&violations)
    );
}

#[cfg(test)]
mod tests;
