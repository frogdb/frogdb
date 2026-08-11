//! Stateful property generation over the real self-fence checker (issue 05, R6).
//!
//! [`ReplicationQuorumChecker`] is all-synchronous over atomics, so the whole
//! decision is reachable from a test with no I/O and no clock advance: generate
//! `(config, replica set, departure code)` triples and fold them through a
//! **real** [`ReplicationTrackerImpl`] holding real [`ReplicaSession`]s. There
//! is no shadow model — the harness never predicts a verdict, it only calls the
//! production entry point a wire event would call and records what came back.
//!
//! # Why this lives here and not in `frogdb-replication`
//!
//! `frogdb-replication` owns the invariant catalog but not the checker, so
//! `INV-FENCE-1` and `INV-SESSION-3` — both claims about the arming latch held
//! against the session registry — were *skipped* everywhere: the catalog omits
//! entries whose inputs are absent, and nothing in that crate can assemble
//! [`ViewField::Fence`]. Issue 04's link harness says so explicitly and leaves
//! them to R6. [`ReplicationQuorumChecker::view`] is the seam that closes the
//! gap; this module is the first caller.
//!
//! [`ViewField::Fence`]: frogdb_replication::view::ViewField::Fence
//!
//! # Why there are two checkers
//!
//! The interesting fence states are about *when a decision was taken*:
//! `has_quorum` is the only thing that installs the arming latch, so a primary
//! that is not being written to has a registry full of streaming replicas and
//! an unarmed checker (issue 19). An audit that calls `has_quorum` after every
//! action destroys that state before the catalog can see it.
//!
//! So the fold drives two checkers over one registry:
//!
//! - `gate` — asked for a decision only when the sequence says so. Its
//!   [`ReplicationQuorumChecker::view`] is what the catalog is checked against.
//!   This is the primary whose writes are rare.
//! - `audited` — asked at every step, which is what the totality and agreement
//!   claims are about.
//!
//! The two are independent by construction: every mutation `has_quorum` makes
//! lands on the checker's own `armed` atomic, never on the tracker, so neither
//! checker can see the other's arming history. Config actions apply to both, so
//! the pair only ever differs in call history.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;

use frogdb_core::command::QuorumChecker;
use frogdb_core::metrics::WriteFenceReporter;
use frogdb_core::{ReplicationTracker, ReplicationTrackerImpl};
use frogdb_replication::invariants::{self, Violation};
use frogdb_replication::replica_session::{
    Phase, ReplicaAnnouncement, ReplicaCapabilities, ReplicaDeparture, ReplicaSession,
};
use frogdb_replication::view::ReplicationView;

use crate::quorum::ReplicationQuorumChecker;

// ---------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------

/// Cases the property runs in the normal suite. Higher than the link harness's
/// budget because a case here builds no files and touches no disk — it is a
/// tracker, two checkers and a handful of atomics.
const DEFAULT_CASES: u32 = 512;

/// Actions per generated sequence. Long enough to reach an armed checker that
/// has lost its replica, been demoted, and re-armed on a successor.
const SEQUENCE_LEN: usize = 24;

/// Environment override, shared with the link and cluster proptests so one
/// nightly knob raises all three.
const CASES_ENV: &str = "PROPTEST_CASES";

/// Replica slots. Three is the multi-replica ceiling ruled for day one (D5).
const REPLICA_SLOTS: usize = 3;

/// Announced listening port of slot `i`. One live session per slot (see
/// [`FenceNode::apply`]), so no two live sessions ever share an announced
/// identity and `INV-SESSION-2` — issue 22's entry — stays out of this
/// harness's way.
const REPLICA_BASE_PORT: u16 = 6580;

/// First ephemeral peer port handed out.
const FIRST_PEER_PORT: u16 = 41_000;

/// How far [`FenceAction::GoSilent`] backdates a session's last ACK.
///
/// Every freshness window this harness configures is far below it and far above
/// any time a case can actually spend, so "fresh" and "stale" are decided by
/// the backdate rather than by the wall clock. That is the same trick
/// `armed_with_stale_replica_rejects_writes` uses, and it is what keeps a
/// property that reads `clock::elapsed` from being a flaky one.
const STALE_BY: Duration = Duration::from_secs(4 * 3600);

// ---------------------------------------------------------------------------
// Alphabet
// ---------------------------------------------------------------------------

/// A live `replication-replica-freshness-timeout-ms`.
///
/// Both values are minutes-to-hours: an operator would configure either, and
/// neither can be crossed by a case's own runtime (see [`STALE_BY`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FreshnessKnob {
    Minute,
    Hour,
}

impl FreshnessKnob {
    fn window(self) -> Duration {
        match self {
            FreshnessKnob::Minute => Duration::from_secs(60),
            FreshnessKnob::Hour => Duration::from_secs(3600),
        }
    }
}

/// How a session's link ends. `Silent` is not here: a peer that vanished
/// without its teardown running is [`FenceAction::GoSilent`], which is the
/// partition this fence exists for and is deliberately *not* a departure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Departure {
    Graceful,
    Lost,
}

impl Departure {
    fn code(self) -> ReplicaDeparture {
        match self {
            Departure::Graceful => ReplicaDeparture::Graceful,
            Departure::Lost => ReplicaDeparture::Lost,
        }
    }
}

/// One event a fence decision can be taken across.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FenceAction {
    /// A replica connects and registers, announcing its listening port.
    Attach { slot: usize },
    /// The session reaches `Streaming` — the only thing that can arm a checker.
    Stream { slot: usize },
    /// `REPLCONF ACK`, which refreshes the session's liveness.
    Ack { slot: usize },
    /// The link is up as far as this node knows, but the ACKs stopped.
    GoSilent { slot: usize },
    /// The tail of `ReplicaSession::run`: phase flip, conditional departure
    /// record, deregistration.
    Detach { slot: usize, departure: Departure },
    /// `CONFIG SET replication-self-fence-on-replica-loss`.
    Toggle { enabled: bool },
    /// `CONFIG SET replication-replica-freshness-timeout-ms`.
    Retune { knob: FreshnessKnob },
    /// Role demotion's fence half: `reset_arming`. Session teardown is
    /// `Detach` and is generated separately, because in `RoleManager::demote`
    /// it runs through `end_primary_stint` and completes asynchronously — a
    /// session can and does outlive the reset.
    Demote,
    /// A write command reaching the gate: `has_quorum` on `gate`.
    Decide,
    /// `/status` or the metrics reporter: `write_fence_reason` on `gate`.
    Report,
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

/// A primary's fence surface: one registry, two checkers (see the module
/// header), and the bookkeeping that lives outside both — which peer is on
/// which socket, and what this node has been through.
struct FenceNode {
    tracker: Arc<ReplicationTrackerImpl>,
    /// The checker the catalog reads. Decides only when the sequence says so.
    gate: ReplicationQuorumChecker,
    /// The checker the totality and agreement audit hammers every step.
    audited: ReplicationQuorumChecker,
    slots: Vec<Option<Arc<ReplicaSession>>>,
    next_peer_port: u16,
    /// Ledger, not a model: set from what the production call *did*, never
    /// from what it was expected to do.
    ledger: Ledger,
}

/// What this node has been observed to do, as evidence for the arming claims.
///
/// Every field is written from a production observation — a phase the session
/// reports, a registry the tracker reports — so nothing here is a prediction of
/// a verdict.
#[derive(Debug, Default, Clone, Copy)]
struct Ledger {
    /// A session reached `Streaming` at some point in this node's life.
    ever_streamed: bool,
    /// A session was streaming at some point since the last `reset_arming`.
    /// Cleared by `Demote` — but only to whatever the registry says right then,
    /// because a session that outlives the reset is still a streaming session
    /// the next decision may legitimately arm on.
    streamed_this_stint: bool,
    /// `reset_arming` ran and nothing has streamed since. The state where an
    /// unarmed checker coexists with a departure record from the stint that
    /// just ended.
    demoted: bool,
}

impl FenceNode {
    fn fresh(enabled: bool, knob: FreshnessKnob) -> Self {
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let gate = ReplicationQuorumChecker::new(tracker.clone(), enabled, knob.window());
        let audited = ReplicationQuorumChecker::new(tracker.clone(), enabled, knob.window());
        Self {
            tracker,
            gate,
            audited,
            slots: (0..REPLICA_SLOTS).map(|_| None).collect(),
            next_peer_port: FIRST_PEER_PORT,
            ledger: Ledger::default(),
        }
    }

    /// The registry and the gate checker in one projection — the two groups
    /// `INV-FENCE-1` and `INV-SESSION-3` require, and the reason this module
    /// exists.
    fn view(&self) -> ReplicationView {
        self.tracker.registry_view().with_fence(self.gate.view())
    }

    /// The tail of `ReplicaSession::run`, exactly as the link harness models
    /// it: the phase flip, the departure record *only* for a session that
    /// actually streamed, and then the deregistration.
    fn run_exit(&self, session: &Arc<ReplicaSession>, departure: Departure) {
        let was_streaming = session.is_streaming();
        session.force_phase_for_test(Phase::Disconnecting);
        if was_streaming {
            self.tracker.record_streaming_departure(departure.code());
        }
        self.tracker.unregister_replica(session.id());
    }

    fn apply(&mut self, action: FenceAction) {
        match action {
            FenceAction::Attach { slot } => {
                // One live session per slot, so two sessions never share an
                // announced identity: the duplicate-identity shape is issue
                // 04's harness to carry (issue 22), not this one's.
                if self.slots[slot].is_some() {
                    return;
                }
                let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.next_peer_port);
                self.next_peer_port += 1;
                let session = self.tracker.register_announced_replica(
                    peer,
                    ReplicaAnnouncement {
                        listening_port: REPLICA_BASE_PORT + slot as u16,
                        capabilities: ReplicaCapabilities {
                            eof: true,
                            psync2: true,
                        },
                        version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    },
                );
                self.slots[slot] = Some(session);
            }

            FenceAction::Stream { slot } => {
                let Some(session) = self.slots[slot].clone() else {
                    return;
                };
                // Phases are forward-only through the production setter, so a
                // session past `Connecting` has already resolved its PSYNC.
                if session.phase() != Phase::Connecting {
                    return;
                }
                session.force_phase_for_test(Phase::Streaming);
                // A replica reaching the live stream retires the departure
                // latch: this node has a follower again. Same call the real
                // PSYNC path makes (`replica_session.rs`).
                self.tracker.clear_streaming_departure();
                self.ledger.ever_streamed = true;
                self.ledger.streamed_this_stint = true;
                self.ledger.demoted = false;
            }

            FenceAction::Ack { slot } => {
                let Some(session) = self.slots[slot].clone() else {
                    return;
                };
                // A session below Streaming has no reader task, so no ACK can
                // arrive on it. Modelling one would be inventing a wire event.
                if !session.is_streaming() {
                    return;
                }
                self.tracker.record_ack(session.id(), 1);
            }

            FenceAction::GoSilent { slot } => {
                let Some(session) = self.slots[slot].clone() else {
                    return;
                };
                session.backdate_last_ack_for_test(STALE_BY);
            }

            FenceAction::Detach { slot, departure } => {
                let Some(session) = self.slots[slot].take() else {
                    return;
                };
                self.run_exit(&session, departure);
            }

            FenceAction::Toggle { enabled } => {
                self.gate.set_self_fence_enabled(enabled);
                self.audited.set_self_fence_enabled(enabled);
            }

            FenceAction::Retune { knob } => {
                let ms = knob.window().as_millis() as u64;
                self.gate.set_freshness_timeout_ms(ms);
                self.audited.set_freshness_timeout_ms(ms);
            }

            FenceAction::Demote => {
                self.gate.reset_arming();
                self.audited.reset_arming();
                // A session that outlived the reset is still evidence for the
                // *next* decision, so the stint ledger is re-read from the
                // registry rather than blanked.
                self.ledger.streamed_this_stint = self.tracker.has_streaming_replica();
                self.ledger.demoted = true;
            }

            FenceAction::Decide => {
                self.gate.has_quorum();
            }

            FenceAction::Report => {
                self.gate.write_fence_reason();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Known defects
// ---------------------------------------------------------------------------

/// Whether a catalog violation is one a *known, filed* defect already accounts
/// for.
///
/// Unlike the link harness's muzzle this filters violations rather than
/// actions: the shapes below are not produced by one action but by the
/// *absence* of one (a decision that was never taken), so there is nothing to
/// refuse to generate. Both arms are pinned by a `#[should_panic]` witness
/// below, so a fix turns the witness red instead of leaving the muzzle silently
/// covering healthy code, and both are keyed on the resolved cause rather than
/// on the entry id alone — see `the_muzzle_only_covers_the_pinned_shapes`.
fn known_defect(violation: &Violation, node: &FenceNode) -> Option<&'static str> {
    if !matches!(violation.id, "INV-FENCE-1" | "INV-SESSION-3") {
        return None;
    }
    // Both entries read the arming latch; an armed checker satisfies each.
    if node.gate.is_armed() {
        return None;
    }
    if node.ledger.demoted {
        // Issue 23: `reset_arming` drops the latch but leaves
        // `last_streaming_departure` standing, so a demoted node reports a
        // departure from the stint that just ended with no witness that the
        // stint ever had a follower. Ordered first because a demotion is the
        // *later* cause: it un-arms a checker issue 19 may never have armed.
        return Some("issue 23: a demotion drops the arming latch but keeps the departure record");
    }
    node.ledger.ever_streamed.then_some(
        // Issue 19: `arm_if_streaming` is only reached from `has_quorum`, so a
        // primary that is not being written to never installs the latch. The
        // registry says a session is streaming (clause a) or that one departed
        // (clause b, and `INV-SESSION-3`) while the checker still reads
        // never-had-a-replica.
        "issue 19: the self-fence arms only on the write path",
    )
}

/// Every violation the catalog reports that no filed defect accounts for.
fn unexplained(node: &FenceNode) -> Vec<Violation> {
    invariants::check_hard(&node.view())
        .into_iter()
        .filter(|violation| known_defect(violation, node).is_none())
        .collect()
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

fn arb_action() -> impl Strategy<Value = FenceAction> {
    let slot = 0..REPLICA_SLOTS;
    prop_oneof![
        // Weighted towards the link lifecycle: a sequence that never gets a
        // session to `Streaming` reaches none of the states R6 is about, and
        // the coverage meta-test below holds that to account.
        6 => slot.clone().prop_map(|slot| FenceAction::Attach { slot }),
        6 => slot.clone().prop_map(|slot| FenceAction::Stream { slot }),
        5 => slot.clone().prop_map(|slot| FenceAction::Ack { slot }),
        4 => slot.clone().prop_map(|slot| FenceAction::GoSilent { slot }),
        4 => (slot, prop_oneof![Just(Departure::Graceful), Just(Departure::Lost)])
            .prop_map(|(slot, departure)| FenceAction::Detach { slot, departure }),
        3 => any::<bool>().prop_map(|enabled| FenceAction::Toggle { enabled }),
        2 => prop_oneof![Just(FreshnessKnob::Minute), Just(FreshnessKnob::Hour)]
            .prop_map(|knob| FenceAction::Retune { knob }),
        2 => Just(FenceAction::Demote),
        5 => Just(FenceAction::Decide),
        3 => Just(FenceAction::Report),
    ]
}

/// The whole generated input: the boot config plus the event sequence. Actions
/// are unconditional — a `Stream` on an empty slot is a no-op, not a rejected
/// call — so there is no generating fold to keep in step with the replay, which
/// is what makes this generator a plain `Vec` where issue 04's could not be.
fn arb_run() -> impl Strategy<Value = (bool, FreshnessKnob, Vec<FenceAction>)> {
    (
        any::<bool>(),
        prop_oneof![Just(FreshnessKnob::Minute), Just(FreshnessKnob::Hour)],
        prop::collection::vec(arb_action(), 1..=SEQUENCE_LEN),
    )
}

fn cases_from(raw: Option<String>) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|cases| *cases > 0)
        .unwrap_or(DEFAULT_CASES)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: cases_from(std::env::var(CASES_ENV).ok()),
        max_shrink_iters: 100_000,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// The audit
// ---------------------------------------------------------------------------

/// Everything `has_quorum` and `write_fence_reason` promise jointly, taken
/// against the `audited` checker after one action.
///
/// The order is the load-bearing part. `has_quorum` is the only caller of
/// `arm_if_streaming`, so a reason read *before* the first decision describes
/// an unarmed checker and a reason read after describes an armed one — those
/// legitimately differ, and pinning the pair in a settled order is what makes
/// the agreement claim about the decision rather than about the call order.
fn audit(node: &FenceNode) -> Result<(), TestCaseError> {
    let checker = &node.audited;

    // Totality: neither call panics, and both answer for every input. The
    // refusal string is a constant, so it is the cheapest witness that the
    // error path is answerable at all.
    let first = checker.has_quorum();
    let reason = checker.write_fence_reason();
    let second = checker.has_quorum();

    prop_assert_eq!(
        first,
        second,
        "a fence decision moved on its own between two calls over an unchanged registry"
    );
    prop_assert_eq!(
        reason.is_some(),
        !second,
        "the reported fence reason ({:?}) disagrees with the decision the write gate would make \
         ({})",
        reason,
        second
    );
    prop_assert!(
        checker.quorum_lost_error().starts_with("SELFFENCE"),
        "the refusal must name the mechanism that produced it, got {:?}",
        checker.quorum_lost_error()
    );
    prop_assert!(
        !checker.is_armed() || node.ledger.streamed_this_stint,
        "the checker armed on a stint in which no session ever reached Streaming"
    );
    Ok(())
}

proptest! {
    #![proptest_config(config())]

    /// **R6.** The self-fence admission decision is total: over any
    /// `(config, replica set, departure code)` history, `has_quorum` and
    /// `write_fence_reason` both answer, agree with each other, and never arm
    /// from a state no session ever streamed — and the arming latch stays
    /// coherent with the registry it is a witness for.
    ///
    /// The oracle is the `frogdb-replication` catalog over a projection this
    /// crate is the only one able to assemble, plus the joint claims about the
    /// two entry points that no single-call test can state.
    #[test]
    fn r6_the_fence_decision_is_total(
        (enabled, knob, actions) in arb_run(),
    ) {
        let mut node = FenceNode::fresh(enabled, knob);
        for (index, action) in actions.iter().enumerate() {
            node.apply(*action);

            // Before the audit: the audit's own `has_quorum` would arm the
            // checker it runs against, which is why it runs against the other
            // one — but the catalog still has to be read at the state the
            // action left behind.
            let violations = unexplained(&node);
            prop_assert!(
                violations.is_empty(),
                "step {index} ({action:?}) left the catalog dirty:\n{}\nafter {:?}",
                invariants::render(&violations),
                &actions[..=index]
            );

            audit(&node)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::strategy::ValueTree;
    use std::collections::BTreeSet;

    fn attach_and_stream(node: &mut FenceNode, slot: usize) {
        node.apply(FenceAction::Attach { slot });
        node.apply(FenceAction::Stream { slot });
        node.apply(FenceAction::Ack { slot });
    }

    #[test]
    fn the_case_budget_defaults_and_is_raised_by_the_environment() {
        assert_eq!(cases_from(None), DEFAULT_CASES);
        assert_eq!(cases_from(Some(" 4096 ".to_string())), 4096);
        assert_eq!(cases_from(Some("0".to_string())), DEFAULT_CASES);
        assert_eq!(cases_from(Some("many".to_string())), DEFAULT_CASES);
    }

    /// A property whose generator cannot reach the states it is about passes
    /// vacuously. This holds the alphabet to producing every one of them.
    #[test]
    fn the_generator_reaches_the_states_the_property_is_about() {
        let mut seen: BTreeSet<&'static str> = BTreeSet::new();
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let strategy = arb_run();

        for _ in 0..600 {
            let (enabled, knob, actions) = strategy
                .new_tree(&mut runner)
                .expect("strategy must produce a value")
                .current();
            let mut node = FenceNode::fresh(enabled, knob);
            for action in actions {
                node.apply(action);
                // Same order the audit takes: the decision first, because it
                // is the only thing that installs the latch the rest read.
                let quorum = node.audited.has_quorum();
                let reason = node.audited.write_fence_reason();
                if node.audited.is_armed() {
                    seen.insert("armed");
                }
                if node.gate.is_armed() && !node.tracker.has_streaming_replica() {
                    seen.insert("armed with no streaming replica");
                }
                if reason.is_some() {
                    seen.insert("fenced");
                }
                if node.tracker.last_streaming_departure() == Some(ReplicaDeparture::Graceful)
                    && !node.tracker.has_streaming_replica()
                    && quorum
                {
                    seen.insert("disarmed by a clean departure");
                }
                if !node.gate.is_armed() && node.tracker.has_streaming_replica() {
                    seen.insert("streaming while unarmed");
                }
                if node.ledger.demoted {
                    seen.insert("demoted");
                }
            }
        }

        let expected: BTreeSet<&'static str> = [
            "armed",
            "armed with no streaming replica",
            "fenced",
            "disarmed by a clean departure",
            "streaming while unarmed",
            "demoted",
        ]
        .into_iter()
        .collect();
        assert_eq!(
            expected.difference(&seen).collect::<Vec<_>>(),
            Vec::<&&str>::new(),
            "the generator never reached these states"
        );
    }

    /// Issue 19's first consequence. A primary with a streaming replica that
    /// has taken no fence decision reports an unarmed checker, which is
    /// `INV-FENCE-1`'s dead-detector clause.
    #[test]
    #[should_panic(expected = "sessions [1] are streaming but the self-fence checker is unarmed")]
    fn pinned_issue_19_a_streaming_session_leaves_the_fence_unarmed() {
        let mut node = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut node, 0);
        let violations = invariants::check_hard(&node.view());
        assert!(violations.is_empty(), "{}", invariants::render(&violations));
    }

    /// Issue 19's second consequence. The replica is *lost* before this node
    /// ever took a decision, so the departure is recorded against a latch that
    /// was never installed — `INV-SESSION-3` plus `INV-FENCE-1`'s disarm
    /// clause.
    #[test]
    #[should_panic(expected = "INV-SESSION-3")]
    fn pinned_issue_19_a_lost_replica_departs_with_no_witness() {
        let mut node = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut node, 0);
        node.apply(FenceAction::Detach {
            slot: 0,
            departure: Departure::Lost,
        });
        let violations = invariants::check_hard(&node.view());
        assert!(violations.is_empty(), "{}", invariants::render(&violations));
    }

    /// Issue 23. Here the latch *was* installed — the node took a decision
    /// while its replica streamed — and the demotion dropped it while leaving
    /// the departure record from the same stint behind.
    #[test]
    #[should_panic(expected = "INV-SESSION-3")]
    fn pinned_issue_23_a_demotion_keeps_the_departure_it_disarmed() {
        let mut node = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut node, 0);
        node.apply(FenceAction::Decide);
        assert!(node.gate.is_armed(), "the decision must install the latch");
        node.apply(FenceAction::Detach {
            slot: 0,
            departure: Departure::Lost,
        });
        node.apply(FenceAction::Demote);
        let violations = invariants::check_hard(&node.view());
        assert!(violations.is_empty(), "{}", invariants::render(&violations));
    }

    /// The muzzle covers two named causes and nothing else.
    #[test]
    fn the_muzzle_only_covers_the_pinned_shapes() {
        // (1) An armed checker is excused from nothing, whatever it has been
        // through. Fabricate a violation of each entry and offer it up.
        let mut armed = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut armed, 0);
        armed.apply(FenceAction::Decide);
        assert!(armed.gate.is_armed());
        armed.ledger.demoted = true;
        for id in ["INV-FENCE-1", "INV-SESSION-3"] {
            assert_eq!(
                known_defect(&Violation::new(id, "fabricated".to_string()), &armed),
                None,
                "{id} must not be excused on an armed checker"
            );
        }

        // (2) An unarmed checker excuses only the two entries that read the
        // latch. Every other entry stands.
        let mut unarmed = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut unarmed, 0);
        assert!(!unarmed.gate.is_armed());
        assert_eq!(
            known_defect(
                &Violation::new("INV-SESSION-2", "fabricated".to_string()),
                &unarmed
            ),
            None,
            "the muzzle must not reach entries that do not read the arming latch"
        );

        // (3) A node no session ever streamed on is excused from nothing: a
        // departure recorded there would be a genuinely new defect, and the
        // harness cannot produce one because `run_exit` records a departure
        // only for a session that reported `Streaming`.
        let mut never = FenceNode::fresh(true, FreshnessKnob::Hour);
        never.apply(FenceAction::Attach { slot: 0 });
        never.apply(FenceAction::Detach {
            slot: 0,
            departure: Departure::Lost,
        });
        assert_eq!(never.tracker.last_streaming_departure(), None);
        assert!(!never.ledger.ever_streamed);
        assert_eq!(
            known_defect(
                &Violation::new("INV-SESSION-3", "fabricated".to_string()),
                &never
            ),
            None,
            "a departure with no stream behind it is not issue 19"
        );

        // (4) The two causes are told apart, so a fix to either turns exactly
        // one witness red.
        let mut demoted = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut demoted, 0);
        demoted.apply(FenceAction::Decide);
        demoted.apply(FenceAction::Detach {
            slot: 0,
            departure: Departure::Lost,
        });
        demoted.apply(FenceAction::Demote);
        let excuse = known_defect(
            &Violation::new("INV-SESSION-3", "fabricated".to_string()),
            &demoted,
        );
        assert!(
            excuse.is_some_and(|reason| reason.starts_with("issue 23")),
            "a post-demotion violation is issue 23, got {excuse:?}"
        );
        let mut never_decided = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut never_decided, 0);
        never_decided.apply(FenceAction::Detach {
            slot: 0,
            departure: Departure::Lost,
        });
        let excuse = known_defect(
            &Violation::new("INV-SESSION-3", "fabricated".to_string()),
            &never_decided,
        );
        assert!(
            excuse.is_some_and(|reason| reason.starts_with("issue 19")),
            "a never-decided violation is issue 19, got {excuse:?}"
        );
    }

    /// The two checkers share a registry and nothing else, which is what makes
    /// the split in the module header sound.
    #[test]
    fn the_two_checkers_do_not_share_an_arming_latch() {
        let mut node = FenceNode::fresh(true, FreshnessKnob::Hour);
        attach_and_stream(&mut node, 0);
        assert!(node.audited.has_quorum());
        assert!(node.audited.is_armed());
        assert!(
            !node.gate.is_armed(),
            "the audited checker's decision must not arm the gate checker"
        );
        node.apply(FenceAction::Decide);
        assert!(node.gate.is_armed());
    }

    /// Staleness comes from the backdate, not from the clock, so the property
    /// cannot flake on a loaded machine.
    #[test]
    fn the_backdate_outruns_every_configured_window() {
        for knob in [FreshnessKnob::Minute, FreshnessKnob::Hour] {
            assert!(
                knob.window() < STALE_BY,
                "{knob:?} must classify a backdated ACK as stale"
            );
        }
    }
}
