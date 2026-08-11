//! Stateful property generation over the real replication link (issue 04, R1).
//!
//! The generator folds a sequence of [`LinkAction`]s through a **real** node —
//! a [`PrimaryReplicationHandler`] over a real state file, a real
//! [`ReplicationTrackerImpl`] registry holding real [`ReplicaSession`]s, a real
//! [`ReplicaFeedGate`], and the real [`AppliedOffset`] apply gate. There is no
//! shadow model: every action calls the production entry point a wire event
//! would call, and the oracle is [`crate::invariants`] — the same catalog the
//! in-crate seam hooks assert against.
//!
//! What the generator *does* model is the part of a node that lives outside
//! this crate: which peer is on which socket, which session the next `PSYNC`
//! belongs to, and whether the process is serving a primary stint. That
//! bookkeeping selects *which* production call to make; it never answers a
//! question the production code could have answered.
//!
//! # Why the fold runs twice
//!
//! Proptest shrinks the *source* of a `prop_map`, so the strategy has to
//! produce the action list, which means it has to know the node state each
//! action lands on — the same fold the property then re-runs. The actions are
//! therefore **symbolic** (`AckOffset::Live`, `PsyncId::Current`, ...): a
//! replication id is minted from randomness, so a literal id captured during
//! generation would not exist during replay. Symbolic parameters resolve
//! against the node at apply time and so resolve identically in both folds.
//!
//! Generation must also not panic — a panic inside a strategy escapes
//! proptest's `catch_unwind` and aborts the run with no counterexample — so the
//! generating fold catches and truncates. The *property* fold is the asserting
//! one, and that is what shrinks.
//!
//! # Coverage this harness does not have
//!
//! `view.fence` is left unset: the self-fence lives in
//! `frogdb-replication-runtime` and no seam in this crate can assemble it. The
//! catalog *skips* entries whose inputs are absent, so `INV-FENCE-1` and
//! `INV-SESSION-3` are not exercised here. They are issue 05's (R6) to force —
//! and with them issue 19 (the self-fence arms only on the write path), which
//! this harness therefore cannot reach and does not muzzle.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use tempfile::TempDir;

use frogdb_types::clock;

use crate::feed_gate::ReplicaFeedGate;
use crate::frame::ReplicationFrame;
use crate::identity::ReplicationIdentity;
use crate::invariants;
use crate::offset_coordinator::OffsetCoordinator;
use crate::primary::PrimaryReplicationHandler;
use crate::primary::replay::ReplayDecision;
use crate::replica::offset::{AppliedOffset, Claim, ReplicaApplyStint, ReplicaOffset};
use crate::replica_session::{
    Phase, ReplicaAnnouncement, ReplicaCapabilities, ReplicaDeparture, ReplicaSession,
};
use crate::state::{
    ReplicationState, StagedReplicationMetadata, consume_staged_replication_metadata,
    read_staged_replication_metadata,
};
use crate::sync_counters::SyncOutcome;
use crate::tracker::ReplicationTrackerImpl;
use crate::view::{
    ApplyGateView, BacklogView, ContinueGrant, OffsetTriple, PhaseChange, ReplicationView, RoleView,
};
use crate::{BacklogConfig, LagThresholdConfig};

// ---------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------

/// Cases the property runs in the normal suite. Small enough that
/// `just test frogdb-replication` stays a development-loop command; the depth
/// comes from the nightly boost, not from the default.
const DEFAULT_CASES: u32 = 96;

/// Actions per generated sequence. Long enough to reach a promoted node with a
/// resumed replica and an evicted backlog, short enough that the two folds
/// (generate + replay) stay cheap given each case builds a real node on a real
/// temp directory.
const SEQUENCE_LEN: usize = 40;

/// Environment override, shared with the cluster proptest so one nightly knob
/// raises both.
const CASES_ENV: &str = "PROPTEST_CASES";

/// Fraction of steps that pick a slot/parameter the node state makes sensible.
/// The remaining fifth is raw entropy, which is what produces the rejected
/// actions R1 must also survive.
const IN_CONTEXT_BIAS: f64 = 0.8;

/// Replica slots. Three is the multi-replica ceiling ruled for day one (D5).
const REPLICA_SLOTS: usize = 3;

/// Announced listening port of slot `i`. The *peer* port differs per session
/// (a reconnect lands on a new ephemeral port), which is exactly what makes a
/// stale session and its successor share one announced identity.
const REPLICA_BASE_PORT: u16 = 6480;

/// First ephemeral peer port handed out.
const FIRST_PEER_PORT: u16 = 40_000;

/// Barrier ceiling declared to the feed-gate projection so `INV-GATE-1`'s
/// budget clause is not vacuous. Every hold this harness publishes is well
/// under it, so a hold cannot expire between the two clock reads
/// `ReplicaFeedGate::view` deliberately takes.
const BARRIER_BUDGET: Duration = Duration::from_secs(3600);

/// Backlog TTL. Nonzero so `expire_idle_backlog` has an elapsed outcome to
/// reach; the harness backdates the idle clock rather than sleeping.
const BACKLOG_TTL_SECS: u64 = 60;

/// A valid replication id this node never headed.
const FOREIGN_REPLICATION_ID: &str = "0123456789abcdef0123456789abcdef01234567";

/// Resolve the case budget from a raw environment value.
fn cases_from(raw: Option<String>) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|cases| *cases > 0)
        .unwrap_or(DEFAULT_CASES)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: cases_from(std::env::var(CASES_ENV).ok()),
        // The counterexample is a `Vec<LinkAction>`; the default 8k-iteration
        // shrink budget is not enough to reduce a 40-step sequence to the two
        // or three actions that actually matter.
        max_shrink_iters: 100_000,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Action vocabulary
// ---------------------------------------------------------------------------

/// Which offset an action names, resolved against the live head at apply time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckOffset {
    /// Exactly the live head — a fully caught-up replica.
    Live,
    /// `n` bytes behind the live head (saturating at zero).
    Behind(u16),
    /// Zero: a replica that has acked nothing.
    Zero,
    /// Past the live head. Only the pinned witnesses reach this — see
    /// [`known_defect`].
    Ahead(u16),
}

/// Which replication id a `PSYNC` carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PsyncId {
    /// The id this node currently heads — the resumable case.
    Current,
    /// The id this node headed before its last promotion, if any.
    Secondary,
    /// A valid id from some other history.
    Foreign,
    /// `?` — the replica asks for a full resync outright.
    Unknown,
}

/// What a replica announced about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Announce {
    /// `REPLCONF listening-port` was sent: the session has an identity.
    Port,
    /// It was not: the session is *unknown*, not "port 0".
    Silent,
}

/// How a link ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Departure {
    /// The session's `run()` exits and deregisters, orderly.
    Graceful,
    /// The session's `run()` exits and deregisters, having lost the link.
    Lost,
    /// The peer is gone but the primary has not noticed yet: nothing happens
    /// to the registry and the session stays where it was. The reconnect that
    /// follows is what produces two sessions on one identity.
    Silent,
}

/// Which replication id a staged checkpoint claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StagedId {
    Current,
    Foreign,
    /// Not 40 hex characters. The staged reader must refuse it.
    Malformed,
}

/// Which apply epoch a divergence admission names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochRef {
    Current,
    /// One past the live epoch — a late admission from a retired stint.
    Stale,
}

/// How long a published feed-gate hold lasts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldSpan {
    /// A deadline already in the past: the gate expires itself.
    Expired,
    Short,
    Long,
}

impl HoldSpan {
    fn deadline(self) -> std::time::Instant {
        let now = clock::now();
        match self {
            HoldSpan::Expired => now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            HoldSpan::Short => now + Duration::from_secs(600),
            HoldSpan::Long => now + Duration::from_secs(1800),
        }
    }
}

/// One thing that can happen to a replication link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkAction {
    /// A write reaches the primary path: live offset advances, backlog records.
    Write { key: u8, value_len: u8 },
    /// `REPLCONF ACK <offset>` from a streaming replica.
    Ack { slot: usize, offset: AckOffset },
    /// A peer connects and announces itself.
    Attach { slot: usize, announce: Announce },
    /// `PSYNC <id> <offset>` — the resync fork, and the phase ladder that
    /// follows whichever way it goes.
    Psync {
        slot: usize,
        id: PsyncId,
        offset: AckOffset,
    },
    /// A link ends.
    Detach { slot: usize, departure: Departure },
    /// This node takes over: `begin_primary_stint`.
    Promote,
    /// This node steps down: `end_primary_stint`, and every session with it.
    Demote { upstream: bool },
    /// A slot-handoff barrier goes up.
    HoldFeed { hold: HoldSpan },
    /// The barrier comes down.
    ReleaseFeed,
    /// The state file is reconciled and persisted.
    SaveState,
    /// The process restarts off whatever is on disk.
    RestoreState,
    /// A full-sync checkpoint's metadata is staged and adopted.
    StageMetadata { id: StagedId, offset: AckOffset },
    /// The maintenance tick frees an idle backlog.
    EvictBacklog,
    /// The apply gate freezes (a replica stops applying).
    Freeze,
    /// A replicated apply failed and the divergence is admitted.
    AdmitDivergence { epoch: EpochRef },
}

/// The action kinds, for the weight table and the coverage meta-tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Variant {
    Write,
    Ack,
    Attach,
    Psync,
    Detach,
    Promote,
    Demote,
    HoldFeed,
    ReleaseFeed,
    SaveState,
    RestoreState,
    StageMetadata,
    EvictBacklog,
    Freeze,
    AdmitDivergence,
}

impl LinkAction {
    fn variant(&self) -> Variant {
        match self {
            LinkAction::Write { .. } => Variant::Write,
            LinkAction::Ack { .. } => Variant::Ack,
            LinkAction::Attach { .. } => Variant::Attach,
            LinkAction::Psync { .. } => Variant::Psync,
            LinkAction::Detach { .. } => Variant::Detach,
            LinkAction::Promote => Variant::Promote,
            LinkAction::Demote { .. } => Variant::Demote,
            LinkAction::HoldFeed { .. } => Variant::HoldFeed,
            LinkAction::ReleaseFeed => Variant::ReleaseFeed,
            LinkAction::SaveState => Variant::SaveState,
            LinkAction::RestoreState => Variant::RestoreState,
            LinkAction::StageMetadata { .. } => Variant::StageMetadata,
            LinkAction::EvictBacklog => Variant::EvictBacklog,
            LinkAction::Freeze => Variant::Freeze,
            LinkAction::AdmitDivergence { .. } => Variant::AdmitDivergence,
        }
    }
}

/// Draw weights. Writes and acks dominate because they are what a link
/// *does*; the structural actions are rarer so a sequence spends most of its
/// length in a configuration rather than rebuilding one.
const WEIGHTS: [(Variant, u32); 15] = [
    (Variant::Write, 16),
    (Variant::Ack, 12),
    (Variant::Attach, 9),
    (Variant::Psync, 10),
    (Variant::Detach, 7),
    (Variant::Promote, 4),
    (Variant::Demote, 3),
    (Variant::HoldFeed, 4),
    (Variant::ReleaseFeed, 3),
    (Variant::SaveState, 3),
    (Variant::RestoreState, 2),
    (Variant::StageMetadata, 3),
    (Variant::EvictBacklog, 3),
    (Variant::Freeze, 2),
    (Variant::AdmitDivergence, 2),
];

const fn total_weight() -> u32 {
    let mut total = 0;
    let mut i = 0;
    while i < WEIGHTS.len() {
        total += WEIGHTS[i].1;
        i += 1;
    }
    total
}

const TOTAL_WEIGHT: u32 = total_weight();

/// Map a uniform draw in `[0, TOTAL_WEIGHT)` onto a variant.
fn variant_for(pick: u32) -> Variant {
    let mut cursor = pick % TOTAL_WEIGHT;
    for (variant, weight) in WEIGHTS {
        if cursor < weight {
            return variant;
        }
        cursor -= weight;
    }
    // Unreachable: the cursor is reduced modulo the sum of the weights.
    WEIGHTS[0].0
}

/// Raw entropy for one step. Kept as plain integers rather than a pre-built
/// action so proptest shrinks toward `0`/`false` — which is what turns a
/// 40-action counterexample into the two actions that matter.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Step {
    variant: u32,
    in_context: bool,
    slot: u8,
    a: u8,
    b: u8,
    flag: bool,
}

fn arb_step() -> impl Strategy<Value = Step> {
    (
        0..TOTAL_WEIGHT,
        proptest::bool::weighted(IN_CONTEXT_BIAS),
        any::<u8>(),
        any::<u8>(),
        any::<u8>(),
        any::<bool>(),
    )
        .prop_map(|(variant, in_context, slot, a, b, flag)| Step {
            variant,
            in_context,
            slot,
            a,
            b,
            flag,
        })
}

// ---------------------------------------------------------------------------
// Chain policy
// ---------------------------------------------------------------------------

/// Whether a node serving a *replica* role may carry streaming sessions of its
/// own (chained replication). Off by default: `INV-ROLE-1` is a documented
/// exception (`done/48-chained-replication-contract.md`), so leaving it on
/// would make the sub-replica case indistinguishable from a bug in the default
/// property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChainPolicy {
    Off,
    On,
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

/// Sessions that share one announced identity, newest last.
#[derive(Default)]
struct Slot {
    sessions: Vec<Arc<ReplicaSession>>,
    /// Whether the announced identity is known (`REPLCONF listening-port` was
    /// sent). Two unannounced sessions are not evidence of one replica.
    announced: bool,
    /// The peer's link is gone but this node has not noticed. A reconnect is
    /// legal while this is set, and it is the only way a second session lands
    /// on an identity that already has one.
    lost: bool,
}

impl Slot {
    fn current(&self) -> Option<&Arc<ReplicaSession>> {
        self.sessions.last()
    }

    /// The session whose socket errors first — the stale one, if any.
    fn oldest(&self) -> Option<&Arc<ReplicaSession>> {
        self.sessions.first()
    }
}

/// Whether an action did anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Accepted,
    Rejected(&'static str),
}

/// A real replication node, plus the socket-level bookkeeping that lives
/// outside this crate.
pub(crate) struct LinkNode {
    dir: TempDir,
    tracker: Arc<ReplicationTrackerImpl>,
    handler: Arc<PrimaryReplicationHandler>,
    feed_gate: Arc<ReplicaFeedGate>,
    applied: AppliedOffset,
    stint: Option<ReplicaApplyStint>,
    role: RoleView,
    slots: Vec<Slot>,
    next_peer_port: u16,
    chains: ChainPolicy,
}

fn state_path(dir: &Path) -> PathBuf {
    dir.join("replication_state.json")
}

fn build_handler(
    dir: &Path,
    tracker: Arc<ReplicationTrackerImpl>,
    feed_gate: Arc<ReplicaFeedGate>,
    state: ReplicationState,
) -> (Arc<PrimaryReplicationHandler>, AppliedOffset) {
    let identity = ReplicationIdentity::adopting(state, &tracker);
    let applied = identity.applied();
    let handler = PrimaryReplicationHandler::new(
        identity,
        state_path(dir),
        tracker,
        None,
        dir.to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        BacklogConfig {
            enabled: true,
            // Small caps on purpose: eviction is the interesting backlog state
            // and a production-sized ring would never reach it in 40 actions.
            max_entries: 16,
            max_bytes: 4096,
            ttl_secs: BACKLOG_TTL_SECS,
        },
        1_000,
        feed_gate,
    );
    (Arc::new(handler), applied)
}

impl LinkNode {
    fn fresh(chains: ChainPolicy) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let tracker = ReplicationTrackerImpl::new_arc();
        let feed_gate = ReplicaFeedGate::open();
        let (handler, applied) = build_handler(
            dir.path(),
            tracker.clone(),
            feed_gate.clone(),
            ReplicationState::new(),
        );
        Self {
            dir,
            tracker,
            handler,
            feed_gate,
            applied,
            stint: None,
            role: RoleView::Primary,
            slots: (0..REPLICA_SLOTS).map(|_| Slot::default()).collect(),
            next_peer_port: FIRST_PEER_PORT,
            chains,
        }
    }

    /// Everything this node can say about itself at once.
    ///
    /// `PrimaryReplicationHandler::view` deliberately omits the registry and
    /// the role — it owns neither — so this is the one place they are put back
    /// together, which is the whole point of a property that owns the node.
    /// The feed gate is re-projected with the barrier budget attached, because
    /// the handler cannot know it (the ceiling lives in `frogdb-cluster`).
    pub(crate) fn view(&self) -> ReplicationView {
        let registry = self.tracker.registry_view();
        self.handler
            .view()
            .with_replicas(registry.replicas.unwrap_or_default(), registry.departure)
            .with_feed_gate(self.feed_gate.view(Some(BARRIER_BUDGET)))
            .with_role(self.role.clone())
    }

    fn live(&self) -> u64 {
        self.handler.current_offset()
    }

    fn resolve_offset(&self, offset: AckOffset) -> u64 {
        let live = self.live();
        match offset {
            AckOffset::Live => live,
            AckOffset::Behind(n) => live.saturating_sub(u64::from(n)),
            AckOffset::Zero => 0,
            AckOffset::Ahead(n) => live.saturating_add(1).saturating_add(u64::from(n)),
        }
    }

    fn resolve_psync_id(&self, id: PsyncId) -> String {
        match id {
            PsyncId::Current => self.handler.replication_id(),
            PsyncId::Secondary => self
                .handler
                .state()
                .secondary_id
                .unwrap_or_else(|| self.handler.replication_id()),
            PsyncId::Foreign => FOREIGN_REPLICATION_ID.to_string(),
            PsyncId::Unknown => "?".to_string(),
        }
    }

    fn is_replica(&self) -> bool {
        matches!(self.role, RoleView::Replica { .. })
    }

    /// Whether a session may reach `Streaming` right now. A node following an
    /// upstream only serves sub-replicas when chaining is enabled.
    fn may_stream(&self) -> bool {
        !self.is_replica() || self.chains == ChainPolicy::On
    }

    /// The tail of `ReplicaSession::run`: the phase flip, the conditional
    /// departure record, and the deregistration, in that order.
    fn run_exit(&self, session: &Arc<ReplicaSession>, departure: ReplicaDeparture) {
        let was_streaming = session.is_streaming();
        session.force_phase_for_test(Phase::Disconnecting);
        if was_streaming {
            self.tracker.record_streaming_departure(departure);
        }
        self.tracker.unregister_replica(session.id());
    }

    fn apply(&mut self, action: &LinkAction) -> Outcome {
        match *action {
            LinkAction::Write { key, value_len } => {
                let value = Bytes::from(vec![b'v'; 1 + usize::from(value_len) % 24]);
                self.handler
                    .broadcast_control_command("SET", &[Bytes::from(format!("key:{key}")), value]);
                Outcome::Accepted
            }

            LinkAction::Ack { slot, offset } => {
                let Some(session) = self.slots[slot].current().cloned() else {
                    return Outcome::Rejected("no session in slot");
                };
                // A session below Streaming has no reader task, so no ACK can
                // arrive on it. Modelling one would be inventing a wire event.
                if session.phase() != Phase::Streaming {
                    return Outcome::Rejected("session is not streaming");
                }
                let acked = self.resolve_offset(offset);
                self.handler.offsets.ingest_replica_ack(session.id(), acked);
                Outcome::Accepted
            }

            LinkAction::Attach { slot, announce } => {
                let state = &self.slots[slot];
                if !state.sessions.is_empty() && !state.lost {
                    return Outcome::Rejected("slot already connected");
                }
                if !self.may_stream() {
                    return Outcome::Rejected("chained replication is disabled");
                }
                let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.next_peer_port);
                self.next_peer_port += 1;
                let listening_port = match announce {
                    Announce::Port => REPLICA_BASE_PORT + slot as u16,
                    Announce::Silent => 0,
                };
                let session = self.tracker.register_announced_replica(
                    peer,
                    ReplicaAnnouncement {
                        listening_port,
                        capabilities: ReplicaCapabilities {
                            eof: true,
                            psync2: true,
                        },
                        version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    },
                );
                let state = &mut self.slots[slot];
                state.announced = announce == Announce::Port;
                state.lost = false;
                state.sessions.push(session);
                Outcome::Accepted
            }

            LinkAction::Psync { slot, id, offset } => {
                let Some(session) = self.slots[slot].current().cloned() else {
                    return Outcome::Rejected("no session in slot");
                };
                // Phases are forward-only through the production setter; a
                // session past `Connecting` has already resolved its PSYNC.
                if session.phase() != Phase::Connecting {
                    return Outcome::Rejected("session already synced");
                }
                if !self.may_stream() {
                    return Outcome::Rejected("chained replication is disabled");
                }
                let requested_id = self.resolve_psync_id(id);
                let req_offset = self.resolve_offset(offset);
                let current = self.live();
                let snapshot = self.handler.state();
                let decision = self.handler.replay.handle_partial_sync_request(
                    &snapshot,
                    &requested_id,
                    req_offset,
                    current,
                );
                match decision {
                    ReplayDecision::Continue(grant) => {
                        self.tracker.record_sync_outcome(SyncOutcome::PartialOk);
                        self.handler
                            .offsets
                            .seed_replica_position(session.id(), grant.resume_offset);
                        session.force_phase_for_test(Phase::Streaming);
                    }
                    ReplayDecision::FullResync(_) => {
                        self.tracker.record_sync_outcome(if id == PsyncId::Unknown {
                            SyncOutcome::FullResyncRequested
                        } else {
                            SyncOutcome::PartialRefused
                        });
                        session.force_phase_for_test(Phase::PreparingCheckpoint);
                        session.force_phase_for_test(Phase::StreamingCheckpoint);
                        // The checkpoint is cut at the live head, so that is
                        // where the replica resumes from.
                        let snapshot_offset = self.live();
                        self.handler
                            .offsets
                            .seed_replica_position(session.id(), snapshot_offset);
                        session.force_phase_for_test(Phase::Streaming);
                    }
                }
                // A replica reaching the live stream retires the departure
                // latch: this node has a follower again.
                self.tracker.clear_streaming_departure();
                Outcome::Accepted
            }

            LinkAction::Detach { slot, departure } => match departure {
                Departure::Silent => {
                    let state = &self.slots[slot];
                    if state.sessions.is_empty() || state.lost {
                        return Outcome::Rejected("nothing to lose");
                    }
                    self.slots[slot].lost = true;
                    Outcome::Accepted
                }
                Departure::Graceful | Departure::Lost => {
                    let Some(session) = self.slots[slot].oldest().cloned() else {
                        return Outcome::Rejected("no session in slot");
                    };
                    let departure = match departure {
                        Departure::Graceful => ReplicaDeparture::Graceful,
                        _ => ReplicaDeparture::Lost,
                    };
                    self.run_exit(&session, departure);
                    let state = &mut self.slots[slot];
                    state.sessions.remove(0);
                    if state.sessions.is_empty() {
                        state.lost = false;
                        state.announced = false;
                    }
                    Outcome::Accepted
                }
            },

            LinkAction::Promote => {
                if !self.is_replica() {
                    return Outcome::Rejected("already serving a primary stint");
                }
                match self.handler.begin_primary_stint() {
                    Ok(_) => {
                        self.role = RoleView::Primary;
                        Outcome::Accepted
                    }
                    Err(_) => Outcome::Rejected("primary stint could not be persisted"),
                }
            }

            LinkAction::Demote { upstream } => {
                if self.is_replica() {
                    return Outcome::Rejected("already following an upstream");
                }
                self.handler.end_primary_stint();
                // `end_primary_stint` asks every session to disconnect; each
                // one's `run()` then takes the exit above. The role flips only
                // once they are gone, which is what keeps `INV-ROLE-1` a claim
                // about a settled node rather than about the teardown.
                let sessions: Vec<Arc<ReplicaSession>> = self
                    .slots
                    .iter()
                    .flat_map(|slot| slot.sessions.iter().cloned())
                    .collect();
                for session in sessions {
                    self.run_exit(&session, ReplicaDeparture::Graceful);
                }
                for slot in &mut self.slots {
                    slot.sessions.clear();
                    slot.lost = false;
                    slot.announced = false;
                }
                self.role = RoleView::Replica {
                    upstream: upstream.then(|| {
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), REPLICA_BASE_PORT - 1)
                    }),
                };
                Outcome::Accepted
            }

            LinkAction::HoldFeed { hold } => {
                let held = self.feed_gate.is_held();
                self.feed_gate.publish(Some(hold.deadline()));
                if held {
                    Outcome::Rejected("a barrier was already up")
                } else {
                    Outcome::Accepted
                }
            }

            LinkAction::ReleaseFeed => {
                let held = self.feed_gate.is_held();
                self.feed_gate.publish(None);
                if held {
                    Outcome::Accepted
                } else {
                    Outcome::Rejected("the feed was already open")
                }
            }

            LinkAction::SaveState => match self.handler.save_state() {
                Ok(()) => Outcome::Accepted,
                Err(_) => Outcome::Rejected("state could not be persisted"),
            },

            LinkAction::RestoreState => {
                // A restart: every session dies with the process, so none of
                // them runs an exit path. Whatever is on disk is the node.
                let Ok(state) = ReplicationState::load_or_create(&state_path(self.dir.path()))
                else {
                    return Outcome::Rejected("state could not be read");
                };
                let tracker = ReplicationTrackerImpl::new_arc();
                let feed_gate = ReplicaFeedGate::open();
                let (handler, applied) =
                    build_handler(self.dir.path(), tracker.clone(), feed_gate.clone(), state);
                self.tracker = tracker;
                self.handler = handler;
                self.feed_gate = feed_gate;
                self.applied = applied;
                self.stint = None;
                for slot in &mut self.slots {
                    slot.sessions.clear();
                    slot.lost = false;
                    slot.announced = false;
                }
                Outcome::Accepted
            }

            LinkAction::StageMetadata { id, offset } => {
                let replication_id = match id {
                    StagedId::Current => self.handler.replication_id(),
                    StagedId::Foreign => FOREIGN_REPLICATION_ID.to_string(),
                    StagedId::Malformed => "not-a-replication-id".to_string(),
                };
                let meta = StagedReplicationMetadata {
                    replication_id,
                    replication_offset: self.resolve_offset(offset),
                    checksum: None,
                };
                let path = ReplicationState::staged_metadata_path(self.dir.path());
                if std::fs::write(&path, serde_json::to_vec(&meta).expect("serialize")).is_err() {
                    return Outcome::Rejected("staged metadata could not be written");
                }
                let read = read_staged_replication_metadata(self.dir.path());
                consume_staged_replication_metadata(self.dir.path());
                match read {
                    Ok(Some(meta)) => {
                        let shared = self.handler.shared_state();
                        let mut guard = shared.write();
                        guard.apply_staged_metadata(&meta);
                        drop(guard);
                        Outcome::Accepted
                    }
                    Ok(None) => Outcome::Rejected("staged metadata was refused"),
                    Err(_) => Outcome::Rejected("staged metadata could not be read"),
                }
            }

            LinkAction::EvictBacklog => {
                // `due` arms its idle clock on the first tick and fires on a
                // later one, so the maintenance path is ticked twice with the
                // clock backdated in between rather than sleeping out a TTL.
                self.handler.expire_idle_backlog();
                self.handler
                    .backlog_ttl()
                    .backdate_idle_clock_for_test(Duration::from_secs(BACKLOG_TTL_SECS + 1));
                if self.handler.expire_idle_backlog() {
                    Outcome::Accepted
                } else {
                    Outcome::Rejected("the backlog is not idle")
                }
            }

            LinkAction::Freeze => {
                self.applied.freeze();
                Outcome::Accepted
            }

            LinkAction::AdmitDivergence { epoch } => {
                if self.stint.is_none() {
                    self.stint = Some(self.applied.begin_replica_stint());
                }
                let stint = self.stint.as_ref().expect("stint was just installed");
                let live_epoch = stint.epoch();
                let named = match epoch {
                    EpochRef::Current => live_epoch,
                    EpochRef::Stale => live_epoch.wrapping_add(1),
                };
                stint.admit_divergence(named);
                if named == live_epoch {
                    Outcome::Accepted
                } else {
                    Outcome::Rejected("the admission names a retired stint")
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Known defects
// ---------------------------------------------------------------------------

/// Actions the generator must not emit because a *known, filed* defect makes
/// them violate the catalog.
///
/// Every arm names the issue it stands for and is pinned by a `#[should_panic]`
/// witness below, so a fix turns the witness red rather than leaving the muzzle
/// silently covering healthy code. The arms match on the *resolved* effect, not
/// on a coarse action shape, so the shapes that merely resemble a defect stay in
/// the property (see `the_muzzle_only_covers_the_pinned_shapes`).
fn known_defect(node: &LinkNode, action: &LinkAction) -> Option<&'static str> {
    match *action {
        // Issue 20: `REPLCONF ACK` is stored by `ReplicaSession::record_ack`
        // whenever it advances, with no ceiling at the live head, and
        // `OffsetCoordinator::ingest_replica_ack` does not clamp it either. A
        // replica that names an offset this primary never wrote is credited
        // with it, which is a HARD `INV-OFFSET-3` violation at the ingesting
        // seam's own hook.
        LinkAction::Ack { slot, offset } => {
            let session = node.slots[slot].current()?;
            if session.phase() != Phase::Streaming {
                return None;
            }
            (node.resolve_offset(offset) > node.live())
                .then_some("issue 21: an ACK above the live head is admitted unclamped")
        }
        // Issue 21: nothing dedupes sessions by announced identity. A replica
        // that reconnects before this node has noticed the old link is gone
        // gets a second session, and nothing kicks the first — so both reach
        // `Streaming` on one `(ip, port)`, which is a HARD `INV-SESSION-2`
        // violation.
        LinkAction::Psync { slot, .. } => {
            let state = &node.slots[slot];
            if !state.announced || !node.may_stream() {
                return None;
            }
            let target = state.current()?;
            if target.phase() != Phase::Connecting {
                return None;
            }
            state
                .sessions
                .iter()
                .rev()
                .skip(1)
                .any(|session| session.phase() == Phase::Streaming)
                .then_some("issue 22: a reconnect streams beside the session it replaces")
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

/// Build the action a step names, against the node as it stands.
fn action_for(node: &LinkNode, step: Step) -> LinkAction {
    // In-context: a slot that currently holds a session. Out of context: any
    // slot, which is how the empty-slot rejections get generated.
    let occupied: Vec<usize> = (0..REPLICA_SLOTS)
        .filter(|slot| !node.slots[*slot].sessions.is_empty())
        .collect();
    let free: Vec<usize> = (0..REPLICA_SLOTS)
        .filter(|slot| node.slots[*slot].sessions.is_empty() || node.slots[*slot].lost)
        .collect();
    let any_slot = usize::from(step.slot) % REPLICA_SLOTS;
    let pick = |pool: &[usize]| -> usize {
        if step.in_context && !pool.is_empty() {
            pool[usize::from(step.slot) % pool.len()]
        } else {
            any_slot
        }
    };

    match variant_for(step.variant) {
        Variant::Write => LinkAction::Write {
            key: step.a,
            value_len: step.b,
        },
        Variant::Ack => {
            let offset = if step.in_context {
                match step.a % 3 {
                    0 => AckOffset::Live,
                    1 => AckOffset::Behind(u16::from(step.b)),
                    _ => AckOffset::Zero,
                }
            } else {
                AckOffset::Zero
            };
            LinkAction::Ack {
                slot: pick(&occupied),
                offset,
            }
        }
        Variant::Attach => LinkAction::Attach {
            slot: pick(&free),
            // Silent is the rarer real shape: most replicas announce.
            announce: if step.a.is_multiple_of(4) {
                Announce::Silent
            } else {
                Announce::Port
            },
        },
        Variant::Psync => {
            let id = if step.in_context {
                match step.a % 3 {
                    0 => PsyncId::Current,
                    1 => PsyncId::Secondary,
                    _ => PsyncId::Unknown,
                }
            } else {
                PsyncId::Foreign
            };
            let offset = if step.in_context {
                match step.b % 3 {
                    0 => AckOffset::Live,
                    1 => AckOffset::Behind(u16::from(step.b)),
                    _ => AckOffset::Zero,
                }
            } else {
                AckOffset::Zero
            };
            LinkAction::Psync {
                slot: pick(&occupied),
                id,
                offset,
            }
        }
        Variant::Detach => LinkAction::Detach {
            slot: pick(&occupied),
            departure: match step.a % 3 {
                0 => Departure::Graceful,
                1 => Departure::Lost,
                _ => Departure::Silent,
            },
        },
        Variant::Promote => LinkAction::Promote,
        Variant::Demote => LinkAction::Demote {
            upstream: step.flag,
        },
        Variant::HoldFeed => LinkAction::HoldFeed {
            hold: match step.a % 3 {
                0 => HoldSpan::Short,
                1 => HoldSpan::Long,
                _ => HoldSpan::Expired,
            },
        },
        Variant::ReleaseFeed => LinkAction::ReleaseFeed,
        Variant::SaveState => LinkAction::SaveState,
        Variant::RestoreState => LinkAction::RestoreState,
        Variant::StageMetadata => LinkAction::StageMetadata {
            id: if step.in_context {
                StagedId::Current
            } else {
                match step.a % 2 {
                    0 => StagedId::Foreign,
                    _ => StagedId::Malformed,
                }
            },
            offset: if step.in_context {
                AckOffset::Behind(u16::from(step.b))
            } else {
                AckOffset::Live
            },
        },
        Variant::EvictBacklog => LinkAction::EvictBacklog,
        Variant::Freeze => LinkAction::Freeze,
        Variant::AdmitDivergence => LinkAction::AdmitDivergence {
            epoch: if step.in_context {
                EpochRef::Current
            } else {
                EpochRef::Stale
            },
        },
    }
}

/// Fold the steps through a real node, keeping the actions that were reachable.
///
/// The apply is caught: a panic here would escape proptest's own `catch_unwind`
/// and abort the run with no counterexample to shrink. Catching truncates the
/// sequence at the offending action *inclusive*, so the property fold — which
/// does not catch — reproduces the panic as a shrinkable failure.
fn build_sequence(steps: &[Step], chains: ChainPolicy) -> Vec<LinkAction> {
    let mut node = LinkNode::fresh(chains);
    let mut actions = Vec::with_capacity(steps.len());
    for step in steps {
        let action = action_for(&node, *step);
        if known_defect(&node, &action).is_some() {
            continue;
        }
        let applied = std::panic::catch_unwind(AssertUnwindSafe(|| {
            node.apply(&action);
        }));
        actions.push(action);
        if applied.is_err() {
            break;
        }
    }
    actions
}

/// A sequence of `len` actions over a node with chaining disabled.
pub(crate) fn arb_link_sequence(len: usize) -> impl Strategy<Value = Vec<LinkAction>> {
    arb_link_sequence_with(len, ChainPolicy::Off)
}

fn arb_link_sequence_with(
    len: usize,
    chains: ChainPolicy,
) -> impl Strategy<Value = Vec<LinkAction>> {
    proptest::collection::vec(arb_step(), len).prop_map(move |steps| build_sequence(&steps, chains))
}

// ---------------------------------------------------------------------------
// Snapshots
//
// R2 and R4 both compare two runs of the same actions. A raw `ReplicationView`
// cannot be compared across runs: a promotion mints a *random* replication id,
// and three fields are wall-clock samples. A `Snapshot` is the view with the
// entropy interned to first-appearance symbols and the clock reads dropped —
// what is left is exactly the part of the node two identical runs must agree
// on.
// ---------------------------------------------------------------------------

/// Length of a replication id, as `generate_replication_id` mints it.
const ID_LEN: usize = 40;

/// Maps the random replication ids a run mints onto `#0`, `#1`, ... in the
/// order they are first seen, so two runs that mint different ids in the same
/// order compare equal.
#[derive(Default)]
struct Interner {
    ids: Vec<String>,
}

impl Interner {
    fn symbol(&mut self, id: &str) -> String {
        let index = match self.ids.iter().position(|known| known == id) {
            Some(index) => index,
            None => {
                self.ids.push(id.to_string());
                self.ids.len() - 1
            }
        };
        format!("#{index}")
    }

    /// The same substitution over rendered text, for the view fields whose
    /// types carry ids inside them.
    fn canon(&mut self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while !rest.is_empty() {
            // The longest run of hex digits at the cursor. Taking the *maximal*
            // run is what stops a 41-hex-digit token being read as an id.
            let run = rest
                .char_indices()
                .find(|(_, ch)| !ch.is_ascii_hexdigit())
                .map_or(rest.len(), |(index, _)| index);
            if run == ID_LEN {
                let symbol = self.symbol(&rest[..run]);
                out.push_str(&symbol);
            } else if run > 0 {
                out.push_str(&rest[..run]);
            } else {
                let mut chars = rest.chars();
                let ch = chars.next().expect("the remainder is non-empty");
                out.push(ch);
                rest = chars.as_str();
                continue;
            }
            rest = &rest[run..];
        }
        out
    }
}

/// The persisted identity, with the minted ids interned.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StateSnapshot {
    replication_id: String,
    secondary_id: Option<String>,
    offset_at_save: u64,
    secondary_offset: i64,
    active_version: Option<String>,
    master_host: Option<String>,
    master_port: Option<u16>,
}

impl StateSnapshot {
    fn of(state: &ReplicationState, interner: &mut Interner) -> Self {
        Self {
            replication_id: interner.symbol(&state.replication_id),
            secondary_id: state.secondary_id.as_deref().map(|id| interner.symbol(id)),
            offset_at_save: state.offset_at_save,
            secondary_offset: state.secondary_offset,
            active_version: state.active_version.clone(),
            master_host: state.master_host.clone(),
            master_port: state.master_port,
        }
    }
}

/// One registered session, minus its ACK age.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionSnapshot {
    id: u64,
    addr: SocketAddr,
    announced_id: Option<(IpAddr, u16)>,
    phase: Phase,
    acked: u64,
    resume_floor: u64,
    // `ReplicaView::last_ack_age` is sampled from the wall clock at capture
    // time, so it differs between two runs of the same actions by however long
    // the first run took. Dropping it is what makes R4 a statement about the
    // decisions rather than about the machine.
}

/// A node's whole projection, comparable across runs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Snapshot {
    state: Option<StateSnapshot>,
    offsets: Option<OffsetTriple>,
    apply_gate: Option<ApplyGateView>,
    backlog: Option<BacklogView>,
    replicas: Option<Vec<SessionSnapshot>>,
    departure: Option<ReplicaDeparture>,
    /// `FeedGateView::hold_remaining` counts down against the wall clock; only
    /// whether the barrier is up, and the budget it is measured against, are
    /// decisions.
    feed_held: Option<bool>,
    barrier_budget: Option<Duration>,
    fence: Option<crate::view::FenceView>,
    role: Option<RoleView>,
    promotion: Option<String>,
    grant: Option<ContinueGrant>,
    phase_change: Option<PhaseChange>,
}

impl Snapshot {
    fn of(view: &ReplicationView, interner: &mut Interner) -> Self {
        // Destructured rather than field-accessed so a new view field is a
        // compile error here instead of a silently uncompared value.
        let ReplicationView {
            state,
            offsets,
            apply_gate,
            backlog,
            replicas,
            departure,
            feed_gate,
            fence,
            role,
            promotion,
            grant,
            phase_change,
        } = view;
        Self {
            state: state
                .as_ref()
                .map(|state| StateSnapshot::of(state, interner)),
            offsets: *offsets,
            apply_gate: *apply_gate,
            backlog: *backlog,
            replicas: replicas.as_ref().map(|replicas| {
                replicas
                    .iter()
                    .map(|replica| SessionSnapshot {
                        id: replica.id,
                        addr: replica.addr,
                        announced_id: replica.announced_id,
                        phase: replica.phase,
                        acked: replica.acked,
                        resume_floor: replica.resume_floor,
                    })
                    .collect()
            }),
            departure: *departure,
            feed_held: feed_gate.map(|gate| gate.is_held),
            barrier_budget: feed_gate.and_then(|gate| gate.barrier_budget),
            fence: *fence,
            role: role.clone(),
            // Carries the pre-mint and post-mint ids, so it goes through the
            // interner as rendered text.
            promotion: promotion
                .as_ref()
                .map(|witness| interner.canon(&format!("{witness:?}"))),
            grant: *grant,
            phase_change: *phase_change,
        }
    }
}

/// One step of a run: what the node said about the action, and where the action
/// left it.
type Step2 = (Outcome, Snapshot);

/// Drive a fresh node through `actions`, snapshotting after each one.
fn replay_snapshots(actions: &[LinkAction]) -> Vec<Step2> {
    let mut node = LinkNode::fresh(ChainPolicy::Off);
    let mut interner = Interner::default();
    actions
        .iter()
        .map(|action| {
            let outcome = node.apply(action);
            (outcome, Snapshot::of(&node.view(), &mut interner))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// R2 — the two persistence vehicles
// ---------------------------------------------------------------------------

/// The two ways this node's replication identity crosses a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Vehicle {
    /// `ReplicationState::save` -> `ReplicationState::load_or_create`: the
    /// state file, written at every save point.
    StateFile,
    /// `read_staged_replication_metadata` -> `apply_staged_metadata`: the
    /// trailer a full-sync checkpoint carries, adopted when the snapshot is
    /// installed.
    StagedMetadata,
}

/// What a round trip carried across.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carried {
    /// Everything. The node is the one it was, so the rest of the run is
    /// comparable with an uninterrupted one.
    Whole,
    /// Everything except the failover window, which the staged vehicle clears
    /// on purpose: adopting a checkpoint means adopting the primary's history
    /// wholesale, and any window this node carried described a stream it no
    /// longer holds (`ReplicationState::apply_staged_metadata`). The node is a
    /// different one from here on and the suffix is not compared.
    WholeExceptTheFailoverWindow,
}

/// Round-trip this node's persisted state through `vehicle` and install the
/// result, asserting the vehicle is lossless on the way.
///
/// The trip runs against a scratch directory, never the node's own: writing the
/// node's state file would leave residue a later `RestoreState` in the same run
/// would read, which is a change to the run rather than a round trip inside it.
fn round_trip(node: &LinkNode, vehicle: Vehicle) -> Result<Carried, String> {
    let scratch = tempfile::tempdir().map_err(|err| format!("scratch dir: {err}"))?;
    let before = node.handler.state();
    match vehicle {
        Vehicle::StateFile => {
            let path = state_path(scratch.path());
            before.save(&path).map_err(|err| format!("save: {err}"))?;
            let after =
                ReplicationState::load_or_create(&path).map_err(|err| format!("load: {err}"))?;
            if after != before {
                return Err(format!(
                    "the state file dropped something across a save point:\n  before: \
                     {before:?}\n  after:  {after:?}"
                ));
            }
            *node.handler.shared_state().write() = after;
            Ok(Carried::Whole)
        }
        Vehicle::StagedMetadata => {
            // The trailer a checkpoint carries describes the identity and the
            // save-point offset of the state that cut it.
            let staged = StagedReplicationMetadata {
                replication_id: before.replication_id.clone(),
                replication_offset: before.offset_at_save,
                checksum: None,
            };
            let path = ReplicationState::staged_metadata_path(scratch.path());
            std::fs::write(
                &path,
                serde_json::to_vec(&staged).map_err(|err| format!("serialize: {err}"))?,
            )
            .map_err(|err| format!("stage: {err}"))?;
            let read = read_staged_replication_metadata(scratch.path())
                .map_err(|err| format!("read staged: {err}"))?
                .ok_or_else(|| "the vehicle refused a trailer it had just written".to_string())?;
            consume_staged_replication_metadata(scratch.path());
            let mut after = before.clone();
            after.apply_staged_metadata(&read);

            // The one difference the vehicle is *allowed* to have, stated
            // exactly: the failover window goes, and nothing else does.
            let expected = ReplicationState {
                secondary_id: None,
                secondary_offset: -1,
                ..before.clone()
            };
            if after != expected {
                return Err(format!(
                    "the staged vehicle dropped more than the failover window:\n  before:   \
                     {before:?}\n  expected: {expected:?}\n  after:    {after:?}"
                ));
            }
            let carried = if before.secondary_id.is_none() && before.secondary_offset == -1 {
                Carried::Whole
            } else {
                Carried::WholeExceptTheFailoverWindow
            };
            *node.handler.shared_state().write() = after;
            Ok(carried)
        }
    }
}

/// A run of `actions` with a `vehicle` round trip spliced in before action
/// `split` (or after the last action when `split == actions.len()`).
struct Interrupted {
    steps: Vec<Step2>,
    carried: Carried,
}

fn replay_with_round_trip(
    actions: &[LinkAction],
    split: usize,
    vehicle: Vehicle,
) -> Result<Interrupted, String> {
    let mut node = LinkNode::fresh(ChainPolicy::Off);
    let mut interner = Interner::default();
    let mut steps = Vec::with_capacity(actions.len());
    let mut carried = Carried::Whole;
    for (index, action) in actions.iter().enumerate() {
        if index == split {
            carried = round_trip(&node, vehicle)?;
        }
        let outcome = node.apply(action);
        steps.push((outcome, Snapshot::of(&node.view(), &mut interner)));
    }
    if split >= actions.len() {
        carried = round_trip(&node, vehicle)?;
    }
    Ok(Interrupted { steps, carried })
}

// ---------------------------------------------------------------------------
// R3 — the PSYNC decision
// ---------------------------------------------------------------------------

/// The replication id a probe asks with. The last four are shapes no
/// well-behaved replica sends, which is the point: the decision is total over
/// the wire, not over the ids this node happens to mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeId {
    Current,
    Secondary,
    Foreign,
    Unknown,
    Empty,
    Uppercase,
    TooShort,
    TooLong,
    NotHex,
}

/// The offset a probe asks from, including the ones outside every window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeOffset {
    Zero,
    Live,
    JustBelowLive,
    JustAboveLive,
    Half,
    AtFloor,
    JustBelowFloor,
    Max,
}

/// One `PSYNC <id> <offset>` put to the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PsyncProbe {
    id: ProbeId,
    offset: ProbeOffset,
}

fn arb_psync_probe() -> impl Strategy<Value = PsyncProbe> {
    let ids = [
        ProbeId::Current,
        ProbeId::Secondary,
        ProbeId::Foreign,
        ProbeId::Unknown,
        ProbeId::Empty,
        ProbeId::Uppercase,
        ProbeId::TooShort,
        ProbeId::TooLong,
        ProbeId::NotHex,
    ];
    let offsets = [
        ProbeOffset::Zero,
        ProbeOffset::Live,
        ProbeOffset::JustBelowLive,
        ProbeOffset::JustAboveLive,
        ProbeOffset::Half,
        ProbeOffset::AtFloor,
        ProbeOffset::JustBelowFloor,
        ProbeOffset::Max,
    ];
    (0..ids.len(), 0..offsets.len()).prop_map(move |(id, offset)| PsyncProbe {
        id: ids[id],
        offset: offsets[offset],
    })
}

impl LinkNode {
    fn probe_id(&self, id: ProbeId) -> String {
        match id {
            ProbeId::Current => self.handler.replication_id(),
            ProbeId::Secondary => self
                .handler
                .state()
                .secondary_id
                .unwrap_or_else(|| self.handler.replication_id()),
            ProbeId::Foreign => FOREIGN_REPLICATION_ID.to_string(),
            ProbeId::Unknown => "?".to_string(),
            ProbeId::Empty => String::new(),
            ProbeId::Uppercase => self.handler.replication_id().to_uppercase(),
            ProbeId::TooShort => self.handler.replication_id()[..ID_LEN - 1].to_string(),
            ProbeId::TooLong => format!("{}0", self.handler.replication_id()),
            ProbeId::NotHex => "z".repeat(ID_LEN),
        }
    }

    fn probe_offset(&self, offset: ProbeOffset) -> u64 {
        let live = self.live();
        let floor = self.handler.replay.backlog_start();
        match offset {
            ProbeOffset::Zero => 0,
            ProbeOffset::Live => live,
            ProbeOffset::JustBelowLive => live.saturating_sub(1),
            ProbeOffset::JustAboveLive => live.saturating_add(1),
            ProbeOffset::Half => live / 2,
            ProbeOffset::AtFloor => floor.unwrap_or(live),
            ProbeOffset::JustBelowFloor => floor.unwrap_or(live).saturating_sub(1),
            ProbeOffset::Max => u64::MAX,
        }
    }
}

/// Everything a `+CONTINUE` must be true of, checked against the ring it was
/// cut from. Returns the first thing that is not.
///
/// `FM-REPLICATION-013/014/015` are the three scenarios this states once: a
/// grant names `(replay_from, resume_offset]`, that range is inside the armed
/// window, and the frames handed over cover it with no hole.
fn grant_is_sound(
    grant: &crate::primary::replay::ReplayGrant,
    req_offset: u64,
    current_offset: u64,
    floor: Option<u64>,
) -> Result<(), String> {
    if grant.replay_from != req_offset {
        return Err(format!(
            "the grant resumes from {} but the replica asked from {req_offset}",
            grant.replay_from
        ));
    }
    if grant.resume_offset != current_offset {
        return Err(format!(
            "the grant lands at {} but the live head is {current_offset}",
            grant.resume_offset
        ));
    }
    if grant.replay_from > grant.resume_offset {
        return Err(format!(
            "the grant names a range that runs backwards: ({}, {}]",
            grant.replay_from, grant.resume_offset
        ));
    }
    match floor {
        None => {
            return Err("a grant was made with no backlog window armed".to_string());
        }
        Some(floor) if floor > grant.replay_from => {
            return Err(format!(
                "the grant resumes from {} but the window floor is {floor}",
                grant.replay_from
            ));
        }
        Some(_) => {}
    }
    // The frames cover `(replay_from, resume_offset]` with no hole: each one
    // begins exactly where the previous ended, the first begins at or before
    // the resume point, and the last ends at the head.
    let mut cursor = grant.replay_from;
    for (index, (end, _shard, payload)) in grant.frames.iter().enumerate() {
        let len = payload.len() as u64;
        let begin = end.checked_sub(len).ok_or_else(|| {
            format!("frame {index} ends at {end} but carries {len} bytes, so it begins below zero")
        })?;
        if index == 0 {
            if begin > cursor {
                return Err(format!(
                    "frame 0 begins at {begin}, leaving a hole above the resume point {cursor}"
                ));
            }
        } else if begin != cursor {
            return Err(format!(
                "frame {index} begins at {begin} but frame {} ended at {cursor}",
                index - 1
            ));
        }
        if *end > grant.resume_offset {
            return Err(format!(
                "frame {index} ends at {end}, above the resume offset {}",
                grant.resume_offset
            ));
        }
        cursor = *end;
    }
    if let Some((end, _, _)) = grant.frames.last()
        && *end != grant.resume_offset
    {
        return Err(format!(
            "the replay stops at {end} but the grant promises {}",
            grant.resume_offset
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// R5 — offset conservation
// ---------------------------------------------------------------------------

/// One move against the offset pair. Each is a real production door: the
/// primary's advance gate, the replica's frame ingest, and the five gate
/// operations the applier drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateAction {
    /// `OffsetCoordinator::advance` — this node's own write.
    Advance { bytes: u8 },
    /// `ReplicaOffset::frame_advance` — a frame arrives from upstream.
    Receive { bytes: u8 },
    /// `ReplicaApplyStint::claim` for the oldest received-but-unclaimed frame.
    Claim { epoch: EpochRef },
    /// `ReplicaApplyStint::land`.
    Land,
    /// `AppliedOffset::freeze` — a promotion takes the gate.
    Freeze,
    /// `ReplicaApplyStint::admit_divergence`.
    AdmitDivergence { epoch: EpochRef },
    /// `AppliedOffset::retire_replica_applies` — a newer stream takes over.
    Retire,
    /// A fresh stream: `begin_replica_stint` plus the `ReplicaOffset` built
    /// over it, which is how production opens one.
    BeginStint,
    /// `ReplicaOffset::reset_to` — a full resync adopts a snapshot position.
    Resync { above: bool, delta: u8 },
}

fn arb_gate_action() -> impl Strategy<Value = GateAction> {
    prop_oneof![
        6 => any::<u8>().prop_map(|bytes| GateAction::Advance { bytes }),
        10 => any::<u8>().prop_map(|bytes| GateAction::Receive { bytes }),
        12 => prop_oneof![Just(EpochRef::Current), Just(EpochRef::Stale)]
            .prop_map(|epoch| GateAction::Claim { epoch }),
        8 => Just(GateAction::Land),
        2 => Just(GateAction::Freeze),
        2 => prop_oneof![Just(EpochRef::Current), Just(EpochRef::Stale)]
            .prop_map(|epoch| GateAction::AdmitDivergence { epoch }),
        2 => Just(GateAction::Retire),
        3 => Just(GateAction::BeginStint),
        3 => (any::<bool>(), any::<u8>())
            .prop_map(|(above, delta)| GateAction::Resync { above, delta }),
    ]
}

/// A real offset pair with both doors open on it, plus the ledger the
/// conservation claim is stated against.
///
/// The ledger is arithmetic over the *verdicts the production code returned* —
/// how many bytes each call reported admitting — not a re-implementation of
/// when it admits them. That is what keeps this a property and not a shadow
/// model: the harness never predicts a `Claim`, it only adds up the ones that
/// came back `Granted`.
struct GateHarness {
    identity: ReplicationIdentity,
    offset: ReplicaOffset,
    applied: AppliedOffset,
    stint: ReplicaApplyStint,
    coordinator: OffsetCoordinator,
    /// Frame sizes received but not yet claimed, oldest first.
    pending: std::collections::VecDeque<u64>,
    /// The position both heads were last rebased to (0, or the last accepted
    /// resync).
    seed: u64,
    /// Bytes the live head was moved by since `seed`.
    received: u64,
    /// Bytes the applied head was moved by since `seed`.
    admitted: u64,
    /// The applied head at the last `land()`, which is what `landed` must hold.
    landed: u64,
}

impl GateHarness {
    fn fresh() -> Self {
        let tracker = ReplicationTrackerImpl::new_arc();
        let identity = ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
        let coordinator = OffsetCoordinator::new(tracker, &identity);
        let applied = identity.applied();
        let stint = applied.begin_replica_stint();
        let offset = ReplicaOffset::new(identity.state(), identity.live(), applied.clone());
        Self {
            identity,
            offset,
            applied,
            stint,
            coordinator,
            pending: std::collections::VecDeque::new(),
            seed: 0,
            received: 0,
            admitted: 0,
            landed: 0,
        }
    }

    fn apply(&mut self, action: GateAction) {
        match action {
            GateAction::Advance { bytes } => {
                // A zero-byte write is not a write: the advance unit is the
                // payload, and every real command has one.
                let n = u64::from(bytes) + 1;
                self.coordinator
                    .advance(&Bytes::from(vec![b'w'; n as usize]));
                self.received += n;
                self.admitted += n;
                // The primary path lands what it advances (`advance_by` moves
                // the landed head with it).
                self.landed = self.landed.max(self.seed + self.admitted);
            }
            GateAction::Receive { bytes } => {
                let n = u64::from(bytes) + 1;
                let frame = ReplicationFrame::new(0, Bytes::from(vec![b'f'; n as usize]));
                self.offset.frame_advance(&frame);
                self.received += n;
                self.pending.push_back(n);
            }
            GateAction::Claim { epoch } => {
                let Some(bytes) = self.pending.front().copied() else {
                    return;
                };
                let live_epoch = self.stint.epoch();
                let named = match epoch {
                    EpochRef::Current => live_epoch,
                    EpochRef::Stale => live_epoch.wrapping_add(1),
                };
                if self.stint.claim(named, bytes) == Claim::Granted {
                    self.pending.pop_front();
                    self.admitted += bytes;
                }
            }
            GateAction::Land => {
                self.stint.land();
                self.landed = self.landed.max(self.seed + self.admitted);
            }
            GateAction::Freeze => {
                self.applied.freeze();
            }
            GateAction::AdmitDivergence { epoch } => {
                let live_epoch = self.stint.epoch();
                let named = match epoch {
                    EpochRef::Current => live_epoch,
                    EpochRef::Stale => live_epoch.wrapping_add(1),
                };
                self.stint.admit_divergence(named);
            }
            GateAction::Retire => {
                self.applied.retire_replica_applies();
            }
            GateAction::BeginStint => {
                self.stint = self.applied.begin_replica_stint();
                self.offset = ReplicaOffset::new(
                    self.identity.state(),
                    self.identity.live(),
                    self.applied.clone(),
                );
                // Frames decoded under the previous stream are void.
                self.pending.clear();
            }
            GateAction::Resync { above, delta } => {
                let here = self.seed + self.admitted;
                let target = if above {
                    here.saturating_add(u64::from(delta))
                } else {
                    here.saturating_sub(u64::from(delta))
                };
                if self.offset.reset_to(target) {
                    // Both heads adopted the snapshot's position, so the ledger
                    // rebases on it.
                    self.seed = target;
                    self.received = 0;
                    self.admitted = 0;
                    self.landed = target;
                    self.pending.clear();
                }
            }
        }
    }

    /// What conservation says the heads must read.
    fn audit(&self) -> Result<(), String> {
        let view = self.offset.view();
        let triple = view
            .offsets
            .ok_or("the replica offset published no triple")?;
        let live = triple
            .live
            .ok_or("the replica offset published no live head")?;
        if live != self.seed + self.received {
            return Err(format!(
                "live is {live} but {} bytes were admitted above the seed {}",
                self.received, self.seed
            ));
        }
        if triple.applied != self.seed + self.admitted {
            return Err(format!(
                "applied is {} but {} bytes were claimed above the seed {}",
                triple.applied, self.admitted, self.seed
            ));
        }
        if triple.landed != self.landed {
            return Err(format!(
                "landed is {} but the last land reported {}",
                triple.landed, self.landed
            ));
        }
        if !(triple.landed <= triple.applied && triple.applied <= live) {
            return Err(format!(
                "the triple is out of order: landed {} applied {} live {live}",
                triple.landed, triple.applied
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// R1
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config())]

    /// **R1.** Every action a replication link can take leaves the node's whole
    /// projection free of HARD invariant violations — including the actions the
    /// node rejects, which is where a half-applied transition would hide.
    ///
    /// The oracle is `check_hard` over the catalog, not a shadow model: the
    /// node is real, and so is every call the actions make.
    #[test]
    fn r1_every_action_leaves_the_catalog_clean(
        actions in arb_link_sequence(SEQUENCE_LEN),
    ) {
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        for (index, action) in actions.iter().enumerate() {
            let outcome = node.apply(action);
            let violations = invariants::check_hard(&node.view());
            prop_assert!(
                violations.is_empty(),
                "action {index} ({action:?}, {outcome:?}) left {} HARD violation(s):\n{}",
                violations.len(),
                invariants::render(&violations),
            );
        }
    }

    /// **R2.** A save point is transparent: the node's persisted state survives
    /// a round trip through either vehicle, and the run continues exactly as if
    /// the trip had not happened.
    ///
    /// The trip is spliced in at a generated point of a generated run, so
    /// "at any point" is literal rather than a handful of hand-picked moments.
    /// The two vehicles are held to the same claim with one stated difference:
    /// the staged trailer clears the failover window on purpose, so a state
    /// that carries one is not comparable past the trip — everything else about
    /// it still is, and `round_trip` asserts that.
    #[test]
    fn r2_a_save_point_round_trip_is_transparent(
        actions in arb_link_sequence(SEQUENCE_LEN),
        at in any::<proptest::sample::Index>(),
    ) {
        let split = at.index(actions.len() + 1);
        let baseline = replay_snapshots(&actions);
        for vehicle in [Vehicle::StateFile, Vehicle::StagedMetadata] {
            let run = replay_with_round_trip(&actions, split, vehicle)
                .map_err(|err| TestCaseError::fail(format!("{vehicle:?} at {split}: {err}")))?;
            prop_assert_eq!(
                run.steps.len(),
                baseline.len(),
                "{:?} at {} changed how far the run got",
                vehicle,
                split,
            );
            if run.carried == Carried::WholeExceptTheFailoverWindow {
                continue;
            }
            for (index, (after, before)) in run.steps.iter().zip(baseline.iter()).enumerate() {
                prop_assert_eq!(
                    after,
                    before,
                    "{:?} at {} changed action {}",
                    vehicle,
                    split,
                    index,
                );
            }
        }
    }

    /// **R3.** The PSYNC decision is total. It answers every `(id, offset)` a
    /// wire can carry with exactly one `ReplayDecision` and no panic, and every
    /// `+CONTINUE` it grants names a range that is inside the armed window and
    /// covered by the frames handed over with no hole.
    ///
    /// Probing after *every* action means the ring the decision is cut from is
    /// itself arbitrary: empty, armed-but-empty, full, evicted, reset by a
    /// promotion, or closed by the TTL.
    #[test]
    fn r3_every_psync_request_yields_one_sound_decision(
        actions in arb_link_sequence(SEQUENCE_LEN),
        probes in proptest::collection::vec(arb_psync_probe(), 1..4),
    ) {
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        for (index, action) in actions.iter().enumerate() {
            node.apply(action);
            for probe in &probes {
                let requested_id = node.probe_id(probe.id);
                let req_offset = node.probe_offset(probe.offset);
                let current = node.live();
                let state = node.handler.state();
                let floor = node.handler.replay.backlog_start();
                let decision = node.handler.replay.handle_partial_sync_request(
                    &state,
                    &requested_id,
                    req_offset,
                    current,
                );
                // A pure decision: asked twice over an unchanged ring it gives
                // the same answer. This is the purity half of R4, stated where
                // the decision lives.
                let again = node.handler.replay.handle_partial_sync_request(
                    &state,
                    &requested_id,
                    req_offset,
                    current,
                );
                prop_assert_eq!(
                    std::mem::discriminant(&decision),
                    std::mem::discriminant(&again),
                    "action {} probe {:?} answered two different ways",
                    index,
                    probe,
                );
                if let ReplayDecision::Continue(grant) = &decision {
                    prop_assert!(
                        grant_is_sound(grant, req_offset, current, floor).is_ok(),
                        "action {} probe {:?} granted an unsound +CONTINUE: {}",
                        index,
                        probe,
                        grant_is_sound(grant, req_offset, current, floor).unwrap_err(),
                    );
                }
            }
        }
    }

    /// **R4.** The same actions drive two fresh nodes to the same place, step
    /// for step — the decision at every action and the whole projection after
    /// it.
    ///
    /// The comparison is over a `Snapshot`, which drops the two things that are
    /// *supposed* to differ between two runs: the ids a promotion mints (kept,
    /// but interned to first-appearance symbols, so their identity relations
    /// are still compared) and the wall-clock samples. Anything else that
    /// differs is a decision that read something it should not have — the
    /// cluster issue 23 shape.
    #[test]
    fn r4_the_same_actions_reach_the_same_node(
        actions in arb_link_sequence(SEQUENCE_LEN),
    ) {
        let first = replay_snapshots(&actions);
        let second = replay_snapshots(&actions);
        prop_assert_eq!(first.len(), second.len());
        for (index, (left, right)) in first.iter().zip(second.iter()).enumerate() {
            prop_assert_eq!(
                left,
                right,
                "two runs of the same actions disagree at action {} ({:?})",
                index,
                actions[index],
            );
        }
    }

    /// **R5.** Offsets are conserved. The live head is the seed plus every byte
    /// admitted through an advance door; the applied head is the seed plus
    /// every byte a claim came back `Granted` for; the landed head is what the
    /// last `land()` reported; and `landed <= applied <= live` holds after
    /// every one of them, under arbitrary interleavings of `claim`, `land`,
    /// `freeze`, `admit_divergence`, `retire_replica_applies` and a resync.
    #[test]
    fn r5_the_offset_triple_is_conserved(
        actions in proptest::collection::vec(arb_gate_action(), 1..64),
    ) {
        let mut harness = GateHarness::fresh();
        prop_assert!(harness.audit().is_ok(), "a fresh pair is already out: {:?}", harness.audit());
        for (index, action) in actions.iter().enumerate() {
            harness.apply(*action);
            if let Err(err) = harness.audit() {
                return Err(TestCaseError::fail(format!(
                    "action {index} ({action:?}) broke conservation: {err}"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    /// Deterministic sample of generated sequences, for the coverage
    /// meta-tests. Each sequence builds a real node, so the count is kept
    /// modest.
    fn sample(count: usize) -> Vec<Vec<LinkAction>> {
        let mut runner = TestRunner::deterministic();
        let strategy = arb_link_sequence(SEQUENCE_LEN);
        (0..count)
            .map(|_| {
                strategy
                    .new_tree(&mut runner)
                    .expect("sequence generation is infallible")
                    .current()
            })
            .collect()
    }

    #[test]
    fn the_case_budget_defaults_and_is_raised_by_the_environment() {
        assert_eq!(cases_from(None), DEFAULT_CASES);
        assert_eq!(cases_from(Some(String::new())), DEFAULT_CASES);
        assert_eq!(cases_from(Some("nonsense".to_string())), DEFAULT_CASES);
        assert_eq!(cases_from(Some("0".to_string())), DEFAULT_CASES);
        assert_eq!(cases_from(Some(" 200000 ".to_string())), 200_000);
    }

    #[test]
    fn the_weight_table_covers_every_action_exactly_once() {
        let mut seen: Vec<Variant> = WEIGHTS.iter().map(|(variant, _)| *variant).collect();
        seen.sort();
        seen.dedup();
        assert_eq!(
            seen.len(),
            WEIGHTS.len(),
            "a variant appears twice in the weight table"
        );
        assert_eq!(
            WEIGHTS.len(),
            15,
            "the weight table must name every LinkAction variant"
        );
        assert!(WEIGHTS.iter().all(|(_, weight)| *weight > 0));
    }

    #[test]
    fn every_weighted_draw_selects_a_variant() {
        let mut hit: Vec<Variant> = (0..TOTAL_WEIGHT).map(variant_for).collect();
        hit.sort();
        hit.dedup();
        assert_eq!(
            hit.len(),
            WEIGHTS.len(),
            "some variant is unreachable from the weighted draw"
        );
    }

    #[test]
    fn the_generator_emits_every_action_variant() {
        let mut seen: Vec<Variant> = sample(24)
            .into_iter()
            .flatten()
            .map(|action| action.variant())
            .collect();
        seen.sort();
        seen.dedup();
        let missing: Vec<Variant> = WEIGHTS
            .iter()
            .map(|(variant, _)| *variant)
            .filter(|variant| !seen.contains(variant))
            .collect();
        assert!(missing.is_empty(), "never generated: {missing:?}");
    }

    #[test]
    fn the_generator_mixes_accepted_and_rejected_actions() {
        let mut accepted = 0usize;
        let mut rejected = 0usize;
        for actions in sample(12) {
            let mut node = LinkNode::fresh(ChainPolicy::Off);
            for action in &actions {
                match node.apply(action) {
                    Outcome::Accepted => accepted += 1,
                    Outcome::Rejected(_) => rejected += 1,
                }
            }
        }
        assert!(accepted > 0, "nothing was ever accepted");
        assert!(
            rejected > 0,
            "nothing was ever rejected — the out-of-context draw is not reaching the node"
        );
    }

    #[test]
    fn the_generator_reaches_the_states_the_property_is_about() {
        let mut granted_partial = false;
        let mut full_resynced = false;
        let mut promoted = false;
        let mut feed_held = false;
        let mut backlog_evicted = false;
        let mut multi_replica = false;

        for actions in sample(20) {
            let mut node = LinkNode::fresh(ChainPolicy::Off);
            for action in &actions {
                let before = node.tracker.sync_counters();
                let outcome = node.apply(action);
                let after = node.tracker.sync_counters();
                granted_partial |= after.partial_ok > before.partial_ok;
                full_resynced |= after.full > before.full;
                promoted |=
                    matches!(action, LinkAction::Promote) && matches!(outcome, Outcome::Accepted);
                feed_held |= node.feed_gate.is_held();
                backlog_evicted |= matches!(action, LinkAction::EvictBacklog)
                    && matches!(outcome, Outcome::Accepted);
                multi_replica |= node.tracker.get_streaming_replicas().len() > 1;
            }
        }

        assert!(granted_partial, "no +CONTINUE was ever granted");
        assert!(full_resynced, "no +FULLRESYNC was ever served");
        assert!(promoted, "the node never took a primary stint");
        assert!(feed_held, "the feed gate was never held");
        assert!(backlog_evicted, "the backlog was never evicted as idle");
        assert!(multi_replica, "two replicas never streamed at once");
    }

    /// Chained replication is off by default, and the entry it would reach is a
    /// documented exception rather than a HARD violation — so turning it on
    /// dirties `check_all` without dirtying `check_hard`.
    #[test]
    fn chained_replication_is_off_by_default_and_only_reaches_the_documented_exception() {
        assert_eq!(
            LinkNode::fresh(ChainPolicy::Off).chains,
            ChainPolicy::Off,
            "the default node must not chain"
        );

        let mut node = LinkNode::fresh(ChainPolicy::On);
        assert_eq!(
            node.apply(&LinkAction::Demote { upstream: true }),
            Outcome::Accepted
        );
        assert_eq!(
            node.apply(&LinkAction::Attach {
                slot: 0,
                announce: Announce::Port,
            }),
            Outcome::Accepted
        );
        assert_eq!(
            node.apply(&LinkAction::Psync {
                slot: 0,
                id: PsyncId::Unknown,
                offset: AckOffset::Zero,
            }),
            Outcome::Accepted
        );

        let view = node.view();
        assert!(
            invariants::check_hard(&view).is_empty(),
            "a sub-replica is not a HARD violation: {}",
            invariants::render(&invariants::check_hard(&view))
        );
        let all = invariants::check_all(&view);
        assert!(
            all.iter().any(|violation| violation.id == "INV-ROLE-1"),
            "a streaming session under a replica role must reach INV-ROLE-1"
        );

        // And the same sequence is unreachable with the flag off.
        let mut off = LinkNode::fresh(ChainPolicy::Off);
        assert_eq!(
            off.apply(&LinkAction::Demote { upstream: true }),
            Outcome::Accepted
        );
        assert!(matches!(
            off.apply(&LinkAction::Attach {
                slot: 0,
                announce: Announce::Port,
            }),
            Outcome::Rejected(_)
        ));
    }

    // -- Pinned defects -----------------------------------------------------
    //
    // One witness per muzzle arm. Each drives the shape the generator is
    // forbidden from emitting and asserts the catalog entry it violates; a fix
    // turns the witness red, which is the point.

    /// `.scratch/replication-correctness/issues/open/21-ack-above-live-head.md`
    #[test]
    #[should_panic(expected = "INV-OFFSET-3")]
    fn pinned_issue_21_an_ack_above_the_live_head_is_admitted() {
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        node.apply(&LinkAction::Attach {
            slot: 0,
            announce: Announce::Port,
        });
        node.apply(&LinkAction::Psync {
            slot: 0,
            id: PsyncId::Unknown,
            offset: AckOffset::Zero,
        });
        node.apply(&LinkAction::Write {
            key: 1,
            value_len: 4,
        });
        // The seam's own hook fires: `ingest_replica_ack` credits the session
        // with an offset this primary never wrote.
        node.apply(&LinkAction::Ack {
            slot: 0,
            offset: AckOffset::Ahead(64),
        });
    }

    /// `.scratch/replication-correctness/issues/open/22-duplicate-streaming-identity.md`
    #[test]
    #[should_panic(expected = "INV-SESSION-2")]
    fn pinned_issue_22_a_reconnect_streams_beside_the_session_it_replaces() {
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        node.apply(&LinkAction::Attach {
            slot: 0,
            announce: Announce::Port,
        });
        node.apply(&LinkAction::Psync {
            slot: 0,
            id: PsyncId::Unknown,
            offset: AckOffset::Zero,
        });
        // The peer's link dies; this node has not noticed.
        node.apply(&LinkAction::Detach {
            slot: 0,
            departure: Departure::Silent,
        });
        node.apply(&LinkAction::Attach {
            slot: 0,
            announce: Announce::Port,
        });
        node.apply(&LinkAction::Psync {
            slot: 0,
            id: PsyncId::Unknown,
            offset: AckOffset::Zero,
        });
        // Nothing kicked the predecessor, so both sessions are streaming on one
        // announced identity. The next registry-wide seam is where it surfaces.
        node.apply(&LinkAction::Write {
            key: 1,
            value_len: 4,
        });
        node.apply(&LinkAction::Ack {
            slot: 0,
            offset: AckOffset::Live,
        });
    }

    /// The muzzle is a statement about two filed defects, not a licence to skip
    /// whatever looks like them. Each near-miss below differs from a pinned
    /// shape in exactly one respect and must stay in the property.
    #[test]
    fn the_muzzle_only_covers_the_pinned_shapes() {
        // Issue 20 — an ack that is *not* above the live head is generated.
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        node.apply(&LinkAction::Attach {
            slot: 0,
            announce: Announce::Port,
        });
        node.apply(&LinkAction::Psync {
            slot: 0,
            id: PsyncId::Unknown,
            offset: AckOffset::Zero,
        });
        node.apply(&LinkAction::Write {
            key: 1,
            value_len: 4,
        });
        for offset in [AckOffset::Live, AckOffset::Zero, AckOffset::Behind(2)] {
            assert!(
                known_defect(&node, &LinkAction::Ack { slot: 0, offset }).is_none(),
                "{offset:?} is at or below the live head and must not be muzzled"
            );
        }
        assert!(
            known_defect(
                &node,
                &LinkAction::Ack {
                    slot: 0,
                    offset: AckOffset::Ahead(1),
                }
            )
            .is_some(),
            "the pinned issue-21 shape must be muzzled"
        );
        // An overshooting ack against an *empty* slot is a rejection, not a
        // defect, so it stays in the property.
        assert!(
            known_defect(
                &node,
                &LinkAction::Ack {
                    slot: 1,
                    offset: AckOffset::Ahead(1),
                }
            )
            .is_none(),
            "an ack with no session behind it is not the pinned shape"
        );

        // Issue 21 — the first PSYNC on an identity, and a second session whose
        // identity is unannounced, are both generated.
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        node.apply(&LinkAction::Attach {
            slot: 0,
            announce: Announce::Port,
        });
        let first_psync = LinkAction::Psync {
            slot: 0,
            id: PsyncId::Unknown,
            offset: AckOffset::Zero,
        };
        assert!(
            known_defect(&node, &first_psync).is_none(),
            "the only session on an identity must not be muzzled"
        );
        node.apply(&first_psync);
        node.apply(&LinkAction::Detach {
            slot: 0,
            departure: Departure::Silent,
        });
        node.apply(&LinkAction::Attach {
            slot: 0,
            announce: Announce::Port,
        });
        assert!(
            known_defect(&node, &first_psync).is_some(),
            "the pinned issue-22 shape must be muzzled"
        );
        // Retire the predecessor and the same reconnect is legal again.
        node.apply(&LinkAction::Detach {
            slot: 0,
            departure: Departure::Lost,
        });
        assert!(
            known_defect(&node, &first_psync).is_none(),
            "with the predecessor gone the reconnect is not the pinned shape"
        );

        // An unannounced pair is two *unknown* sessions, which INV-SESSION-2
        // does not compare — so it is not the pinned shape either.
        let mut node = LinkNode::fresh(ChainPolicy::Off);
        node.apply(&LinkAction::Attach {
            slot: 1,
            announce: Announce::Silent,
        });
        node.apply(&LinkAction::Psync {
            slot: 1,
            id: PsyncId::Unknown,
            offset: AckOffset::Zero,
        });
        node.apply(&LinkAction::Detach {
            slot: 1,
            departure: Departure::Silent,
        });
        node.apply(&LinkAction::Attach {
            slot: 1,
            announce: Announce::Silent,
        });
        assert!(
            known_defect(
                &node,
                &LinkAction::Psync {
                    slot: 1,
                    id: PsyncId::Unknown,
                    offset: AckOffset::Zero,
                }
            )
            .is_none(),
            "unannounced sessions are unknown, not duplicates"
        );
    }
}
