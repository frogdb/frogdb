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
use crate::identity::ReplicationIdentity;
use crate::invariants;
use crate::primary::PrimaryReplicationHandler;
use crate::primary::replay::ReplayDecision;
use crate::replica::offset::{AppliedOffset, ReplicaApplyStint};
use crate::replica_session::{
    Phase, ReplicaAnnouncement, ReplicaCapabilities, ReplicaDeparture, ReplicaSession,
};
use crate::state::{
    ReplicationState, StagedReplicationMetadata, consume_staged_replication_metadata,
    read_staged_replication_metadata,
};
use crate::sync_counters::SyncOutcome;
use crate::tracker::ReplicationTrackerImpl;
use crate::view::{ReplicationView, RoleView};
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
                .then_some("issue 20: an ACK above the live head is admitted unclamped")
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
                .then_some("issue 21: a reconnect streams beside the session it replaces")
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

    /// `.scratch/replication-correctness/issues/open/20-ack-above-live-head.md`
    #[test]
    #[should_panic(expected = "INV-OFFSET-3")]
    fn pinned_issue_20_an_ack_above_the_live_head_is_admitted() {
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

    /// `.scratch/replication-correctness/issues/open/21-duplicate-streaming-identity.md`
    #[test]
    #[should_panic(expected = "INV-SESSION-2")]
    fn pinned_issue_21_a_reconnect_streams_beside_the_session_it_replaces() {
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
            "the pinned issue-20 shape must be muzzled"
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
            "the pinned issue-21 shape must be muzzled"
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
