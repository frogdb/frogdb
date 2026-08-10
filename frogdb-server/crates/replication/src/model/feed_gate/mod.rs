//! Explicit-state model of the slot-handoff replica-feed hold (`stateright`).
//!
//! # What this checks
//!
//! A slot-handoff write barrier holds this node's whole replica feed for the
//! barrier window (FM-CLUSTER-097). Three parties move independently around
//! that hold and none of them is ordered against the others: the barriers
//! themselves arm, re-arm, overlap and release; the clock advances, which is
//! the *only* thing that ends a lapsed hold; and each streaming session polls
//! the gate, sleeps out what it finds there, and ships what it buffered. The
//! hold is correct only if no interleaving of those ever puts a frame on the
//! wire while a barrier is armed, leaves a waiter asleep against a deadline
//! that no longer exists, or leaves the feed held with nothing left that can
//! free it.
//!
//! Unit tests can only assert the interleavings someone wrote down —
//! [`crate::feed_gate`]'s own tests pin eleven of them. This enumerates all of
//! them inside a small scope.
//!
//! # The transition function is production code
//!
//! The model never re-implements the gate. Every transition that touches the
//! hold calls the production decision functions and applies exactly what they
//! answer:
//!
//! * [`decide_feed_hold_until`] derives the value to publish from the armed
//!   barriers — the rule that makes a release belonging to an *ended* barrier
//!   leave the feed held to a later one's deadline;
//! * [`decide_publish`] decides whether a republish stores and wakes, or does
//!   nothing (the pause state republishes on every mutation of itself, so the
//!   no-op arm is the common case);
//! * [`decide_hold`] decides whether a published deadline is still in force at
//!   an instant — read by every session before it ships and every time it wakes.
//!
//! An edit to any of the three changes this model's behaviour with no edit
//! here. The `Instant`s the three take are the model's own discrete ticks
//! offset from one origin, so "the clock advanced" is a state transition rather
//! than a race to be waited out.
//!
//! The model layer contributes only what the *callers* contribute in
//! production, and each piece is a transcription of one caller's control flow:
//!
//! * the pause state's entry fold (`PauseEntry::arm` in `frogdb-core`: never
//!   shorten against a live entry, replace a lapsed one) and its sweep, with
//!   the derived value republished on every mutation
//!   (`ClientRegistry::publish_pause_derived_state`);
//! * `ReplicaFeedGate::released`'s loop — read the hold, register for the
//!   wakeup, re-read, then sleep until *either* the notification or the
//!   deadline;
//! * the streaming session's write task (`ReplicaSession::start_streaming`):
//!   buffer frames while the feed is held, ship them once it is free, and
//!   consult the gate again before the next batch;
//! * `notify_waiters` semantics — a store wakes every session registered at
//!   that moment, and nobody else.
//!
//! Two deliberate elisions, both latency-only. `released()`'s register-then-
//! re-read is one step here rather than two: the alternative order loses a
//! notification, but a lost notification only makes a waiter sleep to the
//! deadline it already had, which is late rather than wrong (and
//! `an_explicit_release_wakes_a_waiter_early` pins the order directly). And a
//! session's buffer is one bit — "has frames to ship" — because frame *order*
//! through a hold is the session's property, not the gate's.
//!
//! # The pre-fix tree is a scope, not a revert
//!
//! [`Scope::honour_the_gate`] is an assumption the callers keep, withdrawn in
//! one small scope: with it `false` the sessions ship without consulting the
//! gate, which is exactly the tree before `8d55cc4f`. That scope produces a
//! counterexample to `no_frame_ships_while_a_barrier_is_armed` in well under a
//! second — the level-3 evidence for the replication-correctness PRD's
//! retro-validation revert (d), obtained without a reverted tree.
//! [`replay`] re-runs the counterexample against the real
//! [`ReplicaFeedGate`](crate::feed_gate::ReplicaFeedGate), with no
//! `stateright` in the loop, and shows the gate refusing what the model shipped.
//!
//! # Scope
//!
//! Up to two slot barriers (`PauseState::slots` is keyed by slot, so two
//! concurrent handoffs are two entries), up to three sessions, a discrete clock
//! bounded by [`Scope::horizon`], and barrier lifetimes drawn from
//! [`Scope::ttls`]. An arm is only offered when its deadline lands inside the
//! horizon: a barrier that cannot lapse before the clock bound would make the
//! liveness property unfalsifiable for a reason that is about the bound rather
//! than about the gate. Production numbers (a ≤100 ms barrier against a 10 s
//! lease) do not matter to any of the three decisions — they are comparisons
//! against a stored `Instant` — so small tick counts lose nothing and keep the
//! clock from multiplying the state space.
//!
//! # The properties have teeth
//!
//! Three edits to the production decisions, each caught by the smoke config in
//! a tenth of a second: `decide_feed_hold_until`'s `max` as a `min` (a
//! composed hold that follows the *shorter* barrier) falsifies
//! `no_frame_ships_while_a_barrier_is_armed`; `decide_hold`'s strict `<` as
//! `<=` falsifies `a_hold_in_force_is_future_and_bounded`; and a
//! `decide_publish` that treats a shortened deadline as no change falsifies
//! `the_gate_agrees_with_the_pause_state`. The last two are exactly the
//! surviving-mutant shapes `cargo mutants` proposes for this file.
//!
//! # Exploration budget
//!
//! Measured on a 10-core M-series laptop, BFS to exhaustion (every config
//! terminates; none is depth- or time-truncated). The floors are asserted by
//! the tests themselves (`MIN_*_STATES`), so a scope change that quietly
//! shrinks the explored space fails the build rather than passing vacuously.
//!
//! | config | scope | unique states | depth | wall | where |
//! |---|---|---|---|---|---|
//! | [`smoke_scope`] | 2 barriers, 2 sessions, horizon 7 | 20,438 | 20 | 0.1s | default suite |
//! | [`unheld_feed_scope`] | pre-`8d55cc4f`; 1 barrier, 1 session, horizon 3 | 44 | 9 | <0.1s | default suite |
//! | [`overlapping_scope`] | 3 barriers, 3 sessions, horizon 12 | 3,942,370 | 41 | 12.9s | nightly |
//! | [`churn_scope`] | 2 barriers, 3 sessions, 2 role changes, horizon 15 | 2,649,370 | 44 | 6.9s | nightly |

use std::time::{Duration, Instant};

use frogdb_types::clock;
use stateright::{Model, Property};

use crate::feed_gate::{FeedGatePublish, decide_feed_hold_until, decide_hold, decide_publish};

mod replay;
mod tests;

/// How much of the space to explore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Scope {
    /// Slot barriers that may be armed at once — `PauseState::slots` entries,
    /// one per slot being handed off.
    pub barriers: usize,
    /// Streaming replica sessions consulting the gate.
    pub sessions: usize,
    /// Ticks the clock may advance through. Every armed deadline lands inside
    /// it (see the [module docs](self)).
    pub horizon: u8,
    /// Barrier lifetimes an arm may choose from — the `timeout_ms` the handoff
    /// passes to `pause_slot`.
    pub ttls: &'static [u8],
    /// Primary stints that may end and begin again along a path. Each one
    /// disconnects every session; the node-wide gate outlives them.
    pub max_role_changes: u8,
    /// Whether the sessions consult the gate at all. `false` is the tree
    /// before `8d55cc4f` — see the [module docs](self).
    pub honour_the_gate: bool,
}

impl Scope {
    /// The longest hold a single arm can establish, which is also the bound
    /// every composed hold respects: the fold never extends past the latest
    /// deadline any one arm asked for.
    fn max_ttl(&self) -> u8 {
        self.ttls
            .iter()
            .copied()
            .max()
            .expect("a scope needs a ttl")
    }
}

/// Where a session is with respect to the hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Phase {
    /// Outside `released()`: it will consult the gate before it ships.
    Awake,
    /// Inside `released()`, registered for the wakeup and sleeping until
    /// either the notification or `deadline`.
    Asleep { deadline: u8, notified: bool },
}

/// One streaming session's write task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Session {
    /// Frames taken off the broadcast and not yet written to the wire. One bit
    /// rather than a queue — see the [module docs](self).
    pub buffered: bool,
    pub phase: Phase,
}

impl Session {
    /// A freshly attached session: `start_streaming` has a backlog tail to
    /// replay and consults the gate before it does.
    fn attached() -> Self {
        Self {
            buffered: true,
            phase: Phase::Awake,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Action {
    /// A slot handoff arms (or re-arms) barrier `b` for `ttl` ticks:
    /// `ClientRegistry::pause_slot`, which folds the entry and republishes.
    Arm(usize, u8),
    /// The handoff completes or aborts: `unpause_slot` drops the entry and
    /// republishes.
    Release(usize),
    /// `sweep_lapsed_pauses`: drop every lapsed entry, then republish.
    Sweep,
    /// The node applies a write and broadcasts the frame; every session
    /// buffers it.
    Write,
    /// Session `s` finds the feed held and sleeps it out (`released()`).
    Wait(usize),
    /// A publish's `notify_waiters` reaches session `s`.
    Wake(usize),
    /// Session `s`'s `sleep_until(deadline)` fires.
    Fire(usize),
    /// Session `s` writes its buffered frames to the wire.
    Ship(usize),
    /// The clock advances one tick.
    Tick,
    /// The primary stint ends and begins again: every session is disconnected
    /// and re-attaches against the same node-wide gate.
    RoleChange,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Sys {
    pub now: u8,
    /// Per barrier, the deadline its pause entry carries, or `None` when the
    /// slot has no entry. Mirrors `PauseState::slots`, lapsed-but-unswept
    /// entries included.
    pub armed: Vec<Option<u8>>,
    /// The gate's cell: what `publish` last stored.
    pub published: Option<u8>,
    pub sessions: Vec<Session>,
    pub role_changes: u8,
    /// Latched: a session put frames on the wire while a barrier was armed.
    pub shipped_under_barrier: bool,
    /// Latched: a publish answered `Unchanged` to a strictly later deadline,
    /// leaving the gate behind the pause state.
    pub dropped_a_later_deadline: bool,
    /// Latched: a publish replaced a live hold with an earlier one — what a
    /// release does while another barrier is still armed.
    pub hold_shortened: bool,
    /// Latched: a stint ended while a hold was in force.
    pub role_change_under_barrier: bool,
}

pub(crate) struct FeedGate {
    pub scope: Scope,
    /// The instant tick 0 stands for. Fixed for the life of the model, so two
    /// ticks are always comparable.
    origin: Instant,
}

impl FeedGate {
    pub(crate) fn new(scope: Scope) -> Self {
        assert!(scope.barriers > 0, "a model with no barrier holds nothing");
        assert!(scope.sessions > 0, "a model with no session ships nothing");
        assert!(
            scope.ttls.iter().all(|&t| t > 0),
            "a zero-tick barrier is lapsed the instant it is armed"
        );
        assert!(
            scope.max_ttl() <= scope.horizon,
            "every armed barrier must be able to lapse inside the horizon"
        );
        Self {
            scope,
            origin: clock::now(),
        }
    }

    /// The instant tick `t` stands for.
    fn at(&self, tick: u8) -> Instant {
        self.origin + Duration::from_millis(u64::from(tick))
    }

    /// The tick an instant this model produced stands for.
    fn tick_of(&self, at: Instant) -> u8 {
        u8::try_from((at - self.origin).as_millis()).expect("model instants are bounded by horizon")
    }

    /// The hold in force right now, per production [`decide_hold`].
    fn hold(&self, sys: &Sys) -> Option<u8> {
        decide_hold(sys.published.map(|d| self.at(d)), self.at(sys.now)).map(|d| self.tick_of(d))
    }

    /// The hold the *pause state* justifies right now — what a republish would
    /// put in the cell, judged at the current instant.
    fn justified_hold(&self, sys: &Sys) -> Option<u8> {
        let derived = decide_feed_hold_until(sys.armed.iter().flatten().map(|&d| self.at(d)));
        decide_hold(derived, self.at(sys.now)).map(|d| self.tick_of(d))
    }

    /// Whether any barrier is armed and in force — the fact a shipped frame is
    /// judged against, read off the pause state rather than off the gate so
    /// the two cannot agree by construction.
    fn barrier_active(&self, sys: &Sys) -> bool {
        sys.armed.iter().flatten().any(|&d| sys.now < d)
    }

    /// Republish the derived hold, exactly as
    /// `ClientRegistry::publish_pause_derived_state` does on every mutation of
    /// the pause state.
    fn publish(&self, sys: &mut Sys) {
        let next = decide_feed_hold_until(sys.armed.iter().flatten().map(|&d| self.at(d)));
        let current = sys.published.map(|d| self.at(d));
        match decide_publish(current, next) {
            FeedGatePublish::Unchanged => {
                if next.is_some_and(|n| current.is_none_or(|c| c < n)) {
                    sys.dropped_a_later_deadline = true;
                }
            }
            FeedGatePublish::Store { held_until } => {
                let stored = held_until.map(|d| self.tick_of(d));
                if let (Some(before), Some(after)) = (sys.published, stored)
                    && after < before
                {
                    sys.hold_shortened = true;
                }
                sys.published = stored;
                // `notify_waiters`: every session registered at this moment,
                // and nobody else.
                for session in sys.sessions.iter_mut() {
                    if let Phase::Asleep { notified, .. } = &mut session.phase {
                        *notified = true;
                    }
                }
            }
        }
    }

    /// A session whose `sleep_until` is due. The clock may not advance past
    /// one: a timer fires at its deadline, so a state where time has run on
    /// with a due sleeper still asleep is not a state production reaches.
    fn due(&self, sys: &Sys, s: usize) -> bool {
        matches!(sys.sessions[s].phase, Phase::Asleep { deadline, .. } if sys.now >= deadline)
    }

    /// Nothing is held and nothing is waiting to ship — the state the liveness
    /// property says every path reaches.
    fn drained(&self, sys: &Sys) -> bool {
        self.hold(sys).is_none()
            && sys
                .sessions
                .iter()
                .all(|s| !s.buffered && s.phase == Phase::Awake)
    }
}

impl Model for FeedGate {
    type State = Sys;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Sys {
            now: 0,
            armed: vec![None; self.scope.barriers],
            published: None,
            sessions: vec![
                Session {
                    buffered: false,
                    phase: Phase::Awake,
                };
                self.scope.sessions
            ],
            role_changes: 0,
            shipped_under_barrier: false,
            dropped_a_later_deadline: false,
            hold_shortened: false,
            role_change_under_barrier: false,
        }]
    }

    fn actions(&self, sys: &Self::State, out: &mut Vec<Self::Action>) {
        for b in 0..self.scope.barriers {
            for &ttl in self.scope.ttls {
                // Only arms whose deadline lands inside the horizon: see the
                // module docs on why the bound has to admit the lapse.
                if sys.now + ttl <= self.scope.horizon {
                    out.push(Action::Arm(b, ttl));
                }
            }
            if sys.armed[b].is_some() {
                out.push(Action::Release(b));
            }
        }
        // `sweep_lapsed_pauses` returns without touching anything when nothing
        // has lapsed.
        if sys.armed.iter().flatten().any(|&d| sys.now >= d) {
            out.push(Action::Sweep);
        }
        if sys.sessions.iter().any(|s| !s.buffered) {
            out.push(Action::Write);
        }

        for s in 0..self.scope.sessions {
            let session = sys.sessions[s];
            match session.phase {
                Phase::Awake => {
                    if !session.buffered {
                        continue;
                    }
                    match self.hold(sys) {
                        // The write task's `while feed_gate.is_held()` loop.
                        Some(_) if self.scope.honour_the_gate => out.push(Action::Wait(s)),
                        // The feed is free — or this tree does not ask.
                        _ => out.push(Action::Ship(s)),
                    }
                }
                Phase::Asleep { notified, .. } => {
                    if notified {
                        out.push(Action::Wake(s));
                    }
                    if self.due(sys, s) {
                        out.push(Action::Fire(s));
                    }
                }
            }
        }

        if sys.role_changes < self.scope.max_role_changes {
            out.push(Action::RoleChange);
        }

        if sys.now < self.scope.horizon && !(0..self.scope.sessions).any(|s| self.due(sys, s)) {
            out.push(Action::Tick);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sys = last.clone();
        match action {
            Action::Arm(b, ttl) => {
                let deadline = sys.now + ttl;
                // `PauseEntry::arm`: fold into a live entry (never shorten),
                // replace one that has already lapsed.
                sys.armed[b] = Some(match sys.armed[b] {
                    Some(live) if sys.now < live => live.max(deadline),
                    _ => deadline,
                });
                self.publish(&mut sys);
            }
            Action::Release(b) => {
                sys.armed[b] = None;
                self.publish(&mut sys);
            }
            Action::Sweep => {
                for entry in sys.armed.iter_mut() {
                    *entry = entry.filter(|&d| sys.now < d);
                }
                self.publish(&mut sys);
            }
            Action::Write => {
                for session in sys.sessions.iter_mut() {
                    session.buffered = true;
                }
            }
            Action::Wait(s) => {
                // `released()`: read the hold, register, re-read. Registering
                // before the re-read is what makes a publish landing in the
                // window a wakeup rather than a lost one.
                let deadline = self.hold(&sys)?;
                sys.sessions[s].phase = Phase::Asleep {
                    deadline,
                    notified: false,
                };
            }
            Action::Wake(s) | Action::Fire(s) => {
                // Either way the loop goes round: it re-reads the hold, and
                // finds it released, shortened or still in force.
                sys.sessions[s].phase = Phase::Awake;
            }
            Action::Ship(s) => {
                if !sys.sessions[s].buffered {
                    return None;
                }
                sys.sessions[s].buffered = false;
                sys.shipped_under_barrier |= self.barrier_active(&sys);
            }
            Action::Tick => {
                if sys.now >= self.scope.horizon {
                    return None;
                }
                sys.now += 1;
            }
            Action::RoleChange => {
                sys.role_change_under_barrier |= self.hold(&sys).is_some();
                for session in sys.sessions.iter_mut() {
                    *session = Session::attached();
                }
                sys.role_changes += 1;
            }
        }
        Some(sys)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // (a) The hold's whole point: nothing reaches a replica while a
            // slot-handoff barrier is armed on this node. Judged against the
            // *pause state*, not against the gate, so a gate that has drifted
            // from it cannot satisfy this by agreeing with itself.
            Property::<Self>::always("no_frame_ships_while_a_barrier_is_armed", |_, sys| {
                !sys.shipped_under_barrier
            }),
            // (b) The two halves of the barrier cannot disagree about whether
            // it is up: the hold in force is always the one the armed entries
            // justify. This is what makes a release belonging to an ended
            // barrier leave a later barrier's hold standing — and it is the
            // clause a republish that decided "nothing changed" too eagerly
            // would break.
            Property::<Self>::always("the_gate_agrees_with_the_pause_state", |m, sys| {
                m.hold(sys) == m.justified_hold(sys)
            }),
            // (c) INV-GATE-1: a hold in force lapses in the future, and within
            // the longest barrier budget anyone asked for. A gate that could
            // store a deadline it was never given — or keep one that has
            // passed — wedges the feed, which is strictly worse than the
            // anomaly the hold closes.
            Property::<Self>::always("a_hold_in_force_is_future_and_bounded", |m, sys| {
                m.hold(sys)
                    .is_none_or(|d| d > sys.now && d - sys.now <= m.scope.max_ttl())
            }),
            // (d) No publish is lost: a session asleep against a deadline the
            // gate no longer holds to has a notification pending (or its own
            // timer is already due). Falsified by a store that woke nobody.
            Property::<Self>::always("no_publish_is_lost", |m, sys| {
                sys.sessions.iter().all(|s| match s.phase {
                    Phase::Awake => true,
                    Phase::Asleep { deadline, notified } => {
                        notified || sys.now >= deadline || m.hold(sys) == Some(deadline)
                    }
                })
            }),
            // (e) `Unchanged` never drops a strictly later deadline — the
            // arm-by-arm statement of (b), latched where it would happen.
            Property::<Self>::always("a_publish_never_drops_a_later_deadline", |_, sys| {
                !sys.dropped_a_later_deadline
            }),
            // (f) Liveness: every hold ends and everything held reaches the
            // wire. The gate carries its own deadline precisely so that no
            // path needs anybody to clear it.
            Property::<Self>::eventually("every_hold_is_eventually_released", |m, sys| {
                m.drained(sys)
            }),
            // Vacuity guards: the scope has to reach the interesting outcomes,
            // or the `always` properties above prove nothing.
            Property::<Self>::sometimes("a_barrier_holds_buffered_frames", |m, sys| {
                m.hold(sys).is_some() && sys.sessions.iter().any(|s| s.buffered)
            }),
            Property::<Self>::sometimes("a_hold_lapses_with_nobody_clearing_it", |_, sys| {
                sys.published.is_some_and(|d| sys.now >= d)
                    && sys.armed.iter().flatten().any(|&d| d <= sys.now)
            }),
            Property::<Self>::sometimes("the_feed_ships_after_a_barrier", |m, sys| {
                sys.published.is_some() && m.hold(sys).is_none() && m.drained(sys)
            }),
        ];

        if self.scope.honour_the_gate {
            props.push(Property::<Self>::sometimes(
                "a_session_sleeps_out_a_hold",
                |_, sys| {
                    sys.sessions
                        .iter()
                        .any(|s| matches!(s.phase, Phase::Asleep { .. }))
                },
            ));
            props.push(Property::<Self>::sometimes(
                "a_publish_wakes_a_sleeping_session",
                |_, sys| {
                    sys.sessions
                        .iter()
                        .any(|s| matches!(s.phase, Phase::Asleep { notified: true, .. }))
                },
            ));
        }

        if self.scope.barriers > 1 {
            props.push(Property::<Self>::sometimes(
                "two_barriers_hold_the_feed_at_once",
                |_, sys| sys.armed.iter().flatten().filter(|&&d| sys.now < d).count() > 1,
            ));
            props.push(Property::<Self>::sometimes(
                "a_release_shortens_a_live_hold",
                |_, sys| sys.hold_shortened,
            ));
        }

        if self.scope.max_role_changes > 0 {
            props.push(Property::<Self>::sometimes(
                "a_role_change_lands_inside_a_barrier_window",
                |_, sys| sys.role_change_under_barrier,
            ));
        }

        props
    }
}

/// Bounded configuration for the default test suite: two barriers that can
/// overlap, two sessions, and a horizon wide enough for one barrier to lapse
/// after another one released. Everything the properties need to be
/// non-vacuous, and nothing that only makes the same shapes longer.
pub(crate) fn smoke_scope() -> Scope {
    Scope {
        barriers: 2,
        sessions: 2,
        horizon: 7,
        ttls: &[1, 2, 3],
        max_role_changes: 0,
        honour_the_gate: true,
    }
}

/// Full budget, depth: three barriers and three sessions over a long horizon —
/// a barrier can be armed, extended, released and re-armed inside one hold
/// while one session sleeps against the old deadline and another is already
/// shipping what an earlier window held.
pub(crate) fn overlapping_scope() -> Scope {
    Scope {
        barriers: 3,
        sessions: 3,
        horizon: 12,
        ttls: &[1, 2, 3],
        max_role_changes: 0,
        honour_the_gate: true,
    }
}

/// Full budget, breadth: session churn — three sessions and two primary stints
/// that end and begin again mid-window, so the sessions that consult the gate
/// after a role change are never the ones that were asleep against it, and the
/// node-wide gate outlives both stints.
pub(crate) fn churn_scope() -> Scope {
    Scope {
        barriers: 2,
        sessions: 3,
        horizon: 15,
        ttls: &[1, 2, 3],
        max_role_changes: 2,
        honour_the_gate: true,
    }
}

/// The tree before `8d55cc4f`: the sessions ship without consulting the gate.
/// The smallest scope that puts a frame on the wire inside a barrier window —
/// small enough that the counterexample path is short enough to transcribe
/// into [`replay`] by hand.
pub(crate) fn unheld_feed_scope() -> Scope {
    Scope {
        barriers: 1,
        sessions: 1,
        horizon: 3,
        ttls: &[2],
        max_role_changes: 0,
        honour_the_gate: false,
    }
}
