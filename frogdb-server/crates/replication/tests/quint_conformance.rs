//! Quint-connect-style conformance harness: replays traces from
//! `specs/quint/replication_feed_gate.qnt` through the real
//! [`frogdb_replication::FeedSequencer`], so the feed-gate model and the seam are
//! checked against each other rather than only against themselves.
//!
//! Campaign task T9d (`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`,
//! wave W3). The seam is the issue-26 option-1 extraction (`src/feed_sequencer.rs`,
//! commit `e31fee92`); the model is the phase-3 feed-gate/barrier session model
//! (`f7234770`), whose header carries the projection table this file implements.
//!
//! # Driver target
//!
//! [`FeedSequencer::step`] is the driver target: the crate's public, pure
//! `(state, input) -> action` decision function. Nothing was widened for this harness —
//! `FeedSequencer`, `FeedInput` and `FeedAction` are already re-exported from the crate
//! root, and the only other read this file takes is the public
//! [`FeedSequencer::held_frames`].
//!
//! # Trace source: generated at test time, no fixtures
//!
//! Same convention as `frogdb-cluster/tests/quint_conformance.rs`: every trace is produced
//! by invoking the `quint` CLI during the test, never checked in. A checked-in fixture goes
//! stale silently — it keeps passing against a model that has moved on, which is exactly the
//! failure mode that left the cluster harness projecting a superseded state shape. Requiring
//! the CLI means a model edit is replayed against the code on the next `cargo test`, and a
//! deleted/renamed `run` fails loudly (`[QNT404] Name not found`). `quint` comes from
//! `.mise.toml` (`npm:@informalsystems/quint`) locally and from the `mise-action` install
//! list in CI's `unit-tests` job; a missing binary is a hard failure with an actionable
//! message, never a silent skip.
//!
//! The flip side of reading the model live: these tests read the *working tree*, so a model
//! mutation battery (`.scratch/formal-spec/*-battery.md`) running against
//! `specs/quint/replication_feed_gate*.qnt` in the same checkout turns them red while a
//! mutant is applied. That is the harness working — a model that no longer describes the
//! seam should fail here — but it is worth knowing before chasing a phantom regression.
//!
//! Deterministic model `run`s are replayed with
//! `quint run <spec> --init <run> --max-steps 0 --mbt` — a named `run` is an action, so
//! `--init` replays exactly the chain `quint test` would, and `--mbt` is what emits the
//! `mbt::actionTaken` variable this harness decodes (`quint test` has no `--mbt`; see the
//! cluster harness header for the full reasoning). The sampled lane additionally uses
//! `--step step --n-traces N --seed <fixed>`.
//!
//! # How a step is decoded: the model's own state delta, never the seam's
//!
//! quint's `--mbt` metadata names the action (`mbt::actionTaken`) but carries **no
//! arguments** for the deterministic `*As` variants a `run` chain calls: `mbt::nondetPicks`
//! is all-`None` in a `--init <run>` trace (verified against every run below). So the
//! arguments a `FeedInput` needs are recovered from the *model's* state delta:
//!
//! | model action | which session | argument | recovered from |
//! |---|---|---|---|
//! | `handoffReplayed(As)` | the one session whose record changed | `resume_offset` | post-state `sessions[s].resume_offset` |
//! | `receiveFrame` | ditto | frame sequence | post-state `sessions[s].cursor` (asserted `= pre + 1`) |
//! | `consultGate` | ditto | `GateHeld(b)` | **pre-state** `armed` — `b = BARRIERS.exists(armed[b] != None)`, the model's own `gateHeld` |
//! | `confirmSent(As)` | ditto | `lag_breached` | post-state `sessions[s].cause == Some(LagBreach)` |
//! | `observeRelease` / `sourceClosed` / `sourceLagged` / `sendFailed` | ditto | (none) | — |
//!
//! Every one of those is a **model** variable read out of the ITF trace. Nothing is
//! recomputed from the sequencer's own state, and nothing is hardcoded per test: there is no
//! script of expected actions, so a rewritten `run` body replays as its new body instead of
//! failing an assertion about a name. This is the specific hole the cluster harness's header
//! warns about (arguments laundered from code state, and silent returns that let a replay
//! pass vacuously) — here, an action name this file does not map is a `panic!`, a
//! session-targeting action whose delta does not identify exactly one session is a `panic!`,
//! and an unmappable model stage is a `panic!`.
//!
//! # What is asserted after every step (for every session, not just the stepped one)
//!
//! `FeedSequencer`'s `Stage` is private by design — the driver reacts to the returned
//! [`FeedAction`], never to a stage. So stage agreement is asserted through the action, which
//! is a total function of the stage the sequencer just entered:
//!
//! ```text
//!   model stage   seam action          also asserted
//!   Handoff    -> AwaitRelease          (from `start()`, before any input)
//!   Receiving  -> Receive
//!   Consulting -> ConsultGate
//!   Holding    -> ReceiveOrRelease
//!   Sending    -> Send(f)               f == model `in_flight`
//!   Ended      -> End(d)                d == model `ending` (Graceful | Lost)
//! ```
//!
//! plus `held_frames() == model held.len()` — which is what makes the FM-REPLICATION-015
//! replay dedup observable from outside (a frame at or below `resume_offset` must not grow
//! the buffer) and what catches a drain that ships the wrong number of frames.
//!
//! # Model state that is deliberately not projected
//!
//! Only fields with no counterpart in this seam, each named here so the exclusion is a
//! decision rather than an oversight:
//!
//!   - `sessions[s].identity`, `stream_seq` — the session registry's dedup key and arrival
//!     order (issue 22). `FeedSequencer` has no identity: supersession reaches it as a
//!     dropped connection, not as a `FeedInput` (see below).
//!   - `sessions[s].acked` — the ACK watermark lives in the tracker
//!     (`tracker.rs`/`offset_coordinator.rs`), not in the feed sequencer. The two model
//!     actions that write it (`replicaAck`, `replicaAckAbove`) are asserted to leave every
//!     projected field of every session untouched, which is the seam-side claim: an ACK is
//!     not a feed decision.
//!   - `sessions[s].sent` / `accepted` — model ghost histories. The seam keeps no history;
//!     the observable consequence (which frame is on the wire now) is `in_flight`, which
//!     *is* projected, and ordering is the model's own `inv_wire_is_a_prefix_of_accepted`.
//!   - `live`, `source_closed`, `armed`, `departure_record`, `ignored_acks`, `next_seq`,
//!     `defects`, `coverage` — environment and node-wide bookkeeping outside one session's
//!     sequencer. `armed` is not unprojectable and is in fact *read*: it is where the
//!     `GateHeld` answer comes from, exactly as the real driver reads
//!     `feed_gate::decide_hold`.
//!
//! # `supersede` is the one model action with no `FeedInput`
//!
//! [`frogdb_replication::ReplicaDeparture`] has two variants, `Graceful` and `Lost`; there is
//! no `Superseded`. That is not an omission — the issue-22 kick is the *session registry*
//! displacing a connection, and the sequencer of the displaced session is dropped rather than
//! stepped. The harness models it that way: on `supersede` it asserts the model's older
//! session reached `Ended`/`Superseded`/`Kicked`, marks its sequencer killed-out-of-band, and
//! from then on asserts only that the model keeps that session `Ended`/`Superseded` — the
//! sequencer is gone, so there is nothing left to compare. Any *other* route to a `Superseded`
//! model state is a hard failure (`expected_action`), so this carve-out cannot widen.
//!
//! # Model-disabled pairs are never exercised
//!
//! `FeedSequencer::step`'s table is total: every `(stage, input)` pair the explicit arms do
//! not cover falls through to `_ => end(Lost)` (driver robustness, not a spec obligation —
//! the model's guards simply disable those pairs, per the model header and
//! `.scratch/formal-spec/t9b-blocked.md`). A trace only ever contains transitions the model
//! *took*, so replaying it can never reach that fallback; `feed` additionally refuses to
//! push an input into a session the seam has already ended, so a future model change that
//! enabled such a pair would fail here loudly instead of being absorbed by the fallback.
//!
//! # Guard runs: the model's `.fail()` steps
//!
//! Several of the mutation battery's gap-closure runs are *pure guard* tests — their point is
//! that an action is refused, not that a state moves (`.then(armBarrier(1).fail())`,
//! `.then(writeFrame.fail())`, `.then(handoffReplayedAs(1, 1, Some(7)).fail())`). quint records
//! such a step as an ITF state carrying `mbt::actionTaken` and **no model variables at all**,
//! because the guard held and there is no successor state to write down.
//!
//! The replay treats that shape, and only that shape, as a refusal: nothing is fed (there is no
//! post-state to recover an argument from), the step is tallied in `Exercised::refused` so the
//! test can require that the run actually reached its refusal, and the step is asserted to be
//! the trace's last — quint only permits `.fail()` terminally, so a variable-free state anywhere
//! else means the projection stopped matching the model and stays a hard failure. A state with
//! *some* variables but not others is likewise a hard failure, never a silent refusal.
//!
//! # FM-CLUSTER-097 carve-out (pending ruling)
//!
//! A link that ends inside an armed barrier window drains its held buffer past the barrier
//! floor without re-consulting the gate — `feed_sequencer.rs:221-230` does it, and the model
//! reproduces it faithfully with a disclosed carve-out in
//! `inv_no_ship_inside_barrier_window`. Seam and model agree, so this harness conforms to
//! both; `close_inside_window_drains_then_ends_graceful` replays the exact sequence. If the
//! ruling in `.scratch/formal-spec/t9b-blocked.md` lands on "the seam is wrong", the model
//! changes first and this harness turns red on the seam — which is the point.
//!
//! # Acceptance demonstration (campaign W3: "the revert test")
//!
//! Recorded here because a harness that cannot fail is worth nothing. Both mutations below
//! were applied to `src/feed_sequencer.rs` as scratch edits, observed red, and reverted
//! byte-for-byte (sha256 of the restored file compared against a pre-mutation copy, and
//! `git diff --stat -- frogdb-server/crates/replication` clean afterwards):
//!
//!   1. **Drain while the gate is held** — `(Stage::Consulting, FeedInput::GateHeld(true))`
//!      returns `self.flush()` instead of parking in `Stage::Holding`. **7 of 13 tests
//!      red** (`hold_then_drain`, `barrier_while_held`, `handoff_waits_for_release`,
//!      `close_inside_window_drains_then_ends_graceful`, `node_wide_hold`,
//!      `send_failed_abandons_buffer`, `sampled_traces`), e.g.
//!
//!      ```text
//!      assertion `left == right` failed: nodeWideHoldTest step 6 (after `consultGate`):
//!      session 1: seam action disagrees with the model stage Holding
//!        left: Send(1)
//!       right: ReceiveOrRelease
//!      ```
//!
//!   2. **Skip the gate consultation after a frame arrives** — `(Stage::Receiving |
//!      Stage::Holding, FeedInput::Received(frame))` buffers and then calls `self.flush()`
//!      instead of returning `FeedAction::ConsultGate`, i.e. it deletes the d-ii rule that
//!      every path to the wire passes through a gate read. **8 of 13 tests red**
//!      (`hold_then_drain`, `handoff_dedup`, `barrier_while_held`,
//!      `close_inside_window_drains_then_ends_graceful`, `lag_breach_ends_at_the_frame`,
//!      `send_failed_abandons_buffer`, `node_wide_hold`, `sampled_traces`), e.g.
//!
//!      ```text
//!      assertion `left == right` failed: holdThenDrainTest step 4 (after `receiveFrame`):
//!      session 1: seam action disagrees with the model stage Consulting
//!        left: Send(1)
//!       right: ConsultGate
//!      ```
//!
//! Note what catches them: not a hand-written expectation in this file, but the model's own
//! post-state stage. Both mutations survive a harness that only replays inputs and checks
//! that nothing panics.

use bytes::Bytes;
use frogdb_replication::frame::ReplicationFrame;
use frogdb_replication::{FeedAction, FeedInput, FeedSequencer, ReplicaDeparture};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

// ---------------------------------------------------------------------------
// Mirror types for the model's state (`specs/quint/replication_feed_gate_types.qnt`).
// ---------------------------------------------------------------------------

/// Quint's `Option[T]` on the wire: adjacently tagged, unit `None`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum QOpt<T> {
    Some(T),
    None,
}

impl<T> QOpt<T> {
    fn as_option(&self) -> Option<&T> {
        match self {
            QOpt::Some(v) => Option::Some(v),
            QOpt::None => Option::None,
        }
    }

    fn is_none(&self) -> bool {
        matches!(self, QOpt::None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum StageQ {
    Handoff,
    Receiving,
    Consulting,
    Holding,
    Sending,
    Ended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum DepartureQ {
    Graceful,
    Lost,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum CauseQ {
    ClosedByPeer,
    ReceiverLagged,
    SendFailure,
    LagBreach,
    Kicked,
}

/// The projected half of the model's `SessionState`. Unprojected fields (`identity`,
/// `stream_seq`, `acked`, `sent`, `accepted`) are listed in the module header with the
/// reason each has no counterpart in this seam; serde ignores them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct SessionQ {
    stage: StageQ,
    resume_offset: i64,
    held: Vec<i64>,
    in_flight: QOpt<i64>,
    ending: QOpt<DepartureQ>,
    cause: QOpt<CauseQ>,
    cursor: i64,
}

/// One ITF state: the model variables this harness reads, plus the action that produced it.
///
/// The variables are `Option` for one reason only, and it is not tolerance for a drifted
/// projection: quint records a terminal `.fail()` step as a state carrying `mbt::actionTaken`
/// and *nothing else* — the action was refused, so there is no successor state to write down.
/// [`FeedStateQ::refused`] is the only place that shape is accepted, and it rejects a state
/// that carries some variables but not others.
#[derive(Debug, Clone, Deserialize)]
struct FeedStateQ {
    sessions: Option<BTreeMap<i64, SessionQ>>,
    /// `Some(floor)` = armed. The node-wide gate is held iff any entry is armed — the
    /// model's `gateHeld`, and the answer the real driver gets from `decide_hold`.
    armed: Option<BTreeMap<i64, QOpt<i64>>>,
    #[serde(rename = "mbt::actionTaken")]
    action: String,
}

impl FeedStateQ {
    /// Whether this state is quint's record of a refused action (a `.fail()` step): every
    /// model variable absent. A state that carries some but not all of them is a projection
    /// that stopped matching the model, and is a hard failure rather than a silent refusal.
    fn refused(&self) -> bool {
        match (self.sessions.is_none(), self.armed.is_none()) {
            (true, true) => true,
            (false, false) => false,
            _ => panic!(
                "model state after `{}` carries some model variables but not others; only a \
                 refused (`.fail()`) step is variable-free, so this is a projection that no \
                 longer matches the model — fix the projection, do not widen it",
                self.action
            ),
        }
    }

    fn sessions(&self) -> &BTreeMap<i64, SessionQ> {
        assert!(
            !self.refused(),
            "model state after `{}` is a refused step; it has no sessions to read",
            self.action
        );
        self.sessions.as_ref().expect("checked by `refused`")
    }

    fn gate_held(&self) -> bool {
        assert!(
            !self.refused(),
            "model state after `{}` is a refused step; it has no barriers to read",
            self.action
        );
        self.armed
            .as_ref()
            .expect("checked by `refused`")
            .values()
            .any(|slot| !slot.is_none())
    }

    fn session(&self, id: i64) -> &SessionQ {
        self.sessions()
            .get(&id)
            .unwrap_or_else(|| panic!("model trace has no session {id}"))
    }
}

// ---------------------------------------------------------------------------
// Trace generation — the `quint` CLI, at test time.
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("resolve repo root from CARGO_MANIFEST_DIR")
}

fn spec_path() -> PathBuf {
    repo_root().join("specs/quint/replication_feed_gate.qnt")
}

/// Run `quint` with the given arguments, or fail with everything needed to diagnose it.
fn run_quint(args: &[&str]) {
    let output = Command::new("quint").args(args).output();
    let output = match output {
        Ok(output) => output,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => panic!(
            "`quint` is not on PATH, so no model trace could be generated.\n\
             This harness replays live model traces on purpose (see the module header): a \
             skip here would silently stop checking the seam against the model.\n\
             Install it with the repo toolchain: `mise install npm:@informalsystems/quint` \
             (pinned in .mise.toml), then re-run.\n\
             Attempted: quint {}",
            args.join(" ")
        ),
        Err(e) => panic!("failed to spawn `quint {}`: {e}", args.join(" ")),
    };
    assert!(
        output.status.success(),
        "`quint {}` failed ({}).\n--- stdout ---\n{}\n--- stderr ---\n{}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn read_trace(path: &Path) -> Vec<FeedStateQ> {
    let json = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read ITF trace {}: {e}", path.display()));
    let trace: itf::Trace<FeedStateQ> = itf::trace_from_str(&json).unwrap_or_else(|e| {
        panic!(
            "deserialize ITF trace {}: {e}\n\
             A projection field that no longer exists in the model is the usual cause — \
             fix the projection, do not widen it with defaults.",
            path.display()
        )
    });
    trace.states.into_iter().map(|state| state.value).collect()
}

/// The trace of one named model `run`, replayed exactly as `quint test` would run it.
fn model_run(name: &str) -> Vec<FeedStateQ> {
    let dir = tempfile::tempdir().expect("tempdir for the ITF trace");
    let out = dir.path().join("trace.itf.json");
    run_quint(&[
        "run",
        spec_path().to_str().expect("spec path is utf-8"),
        "--init",
        name,
        "--max-steps",
        "0",
        "--max-samples",
        "1",
        "--mbt",
        "--out-itf",
        out.to_str().expect("out path is utf-8"),
    ]);
    read_trace(&out)
}

/// Sampled traces of the model's own `step` relation.
fn sampled_runs(seed: &str, traces: usize, max_steps: usize) -> Vec<Vec<FeedStateQ>> {
    let dir = tempfile::tempdir().expect("tempdir for the ITF traces");
    let pattern = dir.path().join("sim_{seq}.itf.json");
    let traces_s = traces.to_string();
    let steps_s = max_steps.to_string();
    run_quint(&[
        "run",
        spec_path().to_str().expect("spec path is utf-8"),
        "--max-samples",
        &traces_s,
        "--n-traces",
        &traces_s,
        "--max-steps",
        &steps_s,
        "--seed",
        seed,
        "--mbt",
        "--out-itf",
        pattern.to_str().expect("out pattern is utf-8"),
    ]);

    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.path())
        .expect("list generated traces")
        .map(|entry| entry.expect("read dir entry").path())
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "`quint run --n-traces {traces}` produced no ITF files"
    );
    files.iter().map(|path| read_trace(path)).collect()
}

// ---------------------------------------------------------------------------
// The seam side: one `FeedSequencer` per model session, plus the last action it returned.
// ---------------------------------------------------------------------------

/// The seam's answer, reduced to what the model can express. `Send` keeps the frame's
/// sequence (compared against the model's `in_flight`) and `End` the departure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Observed {
    AwaitRelease,
    Receive,
    ReceiveOrRelease,
    ConsultGate,
    Send(u64),
    End(ReplicaDeparture),
}

fn observe(action: &FeedAction) -> Observed {
    match action {
        FeedAction::AwaitRelease => Observed::AwaitRelease,
        FeedAction::Receive => Observed::Receive,
        FeedAction::ReceiveOrRelease => Observed::ReceiveOrRelease,
        FeedAction::ConsultGate => Observed::ConsultGate,
        FeedAction::Send(frame) => Observed::Send(frame.sequence),
        FeedAction::End(departure) => Observed::End(*departure),
    }
}

/// What the model says the seam must have answered, read purely from the model's post-state.
fn expected_action(ss: &SessionQ) -> Observed {
    match ss.stage {
        StageQ::Handoff => Observed::AwaitRelease,
        StageQ::Receiving => Observed::Receive,
        StageQ::Consulting => Observed::ConsultGate,
        StageQ::Holding => Observed::ReceiveOrRelease,
        StageQ::Sending => {
            let offset = ss
                .in_flight
                .as_option()
                .copied()
                .expect("model is Sending with no in-flight frame");
            Observed::Send(u64::try_from(offset).expect("model offsets are non-negative"))
        }
        StageQ::Ended => match ss.ending.as_option() {
            Some(DepartureQ::Graceful) => Observed::End(ReplicaDeparture::Graceful),
            Some(DepartureQ::Lost) => Observed::End(ReplicaDeparture::Lost),
            // Reachable only through `supersede`, which is handled out of band before this
            // is ever consulted (module header). Anything else is an unmappable model state.
            Some(DepartureQ::Superseded) => panic!(
                "model session ended `Superseded` without an out-of-band kick: \
                 `ReplicaDeparture` has no such variant, so this state is unprojectable"
            ),
            Option::None => panic!("model session is Ended with no departure classified"),
        },
    }
}

struct SeamSession {
    sequencer: FeedSequencer,
    last: Observed,
    /// The session registry displaced this connection (`supersede`). Its sequencer is gone,
    /// which is what the real driver does; nothing about it is comparable afterwards.
    kicked: bool,
}

fn frame(sequence: i64) -> ReplicationFrame {
    ReplicationFrame::new(
        u64::try_from(sequence).expect("model offsets are non-negative"),
        Bytes::from_static(b"quint-conformance"),
    )
}

/// Tallies of what the replay actually exercised, so a lane cannot pass vacuously.
#[derive(Debug, Default, PartialEq, Eq)]
struct Exercised {
    inputs: usize,
    consult_gate: usize,
    held: usize,
    sends: usize,
    ends: usize,
    kicks: usize,
    /// Steps the model *refused* (`.fail()`): the guard held, so no `FeedInput` exists and
    /// nothing moved. Counted so a pure-guard run can assert it actually reached its refusal
    /// instead of passing on an empty replay.
    refused: usize,
}

// ---------------------------------------------------------------------------
// The replay.
// ---------------------------------------------------------------------------

/// Environment actions: no `FeedInput`, and no session may move.
const ENV_ACTIONS: [&str; 6] = [
    "writeFrame",
    "closeSource",
    "armBarrier",
    "releaseBarrier",
    "lapseBarrier",
    "stutter",
];

/// ACK ingest: not a feed decision, so no `FeedInput` and no projected field may move.
const ACK_ACTIONS: [&str; 3] = ["replicaAck", "replicaAckAbove", "replicaAckAs"];

struct Replay<'a> {
    label: &'a str,
    sessions: BTreeMap<i64, SeamSession>,
    exercised: Exercised,
}

impl<'a> Replay<'a> {
    fn new(label: &'a str, initial: &FeedStateQ) -> Self {
        let sessions = initial
            .sessions()
            .keys()
            .map(|&id| {
                let sequencer = FeedSequencer::new();
                // The barrier wait is the machine's first demand, before a single frame is
                // pulled out of the backlog — the handoff-lane half of FM-CLUSTER-097.
                let last = observe(&sequencer.start());
                (
                    id,
                    SeamSession {
                        sequencer,
                        last,
                        kicked: false,
                    },
                )
            })
            .collect();
        Replay {
            label,
            sessions,
            exercised: Exercised::default(),
        }
    }

    fn seam(&mut self, id: i64) -> &mut SeamSession {
        self.sessions
            .get_mut(&id)
            .unwrap_or_else(|| panic!("model targets session {id}, which the trace never declared"))
    }

    /// Feed one input to one session's real sequencer and record what it answered.
    fn feed(&mut self, step: usize, id: i64, input: FeedInput) {
        let label = self.label;
        let seam = self.seam(id);
        assert!(
            !seam.kicked,
            "{label} step {step}: the model fed an input to session {id}, which the registry \
             already displaced out of band"
        );
        assert!(
            !matches!(seam.last, Observed::End(_)),
            "{label} step {step}: the model fed {input:?} to session {id} after the seam \
             already ended the link. Replaying it would exercise `FeedSequencer::step`'s \
             `_ => end(Lost)` fallback, which the model does not represent — see the module \
             header."
        );
        let action = seam.sequencer.step(input);
        seam.last = observe(&action);
        let observed = seam.last;
        self.exercised.inputs += 1;
        match observed {
            Observed::ConsultGate => self.exercised.consult_gate += 1,
            Observed::ReceiveOrRelease => self.exercised.held += 1,
            Observed::Send(_) => self.exercised.sends += 1,
            Observed::End(_) => self.exercised.ends += 1,
            _ => {}
        }
    }

    /// The single session whose projected record differs between `pre` and `post`.
    fn changed_session(&self, step: usize, pre: &FeedStateQ, post: &FeedStateQ) -> i64 {
        let changed: Vec<i64> = post
            .sessions()
            .iter()
            .filter(|(id, after)| pre.session(**id) != *after)
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(
            changed.len(),
            1,
            "{} step {step} ({}): expected exactly one session to move, {changed:?} did",
            self.label,
            post.action,
        );
        changed[0]
    }

    fn assert_no_session_moved(&self, step: usize, pre: &FeedStateQ, post: &FeedStateQ) {
        for (id, after) in post.sessions() {
            assert_eq!(
                pre.session(*id),
                after,
                "{} step {step}: `{}` moved session {id}, but it has no `FeedInput`",
                self.label,
                post.action,
            );
        }
    }

    /// Every session's seam answer and held-buffer depth, against the model's post-state.
    fn check(&self, step: usize, state: &FeedStateQ) {
        for (id, model) in state.sessions() {
            let seam = &self.sessions[id];
            if seam.kicked {
                assert_eq!(
                    (model.stage, model.ending.as_option().copied()),
                    (StageQ::Ended, Some(DepartureQ::Superseded)),
                    "{} step {step}: session {id} was displaced out of band, so the model \
                     must keep it Ended/Superseded",
                    self.label,
                );
                continue;
            }
            assert_eq!(
                seam.last,
                expected_action(model),
                "{} step {step} (after `{}`): session {id}: seam action disagrees with the \
                 model stage {:?}",
                self.label,
                state.action,
                model.stage,
            );
            assert_eq!(
                seam.sequencer.held_frames(),
                model.held.len(),
                "{} step {step} (after `{}`): session {id}: held-buffer depth disagrees \
                 (model holds {:?})",
                self.label,
                state.action,
                model.held,
            );
        }
    }

    fn replay(mut self, states: &[FeedStateQ]) -> Exercised {
        assert!(
            states.len() >= 2,
            "{}: trace has {} state(s); nothing to replay",
            self.label,
            states.len()
        );
        self.check(0, &states[0]);

        for step in 1..states.len() {
            let pre = &states[step - 1];
            let post = &states[step];
            let action = post.action.as_str();

            // A guard-only run's terminal `.then(<action>.fail())`: quint writes the action
            // name and no model variables, because the guard refused it and there is no
            // successor state. There is nothing to feed — the model already made the
            // assertion — but the step is counted so a pure-guard test can require it. quint
            // only permits `.fail()` as the last step of a run, which is asserted here so a
            // variable-free state anywhere else stays a hard failure.
            if post.refused() {
                assert_eq!(
                    step,
                    states.len() - 1,
                    "{}: step {step} (`{action}`) recorded no model state but is not the last \
                     step; only a terminal `.fail()` may be variable-free",
                    self.label,
                );
                self.exercised.refused += 1;
                continue;
            }

            if ENV_ACTIONS.contains(&action) || ACK_ACTIONS.contains(&action) {
                self.assert_no_session_moved(step, pre, post);
                self.check(step, post);
                continue;
            }

            let id = self.changed_session(step, pre, post);
            let before = pre.session(id);
            let after = post.session(id);

            match action {
                "handoffReplayed" | "handoffReplayedAs" => {
                    assert_eq!(
                        before.stage,
                        StageQ::Handoff,
                        "{} step {step}: handoff replay from a session past the handoff lane",
                        self.label
                    );
                    let resume =
                        u64::try_from(after.resume_offset).expect("offsets are non-negative");
                    self.feed(
                        step,
                        id,
                        FeedInput::HandoffReplayed {
                            resume_offset: resume,
                        },
                    );
                }
                "receiveFrame" => {
                    assert_eq!(
                        after.cursor,
                        before.cursor + 1,
                        "{} step {step}: `receiveFrame` did not advance the cursor by one",
                        self.label
                    );
                    self.feed(step, id, FeedInput::Received(frame(after.cursor)));
                }
                "consultGate" => {
                    // The gate answer is the *pre*-state's node-wide `armed` fold — the
                    // model's own `gateHeld`, i.e. what the driver's `decide_hold` read
                    // would have returned. Deriving it from the outcome instead would make
                    // the assertion that follows circular.
                    self.feed(step, id, FeedInput::GateHeld(pre.gate_held()));
                }
                "observeRelease" => self.feed(step, id, FeedInput::Released),
                "sourceClosed" => self.feed(step, id, FeedInput::SourceClosed),
                "sourceLagged" => self.feed(step, id, FeedInput::SourceLagged),
                "sendFailed" => self.feed(step, id, FeedInput::SendFailed),
                "confirmSent" | "confirmSentAs" => {
                    // `applySent` is the only writer of `LagBreach`, and it writes it exactly
                    // when the transport reported the breach.
                    let breached = after.cause.as_option() == Some(&CauseQ::LagBreach);
                    if breached {
                        assert_eq!(
                            after.stage,
                            StageQ::Ended,
                            "{} step {step}: a lag breach must end the link",
                            self.label
                        );
                    }
                    self.feed(
                        step,
                        id,
                        FeedInput::Sent {
                            lag_breached: breached,
                        },
                    );
                }
                "supersede" => {
                    // No `FeedInput` exists: the registry drops the displaced connection.
                    assert_eq!(
                        (
                            after.stage,
                            after.ending.as_option().copied(),
                            after.cause.as_option().copied()
                        ),
                        (
                            StageQ::Ended,
                            Some(DepartureQ::Superseded),
                            Some(CauseQ::Kicked)
                        ),
                        "{} step {step}: `supersede` must end the older session \
                         Superseded/Kicked",
                        self.label
                    );
                    self.seam(id).kicked = true;
                    self.exercised.kicks += 1;
                }
                other => panic!(
                    "{} step {step}: model action `{other}` has no mapping in this harness. \
                     Add one (with its `FeedInput`, or with the reason it has none) rather \
                     than letting the step pass unexercised.",
                    self.label
                ),
            }

            self.check(step, post);
        }

        self.exercised
    }
}

fn replay_run(name: &str) -> Exercised {
    let states = model_run(name);
    Replay::new(name, &states[0]).replay(&states)
}

// ---------------------------------------------------------------------------
// The model's named runs, each replayed through the real sequencer.
// ---------------------------------------------------------------------------

/// Every `run` in the model, and the test below that replays it. `run_test_coverage`
/// asserts this list is exactly the model's — a new model run that nothing replays is a
/// hole, and a deleted one must not linger here.
const REPLAYED_RUNS: [&str; 23] = [
    "holdThenDrainTest",
    "barrierWhileHeldTest",
    "laggedOverrunDisconnectTest",
    "handoffDedupTest",
    "handoffWaitsForReleaseTest",
    "closeInsideWindowDrainsThenEndsGracefulTest",
    "supersededKickWritesNoDepartureTest",
    "ackAboveLiveIsIgnoredTest",
    "sendFailedAbandonsBufferTest",
    "lagBreachEndsAtTheFrameTest",
    "nodeWideHoldTest",
    // Gap-closure runs added by the T9c feed-gate mutation battery
    // (`.scratch/formal-spec/2026-08-19-feed-gate-battery.md`, commit `2b1f6c90`).
    "closeAndLagGuardsRefuseAHealthySessionTest",
    "channelOverrunRefusesIntakeTest",
    "midSendSessionIsNotClassifiedOutFromUnderItsFrameTest",
    "supersessionAndAckGuardsTest",
    "enteringStreamingClearsTheFenceCellTest",
    "gracefulCloseRecordsItsDepartureTest",
    "barrierFloorIsTheLiveOffsetAtArmTimeTest",
    "emptyBufferHoldLatchesNoCoverageTest",
    "drainAtTheFloorIsNotADivergenceTest",
    "ackWatermarkNeverRetreatsTest",
    "closedSourceAcceptsNoMoreWritesTest",
    "handoffCannotReplayPastTheLiveHeadTest",
];

/// A frame that arrives while a barrier is armed waits, and reaches the wire only once the
/// barrier releases and the session re-reads the gate (TR-REPLICATION-013, FM-CLUSTER-097).
#[test]
fn hold_then_drain() {
    let exercised = replay_run("holdThenDrainTest");
    assert!(exercised.held > 0 && exercised.sends > 0, "{exercised:?}");
}

/// A release from one arm does not open the gate while another is still armed, and the drain
/// is in offset order.
#[test]
fn barrier_while_held() {
    let exercised = replay_run("barrierWhileHeldTest");
    assert!(exercised.sends >= 2, "{exercised:?}");
}

/// The only overrun that disconnects is the upstream broadcast channel's, and it is `Lost`.
#[test]
fn lagged_overrun_disconnect() {
    let exercised = replay_run("laggedOverrunDisconnectTest");
    assert_eq!(exercised.ends, 1, "{exercised:?}");
}

/// The granted replay's overlap is sent exactly once: the live stream's redelivery is
/// dropped rather than buffered (FM-REPLICATION-015), which shows up here as a held-buffer
/// depth that does not grow.
#[test]
fn handoff_dedup() {
    let exercised = replay_run("handoffDedupTest");
    assert!(exercised.consult_gate >= 3, "{exercised:?}");
}

/// A handoff cannot replay into an armed window; it waits the barrier out.
#[test]
fn handoff_waits_for_release() {
    replay_run("handoffWaitsForReleaseTest");
}

/// The disclosed FM-CLUSTER-097 divergence, pinned on both sides: a close inside an armed
/// window drains the held buffer past the barrier floor before reporting `Graceful`. Seam and
/// model agree today — see the module header and `.scratch/formal-spec/t9b-blocked.md`.
#[test]
fn close_inside_window_drains_then_ends_graceful() {
    let exercised = replay_run("closeInsideWindowDrainsThenEndsGracefulTest");
    assert!(exercised.sends > 0 && exercised.ends == 1, "{exercised:?}");
}

/// Issue 22: the kick departs `Superseded` and writes nothing. No `FeedInput` exists for it,
/// so the harness asserts the model's classification and drops the sequencer.
#[test]
fn superseded_kick_writes_no_departure() {
    let exercised = replay_run("supersededKickWritesNoDepartureTest");
    assert_eq!(exercised.kicks, 1, "{exercised:?}");
}

/// Issue 21: an ACK above the live offset is refused and counted. Asserted here as the
/// seam-side claim that ACK ingest is not a feed decision — no projected field moves.
#[test]
fn ack_above_live_is_ignored() {
    let exercised = replay_run("ackAboveLiveIsIgnoredTest");
    assert_eq!(exercised.ends, 0, "{exercised:?}");
}

/// A failed send abandons the frame and the buffer behind it, and the link is `Lost`.
#[test]
fn send_failed_abandons_buffer() {
    let exercised = replay_run("sendFailedAbandonsBufferTest");
    assert_eq!(exercised.ends, 1, "{exercised:?}");
}

/// A delivery that breaches the lag budget counts as sent and ends the link at that frame.
#[test]
fn lag_breach_ends_at_the_frame() {
    let exercised = replay_run("lagBreachEndsAtTheFrameTest");
    assert_eq!(exercised.ends, 1, "{exercised:?}");
}

/// The hold is node-wide, not per-session: one barrier parks every session the node serves.
#[test]
fn node_wide_hold() {
    let exercised = replay_run("nodeWideHoldTest");
    assert!(exercised.held >= 2, "{exercised:?}");
}

// ---------------------------------------------------------------------------
// Gap-closure runs (T9c mutation battery, `.scratch/formal-spec/2026-08-19-feed-gate-battery.md`,
// commit `2b1f6c90`). The model's own comments name the mutation rows each one kills; what is
// added here is the seam half — every one of them replays through the real `FeedSequencer`, so
// a fact the battery pinned model-side is also pinned against the code.
// ---------------------------------------------------------------------------

/// G09/G10/G12: a clean close is not a session's own decision, is unavailable while frames it
/// has not consumed are still in the channel, and a `Lost` disconnect is unavailable to a
/// session nobody overran. The session drains to the head and only then is the close honest —
/// which is the part with a seam trace: one frame all the way to the wire, no end.
#[test]
fn close_and_lag_guards_refuse_a_healthy_session() {
    let exercised = replay_run("closeAndLagGuardsRefuseAHealthySessionTest");
    assert!(exercised.sends == 1 && exercised.ends == 0, "{exercised:?}");
}

/// G05: once the receiver is further behind than the channel is deep the frames are gone, so
/// the intake is refused and the lagged disconnect is the only honest transition. The seam
/// side is the absence: the backlog grows past `CHANNEL_CAP` and no frame is ever fed.
#[test]
fn channel_overrun_refuses_intake() {
    let exercised = replay_run("channelOverrunRefusesIntakeTest");
    assert!(
        exercised.inputs == 1 && exercised.sends == 0 && exercised.ends == 0,
        "{exercised:?}"
    );
}

/// G11/G14: a session with a frame on the wire is not classified out from under it (the
/// lagged disconnect waits for the send to come back, which is what keeps the in-flight frame
/// from being replaced and lost), and a session that has sent nothing cannot report a failed
/// send. The seam holds the frame in `Send` across three further writes.
#[test]
fn mid_send_session_is_not_classified_out_from_under_its_frame() {
    let exercised = replay_run("midSendSessionIsNotClassifiedOutFromUnderItsFrameTest");
    assert!(exercised.sends == 1 && exercised.ends == 0, "{exercised:?}");
}

/// G16/G19/G22/G23 (issue 22): supersession is newest-wins and only newest, a session that is
/// gone displaces nobody, and a session ACKs only while it is streaming. Every supersession
/// here is refused by a guard, so the kick is never taken — the one thing that does happen is
/// the overrun disconnect.
#[test]
fn supersession_and_ack_guards() {
    let exercised = replay_run("supersessionAndAckGuardsTest");
    assert!(exercised.ends == 1 && exercised.kicks == 0, "{exercised:?}");
}

/// M13, FM-REPLICATION-062's other half: a session entering streaming clears the node's
/// last-departure cell, so a predecessor's `Lost` record does not fence the successor's
/// primary. The successor's handoff is fed to its own sequencer after the first has ended.
#[test]
fn entering_streaming_clears_the_fence_cell() {
    let exercised = replay_run("enteringStreamingClearsTheFenceCellTest");
    assert!(
        exercised.ends == 1 && exercised.inputs >= 3,
        "{exercised:?}"
    );
}

/// M26: the graceful path writes its departure record too — the case with nothing buffered,
/// where the classification and the end are the same step, so the seam ends `Graceful`
/// without ever reaching the wire.
#[test]
fn graceful_close_records_its_departure() {
    let exercised = replay_run("gracefulCloseRecordsItsDepartureTest");
    assert!(exercised.ends == 1 && exercised.sends == 0, "{exercised:?}");
}

/// M01/M02/M03/M08: the floor a barrier records is the live offset at arm time — not zero,
/// not the end of the backlog — and an armed barrier is not re-armed onto a higher floor. The
/// re-arm is the model's terminal `.fail()`, so the seam is never stepped at all.
#[test]
fn barrier_floor_is_the_live_offset_at_arm_time() {
    let exercised = replay_run("barrierFloorIsTheLiveOffsetAtArmTimeTest");
    assert!(
        exercised.refused == 1 && exercised.inputs == 0,
        "{exercised:?}"
    );
}

/// M21/M22/M24: a hold that held nothing back is not `heldFrame`, one session holding is not
/// the node-wide case, and a wakeup that finds an empty buffer drained nothing. The seam
/// parks in `ReceiveOrRelease` on an empty buffer and leaves it without shipping anything —
/// the dedup (FM-REPLICATION-015) is what kept the buffer empty.
#[test]
fn empty_buffer_hold_latches_no_coverage() {
    let exercised = replay_run("emptyBufferHoldLatchesNoCoverageTest");
    assert!(exercised.held == 1 && exercised.sends == 0, "{exercised:?}");
}

/// M34: the FM-CLUSTER-097 carve-out's boundary is strict. A frame already written when the
/// barrier armed sits *at* the floor, so shipping it on the way out is ordinary draining, not
/// a drain past the window — the near-miss twin of
/// [`close_inside_window_drains_then_ends_graceful`], and the seam takes the same path.
#[test]
fn drain_at_the_floor_is_not_a_divergence() {
    let exercised = replay_run("drainAtTheFloorIsNotADivergenceTest");
    assert!(
        exercised.held == 1 && exercised.sends == 1 && exercised.ends == 1,
        "{exercised:?}"
    );
}

/// E33, TR-REPLICATION-033: the acked offset is a high-water mark, not the last number the
/// replica said, so a late or reordered ACK below the watermark leaves it alone — and that is
/// an ordinary ACK, neither ignored nor clamped. Asserted here as the seam-side claim that
/// ACK ingest is not a feed decision: no projected field moves.
#[test]
fn ack_watermark_never_retreats() {
    let exercised = replay_run("ackWatermarkNeverRetreatsTest");
    assert!(
        exercised.inputs == 1 && exercised.ends == 0,
        "{exercised:?}"
    );
}

/// M09: a closed source produces no more frames. Pure environment guard — the backlog refuses
/// to grow after the close, and no session is ever stepped.
#[test]
fn closed_source_accepts_no_more_writes() {
    let exercised = replay_run("closedSourceAcceptsNoMoreWritesTest");
    assert!(
        exercised.refused == 1 && exercised.inputs == 0,
        "{exercised:?}"
    );
}

/// M12: the granted replay cannot claim offsets the primary never wrote. The refusal is the
/// *guard*, not the sampler — so no `FeedInput::HandoffReplayed` is ever built, and the
/// sequencer stays in the handoff lane awaiting a release it will never be offered.
#[test]
fn handoff_cannot_replay_past_the_live_head() {
    let exercised = replay_run("handoffCannotReplayPastTheLiveHeadTest");
    assert!(
        exercised.refused == 1 && exercised.inputs == 0,
        "{exercised:?}"
    );
}

/// Sampled traces of the model's own `step` relation, replayed through the same projection.
///
/// The named runs above pin the canonical sequences; this lane is the interleaving fuzz —
/// two sessions, two barriers, arming and lapsing around a live feed. The seed is fixed so a
/// failure is reproducible and CI is not flaky; sweeping it (or raising the budget) is how
/// this lane is deepened. The `Exercised` floor keeps it from passing vacuously if a model
/// change starves the session actions out of the walk.
#[test]
fn sampled_traces() {
    let traces = sampled_runs("0x9d", 40, 18);
    let mut total = Exercised::default();
    for (index, states) in traces.iter().enumerate() {
        let label = format!("sampled trace {index}");
        let exercised = Replay::new(&label, &states[0]).replay(states);
        total.inputs += exercised.inputs;
        total.consult_gate += exercised.consult_gate;
        total.held += exercised.held;
        total.sends += exercised.sends;
        total.ends += exercised.ends;
        total.kicks += exercised.kicks;
        total.refused += exercised.refused;
    }
    assert!(
        total.inputs >= 80 && total.consult_gate > 0 && total.sends > 0 && total.ends > 0,
        "the sampled lane barely exercised the seam: {total:?}"
    );
}

/// The harness replays *every* `run` the model declares.
///
/// The cluster harness's W0 breakage was exactly this hole in reverse: it named runs the
/// model had deleted, and nothing noticed until the whole binary went red. Checking the list
/// against the spec text makes both directions loud — a new model run with no replay here,
/// and a stale name here with no run there.
#[test]
fn run_test_coverage() {
    let spec = std::fs::read_to_string(spec_path()).expect("read the feed-gate model");
    let declared: Vec<String> = spec
        .lines()
        .filter_map(|line| line.trim().strip_prefix("run "))
        .filter_map(|rest| rest.split(':').next())
        .map(|name| name.trim().to_string())
        .collect();
    assert!(
        !declared.is_empty(),
        "found no `run` declarations in the model"
    );

    let mut missing: Vec<&String> = declared
        .iter()
        .filter(|name| !REPLAYED_RUNS.contains(&name.as_str()))
        .collect();
    missing.sort();
    assert!(
        missing.is_empty(),
        "the model declares runs this harness does not replay: {missing:?}"
    );

    let stale: Vec<&str> = REPLAYED_RUNS
        .iter()
        .filter(|name| !declared.iter().any(|d| d == *name))
        .copied()
        .collect();
    assert!(
        stale.is_empty(),
        "this harness names runs the model no longer declares: {stale:?}"
    );
}
