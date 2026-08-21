//! The decision half of a replica session's lifecycle.
//!
//! [`ReplicaSession::run`] is the I/O half — it writes handshake replies, cuts
//! and ships checkpoints, exports the live keyspace, forwards WAL frames and
//! runs the exit handler. What it *decides* is this module: given the phase a
//! session is on and the plain-data facts around it, what phase does it move
//! to and what has to happen, in what order?
//!
//! Split out for the reason [`plan_primary_stint`] and
//! [`select_psync_arm`] were (`primary/promotion.rs`, `replica/psync.rs`): a
//! decision that reads plain data and returns a plain description of the
//! transition can be unit-tested over its whole input space and driven by a
//! model checker, while the method that owns the socket and the filesystem
//! keeps owning only those.
//!
//! # What this buys beyond testability
//!
//! [`step`] is the **only** decider of [`Phase`]. The session's phase writer used
//! to be called from five places scattered across four methods, and
//! `INV-SESSION-1` — "a session's phase only moves forward in the declared
//! order; `Disconnecting` is terminal" — was prose at the top of
//! `replica_session.rs` checked by nothing but the catalog's debug hook. Here it
//! is structural: every arm of the transition table is visible in one match, the
//! catch-all cannot leave [`Phase::Disconnecting`], and a phase that moved
//! backwards would be a table edit rather than a missed call site. The writer
//! itself (`ReplicaSession::commit_phase`) is now reachable from the driver's
//! one commit point and from the test-only phase forcer, and nowhere else.
//!
//! Two orderings that used to be comments are now data:
//!
//! * a checkpoint directory is owed cleanup **only after** the cut succeeded —
//!   [`Effect::OwnCheckpointDir`] is emitted on the two arms a cut that
//!   returned `Ok` can reach ([`StepOutcome::Ok`] and the coverage-breach
//!   abort) and on no other, so a failed cut cannot leave the exit handler
//!   chasing a directory that was never created, and an abandoned sync cannot
//!   leak the one it staged;
//! * the exit handler records the departure **before** it unregisters the
//!   session (FM-REPLICATION-062), because the self-fence's disarm reads
//!   "nothing is streaming" and "the last departure was graceful" as two
//!   separate loads. That order is the order of the [`Effect`]s
//!   [`SessionEvent::Ended`] returns, so a test can assert it without standing
//!   up a socket.
//!
//! # The transition table
//!
//! ```text
//!                      Begin(Partial)                    ReplySent
//!   Connecting ──────────────────────────► Connecting ──────────────► Streaming
//!        │  [SendReply(+CONTINUE)]                    [ClearDeparture, Stream]
//!        │
//!        │  Begin(Full)                     ReplySent, checkpoint source
//!        ├──────────────────────────────────────────────► PreparingCheckpoint
//!        │  [PublishFunctionSnapshot?,                     [Drain? | CutCheckpoint]
//!        │   SendReply(+FULLRESYNC)]
//!        │
//!        │                                  ReplySent, live-dataset source
//!        └──────────────────────────────────────────────► PreparingCheckpoint
//!                                                          [ExportLiveDataset]
//!
//!   PreparingCheckpoint ──CheckpointCut(Ok)──► StreamingCheckpoint
//!                       ──DatasetExported───►  StreamingCheckpoint
//!                       ──CoverageBreached──►  PreparingCheckpoint
//!                                              [OwnCheckpointDir, FailSync]
//!   StreamingCheckpoint ──PayloadSent───────►  Streaming
//!   <any>               ──Ended────────────►   Disconnecting
//! ```
//!
//! [`ReplicaSession::run`]: crate::ReplicaSession::run
//! [`plan_primary_stint`]: crate::primary::promotion::plan_primary_stint
//! [`select_psync_arm`]: crate::replica::psync::select_psync_arm

use std::path::PathBuf;

use crate::replica_session::{Phase, ReplicaDeparture};

/// Which fork of the handshake a session is driving.
///
/// The streamer needs to know for exactly one reason: `sync_partial_ok` counts
/// partial resyncs that were *served*, and the backlog extract that serves one
/// lives in the streamer, not at the grant (see the accounting note in
/// `PrimaryReplicationHandler::handle_psync`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeSource {
    /// Reached here from a granted `+CONTINUE`.
    PartialGrant,
    /// Reached here from a `+FULLRESYNC` whose payload has already been sent;
    /// that transfer was counted as `sync_full` at the fork.
    FullSnapshot,
}

/// A handshake reply, as data rather than as a `format!` at the socket.
///
/// One spelling of each arm, shared by the session that puts it on the wire and
/// by the promotion model, which used to transcribe both lines by hand
/// (`model/promotion`). A transcription that drifts from the wire is a model
/// checking something the code does not do, so there is one renderer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeReply {
    /// `+CONTINUE <replid>` — the replica keeps the history it has.
    Continue {
        /// The history the replica is being told it is resuming on.
        replication_id: String,
    },
    /// `+FULLRESYNC <replid> <offset>` — a whole dataset follows.
    FullResync {
        /// The history the payload belongs to.
        replication_id: String,
        /// The stream position the payload corresponds to.
        offset: u64,
    },
}

impl HandshakeReply {
    /// The reply line, without a terminator — what a log or a model prints.
    pub fn line(&self) -> String {
        match self {
            HandshakeReply::Continue { replication_id } => format!("+CONTINUE {replication_id}"),
            HandshakeReply::FullResync {
                replication_id,
                offset,
            } => format!("+FULLRESYNC {replication_id} {offset}"),
        }
    }

    /// The exact bytes the primary writes, terminator included.
    pub fn render(&self) -> String {
        format!("{}\r\n", self.line())
    }
}

/// Why a sync was abandoned.
///
/// Every arm drops the link with an `io::Error`; the replica retries
/// `PSYNC ? -1` on its reconnect backoff. None of them stage anything, so none
/// of them owe cleanup — which is why [`Effect::OwnCheckpointDir`] is emitted on
/// the success arm only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncFailure {
    /// A shard could not be drained, so acknowledged writes would be missing
    /// from the checkpoint — and for a full resync that hole is permanent,
    /// because nothing was broadcast to replay them from.
    PreCheckpointDrain,
    /// `create_checkpoint` failed. The replica has already been granted
    /// `snapshot_offset`, and no payload that is not this primary's dataset can
    /// honestly follow it (issue 67).
    Checkpoint,
    /// A primary with neither RocksDB nor a live-keyspace export has no dataset
    /// to serve.
    NoLiveSnapshotSource,
    /// A shard's flush hold lapsed before the cut, so the payload may hold
    /// writes above the coverage watermark the trailer would claim for it. The
    /// vector is the replica's only defence against re-executing the
    /// overshipped range, and there is no sound weaker claim to ship instead,
    /// so the sync is abandoned rather than degraded (FM-REPLICATION-066).
    CoverageHoldBreached,
    /// The session was handed an event its phase cannot answer. Unreachable
    /// from [`crate::ReplicaSession::run`], which is a straight line; kept
    /// because a total table is what makes [`Phase::Disconnecting`] terminal by
    /// construction rather than by inspection.
    UnexpectedEvent,
}

impl SyncFailure {
    /// The reason carried on the `io::Error`; the interpreter appends the
    /// underlying error where there is one.
    pub fn reason(self) -> &'static str {
        match self {
            SyncFailure::PreCheckpointDrain => "pre-checkpoint drain failed for FULLRESYNC",
            SyncFailure::Checkpoint => "failed to create checkpoint for FULLRESYNC",
            SyncFailure::NoLiveSnapshotSource => {
                "no live-snapshot source wired: a primary without persistence cannot serve \
                 a FULLRESYNC"
            }
            SyncFailure::CoverageHoldBreached => {
                "full-sync flush hold lapsed before the checkpoint cut, so the payload's \
                 per-shard coverage claim is unsound"
            }
            SyncFailure::UnexpectedEvent => "replica session event does not apply to its phase",
        }
    }

    /// The operator-facing log line emitted as the sync is abandoned.
    pub fn log_message(self) -> &'static str {
        match self {
            SyncFailure::PreCheckpointDrain => "Pre-checkpoint drain failed for FULLRESYNC",
            SyncFailure::Checkpoint => "Failed to create checkpoint for FULLRESYNC",
            SyncFailure::NoLiveSnapshotSource => "No live-snapshot source wired for FULLRESYNC",
            SyncFailure::CoverageHoldBreached => {
                "Full-sync flush hold lapsed before the checkpoint cut; abandoning the sync"
            }
            SyncFailure::UnexpectedEvent => "Replica session event does not apply to its phase",
        }
    }
}

/// How a step of the sync reported back.
///
/// The failure carries its own message because the interpreter is the only
/// place the underlying error exists, and threading it back out through the
/// machine keeps the interpreter from having to remember anything between
/// effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    /// The step succeeded.
    Ok,
    /// The step failed, with the error rendered by the interpreter.
    Failed(String),
}

/// How the link this session was driving ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkOutcome {
    /// The streaming loop classified the end itself.
    Ended(ReplicaDeparture),
    /// A phase propagated an error out. An error out of *any* phase is a lost
    /// link by construction (FM-REPLICATION-062): the fence's safe direction is
    /// to stay armed.
    Errored,
}

/// What the handshake decided, handed to a session that has just registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginSync {
    /// A granted `+CONTINUE`: the replica resumes from `replay_from`, and the
    /// backlog tail `(replay_from, current]` is replayed before the live tail.
    Partial {
        /// The history the grant names, read off the state at grant time.
        replication_id: String,
        /// The offset the replica already holds.
        replay_from: u64,
    },
    /// A granted `+FULLRESYNC`: a whole dataset at `snapshot_offset`.
    ///
    /// `snapshot_offset` is captured from the live tracker **before** the
    /// checkpoint is cut, so the granted offset can only be at or below the data
    /// the payload carries.
    Full {
        /// The history the payload belongs to.
        replication_id: String,
        /// The stream position the payload corresponds to.
        snapshot_offset: u64,
    },
}

/// Something that happened to a session, driving it one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    /// The session is being driven for the first time. What it is driving is
    /// [`SessionView::sync`], not carried here: the assignment is a fact about
    /// the session that outlives this one event, and every later step reads it
    /// too.
    Begin,
    /// The handshake reply is on the wire.
    ReplySent,
    /// The pre-checkpoint drain reported back.
    Drained(StepOutcome),
    /// `create_checkpoint` reported back.
    CheckpointCut(StepOutcome),
    /// The cut succeeded, but a shard's flush hold lapsed before it: the
    /// payload may hold writes above the coverage its trailer would claim, so
    /// the claim is unsound and the sync is abandoned (FM-REPLICATION-066).
    /// Carries the operator-facing list of shards. Reported *after* a cut that
    /// succeeded, so the staged directory is real and is owed cleanup.
    CoverageBreached(String),
    /// The live-keyspace export produced its blobs.
    DatasetExported,
    /// The full-sync payload is on the wire.
    PayloadSent,
    /// The link ended, however it ended.
    Ended(LinkOutcome),
}

/// One thing the interpreter has to do, in the order the machine returned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Publish the function-library registry onto the frame lane.
    ///
    /// Emitted *before* the `+FULLRESYNC` reply and after the offset capture on
    /// purpose: the frame lands inside `(snapshot_offset, current]`, which the
    /// streaming handoff replays, while one broadcast before the capture would
    /// fall inside the snapshot's own range and be skipped.
    PublishFunctionSnapshot,
    /// Write a handshake reply to the socket.
    SendReply(HandshakeReply),
    /// Drain the shards' flush engines into RocksDB before the cut.
    DrainBeforeCheckpoint,
    /// Cut a RocksDB checkpoint into `path`.
    CutCheckpoint {
        /// Where the checkpoint is staged.
        path: PathBuf,
    },
    /// Remember the checkpoint directory so the exit handler deletes it.
    /// Emitted only after a cut that succeeded.
    OwnCheckpointDir {
        /// The directory now owed cleanup.
        path: PathBuf,
    },
    /// Ship the staged checkpoint directory.
    SendCheckpoint {
        /// The directory to enumerate and stream.
        path: PathBuf,
        /// The history the trailing metadata names.
        replication_id: String,
        /// The stream position the trailing metadata names.
        offset: u64,
    },
    /// Export the live keyspace — the full-sync payload of a primary with no
    /// RocksDB to checkpoint (issue 67).
    ExportLiveDataset,
    /// Ship the exported blobs.
    SendLiveDataset {
        /// The history the trailing metadata names.
        replication_id: String,
        /// The stream position the trailing metadata names.
        offset: u64,
    },
    /// Log the completed full resync.
    LogFullResyncComplete {
        /// The stream position the payload corresponded to.
        offset: u64,
    },
    /// Abandon the sync: log it, and propagate an `io::Error` out of `run`.
    FailSync {
        /// Why the sync was abandoned.
        failure: SyncFailure,
        /// The underlying error, where the interpreter had one.
        cause: Option<String>,
    },
    /// A new streaming generation begins, so the departure recorded by the
    /// previous one stops answering for the replica set (FM-REPLICATION-062).
    ClearDeparture,
    /// Enter the live stream: replay the backlog handoff tail, then forward WAL
    /// frames until the link ends.
    Stream {
        /// The offset the replica already holds.
        replay_from: u64,
        /// Which fork of the handshake got here.
        resume: ResumeSource,
    },
    /// Delete the checkpoint directory this session staged.
    CleanCheckpointDir,
    /// Record how a *streaming* link ended. Emitted only for a session that
    /// actually reached [`Phase::Streaming`]: one that never streamed never
    /// armed the self-fence, so it must not be able to disarm it
    /// (FM-REPLICATION-062).
    RecordDeparture(ReplicaDeparture),
    /// Drop the session from the registry — last, because leaving the registry
    /// is what tells a waiting `shutdown_downstream_sessions` that this session
    /// is done with its per-sync resources.
    Unregister,
    /// Log the disconnect.
    LogDisconnect,
}

/// The plain-data facts [`step`] reads. No locks, no clock, no socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionView {
    /// The phase the session is on — the machine's state.
    pub phase: Phase,
    /// What the handshake assigned this session to drive. Fixed for the
    /// session's whole life.
    pub sync: BeginSync,
    /// This session's registry id; it names the checkpoint directory.
    pub replica_id: u64,
    /// Where checkpoint directories are staged.
    pub data_dir: PathBuf,
    /// A RocksDB checkpoint source is wired (`persistence.enabled`).
    pub checkpoint_source: bool,
    /// A pre-checkpoint drain hook is wired.
    pub pre_checkpoint_drain: bool,
    /// A function-library snapshot hook is wired.
    pub function_snapshot: bool,
    /// A live-keyspace export source is wired.
    pub live_snapshot_source: bool,
    /// A checkpoint directory was staged and is owed cleanup.
    pub checkpoint_owed: bool,
}

impl SessionView {
    /// Where this session stages its checkpoint. The name is derived from the
    /// registry id, so two concurrent full syncs cannot collide.
    fn checkpoint_path(&self) -> PathBuf {
        self.data_dir.join(format!("fullsync_{}", self.replica_id))
    }

    /// The history this session's grant named.
    fn replication_id(&self) -> &str {
        match &self.sync {
            BeginSync::Partial { replication_id, .. } | BeginSync::Full { replication_id, .. } => {
                replication_id
            }
        }
    }

    /// The stream position the full-sync payload corresponds to, and the offset
    /// its handoff replay resumes from. A partial resync has no payload; its
    /// resume point is the offset the replica already holds.
    fn payload_offset(&self) -> u64 {
        match &self.sync {
            BeginSync::Partial { replay_from, .. } => *replay_from,
            BeginSync::Full {
                snapshot_offset, ..
            } => *snapshot_offset,
        }
    }
}

/// What one step of the session does: the phase it lands on, and what the
/// interpreter has to perform, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    /// The phase the session is on after this step. Never earlier in the
    /// declared order than the phase it came from (`INV-SESSION-1`).
    pub phase: Phase,
    /// What to do, in this order. The order is load-bearing on the exit path.
    pub effects: Vec<Effect>,
}

impl Transition {
    fn new(phase: Phase, effects: Vec<Effect>) -> Self {
        Self { phase, effects }
    }
}

/// Decide one step of a replica session: pure over `(view, event)`, performs no
/// I/O, takes no lock and reads no clock.
///
/// The table is total. A `(phase, event)` pair the straight-line interpreter
/// cannot produce keeps the phase it was on and abandons the sync with
/// [`SyncFailure::UnexpectedEvent`] — which is also what makes
/// [`Phase::Disconnecting`] terminal by construction: the only arm that leaves
/// it is the one that does not exist.
pub fn step(view: &SessionView, event: &SessionEvent) -> Transition {
    match (view.phase, event) {
        // ── The handshake's decision goes on the wire ───────────────────────
        //
        // The reply is written before the phase moves off `Connecting`: the
        // transition that answers `Begin` stays on `Connecting` and only
        // `ReplySent` moves off it, so an observer never sees a session past
        // `Connecting` with nothing yet on the wire.
        (Phase::Connecting, SessionEvent::Begin) => Transition::new(Phase::Connecting, begin(view)),

        // ── Past the reply: what the granted arm needs next ─────────────────
        (Phase::Connecting, SessionEvent::ReplySent) => reply_sent(view),

        // ── Staging a full-sync payload ─────────────────────────────────────
        (Phase::PreparingCheckpoint, SessionEvent::Drained(StepOutcome::Failed(cause))) => {
            Transition::new(
                Phase::PreparingCheckpoint,
                vec![Effect::FailSync {
                    failure: SyncFailure::PreCheckpointDrain,
                    cause: Some(cause.clone()),
                }],
            )
        }
        (Phase::PreparingCheckpoint, SessionEvent::Drained(StepOutcome::Ok)) => Transition::new(
            Phase::PreparingCheckpoint,
            vec![Effect::CutCheckpoint {
                path: view.checkpoint_path(),
            }],
        ),
        (Phase::PreparingCheckpoint, SessionEvent::CheckpointCut(StepOutcome::Failed(cause))) => {
            // `OwnCheckpointDir` is deliberately absent: nothing was staged, so
            // the exit handler must not go looking for a directory.
            Transition::new(
                Phase::PreparingCheckpoint,
                vec![Effect::FailSync {
                    failure: SyncFailure::Checkpoint,
                    cause: Some(cause.clone()),
                }],
            )
        }
        (Phase::PreparingCheckpoint, SessionEvent::CoverageBreached(cause)) => {
            // The cut *succeeded* here, so unlike the arm above there is a real
            // directory on disk: it is owned first, so the exit handler deletes
            // it, and only then is the sync abandoned. Shipping this payload
            // would hand the replica either a floor the artefact does not
            // honour (silent write loss) or no floor at all (the D1
            // double-apply, silently, under a verbatim `INCR`) — so it is not
            // shipped at all.
            Transition::new(
                Phase::PreparingCheckpoint,
                vec![
                    Effect::OwnCheckpointDir {
                        path: view.checkpoint_path(),
                    },
                    Effect::FailSync {
                        failure: SyncFailure::CoverageHoldBreached,
                        cause: Some(cause.clone()),
                    },
                ],
            )
        }
        (Phase::PreparingCheckpoint, SessionEvent::CheckpointCut(StepOutcome::Ok)) => {
            let path = view.checkpoint_path();
            Transition::new(
                Phase::StreamingCheckpoint,
                vec![
                    // Owed cleanup *only after* a cut that succeeded.
                    Effect::OwnCheckpointDir { path: path.clone() },
                    Effect::SendCheckpoint {
                        path,
                        replication_id: view.replication_id().to_string(),
                        offset: view.payload_offset(),
                    },
                ],
            )
        }
        (Phase::PreparingCheckpoint, SessionEvent::DatasetExported) => Transition::new(
            Phase::StreamingCheckpoint,
            vec![Effect::SendLiveDataset {
                replication_id: view.replication_id().to_string(),
                offset: view.payload_offset(),
            }],
        ),

        // ── The payload is on the wire ──────────────────────────────────────
        (Phase::StreamingCheckpoint, SessionEvent::PayloadSent) => Transition::new(
            Phase::Streaming,
            vec![
                Effect::LogFullResyncComplete {
                    offset: view.payload_offset(),
                },
                Effect::ClearDeparture,
                // Writes that landed during the cut and the transfer — the
                // handoff window `(snapshot_offset, current]` — are replayed
                // from the backlog before the live tail.
                Effect::Stream {
                    replay_from: view.payload_offset(),
                    resume: ResumeSource::FullSnapshot,
                },
            ],
        ),

        // ── The link ended ──────────────────────────────────────────────────
        (_, SessionEvent::Ended(outcome)) => {
            Transition::new(Phase::Disconnecting, exit(view, *outcome))
        }

        // ── Not reachable from the interpreter ──────────────────────────────
        //
        // The phase is kept, which is what makes `Disconnecting` terminal
        // without an arm of its own: nothing here can leave it.
        _ => Transition::new(
            view.phase,
            vec![Effect::FailSync {
                failure: SyncFailure::UnexpectedEvent,
                cause: None,
            }],
        ),
    }
}

/// The handshake reply, plus — for a full resync — the function-library
/// registry that rides the frame lane ahead of it.
fn begin(view: &SessionView) -> Vec<Effect> {
    match &view.sync {
        BeginSync::Partial { replication_id, .. } => {
            vec![Effect::SendReply(HandshakeReply::Continue {
                replication_id: replication_id.clone(),
            })]
        }
        BeginSync::Full {
            replication_id,
            snapshot_offset,
        } => {
            let mut effects = Vec::with_capacity(2);
            if view.function_snapshot {
                effects.push(Effect::PublishFunctionSnapshot);
            }
            effects.push(Effect::SendReply(HandshakeReply::FullResync {
                replication_id: replication_id.clone(),
                offset: *snapshot_offset,
            }));
            effects
        }
    }
}

/// What follows the reply.
///
/// A `+CONTINUE` needs no payload at all — the replica already holds the data —
/// so it goes straight to the live stream. A `+FULLRESYNC` has to produce one,
/// from RocksDB if there is one and from the live keyspace if there is not
/// (issue 67); a primary with neither has nothing honest to send.
fn reply_sent(view: &SessionView) -> Transition {
    match &view.sync {
        BeginSync::Partial { replay_from, .. } => Transition::new(
            Phase::Streaming,
            vec![
                Effect::ClearDeparture,
                Effect::Stream {
                    replay_from: *replay_from,
                    resume: ResumeSource::PartialGrant,
                },
            ],
        ),
        BeginSync::Full { .. } if view.checkpoint_source => {
            // The checkpoint is a snapshot of what RocksDB *holds*, and a write
            // is acknowledged as soon as it is staged in its shard's WAL
            // flush-engine. Cut without draining those engines, the checkpoint
            // silently omits the primary's most recent writes — and for a full
            // resync that is unrecoverable, because with no replica attached
            // when they were made there is no backlog tail to replay them from.
            // So: drain first, cut second.
            let next = if view.pre_checkpoint_drain {
                Effect::DrainBeforeCheckpoint
            } else {
                Effect::CutCheckpoint {
                    path: view.checkpoint_path(),
                }
            };
            Transition::new(Phase::PreparingCheckpoint, vec![next])
        }
        BeginSync::Full { .. } if view.live_snapshot_source => {
            Transition::new(Phase::PreparingCheckpoint, vec![Effect::ExportLiveDataset])
        }
        // No source wired means no way to read the keyspace, and a full resync
        // with no dataset is precisely the bug: fail the sync instead. The
        // phase never leaves `Connecting`, so nothing was staged and nothing is
        // owed cleanup.
        BeginSync::Full { .. } => Transition::new(
            Phase::Connecting,
            vec![Effect::FailSync {
                failure: SyncFailure::NoLiveSnapshotSource,
                cause: None,
            }],
        ),
    }
}

/// The exit handler, as an ordered list.
///
/// The order is the order `ReplicaSession::run` performed these in, and two
/// steps of it are load-bearing (FM-REPLICATION-062): the departure is recorded
/// **before** the unregistration, because the self-fence's disarm reads
/// "nothing is streaming" and "the last departure was graceful" as two separate
/// loads — unregistering first would open a window in which a predecessor's
/// graceful departure is read as this session's and disarms the fence on a link
/// that actually died. And only a session that reached [`Phase::Streaming`]
/// reports a departure at all: one that never streamed never armed the fence.
fn exit(view: &SessionView, outcome: LinkOutcome) -> Vec<Effect> {
    let mut effects = Vec::with_capacity(4);
    if view.checkpoint_owed {
        effects.push(Effect::CleanCheckpointDir);
    }
    if view.phase == Phase::Streaming {
        effects.push(Effect::RecordDeparture(match outcome {
            LinkOutcome::Ended(departure) => departure,
            LinkOutcome::Errored => ReplicaDeparture::Lost,
        }));
    }
    effects.push(Effect::Unregister);
    effects.push(Effect::LogDisconnect);
    effects
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::psync::{PsyncArm, select_psync_arm};
    use crate::state::hex_id;
    use crate::view::{PhaseChange, ReplicationView};

    /// A fully-wired primary: RocksDB, a drain hook, a function hook, and a
    /// live-keyspace export it will never need.
    fn wired(phase: Phase, sync: BeginSync) -> SessionView {
        SessionView {
            phase,
            sync,
            replica_id: 7,
            data_dir: PathBuf::from("/var/lib/frogdb"),
            checkpoint_source: true,
            pre_checkpoint_drain: true,
            function_snapshot: true,
            live_snapshot_source: true,
            checkpoint_owed: false,
        }
    }

    fn full() -> BeginSync {
        BeginSync::Full {
            replication_id: hex_id('a'),
            snapshot_offset: 4096,
        }
    }

    fn partial() -> BeginSync {
        BeginSync::Partial {
            replication_id: hex_id('b'),
            replay_from: 512,
        }
    }

    // ── The handshake fork ──────────────────────────────────────────────────

    /// A `+CONTINUE` has no payload to stage: the replica already holds the
    /// data, so the reply is followed by the live stream and nothing else, and
    /// the stream resumes from the offset the replica named.
    // FM-REPLICATION-015
    #[test]
    fn a_partial_grant_replies_then_streams_from_the_offset_the_replica_named() {
        let view = wired(Phase::Connecting, partial());

        let begin = step(&view, &SessionEvent::Begin);
        assert_eq!(
            begin.phase,
            Phase::Connecting,
            "the reply precedes the move"
        );
        assert_eq!(
            begin.effects,
            vec![Effect::SendReply(HandshakeReply::Continue {
                replication_id: hex_id('b'),
            })],
            "a partial grant publishes no function registry and cuts no checkpoint"
        );

        let sent = step(&wired(begin.phase, partial()), &SessionEvent::ReplySent);
        assert_eq!(sent.phase, Phase::Streaming);
        assert_eq!(
            sent.effects,
            vec![
                Effect::ClearDeparture,
                Effect::Stream {
                    replay_from: 512,
                    resume: ResumeSource::PartialGrant,
                },
            ]
        );
    }

    /// The function registry rides the frame lane, and it is emitted *before*
    /// the `+FULLRESYNC` reply — i.e. after the offset capture the caller made,
    /// so the frame lands inside the handoff window the streamer replays rather
    /// than inside the snapshot's own range, where it would be skipped.
    // FM-REPLICATION-055
    #[test]
    fn a_full_grant_publishes_the_function_registry_ahead_of_its_reply() {
        let begin = step(&wired(Phase::Connecting, full()), &SessionEvent::Begin);
        assert_eq!(
            begin.effects,
            vec![
                Effect::PublishFunctionSnapshot,
                Effect::SendReply(HandshakeReply::FullResync {
                    replication_id: hex_id('a'),
                    offset: 4096,
                }),
            ]
        );
    }

    /// No hook, no frame — and the reply is unaffected either way.
    // FM-REPLICATION-055
    #[test]
    fn a_full_grant_without_a_function_hook_sends_only_the_reply() {
        let mut view = wired(Phase::Connecting, full());
        view.function_snapshot = false;

        let begin = step(&view, &SessionEvent::Begin);
        assert_eq!(
            begin.effects,
            vec![Effect::SendReply(HandshakeReply::FullResync {
                replication_id: hex_id('a'),
                offset: 4096,
            })]
        );
    }

    // ── Staging the payload ─────────────────────────────────────────────────

    /// Drain first, cut second: a checkpoint cut over undrained flush engines
    /// silently omits acknowledged writes, and for a full resync that hole is
    /// permanent.
    // FM-REPLICATION-001
    #[test]
    fn a_full_resync_drains_before_it_cuts() {
        let after_reply = step(&wired(Phase::Connecting, full()), &SessionEvent::ReplySent);
        assert_eq!(after_reply.phase, Phase::PreparingCheckpoint);
        assert_eq!(after_reply.effects, vec![Effect::DrainBeforeCheckpoint]);

        let drained = step(
            &wired(Phase::PreparingCheckpoint, full()),
            &SessionEvent::Drained(StepOutcome::Ok),
        );
        assert_eq!(drained.phase, Phase::PreparingCheckpoint);
        assert_eq!(
            drained.effects,
            vec![Effect::CutCheckpoint {
                path: PathBuf::from("/var/lib/frogdb/fullsync_7"),
            }],
            "the checkpoint is staged under the session's own registry id"
        );
    }

    /// A primary with nothing staged in front of RocksDB cuts straight away —
    /// there is no drain to wait for.
    // FM-REPLICATION-001
    #[test]
    fn a_full_resync_without_a_drain_hook_cuts_straight_away() {
        let mut view = wired(Phase::Connecting, full());
        view.pre_checkpoint_drain = false;

        let after_reply = step(&view, &SessionEvent::ReplySent);
        assert_eq!(after_reply.phase, Phase::PreparingCheckpoint);
        assert_eq!(
            after_reply.effects,
            vec![Effect::CutCheckpoint {
                path: PathBuf::from("/var/lib/frogdb/fullsync_7"),
            }]
        );
    }

    /// A shard that could not be drained leaves acknowledged writes out of the
    /// checkpoint. The sync is abandoned, carrying the reason the interpreter
    /// read off the error — and nothing was staged, so nothing is owed cleanup.
    // FM-REPLICATION-001
    #[test]
    fn a_failed_drain_abandons_the_sync_and_stages_nothing() {
        let failed = step(
            &wired(Phase::PreparingCheckpoint, full()),
            &SessionEvent::Drained(StepOutcome::Failed("shard 3 timed out".into())),
        );
        assert_eq!(failed.phase, Phase::PreparingCheckpoint);
        assert_eq!(
            failed.effects,
            vec![Effect::FailSync {
                failure: SyncFailure::PreCheckpointDrain,
                cause: Some("shard 3 timed out".into()),
            }],
            "a drain that failed never reached the cut, so it owns no directory"
        );
    }

    /// A cut that failed leaves no directory, so the exit handler must not be
    /// told to delete one.
    // FM-REPLICATION-001
    #[test]
    fn a_failed_cut_owns_no_checkpoint_directory() {
        let failed = step(
            &wired(Phase::PreparingCheckpoint, full()),
            &SessionEvent::CheckpointCut(StepOutcome::Failed("no space left on device".into())),
        );
        assert_eq!(failed.phase, Phase::PreparingCheckpoint);
        assert_eq!(
            failed.effects,
            vec![Effect::FailSync {
                failure: SyncFailure::Checkpoint,
                cause: Some("no space left on device".into()),
            }]
        );
    }

    /// A cut whose flush hold lapsed first produced a real directory but not a
    /// trustworthy claim about it: the sync is abandoned, and — unlike the
    /// failed cut above — the directory that *was* staged is owned first so the
    /// exit handler deletes it. No payload effect is emitted at all: the
    /// alternative the row rejects is shipping the payload with a `0` watermark,
    /// which reads as "no floor" and silently restores the double-apply.
    // FM-REPLICATION-066
    #[test]
    fn a_breached_coverage_hold_abandons_the_sync_and_owns_the_directory() {
        let breached = step(
            &wired(Phase::PreparingCheckpoint, full()),
            &SessionEvent::CoverageBreached("shard(s) [1]".into()),
        );
        assert_eq!(breached.phase, Phase::PreparingCheckpoint);
        assert_eq!(
            breached.effects,
            vec![
                Effect::OwnCheckpointDir {
                    path: PathBuf::from("/var/lib/frogdb/fullsync_7"),
                },
                Effect::FailSync {
                    failure: SyncFailure::CoverageHoldBreached,
                    cause: Some("shard(s) [1]".into()),
                },
            ],
            "the staged directory is owed cleanup and the payload is never sent"
        );
    }

    /// …and a cut that succeeded is claimed for cleanup *before* a single byte
    /// of it goes on the wire, so a link that dies mid-transfer still has its
    /// directory removed.
    // FM-REPLICATION-001
    #[test]
    fn a_cut_that_succeeded_is_owned_before_it_is_shipped() {
        let cut = step(
            &wired(Phase::PreparingCheckpoint, full()),
            &SessionEvent::CheckpointCut(StepOutcome::Ok),
        );
        assert_eq!(cut.phase, Phase::StreamingCheckpoint);
        assert_eq!(
            cut.effects,
            vec![
                Effect::OwnCheckpointDir {
                    path: PathBuf::from("/var/lib/frogdb/fullsync_7"),
                },
                Effect::SendCheckpoint {
                    path: PathBuf::from("/var/lib/frogdb/fullsync_7"),
                    replication_id: hex_id('a'),
                    offset: 4096,
                },
            ]
        );
    }

    /// A primary with `persistence.enabled = false` still owes the replica a
    /// dataset: it serializes the keyspace straight to the socket (issue 67).
    // FM-REPLICATION-001
    #[test]
    fn a_primary_without_rocksdb_exports_the_live_keyspace_instead() {
        let mut view = wired(Phase::Connecting, full());
        view.checkpoint_source = false;

        let after_reply = step(&view, &SessionEvent::ReplySent);
        assert_eq!(after_reply.phase, Phase::PreparingCheckpoint);
        assert_eq!(after_reply.effects, vec![Effect::ExportLiveDataset]);

        let mut exported_from = wired(Phase::PreparingCheckpoint, full());
        exported_from.checkpoint_source = false;
        let exported = step(&exported_from, &SessionEvent::DatasetExported);
        assert_eq!(exported.phase, Phase::StreamingCheckpoint);
        assert_eq!(
            exported.effects,
            vec![Effect::SendLiveDataset {
                replication_id: hex_id('a'),
                offset: 4096,
            }],
            "the live-dataset path stages no directory, so it owns none"
        );
    }

    /// Neither source wired is the bug the diskless path exists to prevent: a
    /// granted `+FULLRESYNC` with no dataset behind it. The phase never leaves
    /// `Connecting`, which is what tells the exit handler nothing was staged.
    // FM-REPLICATION-001
    #[test]
    fn a_primary_with_no_dataset_source_at_all_abandons_the_sync() {
        let mut view = wired(Phase::Connecting, full());
        view.checkpoint_source = false;
        view.live_snapshot_source = false;

        let after_reply = step(&view, &SessionEvent::ReplySent);
        assert_eq!(after_reply.phase, Phase::Connecting);
        assert_eq!(
            after_reply.effects,
            vec![Effect::FailSync {
                failure: SyncFailure::NoLiveSnapshotSource,
                cause: None,
            }]
        );
    }

    /// The offset the reply granted is the offset the payload's trailer names
    /// and the offset the handoff replay resumes from — one number, read three
    /// times, so the replica cannot be handed a position its data does not
    /// reach.
    // FM-REPLICATION-004
    #[test]
    fn the_granted_offset_is_the_one_the_payload_and_the_replay_both_use() {
        let granted = match step(&wired(Phase::Connecting, full()), &SessionEvent::Begin)
            .effects
            .pop()
        {
            Some(Effect::SendReply(HandshakeReply::FullResync { offset, .. })) => offset,
            other => panic!("expected a +FULLRESYNC reply, got {other:?}"),
        };

        let shipped = match &step(
            &wired(Phase::PreparingCheckpoint, full()),
            &SessionEvent::CheckpointCut(StepOutcome::Ok),
        )
        .effects[1]
        {
            Effect::SendCheckpoint { offset, .. } => *offset,
            other => panic!("expected the checkpoint to be shipped, got {other:?}"),
        };

        let handoff = step(
            &wired(Phase::StreamingCheckpoint, full()),
            &SessionEvent::PayloadSent,
        );
        assert_eq!(handoff.phase, Phase::Streaming);
        assert_eq!(
            handoff.effects,
            vec![
                Effect::LogFullResyncComplete { offset: granted },
                Effect::ClearDeparture,
                Effect::Stream {
                    replay_from: granted,
                    resume: ResumeSource::FullSnapshot,
                },
            ]
        );
        assert_eq!(granted, shipped);
    }

    // ── The exit handler ────────────────────────────────────────────────────

    /// A session that never streamed never armed the self-fence, so it must not
    /// be able to disarm it.
    // FM-REPLICATION-062
    #[test]
    fn only_a_session_that_reached_streaming_reports_a_departure() {
        for phase in [
            Phase::Connecting,
            Phase::PreparingCheckpoint,
            Phase::StreamingCheckpoint,
        ] {
            let exit = step(
                &wired(phase, full()),
                &SessionEvent::Ended(LinkOutcome::Ended(ReplicaDeparture::Graceful)),
            );
            assert_eq!(exit.phase, Phase::Disconnecting);
            assert!(
                !exit
                    .effects
                    .iter()
                    .any(|e| matches!(e, Effect::RecordDeparture(_))),
                "a session that died in {phase} has no departure to classify"
            );
        }

        let streamed = step(
            &wired(Phase::Streaming, full()),
            &SessionEvent::Ended(LinkOutcome::Ended(ReplicaDeparture::Graceful)),
        );
        assert!(
            streamed
                .effects
                .contains(&Effect::RecordDeparture(ReplicaDeparture::Graceful))
        );
    }

    /// The record lands **before** the session leaves the registry. The fence's
    /// disarm reads "nothing is streaming" and "the last departure was
    /// graceful" as two separate loads; unregistering first opens a window in
    /// which a predecessor's graceful departure answers for a link that
    /// actually died.
    // FM-REPLICATION-062
    #[test]
    fn the_departure_is_recorded_before_the_session_leaves_the_registry() {
        let mut view = wired(Phase::Streaming, full());
        view.checkpoint_owed = true;

        let exit = step(
            &view,
            &SessionEvent::Ended(LinkOutcome::Ended(ReplicaDeparture::Lost)),
        );
        assert_eq!(
            exit.effects,
            vec![
                Effect::CleanCheckpointDir,
                Effect::RecordDeparture(ReplicaDeparture::Lost),
                Effect::Unregister,
                Effect::LogDisconnect,
            ]
        );
    }

    /// An error propagated out of any phase is a lost link by construction, so
    /// a new failure path is classified conservatively without being touched.
    // FM-REPLICATION-062
    #[test]
    fn an_error_out_of_any_phase_is_a_lost_departure() {
        let exit = step(
            &wired(Phase::Streaming, full()),
            &SessionEvent::Ended(LinkOutcome::Errored),
        );
        assert!(
            exit.effects
                .contains(&Effect::RecordDeparture(ReplicaDeparture::Lost))
        );
    }

    /// Cleanup is owed only by a session that staged a directory.
    // FM-REPLICATION-001
    #[test]
    fn a_session_that_staged_no_checkpoint_cleans_no_directory() {
        let exit = step(
            &wired(Phase::Streaming, partial()),
            &SessionEvent::Ended(LinkOutcome::Ended(ReplicaDeparture::Graceful)),
        );
        assert_eq!(
            exit.effects,
            vec![
                Effect::RecordDeparture(ReplicaDeparture::Graceful),
                Effect::Unregister,
                Effect::LogDisconnect,
            ]
        );
    }

    // ── The table as a whole ────────────────────────────────────────────────

    const PHASES: [Phase; 5] = [
        Phase::Connecting,
        Phase::PreparingCheckpoint,
        Phase::StreamingCheckpoint,
        Phase::Streaming,
        Phase::Disconnecting,
    ];

    fn every_event() -> Vec<SessionEvent> {
        vec![
            SessionEvent::Begin,
            SessionEvent::ReplySent,
            SessionEvent::Drained(StepOutcome::Ok),
            SessionEvent::Drained(StepOutcome::Failed("x".into())),
            SessionEvent::CheckpointCut(StepOutcome::Ok),
            SessionEvent::CheckpointCut(StepOutcome::Failed("x".into())),
            SessionEvent::CoverageBreached("shard(s) [1]".into()),
            SessionEvent::DatasetExported,
            SessionEvent::PayloadSent,
            SessionEvent::Ended(LinkOutcome::Ended(ReplicaDeparture::Graceful)),
            SessionEvent::Ended(LinkOutcome::Ended(ReplicaDeparture::Lost)),
            SessionEvent::Ended(LinkOutcome::Errored),
        ]
    }

    /// Every wiring of every source, crossed with every phase and both arms.
    fn every_view() -> Vec<SessionView> {
        let mut views = Vec::new();
        for phase in PHASES {
            for sync in [full(), partial()] {
                for bits in 0u8..32 {
                    let mut view = wired(phase, sync.clone());
                    view.checkpoint_source = bits & 1 != 0;
                    view.pre_checkpoint_drain = bits & 2 != 0;
                    view.function_snapshot = bits & 4 != 0;
                    view.live_snapshot_source = bits & 8 != 0;
                    view.checkpoint_owed = bits & 16 != 0;
                    views.push(view);
                }
            }
        }
        views
    }

    /// `INV-SESSION-1`, structurally: there is one writer of `Phase`, and no
    /// input to it produces a backwards move or an escape from the terminal.
    /// Checked through the production catalog rather than a local rank table,
    /// so deleting the catalog entry takes this test down with it.
    #[test]
    fn no_step_of_the_table_moves_a_phase_backwards_or_leaves_the_terminal() {
        // The check has to be able to fail, or the sweep below is vacuous: a
        // deleted `INV-SESSION-1` would otherwise make this test pass by saying
        // nothing. This is the assertion that takes it down with the catalog.
        assert!(
            !crate::invariants::check_hard(&ReplicationView::empty().with_phase_change(
                PhaseChange {
                    replica_id: 1,
                    from: Phase::Streaming,
                    to: Phase::Connecting,
                }
            ))
            .is_empty(),
            "the catalog must still report a backwards phase move"
        );

        for view in every_view() {
            for event in every_event() {
                let to = step(&view, &event).phase;
                let violations = crate::invariants::check_hard(
                    &ReplicationView::empty().with_phase_change(PhaseChange {
                        replica_id: view.replica_id,
                        from: view.phase,
                        to,
                    }),
                );
                assert!(
                    violations.is_empty(),
                    "{} + {event:?} -> {to}: {}",
                    view.phase,
                    crate::invariants::render(&violations),
                );
            }
        }
    }

    /// The terminal is terminal: nothing that reaches `Disconnecting` does
    /// anything afterwards except refuse.
    #[test]
    fn nothing_leaves_the_disconnecting_phase() {
        for view in every_view()
            .into_iter()
            .filter(|v| v.phase == Phase::Disconnecting)
        {
            for event in every_event() {
                assert_eq!(step(&view, &event).phase, Phase::Disconnecting);
            }
        }
    }

    /// The table is total. A pair the straight-line interpreter cannot produce
    /// keeps its phase and abandons the sync, rather than falling through to
    /// whichever arm happened to be next.
    #[test]
    fn an_event_a_phase_cannot_answer_abandons_the_sync() {
        let refused = step(
            &wired(Phase::StreamingCheckpoint, full()),
            &SessionEvent::Begin,
        );
        assert_eq!(refused.phase, Phase::StreamingCheckpoint);
        assert_eq!(
            refused.effects,
            vec![Effect::FailSync {
                failure: SyncFailure::UnexpectedEvent,
                cause: None,
            }]
        );
    }

    /// Every abandonment names itself in the error and in the log, and no two
    /// reasons collide.
    #[test]
    fn every_sync_failure_carries_a_distinct_reason_and_log_line() {
        let all = [
            SyncFailure::PreCheckpointDrain,
            SyncFailure::Checkpoint,
            SyncFailure::NoLiveSnapshotSource,
            SyncFailure::CoverageHoldBreached,
            SyncFailure::UnexpectedEvent,
        ];
        let reasons: std::collections::BTreeSet<_> = all.iter().map(|f| f.reason()).collect();
        let logs: std::collections::BTreeSet<_> = all.iter().map(|f| f.log_message()).collect();
        assert_eq!(reasons.len(), all.len());
        assert_eq!(logs.len(), all.len());
    }

    // ── The wire ────────────────────────────────────────────────────────────

    /// One renderer for both arms, and what it renders is what the replica's
    /// own arm selector parses back. The model checker prints the same lines
    /// through [`HandshakeReply::line`], so a wire change cannot leave the
    /// model checking a protocol the primary no longer speaks.
    // FM-REPLICATION-015
    #[test]
    fn the_handshake_replies_render_the_lines_the_replica_parses_back() {
        let cont = HandshakeReply::Continue {
            replication_id: hex_id('c'),
        };
        assert_eq!(cont.render(), format!("+CONTINUE {}\r\n", hex_id('c')));
        assert_eq!(
            select_psync_arm(&cont.line()).expect("the replica parses its own primary"),
            PsyncArm::Continue {
                granted_id: Some(hex_id('c')),
            }
        );

        let full = HandshakeReply::FullResync {
            replication_id: hex_id('d'),
            offset: 91,
        };
        assert_eq!(full.render(), format!("+FULLRESYNC {} 91\r\n", hex_id('d')));
        assert_eq!(
            select_psync_arm(&full.line()).expect("the replica parses its own primary"),
            PsyncArm::FullResync {
                granted_id: hex_id('d'),
                granted_offset: 91,
            }
        );
    }
}
