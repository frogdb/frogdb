//! The decision half of a streaming replica session's live feed.
//!
//! [`crate::feed_gate`] decides what a slot-handoff barrier *is* — the deadline
//! a set of armed pauses composes to, and whether a published deadline is still
//! in force. What consumes that answer is this module: given the state of one
//! session's feed, when does a frame go on the wire, when is it buffered behind
//! an armed barrier, in what order does the buffer drain when the barrier
//! releases, and what ends the link.
//!
//! Split out for the reason [`crate::session_machine`] was: the session's own
//! write task is an async loop over a broadcast receiver, a socket and a gate,
//! and a decision that reads plain data and returns a plain description of what
//! to do next can be unit-tested over its whole input space and driven by a
//! model checker, while the task that owns the socket keeps owning only that.
//!
//! # Why the gate consultation is an *output*
//!
//! The two points where a streaming session consults the barrier —
//! [`FeedAction::AwaitRelease`] before the handoff tail, and
//! [`FeedAction::ConsultGate`]/[`FeedAction::ReceiveOrRelease`] in the live loop
//! — are things [`FeedSequencer::step`] *asks for*, not branches the driver
//! takes on its own. A session that stops honouring the barrier is then a driver
//! that ignores an action the machine returned, rather than a deleted `await`
//! buried in a loop: the difference between a defect a harness driving this seam
//! can see and one only an integration test notices (replication-correctness
//! issue 26).
//!
//! # The sequence
//!
//! ```text
//!   AwaitRelease ──HandoffReplayed──► Receive ◄─────────────┐
//!                                        │                  │
//!                                   Received                │
//!                                        ▼                  │
//!        ┌────────────────────────► ConsultGate             │
//!        │                              │  │                │
//!   Received / Released      GateHeld(true) GateHeld(false)  │
//!        │                              ▼  │                │
//!        └───────────────────── ReceiveOrRelease            │
//!                                          ▼                │
//!                                        Send ──Sent────────┘
//!                                                (buffer empty)
//! ```
//!
//! Every path that ends the link — the broadcaster closing, the receiver
//! lagging off the channel, a frame that cannot be encoded or written, a lag
//! threshold breached — lands on [`FeedAction::End`]. A close or a lag observed
//! *while the buffer is not empty* still drains it first: the barrier held those
//! frames, and the link closing is not a reason to drop them on the floor.

use std::collections::VecDeque;

use crate::frame::ReplicationFrame;
use crate::replica_session::ReplicaDeparture;

/// What the driver must do before it can supply the next [`FeedInput`].
#[derive(Debug)]
pub enum FeedAction {
    /// Wait out an armed slot-handoff barrier, then replay the handoff tail.
    ///
    /// The tail is feed too, so a session that handshakes mid-barrier must not
    /// pull the held writes straight out of the backlog (FM-CLUSTER-097). The
    /// wait is bounded by the barrier's own deadline, so it cannot wedge.
    AwaitRelease,
    /// Take the next frame off the WAL broadcast. Nothing is buffered and no
    /// barrier is known to be armed, so this is a plain wait.
    Receive,
    /// A barrier *is* armed: race the next broadcast frame against its release,
    /// so the session keeps draining the channel instead of lagging off it.
    ReceiveOrRelease,
    /// Ask the gate whether a barrier is armed right now.
    ConsultGate,
    /// Put this frame on the wire.
    Send(ReplicationFrame),
    /// The link is over, however it ended.
    End(ReplicaDeparture),
}

/// What happened when the driver performed a [`FeedAction`].
#[derive(Debug)]
pub enum FeedInput {
    /// The handoff lane finished: the barrier released, and the backlog tail
    /// was replayed up to `resume_offset` (the grant's `replay_from` when the
    /// tail was empty). Frames at or below it are already on the wire.
    HandoffReplayed {
        /// The last offset the handoff replay actually streamed.
        resume_offset: u64,
    },
    /// A frame arrived on the broadcast.
    Received(ReplicationFrame),
    /// The primary's own broadcaster went away — this node is shutting down or
    /// ending its primary stint.
    SourceClosed,
    /// The broadcast dropped frames this session will never see.
    SourceLagged,
    /// The gate's answer to [`FeedAction::ConsultGate`].
    GateHeld(bool),
    /// The barrier released while the session was waiting it out.
    Released,
    /// The frame reached the wire.
    Sent {
        /// The lag policy's verdict, evaluated after the write landed.
        lag_breached: bool,
    },
    /// The frame could not be encoded, or could not be written.
    SendFailed,
}

/// Where in the sequence the feed is. Private: the driver reacts to
/// [`FeedAction`]s and never inspects the stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Before the handoff tail: the barrier wait is owed.
    Handoff,
    /// Waiting for the next frame, with nothing buffered.
    Receiving,
    /// The gate has been asked; waiting for its answer.
    Consulting,
    /// A barrier is armed: buffering the broadcast until it releases.
    Holding,
    /// A buffered frame is on its way to the wire.
    Sending,
    /// The link is over.
    Ended,
}

/// The live feed's sequencing, as a state machine over plain data.
///
/// Deterministic and side-effect free: no locks, no socket, no clock. The
/// barrier's deadline never appears here at all — whether a hold is in force is
/// an *input* ([`FeedInput::GateHeld`]), decided by
/// [`crate::feed_gate::decide_hold`] against a clock the driver reads.
#[derive(Debug)]
pub struct FeedSequencer {
    /// Frames at or below this offset were already sent by the handoff replay;
    /// re-sending them would make the replica apply them twice.
    resume_offset: u64,
    /// Frames the barrier is keeping off the wire, in offset order. Empty
    /// whenever no barrier is armed, which is the overwhelmingly common case.
    /// Buffering here rather than leaving them in the broadcast channel is what
    /// keeps a held session from tripping the `Lagged` disconnect and resyncing
    /// its way around the barrier; the buffer is bounded by the writes a node
    /// takes inside one barrier window, because the gate expires itself on the
    /// deadline the barrier armed it with.
    held: VecDeque<ReplicationFrame>,
    /// How the link ended, once something ended it. The buffer still drains
    /// before the departure is reported.
    ending: Option<ReplicaDeparture>,
    /// The stage the next input is interpreted against.
    stage: Stage,
}

impl Default for FeedSequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl FeedSequencer {
    /// A session that has just been granted its sync and has not touched the
    /// feed yet.
    pub fn new() -> Self {
        Self {
            resume_offset: 0,
            held: VecDeque::new(),
            ending: None,
            stage: Stage::Handoff,
        }
    }

    /// What to do before the first input: wait the barrier out, because the
    /// handoff tail is feed too.
    pub fn start(&self) -> FeedAction {
        FeedAction::AwaitRelease
    }

    /// How many frames the barrier is currently holding off the wire.
    pub fn held_frames(&self) -> usize {
        self.held.len()
    }

    /// Feed one input and get back what the driver must do next.
    ///
    /// Pure over `(state, input)`. The table is total: a pair the driver cannot
    /// produce ends the link as [`ReplicaDeparture::Lost`], which is the fence's
    /// safe direction (FM-REPLICATION-062) and is what makes [`Stage::Ended`]
    /// terminal without an arm of its own.
    pub fn step(&mut self, input: FeedInput) -> FeedAction {
        match (self.stage, input) {
            // ── The handoff lane is behind us ───────────────────────────────
            (Stage::Handoff, FeedInput::HandoffReplayed { resume_offset }) => {
                self.resume_offset = resume_offset;
                self.flush()
            }

            // ── A frame arrived: buffer it, then ask about the barrier ──────
            //
            // One rule for both lanes: whether the session was idle or already
            // waiting a barrier out, the next thing it does is re-read the
            // gate. Buffering unconditionally is what keeps the drain in offset
            // order — a frame is never written ahead of one already held.
            (Stage::Receiving | Stage::Holding, FeedInput::Received(frame)) => {
                self.buffer(frame);
                self.stage = Stage::Consulting;
                FeedAction::ConsultGate
            }
            (Stage::Holding, FeedInput::Released) => {
                self.stage = Stage::Consulting;
                FeedAction::ConsultGate
            }

            // ── The gate answered ───────────────────────────────────────────
            (Stage::Consulting, FeedInput::GateHeld(true)) => {
                self.stage = Stage::Holding;
                FeedAction::ReceiveOrRelease
            }
            (Stage::Consulting, FeedInput::GateHeld(false)) => self.flush(),

            // ── The frame source ended ──────────────────────────────────────
            //
            // Both arms flush first: the link closing is not a reason to drop
            // what the barrier was holding.
            (Stage::Receiving | Stage::Holding, FeedInput::SourceClosed) => {
                self.ending = Some(ReplicaDeparture::Graceful);
                self.flush()
            }
            (Stage::Receiving | Stage::Holding, FeedInput::SourceLagged) => {
                self.ending = Some(ReplicaDeparture::Lost);
                self.flush()
            }

            // ── A buffered frame reached the wire, or did not ───────────────
            (
                Stage::Sending,
                FeedInput::Sent {
                    lag_breached: false,
                },
            ) => self.flush(),
            // This arm is behaviorally identical to the `_` fence below — it
            // exists so the two driver-reachable failure inputs are named
            // rather than swallowed by the "not reachable" catch-all. Deleting
            // it is an equivalent mutant (excluded in .cargo/mutants.toml).
            (Stage::Sending, FeedInput::Sent { lag_breached: true })
            | (Stage::Sending, FeedInput::SendFailed) => self.end(ReplicaDeparture::Lost),

            // ── Not reachable from the driver ───────────────────────────────
            _ => self.end(ReplicaDeparture::Lost),
        }
    }

    /// Take a frame into the buffer unless the handoff replay already sent it.
    fn buffer(&mut self, frame: ReplicationFrame) {
        if frame.sequence > self.resume_offset {
            self.held.push_back(frame);
        }
    }

    /// The feed is free: ship what is buffered, in offset order, then report
    /// the departure if something already ended the link, or go back to
    /// waiting on the broadcast.
    fn flush(&mut self) -> FeedAction {
        if let Some(frame) = self.held.pop_front() {
            self.stage = Stage::Sending;
            return FeedAction::Send(frame);
        }
        match self.ending {
            Some(departure) => self.end(departure),
            None => {
                self.stage = Stage::Receiving;
                FeedAction::Receive
            }
        }
    }

    /// End the link, abandoning anything still buffered — a send that failed or
    /// a lag threshold breached means the wire is gone, not paused.
    fn end(&mut self, departure: ReplicaDeparture) -> FeedAction {
        self.stage = Stage::Ended;
        self.held.clear();
        FeedAction::End(departure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn frame(sequence: u64) -> ReplicationFrame {
        ReplicationFrame::new(sequence, Bytes::from_static(b"payload"))
    }

    /// A sequencer past the handoff lane, resuming at `resume_offset`.
    fn streaming(resume_offset: u64) -> FeedSequencer {
        let mut sequencer = FeedSequencer::new();
        assert!(matches!(sequencer.start(), FeedAction::AwaitRelease));
        let action = sequencer.step(FeedInput::HandoffReplayed { resume_offset });
        assert!(matches!(action, FeedAction::Receive));
        sequencer
    }

    /// The offset a `Send` names, or `None` for any other action.
    fn sending(action: &FeedAction) -> Option<u64> {
        match action {
            FeedAction::Send(frame) => Some(frame.sequence),
            _ => None,
        }
    }

    /// The departure an `End` names, or `None` for any other action.
    fn ending(action: &FeedAction) -> Option<ReplicaDeparture> {
        match action {
            FeedAction::End(departure) => Some(*departure),
            _ => None,
        }
    }

    /// The barrier wait is the machine's *first* demand, before a single frame
    /// is pulled out of the backlog — the handoff lane's half of
    /// FM-CLUSTER-097. Nothing else can be asked for until the tail is in.
    #[test]
    fn the_first_demand_is_the_barrier_wait() {
        let sequencer = FeedSequencer::new();
        assert!(matches!(sequencer.start(), FeedAction::AwaitRelease));
    }

    /// With no barrier armed, a frame is consulted-then-shipped and the session
    /// goes straight back to the broadcast.
    #[test]
    fn an_unheld_frame_is_shipped_and_the_session_waits_again() {
        let mut sequencer = streaming(10);
        assert!(matches!(
            sequencer.step(FeedInput::Received(frame(11))),
            FeedAction::ConsultGate
        ));
        let action = sequencer.step(FeedInput::GateHeld(false));
        assert_eq!(sending(&action), Some(11));
        assert!(matches!(
            sequencer.step(FeedInput::Sent {
                lag_breached: false
            }),
            FeedAction::Receive
        ));
        assert_eq!(sequencer.held_frames(), 0);
    }

    /// The handoff overlap is sent exactly once: a frame the replay already put
    /// on the wire is dropped rather than buffered, so the replica cannot
    /// double-apply it.
    #[test]
    fn a_frame_the_handoff_replay_already_sent_is_dropped() {
        let mut sequencer = streaming(10);
        for sequence in [9, 10] {
            assert!(matches!(
                sequencer.step(FeedInput::Received(frame(sequence))),
                FeedAction::ConsultGate
            ));
            assert!(matches!(
                sequencer.step(FeedInput::GateHeld(false)),
                FeedAction::Receive,
            ));
            assert_eq!(sequencer.held_frames(), 0);
        }
    }

    /// Hold, then flush: while the gate answers held the session keeps draining
    /// the broadcast into the buffer and never writes; when it releases, the
    /// whole buffer goes out in offset order.
    #[test]
    fn a_held_feed_buffers_in_order_and_drains_on_release() {
        let mut sequencer = streaming(0);
        assert!(matches!(
            sequencer.step(FeedInput::Received(frame(1))),
            FeedAction::ConsultGate
        ));
        assert!(matches!(
            sequencer.step(FeedInput::GateHeld(true)),
            FeedAction::ReceiveOrRelease
        ));

        // Two more arrive inside the window. Each one re-reads the gate, and
        // each time the answer is still "held" — nothing is written.
        for sequence in [2, 3] {
            assert!(matches!(
                sequencer.step(FeedInput::Received(frame(sequence))),
                FeedAction::ConsultGate
            ));
            assert!(matches!(
                sequencer.step(FeedInput::GateHeld(true)),
                FeedAction::ReceiveOrRelease
            ));
        }
        assert_eq!(sequencer.held_frames(), 3);

        // The barrier lifts. The release alone does not open the feed — the
        // gate is re-read, because a later barrier may still hold it.
        assert!(matches!(
            sequencer.step(FeedInput::Released),
            FeedAction::ConsultGate
        ));
        let mut action = sequencer.step(FeedInput::GateHeld(false));
        for sequence in [1, 2, 3] {
            assert_eq!(sending(&action), Some(sequence));
            action = sequencer.step(FeedInput::Sent {
                lag_breached: false,
            });
        }
        assert!(matches!(action, FeedAction::Receive));
    }

    /// A release that another armed barrier outlives does not open the feed:
    /// the machine re-reads the gate rather than trusting the wakeup.
    #[test]
    fn a_release_that_another_barrier_outlives_keeps_the_feed_held() {
        let mut sequencer = streaming(0);
        sequencer.step(FeedInput::Received(frame(1)));
        sequencer.step(FeedInput::GateHeld(true));
        assert!(matches!(
            sequencer.step(FeedInput::Released),
            FeedAction::ConsultGate
        ));
        assert!(matches!(
            sequencer.step(FeedInput::GateHeld(true)),
            FeedAction::ReceiveOrRelease
        ));
        assert_eq!(sequencer.held_frames(), 1);
    }

    /// The broadcaster going away inside a barrier window still flushes what
    /// the barrier held before the link is reported closed.
    #[test]
    fn a_close_inside_the_window_flushes_before_it_ends_the_link() {
        let mut sequencer = streaming(0);
        sequencer.step(FeedInput::Received(frame(1)));
        assert!(matches!(
            sequencer.step(FeedInput::GateHeld(true)),
            FeedAction::ReceiveOrRelease
        ));

        let action = sequencer.step(FeedInput::SourceClosed);
        assert_eq!(sending(&action), Some(1));
        let action = sequencer.step(FeedInput::Sent {
            lag_breached: false,
        });
        assert_eq!(ending(&action), Some(ReplicaDeparture::Graceful));
    }

    /// Lagging off the broadcast is a lost link — but the frames the barrier
    /// already buffered are still the replica's, and go out first.
    #[test]
    fn a_lagged_receiver_flushes_then_reports_a_lost_link() {
        let mut sequencer = streaming(0);
        sequencer.step(FeedInput::Received(frame(1)));
        sequencer.step(FeedInput::GateHeld(true));

        let action = sequencer.step(FeedInput::SourceLagged);
        assert_eq!(sending(&action), Some(1));
        let action = sequencer.step(FeedInput::Sent {
            lag_breached: false,
        });
        assert_eq!(ending(&action), Some(ReplicaDeparture::Lost));
    }

    /// With nothing buffered there is nothing to flush, and the source's end is
    /// reported straight away.
    #[test]
    fn an_idle_session_reports_its_sources_end_immediately() {
        let mut sequencer = streaming(0);
        assert_eq!(
            ending(&sequencer.step(FeedInput::SourceClosed)),
            Some(ReplicaDeparture::Graceful)
        );

        let mut sequencer = streaming(0);
        assert_eq!(
            ending(&sequencer.step(FeedInput::SourceLagged)),
            Some(ReplicaDeparture::Lost)
        );
    }

    /// A frame that cannot reach the wire ends the link as lost, and the rest
    /// of the buffer goes with it: the wire is gone, not paused. The graceful
    /// close the buffer was draining towards does not survive it.
    #[test]
    fn a_failed_send_loses_the_link_and_abandons_the_buffer() {
        let mut sequencer = streaming(0);
        sequencer.step(FeedInput::Received(frame(1)));
        sequencer.step(FeedInput::GateHeld(true));
        sequencer.step(FeedInput::Received(frame(2)));
        sequencer.step(FeedInput::GateHeld(true));
        assert_eq!(
            ending(&sequencer.step(FeedInput::SourceClosed)),
            None,
            "the close must flush the buffer before it ends the link"
        );

        assert_eq!(
            ending(&sequencer.step(FeedInput::SendFailed)),
            Some(ReplicaDeparture::Lost)
        );
        assert_eq!(sequencer.held_frames(), 0);
    }

    /// A lag threshold breached after a successful write ends the link at that
    /// frame, without shipping the rest of the buffer.
    #[test]
    fn a_lag_breach_ends_the_link_at_the_frame_it_fired_on() {
        let mut sequencer = streaming(0);
        sequencer.step(FeedInput::Received(frame(1)));
        sequencer.step(FeedInput::GateHeld(true));
        sequencer.step(FeedInput::Received(frame(2)));
        sequencer.step(FeedInput::GateHeld(false));

        assert_eq!(
            ending(&sequencer.step(FeedInput::Sent { lag_breached: true })),
            Some(ReplicaDeparture::Lost)
        );
        assert_eq!(sequencer.held_frames(), 0);
    }

    /// The table is total, and every pair the driver cannot produce takes the
    /// fence's safe direction. `Ended` is terminal by construction: nothing
    /// leaves it.
    #[test]
    fn an_impossible_pair_loses_the_link_and_stays_ended() {
        let mut sequencer = streaming(0);
        assert_eq!(
            ending(&sequencer.step(FeedInput::Sent {
                lag_breached: false
            })),
            Some(ReplicaDeparture::Lost),
            "nothing was sent, so there is no send to report"
        );
        assert_eq!(
            ending(&sequencer.step(FeedInput::Received(frame(1)))),
            Some(ReplicaDeparture::Lost),
            "a link that ended does not start receiving again"
        );
    }
}
