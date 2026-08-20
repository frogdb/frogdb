// replication_feed_gate — the runnable main model of the replica feed-gate / barrier
// session sequencing (formal-spec phase 3, campaign task T9b; the replication-issue-26
// class). One node, a small set of replica-facing streaming sessions sharing one node-wide
// feed gate, and the minimal environment they run against: a primary whose live offset
// advances, an upstream frame source that can close or overrun a receiver, and barriers
// that arm and release.
//
// This file holds everything checkable — the witnesses, the `val inv_*` invariants (which
// is also what makes scripts/quint-models.sh treat this file, and not its three satellite
// modules, as a runnable model), and the `run` tests that pin the canonical sequences.
//
// ---------------------------------------------------------------------------
// Rows modeled
// ---------------------------------------------------------------------------
//   TR-REPLICATION-013 — arming and releasing a barrier publishes a feed-gate deadline,
//     and a session consults the gate before it puts anything on the wire. Modeled as the
//     `armed` map, `gateHeld`, and the `Consulting` stage every path to the wire passes
//     through. Catalog: INV-GATE-1.
//   FM-CLUSTER-097 — the node-wide feed hold: nothing the node applies during the barrier
//     window reaches the replica; when the barrier ends — completed, aborted, or simply
//     lapsed — the feed resumes and the replica converges on everything held, in offset
//     order and with nothing dropped; a replica that PSYNCs *into* the window waits before
//     it replays the backlog tail. Modeled as `inv_no_ship_inside_barrier_window`,
//     `inv_nothing_held_is_dropped`, the FIFO family, and `canHandoff`'s gate conjunct.
//     Cited across areas deliberately: the barrier is a cluster row, the feed it holds is
//     replication state, and this model is where the two meet.
//   TR-REPLICATION-007 / FM-REPLICATION-062 — a departure is classified before the session
//     leaves the registry, and only a graceful departure disarms the self-fence. Modeled
//     as `Departure`/`EndCause`, the classify-then-drain split in `applyClassify`, and the
//     node-wide `departure_record`. Catalog: INV-FENCE-1, INV-SESSION-3.
//   FM-REPLICATION-015 — a granted `+CONTINUE` replays exactly `(replay_from, current]`,
//     once, in order. Modeled as `resume_offset` and the dedup in `applyReceiveFrame`,
//     checked by `inv_no_resend_below_resume` and the FIFO family. Catalog: INV-BACKLOG-2.
//   TR-REPLICATION-030 — the live offset advances as the backlog is appended (`writeFrame`).
//   TR-REPLICATION-018 / TR-REPLICATION-033 — ACK ingest and "a replica ACKs the landed
//     offset". Modeled as `admitAck`, carrying replication issue 21's AMENDED ruling:
//     an ACK above the live offset is ignored *entirely* and counted, never clamped.
//     Catalog: INV-OFFSET-3.
//   Replication issue 22 (AMENDED) — a session displaced by a newer one announcing the
//     same replica identity departs `Superseded` and writes **no** departure record; an
//     unannounced identity is never deduped. Catalog: INV-SESSION-2.
//
// ---------------------------------------------------------------------------
// Projection onto the real seam
// ---------------------------------------------------------------------------
// The decision points are `FeedSequencer::step(FeedInput) -> FeedAction` in
// frogdb-server/crates/replication/src/feed_sequencer.rs. This model is a *spec* model —
// its obligations come from the rows above, not from that code — but its actions are
// deliberately aligned one-to-one with the seam's inputs so a later quint-connect harness
// can replay a trace through the real struct:
//
//   model action            FeedInput                    FeedAction the seam returns
//   ---------------------------------------------------------------------------------
//   handoffReplayedAs       HandoffReplayed{resume}      Send | Receive
//   receiveFrame            Received(frame)              ConsultGate
//   consultGate (held)      GateHeld(true)               ReceiveOrRelease
//   consultGate (open)      GateHeld(false)              Send | Receive | End
//   observeRelease          Released                     ConsultGate
//   sourceClosed            SourceClosed                 Send | End(Graceful)
//   sourceLagged            SourceLagged                 Send | End(Lost)
//   confirmSentAs(_, false) Sent{lag_breached: false}    Send | Receive | End
//   confirmSentAs(_, true)  Sent{lag_breached: true}     End(Lost)
//   sendFailed              SendFailed                   End(Lost)
//   supersede               (none — out-of-band kick)    End(Superseded)
//
// Environment actions (`writeFrame`, `closeSource`, `armBarrier`, `releaseBarrier`,
// `lapseBarrier`, `replicaAck`, `replicaAckAbove`) have no `FeedInput`: they are what makes
// the inputs happen, and a connect harness drives them on the outside of the sequencer.
//
// ---------------------------------------------------------------------------
// Deliberately not modeled
// ---------------------------------------------------------------------------
//   - No wall clock, anywhere. The gate's clock is driver-side: the deadline reaches the
//     sequencer as a `GateHeld` answer, and a deadline expiring with nothing releasing it
//     is the nondeterministic `lapseBarrier`. This is the campaign's global ruling, and it
//     is why `gateHeld` is a non-emptiness test rather than a `now < deadline` comparison.
//   - No hold-overflow disconnect. The held buffer is unbounded on purpose: the real
//     sequencer has no size bound either, and the only overrun that disconnects a replica
//     is the upstream broadcast channel's (`sourceLagged`). Modeling a buffer cap would
//     invent a departure the system cannot produce.
//   - The seam's total transition table. `FeedSequencer::step` has a `_ => end(Lost)`
//     fallback for input/stage pairs its driver cannot produce; that is driver robustness,
//     not a spec obligation, and the model's guards simply disable those pairs.
//   - `RecordDeparture`-before-`Unregister` effect ordering (FM-REPLICATION-062's other
//     half). That ordering belongs to the session-registry seam, not this one; the model
//     carries only the *content* rule — what a departure is classified as, and whether it
//     is recorded at all.
//   - The handoff replay is atomic. The gate decision that matters is the one taken before
//     the replay starts (`canHandoff`); replaying frame by frame would multiply the state
//     space without adding a decision point.
//
// ---------------------------------------------------------------------------
// The ending-drain carve-out (ruled, not a divergence)
// ---------------------------------------------------------------------------
// The seam's `SourceClosed`/`SourceLagged` arms set `ending` and then call `flush()`
// *without* re-reading the gate, so a link that ends inside the window drains its held
// buffer past the barrier floor (the seam's own tests assert this: a close inside the
// window flushes before it ends the link). This model was filed as a spec/seam divergence
// against FM-CLUSTER-097's then-unconditional "nothing applied during the window reaches
// the replica" (.scratch/formal-spec/t9b-blocked.md).
//
// **Ruled 2026-08-20 — R3 of the quint-completeness campaign ledger**
// (.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md): the ending drain is
// CORRECT and the row was too strong. FM-CLUSTER-097's Observable now carries the "unless
// the link is ending" clause — a session that has already classified its departure drains
// the frames it already accepted, because dropping that tail is strictly worse for a
// replica about to reconnect and PSYNC from its own offset — together with the protection
// that makes it safe: a departing replica is not a promotion candidate for the slot under
// handoff.
//
// So `inv_no_ship_inside_barrier_window`'s `isEnding` carve-out is the ruled semantics, not
// a tolerated deviation. It stays narrow — only a session that has *already* classified its
// departure, only for the buffer it drains on the way out — and it stays non-vacuous via
// `coverage.endingDrainedPastBarrierFloor`, its witness, and
// `closeInsideWindowDrainsThenEndsGracefulTest`, so it cannot be deleted or become
// unreachable without an oracle noticing.
//
// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------
//   replication_feed_gate_types.qnt   — types and sizing constants
//   replication_feed_gate_logic.qnt   — `can*` guards and `apply*` updates (pure)
//   replication_feed_gate_machine.qnt — `var`s, `init`, actions, `step`
//   replication_feed_gate.qnt         — this file: witnesses, invariants, `run` tests
module replication_feed_gate {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"
  import replication_feed_gate_types.* from "./replication_feed_gate_types"
  import replication_feed_gate_logic.* from "./replication_feed_gate_logic"
  import replication_feed_gate_machine.* from "./replication_feed_gate_machine"

  // ===========================================================================
  // Witnesses — reachability, one per behaviour the invariants below depend on.
  // A witness reported in 0 traces means the behaviour is unreachable and every
  // invariant that quantifies over it is vacuous.
  // ===========================================================================

  val witnessHeldFrame: bool = coverage.heldFrame
  val witnessDrainedOnRelease: bool = coverage.drainedOnRelease
  val witnessOverlappingHold: bool = coverage.overlappingHold
  val witnessBothSessionsHeld: bool = coverage.bothSessionsHeld
  val witnessBarrierReleased: bool = coverage.barrierReleased
  val witnessBarrierLapsed: bool = coverage.barrierLapsed
  val witnessHandoffAfterRelease: bool = coverage.handoffAfterRelease
  val witnessHandoffDeduped: bool = coverage.handoffDeduped
  val witnessGracefulEnd: bool = coverage.gracefulEnd
  val witnessLostEnd: bool = coverage.lostEnd
  val witnessSupersededEnd: bool = coverage.supersededEnd
  val witnessLaggedDisconnect: bool = coverage.laggedDisconnect
  val witnessSendFailed: bool = coverage.sendFailed
  val witnessLagBreach: bool = coverage.lagBreach
  // Reachable but deep: it needs a barrier armed *before* the frame was written, the
  // frame buffered, and the link then ending before anything releases the barrier — about
  // eight specific choices in a row, so a uniform random walk finds it roughly 16 times in
  // 200 000 samples and essentially never at the CI smoke budget. Its deterministic
  // carrier is `closeInsideWindowDrainsThenEndsGracefulTest` below, which is what keeps
  // the carve-out in `inv_no_ship_inside_barrier_window` from being vacuous.
  val witnessEndingDrainedPastFloor: bool = coverage.endingDrainedPastBarrierFloor
  val witnessAckIgnored: bool = coverage.ackIgnored
  // Not a coverage ghost: the plain "a session actually put something on the wire" check,
  // so a run whose sessions all die before streaming is visibly different from one that works.
  val witnessWireAdvanced: bool = SESSIONS.exists(s => sessions.get(s).sent.length() > 0)

  // ===========================================================================
  // The barrier window (TR-REPLICATION-013, FM-CLUSTER-097; INV-GATE-1)
  // ===========================================================================

  // Nothing applied during a barrier window reaches a replica.
  //
  // Stated against each armed barrier's *floor* — the live offset at the moment it armed —
  // rather than against a clock or a "was held" history. That works because the live offset
  // only advances: every frame that was already on the wire when the barrier armed is at or
  // below the floor, so "this session's wire went past the floor" is exactly "a frame
  // shipped after this barrier armed". It also gets the awkward case right for free: a
  // barrier that arms in the middle of a drain records a floor above everything already
  // buffered, so the drain in progress is not retroactively a violation.
  //
  // The `isEnding` carve-out is FM-CLUSTER-097's ruled "unless the link is ending" clause
  // — ruling R3, 2026-08-20, in .scratch/formal-spec/2026-08-19-quint-completeness-campaign.md
  // (analysis in .scratch/formal-spec/t9b-blocked.md); see this file's header. It excuses
  // only a session that has *already* classified its departure, and only for the buffer it
  // is draining on the way out; a session that is still live gets no exemption. What makes
  // it safe is stated at the row: a departing replica is not a promotion candidate for the
  // slot under handoff. `witnessEndingDrainedPastFloor` proves the carve-out is actually
  // exercised, so it cannot be quietly load-bearing.
  val inv_no_ship_inside_barrier_window: bool =
    BARRIERS.forall(b =>
      SESSIONS.forall(s => {
        val ss = sessions.get(s)
        isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)
      }))

  // Nothing held is dropped: for a session that is still live, every frame it accepted is
  // accounted for — already on the wire, in flight, or still buffered. This is the
  // "converges on everything held ... with nothing dropped" half of FM-CLUSTER-097. Ended
  // sessions are excluded because ending *is* the licence to drop the buffer.
  val inv_nothing_held_is_dropped: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      val inFlight = if (ss.in_flight == None) 0 else 1
      ss.stage == Ended or
        ss.accepted.length() == ss.sent.length() + ss.held.length() + inFlight
    })

  // ===========================================================================
  // Ordering on the wire (FM-REPLICATION-015; INV-BACKLOG-2)
  // ===========================================================================

  // FIFO, no skip, no duplicate — one claim. The wire is an exact prefix of what the
  // session accepted: every position matches, in order, so nothing was reordered, nothing
  // was skipped over, and nothing was sent twice (a duplicate would shift every later
  // position). The prefix may be short — frames still buffered or in flight are the
  // difference — but it is never wrong.
  val inv_wire_is_a_prefix_of_accepted: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      isPrefix(ss.sent, ss.accepted)
    })

  val inv_offsets_strictly_increase: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      and {
        strictlyIncreasing(ss.sent),
        strictlyIncreasing(ss.accepted),
        strictlyIncreasing(ss.held),
      }
    })

  // The buffer is strictly ahead of the wire: nothing waiting to be sent has already been
  // sent. Together with the prefix invariant this is what makes the drain order the same
  // order the frames arrived in.
  val inv_buffer_ahead_of_the_wire: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      ss.held.indices().forall(i => ss.held.nth(i) > wireMax(ss))
    })

  // FM-REPLICATION-015's "once": the live stream never re-sends a frame the granted replay
  // already put on the wire. Ghost-backed because the direct statement is unavailable —
  // the handoff legitimately seeds the wire with frames at or below `resume_offset`, so
  // only a *live-path* send below it is the violation, and that is a property of the
  // transition rather than of the state.
  val inv_no_resend_below_resume: bool = not(defects.resentBelowResume)

  // ===========================================================================
  // Departure classification (TR-REPLICATION-007, FM-REPLICATION-062;
  // INV-FENCE-1, INV-SESSION-2, INV-SESSION-3)
  // ===========================================================================

  // A session that has ended has been classified, and the classification matches the input
  // that produced it. Three distinct causes fold to `Lost`, so this is checked against the
  // cause rather than against the departure alone.
  val inv_departure_matches_its_cause: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      and {
        ss.stage != Ended or (ss.ending != None and ss.cause != None),
        optForall(ss.cause, c => optExists(ss.ending, d => expectedDeparture(c) == d)),
      }
    })

  // A graceful departure means the link went down clean, so it must have drained first.
  // A session that reported `Graceful` while it still held frames would tell the fence
  // "this replica is fine" about a replica that is missing writes.
  val inv_graceful_end_flushed_everything: bool = not(defects.gracefulEndDroppedFrames)

  // Replication issue 22 (AMENDED): a superseded session writes no departure record. Its
  // record would land after the successor's enter-streaming clear and fence a primary
  // whose replica is in fact healthy.
  val inv_superseded_writes_no_departure: bool =
    not(defects.supersededRecordedDeparture)

  val inv_departure_record_is_never_superseded: bool =
    optForall(departure_record, d => d != Superseded)

  // Every recorded departure describes a session that actually ended, with that
  // classification — the fence is never armed or disarmed by a record nothing produced.
  val inv_departure_record_backed_by_an_ended_session: bool =
    optForall(departure_record, d =>
      SESSIONS.exists(s => {
        val ss = sessions.get(s)
        ss.stage == Ended and optExists(ss.ending, e => e == d)
      }))

  // An unannounced identity is never deduped (issue 22): a session that announced nothing
  // can neither supersede nor be superseded, so it never ends `Superseded`.
  val inv_unannounced_is_never_superseded: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      ss.identity != None or not(optExists(ss.ending, d => d == Superseded))
    })

  // ===========================================================================
  // Offsets and ACKs (TR-REPLICATION-018 amended by issue 21, TR-REPLICATION-033;
  // INV-OFFSET-3)
  // ===========================================================================

  // Nothing is credited past the live head.
  val inv_ack_never_above_live: bool =
    SESSIONS.forall(s => sessions.get(s).acked <= live)

  // Nor past what this session actually put on the wire: a replica ACKs the offset it
  // landed, never one it could not have seen (TR-REPLICATION-033).
  val inv_ack_never_above_the_wire: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      ss.acked <= wireMax(ss)
    })

  // Issue 21 (AMENDED): an impossible ACK is ignored, not clamped. Clamping would make the
  // primary a writer of the replica's acked offset — a clamped ACK is indistinguishable
  // from a real one, so a `WAIT` could be satisfied by a number the primary invented.
  val inv_ack_ignored_not_clamped: bool = not(defects.clampedAck)

  // Every offset the model tracks stays within what the primary has actually written.
  val inv_offsets_within_live: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      and {
        ss.cursor <= live,
        ss.resume_offset <= live,
        wireMax(ss) <= live,
        ss.accepted.indices().forall(i => ss.accepted.nth(i) <= live),
      }
    })

  // ===========================================================================
  // Session sequencing shape (INV-SESSION-2, INV-SESSION-3)
  // ===========================================================================

  // The stage and the rest of the record cannot disagree: a frame is in flight exactly
  // while the session is `Sending`, an ended session holds nothing, and a session still in
  // handoff has put nothing on the wire and claimed no identity.
  val inv_stage_shape: bool =
    SESSIONS.forall(s => {
      val ss = sessions.get(s)
      and {
        (ss.stage == Sending) == (ss.in_flight != None),
        ss.stage != Ended or (ss.held.length() == 0 and ss.in_flight == None),
        ss.stage != Handoff or and {
          ss.sent.length() == 0,
          ss.accepted.length() == 0,
          ss.identity == None,
          ss.resume_offset == 0,
        },
      }
    })

  // Two live sessions never share an announced identity — the supersession kick is what
  // keeps `INV-SESSION-2`'s "one entry per live session" true, and a `WAIT` that counted a
  // doubled identity would count one follower twice.
  //
  // Deliberately *not* an invariant of this model: the model lets the successor reach
  // streaming and only then kicks the predecessor, so the overlap is reachable by
  // construction. What is checked instead is that the overlap is always resolvable — the
  // kick is enabled whenever it exists — which is the obligation the sequencing owes.
  val inv_duplicate_identity_is_always_kickable: bool =
    SESSIONS.forall(a =>
      SESSIONS.forall(b =>
        (a == b) or
        not(and {
          sessions.get(a).stage != Handoff, sessions.get(a).stage != Ended,
          sessions.get(b).stage != Handoff, sessions.get(b).stage != Ended,
          optExists(sessions.get(a).identity, i => announced(sessions.get(b), i)),
        }) or
        canSupersede(sessions.get(a), sessions.get(b)) or
        canSupersede(sessions.get(b), sessions.get(a))))

  // ===========================================================================
  // Run tests — the canonical sequences, pinned.
  // ===========================================================================

  // Hold, then drain: a frame that arrives while a barrier is armed waits, and reaches the
  // wire only once the barrier releases and the session re-reads the gate.
  run holdThenDrainTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(armBarrier(1))
      .then(receiveFrame(1))
      .then(consultGate(1))
      .expect(and {
        sessions.get(1).stage == Holding,
        sessions.get(1).held == List(1),
        sessions.get(1).sent == List(),
        coverage.heldFrame,
      })
      .then(releaseBarrier(1))
      .then(observeRelease(1))
      .expect(coverage.drainedOnRelease)
      .then(consultGate(1))
      .expect(sessions.get(1).in_flight == Some(1))
      .then(confirmSentAs(1, false))
      .expect(and {
        sessions.get(1).sent == List(1),
        sessions.get(1).stage == Receiving,
        sessions.get(1).held == List(),
      })

  // Overlapping barriers: a release from one arm does not open the gate while the other is
  // still armed, and the drain when it finally comes is in offset order.
  run barrierWhileHeldTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(armBarrier(1))
      .then(receiveFrame(1))
      .then(consultGate(1))
      .then(writeFrame)
      .then(armBarrier(2))
      .expect(coverage.overlappingHold)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .expect(sessions.get(1).held == List(1, 2))
      .then(releaseBarrier(1))
      .then(observeRelease(1))
      .then(consultGate(1))
      .expect(and { sessions.get(1).stage == Holding, sessions.get(1).sent == List() })
      .then(lapseBarrier(2))
      .then(observeRelease(1))
      .then(consultGate(1))
      .then(confirmSentAs(1, false))
      .then(confirmSentAs(1, false))
      .expect(and {
        sessions.get(1).sent == List(1, 2),
        coverage.barrierLapsed,
        coverage.barrierReleased,
      })

  // The only overrun that disconnects: the upstream broadcast channel, not the held
  // buffer. Classified `Lost`, and the record lands.
  run laggedOverrunDisconnectTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(writeFrame)
      .then(writeFrame)
      .then(sourceLagged(1))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).ending == Some(Lost),
        sessions.get(1).cause == Some(ReceiverLagged),
        departure_record == Some(Lost),
        coverage.laggedDisconnect,
        coverage.lostEnd,
      })

  // A granted replay puts `(0, 2]` on the wire once; the live stream redelivers those same
  // frames and the dedup drops them, then carries on from 3.
  run handoffDedupTest: bool =
    init
      .then(writeFrame)
      .then(writeFrame)
      .then(handoffReplayedAs(1, 2, Some(7)))
      .expect(sessions.get(1).sent == List(1, 2))
      .then(receiveFrame(1))
      .expect(and { coverage.handoffDeduped, sessions.get(1).held == List() })
      .then(consultGate(1))
      .then(receiveFrame(1))
      .then(consultGate(1))
      .then(writeFrame)
      .then(receiveFrame(1))
      .expect(sessions.get(1).held == List(3))
      .then(consultGate(1))
      .then(confirmSentAs(1, false))
      .expect(and {
        sessions.get(1).sent == List(1, 2, 3),
        not(defects.resentBelowResume),
      })

  // A handoff cannot replay into an armed window; it waits for the barrier to end.
  run handoffWaitsForReleaseTest: bool =
    init
      .then(armBarrier(1))
      .then(writeFrame)
      .expect(not(canHandoff(sessions.get(1), gateHeld(armed))))
      .then(releaseBarrier(1))
      .then(handoffReplayedAs(1, 1, Some(7)))
      .expect(and {
        sessions.get(1).sent == List(1),
        coverage.handoffAfterRelease,
      })

  // The disclosed divergence, pinned so it cannot change unnoticed: a source close inside
  // an armed window drains the held buffer past the barrier floor before reporting
  // `Graceful`. If the ruling in .scratch/formal-spec/t9b-blocked.md goes the other way,
  // this test is the thing that must change.
  run closeInsideWindowDrainsThenEndsGracefulTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(armBarrier(1))
      .then(writeFrame)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .expect(sessions.get(1).stage == Holding)
      .then(closeSource)
      .then(sourceClosed(1))
      .expect(and {
        sessions.get(1).in_flight == Some(1),
        sessions.get(1).ending == Some(Graceful),
        gateHeld(armed),
      })
      .then(confirmSentAs(1, false))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).sent == List(1),
        departure_record == Some(Graceful),
        coverage.endingDrainedPastBarrierFloor,
        not(defects.gracefulEndDroppedFrames),
        // The carve-out is *reached* here — barrier 1 is still armed on floor 0 and the wire
        // is at 1 — so this is the gated statement that the ending drain (FM-CLUSTER-097's
        // "unless the link is ending" clause, ruling R3) is what the invariant permits.
        // Sampled runs do not reach this state (the witness sits at 0 traces), so without
        // this line the carve-out could be deleted with no oracle noticing; deleting it
        // fails here, which is the alarm the file header promises.
        inv_no_ship_inside_barrier_window,
      })

  // Issue 22: the kick departs `Superseded` and writes nothing.
  run supersededKickWritesNoDepartureTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(handoffReplayedAs(2, 0, Some(7)))
      .then(supersede(2, 1))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).ending == Some(Superseded),
        sessions.get(1).cause == Some(Kicked),
        departure_record == None,
        not(defects.supersededRecordedDeparture),
        coverage.supersededEnd,
      })

  // Issue 21: an ACK above the live offset is refused outright and counted — the watermark
  // does not move to it, and does not move to the live offset either.
  run ackAboveLiveIsIgnoredTest: bool =
    init
      .then(writeFrame)
      .then(handoffReplayedAs(1, 1, Some(7)))
      .then(replicaAck(1))
      .expect(sessions.get(1).acked == 1)
      .then(writeFrame)
      .then(replicaAckAbove(1))
      .expect(and {
        sessions.get(1).acked == 1,
        ignored_acks == 1,
        coverage.ackIgnored,
        not(defects.clampedAck),
      })

  // A failed send abandons the frame *and* the buffer behind it, and the link is Lost —
  // the frame that never landed never enters the wire history.
  run sendFailedAbandonsBufferTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(armBarrier(1))
      .then(writeFrame)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .then(writeFrame)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .expect(sessions.get(1).held == List(1, 2))
      .then(releaseBarrier(1))
      .then(observeRelease(1))
      .then(consultGate(1))
      .expect(and { sessions.get(1).in_flight == Some(1), sessions.get(1).held == List(2) })
      .then(sendFailed(1))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).ending == Some(Lost),
        sessions.get(1).sent == List(),
        sessions.get(1).held == List(),
        departure_record == Some(Lost),
        coverage.sendFailed,
      })

  // A delivery that breaches the lag budget still counts as sent — the frame is on the
  // wire — and the link ends `Lost` at that frame.
  run lagBreachEndsAtTheFrameTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .then(confirmSentAs(1, true))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).sent == List(1),
        sessions.get(1).ending == Some(Lost),
        sessions.get(1).cause == Some(LagBreach),
        coverage.lagBreach,
      })

  // The hold is node-wide, not per-session (FM-CLUSTER-097): one barrier parks every
  // session the node serves.
  run nodeWideHoldTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(handoffReplayedAs(2, 0, Some(8)))
      .then(armBarrier(1))
      .then(writeFrame)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .then(receiveFrame(2))
      .then(consultGate(2))
      .expect(and {
        sessions.get(1).stage == Holding,
        sessions.get(2).stage == Holding,
        coverage.bothSessionsHeld,
      })

  // ===========================================================================
  // Gap-closure tests (T9c mutation battery, lifecycle step 5). Each pins a fact the
  // battery found unpinned: the row ids in the comments are the mutations these kill.
  // See .scratch/formal-spec/2026-08-19-feed-gate-battery.md.
  // ===========================================================================

  // A clean close is not something a session may decide on its own (G09), and it may not
  // decide it while frames it has not consumed are still in the channel (G10). Nor is a
  // `Lost` disconnect available to a session that has not actually been overrun (G12).
  run closeAndLagGuardsRefuseAHealthySessionTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      // Caught up and idle, but the source is open: there is nothing to call a clean end.
      .expect(and {
        sessions.get(1).cursor == live,
        not(canSourceClosed(sessions.get(1), source_closed, live)),
      })
      .then(writeFrame)
      .expect(and {
        not(canSourceClosed(sessions.get(1), source_closed, live)),
        not(canSourceLagged(sessions.get(1), live)),
      })
      .then(closeSource)
      .expect(not(canSourceClosed(sessions.get(1), source_closed, live)))
      .then(receiveFrame(1))
      .then(consultGate(1))
      .then(confirmSentAs(1, false))
      .expect(and {
        sessions.get(1).cursor == live,
        canSourceClosed(sessions.get(1), source_closed, live),
      })

  // Once the receiver is further behind than the channel is deep the frames are gone: the
  // intake is refused (G05) and the lagged disconnect is the only honest transition.
  run channelOverrunRefusesIntakeTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(writeFrame)
      .then(writeFrame)
      .expect(and {
        live - sessions.get(1).cursor > CHANNEL_CAP,
        not(canReceiveFrame(sessions.get(1), live)),
        canSourceLagged(sessions.get(1), live),
      })

  // A session with a frame on the wire is not classified out from under it: the lagged
  // disconnect waits for the send to come back (G11), which is what keeps the in-flight
  // frame from being replaced and lost. And a session that has sent nothing cannot report
  // a failed send (G14).
  run midSendSessionIsNotClassifiedOutFromUnderItsFrameTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(handoffReplayedAs(2, 0, Some(8)))
      .then(writeFrame)
      .then(receiveFrame(1))
      .then(consultGate(1))
      .expect(sessions.get(1).stage == Sending)
      .then(writeFrame)
      .then(writeFrame)
      .then(writeFrame)
      .expect(and {
        live - sessions.get(1).cursor > CHANNEL_CAP,
        not(canSourceLagged(sessions.get(1), live)),
        sessions.get(2).stage == Receiving,
        not(canSendFailed(sessions.get(2))),
      })

  // Supersession is newest-wins and only newest (G19), a session that is gone displaces
  // nobody (G16), and a session ACKs only while it is streaming — not during the handoff
  // replay (G22) and not after it has ended (G23).
  run supersessionAndAckGuardsTest: bool =
    init
      .expect(not(canAck(sessions.get(1))))
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(handoffReplayedAs(2, 0, Some(7)))
      .expect(and {
        canAck(sessions.get(1)),
        not(canSupersede(sessions.get(1), sessions.get(2))),
        canSupersede(sessions.get(2), sessions.get(1)),
      })
      .then(writeFrame)
      .then(writeFrame)
      .then(writeFrame)
      .then(sourceLagged(2))
      .expect(and {
        sessions.get(2).stage == Ended,
        not(canSupersede(sessions.get(2), sessions.get(1))),
        not(canAck(sessions.get(2))),
      })

  // FM-REPLICATION-062's other half: a session entering streaming clears the node's
  // last-departure cell (M13), so a predecessor's `Lost` record does not fence the
  // successor's primary.
  run enteringStreamingClearsTheFenceCellTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(writeFrame)
      .then(writeFrame)
      .then(sourceLagged(1))
      .expect(departure_record == Some(Lost))
      .then(handoffReplayedAs(2, 0, Some(8)))
      .expect(departure_record == None)

  // The graceful path writes its record too (M26) — the case with nothing buffered, where
  // the classification and the end are the same step.
  run gracefulCloseRecordsItsDepartureTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(closeSource)
      .then(sourceClosed(1))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).ending == Some(Graceful),
        sessions.get(1).cause == Some(ClosedByPeer),
        departure_record == Some(Graceful),
        coverage.gracefulEnd,
        not(defects.gracefulEndDroppedFrames),
      })

  // The floor a barrier records is the live offset at arm time — not zero, not the end of
  // the backlog (M01, M02) — an armed barrier is not re-armed onto a higher floor (M03),
  // and the overlapping-hold coverage means what it says (M08).
  run barrierFloorIsTheLiveOffsetAtArmTimeTest: bool =
    init
      .then(writeFrame)
      .then(armBarrier(1))
      .expect(and {
        armed.get(1) == Some(1),
        not(coverage.overlappingHold),
      })
      .then(writeFrame)
      .then(armBarrier(2))
      .expect(and {
        armed.get(1) == Some(1),
        armed.get(2) == Some(2),
        coverage.overlappingHold,
      })
      // `.fail()` is the terminal step of the trace (see the quint-lang tests guideline):
      // re-arming an already-armed barrier is refused.
      .then(armBarrier(1).fail())

  // Coverage that would be spuriously true if the ghosts were widened: a hold that held
  // nothing back is not `heldFrame` (M21), one session holding is not the node-wide case
  // (M22), and a wakeup that finds an empty buffer drained nothing (M24).
  run emptyBufferHoldLatchesNoCoverageTest: bool =
    init
      .then(writeFrame)
      .then(writeFrame)
      .then(handoffReplayedAs(1, 2, Some(7)))
      .then(armBarrier(1))
      .then(receiveFrame(1))
      .expect(and { coverage.handoffDeduped, sessions.get(1).held == List() })
      .then(consultGate(1))
      .expect(and {
        sessions.get(1).stage == Holding,
        not(coverage.heldFrame),
        not(coverage.bothSessionsHeld),
      })
      .then(releaseBarrier(1))
      .then(observeRelease(1))
      .expect(not(coverage.drainedOnRelease))

  // The disclosed carve-out's boundary is strict (M34): a frame that was already written
  // when the barrier armed sits *at* the floor, and shipping it on the way out is ordinary
  // draining, not a drain past the window.
  run drainAtTheFloorIsNotADivergenceTest: bool =
    init
      .then(handoffReplayedAs(1, 0, Some(7)))
      .then(writeFrame)
      .then(armBarrier(1))
      .expect(armed.get(1) == Some(1))
      .then(receiveFrame(1))
      .then(consultGate(1))
      .expect(sessions.get(1).stage == Holding)
      .then(closeSource)
      .then(sourceClosed(1))
      .expect(sessions.get(1).in_flight == Some(1))
      .then(confirmSentAs(1, false))
      .expect(and {
        sessions.get(1).stage == Ended,
        sessions.get(1).sent == List(1),
        not(coverage.endingDrainedPastBarrierFloor),
      })

  // TR-REPLICATION-033: the acked offset is a high-water mark, not the last number the
  // replica said (E33). A late or reordered ACK below the watermark leaves it alone — and
  // that is an ordinary ACK, neither ignored nor clamped.
  run ackWatermarkNeverRetreatsTest: bool =
    init
      .then(writeFrame)
      .then(writeFrame)
      .then(handoffReplayedAs(1, 2, Some(7)))
      .then(replicaAck(1))
      .expect(sessions.get(1).acked == 2)
      .then(replicaAckAs(1, 1))
      .expect(and {
        sessions.get(1).acked == 2,
        ignored_acks == 0,
        not(defects.clampedAck),
      })

  // A closed source produces no more frames (M09).
  run closedSourceAcceptsNoMoreWritesTest: bool =
    init
      .then(writeFrame)
      .then(closeSource)
      .expect(live == 1)
      // `.fail()` is the terminal step: the backlog does not grow after the close.
      .then(writeFrame.fail())

  // The granted replay cannot claim offsets the primary never wrote (M12). The sampled
  // form (`handoffReplayed`) picks `r` from `0.to(live)` and so cannot reach past the head
  // on its own — this is the gated statement that the *guard*, not the sampler, is what
  // forbids it.
  run handoffCannotReplayPastTheLiveHeadTest: bool =
    init
      .expect(live == 0)
      // `.fail()` is the terminal step: the replay is refused, not clamped.
      .then(handoffReplayedAs(1, 1, Some(7)).fail())
}
