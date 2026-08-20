// replication_feed_gate_logic — guards and updates for the replica feed-gate / barrier
// session model (formal-spec phase 3, task T9b). The functional half: one `can*` guard and
// one `apply*` update per modeled transition, plus the shape predicates the invariants in
// replication_feed_gate.qnt are stated with.
//
// Rows whose logic lives here: TR-REPLICATION-013 (the feed-gate hold/release the
// `gateHeld` read decides), TR-REPLICATION-007 and FM-REPLICATION-062 (departure
// classification, `applyClassify`/`applyEnd`), FM-REPLICATION-015 (the replay dedup in
// `applyReceiveFrame`), TR-REPLICATION-018 and TR-REPLICATION-033 (`admitAck`/`ackIgnored`,
// the issue-21 ignore-and-count rule), FM-CLUSTER-097 (the node-wide hold `gateHeld`
// folds every armed barrier into).
//
// Single responsibility: pure computation. Nothing here reads or writes a `var`; every
// definition takes the state it needs as an argument and returns a value, so each can be
// exercised in the REPL against a hand-built state.
//   Belongs here:     `pure def` guards (`can*`), `pure def` updates (`apply*`), and the
//                     pure shape predicates (`isPrefix`, `strictlyIncreasing`, ...).
//   Does NOT belong:  `var`s, `action`s, `init`/`step` (replication_feed_gate_machine.qnt);
//                     invariants, witnesses and `run` tests (replication_feed_gate.qnt);
//                     types and sizing constants (replication_feed_gate_types.qnt).
module replication_feed_gate_logic {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"
  import replication_feed_gate_types.* from "./replication_feed_gate_types"

  // ---------------------------------------------------------------------------
  // The gate (TR-REPLICATION-013, FM-CLUSTER-097).
  // ---------------------------------------------------------------------------

  // Is the node-wide feed gate holding right now?
  //
  // The real gate stores an `Instant` — the latest deadline any armed barrier published —
  // and `decide_hold(published, now)` answers "is that deadline still in the future?".
  // With the clock removed (the campaign's global no-wall-clock ruling: this model reads no
  // wall clock, and the driver hands the sequencer the answer as a `GateHeld` input),
  // "some published deadline is still in force" *is* "some barrier is still armed" — the
  // max-of-deadlines fold collapses to a non-emptiness test over the armed set. That is
  // why a barrier that ends by its deadline lapsing (`lapseBarrier` in the machine) is the
  // same disarm as one that ends by the handoff completing: the model keeps them distinct
  // actions so coverage can tell them apart, not because the gate can.
  //
  // It is a *node-wide* read, not per-session: FM-CLUSTER-097 holds the feed for every
  // replica the node serves, which is the fact a one-session model could not express.
  pure def gateHeld(armed: BarrierId -> Option[Offset]): bool =
    BARRIERS.exists(b => armed.get(b) != None)

  // ---------------------------------------------------------------------------
  // Session-state reads.
  // ---------------------------------------------------------------------------

  // A departure has been classified but not yet reported — the session is draining toward
  // its end. TR-REPLICATION-007's "classified before it leaves the registry" is exactly
  // this gap being non-empty.
  pure def isEnding(ss: SessionState): bool = ss.ending != None

  pure def endsGracefully(ss: SessionState): bool = optExists(ss.ending, d => d == Graceful)

  // This session announced replica identity `i`. An *unannounced* identity (`None`) matches
  // nothing, including another unannounced session: the issue-22 amendment rules that
  // dedup keys on the announced replica/node id, so a session that announced nothing can
  // neither supersede nor be superseded.
  pure def announced(ss: SessionState, i: int): bool = optExists(ss.identity, x => x == i)

  // The furthest offset this session has put on the wire (0 if it has sent nothing).
  pure def wireMax(ss: SessionState): Offset = ss.sent.foldl(0, (a, x) => max2(a, x))

  // ---------------------------------------------------------------------------
  // Pure shape predicates the invariants are stated with.
  // ---------------------------------------------------------------------------

  // `short` is an exact prefix of `long`. Stating FIFO this way collapses three separate
  // claims into one: nothing on the wire was skipped (every prefix position matches),
  // nothing was reordered (positions match *in order*), and nothing was duplicated (a
  // duplicate would shift every later position).
  pure def isPrefix(short: List[Offset], long: List[Offset]): bool = and {
    short.length() <= long.length(),
    short.indices().forall(i => short.nth(i) == long.nth(i)),
  }

  pure def strictlyIncreasing(l: List[Offset]): bool =
    l.indices().forall(i => i == 0 or l.nth(i - 1) < l.nth(i))

  // ---------------------------------------------------------------------------
  // Terminal transitions (TR-REPLICATION-007, FM-REPLICATION-062).
  // ---------------------------------------------------------------------------

  // End the link, keeping whatever cause was already recorded. The buffer is dropped: past
  // this point there is no link to drain it onto.
  pure def applyEndWith(ss: SessionState, d: Departure): SessionState =
    { ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }

  // End the link, classifying it in the same step — the paths where the input that ends
  // the session and the reason it ended are the same event (a failed send, a frame that
  // breached the lag budget, a supersession).
  pure def applyEnd(ss: SessionState, d: Departure, c: EndCause): SessionState =
    applyEndWith({ ...ss, cause: Some(c) }, d)

  // The seam's `flush()`: hand the oldest held frame to the transport if there is one;
  // otherwise honour a pending departure; otherwise go back to waiting for frames.
  //
  // The ordering is the load-bearing part — a classified departure is reported only once
  // the buffer is empty, which is what "flush before you end" means. It is also where this
  // model's one known spec/seam divergence lives: reaching `applyFlush` with a departure
  // pending does not re-read the gate, so a link that ends inside a barrier window drains
  // past the barrier floor. See `inv_no_ship_inside_barrier_window` in
  // replication_feed_gate.qnt and .scratch/formal-spec/t9b-blocked.md.
  pure def applyFlush(ss: SessionState): SessionState =
    if (ss.held.length() > 0)
      { ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held.tail() }
    else
      match ss.ending {
        | Some(d) => applyEndWith(ss, d)
        | None => { ...ss, stage: Receiving, in_flight: None }
      }

  // Classify now, report later: record the departure and its cause, then drain.
  pure def applyClassify(ss: SessionState, d: Departure, c: EndCause): SessionState =
    applyFlush({ ...ss, ending: Some(d), cause: Some(c) })

  // The departure class each input class must produce (TR-REPLICATION-007: a departure is
  // *classified*, and the classification is a function of what happened, not a default).
  // Three distinct causes fold to `Lost`, which is why the model carries the cause
  // alongside the departure — a `Departure` alone cannot say whether the classification
  // matched its input.
  pure def expectedDeparture(c: EndCause): Departure =
    match c {
      | ClosedByPeer => Graceful
      | ReceiverLagged => Lost
      | SendFailure => Lost
      | LagBreach => Lost
      | Kicked => Superseded
    }

  // The node-wide "last streaming departure" cell of FM-REPLICATION-062, after this
  // session's transition. Two rules in one place:
  //   - A record is written only when a session actually reaches `Ended` — the
  //     classification that happens earlier (TR-REPLICATION-007) is not yet a record, so a
  //     session draining toward its departure has not fenced anything.
  //   - A `Superseded` departure writes **no** record (issue 22, AMENDED). This session did
  //     not fail; it was replaced. Its record would land after the successor's
  //     enter-streaming clear and fence a primary whose replica is in fact healthy.
  pure def recordFor(prev: Option[Departure], post: SessionState): Option[Departure] =
    if (post.stage != Ended) prev else
      match post.ending {
        | Some(d) => if (d == Superseded) prev else Some(d)
        | None => prev
      }

  // ---------------------------------------------------------------------------
  // Handoff (FM-REPLICATION-015).
  // ---------------------------------------------------------------------------

  // A session may replay the backlog tail it was granted only while the gate is open.
  // FM-CLUSTER-097: "a replica that PSYNCs *into* the window waits before it replays the
  // backlog tail" — the seam's `start()` returns `AwaitRelease` for exactly this reason.
  pure def canHandoff(ss: SessionState, held: bool): bool = and {
    ss.stage == Handoff,
    not(held),
  }

  // The granted replay, modeled as one atomic step: everything up to `r` reaches the wire,
  // and `resume_offset` records it so the live stream does not send any of it a second
  // time. Modeling the replay atomically (rather than frame by frame) is deliberate — the
  // gate decision that matters is the one taken *before* the replay starts, and that is
  // `canHandoff`'s guard.
  pure def applyHandoff(ss: SessionState, r: Offset, ident: Option[int], seq: int): SessionState =
    { ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,
      accepted: range(1, r + 1), sent: range(1, r + 1) }

  // ---------------------------------------------------------------------------
  // The live stream.
  // ---------------------------------------------------------------------------

  // A frame can be taken from the upstream broadcast channel while the session is idle or
  // holding — holding stops the *wire*, never the intake (FM-CLUSTER-097: the replica
  // "converges on everything held", so everything must still be taken in). The
  // `CHANNEL_CAP` conjunct is the other side of `canSourceLagged`: once the receiver is
  // further behind than the channel is deep, the frames are gone and the only honest
  // transition is the lagged one.
  pure def canReceiveFrame(ss: SessionState, live: Offset): bool = and {
    ss.stage == Receiving or ss.stage == Holding,
    ss.cursor < live,
    live - ss.cursor <= CHANNEL_CAP,
  }

  // Take the next frame in, then go consult the gate. The dedup filter is
  // FM-REPLICATION-015's: a frame at or below `resume_offset` is already on the wire from
  // the handoff replay, so it is consumed and dropped rather than buffered — it never
  // enters `accepted`, and so can never reach `sent`.
  pure def applyReceiveFrame(ss: SessionState): SessionState = {
    val f = ss.cursor + 1
    val advanced = { ...ss, cursor: f, stage: Consulting }
    if (f > ss.resume_offset)
      { ...advanced, held: ss.held.append(f), accepted: ss.accepted.append(f) }
    else
      advanced
  }

  // Was this frame dropped as an already-replayed duplicate?
  pure def dedupsFrame(ss: SessionState): bool = ss.cursor + 1 <= ss.resume_offset

  pure def canConsult(ss: SessionState): bool = ss.stage == Consulting

  // The gate said "held": park, and wait for either another frame or a release wakeup.
  pure def applyHold(ss: SessionState): SessionState = { ...ss, stage: Holding }

  // A release wakeup. Deliberately not guarded on the gate actually being open: the real
  // release is a notification, and by the time the session acts on it another barrier may
  // have armed. The session re-reads the gate from `Consulting` rather than trusting the
  // wakeup, which is the behaviour that makes overlapping holds safe.
  pure def canObserveRelease(ss: SessionState): bool = ss.stage == Holding

  // The upstream source closed cleanly. Requiring the cursor to have caught up models the
  // broadcast channel reporting `Closed` only once its buffer has drained — without it the
  // model would let a session call a departure `Graceful` while frames it never consumed
  // were still in the channel, which is a modeling artifact rather than a system behaviour.
  pure def canSourceClosed(ss: SessionState, closed: bool, live: Offset): bool = and {
    ss.stage == Receiving or ss.stage == Holding,
    closed,
    ss.cursor == live,
  }

  // The receiver fell further behind than the channel is deep: frames are gone, so the
  // link is Lost, not Graceful (TR-REPLICATION-007). This — not any bound on the held
  // buffer — is the only overrun that disconnects a replica.
  pure def canSourceLagged(ss: SessionState, live: Offset): bool = and {
    ss.stage == Receiving or ss.stage == Holding,
    live - ss.cursor > CHANNEL_CAP,
  }

  pure def canConfirmSent(ss: SessionState): bool = and {
    ss.stage == Sending,
    ss.in_flight != None,
  }

  // The in-flight frame landed. `breach` is the transport reporting that the write, though
  // delivered, put the connection over its lag budget: the frame counts as sent (it is on
  // the wire) and the link ends Lost at that frame.
  pure def applySent(ss: SessionState, breach: bool): SessionState = {
    val landed = match ss.in_flight {
      | Some(o) => ss.sent.append(o)
      | None => ss.sent
    }
    val after = { ...ss, sent: landed, in_flight: None }
    if (breach) applyEnd(after, Lost, LagBreach) else applyFlush(after)
  }

  // The send failed: the in-flight frame did NOT land, so it never enters `sent`, and the
  // buffer behind it is abandoned with the link.
  pure def canSendFailed(ss: SessionState): bool = ss.stage == Sending

  // ---------------------------------------------------------------------------
  // Supersession (replication issue 22, AMENDED).
  // ---------------------------------------------------------------------------

  // A newer session displaces an older one announcing the same replica identity. Ordering
  // is by arrival sequence, never by a timestamp (global no-wall-clock ruling). Both
  // sessions must be past handoff and still live: a session still replaying has not
  // claimed the identity yet, and an ended one has nothing left to displace.
  pure def canSupersede(newer: SessionState, older: SessionState): bool = and {
    newer.stage != Handoff, newer.stage != Ended,
    older.stage != Handoff, older.stage != Ended,
    newer.stream_seq > older.stream_seq,
    optExists(newer.identity, i => announced(older, i)),
  }

  // ---------------------------------------------------------------------------
  // ACK ingest (TR-REPLICATION-018 as amended by issue 21, TR-REPLICATION-033).
  // ---------------------------------------------------------------------------

  // An ACK strictly above the live offset is impossible from an honest replica: nothing
  // above the live offset exists to have landed.
  pure def ackIgnored(reported: Offset, live: Offset): bool = reported > live

  // The ruled ingest rule: an impossible ACK is **ignored entirely** — the watermark does
  // not move at all — and counted, rather than clamped to the live offset. Issue 21's
  // amendment reversed the clamp precisely because clamping makes the primary a *writer*
  // of the replica's acked offset: a clamped ACK is indistinguishable from a real one, so
  // a WAIT could be satisfied by a number the primary invented. Ignoring keeps the
  // watermark a pure function of what the replica actually reported, and the count is what
  // makes the anomaly visible instead of silent.
  //
  // Everything else is the ordinary never-retreating watermark of TR-REPLICATION-033.
  pure def admitAck(ss: SessionState, reported: Offset, live: Offset): Offset =
    if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)

  // A session ACKs only once it is streaming; an ended one has no connection to ACK over.
  pure def canAck(ss: SessionState): bool = and {
    ss.stage != Handoff,
    ss.stage != Ended,
  }

  // ---------------------------------------------------------------------------
  // Outcome predicates the ghost battery is recomputed from.
  // ---------------------------------------------------------------------------

  // A graceful end that left frames behind: the session reported "clean shutdown" while
  // data it had accepted was still undelivered. Read from the transition (pre-state buffer,
  // post-state outcome), never from a guard — a weakened `applyFlush` that ends without
  // draining shows up here.
  //
  // Two ways to leave data behind, and the second needs care: an in-flight frame is only
  // *lost* if it did not reach the wire in this same step. The ordinary `Sent` path ends a
  // session with a frame in flight on the way in and that frame appended to `sent` on the
  // way out, which is a clean end, not a drop — hence the `sent` length comparison rather
  // than a bare `in_flight != None`.
  pure def gracefulLoss(pre: SessionState, post: SessionState): bool = and {
    post.stage == Ended,
    endsGracefully(post),
    or {
      pre.held.length() > 0,
      pre.in_flight != None and post.sent.length() == pre.sent.length(),
    },
  }

  // A frame reached the wire on the live path even though the handoff replay had already
  // put it there (FM-REPLICATION-015's "once"). Recomputed from the post-state wire.
  pure def resentBelowResume(post: SessionState): bool =
    post.sent.select(o => o <= post.resume_offset).length() > post.resume_offset
}
