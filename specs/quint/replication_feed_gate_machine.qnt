// replication_feed_gate_machine — the state machine of the replica feed-gate / barrier
// session model (formal-spec phase 3, task T9b): the `sessions` map of per-session
// sequencing state, the minimal environment a session runs against (the primary's `live`
// offset, whether the upstream source has closed, the `armed` barrier set the node-wide
// feed gate is derived from), the node-wide `departure_record` of FM-REPLICATION-062, the
// issue-21 `ignored_acks` counter, the mandatory `defects`/`coverage` ghost battery, and
// one guarded action per modeled transition.
//
// Guards and updates themselves live in replication_feed_gate_logic.qnt; the model-wide
// header (rows modeled, projection onto the real seam, deviations) lives in
// replication_feed_gate.qnt.
//
// Single responsibility: the wiring — which variables each transition writes, and which
// nondeterministic choices `step` offers.
//   Belongs here:     `var` declarations, `init`, one `action` per modeled transition,
//                     `stutter`, `step`, and the deterministic `*As` variants tests drive.
//   Does NOT belong:  guard/update logic (replication_feed_gate_logic.qnt) — an action
//                     here should read as "check `can*`, write `apply*`"; invariants,
//                     witnesses and `run` tests (replication_feed_gate.qnt); types and
//                     constants (replication_feed_gate_types.qnt).
module replication_feed_gate_machine {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"
  import replication_feed_gate_types.* from "./replication_feed_gate_types"
  import replication_feed_gate_logic.* from "./replication_feed_gate_logic"

  // One `FeedSequencer` per session, keyed by the session identity the connection runs
  // under. The *announced replica* identity is a field inside (`identity`), not the key:
  // two sessions announcing the same replica is precisely the supersession case.
  var sessions: SessionId -> SessionState

  // The primary's live replication offset (TR-REPLICATION-030: it advances as the backlog
  // is appended). Monotone, bounded by `MAX_WRITES`.
  var live: Offset

  // The upstream frame source has been shut down cleanly. Latches: a closed source never
  // reopens; a session that has not observed it yet still may.
  var source_closed: bool

  // The armed barriers, each recording the live offset at the moment it armed — its
  // *floor*. The floor is what makes "nothing applied during the window reaches the
  // replica" (FM-CLUSTER-097) checkable without a clock: every frame that was already on
  // the wire when the barrier armed is at or below the floor (the live offset only
  // advances), so "some session's wire went past this barrier's floor" is exactly "a frame
  // shipped after this barrier armed".
  //
  // `Some(floor)` = armed, `None` = not armed. The node-wide gate is held iff any entry is
  // `Some` — see `gateHeld` in the logic module for why the real max-of-deadlines fold
  // collapses to that.
  var armed: BarrierId -> Option[Offset]

  // The node-wide "last streaming departure" cell of FM-REPLICATION-062: written when a
  // session ends, cleared as a session enters streaming. Only a `Graceful` record disarms
  // the self-fence, and a `Superseded` departure writes nothing at all (issue 22).
  var departure_record: Option[Departure]

  // How many ACKs were refused for reporting an offset above the live one (issue 21's
  // "ignored entirely **and counted**"). The count is the whole reason ignoring is safe:
  // without it the anomaly would be silent.
  var ignored_acks: int

  // Arrival order among sessions that reached streaming. A model device standing in for
  // "which session announced this identity most recently", deliberately not a timestamp
  // (global no-wall-clock ruling).
  var next_seq: int

  // Ghost flags, set when an action took the *unsafe* variant of a ruled obligation. Never
  // read by a guard; only by invariants. Each is recomputed inside the action that could
  // take the unsafe variant, from that action's own post-state/outcome rather than from
  // its guard, so weakening an update shows up here. Same convention as the cluster
  // models' `defects` records; every field is a `lib_monotone.latch`.
  var defects: {
    // Issue 21 (AMENDED) — an ACK ingest moved the watermark to something *other* than
    // what the replica reported, i.e. clamped instead of ignoring. See `admitAck`.
    clampedAck: bool,
    // Issue 22 (AMENDED) — a superseded session wrote a departure record.
    supersededRecordedDeparture: bool,
    // FM-REPLICATION-015 — the live stream put a frame on the wire that the handoff replay
    // had already sent, i.e. the `resume_offset` dedup let a duplicate through.
    resentBelowResume: bool,
    // TR-REPLICATION-007 — a session reported a `Graceful` departure while it still had
    // buffered or in-flight frames: it called a lossy end clean.
    gracefulEndDroppedFrames: bool,
  }

  // Ghost flags, set when a behaviour the witnesses care about happened. Never read by a
  // guard. Their job is to make "this rule was never exercised" impossible to hide: a
  // witness over each field is what distinguishes a vacuously-true invariant from a
  // tested one.
  var coverage: {
    // A frame was buffered rather than shipped because the gate was held.
    heldFrame: bool,
    // A release wakeup reached a session that had frames waiting — the drain case.
    drainedOnRelease: bool,
    // A barrier armed while another was still armed (the overlapping-hold case that makes
    // the gate a deadline rather than a latch).
    overlappingHold: bool,
    // Every session on the node was holding at once — the node-wide half of
    // FM-CLUSTER-097, invisible to a single-session model.
    bothSessionsHeld: bool,
    // A barrier ended by an explicit release, and (separately) by its deadline lapsing
    // with no explicit release at all.
    barrierReleased: bool,
    barrierLapsed: bool,
    // A handoff replayed its backlog tail only after a barrier had ended — the
    // "PSYNC into the window waits" case of FM-CLUSTER-097.
    handoffAfterRelease: bool,
    // The live stream redelivered a frame the handoff had already replayed, and the dedup
    // dropped it (FM-REPLICATION-015).
    handoffDeduped: bool,
    // Each departure classification was actually reached.
    gracefulEnd: bool,
    lostEnd: bool,
    supersededEnd: bool,
    // The three distinct `Lost` causes, each reached on its own.
    laggedDisconnect: bool,
    sendFailed: bool,
    lagBreach: bool,
    // The disclosed divergence: a session that had already classified its departure
    // drained a frame past an armed barrier's floor. See `inv_no_ship_inside_barrier_window`
    // in replication_feed_gate.qnt and .scratch/formal-spec/t9b-blocked.md.
    endingDrainedPastBarrierFloor: bool,
    // An ACK above the live offset was refused and counted (issue 21).
    ackIgnored: bool,
  }

  // ---------------------------------------------------------------------------
  // Frame keepers — the variables an action leaves alone.
  // ---------------------------------------------------------------------------

  action keepSessions: bool = sessions' = sessions
  action keepEnv: bool = all { live' = live, source_closed' = source_closed, armed' = armed }
  action keepRecord: bool = departure_record' = departure_record
  action keepAcks: bool = ignored_acks' = ignored_acks
  action keepSeq: bool = next_seq' = next_seq
  action keepDefects: bool = defects' = defects
  action keepCoverage: bool = coverage' = coverage

  action init: bool = all {
    sessions' = SESSIONS.mapBy(_ => {
      stage: Handoff, resume_offset: 0, held: List(), in_flight: None,
      ending: None, cause: None, identity: None, stream_seq: 0, cursor: 0,
      accepted: List(), sent: List(), acked: 0,
    }),
    live' = 0,
    source_closed' = false,
    armed' = BARRIERS.mapBy(_ => None),
    departure_record' = None,
    ignored_acks' = 0,
    next_seq' = 1,
    defects' = {
      clampedAck: false, supersededRecordedDeparture: false,
      resentBelowResume: false, gracefulEndDroppedFrames: false,
    },
    coverage' = {
      heldFrame: false, drainedOnRelease: false, overlappingHold: false,
      bothSessionsHeld: false, barrierReleased: false, barrierLapsed: false,
      handoffAfterRelease: false, handoffDeduped: false,
      gracefulEnd: false, lostEnd: false, supersededEnd: false,
      laggedDisconnect: false, sendFailed: false, lagBreach: false,
      endingDrainedPastBarrierFloor: false, ackIgnored: false,
    },
  }

  // ---------------------------------------------------------------------------
  // Environment: the primary and the barriers. None of these is a `FeedInput` — they are
  // what makes the inputs happen.
  // ---------------------------------------------------------------------------

  // TR-REPLICATION-030 — a write lands, the live offset advances, the frame becomes
  // available on the broadcast channel.
  action writeFrame: bool = all {
    live < MAX_WRITES,
    not(source_closed),
    live' = live + 1,
    source_closed' = source_closed,
    armed' = armed,
    keepSessions, keepRecord, keepAcks, keepSeq, keepDefects, keepCoverage,
  }

  action closeSource: bool = all {
    not(source_closed),
    source_closed' = true,
    live' = live,
    armed' = armed,
    keepSessions, keepRecord, keepAcks, keepSeq, keepDefects, keepCoverage,
  }

  // TR-REPLICATION-013 — arming a barrier publishes a feed-gate deadline. The floor
  // recorded here is the live offset at arm time; see `armed`.
  action armBarrier(b: BarrierId): bool = all {
    armed.get(b) == None,
    armed' = armed.set(b, Some(live)),
    live' = live,
    source_closed' = source_closed,
    keepSessions, keepRecord, keepAcks, keepSeq, keepDefects,
    coverage' = { ...coverage,
      overlappingHold: latch(coverage.overlappingHold, gateHeld(armed)),
    },
  }

  // The barrier ends because the work it guarded finished (the handoff completed or
  // aborted).
  action releaseBarrier(b: BarrierId): bool = all {
    armed.get(b) != None,
    armed' = armed.set(b, None),
    live' = live,
    source_closed' = source_closed,
    keepSessions, keepRecord, keepAcks, keepSeq, keepDefects,
    coverage' = { ...coverage, barrierReleased: latch(coverage.barrierReleased, true) },
  }

  // The barrier ends because its published deadline passed with nothing explicitly
  // releasing it — FM-CLUSTER-097's "or the pause simply lapses". A separate action rather
  // than a clock: the model reads no wall clock, so deadline expiry is a nondeterministic
  // event. The gate cannot tell this apart from `releaseBarrier`; coverage can.
  action lapseBarrier(b: BarrierId): bool = all {
    armed.get(b) != None,
    armed' = armed.set(b, None),
    live' = live,
    source_closed' = source_closed,
    keepSessions, keepRecord, keepAcks, keepSeq, keepDefects,
    coverage' = { ...coverage, barrierLapsed: latch(coverage.barrierLapsed, true) },
  }

  // ---------------------------------------------------------------------------
  // Session sequencing. Each action below is one `FeedInput` fed to one `FeedSequencer`;
  // the projection table is in replication_feed_gate.qnt's header.
  // ---------------------------------------------------------------------------

  // `HandoffReplayed { resume_offset }`. Deterministic variant: `r` (how far the granted
  // replay went) and `ident` (the announced replica identity, `None` for an unannounced
  // one) are given rather than picked.
  action handoffReplayedAs(s: SessionId, r: Offset, ident: Option[int]): bool = {
    val ss = sessions.get(s)
    all {
      canHandoff(ss, gateHeld(armed)),
      r <= live,
      sessions' = sessions.set(s, applyHandoff(ss, r, ident, next_seq)),
      next_seq' = next_seq + 1,
      // Entering streaming clears the node's last-departure record (FM-REPLICATION-062).
      departure_record' = None,
      keepEnv, keepAcks, keepDefects,
      coverage' = { ...coverage,
        handoffAfterRelease: latch(coverage.handoffAfterRelease,
          coverage.barrierReleased or coverage.barrierLapsed),
      },
    }
  }

  // The nondeterministic form `step` samples: the granted replay may end anywhere at or
  // below the live offset (`0` = a session that starts from the head), and the announced
  // identity may be either of the two or absent altogether.
  action handoffReplayed(s: SessionId): bool = {
    nondet r = 0.to(live).oneOf()
    nondet ident = IDENTITIES.map(i => Some(i)).union(Set(None)).oneOf()
    handoffReplayedAs(s, r, ident)
  }

  // `Received(frame)` — take the next channel frame in and go consult the gate. Enabled
  // while holding as well as while idle: a hold stops the wire, not the intake.
  action receiveFrame(s: SessionId): bool = {
    val ss = sessions.get(s)
    all {
      canReceiveFrame(ss, live),
      sessions' = sessions.set(s, applyReceiveFrame(ss)),
      keepEnv, keepRecord, keepAcks, keepSeq, keepDefects,
      coverage' = { ...coverage,
        handoffDeduped: latch(coverage.handoffDeduped, dedupsFrame(ss)),
      },
    }
  }

  // `GateHeld(bool)` — the decision point. This is the *only* action that can move a
  // session from "has frames" to "is putting one on the wire", which is what makes "no
  // frame ships without a gate read" a structural property of the model rather than an
  // invariant that has to chase every path.
  action consultGate(s: SessionId): bool = {
    val ss = sessions.get(s)
    val held = gateHeld(armed)
    val post = if (held) applyHold(ss) else applyFlush(ss)
    val newSessions = sessions.set(s, post)
    all {
      canConsult(ss),
      sessions' = newSessions,
      departure_record' = recordFor(departure_record, post),
      keepEnv, keepAcks, keepSeq,
      defects' = { ...defects,
        gracefulEndDroppedFrames:
          latch(defects.gracefulEndDroppedFrames, gracefulLoss(ss, post)),
      },
      coverage' = { ...coverage,
        heldFrame: latch(coverage.heldFrame, held and ss.held.length() > 0),
        bothSessionsHeld: latch(coverage.bothSessionsHeld,
          SESSIONS.forall(x => newSessions.get(x).stage == Holding)),
        gracefulEnd: latch(coverage.gracefulEnd,
          post.stage == Ended and endsGracefully(post)),
      },
    }
  }

  // `Released` — a release wakeup. Sends the session back to the decision point rather
  // than straight to the wire, so a barrier that armed in the meantime is still honoured.
  action observeRelease(s: SessionId): bool = {
    val ss = sessions.get(s)
    all {
      canObserveRelease(ss),
      sessions' = sessions.set(s, { ...ss, stage: Consulting }),
      keepEnv, keepRecord, keepAcks, keepSeq, keepDefects,
      coverage' = { ...coverage,
        drainedOnRelease: latch(coverage.drainedOnRelease,
          ss.held.length() > 0 and not(gateHeld(armed))),
      },
    }
  }

  // `SourceClosed` — classify `Graceful` (TR-REPLICATION-007), then drain.
  action sourceClosed(s: SessionId): bool = {
    val ss = sessions.get(s)
    val post = applyClassify(ss, Graceful, ClosedByPeer)
    all {
      canSourceClosed(ss, source_closed, live),
      sessions' = sessions.set(s, post),
      departure_record' = recordFor(departure_record, post),
      keepEnv, keepAcks, keepSeq,
      defects' = { ...defects,
        gracefulEndDroppedFrames:
          latch(defects.gracefulEndDroppedFrames, gracefulLoss(ss, post)),
      },
      coverage' = { ...coverage,
        gracefulEnd: latch(coverage.gracefulEnd,
          post.stage == Ended and endsGracefully(post)),
      },
    }
  }

  // `SourceLagged` — the broadcast channel overran this receiver. Classify `Lost`, then
  // drain whatever is still buffered. Note what this is *not*: there is no hold-overflow
  // disconnect anywhere in this model, because the held buffer has no bound. This is the
  // only overrun that ends a link.
  action sourceLagged(s: SessionId): bool = {
    val ss = sessions.get(s)
    val post = applyClassify(ss, Lost, ReceiverLagged)
    all {
      canSourceLagged(ss, live),
      sessions' = sessions.set(s, post),
      departure_record' = recordFor(departure_record, post),
      keepEnv, keepAcks, keepSeq, keepDefects,
      coverage' = { ...coverage,
        laggedDisconnect: latch(coverage.laggedDisconnect, true),
        lostEnd: latch(coverage.lostEnd, post.stage == Ended),
      },
    }
  }

  // `Sent { lag_breached }` — the in-flight frame landed. Deterministic variant: whether
  // the delivery breached the connection's lag budget is given rather than picked.
  action confirmSentAs(s: SessionId, breach: bool): bool = {
    val ss = sessions.get(s)
    val post = applySent(ss, breach)
    val shipped = ss.in_flight
    all {
      canConfirmSent(ss),
      sessions' = sessions.set(s, post),
      departure_record' = recordFor(departure_record, post),
      keepEnv, keepAcks, keepSeq,
      defects' = { ...defects,
        resentBelowResume: latch(defects.resentBelowResume, resentBelowResume(post)),
        gracefulEndDroppedFrames:
          latch(defects.gracefulEndDroppedFrames, gracefulLoss(ss, post)),
      },
      coverage' = { ...coverage,
        lagBreach: latch(coverage.lagBreach, breach),
        gracefulEnd: latch(coverage.gracefulEnd,
          post.stage == Ended and endsGracefully(post)),
        lostEnd: latch(coverage.lostEnd, post.stage == Ended and breach),
        endingDrainedPastBarrierFloor: latch(coverage.endingDrainedPastBarrierFloor,
          isEnding(ss) and BARRIERS.exists(b =>
            optExists(armed.get(b), f => optExists(shipped, o => o > f)))),
      },
    }
  }

  action confirmSent(s: SessionId): bool = {
    nondet breach = Set(true, false).oneOf()
    confirmSentAs(s, breach)
  }

  // `SendFailed` — the frame never landed, so it never enters the wire history, and the
  // buffer behind it goes with the link. `Lost`, always (TR-REPLICATION-007).
  action sendFailed(s: SessionId): bool = {
    val ss = sessions.get(s)
    val post = applyEnd(ss, Lost, SendFailure)
    all {
      canSendFailed(ss),
      sessions' = sessions.set(s, post),
      departure_record' = recordFor(departure_record, post),
      keepEnv, keepAcks, keepSeq, keepDefects,
      coverage' = { ...coverage,
        sendFailed: latch(coverage.sendFailed, true),
        lostEnd: latch(coverage.lostEnd, true),
      },
    }
  }

  // The ruled issue-22 kick: a newer session announcing the same replica identity displaces
  // an older one. Not a `FeedInput` in the seam's table — the predecessor's driver ends the
  // link out of band — but it is a departure, and the whole point of the ruling is *which*
  // departure it is: `Superseded`, writing no record.
  action supersede(newer: SessionId, older: SessionId): bool = {
    val nss = sessions.get(newer)
    val oss = sessions.get(older)
    val post = applyEnd(oss, Superseded, Kicked)
    val newRecord = recordFor(departure_record, post)
    all {
      newer != older,
      canSupersede(nss, oss),
      sessions' = sessions.set(older, post),
      departure_record' = newRecord,
      keepEnv, keepAcks, keepSeq,
      defects' = { ...defects,
        supersededRecordedDeparture:
          latch(defects.supersededRecordedDeparture, newRecord != departure_record),
      },
      coverage' = { ...coverage,
        supersededEnd: latch(coverage.supersededEnd, true),
      },
    }
  }

  // ---------------------------------------------------------------------------
  // ACK ingest (TR-REPLICATION-018 as amended by issue 21, TR-REPLICATION-033).
  // ---------------------------------------------------------------------------

  // Deterministic variant: the replica reports `reported`. Written as the general rule
  // rather than as two special cases, so a mutation that turns the refusal into a clamp is
  // visible in `defects.clampedAck` rather than being unrepresentable.
  action replicaAckAs(s: SessionId, reported: Offset): bool = {
    val ss = sessions.get(s)
    val admitted = admitAck(ss, reported, live)
    all {
      canAck(ss),
      reported >= 0,
      sessions' = sessions.set(s, { ...ss, acked: admitted }),
      ignored_acks' = if (ackIgnored(reported, live)) ignored_acks + 1 else ignored_acks,
      keepEnv, keepRecord, keepSeq,
      defects' = { ...defects,
        // The watermark moved to something the replica never reported: that is a clamp,
        // and a clamp makes the primary a writer of the replica's acked offset.
        clampedAck: latch(defects.clampedAck,
          admitted != ss.acked and admitted != reported),
      },
      coverage' = { ...coverage,
        ackIgnored: latch(coverage.ackIgnored, ackIgnored(reported, live)),
      },
    }
  }

  // An honest replica ACKs the offset it landed — the far end of its wire, never the live
  // offset it has not seen (TR-REPLICATION-033).
  action replicaAck(s: SessionId): bool = replicaAckAs(s, wireMax(sessions.get(s)))

  // A wire-hostile ACK: an offset above the live one, which nothing could have landed.
  // Issue 21 rules it ignored and counted.
  action replicaAckAbove(s: SessionId): bool = replicaAckAs(s, live + 1)

  action stutter: bool = all {
    keepSessions, keepEnv, keepRecord, keepAcks, keepSeq, keepDefects, keepCoverage,
  }

  action step: bool = {
    nondet s = SESSIONS.oneOf()
    nondet t = SESSIONS.oneOf()
    nondet b = BARRIERS.oneOf()
    any {
      handoffReplayed(s),
      receiveFrame(s),
      consultGate(s),
      observeRelease(s),
      sourceClosed(s),
      sourceLagged(s),
      confirmSent(s),
      sendFailed(s),
      supersede(s, t),
      replicaAck(s),
      replicaAckAbove(s),
      writeFrame,
      closeSource,
      armBarrier(b),
      releaseBarrier(b),
      lapseBarrier(b),
      stutter,
    }
  }
}
