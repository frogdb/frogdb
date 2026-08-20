// Full-sync / PSYNC handoff — state machine.
//
// State variables, `init`, the guarded actions, and the defect/coverage ghosts.
// Properties live in the main module `replication_fullsync`; the pure decisions
// these actions are built from live in `replication_fullsync_logic`.
//
// Rows driving the actions here:
//   TR-REPLICATION-001  psyncRequest: the arm decision
//   TR-REPLICATION-002  spliceStream: backlog tail + ResumeOffset seed
//   TR-REPLICATION-003  cutCheckpoint: the checkpoint cut at a captured offset
//   TR-REPLICATION-005  deliverPayload: the trailer install
//   TR-REPLICATION-006  abandonSplice: failure before Streaming, no stream
//   TR-REPLICATION-014  receiveFrame / applyFrame: the frame consumer
//   TR-REPLICATION-015  deliverPayload: absolute offset pair installed
//   TR-REPLICATION-016  promote: the window freezes at the applied offset
//   TR-REPLICATION-018  recordAck / ignoreAck: the issue-21 ruling
//   TR-REPLICATION-019  psyncRequest: +CONTINUE shifts the inherited id
//   TR-REPLICATION-020  deliverPayload: the trailer clears the failover window
//   TR-REPLICATION-021  restartNode: identity gated on intact recovery
//   TR-REPLICATION-022  psyncRequest: dedup on the announced identity
//   TR-REPLICATION-030  writeOnPrimary: the write path advances the live offset
//   FM-REPLICATION-004  cutCheckpoint: the acked-write-loss ghost
//   FM-REPLICATION-012  spliceStream / abandonSplice: contiguous or not at all
//   FM-REPLICATION-016  evictBacklog: eviction raises the floor
//   FM-REPLICATION-019  promote: mint, freeze, re-arm — in one step
//   FM-REPLICATION-065  redeliverFrame: the receiver-side skip (R7)
//   INV-SESSION-1       a session belongs to exactly one replica link
//
// Ghost conventions (phase-2 plan): `defects` latches when the *unsafe* variant
// of a ruled precondition would have been taken, and is never read by a guard —
// only by invariants. Each flag is recomputed inside the action that could take
// the unsafe variant, from that action's own *outcome*, so mutating the guard
// shows up here. `coverage` latches when a behaviour a witness cares about
// actually happened.
module replication_fullsync_machine {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"
  import replication_fullsync_types.* from "./replication_fullsync_types"
  import replication_fullsync_logic.* from "./replication_fullsync_logic"

  var nodes: NodeId -> NodeState

  // Session slots are total: a slot in phase `Disconnected` is free. Keeping the
  // map total avoids `Option` juggling on every read; the registry semantics are
  // unchanged because a disconnected session is exactly one the primary no
  // longer serves.
  var sessions: SessionId -> Session

  var ctl: Ctl

  var defects: {
    // TR-REPLICATION-018 / INV-OFFSET-3: AckedOffset written above the head.
    ackAboveHead: bool,
    // Issue-21 amendment: the ignore branch wrote AckedOffset anyway (the
    // reversed clamp). Latches on a clamping mutation.
    ackWrittenOnIgnore: bool,
    // FM-REPLICATION-004: the payload covers less than the granted offset — the
    // historical missing-WAL-drain shape, an acked write in neither the payload
    // nor the replayed range.
    cutBelowGrant: bool,
    // FM-REPLICATION-015: a streamed frame landed past the end of the dataset.
    spliceGap: bool,
    // A streamed frame landed with different content on a position at or below
    // the offset the grant claims agreement on — two histories disagreeing
    // inside the window they both assert.
    spliceDiverged: bool,
    // R5 (2026-08-20 ruling): a streamed frame landed with different content on
    // a position *above* the grant's claim — a forked tail that survived into
    // the splice. Before the ruling this was lawful correction and lived in
    // `coverage`; `truncateAboveClaim` now removes the class at accept time, so
    // reaching it means the truncation did not happen.
    forkedTailApplied: bool,
    // R4 (2026-08-20 ruling): re-delivery of a position the replica already
    // holds must be a no-op. Latches if a `Reapplied` landing changed the data.
    reapplyChangedData: bool,
    // R4: the applied position never regresses on a frame landing.
    replayRegressedApplied: bool,
    // R7 (2026-08-19 ruling) / FM-REPLICATION-065: a frame the replica's
    // applied head already covers was not disposed of as a skip — it landed,
    // or it was dropped without being counted.
    skippedFrameLanded: bool,
    // FM-REPLICATION-012 / -014: a partial grant issued below the armed floor.
    grantBelowFloor: bool,
    // FM-REPLICATION-013: a partial grant issued over a history the primary
    // does not head and does not hold a window for.
    grantForeignHistory: bool,
    // FM-REPLICATION-020 / issue-24: an identity write that did not carry its
    // paired offset/window in the same step.
    tornIdentity: bool,
    // Issue-24 amendment point 2: the recovered offset did not commit with the
    // data it names.
    restartOffsetTorn: bool,
    // Issue-22 amendment: a superseded predecessor recorded a departure.
    supersededDeparture: bool,
  }

  var coverage: {
    overshipped: bool,
    reapplied: bool,
    // R5: a partial-resync accept dropped at least one position the replica
    // held above its claim — the FM-REPLICATION-004 overshipped tail being
    // discarded before the splice rather than overwritten after it.
    overshipTruncated: bool,
    spliceAbandoned: bool,
    partialViaSecondary: bool,
    evictionForcedFull: bool,
    ackIgnored: bool,
    // R7: the replica ignored a frame its applied head already covered.
    frameSkipped: bool,
    superseded: bool,
    settleDiscardedFrames: bool,
    uncleanRestartReminted: bool,
    tailApplied: bool,
    corpseSession: bool,
    fullSyncStreaming: bool,
  }

  // --- Helpers ---

  pure def linkIs(r: NodeState, sid: SessionId): bool =
    optExists(r.link, k => k == sid)

  pure def freshSession(p: NodeId, r: NodeId): Session = {
    primary: p, replica: r, identity: None, phase: Disconnected, arm: FullSnapshot,
    granted_gen: NO_HISTORY, grant_offset: 0, payload: List(), payload_cut: false,
    acked: 0, resume_offset: 0,
  }

  // The (generation, live offset, role) shadow the pairing invariant compares
  // the next state against. Refreshed by every action, so it always holds the
  // immediately preceding state.
  def pairSnapshot: NodeId -> { gen: Gen, off: Offset, primary: bool } =
    NODES.mapBy(n => {
      gen: nodes.get(n).replid,
      off: nodes.get(n).recv,
      primary: nodes.get(n).role == Primary,
    })

  def ctlNextD(dw: int, dg: int, dd: int): Ctl = {
    next_write: ctl.next_write + dw,
    next_gen: ctl.next_gen + dg,
    prev_pair: pairSnapshot,
    disrupts_left: ctl.disrupts_left - dd,
  }

  def ctlNext(dw: int, dg: int): Ctl = ctlNextD(dw, dg, 0)

  def freeSids: Set[SessionId] =
    SIDS.filter(k => sessions.get(k).phase == Disconnected)

  // Every session slot the node `n` participates in, cleared. A restart drops
  // the whole registry it owned and every link into it.
  def sessionsWithout(n: NodeId): SessionId -> Session =
    SIDS.mapBy(k => {
      val s = sessions.get(k)
      if (s.primary == n or s.replica == n) { ...s, phase: Disconnected } else s
    })

  def unlinkedFrom(base: NodeId -> NodeState, n: NodeId): NodeId -> NodeState =
    NODES.mapBy(m => {
      val nm = base.get(m)
      val dead = optExists(nm.link, k => {
        val s = sessions.get(k)
        s.primary == n or s.replica == n
      })
      if (dead) { ...nm, link: None } else nm
    })

  // --- Frame keepers ---

  action keepNodes: bool = nodes' = nodes
  action keepSessions: bool = sessions' = sessions
  action keepCtl: bool = ctl' = ctlNext(0, 0)
  action keepDefects: bool = defects' = defects
  action keepCoverage: bool = coverage' = coverage

  // --- init ---

  action init: bool = {
    val n0 = NODES.mapBy(n =>
      if (n == INITIAL_PRIMARY)
        { role: Primary, data: List(), replid: FIRST_GEN, replid2: None,
          second_offset: NO_WINDOW, recv: 0, applied: 0, floor: UNARMED,
          link: None, last_departure: None, acks_ignored: 0,
          frames_skipped: 0 }
      else
        { role: Replica, data: List(), replid: NO_HISTORY, replid2: None,
          second_offset: NO_WINDOW, recv: 0, applied: 0, floor: UNARMED,
          link: None, last_departure: None, acks_ignored: 0,
          frames_skipped: 0 })
    all {
      nodes' = n0,
      sessions' = SIDS.mapBy(_ => freshSession(INITIAL_PRIMARY, INITIAL_PRIMARY)),
      ctl' = {
        next_write: 1,
        next_gen: FIRST_GEN + 1,
        prev_pair: NODES.mapBy(n => {
          gen: n0.get(n).replid, off: n0.get(n).recv,
          primary: n0.get(n).role == Primary,
        }),
        disrupts_left: MAX_DISRUPTS,
      },
      defects' = {
        ackAboveHead: false, ackWrittenOnIgnore: false, cutBelowGrant: false,
        spliceGap: false, spliceDiverged: false, grantBelowFloor: false,
        grantForeignHistory: false, tornIdentity: false, restartOffsetTorn: false,
        supersededDeparture: false, forkedTailApplied: false,
        reapplyChangedData: false, replayRegressedApplied: false,
        skippedFrameLanded: false,
      },
      coverage' = {
        overshipped: false, reapplied: false, overshipTruncated: false,
        frameSkipped: false,
        spliceAbandoned: false,
        partialViaSecondary: false, evictionForcedFull: false, ackIgnored: false,
        superseded: false, settleDiscardedFrames: false,
        uncleanRestartReminted: false, tailApplied: false, corpseSession: false,
        fullSyncStreaming: false,
      },
    }
  }

  // --- Primary write path (TR-REPLICATION-030) ---

  action writeOnPrimaryAs(p: NodeId): bool = all {
    nodes.get(p).role == Primary,
    ctl.next_write <= MAX_WRITES,
    nodes' = nodes.set(p, applyWriteOnPrimary(nodes.get(p), ctl.next_write)),
    keepSessions,
    ctl' = ctlNext(1, 0),
    keepDefects,
    keepCoverage,
  }

  // --- Backlog eviction (FM-REPLICATION-016) ---

  action evictBacklogAs(p: NodeId): bool = all {
    canEvictBacklog(nodes.get(p)),
    nodes' = nodes.set(p, applyEvictBacklog(nodes.get(p))),
    keepSessions,
    keepCtl,
    keepDefects,
    keepCoverage,
  }

  // --- PSYNC (TR-REPLICATION-001/019/022, FM-REPLICATION-013) ---

  action psyncRequestAs(r: NodeId, p: NodeId, announced: bool, sid: SessionId): bool = {
    val rn = nodes.get(r)
    val pn = nodes.get(p)
    val arm = decideArm(pn, rn)
    // Issue-22 ruling: a duplicate registration for an *announced* identity kicks
    // the predecessor with a `Superseded` outcome — the session goes, and no
    // departure is recorded. Dedup is keyed on the replica/node identity, never
    // on an address; an unannounced peer (`listening_port == 0`) is exempt.
    val victims = SIDS.filter(k => and {
      announced,
      k != sid,
      sessions.get(k).phase != Disconnected,
      sessions.get(k).primary == p,
      sessions.get(k).identity == Some(r),
    })
    val granted = {
      primary: p,
      replica: r,
      identity: if (announced) Some(r) else None,
      // A partial grant needs no checkpoint; a full snapshot starts at the
      // checkpoint-preparation phase.
      phase: if (arm == PartialGrant) Connecting else PreparingCheckpoint,
      arm: arm,
      granted_gen: pn.replid,
      // FM-REPLICATION-004: on the full arm the offset is captured *here*,
      // before anything is cut or exported.
      grant_offset: if (arm == PartialGrant) rn.applied else pn.recv,
      payload: List(),
      payload_cut: false,
      acked: 0,
      resume_offset: 0,
    }
    val rPost = {
      ...rn,
      link: Some(sid),
      // TR-REPLICATION-019: the granted `+CONTINUE` shifts the inherited id at
      // the grant. On the full arm the replica adopts *neither* half yet — it
      // rewinds the offset and waits for the trailer (FM-REPLICATION-001).
      replid: if (arm == PartialGrant) pn.replid else rn.replid,
      // Either way the previous connection's decoded-but-unapplied bytes died
      // with it: the replica resumes from what it applied, not what it received.
      recv: if (arm == PartialGrant) rn.applied else 0,
      applied: if (arm == PartialGrant) rn.applied else 0,
      // R5: divergence truncation at the accept. A partial grant asserts
      // agreement only up to the claimed offset, so anything the replica holds
      // above it is discarded here rather than being left to be overwritten (or
      // not) by whatever the granting history happens to write next. The full
      // arm needs no rule: the trailer install replaces the dataset wholesale.
      data: if (arm == PartialGrant) truncateAboveClaim(rn.data, rn.applied) else rn.data,
    }
    val nodesPost = nodes.set(r, rPost)
    all {
      r != p,
      rn.role == Replica,
      pn.role == Primary,
      rn.link == None,
      sessions.get(sid).phase == Disconnected,
      nodes' = nodesPost,
      sessions' = SIDS.mapBy(k =>
        if (k == sid) granted
        else if (victims.contains(k)) { ...sessions.get(k), phase: Disconnected }
        else sessions.get(k)),
      keepCtl,
      defects' = {
        ...defects,
        grantBelowFloor: latch(defects.grantBelowFloor,
          arm == PartialGrant and not(floorAdmits(pn, rn.applied))),
        grantForeignHistory: latch(defects.grantForeignHistory,
          arm == PartialGrant and not(windowContains(pn, rn.replid, rn.applied))),
        supersededDeparture: latch(defects.supersededDeparture,
          victims.size() > 0 and nodesPost.get(p).last_departure != pn.last_departure),
      },
      coverage' = {
        ...coverage,
        partialViaSecondary: latch(coverage.partialViaSecondary,
          arm == PartialGrant and grantedViaSecondary(pn, rn)),
        evictionForcedFull: latch(coverage.evictionForcedFull,
          arm == FullSnapshot and degradedByFloor(pn, rn)),
        superseded: latch(coverage.superseded, victims.size() > 0),
        overshipTruncated: latch(coverage.overshipTruncated,
          arm == PartialGrant and rn.data.length() > rn.applied),
      },
    }
  }

  // --- Checkpoint cut (TR-REPLICATION-003, FM-REPLICATION-004) ---

  action cutCheckpointAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    val payload = cutPayload(pn)
    all {
      s.phase == PreparingCheckpoint,
      keepNodes,
      sessions' = sessions.set(sid,
        { ...s, phase: StreamingCheckpoint, payload: payload, payload_cut: true }),
      keepCtl,
      defects' = { ...defects,
        cutBelowGrant: latch(defects.cutBelowGrant, cutBelowGrant(payload, s.grant_offset)) },
      coverage' = { ...coverage,
        overshipped: latch(coverage.overshipped, cutAboveGrant(payload, s.grant_offset)) },
    }
  }

  // --- Trailer install (TR-REPLICATION-005/015/020) ---

  action deliverPayloadAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val rn = nodes.get(s.replica)
    val rPost = applyInstallPayload(rn, s)
    all {
      s.phase == StreamingCheckpoint,
      // Only the replica's own live link may write its dataset. A corpse session
      // on a primary the replica has since left holds no socket to it, so it
      // cannot install, stream or land anything (issue-22 shape).
      linkIs(rn, sid),
      nodes' = nodes.set(s.replica, rPost),
      sessions' = sessions.set(sid, { ...s, phase: PayloadInstalled }),
      keepCtl,
      defects' = { ...defects,
        tornIdentity: latch(defects.tornIdentity, not(installPairedOk(rPost, s))) },
      keepCoverage,
    }
  }

  // --- Splice (TR-REPLICATION-002, FM-REPLICATION-012) ---

  action spliceStreamAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    all {
      s.phase == PayloadInstalled or s.phase == Connecting,
      spliceRangeAvailable(pn, s.grant_offset),
      keepNodes,
      sessions' = sessions.set(sid,
        { ...s, phase: Streaming, resume_offset: s.grant_offset }),
      keepCtl,
      keepDefects,
      coverage' = { ...coverage,
        fullSyncStreaming: latch(coverage.fullSyncStreaming, s.arm == FullSnapshot) },
    }
  }

  // FM-REPLICATION-012 / TR-REPLICATION-006: the window closed between the grant
  // and the extraction, so the replay cannot be contiguous and does not happen
  // at all. The link fails before Streaming, so nothing is streamed.
  action abandonSpliceAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    val rn = nodes.get(s.replica)
    all {
      s.phase == PayloadInstalled or s.phase == Connecting,
      not(spliceRangeAvailable(pn, s.grant_offset)),
      nodes' = nodes.set(s.replica, { ...rn, link: None }),
      sessions' = sessions.set(sid, { ...s, phase: Disconnected }),
      keepCtl,
      keepDefects,
      coverage' = { ...coverage,
        spliceAbandoned: latch(coverage.spliceAbandoned, true) },
    }
  }

  // --- Frame consumer (TR-REPLICATION-014, FM-REPLICATION-015) ---

  action receiveFrameAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    val rn = nodes.get(s.replica)
    all {
      s.phase == Streaming,
      linkIs(rn, sid),
      rn.recv < pn.recv,
      rn.recv < pn.data.length(),
      nodes' = nodes.set(s.replica, { ...rn, recv: rn.recv + 1 }),
      keepSessions,
      keepCtl,
      keepDefects,
      keepCoverage,
    }
  }

  action applyFrameAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    val rn = nodes.get(s.replica)
    val k = rn.applied + 1
    // Sentinel-guarded read: the `k <= pn.data.length()` conjunct below disables
    // the action whenever the sentinel would be used, so it never commits.
    val w = if (k <= pn.data.length()) pn.data.nth(k - 1) else NO_HISTORY
    val outcome = classifyApply(rn.data, k, w)
    all {
      s.phase == Streaming,
      linkIs(rn, sid),
      rn.applied < rn.recv,
      k <= pn.data.length(),
      nodes' = nodes.set(s.replica,
        { ...rn, data: applyFrameToData(rn.data, k, w), applied: k }),
      keepSessions,
      keepCtl,
      defects' = { ...defects,
        spliceGap: latch(defects.spliceGap, outcome == Gapped),
        // Divergence inside the window the grant claims. See `classifyApply`.
        spliceDiverged: latch(defects.spliceDiverged,
          outcome == Diverged and k <= s.resume_offset),
        // R5: divergence *above* the claim. Unreachable once the accept
        // truncates, which is exactly what makes this a falsifiable pin.
        forkedTailApplied: latch(defects.forkedTailApplied,
          outcome == Diverged and k > s.resume_offset),
        // R4: a re-delivered position must land as a no-op, and the applied
        // position must not move backwards on a landing.
        reapplyChangedData: latch(defects.reapplyChangedData,
          outcome == Reapplied and applyFrameToData(rn.data, k, w) != rn.data),
        replayRegressedApplied: latch(defects.replayRegressedApplied,
          k <= rn.applied) },
      coverage' = { ...coverage,
        reapplied: latch(coverage.reapplied, outcome == Reapplied),
        tailApplied: latch(coverage.tailApplied, true) },
    }
  }

  // --- Re-delivery below the applied head (FM-REPLICATION-065, R7) ---
  //
  // The frame stream is peer-supplied input, so — exactly as with the wire ack
  // below — the model admits a frame no correct primary would send: R7 rules
  // the *receiver* authoritative for dedup, and a model of an input validation
  // must admit the invalid input. The class is "a frame at or below the offset
  // this replica has already applied", which on the wire arrives from a
  // mis-computed resume point or a duplicated backlog extraction.
  //
  // Unlike the ack pair (`recordAckAt`/`ignoreAckAt`), this is a single action
  // that *decides* internally on `frameIsCovered`, mirroring the consumer loop
  // it models — the implementation checks coverage inline, before the claim,
  // and falls through to the apply path otherwise. That shape is also what
  // makes the boundary falsifiable: narrowing `frameIsCovered` to `<` routes
  // the `k == applied` re-delivery into the landing branch instead of the skip,
  // and `skipDisposedOk` catches it. A guard-only split could not — it would
  // merely disable the action.
  action redeliverFrameAt(sid: SessionId, k: Offset): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    val rn = nodes.get(s.replica)
    // Sentinel-guarded read, as in `applyFrameAt`: the `k <= pn.data.length()`
    // conjunct disables the action whenever the sentinel would be used.
    val w = if (k >= 1 and k <= pn.data.length()) pn.data.nth(k - 1) else NO_HISTORY
    val rnPost =
      if (frameIsCovered(rn, k)) applySkip(rn)
      else { ...rn, data: applyFrameToData(rn.data, k, w), applied: max2(rn.applied, k) }
    all {
      s.phase == Streaming,
      linkIs(rn, sid),
      // The input class: a position this replica has already applied. `k >= 1`
      // keeps the offset a real position rather than the pre-history zero.
      k >= 1,
      k <= rn.applied,
      k <= pn.data.length(),
      nodes' = nodes.set(s.replica, rnPost),
      keepSessions,
      keepCtl,
      defects' = { ...defects,
        skippedFrameLanded: latch(defects.skippedFrameLanded,
          not(skipDisposedOk(rn, rnPost))) },
      coverage' = { ...coverage, frameSkipped: latch(coverage.frameSkipped, true) },
    }
  }

  // --- ACK ingest (TR-REPLICATION-018, issue-21 ruling) ---
  //
  // The wire ack is peer-supplied input, so the model admits values no correct
  // FrogDB replica would send: the ruling being modelled *is* a validation of
  // untrusted input, and a model of an input validation must admit the invalid
  // input. The candidate set is the honest ack (the replica's landed offset) and
  // one above the primary's live head.

  action recordAckAt(sid: SessionId, wire: Offset): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    val sPost = applyAck(s, wire)
    all {
      s.phase == Streaming,
      not(ackIsAboveHead(pn, wire)),
      keepNodes,
      sessions' = sessions.set(sid, sPost),
      keepCtl,
      defects' = { ...defects,
        ackAboveHead: latch(defects.ackAboveHead, sPost.acked > pn.recv) },
      keepCoverage,
    }
  }

  action ignoreAckAt(sid: SessionId, wire: Offset): bool = {
    val s = sessions.get(sid)
    val pn = nodes.get(s.primary)
    // Ruled: ignored entirely. No AckedOffset write, no disconnect, one counter.
    val sessionsPost = sessions
    all {
      s.phase == Streaming,
      ackIsAboveHead(pn, wire),
      nodes' = nodes.set(s.primary, { ...pn, acks_ignored: pn.acks_ignored + 1 }),
      sessions' = sessionsPost,
      keepCtl,
      defects' = { ...defects,
        ackWrittenOnIgnore: latch(defects.ackWrittenOnIgnore,
          sessionsPost.get(sid).acked != s.acked) },
      coverage' = { ...coverage, ackIgnored: latch(coverage.ackIgnored, true) },
    }
  }

  // --- Promotion (TR-REPLICATION-016, FM-REPLICATION-019/-020, issue-24) ---

  action promoteAs(n: NodeId): bool = {
    val nn = nodes.get(n)
    val nPost = applyPromote(nn, ctl.next_gen)
    all {
      nn.role == Replica,
      ctl.next_gen <= FIRST_GEN + MAX_GENS,
      ctl.disrupts_left > 0,
      nodes' = nodes.set(n, nPost),
      // The links this node held as a replica die with the role change. The
      // primary-side departure classification is the feed-gate model's subject,
      // not this one's.
      sessions' = SIDS.mapBy(k => {
        val s = sessions.get(k)
        if (s.replica == n) { ...s, phase: Disconnected } else s
      }),
      ctl' = ctlNextD(0, 1, 1),
      defects' = { ...defects,
        tornIdentity: latch(defects.tornIdentity, not(promotePairedOk(nn, nPost))) },
      coverage' = { ...coverage,
        settleDiscardedFrames: latch(coverage.settleDiscardedFrames, nn.recv > nn.applied) },
    }
  }

  // --- Restart (TR-REPLICATION-021, issue-24 (a)+(b)) ---

  action restartNodeAs(n: NodeId, unclean: bool, keep: Offset): bool = {
    val nn = nodes.get(n)
    val nPost = applyRestart(nn, unclean, keep, ctl.next_gen)
    val keptId = restartKeepsIdentity(nn, unclean, keep)
    all {
      nn.role == Primary,
      keep <= nn.applied,
      keptId or ctl.next_gen <= FIRST_GEN + MAX_GENS,
      ctl.disrupts_left > 0,
      nodes' = unlinkedFrom(nodes.set(n, nPost), n),
      sessions' = sessionsWithout(n),
      ctl' = ctlNextD(0, if (keptId) 0 else 1, 1),
      defects' = { ...defects,
        restartOffsetTorn: latch(defects.restartOffsetTorn, not(restartOffsetPairedOk(nPost))),
        tornIdentity: latch(defects.tornIdentity,
          nPost.replid != nn.replid and
            not(nPost.replid2 == Some(nn.replid) and nPost.second_offset == keep)) },
      coverage' = { ...coverage,
        uncleanRestartReminted: latch(coverage.uncleanRestartReminted, not(keptId)) },
    }
  }

  // --- Link lifecycle ---

  // The replica's link dies while the primary still has the session registered:
  // the corpse the issue-22 dedup exists to clear.
  action dropLinkAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val rn = nodes.get(s.replica)
    all {
      s.phase == Streaming,
      linkIs(rn, sid),
      nodes' = nodes.set(s.replica, { ...rn, link: None }),
      keepSessions,
      keepCtl,
      keepDefects,
      coverage' = { ...coverage, corpseSession: latch(coverage.corpseSession, true) },
    }
  }

  action endSessionAt(sid: SessionId): bool = {
    val s = sessions.get(sid)
    val rn = nodes.get(s.replica)
    val pn = nodes.get(s.primary)
    all {
      s.phase != Disconnected,
      nodes' = nodes
        .set(s.replica, if (linkIs(rn, sid)) { ...rn, link: None } else rn)
        .set(s.primary,
          if (s.phase == Streaming) { ...pn, last_departure: Some(Graceful) } else pn),
      sessions' = sessions.set(sid, { ...s, phase: Disconnected }),
      keepCtl,
      keepDefects,
      keepCoverage,
    }
  }

  // --- Nondet biasing ---
  //
  // Narrow each `oneOf` to the currently-interesting values, falling back to the
  // full domain when that set is empty. This changes only which transitions the
  // randomized walk is *likely* to try, never which are legal: no guard or
  // postcondition reads any of these. Without it the walk spends most of its
  // budget picking free session slots and non-primary write targets, and the
  // deep handoff paths (splice abandonment, the reapplied overlap) are reached
  // in well under 1% of traces.

  val primaryCands: Set[NodeId] = {
    val c = NODES.filter(n => nodes.get(n).role == Primary)
    if (c == Set()) NODES else c
  }

  val replicaCands: Set[NodeId] = {
    val c = NODES.filter(n => nodes.get(n).role == Replica)
    if (c == Set()) NODES else c
  }

  // A replica holding frames it has received but not applied is the interesting
  // promotion subject: `settle_at_applied` discards exactly that tail
  // (TR-REPLICATION-016). Prefer one when it exists.
  val promoteCands: Set[NodeId] = {
    val hot = NODES.filter(n => {
      val nn = nodes.get(n)
      nn.role == Replica and nn.recv > nn.applied
    })
    if (hot == Set()) replicaCands else hot
  }

  val unlinkedReplicaCands: Set[NodeId] = {
    val c = NODES.filter(n => nodes.get(n).role == Replica and nodes.get(n).link == None)
    if (c == Set()) NODES else c
  }

  // A replica still carrying data past its own offset — the FM-REPLICATION-004
  // overshipped tail — is the interesting PSYNC subject, because its next grant
  // is what does or does not reconcile that tail. Prefer one when it exists.
  val psyncReplicaCands: Set[NodeId] = {
    val forked = unlinkedReplicaCands.filter(n => {
      val nn = nodes.get(n)
      nn.data.length() > nn.applied
    })
    if (forked == Set()) unlinkedReplicaCands else forked
  }

  val liveSidCands: Set[SessionId] = {
    val c = SIDS.filter(k => sessions.get(k).phase != Disconnected)
    if (c == Set()) SIDS else c
  }

  val freeSidCands: Set[SessionId] = {
    val c = freeSids
    if (c == Set()) SIDS else c
  }

  // --- step ---

  action writeStep: bool = { nondet p = primaryCands.oneOf() writeOnPrimaryAs(p) }
  action evictStep: bool = { nondet p = primaryCands.oneOf() evictBacklogAs(p) }

  action psyncStep: bool = {
    nondet r = psyncReplicaCands.oneOf()
    nondet p = primaryCands.oneOf()
    nondet announced = Set(false, true).oneOf()
    nondet sid = freeSidCands.oneOf()
    psyncRequestAs(r, p, announced, sid)
  }

  action cutStep: bool = { nondet sid = liveSidCands.oneOf() cutCheckpointAt(sid) }
  action deliverStep: bool = { nondet sid = liveSidCands.oneOf() deliverPayloadAt(sid) }
  action spliceStep: bool = { nondet sid = liveSidCands.oneOf() spliceStreamAt(sid) }
  action abandonStep: bool = { nondet sid = liveSidCands.oneOf() abandonSpliceAt(sid) }
  action receiveStep: bool = { nondet sid = liveSidCands.oneOf() receiveFrameAt(sid) }
  action applyStep: bool = { nondet sid = liveSidCands.oneOf() applyFrameAt(sid) }

  // The two re-delivery offsets worth sampling: the boundary the rule turns on
  // (a frame ending exactly at the applied head) and the deepest one there is.
  action redeliverStep: bool = {
    nondet sid = liveSidCands.oneOf()
    val s = sessions.get(sid)
    nondet k = Set(nodes.get(s.replica).applied, 1).oneOf()
    redeliverFrameAt(sid, k)
  }

  action ackStep: bool = {
    nondet sid = liveSidCands.oneOf()
    val s = sessions.get(sid)
    nondet wire = Set(nodes.get(s.replica).applied, nodes.get(s.primary).recv + 1).oneOf()
    any { recordAckAt(sid, wire), ignoreAckAt(sid, wire) }
  }

  action promoteStep: bool = { nondet n = promoteCands.oneOf() promoteAs(n) }

  // Recovering to exactly the offset some replica already holds is the case that
  // decides whether that replica resumes through the failover window or is sent
  // a whole new dataset, so prefer those recovery points.
  def keepCands(n: NodeId): Set[Offset] = {
    val hi = nodes.get(n).applied
    val aligned = NODES.map(m => nodes.get(m).applied).filter(o => o >= 0 and o <= hi)
    if (aligned == Set()) 0.to(hi) else aligned
  }

  action restartStep: bool = {
    nondet n = primaryCands.oneOf()
    nondet unclean = Set(false, true).oneOf()
    nondet keep = keepCands(n).oneOf()
    restartNodeAs(n, unclean, keep)
  }

  action dropLinkStep: bool = { nondet sid = liveSidCands.oneOf() dropLinkAt(sid) }
  action endSessionStep: bool = { nondet sid = liveSidCands.oneOf() endSessionAt(sid) }

  // The four arms that tear a session down. Grouped under one `any` so the walk
  // reaches for teardown about as often as it reaches for any single progress
  // arm, instead of four times as often. Nesting `any` changes the sampling
  // weight only: every one of these transitions stays exactly as legal, and the
  // deterministic `run` tests below drive them directly.
  action teardownStep: bool = any {
    promoteStep,
    dropLinkStep,
    any { restartStep, endSessionStep },
  }

  action step: bool = any {
    writeStep,
    evictStep,
    psyncStep,
    cutStep,
    deliverStep,
    spliceStep,
    abandonStep,
    receiveStep,
    applyStep,
    redeliverStep,
    ackStep,
    teardownStep,
  }
}
