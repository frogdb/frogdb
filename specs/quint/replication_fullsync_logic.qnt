// Full-sync / PSYNC handoff — pure guards and state updates.
//
// Every decision the handoff makes, as `pure def`s over explicit state values:
// the PSYNC arm decision (TR-REPLICATION-001, FM-REPLICATION-013), the backlog
// window and floor checks (FM-REPLICATION-012, FM-REPLICATION-014,
// FM-REPLICATION-016), the checkpoint cut and trailer install
// (TR-REPLICATION-003, TR-REPLICATION-005, TR-REPLICATION-015,
// TR-REPLICATION-020, FM-REPLICATION-004), frame application at the splice
// (TR-REPLICATION-014, FM-REPLICATION-015), ACK ingest under the issue-21
// ruling (TR-REPLICATION-018), and the promotion / unclean-restart identity
// shift under the issue-24 rulings (TR-REPLICATION-016, TR-REPLICATION-021,
// FM-REPLICATION-019, FM-REPLICATION-020, INV-REPLID-1, INV-REPLID-2,
// INV-REPLID-3, INV-OFFSET-4).
//
// No wall clock appears anywhere: every window in this model is bounded by an
// offset or a generation, per the campaign's global no-wall-clock ruling.
module replication_fullsync_logic {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"
  import replication_fullsync_types.* from "./replication_fullsync_types"

  // --- Histories ---

  // Positions 1..n of `d` carry exactly what positions 1..n of `hist` carry.
  // The workhorse of the acked-write-loss property: a replica that claims to
  // have landed `n` must hold the primary's first `n` writes, in order.
  pure def prefixAgrees(d: List[WriteId], hist: List[WriteId], n: int): bool =
    if (n <= 0) true
    else if (n > d.length() or n > hist.length()) false
    else 0.to(n - 1).forall(i => d.nth(i) == hist.nth(i))

  // --- PSYNC arm decision (TR-REPLICATION-001, FM-REPLICATION-013) ---

  // `window_contains`: the two-bound check, replid-first. The primary arm needs
  // id equality *and* an offset the primary has actually reached; the secondary
  // arm needs the inherited id and an offset inside the frozen window, whose
  // boundary is inclusive (FM-REPLICATION-019 — no Redis +1).
  pure def windowContains(p: NodeState, id: Gen, off: Offset): bool =
    or {
      p.replid == id and off <= p.recv,
      and {
        optExists(p.replid2, g => g == id),
        p.second_offset != NO_WINDOW,
        off <= p.second_offset,
      },
    }

  // The armed floor is the only lower bound (FM-REPLICATION-014): unarmed always
  // refuses, and the resumable side is inclusive (`req == floor` continues).
  pure def floorAdmits(p: NodeState, off: Offset): bool =
    p.floor != UNARMED and off >= p.floor

  // The ordered decision of FM-REPLICATION-013: no history at all, then the
  // replid-first window check, then — last and unconditionally — the floor.
  pure def decideArm(p: NodeState, r: NodeState): Arm =
    if (r.replid == NO_HISTORY) FullSnapshot
    else if (not(windowContains(p, r.replid, r.applied))) FullSnapshot
    else if (not(floorAdmits(p, r.applied))) FullSnapshot
    else PartialGrant

  // True when the *only* reason the arm degraded to a full snapshot was the
  // floor — the FM-REPLICATION-016 backlog-eviction path
  // (`FullResyncReason::BacklogEvicted`). Used for a coverage ghost.
  pure def degradedByFloor(p: NodeState, r: NodeState): bool =
    and {
      r.replid != NO_HISTORY,
      windowContains(p, r.replid, r.applied),
      not(floorAdmits(p, r.applied)),
    }

  // True when the partial grant was served out of the inherited (replid2)
  // window rather than the primary's own id — the failover-resume path of
  // FM-REPLICATION-019.
  pure def grantedViaSecondary(p: NodeState, r: NodeState): bool =
    p.replid != r.replid and optExists(p.replid2, g => g == r.replid)

  // --- Primary write path (TR-REPLICATION-030) ---

  pure def applyWriteOnPrimary(p: NodeState, w: WriteId): NodeState = {
    ...p,
    data: p.data.append(w),
    recv: p.recv + 1,
    applied: p.applied + 1,
    // A first push into an unarmed buffer opens the window at this command's
    // start offset, which for a unit-length write is the pre-write head.
    floor: if (p.floor == UNARMED) p.recv else p.floor,
  }

  // FM-REPLICATION-016: FIFO eviction raises the floor to the evicted entry's
  // *end* offset, so the retained range stays a contiguous suffix and the floor
  // always names a real resume point.
  pure def canEvictBacklog(p: NodeState): bool =
    p.role == Primary and p.floor != UNARMED and p.recv - p.floor > BACKLOG_CAP

  pure def applyEvictBacklog(p: NodeState): NodeState =
    { ...p, floor: p.floor + 1 }

  // --- Checkpoint cut (TR-REPLICATION-003, FM-REPLICATION-004) ---

  // The cut drains the shard WALs into the store *before* exporting, so the
  // payload covers every acknowledged write — including ones that landed after
  // the grant captured `snapshot_offset`. That is why the safe direction is
  // structural: `grant_offset <= payload.length()`. The historical defect (the
  // pre-checkpoint drain missing) is the opposite direction, and is what
  // `cutBelowGrant` below detects.
  pure def cutPayload(p: NodeState): List[WriteId] = p.data

  pure def cutBelowGrant(payload: List[WriteId], grant: Offset): bool =
    payload.length() < grant

  pure def cutAboveGrant(payload: List[WriteId], grant: Offset): bool =
    payload.length() > grant

  // --- Trailer install (TR-REPLICATION-015, TR-REPLICATION-020) ---

  // The replica adopts the trailer identity and the absolute offset pair in one
  // step: id, window and both offsets move together (issue-24 atomic pairing,
  // FM-REPLICATION-020). TR-REPLICATION-020 clears the failover window — a full
  // resync closes any window inherited from a prior promotion.
  pure def applyInstallPayload(r: NodeState, s: Session): NodeState = {
    ...r,
    data: s.payload,
    replid: s.granted_gen,
    replid2: None,
    second_offset: NO_WINDOW,
    recv: s.grant_offset,
    applied: s.grant_offset,
  }

  // Postcondition of a lawful install: nothing is left half-written. A mutation
  // that drops the window clear, or that installs the id without the offsets,
  // fails this.
  pure def installPairedOk(post: NodeState, s: Session): bool =
    and {
      post.replid == s.granted_gen,
      post.replid2 == None,
      post.second_offset == NO_WINDOW,
      post.recv == s.grant_offset,
      post.applied == s.grant_offset,
    }

  // --- Splice (TR-REPLICATION-002, FM-REPLICATION-012) ---

  // The window is re-checked at *extraction*, not only at the grant: eviction
  // between the two closes it and the replay must fail rather than skip. An
  // empty replay range needs no history at all, so it is served regardless.
  pure def spliceRangeAvailable(p: NodeState, from: Offset): bool =
    from >= p.recv or floorAdmits(p, from)

  // --- Frame application (TR-REPLICATION-014, FM-REPLICATION-015) ---

  // Classify the landing of the frame for offset `k` carrying `w`, against the
  // dataset the replica already holds.
  //
  // `Reapplied` is the overlap FM-REPLICATION-001 declares a deliberate
  // non-guarantee: the checkpoint may carry writes past the granted offset, and
  // the backlog replay then re-ships them ("re-applies a tail it already holds").
  // R4 (2026-08-20) rules the re-landing a no-op, which in this model's
  // unit-write log it is by construction — `applyFrameToData` overwrites a
  // position with the identical write id. `defects.reapplyChangedData` makes
  // that construction falsifiable rather than incidental. See the discrepancy
  // note in the main module for the half of R4 the log abstraction cannot carry.
  //
  // `Diverged` is read against the *window the grant claims*, which is why the
  // caller pairs it with the session's resume offset. At or below that offset
  // both sides assert they hold the same history, so different content there is
  // a defect (`defects.spliceDiverged`). Above it the shape is the
  // FM-REPLICATION-004 overshipped tail — payload data the replica installed
  // past the granted offset. R5 (2026-08-20) removes that class at the accept
  // (`truncateAboveClaim`), so reaching it is now also a defect
  // (`defects.forkedTailApplied`, pinned by `overshippedTailMeetsFailoverTest`).
  pure def classifyApply(d: List[WriteId], k: Offset, w: WriteId): ApplyOutcome =
    if (k == d.length() + 1) Appended
    else if (k > d.length() + 1) Gapped
    else if (d.nth(k - 1) == w) Reapplied
    else Diverged

  // A frame is a command applied to the keyspace, not an append to a log, so a
  // landing at a position the replica already holds *overwrites* it. Modelling
  // that as a no-op would be a fidelity bug: it would leave the replica showing
  // a replaced history's content at a position the live history has since
  // rewritten, and every downstream property would read that as divergence.
  // R5 (2026-08-20 ruling): Raft-style divergence truncation on a partial-resync
// accept. Everything strictly above the offset the replica claims is dropped
// before the splice, so a position the replica holds but does not claim can
// never survive into a history that will not overwrite it. The claimed prefix
// is untouched — it is what the grant asserts agreement on.
pure def truncateAboveClaim(d: List[WriteId], claim: Offset): List[WriteId] =
  if (claim < d.length()) d.slice(0, claim) else d

pure def applyFrameToData(d: List[WriteId], k: Offset, w: WriteId): List[WriteId] =
    if (k == d.length() + 1) d.append(w) else d.replaceAt(k - 1, w)

  // --- ACK ingest (TR-REPLICATION-018, issue-21 ruling) ---

  // Ruled "ignore + count", not "clamp": an ack strictly above the primary's
  // live head is discarded whole — no AckedOffset write, no disconnect — and a
  // counter is bumped. Clamping was reversed because it would make the primary a
  // *writer* of AckedOffset (Raft shape: a leader never advances a follower's
  // match index past its own log).
  pure def ackIsAboveHead(p: NodeState, wire: Offset): bool = wire > p.recv

  pure def applyAck(s: Session, wire: Offset): Session =
    { ...s, acked: max2(s.acked, wire) }

  // --- Promotion (TR-REPLICATION-016, FM-REPLICATION-019, issue-24) ---

  // One step, by construction: mint a fresh id, shift the outgoing id into the
  // failover window at the applied boundary, settle the live head down onto that
  // boundary (`settle_at_applied` — frames received but never applied are
  // discarded, never granted to anyone), and re-arm the backlog floor from the
  // same boundary. Nothing here can be observed half-done, which is exactly the
  // FM-REPLICATION-020 not-observable and the issue-24 atomic-pairing ruling.
  pure def applyPromote(n: NodeState, freshGen: Gen): NodeState = {
    ...n,
    role: Primary,
    replid: freshGen,
    replid2: Some(n.replid),
    second_offset: n.applied,
    recv: n.applied,
    floor: n.applied,
    link: None,
  }

  // Postcondition of a lawful promotion, recomputed from the outcome so a
  // mutation to any one write shows up (defects-family convention).
  pure def promotePairedOk(pre: NodeState, post: NodeState): bool =
    and {
      post.replid2 == Some(pre.replid),
      post.second_offset == pre.applied,
      post.recv == pre.applied,
      post.applied == pre.applied,
      post.floor == pre.applied,
      post.replid != pre.replid,
    }

  // --- Restart (TR-REPLICATION-021, issue-24 (a)+(b)) ---

  // Ruled: a restart keeps its identity only when it recovered its dataset
  // *intact*. Every other restart mints a fresh primary id and shifts the old id
  // into the failover window at the boundary it can actually prove — the
  // recovered offset — so a replica that ran further can never be granted
  // `+CONTINUE` over history the primary no longer has.
  pure def restartKeepsIdentity(pre: NodeState, unclean: bool, keep: Offset): bool =
    not(unclean) and keep == pre.applied

  // The recovered offset is whatever the recovered dataset actually proves, and
  // the two commit together (issue-24 amendment point 2: the persisted offset
  // commits in the same atomic unit as the write it names). Modelling them as
  // one assignment is the ruling; `restartOffsetPairedOk` is what a mutation
  // separating them would trip.
  pure def applyRestart(pre: NodeState, unclean: bool, keep: Offset, freshGen: Gen): NodeState =
    if (restartKeepsIdentity(pre, unclean, keep))
      { ...pre, role: Primary, data: pre.data.slice(0, keep), recv: keep, applied: keep,
        floor: UNARMED, link: None, last_departure: None }
    else
      { ...pre, role: Primary, data: pre.data.slice(0, keep), recv: keep, applied: keep,
        floor: UNARMED, link: None, last_departure: None,
        replid: freshGen, replid2: Some(pre.replid), second_offset: keep }

  pure def restartOffsetPairedOk(post: NodeState): bool =
    post.recv == post.data.length() and post.applied == post.data.length()

  // --- Identity pair ordering (issue-24) ---

  // A primary's (generation, live offset) pair never regresses lexicographically:
  // writes raise the offset at a fixed generation, and every event that lowers
  // the offset — promotion settle, lossy restart — mints a strictly greater
  // generation first. This is the whole point of `lexGt` at an (int, int).
  pure def identityPairOk(prevGen: Gen, prevOff: Offset, curGen: Gen, curOff: Offset): bool =
    (curGen == prevGen and curOff == prevOff) or lexGt((curGen, curOff), (prevGen, prevOff))
}
