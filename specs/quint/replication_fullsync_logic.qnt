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

  // --- Shards and coverage vectors (issue 35, rulings R13/R14) ---

  // Which shard executed a write. Keys hash to shards; the model does not carry
  // keys, and what the coverage rules turn on is only that *consecutive offsets
  // can belong to different shards* — so alternating by write id is the whole
  // content of this definition.
  pure def shardOf(w: WriteId): ShardId = if (w % 2 == 1) 1 else 2

  pure val noFloors: Coverage = SHARDS.mapBy(_ => NO_FLOOR)

  // `max(Y_s)`: the offset at which the whole vector retires. Below it some
  // shard's payload half reaches past the claim, so the floors are still in
  // force and a window grant is refused (ruling R14).
  pure def maxFloor(c: Coverage): Offset =
    SHARDS.fold(NO_FLOOR, (acc, s) => max2(acc, c.get(s)))

  // A vector that does not describe every shard at the same offset — the torn
  // checkpoint. The mixed skip/apply inside one cross-shard group is exactly
  // what mends it.
  pure def coverageIsTorn(c: Coverage): bool =
    SHARDS.exists(a => SHARDS.exists(b => c.get(a) != c.get(b)))

  // The highest offset at or below `bound` that `d` holds for shard `sh`, or
  // `NO_FLOOR`. `HOLE` positions are not attributed to any shard: they are the
  // absence of a write, not a write of shard `shardOf(HOLE)`.
  pure def lastOffsetOfShard(d: List[WriteId], sh: ShardId, bound: Offset): Offset =
    d.indices().fold(NO_FLOOR, (acc, i) =>
      if (i + 1 <= bound and d.nth(i) != HOLE and shardOf(d.nth(i)) == sh)
        max2(acc, i + 1)
      else acc)

  // The vector a payload's content actually justifies. Both directions matter:
  // claiming above the content makes the replica skip a frame it does not hold
  // (loss), claiming below it makes the replica re-execute one it does (the D1
  // double-apply).
  pure def coverageOfPayload(d: List[WriteId]): Coverage =
    SHARDS.mapBy(sh => lastOffsetOfShard(d, sh, d.length()))

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

  // Ruling R14 — the refusal, and the exact cell it applies to. A `+CONTINUE`
  // whose grant carries a *different* replication id than the replica presented
  // is a window grant: the primary is serving history it inherited, not history
  // it wrote. That is the only cell where effects sitting above the replica's
  // claim are dangerous, because the granting history may never reproduce them.
  //
  //   * same-history grant: refusing would be wrong, not merely wasteful — the
  //     overshipped effects are a prefix of exactly what will be replayed, and
  //     the floors dedup them exactly;
  //   * window grant at `applied >= max(Y_s)`: a clean shared-prefix state,
  //     nothing above the claim, stays partial;
  //   * window grant at `applied < max(Y_s)`: refused, degrade to a full resync,
  //     which replaces the dataset and wipes the residue.
  //
  // Ruling R16 adds the second disjunct: a crash-recovered stint has a keyspace
  // possibly ahead of its claim and *no vector at all*, so the floor guard is
  // blind there and every window grant is refused until issue 36 lands.
  pure def windowGrantRefused(p: NodeState, r: NodeState): bool =
    and {
      p.replid != r.replid,
      r.recovered or r.applied < maxFloor(r.floors),
    }

  // The ordered decision of FM-REPLICATION-013: no history at all, then the
  // replid-first window check, then the floor, then — last — the R14/R16
  // refusal, which turns a grant the primary *would* serve into a full resync
  // on the replica's own evidence about its keyspace.
  pure def decideArm(p: NodeState, r: NodeState): Arm =
    if (r.replid == NO_HISTORY) FullSnapshot
    else if (not(windowContains(p, r.replid, r.applied))) FullSnapshot
    else if (not(floorAdmits(p, r.applied))) FullSnapshot
    else if (windowGrantRefused(p, r)) FullSnapshot
    else PartialGrant

  // True when the *only* reason the arm degraded was the R14/R16 refusal — the
  // coverage ghost for the refusal path.
  pure def degradedByRefusal(p: NodeState, r: NodeState): bool =
    and {
      r.replid != NO_HISTORY,
      windowContains(p, r.replid, r.applied),
      floorAdmits(p, r.applied),
      windowGrantRefused(p, r),
    }

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
  //
  // The `optExists` conjunct is an **equivalent mutant** at the single call site
  // (`psyncRequestAs`'s `coverage.partialViaSecondary` latch, guarded by `arm ==
  // PartialGrant`) and no test can kill dropping it — battery row F17, resolved
  // 2026-08-20. `PartialGrant` means `windowContains(p, r.replid, r.applied)`
  // held, and its only two disjuncts are the primary's own id and the inherited
  // one; the first is false exactly when `p.replid != r.replid`, so the second
  // must have held and `optExists` is already implied. The conjunct stays
  // because this predicate is read on its own — its *name* promises the
  // secondary window, and a future caller without the `PartialGrant` guard would
  // need it.
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
  // Issue 35 / ruling R13: the cut is *per shard*. Each shard's half of the
  // artefact is captured at that shard's own drain-ack (checkpoint path) or
  // export message (live path), and the flush engine is held between the ack
  // and the cut, so the watermark is exact: everything the shard had broadcast
  // by then is in, nothing later is. Different shards ack at different offsets,
  // which is what tears the payload.
  //
  // `bounds` is that per-shard capture point. `cutCoverage` is the vector the
  // trailer carries — the highest offset each shard actually contributed, which
  // is at or below its bound (a shard's last write can predate its ack).
  pure def cutCoverage(p: NodeState, bounds: Coverage): Coverage =
    SHARDS.mapBy(sh => lastOffsetOfShard(p.data, sh, bounds.get(sh)))

  // The artefact those watermarks describe: position `k` carries the write iff
  // that write's own shard was captured at or above `k`. Everything else is a
  // `HOLE` — a position the payload does not hold, which the replay must fill.
  // The list stops at `max(Y_s)`; nothing above any watermark is in the payload
  // by construction, which is the half of exactness the flush hold buys.
  pure def cutPayloadWith(d: List[WriteId], y: Coverage): List[WriteId] =
    range(1, maxFloor(y) + 1).foldl(List(), (acc, k) =>
      acc.append(
        if (d.nth(k - 1) != HOLE and k <= y.get(shardOf(d.nth(k - 1)))) d.nth(k - 1)
        else HOLE))

  pure def cutBelowGrant(payload: List[WriteId], grant: Offset): bool =
    payload.length() < grant

  pure def cutAboveGrant(payload: List[WriteId], grant: Offset): bool =
    payload.length() > grant

  // --- Trailer install (TR-REPLICATION-015, TR-REPLICATION-020) ---

  // The replica adopts the trailer identity and the absolute offset pair in one
  // step: id, window and both offsets move together (issue-24 atomic pairing,
  // FM-REPLICATION-020). TR-REPLICATION-020 clears the failover window — a full
  // resync closes any window inherited from a prior promotion.
  // The trailer's coverage vector is installed in the same step as the offsets
  // it belongs to (issue 35): the floors describe *this* payload, so a vector
  // that outlived its payload would be describing a keyspace that no longer
  // exists. Installing also clears `recovered` — the stint's offsets now come
  // from a completed sync, and the vector says exactly how far past them the
  // keyspace reaches.
  pure def applyInstallPayload(r: NodeState, s: Session): NodeState = {
    ...r,
    data: s.payload,
    replid: s.granted_gen,
    replid2: None,
    second_offset: NO_WINDOW,
    recv: s.grant_offset,
    applied: s.grant_offset,
    floors: s.coverage,
    recovered: false,
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
      post.floors == s.coverage,
      not(post.recovered),
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
  // past the granted offset. Issue 35 removes that class ahead of the accept —
  // the floors prevent the re-execution and `windowGrantRefused` refuses the one
  // grant that could leave the residue forked — so reaching it is now also a
  // defect (`defects.forkedTailApplied`, pinned by
  // `overshippedTailMeetsFailoverTest`).
  //
  // Note the split from `frameIsCovered` below: `Reapplied` is a position the
  // replica's *data* already holds but its applied head does not cover (the
  // checkpoint overship). A position the head *covers* never reaches this
  // classification at all — the receiver skips it first.
  // Issue 35 adds the `HOLE` case: a position inside the installed payload's
  // span that the payload did not carry, because the cut caught that position's
  // shard earlier than the shard next to it. Filling it is an ordinary landing —
  // there is nothing there to re-apply or to contradict — and it is the other
  // half of the torn-checkpoint mend.
  pure def classifyApply(d: List[WriteId], k: Offset, w: WriteId): ApplyOutcome =
    if (k == d.length() + 1) Appended
    else if (k > d.length() + 1) Gapped
    else if (d.nth(k - 1) == HOLE) Appended
    else if (d.nth(k - 1) == w) Reapplied
    else Diverged

  // A frame is a command applied to the keyspace, not an append to a log, so a
  // landing at a position the replica already holds *overwrites* it. Modelling
  // that as a no-op would be a fidelity bug: it would leave the replica showing
  // a replaced history's content at a position the live history has since
  // rewritten, and every downstream property would read that as divergence.
  // R5's in-place truncation at the partial-resync accept was **superseded by
  // issue 35** (2026-08-21). A replica cannot invert a landed effect: the stream
  // propagates verbatim, so "drop everything above the claim" would have to undo
  // an `INCR` or an `LPUSH` it never recorded the pre-image of. TR-REPLICATION-034's
  // postcondition is therefore implemented as *prevent plus refuse* —
  // `frameCoveredByFloor` prevents the tail from being re-executed, and
  // `windowGrantRefused` refuses the one grant under which residue above the
  // claim could be left forked. This model tracks the same class with the same
  // ghost (`defects.forkedTailApplied`), so
  // `inv_no_forked_tail_above_the_claim` now states an implemented rule rather
  // than a model-only one.
  pure def applyFrameToData(d: List[WriteId], k: Offset, w: WriteId): List[WriteId] =
    if (k == d.length() + 1) d.append(w) else d.replaceAt(k - 1, w)

  // --- Receiver-side skip (R7, FM-REPLICATION-065) ---
  //
  // R7 (2026-08-19 ruling, issue 34): dedup is *receiver-authoritative*, Raft
  // style. A frame the replica's applied head already covers is ignored whole,
  // whoever sent it and for whatever reason. The sender-side checks
  // (FM-REPLICATION-012/-015) stay, but they are no longer the only thing
  // between a mis-computed resume and a double-applied write.
  //
  // The comparison is inclusive because a frame is addressed by the offset it
  // ends at — the position it occupies here, `frame.sequence` in the
  // implementation — so `k == applied` is a frame the head covers exactly.
  pure def frameIsCovered(n: NodeState, k: Offset): bool = k <= n.applied

  // The ruled disposition of a covered frame: nothing about the history moves,
  // and the skip is counted (the R-21 "ignore + count" shape — a silent drop
  // leaves the operator with no evidence of the sender bug).
  pure def applySkip(n: NodeState): NodeState =
    { ...n, frames_skipped: n.frames_skipped + 1 }

  // --- Per-shard skip floors (issue 35, FM-REPLICATION-066) ---
  //
  // The floors extend `covers()` past the applied head, one shard at a time. A
  // shard-`s` frame ending at or below `floors[s]` was executed on the primary
  // *before* shard `s` was captured, so its effect is already in the installed
  // keyspace; re-executing it is the D1 double-apply, silent for `SET` and
  // corruption for `INCR`.
  //
  // The comparison is inclusive and against the frame's *own* shard's floor:
  // frames are shard-local, so a frame never straddles `Y_s` — it is that
  // shard's own frame boundary. That is what makes mixed skip/apply inside one
  // cross-shard group correct rather than a hole: each half is measured against
  // the watermark that describes it.
  pure def frameCoveredByFloor(n: NodeState, k: Offset, w: WriteId): bool =
    w != HOLE and k <= n.floors.get(shardOf(w))

  // The receiver's whole decision, in order: the applied head first
  // (FM-REPLICATION-065, whoever sent it), then this frame's shard's floor, then
  // the landing.
  pure def frameDisposition(n: NodeState, k: Offset, w: WriteId): FrameDisposition =
    if (frameIsCovered(n, k)) CoveredByHead
    else if (frameCoveredByFloor(n, k, w)) CoveredByFloor
    else Land

  // A floor-skipped frame is not a dropped frame: it *claims its bytes*. The
  // offset it ends at is exactly the head the replica now holds, so the applied
  // position advances over it while the dataset stays untouched — anything else
  // would stall the stream or lose the position.
  pure def applyFloorSkip(n: NodeState, k: Offset): NodeState =
    { ...n, applied: k, floor_skipped: n.floor_skipped + 1 }

  // Recomputed from the outcome, defects-family convention: a mutation that
  // lands the frame anyway, that forgets to advance the head, or that drops the
  // count shows up as a defect rather than passing silently.
  pure def floorSkipDisposedOk(pre: NodeState, post: NodeState, k: Offset): bool =
    and {
      post.data == pre.data,
      post.applied == k,
      post.recv == pre.recv,
      post.floor_skipped == pre.floor_skipped + 1,
    }

  // Recomputed from the outcome so a mutation that lands a covered frame — or
  // drops it without counting — shows up in the defect ghost rather than
  // passing silently (defects-family convention).
  pure def skipDisposedOk(pre: NodeState, post: NodeState): bool =
    and {
      post.data == pre.data,
      post.applied == pre.applied,
      post.recv == pre.recv,
      post.frames_skipped == pre.frames_skipped + 1,
    }

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
  // Ruling R16 (issue 35): a restarted node's offsets come from crash recovery,
  // so the stint is marked `recovered` and the floors it may have held do not
  // survive — the vector described a payload, and what came back off disk is not
  // that payload. Only an install clears the mark.
  //
  // *Model gap, deliberate.* `restartNodeAs` comes back as a Primary and
  // `inv_primary_role_is_terminal` keeps it there, so no reachable state in this
  // model presents a *recovered replica* at a PSYNC. R16's guard is therefore
  // stated and checked over the pure decision
  // (`recoveredStintRefusesTheWindowGrantTest`) rather than driven through the
  // machine, and the reachable half lives in the implementation's own forcing
  // tests (`a_recovered_stint_refuses_every_window_grant`,
  // `a_recovered_identity_comes_back_with_its_floors`).
  pure def applyRestart(pre: NodeState, unclean: bool, keep: Offset, freshGen: Gen): NodeState =
    if (restartKeepsIdentity(pre, unclean, keep))
      { ...pre, role: Primary, data: pre.data.slice(0, keep), recv: keep, applied: keep,
        floor: UNARMED, link: None, last_departure: None,
        floors: noFloors, recovered: true }
    else
      { ...pre, role: Primary, data: pre.data.slice(0, keep), recv: keep, applied: keep,
        floor: UNARMED, link: None, last_departure: None,
        floors: noFloors, recovered: true,
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
