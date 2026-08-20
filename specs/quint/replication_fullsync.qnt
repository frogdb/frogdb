// Full-sync / PSYNC handoff — properties, witnesses and scenarios.
//
// Phase-3 replication model of the primary -> replica sync-to-stream handoff:
// the PSYNC arm decision, the checkpoint cut, the trailer install, the backlog
// splice, and the replication-id / offset pairing across promotion and restart.
// Built from the spec rows, not transcribed from the code.
//
// Two defect classes are the reason this model exists:
//
//   1. **The full-sync checkpoint acked-write-loss class.** `snapshot_offset` is
//      captured before the checkpoint is cut, so an acknowledged write that
//      lands between the capture and the cut must be either in the payload or in
//      the replayed range — never in neither. The model makes the losing shape
//      expressible (`defects.cutBelowGrant`, and semantically
//      `inv_applied_covered_by_data`) rather than excluding it by construction.
//   2. **The issue-24 replid2-shift / atomic-offset-pairing rulings.** Every
//      identity write carries its paired offset and window in the same step, and
//      a primary's (generation, live offset) pair never regresses
//      lexicographically. `inv_replid_offset_paired` and
//      `inv_identity_pair_monotone` are the checkable forms.
//
// Rows and catalogued invariants this module asserts against:
//   TR-REPLICATION-001  inv_partial_grant_sound
//   TR-REPLICATION-002  spliceStream seeds ResumeOffset at the splice point
//   TR-REPLICATION-003  inv_payload_covers_grant
//   TR-REPLICATION-005  inv_no_acked_write_lost_across_fullsync
//   TR-REPLICATION-006  spliceOverEvictedRangeFailsTest
//   TR-REPLICATION-014  inv_offsets_ordered
//   TR-REPLICATION-015  inv_applied_covered_by_data
//   TR-REPLICATION-016  promotionMidSyncTest, inv_second_offset_not_above_live
//   TR-REPLICATION-018  inv_ack_never_above_live_head
//   TR-REPLICATION-019  partialResyncViaReplid2WindowTest
//   TR-REPLICATION-020  inv_failover_window_whole
//   TR-REPLICATION-019  overshippedTailMeetsFailoverTest (the window resume)
//   TR-REPLICATION-021  uncleanRestartMintsFreshIdTest, inv_restart_pairs_offset
//   TR-REPLICATION-022  inv_one_session_per_announced_identity,
//                       inv_superseded_records_no_departure
//   TR-REPLICATION-034  inv_no_forked_tail_above_the_claim (R5, model-only)
//   FM-REPLICATION-001  inv_reapply_is_a_noop (R4)
//   FM-REPLICATION-065  inv_reapply_is_a_noop (R7, the receiver-side skip)
//   FM-REPLICATION-004  inv_payload_covers_grant, inv_applied_covered_by_data
//   FM-REPLICATION-005  inv_splice_continuity
//   FM-REPLICATION-012  spliceOverEvictedRangeFailsTest
//   FM-REPLICATION-013  inv_partial_grant_sound
//   FM-REPLICATION-014  backlogEvictionForcesFullResyncTest
//   FM-REPLICATION-015  inv_splice_continuity
//   FM-REPLICATION-016  inv_backlog_floor_sound
//   FM-REPLICATION-019  inv_second_offset_not_above_live, inv_failover_window_whole
//   FM-REPLICATION-020  inv_replid_offset_paired
//   INV-OFFSET-1        inv_offsets_ordered
//   INV-OFFSET-3        inv_ack_never_above_live_head
//   INV-OFFSET-4        inv_second_offset_not_above_live
//   INV-REPLID-1        inv_failover_window_whole
//   INV-REPLID-2        inv_second_offset_not_above_live
//   INV-REPLID-3        inv_replids_distinct
//   INV-BACKLOG-1       inv_backlog_floor_sound
//   INV-BACKLOG-2       inv_partial_grant_sound
//   INV-BACKLOG-3       inv_backlog_floor_sound
//   INV-SESSION-1       inv_link_points_at_a_live_session
//   INV-SESSION-2       inv_one_session_per_announced_identity
//
// **R4 (2026-08-20 ruling) — the checkpoint-cut overlap is offset-addressed.**
// The discrepancy this block used to record is ruled. FM-REPLICATION-004
// declares the safe direction structural — `snapshot_offset <= payload data` —
// FM-REPLICATION-001 lists the resulting overlap as a deliberate non-guarantee,
// and R4 rules the overlap harmless *by construction*: replay is
// offset-addressed, so a frame at or below the offset the replica claims is
// never delivered, and re-delivery is a no-op. FM-REPLICATION-004/001 stand;
// issue-24's amendment point 2 narrows to what it actually forces — the
// persisted offset commits atomically with the write it names
// (`inv_restart_pairs_offset`). The model asserts the licensed half:
// `inv_reapply_is_a_noop` makes the no-op falsifiable rather than incidental,
// and `coverage.reapplied` (`witnessReappliedOverlap`, pinned deterministically
// by `writeDuringTransferSurvivesTest`) is the witness that the overlap is
// exercised at all.
//
// **R7 (2026-08-19 ruling, issue 34) — the skip is receiver-authoritative.**
// R4's follow-up is ruled and shipped. The sender-side checks R4 leaned on —
// FM-REPLICATION-015's sequence dedup (`frame.sequence <= resume_offset`) plus
// FM-REPLICATION-012's contiguity check on the extraction — are enforced
// against the offset the replica *claimed*, so a mis-computed resume point on
// the primary put a re-applied write straight into the replica's keyspace.
// FM-REPLICATION-065 moves the authority to the receiver, Raft style: a frame
// at or below the replica's own applied head is ignored whole and counted,
// whoever sent it. `inv_reapply_is_a_noop` therefore stops being a pure
// abstraction guard — its third conjunct is modelled behaviour, driven by
// `redeliverFrameAt` (the invalid input a correct primary never sends) and
// witnessed by `witnessFrameSkipped` / `skipBelowAppliedHeadTest`.
//
// *What the receiver skip does not cover.* The checkpoint-cut overship sits
// strictly *above* the applied head — payload data the replica installed but
// does not claim — so the skip is not what makes its re-landing harmless. In
// this model's unit-write log it is harmless by construction (a position is
// overwritten with the identical write id), while a keyspace under verbatim
// propagation (`replication_forms`, `core/src/shard/post_execution.rs:87` —
// "everything else propagates verbatim") has no such property, and
// FM-REPLICATION-015's own not-observable says so: re-applying a boundary
// command "for a non-idempotent replicated command ... is corruption, not a
// no-op". That range is R5's subject, not R7's: `truncateAboveClaim` removes
// the class at the partial-resync accept (`inv_no_forked_tail_above_the_claim`,
// TR-REPLICATION-034), which is where the residue is tracked.
//
// **R5 (2026-08-20 ruling) — the overshipped tail is truncated above the claim.**
// A replica that installs a payload covering more than its granted offset holds
// writes its own offset does not claim. If the primary's history is replaced
// before the replay reconciles that tail — a lossy restart, a promotion — the
// replica is still inside the failover window at its *claimed* offset and is
// granted a partial resync. R5 rules that the accept discards everything above
// the claim before splicing (Raft-style divergence truncation,
// `truncateAboveClaim`), which removes by construction both the overwrite and
// the residue class the model previously could not see: a forked position the
// *new* history never reaches, which no reconnect would ever ask for again.
// What was tracked as lawful correction is now forbidden
// (`inv_no_forked_tail_above_the_claim`), and `coverage.overshipTruncated`
// records the drop. `overshippedTailMeetsFailoverTest` pins the whole sequence.
// The rule is **model-only today**: the Rust `+CONTINUE` accept
// (`replica/connection.rs:300`) shifts the replication id and resumes, and
// discards nothing.
//
// R5 governs the partial-resync **accept** and nothing else. `applyPromote`
// does not truncate, so a node promoted while holding an overshipped tail still
// carries positions above its own `recv`; that path is
// `promotedOvershipDoesNotServeForkedTailTest`, and it stays governed by the
// serve-side properties (a promoted node does not serve above its head), not by
// the truncation rule.
module replication_fullsync {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"
  import replication_fullsync_types.* from "./replication_fullsync_types"
  import replication_fullsync_logic.* from "./replication_fullsync_logic"
  import replication_fullsync_machine.* from "./replication_fullsync_machine"

  // --- Acked-write survival across the handoff ---

  // The property this model was built for. For every streaming session whose
  // replica is on the primary's own history, everything the replica claims to
  // have landed is actually present, at the right position, with the right
  // content. An acked write dropped at the checkpoint cut — in neither the
  // payload nor the replayed range — breaks this, as does a divergent history
  // served under a matching replication id.
  val inv_no_acked_write_lost_across_fullsync: bool =
    SIDS.forall(k => {
      val s = sessions.get(k)
      val p = nodes.get(s.primary)
      val r = nodes.get(s.replica)
      (s.phase == Streaming and r.replid == p.replid)
        implies prefixAgrees(r.data, p.data, r.applied)
    })

  // The structural half of the same class, and the one that catches the
  // historical missing-WAL-drain defect directly: a node never claims to have
  // landed more than its dataset holds. A checkpoint cut below the granted
  // offset installs `applied = grant_offset > payload.length()` and trips this.
  val inv_applied_covered_by_data: bool =
    NODES.forall(n => nodes.get(n).applied <= nodes.get(n).data.length())

  // The cut itself, as a ghost over the ruled precondition (drain before
  // export). FM-REPLICATION-004's not-observable: `offset > data`.
  val inv_payload_covers_grant: bool = not(defects.cutBelowGrant)

  // --- Splice continuity ---

  // The replayed range and the live tail leave no gap between them, and no
  // streamed frame contradicts a position inside the window its grant claims
  // agreement on. See the header notes for why the *overlap* case and the
  // overshipped tail above the grant are tracked rather than asserted.
  val inv_splice_continuity: bool =
    not(defects.spliceGap) and not(defects.spliceDiverged)

  // R4: re-delivery of a position the replica already holds is a no-op, and a
  // frame landing never moves the applied position backwards. The first half is
  // what makes FM-REPLICATION-004's structural overship harmless in the model's
  // write-log abstraction; the second is the ordering the offset addressing
  // rests on. Both are true by construction of `applyFrameToData`/`applyFrameAt`
  // — the ghosts exist so a mutation of either cannot pass silently.
  //
  // R7 (issue 34, FM-REPLICATION-065) adds the third conjunct and is the one
  // that is *behaviour* rather than construction: a frame at or below the
  // replica's applied head is ignored whole and counted, whatever the sender
  // believed. `redeliverFrameAt` admits the frame no correct primary would
  // send, `frameIsCovered` decides, and `skipDisposedOk` recomputes the ruled
  // disposition — so narrowing the comparison to `<`, landing the frame, or
  // dropping it without counting all latch `defects.skippedFrameLanded`.
  val inv_reapply_is_a_noop: bool =
    not(defects.reapplyChangedData)
      and not(defects.replayRegressedApplied)
      and not(defects.skippedFrameLanded)

  // R5: no frame ever lands on a divergent position *above* the offset its
  // grant claims. The partial-resync accept truncates the replica's data to its
  // claim (`truncateAboveClaim`), so a forked tail cannot survive into the
  // splice to be overwritten — or, worse, to be left as residue the new history
  // never reaches.
  val inv_no_forked_tail_above_the_claim: bool = not(defects.forkedTailApplied)

  // --- Partial-resync soundness ---

  // A `+CONTINUE` is granted only over history the primary can actually serve:
  // an id it heads or holds a failover window for, and an offset the armed
  // backlog floor still covers. Either half missing means the replica resumes
  // over a range that was never sent or never existed.
  val inv_partial_grant_sound: bool =
    not(defects.grantBelowFloor) and not(defects.grantForeignHistory)

  // --- Identity / offset pairing (issue-24) ---

  // No reachable state observes a replication id updated without the offset and
  // window that belong with it.
  val inv_replid_offset_paired: bool = not(defects.tornIdentity)

  // The recovered offset commits with the data it names.
  val inv_restart_pairs_offset: bool = not(defects.restartOffsetTorn)

  // The (generation, live offset) pair never regresses lexicographically across
  // *any* step into Primary: writes raise the offset at a fixed generation, and
  // every event that lowers the offset mints a strictly greater generation in
  // the same step. A restart that keeps its id while rewinding its offset — the
  // issue-24 defect — breaks this immediately.
  //
  // R6 (2026-08-20 ruling) dropped the `prev.primary` antecedent this carried
  // until battery row I09. The pair is an *operator-facing* total order — a
  // later observation is never below an earlier one — so a promotion is inside
  // the order, not exempt from it: `applyPromote` rewinds `recv` to `applied`
  // but mints a strictly greater generation in the same step, so gen-domination
  // carries the pair forward (`promotionMidSyncTest` pins exactly that shape:
  // offset 3 -> 2 under gen 1 -> 2). Widening also removes the shadow's
  // `primary` flag from the invariant's reach, which is why
  // `inv_primary_role_is_terminal` keeps it honest on its own.
  val inv_identity_pair_monotone: bool =
    NODES.forall(n => {
      val prev = ctl.prev_pair.get(n)
      val cur = nodes.get(n)
      (cur.role == Primary)
        implies identityPairOk(prev.gen, prev.off, cur.replid, cur.recv)
    })

  // The failover window is whole or absent: the inherited id exists exactly when
  // its frozen boundary does.
  val inv_failover_window_whole: bool =
    NODES.forall(n => {
      val s = nodes.get(n)
      (s.replid2 != None) iff (s.second_offset != NO_WINDOW)
    })

  val inv_replids_distinct: bool =
    NODES.forall(n => {
      val s = nodes.get(n)
      optForall(s.replid2, g => g != s.replid)
    })

  // The frozen boundary never exceeds the head that node is prepared to serve.
  val inv_second_offset_not_above_live: bool =
    NODES.forall(n => {
      val s = nodes.get(n)
      (s.role == Primary and s.second_offset != NO_WINDOW)
        implies s.second_offset <= s.recv
    })

  // --- Offsets and backlog ---

  val inv_offsets_ordered: bool =
    NODES.forall(n => nodes.get(n).applied <= nodes.get(n).recv)

  // An armed floor names a real, reachable resume point inside the history.
  val inv_backlog_floor_sound: bool =
    NODES.forall(n => {
      val s = nodes.get(n)
      (s.floor != UNARMED) implies (s.floor >= 0 and s.floor <= s.recv)
    })

  // --- ACK ingest (issue-21) ---

  // AckedOffset is never credited past the primary's own live head, and the
  // ignore branch never writes it — the clamp the amendment reversed.
  val inv_ack_never_above_live_head: bool =
    not(defects.ackAboveHead) and not(defects.ackWrittenOnIgnore)

  // --- Sessions (issue-22) ---

  // At most one registered session per announced replica identity per primary.
  val inv_one_session_per_announced_identity: bool =
    SIDS.forall(a => SIDS.forall(b => {
      val sa = sessions.get(a)
      val sb = sessions.get(b)
      and {
        a != b,
        sa.phase != Disconnected,
        sb.phase != Disconnected,
        sa.primary == sb.primary,
        sa.identity != None,
        sa.identity == sb.identity,
      } implies false
    }))

  // A replica's link, when it has one, names a session that is still registered
  // and belongs to it.
  val inv_link_points_at_a_live_session: bool =
    NODES.forall(n => optForall(nodes.get(n).link, k => {
      val s = sessions.get(k)
      s.phase != Disconnected and s.replica == n
    }))

  // The predecessor kicked by a duplicate registration records no departure.
  val inv_superseded_records_no_departure: bool = not(defects.supersededDeparture)

  // --- Structural invariants (mutation-battery gap closure, T10b) ---
  //
  // Each of these closes a battery row. Some kill their mutation outright; the
  // rest are the *falsifiable tripwires* behind an equivalent-mutant argument —
  // a guard whose removal is harmless only because the invariant below holds, so
  // the claim "that conjunct is redundant" is checked rather than asserted in
  // prose. The row each one answers is named at its definition; the verdict
  // table lives in `.scratch/formal-spec/2026-08-20-fullsync-battery.md`.

  // Roles are monotone: a node that was a primary in the previous state still
  // is one. Rows I10 and M03 (the `prev.primary` half of the pairing shadow).
  val inv_primary_role_is_terminal: bool =
    NODES.forall(n => ctl.prev_pair.get(n).primary implies nodes.get(n).role == Primary)

  // A primary serves exactly what it has applied — it never carries a decoded
  // tail of its own. Rows M02, M69 (with inv_applied_covered_by_data, this is
  // what makes the frame consumer's `pn.data` bound redundant).
  val inv_primary_applied_is_head: bool =
    NODES.forall(n =>
      (nodes.get(n).role == Primary) implies (nodes.get(n).applied == nodes.get(n).recv))

  // A primary holds no upstream link of its own. Row F65 (the restart's
  // `link: None` write).
  val inv_primary_holds_no_link: bool =
    NODES.forall(n => (nodes.get(n).role == Primary) implies (nodes.get(n).link == None))

  // Only a primary ever holds a failover window: a replica's window is closed.
  // Rows F33, F34 (the trailer's window clear) and I05.
  val inv_replica_holds_no_window: bool =
    NODES.forall(n => (nodes.get(n).role == Replica) implies and {
      nodes.get(n).replid2 == None,
      nodes.get(n).second_offset == NO_WINDOW,
    })

  // Only a primary ever arms a backlog floor. Row F23.
  val inv_only_primaries_arm_a_floor: bool =
    NODES.forall(n => (nodes.get(n).role == Replica) implies (nodes.get(n).floor == UNARMED))

  // A registered session's replica is a replica. Rows M06, M94.
  val inv_registered_session_replica_is_replica: bool =
    SIDS.forall(k => (sessions.get(k).phase != Disconnected)
      implies (nodes.get(sessions.get(k).replica).role == Replica))

  // Before Streaming a session is always held by its replica's own live link:
  // the corpse is a Streaming-only shape. Row M53.
  val inv_prestream_session_is_linked: bool =
    SIDS.forall(k => {
      val s = sessions.get(k)
      and { s.phase != Disconnected, s.phase != Streaming }
        implies linkIs(nodes.get(s.replica), k)
    })

  // ...and before Streaming nothing is decoded-but-unapplied: the gap between
  // `recv` and `applied` opens only under a live stream. Row M71.
  val inv_prestream_link_has_nothing_pending: bool =
    SIDS.forall(k => {
      val s = sessions.get(k)
      val r = nodes.get(s.replica)
      and { s.phase != Disconnected, s.phase != Streaming, linkIs(r, k) }
        implies r.applied == r.recv
    })

  // A streaming replica never received past the history its primary holds.
  // Row M74 (the sentinel bound on the apply path).
  val inv_linked_replica_recv_within_primary_data: bool =
    SIDS.forall(k => {
      val s = sessions.get(k)
      val r = nodes.get(s.replica)
      and { s.phase == Streaming, linkIs(r, k) }
        implies r.recv <= nodes.get(s.primary).data.length()
    })

  // A replica claiming a primary's own history never claims more of it than
  // that primary holds. Row F01 (the `off <= p.recv` bound on the primary arm).
  val inv_replica_not_ahead_of_matching_primary: bool =
    NODES.forall(r => NODES.forall(p => and {
      nodes.get(r).role == Replica,
      nodes.get(p).role == Primary,
      nodes.get(r).replid == nodes.get(p).replid,
    } implies nodes.get(r).applied <= nodes.get(p).recv))

  // Every payload a session installed came out of a checkpoint cut. Row M49 —
  // and the only reader of `payload_cut`, which the battery found otherwise
  // dead.
  val inv_installed_payload_was_cut: bool =
    SIDS.forall(k => {
      val s = sessions.get(k)
      and {
        s.arm == FullSnapshot,
        or { s.phase == StreamingCheckpoint, s.phase == PayloadInstalled, s.phase == Streaming },
      } implies s.payload_cut
    })

  // --- Witnesses ---

  val witnessFullSyncStreaming: bool = coverage.fullSyncStreaming
  val witnessWriteDuringTransfer: bool = coverage.overshipped
  val witnessReappliedOverlap: bool = coverage.reapplied
  val witnessOvershipTruncated: bool = coverage.overshipTruncated
  val witnessTailApplied: bool = coverage.tailApplied
  val witnessPartialViaSecondary: bool = coverage.partialViaSecondary
  val witnessEvictionForcedFullResync: bool = coverage.evictionForcedFull
  val witnessSpliceAbandoned: bool = coverage.spliceAbandoned
  val witnessAckIgnored: bool = coverage.ackIgnored
  val witnessFrameSkipped: bool = coverage.frameSkipped
  val witnessSuperseded: bool = coverage.superseded
  val witnessCorpseSession: bool = coverage.corpseSession
  val witnessSettleDiscardedFrames: bool = coverage.settleDiscardedFrames
  val witnessUncleanRestartReminted: bool = coverage.uncleanRestartReminted
  val witnessBacklogEvicted: bool = NODES.exists(n => nodes.get(n).floor > 0)
  val witnessAckRecorded: bool = SIDS.exists(k => sessions.get(k).acked > 0)
  val witnessPromotedWithWindow: bool =
    NODES.exists(n => nodes.get(n).role == Primary and nodes.get(n).replid2 != None)
  val witnessPartialGrant: bool =
    SIDS.exists(k => sessions.get(k).phase != Disconnected and sessions.get(k).arm == PartialGrant)

  // --- Scenarios ---

  // Assertion frame: checks a predicate without advancing the model.
  action check(p: bool): bool = all {
    assert(p),
    keepNodes,
    keepSessions,
    keepCtl,
    keepDefects,
    keepCoverage,
  }

  // 1. Happy path: two acked writes, a full resync, and a replica streaming at
  //    the primary's head holding exactly the primary's history.
  run happyFullSyncTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).arm == FullSnapshot and sessions.get(1).grant_offset == 2))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(check(and {
        sessions.get(1).phase == Streaming,
        // TR-REPLICATION-002: the resume point is seeded from the grant.
        sessions.get(1).resume_offset == 2,
        sessions.get(1).payload_cut,
        nodes.get(2).applied == 2,
        nodes.get(2).replid == nodes.get(1).replid,
        nodes.get(2).data == nodes.get(1).data,
        inv_no_acked_write_lost_across_fullsync,
        inv_applied_covered_by_data,
        inv_splice_continuity,
        not(coverage.overshipped),
      }))

  // 2. A write acknowledged *after* the offset was captured and *before* the cut
  //    survives the handoff: it rides the payload, the backlog replay re-ships
  //    it, and the replica ends holding the primary's whole history.
  run writeDuringTransferSurvivesTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).grant_offset == 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(check(coverage.overshipped and not(defects.cutBelowGrant)))
      .then(deliverPayloadAt(1))
      .then(check(nodes.get(2).applied == 1 and nodes.get(2).data.length() == 2))
      .then(spliceStreamAt(1))
      .then(receiveFrameAt(1))
      .then(applyFrameAt(1))
      .then(check(and {
        nodes.get(2).applied == 2,
        nodes.get(2).data == nodes.get(1).data,
        coverage.reapplied,
        coverage.tailApplied,
        inv_no_acked_write_lost_across_fullsync,
        inv_splice_continuity,
        inv_applied_covered_by_data,
        // R4: the deterministic floor under `witnessReappliedOverlap`. The
        // re-landing of offset 2 — already in the payload — changed nothing,
        // and the applied position advanced rather than regressing.
        inv_reapply_is_a_noop,
      }))

  // 3. A replica promoted mid-sync freezes the inherited history at what it
  //    *applied*, not at what it received, and mints a fresh id in the same step
  //    (TR-REPLICATION-016, FM-REPLICATION-019, issue-24 atomic pairing).
  run promotionMidSyncTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(writeOnPrimaryAs(1))
      .then(receiveFrameAt(1))
      .then(check(nodes.get(2).recv == 3 and nodes.get(2).applied == 2))
      .then(promoteAs(2))
      .then(check(and {
        nodes.get(2).role == Primary,
        nodes.get(2).replid == FIRST_GEN + 1,
        nodes.get(2).replid2 == Some(FIRST_GEN),
        // frozen at the applied offset, inclusive — never at the received head
        nodes.get(2).second_offset == 2,
        nodes.get(2).recv == 2,
        nodes.get(2).floor == 2,
        // The links it held as a replica die with the role change.
        sessions.get(1).phase == Disconnected,
        // The pairing shadow holds the *immediately preceding* state, refreshed
        // by this very step: gen 1 at the received head, not yet a primary.
        ctl.prev_pair.get(2) == { gen: FIRST_GEN, off: 3, primary: false },
        coverage.settleDiscardedFrames,
        inv_replid_offset_paired,
        // R6's forcing pin. This is the step the widened
        // `inv_identity_pair_monotone` newly reaches: the predecessor was a
        // *replica* (`primary: false`), and the promotion rewinds the offset
        // (3 -> 2), so the pair advances only because the generation strictly
        // dominates (1 -> 2). Assert the domination directly as well, so a
        // mutation that kept the offset ordering by minting an equal generation
        // is caught here and not merely by the invariant's lexicographic form.
        lexGt((nodes.get(2).replid, nodes.get(2).recv),
              (ctl.prev_pair.get(2).gen, ctl.prev_pair.get(2).off)),
        nodes.get(2).replid > ctl.prev_pair.get(2).gen,
        nodes.get(2).recv < ctl.prev_pair.get(2).off,
        inv_identity_pair_monotone,
        inv_second_offset_not_above_live,
        inv_failover_window_whole,
      }))

  // 4. After that promotion a sibling replica resumes through the *inherited*
  //    window with a partial grant, adopting the new head's id at the
  //    `+CONTINUE` (TR-REPLICATION-019, FM-REPLICATION-019).
  run partialResyncViaReplid2WindowTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(3, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(cutCheckpointAt(2))
      .then(deliverPayloadAt(2))
      .then(spliceStreamAt(2))
      .then(promoteAs(2))
      .then(endSessionAt(1))
      .then(psyncRequestAs(3, 2, true, 1))
      .then(check(and {
        sessions.get(1).arm == PartialGrant,
        sessions.get(1).grant_offset == 2,
        nodes.get(3).replid == FIRST_GEN + 1,
        coverage.partialViaSecondary,
        inv_partial_grant_sound,
        inv_no_acked_write_lost_across_fullsync,
      }))

  // 5. Backlog eviction raises the floor past the replica's offset, so the next
  //    PSYNC degrades to a full resync instead of continuing over a range the
  //    primary no longer holds (FM-REPLICATION-014, FM-REPLICATION-016).
  run backlogEvictionForcesFullResyncTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(endSessionAt(1))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(evictBacklogAs(1))
      .then(evictBacklogAs(1))
      .then(check(and {
        nodes.get(1).floor == 2,
        nodes.get(2).applied == 1,
        // The streaming teardown above recorded a graceful departure.
        nodes.get(1).last_departure == Some(Graceful),
      }))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(and {
        sessions.get(1).arm == FullSnapshot,
        coverage.evictionForcedFull,
        inv_partial_grant_sound,
      }))

  // 6. The window can close *between* the grant and the extraction. The replay
  //    is then not contiguous, so it does not happen at all: the splice is
  //    disabled and the link is abandoned before Streaming
  //    (FM-REPLICATION-012, TR-REPLICATION-006).
  run spliceOverEvictedRangeFailsTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(endSessionAt(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).arm == PartialGrant and sessions.get(1).grant_offset == 1))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(evictBacklogAs(1))
      .then(evictBacklogAs(1))
      .then(spliceStreamAt(1))
      .fail()

  run truncatedSpliceIsAbandonedTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(endSessionAt(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(evictBacklogAs(1))
      .then(evictBacklogAs(1))
      .then(abandonSpliceAt(1))
      .then(check(and {
        sessions.get(1).phase == Disconnected,
        nodes.get(2).link == None,
        coverage.spliceAbandoned,
      }))

  // 7. Issue-21 ruling: an ack above the primary's live head is ignored whole —
  //    AckedOffset is untouched, the session survives, a counter moves.
  run ackAboveLiveHeadIgnoredTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(recordAckAt(1, 2))
      .then(check(sessions.get(1).acked == 2))
      // AckedOffset never regresses: a late, lower, *honest* ack is absorbed.
      .then(recordAckAt(1, 1))
      .then(check(sessions.get(1).acked == 2))
      .then(ignoreAckAt(1, 3))
      .then(check(and {
        sessions.get(1).acked == 2,
        sessions.get(1).phase == Streaming,
        nodes.get(1).acks_ignored == 1,
        coverage.ackIgnored,
        inv_ack_never_above_live_head,
      }))

  // 8. Issue-22 ruling: a reconnecting replica supersedes its own corpse — the
  //    predecessor session goes, and no departure is recorded for it.
  run supersededPredecessorTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(dropLinkAt(1))
      .then(check(sessions.get(1).phase == Streaming and nodes.get(2).link == None))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(and {
        sessions.get(1).phase == Disconnected,
        sessions.get(2).phase != Disconnected,
        nodes.get(1).last_departure == None,
        coverage.superseded,
        coverage.corpseSession,
        inv_one_session_per_announced_identity,
        inv_superseded_records_no_departure,
      }))

  // 9. Issue-24 (a)+(b): a restart that did not recover its dataset intact mints
  //    a fresh id and shifts the old one into the failover window at the offset
  //    it can prove — and the offset it publishes is exactly what the recovered
  //    data holds.
  run uncleanRestartMintsFreshIdTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(restartNodeAs(1, true, 1))
      .then(check(and {
        nodes.get(1).replid == FIRST_GEN + 1,
        nodes.get(1).replid2 == Some(FIRST_GEN),
        nodes.get(1).second_offset == 1,
        nodes.get(1).recv == 1,
        nodes.get(1).data.length() == 1,
        // A reboot comes up with an unarmed backlog: nothing is resumable until
        // the first push arms it again.
        nodes.get(1).floor == UNARMED,
        coverage.uncleanRestartReminted,
        inv_restart_pairs_offset,
        inv_replid_offset_paired,
        inv_identity_pair_monotone,
        inv_applied_covered_by_data,
      }))

  // 10. …and a replica that ran past the recovered boundary cannot be granted a
  //     `+CONTINUE` over history the restarted primary no longer holds.
  run restartRefusesAheadReplicaTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(endSessionAt(1))
      .then(restartNodeAs(1, true, 1))
      .then(check(and {
        nodes.get(2).applied == 2,
        nodes.get(2).link == None,
        // The reboot forgets the departure the session before it recorded.
        nodes.get(1).last_departure == None,
      }))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(and {
        sessions.get(1).arm == FullSnapshot,
        // The window refused it, not the floor: this is not the eviction path.
        not(coverage.evictionForcedFull),
        // On the full arm the replica adopts neither half of the identity pair
        // until the trailer lands: it keeps its old id and rewinds to zero.
        nodes.get(2).replid == FIRST_GEN,
        nodes.get(2).recv == 0,
        nodes.get(2).applied == 0,
        inv_partial_grant_sound,
        inv_identity_pair_monotone,
      }))

  // 11. FM-REPLICATION-065 (R7): the primary re-ships frames the replica has
  //     already applied — a mis-computed resume point, the shape the sender-side
  //     dedup is supposed to prevent and sometimes does not. The replica is
  //     receiver-authoritative: it ignores both, at the boundary (a frame ending
  //     exactly at the applied head) and below it, and counts them. Nothing about
  //     its history moves, so the primary's own history stays the one it holds.
  run skipBelowAppliedHeadTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(check(nodes.get(2).applied == 2 and nodes.get(2).frames_skipped == 0))
      // The boundary: `sequence == applied`. This is the case a `<` comparison
      // would let through.
      .then(redeliverFrameAt(1, 2))
      // …and a frame from well below the head.
      .then(redeliverFrameAt(1, 1))
      .then(check(and {
        nodes.get(2).applied == 2,
        nodes.get(2).recv == 2,
        nodes.get(2).data == nodes.get(1).data,
        // Ignored *and* counted — a silent drop leaves the operator with no
        // evidence of the sender bug (the R-21 "ignore + count" shape).
        nodes.get(2).frames_skipped == 2,
        coverage.frameSkipped,
        inv_reapply_is_a_noop,
        inv_no_acked_write_lost_across_fullsync,
        inv_offsets_ordered,
      }))

  // The FM-REPLICATION-004 overshipped tail meeting a failover, and R5's
  // truncation removing it. Pinned deterministically because the random walk
  // reaches the shape in roughly 0.2% of traces at 4000x40 and not at all at the
  // gate's depth — it needs an overship, a lossy restart, and a re-grant of the
  // *same* replica, in that order, before the replay has reconciled the tail.
  //
  // The shape: replica 2 installs a payload covering offset 2 while its granted
  // offset is 1, so it holds a write its own offset does not claim. Primary 1
  // then restarts having recovered only offset 1, mints a new history, and
  // writes a *different* write at offset 2. The replica is inside the failover
  // window at offset 1, so it is granted a partial resync.
  //
  // R5 (2026-08-20) rules what happens next. Before the ruling the forked
  // position survived the accept and was overwritten by the replay — lawful
  // correction, tracked in `coverage`, and silent about the residue case where
  // the new history never reaches that position at all. Now the accept
  // truncates to the claim, so the fork is gone *before* the splice: the replay
  // appends rather than overwriting, `inv_no_forked_tail_above_the_claim` holds,
  // and no position the new history will not reach can be left behind.
  // `coverage.overshipTruncated` records the drop.
  run overshippedTailMeetsFailoverTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(check(and {
        // Overshipped: the payload carried offset 2, the grant claimed offset 1.
        nodes.get(2).data.length() == 2,
        nodes.get(2).applied == 1,
        coverage.overshipped,
        not(defects.cutBelowGrant),
      }))
      .then(restartNodeAs(1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(and {
        sessions.get(2).arm == PartialGrant,
        sessions.get(2).grant_offset == 1,
        coverage.partialViaSecondary,
        // R5: the accept dropped the forked position rather than leaving it for
        // the replay. The replica now holds exactly what it claims.
        coverage.overshipTruncated,
        nodes.get(2).data.length() == 1,
        nodes.get(2).applied == 1,
        prefixAgrees(nodes.get(2).data, nodes.get(1).data, nodes.get(2).applied),
      }))
      .then(spliceStreamAt(2))
      .then(receiveFrameAt(2))
      .then(applyFrameAt(2))
      .then(check(and {
        // The new history's offset 2 *appended* — there was nothing left above
        // the claim to overwrite, so the forked-tail defect never latches.
        inv_no_forked_tail_above_the_claim,
        nodes.get(2).data == nodes.get(1).data,
        inv_splice_continuity,
        inv_no_acked_write_lost_across_fullsync,
        prefixAgrees(nodes.get(2).data, nodes.get(1).data, nodes.get(2).applied),
      }))

  // --- Scenarios added by the T10b mutation battery (gap closure) ---
  //
  // Every scenario below exists to kill a specific battery row that both
  // oracles missed; the row id is named on each. Disabled-action scenarios use
  // `.fail()`: the run fails precisely because the guard the row deletes is the
  // one holding the action back.

  // Row M10/M12: the initial state, and the first push arming the floor at the
  // command's start offset (the pre-write head).
  run initStateTest =
    init
      .then(check(and {
        nodes.get(1).role == Primary,
        nodes.get(1).replid == FIRST_GEN,
        nodes.get(1).floor == UNARMED,
        nodes.get(1).recv == 0,
        nodes.get(2).role == Replica,
        nodes.get(2).replid == NO_HISTORY,
        nodes.get(2).floor == UNARMED,
        nodes.get(2).recv == 0,
      }))
      .then(writeOnPrimaryAs(1))
      .then(check(nodes.get(1).floor == 0 and nodes.get(1).recv == 1))

  // Row F09: a replica that has never held a history full-syncs even when the
  // primary's inherited window nominally covers the `PSYNC ? -1` sentinel.
  run noHistoryStillFullSyncsTest =
    init
      .then(promoteAs(2))
      .then(check(and {
        nodes.get(2).role == Primary,
        nodes.get(2).replid2 == Some(NO_HISTORY),
        nodes.get(2).second_offset == 0,
        nodes.get(2).floor == 0,
      }))
      .then(psyncRequestAs(3, 2, true, 1))
      .then(check(sessions.get(1).arm == FullSnapshot))

  // Row F07: an unarmed floor refuses a `+CONTINUE` outright, even to a replica
  // whose id and offset the window would otherwise admit.
  run unarmedFloorRefusesPartialTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(endSessionAt(1))
      .then(restartNodeAs(1, false, 1))
      .then(check(and {
        nodes.get(1).floor == UNARMED,
        nodes.get(1).replid == FIRST_GEN,
        nodes.get(2).replid == FIRST_GEN,
        nodes.get(2).applied == 1,
      }))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).arm == FullSnapshot))

  // Row F24: an unarmed buffer has nothing to evict.
  run unarmedBacklogHasNothingToEvictTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(restartNodeAs(1, false, 1))
      .then(evictBacklogAs(1))
      .fail()

  // Row F25: eviction happens strictly above capacity, never at it.
  run evictionNeedsAboveCapacityTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(evictBacklogAs(1))
      .fail()

  // Row F39: an empty replay range needs no backlog at all — a replica granted
  // at the primary's own head splices over an unarmed buffer.
  run spliceAtHeadOverUnarmedBacklogTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(restartNodeAs(1, false, 1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(check(and {
        nodes.get(1).floor == UNARMED,
        sessions.get(1).phase == Streaming,
        coverage.fullSyncStreaming,
      }))

  // Rows F11, M97: the arm decision reads the replica's *applied* offset, not
  // its received head. Replica 3 holds a decoded-but-unapplied frame; the
  // inherited window covers what it applied and nothing more, so the resume is
  // granted. (M97: the promoted node discarded nothing, so the settle ghost
  // stays dark.)
  run windowCheckedAtAppliedTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(3, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(cutCheckpointAt(2))
      .then(deliverPayloadAt(2))
      .then(spliceStreamAt(2))
      .then(writeOnPrimaryAs(1))
      .then(receiveFrameAt(1))
      .then(check(nodes.get(3).recv == 2 and nodes.get(3).applied == 1))
      .then(promoteAs(2))
      .then(endSessionAt(1))
      .then(psyncRequestAs(3, 2, true, 1))
      .then(check(and {
        sessions.get(1).arm == PartialGrant,
        sessions.get(1).grant_offset == 1,
        nodes.get(2).second_offset == 1,
        coverage.partialViaSecondary,
        not(coverage.settleDiscardedFrames),
        inv_partial_grant_sound,
      }))

  // Rows M27, M28, M32: a reconnect resumes from what the replica *applied* —
  // the previous connection's decoded-but-unapplied bytes died with it — and
  // the new session starts with no ack credited.
  run psyncResetsReplicaPositionTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(writeOnPrimaryAs(1))
      .then(receiveFrameAt(1))
      .then(dropLinkAt(1))
      .then(check(nodes.get(2).recv == 2 and nodes.get(2).applied == 1))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(and {
        sessions.get(2).arm == PartialGrant,
        sessions.get(2).grant_offset == 1,
        sessions.get(2).acked == 0,
        nodes.get(2).recv == 1,
        nodes.get(2).applied == 1,
        sessions.get(1).phase == Disconnected,
        coverage.superseded,
      }))

  // Row M46: the cut happens once, at the preparing phase.
  run cutHappensOnceTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(cutCheckpointAt(1))
      .fail()

  // Row M62: a splice is abandoned only when the replay range is gone.
  run abandonNeedsUnavailableRangeTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(abandonSpliceAt(1))
      .fail()

  // Row M66: frames arrive only after the splice, never during the transfer.
  run framesOnlyOnStreamingSessionTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(writeOnPrimaryAs(1))
      .then(receiveFrameAt(1))
      .fail()

  // Row M67 (issue-22): a corpse session holds no socket, so it cannot feed the
  // replica that has since left it — even while the session is still Streaming.
  run corpseCannotFeedReplicaTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(dropLinkAt(1))
      .then(writeOnPrimaryAs(1))
      .then(receiveFrameAt(1))
      .fail()

  // Row M83: ACK ingest belongs to the streaming phase.
  run ackOnlyWhileStreamingTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(recordAckAt(1, 1))
      .fail()

  // Row M87 (issue-21): an honest ack cannot take the ignore branch.
  run honestAckIsNotIgnoredTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(ignoreAckAt(1, 1))
      .fail()

  // Row M13: a replica accepts no local write.
  run replicaAcceptsNoWriteTest =
    init.then(writeOnPrimaryAs(2)).fail()

  // Row M18: PSYNC targets a primary.
  run psyncTargetsAPrimaryTest =
    init.then(psyncRequestAs(2, 3, true, 1)).fail()

  // Row M19: an already-linked replica does not open a second registration.
  run linkedReplicaDoesNotRepsyncTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(psyncRequestAs(2, 1, true, 2))
      .fail()

  // Row M92: a primary is not promoted.
  run primaryIsNotPromotedTest =
    init.then(promoteAs(1)).fail()

  // Row M98: the restart subject is a primary.
  run replicaIsNotRestartedTest =
    init.then(restartNodeAs(2, true, 0)).fail()

  // Row M99 (TR-REPLICATION-021): recovery never invents data. Node 2 holds an
  // overshipped tail — a payload position its own offset does not claim — and
  // a restart still cannot recover *past* what it applied.
  run restartCannotInventDataTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(promoteAs(2))
      .then(restartNodeAs(2, false, 2))
      .fail()

  // Row M68: the same overshipped tail, on a promoted primary, is never served.
  // The head it settled at is the head it streams from; the forked position
  // above it is residue no reconnect can ask for.
  run promotedOvershipDoesNotServeForkedTailTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(promoteAs(2))
      .then(psyncRequestAs(3, 2, true, 2))
      .then(cutCheckpointAt(2))
      .then(deliverPayloadAt(2))
      .then(spliceStreamAt(2))
      .then(receiveFrameAt(2))
      .fail()

  // Row M35 (TR-REPLICATION-022): an unannounced registration
  // (`listening_port == 0`) is exempt from dedup — it kicks nobody.
  run unannouncedRegistrationDoesNotDedupTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(dropLinkAt(1))
      .then(psyncRequestAs(2, 1, false, 2))
      .then(check(and {
        sessions.get(1).phase == Streaming,
        sessions.get(2).identity == None,
        not(coverage.superseded),
        inv_one_session_per_announced_identity,
      }))

  // Row M37 (issue-22): dedup is per-primary. Registering with a *different*
  // primary leaves the predecessor's session on the old one alone.
  run dedupIsPerPrimaryTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(dropLinkAt(1))
      .then(promoteAs(3))
      .then(psyncRequestAs(2, 3, true, 2))
      .then(check(and {
        sessions.get(1).phase == Streaming,
        sessions.get(1).primary == 1,
        sessions.get(2).primary == 3,
        not(coverage.superseded),
      }))

  // Row M109: tearing down a corpse clears only its own holder's link — never
  // the link the replica has since opened elsewhere.
  run corpseTeardownKeepsLiveLinkTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(dropLinkAt(1))
      .then(psyncRequestAs(2, 1, false, 2))
      .then(check(nodes.get(2).link == Some(2)))
      .then(endSessionAt(1))
      .then(check(and {
        nodes.get(2).link == Some(2),
        sessions.get(1).phase == Disconnected,
        sessions.get(2).phase != Disconnected,
        inv_link_points_at_a_live_session,
      }))

  // Row M106: and a link is dropped only by the session that holds it.
  run corpseLinkNotDroppableTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(dropLinkAt(1))
      .then(psyncRequestAs(2, 1, false, 2))
      .then(dropLinkAt(1))
      .fail()

  // Row F59: it is the *unclean* shutdown that mints a fresh id, not the
  // recovery point. A reboot that comes back holding everything it had applied
  // still comes up on a new history: an unclean stop is what makes the old id
  // untrustworthy (TR-021), and a full recovery does not make it trustworthy
  // again. `uncleanRestartMintsFreshIdTest` recovers *below* the applied
  // offset, so it cannot tell the two conditions apart on its own.
  run uncleanRestartAtFullRecoveryStillRemintsTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(check(nodes.get(1).applied == 1))
      .then(restartNodeAs(1, true, 1))
      .then(check(and {
        // Nothing was lost — the recovery point is the applied head…
        nodes.get(1).data.length() == 1,
        nodes.get(1).applied == 1,
        // …and the identity is reminted anyway, with the old one frozen at it.
        nodes.get(1).replid == FIRST_GEN + 1,
        nodes.get(1).replid2 == Some(FIRST_GEN),
        nodes.get(1).second_offset == 1,
        coverage.uncleanRestartReminted,
        inv_restart_pairs_offset,
        inv_replid_offset_paired,
      }))
}
