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
// **Recorded discrepancy, deliberately left unasserted.** FM-REPLICATION-004
// declares the safe direction structural — `snapshot_offset <= payload data` —
// and FM-REPLICATION-001 lists the resulting overlap as a deliberate
// non-guarantee ("re-applies a tail it already holds, which is idempotent for
// the replicated write stream"). Issue-24's amendment point 2 makes the opposite
// claim about the *same* shape: an offset biased low relative to the data it
// names gives non-idempotent replay (INCR/LPUSH/APPEND applied twice), which is
// why the persisted offset must commit in the same atomic unit as the write it
// names. The model records the overlap (`coverage.reapplied`) and asserts only
// the outcomes both readings agree are wrong: a gap, and a divergent position
// *inside the window the grant claims*. Resolving which reading governs the
// checkpoint cut is a design ruling, not a modelling choice.
//
// **The overshipped tail across a failover.** The same overship has a second
// consequence the rows do not speak to. A replica that installs a payload
// covering more than its granted offset holds writes its own offset does not
// claim. If the primary's history is replaced before the replay reconciles that
// tail — a lossy restart, a promotion — the replica is still inside the failover
// window at its *claimed* offset and is granted a partial resync, and the new
// history's frames then land on positions the old history wrote. Overwriting
// them is the correction, not the damage, so the model records it
// (`coverage.forkedTailReplaced`, pinned by `overshippedTailMeetsFailoverTest`)
// and does not treat it as divergence. What the model cannot see, because a
// unit-write log is not a keyspace, is residue: a forked position the *new*
// history never reaches, which no reconnect will ever ask for again. Whether
// FM-REPLICATION-004's structural overship needs a matching rule on the replica
// side is an open question for the spec, not something this model decides.
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

  // A primary's (generation, live offset) pair never regresses lexicographically:
  // writes raise the offset at a fixed generation, and every event that lowers
  // the offset mints a strictly greater generation in the same step. A restart
  // that keeps its id while rewinding its offset — the issue-24 defect — breaks
  // this immediately.
  val inv_identity_pair_monotone: bool =
    NODES.forall(n => {
      val prev = ctl.prev_pair.get(n)
      val cur = nodes.get(n)
      (prev.primary and cur.role == Primary)
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

  // --- Witnesses ---

  val witnessFullSyncStreaming: bool = coverage.fullSyncStreaming
  val witnessWriteDuringTransfer: bool = coverage.overshipped
  val witnessReappliedOverlap: bool = coverage.reapplied
  val witnessTailApplied: bool = coverage.tailApplied
  val witnessPartialViaSecondary: bool = coverage.partialViaSecondary
  val witnessEvictionForcedFullResync: bool = coverage.evictionForcedFull
  val witnessSpliceAbandoned: bool = coverage.spliceAbandoned
  val witnessAckIgnored: bool = coverage.ackIgnored
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
        inv_no_acked_write_lost_across_fullsync,
        inv_splice_continuity,
        inv_applied_covered_by_data,
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
        coverage.settleDiscardedFrames,
        inv_replid_offset_paired,
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
      .then(check(nodes.get(1).floor == 2 and nodes.get(2).applied == 1))
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
      .then(restartNodeAs(1, true, 1))
      .then(check(nodes.get(2).applied == 2 and nodes.get(2).link == None))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(and {
        sessions.get(1).arm == FullSnapshot,
        inv_partial_grant_sound,
        inv_identity_pair_monotone,
      }))

  // The FM-REPLICATION-004 overshipped tail meeting a failover, pinned
  // deterministically because the random walk reaches it in roughly 0.2% of
  // traces at 4000x40 and not at all at the gate's depth — it needs an overship,
  // a lossy restart, and a re-grant of the *same* replica, in that order, before
  // the replay has reconciled the tail. There is deliberately no
  // `witness*` for it: a witness the gate reads as 0 would be indistinguishable
  // from dead behaviour.
  //
  // The shape: replica 2 installs a payload covering offset 2 while its granted
  // offset is 1, so it holds a write its own offset does not claim. Primary 1
  // then restarts having recovered only offset 1, mints a new history, and
  // writes a *different* write at offset 2. The replica is inside the failover
  // window at offset 1, so it is granted a partial resync and the replay
  // overwrites the forked position. That overwrite is lawful — it is the
  // correction, not the damage — so it lands in `coverage`, not `defects`, and
  // the replica ends agreeing with the live history over its whole claimed
  // prefix.
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
      }))
      .then(spliceStreamAt(2))
      .then(receiveFrameAt(2))
      .then(applyFrameAt(2))
      .then(check(and {
        coverage.forkedTailReplaced,
        inv_splice_continuity,
        inv_no_acked_write_lost_across_fullsync,
        prefixAgrees(nodes.get(2).data, nodes.get(1).data, nodes.get(2).applied),
      }))
}
