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
//   TR-REPLICATION-034  inv_no_forked_tail_above_the_claim
//   FM-REPLICATION-066  inv_coverage_brackets_the_payload,
//                       inv_overship_is_skipped_not_reapplied,
//                       inv_no_hole_below_the_claim
//   FM-REPLICATION-067  inv_mint_is_wire_order, inv_stamps_match_data,
//                       inv_cut_claims_stamps
//   FM-PERSISTENCE-060  inv_stamps_match_data (model shadow of the batch-atomic
//                       stamp; the crash/atomicity half is implementation-only)
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
// `inv_reapply_is_a_noop` makes the no-op falsifiable rather than incidental.
// Issue 35 narrows R4's licence further: the overlap is no longer *replayed* at
// all, it is skipped against the per-shard coverage vector, and
// `coverage.overshipSkipped` (`witnessOvershipSkipped`, pinned deterministically
// by `writeDuringTransferSurvivesTest`) witnesses that the range is exercised.
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
// no-op". That range is issue 35's subject, not R7's, and it is tracked under
// `inv_no_forked_tail_above_the_claim` (TR-REPLICATION-034).
//
// **Issue 35 (R12–R16, 2026-08-21) — the overship is described, not replayed.**
// A replica that installs a payload covering more than its granted offset holds
// writes its own offset does not claim, and until this ruling nothing told it
// where that residue ended. The replay resumed at the granted offset and
// re-executed every effect the payload already carried: harmless for the unit
// writes this model logs, corruption for a verbatim `INCR` or `LPUSH`. That is
// D1 (R12), and it is what FM-REPLICATION-066 forces.
//
// The fix is a **per-shard coverage vector** `Y_s`, captured at each shard's
// drain-ack with the flush engine held between drain-ack and cut, shipped in the
// full-sync metadata and installed as per-shard skip floors. The vector is
// *exact* in both directions — `inv_coverage_brackets_the_payload` states that
// equality — because either error is a defect: underclaiming re-executes the
// overship (D1 again), overclaiming skips a frame whose effect the replica does
// not hold and leaves a hole under the claimed prefix
// (`inv_no_hole_below_the_claim`). A frame is measured against *its own* shard's
// floor, so a torn checkpoint is mended frame by frame with mixed skip/apply
// inside one cross-shard group; that is the required behaviour, not a
// compromise, and `tornCheckpointIsMendedExactlyOnceTest` pins it.
//
// Shards are captured independently, so the vector is per-shard rather than
// scalar: a global watermark would have to pick the minimum (re-executing
// everything above it) or the maximum (dropping writes below it).
//
// The residue class the earlier ruling could not remove — a forked position the
// *new* history never reaches — is closed by **refusal** rather than
// truncation. A replica cannot undo a verbatim `INCR` it holds no pre-image of,
// so the accept does not truncate; instead a **window** grant (`+CONTINUE` under
// a different replication id) is refused while `applied < max(Y_s)`, degrading
// to a full resync that replaces the dataset wholesale (R14,
// `windowGrantRefused`). Same-history grants are never refused: the overship is
// a prefix of what that primary will replay and the floors dedup it exactly.
// R16 adds the blind case — a stint whose offsets came from crash recovery has
// residue and no vector describing it, so it refuses every window grant until
// issue 36 makes the artefact self-describing.
//
// Prevention plus refusal is now in the implementation
// (TR-REPLICATION-034, FM-REPLICATION-066), so
// `inv_no_forked_tail_above_the_claim` has **graduated from model-only**.
// `overshippedTailMeetsFailoverTest` pins the refusal,
// `sameHistoryGrantIsNeverRefusedTest` and `theRefusalEndsAtTheCeilingTest` pin
// its two boundaries.
//
// The rule governs the partial-resync **accept** and nothing else.
// `applyPromote` does not truncate, so a node promoted while holding an
// overshipped tail still carries positions above its own `recv`; that path is
// `promotedOvershipDoesNotServeForkedTailTest`, and it stays governed by the
// serve-side properties (a promoted node does not serve above its head).
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

  // No frame ever lands on a divergent position *above* the offset its grant
  // claims. R5 stated this over an in-place truncation at the accept and it was
  // model-only; issue 35 implements it as prevent-plus-refuse — the per-shard
  // floors stop the tail being re-executed, and `windowGrantRefused` refuses the
  // one grant under which the residue could be left forked (a history that
  // replaced the one the tail came from and may never reach those positions).
  // Both halves are now in the implementation (TR-REPLICATION-034,
  // FM-REPLICATION-066), so this invariant has graduated from model-only.
  val inv_no_forked_tail_above_the_claim: bool = not(defects.forkedTailApplied)

  // --- Per-shard coverage (issue 35, FM-REPLICATION-066) ---

  // The trailer's vector describes the artefact it rides on, exactly. Both
  // directions are failures with names: a watermark *above* what the payload
  // holds makes the replica skip a frame whose effect it does not have (silent
  // loss), and one *below* it makes the replica execute a frame whose effect it
  // already has (the D1 double-apply, corruption under a verbatim `INCR`).
  //
  // Stated as an equality against the payload's own content rather than as two
  // inequalities, because that is the exactness ruling R13 buys with the flush
  // hold: shards are single-threaded and `ReplicationBroadcast` is the terminal
  // write effect, so at the drain-ack every executed write has been broadcast
  // and the watermark is neither behind nor ahead.
  val inv_coverage_brackets_the_payload: bool =
    SIDS.forall(k => {
      val s = sessions.get(k)
      s.payload_cut implies (s.coverage == coverageOfPayload(s.payload))
    })

  // The consequence on the receiver, and the one an operator would see: no node
  // has a gap below the offset it claims. A floor that overclaims turns a frame
  // the replica needed into a skip, and the hole it leaves sits under the
  // claimed prefix — the range every downstream reader, and every later grant,
  // treats as agreed history.
  val inv_no_hole_below_the_claim: bool =
    NODES.forall(n => {
      val d = nodes.get(n).data
      val claim = nodes.get(n).applied
      d.indices().forall(i => (i + 1 <= claim) implies (d.nth(i) != HOLE))
    })

  // D1: a frame whose effect the installed payload already carried was executed
  // again, and the ruled disposition of a floor-covered frame (skip, advance the
  // head over it, count it) held on every skip.
  val inv_overship_is_skipped_not_reapplied: bool =
    not(defects.overshipDoubleApplied) and not(defects.floorSkipMisdisposed)

  // --- Per-shard stamps (issue 37, FM-REPLICATION-067 / FM-PERSISTENCE-060) ---

  // The claim a node's stores carry equals the coverage of the data they hold —
  // continuously, on every node, in every reachable state. This is the model
  // shadow of FM-PERSISTENCE-060's batch atomicity: a stamp that could commit
  // without its data (claim ahead — recovery skips frames whose effects it does
  // not hold) or data without its stamp (claim behind — recovery re-executes
  // effects it already holds, the D1 shape) would each break one direction of
  // this equality. The model has no torn intermediate state to express, which
  // is the point: minting at the persist point makes the fused assignment the
  // only assignment.
  //
  // Guarded on `promotedWithUnappliedTail` (issue 40): once a replica promotes
  // over an installed-but-unapplied payload, the model's offset↔index identity
  // is broken at HEAD — the stamp invariants scope themselves out of that
  // region until issue 40 rules the intended semantics.
  val inv_stamps_match_data: bool =
    not(coverage.promotedWithUnappliedTail) implies
    NODES.forall(n =>
      nodes.get(n).stamps == coverageOfPayload(nodes.get(n).data))

  // FM-REPLICATION-067's fusion, stated over the wire itself: the backlog (a
  // primary's `data` is also the stream content it serves) and the stamps
  // advance in the same order. Every position on the wire is covered by its own
  // shard's stamp — the stamp was minted in the same critical section that
  // enqueued the frame, so it can never lag — and no shard's stamp reaches past
  // the wire head — it can never lead. Weaker than `inv_stamps_match_data` by
  // design: this is the ordering claim alone, so a mutation that reorders the
  // mint against the enqueue trips it without needing the full equality.
  // Guarded on `promotedWithUnappliedTail` like `inv_stamps_match_data` —
  // see issue 40.
  val inv_mint_is_wire_order: bool =
    not(coverage.promotedWithUnappliedTail) implies
    NODES.forall(n => {
      val d = nodes.get(n).data
      val st = nodes.get(n).stamps
      and {
        d.indices().forall(i =>
          d.nth(i) != HOLE implies (i + 1) <= st.get(shardOf(d.nth(i)))),
        SHARDS.forall(sh => st.get(sh) <= d.length()),
      }
    })

  // At the cut, the artefact's claim reads off the sender's stamps: each
  // shard's bound in the trailer vector equals the stamp a store recovering the
  // payload would surface (issue 37's recovery contract — recovery reads the
  // same keys the cut wrote), and no bound exceeds what the primary has stamped
  // (a claim above the persisted coverage would promise history no store
  // holds). The second half is guarded on a live session: a disconnected
  // session keeps its cut artefact, but its primary may since have restarted
  // and truncated — the stale artefact makes no claim about the new stint's
  // stamps. Overlaps `inv_coverage_brackets_the_payload` on the equality half
  // deliberately: that row states the artefact's internal exactness, this one
  // pins the artefact to the *stamps* as its source.
  // Guarded on `promotedWithUnappliedTail` like `inv_stamps_match_data` —
  // see issue 40.
  val inv_cut_claims_stamps: bool =
    not(coverage.promotedWithUnappliedTail) implies
    SIDS.forall(k => {
      val s = sessions.get(k)
      val p = nodes.get(s.primary)
      and {
        s.payload_cut implies SHARDS.forall(sh =>
          s.coverage.get(sh) == coverageOfPayload(s.payload).get(sh)),
        (s.payload_cut and s.phase != Disconnected) implies SHARDS.forall(sh =>
          s.coverage.get(sh) <= p.stamps.get(sh)),
      }
    })

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
  val witnessOvershipSkipped: bool = coverage.overshipSkipped
  val witnessTornPayloadCut: bool = coverage.tornPayloadCut
  val witnessTornGroupMended: bool = coverage.tornGroupMended
  val witnessWindowGrantRefused: bool = coverage.windowGrantRefused
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
  //
  //    This is D1's exact shape (issue 35). Before the coverage vector the
  //    re-shipped frame was *executed* against a keyspace that already held its
  //    effect — invisible in this model's write-log abstraction, corruption on a
  //    verbatim `INCR`. Now its shard's floor covers it: the frame is skipped,
  //    the head advances over it, and the skip is counted.
  run writeDuringTransferSurvivesTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).grant_offset == 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(check(coverage.overshipped and not(defects.cutBelowGrant)))
      .then(deliverPayloadAt(1))
      .then(check(and {
        nodes.get(2).applied == 1,
        nodes.get(2).data.length() == 2,
        // The trailer's vector: shard 1 contributed offset 1, shard 2 offset 2.
        nodes.get(2).floors == Map(1 -> 1, 2 -> 2),
        // Still in force — the payload reaches past what the replica claims.
        nodes.get(2).applied < maxFloor(nodes.get(2).floors),
        inv_coverage_brackets_the_payload,
      }))
      .then(spliceStreamAt(1))
      .then(receiveFrameAt(1))
      .then(applyFrameAt(1))
      .then(check(and {
        nodes.get(2).applied == 2,
        nodes.get(2).data == nodes.get(1).data,
        // Skipped by shard 2's floor, not executed: the frame claimed its bytes
        // (the head advanced over it) and the skip was counted.
        coverage.overshipSkipped,
        nodes.get(2).floor_skipped == 1,
        not(coverage.tailApplied),
        inv_overship_is_skipped_not_reapplied,
        inv_no_acked_write_lost_across_fullsync,
        inv_no_hole_below_the_claim,
        inv_splice_continuity,
        inv_applied_covered_by_data,
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

  // The FM-REPLICATION-004 overshipped tail meeting a failover, and ruling R14's
  // refusal removing it. Pinned deterministically because the random walk
  // reaches the shape in roughly 0.2% of traces at 4000x40 and not at all at the
  // gate's depth — it needs an overship, a lossy restart, and a re-grant of the
  // *same* replica, in that order, before the replay has reconciled the tail.
  //
  // The shape: replica 2 installs a payload covering offset 2 while its granted
  // offset is 1, so it holds a write its own offset does not claim. Primary 1
  // then restarts having recovered only offset 1, mints a new history, and
  // writes a *different* write at offset 2. The replica is inside the failover
  // window at offset 1, so the primary *would* serve it a partial resync.
  //
  // Issue 35 (2026-08-21) rules what happens next, superseding R5's in-place
  // truncation — a replica cannot invert a landed effect, so "discard above the
  // claim" is not implementable at the accept. The replica refuses the grant
  // instead: the id it is being handed is not the id it presented (a window
  // grant), and its floors are still in force (`applied < max(Y_s)`), so the
  // effects above its claim came from a history this primary may never
  // reproduce. It degrades to a full resync, which replaces the dataset
  // wholesale and takes the residue with it.
  //
  // Note what is *not* refused: the same-history reconnect. There the
  // overshipped effects are a prefix of exactly what will be replayed and the
  // floors dedup them — `writeDuringTransferSurvivesTest` is that case.
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
        // …and the vector says so, which is what makes the refusal possible.
        nodes.get(2).floors == Map(1 -> 1, 2 -> 2),
      }))
      .then(restartNodeAs(1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(and {
        // Refused: a window grant while the floors still reach past the claim.
        sessions.get(2).arm == FullSnapshot,
        coverage.windowGrantRefused,
        // The primary would have served it — the request is inside the frozen
        // window and above the armed floor. Nothing on the sender side changed.
        windowContains(nodes.get(1), FIRST_GEN, 1),
        floorAdmits(nodes.get(1), 1),
        // The tail is still there; the accept discarded nothing, because a
        // verbatim stream leaves nothing to discard it with.
        nodes.get(2).data.length() == 2,
      }))
      .then(cutCheckpointAt(2))
      .then(deliverPayloadAt(2))
      .then(check(and {
        // The full resync replaced the dataset, residue and all.
        nodes.get(2).data == nodes.get(1).data,
        nodes.get(2).applied == 2,
        inv_no_forked_tail_above_the_claim,
        inv_no_hole_below_the_claim,
        inv_coverage_brackets_the_payload,
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

  // Row M61: the full-sync streaming ghost is latched by the *splice*, not by
  // the arm the grant carried, so a replica that acquires its history and then
  // loses the session before splicing takes its first splice on the partial arm
  // with the ghost still dark. `deliverPayloadAt` is what writes `replid`;
  // `endSessionAt`'s only guard is `phase != Disconnected`, so it fires at
  // `PayloadInstalled`. This is the negative assertion the T10b battery's row
  // M61 lacked: without it, latching the ghost unconditionally is undetectable.
  run partialSpliceLeavesFullSyncGhostDarkTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).arm == FullSnapshot and sessions.get(1).grant_offset == 2))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(check(and {
        // The history is the replica's already, one transition before any splice.
        nodes.get(2).replid == FIRST_GEN,
        nodes.get(2).applied == 2,
        not(coverage.fullSyncStreaming),
      }))
      .then(endSessionAt(1))
      .then(check(nodes.get(2).link == None and sessions.get(1).phase == Disconnected))
      .then(psyncRequestAs(2, 1, true, 2))
      // …so `decideArm` now reads a real `replid`, and both the window and the
      // floor admit it.
      .then(check(sessions.get(2).arm == PartialGrant and sessions.get(2).grant_offset == 2))
      .then(spliceStreamAt(2))
      .then(check(and {
        sessions.get(2).phase == Streaming,
        // The row's assertion: a splice happened and the ghost is still dark.
        not(coverage.fullSyncStreaming),
        // …on a lawful path, not a defect one.
        not(defects.spliceGap),
        not(defects.spliceDiverged),
        not(defects.grantBelowFloor),
        not(defects.grantForeignHistory),
        not(defects.tornIdentity),
        not(defects.forkedTailApplied),
        inv_splice_continuity,
        inv_applied_covered_by_data,
        inv_no_acked_write_lost_across_fullsync,
      }))

  // --- Scenarios for the per-shard coverage vector (issue 35) ---

  // The torn checkpoint, mended exactly once. Shard 1 was captured at offset 1
  // and shard 2 at offset 4, so the payload holds offsets 1, 2 and 4 and has a
  // *hole* at 3 — a position inside its own span that the artefact does not
  // carry. The replay covers 3 and 4 in one contiguous range and the replica
  // treats them differently: 3 lands (shard 1's floor is below it), 4 is skipped
  // (shard 2's floor covers it).
  //
  // Mixed skip/apply inside one cross-shard group is not a compromise here, it
  // is the requirement. Frames are shard-local, so a frame never straddles a
  // watermark; measuring each frame against *its own* shard's floor is what
  // makes the mend exact, and measuring them all against one global watermark
  // would either re-execute offset 4 or lose offset 3.
  run tornCheckpointIsMendedExactlyOnceTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).grant_offset == 2))
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      // Shard 1 drained at offset 2, shard 2 at offset 4.
      .then(cutCheckpointTornAt(1, 2, 4))
      .then(check(and {
        sessions.get(1).coverage == Map(1 -> 1, 2 -> 4),
        sessions.get(1).payload == List(1, 2, HOLE, 4),
        coverage.tornPayloadCut,
        inv_coverage_brackets_the_payload,
      }))
      .then(deliverPayloadAt(1))
      .then(check(and {
        nodes.get(2).applied == 2,
        nodes.get(2).floors == Map(1 -> 1, 2 -> 4),
        // The hole is above the claim, where a hole is lawful.
        inv_no_hole_below_the_claim,
      }))
      .then(spliceStreamAt(1))
      .then(receiveFrameAt(1))
      .then(applyFrameAt(1))
      .then(check(and {
        // Offset 3 landed: shard 1's watermark is below it, so the payload did
        // not carry its effect.
        nodes.get(2).data.nth(2) == 3,
        nodes.get(2).applied == 3,
        nodes.get(2).floor_skipped == 0,
        coverage.tailApplied,
      }))
      .then(receiveFrameAt(1))
      .then(applyFrameAt(1))
      .then(check(and {
        // Offset 4 was skipped: shard 2's watermark covers it. It still claimed
        // its bytes — the head advanced over it — and the skip was counted.
        nodes.get(2).applied == 4,
        nodes.get(2).floor_skipped == 1,
        coverage.overshipSkipped,
        coverage.tornGroupMended,
        // Whole, with no position executed twice and none missing.
        nodes.get(2).data == nodes.get(1).data,
        inv_overship_is_skipped_not_reapplied,
        inv_no_hole_below_the_claim,
        inv_no_acked_write_lost_across_fullsync,
        inv_splice_continuity,
      }))

  // Ruling R14's safe side, and the reason the refusal is scoped to window
  // grants: a reconnect to the *same* history is granted a partial resync while
  // the floors are still in force, and that is correct. The overshipped effects
  // are a prefix of exactly what this primary will replay, and the floors dedup
  // them exactly. Refusing here would cost a full resync on every reconnect
  // that interrupts a replay.
  run sameHistoryGrantIsNeverRefusedTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      // The link dies before the replay reconciles the overship.
      .then(endSessionAt(1))
      .then(check(and {
        nodes.get(2).applied == 1,
        nodes.get(2).applied < maxFloor(nodes.get(2).floors),
      }))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(and {
        // Same history: granted, floors in force or not.
        sessions.get(2).arm == PartialGrant,
        sessions.get(2).grant_offset == 1,
        not(coverage.windowGrantRefused),
        // The floors survived the reconnect — they describe the keyspace, which
        // the reconnect did not touch.
        nodes.get(2).floors == Map(1 -> 1, 2 -> 2),
      }))
      .then(spliceStreamAt(2))
      .then(receiveFrameAt(2))
      .then(applyFrameAt(2))
      .then(check(and {
        // …and they did their job on the replayed prefix.
        nodes.get(2).floor_skipped == 1,
        nodes.get(2).data == nodes.get(1).data,
        inv_overship_is_skipped_not_reapplied,
        inv_no_acked_write_lost_across_fullsync,
      }))

  // The refusal ends exactly at the ceiling. Once the replica's head reaches
  // `max(Y_s)` nothing above its claim is unaccounted for any more, so a window
  // grant at that offset is a clean shared-prefix state and stays partial. This
  // is the boundary a `<=` in place of `<` would move.
  run theRefusalEndsAtTheCeilingTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(writeOnPrimaryAs(1))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(spliceStreamAt(1))
      .then(receiveFrameAt(1))
      // The floor skip carries the head up to the ceiling.
      .then(applyFrameAt(1))
      .then(check(nodes.get(2).applied == maxFloor(nodes.get(2).floors)))
      // A lossy restart replaces the history and freezes the old one at 2.
      .then(restartNodeAs(1, true, 2))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(and {
        // A window grant — and it is *not* refused.
        coverage.partialViaSecondary,
        sessions.get(2).arm == PartialGrant,
        not(coverage.windowGrantRefused),
        nodes.get(2).applied == 2,
      }))

  // Ruling R16, over the decision itself. The machine cannot reach the state —
  // `restartNodeAs` comes back as a Primary and `inv_primary_role_is_terminal`
  // keeps it there, so no *recovered replica* ever presents a PSYNC here — so
  // the rule is checked where it is decided. The reachable half is forced in the
  // implementation (`a_recovered_stint_refuses_every_window_grant`).
  //
  // A crash-recovered stint has a keyspace possibly ahead of its claim and no
  // vector describing by how much, so the floor guard is blind: it refuses every
  // window grant until issue 36 makes the artefact self-describing. What it must
  // *not* do is refuse a same-history grant — that would be a full resync on
  // every restart of the primary a healthy replica is following.
  run aRecoveredStintRefusesTheWindowGrantTest =
    init
      .then(check(
        val p = { ...nodes.get(1), replid: 7 }
        val clean = { ...nodes.get(2), replid: 3, applied: 5,
                      floors: noFloors, recovered: false }
        val crashed = { ...clean, recovered: true }
        and {
          // Clean stint, nothing above the claim: the window grant stands.
          not(windowGrantRefused(p, clean)),
          // Same stint after a crash recovery: refused, with no vector to
          // consult.
          windowGrantRefused(p, crashed),
          // Same history is never refused, recovered or not — R14's scope.
          not(windowGrantRefused({ ...p, replid: 3 }, crashed)),
          // And the floor guard is the other, independent trigger.
          windowGrantRefused(p, { ...clean, floors: Map(1 -> 0, 2 -> 6) }),
        }))

  // Why the vector has to be *exact*, stated as the two ways it can be wrong.
  // `inv_coverage_brackets_the_payload` is the invariant that separates the
  // exact vector from both, and this is what each error does to a frame.
  run anInexactVectorLosesOrDoubleAppliesTest =
    init
      .then(check(
        // Offsets 1 (shard 1) and 2 (shard 2), both in the payload.
        val payload = List(1, 2)
        val exact = coverageOfPayload(payload)
        val r = { ...nodes.get(2), data: payload, applied: 1, floors: exact }
        and {
          exact == Map(1 -> 1, 2 -> 2),
          // The head outranks the floors: a frame at or below the claim is the
          // FM-REPLICATION-065 skip, whatever the vector says.
          frameDisposition(r, 1, 1) == CoveredByHead,
          // Exact: the overshipped frame is skipped, not executed again.
          frameDisposition(r, 2, 2) == CoveredByFloor,
          // Underclaimed by one: the same frame is executed against a keyspace
          // that already holds its effect — D1, corruption on a verbatim `INCR`.
          frameDisposition({ ...r, floors: Map(1 -> 1, 2 -> 1) }, 2, 2) == Land,
          // Overclaimed: a frame whose effect the replica does *not* hold is
          // skipped, leaving a hole under the claimed prefix.
          frameDisposition({ ...r, floors: Map(1 -> 1, 2 -> 4) }, 4, 4) == CoveredByFloor,
          // Only the exact vector brackets the payload.
          exact == coverageOfPayload(payload),
          Map(1 -> 1, 2 -> 1) != coverageOfPayload(payload),
          Map(1 -> 1, 2 -> 4) != coverageOfPayload(payload),
        }))

  // A shard's capture point never precedes the granted offset: the offset is
  // taken first and the drain-ack follows, so every watermark is at or above it.
  // The action is disabled below that bound, which is what keeps
  // `defects.cutBelowGrant` about a missing drain rather than about a legal cut.
  run captureNeverPrecedesTheGrantTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).grant_offset == 2))
      .then(cutCheckpointTornAt(1, 1, 2))
      .fail()
}
