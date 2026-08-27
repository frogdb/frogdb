# 35: Full-sync overship — per-shard coverage vector, floors, and the window-grant refusal

Status: done

Closed on user sign-off 2026-08-22: the breach-abort amendment and the
`full_sync_hold_breaches` counter both landed — the counter-first condition.

Ruled 2026-08-21, campaign ledger R12–R16
(`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`). This issue owns two
defects and folds TR-REPLICATION-034 into its fix:

- **D1 (new, worse than the row): the healthy full-sync handoff double-applies.**
  `snapshot_offset` is captured before the drain and the cut
  (`replica_session.rs:1199`; machine order `session_machine.rs:512-563`). Writes
  landing during the drain enter the checkpoint above it (`checkpoint_quiesce.rs:34-35`
  says so verbatim), and the offset `fetch_add` lives in `ReplicationBroadcast` — the
  *last* write effect (`post_execution.rs:282-292`, `primary/mod.rs:857-874`) — so a
  write is drainable before it is even counted. The handoff then replays
  `(snapshot_offset, current]`, re-executing the overshipped writes the installed
  keyspace already holds: FM-REPLICATION-065's `covers()` skips at-or-below the head
  only (`apply.rs:438`, `replica/offset.rs:502-504`), these sit above. The stream is
  verbatim and non-idempotent (`INCR`/`LPUSH`/`APPEND`) — silent divergence with both
  nodes reporting the same offset, in the plain full-sync path, no link break needed.
  The live-dataset path has the same shape (sequential per-shard export, no drain, no
  pause — `export.rs:29-56`). **No test pins it**: the FM-004 forcing tests prove
  no-loss only, and `test_writes_during_full_sync_are_not_lost` uses idempotent `SET`s
  exclusively.
- **D2 (TR-REPLICATION-034): replaced-history residue above the claim.** Link breaks
  before the handoff reconciles the tail; a `+CONTINUE` granted through the failover
  window resumes over old-history effects the granting history may never rewrite.

## The fix (V — ruled R13)

One mechanism, both payload paths, receiver-authoritative like issue 34:

1. **Per-shard coverage watermarks in the trailer.** Each shard reports `Y_s` — its
   last-broadcast offset at its capture point — exactly, because shards are
   single-threaded and both capture points are messages the shard task processes:
   - checkpoint path: the shard's drain-ack (`FlushWal` processing) — and the flush
     engine is **held between a shard's drain-ack and the cut** so nothing above `Y_s`
     slips into RocksDB via background/inline flush (the cut is hard-link-fast; under
     Sync durability the hold briefly holds acks too — document the bound);
   - live-dataset path: the shard's export message (`export.rs`) — no hold needed, the
     export *is* the capture.
   `FullSyncMetadata` grows the vector alongside the existing single offset
   (`fullsync.rs:47-68`); wire goldens updated. The single trailer offset stays the
   prefix bound (today's capture is `≤ min(Y_s)` and remains sound; tightening it to
   `min(Y_s)` is optional).
2. **Per-shard skip floors on the replica.** Install adopts the vector; the handoff
   frame check extends FM-REPLICATION-065's `covers()`: a shard-`s` frame ending
   `≤ Y_s` is skipped (counted, not silent — same discipline as issue 34). A shard-`s`
   frame never straddles `Y_s` (it is that shard's own frame boundary). Mixed
   skip/apply inside a cross-shard group is **correct and required**: a torn checkpoint
   (shard A drained before the txn, shard B after) contains exactly the per-shard
   halves the floors describe, and the mixed replay mends the tear exactly once. Spec
   the interaction with the FM-065 group rules explicitly (head still moves at `EXEC`
   by the whole group's byte total).
3. **Window-grant refusal (ruled R14 — scope: window grants only).** The replica keeps
   the vector until `applied ≥ max(Y_s)`; a `+CONTINUE` carrying `granted_id` (history
   replaced) while `applied < max(Y_s)` is refused — degrade to full resync, which
   wipes the residue. Same-history grants are never refused: the floors dedup them
   exactly (safety argument in ledger R14). A window grant at `applied ≥ max(Y_s)` is a
   clean shared-prefix state and stays partial.
4. **Floor persistence (ruled R15).** The vector rides in the FM-PERSISTENCE-039
   staged `replication_metadata.json` (`fullsync/stager.rs:145-160`), so a crash
   between install and reconcile recovers the floors. Floors reset at each install.
5. **Interim recovery rule (ruled R16).** A stint whose offsets came from
   crash-recovery (not a completed sync or clean shutdown) has no vector and a keyspace
   possibly ahead of its claim: it refuses window grants unconditionally until issue 36
   (S) lands. The same-history restart bias stays a documented gap owned by issue 36.

## Spec work

- New FM row for D1 (sender vector + receiver floors + counters); forcing tests in
  `frogdb-replication` per push discipline, plus an integration test that **pins a
  non-idempotent write inside the capture→cut window** (the missing test class).
- TR-REPLICATION-034: rewrite the Pending row to this ruling — the postcondition's
  "discard above the claim" is implemented as prevent-plus-refuse (floors + R14), not
  in-place truncation; Bug refs → this issue; drop the "choosing between them is a
  follow-up" note.
- FM-REPLICATION-004 gains the vector as the exact-pairing companion to its
  `offset ≤ data` direction.
- Model: `replication_fullsync.qnt` grows per-shard coverage (2 shards suffice) —
  floors, the refusal guard, and `inv_no_forked_tail_above_the_claim` graduates from
  model-only; battery rows for the new guards; witness for the torn-group mend.

## Acceptance

- [x] D1 forcing test red-before/green-after: non-idempotent write pinned in the
      capture→cut window double-applies before the fix, applies exactly once after
      — `a_write_pinned_in_the_capture_to_cut_window_lands_exactly_once`
      (`replica_session.rs`, →c5dee351). End to end over the wire: the pre-checkpoint
      hook broadcasts an `INCR` and flushes its effect, so the payload holds the effect
      and the backlog holds the frame; the primary's own session driver cuts, writes the
      trailer and replays the backlog onto the socket, and those frames come back into
      the replica's consume loop. **Red before**: with `floors_in_force` forced to
      `false` (the pre-row behaviour) the assertion fails `left: 2, right: 1` — the
      counter reaches 2 where the primary has 1, both nodes reporting the same offset.
      **Green after**: 1, `floor_skipped == 1`, and the head still claims the frame's
      bytes.
- [x] Vector in trailer on both payload paths; wire goldens updated — →84fd1154
      (checkpoint drain + live export), `ShardCoverage` as the trailer's fifth field;
      a four-field trailer is refused at the parse rather than read as "no coverage"
      (`a_trailer_without_the_coverage_field_is_refused`).
- [x] Flush hold between drain-ack and cut on the checkpoint path, bound documented —
      →6e26ca0b (`FlushHold`, `frogdb-persistence`) and →84fd1154 (`CaptureHold` on the
      session). `FULL_SYNC_HOLD = 10s`. **Amended 2026-08-21: breach aborts the sync
      (user ruling).** A shard whose hold lapses before the cut fails the whole full
      sync — `release_hold` names the breached shards and downgrades nothing, the driver
      turns that into `SessionEvent::CoverageBreached` →
      `SyncFailure::CoverageHoldBreached`, so the payload and its trailer are never
      written, the link drops and the replica retries from scratch (the staged directory
      is owned first, so the abort cleans it). The landed behaviour — report `Y_s = 0`
      for that shard and ship anyway — was wrong: `0` is not a weaker floor but *no*
      floor, so that shard's overshipped range re-executes and D1's silent divergence
      returns in the slow-cut shape, invisible to both nodes. `0` stays representable on
      the wire for `serde(default)` reads; no sender ever produces one this way.
      (`a_breached_hold_names_its_shard_and_downgrades_no_claim`,
      `a_breached_coverage_hold_abandons_the_sync_and_owns_the_directory`,
      `a_breached_hold_aborts_the_sync`.)
- [x] Per-shard floors extend `covers()`; counted skips; group-mend semantics specced
      and forced — →e0be6a90. `frame_disposition` is one pure total predicate ordering
      head-then-floor; `ConsumeStats.floor_skipped` per stint and
      `AppliedOffset::floor_skipped()` node-wide, one `DEBUG` per frame. The torn
      cross-shard group is mended by mixed skip/apply
      (`a_torn_cross_shard_transaction_is_mended_exactly_once`), which
      FM-REPLICATION-066's "NOT observable" cell states as required rather than
      tolerated.
- [x] R14 refusal + R16 recovery rule forced by tests — →3f0ffdc9.
      `window_grant_verdict` is a second pure seam over four inputs; same-history
      grants are never refused (`a_same_history_grant_is_never_refused`), the refusal
      ends exactly at the ceiling (`the_refusal_ends_exactly_at_the_ceiling`), and a
      crash-recovered stint refuses unconditionally
      (`a_recovered_stint_refuses_every_window_grant`).
- [x] Vector persisted in staged metadata (R15) and recovered after a crash-in-window —
      →a318b9de. `StagedReplicationMetadata.coverage` and
      `ReplicationState.coverage_at_save`, both `#[serde(default)]` so older metadata
      reads back as *no* floors. Floors already passed by the head are not persisted:
      a node in steady state saves an empty vector
      (`a_save_point_persists_the_floors_in_force`,
      `a_recovered_identity_comes_back_with_its_floors`).
- [x] TR-034 / FM-004 / new FM row spec edits; `just lint-spec` green — →5d628c28,
      →c5dee351. TR-034's postcondition is restated as prevent-plus-refuse (in-place
      truncation is deliberately *not* implemented — a replica cannot invert a verbatim
      `INCR`), FM-004 gains the vector as its exact-pairing companion, and
      FM-REPLICATION-066 is added with 43 forcing tests. `just lint-spec`: OK, 308
      failure modes, 1671 test references / 1671 tags.
- [x] Model extended; battery rows CAUGHT; `just mutants-diff frogdb-replication`
      before push — →7a4aabd4. `replication_fullsync{,_types,_logic,_machine}.qnt` grow
      two shards, a `HOLE` sentinel for the torn cut, per-shard floors and the refusal
      guard; `truncateAboveClaim` is dropped and
      `inv_no_forked_tail_above_the_claim` **graduates from model-only**. Three new
      invariants (`inv_coverage_brackets_the_payload`,
      `inv_no_hole_below_the_claim`, `inv_overship_is_skipped_not_reapplied`), four new
      witnesses, six new battery scenarios. `just quint-run` green (46 tests, sampled
      walk clean, all 20 witnesses reached — `witnessOvershipSkipped` 38,
      `witnessTornPayloadCut` 201, `witnessTornGroupMended` 38,
      `witnessWindowGrantRefused` 18, so no exemption entries needed).
      `just mutants-diff frogdb-replication`: **84 mutants, 68 caught, 16 unviable,
      0 missed** (`target/mutants/frogdb-replication-diff`). The first run surfaced
      four survivors, all on `ShardCoverage`'s accessors, closed in →a31546b8:
      `len()` had no caller anywhere in the workspace and was deleted (two mutants);
      `is_empty() -> true` survived because every assertion expected `true`, so
      `an_empty_coverage_field_parses_as_no_watermarks` now also pins the
      all-zero vector as *non*-empty (a claim of two floors, not the absence of a
      claim); `none() -> Default::default()` is genuinely equivalent — the derived
      `Default` is the empty vector — and is documented at the constructor and
      excluded by exact name in `.cargo/mutants.toml`. Suite after the fix:
      611/611 passed, 6 skipped; `just lint-spec` OK.

## Residue

Issue 36 was ruled 2026-08-22 (PRD, ledger R17–R24) and settles this issue's interim
machinery's retirement schedule:

- **R15 plumbing retires via [issue 38](../open/38-replica-stamps-floors-unification.md).**
  The staged `replication_metadata.json` coverage vector and
  `ReplicationState.coverage_at_save` delete once floors live in the per-shard stamp
  keys — an installed checkpoint *contains* the primary's stamps, so install adopts the
  floors atomically with the data.
- **R16's unconditional refusal retires via issue 38.** A crash-recovered replica stint
  reconstructs exact floors and head from its stamps; `OffsetProvenance::Recovered`
  stops being a refusal class. The `applyRestart` model gap noted below opens up then
  (R24: real restart transitions).
- **The flush hold + breach-abort + breach counter delete via
  [issue 39](../open/39-flush-hold-deletion-sender-reads-artifact.md).** The sender reads
  `Y_s` from the cut artifact's own stamps, so the artifact cannot disagree with its
  claim by construction. The drain stays. The trailer `ShardCoverage` **stays** as the
  wire form (live path has no artifact).
- `WRITE_EFFECT_ORDER` restructuring (mint at persist) is
  [issue 37](../open/37-mint-at-persist-and-primary-stamps.md)'s subject, as anticipated.
- **Model gap (documented at `applyRestart`)**: `restartNodeAs` returns a Primary and
  `inv_primary_role_is_terminal` keeps it there, so no reachable state presents a
  *recovered replica* at a PSYNC. R16 is therefore checked over the pure decision
  (`aRecoveredStintRefusesTheWindowGrantTest`) plus the Rust forcing tests — acceptable
  precisely because the rule is interim; 38/24 model restart properly (R24).
