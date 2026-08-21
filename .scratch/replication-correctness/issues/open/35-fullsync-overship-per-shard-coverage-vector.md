# 35: Full-sync overship — per-shard coverage vector, floors, and the window-grant refusal

Status: ready-for-agent

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

- [ ] D1 forcing test red-before/green-after: non-idempotent write pinned in the
      capture→cut window double-applies before the fix, applies exactly once after
- [ ] Vector in trailer on both payload paths; wire goldens updated
- [ ] Flush hold between drain-ack and cut on the checkpoint path, bound documented
- [ ] Per-shard floors extend `covers()`; counted skips; group-mend semantics specced
      and forced
- [ ] R14 refusal + R16 recovery rule forced by tests
- [ ] Vector persisted in staged metadata (R15) and recovered after a crash-in-window
- [ ] TR-034 / FM-004 / new FM row spec edits; `just lint-spec` green
- [ ] Model extended; battery rows CAUGHT; `just mutants-diff frogdb-replication`
      before push
