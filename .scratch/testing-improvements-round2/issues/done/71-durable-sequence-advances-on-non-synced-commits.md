# `FlushOutcomes::durable_sequence` advances on `sync = false` commits — "durable" means "handed to RocksDB"

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/13 F3 · MASTER.md §3 (durability)
Score: severity 5 · likelihood 4 · effort 2 · priority 21
Area: frogdb-persistence / WAL flush engine

## Context

The WAL flush engine advances its durable sequence whether or not the commit was fsynced. Every
consumer built on that number — `WAIT`, `LASTSAVE`, replication acks — therefore reports data as
durable when it is only in the RocksDB memtable, and a power loss loses acknowledged writes.
This is a durability violation, not a metrics bug. It is invisible in `Sync` mode, where the two
notions coincide; `Periodic` and `Async` are the non-default-but-common modes where they diverge.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`persistence/src/wal/flush.rs` — `RocksSink::commit` calls `rocks.record_wal_watermark()` only
when `result.is_ok() && sync`, but `FlushOutcomes` advances `durable_seq` regardless of `sync`.
Round 1 issue 12 (fsync boundary) named this as an unfiled follow-up and it was never filed.
`flush()` also early-returns `Ok(())` when `staged_len() == 0` **without** advancing
`durable_seq`, so the two notions disagree in both directions.

Why nothing catches it: in `Sync` mode the two coincide, which is why no test notices — the
suite never drives the flush engine in `Periodic`/`Async` and asserts on the reported sequence.

## What to fix

1. Advance `durable_seq` only for commits that were actually fsynced; track a separate
   "staged/handed off" sequence if consumers need one.
2. Make the empty-stage early return report the correct `durable_seq` rather than a stale one.
3. Audit the consumers (`WAIT`, `LASTSAVE`, replication acks) so each reads the sequence whose
   meaning it actually needs.

## Acceptance criteria

- [x] A crate-level test drives the `FlushEngine` in `Periodic{interval_ms: 60_000}` with a sink
      that records `sync`, commits N ops, and asserts `durable_sequence()` does **not** advance
      past the last fsync'd sequence. Fails today.
- [x] Forcing a periodic sync then advances it to exactly N.
- [x] A no-op `flush()` on an empty stage reports the correct `durable_seq`, not a stale one.
- [x] The same assertions run for `Async` mode, and `Sync` mode is asserted unchanged.

## Test boundary

Level 2 (crate API of the WAL flush engine) — the two notions of "durable" are only
distinguishable at the sink seam. Not level 3 or 4: a server test could not distinguish them
without a power-loss primitive, which is exactly the confusion that hid the defect.

## Depends on

nothing to write the level-2 test. A full end-to-end proof (acknowledged write lost after power
loss) would additionally need issue 02 (I2 — subprocess-SIGKILL crash primitive),
`.scratch/testing-improvements-round2/issues/`, which the proposal did not itself request.

## Resolution

Confirmed and fixed. The premise about the *defect* was right; the premise about the *consumers*
was wrong — see "Correction" below.

### Root cause

`FlushOutcomes` carried a single watermark, `durable_seq`, and both commit paths advanced it
unconditionally:

- `FlushEngine::flush`, `frogdb-server/crates/persistence/src/wal/flush.rs:638`
- `FlushEngine::apply_clear`, `frogdb-server/crates/persistence/src/wal/flush.rs:582`

Both call `sink.commit(self.is_sync)`, where `is_sync` is true only in `DurabilityMode::Sync`.
So in `Periodic` and `Async` the commit does not fsync, yet the watermark advanced anyway — the
number named "durable" only ever meant "handed to RocksDB".

`test_fsync_seam_sync_flag_matches_mode` already pinned that in `Periodic`/`Async` *every*
commit has `sync == false`, including an explicit `WalCommand::Flush` from
`flush_through`/`flush_async`. That rules out "make the explicit flush sync" as the fix and
makes the two-watermark split the correct one. It also showed FM-PERSISTENCE-003/004's prose was
wrong: both rows claimed a `WalCommand::Flush` passed `sync = true`. Both were rewritten.

### Fix

Split the one watermark in two (`wal/flush.rs`):

- `committed_seq` — highest sequence that reached RocksDB. Advances on every successful commit.
- `synced_seq` — highest sequence an fsync covered. `record_success(max_seq, synced)` advances it
  only when the commit actually fsynced (`self.is_sync`), or when an out-of-band sync publishes
  it.

`FlushOutcomes::durable_sequence()` now reads `synced_seq`; the new `committed_sequence()` reads
`committed_seq`.

Out-of-band advance: `RocksStore::durable_sync` (new, `rocks/mod.rs`) snapshots each registered
writer's committed sequence, runs the flush, then publishes that sequence to each writer's
`synced_seq` and re-records the on-disk WAL watermark. Writers register through the new
`DurableSyncTarget` trait (weak refs, pruned on registration). `spawn_periodic_sync` calls
`durable_sync` instead of a bare `flush`, so periodic mode's fsync now actually moves the
durable watermark.

`confirm_durable_through` deliberately keeps reading *committed*: `Durability::Confirm` asks
"did this batch reach storage", not "was it fsynced", and switching it would make
`Confirm` fail permanently in periodic/async. Documented at the call site.

### Correction to this issue's premise

`durable_sequence` has exactly two consumers, and neither is what the issue claims:

1. `FlushOutcomes::confirm_durable_through` (above), and
2. `WalLagStats.durable_sequence`, which is not folded into `ShardWalLag` or `WalLagAggregate` and
   reaches no rendered field.

`WAIT`, `LASTSAVE`, and replication acks do **not** read it. The blast radius was therefore
`Durability::Confirm` plus an unrendered lag stat, not acknowledged-write loss on `WAIT`. The
defect is still real (the number lied, and it is the number any future durability consumer would
reach for), but it was not live-visible.

`WalLagStats.durable_sequence` was *renamed* to `committed_sequence` rather than joined by a
second field: it always meant committed, and adding a genuinely-durable sibling nothing renders
would be dead weight.

### Tests

`frogdb-server/crates/persistence/src/wal/tests.rs`, tagged `// FM-PERSISTENCE-043`:

- `durable_sequence_tracks_only_fsynced_commits_in_periodic_mode`
- `durable_sequence_tracks_only_fsynced_commits_in_async_mode`
- `sync_mode_durable_sequence_equals_committed_sequence`
- `empty_flush_advances_neither_sequence`

Pre-fix, three of the four fail; `sync_mode_...` passes, which is precisely why no existing test
caught this — the defect is invisible in the one mode where the two notions coincide.

### Spec

New row **FM-PERSISTENCE-043** (the reported durable sequence counts only fsynced commits) in
`.scratch/hardening/specs/persistence-failure-modes.md`. FM-PERSISTENCE-003 and -004 had their
false `sync = true` claims corrected.
