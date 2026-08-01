# `FlushOutcomes::durable_sequence` advances on `sync = false` commits — "durable" means "handed to RocksDB"

Status: ready-for-agent
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

- [ ] A crate-level test drives the `FlushEngine` in `Periodic{interval_ms: 60_000}` with a sink
      that records `sync`, commits N ops, and asserts `durable_sequence()` does **not** advance
      past the last fsync'd sequence. Fails today.
- [ ] Forcing a periodic sync then advances it to exactly N.
- [ ] A no-op `flush()` on an empty stage reports the correct `durable_seq`, not a stale one.
- [ ] The same assertions run for `Async` mode, and `Sync` mode is asserted unchanged.

## Test boundary

Level 2 (crate API of the WAL flush engine) — the two notions of "durable" are only
distinguishable at the sink seam. Not level 3 or 4: a server test could not distinguish them
without a power-loss primitive, which is exactly the confusion that hid the defect.

## Depends on

nothing to write the level-2 test. A full end-to-end proof (acknowledged write lost after power
loss) would additionally need issue 02 (I2 — subprocess-SIGKILL crash primitive),
`.scratch/testing-improvements-round2/issues/`, which the proposal did not itself request.
