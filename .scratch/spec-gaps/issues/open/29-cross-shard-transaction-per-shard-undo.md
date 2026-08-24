# Cross-shard MULTI/EXEC in standalone: design crash-atomicity across per-shard WALs

Status: ready-for-human

Size: L

Stage 1 delivered: design doc at
[`.scratch/spec-gaps/2026-08-23-issue29-standalone-cross-shard-exec-design.md`](../../2026-08-23-issue29-standalone-cross-shard-exec-design.md)
(covers points 1-5, nine open questions) — stage 2 implementation awaits HITL review of it.

> **Ruling (2026-08-22, R8 in
> [work-item rulings](../../../cluster-correctness/2026-08-22-work-item-rulings.md)):**
> this is a **standalone-only design task**, ready-for-agent for the design phase.
> Refusing cross-shard `MULTI`/`EXEC` with `-CROSSSLOT` in standalone is a real
> Redis-compat gap — standalone Redis has one keyspace and accepts any key mix; our
> refusal is a deviation that is not an improvement. The original framing ("build
> per-shard undo") is corrected: Redis `EXEC` has **run-all semantics** — a command
> failing mid-EXEC does not abort the others, so there is no runtime rollback to build.
> The actual missing piece is **crash-atomicity across per-shard WALs** (transaction
> markers / commit record — the equivalent of Redis wrapping the batch in one AOF
> MULTI..EXEC block). VLL's continuation lock already provides cross-shard isolation
> (DragonflyDB proves the shape on the same lock design). **Cluster mode keeps
> `CROSSSLOT`** — Redis-cluster parity and migration safety. Two-stage: design doc with
> HITL review first, then implementation.

## Parent

[issue 10](10-txn-vll-advisory-sweep.md) — its A4 item: FM-TXN-019/FM-TXN-021 rationale reworded
from "there is no cross-shard rollback story" (read as structurally impossible) to naming the
actual missing piece, and filing this follow-up.

## What is wrong

[FM-TXN-019](../../../specs/txn.md#fm-txn-019--exec-of-a-batch-that-folded-to-more-than-one-shard)
and [FM-TXN-021](../../../specs/txn.md#fm-txn-021--allow-cross-slot-standalone-does-not-relax-transactions)
refuse a shard-spanning `MULTI…EXEC` with `CROSSSLOT` unconditionally, in both cluster mode and
standalone (even with `allow_cross_slot_standalone` on, which *does* let single-key ops cross
shards via VLL). In standalone that refusal breaks real Redis clients: against standalone Redis,
`MULTI; SET a ..; SET b ..; EXEC` works for any keys.

What a compatible implementation needs, precisely:

- **Isolation**: no other client observes the batch partially applied. VLL's continuation lock
  already provides this — it is exactly what a cross-shard script uses.
- **Runtime "atomicity"**: Redis's contract is run-all — errors from individual commands inside
  `EXEC` do not abort the rest and are returned in the reply array. Nothing to build.
- **Crash-atomicity**: after a crash mid-EXEC, recovery must replay all of the batch's writes or
  none. Redis gets this by wrapping the batch in a single AOF `MULTI..EXEC` unit. With per-shard
  WALs this needs a cross-WAL commit protocol: transaction markers plus a commit record (or
  staging + two-phase apply swept through the continuation lock's shard set).

## What to build

**Stage 1 — design doc (this issue's ready-for-agent scope):**

1. The cross-WAL crash-atomicity protocol: marker/commit-record layout, which WAL (or separate
   log) owns the commit decision, replay rules on recovery (discard unmarked partial batches,
   apply committed ones), interaction with WAL truncation/checkpointing.
2. The dispatch shape: how a standalone cross-shard EXEC folds onto the continuation lock
   (scatter the queued commands per shard under one continuation, gather replies in queue order).
3. Failure/timeout observability during the commit window — mirrors the scatter path's AMBIGUOUS
   outcome family (TR-VLL-019).
4. Replication story: how the batch feeds the replica stream as one atomic unit.
5. Spec deltas: FM-TXN-019 narrowed to cluster mode, FM-TXN-021 rewritten (or retired) for the
   new standalone behavior, new FM/TR rows for the commit protocol, deviations-table row updated.

**Stage 2 — implementation** (separate dispatch after HITL review of the design doc; spec-first
per LOCKED-area discipline).

## Acceptance criteria

- [ ] Design doc covering points 1-5 above, reviewed by the design owner (HITL) before any code
- [ ] Design keeps cluster-mode `CROSSSLOT` intact and states why
- [ ] After stage 2: cross-shard standalone EXEC has its own FM/TR rows, `just lint-spec` green;
      FM-TXN-019/021 + deviations table updated; crash-atomicity forced by a
      kill-mid-EXEC/recover test
- [ ] After stage 2: `just mutants-diff frogdb-txn frogdb-vll` triaged on touched code

## Blocked by

None - design phase can start immediately. Stage 2 blocked on HITL design review.
