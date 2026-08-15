# A cross-shard transaction has no per-shard undo, so CROSSSLOT is an engineering gap, not a wall

Status: needs-triage

## Parent

[issue 10](10-txn-vll-advisory-sweep.md) — its A4 item: FM-TXN-019/FM-TXN-021 rationale reworded
from "there is no cross-shard rollback story" (read as structurally impossible) to naming the actual
missing piece — per-shard undo/compensation — and filing this as the follow-up that would build it.

## What is wrong

[FM-TXN-019](../../../specs/txn.md#fm-txn-019--exec-of-a-batch-that-folded-to-more-than-one-shard)
and [FM-TXN-021](../../../specs/txn.md#fm-txn-021--allow-cross-slot-standalone-does-not-relax-transactions)
refuse a shard-spanning `MULTI…EXEC` with `CROSSSLOT` unconditionally, in both cluster mode and
standalone (even with `allow_cross_slot_standalone` on, which *does* let single-key ops cross
shards via VLL). The old rationale framed this as inherent: "transactions are deliberately denied
the cross-shard atomicity single ops get, because there is no cross-shard rollback story" — worded
as if no rollback story could exist, rather than naming what is actually missing.

What is actually missing is narrower: VLL's continuation lock already buys cross-shard *mutual
exclusion* for the whole batch (that is exactly what a cross-shard script uses it for). What it does
not buy is *atomicity* — if a batch commits writes on shard A and then fails partway through shard
B, nothing on shard A knows how to undo what it already applied. A single VLL op never needs this
because it either fully executes or the client's one reply says otherwise; a transaction's contract
is all-or-nothing across every queued command, which requires compensating (or WAL-level) undo on
every shard that already committed a piece of the batch before the failure. That per-shard undo
mechanism does not exist today, at any layer (VLL, WAL, or otherwise) — CROSSSLOT is the safe answer
given that gap, not a permanent architectural boundary.

## What to build

1. Design the per-shard undo mechanism a cross-shard transaction commit would need: options include
   per-shard WAL-level compensation records applied on abort, or holding writes in a per-shard
   staging area until every participating shard is ready, then a two-phase commit-or-undo swept
   through the continuation lock's shard set.
2. Decide the observable failure/timeout shape during the undo window (mirrors the scatter path's
   AMBIGUOUS outcome family, TR-VLL-019 — a cross-shard transaction commit has the same "already
   applied on shard A, unknown on B" problem a scatter gather timeout does).
3. Once built, FM-TXN-019/FM-TXN-021 (and the "Redis deviations" table row for them) would need to
   either state the new atomicity story or, if this is deliberately never built, gain a Rulings field
   explaining why the gap is intentional rather than a queued fix.

## Acceptance criteria

- [ ] A design for per-shard undo exists (or the gap is explicitly ruled permanent, with a rowed
      reason) before any code lands
- [ ] If built: cross-shard transactions get their own FM/TR rows describing the commit/undo
      protocol, `just lint-spec` green
- [ ] If built: FM-TXN-019/FM-TXN-021 updated to reflect the new behavior (CROSSSLOT may no longer be
      unconditional), and the Redis deviations table row updated to match
- [ ] `just mutants-diff frogdb-txn frogdb-vll` triaged on touched code

## Blocked by

None, but it is a design task before it is an implementation task — the commit/undo protocol should
be brainstormed and possibly reviewed before code is written. This is scoped as a follow-up, not a
commitment to build it: the acceptance criteria's first line allows landing a "ruled permanent, here
is why" outcome instead of the mechanism itself.
