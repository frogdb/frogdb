# 27: the replica-feed hold buffer gets its byte cap

Status: needs-triage
Type: AFK
Origin: [issue 22](22-lock-memory-spec.md) audit, 2026-09-05 — the locked
[specs/memory.md](../../../../specs/memory.md) records this as an open item
Area: frogdb-cluster-runtime + frogdb-replication-runtime + frogdb-memory
Phase: 5 — network memory

## Why

[specs/cluster.md](../../../../specs/cluster.md) TR-CLUSTER-016 rules a byte cap on the
replica-feed hold buffer during an armed slot barrier. The code does not have one. A hold
with no cap is a buffer that does neither of the two things the memory vocabulary allows —
it neither sheds nor backpressures, it simply grows for as long as the barrier stays armed.

This was deliberately not implemented against the current cluster row: the cluster campaign
is rewriting TR-CLUSTER-016 to ordinary replica-feed backpressure, and implementing a cap
against a row that is about to change shape would be work thrown away. Issue 22's audit
therefore left it as an open item in `specs/memory.md` rather than writing a row for it.

## What to build

Once the cluster campaign has settled the rewrite:

1. **Charge the hold buffer** to `replication_backlog`
   ([issue 24](24-budget-growth-allowlist-burndown.md) opens that budget), so the held bytes
   appear in the operator's per-subsystem breakdown while a barrier is armed.
2. **State the disposition.** Backpressure at the feed is the expected answer — the producer
   is the local shard and can be made to wait — with shed (drop the link, force a resync) as
   the terminal case if the barrier outlives the allowance.
3. **Row it here**, in `specs/memory.md`, once. Whatever shape the cluster rewrite settles
   on, the byte cap is a memory contract written in the cluster spec's vocabulary, and it
   belongs in one place.

## Acceptance criteria

- An armed barrier holding a feed cannot grow the process without bound; the bound is
  observable in the memory breakdown while it is held.
- `specs/memory.md` carries the row with its forcing test; `specs/cluster.md`'s rewritten
  row cross-references it rather than restating the cap.
- The open item is removed from `specs/memory.md`.

## Blocked on

The cluster campaign's rewrite of TR-CLUSTER-016. Do not implement against the current row.
