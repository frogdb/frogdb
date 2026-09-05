# 26: the replication receive path holds a budget, not just a ceiling

Status: needs-triage
Type: AFK
Origin: [issue 22](22-lock-memory-spec.md) audit, 2026-09-05 — the locked
[specs/memory.md](../../../../specs/memory.md) records this as an open item
Area: frogdb-replication (receive path) + frogdb-memory
Phase: 5 — network memory

## Why

[specs/replication.md](../../../../specs/replication.md) FM-REPLICATION-068 bounds a
wire-supplied dataset blob against a 16 GiB constant before allocating it, and is honest
about what that buys: *"A ceiling is not a budget."*

`specs/memory.md`'s vocabulary makes the distinction contractual. A **ceiling** is a
constant bound at one site that makes an absurd value fail cleanly — a length that cannot be
a real dataset. A **budget** is a live allowance derived from what this node actually has,
which can refuse a perfectly reasonable request because the node is full. A ceiling defends
against a lie; a budget defends against the truth. The receive path currently has only the
first, so a joining replica on a small node can be told an entirely plausible dataset size
and accept it right into an OOM kill.

Issue 22's audit kept the migration as an open item rather than writing a memory row for
behaviour that does not exist.

## What to build

1. **A `Budget` on the receive path**, opened by the replica's broker against a subsystem
   (most likely `fullsync_staging`, shared with
   [issue 24](24-budget-growth-allowlist-burndown.md)), charged before the blob is
   allocated and released when it is applied or abandoned. Disposition is backpressure at
   the link, not shed: a refused full sync should fail the sync attempt cleanly and let the
   replica retry, not drop bytes mid-dataset.
2. **Keep the ceiling.** It stays as the cheap early refusal for a header that cannot be
   honest, ahead of any budget arithmetic. FM-REPLICATION-068 is amended to say the ceiling
   is the first of two gates, not the only one.
3. **A row in `specs/memory.md`** for the budget half, cross-referencing FM-REPLICATION-068
   rather than duplicating it — one implementation, two rows, the way FM-MEMORY-001 and
   FM-REPLICATION-069 already split the output-buffer account.

## Acceptance criteria

- A replica whose node cannot hold the announced dataset refuses the sync with a distinct,
  observable outcome rather than being OOM-killed.
- FM-REPLICATION-068 is amended (not deleted) and the new memory row exists with forcing
  tests.
- The open item is removed from `specs/memory.md`.

## Out of scope

Chunked/streamed application of the dataset — FM-REPLICATION-068 already reads a chunk at a
time; this issue is about whether the node agreed to hold the result.
