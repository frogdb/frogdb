# 25: a snapshot handle, and a full sync that does not materialize

Status: needs-triage
Type: AFK
Origin: [issue 22](22-lock-memory-spec.md) audit, 2026-09-05 — the locked
[specs/memory.md](../../../../specs/memory.md) records this as an open item
Area: frogdb-table + frogdb-core (store) + frogdb-replication (full sync)
Phase: 5 — network memory

## Why

[PRD.md](../../PRD.md) R6 wants an in-process substitute for Redis's fork: a reader that
sees a consistent keyspace while writers continue, at a memory cost proportional to what it
has not yet streamed rather than to the dataset. The draft spec called that a **snapshot
handle** and reserved a whole failure-mode group for it.

Issue 22's audit found no such thing in the code. What exists is record-level refcount
copy-on-write inside `frogdb-table` (`record.rs`), which is a different and much smaller
guarantee: it makes an individual value cheap to alias, not the keyspace cheap to read. A
full sync still materializes the dataset, and the double materialization the group was
meant to make impossible remains a known cost. So the group was dropped rather than locked —
a locked spec describes what exists.

## What to build

A `SnapshotHandle` over a shard's keyspace, with these properties, each of which becomes a
row in `specs/memory.md`:

1. **Point in time.** A handle never observes a write that landed after it was taken.
2. **Writers are never blocked.** A write during the window copies; it does not wait for the
   reader.
3. **Bounded peak.** The export's memory is a function of what it has not yet streamed plus
   whatever writers copied during the window — not of the dataset's size.
4. **Release is complete.** Dropping the handle releases everything the copies were
   protecting.

Then rewire full sync onto it, so the `fullsync_staging` budget
([issue 24](24-budget-growth-allowlist-burndown.md)) has something bounded to charge rather
than a materialized dataset to account for after the fact.

## Acceptance criteria

- A shard can serve a full sync while accepting writes, with peak RSS bounded by the
  streaming window rather than by dataset size — measured, not asserted.
- `specs/memory.md` carries the four rows above with real-thread forcing tests (the
  simulation cannot force any of them: see "What the simulation cannot force").
- The snapshot-handle open item is removed from the spec's Open items section, and the
  vocabulary entry is added back to the invariant table.

## Out of scope

Background exports other than full sync (`SAVE`/`BGSAVE` shapes) — the same handle should
serve them, but this issue lands the handle against the one consumer whose cost is measured.
