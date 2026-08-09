# 05 — Frozen encoding fixtures for ClusterCommand + ClusterStateInner

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W2 (round-2 87/F6).

## What to build

Golden JSON fixtures checked into `frogdb-cluster` tests for every `ClusterCommand`
variant and a populated `ClusterStateInner`, with round-trip assertions both directions
(serialize matches the golden file; the golden file deserializes to the value). A silent
serde rename is a rolling-upgrade wire break today — Raft log entries and snapshots cross
node versions.

## Acceptance criteria

- [ ] Golden files cover all 18 command variants + a state with every collection
      populated (nodes, slots, migrations, live handoff, nonzero `handoff_seq`)
- [ ] Renaming any serde field fails a test naming the golden file
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

None - can start immediately.
