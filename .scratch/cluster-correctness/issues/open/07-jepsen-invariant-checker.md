# 07 — Jepsen invariant checker over DEBUG CLUSTER CHECK

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5.

## What to build

A Jepsen checker that calls `DEBUG CLUSTER CHECK` on every node at nemesis quiesce
points and at final read; any non-empty reply fails the test with the violation IDs in
the analysis output. Wire it into the raft-topology workloads by default.

## Acceptance criteria

- [ ] Checker runs at quiesce + final on all raft workloads
- [ ] A seeded violation (fault-injection run against a build with a deliberately broken
      invariant) fails the test naming the invariant ID
- [ ] Checker overhead does not distort nemesis timing (calls only at quiesce)

## Blocked by

- Issue 06 (`.scratch/cluster-correctness/issues/`) — consumes the command.
