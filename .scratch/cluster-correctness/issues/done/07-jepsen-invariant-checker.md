# 07 — Jepsen invariant checker over DEBUG CLUSTER CHECK

Status: done

## Resolution (2026-08-10)

`testing/jepsen/frogdb/src/jepsen/frogdb/invariant.clj` sweeps `DEBUG CLUSTER CHECK`
across all nodes at nemesis quiesce points and at final read; wired into the raft
workloads by default. Results surface as a `:cluster-invariants` map
(`:sweeps-run` / `:violating-sweeps` / `:violation-ids` / per-node `:details`) and any
violating sweep fails the test. A double-counting bug (each sweep counted once per node)
was found and fixed during validation.

Acceptance evidence (store runs, split-brain-raft-partition workload):
- **Seeded violation caught**: run `20260809T230633` against a build carrying a throwaway
  always-firing invariant → `:valid? false`, `:violation-ids ["INV-SEEDED-TEST-1"]`,
  both sweeps flagged. Throwaway patch reverted; `INV-SEEDED-TEST` appears nowhere in
  the source tree.
- **Clean run green**: run `20260809T234818` on the clean build → `:valid? true`,
  `:sweeps-run 2, :violating-sweeps 0`, workload + stats + perf all valid.
- Checker calls only at quiesce/final, so nemesis timing is undistorted.

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
