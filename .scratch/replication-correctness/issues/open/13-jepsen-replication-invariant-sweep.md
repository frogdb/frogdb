# 13 — Jepsen invariant sweep over `DEBUG REPLICATION CHECK`

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5 (the Jepsen half).

## What to build

`testing/jepsen/frogdb/src/jepsen/frogdb/invariant.clj` already implements the whole quiesce+final
pattern as a nemesis wrapper: `wrap-nemesis` intercepts one `:f` and delegates everything else, so
it composes with any nemesis package with zero per-workload changes, and `core.clj:268/276/295/307`
wires it for `cluster-mode?`. What it does not do is vary the command it calls or the result key it
reports under.

Parameterize both, then extend the wiring to `Topology.REPLICATION`. That is one change covering
all nine replication workloads (`testing/jepsen/run.py:256-380`): `replication`, `lag`, `zombie`,
`split-brain`, `register-partition`, `replication-chaos`, `replication-failover`,
`replication-failover-chain` and `replication-clock-skew`. The cluster wiring keeps its current
behavior — this is a generalization, not a rewrite.

Carry over the counting bug validation found on the cluster side: **every check op appears twice in
the history** (dispatch + completion), so the checker must filter on `(map? (:value op))`, not on
`:f` alone, or every sweep is counted once per node. Keep the calls at quiesce and final only, so
nemesis timing stays undistorted.

Validation mirrors cluster issue 07's evidence format, and the two runs are the acceptance
evidence, not a formality:

- **seeded violation** — one fault-injection run against a build carrying a throwaway always-firing
  invariant, which must come back `:valid? false` naming the invariant id in the analysis output;
  the throwaway patch is then reverted and the seeded id must appear nowhere in the source tree;
- **clean run** — one run on the clean build coming back `:valid? true` with sweeps run > 0 and
  violating sweeps 0.

Both cited by store run timestamp in this issue when it closes.

## Acceptance criteria

- [ ] `invariant.clj` parameterized on the DEBUG command and the result key; the cluster wiring's
      behavior is unchanged
- [ ] All nine `Topology.REPLICATION` workloads sweep `DEBUG REPLICATION CHECK` at quiesce and
      final, with no per-workload changes beyond the wiring
- [ ] The `(map? (:value op))` filter is carried over, with evidence that sweep counts are right
      (not doubled)
- [ ] Seeded-violation run fails naming the invariant id; the throwaway patch is reverted and the
      seeded id is absent from the tree
- [ ] Clean run green (`:valid? true`, sweeps run > 0, violating sweeps 0); both store run ids
      recorded here

## Blocked by

- Issue 03 (`.scratch/replication-correctness/issues/`) — consumes the command, and needs it
  always-compiled because Jepsen runs release binaries.
