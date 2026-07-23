# Jepsen: cluster membership changes (join/leave) never tested under fault injection

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: jepsen

## Context

The harness already has the building blocks to test membership changes under faults, but nothing
wires them together. `membership.clj` plus the composed nemesis `raft-cluster-membership`
(`nemesis.clj:1195-1221`, dispatched at `nemesis.clj:1254`, accepted by `core.clj:223`) exist and
are accepted by the harness's CLI machinery — but no `TestDefinition` in `run.py` actually uses
`raft-cluster-membership`. Separately, `cluster_formation.clj:172-203` defines a
`membership-change-generator` gated behind a `:membership-changes` flag, but there is no CLI flag
exposed anywhere in `core.clj`'s `cli-opts` (lines 211-259) to turn it on. As a result, node
join/leave behavior under concurrent faults (partition, kill, clock skew, etc.) is tested by
nothing at all — only clean join/leave (if that) is ever exercised.

## What to build

- New `raft-membership` `TestDefinition` in `run.py` combining a membership-aware workload
  (`membership-routing` or `cluster-formation` with `:membership-changes` enabled) with the
  `raft-cluster-membership` composed nemesis.
- Add a `:membership-changes` CLI flag to `core.clj`'s `cli-opts` so
  `cluster_formation.clj`'s `membership-change-generator` is actually reachable from the command
  line, not just from Clojure code that never gets exercised.

## Acceptance criteria

- [ ] `run.py` includes a `TestDefinition` that runs a membership-aware workload against the
      `raft-cluster-membership` nemesis
- [ ] `:membership-changes` is a real CLI flag (documented in `--help`) that enables
      `cluster_formation.clj`'s `membership-change-generator`
- [ ] At least one run of the new test definition executed with results attached to this issue

## Blocked by

None - can start immediately. Related to `04-jepsen-orphaned-workloads-wire-or-delete.md`
(`membership-routing` workload) — coordinate checker fixes there with the new fault-injection
coverage here to avoid duplicate work on the same workload's checker.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/nemesis.clj:1195-1221` — `raft-cluster-membership`
  composed nemesis definition
- `testing/jepsen/frogdb/src/jepsen/frogdb/nemesis.clj:1254` — dispatch
- `testing/jepsen/frogdb/src/jepsen/frogdb/core.clj:223` — nemesis accepted
- `testing/jepsen/frogdb/src/jepsen/frogdb/cluster_formation.clj:172-203` — membership-change-generator,
  gated by unexposed `:membership-changes` flag
- `testing/jepsen/frogdb/src/jepsen/frogdb/core.clj:211-259` — cli-opts (flag absent)
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `membership-fault-testing-absent`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same, "0 uses of raft-cluster-membership in run.py; no
  CLI flag for :membership-changes")
