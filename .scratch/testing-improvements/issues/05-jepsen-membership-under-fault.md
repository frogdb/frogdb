# Jepsen: cluster membership changes (join/leave) never tested under fault injection

Status: done
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

## Resolution

Status: **done** (2026-07-23).

### What was wired

1. **`raft-membership` TestDefinition** (`testing/jepsen/run.py`) — the `membership-routing`
   workload (add node via CLUSTER MEET + slot handoff, with acked-write durability checking
   from issue 04) driven concurrently against the `raft-cluster-membership` composed nemesis.
   Unlike the pre-existing `membership-routing` test (nemesis `none`), the add-node + slot
   migration + traffic now all run *under fault load* (kills, pauses, partitions) plus extra
   node join/leave churn. Suites: `raft`, `all`. This satisfies acceptance criterion 1 — a
   membership-aware workload with a data-safety checker running against the
   `raft-cluster-membership` nemesis.

2. **`--membership-changes` CLI flag** (`testing/jepsen/frogdb/src/jepsen/frogdb/core.clj`
   `cli-opts`) — boolean, default false, documented in `lein run test --help` (jepsen
   auto-generates help from the opt-spec). It flows to `(:membership-changes opts)`, which
   `cluster_formation.clj`'s `workload` already consumes to switch to
   `membership-change-generator`. Confirmed reachable: the parsed test options in the run log
   show `:membership-changes false`, proving the option is registered. Satisfies criterion 2.

3. **`cluster-membership` TestDefinition** (`run.py`) — `cluster-formation` workload with
   `membership_changes=True` under the `raft-cluster-membership` nemesis, so the new flag is
   exercised on an automated path (not left as fresh dead config). Required a new
   `membership_changes: bool` field on `TestDefinition`, threaded into both `run_test`
   (`--membership-changes`) and `generate_batch_edn` (`:membership-changes true`), mirroring
   the existing `cluster_flag` handling.

### Two defects found and fixed in the previously-dead `raft-cluster-membership` nemesis

Wiring the nemesis into a real test surfaced bugs in code that had never executed:

- **Incomplete composed nemesis (silent fault loss).** `raft-cluster-membership` reused
  `raft-cluster-generator`, which emits `:add-latency` and `:disk-readonly/:disk-recover`
  ops, but only composed 4 of the 7 fault handlers `raft-cluster-nemesis` has. Those ops threw
  `"no nemesis can handle :add-latency"` and were silently downgraded to `:info` — the intended
  latency/disk/clock/memory faults never fired. Fixed by composing the full fault set
  (kill, pause, partition, slow-network, disk, clock, memory) plus membership. First run
  showed the `no nemesis can handle` exception; after the fix the re-run shows **0**
  occurrences. The `:final-generator` was also extended to recover every fault it can now
  apply (network-heal-all, per-node disk-recover, memory-release, clock-reset) so final reads
  run against a healthy cluster.

- **Unfair membership churn (would produce false data-loss verdicts).** The membership nemesis
  picks any spare node to join/leave. Because the `membership-routing` workload explicitly adds
  n4 and migrates a slot onto it, letting the nemesis FORGET n4 drops a live slot owner and
  destroys acked writes through operator misuse rather than a real DB fault. Restricted the
  composed nemesis's membership pool to spare node n5 (owns no slots), keeping join/leave a
  clean, orthogonal fault. Callers may still override `:initial-nodes`/`:all-nodes` via opts.

### Run evidence (CLEAN topology, `just jepsen-down` then fresh `up`)

Two end-to-end runs of `raft-membership` (`membership-routing` + `raft-cluster-membership`,
`--time-limit 120`, reusing existing `frogdb:latest` image — no server code changed):

- Run 1 (before nemesis fix): **PASS** (2:17). `:valid? true`, `:durable? true`,
  `:lost-write-keys []`, `:node-added? true`, `:migration-completed? true`,
  `:final-slot-owner "n4"`, `:redirect-info-count 27` / limit 50 (`:redirects-ok? true`),
  `:failed-ops 0`. Exceptions checker surfaced 1× `no nemesis can handle :add-latency`
  (the bug above; non-fatal, but the fault was lost).
- Run 2 (after nemesis fix): **PASS** (2:34). `:valid? true`, `:durable? true`,
  `:lost-write-keys []`, `:node-added? true`, `:migration-completed? true`,
  `:final-slot-owner "n4"`, `:redirect-info-count 24` / limit 50, `:failed-ops 0`, and
  **0** `no nemesis can handle` occurrences — all faults now handled.

**Data-loss finding: none on a clean topology.** Issue 04 observed `durable? false` (lost
`{mem-route}:k:96`) on a DIRTY topology (stale container state). Under this new fault-injection
coverage on a CLEAN topology, both runs report `:durable? true` with an empty `:lost-write-keys`
set — the acked-write loss from 04 did NOT reproduce, corroborating that it was a stale-state
artifact rather than real data loss. `lein check` passes for all touched namespaces.
