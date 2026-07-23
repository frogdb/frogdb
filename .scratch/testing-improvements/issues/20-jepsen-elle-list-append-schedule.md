# Elle list-append workload built but never scheduled in any Jepsen suite

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`testing/jepsen/frogdb/src/jepsen/frogdb/list_append.clj` implements a full RPUSH/LRANGE workload wired to `jepsen.tests.cycle.append` with `:strict-serializable` consistency checking (line 239) — Elle's list-append checker is widely considered the strongest cycle-anomaly detector available (used by Redis/Valkey/DragonflyDB Jepsen work for exactly this reason). It has a cluster-aware client and is registered in `core.clj:93`. Despite being fully built, `run.py`'s `TESTS` table (lines 92-295) never includes a `list-append` `TestDefinition` — the workload runs nowhere, in CI or manually, under the current harness invocation surface.

Expected: the strongest anomaly detector in the harness should run at least under the base "none" nemesis for the single-node/replication suite, and under the raft-cluster nemesis for the cluster suite — mirroring how other Elle-backed workloads (e.g. `cross_slot.clj`) are scheduled.

## What to build

- Add a `list-append` `TestDefinition` to the `single`/base suite with nemesis `none` (baseline correctness).
- Add `list-append` with the `raft-cluster` nemesis to the raft/cluster suite, exercising the strict-serializable checker under real fault injection (partitions/kills), consistent with how other high-value workloads are composed in `run.py`.

## Acceptance criteria

- [ ] `run.py` `TESTS` table includes at least one `list-append` `TestDefinition` with nemesis `none`.
- [ ] `run.py` `TESTS` table includes a `list-append` `TestDefinition` composed with the `raft-cluster` nemesis (or equivalent fault-injecting nemesis used by other raft-suite workloads).
- [ ] Both entries execute successfully (or with an understood/triaged failure) in a manual harness run before merging.
- [ ] Entries are picked up by whatever suite grouping is used for nightly/manual invocation (`just` recipe or `run.py` suite name), not just addressable by hand.

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/list_append.clj:239` (strict-serializable Elle check), registered `core.clj:93`
- `testing/jepsen/frogdb/run.py` (`TESTS` table, lines 92-295 — no `list-append` entry)
- `testing/jepsen/frogdb/src/jepsen/frogdb/cross_slot.clj:382` (sibling Elle-backed workload pattern for reference)
