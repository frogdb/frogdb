# Elle list-append workload built but never scheduled in any Jepsen suite

Status: done
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

## Resolution

Resolved 2026-07-23. The Elle `list-append` strict-serializable workload is now
scheduled in two `TestDefinition`s in `testing/jepsen/run.py`, and a latent wiring
bug that had gone undetected precisely because the workload was never scheduled
was found and fixed.

### Scheduling (acceptance criteria)

- `list-append` — workload `list-append`, nemesis `none`, `Topology.SINGLE`,
  30s, `suites=("single", "crash", "all")`. Baseline strict-serializability of the
  transactional RPUSH/LRANGE path with no faults. (Criteria 1 + 4.)
- `list-append-raft` — workload `list-append`, nemesis `raft-cluster`,
  `Topology.RAFT`, `cluster_flag=True`, 120s, `suites=("raft", "all")`. Drives the
  Elle checker under the harshest composed fault set (kills + pauses + partitions +
  slow-net + disk + clock + memory on the core nodes). (Criteria 2 + 4.)

Both are picked up by their suite groupings (`single`/`crash`/`all` and
`raft`/`all`), so they run in any manual/nightly suite invocation, not only by
name. `run.py list` now shows `single` = 11 tests and `raft` = 16 tests.

### Wiring bug found and fixed (the reason it "was never run")

Scheduling the workload for the first time immediately crashed Elle's checker with
a `NullPointerException` in `elle.txn/intermediate-write-indices` ("op.value is
null"), reported as "Errors occurred during analysis, but no anomalies found"
(verdict `:valid? :unknown` → FAIL). Root cause: `list_append.clj` was missing the
final-reads integration that the *already-scheduled* sibling Elle workload
(`elle_rw_register.clj`) has. `core.clj`'s generic final-reads phase emits plain
`{:f :read}` ops (nil `:value`); the list-append client neither guarded against
them nor supplied a compatible `:final-generator`, so nil-valued ops entered the
Elle append history and blew up the checker. Ported both pieces from
`elle_rw_register.clj`:

1. A client-side `:read` guard in both `ElleListAppendClient` and
   `ClusterElleListAppendClient` `invoke!` that acks final-read ops with `:value []`.
2. A `:final-generator` on the workload map emitting Elle-shaped read-only txns
   (`{:f :txn :value [[:r k nil] ...]}`) instead of relying on the nil-value fallback.

This is a real harness bug that only a scheduled run could surface — the workload
had never executed end-to-end before.

### Smoke-run results (Docker, `--time-limit 60`, clean topologies)

- `list-append` (single, none): PASS — `:valid? true`, "Everything looks good!",
  no anomalies.
- `list-append-raft` (raft, raft-cluster): PASS — `:valid? true`, no anomalies.
  Substantive run: cluster converged (`state=ok`, leader elected); nemesis fired
  real faults (leader-isolated + asymmetric partitions, pauses on n2, heal); Elle
  verified 152,594 `:txn` ops (all `:ok`, 0 fail, 0 info) as strict-serializable.

No Elle anomalies (G0/G1a/G1b/G1c/G2) or strict-serializability violations were
found on either variant, so both remain in the default suites (no red variant to
quarantine). `lein check` clean; `ruff check`/`format --check` clean. `store/`
artifacts and scratch logs excluded from the commit.

### Files changed

- `testing/jepsen/run.py` — two `TestDefinition` entries.
- `testing/jepsen/frogdb/src/jepsen/frogdb/list_append.clj` — `:read` guard in both
  clients + `:final-generator` on the workload (+ `jepsen.generator` require).
