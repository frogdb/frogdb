# 24: cluster handoff/migration/finalization tests regressed on `main` after the output-buffer series

Status: done
Type: AFK
Origin: build-toolchain issue 02 triage, 2026-09-04 — `main` CI red since `cdbd9c3ee`
Area: frogdb-server (connection/, net) + frogdb-cluster / frogdb-cluster-runtime (LOCKED)
Phase: 5 — regression from issue 18

## Why

Every `Test` run on `main` since the first red run (`cdbd9c3ee`, 2026-09-02 22:08Z; last green
`b3981f777`, 02:03Z) fails the **Unit Tests** job on the same `frogdb-server` cluster
integration tests. Latest run `33704981830` on `76b2a6dae`: 9049 tests, 7 failed, 2 timed out
(first red: 6 failed, 29 timed out). Failing set across both runs:

- `cluster_handoff_barrier::the_barrier_holds_the_replica_feed_until_the_handoff_releases_it`
- `cluster_handoff_barrier::a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner`
- `cluster_handoff_barrier::a_write_parked_by_the_barrier_wakes_up_redirected` (first red)
- `cluster_handoff_barrier::an_exec_parked_by_the_barrier_wakes_up_redirected` (first red)
- `cluster_migration::test_e2e_migration_batched_keys`
- `cluster_migration::test_e2e_migration_empty_slot`, `::test_e2e_migration_bloom_filter` (first red)
- `cluster_finalization_window::no_write_is_acknowledged_after_the_slot_is_handed_over_under_load` (70 s, timeout)

All retry 2–3× and stay red — not flakes. The cluster crates and these test files are untouched
in the window. The only server/net commits between green and red are issue 18's output-buffer
series, `b012272d3..e090f0335` (per-core buffer pool, `NetworkOutput` budget, output-buffer
limit seam, housekeeping-tick accounting, transient reply accumulators). Working hypothesis:
migration and replica-feed traffic — large, bursty, on connections that hand off away from
`ConnectionHandler` — is now charged, limited, or starved by that path. Issue 23 already
documents that the PSYNC handoff drops the `OutputBufferAccount` charge; the same handoff
shape exists for migration.

Cluster and replication are LOCKED areas: the fix is spec-first (failure-mode row → failing
test → fix), and the fix issue is carved from this investigation, not from this text.

## What to establish (investigation, no fix)

1. Reproduce locally: `just test frogdb-server 'cluster_handoff_barrier|cluster_migration|cluster_finalization_window'` on `origin/main`. Capture the assertion / timeout output per test.
2. Bisect `b3981f777..cdbd9c3ee` over the server/net commits to the first bad commit, running only that test filter.
3. Root cause: which seam (buffer pool, `NetworkOutput` budget, limit enforcement, housekeeping tick, transient accumulators) and why it breaks handoff/migration/finalization. Name the code path and the spec row(s) (`specs/cluster.md`, `specs/replication.md`, `specs/memory.md`) whose contract it violates or that is missing.
4. Proposed fix shape, and whether it overlaps issue 23.

## Acceptance criteria (for the investigation)

- [ ] Per-test failure output captured from a local run on `origin/main`
- [ ] First bad commit identified, with the bisect log
- [ ] Root-cause narrative naming the seam and the spec row(s)
- [ ] Recommended fix issue text (spec row, forcing test, files) ready to carve

## Depends on

Nothing. Overlaps: issue 23 (replica feed accounting, same handoff seam).

## Investigation 2026-09-04

Reproduced at `76b2a6dae`: 6 failed + 1 timed out + 9 flaky of 46 (360 s; quiet baseline
`b3981f777` 46/46 in 45 s). Bisect over the 3-binary filter
`-E 'binary(cluster_handoff_barrier) + binary(cluster_migration) + binary(cluster_finalization_window)'`
(the `just test frogdb-server '<pattern>'` form matches test *names*, not binaries, and selects
none of these; a single-binary run passes everywhere and is non-diagnostic):

| commit | result |
|---|---|
| `b3981f777` | 46/46, 45.2 s |
| `fe96d448a` output-buffer seam | 46/46, 48.4 s |
| `22bf8c884` | 46/46, 48.6 s |
| **`e67002d6f`** housekeeping tick | 1 fail / 7 fail (two runs), 57–118 s — **first bad** |
| `cdbd9c3ee` (CI first red) | 3 fail, 59.9 s |
| `dee17c47f` JSON tape | 9 fail, 104.7 s |
| `76b2a6dae` | 6 fail + 1 timeout + 9 flaky, 360 s |

**Root cause:** not a memory/limit defect. Every failure is one of the finalizer's three timeout
arms in `frogdb-server/crates/server/src/slot_migration/mod.rs::complete()` (`source did not
drain in 50ms` dominant; `prepare did not become visible`; `handoff barrier window elapsed`),
surfaced through `cluster_handoff_barrier.rs:224` or `SETSLOT NODE failed`. `HANDOFF_DRAIN_WAIT_MS`
(50 ms, `frogdb-cluster/src/types.rs:656`) was sized against the quiet-machine finalization
measurement of 2026-08-05 with headroom against the *residual*, not against scheduler latency.
Under nextest's `cluster` group (`max-threads = 2`) two 3-node debug clusters plus 32 writers
share the cores and a Raft round trip routinely exceeds 50 ms. `e67002d6f` (1 Hz `IDLE_TICK`
housekeeping arm on every connection; per-flush `account_buffered_output()` replacing
`note_drained()`) is the first commit whose added per-connection cost crosses the threshold;
every later series (JSON tape, jemalloc arenas, packed types) adds more, so reverting
`e67002d6f` alone would not restore green. `test_e2e_migration_empty_slot` fails with zero
keys, and solo runs pass, which rules out reply-volume/`NetworkOutput` shedding (ceiling 512 MiB).
The handoff code path is byte-identical across the window.

**Spec:** nothing violated — FM-CLUSTER-091 (`specs/cluster.md:1862`) is honoured to the letter;
the source is descheduled, not wedged, and no row distinguishes the two. Missing row drafted as
**FM-CLUSTER-104**: a slow-but-live source still finalizes; budgets derived from observed
round-trip latency; `HANDOFF_BARRIER_MS` unchanged (FM-CLUSTER-095 fence); wedged source still
aborts within `HANDOFF_DRAIN_TIMEOUT_MS`.

**Proposed fix issue (M, LOCKED cluster, spec-first):** add FM-CLUSTER-104 + FM-CLUSTER-091
cross-reference; move the budget derivation into `frogdb-cluster` (beside the constants) so the
forcing unit test `a_slow_but_live_source_still_finalizes` lives in the mutated crate; have
`await_prepared_seq`/`await_drained`/`poll_handoff` call it. Not the fix: raising
`HANDOFF_BARRIER_MS`; capping the nextest cluster group at 1. No overlap with issue 23 (no shared
file, spec area or failure mode) — sequence after it only to avoid racing the suite's green.

**Open rulings:** (1) adaptive EWMA budget vs `max(50 ms, k × heartbeat_interval)` vs bounded
retry; (2) whether `.config/nextest.toml` `cluster.max-threads` should drop to 1 as a separate,
deliberate ruling; (3) confirm on the aarch64 testbox whether the margin is laptop-specific.
Incidental finding filed as issue 25 (jemalloc arenas created in test binaries that never make
jemalloc the global allocator).

## Resolution

Investigation issue; closed 2026-09-04 with the findings above. The fix is carved as
[issue 26](../open/26-slot-handoff-drain-budget-derived-from-heartbeat-interval.md) under
build-toolchain D5 (derived budget `max(50 ms, k × heartbeat_interval)` inside an unchanged
barrier; nextest `cluster.max-threads` stays 2; no testbox confirmation). The spec row drafted
above as FM-CLUSTER-104 lands as **FM-CLUSTER-108** — 104 already names the barrier
reconstruction row. Incidental finding → [issue 25](../open/25-jemalloc-arenas-created-when-jemalloc-is-not-the-global-allocator.md).
