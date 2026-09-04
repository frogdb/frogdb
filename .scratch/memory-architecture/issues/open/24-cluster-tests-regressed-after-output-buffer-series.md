# 24: cluster handoff/migration/finalization tests regressed on `main` after the output-buffer series

Status: needs-triage
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
