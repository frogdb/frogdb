# Fix stale test/doc-comments; investigate flaky cluster-info test under coverage instrumentation

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Cluster / Persistence / Basic Commands / CI

## Context

Several stale doc-comments were identified across the audit, plus one flaky test surfaced by the
coverage run. Bundled here as low-cost cleanup:

1. **`integration_cluster.rs:6-9`** — doc-comment claims "Many tests marked `#[ignore]`… CLUSTER
   commands currently return hardcoded standalone responses." This is fully stale per the F-report
   premise correction: zero real `#[ignore]` attributes remain in the file; `CLUSTER
   INFO`/`NODES`/`SLOTS`/`SHARDS` render live `ClusterState` (`commands/cluster/mod.rs:182-448`) via
   real Raft; hardcoded strings only occur in genuine standalone mode (`ctx.cluster_state == None`).

2. **`integration_dump_restore.rs:5`** — module doc-comment is stale per the D-report's
   "Verified NOT gaps" section: DUMP/RESTORE for Stream/Bloom/TimeSeries are no longer stubbed;
   active round-trip tests exist (`integration_dump_restore.rs:188,209,229`), and zero `#[ignore]`
   attributes remain for these types.

3. **`info_tcl.rs:42-56`** — stale per A-report gap 1: claims the errorstats feature is
   unimplemented and excludes all 10 ported upstream errorstats tests, but errorstats is fully
   implemented (counters + 128-cap at `client_registry/mod.rs:37-88`, prefix logic `:96-99`,
   dispatch wiring `dispatch.rs:614-622` and `connection.rs:375-379`, rendering
   `info/sections.rs:532-542`). **Check task 44 (`errorstats-e2e`) status before editing** — if task
   44 already fixes this doc-comment as part of its own work, do not duplicate/conflict with it here.

4. **`test_frogdb_version_reports_cluster_info`** — flaky under the coverage-instrumented
   environment per `.scratch/testing-improvements/audit/coverage-summary.md` (failed 1 of 3 runs under
   coverage instrumentation, passed on retry). Needs investigation and deflaking — most likely a
   timing assumption that coverage instrumentation's slower execution violates, but should be
   root-caused rather than assumed.

## What to build

Rewrite the three stale doc-comments to reflect actual current behavior; root-cause and deflake the
flaky test.

## Acceptance criteria

- [ ] `integration_cluster.rs:6-9` doc-comment rewritten to reflect live `ClusterState` rendering via
      real Raft; stale `#[ignore]`/hardcoded-response claim removed.
- [ ] `integration_dump_restore.rs:5` module doc-comment updated to remove stale "stubbed" claims for
      Stream/Bloom/TimeSeries.
- [ ] `info_tcl.rs:42-56` doc-comment fixed to stop excluding the 10 ported upstream errorstats tests
      (coordinate with task 44 first — confirm it hasn't already landed this fix before duplicating).
- [ ] `test_frogdb_version_reports_cluster_info` root-caused; either deflaked outright, or given an
      explicit, documented retry/tolerance with a code comment explaining the coverage-instrumentation
      timing sensitivity — not a silent skip or ignore.

## Resolution

**Doc-comments.**

- `integration_cluster.rs:6-9` rewritten: states that every test runs against a live Raft-replicated
  `ClusterState` and that the hardcoded standalone responses only apply when
  `ctx.cluster_state == None`. Verified zero `#[ignore]` attributes in the file.
- `integration_dump_restore.rs:1-7` rewritten: no type's serialization is stubbed and no test is
  `#[ignore]`d. Also fixed two stale artifacts the audit did not name: the section banner
  "Known-gap documentation tests (stubbed serialization)" → "Module-type round-trips
  (Stream / BloomFilter / TimeSeries)", and three "After proper serialization, …" comments that
  implied the round-trips were aspirational.
- `info_tcl.rs:42-56` — **no change needed**. Task 44 already rewrote this header; it now documents
  errorstats/commandstats as implemented, ports the upstream scenarios, and files the residual
  dispatch-stage gap as issue 63. Editing it would have duplicated/conflicted with task 44.
- Sweep for other stale headers found one more: `string_tcl.rs:1697` claimed
  "Extended SET with IFEQ / IFNE / IFDEQ / IFDNE … not yet implemented in FrogDB" while the ten
  tests directly beneath it assert the features working. Corrected.

**Pre-existing breakage found and fixed (unrelated to this issue, but blocking).**
Commit `a3bbb204` truncated `integration_cluster.rs`: the tail of
`test_cluster_scan_is_per_node_and_unions_to_full_keyspace` lost
`harness.shutdown_all().await;`, its closing `}`, and the opening `// ===` rule of the following
banner. The file did not parse, so `cargo fmt` and the whole `frogdb-server` test target were
broken on `main`. Restored.

**FINALIZE-family env-dependent failures — root cause.**
Not coverage-instrumentation timing, and not a version skew between environments: `CARGO_PKG_VERSION`
is `0.1.0` in both. It is a lost-update race in the test fixture.

After startup each node proposes a one-shot `AddNode` for *itself* over Raft
(`server/src/server/cluster_init.rs:419`) to correct the guessed client address. Applying `AddNode`
replaces the entire `NodeInfo` — real address *and* real `env!("CARGO_PKG_VERSION")`
(`cluster/src/types.rs:97`). `harness.fake_all_node_versions("0.2.0")` writes only to each node's
in-memory `ClusterState`, so when a self-registration commits *after* the fake, it silently clobbers
`0.2.0` back to `0.1.0`. `FROGDB.FINALIZE` then rejects the cluster at
`cluster/src/commands.rs:392` with
`ERR invalid operation: node <id> is at version 0.1.0 but finalization requires 0.2.0`.

`wait_for_cluster_convergence` does not order against this; only address convergence does, because
the corrected address is the observable proof that the self-registration entry has applied.
`test_frogdb_finalize_success` and `test_info_gate_active_after_finalize` had already hit this and
carried a hand-rolled `wait_for_address_convergence` guard; `test_frogdb_version_reports_cluster_info`
and `test_admin_upgrade_status_after_finalize` did not. macOS happened to schedule the
self-registration before the fake; the aarch64 Linux testbox reliably lands it after, which is why
the split was environmental and deterministic on each side.

**Fix.** Moved the guard into the harness instead of the call sites, so the footgun cannot recur:
`fake_all_node_versions` / `fake_node_version` (`test-harness/src/cluster_harness.rs`) are now `async`,
await `wait_for_address_convergence` before writing, and return `Result`. The two hand-rolled guards
were removed, and all 15 call sites updated. Also replaced two fixed
`sleep(500ms)`-then-assert races with bounded polls (in `test_frogdb_version_reports_cluster_info`
and the rolling-upgrade test), and made the `Finalize should succeed` assertion print the response
so a future failure is diagnosable rather than opaque.

**Verification.** Both previously-env-dependent tests now pass in both environments.

Blacksmith testbox (aarch64 Linux, `tbx_01kykh0hrtbh4pwsxe7rfdzsc6`), 3 consecutive runs, `--retries 0`:

```
PASS [   1.198s] (1/2) frogdb-server::main integration_cluster::test_admin_upgrade_status_after_finalize
PASS [   1.212s] (2/2) frogdb-server::main integration_cluster::test_frogdb_version_reports_cluster_info
  Summary [   1.219s] 2 tests run: 2 passed, 1956 skipped     (x3, no FAIL)
```

Local (macOS arm64), 4 runs: `2 tests run: 2 passed, 1956 skipped` each time. The full
FINALIZE/upgrade family (`-E 'test(/finalize|upgrade|info_gate|frogdb_version/)'`) is also green —
`12 tests run: 12 passed, 1946 skipped`, including `test_rolling_upgrade_with_continuous_traffic`,
which exercises `fake_node_version` across node restarts.

Note: the first testbox run of this task failed to compile on the `a3bbb204` truncation described
above, which independently confirmed that breakage was on `main` and not a local artifact.

## Blocked by

Check task 44 (`errorstats-e2e`) status before touching `info_tcl.rs:42-56`, to avoid duplicate or
conflicting edits to the same doc-comment.

## References

- .scratch/testing-improvements/audit/F-cluster.md (premise correction, line 3)
- .scratch/testing-improvements/audit/D-persistence.md (Verified NOT gaps section, line ~44)
- .scratch/testing-improvements/audit/A-basic-commands.md #1 (`errorstats-info-untested-end-to-end`)
- .scratch/testing-improvements/audit/verdicts-A.md #1
- .scratch/testing-improvements/audit/coverage-summary.md (flaky test entry)
- frogdb-server/crates/server/tests/integration_cluster.rs:6-9
- frogdb-server/crates/server/tests/integration_dump_restore.rs:5
- frogdb-server/crates/redis-regression/tests/info_tcl.rs:42-56
