# Fix stale test/doc-comments; investigate flaky cluster-info test under coverage instrumentation

Status: needs-triage
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
