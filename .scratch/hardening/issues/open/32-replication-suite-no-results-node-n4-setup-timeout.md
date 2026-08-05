# 32 — Replication suite emits NO RESULTS: node n4 never becomes ready during Docker topology
setup

Status: needs-triage
Type: bug (infra flake or startup regression — jepsen Docker topology / server boot)
Origin: split out of issue 26 while investigating jepsen-nightly manual dispatch run
30828159222 (2026-08-03), which flagged `frogdb-replication-failover-docker-replication` as
"NO RESULTS".

## What happened

Issue 26 asked whether the single test's NO RESULTS was a small wiring bug. It is not — the
entire "replication" suite (all 7 tests: `replication`, `lag`, `split-brain`, `zombie`,
`replication-chaos`, `partition-recovery`, `replication-failover`) crashed identically, before
any workload ran, with:

```
clojure.lang.ExceptionInfo: throw+: {:type :timeout, :message "Timed out waiting for FrogDB on n4"}
  at jepsen.frogdb.client$wait_for_ready ...
  at jepsen.frogdb.db$replication_db$reify__9942.setup_BANG_ (db.clj:227)
```

CI summary for the suite: "7 tests, 7 unknown, in 3:52", "0 successes, 0 unknown, 7 crashed, 0
failures", `suite replication: FAILED (run.py exit 2)`. Every test in the suite failed to even
start a workload because node `n4`'s FrogDB server never reported ready during Docker topology
setup (`replication-db`'s `setup!` in `db.clj`, via `wait_for_ready`). This is why one specific
test (`replication-failover`) showed as "NO RESULTS" in whatever summary view issue 26's author
was looking at — it's a downstream symptom of the whole suite crashing at setup, not a
test-specific wiring bug.

## Why this is out of jepsen-Clojure scope (as currently understood)

This looks like either:
- Docker/CI infra flakiness (resource contention, slow image pull, port clash) on the specific
  `n4` container for this run, or
- A genuine FrogDB server startup regression/slowness that only shows up under the replication
  topology's container/resource profile (4 nodes) and isn't reproduced by lighter suites.

Distinguishing these requires either a rerun of the replication suite (to see if it's
reproducible) or inspecting `n4`'s own container/server logs from this run (not yet pulled) to
see whether the FrogDB process on `n4` was slow to bind, crashed, or never started at all.

## Suggested next steps

- [ ] Pull node `n4`'s server-side logs (not just the jepsen control-node log) from run
      30828159222 / job 91735040211 to see whether the FrogDB process even started.
- [ ] Re-run the replication suite (or just `replication-failover`) to check reproducibility;
      if it reliably reproduces, treat as a real startup regression and bisect; if it's a
      one-off, consider whether `wait_for_ready`'s timeout in `db.clj` is too tight for CI
      resource contention on a 4-node topology.
- [ ] Check whether other suites in the same run also touch `n4`-equivalent 4th-node topologies
      and whether they showed similar (even if not fatal) slowness.

## References

- Run: https://github.com/frogdb/frogdb/actions/runs/30828159222 (job 91735040211)
- `testing/jepsen/frogdb/src/jepsen/frogdb/db.clj` — `replication-db`, `setup!` (~line 227)
- `testing/jepsen/frogdb/src/jepsen/frogdb/client.clj` — `wait-for-ready`
- Split out of `.scratch/hardening/issues/done/26-jepsen-raft-suites-fail-on-harness-npe.md`
