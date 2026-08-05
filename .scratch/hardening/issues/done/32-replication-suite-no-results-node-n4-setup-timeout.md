# 32 — Replication suite emits NO RESULTS: node n4 never becomes ready during Docker topology
setup

Status: done
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

## Resolution (2026-08-05)

Root cause: **not** Docker/CI resource contention and **not** a FrogDB server startup
regression. `n4` was timing out because `n4` isn't part of the replication topology at all —
`docker-compose.replication.yml` only defines `n1`/`n2`/`n3` — yet the jepsen client kept
trying to bring it up and wait for it to become ready.

`frogdb-test` (`core.clj`) selects the client's node list with:

```clojure
multi-node? (if (seq (:nodes opts)) (vec (:nodes opts)) ["n1" "n2" "n3"])
```

intending "an explicit `--node`/`--nodes` override wins, otherwise default to the 3-node
replication set." But jepsen's own CLI plumbing (`jepsen.cli/parse-nodes`, run as part of
`cli/test-opt-fn` before `frogdb-test` ever executes) unconditionally populates `:nodes`,
defaulting to `jepsen.cli/default-nodes` — a 5-node `["n1" "n2" "n3" "n4" "n5"]` list sized for
the 5-node Raft topology — whenever no `--node`/`--nodes`/`--nodes-file` flag was passed at
all. `(seq (:nodes opts))` is therefore always true, so the "otherwise" branch was dead code:
every replication-suite invocation silently ran against jepsen's unrequested 5-node default
instead of the compose file's 3 nodes, and setup hung waiting for `n4`/`n5` FrogDB servers that
were never started.

This regressed in commit `b3acce28` (2026-07-23); local `store/` timestamps confirm no
replication-suite run had produced results since.

Fix (`118ec72b`, this branch): compare `:nodes` by value against `cli/default-nodes` instead of
just checking non-empty, so jepsen's own unrequested default is distinguished from a real
operator override:

```clojure
multi-node? (if (and (seq (:nodes opts))
                      (not= (vec (:nodes opts)) cli/default-nodes))
              (vec (:nodes opts))
              ["n1" "n2" "n3"])
```

(`parse-nodes` rebuilds the vector via `concat`+`vec`, so it is no longer `identical?` to the
original `cli/default-nodes` var — value comparison via `not=` is required, `identical?` would
not work.)

Verification: `db.clj`'s `wait_for_ready` and `docker-compose.replication.yml` needed no
changes — the fix is entirely in `core.clj`'s node-selection logic. Reran the full replication
suite (`python3 testing/jepsen/run.py run --suite replication --no-build`) end to end:
`docker ps` confirmed only `frogdb-repl-n1`/`n2`/`n3` came up (no attempt to start a nonexistent
`n4`), and all 7 tests produced real results and passed:

```
store/frogdb-replication-docker-replication/20260805T130122.257-0400
store/frogdb-lag-docker-replication/20260805T130201.248-0400
store/frogdb-split-brain-partition-docker-replication/20260805T130238.603-0400
store/frogdb-zombie-partition-docker-replication/20260805T130347.999-0400
store/frogdb-replication-all-replication-docker-replication/20260805T130458.129-0400
store/frogdb-partition-recovery-partition-docker-replication/20260805T130714.759-0400
store/frogdb-replication-failover-docker-replication/20260805T130901.030-0400

7 successes, 0 unknown, 0 crashed, 0 failures

  replication           replication  0:42  PASS
  lag                   replication  0:37  PASS
  split-brain           replication  1:09  PASS
  zombie                replication  1:10  PASS
  replication-chaos     replication  2:16  PASS
  partition-recovery    replication  1:46  PASS
  replication-failover  replication  0:26  PASS

  7 tests, 7 passed, in 8:09
```

`replication-failover` — the exact test originally reported as "NO RESULTS" in this issue's
filing — now runs and passes (`:valid? true`, `:failover-occurred? true`,
`:durable-loss []`, `:async-loss-count 0`). No `n4`/`n5` timeout anywhere in the run.
