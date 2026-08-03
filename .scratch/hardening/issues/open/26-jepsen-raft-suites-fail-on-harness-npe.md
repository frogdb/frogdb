# 26 — Jepsen raft-suite runs fail on a harness NPE (bare read op outside the independent wrapper)

Status: ready-for-agent
Type: bug (test harness — Clojure, cluster suites)
Origin: jepsen-nightly manual dispatch run 30828159222 (2026-08-03), the first run after the
Docker-build chain was fixed. 20 tests: 17 passed, 3 failed — all three in the raft suite.

## What happened

`slot-migration` (0:19), `slot-migration-partition` (1:45) and `raft-chaos` (2:15) all fail with
worker-thread NPEs, not with anomalies:

```
jepsen.frogdb.hash Unexpected error:
java.lang.NullPointerException: Cannot invoke "clojure.lang.Named.getName()" because "x" is null
  at clojure.core$name.invokeStatic (core.clj:1610)
  at jepsen.frogdb.hash.HashClient invoke_BANG_ (hash.clj:57)
```

The replication suites in the same run were clean ("Everything looks good!", `:valid? true`
across set/read/repl-offsets checkers), so this is confined to the raft/cluster workloads.

## Root cause (located, not fixed)

`testing/jepsen/frogdb/src/jepsen/frogdb/hash.clj`:

- `ind-read-op` (line ~196) emits a bare `{:type :invoke :f :read}` — no `:value` — with the
  documented expectation that "the value is added by the independent generator wrapper"
  (`independent/tuple`, line ~228).
- `invoke!` (line ~48) destructures `[field v]` from `(:value op)`; for a bare read this yields
  `field = nil`, and `(name field)` at line 57 throws.
- The three failing suites evidently route `ind-read-op` (or an equivalently value-less op)
  to the client without the independent wrapper. The comment at line ~130 already knows a bare
  `{:f :read}` is dangerous ("cannot be keyed") but the guard is not on the `invoke!` path.

Also in the same run: `frogdb-replication-failover-docker-replication` reported `NO RESULTS`
(20260803T163302.774Z store entry) — investigate whether it is the same NPE, a teardown race,
or an unrelated topology failure.

## Fix directions (pick at implementation time)

1. Find where the three raft workloads compose their generators and wrap the field ops in
   `independent/tuple` like the plain hash workload does; or
2. Make `invoke!` refuse a value-less op explicitly (`:type :fail, :error :missing-field`) so a
   composition bug reads as a red harness assertion instead of an NPE storm; ideally both.

## Acceptance criteria

- [ ] The three raft suites run to a verdict (pass or genuine anomaly), no NPEs in the store logs.
- [ ] A value-less `:read`/`:write` op reaching `HashClient invoke!` produces a typed failure,
      not an NPE.
- [ ] `frogdb-replication-failover-docker-replication` NO RESULTS explained (fixed or filed
      separately).

## References

- Run: https://github.com/frogdb/frogdb/actions/runs/30828159222
- `testing/jepsen/frogdb/src/jepsen/frogdb/hash.clj` — `invoke!` (~48-57), `ind-read-op` (~196),
  independent wrapper (~228).
- Artifact-upload colon-path failure from the same run fixed separately in workflow_gen
  (tar-before-upload).
