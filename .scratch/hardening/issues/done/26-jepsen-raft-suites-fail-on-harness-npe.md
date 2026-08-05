# 26 — Jepsen raft-suite runs fail on a harness NPE (bare read op outside the independent wrapper)

Status: done
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

## Resolution

The issue's root-cause narrative was re-investigated against the raw CI job log (`gh run view
--job 91735040211 --log`, ~755k lines) rather than the summary alone, and two of its three
claims did not hold up. Findings, in order:

**1. The independent-generator path (`ind-read-op`, `independent-generator`,
`independent/concurrent-generator`) is not buggy.** Decompiled the vendored jepsen 0.3.5 jar
(`jepsen/independent.clj`) and confirmed `ConcurrentGenerator` wraps every field's per-op
generator through `(tuple-gen k (fgen k))` (independent.clj:200), and `tuple-gen` unconditionally
rewrites every `:invoke` op's `:value` to `(tuple k (:value op))` (independent.clj:100-107) —
including when the op supplied no `:value` at all, as `ind-read-op` deliberately does not (its
own docstring: "value is added by the independent generator wrapper"). So a bare
`{:type :invoke :f :read}` from `ind-read-op` reaches `IndependentHashClient.invoke!` already
correctly tupled with a non-nil field; `ind-write-op` relies on the exact same implicit wrapping
(it also does not call `independent/tuple` explicitly — there is no explicit-tuple "write path"
to mirror, contrary to the issue's fix direction #1). No raft workload routes ops around this
wrapper; `slot_migration.clj` and `key_routing.clj` don't even depend on `jepsen.frogdb.hash`.

**2. The real, reproducible NPE is in the *non-independent* `HashClient`, and it fires in the
"single" suite, not the raft suite.** `core.clj`'s shared test generator (`frogdb-test`) appends
a generic final-reads fallback — `(gen/repeat {:f :read})`, a genuinely bare, unkeyed op — to
*any* workload that doesn't set its own `:final-generator` (core.clj:190-260). The independent
branch of `hash/workload` already set `:final-generator (independent-final-read-generator)`; the
plain branch did not, so its `HashClient.invoke!` received the bare op during the "Final
reads..." phase and NPE'd at `(name field)` with `field = nil` — this matches the log's stack
trace (`hash.clj:57`) exactly. It was confirmed in the log occurring twice during the
`frogdb-hash-docker` test of the *single* suite (20260803T160028.991Z and retry
20260803T161011.600Z), not the raft suite. It was invisible in the CI summary ("single suite: OK,
15/15 passed") because `check-hash-history` only inspects `:type :ok` ops, silently absorbing the
`:fail`/exception.

**3. The raft suite's 3 actual failures (`slot-migration`, `slot-migration-partition`,
`raft-chaos`) have nothing to do with this NPE.** Grep of the isolated raft-suite log range
found zero `NullPointerException` occurrences. They are genuine correctness bugs — split out to
issue 31.

**4. `frogdb-replication-failover-docker-replication`'s "NO RESULTS"** is a symptom of the whole
7-test replication suite crashing at Docker topology setup (`Timed out waiting for FrogDB on
n4`), unrelated to any of the above and to jepsen-Clojure wiring generally — split out to issue
32.

### Fixes applied (both requested layers, scoped to the confirmed real bug)

`testing/jepsen/frogdb/src/jepsen/frogdb/hash.clj`:

- `HashClient.invoke!` (~line 48): added a defensive nil-field guard — `(if (nil? field) (assoc
  op :type :fail :error :no-field) (case (:f op) ...))` — mirroring the guard
  `IndependentHashClient.invoke!` already had. A value-less op now fails loudly with
  `:error :no-field` instead of NPE-ing on `(name nil)`.
- `workload` (~line 309), non-independent branch: added an explicit `:final-generator
  (final-read-generator)`, mirroring the independent branch, and simplified `:generator` back to
  plain `(generator opts)` (the final-reads clients call that used to be nested inside
  `:generator` via `gen/phases` is now redundant with the top-level `:final-generator` key that
  `core.clj` already composes in, so it was removed to avoid double final-reads). This is the
  actual root-cause fix: `core.clj`'s generic bare-read fallback can no longer reach this
  workload's client at all.

### Verification

- `lein check` run from `testing/jepsen/frogdb` (required `dangerouslyDisableSandbox` — the
  sandboxed run failed on `mktemp`/`java.io.IOException: Operation not permitted` writing to
  `$TMPDIR`, unrelated to the code): all namespaces compiled cleanly, including
  `jepsen.frogdb.hash`; only pre-existing, unrelated reflection warnings in `lag.clj`.
- Manual paren-balance check of `hash.clj` (Python bracket-depth walk): balanced, depth 0.
- No full jepsen docker run was attempted locally, per task scope.

### Follow-ups filed

- Issue 31 — raft suite's genuine failures: `slot-migration`/`slot-migration-partition` queued
  transaction MOVED-redirected at EXEC time; `key-routing`'s wrong-value reads under raft-chaos.
- Issue 32 — replication suite NO RESULTS: node `n4` timeout during Docker topology setup,
  crashing all 7 replication-suite tests before any workload runs.
