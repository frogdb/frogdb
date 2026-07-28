# Jepsen: three single-node checkers are weaker than the fault coverage they gate

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: Jepsen harness (area G)

## Context

Three related checker weaknesses, all confirmed in the adversarial pass:

1. `cluster_formation.clj:229` — `:valid? (or (= "ok" final-state) (nil? final-state))`. A
   `nil` final-state (e.g., the check never actually ran / couldn't observe state) passes as
   valid. Node-count consistency across the cluster is never checked at all.
2. `hash.clj:234` — defaults to a weak (non-independent) checking path. The stronger
   linearizable independent-checker (`hash.clj:204-211`) is gated behind `--independent`, which
   `run.py` never passes to any test invocation — so the stronger checker never actually runs in
   practice.
3. `sortedset.clj` — tracks `latest-scores` (`:274-281`) as it observes writes, but the final
   `:valid?` computation (`:291`) never cross-checks the final read against those tracked scores
   — the tracking data is collected and then discarded without being used for verification.

Each of these means a Jepsen run can complete "green" while missing exactly the class of bug the
checker exists to catch — nil-state cluster formation issues, non-independent hash inconsistency,
and sortedset score drift.

Verdict (adversarial pass): CONFIRMED L2/C2, all three sub-claims verified independently
(confirmed zero `--independent` occurrences anywhere in `run.py`).

## What to build

Fix all three checkers:
- `cluster_formation.clj`: fail (not pass) on `nil` final-state; add node-count consistency
  assertion.
- `hash.clj`: wire `--independent` into the relevant `run.py` TestDefinition(s) so the
  linearizable independent-checker actually runs.
- `sortedset.clj`: cross-check the final read against tracked `latest-scores` in the `:valid?`
  computation.

## Acceptance criteria

- [ ] `cluster_formation.clj` `:valid?` returns `false` when `final-state` is `nil`.
- [ ] `cluster_formation.clj` asserts node-count consistency as part of `:valid?`.
- [ ] At least one `run.py` TestDefinition for the hash workload passes `--independent`,
      exercising the linearizable independent-checker path.
- [ ] `sortedset.clj` `:valid?` fails when the final read diverges from tracked `latest-scores`.
- [ ] A deliberately-broken run of each workload (e.g., inject a known-bad final state/score) is
      manually or CI-verified to now fail where it previously would have passed.

## Blocked by

None - can start immediately

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/cluster_formation.clj:229`
- `testing/jepsen/frogdb/src/jepsen/frogdb/hash.clj:204-211,234`
- `testing/jepsen/frogdb/src/jepsen/frogdb/sortedset.clj:274-281,291`
- `testing/jepsen/frogdb/run.py`
- `.scratch/testing-improvements/audit/G-jepsen-harness.md` (`weak-single-node-checkers`)
- `.scratch/testing-improvements/audit/verdicts-G.md`

## Resolution

All three checker weaknesses fixed, plus a latent bug in the hash independent
path that had gone unnoticed precisely because `--independent` never ran.

### cluster_formation.clj
`:valid?` is now `(and state-ok? node-count-consistent?)`:
- `state-ok?` requires an explicit `(= "ok" final-state)` — a `nil`/absent
  final cluster-state now FAILS (the old `(or (= "ok" ...) (nil? ...))` vacuous
  pass is gone).
- `node-count-consistent?` requires at least one observed CLUSTER NODES count,
  every count positive, and — for a stable (non-membership) run — every count
  equal to the configured cluster size `(count (:nodes test))`. Membership-change
  runs (`:membership-changes test`) vary the count by design, so there only the
  positivity requirement applies. New diagnostic keys: `:state-ok?`,
  `:node-count-consistent?`, `:expected-node-count`, `:node-counts-observed`.

### sortedset.clj
The previously-collected-but-discarded `latest-scores` map is now used: for
every member present in the final ZRANGE, its observed score must equal the last
tracked ZADD/ZADD-XX score for that member (`score-drift`), else the run FAILS.
Phantom members handle the never-written case; members removed then not re-added
are absent from the final read so are not cross-checked (sound). Soundness rests
on the final-read phase running after mutations quiesce and single-node
workloads running at concurrency `1n` (serial). New key: `:score-drift`.

### hash.clj + run.py (--independent wiring)
- `run.py` `TestDefinition` gained an `independent` field; `run_test` and the
  batch-EDN generator now emit `--independent`/`:independent true`. New
  `hash-independent` TestDefinition (workload `hash`, `--independent`,
  `--concurrency 10`, suites single/crash/all) exercises the linearizable
  independent-checker path that no invocation previously reached.
- **Latent bug fixed**: `jepsen.independent/tuple` is a `clojure.lang.MapEntry`
  and `tuple?` (which drives per-key subhistory grouping) only accepts MapEntry.
  The old `HashClient` replaced op `:value` with a plain vector `[field v]`, so
  the independent checker keyed on nothing and per-field subhistories collapsed —
  the path would have "passed" while checking nothing. Added a dedicated
  `IndependentHashClient` (mirroring register.clj) that returns real
  `independent/tuple` values. Also fixed the independent generator's frozen
  `(rand-int 100)` (same op forever) via `ind-write-op`, and added a
  tuple-valued `independent-final-read-generator` so the final-read phase does
  not feed the checker un-keyed bare reads.

### Validation
- Smoke runs on CLEAN topology (`--no-build`, short time-limit), all
  `:valid? true`:
  - `hash-independent` (single): PASS — results show 5 keyed subhistories
    `:f1`-`:f5`, each with `:linear {:valid? true}` (proves keying + checker run).
  - `sortedset` (single): PASS — `:score-drift nil`.
  - `cluster-formation` (raft): PASS — `:state-ok? true`,
    `:node-count-consistent? true`, `:node-counts-observed (3)`.
- Deliberate-fault sanity: `test/jepsen/frogdb/weak_checkers_test.clj` (14 tests,
  32 assertions, all green) feeds each strengthened checker synthetic bad
  histories proving the gates fire — nil/`"fail"` final-state fails; split/zero/
  absent node-counts fail; membership runs tolerate varying counts; score drift
  fails (phantom + ordering still fail); tuple-valued histories key per field
  while plain-vector histories collapse to no keys; a per-field register read of
  a never-written value is caught as non-linearizable.
- `lein check` clean (only pre-existing lag.clj reflection warnings); `ruff`
  clean.
