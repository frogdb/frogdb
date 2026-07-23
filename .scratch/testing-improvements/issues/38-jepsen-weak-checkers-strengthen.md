# Jepsen: three single-node checkers are weaker than the fault coverage they gate

Status: needs-triage
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
