# Jepsen: 5 registered cluster workloads never run + membership_routing checker doesn't assert durability

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: jepsen

## Context

`core.clj:95-100` registers, and `core.clj:143-147` routes, five Jepsen workloads —
`migration-recovery`, `concurrent-migration`, `partition-recovery`, `membership-routing`,
`rolling-restart` — but none of them appear in `run.py`'s `TESTS` (`run.py:92-295`). These are
plausibly the highest-value scenarios in the harness (migration under failure, membership changes,
rolling restarts) and they never execute in practice. Git history shows these were added
deliberately: commit `d7c60603` "implement 6 new Jepsen workloads from gap analysis plan" — the
intent was to close coverage gaps, but the workloads were never wired into the run matrix and there
is no parked/flaky/WIP comment marking them as intentionally disabled. This is an undocumented
orphan state, not a deliberate deferral.

Compounding this, `membership_routing.clj` itself has a checker gap even if it were wired up: its
docstring (lines 10, 257-259) promises "no acked-write loss" across membership changes, and it does
compute the relevant data (`written-keys`/`reads-with-nil` at lines 268, 273) — but `:valid?`
(line 287) only checks `(and node-added? migration-completed?)` and never actually asserts the
durability property the docstring claims. A run that silently loses acked writes during a
membership change would still report `:valid? true`. Separately, redirect exceptions in this
workload are downgraded to `:info` severity (lines 169-171) and are never thresholded, so an
unbounded volume of redirect failures produces no signal at all.

## What to build

- Add `run.py` `TestDefinition`s for all five orphaned workloads (`migration-recovery`,
  `concurrent-migration`, `partition-recovery`, `membership-routing`, `rolling-restart`), or — for
  any that turn out to be genuinely not ready — explicitly delete them and their registration
  rather than leaving them silently unreachable.
- Fix `membership_routing.clj`'s `:valid?` to actually assert the durability property it documents:
  every acknowledged write present in the reads (or explicitly accounted for), not just
  `node-added? and migration-completed?`.
- Add a threshold on the `:info`-downgraded redirect count (currently unbounded and unthresholded)
  so pathological redirect volume fails the run instead of being silently absorbed.

## Acceptance criteria

- [ ] `migration-recovery`, `concurrent-migration`, `partition-recovery`, `membership-routing`,
      `rolling-restart` each either run via a `run.py` `TestDefinition` or are deleted with a
      documented reason
- [ ] `membership_routing.clj` `:valid?` asserts acked-write durability across membership changes
      (not just node-added/migration-completed), matching its own docstring
- [ ] Redirect-exception count in `membership_routing.clj` has an explicit pass/fail threshold
- [ ] Each wired workload run at least once with results attached to this issue

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/core.clj:95-100,143-147` — workload registration/routing
- `testing/jepsen/run.py:92-295` — TESTS matrix (orphaned workloads absent)
- `testing/jepsen/frogdb/src/jepsen/frogdb/membership_routing.clj:10,257-259` (docstring),
  `:268,273` (computed-not-asserted data), `:287` (`:valid?`), `:169-171` (unthresholded `:info`
  downgrade)
- Git: commit `d7c60603` "implement 6 new Jepsen workloads from gap analysis plan"
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `orphaned-cluster-workloads`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same)
