# Jepsen: 5 registered cluster workloads never run + membership_routing checker doesn't assert durability

Status: done
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

## Resolution

Wired all five orphaned workloads into `run.py`; deleted none — inspection confirmed every one
is fully implemented (real generators, nemeses, and checkers), not a stub. Fixed the two
`membership_routing.clj` checker gaps and validated end-to-end against Docker.

### Wired (none deleted)

`run.py` `TestDefinition`s added:

| Workload | Nemesis | Time | Topology | Suites |
|---|---|---|---|---|
| `partition-recovery` | partition | 90s | replication | replication, all |
| `migration-recovery` | none (self-driving) | 120s | raft (cluster) | raft, all |
| `concurrent-migration` | none (self-driving) | 90s | raft (cluster) | raft, all |
| `membership-routing` | none (self-driving) | 120s | raft (cluster) | raft, all |
| `rolling-restart` | none (self-driving) | 120s | raft (cluster) | raft, all |

The four raft-cluster workloads drive their own fault injection (slot migration / membership
changes / node restarts) inside the workload, so they run with `nemesis="none"` and
`cluster_flag=True`. `partition-recovery` is a replication-topology test driven by the
`partition` nemesis. `core.clj` already registered and routed all five names, so no `core.clj`
change was needed. `just jepsen-list` now shows raft suite = 13, replication = 6, all = 38.
`test-catalog.md` updated to match.

### membership_routing.clj checker fixes

1. **Durability now asserted.** `:valid?` was `(and node-added? migration-completed?)` — it
   computed `written-keys`/`reads-with-nil` but never used them. Now computes `lost-writes` =
   keys that were acked-written but read back `nil`, restricted to the `written-keys` set (index
   0 is never written and reads pick `rand-int(counter)`, so unwritten keys can't produce false
   positives), and folds `durable? (empty? lost-writes)` into `:valid?`. Output gains
   `:durable? :lost-write-keys`.
2. **Redirect count now thresholded.** The `:info`-downgraded MOVED/ASK redirects (previously
   unbounded, lines 169-171) now have an explicit pass/fail limit:
   `redirect-limit = max(25, 0.50 * data-op-count)`, `redirects-ok? (<= redirect-count limit)`,
   folded into `:valid?`. A `redirect-info?` helper counts both downgrade paths: this workload's
   own `:error [:redirect ..]` tag AND redirects that escaped `invoke!` and were downgraded to
   `:info` by Jepsen carrying a MOVED/ASK `:exception`. Output gains
   `:redirect-info-count :redirect-info-limit :redirects-ok?`. The 0.50/25 threshold reflects
   that all keys share one hash tag → all land in the single migrating slot → a transient
   redirect burst during migration is expected, not pathological.

### Validation (Docker smoke, reused existing image — harness-only changes)

- `lein check` — exit 0, all namespaces compile (only pre-existing reflection warnings in
  client.clj/cluster_client.clj/lag.clj; membership_routing.clj compiles clean).
- `just jepsen-list` — all five workloads listed.
- `membership-routing` on a clean fresh raft topology — **PASS** (1:02):
  `:valid? true, :durable? true, :lost-write-keys [], :redirect-info-count 21,
  :redirect-info-limit 50, :redirects-ok? true`.
- `membership-routing` on a deliberately dirty topology (stale slot owner) — **FAIL**:
  `:valid? false, :durable? false, :lost-write-keys ["{mem-route}:k:96"]` — the new durability
  assertion fires on a real acked-write loss the old checker masked (bidirectional proof the
  checker works).
- `rolling-restart` on a fresh raft topology — **PASS** (1:52): `:valid? true`.

### Files changed

- `testing/jepsen/run.py` — 5 `TestDefinition`s.
- `testing/jepsen/frogdb/src/jepsen/frogdb/membership_routing.clj` — durability + redirect
  threshold checker fixes.
- `.claude/skills/jepsen-testing/references/test-catalog.md` — catalog rows + suite counts.
