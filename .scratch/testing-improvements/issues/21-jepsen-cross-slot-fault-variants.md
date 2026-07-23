# Cross-slot Elle workload never runs under fault injection

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`cross_slot.clj:382` implements a rigorous conservation checker for cross-slot multi-key operations (the same pattern recommended for reuse in fixing the slot-migration checker, see task 08). However `run.py:201-208` only ever runs it with nemesis `none` — no partition, no kill, no migration-under-fault variant. This means the strongest cross-slot correctness checker the harness has never actually observes a fault while it's checking, so any cross-slot atomicity regression that only manifests under partition/kill (the realistic failure mode for a VLL cross-shard commit) is invisible to this workload.

## What to build

- Add `cross-slot-partition` `TestDefinition`: `cross_slot.clj` workload composed with a partition nemesis, in the raft/cluster suite.
- Add `cross-slot-kill` `TestDefinition`: `cross_slot.clj` workload composed with a process-kill nemesis.
- Ensure both run against a multi-shard cluster topology so cross-slot ops actually span shards under the fault.

## Acceptance criteria

- [ ] `run.py` `TESTS` table includes `cross-slot-partition` (cross_slot workload + partition nemesis).
- [ ] `run.py` `TESTS` table includes `cross-slot-kill` (cross_slot workload + kill nemesis).
- [ ] Both run successfully (or with a triaged, understood failure) in a manual harness invocation before merging.
- [ ] Both are reachable via the same suite grouping mechanism as the existing `cross-slot` (none) entry.

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/cross_slot.clj:382` (conservation checker)
- `testing/jepsen/frogdb/run.py:201-208` (current `cross-slot` entry, nemesis `none` only)
