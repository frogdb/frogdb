# 04 — Flaky: tcl_sharded_pubsub_multi_exec_with_write_operation_on_replica (READONLY vs MOVED)

Status: needs-triage
Type: AFK (investigation)
Origin: phase-4b task 9 (observed failing, stash-verified pre-existing); did NOT reproduce in task-17 or post-merge verification runs 2026-07-21

## What to build

Root-cause the intermittent failure: the redis-regression test `tcl_sharded_pubsub_multi_exec_with_write_operation_on_replica` sometimes gets `-READONLY` where it expects `-MOVED` for a write inside MULTI/EXEC on a replica. Likely an ordering question in the error-precedence check (replica read-only check vs cluster slot-ownership check) that surfaces only under some timing/state — determine which precedence Redis/Valkey mandate (Redis returns MOVED for non-owned slots even on replicas; READONLY only for owned-slot writes without READWRITE) and whether FrogDB's check order is timing-dependent.

## Acceptance criteria

- [ ] Failure reproduced deterministically (loop/seed) or precedence bug found by inspection with a pinned regression test
- [ ] Error precedence documented and matches Redis behavior
- [ ] Test stable across ≥20 consecutive runs

## Blocked by

None - can start immediately.
