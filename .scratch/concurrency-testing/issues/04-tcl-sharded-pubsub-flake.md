# 04 — Flaky: tcl_sharded_pubsub_multi_exec_with_write_operation_on_replica (READONLY vs MOVED)

Status: done
Type: AFK (investigation)
Origin: phase-4b task 9 (observed failing, stash-verified pre-existing); did NOT reproduce in task-17 or post-merge verification runs 2026-07-21
Resolution: root-cause bug found by inspection, fixed, pinned with a deterministic regression test

## What to build

Root-cause the intermittent failure: the redis-regression test `tcl_sharded_pubsub_multi_exec_with_write_operation_on_replica` sometimes gets `-READONLY` where it expects `-MOVED` for a write inside MULTI/EXEC on a replica. Likely an ordering question in the error-precedence check (replica read-only check vs cluster slot-ownership check) that surfaces only under some timing/state — determine which precedence Redis/Valkey mandate (Redis returns MOVED for non-owned slots even on replicas; READONLY only for owned-slot writes without READWRITE) and whether FrogDB's check order is timing-dependent.

## Root cause (confirmed)

An **error-precedence inversion** in the pre-dispatch gauntlet, made observable by a
role-flag timing race.

- FrogDB's replica read-only rejection lives in the `PreChecks` dispatch stage
  (`PreDispatchView::run_pre_checks`, `guards.rs`), which is stage **1** of
  `PRE_DISPATCH_ORDER` (`dispatch.rs`). The cluster slot-ownership check that
  produces `-MOVED` runs **later** — inside `TransactionQueue` (stage 5, via
  `try_queue_in_transaction` → `validate_cluster_slots`) on the MULTI path, and at
  `ClusterSlotValidation` (stage 13) on the direct path.
- So when the connection's replica read-only flag (`is_replica: AtomicBool`) is set,
  a keyed write on a cluster replica short-circuits with `-READONLY` at stage 1,
  **before** the slot check can issue the required `-MOVED`.

**Why it is timing-dependent.** The read-only flag is applied asynchronously, decoupled
from the cluster-state role the test harness waits on:
`CLUSTER REPLICATE` → Raft `SetRole{Replica}` commit → `ClusterStateMachine` emits a
`DemotionEvent` (`cluster/src/state.rs`) → demotion-event consumer calls
`RoleManagerHandle::request_demote` → `RoleManager::demote` stores `is_replica = true`
(`role_manager.rs`). The harness's `wait_for_role_propagation` only waits for the
*cluster-state* role to propagate (Raft-committed), not for this downstream flag store.
So a queued write races the flag:
- flag not yet set → `PreChecks` passes → slot check returns `-MOVED` ✓ (the common case)
- flag already set → `PreChecks` short-circuits with `-READONLY` ✗ (the flake)

The slot for `foo` is owned by the primary regardless of the flag (the same `SetRole`
reassigns slot ownership), so the *correct* answer is always `-MOVED`; only the stale
precedence leaks `-READONLY`.

## Redis-mandated precedence (verified)

Redis `processCommand` (`server.c`) runs the cluster redirection block
(`getNodeByQuery` → `-MOVED`/`-ASK`/`-CROSSSLOT`) **before** the read-only-replica
rejection (`server.masterhost && server.repl_slave_ro` → `-READONLY`), which in turn
runs before `queueMultiCommand`. Net precedence: **MOVED > READONLY > MULTI-queue**.
A keyed write to a cluster replica therefore always returns `-MOVED` (redirect to the
slot's primary), never `-READONLY`. `-READONLY` in cluster mode is reserved for
non-redirectable writes (keyless commands like FLUSHALL, which `getNodeByQuery` leaves
on `myself`).

## Fix

`frogdb-server/crates/server/src/connection/guards.rs` — `run_pre_checks` now defers the
replica read-only rejection to the cluster slot check when the write is slot-redirectable
(new helper `write_defers_to_cluster_redirect`: cluster mode on + command not
cluster-exempt + command has keys). Such writes then get `-MOVED`/`-CROSSSLOT`/`-ASK`
from `validate_cluster_slots`, matching Redis. Keyless writes and standalone-replication
writes are unchanged (still `-READONLY`). This makes the reply **deterministic** and
independent of the async role-flag timing.

## Verification

- Deterministic regression test added:
  `frogdb-server/crates/redis-regression/tests/cluster_sharded_pubsub_tcl.rs` →
  `tcl_sharded_pubsub_multi_exec_write_on_replica_moved_beats_readonly`. It waits for
  `INFO replication` to report `role:slave` (read-only flag applied) **before** the
  queued write, forcing the previously-losing race, then asserts `-MOVED` + `-EXECABORT`.
- Fix-reverted probe: with the deferral disabled, the new test fails deterministically
  with `got READONLY You can't write against a read only replica.` — confirming the
  root cause and that the test guards it.
- Stability: the original flaky test + the new regression test ran **25/25 consecutive
  green**. Full `cluster_sharded_pubsub_tcl` suite (7 tests) green. `frogdb-server`
  guards/dispatch/replica/replication tests (192) green. `cargo clippy -p frogdb-server`
  clean.

## Acceptance criteria

- [x] Failure reproduced deterministically (loop/seed) or precedence bug found by inspection with a pinned regression test
- [x] Error precedence documented and matches Redis behavior
- [x] Test stable across ≥20 consecutive runs (25/25)

## Blocked by

None - can start immediately.
