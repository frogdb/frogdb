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

`frogdb-server/crates/server/src/connection/guards.rs` — `run_pre_checks` defers the
replica read-only rejection to the cluster slot check when the keyed write targets a
slot **owned by another node** (helper `write_defers_to_cluster_redirect`: cluster mode
on + command not cluster-exempt + has keys + `get_slot_owner(slot) == Some(other)`).
Such writes then get `-MOVED`/`-CROSSSLOT`/`-ASK` from `validate_cluster_slots`, matching
Redis. If this replica owns the slot, the slot is unassigned, or it's a keyless /
standalone-replication write, the deferral does **not** fire and `-READONLY` still wins.
This makes the reply **deterministic** (independent of the async role-flag timing) for
the common case while keeping the safe answer for the pathological one.

The deferral is deliberately **ownership-aware** rather than assuming the invariant "a
replica never owns the slot for its keys". See "Adversarial review" — FrogDB auto-assigns
slots to bootstrapping nodes, so a slot-owning replica is reachable; the ownership check
makes the fix self-contained (no dependency on a topology-level invariant).

## Verification

- Deterministic regression test added:
  `frogdb-server/crates/redis-regression/tests/cluster_sharded_pubsub_tcl.rs` →
  `tcl_sharded_pubsub_multi_exec_write_on_replica_moved_beats_readonly`. It waits for
  `INFO replication` to report `role:slave` (read-only flag applied) **before** the
  queued write, forcing the previously-losing race, then asserts `-MOVED` + `-EXECABORT`.
- Fix-reverted probe: with the deferral disabled, the new test fails deterministically
  with `got READONLY You can't write against a read only replica.` — confirming the
  root cause and that the test guards it.
- Stability: original flaky test + new regression test **25/25 consecutive green**;
  full `cluster_sharded_pubsub_tcl` suite (7 tests) **10/10 consecutive green, 0 retries**.
  `frogdb-server` guards/dispatch/replica/replication tests (192) green.
  `cargo clippy -p frogdb-server --all-targets` clean.

## Adversarial review (round 2) — ownership-aware deferral

Review found the first cut of the fix relied on a false invariant, "a replica never owns
the slot for its keys", and would make the pathological case **worse**:

- `CLUSTER REPLICATE` (`SetRole{Replica}`) flips only role/primary_id, never
  `slot_assignment`, and FrogDB **auto-assigns slots to bootstrapping nodes**
  (`cluster_init.rs`, gated by the lowest-node-id bootstrap heuristic). So a node that
  self-assigned slots and later becomes a replica is a **reachable** slot-owning replica.
  Confirmed empirically: the very harness `start_cluster_with_replicas` produces one
  (a `wait_for_node_slotless` probe times out — the joined node keeps its slots).
- A blanket "defer any keyed write on a replica" would then route `LocalServe` and
  **execute** the write on the read-only replica (silent divergence) — strictly worse
  than the old unconditional `-READONLY` (an accidental safety net).

**Closure (self-contained, no topology change):** the deferral now checks committed slot
ownership (`ClusterState::get_slot_owner`) and defers **only** when the slot is owned by a
*different* node — exactly when `validate_cluster_slots` will emit `-MOVED`. A slot-owning
replica (or unassigned slot) falls through to `-READONLY`, so a replica can never execute
a keyed write locally. No dependency on any demotion-time invariant.

**Rejected alternative — topology guard.** An earlier attempt rejected demoting a
slot-owning node in `ClusterCommand::SetRole` apply
(`ClusterError::NodeHasAssignedSlots`, Redis `clusterCommand` parity). It is correct for
Redis (nodes start empty) but **breaks FrogDB**, whose freshly-joined nodes self-bootstrap
slots — it rejected the harness's (and real deployments') legitimate replica creation
(all replica sharded-pubsub tests timed out on role propagation). Reverted. The proper
place to fix that mismatch is a dedicated cluster-formation issue (a node joining an
existing cluster should not self-assign slots, or demotion should atomically shed empty
slots to the new primary) — see follow-up note below. The graceful `Failover` command
already transfers slots off the demoted node atomically, so that path was never at risk.

**Follow-up (new issue recommended):** FrogDB's lowest-id bootstrap heuristic lets a
dynamically-added node self-assign slots, producing slot-owning replicas after
`CLUSTER REPLICATE`. Harmless to this flake now that the read path is ownership-aware, but
it is a latent correctness/writes-to-owned-slot concern worth hardening at the
formation/topology layer.

**Residual (future hardening):** a slot MIGRATING import-target that is simultaneously a
replica with `ASKING` set could `AcceptImporting` a local write; exotic, out of scope,
noted in the `write_defers_to_cluster_redirect` doc comment context.

**Harness check (point 4)**: `start_cluster_with_replicas` demotes only freshly
`add_node`'d peers, which do CLUSTER MEET only and own zero slots — so the guard
does not break the sharded-pubsub tcl setup (verified: `add_node` assigns no
slots; sharded-pubsub tests stay green).

**Residual (future hardening, not addressed)**: a slot MIGRATING import-target
that is simultaneously a replica could `AcceptImporting` a local write. Exotic
and out of scope for this flake; noted in the `write_defers_to_cluster_redirect`
doc comment.

## Acceptance criteria

- [x] Failure reproduced deterministically (loop/seed) or precedence bug found by inspection with a pinned regression test
- [x] Error precedence documented and matches Redis behavior
- [x] Test stable across ≥20 consecutive runs (25/25)

## Blocked by

None - can start immediately.
