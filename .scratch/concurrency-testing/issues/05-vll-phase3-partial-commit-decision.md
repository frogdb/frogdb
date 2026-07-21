# 05 — Design decision: durable partial commit on VLL phase-3 dispatch failure

Status: ready-for-human
Type: HITL
Origin: phase-4b task-10 review (controller flag; documented design, not a bug)

## Decision needed

`VllCoordinator` deliberately does NOT abort shards that already received `VllExecute` when a later shard's phase-3 dispatch fails: it aborts only `shard_ids[idx..]` (`frogdb-server/crates/vll/src/coordinator.rs:266-288`, pinned by `phase3_failure_aborts_remaining_holders_not_positions` and integration-pinned two-sided in `crates/core/tests/shard_driver/scenario_s3.rs`). Consequence: a cross-shard VLL-EXEC can land as a **durable partial commit** — executed shards keep their writes, failed/undispatched shards roll back.

Rationale for the current design: post-lock-acquisition VLL determinism — participants that already executed release their own locks on completion and must not be aborted. But a phase-3 dispatch failure models shard-infra failure, which a deterministic engine would typically resolve by log-replay **re-execution** of the failed participant, not by abandoning atomicity.

## Prior art (researched 2026-07-21)

- **Redis:** EXEC has no rollback by documented design — a runtime error in one queued command does not undo the others. Partial application on error is the accepted contract.
- **DragonflyDB** ([transaction.md](https://github.com/dragonflydb/dragonfly/blob/main/docs/transaction.md), [transactions blog](https://www.dragonflydb.io/blog/transactions-in-dragonfly)): structurally sidesteps this decision.
  - Pre-execution scheduling failure (ordering conflict): "the coordinator reverts all shards and retries with a new TxId" — abort-all before anything applies (FrogDB's phase-1/2 handling matches).
  - Post-schedule: "Once scheduled, a transaction never rolls back or retries." No undo machinery. And **no shard-infra failure model**: shards are fibers in one process, dispatch is an in-process queue push that cannot fail while the process lives; thread death = process death. FrogDB's phase-3 window (`send_execute` Err = shard worker's channel closed while the node keeps serving) has no Dragonfly analogue.
  - Atomicity is even opt-out: `multi_exec_mode` non_atomic mode carries "No atomicity guarantee" as a perf feature.
  - Replica side: they DID invest — v0.13.0 "feat(replica): atomicity for multi shard commands" ([PR #598](https://github.com/dragonflydb/dragonfly/pull/598)) keeps per-shard async journal flows from exposing partial multi-shard txns on replicas. **Directly relevant to FrogDB phase 6 replication.**
- Takeaway: no engine in this space rolls back applied transaction writes; they keep the *failure model* narrow instead. That is the strongest argument for options 1/4 and against option 3.

## Options

1. **Accept as designed** — document the partial-commit window as a stated consistency property of cross-shard EXEC under infra failure (client-visible caveat). Industry-consistent (see prior art). Cost ≈ one paragraph; S3 already pins the behavior. Risk: becomes a real product behavior the moment phase 6 makes shard-level failure survivable.
2. **Re-execution path** — persist the decided VLL batch (intent record before phase-3 dispatch) and replay the failed shard's portion on recovery. Restores durable atomicity, NOT live visibility (executed shards released locks; readers see partial state until recovery). Requires: coordinator commit-point persistence in the hot path, txn framing in per-shard WALs (pairs with the rollback-mode WAL partial-append deferral), recovery scanner + deterministic re-executor, idempotence for the phase-4 executed-but-unconfirmed window (response channel dropped/timeout after VllExecute delivered). Also forces an EXEC-ack semantics decision (block / error-but-applies-later / Calvin-style ack-on-log). Durability-phase scope.
   - **2b (cheaper variant): abort-on-recovery** — intent record + per-shard completion markers; recovery **discards** incomplete cross-shard txns instead of re-executing. Sound because the coordinator died before acking EXEC to the client. Skips the deterministic-replay machinery entirely; still needs txn framing + intent record. Only coherent combined with option 4 (node must not keep serving after a phase-3 failure, else discarding at recovery contradicts already-served reads).
3. **Abort-all with executed-shard compensation** — conflicts with the VLL lock-release invariant (executed participants released their own locks); no prior art does write-undo. Dead end, listed for completeness.
4. **Fail-stop (Dragonfly-equivalent)** — treat `send_execute` failure as node-fatal: a dead shard worker in a live node is an undefined-state node; escalate to shutdown instead of serving a partial commit. Clients never observe live partial state, matching Dragonfly's structural position. WAL recovery still resurrects the partial commit unless paired with 2b's txn framing — 4 + 2b together = full atomicity without live replay.

## Acceptance criteria

- [ ] Human decision recorded here (option + rationale)
- [ ] If option 1: consistency caveat documented in the appropriate CONTEXT.md/spec
- [ ] If option 2/2b/4: follow-up implementation issue(s) filed in the durability phase (txn framing, intent record, recovery policy; escalation path for option 4)
- [ ] Phase-6 planning note: replica-side multi-shard atomicity (Dragonfly PR #598 precedent) regardless of option chosen
