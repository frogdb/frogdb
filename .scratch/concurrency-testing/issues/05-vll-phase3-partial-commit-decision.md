# 05 — Design decision: durable partial commit on VLL phase-3 dispatch failure

Status: ready-for-human
Type: HITL
Origin: phase-4b task-10 review (controller flag; documented design, not a bug)

## Decision needed

`VllCoordinator` deliberately does NOT abort shards that already received `VllExecute` when a later shard's phase-3 dispatch fails: it aborts only `shard_ids[idx..]` (`frogdb-server/crates/vll/src/coordinator.rs:266-288`, pinned by `phase3_failure_aborts_remaining_holders_not_positions` and integration-pinned two-sided in `crates/core/tests/shard_driver/scenario_s3.rs`). Consequence: a cross-shard VLL-EXEC can land as a **durable partial commit** — executed shards keep their writes, failed/undispatched shards roll back.

Rationale for the current design: post-lock-acquisition VLL determinism — participants that already executed release their own locks on completion and must not be aborted. But a phase-3 dispatch failure models shard-infra failure, which a deterministic engine would typically resolve by log-replay **re-execution** of the failed participant, not by abandoning atomicity.

## Options

1. **Accept as designed** — document the partial-commit window as a stated consistency property of cross-shard EXEC under infra failure (client-visible caveat).
2. **Re-execution path** — persist the decided VLL batch and replay the failed shard's portion on recovery (restores atomicity; durability-phase work, pairs with the rollback-mode WAL partial-append deferral noted in the 4b plan's Honest Scoping Notes).
3. **Abort-all with executed-shard compensation** — likely conflicts with the lock-release invariant; probably a dead end, listed for completeness.

## Acceptance criteria

- [ ] Human decision recorded here (option + rationale)
- [ ] If option 1: consistency caveat documented in the appropriate CONTEXT.md/spec
- [ ] If option 2: follow-up implementation issue(s) filed in the durability phase
