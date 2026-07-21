# 02 — S4: assert `-ERR shard busy` for a third conn DURING held continuation lock

Status: needs-triage
Type: AFK
Origin: phase-4b task-11 review (coverage gap)

## What to build

`frogdb-server/crates/core/tests/shard_driver/scenario_s4.rs` currently proves the shard resumes after a panicking continuation-lock holder releases (post-release SET succeeds). It never covers the gate's positive branch: a third connection's `Execute` dispatched *while* the locks are held should receive `Response::error("ERR shard busy...")` (`can_execute_during_lock`, `worker.rs`; wired in `dispatch_core.rs`).

## Acceptance criteria

- [ ] While both shards' continuation locks are held, a third conn's `Execute` observes the shard-busy error (assert on the reply, not on lock introspection alone)
- [ ] Existing S4 panic/release/resume assertions unchanged and green

## Blocked by

None - can start immediately.
