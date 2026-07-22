# 08 — Blocking-waiter `check_key` lazy purges drain one message late

Status: needs-triage
Type: AFK
Origin: lazy-expiry parity final whole-branch review (2026-07-21, merge `0b741829`) — LOW, non-blocking

## What to build

The three `WaiterSatisfaction::check_key` impls (`frogdb-server/crates/core/src/shard/blocking.rs`
:443, :626, :749) call `store.purge_if_expired(key)` directly, populating the `lazily_purged`
buffer, but `drive_satisfaction` never drains it. Because `try_satisfy_*_waiters` runs in the
post-execution pipeline (`shard/post_execution.rs:446-448`) *after* `execute_command_inner`'s
wrapper has already drained, a blocking-wake lazy purge leaves its report in the buffer until the
**next** message drains it — a deferred version bump + XREADGROUP drain (or a discard if the next
message is a WATCH).

Not a correctness regression: EXEC re-drains via `purge_expired_watches` before `check_watches`,
the effects are idempotent, and the extra bump stays inside the accepted coarse over-abort
envelope (fix-stage plan, design ruling 1). But the "every lazy purge drains at its own seam"
contract should hold uniformly — fix by draining (or explicitly discarding, with rationale) at
the `drive_satisfaction` seam, or by routing these purges through a draining wrapper.

## Acceptance criteria

- [ ] Blocking-waiter lazy purges apply (or explicitly discard, documented) their effects at the
      `drive_satisfaction` seam — no report survives into the next message
- [ ] A worker-level test pins it (mirror `scatter_mget_drains_lazy_purge_report`: purge fired
      via the waiter path AND buffer empty afterwards)
- [ ] Existing blocking/waiter suites and the lazy-expiry `regression_` pins stay green

## Blocked by

None — can start immediately.

## References

- Fix-stage plan: `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- Ledger: `.superpowers/sdd/lazy-expiry/progress.md` (final-review finding)
- Seam pattern to mirror: `execute_scatter_part` wrapper, `frogdb-server/crates/core/src/shard/execution.rs`
