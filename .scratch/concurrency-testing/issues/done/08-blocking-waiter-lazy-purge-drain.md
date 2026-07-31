# 08 — Blocking-waiter `check_key` lazy purges drain one message late

Status: done
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

- [x] Blocking-waiter lazy purges **apply** their effects at the `drive_satisfaction` seam — no
      report survives into the next message. `drive_satisfaction` was split into a draining wrapper
      + `drive_satisfaction_body` (mirroring `execute_scatter_part`/`execute_scatter_part_body`):
      the wrapper calls `apply_lazy_purge_effects()` after the body's loop and its BLMove cascade
      fully unwind. One drain point covers all three `check_key` impls; the cascade recurses through
      the body, so effects drain exactly once per wake chain.
      (`frogdb-server/crates/core/src/shard/blocking.rs`, `try_satisfy_*` → `drive_satisfaction`)
- [x] A worker-level test pins it — `waiter_satisfaction_drains_lazy_purge_report`
      (`shard::blocking::tests`), mirroring `scatter_mget_drains_lazy_purge_report`: purge fired via
      the BLPOP waiter path (key absent, waiter still blocked), buffer empty afterwards, AND the
      parity version bump landed at this seam (only `apply_lazy_purge_effects` could bump — the
      waiter never reached `Done`).
- [x] Existing blocking/waiter suites and the lazy-expiry `regression_` pins stay green — full
      `frogdb-core` 796/796, turmoil pins 3/3 (`regression_watch_second_watcher_aborts_realpath`,
      `watch_lazy_expiry_false_negative_realpath`, `regression_watch_read_lazy_purge_aborts_realpath`),
      `just lint frogdb-core` clean.

## Decision: apply (not discard)

Applying effects at the seam is safe and parity-consistent (preferred option). Reentrancy analysis:
`apply_lazy_purge_effects` → `drain_stream_waiters_with_error` pops the wait queue, and the driver
mutates that same queue in its loop — but the drain runs from the **wrapper**, after
`drive_satisfaction_body` and every recursive BLMove-cascade frame have returned, so no waiter-queue
iteration is in flight when it fires. (The stream `DrainNoGroup` branch already drains its own key's
XREADGROUP waiters inside the loop; the post-loop `apply_lazy_purge_effects` re-drain is idempotent
and additionally supplies the version bump the in-loop path omits.)

## Blocked by

None — can start immediately.

## References

- Fix-stage plan: `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- Ledger: `.superpowers/sdd/lazy-expiry/progress.md` (final-review finding)
- Seam pattern to mirror: `execute_scatter_part` wrapper, `frogdb-server/crates/core/src/shard/execution.rs`
