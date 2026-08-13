# No seam to stop the periodic syncer, so FM-PERSISTENCE-003's headline NOT-observable is unwitnessed

Status: ready-for-agent
Type: gap (test seam missing) — a specified NOT-observable that no test can force
Severity: likelihood 2/3 (a syncer thread that dies leaves `periodic` silently degraded to
`async`), consequence 3/3 (acknowledged writes older than the interval are lost while `INFO`
still reports `durability_mode:periodic`) — score 6
Area: core / persistence durability

## Problem

[FM-PERSISTENCE-003](../../../../specs/persistence.md) names this
NOT-observable:

> Loss of a write older than the interval, i.e. an unbounded window: the periodic sync thread
> silently stopping would turn `periodic` into `async` while `INFO` still reports
> `durability_mode:periodic`.

Nothing witnesses it. The three tests in the row's `Forced by` cell
(`test_periodic_mode_loss_bounded_by_flush_interval`, `test_periodic_mode_after_interval`,
`test_periodic_mode_within_window`) all witness the *positive* half of the contract — writes
older than the interval survive a crash, writes inside the window may not. None of them can
witness the negative half, because there is no way to make the syncer stop.

`spawn_periodic_sync` owns a `tokio` task with no handle returned to the caller and no injectable
failure: a test can neither abort it nor make `RocksStore::durable_sync` fail, so the "syncer
silently stopped" state is unreachable from a test. `W3a` strengthened the three existing witnesses
against the real crash primitive but could not close this sub-claim, and it was left honestly
uncovered rather than papered over with a test that asserts something adjacent.

The campaign-2 PRD already names this as a residual tooling gap (§3.3, "a seam to kill the periodic
syncer").

## Fix

Give `spawn_periodic_sync` a seam. Two candidates, in preference order:

1. **Return the `JoinHandle`** (or an owning guard) from `spawn_periodic_sync` so a test can
   `abort()` the syncer mid-run, write past an interval, crash, and assert the loss is *unbounded*
   — i.e. that a write acknowledged several intervals before the crash is gone. This is the
   direct witness the row asks for, and the handle is useful for graceful shutdown besides.
2. **A fallible `durable_sync` seam** so the syncer can be made to fail every tick without being
   killed, witnessing the same degradation plus whatever the sync path is supposed to log/count
   when it cannot sync.

Either way, the follow-on work is a test that:

- boots `periodic` mode, writes and lets one interval elapse (baseline durable),
- stops the syncer,
- writes a second batch and lets **several** intervals elapse,
- crashes and reopens,
- asserts the second batch is gone — proving the window really was unbounded once the syncer
  stopped, so the assertion fails the day the syncer is made resilient (which is the point).

Then move this reference out of FM-PERSISTENCE-003's `Bug refs` and into its `Forced by`.
