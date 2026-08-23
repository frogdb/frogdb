# A panic in a VLL write path escalates to process fail-stop

Status: done

Size: S

> **Ruling (2026-08-22, R7 in
> [work-item rulings](../../../cluster-correctness/2026-08-22-work-item-rulings.md)):**
> issue 07 item 4's "restart the shard from its WAL" is **re-ruled to process
> fail-stop**. CockroachDB, Scylla, Redis, and FoundationDB all treat a mid-write panic
> as process-fatal — none hot-restarts a shard. A bespoke single-shard hot-restart is a
> large new correctness surface (in-flight cross-shard scatter parts, sibling-held
> locks, client visibility during replay) bought for a rare event that
> replication/failover already covers. Existing startup recovery replays the WALs; that
> is the restart-from-clean-state mechanism, and it already exists.

## Parent

[issue 07](07-vll-continuation-package.md) — the fourth item of its ruling
("Panic in a VLL write path → restart the shard from WAL (clean state) rather than trusting the
catch_unwind boundary; extend the NOT-observable with 'partial writes surviving a panic'"), carved
out because it was the one item of that package with no mechanism to build on. The re-ruling above
replaces the single-shard-restart mechanism with process exit; the correctness goal (never serve
from possibly-broken structures after a write-path panic) is unchanged.

## What is wrong

[FM-VLL-005](../../../specs/vll.md#fm-vll-005--a-granted-op-panics-while-executing) pins that a
panic inside `execute_scatter_part` is caught at the shard boundary, that the dequeue/release
pairing survives the unwind, and that the shard answers the next message normally. What it does
*not* say is what happened to the data the panicking op was halfway through mutating. Continuing to
serve after a *write-path* panic means continuing against structures whose invariants may have been
broken mid-mutation, and for a cross-shard op it means sibling shards may hold writes this shard
never completed.

## What to build

1. A panic caught in a VLL *write* path (`handle_vll_execute` over a write op) escalates to a
   clean process exit (orderly shutdown path, exit code distinguishable from a crash) instead of
   resuming on the existing state. Startup recovery replays the WALs — no new mechanism.
2. Read paths keep today's isolate-and-continue behavior — there is nothing half-mutated to clean
   up.
3. Extend FM-VLL-005 to state the write/read split and add "partial writes surviving a panic" to
   its NOT observable — trivially forceable now: panic mid-`execute_scatter_part` on a write op,
   assert the process exits (or, in-harness, that the escalation hook fires) and that after
   restart-recovery no partial write is readable.

## Acceptance criteria

- A panic in a VLL write path triggers process fail-stop; a panic in a read path does not
  (both directions forced)
- FM-VLL-005 updated with the write/read split and the "partial writes surviving a panic"
  NOT observable, forcing test named; `just lint-spec` green
- TR-VLL-020's `Pending` field removed
- `just mutants-diff frogdb-vll` triaged on touched code

## Blocked by

None - can start immediately.

## Outcome

Built as ruled. The escalation lives in `frogdb-vll` so the forcing tests count toward that
crate's mutation gate:

- `frogdb-server/crates/vll/src/fail_stop.rs` (new) — `FailStopSink` (injectable),
  `ProcessExitFailStop` (production: stderr line, then `std::process::exit(70)` =
  `EX_SOFTWARE`, distinct from 101/134/0), `WritePathPanic`, `PanicEscalation`.
- `VllShardState::release_after_panic` decides the split from the `LockMode` the op declared at
  enqueue (`VllPendingOp`/`DequeuedOp` now carry `mode`): read → `Isolated`, write → fires the
  sink → `FailStop`. Every `VllShardState` installs `ProcessExitFailStop` by default, so unwired
  code fails closed; a test that deliberately panics a write op opts out with
  `set_fail_stop_sink`.
- `handle_vll_execute`'s panic arm (`frogdb-core`) calls it after the existing lock release.
- No new recovery mechanism: startup WAL replay is the restart path, as ruled.

Forcing tests (all tagged `// FM-VLL-005`):

- `a_write_path_panic_escalates_to_process_fail_stop` (frogdb-vll, `shard.rs`)
- `a_read_path_panic_is_isolated_without_fail_stop` (frogdb-vll, `shard.rs`)
- `a_dequeued_op_carries_the_mode_it_declared` (frogdb-vll, `shard.rs`) — pins the input the
  split reads
- `a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving` (frogdb-core,
  `panic_guard.rs`) — extended to inject a recording sink and assert the escalation fired once

Spec: FM-VLL-005 states the write/read split, exit code, and the new "partial writes surviving a
panic" NOT observable; TR-VLL-020 restated as built and its `Pending` field removed.
`ProcessExitFailStop::fail_stop` is excluded in `.cargo/mutants.toml` (a mutant of a body whose
real effect is exiting the test process is unobservable by construction; the *decision* to call
it is what the forcing tests pin).
