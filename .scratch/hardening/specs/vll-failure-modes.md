# VLL (multi-shard locking) — failure modes

Status: LOCKED (2026-08-01) — Phase 1 mutation gate passed (frogdb-vll 100% vs 90% gate).
Behavior changes to this area are spec-first; see docs/agents/hardening-campaign.md
"Locked area rules".

Every way the shard-exclusive half of VLL can refuse a request, one table per mode. Same
contract as [Transactions — failure modes](txn-failure-modes.md): a mutant that survives is a
row nothing forces.

Scope: the per-shard state machine and the cross-shard coordinator in
`frogdb-server/crates/vll/src/{shard,coordinator}.rs` — specifically the **continuation lock**
(the shard-exclusive lock a cross-shard Lua script / MULTI takes) and its interaction with the
SCA scatter queue. Rows stop at what the shard reports over `ShardReadyResult`; the mapping to
the client's reply is named in `Observable` and lives in
`frogdb-server/crates/server/src/scatter/executor.rs` (`scatter_error_to_response`) and
`frogdb-server/crates/server/src/connection/scripting/eval.rs`
(`continuation_error_to_response`).

One caller-side obligation is in scope even though it lives outside those two files: the
shard worker must pair every `dequeue_for_execution` with a `release_after_execution`
(`frogdb-server/crates/core/src/shard/vll.rs`). The lock table cannot enforce this from
the inside — a caller that never returns leaves the entry held forever — so the way that
pairing survives an abnormal exit is rowed here ([FM-VLL-005](#fm-vll-005--a-granted-op-panics-while-executing)).

Not yet rowed: the scatter phases themselves (lock-request dispatch failure, phase-2/3 partial
failure unwind, gather timeouts). Those paths have tests — `phase1_dispatch_failure_aborts_shards_already_holding_intents`,
`phase3_failure_aborts_remaining_holders_not_positions`,
`phase2_failure_aborts_real_shard_ids_for_sparse_participants` — but their observable is a
generic `-ERR VLL lock acquisition failed`; the modes they distinguish are internal
(which shard is unwound), so they are left unspecced pending the phase-1 spec pass.

## How to read a row

Fields as in [txn-failure-modes.md](txn-failure-modes.md#how-to-read-a-row). `Outcome variant`
here names the `ShardReadyResult` the shard sends and the coordinator error it becomes.

---

## FM-VLL-001 — SCA request refused while a continuation lock is held

| Field | Value |
|---|---|
| Trigger | A scatter (SCA) lock request reaches a shard whose continuation lock is held. The owner routes its own work through `ScriptSubCommand`, never SCA, so such a request is always from a different connection. |
| Observable | The shard replies `Failed(ShardBusy)` immediately; the coordinator aborts every participant and the client sees `-BUSY shard busy with continuation lock; retry`. |
| NOT observable | The request being queued behind the lock — it would interleave with the owner's sub-commands and break isolation; the request being granted; queue depth or the lock table changing. |
| Invariant | `enqueue_lock_request` short-circuits on `continuation_held_or_pending()` before `ensure_initialized`, so no intent is declared and `queue_depth()` is unchanged — the rejection leaves nothing to clean up. The *pending* half of that predicate is [FM-VLL-004](#fm-vll-004--sca-request-refused-while-a-continuation-request-is-parked). |
| Outcome variant | `ShardReadyResult::Failed(VllError::ShardBusy)` → `ScatterError::LockFailed` |
| Forced by | `sca_lock_request_rejected_while_continuation_held` |
| Bug refs | none |

## FM-VLL-002 — continuation lock refused while another connection holds one

| Field | Value |
|---|---|
| Trigger | A second continuation-lock request (cross-shard Lua / MULTI) reaches a shard that already holds one for a different transaction, or that already has one parked waiting for its queue to drain ([FM-VLL-003](#fm-vll-003--continuation-lock-requested-while-the-shard-queue-has-not-drained)). |
| Observable | `Failed(ShardBusy)` from that shard; the client sees `-ERR lock acquisition failed: Shard busy with continuation lock`. The caller's primary work never runs. |
| NOT observable | Two owners of the same shard; a second request queueing behind the first (two parked requests would race for the same drain); the losing caller's already-granted locks on *other* shards staying held (nothing else would ever release them); `run` being invoked after a failed acquisition. |
| Invariant | The shard takes or parks a request only from `continuation_lock == None && pending_continuation == None` — one continuation claim per shard at a time, held or pending. Coordinator-side, every failure path drops `release_txs`, which fires the release signal for each shard that did grant; `acquire_continuation_and_run` owns the guard for the whole run, so the release happens whether the work returns or panics. |
| Outcome variant | `ShardReadyResult::Failed(VllError::ShardBusy)` → `ContinuationError::LockFailed` |
| Forced by | `continuation_lock_blocks_second_acquire`, `second_continuation_request_refused_while_one_is_parked`, `acquire_continuation_releases_partially_acquired_on_failure`, `acquire_continuation_and_run_skips_run_and_releases_on_failure` |
| Bug refs | none |

## FM-VLL-003 — continuation lock requested while the shard queue has not drained

| Field | Value |
|---|---|
| Trigger | A continuation-lock request reaches a shard that is not drained — its VLL queue still holds SCA ops, or an op it dequeued is still executing. The shard can only be taken exclusively once both are clear. |
| Observable | Drained shard (queue never used, or already drained): the lock is granted synchronously. Otherwise the request is **parked** — the shard replies nothing yet, keeps processing messages, and grants the lock from the drain point (the last queued op's `release_after_execution`, or its `abort`). If the shard is still not drained `CONTINUATION_DRAIN_TIMEOUT` (2 s) after parking, the host's continuation-event arm fires and the request gets `Failed(LockTimeout)` — the client sees `-ERR lock acquisition failed: VLL lock acquisition timeout`. |
| NOT observable | The requesting call blocking, sleeping, or polling — it is synchronous and returns to the shard event loop immediately, so the queue it waits on can still drain (see the liveness note); the lock granted over ops still in the queue or over a dequeued op still executing (such an op releases its own locks, so the "exclusive" owner would not be alone); a lock, a parked request, or a stored release receiver left behind after the timeout — `continuation_lock_owner()` and `continuation_lock_snapshot()` must both stay empty and the shard must accept SCA work again, or every later SCA request on that shard would be refused by FM-VLL-001/FM-VLL-004 forever. |
| Invariant | `request_continuation_lock` is not `async`: it grants when `is_drained()` (queue empty *and* no dequeued op outstanding), else stores a `PendingContinuation` carrying the caller's `ready_tx`/`release_rx` and a `deadline`. Grants happen only at that call and at the drain points, and only through `grant_continuation`, which installs the lock and the release receiver together — and installs neither if the requester has already given up (`ready_tx` closed). The deadline is served by `next_continuation_event`, the single host-loop arm that also serves the release signal. |
| Outcome variant | `ShardReadyResult::Failed(VllError::LockTimeout)` → `ContinuationError::LockFailed` |
| Forced by | `continuation_lock_parks_then_grants_when_the_queue_drains`, `continuation_lock_times_out_when_the_queue_never_drains`, `continuation_lock_acquires_once_queue_drains`, `continuation_lock_parks_while_a_dequeued_op_is_still_executing`, `parked_continuation_deadline_survives_cancellation`, `continuation_request_does_not_stall_the_shard_event_loop` |
| Bug refs | [issue 02](../issues/02-continuation-drain-wait-blocks-event-loop.md) |

> **Liveness note.** The drain wait used to run *inside* the shard's own event loop
> (`acquire_continuation_lock` awaited from `handle_vll_continuation_lock`), and the queue only
> drains when that loop processes a `VllExecute` — so a queue that was non-empty on entry could
> never drain during the wait: the outcome was always `LockTimeout` after the full 2 s, during
> which the shard processed nothing. Parking inverts the dependency: the request returns to the
> event loop, the loop drains the queue, and the drain grants the lock. The 2 s deadline is now a
> real timeout (only reached when the queue genuinely does not drain), and it stays below the
> coordinator's `DEFAULT_LOCK_ACQUISITION_TIMEOUT` (4 s) so the shard, not the coordinator,
> resolves the request and cleans up after it.

## FM-VLL-004 — SCA request refused while a continuation request is parked

| Field | Value |
|---|---|
| Trigger | A scatter (SCA) lock request reaches a shard that has a continuation-lock request parked waiting for its queue to drain ([FM-VLL-003](#fm-vll-003--continuation-lock-requested-while-the-shard-queue-has-not-drained)). |
| Observable | Same as [FM-VLL-001](#fm-vll-001--sca-request-refused-while-a-continuation-lock-is-held): `Failed(ShardBusy)` immediately, `enqueue_failed`, and the client sees `-BUSY shard busy with continuation lock; retry`. Once the parked request is granted or times out, the shard accepts SCA work again. |
| NOT observable | The request being enqueued behind the parked continuation — new arrivals would keep the queue non-empty and starve the drain until the deadline, turning a transient wait into a guaranteed `LockTimeout`; queue depth or the lock table changing. |
| Invariant | Parking raises a drain barrier: the queue can only shrink while a request is parked, so the drain terminates (each queued op either executes or is aborted by its own coordinator) or the deadline fires. The refusal shares FM-VLL-001's short-circuit, so it declares no intent and leaves nothing to clean up. |
| Outcome variant | `ShardReadyResult::Failed(VllError::ShardBusy)` → `ScatterError::LockFailed` |
| Forced by | `sca_lock_request_refused_while_a_continuation_request_is_parked` |
| Bug refs | [issue 02](../issues/02-continuation-drain-wait-blocks-event-loop.md) |

---

## FM-VLL-005 — a granted op panics while executing

| Field | Value |
|---|---|
| Trigger | An op that already holds its locks panics inside `execute_scatter_part` — an `assert!`, an index, an overflow, anywhere below the shard boundary including inside a dependency. Round-2 issue 63 (`FT.SEARCH … LIMIT 0 0` reaching tantivy's `assert_ne!(limit, 0)`) was one such panic, reachable before authentication. |
| Observable | The op's coordinator receives an error-shaped `PartialResult` for this shard — the same per-op shape [FM-VLL-001](#fm-vll-001--sca-request-refused-while-a-continuation-lock-is-held) uses, so the merge recognizes it — carrying `ERR internal error` and no data. `frogdb_shard_panics_isolated_total{site="vll_execute"}` increments and one `error!` names the shard, the op and the panic message. The shard answers the next message normally. |
| NOT observable | The worker task dying (the supervisor turns that into `process::abort()`, so one op would take the node down); the op's key intents surviving in the lock table, or `executing_ops` staying incremented — either leaks the locks to a command that no longer exists and blocks every later request on those keys plus any parked continuation lock forever; the panic message reaching the client; a truncated result folding into the merge as success. |
| Invariant | `dequeue_for_execution` and `release_after_execution` stay paired across an unwind: the panic is caught inside `handle_vll_execute` (`frogdb-server/crates/core/src/shard/vll.rs`), so the release runs on the panic path exactly as on the success path. Isolation is a structural backstop, never a licence to leave the panicking arithmetic unfixed — a non-zero counter is always a bug. |
| Outcome variant | n/a — the locks were already granted; the failure is in execution, not acquisition. |
| Forced by | `a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving` |
| Bug refs | [campaign-2 issue 07](../../hardening-2/issues/done/07-no-panic-isolation-at-the-shard-boundary.md), [round-2 issue 63](../../testing-improvements-round2/issues/done/63-ft-search-limit-0-0-panics-the-shard-worker.md) |
