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
| Invariant | `enqueue_lock_request` short-circuits on `continuation_lock.is_some()` before `ensure_initialized`, so no intent is declared and `queue_depth()` is unchanged — the rejection leaves nothing to clean up. |
| Outcome variant | `ShardReadyResult::Failed(VllError::ShardBusy)` → `ScatterError::LockFailed` |
| Forced by | `sca_lock_request_rejected_while_continuation_held` |
| Bug refs | none |

## FM-VLL-002 — continuation lock refused while another connection holds one

| Field | Value |
|---|---|
| Trigger | A second continuation-lock request (cross-shard Lua / MULTI) reaches a shard that already holds one for a different transaction. |
| Observable | `Failed(ShardBusy)` from that shard; the client sees `-ERR lock acquisition failed: Shard busy with continuation lock`. The caller's primary work never runs. |
| NOT observable | Two owners of the same shard; the losing caller's already-granted locks on *other* shards staying held (nothing else would ever release them); `run` being invoked after a failed acquisition. |
| Invariant | The shard takes the lock only from `continuation_lock == None`. Coordinator-side, every failure path drops `release_txs`, which fires the release signal for each shard that did grant; `acquire_continuation_and_run` owns the guard for the whole run, so the release happens whether the work returns or panics. |
| Outcome variant | `ShardReadyResult::Failed(VllError::ShardBusy)` → `ContinuationError::LockFailed` |
| Forced by | `continuation_lock_blocks_second_acquire`, `acquire_continuation_releases_partially_acquired_on_failure`, `acquire_continuation_and_run_skips_run_and_releases_on_failure` |
| Bug refs | none |

## FM-VLL-003 — continuation lock requested while the shard queue has not drained

| Field | Value |
|---|---|
| Trigger | A continuation-lock request reaches a shard whose VLL queue still holds SCA ops. The shard can only be taken exclusively once the queue is empty, so the request waits for it to drain. |
| Observable | If the queue is empty (never used, or already drained) the lock is granted. Otherwise, after `CONTINUATION_DRAIN_TIMEOUT` (2 s), `Failed(LockTimeout)` — the client sees `-ERR lock acquisition failed: VLL lock acquisition timeout`. |
| NOT observable | The lock granted over ops still in the queue (a queued op executes and releases its own locks, so the "exclusive" owner would not be alone); a lock, or a stored release receiver, left behind after the timeout — `continuation_lock_owner()` and `continuation_lock_snapshot()` must both stay empty, or every later SCA request on that shard would be refused by FM-VLL-001 forever. |
| Invariant | The drain wait is a `tokio::time::timeout` around a poll loop on `queue.is_empty()`; the lock and the release receiver are installed only after the wait reports drained. |
| Outcome variant | `ShardReadyResult::Failed(VllError::LockTimeout)` → `ContinuationError::LockFailed` |
| Forced by | `continuation_lock_times_out_while_queue_is_not_empty`, `continuation_lock_acquires_once_queue_drains` |
| Bug refs | none |

> **Liveness note.** `acquire_continuation_lock` is awaited from the shard's own event loop
> (`handle_vll_continuation_lock`), and the queue only drains when that loop processes a
> `VllExecute`. A queue that is non-empty on entry therefore cannot drain while the wait runs:
> the outcome is always `LockTimeout`, and the shard processes no messages at all for the full
> 2 s. The drain loop as written can only succeed for a queue that was already empty. Fixing
> this means either failing fast or draining from the event loop instead of inside the
> handler; the rows above pin today's behavior so the change is visible when it is made.
