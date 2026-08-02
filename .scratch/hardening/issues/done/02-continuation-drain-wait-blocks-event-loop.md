# Continuation-lock drain wait blocks the shard event loop it depends on

Status: done
Type: bug (liveness)
Severity: likelihood 3/3 (any continuation-lock request against a busy shard), consequence 2/3
(2s shard stall, no data loss) — score 6
Area: vll / core shard

## Problem

`VllShardState::acquire_continuation_lock` waits for the transaction queue to drain, but it is
awaited from inside the shard's own event loop (`handle_vll_continuation_lock`,
`frogdb-server/crates/core/src/shard/vll.rs:75`). The queue only drains when that same loop
processes `VllExecute` messages — which it cannot do while the handler is awaiting. Consequence:

- A queue that is non-empty on entry can NEVER drain during the wait; the outcome is always
  `LockTimeout` after the full `CONTINUATION_DRAIN_TIMEOUT` (2s).
- For those 2 seconds the shard processes no messages at all — a per-shard stall visible to
  every client touching that shard.
- The drain loop only ever succeeds for an already-empty queue, so the wait buys nothing.

Discovered during Phase 1d mutation gap-fill; current behavior pinned by FM-VLL-003
(`.scratch/hardening/specs/vll-failure-modes.md`) with a liveness note.

## Candidate fixes

1. Fail fast: non-empty queue → immediate `LockTimeout`-equivalent refusal (same client-visible
   error, no stall). Smallest change; relies on caller retry.
2. Drain-aware: park the lock request and let the event loop complete queued work, granting the
   lock when the queue empties (correct liveness, more machinery).

## Also noticed (minor)

`enqueue_lock_request` returns `queue_depth_warning: Some(depth)` unconditionally on
`QueueFull`/enqueue-error paths, ignoring `QUEUE_DEPTH_WARN_THRESHOLD` — misleading if
`max_queue_depth` is configured below the warn threshold.

## Resolution

Candidate 2 (drain-aware parking). Candidate 1 was rejected on caller evidence: the only
upstream caller is the cross-shard script path (`execute_cross_shard_script` →
`VllCoordinator::acquire_continuation_and_run`), and neither it nor `scatter` retries a
`ContinuationError::LockFailed` — the error goes straight to the client as
`-ERR lock acquisition failed: ...`. Failing fast would have converted a transient condition
(a shard with queued SCA work) into a client-visible hard error on every collision. This also
matches DragonflyDB, whose shard fiber never blocks: a global/exclusive transaction is enqueued
on the shard's `TxQueue` and gains exclusivity by queue position, with the shard loop granting
it once earlier work finishes (`EngineShard::PollExecution`, `continuation_trans_`). The one
deliberate divergence is that FrogDB keeps a bounded 2 s deadline instead of DragonflyDB's
unbounded wait, because FrogDB's coordinator does not retry and has its own 4 s timeout above.

Changes:

- `VllShardState::request_continuation_lock` replaces `acquire_continuation_lock` and is
  **synchronous**: it grants when the shard `is_drained()`, otherwise stores a
  `PendingContinuation` (ready/release channels plus an absolute deadline) and returns.
  `handle_vll_continuation_lock` no longer awaits, so the shard event loop keeps running.
- `is_drained()` is queue-empty **and** `executing_ops == 0`, a new counter incremented in
  `dequeue_for_execution` and decremented in `release_after_execution`; a dequeued-but-executing
  op releases its own locks, so granting over it would not be exclusive.
- Grants happen at the drain points (`release_after_execution`, `abort`) via
  `try_grant_pending_continuation`, and only through `grant_continuation`, which installs the
  lock and the release receiver together — and installs neither if the requester already gave up.
- `next_continuation_event() -> ContinuationEvent` replaces `await_continuation_release`, serving
  both the release signal and the parked deadline from the single host-loop `select!` arm (only
  one arm may borrow `self.vll` mutably). Cancel-safe in both roles: the deadline is absolute.
- Parking raises a drain barrier — `enqueue_lock_request` short-circuits with `ShardBusy` while a
  request is parked (new FM-VLL-004), so the queue can only shrink and the drain terminates.
- `queue_depth_warning` is now computed once as
  `(queue_depth >= QUEUE_DEPTH_WARN_THRESHOLD).then_some(queue_depth)` and used on every return
  path, fixing the minor wart.

Spec: FM-VLL-003 rewritten to the parking behavior with the liveness note in past tense,
FM-VLL-001/002 extended for the parked claim, FM-VLL-004 added.
