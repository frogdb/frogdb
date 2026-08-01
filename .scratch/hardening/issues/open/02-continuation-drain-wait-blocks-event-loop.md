# Continuation-lock drain wait blocks the shard event loop it depends on

Status: needs-triage
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
