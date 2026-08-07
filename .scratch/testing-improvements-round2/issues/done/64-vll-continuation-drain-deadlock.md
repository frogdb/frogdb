# VLL continuation-lock drain deadlocks the shard for 2 s and then always fails

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/12 F5 · MASTER.md §3 (availability / resource)
Score: severity 4 · likelihood 3 · effort 3 · priority 15
Area: frogdb-vll / frogdb-core shard event loop

## Context

Acquiring a VLL continuation lock waits for the shard's transaction queue to drain — but it
waits *inline* on the shard's own event loop, so the messages that would drain the queue can
never be processed. Every client on that shard hangs for `CONTINUATION_DRAIN_TIMEOUT` (2000 ms)
and the acquisition then returns `LockTimeout` regardless, so cross-shard scripts cannot
succeed whenever any SCA op is queued. The queue is non-empty for the whole duration of any
scatter, which is normal traffic on the enabling config.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`crates/vll/src/shard.rs:247-262` — the drain loop polls
`self.tx_queue.as_ref().map(|q| !q.is_empty())` and `tokio::time::sleep(CONTINUATION_DRAIN_POLL)`
until `CONTINUATION_DRAIN_TIMEOUT` (2000 ms), then sends
`ShardReadyResult::Failed(VllError::LockTimeout)`. It is awaited from
`core/src/shard/vll.rs::handle_vll_continuation_lock`, which is awaited from
`dispatch_message(msg).await` **inline** in the `tokio::select!` message arm of
`core/src/shard/event_loop.rs`. Queue entries are only removed by `VllExecute`/`VllAbort`,
which are messages — so the loop is waiting for progress it is itself blocking.

Why nothing catches it: coverage confirms nobody has ever run it — `shard.rs:252` (the loop
closure) is `untested`, exec=0, and `queue.rs:129 is_empty` is `untested`. The 7 inline
`shard.rs` tests never enqueue an SCA op before requesting a continuation lock, and
`scenario_s4` starts from an empty queue.

## What to fix

1. Take continuation acquisition off the inline `dispatch_message(msg).await` path, or make the
   drain condition edge-triggered so the event loop keeps servicing `VllExecute`/`VllAbort`
   while the acquisition is pending. Proposal 12's cross-area note records this as an
   architecture decision for the `crates/core` owner, not a test-only change.
2. Ensure a failed acquisition still returns promptly and does not hold the shard.
3. Cover `queue.rs:129 is_empty` and the drain closure at `shard.rs:252` with the new test so
   they stop reading as `untested`.

## Acceptance criteria

- [ ] A `core/tests/shard_driver/` scenario enqueues a scatter lock request on shard 0 (left
      Ready, un-executed), then requests a continuation lock on shard 0 from another connection,
      and asserts the acquisition resolves in well under `CONTINUATION_DRAIN_TIMEOUT`. Fails
      today.
- [ ] The same scenario asserts an unrelated `PING`/`GET` on that shard is still answered while
      the acquisition is pending — i.e. the shard is not blocked. Fails today.
- [ ] `shard.rs:252` and `queue.rs:129` are executed by the new test.

## Test boundary

Level 3 (`core/tests/shard_driver/`) — the bug *is* the real shard event loop blocking, so the
test needs a real `ShardWorker` and a real coordinator; scenario S3 already builds exactly that.
Not level 4: the socket adds nothing, and the hang is observable at the driver seam.

## Depends on

nothing — proposal 12 records that `shard_driver` already builds the real `ShardWorker`s and
coordinator this scenario needs

## Re-triage 2026-08-06

**Verdict: fixed**

Fixed by commit **`3044970b`** ("fix(vll): park continuation-lock requests instead of blocking the
shard event loop"), tracked as hardening
`.scratch/hardening/issues/done/02-continuation-drain-wait-blocks-event-loop.md` and specced as
**FM-VLL-003** (with **FM-VLL-004** for the SCA-refusal side); both rows carry this as their
`Bug refs`. The polling loop is gone: `request_continuation_lock`
(`frogdb-server/crates/vll/src/shard.rs:301-325`) is **no longer `async`** — it grants immediately
when `is_drained()` (`:333-335`: queue empty *and* `executing_ops == 0`), otherwise stores a
`PendingContinuation { …, deadline: Instant::now() + CONTINUATION_DRAIN_TIMEOUT }` and returns to
the event loop, which then drains the queue and grants from the drain point. The 2 s constant
survives only as a real timeout served by the host's `next_continuation_event` arm. The spec's
liveness note (`vll-failure-modes.md:70-78`) describes exactly this issue's mechanism as the
*former* behaviour. Both acceptance criteria are discharged by
`continuation_request_does_not_stall_the_shard_event_loop`
(`frogdb-server/crates/core/src/shard/vll.rs:127`) plus
`continuation_lock_parks_then_grants_when_the_queue_drains` (`vll/src/shard.rs:682`),
`continuation_lock_times_out_when_the_queue_never_drains` (`:722`),
`continuation_lock_parks_while_a_dequeued_op_is_still_executing`,
`parked_continuation_deadline_survives_cancellation` and
`sca_lock_request_refused_while_a_continuation_request_is_parked`. `frogdb-vll` is a Phase-1
locked crate at a 0.90 mutation gate.
