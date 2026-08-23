# Dead-watch off-target round-trip: close the WATCH false-negative window via generation-carry

Status: done

Size: M

> **Ruling (2026-08-22, R10 in
> [work-item rulings](../../../cluster-correctness/2026-08-22-work-item-rulings.md)):**
> file and fix with the [issue 09](done/09-routing-epoch-carry-and-watch-recheck.md)
> generation-carry treatment.

## Parent

[TR-TXN-028](../../../specs/txn.md#tr-txn-028--a-dead-watch-off-the-target-shard-gets-its-own-round-trip)
— the row itself stated the defect (since rewritten): "The off-target check is not atomic with the
target's commit: a write to an off-target watched slot landing between the two is not seen (a
bounded WATCH false-negative window that only dead watches can enter)."

## What is wrong

At EXEC, watches on shards other than the target (reachable only for `live_at_watch = false`
entries) get their own watch-only round-trips *before* the batch runs
(`frogdb-server/crates/txn/src/exec.rs:313-318`). If all report clean, the target's batch then
commits — but nothing re-checks the off-target keys at commit time. A write to an off-target
watched key that lands between its pre-batch check and the target's commit is invisible: the EXEC
commits even though a watched key changed after WATCH. That violates the WATCH contract's spirit
(optimistic CAS over the whole watch set as of commit) in a narrow but real window.

## What to build

Issue-09 treatment, applied to watch generations: the pre-batch off-target round-trip returns each
key's **generation** (version) rather than a bare clean/dirty verdict; the target shard's commit
step carries those generations and the off-target shards re-verify at commit… or, simpler and
matching issue 09's shape:

1. The off-target watch-only round-trip captures the observed generation per watched key.
2. The target's atomic commit step includes those captured generations in its CAS input — the
   commit aborts (`WatchAborted`, nil reply) if any carried generation no longer matches when the
   off-target shard confirms at commit time.
3. Exact protocol shape (second confirmation round vs. carrying generations into a single
   commit-time verification) follows the issue-09 precedent (routing-epoch carry + watch recheck);
   reuse its machinery where it exists.
4. Spec-first: TR-TXN-028's Postcondition rewritten — the false-negative-window sentence is
   deleted and replaced by the carry protocol; forcing test injects a write to the off-target
   watched key between the pre-batch check and the commit (concurrency-harness hook) and asserts
   `WatchAborted`.

## Acceptance criteria

- [x] TR-TXN-028 rewritten: no false-negative window stated; carry/verify protocol rowed;
      `just lint-spec` green
- [x] Forcing test: interleaved off-target write between pre-check and commit → EXEC aborts nil
- [x] Existing dead-watch tests (issue-25 family) stay green
- [x] `just mutants-diff frogdb-txn` triaged on touched code (run by the orchestrating session
      before push)

## Blocked by

None - can start immediately.

## Resolution

Shape chosen: **generation-carry into a single commit-time verification**, not a second
confirmation round. A confirmation round would be another pair of messages after the target has
already been asked to commit, and it still could not un-commit; the verification has to happen
*inside* the target's atomic step, which is exactly where the issue-09 `routing_fence` check
already lives.

- `frogdb-core`: `SlotVersions` keeps its per-slot stamps and the shard epoch in shared atomic
  cells, so the new `WatchFence` can hand a *reader* to another shard while the owning shard keeps
  bumping them. `WatchFenceRole` (`Mint` on the off-target probe, `Verify(fences)` on the batch;
  `Verify(vec![])` is the default) rides on `CoreMsg::ExecTransaction`, and
  `TransactionResult::WatchesFenced` carries the minted handles back. The worker verifies carried
  fences immediately after its own `watch_abort_reason` check and before the first queued command,
  aborting with `WatchAborted` + `reason="watched_slot_write"`.
- `frogdb-txn`: `run_shard_transaction` takes the role and returns `(results, fences)`, so both the
  probe and the batch stay one function and no arm is `unreachable!`. The off-target loop mints,
  the target's batch carries.
- The pre-batch probe is kept, not replaced: it fails fast on the shard that can name the abort
  reason, and it is what produces the generations. The carry is what makes its verdict hold to the
  commit.

Forcing tests (all tagged `FM-TXN-020`):

- `frogdb-txn` — `an_off_target_watch_written_after_its_check_still_aborts_the_commit` (the
  interleaved write, injected by `MockTxnHost` the instant the probe answers) and
  `an_untouched_off_target_watch_commits_on_the_carried_fence` (the liveness half).
- `frogdb-core` — `a_carried_watch_fence_that_moved_refuses_the_apply_before_any_command_runs`,
  `a_carried_watch_fence_that_still_reads_current_admits_the_apply`,
  `a_mint_probe_answers_fences_that_track_this_shards_writes`.
