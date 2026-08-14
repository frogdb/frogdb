# 13: Blocking wait becomes a run-loop state — parked clients see EOF and CLIENT KILL

Status: ready-for-agent

## Origin

Distsys-review CRIT-4 + CRIT-5
(`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`), ruled by the user
2026-08-14: structural fix, one bundled issue
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`handle_blocking_wait` is awaited **inline inside the frame branch** of the connection
run-loop `select!` (`connection.rs:609-700`, `process_one_command(frame).await` at
`:649`), so for the entire duration of a blocking wait the task polls only the
coordinator's three-branch select (response / unblock / deadline,
`connection/blocking/coordinator.rs:93-110`). Two critical consequences:

**CRIT-4 — disconnect-while-parked is undetectable, and a later push is silently lost.**
The socket is not polled during the wait, so EOF cannot be observed; and `cleanup_wait`
calls `state.end_block()` *before* returning, so by the time the loop re-enters
`select!`, `blocked_shard()` is `None` and the blocking branch of
`notify_connection_closed` (`lifecycle.rs:206-215`) is dead code on this path —
TR-BLOCKING-013's postcondition is unreachable. A client parked in `BLPOP jobs 0` whose
process dies leaks the task, the FD, the `WaitEntry`, and the per-key/global waiter
budgets forever. When a producer later runs `LPUSH jobs x`, `drive_satisfaction_body`
pops the element and `response_tx.send(reply)` **succeeds** (the receiver is alive inside
the orphaned task), so the `Err(_)` restore arm never fires: the element is removed from
the store and written to a dead socket — a silently lost write, precisely what
FM-BLOCKING-005 declares NOT observable.

**CRIT-5 — CLIENT KILL cannot terminate a parked client.** `killed()` is select branch 1
of the run loop (`connection.rs:611-614`) — not polled during the wait — and the
coordinator select has no kill branch; `kill_by_id` (`client_registry/mod.rs:709-717`)
sends only `kill_tx`, never `unblock_tx`. The command reports success and does nothing,
forever. Combined with CRIT-4 the operator has no way to reclaim leaked parked
connections (`CLIENT UNBLOCK` needs each id and does not close the connection); a node
cannot be cleanly drained. `specs/blocking.md` has no CLIENT KILL row at all — the
"every blocked state is leavable" argument rests solely on UNBLOCK.

Mature-system shape: in Redis/Valkey/Dragonfly a blocked client is a *state on a
still-readable connection*, never a suspended read loop — `readQueryFromClient` sees EOF
and `freeClient` → `unblockClient` cleans up in the same tick, and `CLIENT KILL` (the
documented node-drain primitive, incl. `LADDR`) unblocks before freeing. FrogDB's
inline-await structure is the root architectural divergence and the single cause of both
findings.

## What to build (spec-first)

1. Spec rows first:
   - Amend TR-BLOCKING-013: the postcondition (parked connection close →
     `UnregisterWait`) becomes reachable via the restructure; until the code lands the
     row carries a `(current code)` cell stating the leak.
   - New TR row: CLIENT KILL against a blocked client — kill terminates the wait,
     unregisters the waiter, releases budgets, closes the connection (Redis parity).
   - New FM row(s): mid-wait disconnect → later push stays in the store (no
     consumed-by-nobody delivery); kill-while-parked → connection actually terminates.
2. **Restructure**: the blocking wait becomes a run-loop state rather than an inline
   await — the connection task returns to its `select!` while parked, keeping the
   socket, `killed()`, and the wait-completion future all polled concurrently.
   `end_block()`/cleanup ordering redone so `notify_connection_closed`'s blocking branch
   is live on every exit path.
3. Forcing tests:
   - CRIT-4: connect, `BLPOP k 0`, close the socket, `LPUSH k v` from another
     connection, assert `LLEN k == 1` and the wait queue is empty (`DEBUG WAITQUEUE`).
   - CRIT-5: connect, `BLPOP k 0`, `CLIENT KILL ID <n>` from another connection, assert
     the killed connection closes, the waiter is unregistered, and budgets are released.
4. Conservation checkers: add a mid-wait-disconnect generator so this class is covered
   systematically, not just by the two forcing tests.

## Interaction notes

- Ordering vs spec-gaps [issue 08](08-blocking-command-rows.md): 08's H5 ruling
  (`-ERR shard unavailable` on `Err(_)`) and the MAJ-11 amendment (drops stop signaling)
  reshape the same coordinator; implement together or sequence 08 → 13.
- The restructure touches the connection run loop — re-run the blocking family plus
  pipeline-ordering tests (Invariant 10, connection ordering).

## Acceptance criteria

- [ ] Spec rows added/amended; `just lint-spec` green
- [ ] Wait is a run-loop state; socket + kill polled while parked
- [ ] Both forcing tests fail on the pre-fix tree, pass post-fix
- [ ] Mid-wait-disconnect generator in the conservation checkers
- [ ] Blocking + pipeline-ordering suites green

## Blocked by

None — can start immediately (sequence with issue 08 preferred, see notes).
