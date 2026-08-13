# A client that disconnects while blocked leaks the connection, the shard waiter and a maxclients slot forever

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/03 F3 · MASTER.md §3 (availability / resource)
Score: severity 4 · likelihood 4 · effort 3 · priority 17
Area: frogdb-server / connection blocking; frogdb-core shard blocking

## Context

`BLPOP key 0` is the standard queue-consumer idiom, and the consumer process being killed or
its network dropping is an everyday event. When it happens, nothing notices: the blocking
coordinator races only three futures and has no socket/EOF input, the shard never GCs a waiter
registered with `deadline: None`, and the acceptor's connection-count decrement never runs.
`blocked_clients` and `connected_clients` both drift upward with no operator remedy, and the
server eventually hits `maxclients` and refuses all new clients until restart. The ghost waiter
is also a data-loss corollary: it can consume a value a live client should have received.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`connection/blocking/coordinator.rs` races exactly three futures (`response_rx`,
`unblock.unblocked()`, deadline) with `biased` — there is no socket/EOF input. The connection's
main `tokio::select!` in `connection.rs:556` (which *would* see the read half close) is not running
while `process_one_command` awaits the blocking handler. Shard side:
`connection/blocking.rs:44` sets `deadline = (timeout > 0.0).then(...)`, so `BLPOP k 0` registers
`deadline: None`, and `core/src/shard/blocking.rs` only expires waiters via
`entry.deadline.is_some_and(|d| d <= Instant::now())` — a `None` deadline is never GC'd. The
acceptor's `current_connections.fetch_sub` (`acceptor.rs:353`) is a plain statement after
`handler.run().await`, so it never runs either.

Proposal 03's cross-area note assigns the waiter-GC half to `frogdb-core`
(`core/src/shard/blocking.rs` never expires a `deadline: None` waiter).

## What to fix

1. Add an EOF/socket-closed arm to the blocking coordinator's `tokio::select!` so a dropped
   connection unblocks the command.
2. Make the shard GC waiters whose owning connection is gone, independent of `deadline` —
   a `None` deadline must not mean "never collectable".
3. Ensure `current_connections.fetch_sub` (`acceptor.rs:353`) runs on every exit path
   (guard/`Drop`), not only on a normal return from `handler.run().await`.
4. Confirm `blocked_clients` and `connected_clients` in `INFO` return to their baseline.

## Acceptance criteria

- [ ] A server integration test connects, issues `BLPOP k 0`, calls
      `wait_for_blocked_clients(1)` (`test-harness/src/server.rs:819`), drops the socket, and
      asserts `blocked_clients` and `connected_clients` return to 0 within a bound. Fails today.
- [ ] After the drop, a second client `LPUSH k v` and the pushed element is **still there** —
      not consumed by the ghost waiter. Fails today.
- [ ] Repeating the cycle N times does not move `connected_clients` off its baseline, i.e. the
      maxclients slot is not leaked.

## Test boundary

Level 4 (server integration over RESP) — the trigger is a real socket closing, which no lower
level can produce; the harness already has `wait_for_blocked_clients`. Not level 3: the
`shard_driver` has no connection layer and therefore cannot express the disconnect at all.

## Depends on

nothing

## Re-triage 2026-08-06

**Verdict: still-valid**

All three legs reproduce on today's tree. (1) `BlockingWaitCoordinator::wait_for_response`
(`frogdb-server/crates/server/src/connection/blocking/coordinator.rs:93-110`) still races exactly
`response_rx` / `unblock.unblocked()` / the deadline under `biased` — there is no socket/EOF arm.
(2) `WaitQueue::collect_expired`
(`frogdb-server/crates/core/src/shard/wait_queue.rs:297-308`) skips any entry whose `deadline` is
`None`, and `connection/blocking.rs:48` (was cited as `:44`) still sets
`let deadline = (timeout > 0.0).then(...)`, so `BLPOP k 0` registers a permanently uncollectable
waiter. (3) `acceptor.rs:353` is still a bare `current_connections.fetch_sub(...)` statement
after `handler.run().await` (`acceptor.rs:348-354`) — no guard, no `Drop`. The Phase-2/4 hardening
did not touch this: `specs/blocking.md` has no disconnect row
(FM-BLOCKING-004 is *response channel closed*, a different trigger), and `rg` finds no
disconnect-while-blocked test anywhere under `frogdb-server/crates/*/tests/`.
