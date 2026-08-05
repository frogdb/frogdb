# `WAIT` counts a replica that is still installing its full-resync checkpoint

Status: needs-triage
Type: bug (durability claim)
Severity: likelihood 2/3 (every full resync has this window; it widens with dataset size),
consequence 3/3 (`WAIT` is the only durability handshake a client has, and it reports acknowledgement
for a node that answers `nil` for the same key) — score 6
Area: replication / WAIT / primary session lifecycle

## Problem

Found while repairing issue 19's decorative tests. Between the moment a primary finishes *sending* a
full-resync payload and the moment the replica finishes *installing* it, the primary reports that
replica as caught up and `WAIT` counts it:

- primary: `slave0:ip=…,state=online,offset=<master_repl_offset>` and `WAIT 1 500` -> `1`
- the same replica, at the same instant: `master_link_status:down`, `master_repl_offset:0`, and
  `GET wait_test_key` -> nil

So `WAIT n t` returning `n` does not imply the write is readable on `n` replicas, which is the only
thing a client calls `WAIT` to learn. This is the FM-REPLICATION-001 failure shape ("a replica
reported ready while holding none of the primary's data") seen from the primary's side, and it
directly contradicts FM-REPLICATION-043's NOT-observable clause, which forbids a `slaveN:` line for
a replica that is not streaming.

## Suspected mechanism (needs confirmation before a fix is designed)

Two candidates, not mutually exclusive, both in
`frogdb-server/crates/replication/src/replica_session.rs`:

1. `ReplicaSession::start_streaming` sets `Phase::Streaming` on its first line
   (`replica_session.rs:1001`) — i.e. when the *primary* starts forwarding, which is a fact about the
   sender, not about the receiver. `get_streaming_replicas()` and therefore both INFO renderers,
   `connected_slaves`, and `WaitCoordinator::count_acked` all key on that phase.
2. `ReplicaSession::seed_acked_position` (`replica_session.rs:489`) advances the same monotonic
   `acked_offset` the replica's own `REPLCONF ACK` advances, using the checkpoint's
   `snapshot_offset`. Its doc comment is explicit that this is "a *primary* bookkeeping fact … not a
   replica acknowledgement read off the wire" — but `count_acked` cannot tell the two apart, so a
   position the replica never acknowledged is counted as one it did.

Candidate 2 is the sharper one: the seed exists so lag accounting and `+CONTINUE` resume points are
right, and those are legitimate uses. `WAIT`'s question ("how many replicas have durably taken this
offset") is a different question about the same number.

## Candidate remedy shapes

- Keep the seed, but have `count_acked` require a real ACK: track the seeded position separately
  from the wire-acked one, and let `WAIT` count only the latter. Costs an extra atomic per session;
  keeps every other consumer unchanged.
- Or delay `Phase::Streaming` until the replica's first post-install `REPLCONF ACK` lands, which
  also fixes the `slaveN:`/`connected_slaves` lie — but that changes when a replica becomes visible
  in INFO at all, which is issue 21's territory and should be decided with it.

Whichever is chosen, the seeded value must not vanish: `+CONTINUE` resume and the lag clock depend
on it.

## Forcing test (already written, deliberately unasserted)

`test_wait_during_replica_resync` in
`frogdb-server/crates/server/tests/integration_replication.rs` reproduces the window and carries a
comment naming the exact claim it declines to assert. Turning that comment into
`assert!(count == 0 || replica_serves(key))` is the acceptance test.

Also wanted:

- `wait_does_not_count_a_seeded_position_that_was_never_acked` — unit level, at the
  `ReplicaSession`/`WaitCoordinator` seam.

## It already fails a suite test (2026-08-05)

The window is no longer only reachable through the deliberately-unasserted probe: it fails
`integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence`
under whole-suite parallel load, at ~0.5 s into the test (so not a timeout — the wait is
*satisfied*, wrongly):

```
assertion `left == right` failed: keyspace sizes should match before the stall
  left: Integer(1)
 right: Integer(0)
```

That test waits for `connected_slaves:1`, writes one seed key, polls `WAIT 1 1000` until it
returns 1, and then compares `DBSIZE` on both nodes. `WAIT` returns 1 while the replica's
keyspace is still empty — the durability claim this issue is about, in an assertion that is
already in the suite.

Sequence: `start_streaming` publishes `Phase::Streaming` at its top (candidate 1), the test's
`wait_connected_slaves` sees it, the seed `SET` lands and advances the live offset, and then
`seed_replica_position` (candidate 2) credits the session's `acked_offset` with the backlog tail
— which now covers that `SET` — before the replica has decoded, let alone applied, any of it.
The persistence case is the one that fails because its full resync goes through the checkpoint
branch (drain → `spawn_blocking(create_checkpoint)` → file transfer), which holds the window open
far longer than the diskless branch.

Reproduced 3/3 on whole-suite runs, including at `6cfdb026` with no local changes, so it is not a
regression from any in-flight work; it passes when run in isolation, where the seed lands before
the test's `SET`. Distinct from issue 01 (that one was frame loss on the receive→stream handoff
and failed the *earlier* `seed write should be acked by the healthy replica` assertion; it is
fixed).

Whichever remedy is chosen above, this assertion is the second acceptance test.

## Spec impact

FM-REPLICATION-037's NOT-observable half gains the case: a `WAIT` count that includes a replica
which has not acknowledged the offset from the wire. FM-REPLICATION-043's "no `slaveN:` line for a
replica that is not streaming" needs a note that the *definition* of streaming is currently
sender-side, which is what lets this through.
