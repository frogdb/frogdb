# 06 — `ReplicaReplicationHandler` must honor its shutdown watch

Status: ready-for-human

## What to build

`ReplicaReplicationHandler::start()` ignores its own `shutdown` watch channel: `handler.stop()`
cannot break the reconnect loop, and only task `abort()` actually stops it. Runtime streams
(round-8 `RealReplicaStream::drop`) abort correctly, but promoting a *boot*-spawned Replica
stops `consume_frames` via the role flag while the boot reconnect loop keeps dialing the old
Primary forever. Also: `Server::shutdown_subsystems` "saves" `self.replica_handler` state that
`start_subsystems` already `take()`d — dead code.

End-to-end: `REPLICAOF NO ONE` on a boot-configured Replica fully stops replication activity
(no reconnect attempts observable), and clean server shutdown contains no dead save path.

## Acceptance criteria

- [x] Reconnect loop selects on the shutdown watch; `stop()` terminates it without abort
- [x] Role Promotion of a boot-spawned Replica stops the reconnect loop (test: promote, assert no further connection attempts to the old Primary)
- [x] Dead `shutdown_subsystems` save of the taken `replica_handler` deleted
- [x] Existing replication integration tests stay green

## Blocked by

None - can start immediately

## Source

Round-8 P05 agent report (pre-existing gap, unchanged by the RoleManager work).

## Comments (2026-07-20)

Implemented in two commits.

**`frogdb-replication` crate (`4ee29527`)** — `ReplicaReplicationHandler::start()` now
`tokio::select!`s (biased, shutdown branch first) on its own `shutdown` watch at both places the
loop can block: the in-flight `connect_and_sync()` future, and the backoff `sleep` between
attempts. `stop()` breaks the loop directly; no `task.abort()` needed. A pre-start `stop()` is
also honored via a `borrow()` check before the loop starts.

While writing the "stop before start" test, found and fixed a real race in `stop()` itself:
`ReplicaReplicationHandler::new()` discards its constructor-time watch receiver, so between
construction and `start()`'s first `.subscribe()` the channel has zero receivers. Plain
`watch::Sender::send()` is a silent no-op with zero receivers (confirmed against tokio 1.50
source) — the value is never stored, so a `stop()` racing ahead of `start()` was previously lost
entirely. Switched `stop()` to `send_replace()`, which unconditionally stores the value
regardless of receiver count, so a subscriber that shows up later still observes it (both via the
stored value and any subsequent `changed()`).

**`frogdb-server` crate (`3cc919f0`)** — `RoleManager` gained a `boot_replica_handler:
Option<Arc<ReplicaReplicationHandler>>` field and a `register_boot_replica_handler(handler,
primary)` method (also exposed on `RoleManagerHandle`). `Server::start_subsystems` calls it
immediately after taking `self.replica_handler`/`self.replica_frame_rx` and before spawning the
acceptor, so no `REPLICAOF` can race ahead of the registration. `promote()` and `demote()` both
`.take()` and `.stop()` the registered handler alongside their existing `stream = None` teardown,
so a Role Promotion (or a runtime demote to a different primary) also halts the boot reconnect
loop. Deliberately did not route boot construction through `RealReplicaStreamer` — that path
always full-resyncs from a fresh `ReplicationState`, which would throw away the boot handler's
partial-resync capability built from persisted state.

Also deleted the dead `shutdown_subsystems` block that tried to `save_state()` on
`self.replica_handler`: `start_subsystems` always `take()`s it first, so the field is `None` by
shutdown and the block never ran. Left a comment noting there is currently no replacement
persistence hook for replica state (unlike the primary handler's pre-snapshot hook) — a clean
restart resumes from the last-recovered offset, not the exact shutdown offset. That gap is
pre-existing and out of scope for this fix.

Test evidence:
- `frogdb-replication` — `test_stop_terminates_reconnect_loop_without_abort` and
  `test_stop_before_start_prevents_any_connection_attempt` (new), plus full crate suite:
  136/136 passed.
- `frogdb-server` — `role_manager::tests::promote_stops_registered_boot_replica_handler` (new,
  covers acceptance criterion 2 directly: registers a boot handler pointed at an
  always-refusing `ConnectFactory`, lets it retry, promotes, asserts the task terminates without
  abort and no further connection attempts occur), plus all 6 pre-existing `role_manager` tests:
  7/7 passed.
- `frogdb-server` `tests/integration_replication.rs`: 133/133 passed (criterion 4).
- `just check` (full workspace, all-targets): clean.
