# 06 — `ReplicaReplicationHandler` must honor its shutdown watch

Status: needs-triage

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

- [ ] Reconnect loop selects on the shutdown watch; `stop()` terminates it without abort
- [ ] Role Promotion of a boot-spawned Replica stops the reconnect loop (test: promote, assert no further connection attempts to the old Primary)
- [ ] Dead `shutdown_subsystems` save of the taken `replica_handler` deleted
- [ ] Existing replication integration tests stay green

## Blocked by

None - can start immediately

## Source

Round-8 P05 agent report (pre-existing gap, unchanged by the RoleManager work).
