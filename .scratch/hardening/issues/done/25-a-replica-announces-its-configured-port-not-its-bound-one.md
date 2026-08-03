# A replica announces its configured port, not the one it bound

Status: done
Type: bug (observability)
Severity: likelihood 2/3 (every replica whose `server.port` is `0`, i.e. every OS-assigned
deployment and the whole integration test harness), consequence 2/3 (`INFO replication` and `ROLE`
on the primary report `port=0` for a replica that is perfectly reachable — the same unusable
address issue 16 removed, arriving from the other end of the handshake) — score 4
Area: replication / handshake

## Problem

Issue 16 fixed the *recording* half: the primary now stores whatever a replica announces with
`REPLCONF listening-port` and renders it. This issue is the *announcing* half — the value the
replica puts on the wire was `config.server.port`, snapshotted before the listener binds:

- `server/src/server/replication_init.rs` → `ReplicaReplicationHandler::new(addr, config.server.port, …)`
  (boot-configured replica).
- `server/src/role_manager.rs` → `RealReplicaStreamer::new` stored `listening_port: config.server.port`
  (every stream started by a runtime demotion — `REPLICAOF`, cluster failover).

`config.server.port == 0` means "let the OS assign one" in FrogDB (`test-harness/src/server.rs`
sets exactly that, and `cluster_init` already compensates for it when it advertises this node's
cluster client address). The server then serves on the assigned port but announces `0`, so the
primary renders:

```
slave0:ip=127.0.0.1,port=0,state=online,offset=1234,lag=0
```

which is the precise wire text issue 16 was closed to eliminate. `ROLE` on the primary reports the
same `0`.

Found by the forcing test written for FM-REPLICATION-049 while closing issue 19's decorative-test
class: `test_info_replication_shows_all_replicas` asserted each `slaveN:` line's `port=` against
the replica's real port and failed `left: [0, 0] right: [59658, 59662]`. Every test that had
covered this path asserted only that a port field was *present*, and the unit tests fed the
announcement in by hand, so neither end saw the wiring.

## Resolution

Fixed. The announced port now comes off the bound listener, once, and is threaded to both roles:

- `Server::with_listeners` computes `let listening_port = infra.listener.local_addr()?.port();`
  immediately after phase 1. For a configured non-zero port this is that same port (the listener
  bound it); for `port 0` it is what the OS assigned. There is no branch on `config.server.port`,
  so the two cases cannot drift.
- `init_replication` takes `listening_port: u16` and hands it to `ReplicaReplicationHandler::new`.
- `init_cluster` takes it too and hands it to `RealReplicaStreamer::new`, which no longer reads
  `config.server.port` at all.

`ReplicaReplicationHandler::listening_port()` was added (mirroring `primary_addr()` /
`shared_offset()`) so the wiring can be asserted at the seam without opening a socket.

## Forcing tests

- `the_handshake_announces_the_port_it_was_given` (frogdb-replication, unit): drives the real
  `ReplicaConnection::handshake` over an in-memory duplex and asserts the first thing on the wire
  is `REPLCONF listening-port 7001` byte for byte.
- `runtime_stream_announces_the_streamers_listening_port` (frogdb-server, unit): a demotion stream
  built by `RealReplicaStreamer::build_handler` announces the streamer's port, not a default.
- `test_info_replication_shows_all_replicas` (frogdb-server, integration): the end-to-end witness —
  two real replicas attach and each `slaveN:` line's `port=` equals that replica's actual port.
  This is the only test that covers the bind → announce → render path, which is where the bug was.

Spec row **FM-REPLICATION-049** (Trigger, Observable, NOT observable, Invariant, Forced by).
