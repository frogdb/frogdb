# REPLCONF listening-port and capa are parsed then discarded

Status: done
Type: bug (observability)
Severity: likelihood 3/3 (every replica handshake, unconditionally), consequence 2/3 (`INFO
replication` and `ROLE` report `port=0` for every replica, so no tool can reach a replica from the
primary's view; capability negotiation is decided but not recorded) — score 6
Area: replication / handshake

## Problem

The `REPLCONF` handler (`frogdb-server/crates/server/src/commands/replication.rs:245`) parses the
port, logs it, and returns `+OK` with a comment describing work that does not happen:

```rust
tracing::debug!(port = port, "REPLCONF listening-port");
// The server will store this in the replica connection state
Ok(Response::ok())
```

`capa` (`:258`) is identical — capabilities are collected into a `Vec<&str>`, logged, and dropped,
with the same "the server will store these" comment. Nothing in `frogdb-server/crates/server/src/`
sets either field: `ReplicaSession::new` (`replica_session.rs:191-192`) initialises
`listening_port: 0` and `capabilities: ReplicaCapabilities::default()`, and grep finds no writer
for either.

The reader side is fully built and works — `listening_port()` (`replica_session.rs:222`),
`info_handler.rs:133`, `commands/replication.rs:667` (`ROLE`) — so the value flows all the way to
the wire, it is just always `0`:

```
slave0:ip=127.0.0.1,port=0,state=online,offset=1234,lag=0
```

Redis reports the replica's *announced* port here precisely so an operator or an orchestrator can
connect to the replica using the primary's `INFO`. `port=0` is not a degraded answer, it is a wrong
one. `ReplicaCapabilities::parse_capa` exists and is unit-tested (`tracker.rs:546-552`), so the
parsing half is verified in isolation while the storing half does not exist — which is why no test
caught this.

The existing test `test_replconf_listening_port` (`integration_replication.rs:41`) asserts only
that the command returns `+OK`, which it does.

## Candidate fix

Add setters on `ReplicaSession` (`set_listening_port`, `set_capabilities`) and call them from the
connection-coupled `REPLCONF` path — the same place `PSYNC` reaches the session, since the
command-registry handler has no session handle. If threading the session into the registry handler
is awkward, move `listening-port`/`capa` into the connection handler alongside `PSYNC`, which is
where the comment implies they were always meant to live. Delete both stale comments once the
storage exists.

While there: `INFO`'s `state=online` and `lag=0` are string literals in both renderers
(`info/sections.rs:426`, `commands/info.rs:404`) even though `Phase` and `replica_lag_secs` are
available — same observability-accuracy class, worth folding into this fix or splitting out.

## Forcing tests

Extend `test_replconf_listening_port` past `+OK`: complete a handshake announcing port 7001 and
assert the primary's `INFO replication` renders `port=7001`, and that `ROLE` reports the same. A
capability test asserting a replica that announces `capa eof psync2` is recorded with both, and one
announcing an unknown capability is recorded without it and still completes the handshake (spec row
FM-REPLICATION-016 already covers the "unknown REPLCONF never breaks the handshake" half).

## Resolution

Fixed. The announcement is now recorded on the connection at `REPLCONF` time and applied when the
session is constructed at `PSYNC` time.

**The candidate fix above is not implementable as written.** There are no setters to add because
there is no session to set: `ReplicaSession` is created by `tracker.register_replica`, which runs
inside `PrimaryReplicationHandler::handle_psync` — one command *after* the `REPLCONF` that carries
the identity. The second option the issue offers ("move `listening-port`/`capa` into the connection
handler alongside `PSYNC`") is the one taken, in the stronger form of passing the announcement into
construction rather than mutating a session afterwards, so a registered session is never observable
in a placeholder state.

- `AnnouncedOption::parse(&args) -> Result<Option<AnnouncedOption>, AnnouncementError>`
  (`frogdb-server/crates/replication/src/replica_session.rs`) is the single parser for the two
  self-describing options; `Ok(None)` means "this `REPLCONF` announces nothing" (`ACK`, `GETACK`,
  unknown), so the forward-compatible `+OK` of FM-REPLICATION-018 is untouched.
- `DispatchStage::PsyncIntercept` was renamed `DispatchStage::ReplicationHandshake` — it now owns
  both commands of the handshake. Its `REPLCONF` arm folds the parsed option into
  `ConnectionHandler.replica_announcement: ReplicaAnnouncement` (a plain connection field, so it is
  per-link by construction) and answers `+OK`; its `PSYNC` arm passes that value into
  `handle_psync`, which hands it to `tracker.register_announced_replica` →
  `ReplicaSession::announced`.
- The shard-path `ReplconfCommand::execute` arm still exists for the `MULTI`-queued path that never
  reaches the connection stage. It calls the same parser and the same `announcement_error` mapping,
  so both paths answer identically on the wire. Both stale comments are deleted.
- One wire-text change: a non-UTF-8 `listening-port` value now answers `ERR invalid port number`
  rather than `ERR invalid port encoding`. No test asserted the old string.

The `state=online` / `lag=0` half the issue flagged was folded in rather than split out, closing
**GAP-2** and **GAP-3**: `ReplicaLine` now carries a `ReplicaState` projected from `Phase`
(`wait_bgsave` / `send_bulk` / `online` / `offline`) and a `lag_secs` that is the whole-second age of
the replica's last ACK — the same `ack_age_secs` measure `LagPolicy` disconnects on. `ReplicaState`
is total over `Phase`, so `Disconnecting` renders `offline` rather than dropping the line: Redis
skips a slave whose state it does not recognise, but dropping the line here would desynchronize
`connected_slaves` from the `slaveN:` count, which FM-REPLICATION-043 forbids. That is a new
Redis-deviations row.

Both renderers now share one spelling of the line — `ReplicaLine::from_replica` and
`ReplicaLine::render(index)` — so a field cannot be added to one and forgotten in the other.

Forcing tests (crate-level, so `cargo mutants -p` reaches them):
`an_announced_session_reports_the_port_and_capabilities_it_was_told`,
`an_unknown_capability_is_recorded_as_absent_not_rejected`,
`a_repeated_option_overwrites_only_its_own_kind`, `lag_secs_is_the_age_of_the_last_ack`,
`a_psync_carries_the_announcement_into_the_registry` (frogdb-replication);
`a_slave_line_is_projected_from_the_replica_not_from_literals`,
`a_slave_line_reports_the_real_ack_age`, `role_reports_the_announced_port` (frogdb-server, unit).
`announcement_errors_keep_their_wire_shape` tags **FM-REPLICATION-018** instead, since it pins that
row's "a malformed `listening-port` is rejected locally, not escalated" clause.

Spec row **FM-REPLICATION-049**. FM-REPLICATION-018's non-guarantee bullet and FM-REPLICATION-043's
Observable/Invariant cells and tail both need amending — the replacement text is in the fragment
this change shipped with.

**The issue's FM reference is stale:** it names FM-REPLICATION-016 for the "unknown REPLCONF never
breaks the handshake" half; that row is now FM-REPLICATION-018.
