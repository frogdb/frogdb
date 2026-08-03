# REPLCONF listening-port and capa are parsed then discarded

Status: needs-triage
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
