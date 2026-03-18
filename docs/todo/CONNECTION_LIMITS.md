# Max Clients + Connection Rejection

## Overview

Two metrics depend on an unimplemented `max_clients` feature:

| Metric | Type | Description |
| --- | --- | --- |
| `frogdb_connections_max` | gauge | Maximum configured connections (currently hardcoded to 0) |
| `frogdb_connections_rejected_total` | counter | Connections rejected due to limit (labels: `reason`) |

These cannot be meaningfully wired until the server enforces a connection limit.

## Current state

- `ConnectionsMax` is defined in `telemetry/src/definitions.rs` but never set — effectively 0.
- `ConnectionsRejected` is defined with a `RejectionReason` label (`telemetry/src/labels.rs`)
  but never incremented.
- The acceptor loop in `server/src/server/mod.rs` accepts all connections unconditionally.
- There is no `max_clients` or `maxclients` config field.

## Implementation plan

### 1. Config addition

Add `max_clients: Option<u32>` to the server config (default: `None` = unlimited, matching
Redis's default of 10000 when unset).

- Wire through `CONFIG GET maxclients` / `CONFIG SET maxclients`
- On startup or config change, call `ConnectionsMax::set(&*recorder, value)`

### 2. Acceptor gate

In the acceptor loop (`server/src/server/mod.rs`), after `listener.accept()`:

```text
if current_connections >= max_clients {
    ConnectionsRejected::inc(&*recorder, RejectionReason::MaxClients);
    // Send -ERR max clients reached, then close
    continue;
}
```

The `ConnectionsCurrent` gauge is already wired, so `current_connections` is available.

### 3. Rejection reasons

The `RejectionReason` label enum in `telemetry/src/labels.rs` should cover at least:

- `MaxClients` — connection limit reached
- Future: `Acl` (ACL-based rejection), `Tls` (TLS handshake failure)

### 4. Redis compatibility

- `INFO clients` should report `maxclients` (currently may report 0)
- `CONFIG GET maxclients` / `CONFIG SET maxclients` should work
- Error message on rejection: `-ERR max number of clients reached`

## Dependencies

- None — this is a self-contained feature that only touches `server` and `telemetry` crates.
