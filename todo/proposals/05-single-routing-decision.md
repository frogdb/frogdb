# Proposal: Single Routing Decision

Status: proposed
Date: 2026-06-12

## Problem

The op→handler routing decision — mapping a command's `ConnectionLevelOp` to the
`ConnectionLevelHandler` that will execute it — is made in two separate modules:

- `frogdb-server/crates/server/src/connection/router.rs:211` —
  `CommandRouter::op_to_handler(op: &ConnectionLevelOp) -> ConnectionLevelHandler`
- `frogdb-server/crates/server/src/connection/dispatch.rs:129` —
  `ConnectionHandler::refine_handler(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler`

Two modules answering the same question is a seam in the wrong place: any change to command
classification must be made twice, and drift between the copies means a command classified one way
but dispatched another.

**The honest account: they are not pure duplicates, and the drift has already happened.**
`refine_handler` takes `cmd_name` and is strictly more refined — it splits coarse ops into specific
handlers (`Admin` + `"CONFIG"` → `Config`, `Auth` + `"HELLO"` → `Hello`, `PubSub` + `"SSUBSCRIBE"` →
`ShardedPubSub`, `Scripting` + `"FCALL"` → `Function`). `op_to_handler` is the stale coarse version:
it maps `Admin` → `Client` unconditionally, so via that path CONFIG would dispatch to
`handle_client_command`, HELLO to `handle_auth`, FCALL to the scripting handler. Fourteen of the 23
`ConnectionLevelHandler` variants (`Hello`, `Acl`, `Config`, `Info`, `Debug`, `Slowlog`, `Memory`,
`Latency`, `Hotkeys`, `Status`, `Monitor`, `ShardedPubSub`, `Function`, `FtCursor`) are unreachable
through `op_to_handler`.

The only reason this drift is not a live bug is that **`CommandRouter::route` has no production
callers**. A workspace-wide grep finds `CommandRouter::route(` invoked only from `router.rs`'s own
`#[cfg(test)]` module (router.rs:231–267). `RouteResult`, `ScatterStrategy`, and
`impl From<MergeStrategy> for ScatterStrategy` (router.rs:16–132) are likewise referenced nowhere
outside `router.rs`. The module fails the deletion test: delete `CommandRouter`, `RouteResult`, and
`ScatterStrategy`, and the server compiles and behaves identically. Its tests pass by testing dead
code, which is worse than no tests — they assert the stale mapping is correct.

The duplication extends beyond op→handler:

- **Pub/sub-mode gating** exists twice: `route_pubsub_mode` (router.rs:188, dead) vs. the live
  in-pubsub dispatch in `route_and_execute_with_transaction` (dispatch.rs:534).
- **Transaction-immediate commands** exist twice and **already disagree**:
  `is_immediate_in_transaction` (router.rs:203) lists `MULTI | EXEC | DISCARD | WATCH | UNWATCH |
  QUIT | RESET`; the live check (dispatch.rs:542) lists `MULTI | EXEC | DISCARD | WATCH | UNWATCH |
  RESET` — no `QUIT`. Same decision, two answers.

Secondary observation (context, not the fix): the `ConnectionHandler` interface is diffuse —
`impl ConnectionHandler` blocks are spread across 37 files under `connection/handlers/`, plus
`dispatch.rs`, `routing.rs`, and others, with no single place showing the interface. That hurts
locality but is a larger reorganization; this proposal scopes to the routing duplication, where a
small change has high leverage.

## Current state

### The dead twin — router.rs

```rust
// router.rs:153 — no production callers; only router.rs's own tests call this.
pub fn route(
    cmd_name: &str,
    strategy: &ExecutionStrategy,
    in_transaction: bool,
    in_pubsub: bool,
) -> RouteResult {
    if in_pubsub {
        return Self::route_pubsub_mode(cmd_name);
    }
    if in_transaction && !Self::is_immediate_in_transaction(cmd_name) {
        return RouteResult::QueueInTransaction;
    }
    match strategy {
        ExecutionStrategy::Standard => RouteResult::RouteToShard { shard_id: 0 },
        ExecutionStrategy::ConnectionLevel(op) => {
            RouteResult::ConnectionLevel(Self::op_to_handler(op))
        }
        // ... Blocking / ScatterGather / RaftConsensus / AsyncExternal / ServerWide
    }
}

// router.rs:211 — the coarse, stale mapping.
fn op_to_handler(op: &ConnectionLevelOp) -> ConnectionLevelHandler {
    match op {
        ConnectionLevelOp::Auth => ConnectionLevelHandler::Auth,       // HELLO lost
        ConnectionLevelOp::PubSub => ConnectionLevelHandler::PubSub,   // SSUBSCRIBE lost
        ConnectionLevelOp::Scripting => ConnectionLevelHandler::Scripting, // FCALL lost
        ConnectionLevelOp::Admin => ConnectionLevelHandler::Client,    // CONFIG/ACL/INFO/... lost
        ConnectionLevelOp::Transaction => ConnectionLevelHandler::Transaction,
        ConnectionLevelOp::ConnectionState => ConnectionLevelHandler::ConnectionState,
        ConnectionLevelOp::Replication => ConnectionLevelHandler::Replication,
        ConnectionLevelOp::Persistence => ConnectionLevelHandler::Persistence,
    }
}
```

### The live mapping — dispatch.rs

```rust
// dispatch.rs:116 — registry lookup + refinement; this is the real routing decision.
pub(crate) fn connection_level_handler_for(
    &self,
    cmd_name: &str,
) -> Option<ConnectionLevelHandler> {
    let entry = self.core.registry.get_entry(cmd_name)?;
    match entry.execution_strategy() {
        ExecutionStrategy::ConnectionLevel(op) => Some(Self::refine_handler(&op, cmd_name)),
        _ => None,
    }
}

// dispatch.rs:129 — the refinement op_to_handler lacks. cmd_name is load-bearing.
fn refine_handler(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler {
    match op {
        ConnectionLevelOp::Admin => match cmd_name {
            "CLIENT" => ConnectionLevelHandler::Client,
            "CONFIG" => ConnectionLevelHandler::Config,
            "ACL" => ConnectionLevelHandler::Acl,
            "INFO" => ConnectionLevelHandler::Info,
            // ... DEBUG / SLOWLOG / MEMORY / LATENCY / HOTKEYS / STATUS / MONITOR / FT.CURSOR
            _ => ConnectionLevelHandler::Client, // fallback
        },
        ConnectionLevelOp::Auth => match cmd_name {
            "HELLO" => ConnectionLevelHandler::Hello,
            _ => ConnectionLevelHandler::Auth,
        },
        ConnectionLevelOp::PubSub => match cmd_name {
            "SSUBSCRIBE" | "SUNSUBSCRIBE" | "SPUBLISH" => ConnectionLevelHandler::ShardedPubSub,
            _ => ConnectionLevelHandler::PubSub,
        },
        ConnectionLevelOp::Scripting => match cmd_name {
            "FCALL" | "FCALL_RO" | "FUNCTION" => ConnectionLevelHandler::Function,
            _ => ConnectionLevelHandler::Scripting,
        },
        // Transaction / ConnectionState / Replication / Persistence map 1:1
    }
}
```

### Call flow (live path)

```
lifecycle → route_and_execute_with_transaction(cmd, cmd_name)        dispatch.rs:505
    ├─ AUTH/HELLO/ACL short-circuit                                  dispatch.rs:511–523
    ├─ pubsub mode → dispatch_connection_state(...)                  dispatch.rs:534
    ├─ MULTI/EXEC/... immediate                                      dispatch.rs:542
    │     └─ connection_level_handler_for → refine_handler           dispatch.rs:545
    ├─ transaction queueing                                          dispatch.rs:558
    ├─ connection-level dispatch                                     dispatch.rs:594
    │     └─ connection_level_handler_for → refine_handler
    │           └─ dispatch_connection_level(handler, cmd_name, args) dispatch.rs:170
    ├─ server-wide table (SERVER_WIDE_HANDLERS)                      dispatch.rs:616
    └─ route_and_execute → shard routing                             routing.rs:28

EXEC-time (deferred connection-level commands inside MULTI):
handle_exec → connection_level_handler_for                           handlers/transaction.rs:157
            → execute_connection_level_in_transaction                handlers/transaction.rs:610

CommandRouter::route → op_to_handler                                 router.rs:153 — DEAD
```

The pieces of `router.rs` that pass the deletion test are the `ConnectionLevelHandler` enum
(router.rs:53), used by `dispatch.rs` and `handlers/transaction.rs` — everything else is dead.

## Proposed design

One routing module owns the op→handler mapping; dispatch consumes its output. Routing becomes a
deep module behind a narrow interface: two pure functions in, a handler out. Delete the dead twin.

### router.rs after

```rust
//! Connection-level command routing.
//!
//! Owns the single op→handler decision. Pure functions over registry data —
//! no connection state, no I/O — so the whole mapping is table-testable.

use frogdb_core::{CommandRegistry, ConnectionLevelOp, ExecutionStrategy};

/// Which connection-level handler executes a command. (Enum unchanged, minus dead variants.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionLevelHandler { Auth, Hello, Acl, PubSub, ShardedPubSub, /* ... */ }

/// The single routing decision: registry strategy → handler.
/// Returns `None` for commands that are not connection-level (Standard, ServerWide, ...).
pub(crate) fn route_connection_level(
    registry: &CommandRegistry,
    cmd_name: &str,
) -> Option<ConnectionLevelHandler> {
    let entry = registry.get_entry(cmd_name)?;
    match entry.execution_strategy() {
        ExecutionStrategy::ConnectionLevel(op) => Some(handler_for(&op, cmd_name)),
        _ => None,
    }
}

/// Pure op→handler mapping. `cmd_name` refines coarse ops (Admin, Auth, PubSub, Scripting)
/// into specific handlers; this is the body of today's `refine_handler`, moved verbatim.
pub(crate) fn handler_for(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler {
    match op {
        ConnectionLevelOp::Admin => match cmd_name { /* CLIENT/CONFIG/ACL/... */ },
        ConnectionLevelOp::Auth => match cmd_name { "HELLO" => Hello, _ => Auth },
        // ... exactly refine_handler's arms today (dispatch.rs:129–163)
    }
}
```

Deleted (all fail the deletion test): `CommandRouter`, `RouteResult`, `ScatterStrategy`,
`From<MergeStrategy>`, `route_pubsub_mode`, `is_immediate_in_transaction`, `op_to_handler`, and the
tests that exercise them. The unreachable `ConnectionLevelHandler::Cluster` variant (constructed
nowhere; `dispatch_connection_level` maps it to `None` at dispatch.rs:221) goes with them.

### dispatch.rs before/after

```rust
// BEFORE (dispatch.rs:116–163): dispatch module owns lookup + the 35-line mapping.
impl ConnectionHandler {
    pub(crate) fn connection_level_handler_for(&self, cmd_name: &str)
        -> Option<ConnectionLevelHandler> {
        let entry = self.core.registry.get_entry(cmd_name)?;
        match entry.execution_strategy() {
            ExecutionStrategy::ConnectionLevel(op) => Some(Self::refine_handler(&op, cmd_name)),
            _ => None,
        }
    }
    fn refine_handler(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler { /* 35 lines */ }
}

// AFTER: dispatch consumes the routing module's output; the mapping lives in one place.
impl ConnectionHandler {
    pub(crate) fn connection_level_handler_for(&self, cmd_name: &str)
        -> Option<ConnectionLevelHandler> {
        router::route_connection_level(&self.core.registry, cmd_name)
    }
}
```

Callers (`dispatch.rs:545`, `dispatch.rs:594`, `handlers/transaction.rs:157`,
`handlers/transaction.rs:610`) are unchanged — `connection_level_handler_for` keeps its signature,
so the dispatch path's shape is identical; only the decision's home moves. The refinement
genuinely needs `cmd_name` (CONFIG vs CLIENT, HELLO vs AUTH, FCALL vs EVAL), so the unified
function keeps that parameter — it stays pure because `cmd_name` is data, not state.

Mode-dependent sequencing (pub/sub gating at dispatch.rs:534, transaction queueing at
dispatch.rs:558) is deliberately **not** absorbed: those decisions depend on live connection state
(`self.state.pubsub`, `self.state.transaction.queue`) and ordering against pre-checks, pause, and
arity validation. Routing answers "which handler"; dispatch answers "whether and when". Folding
state-dependent sequencing into the routing module would make it shallow again — wide interface,
thin logic.

This mirrors a refactor that already worked here: `SERVER_WIDE_HANDLERS` (dispatch.rs:38) replaced
scattered ServerWide dispatch with one name-keyed table enforced by `dispatch_table_covers_registry`
(dispatch.rs:713). Same move, applied to connection-level routing.

## Migration plan

Single PR; no behavior change (the dead path has no behavior to change).

1. Delete `CommandRouter`, `RouteResult`, `ScatterStrategy`, the `From<MergeStrategy>` impl, and
   router.rs's tests of them. Keep `ConnectionLevelHandler`; drop the `Cluster` variant and its
   `None` arm in `dispatch_connection_level`.
2. Move `refine_handler`'s body to `router::handler_for` and the registry lookup to
   `router::route_connection_level`; reduce `connection_level_handler_for` to a delegation.
3. Add the table tests below. `just check frogdb-server && just test frogdb-server` to verify.
4. Optional follow-up (separate PR): rename to resolve the `router.rs` / `routing.rs` near-collision
   (e.g. `router.rs` → `connection_routing.rs`, since `routing.rs` is shard routing).

## Testing impact

Routing becomes a pure function over registry data — exhaustively table-testable with no server
setup, no tokio runtime, no shards:

- **Registry-driven totality test** (pattern: `dispatch_table_covers_registry`, dispatch.rs:713):
  build the real registry via `register_commands`, and for every command whose strategy is
  `ExecutionStrategy::ConnectionLevel`, assert `route_connection_level` returns `Some(handler)` and
  that `dispatch_connection_level` has a non-fall-through arm for it (Replication excepted — PSYNC's
  fall-through is intentional, dispatch.rs:225).
- **Exhaustive op table test**: one table of `(ConnectionLevelOp, cmd_name, expected handler)` rows
  covering every op variant and every cmd_name branch in `handler_for` (CONFIG, ACL, HELLO,
  SSUBSCRIBE, FCALL, FT.CURSOR, fallbacks). The `match` is already exhaustive over ops at compile
  time; the table makes the cmd_name refinements regression-proof.
- **Reachability test**: every `ConnectionLevelHandler` variant is produced for at least one
  registered command — the automated deletion test that would have caught the dead `Cluster`
  variant and the stale `op_to_handler` years of drift.

Net: the existing router.rs tests of dead code are replaced by tests of the code that actually
runs, and adding a new connection-level command that forgets a dispatch arm fails CI instead of
silently falling through.

## Risks / open questions

- **`cmd_name` is load-bearing, permanently.** The Admin family shares one `ConnectionLevelOp` but
  fans out to 12 handlers. Alternative: split `ConnectionLevelOp` in frogdb-core so each command
  declares its handler directly and the refinement disappears. More invasive (touches command
  registration across the core crate) — deferred, but it would make `handler_for` a 1:1 map.
- **Runtime state stays in dispatch.** Pub/sub-mode gating and transaction queueing need
  `&self.state`; keeping them out of the routing module is a judgment call. If a third mode arrives
  (e.g. MONITOR mode), revisit whether a state-aware `route(cmd, &ConnectionState)` layer above
  `handler_for` earns its keep.
- **`Replication` fall-through.** `route_connection_level` returns `Some(Replication)` but dispatch
  intentionally returns `None` so PSYNC reaches `route_and_execute` with the full command
  (dispatch.rs:225, :604). The unified design preserves this; a cleaner end state would route PSYNC
  via its own strategy rather than a handler that declines to handle.
- **EXEC-time second table.** `execute_connection_level_in_transaction`
  (handlers/transaction.rs:610) consumes the same handler but re-matches per command with its own
  semantics (deferred execution, RESP3 push conversion). That is a dispatch-policy duplicate, not a
  routing duplicate — out of scope here, but the same locality argument applies if it drifts.
- **37-file `impl ConnectionHandler` diffusion** remains. This proposal fixes where the routing
  decision lives, not where handlers live; a handlers-interface consolidation would be a separate,
  larger proposal.
