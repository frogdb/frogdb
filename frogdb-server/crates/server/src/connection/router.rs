//! Connection-level command routing.
//!
//! Owns the single op→handler decision: mapping a command's
//! [`ConnectionLevelOp`] to the [`ConnectionLevelHandler`] that executes it.
//! These are pure functions over registry data — no connection state, no I/O —
//! so the whole mapping is exhaustively table-testable.
//!
//! Runtime sequencing (pub/sub-mode gating, transaction queueing) deliberately
//! stays in [`crate::connection::dispatch`]: those decisions depend on live
//! connection state. Routing answers "which handler"; dispatch answers
//! "whether and when".

use frogdb_core::{CommandRegistry, ConnectionLevelOp, ExecutionStrategy};

/// Connection-level command handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionLevelHandler {
    /// Client commands (CLIENT ID, CLIENT LIST, etc.).
    Client,
    /// Config commands (CONFIG GET, CONFIG SET).
    Config,
    /// Replication commands (PSYNC, REPLCONF, etc.).
    Replication,
    /// Persistence commands (BGSAVE, LASTSAVE).
    Persistence,
}

/// The single routing decision: look up a command's execution strategy in the
/// registry and, when it is connection-level, refine it into a concrete handler.
///
/// Returns `Some(handler)` for commands declaring an
/// [`ExecutionStrategy::ConnectionLevel`] strategy (with the handler refined by
/// command name, e.g. `Admin` + `CONFIG` → `Config`). Returns `None` for any
/// other strategy (`Standard`, `ServerWide`, `ScatterGather`, ...).
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

/// Pure op→handler mapping.
///
/// `cmd_name` is load-bearing: the coarse ops (`Admin`, `PubSub`) fan out to
/// multiple handlers keyed on the command name; the remaining ops map 1:1.
pub(crate) fn handler_for(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler {
    match op {
        ConnectionLevelOp::Admin => match cmd_name {
            "CONFIG" => ConnectionLevelHandler::Config,
            // CLIENT, DEBUG, and MONITOR migrated behind the ConnCtx seam
            // (dispatched via the registry union): they dropped their dedicated
            // arms and now fall back to `Client` here like the other migrated
            // Admin commands (ACL/INFO), but are intercepted earlier by
            // `dispatch_connection_command` so the fallback is never reached.
            _ => ConnectionLevelHandler::Client, // fallback
        },
        // AUTH/HELLO (`Auth`), RESET/ASKING/READONLY/READWRITE
        // (`ConnectionState`), the pub/sub family (`PubSub`), Scripting/Function
        // (`Scripting`), and the transaction family (`Transaction`:
        // MULTI/EXEC/DISCARD/WATCH/UNWATCH) are all migrated behind the ConnCtx
        // seam and dispatched as connection commands (AUTH/HELLO pre-auth; RESET
        // early; ASKING/READONLY/READWRITE via the mutable registry union;
        // SUBSCRIBE/…/PUBSUB via the multi-response registry union;
        // EVAL/EVALSHA/SCRIPT/FCALL/FUNCTION via the registry union;
        // MULTI/EXEC/DISCARD/WATCH/UNWATCH via `dispatch_transaction_command`),
        // so these arms are never reached for them. They fall back to `Client` to
        // keep `handler_for` total (the same shape ACL/INFO took when they
        // dropped their router variants).
        ConnectionLevelOp::Auth
        | ConnectionLevelOp::ConnectionState
        | ConnectionLevelOp::PubSub
        | ConnectionLevelOp::Scripting
        | ConnectionLevelOp::Transaction => ConnectionLevelHandler::Client,
        ConnectionLevelOp::Replication => ConnectionLevelHandler::Replication,
        ConnectionLevelOp::Persistence => ConnectionLevelHandler::Persistence,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use frogdb_core::{CommandRegistry, ConnectionLevelOp, ExecutionStrategy};

    use super::{ConnectionLevelHandler, handler_for, route_connection_level};

    /// Every `ConnectionLevelHandler` variant, kept exhaustive at compile time
    /// by [`variant_index`]: adding a variant forces a new match arm there,
    /// and [`handler_list_is_exhaustive`] then forces it into this list.
    const ALL_HANDLERS: &[ConnectionLevelHandler] = &[
        ConnectionLevelHandler::Client,
        ConnectionLevelHandler::Config,
        ConnectionLevelHandler::Replication,
        ConnectionLevelHandler::Persistence,
    ];

    /// Number of `ConnectionLevelHandler` variants. Bumped together with a new
    /// arm in [`variant_index`].
    const VARIANT_COUNT: usize = 4;

    /// Stable index per variant. The exhaustive `match` is the compile-time
    /// guard: adding a variant breaks compilation here until it is given an
    /// index (and `VARIANT_COUNT` is bumped), which in turn forces it into
    /// `ALL_HANDLERS` via [`handler_list_is_exhaustive`].
    fn variant_index(handler: ConnectionLevelHandler) -> usize {
        match handler {
            ConnectionLevelHandler::Client => 0,
            ConnectionLevelHandler::Config => 1,
            ConnectionLevelHandler::Replication => 2,
            ConnectionLevelHandler::Persistence => 3,
        }
    }

    /// `ALL_HANDLERS` lists every variant exactly once.
    #[test]
    fn handler_list_is_exhaustive() {
        assert_eq!(ALL_HANDLERS.len(), VARIANT_COUNT);
        let mut seen = [false; VARIANT_COUNT];
        for handler in ALL_HANDLERS {
            let idx = variant_index(*handler);
            assert!(!seen[idx], "duplicate variant in ALL_HANDLERS: {handler:?}");
            seen[idx] = true;
        }
        assert!(
            seen.iter().all(|&s| s),
            "ALL_HANDLERS is missing a ConnectionLevelHandler variant"
        );
    }

    /// Exhaustive op table: every `ConnectionLevelOp` variant and every
    /// `cmd_name` refinement branch in `handler_for`, including fallbacks.
    /// Pure — no registry, no server, no runtime.
    #[test]
    fn handler_for_op_table() {
        use ConnectionLevelHandler as H;
        use ConnectionLevelOp as Op;

        let cases: &[(Op, &str, H)] = &[
            // Admin fans out by command name.
            (Op::Admin, "CLIENT", H::Client),
            (Op::Admin, "CONFIG", H::Config),
            // ACL and INFO both migrated behind the ConnCtx seam (dispatched via
            // the registry union): they dropped their router variants and now
            // fall back to Client in handler_for, but are intercepted earlier by
            // dispatch_connection_command so the fallback is never reached.
            (Op::Admin, "ACL", H::Client),
            (Op::Admin, "INFO", H::Client),
            // DEBUG and MONITOR both migrated behind the ConnCtx seam (dispatched
            // via the registry union): they dropped their router variants and now
            // fall back to Client in handler_for, but are intercepted earlier by
            // dispatch_connection_command so the fallback is never reached.
            (Op::Admin, "DEBUG", H::Client),
            (Op::Admin, "MONITOR", H::Client),
            (Op::Admin, "WHATEVER", H::Client), // fallback
            // AUTH and HELLO both migrated behind the ConnCtx seam (dispatched
            // pre-auth via the registry union): they dropped their router
            // variants and now fall back to Client in handler_for, but are
            // intercepted earlier so the fallback is never reached.
            (Op::Auth, "HELLO", H::Client),
            (Op::Auth, "AUTH", H::Client),
            // The pub/sub family migrated behind the ConnCtx seam (dispatched via
            // the multi-response registry union): it dropped its PubSub /
            // ShardedPubSub router variants and now falls back to Client in
            // handler_for, but is intercepted earlier by
            // dispatch_connection_command so the fallback is never reached.
            (Op::PubSub, "SSUBSCRIBE", H::Client),
            (Op::PubSub, "SUNSUBSCRIBE", H::Client),
            (Op::PubSub, "SPUBLISH", H::Client),
            (Op::PubSub, "SUBSCRIBE", H::Client),
            (Op::PubSub, "PUBLISH", H::Client),
            // Scripting/Function migrated behind the ConnCtx seam (dispatched via
            // the registry union): they dropped their router variants and now fall
            // back to Client in handler_for, but are intercepted earlier by
            // dispatch_connection_command so the fallback is never reached.
            (Op::Scripting, "EVAL", H::Client),
            (Op::Scripting, "EVALSHA", H::Client),
            (Op::Scripting, "SCRIPT", H::Client),
            (Op::Scripting, "FCALL", H::Client),
            (Op::Scripting, "FCALL_RO", H::Client),
            (Op::Scripting, "FUNCTION", H::Client),
            // The transaction family migrated behind the ConnCtx seam
            // (dispatched via `dispatch_transaction_command`): it dropped its
            // Transaction router variant and now falls back to Client in
            // handler_for, but is intercepted earlier so the fallback is never
            // reached.
            (Op::Transaction, "MULTI", H::Client),
            (Op::Transaction, "EXEC", H::Client),
            // 1:1 ops (cmd_name irrelevant).
            // RESET/ASKING/READONLY/READWRITE migrated behind the ConnCtx seam
            // (dispatched as mutating connection commands): they dropped their
            // router variant and now fall back to Client in handler_for, but are
            // intercepted earlier so the fallback is never reached.
            (Op::ConnectionState, "RESET", H::Client),
            (Op::ConnectionState, "ASKING", H::Client),
            (Op::Replication, "PSYNC", H::Replication),
            (Op::Persistence, "BGSAVE", H::Persistence),
        ];

        for (op, cmd_name, expected) in cases {
            assert_eq!(
                handler_for(op, cmd_name),
                *expected,
                "handler_for({op:?}, {cmd_name:?})"
            );
        }
    }

    /// Registry-driven totality: every registered command whose strategy is
    /// `ConnectionLevel` resolves to a handler via `route_connection_level`.
    #[test]
    fn route_connection_level_covers_registry() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let unresolved: Vec<String> = registry
            .iter()
            .filter(|(_, entry)| {
                matches!(
                    entry.execution_strategy(),
                    ExecutionStrategy::ConnectionLevel(_)
                )
            })
            .map(|(name, _)| name.to_string())
            .filter(|name| route_connection_level(&registry, &name.to_ascii_uppercase()).is_none())
            .collect();

        assert!(
            unresolved.is_empty(),
            "connection-level commands with no handler: {unresolved:?}"
        );
    }

    /// Reachability: every `ConnectionLevelHandler` variant is produced by at
    /// least one registered command. This is the automated deletion test that
    /// would have caught the formerly-dead `Cluster` variant.
    #[test]
    fn every_handler_reachable_from_registry() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let mut produced: HashSet<ConnectionLevelHandler> = HashSet::new();
        for (name, entry) in registry.iter() {
            if let ExecutionStrategy::ConnectionLevel(op) = entry.execution_strategy() {
                produced.insert(handler_for(&op, &name.to_ascii_uppercase()));
            }
        }

        let unreachable: Vec<ConnectionLevelHandler> = ALL_HANDLERS
            .iter()
            .copied()
            .filter(|h| !produced.contains(h))
            .collect();

        assert!(
            unreachable.is_empty(),
            "handlers never produced by any registered command: {unreachable:?}"
        );
    }
}
