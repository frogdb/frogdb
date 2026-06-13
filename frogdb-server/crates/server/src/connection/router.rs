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
    /// Authentication commands (AUTH).
    Auth,
    /// Protocol negotiation (HELLO).
    Hello,
    /// ACL commands.
    Acl,
    /// Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.).
    PubSub,
    /// Sharded Pub/Sub commands (SSUBSCRIBE, SPUBLISH).
    ShardedPubSub,
    /// Transaction commands (MULTI, EXEC, DISCARD).
    Transaction,
    /// Scripting commands (EVAL, EVALSHA, SCRIPT).
    Scripting,
    /// Function commands (FCALL, FUNCTION).
    Function,
    /// Client commands (CLIENT ID, CLIENT LIST, etc.).
    Client,
    /// Config commands (CONFIG GET, CONFIG SET).
    Config,
    /// Info commands.
    Info,
    /// Debug commands.
    Debug,
    /// Slowlog commands.
    Slowlog,
    /// Memory commands.
    Memory,
    /// Latency commands.
    Latency,
    /// Hotkeys commands.
    Hotkeys,
    /// Status commands.
    Status,
    /// Monitor command (MONITOR).
    Monitor,
    /// Connection state commands (RESET, SELECT, QUIT).
    ConnectionState,
    /// Replication commands (PSYNC, REPLCONF, etc.).
    Replication,
    /// Persistence commands (BGSAVE, LASTSAVE).
    Persistence,
    /// FT.CURSOR commands (cursor-based aggregate pagination).
    FtCursor,
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
/// `cmd_name` is load-bearing: the coarse ops (`Admin`, `Auth`, `PubSub`,
/// `Scripting`) fan out to multiple handlers keyed on the command name; the
/// remaining ops map 1:1.
pub(crate) fn handler_for(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler {
    match op {
        ConnectionLevelOp::Admin => match cmd_name {
            "CLIENT" => ConnectionLevelHandler::Client,
            "CONFIG" => ConnectionLevelHandler::Config,
            "ACL" => ConnectionLevelHandler::Acl,
            "INFO" => ConnectionLevelHandler::Info,
            "DEBUG" => ConnectionLevelHandler::Debug,
            "SLOWLOG" => ConnectionLevelHandler::Slowlog,
            "MEMORY" => ConnectionLevelHandler::Memory,
            "LATENCY" => ConnectionLevelHandler::Latency,
            "HOTKEYS" => ConnectionLevelHandler::Hotkeys,
            "STATUS" => ConnectionLevelHandler::Status,
            "MONITOR" => ConnectionLevelHandler::Monitor,
            "FT.CURSOR" => ConnectionLevelHandler::FtCursor,
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
        ConnectionLevelOp::Transaction => ConnectionLevelHandler::Transaction,
        ConnectionLevelOp::ConnectionState => ConnectionLevelHandler::ConnectionState,
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
        ConnectionLevelHandler::Auth,
        ConnectionLevelHandler::Hello,
        ConnectionLevelHandler::Acl,
        ConnectionLevelHandler::PubSub,
        ConnectionLevelHandler::ShardedPubSub,
        ConnectionLevelHandler::Transaction,
        ConnectionLevelHandler::Scripting,
        ConnectionLevelHandler::Function,
        ConnectionLevelHandler::Client,
        ConnectionLevelHandler::Config,
        ConnectionLevelHandler::Info,
        ConnectionLevelHandler::Debug,
        ConnectionLevelHandler::Slowlog,
        ConnectionLevelHandler::Memory,
        ConnectionLevelHandler::Latency,
        ConnectionLevelHandler::Hotkeys,
        ConnectionLevelHandler::Status,
        ConnectionLevelHandler::Monitor,
        ConnectionLevelHandler::ConnectionState,
        ConnectionLevelHandler::Replication,
        ConnectionLevelHandler::Persistence,
        ConnectionLevelHandler::FtCursor,
    ];

    /// Number of `ConnectionLevelHandler` variants. Bumped together with a new
    /// arm in [`variant_index`].
    const VARIANT_COUNT: usize = 22;

    /// Stable index per variant. The exhaustive `match` is the compile-time
    /// guard: adding a variant breaks compilation here until it is given an
    /// index (and `VARIANT_COUNT` is bumped), which in turn forces it into
    /// `ALL_HANDLERS` via [`handler_list_is_exhaustive`].
    fn variant_index(handler: ConnectionLevelHandler) -> usize {
        match handler {
            ConnectionLevelHandler::Auth => 0,
            ConnectionLevelHandler::Hello => 1,
            ConnectionLevelHandler::Acl => 2,
            ConnectionLevelHandler::PubSub => 3,
            ConnectionLevelHandler::ShardedPubSub => 4,
            ConnectionLevelHandler::Transaction => 5,
            ConnectionLevelHandler::Scripting => 6,
            ConnectionLevelHandler::Function => 7,
            ConnectionLevelHandler::Client => 8,
            ConnectionLevelHandler::Config => 9,
            ConnectionLevelHandler::Info => 10,
            ConnectionLevelHandler::Debug => 11,
            ConnectionLevelHandler::Slowlog => 12,
            ConnectionLevelHandler::Memory => 13,
            ConnectionLevelHandler::Latency => 14,
            ConnectionLevelHandler::Hotkeys => 15,
            ConnectionLevelHandler::Status => 16,
            ConnectionLevelHandler::Monitor => 17,
            ConnectionLevelHandler::ConnectionState => 18,
            ConnectionLevelHandler::Replication => 19,
            ConnectionLevelHandler::Persistence => 20,
            ConnectionLevelHandler::FtCursor => 21,
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
            (Op::Admin, "ACL", H::Acl),
            (Op::Admin, "INFO", H::Info),
            (Op::Admin, "DEBUG", H::Debug),
            (Op::Admin, "SLOWLOG", H::Slowlog),
            (Op::Admin, "MEMORY", H::Memory),
            (Op::Admin, "LATENCY", H::Latency),
            (Op::Admin, "HOTKEYS", H::Hotkeys),
            (Op::Admin, "STATUS", H::Status),
            (Op::Admin, "MONITOR", H::Monitor),
            (Op::Admin, "FT.CURSOR", H::FtCursor),
            (Op::Admin, "WHATEVER", H::Client), // fallback
            // Auth refines HELLO.
            (Op::Auth, "HELLO", H::Hello),
            (Op::Auth, "AUTH", H::Auth),
            (Op::Auth, "WHATEVER", H::Auth), // fallback
            // PubSub refines the sharded family.
            (Op::PubSub, "SSUBSCRIBE", H::ShardedPubSub),
            (Op::PubSub, "SUNSUBSCRIBE", H::ShardedPubSub),
            (Op::PubSub, "SPUBLISH", H::ShardedPubSub),
            (Op::PubSub, "SUBSCRIBE", H::PubSub),
            (Op::PubSub, "PUBLISH", H::PubSub), // fallback
            // Scripting refines the function family.
            (Op::Scripting, "FCALL", H::Function),
            (Op::Scripting, "FCALL_RO", H::Function),
            (Op::Scripting, "FUNCTION", H::Function),
            (Op::Scripting, "EVAL", H::Scripting),
            (Op::Scripting, "SCRIPT", H::Scripting), // fallback
            // 1:1 ops (cmd_name irrelevant).
            (Op::Transaction, "MULTI", H::Transaction),
            (Op::ConnectionState, "RESET", H::ConnectionState),
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
