//! Command metadata for connection-level commands.
//!
//! These commands are handled directly by the connection handler, not through
//! the normal shard routing. They provide metadata (name, arity, flags) for
//! command introspection (COMMAND INFO) without implementing execute().

use bytes::Bytes;
use frogdb_core::{Arity, CommandFlags, CommandMetadata, ConnectionLevelOp, ExecutionStrategy};

/// Macro to generate metadata-only command implementations.
macro_rules! metadata_command {
    ($name:ident, $cmd:literal, $arity:expr, $flags:expr, $strategy:expr) => {
        pub struct $name;

        impl CommandMetadata for $name {
            fn name(&self) -> &'static str {
                $cmd
            }

            fn arity(&self) -> Arity {
                $arity
            }

            fn flags(&self) -> CommandFlags {
                $flags
            }

            fn execution_strategy(&self) -> ExecutionStrategy {
                $strategy
            }

            fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
                vec![]
            }
        }
    };
}

// Pub/Sub commands (SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/SSUBSCRIBE/
// SUNSUBSCRIBE/PUBLISH/SPUBLISH/PUBSUB) were migrated behind the ConnCtx seam;
// their specs now live with their executors in
// `crate::connection::pubsub_conn_command` and they are registered via
// `register_connection`.

// =============================================================================
// Transaction Commands (handled at connection level)
// =============================================================================

metadata_command!(
    MultiMetadata,
    "MULTI",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
);

metadata_command!(
    ExecMetadata,
    "EXEC",
    Arity::Fixed(0),
    CommandFlags::WRITE
        | CommandFlags::NOSCRIPT
        | CommandFlags::LOADING
        | CommandFlags::STALE
        | CommandFlags::SKIP_SLOWLOG,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
);

metadata_command!(
    DiscardMetadata,
    "DISCARD",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
);

metadata_command!(
    WatchMetadata,
    "WATCH",
    Arity::AtLeast(1),
    CommandFlags::FAST | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
);

metadata_command!(
    UnwatchMetadata,
    "UNWATCH",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
);

// =============================================================================
// Connection State Commands (handled at connection level)
// =============================================================================

metadata_command!(
    QuitMetadata,
    "QUIT",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
);

// ASKING / READONLY / READWRITE / RESET were migrated behind the ConnCtx seam
// as mutating connection commands; their specs now live with their executors in
// `crate::connection::connection_state_conn_command` and they are registered via
// `register_connection`.

// =============================================================================
// Authentication Commands (handled at connection level)
// =============================================================================

metadata_command!(
    AuthMetadata,
    "AUTH",
    Arity::AtLeast(1),
    CommandFlags::FAST
        | CommandFlags::LOADING
        | CommandFlags::STALE
        | CommandFlags::NOSCRIPT
        | CommandFlags::NO_PROPAGATE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
);

metadata_command!(
    HelloMetadata,
    "HELLO",
    Arity::AtLeast(0),
    CommandFlags::FAST
        | CommandFlags::LOADING
        | CommandFlags::STALE
        | CommandFlags::NOSCRIPT
        | CommandFlags::NO_PROPAGATE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
);

// =============================================================================
// Persistence Commands (handled at connection level)
// =============================================================================

metadata_command!(
    BgsaveMetadata,
    "BGSAVE",
    Arity::Range { min: 0, max: 1 },
    CommandFlags::ADMIN,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence)
);

metadata_command!(
    LastsaveMetadata,
    "LASTSAVE",
    Arity::Fixed(0),
    CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence)
);

