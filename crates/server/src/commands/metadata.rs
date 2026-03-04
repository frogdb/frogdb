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

// =============================================================================
// Pub/Sub Commands (handled at connection level)
// =============================================================================

metadata_command!(
    SubscribeMetadata,
    "SUBSCRIBE",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    PsubscribeMetadata,
    "PSUBSCRIBE",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    SsubscribeMetadata,
    "SSUBSCRIBE",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    UnsubscribeMetadata,
    "UNSUBSCRIBE",
    Arity::AtLeast(0),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    PunsubscribeMetadata,
    "PUNSUBSCRIBE",
    Arity::AtLeast(0),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    SunsubscribeMetadata,
    "SUNSUBSCRIBE",
    Arity::AtLeast(0),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    PublishMetadata,
    "PUBLISH",
    Arity::Fixed(2),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::FAST,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    SpublishMetadata,
    "SPUBLISH",
    Arity::Fixed(2),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::FAST,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

metadata_command!(
    PubsubMetadata,
    "PUBSUB",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
);

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

metadata_command!(
    AskingMetadata,
    "ASKING",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
);

metadata_command!(
    ReadonlyMetadata,
    "READONLY",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
);

metadata_command!(
    ReadwriteMetadata,
    "READWRITE",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
);

metadata_command!(
    ResetMetadata,
    "RESET",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::NOSCRIPT,
    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
);

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
