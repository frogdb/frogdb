//! Stub implementations for unimplemented Redis commands.
//!
//! These commands are recognized by the server but return a "not implemented" error.
//! This allows clients to discover which commands exist but aren't yet functional,
//! rather than receiving "unknown command" errors.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

/// Macro to generate stub command implementations.
macro_rules! stub_command {
    ($name:ident, $cmd:literal, $arity:expr, $flags:expr) => {
        pub struct $name;

        impl Command for $name {
            fn name(&self) -> &'static str {
                $cmd
            }

            fn arity(&self) -> Arity {
                $arity
            }

            fn flags(&self) -> CommandFlags {
                $flags
            }

            fn execute(
                &self,
                _ctx: &mut CommandContext,
                _args: &[Bytes],
            ) -> Result<Response, CommandError> {
                Err(CommandError::NotImplemented { command: $cmd })
            }

            fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
                vec![]
            }
        }
    };
}

// =============================================================================
// Pub/Sub Commands
// =============================================================================

stub_command!(
    SubscribeCommand,
    "SUBSCRIBE",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    PsubscribeCommand,
    "PSUBSCRIBE",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    SsubscribeCommand,
    "SSUBSCRIBE",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    UnsubscribeCommand,
    "UNSUBSCRIBE",
    Arity::AtLeast(0),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    PunsubscribeCommand,
    "PUNSUBSCRIBE",
    Arity::AtLeast(0),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    SunsubscribeCommand,
    "SUNSUBSCRIBE",
    Arity::AtLeast(0),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    PublishCommand,
    "PUBLISH",
    Arity::Fixed(2),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::FAST
);

stub_command!(
    SpublishCommand,
    "SPUBLISH",
    Arity::Fixed(2),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::FAST
);

stub_command!(
    PubsubCommand,
    "PUBSUB",
    Arity::AtLeast(1),
    CommandFlags::PUBSUB | CommandFlags::LOADING | CommandFlags::STALE
);

// =============================================================================
// Cluster Commands
// =============================================================================

stub_command!(
    ClusterCommand,
    "CLUSTER",
    Arity::AtLeast(1),
    CommandFlags::ADMIN | CommandFlags::STALE
);

stub_command!(
    AskingCommand,
    "ASKING",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::STALE
);

stub_command!(
    ReadonlyCommand,
    "READONLY",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    ReadwriteCommand,
    "READWRITE",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
);

// =============================================================================
// Replication Commands
// =============================================================================

stub_command!(
    ReplicaofCommand,
    "REPLICAOF",
    Arity::Fixed(2),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::STALE
);

stub_command!(
    SlaveofCommand,
    "SLAVEOF",
    Arity::Fixed(2),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::STALE
);

stub_command!(
    WaitCommand,
    "WAIT",
    Arity::Fixed(2),
    CommandFlags::NOSCRIPT
);

stub_command!(
    WaitaofCommand,
    "WAITAOF",
    Arity::Fixed(3),
    CommandFlags::NOSCRIPT
);

stub_command!(
    PsyncCommand,
    "PSYNC",
    Arity::Fixed(2),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT
);

stub_command!(
    ReplconfCommand,
    "REPLCONF",
    Arity::AtLeast(0),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
);

// =============================================================================
// Memory Commands
// =============================================================================

stub_command!(
    MemoryCommand,
    "MEMORY",
    Arity::AtLeast(1),
    CommandFlags::READONLY | CommandFlags::RANDOM
);

// =============================================================================
// Latency Commands
// =============================================================================

stub_command!(
    LatencyCommand,
    "LATENCY",
    Arity::AtLeast(1),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
);

// =============================================================================
// Module Commands
// =============================================================================

stub_command!(
    ModuleCommand,
    "MODULE",
    Arity::AtLeast(1),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT
);

// =============================================================================
// Function Commands
// =============================================================================

stub_command!(
    FunctionCommand,
    "FUNCTION",
    Arity::AtLeast(1),
    CommandFlags::NOSCRIPT
);

stub_command!(
    FcallCommand,
    "FCALL",
    Arity::AtLeast(2),
    CommandFlags::SCRIPT | CommandFlags::NOSCRIPT | CommandFlags::STALE
);

stub_command!(
    FcallRoCommand,
    "FCALL_RO",
    Arity::AtLeast(2),
    CommandFlags::SCRIPT | CommandFlags::NOSCRIPT | CommandFlags::READONLY | CommandFlags::STALE
);

// =============================================================================
// Generic/Keys Commands
// =============================================================================

stub_command!(
    MigrateCommand,
    "MIGRATE",
    Arity::AtLeast(5),
    CommandFlags::WRITE
);

stub_command!(
    MoveCommand,
    "MOVE",
    Arity::Fixed(2),
    CommandFlags::WRITE | CommandFlags::FAST
);

// =============================================================================
// Connection Commands
// =============================================================================

stub_command!(
    SelectCommand,
    "SELECT",
    Arity::Fixed(1),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
);

stub_command!(
    SwapdbCommand,
    "SWAPDB",
    Arity::Fixed(2),
    CommandFlags::WRITE | CommandFlags::FAST
);

stub_command!(
    ResetCommand,
    "RESET",
    Arity::Fixed(0),
    CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::NOSCRIPT
);

// =============================================================================
// Server Commands
// =============================================================================

stub_command!(
    BgrewriteaofCommand,
    "BGREWRITEAOF",
    Arity::Fixed(0),
    CommandFlags::ADMIN
);

stub_command!(
    LolwutCommand,
    "LOLWUT",
    Arity::AtLeast(0),
    CommandFlags::READONLY | CommandFlags::FAST
);

stub_command!(
    RoleCommand,
    "ROLE",
    Arity::Fixed(0),
    CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::FAST | CommandFlags::NOSCRIPT
);

stub_command!(
    SaveCommand,
    "SAVE",
    Arity::Fixed(0),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT
);

stub_command!(
    MonitorCommand,
    "MONITOR",
    Arity::Fixed(0),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT
);

// =============================================================================
// String Commands (deprecated/missing)
// =============================================================================

stub_command!(
    GetsetCommand,
    "GETSET",
    Arity::Fixed(2),
    CommandFlags::WRITE | CommandFlags::FAST
);

stub_command!(
    SubstrCommand,
    "SUBSTR",
    Arity::Fixed(3),
    CommandFlags::READONLY
);

// =============================================================================
// List Commands (deprecated)
// =============================================================================

stub_command!(
    RpoplpushCommand,
    "RPOPLPUSH",
    Arity::Fixed(2),
    CommandFlags::WRITE
);

// =============================================================================
// Replication Commands (additional)
// =============================================================================

stub_command!(
    SyncCommand,
    "SYNC",
    Arity::Fixed(0),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::READONLY
);
