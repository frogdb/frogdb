//! Stub implementations for unimplemented Redis commands.
//!
//! These commands are recognized by the server but return a "not implemented" error.
//! This allows clients to discover which commands exist but aren't yet functional,
//! rather than receiving "unknown command" errors.
//!
//! Note: Commands that have been implemented elsewhere (e.g., replication commands
//! in the replication module, pub/sub commands as metadata) should not be here.
//! This file only contains stubs for commands that are truly not yet implemented.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use std::sync::LazyLock;

/// Macro to generate stub command implementations.
///
/// Stubs are keyless and never persist or notify. `flags` is built at runtime
/// in a `LazyLock` because `CommandFlags` bit-or is not `const`.
macro_rules! stub_command {
    ($name:ident, $cmd:literal, $arity:expr, $flags:expr) => {
        pub struct $name;

        impl Command for $name {
            fn spec(&self) -> &'static CommandSpec {
                static SPEC: LazyLock<CommandSpec> = LazyLock::new(|| CommandSpec {
                    name: $cmd,
                    arity: $arity,
                    flags: $flags,
                    keys: KeySpec::None,
                    access: AccessSpec::Uniform,
                    wal: WalStrategy::NoOp,
                    wakes: WaiterWake::None,
                    event: EventSpec::NotApplicable,
                    requires_same_slot: false,
                    lookup: LookupSpec::None,
                    mutation: frogdb_core::ConnMutation::None,
                    strategy: ExecutionStrategy::Standard,
                });
                &SPEC
            }

            fn is_stub(&self) -> bool {
                true
            }

            fn execute(
                &self,
                _ctx: &mut CommandContext,
                _args: &[Bytes],
            ) -> Result<Response, CommandError> {
                Err(CommandError::NotImplemented { command: $cmd })
            }
        }
    };
}

// =============================================================================
// Replication Commands (not yet implemented)
// =============================================================================

stub_command!(
    WaitaofCommand,
    "WAITAOF",
    Arity::Fixed(3),
    CommandFlags::NOSCRIPT
);

// =============================================================================
// Module Commands
// =============================================================================

pub struct ModuleCommand;

impl Command for ModuleCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "MODULE",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::ADMIN.union(CommandFlags::NOSCRIPT),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn is_stub(&self) -> bool {
        true
    }

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if !args.is_empty() {
            let sub = args[0].to_ascii_uppercase();
            if sub.as_slice() == b"HELP" {
                let help = vec![
                    "MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "LIST",
                    "    Return a list of loaded modules.",
                    "LOAD <path> [<arg> ...]",
                    "    Load a module library from a dynamic library.",
                    "LOADEX <path> [CONFIG <name> <value> ...] [ARGS <arg> ...]",
                    "    Load a module library from a dynamic library.",
                    "UNLOAD <name>",
                    "    Unload a module.",
                    "HELP",
                    "    Return subcommand help summary.",
                ];
                return Ok(Response::Array(
                    help.into_iter().map(Response::bulk).collect(),
                ));
            }
        }
        Err(CommandError::NotImplemented { command: "MODULE" })
    }
}

// =============================================================================
// Commands intentionally not supported by FrogDB
// =============================================================================

/// Macro to generate commands that reject with NotSupported and a reason.
macro_rules! not_supported_command {
    ($name:ident, $cmd:literal, $arity:expr, $flags:expr, $reason:literal) => {
        pub struct $name;

        impl Command for $name {
            fn spec(&self) -> &'static CommandSpec {
                static SPEC: LazyLock<CommandSpec> = LazyLock::new(|| CommandSpec {
                    name: $cmd,
                    arity: $arity,
                    flags: $flags,
                    keys: KeySpec::None,
                    access: AccessSpec::Uniform,
                    wal: WalStrategy::NoOp,
                    wakes: WaiterWake::None,
                    event: EventSpec::NotApplicable,
                    requires_same_slot: false,
                    lookup: LookupSpec::None,
                    mutation: frogdb_core::ConnMutation::None,
                    strategy: ExecutionStrategy::Standard,
                });
                &SPEC
            }

            fn is_stub(&self) -> bool {
                true
            }

            fn execute(
                &self,
                _ctx: &mut CommandContext,
                _args: &[Bytes],
            ) -> Result<Response, CommandError> {
                Err(CommandError::NotSupported {
                    command: $cmd,
                    reason: $reason,
                })
            }
        }
    };
}

not_supported_command!(
    SaveCommand,
    "SAVE",
    Arity::Fixed(0),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT,
    "FrogDB uses continuous WAL persistence. Use BGSAVE for snapshots."
);

not_supported_command!(
    BgrewriteaofCommand,
    "BGREWRITEAOF",
    Arity::Fixed(0),
    CommandFlags::ADMIN,
    "FrogDB has no AOF. WAL compaction is handled automatically by RocksDB."
);

not_supported_command!(
    SyncCommand,
    "SYNC",
    Arity::Fixed(0),
    CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::READONLY,
    "Legacy replication protocol. Use PSYNC instead."
);

// =============================================================================
// Database-specifying commands (FrogDB is single-database-per-instance)
// =============================================================================

/// Macro to generate commands that reject with DatabaseNotSupported.
macro_rules! db_not_supported_command {
    ($name:ident, $cmd:literal, $arity:expr, $flags:expr) => {
        pub struct $name;

        impl Command for $name {
            fn spec(&self) -> &'static CommandSpec {
                // These are WRITE commands that reject before writing, so they
                // suppress notifications and persist nothing (see WAL allowlist).
                static SPEC: LazyLock<CommandSpec> = LazyLock::new(|| CommandSpec {
                    name: $cmd,
                    arity: $arity,
                    flags: $flags,
                    keys: KeySpec::None,
                    access: AccessSpec::Uniform,
                    wal: WalStrategy::NoOp,
                    wakes: WaiterWake::None,
                    event: EventSpec::Suppressed,
                    requires_same_slot: false,
                    lookup: LookupSpec::None,
                    mutation: frogdb_core::ConnMutation::None,
                    strategy: ExecutionStrategy::Standard,
                });
                &SPEC
            }

            fn is_stub(&self) -> bool {
                true
            }

            fn execute(
                &self,
                _ctx: &mut CommandContext,
                _args: &[Bytes],
            ) -> Result<Response, CommandError> {
                Err(CommandError::DatabaseNotSupported { command: $cmd })
            }
        }
    };
}

db_not_supported_command!(
    MoveCommand,
    "MOVE",
    Arity::Fixed(2),
    CommandFlags::WRITE | CommandFlags::FAST
);

db_not_supported_command!(
    SwapdbCommand,
    "SWAPDB",
    Arity::Fixed(2),
    CommandFlags::WRITE | CommandFlags::FAST
);

/// SELECT 0 is accepted as a no-op (returns OK).
/// SELECT <non-zero> returns DatabaseNotSupported.
pub struct SelectCommand;

impl Command for SelectCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SELECT",
            arity: Arity::Fixed(1),
            flags: CommandFlags::FAST
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn is_stub(&self) -> bool {
        true
    }

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args[0].as_ref() == b"0" {
            Ok(Response::ok())
        } else {
            // Match Redis error format so scripts can check for "DB index"
            Err(CommandError::InvalidArgument {
                message: "DB index is out of range".to_string(),
            })
        }
    }
}

// =============================================================================
// List Commands (deprecated)
// =============================================================================

// RPOPLPUSH moved to frogdb_commands::list::RpoplpushCommand
