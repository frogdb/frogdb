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
    fn name(&self) -> &'static str {
        "MODULE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
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
                Err(CommandError::NotSupported {
                    command: $cmd,
                    reason: $reason,
                })
            }

            fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
                vec![]
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
                Err(CommandError::DatabaseNotSupported { command: $cmd })
            }

            fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
                vec![]
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
    fn name(&self) -> &'static str {
        "SELECT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// List Commands (deprecated)
// =============================================================================

// RPOPLPUSH moved to frogdb_commands::list::RpoplpushCommand
