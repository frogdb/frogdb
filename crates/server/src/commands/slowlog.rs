//! SLOWLOG command.
//!
//! This command is handled specially in the connection handler since it
//! needs to scatter-gather across all shards.
//!
//! Supported subcommands:
//! - SLOWLOG GET [count]: Get recent slow queries
//! - SLOWLOG LEN: Get total number of entries
//! - SLOWLOG RESET: Clear the slow log
//! - SLOWLOG HELP: Show help

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

/// SLOWLOG command - slow query log management.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since SLOWLOG needs scatter-gather across all shards.
pub struct SlowlogCommand;

impl Command for SlowlogCommand {
    fn name(&self) -> &'static str {
        "SLOWLOG"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SLOWLOG <subcommand> [args...]
    }

    fn flags(&self) -> CommandFlags {
        // SKIP_SLOWLOG prevents the SLOWLOG command itself from being logged
        CommandFlags::READONLY
            | CommandFlags::ADMIN
            | CommandFlags::FAST
            | CommandFlags::SKIP_SLOWLOG
            | CommandFlags::LOADING
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - SLOWLOG is handled specially in connection.rs
        Err(CommandError::InvalidArgument {
            message: "SLOWLOG command should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
