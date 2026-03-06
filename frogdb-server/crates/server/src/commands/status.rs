//! STATUS command.
//!
//! This command is handled specially in the connection handler since it
//! needs to collect status from various server components.
//!
//! Supported subcommands:
//! - STATUS JSON: Returns machine-readable server status as JSON
//! - STATUS HELP: Show subcommand help

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// STATUS command - server status and health information.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since STATUS needs to collect data from various server components.
pub struct StatusCommand;

impl Command for StatusCommand {
    fn name(&self) -> &'static str {
        "STATUS"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 0, max: 1 } // STATUS or STATUS <subcommand>
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        // STATUS is handled at connection level (collects data from various components)
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - STATUS is handled specially in connection.rs
        Err(CommandError::InvalidArgument {
            message: "STATUS command should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
