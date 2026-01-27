//! CLIENT commands.
//!
//! These commands are handled specially in the connection handler since they
//! need access to the ClientRegistry which is not part of CommandContext.
//!
//! Supported commands:
//! - CLIENT ID: Return the connection ID
//! - CLIENT SETNAME: Set a connection name
//! - CLIENT GETNAME: Get the connection name
//! - CLIENT LIST: List all connections
//! - CLIENT INFO: Get current connection info
//! - CLIENT KILL: Terminate connections
//! - CLIENT PAUSE: Pause command processing
//! - CLIENT UNPAUSE: Resume command processing

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// CLIENT command - connection management commands.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since CLIENT commands need access to the ClientRegistry.
pub struct ClientCommand;

impl Command for ClientCommand {
    fn name(&self) -> &'static str {
        "CLIENT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // CLIENT <subcommand> [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - CLIENT is handled specially in connection.rs
        Err(CommandError::InvalidArgument {
            message: "CLIENT command should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
