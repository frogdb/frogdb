//! AUTH command implementation.
//!
//! Note: The AUTH command is handled specially in connection.rs because it
//! needs access to the AclManager and connection state. This module provides
//! the command registration for help/documentation purposes.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// AUTH command - authenticate to the server.
///
/// Syntax:
/// - AUTH <password>           - Authenticate with default user
/// - AUTH <username> <password> - Authenticate with named user
pub struct Auth;

impl Command for Auth {
    fn name(&self) -> &'static str {
        "AUTH"
    }

    fn arity(&self) -> Arity {
        // AUTH can have 1 or 2 arguments
        Arity::Range { min: 1, max: 2 }
    }

    fn flags(&self) -> CommandFlags {
        // AUTH is a fast connection command that doesn't require authentication
        CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should never be called - AUTH is handled in connection.rs
        Err(CommandError::Internal {
            message: "AUTH should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // AUTH is a keyless command
        vec![]
    }
}
