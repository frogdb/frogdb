//! AUTH command implementation.
//!
//! Note: The AUTH command is handled specially in connection.rs because it
//! needs access to the AclManager and connection state. This module provides
//! the command registration for help/documentation purposes.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// AUTH command - authenticate to the server.
///
/// Syntax:
/// - AUTH <password>           - Authenticate with default user
/// - AUTH <username> <password> - Authenticate with named user
pub struct Auth;

impl Command for Auth {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "AUTH",
            arity: Arity::Range { min: 1, max: 2 },
            flags: CommandFlags::FAST,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth),
        };
        &SPEC
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
}
