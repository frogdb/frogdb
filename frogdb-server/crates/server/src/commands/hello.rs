//! HELLO command.
//!
//! The HELLO command is used for protocol negotiation (RESP2/RESP3).
//! It is handled specially by the connection handler, not executed by shards.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// HELLO command for protocol negotiation.
///
/// This is a stub - the actual implementation is in the connection handler
/// because it needs to modify connection state directly.
pub struct HelloCommand;

impl Command for HelloCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HELLO",
            arity: Arity::AtLeast(0),
            flags: CommandFlags::FAST
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should never be called - HELLO is intercepted by the connection handler
        Err(CommandError::InvalidArgument {
            message: "HELLO command must be handled by connection handler".to_string(),
        })
    }
}
