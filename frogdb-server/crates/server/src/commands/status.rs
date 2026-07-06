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
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// STATUS command - server status and health information.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since STATUS needs to collect data from various server components.
pub struct StatusCommand;

impl Command for StatusCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "STATUS",
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::READONLY
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE)
                .union(CommandFlags::FAST),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
        };
        &SPEC
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
}
