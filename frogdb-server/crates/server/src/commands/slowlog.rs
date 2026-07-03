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
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// SLOWLOG command - slow query log management.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since SLOWLOG needs scatter-gather across all shards.
pub struct SlowlogCommand;

impl Command for SlowlogCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SLOWLOG",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY
                .union(CommandFlags::ADMIN)
                .union(CommandFlags::FAST)
                .union(CommandFlags::SKIP_SLOWLOG)
                .union(CommandFlags::LOADING),
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
        // SLOWLOG is handled at connection level (scatter-gather across shards)
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
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
}
