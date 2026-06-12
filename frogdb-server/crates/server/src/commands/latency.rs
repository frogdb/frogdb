//! LATENCY command.
//!
//! This command is handled specially in the connection handler since it
//! needs to scatter-gather across all shards.
//!
//! Supported subcommands:
//! - LATENCY DOCTOR: Diagnose latency issues
//! - LATENCY GRAPH <event>: Show ASCII latency graph
//! - LATENCY HELP: Show subcommand help
//! - LATENCY HISTOGRAM [command...]: Show command latency histogram
//! - LATENCY HISTORY <event>: Get historical latency data
//! - LATENCY LATEST: Get latest latency samples
//! - LATENCY RESET [event...]: Clear latency data

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// LATENCY command - latency monitoring and diagnostics.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since LATENCY needs scatter-gather across all shards.
pub struct LatencyCommand;

impl Command for LatencyCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "LATENCY",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::ADMIN
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        Some(&SPEC)
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        // LATENCY is handled at connection level (scatter-gather across shards)
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - LATENCY is handled specially in connection.rs
        Err(CommandError::InvalidArgument {
            message: "LATENCY command should be handled by connection handler".to_string(),
        })
    }
}
