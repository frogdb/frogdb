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
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// LATENCY command - latency monitoring and diagnostics.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since LATENCY needs scatter-gather across all shards.
pub struct LatencyCommand;

impl Command for LatencyCommand {
    fn name(&self) -> &'static str {
        "LATENCY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // LATENCY <subcommand> [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
