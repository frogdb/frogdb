//! MEMORY command.
//!
//! This command is handled specially in the connection handler since it
//! needs to scatter-gather across all shards for MEMORY STATS, or route
//! MEMORY USAGE to a specific shard based on the key.
//!
//! Supported subcommands:
//! - MEMORY DOCTOR: Diagnose memory issues
//! - MEMORY HELP: Show subcommand help
//! - MEMORY MALLOC-SIZE <size>: Get allocator usable size (stub)
//! - MEMORY PURGE: Force memory purge (stub)
//! - MEMORY STATS: Get detailed memory statistics
//! - MEMORY USAGE <key> [SAMPLES count]: Get memory usage for a key

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// MEMORY command - memory introspection and diagnostics.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since MEMORY needs scatter-gather across shards or key-based routing.
pub struct MemoryCommand;

impl Command for MemoryCommand {
    fn name(&self) -> &'static str {
        "MEMORY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // MEMORY <subcommand> [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::RANDOM
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        // MEMORY is handled at connection level (scatter-gather for STATS, key-based for USAGE)
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - MEMORY is handled specially in connection.rs
        Err(CommandError::InvalidArgument {
            message: "MEMORY command should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command (MEMORY USAGE extracts key from args in the handler)
    }
}
