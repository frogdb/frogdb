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
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// MEMORY command - memory introspection and diagnostics.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since MEMORY needs scatter-gather across shards or key-based routing.
pub struct MemoryCommand;

impl Command for MemoryCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "MEMORY",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY.union(CommandFlags::RANDOM),
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
}
