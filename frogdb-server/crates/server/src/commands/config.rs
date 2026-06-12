//! CONFIG commands.
//!
//! These commands are handled specially in the connection handler since they
//! need access to the ConfigManager which is not part of CommandContext.
//!
//! Supported commands:
//! - CONFIG GET <pattern>: Return parameters matching the pattern
//! - CONFIG SET <param> <value>: Set a mutable configuration parameter
//! - CONFIG HELP: Print help information

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// CONFIG command - configuration management commands.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since CONFIG commands need access to the ConfigManager.
pub struct ConfigCommand;

impl Command for ConfigCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "CONFIG",
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
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - CONFIG is handled specially in connection.rs
        Err(CommandError::InvalidArgument {
            message: "CONFIG command should be handled by connection handler".to_string(),
        })
    }
}
