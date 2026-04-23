//! HOTKEYS command.
//!
//! This command is handled at the connection level since it manages a
//! server-global sampling session shared across all connections.
//!
//! Supported subcommands:
//! - HOTKEYS START METRICS <count> <metric...> [COUNT n] [DURATION ms] [SAMPLE ratio] [SLOTS count slot...]
//! - HOTKEYS STOP: Stop an active session
//! - HOTKEYS RESET: Reset a stopped session to idle
//! - HOTKEYS GET: Retrieve session data

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// HOTKEYS command - hot key detection via sampling.
/// Note: This is a stub implementation. The actual logic is in the
/// connection handler since HOTKEYS manages a server-global session.
pub struct HotkeysCommand;

impl Command for HotkeysCommand {
    fn name(&self) -> &'static str {
        "HOTKEYS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // HOTKEYS <subcommand> [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should not be called - HOTKEYS is handled specially in connection handler
        Err(CommandError::InvalidArgument {
            message: "HOTKEYS command should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
