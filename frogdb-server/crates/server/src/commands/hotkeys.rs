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
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// HOTKEYS command - hot key detection via sampling.
/// Note: This is a stub implementation. The actual logic is in the
/// connection handler since HOTKEYS manages a server-global session.
pub struct HotkeysCommand;

impl Command for HotkeysCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HOTKEYS",
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
        // This should not be called - HOTKEYS is handled specially in connection handler
        Err(CommandError::InvalidArgument {
            message: "HOTKEYS command should be handled by connection handler".to_string(),
        })
    }
}
