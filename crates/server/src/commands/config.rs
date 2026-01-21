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
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

/// CONFIG command - configuration management commands.
/// Note: This is a stub implementation. The actual logic is in connection.rs
/// since CONFIG commands need access to the ConfigManager.
pub struct ConfigCommand;

impl Command for ConfigCommand {
    fn name(&self) -> &'static str {
        "CONFIG"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // CONFIG <subcommand> [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
