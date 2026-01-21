//! HELLO command.
//!
//! The HELLO command is used for protocol negotiation (RESP2/RESP3).
//! It is handled specially by the connection handler, not executed by shards.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

/// HELLO command for protocol negotiation.
///
/// This is a stub - the actual implementation is in the connection handler
/// because it needs to modify connection state directly.
pub struct HelloCommand;

impl Command for HelloCommand {
    fn name(&self) -> &'static str {
        "HELLO"
    }

    fn arity(&self) -> Arity {
        // HELLO [protover [AUTH username password] [SETNAME clientname]]
        Arity::AtLeast(0)
    }

    fn flags(&self) -> CommandFlags {
        // Fast, no-script, connection-specific command
        CommandFlags::FAST | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should never be called - HELLO is intercepted by the connection handler
        Err(CommandError::InvalidArgument {
            message: "HELLO command must be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // No keys
        vec![]
    }
}
