//! ACL command implementations.
//!
//! Note: ACL commands are handled specially in connection.rs because they
//! need access to the AclManager. This module provides command registration
//! for help/documentation purposes.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

/// ACL command - manage access control lists.
///
/// Subcommands:
/// - ACL SETUSER <username> [rules...] - Create/modify user
/// - ACL DELUSER <username> [...]      - Delete users
/// - ACL LIST                          - List all users with rules
/// - ACL GETUSER <username>            - Get user configuration
/// - ACL USERS                         - List all usernames
/// - ACL CAT [category]                - List categories or commands in category
/// - ACL WHOAMI                        - Return current username
/// - ACL GENPASS [bits]                - Generate secure random password
/// - ACL LOG [count|RESET]             - View/reset security log
/// - ACL SAVE                          - Persist to aclfile
/// - ACL LOAD                          - Reload from aclfile
/// - ACL HELP                          - Show help
pub struct Acl;

impl Command for Acl {
    fn name(&self) -> &'static str {
        "ACL"
    }

    fn arity(&self) -> Arity {
        // ACL requires at least one subcommand
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        // ACL is an admin command
        CommandFlags::ADMIN
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        // ACL is handled at connection level (needs access to AclManager)
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // This should never be called - ACL is handled in connection.rs
        Err(CommandError::Internal {
            message: "ACL should be handled by connection handler".to_string(),
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // ACL is a keyless command
        vec![]
    }
}
