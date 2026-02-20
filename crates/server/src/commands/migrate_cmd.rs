//! MIGRATE command implementation.
//!
//! MIGRATE moves keys atomically from source to target Redis server.
//! Since this requires async network I/O and the command interface is synchronous,
//! this command is handled specially by the connection handler.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use crate::migrate::MigrateArgs;

/// MIGRATE command - move keys to another Redis instance.
///
/// Format: MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key...]
pub struct MigrateCommand;

impl Command for MigrateCommand {
    fn name(&self) -> &'static str {
        "MIGRATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5)
    }

    fn flags(&self) -> CommandFlags {
        // MIGRATE modifies data (deletes source key unless COPY) and is slow
        CommandFlags::WRITE | CommandFlags::NOSCRIPT
    }

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Parse arguments to validate them
        MigrateArgs::parse(args).map_err(|e| CommandError::InvalidArgument { message: e })?;

        // Return a special response indicating async migration is needed
        // The connection handler will intercept this and perform the migration
        Ok(Response::MigrateNeeded {
            args: args.to_vec(),
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Extract keys from the command
        // Format: MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key...]
        let mut keys = Vec::new();

        if args.len() >= 3 {
            let key = &args[2];
            if !key.is_empty() {
                keys.push(key.as_ref());
            }
        }

        // Look for KEYS argument
        let mut i = 5;
        while i < args.len() {
            if args[i].to_ascii_uppercase() == b"KEYS" {
                for j in (i + 1)..args.len() {
                    keys.push(args[j].as_ref());
                }
                break;
            }
            // Skip AUTH and AUTH2 arguments
            let arg = args[i].to_ascii_uppercase();
            if arg == b"AUTH" {
                i += 2;
            } else if arg == b"AUTH2" {
                i += 3;
            } else {
                i += 1;
            }
        }

        keys
    }
}
