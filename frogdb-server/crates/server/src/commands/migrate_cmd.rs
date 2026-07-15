//! MIGRATE command implementation.
//!
//! MIGRATE moves keys atomically from source to target Redis server.
//! Since this requires async network I/O and the command interface is synchronous,
//! this command is handled specially by the connection handler.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, ServerWideOp, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// MIGRATE command - move keys to another Redis instance.
///
/// Format: MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key...]
pub struct MigrateCommand;

impl Command for MigrateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "MIGRATE",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::Migrate),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (handle_migrate
        // does its own parsing and async network I/O), never on a shard.
        // Reaching this shard-side executor is a routing regression (or a Lua
        // redis.call, which cannot perform the async migration) -- fail loudly
        // rather than leak an internal MigrateNeeded signal.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
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
                for arg in args[(i + 1)..].iter() {
                    keys.push(arg.as_ref());
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
