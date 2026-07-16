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
        // Key selection follows the shared MIGRATE grammar walker so the
        // dispatcher (slot validation, ACL checks, locking) always guards the
        // exact key set MigrateArgs::parse migrates.
        crate::migrate::key_positions(args)
            .into_iter()
            .map(|i| args[i].as_ref())
            .collect()
    }
}
