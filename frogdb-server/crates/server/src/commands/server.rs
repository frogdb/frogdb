//! Server commands.
//!
//! Commands for server management:
//! - DBSIZE: Return the number of keys
//! - FLUSHDB: Remove all keys from the current database
//! - FLUSHALL: Remove all keys from all databases
//! - TIME: Return the server time
//! - SHUTDOWN: Gracefully shut down the server

use bytes::Bytes;
use frogdb_core::clock;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, ServerWideOp, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use std::time::UNIX_EPOCH;

// ============================================================================
// DBSIZE - Return key count
// ============================================================================

pub struct DbsizeCommand;

impl Command for DbsizeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "DBSIZE",
            docs: frogdb_core::CommandDocs {
                summary: "Returns the number of keys in the database.",
                since: "1.0.0",
                group: "server",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::DbSize),
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, _args: &[Bytes]) -> Result<Response, CommandError> {
        let count = ctx.store.len();
        Ok(Response::Integer(count as i64))
    }
}

// ============================================================================
// FLUSHDB - Clear all keys in database
// ============================================================================

pub struct FlushdbCommand;

impl Command for FlushdbCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FLUSHDB",
            docs: frogdb_core::CommandDocs {
                summary: "Remove all keys from the current database.",
                since: "1.0.0",
                group: "server",
                complexity: Some("O(N) where N is the number of keys in the selected database"),
            },
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::ClearShard,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FlushDb),
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Parse optional ASYNC/SYNC argument
        // Note: We only support SYNC for now
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            match mode.as_slice() {
                b"ASYNC" | b"SYNC" => {
                    // Accept but treat both as SYNC for now
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        // Clear local shard
        // In broadcast mode, connection.rs will send to all shards
        ctx.store.clear();
        Ok(Response::ok())
    }
}

// ============================================================================
// FLUSHALL - Clear all keys in all databases
// ============================================================================

pub struct FlushallCommand;

impl Command for FlushallCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FLUSHALL",
            docs: frogdb_core::CommandDocs {
                summary: "Removes all keys from all databases.",
                since: "1.0.0",
                group: "server",
                complexity: Some("O(N) where N is the total number of keys in all databases"),
            },
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::ClearShard,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FlushAll),
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Same as FLUSHDB since we only have one database
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            match mode.as_slice() {
                b"ASYNC" | b"SYNC" => {
                    // Accept but treat both as SYNC for now
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        ctx.store.clear();
        Ok(Response::ok())
    }
}

// ============================================================================
// TIME - Return server time
// ============================================================================

pub struct TimeCommand;

impl Command for TimeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "TIME",
            docs: frogdb_core::CommandDocs {
                summary: "Returns the server time.",
                since: "2.6.0",
                group: "server",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY
                .union(CommandFlags::FAST)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let now = clock::system_now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let secs = now.as_secs();
        let micros = now.subsec_micros();

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(secs.to_string())),
            Response::bulk(Bytes::from(micros.to_string())),
        ]))
    }
}

// ============================================================================
// SHUTDOWN - Gracefully shutdown the server
// ============================================================================

pub struct ShutdownCommand;

impl Command for ShutdownCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SHUTDOWN",
            docs: frogdb_core::CommandDocs {
                summary: "Synchronously saves the database(s) to disk and shuts down the Redis server.",
                since: "1.0.0",
                group: "server",
                complexity: Some(
                    "O(N) when saving, where N is the total number of keys in all databases when saving data, otherwise O(1)",
                ),
            },
            arity: Arity::Range { min: 0, max: 2 },
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
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::Shutdown),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide
        // (handle_shutdown), never on a shard. Reaching this shard-side
        // executor is a routing regression -- fail loudly rather than
        // fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// ============================================================================
// LOLWUT - Display frog art
// ============================================================================

const FROG_ART: &str = include_str!("frog-art.txt");

pub struct LolwutCommand;

impl Command for LolwutCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LOLWUT",
            docs: frogdb_core::CommandDocs {
                summary: "Displays computer art and the Redis version",
                since: "5.0.0",
                group: "server",
                complexity: None,
            },
            arity: Arity::AtLeast(0),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::bulk(Bytes::from(FROG_ART)))
    }
}
