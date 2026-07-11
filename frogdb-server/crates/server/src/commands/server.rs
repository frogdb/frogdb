//! Server commands.
//!
//! Commands for server management:
//! - DBSIZE: Return the number of keys
//! - FLUSHDB: Remove all keys from the current database
//! - FLUSHALL: Remove all keys from all databases
//! - TIME: Return the server time
//! - SHUTDOWN: Gracefully shut down the server

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// DBSIZE - Return key count
// ============================================================================

pub struct DbsizeCommand;

impl Command for DbsizeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "DBSIZE",
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
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
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::ClearShard,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
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
            arity: Arity::Range { min: 0, max: 1 },
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::ClearShard,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
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
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let now = SystemTime::now()
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
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Parse optional NOSAVE/SAVE and NOW arguments
        // Note: Actual shutdown is handled by the connection handler
        // This command just returns OK; the caller (connection.rs) should trigger shutdown

        // In a real implementation, this would trigger a graceful shutdown
        // For now, we return an error indicating that shutdown should be handled specially
        Err(CommandError::InvalidArgument {
            message: "SHUTDOWN command should be handled by connection handler".to_string(),
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
            arity: Arity::AtLeast(0),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
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
