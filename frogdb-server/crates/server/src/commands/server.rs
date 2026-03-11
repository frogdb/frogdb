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
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, ServerWideOp,
    WalStrategy,
};
use frogdb_protocol::Response;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// DBSIZE - Return key count
// ============================================================================

pub struct DbsizeCommand;

impl Command for DbsizeCommand {
    fn name(&self) -> &'static str {
        "DBSIZE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::DbSize)
    }

    fn execute(&self, ctx: &mut CommandContext, _args: &[Bytes]) -> Result<Response, CommandError> {
        let count = ctx.store.len();
        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless - scatter-gather
    }
}

// ============================================================================
// FLUSHDB - Clear all keys in database
// ============================================================================

pub struct FlushdbCommand;

impl Command for FlushdbCommand {
    fn name(&self) -> &'static str {
        "FLUSHDB"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 0, max: 1 } // FLUSHDB [ASYNC|SYNC]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FlushDb)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless - broadcast to all shards
    }
}

// ============================================================================
// FLUSHALL - Clear all keys in all databases
// ============================================================================

pub struct FlushallCommand;

impl Command for FlushallCommand {
    fn name(&self) -> &'static str {
        "FLUSHALL"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 0, max: 1 } // FLUSHALL [ASYNC|SYNC]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FlushAll)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless - broadcast to all shards
    }
}

// ============================================================================
// TIME - Return server time
// ============================================================================

pub struct TimeCommand;

impl Command for TimeCommand {
    fn name(&self) -> &'static str {
        "TIME"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless - local execution
    }
}

// ============================================================================
// SHUTDOWN - Gracefully shutdown the server
// ============================================================================

pub struct ShutdownCommand;

impl Command for ShutdownCommand {
    fn name(&self) -> &'static str {
        "SHUTDOWN"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 0, max: 2 } // SHUTDOWN [NOSAVE|SAVE] [NOW]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::Shutdown)
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless
    }
}

// ============================================================================
// LOLWUT - Display frog art
// ============================================================================

const FROG_ART: &str = include_str!("frog-art.txt");

pub struct LolwutCommand;

impl Command for LolwutCommand {
    fn name(&self) -> &'static str {
        "LOLWUT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(0) // LOLWUT [VERSION version]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        Ok(Response::bulk(Bytes::from(FROG_ART)))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}
