//! Transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
//!
//! These commands implement Redis-compatible MULTI/EXEC transactions with
//! WATCH-based optimistic locking. Transactions are single-shard only in
//! Phase 7.1 (keys must hash to the same slot).
//!
//! Note: The actual transaction logic is handled in the connection handler,
//! not in these command implementations. These structs exist primarily for
//! command registration and metadata (arity, flags, key extraction).

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;

// ============================================================================
// MULTI - Start a transaction block
// ============================================================================

pub struct MultiCommand;

impl Command for MultiCommand {
    fn name(&self) -> &'static str {
        "MULTI"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Actual handling is done in ConnectionHandler
        // This should not be called directly
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}

// ============================================================================
// EXEC - Execute all queued commands in a transaction
// ============================================================================

pub struct ExecCommand;

impl Command for ExecCommand {
    fn name(&self) -> &'static str {
        "EXEC"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        // EXEC is not FAST because it may execute multiple commands
        CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Actual handling is done in ConnectionHandler
        // This should not be called directly
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command (keys are in the queued commands)
    }
}

// ============================================================================
// DISCARD - Abort the transaction and clear the command queue
// ============================================================================

pub struct DiscardCommand;

impl Command for DiscardCommand {
    fn name(&self) -> &'static str {
        "DISCARD"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Actual handling is done in ConnectionHandler
        // This should not be called directly
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}

// ============================================================================
// WATCH - Watch keys for modifications (optimistic locking)
// ============================================================================

pub struct WatchCommand;

impl Command for WatchCommand {
    fn name(&self) -> &'static str {
        "WATCH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Actual handling is done in ConnectionHandler
        // This should not be called directly
        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // All arguments are keys to watch
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// UNWATCH - Forget all watched keys
// ============================================================================

pub struct UnwatchCommand;

impl Command for UnwatchCommand {
    fn name(&self) -> &'static str {
        "UNWATCH"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Actual handling is done in ConnectionHandler
        // This should not be called directly
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless command
    }
}
