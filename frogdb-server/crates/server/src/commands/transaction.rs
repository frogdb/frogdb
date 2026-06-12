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
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

// ============================================================================
// MULTI - Start a transaction block
// ============================================================================

pub struct MultiCommand;

impl Command for MultiCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "MULTI",
            arity: Arity::Fixed(0),
            flags: CommandFlags::FAST
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// EXEC - Execute all queued commands in a transaction
// ============================================================================

pub struct ExecCommand;

impl Command for ExecCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "EXEC",
            arity: Arity::Fixed(0),
            flags: CommandFlags::LOADING.union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// DISCARD - Abort the transaction and clear the command queue
// ============================================================================

pub struct DiscardCommand;

impl Command for DiscardCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "DISCARD",
            arity: Arity::Fixed(0),
            flags: CommandFlags::FAST
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// WATCH - Watch keys for modifications (optimistic locking)
// ============================================================================

pub struct WatchCommand;

impl Command for WatchCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "WATCH",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::FAST
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// UNWATCH - Forget all watched keys
// ============================================================================

pub struct UnwatchCommand;

impl Command for UnwatchCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "UNWATCH",
            arity: Arity::Fixed(0),
            flags: CommandFlags::FAST
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}
