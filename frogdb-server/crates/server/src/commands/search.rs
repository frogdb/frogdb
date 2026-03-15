//! FT.* search command definitions.
//!
//! These commands use the ServerWide execution strategy. The actual logic
//! lives in the scatter handlers; the `execute()` method is never called.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, ServerWideOp,
    WalStrategy,
};
use frogdb_protocol::Response;

// =============================================================================
// FT.CREATE
// =============================================================================

/// FT.CREATE index ON HASH [PREFIX count prefix ...] SCHEMA field type [options] ...
pub struct FtCreateCommand;

impl Command for FtCreateCommand {
    fn name(&self) -> &'static str {
        "FT.CREATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // FT.CREATE idx ON HASH SCHEMA ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtCreate)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.ALTER
// =============================================================================

/// FT.ALTER index SCHEMA ADD field type [options] ...
pub struct FtAlterCommand;

impl Command for FtAlterCommand {
    fn name(&self) -> &'static str {
        "FT.ALTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // FT.ALTER idx SCHEMA ADD field type
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtAlter)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SEARCH
// =============================================================================

/// FT.SEARCH index query [NOCONTENT] [WITHSCORES] [LIMIT offset num]
///   [RETURN count field ...] [SORTBY field [ASC|DESC]]
pub struct FtSearchCommand;

impl Command for FtSearchCommand {
    fn name(&self) -> &'static str {
        "FT.SEARCH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // FT.SEARCH idx query
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtSearch)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.DROPINDEX
// =============================================================================

/// FT.DROPINDEX index [DD]
pub struct FtDropIndexCommand;

impl Command for FtDropIndexCommand {
    fn name(&self) -> &'static str {
        "FT.DROPINDEX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // FT.DROPINDEX idx
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtDropIndex)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.INFO
// =============================================================================

/// FT.INFO index
pub struct FtInfoCommand;

impl Command for FtInfoCommand {
    fn name(&self) -> &'static str {
        "FT.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // FT.INFO idx
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtInfo)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT._LIST
// =============================================================================

/// FT._LIST
pub struct FtListCommand;

impl Command for FtListCommand {
    fn name(&self) -> &'static str {
        "FT._LIST"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0) // FT._LIST
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtList)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SYNUPDATE
// =============================================================================

/// FT.SYNUPDATE index group_id [SKIPINITIALSCAN] term1 term2 ...
pub struct FtSynupdateCommand;

impl Command for FtSynupdateCommand {
    fn name(&self) -> &'static str {
        "FT.SYNUPDATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // FT.SYNUPDATE idx group_id term1
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtSynupdate)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SYNDUMP
// =============================================================================

/// FT.SYNDUMP index
pub struct FtSyndumpCommand;

impl Command for FtSyndumpCommand {
    fn name(&self) -> &'static str {
        "FT.SYNDUMP"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // FT.SYNDUMP idx
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtSyndump)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}
