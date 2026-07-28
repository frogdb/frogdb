//! FT.* search command definitions.
//!
//! ServerWide commands (FT.CREATE, FT.SEARCH, etc.) never execute on a shard;
//! the actual logic lives in scatter handlers reached via
//! `ConnectionHandler::dispatch_server_wide`. Their `execute()` methods return
//! a loud internal error so a routing regression yields an ERR reply instead
//! of a fabricated success.
//!
//! Key-based commands (FT.SUGADD, FT.SUGGET, etc.) use Standard execution and
//! implement their logic directly in execute().

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, HashValue, KeySpec, LookupSpec, ServerWideOp, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

// =============================================================================
// FT.CREATE
// =============================================================================

/// FT.CREATE index ON HASH [PREFIX count prefix ...] SCHEMA field type [options] ...
pub struct FtCreateCommand;

impl Command for FtCreateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.CREATE",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtCreate),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.ALTER
// =============================================================================

/// FT.ALTER index SCHEMA ADD field type [options] ...
pub struct FtAlterCommand;

impl Command for FtAlterCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.ALTER",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtAlter),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.SEARCH
// =============================================================================

/// FT.SEARCH index query [NOCONTENT] [WITHSCORES] [LIMIT offset num]
///   [RETURN count field ...] [SORTBY field [ASC|DESC]]
pub struct FtSearchCommand;

impl Command for FtSearchCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SEARCH",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtSearch),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.DROPINDEX
// =============================================================================

/// FT.DROPINDEX index [DD]
pub struct FtDropIndexCommand;

impl Command for FtDropIndexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.DROPINDEX",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtDropIndex),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.INFO
// =============================================================================

/// FT.INFO index
pub struct FtInfoCommand;

impl Command for FtInfoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.INFO",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtInfo),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT._LIST
// =============================================================================

/// FT._LIST
pub struct FtListCommand;

impl Command for FtListCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT._LIST",
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtList),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.AGGREGATE
// =============================================================================

/// FT.AGGREGATE index query [GROUPBY nargs @field ... REDUCE func nargs @arg ... [AS alias] ...]
///   [SORTBY nargs @field [ASC|DESC] ...] [LIMIT offset count]
pub struct FtAggregateCommand;

impl Command for FtAggregateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.AGGREGATE",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtAggregate),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.HYBRID
// =============================================================================

/// FT.HYBRID index SEARCH query VSIM @field $param [COMBINE RRF|LINEAR count ...]
///   [LIMIT offset num] [SORTBY field [ASC|DESC]] [NOSORT]
///   [LOAD count field ...] [GROUPBY ...] [APPLY ...] [PARAMS nargs key value ...]
pub struct FtHybridCommand;

impl Command for FtHybridCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.HYBRID",
            arity: Arity::AtLeast(6),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtHybrid),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.SYNUPDATE
// =============================================================================

/// FT.SYNUPDATE index group_id [SKIPINITIALSCAN] term1 term2 ...
pub struct FtSynupdateCommand;

impl Command for FtSynupdateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SYNUPDATE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtSynUpdate),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.SYNDUMP
// =============================================================================

/// FT.SYNDUMP index
pub struct FtSyndumpCommand;

impl Command for FtSyndumpCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SYNDUMP",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtSynDump),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.SUGADD
// =============================================================================

// =============================================================================
// FT.ALIASADD
// =============================================================================

pub struct FtAliasaddCommand;

impl Command for FtAliasaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.ALIASADD",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtAliasAdd),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.ALIASDEL
// =============================================================================

pub struct FtAliasdelCommand;

impl Command for FtAliasdelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.ALIASDEL",
            arity: Arity::Fixed(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtAliasDel),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.ALIASUPDATE
// =============================================================================

pub struct FtAliasupdateCommand;

impl Command for FtAliasupdateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.ALIASUPDATE",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtAliasUpdate),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.TAGVALS
// =============================================================================

pub struct FtTagvalsCommand;

impl Command for FtTagvalsCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.TAGVALS",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtTagVals),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.DICTADD
// =============================================================================

pub struct FtDictaddCommand;

impl Command for FtDictaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.DICTADD",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtDictAdd),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.DICTDEL
// =============================================================================

pub struct FtDictdelCommand;

impl Command for FtDictdelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.DICTDEL",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtDictDel),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.DICTDUMP
// =============================================================================

pub struct FtDictdumpCommand;

impl Command for FtDictdumpCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.DICTDUMP",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtDictDump),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.CONFIG
// =============================================================================

pub struct FtConfigCommand;

impl Command for FtConfigCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.CONFIG",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtConfig),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.SPELLCHECK
// =============================================================================

pub struct FtSpellcheckCommand;

impl Command for FtSpellcheckCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SPELLCHECK",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtSpellCheck),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.SUGADD
// =============================================================================

const PAYLOAD_PREFIX: &[u8] = b"__pl__";

/// FT.SUGADD key string score [INCR] [PAYLOAD payload]
pub struct FtSugaddCommand;

impl Command for FtSugaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SUGADD",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 3 {
            return Err(CommandError::WrongArity {
                command: "FT.SUGADD",
            });
        }

        let key = &args[0];
        let suggestion =
            std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
                message: "invalid suggestion string".into(),
            })?;
        let score: f64 = std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| CommandError::InvalidArgument {
                message: "invalid score".into(),
            })?;

        let mut incr = false;
        let mut payload: Option<&[u8]> = None;
        let mut i = 3;
        while i < args.len() {
            let arg_upper = args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"INCR" => {
                    incr = true;
                    i += 1;
                }
                b"PAYLOAD" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    payload = Some(&args[i + 1]);
                    i += 2;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        let hash = ctx.get_or_create::<HashValue>(key)?;

        let new_score = if incr {
            let existing: f64 = hash
                .get(suggestion.as_bytes())
                .and_then(|v| std::str::from_utf8(&v).ok().map(|s| s.to_owned()))
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            existing + score
        } else {
            score
        };

        hash.set(
            Bytes::copy_from_slice(suggestion.as_bytes()),
            Bytes::copy_from_slice(new_score.to_string().as_bytes()),
            frogdb_core::ListpackThresholds::DEFAULT_HASH,
        );

        if let Some(pl) = payload {
            let mut pl_key = Vec::with_capacity(PAYLOAD_PREFIX.len() + suggestion.len());
            pl_key.extend_from_slice(PAYLOAD_PREFIX);
            pl_key.extend_from_slice(suggestion.as_bytes());
            hash.set(
                Bytes::from(pl_key),
                Bytes::copy_from_slice(pl),
                frogdb_core::ListpackThresholds::DEFAULT_HASH,
            );
        }

        let count = hash
            .iter()
            .filter(|(k, _)| !k.starts_with(PAYLOAD_PREFIX))
            .count();
        Ok(Response::Integer(count as i64))
    }
}

// =============================================================================
// FT.SUGGET
// =============================================================================

/// FT.SUGGET key prefix [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX num]
pub struct FtSuggetCommand;

impl Command for FtSuggetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SUGGET",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "FT.SUGGET",
            });
        }

        let prefix = std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
            message: "invalid prefix string".into(),
        })?;

        let mut fuzzy = false;
        let mut with_scores = false;
        let mut with_payloads = false;
        let mut max_results: usize = 5;
        let mut i = 2;
        while i < args.len() {
            let arg_upper = args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"FUZZY" => {
                    fuzzy = true;
                    i += 1;
                }
                b"WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                b"WITHPAYLOADS" => {
                    with_payloads = true;
                    i += 1;
                }
                b"MAX" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    max_results = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or(CommandError::NotInteger)?;
                    i += 2;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        let key = &args[0];
        let value = match ctx.store.get_with_expiry_check(key) {
            Some(v) => v,
            None => return Ok(Response::Null),
        };
        let hash = match value.as_hash() {
            Some(h) => h,
            None => return Ok(Response::Null),
        };

        let prefix_lower = prefix.to_lowercase();
        let mut matches: Vec<(String, f64, Option<Bytes>)> = Vec::new();

        for (k, v) in hash.iter() {
            if k.starts_with(PAYLOAD_PREFIX) {
                continue;
            }
            let suggestion = match std::str::from_utf8(&k) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let suggestion_lower = suggestion.to_lowercase();

            let is_match = if fuzzy {
                suggestion_lower.starts_with(&prefix_lower)
                    || frogdb_search::suggest::levenshtein_distance(
                        &prefix_lower,
                        &suggestion_lower[..suggestion_lower.len().min(prefix_lower.len())],
                    ) <= 1
            } else {
                suggestion_lower.starts_with(&prefix_lower)
            };

            if is_match {
                let score: f64 = std::str::from_utf8(&v)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                let payload = if with_payloads {
                    let mut pl_key = Vec::with_capacity(PAYLOAD_PREFIX.len() + k.len());
                    pl_key.extend_from_slice(PAYLOAD_PREFIX);
                    pl_key.extend_from_slice(&k);
                    hash.get(&pl_key).map(|v| Bytes::copy_from_slice(&v))
                } else {
                    None
                };

                matches.push((suggestion.to_string(), score, payload));
            }
        }

        matches.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        matches.truncate(max_results);

        if matches.is_empty() {
            return Ok(Response::Null);
        }

        let mut result = Vec::new();
        for (suggestion, score, payload) in matches {
            result.push(Response::bulk(Bytes::copy_from_slice(
                suggestion.as_bytes(),
            )));
            if with_scores {
                result.push(Response::bulk(Bytes::copy_from_slice(
                    score.to_string().as_bytes(),
                )));
            }
            if with_payloads {
                match payload {
                    Some(pl) => result.push(Response::bulk(pl)),
                    None => result.push(Response::Null),
                }
            }
        }

        Ok(Response::Array(result))
    }
}

// =============================================================================
// FT.SUGDEL
// =============================================================================

/// FT.SUGDEL key string
pub struct FtSugdelCommand;

impl Command for FtSugdelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SUGDEL",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "FT.SUGDEL",
            });
        }

        let key = &args[0];
        let suggestion = &args[1];

        let hash = match ctx.store.get_mut(key) {
            Some(value) => match value.as_hash_mut() {
                Some(h) => h,
                None => return Ok(Response::Integer(0)),
            },
            None => return Ok(Response::Integer(0)),
        };

        let removed = hash.remove(suggestion.as_ref());

        // Also remove any associated payload
        let mut pl_key = Vec::with_capacity(PAYLOAD_PREFIX.len() + suggestion.len());
        pl_key.extend_from_slice(PAYLOAD_PREFIX);
        pl_key.extend_from_slice(suggestion);
        hash.remove(pl_key.as_slice());

        Ok(Response::Integer(if removed { 1 } else { 0 }))
    }
}

// =============================================================================
// FT.SUGLEN
// =============================================================================

/// FT.SUGLEN key
pub struct FtSuglenCommand;

impl Command for FtSuglenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.SUGLEN",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
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

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArity {
                command: "FT.SUGLEN",
            });
        }

        let key = &args[0];
        let value = match ctx.store.get_with_expiry_check(key) {
            Some(v) => v,
            None => return Ok(Response::Integer(0)),
        };
        let hash = match value.as_hash() {
            Some(h) => h,
            None => return Ok(Response::Integer(0)),
        };

        let count = hash
            .iter()
            .filter(|(k, _)| !k.starts_with(PAYLOAD_PREFIX))
            .count();
        Ok(Response::Integer(count as i64))
    }
}

// =============================================================================
// FT.EXPLAIN
// =============================================================================

/// FT.EXPLAIN index query
pub struct FtExplainCommand;

impl Command for FtExplainCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.EXPLAIN",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtExplain),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.PROFILE
// =============================================================================

/// FT.PROFILE index SEARCH|AGGREGATE [LIMITED] QUERY query [args...]
pub struct FtProfileCommand;

impl Command for FtProfileCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.PROFILE",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtProfile),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}

// =============================================================================
// FT.EXPLAINCLI
// =============================================================================

/// FT.EXPLAINCLI index query
pub struct FtExplainCliCommand;

impl Command for FtExplainCliCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FT.EXPLAINCLI",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::FtExplainCli),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (all-shard
        // fan-out), never on a shard. Reaching this shard-side executor is a
        // routing regression (or a Lua redis.call, which cannot run a
        // server-wide command against a single shard) -- fail loudly rather
        // than fabricate a reply.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }
}
