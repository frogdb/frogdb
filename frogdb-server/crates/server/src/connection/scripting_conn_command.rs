//! The scripting / function connection commands.
//!
//! EVAL, EVAL_RO, EVALSHA, EVALSHA_RO, SCRIPT, FCALL, FCALL_RO, and FUNCTION are
//! migrated behind the [`ConnectionCommand`] seam. Each is a *pure-read*
//! connection command over [`ConnCtx`]: it drives the script executor / Lua VM /
//! function registry / cross-shard EVAL coordinator through
//! [`ConnCtx::scripting`] ([`ScriptingProvider`]), which the server implements
//! for [`ConnectionHandler`] by delegating to the existing
//! `handlers/scripting/*` helpers. The eval engine and the cross-shard VLL
//! continuation logic are unchanged — only the dispatch entry point moves behind
//! the seam.
//!
//! # Keyed connection commands
//!
//! EVAL/EVALSHA/FCALL (and their `_RO` variants) are the first connection
//! commands with keys: the keys are located by a runtime `numkeys` count, so
//! their [`CommandSpec`] declares [`KeySpec::Dynamic`] (+ `MOVABLEKEYS`, matching
//! Redis' `COMMAND INFO`) and each overrides
//! [`ConnectionCommand::dynamic_keys`] to extract them. This feeds the registry's
//! key machinery (transaction key-folding, `COMMAND GETKEYS`/`GETKEYSANDFLAGS`),
//! all of which resolve keys through the registry union (`get_entry`) — so these
//! connection executors are the single source of key extraction, with no
//! shard-local stub needed.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

/// Bridges the connection-command seam to the server's scripting handlers. Each
/// method delegates to the existing `handlers/scripting/*` entry point on
/// [`ConnectionHandler`], which owns the executor / function registry / VLL
/// cross-shard EVAL logic.
impl frogdb_core::ScriptingProvider for ConnectionHandler {
    fn eval<'a>(&'a self, args: &'a [Bytes], read_only: bool) -> BoxFuture<'a, Response> {
        Box::pin(self.handle_eval(args, read_only))
    }
    fn evalsha<'a>(&'a self, args: &'a [Bytes], read_only: bool) -> BoxFuture<'a, Response> {
        Box::pin(self.handle_evalsha(args, read_only))
    }
    fn script<'a>(&'a self, args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(self.handle_script(args))
    }
    fn fcall<'a>(&'a self, args: &'a [Bytes], read_only: bool) -> BoxFuture<'a, Response> {
        Box::pin(self.handle_fcall(args, read_only))
    }
    fn function<'a>(&'a self, args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(self.handle_function(args))
    }
}

/// Extract the keys of an `… numkeys key…` layout (EVAL/EVALSHA/FCALL): the count
/// N lives at `args[1]`, and N keys start at `args[2]`.
fn numkeys_keys(args: &[Bytes]) -> Vec<&[u8]> {
    if args.len() < 2 {
        return Vec::new();
    }
    let numkeys = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    if numkeys == 0 || args.len() < 2 + numkeys {
        return Vec::new();
    }
    args[2..2 + numkeys].iter().map(|k| k.as_ref()).collect()
}

// ---------------------------------------------------------------------------
// EVAL / EVAL_RO / EVALSHA / EVALSHA_RO
// ---------------------------------------------------------------------------

/// The `CommandSpec` for EVAL. Strategy is `ConnectionLevel(Scripting)`,
/// validated by the registry against the `Connection` executor variant.
static EVAL_SPEC: CommandSpec = CommandSpec {
    name: "EVAL",
    arity: Arity::AtLeast(2),
    flags: CommandFlags::SCRIPT
        .union(CommandFlags::NONDETERMINISTIC)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::UniformRW,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` EVAL executor.
pub(crate) static EVAL_CONN_COMMAND: EvalConnCommand = EvalConnCommand;

/// EVAL — execute a Lua script (source).
pub(crate) struct EvalConnCommand;

impl ConnectionCommand for EvalConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &EVAL_SPEC
    }
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        numkeys_keys(args)
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.eval(args, false)
    }
}

/// The `CommandSpec` for EVAL_RO (read-only: replica-eligible, write-rejecting).
static EVAL_RO_SPEC: CommandSpec = CommandSpec {
    name: "EVAL_RO",
    arity: Arity::AtLeast(2),
    flags: CommandFlags::SCRIPT
        .union(CommandFlags::READONLY)
        .union(CommandFlags::NONDETERMINISTIC)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` EVAL_RO executor.
pub(crate) static EVAL_RO_CONN_COMMAND: EvalRoConnCommand = EvalRoConnCommand;

/// EVAL_RO — execute a Lua script (source) in read-only mode.
pub(crate) struct EvalRoConnCommand;

impl ConnectionCommand for EvalRoConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &EVAL_RO_SPEC
    }
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        numkeys_keys(args)
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.eval(args, true)
    }
}

/// The `CommandSpec` for EVALSHA.
static EVALSHA_SPEC: CommandSpec = CommandSpec {
    name: "EVALSHA",
    arity: Arity::AtLeast(2),
    flags: CommandFlags::SCRIPT
        .union(CommandFlags::NONDETERMINISTIC)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::UniformRW,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` EVALSHA executor.
pub(crate) static EVALSHA_CONN_COMMAND: EvalshaConnCommand = EvalshaConnCommand;

/// EVALSHA — execute a cached Lua script by SHA1.
pub(crate) struct EvalshaConnCommand;

impl ConnectionCommand for EvalshaConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &EVALSHA_SPEC
    }
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        numkeys_keys(args)
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.evalsha(args, false)
    }
}

/// The `CommandSpec` for EVALSHA_RO.
static EVALSHA_RO_SPEC: CommandSpec = CommandSpec {
    name: "EVALSHA_RO",
    arity: Arity::AtLeast(2),
    flags: CommandFlags::SCRIPT
        .union(CommandFlags::READONLY)
        .union(CommandFlags::NONDETERMINISTIC)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` EVALSHA_RO executor.
pub(crate) static EVALSHA_RO_CONN_COMMAND: EvalshaRoConnCommand = EvalshaRoConnCommand;

/// EVALSHA_RO — execute a cached Lua script by SHA1 in read-only mode.
pub(crate) struct EvalshaRoConnCommand;

impl ConnectionCommand for EvalshaRoConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &EVALSHA_RO_SPEC
    }
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        numkeys_keys(args)
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.evalsha(args, true)
    }
}

// ---------------------------------------------------------------------------
// SCRIPT
// ---------------------------------------------------------------------------

/// The `CommandSpec` for SCRIPT (LOAD/EXISTS/FLUSH/KILL/HELP). Keyless.
static SCRIPT_SPEC: CommandSpec = CommandSpec {
    name: "SCRIPT",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::NOSCRIPT
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` SCRIPT executor.
pub(crate) static SCRIPT_CONN_COMMAND: ScriptConnCommand = ScriptConnCommand;

/// SCRIPT — manage the Lua script cache.
pub(crate) struct ScriptConnCommand;

impl ConnectionCommand for ScriptConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &SCRIPT_SPEC
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.script(args)
    }
}

// ---------------------------------------------------------------------------
// FCALL / FCALL_RO / FUNCTION
// ---------------------------------------------------------------------------

/// The `CommandSpec` for FCALL.
static FCALL_SPEC: CommandSpec = CommandSpec {
    name: "FCALL",
    arity: Arity::AtLeast(2),
    flags: CommandFlags::SCRIPT
        .union(CommandFlags::NOSCRIPT)
        .union(CommandFlags::STALE)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::UniformRW,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` FCALL executor.
pub(crate) static FCALL_CONN_COMMAND: FcallConnCommand = FcallConnCommand;

/// FCALL — call a registered function.
pub(crate) struct FcallConnCommand;

impl ConnectionCommand for FcallConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &FCALL_SPEC
    }
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        numkeys_keys(args)
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.fcall(args, false)
    }
}

/// The `CommandSpec` for FCALL_RO.
static FCALL_RO_SPEC: CommandSpec = CommandSpec {
    name: "FCALL_RO",
    arity: Arity::AtLeast(2),
    flags: CommandFlags::SCRIPT
        .union(CommandFlags::NOSCRIPT)
        .union(CommandFlags::READONLY)
        .union(CommandFlags::STALE)
        .union(CommandFlags::MOVABLEKEYS),
    keys: KeySpec::Dynamic,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` FCALL_RO executor.
pub(crate) static FCALL_RO_CONN_COMMAND: FcallRoConnCommand = FcallRoConnCommand;

/// FCALL_RO — call a registered read-only function.
pub(crate) struct FcallRoConnCommand;

impl ConnectionCommand for FcallRoConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &FCALL_RO_SPEC
    }
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        numkeys_keys(args)
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.fcall(args, true)
    }
}

/// The `CommandSpec` for FUNCTION (LOAD/DELETE/FLUSH/LIST/DUMP/RESTORE/STATS/
/// KILL/HELP). Keyless.
static FUNCTION_SPEC: CommandSpec = CommandSpec {
    name: "FUNCTION",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::NOSCRIPT,
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting),
};

/// The registrable, `'static` FUNCTION executor.
pub(crate) static FUNCTION_CONN_COMMAND: FunctionConnCommand = FunctionConnCommand;

/// FUNCTION — manage the function-library registry.
pub(crate) struct FunctionConnCommand;

impl ConnectionCommand for FunctionConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &FUNCTION_SPEC
    }
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        ctx.scripting.function(args)
    }
}
