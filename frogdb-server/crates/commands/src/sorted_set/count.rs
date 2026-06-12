use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::utils::{parse_lex_bound, parse_score_bound};

// ============================================================================
// ZCOUNT - Count members in score range
// ============================================================================

pub struct ZcountCommand;

impl Command for ZcountCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZCOUNT",
            arity: Arity::Fixed(3),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_score_bound(&args[1])?;
        let max = parse_score_bound(&args[2])?;

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                let count = zset.count_by_score(&min, &max);
                Ok(Response::Integer(count as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// ZLEXCOUNT - Count members in lex range
// ============================================================================

pub struct ZlexcountCommand;

impl Command for ZlexcountCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZLEXCOUNT",
            arity: Arity::Fixed(3),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_lex_bound(&args[1])?;
        let max = parse_lex_bound(&args[2])?;

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                let count = zset.count_by_lex(&min, &max);
                Ok(Response::Integer(count as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }
}
