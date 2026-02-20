use bytes::Bytes;
use frogdb_core::{impl_keys_first, Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use crate::commands::utils::{parse_lex_bound, parse_score_bound};

// ============================================================================
// ZCOUNT - Count members in score range
// ============================================================================

pub struct ZcountCommand;

impl Command for ZcountCommand {
    fn name(&self) -> &'static str {
        "ZCOUNT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // ZCOUNT key min max
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
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

    impl_keys_first!();
}

// ============================================================================
// ZLEXCOUNT - Count members in lex range
// ============================================================================

pub struct ZlexcountCommand;

impl Command for ZlexcountCommand {
    fn name(&self) -> &'static str {
        "ZLEXCOUNT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // ZLEXCOUNT key min max
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
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

    impl_keys_first!();
}
