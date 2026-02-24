use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use crate::utils::format_float;

// ============================================================================
// ZRANK - Get rank (ascending)
// ============================================================================

pub struct ZrankCommand;

impl Command for ZrankCommand {
    fn name(&self) -> &'static str {
        "ZRANK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZRANK key member [WITHSCORE]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];
        let with_score = args.len() > 2
            && args[2].to_ascii_uppercase().as_slice() == b"WITHSCORE";

        let null_response = if with_score {
            Response::NullArray
        } else {
            Response::null()
        };

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                match zset.rank(member) {
                    Some(rank) => {
                        if with_score {
                            let score = zset.get_score(member).unwrap_or(0.0);
                            Ok(Response::Array(vec![
                                Response::Integer(rank as i64),
                                Response::bulk(Bytes::from(format_float(score))),
                            ]))
                        } else {
                            Ok(Response::Integer(rank as i64))
                        }
                    }
                    None => Ok(null_response),
                }
            }
            None => Ok(null_response),
        }
    }

    impl_keys_first!();
}

// ============================================================================
// ZREVRANK - Get rank (descending)
// ============================================================================

pub struct ZrevrankCommand;

impl Command for ZrevrankCommand {
    fn name(&self) -> &'static str {
        "ZREVRANK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZREVRANK key member [WITHSCORE]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];
        let with_score = args.len() > 2
            && args[2].to_ascii_uppercase().as_slice() == b"WITHSCORE";

        let null_response = if with_score {
            Response::NullArray
        } else {
            Response::null()
        };

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                match zset.rev_rank(member) {
                    Some(rank) => {
                        if with_score {
                            let score = zset.get_score(member).unwrap_or(0.0);
                            Ok(Response::Array(vec![
                                Response::Integer(rank as i64),
                                Response::bulk(Bytes::from(format_float(score))),
                            ]))
                        } else {
                            Ok(Response::Integer(rank as i64))
                        }
                    }
                    None => Ok(null_response),
                }
            }
            None => Ok(null_response),
        }
    }

    impl_keys_first!();
}
