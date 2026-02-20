use bytes::Bytes;
use frogdb_core::{impl_keys_first, Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

// ============================================================================
// ZRANK - Get rank (ascending)
// ============================================================================

pub struct ZrankCommand;

impl Command for ZrankCommand {
    fn name(&self) -> &'static str {
        "ZRANK"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // ZRANK key member
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                match zset.rank(member) {
                    Some(rank) => Ok(Response::Integer(rank as i64)),
                    None => Ok(Response::null()),
                }
            }
            None => Ok(Response::null()),
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
        Arity::Fixed(2) // ZREVRANK key member
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                match zset.rev_rank(member) {
                    Some(rank) => Ok(Response::Integer(rank as i64)),
                    None => Ok(Response::null()),
                }
            }
            None => Ok(Response::null()),
        }
    }

    impl_keys_first!();
}
