use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::utils::format_float;

// ============================================================================
// ZRANK - Get rank (ascending)
// ============================================================================

pub struct ZrankCommand;

impl Command for ZrankCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZRANK",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
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
        let key = &args[0];
        let member = &args[1];
        let with_score = args.len() > 2 && args[2].to_ascii_uppercase().as_slice() == b"WITHSCORE";

        let null_response = if with_score {
            Response::NullArray
        } else {
            Response::null()
        };

        match ctx.store.get_zset(key)? {
            Some(zset) => match zset.rank(member) {
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
            },
            None => Ok(null_response),
        }
    }
}

// ============================================================================
// ZREVRANK - Get rank (descending)
// ============================================================================

pub struct ZrevrankCommand;

impl Command for ZrevrankCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREVRANK",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
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
        let key = &args[0];
        let member = &args[1];
        let with_score = args.len() > 2 && args[2].to_ascii_uppercase().as_slice() == b"WITHSCORE";

        let null_response = if with_score {
            Response::NullArray
        } else {
            Response::null()
        };

        match ctx.store.get_zset(key)? {
            Some(zset) => match zset.rev_rank(member) {
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
            },
            None => Ok(null_response),
        }
    }
}
