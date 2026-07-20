use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

use crate::utils::{
    members_array, parse_i64, parse_lex_bound, parse_limit_clause, parse_score_bound, scored_array,
    scored_array_resp3,
};

// ============================================================================
// ZRANGE - Unified range query (Redis 6.2+)
// ============================================================================

pub struct ZrangeCommand;

impl Command for ZrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZRANGE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::FirstKey,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();

        // Parse options
        let mut by_score = false;
        let mut by_lex = false;
        let mut rev = false;
        let mut with_scores = false;
        let mut limit_offset: usize = 0;
        let mut limit_count: Option<usize> = None;
        let mut has_limit = false;

        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"BYSCORE") {
                by_score = true;
            } else if parser.try_flag(b"BYLEX") {
                by_lex = true;
            } else if parser.try_flag(b"REV") {
                rev = true;
            } else if parser.try_flag(b"WITHSCORES") {
                with_scores = true;
            } else if parser.try_flag(b"LIMIT") {
                has_limit = true;
                (limit_offset, limit_count) = parse_limit_clause(&mut parser)?;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Cannot use both BYSCORE and BYLEX
        if by_score && by_lex {
            return Err(CommandError::SyntaxError);
        }

        // LIMIT requires BYSCORE or BYLEX
        if has_limit && !by_score && !by_lex {
            return Err(CommandError::SyntaxError);
        }

        // BYLEX is incompatible with WITHSCORES
        if by_lex && with_scores {
            return Err(CommandError::SyntaxError);
        }

        let Some(zset) = ctx.store.get_zset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let results = if by_score {
            // In REV mode, args[1] is the max and args[2] is the min (swapped from forward)
            if rev {
                let max = parse_score_bound(&args[1])?;
                let min = parse_score_bound(&args[2])?;
                zset.rev_range_by_score(&min, &max, limit_offset, limit_count)
            } else {
                let min = parse_score_bound(&args[1])?;
                let max = parse_score_bound(&args[2])?;
                zset.range_by_score(&min, &max, limit_offset, limit_count)
            }
        } else if by_lex {
            // In REV mode, args[1] is the max and args[2] is the min (swapped from forward)
            if rev {
                let max = parse_lex_bound(&args[1])?;
                let min = parse_lex_bound(&args[2])?;
                zset.rev_range_by_lex(&min, &max, limit_offset, limit_count)
            } else {
                let min = parse_lex_bound(&args[1])?;
                let max = parse_lex_bound(&args[2])?;
                zset.range_by_lex(&min, &max, limit_offset, limit_count)
            }
        } else {
            // Range by rank (default)
            let start = parse_i64(&args[1])?;
            let end = parse_i64(&args[2])?;

            if rev {
                zset.rev_range_by_rank(start, end)
            } else {
                zset.range_by_rank(start, end)
            }
        };

        // In RESP3, WITHSCORES responses use nested [member, Double(score)] pairs
        if is_resp3 && with_scores {
            Ok(scored_array_resp3(results, true))
        } else {
            Ok(scored_array(results, with_scores))
        }
    }
}

// ============================================================================
// ZRANGEBYSCORE - Legacy score range (ascending)
// ============================================================================

pub struct ZrangebyscoreCommand;

impl Command for ZrangebyscoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZRANGEBYSCORE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_score_bound(&args[1])?;
        let max = parse_score_bound(&args[2])?;

        let mut with_scores = false;
        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"WITHSCORES") {
                with_scores = true;
            } else if parser.try_flag(b"LIMIT") {
                (offset, count) = parse_limit_clause(&mut parser)?;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        let Some(zset) = ctx.store.get_zset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let results = zset.range_by_score(&min, &max, offset, count);

        Ok(scored_array(results, with_scores))
    }
}

// ============================================================================
// ZREVRANGE - Legacy reverse range by rank (deprecated, use ZRANGE REV)
// ============================================================================

pub struct ZrevrangeCommand;

impl Command for ZrevrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREVRANGE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = parse_i64(&args[1])?;
        let end = parse_i64(&args[2])?;

        // Only a single trailing WITHSCORES is accepted; anything else is a
        // syntax error.
        let mut parser = ArgParser::from_position(args, 3);
        let with_scores = parser.try_flag(b"WITHSCORES");
        if parser.has_more() {
            return Err(CommandError::SyntaxError);
        }

        let Some(zset) = ctx.store.get_zset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let results = zset.rev_range_by_rank(start, end);

        Ok(scored_array(results, with_scores))
    }
}

// ============================================================================
// ZREVRANGEBYSCORE - Legacy score range (descending)
// ============================================================================

pub struct ZrevrangebyscoreCommand;

impl Command for ZrevrangebyscoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREVRANGEBYSCORE",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        // Note: For ZREVRANGEBYSCORE, args are max min (reversed from ZRANGEBYSCORE)
        let max = parse_score_bound(&args[1])?;
        let min = parse_score_bound(&args[2])?;

        let mut with_scores = false;
        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"WITHSCORES") {
                with_scores = true;
            } else if parser.try_flag(b"LIMIT") {
                (offset, count) = parse_limit_clause(&mut parser)?;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        let Some(zset) = ctx.store.get_zset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let results = zset.rev_range_by_score(&min, &max, offset, count);

        Ok(scored_array(results, with_scores))
    }
}

// ============================================================================
// ZRANGEBYLEX - Legacy lex range (ascending)
// ============================================================================

pub struct ZrangebylexCommand;

impl Command for ZrangebylexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZRANGEBYLEX",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_lex_bound(&args[1])?;
        let max = parse_lex_bound(&args[2])?;

        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"LIMIT") {
                (offset, count) = parse_limit_clause(&mut parser)?;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        let Some(zset) = ctx.store.get_zset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let results = zset.range_by_lex(&min, &max, offset, count);

        Ok(members_array(results))
    }
}

// ============================================================================
// ZREVRANGEBYLEX - Legacy lex range (descending)
// ============================================================================

pub struct ZrevrangebylexCommand;

impl Command for ZrevrangebylexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREVRANGEBYLEX",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        // Note: For ZREVRANGEBYLEX, args are max min (reversed)
        let max = parse_lex_bound(&args[1])?;
        let min = parse_lex_bound(&args[2])?;

        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut parser = ArgParser::from_position(args, 3);
        while parser.has_more() {
            if parser.try_flag(b"LIMIT") {
                (offset, count) = parse_limit_clause(&mut parser)?;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        let Some(zset) = ctx.store.get_zset(key)? else {
            return Ok(Response::Array(vec![]));
        };

        let results = zset.rev_range_by_lex(&min, &max, offset, count);

        Ok(members_array(results))
    }
}
