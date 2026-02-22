use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use crate::commands::utils::{
    members_array, parse_i64, parse_lex_bound, parse_score_bound, parse_usize, scored_array,
};

// ============================================================================
// ZRANGE - Unified range query (Redis 6.2+)
// ============================================================================

pub struct ZrangeCommand;

impl Command for ZrangeCommand {
    fn name(&self) -> &'static str {
        "ZRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse options
        let mut by_score = false;
        let mut by_lex = false;
        let mut rev = false;
        let mut with_scores = false;
        let mut limit_offset: usize = 0;
        let mut limit_count: Option<usize> = None;

        let mut i = 3;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"BYSCORE" => by_score = true,
                b"BYLEX" => by_lex = true,
                b"REV" => rev = true,
                b"WITHSCORES" => with_scores = true,
                b"LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    limit_offset = parse_usize(&args[i + 1])?;
                    let count = parse_i64(&args[i + 2])?;
                    limit_count = if count < 0 {
                        None // -1 means no limit
                    } else {
                        Some(count as usize)
                    };
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        // Cannot use both BYSCORE and BYLEX
        if by_score && by_lex {
            return Err(CommandError::SyntaxError);
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };

        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let results = if by_score {
            // Range by score
            let min = parse_score_bound(&args[1])?;
            let max = parse_score_bound(&args[2])?;

            if rev {
                zset.rev_range_by_score(&min, &max, limit_offset, limit_count)
            } else {
                zset.range_by_score(&min, &max, limit_offset, limit_count)
            }
        } else if by_lex {
            // Range by lex
            let min = parse_lex_bound(&args[1])?;
            let max = parse_lex_bound(&args[2])?;

            if rev {
                zset.rev_range_by_lex(&min, &max, limit_offset, limit_count)
            } else {
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

        Ok(scored_array(results, with_scores))
    }

    impl_keys_first!();
}

// ============================================================================
// ZRANGEBYSCORE - Legacy score range (ascending)
// ============================================================================

pub struct ZrangebyscoreCommand;

impl Command for ZrangebyscoreCommand {
    fn name(&self) -> &'static str {
        "ZRANGEBYSCORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_score_bound(&args[1])?;
        let max = parse_score_bound(&args[2])?;

        let mut with_scores = false;
        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut i = 3;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"WITHSCORES" => with_scores = true,
                b"LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    offset = parse_usize(&args[i + 1])?;
                    let c = parse_i64(&args[i + 2])?;
                    count = if c < 0 { None } else { Some(c as usize) };
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let results = zset.range_by_score(&min, &max, offset, count);

        Ok(scored_array(results, with_scores))
    }

    impl_keys_first!();
}

// ============================================================================
// ZREVRANGEBYSCORE - Legacy score range (descending)
// ============================================================================

pub struct ZrevrangebyscoreCommand;

impl Command for ZrevrangebyscoreCommand {
    fn name(&self) -> &'static str {
        "ZREVRANGEBYSCORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        // Note: For ZREVRANGEBYSCORE, args are max min (reversed from ZRANGEBYSCORE)
        let max = parse_score_bound(&args[1])?;
        let min = parse_score_bound(&args[2])?;

        let mut with_scores = false;
        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut i = 3;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"WITHSCORES" => with_scores = true,
                b"LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    offset = parse_usize(&args[i + 1])?;
                    let c = parse_i64(&args[i + 2])?;
                    count = if c < 0 { None } else { Some(c as usize) };
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let results = zset.rev_range_by_score(&min, &max, offset, count);

        Ok(scored_array(results, with_scores))
    }

    impl_keys_first!();
}

// ============================================================================
// ZRANGEBYLEX - Legacy lex range (ascending)
// ============================================================================

pub struct ZrangebylexCommand;

impl Command for ZrangebylexCommand {
    fn name(&self) -> &'static str {
        "ZRANGEBYLEX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZRANGEBYLEX key min max [LIMIT offset count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_lex_bound(&args[1])?;
        let max = parse_lex_bound(&args[2])?;

        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut i = 3;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    offset = parse_usize(&args[i + 1])?;
                    let c = parse_i64(&args[i + 2])?;
                    count = if c < 0 { None } else { Some(c as usize) };
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let results = zset.range_by_lex(&min, &max, offset, count);

        Ok(members_array(results))
    }

    impl_keys_first!();
}

// ============================================================================
// ZREVRANGEBYLEX - Legacy lex range (descending)
// ============================================================================

pub struct ZrevrangebylexCommand;

impl Command for ZrevrangebylexCommand {
    fn name(&self) -> &'static str {
        "ZREVRANGEBYLEX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZREVRANGEBYLEX key max min [LIMIT offset count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        // Note: For ZREVRANGEBYLEX, args are max min (reversed)
        let max = parse_lex_bound(&args[1])?;
        let min = parse_lex_bound(&args[2])?;

        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut i = 3;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    offset = parse_usize(&args[i + 1])?;
                    let c = parse_i64(&args[i + 2])?;
                    count = if c < 0 { None } else { Some(c as usize) };
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let results = zset.rev_range_by_lex(&min, &max, offset, count);

        Ok(members_array(results))
    }

    impl_keys_first!();
}
