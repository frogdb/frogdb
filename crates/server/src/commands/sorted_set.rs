//! Sorted set commands.
//!
//! Commands for sorted set manipulation:
//! - ZADD, ZREM, ZSCORE, ZMSCORE, ZCARD, ZINCRBY - basic operations
//! - ZRANK, ZREVRANK - ranking
//! - ZRANGE, ZRANGEBYSCORE, ZRANGEBYLEX, etc. - range queries
//! - ZPOPMIN, ZPOPMAX, ZMPOP, ZRANDMEMBER - pop & random
//! - ZUNION, ZUNIONSTORE, ZINTER, ZINTERSTORE, ZDIFF, ZDIFFSTORE - set operations
//! - ZSCAN, ZRANGESTORE, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX - other

use bytes::Bytes;
use frogdb_core::{
    shard_for_key, Arity, Command, CommandContext, CommandError, CommandFlags, SortedSetValue,
    Value,
};
use frogdb_protocol::Response;
use std::collections::HashMap;

use super::utils::{
    format_float, get_or_create_zset, parse_f64, parse_i64, parse_lex_bound, parse_score_bound,
    parse_usize,
};

// ============================================================================
// ZADD - Add members to sorted set
// ============================================================================

pub struct ZaddCommand;

impl Command for ZaddCommand {
    fn name(&self) -> &'static str {
        "ZADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZADD key [options] score member [score member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse options
        let mut nx = false; // Only add new elements
        let mut xx = false; // Only update existing elements
        let mut gt = false; // Only update if new score > current
        let mut lt = false; // Only update if new score < current
        let mut ch = false; // Return number of changed elements (not just added)
        let mut incr = false; // Increment mode (ZINCRBY behavior)

        let mut i = 1;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"NX" => {
                    if xx {
                        return Err(CommandError::InvalidArgument {
                            message: "XX and NX options at the same time are not compatible"
                                .to_string(),
                        });
                    }
                    nx = true;
                }
                b"XX" => {
                    if nx {
                        return Err(CommandError::InvalidArgument {
                            message: "XX and NX options at the same time are not compatible"
                                .to_string(),
                        });
                    }
                    xx = true;
                }
                b"GT" => {
                    if lt {
                        return Err(CommandError::InvalidArgument {
                            message: "GT, LT, and NX options at the same time are not compatible"
                                .to_string(),
                        });
                    }
                    gt = true;
                }
                b"LT" => {
                    if gt {
                        return Err(CommandError::InvalidArgument {
                            message: "GT, LT, and NX options at the same time are not compatible"
                                .to_string(),
                        });
                    }
                    lt = true;
                }
                b"CH" => ch = true,
                b"INCR" => incr = true,
                _ => break, // Start of score-member pairs
            }
            i += 1;
        }

        // Check for GT/LT with NX
        if nx && (gt || lt) {
            return Err(CommandError::InvalidArgument {
                message: "GT, LT, and NX options at the same time are not compatible".to_string(),
            });
        }

        // Parse score-member pairs
        let remaining = &args[i..];
        if remaining.len() % 2 != 0 || remaining.is_empty() {
            return Err(CommandError::SyntaxError);
        }

        // INCR mode requires exactly one score-member pair
        if incr && remaining.len() != 2 {
            return Err(CommandError::InvalidArgument {
                message: "INCR option supports a single increment-element pair".to_string(),
            });
        }

        let mut pairs = Vec::with_capacity(remaining.len() / 2);
        for chunk in remaining.chunks(2) {
            let score = parse_f64(&chunk[0])?;
            let member = chunk[1].clone();
            pairs.push((member, score));
        }

        // Check NaN
        for (_, score) in &pairs {
            if score.is_nan() {
                return Err(CommandError::NotFloat);
            }
        }

        // Get or create the sorted set
        let zset = get_or_create_zset(ctx, key)?;

        if incr {
            // INCR mode - return the new score
            let (member, score) = pairs.into_iter().next().unwrap();

            let current_score = zset.get_score(&member);

            // Apply NX/XX conditions
            if nx && current_score.is_some() {
                return Ok(Response::null());
            }
            if xx && current_score.is_none() {
                return Ok(Response::null());
            }

            let new_score = if let Some(old) = current_score {
                let candidate = old + score;

                // Apply GT/LT conditions
                if gt && candidate <= old {
                    return Ok(Response::bulk(Bytes::from(format_float(old))));
                }
                if lt && candidate >= old {
                    return Ok(Response::bulk(Bytes::from(format_float(old))));
                }

                zset.add(member, candidate);
                candidate
            } else {
                zset.add(member, score);
                score
            };

            Ok(Response::bulk(Bytes::from(format_float(new_score))))
        } else {
            // Normal mode - return count
            let mut added = 0;
            let mut changed = 0;

            for (member, score) in pairs {
                let current_score = zset.get_score(&member);

                // Apply NX condition
                if nx && current_score.is_some() {
                    continue;
                }

                // Apply XX condition
                if xx && current_score.is_none() {
                    continue;
                }

                // Apply GT/LT conditions for updates
                if let Some(old) = current_score {
                    if gt && score <= old {
                        continue;
                    }
                    if lt && score >= old {
                        continue;
                    }
                }

                let result = zset.add(member, score);
                if result.added {
                    added += 1;
                }
                if result.changed {
                    changed += 1;
                }
            }

            if ch {
                Ok(Response::Integer((added + changed) as i64))
            } else {
                Ok(Response::Integer(added as i64))
            }
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZREM - Remove members from sorted set
// ============================================================================

pub struct ZremCommand;

impl Command for ZremCommand {
    fn name(&self) -> &'static str {
        "ZREM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZREM key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Integer(0)),
        };

        let mut removed = 0;
        for member in &args[1..] {
            if zset.remove(member).is_some() {
                removed += 1;
            }
        }

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZSCORE - Get score of member
// ============================================================================

pub struct ZscoreCommand;

impl Command for ZscoreCommand {
    fn name(&self) -> &'static str {
        "ZSCORE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // ZSCORE key member
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                match zset.get_score(member) {
                    Some(score) => {
                        if ctx.protocol_version.is_resp3() {
                            Ok(Response::Double(score))
                        } else {
                            Ok(Response::bulk(Bytes::from(format_float(score))))
                        }
                    }
                    None => Ok(Response::null()),
                }
            }
            None => Ok(Response::null()),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZMSCORE - Get scores of multiple members
// ============================================================================

pub struct ZmscoreCommand;

impl Command for ZmscoreCommand {
    fn name(&self) -> &'static str {
        "ZMSCORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZMSCORE key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                let scores: Vec<Response> = args[1..]
                    .iter()
                    .map(|member| match zset.get_score(member) {
                        Some(score) => {
                            if is_resp3 {
                                Response::Double(score)
                            } else {
                                Response::bulk(Bytes::from(format_float(score)))
                            }
                        }
                        None => Response::null(),
                    })
                    .collect();
                Ok(Response::Array(scores))
            }
            None => {
                // Key doesn't exist, return nulls for all members
                let nulls: Vec<Response> = args[1..].iter().map(|_| Response::null()).collect();
                Ok(Response::Array(nulls))
            }
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZCARD - Get cardinality
// ============================================================================

pub struct ZcardCommand;

impl Command for ZcardCommand {
    fn name(&self) -> &'static str {
        "ZCARD"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // ZCARD key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                Ok(Response::Integer(zset.len() as i64))
            }
            None => Ok(Response::Integer(0)),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZINCRBY - Increment score
// ============================================================================

pub struct ZincrbyCommand;

impl Command for ZincrbyCommand {
    fn name(&self) -> &'static str {
        "ZINCRBY"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // ZINCRBY key increment member
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let increment = parse_f64(&args[1])?;
        let member = args[2].clone();

        if increment.is_nan() {
            return Err(CommandError::NotFloat);
        }

        let zset = get_or_create_zset(ctx, key)?;
        let new_score = zset.incr(member, increment);

        if ctx.protocol_version.is_resp3() {
            Ok(Response::Double(new_score))
        } else {
            Ok(Response::bulk(Bytes::from(format_float(new_score))))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

        // Build response
        if with_scores {
            let response: Vec<Response> = results
                .into_iter()
                .flat_map(|(member, score)| {
                    vec![
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect();
            Ok(Response::Array(response))
        } else {
            let response: Vec<Response> = results
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect();
            Ok(Response::Array(response))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

        if with_scores {
            let response: Vec<Response> = results
                .into_iter()
                .flat_map(|(member, score)| {
                    vec![
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect();
            Ok(Response::Array(response))
        } else {
            let response: Vec<Response> = results
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect();
            Ok(Response::Array(response))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

        if with_scores {
            let response: Vec<Response> = results
                .into_iter()
                .flat_map(|(member, score)| {
                    vec![
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect();
            Ok(Response::Array(response))
        } else {
            let response: Vec<Response> = results
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect();
            Ok(Response::Array(response))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

        let response: Vec<Response> = results
            .into_iter()
            .map(|(member, _)| Response::bulk(member))
            .collect();
        Ok(Response::Array(response))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

        let response: Vec<Response> = results
            .into_iter()
            .map(|(member, _)| Response::bulk(member))
            .collect();
        Ok(Response::Array(response))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZPOPMIN - Pop minimum score members
// ============================================================================

pub struct ZpopminCommand;

impl Command for ZpopminCommand {
    fn name(&self) -> &'static str {
        "ZPOPMIN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // ZPOPMIN key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let count = if args.len() > 1 {
            parse_usize(&args[1])?
        } else {
            1
        };

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Array(vec![])),
        };

        let results = zset.pop_min(count);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        let response: Vec<Response> = results
            .into_iter()
            .flat_map(|(member, score)| {
                vec![
                    Response::bulk(member),
                    Response::bulk(Bytes::from(format_float(score))),
                ]
            })
            .collect();
        Ok(Response::Array(response))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZPOPMAX - Pop maximum score members
// ============================================================================

pub struct ZpopmaxCommand;

impl Command for ZpopmaxCommand {
    fn name(&self) -> &'static str {
        "ZPOPMAX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // ZPOPMAX key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let count = if args.len() > 1 {
            parse_usize(&args[1])?
        } else {
            1
        };

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Array(vec![])),
        };

        let results = zset.pop_max(count);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        let response: Vec<Response> = results
            .into_iter()
            .flat_map(|(member, score)| {
                vec![
                    Response::bulk(member),
                    Response::bulk(Bytes::from(format_float(score))),
                ]
            })
            .collect();
        Ok(Response::Array(response))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZMPOP - Pop from multiple keys
// ============================================================================

pub struct ZmpopCommand;

impl Command for ZmpopCommand {
    fn name(&self) -> &'static str {
        "ZMPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let remaining = &args[numkeys + 1..];

        // Check all keys are in same shard
        let first_shard = shard_for_key(&keys[0], ctx.num_shards);
        for key in &keys[1..] {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        // Parse MIN|MAX and COUNT
        if remaining.is_empty() {
            return Err(CommandError::SyntaxError);
        }

        let direction = remaining[0].to_ascii_uppercase();
        let is_min = match direction.as_slice() {
            b"MIN" => true,
            b"MAX" => false,
            _ => return Err(CommandError::SyntaxError),
        };

        let mut count: usize = 1;
        let mut i = 1;
        while i < remaining.len() {
            let opt = remaining[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"COUNT" => {
                    if i + 1 >= remaining.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&remaining[i + 1])?;
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        // Find first non-empty key
        for key in keys {
            let zset = match ctx.store.get_mut(key) {
                Some(value) => match value.as_sorted_set_mut() {
                    Some(zset) if !zset.is_empty() => zset,
                    _ => continue,
                },
                None => continue,
            };

            let results = if is_min {
                zset.pop_min(count)
            } else {
                zset.pop_max(count)
            };

            // Clean up empty sorted set
            if zset.is_empty() {
                ctx.store.delete(key);
            }

            if !results.is_empty() {
                let elements: Vec<Response> = results
                    .into_iter()
                    .flat_map(|(member, score)| {
                        vec![
                            Response::bulk(member),
                            Response::bulk(Bytes::from(format_float(score))),
                        ]
                    })
                    .collect();

                return Ok(Response::Array(vec![
                    Response::bulk(key.clone()),
                    Response::Array(elements),
                ]));
            }
        }

        Ok(Response::null())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZRANDMEMBER - Get random members
// ============================================================================

pub struct ZrandmemberCommand;

impl Command for ZrandmemberCommand {
    fn name(&self) -> &'static str {
        "ZRANDMEMBER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // ZRANDMEMBER key [count [WITHSCORES]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => {
                if args.len() > 1 {
                    return Ok(Response::Array(vec![]));
                } else {
                    return Ok(Response::null());
                }
            }
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        if args.len() == 1 {
            // Single random member
            let results = zset.random_members(1);
            if let Some((member, _)) = results.first() {
                Ok(Response::bulk(member.clone()))
            } else {
                Ok(Response::null())
            }
        } else {
            let count = parse_i64(&args[1])?;
            let with_scores = args.len() > 2
                && args[2].to_ascii_uppercase().as_slice() == b"WITHSCORES";

            let results = zset.random_members(count);

            if with_scores {
                let response: Vec<Response> = results
                    .into_iter()
                    .flat_map(|(member, score)| {
                        vec![
                            Response::bulk(member),
                            Response::bulk(Bytes::from(format_float(score))),
                        ]
                    })
                    .collect();
                Ok(Response::Array(response))
            } else {
                let response: Vec<Response> = results
                    .into_iter()
                    .map(|(member, _)| Response::bulk(member))
                    .collect();
                Ok(Response::Array(response))
            }
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// Set Operations - ZUNION, ZUNIONSTORE, ZINTER, ZINTERSTORE, ZINTERCARD, ZDIFF, ZDIFFSTORE
// ============================================================================

/// Aggregate function for set operations.
#[derive(Clone, Copy)]
enum AggregateFunc {
    Sum,
    Min,
    Max,
}

/// Parse weights and aggregate options for set operations.
fn parse_set_op_options(
    args: &[Bytes],
    numkeys: usize,
) -> Result<(Vec<f64>, AggregateFunc, bool), CommandError> {
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = AggregateFunc::Sum;
    let mut with_scores = false;

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"WEIGHTS" => {
                if i + numkeys >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                for j in 0..numkeys {
                    weights[j] = parse_f64(&args[i + 1 + j])?;
                }
                i += numkeys + 1;
            }
            b"AGGREGATE" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let agg = args[i + 1].to_ascii_uppercase();
                aggregate = match agg.as_slice() {
                    b"SUM" => AggregateFunc::Sum,
                    b"MIN" => AggregateFunc::Min,
                    b"MAX" => AggregateFunc::Max,
                    _ => return Err(CommandError::SyntaxError),
                };
                i += 2;
            }
            b"WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    Ok((weights, aggregate, with_scores))
}

/// Apply aggregate function.
fn apply_aggregate(scores: &[f64], func: AggregateFunc) -> f64 {
    match func {
        AggregateFunc::Sum => scores.iter().sum(),
        AggregateFunc::Min => scores.iter().cloned().fold(f64::INFINITY, f64::min),
        AggregateFunc::Max => scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
    }
}

// ============================================================================
// ZUNION - Union without store
// ============================================================================

pub struct ZunionCommand;

impl Command for ZunionCommand {
    fn name(&self) -> &'static str {
        "ZUNION"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let options = &args[numkeys + 1..];

        // Check all keys are in same shard
        let first_shard = shard_for_key(&keys[0], ctx.num_shards);
        for key in &keys[1..] {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let (weights, aggregate, with_scores) = parse_set_op_options(options, numkeys)?;

        // Collect all members with their weighted scores
        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                for (member, score) in zset.iter() {
                    let weighted_score = score * weights[i];
                    result
                        .entry(member.clone())
                        .or_default()
                        .push(weighted_score);
                }
            }
        }

        // Build sorted result
        let mut members: Vec<(Bytes, f64)> = result
            .into_iter()
            .map(|(member, scores)| (member, apply_aggregate(&scores, aggregate)))
            .collect();
        members.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        if with_scores {
            let response: Vec<Response> = members
                .into_iter()
                .flat_map(|(member, score)| {
                    vec![
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect();
            Ok(Response::Array(response))
        } else {
            let response: Vec<Response> = members
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect();
            Ok(Response::Array(response))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZUNIONSTORE - Union and store
// ============================================================================

pub struct ZunionstoreCommand;

impl Command for ZunionstoreCommand {
    fn name(&self) -> &'static str {
        "ZUNIONSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let numkeys = parse_usize(&args[1])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..numkeys + 2];
        let options = &args[numkeys + 2..];

        // Check all keys (including dest) are in same shard
        let first_shard = shard_for_key(&dest, ctx.num_shards);
        for key in keys {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let (weights, aggregate, _) = parse_set_op_options(options, numkeys)?;

        // Collect all members with their weighted scores
        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                for (member, score) in zset.iter() {
                    let weighted_score = score * weights[i];
                    result
                        .entry(member.clone())
                        .or_default()
                        .push(weighted_score);
                }
            }
        }

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, scores) in result {
            let score = apply_aggregate(&scores, aggregate);
            new_zset.add(member, score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let mut keys = vec![args[0].as_ref()];
        keys.extend(args[2..].iter().take(numkeys).map(|a| a.as_ref()));
        keys
    }
}

// ============================================================================
// ZINTER - Intersection without store
// ============================================================================

pub struct ZinterCommand;

impl Command for ZinterCommand {
    fn name(&self) -> &'static str {
        "ZINTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let options = &args[numkeys + 1..];

        // Check all keys are in same shard
        let first_shard = shard_for_key(&keys[0], ctx.num_shards);
        for key in &keys[1..] {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let (weights, aggregate, with_scores) = parse_set_op_options(options, numkeys)?;

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();
        for (member, score) in first_zset.iter() {
            result.insert(member.clone(), vec![score * weights[0]]);
        }

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Array(vec![])),
            };
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            result.retain(|member, scores| {
                if let Some(score) = zset.get_score(member) {
                    scores.push(score * weights[i]);
                    true
                } else {
                    false
                }
            });

            if result.is_empty() {
                return Ok(Response::Array(vec![]));
            }
        }

        // Build sorted result
        let mut members: Vec<(Bytes, f64)> = result
            .into_iter()
            .map(|(member, scores)| (member, apply_aggregate(&scores, aggregate)))
            .collect();
        members.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        if with_scores {
            let response: Vec<Response> = members
                .into_iter()
                .flat_map(|(member, score)| {
                    vec![
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect();
            Ok(Response::Array(response))
        } else {
            let response: Vec<Response> = members
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect();
            Ok(Response::Array(response))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZINTERSTORE - Intersection and store
// ============================================================================

pub struct ZinterstoreCommand;

impl Command for ZinterstoreCommand {
    fn name(&self) -> &'static str {
        "ZINTERSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZINTERSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let numkeys = parse_usize(&args[1])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..numkeys + 2];
        let options = &args[numkeys + 2..];

        // Check all keys (including dest) are in same shard
        let first_shard = shard_for_key(&dest, ctx.num_shards);
        for key in keys {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let (weights, aggregate, _) = parse_set_op_options(options, numkeys)?;

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();
        for (member, score) in first_zset.iter() {
            result.insert(member.clone(), vec![score * weights[0]]);
        }

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => {
                    ctx.store.delete(&dest);
                    return Ok(Response::Integer(0));
                }
            };
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            result.retain(|member, scores| {
                if let Some(score) = zset.get_score(member) {
                    scores.push(score * weights[i]);
                    true
                } else {
                    false
                }
            });

            if result.is_empty() {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        }

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, scores) in result {
            let score = apply_aggregate(&scores, aggregate);
            new_zset.add(member, score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let mut keys = vec![args[0].as_ref()];
        keys.extend(args[2..].iter().take(numkeys).map(|a| a.as_ref()));
        keys
    }
}

// ============================================================================
// ZINTERCARD - Intersection cardinality
// ============================================================================

pub struct ZintercardCommand;

impl Command for ZintercardCommand {
    fn name(&self) -> &'static str {
        "ZINTERCARD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZINTERCARD numkeys key [key ...] [LIMIT limit]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let remaining = &args[numkeys + 1..];

        // Check all keys are in same shard
        let first_shard = shard_for_key(&keys[0], ctx.num_shards);
        for key in &keys[1..] {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        // Parse LIMIT option
        let mut limit: Option<usize> = None;
        let mut i = 0;
        while i < remaining.len() {
            let opt = remaining[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIMIT" => {
                    if i + 1 >= remaining.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    limit = Some(parse_usize(&remaining[i + 1])?);
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Integer(0)),
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut members: std::collections::HashSet<Bytes> = first_zset
            .iter()
            .map(|(member, _)| member.clone())
            .collect();

        // Intersect with remaining sets
        for key in keys.iter().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Integer(0)),
            };
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            members.retain(|member| zset.contains(member));

            if members.is_empty() {
                return Ok(Response::Integer(0));
            }
        }

        let count = match limit {
            Some(l) if l > 0 => members.len().min(l),
            _ => members.len(),
        };

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZDIFF - Difference without store
// ============================================================================

pub struct ZdiffCommand;

impl Command for ZdiffCommand {
    fn name(&self) -> &'static str {
        "ZDIFF"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZDIFF numkeys key [key ...] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let remaining = &args[numkeys + 1..];

        // Check all keys are in same shard
        let first_shard = shard_for_key(&keys[0], ctx.num_shards);
        for key in &keys[1..] {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let with_scores = !remaining.is_empty()
            && remaining[0].to_ascii_uppercase().as_slice() == b"WITHSCORES";

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, f64> = first_zset
            .iter()
            .map(|(member, score)| (member.clone(), score))
            .collect();

        // Remove members that exist in other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                result.retain(|member, _| !zset.contains(member));
            }

            if result.is_empty() {
                return Ok(Response::Array(vec![]));
            }
        }

        // Build sorted result
        let mut members: Vec<(Bytes, f64)> = result.into_iter().collect();
        members.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        if with_scores {
            let response: Vec<Response> = members
                .into_iter()
                .flat_map(|(member, score)| {
                    vec![
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect();
            Ok(Response::Array(response))
        } else {
            let response: Vec<Response> = members
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect();
            Ok(Response::Array(response))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZDIFFSTORE - Difference and store
// ============================================================================

pub struct ZdiffstoreCommand;

impl Command for ZdiffstoreCommand {
    fn name(&self) -> &'static str {
        "ZDIFFSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZDIFFSTORE destination numkeys key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let numkeys = parse_usize(&args[1])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..numkeys + 2];

        // Check all keys (including dest) are in same shard
        let first_shard = shard_for_key(&dest, ctx.num_shards);
        for key in keys {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, f64> = first_zset
            .iter()
            .map(|(member, score)| (member.clone(), score))
            .collect();

        // Remove members that exist in other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                result.retain(|member, _| !zset.contains(member));
            }

            if result.is_empty() {
                break;
            }
        }

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, score) in result {
            new_zset.add(member, score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let mut keys = vec![args[0].as_ref()];
        keys.extend(args[2..].iter().take(numkeys).map(|a| a.as_ref()));
        keys
    }
}

// ============================================================================
// ZSCAN - Cursor-based iteration
// ============================================================================

pub struct ZscanCommand;

impl Command for ZscanCommand {
    fn name(&self) -> &'static str {
        "ZSCAN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZSCAN key cursor [MATCH pattern] [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor = parse_usize(&args[1])?;

        let mut match_pattern: Option<&[u8]> = None;
        let mut count: usize = 10;

        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    match_pattern = Some(&args[i + 1]);
                    i += 2;
                }
                b"COUNT" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&args[i + 1])?;
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => {
                return Ok(Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"0")),
                    Response::Array(vec![]),
                ]));
            }
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        // Simple cursor-based iteration (not true cursor, just offset)
        let all_members: Vec<_> = zset.to_vec();
        let total = all_members.len();

        if cursor >= total {
            return Ok(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"0")),
                Response::Array(vec![]),
            ]));
        }

        let mut result = Vec::new();
        let mut new_cursor = 0;

        for (i, (member, score)) in all_members.into_iter().enumerate().skip(cursor) {
            if result.len() >= count * 2 {
                new_cursor = i;
                break;
            }

            // Apply pattern match if specified
            if let Some(pattern) = match_pattern {
                if !simple_glob_match(pattern, &member) {
                    continue;
                }
            }

            result.push(Response::bulk(member));
            result.push(Response::bulk(Bytes::from(format_float(score))));
        }

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(new_cursor.to_string())),
            Response::Array(result),
        ]))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

/// Simple glob pattern matching (supports * and ?)
fn simple_glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut star_t = None;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_p = Some(p);
            star_t = Some(t);
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t = Some(star_t.unwrap() + 1);
            t = star_t.unwrap();
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

// ============================================================================
// ZRANGESTORE - Range and store
// ============================================================================

pub struct ZrangestoreCommand;

impl Command for ZrangestoreCommand {
    fn name(&self) -> &'static str {
        "ZRANGESTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let src = &args[1];

        // Check same shard
        if shard_for_key(&dest, ctx.num_shards) != shard_for_key(src, ctx.num_shards) {
            return Err(CommandError::CrossSlot);
        }

        // Parse options (similar to ZRANGE)
        let mut by_score = false;
        let mut by_lex = false;
        let mut rev = false;
        let mut limit_offset: usize = 0;
        let mut limit_count: Option<usize> = None;

        let mut i = 4;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"BYSCORE" => by_score = true,
                b"BYLEX" => by_lex = true,
                b"REV" => rev = true,
                b"LIMIT" => {
                    if i + 2 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    limit_offset = parse_usize(&args[i + 1])?;
                    let count = parse_i64(&args[i + 2])?;
                    limit_count = if count < 0 {
                        None
                    } else {
                        Some(count as usize)
                    };
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        if by_score && by_lex {
            return Err(CommandError::SyntaxError);
        }

        let value = match ctx.store.get(src) {
            Some(v) => v,
            None => {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let results = if by_score {
            let min = parse_score_bound(&args[2])?;
            let max = parse_score_bound(&args[3])?;

            if rev {
                zset.rev_range_by_score(&min, &max, limit_offset, limit_count)
            } else {
                zset.range_by_score(&min, &max, limit_offset, limit_count)
            }
        } else if by_lex {
            let min = parse_lex_bound(&args[2])?;
            let max = parse_lex_bound(&args[3])?;

            if rev {
                zset.rev_range_by_lex(&min, &max, limit_offset, limit_count)
            } else {
                zset.range_by_lex(&min, &max, limit_offset, limit_count)
            }
        } else {
            let start = parse_i64(&args[2])?;
            let end = parse_i64(&args[3])?;

            if rev {
                zset.rev_range_by_rank(start, end)
            } else {
                zset.range_by_rank(start, end)
            }
        };

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, score) in &results {
            new_zset.add(member.clone(), *score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            vec![&args[0], &args[1]]
        }
    }
}

// ============================================================================
// ZREMRANGEBYRANK - Remove by rank range
// ============================================================================

pub struct ZremrangebyrankCommand;

impl Command for ZremrangebyrankCommand {
    fn name(&self) -> &'static str {
        "ZREMRANGEBYRANK"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // ZREMRANGEBYRANK key start stop
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = parse_i64(&args[1])?;
        let stop = parse_i64(&args[2])?;

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Integer(0)),
        };

        let removed = zset.remove_range_by_rank(start, stop);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZREMRANGEBYSCORE - Remove by score range
// ============================================================================

pub struct ZremrangebyscoreCommand;

impl Command for ZremrangebyscoreCommand {
    fn name(&self) -> &'static str {
        "ZREMRANGEBYSCORE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // ZREMRANGEBYSCORE key min max
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_score_bound(&args[1])?;
        let max = parse_score_bound(&args[2])?;

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Integer(0)),
        };

        let removed = zset.remove_range_by_score(&min, &max);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// ZREMRANGEBYLEX - Remove by lex range
// ============================================================================

pub struct ZremrangebylexCommand;

impl Command for ZremrangebylexCommand {
    fn name(&self) -> &'static str {
        "ZREMRANGEBYLEX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // ZREMRANGEBYLEX key min max
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let min = parse_lex_bound(&args[1])?;
        let max = parse_lex_bound(&args[2])?;

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Integer(0)),
        };

        let removed = zset.remove_range_by_lex(&min, &max);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
