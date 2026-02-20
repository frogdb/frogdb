use bytes::Bytes;
use frogdb_core::{impl_keys_first, Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use crate::commands::utils::{format_float, get_or_create_zset, parse_f64, ZaddOptions};

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

        // Parse options using shared utility
        let (opts, i) = ZaddOptions::parse(args, 1)?;
        let nx = opts.nx_xx.nx;
        let xx = opts.nx_xx.xx;
        let gt = opts.gt_lt.gt;
        let lt = opts.gt_lt.lt;
        let ch = opts.ch;
        let incr = opts.incr;

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

    impl_keys_first!();
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

    impl_keys_first!();
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

    impl_keys_first!();
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

    impl_keys_first!();
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

    impl_keys_first!();
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

    impl_keys_first!();
}
