use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, SortedSetValue, Value, WalStrategy,
    impl_keys_first, shard_for_key,
};
use frogdb_protocol::Response;

use crate::utils::{parse_i64, parse_lex_bound, parse_score_bound, parse_usize};

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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistDestination(0)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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
        let mut has_limit = false;

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
                    has_limit = true;
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

        // LIMIT requires BYSCORE or BYLEX
        if has_limit && !by_score && !by_lex {
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
            // In REV mode, args[2] is the max and args[3] is the min (swapped from forward)
            if rev {
                let max = parse_score_bound(&args[2])?;
                let min = parse_score_bound(&args[3])?;
                zset.rev_range_by_score(&min, &max, limit_offset, limit_count)
            } else {
                let min = parse_score_bound(&args[2])?;
                let max = parse_score_bound(&args[3])?;
                zset.range_by_score(&min, &max, limit_offset, limit_count)
            }
        } else if by_lex {
            // In REV mode, args[2] is the max and args[3] is the min (swapped from forward)
            if rev {
                let max = parse_lex_bound(&args[2])?;
                let min = parse_lex_bound(&args[3])?;
                zset.rev_range_by_lex(&min, &max, limit_offset, limit_count)
            } else {
                let min = parse_lex_bound(&args[2])?;
                let max = parse_lex_bound(&args[3])?;
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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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

    impl_keys_first!();
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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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

    impl_keys_first!();
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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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

    impl_keys_first!();
}
