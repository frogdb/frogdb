use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeyAccessFlag, KeySpec, KeyspaceEventFlags, LookupSpec,
    SortedSetValue, StoreTypedFamilyExt, Value, WaiterWake, WalStrategy, shard_for_key,
};
use frogdb_protocol::Response;

use crate::utils::{parse_i64, parse_lex_bound, parse_limit_clause, parse_score_bound};

// ============================================================================
// ZRANGESTORE - Range and store
// ============================================================================

pub struct ZrangestoreCommand;

impl Command for ZrangestoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZRANGESTORE",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            // Destination (key 0) is overwritten; the source (key 1) is read-only.
            access: AccessSpec::Positional(&[KeyAccessFlag::OW, KeyAccessFlag::R]),
            wal: WalStrategy::PersistDestination,
            wakes: WaiterWake::None,
            event: EventSpec::EmitsAt {
                class: KeyspaceEventFlags::ZSET,
                name: "zrangestore",
                key_index: 0,
            },
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
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

        let mut parser = ArgParser::from_position(args, 4);
        while parser.has_more() {
            if parser.try_flag(b"BYSCORE") {
                by_score = true;
            } else if parser.try_flag(b"BYLEX") {
                by_lex = true;
            } else if parser.try_flag(b"REV") {
                rev = true;
            } else if parser.try_flag(b"LIMIT") {
                has_limit = true;
                (limit_offset, limit_count) = parse_limit_clause(&mut parser)?;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        if by_score && by_lex {
            return Err(CommandError::SyntaxError);
        }

        // LIMIT requires BYSCORE or BYLEX
        if has_limit && !by_score && !by_lex {
            return Err(CommandError::SyntaxError);
        }

        let Some(zset) = ctx.store.get_zset(src)? else {
            ctx.store.delete(&dest);
            return Ok(Response::Integer(0));
        };

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
}

// ============================================================================
// ZREMRANGEBYRANK - Remove by rank range
// ============================================================================

pub struct ZremrangebyrankCommand;

impl Command for ZremrangebyrankCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREMRANGEBYRANK",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
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
        let start = parse_i64(&args[1])?;
        let stop = parse_i64(&args[2])?;

        let zset = match ctx.store.get_zset_mut(key)? {
            Some(zset) => zset,
            None => return Ok(Response::Integer(0)),
        };

        let removed = zset.remove_range_by_rank(start, stop);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }
}

// ============================================================================
// ZREMRANGEBYSCORE - Remove by score range
// ============================================================================

pub struct ZremrangebyscoreCommand;

impl Command for ZremrangebyscoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREMRANGEBYSCORE",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
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
        let min = parse_score_bound(&args[1])?;
        let max = parse_score_bound(&args[2])?;

        let zset = match ctx.store.get_zset_mut(key)? {
            Some(zset) => zset,
            None => return Ok(Response::Integer(0)),
        };

        let removed = zset.remove_range_by_score(&min, &max);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }
}

// ============================================================================
// ZREMRANGEBYLEX - Remove by lex range
// ============================================================================

pub struct ZremrangebylexCommand;

impl Command for ZremrangebylexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZREMRANGEBYLEX",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
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
        let min = parse_lex_bound(&args[1])?;
        let max = parse_lex_bound(&args[2])?;

        let zset = match ctx.store.get_zset_mut(key)? {
            Some(zset) => zset,
            None => return Ok(Response::Integer(0)),
        };

        let removed = zset.remove_range_by_lex(&min, &max);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }
}
