//! Blocking list and sorted set commands.
//!
//! These commands block the client until data is available or a timeout occurs.
//! They are implemented using the wait queue infrastructure in ShardWorker.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, KeyspaceEventFlags, WaiterWake, WalStrategy,
};
use frogdb_protocol::{BlockingOp, Direction, Response};

use crate::utils::{parse_i64, parse_usize, score_response};

// ============================================================================
// BLPOP - Blocking left pop
// ============================================================================

/// BLPOP key [key ...] timeout
///
/// Removes and returns the first element of one of the lists, blocking until
/// one is available or the timeout is reached.
pub struct BlpopCommand;

impl Command for BlpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BLPOP",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE
                .union(CommandFlags::BLOCKING)
                .union(CommandFlags::FAST),
            keys: KeySpec::AllButLast,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lpop",
            },
            requires_same_slot: true, // All keys must be in the same shard,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity { command: "blpop" });
        }

        // Last argument is timeout
        let timeout_bytes = &args[args.len() - 1];
        let timeout = parse_timeout(timeout_bytes)?;
        let keys = &args[..args.len() - 1];

        // Try immediate pop from left
        for key in keys {
            if let Some(value) = ctx.store.get(key) {
                if value.key_type() != frogdb_core::KeyType::List {
                    return Err(CommandError::WrongType);
                }
                if let Some(list) = value.as_list()
                    && !list.is_empty()
                {
                    // Pop from left
                    if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut())
                        && let Some(elem) = list_mut.pop_front()
                    {
                        // Delete empty list
                        if list_mut.is_empty() {
                            ctx.store.delete(key);
                        }
                        return Ok(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::bulk(elem),
                        ]));
                    }
                }
            }
        }

        // No data available - signal that blocking is needed
        // Return a special response that the connection handler recognizes
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BLPop,
        })
    }
}

// ============================================================================
// BRPOP - Blocking right pop
// ============================================================================

/// BRPOP key [key ...] timeout
///
/// Removes and returns the last element of one of the lists, blocking until
/// one is available or the timeout is reached.
pub struct BrpopCommand;

impl Command for BrpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BRPOP",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE
                .union(CommandFlags::BLOCKING)
                .union(CommandFlags::FAST),
            keys: KeySpec::AllButLast,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "rpop",
            },
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity { command: "brpop" });
        }

        // Last argument is timeout
        let timeout_bytes = &args[args.len() - 1];
        let timeout = parse_timeout(timeout_bytes)?;
        let keys = &args[..args.len() - 1];

        // Try immediate pop from right
        for key in keys {
            if let Some(value) = ctx.store.get(key) {
                if value.key_type() != frogdb_core::KeyType::List {
                    return Err(CommandError::WrongType);
                }
                if let Some(list) = value.as_list()
                    && !list.is_empty()
                {
                    // Pop from right
                    if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut())
                        && let Some(elem) = list_mut.pop_back()
                    {
                        // Delete empty list
                        if list_mut.is_empty() {
                            ctx.store.delete(key);
                        }
                        return Ok(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::bulk(elem),
                        ]));
                    }
                }
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BRPop,
        })
    }
}

// ============================================================================
// BLMOVE - Blocking list move
// ============================================================================

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
///
/// Pops an element from a list, pushes it to another list, and returns it.
/// Blocks if the source list is empty.
pub struct BlmoveCommand;

impl Command for BlmoveCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BLMOVE",
            arity: Arity::Fixed(5),
            flags: CommandFlags::WRITE.union(CommandFlags::BLOCKING),
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Uniform,
            wal: WalStrategy::MoveKeys,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lmove",
            },
            requires_same_slot: true, // Source and destination must be in same shard,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() != 5 {
            return Err(CommandError::WrongArity { command: "blmove" });
        }

        let source = &args[0];
        let dest = &args[1];
        let src_dir = Direction::parse(&args[2]).ok_or(CommandError::SyntaxError)?;
        let dest_dir = Direction::parse(&args[3]).ok_or(CommandError::SyntaxError)?;
        let timeout = parse_timeout(&args[4])?;

        // Try immediate operation
        if let Some(value) = ctx.store.get(source) {
            if value.key_type() != frogdb_core::KeyType::List {
                return Err(CommandError::WrongType);
            }
            if let Some(list) = value.as_list()
                && !list.is_empty()
            {
                // Pop from source
                let elem = if let Some(list_mut) =
                    ctx.store.get_mut(source).and_then(|v| v.as_list_mut())
                {
                    match src_dir {
                        Direction::Left => list_mut.pop_front(),
                        Direction::Right => list_mut.pop_back(),
                    }
                } else {
                    None
                };

                if let Some(elem) = elem {
                    // Delete empty source list
                    delete_if_empty_list(ctx, source);

                    // Create destination list if needed
                    if ctx.store.get(dest).is_none() {
                        ctx.store.set(dest.clone(), frogdb_core::Value::list());
                    } else if let Some(v) = ctx.store.get(dest)
                        && v.key_type() != frogdb_core::KeyType::List
                    {
                        return Err(CommandError::WrongType);
                    }

                    // Push to destination
                    if let Some(dest_list) = ctx.store.get_mut(dest).and_then(|v| v.as_list_mut()) {
                        match dest_dir {
                            Direction::Left => dest_list.push_front(elem.clone()),
                            Direction::Right => dest_list.push_back(elem.clone()),
                        }
                    }

                    return Ok(Response::bulk(elem));
                }
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: vec![source.clone()],
            timeout,
            op: BlockingOp::BLMove {
                dest: dest.clone(),
                src_dir,
                dest_dir,
            },
        })
    }
}

// ============================================================================
// BLMPOP - Blocking list multi-pop
// ============================================================================

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// Pops one or more elements from the first non-empty list.
pub struct BlmpopCommand;

impl Command for BlmpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BLMPOP",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::BLOCKING),
            keys: KeySpec::NumkeysAt {
                numkeys: 1,
                first: 2,
            },
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 4 {
            return Err(CommandError::WrongArity { command: "blmpop" });
        }

        let timeout = parse_timeout(&args[0])?;
        let numkeys = parse_usize(&args[1]).map_err(|_| CommandError::InvalidArgument {
            message: "numkeys can't be non-positive value".to_string(),
        })?;

        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..2 + numkeys];
        let direction_idx = 2 + numkeys;

        if direction_idx >= args.len() {
            return Err(CommandError::SyntaxError);
        }

        let direction = Direction::parse(&args[direction_idx]).ok_or(CommandError::SyntaxError)?;

        // Parse optional COUNT
        let mut count = 1usize;
        let mut count_seen = false;
        let mut i = direction_idx + 1;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"COUNT") {
                if count_seen {
                    return Err(CommandError::SyntaxError);
                }
                count_seen = true;
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let c = parse_i64(&args[i + 1])?;
                if c <= 0 {
                    return Err(CommandError::InvalidArgument {
                        message: "count must be positive".to_string(),
                    });
                }
                count = c as usize;
                i += 2;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Try immediate pop
        for key in keys {
            if let Some(value) = ctx.store.get(key) {
                if value.key_type() != frogdb_core::KeyType::List {
                    return Err(CommandError::WrongType);
                }
                if let Some(list) = value.as_list()
                    && !list.is_empty()
                {
                    let mut elements = Vec::new();

                    if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut()) {
                        for _ in 0..count {
                            let elem = match direction {
                                Direction::Left => list_mut.pop_front(),
                                Direction::Right => list_mut.pop_back(),
                            };
                            match elem {
                                Some(e) => elements.push(Response::bulk(e)),
                                None => break,
                            }
                        }
                    }

                    if !elements.is_empty() {
                        // Delete empty list
                        delete_if_empty_list(ctx, key);

                        return Ok(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::Array(elements),
                        ]));
                    }
                }
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BLMPop { direction, count },
        })
    }
}

// ============================================================================
// BZPOPMIN - Blocking sorted set pop minimum
// ============================================================================

/// BZPOPMIN key [key ...] timeout
///
/// Removes and returns the member with the lowest score from one of the
/// sorted sets, blocking until one is available or the timeout is reached.
pub struct BzpopminCommand;

impl Command for BzpopminCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BZPOPMIN",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE
                .union(CommandFlags::BLOCKING)
                .union(CommandFlags::FAST),
            keys: KeySpec::AllButLast,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::ZSET,
                name: "zpopmin",
            },
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "bzpopmin",
            });
        }

        let timeout_bytes = &args[args.len() - 1];
        let timeout = parse_timeout(timeout_bytes)?;
        let keys = &args[..args.len() - 1];

        // Try immediate pop
        for key in keys {
            if let Some(value) = ctx.store.get(key) {
                if value.key_type() != frogdb_core::KeyType::SortedSet {
                    return Err(CommandError::WrongType);
                }
                if let Some(zset) = value.as_sorted_set()
                    && !zset.is_empty()
                    && let Some(zset_mut) =
                        ctx.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                {
                    let popped = zset_mut.pop_min(1);
                    if let Some((member, score)) = popped.into_iter().next() {
                        // Delete empty zset
                        if zset_mut.is_empty() {
                            ctx.store.delete(key);
                        }
                        let is_resp3 = ctx.protocol_version.is_resp3();
                        return Ok(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::bulk(member),
                            score_response(score, is_resp3),
                        ]));
                    }
                }
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BZPopMin,
        })
    }
}

// ============================================================================
// BZPOPMAX - Blocking sorted set pop maximum
// ============================================================================

/// BZPOPMAX key [key ...] timeout
///
/// Removes and returns the member with the highest score from one of the
/// sorted sets, blocking until one is available or the timeout is reached.
pub struct BzpopmaxCommand;

impl Command for BzpopmaxCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BZPOPMAX",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE
                .union(CommandFlags::BLOCKING)
                .union(CommandFlags::FAST),
            keys: KeySpec::AllButLast,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::ZSET,
                name: "zpopmax",
            },
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "bzpopmax",
            });
        }

        let timeout_bytes = &args[args.len() - 1];
        let timeout = parse_timeout(timeout_bytes)?;
        let keys = &args[..args.len() - 1];

        // Try immediate pop
        for key in keys {
            if let Some(value) = ctx.store.get(key) {
                if value.key_type() != frogdb_core::KeyType::SortedSet {
                    return Err(CommandError::WrongType);
                }
                if let Some(zset) = value.as_sorted_set()
                    && !zset.is_empty()
                    && let Some(zset_mut) =
                        ctx.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                {
                    let popped = zset_mut.pop_max(1);
                    if let Some((member, score)) = popped.into_iter().next() {
                        // Delete empty zset
                        if zset_mut.is_empty() {
                            ctx.store.delete(key);
                        }
                        let is_resp3 = ctx.protocol_version.is_resp3();
                        return Ok(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::bulk(member),
                            score_response(score, is_resp3),
                        ]));
                    }
                }
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BZPopMax,
        })
    }
}

// ============================================================================
// BZMPOP - Blocking sorted set multi-pop
// ============================================================================

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
///
/// Pops one or more elements from the first non-empty sorted set.
pub struct BzmpopCommand;

impl Command for BzmpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BZMPOP",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::BLOCKING),
            keys: KeySpec::NumkeysAt {
                numkeys: 1,
                first: 2,
            },
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 4 {
            return Err(CommandError::WrongArity { command: "bzmpop" });
        }

        let timeout = parse_timeout(&args[0])?;
        let numkeys = parse_usize(&args[1]).map_err(|_| CommandError::InvalidArgument {
            message: "numkeys can't be non-positive value".to_string(),
        })?;

        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..2 + numkeys];
        let minmax_idx = 2 + numkeys;

        if minmax_idx >= args.len() {
            return Err(CommandError::SyntaxError);
        }

        let min = if args[minmax_idx].eq_ignore_ascii_case(b"MIN") {
            true
        } else if args[minmax_idx].eq_ignore_ascii_case(b"MAX") {
            false
        } else {
            return Err(CommandError::SyntaxError);
        };

        // Parse optional COUNT
        let mut count = 1usize;
        let mut count_seen = false;
        let mut i = minmax_idx + 1;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"COUNT") {
                if count_seen {
                    return Err(CommandError::SyntaxError);
                }
                count_seen = true;
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let c = parse_i64(&args[i + 1])?;
                if c <= 0 {
                    return Err(CommandError::InvalidArgument {
                        message: "count must be positive".to_string(),
                    });
                }
                count = c as usize;
                i += 2;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Try immediate pop
        for key in keys {
            if let Some(value) = ctx.store.get(key) {
                if value.key_type() != frogdb_core::KeyType::SortedSet {
                    return Err(CommandError::WrongType);
                }
                if let Some(zset) = value.as_sorted_set()
                    && !zset.is_empty()
                {
                    let mut elements = Vec::new();
                    let is_resp3 = ctx.protocol_version.is_resp3();

                    if let Some(zset_mut) =
                        ctx.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                    {
                        let popped = if min {
                            zset_mut.pop_min(count)
                        } else {
                            zset_mut.pop_max(count)
                        };
                        for (member, score) in popped {
                            elements.push(Response::Array(vec![
                                Response::bulk(member),
                                score_response(score, is_resp3),
                            ]));
                        }
                    }

                    if !elements.is_empty() {
                        // Delete empty zset
                        delete_if_empty_zset(ctx, key);

                        return Ok(Response::Array(vec![
                            Response::bulk(key.clone()),
                            Response::Array(elements),
                        ]));
                    }
                }
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BZMPop { min, count },
        })
    }
}

// ============================================================================
// BRPOPLPUSH - Blocking right pop, left push (deprecated, for compatibility)
// ============================================================================

/// BRPOPLPUSH source destination timeout
///
/// Atomically pops from the tail of source and pushes to the head of destination.
/// Deprecated in favor of BLMOVE, but included for backwards compatibility.
pub struct BrpoplpushCommand;

impl Command for BrpoplpushCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "BRPOPLPUSH",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::BLOCKING),
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Uniform,
            wal: WalStrategy::MoveKeys,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() != 3 {
            return Err(CommandError::WrongArity {
                command: "brpoplpush",
            });
        }

        let source = &args[0];
        let dest = &args[1];
        let timeout = parse_timeout(&args[2])?;

        // Try immediate operation
        if let Some(value) = ctx.store.get(source) {
            if value.key_type() != frogdb_core::KeyType::List {
                return Err(CommandError::WrongType);
            }
            if let Some(list) = value.as_list()
                && !list.is_empty()
            {
                // Pop from right of source
                let elem = if let Some(list_mut) =
                    ctx.store.get_mut(source).and_then(|v| v.as_list_mut())
                {
                    list_mut.pop_back()
                } else {
                    None
                };

                if let Some(elem) = elem {
                    // Delete empty source list
                    delete_if_empty_list(ctx, source);

                    // Create destination list if needed
                    if ctx.store.get(dest).is_none() {
                        ctx.store.set(dest.clone(), frogdb_core::Value::list());
                    } else if let Some(v) = ctx.store.get(dest)
                        && v.key_type() != frogdb_core::KeyType::List
                    {
                        return Err(CommandError::WrongType);
                    }

                    // Push to left of destination
                    if let Some(dest_list) = ctx.store.get_mut(dest).and_then(|v| v.as_list_mut()) {
                        dest_list.push_front(elem.clone());
                    }

                    return Ok(Response::bulk(elem));
                }
            }
        }

        // No data available - signal that blocking is needed
        // BRPOPLPUSH is equivalent to BLMOVE source dest RIGHT LEFT
        Ok(Response::BlockingNeeded {
            keys: vec![source.clone()],
            timeout,
            op: BlockingOp::BLMove {
                dest: dest.clone(),
                src_dir: Direction::Right,
                dest_dir: Direction::Left,
            },
        })
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse a timeout argument (seconds, can be a float, 0 = block forever).
fn parse_timeout(arg: &[u8]) -> Result<f64, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::SyntaxError)?;
    let timeout: f64 = s.parse().map_err(|_| CommandError::InvalidArgument {
        message: "timeout is not a float or out of range".to_string(),
    })?;
    if timeout.is_infinite() || timeout.is_nan() {
        return Err(CommandError::InvalidArgument {
            message: "timeout is out of range".to_string(),
        });
    }
    if timeout < 0.0 {
        return Err(CommandError::InvalidArgument {
            message: "timeout is negative".to_string(),
        });
    }
    Ok(timeout)
}

/// Delete a list key if it's empty.
fn delete_if_empty_list(ctx: &mut CommandContext, key: &Bytes) {
    if let Some(value) = ctx.store.get(key)
        && let Some(list) = value.as_list()
        && list.is_empty()
    {
        ctx.store.delete(key);
    }
}

/// Delete a sorted set key if it's empty.
fn delete_if_empty_zset(ctx: &mut CommandContext, key: &Bytes) {
    if let Some(value) = ctx.store.get(key)
        && let Some(zset) = value.as_sorted_set()
        && zset.is_empty()
    {
        ctx.store.delete(key);
    }
}
