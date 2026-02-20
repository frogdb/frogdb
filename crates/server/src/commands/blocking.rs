//! Blocking list and sorted set commands.
//!
//! These commands block the client until data is available or a timeout occurs.
//! They are implemented using the wait queue infrastructure in ShardWorker.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy};
use frogdb_protocol::{BlockingOp, Direction, Response};

// ============================================================================
// BLPOP - Blocking left pop
// ============================================================================

/// BLPOP key [key ...] timeout
///
/// Removes and returns the first element of one of the lists, blocking until
/// one is available or the timeout is reached.
pub struct BlpopCommand;

impl Command for BlpopCommand {
    fn name(&self) -> &'static str {
        "BLPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // key [key ...] timeout
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity { command: "BLPOP" });
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
                if let Some(list) = value.as_list() {
                    if !list.is_empty() {
                        // Pop from left
                        if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut())
                        {
                            if let Some(elem) = list_mut.pop_front() {
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }
        args[..args.len() - 1].iter().map(|b| b.as_ref()).collect()
    }

    fn requires_same_slot(&self) -> bool {
        true // All keys must be in the same shard
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
    fn name(&self) -> &'static str {
        "BRPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // key [key ...] timeout
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity { command: "BRPOP" });
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
                if let Some(list) = value.as_list() {
                    if !list.is_empty() {
                        // Pop from right
                        if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut())
                        {
                            if let Some(elem) = list_mut.pop_back() {
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
            }
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BRPop,
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }
        args[..args.len() - 1].iter().map(|b| b.as_ref()).collect()
    }

    fn requires_same_slot(&self) -> bool {
        true
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
    fn name(&self) -> &'static str {
        "BLMOVE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(5) // source dest src_dir dest_dir timeout
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() != 5 {
            return Err(CommandError::WrongArity { command: "BLMOVE" });
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
            if let Some(list) = value.as_list() {
                if !list.is_empty() {
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
                        } else if let Some(v) = ctx.store.get(dest) {
                            if v.key_type() != frogdb_core::KeyType::List {
                                return Err(CommandError::WrongType);
                            }
                        }

                        // Push to destination
                        if let Some(dest_list) =
                            ctx.store.get_mut(dest).and_then(|v| v.as_list_mut())
                        {
                            match dest_dir {
                                Direction::Left => dest_list.push_front(elem.clone()),
                                Direction::Right => dest_list.push_back(elem.clone()),
                            }
                        }

                        return Ok(Response::bulk(elem));
                    }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            vec![&args[0], &args[1]]
        } else {
            vec![]
        }
    }

    fn requires_same_slot(&self) -> bool {
        true // Source and destination must be in same shard
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
    fn name(&self) -> &'static str {
        "BLMPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // timeout numkeys key direction
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 4 {
            return Err(CommandError::WrongArity { command: "BLMPOP" });
        }

        let timeout = parse_timeout(&args[0])?;
        let numkeys: usize = parse_int(&args[1])? as usize;

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
        let mut i = direction_idx + 1;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"COUNT") {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = parse_int(&args[i + 1])? as usize;
                if count == 0 {
                    return Err(CommandError::InvalidArgument {
                        message: "count must be positive".to_string(),
                    });
                }
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
                if let Some(list) = value.as_list() {
                    if !list.is_empty() {
                        let mut elements = Vec::new();

                        if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut())
                        {
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
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BLMPop { direction, count },
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 4 {
            return vec![];
        }
        let numkeys: usize = match std::str::from_utf8(&args[1]) {
            Ok(s) => s.parse().unwrap_or(0),
            Err(_) => 0,
        };
        if numkeys == 0 || args.len() < 2 + numkeys {
            return vec![];
        }
        args[2..2 + numkeys].iter().map(|b| b.as_ref()).collect()
    }

    fn requires_same_slot(&self) -> bool {
        true
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
    fn name(&self) -> &'static str {
        "BZPOPMIN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "BZPOPMIN",
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
                if let Some(zset) = value.as_sorted_set() {
                    if !zset.is_empty() {
                        if let Some(zset_mut) =
                            ctx.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                        {
                            let popped = zset_mut.pop_min(1);
                            if let Some((member, score)) = popped.into_iter().next() {
                                // Delete empty zset
                                if zset_mut.is_empty() {
                                    ctx.store.delete(key);
                                }
                                return Ok(Response::Array(vec![
                                    Response::bulk(key.clone()),
                                    Response::bulk(member),
                                    Response::bulk(Bytes::from(score.to_string())),
                                ]));
                            }
                        }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }
        args[..args.len() - 1].iter().map(|b| b.as_ref()).collect()
    }

    fn requires_same_slot(&self) -> bool {
        true
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
    fn name(&self) -> &'static str {
        "BZPOPMAX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "BZPOPMAX",
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
                if let Some(zset) = value.as_sorted_set() {
                    if !zset.is_empty() {
                        if let Some(zset_mut) =
                            ctx.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                        {
                            let popped = zset_mut.pop_max(1);
                            if let Some((member, score)) = popped.into_iter().next() {
                                // Delete empty zset
                                if zset_mut.is_empty() {
                                    ctx.store.delete(key);
                                }
                                return Ok(Response::Array(vec![
                                    Response::bulk(key.clone()),
                                    Response::bulk(member),
                                    Response::bulk(Bytes::from(score.to_string())),
                                ]));
                            }
                        }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }
        args[..args.len() - 1].iter().map(|b| b.as_ref()).collect()
    }

    fn requires_same_slot(&self) -> bool {
        true
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
    fn name(&self) -> &'static str {
        "BZMPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() < 4 {
            return Err(CommandError::WrongArity { command: "BZMPOP" });
        }

        let timeout = parse_timeout(&args[0])?;
        let numkeys: usize = parse_int(&args[1])? as usize;

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
        let mut i = minmax_idx + 1;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"COUNT") {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = parse_int(&args[i + 1])? as usize;
                if count == 0 {
                    return Err(CommandError::InvalidArgument {
                        message: "count must be positive".to_string(),
                    });
                }
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
                if let Some(zset) = value.as_sorted_set() {
                    if !zset.is_empty() {
                        let mut elements = Vec::new();

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
                                    Response::bulk(Bytes::from(score.to_string())),
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
        }

        // No data available - signal that blocking is needed
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BZMPop { min, count },
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 4 {
            return vec![];
        }
        let numkeys: usize = match std::str::from_utf8(&args[1]) {
            Ok(s) => s.parse().unwrap_or(0),
            Err(_) => 0,
        };
        if numkeys == 0 || args.len() < 2 + numkeys {
            return vec![];
        }
        args[2..2 + numkeys].iter().map(|b| b.as_ref()).collect()
    }

    fn requires_same_slot(&self) -> bool {
        true
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
    fn name(&self) -> &'static str {
        "BRPOPLPUSH"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // source destination timeout
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking {
            default_timeout: None,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() != 3 {
            return Err(CommandError::WrongArity {
                command: "BRPOPLPUSH",
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
            if let Some(list) = value.as_list() {
                if !list.is_empty() {
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
                        } else if let Some(v) = ctx.store.get(dest) {
                            if v.key_type() != frogdb_core::KeyType::List {
                                return Err(CommandError::WrongType);
                            }
                        }

                        // Push to left of destination
                        if let Some(dest_list) =
                            ctx.store.get_mut(dest).and_then(|v| v.as_list_mut())
                        {
                            dest_list.push_front(elem.clone());
                        }

                        return Ok(Response::bulk(elem));
                    }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            vec![&args[0], &args[1]]
        } else {
            vec![]
        }
    }

    fn requires_same_slot(&self) -> bool {
        true
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
    if timeout < 0.0 {
        return Err(CommandError::InvalidArgument {
            message: "timeout is negative".to_string(),
        });
    }
    Ok(timeout)
}

/// Parse an integer argument.
fn parse_int(arg: &[u8]) -> Result<i64, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::NotInteger)?;
    s.parse().map_err(|_| CommandError::NotInteger)
}

/// Delete a list key if it's empty.
fn delete_if_empty_list(ctx: &mut CommandContext, key: &Bytes) {
    if let Some(value) = ctx.store.get(key) {
        if let Some(list) = value.as_list() {
            if list.is_empty() {
                ctx.store.delete(key);
            }
        }
    }
}

/// Delete a sorted set key if it's empty.
fn delete_if_empty_zset(ctx: &mut CommandContext, key: &Bytes) {
    if let Some(value) = ctx.store.get(key) {
        if let Some(zset) = value.as_sorted_set() {
            if zset.is_empty() {
                ctx.store.delete(key);
            }
        }
    }
}
