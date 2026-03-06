//! List commands.
//!
//! Commands for list manipulation:
//! - LPUSH, RPUSH, LPUSHX, RPUSHX - push operations
//! - LPOP, RPOP - pop operations
//! - LLEN - length
//! - LRANGE - range queries
//! - LINDEX, LSET - element access
//! - LINSERT, LREM, LTRIM - modification
//! - LPOS - find element position
//! - LMOVE, LMPOP - advanced operations
//!
//! Note: Blocking commands (BLPOP, BRPOP, BLMOVE) deferred to Phase 11.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, WalStrategy};
use frogdb_protocol::Response;

use super::utils::{get_or_create_list, parse_i64, parse_usize};

// ============================================================================
// LPUSH - Push elements to front of list
// ============================================================================

pub struct LpushCommand;

impl Command for LpushCommand {
    fn name(&self) -> &'static str {
        "LPUSH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // LPUSH key element [element ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let list = get_or_create_list(ctx, key)?;

        // Push in order - first arg first (ends up at tail of head)
        for elem in &args[1..] {
            list.push_front(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
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
// RPUSH - Push elements to back of list
// ============================================================================

pub struct RpushCommand;

impl Command for RpushCommand {
    fn name(&self) -> &'static str {
        "RPUSH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // RPUSH key element [element ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let list = get_or_create_list(ctx, key)?;

        for elem in &args[1..] {
            list.push_back(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
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
// LPUSHX - Push to front only if list exists
// ============================================================================

pub struct LpushxCommand;

impl Command for LpushxCommand {
    fn name(&self) -> &'static str {
        "LPUSHX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // LPUSHX key element [element ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        match ctx.store.get(key) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::Integer(0)),
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

        for elem in &args[1..] {
            list.push_front(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
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
// RPUSHX - Push to back only if list exists
// ============================================================================

pub struct RpushxCommand;

impl Command for RpushxCommand {
    fn name(&self) -> &'static str {
        "RPUSHX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // RPUSHX key element [element ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        match ctx.store.get(key) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::Integer(0)),
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

        for elem in &args[1..] {
            list.push_back(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
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
// LPOP - Pop elements from front
// ============================================================================

pub struct LpopCommand;

impl Command for LpopCommand {
    fn name(&self) -> &'static str {
        "LPOP"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 1, max: 2 } // LPOP key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_usize(&args[1])?)
        } else {
            None
        };

        // Check if key exists
        if ctx.store.get(key).is_none() {
            return Ok(if count.is_some() {
                Response::NullArray
            } else {
                Response::null()
            });
        }

        // Verify type
        if ctx.store.get(key).unwrap().as_list().is_none() {
            return Err(CommandError::WrongType);
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

        match count {
            Some(c) => {
                let mut results = Vec::with_capacity(c);
                for _ in 0..c {
                    if let Some(elem) = list.pop_front() {
                        results.push(Response::bulk(elem));
                    } else {
                        break;
                    }
                }

                // Delete key if list is now empty
                if list.is_empty() {
                    ctx.store.delete(key);
                }

                // When count arg is present, always return array (even if empty)
                Ok(Response::Array(results))
            }
            None => {
                let result = list.pop_front();

                // Delete key if list is now empty
                if list.is_empty() {
                    ctx.store.delete(key);
                }

                match result {
                    Some(elem) => Ok(Response::bulk(elem)),
                    None => Ok(Response::null()),
                }
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
// RPOP - Pop elements from back
// ============================================================================

pub struct RpopCommand;

impl Command for RpopCommand {
    fn name(&self) -> &'static str {
        "RPOP"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 1, max: 2 } // RPOP key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_usize(&args[1])?)
        } else {
            None
        };

        // Check if key exists
        if ctx.store.get(key).is_none() {
            return Ok(if count.is_some() {
                Response::NullArray
            } else {
                Response::null()
            });
        }

        // Verify type
        if ctx.store.get(key).unwrap().as_list().is_none() {
            return Err(CommandError::WrongType);
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

        match count {
            Some(c) => {
                let mut results = Vec::with_capacity(c);
                for _ in 0..c {
                    if let Some(elem) = list.pop_back() {
                        results.push(Response::bulk(elem));
                    } else {
                        break;
                    }
                }

                // Delete key if list is now empty
                if list.is_empty() {
                    ctx.store.delete(key);
                }

                // When count arg is present, always return array (even if empty)
                Ok(Response::Array(results))
            }
            None => {
                let result = list.pop_back();

                // Delete key if list is now empty
                if list.is_empty() {
                    ctx.store.delete(key);
                }

                match result {
                    Some(elem) => Ok(Response::bulk(elem)),
                    None => Ok(Response::null()),
                }
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
// LLEN - Get list length
// ============================================================================

pub struct LlenCommand;

impl Command for LlenCommand {
    fn name(&self) -> &'static str {
        "LLEN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // LLEN key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                if let Some(list) = value.as_list() {
                    Ok(Response::Integer(list.len() as i64))
                } else {
                    Err(CommandError::WrongType)
                }
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
// LRANGE - Get range of elements
// ============================================================================

pub struct LrangeCommand;

impl Command for LrangeCommand {
    fn name(&self) -> &'static str {
        "LRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // LRANGE key start stop
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = parse_i64(&args[1])?;
        let stop = parse_i64(&args[2])?;

        match ctx.store.get(key) {
            Some(value) => {
                if let Some(list) = value.as_list() {
                    let results: Vec<Response> = list
                        .range_iter(start, stop)
                        .map(|b| Response::bulk(b.clone()))
                        .collect();
                    Ok(Response::Array(results))
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => Ok(Response::Array(vec![])),
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
// LINDEX - Get element by index
// ============================================================================

pub struct LindexCommand;

impl Command for LindexCommand {
    fn name(&self) -> &'static str {
        "LINDEX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // LINDEX key index
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let index = parse_i64(&args[1])?;

        match ctx.store.get(key) {
            Some(value) => {
                if let Some(list) = value.as_list() {
                    match list.get(index) {
                        Some(elem) => Ok(Response::bulk(elem.clone())),
                        None => Ok(Response::null()),
                    }
                } else {
                    Err(CommandError::WrongType)
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
// LSET - Set element by index
// ============================================================================

pub struct LsetCommand;

impl Command for LsetCommand {
    fn name(&self) -> &'static str {
        "LSET"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // LSET key index element
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let index = parse_i64(&args[1])?;
        let element = args[2].clone();

        // Check if key exists
        match ctx.store.get(key) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => {
                return Err(CommandError::InvalidArgument {
                    message: "no such key".to_string(),
                });
            }
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

        if list.set(index, element) {
            Ok(Response::ok())
        } else {
            Err(CommandError::InvalidArgument {
                message: "index out of range".to_string(),
            })
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
// LINSERT - Insert element before/after pivot
// ============================================================================

pub struct LinsertCommand;

impl Command for LinsertCommand {
    fn name(&self) -> &'static str {
        "LINSERT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(4) // LINSERT key BEFORE|AFTER pivot element
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let position = args[1].to_ascii_uppercase();
        let pivot = &args[2];
        let element = args[3].clone();

        let before = match position.as_slice() {
            b"BEFORE" => true,
            b"AFTER" => false,
            _ => return Err(CommandError::SyntaxError),
        };

        // Check if key exists
        match ctx.store.get(key) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::Integer(0)),
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();
        let result = list.insert(before, pivot, element);
        Ok(Response::Integer(result))
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
// LREM - Remove elements
// ============================================================================

pub struct LremCommand;

impl Command for LremCommand {
    fn name(&self) -> &'static str {
        "LREM"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // LREM key count element
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let count = parse_i64(&args[1])?;
        let element = &args[2];

        // Check if key exists
        match ctx.store.get(key) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::Integer(0)),
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();
        let removed = list.remove(count, element);

        // Delete key if list is now empty
        if list.is_empty() {
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
// LTRIM - Trim list to range
// ============================================================================

pub struct LtrimCommand;

impl Command for LtrimCommand {
    fn name(&self) -> &'static str {
        "LTRIM"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // LTRIM key start stop
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

        // Check if key exists
        match ctx.store.get(key) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::ok()),
        }

        let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();
        list.trim(start, stop);

        // Delete key if list is now empty
        if list.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::ok())
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
// LPOS - Find position of element
// ============================================================================

pub struct LposCommand;

impl Command for LposCommand {
    fn name(&self) -> &'static str {
        "LPOS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];

        // Parse options
        let mut rank: i64 = 1;
        let mut count: Option<usize> = None;
        let mut maxlen: Option<usize> = None;

        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"RANK" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    rank = parse_i64(&args[i])?;
                    if rank == i64::MIN {
                        return Err(CommandError::InvalidArgument {
                            message: "value is out of range".to_string(),
                        });
                    }
                    if rank == 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list".to_string(),
                        });
                    }
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = Some(parse_usize(&args[i])?);
                }
                b"MAXLEN" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    maxlen = Some(parse_usize(&args[i])?);
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        match ctx.store.get(key) {
            Some(value) => {
                if let Some(list) = value.as_list() {
                    // Adjust rank for 0-based indexing
                    let adjusted_rank = if rank > 0 { rank - 1 } else { rank };

                    let result_count = match count {
                        Some(0) => usize::MAX, // COUNT 0 means return all matches
                        Some(n) => n,
                        None => 1,
                    };
                    let positions = list.position(element, adjusted_rank, result_count, maxlen);

                    if count.is_some() {
                        // Return array of positions
                        let results: Vec<Response> = positions
                            .into_iter()
                            .map(|p| Response::Integer(p as i64))
                            .collect();
                        Ok(Response::Array(results))
                    } else {
                        // Return single position or null
                        match positions.first() {
                            Some(&pos) => Ok(Response::Integer(pos as i64)),
                            None => Ok(Response::null()),
                        }
                    }
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => {
                if count.is_some() {
                    Ok(Response::Array(vec![]))
                } else {
                    Ok(Response::null())
                }
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
// RPOPLPUSH - Pop from tail of source, push to head of dest (deprecated)
// ============================================================================

pub struct RpoplpushCommand;

impl Command for RpoplpushCommand {
    fn name(&self) -> &'static str {
        "RPOPLPUSH"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // RPOPLPUSH source destination
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::MoveKeys
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];

        // Check source exists and is correct type
        match ctx.store.get(source) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::null()),
        }

        // Check dest is correct type if it exists
        if let Some(v) = ctx.store.get(dest)
            && v.as_list().is_none()
        {
            return Err(CommandError::WrongType);
        }

        // Pop from right of source
        let source_list = ctx.store.get_mut(source).unwrap().as_list_mut().unwrap();
        let element = source_list.pop_back();

        let element = match element {
            Some(e) => e,
            None => return Ok(Response::null()),
        };

        // Delete source if empty
        if source_list.is_empty() {
            ctx.store.delete(source);
        }

        // Push to left of dest (create if needed)
        let dest_list = get_or_create_list(ctx, dest)?;
        dest_list.push_front(element.clone());

        Ok(Response::bulk(element))
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
// LMOVE - Move element between lists
// ============================================================================

pub struct LmoveCommand;

impl Command for LmoveCommand {
    fn name(&self) -> &'static str {
        "LMOVE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(4) // LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::MoveKeys
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];
        let wherefrom = args[2].to_ascii_uppercase();
        let whereto = args[3].to_ascii_uppercase();

        let pop_left = match wherefrom.as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return Err(CommandError::SyntaxError),
        };

        let push_left = match whereto.as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return Err(CommandError::SyntaxError),
        };

        // Check source exists and is correct type
        match ctx.store.get(source) {
            Some(v) => {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::null()),
        }

        // Check dest is correct type if it exists
        if let Some(v) = ctx.store.get(dest)
            && v.as_list().is_none()
        {
            return Err(CommandError::WrongType);
        }

        // Pop from source
        let source_list = ctx.store.get_mut(source).unwrap().as_list_mut().unwrap();
        let element = if pop_left {
            source_list.pop_front()
        } else {
            source_list.pop_back()
        };

        let element = match element {
            Some(e) => e,
            None => return Ok(Response::null()),
        };

        // Delete source if empty
        if source_list.is_empty() {
            ctx.store.delete(source);
        }

        // Push to dest (create if needed)
        let dest_list = get_or_create_list(ctx, dest)?;
        if push_left {
            dest_list.push_front(element.clone());
        } else {
            dest_list.push_back(element.clone());
        }

        Ok(Response::bulk(element))
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
// LMPOP - Pop from multiple lists
// ============================================================================

pub struct LmpopCommand;

impl Command for LmpopCommand {
    fn name(&self) -> &'static str {
        "LMPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0]).map_err(|_| CommandError::InvalidArgument {
            message: "numkeys can't be non-positive value".to_string(),
        })?;

        if numkeys == 0 {
            return Err(CommandError::InvalidArgument {
                message: "numkeys can't be non-positive value".to_string(),
            });
        }

        if args.len() < 1 + numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys_end = 1 + numkeys;
        let direction = args[keys_end].to_ascii_uppercase();

        let pop_left = match direction.as_slice() {
            b"LEFT" => true,
            b"RIGHT" => false,
            _ => return Err(CommandError::SyntaxError),
        };

        // Parse optional COUNT (only allowed once)
        let mut count: usize = 1;
        let mut count_seen = false;
        let mut i = keys_end + 1;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            if opt.as_slice() == b"COUNT" {
                if count_seen {
                    return Err(CommandError::SyntaxError);
                }
                count_seen = true;
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let c = parse_i64(&args[i]).map_err(|_| CommandError::InvalidArgument {
                    message: "count value of LMPOP command is not an positive value".to_string(),
                })?;
                if c <= 0 {
                    return Err(CommandError::InvalidArgument {
                        message: "count value of LMPOP command is not an positive value"
                            .to_string(),
                    });
                }
                count = c as usize;
            } else {
                return Err(CommandError::SyntaxError);
            }
            i += 1;
        }

        // Find first non-empty list
        for key in &args[1..keys_end] {
            // Check type
            if let Some(v) = ctx.store.get(key) {
                if v.as_list().is_none() {
                    return Err(CommandError::WrongType);
                }
                if v.as_list().unwrap().is_empty() {
                    continue;
                }
            } else {
                continue;
            }

            // Pop from this list
            let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();
            let mut elements = Vec::with_capacity(count);

            for _ in 0..count {
                let elem = if pop_left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                match elem {
                    Some(e) => elements.push(Response::bulk(e)),
                    None => break,
                }
            }

            // Delete key if empty
            if list.is_empty() {
                ctx.store.delete(key);
            }

            if elements.is_empty() {
                continue;
            }

            return Ok(Response::Array(vec![
                Response::bulk(key.clone()),
                Response::Array(elements),
            ]));
        }

        // No non-empty list found
        Ok(Response::null())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }
        if let Ok(numkeys) = parse_usize(&args[0]) {
            args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
        } else {
            vec![]
        }
    }
}
