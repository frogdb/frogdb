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
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeyAccessFlag, KeySpec, KeyspaceEventFlags, StoreTypedFamilyExt, WaiterKind, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;

use super::utils::{get_or_create_list, parse_i64, parse_usize};

// ============================================================================
// LPUSH - Push elements to front of list
// ============================================================================

pub struct LpushCommand;

impl Command for LpushCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LPUSH",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lpush",
            },
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// RPUSH - Push elements to back of list
// ============================================================================

pub struct RpushCommand;

impl Command for RpushCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RPUSH",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "rpush",
            },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let list = get_or_create_list(ctx, key)?;

        for elem in &args[1..] {
            list.push_back(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
    }
}

// ============================================================================
// LPUSHX - Push to front only if list exists
// ============================================================================

pub struct LpushxCommand;

impl Command for LpushxCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LPUSHX",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(Response::Integer(0));
        };

        for elem in &args[1..] {
            list.push_front(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
    }
}

// ============================================================================
// RPUSHX - Push to back only if list exists
// ============================================================================

pub struct RpushxCommand;

impl Command for RpushxCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RPUSHX",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(Response::Integer(0));
        };

        for elem in &args[1..] {
            list.push_back(elem.clone());
        }

        Ok(Response::Integer(list.len() as i64))
    }
}

// ============================================================================
// LPOP - Pop elements from front
// ============================================================================

pub struct LpopCommand;

impl Command for LpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LPOP",
            arity: Arity::Range { min: 1, max: 2 },
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lpop",
            },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_usize(&args[1])?)
        } else {
            None
        };

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(if count.is_some() {
                Response::NullArray
            } else {
                Response::null()
            });
        };

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
}

// ============================================================================
// RPOP - Pop elements from back
// ============================================================================

pub struct RpopCommand;

impl Command for RpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RPOP",
            arity: Arity::Range { min: 1, max: 2 },
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "rpop",
            },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_usize(&args[1])?)
        } else {
            None
        };

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(if count.is_some() {
                Response::NullArray
            } else {
                Response::null()
            });
        };

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
}

// ============================================================================
// LLEN - Get list length
// ============================================================================

pub struct LlenCommand;

impl Command for LlenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LLEN",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// LRANGE - Get range of elements
// ============================================================================

pub struct LrangeCommand;

impl Command for LrangeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LRANGE",
            arity: Arity::Fixed(3),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// LINDEX - Get element by index
// ============================================================================

pub struct LindexCommand;

impl Command for LindexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LINDEX",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::TRACKS_KEYSPACE),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let index = parse_i64(&args[1])?;

        match ctx.store.get(key) {
            Some(value) => {
                // Keyspace hit: the key (list) exists. An out-of-range INDEX
                // still counts as a hit because the key lookup succeeded —
                // matches Redis lookup-level accounting, not reply shape.
                ctx.record_keyspace_lookup(true);
                if let Some(list) = value.as_list() {
                    match list.get(index) {
                        Some(elem) => Ok(Response::bulk(elem.clone())),
                        None => Ok(Response::null()),
                    }
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => {
                ctx.record_keyspace_lookup(false);
                Ok(Response::null())
            }
        }
    }
}

// ============================================================================
// LSET - Set element by index
// ============================================================================

pub struct LsetCommand;

impl Command for LsetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LSET",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lset",
            },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let index = parse_i64(&args[1])?;
        let element = args[2].clone();

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Err(CommandError::InvalidArgument {
                message: "no such key".to_string(),
            });
        };

        if list.set(index, element) {
            Ok(Response::ok())
        } else {
            Err(CommandError::InvalidArgument {
                message: "index out of range".to_string(),
            })
        }
    }
}

// ============================================================================
// LINSERT - Insert element before/after pivot
// ============================================================================

pub struct LinsertCommand;

impl Command for LinsertCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LINSERT",
            arity: Arity::Fixed(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "linsert",
            },
            requires_same_slot: false,
        };
        &SPEC
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

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(Response::Integer(0));
        };
        let result = list.insert(before, pivot, element);
        Ok(Response::Integer(result))
    }
}

// ============================================================================
// LREM - Remove elements
// ============================================================================

pub struct LremCommand;

impl Command for LremCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LREM",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lrem",
            },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let count = parse_i64(&args[1])?;
        let element = &args[2];

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(Response::Integer(0));
        };
        let removed = list.remove(count, element);

        // Delete key if list is now empty
        if list.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed as i64))
    }
}

// ============================================================================
// LTRIM - Trim list to range
// ============================================================================

pub struct LtrimCommand;

impl Command for LtrimCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LTRIM",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "ltrim",
            },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = parse_i64(&args[1])?;
        let stop = parse_i64(&args[2])?;

        let Some(list) = ctx.store.get_list_mut(key)? else {
            return Ok(Response::ok());
        };
        list.trim(start, stop);

        // Delete key if list is now empty
        if list.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::ok())
    }
}

// ============================================================================
// LPOS - Find position of element
// ============================================================================

pub struct LposCommand;

impl Command for LposCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LPOS",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
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
}

// ============================================================================
// RPOPLPUSH - Pop from tail of source, push to head of dest (deprecated)
// ============================================================================

pub struct RpoplpushCommand;

impl Command for RpoplpushCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RPOPLPUSH",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::RW]),
            wal: WalStrategy::MoveKeys,
            wakes: // Pushes onto the destination list, so a client blocked in
        // BLPOP/BRPOP/BLMOVE on the destination key must be woken.
        WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Emits { class: KeyspaceEventFlags::LIST, name: "rpoplpush" },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];

        // Source must be a list; null if missing. Checked before dest to match
        // Redis ordering (a missing source short-circuits without a dest check).
        if ctx.store.get_list(source)?.is_none() {
            return Ok(Response::null());
        }

        // Dest must be a list if it exists; up-front check so the source borrow
        // below stays disjoint.
        ctx.store.check_list(dest)?;

        // Pop from right of source.
        let Some(source_list) = ctx.store.get_list_mut(source)? else {
            return Ok(Response::null());
        };
        let element = match source_list.pop_back() {
            Some(e) => e,
            None => return Ok(Response::null()),
        };

        // Delete source if empty
        if source_list.is_empty() {
            ctx.store.delete(source);
        }

        // Push to left of dest (create if needed)
        let dest_list = ctx.store.get_or_create_list(dest)?;
        dest_list.push_front(element.clone());

        Ok(Response::bulk(element))
    }
}

// ============================================================================
// LMOVE - Move element between lists
// ============================================================================

pub struct LmoveCommand;

impl Command for LmoveCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LMOVE",
            arity: Arity::Fixed(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::RW]),
            wal: WalStrategy::MoveKeys,
            wakes: // Pushes onto the destination list, so a client blocked in
        // BLPOP/BRPOP/BLMOVE on the destination key must be woken.
        WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Emits { class: KeyspaceEventFlags::LIST, name: "lmove" },
            requires_same_slot: false,
        };
        &SPEC
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

        // Source must be a list; null if missing. Checked before dest to match
        // Redis ordering (a missing source short-circuits without a dest check).
        if ctx.store.get_list(source)?.is_none() {
            return Ok(Response::null());
        }

        // Dest must be a list if it exists; up-front check so the source borrow
        // below stays disjoint.
        ctx.store.check_list(dest)?;

        // Pop from source.
        let Some(source_list) = ctx.store.get_list_mut(source)? else {
            return Ok(Response::null());
        };
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
        let dest_list = ctx.store.get_or_create_list(dest)?;
        if push_left {
            dest_list.push_front(element.clone());
        } else {
            dest_list.push_back(element.clone());
        }

        Ok(Response::bulk(element))
    }
}

// ============================================================================
// LMPOP - Pop from multiple lists
// ============================================================================

pub struct LmpopCommand;

impl Command for LmpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LMPOP",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::NumkeysAt {
                numkeys: 0,
                first: 1,
            },
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
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
            // Type-check and skip missing/empty lists on the shared handle so a
            // wrong-typed key errors and an empty one is skipped without COW.
            match ctx.store.get_list(key)? {
                Some(l) if !l.is_empty() => {}
                _ => continue,
            }

            // Pop from this list.
            let Some(list) = ctx.store.get_list_mut(key)? else {
                continue;
            };
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    #[test]
    fn lrem_declares_list_keyspace_event() {
        // Regression: LREM previously emitted no keyspace notification because
        // it never declared a keyspace event.
        assert_eq!(
            LremCommand.spec().event,
            EventSpec::Emits {
                class: KeyspaceEventFlags::LIST,
                name: "lrem"
            }
        );
    }

    #[test]
    fn rpoplpush_wakes_list_waiters_on_both_keys() {
        // Regression: RPOPLPUSH pushes onto the destination list, so a client
        // blocked in BLPOP/BRPOP/BLMOVE on the destination must be woken.
        assert_eq!(
            RpoplpushCommand.spec().wakes,
            WaiterWake::Kind(WaiterKind::List)
        );
        let a = args(&["src", "dest"]);
        let keys = RpoplpushCommand.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref(), b"dest".as_ref()]);
    }

    #[test]
    fn lmove_wakes_list_waiters_on_both_keys() {
        // Regression: LMOVE pushes onto the destination list, so a client
        // blocked in BLPOP/BRPOP/BLMOVE on the destination must be woken.
        assert_eq!(
            LmoveCommand.spec().wakes,
            WaiterWake::Kind(WaiterKind::List)
        );
        let a = args(&["src", "dest", "LEFT", "RIGHT"]);
        let keys = LmoveCommand.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref(), b"dest".as_ref()]);
    }
}
