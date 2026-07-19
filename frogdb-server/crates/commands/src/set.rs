//! Set commands.
//!
//! Commands for set manipulation:
//! - SADD, SREM, SMEMBERS - basic operations
//! - SISMEMBER, SMISMEMBER, SCARD - inspection
//! - SUNION, SINTER, SDIFF - set operations
//! - SUNIONSTORE, SINTERSTORE, SDIFFSTORE - store results
//! - SINTERCARD - cardinality of intersection
//! - SRANDMEMBER, SPOP - random operations
//! - SMOVE, SSCAN - other operations

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeyAccessFlag, KeySpec, KeyspaceEventFlags, ListpackThresholds,
    LookupSpec, SetValue, StoreTypedFamilyExt, Value, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::utils::{parse_i64, parse_usize};

/// Get an existing set (cloned), returning None if key doesn't exist, Error if wrong type.
fn get_set_inline(ctx: &mut CommandContext, key: &Bytes) -> Result<Option<SetValue>, CommandError> {
    match ctx.store.get(key) {
        Some(value) => {
            if let Some(set) = value.as_set() {
                Ok(Some(set.clone()))
            } else {
                Err(CommandError::WrongType)
            }
        }
        None => Ok(None),
    }
}

// ============================================================================
// SADD - Add members to set
// ============================================================================

pub struct SaddCommand;

impl Command for SaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SADD",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::SET,
                name: "sadd",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let set = ctx.get_or_create::<SetValue>(key)?;

        let mut added = 0i64;
        for member in &args[1..] {
            if set.add(member.clone(), ListpackThresholds::DEFAULT_SET) {
                added += 1;
            }
        }

        Ok(Response::Integer(added))
    }
}

// ============================================================================
// SREM - Remove members from set
// ============================================================================

pub struct SremCommand;

impl Command for SremCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SREM",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::SET,
                name: "srem",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let Some(set) = ctx.store.get_set_mut(key)? else {
            return Ok(Response::Integer(0));
        };

        let mut removed = 0i64;
        for member in &args[1..] {
            if set.remove(member) {
                removed += 1;
            }
        }

        // Delete key if set is now empty
        if set.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed))
    }
}

// ============================================================================
// SMEMBERS - Get all members
// ============================================================================

pub struct SmembersCommand;

impl Command for SmembersCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SMEMBERS",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::FirstKey,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match get_set_inline(ctx, key)? {
            Some(set) => {
                let results: Vec<Response> = set.members().map(Response::bulk).collect();
                if ctx.protocol_version.is_resp3() {
                    Ok(Response::Set(results))
                } else {
                    Ok(Response::Array(results))
                }
            }
            None => {
                if ctx.protocol_version.is_resp3() {
                    Ok(Response::Set(vec![]))
                } else {
                    Ok(Response::Array(vec![]))
                }
            }
        }
    }
}

// ============================================================================
// SISMEMBER - Check if member exists
// ============================================================================

pub struct SismemberCommand;

impl Command for SismemberCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SISMEMBER",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];

        match get_set_inline(ctx, key)? {
            Some(set) => {
                if set.contains(member) {
                    Ok(Response::Integer(1))
                } else {
                    Ok(Response::Integer(0))
                }
            }
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// SMISMEMBER - Check if multiple members exist
// ============================================================================

pub struct SmismemberCommand;

impl Command for SmismemberCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SMISMEMBER",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match get_set_inline(ctx, key)? {
            Some(set) => {
                let results: Vec<Response> = args[1..]
                    .iter()
                    .map(|member| {
                        if set.contains(member) {
                            Response::Integer(1)
                        } else {
                            Response::Integer(0)
                        }
                    })
                    .collect();
                Ok(Response::Array(results))
            }
            None => {
                let results: Vec<Response> =
                    args[1..].iter().map(|_| Response::Integer(0)).collect();
                Ok(Response::Array(results))
            }
        }
    }
}

// ============================================================================
// SCARD - Get set cardinality
// ============================================================================

pub struct ScardCommand;

impl Command for ScardCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SCARD",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::FirstKey,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match get_set_inline(ctx, key)? {
            Some(set) => Ok(Response::Integer(set.len() as i64)),
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// SUNION - Return union of sets
// ============================================================================

pub struct SunionCommand;

impl Command for SunionCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SUNION",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[0])? {
            Some(s) => s.clone(),
            None => SetValue::new(),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[1..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s);
            }
        }

        let result = first.union(others.iter());
        let members: Vec<Response> = result.members().map(Response::bulk).collect();
        if ctx.protocol_version.is_resp3() {
            Ok(Response::Set(members))
        } else {
            Ok(Response::Array(members))
        }
    }
}

// ============================================================================
// SINTER - Return intersection of sets
// ============================================================================

pub struct SinterCommand;

impl Command for SinterCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SINTER",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let empty_result = if ctx.protocol_version.is_resp3() {
            Response::Set(vec![])
        } else {
            Response::Array(vec![])
        };

        // Type-check all keys first (Redis validates all keys before returning)
        for key in args.iter() {
            get_set_inline(ctx, key)?;
        }

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[0])? {
            Some(s) => s.clone(),
            None => return Ok(empty_result), // Empty set intersects to empty
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[1..] {
            match get_set_inline(ctx, key)? {
                Some(s) => others.push(s),
                None => return Ok(empty_result.clone()), // Missing key = empty intersection
            }
        }

        let result = first.intersection(others.iter());
        let members: Vec<Response> = result.members().map(Response::bulk).collect();
        if ctx.protocol_version.is_resp3() {
            Ok(Response::Set(members))
        } else {
            Ok(Response::Array(members))
        }
    }
}

// ============================================================================
// SDIFF - Return difference of sets
// ============================================================================

pub struct SdiffCommand;

impl Command for SdiffCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SDIFF",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let empty_result = if ctx.protocol_version.is_resp3() {
            Response::Set(vec![])
        } else {
            Response::Array(vec![])
        };

        // Type-check all keys first (Redis validates all keys before returning)
        for key in args.iter() {
            get_set_inline(ctx, key)?;
        }

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[0])? {
            Some(s) => s.clone(),
            None => return Ok(empty_result),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[1..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s);
            }
        }

        let result = first.difference(others.iter());
        let members: Vec<Response> = result.members().map(Response::bulk).collect();
        if ctx.protocol_version.is_resp3() {
            Ok(Response::Set(members))
        } else {
            Ok(Response::Array(members))
        }
    }
}

// ============================================================================
// SUNIONSTORE - Store union of sets
// ============================================================================

pub struct SunionstoreCommand;

impl Command for SunionstoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SUNIONSTORE",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::All,
            // Destination (key 0) is overwritten; the source keys are read-only.
            access: AccessSpec::Positional(&[KeyAccessFlag::OW, KeyAccessFlag::R]),
            wal: WalStrategy::PersistDestination,
            wakes: WaiterWake::None,
            event: EventSpec::EmitsAt {
                class: KeyspaceEventFlags::SET,
                name: "sunionstore",
                key_index: 0,
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest = &args[0];

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => SetValue::new(),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s.clone());
            }
        }

        let result = first.union(others.iter());

        let len = result.len() as i64;

        // Store the result
        if result.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::Set(result));
        }

        Ok(Response::Integer(len))
    }
}

// ============================================================================
// SINTERSTORE - Store intersection of sets
// ============================================================================

pub struct SinterstoreCommand;

impl Command for SinterstoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SINTERSTORE",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::All,
            // Destination (key 0) is overwritten; the source keys are read-only.
            access: AccessSpec::Positional(&[KeyAccessFlag::OW, KeyAccessFlag::R]),
            wal: WalStrategy::PersistDestination,
            wakes: WaiterWake::None,
            event: EventSpec::EmitsAt {
                class: KeyspaceEventFlags::SET,
                name: "sinterstore",
                key_index: 0,
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest = &args[0];

        // Type-check all source keys first (Redis validates all keys before returning)
        for key in &args[1..] {
            get_set_inline(ctx, key)?;
        }

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => {
                ctx.store.delete(dest);
                return Ok(Response::Integer(0));
            }
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..] {
            match get_set_inline(ctx, key)? {
                Some(s) => others.push(s.clone()),
                None => {
                    ctx.store.delete(dest);
                    return Ok(Response::Integer(0));
                }
            }
        }

        let result = first.intersection(others.iter());

        let len = result.len() as i64;

        // Store the result
        if result.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::Set(result));
        }

        Ok(Response::Integer(len))
    }
}

// ============================================================================
// SDIFFSTORE - Store difference of sets
// ============================================================================

pub struct SdiffstoreCommand;

impl Command for SdiffstoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SDIFFSTORE",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::All,
            // Destination (key 0) is overwritten; the source keys are read-only.
            access: AccessSpec::Positional(&[KeyAccessFlag::OW, KeyAccessFlag::R]),
            wal: WalStrategy::PersistDestination,
            wakes: WaiterWake::None,
            event: EventSpec::EmitsAt {
                class: KeyspaceEventFlags::SET,
                name: "sdiffstore",
                key_index: 0,
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest = &args[0];

        // Type-check all source keys first (Redis validates all keys before returning)
        for key in &args[1..] {
            get_set_inline(ctx, key)?;
        }

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => {
                ctx.store.delete(dest);
                return Ok(Response::Integer(0));
            }
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s.clone());
            }
        }

        let result = first.difference(others.iter());

        let len = result.len() as i64;

        // Store the result
        if result.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::Set(result));
        }

        Ok(Response::Integer(len))
    }
}

// ============================================================================
// SINTERCARD - Return cardinality of intersection
// ============================================================================

pub struct SintercardCommand;

impl Command for SintercardCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SINTERCARD",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::NumkeysAt {
                numkeys: 0,
                first: 1,
            },
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::EveryKey,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys_i64 = parse_i64(&args[0]).map_err(|_| CommandError::InvalidArgument {
            message: "numkeys can't be non-positive value".to_string(),
        })?;

        if numkeys_i64 <= 0 {
            return Err(CommandError::InvalidArgument {
                message: "numkeys can't be non-positive value".to_string(),
            });
        }

        let numkeys = numkeys_i64 as usize;

        if args.len() < 1 + numkeys {
            return Err(CommandError::InvalidArgument {
                message: "Number of keys can't be greater than number of args".to_string(),
            });
        }

        // Parse optional LIMIT
        let mut limit: Option<usize> = None;
        let key_end = 1 + numkeys;
        let mut parser = ArgParser::from_position(args, key_end);
        while parser.has_more() {
            if parser.try_flag(b"LIMIT") {
                let val = parser.next_arg()?;
                limit = Some(parse_usize(val).map_err(|_| CommandError::InvalidArgument {
                    message: "LIMIT can't be negative".to_string(),
                })?);
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => return Ok(Response::Integer(0)),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..key_end] {
            match get_set_inline(ctx, key)? {
                Some(s) => others.push(s),
                None => return Ok(Response::Integer(0)),
            }
        }

        let result = first.intersection(others.iter());
        let mut count = result.len();

        if let Some(l) = limit
            && l > 0
        {
            count = count.min(l);
        }

        Ok(Response::Integer(count as i64))
    }
}

// ============================================================================
// SRANDMEMBER - Get random members
// ============================================================================

pub struct SrandmemberCommand;

impl Command for SrandmemberCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SRANDMEMBER",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_i64(&args[1])?)
        } else {
            None
        };

        match get_set_inline(ctx, key)? {
            Some(set) => {
                if set.is_empty() {
                    if count.is_some() {
                        return Ok(Response::Array(vec![]));
                    } else {
                        return Ok(Response::null());
                    }
                }

                match count {
                    Some(c) => {
                        let members = set.random_members(c);
                        let results: Vec<Response> =
                            members.into_iter().map(Response::bulk).collect();
                        Ok(Response::Array(results))
                    }
                    None => {
                        // Single member mode
                        let members = set.random_members(1);
                        if let Some(member) = members.into_iter().next() {
                            Ok(Response::bulk(member))
                        } else {
                            Ok(Response::null())
                        }
                    }
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
// SPOP - Remove and return random members
// ============================================================================

pub struct SpopCommand;

impl Command for SpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SPOP",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::SET,
                name: "spop",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
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

        let Some(set) = ctx.store.get_set_mut(key)? else {
            if count.is_some() {
                return Ok(Response::Array(vec![]));
            } else {
                return Ok(Response::null());
            }
        };

        match count {
            Some(c) => {
                let members = set.pop_many(c);
                let results: Vec<Response> = members.into_iter().map(Response::bulk).collect();

                // Delete key if set is now empty
                if set.is_empty() {
                    ctx.store.delete(key);
                }

                Ok(Response::Array(results))
            }
            None => {
                // Single member mode
                match set.pop() {
                    Some(member) => {
                        // Delete key if set is now empty
                        if set.is_empty() {
                            ctx.store.delete(key);
                        }
                        Ok(Response::bulk(member))
                    }
                    None => Ok(Response::null()),
                }
            }
        }
    }
}

// ============================================================================
// SMOVE - Move member from one set to another
// ============================================================================

pub struct SmoveCommand;

impl Command for SmoveCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SMOVE",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::W]),
            wal: WalStrategy::MoveKeys,
            wakes: WaiterWake::None,
            // Runtime-deposited (proposal 44): SMOVE is SREM+SADD internally, so
            // Redis emits `srem` on the source and `sadd` on the destination
            // (t_set.c smoveCommand:36,66) — not a bespoke `smove` event. Fires
            // only when the member actually moved; a non-member no-op is silent.
            event: EventSpec::Dynamic,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];
        let member = &args[2];

        // Source must be a set; 0 if missing. Checked before dest to match
        // Redis ordering (a missing source short-circuits without a dest check).
        if ctx.store.get_set(source)?.is_none() {
            return Ok(Response::Integer(0));
        }

        // Dest must be a set if it exists; up-front check so the source borrow
        // below stays disjoint.
        ctx.store.check_set(dest)?;

        // src == dst short-circuit (t_set.c smoveCommand: `if (srcset == dstset)`).
        // Redis performs no modification and emits no events; it just replies 1
        // when the member is present, 0 otherwise. Declaring the write a no-op
        // skips the whole effect pipeline (WAL, replication, notifications,
        // WATCH bump) — matching Redis, which returns before signalModifiedKey.
        if source == dest {
            let is_member = ctx
                .store
                .get_set(source)?
                .is_some_and(|set| set.contains(member));
            ctx.effects.write_was_noop = true;
            return Ok(Response::Integer(if is_member { 1 } else { 0 }));
        }

        // Remove from source.
        let Some(source_set) = ctx.store.get_set_mut(source)? else {
            return Ok(Response::Integer(0));
        };
        if !source_set.remove(member) {
            return Ok(Response::Integer(0));
        }

        // Delete source if empty
        if source_set.is_empty() {
            ctx.store.delete(source);
        }

        // Remove from source succeeded: `srem` on the source (t_set.c:36).
        ctx.notify_event(source.clone(), "srem", KeyspaceEventFlags::SET);

        // Add to dest (create if needed). Redis only emits `sadd` when the
        // element was newly added (setTypeAdd truthy, t_set.c:66); if the member
        // was already present in the destination, the add is a no-op and no
        // `sadd` fires — but `srem` on the source still did.
        let dest_set = ctx.store.get_or_create_set(dest)?;
        if dest_set.add(member.clone(), ListpackThresholds::DEFAULT_SET) {
            ctx.notify_event(dest.clone(), "sadd", KeyspaceEventFlags::SET);
        }

        Ok(Response::Integer(1))
    }
}

// ============================================================================
// SSCAN - Cursor-based iteration
// ============================================================================

pub struct SscanCommand;

impl Command for SscanCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SSCAN",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse cursor, [MATCH pattern], [COUNT count]; no SSCAN-specific flags.
        let request = crate::utils::ScanRequest::parse(&args[1..], |_| Ok(false))?;

        let set = get_set_inline(ctx, key)?;
        let items = set.as_ref().map(|s| s.members());

        Ok(crate::utils::scan_reply(
            &request,
            items,
            |m: &Bytes| m.as_ref(),
            |m: Bytes, results: &mut Vec<Response>| {
                results.push(Response::bulk(m));
            },
        ))
    }
}
