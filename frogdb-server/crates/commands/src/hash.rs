//! Hash commands.
//!
//! Commands for hash manipulation:
//! - HSET, HSETNX, HGET, HDEL, HMSET, HMGET - basic operations
//! - HGETALL, HKEYS, HVALS - retrieve all data
//! - HEXISTS, HLEN, HSTRLEN - inspection
//! - HINCRBY, HINCRBYFLOAT - numeric operations
//! - HSCAN, HRANDFIELD - iteration & random
//! - HGETDEL, HGETEX, HSETEX - Redis 8.0 atomic hash operations

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, HashValue, KeySpec, KeyspaceEventFlags, ListpackThresholds, LookupSpec,
    StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::expiry::{
    instant_to_unix_ms, instant_to_unix_secs, parse_expire_conditions_from_slice,
    unix_ms_to_instant, unix_secs_to_instant,
};
use super::utils::{
    ExpiryErr, checked_expire_value, format_float, parse_f64, parse_i64, reject_non_finite_delta,
};
use std::time::{Duration, Instant};

// ============================================================================
// HSET - Set hash fields
// ============================================================================

pub struct HsetCommand;

impl Command for HsetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HSET",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hset",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Must have even number of field-value pairs
        if !(args.len() - 1).is_multiple_of(2) {
            return Err(CommandError::InvalidArgument {
                message: "wrong number of arguments for 'hset' command".to_string(),
            });
        }

        let new_fields = {
            let hash = ctx.get_or_create::<HashValue>(key)?;

            let mut new_fields = 0i64;
            for chunk in args[1..].chunks(2) {
                let field = chunk[0].clone();
                let value = chunk[1].clone();
                if hash.set(field, value, ListpackThresholds::DEFAULT_HASH) {
                    new_fields += 1;
                }
            }
            new_fields
        };

        // HSET clears field expiry on overwritten fields - update store index
        for chunk in args[1..].chunks(2) {
            ctx.store.remove_field_expiry(key, &chunk[0]);
        }

        Ok(Response::Integer(new_fields))
    }
}

// ============================================================================
// HSETNX - Set hash field only if not exists
// ============================================================================

pub struct HsetnxCommand;

impl Command for HsetnxCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HSETNX",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hset",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let value = args[2].clone();

        let hash = ctx.get_or_create::<HashValue>(key)?;

        if hash.set_nx(field, value, ListpackThresholds::DEFAULT_HASH) {
            Ok(Response::Integer(1))
        } else {
            Ok(Response::Integer(0))
        }
    }
}

// ============================================================================
// HGET - Get hash field
// ============================================================================

pub struct HgetCommand;

impl Command for HgetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HGET",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            // Counted at the seam from the hash KEY's existence; a present hash
            // with a missing field is still a hit.
            lookup: LookupSpec::FirstKey,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = &args[1];

        // Lazy field expiry: purge expired fields
        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => match hash.get(field) {
                Some(v) => Ok(Response::bulk(v)),
                None => Ok(Response::null()),
            },
            None => Ok(Response::null()),
        }
    }
}

// ============================================================================
// HDEL - Delete hash fields
// ============================================================================

pub struct HdelCommand;

impl Command for HdelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HDEL",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hdel",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let (deleted, is_empty) = {
            let Some(hash) = ctx.store.get_hash_mut(key)? else {
                return Ok(Response::Integer(0));
            };

            let mut deleted = 0i64;
            for field in &args[1..] {
                if hash.remove(field) {
                    deleted += 1;
                }
            }

            (deleted, hash.is_empty())
        };

        // Clean up field expiry index for deleted fields
        for field in &args[1..] {
            ctx.store.remove_field_expiry(key, field);
        }

        // Delete key if hash is now empty
        if is_empty {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(deleted))
    }
}

// ============================================================================
// HMSET - Set multiple hash fields (deprecated alias for HSET)
// ============================================================================

pub struct HmsetCommand;

impl Command for HmsetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HMSET",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hset",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Must have even number of field-value pairs
        if !(args.len() - 1).is_multiple_of(2) {
            return Err(CommandError::InvalidArgument {
                message: "wrong number of arguments for 'hmset' command".to_string(),
            });
        }

        let hash = ctx.get_or_create::<HashValue>(key)?;

        for chunk in args[1..].chunks(2) {
            let field = chunk[0].clone();
            let value = chunk[1].clone();
            hash.set(field, value, ListpackThresholds::DEFAULT_HASH);
        }

        Ok(Response::ok())
    }
}

// ============================================================================
// HMGET - Get multiple hash fields
// ============================================================================

pub struct HmgetCommand;

impl Command for HmgetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HMGET",
            arity: Arity::AtLeast(2),
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

        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => {
                let results: Vec<Response> = args[1..]
                    .iter()
                    .map(|field| match hash.get(field) {
                        Some(v) => Response::bulk(v),
                        None => Response::null(),
                    })
                    .collect();
                Ok(Response::Array(results))
            }
            None => {
                // Key doesn't exist - return array of nulls
                let results: Vec<Response> = args[1..].iter().map(|_| Response::null()).collect();
                Ok(Response::Array(results))
            }
        }
    }
}

// ============================================================================
// HGETALL - Get all fields and values
// ============================================================================

pub struct HgetallCommand;

impl Command for HgetallCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HGETALL",
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

        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => {
                if ctx.protocol_version.is_resp3() {
                    // RESP3: Return as Map
                    let pairs: Vec<(Response, Response)> = hash
                        .iter()
                        .map(|(field, value)| (Response::bulk(field), Response::bulk(value)))
                        .collect();
                    Ok(Response::Map(pairs))
                } else {
                    // RESP2: Return as flattened Array
                    let mut results = Vec::with_capacity(hash.len() * 2);
                    for (field, value) in hash.iter() {
                        results.push(Response::bulk(field));
                        results.push(Response::bulk(value));
                    }
                    Ok(Response::Array(results))
                }
            }
            None => {
                if ctx.protocol_version.is_resp3() {
                    Ok(Response::Map(vec![]))
                } else {
                    Ok(Response::Array(vec![]))
                }
            }
        }
    }
}

// ============================================================================
// HKEYS - Get all field names
// ============================================================================

pub struct HkeysCommand;

impl Command for HkeysCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HKEYS",
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

        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => {
                let results: Vec<Response> = hash.keys().map(Response::bulk).collect();
                Ok(Response::Array(results))
            }
            None => Ok(Response::Array(vec![])),
        }
    }
}

// ============================================================================
// HVALS - Get all values
// ============================================================================

pub struct HvalsCommand;

impl Command for HvalsCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HVALS",
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

        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => {
                let results: Vec<Response> = hash.values().map(Response::bulk).collect();
                Ok(Response::Array(results))
            }
            None => Ok(Response::Array(vec![])),
        }
    }
}

// ============================================================================
// HEXISTS - Check if field exists
// ============================================================================

pub struct HexistsCommand;

impl Command for HexistsCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HEXISTS",
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
        let field = &args[1];

        // Lazy field expiry: purge expired fields
        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => {
                if hash.contains(field) {
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
// HLEN - Get number of fields
// ============================================================================

pub struct HlenCommand;

impl Command for HlenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HLEN",
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

        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => Ok(Response::Integer(hash.len() as i64)),
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// HINCRBY - Increment integer field
// ============================================================================

pub struct HincrbyCommand;

impl Command for HincrbyCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HINCRBY",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hincrby",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let increment = parse_i64(&args[2])?;

        let hash = ctx.get_or_create::<HashValue>(key)?;
        let new_val = hash.incr_by(field, increment, ListpackThresholds::DEFAULT_HASH)?;
        Ok(Response::Integer(new_val))
    }
}

// ============================================================================
// HINCRBYFLOAT - Increment float field
// ============================================================================

pub struct HincrbyfloatCommand;

impl Command for HincrbyfloatCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HINCRBYFLOAT",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hincrbyfloat",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let increment = parse_f64(&args[2])?;
        reject_non_finite_delta(increment)?;

        let hash = ctx.get_or_create::<HashValue>(key)?;
        let new_val = hash.incr_by_float(field, increment, ListpackThresholds::DEFAULT_HASH)?;
        // Always return bulk string (not Double), matching Redis behavior
        Ok(Response::bulk(Bytes::from(format_float(new_val))))
    }
}

// ============================================================================
// HSTRLEN - Get length of field value
// ============================================================================

pub struct HstrlenCommand;

impl Command for HstrlenCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HSTRLEN",
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
        let field = &args[1];

        // Lazy field expiry: purge expired fields
        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => match hash.get(field) {
                Some(v) => Ok(Response::Integer(v.len() as i64)),
                None => Ok(Response::Integer(0)),
            },
            None => Ok(Response::Integer(0)),
        }
    }
}

// ============================================================================
// HSCAN - Cursor-based iteration
// ============================================================================

pub struct HscanCommand;

impl Command for HscanCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HSCAN",
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

        // Parse cursor, [MATCH pattern], [COUNT count], and HSCAN's [NOVALUES].
        let mut novalues = false;
        let request = crate::utils::ScanRequest::parse(&args[1..], |parser| {
            if parser.try_flag(b"NOVALUES") {
                novalues = true;
                Ok(true)
            } else {
                Ok(false)
            }
        })?;

        ctx.store.purge_expired_hash_fields(key);

        let hash = ctx.store.get_hash(key)?;
        let items = hash.as_ref().map(|h| h.iter());

        Ok(crate::utils::scan_reply(
            &request,
            items,
            |entry: &(Bytes, Bytes)| entry.0.as_ref(),
            |entry: (Bytes, Bytes), results: &mut Vec<Response>| {
                results.push(Response::bulk(entry.0));
                if !novalues {
                    results.push(Response::bulk(entry.1));
                }
            },
        ))
    }
}

// ============================================================================
// HRANDFIELD - Get random fields
// ============================================================================

pub struct HrandfieldCommand;

impl Command for HrandfieldCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HRANDFIELD",
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
            parse_i64(&args[1])?
        } else {
            1 // Default: return single field
        };

        let with_values = if args.len() > 2 {
            let opt = args[2].to_ascii_uppercase();
            if opt.as_slice() == b"WITHVALUES" {
                true
            } else {
                return Err(CommandError::SyntaxError);
            }
        } else {
            false
        };

        // Overflow protection: i64::MIN can't be negated, and very large negative
        // counts with WITHVALUES would overflow the result array size
        if count < 0 {
            if count == i64::MIN {
                return Err(CommandError::InvalidArgument {
                    message: "value is out of range".to_string(),
                });
            }
            if with_values && count.checked_neg().and_then(|c| c.checked_mul(2)).is_none() {
                return Err(CommandError::InvalidArgument {
                    message: "value is out of range".to_string(),
                });
            }
        }

        ctx.store.purge_expired_hash_fields(key);

        match ctx.store.get_hash(key)? {
            Some(hash) => {
                if hash.is_empty() {
                    if args.len() == 1 {
                        return Ok(Response::null());
                    } else {
                        return Ok(Response::Array(vec![]));
                    }
                }

                let random_fields = hash.random_fields(count, with_values);

                if args.len() == 1 {
                    // Single field mode - return just the field
                    if let Some((field, _)) = random_fields.first() {
                        return Ok(Response::bulk(field.clone()));
                    } else {
                        return Ok(Response::null());
                    }
                }

                // Multiple fields mode
                if with_values {
                    if ctx.protocol_version.is_resp3() {
                        // RESP3: return as array of [field, value] pairs
                        let results: Vec<_> = random_fields
                            .into_iter()
                            .map(|(f, v)| {
                                Response::Array(vec![Response::bulk(f), Response::bulk(v.unwrap())])
                            })
                            .collect();
                        Ok(Response::Array(results))
                    } else {
                        // RESP2: flat array of field, value, field, value, ...
                        let mut results = Vec::with_capacity(random_fields.len() * 2);
                        for (field, value) in random_fields {
                            results.push(Response::bulk(field));
                            if let Some(v) = value {
                                results.push(Response::bulk(v));
                            }
                        }
                        Ok(Response::Array(results))
                    }
                } else {
                    let results: Vec<Response> = random_fields
                        .into_iter()
                        .map(|(field, _)| Response::bulk(field))
                        .collect();
                    Ok(Response::Array(results))
                }
            }
            None => {
                if args.len() == 1 {
                    Ok(Response::null())
                } else {
                    Ok(Response::Array(vec![]))
                }
            }
        }
    }
}

// ============================================================================
// Hash field expiry helpers
// ============================================================================

/// Parse the FIELDS numfields field... portion of hash field expiry commands.
/// Returns (condition_args_slice, fields_slice) or error.
fn parse_hexpire_args(
    args: &[Bytes],
    time_arg_offset: usize,
) -> Result<(&[Bytes], &[Bytes]), CommandError> {
    // After key and time value, scan for condition flags (NX/XX/GT/LT) then FIELDS keyword
    let remaining = &args[time_arg_offset..];

    let mut fields_keyword_pos = None;
    for (i, arg) in remaining.iter().enumerate() {
        if arg.eq_ignore_ascii_case(b"FIELDS") {
            fields_keyword_pos = Some(i);
            break;
        }
    }

    let fields_pos = fields_keyword_pos.ok_or(CommandError::SyntaxError)?;

    let condition_args = &remaining[..fields_pos];
    let after_fields = &remaining[fields_pos + 1..]; // skip FIELDS keyword

    if after_fields.is_empty() {
        return Err(CommandError::SyntaxError);
    }

    // Parse numfields
    let numfields = std::str::from_utf8(&after_fields[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(CommandError::NotInteger)?;

    let field_args = &after_fields[1..];

    if field_args.len() != numfields {
        return Err(CommandError::InvalidArgument {
            message: format!(
                "parameter 'numfields' value of {} does not match number of arguments",
                numfields
            ),
        });
    }

    if numfields == 0 {
        return Err(CommandError::InvalidArgument {
            message: "parameter 'numfields' value of 0 is not positive".to_string(),
        });
    }

    Ok((condition_args, field_args))
}

/// Parse FIELDS numfields field... for read-only commands (no condition flags).
fn parse_hexpire_read_args(args: &[Bytes]) -> Result<&[Bytes], CommandError> {
    // After key, expect FIELDS keyword
    if args.len() < 3 {
        return Err(CommandError::SyntaxError);
    }

    if !args[0].eq_ignore_ascii_case(b"FIELDS") {
        return Err(CommandError::SyntaxError);
    }

    let numfields = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(CommandError::NotInteger)?;

    let field_args = &args[2..];

    if field_args.len() != numfields {
        return Err(CommandError::InvalidArgument {
            message: format!(
                "parameter 'numfields' value of {} does not match number of arguments",
                numfields
            ),
        });
    }

    if numfields == 0 {
        return Err(CommandError::InvalidArgument {
            message: "parameter 'numfields' value of 0 is not positive".to_string(),
        });
    }

    Ok(field_args)
}

/// Shared implementation for HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT.
///
/// `time_converter` converts the raw time argument to an `Option<Instant>`.
/// Returns `None` if the time value means "already expired / delete immediately".
fn execute_hexpire_common(
    ctx: &mut CommandContext,
    args: &[Bytes],
    _command_name: &str,
    time_converter: impl Fn(i64) -> Option<Instant>,
    is_past_or_zero: impl Fn(i64) -> bool,
) -> Result<Response, CommandError> {
    let key = &args[0];
    let time_val = parse_i64(&args[1])?;

    // Reject negative time values (Redis returns ERR)
    if time_val < 0 {
        return Err(CommandError::InvalidArgument {
            message: "invalid expire time, must be >= 0".to_string(),
        });
    }

    let (condition_args, field_args) = parse_hexpire_args(args, 2)?;
    let conditions = parse_expire_conditions_from_slice(condition_args)?;

    // Check key exists and is a hash
    let hash = match ctx.store.get_hash(key)? {
        Some(h) => h,
        None => {
            // Key not found: return array of -2 for each field
            let results: Vec<Response> = field_args.iter().map(|_| Response::Integer(-2)).collect();
            return Ok(Response::Array(results));
        }
    };

    // Gather field existence and current expiry info with immutable access first
    let field_info: Vec<(Bytes, bool, Option<Instant>)> = field_args
        .iter()
        .map(|f| {
            let exists = hash.contains(f);
            let expiry = if exists {
                ctx.store.get_field_expiry(key, f)
            } else {
                None
            };
            (f.clone(), exists, expiry)
        })
        .collect();
    // Drop the shared read handle before the mutable pass below, so
    // `get_hash_mut` does not copy-on-write a still-shared value.
    drop(hash);

    // Now determine actions for each field
    enum FieldAction {
        NotFound,
        Delete,
        Skip,
        SetExpiry(Instant),
    }

    let mut actions = Vec::with_capacity(field_info.len());
    let mut results = Vec::with_capacity(field_info.len());

    for (_field, exists, current_expiry) in &field_info {
        if !exists {
            actions.push(FieldAction::NotFound);
            results.push(Response::Integer(-2));
            continue;
        }

        // Check if time is past/zero -- delete the field
        if is_past_or_zero(time_val) {
            actions.push(FieldAction::Delete);
            results.push(Response::Integer(2));
            continue;
        }

        let expires_at = match time_converter(time_val) {
            Some(e) => e,
            None => {
                actions.push(FieldAction::Skip);
                results.push(Response::Integer(0));
                continue;
            }
        };

        // If expires_at is in the past, delete the field
        if expires_at <= Instant::now() {
            actions.push(FieldAction::Delete);
            results.push(Response::Integer(2));
            continue;
        }

        // Check NX/XX conditions
        if conditions.nx && current_expiry.is_some() {
            actions.push(FieldAction::Skip);
            results.push(Response::Integer(0));
            continue;
        }
        if conditions.xx && current_expiry.is_none() {
            actions.push(FieldAction::Skip);
            results.push(Response::Integer(0));
            continue;
        }

        // Check GT/LT conditions
        if conditions.gt {
            match current_expiry {
                Some(current) if expires_at <= *current => {
                    actions.push(FieldAction::Skip);
                    results.push(Response::Integer(0));
                    continue;
                }
                None => {
                    actions.push(FieldAction::Skip);
                    results.push(Response::Integer(0));
                    continue;
                }
                _ => {}
            }
        }
        if conditions.lt
            && let Some(current) = current_expiry
            && expires_at >= *current
        {
            actions.push(FieldAction::Skip);
            results.push(Response::Integer(0));
            continue;
        }

        actions.push(FieldAction::SetExpiry(expires_at));
        results.push(Response::Integer(1));
    }

    // Apply mutations with mutable access. The key was validated as a present
    // hash above; the let-else keeps this total without a panic path.
    let Some(hash) = ctx.store.get_hash_mut(key)? else {
        let results: Vec<Response> = field_args.iter().map(|_| Response::Integer(-2)).collect();
        return Ok(Response::Array(results));
    };

    for (i, (field, _, _)) in field_info.iter().enumerate() {
        match &actions[i] {
            FieldAction::Delete => {
                hash.remove(field);
            }
            FieldAction::SetExpiry(expires_at) => {
                hash.set_field_expiry(field, *expires_at);
            }
            _ => {}
        }
    }

    let is_empty = hash.is_empty();

    // Update store-level field expiry index (drop hash borrow first)
    for (i, (field, _, _)) in field_info.iter().enumerate() {
        match &actions[i] {
            FieldAction::Delete => {
                ctx.store.remove_field_expiry(key, field);
            }
            FieldAction::SetExpiry(expires_at) => {
                ctx.store.set_field_expiry(key, field, *expires_at);
            }
            _ => {}
        }
    }

    // If hash is now empty, delete the key
    if is_empty {
        ctx.store.delete(key);
    }

    Ok(Response::Array(results))
}

/// Shared implementation for HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME.
fn execute_httl_common(
    ctx: &mut CommandContext,
    args: &[Bytes],
    converter: impl Fn(Instant) -> i64,
) -> Result<Response, CommandError> {
    let key = &args[0];
    let field_args = parse_hexpire_read_args(&args[1..])?;

    // Purge expired fields first (lazy expiry)
    ctx.store.purge_expired_hash_fields(key);

    let hash = match ctx.store.get_hash(key)? {
        Some(h) => h,
        None => {
            let results: Vec<Response> = field_args.iter().map(|_| Response::Integer(-2)).collect();
            return Ok(Response::Array(results));
        }
    };

    let mut results = Vec::with_capacity(field_args.len());
    for field_arg in field_args {
        if !hash.contains(field_arg) {
            results.push(Response::Integer(-2));
            continue;
        }

        match ctx.store.get_field_expiry(key, field_arg) {
            Some(expires_at) => {
                results.push(Response::Integer(converter(expires_at)));
            }
            None => {
                results.push(Response::Integer(-1));
            }
        }
    }

    Ok(Response::Array(results))
}

// ============================================================================
// HEXPIRE - Set field expiry in seconds
// ============================================================================

pub struct HexpireCommand;

impl Command for HexpireCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HEXPIRE",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_hexpire_common(
            ctx,
            args,
            "hexpire",
            |secs| {
                if secs <= 0 {
                    return None;
                }
                Some(Instant::now() + Duration::from_secs(secs as u64))
            },
            |secs| secs <= 0,
        )
    }
}

// ============================================================================
// HPEXPIRE - Set field expiry in milliseconds
// ============================================================================

pub struct HpexpireCommand;

impl Command for HpexpireCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HPEXPIRE",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_hexpire_common(
            ctx,
            args,
            "hpexpire",
            |ms| {
                if ms <= 0 {
                    return None;
                }
                Some(Instant::now() + Duration::from_millis(ms as u64))
            },
            |ms| ms <= 0,
        )
    }
}

// ============================================================================
// HEXPIREAT - Set field expiry as Unix timestamp (seconds)
// ============================================================================

pub struct HexpireatCommand;

impl Command for HexpireatCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HEXPIREAT",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_hexpire_common(
            ctx,
            args,
            "hexpireat",
            |ts| {
                if ts < 0 {
                    return None;
                }
                unix_secs_to_instant(ts as u64)
            },
            |ts| ts < 0,
        )
    }
}

// ============================================================================
// HPEXPIREAT - Set field expiry as Unix timestamp (milliseconds)
// ============================================================================

pub struct HpexpireatCommand;

impl Command for HpexpireatCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HPEXPIREAT",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        execute_hexpire_common(
            ctx,
            args,
            "hpexpireat",
            |ts| {
                if ts < 0 {
                    return None;
                }
                unix_ms_to_instant(ts as u64)
            },
            |ts| ts < 0,
        )
    }
}

// ============================================================================
// HTTL - Get remaining TTL for hash fields (seconds)
// ============================================================================

pub struct HttlCommand;

impl Command for HttlCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HTTL",
            arity: Arity::AtLeast(4),
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
        execute_httl_common(ctx, args, |expires_at| {
            let now = Instant::now();
            if expires_at <= now {
                return -2;
            }
            let remaining = expires_at.duration_since(now);
            let secs = remaining.as_secs() as i64;
            let subsec = remaining.subsec_nanos();
            if subsec > 0 { secs + 1 } else { secs }
        })
    }
}

// ============================================================================
// HPTTL - Get remaining TTL for hash fields (milliseconds)
// ============================================================================

pub struct HpttlCommand;

impl Command for HpttlCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HPTTL",
            arity: Arity::AtLeast(4),
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
        execute_httl_common(ctx, args, |expires_at| {
            let now = Instant::now();
            if expires_at <= now {
                return -2;
            }
            expires_at.duration_since(now).as_millis() as i64
        })
    }
}

// ============================================================================
// HEXPIRETIME - Get Unix expiry time for hash fields (seconds)
// ============================================================================

pub struct HexpiretimeCommand;

impl Command for HexpiretimeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HEXPIRETIME",
            arity: Arity::AtLeast(4),
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
        execute_httl_common(ctx, args, instant_to_unix_secs)
    }
}

// ============================================================================
// HPEXPIRETIME - Get Unix expiry time for hash fields (milliseconds)
// ============================================================================

pub struct HpexpiretimeCommand;

impl Command for HpexpiretimeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HPEXPIRETIME",
            arity: Arity::AtLeast(4),
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
        execute_httl_common(ctx, args, instant_to_unix_ms)
    }
}

// ============================================================================
// HPERSIST - Remove expiry from hash fields
// ============================================================================

pub struct HpersistCommand;

impl Command for HpersistCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HPERSIST",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field_args = parse_hexpire_read_args(&args[1..])?;

        let hash = match ctx.store.get_hash(key)? {
            Some(h) => h,
            None => {
                let results: Vec<Response> =
                    field_args.iter().map(|_| Response::Integer(-2)).collect();
                return Ok(Response::Array(results));
            }
        };

        // Gather field info with immutable access
        let field_info: Vec<(Bytes, bool, bool)> = field_args
            .iter()
            .map(|f| {
                let exists = hash.contains(f);
                let has_expiry = if exists {
                    ctx.store.get_field_expiry(key, f).is_some()
                } else {
                    false
                };
                (f.clone(), exists, has_expiry)
            })
            .collect();
        drop(hash);

        let mut results = Vec::with_capacity(field_info.len());
        let mut fields_to_persist: Vec<Bytes> = Vec::new();

        for (field, exists, has_expiry) in &field_info {
            if !exists {
                results.push(Response::Integer(-2));
                continue;
            }
            if *has_expiry {
                fields_to_persist.push(field.clone());
                results.push(Response::Integer(1));
            } else {
                results.push(Response::Integer(-1));
            }
        }

        // Apply mutations
        if !fields_to_persist.is_empty()
            && let Some(hash) = ctx.store.get_hash_mut(key)?
        {
            for field in &fields_to_persist {
                hash.remove_field_expiry(field);
            }
        }

        // Update store-level index
        for field in &fields_to_persist {
            ctx.store.remove_field_expiry(key, field);
        }

        Ok(Response::Array(results))
    }
}

// ============================================================================
// Parsing helpers for HGETDEL / HGETEX / HSETEX
// ============================================================================

/// Expiry action for HGETEX / HSETEX commands.
enum FieldExpiryAction {
    /// No expiry option given.
    None,
    /// EX/PX/EXAT/PXAT → computed Instant.
    SetExpiry(Instant),
    /// PERSIST — remove existing field expiry (HGETEX only).
    Persist,
    /// KEEPTTL — retain existing field TTL (HSETEX only).
    KeepTtl,
}

/// Parse optional expiry option from args starting at `offset`.
/// Returns `(FieldExpiryAction, number_of_args_consumed)`.
/// Recognised options: EX, PX, EXAT, PXAT, PERSIST, KEEPTTL.
fn parse_field_expiry_option(
    args: &[Bytes],
    offset: usize,
    allow_persist: bool,
    allow_keepttl: bool,
) -> Result<(FieldExpiryAction, usize), CommandError> {
    if offset >= args.len() {
        return Ok((FieldExpiryAction::None, 0));
    }

    let keyword = args[offset].to_ascii_uppercase();
    match keyword.as_slice() {
        b"EX" => {
            let val = args.get(offset + 1).ok_or(CommandError::SyntaxError)?;
            // Field-expiry commands use the name-less message shape. The
            // seconds units (EX/EXAT) guard the secs*1000 conversion, matching
            // Redis's t_hash.c parseExpireTime(UNIT_SECONDS) rejection.
            let secs = checked_expire_value(parse_i64(val)?, true, ExpiryErr::Unnamed)?;
            let instant = Instant::now() + Duration::from_secs(secs);
            Ok((FieldExpiryAction::SetExpiry(instant), 2))
        }
        b"PX" => {
            let val = args.get(offset + 1).ok_or(CommandError::SyntaxError)?;
            let ms = checked_expire_value(parse_i64(val)?, false, ExpiryErr::Unnamed)?;
            let instant = Instant::now() + Duration::from_millis(ms);
            Ok((FieldExpiryAction::SetExpiry(instant), 2))
        }
        b"EXAT" => {
            let val = args.get(offset + 1).ok_or(CommandError::SyntaxError)?;
            let ts = checked_expire_value(parse_i64(val)?, true, ExpiryErr::Unnamed)?;
            let instant = unix_secs_to_instant(ts).ok_or(CommandError::NotInteger)?;
            Ok((FieldExpiryAction::SetExpiry(instant), 2))
        }
        b"PXAT" => {
            let val = args.get(offset + 1).ok_or(CommandError::SyntaxError)?;
            let ts = checked_expire_value(parse_i64(val)?, false, ExpiryErr::Unnamed)?;
            let instant = unix_ms_to_instant(ts).ok_or(CommandError::NotInteger)?;
            Ok((FieldExpiryAction::SetExpiry(instant), 2))
        }
        b"PERSIST" if allow_persist => Ok((FieldExpiryAction::Persist, 1)),
        b"KEEPTTL" if allow_keepttl => Ok((FieldExpiryAction::KeepTtl, 1)),
        _ => Ok((FieldExpiryAction::None, 0)),
    }
}

/// Parse `FIELDS numfields field [field ...]` from `args` starting at `offset`.
/// If `expect_pairs` is true, expects field-value pairs instead of bare fields.
/// Returns the slice of field (or field-value) args and the number of logical entries (numfields).
fn parse_fields_keyword(
    args: &[Bytes],
    offset: usize,
    expect_pairs: bool,
) -> Result<(&[Bytes], usize), CommandError> {
    if offset >= args.len() {
        return Err(CommandError::SyntaxError);
    }

    if !args[offset].eq_ignore_ascii_case(b"FIELDS") {
        return Err(CommandError::SyntaxError);
    }

    let numfields_pos = offset + 1;
    if numfields_pos >= args.len() {
        return Err(CommandError::SyntaxError);
    }

    let numfields = std::str::from_utf8(&args[numfields_pos])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(CommandError::NotInteger)?;

    if numfields == 0 {
        return Err(CommandError::InvalidArgument {
            message: "parameter 'numfields' value of 0 is not positive".to_string(),
        });
    }

    let data = &args[numfields_pos + 1..];
    let expected_len = if expect_pairs {
        numfields * 2
    } else {
        numfields
    };

    if data.len() != expected_len {
        return Err(CommandError::InvalidArgument {
            message: format!(
                "parameter 'numfields' value of {} does not match number of arguments",
                numfields
            ),
        });
    }

    Ok((data, numfields))
}

// ============================================================================
// HGETDEL - Get field values and atomically delete them
// ============================================================================

pub struct HgetdelCommand;

impl Command for HgetdelCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HGETDEL",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse: FIELDS numfields field [field ...]
        let (field_args, _numfields) = parse_fields_keyword(args, 1, false)?;

        // Lazy expiry
        ctx.store.purge_expired_hash_fields(key);

        // Check key exists
        let hash = match ctx.store.get_hash(key)? {
            Some(h) => h,
            None => {
                let results: Vec<Response> = field_args.iter().map(|_| Response::null()).collect();
                return Ok(Response::Array(results));
            }
        };

        // Phase 1 (immutable): read values
        let values: Vec<(Response, bool)> = field_args
            .iter()
            .map(|field| match hash.get(field) {
                Some(v) => (Response::bulk(v), true),
                None => (Response::null(), false),
            })
            .collect();
        drop(hash);

        // Phase 2 (mutable): delete existing fields. The key was validated as a
        // present hash above; the let-else keeps this total without a panic path.
        let Some(hash) = ctx.store.get_hash_mut(key)? else {
            let results: Vec<Response> = field_args.iter().map(|_| Response::null()).collect();
            return Ok(Response::Array(results));
        };
        for (i, field) in field_args.iter().enumerate() {
            if values[i].1 {
                hash.remove(field);
            }
        }
        let is_empty = hash.is_empty();

        // Phase 3: update store index
        for (i, field) in field_args.iter().enumerate() {
            if values[i].1 {
                ctx.store.remove_field_expiry(key, field);
            }
        }

        if is_empty {
            ctx.store.delete(key);
        }

        let results: Vec<Response> = values.into_iter().map(|(r, _)| r).collect();
        Ok(Response::Array(results))
    }
}

// ============================================================================
// HGETEX - Get field values and optionally set/remove their expiry
// ============================================================================

pub struct HgetexCommand;

impl Command for HgetexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HGETEX",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse optional expiry option before FIELDS keyword
        let (expiry_action, consumed) = parse_field_expiry_option(args, 1, true, false)?;

        // Parse: FIELDS numfields field [field ...]
        let fields_offset = 1 + consumed;
        let (field_args, _numfields) = parse_fields_keyword(args, fields_offset, false)?;

        // Lazy expiry
        ctx.store.purge_expired_hash_fields(key);

        // Check key exists
        let hash = match ctx.store.get_hash(key)? {
            Some(h) => h,
            None => {
                let results: Vec<Response> = field_args.iter().map(|_| Response::null()).collect();
                return Ok(Response::Array(results));
            }
        };

        // Phase 1 (immutable): read values and track which fields exist
        let values: Vec<(Response, bool)> = field_args
            .iter()
            .map(|field| match hash.get(field) {
                Some(v) => (Response::bulk(v), true),
                None => (Response::null(), false),
            })
            .collect();
        drop(hash);

        // Phase 2 (mutable): apply expiry action to existing fields. The key was
        // validated as a present hash above.
        match &expiry_action {
            FieldExpiryAction::SetExpiry(instant) => {
                if let Some(hash) = ctx.store.get_hash_mut(key)? {
                    for (i, field) in field_args.iter().enumerate() {
                        if values[i].1 {
                            hash.set_field_expiry(field, *instant);
                        }
                    }
                }
            }
            FieldExpiryAction::Persist => {
                if let Some(hash) = ctx.store.get_hash_mut(key)? {
                    for (i, field) in field_args.iter().enumerate() {
                        if values[i].1 {
                            hash.remove_field_expiry(field);
                        }
                    }
                }
            }
            FieldExpiryAction::None | FieldExpiryAction::KeepTtl => {}
        }

        // Phase 3: sync store index
        match &expiry_action {
            FieldExpiryAction::SetExpiry(instant) => {
                for (i, field) in field_args.iter().enumerate() {
                    if values[i].1 {
                        ctx.store.set_field_expiry(key, field, *instant);
                    }
                }
            }
            FieldExpiryAction::Persist => {
                for (i, field) in field_args.iter().enumerate() {
                    if values[i].1 {
                        ctx.store.remove_field_expiry(key, field);
                    }
                }
            }
            FieldExpiryAction::None | FieldExpiryAction::KeepTtl => {}
        }

        let results: Vec<Response> = values.into_iter().map(|(r, _)| r).collect();
        Ok(Response::Array(results))
    }
}

// ============================================================================
// HSETEX - Set field-value pairs with optional expiry and conditions
// ============================================================================

pub struct HsetexCommand;

impl Command for HsetexCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "HSETEX",
            arity: Arity::AtLeast(6),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::HASH,
                name: "hset",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse optional FNX/FXX condition
        let mut offset = 1;
        let mut fnx = false;
        let mut fxx = false;

        if offset < args.len() {
            let upper = args[offset].to_ascii_uppercase();
            match upper.as_slice() {
                b"FNX" => {
                    fnx = true;
                    offset += 1;
                }
                b"FXX" => {
                    fxx = true;
                    offset += 1;
                }
                _ => {}
            }
        }

        // Parse optional expiry option
        let (expiry_action, consumed) = parse_field_expiry_option(args, offset, false, true)?;
        offset += consumed;

        // Parse: FIELDS numfields field value [field value ...]
        let (pair_args, _numfields) = parse_fields_keyword(args, offset, true)?;

        // Extract field-value pairs
        let pairs: Vec<(&Bytes, &Bytes)> = pair_args
            .chunks(2)
            .map(|chunk| (&chunk[0], &chunk[1]))
            .collect();

        // Phase 1 (immutable): check FNX/FXX conditions and gather KEEPTTL expiries
        // We need get_or_create for the create case, but must read state first.
        {
            let _ = ctx.get_or_create::<HashValue>(key)?;
        }

        // Now use immutable access for condition checks and expiry gathering
        let value = ctx.store.get(key).unwrap();
        let hash = value.as_hash().unwrap();

        if fnx {
            for (field, _) in &pairs {
                if hash.contains(field) {
                    return Ok(Response::Integer(0));
                }
            }
        }
        if fxx {
            for (field, _) in &pairs {
                if !hash.contains(field) {
                    return Ok(Response::Integer(0));
                }
            }
        }

        // For KEEPTTL: save existing field expiries before overwriting
        let saved_expiries: Vec<Option<Instant>> =
            if matches!(expiry_action, FieldExpiryAction::KeepTtl) {
                pairs
                    .iter()
                    .map(|(field, _)| {
                        hash.get_field_expiry(field)
                            .or_else(|| ctx.store.get_field_expiry(key, field))
                    })
                    .collect()
            } else {
                Vec::new()
            };
        drop(value);

        // Phase 2 (mutable): set all fields (this clears field expiry on each
        // field internally). The key was get-or-created as a hash above.
        if let Some(hash) = ctx.store.get_hash_mut(key)? {
            for (field, value) in &pairs {
                hash.set(
                    (*field).clone(),
                    (*value).clone(),
                    ListpackThresholds::DEFAULT_HASH,
                );
            }
        }

        // Clear store index for all fields (HSET clears field expiry)
        for (field, _) in &pairs {
            ctx.store.remove_field_expiry(key, field);
        }

        // Apply expiry
        match &expiry_action {
            FieldExpiryAction::SetExpiry(instant) => {
                if let Some(hash) = ctx.store.get_hash_mut(key)? {
                    for (field, _) in &pairs {
                        hash.set_field_expiry(field, *instant);
                    }
                }
                // Drop hash borrow before store index updates
                for (field, _) in &pairs {
                    ctx.store.set_field_expiry(key, field, *instant);
                }
            }
            FieldExpiryAction::KeepTtl => {
                if let Some(hash) = ctx.store.get_hash_mut(key)? {
                    for (i, (field, _)) in pairs.iter().enumerate() {
                        if let Some(expiry) = saved_expiries[i] {
                            hash.set_field_expiry(field, expiry);
                        }
                    }
                }
                // Drop hash borrow before store index updates
                for (i, (field, _)) in pairs.iter().enumerate() {
                    if let Some(expiry) = saved_expiries[i] {
                        ctx.store.set_field_expiry(key, field, expiry);
                    }
                }
            }
            FieldExpiryAction::None | FieldExpiryAction::Persist => {}
        }

        Ok(Response::Integer(1))
    }
}

#[cfg(test)]
mod expiry_grammar_pin_tests {
    //! Wire-compat pins for HGETEX / HSETEX field-expiry grammar. These use the
    //! name-less `invalid expire time in command` message shape (distinct from
    //! the SET family's quoted form) and carry no secs*1000 overflow guard.
    use super::*;
    use frogdb_core::HashMapStore;
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn ctx() -> CommandContext<'static> {
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    fn invalid_msg<C: Command>(cmd: C, parts: &[&str]) -> String {
        let mut c = ctx();
        match cmd.execute(&mut c, &args(parts)) {
            Err(CommandError::InvalidArgument { message }) => message,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn hgetex_ex_zero_message() {
        assert_eq!(
            invalid_msg(HgetexCommand, &["h", "EX", "0", "FIELDS", "1", "f"]),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hgetex_px_negative_message() {
        assert_eq!(
            invalid_msg(HgetexCommand, &["h", "PX", "-1", "FIELDS", "1", "f"]),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hgetex_exat_zero_message() {
        assert_eq!(
            invalid_msg(HgetexCommand, &["h", "EXAT", "0", "FIELDS", "1", "f"]),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hgetex_pxat_zero_message() {
        assert_eq!(
            invalid_msg(HgetexCommand, &["h", "PXAT", "0", "FIELDS", "1", "f"]),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hsetex_ex_zero_message() {
        assert_eq!(
            invalid_msg(HsetexCommand, &["h", "EX", "0", "FIELDS", "1", "f", "v"]),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hsetex_px_negative_message() {
        assert_eq!(
            invalid_msg(HsetexCommand, &["h", "PX", "-2", "FIELDS", "1", "f", "v"]),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hgetex_ex_secs_overflow_rejected() {
        // Redis parity: t_hash.c parseExpireTime rejects over-large seconds
        // values for the seconds units (EX/EXAT), keeping the name-less shape.
        assert_eq!(
            invalid_msg(
                HgetexCommand,
                &["h", "EX", "18446744073709551", "FIELDS", "1", "f"]
            ),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hgetex_exat_secs_overflow_rejected() {
        assert_eq!(
            invalid_msg(
                HgetexCommand,
                &["h", "EXAT", "18446744073709551", "FIELDS", "1", "f"]
            ),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hsetex_ex_secs_overflow_rejected() {
        assert_eq!(
            invalid_msg(
                HsetexCommand,
                &["h", "EX", "18446744073709551", "FIELDS", "1", "f", "v"]
            ),
            "invalid expire time in command"
        );
    }

    #[test]
    fn hsetex_px_large_value_accepted() {
        // PX is a milliseconds unit: no *1000 guard applies.
        let mut c = ctx();
        let r = HsetexCommand
            .execute(
                &mut c,
                &args(&["h", "PX", "18446744073709551", "FIELDS", "1", "f", "v"]),
            )
            .unwrap();
        assert_eq!(r, Response::Integer(1));
    }
}
