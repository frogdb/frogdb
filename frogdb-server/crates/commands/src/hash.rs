//! Hash commands.
//!
//! Commands for hash manipulation:
//! - HSET, HSETNX, HGET, HDEL, HMSET, HMGET - basic operations
//! - HGETALL, HKEYS, HVALS - retrieve all data
//! - HEXISTS, HLEN, HSTRLEN - inspection
//! - HINCRBY, HINCRBYFLOAT - numeric operations
//! - HSCAN, HRANDFIELD - iteration & random

use bytes::Bytes;
use frogdb_core::{
    ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, WalStrategy,
};
use frogdb_protocol::Response;

use super::utils::{format_float, get_or_create_hash, parse_f64, parse_i64};

// ============================================================================
// HSET - Set hash fields
// ============================================================================

pub struct HsetCommand;

impl Command for HsetCommand {
    fn name(&self) -> &'static str {
        "HSET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // HSET key field value [field value ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Must have even number of field-value pairs
        if !(args.len() - 1).is_multiple_of(2) {
            return Err(CommandError::InvalidArgument {
                message: "wrong number of arguments for 'hset' command".to_string(),
            });
        }

        let hash = get_or_create_hash(ctx, key)?;

        let mut new_fields = 0i64;
        for chunk in args[1..].chunks(2) {
            let field = chunk[0].clone();
            let value = chunk[1].clone();
            if hash.set(field, value) {
                new_fields += 1;
            }
        }

        Ok(Response::Integer(new_fields))
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
// HSETNX - Set hash field only if not exists
// ============================================================================

pub struct HsetnxCommand;

impl Command for HsetnxCommand {
    fn name(&self) -> &'static str {
        "HSETNX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // HSETNX key field value
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let value = args[2].clone();

        let hash = get_or_create_hash(ctx, key)?;

        if hash.set_nx(field, value) {
            Ok(Response::Integer(1))
        } else {
            Ok(Response::Integer(0))
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
// HGET - Get hash field
// ============================================================================

pub struct HgetCommand;

impl Command for HgetCommand {
    fn name(&self) -> &'static str {
        "HGET"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // HGET key field
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = &args[1];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    match hash.get(field) {
                        Some(v) => Ok(Response::bulk(v.clone())),
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
// HDEL - Delete hash fields
// ============================================================================

pub struct HdelCommand;

impl Command for HdelCommand {
    fn name(&self) -> &'static str {
        "HDEL"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // HDEL key field [field ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistOrDeleteFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        if ctx.store.get(key).is_none() {
            return Ok(Response::Integer(0));
        }

        // Verify type
        if ctx.store.get(key).unwrap().as_hash().is_none() {
            return Err(CommandError::WrongType);
        }

        let hash = ctx.store.get_mut(key).unwrap().as_hash_mut().unwrap();

        let mut deleted = 0i64;
        for field in &args[1..] {
            if hash.remove(field) {
                deleted += 1;
            }
        }

        // Delete key if hash is now empty
        if hash.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(deleted))
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
// HMSET - Set multiple hash fields (deprecated alias for HSET)
// ============================================================================

pub struct HmsetCommand;

impl Command for HmsetCommand {
    fn name(&self) -> &'static str {
        "HMSET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // HMSET key field value [field value ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Must have even number of field-value pairs
        if !(args.len() - 1).is_multiple_of(2) {
            return Err(CommandError::InvalidArgument {
                message: "wrong number of arguments for 'hmset' command".to_string(),
            });
        }

        let hash = get_or_create_hash(ctx, key)?;

        for chunk in args[1..].chunks(2) {
            let field = chunk[0].clone();
            let value = chunk[1].clone();
            hash.set(field, value);
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
// HMGET - Get multiple hash fields
// ============================================================================

pub struct HmgetCommand;

impl Command for HmgetCommand {
    fn name(&self) -> &'static str {
        "HMGET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // HMGET key field [field ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    let results: Vec<Response> = args[1..]
                        .iter()
                        .map(|field| match hash.get(field) {
                            Some(v) => Response::bulk(v.clone()),
                            None => Response::null(),
                        })
                        .collect();
                    Ok(Response::Array(results))
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => {
                // Key doesn't exist - return array of nulls
                let results: Vec<Response> = args[1..].iter().map(|_| Response::null()).collect();
                Ok(Response::Array(results))
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
// HGETALL - Get all fields and values
// ============================================================================

pub struct HgetallCommand;

impl Command for HgetallCommand {
    fn name(&self) -> &'static str {
        "HGETALL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // HGETALL key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    if ctx.protocol_version.is_resp3() {
                        // RESP3: Return as Map
                        let pairs: Vec<(Response, Response)> = hash
                            .iter()
                            .map(|(field, value)| {
                                (Response::bulk(field.clone()), Response::bulk(value.clone()))
                            })
                            .collect();
                        Ok(Response::Map(pairs))
                    } else {
                        // RESP2: Return as flattened Array
                        let mut results = Vec::with_capacity(hash.len() * 2);
                        for (field, value) in hash.iter() {
                            results.push(Response::bulk(field.clone()));
                            results.push(Response::bulk(value.clone()));
                        }
                        Ok(Response::Array(results))
                    }
                } else {
                    Err(CommandError::WrongType)
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// HKEYS - Get all field names
// ============================================================================

pub struct HkeysCommand;

impl Command for HkeysCommand {
    fn name(&self) -> &'static str {
        "HKEYS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // HKEYS key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    let results: Vec<Response> =
                        hash.keys().map(|k| Response::bulk(k.clone())).collect();
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
// HVALS - Get all values
// ============================================================================

pub struct HvalsCommand;

impl Command for HvalsCommand {
    fn name(&self) -> &'static str {
        "HVALS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // HVALS key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    let results: Vec<Response> =
                        hash.values().map(|v| Response::bulk(v.clone())).collect();
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
// HEXISTS - Check if field exists
// ============================================================================

pub struct HexistsCommand;

impl Command for HexistsCommand {
    fn name(&self) -> &'static str {
        "HEXISTS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // HEXISTS key field
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = &args[1];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    if hash.contains(field) {
                        Ok(Response::Integer(1))
                    } else {
                        Ok(Response::Integer(0))
                    }
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
// HLEN - Get number of fields
// ============================================================================

pub struct HlenCommand;

impl Command for HlenCommand {
    fn name(&self) -> &'static str {
        "HLEN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // HLEN key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    Ok(Response::Integer(hash.len() as i64))
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
// HINCRBY - Increment integer field
// ============================================================================

pub struct HincrbyCommand;

impl Command for HincrbyCommand {
    fn name(&self) -> &'static str {
        "HINCRBY"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // HINCRBY key field increment
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let increment = parse_i64(&args[2])?;

        let hash = get_or_create_hash(ctx, key)?;

        match hash.incr_by(field, increment) {
            Ok(new_val) => Ok(Response::Integer(new_val)),
            Err(frogdb_core::IncrementError::NotInteger) => Err(CommandError::InvalidArgument {
                message: "hash value is not an integer".to_string(),
            }),
            Err(frogdb_core::IncrementError::Overflow) => Err(CommandError::InvalidArgument {
                message: "increment or decrement would overflow".to_string(),
            }),
            Err(_) => Err(CommandError::InvalidArgument {
                message: "hash value is not an integer".to_string(),
            }),
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
// HINCRBYFLOAT - Increment float field
// ============================================================================

pub struct HincrbyfloatCommand;

impl Command for HincrbyfloatCommand {
    fn name(&self) -> &'static str {
        "HINCRBYFLOAT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // HINCRBYFLOAT key field increment
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let increment = parse_f64(&args[2])?;

        if increment.is_nan() || increment.is_infinite() {
            return Err(CommandError::InvalidArgument {
                message: "value is NaN or Infinity".to_string(),
            });
        }

        let hash = get_or_create_hash(ctx, key)?;

        match hash.incr_by_float(field, increment) {
            Ok(new_val) => {
                // Always return bulk string (not Double), matching Redis behavior
                Ok(Response::bulk(Bytes::from(format_float(new_val))))
            }
            Err(frogdb_core::IncrementError::NotFloat) => Err(CommandError::InvalidArgument {
                message: "hash value is not a float".to_string(),
            }),
            Err(frogdb_core::IncrementError::Overflow) => Err(CommandError::InvalidArgument {
                message: "increment would produce NaN or Infinity".to_string(),
            }),
            Err(_) => Err(CommandError::InvalidArgument {
                message: "hash value is not a float".to_string(),
            }),
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
// HSTRLEN - Get length of field value
// ============================================================================

pub struct HstrlenCommand;

impl Command for HstrlenCommand {
    fn name(&self) -> &'static str {
        "HSTRLEN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // HSTRLEN key field
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = &args[1];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    match hash.get(field) {
                        Some(v) => Ok(Response::Integer(v.len() as i64)),
                        None => Ok(Response::Integer(0)),
                    }
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
// HSCAN - Cursor-based iteration
// ============================================================================

pub struct HscanCommand;

impl Command for HscanCommand {
    fn name(&self) -> &'static str {
        "HSCAN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // HSCAN key cursor [MATCH pattern] [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor: u64 = crate::utils::parse_u64(&args[1])?;

        // Parse options [MATCH pattern] [COUNT count]
        let mut match_pattern: Option<&[u8]> = None;
        let mut count: usize = 10;
        let mut parser = ArgParser::from_position(args, 2);

        while parser.has_more() {
            if let Some(value) = parser.try_flag_value(b"MATCH")? {
                match_pattern = Some(value.as_ref());
            } else if let Some(value) = parser.try_flag_usize(b"COUNT")? {
                count = value;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    let (new_cursor, results) = crate::utils::hash_cursor_scan(
                        hash.iter(),
                        cursor,
                        count,
                        match_pattern,
                        |entry: &(&Bytes, &Bytes)| entry.0.as_ref(),
                        |entry: (&Bytes, &Bytes), results: &mut Vec<Response>| {
                            results.push(Response::bulk(Bytes::clone(entry.0)));
                            results.push(Response::bulk(Bytes::clone(entry.1)));
                        },
                    );

                    Ok(Response::Array(vec![
                        Response::bulk(Bytes::from(new_cursor.to_string())),
                        Response::Array(results),
                    ]))
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => {
                // Key doesn't exist - return empty scan
                Ok(Response::Array(vec![
                    Response::bulk(Bytes::from("0")),
                    Response::Array(vec![]),
                ]))
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
// HRANDFIELD - Get random fields
// ============================================================================

pub struct HrandfieldCommand;

impl Command for HrandfieldCommand {
    fn name(&self) -> &'static str {
        "HRANDFIELD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // HRANDFIELD key [count [WITHVALUES]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
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
                                    Response::Array(vec![
                                        Response::bulk(f),
                                        Response::bulk(v.unwrap()),
                                    ])
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
                } else {
                    Err(CommandError::WrongType)
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
