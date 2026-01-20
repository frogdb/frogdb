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
    Arity, Command, CommandContext, CommandError, CommandFlags, HashValue, Value,
};
use frogdb_protocol::Response;

/// Parse a string as i64.
fn parse_i64(arg: &[u8]) -> Result<i64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Parse a string as f64.
fn parse_f64(arg: &[u8]) -> Result<f64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| {
            if s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("+inf") {
                Some(f64::INFINITY)
            } else if s.eq_ignore_ascii_case("-inf") {
                Some(f64::NEG_INFINITY)
            } else {
                s.parse().ok()
            }
        })
        .ok_or(CommandError::NotFloat)
}

/// Parse a string as usize.
fn parse_usize(arg: &[u8]) -> Result<usize, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Format a float for Redis compatibility.
fn format_float(f: f64) -> String {
    if f == f64::INFINITY {
        return "inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if f == 0.0 {
        return "0".to_string();
    }

    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{:.0}", f);
    }

    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

/// Get or create a hash, returning an error if the key exists but is wrong type.
fn get_or_create_hash<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut HashValue, CommandError> {
    // Check if key exists and is wrong type
    if let Some(value) = ctx.store.get(key) {
        if value.as_hash().is_none() {
            return Err(CommandError::WrongType);
        }
    } else {
        // Create new hash
        ctx.store.set(key.clone(), Value::hash());
    }

    // Now get mutable reference
    ctx.store
        .get_mut(key)
        .and_then(|v| v.as_hash_mut())
        .ok_or(CommandError::WrongType)
}

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    let mut results = Vec::with_capacity(hash.len() * 2);
                    for (field, value) in hash.iter() {
                        results.push(Response::bulk(field.clone()));
                        results.push(Response::bulk(value.clone()));
                    }
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let field = args[1].clone();
        let increment = parse_f64(&args[2])?;

        let hash = get_or_create_hash(ctx, key)?;

        match hash.incr_by_float(field, increment) {
            Ok(new_val) => Ok(Response::bulk(Bytes::from(format_float(new_val)))),
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor = parse_usize(&args[1])?;

        // Parse options
        let mut _match_pattern: Option<&[u8]> = None;
        let mut count: usize = 10;

        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    _match_pattern = Some(&args[i]);
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&args[i])?;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                if let Some(hash) = value.as_hash() {
                    // Simple cursor-based scan implementation
                    // In real Redis, MATCH pattern is applied
                    let entries: Vec<_> = hash.iter().collect();
                    let total = entries.len();

                    if cursor >= total {
                        // Return empty with cursor 0
                        return Ok(Response::Array(vec![
                            Response::bulk(Bytes::from("0")),
                            Response::Array(vec![]),
                        ]));
                    }

                    let end = (cursor + count).min(total);
                    let next_cursor = if end >= total { 0 } else { end };

                    let mut results = Vec::with_capacity((end - cursor) * 2);
                    for (field, value) in entries.into_iter().skip(cursor).take(count) {
                        results.push(Response::bulk(field.clone()));
                        results.push(Response::bulk(value.clone()));
                    }

                    Ok(Response::Array(vec![
                        Response::bulk(Bytes::from(next_cursor.to_string())),
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
                        let mut results = Vec::with_capacity(random_fields.len() * 2);
                        for (field, value) in random_fields {
                            results.push(Response::bulk(field));
                            if let Some(v) = value {
                                results.push(Response::bulk(v));
                            }
                        }
                        Ok(Response::Array(results))
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
