//! String commands.
//!
//! Commands for string manipulation:
//! - SETNX, SETEX, PSETEX - SET variants
//! - APPEND, STRLEN - string operations
//! - GETRANGE, SETRANGE, SUBSTR - substring operations
//! - GETDEL, GETEX - GET variants
//! - INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT - numeric operations

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, Expiry,
    IncrementError, MergeStrategy, SetCondition, SetOptions, SetResult, StringValue, Value,
};
use frogdb_protocol::Response;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::utils::{format_float, parse_f64, parse_i64, parse_u64};

// ============================================================================
// SETNX - SET if Not eXists
// ============================================================================

pub struct SetnxCommand;

impl Command for SetnxCommand {
    fn name(&self) -> &'static str {
        "SETNX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let value = args[1].clone();

        let opts = SetOptions {
            condition: SetCondition::NX,
            ..Default::default()
        };

        match ctx.store.set_with_options(key, Value::string(value), opts) {
            SetResult::Ok | SetResult::OkWithOldValue(_) => Ok(Response::Integer(1)),
            SetResult::NotSet => Ok(Response::Integer(0)),
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
// SETEX - SET with EXpiry (seconds)
// ============================================================================

pub struct SetexCommand;

impl Command for SetexCommand {
    fn name(&self) -> &'static str {
        "SETEX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // SETEX key seconds value
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let seconds = parse_i64(&args[1])?;
        let value = args[2].clone();

        if seconds <= 0 {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'setex' command".to_string(),
            });
        }

        let opts = SetOptions {
            expiry: Some(Expiry::Ex(seconds as u64)),
            ..Default::default()
        };

        ctx.store.set_with_options(key, Value::string(value), opts);
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
// PSETEX - SET with EXpiry (milliseconds)
// ============================================================================

pub struct PsetexCommand;

impl Command for PsetexCommand {
    fn name(&self) -> &'static str {
        "PSETEX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // PSETEX key milliseconds value
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let ms = parse_i64(&args[1])?;
        let value = args[2].clone();

        if ms <= 0 {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'psetex' command".to_string(),
            });
        }

        let opts = SetOptions {
            expiry: Some(Expiry::Px(ms as u64)),
            ..Default::default()
        };

        ctx.store.set_with_options(key, Value::string(value), opts);
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
// APPEND - Append to string
// ============================================================================

pub struct AppendCommand;

impl Command for AppendCommand {
    fn name(&self) -> &'static str {
        "APPEND"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let value = &args[1];

        // Get or create the string
        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                let new_len = sv.append(value);
                Ok(Response::Integer(new_len as i64))
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create it
            ctx.store.set(key.clone(), Value::string(value.clone()));
            Ok(Response::Integer(value.len() as i64))
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
// STRLEN - Get string length
// ============================================================================

pub struct StrlenCommand;

impl Command for StrlenCommand {
    fn name(&self) -> &'static str {
        "STRLEN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    Ok(Response::Integer(sv.len() as i64))
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
// GETRANGE - Get substring
// ============================================================================

pub struct GetrangeCommand;

impl Command for GetrangeCommand {
    fn name(&self) -> &'static str {
        "GETRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // GETRANGE key start end
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let start = parse_i64(&args[1])?;
        let end = parse_i64(&args[2])?;

        match ctx.store.get(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    let range = sv.get_range(start, end);
                    Ok(Response::bulk(range))
                } else {
                    Err(CommandError::WrongType)
                }
            }
            None => Ok(Response::bulk(Bytes::new())),
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
// SETRANGE - Overwrite part of string
// ============================================================================

pub struct SetrangeCommand;

impl Command for SetrangeCommand {
    fn name(&self) -> &'static str {
        "SETRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // SETRANGE key offset value
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let offset = parse_u64(&args[1])? as usize;
        let value = &args[2];

        // Max string length check (Redis uses 512MB, we'll use 512MB too)
        const MAX_STRING_LEN: usize = 512 * 1024 * 1024;
        if offset + value.len() > MAX_STRING_LEN {
            return Err(CommandError::InvalidArgument {
                message: "string exceeds maximum allowed size".to_string(),
            });
        }

        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                let new_len = sv.set_range(offset, value);
                Ok(Response::Integer(new_len as i64))
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist - Redis does not create the key when offset=0
            // and value is empty
            if offset == 0 && value.is_empty() {
                return Ok(Response::Integer(0));
            }
            // Create with padding
            let mut sv = StringValue::new(Bytes::new());
            let new_len = sv.set_range(offset, value);
            ctx.store.set(key.clone(), Value::String(sv));
            Ok(Response::Integer(new_len as i64))
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
// GETDEL - GET and DELETE atomically
// ============================================================================

pub struct GetdelCommand;

impl Command for GetdelCommand {
    fn name(&self) -> &'static str {
        "GETDEL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get_and_delete(key) {
            Some(value) => {
                if let Some(sv) = value.as_string() {
                    Ok(Response::bulk(sv.as_bytes()))
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
// GETEX - GET with EXpiry modification
// ============================================================================

pub struct GetexCommand;

impl Command for GetexCommand {
    fn name(&self) -> &'static str {
        "GETEX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // GETEX key [EXAT|PXAT|EX|PX|PERSIST]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // First, get the value
        let value = match ctx.store.get_with_expiry_check(key) {
            Some(v) => v,
            None => return Ok(Response::null()),
        };

        let sv = value.as_string().ok_or(CommandError::WrongType)?;
        let result = Response::bulk(sv.as_bytes());

        // Parse options
        let mut i = 1;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"EX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let seconds = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if seconds <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'getex' command".to_string(),
                        });
                    }
                    if seconds > i64::MAX / 1000 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'getex' command".to_string(),
                        });
                    }
                    let expires_at = Instant::now() + Duration::from_secs(seconds as u64);
                    ctx.store.set_expiry(key, expires_at);
                }
                b"PX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ms = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if ms <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'getex' command".to_string(),
                        });
                    }
                    let expires_at = Instant::now() + Duration::from_millis(ms as u64);
                    ctx.store.set_expiry(key, expires_at);
                }
                b"EXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if ts <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'getex' command".to_string(),
                        });
                    }
                    let target = UNIX_EPOCH + Duration::from_secs(ts as u64);
                    let now = SystemTime::now();
                    if let Ok(duration) = target.duration_since(now) {
                        ctx.store.set_expiry(key, Instant::now() + duration);
                    }
                }
                b"PXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_i64(&args[i]).map_err(|_| CommandError::NotInteger)?;
                    if ts <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "invalid expire time in 'getex' command".to_string(),
                        });
                    }
                    let target = UNIX_EPOCH + Duration::from_millis(ts as u64);
                    let now = SystemTime::now();
                    if let Ok(duration) = target.duration_since(now) {
                        ctx.store.set_expiry(key, Instant::now() + duration);
                    }
                }
                b"PERSIST" => {
                    ctx.store.persist(key);
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        Ok(result)
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
// INCR - Increment by 1
// ============================================================================

pub struct IncrCommand;

impl Command for IncrCommand {
    fn name(&self) -> &'static str {
        "INCR"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment(1) {
                    Ok(new_val) => Ok(Response::Integer(new_val)),
                    Err(IncrementError::NotInteger) => Err(CommandError::NotInteger),
                    Err(IncrementError::Overflow) => Err(CommandError::NotInteger),
                    Err(IncrementError::NotFloat) => Err(CommandError::NotInteger),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create with value 1
            ctx.store
                .set(key.clone(), Value::String(StringValue::from_integer(1)));
            Ok(Response::Integer(1))
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
// DECR - Decrement by 1
// ============================================================================

pub struct DecrCommand;

impl Command for DecrCommand {
    fn name(&self) -> &'static str {
        "DECR"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment(-1) {
                    Ok(new_val) => Ok(Response::Integer(new_val)),
                    Err(IncrementError::NotInteger) => Err(CommandError::NotInteger),
                    Err(IncrementError::Overflow) => Err(CommandError::NotInteger),
                    Err(IncrementError::NotFloat) => Err(CommandError::NotInteger),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create with value -1
            ctx.store
                .set(key.clone(), Value::String(StringValue::from_integer(-1)));
            Ok(Response::Integer(-1))
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
// INCRBY - Increment by N
// ============================================================================

pub struct IncrbyCommand;

impl Command for IncrbyCommand {
    fn name(&self) -> &'static str {
        "INCRBY"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let delta = parse_i64(&args[1])?;

        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment(delta) {
                    Ok(new_val) => Ok(Response::Integer(new_val)),
                    Err(IncrementError::NotInteger) => Err(CommandError::NotInteger),
                    Err(IncrementError::Overflow) => Err(CommandError::NotInteger),
                    Err(IncrementError::NotFloat) => Err(CommandError::NotInteger),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create with value delta
            ctx.store
                .set(key.clone(), Value::String(StringValue::from_integer(delta)));
            Ok(Response::Integer(delta))
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
// DECRBY - Decrement by N
// ============================================================================

pub struct DecrbyCommand;

impl Command for DecrbyCommand {
    fn name(&self) -> &'static str {
        "DECRBY"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let delta = parse_i64(&args[1])?;
        let neg_delta = delta.checked_neg().ok_or(CommandError::NotInteger)?;

        // Decrement is just negative increment
        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment(neg_delta) {
                    Ok(new_val) => Ok(Response::Integer(new_val)),
                    Err(IncrementError::NotInteger) => Err(CommandError::NotInteger),
                    Err(IncrementError::Overflow) => Err(CommandError::NotInteger),
                    Err(IncrementError::NotFloat) => Err(CommandError::NotInteger),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create with value -delta
            ctx.store.set(
                key.clone(),
                Value::String(StringValue::from_integer(neg_delta)),
            );
            Ok(Response::Integer(neg_delta))
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
// INCRBYFLOAT - Increment by float
// ============================================================================

pub struct IncrbyfloatCommand;

impl Command for IncrbyfloatCommand {
    fn name(&self) -> &'static str {
        "INCRBYFLOAT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let delta = parse_f64(&args[1])?;

        if delta.is_nan() || delta.is_infinite() {
            return Err(CommandError::InvalidArgument {
                message: "increment would produce NaN or Infinity".to_string(),
            });
        }

        let is_resp3 = ctx.protocol_version.is_resp3();

        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment_float(delta) {
                    Ok(new_val) => {
                        if is_resp3 {
                            Ok(Response::Double(new_val))
                        } else {
                            Ok(Response::bulk(Bytes::from(format_float(new_val))))
                        }
                    }
                    Err(IncrementError::NotFloat) => Err(CommandError::NotFloat),
                    Err(IncrementError::NotInteger) => Err(CommandError::NotFloat),
                    Err(IncrementError::Overflow) => Err(CommandError::InvalidArgument {
                        message: "increment would produce NaN or Infinity".to_string(),
                    }),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create with value delta
            let sv = StringValue::new(Bytes::from(format_float(delta)));
            ctx.store.set(key.clone(), Value::String(sv));
            if is_resp3 {
                Ok(Response::Double(delta))
            } else {
                Ok(Response::bulk(Bytes::from(format_float(delta))))
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
// MGET - Get multiple values
// ============================================================================

pub struct MgetCommand;

impl Command for MgetCommand {
    fn name(&self) -> &'static str {
        "MGET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::OrderedArray,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Single-shard execution (multi-shard handled by connection routing)
        let results: Vec<Response> = args
            .iter()
            .map(|key| match ctx.store.get_with_expiry_check(key) {
                Some(value) => value
                    .as_string()
                    .map(|sv| Response::bulk(sv.as_bytes()))
                    .unwrap_or(Response::null()),
                None => Response::null(),
            })
            .collect();
        Ok(Response::Array(results))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// MSET - Set multiple key-value pairs
// ============================================================================

pub struct MsetCommand;

impl Command for MsetCommand {
    fn name(&self) -> &'static str {
        "MSET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::AllOk,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if !args.len().is_multiple_of(2) {
            return Err(CommandError::WrongArity { command: "mset" });
        }

        for pair in args.chunks(2) {
            ctx.store
                .set(pair[0].clone(), Value::string(pair[1].clone()));
        }

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().step_by(2).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// MSETNX - Set multiple key-value pairs only if none exist
// ============================================================================

pub struct MsetnxCommand;

impl Command for MsetnxCommand {
    fn name(&self) -> &'static str {
        "MSETNX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if !args.len().is_multiple_of(2) {
            return Err(CommandError::WrongArity { command: "msetnx" });
        }

        // Check if any key already exists
        for pair in args.chunks(2) {
            if ctx.store.contains(&pair[0]) {
                return Ok(Response::Integer(0));
            }
        }

        // None exist, set all
        for pair in args.chunks(2) {
            ctx.store
                .set(pair[0].clone(), Value::string(pair[1].clone()));
        }

        Ok(Response::Integer(1))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().step_by(2).map(|a| a.as_ref()).collect()
    }

    /// MSETNX always requires same-slot for atomicity, even with allow_cross_slot_standalone.
    fn requires_same_slot(&self) -> bool {
        true
    }
}

// ============================================================================
// LCS - Longest Common Subsequence
// ============================================================================

pub struct LcsCommand;

/// Options for the LCS command.
#[derive(Default)]
struct LcsOptions {
    /// Return only the length of the LCS, not the string.
    len_only: bool,
    /// Return match positions (IDX mode).
    idx: bool,
    /// Minimum match length to report (for IDX mode).
    min_match_len: usize,
    /// Include match lengths in output (for IDX mode).
    with_match_len: bool,
}

/// Represents a match in the IDX output.
#[derive(Debug)]
struct LcsMatch {
    /// Start and end positions in the first string.
    a_range: (usize, usize),
    /// Start and end positions in the second string.
    b_range: (usize, usize),
    /// Length of the match.
    len: usize,
}

impl Command for LcsCommand {
    fn name(&self) -> &'static str {
        "LCS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key1 = &args[0];
        let key2 = &args[1];

        // Parse options
        let mut opts = LcsOptions::default();
        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LEN" => {
                    opts.len_only = true;
                    i += 1;
                }
                b"IDX" => {
                    opts.idx = true;
                    i += 1;
                }
                b"MINMATCHLEN" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::InvalidArgument {
                            message: "MINMATCHLEN requires an argument".to_string(),
                        });
                    }
                    opts.min_match_len = parse_u64(&args[i])? as usize;
                    i += 1;
                }
                b"WITHMATCHLEN" => {
                    opts.with_match_len = true;
                    i += 1;
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        message: format!("Unknown option '{}'", String::from_utf8_lossy(&opt)),
                    });
                }
            }
        }

        // WITHMATCHLEN requires IDX
        if opts.with_match_len && !opts.idx {
            return Err(CommandError::InvalidArgument {
                message: "WITHMATCHLEN requires IDX".to_string(),
            });
        }

        // Get the two strings (missing keys treated as empty)
        let s1 = ctx
            .store
            .get(key1)
            .and_then(|v| v.as_string().map(|sv| sv.as_bytes().to_vec()))
            .unwrap_or_default();
        let s2 = ctx
            .store
            .get(key2)
            .and_then(|v| v.as_string().map(|sv| sv.as_bytes().to_vec()))
            .unwrap_or_default();

        // Handle empty strings
        if s1.is_empty() || s2.is_empty() {
            if opts.idx {
                // Return empty matches structure
                let response = vec![
                    Response::bulk(Bytes::from_static(b"matches")),
                    Response::Array(vec![]),
                    Response::bulk(Bytes::from_static(b"len")),
                    Response::Integer(0),
                ];
                return Ok(Response::Array(response));
            } else if opts.len_only {
                return Ok(Response::Integer(0));
            } else {
                return Ok(Response::bulk(Bytes::new()));
            }
        }

        // Build DP table
        let m = s1.len();
        let n = s2.len();
        let mut dp = vec![vec![0usize; n + 1]; m + 1];

        for i in 1..=m {
            for j in 1..=n {
                if s1[i - 1] == s2[j - 1] {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                }
            }
        }

        let lcs_len = dp[m][n];

        // If only length is needed
        if opts.len_only {
            return Ok(Response::Integer(lcs_len as i64));
        }

        // If IDX mode - extract match positions
        if opts.idx {
            let matches = extract_lcs_matches(&s1, &s2, &dp, opts.min_match_len);

            // Build response
            let mut matches_arr = Vec::with_capacity(matches.len());
            for m in matches {
                let match_entry = if opts.with_match_len {
                    Response::Array(vec![
                        Response::Array(vec![
                            Response::Integer(m.a_range.0 as i64),
                            Response::Integer(m.a_range.1 as i64),
                        ]),
                        Response::Array(vec![
                            Response::Integer(m.b_range.0 as i64),
                            Response::Integer(m.b_range.1 as i64),
                        ]),
                        Response::Integer(m.len as i64),
                    ])
                } else {
                    Response::Array(vec![
                        Response::Array(vec![
                            Response::Integer(m.a_range.0 as i64),
                            Response::Integer(m.a_range.1 as i64),
                        ]),
                        Response::Array(vec![
                            Response::Integer(m.b_range.0 as i64),
                            Response::Integer(m.b_range.1 as i64),
                        ]),
                    ])
                };
                matches_arr.push(match_entry);
            }

            let response = vec![
                Response::bulk(Bytes::from_static(b"matches")),
                Response::Array(matches_arr),
                Response::bulk(Bytes::from_static(b"len")),
                Response::Integer(lcs_len as i64),
            ];
            return Ok(Response::Array(response));
        }

        // Default: return the LCS string via backtracking
        let mut lcs = Vec::with_capacity(lcs_len);
        let mut i = m;
        let mut j = n;
        while i > 0 && j > 0 {
            if s1[i - 1] == s2[j - 1] {
                lcs.push(s1[i - 1]);
                i -= 1;
                j -= 1;
            } else if dp[i - 1][j] > dp[i][j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
        lcs.reverse();

        Ok(Response::bulk(Bytes::from(lcs)))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            vec![&args[0], &args[1]]
        }
    }
}

/// Extract match ranges from the LCS DP table.
/// Returns matches in reverse order (from end to start).
fn extract_lcs_matches(s1: &[u8], s2: &[u8], dp: &[Vec<usize>], min_len: usize) -> Vec<LcsMatch> {
    let m = s1.len();
    let n = s2.len();

    let mut matches = Vec::new();
    let mut i = m;
    let mut j = n;

    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            // Found a matching character, trace back the contiguous match
            let match_end_i = i - 1;
            let match_end_j = j - 1;
            let mut match_len = 1;

            i -= 1;
            j -= 1;

            // Continue while characters match and we're still in the LCS path
            while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] && dp[i][j] == dp[i - 1][j - 1] + 1 {
                match_len += 1;
                i -= 1;
                j -= 1;
            }

            // Only include if meets minimum length
            if match_len >= min_len {
                matches.push(LcsMatch {
                    a_range: (i, match_end_i),
                    b_range: (j, match_end_j),
                    len: match_len,
                });
            }
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    matches
}

// ============================================================================
// GETSET - Set key to value and return old value (deprecated, use SET GET)
// ============================================================================

pub struct GetsetCommand;

impl Command for GetsetCommand {
    fn name(&self) -> &'static str {
        "GETSET"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let new_value = args[1].clone();

        // Get old value (must be a string if it exists)
        let old = match ctx.store.get(key) {
            Some(v) => {
                if let Some(sv) = v.as_string() {
                    Some(sv.as_bytes())
                } else {
                    return Err(CommandError::WrongType);
                }
            }
            None => None,
        };

        // Set new value (unconditionally, clears any TTL)
        ctx.store.set(key.clone(), Value::string(new_value));

        match old {
            Some(bytes) => Ok(Response::bulk(bytes)),
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
// SUBSTR - Deprecated alias for GETRANGE
// ============================================================================

pub struct SubstrCommand;

impl Command for SubstrCommand {
    fn name(&self) -> &'static str {
        "SUBSTR"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // SUBSTR key start end
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // SUBSTR is a deprecated alias for GETRANGE
        GetrangeCommand.execute(ctx, args)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
