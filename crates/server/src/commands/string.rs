//! String commands.
//!
//! Commands for string manipulation:
//! - SETNX, SETEX, PSETEX - SET variants
//! - APPEND, STRLEN - string operations
//! - GETRANGE, SETRANGE - substring operations
//! - GETDEL, GETEX - GET variants
//! - INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT - numeric operations

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, Expiry, IncrementError,
    SetCondition, SetOptions, SetResult, StringValue, Value,
};
use frogdb_protocol::Response;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Parse a string as i64.
fn parse_i64(arg: &[u8]) -> Result<i64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Parse a string as u64.
fn parse_u64(arg: &[u8]) -> Result<u64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Parse a string as f64.
fn parse_f64(arg: &[u8]) -> Result<f64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotFloat)
}

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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let seconds = parse_u64(&args[1])?;
        let value = args[2].clone();

        if seconds == 0 {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'setex' command".to_string(),
            });
        }

        let opts = SetOptions {
            expiry: Some(Expiry::Ex(seconds)),
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = args[0].clone();
        let ms = parse_u64(&args[1])?;
        let value = args[2].clone();

        if ms == 0 {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'psetex' command".to_string(),
            });
        }

        let opts = SetOptions {
            expiry: Some(Expiry::Px(ms)),
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
            // Key doesn't exist, create with padding
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
                    let seconds = parse_u64(&args[i])?;
                    let expires_at = Instant::now() + Duration::from_secs(seconds);
                    ctx.store.set_expiry(key, expires_at);
                }
                b"PX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ms = parse_u64(&args[i])?;
                    let expires_at = Instant::now() + Duration::from_millis(ms);
                    ctx.store.set_expiry(key, expires_at);
                }
                b"EXAT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let ts = parse_u64(&args[i])?;
                    let target = UNIX_EPOCH + Duration::from_secs(ts);
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
                    let ts = parse_u64(&args[i])?;
                    let target = UNIX_EPOCH + Duration::from_millis(ts);
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let delta = parse_i64(&args[1])?;

        // Decrement is just negative increment
        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment(-delta) {
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
            ctx.store
                .set(key.clone(), Value::String(StringValue::from_integer(-delta)));
            Ok(Response::Integer(-delta))
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let delta = parse_f64(&args[1])?;

        if delta.is_nan() || delta.is_infinite() {
            return Err(CommandError::NotFloat);
        }

        if let Some(existing) = ctx.store.get_mut(key) {
            if let Some(sv) = existing.as_string_mut() {
                match sv.increment_float(delta) {
                    Ok(new_val) => {
                        // Return the new value as bulk string
                        Ok(Response::bulk(Bytes::from(format_float(new_val))))
                    }
                    Err(IncrementError::NotFloat) => Err(CommandError::NotFloat),
                    Err(IncrementError::NotInteger) => Err(CommandError::NotFloat),
                    Err(IncrementError::Overflow) => Err(CommandError::NotFloat),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key doesn't exist, create with value delta
            let sv = StringValue::new(Bytes::from(format_float(delta)));
            ctx.store.set(key.clone(), Value::String(sv));
            Ok(Response::bulk(Bytes::from(format_float(delta))))
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

/// Format a float for Redis compatibility.
fn format_float(f: f64) -> String {
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
