//! TTL/Expiry commands.
//!
//! Commands for managing key expiration:
//! - EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT - set expiration
//! - TTL, PTTL - get remaining time
//! - EXPIRETIME, PEXPIRETIME - get absolute expiry
//! - PERSIST - remove expiration

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Helper to convert Unix timestamp (seconds) to Instant.
fn unix_secs_to_instant(ts: u64) -> Option<Instant> {
    let target = UNIX_EPOCH + Duration::from_secs(ts);
    let now_system = SystemTime::now();
    let now_instant = Instant::now();

    if let Ok(duration) = target.duration_since(now_system) {
        Some(now_instant + duration)
    } else {
        // Already in the past
        Some(now_instant)
    }
}

/// Helper to convert Unix timestamp (milliseconds) to Instant.
fn unix_ms_to_instant(ts: u64) -> Option<Instant> {
    let target = UNIX_EPOCH + Duration::from_millis(ts);
    let now_system = SystemTime::now();
    let now_instant = Instant::now();

    if let Ok(duration) = target.duration_since(now_system) {
        Some(now_instant + duration)
    } else {
        // Already in the past
        Some(now_instant)
    }
}

/// Helper to convert Instant to Unix timestamp (seconds).
fn instant_to_unix_secs(instant: Instant) -> i64 {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    if instant > now_instant {
        let duration = instant.duration_since(now_instant);
        let target = now_system + duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(-1)
    } else {
        // Already expired
        let duration = now_instant.duration_since(instant);
        let target = now_system - duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(-1)
    }
}

/// Helper to convert Instant to Unix timestamp (milliseconds).
fn instant_to_unix_ms(instant: Instant) -> i64 {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    if instant > now_instant {
        let duration = instant.duration_since(now_instant);
        let target = now_system + duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(-1)
    } else {
        // Already expired
        let duration = now_instant.duration_since(instant);
        let target = now_system - duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(-1)
    }
}

use super::utils::parse_i64;

/// Condition for EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT subcommands.
enum ExpireCondition {
    /// NX: Set expiry only if key has no expiry.
    Nx,
    /// XX: Set expiry only if key already has expiry.
    Xx,
    /// GT: Set expiry only if new expiry > current expiry.
    Gt,
    /// LT: Set expiry only if new expiry < current expiry.
    Lt,
}

/// Parse an optional NX/XX/GT/LT subcommand from the argument slice.
fn parse_expire_condition(args: &[Bytes]) -> Result<Option<ExpireCondition>, CommandError> {
    if args.len() > 2 {
        let sub = args[2].to_ascii_uppercase();
        match sub.as_slice() {
            b"NX" => Ok(Some(ExpireCondition::Nx)),
            b"XX" => Ok(Some(ExpireCondition::Xx)),
            b"GT" => Ok(Some(ExpireCondition::Gt)),
            b"LT" => Ok(Some(ExpireCondition::Lt)),
            _ => Err(CommandError::SyntaxError),
        }
    } else {
        Ok(None)
    }
}

// ============================================================================
// EXPIRE - Set key expiration in seconds
// ============================================================================

pub struct ExpireCommand;

impl Command for ExpireCommand {
    fn name(&self) -> &'static str {
        "EXPIRE"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 3 } // EXPIRE key seconds [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let seconds = parse_i64(&args[1])?;

        // Reject values that would overflow when added to current time
        if seconds > i64::MAX / 1000 {
            return Err(CommandError::NotInteger);
        }

        let subcommand = parse_expire_condition(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        match &subcommand {
            Some(ExpireCondition::Nx) if current_expiry.is_some() => {
                return Ok(Response::Integer(0));
            }
            Some(ExpireCondition::Xx) if current_expiry.is_none() => {
                return Ok(Response::Integer(0));
            }
            _ => {}
        }

        if seconds <= 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = Instant::now() + Duration::from_secs(seconds as u64);

        // Check GT/LT conditions
        if let Some(current) = current_expiry {
            match &subcommand {
                Some(ExpireCondition::Gt) if expires_at <= current => {
                    return Ok(Response::Integer(0));
                }
                Some(ExpireCondition::Lt) if expires_at >= current => {
                    return Ok(Response::Integer(0));
                }
                _ => {}
            }
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
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
// PEXPIRE - Set key expiration in milliseconds
// ============================================================================

pub struct PexpireCommand;

impl Command for PexpireCommand {
    fn name(&self) -> &'static str {
        "PEXPIRE"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 3 } // PEXPIRE key milliseconds [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let ms = parse_i64(&args[1])?;

        // Reject values that would overflow when added to current time
        if ms > i64::MAX / 1000 {
            return Err(CommandError::NotInteger);
        }

        let subcommand = parse_expire_condition(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        match &subcommand {
            Some(ExpireCondition::Nx) if current_expiry.is_some() => {
                return Ok(Response::Integer(0));
            }
            Some(ExpireCondition::Xx) if current_expiry.is_none() => {
                return Ok(Response::Integer(0));
            }
            _ => {}
        }

        if ms <= 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = Instant::now() + Duration::from_millis(ms as u64);

        // Check GT/LT conditions
        if let Some(current) = current_expiry {
            match &subcommand {
                Some(ExpireCondition::Gt) if expires_at <= current => {
                    return Ok(Response::Integer(0));
                }
                Some(ExpireCondition::Lt) if expires_at >= current => {
                    return Ok(Response::Integer(0));
                }
                _ => {}
            }
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
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
// EXPIREAT - Set key expiration at Unix timestamp (seconds)
// ============================================================================

pub struct ExpireatCommand;

impl Command for ExpireatCommand {
    fn name(&self) -> &'static str {
        "EXPIREAT"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 3 } // EXPIREAT key timestamp [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let timestamp = parse_i64(&args[1])?;

        let subcommand = parse_expire_condition(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        match &subcommand {
            Some(ExpireCondition::Nx) if current_expiry.is_some() => {
                return Ok(Response::Integer(0));
            }
            Some(ExpireCondition::Xx) if current_expiry.is_none() => {
                return Ok(Response::Integer(0));
            }
            _ => {}
        }

        // Negative timestamps mean already expired — delete the key
        if timestamp < 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = unix_secs_to_instant(timestamp as u64).ok_or(CommandError::NotInteger)?;

        // If already expired, delete the key
        if expires_at <= Instant::now() {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        // Check GT/LT conditions
        if let Some(current) = current_expiry {
            match &subcommand {
                Some(ExpireCondition::Gt) if expires_at <= current => {
                    return Ok(Response::Integer(0));
                }
                Some(ExpireCondition::Lt) if expires_at >= current => {
                    return Ok(Response::Integer(0));
                }
                _ => {}
            }
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
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
// PEXPIREAT - Set key expiration at Unix timestamp (milliseconds)
// ============================================================================

pub struct PexpireatCommand;

impl Command for PexpireatCommand {
    fn name(&self) -> &'static str {
        "PEXPIREAT"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 3 } // PEXPIREAT key timestamp_ms [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let timestamp_ms = parse_i64(&args[1])?;

        let subcommand = parse_expire_condition(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        match &subcommand {
            Some(ExpireCondition::Nx) if current_expiry.is_some() => {
                return Ok(Response::Integer(0));
            }
            Some(ExpireCondition::Xx) if current_expiry.is_none() => {
                return Ok(Response::Integer(0));
            }
            _ => {}
        }

        // Negative timestamps mean already expired — delete the key
        if timestamp_ms < 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = unix_ms_to_instant(timestamp_ms as u64).ok_or(CommandError::NotInteger)?;

        // If already expired, delete the key
        if expires_at <= Instant::now() {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        // Check GT/LT conditions
        if let Some(current) = current_expiry {
            match &subcommand {
                Some(ExpireCondition::Gt) if expires_at <= current => {
                    return Ok(Response::Integer(0));
                }
                Some(ExpireCondition::Lt) if expires_at >= current => {
                    return Ok(Response::Integer(0));
                }
                _ => {}
            }
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
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
// TTL - Get remaining time to live in seconds
// ============================================================================

pub struct TtlCommand;

impl Command for TtlCommand {
    fn name(&self) -> &'static str {
        "TTL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(-2)); // Key does not exist
        }

        match ctx.store.get_expiry(key) {
            Some(expires_at) => {
                let now = Instant::now();
                if expires_at <= now {
                    // Already expired (lazy expiry will clean it up)
                    Ok(Response::Integer(-2))
                } else {
                    let remaining = expires_at.duration_since(now);
                    // Use ceiling division: any sub-second remainder rounds up
                    let secs = remaining.as_secs() as i64;
                    let subsec = remaining.subsec_nanos();
                    Ok(Response::Integer(if subsec > 0 { secs + 1 } else { secs }))
                }
            }
            None => Ok(Response::Integer(-1)), // Key exists but has no expiry
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
// PTTL - Get remaining time to live in milliseconds
// ============================================================================

pub struct PttlCommand;

impl Command for PttlCommand {
    fn name(&self) -> &'static str {
        "PTTL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(-2)); // Key does not exist
        }

        match ctx.store.get_expiry(key) {
            Some(expires_at) => {
                let now = Instant::now();
                if expires_at <= now {
                    Ok(Response::Integer(-2))
                } else {
                    let remaining = expires_at.duration_since(now);
                    Ok(Response::Integer(remaining.as_millis() as i64))
                }
            }
            None => Ok(Response::Integer(-1)), // Key exists but has no expiry
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
// PERSIST - Remove expiration from a key
// ============================================================================

pub struct PersistCommand;

impl Command for PersistCommand {
    fn name(&self) -> &'static str {
        "PERSIST"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let result = ctx.store.persist(key);
        Ok(Response::Integer(if result { 1 } else { 0 }))
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
// EXPIRETIME - Get absolute Unix expiration timestamp (seconds)
// ============================================================================

pub struct ExpiretimeCommand;

impl Command for ExpiretimeCommand {
    fn name(&self) -> &'static str {
        "EXPIRETIME"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(-2)); // Key does not exist
        }

        match ctx.store.get_expiry(key) {
            Some(expires_at) => {
                let unix_ts = instant_to_unix_secs(expires_at);
                Ok(Response::Integer(unix_ts))
            }
            None => Ok(Response::Integer(-1)), // Key exists but has no expiry
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
// PEXPIRETIME - Get absolute Unix expiration timestamp (milliseconds)
// ============================================================================

pub struct PexpiretimeCommand;

impl Command for PexpiretimeCommand {
    fn name(&self) -> &'static str {
        "PEXPIRETIME"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(-2)); // Key does not exist
        }

        match ctx.store.get_expiry(key) {
            Some(expires_at) => {
                let unix_ts = instant_to_unix_ms(expires_at);
                Ok(Response::Integer(unix_ts))
            }
            None => Ok(Response::Integer(-1)), // Key exists but has no expiry
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
