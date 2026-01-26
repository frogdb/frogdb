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

use super::utils::{parse_i64, parse_u64};

// ============================================================================
// EXPIRE - Set key expiration in seconds
// ============================================================================

pub struct ExpireCommand;

impl Command for ExpireCommand {
    fn name(&self) -> &'static str {
        "EXPIRE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // EXPIRE key seconds
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
        let seconds = parse_i64(&args[1])?;

        if seconds <= 0 {
            // Non-positive expiry means delete the key
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = Instant::now() + Duration::from_secs(seconds as u64);
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
        Arity::Fixed(2) // PEXPIRE key milliseconds
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
        let ms = parse_i64(&args[1])?;

        if ms <= 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = Instant::now() + Duration::from_millis(ms as u64);
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
        Arity::Fixed(2) // EXPIREAT key timestamp
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
        let timestamp = parse_u64(&args[1])?;

        let expires_at = unix_secs_to_instant(timestamp).ok_or(CommandError::NotInteger)?;

        // If already expired, delete the key
        if expires_at <= Instant::now() {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
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
        Arity::Fixed(2) // PEXPIREAT key timestamp_ms
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
        let timestamp_ms = parse_u64(&args[1])?;

        let expires_at = unix_ms_to_instant(timestamp_ms).ok_or(CommandError::NotInteger)?;

        // If already expired, delete the key
        if expires_at <= Instant::now() {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
                    Ok(Response::Integer(remaining.as_secs() as i64))
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
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
