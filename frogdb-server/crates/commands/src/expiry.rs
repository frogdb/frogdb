//! TTL/Expiry commands.
//!
//! Commands for managing key expiration:
//! - EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT - set expiration
//! - TTL, PTTL - get remaining time
//! - EXPIRETIME, PEXPIRETIME - get absolute expiry
//! - PERSIST - remove expiration

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, KeyspaceEventFlags, WalStrategy,
};
use frogdb_protocol::Response;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Helper to convert Unix timestamp (seconds) to Instant.
pub(crate) fn unix_secs_to_instant(ts: u64) -> Option<Instant> {
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
pub(crate) fn unix_ms_to_instant(ts: u64) -> Option<Instant> {
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
pub(crate) fn instant_to_unix_secs(instant: Instant) -> i64 {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    if instant > now_instant {
        let duration = instant.duration_since(now_instant);
        let target = now_system + duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| {
                let secs = d.as_secs() as i64;
                // Round to nearest second to handle Instant↔SystemTime jitter
                if d.subsec_nanos() >= 500_000_000 {
                    secs + 1
                } else {
                    secs
                }
            })
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
pub(crate) fn instant_to_unix_ms(instant: Instant) -> i64 {
    let now_instant = Instant::now();
    let now_system = SystemTime::now();

    if instant > now_instant {
        let duration = instant.duration_since(now_instant);
        let target = now_system + duration;
        target
            .duration_since(UNIX_EPOCH)
            .map(|d| {
                let ms = d.as_millis() as i64;
                // Round to nearest ms to handle Instant↔SystemTime jitter
                if d.subsec_nanos() % 1_000_000 >= 500_000 {
                    ms + 1
                } else {
                    ms
                }
            })
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

/// Parsed conditions for EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT subcommands.
/// Redis allows combining NX/XX with GT/LT (e.g., "EXPIRE key 100 GT XX").
pub(crate) struct ExpireConditions {
    /// NX: Set expiry only if key has no expiry.
    pub(crate) nx: bool,
    /// XX: Set expiry only if key already has expiry.
    pub(crate) xx: bool,
    /// GT: Set expiry only if new expiry > current expiry.
    pub(crate) gt: bool,
    /// LT: Set expiry only if new expiry < current expiry.
    pub(crate) lt: bool,
}

impl ExpireConditions {
    pub(crate) fn none() -> Self {
        Self {
            nx: false,
            xx: false,
            gt: false,
            lt: false,
        }
    }
}

/// Parse optional NX/XX/GT/LT subcommands from the argument slice.
/// Redis allows combined subcommands like "EXPIRE key 100 GT XX".
pub(crate) fn parse_expire_conditions(args: &[Bytes]) -> Result<ExpireConditions, CommandError> {
    let mut conditions = ExpireConditions::none();

    for arg in args.iter().skip(2) {
        let sub = arg.to_ascii_uppercase();
        match sub.as_slice() {
            b"NX" => conditions.nx = true,
            b"XX" => conditions.xx = true,
            b"GT" => conditions.gt = true,
            b"LT" => conditions.lt = true,
            _ => {
                let option_str = String::from_utf8_lossy(&sub);
                return Err(CommandError::InvalidArgument {
                    message: format!("Unsupported option {}", option_str),
                });
            }
        }
    }

    // NX and (XX|GT|LT) are mutually exclusive
    if conditions.nx && (conditions.xx || conditions.gt || conditions.lt) {
        return Err(CommandError::InvalidArgument {
            message: "NX and XX, GT or LT options at the same time are not compatible".to_string(),
        });
    }
    // GT and LT are mutually exclusive
    if conditions.gt && conditions.lt {
        return Err(CommandError::InvalidArgument {
            message: "GT and LT options at the same time are not compatible".to_string(),
        });
    }

    Ok(conditions)
}

/// Parse NX/XX/GT/LT conditions from an arbitrary slice of arguments.
/// Unlike `parse_expire_conditions`, this takes exactly the condition args.
pub(crate) fn parse_expire_conditions_from_slice(
    args: &[Bytes],
) -> Result<ExpireConditions, CommandError> {
    let mut conditions = ExpireConditions::none();

    for arg in args {
        let sub = arg.to_ascii_uppercase();
        match sub.as_slice() {
            b"NX" => conditions.nx = true,
            b"XX" => conditions.xx = true,
            b"GT" => conditions.gt = true,
            b"LT" => conditions.lt = true,
            _ => {
                let option_str = String::from_utf8_lossy(&sub);
                return Err(CommandError::InvalidArgument {
                    message: format!("Unsupported option {}", option_str),
                });
            }
        }
    }

    // NX and (XX|GT|LT) are mutually exclusive
    if conditions.nx && (conditions.xx || conditions.gt || conditions.lt) {
        return Err(CommandError::InvalidArgument {
            message: "NX and XX, GT or LT options at the same time are not compatible".to_string(),
        });
    }
    // GT and LT are mutually exclusive
    if conditions.gt && conditions.lt {
        return Err(CommandError::InvalidArgument {
            message: "GT and LT options at the same time are not compatible".to_string(),
        });
    }

    Ok(conditions)
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
        Arity::Range { min: 2, max: 4 } // EXPIRE key seconds [NX|XX|GT|LT] [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let seconds = parse_i64(&args[1])?;

        // Reject values outside i64 millisecond range
        if !(i64::MIN / 1000..=i64::MAX / 1000).contains(&seconds) {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'expire' command".to_string(),
            });
        }
        // Also reject if converting to ms and adding to current time would overflow
        let ms = seconds * 1000;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        if ms > i64::MAX - now_ms {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'expire' command".to_string(),
            });
        }

        let conditions = parse_expire_conditions(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        if conditions.nx && current_expiry.is_some() {
            return Ok(Response::Integer(0));
        }
        if conditions.xx && current_expiry.is_none() {
            return Ok(Response::Integer(0));
        }

        if seconds <= 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = Instant::now() + Duration::from_secs(seconds as u64);

        // Check GT/LT conditions
        // GT on key without TTL: return 0 (Redis behavior: GT requires existing TTL to compare)
        if conditions.gt {
            match current_expiry {
                Some(current) if expires_at <= current => return Ok(Response::Integer(0)),
                None => return Ok(Response::Integer(0)),
                _ => {}
            }
        }
        if conditions.lt
            && let Some(current) = current_expiry
            && expires_at >= current
        {
            return Ok(Response::Integer(0));
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        Some(KeyspaceEventFlags::GENERIC)
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
        Arity::Range { min: 2, max: 4 } // PEXPIRE key milliseconds [NX|XX|GT|LT] [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let ms = parse_i64(&args[1])?;

        // Reject if adding to current time would overflow
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        if ms > i64::MAX - now_ms {
            return Err(CommandError::InvalidArgument {
                message: "invalid expire time in 'pexpire' command".to_string(),
            });
        }

        let conditions = parse_expire_conditions(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        if conditions.nx && current_expiry.is_some() {
            return Ok(Response::Integer(0));
        }
        if conditions.xx && current_expiry.is_none() {
            return Ok(Response::Integer(0));
        }

        if ms <= 0 {
            let deleted = ctx.store.delete(key);
            return Ok(Response::Integer(if deleted { 1 } else { 0 }));
        }

        let expires_at = Instant::now() + Duration::from_millis(ms as u64);

        // Check GT/LT conditions
        if conditions.gt {
            match current_expiry {
                Some(current) if expires_at <= current => return Ok(Response::Integer(0)),
                None => return Ok(Response::Integer(0)),
                _ => {}
            }
        }
        if conditions.lt
            && let Some(current) = current_expiry
            && expires_at >= current
        {
            return Ok(Response::Integer(0));
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        Some(KeyspaceEventFlags::GENERIC)
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
        Arity::Range { min: 2, max: 4 } // EXPIREAT key timestamp [NX|XX|GT|LT] [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let timestamp = parse_i64(&args[1])?;

        let conditions = parse_expire_conditions(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        if conditions.nx && current_expiry.is_some() {
            return Ok(Response::Integer(0));
        }
        if conditions.xx && current_expiry.is_none() {
            return Ok(Response::Integer(0));
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
        if conditions.gt {
            match current_expiry {
                Some(current) if expires_at <= current => return Ok(Response::Integer(0)),
                None => return Ok(Response::Integer(0)),
                _ => {}
            }
        }
        if conditions.lt
            && let Some(current) = current_expiry
            && expires_at >= current
        {
            return Ok(Response::Integer(0));
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        Some(KeyspaceEventFlags::GENERIC)
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
        Arity::Range { min: 2, max: 4 } // PEXPIREAT key timestamp_ms [NX|XX|GT|LT] [NX|XX|GT|LT]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let timestamp_ms = parse_i64(&args[1])?;

        let conditions = parse_expire_conditions(args)?;

        // Check if key exists
        if !ctx.store.contains(key) {
            return Ok(Response::Integer(0));
        }

        let current_expiry = ctx.store.get_expiry(key);

        // Check NX/XX conditions
        if conditions.nx && current_expiry.is_some() {
            return Ok(Response::Integer(0));
        }
        if conditions.xx && current_expiry.is_none() {
            return Ok(Response::Integer(0));
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
        if conditions.gt {
            match current_expiry {
                Some(current) if expires_at <= current => return Ok(Response::Integer(0)),
                None => return Ok(Response::Integer(0)),
                _ => {}
            }
        }
        if conditions.lt
            && let Some(current) = current_expiry
            && expires_at >= current
        {
            return Ok(Response::Integer(0));
        }

        let result = ctx.store.set_expiry(key, expires_at);
        Ok(Response::Integer(if result { 1 } else { 0 }))
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        Some(KeyspaceEventFlags::GENERIC)
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

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let result = ctx.store.persist(key);
        Ok(Response::Integer(if result { 1 } else { 0 }))
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        Some(KeyspaceEventFlags::GENERIC)
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
