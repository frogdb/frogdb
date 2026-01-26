//! Shared utilities for command implementations.
//!
//! This module provides common parsing functions and helpers used across
//! multiple command modules to avoid code duplication.

use bytes::Bytes;
use frogdb_core::{
    CommandContext, CommandError, HashValue, LexBound, ListValue, ScoreBound, SetValue,
    SortedSetValue, StreamValue, Value,
};

// ============================================================================
// Parsing Utilities
// ============================================================================

/// Parse a byte slice as i64.
pub fn parse_i64(arg: &[u8]) -> Result<i64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Parse a byte slice as u64.
pub fn parse_u64(arg: &[u8]) -> Result<u64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Parse a byte slice as f64.
///
/// Supports special values:
/// - "inf" or "+inf" -> f64::INFINITY
/// - "-inf" -> f64::NEG_INFINITY
pub fn parse_f64(arg: &[u8]) -> Result<f64, CommandError> {
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

/// Parse a byte slice as usize.
pub fn parse_usize(arg: &[u8]) -> Result<usize, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

/// Format a float for Redis compatibility.
///
/// Handles special cases:
/// - Infinity values are formatted as "inf" or "-inf"
/// - Integers are formatted without decimal point
/// - Trailing zeros are removed
pub fn format_float(f: f64) -> String {
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

// ============================================================================
// Sorted Set Parsing Utilities
// ============================================================================

/// Parse a score bound (for ZRANGEBYSCORE, ZCOUNT, etc.).
pub fn parse_score_bound(arg: &[u8]) -> Result<ScoreBound, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::SyntaxError)?;

    if s.eq_ignore_ascii_case("-inf") {
        return Ok(ScoreBound::NegInf);
    }
    if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        return Ok(ScoreBound::PosInf);
    }

    if let Some(rest) = s.strip_prefix('(') {
        // Exclusive bound
        let value: f64 = rest.parse().map_err(|_| CommandError::NotFloat)?;
        Ok(ScoreBound::Exclusive(value))
    } else {
        // Inclusive bound
        let value: f64 = s.parse().map_err(|_| CommandError::NotFloat)?;
        Ok(ScoreBound::Inclusive(value))
    }
}

/// Parse a lex bound (for ZRANGEBYLEX, ZLEXCOUNT, etc.).
pub fn parse_lex_bound(arg: &[u8]) -> Result<LexBound, CommandError> {
    if arg.is_empty() {
        return Err(CommandError::SyntaxError);
    }

    match arg[0] {
        b'-' if arg.len() == 1 => Ok(LexBound::Min),
        b'+' if arg.len() == 1 => Ok(LexBound::Max),
        b'[' => Ok(LexBound::Inclusive(Bytes::copy_from_slice(&arg[1..]))),
        b'(' => Ok(LexBound::Exclusive(Bytes::copy_from_slice(&arg[1..]))),
        _ => Err(CommandError::InvalidArgument {
            message: "min or max not valid string range item".to_string(),
        }),
    }
}

// ============================================================================
// Get or Create Helpers
// ============================================================================

/// Get or create a list, returning an error if the key exists but is wrong type.
pub fn get_or_create_list<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut ListValue, CommandError> {
    // Check if key exists and is wrong type
    if let Some(value) = ctx.store.get(key) {
        if value.as_list().is_none() {
            return Err(CommandError::WrongType);
        }
    } else {
        // Create new list
        ctx.store.set(key.clone(), Value::list());
    }

    // Now get mutable reference
    ctx.store
        .get_mut(key)
        .and_then(|v| v.as_list_mut())
        .ok_or(CommandError::WrongType)
}

/// Get or create a set, returning an error if the key exists but is wrong type.
pub fn get_or_create_set<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut SetValue, CommandError> {
    // Check if key exists and is wrong type
    if let Some(value) = ctx.store.get(key) {
        if value.as_set().is_none() {
            return Err(CommandError::WrongType);
        }
    } else {
        // Create new set
        ctx.store.set(key.clone(), Value::set());
    }

    // Now get mutable reference
    ctx.store
        .get_mut(key)
        .and_then(|v| v.as_set_mut())
        .ok_or(CommandError::WrongType)
}

/// Get or create a hash, returning an error if the key exists but is wrong type.
pub fn get_or_create_hash<'a>(
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

/// Get or create a sorted set, returning an error if the key exists but is wrong type.
pub fn get_or_create_zset<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut SortedSetValue, CommandError> {
    // Check if key exists and is wrong type
    if let Some(value) = ctx.store.get(key) {
        if value.as_sorted_set().is_none() {
            return Err(CommandError::WrongType);
        }
    } else {
        // Create new sorted set
        ctx.store.set(key.clone(), Value::sorted_set());
    }

    // Now get mutable reference
    ctx.store
        .get_mut(key)
        .and_then(|v| v.as_sorted_set_mut())
        .ok_or(CommandError::WrongType)
}

/// Get or create a stream, returning an error if the key exists but is wrong type.
pub fn get_or_create_stream<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut StreamValue, CommandError> {
    // Check if key exists and is wrong type
    if let Some(value) = ctx.store.get(key) {
        if value.as_stream().is_none() {
            return Err(CommandError::WrongType);
        }
    } else {
        // Create new stream
        ctx.store.set(key.clone(), Value::stream());
    }

    // Now get mutable reference
    ctx.store
        .get_mut(key)
        .and_then(|v| v.as_stream_mut())
        .ok_or(CommandError::WrongType)
}
