//! Shared utilities for command implementations.
//!
//! This module provides common parsing functions and helpers used across
//! multiple command modules to avoid code duplication.

use bytes::Bytes;
use frogdb_core::{
    CommandContext, CommandError, HashValue, LexBound, ListValue, ScoreBound, SetValue,
    SortedSetValue, StreamValue,
};

// ============================================================================
// Parsing Utilities - Re-exported from frogdb_core for backwards compatibility
// ============================================================================

// Re-export generic parsing functions from core.
// These now accept any type implementing AsRef<[u8]>, including &[u8] and &Bytes.
pub use frogdb_core::{parse_f64, parse_i64, parse_u64, parse_usize};

// Re-export the generic get_or_create helper from core.
pub use frogdb_core::get_or_create;

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
// Get or Create Helpers - Thin wrappers around generic get_or_create
// ============================================================================

/// Get or create a list, returning an error if the key exists but is wrong type.
///
/// This is a convenience wrapper around `get_or_create::<ListValue>()`.
#[inline]
pub fn get_or_create_list<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut ListValue, CommandError> {
    get_or_create::<ListValue>(ctx, key)
}

/// Get or create a set, returning an error if the key exists but is wrong type.
///
/// This is a convenience wrapper around `get_or_create::<SetValue>()`.
#[inline]
pub fn get_or_create_set<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut SetValue, CommandError> {
    get_or_create::<SetValue>(ctx, key)
}

/// Get or create a hash, returning an error if the key exists but is wrong type.
///
/// This is a convenience wrapper around `get_or_create::<HashValue>()`.
#[inline]
pub fn get_or_create_hash<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut HashValue, CommandError> {
    get_or_create::<HashValue>(ctx, key)
}

/// Get or create a sorted set, returning an error if the key exists but is wrong type.
///
/// This is a convenience wrapper around `get_or_create::<SortedSetValue>()`.
#[inline]
pub fn get_or_create_zset<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut SortedSetValue, CommandError> {
    get_or_create::<SortedSetValue>(ctx, key)
}

/// Get or create a stream, returning an error if the key exists but is wrong type.
///
/// This is a convenience wrapper around `get_or_create::<StreamValue>()`.
#[inline]
pub fn get_or_create_stream<'a>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut StreamValue, CommandError> {
    get_or_create::<StreamValue>(ctx, key)
}
