//! Shared utilities for command implementations.
//!
//! This module provides common parsing functions and helpers used across
//! multiple command modules to avoid code duplication.

use bytes::Bytes;
use frogdb_core::{
    CommandContext, CommandError, HashValue, LexBound, ListValue, ScoreBound, SetValue,
    SortedSetValue, StreamTrimMode, StreamValue,
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

// ============================================================================
// Common Option Parsing Utilities
// ============================================================================

/// NX/XX mutually exclusive options (used by ZADD, SET, GEOADD, etc.).
///
/// - NX: Only perform the operation if the element does not already exist.
/// - XX: Only perform the operation if the element already exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NxXxOptions {
    /// Only add new elements (don't update existing).
    pub nx: bool,
    /// Only update existing elements (don't add new).
    pub xx: bool,
}

impl NxXxOptions {
    /// Parse NX/XX option from an argument.
    ///
    /// Returns `Some(updated_options)` if the argument was recognized,
    /// `None` if the argument is not NX or XX.
    ///
    /// Returns an error if NX and XX are both set.
    pub fn try_parse(&self, arg: &[u8]) -> Result<Option<Self>, CommandError> {
        let upper = arg.to_ascii_uppercase();
        match upper.as_slice() {
            b"NX" => {
                if self.xx {
                    return Err(CommandError::InvalidArgument {
                        message: "XX and NX options at the same time are not compatible".to_string(),
                    });
                }
                Ok(Some(Self { nx: true, ..*self }))
            }
            b"XX" => {
                if self.nx {
                    return Err(CommandError::InvalidArgument {
                        message: "XX and NX options at the same time are not compatible".to_string(),
                    });
                }
                Ok(Some(Self { xx: true, ..*self }))
            }
            _ => Ok(None),
        }
    }
}

/// GT/LT mutually exclusive options (used by ZADD, etc.).
///
/// - GT: Only update if new score is greater than current score.
/// - LT: Only update if new score is less than current score.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GtLtOptions {
    /// Only update if new score > current score.
    pub gt: bool,
    /// Only update if new score < current score.
    pub lt: bool,
}

impl GtLtOptions {
    /// Parse GT/LT option from an argument.
    ///
    /// Returns `Some(updated_options)` if the argument was recognized,
    /// `None` if the argument is not GT or LT.
    ///
    /// Returns an error if GT and LT are both set.
    pub fn try_parse(&self, arg: &[u8]) -> Result<Option<Self>, CommandError> {
        let upper = arg.to_ascii_uppercase();
        match upper.as_slice() {
            b"GT" => {
                if self.lt {
                    return Err(CommandError::InvalidArgument {
                        message: "GT and LT options at the same time are not compatible".to_string(),
                    });
                }
                Ok(Some(Self { gt: true, ..*self }))
            }
            b"LT" => {
                if self.gt {
                    return Err(CommandError::InvalidArgument {
                        message: "GT and LT options at the same time are not compatible".to_string(),
                    });
                }
                Ok(Some(Self { lt: true, ..*self }))
            }
            _ => Ok(None),
        }
    }
}

/// Combined options for ZADD command.
///
/// Parses: NX, XX, GT, LT, CH, INCR options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ZaddOptions {
    /// NX/XX options.
    pub nx_xx: NxXxOptions,
    /// GT/LT options.
    pub gt_lt: GtLtOptions,
    /// Return number of changed elements (not just added).
    pub ch: bool,
    /// Increment mode (ZINCRBY behavior).
    pub incr: bool,
}

impl ZaddOptions {
    /// Parse all ZADD options from arguments starting at given index.
    ///
    /// Returns `(options, next_index)` where `next_index` points to the first
    /// argument that is not an option (i.e., start of score-member pairs).
    pub fn parse(args: &[Bytes], mut i: usize) -> Result<(Self, usize), CommandError> {
        let mut opts = Self::default();

        while i < args.len() {
            let arg = args[i].as_ref();
            let upper = arg.to_ascii_uppercase();

            match upper.as_slice() {
                b"NX" | b"XX" => {
                    if let Some(new_nx_xx) = opts.nx_xx.try_parse(arg)? {
                        opts.nx_xx = new_nx_xx;
                    } else {
                        break;
                    }
                }
                b"GT" | b"LT" => {
                    if let Some(new_gt_lt) = opts.gt_lt.try_parse(arg)? {
                        opts.gt_lt = new_gt_lt;
                    } else {
                        break;
                    }
                }
                b"CH" => opts.ch = true,
                b"INCR" => opts.incr = true,
                _ => break, // Start of score-member pairs
            }
            i += 1;
        }

        // Check for GT/LT with NX - not compatible
        if opts.nx_xx.nx && (opts.gt_lt.gt || opts.gt_lt.lt) {
            return Err(CommandError::InvalidArgument {
                message: "GT, LT, and NX options at the same time are not compatible".to_string(),
            });
        }

        Ok((opts, i))
    }
}

// ============================================================================
// Stream Trim Option Parsing Utilities
// ============================================================================

/// Parse trim mode from `=` or `~` symbol.
///
/// Returns `(mode, consumed)` where `consumed` is true if a mode symbol was
/// found, false if using the default (exact).
pub fn parse_trim_mode(arg: &[u8]) -> (StreamTrimMode, bool) {
    match arg {
        b"=" => (StreamTrimMode::Exact, true),
        b"~" => (StreamTrimMode::Approximate, true),
        _ => (StreamTrimMode::Exact, false),
    }
}

/// Parse an optional LIMIT clause from arguments.
///
/// If `args[i]` is "LIMIT", parses the limit value from `args[i+1]` and
/// returns `(limit_value, next_index)`. Otherwise returns `(0, i)`.
pub fn parse_optional_limit(args: &[Bytes], i: usize) -> Result<(usize, usize), CommandError> {
    if i < args.len() && args[i].to_ascii_uppercase().as_slice() == b"LIMIT" {
        let next = i + 1;
        if next >= args.len() {
            return Err(CommandError::SyntaxError);
        }
        let limit = parse_usize(&args[next])?;
        Ok((limit, next + 1))
    } else {
        Ok((0, i))
    }
}
