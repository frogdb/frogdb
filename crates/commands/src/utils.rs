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
/// Uses `ryu` for accurate, round-trip-safe float-to-string conversion,
/// matching Redis's `ld2string` behavior for extreme values (very small
/// or very large numbers that need scientific notation).
///
/// Handles special cases:
/// - Infinity values are formatted as "inf" or "-inf"
/// - Zero is formatted as "0"
/// - Integers (within safe range) are formatted without decimal point
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

    // Use ryu for accurate round-trip representation.
    // ryu produces minimal-length representations that round-trip correctly.
    let mut buf = ryu::Buffer::new();
    let s = buf.format(f);

    // ryu may produce "1e100" or "1.5e-10" style output — Redis uses similar
    // notation but formats it like "1e100" (no leading zero in exponent).
    // ryu already matches this convention, so we can return as-is.
    s.to_string()
}

/// Format a float for increment commands (HINCRBYFLOAT, INCRBYFLOAT).
///
/// Like `format_float` but always includes a decimal point for whole numbers
/// (e.g., "17179869185.0" instead of "17179869185"), matching Redis's
/// `ld2string` behavior for increment results.
pub fn format_float_incr(f: f64) -> String {
    let s = format_float(f);
    // If the result has no decimal point and no exponent notation, add ".0"
    if !s.contains('.') && !s.contains('e') && !s.contains('E') && s != "inf" && s != "-inf" {
        format!("{}.0", s)
    } else {
        s
    }
}

// ============================================================================
// Glob Pattern Matching
// ============================================================================

/// Simple glob pattern matching (supports * and ?).
///
/// Used by SCAN, SSCAN, HSCAN, ZSCAN MATCH filters.
pub fn simple_glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut star_t = None;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_p = Some(p);
            star_t = Some(t);
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t = Some(star_t.unwrap() + 1);
            t = star_t.unwrap();
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

// ============================================================================
// Hash-based scan cursor for SSCAN/HSCAN
// ============================================================================

/// Compute a deterministic hash for a byte sequence using SipHash with fixed seed.
///
/// This provides a stable ordering for scan cursors that is not affected by
/// hash table resizing or element insertion/removal order.
fn scan_hash(key: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    key.hash(&mut hasher);
    let h = hasher.finish();
    // Reserve 0 as "scan complete" marker — remap hash 0 to 1
    if h == 0 { 1 } else { h }
}

/// Perform a hash-based cursor scan over an iterator of items.
///
/// Returns `(new_cursor, results)` where `new_cursor` is 0 when the scan is complete.
///
/// The `hash_key` function extracts the bytes used for hash-based ordering from each item.
/// The `emit` function converts an item into Response values appended to `results`.
/// `count` is the maximum number of items to emit (not response elements).
pub fn hash_cursor_scan<T>(
    items: impl Iterator<Item = T>,
    cursor: u64,
    count: usize,
    match_pattern: Option<&[u8]>,
    hash_key: impl Fn(&T) -> &[u8],
    emit: impl Fn(T, &mut Vec<frogdb_protocol::Response>),
) -> (u64, Vec<frogdb_protocol::Response>) {
    // Collect items with their hashes
    let mut hashed: Vec<(u64, T)> = items
        .map(|item| (scan_hash(hash_key(&item)), item))
        .collect();

    // Sort by hash for deterministic ordering
    hashed.sort_unstable_by_key(|(h, _)| *h);

    // Find starting position: first item with hash >= cursor (cursor 0 starts from beginning)
    let start = if cursor == 0 {
        0
    } else {
        hashed.partition_point(|(h, _)| *h < cursor)
    };

    let mut results = Vec::new();
    let mut emitted = 0usize;
    let mut new_cursor = 0u64;

    for (hash, item) in hashed.into_iter().skip(start) {
        if emitted >= count {
            new_cursor = hash;
            break;
        }

        if let Some(pattern) = match_pattern
            && !simple_glob_match(pattern, hash_key(&item))
        {
            continue;
        }

        emit(item, &mut results);
        emitted += 1;
    }

    (new_cursor, results)
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
        if value.is_nan() {
            return Err(CommandError::NotFloat);
        }
        Ok(ScoreBound::Exclusive(value))
    } else {
        // Inclusive bound
        let value: f64 = s.parse().map_err(|_| CommandError::NotFloat)?;
        if value.is_nan() {
            return Err(CommandError::NotFloat);
        }
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
                        message: "XX and NX options at the same time are not compatible"
                            .to_string(),
                    });
                }
                Ok(Some(Self { nx: true, ..*self }))
            }
            b"XX" => {
                if self.nx {
                    return Err(CommandError::InvalidArgument {
                        message: "XX and NX options at the same time are not compatible"
                            .to_string(),
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
                        message: "GT and LT options at the same time are not compatible"
                            .to_string(),
                    });
                }
                Ok(Some(Self { gt: true, ..*self }))
            }
            b"LT" => {
                if self.gt {
                    return Err(CommandError::InvalidArgument {
                        message: "GT and LT options at the same time are not compatible"
                            .to_string(),
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

// ============================================================================
// Sorted Set Range Utilities
// ============================================================================

/// Limit options for range commands (ZRANGE, ZRANGEBYSCORE, etc.).
///
/// This represents the `LIMIT offset count` clause where:
/// - `offset` is the number of elements to skip
/// - `count` is the maximum number of elements to return (None = unlimited)
#[derive(Debug, Clone, Copy, Default)]
pub struct LimitOptions {
    /// Number of elements to skip.
    pub offset: usize,
    /// Maximum number of elements to return (None = unlimited).
    pub count: Option<usize>,
}

impl LimitOptions {
    /// Parse LIMIT offset count from arguments.
    ///
    /// Returns `(options, args_consumed)` where `args_consumed` is 2 (LIMIT is already consumed).
    /// A count of -1 means unlimited (represented as None).
    ///
    /// # Arguments
    ///
    /// * `args` - The remaining arguments after LIMIT keyword
    /// * `offset_idx` - Index of the offset argument
    pub fn parse(args: &[Bytes], offset_idx: usize) -> Result<Self, CommandError> {
        if offset_idx + 1 >= args.len() {
            return Err(CommandError::SyntaxError);
        }

        let offset = parse_usize(&args[offset_idx])?;
        let count = parse_i64(&args[offset_idx + 1])?;

        let count = if count < 0 {
            None // -1 means no limit
        } else {
            Some(count as usize)
        };

        Ok(Self { offset, count })
    }
}

/// Options for sorted set range commands.
///
/// This struct consolidates common options used by ZRANGE, ZRANGEBYSCORE,
/// ZRANGEBYLEX, and similar commands.
#[derive(Debug, Clone, Default)]
pub struct RangeOptions {
    /// LIMIT offset count options.
    pub limit: LimitOptions,
    /// Whether to include scores in the response.
    pub with_scores: bool,
    /// Whether to reverse the order (for REV option).
    pub rev: bool,
}

impl RangeOptions {
    /// Check if WITHSCORES is specified.
    #[inline]
    pub fn with_scores(&self) -> bool {
        self.with_scores
    }

    /// Get the offset.
    #[inline]
    pub fn offset(&self) -> usize {
        self.limit.offset
    }

    /// Get the count (None means unlimited).
    #[inline]
    pub fn count(&self) -> Option<usize> {
        self.limit.count
    }
}

use frogdb_protocol::Response;

/// Return a score as the appropriate Response type.
///
/// In RESP3 mode, finite scores are returned as `Response::Double` so that
/// clients receive the RESP3 Double wire type.  Infinity/NaN values are
/// always returned as bulk strings (`"inf"`, `"-inf"`) to avoid client-side
/// formatting differences (e.g., Tcl displays float infinity as `Inf`).
#[inline]
pub fn score_response(score: f64, is_resp3: bool) -> Response {
    if is_resp3 && score.is_finite() {
        Response::Double(score)
    } else {
        Response::bulk(Bytes::from(format_float(score)))
    }
}

/// Format a sorted set response with optional scores (RESP2 flat format).
///
/// When `with_scores` is true, returns an array with alternating member/score pairs.
/// When `with_scores` is false, returns just the members.
/// Scores are always returned as bulk strings.
///
/// # Example
///
/// ```rust,ignore
/// let results = zset.range_by_score(&min, &max, 0, None);
/// Ok(scored_array(results, with_scores))
/// ```
#[inline]
pub fn scored_array(members: Vec<(Bytes, f64)>, with_scores: bool) -> Response {
    if with_scores {
        Response::Array(
            members
                .into_iter()
                .flat_map(|(member, score)| {
                    [
                        Response::bulk(member),
                        Response::bulk(Bytes::from(format_float(score))),
                    ]
                })
                .collect(),
        )
    } else {
        Response::Array(
            members
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect(),
        )
    }
}

/// Format a sorted set response with optional scores, using RESP3 Double for scores.
///
/// Same flat format as `scored_array`, but uses `score_response` for RESP3-aware
/// score formatting (Double for finite values, bulk string for infinity).
/// Used by pop commands (ZPOPMIN, ZPOPMAX, ZMPOP).
#[inline]
pub fn scored_array_with_scores_resp3(
    members: Vec<(Bytes, f64)>,
    with_scores: bool,
    is_resp3: bool,
) -> Response {
    if with_scores {
        Response::Array(
            members
                .into_iter()
                .flat_map(|(member, score)| {
                    [Response::bulk(member), score_response(score, is_resp3)]
                })
                .collect(),
        )
    } else {
        Response::Array(
            members
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect(),
        )
    }
}

/// Format a sorted set response with optional scores (RESP3 nested pair format).
///
/// In RESP3, WITHSCORES responses use an array of `[member, Double(score)]` pairs.
/// When `with_scores` is false, returns just the members (same as `scored_array`).
#[inline]
pub fn scored_array_resp3(members: Vec<(Bytes, f64)>, with_scores: bool) -> Response {
    if with_scores {
        Response::Array(
            members
                .into_iter()
                .map(|(member, score)| {
                    Response::Array(vec![Response::bulk(member), score_response(score, true)])
                })
                .collect(),
        )
    } else {
        Response::Array(
            members
                .into_iter()
                .map(|(member, _)| Response::bulk(member))
                .collect(),
        )
    }
}

/// Format a sorted set response with optional scores (alias for compatibility).
///
/// Deprecated: Use `scored_array` instead.
#[inline]
pub fn format_scored_response(members: Vec<(Bytes, f64)>, with_scores: bool) -> Response {
    scored_array(members, with_scores)
}

/// Format a pop response (always includes scores).
///
/// Used by ZPOPMIN, ZPOPMAX, ZMPOP - these always return member/score pairs.
#[inline]
pub fn pop_response(members: Vec<(Bytes, f64)>) -> Response {
    scored_array(members, true)
}

/// Members-only array response (no scores).
///
/// Used by lexicographic ranges and other member-only responses.
#[inline]
pub fn members_array(members: Vec<(Bytes, f64)>) -> Response {
    scored_array(members, false)
}

// ============================================================================
// Routing Utilities
// ============================================================================

/// Check that all keys hash to the same shard, returning an error if not.
#[inline]
pub fn require_same_shard(keys: &[Bytes], num_shards: usize) -> Result<(), CommandError> {
    use frogdb_core::shard_for_key;

    if keys.len() < 2 {
        return Ok(());
    }

    let first_shard = shard_for_key(&keys[0], num_shards);
    for key in &keys[1..] {
        if shard_for_key(key, num_shards) != first_shard {
            return Err(CommandError::CrossSlot);
        }
    }
    Ok(())
}
