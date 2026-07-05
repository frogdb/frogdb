//! Shared utilities for command implementations.
//!
//! This module provides common parsing functions and helpers used across
//! multiple command modules to avoid code duplication.

use bytes::Bytes;
use frogdb_core::{ArgParser, CommandError, IncrementError, LexBound, ScoreBound, StreamTrimMode};
use frogdb_protocol::Response;

// ============================================================================
// Parsing Utilities - Re-exported from frogdb_core for backwards compatibility
// ============================================================================

// Re-export generic parsing functions from core.
// These now accept any type implementing AsRef<[u8]>, including &[u8] and &Bytes.
pub use frogdb_core::{parse_f64, parse_i64, parse_u64, parse_usize};

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

    // Redis uses %.17g which uses decimal notation for exponents < 17 (i.e., values < 1e17).
    // For integer-valued floats within f64's exact integer range (up to 2^53 ≈ 9e15),
    // return as plain integer string matching Redis's behavior.
    if f.fract() == 0.0 && f.abs() < 1e17 {
        return format!("{:.0}", f);
    }

    // Use ryu for accurate round-trip representation.
    // ryu produces minimal-length representations that round-trip correctly.
    let mut buf = ryu::Buffer::new();
    let s = buf.format(f);

    // Redis uses C's %.17g format which includes "e+308" (with explicit '+' sign
    // for positive exponents). ryu produces "e308" (no '+'), so we normalize here.
    if let Some(e_pos) = s.find('e') {
        let after_e = &s[e_pos + 1..];
        if !after_e.starts_with('-') && !after_e.starts_with('+') {
            return format!("{}e+{}", &s[..e_pos], after_e);
        }
    }
    s.to_string()
}

// ============================================================================
// Increment Utilities
// ============================================================================

/// Reject a non-finite increment operand up front, shared by INCRBYFLOAT and
/// HINCRBYFLOAT.
///
/// A NaN or infinite delta can never produce a finite result (current + delta
/// is NaN/infinite whenever delta is), so there's no need to touch the store
/// to discover that — this mirrors the final NaN/Infinity check the typed
/// `increment_float`/`incr_by_float` methods perform on the *result*, using
/// the same canonical message via [`IncrementError::NotFinite`].
///
/// Not used by ZINCRBY: sorted-set scores may legitimately be infinite, so
/// ZINCRBY only rejects a NaN delta (via `CommandError::NotFloat`, matching
/// Redis's `getDoubleFromObjectOrReply` parse-time check) and otherwise
/// relies on `SortedSetValue::incr`'s own NaN-result check.
pub fn reject_non_finite_delta(delta: f64) -> Result<(), CommandError> {
    if delta.is_nan() || delta.is_infinite() {
        return Err(IncrementError::NotFinite.into());
    }
    Ok(())
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
// SCAN family: shared request parsing and reply shaping
// ============================================================================
//
// SCAN, HSCAN, SSCAN, and ZSCAN all parse `cursor [MATCH pattern] [COUNT
// count]` (plus one command-specific flag: SCAN's TYPE, HSCAN's NOVALUES) and,
// for the per-key variants (HSCAN/SSCAN/ZSCAN), share the same reply shape:
// look up the key, reply with the empty envelope `[bulk "0", []]` if it's
// missing, otherwise drive `hash_cursor_scan` and wrap the result as `[cursor,
// [items...]]`. [`ScanRequest::parse`] and [`scan_reply`] capture that
// skeleton so each command only supplies its key lookup and item projection.

/// Parsed `cursor [MATCH pattern] [COUNT count]` arguments shared by the whole
/// SCAN family (SCAN, HSCAN, SSCAN, ZSCAN).
#[derive(Debug)]
pub struct ScanRequest<'a> {
    pub cursor: u64,
    pub pattern: Option<&'a [u8]>,
    pub count: usize,
}

impl<'a> ScanRequest<'a> {
    /// Parse `cursor [MATCH pattern] [COUNT count]` from `args`, delegating
    /// any token the parser doesn't recognize as `MATCH`/`COUNT` to `extra`.
    ///
    /// `extra` is invoked with the parser positioned at the unrecognized
    /// token. It should return `Ok(true)` if it consumed a flag, `Ok(false)`
    /// if the token isn't recognized either (which yields `SyntaxError`), or
    /// propagate a parse error. Commands with no extra flags can pass
    /// `|_| Ok(false)`.
    ///
    /// Matches Redis's `parseScanCursorOrReply`: an unparseable cursor always
    /// produces `CommandError::InvalidArgument` with message "invalid cursor"
    /// (not `NotInteger`), consistently across the whole SCAN family.
    pub fn parse(
        args: &'a [Bytes],
        mut extra: impl FnMut(&mut ArgParser<'a>) -> Result<bool, CommandError>,
    ) -> Result<Self, CommandError> {
        let mut parser = ArgParser::new(args);
        let cursor: u64 = parser
            .next_parsed()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid cursor".to_string(),
            })?;

        let mut pattern: Option<&[u8]> = None;
        let mut count: usize = 10; // Redis default

        while parser.has_more() {
            if let Some(value) = parser.try_flag_value(b"MATCH")? {
                pattern = Some(value.as_ref());
            } else if let Some(value) = parser.try_flag_usize(b"COUNT")? {
                count = value;
            } else if extra(&mut parser)? {
                // Consumed by the caller-specific hook (e.g. TYPE, NOVALUES).
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        Ok(Self {
            cursor,
            pattern,
            count,
        })
    }
}

/// The empty scan envelope `[bulk "0", []]` Redis returns for HSCAN/SSCAN/
/// ZSCAN when the scanned key doesn't exist.
pub fn empty_scan_reply() -> Response {
    Response::Array(vec![
        Response::bulk(Bytes::from_static(b"0")),
        Response::Array(Vec::new()),
    ])
}

/// Drive a per-key cursor scan (the HSCAN/SSCAN/ZSCAN shape).
///
/// If `items` is `None` (the key doesn't exist), replies with
/// [`empty_scan_reply`]. Otherwise runs [`hash_cursor_scan`] over the
/// collection and wraps the result as `[bulk cursor, [items...]]`. Callers
/// resolve the key (including the `WrongType` check) and supply the
/// hash/emit projections; those are the only genuinely per-command pieces.
pub fn scan_reply<T>(
    request: &ScanRequest<'_>,
    items: Option<impl Iterator<Item = T>>,
    hash_key: impl Fn(&T) -> &[u8],
    emit: impl Fn(T, &mut Vec<Response>),
) -> Response {
    let Some(items) = items else {
        return empty_scan_reply();
    };

    let (new_cursor, results) = hash_cursor_scan(
        items,
        request.cursor,
        request.count,
        request.pattern,
        hash_key,
        emit,
    );

    Response::Array(vec![
        Response::bulk(Bytes::from(new_cursor.to_string())),
        Response::Array(results),
    ])
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

/// Return a score as the appropriate Response type.
///
/// In RESP3 mode, finite scores are returned as `Response::Double` so that
/// clients receive the RESP3 Double wire type.  Infinity/NaN values are
/// always returned as bulk strings (`"inf"`, `"-inf"`) to avoid client-side
/// formatting differences (e.g., Tcl displays float infinity as `Inf`).
///
/// In RESP2 mode, scores are always returned as bulk strings (matching Redis behavior).
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

#[cfg(test)]
mod scan_seam_tests {
    use super::*;

    fn bytes_vec(strs: &[&str]) -> Vec<Bytes> {
        strs.iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    /// The empty-key envelope is `[bulk "0", []]`.
    fn assert_empty_envelope(resp: &Response) {
        match resp {
            Response::Array(items) => {
                assert_eq!(items.len(), 2, "envelope must be [cursor, array]");
                assert!(
                    matches!(&items[0], Response::Bulk(Some(c)) if c.as_ref() == b"0"),
                    "cursor must be bulk \"0\""
                );
                assert!(
                    matches!(&items[1], Response::Array(inner) if inner.is_empty()),
                    "items must be an empty array"
                );
            }
            other => panic!("expected array envelope, got {other:?}"),
        }
    }

    #[test]
    fn parses_cursor_only() {
        let args = bytes_vec(&["42"]);
        let req = ScanRequest::parse(&args, |_| Ok(false)).unwrap();
        assert_eq!(req.cursor, 42);
        assert_eq!(req.pattern, None);
        assert_eq!(req.count, 10); // Redis default
    }

    #[test]
    fn parses_match_and_count() {
        let args = bytes_vec(&["7", "MATCH", "foo*", "COUNT", "50"]);
        let req = ScanRequest::parse(&args, |_| Ok(false)).unwrap();
        assert_eq!(req.cursor, 7);
        assert_eq!(req.pattern, Some(b"foo*".as_slice()));
        assert_eq!(req.count, 50);
    }

    #[test]
    fn options_are_case_insensitive() {
        let args = bytes_vec(&["0", "match", "a?c", "count", "3"]);
        let req = ScanRequest::parse(&args, |_| Ok(false)).unwrap();
        assert_eq!(req.pattern, Some(b"a?c".as_slice()));
        assert_eq!(req.count, 3);
    }

    #[test]
    fn cursor_round_trip_through_reply() {
        // A scan that cannot emit everything in one COUNT must report a
        // non-zero continuation cursor that a follow-up call resumes from.
        let members = bytes_vec(&["a", "b", "c", "d", "e"]);
        let first = ScanRequest {
            cursor: 0,
            pattern: None,
            count: 2,
        };
        let resp = scan_reply(
            &first,
            Some(members.iter().cloned()),
            |m: &Bytes| m.as_ref(),
            |m, out: &mut Vec<Response>| out.push(Response::bulk(m)),
        );

        let Response::Array(items) = resp else {
            panic!("expected array envelope");
        };
        let Response::Bulk(Some(cursor_bytes)) = &items[0] else {
            panic!("expected bulk cursor");
        };
        let next: u64 = std::str::from_utf8(cursor_bytes).unwrap().parse().unwrap();
        assert_ne!(next, 0, "partial scan must return a non-zero cursor");
        let Response::Array(emitted) = &items[1] else {
            panic!("expected inner array");
        };
        assert_eq!(emitted.len(), 2, "COUNT 2 emits exactly two members");

        // Resume from the returned cursor and drain the rest.
        let mut seen = emitted.len();
        let mut cursor = next;
        while cursor != 0 {
            let req = ScanRequest {
                cursor,
                pattern: None,
                count: 2,
            };
            let resp = scan_reply(
                &req,
                Some(members.iter().cloned()),
                |m: &Bytes| m.as_ref(),
                |m, out: &mut Vec<Response>| out.push(Response::bulk(m)),
            );
            let Response::Array(items) = resp else {
                panic!("expected array envelope");
            };
            let Response::Bulk(Some(c)) = &items[0] else {
                panic!("expected bulk cursor");
            };
            cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
            let Response::Array(emitted) = &items[1] else {
                panic!("expected inner array");
            };
            seen += emitted.len();
        }
        assert_eq!(
            seen,
            members.len(),
            "full iteration visits every member once"
        );
    }

    #[test]
    fn unknown_option_is_syntax_error() {
        let args = bytes_vec(&["0", "BOGUS"]);
        let err = ScanRequest::parse(&args, |_| Ok(false)).unwrap_err();
        assert!(matches!(err, CommandError::SyntaxError));
    }

    #[test]
    fn bad_cursor_is_invalid_argument() {
        let args = bytes_vec(&["notanumber"]);
        let err = ScanRequest::parse(&args, |_| Ok(false)).unwrap_err();
        match err {
            CommandError::InvalidArgument { message } => assert_eq!(message, "invalid cursor"),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn missing_flag_value_is_syntax_error() {
        // MATCH with no following pattern.
        let args = bytes_vec(&["0", "MATCH"]);
        let err = ScanRequest::parse(&args, |_| Ok(false)).unwrap_err();
        assert!(matches!(err, CommandError::SyntaxError));
    }

    #[test]
    fn empty_key_yields_empty_envelope() {
        let req = ScanRequest {
            cursor: 0,
            pattern: None,
            count: 10,
        };
        let resp = scan_reply(
            &req,
            None::<std::iter::Empty<Bytes>>,
            |m: &Bytes| m.as_ref(),
            |m, out: &mut Vec<Response>| out.push(Response::bulk(m)),
        );
        assert_empty_envelope(&resp);
    }

    #[test]
    fn empty_scan_reply_shape() {
        assert_empty_envelope(&empty_scan_reply());
    }

    #[test]
    fn hscan_extra_flag_novalues_toggles() {
        // The `extra` hook is how HSCAN threads NOVALUES through the shared parser.
        let mut novalues = false;
        let args = bytes_vec(&["0", "NOVALUES"]);
        let req = ScanRequest::parse(&args, |parser| {
            if parser.try_flag(b"NOVALUES") {
                novalues = true;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .unwrap();
        assert_eq!(req.cursor, 0);
        assert!(novalues, "NOVALUES must be consumed by the extra hook");

        // With NOVALUES set, the emit closure skips values, so a single
        // field/value pair yields one element instead of two.
        let fields = vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
        let resp = scan_reply(
            &req,
            Some(fields.into_iter()),
            |e: &(Bytes, Bytes)| e.0.as_ref(),
            |e: (Bytes, Bytes), out: &mut Vec<Response>| {
                out.push(Response::bulk(e.0));
                if !novalues {
                    out.push(Response::bulk(e.1));
                }
            },
        );
        let Response::Array(items) = resp else {
            panic!("expected array envelope");
        };
        let Response::Array(emitted) = &items[1] else {
            panic!("expected inner array");
        };
        assert_eq!(emitted.len(), 1, "NOVALUES emits field only");
    }

    #[test]
    fn extra_hook_takes_precedence_over_syntax_error() {
        // A token the base parser rejects but the hook accepts is not an error.
        let args = bytes_vec(&["0", "NOVALUES", "MATCH", "x*"]);
        let mut novalues = false;
        let req = ScanRequest::parse(&args, |parser| {
            if parser.try_flag(b"NOVALUES") {
                novalues = true;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .unwrap();
        assert!(novalues);
        assert_eq!(req.pattern, Some(b"x*".as_slice()));
    }
}
