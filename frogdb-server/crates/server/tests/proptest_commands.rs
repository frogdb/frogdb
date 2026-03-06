//! Property-based tests for command argument parsing.
//!
//! Since the parse_* functions are module-private, we test parsing behavior
//! through the public types exported from frogdb_core.

use proptest::prelude::*;

use frogdb_core::{LexBound, ScoreBound};

/// Configuration for proptest - run more cases than default for fuzzing
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(1000)
}

// Helper functions that mirror the private parse functions in command modules.
// These test the same parsing logic that commands use.

/// Parse an i64 from bytes (mirrors parse_i64 in commands).
fn try_parse_i64(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok().and_then(|s| s.parse().ok())
}

/// Parse an f64 from bytes with inf/-inf support (mirrors parse_f64 in commands).
fn try_parse_f64(arg: &[u8]) -> Option<f64> {
    std::str::from_utf8(arg).ok().and_then(|s| {
        if s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("+inf") {
            Some(f64::INFINITY)
        } else if s.eq_ignore_ascii_case("-inf") {
            Some(f64::NEG_INFINITY)
        } else {
            s.parse().ok()
        }
    })
}

/// Parse a usize from bytes (mirrors parse_usize in commands).
fn try_parse_usize(arg: &[u8]) -> Option<usize> {
    std::str::from_utf8(arg).ok().and_then(|s| s.parse().ok())
}

/// Parse a score bound (mirrors parse_score_bound in sorted_set.rs).
fn try_parse_score_bound(arg: &[u8]) -> Option<ScoreBound> {
    let s = std::str::from_utf8(arg).ok()?;

    if s.eq_ignore_ascii_case("-inf") {
        return Some(ScoreBound::NegInf);
    }
    if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        return Some(ScoreBound::PosInf);
    }

    if let Some(rest) = s.strip_prefix('(') {
        let value: f64 = rest.parse().ok()?;
        Some(ScoreBound::Exclusive(value))
    } else {
        let value: f64 = s.parse().ok()?;
        Some(ScoreBound::Inclusive(value))
    }
}

/// Parse a lex bound (mirrors parse_lex_bound in sorted_set.rs).
fn try_parse_lex_bound(arg: &[u8]) -> Option<LexBound> {
    if arg.is_empty() {
        return None;
    }

    match arg[0] {
        b'-' if arg.len() == 1 => Some(LexBound::Min),
        b'+' if arg.len() == 1 => Some(LexBound::Max),
        b'[' => Some(LexBound::Inclusive(bytes::Bytes::copy_from_slice(
            &arg[1..],
        ))),
        b'(' => Some(LexBound::Exclusive(bytes::Bytes::copy_from_slice(
            &arg[1..],
        ))),
        _ => None,
    }
}

proptest! {
    #![proptest_config(config())]

    /// parse_i64 should never panic on arbitrary bytes.
    #[test]
    fn parse_i64_never_panics(data: Vec<u8>) {
        let _ = try_parse_i64(&data);
    }

    /// parse_f64 should never panic on arbitrary bytes.
    #[test]
    fn parse_f64_never_panics(data: Vec<u8>) {
        let _ = try_parse_f64(&data);
    }

    /// parse_usize should never panic on arbitrary bytes.
    #[test]
    fn parse_usize_never_panics(data: Vec<u8>) {
        let _ = try_parse_usize(&data);
    }

    /// parse_score_bound should never panic on arbitrary bytes.
    #[test]
    fn parse_score_bound_never_panics(data: Vec<u8>) {
        let _ = try_parse_score_bound(&data);
    }

    /// parse_lex_bound should never panic on arbitrary bytes.
    #[test]
    fn parse_lex_bound_never_panics(data: Vec<u8>) {
        let _ = try_parse_lex_bound(&data);
    }

    /// Valid i64 strings should parse correctly.
    #[test]
    fn valid_i64_parses(i: i64) {
        let s = i.to_string();
        let result = try_parse_i64(s.as_bytes());
        prop_assert_eq!(result, Some(i));
    }

    /// Valid usize strings should parse correctly.
    #[test]
    fn valid_usize_parses(u in 0usize..usize::MAX) {
        let s = u.to_string();
        let result = try_parse_usize(s.as_bytes());
        prop_assert_eq!(result, Some(u));
    }

    /// Valid f64 strings should parse correctly.
    #[test]
    fn valid_f64_parses(f in -1e10f64..1e10f64) {
        // Skip NaN
        if f.is_nan() {
            return Ok(());
        }
        let s = f.to_string();
        let result = try_parse_f64(s.as_bytes());
        prop_assert!(result.is_some());
        let parsed = result.unwrap();
        // Allow some floating point tolerance
        if f == 0.0 {
            prop_assert!((parsed - f).abs() < f64::EPSILON);
        } else {
            prop_assert!((parsed - f).abs() / f.abs() < 1e-10);
        }
    }

    /// Special float values should parse correctly.
    #[test]
    fn special_floats_parse(_dummy: bool) {
        // Positive infinity
        prop_assert_eq!(try_parse_f64(b"inf"), Some(f64::INFINITY));
        prop_assert_eq!(try_parse_f64(b"+inf"), Some(f64::INFINITY));
        prop_assert_eq!(try_parse_f64(b"INF"), Some(f64::INFINITY));
        prop_assert_eq!(try_parse_f64(b"+INF"), Some(f64::INFINITY));

        // Negative infinity
        prop_assert_eq!(try_parse_f64(b"-inf"), Some(f64::NEG_INFINITY));
        prop_assert_eq!(try_parse_f64(b"-INF"), Some(f64::NEG_INFINITY));
    }

    /// Score bounds should parse correctly.
    #[test]
    fn score_bounds_parse_correctly(f in -1e10f64..1e10f64) {
        if f.is_nan() {
            return Ok(());
        }

        // Inclusive bound
        let s = f.to_string();
        let result = try_parse_score_bound(s.as_bytes());
        prop_assert!(result.is_some());
        match result.unwrap() {
            ScoreBound::Inclusive(parsed) => {
                if f == 0.0 {
                    prop_assert!((parsed - f).abs() < f64::EPSILON);
                } else {
                    prop_assert!((parsed - f).abs() / f.abs() < 1e-10);
                }
            }
            _ => prop_assert!(false, "Expected Inclusive"),
        }

        // Exclusive bound
        let s = format!("({}", f);
        let result = try_parse_score_bound(s.as_bytes());
        prop_assert!(result.is_some());
        match result.unwrap() {
            ScoreBound::Exclusive(parsed) => {
                if f == 0.0 {
                    prop_assert!((parsed - f).abs() < f64::EPSILON);
                } else {
                    prop_assert!((parsed - f).abs() / f.abs() < 1e-10);
                }
            }
            _ => prop_assert!(false, "Expected Exclusive"),
        }
    }

    /// Special score bounds should parse correctly.
    #[test]
    fn special_score_bounds_parse(_dummy: bool) {
        prop_assert!(matches!(try_parse_score_bound(b"-inf"), Some(ScoreBound::NegInf)));
        prop_assert!(matches!(try_parse_score_bound(b"+inf"), Some(ScoreBound::PosInf)));
        prop_assert!(matches!(try_parse_score_bound(b"inf"), Some(ScoreBound::PosInf)));
        prop_assert!(matches!(try_parse_score_bound(b"-INF"), Some(ScoreBound::NegInf)));
        prop_assert!(matches!(try_parse_score_bound(b"+INF"), Some(ScoreBound::PosInf)));
    }

    /// Lex bounds should parse correctly.
    #[test]
    fn lex_bounds_parse_correctly(value: Vec<u8>) {
        // Min
        prop_assert!(matches!(try_parse_lex_bound(b"-"), Some(LexBound::Min)));

        // Max
        prop_assert!(matches!(try_parse_lex_bound(b"+"), Some(LexBound::Max)));

        // Inclusive
        let mut inclusive = vec![b'['];
        inclusive.extend_from_slice(&value);
        let result = try_parse_lex_bound(&inclusive);
        prop_assert!(result.is_some());
        match result.unwrap() {
            LexBound::Inclusive(v) => prop_assert_eq!(v.as_ref(), &value[..]),
            _ => prop_assert!(false, "Expected Inclusive"),
        }

        // Exclusive
        let mut exclusive = vec![b'('];
        exclusive.extend_from_slice(&value);
        let result = try_parse_lex_bound(&exclusive);
        prop_assert!(result.is_some());
        match result.unwrap() {
            LexBound::Exclusive(v) => prop_assert_eq!(v.as_ref(), &value[..]),
            _ => prop_assert!(false, "Expected Exclusive"),
        }
    }

    /// Invalid lex bounds should return None.
    #[test]
    fn invalid_lex_bounds_return_none(c in "[a-zA-Z0-9]") {
        // Anything not starting with -, +, [, or ( is invalid
        let result = try_parse_lex_bound(c.as_bytes());
        prop_assert!(result.is_none());
    }

    /// Empty input should return None for lex bounds.
    #[test]
    fn empty_lex_bound_returns_none(_dummy: bool) {
        prop_assert!(try_parse_lex_bound(b"").is_none());
    }

    /// Numbers beyond i64 range should fail to parse.
    #[test]
    fn overflow_i64_returns_none(_dummy: bool) {
        // Beyond i64::MAX
        let big = "99999999999999999999999999";
        prop_assert!(try_parse_i64(big.as_bytes()).is_none());

        // Below i64::MIN
        let small = "-99999999999999999999999999";
        prop_assert!(try_parse_i64(small.as_bytes()).is_none());
    }

    /// Numbers beyond usize range should fail to parse.
    #[test]
    fn overflow_usize_returns_none(_dummy: bool) {
        // Beyond usize::MAX (on 64-bit)
        let big = "999999999999999999999999999999";
        prop_assert!(try_parse_usize(big.as_bytes()).is_none());

        // Negative numbers
        prop_assert!(try_parse_usize(b"-1").is_none());
    }

    /// Invalid UTF-8 should return None.
    #[test]
    fn invalid_utf8_returns_none(bytes in prop::collection::vec(128u8..=255u8, 1..20)) {
        prop_assert!(try_parse_i64(&bytes).is_none());
        prop_assert!(try_parse_f64(&bytes).is_none());
        prop_assert!(try_parse_usize(&bytes).is_none());
        prop_assert!(try_parse_score_bound(&bytes).is_none());
    }

    /// Whitespace should cause parse failures.
    #[test]
    fn whitespace_causes_failure(i: i64) {
        let s = format!(" {}", i);
        prop_assert!(try_parse_i64(s.as_bytes()).is_none());

        let s = format!("{} ", i);
        prop_assert!(try_parse_i64(s.as_bytes()).is_none());
    }

    /// Numbers with leading zeros should still parse.
    #[test]
    fn leading_zeros_parse(i in 0i64..1000) {
        let s = format!("{:010}", i);
        let result = try_parse_i64(s.as_bytes());
        prop_assert_eq!(result, Some(i));
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_parse_zero() {
        assert_eq!(try_parse_i64(b"0"), Some(0));
        assert_eq!(try_parse_f64(b"0"), Some(0.0));
        assert_eq!(try_parse_usize(b"0"), Some(0));
    }

    #[test]
    fn test_parse_negative_zero() {
        assert_eq!(try_parse_f64(b"-0"), Some(-0.0));
        // -0 is still 0 for i64
        assert_eq!(try_parse_i64(b"-0"), Some(0));
    }

    #[test]
    fn test_parse_boundaries() {
        assert_eq!(try_parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(try_parse_i64(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn test_parse_empty() {
        assert_eq!(try_parse_i64(b""), None);
        assert_eq!(try_parse_f64(b""), None);
        assert_eq!(try_parse_usize(b""), None);
        assert_eq!(try_parse_score_bound(b""), None);
    }

    #[test]
    fn test_parse_just_sign() {
        assert_eq!(try_parse_i64(b"-"), None);
        assert_eq!(try_parse_i64(b"+"), None);
        assert_eq!(try_parse_f64(b"-"), None);
        assert_eq!(try_parse_f64(b"+"), None);
    }

    #[test]
    fn test_parse_decimal() {
        assert_eq!(try_parse_i64(b"3.14"), None); // i64 doesn't accept decimals
        assert!(try_parse_f64(b"3.14").is_some());
    }

    #[test]
    fn test_parse_scientific_notation() {
        // i64 doesn't support scientific notation
        assert_eq!(try_parse_i64(b"1e5"), None);

        // f64 does
        let result = try_parse_f64(b"1e5");
        assert!(result.is_some());
        assert!((result.unwrap() - 100000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lex_bound_edge_cases() {
        // Just the prefix character should work
        assert!(matches!(
            try_parse_lex_bound(b"["),
            Some(LexBound::Inclusive(_))
        ));
        assert!(matches!(
            try_parse_lex_bound(b"("),
            Some(LexBound::Exclusive(_))
        ));

        // - and + with extra chars should fail
        assert!(try_parse_lex_bound(b"-extra").is_none());
        assert!(try_parse_lex_bound(b"+extra").is_none());
    }

    #[test]
    fn test_score_bound_edge_cases() {
        // Exclusive with nothing after ( is invalid
        assert!(try_parse_score_bound(b"(").is_none());

        // Just ( followed by valid number
        let result = try_parse_score_bound(b"(0");
        assert!(matches!(result, Some(ScoreBound::Exclusive(f)) if f == 0.0));
    }
}
