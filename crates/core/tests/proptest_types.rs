//! Property-based tests for type parsing (StreamId, bounds, etc.).
//!
//! Tests that parsing functions never panic on arbitrary input.

use proptest::prelude::*;

use frogdb_core::{StreamId, StreamIdParseError, StreamIdSpec, StreamRangeBound};

/// Configuration for proptest - run more cases than default for fuzzing
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(1000)
}

proptest! {
    #![proptest_config(config())]

    /// StreamId::parse should never panic on arbitrary bytes.
    #[test]
    fn stream_id_parse_never_panics(data: Vec<u8>) {
        let _: Result<StreamId, StreamIdParseError> = StreamId::parse(&data);
    }

    /// StreamId::parse_range_bound should never panic on arbitrary bytes.
    #[test]
    fn stream_id_parse_range_bound_never_panics(data: Vec<u8>) {
        let _: Result<StreamRangeBound, StreamIdParseError> = StreamId::parse_range_bound(&data);
    }

    /// StreamId::parse_for_add should never panic on arbitrary bytes.
    #[test]
    fn stream_id_parse_for_add_never_panics(data: Vec<u8>) {
        let _: Result<StreamIdSpec, StreamIdParseError> = StreamId::parse_for_add(&data);
    }

    /// Valid stream ID string should parse and roundtrip.
    #[test]
    fn valid_stream_id_roundtrip(ms: u64, seq: u64) {
        let id = StreamId::new(ms, seq);
        let formatted = format!("{}-{}", ms, seq);
        let parsed = StreamId::parse(formatted.as_bytes());

        prop_assert!(parsed.is_ok());
        let parsed_id = parsed.unwrap();
        prop_assert_eq!(id.ms, parsed_id.ms);
        prop_assert_eq!(id.seq, parsed_id.seq);
    }

    /// Milliseconds-only format should parse with seq=0.
    #[test]
    fn ms_only_stream_id_parses(ms: u64) {
        let formatted = format!("{}", ms);
        let parsed = StreamId::parse(formatted.as_bytes());

        prop_assert!(parsed.is_ok());
        let parsed_id = parsed.unwrap();
        prop_assert_eq!(parsed_id.ms, ms);
        prop_assert_eq!(parsed_id.seq, 0);
    }

    /// Special stream ID values for range bounds.
    #[test]
    fn special_range_bounds_parse_correctly(_dummy: bool) {
        // Minimum
        let min = StreamId::parse_range_bound(b"-");
        prop_assert!(matches!(min, Ok(StreamRangeBound::Min)));

        // Maximum
        let max = StreamId::parse_range_bound(b"+");
        prop_assert!(matches!(max, Ok(StreamRangeBound::Max)));
    }

    /// Exclusive range bound should parse correctly.
    #[test]
    fn exclusive_range_bound_parses(ms: u64, seq: u64) {
        let formatted = format!("({}-{}", ms, seq);
        let parsed = StreamId::parse_range_bound(formatted.as_bytes());

        prop_assert!(parsed.is_ok());
        match parsed.unwrap() {
            StreamRangeBound::Exclusive(id) => {
                prop_assert_eq!(id.ms, ms);
                prop_assert_eq!(id.seq, seq);
            }
            _ => prop_assert!(false, "Expected Exclusive variant"),
        }
    }

    /// Inclusive range bound should parse correctly.
    #[test]
    fn inclusive_range_bound_parses(ms: u64, seq: u64) {
        let formatted = format!("{}-{}", ms, seq);
        let parsed = StreamId::parse_range_bound(formatted.as_bytes());

        prop_assert!(parsed.is_ok());
        match parsed.unwrap() {
            StreamRangeBound::Inclusive(id) => {
                prop_assert_eq!(id.ms, ms);
                prop_assert_eq!(id.seq, seq);
            }
            _ => prop_assert!(false, "Expected Inclusive variant"),
        }
    }

    /// Auto-generate ID spec should parse.
    #[test]
    fn auto_id_spec_parses(_dummy: bool) {
        let parsed = StreamId::parse_for_add(b"*");
        prop_assert!(matches!(parsed, Ok(StreamIdSpec::Auto)));
    }

    /// Auto-sequence ID spec should parse.
    #[test]
    fn auto_seq_id_spec_parses(ms: u64) {
        let formatted = format!("{}-*", ms);
        let parsed = StreamId::parse_for_add(formatted.as_bytes());

        prop_assert!(parsed.is_ok());
        match parsed.unwrap() {
            StreamIdSpec::AutoSeq(parsed_ms) => {
                prop_assert_eq!(parsed_ms, ms);
            }
            _ => prop_assert!(false, "Expected AutoSeq variant"),
        }
    }

    /// Explicit ID spec should parse.
    #[test]
    fn explicit_id_spec_parses(ms: u64, seq: u64) {
        let formatted = format!("{}-{}", ms, seq);
        let parsed = StreamId::parse_for_add(formatted.as_bytes());

        prop_assert!(parsed.is_ok());
        match parsed.unwrap() {
            StreamIdSpec::Explicit(id) => {
                prop_assert_eq!(id.ms, ms);
                prop_assert_eq!(id.seq, seq);
            }
            _ => prop_assert!(false, "Expected Explicit variant"),
        }
    }

    /// Invalid UTF-8 should return error, not panic.
    #[test]
    fn invalid_utf8_stream_id_returns_error(bytes in prop::collection::vec(128u8..=255u8, 1..20)) {
        let result = StreamId::parse(&bytes);
        // Should either be an error (invalid format) or Ok if it happens to be valid
        // The key point is it doesn't panic
        let _ = result;
    }

    /// Very large numbers in stream ID should not panic.
    #[test]
    fn large_number_stream_id_doesnt_panic(_dummy: bool) {
        // u64::MAX as string
        let large_ms = format!("{}", u64::MAX);
        let _ = StreamId::parse(large_ms.as_bytes());

        // Beyond u64::MAX
        let too_large = "99999999999999999999999999999999";
        let _ = StreamId::parse(too_large.as_bytes());

        // With sequence
        let large_both = format!("{}-{}", u64::MAX, u64::MAX);
        let _ = StreamId::parse(large_both.as_bytes());
    }

    /// Negative numbers should fail to parse but not panic.
    #[test]
    fn negative_number_stream_id_doesnt_panic(ms: i64, seq: i64) {
        let formatted = format!("{}-{}", ms, seq);
        let _ = StreamId::parse(formatted.as_bytes());
    }

    /// Empty string should not panic.
    #[test]
    fn empty_stream_id_doesnt_panic(_dummy: bool) {
        let result = StreamId::parse(b"");
        prop_assert!(result.is_err());

        let result = StreamId::parse_range_bound(b"");
        prop_assert!(result.is_err());

        let result = StreamId::parse_for_add(b"");
        prop_assert!(result.is_err());
    }

    /// Various malformed inputs should not panic.
    #[test]
    fn malformed_stream_id_doesnt_panic(s in "[^0-9]{0,20}") {
        let _ = StreamId::parse(s.as_bytes());
        let _ = StreamId::parse_range_bound(s.as_bytes());
        let _ = StreamId::parse_for_add(s.as_bytes());
    }

    /// Multiple dashes should not panic.
    #[test]
    fn multiple_dashes_stream_id_doesnt_panic(parts in prop::collection::vec("[0-9]{1,5}", 1..5)) {
        let s = parts.join("-");
        let _ = StreamId::parse(s.as_bytes());
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_stream_id_min_max() {
        let min = StreamId::min();
        assert_eq!(min.ms, 0);
        assert_eq!(min.seq, 0);

        let max = StreamId::max();
        assert_eq!(max.ms, u64::MAX);
        assert_eq!(max.seq, u64::MAX);
    }

    #[test]
    fn test_stream_id_is_zero() {
        let zero = StreamId::new(0, 0);
        assert!(zero.is_zero());

        let nonzero = StreamId::new(1, 0);
        assert!(!nonzero.is_zero());
    }

    #[test]
    fn test_stream_id_ordering() {
        let a = StreamId::new(1, 0);
        let b = StreamId::new(1, 1);
        let c = StreamId::new(2, 0);

        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_whitespace_stream_id() {
        // Leading/trailing whitespace
        assert!(StreamId::parse(b" 123-456").is_err());
        assert!(StreamId::parse(b"123-456 ").is_err());
        assert!(StreamId::parse(b" 123-456 ").is_err());
    }

    #[test]
    fn test_special_characters_stream_id() {
        assert!(StreamId::parse(b"123.456").is_err());
        assert!(StreamId::parse(b"123:456").is_err());
        assert!(StreamId::parse(b"123_456").is_err());
        assert!(StreamId::parse(b"123/456").is_err());
    }
}
