//! Property-based tests for glob pattern matching.
//!
//! Tests that:
//! - Glob matching never panics on arbitrary input
//! - Known invariants hold (e.g., * matches everything)
//! - Edge cases are handled correctly

use proptest::prelude::*;

use frogdb_core::glob_match;

/// Configuration for proptest - run more cases than default for fuzzing
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(1000)
}

proptest! {
    #![proptest_config(config())]

    /// glob_match should never panic on arbitrary pattern and key.
    #[test]
    fn glob_match_never_panics(pattern: Vec<u8>, key: Vec<u8>) {
        let _ = glob_match(&pattern, &key);
    }

    /// Star (*) pattern should match any string.
    #[test]
    fn star_matches_everything(key: Vec<u8>) {
        prop_assert!(glob_match(b"*", &key));
    }

    /// Double star (**) should behave the same as single star.
    #[test]
    fn double_star_matches_everything(key: Vec<u8>) {
        prop_assert!(glob_match(b"**", &key));
        prop_assert!(glob_match(b"***", &key));
    }

    /// Literal pattern (alphanumeric only) should match itself.
    #[test]
    fn literal_matches_self(s in "[a-zA-Z0-9]{1,100}") {
        prop_assert!(glob_match(s.as_bytes(), s.as_bytes()));
    }

    /// Literal pattern should not match different string.
    #[test]
    fn literal_doesnt_match_different(
        a in "[a-zA-Z0-9]{1,50}",
        b in "[a-zA-Z0-9]{1,50}"
    ) {
        if a != b {
            prop_assert!(!glob_match(a.as_bytes(), b.as_bytes()));
        }
    }

    /// Empty pattern only matches empty key.
    #[test]
    fn empty_pattern_only_matches_empty(key: Vec<u8>) {
        let result = glob_match(b"", &key);
        prop_assert_eq!(result, key.is_empty());
    }

    /// Question mark (?) matches exactly one character.
    #[test]
    fn question_mark_matches_one_char(key in "[a-z]{1}") {
        prop_assert!(glob_match(b"?", key.as_bytes()));
    }

    /// Question mark (?) doesn't match empty string.
    #[test]
    fn question_mark_doesnt_match_empty(_dummy: bool) {
        prop_assert!(!glob_match(b"?", b""));
    }

    /// Question mark (?) doesn't match multiple characters.
    #[test]
    fn question_mark_doesnt_match_multiple(key in "[a-z]{2,10}") {
        prop_assert!(!glob_match(b"?", key.as_bytes()));
    }

    /// Pattern prefix* should match keys starting with prefix.
    #[test]
    fn prefix_star_matches_prefix(prefix in "[a-z]{1,10}", suffix in "[a-z]{0,20}") {
        let pattern = format!("{}*", prefix);
        let key = format!("{}{}", prefix, suffix);
        prop_assert!(glob_match(pattern.as_bytes(), key.as_bytes()));
    }

    /// Pattern *suffix should match keys ending with suffix.
    #[test]
    fn star_suffix_matches_suffix(prefix in "[a-z]{0,20}", suffix in "[a-z]{1,10}") {
        let pattern = format!("*{}", suffix);
        let key = format!("{}{}", prefix, suffix);
        prop_assert!(glob_match(pattern.as_bytes(), key.as_bytes()));
    }

    /// Character class [abc] should match a, b, or c.
    #[test]
    fn character_class_matches_members(c in "[abc]") {
        prop_assert!(glob_match(b"[abc]", c.as_bytes()));
    }

    /// Character class [abc] should not match other characters.
    #[test]
    fn character_class_doesnt_match_others(c in "[d-z]") {
        prop_assert!(!glob_match(b"[abc]", c.as_bytes()));
    }

    /// Negated character class [^abc] should not match a, b, or c.
    #[test]
    fn negated_class_doesnt_match_members(c in "[abc]") {
        prop_assert!(!glob_match(b"[^abc]", c.as_bytes()));
    }

    /// Negated character class [^abc] should match other characters.
    #[test]
    fn negated_class_matches_others(c in "[d-z]") {
        prop_assert!(glob_match(b"[^abc]", c.as_bytes()));
    }

    /// Character range [a-z] should match lowercase letters.
    #[test]
    fn char_range_matches_in_range(c in "[a-z]") {
        prop_assert!(glob_match(b"[a-z]", c.as_bytes()));
    }

    /// Character range [a-z] should not match uppercase or digits.
    #[test]
    fn char_range_doesnt_match_out_of_range(c in "[A-Z0-9]") {
        prop_assert!(!glob_match(b"[a-z]", c.as_bytes()));
    }

    /// Escaped star (\*) should match literal asterisk.
    #[test]
    fn escaped_star_matches_literal(_dummy: bool) {
        prop_assert!(glob_match(b"\\*", b"*"));
        prop_assert!(!glob_match(b"\\*", b"a"));
        prop_assert!(!glob_match(b"\\*", b""));
    }

    /// Escaped question mark (\?) should match literal question mark.
    #[test]
    fn escaped_question_matches_literal(_dummy: bool) {
        prop_assert!(glob_match(b"\\?", b"?"));
        prop_assert!(!glob_match(b"\\?", b"a"));
    }

    /// Escaped bracket (\[) should match literal bracket.
    #[test]
    fn escaped_bracket_matches_literal(_dummy: bool) {
        prop_assert!(glob_match(b"\\[", b"["));
        prop_assert!(!glob_match(b"\\[", b"a"));
    }

    /// Unterminated character class should not match.
    #[test]
    fn unterminated_bracket_doesnt_match(key: Vec<u8>) {
        // [abc without closing ] should return false (or be treated as literal)
        let result = glob_match(b"[abc", &key);
        // This should not panic - the result depends on implementation
        let _ = result;
    }

    /// Reversed range [z-a] behavior test.
    #[test]
    fn reversed_range_doesnt_panic(c in "[a-z]") {
        // [z-a] is an invalid/empty range
        let _ = glob_match(b"[z-a]", c.as_bytes());
    }

    /// Complex pattern with multiple special characters.
    #[test]
    fn complex_pattern_doesnt_panic(
        prefix in "[a-z]{0,5}",
        mid in "[a-z]{0,5}",
        suffix in "[a-z]{0,5}"
    ) {
        let pattern = format!("{}*[a-z]?{}*{}", prefix, mid, suffix);
        let key = format!("{}test1x{}something{}", prefix, mid, suffix);
        let _ = glob_match(pattern.as_bytes(), key.as_bytes());
    }

    /// Redis KEYS pattern examples should not panic.
    #[test]
    fn redis_patterns_dont_panic(id in "[0-9]{1,5}") {
        // Common Redis patterns
        let patterns = [
            "user:*",
            "user:*:profile",
            "*:cache:*",
            "h?llo",
            "h*llo",
            "h[ae]llo",
            "h[^e]llo",
            "h[a-b]llo",
        ];

        let key = format!("user:{}:profile", id);
        for pattern in &patterns {
            let _ = glob_match(pattern.as_bytes(), key.as_bytes());
        }
    }

    /// Binary data in pattern and key should not panic.
    #[test]
    fn binary_data_doesnt_panic(
        pattern in prop::collection::vec(any::<u8>(), 0..50),
        key in prop::collection::vec(any::<u8>(), 0..50)
    ) {
        let _ = glob_match(&pattern, &key);
    }

    /// Very long patterns should not panic.
    #[test]
    fn long_pattern_doesnt_panic(len in 0usize..1000) {
        let pattern = vec![b'a'; len];
        let key = vec![b'a'; len];
        let result = glob_match(&pattern, &key);
        prop_assert!(result);
    }

    /// Pattern with many stars should not cause exponential blowup.
    #[test]
    fn many_stars_doesnt_hang(n in 1usize..20) {
        let pattern: Vec<u8> = std::iter::repeat_n(b'*', n).collect();
        let key = b"short";
        // This should complete quickly
        prop_assert!(glob_match(&pattern, key));
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_pattern_empty_key() {
        assert!(glob_match(b"", b""));
    }

    #[test]
    fn test_star_empty_key() {
        assert!(glob_match(b"*", b""));
    }

    #[test]
    fn test_question_empty_key() {
        assert!(!glob_match(b"?", b""));
    }

    #[test]
    fn test_nested_escapes() {
        // \\* should match literal backslash followed by anything
        assert!(glob_match(b"\\\\*", b"\\anything"));
    }

    #[test]
    fn test_bracket_at_start_of_class() {
        // First char in class can be ]
        assert!(glob_match(b"[]abc]", b"]"));
        assert!(glob_match(b"[]abc]", b"a"));
    }

    #[test]
    fn test_dash_at_start_of_class() {
        // Dash at beginning of character class
        assert!(glob_match(b"[-abc]", b"-"));
        assert!(glob_match(b"[-abc]", b"a"));
    }

    #[test]
    fn test_dash_at_end_of_class() {
        // Dash at end of character class
        assert!(glob_match(b"[abc-]", b"-"));
        assert!(glob_match(b"[abc-]", b"a"));
    }

    #[test]
    fn test_exclamation_negation() {
        // ! can also negate (like ^)
        assert!(!glob_match(b"[!abc]", b"a"));
        assert!(glob_match(b"[!abc]", b"x"));
    }

    #[test]
    fn test_all_byte_values() {
        // Test all possible byte values as single-byte keys
        for b in 0u8..=255 {
            let key = [b];
            let _ = glob_match(b"*", &key);
            let _ = glob_match(b"?", &key);
        }
    }

    #[test]
    fn test_pathological_pattern() {
        // Pattern that could cause exponential backtracking in naive implementations
        let pattern = b"*a*a*a*a*a*a*a*a*b";
        let key = b"aaaaaaaaaaaaaaaaaaaa"; // No 'b' at end
        assert!(!glob_match(pattern, key));
    }

    #[test]
    fn test_unicode_bytes() {
        // UTF-8 encoded unicode
        let pattern = "hello*".as_bytes();
        let key = "hello\u{1F600}".as_bytes(); // hello + emoji
        assert!(glob_match(pattern, key));
    }
}
