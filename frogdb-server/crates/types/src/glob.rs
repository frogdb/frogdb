//! Full Redis-compatible glob pattern matching.
//!
//! Uses an iterative algorithm with O(nm) worst case (no exponential backtracking).
//!
//! Supports:
//! - `*` matches any sequence of characters
//! - `?` matches any single character
//! - `[abc]` matches a, b, or c
//! - `[^abc]` or `[!abc]` matches anything except a, b, c
//! - `[a-z]` matches character ranges
//! - `\` escapes the next character

/// Match a glob pattern against a key.
///
/// Returns true if the key matches the pattern.
pub fn glob_match(pattern: &[u8], key: &[u8]) -> bool {
    // Fast path: match-all
    if pattern == b"*" {
        return true;
    }
    // Fast path: exact match (no special chars)
    if !pattern
        .iter()
        .any(|&b| matches!(b, b'*' | b'?' | b'[' | b'\\'))
    {
        return pattern == key;
    }
    // Fast path: prefix* (e.g., "user:*")
    if pattern.len() >= 2
        && pattern[pattern.len() - 1] == b'*'
        && !pattern[..pattern.len() - 1]
            .iter()
            .any(|&b| matches!(b, b'*' | b'?' | b'[' | b'\\'))
    {
        return key.starts_with(&pattern[..pattern.len() - 1]);
    }
    // Fast path: *suffix (e.g., "*:profile")
    if pattern.len() >= 2
        && pattern[0] == b'*'
        && !pattern[1..]
            .iter()
            .any(|&b| matches!(b, b'*' | b'?' | b'[' | b'\\'))
    {
        return key.ends_with(&pattern[1..]);
    }
    // Full iterative match
    let mut pi = 0; // pattern index
    let mut ki = 0; // key index
    let mut star_pi = usize::MAX; // pattern index after last *
    let mut star_ki = 0; // key index at last * match

    while ki < key.len() {
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    // Skip consecutive stars
                    while pi < pattern.len() && pattern[pi] == b'*' {
                        pi += 1;
                    }
                    // Save backtrack point: resume pattern after *, key from ki+1
                    star_pi = pi;
                    star_ki = ki;
                    // Don't advance ki — try matching * against 0 chars first
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ki += 1;
                    continue;
                }
                b'[' => {
                    if let Some((true, new_pi)) = match_char_class(pattern, pi + 1, key[ki]) {
                        pi = new_pi;
                        ki += 1;
                        continue;
                    }
                    // Fall through to backtrack
                }
                b'\\' => {
                    if pi + 1 < pattern.len() && pattern[pi + 1] == key[ki] {
                        pi += 2;
                        ki += 1;
                        continue;
                    }
                    // Fall through to backtrack
                }
                c => {
                    if c == key[ki] {
                        pi += 1;
                        ki += 1;
                        continue;
                    }
                    // Fall through to backtrack
                }
            }
        }

        // Mismatch (or pattern exhausted while key remains) — try backtracking
        if star_pi != usize::MAX {
            pi = star_pi;
            star_ki += 1;
            ki = star_ki;
        } else {
            return false;
        }
    }

    // Key consumed — skip trailing stars in pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Try to match a character class starting at `pi` (just past the opening `[`).
/// Returns `Some((matched, new_pi))` where `new_pi` points past the closing `]`,
/// or `None` if the class is malformed.
fn match_char_class(pattern: &[u8], mut pi: usize, ch: u8) -> Option<(bool, usize)> {
    let negated = pi < pattern.len() && (pattern[pi] == b'^' || pattern[pi] == b'!');
    if negated {
        pi += 1;
    }

    let mut matched = false;
    let mut prev_char: Option<u8> = None;
    let mut in_range = false;

    loop {
        if pi >= pattern.len() {
            return None; // Unterminated character class
        }

        let c = pattern[pi];
        pi += 1;

        match c {
            b']' if prev_char.is_some() => {
                // End of class (only valid after at least one char)
                break;
            }
            b'\\' => {
                if pi >= pattern.len() {
                    return None;
                }
                let esc = pattern[pi];
                pi += 1;
                if in_range {
                    if let Some(start) = prev_char
                        && ch >= start
                        && ch <= esc
                    {
                        matched = true;
                    }
                    in_range = false;
                } else if ch == esc {
                    matched = true;
                }
                prev_char = Some(esc);
            }
            b'-' if prev_char.is_some() && pi < pattern.len() && pattern[pi] != b']' => {
                in_range = true;
            }
            _ => {
                if in_range {
                    if let Some(start) = prev_char
                        && ch >= start
                        && ch <= c
                    {
                        matched = true;
                    }
                    in_range = false;
                } else if ch == c {
                    matched = true;
                }
                prev_char = Some(c);
            }
        }
    }

    if negated {
        Some((!matched, pi))
    } else {
        Some((matched, pi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_star_matches_anything() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"a"));
    }

    #[test]
    fn test_star_prefix() {
        assert!(glob_match(b"prefix*", b"prefix123"));
        assert!(glob_match(b"prefix*", b"prefix"));
        assert!(!glob_match(b"prefix*", b"notprefix"));
    }

    #[test]
    fn test_star_suffix() {
        assert!(glob_match(b"*suffix", b"anysuffix"));
        assert!(glob_match(b"*suffix", b"suffix"));
        assert!(!glob_match(b"*suffix", b"suffixnot"));
    }

    #[test]
    fn test_star_middle() {
        assert!(glob_match(b"pre*suf", b"presuf"));
        assert!(glob_match(b"pre*suf", b"premiddlesuf"));
        assert!(!glob_match(b"pre*suf", b"presuffix"));
    }

    #[test]
    fn test_multiple_stars() {
        assert!(glob_match(b"*a*", b"abc"));
        assert!(glob_match(b"*a*", b"bac"));
        assert!(glob_match(b"*a*", b"a"));
        assert!(!glob_match(b"*a*", b"bbb"));
    }

    #[test]
    fn test_question_mark() {
        assert!(glob_match(b"?", b"a"));
        assert!(!glob_match(b"?", b""));
        assert!(!glob_match(b"?", b"ab"));
    }

    #[test]
    fn test_question_mark_pattern() {
        assert!(glob_match(b"a?c", b"abc"));
        assert!(glob_match(b"a?c", b"aXc"));
        assert!(!glob_match(b"a?c", b"ac"));
        assert!(!glob_match(b"a?c", b"abbc"));
    }

    #[test]
    fn test_character_class() {
        assert!(glob_match(b"[abc]", b"a"));
        assert!(glob_match(b"[abc]", b"b"));
        assert!(glob_match(b"[abc]", b"c"));
        assert!(!glob_match(b"[abc]", b"d"));
        assert!(!glob_match(b"[abc]", b""));
    }

    #[test]
    fn test_character_class_negated() {
        assert!(!glob_match(b"[^abc]", b"a"));
        assert!(!glob_match(b"[!abc]", b"b"));
        assert!(glob_match(b"[^abc]", b"d"));
        assert!(glob_match(b"[!abc]", b"x"));
    }

    #[test]
    fn test_character_range() {
        assert!(glob_match(b"[a-z]", b"m"));
        assert!(glob_match(b"[a-z]", b"a"));
        assert!(glob_match(b"[a-z]", b"z"));
        assert!(!glob_match(b"[a-z]", b"A"));
        assert!(!glob_match(b"[a-z]", b"1"));
    }

    #[test]
    fn test_character_range_multiple() {
        assert!(glob_match(b"[a-zA-Z0-9]", b"m"));
        assert!(glob_match(b"[a-zA-Z0-9]", b"M"));
        assert!(glob_match(b"[a-zA-Z0-9]", b"5"));
        assert!(!glob_match(b"[a-zA-Z0-9]", b"!"));
    }

    #[test]
    fn test_escape() {
        assert!(glob_match(b"\\*", b"*"));
        assert!(glob_match(b"\\?", b"?"));
        assert!(glob_match(b"\\[", b"["));
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"\\*", b"a"));
    }

    #[test]
    fn test_complex_pattern() {
        assert!(glob_match(b"user:*:profile", b"user:123:profile"));
        assert!(glob_match(b"user:*:profile", b"user:abc:profile"));
        assert!(!glob_match(b"user:*:profile", b"user:123:settings"));
    }

    #[test]
    fn test_exact_match() {
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"notexact"));
        assert!(!glob_match(b"exact", b"exactnot"));
    }

    #[test]
    fn test_empty() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"a"));
        assert!(!glob_match(b"a", b""));
    }

    #[test]
    fn test_h_llo_pattern() {
        // Redis example: h?llo
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(glob_match(b"h?llo", b"hxllo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(!glob_match(b"h?llo", b"heello"));
    }

    #[test]
    fn test_h_star_llo_pattern() {
        // Redis example: h*llo
        assert!(glob_match(b"h*llo", b"hllo"));
        assert!(glob_match(b"h*llo", b"hello"));
        assert!(glob_match(b"h*llo", b"heeeello"));
    }

    #[test]
    fn test_h_ae_llo_pattern() {
        // Redis example: h[ae]llo
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn test_h_not_e_llo_pattern() {
        // Redis example: h[^e]llo
        assert!(!glob_match(b"h[^e]llo", b"hello"));
        assert!(glob_match(b"h[^e]llo", b"hallo"));
        assert!(glob_match(b"h[^e]llo", b"hbllo"));
    }

    #[test]
    fn test_h_a_b_llo_pattern() {
        // Redis example: h[a-b]llo
        assert!(glob_match(b"h[a-b]llo", b"hallo"));
        assert!(glob_match(b"h[a-b]llo", b"hbllo"));
        assert!(!glob_match(b"h[a-b]llo", b"hcllo"));
    }

    #[test]
    fn test_dash_in_class() {
        // Dash at beginning or end of character class
        assert!(glob_match(b"[-abc]", b"-"));
        assert!(glob_match(b"[abc-]", b"-"));
        assert!(glob_match(b"[-abc]", b"a"));
    }

    #[test]
    fn test_bracket_in_class() {
        // First char in class can be ]
        assert!(glob_match(b"[]abc]", b"]"));
        assert!(glob_match(b"[]abc]", b"a"));
    }

    #[test]
    fn test_catastrophic_backtracking() {
        // This pattern caused exponential backtracking with the recursive algorithm.
        // It should complete in milliseconds with the iterative algorithm.
        let key = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let pattern = b"a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b";
        assert!(!glob_match(pattern, key));
    }
}
