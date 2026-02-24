//! Full Redis-compatible glob pattern matching.
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
    // Full recursive match
    let mut pattern_iter = pattern.iter().peekable();
    let mut key_iter = key.iter().peekable();

    glob_match_impl(&mut pattern_iter, &mut key_iter)
}

fn glob_match_impl<'a>(
    pattern: &mut std::iter::Peekable<std::slice::Iter<'a, u8>>,
    key: &mut std::iter::Peekable<std::slice::Iter<'a, u8>>,
) -> bool {
    while let Some(&p) = pattern.next() {
        match p {
            b'*' => {
                // Skip consecutive stars
                while pattern.peek() == Some(&&b'*') {
                    pattern.next();
                }

                // If * is at the end, match everything
                if pattern.peek().is_none() {
                    return true;
                }

                // Try matching * against 0 or more characters
                loop {
                    // Clone iterators to try matching from current position
                    let mut pattern_clone = pattern.clone();
                    let mut key_clone = key.clone();

                    if glob_match_impl(&mut pattern_clone, &mut key_clone) {
                        return true;
                    }

                    // Advance key by one character
                    if key.next().is_none() {
                        return false;
                    }
                }
            }
            b'?' => {
                // Match exactly one character
                if key.next().is_none() {
                    return false;
                }
            }
            b'[' => {
                // Character class
                let Some(&k) = key.next() else {
                    return false;
                };

                let negated = pattern.peek() == Some(&&b'^') || pattern.peek() == Some(&&b'!');
                if negated {
                    pattern.next();
                }

                let mut matched = false;
                let mut prev_char: Option<u8> = None;
                let mut in_range = false;

                loop {
                    match pattern.next() {
                        Some(&b']') if prev_char.is_some() => {
                            // End of character class (but only if we've seen at least one char)
                            break;
                        }
                        Some(&b'\\') => {
                            // Escaped character in class
                            if let Some(&c) = pattern.next() {
                                if in_range {
                                    // Range: prev_char-c
                                    if let Some(start) = prev_char
                                        && k >= start
                                        && k <= c
                                    {
                                        matched = true;
                                    }
                                    in_range = false;
                                } else if k == c {
                                    matched = true;
                                }
                                prev_char = Some(c);
                            } else {
                                return false; // Unterminated escape
                            }
                        }
                        Some(&b'-') if prev_char.is_some() && pattern.peek() != Some(&&b']') => {
                            // Range indicator
                            in_range = true;
                        }
                        Some(&c) => {
                            if in_range {
                                // Range: prev_char-c
                                if let Some(start) = prev_char
                                    && k >= start
                                    && k <= c
                                {
                                    matched = true;
                                }
                                in_range = false;
                            } else if k == c {
                                matched = true;
                            }
                            prev_char = Some(c);
                        }
                        None => {
                            // Unterminated character class
                            return false;
                        }
                    }
                }

                if negated {
                    if matched {
                        return false;
                    }
                } else if !matched {
                    return false;
                }
            }
            b'\\' => {
                // Escaped character
                let Some(&escaped) = pattern.next() else {
                    return false; // Unterminated escape
                };
                let Some(&k) = key.next() else {
                    return false;
                };
                if escaped != k {
                    return false;
                }
            }
            _ => {
                // Literal character
                let Some(&k) = key.next() else {
                    return false;
                };
                if p != k {
                    return false;
                }
            }
        }
    }

    // Pattern consumed; key must also be consumed
    key.peek().is_none()
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
}
