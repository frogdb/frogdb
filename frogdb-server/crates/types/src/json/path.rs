//! JSONPath parsing and evaluation against a [`JsonTape`](super::tape::JsonTape).
//!
//! A path expands to the set of tape word offsets it matches. Offsets are the
//! currency of both reads (hand them back as [`TapeRef`] cursors) and writes (key
//! an edit map by them and rebuild the tape), so navigation happens exactly once
//! per command.

use super::JsonError;
use super::tape::TapeRef;

/// One parsed path step.
#[derive(Debug, Clone)]
pub(super) enum PathPattern {
    Key(String),
    Index(i64),
    Wildcard,
}

/// Normalize a JSONPath to standard `$`-rooted format.
pub(super) fn normalize_path(path: &str) -> String {
    let path = path.trim();
    if path.is_empty() || path == "$" || path == "." {
        return "$".to_string();
    }

    // Handle legacy dot notation
    if path.starts_with('.') && !path.starts_with("..") {
        // Convert .foo.bar to $.foo.bar
        return format!("${}", path);
    }

    if !path.starts_with('$') {
        return format!("$.{}", path);
    }

    path.to_string()
}

/// Parse a normalized JSONPath into steps.
pub(super) fn parse_path_patterns(path: &str) -> Result<Vec<PathPattern>, JsonError> {
    let mut patterns = Vec::new();
    let mut chars = path.chars().peekable();

    // Skip the leading $
    if chars.peek() == Some(&'$') {
        chars.next();
    }

    while let Some(c) = chars.next() {
        match c {
            '.' => {
                if chars.peek() == Some(&'.') {
                    // Recursive descent - not fully supported, treat as error for now
                    return Err(JsonError::InvalidPath(
                        "recursive descent not supported".to_string(),
                    ));
                }
                // Read key name
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(chars.next().unwrap());
                }
                if key == "*" {
                    patterns.push(PathPattern::Wildcard);
                } else if !key.is_empty() {
                    patterns.push(PathPattern::Key(key));
                }
            }
            '[' => {
                let mut bracket_content = String::new();
                let mut depth = 1;
                for c in chars.by_ref() {
                    if c == '[' {
                        depth += 1;
                    } else if c == ']' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    bracket_content.push(c);
                }

                let content = bracket_content.trim();
                if content == "*" {
                    patterns.push(PathPattern::Wildcard);
                } else if content.starts_with('\'') || content.starts_with('"') {
                    // String key
                    let key = content.trim_matches(|c| c == '\'' || c == '"');
                    patterns.push(PathPattern::Key(key.to_string()));
                } else if let Ok(idx) = content.parse::<i64>() {
                    patterns.push(PathPattern::Index(idx));
                } else {
                    return Err(JsonError::InvalidPath(format!(
                        "invalid bracket content: {}",
                        content
                    )));
                }
            }
            _ => {
                return Err(JsonError::InvalidPath(format!(
                    "unexpected character: {}",
                    c
                )));
            }
        }
    }

    Ok(patterns)
}

/// Expand `path` against `root`, returning the tape offset of every match in
/// document order.
///
/// The root path matches the document itself (offset 0).
pub(super) fn match_offsets(path: &str, root: TapeRef<'_>) -> Result<Vec<usize>, JsonError> {
    let normalized = normalize_path(path);
    if normalized == "$" {
        return Ok(vec![root.offset()]);
    }

    let patterns = parse_path_patterns(&normalized)?;
    let mut matches = Vec::new();
    expand(&patterns, root, &mut matches);
    Ok(matches)
}

fn expand(patterns: &[PathPattern], node: TapeRef<'_>, out: &mut Vec<usize>) {
    let Some((pattern, rest)) = patterns.split_first() else {
        out.push(node.offset());
        return;
    };

    match pattern {
        PathPattern::Key(key) => {
            if let Some(child) = node.member(key) {
                expand(rest, child, out);
            }
        }
        PathPattern::Index(index) => {
            if let Some(len) = node.is_array().then(|| node.container_len().unwrap_or(0)) {
                let resolved = if *index < 0 {
                    len as i64 + index
                } else {
                    *index
                };
                if resolved >= 0
                    && let Some(child) = node.element(resolved as usize)
                {
                    expand(rest, child, out);
                }
            }
        }
        PathPattern::Wildcard => {
            for child in node.elements() {
                expand(rest, child, out);
            }
            for (_, child) in node.members() {
                expand(rest, child, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tape::JsonTape;
    use super::*;
    use serde_json::json;

    fn matches(doc: serde_json::Value, path: &str) -> Vec<String> {
        let tape = JsonTape::from_value(&doc);
        match_offsets(path, tape.root())
            .unwrap()
            .into_iter()
            .map(|at| tape.node_at(at).to_json_string())
            .collect()
    }

    #[test]
    fn root_matches_document() {
        assert_eq!(matches(json!({"a": 1}), "$"), vec![r#"{"a":1}"#]);
        assert_eq!(matches(json!({"a": 1}), "."), vec![r#"{"a":1}"#]);
        assert_eq!(matches(json!({"a": 1}), ""), vec![r#"{"a":1}"#]);
    }

    #[test]
    fn keys_and_indices_resolve() {
        let doc = json!({"a": {"b": [10, 20, 30]}});
        assert_eq!(matches(doc.clone(), "$.a.b[0]"), vec!["10"]);
        assert_eq!(matches(doc.clone(), "$.a.b[-1]"), vec!["30"]);
        assert_eq!(matches(doc.clone(), "$.a.b[3]"), Vec::<String>::new());
        assert_eq!(matches(doc.clone(), "$.a.b[-9]"), Vec::<String>::new());
        assert_eq!(matches(doc.clone(), "a.b[1]"), vec!["20"]);
        assert_eq!(matches(doc, "$['a']['b'][1]"), vec!["20"]);
    }

    #[test]
    fn wildcards_expand_both_container_kinds() {
        assert_eq!(matches(json!([1, 2]), "$[*]"), vec!["1", "2"]);
        assert_eq!(matches(json!({"a": 1, "b": 2}), "$.*"), vec!["1", "2"]);
        assert_eq!(
            matches(json!({"items": [{"id": 1}, {"id": 2}]}), "$.items[*].id"),
            vec!["1", "2"]
        );
        assert_eq!(matches(json!(7), "$.*"), Vec::<String>::new());
    }

    #[test]
    fn unsupported_syntax_is_rejected() {
        let tape = JsonTape::from_value(&json!({"a": 1}));
        assert!(matches!(
            match_offsets("$..a", tape.root()),
            Err(JsonError::InvalidPath(_))
        ));
        assert!(matches!(
            match_offsets("$[oops]", tape.root()),
            Err(JsonError::InvalidPath(_))
        ));
    }
}
