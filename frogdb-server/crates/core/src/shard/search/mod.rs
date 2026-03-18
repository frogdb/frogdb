mod aliases;
mod config;
mod create;
mod dict;
mod index_mgmt;
mod query;
mod spellcheck;
mod synonyms;
mod tagvals;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Resolve an index name through the alias map.
    pub(crate) fn resolve_index_name<'a>(&'a self, name: &'a str) -> &'a str {
        self.search
            .aliases
            .get(name)
            .map(|s| s.as_str())
            .unwrap_or(name)
    }

    /// Persist the alias map to RocksDB search_meta CF.
    pub(crate) fn persist_aliases(&self) {
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&self.search.aliases)
            && let Err(e) = rocks.put_search_meta(self.identity.shard_id, b"__aliases__", &json)
        {
            tracing::error!(error = %e, "Failed to persist search index aliases");
        }
    }

    /// Persist a dictionary to RocksDB search_meta CF.
    pub(crate) fn persist_dict(&self, dict_name: &str) {
        if let Some(ref rocks) = self.persistence.rocks_store {
            let key = format!("__dict__:{}", dict_name);
            if let Some(dict) = self.search.dictionaries.get(dict_name) {
                let terms: Vec<&String> = dict.iter().collect();
                if let Ok(json) = serde_json::to_vec(&terms)
                    && let Err(e) =
                        rocks.put_search_meta(self.identity.shard_id, key.as_bytes(), &json)
                {
                    tracing::error!(error = %e, "Failed to persist search dictionary");
                }
            }
        }
    }

    /// Persist search config to RocksDB search_meta CF.
    pub(crate) fn persist_search_config(&self) {
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&self.search.config)
            && let Err(e) = rocks.put_search_meta(self.identity.shard_id, b"__config__", &json)
        {
            tracing::error!(error = %e, "Failed to persist search config");
        }
    }
}

/// Simple glob matching for FT.CONFIG GET patterns.
pub(crate) fn glob_match_simple(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    // Case-insensitive comparison
    let p = pattern.to_ascii_uppercase();
    let t = text.to_ascii_uppercase();
    if let Some(inner) = p.strip_prefix('*').and_then(|s| s.strip_suffix('*')) {
        t.contains(inner)
    } else if let Some(suffix) = p.strip_prefix('*') {
        t.ends_with(suffix)
    } else if let Some(prefix) = p.strip_suffix('*') {
        t.starts_with(prefix)
    } else {
        p == t
    }
}

/// Parse a KNN query from the query string.
///
/// Expected format: `*=>[KNN k @field $param_name]`
/// Returns: `Some((k, field_name, param_name))` if a KNN query is found.
pub(crate) fn parse_knn_query(query_str: &str) -> Option<(usize, String, String)> {
    // Look for the "=>[KNN ..." pattern
    let knn_start = query_str.find("=>[KNN")?;
    let bracket_content = &query_str[knn_start + 2..]; // skip "=>"
    let open = bracket_content.find('[')?;
    let close = bracket_content.find(']')?;
    let inner = bracket_content[open + 1..close].trim();

    // Parse: KNN k @field $param
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() < 4 || !parts[0].eq_ignore_ascii_case("KNN") {
        return None;
    }

    let k: usize = parts[1].parse().ok()?;
    let field = parts[2].strip_prefix('@')?.to_string();
    let param = parts[3].strip_prefix('$')?.to_string();

    Some((k, field, param))
}

/// Single-pass $param substitution that avoids cascading substitution bugs.
pub(crate) fn substitute_params(
    query: &str,
    params: &std::collections::HashMap<String, bytes::Bytes>,
) -> String {
    let mut result = String::with_capacity(query.len());
    let mut chars = query.char_indices().peekable();
    while let Some((i, ch)) = chars.next() {
        if ch == '$' {
            let start = i + 1;
            while let Some(&(_, c)) = chars.peek() {
                if c.is_alphanumeric() || c == '_' {
                    chars.next();
                } else {
                    break;
                }
            }
            let end = chars.peek().map(|&(i, _)| i).unwrap_or(query.len());
            let name = &query[start..end];
            if !name.is_empty() {
                if let Some(value) = params.get(name) {
                    result.push_str(&String::from_utf8_lossy(value));
                } else {
                    result.push('$');
                    result.push_str(name);
                }
            } else {
                result.push('$');
            }
        } else {
            result.push(ch);
        }
    }
    result
}
