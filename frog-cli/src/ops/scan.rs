use anyhow::{Context, Result};
use serde::Serialize;

use crate::util::{extract_int, extract_int_opt, extract_string, format_bytes};

/// Options for a keyspace scan operation.
pub struct ScanOpts<'a> {
    pub pattern: &'a str,
    pub key_type: Option<&'a str>,
    pub batch: u64,
    pub limit: Option<usize>,
    pub with_ttl: bool,
    pub with_type: bool,
    pub with_memory: bool,
}

/// Information about a single key from a scan.
#[derive(Debug, Clone, Serialize)]
pub struct KeyInfo {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<i64>,
}

/// Summary of a completed scan.
#[derive(Debug, Serialize)]
pub struct ScanSummary {
    pub keys: Vec<KeyInfo>,
    pub total_scanned: usize,
}

/// Scan the keyspace with optional enrichment (TYPE, TTL, MEMORY USAGE).
///
/// The `on_progress` callback is invoked after each batch with the total number
/// of keys collected so far.
pub async fn scan_keyspace(
    conn: &mut redis::aio::MultiplexedConnection,
    opts: &ScanOpts<'_>,
    on_progress: impl Fn(usize),
) -> Result<ScanSummary> {
    let limit = opts.limit.unwrap_or(usize::MAX);
    let enrich = opts.with_ttl || opts.with_type || opts.with_memory;

    let mut keys = Vec::new();
    let mut cursor: u64 = 0;

    loop {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor)
            .arg("MATCH")
            .arg(opts.pattern)
            .arg("COUNT")
            .arg(opts.batch);
        if let Some(kt) = opts.key_type {
            cmd.arg("TYPE").arg(kt);
        }

        let (new_cursor, batch_keys): (u64, Vec<String>) =
            cmd.query_async(conn).await.context("SCAN failed")?;

        if enrich && !batch_keys.is_empty() {
            let enriched = enrich_keys(conn, &batch_keys, opts).await?;
            for info in enriched {
                keys.push(info);
                if keys.len() >= limit {
                    break;
                }
            }
        } else {
            for key in &batch_keys {
                keys.push(KeyInfo {
                    key: key.clone(),
                    ttl: None,
                    key_type: None,
                    memory_bytes: None,
                });
                if keys.len() >= limit {
                    break;
                }
            }
        }

        on_progress(keys.len());

        cursor = new_cursor;
        if cursor == 0 || keys.len() >= limit {
            break;
        }
    }

    keys.truncate(limit);
    let total_scanned = keys.len();
    Ok(ScanSummary {
        keys,
        total_scanned,
    })
}

/// Pipeline TYPE/TTL/MEMORY USAGE queries for a batch of keys.
async fn enrich_keys(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    opts: &ScanOpts<'_>,
) -> Result<Vec<KeyInfo>> {
    let mut pipe = redis::pipe();
    for key in keys {
        if opts.with_type {
            pipe.cmd("TYPE").arg(key);
        }
        if opts.with_ttl {
            pipe.cmd("TTL").arg(key);
        }
        if opts.with_memory {
            pipe.cmd("MEMORY").arg("USAGE").arg(key);
        }
    }
    let results: Vec<redis::Value> = pipe
        .query_async(conn)
        .await
        .context("enrichment pipeline failed")?;

    let fields_per_key =
        opts.with_type as usize + opts.with_ttl as usize + opts.with_memory as usize;

    let mut infos = Vec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let base = i * fields_per_key;
        let mut idx = base;

        let key_type = if opts.with_type {
            let v = extract_string(&results[idx]);
            idx += 1;
            Some(v)
        } else {
            None
        };

        let ttl = if opts.with_ttl {
            let v = extract_int(&results[idx]);
            idx += 1;
            Some(v)
        } else {
            None
        };

        let memory_bytes = if opts.with_memory {
            let v = extract_int_opt(&results[idx]);
            let _ = idx;
            v
        } else {
            None
        };

        infos.push(KeyInfo {
            key: key.clone(),
            ttl,
            key_type,
            memory_bytes,
        });
    }

    Ok(infos)
}

/// Aggregate key info by type for keyspace summary.
#[derive(Debug, Serialize)]
pub struct KeyspaceTypeSummary {
    pub key_type: String,
    pub count: usize,
    pub total_memory: u64,
    pub avg_memory: u64,
}

/// Compute keyspace summary from scan results.
pub fn summarize_keyspace(keys: &[KeyInfo]) -> Vec<KeyspaceTypeSummary> {
    use std::collections::HashMap;

    let mut by_type: HashMap<String, (usize, u64)> = HashMap::new();
    for k in keys {
        let t = k.key_type.as_deref().unwrap_or("unknown").to_string();
        let mem = k.memory_bytes.unwrap_or(0).max(0) as u64;
        let entry = by_type.entry(t).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += mem;
    }

    let mut summaries: Vec<KeyspaceTypeSummary> = by_type
        .into_iter()
        .map(|(key_type, (count, total_memory))| KeyspaceTypeSummary {
            key_type,
            count,
            total_memory,
            avg_memory: if count > 0 {
                total_memory / count as u64
            } else {
                0
            },
        })
        .collect();
    summaries.sort_by(|a, b| b.count.cmp(&a.count));
    summaries
}

/// Find the top N largest keys per type from scan results.
pub fn find_bigkeys(keys: &[KeyInfo], top: usize) -> Vec<KeyInfo> {
    let mut sorted: Vec<&KeyInfo> = keys.iter().filter(|k| k.memory_bytes.is_some()).collect();
    sorted.sort_by(|a, b| {
        b.memory_bytes
            .unwrap_or(0)
            .cmp(&a.memory_bytes.unwrap_or(0))
    });

    // Group by type, take top N per type
    use std::collections::HashMap;
    let mut by_type: HashMap<&str, Vec<&KeyInfo>> = HashMap::new();
    for k in &sorted {
        let t = k.key_type.as_deref().unwrap_or("unknown");
        by_type.entry(t).or_default().push(k);
    }

    let mut result = Vec::new();
    for group in by_type.values() {
        for k in group.iter().take(top) {
            result.push((*k).clone());
        }
    }
    result.sort_by(|a, b| {
        b.memory_bytes
            .unwrap_or(0)
            .cmp(&a.memory_bytes.unwrap_or(0))
    });
    result
}

/// Format a KeyInfo for display.
pub fn format_key_info(k: &KeyInfo) -> String {
    let mem = k
        .memory_bytes
        .map(|m| format_bytes(m.max(0) as u64))
        .unwrap_or_else(|| "-".to_string());
    let t = k.key_type.as_deref().unwrap_or("-");
    format!("{:<40} {:<10} {}", k.key, t, mem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summarize_keyspace() {
        let keys = vec![
            KeyInfo {
                key: "a".into(),
                ttl: None,
                key_type: Some("string".into()),
                memory_bytes: Some(100),
            },
            KeyInfo {
                key: "b".into(),
                ttl: None,
                key_type: Some("string".into()),
                memory_bytes: Some(200),
            },
            KeyInfo {
                key: "c".into(),
                ttl: None,
                key_type: Some("hash".into()),
                memory_bytes: Some(500),
            },
        ];
        let summary = summarize_keyspace(&keys);
        assert_eq!(summary.len(), 2);
        let strings = summary.iter().find(|s| s.key_type == "string").unwrap();
        assert_eq!(strings.count, 2);
        assert_eq!(strings.total_memory, 300);
        assert_eq!(strings.avg_memory, 150);
    }

    #[test]
    fn test_find_bigkeys() {
        let keys = vec![
            KeyInfo {
                key: "small".into(),
                ttl: None,
                key_type: Some("string".into()),
                memory_bytes: Some(10),
            },
            KeyInfo {
                key: "big".into(),
                ttl: None,
                key_type: Some("string".into()),
                memory_bytes: Some(1000),
            },
            KeyInfo {
                key: "medium".into(),
                ttl: None,
                key_type: Some("string".into()),
                memory_bytes: Some(500),
            },
        ];
        let big = find_bigkeys(&keys, 1);
        assert_eq!(big.len(), 1);
        assert_eq!(big[0].key, "big");
    }
}
