use anyhow::{Context, Result};
use clap::Args;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output};
use crate::util::{extract_int, extract_int_opt, extract_string, format_bytes};

#[derive(Args, Debug)]
pub struct ScanArgs {
    /// Glob pattern to match keys
    #[arg(long, name = "match", default_value = "*")]
    pub match_pattern: String,

    /// Filter by key type (string, list, set, zset, hash, stream)
    #[arg(long, name = "type")]
    pub key_type: Option<String>,

    /// Include TTL for each key
    #[arg(long)]
    pub with_ttl: bool,

    /// Include type for each key
    #[arg(long)]
    pub with_type: bool,

    /// Include memory usage for each key
    #[arg(long)]
    pub with_memory: bool,

    /// Maximum number of keys to return
    #[arg(long)]
    pub limit: Option<usize>,

    /// SCAN COUNT hint (batch size per iteration)
    #[arg(long, default_value_t = 100)]
    pub count: u64,
}

#[derive(Debug, Serialize)]
struct ScanEntry {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ScanResult {
    entries: Vec<ScanEntry>,
    total: usize,
}

impl Renderable for ScanResult {
    fn render_table(&self, _no_color: bool) -> String {
        if self.entries.is_empty() {
            return "No keys found.\n".to_string();
        }

        // Determine which columns to show
        let show_type = self.entries.iter().any(|e| e.key_type.is_some());
        let show_ttl = self.entries.iter().any(|e| e.ttl.is_some());
        let show_memory = self.entries.iter().any(|e| e.memory.is_some());

        let mut out = String::new();

        // Header
        out.push_str(&format!("{:<40}", "KEY"));
        if show_type {
            out.push_str(&format!(" {:<10}", "TYPE"));
        }
        if show_ttl {
            out.push_str(&format!(" {:<10}", "TTL"));
        }
        if show_memory {
            out.push_str(&format!(" {:<12}", "MEMORY"));
        }
        out.push('\n');

        for entry in &self.entries {
            let key_display = if entry.key.len() > 38 {
                format!("{}...", &entry.key[..35])
            } else {
                entry.key.clone()
            };
            out.push_str(&format!("{:<40}", key_display));

            if show_type {
                out.push_str(&format!(
                    " {:<10}",
                    entry.key_type.as_deref().unwrap_or("-")
                ));
            }
            if show_ttl {
                let ttl_str = match entry.ttl {
                    Some(-1) => "none".to_string(),
                    Some(-2) => "expired".to_string(),
                    Some(t) => format!("{t}s"),
                    None => "-".to_string(),
                };
                out.push_str(&format!(" {:<10}", ttl_str));
            }
            if show_memory {
                let mem_str = match entry.memory {
                    Some(m) if m >= 0 => format_bytes(m as u64),
                    _ => "-".to_string(),
                };
                out.push_str(&format!(" {:<12}", mem_str));
            }
            out.push('\n');
        }

        out.push_str(&format!("\n({} keys)\n", self.total));
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.entries
            .iter()
            .map(|e| e.key.clone())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    }
}

pub async fn run(args: &ScanArgs, ctx: &mut ConnectionContext) -> Result<i32> {
    let conn = ctx.resp().await?;
    let limit = args.limit.unwrap_or(usize::MAX);
    let enrich = args.with_ttl || args.with_type || args.with_memory;

    let mut entries = Vec::new();
    let mut cursor: u64 = 0;

    loop {
        // Build SCAN command
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor)
            .arg("MATCH")
            .arg(&args.match_pattern)
            .arg("COUNT")
            .arg(args.count);
        if let Some(ref kt) = args.key_type {
            cmd.arg("TYPE").arg(kt);
        }

        let (new_cursor, keys): (u64, Vec<String>) =
            cmd.query_async(conn).await.context("SCAN failed")?;

        if enrich && !keys.is_empty() {
            // Pipeline enrichment queries
            let mut pipe = redis::pipe();
            for key in &keys {
                if args.with_type {
                    pipe.cmd("TYPE").arg(key);
                }
                if args.with_ttl {
                    pipe.cmd("TTL").arg(key);
                }
                if args.with_memory {
                    pipe.cmd("MEMORY").arg("USAGE").arg(key);
                }
            }
            let results: Vec<redis::Value> = pipe
                .query_async(conn)
                .await
                .context("enrichment pipeline failed")?;

            let fields_per_key =
                args.with_type as usize + args.with_ttl as usize + args.with_memory as usize;
            for (i, key) in keys.iter().enumerate() {
                let base = i * fields_per_key;
                let mut idx = base;

                let key_type = if args.with_type {
                    let v = extract_string(&results[idx]);
                    idx += 1;
                    Some(v)
                } else {
                    None
                };

                let ttl = if args.with_ttl {
                    let v = extract_int(&results[idx]);
                    idx += 1;
                    Some(v)
                } else {
                    None
                };

                let memory = if args.with_memory {
                    let v = extract_int_opt(&results[idx]);
                    let _ = idx; // suppress unused warning
                    v
                } else {
                    None
                };

                entries.push(ScanEntry {
                    key: key.clone(),
                    ttl,
                    key_type,
                    memory,
                });

                if entries.len() >= limit {
                    break;
                }
            }
        } else {
            for key in &keys {
                entries.push(ScanEntry {
                    key: key.clone(),
                    ttl: None,
                    key_type: None,
                    memory: None,
                });
                if entries.len() >= limit {
                    break;
                }
            }
        }

        cursor = new_cursor;
        if cursor == 0 || entries.len() >= limit {
            break;
        }
    }

    entries.truncate(limit);
    let total = entries.len();

    let result = ScanResult { entries, total };
    print_output(&result, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

