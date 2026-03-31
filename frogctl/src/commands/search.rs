use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output, render_value, value_to_json};

#[derive(Subcommand, Debug)]
pub enum SearchCommand {
    /// Execute a search query
    Query {
        /// Index name
        index: String,

        /// Search query string
        query: String,

        /// Result offset
        #[arg(long, default_value_t = 0)]
        offset: u64,

        /// Number of results
        #[arg(long, default_value_t = 10)]
        num: u64,

        /// Sort by field
        #[arg(long)]
        sortby: Option<String>,

        /// Return only document IDs
        #[arg(long)]
        nocontent: bool,

        /// Include scores in output
        #[arg(long)]
        withscores: bool,
    },

    /// Create a new search index
    Create {
        /// Index name
        index: String,

        /// Key type: hash or json
        #[arg(long, default_value = "hash")]
        on: String,

        /// Key prefix to index
        #[arg(long)]
        prefix: Vec<String>,

        /// Schema definitions (field:type pairs, e.g. title:TEXT body:TEXT)
        #[arg(required = true, trailing_var_arg = true)]
        schema: Vec<String>,
    },

    /// Show index info
    Info {
        /// Index name
        index: String,
    },

    /// List all search indices
    List,

    /// Drop a search index
    Drop {
        /// Index name
        index: String,

        /// Also delete indexed documents
        #[arg(long)]
        dd: bool,
    },

    /// Run an aggregate query
    Aggregate {
        /// Index name
        index: String,

        /// Aggregate query string
        query: String,

        /// Additional arguments (GROUPBY, SORTBY, LIMIT, etc.)
        #[arg(trailing_var_arg = true)]
        extra: Vec<String>,
    },
}

// --- Renderable structs ---

#[derive(Debug, Serialize)]
struct SearchResult {
    total: i64,
    docs: Vec<SearchDoc>,
}

#[derive(Debug, Serialize)]
struct SearchDoc {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f64>,
    fields: Vec<(String, String)>,
}

impl Renderable for SearchResult {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = format!("{} results:\n\n", self.total);
        for doc in &self.docs {
            out.push_str(&format!("--- {} ---", doc.id));
            if let Some(score) = doc.score {
                out.push_str(&format!(" (score: {score:.4})"));
            }
            out.push('\n');
            for (k, v) in &doc.fields {
                let display = if v.len() > 80 {
                    format!("{}...", &v[..77])
                } else {
                    v.clone()
                };
                out.push_str(&format!("  {k}: {display}\n"));
            }
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

#[derive(Debug, Serialize)]
struct IndexInfo {
    fields: Vec<(String, String)>,
}

impl Renderable for IndexInfo {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = String::new();
        for (k, v) in &self.fields {
            out.push_str(&format!("{k}: {v}\n"));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        let map: serde_json::Map<String, serde_json::Value> = self
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        serde_json::Value::Object(map)
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

// --- Dispatch ---

pub async fn run(cmd: &SearchCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        SearchCommand::Query {
            index,
            query,
            offset,
            num,
            sortby,
            nocontent,
            withscores,
        } => {
            run_query(
                index,
                query,
                *offset,
                *num,
                sortby.as_deref(),
                *nocontent,
                *withscores,
                ctx,
            )
            .await
        }
        SearchCommand::Create {
            index,
            on,
            prefix,
            schema,
        } => run_create(index, on, prefix, schema, ctx).await,
        SearchCommand::Info { index } => run_info(index, ctx).await,
        SearchCommand::List => run_list(ctx).await,
        SearchCommand::Drop { index, dd } => run_drop(index, *dd, ctx).await,
        SearchCommand::Aggregate {
            index,
            query,
            extra,
        } => run_aggregate(index, query, extra, ctx).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_query(
    index: &str,
    query: &str,
    offset: u64,
    num: u64,
    sortby: Option<&str>,
    nocontent: bool,
    withscores: bool,
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let mut args: Vec<String> = vec![index.to_string(), query.to_string()];

    if nocontent {
        args.push("NOCONTENT".to_string());
    }
    if withscores {
        args.push("WITHSCORES".to_string());
    }
    if let Some(field) = sortby {
        args.push("SORTBY".to_string());
        args.push(field.to_string());
    }
    args.push("LIMIT".to_string());
    args.push(offset.to_string());
    args.push(num.to_string());

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let value = ctx
        .cmd_value("FT.SEARCH", &arg_refs)
        .await
        .context("FT.SEARCH failed")?;

    if matches!(
        ctx.global().output,
        crate::cli::OutputMode::Table | crate::cli::OutputMode::Raw
    ) {
        let result = parse_search_result(&value, withscores, nocontent);
        print_output(&result, ctx.global().output, ctx.global().no_color);
    } else {
        render_value(&value, ctx.global().output, ctx.global().no_color);
    }
    Ok(0)
}

fn parse_search_result(value: &redis::Value, withscores: bool, nocontent: bool) -> SearchResult {
    let mut total = 0i64;
    let mut docs = Vec::new();

    if let redis::Value::Array(items) = value {
        if let Some(redis::Value::Int(n)) = items.first() {
            total = *n;
        }

        let mut i = 1;
        while i < items.len() {
            let id = extract_string(&items[i]);
            i += 1;

            let score = if withscores && i < items.len() {
                let s = extract_string(&items[i]);
                i += 1;
                s.parse::<f64>().ok()
            } else {
                None
            };

            let fields = if !nocontent && i < items.len() {
                if let redis::Value::Array(pairs) = &items[i] {
                    let f = parse_field_pairs(pairs);
                    i += 1;
                    f
                } else {
                    i += 1;
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            docs.push(SearchDoc { id, score, fields });
        }
    }

    SearchResult { total, docs }
}

fn parse_field_pairs(items: &[redis::Value]) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut iter = items.iter();
    while let Some(key) = iter.next() {
        if let Some(val) = iter.next() {
            pairs.push((extract_string(key), extract_string(val)));
        }
    }
    pairs
}

async fn run_create(
    index: &str,
    on: &str,
    prefixes: &[String],
    schema: &[String],
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let mut args: Vec<String> = vec![index.to_string(), "ON".to_string(), on.to_uppercase()];

    if !prefixes.is_empty() {
        args.push("PREFIX".to_string());
        args.push(prefixes.len().to_string());
        for p in prefixes {
            args.push(p.clone());
        }
    }

    args.push("SCHEMA".to_string());
    for field_spec in schema {
        // Support field:type syntax
        if let Some((field, ftype)) = field_spec.split_once(':') {
            args.push(field.to_string());
            args.push(ftype.to_uppercase());
        } else {
            args.push(field_spec.clone());
        }
    }

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let value = ctx
        .cmd_value("FT.CREATE", &arg_refs)
        .await
        .context("FT.CREATE failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_info(index: &str, ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("FT.INFO", &[index])
        .await
        .context("FT.INFO failed")?;

    if matches!(
        ctx.global().output,
        crate::cli::OutputMode::Table | crate::cli::OutputMode::Raw
    ) {
        if let redis::Value::Array(items) = &value {
            let fields = parse_alternating_kv(items);
            let info = IndexInfo { fields };
            print_output(&info, ctx.global().output, ctx.global().no_color);
        } else {
            render_value(&value, ctx.global().output, ctx.global().no_color);
        }
    } else {
        let json = value_to_json(&value);
        println!("{}", serde_json::to_string_pretty(&json).unwrap());
    }
    Ok(0)
}

fn parse_alternating_kv(items: &[redis::Value]) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut iter = items.iter();
    while let Some(key) = iter.next() {
        if let Some(val) = iter.next() {
            let k = extract_string(key);
            let v = match val {
                redis::Value::Array(arr) => {
                    let parts: Vec<String> = arr.iter().map(extract_string).collect();
                    parts.join(", ")
                }
                _ => extract_string(val),
            };
            pairs.push((k, v));
        }
    }
    pairs
}

async fn run_list(ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("FT._LIST", &[])
        .await
        .context("FT._LIST failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_drop(index: &str, dd: bool, ctx: &mut ConnectionContext) -> Result<i32> {
    let args = if dd { vec![index, "DD"] } else { vec![index] };
    let value = ctx
        .cmd_value("FT.DROPINDEX", &args)
        .await
        .context("FT.DROPINDEX failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_aggregate(
    index: &str,
    query: &str,
    extra: &[String],
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let mut args: Vec<&str> = vec![index, query];
    for e in extra {
        args.push(e.as_str());
    }

    let value = ctx
        .cmd_value("FT.AGGREGATE", &args)
        .await
        .context("FT.AGGREGATE failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

fn extract_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(n) => n.to_string(),
        _ => String::new(),
    }
}
