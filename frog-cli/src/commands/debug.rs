use std::io::{IsTerminal, Write, stdout};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::info_parser::InfoResponse;
use crate::output::{Renderable, print_output};
use crate::util::{extract_command, extract_int, format_duration_us, format_unix_time};

#[derive(Subcommand, Debug)]
pub enum DebugCommand {
    /// Collect a diagnostic bundle into a ZIP archive
    Zip {
        /// Collect from multiple nodes
        #[arg(long, num_args = 1..)]
        nodes: Option<Vec<String>>,

        /// Output file path
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Redact passwords and tokens from output
        #[arg(long)]
        redact: bool,
    },

    /// Continuous PING round-trip latency measurement
    Latency {
        #[command(subcommand)]
        subcommand: Option<LatencySubcommand>,

        /// Number of PINGs per measurement
        #[arg(long, default_value_t = 1000)]
        samples: u64,

        /// Delay between PINGs in ms
        #[arg(long, default_value_t = 0)]
        interval: u64,

        /// Periodic snapshot mode (every 15s)
        #[arg(long)]
        history: bool,

        /// Show ASCII latency distribution histogram
        #[arg(long)]
        dist: bool,
    },

    /// Memory diagnostics
    Memory {
        #[command(subcommand)]
        subcommand: MemorySubcommand,
    },

    /// Hot shard analysis
    Hotshards {
        /// Continuously refresh
        #[arg(long)]
        watch: bool,

        /// Refresh interval in ms
        #[arg(long, default_value_t = 2000)]
        interval: u64,

        /// Server-side stats collection period in seconds
        #[arg(long, default_value_t = 10)]
        period: u64,

        /// Fan-out across multiple nodes
        #[arg(long, num_args = 1..)]
        all: Option<Vec<String>>,
    },

    /// Inspect and analyze the slow query log
    Slowlog {
        /// Number of entries to fetch
        #[arg(long)]
        count: Option<u64>,

        /// Aggregate analysis by command
        #[arg(long)]
        analyze: bool,

        /// Collect from multiple nodes
        #[arg(long, num_args = 1..)]
        all: Option<Vec<String>>,

        /// Clear the slow log after reading
        #[arg(long)]
        reset: bool,
    },

    /// VLL queue inspection
    Vll {
        /// Show detailed queue for specific shard
        #[arg(long)]
        shard: Option<u64>,

        /// Continuously refresh
        #[arg(long)]
        watch: bool,
    },

    /// Formatted display of all client connections
    Connections {
        /// Sort by field: idle, omem, age
        #[arg(long)]
        sort: Option<String>,

        /// Filter expression (e.g. idle>300, flags=b)
        #[arg(long)]
        filter: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum LatencySubcommand {
    /// Fetch server-side latency analysis
    Doctor,

    /// Fetch server-side latency history and render an ASCII graph
    Graph {
        /// Event name
        event: String,
    },

    /// Fetch per-command latency histograms
    Histogram {
        /// Command names (empty = all)
        #[arg(num_args = 0..)]
        commands: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum MemorySubcommand {
    /// Formatted display of MEMORY STATS
    Stats,

    /// MEMORY DOCTOR with shard analysis
    Doctor,

    /// Scan keyspace for the largest keys
    Bigkeys {
        /// Filter by data type
        #[arg(long, name = "type")]
        key_type: Option<String>,

        /// Show top N keys per type
        #[arg(long, default_value_t = 1)]
        top: u64,

        /// SCAN sample count (0 = full scan)
        #[arg(long, default_value_t = 0)]
        samples: u64,
    },

    /// Scan every key with MEMORY USAGE and report summary
    Memkeys,
}

// --- Latency ---

#[derive(Debug, Serialize)]
struct LatencyResult {
    samples: u64,
    min_us: u64,
    max_us: u64,
    avg_us: u64,
    p50_us: u64,
    p99_us: u64,
}

impl Renderable for LatencyResult {
    fn render_table(&self, _no_color: bool) -> String {
        format!(
            "Latency ({} samples):\n  min: {}us\n  avg: {}us\n  p50: {}us\n  p99: {}us\n  max: {}us\n",
            self.samples, self.min_us, self.avg_us, self.p50_us, self.p99_us, self.max_us
        )
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

// --- Connections ---

#[derive(Debug, Serialize)]
pub(crate) struct ClientInfo {
    pub(crate) id: String,
    pub(crate) addr: String,
    pub(crate) name: String,
    pub(crate) age: String,
    pub(crate) idle: String,
    pub(crate) flags: String,
    pub(crate) db: String,
    pub(crate) cmd: String,
    pub(crate) omem: String,
}

#[derive(Debug, Serialize)]
struct ConnectionsResult {
    clients: Vec<ClientInfo>,
}

impl Renderable for ConnectionsResult {
    fn render_table(&self, _no_color: bool) -> String {
        if self.clients.is_empty() {
            return "No clients connected.\n".to_string();
        }
        let mut out = format!(
            "{:<6} {:<22} {:<12} {:<6} {:<6} {:<6} {:<4} {:<10} {}\n",
            "ID", "ADDR", "NAME", "AGE", "IDLE", "FLAGS", "DB", "CMD", "OMEM"
        );
        for c in &self.clients {
            out.push_str(&format!(
                "{:<6} {:<22} {:<12} {:<6} {:<6} {:<6} {:<4} {:<10} {}\n",
                c.id, c.addr, c.name, c.age, c.idle, c.flags, c.db, c.cmd, c.omem
            ));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.clients).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

// --- Slowlog ---

#[derive(Debug, Serialize)]
struct SlowlogEntry {
    id: i64,
    timestamp: i64,
    duration_us: i64,
    command: String,
}

#[derive(Debug, Serialize)]
struct SlowlogResult {
    entries: Vec<SlowlogEntry>,
}

impl Renderable for SlowlogResult {
    fn render_table(&self, _no_color: bool) -> String {
        if self.entries.is_empty() {
            return "Slow log is empty.\n".to_string();
        }
        let mut out = format!(
            "{:<8} {:<12} {:<22} {}\n",
            "ID", "DURATION", "TIME", "COMMAND"
        );
        for e in &self.entries {
            let duration = if e.duration_us >= 1_000_000 {
                format!("{:.2}s", e.duration_us as f64 / 1_000_000.0)
            } else if e.duration_us >= 1000 {
                format!("{:.1}ms", e.duration_us as f64 / 1000.0)
            } else {
                format!("{}us", e.duration_us)
            };
            let time = format_unix_time(e.timestamp);
            let cmd_display = if e.command.len() > 60 {
                format!("{}...", &e.command[..57])
            } else {
                e.command.clone()
            };
            out.push_str(&format!(
                "{:<8} {:<12} {:<22} {}\n",
                e.id, duration, time, cmd_display
            ));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.entries).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

// --- Slowlog Analysis ---

#[derive(Debug, Serialize)]
struct SlowlogAnalysis {
    entries: Vec<SlowlogAnalysisEntry>,
    total_calls: usize,
    total_duration_us: i64,
}

#[derive(Debug, Serialize)]
struct SlowlogAnalysisEntry {
    command: String,
    count: usize,
    total_us: i64,
    avg_us: i64,
    max_us: i64,
}

impl Renderable for SlowlogAnalysis {
    fn render_table(&self, _no_color: bool) -> String {
        if self.entries.is_empty() {
            return "Slow log is empty.\n".to_string();
        }
        let mut out = format!(
            "Slow log analysis ({} entries, total {}ms):\n\n",
            self.total_calls,
            self.total_duration_us / 1000
        );
        out.push_str(&format!(
            "{:<16} {:<8} {:<12} {:<12} {}\n",
            "COMMAND", "COUNT", "TOTAL", "AVG", "MAX"
        ));
        for e in &self.entries {
            out.push_str(&format!(
                "{:<16} {:<8} {:<12} {:<12} {}\n",
                e.command,
                e.count,
                format_duration_us(e.total_us),
                format_duration_us(e.avg_us),
                format_duration_us(e.max_us),
            ));
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

// --- Memory ---

#[derive(Debug, Serialize)]
struct MemoryStatus {
    used_memory_human: String,
    used_memory_peak_human: String,
    used_memory_rss_human: String,
    maxmemory_human: String,
    maxmemory_policy: String,
    mem_fragmentation_ratio: String,
    mem_allocator: String,
}

impl Renderable for MemoryStatus {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = String::from("Memory:\n");
        out.push_str(&format!("  Used: {}\n", self.used_memory_human));
        out.push_str(&format!("  Peak: {}\n", self.used_memory_peak_human));
        out.push_str(&format!("  RSS: {}\n", self.used_memory_rss_human));
        out.push_str(&format!("  Max: {}\n", self.maxmemory_human));
        out.push_str(&format!("  Policy: {}\n", self.maxmemory_policy));
        out.push_str(&format!(
            "  Fragmentation Ratio: {}\n",
            self.mem_fragmentation_ratio
        ));
        out.push_str(&format!("  Allocator: {}\n", self.mem_allocator));
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

// --- Dispatch ---

pub async fn run(cmd: &DebugCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        DebugCommand::Zip { .. } => {
            anyhow::bail!("frog debug zip: not yet implemented")
        }
        DebugCommand::Latency {
            subcommand,
            samples,
            interval,
            dist,
            ..
        } => match subcommand {
            None => run_latency(*samples, *interval, *dist, ctx).await,
            Some(LatencySubcommand::Doctor) => {
                anyhow::bail!("frog debug latency doctor: not yet implemented")
            }
            Some(LatencySubcommand::Graph { .. }) => {
                anyhow::bail!("frog debug latency graph: not yet implemented")
            }
            Some(LatencySubcommand::Histogram { .. }) => {
                anyhow::bail!("frog debug latency histogram: not yet implemented")
            }
        },
        DebugCommand::Memory { subcommand } => match subcommand {
            MemorySubcommand::Stats => run_memory_stats(ctx).await,
            MemorySubcommand::Doctor => run_memory_doctor(ctx).await,
            MemorySubcommand::Bigkeys { .. } => {
                anyhow::bail!("frog debug memory bigkeys: not yet implemented")
            }
            MemorySubcommand::Memkeys => {
                anyhow::bail!("frog debug memory memkeys: not yet implemented")
            }
        },
        DebugCommand::Hotshards { .. } => {
            anyhow::bail!("frog debug hotshards: not yet implemented")
        }
        DebugCommand::Slowlog {
            count,
            analyze,
            reset,
            ..
        } => run_slowlog(*count, *analyze, *reset, ctx).await,
        DebugCommand::Vll { .. } => {
            anyhow::bail!("frog debug vll: not yet implemented")
        }
        DebugCommand::Connections { sort, filter } => {
            run_connections(sort.as_deref(), filter.as_deref(), ctx).await
        }
    }
}

// --- Latency Implementation ---

async fn run_latency(
    samples: u64,
    interval_ms: u64,
    dist: bool,
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let is_tty = stdout().is_terminal();

    // Warm up connection
    ctx.ping().await.context("initial PING failed")?;

    let mut latencies = Vec::with_capacity(samples as usize);

    for i in 0..samples {
        if is_tty && i > 0 && i % 100 == 0 {
            print!("\r  Sampling... {i}/{samples}");
            stdout().flush()?;
        }

        let start = Instant::now();
        ctx.ping().await.context("PING failed during sampling")?;
        let elapsed = start.elapsed();
        latencies.push(elapsed.as_micros() as u64);

        if interval_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        }
    }

    if is_tty {
        print!("\r\x1b[2K");
        stdout().flush()?;
    }

    latencies.sort_unstable();

    let min = latencies[0];
    let max = latencies[latencies.len() - 1];
    let sum: u64 = latencies.iter().sum();
    let avg = sum / samples;
    let p50 = latencies[(samples as f64 * 0.50) as usize];
    let p99 = latencies[(samples as f64 * 0.99).min((samples - 1) as f64) as usize];

    let result = LatencyResult {
        samples,
        min_us: min,
        max_us: max,
        avg_us: avg,
        p50_us: p50,
        p99_us: p99,
    };
    print_output(&result, ctx.global().output, ctx.global().no_color);

    if dist {
        print_histogram(&latencies);
    }

    Ok(0)
}

fn print_histogram(latencies: &[u64]) {
    const BUCKETS: usize = 20;
    const BAR_WIDTH: usize = 40;

    let min = latencies[0];
    let max = latencies[latencies.len() - 1];

    if min == max {
        println!("\nAll samples: {min}us");
        return;
    }

    let range = max - min;
    let bucket_size = (range as f64 / BUCKETS as f64).ceil() as u64;
    let mut counts = [0u64; BUCKETS];

    for &v in latencies {
        let idx = ((v - min) / bucket_size.max(1)) as usize;
        counts[idx.min(BUCKETS - 1)] += 1;
    }

    let max_count = *counts.iter().max().unwrap_or(&1);

    println!("\nLatency Distribution:");
    for (i, &count) in counts.iter().enumerate() {
        let lo = min + (i as u64) * bucket_size;
        let hi = lo + bucket_size;
        let bar_len = if max_count > 0 {
            (count as f64 / max_count as f64 * BAR_WIDTH as f64) as usize
        } else {
            0
        };
        let bar: String = "#".repeat(bar_len);
        println!(
            "  {lo:>6}-{hi:<6}us |{bar:<width$}| {count}",
            width = BAR_WIDTH
        );
    }
}

// --- Connections Implementation ---

async fn run_connections(
    sort_by: Option<&str>,
    _filter: Option<&str>,
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let raw = ctx
        .cmd("CLIENT", &["LIST"])
        .await
        .context("CLIENT LIST failed")?;

    let mut clients: Vec<ClientInfo> = raw
        .lines()
        .filter(|l| !l.is_empty())
        .map(parse_client_line)
        .collect();

    if let Some(field) = sort_by {
        match field {
            "idle" => clients.sort_by_key(|c| c.idle.parse::<u64>().unwrap_or(0)),
            "age" => clients.sort_by_key(|c| c.age.parse::<u64>().unwrap_or(0)),
            "omem" => clients.sort_by_key(|c| c.omem.parse::<u64>().unwrap_or(0)),
            _ => {}
        }
        clients.reverse();
    }

    let result = ConnectionsResult { clients };
    print_output(&result, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

pub(crate) fn parse_client_line(line: &str) -> ClientInfo {
    let mut id = String::new();
    let mut addr = String::new();
    let mut name = String::new();
    let mut age = String::new();
    let mut idle = String::new();
    let mut flags = String::new();
    let mut db = String::new();
    let mut cmd = String::new();
    let mut omem = String::new();

    for part in line.split(' ') {
        if let Some((k, v)) = part.split_once('=') {
            match k {
                "id" => id = v.to_string(),
                "addr" => addr = v.to_string(),
                "name" => name = v.to_string(),
                "age" => age = v.to_string(),
                "idle" => idle = v.to_string(),
                "flags" => flags = v.to_string(),
                "db" => db = v.to_string(),
                "cmd" => cmd = v.to_string(),
                "omem" => omem = v.to_string(),
                _ => {}
            }
        }
    }

    ClientInfo {
        id,
        addr,
        name,
        age,
        idle,
        flags,
        db,
        cmd,
        omem,
    }
}

// --- Slowlog Implementation ---

async fn run_slowlog(
    count: Option<u64>,
    analyze: bool,
    reset: bool,
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let count_str = count.unwrap_or(128).to_string();
    let value = ctx
        .cmd_value("SLOWLOG", &["GET", &count_str])
        .await
        .context("SLOWLOG GET failed")?;

    let entries = parse_slowlog_value(&value);

    if analyze {
        let analysis = analyze_slowlog(&entries);
        print_output(&analysis, ctx.global().output, ctx.global().no_color);
    } else {
        let result = SlowlogResult { entries };
        print_output(&result, ctx.global().output, ctx.global().no_color);
    }

    if reset {
        ctx.cmd("SLOWLOG", &["RESET"])
            .await
            .context("SLOWLOG RESET failed")?;
        println!("Slow log cleared.");
    }

    Ok(0)
}

fn parse_slowlog_value(value: &redis::Value) -> Vec<SlowlogEntry> {
    let mut entries = Vec::new();
    if let redis::Value::Array(items) = value {
        for item in items {
            if let redis::Value::Array(parts) = item
                && parts.len() >= 4
            {
                let id = extract_int(&parts[0]);
                let timestamp = extract_int(&parts[1]);
                let duration_us = extract_int(&parts[2]);
                let command = extract_command(&parts[3]);
                entries.push(SlowlogEntry {
                    id,
                    timestamp,
                    duration_us,
                    command,
                });
            }
        }
    }
    entries
}

fn analyze_slowlog(entries: &[SlowlogEntry]) -> SlowlogAnalysis {
    use std::collections::HashMap;

    let mut by_cmd: HashMap<String, (usize, i64, i64)> = HashMap::new(); // (count, total_us, max_us)

    for e in entries {
        let cmd_name = e
            .command
            .split_whitespace()
            .next()
            .unwrap_or("?")
            .to_uppercase();
        let entry = by_cmd.entry(cmd_name).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += e.duration_us;
        entry.2 = entry.2.max(e.duration_us);
    }

    let mut analysis_entries: Vec<SlowlogAnalysisEntry> = by_cmd
        .into_iter()
        .map(
            |(command, (count, total_us, max_us))| SlowlogAnalysisEntry {
                command,
                count,
                total_us,
                avg_us: if count > 0 {
                    total_us / count as i64
                } else {
                    0
                },
                max_us,
            },
        )
        .collect();

    analysis_entries.sort_by(|a, b| b.total_us.cmp(&a.total_us));

    let total_calls = entries.len();
    let total_duration_us: i64 = entries.iter().map(|e| e.duration_us).sum();

    SlowlogAnalysis {
        entries: analysis_entries,
        total_calls,
        total_duration_us,
    }
}

// --- Memory Implementation ---

async fn run_memory_stats(ctx: &mut ConnectionContext) -> Result<i32> {
    let raw = ctx.info(&["memory"]).await.context("INFO memory failed")?;
    let info = InfoResponse::parse(&raw);

    let status = MemoryStatus {
        used_memory_human: info
            .get("memory", "used_memory_human")
            .unwrap_or("unknown")
            .to_string(),
        used_memory_peak_human: info
            .get("memory", "used_memory_peak_human")
            .unwrap_or("unknown")
            .to_string(),
        used_memory_rss_human: info
            .get("memory", "used_memory_rss_human")
            .unwrap_or("unknown")
            .to_string(),
        maxmemory_human: info
            .get("memory", "maxmemory_human")
            .unwrap_or("0B (no limit)")
            .to_string(),
        maxmemory_policy: info
            .get("memory", "maxmemory_policy")
            .unwrap_or("unknown")
            .to_string(),
        mem_fragmentation_ratio: info
            .get("memory", "mem_fragmentation_ratio")
            .unwrap_or("unknown")
            .to_string(),
        mem_allocator: info
            .get("memory", "mem_allocator")
            .unwrap_or("unknown")
            .to_string(),
    };

    print_output(&status, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_memory_doctor(ctx: &mut ConnectionContext) -> Result<i32> {
    let result = ctx
        .cmd("MEMORY", &["DOCTOR"])
        .await
        .context("MEMORY DOCTOR failed")?;
    println!("{result}");
    Ok(0)
}
