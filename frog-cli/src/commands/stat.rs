use std::io::{IsTerminal, Write, stdout};

use anyhow::Result;
use clap::Args;

use crate::connection::ConnectionContext;
use crate::info_parser::InfoResponse;

#[derive(Args, Debug)]
pub struct StatArgs {
    /// Poll interval in milliseconds
    #[arg(long, default_value_t = 1000)]
    pub interval: u64,

    /// Exit after N samples
    #[arg(long)]
    pub count: Option<u64>,

    /// Suppress header line
    #[arg(long)]
    pub no_header: bool,
}

struct PrevStats {
    keyspace_hits: u64,
    keyspace_misses: u64,
}

pub async fn run(args: &StatArgs, ctx: &mut ConnectionContext) -> Result<i32> {
    let is_tty = stdout().is_terminal();
    let host = ctx.global().host.clone();
    let port = ctx.global().port;

    if !args.no_header {
        println!("--- {host}:{port} (every {}ms) ---", args.interval);
        println!(
            "{:<12} {:<12} {:<10} {:<10} {:<10} {:<10} {:<10}",
            "KEYS", "MEMORY", "CLIENTS", "OPS/SEC", "HIT RATE", "NET IN", "NET OUT"
        );
    }

    let mut prev: Option<PrevStats> = None;
    let mut samples = 0u64;

    loop {
        // Check for SIGINT
        let info_future = ctx.info(&["stats", "memory", "clients", "keyspace"]);

        let raw = tokio::select! {
            result = info_future => result?,
            _ = tokio::signal::ctrl_c() => break,
        };

        let info = InfoResponse::parse(&raw);

        let keys = parse_keyspace_keys(&info);
        let memory = info
            .get("memory", "used_memory_human")
            .unwrap_or("0B")
            .to_string();
        let clients: u64 = info.get_parsed("clients", "connected_clients").unwrap_or(0);
        let ops: u64 = info
            .get_parsed("stats", "instantaneous_ops_per_sec")
            .unwrap_or(0);
        let hits: u64 = info.get_parsed("stats", "keyspace_hits").unwrap_or(0);
        let misses: u64 = info.get_parsed("stats", "keyspace_misses").unwrap_or(0);
        let net_in: f64 = info
            .get_parsed("stats", "instantaneous_input_kbps")
            .unwrap_or(0.0);
        let net_out: f64 = info
            .get_parsed("stats", "instantaneous_output_kbps")
            .unwrap_or(0.0);

        let hit_rate = match &prev {
            Some(p) => {
                let delta_hits = hits.saturating_sub(p.keyspace_hits);
                let delta_misses = misses.saturating_sub(p.keyspace_misses);
                let total = delta_hits + delta_misses;
                if total > 0 {
                    format!("{:.1}%", (delta_hits as f64 / total as f64) * 100.0)
                } else {
                    "-".to_string()
                }
            }
            None => {
                // First sample: cumulative
                let total = hits + misses;
                if total > 0 {
                    format!("{:.1}%", (hits as f64 / total as f64) * 100.0)
                } else {
                    "-".to_string()
                }
            }
        };

        prev = Some(PrevStats {
            keyspace_hits: hits,
            keyspace_misses: misses,
        });

        let line = format!(
            "{:<12} {:<12} {:<10} {:<10} {:<10} {:<10} {:<10}",
            format_keys(keys),
            memory.trim(),
            clients,
            format_ops(ops),
            hit_rate,
            format_kbps(net_in),
            format_kbps(net_out),
        );

        if is_tty && samples > 0 {
            // Overwrite previous data line
            print!("\x1b[2K\r{line}");
            stdout().flush()?;
        } else {
            println!("{line}");
        }

        samples += 1;

        if let Some(count) = args.count
            && samples >= count
        {
            if is_tty {
                println!();
            }
            break;
        }

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(args.interval)) => {},
            _ = tokio::signal::ctrl_c() => {
                if is_tty {
                    println!();
                }
                break;
            }
        }
    }

    Ok(0)
}

/// Parse total key count from the keyspace section.
/// Format: `db0:keys=N,expires=M,avg_ttl=T`
fn parse_keyspace_keys(info: &InfoResponse) -> u64 {
    let db0 = match info.get("keyspace", "db0") {
        Some(v) => v,
        None => return 0,
    };
    for part in db0.split(',') {
        if let Some(val) = part.strip_prefix("keys=") {
            return val.parse().unwrap_or(0);
        }
    }
    0
}

fn format_keys(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_ops(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_kbps(kbps: f64) -> String {
    if kbps >= 1024.0 {
        format!("{:.1} MB/s", kbps / 1024.0)
    } else {
        format!("{:.1} KB/s", kbps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_keyspace_keys() {
        let raw = "# Keyspace\r\ndb0:keys=1000,expires=0,avg_ttl=0\r\n";
        let info = InfoResponse::parse(raw);
        assert_eq!(parse_keyspace_keys(&info), 1000);
    }

    #[test]
    fn test_parse_keyspace_empty() {
        let raw = "# Keyspace\r\n";
        let info = InfoResponse::parse(raw);
        assert_eq!(parse_keyspace_keys(&info), 0);
    }

    #[test]
    fn test_format_keys() {
        assert_eq!(format_keys(0), "0");
        assert_eq!(format_keys(500), "500");
        assert_eq!(format_keys(1500), "1.5K");
        assert_eq!(format_keys(1_500_000), "1.5M");
    }

    #[test]
    fn test_format_kbps() {
        assert_eq!(format_kbps(0.0), "0.0 KB/s");
        assert_eq!(format_kbps(512.0), "512.0 KB/s");
        assert_eq!(format_kbps(2048.0), "2.0 MB/s");
    }
}
