use anyhow::{Context, Result};
use clap::Args;
use serde::Serialize;

use crate::cli::OutputMode;
use crate::connection::ConnectionContext;
use crate::info_parser::InfoResponse;
use crate::output::{Renderable, print_output};

#[derive(Args, Debug)]
pub struct HealthArgs {
    /// Check admin HTTP health endpoint
    #[arg(long)]
    pub admin: bool,

    /// Check liveness probe (metrics HTTP)
    #[arg(long)]
    pub live: bool,

    /// Check readiness probe (metrics HTTP)
    #[arg(long)]
    pub ready: bool,

    /// Fan-out health check across multiple nodes
    #[arg(long, num_args = 1..)]
    pub all: Option<Vec<String>>,

    /// Force JSON output
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResult {
    pub node: String,
    pub status: HealthStatus,
    pub version: Option<String>,
    pub role: Option<String>,
    pub uptime: Option<String>,
    pub memory: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
    Unreachable,
}

impl Renderable for HealthResult {
    fn render_table(&self, _no_color: bool) -> String {
        match self.status {
            HealthStatus::Unreachable => {
                let err = self.error.as_deref().unwrap_or("unknown error");
                format!("{}\nStatus: UNREACHABLE ({})\n", self.node, err)
            }
            HealthStatus::Unhealthy => {
                let err = self.error.as_deref().unwrap_or("unhealthy");
                format!("{}\nStatus: UNHEALTHY ({})\n", self.node, err)
            }
            HealthStatus::Healthy => {
                let version = self.version.as_deref().unwrap_or("unknown");
                let role = self.role.as_deref().unwrap_or("unknown");
                let uptime = self.uptime.as_deref().unwrap_or("unknown");
                let memory = self.memory.as_deref().unwrap_or("unknown");
                format!(
                    "frogdb {version} @ {}\nRole: {role} | Uptime: {uptime} | Memory: {memory}\nStatus: HEALTHY\n",
                    self.node
                )
            }
        }
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

#[derive(Serialize)]
struct FanOutResult {
    nodes: Vec<HealthResult>,
}

impl Renderable for FanOutResult {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = format!(
            "{:<22} {:<12} {:<10} {:<10} {}\n",
            "NODE", "STATUS", "ROLE", "UPTIME", "MEMORY"
        );
        for r in &self.nodes {
            let status = match r.status {
                HealthStatus::Healthy => "HEALTHY",
                HealthStatus::Unhealthy => "UNHEALTHY",
                HealthStatus::Unreachable => "UNREACHABLE",
            };
            let role = r.role.as_deref().unwrap_or("-");
            let uptime = r.uptime.as_deref().unwrap_or("-");
            let memory = r.memory.as_deref().unwrap_or("-");
            out.push_str(&format!(
                "{:<22} {:<12} {:<10} {:<10} {}\n",
                r.node, status, role, uptime, memory
            ));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.nodes).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

pub async fn run(args: &HealthArgs, ctx: &mut ConnectionContext) -> Result<i32> {
    let output_mode = if args.json {
        OutputMode::Json
    } else {
        ctx.global().output
    };
    let no_color = ctx.global().no_color;

    // Fan-out mode
    if let Some(ref addrs) = args.all {
        return run_fanout(addrs, ctx, output_mode, no_color).await;
    }

    // Liveness probe
    if args.live {
        return run_probe(ctx, "/health/live", "liveness", output_mode, no_color).await;
    }

    // Readiness probe
    if args.ready {
        return run_probe(ctx, "/health/ready", "readiness", output_mode, no_color).await;
    }

    // Admin HTTP health
    if args.admin {
        return run_admin_health(ctx, output_mode, no_color).await;
    }

    // Default: RESP health check
    run_resp_health(ctx, output_mode, no_color).await
}

async fn run_resp_health(
    ctx: &mut ConnectionContext,
    output_mode: OutputMode,
    no_color: bool,
) -> Result<i32> {
    let node = format!("{}:{}", ctx.global().host, ctx.global().port);

    match check_node_health(ctx).await {
        Ok(result) => {
            let exit_code = status_to_exit(result.status);
            print_output(&result, output_mode, no_color);
            Ok(exit_code)
        }
        Err(e) => {
            let result = HealthResult {
                node,
                status: HealthStatus::Unreachable,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: Some(e.to_string()),
            };
            print_output(&result, output_mode, no_color);
            Ok(2)
        }
    }
}

async fn check_node_health(ctx: &mut ConnectionContext) -> Result<HealthResult> {
    let node = format!("{}:{}", ctx.global().host, ctx.global().port);

    // PING
    ctx.ping().await.context("PING failed")?;

    // INFO server + memory
    let raw = ctx
        .info(&["server", "memory"])
        .await
        .context("INFO failed")?;
    let info = InfoResponse::parse(&raw);

    let version = info
        .get("server", "frogdb_version")
        .unwrap_or("unknown")
        .to_string();
    let role = info
        .get("replication", "role")
        .unwrap_or("master")
        .to_string();

    let uptime_secs: u64 = info.get_parsed("server", "uptime_in_seconds").unwrap_or(0);
    let uptime = format_uptime(uptime_secs);

    let used: u64 = info.get_parsed("memory", "used_memory").unwrap_or(0);
    let max: u64 = info.get_parsed("memory", "maxmemory").unwrap_or(0);
    let memory = format_memory(used, max);

    // Also fetch replication info for role (server section doesn't include it)
    let raw_repl = ctx.info(&["replication"]).await.unwrap_or_default();
    let repl_info = InfoResponse::parse(&raw_repl);
    let role = repl_info
        .get("replication", "role")
        .map(|r| if r == "slave" { "replica" } else { r })
        .unwrap_or(&role)
        .to_string();

    Ok(HealthResult {
        node,
        status: HealthStatus::Healthy,
        version: Some(version),
        role: Some(role),
        uptime: Some(uptime),
        memory: Some(memory),
        error: None,
    })
}

async fn check_remote_health(ctx: &ConnectionContext, addr: &str) -> HealthResult {
    let result = async {
        let mut conn = ctx.resp_to(addr).await?;

        // PING
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;

        // INFO
        let raw: String = redis::cmd("INFO")
            .arg("server")
            .arg("memory")
            .arg("replication")
            .query_async(&mut conn)
            .await?;
        let info = InfoResponse::parse(&raw);

        let version = info
            .get("server", "frogdb_version")
            .unwrap_or("unknown")
            .to_string();
        let role = info
            .get("replication", "role")
            .map(|r| if r == "slave" { "replica" } else { r })
            .unwrap_or("master")
            .to_string();
        let uptime_secs: u64 = info.get_parsed("server", "uptime_in_seconds").unwrap_or(0);
        let used: u64 = info.get_parsed("memory", "used_memory").unwrap_or(0);
        let max: u64 = info.get_parsed("memory", "maxmemory").unwrap_or(0);

        Ok::<HealthResult, anyhow::Error>(HealthResult {
            node: addr.to_string(),
            status: HealthStatus::Healthy,
            version: Some(version),
            role: Some(role),
            uptime: Some(format_uptime(uptime_secs)),
            memory: Some(format_memory(used, max)),
            error: None,
        })
    }
    .await;

    result.unwrap_or_else(|e| HealthResult {
        node: addr.to_string(),
        status: HealthStatus::Unreachable,
        version: None,
        role: None,
        uptime: None,
        memory: None,
        error: Some(e.to_string()),
    })
}

async fn run_fanout(
    addrs: &[String],
    ctx: &ConnectionContext,
    output_mode: OutputMode,
    no_color: bool,
) -> Result<i32> {
    let futures: Vec<_> = addrs
        .iter()
        .map(|addr| check_remote_health(ctx, addr))
        .collect();

    let results = futures::future::join_all(futures).await;

    let worst = results
        .iter()
        .map(|r| r.status)
        .max()
        .unwrap_or(HealthStatus::Healthy);

    let fan_out = FanOutResult { nodes: results };
    print_output(&fan_out, output_mode, no_color);
    Ok(status_to_exit(worst))
}

async fn run_probe(
    ctx: &ConnectionContext,
    path: &str,
    name: &str,
    output_mode: OutputMode,
    no_color: bool,
) -> Result<i32> {
    let node = format!("{}:{}", ctx.global().host, ctx.global().port);
    match ctx.metrics_get(path).await {
        Ok(resp) if resp.status().is_success() => {
            let result = HealthResult {
                node,
                status: HealthStatus::Healthy,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: None,
            };
            print_output(&result, output_mode, no_color);
            Ok(0)
        }
        Ok(resp) => {
            let result = HealthResult {
                node,
                status: HealthStatus::Unhealthy,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: Some(format!("{name} probe returned {}", resp.status())),
            };
            print_output(&result, output_mode, no_color);
            Ok(1)
        }
        Err(e) => {
            let result = HealthResult {
                node,
                status: HealthStatus::Unreachable,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: Some(e.to_string()),
            };
            print_output(&result, output_mode, no_color);
            Ok(2)
        }
    }
}

async fn run_admin_health(
    ctx: &ConnectionContext,
    output_mode: OutputMode,
    no_color: bool,
) -> Result<i32> {
    let node = format!("{}:{}", ctx.global().host, ctx.global().port);
    match ctx.admin_get("/admin/health").await {
        Ok(resp) if resp.status().is_success() => {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let status_str = body
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let health_status = if status_str == "ok" {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            };
            let result = HealthResult {
                node,
                status: health_status,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: if health_status == HealthStatus::Unhealthy {
                    Some(format!("admin status: {status_str}"))
                } else {
                    None
                },
            };
            print_output(&result, output_mode, no_color);
            Ok(status_to_exit(health_status))
        }
        Ok(resp) => {
            let result = HealthResult {
                node,
                status: HealthStatus::Unhealthy,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: Some(format!("admin health returned {}", resp.status())),
            };
            print_output(&result, output_mode, no_color);
            Ok(1)
        }
        Err(e) => {
            let result = HealthResult {
                node,
                status: HealthStatus::Unreachable,
                version: None,
                role: None,
                uptime: None,
                memory: None,
                error: Some(e.to_string()),
            };
            print_output(&result, output_mode, no_color);
            Ok(2)
        }
    }
}

fn status_to_exit(status: HealthStatus) -> i32 {
    match status {
        HealthStatus::Healthy => 0,
        HealthStatus::Unhealthy => 1,
        HealthStatus::Unreachable => 2,
    }
}

fn format_uptime(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    format!("{days}d {hours}h")
}

fn format_memory(used: u64, max: u64) -> String {
    let used_str = format_bytes(used);
    if max == 0 {
        used_str
    } else {
        let pct = if max > 0 {
            (used as f64 / max as f64) * 100.0
        } else {
            0.0
        };
        let max_str = format_bytes(max);
        format!("{used_str}/{max_str} ({pct:.0}%)")
    }
}

fn format_bytes(bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes}B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uptime() {
        assert_eq!(format_uptime(0), "0d 0h");
        assert_eq!(format_uptime(3600), "0d 1h");
        assert_eq!(format_uptime(86400 + 3600 * 5), "1d 5h");
        assert_eq!(format_uptime(86400 * 3 + 3600 * 12), "3d 12h");
    }

    #[test]
    fn test_format_memory() {
        assert_eq!(format_memory(0, 0), "0B");
        assert_eq!(format_memory(1024, 0), "1.0KB");
        assert_eq!(
            format_memory(2 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024),
            "2.0GB/8.0GB (25%)"
        );
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(1536), "1.5KB");
        assert_eq!(format_bytes(1048576), "1.0MB");
        assert_eq!(format_bytes(1073741824), "1.0GB");
    }
}
