use anyhow::{Context, Result};
use clap::Subcommand;
use serde::{Deserialize, Serialize};

use crate::connection::ConnectionContext;

#[derive(Subcommand, Debug)]
pub enum UpgradeCommand {
    /// Show the current version state of the cluster
    Status,

    /// Pre-flight compatibility and readiness checks
    Check {
        /// Target version to upgrade to
        #[arg(long)]
        target_version: String,
    },

    /// Generate an ordered upgrade plan
    Plan {
        /// Target version to upgrade to
        #[arg(long)]
        target_version: String,
    },

    /// Guided single-node upgrade
    Node {
        /// Node address (host:port)
        addr: String,

        /// Graceful shutdown timeout in seconds
        #[arg(long, default_value_t = 30)]
        shutdown_timeout: u64,

        /// Timeout waiting for node to rejoin in seconds
        #[arg(long, default_value_t = 120)]
        rejoin_timeout: u64,

        /// Skip confirmation prompts
        #[arg(long)]
        yes: bool,
    },

    /// Analyze whether rollback is safe
    Rollback,

    /// Finalize an upgrade (irreversible)
    Finalize {
        /// Version to finalize
        #[arg(long)]
        version: String,

        /// Skip interactive confirmation
        #[arg(long)]
        yes: bool,
    },
}

/// Version info returned by FROGDB.VERSION.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionInfo {
    binary_version: String,
    cluster_version: String,
    active_version: String,
}

/// Upgrade status from admin HTTP endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpgradeStatusResponse {
    binary_version: String,
    active_version: Option<String>,
    cluster_version: Option<String>,
    mixed_version: bool,
    nodes: Vec<NodeVersionInfo>,
    gated_features: Vec<GatedFeatureInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeVersionInfo {
    id: u64,
    addr: String,
    binary_version: String,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GatedFeatureInfo {
    name: String,
    min_version: String,
    description: String,
    active: bool,
}

/// Query FROGDB.VERSION via RESP and parse the response.
async fn query_version(ctx: &mut ConnectionContext) -> Result<VersionInfo> {
    let conn = ctx.resp().await?;
    let result: Vec<String> = redis::cmd("FROGDB.VERSION")
        .query_async(conn)
        .await
        .context("FROGDB.VERSION command failed")?;

    // Response is a flat array: [key, value, key, value, ...]
    let mut info = VersionInfo {
        binary_version: String::new(),
        cluster_version: String::new(),
        active_version: String::new(),
    };
    for chunk in result.chunks(2) {
        if chunk.len() == 2 {
            match chunk[0].as_str() {
                "binary_version" => info.binary_version = chunk[1].clone(),
                "cluster_version" => info.cluster_version = chunk[1].clone(),
                "active_version" => info.active_version = chunk[1].clone(),
                _ => {}
            }
        }
    }
    Ok(info)
}

pub async fn run(cmd: &UpgradeCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        UpgradeCommand::Status => run_status(ctx).await,
        UpgradeCommand::Check { target_version } => run_check(ctx, target_version).await,
        UpgradeCommand::Plan { target_version } => run_plan(ctx, target_version).await,
        UpgradeCommand::Node { .. } => {
            anyhow::bail!("frog upgrade node: not yet implemented")
        }
        UpgradeCommand::Rollback => run_rollback(ctx).await,
        UpgradeCommand::Finalize { version, yes } => run_finalize(ctx, version, *yes).await,
    }
}

async fn run_status(ctx: &mut ConnectionContext) -> Result<i32> {
    // Try admin HTTP first for richer info
    let resp = ctx.admin_get("/admin/upgrade-status").await;
    match resp {
        Ok(response) if response.status().is_success() => {
            let status: UpgradeStatusResponse = response.json().await?;
            println!("Cluster Version Status");
            println!();
            println!(
                "  Active Version:   {}",
                status.active_version.as_deref().unwrap_or("(none)")
            );
            println!(
                "  Cluster Version:  {}",
                status.cluster_version.as_deref().unwrap_or("(none)")
            );
            println!(
                "  Mixed-Version:    {}",
                if status.mixed_version {
                    "YES — upgrade in progress"
                } else {
                    "no"
                }
            );
            println!();

            if !status.nodes.is_empty() {
                println!("  {:<25} {:<18} {}", "NODE", "BINARY VERSION", "STATUS");
                for node in &status.nodes {
                    println!(
                        "  {:<25} {:<18} {}",
                        node.addr, node.binary_version, node.status
                    );
                }
                println!();
            }

            if !status.gated_features.is_empty() {
                println!("  Gated Features:");
                for feat in &status.gated_features {
                    let state = if feat.active { "active" } else { "blocked" };
                    println!("    {:<25} {} ({})", feat.name, feat.description, state);
                }
            } else {
                println!("  Gated Features: (none)");
            }

            Ok(0)
        }
        _ => {
            // Fall back to RESP FROGDB.VERSION
            let info = query_version(ctx).await?;
            println!("Cluster Version Status");
            println!();
            println!("  Binary Version:   {}", info.binary_version);
            println!("  Cluster Version:  {}", info.cluster_version);
            println!(
                "  Active Version:   {}",
                if info.active_version.is_empty() {
                    "(none)"
                } else {
                    &info.active_version
                }
            );
            Ok(0)
        }
    }
}

async fn run_check(ctx: &mut ConnectionContext, target_version: &str) -> Result<i32> {
    println!("Pre-flight Check: → {}", target_version);
    println!();

    let info = query_version(ctx).await?;
    let warnings = 0u32;
    let mut errors = 0u32;

    // Check: version jump supported
    if let (Ok(current), Ok(target)) = (
        semver::Version::parse(&info.cluster_version),
        semver::Version::parse(target_version),
    ) {
        if target.minor == current.minor + 1 && target.major == current.major {
            println!(
                "  ✓ Version jump supported ({} → {}, one minor)",
                info.cluster_version, target_version
            );
        } else if target.minor > current.minor + 1 || target.major != current.major {
            println!(
                "  ✗ Version jump too large ({} → {}, max one minor)",
                info.cluster_version, target_version
            );
            errors += 1;
        } else if target <= current {
            println!(
                "  ✗ Target version {} <= current {}",
                target_version, info.cluster_version
            );
            errors += 1;
        } else {
            // Patch-only upgrade, no finalization needed
            println!(
                "  ✓ Patch upgrade ({} → {}, no finalization needed)",
                info.cluster_version, target_version
            );
        }
    } else {
        println!("  ✗ Could not parse version(s)");
        errors += 1;
    }

    // Check: node reachable
    println!("  ✓ Node reachable (connected via RESP)");

    // Check: active version state
    if info.active_version.is_empty() {
        println!("  ✓ No active version (pre-versioning or standalone)");
    } else {
        println!("  ✓ Active version: {}", info.active_version);
    }

    println!();
    if errors > 0 {
        println!("  Result: BLOCKED ({} error(s))", errors);
        Ok(1)
    } else if warnings > 0 {
        println!("  Result: READY ({} warning(s))", warnings);
        Ok(0)
    } else {
        println!("  Result: READY");
        Ok(0)
    }
}

async fn run_plan(ctx: &mut ConnectionContext, target_version: &str) -> Result<i32> {
    let info = query_version(ctx).await?;
    println!(
        "Rolling Upgrade Plan: {} → {}",
        info.cluster_version, target_version
    );
    println!();

    // Try to get node list from admin HTTP
    let resp = ctx.admin_get("/admin/upgrade-status").await;
    match resp {
        Ok(response) if response.status().is_success() => {
            let status: UpgradeStatusResponse = response.json().await?;
            if status.nodes.is_empty() {
                println!("  Mode: Standalone");
                println!();
                println!("  1. Create a snapshot (BGSAVE or frog backup trigger)");
                println!("  2. Stop the server");
                println!("  3. Replace the binary with {}", target_version);
                println!("  4. Start the server");
                println!("  5. Verify health (frog health)");
                println!();
                println!("  No finalization needed for standalone mode.");
            } else {
                println!("  Mode: Cluster ({} nodes)", status.nodes.len());
                println!("  Estimated steps: {}", status.nodes.len() + 2);
                println!();
                println!(
                    "  {:<5} {:<30} {:<25} {}",
                    "STEP", "ACTION", "NODE", "NOTES"
                );
                for (i, node) in status.nodes.iter().enumerate() {
                    println!(
                        "  {:<5} {:<30} {:<25} {}",
                        i + 1,
                        "Upgrade node",
                        node.addr,
                        format!("current: {}", node.binary_version)
                    );
                }
                let step = status.nodes.len() + 1;
                println!(
                    "  {:<5} {:<30} {:<25} {}",
                    step, "Verify all nodes upgraded", "", "frog upgrade status"
                );
                println!(
                    "  {:<5} {:<30} {:<25} {}",
                    step + 1,
                    "Finalize",
                    "",
                    format!("frog upgrade finalize --version {}", target_version)
                );
            }
        }
        _ => {
            println!("  Mode: Standalone (admin HTTP unavailable)");
            println!();
            println!("  1. Create a snapshot");
            println!("  2. Stop the server, replace binary, restart");
            println!("  3. Verify health");
        }
    }

    Ok(0)
}

async fn run_rollback(ctx: &mut ConnectionContext) -> Result<i32> {
    let info = query_version(ctx).await?;
    println!("Rollback Analysis");
    println!();

    if info.active_version.is_empty() || info.active_version == info.cluster_version {
        println!(
            "  Active Version:  {} ({})",
            if info.active_version.is_empty() {
                "(none)"
            } else {
                &info.active_version
            },
            "not finalized to a newer version"
        );
        println!("  Status:          SAFE TO ROLLBACK");
        println!();
        println!("  No version-gated features have been activated.");
        println!("  You can safely stop upgraded nodes, replace with old binary, and restart.");
        Ok(0)
    } else {
        println!("  Active Version:  {} (FINALIZED)", info.active_version);
        println!("  Status:          UNSAFE — ROLLBACK NOT RECOMMENDED");
        println!();
        println!("  Finalization has occurred. Rolling back may cause:");
        println!("    ✗ Old nodes cannot read data written in new formats");
        println!("    ✗ Potential data corruption or read failures");
        println!();
        println!("  Options:");
        println!("    1. Fix forward — address the issue on the current version");
        println!("    2. Restore from pre-upgrade backup (data since finalization will be lost)");
        Ok(1)
    }
}

async fn run_finalize(ctx: &mut ConnectionContext, version: &str, yes: bool) -> Result<i32> {
    // Pre-check: get current version info
    let info = query_version(ctx).await?;

    println!("Finalize Upgrade to {}", version);
    println!();

    // Verify cluster version matches target
    if info.cluster_version != version {
        println!(
            "  ✗ Cluster version {} does not match target {}",
            info.cluster_version, version
        );
        println!("    Not all nodes may be upgraded yet. Run `frog upgrade status` to check.");
        return Ok(1);
    }

    println!("  ✓ Cluster version matches target ({})", version);

    // Warning
    println!();
    println!("  WARNING: Finalization is IRREVERSIBLE.");
    println!("  After finalization, rolling back to a previous version is NOT SAFE.");

    // Interactive confirmation
    if !yes {
        eprint!("\n  Type 'FINALIZE' to confirm: ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "FINALIZE" {
            println!("  Aborted.");
            return Ok(1);
        }
    }

    // Send FROGDB.FINALIZE command
    println!();
    println!("  Submitting FinalizeUpgrade...");
    let conn = ctx.resp().await?;
    let result: redis::RedisResult<String> = redis::cmd("FROGDB.FINALIZE")
        .arg(version)
        .query_async(conn)
        .await;

    match result {
        Ok(resp) => {
            println!("  ✓ Finalization complete: {}", resp);
            println!();
            println!("  Upgrade to {} complete.", version);
            Ok(0)
        }
        Err(e) => {
            println!("  ✗ Finalization failed: {}", e);
            Ok(1)
        }
    }
}
