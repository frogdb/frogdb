use std::collections::BTreeMap;

use anyhow::Context;
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output};

#[derive(Subcommand, Debug)]
pub enum ClusterCommand {
    /// Bootstrap a new cluster from standalone nodes
    Create {
        /// Node addresses (host:port)
        #[arg(required = true, num_args = 1..)]
        addrs: Vec<String>,

        /// Number of replicas per primary
        #[arg(long, default_value_t = 0)]
        replicas: u32,

        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },

    /// Display cluster summary
    Info,

    /// Validate cluster health invariants
    Check,

    /// Auto-repair common cluster issues
    Fix,

    /// Add a new node to the cluster
    AddNode {
        /// Node address (host:port)
        addr: String,

        /// Join as replica of specified primary
        #[arg(long)]
        replica_of: Option<String>,
    },

    /// Remove a node from the cluster
    DelNode {
        /// Node address (host:port)
        addr: String,

        /// Force-remove primary (slots become unassigned)
        #[arg(long)]
        force: bool,
    },

    /// Migrate slots between nodes
    Reshard {
        /// Source node
        #[arg(long)]
        from: String,

        /// Target node
        #[arg(long)]
        to: String,

        /// Number of slots to migrate
        #[arg(long)]
        slots: Option<u32>,

        /// Specific slot range to migrate (start-end)
        #[arg(long)]
        slot_range: Option<String>,

        /// Per-slot migration timeout in ms
        #[arg(long, default_value_t = 60000)]
        timeout: u64,
    },

    /// Redistribute slots proportionally across primaries
    Rebalance {
        /// Relative weight for node (addr=weight)
        #[arg(long)]
        weight: Vec<String>,

        /// Rebalance only if imbalance exceeds threshold percentage
        #[arg(long, default_value_t = 2.0)]
        threshold: f64,

        /// Use hot shard data to inform slot placement
        #[arg(long)]
        use_hot_shards: bool,

        /// Show planned migrations without executing
        #[arg(long)]
        dry_run: bool,

        /// Concurrent slot migrations
        #[arg(long, default_value_t = 1)]
        pipeline: u32,
    },

    /// Trigger manual failover
    Failover {
        /// Force failover (skip sync check)
        #[arg(long)]
        force: bool,

        /// Takeover without primary agreement
        #[arg(long)]
        takeover: bool,
    },

    /// ASCII tree visualization of cluster topology
    Topology,

    /// Dump the slot-to-node mapping
    Slots {
        /// Machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

/// One node as reported by a single `CLUSTER NODES` line.
///
/// Only the fields the consistency checks need are kept; the wire line carries
/// more (ping/pong timestamps, slot ranges) that no check reads yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterNodeEntry {
    /// 40-hex node id.
    pub id: String,
    /// Client address (`ip:port`, cluster-bus port stripped).
    pub addr: String,
    /// Flag tokens (`myself`, `master`, `slave`, `fail`, ...).
    pub flags: Vec<String>,
    /// The node's claimed configuration epoch. `0` means unassigned.
    pub config_epoch: u64,
}

impl ClusterNodeEntry {
    /// Whether this node is a primary (`master` flag).
    pub fn is_primary(&self) -> bool {
        self.flags.iter().any(|f| f == "master")
    }

    /// `<id> (<addr>)`, the form findings use to name a node.
    fn label(&self) -> String {
        format!("{} ({})", self.id, self.addr)
    }
}

/// Parse `CLUSTER NODES` output.
///
/// Wire format (one node per line, Redis-compatible):
/// `<id> <ip:port@cport> <flags> <primary-id> <ping> <pong> <config-epoch> <link-state> [slots...]`
///
/// Lines that do not carry at least the eight fixed fields are skipped rather
/// than failing the parse: a truncated or unknown trailing line must not stop
/// the checks from running over the nodes that did parse.
pub fn parse_cluster_nodes(text: &str) -> Vec<ClusterNodeEntry> {
    text.lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 8 {
                return None;
            }
            let addr = fields[1].split('@').next().unwrap_or(fields[1]).to_string();
            Some(ClusterNodeEntry {
                id: fields[0].to_string(),
                addr,
                flags: fields[2].split(',').map(|f| f.to_string()).collect(),
                config_epoch: fields[6].parse().unwrap_or(0),
            })
        })
        .collect()
}

/// Verdict of a single consistency check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CheckStatus {
    /// The invariant holds.
    Ok,
    /// The invariant is violated; `findings` says how.
    Failed,
}

/// The result of one consistency check.
#[derive(Debug, Clone, Serialize)]
pub struct CheckOutcome {
    /// Stable check identifier, e.g. `epoch-collision`.
    pub name: &'static str,
    /// Whether the invariant holds.
    pub status: CheckStatus,
    /// One-line verdict, shown whether the check passed or failed.
    pub summary: String,
    /// One entry per violation. Empty when the check passed.
    pub findings: Vec<String>,
}

impl CheckOutcome {
    fn ok(name: &'static str, summary: impl Into<String>) -> Self {
        Self {
            name,
            status: CheckStatus::Ok,
            summary: summary.into(),
            findings: Vec::new(),
        }
    }

    fn failed(name: &'static str, summary: impl Into<String>, findings: Vec<String>) -> Self {
        Self {
            name,
            status: CheckStatus::Failed,
            summary: summary.into(),
            findings,
        }
    }
}

/// Every consistency check `frogctl cluster check` runs, in report order.
///
/// Each check is a pure function of the parsed node table, so adding one (slot
/// coverage, open migrations, ...) means writing the function and listing it
/// here — the fan-out, rendering, and exit-code logic stay untouched.
const CHECKS: &[fn(&[ClusterNodeEntry]) -> CheckOutcome] = &[check_epoch_collisions];

/// Flag primaries that claim the same nonzero `config_epoch`.
///
/// A `config_epoch` arbitrates who owns a slot when two nodes disagree, so two
/// primaries holding the same one makes that arbitration undecidable — this is
/// the violation `redis-cli --cluster check` reports, and the one FrogDB's
/// `AddNode` transition prevents (see the cluster architecture docs).
///
/// Two exclusions, matching the state machine's resolution policy:
/// - `config_epoch == 0` means "unassigned" (a freshly bootstrapped node), so
///   any number of nodes may hold it.
/// - Replicas are ignored. Only a primary's epoch arbitrates slot ownership,
///   and Redis's own collision handling skips replicas for the same reason.
fn check_epoch_collisions(nodes: &[ClusterNodeEntry]) -> CheckOutcome {
    const NAME: &str = "epoch-collision";

    let mut by_epoch: BTreeMap<u64, Vec<&ClusterNodeEntry>> = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|n| n.is_primary() && n.config_epoch != 0)
    {
        by_epoch.entry(node.config_epoch).or_default().push(node);
    }

    let findings: Vec<String> = by_epoch
        .iter()
        .filter(|(_, claimants)| claimants.len() > 1)
        .map(|(epoch, claimants)| {
            let mut labels: Vec<String> = claimants.iter().map(|n| n.label()).collect();
            labels.sort();
            format!("config_epoch {} claimed by {}", epoch, labels.join(", "))
        })
        .collect();

    let primaries = nodes.iter().filter(|n| n.is_primary()).count();
    if findings.is_empty() {
        CheckOutcome::ok(
            NAME,
            format!("{} primaries hold distinct config epochs", primaries),
        )
    } else {
        CheckOutcome::failed(
            NAME,
            format!(
                "{} colliding config epoch(s) across {} primaries",
                findings.len(),
                primaries
            ),
            findings,
        )
    }
}

/// The full result of `frogctl cluster check`.
#[derive(Debug, Clone, Serialize)]
pub struct CheckReport {
    /// Outcome of every check in [`CHECKS`], in order.
    pub checks: Vec<CheckOutcome>,
}

impl CheckReport {
    /// Run every check against a parsed node table.
    pub fn run(nodes: &[ClusterNodeEntry]) -> Self {
        Self {
            checks: CHECKS.iter().map(|check| check(nodes)).collect(),
        }
    }

    /// Whether every check passed.
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.status == CheckStatus::Ok)
    }

    /// Process exit code: `0` when every check passed, `1` on any finding.
    pub fn exit_code(&self) -> i32 {
        if self.passed() { 0 } else { 1 }
    }
}

impl Renderable for CheckReport {
    fn render_table(&self, _no_color: bool) -> String {
        let failed = self
            .checks
            .iter()
            .filter(|c| c.status == CheckStatus::Failed)
            .count();

        let mut out = format!(
            "Cluster check: {} check(s), {} failed\n",
            self.checks.len(),
            failed
        );
        for check in &self.checks {
            let tag = match check.status {
                CheckStatus::Ok => "[OK]  ",
                CheckStatus::Failed => "[FAIL]",
            };
            out.push_str(&format!("{} {}: {}\n", tag, check.name, check.summary));
            for finding in &check.findings {
                out.push_str(&format!("       - {}\n", finding));
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

/// `frogctl cluster check` — read the cluster's node table and report every
/// invariant violation found. Exits nonzero when anything is reported.
async fn run_check(ctx: &mut ConnectionContext) -> anyhow::Result<i32> {
    let raw = ctx
        .cmd("CLUSTER", &["NODES"])
        .await
        .context("failed to read CLUSTER NODES")?;

    let nodes = parse_cluster_nodes(&raw);
    if nodes.is_empty() {
        anyhow::bail!("CLUSTER NODES returned no parseable nodes; cannot check the cluster");
    }

    let report = CheckReport::run(&nodes);
    print_output(&report, ctx.global().output, ctx.global().no_color);
    Ok(report.exit_code())
}

pub async fn run(cmd: &ClusterCommand, ctx: &mut ConnectionContext) -> anyhow::Result<i32> {
    match cmd {
        ClusterCommand::Create { .. } => {
            anyhow::bail!("frogctl cluster create: not yet implemented")
        }
        ClusterCommand::Info => {
            anyhow::bail!("frogctl cluster info: not yet implemented")
        }
        ClusterCommand::Check => run_check(ctx).await,
        ClusterCommand::Fix => {
            anyhow::bail!("frogctl cluster fix: not yet implemented")
        }
        ClusterCommand::AddNode { .. } => {
            anyhow::bail!("frogctl cluster add-node: not yet implemented")
        }
        ClusterCommand::DelNode { .. } => {
            anyhow::bail!("frogctl cluster del-node: not yet implemented")
        }
        ClusterCommand::Reshard { .. } => {
            anyhow::bail!("frogctl cluster reshard: not yet implemented")
        }
        ClusterCommand::Rebalance { .. } => {
            anyhow::bail!("frogctl cluster rebalance: not yet implemented")
        }
        ClusterCommand::Failover { .. } => {
            anyhow::bail!("frogctl cluster failover: not yet implemented")
        }
        ClusterCommand::Topology => {
            anyhow::bail!("frogctl cluster topology: not yet implemented")
        }
        ClusterCommand::Slots { .. } => {
            anyhow::bail!("frogctl cluster slots: not yet implemented")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODE_1: &str = "0000000000000000000000000000000000000001";
    const NODE_2: &str = "0000000000000000000000000000000000000002";

    /// Three primaries with distinct epochs, exactly as a healthy cluster
    /// renders them.
    const HEALTHY: &str = "\
0000000000000000000000000000000000000001 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460
0000000000000000000000000000000000000002 127.0.0.1:6380@16380 master - 0 0 2 connected 5461-10922
0000000000000000000000000000000000000003 127.0.0.1:6381@16381 master - 0 0 3 connected 10923-16383
";

    fn epoch_check(report: &CheckReport) -> &CheckOutcome {
        report
            .checks
            .iter()
            .find(|c| c.name == "epoch-collision")
            .expect("epoch-collision check must run")
    }

    #[test]
    fn parses_cluster_nodes_lines() {
        let nodes = parse_cluster_nodes(HEALTHY);
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].id, NODE_1);
        assert_eq!(nodes[0].addr, "127.0.0.1:6379", "bus port is stripped");
        assert!(nodes[0].flags.contains(&"myself".to_string()));
        assert!(nodes[0].is_primary());
        assert_eq!(nodes[0].config_epoch, 1);
    }

    #[test]
    fn skips_unparseable_lines() {
        let text = format!("{HEALTHY}garbage\n\n");
        assert_eq!(parse_cluster_nodes(&text).len(), 3);
    }

    #[test]
    fn healthy_cluster_passes_every_check() {
        let report = CheckReport::run(&parse_cluster_nodes(HEALTHY));
        assert!(report.passed());
        assert_eq!(report.exit_code(), 0);
        assert!(epoch_check(&report).findings.is_empty());
    }

    #[test]
    fn detects_two_primaries_sharing_an_epoch() {
        let text = "\
0000000000000000000000000000000000000001 127.0.0.1:6379@16379 myself,master - 0 0 7 connected 0-5460
0000000000000000000000000000000000000002 127.0.0.1:6380@16380 master - 0 0 7 connected 5461-10922
0000000000000000000000000000000000000003 127.0.0.1:6381@16381 master - 0 0 3 connected 10923-16383
";
        let report = CheckReport::run(&parse_cluster_nodes(text));
        assert!(!report.passed());
        assert_eq!(report.exit_code(), 1);

        let outcome = epoch_check(&report);
        assert_eq!(outcome.status, CheckStatus::Failed);
        assert_eq!(outcome.findings.len(), 1);
        let finding = &outcome.findings[0];
        assert!(finding.contains("config_epoch 7"), "got: {finding}");
        assert!(finding.contains(NODE_1), "got: {finding}");
        assert!(finding.contains(NODE_2), "got: {finding}");
        assert!(finding.contains("127.0.0.1:6380"), "got: {finding}");
    }

    /// Every colliding epoch is reported, not just the first one found.
    #[test]
    fn reports_each_colliding_epoch_separately() {
        let text = "\
0000000000000000000000000000000000000001 127.0.0.1:6379@16379 master - 0 0 7 connected 0-100
0000000000000000000000000000000000000002 127.0.0.1:6380@16380 master - 0 0 7 connected 101-200
0000000000000000000000000000000000000003 127.0.0.1:6381@16381 master - 0 0 9 connected 201-300
0000000000000000000000000000000000000004 127.0.0.1:6382@16382 master - 0 0 9 connected 301-400
";
        let report = CheckReport::run(&parse_cluster_nodes(text));
        let findings = &epoch_check(&report).findings;
        assert_eq!(findings.len(), 2);
        assert!(findings[0].contains("config_epoch 7"), "got: {findings:?}");
        assert!(findings[1].contains("config_epoch 9"), "got: {findings:?}");
    }

    /// `config_epoch 0` is the unassigned value every freshly bootstrapped node
    /// carries — shared zeros are normal, not a collision.
    #[test]
    fn zero_epoch_is_not_a_collision() {
        let text = "\
0000000000000000000000000000000000000001 127.0.0.1:6379@16379 myself,master - 0 0 0 connected 0-8191
0000000000000000000000000000000000000002 127.0.0.1:6380@16380 master - 0 0 0 connected 8192-16383
";
        assert!(CheckReport::run(&parse_cluster_nodes(text)).passed());
    }

    /// A replica sharing its primary's epoch is expected (only a primary's epoch
    /// arbitrates slot ownership), so it must not be reported.
    #[test]
    fn replica_sharing_primary_epoch_is_not_a_collision() {
        let text = "\
0000000000000000000000000000000000000001 127.0.0.1:6379@16379 myself,master - 0 0 4 connected 0-16383
0000000000000000000000000000000000000002 127.0.0.1:6380@16380 slave 0000000000000000000000000000000000000001 0 0 4 connected
";
        assert!(CheckReport::run(&parse_cluster_nodes(text)).passed());
    }

    #[test]
    fn failed_report_renders_the_colliding_pair() {
        let text = "\
0000000000000000000000000000000000000001 127.0.0.1:6379@16379 master - 0 0 7 connected 0-8191
0000000000000000000000000000000000000002 127.0.0.1:6380@16380 master - 0 0 7 connected 8192-16383
";
        let rendered = CheckReport::run(&parse_cluster_nodes(text)).render_table(true);
        assert!(
            rendered.contains("[FAIL] epoch-collision"),
            "got: {rendered}"
        );
        assert!(rendered.contains("config_epoch 7"), "got: {rendered}");
        assert!(rendered.contains("127.0.0.1:6379"), "got: {rendered}");
        assert!(rendered.contains("127.0.0.1:6380"), "got: {rendered}");
    }

    #[test]
    fn healthy_report_renders_an_ok_line() {
        let rendered = CheckReport::run(&parse_cluster_nodes(HEALTHY)).render_table(true);
        assert!(rendered.contains("[OK]"), "got: {rendered}");
        assert!(rendered.contains("epoch-collision"), "got: {rendered}");
    }
}
