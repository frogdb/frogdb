use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::info_parser::InfoResponse;
use crate::output::{Renderable, print_output};

#[derive(Subcommand, Debug)]
pub enum ReplicationCommand {
    /// Display replication status for the connected node
    Status,

    /// Per-replica replication lag monitoring
    Lag {
        /// Continuously refresh
        #[arg(long)]
        watch: bool,

        /// Refresh interval in ms
        #[arg(long, default_value_t = 1000)]
        interval: u64,

        /// Highlight replicas lagging more than threshold bytes
        #[arg(long)]
        threshold: Option<u64>,
    },

    /// Promote a replica to primary
    Promote {
        /// Replica address (host:port)
        addr: String,
    },

    /// ASCII tree of primary-replica relationships
    Topology,
}

#[derive(Debug, Serialize)]
struct ReplicaInfo {
    addr: String,
    state: String,
    offset: u64,
    lag: i64,
}

#[derive(Debug, Serialize)]
struct ReplicationStatus {
    role: String,
    connected_slaves: u64,
    master_repl_offset: u64,
    master_host: Option<String>,
    master_port: Option<u16>,
    master_link_status: Option<String>,
    slave_repl_offset: Option<u64>,
    replicas: Vec<ReplicaInfo>,
}

impl Renderable for ReplicationStatus {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = String::new();
        out.push_str(&format!("Role: {}\n", self.role));

        if self.role == "replica" || self.role == "slave" {
            if let Some(ref host) = self.master_host {
                let port = self.master_port.unwrap_or(6379);
                out.push_str(&format!("Master: {host}:{port}\n"));
            }
            if let Some(ref status) = self.master_link_status {
                out.push_str(&format!("Link Status: {status}\n"));
            }
            if let Some(offset) = self.slave_repl_offset {
                out.push_str(&format!("Slave Repl Offset: {offset}\n"));
            }
        }

        out.push_str(&format!(
            "Master Repl Offset: {}\n",
            self.master_repl_offset
        ));
        out.push_str(&format!("Connected Replicas: {}\n", self.connected_slaves));

        if !self.replicas.is_empty() {
            out.push_str(&format!(
                "\n{:<22} {:<10} {:<16} {}\n",
                "REPLICA", "STATE", "OFFSET", "LAG"
            ));
            for r in &self.replicas {
                out.push_str(&format!(
                    "{:<22} {:<10} {:<16} {}\n",
                    r.addr, r.state, r.offset, r.lag
                ));
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

pub async fn run(cmd: &ReplicationCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        ReplicationCommand::Status => run_status(ctx).await,
        ReplicationCommand::Lag { .. } => {
            anyhow::bail!("frog replication lag: not yet implemented")
        }
        ReplicationCommand::Promote { addr } => run_promote(addr, ctx).await,
        ReplicationCommand::Topology => run_topology(ctx).await,
    }
}

async fn run_status(ctx: &mut ConnectionContext) -> Result<i32> {
    let raw = ctx
        .info(&["replication"])
        .await
        .context("INFO replication failed")?;
    let info = InfoResponse::parse(&raw);

    let role = info
        .get("replication", "role")
        .map(|r| if r == "slave" { "replica" } else { r })
        .unwrap_or("unknown")
        .to_string();
    let connected_slaves: u64 = info
        .get_parsed("replication", "connected_slaves")
        .unwrap_or(0);
    let master_repl_offset: u64 = info
        .get_parsed("replication", "master_repl_offset")
        .unwrap_or(0);
    let master_host = info
        .get("replication", "master_host")
        .map(|s| s.to_string());
    let master_port: Option<u16> = info.get_parsed("replication", "master_port");
    let master_link_status = info
        .get("replication", "master_link_status")
        .map(|s| s.to_string());
    let slave_repl_offset: Option<u64> = info.get_parsed("replication", "slave_repl_offset");

    let mut replicas = Vec::new();
    for i in 0..connected_slaves {
        let key = format!("slave{i}");
        if let Some(val) = info.get("replication", &key)
            && let Some(replica) = parse_slave_info(val)
        {
            replicas.push(replica);
        }
    }

    let status = ReplicationStatus {
        role,
        connected_slaves,
        master_repl_offset,
        master_host,
        master_port,
        master_link_status,
        slave_repl_offset,
        replicas,
    };

    print_output(&status, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_promote(addr: &str, ctx: &mut ConnectionContext) -> Result<i32> {
    let mut conn = ctx.resp_to(addr).await?;
    let result: String = redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query_async(&mut conn)
        .await
        .context("REPLICAOF NO ONE failed")?;
    println!("{addr}: {result}");
    Ok(0)
}

async fn run_topology(ctx: &mut ConnectionContext) -> Result<i32> {
    let raw = ctx
        .info(&["replication", "server"])
        .await
        .context("INFO failed")?;
    let info = InfoResponse::parse(&raw);

    let role = info.get("replication", "role").unwrap_or("unknown");
    let host = &ctx.global().host;
    let port = ctx.global().port;
    let connected_slaves: u64 = info
        .get_parsed("replication", "connected_slaves")
        .unwrap_or(0);

    let display_role = if role == "slave" { "replica" } else { role };
    println!("{display_role} {host}:{port}");

    for i in 0..connected_slaves {
        let key = format!("slave{i}");
        if let Some(val) = info.get("replication", &key)
            && let Some(r) = parse_slave_info(val)
        {
            let connector = if i == connected_slaves - 1 {
                "\u{2514}\u{2500}\u{2500}"
            } else {
                "\u{251c}\u{2500}\u{2500}"
            };
            println!(
                " {connector} replica {} [state={}, offset={}, lag={}]",
                r.addr, r.state, r.offset, r.lag
            );
        }
    }

    Ok(0)
}

/// Parse `ip=...,port=...,state=...,offset=...,lag=...`
fn parse_slave_info(val: &str) -> Option<ReplicaInfo> {
    let mut ip = "";
    let mut port = "";
    let mut state = "unknown";
    let mut offset = 0u64;
    let mut lag = 0i64;

    for part in val.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            match k {
                "ip" => ip = v,
                "port" => port = v,
                "state" => state = v,
                "offset" => offset = v.parse().unwrap_or(0),
                "lag" => lag = v.parse().unwrap_or(0),
                _ => {}
            }
        }
    }

    if ip.is_empty() || port.is_empty() {
        return None;
    }

    Some(ReplicaInfo {
        addr: format!("{ip}:{port}"),
        state: state.to_string(),
        offset,
        lag,
    })
}
