use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::info_parser::InfoResponse;
use crate::output::{Renderable, print_output};
use crate::util::format_unix_time;

#[derive(Subcommand, Debug)]
pub enum BackupCommand {
    /// Trigger a background save (snapshot)
    Trigger,

    /// Check snapshot/persistence status
    Status,

    /// Export the entire dataset to a portable format
    Export {
        /// Output directory
        #[arg(short, long)]
        output: PathBuf,

        /// SCAN pattern filter
        #[arg(long, name = "match")]
        match_pattern: Option<String>,

        /// SCAN batch size
        #[arg(long, default_value_t = 1000)]
        count: u64,

        /// Filter by data type
        #[arg(long, name = "type")]
        key_type: Option<String>,
    },

    /// Import a previously exported dataset
    Import {
        /// Input directory
        #[arg(short, long)]
        input: PathBuf,

        /// Overwrite existing keys
        #[arg(long)]
        replace: bool,

        /// RESTORE pipeline depth
        #[arg(long, default_value_t = 64)]
        pipeline: u64,

        /// Preserve original TTLs
        #[arg(long, default_value_t = true)]
        ttl: bool,
    },

    /// Verify integrity of an export archive
    Verify {
        /// Directory to verify
        dir: PathBuf,
    },
}

#[derive(Debug, Serialize)]
struct PersistenceStatus {
    rdb_bgsave_in_progress: bool,
    rdb_last_save_time: i64,
    rdb_last_bgsave_status: String,
    rdb_last_bgsave_time_sec: i64,
    aof_enabled: bool,
    aof_rewrite_in_progress: bool,
    aof_last_rewrite_status: String,
}

impl Renderable for PersistenceStatus {
    fn render_table(&self, _no_color: bool) -> String {
        let save_time = if self.rdb_last_save_time > 0 {
            format_unix_time(self.rdb_last_save_time)
        } else {
            "never".to_string()
        };

        let mut out = String::from("RDB Persistence:\n");
        out.push_str(&format!("  Last Save: {save_time}\n"));
        out.push_str(&format!("  Last Status: {}\n", self.rdb_last_bgsave_status));
        out.push_str(&format!(
            "  BGSAVE In Progress: {}\n",
            if self.rdb_bgsave_in_progress {
                "yes"
            } else {
                "no"
            }
        ));
        if self.rdb_last_bgsave_time_sec >= 0 {
            out.push_str(&format!(
                "  Last BGSAVE Duration: {}s\n",
                self.rdb_last_bgsave_time_sec
            ));
        }
        out.push_str("\nAOF/WAL Persistence:\n");
        out.push_str(&format!(
            "  Enabled: {}\n",
            if self.aof_enabled { "yes" } else { "no" }
        ));
        out.push_str(&format!(
            "  Rewrite In Progress: {}\n",
            if self.aof_rewrite_in_progress {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!(
            "  Last Rewrite Status: {}\n",
            self.aof_last_rewrite_status
        ));
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

pub async fn run(cmd: &BackupCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        BackupCommand::Trigger => run_trigger(ctx).await,
        BackupCommand::Status => run_status(ctx).await,
        BackupCommand::Export { .. } => {
            anyhow::bail!("frog backup export: not yet implemented")
        }
        BackupCommand::Import { .. } => {
            anyhow::bail!("frog backup import: not yet implemented")
        }
        BackupCommand::Verify { .. } => {
            anyhow::bail!("frog backup verify: not yet implemented")
        }
    }
}

async fn run_trigger(ctx: &mut ConnectionContext) -> Result<i32> {
    let result = ctx.cmd("BGSAVE", &[]).await.context("BGSAVE failed")?;
    println!("{result}");
    Ok(0)
}

async fn run_status(ctx: &mut ConnectionContext) -> Result<i32> {
    let raw = ctx
        .info(&["persistence"])
        .await
        .context("INFO persistence failed")?;
    let info = InfoResponse::parse(&raw);

    let status = PersistenceStatus {
        rdb_bgsave_in_progress: info
            .get_parsed::<u64>("persistence", "rdb_bgsave_in_progress")
            .unwrap_or(0)
            != 0,
        rdb_last_save_time: info
            .get_parsed("persistence", "rdb_last_save_time")
            .unwrap_or(0),
        rdb_last_bgsave_status: info
            .get("persistence", "rdb_last_bgsave_status")
            .unwrap_or("unknown")
            .to_string(),
        rdb_last_bgsave_time_sec: info
            .get_parsed("persistence", "rdb_last_bgsave_time_sec")
            .unwrap_or(-1),
        aof_enabled: info
            .get_parsed::<u64>("persistence", "aof_enabled")
            .unwrap_or(0)
            != 0,
        aof_rewrite_in_progress: info
            .get_parsed::<u64>("persistence", "aof_rewrite_in_progress")
            .unwrap_or(0)
            != 0,
        aof_last_rewrite_status: info
            .get("persistence", "aof_last_rewrite_status")
            .unwrap_or("unknown")
            .to_string(),
    };

    print_output(&status, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

