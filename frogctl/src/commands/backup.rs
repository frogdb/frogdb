use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Subcommand;
use frogdb_persistence::PayloadCheck;
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

    /// Verify a checkpoint payload on disk against the manifest shipped inside it
    ///
    /// Runs offline (no server connection) against a snapshot's `checkpoint/`
    /// directory, a staged `<data-dir>/staging/`, or a
    /// `<data-dir>/backup/db_backup_*` — the same
    /// check the boot-time install runs, so an operator can find out that a
    /// backup is unusable before the outage they need it for. Exits non-zero if
    /// the payload does not verify.
    CheckpointVerify {
        /// Directory holding the checkpoint payload
        dir: PathBuf,

        /// Check file sizes only, skipping the per-file checksum pass
        ///
        /// Sizes are what the install itself checks; checksums additionally
        /// catch bit-rot on long-lived backup media, at the cost of reading
        /// every byte.
        #[arg(long)]
        quick: bool,
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
            anyhow::bail!("frogctl backup export: not yet implemented")
        }
        BackupCommand::Import { .. } => {
            anyhow::bail!("frogctl backup import: not yet implemented")
        }
        BackupCommand::Verify { .. } => {
            anyhow::bail!("frogctl backup verify: not yet implemented")
        }
        BackupCommand::CheckpointVerify { dir, quick } => {
            Ok(run_checkpoint_verify(dir, *quick, ctx))
        }
    }
}

/// The verdict on one checkpoint payload. Deliberately reports *how* the
/// payload was checked, not just pass/fail: "verified" against a payload that
/// ships no manifest is a much weaker statement than one that does, and an
/// operator deciding whether to trust a backup needs to see which they got.
#[derive(Debug, Serialize)]
struct CheckpointVerification {
    dir: String,
    ok: bool,
    /// The `MANIFEST-NNNNNN` that `CURRENT` resolves to.
    manifest: Option<String>,
    /// Whether the payload carried a `frogdb_payload.json` to check against.
    payload_manifest: bool,
    checksums_verified: bool,
    files_checked: usize,
    bytes_checked: u64,
    error: Option<String>,
}

impl Renderable for CheckpointVerification {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = format!("Checkpoint: {}\n", self.dir);
        out.push_str(&format!(
            "  Result: {}\n",
            if self.ok { "OK" } else { "FAILED" }
        ));
        if let Some(e) = &self.error {
            out.push_str(&format!("  Error: {e}\n"));
            return out;
        }
        out.push_str(&format!(
            "  RocksDB Manifest: {}\n",
            self.manifest.as_deref().unwrap_or("unknown")
        ));
        if self.payload_manifest {
            out.push_str(&format!(
                "  Payload Manifest: present ({} files, {} bytes, checksums {})\n",
                self.files_checked,
                self.bytes_checked,
                if self.checksums_verified {
                    "verified"
                } else {
                    "skipped (--quick)"
                }
            ));
        } else {
            out.push_str(
                "  Payload Manifest: absent — structure checked only; this payload \
                 predates payload manifests or was not produced by the snapshot stager\n",
            );
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

/// Offline: never touches the connection, so a payload can be checked on a node
/// whose server is down — which is exactly when a backup gets looked at.
fn run_checkpoint_verify(dir: &Path, quick: bool, ctx: &ConnectionContext) -> i32 {
    let check = if quick {
        PayloadCheck::Sizes
    } else {
        PayloadCheck::Checksums
    };
    let verdict = match frogdb_persistence::verify_payload(dir, check) {
        Ok(report) => CheckpointVerification {
            dir: dir.display().to_string(),
            ok: true,
            manifest: Some(report.manifest.clone()),
            payload_manifest: report.payload_manifest_present,
            checksums_verified: report.checksums_verified,
            files_checked: report.files_checked,
            bytes_checked: report.bytes_checked,
            error: None,
        },
        Err(e) => CheckpointVerification {
            dir: dir.display().to_string(),
            ok: false,
            manifest: None,
            payload_manifest: false,
            checksums_verified: false,
            files_checked: 0,
            bytes_checked: 0,
            error: Some(e.to_string()),
        },
    };
    let ok = verdict.ok;
    print_output(&verdict, ctx.global().output, ctx.global().no_color);
    // Non-zero on failure: this command exists to be run from a pre-restore
    // script, and a script must be able to branch on the verdict.
    i32::from(!ok)
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
