use std::path::PathBuf;

use clap::Subcommand;

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

pub async fn run(cmd: &BackupCommand) -> anyhow::Result<i32> {
    match cmd {
        BackupCommand::Trigger => {
            anyhow::bail!("frog backup trigger: not yet implemented")
        }
        BackupCommand::Status => {
            anyhow::bail!("frog backup status: not yet implemented")
        }
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
