use std::path::PathBuf;

use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum DataCommand {
    /// Scan keyspace for the largest keys (alias for debug memory bigkeys)
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

    /// Scan every key with MEMORY USAGE (alias for debug memory memkeys)
    Memkeys,

    /// Keyspace summary: key count by type, memory distribution, expiry stats
    Keyspace {
        /// Sample size for memory estimation
        #[arg(long, default_value_t = 10000)]
        samples: u64,
    },

    /// Export dataset (alias for backup export)
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

    /// Import dataset (alias for backup import)
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

    /// Pipe raw RESP commands from stdin
    Pipe {
        /// Pipeline batch size
        #[arg(long, default_value_t = 1000)]
        batch: u64,
    },

    /// Show which hash slot and node a key maps to
    Slot {
        /// Key to look up
        key: String,

        /// Show internal shard via DEBUG HASHING
        #[arg(long)]
        internal: bool,
    },
}

pub async fn run(cmd: &DataCommand) -> anyhow::Result<i32> {
    match cmd {
        DataCommand::Bigkeys { .. } => {
            anyhow::bail!("frog data bigkeys: not yet implemented")
        }
        DataCommand::Memkeys => {
            anyhow::bail!("frog data memkeys: not yet implemented")
        }
        DataCommand::Keyspace { .. } => {
            anyhow::bail!("frog data keyspace: not yet implemented")
        }
        DataCommand::Export { .. } => {
            anyhow::bail!("frog data export: not yet implemented")
        }
        DataCommand::Import { .. } => {
            anyhow::bail!("frog data import: not yet implemented")
        }
        DataCommand::Pipe { .. } => {
            anyhow::bail!("frog data pipe: not yet implemented")
        }
        DataCommand::Slot { .. } => {
            anyhow::bail!("frog data slot: not yet implemented")
        }
    }
}
