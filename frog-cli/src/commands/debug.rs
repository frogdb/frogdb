use std::path::PathBuf;

use clap::Subcommand;

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

pub async fn run(cmd: &DebugCommand) -> anyhow::Result<i32> {
    match cmd {
        DebugCommand::Zip { .. } => {
            anyhow::bail!("frog debug zip: not yet implemented")
        }
        DebugCommand::Latency { .. } => {
            anyhow::bail!("frog debug latency: not yet implemented")
        }
        DebugCommand::Memory { subcommand } => match subcommand {
            MemorySubcommand::Stats => {
                anyhow::bail!("frog debug memory stats: not yet implemented")
            }
            MemorySubcommand::Doctor => {
                anyhow::bail!("frog debug memory doctor: not yet implemented")
            }
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
        DebugCommand::Slowlog { .. } => {
            anyhow::bail!("frog debug slowlog: not yet implemented")
        }
        DebugCommand::Vll { .. } => {
            anyhow::bail!("frog debug vll: not yet implemented")
        }
        DebugCommand::Connections { .. } => {
            anyhow::bail!("frog debug connections: not yet implemented")
        }
    }
}
