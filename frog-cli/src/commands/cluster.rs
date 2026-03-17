use clap::Subcommand;

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

pub async fn run(cmd: &ClusterCommand) -> anyhow::Result<i32> {
    match cmd {
        ClusterCommand::Create { .. } => {
            anyhow::bail!("frog cluster create: not yet implemented")
        }
        ClusterCommand::Info => {
            anyhow::bail!("frog cluster info: not yet implemented")
        }
        ClusterCommand::Check => {
            anyhow::bail!("frog cluster check: not yet implemented")
        }
        ClusterCommand::Fix => {
            anyhow::bail!("frog cluster fix: not yet implemented")
        }
        ClusterCommand::AddNode { .. } => {
            anyhow::bail!("frog cluster add-node: not yet implemented")
        }
        ClusterCommand::DelNode { .. } => {
            anyhow::bail!("frog cluster del-node: not yet implemented")
        }
        ClusterCommand::Reshard { .. } => {
            anyhow::bail!("frog cluster reshard: not yet implemented")
        }
        ClusterCommand::Rebalance { .. } => {
            anyhow::bail!("frog cluster rebalance: not yet implemented")
        }
        ClusterCommand::Failover { .. } => {
            anyhow::bail!("frog cluster failover: not yet implemented")
        }
        ClusterCommand::Topology => {
            anyhow::bail!("frog cluster topology: not yet implemented")
        }
        ClusterCommand::Slots { .. } => {
            anyhow::bail!("frog cluster slots: not yet implemented")
        }
    }
}
