use clap::Subcommand;

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

pub async fn run(cmd: &ReplicationCommand) -> anyhow::Result<i32> {
    match cmd {
        ReplicationCommand::Status => {
            anyhow::bail!("frog replication status: not yet implemented")
        }
        ReplicationCommand::Lag { .. } => {
            anyhow::bail!("frog replication lag: not yet implemented")
        }
        ReplicationCommand::Promote { .. } => {
            anyhow::bail!("frog replication promote: not yet implemented")
        }
        ReplicationCommand::Topology => {
            anyhow::bail!("frog replication topology: not yet implemented")
        }
    }
}
