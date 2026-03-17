use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::commands::{
    backup::BackupCommand, cluster::ClusterCommand, config::ConfigCommand, data::DataCommand,
    debug::DebugCommand, health::HealthArgs, replication::ReplicationCommand, stat::StatArgs,
};

#[derive(Parser, Debug)]
#[command(name = "frog", about = "FrogDB operational tooling", version)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalOpts,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Parser, Debug, Clone)]
pub struct GlobalOpts {
    /// Server hostname or IP
    #[arg(short = 'H', long, default_value = "127.0.0.1", global = true)]
    pub host: String,

    /// RESP port
    #[arg(short, long, default_value_t = 6379, global = true)]
    pub port: u16,

    /// Password (AUTH <password>)
    #[arg(short = 'a', long = "auth", global = true)]
    pub auth: Option<String>,

    /// ACL username (AUTH <user> <password>)
    #[arg(short = 'u', long = "user", global = true)]
    pub user: Option<String>,

    /// Enable TLS for RESP connections
    #[arg(long, global = true)]
    pub tls: bool,

    /// Client certificate path (mTLS)
    #[arg(long, global = true)]
    pub tls_cert: Option<PathBuf>,

    /// Client private key path (mTLS)
    #[arg(long, global = true)]
    pub tls_key: Option<PathBuf>,

    /// CA certificate path
    #[arg(long, global = true)]
    pub tls_ca: Option<PathBuf>,

    /// Admin HTTP base URL (e.g. http://127.0.0.1:6380)
    #[arg(long, global = true)]
    pub admin_url: Option<String>,

    /// Metrics/observability HTTP base URL (e.g. http://127.0.0.1:9090)
    #[arg(long, global = true)]
    pub metrics_url: Option<String>,

    /// Output format
    #[arg(short, long, default_value = "table", global = true)]
    pub output: OutputMode,

    /// Disable ANSI colors
    #[arg(long, global = true)]
    pub no_color: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
    Raw,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Health checking
    Health(HealthArgs),

    /// Real-time monitoring dashboard
    Stat(StatArgs),

    /// Configuration management
    #[command(subcommand)]
    Config(ConfigCommand),

    /// Cluster operations
    #[command(subcommand)]
    Cluster(ClusterCommand),

    /// Replication management
    #[command(subcommand)]
    Replication(ReplicationCommand),

    /// Diagnostics & debugging
    #[command(subcommand)]
    Debug(DebugCommand),

    /// Backup & restore
    #[command(subcommand)]
    Backup(BackupCommand),

    /// Data utilities
    #[command(subcommand)]
    Data(DataCommand),
}
