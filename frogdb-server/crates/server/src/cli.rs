//! Command-line interface for the `frogdb-server` binary.
//!
//! Kept in the library (rather than `main.rs`) so both the binary and the
//! `docs-gen` tool can introspect the same [`clap`] definition.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "frogdb-server")]
#[command(author, version, about = "FrogDB - A high-performance Redis-compatible database", long_about = None)]
pub struct Cli {
    /// Path to configuration file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<std::path::PathBuf>,

    /// Bind address
    #[arg(short, long, value_name = "ADDR")]
    pub bind: Option<String>,

    /// Listen port
    #[arg(short, long, value_name = "PORT")]
    pub port: Option<u16>,

    /// Number of shards (default: 1, "auto" = num_cpus)
    #[arg(short, long, value_name = "N")]
    pub shards: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, value_name = "LEVEL")]
    pub log_level: Option<String>,

    /// Log format (pretty, json)
    #[arg(long, value_name = "FORMAT")]
    pub log_format: Option<String>,

    /// Admin bind address (overrides config)
    #[arg(long, value_name = "ADDR")]
    pub admin_bind: Option<String>,

    /// Admin port (overrides config, implies admin.enabled=true)
    #[arg(long, value_name = "PORT")]
    pub admin_port: Option<u16>,

    /// HTTP server bind address (overrides config)
    #[arg(long, value_name = "ADDR")]
    pub http_bind: Option<String>,

    /// HTTP server port (overrides config)
    #[arg(long, value_name = "PORT")]
    pub http_port: Option<u16>,

    /// Bearer token for protected HTTP endpoints (/admin/*, /debug/*)
    #[arg(long, value_name = "TOKEN")]
    pub http_token: Option<String>,

    /// Enable TLS
    #[arg(long)]
    pub tls_enabled: bool,

    /// Path to TLS certificate file (PEM)
    #[arg(long, value_name = "FILE")]
    pub tls_cert_file: Option<std::path::PathBuf>,

    /// Path to TLS private key file (PEM)
    #[arg(long, value_name = "FILE")]
    pub tls_key_file: Option<std::path::PathBuf>,

    /// Path to TLS CA certificate file (PEM) for client verification
    #[arg(long, value_name = "FILE")]
    pub tls_ca_file: Option<std::path::PathBuf>,

    /// TLS listen port
    #[arg(long, value_name = "PORT")]
    pub tls_port: Option<u16>,

    /// Client certificate mode: none, optional, required
    #[arg(long, value_name = "MODE")]
    pub tls_require_client_cert: Option<String>,

    /// Encrypt replication connections with TLS
    #[arg(long)]
    pub tls_replication: bool,

    /// Encrypt cluster bus connections with TLS
    #[arg(long)]
    pub tls_cluster: bool,

    /// Generate default configuration file
    #[arg(long)]
    pub generate_config: bool,

    /// Run intrinsic latency test for N seconds and exit (standalone mode)
    #[arg(long, value_name = "SECONDS")]
    pub intrinsic_latency: Option<u64>,

    /// Run latency check at startup before accepting connections
    #[arg(long)]
    pub startup_latency_check: bool,
}
