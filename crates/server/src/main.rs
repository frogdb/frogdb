//! FrogDB Server Binary
//!
//! A high-performance, Redis-compatible database server.

use anyhow::Result;
use clap::Parser;
use frogdb_server::{Config, Server};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "frogdb-server")]
#[command(author, version, about = "FrogDB - A high-performance Redis-compatible database", long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, value_name = "FILE")]
    config: Option<std::path::PathBuf>,

    /// Bind address
    #[arg(short, long, value_name = "ADDR")]
    bind: Option<String>,

    /// Listen port
    #[arg(short, long, value_name = "PORT")]
    port: Option<u16>,

    /// Number of shards (default: 1, "auto" = num_cpus)
    #[arg(short, long, value_name = "N")]
    shards: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, value_name = "LEVEL")]
    log_level: Option<String>,

    /// Log format (pretty, json)
    #[arg(long, value_name = "FORMAT")]
    log_format: Option<String>,

    /// Generate default configuration file
    #[arg(long)]
    generate_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Handle --generate-config
    if cli.generate_config {
        let default_config = Config::default_toml();
        println!("{}", default_config);
        return Ok(());
    }

    // Load configuration
    let config = Config::load(
        cli.config.as_deref(),
        cli.bind,
        cli.port,
        cli.shards,
        cli.log_level,
        cli.log_format,
    )?;

    // Initialize logging
    config.init_logging()?;

    info!(
        bind = %config.server.bind,
        port = config.server.port,
        shards = config.server.num_shards,
        log_level = %config.logging.level,
        log_format = %config.logging.format,
        "Starting FrogDB server"
    );

    // Create and run server
    let server = Server::new(config).await?;
    server.run().await?;

    Ok(())
}
