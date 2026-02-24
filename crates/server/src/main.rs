//! FrogDB Server Binary
//!
//! A high-performance, Redis-compatible database server.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Result;
use clap::Parser;
use frogdb_server::{Config, Server, latency_test};
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

    /// Admin bind address (overrides config)
    #[arg(long, value_name = "ADDR")]
    admin_bind: Option<String>,

    /// Admin port (overrides config, implies admin.enabled=true)
    #[arg(long, value_name = "PORT")]
    admin_port: Option<u16>,

    /// Generate default configuration file
    #[arg(long)]
    generate_config: bool,

    /// Run intrinsic latency test for N seconds and exit (standalone mode)
    #[arg(long, value_name = "SECONDS")]
    intrinsic_latency: Option<u64>,

    /// Run latency check at startup before accepting connections
    #[arg(long)]
    startup_latency_check: bool,
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

    // Handle --intrinsic-latency (standalone mode)
    if let Some(duration) = cli.intrinsic_latency {
        println!("Running intrinsic latency test for {} seconds...", duration);
        println!("(Press Ctrl+C to abort)\n");

        let progress_callback: latency_test::ProgressCallback = Box::new(|max_us| {
            println!("Max latency so far: {} microseconds.", max_us);
        });

        let result = latency_test::run_intrinsic_latency_test(duration, Some(progress_callback));
        latency_test::print_latency_report(&result);
        return Ok(());
    }

    // Load configuration
    let mut config = Config::load(
        cli.config.as_deref(),
        cli.bind,
        cli.port,
        cli.shards,
        cli.log_level,
        cli.log_format,
        cli.admin_bind,
        cli.admin_port,
    )?;

    // Apply --startup-latency-check CLI override
    if cli.startup_latency_check {
        config.latency.startup_test = true;
    }

    // Initialize logging
    config.init_logging()?;

    info!(config = %config.to_json(), "Starting FrogDB server");

    // Create and run server
    let server = Server::new(config).await?;
    server.run().await?;

    Ok(())
}
