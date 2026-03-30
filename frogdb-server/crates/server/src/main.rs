//! FrogDB Server Binary
//!
//! A high-performance, Redis-compatible database server.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Result;
use clap::Parser;
use frogdb_server::{
    Config, Server,
    config::{ConfigLoader, TlsCliOverrides},
    latency_test,
};
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

    /// HTTP server bind address (overrides config)
    #[arg(long, value_name = "ADDR")]
    http_bind: Option<String>,

    /// HTTP server port (overrides config)
    #[arg(long, value_name = "PORT")]
    http_port: Option<u16>,

    /// Bearer token for protected HTTP endpoints (/admin/*, /debug/*)
    #[arg(long, value_name = "TOKEN")]
    http_token: Option<String>,

    /// Enable TLS
    #[arg(long)]
    tls_enabled: bool,

    /// Path to TLS certificate file (PEM)
    #[arg(long, value_name = "FILE")]
    tls_cert_file: Option<std::path::PathBuf>,

    /// Path to TLS private key file (PEM)
    #[arg(long, value_name = "FILE")]
    tls_key_file: Option<std::path::PathBuf>,

    /// Path to TLS CA certificate file (PEM) for client verification
    #[arg(long, value_name = "FILE")]
    tls_ca_file: Option<std::path::PathBuf>,

    /// TLS listen port
    #[arg(long, value_name = "PORT")]
    tls_port: Option<u16>,

    /// Client certificate mode: none, optional, required
    #[arg(long, value_name = "MODE")]
    tls_require_client_cert: Option<String>,

    /// Encrypt replication connections with TLS
    #[arg(long)]
    tls_replication: bool,

    /// Encrypt cluster bus connections with TLS
    #[arg(long)]
    tls_cluster: bool,

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

fn main() -> Result<()> {
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
    let tls_overrides = TlsCliOverrides {
        enabled: cli.tls_enabled,
        cert_file: cli.tls_cert_file,
        key_file: cli.tls_key_file,
        ca_file: cli.tls_ca_file,
        port: cli.tls_port,
        require_client_cert: cli.tls_require_client_cert,
        replication: cli.tls_replication,
        cluster: cli.tls_cluster,
    };
    let mut config = Config::load(
        cli.config.as_deref(),
        cli.bind,
        cli.port,
        cli.shards,
        cli.log_level,
        cli.log_format,
        cli.admin_bind,
        cli.admin_port,
        cli.http_bind,
        cli.http_port,
        cli.http_token,
        tls_overrides,
    )?;

    // Apply --startup-latency-check CLI override
    if cli.startup_latency_check {
        config.latency.startup_test = true;
    }

    // --- Causal profiling setup (compile-time + runtime gated) ---
    #[cfg(all(tokio_unstable, feature = "causal-profile"))]
    let profiler = {
        use tokio_coz::{CausalProfiler, ProfilerConfig, SelectionStrategy};
        CausalProfiler::new(
            ProfilerConfig::new()
                .experiment_duration(std::time::Duration::from_secs(1))
                .speedup_steps(vec![0, 50, 100])
                .rounds_per_experiment(4)
                .selection_strategy(SelectionStrategy::RoundRobin)
                .output_path("causal-profile.json"),
        )
    };

    // Initialize logging (with SpanTracker layer when profiling)
    #[cfg(all(tokio_unstable, feature = "causal-profile"))]
    let (log_reload_handle, _logging_guard) =
        config.init_logging_with_layer(profiler.tracing_layer())?;

    #[cfg(all(
        feature = "profiling",
        not(all(tokio_unstable, feature = "causal-profile"))
    ))]
    let (_flame_guard, log_reload_handle, _logging_guard) = {
        let path =
            std::env::var("FROGDB_FLAME_OUTPUT").unwrap_or_else(|_| "tracing-flame.folded".into());
        let (flame_layer, guard) = tracing_flame::FlameLayer::with_file(&path)
            .expect("failed to create flame output file");
        let (handle, logging_guard) = config.init_logging_with_layer(flame_layer)?;
        info!(output = %path, "tracing-flame profiling enabled");
        (guard, handle, logging_guard)
    };

    #[cfg(not(any(all(tokio_unstable, feature = "causal-profile"), feature = "profiling")))]
    let (log_reload_handle, _logging_guard) = config.init_logging()?;

    // Register USDT probes with the kernel tracing infrastructure
    frogdb_core::probes::register().expect("Failed to register USDT probes");

    info!(config = %config.to_json(), "Starting FrogDB server");

    // Build runtime with hooks when profiling
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();

    #[cfg(all(tokio_unstable, feature = "causal-profile"))]
    {
        builder
            .on_task_spawn(profiler.on_task_spawn())
            .on_before_task_poll(profiler.on_before_task_poll())
            .on_after_task_poll(profiler.on_after_task_poll())
            .on_task_terminate(profiler.on_task_terminate());
    }

    let runtime = builder.build()?;

    runtime.block_on(async {
        #[cfg(all(tokio_unstable, feature = "causal-profile"))]
        if std::env::var("COZ_PROFILE").is_ok() {
            info!("Causal profiling enabled — starting experiment engine");
            profiler.start().await;
        }

        let server = Server::new(config, log_reload_handle).await?;
        server.run().await
    })?;

    #[cfg(all(tokio_unstable, feature = "causal-profile"))]
    profiler.report();

    Ok(())
}
