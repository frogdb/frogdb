//! FrogDB Server Binary
//!
//! A high-performance, Redis-compatible database server.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Overrides jemalloc's weak `malloc_conf` symbol, which it reads once during
/// initialization — before `main`, and before any allocation this program makes.
///
/// The definition lives here rather than in the library because a symbol may be
/// defined only once in a linked image, and the library's own test binary links
/// both itself and the library. See [`frogdb_server::malloc_conf`] for what the
/// option string asks for and why.
#[cfg(not(target_env = "msvc"))]
#[used]
#[unsafe(export_name = "_rjem_malloc_conf")]
static MALLOC_CONF: &[u8; frogdb_server::malloc_conf::MALLOC_CONF_LEN] =
    frogdb_server::malloc_conf::MALLOC_CONF;

use anyhow::Result;
use clap::Parser;
use frogdb_server::{
    Config, Server,
    cli::Cli,
    config::{ConfigLoader, TlsCliOverrides},
    latency_test,
};
use tracing::info;

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

    // Apply --force-fresh-data-dir CLI override. Deliberately applied here
    // rather than merged through figment: the flag has no config-file or
    // environment spelling, so it cannot outlive the boot it was passed for.
    if cli.force_fresh_data_dir {
        config.persistence.force_fresh_data_dir = true;
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

    // Build runtime with hooks when profiling.
    //
    // This runtime is no longer the data path: shard workers and the client
    // connections pinned to them run on their own OS threads with their own
    // current-thread runtimes (see `frogdb_net::RealShardExecutor`). What is
    // left here is the acceptor, observability, replication, cluster and
    // background tasks. Size it so it does not fight the shard cores for CPU:
    // reserve one core per shard, with a floor of two workers so a blocking
    // background task can never starve the acceptor.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let ambient_workers = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
        .saturating_sub(config.server.num_shards)
        .max(2);
    builder.worker_threads(ambient_workers);
    builder.thread_name("frogdb-ambient");
    builder.enable_all();
    info!(
        worker_threads = ambient_workers,
        shard_threads = config.server.num_shards,
        "Ambient runtime sized around the shard threads"
    );

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

#[cfg(test)]
mod tests {
    /// The override only counts if jemalloc read it. Assert against the live
    /// allocator in the one binary that defines the symbol — the library's own
    /// test binary does not, so this can only be checked here.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn jemalloc_applies_the_requested_options() {
        assert_eq!(
            frogdb_server::malloc_conf::applied(),
            Some(true),
            "jemalloc ignored `{}` (it reports narenas={:?}, decay={:?}); check the `_rjem_` \
             symbol prefix",
            frogdb_server::malloc_conf::requested(),
            frogdb_telemetry::jemalloc::configured_narenas(),
            frogdb_telemetry::jemalloc::configured_decay(),
        );
    }
}
