//! Config loading, validation, logging initialization, and default TOML generation.

use super::LoggingGuard;
use anyhow::{Context, Result};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use frogdb_config::{
    Config,
    logging::{LogOutput, RotationConfig, RotationFrequency},
};
use std::path::Path;
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt, prelude::*, reload};

/// Extension trait for Config runtime loading and logging initialization.
#[allow(clippy::too_many_arguments)]
pub trait ConfigLoader {
    fn load(
        config_path: Option<&Path>,
        bind: Option<String>,
        port: Option<u16>,
        shards: Option<String>,
        log_level: Option<String>,
        log_format: Option<String>,
        admin_bind: Option<String>,
        admin_port: Option<u16>,
        admin_http_port: Option<u16>,
    ) -> Result<Config>;

    fn init_logging(&self) -> Result<(crate::runtime_config::LogReloadHandle, LoggingGuard)>;

    fn init_logging_with_layer<L>(
        &self,
        extra_layer: L,
    ) -> Result<(crate::runtime_config::LogReloadHandle, LoggingGuard)>
    where
        L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static;

    fn build_file_writer(
        &self,
    ) -> Result<(
        Option<tracing_appender::non_blocking::NonBlocking>,
        Option<tracing_appender::non_blocking::WorkerGuard>,
    )>;

    fn default_toml() -> String;
}

impl ConfigLoader for Config {
    #[allow(clippy::too_many_arguments)]
    fn load(
        config_path: Option<&Path>,
        bind: Option<String>,
        port: Option<u16>,
        shards: Option<String>,
        log_level: Option<String>,
        log_format: Option<String>,
        admin_bind: Option<String>,
        admin_port: Option<u16>,
        admin_http_port: Option<u16>,
    ) -> Result<Config> {
        let mut figment = Figment::new().merge(Serialized::defaults(Config::default()));
        if let Some(path) = config_path {
            if !path.exists() {
                anyhow::bail!("config file not found: {}", path.display());
            }
            figment = figment.merge(Toml::file(path));
        } else {
            let default_path = Path::new("frogdb.toml");
            if default_path.exists() {
                figment = figment.merge(Toml::file(default_path).nested());
            } else {
                tracing::warn!("Default config file 'frogdb.toml' not found, using defaults");
            }
        }
        figment = figment.merge(Env::prefixed("FROGDB_").split("__"));
        let mut cli_overrides = Config::default();
        if let Some(ref bind) = bind {
            cli_overrides.server.bind = bind.clone();
        }
        if let Some(port) = port {
            cli_overrides.server.port = port;
        }
        if let Some(ref shards) = shards {
            cli_overrides.server.num_shards = if shards == "auto" {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(1)
            } else {
                shards.parse().context("Invalid shard count")?
            };
        }
        if let Some(ref level) = log_level {
            cli_overrides.logging.level = level.clone();
        }
        if let Some(ref format) = log_format {
            cli_overrides.logging.format = format.clone();
        }
        let mut config: Config = figment.extract().context("Failed to load configuration")?;
        if bind.is_some() {
            config.server.bind = cli_overrides.server.bind;
        }
        if port.is_some() {
            config.server.port = cli_overrides.server.port;
        }
        if shards.is_some() {
            config.server.num_shards = cli_overrides.server.num_shards;
        }
        if log_level.is_some() {
            config.logging.level = cli_overrides.logging.level;
        }
        if log_format.is_some() {
            config.logging.format = cli_overrides.logging.format;
        }
        if let Some(port) = admin_port {
            config.admin.enabled = true;
            config.admin.port = port;
            // Default http_port to admin port + 1 unless explicitly set
            if admin_http_port.is_none() {
                config.admin.http_port = port.saturating_add(1);
            }
        }
        if let Some(http_port) = admin_http_port {
            config.admin.http_port = http_port;
        }
        if let Some(ref bind) = admin_bind {
            config.admin.bind = bind.clone();
        }
        config.validate()?;
        Ok(config)
    }

    fn init_logging(&self) -> Result<(crate::runtime_config::LogReloadHandle, LoggingGuard)> {
        init_logging_inner::<tracing_subscriber::layer::Identity>(self, None)
    }

    fn init_logging_with_layer<L>(
        &self,
        extra_layer: L,
    ) -> Result<(crate::runtime_config::LogReloadHandle, LoggingGuard)>
    where
        L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
    {
        init_logging_inner(self, Some(extra_layer))
    }

    fn build_file_writer(
        &self,
    ) -> Result<(
        Option<tracing_appender::non_blocking::NonBlocking>,
        Option<tracing_appender::non_blocking::WorkerGuard>,
    )> {
        build_file_writer_impl(self)
    }

    fn default_toml() -> String {
        default_toml_impl()
    }
}

fn init_logging_inner<L>(
    config: &Config,
    extra_layer: Option<L>,
) -> Result<(crate::runtime_config::LogReloadHandle, LoggingGuard)>
where
    L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    use crate::runtime_config::LogReloadHandle;
    use tracing_subscriber::fmt::writer::BoxMakeWriter;
    let console_writer: Option<BoxMakeWriter> = match config.logging.output {
        LogOutput::Stdout => Some(BoxMakeWriter::new(std::io::stdout)),
        LogOutput::Stderr => Some(BoxMakeWriter::new(std::io::stderr)),
        LogOutput::None => None,
    };
    let (file_writer, file_guard) = build_file_writer_impl(config)?;
    let guard = LoggingGuard {
        _file_guard: file_guard,
    };
    let is_json = config.logging.format.to_lowercase() == "json";
    if std::env::var("RUST_LOG").is_ok() {
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(&config.logging.level));
        let (filter_layer, reload_handle) = reload::Layer::new(env_filter);
        if is_json {
            let console = console_writer.map(|w| fmt::layer().json().with_writer(w));
            let file = file_writer.map(|w| fmt::layer().json().with_writer(w));
            tracing_subscriber::registry()
                .with(extra_layer)
                .with(filter_layer)
                .with(console)
                .with(file)
                .init();
        } else {
            let console = console_writer.map(|w| fmt::layer().with_writer(w));
            let file = file_writer.map(|w| fmt::layer().with_writer(w));
            tracing_subscriber::registry()
                .with(extra_layer)
                .with(filter_layer)
                .with(console)
                .with(file)
                .init();
        }
        Ok((
            LogReloadHandle::new(Box::new(move |level: &str| {
                let filter =
                    EnvFilter::try_new(level).map_err(|e| format!("invalid EnvFilter: {e}"))?;
                reload_handle
                    .reload(filter)
                    .map_err(|e| format!("reload failed: {e}"))
            })),
            guard,
        ))
    } else {
        let level: LevelFilter = config.logging.level.parse().unwrap_or(LevelFilter::INFO);
        let (filter_layer, reload_handle) = reload::Layer::new(level);
        if is_json {
            let console = console_writer.map(|w| fmt::layer().json().with_writer(w));
            let file = file_writer.map(|w| fmt::layer().json().with_writer(w));
            tracing_subscriber::registry()
                .with(extra_layer)
                .with(filter_layer)
                .with(console)
                .with(file)
                .init();
        } else {
            let console = console_writer.map(|w| fmt::layer().with_writer(w));
            let file = file_writer.map(|w| fmt::layer().with_writer(w));
            tracing_subscriber::registry()
                .with(extra_layer)
                .with(filter_layer)
                .with(console)
                .with(file)
                .init();
        }
        Ok((
            LogReloadHandle::new(Box::new(move |level: &str| {
                let filter: LevelFilter = level
                    .parse()
                    .map_err(|e| format!("invalid LevelFilter: {e}"))?;
                reload_handle
                    .reload(filter)
                    .map_err(|e| format!("reload failed: {e}"))
            })),
            guard,
        ))
    }
}

fn build_file_writer_impl(
    config: &Config,
) -> Result<(
    Option<tracing_appender::non_blocking::NonBlocking>,
    Option<tracing_appender::non_blocking::WorkerGuard>,
)> {
    let file_path = match config.logging.file_path {
        Some(ref p) => p,
        None => return Ok((None, None)),
    };
    let writer: Box<dyn std::io::Write + Send + Sync> =
        if let Some(ref rotation) = config.logging.rotation {
            let appender = rolling_file::BasicRollingFileAppender::new(
                file_path,
                build_rolling_condition(rotation),
                rotation.max_files as usize,
            )
            .with_context(|| {
                format!(
                    "failed to create rolling file appender at '{}'",
                    file_path.display()
                )
            })?;
            Box::new(appender)
        } else {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)
                .with_context(|| format!("failed to open log file '{}'", file_path.display()))?;
            Box::new(file)
        };
    let (non_blocking, guard) = tracing_appender::non_blocking(writer);
    Ok((Some(non_blocking), Some(guard)))
}

fn build_rolling_condition(rotation: &RotationConfig) -> rolling_file::RollingConditionBasic {
    use rolling_file::RollingConditionBasic;
    match (&rotation.frequency, rotation.max_size_mb) {
        (RotationFrequency::Never, 0) => RollingConditionBasic::new().daily(),
        (RotationFrequency::Never, size) => {
            RollingConditionBasic::new().max_size(size * 1024 * 1024)
        }
        (RotationFrequency::Daily, 0) => RollingConditionBasic::new().daily(),
        (RotationFrequency::Daily, size) => RollingConditionBasic::new()
            .daily()
            .max_size(size * 1024 * 1024),
        (RotationFrequency::Hourly, 0) => RollingConditionBasic::new().hourly(),
        (RotationFrequency::Hourly, size) => RollingConditionBasic::new()
            .hourly()
            .max_size(size * 1024 * 1024),
    }
}

fn default_toml_impl() -> String {
    r#"# FrogDB Configuration File

[server]
bind = "127.0.0.1"
port = 6379
num_shards = 1
allow_cross_slot_standalone = false
scatter_gather_timeout_ms = 5000

[logging]
level = "info"
format = "pretty"
output = "stdout"
per_request_spans = false

[persistence]
enabled = true
data_dir = "./frogdb-data"
durability_mode = "periodic"
sync_interval_ms = 1000
write_buffer_size_mb = 64
compression = "lz4"
block_cache_size_mb = 256
bloom_filter_bits = 10
max_write_buffer_number = 4
compaction_rate_limit_mb = 0
batch_size_threshold_kb = 4096
batch_timeout_ms = 10

[snapshot]
snapshot_dir = "./snapshots"
snapshot_interval_secs = 3600
max_snapshots = 5

[metrics]
enabled = true
bind = "0.0.0.0"
port = 9090
otlp_enabled = false
otlp_endpoint = "http://localhost:4317"
otlp_interval_secs = 15

[tracing]
enabled = false
otlp_endpoint = "http://localhost:4317"
sampling_rate = 1.0
service_name = "frogdb"
scatter_gather_spans = false
shard_spans = false
persistence_spans = false

[memory]
maxmemory = 0
maxmemory_policy = "noeviction"
maxmemory_samples = 5
lfu_log_factor = 10
lfu_decay_time = 1

[security]
requirepass = ""

[acl]
aclfile = ""
log_max_len = 128

[slowlog]
log_slower_than = 10000
max_len = 128
max_arg_len = 128

[json]
max_depth = 128
max_size = 67108864

[vll]
max_queue_depth = 10000
lock_acquisition_timeout_ms = 4000
per_shard_lock_timeout_ms = 2000
timeout_check_interval_ms = 100
max_continuation_lock_ms = 65000

[replication]
role = "standalone"
primary_host = ""
primary_port = 6379
min_replicas_to_write = 0
min_replicas_timeout_ms = 5000
ack_interval_ms = 1000
fullsync_timeout_secs = 300
fullsync_max_memory_mb = 512
state_file = "replication_state.json"
connect_timeout_ms = 5000
handshake_timeout_ms = 10000
reconnect_backoff_initial_ms = 100
reconnect_backoff_max_ms = 30000
self_fence_on_replica_loss = true
replica_freshness_timeout_ms = 3000
replica_write_timeout_ms = 5000

[cluster]
enabled = false
node_id = 0
client_addr = ""
cluster_bus_addr = "127.0.0.1:16379"
initial_nodes = []
data_dir = "./frogdb-cluster"
election_timeout_ms = 1000
heartbeat_interval_ms = 250
connect_timeout_ms = 5000
request_timeout_ms = 10000
auto_failover = false
fail_threshold = 5

[admin]
enabled = false
port = 6380
bind = "127.0.0.1"
http_port = 6381

[status]
memory_warning_percent = 90
connection_warning_percent = 90

[latency]
startup_test = false
startup_test_duration_secs = 5
warning_threshold_us = 2000

[latency_bands]
enabled = false
bands = [1, 5, 10, 50, 100, 500]
"#
    .to_string()
}
