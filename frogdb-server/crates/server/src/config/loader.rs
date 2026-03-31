//! Config loading, validation, logging initialization, and default TOML generation.

use super::LoggingGuard;
use anyhow::{Context, Result};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use frogdb_config::{
    ClientCertMode, Config,
    logging::{LogOutput, RotationConfig, RotationFrequency},
};
use std::path::{Path, PathBuf};
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt, prelude::*, reload};

/// TLS CLI overrides.
pub struct TlsCliOverrides {
    pub enabled: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
    pub port: Option<u16>,
    pub require_client_cert: Option<String>,
    pub replication: bool,
    pub cluster: bool,
}

/// Extension trait for Config runtime loading and logging initialization.
#[allow(clippy::too_many_arguments)]
pub trait ConfigLoader {
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
        http_bind: Option<String>,
        http_port: Option<u16>,
        http_token: Option<String>,
        tls: TlsCliOverrides,
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
        http_bind: Option<String>,
        http_port: Option<u16>,
        http_token: Option<String>,
        tls: TlsCliOverrides,
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
        // Map env var underscores to hyphens to match kebab-case serde rename.
        // Double underscores (`__`) are the section separator (handled by split),
        // so we protect them before converting single underscores to hyphens.
        figment = figment.merge(
            Env::prefixed("FROGDB_").split("__").map(|key| {
                key.as_str()
                    .replace("__", "\x00")
                    .replace('_', "-")
                    .replace('\x00', "__")
                    .into()
            }),
        );
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
        }
        if let Some(ref bind) = admin_bind {
            config.admin.bind = bind.clone();
        }
        if let Some(ref bind) = http_bind {
            config.http.bind = bind.clone();
        }
        if let Some(port) = http_port {
            config.http.port = port;
        }
        if let Some(ref token) = http_token {
            config.http.token = Some(token.clone());
        }
        // Apply TLS CLI overrides
        if tls.enabled {
            config.tls.enabled = true;
        }
        if let Some(cert_file) = tls.cert_file {
            config.tls.cert_file = cert_file;
        }
        if let Some(key_file) = tls.key_file {
            config.tls.key_file = key_file;
        }
        if let Some(ca_file) = tls.ca_file {
            config.tls.ca_file = Some(ca_file);
        }
        if let Some(port) = tls.port {
            config.tls.tls_port = port;
        }
        if let Some(ref mode) = tls.require_client_cert {
            config.tls.require_client_cert = match mode.to_lowercase().as_str() {
                "none" => ClientCertMode::None,
                "optional" => ClientCertMode::Optional,
                "required" => ClientCertMode::Required,
                other => anyhow::bail!(
                    "Invalid --tls-require-client-cert value '{}', expected: none, optional, required",
                    other
                ),
            };
        }
        if tls.replication {
            config.tls.tls_replication = true;
        }
        if tls.cluster {
            config.tls.tls_cluster = true;
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
num-shards = 1
allow-cross-slot-standalone = false
scatter-gather-timeout-ms = 5000

[logging]
level = "info"
format = "pretty"
output = "stdout"
per-request-spans = false

[persistence]
enabled = true
data-dir = "./frogdb-data"
durability-mode = "periodic"
sync-interval-ms = 1000
write-buffer-size-mb = 64
compression = "lz4"
block-cache-size-mb = 256
bloom-filter-bits = 10
max-write-buffer-number = 4
compaction-rate-limit-mb = 0
batch-size-threshold-kb = 4096
batch-timeout-ms = 10

[snapshot]
snapshot-dir = "./snapshots"
snapshot-interval-secs = 3600
max-snapshots = 5

[http]
enabled = true
bind = "127.0.0.1"
port = 9090
# token = "my-secret-token"

[metrics]
otlp-enabled = false
otlp-endpoint = "http://localhost:4317"
otlp-interval-secs = 15

[tracing]
enabled = false
otlp-endpoint = "http://localhost:4317"
sampling-rate = 1.0
service-name = "frogdb"
scatter-gather-spans = false
shard-spans = false
persistence-spans = false

[memory]
maxmemory = 0
maxmemory-policy = "noeviction"
maxmemory-samples = 5
lfu-log-factor = 10
lfu-decay-time = 1

[security]
requirepass = ""

[acl]
aclfile = ""
log-max-len = 128

[slowlog]
log-slower-than = 10000
max-len = 128
max-arg-len = 128

[json]
max-depth = 128
max-size = 67108864

[vll]
max-queue-depth = 10000
lock-acquisition-timeout-ms = 4000
per-shard-lock-timeout-ms = 2000
timeout-check-interval-ms = 100
max-continuation-lock-ms = 65000

[replication]
role = "standalone"
primary-host = ""
primary-port = 6379
min-replicas-to-write = 0
min-replicas-timeout-ms = 5000
ack-interval-ms = 1000
fullsync-timeout-secs = 300
fullsync-max-memory-mb = 512
state-file = "replication_state.json"
connect-timeout-ms = 5000
handshake-timeout-ms = 10000
reconnect-backoff-initial-ms = 100
reconnect-backoff-max-ms = 30000
self-fence-on-replica-loss = true
replica-freshness-timeout-ms = 3000
replica-write-timeout-ms = 5000

[cluster]
enabled = false
node-id = 0
client-addr = ""
cluster-bus-addr = "127.0.0.1:16379"
initial-nodes = []
data-dir = "./frogdb-cluster"
election-timeout-ms = 1000
heartbeat-interval-ms = 250
connect-timeout-ms = 5000
request-timeout-ms = 10000
auto-failover = false
fail-threshold = 5

[admin]
enabled = false
port = 6382
bind = "127.0.0.1"

[tls]
enabled = false
# cert-file = "/path/to/server.crt"
# key-file = "/path/to/server.key"
# ca-file = "/path/to/ca.crt"
tls-port = 6380
require-client-cert = "none"
protocols = ["1.3", "1.2"]
no-tls-on-admin-port = true
handshake-timeout-ms = 10000

[status]
memory-warning-percent = 90
connection-warning-percent = 90

[latency]
startup-test = false
startup-test-duration-secs = 5
warning-threshold-us = 2000

[latency-bands]
enabled = false
bands = [1, 5, 10, 50, 100, 500]
"#
    .to_string()
}
