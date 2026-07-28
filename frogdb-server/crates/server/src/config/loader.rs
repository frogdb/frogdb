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
        figment = figment.merge(Env::prefixed("FROGDB_").split("__").map(|key| {
            key.as_str()
                .replace("__", "\x00")
                .replace('_', "-")
                .replace('\x00', "__")
                .into()
        }));
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

        // Store the resolved config file path for CONFIG REWRITE
        if let Some(path) = config_path {
            config.config_source_path = Some(path.to_path_buf());
        } else {
            let default_path = Path::new("frogdb.toml");
            if default_path.exists() {
                config.config_source_path =
                    Some(std::fs::canonicalize(default_path).unwrap_or(default_path.to_path_buf()));
            }
        }

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

/// Header comment prepended to the generated default config TOML.
const DEFAULT_TOML_HEADER: &str = "\
# FrogDB Configuration File
#
# This file is generated by serializing `Config::default()` to TOML — it is
# NOT a hand-maintained copy, so it cannot drift from the real defaults the
# server actually uses (see `default_toml_impl()` in
# frogdb-server/crates/server/src/config/loader.rs). Regenerate an identical
# copy at any time with `frogdb-server --generate-config`.
#
# Optional fields whose default is \"unset\" (e.g. tls.ca-file,
# tls.client-cert-file, tls.client-key-file, http.token, logging.file-path,
# logging.rotation) are omitted below — TOML has no null literal, so an
# unset value is represented by the key's absence. See reference/configuration
# for their syntax and defaults.
";

fn default_toml_impl() -> String {
    let body = toml::to_string(&Config::default())
        .expect("Config::default() must serialize to TOML — all fields are TOML-representable");
    format!("{DEFAULT_TOML_HEADER}\n{body}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `default_toml_impl()` — and therefore `--generate-config` and
    /// docs-gen's `example-config.toml` — must always be a faithful
    /// serialization of `Config::default()`. This is a regression guard: if
    /// `default_toml_impl()` is ever changed back to a hand-maintained
    /// string literal, this test fails the moment it falls out of sync with
    /// `Config`, at the key level (missing sections, missing keys, or wrong
    /// default values), not just the section level.
    #[test]
    fn default_toml_round_trips_to_config_default() {
        let text = default_toml_impl();
        let parsed: Config =
            toml::from_str(&text).expect("default_toml_impl() output must parse as a valid Config");
        let parsed_json = serde_json::to_value(&parsed).expect("Config must serialize to JSON");
        let default_json =
            serde_json::to_value(Config::default()).expect("Config must serialize to JSON");
        assert_eq!(
            parsed_json, default_json,
            "default_toml_impl() output does not round-trip to Config::default(); \
             if this is a hand-maintained string again, prefer regenerating it via \
             `toml::to_string(&Config::default())` instead of hand-editing"
        );
    }

    /// Every top-level section of `Config` (i.e. every field on the `Config`
    /// struct that isn't `#[serde(skip)]`) must appear as a `[section]`
    /// header in the generated default TOML. This is section-level coverage
    /// specifically for the sections that regressed before this test existed:
    /// blocking, tiered-storage, compat, debug-bundle, monitor.
    #[test]
    fn default_toml_contains_every_config_section() {
        let text = default_toml_impl();
        let default_json = serde_json::to_value(Config::default()).expect("Config must serialize");
        let sections = default_json
            .as_object()
            .expect("Config serializes to a JSON object");
        // Floor lowered 25 -> 24 in the issue-14 consolidation pass: the
        // config-wiring lanes deleted the dead `vll` and `hotshards` sections,
        // so 24 is the honest current section count. Raise this floor when
        // sections are added; only lower it when one is deliberately removed.
        assert!(
            sections.len() >= 24,
            "expected at least 24 top-level config sections, found {} — \
             this sanity floor should be raised, not deleted, if it fails",
            sections.len()
        );
        for section in sections.keys() {
            let header = format!("[{section}]");
            assert!(
                text.contains(&header),
                "generated default TOML is missing section `{header}`"
            );
        }
    }

    /// Names sections that were missing from the hand-maintained
    /// `default_toml_impl()` string prior to this fix, so a regression to a
    /// hand-maintained (and incomplete) string is caught with a specific,
    /// readable failure rather than only the generic round-trip assertion.
    #[test]
    fn default_toml_contains_previously_missing_sections() {
        let text = default_toml_impl();
        for section in [
            "blocking",
            "tiered-storage",
            "compat",
            "debug-bundle",
            "monitor",
        ] {
            assert!(
                text.contains(&format!("[{section}]")),
                "generated default TOML is missing section `[{section}]`"
            );
        }
    }
}
