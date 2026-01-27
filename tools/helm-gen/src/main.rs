//! Helm chart code generator for FrogDB.
//!
//! Generates Helm chart files from the FrogDB configuration struct:
//! - Chart.yaml (appVersion from Cargo.toml)
//! - values.yaml (defaults from Config::default())
//! - values.schema.json (JSON Schema from schemars)
//! - templates/configmap.yaml (frogdb.toml template)

use anyhow::{Context, Result};
use clap::Parser;
use frogdb_server::config::Config;
use schemars::schema_for;
use std::fs;
use std::path::PathBuf;

const GENERATED_HEADER: &str = r#"# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: crates/server/src/config/
# Regenerate with: just helm-gen
# =============================================================================
"#;

#[derive(Parser, Debug)]
#[command(name = "helm-gen", about = "Generate Helm chart files from FrogDB config")]
struct Args {
    /// Output directory for the Helm chart
    #[arg(short, long, default_value = "deploy/helm/frogdb")]
    output: PathBuf,

    /// Check mode - verify generated files match existing ones
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Get workspace root
    let workspace_root = std::env::current_dir()?;

    // Read version from workspace Cargo.toml
    let cargo_toml_path = workspace_root.join("Cargo.toml");
    let cargo_toml = fs::read_to_string(&cargo_toml_path)
        .with_context(|| format!("Failed to read {}", cargo_toml_path.display()))?;
    let cargo: toml::Value = toml::from_str(&cargo_toml)?;
    let version = cargo
        .get("workspace")
        .and_then(|w| w.get("package"))
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .context("Failed to get workspace.package.version from Cargo.toml")?;

    let output_dir = workspace_root.join(&args.output);

    if args.check {
        check_files(&output_dir, version)?;
    } else {
        generate_files(&output_dir, version)?;
    }

    Ok(())
}

fn generate_files(output_dir: &PathBuf, version: &str) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::create_dir_all(output_dir.join("templates"))?;

    // Generate Chart.yaml
    let chart_yaml = generate_chart_yaml(version);
    let chart_path = output_dir.join("Chart.yaml");
    fs::write(&chart_path, &chart_yaml)?;
    println!("Generated: {}", chart_path.display());

    // Generate values.yaml
    let values_yaml = generate_values_yaml()?;
    let values_path = output_dir.join("values.yaml");
    fs::write(&values_path, &values_yaml)?;
    println!("Generated: {}", values_path.display());

    // Generate values.schema.json
    let schema_json = generate_schema_json()?;
    let schema_path = output_dir.join("values.schema.json");
    fs::write(&schema_path, &schema_json)?;
    println!("Generated: {}", schema_path.display());

    // Generate templates/configmap.yaml
    let configmap_yaml = generate_configmap_yaml();
    let configmap_path = output_dir.join("templates/configmap.yaml");
    fs::write(&configmap_path, &configmap_yaml)?;
    println!("Generated: {}", configmap_path.display());

    println!("\nHelm chart files generated successfully!");
    Ok(())
}

fn check_files(output_dir: &PathBuf, version: &str) -> Result<()> {
    let mut has_diff = false;

    // Check Chart.yaml
    let chart_yaml = generate_chart_yaml(version);
    let chart_path = output_dir.join("Chart.yaml");
    if check_file(&chart_path, &chart_yaml)? {
        has_diff = true;
    }

    // Check values.yaml
    let values_yaml = generate_values_yaml()?;
    let values_path = output_dir.join("values.yaml");
    if check_file(&values_path, &values_yaml)? {
        has_diff = true;
    }

    // Check values.schema.json
    let schema_json = generate_schema_json()?;
    let schema_path = output_dir.join("values.schema.json");
    if check_file(&schema_path, &schema_json)? {
        has_diff = true;
    }

    // Check templates/configmap.yaml
    let configmap_yaml = generate_configmap_yaml();
    let configmap_path = output_dir.join("templates/configmap.yaml");
    if check_file(&configmap_path, &configmap_yaml)? {
        has_diff = true;
    }

    if has_diff {
        anyhow::bail!("Generated files differ from checked-in files. Run 'just helm-gen' to regenerate.");
    }

    println!("All generated files are up to date.");
    Ok(())
}

fn check_file(path: &PathBuf, expected: &str) -> Result<bool> {
    if !path.exists() {
        eprintln!("Missing: {}", path.display());
        return Ok(true);
    }

    let actual = fs::read_to_string(path)?;
    if actual != expected {
        eprintln!("Differs: {}", path.display());
        return Ok(true);
    }

    Ok(false)
}

fn generate_chart_yaml(version: &str) -> String {
    format!(
        r#"{GENERATED_HEADER}
apiVersion: v2
name: frogdb
description: A high-performance Redis-compatible database with Raft consensus
type: application
version: {version}
appVersion: "{version}"
keywords:
  - database
  - redis
  - key-value
  - raft
  - distributed
home: https://github.com/nathanjordan/frogdb
sources:
  - https://github.com/nathanjordan/frogdb
maintainers:
  - name: Nathan Jordan
    url: https://github.com/nathanjordan
"#
    )
}

fn generate_values_yaml() -> Result<String> {
    let config = Config::default();

    // Convert Config to a structured values format for Helm
    let values = HelmValues::from_config(&config);

    let yaml = serde_yaml::to_string(&values)?;
    Ok(format!("{GENERATED_HEADER}\n{yaml}"))
}

fn generate_schema_json() -> Result<String> {
    let schema = schema_for!(HelmValues);
    let json = serde_json::to_string_pretty(&schema)?;
    Ok(json)
}

fn generate_configmap_yaml() -> String {
    format!(
        r#"{GENERATED_HEADER}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{{{ include "frogdb.fullname" . }}}}-config
  labels:
    {{{{- include "frogdb.labels" . | nindent 4 }}}}
data:
  frogdb.toml: |
    # FrogDB Configuration
    # Generated from Helm values

    [server]
    bind = "0.0.0.0"
    port = {{{{ .Values.frogdb.port }}}}
    num_shards = {{{{ .Values.frogdb.numShards }}}}
    allow_cross_slot_standalone = {{{{ .Values.frogdb.allowCrossSlotStandalone }}}}
    scatter_gather_timeout_ms = {{{{ .Values.frogdb.scatterGatherTimeoutMs }}}}

    [logging]
    level = "{{{{ .Values.frogdb.logging.level }}}}"
    format = "{{{{ .Values.frogdb.logging.format }}}}"

    [persistence]
    enabled = {{{{ .Values.frogdb.persistence.enabled }}}}
    data_dir = "{{{{ .Values.frogdb.persistence.dataDir }}}}"
    durability_mode = "{{{{ .Values.frogdb.persistence.durabilityMode }}}}"
    sync_interval_ms = {{{{ .Values.frogdb.persistence.syncIntervalMs }}}}
    write_buffer_size_mb = {{{{ .Values.frogdb.persistence.writeBufferSizeMb }}}}
    compression = "{{{{ .Values.frogdb.persistence.compression }}}}"

    [snapshot]
    snapshot_dir = "{{{{ .Values.frogdb.snapshot.dir }}}}"
    snapshot_interval_secs = {{{{ .Values.frogdb.snapshot.intervalSecs }}}}
    max_snapshots = {{{{ .Values.frogdb.snapshot.maxSnapshots }}}}

    [metrics]
    enabled = {{{{ .Values.frogdb.metrics.enabled }}}}
    bind = "0.0.0.0"
    port = {{{{ .Values.frogdb.metrics.port }}}}
    {{{{- if .Values.frogdb.metrics.otlp.enabled }}}}
    otlp_enabled = true
    otlp_endpoint = "{{{{ .Values.frogdb.metrics.otlp.endpoint }}}}"
    otlp_interval_secs = {{{{ .Values.frogdb.metrics.otlp.intervalSecs }}}}
    {{{{- end }}}}

    [admin]
    enabled = {{{{ .Values.frogdb.admin.enabled }}}}
    bind = "0.0.0.0"
    port = {{{{ .Values.frogdb.admin.port }}}}

    [memory]
    maxmemory = {{{{ .Values.frogdb.memory.maxmemory }}}}
    maxmemory_policy = "{{{{ .Values.frogdb.memory.policy }}}}"
    maxmemory_samples = {{{{ .Values.frogdb.memory.samples }}}}

    [security]
    {{{{- if .Values.frogdb.security.requirepass }}}}
    requirepass = "{{{{ .Values.frogdb.security.requirepass }}}}"
    {{{{- end }}}}

    [cluster]
    enabled = {{{{ .Values.cluster.enabled }}}}
    {{{{- if .Values.cluster.enabled }}}}
    node_id = 0
    cluster_bus_addr = "0.0.0.0:{{{{ .Values.cluster.busPort }}}}"
    data_dir = "{{{{ .Values.cluster.dataDir }}}}"
    election_timeout_ms = {{{{ .Values.cluster.electionTimeoutMs }}}}
    heartbeat_interval_ms = {{{{ .Values.cluster.heartbeatIntervalMs }}}}
    auto_failover = {{{{ .Values.cluster.autoFailover }}}}
    {{{{- end }}}}
"#
    )
}

/// Helm values structure that maps to values.yaml
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct HelmValues {
    /// Number of replicas (1 for standalone, 3+ for cluster)
    replica_count: u32,

    /// Container image configuration
    image: ImageConfig,

    /// FrogDB configuration
    frogdb: FrogdbConfig,

    /// Cluster mode configuration
    cluster: ClusterConfig,

    /// Kubernetes service configuration
    service: ServiceConfig,

    /// Resource limits and requests
    resources: ResourceConfig,

    /// Persistence volume configuration
    persistence: PersistenceVolumeConfig,

    /// Pod disruption budget
    pod_disruption_budget: PdbConfig,

    /// ServiceMonitor for Prometheus Operator
    service_monitor: ServiceMonitorConfig,

    /// Node selector
    #[serde(default)]
    node_selector: std::collections::HashMap<String, String>,

    /// Tolerations
    #[serde(default)]
    tolerations: Vec<serde_json::Value>,

    /// Affinity rules
    #[serde(default)]
    affinity: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ImageConfig {
    repository: String,
    tag: String,
    pull_policy: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct FrogdbConfig {
    port: u16,
    num_shards: usize,
    allow_cross_slot_standalone: bool,
    scatter_gather_timeout_ms: u64,
    logging: LoggingConfig,
    persistence: PersistenceConfig,
    snapshot: SnapshotConfig,
    metrics: MetricsConfig,
    admin: AdminConfig,
    memory: MemoryConfig,
    security: SecurityConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct LoggingConfig {
    level: String,
    format: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PersistenceConfig {
    enabled: bool,
    data_dir: String,
    durability_mode: String,
    sync_interval_ms: u64,
    write_buffer_size_mb: usize,
    compression: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SnapshotConfig {
    dir: String,
    interval_secs: u64,
    max_snapshots: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct MetricsConfig {
    enabled: bool,
    port: u16,
    otlp: OtlpConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct OtlpConfig {
    enabled: bool,
    endpoint: String,
    interval_secs: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AdminConfig {
    enabled: bool,
    port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct MemoryConfig {
    maxmemory: u64,
    policy: String,
    samples: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SecurityConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    requirepass: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ClusterConfig {
    enabled: bool,
    bus_port: u16,
    data_dir: String,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
    auto_failover: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ServiceConfig {
    #[serde(rename = "type")]
    service_type: String,
    port: u16,
    annotations: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ResourceConfig {
    limits: ResourceSpec,
    requests: ResourceSpec,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ResourceSpec {
    cpu: String,
    memory: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PersistenceVolumeConfig {
    enabled: bool,
    storage_class: String,
    size: String,
    access_modes: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PdbConfig {
    enabled: bool,
    min_available: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ServiceMonitorConfig {
    enabled: bool,
    interval: String,
    scrape_timeout: String,
}

impl HelmValues {
    fn from_config(config: &Config) -> Self {
        Self {
            replica_count: 1,
            image: ImageConfig {
                repository: "ghcr.io/nathanjordan/frogdb".to_string(),
                tag: "latest".to_string(),
                pull_policy: "IfNotPresent".to_string(),
            },
            frogdb: FrogdbConfig {
                port: config.server.port,
                num_shards: config.server.num_shards,
                allow_cross_slot_standalone: config.server.allow_cross_slot_standalone,
                scatter_gather_timeout_ms: config.server.scatter_gather_timeout_ms,
                logging: LoggingConfig {
                    level: config.logging.level.clone(),
                    format: config.logging.format.clone(),
                },
                persistence: PersistenceConfig {
                    enabled: config.persistence.enabled,
                    data_dir: "/data".to_string(),
                    durability_mode: config.persistence.durability_mode.clone(),
                    sync_interval_ms: config.persistence.sync_interval_ms,
                    write_buffer_size_mb: config.persistence.write_buffer_size_mb,
                    compression: config.persistence.compression.clone(),
                },
                snapshot: SnapshotConfig {
                    dir: "/data/snapshots".to_string(),
                    interval_secs: config.snapshot.snapshot_interval_secs,
                    max_snapshots: config.snapshot.max_snapshots,
                },
                metrics: MetricsConfig {
                    enabled: config.metrics.enabled,
                    port: config.metrics.port,
                    otlp: OtlpConfig {
                        enabled: config.metrics.otlp_enabled,
                        endpoint: config.metrics.otlp_endpoint.clone(),
                        interval_secs: config.metrics.otlp_interval_secs,
                    },
                },
                admin: AdminConfig {
                    enabled: config.admin.enabled,
                    port: config.admin.port,
                },
                memory: MemoryConfig {
                    maxmemory: config.memory.maxmemory,
                    policy: config.memory.maxmemory_policy.clone(),
                    samples: config.memory.maxmemory_samples,
                },
                security: SecurityConfig {
                    requirepass: if config.security.requirepass.is_empty() {
                        None
                    } else {
                        Some(config.security.requirepass.clone())
                    },
                },
            },
            cluster: ClusterConfig {
                enabled: config.cluster.enabled,
                bus_port: 16379,
                data_dir: "/data/cluster".to_string(),
                election_timeout_ms: config.cluster.election_timeout_ms,
                heartbeat_interval_ms: config.cluster.heartbeat_interval_ms,
                auto_failover: config.cluster.auto_failover,
            },
            service: ServiceConfig {
                service_type: "ClusterIP".to_string(),
                port: 6379,
                annotations: std::collections::HashMap::new(),
            },
            resources: ResourceConfig {
                limits: ResourceSpec {
                    cpu: "2".to_string(),
                    memory: "4Gi".to_string(),
                },
                requests: ResourceSpec {
                    cpu: "500m".to_string(),
                    memory: "1Gi".to_string(),
                },
            },
            persistence: PersistenceVolumeConfig {
                enabled: true,
                storage_class: "".to_string(),
                size: "10Gi".to_string(),
                access_modes: vec!["ReadWriteOnce".to_string()],
            },
            pod_disruption_budget: PdbConfig {
                enabled: true,
                min_available: 1,
            },
            service_monitor: ServiceMonitorConfig {
                enabled: false,
                interval: "30s".to_string(),
                scrape_timeout: "10s".to_string(),
            },
            node_selector: std::collections::HashMap::new(),
            tolerations: vec![],
            affinity: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}
