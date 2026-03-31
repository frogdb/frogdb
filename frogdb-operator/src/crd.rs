//! FrogDB Custom Resource Definition types.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// FrogDB custom resource specification.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "frogdb.io",
    version = "v1alpha1",
    kind = "FrogDB",
    plural = "frogdbs",
    status = "FrogDBStatus",
    namespaced,
    printcolumn = r#"{"name":"Mode","type":"string","jsonPath":".spec.mode"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct FrogDBSpec {
    /// Deployment mode: "standalone" or "cluster".
    #[serde(default = "default_mode")]
    pub mode: String,

    /// Number of replicas.
    /// - standalone mode: 1 = single instance, >1 = primary + replicas
    /// - cluster mode: must be odd and >= 3
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Container image configuration.
    #[serde(default)]
    pub image: ImageSpec,

    /// FrogDB configuration (maps to frogdb.toml sections).
    #[serde(default)]
    pub config: FrogDBConfigSpec,

    /// Cluster-specific configuration (only used when mode=cluster).
    #[serde(default)]
    pub cluster: Option<ClusterSpec>,

    /// Resource requests and limits.
    #[serde(default)]
    pub resources: Option<ResourceRequirements>,

    /// Persistent storage configuration.
    #[serde(default)]
    pub storage: StorageSpec,

    /// Pod disruption budget configuration.
    #[serde(default)]
    pub pod_disruption_budget: PDBSpec,

    /// Rolling upgrade configuration.
    #[serde(default)]
    pub upgrade: Option<UpgradeSpec>,
}

fn default_mode() -> String {
    "standalone".to_string()
}

fn default_replicas() -> i32 {
    1
}

/// Container image configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImageSpec {
    /// Container image repository.
    #[serde(default = "default_image_repository")]
    pub repository: String,

    /// Container image tag.
    #[serde(default = "default_image_tag")]
    pub tag: String,

    /// Image pull policy.
    #[serde(default = "default_pull_policy")]
    pub pull_policy: String,
}

fn default_image_repository() -> String {
    "ghcr.io/nathanjordan/frogdb".to_string()
}

fn default_image_tag() -> String {
    "latest".to_string()
}

fn default_pull_policy() -> String {
    "IfNotPresent".to_string()
}

impl Default for ImageSpec {
    fn default() -> Self {
        Self {
            repository: default_image_repository(),
            tag: default_image_tag(),
            pull_policy: default_pull_policy(),
        }
    }
}

/// FrogDB application configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FrogDBConfigSpec {
    /// Server listen port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Number of data shards.
    #[serde(default = "default_num_shards")]
    pub num_shards: usize,

    /// Persistence configuration.
    #[serde(default)]
    pub persistence: PersistenceSpec,

    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsSpec,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingSpec,

    /// Memory configuration.
    #[serde(default)]
    pub memory: MemorySpec,
}

impl Default for FrogDBConfigSpec {
    fn default() -> Self {
        Self {
            port: default_port(),
            num_shards: default_num_shards(),
            persistence: PersistenceSpec::default(),
            metrics: MetricsSpec::default(),
            logging: LoggingSpec::default(),
            memory: MemorySpec::default(),
        }
    }
}

fn default_port() -> u16 {
    6379
}

fn default_num_shards() -> usize {
    1
}

/// Persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceSpec {
    /// Enable persistence.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Durability mode: "async", "periodic", or "sync".
    #[serde(default = "default_durability_mode")]
    pub durability_mode: String,
}

fn default_true() -> bool {
    true
}

fn default_durability_mode() -> String {
    "periodic".to_string()
}

impl Default for PersistenceSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            durability_mode: default_durability_mode(),
        }
    }
}

/// Metrics configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSpec {
    /// Enable metrics endpoint.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metrics port.
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

fn default_metrics_port() -> u16 {
    9090
}

impl Default for MetricsSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            port: default_metrics_port(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoggingSpec {
    /// Log level.
    #[serde(default = "default_log_level")]
    pub level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LoggingSpec {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

/// Memory configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MemorySpec {
    /// Maximum memory in bytes (0 = unlimited).
    #[serde(default)]
    pub maxmemory: u64,

    /// Eviction policy.
    #[serde(default = "default_policy")]
    pub policy: String,
}

impl Default for MemorySpec {
    fn default() -> Self {
        Self {
            maxmemory: 0,
            policy: default_policy(),
        }
    }
}

fn default_policy() -> String {
    "noeviction".to_string()
}

/// Rolling upgrade configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeSpec {
    /// Minimum delay between pod restarts during rolling upgrade (seconds).
    #[serde(default = "default_min_upgrade_delay")]
    pub min_upgrade_delay_secs: u64,

    /// Whether to auto-finalize after all pods are upgraded.
    #[serde(default)]
    pub auto_finalize: bool,
}

fn default_min_upgrade_delay() -> u64 {
    30
}

impl Default for UpgradeSpec {
    fn default() -> Self {
        Self {
            min_upgrade_delay_secs: default_min_upgrade_delay(),
            auto_finalize: false,
        }
    }
}

/// Cluster-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSpec {
    /// Cluster bus port for Raft communication.
    #[serde(default = "default_bus_port")]
    pub bus_port: u16,

    /// Election timeout in milliseconds.
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,

    /// Heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    /// Enable automatic failover.
    #[serde(default)]
    pub auto_failover: bool,
}

fn default_bus_port() -> u16 {
    16379
}

fn default_election_timeout() -> u64 {
    1000
}

fn default_heartbeat_interval() -> u64 {
    250
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            bus_port: default_bus_port(),
            election_timeout_ms: default_election_timeout(),
            heartbeat_interval_ms: default_heartbeat_interval(),
            auto_failover: false,
        }
    }
}

/// Resource requirements (using String values for k8s resource quantities like "500m", "1Gi").
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    /// Resource requests (e.g. {"cpu": "500m", "memory": "1Gi"}).
    #[serde(default)]
    pub requests: Option<BTreeMap<String, String>>,

    /// Resource limits (e.g. {"cpu": "2", "memory": "4Gi"}).
    #[serde(default)]
    pub limits: Option<BTreeMap<String, String>>,
}

/// Storage configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// Storage size.
    #[serde(default = "default_storage_size")]
    pub size: String,

    /// Storage class name (empty = default).
    #[serde(default)]
    pub storage_class: String,
}

fn default_storage_size() -> String {
    "10Gi".to_string()
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            size: default_storage_size(),
            storage_class: String::new(),
        }
    }
}

/// Pod disruption budget configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PDBSpec {
    /// Enable PDB.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum available pods. Auto-set to quorum for cluster mode.
    #[serde(default = "default_min_available")]
    pub min_available: Option<i32>,
}

fn default_min_available() -> Option<i32> {
    Some(1)
}

impl Default for PDBSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            min_available: default_min_available(),
        }
    }
}

/// FrogDB status subresource.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FrogDBStatus {
    /// Total desired replicas.
    #[serde(default)]
    pub replicas: i32,

    /// Number of ready replicas.
    #[serde(default)]
    pub ready_replicas: i32,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Name of the primary pod (standalone mode with replicas or cluster mode).
    #[serde(default)]
    pub primary_pod: Option<String>,

    /// Whether a rolling upgrade is in progress.
    #[serde(default)]
    pub upgrade_in_progress: bool,

    /// The current image tag (version) running on the cluster.
    #[serde(default)]
    pub current_version: Option<String>,

    /// The target image tag (version) being rolled out.
    #[serde(default)]
    pub target_version: Option<String>,

    /// Status conditions.
    #[serde(default)]
    pub conditions: Vec<FrogDBCondition>,
}

/// A condition on the FrogDB resource.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FrogDBCondition {
    /// Type of condition: Available, Progressing, Degraded.
    #[serde(rename = "type")]
    pub type_: String,

    /// Status: "True", "False", or "Unknown".
    pub status: String,

    /// Last time the condition transitioned.
    #[serde(default)]
    pub last_transition_time: Option<String>,

    /// Reason for the condition.
    #[serde(default)]
    pub reason: Option<String>,

    /// Human-readable message.
    #[serde(default)]
    pub message: Option<String>,
}

impl FrogDBSpec {
    /// Effective PDB minAvailable, auto-set to quorum for cluster mode.
    pub fn effective_min_available(&self) -> i32 {
        if self.mode == "cluster" {
            self.replicas / 2 + 1
        } else {
            self.pod_disruption_budget.min_available.unwrap_or(1)
        }
    }

    /// Validate the spec.
    pub fn validate(&self) -> Result<(), String> {
        match self.mode.as_str() {
            "standalone" => {
                if self.replicas < 1 {
                    return Err("replicas must be >= 1 for standalone mode".into());
                }
            }
            "cluster" => {
                if self.replicas < 3 {
                    return Err("replicas must be >= 3 for cluster mode".into());
                }
                if self.replicas % 2 == 0 {
                    return Err("replicas must be odd for cluster mode (Raft quorum)".into());
                }
            }
            other => {
                return Err(format!(
                    "invalid mode '{}', expected 'standalone' or 'cluster'",
                    other
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_spec() {
        let spec = default_spec();
        assert_eq!(spec.mode, "standalone");
        assert_eq!(spec.replicas, 1);
        assert_eq!(spec.image.repository, "ghcr.io/nathanjordan/frogdb");
        assert!(spec.upgrade.is_none());
    }

    #[test]
    fn test_validate_standalone() {
        let spec = FrogDBSpec {
            mode: "standalone".into(),
            replicas: 1,
            ..default_spec()
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_validate_cluster_odd() {
        let spec = FrogDBSpec {
            mode: "cluster".into(),
            replicas: 3,
            ..default_spec()
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_validate_cluster_even_fails() {
        let spec = FrogDBSpec {
            mode: "cluster".into(),
            replicas: 4,
            ..default_spec()
        };
        assert!(spec.validate().is_err());
    }

    #[test]
    fn test_validate_cluster_too_few() {
        let spec = FrogDBSpec {
            mode: "cluster".into(),
            replicas: 1,
            ..default_spec()
        };
        assert!(spec.validate().is_err());
    }

    #[test]
    fn test_effective_min_available_cluster() {
        let spec = FrogDBSpec {
            mode: "cluster".into(),
            replicas: 5,
            ..default_spec()
        };
        assert_eq!(spec.effective_min_available(), 3);
    }

    #[test]
    fn test_effective_min_available_standalone() {
        let spec = FrogDBSpec {
            mode: "standalone".into(),
            replicas: 3,
            ..default_spec()
        };
        assert_eq!(spec.effective_min_available(), 1);
    }

    fn default_spec() -> FrogDBSpec {
        FrogDBSpec {
            mode: default_mode(),
            replicas: default_replicas(),
            image: ImageSpec::default(),
            config: FrogDBConfigSpec::default(),
            cluster: None,
            resources: None,
            storage: StorageSpec::default(),
            pod_disruption_budget: PDBSpec::default(),
            upgrade: None,
        }
    }

    #[test]
    fn test_upgrade_spec_defaults() {
        let upgrade = UpgradeSpec::default();
        assert_eq!(upgrade.min_upgrade_delay_secs, 30);
        assert!(!upgrade.auto_finalize);
    }

    #[test]
    fn test_spec_with_upgrade() {
        let spec = FrogDBSpec {
            upgrade: Some(UpgradeSpec {
                min_upgrade_delay_secs: 60,
                auto_finalize: true,
            }),
            ..default_spec()
        };
        assert!(spec.validate().is_ok());
        let upgrade = spec.upgrade.unwrap();
        assert_eq!(upgrade.min_upgrade_delay_secs, 60);
        assert!(upgrade.auto_finalize);
    }

    #[test]
    fn test_status_upgrade_fields() {
        let status = FrogDBStatus {
            upgrade_in_progress: true,
            current_version: Some("v0.1.0".to_string()),
            target_version: Some("v0.2.0".to_string()),
            ..Default::default()
        };
        assert!(status.upgrade_in_progress);
        assert_eq!(status.current_version.as_deref(), Some("v0.1.0"));
        assert_eq!(status.target_version.as_deref(), Some("v0.2.0"));
    }
}
