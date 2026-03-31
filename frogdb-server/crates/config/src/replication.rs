//! Replication configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Replication configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfigSection {
    /// Replication role: "standalone", "primary", or "replica".
    /// - standalone: No replication (default)
    /// - primary: Accept replica connections
    /// - replica: Connect to a primary
    #[serde(default = "default_replication_role")]
    pub role: String,

    /// Primary host (for replica role).
    /// When role is "replica", this specifies the primary to connect to.
    #[serde(default)]
    pub primary_host: String,

    /// Primary port (for replica role).
    #[serde(default = "default_primary_port")]
    pub primary_port: u16,

    /// Minimum replicas required to acknowledge writes (for primary role).
    /// If set > 0, writes will wait for this many replicas to acknowledge
    /// before returning success.
    #[serde(default)]
    pub min_replicas_to_write: u32,

    /// Timeout for min_replicas_to_write in milliseconds.
    /// If replicas don't acknowledge within this time, the write still succeeds
    /// but returns with fewer acknowledged replicas.
    #[serde(default = "default_min_replicas_timeout_ms")]
    pub min_replicas_timeout_ms: u64,

    /// ACK interval - how often replicas send ACKs to primary (milliseconds).
    #[serde(default = "default_ack_interval_ms")]
    pub ack_interval_ms: u64,

    /// Full sync timeout (seconds).
    /// Maximum time to wait for a full sync operation.
    #[serde(default = "default_fullsync_timeout_secs")]
    pub fullsync_timeout_secs: u64,

    /// Maximum memory for full sync buffering (MB).
    /// If exceeded, FULLRESYNC requests will be rejected.
    #[serde(default = "default_fullsync_max_memory_mb")]
    pub fullsync_max_memory_mb: usize,

    /// Replication state file path.
    /// Stores replication ID and offset for partial sync recovery.
    #[serde(default = "default_replication_state_file")]
    pub state_file: String,

    /// Connection timeout for replica connecting to primary (milliseconds).
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Handshake timeout during replication setup (milliseconds).
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,

    /// Reconnection backoff - initial delay (milliseconds).
    #[serde(default = "default_reconnect_backoff_initial_ms")]
    pub reconnect_backoff_initial_ms: u64,

    /// Reconnection backoff - maximum delay (milliseconds).
    #[serde(default = "default_reconnect_backoff_max_ms")]
    pub reconnect_backoff_max_ms: u64,

    /// Max replication lag in bytes before proactive disconnect. 0 = disabled.
    #[serde(default)]
    pub replication_lag_threshold_bytes: u64,

    /// Max replication lag in seconds (since last ACK) before proactive disconnect. 0 = disabled.
    #[serde(default)]
    pub replication_lag_threshold_secs: u64,

    /// Cooldown seconds after proactive lag disconnect before allowing another.
    #[serde(default = "default_fullresync_cooldown_secs")]
    pub fullresync_cooldown_secs: u64,

    /// Enable split-brain discarded-writes logging.
    /// When a primary is demoted, divergent writes are logged before resync.
    #[serde(default = "default_split_brain_log_enabled")]
    pub split_brain_log_enabled: bool,

    /// Maximum number of recent commands to buffer for split-brain detection.
    #[serde(default = "default_split_brain_buffer_size")]
    pub split_brain_buffer_size: usize,

    /// Maximum memory in MB for the split-brain command buffer.
    #[serde(default = "default_split_brain_buffer_max_mb")]
    pub split_brain_buffer_max_mb: usize,

    /// Reject writes when primary loses all replica ACK freshness.
    /// Prevents zombie writes during network partitions.
    #[serde(default = "default_self_fence_on_replica_loss")]
    pub self_fence_on_replica_loss: bool,

    /// Freshness timeout for replica ACKs (ms).
    /// If no replica ACKs within this window, the primary fences itself.
    /// Should be >= 3x ack_interval_ms to tolerate missed ACKs.
    #[serde(default = "default_replica_freshness_timeout_ms")]
    pub replica_freshness_timeout_ms: u64,

    /// Write timeout for streaming to replicas (ms). 0 = disabled.
    /// Forces TCP disconnect when iptables drops packets.
    #[serde(default = "default_replica_write_timeout_ms")]
    pub replica_write_timeout_ms: u64,
}

fn default_replication_role() -> String {
    "standalone".to_string()
}

pub const DEFAULT_PRIMARY_PORT: u16 = 6379;
pub const DEFAULT_MIN_REPLICAS_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_ACK_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_FULLSYNC_TIMEOUT_SECS: u64 = 300;
pub const DEFAULT_FULLSYNC_MAX_MEMORY_MB: usize = 512;
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10000;
pub const DEFAULT_RECONNECT_BACKOFF_INITIAL_MS: u64 = 100;
pub const DEFAULT_RECONNECT_BACKOFF_MAX_MS: u64 = 30000;
pub const DEFAULT_SPLIT_BRAIN_LOG_ENABLED: bool = true;
pub const DEFAULT_SPLIT_BRAIN_BUFFER_SIZE: usize = 10_000;
pub const DEFAULT_SPLIT_BRAIN_BUFFER_MAX_MB: usize = 64;
pub const DEFAULT_SELF_FENCE_ON_REPLICA_LOSS: bool = true;
pub const DEFAULT_REPLICA_FRESHNESS_TIMEOUT_MS: u64 = 3000;
pub const DEFAULT_REPLICA_WRITE_TIMEOUT_MS: u64 = 5000;

fn default_primary_port() -> u16 {
    DEFAULT_PRIMARY_PORT
}

fn default_min_replicas_timeout_ms() -> u64 {
    DEFAULT_MIN_REPLICAS_TIMEOUT_MS
}

fn default_ack_interval_ms() -> u64 {
    DEFAULT_ACK_INTERVAL_MS
}

fn default_fullsync_timeout_secs() -> u64 {
    DEFAULT_FULLSYNC_TIMEOUT_SECS
}

fn default_fullsync_max_memory_mb() -> usize {
    DEFAULT_FULLSYNC_MAX_MEMORY_MB
}

fn default_replication_state_file() -> String {
    "replication_state.json".to_string()
}

fn default_connect_timeout_ms() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_MS
}

fn default_handshake_timeout_ms() -> u64 {
    DEFAULT_HANDSHAKE_TIMEOUT_MS
}

fn default_reconnect_backoff_initial_ms() -> u64 {
    DEFAULT_RECONNECT_BACKOFF_INITIAL_MS
}

fn default_reconnect_backoff_max_ms() -> u64 {
    DEFAULT_RECONNECT_BACKOFF_MAX_MS
}

fn default_fullresync_cooldown_secs() -> u64 {
    60
}

fn default_split_brain_log_enabled() -> bool {
    DEFAULT_SPLIT_BRAIN_LOG_ENABLED
}

fn default_split_brain_buffer_size() -> usize {
    DEFAULT_SPLIT_BRAIN_BUFFER_SIZE
}

fn default_split_brain_buffer_max_mb() -> usize {
    DEFAULT_SPLIT_BRAIN_BUFFER_MAX_MB
}

fn default_self_fence_on_replica_loss() -> bool {
    DEFAULT_SELF_FENCE_ON_REPLICA_LOSS
}

fn default_replica_freshness_timeout_ms() -> u64 {
    DEFAULT_REPLICA_FRESHNESS_TIMEOUT_MS
}

fn default_replica_write_timeout_ms() -> u64 {
    DEFAULT_REPLICA_WRITE_TIMEOUT_MS
}

impl Default for ReplicationConfigSection {
    fn default() -> Self {
        Self {
            role: default_replication_role(),
            primary_host: String::new(),
            primary_port: default_primary_port(),
            min_replicas_to_write: 0,
            min_replicas_timeout_ms: default_min_replicas_timeout_ms(),
            ack_interval_ms: default_ack_interval_ms(),
            fullsync_timeout_secs: default_fullsync_timeout_secs(),
            fullsync_max_memory_mb: default_fullsync_max_memory_mb(),
            state_file: default_replication_state_file(),
            connect_timeout_ms: default_connect_timeout_ms(),
            handshake_timeout_ms: default_handshake_timeout_ms(),
            reconnect_backoff_initial_ms: default_reconnect_backoff_initial_ms(),
            reconnect_backoff_max_ms: default_reconnect_backoff_max_ms(),
            replication_lag_threshold_bytes: 0,
            replication_lag_threshold_secs: 0,
            fullresync_cooldown_secs: default_fullresync_cooldown_secs(),
            split_brain_log_enabled: default_split_brain_log_enabled(),
            split_brain_buffer_size: default_split_brain_buffer_size(),
            split_brain_buffer_max_mb: default_split_brain_buffer_max_mb(),
            self_fence_on_replica_loss: default_self_fence_on_replica_loss(),
            replica_freshness_timeout_ms: default_replica_freshness_timeout_ms(),
            replica_write_timeout_ms: default_replica_write_timeout_ms(),
        }
    }
}

impl ReplicationConfigSection {
    /// Validate the replication configuration.
    pub fn validate(&self) -> Result<()> {
        let valid_roles = ["standalone", "primary", "replica"];
        if !valid_roles.contains(&self.role.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid replication role '{}', expected one of: {}",
                self.role,
                valid_roles.join(", ")
            );
        }

        if self.role.to_lowercase() == "replica" && self.primary_host.is_empty() {
            anyhow::bail!("primary_host must be specified when role is 'replica'");
        }

        if self.fullsync_timeout_secs == 0 {
            anyhow::bail!("fullsync_timeout_secs must be > 0");
        }

        if self.fullsync_max_memory_mb == 0 {
            anyhow::bail!("fullsync_max_memory_mb must be > 0");
        }

        if self.ack_interval_ms == 0 {
            anyhow::bail!("ack_interval_ms must be > 0");
        }

        if self.connect_timeout_ms == 0 {
            anyhow::bail!("replication.connect_timeout_ms must be > 0");
        }

        if self.handshake_timeout_ms == 0 {
            anyhow::bail!("replication.handshake_timeout_ms must be > 0");
        }

        if self.reconnect_backoff_initial_ms == 0 {
            anyhow::bail!(
                "replication.reconnect_backoff_initial_ms must be > 0 (would cause tight reconnect loops)"
            );
        }

        if self.self_fence_on_replica_loss
            && self.replica_freshness_timeout_ms < self.ack_interval_ms * 3
        {
            tracing::warn!(
                replica_freshness_timeout_ms = self.replica_freshness_timeout_ms,
                ack_interval_ms = self.ack_interval_ms,
                recommended_minimum = self.ack_interval_ms * 3,
                "replica_freshness_timeout_ms is less than 3x ack_interval_ms; \
                 this may cause spurious write rejections"
            );
        }

        Ok(())
    }

    /// Check if this node is a primary.
    pub fn is_primary(&self) -> bool {
        self.role.to_lowercase() == "primary"
    }

    /// Check if this node is a replica.
    pub fn is_replica(&self) -> bool {
        self.role.to_lowercase() == "replica"
    }

    /// Check if this node is standalone.
    pub fn is_standalone(&self) -> bool {
        self.role.to_lowercase() == "standalone"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_replication_config() {
        let config = ReplicationConfigSection::default();
        assert_eq!(config.role, "standalone");
        assert!(config.primary_host.is_empty());
        assert_eq!(config.primary_port, DEFAULT_PRIMARY_PORT);
        assert_eq!(config.min_replicas_to_write, 0);
    }

    #[test]
    fn test_replication_config_role_helpers() {
        let mut config = ReplicationConfigSection::default();
        assert!(config.is_standalone());
        assert!(!config.is_primary());
        assert!(!config.is_replica());

        config.role = "primary".to_string();
        assert!(!config.is_standalone());
        assert!(config.is_primary());
        assert!(!config.is_replica());

        config.role = "replica".to_string();
        config.primary_host = "127.0.0.1".to_string();
        assert!(!config.is_standalone());
        assert!(!config.is_primary());
        assert!(config.is_replica());
    }

    #[test]
    fn test_replication_config_validate_invalid_role() {
        let config = ReplicationConfigSection {
            role: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_valid_roles() {
        for role in ["standalone", "primary", "replica"] {
            let mut config = ReplicationConfigSection {
                role: role.to_string(),
                ..Default::default()
            };
            if role == "replica" {
                config.primary_host = "127.0.0.1".to_string();
            }
            assert!(config.validate().is_ok(), "Role {} should be valid", role);
        }
    }

    #[test]
    fn test_replication_config_validate_replica_without_host() {
        let config = ReplicationConfigSection {
            role: "replica".to_string(),
            primary_host: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
