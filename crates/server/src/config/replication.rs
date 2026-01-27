//! Replication configuration.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Replication configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
}

fn default_replication_role() -> String {
    "standalone".to_string()
}

fn default_primary_port() -> u16 {
    6379
}

fn default_min_replicas_timeout_ms() -> u64 {
    5000
}

fn default_ack_interval_ms() -> u64 {
    1000
}

fn default_fullsync_timeout_secs() -> u64 {
    300
}

fn default_fullsync_max_memory_mb() -> usize {
    512
}

fn default_replication_state_file() -> String {
    "replication_state.json".to_string()
}

fn default_connect_timeout_ms() -> u64 {
    5000
}

fn default_handshake_timeout_ms() -> u64 {
    10000
}

fn default_reconnect_backoff_initial_ms() -> u64 {
    100
}

fn default_reconnect_backoff_max_ms() -> u64 {
    30000
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

        // If replica, primary_host must be specified
        if self.role.to_lowercase() == "replica" && self.primary_host.is_empty() {
            anyhow::bail!("primary_host must be specified when role is 'replica'");
        }

        // Validate timeouts
        if self.fullsync_timeout_secs == 0 {
            anyhow::bail!("fullsync_timeout_secs must be > 0");
        }

        if self.fullsync_max_memory_mb == 0 {
            anyhow::bail!("fullsync_max_memory_mb must be > 0");
        }

        if self.ack_interval_ms == 0 {
            anyhow::bail!("ack_interval_ms must be > 0");
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

    /// Convert to core ReplicationConfig.
    pub fn to_core_config(&self) -> frogdb_core::ReplicationConfig {
        match self.role.to_lowercase().as_str() {
            "primary" => frogdb_core::ReplicationConfig::Primary {
                min_replicas_to_write: self.min_replicas_to_write,
            },
            "replica" => frogdb_core::ReplicationConfig::Replica {
                primary_addr: format!("{}:{}", self.primary_host, self.primary_port),
            },
            _ => frogdb_core::ReplicationConfig::Standalone,
        }
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
        assert_eq!(config.primary_port, 6379);
        assert_eq!(config.min_replicas_to_write, 0);
        assert_eq!(config.min_replicas_timeout_ms, 5000);
        assert_eq!(config.ack_interval_ms, 1000);
        assert_eq!(config.fullsync_timeout_secs, 300);
        assert_eq!(config.fullsync_max_memory_mb, 512);
        assert_eq!(config.state_file, "replication_state.json");
        assert_eq!(config.connect_timeout_ms, 5000);
        assert_eq!(config.handshake_timeout_ms, 10000);
        assert_eq!(config.reconnect_backoff_initial_ms, 100);
        assert_eq!(config.reconnect_backoff_max_ms, 30000);
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
        let mut config = ReplicationConfigSection::default();
        config.role = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_valid_roles() {
        for role in ["standalone", "primary", "replica"] {
            let mut config = ReplicationConfigSection::default();
            config.role = role.to_string();
            if role == "replica" {
                config.primary_host = "127.0.0.1".to_string();
            }
            assert!(config.validate().is_ok(), "Role {} should be valid", role);
        }
    }

    #[test]
    fn test_replication_config_validate_replica_without_host() {
        let mut config = ReplicationConfigSection::default();
        config.role = "replica".to_string();
        config.primary_host = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_zero_fullsync_timeout() {
        let mut config = ReplicationConfigSection::default();
        config.fullsync_timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_zero_fullsync_memory() {
        let mut config = ReplicationConfigSection::default();
        config.fullsync_max_memory_mb = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_zero_ack_interval() {
        let mut config = ReplicationConfigSection::default();
        config.ack_interval_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_to_core_config() {
        // Test standalone
        let config = ReplicationConfigSection::default();
        let core = config.to_core_config();
        assert!(matches!(core, frogdb_core::ReplicationConfig::Standalone));

        // Test primary
        let mut config = ReplicationConfigSection::default();
        config.role = "primary".to_string();
        config.min_replicas_to_write = 2;
        let core = config.to_core_config();
        assert!(matches!(
            core,
            frogdb_core::ReplicationConfig::Primary { min_replicas_to_write: 2 }
        ));

        // Test replica
        let mut config = ReplicationConfigSection::default();
        config.role = "replica".to_string();
        config.primary_host = "192.168.1.1".to_string();
        config.primary_port = 6380;
        let core = config.to_core_config();
        if let frogdb_core::ReplicationConfig::Replica { primary_addr } = core {
            assert_eq!(primary_addr, "192.168.1.1:6380");
        } else {
            panic!("Expected Replica config");
        }
    }
}
