//! Replication configuration.

use anyhow::Result;
use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Replication configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "replication")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ReplicationConfigSection {
    /// Replication role: "standalone", "primary", or "replica".
    /// - standalone: No replication
    /// - primary: Accept replica connections
    /// - replica: Connect to a primary
    #[serde(default = "default_replication_role")]
    #[param(skip)]
    // skip: replication role set via REPLICAOF/failover, not CONFIG (Redis has no role param)
    pub role: String,

    /// Primary host (for replica role).
    /// When role is "replica", this specifies the primary to connect to.
    #[serde(default)]
    #[param(skip)] // skip: bootstrap replication topology; set via REPLICAOF, not CONFIG
    pub primary_host: String,

    /// Primary port (for replica role).
    #[serde(default = "default_primary_port")]
    #[param(skip)] // skip: bootstrap replication topology; set via REPLICAOF, not CONFIG
    pub primary_port: u16,

    /// Minimum replicas required to acknowledge writes (for primary role).
    /// If set > 0, writes will wait for this many replicas to acknowledge
    /// before returning success.
    #[serde(default)]
    #[param(mutable)]
    pub min_replicas_to_write: u32,

    /// ACK-freshness window for [`Self::min_replicas_to_write`], in
    /// milliseconds: a streaming replica counts as "good" only while its last
    /// ACK is newer than this. `0` disables the freshness check entirely (every
    /// streaming replica counts), which is Redis's documented
    /// `min-replicas-max-lag 0` meaning and is deliberately kept.
    ///
    /// Milliseconds is the native unit and the one CONFIG name
    /// `min-replicas-max-lag-ms` serves losslessly. Redis's seconds-valued
    /// `min-replicas-max-lag` is registered separately as a virtual alias over
    /// this same value; it is the *alias* that rounds, so this field — the one
    /// CONFIG REWRITE persists — always holds the exact window.
    #[serde(default = "default_min_replicas_timeout_ms")]
    #[param(mutable, name = "min-replicas-max-lag-ms")]
    pub min_replicas_timeout_ms: u64,

    /// ACK interval - how often replicas send ACKs to primary (milliseconds).
    #[serde(default = "default_ack_interval_ms")]
    // issue-14: consumed at replica boot (ACK tick cadence); immutable CONFIG GET-only.
    #[param(name = "repl-ack-interval-ms")]
    pub ack_interval_ms: u64,

    /// Replication state file path.
    /// Stores replication ID and offset for partial sync recovery.
    #[serde(default = "default_replication_state_file")]
    #[param(skip)]
    // skip: internal replication state file path; no Redis CONFIG analogue, no operator story
    pub state_file: String,

    /// Connection timeout for replica connecting to primary (milliseconds).
    #[serde(default = "default_connect_timeout_ms")]
    #[param(skip)]
    // skip: borderline: internal replica-connect timeout Redis folds into repl-timeout
    pub connect_timeout_ms: u64,

    /// Handshake timeout during replication setup (milliseconds).
    #[serde(default = "default_handshake_timeout_ms")]
    #[param(skip)]
    // skip: borderline: internal replication-handshake timeout Redis folds into repl-timeout
    pub handshake_timeout_ms: u64,

    /// Reconnection backoff - initial delay (milliseconds).
    #[serde(default = "default_reconnect_backoff_initial_ms")]
    #[param(skip)]
    // skip: borderline: FrogDB reconnect backoff; Redis has no reconnect-backoff CONFIG
    pub reconnect_backoff_initial_ms: u64,

    /// Reconnection backoff - maximum delay (milliseconds).
    #[serde(default = "default_reconnect_backoff_max_ms")]
    #[param(skip)]
    // skip: borderline: FrogDB reconnect backoff; Redis has no reconnect-backoff CONFIG
    pub reconnect_backoff_max_ms: u64,

    /// Max replication lag in bytes before proactive disconnect. 0 = disabled.
    #[serde(default)]
    #[param(mutable)]
    pub replication_lag_threshold_bytes: u64,

    /// Max replication lag in seconds (since last ACK) before proactive disconnect. 0 = disabled.
    #[serde(default)]
    #[param(mutable)]
    pub replication_lag_threshold_secs: u64,

    /// Cooldown seconds after proactive lag disconnect before allowing another.
    #[serde(default = "default_fullresync_cooldown_secs")]
    #[param(skip)]
    // skip: borderline: FrogDB-internal lag-disconnect cooldown; no Redis analogue
    pub fullresync_cooldown_secs: u64,

    /// Write a divergent-writes audit file when a demoted primary is found to
    /// have diverged from the new primary (log-only).
    ///
    /// This flag controls ONLY that file. It does not gate the demotion, and —
    /// since issue 14 — it does not gate the replication backlog either: the
    /// backlog has its own [`Self::backlog_enabled`], so turning this off stops
    /// log files accumulating without costing every reconnecting replica a full
    /// resync. Automatic Role Demotion during failover always runs in cluster
    /// mode regardless of this setting; the kill-switch for cluster behavior is
    /// `cluster.enabled`, not this flag.
    #[serde(default = "default_split_brain_log_enabled")]
    #[param(skip)]
    // skip: FrogDB-specific split-brain discarded-writes logging toggle; diagnostic, no Redis analogue
    pub split_brain_log_enabled: bool,

    /// Whether the replication backlog is populated at all.
    ///
    /// The backlog is the ring of recent commands a `+CONTINUE` replays from
    /// (Redis's `repl-backlog`), and the same ring the split-brain audit reads
    /// its divergent writes out of. `false` means every reconnecting replica
    /// pays for a full checkpoint transfer, so it is an availability knob, not a
    /// diagnostic one — which is exactly why it is no longer spelled
    /// [`Self::split_brain_log_enabled`].
    #[serde(default = "default_backlog_enabled")]
    #[param(skip)]
    // skip: capacity is fixed when the ring is built, so CONFIG SET could only report a
    // change the running buffer never made; Redis's live `repl-backlog-size` resize is unbuilt
    pub backlog_enabled: bool,

    /// Maximum number of recent commands the replication backlog retains — the
    /// entry-count half of its two caps (see [`Self::backlog_max_mb`]).
    ///
    /// Bounds how far a replica may fall behind and still reconnect with a
    /// `+CONTINUE`. Must be > 0.
    #[serde(default = "default_backlog_size")]
    #[param(skip)]
    // skip: capacity is fixed when the ring is built, so CONFIG SET could only report a
    // change the running buffer never made; Redis's live `repl-backlog-size` resize is unbuilt
    pub backlog_size: usize,

    /// Maximum memory in MB the replication backlog retains — the byte half of
    /// its two caps (Redis's `repl-backlog-size`, which is bytes-only).
    ///
    /// Must be > 0, and small enough that its byte form fits a `usize`.
    #[serde(default = "default_backlog_max_mb")]
    #[param(skip)]
    // skip: capacity is fixed when the ring is built, so CONFIG SET could only report a
    // change the running buffer never made; Redis's live `repl-backlog-size` resize is unbuilt
    pub backlog_max_mb: usize,

    /// Reject writes when primary loses all replica ACK freshness, with
    /// `SELFFENCE writes rejected: no fresh streaming replica
    /// (self-fence-on-replica-loss)`. Prevents zombie writes during network
    /// partitions.
    ///
    /// The fence arms only once a replica has actually streamed, and it drops
    /// again when the last streaming replica leaves *cleanly* (an orderly
    /// close, `REPLICAOF NO ONE`, a primary-initiated teardown), so a
    /// deliberate decommission does not fence the primary. A replica that was
    /// lost — killed, partitioned, or disconnected for lag — or one that is
    /// still attached but silent keeps it engaged until a fresh replica
    /// streams again.
    #[serde(default = "default_self_fence_on_replica_loss")]
    #[param(mutable)]
    pub self_fence_on_replica_loss: bool,

    /// Freshness timeout for replica ACKs (ms).
    /// If no replica ACKs within this window, the primary fences itself.
    /// Should be >= 3x ack_interval_ms to tolerate missed ACKs.
    #[serde(default = "default_replica_freshness_timeout_ms")]
    #[param(mutable)]
    pub replica_freshness_timeout_ms: u64,

    /// Seconds with zero connected replicas after which the replication backlog
    /// is freed and its resume window closed (Redis `repl-backlog-ttl`).
    /// 0 = keep the backlog forever.
    ///
    /// Freeing costs a reconnecting replica a full resync instead of a
    /// `+CONTINUE`; keeping it costs ring-buffer memory and a push per write for
    /// resume history nobody may ever ask for.
    #[serde(default = "default_backlog_ttl_secs")]
    #[param(mutable, name = "repl-backlog-ttl")]
    pub backlog_ttl_secs: u64,

    /// Write timeout for streaming to replicas (ms). 0 = disabled.
    /// Forces TCP disconnect when iptables drops packets.
    #[serde(default = "default_replica_write_timeout_ms")]
    #[param(skip)]
    // skip: borderline: internal replica-stream write timeout Redis folds into repl-timeout
    pub replica_write_timeout_ms: u64,

    /// Most commands a replica buffers for one replicated `MULTI` before it
    /// gives up on the `EXEC` that would close it.
    ///
    /// A group is held in memory until its `EXEC` arrives, so a primary that
    /// never sends one would otherwise grow the replica's buffer without limit.
    /// On breach the group is dropped and the link is forced back through a full
    /// resync. Sized to sit far above any real transaction — a group this long
    /// is a broken stream, not a big one.
    #[serde(default = "default_replica_txn_max_commands")]
    #[param(skip)]
    // skip: internal replica-side MULTI reconstruction ceiling; no Redis analogue
    pub replica_txn_max_commands: usize,

    /// Most stream bytes one buffered replicated `MULTI` may account for, the
    /// byte-sized half of [`Self::replica_txn_max_commands`]. Needed separately
    /// because a few very large values breach memory long before the command
    /// count does.
    #[serde(default = "default_replica_txn_max_bytes")]
    #[param(skip)]
    // skip: internal replica-side MULTI reconstruction ceiling; no Redis analogue
    pub replica_txn_max_bytes: u64,
}

fn default_replication_role() -> String {
    "standalone".to_string()
}

pub const DEFAULT_PRIMARY_PORT: u16 = 6379;
pub const DEFAULT_MIN_REPLICAS_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_ACK_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10000;
pub const DEFAULT_RECONNECT_BACKOFF_INITIAL_MS: u64 = 100;
pub const DEFAULT_RECONNECT_BACKOFF_MAX_MS: u64 = 30000;
pub const DEFAULT_SPLIT_BRAIN_LOG_ENABLED: bool = true;
/// Mirrors `frogdb_replication::BacklogConfig::default().enabled`.
pub const DEFAULT_BACKLOG_ENABLED: bool = true;
/// Mirrors `frogdb_replication::BacklogConfig::default().max_entries`.
pub const DEFAULT_BACKLOG_SIZE: usize = 10_000;
/// Mirrors `frogdb_replication::BacklogConfig::default().max_bytes`, in MB.
pub const DEFAULT_BACKLOG_MAX_MB: usize = 64;
pub const DEFAULT_SELF_FENCE_ON_REPLICA_LOSS: bool = true;
pub const DEFAULT_REPLICA_FRESHNESS_TIMEOUT_MS: u64 = 3000;
pub const DEFAULT_REPLICA_WRITE_TIMEOUT_MS: u64 = 5000;
/// Redis's `repl-backlog-ttl` default: one hour with no replicas.
pub const DEFAULT_BACKLOG_TTL_SECS: u64 = 3600;
/// Mirrors `frogdb_replication::DEFAULT_REPLICA_TXN_MAX_COMMANDS`.
pub const DEFAULT_REPLICA_TXN_MAX_COMMANDS: usize = 1_000_000;
/// Mirrors `frogdb_replication::DEFAULT_REPLICA_TXN_MAX_BYTES` — 1 GiB.
pub const DEFAULT_REPLICA_TXN_MAX_BYTES: u64 = 1024 * 1024 * 1024;

fn default_primary_port() -> u16 {
    DEFAULT_PRIMARY_PORT
}

fn default_min_replicas_timeout_ms() -> u64 {
    DEFAULT_MIN_REPLICAS_TIMEOUT_MS
}

fn default_ack_interval_ms() -> u64 {
    DEFAULT_ACK_INTERVAL_MS
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

fn default_backlog_enabled() -> bool {
    DEFAULT_BACKLOG_ENABLED
}

fn default_backlog_size() -> usize {
    DEFAULT_BACKLOG_SIZE
}

fn default_backlog_max_mb() -> usize {
    DEFAULT_BACKLOG_MAX_MB
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

fn default_backlog_ttl_secs() -> u64 {
    DEFAULT_BACKLOG_TTL_SECS
}

fn default_replica_txn_max_commands() -> usize {
    DEFAULT_REPLICA_TXN_MAX_COMMANDS
}

fn default_replica_txn_max_bytes() -> u64 {
    DEFAULT_REPLICA_TXN_MAX_BYTES
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
            state_file: default_replication_state_file(),
            connect_timeout_ms: default_connect_timeout_ms(),
            handshake_timeout_ms: default_handshake_timeout_ms(),
            reconnect_backoff_initial_ms: default_reconnect_backoff_initial_ms(),
            reconnect_backoff_max_ms: default_reconnect_backoff_max_ms(),
            replication_lag_threshold_bytes: 0,
            replication_lag_threshold_secs: 0,
            fullresync_cooldown_secs: default_fullresync_cooldown_secs(),
            split_brain_log_enabled: default_split_brain_log_enabled(),
            backlog_enabled: default_backlog_enabled(),
            backlog_size: default_backlog_size(),
            backlog_max_mb: default_backlog_max_mb(),
            self_fence_on_replica_loss: default_self_fence_on_replica_loss(),
            replica_freshness_timeout_ms: default_replica_freshness_timeout_ms(),
            replica_write_timeout_ms: default_replica_write_timeout_ms(),
            backlog_ttl_secs: default_backlog_ttl_secs(),
            replica_txn_max_commands: default_replica_txn_max_commands(),
            replica_txn_max_bytes: default_replica_txn_max_bytes(),
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

        // Matches the CONFIG SET rule: 0 would mean every replica is instantly
        // stale, fencing writes permanently.
        if self.replica_freshness_timeout_ms == 0 {
            anyhow::bail!("replication.replica_freshness_timeout_ms must be > 0");
        }

        // A zero ceiling would abandon every replicated transaction, including
        // the legitimate ones — a disabled bound is the bug this exists to fix,
        // so there is no "0 = unlimited" reading of these.
        if self.replica_txn_max_commands == 0 {
            anyhow::bail!("replication.replica_txn_max_commands must be > 0");
        }

        if self.replica_txn_max_bytes == 0 {
            anyhow::bail!("replication.replica_txn_max_bytes must be > 0");
        }

        // Both backlog caps are eviction bounds, and an eviction loop cannot
        // drain below empty: `0` is not "no backlog" (that is
        // `backlog_enabled = false`), it is a cap the loop can never satisfy.
        // `ReplicationRingBuffer::push` no longer hangs on it, but a buffer that
        // retains one command is not a backlog either, so it is refused here.
        if self.backlog_size == 0 {
            anyhow::bail!(
                "replication.backlog_size must be > 0 (use backlog_enabled = false to disable the backlog)"
            );
        }

        if self.backlog_max_mb == 0 {
            anyhow::bail!(
                "replication.backlog_max_mb must be > 0 (use backlog_enabled = false to disable the backlog)"
            );
        }

        if self.backlog_max_bytes().is_none() {
            anyhow::bail!(
                "replication.backlog_max_mb ({}) overflows a usize when converted to bytes",
                self.backlog_max_mb
            );
        }

        let recommended_minimum = self.ack_interval_ms.saturating_mul(3);
        if self.self_fence_on_replica_loss
            && self.replica_freshness_timeout_ms < recommended_minimum
        {
            tracing::warn!(
                replica_freshness_timeout_ms = self.replica_freshness_timeout_ms,
                ack_interval_ms = self.ack_interval_ms,
                recommended_minimum,
                "replica_freshness_timeout_ms is less than 3x ack_interval_ms; \
                 this may cause spurious write rejections"
            );
        }

        Ok(())
    }

    /// [`Self::backlog_max_mb`] in bytes, or `None` if the conversion overflows
    /// a `usize`.
    ///
    /// The MB→byte multiplication used to be an unchecked `* 1024 * 1024` at
    /// the wiring site, where a wrapped product would silently hand the ring
    /// buffer a tiny (or zero) byte cap — the caller least able to notice. It
    /// lives here so `validate()` and the wiring share one spelling and the
    /// failure is a boot error rather than a mis-sized backlog.
    pub fn backlog_max_bytes(&self) -> Option<usize> {
        self.backlog_max_mb.checked_mul(1024 * 1024)
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

    // FM-REPLICATION-045
    #[test]
    fn zero_replicated_txn_ceilings_are_rejected() {
        // A ceiling of 0 abandons every replicated transaction, legal ones
        // included — "unlimited" is the bug these exist to close, so neither
        // axis may be read that way.
        for (label, config) in [
            (
                "replica_txn_max_commands",
                ReplicationConfigSection {
                    replica_txn_max_commands: 0,
                    ..Default::default()
                },
            ),
            (
                "replica_txn_max_bytes",
                ReplicationConfigSection {
                    replica_txn_max_bytes: 0,
                    ..Default::default()
                },
            ),
        ] {
            let err = config.validate().unwrap_err();
            assert!(err.to_string().contains(label), "{label}: {err}");
        }
    }

    // FM-REPLICATION-047
    #[test]
    fn zero_backlog_caps_are_rejected_and_the_mb_conversion_is_checked() {
        // `0` on either cap is an eviction bound the loop can never satisfy —
        // it used to spin `ReplicationRingBuffer::push` forever under the
        // entries lock. "No backlog" is spelled `backlog_enabled = false`, so
        // neither cap has a "0 = disabled" reading.
        for (label, config) in [
            (
                "backlog_size",
                ReplicationConfigSection {
                    backlog_size: 0,
                    ..Default::default()
                },
            ),
            (
                "backlog_max_mb",
                ReplicationConfigSection {
                    backlog_max_mb: 0,
                    ..Default::default()
                },
            ),
        ] {
            let err = config.validate().unwrap_err();
            assert!(err.to_string().contains(label), "{label}: {err}");
            assert!(
                err.to_string().contains("backlog_enabled"),
                "{label}: the error must point at the real disable switch: {err}"
            );
        }

        // The MB→byte conversion is checked, so an absurd-but-typeable value is
        // a boot error rather than a wrapped (and possibly zero) byte cap.
        let overflowing = ReplicationConfigSection {
            backlog_max_mb: usize::MAX,
            ..Default::default()
        };
        assert!(overflowing.backlog_max_bytes().is_none());
        let err = overflowing.validate().unwrap_err();
        assert!(err.to_string().contains("overflows"), "{err}");

        // The default is expressible and is the value the ring buffer is built
        // with, so the wiring has a byte cap to read.
        let ok = ReplicationConfigSection::default();
        assert_eq!(
            ok.backlog_max_bytes(),
            Some(DEFAULT_BACKLOG_MAX_MB * 1024 * 1024)
        );
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn zero_replica_freshness_timeout_is_rejected() {
        // CONFIG SET rejects 0; boot accepting it meant a config file could set
        // a value the running server would refuse to be talked into.
        let config = ReplicationConfigSection {
            replica_freshness_timeout_ms: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("replica_freshness_timeout_ms"),
            "{err}"
        );
    }

    /// The freshness/ack-interval advisory must not panic on absurd input.
    #[test]
    fn freshness_advisory_does_not_overflow() {
        let config = ReplicationConfigSection {
            self_fence_on_replica_loss: true,
            ack_interval_ms: u64::MAX,
            replica_freshness_timeout_ms: 1,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
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
