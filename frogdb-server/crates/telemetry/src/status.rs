//! Machine-readable status JSON endpoint.
//!
//! Provides a comprehensive JSON snapshot of server health accessible via:
//! - `GET /status/json` HTTP endpoint
//! - `STATUS [JSON]` Redis-protocol command
//!
//! Inspired by FoundationDB's `status json` command.

use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use frogdb_core::{
    ClientFlags, ClientRegistry, MetricsRecorder, ShardSender, ShardWalLag, WalLagAggregate,
};

use crate::definitions::{
    CommandsTotal, SnapshotInProgress, SnapshotLastTimestamp, WalBytes, WalWrites,
};
use crate::health::HealthChecker;
use crate::node_state::{NodeStateSnapshot, ShardState};

/// Deadline for the single node-state scatter behind `/status`. `/status` is a
/// best-effort health surface: a shard that misses this deadline yields an empty
/// snapshot (see [`StatusCollector::collect`]) rather than blocking the report.
const STATUS_SCATTER_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for the status collector.
#[derive(Debug, Clone)]
pub struct StatusCollectorConfig {
    /// Threshold percentage for memory warning (default 90).
    pub memory_warning_percent: u8,
    /// Threshold percentage for connection warning (default 90).
    pub connection_warning_percent: u8,
    /// Durability lag warning threshold in milliseconds (default 5000 = 5 seconds).
    pub durability_lag_warning_ms: u64,
    /// Durability lag critical threshold in milliseconds (default 30000 = 30 seconds).
    pub durability_lag_critical_ms: u64,
}

impl Default for StatusCollectorConfig {
    fn default() -> Self {
        Self {
            memory_warning_percent: 90,
            connection_warning_percent: 90,
            durability_lag_warning_ms: 5000,   // 5 seconds
            durability_lag_critical_ms: 30000, // 30 seconds
        }
    }
}

/// Top-level server status response.
#[derive(Debug, Clone, Serialize)]
pub struct ServerStatus {
    /// FrogDB server information.
    pub frogdb: FrogDbInfo,
    /// Cluster/mode information.
    pub cluster: ClusterStatus,
    /// Health status with any issues.
    pub health: HealthStatusInfo,
    /// Connected clients information.
    pub clients: ClientsStatus,
    /// Memory statistics.
    pub memory: MemoryStatus,
    /// Persistence status.
    pub persistence: PersistenceStatus,
    /// Per-shard statistics.
    pub shards: Vec<ShardStatus>,
    /// Keyspace statistics.
    pub keyspace: KeyspaceStatus,
    /// Command statistics.
    pub commands: CommandsStatus,
}

/// FrogDB server information.
#[derive(Debug, Clone, Serialize)]
pub struct FrogDbInfo {
    /// Server version.
    pub version: String,
    /// Uptime in seconds.
    pub uptime_secs: u64,
    /// Process ID.
    pub process_id: u32,
    /// Unix timestamp.
    pub timestamp: u64,
    /// ISO 8601 timestamp.
    pub timestamp_iso: String,
}

/// Cluster/mode status.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatus {
    /// Whether the database is available.
    pub database_available: bool,
    /// Operating mode (standalone, cluster, etc.).
    pub mode: String,
    /// Number of shards.
    pub num_shards: usize,
}

/// Health status with issues.
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatusInfo {
    /// Overall health status (healthy, degraded, unhealthy).
    pub status: String,
    /// List of detected issues.
    pub issues: Vec<HealthIssue>,
}

/// A health issue detected in the system.
#[derive(Debug, Clone, Serialize)]
pub struct HealthIssue {
    /// Severity level (warning, critical).
    pub severity: String,
    /// Issue code for programmatic handling.
    pub code: String,
    /// Human-readable message.
    pub message: String,
}

/// Connected clients status.
#[derive(Debug, Clone, Serialize)]
pub struct ClientsStatus {
    /// Number of currently connected clients.
    pub connected: usize,
    /// Maximum allowed clients (0 = unlimited).
    pub max_clients: usize,
    /// Number of blocked clients.
    pub blocked: usize,
}

/// Memory statistics.
#[derive(Debug, Clone, Serialize)]
pub struct MemoryStatus {
    /// Memory used by data in bytes.
    pub used_bytes: u64,
    /// Peak memory usage in bytes.
    pub peak_bytes: u64,
    /// Memory limit in bytes (0 = unlimited).
    pub limit_bytes: u64,
    /// Memory fragmentation ratio.
    pub fragmentation_ratio: f64,
    /// Resident set size in bytes (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
}

/// Persistence status.
#[derive(Debug, Clone, Serialize)]
pub struct PersistenceStatus {
    /// Whether persistence is enabled.
    pub enabled: bool,
    /// Durability mode (async, periodic, sync).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durability_mode: Option<String>,
    /// Snapshot status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotStatus>,
    /// WAL status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal: Option<WalStatus>,
}

/// Snapshot status.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotStatus {
    /// Whether a snapshot is currently in progress.
    pub in_progress: bool,
    /// Unix timestamp of last snapshot (0 if never).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_timestamp: Option<u64>,
}

/// WAL (Write-Ahead Log) status.
#[derive(Debug, Clone, Serialize)]
pub struct WalStatus {
    /// Total WAL writes since startup.
    pub total_writes: u64,
    /// Total WAL bytes written.
    pub total_bytes: u64,
    /// Lag statistics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<WalLagStatus>,
}

/// WAL lag status for durability monitoring.
#[derive(Debug, Clone, Serialize)]
pub struct WalLagStatus {
    /// Total pending operations across all shards.
    pub pending_ops_total: usize,
    /// Total pending bytes across all shards.
    pub pending_bytes_total: usize,
    /// Maximum durability lag across all shards (milliseconds).
    pub max_durability_lag_ms: u64,
    /// Average durability lag across all shards (milliseconds).
    pub avg_durability_lag_ms: f64,
    /// Total failed flush attempts across all shards since startup.
    pub flush_failures_total: u64,
    /// Total WAL entries dropped in failed flushes across all shards since
    /// startup. Losses are permanent; this never decreases.
    pub lost_ops_total: u64,
    /// False if any shard's most recent flush attempt failed.
    pub last_flush_ok: bool,
    /// Per-shard lag details.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub shards: Vec<ShardLagStatus>,
}

impl From<Option<&WalLagAggregate>> for WalLagStatus {
    /// Render the shared aggregate into the JSON status shape. `None` (no shard
    /// reported WAL stats) becomes an all-zero status, matching the historical
    /// behavior when persistence was enabled but no shard participated.
    fn from(agg: Option<&WalLagAggregate>) -> Self {
        match agg {
            None => WalLagStatus {
                pending_ops_total: 0,
                pending_bytes_total: 0,
                max_durability_lag_ms: 0,
                avg_durability_lag_ms: 0.0,
                flush_failures_total: 0,
                lost_ops_total: 0,
                last_flush_ok: true,
                shards: Vec::new(),
            },
            Some(agg) => WalLagStatus {
                pending_ops_total: agg.pending_ops,
                pending_bytes_total: agg.pending_bytes,
                max_durability_lag_ms: agg.max_durability_lag_ms,
                avg_durability_lag_ms: agg.avg_durability_lag_ms(),
                flush_failures_total: agg.flush_failures,
                lost_ops_total: agg.lost_ops,
                last_flush_ok: agg.last_flush_ok,
                shards: agg.per_shard.iter().map(ShardLagStatus::from).collect(),
            },
        }
    }
}

/// Per-shard lag status.
#[derive(Debug, Clone, Serialize)]
pub struct ShardLagStatus {
    /// Shard ID.
    pub shard_id: usize,
    /// Pending operations for this shard.
    pub pending_ops: usize,
    /// Pending bytes for this shard.
    pub pending_bytes: usize,
    /// Durability lag in milliseconds.
    pub durability_lag_ms: u64,
    /// Failed flush attempts since startup.
    pub flush_failures: u64,
    /// WAL entries dropped in failed flushes since startup.
    pub lost_ops: u64,
    /// Whether the most recent flush attempt succeeded.
    pub last_flush_ok: bool,
}

impl From<&ShardWalLag> for ShardLagStatus {
    fn from(s: &ShardWalLag) -> Self {
        ShardLagStatus {
            shard_id: s.shard_id,
            pending_ops: s.pending_ops,
            pending_bytes: s.pending_bytes,
            durability_lag_ms: s.durability_lag_ms,
            flush_failures: s.flush_failures,
            lost_ops: s.lost_ops,
            last_flush_ok: s.last_flush_ok,
        }
    }
}

/// Per-shard statistics.
///
/// A pure JSON renderer over one [`ShardState`] row from the shared
/// [`NodeStateSnapshot`] — it carries no aggregation of its own.
#[derive(Debug, Clone, Serialize)]
pub struct ShardStatus {
    /// Shard ID.
    pub id: usize,
    /// Number of keys in the shard.
    pub keys: usize,
    /// Memory used by the shard in bytes.
    pub memory_bytes: u64,
    /// Peak memory used by the shard.
    pub peak_memory_bytes: u64,
}

impl From<&ShardState> for ShardStatus {
    fn from(s: &ShardState) -> Self {
        ShardStatus {
            id: s.shard_id,
            keys: s.keys,
            memory_bytes: s.data_memory as u64,
            peak_memory_bytes: s.peak_memory,
        }
    }
}

/// Keyspace statistics.
#[derive(Debug, Clone, Serialize)]
pub struct KeyspaceStatus {
    /// Total number of keys across all shards.
    pub total_keys: usize,
    /// Total expired keys since startup.
    pub expired_keys_total: u64,
}

/// Command statistics.
#[derive(Debug, Clone, Serialize)]
pub struct CommandsStatus {
    /// Total commands processed since startup, from the same
    /// `frogdb_commands_total` counter Prometheus scrapes.
    pub total_processed: u64,
    /// Instantaneous operations per second.
    ///
    /// Omitted (`None`) rather than faked: FrogDB has no server-wide
    /// instantaneous-rate sampler on the status path (per-shard windowed rates
    /// exist only in the debug hot-shard report), so there is no accurate value
    /// to report here. Consumers derive a rate from `total_processed` deltas or
    /// scrape the Prometheus counter. Reporting a lifetime average under an
    /// "instantaneous" name would be misleading. See INFO's
    /// `instantaneous_ops_per_sec`, which is stubbed for the same reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_per_sec: Option<f64>,
}

/// Live view of this node's operating mode for the status surfaces.
///
/// Cluster mode is config-static, but the primary/replica axis flips at runtime
/// (`REPLICAOF` promote/demote via the `RoleManager`). This reads the same
/// `Arc<AtomicBool>` the write guard, `ROLE`, and INFO key on, so `/status`,
/// `STATUS JSON`, and the debug node-state provider can never disagree with INFO.
#[derive(Clone)]
pub struct LiveMode {
    cluster_enabled: bool,
    is_replica: Arc<AtomicBool>,
    /// Mode reported when the node is not currently a replica. A node that
    /// booted as a primary *or* replica passes `"primary"` here — a boot replica
    /// promoted via `REPLICAOF NO ONE` is a real primary and must match INFO's
    /// `role:master` — while a standalone node passes `"standalone"`.
    non_replica_label: &'static str,
}

impl LiveMode {
    /// Build a live mode view over the shared role flag.
    pub fn new(
        cluster_enabled: bool,
        is_replica: Arc<AtomicBool>,
        non_replica_label: &'static str,
    ) -> Self {
        Self {
            cluster_enabled,
            is_replica,
            non_replica_label,
        }
    }

    /// The current operating mode: `"cluster"` (config-static), else `"replica"`
    /// when the live role flag is set, else the non-replica label.
    pub fn current(&self) -> &'static str {
        if self.cluster_enabled {
            "cluster"
        } else if self.is_replica.load(Ordering::Acquire) {
            "replica"
        } else {
            self.non_replica_label
        }
    }

    /// Live replica-flag read, independent of the cluster label. Matches how
    /// INFO/ROLE gate primary-target reporting.
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(Ordering::Acquire)
    }
}

/// Collects status information from various server components.
pub struct StatusCollector {
    config: StatusCollectorConfig,
    health_checker: HealthChecker,
    shard_senders: Arc<Vec<ShardSender>>,
    client_registry: Arc<ClientRegistry>,
    /// Metrics recorder — the single source of truth shared with `/metrics` and
    /// INFO, so all three views of a counter can never disagree. Held as the
    /// object-safe trait (not the concrete `PrometheusRecorder`) so the collector
    /// is buildable in every server configuration: the same collector renders the
    /// HTTP `/status` endpoint and the STATUS JSON command, and when metrics are
    /// disabled the no-op recorder reports absent counters as `0`/`None` rather
    /// than faking them.
    recorder: Arc<dyn MetricsRecorder>,
    start_time: Instant,
    // Configuration values needed for status
    max_clients: Arc<AtomicU64>,
    maxmemory: u64,
    persistence_enabled: bool,
    durability_mode: String,
    mode: LiveMode,
}

impl StatusCollector {
    /// Create a new status collector.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: StatusCollectorConfig,
        health_checker: HealthChecker,
        shard_senders: Arc<Vec<ShardSender>>,
        client_registry: Arc<ClientRegistry>,
        recorder: Arc<dyn MetricsRecorder>,
        start_time: Instant,
        max_clients: Arc<AtomicU64>,
        maxmemory: u64,
        persistence_enabled: bool,
        durability_mode: String,
        mode: LiveMode,
    ) -> Self {
        Self {
            config,
            health_checker,
            shard_senders,
            client_registry,
            recorder,
            start_time,
            max_clients,
            maxmemory,
            persistence_enabled,
            durability_mode,
            mode,
        }
    }

    /// Collect the current server status.
    pub async fn collect(&self) -> ServerStatus {
        // One scatter for every per-shard observable: memory, keyspace, and WAL
        // lag are folded once into the shared node-state snapshot — the same
        // snapshot INFO and the debug UI render from, so the three surfaces can
        // never disagree. `/status` is a best-effort health surface, so a shard
        // failure yields an empty snapshot rather than erroring the report.
        let node_state =
            NodeStateSnapshot::collect(self.shard_senders.as_slice(), STATUS_SCATTER_TIMEOUT)
                .await
                .unwrap_or_default();

        // WAL lag view, only when persistence is enabled. Rendered from the
        // snapshot's shared aggregate, so it can never disagree with INFO.
        let wal_lag_stats = self
            .persistence_enabled
            .then(|| WalLagStatus::from(node_state.wal.as_ref()));

        // Build client status
        let clients_status = self.build_clients_status();

        // Build memory status
        let memory_status = self.build_memory_status(&node_state);

        // Build health status with issue detection
        let health_status =
            self.build_health_status(&clients_status, &memory_status, &wal_lag_stats);

        // Build shard status — pure renderers over the snapshot's per-shard rows.
        let shards: Vec<ShardStatus> = node_state.per_shard.iter().map(ShardStatus::from).collect();

        // Totals come straight off the snapshot aggregate. `expired_keys` is the
        // *same* accumulator INFO aggregates (active + lazy expiry); the
        // Prometheus `frogdb_keys_expired_total` counter is deliberately NOT used
        // here as it is active-only and would disagree with INFO.
        let total_keys: usize = node_state.keys;
        let expired_keys_total: u64 = node_state.expired_keys;

        // Recorder-sourced counters/gauges — the shared source of truth with
        // `/metrics` and INFO. Absent families (nothing emitted yet) read as 0.
        let wal_total_writes = self.recorder.counter_value(WalWrites::NAME).unwrap_or(0);
        let wal_total_bytes = self.recorder.counter_value(WalBytes::NAME).unwrap_or(0);
        let commands_total_processed = self
            .recorder
            .counter_value(CommandsTotal::NAME)
            .unwrap_or(0);
        let snapshot_in_progress = self
            .recorder
            .gauge_value(SnapshotInProgress::NAME)
            .is_some_and(|v| v > 0.5);
        // Last successful-snapshot unix timestamp; 0/absent means "never".
        let snapshot_last_timestamp = self
            .recorder
            .gauge_value(SnapshotLastTimestamp::NAME)
            .filter(|v| *v > 0.0)
            .map(|v| v as u64);

        // Get timestamp
        let now = SystemTime::now();
        let timestamp = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let timestamp_iso = format_iso8601(now);

        ServerStatus {
            frogdb: FrogDbInfo {
                version: env!("CARGO_PKG_VERSION").to_string(),
                uptime_secs: self.start_time.elapsed().as_secs(),
                process_id: std::process::id(),
                timestamp,
                timestamp_iso,
            },
            cluster: ClusterStatus {
                database_available: self.health_checker.check_ready().is_ok(),
                mode: self.mode.current().to_string(),
                num_shards: self.shard_senders.len(),
            },
            health: health_status,
            clients: clients_status,
            memory: memory_status,
            persistence: PersistenceStatus {
                enabled: self.persistence_enabled,
                durability_mode: if self.persistence_enabled {
                    Some(self.durability_mode.clone())
                } else {
                    None
                },
                snapshot: if self.persistence_enabled {
                    Some(SnapshotStatus {
                        in_progress: snapshot_in_progress,
                        last_timestamp: snapshot_last_timestamp,
                    })
                } else {
                    None
                },
                wal: if self.persistence_enabled {
                    Some(WalStatus {
                        total_writes: wal_total_writes,
                        total_bytes: wal_total_bytes,
                        lag: wal_lag_stats,
                    })
                } else {
                    None
                },
            },
            shards,
            keyspace: KeyspaceStatus {
                total_keys,
                expired_keys_total,
            },
            commands: CommandsStatus {
                total_processed: commands_total_processed,
                // No accurate instantaneous-rate source on the status path.
                ops_per_sec: None,
            },
        }
    }

    /// Build client status from registry.
    fn build_clients_status(&self) -> ClientsStatus {
        let clients = self.client_registry.list();
        let blocked = clients
            .iter()
            .filter(|c| c.flags.contains(ClientFlags::BLOCKED))
            .count();

        ClientsStatus {
            connected: clients.len(),
            max_clients: self.max_clients.load(Ordering::Relaxed) as usize,
            blocked,
        }
    }

    /// Build memory status from the shared node-state snapshot.
    fn build_memory_status(&self, node_state: &NodeStateSnapshot) -> MemoryStatus {
        let used_bytes: u64 = node_state.used_memory as u64;
        let peak_bytes: u64 = node_state.peak_memory;

        // Try to get RSS from system metrics
        let rss_bytes = get_process_rss();

        // Calculate fragmentation ratio (RSS / used, or 1.0 if RSS not available)
        let fragmentation_ratio = if let Some(rss) = rss_bytes {
            if used_bytes > 0 {
                rss as f64 / used_bytes as f64
            } else {
                1.0
            }
        } else {
            1.0
        };

        MemoryStatus {
            used_bytes,
            peak_bytes,
            limit_bytes: self.maxmemory,
            fragmentation_ratio,
            rss_bytes,
        }
    }

    /// Build health status with issue detection.
    fn build_health_status(
        &self,
        clients: &ClientsStatus,
        memory: &MemoryStatus,
        wal_lag: &Option<WalLagStatus>,
    ) -> HealthStatusInfo {
        let mut issues = Vec::new();

        // Check memory pressure
        if memory.limit_bytes > 0 {
            let usage_percent =
                (memory.used_bytes as f64 / memory.limit_bytes as f64 * 100.0) as u8;
            if usage_percent >= self.config.memory_warning_percent {
                let severity = if usage_percent >= 95 {
                    "critical"
                } else {
                    "warning"
                };
                issues.push(HealthIssue {
                    severity: severity.to_string(),
                    code: "MEMORY_PRESSURE".to_string(),
                    message: format!("Memory usage at {}%", usage_percent),
                });
            }
        }

        // Check connection limit
        if clients.max_clients > 0 {
            let usage_percent =
                (clients.connected as f64 / clients.max_clients as f64 * 100.0) as u8;
            if usage_percent >= self.config.connection_warning_percent {
                issues.push(HealthIssue {
                    severity: "warning".to_string(),
                    code: "NEAR_CONNECTION_LIMIT".to_string(),
                    message: format!(
                        "Client connections at {}% ({}/{})",
                        usage_percent, clients.connected, clients.max_clients
                    ),
                });
            }
        }

        // Check durability lag (persistence health)
        if let Some(lag) = wal_lag {
            if lag.max_durability_lag_ms >= self.config.durability_lag_critical_ms {
                issues.push(HealthIssue {
                    severity: "critical".to_string(),
                    code: "HIGH_DURABILITY_LAG".to_string(),
                    message: format!(
                        "Durability lag at {}ms exceeds critical threshold ({}ms)",
                        lag.max_durability_lag_ms, self.config.durability_lag_critical_ms
                    ),
                });
            } else if lag.max_durability_lag_ms >= self.config.durability_lag_warning_ms {
                issues.push(HealthIssue {
                    severity: "warning".to_string(),
                    code: "ELEVATED_DURABILITY_LAG".to_string(),
                    message: format!(
                        "Durability lag at {}ms exceeds warning threshold ({}ms)",
                        lag.max_durability_lag_ms, self.config.durability_lag_warning_ms
                    ),
                });
            }
        }

        // Check if server is ready
        if !self.health_checker.check_ready().is_ok() {
            issues.push(HealthIssue {
                severity: "critical".to_string(),
                code: "NOT_READY".to_string(),
                message: "Server is not ready".to_string(),
            });
        }

        // Determine overall status
        let status = if issues.iter().any(|i| i.severity == "critical") {
            "unhealthy"
        } else if !issues.is_empty() {
            "degraded"
        } else {
            "healthy"
        };

        HealthStatusInfo {
            status: status.to_string(),
            issues,
        }
    }

    /// Serialize status to pretty-printed JSON.
    pub fn to_json(&self, status: &ServerStatus) -> String {
        serde_json::to_string_pretty(status).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Format a SystemTime as ISO 8601.
fn format_iso8601(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();

    // Calculate components
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate date from days since epoch (1970-01-01)
    let mut year = 1970i32;
    let mut remaining_days = days_since_epoch as i32;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let is_leap = is_leap_year(year);
    let days_in_months: [i32; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &days_in_months {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }
    let day = remaining_days + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Get the process RSS (Resident Set Size) in bytes.
fn get_process_rss() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        // Read from /proc/self/statm
        if let Ok(contents) = std::fs::read_to_string("/proc/self/statm") {
            let fields: Vec<&str> = contents.split_whitespace().collect();
            if fields.len() >= 2
                && let Ok(pages) = fields[1].parse::<u64>()
            {
                // Page size is typically 4096 bytes
                return Some(pages * 4096);
            }
        }
        None
    }

    #[cfg(not(target_os = "linux"))]
    {
        // For non-Linux platforms, RSS is not easily available without libc
        // Return None to indicate it's not available
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_format_iso8601() {
        // 1706270400 = 2024-01-26T12:00:00Z
        let time = UNIX_EPOCH + Duration::from_secs(1706270400);
        let iso = format_iso8601(time);
        assert_eq!(iso, "2024-01-26T12:00:00Z");
    }

    #[test]
    fn test_status_collector_config_default() {
        let config = StatusCollectorConfig::default();
        assert_eq!(config.memory_warning_percent, 90);
        assert_eq!(config.connection_warning_percent, 90);
    }

    #[test]
    fn test_health_issue_serialization() {
        let issue = HealthIssue {
            severity: "warning".to_string(),
            code: "MEMORY_PRESSURE".to_string(),
            message: "Memory usage at 92%".to_string(),
        };
        let json = serde_json::to_string(&issue).unwrap();
        assert!(json.contains("MEMORY_PRESSURE"));
        assert!(json.contains("warning"));
    }

    #[test]
    fn test_frogdb_info_serialization() {
        let info = FrogDbInfo {
            version: "0.1.0".to_string(),
            uptime_secs: 3600,
            process_id: 12345,
            timestamp: 1706284800,
            timestamp_iso: "2024-01-26T12:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("0.1.0"));
        assert!(json.contains("3600"));
    }

    #[test]
    fn test_clients_status_serialization() {
        let status = ClientsStatus {
            connected: 42,
            max_clients: 10000,
            blocked: 3,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("42"));
        assert!(json.contains("10000"));
        assert!(json.contains("3"));
    }

    #[test]
    fn test_memory_status_serialization() {
        let status = MemoryStatus {
            used_bytes: 104857600,
            peak_bytes: 125829120,
            limit_bytes: 1073741824,
            fragmentation_ratio: 1.02,
            rss_bytes: Some(134217728),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("104857600"));
        assert!(json.contains("1.02"));
    }

    #[test]
    fn test_memory_status_without_rss() {
        let status = MemoryStatus {
            used_bytes: 100,
            peak_bytes: 200,
            limit_bytes: 0,
            fragmentation_ratio: 1.0,
            rss_bytes: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        // rss_bytes should be skipped when None
        assert!(!json.contains("rss_bytes"));
    }

    #[test]
    fn test_persistence_status_disabled() {
        let status = PersistenceStatus {
            enabled: false,
            durability_mode: None,
            snapshot: None,
            wal: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("false"));
        // Optional fields should be skipped
        assert!(!json.contains("durability_mode"));
        assert!(!json.contains("snapshot"));
        assert!(!json.contains("wal"));
    }

    #[test]
    fn test_persistence_status_enabled() {
        let status = PersistenceStatus {
            enabled: true,
            durability_mode: Some("periodic".to_string()),
            snapshot: Some(SnapshotStatus {
                in_progress: false,
                last_timestamp: Some(1706280000),
            }),
            wal: Some(WalStatus {
                total_writes: 10000,
                total_bytes: 1048576,
                lag: Some(WalLagStatus {
                    pending_ops_total: 5,
                    pending_bytes_total: 1024,
                    max_durability_lag_ms: 10,
                    avg_durability_lag_ms: 8.0,
                    flush_failures_total: 0,
                    lost_ops_total: 0,
                    last_flush_ok: true,
                    shards: vec![],
                }),
            }),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("periodic"));
        assert!(json.contains("10000"));
        assert!(json.contains("pending_ops_total"));
    }

    #[test]
    fn test_is_leap_year() {
        assert!(is_leap_year(2000)); // Divisible by 400
        assert!(!is_leap_year(1900)); // Divisible by 100 but not 400
        assert!(is_leap_year(2024)); // Divisible by 4 but not 100
        assert!(!is_leap_year(2023)); // Not divisible by 4
    }

    // ------------------------------------------------------------------------
    // Live-source wiring tests: prove each formerly-hardcoded `/status` field
    // now reflects its real source (the shared PrometheusRecorder and the
    // per-shard stats INFO also reads), not a stub.
    // ------------------------------------------------------------------------

    use crate::prometheus_recorder::PrometheusRecorder;
    use frogdb_core::{
        ClientRegistry, Envelope, InfoShardSnapshot, ShardMemoryStats, ShardMessage, ShardSender,
    };

    /// Build a collector over the given shard senders and recorder.
    fn test_collector(
        recorder: Arc<PrometheusRecorder>,
        shard_senders: Vec<ShardSender>,
        persistence_enabled: bool,
    ) -> StatusCollector {
        StatusCollector::new(
            StatusCollectorConfig::default(),
            HealthChecker::new(),
            Arc::new(shard_senders),
            Arc::new(ClientRegistry::new()),
            recorder as Arc<dyn MetricsRecorder>,
            Instant::now(),
            Arc::new(AtomicU64::new(0)),
            0,
            persistence_enabled,
            "async".to_string(),
            LiveMode::new(false, Arc::new(AtomicBool::new(false)), "standalone"),
        )
    }

    /// Spawn a mock shard that answers the single `InfoSnapshot` scatter with
    /// `stats` and drops any other message (its response channel closes,
    /// yielding defaults).
    fn spawn_mock_shard(stats: ShardMemoryStats) -> ShardSender {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Envelope>(8);
        tokio::spawn(async move {
            while let Some(env) = rx.recv().await {
                if let ShardMessage::InfoSnapshot { response_tx } = env.message {
                    let _ = response_tx.send(InfoShardSnapshot {
                        shard_id: stats.shard_id,
                        memory: stats.clone(),
                        ..Default::default()
                    });
                }
            }
        });
        ShardSender::new(tx)
    }

    #[tokio::test]
    async fn test_wal_and_command_counters_sourced_from_recorder() {
        let recorder = Arc::new(PrometheusRecorder::new());
        // Emit through the same typed handles INFO / `/metrics` use.
        WalWrites::inc_by(&*recorder, 7, "0");
        WalWrites::inc_by(&*recorder, 3, "1");
        WalBytes::inc_by(&*recorder, 4096, "0");
        CommandsTotal::inc_by(&*recorder, 5, "GET");
        CommandsTotal::inc_by(&*recorder, 2, "SET");

        let collector = test_collector(recorder.clone(), vec![], true);
        let status = collector.collect().await;

        let wal = status.persistence.wal.expect("wal present");
        assert_eq!(wal.total_writes, 10, "sum across shard labels");
        assert_eq!(wal.total_bytes, 4096);
        assert_eq!(
            status.commands.total_processed, 7,
            "sum across command labels"
        );
        // Must agree with what INFO would read from the same counters.
        assert_eq!(
            wal.total_writes,
            recorder.get_counter_value(WalWrites::NAME).unwrap() as u64
        );
        assert_eq!(
            status.commands.total_processed,
            recorder.get_counter_value(CommandsTotal::NAME).unwrap() as u64
        );
    }

    #[tokio::test]
    async fn test_snapshot_status_sourced_from_recorder() {
        let recorder = Arc::new(PrometheusRecorder::new());
        SnapshotInProgress::set(&*recorder, 1.0);
        SnapshotLastTimestamp::set(&*recorder, 1_706_280_000.0);

        let collector = test_collector(recorder.clone(), vec![], true);
        let snap = collector
            .collect()
            .await
            .persistence
            .snapshot
            .expect("snapshot present");
        assert!(snap.in_progress, "in_progress gauge = 1 -> true");
        assert_eq!(snap.last_timestamp, Some(1_706_280_000));

        // Flip in_progress off and clear timestamp -> false / None.
        SnapshotInProgress::set(&*recorder, 0.0);
        let recorder2 = Arc::new(PrometheusRecorder::new());
        let idle = test_collector(recorder2, vec![], true)
            .collect()
            .await
            .persistence
            .snapshot
            .expect("snapshot present");
        assert!(!idle.in_progress, "no gauge emitted -> false");
        assert_eq!(idle.last_timestamp, None, "never snapshotted -> None");
    }

    #[tokio::test]
    async fn status_shards_and_totals_render_from_node_state_snapshot() {
        // Prove the /status surface renders per-shard rows and node-wide totals
        // from the single NodeStateSnapshot scatter — the same rows and sums INFO
        // and the debug UI read, so the three surfaces cannot disagree.
        let recorder = Arc::new(PrometheusRecorder::new());
        let shard0 = spawn_mock_shard(ShardMemoryStats {
            shard_id: 0,
            keys: 3,
            data_memory: 300,
            peak_memory: 600,
            ..Default::default()
        });
        let shard1 = spawn_mock_shard(ShardMemoryStats {
            shard_id: 1,
            keys: 5,
            data_memory: 500,
            peak_memory: 1000,
            ..Default::default()
        });
        let status = test_collector(recorder, vec![shard0, shard1], false)
            .collect()
            .await;

        // Per-shard rows come straight off the snapshot's per_shard rows.
        assert_eq!(status.shards.len(), 2);
        assert_eq!(status.shards[0].id, 0);
        assert_eq!(status.shards[0].keys, 3);
        assert_eq!(status.shards[0].memory_bytes, 300);
        assert_eq!(status.shards[1].keys, 5);
        assert_eq!(status.shards[1].peak_memory_bytes, 1000);

        // Node-wide aggregates are the sum INFO's memory/keyspace sections report.
        assert_eq!(status.memory.used_bytes, 800);
        assert_eq!(status.memory.peak_bytes, 1600);
        assert_eq!(status.keyspace.total_keys, 8);
    }

    #[tokio::test]
    async fn test_expired_keys_total_sums_shard_stats() {
        // expired_keys_total is sourced from the same per-shard accumulator INFO
        // aggregates (active + lazy expiry), NOT the active-only Prometheus
        // counter — so the two views agree.
        let recorder = Arc::new(PrometheusRecorder::new());
        let shard0 = spawn_mock_shard(ShardMemoryStats {
            shard_id: 0,
            expired_keys: 12,
            keys: 3,
            ..Default::default()
        });
        let shard1 = spawn_mock_shard(ShardMemoryStats {
            shard_id: 1,
            expired_keys: 8,
            keys: 5,
            ..Default::default()
        });
        let collector = test_collector(recorder, vec![shard0, shard1], false);
        let status = collector.collect().await;
        assert_eq!(status.keyspace.expired_keys_total, 20);
        assert_eq!(status.keyspace.total_keys, 8);
    }

    #[tokio::test]
    async fn test_ops_per_sec_omitted_not_faked() {
        // No instantaneous-rate source exists; the field is omitted rather than
        // reported as a misleading zero/average.
        let recorder = Arc::new(PrometheusRecorder::new());
        let collector = test_collector(recorder, vec![], true);
        let status = collector.collect().await;
        assert_eq!(status.commands.ops_per_sec, None);
        let json = collector.to_json(&status);
        assert!(
            !json.contains("ops_per_sec"),
            "ops_per_sec must be absent, got:\n{json}"
        );
    }

    #[tokio::test]
    async fn status_mode_tracks_live_role_flag() {
        // The cluster.mode field must follow the shared role flag at runtime, not
        // freeze at its boot value — after a REPLICAOF demotion `/status` and
        // STATUS JSON must report "replica" (matching INFO's role:slave), and flip
        // back to "primary" on REPLICAOF NO ONE.
        let recorder = Arc::new(PrometheusRecorder::new());
        let flag = Arc::new(AtomicBool::new(false));
        let collector = StatusCollector::new(
            StatusCollectorConfig::default(),
            HealthChecker::new(),
            Arc::new(vec![]),
            Arc::new(ClientRegistry::new()),
            recorder as Arc<dyn MetricsRecorder>,
            Instant::now(),
            Arc::new(AtomicU64::new(0)),
            0,
            false,
            "async".to_string(),
            LiveMode::new(false, flag.clone(), "primary"),
        );

        // Boot role: not a replica -> the non-replica label.
        assert_eq!(collector.collect().await.cluster.mode, "primary");

        // Runtime demotion flips the shared flag -> "replica".
        flag.store(true, Ordering::Release);
        assert_eq!(collector.collect().await.cluster.mode, "replica");

        // Runtime promotion clears it -> back to the non-replica label.
        flag.store(false, Ordering::Release);
        assert_eq!(collector.collect().await.cluster.mode, "primary");
    }

    #[test]
    fn live_mode_cluster_is_static_regardless_of_flag() {
        // Cluster mode is config-only: the role flag never overrides it.
        let flag = Arc::new(AtomicBool::new(true));
        let mode = LiveMode::new(true, flag.clone(), "primary");
        assert_eq!(mode.current(), "cluster");
        flag.store(false, Ordering::Release);
        assert_eq!(mode.current(), "cluster");
    }

    #[test]
    fn test_commands_status_serde_round_trip_omits_ops_per_sec() {
        let status = CommandsStatus {
            total_processed: 42,
            ops_per_sec: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"total_processed\":42"));
        assert!(!json.contains("ops_per_sec"));
    }
}
