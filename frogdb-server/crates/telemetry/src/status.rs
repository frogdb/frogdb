//! Machine-readable status JSON endpoint.
//!
//! Provides a comprehensive JSON snapshot of server health accessible via:
//! - `GET /status/json` HTTP endpoint
//! - `STATUS [JSON]` Redis-protocol command
//!
//! Inspired by FoundationDB's `status json` command.

use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

use frogdb_core::{
    ClientFlags, ClientRegistry, ShardMemoryStats, ShardMessage, ShardSender,
    WalLagStatsResponse,
};

use crate::health::HealthChecker;
use crate::prometheus_recorder::PrometheusRecorder;

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
    /// Maximum sync lag across all shards (milliseconds, if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sync_lag_ms: Option<u64>,
    /// Per-shard lag details.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub shards: Vec<ShardLagStatus>,
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
    /// Sync lag in milliseconds (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_lag_ms: Option<u64>,
}

/// Per-shard statistics.
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
    /// Total commands processed since startup.
    pub total_processed: u64,
    /// Current operations per second.
    pub ops_per_sec: f64,
}

/// Collects status information from various server components.
pub struct StatusCollector {
    config: StatusCollectorConfig,
    health_checker: HealthChecker,
    shard_senders: Arc<Vec<ShardSender>>,
    client_registry: Arc<ClientRegistry>,
    _recorder: Arc<PrometheusRecorder>,
    start_time: Instant,
    // Configuration values needed for status
    max_clients: Arc<AtomicU64>,
    maxmemory: u64,
    persistence_enabled: bool,
    durability_mode: String,
    mode: String,
}

impl StatusCollector {
    /// Create a new status collector.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: StatusCollectorConfig,
        health_checker: HealthChecker,
        shard_senders: Arc<Vec<ShardSender>>,
        client_registry: Arc<ClientRegistry>,
        recorder: Arc<PrometheusRecorder>,
        start_time: Instant,
        max_clients: Arc<AtomicU64>,
        maxmemory: u64,
        persistence_enabled: bool,
        durability_mode: String,
        mode: String,
    ) -> Self {
        Self {
            config,
            health_checker,
            shard_senders,
            client_registry,
            _recorder: recorder,
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
        // Gather shard memory stats
        let shard_stats = self.gather_shard_stats().await;

        // Gather WAL lag stats if persistence is enabled
        let wal_lag_stats = if self.persistence_enabled {
            Some(self.gather_wal_lag_stats().await)
        } else {
            None
        };

        // Build client status
        let clients_status = self.build_clients_status();

        // Build memory status
        let memory_status = self.build_memory_status(&shard_stats);

        // Build health status with issue detection
        let health_status =
            self.build_health_status(&clients_status, &memory_status, &wal_lag_stats);

        // Build shard status
        let shards: Vec<ShardStatus> = shard_stats
            .iter()
            .enumerate()
            .map(|(id, stats)| ShardStatus {
                id,
                keys: stats.keys,
                memory_bytes: stats.data_memory as u64,
                peak_memory_bytes: stats.peak_memory,
            })
            .collect();

        // Calculate totals
        let total_keys: usize = shard_stats.iter().map(|s| s.keys).sum();

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
                mode: self.mode.clone(),
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
                        in_progress: false,   // Would need snapshot coordinator integration
                        last_timestamp: None, // Would need snapshot coordinator integration
                    })
                } else {
                    None
                },
                wal: if self.persistence_enabled {
                    Some(WalStatus {
                        total_writes: 0, // Would need WAL integration
                        total_bytes: 0,  // Would need WAL integration
                        lag: wal_lag_stats,
                    })
                } else {
                    None
                },
            },
            shards,
            keyspace: KeyspaceStatus {
                total_keys,
                expired_keys_total: 0, // Would need metrics integration
            },
            commands: CommandsStatus {
                total_processed: 0, // Would need metrics integration
                ops_per_sec: 0.0,   // Would need metrics integration
            },
        }
    }

    /// Gather memory stats from all shards.
    async fn gather_shard_stats(&self) -> Vec<ShardMemoryStats> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
            {
                if let Ok(shard_stats) = response_rx.await {
                    stats.push(shard_stats);
                } else {
                    // Use default stats if shard doesn't respond
                    stats.push(ShardMemoryStats::default());
                }
            } else {
                stats.push(ShardMemoryStats::default());
            }
        }

        stats
    }

    /// Gather WAL lag stats from all shards.
    async fn gather_wal_lag_stats(&self) -> WalLagStatus {
        let mut responses: Vec<WalLagStatsResponse> = Vec::with_capacity(self.shard_senders.len());

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::WalLagStats { response_tx })
                .await
                .is_ok()
            {
                if let Ok(lag_response) = response_rx.await {
                    responses.push(lag_response);
                } else {
                    responses.push(WalLagStatsResponse::default());
                }
            } else {
                responses.push(WalLagStatsResponse::default());
            }
        }

        // Aggregate stats
        let mut pending_ops_total = 0usize;
        let mut pending_bytes_total = 0usize;
        let mut max_durability_lag_ms = 0u64;
        let mut total_durability_lag_ms = 0u64;
        let mut max_sync_lag_ms: Option<u64> = None;
        let mut enabled_shards = 0usize;
        let mut shards = Vec::new();

        for response in &responses {
            if let Some(ref lag_stats) = response.lag_stats {
                enabled_shards += 1;
                pending_ops_total += lag_stats.pending_ops;
                pending_bytes_total += lag_stats.pending_bytes;
                total_durability_lag_ms += lag_stats.durability_lag_ms;

                if lag_stats.durability_lag_ms > max_durability_lag_ms {
                    max_durability_lag_ms = lag_stats.durability_lag_ms;
                }

                if let Some(sync_lag) = lag_stats.sync_lag_ms {
                    max_sync_lag_ms = Some(max_sync_lag_ms.unwrap_or(0).max(sync_lag));
                }

                shards.push(ShardLagStatus {
                    shard_id: lag_stats.shard_id,
                    pending_ops: lag_stats.pending_ops,
                    pending_bytes: lag_stats.pending_bytes,
                    durability_lag_ms: lag_stats.durability_lag_ms,
                    sync_lag_ms: lag_stats.sync_lag_ms,
                });
            }
        }

        let avg_durability_lag_ms = if enabled_shards > 0 {
            total_durability_lag_ms as f64 / enabled_shards as f64
        } else {
            0.0
        };

        WalLagStatus {
            pending_ops_total,
            pending_bytes_total,
            max_durability_lag_ms,
            avg_durability_lag_ms,
            max_sync_lag_ms,
            shards,
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

    /// Build memory status from shard stats.
    fn build_memory_status(&self, shard_stats: &[ShardMemoryStats]) -> MemoryStatus {
        let used_bytes: u64 = shard_stats.iter().map(|s| s.data_memory as u64).sum();
        let peak_bytes: u64 = shard_stats.iter().map(|s| s.peak_memory).sum();

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
                    max_sync_lag_ms: Some(100),
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
}
