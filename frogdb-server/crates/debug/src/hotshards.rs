//! Hot shard detection and reporting.
//!
//! Provides tools to identify shards receiving disproportionate traffic,
//! which is critical for FrogDB's shared-nothing architecture.
//!
//! Usage:
//! - `DEBUG HOTSHARDS [PERIOD <seconds>]` - detailed per-shard traffic analysis
//! - `INFO hotshards` - summary statistics section

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use frogdb_core::{HotShardStatsResponse, ShardMessage, ShardSender};

/// Configuration for hot shard detection.
#[derive(Debug, Clone)]
pub struct HotShardConfig {
    /// Threshold percentage for "HOT" status (default: 20.0).
    pub hot_threshold_percent: f64,
    /// Threshold percentage for "WARM" status (default: 15.0).
    pub warm_threshold_percent: f64,
    /// Default period for stats collection in seconds (default: 10).
    pub default_period_secs: u64,
}

impl Default for HotShardConfig {
    fn default() -> Self {
        Self {
            hot_threshold_percent: 20.0,
            warm_threshold_percent: 15.0,
            default_period_secs: 10,
        }
    }
}

/// Status classification for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    /// Shard is receiving disproportionately high traffic.
    Hot,
    /// Shard is receiving elevated but not critical traffic.
    Warm,
    /// Shard traffic is within normal bounds.
    Ok,
}

impl ShardStatus {
    /// Returns the status string for display.
    pub fn as_str(&self) -> &'static str {
        match self {
            ShardStatus::Hot => "HOT",
            ShardStatus::Warm => "WARM",
            ShardStatus::Ok => "OK",
        }
    }
}

/// Statistics for a single shard.
#[derive(Debug, Clone)]
pub struct ShardStats {
    /// Shard ID.
    pub shard_id: usize,
    /// Total operations per second.
    pub ops_per_sec: f64,
    /// Read operations per second.
    pub reads_per_sec: f64,
    /// Write operations per second.
    pub writes_per_sec: f64,
    /// Percentage of total traffic this shard handles.
    pub percentage: f64,
    /// Current queue depth.
    pub queue_depth: usize,
    /// Status classification.
    pub status: ShardStatus,
}

/// Report from hot shard analysis.
#[derive(Debug, Clone)]
pub struct HotShardReport {
    /// Period in seconds over which stats were collected.
    pub period_secs: u64,
    /// Total ops/sec across all shards.
    pub total_ops_sec: f64,
    /// Per-shard statistics (sorted by ops_per_sec descending).
    pub shards: Vec<ShardStats>,
    /// Imbalance ratio (max_ops_sec / avg_ops_sec).
    pub imbalance_ratio: f64,
    /// Generated recommendations.
    pub recommendations: Vec<String>,
    /// Number of hot shards.
    pub hot_count: usize,
    /// Number of warm shards.
    pub warm_count: usize,
}

/// Collector for hot shard statistics.
pub struct HotShardCollector {
    shard_senders: Arc<Vec<ShardSender>>,
    config: HotShardConfig,
}

impl HotShardCollector {
    /// Create a new hot shard collector.
    pub fn new(
        shard_senders: Arc<Vec<ShardSender>>,
        config: HotShardConfig,
    ) -> Self {
        Self {
            shard_senders,
            config,
        }
    }

    /// Collect hot shard statistics.
    pub async fn collect(&self, period_secs: Option<u64>) -> HotShardReport {
        let period_secs = period_secs.unwrap_or(self.config.default_period_secs);

        // Gather stats from all shards
        let shard_responses = self.gather_shard_stats(period_secs).await;

        // Calculate totals
        let total_ops_sec: f64 = shard_responses.iter().map(|s| s.ops_per_sec).sum();
        let num_shards = shard_responses.len();
        let avg_ops_sec = if num_shards > 0 {
            total_ops_sec / num_shards as f64
        } else {
            0.0
        };

        // Find max ops/sec
        let max_ops_sec = shard_responses
            .iter()
            .map(|s| s.ops_per_sec)
            .fold(0.0f64, f64::max);

        // Calculate imbalance ratio
        let imbalance_ratio = if avg_ops_sec > 0.0 {
            max_ops_sec / avg_ops_sec
        } else {
            1.0
        };

        // Calculate expected percentage per shard
        let expected_pct = if num_shards > 0 {
            100.0 / num_shards as f64
        } else {
            100.0
        };

        // Build shard stats with status classification
        let mut shards: Vec<ShardStats> = shard_responses
            .into_iter()
            .map(|resp| {
                let percentage = if total_ops_sec > 0.0 {
                    (resp.ops_per_sec / total_ops_sec) * 100.0
                } else {
                    0.0
                };

                let status = if percentage >= self.config.hot_threshold_percent {
                    ShardStatus::Hot
                } else if percentage >= self.config.warm_threshold_percent {
                    ShardStatus::Warm
                } else {
                    ShardStatus::Ok
                };

                ShardStats {
                    shard_id: resp.shard_id,
                    ops_per_sec: resp.ops_per_sec,
                    reads_per_sec: resp.reads_per_sec,
                    writes_per_sec: resp.writes_per_sec,
                    percentage,
                    queue_depth: resp.queue_depth,
                    status,
                }
            })
            .collect();

        // Sort by ops_per_sec descending
        shards.sort_by(|a, b| {
            b.ops_per_sec
                .partial_cmp(&a.ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Count hot and warm shards
        let hot_count = shards
            .iter()
            .filter(|s| s.status == ShardStatus::Hot)
            .count();
        let warm_count = shards
            .iter()
            .filter(|s| s.status == ShardStatus::Warm)
            .count();

        // Generate recommendations
        let recommendations = self.generate_recommendations(&shards, imbalance_ratio, expected_pct);

        HotShardReport {
            period_secs,
            total_ops_sec,
            shards,
            imbalance_ratio,
            recommendations,
            hot_count,
            warm_count,
        }
    }

    /// Gather stats from all shards.
    async fn gather_shard_stats(&self, period_secs: u64) -> Vec<HotShardStatsResponse> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::HotShardStats {
                    period_secs,
                    response_tx,
                })
                .await
                .is_ok()
            {
                if let Ok(shard_stats) = response_rx.await {
                    stats.push(shard_stats);
                } else {
                    // Use default stats if shard doesn't respond
                    stats.push(HotShardStatsResponse {
                        shard_id,
                        ops_per_sec: 0.0,
                        reads_per_sec: 0.0,
                        writes_per_sec: 0.0,
                        queue_depth: 0,
                    });
                }
            } else {
                stats.push(HotShardStatsResponse {
                    shard_id,
                    ops_per_sec: 0.0,
                    reads_per_sec: 0.0,
                    writes_per_sec: 0.0,
                    queue_depth: 0,
                });
            }
        }

        stats
    }

    /// Generate recommendations based on the collected stats.
    fn generate_recommendations(
        &self,
        shards: &[ShardStats],
        imbalance_ratio: f64,
        expected_pct: f64,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Check for single shard with high traffic
        for shard in shards {
            if shard.percentage > 25.0 {
                recommendations.push(format!(
                    "Shard {} receives {:.1}% of traffic (expected: {:.1}%)",
                    shard.shard_id, shard.percentage, expected_pct
                ));
            }
        }

        // Check for high imbalance ratio
        if imbalance_ratio > 3.0 {
            recommendations.push(format!(
                "High traffic imbalance detected (ratio: {:.1}x)",
                imbalance_ratio
            ));
        }

        // Check for hot shards
        let hot_shards: Vec<_> = shards
            .iter()
            .filter(|s| s.status == ShardStatus::Hot)
            .collect();

        if !hot_shards.is_empty() {
            recommendations.push("Use DEBUG HASHING <key> to check shard assignment".to_string());
        }

        // Check for multiple hot shards
        if hot_shards.len() > 1 {
            recommendations
                .push("Consider reviewing key naming patterns for better distribution".to_string());
        }

        recommendations
    }
}

/// Format a hot shard report for DEBUG HOTSHARDS output.
pub fn format_hotshards_report(report: &HotShardReport) -> String {
    let mut output = String::new();

    // Header
    output.push_str(&format!(
        "# Hot Shard Report (period: {}s)\n",
        report.period_secs
    ));
    output.push_str(&format!(
        "# Total: {:.0} ops/sec across {} shards\n",
        report.total_ops_sec,
        report.shards.len()
    ));
    output.push_str(&format!(
        "# Imbalance ratio: {:.1}x (max/avg)\n",
        report.imbalance_ratio
    ));
    output.push('\n');

    // Table header
    output.push_str("shard  ops/sec  reads/sec  writes/sec  pct    queue  status\n");
    output.push_str("-----  -------  ---------  ----------  -----  -----  ------\n");

    // Table rows
    for shard in &report.shards {
        output.push_str(&format!(
            "{:<5}  {:>7.0}  {:>9.0}  {:>10.0}  {:>5.1}%  {:>5}  {}\n",
            shard.shard_id,
            shard.ops_per_sec,
            shard.reads_per_sec,
            shard.writes_per_sec,
            shard.percentage,
            shard.queue_depth,
            shard.status.as_str()
        ));
    }

    // Recommendations
    if !report.recommendations.is_empty() {
        output.push('\n');
        output.push_str("# Recommendations:\n");
        for rec in &report.recommendations {
            output.push_str(&format!("# - {}\n", rec));
        }
    }

    output
}

/// Format hot shard info for INFO hotshards output.
pub fn format_hotshards_info(report: &HotShardReport) -> String {
    let hottest_shard = report.shards.first().map(|s| s.shard_id).unwrap_or(0);

    let max_ops_sec = report.shards.first().map(|s| s.ops_per_sec).unwrap_or(0.0);

    let avg_ops_sec = if !report.shards.is_empty() {
        report.total_ops_sec / report.shards.len() as f64
    } else {
        0.0
    };

    format!(
        "# Hotshards\r\n\
         num_shards:{}\r\n\
         hot_shards:{}\r\n\
         warm_shards:{}\r\n\
         total_ops_sec:{:.0}\r\n\
         max_ops_sec:{:.0}\r\n\
         avg_ops_sec:{:.0}\r\n\
         imbalance_ratio:{:.2}\r\n\
         hottest_shard:{}\r\n",
        report.shards.len(),
        report.hot_count,
        report.warm_count,
        report.total_ops_sec,
        max_ops_sec,
        avg_ops_sec,
        report.imbalance_ratio,
        hottest_shard
    )
}

// ============================================================================
// Trait Implementations
// ============================================================================

impl std::fmt::Display for HotShardReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_hotshards_report(self))
    }
}

impl frogdb_core::HotShardReport for HotShardReport {}

impl frogdb_core::HotShardDetector for HotShardCollector {
    fn collect(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Arc<dyn frogdb_core::HotShardReport>> + Send + '_>,
    > {
        Box::pin(async move {
            // Use default period from config
            let report = self.collect(None).await;
            Arc::new(report) as Arc<dyn frogdb_core::HotShardReport>
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_status() {
        assert_eq!(ShardStatus::Hot.as_str(), "HOT");
        assert_eq!(ShardStatus::Warm.as_str(), "WARM");
        assert_eq!(ShardStatus::Ok.as_str(), "OK");
    }

    #[test]
    fn test_default_config() {
        let config = HotShardConfig::default();
        assert_eq!(config.hot_threshold_percent, 20.0);
        assert_eq!(config.warm_threshold_percent, 15.0);
        assert_eq!(config.default_period_secs, 10);
    }

    #[test]
    fn test_format_info() {
        let report = HotShardReport {
            period_secs: 10,
            total_ops_sec: 1000.0,
            shards: vec![
                ShardStats {
                    shard_id: 0,
                    ops_per_sec: 500.0,
                    reads_per_sec: 400.0,
                    writes_per_sec: 100.0,
                    percentage: 50.0,
                    queue_depth: 5,
                    status: ShardStatus::Hot,
                },
                ShardStats {
                    shard_id: 1,
                    ops_per_sec: 500.0,
                    reads_per_sec: 400.0,
                    writes_per_sec: 100.0,
                    percentage: 50.0,
                    queue_depth: 3,
                    status: ShardStatus::Ok,
                },
            ],
            imbalance_ratio: 1.0,
            recommendations: vec![],
            hot_count: 1,
            warm_count: 0,
        };

        let info = format_hotshards_info(&report);
        assert!(info.contains("num_shards:2"));
        assert!(info.contains("hot_shards:1"));
        assert!(info.contains("total_ops_sec:1000"));
    }
}
