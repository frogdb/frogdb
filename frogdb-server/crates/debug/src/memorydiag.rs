//! Memory diagnostics and MEMORY DOCTOR reporting.
//!
//! Provides tools to identify memory issues:
//! - Big key scanning (keys larger than threshold)
//! - Shard memory variance (detecting imbalanced memory distribution)
//! - Estimated memory savings

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use frogdb_core::{BigKeyInfo, BigKeysScanResponse, ShardMemoryStats, ShardMessage};

/// Configuration for memory diagnostics.
#[derive(Debug, Clone)]
pub struct MemoryDiagConfig {
    /// Threshold in bytes for big key detection (default: 1MB).
    pub big_key_threshold_bytes: usize,
    /// Maximum number of big keys to report per shard (default: 100).
    pub max_big_keys_per_shard: usize,
    /// Threshold for shard memory imbalance as coefficient of variation percent (default: 25.0).
    pub imbalance_threshold_percent: f64,
}

impl Default for MemoryDiagConfig {
    fn default() -> Self {
        Self {
            big_key_threshold_bytes: 1_048_576, // 1MB
            max_big_keys_per_shard: 100,
            imbalance_threshold_percent: 25.0,
        }
    }
}

/// Statistics about shard memory variance.
#[derive(Debug, Clone)]
pub struct ShardMemoryVariance {
    /// Average memory per shard in bytes.
    pub average_bytes: f64,
    /// Standard deviation of memory across shards.
    pub std_dev_bytes: f64,
    /// Coefficient of variation (std_dev / average * 100).
    pub coefficient_of_variation: f64,
    /// Shard with maximum memory.
    pub max_shard: usize,
    /// Shard with minimum memory.
    pub min_shard: usize,
    /// Memory range (max - min) in bytes.
    pub range_bytes: usize,
    /// Maximum memory value.
    pub max_bytes: usize,
    /// Minimum memory value.
    pub min_bytes: usize,
}

/// Summary of memory state.
#[derive(Debug, Clone)]
pub struct MemorySummary {
    /// Total memory used by data across all shards.
    pub total_data_memory: usize,
    /// Total number of keys across all shards.
    pub total_keys: usize,
    /// Total overhead estimate across all shards.
    pub total_overhead: usize,
    /// Peak memory (highest across shards).
    pub total_peak: u64,
    /// Total memory limit across all shards (sum of shard limits).
    pub total_limit: u64,
    /// Number of shards.
    pub num_shards: usize,
    /// Number of big keys found.
    pub num_big_keys: usize,
    /// Total bytes used by big keys (potential savings).
    pub total_big_key_bytes: usize,
}

/// Complete memory diagnostics report.
#[derive(Debug, Clone)]
pub struct MemoryDiagReport {
    /// Per-shard memory statistics.
    pub shard_stats: Vec<ShardMemoryStats>,
    /// Big keys found (sorted by size descending).
    pub big_keys: Vec<BigKeyInfo>,
    /// Shard memory variance analysis.
    pub variance: ShardMemoryVariance,
    /// Summary statistics.
    pub summary: MemorySummary,
    /// Whether big key scan was truncated.
    pub big_keys_truncated: bool,
    /// Configuration used for the report.
    pub config: MemoryDiagConfig,
}

/// Collector for memory diagnostics.
pub struct MemoryDiagCollector {
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    config: MemoryDiagConfig,
}

impl MemoryDiagCollector {
    /// Create a new memory diagnostics collector.
    pub fn new(
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        config: MemoryDiagConfig,
    ) -> Self {
        Self {
            shard_senders,
            config,
        }
    }

    /// Collect full memory diagnostics report.
    pub async fn collect(&self) -> MemoryDiagReport {
        // Gather memory stats and big keys in parallel
        let (shard_stats, big_keys_responses) =
            tokio::join!(self.gather_memory_stats(), self.gather_big_keys());

        // Aggregate big keys from all shards
        let mut all_big_keys: Vec<BigKeyInfo> = big_keys_responses
            .iter()
            .flat_map(|r| r.big_keys.clone())
            .collect();

        // Sort by size descending
        all_big_keys.sort_by(|a, b| b.memory_bytes.cmp(&a.memory_bytes));

        // Calculate total big key bytes and check if any scan was truncated
        let total_big_key_bytes: usize = all_big_keys.iter().map(|k| k.memory_bytes).sum();
        let big_keys_truncated = big_keys_responses.iter().any(|r| r.truncated);

        // Limit total big keys reported
        let max_total_big_keys = self.config.max_big_keys_per_shard * self.shard_senders.len();
        if all_big_keys.len() > max_total_big_keys {
            all_big_keys.truncate(max_total_big_keys);
        }

        // Calculate variance
        let variance = calculate_variance(&shard_stats);

        // Calculate summary
        let summary = MemorySummary {
            total_data_memory: shard_stats.iter().map(|s| s.data_memory).sum(),
            total_keys: shard_stats.iter().map(|s| s.keys).sum(),
            total_overhead: shard_stats.iter().map(|s| s.overhead_estimate).sum(),
            total_peak: shard_stats.iter().map(|s| s.peak_memory).max().unwrap_or(0),
            total_limit: shard_stats.iter().map(|s| s.memory_limit).sum(),
            num_shards: shard_stats.len(),
            num_big_keys: all_big_keys.len(),
            total_big_key_bytes,
        };

        MemoryDiagReport {
            shard_stats,
            big_keys: all_big_keys,
            variance,
            summary,
            big_keys_truncated,
            config: self.config.clone(),
        }
    }

    /// Gather memory stats from all shards.
    async fn gather_memory_stats(&self) -> Vec<ShardMemoryStats> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
            {
                if let Ok(shard_stats) = response_rx.await {
                    stats.push(shard_stats);
                } else {
                    stats.push(ShardMemoryStats {
                        shard_id,
                        ..Default::default()
                    });
                }
            } else {
                stats.push(ShardMemoryStats {
                    shard_id,
                    ..Default::default()
                });
            }
        }

        stats
    }

    /// Gather big keys from all shards.
    async fn gather_big_keys(&self) -> Vec<BigKeysScanResponse> {
        let mut responses = Vec::with_capacity(self.shard_senders.len());

        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::ScanBigKeys {
                    threshold_bytes: self.config.big_key_threshold_bytes,
                    max_keys: self.config.max_big_keys_per_shard,
                    response_tx,
                })
                .await
                .is_ok()
            {
                if let Ok(response) = response_rx.await {
                    responses.push(response);
                } else {
                    responses.push(BigKeysScanResponse {
                        shard_id,
                        ..Default::default()
                    });
                }
            } else {
                responses.push(BigKeysScanResponse {
                    shard_id,
                    ..Default::default()
                });
            }
        }

        responses
    }
}

/// Calculate variance statistics for shard memory distribution.
pub fn calculate_variance(shard_stats: &[ShardMemoryStats]) -> ShardMemoryVariance {
    if shard_stats.is_empty() {
        return ShardMemoryVariance {
            average_bytes: 0.0,
            std_dev_bytes: 0.0,
            coefficient_of_variation: 0.0,
            max_shard: 0,
            min_shard: 0,
            range_bytes: 0,
            max_bytes: 0,
            min_bytes: 0,
        };
    }

    let memories: Vec<usize> = shard_stats.iter().map(|s| s.data_memory).collect();
    let n = memories.len() as f64;

    // Calculate average
    let sum: usize = memories.iter().sum();
    let average_bytes = sum as f64 / n;

    // Calculate standard deviation
    let variance: f64 = memories
        .iter()
        .map(|&m| {
            let diff = m as f64 - average_bytes;
            diff * diff
        })
        .sum::<f64>()
        / n;
    let std_dev_bytes = variance.sqrt();

    // Calculate coefficient of variation (as percentage)
    let coefficient_of_variation = if average_bytes > 0.0 {
        (std_dev_bytes / average_bytes) * 100.0
    } else {
        0.0
    };

    // Find max and min shards
    let (max_shard, max_bytes) = shard_stats
        .iter()
        .map(|s| (s.shard_id, s.data_memory))
        .max_by_key(|&(_, mem)| mem)
        .unwrap_or((0, 0));

    let (min_shard, min_bytes) = shard_stats
        .iter()
        .map(|s| (s.shard_id, s.data_memory))
        .min_by_key(|&(_, mem)| mem)
        .unwrap_or((0, 0));

    let range_bytes = max_bytes.saturating_sub(min_bytes);

    ShardMemoryVariance {
        average_bytes,
        std_dev_bytes,
        coefficient_of_variation,
        max_shard,
        min_shard,
        range_bytes,
        max_bytes,
        min_bytes,
    }
}

/// Format a memory diagnostics report for MEMORY DOCTOR output.
pub fn format_report(report: &MemoryDiagReport) -> String {
    let mut output = String::new();

    // Summary section
    output.push_str("=== Summary ===\n");
    output.push_str(&format!("Total keys: {}\n", report.summary.total_keys));
    output.push_str(&format!(
        "Total data memory: {} bytes\n",
        report.summary.total_data_memory
    ));
    output.push_str(&format!(
        "Number of shards: {}\n",
        report.summary.num_shards
    ));
    output.push('\n');

    // Shard memory distribution section (only if multiple shards)
    if report.summary.num_shards > 1 {
        output.push_str("=== Shard Memory Distribution ===\n");
        output.push_str(&format!(
            "Average per shard: {:.0} bytes\n",
            report.variance.average_bytes
        ));
        output.push_str(&format!(
            "Std deviation: {:.0} bytes\n",
            report.variance.std_dev_bytes
        ));
        output.push_str(&format!(
            "Coefficient of variation: {:.1}%\n",
            report.variance.coefficient_of_variation
        ));
        output.push_str(&format!(
            "Range: {} bytes (shard {} to shard {})\n",
            report.variance.range_bytes, report.variance.min_shard, report.variance.max_shard
        ));

        // Show percentage difference for max shard
        if report.variance.average_bytes > 0.0 {
            let max_diff_pct = ((report.variance.max_bytes as f64 - report.variance.average_bytes)
                / report.variance.average_bytes)
                * 100.0;
            if max_diff_pct > 10.0 {
                output.push_str(&format!(
                    "* Shard {} has {:.1}% more memory than average\n",
                    report.variance.max_shard, max_diff_pct
                ));
            }
        }
        output.push('\n');
    }

    // Big keys section
    if !report.big_keys.is_empty() {
        output.push_str(&format!(
            "=== Big Keys (>{} bytes) ===\n",
            report.config.big_key_threshold_bytes
        ));
        for key_info in &report.big_keys {
            let key_display = format_key_display(&key_info.key, 50);
            output.push_str(&format!(
                "  {} ({}) - {} bytes\n",
                key_display, key_info.key_type, key_info.memory_bytes
            ));
        }
        if report.big_keys_truncated {
            output.push_str("  ... (results truncated)\n");
        }
        output.push_str(&format!(
            "Total big key memory: {} bytes\n",
            report.summary.total_big_key_bytes
        ));
        output.push('\n');
    }

    // Issues detected section
    output.push_str("=== Issues Detected ===\n");
    let mut issues_found = false;

    // Check memory usage ratio
    if report.summary.total_limit > 0 {
        let usage_ratio =
            report.summary.total_data_memory as f64 / report.summary.total_limit as f64;
        if usage_ratio > 0.9 {
            output.push_str(&format!(
                "* High memory usage: {:.1}% of {} bytes limit\n",
                usage_ratio * 100.0,
                report.summary.total_limit
            ));
            issues_found = true;
        } else if usage_ratio > 0.75 {
            output.push_str(&format!(
                "* Moderate memory usage: {:.1}% of {} bytes limit\n",
                usage_ratio * 100.0,
                report.summary.total_limit
            ));
            issues_found = true;
        }
    }

    // Check overhead ratio
    if report.summary.total_data_memory > 0 {
        let overhead_ratio =
            report.summary.total_overhead as f64 / report.summary.total_data_memory as f64;
        if overhead_ratio > 0.5 {
            output.push_str(&format!(
                "* High overhead ratio: {:.1}% overhead detected\n",
                overhead_ratio * 100.0
            ));
            issues_found = true;
        }
    }

    // Check peak memory
    if report.summary.total_peak > 0 && report.summary.total_data_memory > 0 {
        let peak_ratio = report.summary.total_peak as f64 / report.summary.total_data_memory as f64;
        if peak_ratio > 1.5 {
            output.push_str(&format!(
                "* Peak memory was {:.1}x higher than current usage\n",
                peak_ratio
            ));
            issues_found = true;
        }
    }

    // Check shard memory imbalance (only if multiple shards)
    if report.summary.num_shards > 1
        && report.variance.coefficient_of_variation > report.config.imbalance_threshold_percent
    {
        output.push_str(&format!(
            "* Shard memory imbalance detected (CV: {:.1}%)\n",
            report.variance.coefficient_of_variation
        ));
        issues_found = true;
    }

    // Report big keys
    if !report.big_keys.is_empty() {
        output.push_str(&format!(
            "* {} big key(s) found using {} bytes\n",
            report.summary.num_big_keys, report.summary.total_big_key_bytes
        ));
        issues_found = true;
    }

    if !issues_found {
        output.push_str("* No memory issues detected.\n");
    }

    output
}

/// Format a key for display, truncating if necessary and using lossy UTF-8.
fn format_key_display(key: &[u8], max_len: usize) -> String {
    let display = String::from_utf8_lossy(key);
    if display.len() > max_len {
        format!("{}...", &display[..max_len])
    } else {
        display.to_string()
    }
}

// ============================================================================
// Trait Implementations
// ============================================================================

impl std::fmt::Display for MemoryDiagReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_report(self))
    }
}

impl frogdb_core::MemoryReport for MemoryDiagReport {}

impl frogdb_core::MemoryDiagnosticsCollector for MemoryDiagCollector {
    fn collect(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Arc<dyn frogdb_core::MemoryReport>> + Send + '_>,
    > {
        Box::pin(async move {
            let report = self.collect().await;
            Arc::new(report) as Arc<dyn frogdb_core::MemoryReport>
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MemoryDiagConfig::default();
        assert_eq!(config.big_key_threshold_bytes, 1_048_576);
        assert_eq!(config.max_big_keys_per_shard, 100);
        assert_eq!(config.imbalance_threshold_percent, 25.0);
    }

    #[test]
    fn test_variance_calculation_empty() {
        let variance = calculate_variance(&[]);
        assert_eq!(variance.average_bytes, 0.0);
        assert_eq!(variance.std_dev_bytes, 0.0);
        assert_eq!(variance.coefficient_of_variation, 0.0);
    }

    #[test]
    fn test_variance_calculation_single_shard() {
        let stats = vec![ShardMemoryStats {
            shard_id: 0,
            data_memory: 1000,
            keys: 10,
            peak_memory: 1000,
            memory_limit: 10000,
            overhead_estimate: 100,
        }];
        let variance = calculate_variance(&stats);
        assert_eq!(variance.average_bytes, 1000.0);
        assert_eq!(variance.std_dev_bytes, 0.0);
        assert_eq!(variance.coefficient_of_variation, 0.0);
        assert_eq!(variance.max_shard, 0);
        assert_eq!(variance.min_shard, 0);
    }

    #[test]
    fn test_variance_calculation_balanced() {
        let stats = vec![
            ShardMemoryStats {
                shard_id: 0,
                data_memory: 1000,
                keys: 10,
                peak_memory: 1000,
                memory_limit: 10000,
                overhead_estimate: 100,
            },
            ShardMemoryStats {
                shard_id: 1,
                data_memory: 1000,
                keys: 10,
                peak_memory: 1000,
                memory_limit: 10000,
                overhead_estimate: 100,
            },
        ];
        let variance = calculate_variance(&stats);
        assert_eq!(variance.average_bytes, 1000.0);
        assert_eq!(variance.std_dev_bytes, 0.0);
        assert_eq!(variance.coefficient_of_variation, 0.0);
        assert_eq!(variance.range_bytes, 0);
    }

    #[test]
    fn test_variance_calculation_imbalanced() {
        let stats = vec![
            ShardMemoryStats {
                shard_id: 0,
                data_memory: 1000,
                keys: 10,
                peak_memory: 1000,
                memory_limit: 10000,
                overhead_estimate: 100,
            },
            ShardMemoryStats {
                shard_id: 1,
                data_memory: 3000,
                keys: 30,
                peak_memory: 3000,
                memory_limit: 10000,
                overhead_estimate: 300,
            },
        ];
        let variance = calculate_variance(&stats);
        assert_eq!(variance.average_bytes, 2000.0);
        assert_eq!(variance.std_dev_bytes, 1000.0);
        assert_eq!(variance.coefficient_of_variation, 50.0);
        assert_eq!(variance.max_shard, 1);
        assert_eq!(variance.min_shard, 0);
        assert_eq!(variance.range_bytes, 2000);
        assert_eq!(variance.max_bytes, 3000);
        assert_eq!(variance.min_bytes, 1000);
    }

    #[test]
    fn test_summary_calculation() {
        let config = MemoryDiagConfig::default();
        let shard_stats = vec![
            ShardMemoryStats {
                shard_id: 0,
                data_memory: 1000,
                keys: 10,
                peak_memory: 1500,
                memory_limit: 10000,
                overhead_estimate: 100,
            },
            ShardMemoryStats {
                shard_id: 1,
                data_memory: 2000,
                keys: 20,
                peak_memory: 2500,
                memory_limit: 10000,
                overhead_estimate: 200,
            },
        ];

        let summary = MemorySummary {
            total_data_memory: shard_stats.iter().map(|s| s.data_memory).sum(),
            total_keys: shard_stats.iter().map(|s| s.keys).sum(),
            total_overhead: shard_stats.iter().map(|s| s.overhead_estimate).sum(),
            total_peak: shard_stats.iter().map(|s| s.peak_memory).max().unwrap_or(0),
            total_limit: shard_stats.iter().map(|s| s.memory_limit).sum(),
            num_shards: shard_stats.len(),
            num_big_keys: 0,
            total_big_key_bytes: 0,
        };

        assert_eq!(summary.total_data_memory, 3000);
        assert_eq!(summary.total_keys, 30);
        assert_eq!(summary.total_overhead, 300);
        assert_eq!(summary.total_peak, 2500);
        assert_eq!(summary.total_limit, 20000);
        assert_eq!(summary.num_shards, 2);

        // Test with a report
        let variance = calculate_variance(&shard_stats);
        let report = MemoryDiagReport {
            shard_stats,
            big_keys: vec![],
            variance,
            summary,
            big_keys_truncated: false,
            config,
        };

        let formatted = format_report(&report);
        assert!(formatted.contains("Total keys: 30"));
        assert!(formatted.contains("Total data memory: 3000 bytes"));
        assert!(formatted.contains("Number of shards: 2"));
    }

    #[test]
    fn test_format_key_display_short() {
        let key = b"short_key";
        let display = format_key_display(key, 50);
        assert_eq!(display, "short_key");
    }

    #[test]
    fn test_format_key_display_truncated() {
        let key = b"this_is_a_very_long_key_that_should_be_truncated_for_display";
        let display = format_key_display(key, 20);
        assert!(display.ends_with("..."));
        assert!(display.len() <= 23); // 20 + "..."
    }

    #[test]
    fn test_format_report_no_issues() {
        let config = MemoryDiagConfig::default();
        let shard_stats = vec![ShardMemoryStats {
            shard_id: 0,
            data_memory: 1000,
            keys: 10,
            peak_memory: 1000,
            memory_limit: 0, // unlimited
            overhead_estimate: 100,
        }];

        let variance = calculate_variance(&shard_stats);
        let summary = MemorySummary {
            total_data_memory: 1000,
            total_keys: 10,
            total_overhead: 100,
            total_peak: 1000,
            total_limit: 0,
            num_shards: 1,
            num_big_keys: 0,
            total_big_key_bytes: 0,
        };

        let report = MemoryDiagReport {
            shard_stats,
            big_keys: vec![],
            variance,
            summary,
            big_keys_truncated: false,
            config,
        };

        let formatted = format_report(&report);
        assert!(formatted.contains("No memory issues detected"));
    }

    #[test]
    fn test_format_report_with_big_keys() {
        let config = MemoryDiagConfig::default();
        let shard_stats = vec![ShardMemoryStats {
            shard_id: 0,
            data_memory: 3_000_000,
            keys: 10,
            peak_memory: 3_000_000,
            memory_limit: 0,
            overhead_estimate: 100,
        }];

        let big_keys = vec![BigKeyInfo {
            key: bytes::Bytes::from("big_key_1"),
            key_type: "string".to_string(),
            memory_bytes: 2_000_000,
        }];

        let variance = calculate_variance(&shard_stats);
        let summary = MemorySummary {
            total_data_memory: 3_000_000,
            total_keys: 10,
            total_overhead: 100,
            total_peak: 3_000_000,
            total_limit: 0,
            num_shards: 1,
            num_big_keys: 1,
            total_big_key_bytes: 2_000_000,
        };

        let report = MemoryDiagReport {
            shard_stats,
            big_keys,
            variance,
            summary,
            big_keys_truncated: false,
            config,
        };

        let formatted = format_report(&report);
        assert!(formatted.contains("=== Big Keys"));
        assert!(formatted.contains("big_key_1"));
        assert!(formatted.contains("string"));
        assert!(formatted.contains("2000000"));
        assert!(formatted.contains("1 big key(s) found"));
    }

    #[test]
    fn test_format_report_high_memory_usage() {
        let config = MemoryDiagConfig::default();
        let shard_stats = vec![ShardMemoryStats {
            shard_id: 0,
            data_memory: 95_000_000,
            keys: 1000,
            peak_memory: 95_000_000,
            memory_limit: 100_000_000,
            overhead_estimate: 1_000_000,
        }];

        let variance = calculate_variance(&shard_stats);
        let summary = MemorySummary {
            total_data_memory: 95_000_000,
            total_keys: 1000,
            total_overhead: 1_000_000,
            total_peak: 95_000_000,
            total_limit: 100_000_000,
            num_shards: 1,
            num_big_keys: 0,
            total_big_key_bytes: 0,
        };

        let report = MemoryDiagReport {
            shard_stats,
            big_keys: vec![],
            variance,
            summary,
            big_keys_truncated: false,
            config,
        };

        let formatted = format_report(&report);
        assert!(formatted.contains("High memory usage"));
        assert!(formatted.contains("95.0%"));
    }
}
