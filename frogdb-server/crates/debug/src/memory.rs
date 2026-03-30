//! Memory diagnostics for MEMORY DOCTOR command.
//!
//! Collects memory stats from all shards, analyzes for common issues,
//! and produces a human-readable diagnostic report.

use std::sync::Arc;

use frogdb_core::{BigKeysScanResponse, ShardMemoryStats, ShardMessage, ShardSender};
use tokio::sync::oneshot;

use crate::config::MemoryDiagConfig;

/// Collected memory diagnostic report.
#[derive(Debug)]
pub struct MemoryDiagReport {
    /// Per-shard memory statistics.
    pub shard_stats: Vec<ShardMemoryStats>,
    /// Big keys found across all shards.
    pub big_keys: Vec<BigKeysScanResponse>,
    /// Diagnostic findings.
    pub findings: Vec<String>,
}

/// Collects memory diagnostics from shards.
pub struct MemoryDiagCollector {
    shard_senders: Arc<Vec<ShardSender>>,
    config: MemoryDiagConfig,
}

impl MemoryDiagCollector {
    /// Create a new memory diagnostics collector.
    pub fn new(
        shard_senders: Arc<Vec<ShardSender>>,
        config: MemoryDiagConfig,
    ) -> Self {
        Self {
            shard_senders,
            config,
        }
    }

    /// Collect memory diagnostics from all shards.
    pub async fn collect(&self) -> MemoryDiagReport {
        let shard_stats = self.gather_memory_stats().await;
        let big_keys = self.scan_big_keys().await;
        let findings = self.analyze(&shard_stats, &big_keys);

        MemoryDiagReport {
            shard_stats,
            big_keys,
            findings,
        }
    }

    /// Gather memory stats from all shards.
    async fn gather_memory_stats(&self) -> Vec<ShardMemoryStats> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
                && let Ok(shard_stats) = response_rx.await
            {
                stats.push(shard_stats);
            }
        }

        stats
    }

    /// Scan all shards for big keys.
    async fn scan_big_keys(&self) -> Vec<BigKeysScanResponse> {
        let mut results = Vec::with_capacity(self.shard_senders.len());

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::ScanBigKeys {
                    threshold_bytes: self.config.big_key_threshold_bytes,
                    max_keys: self.config.max_big_keys_per_shard,
                    response_tx,
                })
                .await
                .is_ok()
                && let Ok(scan_result) = response_rx.await
            {
                results.push(scan_result);
            }
        }

        results
    }

    /// Analyze collected data and produce diagnostic findings.
    fn analyze(
        &self,
        shard_stats: &[ShardMemoryStats],
        big_keys: &[BigKeysScanResponse],
    ) -> Vec<String> {
        let mut findings = Vec::new();

        if shard_stats.is_empty() {
            findings.push("Sam, I have no memory data to analyze.".to_string());
            return findings;
        }

        // Check for memory imbalance between shards
        if shard_stats.len() > 1 {
            let memories: Vec<f64> = shard_stats.iter().map(|s| s.data_memory as f64).collect();
            let mean = memories.iter().sum::<f64>() / memories.len() as f64;

            if mean > 0.0 {
                let variance = memories.iter().map(|m| (m - mean).powi(2)).sum::<f64>()
                    / memories.len() as f64;
                let cv = (variance.sqrt() / mean) * 100.0;

                if cv > self.config.imbalance_threshold_percent {
                    findings.push(format!(
                        "Memory imbalance detected: coefficient of variation is {:.1}% (threshold: {:.1}%). Consider checking key distribution.",
                        cv, self.config.imbalance_threshold_percent
                    ));
                }
            }
        }

        // Check for big keys
        let total_big_keys: usize = big_keys.iter().map(|s| s.big_keys.len()).sum();
        if total_big_keys > 0 {
            findings.push(format!(
                "{} big key(s) found (>{} bytes) across {} shard(s).",
                total_big_keys,
                self.config.big_key_threshold_bytes,
                big_keys.iter().filter(|s| !s.big_keys.is_empty()).count()
            ));
        }

        // Check for high memory usage
        for stats in shard_stats {
            if stats.memory_limit > 0 {
                let usage_pct = (stats.data_memory as f64 / stats.memory_limit as f64) * 100.0;
                if usage_pct > 90.0 {
                    findings.push(format!(
                        "Shard {} is using {:.1}% of its memory limit.",
                        stats.shard_id, usage_pct
                    ));
                }
            }
        }

        if findings.is_empty() {
            findings.push("Sam, I have no memory problems to report.".to_string());
        }

        findings
    }
}

/// Format a memory diagnostic report as a human-readable string.
///
/// Output format with sections:
/// - Summary: overall stats (keys, memory, shards)
/// - Big Keys: listed if any are found
/// - Issues Detected: diagnostic findings
pub fn format_memory_report(report: &MemoryDiagReport) -> String {
    let mut lines = Vec::new();

    // === Summary ===
    let total_keys: usize = report.shard_stats.iter().map(|s| s.keys).sum();
    let total_memory: usize = report.shard_stats.iter().map(|s| s.data_memory).sum();
    let num_shards = report.shard_stats.len();

    lines.push("=== Summary ===".to_string());
    lines.push(format!("Total keys: {}", total_keys));
    lines.push(format!("Total data memory: {} bytes", total_memory));
    lines.push(format!("Number of shards: {}", num_shards));
    lines.push(String::new());

    // === Big Keys === (only if any found)
    let total_big_keys: usize = report.big_keys.iter().map(|s| s.big_keys.len()).sum();
    if total_big_keys > 0 {
        lines.push(format!("=== Big Keys ({} found) ===", total_big_keys));
        for shard_result in &report.big_keys {
            for key_info in &shard_result.big_keys {
                let key_str = String::from_utf8_lossy(&key_info.key);
                lines.push(format!(
                    "  shard:{} key:{} type:{} size:{} bytes",
                    shard_result.shard_id, key_str, key_info.key_type, key_info.memory_bytes
                ));
            }
        }
        lines.push(String::new());
    }

    // === Issues Detected ===
    lines.push("=== Issues Detected ===".to_string());
    if report.findings.is_empty() && total_big_keys == 0 {
        lines.push("Sam, I have no memory problems to report.".to_string());
    } else {
        for finding in &report.findings {
            lines.push(finding.clone());
        }
    }

    lines.join("\n")
}
