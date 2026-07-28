//! Hot shard detection and reporting.
//!
//! Identifies shards receiving disproportionate traffic, which matters in
//! FrogDB's shared-nothing, thread-per-core architecture: a single hot shard
//! caps throughput no matter how many cores the node has.
//!
//! The collector is *pull*-based. Each [`crate::HotShardCollector::collect`]
//! scatters [`ObservabilityMsg::HotShardStats`] to every shard; each shard
//! answers from its own 60x1s ring buffer of operation counts, which the shard
//! event loop feeds on every dispatched command (single-shard and scatter
//! paths alike). There is no sampling task to keep alive — the data is already
//! there when the report is requested.
//!
//! Surfaces: `FROGDB.HOTSHARDS [PERIOD <seconds>]`, the `hot_shards` section of
//! the `/status` JSON, and the debug web UI performance panel. All three render
//! the same [`HotShardSnapshot`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::oneshot;

use frogdb_core::{
    BoxFuture, HotShardDetector, HotShardSnapshot, HotShardStatsResponse, ObservabilityMsg,
    OperationCounters, ShardLoad, ShardLoadClass, ShardSender,
};

pub use crate::config::HotShardConfig;

/// Live-tunable hot-shard thresholds.
///
/// The collector reads every threshold through this on each `collect`, so
/// `CONFIG SET hotshards-hot-threshold-percent` (and its siblings) retunes a
/// running collector without rebuilding it. `f64` fields are stored
/// as their IEEE-754 bit patterns in an [`AtomicU64`]; all accesses are
/// `Relaxed` because the values are independent tuning knobs, not a
/// consistency-coupled group.
#[derive(Debug)]
pub struct SharedHotShardConfig {
    hot_threshold_percent: AtomicU64,
    warm_threshold_percent: AtomicU64,
    default_period_secs: AtomicU64,
    /// The feature's kill switch. Held as an `Arc<AtomicBool>` rather than a
    /// plain atomic because the shard workers need the *same* cell: each one
    /// reads it per dispatched command to decide whether to touch its op-rate
    /// ring at all, so `CONFIG SET hotshards-enabled no` stops the accounting
    /// itself, not merely the reporting.
    enabled: Arc<AtomicBool>,
}

impl SharedHotShardConfig {
    /// Seed the shared state from a static config.
    pub fn new(config: &HotShardConfig) -> Self {
        Self {
            hot_threshold_percent: AtomicU64::new(config.hot_threshold_percent.to_bits()),
            warm_threshold_percent: AtomicU64::new(config.warm_threshold_percent.to_bits()),
            default_period_secs: AtomicU64::new(config.default_period_secs),
            enabled: Arc::new(AtomicBool::new(config.enabled)),
        }
    }

    /// Current HOT classification threshold, as a traffic-share percentage.
    pub fn hot_threshold_percent(&self) -> f64 {
        f64::from_bits(self.hot_threshold_percent.load(Ordering::Relaxed))
    }

    /// Retune the HOT classification threshold.
    pub fn set_hot_threshold_percent(&self, percent: f64) {
        self.hot_threshold_percent
            .store(percent.to_bits(), Ordering::Relaxed);
    }

    /// Current WARM classification threshold, as a traffic-share percentage.
    pub fn warm_threshold_percent(&self) -> f64 {
        f64::from_bits(self.warm_threshold_percent.load(Ordering::Relaxed))
    }

    /// Retune the WARM classification threshold.
    pub fn set_warm_threshold_percent(&self, percent: f64) {
        self.warm_threshold_percent
            .store(percent.to_bits(), Ordering::Relaxed);
    }

    /// Current default sampling window, in seconds.
    pub fn default_period_secs(&self) -> u64 {
        self.default_period_secs.load(Ordering::Relaxed)
    }

    /// Retune the default sampling window.
    pub fn set_default_period_secs(&self, secs: u64) {
        self.default_period_secs.store(secs, Ordering::Relaxed);
    }

    /// Whether hot-shard accounting is switched on.
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Turn hot-shard accounting on or off. Reachable from `ConfigManager` for
    /// `CONFIG SET hotshards-enabled`; shard workers see it on their next
    /// dispatched command.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// The kill-switch cell itself, for the shard workers' per-command check.
    pub fn enabled_flag(&self) -> Arc<AtomicBool> {
        self.enabled.clone()
    }

    /// Read every knob back out as a plain config value.
    pub fn snapshot(&self) -> HotShardConfig {
        HotShardConfig {
            hot_threshold_percent: self.hot_threshold_percent(),
            warm_threshold_percent: self.warm_threshold_percent(),
            default_period_secs: self.default_period_secs(),
            enabled: self.enabled(),
        }
    }
}

impl Default for SharedHotShardConfig {
    fn default() -> Self {
        Self::new(&HotShardConfig::default())
    }
}

/// Collector for hot shard statistics.
pub struct HotShardCollector {
    shard_senders: Arc<Vec<ShardSender>>,
    config: Arc<SharedHotShardConfig>,
}

impl HotShardCollector {
    /// Create a new hot shard collector over the node's shard mailboxes.
    pub fn new(shard_senders: Arc<Vec<ShardSender>>, config: &HotShardConfig) -> Self {
        Self {
            shard_senders,
            config: Arc::new(SharedHotShardConfig::new(config)),
        }
    }

    /// Create a collector that *adopts* an existing shared threshold handle
    /// rather than minting its own.
    ///
    /// Used at startup so the collector and `ConfigManager` (which owns the
    /// handle from construction, before the collector exists) share one cell:
    /// `CONFIG SET hotshards-*` then retunes the live collector, and CONFIG
    /// GET/REWRITE read back the same values the collector classifies with.
    pub fn with_shared_config(
        shard_senders: Arc<Vec<ShardSender>>,
        config: Arc<SharedHotShardConfig>,
    ) -> Self {
        Self {
            shard_senders,
            config,
        }
    }

    /// The live-tunable threshold handle, so the runtime config layer can
    /// retune this collector in place.
    pub fn shared_config(&self) -> &Arc<SharedHotShardConfig> {
        &self.config
    }

    /// Collect per-shard load over `period_secs` (the configured default when
    /// `None`).
    pub async fn collect(&self, period_secs: Option<u64>) -> HotShardSnapshot {
        // Read the tuning knobs once per report so a concurrent retune cannot
        // classify different shards of the same report against different
        // thresholds.
        let hot_threshold = self.config.hot_threshold_percent();
        let warm_threshold = self.config.warm_threshold_percent();
        // The window a shard can actually answer for is bounded by its ring, so
        // report the *clamped* value rather than echoing the request back: a
        // reply that says `period_secs 3600` over 60 seconds of data is a lie
        // about what was measured.
        let period_secs = period_secs
            .unwrap_or_else(|| self.config.default_period_secs())
            .min(OperationCounters::MAX_WINDOW_SECS);

        // Kill switch: no shard is recording, so report *no shards* rather than
        // a fleet of fabricated zeros (which would read as a healthy, idle
        // node), and say why.
        if !self.config.enabled() {
            return HotShardSnapshot {
                period_secs,
                total_ops_per_sec: 0.0,
                imbalance_ratio: 1.0,
                hot_count: 0,
                warm_count: 0,
                shards: Vec::new(),
                recommendations: vec![
                    "Hot-shard accounting is disabled (hotshards-enabled no); no per-shard \
                     rates are being recorded"
                        .to_string(),
                ],
            };
        }

        let shard_responses = self.gather_shard_stats(period_secs).await;

        let total_ops_per_sec: f64 = shard_responses.iter().map(|s| s.ops_per_sec).sum();
        let num_shards = shard_responses.len();
        let avg_ops_per_sec = if num_shards > 0 {
            total_ops_per_sec / num_shards as f64
        } else {
            0.0
        };
        let max_ops_per_sec = shard_responses
            .iter()
            .map(|s| s.ops_per_sec)
            .fold(0.0f64, f64::max);

        let imbalance_ratio = if avg_ops_per_sec > 0.0 {
            max_ops_per_sec / avg_ops_per_sec
        } else {
            1.0
        };

        // The share each shard would carry under a perfectly uniform hash.
        let expected_pct = if num_shards > 0 {
            100.0 / num_shards as f64
        } else {
            100.0
        };

        let mut shards: Vec<ShardLoad> = shard_responses
            .into_iter()
            .map(|resp| {
                let percentage = if total_ops_per_sec > 0.0 {
                    (resp.ops_per_sec / total_ops_per_sec) * 100.0
                } else {
                    0.0
                };

                // An idle fleet has every shard at 0%, which must never read as
                // HOT even if a threshold is configured at 0.
                let class = if percentage <= 0.0 {
                    ShardLoadClass::Ok
                } else if percentage >= hot_threshold {
                    ShardLoadClass::Hot
                } else if percentage >= warm_threshold {
                    ShardLoadClass::Warm
                } else {
                    ShardLoadClass::Ok
                };

                ShardLoad {
                    shard_id: resp.shard_id,
                    ops_per_sec: resp.ops_per_sec,
                    reads_per_sec: resp.reads_per_sec,
                    writes_per_sec: resp.writes_per_sec,
                    percentage,
                    queue_depth: resp.queue_depth,
                    class,
                }
            })
            .collect();

        shards.sort_by(|a, b| {
            b.ops_per_sec
                .partial_cmp(&a.ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.shard_id.cmp(&b.shard_id))
        });

        let hot_count = shards
            .iter()
            .filter(|s| s.class == ShardLoadClass::Hot)
            .count();
        let warm_count = shards
            .iter()
            .filter(|s| s.class == ShardLoadClass::Warm)
            .count();

        let recommendations = generate_recommendations(&shards, imbalance_ratio, expected_pct);

        HotShardSnapshot {
            period_secs,
            total_ops_per_sec,
            imbalance_ratio,
            hot_count,
            warm_count,
            shards,
            recommendations,
        }
    }

    /// Ask every shard for its windowed op-rates. A shard that cannot be
    /// reached (mailbox closed, worker gone) is reported at zero rather than
    /// dropped, so the shard list always covers the whole node and percentages
    /// stay comparable across reports.
    async fn gather_shard_stats(&self, period_secs: u64) -> Vec<HotShardStatsResponse> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let delivered = sender
                .send(ObservabilityMsg::HotShardStats {
                    period_secs,
                    response_tx,
                })
                .await
                .is_ok();

            match if delivered {
                response_rx.await.ok()
            } else {
                None
            } {
                Some(shard_stats) => stats.push(shard_stats),
                None => stats.push(HotShardStatsResponse {
                    shard_id,
                    ops_per_sec: 0.0,
                    reads_per_sec: 0.0,
                    writes_per_sec: 0.0,
                    queue_depth: 0,
                }),
            }
        }

        stats
    }
}

impl HotShardDetector for HotShardCollector {
    fn collect_snapshot(&self, period_secs: Option<u64>) -> BoxFuture<'_, HotShardSnapshot> {
        Box::pin(async move { HotShardCollector::collect(self, period_secs).await })
    }
}

/// Derive operator hints from a classified shard list.
fn generate_recommendations(
    shards: &[ShardLoad],
    imbalance_ratio: f64,
    expected_pct: f64,
) -> Vec<String> {
    let mut recommendations = Vec::new();

    for shard in shards {
        if shard.percentage > 25.0 {
            recommendations.push(format!(
                "Shard {} receives {:.1}% of traffic (expected: {:.1}%)",
                shard.shard_id, shard.percentage, expected_pct
            ));
        }
    }

    if imbalance_ratio > 3.0 {
        recommendations.push(format!(
            "High traffic imbalance detected (ratio: {:.1}x)",
            imbalance_ratio
        ));
    }

    let hot_shards = shards
        .iter()
        .filter(|s| s.class == ShardLoadClass::Hot)
        .count();

    if hot_shards > 0 {
        recommendations.push("Use DEBUG HASHING <key> to check shard assignment".to_string());
    }

    if hot_shards > 1 {
        recommendations
            .push("Consider reviewing key naming patterns for better distribution".to_string());
    }

    recommendations
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(shard_id: usize, ops: f64, pct: f64, class: ShardLoadClass) -> ShardLoad {
        ShardLoad {
            shard_id,
            ops_per_sec: ops,
            reads_per_sec: ops * 0.8,
            writes_per_sec: ops * 0.2,
            percentage: pct,
            queue_depth: 0,
            class,
        }
    }

    #[test]
    fn shard_load_class_labels() {
        assert_eq!(ShardLoadClass::Hot.as_str(), "HOT");
        assert_eq!(ShardLoadClass::Warm.as_str(), "WARM");
        assert_eq!(ShardLoadClass::Ok.as_str(), "OK");
    }

    #[test]
    fn default_config_matches_documented_defaults() {
        let config = HotShardConfig::default();
        assert_eq!(config.hot_threshold_percent, 20.0);
        assert_eq!(config.warm_threshold_percent, 15.0);
        assert_eq!(config.default_period_secs, 10);
        assert!(config.enabled);
    }

    #[test]
    fn shared_config_round_trips_and_retunes() {
        let shared = SharedHotShardConfig::new(&HotShardConfig::default());
        assert_eq!(shared.hot_threshold_percent(), 20.0);
        assert_eq!(shared.warm_threshold_percent(), 15.0);
        assert_eq!(shared.default_period_secs(), 10);

        shared.set_hot_threshold_percent(42.5);
        shared.set_warm_threshold_percent(11.25);
        shared.set_default_period_secs(30);
        shared.set_enabled(false);

        let snapshot = shared.snapshot();
        assert_eq!(snapshot.hot_threshold_percent, 42.5);
        assert_eq!(snapshot.warm_threshold_percent, 11.25);
        assert_eq!(snapshot.default_period_secs, 30);
        assert!(!snapshot.enabled);
    }

    /// The kill switch is one cell shared with the shard workers: the flag the
    /// workers hold must see a `CONFIG SET hotshards-enabled` through the
    /// collector's handle, or the two halves of the feature could disagree.
    #[test]
    fn enabled_flag_is_shared_with_shard_workers() {
        let shared = SharedHotShardConfig::new(&HotShardConfig::default());
        let worker_flag = shared.enabled_flag();
        assert!(worker_flag.load(Ordering::Relaxed));
        shared.set_enabled(false);
        assert!(!worker_flag.load(Ordering::Relaxed));
        assert!(!shared.enabled());
    }

    #[tokio::test]
    async fn disabled_collector_reports_no_shards_and_says_why() {
        let collector = HotShardCollector::new(Arc::new(Vec::new()), &HotShardConfig::default());
        collector.shared_config().set_enabled(false);
        let report = collector.collect(None).await;
        assert!(report.shards.is_empty());
        assert_eq!(report.total_ops_per_sec, 0.0);
        assert!(
            report
                .recommendations
                .iter()
                .any(|r| r.contains("hotshards-enabled no")),
            "a disabled report must name the switch that silenced it: {:?}",
            report.recommendations
        );
    }

    /// The reported window is the one the shards can answer for: a request
    /// beyond the ring is clamped, and the clamped value is what every surface
    /// renders.
    #[tokio::test]
    async fn reported_period_is_the_clamped_window() {
        let collector = HotShardCollector::new(Arc::new(Vec::new()), &HotShardConfig::default());
        assert_eq!(
            collector.collect(Some(86_400)).await.period_secs,
            OperationCounters::MAX_WINDOW_SECS
        );
        // A configured default beyond the ring is clamped the same way.
        collector.shared_config().set_default_period_secs(3_600);
        assert_eq!(
            collector.collect(None).await.period_secs,
            OperationCounters::MAX_WINDOW_SECS
        );
    }

    #[tokio::test]
    async fn collect_with_no_shards_is_empty_and_balanced() {
        let collector = HotShardCollector::new(Arc::new(Vec::new()), &HotShardConfig::default());
        let report = collector.collect(None).await;
        assert_eq!(report.period_secs, 10);
        assert_eq!(report.total_ops_per_sec, 0.0);
        assert_eq!(report.imbalance_ratio, 1.0);
        assert_eq!(report.hot_count, 0);
        assert!(report.shards.is_empty());
    }

    #[tokio::test]
    async fn collect_honours_retuned_default_period() {
        let collector = HotShardCollector::new(Arc::new(Vec::new()), &HotShardConfig::default());
        collector.shared_config().set_default_period_secs(45);
        assert_eq!(collector.collect(None).await.period_secs, 45);
        // An explicit period still wins over the configured default.
        assert_eq!(collector.collect(Some(5)).await.period_secs, 5);
    }

    #[test]
    fn recommendations_flag_skew_and_imbalance() {
        let shards = vec![
            load(0, 900.0, 90.0, ShardLoadClass::Hot),
            load(1, 100.0, 10.0, ShardLoadClass::Ok),
        ];
        let recs = generate_recommendations(&shards, 1.8, 50.0);
        assert!(recs.iter().any(|r| r.contains("Shard 0 receives 90.0%")));
        assert!(recs.iter().any(|r| r.contains("DEBUG HASHING")));
        // Imbalance ratio below the 3x bar must not raise the imbalance hint.
        assert!(!recs.iter().any(|r| r.contains("High traffic imbalance")));
    }

    #[test]
    fn idle_fleet_is_never_hot() {
        let shards = vec![load(0, 0.0, 0.0, ShardLoadClass::Ok)];
        assert!(generate_recommendations(&shards, 1.0, 100.0).is_empty());
    }
}
