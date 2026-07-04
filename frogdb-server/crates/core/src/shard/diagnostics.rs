use std::sync::atomic::Ordering;

use bytes::Bytes;

use frogdb_types::metrics::definitions::{
    BlockedKeys, KeysTotal, KeysWithExpiry, MemoryPeakBytes, MemoryUsedBytes, PubsubChannels,
    PubsubPatterns, PubsubSubscribers, ShardKeys, ShardMemoryBytes, ShardQueueDepth,
    WalDurabilityLagMs, WalLastFlushOk, WalLastFlushTimestamp, WalPendingBytes, WalPendingOps,
};

use crate::store::Store;

use super::counters::HotShardStatsResponse;
use super::message::ScatterOp;
use super::types::{
    BigKeyInfo, BigKeysScanResponse, InfoShardSnapshot, ShardMemoryStats, TieredCounts,
    VllContinuationLockInfo, VllKeyIntentInfo, VllPendingOpInfo, VllQueueInfo, WalLagStatsResponse,
};
use super::worker::ShardWorker;

impl ShardWorker {
    /// Calculate memory usage for a specific key.
    ///
    /// Returns None if the key doesn't exist.
    pub(crate) fn calculate_key_memory_usage(&self, key: &[u8]) -> Option<usize> {
        let value = self.store.get_hot(key)?;

        // Calculate approximate memory usage:
        // - Key size
        // - Value size (using memory_size method on Value)
        // - Overhead for metadata, expiry tracking, etc.
        let key_size = key.len();
        let value_size = value.memory_size();
        let overhead = std::mem::size_of::<crate::types::KeyMetadata>() + 64; // Rough estimate for hashmap entry overhead

        Some(key_size + value_size + overhead)
    }

    /// Collect memory statistics for this shard.
    pub(crate) fn collect_memory_stats(&self) -> ShardMemoryStats {
        let data_memory = self.store.memory_used();
        let keys = self.store.len();

        // Estimate overhead: hashmap overhead per key + shard-level structures
        let per_key_overhead = 64; // Rough estimate for HashMap entry overhead
        let overhead_estimate = keys * per_key_overhead + 1024; // Plus shard structures

        ShardMemoryStats {
            shard_id: self.shard_id(),
            data_memory,
            keys,
            peak_memory: self.observability.peak_memory,
            memory_limit: self.eviction.memory_limit,
            overhead_estimate,
            evicted_keys: self.observability.evicted_keys,
            expired_keys: self.store.expired_keys(),
            lazyfreed_objects: self.observability.lazyfreed_objects,
        }
    }

    /// Collect WAL lag statistics for this shard.
    pub(crate) fn collect_wal_lag_stats(&self) -> WalLagStatsResponse {
        if let Some(ref wal_writer) = self.persistence.wal_writer {
            let lag_stats = wal_writer.lag_stats();
            WalLagStatsResponse {
                shard_id: self.shard_id(),
                persistence_enabled: true,
                lag_stats: Some(lag_stats),
            }
        } else {
            WalLagStatsResponse {
                shard_id: self.shard_id(),
                persistence_enabled: false,
                lag_stats: None,
            }
        }
    }

    /// Collect the combined INFO snapshot for this shard.
    ///
    /// Bundles the per-shard data INFO needs (memory + eviction counters,
    /// dirty counter, tiered counters, keysize histograms, WAL lag, and replica
    /// identity) so INFO can gather it all in a single fleet scatter.
    pub(crate) fn collect_info_snapshot(&self) -> InfoShardSnapshot {
        InfoShardSnapshot {
            shard_id: self.shard_id(),
            memory: self.collect_memory_stats(),
            dirty: self.store.dirty(),
            tiered: TieredCounts {
                hot_keys: self.store.hot_key_count(),
                warm_keys: self.store.warm_key_count(),
                promotions: self.store.promotion_count(),
                demotions: self.store.demotion_count(),
                expired_on_promote: self.store.expired_on_promote_count(),
            },
            keysizes: self.store.keysizes().clone(),
            wal_lag: self.collect_wal_lag_stats().lag_stats,
            master_host: self.identity.master_host.clone(),
            master_port: self.identity.master_port,
        }
    }

    /// Scan for big keys (keys larger than threshold_bytes).
    pub(crate) fn scan_big_keys(
        &self,
        threshold_bytes: usize,
        max_keys: usize,
    ) -> BigKeysScanResponse {
        let all_keys = self.store.all_keys();
        let keys_scanned = all_keys.len();
        let mut big_keys = Vec::new();

        for key in all_keys {
            if let Some(memory) = self.calculate_key_memory_usage(&key)
                && memory >= threshold_bytes
                && let Some(value) = self.store.get_hot(&key)
            {
                big_keys.push(BigKeyInfo {
                    key: key.clone(),
                    key_type: value.key_type().as_str().to_string(),
                    memory_bytes: memory,
                });
                if big_keys.len() >= max_keys {
                    break;
                }
            }
        }

        // Sort by memory usage descending
        big_keys.sort_by(|a, b| b.memory_bytes.cmp(&a.memory_bytes));

        // Calculate truncated before moving big_keys
        let truncated = big_keys.len() >= max_keys;

        BigKeysScanResponse {
            shard_id: self.shard_id(),
            big_keys,
            keys_scanned,
            truncated,
        }
    }

    /// Calculate hot shard statistics for the given period.
    pub(crate) fn calculate_hot_shard_stats(&mut self, period_secs: u64) -> HotShardStatsResponse {
        let (ops_per_sec, reads_per_sec, writes_per_sec) = self
            .observability
            .operation_counters
            .calculate_ops_per_sec(period_secs);

        HotShardStatsResponse {
            shard_id: self.shard_id(),
            ops_per_sec,
            reads_per_sec,
            writes_per_sec,
            queue_depth: self.observability.queue_depth.load(Ordering::Relaxed),
        }
    }

    /// Collect VLL queue information for debugging.
    pub(crate) fn collect_vll_queue_info(&self) -> VllQueueInfo {
        let mut info = VllQueueInfo {
            shard_id: self.shard_id(),
            queue_depth: self.vll.queue_depth(),
            ..Default::default()
        };

        for snap in self.vll.iter_pending_ops() {
            if snap.state == crate::vll::PendingOpState::Executing {
                info.executing_txid = Some(snap.txid);
            }
            info.pending_ops.push(VllPendingOpInfo {
                txid: snap.txid,
                operation: Self::format_scatter_op(snap.operation),
                key_count: snap.key_count,
                state: format!("{:?}", snap.state),
                age_ms: snap.age_ms,
            });
        }

        if let Some(lock) = self.vll.continuation_lock_snapshot() {
            info.continuation_lock = Some(VllContinuationLockInfo {
                txid: lock.txid,
                conn_id: lock.conn_id,
                age_ms: lock.age_ms,
            });
        }

        for snap in self.vll.intent_snapshots() {
            info.intent_table.push(VllKeyIntentInfo {
                key: Self::format_key_for_display(&snap.key),
                txids: snap.txids,
                lock_state: snap.lock_state,
            });
        }

        info
    }

    /// Format a ScatterOp for display.
    fn format_scatter_op(op: &ScatterOp) -> String {
        match op {
            ScatterOp::MGet => "MGET".to_string(),
            ScatterOp::MSet { .. } => "MSET".to_string(),
            ScatterOp::Del => "DEL".to_string(),
            ScatterOp::Exists => "EXISTS".to_string(),
            ScatterOp::Touch => "TOUCH".to_string(),
            ScatterOp::Unlink => "UNLINK".to_string(),
            ScatterOp::Keys { .. } => "KEYS".to_string(),
            ScatterOp::DbSize => "DBSIZE".to_string(),
            ScatterOp::FlushDb => "FLUSHDB".to_string(),
            ScatterOp::Scan { .. } => "SCAN".to_string(),
            ScatterOp::Copy { .. } => "COPY".to_string(),
            ScatterOp::CopySet { .. } => "COPYSET".to_string(),
            ScatterOp::RandomKey => "RANDOMKEY".to_string(),
            ScatterOp::Dump => "DUMP".to_string(),
            ScatterOp::TsQueryIndex { .. } => "TS.QUERYINDEX".to_string(),
            ScatterOp::TsMget { .. } => "TS.MGET".to_string(),
            ScatterOp::TsMrange { reverse, .. } => if *reverse {
                "TS.MREVRANGE"
            } else {
                "TS.MRANGE"
            }
            .to_string(),
            ScatterOp::FtCreate { .. } => "FT.CREATE".to_string(),
            ScatterOp::FtSearch { .. } => "FT.SEARCH".to_string(),
            ScatterOp::FtDropIndex { .. } => "FT.DROPINDEX".to_string(),
            ScatterOp::FtInfo { .. } => "FT.INFO".to_string(),
            ScatterOp::FtList => "FT._LIST".to_string(),
            ScatterOp::FtAlter { .. } => "FT.ALTER".to_string(),
            ScatterOp::FtSynupdate { .. } => "FT.SYNUPDATE".to_string(),
            ScatterOp::FtSyndump { .. } => "FT.SYNDUMP".to_string(),
            ScatterOp::FtAggregate { .. } => "FT.AGGREGATE".to_string(),
            ScatterOp::FtHybrid { .. } => "FT.HYBRID".to_string(),
            ScatterOp::FtAliasadd { .. } => "FT.ALIASADD".to_string(),
            ScatterOp::FtAliasdel { .. } => "FT.ALIASDEL".to_string(),
            ScatterOp::FtAliasupdate { .. } => "FT.ALIASUPDATE".to_string(),
            ScatterOp::FtTagvals { .. } => "FT.TAGVALS".to_string(),
            ScatterOp::FtDictadd { .. } => "FT.DICTADD".to_string(),
            ScatterOp::FtDictdel { .. } => "FT.DICTDEL".to_string(),
            ScatterOp::FtDictdump { .. } => "FT.DICTDUMP".to_string(),
            ScatterOp::FtConfig { .. } => "FT.CONFIG".to_string(),
            ScatterOp::FtSpellcheck { .. } => "FT.SPELLCHECK".to_string(),
            ScatterOp::FtExplain { .. } => "FT.EXPLAIN".to_string(),
            ScatterOp::EsAll { .. } => "ES.ALL".to_string(),
        }
    }

    /// Collect and emit shard metrics periodically.
    pub(crate) fn collect_shard_metrics(&mut self) {
        let shard_label = self.shard_id().to_string();
        let memory_used = self.store.memory_used() as u64;

        // Update shared memory atomic for SystemMetricsCollector
        if let Some(ref vec) = self.observability.shard_memory_used
            && let Some(slot) = vec.get(self.shard_id())
        {
            slot.store(memory_used, std::sync::atomic::Ordering::Relaxed);
        }

        // Update peak memory if current exceeds it
        if memory_used > self.observability.peak_memory {
            self.observability.peak_memory = memory_used;
        }

        // Memory used by this shard
        ShardMemoryBytes::set(
            &*self.observability.metrics_recorder,
            memory_used as f64,
            &shard_label,
        );

        // Per-shard memory metrics
        MemoryUsedBytes::set(
            &*self.observability.metrics_recorder,
            memory_used as f64,
            &shard_label,
        );

        // Peak memory for this shard
        MemoryPeakBytes::set(
            &*self.observability.metrics_recorder,
            self.observability.peak_memory as f64,
            &shard_label,
        );

        // Keyspace metrics: key count
        let key_count = self.store.len() as f64;

        ShardKeys::set(
            &*self.observability.metrics_recorder,
            key_count,
            &shard_label,
        );

        KeysTotal::set(
            &*self.observability.metrics_recorder,
            key_count,
            &shard_label,
        );

        // Keys with expiry (using cleaner abstraction)
        KeysWithExpiry::set(
            &*self.observability.metrics_recorder,
            self.store.keys_with_expiry_count() as f64,
            &shard_label,
        );

        // Pub/Sub gauges
        PubsubChannels::set(
            &*self.observability.metrics_recorder,
            self.subscriptions.unique_channel_count() as f64,
            &shard_label,
        );
        PubsubPatterns::set(
            &*self.observability.metrics_recorder,
            self.subscriptions.unique_pattern_count() as f64,
            &shard_label,
        );
        PubsubSubscribers::set(
            &*self.observability.metrics_recorder,
            self.subscriptions.total_subscription_count() as f64,
            &shard_label,
        );

        // Blocked keys gauge
        BlockedKeys::set(
            &*self.observability.metrics_recorder,
            self.wait_queue.blocked_keys_count() as f64,
            &shard_label,
        );

        // Shard queue depth
        ShardQueueDepth::set(
            &*self.observability.metrics_recorder,
            self.message_rx.len() as f64,
            &shard_label,
        );

        // WAL lag metrics
        if let Some(ref wal) = self.persistence.wal_writer {
            let stats = wal.lag_stats();
            WalPendingOps::set(
                &*self.observability.metrics_recorder,
                stats.pending_ops as f64,
                &shard_label,
            );
            WalPendingBytes::set(
                &*self.observability.metrics_recorder,
                stats.pending_bytes as f64,
                &shard_label,
            );
            WalLastFlushTimestamp::set(
                &*self.observability.metrics_recorder,
                stats.last_flush_timestamp_ms as f64,
                &shard_label,
            );
            WalDurabilityLagMs::set(
                &*self.observability.metrics_recorder,
                stats.durability_lag_ms as f64,
                &shard_label,
            );
            WalLastFlushOk::set(
                &*self.observability.metrics_recorder,
                if stats.last_flush_ok { 1.0 } else { 0.0 },
                &shard_label,
            );
        }
    }

    /// Format a key for display, truncating if too long.
    fn format_key_for_display(key: &Bytes) -> String {
        const MAX_KEY_DISPLAY_LEN: usize = 64;
        match std::str::from_utf8(key) {
            Ok(s) => {
                if s.len() > MAX_KEY_DISPLAY_LEN {
                    format!("{}...", &s[..MAX_KEY_DISPLAY_LEN])
                } else {
                    s.to_string()
                }
            }
            Err(_) => {
                // Binary key - show hex
                let hex: String = key.iter().take(32).map(|b| format!("{:02x}", b)).collect();
                if key.len() > 32 {
                    format!("0x{}...", hex)
                } else {
                    format!("0x{}", hex)
                }
            }
        }
    }
}
