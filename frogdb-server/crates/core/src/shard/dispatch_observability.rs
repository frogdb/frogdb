use super::message::ObservabilityMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch observability messages (slowlog, memory, latency, stats, config).
    pub(super) fn dispatch_observability(&mut self, msg: ObservabilityMsg) {
        match msg {
            ObservabilityMsg::SlowlogGet { count, response_tx } => {
                let entries = self.observability.slowlog().get(count);
                let _ = response_tx.send(entries);
            }
            ObservabilityMsg::SlowlogLen { response_tx } => {
                let _ = response_tx.send(self.observability.slowlog().len());
            }
            ObservabilityMsg::SlowlogReset { response_tx } => {
                self.observability.slowlog_mut().reset();
                let _ = response_tx.send(());
            }
            ObservabilityMsg::SlowlogAdd {
                duration_us,
                command,
                client_addr,
                client_name,
                max_len,
            } => {
                self.observability.slowlog_mut().set_max_len(max_len);
                self.observability.slowlog_mut().add_pre_truncated(
                    duration_us,
                    command,
                    client_addr,
                    client_name,
                );
            }
            ObservabilityMsg::MemoryUsage {
                key,
                samples: _,
                response_tx,
            } => {
                let usage = self.calculate_key_memory_usage(&key);
                let _ = response_tx.send(usage);
            }
            ObservabilityMsg::MemoryStats { response_tx } => {
                let stats = self.collect_memory_stats();
                let _ = response_tx.send(stats);
            }
            ObservabilityMsg::InfoSnapshot { response_tx } => {
                let snapshot = self.collect_info_snapshot();
                let _ = response_tx.send(snapshot);
            }
            ObservabilityMsg::ScanBigKeys {
                threshold_bytes,
                max_keys,
                response_tx,
            } => {
                let result = self.scan_big_keys(threshold_bytes, max_keys);
                let _ = response_tx.send(result);
            }
            ObservabilityMsg::LatencyLatest { response_tx } => {
                let latest = self.observability.latency_monitor().latest();
                let _ = response_tx.send(latest);
            }
            ObservabilityMsg::LatencyHistory { event, response_tx } => {
                let history = self.observability.latency_monitor().history(event);
                let _ = response_tx.send(history);
            }
            ObservabilityMsg::LatencyReset {
                events,
                response_tx,
            } => {
                self.observability.latency_monitor_mut().reset(&events);
                let _ = response_tx.send(());
            }
            ObservabilityMsg::ResetStats { response_tx } => {
                self.observability.reset_stats();
                self.store.reset_expired_keys();
                let _ = response_tx.send(());
            }
            ObservabilityMsg::HotShardStats {
                period_secs,
                response_tx,
            } => {
                let stats = self.calculate_hot_shard_stats(period_secs);
                let _ = response_tx.send(stats);
            }
            ObservabilityMsg::UpdateConfig {
                eviction_config,
                response_tx,
            } => {
                if let Some(config) = eviction_config {
                    self.eviction.update_config(config, self.num_shards());
                    tracing::info!(shard_id = self.shard_id(), "Shard config updated");
                }
                let _ = response_tx.send(());
            }
            ObservabilityMsg::SetActiveExpire {
                enabled,
                response_tx,
            } => {
                self.debug_active_expire_disabled = !enabled;
                tracing::debug!(
                    shard_id = self.shard_id(),
                    enabled,
                    "Active expire toggled via DEBUG SET-ACTIVE-EXPIRE"
                );
                let _ = response_tx.send(());
            }
            ObservabilityMsg::SetKeyMemoryHistograms {
                enabled,
                response_tx,
            } => {
                self.store.set_key_memory_enabled(enabled);
                if !enabled {
                    // Clear existing key-memory histogram data when disabling
                    self.store.keysizes_mut().key_memory.clear();
                }
                tracing::debug!(
                    shard_id = self.shard_id(),
                    enabled,
                    "Key-memory histograms toggled"
                );
                let _ = response_tx.send(());
            }
            ObservabilityMsg::KeysizesSnapshot { response_tx } => {
                let snap = Some(self.store.keysizes().clone());
                let _ = response_tx.send(snap);
            }
            ObservabilityMsg::AllocsizeInSlot { slot, response_tx } => {
                let size = self.store.allocsize_in_slot(slot);
                let _ = response_tx.send(size);
            }
        }
    }
}
