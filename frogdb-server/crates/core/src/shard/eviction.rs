use std::time::Instant;

use crate::error::CommandError;
use crate::eviction::{
    EvictionCandidate, EvictionPolicy, EvictionRanker, LfuRanker, LruRanker, TtlRanker,
};
use crate::keyspace_event::KeyspaceEventFlags;
use crate::store::Store;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Check if we're over the memory limit.
    fn is_over_memory_limit(&self) -> bool {
        if self.eviction.memory_limit == 0 {
            return false;
        }
        self.store.memory_used() as u64 > self.eviction.memory_limit
    }

    /// Check memory and evict if needed before a write operation.
    ///
    /// Returns Ok(()) if memory is available (or was freed via eviction),
    /// Returns Err(CommandError::OutOfMemory) if write should be rejected.
    pub(crate) fn check_memory_for_write(&mut self) -> Result<(), CommandError> {
        // No limit configured
        if self.eviction.memory_limit == 0 {
            return Ok(());
        }

        // Check if we're over limit
        if !self.is_over_memory_limit() {
            return Ok(());
        }

        // Fire USDT probe: memory-pressure
        crate::probes::fire_memory_pressure(
            self.store.memory_used() as u64,
            self.eviction.memory_limit,
            if self.eviction.config.policy == EvictionPolicy::NoEviction {
                "reject"
            } else {
                "evict"
            },
        );

        // Try to evict if policy allows
        if self.eviction.config.policy == EvictionPolicy::NoEviction {
            tracing::warn!(
                shard_id = self.shard_id(),
                memory_used = self.store.memory_used(),
                memory_limit = self.eviction.memory_limit,
                "OOM rejected write"
            );
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_eviction_oom_total",
                1,
                &[("shard", &shard_label)],
            );
            return Err(CommandError::OutOfMemory);
        }

        tracing::debug!(
            shard_id = self.shard_id(),
            memory_used = self.store.memory_used(),
            memory_limit = self.eviction.memory_limit,
            "Eviction triggered"
        );

        // Attempt eviction
        let max_attempts = 10; // Limit attempts to avoid infinite loop
        for _ in 0..max_attempts {
            if !self.is_over_memory_limit() {
                return Ok(());
            }

            if !self.evict_one() {
                // No more keys to evict
                tracing::warn!(
                    shard_id = self.shard_id(),
                    policy = %self.eviction.config.policy,
                    "No volatile keys for eviction"
                );
                tracing::warn!(
                    shard_id = self.shard_id(),
                    memory_used = self.store.memory_used(),
                    memory_limit = self.eviction.memory_limit,
                    "OOM rejected write"
                );
                let shard_label = self.shard_id().to_string();
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_eviction_oom_total",
                    1,
                    &[("shard", &shard_label)],
                );
                return Err(CommandError::OutOfMemory);
            }
        }

        // Still over limit after max attempts
        if self.is_over_memory_limit() {
            tracing::warn!(
                shard_id = self.shard_id(),
                memory_used = self.store.memory_used(),
                memory_limit = self.eviction.memory_limit,
                "OOM rejected write"
            );
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_eviction_oom_total",
                1,
                &[("shard", &shard_label)],
            );
            return Err(CommandError::OutOfMemory);
        }

        Ok(())
    }

    /// Evict one key based on the configured policy.
    ///
    /// Returns true if a key was evicted, false if no suitable key found.
    fn evict_one(&mut self) -> bool {
        match self.eviction.config.policy {
            EvictionPolicy::NoEviction => false,
            EvictionPolicy::AllkeysRandom => self.evict_random(false),
            EvictionPolicy::VolatileRandom => self.evict_random(true),
            EvictionPolicy::AllkeysLru => self.evict_with_ranker(false, &LruRanker),
            EvictionPolicy::VolatileLru => self.evict_with_ranker(true, &LruRanker),
            EvictionPolicy::AllkeysLfu => self.evict_with_ranker(false, &LfuRanker),
            EvictionPolicy::VolatileLfu => self.evict_with_ranker(true, &LfuRanker),
            EvictionPolicy::VolatileTtl => self.evict_with_ranker(true, &TtlRanker),
            EvictionPolicy::TieredLru => self.demote_with_ranker(false, &LruRanker),
            EvictionPolicy::TieredLfu => self.demote_with_ranker(false, &LfuRanker),
        }
    }

    /// Evict a random key.
    fn evict_random(&mut self, volatile_only: bool) -> bool {
        let key = if volatile_only {
            // Sample from keys with TTL
            let keys = self.store.sample_volatile_keys(1);
            keys.into_iter().next()
        } else {
            // Sample from all keys
            self.store.random_key()
        };

        if let Some(key) = key {
            self.delete_for_eviction(&key)
        } else {
            false
        }
    }

    /// Sample keys into the eviction pool using the given [`EvictionRanker`].
    ///
    /// This is the single home of the sample-then-insert loop and the sample
    /// metric shared by every ranking policy. `volatile_only` stays a parameter
    /// (not part of the ranker) because allkeys/volatile variants share a ranker
    /// and differ only in sampling scope.
    fn sample_with_ranker<R: EvictionRanker>(&mut self, volatile_only: bool, ranker: &R) {
        let samples = self.eviction.config.maxmemory_samples;
        let now = Instant::now();

        let keys = if volatile_only {
            self.store.sample_volatile_keys(samples)
        } else {
            self.store.sample_keys(samples)
        };

        let shard_label = self.shard_id().to_string();
        self.observability.metrics_recorder.increment_counter(
            "frogdb_eviction_samples_total",
            keys.len() as u64,
            &[("shard", &shard_label)],
        );

        for key in keys {
            if let Some(metadata) = self.store.get_metadata(&key) {
                let candidate = EvictionCandidate::from_metadata(
                    key,
                    metadata.last_access,
                    metadata.lfu_counter,
                    metadata.expires_at,
                    now,
                );
                self.eviction
                    .pool
                    .maybe_insert_with_ranker(candidate, ranker);
            }
        }
    }

    /// Evict the worst key for the given ranker (sample, then delete the worst).
    fn evict_with_ranker<R: EvictionRanker>(&mut self, volatile_only: bool, ranker: &R) -> bool {
        self.sample_with_ranker(volatile_only, ranker);

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction.pool.pop_worst() {
            self.delete_for_eviction(&candidate.key)
        } else {
            false
        }
    }

    /// Demote the worst key for the given ranker to the warm tier.
    fn demote_with_ranker<R: EvictionRanker>(&mut self, volatile_only: bool, ranker: &R) -> bool {
        self.sample_with_ranker(volatile_only, ranker);

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction.pool.pop_worst() {
            self.demote_for_eviction(&candidate.key)
        } else {
            false
        }
    }

    /// Demote a key to warm tier for eviction (updates metrics and pool).
    fn demote_for_eviction(&mut self, key: &[u8]) -> bool {
        // Remove from eviction pool
        self.eviction.pool.remove(key);

        // Try to demote
        match self.store.demote_key(key) {
            Ok(bytes_freed) => {
                self.increment_version();

                // Emit evicted keyspace notification for demotions too
                self.emit_keyspace_notification(key, "evicted", KeyspaceEventFlags::EVICTED);

                let shard_label = self.shard_id().to_string();
                let policy_label = self.eviction.config.policy.to_string();
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_tiered_demotions_total",
                    1,
                    &[("shard", &shard_label), ("policy", &policy_label)],
                );
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_tiered_bytes_demoted_total",
                    bytes_freed as u64,
                    &[("shard", &shard_label)],
                );

                // Fire USDT probe: key-evicted
                crate::probes::fire_key_evicted(
                    std::str::from_utf8(key).unwrap_or("<binary>"),
                    self.shard_id() as u64,
                    &self.eviction.config.policy.to_string(),
                );

                tracing::debug!(
                    shard_id = self.shard_id(),
                    key = %String::from_utf8_lossy(key),
                    bytes_freed,
                    policy = %self.eviction.config.policy,
                    "Demoted key to warm tier"
                );

                true
            }
            Err(e) => {
                tracing::warn!(
                    shard_id = self.shard_id(),
                    key = %String::from_utf8_lossy(key),
                    error = %e,
                    "Failed to demote key"
                );
                // Fall back to deletion
                self.delete_for_eviction(key)
            }
        }
    }

    /// Delete a key for eviction (updates metrics and pool).
    fn delete_for_eviction(&mut self, key: &[u8]) -> bool {
        // Get memory size before deletion for metrics
        let memory_freed = self
            .store
            .get_metadata(key)
            .map(|m| m.memory_size)
            .unwrap_or(0);

        // Remove from eviction pool
        self.eviction.pool.remove(key);

        // Delete the key
        if self.store.delete(key) {
            self.increment_version();
            self.observability.evicted_keys += 1;

            // Emit evicted keyspace notification
            self.emit_keyspace_notification(key, "evicted", KeyspaceEventFlags::EVICTED);

            // Record eviction metrics
            let shard_label = self.shard_id().to_string();
            let policy_label = self.eviction.config.policy.to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_eviction_keys_total",
                1,
                &[("shard", &shard_label), ("policy", &policy_label)],
            );
            self.observability.metrics_recorder.increment_counter(
                "frogdb_eviction_bytes_total",
                memory_freed as u64,
                &[("shard", &shard_label)],
            );

            tracing::debug!(
                shard_id = self.shard_id(),
                key = %String::from_utf8_lossy(key),
                memory_freed = memory_freed,
                policy = %self.eviction.config.policy,
                "Evicted key"
            );

            true
        } else {
            false
        }
    }
}
