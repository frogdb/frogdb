use std::time::Instant;

use frogdb_types::metrics::definitions::{
    EvictionBytesTotal, EvictionKeysTotal, EvictionOomTotal, EvictionSamplesTotal,
    TieredBytesSpilled, TieredSpills,
};

use bytes::Bytes;

use crate::error::CommandError;
use crate::eviction::{
    EvictionCandidate, EvictionPolicy, EvictionRanker, LfuRanker, LruRanker, TtlRanker,
};
use crate::store::Store;

use super::post_execution::{ENGINE_INTERNAL_CONN_ID, RemovalPropagation, RemovalReason};
use super::worker::ShardWorker;

impl ShardWorker {
    /// Check if we're over the memory limit.
    fn is_over_memory_limit(&self) -> bool {
        if self.eviction.memory_limit() == 0 {
            return false;
        }
        self.store.memory_used() as u64 > self.eviction.memory_limit()
    }

    /// Check memory and evict if needed before a write operation.
    ///
    /// Returns Ok(()) if memory is available (or was freed via eviction),
    /// Returns Err(CommandError::OutOfMemory) if write should be rejected.
    pub(crate) async fn check_memory_for_write(&mut self) -> Result<(), CommandError> {
        // No limit configured
        if self.eviction.memory_limit() == 0 {
            return Ok(());
        }

        // Check if we're over limit
        if !self.is_over_memory_limit() {
            return Ok(());
        }

        // Fire USDT probe: memory-pressure
        crate::probes::fire_memory_pressure(
            self.store.memory_used() as u64,
            self.eviction.memory_limit(),
            if self.eviction.is_no_eviction() {
                "reject"
            } else {
                "evict"
            },
        );

        // Try to evict if policy allows
        if self.eviction.is_no_eviction() {
            tracing::warn!(
                shard_id = self.shard_id(),
                memory_used = self.store.memory_used(),
                memory_limit = self.eviction.memory_limit(),
                "OOM rejected write"
            );
            let shard_label = self.shard_id().to_string();
            EvictionOomTotal::inc(self.observability.metrics(), &shard_label);
            return Err(CommandError::OutOfMemory);
        }

        tracing::debug!(
            shard_id = self.shard_id(),
            memory_used = self.store.memory_used(),
            memory_limit = self.eviction.memory_limit(),
            "Eviction triggered"
        );

        // Attempt eviction
        let max_attempts = 10; // Limit attempts to avoid infinite loop
        for _ in 0..max_attempts {
            if !self.is_over_memory_limit() {
                return Ok(());
            }

            if !self.evict_one().await {
                // No more keys to evict
                tracing::warn!(
                    shard_id = self.shard_id(),
                    policy = %self.eviction.policy(),
                    "No volatile keys for eviction"
                );
                tracing::warn!(
                    shard_id = self.shard_id(),
                    memory_used = self.store.memory_used(),
                    memory_limit = self.eviction.memory_limit(),
                    "OOM rejected write"
                );
                let shard_label = self.shard_id().to_string();
                EvictionOomTotal::inc(self.observability.metrics(), &shard_label);
                return Err(CommandError::OutOfMemory);
            }
        }

        // Still over limit after max attempts
        if self.is_over_memory_limit() {
            tracing::warn!(
                shard_id = self.shard_id(),
                memory_used = self.store.memory_used(),
                memory_limit = self.eviction.memory_limit(),
                "OOM rejected write"
            );
            let shard_label = self.shard_id().to_string();
            EvictionOomTotal::inc(self.observability.metrics(), &shard_label);
            return Err(CommandError::OutOfMemory);
        }

        Ok(())
    }

    /// Evict one key based on the configured policy.
    ///
    /// Returns true if a key was evicted, false if no suitable key found.
    async fn evict_one(&mut self) -> bool {
        match self.eviction.policy() {
            EvictionPolicy::NoEviction => false,
            EvictionPolicy::AllkeysRandom => self.evict_random(false).await,
            EvictionPolicy::VolatileRandom => self.evict_random(true).await,
            EvictionPolicy::AllkeysLru => self.evict_with_ranker(false, &LruRanker).await,
            EvictionPolicy::VolatileLru => self.evict_with_ranker(true, &LruRanker).await,
            EvictionPolicy::AllkeysLfu => self.evict_with_ranker(false, &LfuRanker).await,
            EvictionPolicy::VolatileLfu => self.evict_with_ranker(true, &LfuRanker).await,
            EvictionPolicy::VolatileTtl => self.evict_with_ranker(true, &TtlRanker).await,
            EvictionPolicy::TieredLru => self.spill_with_ranker(false, &LruRanker).await,
            EvictionPolicy::TieredLfu => self.spill_with_ranker(false, &LfuRanker).await,
        }
    }

    /// Evict a random key.
    async fn evict_random(&mut self, volatile_only: bool) -> bool {
        let key = if volatile_only {
            // Sample from keys with TTL
            let keys = self.store.sample_volatile_keys(1);
            keys.into_iter().next()
        } else {
            // Sample from all keys
            self.store.random_key()
        };

        if let Some(key) = key {
            self.delete_for_eviction(&key).await
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
        let samples = self.eviction.maxmemory_samples();
        let now = Instant::now();

        let keys = if volatile_only {
            self.store.sample_volatile_keys(samples)
        } else {
            self.store.sample_keys(samples)
        };

        let shard_label = self.shard_id().to_string();
        let policy_label = self.eviction.policy_label();
        EvictionSamplesTotal::inc_by(
            self.observability.metrics(),
            keys.len() as u64,
            &shard_label,
            &policy_label,
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
                self.eviction.consider_candidate(candidate, ranker);
            }
        }
    }

    /// Evict the worst key for the given ranker (sample, then delete the worst).
    async fn evict_with_ranker<R: EvictionRanker>(
        &mut self,
        volatile_only: bool,
        ranker: &R,
    ) -> bool {
        self.sample_with_ranker(volatile_only, ranker);

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction.take_worst_candidate() {
            self.delete_for_eviction(&candidate.key).await
        } else {
            false
        }
    }

    /// Spill the worst key for the given ranker to the warm tier.
    async fn spill_with_ranker<R: EvictionRanker>(
        &mut self,
        volatile_only: bool,
        ranker: &R,
    ) -> bool {
        self.sample_with_ranker(volatile_only, ranker);

        // Get worst candidate from pool
        if let Some(candidate) = self.eviction.take_worst_candidate() {
            self.spill_for_eviction(&candidate.key).await
        } else {
            false
        }
    }

    /// Spill a key to warm tier for eviction (updates metrics and pool).
    ///
    /// A spill is TIERING, **not** a removal — the value moves hot→warm and
    /// stays logically present (it unspills on next access), so it must be
    /// invisible to clients and replicas. It deliberately does **not** route
    /// through [`ShardWorker::run_internal_removal_effects`], and — unlike a
    /// real eviction — it must NOT bump the key's WATCH version (a WATCH on an
    /// unchanged, still-readable value must survive EXEC) nor emit an `evicted`
    /// keyspace notification (`evicted` means the key is gone; here it is not).
    /// Observability comes from the `TieredSpills` / `TieredBytesSpilled`
    /// metrics below, matching how Redis-on-flash and Dragonfly SSD tiering
    /// surface spills — there is no standard spill keyspace event. Only its
    /// fallback-to-delete path is a real removal, and that awaits
    /// `delete_for_eviction`.
    async fn spill_for_eviction(&mut self, key: &[u8]) -> bool {
        // Remove from eviction pool
        self.eviction.forget_key(key);

        // Try to spill
        match self.store.spill_key(key) {
            Ok(bytes_freed) => {
                let shard_label = self.shard_id().to_string();
                let policy_label = self.eviction.policy_label();
                TieredSpills::inc(self.observability.metrics(), &shard_label, &policy_label);
                TieredBytesSpilled::inc_by(
                    self.observability.metrics(),
                    bytes_freed as u64,
                    &shard_label,
                );

                // NB: no `key-evicted` USDT probe here — a spill is not an
                // eviction (the value is still present), and firing it would show
                // phantom evictions to anyone tracing `key-evicted`. Spills are
                // observed via the `TieredSpills` / `TieredBytesSpilled` metrics
                // above.

                tracing::debug!(
                    shard_id = self.shard_id(),
                    key = %String::from_utf8_lossy(key),
                    bytes_freed,
                    policy = %self.eviction.policy(),
                    "Spilled key to warm tier"
                );

                true
            }
            Err(e) => {
                tracing::warn!(
                    shard_id = self.shard_id(),
                    key = %String::from_utf8_lossy(key),
                    error = %e,
                    "Failed to spill key"
                );
                // Fall back to deletion
                self.delete_for_eviction(key).await
            }
        }
    }

    /// Delete a key for eviction (updates metrics and pool).
    ///
    /// The removal's canonical write effects — version bump, client-tracking
    /// invalidation, the `evicted` keyspace notification, dirty counter, waiter
    /// drain, **WAL delete** (durability — an evicted non-TTL key must not
    /// resurrect on restart), **search-index delete**, and (policy-gated)
    /// replication — all come from the shared pipeline via
    /// [`ShardWorker::run_internal_removal_effects`], reconstructing the removal
    /// as a synthetic `DEL`. Only the eviction-specific accounting
    /// (`record_evicted`, the `EvictionKeys`/`EvictionBytes` metrics, the probe)
    /// stays here — mirroring how `scatter_del` keeps lazyfree accounting local.
    async fn delete_for_eviction(&mut self, key: &[u8]) -> bool {
        // Get memory size before deletion for metrics
        let memory_freed = self
            .store
            .get_metadata(key)
            .map(|m| m.memory_size)
            .unwrap_or(0);

        // Remove from eviction pool
        self.eviction.forget_key(key);

        // Delete the key
        if self.store.delete(key) {
            self.observability.record_evicted();

            // Record eviction metrics (not pipeline effects — kept local).
            let shard_label = self.shard_id().to_string();
            let policy_label = self.eviction.policy_label();
            EvictionKeysTotal::inc(self.observability.metrics(), &shard_label, &policy_label);
            EvictionBytesTotal::inc_by(
                self.observability.metrics(),
                memory_freed as u64,
                &shard_label,
            );

            tracing::debug!(
                shard_id = self.shard_id(),
                key = %String::from_utf8_lossy(key),
                memory_freed = memory_freed,
                policy = %self.eviction.policy(),
                "Evicted key"
            );

            // Drive the canonical removal effects (incl. WAL delete + search
            // delete + tracking invalidation + `evicted` notification) through
            // the shared pipeline. Eviction persists the tombstone (`wal: true`)
            // so restart cannot resurrect the key; replication is enabled so a
            // replica evicts the same key the primary did (no divergence).
            let removed = vec![Bytes::copy_from_slice(key)];
            self.run_internal_removal_effects(
                vec![(RemovalReason::Evicted, removed)],
                RemovalPropagation {
                    wal: true,
                    replicate: true,
                },
                ENGINE_INTERNAL_CONN_ID,
            )
            .await;

            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod eviction_effect_tests {
    //! End-to-end pins that eviction now drives the canonical write-effect
    //! pipeline: the removal replicates to replicas (a synthetic `DEL`), emits an
    //! `evicted` notification, and — the headline — persists a WAL tombstone so
    //! an evicted non-TTL key does **not** resurrect on restart. The old
    //! hand-rolled `delete_for_eviction` skipped replication, WAL, search, and
    //! tracking; these are the red-first regression guards for that live bug.
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use crate::keyspace_event::KeyspaceEventFlags;

    use crate::command::{Arity, Command, CommandContext, CommandFlags, WaiterWake, WalStrategy};
    use crate::command_spec::{
        AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec, ReindexSpec,
    };
    use crate::persistence::wal::{DurabilityMode, WalConfig};
    use crate::persistence::{RocksConfig, RocksStore, recover_shard};
    use crate::registry::CommandRegistry;
    use crate::replication::{ReplicationBroadcaster, SharedBroadcaster};
    use crate::shard::builder::ShardWorkerBuilder;
    use crate::shard::message::{ShardReceiver, ShardSender, WatchEntry};
    use crate::store::HashMapStore;
    use crate::types::Value;
    use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

    #[derive(Default)]
    struct RecordingBroadcaster {
        commands: Mutex<Vec<(String, Vec<Bytes>)>>,
    }
    impl ReplicationBroadcaster for RecordingBroadcaster {
        fn broadcast_command_on_shard(&self, _s: u16, cmd: &str, args: &[Bytes]) -> u64 {
            let mut g = self.commands.lock().unwrap();
            g.push((cmd.to_string(), args.to_vec()));
            g.len() as u64
        }
        fn is_active(&self) -> bool {
            true
        }
        fn current_offset(&self) -> u64 {
            self.commands.lock().unwrap().len() as u64
        }
    }

    /// Real-enough `SET`: stores the value so the WAL's deferred `write_set`
    /// (Put) has a value to persist.
    struct MockSet;
    impl Command for MockSet {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
                arity: Arity::AtLeast(2),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::PersistFirstKey,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            ctx.store
                .set(args[0].clone(), Value::string(args[1].clone()));
            Ok(Response::ok())
        }
    }

    /// `DEL` stand-in the internal-removal pipeline resolves from the registry.
    struct MockDel;
    impl Command for MockDel {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "DEL",
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE,
                keys: KeySpec::All,
                access: AccessSpec::Uniform,
                wal: WalStrategy::DeleteKeys,
                wakes: WaiterWake::All,
                event: EventSpec::Emits {
                    class: KeyspaceEventFlags::GENERIC,
                    name: "del",
                },
                requires_same_slot: false,
                reindex: ReindexSpec::DeleteKeys,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    fn registry() -> CommandRegistry {
        let mut r = CommandRegistry::new();
        r.register(MockSet);
        r.register(MockDel);
        r
    }

    /// Bare (no persistence) worker with a recording broadcaster + DEL/SET.
    fn worker(bc: SharedBroadcaster) -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_c_tx, c_rx) = mpsc::channel(16);
        let mut w = ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            c_rx,
            Arc::new(vec![ShardSender::new(msg_tx)]),
            Arc::new(registry()),
            crate::eviction::EvictionConfig::default(),
            Arc::new(crate::noop::NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            bc,
        );
        let flags = KeyspaceEventFlags::KEYSPACE
            | KeyspaceEventFlags::KEYEVENT
            | KeyspaceEventFlags::EVICTED;
        w.set_notify_keyspace_events(Arc::new(AtomicU32::new(flags.bits())));
        w
    }

    /// Persistent worker (real RocksDB + WAL) sharing `rocks`, for the restart
    /// round-trip.
    fn persistent_worker(rocks: Arc<RocksStore>, bc: SharedBroadcaster) -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_c_tx, c_rx) = mpsc::channel(16);
        let wal_config = WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 1 << 20,
            batch_timeout_ms: 1000,
            ..Default::default()
        };
        ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(msg_rx))
            .with_new_conn_rx(c_rx)
            .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
            .with_registry(Arc::new(registry()))
            .with_store(HashMapStore::new())
            .with_persistence(rocks, wal_config)
            .with_replication(bc)
            .build()
    }

    async fn flush(w: &ShardWorker) {
        w.persistence
            .wal_writer()
            .unwrap()
            .flush_async()
            .await
            .unwrap();
    }

    /// Eviction replicates a synthetic `DEL` and emits an `evicted` notification.
    /// Old `delete_for_eviction` skipped the broadcast entirely (replica keeps a
    /// key the primary evicted → divergence).
    #[tokio::test]
    async fn eviction_replicates_del_and_notifies() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut w = worker(bc.clone() as SharedBroadcaster);

        let (ntx, mut nrx) = crate::pubsub::PubSubSender::unbounded();
        w.subscriptions
            .subscribe(Bytes::from_static(b"__keyevent@0__:evicted"), 1, ntx);

        w.store.set(Bytes::from_static(b"k"), Value::string("v"));
        assert!(w.delete_for_eviction(b"k").await);

        // Broadcast (previously skipped): a DEL over the evicted key.
        let cmds = bc.commands.lock().unwrap();
        assert_eq!(cmds.len(), 1, "eviction must replicate a DEL");
        assert_eq!(cmds[0].0, "DEL");
        assert_eq!(cmds[0].1, vec![Bytes::from_static(b"k")]);

        // `evicted` keyevent still fires.
        match nrx.try_recv() {
            Ok(crate::pubsub::PubSubMessage::Message { channel, payload }) => {
                assert_eq!(&channel[..], b"__keyevent@0__:evicted");
                assert_eq!(&payload[..], b"k");
            }
            other => panic!("expected an `evicted` keyevent, got {other:?}"),
        }
    }

    /// The headline durability regression: an evicted **non-TTL** key must not
    /// resurrect on restart. Old `delete_for_eviction` skipped the WAL, so
    /// RocksDB kept the key's Put with no tombstone and recovery reloaded it.
    #[tokio::test]
    async fn eviction_is_durable_across_restart() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut w = persistent_worker(rocks.clone(), bc as SharedBroadcaster);

        // Write a non-TTL key through the full write path (persists a Put).
        let set = ParsedCommand::new(
            Bytes::from_static(b"SET"),
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
        );
        w.execute_command(&set, 1, ProtocolVersion::Resp2, false)
            .await;
        flush(&w).await;

        // Sanity: the key survives a restart before eviction.
        let (store, _idx, _stats) = recover_shard(&rocks, 0).unwrap();
        assert!(
            store.contains(b"k"),
            "key must be persisted before eviction"
        );

        // Evict it, then flush the tombstone.
        assert!(w.delete_for_eviction(b"k").await);
        flush(&w).await;

        // Restart via the recovery path: the evicted key must be ABSENT.
        let (store, _idx, _stats) = recover_shard(&rocks, 0).unwrap();
        assert!(
            !store.contains(b"k"),
            "evicted non-TTL key resurrected on restart — WAL tombstone missing"
        );
    }

    /// A bare worker whose store has a warm tier configured, so `spill_key`
    /// actually spills (hot→warm) instead of erroring and falling back to a
    /// real delete. Returns the `TempDir` so the RocksDB warm CF outlives the
    /// worker for the test's duration.
    fn worker_with_warm(bc: SharedBroadcaster) -> (ShardWorker, TempDir) {
        let mut w = worker(bc);
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open_with_warm(tmp.path(), 1, &RocksConfig::default(), true).unwrap(),
        );
        w.store.set_warm_store(rocks, 0);
        (w, tmp)
    }

    /// Spill is TIERING, not removal: the value moves hot→warm but stays
    /// readable (unspills on access), so a client WATCHing the key must NOT get
    /// a spurious EXEC abort. Red before the fix: `spill_for_eviction` bumped
    /// the key's slot version, dirtying the watch even though nothing changed.
    #[tokio::test]
    async fn spill_preserves_watch_version() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let (mut w, _tmp) = worker_with_warm(bc as SharedBroadcaster);

        w.store
            .set(Bytes::from_static(b"k"), Value::string("a spillable value"));

        // Snapshot the WATCH version at watch time.
        let watched_ver = w.get_key_version(b"k");
        let watch = [WatchEntry {
            key: Bytes::from_static(b"k"),
            version: watched_ver,
            live_at_watch: true,
        }];
        assert!(
            w.check_watches(&watch),
            "sanity: a fresh watch is satisfied"
        );

        // Spill the key: value leaves RAM but remains logically present.
        assert!(w.spill_for_eviction(b"k").await, "spill must succeed");
        assert_eq!(
            w.store.warm_tier().warm_keys(),
            1,
            "the key must actually have spilled (not fallen back to delete)"
        );
        assert!(
            w.store.contains(b"k"),
            "a spilled key is still logically present"
        );

        // The value is unchanged, so the WATCH version must be untouched and the
        // watch must survive EXEC.
        assert_eq!(
            w.get_key_version(b"k"),
            watched_ver,
            "spill must NOT bump the WATCH version (unchanged value)"
        );
        assert!(
            w.check_watches(&watch),
            "WATCH on a merely-spilled key must survive EXEC"
        );
    }

    /// A spill emits NO `evicted` keyspace notification (the value is still
    /// readable — `evicted` means gone, misleading here), while a TRUE eviction
    /// still does. Red before the fix: `spill_for_eviction` emitted `evicted`.
    #[tokio::test]
    async fn spill_emits_no_evicted_notification_but_true_eviction_does() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let (mut w, _tmp) = worker_with_warm(bc as SharedBroadcaster);

        let (ntx, mut nrx) = crate::pubsub::PubSubSender::unbounded();
        w.subscriptions
            .subscribe(Bytes::from_static(b"__keyevent@0__:evicted"), 1, ntx);

        // Spill a key: still readable afterwards → NO `evicted` event.
        w.store.set(
            Bytes::from_static(b"spilled"),
            Value::string("readable after spill"),
        );
        assert!(w.spill_for_eviction(b"spilled").await);
        assert_eq!(
            w.store.warm_tier().warm_keys(),
            1,
            "the key must actually have spilled (not fallen back to delete)"
        );
        assert!(
            nrx.try_recv().is_err(),
            "a spill must NOT emit an `evicted` keyspace notification"
        );

        // Regression guard: a TRUE eviction (delete) still emits `evicted`.
        w.store.set(Bytes::from_static(b"gone"), Value::string("v"));
        assert!(w.delete_for_eviction(b"gone").await);
        match nrx.try_recv() {
            Ok(crate::pubsub::PubSubMessage::Message { channel, payload }) => {
                assert_eq!(&channel[..], b"__keyevent@0__:evicted");
                assert_eq!(&payload[..], b"gone");
            }
            other => panic!("a true eviction must still emit `evicted`, got {other:?}"),
        }
    }
}
