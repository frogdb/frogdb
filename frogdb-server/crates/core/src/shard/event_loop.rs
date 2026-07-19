use std::time::{Duration, Instant};

use frogdb_types::metrics::definitions::{FieldsExpired, KeysExpired, ShardQueueLatency};

use crate::keyspace_event::KeyspaceEventFlags;

use super::active_expiry::ExpiryResult;
use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Run the shard worker event loop.
    pub async fn run(mut self) {
        tracing::info!(shard_id = self.shard_id(), "Shard worker started");

        // Active expiry runs every 100ms
        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));

        // Metrics collection runs every 10 seconds
        let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

        // Blocking waiter timeout check runs every 100ms
        let mut waiter_timeout_interval = tokio::time::interval(Duration::from_millis(100));

        // Search index commit runs every 1 second
        let mut search_commit_interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages — dispatch to grouped sub-handlers
                Some(envelope) = self.message_rx.recv() => {
                    let queue_latency = envelope.enqueued_at.elapsed().as_secs_f64();
                    let msg = envelope.message;

                    crate::probes::fire_shard_message_received(
                        self.shard_id() as u64,
                        msg.probe_type_str(),
                        self.message_rx.len() as u64,
                    );

                    ShardQueueLatency::observe(
                        self.observability.metrics(),
                        queue_latency,
                        self.identity.shard_label(),
                    );

                    if self.dispatch_message(msg).await {
                        break;
                    }
                }

                // Active expiry task
                _ = expiry_interval.tick() => {
                    if self.per_request_spans.load(std::sync::atomic::Ordering::Relaxed) {
                        let _span = tracing::info_span!("active_expiry", shard_id = self.shard_id()).entered();
                        self.run_active_expiry();
                    } else {
                        self.run_active_expiry();
                    }
                }

                // Periodic metrics collection
                _ = metrics_interval.tick() => {
                    self.collect_shard_metrics();
                }

                // Blocking waiter timeout check
                _ = waiter_timeout_interval.tick() => {
                    self.check_waiter_timeouts();
                }

                // Periodic search index commit
                _ = search_commit_interval.tick() => {
                    let sid = self.identity.shard_id();
                    for idx in self.search.indexes.values_mut() {
                        if idx.is_dirty() && let Err(e) = idx.commit() {
                            tracing::error!(shard_id = sid, error = %e, "Failed to commit search index");
                        }
                    }
                }

                // Check for continuation lock release signal
                _ = self.vll.await_continuation_release() => {
                    self.vll.clear_continuation_lock();
                    tracing::debug!(shard_id = self.shard_id(), "Continuation lock released");
                }

                else => break,
            }
        }

        // Final search index commit
        {
            let sid = self.identity.shard_id();
            for idx in self.search.indexes.values_mut() {
                if idx.is_dirty()
                    && let Err(e) = idx.commit()
                {
                    tracing::error!(shard_id = sid, error = %e, "Failed to commit search index on shutdown");
                }
            }
        }

        // Final WAL flush
        if let Some(wal) = self.persistence.wal_writer()
            && let Err(e) = wal.flush_async().await
        {
            tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL on exit");
        }
    }

    /// Run active expiry with time budget.
    ///
    /// Thin shard-side wrapper: the pause/disable gates read shard-owned atomics
    /// and stay here; the decision + deletion half is delegated to
    /// [`ActiveExpiryCoordinator::run_cycle`], and the side effects are applied
    /// past the seam from the returned [`ExpiryResult`].
    fn run_active_expiry(&mut self) {
        // Sync the expiry_paused flag to the store for passive expiry suppression.
        let paused = self
            .expiry_paused
            .load(std::sync::atomic::Ordering::Relaxed);
        self.store.set_expiry_suppressed(paused);

        // Skip active expiry during CLIENT PAUSE to prevent master/replica divergence.
        if paused {
            return;
        }

        // Skip active expiry when disabled via DEBUG SET-ACTIVE-EXPIRE 0.
        if self.debug_active_expire_disabled {
            return;
        }

        // Disjoint-field borrow: `self.expiry` and `self.store` are distinct fields.
        let result = self.expiry.run_cycle(&mut self.store, Instant::now());
        self.apply_expiry_effects(result);
    }

    /// Apply the side effects of an active-expiry cycle.
    ///
    /// This is the shard side of the seam: it owns the state the coordinator is
    /// deliberately blind to — client tracking, search indexes, keyspace
    /// notifications, USDT probes, metrics, and the version counter — and drives
    /// them from the `ExpiryResult`.
    fn apply_expiry_effects(&mut self, result: ExpiryResult) {
        // Keys whose own TTL elapsed: full effect set.
        for key in &result.deleted_keys {
            // Invalidate tracked clients for expired key
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }

            // Remove from search indexes
            self.delete_from_search_indexes(key);

            // Emit expired keyspace notification
            self.emit_keyspace_notification(key, "expired", KeyspaceEventFlags::EXPIRED);

            // Fire USDT probe: key-expired
            crate::probes::fire_key_expired(
                std::str::from_utf8(key).unwrap_or("<binary>"),
                self.shard_id() as u64,
            );
        }

        // Keys removed because their last hash field expired. Redis emits a
        // generic `del` for a key whose hash empties via field TTL (distinct
        // from the whole-key `expired` event above). Match that, and fire the
        // key-expired probe so the removal is not invisible to observers.
        for key in &result.emptied_keys {
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }

            self.delete_from_search_indexes(key);

            self.emit_keyspace_notification(key, "del", KeyspaceEventFlags::GENERIC);

            crate::probes::fire_key_expired(
                std::str::from_utf8(key).unwrap_or("<binary>"),
                self.shard_id() as u64,
            );
        }

        // Record expired keys metric and increment version.
        if result.is_empty() {
            return;
        }
        let shard_label = self.shard_id().to_string();
        // Count every key removed this cycle exactly once — key-level TTL AND
        // field-emptied — so INFO `expired_keys` and `frogdb_keys_expired_total`
        // do not under-count genuine expirations. The fields that triggered an
        // emptied key are counted separately below, so no double-count.
        let keys_expired = result.keys_expired();
        if keys_expired > 0 {
            self.store.add_expired_keys(keys_expired);
            KeysExpired::inc_by(self.observability.metrics(), keys_expired, &shard_label);
        }
        if result.fields_expired > 0 {
            FieldsExpired::inc_by(
                self.observability.metrics(),
                result.fields_expired,
                &shard_label,
            );
        }
        self.increment_version();
    }

    /// Dispatch a shard message to the appropriate handler.
    /// Returns `true` if the event loop should break (shutdown).
    async fn dispatch_message(&mut self, msg: ShardMessage) -> bool {
        use ShardMessage::*;
        match msg {
            Execute { .. } | ScatterRequest { .. } | GetVersion { .. } | ExecTransaction { .. } => {
                self.dispatch_core(msg).await
            }
            Subscribe { .. }
            | Unsubscribe { .. }
            | PSubscribe { .. }
            | PUnsubscribe { .. }
            | Publish { .. }
            | PublishKeyspace { .. }
            | ShardedSubscribe { .. }
            | ShardedUnsubscribe { .. }
            | ShardedPublish { .. }
            | PubSubIntrospection { .. }
            | ConnectionClosed { .. } => {
                self.dispatch_pubsub(msg);
                false
            }
            TrackingRegister { .. }
            | TrackingUnregister { .. }
            | TrackingBroadcastRegister { .. } => {
                self.dispatch_tracking(msg);
                false
            }
            EvalScript { .. }
            | EvalScriptSha { .. }
            | ScriptLoad { .. }
            | ScriptExists { .. }
            | ScriptFlush { .. }
            | ScriptKill { .. }
            | ScriptSubCommand { .. }
            | FunctionCall { .. } => self.dispatch_scripting(msg).await,
            BlockWait { .. } | UnregisterWait { .. } => {
                self.dispatch_blocking(msg);
                false
            }
            SlowlogGet { .. }
            | SlowlogLen { .. }
            | SlowlogReset { .. }
            | SlowlogAdd { .. }
            | MemoryUsage { .. }
            | MemoryStats { .. }
            | WalLagStats { .. }
            | InfoSnapshot { .. }
            | ScanBigKeys { .. }
            | LatencyLatest { .. }
            | LatencyHistory { .. }
            | LatencyReset { .. }
            | ResetStats { .. }
            | HotShardStats { .. }
            | UpdateConfig { .. }
            | SetActiveExpire { .. }
            | SetKeyMemoryHistograms { .. }
            | KeysizesSnapshot { .. }
            | AllocsizeInSlot { .. } => {
                self.dispatch_observability(msg);
                false
            }
            VllLockRequest { .. }
            | VllExecute { .. }
            | VllAbort { .. }
            | VllContinuationLock { .. }
            | GetVllQueueInfo { .. } => self.dispatch_vll(msg).await,
            GetLockTableInfo { .. } | GetWaitQueueInfo { .. } | MemoryCheck { .. } => {
                self.dispatch_debug_introspection(msg);
                false
            }
            SlotMigrated { .. } | RaftCommand { .. } => self.dispatch_cluster(msg).await,
            FlushSearchIndexes { .. } | GetPubSubLimitsInfo { .. } => {
                self.dispatch_search(msg);
                false
            }
            Shutdown => {
                tracing::info!(shard_id = self.shard_id(), "Shard worker shutting down");
                if let Some(wal) = self.persistence.wal_writer()
                    && let Err(e) = wal.flush_async().await
                {
                    tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL on shutdown");
                }
                true
            }
        }
    }
}

#[cfg(test)]
mod effect_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::ExpiryResult;
    use crate::eviction::EvictionConfig;
    use crate::keyspace_event::KeyspaceEventFlags;
    use crate::noop::MetricsRecorder;
    use crate::pubsub::{PubSubMessage, PubSubSender};
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{Envelope, ShardReceiver};

    /// Records counter increments so tests can read cumulative totals back.
    #[derive(Default)]
    struct RecordingRecorder {
        counters: Mutex<HashMap<String, u64>>,
    }

    impl MetricsRecorder for RecordingRecorder {
        fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
            *self
                .counters
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert(0) += value;
        }
        fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn counter_value(&self, name: &str) -> Option<u64> {
            self.counters.lock().unwrap().get(name).copied()
        }
    }

    /// Build a bare in-memory shard worker (no persistence) using the given
    /// metrics recorder. Holds the channel send-halves alive so the receivers
    /// stay open for the worker's lifetime.
    fn build_worker(
        recorder: Arc<dyn MetricsRecorder>,
    ) -> (
        ShardWorker,
        mpsc::Sender<Envelope>,
        mpsc::Sender<crate::shard::NewConnection>,
    ) {
        let (msg_tx, msg_rx) = mpsc::channel::<Envelope>(8);
        let (conn_tx, conn_rx) = mpsc::channel::<crate::shard::NewConnection>(8);
        let worker = ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            Arc::new(vec![]),
            Arc::new(CommandRegistry::new()),
            EvictionConfig::default(),
            recorder,
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        );
        (worker, msg_tx, conn_tx)
    }

    /// Enable keyspace notifications (keyspace + keyevent, generic + expired
    /// classes) and subscribe `rx` to the given key-event channels.
    fn enable_notifications_and_subscribe(
        worker: &mut ShardWorker,
        event_channels: &[&str],
    ) -> mpsc::UnboundedReceiver<PubSubMessage> {
        let flags = KeyspaceEventFlags::KEYSPACE
            | KeyspaceEventFlags::KEYEVENT
            | KeyspaceEventFlags::GENERIC
            | KeyspaceEventFlags::EXPIRED;
        worker.set_notify_keyspace_events(Arc::new(AtomicU32::new(flags.bits())));

        let (tx, rx): (PubSubSender, _) = mpsc::unbounded_channel();
        for ch in event_channels {
            worker
                .subscriptions
                .subscribe(Bytes::from(ch.to_string()), 1, tx.clone());
        }
        rx
    }

    /// Collect all currently-queued (channel, payload) pairs.
    fn drain(rx: &mut mpsc::UnboundedReceiver<PubSubMessage>) -> Vec<(String, String)> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let PubSubMessage::Message { channel, payload } = msg {
                out.push((
                    String::from_utf8_lossy(&channel).into_owned(),
                    String::from_utf8_lossy(&payload).into_owned(),
                ));
            }
        }
        out
    }

    #[test]
    fn notifications_fired_for_both_deletion_paths() {
        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);
        let mut rx = enable_notifications_and_subscribe(
            &mut worker,
            &["__keyevent@0__:expired", "__keyevent@0__:del"],
        );

        let result = ExpiryResult {
            deleted_keys: vec![Bytes::from("plain")],
            emptied_keys: vec![Bytes::from("h")],
            fields_expired: 1,
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result);

        let events = drain(&mut rx);
        // Key-level TTL key -> `expired`; field-emptied key -> `del`.
        assert!(
            events.contains(&("__keyevent@0__:expired".into(), "plain".into())),
            "expected `expired` event for key-level expiry, got {events:?}"
        );
        assert!(
            events.contains(&("__keyevent@0__:del".into(), "h".into())),
            "expected `del` event for field-emptied key, got {events:?}"
        );
    }

    #[test]
    fn expired_keys_stat_counts_both_paths_without_double_count() {
        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        let result = ExpiryResult {
            // 2 key-level + 1 field-emptied = 3 keys removed.
            deleted_keys: vec![Bytes::from("a"), Bytes::from("b")],
            emptied_keys: vec![Bytes::from("h")],
            fields_expired: 4,
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result);

        // Key counter: 3 keys (both paths), counted once each.
        assert_eq!(recorder.counter_value("frogdb_keys_expired_total"), Some(3));
        assert_eq!(worker.store.expired_keys(), 3);
        // Field counter: independent unit, no overlap with the key counter.
        assert_eq!(
            recorder.counter_value("frogdb_fields_expired_total"),
            Some(4)
        );
    }

    #[test]
    fn empty_result_records_nothing() {
        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        worker.apply_expiry_effects(ExpiryResult::default());

        assert_eq!(recorder.counter_value("frogdb_keys_expired_total"), None);
        assert_eq!(recorder.counter_value("frogdb_fields_expired_total"), None);
        assert_eq!(worker.store.expired_keys(), 0);
    }
}
