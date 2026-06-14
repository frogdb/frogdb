use std::time::{Duration, Instant};

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

                    self.observability.metrics_recorder.record_histogram(
                        "frogdb_shard_queue_latency_seconds",
                        queue_latency,
                        &[("shard", &self.identity.shard_label)],
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
                    let sid = self.identity.shard_id;
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
            let sid = self.identity.shard_id;
            for idx in self.search.indexes.values_mut() {
                if idx.is_dirty()
                    && let Err(e) = idx.commit()
                {
                    tracing::error!(shard_id = sid, error = %e, "Failed to commit search index on shutdown");
                }
            }
        }

        // Final WAL flush
        if let Some(ref wal) = self.persistence.wal_writer
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

        // Keys removed because their last hash field expired.
        for key in &result.emptied_keys {
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }

            self.delete_from_search_indexes(key);
            // Behavior-preserving: no notification/probe here yet (see correctness flags).
        }

        // Record expired keys metric and increment version.
        if result.is_empty() {
            return;
        }
        let shard_label = self.shard_id().to_string();
        // Behavior-preserving: key counter still tracks key-level expirations
        // only (field-emptied keys are folded in by a later correctness fix).
        let keys_expired = result.deleted_keys.len() as u64;
        if keys_expired > 0 {
            self.store.add_expired_keys(keys_expired);
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keys_expired_total",
                keys_expired,
                &[("shard", &shard_label)],
            );
        }
        if result.fields_expired > 0 {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_fields_expired_total",
                result.fields_expired,
                &[("shard", &shard_label)],
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
            | FunctionCall { .. } => self.dispatch_scripting(msg),
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
            SlotMigrated { .. } | RaftCommand { .. } => self.dispatch_cluster(msg).await,
            FlushSearchIndexes { .. } | GetPubSubLimitsInfo { .. } => {
                self.dispatch_search(msg);
                false
            }
            Shutdown => {
                tracing::info!(shard_id = self.shard_id(), "Shard worker shutting down");
                if let Some(ref wal) = self.persistence.wal_writer
                    && let Err(e) = wal.flush_async().await
                {
                    tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL on shutdown");
                }
                true
            }
        }
    }
}
