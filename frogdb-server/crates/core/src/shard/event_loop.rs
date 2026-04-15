use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::store::Store;

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
                _ = async {
                    match &mut self.vll.pending_continuation_release {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Release signal received - clear the continuation lock
                    self.vll.continuation_lock = None;
                    self.vll.pending_continuation_release = None;
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
    /// This method deletes expired keys up to a time budget to avoid
    /// blocking the event loop for too long.
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

        let budget = Duration::from_millis(25);
        let start = Instant::now();
        let now = Instant::now();

        // Get expired keys using the cleaner abstraction
        let expired = self.store.get_expired_keys(now);
        let mut deleted_count = 0u64;

        for key in expired {
            if start.elapsed() > budget {
                tracing::trace!(shard_id = self.shard_id(), "Active expiry budget exhausted");
                break;
            }

            // Delete the key
            if self.store.delete(&key) {
                deleted_count += 1;

                // Invalidate tracked clients for expired key
                if self.tracking.has_tracking_clients() {
                    self.tracking.invalidate_keys(&[key.as_ref()], 0);
                }

                // Remove from search indexes
                self.delete_from_search_indexes(&key);

                // Fire USDT probe: key-expired
                crate::probes::fire_key_expired(
                    std::str::from_utf8(&key).unwrap_or("<binary>"),
                    self.shard_id() as u64,
                );

                tracing::trace!(
                    shard_id = self.shard_id(),
                    key = %String::from_utf8_lossy(&key),
                    "Active expiry deleted key"
                );
            }
        }

        // Field-level expiry sweep
        let expired_fields = self.store.get_expired_fields(now);
        let mut field_deleted_count = 0u64;

        // Collect unique keys that have expired fields
        let mut keys_with_expired_fields: Vec<Bytes> = Vec::new();
        let mut seen_keys: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
        for (key, _field) in expired_fields {
            if seen_keys.insert(key.clone()) {
                keys_with_expired_fields.push(key);
            }
        }

        for key in keys_with_expired_fields {
            if start.elapsed() > budget {
                tracing::trace!(
                    shard_id = self.shard_id(),
                    "Active field expiry budget exhausted"
                );
                break;
            }

            let key_existed_before = self.store.get(&key).is_some();
            let purged = self.store.purge_expired_hash_fields(&key) as u64;
            field_deleted_count += purged;

            // If the key existed before but is gone now, the hash was emptied and deleted
            if key_existed_before && self.store.get(&key).is_none() {
                if self.tracking.has_tracking_clients() {
                    self.tracking.invalidate_keys(&[key.as_ref()], 0);
                }

                self.delete_from_search_indexes(&key);
            }
        }

        // Record expired keys metric and increment version
        let total_expired = deleted_count + field_deleted_count;
        if total_expired > 0 {
            let shard_label = self.shard_id().to_string();
            if deleted_count > 0 {
                self.store.add_expired_keys(deleted_count);
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_keys_expired_total",
                    deleted_count,
                    &[("shard", &shard_label)],
                );
            }
            if field_deleted_count > 0 {
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_fields_expired_total",
                    field_deleted_count,
                    &[("shard", &shard_label)],
                );
            }
            self.increment_version();
        }
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
            | UpdateConfig { .. } => {
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
