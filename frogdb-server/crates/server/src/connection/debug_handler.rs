//! DEBUG command provider.
//!
//! DEBUG is dispatched through the [`frogdb_core::ConnectionCommand`] seam (see
//! [`crate::connection::debug_conn_command`]): its executor owns the subcommand
//! routing and argument parsing and delegates the per-subcommand *I/O* here, via
//! the [`frogdb_core::DebugProvider`] impl on `ConnectionHandler`. Only the work
//! that needs handler-owned state lives behind the seam — the `shared_tracer`,
//! per-shard round-trips, this connection's own subscription counts, the
//! `frogdb_debug` bundle machinery, and the `enable-debug-command` gate. The
//! logic is identical to the pre-migration `handle_debug_*` helpers, so every
//! subcommand's wire output is byte-for-byte unchanged.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::shard::{
    ExpiryIndexCheckInfo, LockTableInfo, MemoryCheckInfo, VllQueueInfo, WaitQueueInfo,
    WaitQueueLogInfo,
};
use frogdb_core::{BoxFuture, DebugProvider, KeysizeHistograms};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::replication::PrimaryReplicationHandler;

impl DebugProvider for ConnectionHandler {
    fn debug_command_enabled(&self) -> bool {
        self.enable_debug_command
    }

    /// DEBUG TRACING STATUS.
    fn tracing_status(&self) -> Response {
        match &self.observability.shared_tracer {
            Some(tracer) => {
                let status = tracer.get_status();
                let lines = [
                    format!("enabled:{}", if status.enabled { "yes" } else { "no" }),
                    format!("sampling_rate:{}", status.sampling_rate),
                    format!("otlp_endpoint:{}", status.otlp_endpoint),
                    format!("service_name:{}", status.service_name),
                    format!("recent_traces_count:{}", status.recent_traces_count),
                    format!("scatter_gather_spans:{}", status.scatter_gather_spans),
                    format!("shard_spans:{}", status.shard_spans),
                    format!("persistence_spans:{}", status.persistence_spans),
                ];
                Response::Bulk(Some(Bytes::from(lines.join("\r\n"))))
            }
            None => Response::Bulk(Some(Bytes::from(
                "enabled:no\r\nreason:tracer not configured",
            ))),
        }
    }

    /// DEBUG TRACING RECENT [count] — the executor parses `count`.
    fn tracing_recent(&self, count: usize) -> Response {
        match &self.observability.shared_tracer {
            Some(tracer) => {
                let traces = tracer.get_recent_traces(count);
                let entries: Vec<Response> = traces
                    .iter()
                    .map(|t| {
                        Response::Array(vec![
                            Response::Bulk(Some(Bytes::from(t.trace_id.clone()))),
                            Response::Integer(t.timestamp_ms as i64),
                            Response::Bulk(Some(Bytes::from(t.command.clone()))),
                            Response::Integer(if t.sampled { 1 } else { 0 }),
                        ])
                    })
                    .collect();
                Response::Array(entries)
            }
            None => Response::Array(vec![]),
        }
    }

    /// DEBUG VLL [shard_id] — gather VLL queue info from the selected shard(s).
    /// The executor validated `shard_filter` and formats the reply.
    fn gather_vll<'a>(&'a self, shard_filter: Option<usize>) -> BoxFuture<'a, Vec<VllQueueInfo>> {
        Box::pin(async move {
            match shard_filter {
                // Single-shard DEBUG VLL <shard_id>: route the one round-trip
                // through the same timed send/timeout helper (best-effort — an
                // unavailable shard yields an empty snapshot).
                Some(id) => {
                    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                    let msg = frogdb_core::shard::VllMsg::GetVllQueueInfo { response_tx };
                    match self.scatter_gather().query_one(id, msg, response_rx).await {
                        Ok(info) => vec![info],
                        Err(_) => Vec::new(),
                    }
                }
                None => {
                    self.scatter_gather()
                        .gather_all(|_shard, response_tx| {
                            frogdb_core::shard::VllMsg::GetVllQueueInfo { response_tx }
                        })
                        .await
                }
            }
        })
    }

    /// DEBUG LOCKTABLE — gather the VLL lock-table snapshot from every shard.
    fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>> {
        Box::pin(async move {
            self.scatter_gather()
                .gather_all(|_shard, response_tx| {
                    frogdb_core::shard::DebugIntrospectionMsg::GetLockTableInfo { response_tx }
                })
                .await
        })
    }

    /// DEBUG WAITQUEUE — gather the blocking-waiter snapshot from every shard.
    fn gather_wait_queue<'a>(&'a self) -> BoxFuture<'a, Vec<WaitQueueInfo>> {
        Box::pin(async move {
            self.scatter_gather()
                .gather_all(|_shard, response_tx| {
                    frogdb_core::shard::DebugIntrospectionMsg::GetWaitQueueInfo { response_tx }
                })
                .await
        })
    }

    /// DEBUG WAITQUEUE-LOG — gather the blocking-registration journal from
    /// every shard.
    fn gather_wait_queue_log<'a>(&'a self) -> BoxFuture<'a, Vec<WaitQueueLogInfo>> {
        Box::pin(async move {
            self.scatter_gather()
                .gather_all(|_shard, response_tx| {
                    frogdb_core::shard::DebugIntrospectionMsg::GetWaitQueueLog { response_tx }
                })
                .await
        })
    }

    /// DEBUG MEMORY-CHECK — gather tracked-vs-recomputed memory from every shard.
    fn memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<MemoryCheckInfo>> {
        Box::pin(async move {
            self.scatter_gather()
                .gather_all(|_shard, response_tx| {
                    frogdb_core::shard::DebugIntrospectionMsg::MemoryCheck { response_tx }
                })
                .await
        })
    }

    /// DEBUG EXPIRY-INDEX-CHECK — gather the expiry-index audit from every shard.
    fn expiry_index_check<'a>(&'a self) -> BoxFuture<'a, Vec<ExpiryIndexCheckInfo>> {
        Box::pin(async move {
            self.scatter_gather()
                .gather_all(|_shard, response_tx| {
                    frogdb_core::shard::DebugIntrospectionMsg::ExpiryIndexCheck { response_tx }
                })
                .await
        })
    }

    /// DEBUG PUBSUB LIMITS — per-connection and per-shard subscription usage.
    fn pubsub_limits<'a>(&'a self) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            use frogdb_core::pubsub::{
                MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
                MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD, MAX_UNIQUE_CHANNELS_PER_SHARD,
                MAX_UNIQUE_PATTERNS_PER_SHARD,
            };
            use tokio::sync::oneshot;

            // Connection-level counts
            let conn_counts = self.state.subscription_counts();
            let conn_subscriptions = conn_counts.channels;
            let conn_patterns = conn_counts.patterns;

            // Shard-level counts from shard 0 (broadcast pub/sub coordinator)
            let (response_tx, response_rx) = oneshot::channel();
            let send_result = self.core.shard_senders[0]
                .send(frogdb_core::shard::SearchMsg::GetPubSubLimitsInfo { response_tx })
                .await;

            let (shard_total, shard_channels, shard_patterns) = if send_result.is_ok() {
                let timeout = std::time::Duration::from_secs(5);
                match tokio::time::timeout(timeout, response_rx).await {
                    Ok(Ok(info)) => (
                        info.total_subscriptions,
                        info.unique_channels,
                        info.unique_patterns,
                    ),
                    _ => {
                        return Response::error("ERR timeout waiting for shard pub/sub info");
                    }
                }
            } else {
                return Response::error("ERR failed to query shard pub/sub info");
            };

            let lines = [
                format!(
                    "connection_subscriptions: {}/{}",
                    conn_subscriptions, MAX_SUBSCRIPTIONS_PER_CONNECTION
                ),
                format!(
                    "connection_patterns: {}/{}",
                    conn_patterns, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION
                ),
                format!(
                    "shard_total_subscriptions: {}/{}",
                    shard_total, MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD
                ),
                format!(
                    "shard_unique_channels: {}/{}",
                    shard_channels, MAX_UNIQUE_CHANNELS_PER_SHARD
                ),
                format!(
                    "shard_unique_patterns: {}/{}",
                    shard_patterns, MAX_UNIQUE_PATTERNS_PER_SHARD
                ),
            ];

            Response::Bulk(Some(Bytes::from(lines.join("\r\n"))))
        })
    }

    /// DEBUG BUNDLE GENERATE [DURATION <seconds>] — the executor parses the
    /// duration. Returns the bundle id.
    fn bundle_generate<'a>(&'a self, duration_secs: u64) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            // Create bundle config and collector
            let config = frogdb_debug::BundleConfig::default();
            let collector = frogdb_debug::DiagnosticCollector::new(
                self.core.shard_senders.clone(),
                self.observability.shared_tracer.clone(),
                config.clone(),
            );

            // Collect diagnostic data
            let data = if duration_secs == 0 {
                collector.collect_instant().await
            } else {
                collector.collect_with_duration(duration_secs).await
            };

            // Generate the bundle
            let generator = frogdb_debug::BundleGenerator::new(config.clone());
            let id = frogdb_debug::BundleGenerator::generate_id();

            match generator.create_zip(&id, &data, duration_secs) {
                Ok(zip_data) => {
                    // Try to store the bundle for later HTTP download
                    let store = frogdb_debug::BundleStore::new(config);
                    if let Err(e) = store.store(&id, &zip_data) {
                        tracing::warn!(error = %e, "Failed to store bundle (HTTP download may not work)");
                    }

                    // Return the bundle ID
                    Response::Bulk(Some(Bytes::from(id)))
                }
                Err(e) => Response::error(format!("ERR Failed to generate bundle: {}", e)),
            }
        })
    }

    /// DEBUG BUNDLE LIST — list stored diagnostic bundles.
    fn bundle_list(&self) -> Response {
        let config = frogdb_debug::BundleConfig::default();
        let store = frogdb_debug::BundleStore::new(config);
        let bundles = store.list();

        let entries: Vec<Response> = bundles
            .into_iter()
            .map(|b| {
                Response::Array(vec![
                    Response::Bulk(Some(Bytes::from(b.id))),
                    Response::Integer(b.created_at as i64),
                    Response::Integer(b.size_bytes as i64),
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// DEBUG SET-ACTIVE-EXPIRE 0|1 — toggle active expiration across all shards.
    fn set_active_expire<'a>(&'a self, enabled: bool) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Await-and-discard: the replies are only a barrier confirming every
            // shard applied the toggle. Bounded by the shared deadline (was
            // unbounded).
            let _ = self
                .scatter_gather()
                .gather_all(
                    |_shard, response_tx| frogdb_core::ObservabilityMsg::SetActiveExpire {
                        enabled,
                        response_tx,
                    },
                )
                .await;
        })
    }

    /// DEBUG EXPIRE-BACKDATE <key> <ms> — rewrite the key's expiry deadline into
    /// the past on the owning shard. Routes one keyed round-trip through the same
    /// timed send/timeout helper the other single-shard probes use.
    fn expire_backdate<'a>(
        &'a self,
        shard_id: usize,
        key: Bytes,
        ms: u64,
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let msg = frogdb_core::shard::DebugIntrospectionMsg::ExpireBackdate {
                key,
                ms,
                response_tx,
            };
            match self
                .scatter_gather()
                .query_one(shard_id, msg, response_rx)
                .await
            {
                Ok(frogdb_core::store::BackdateExpiryResult::Backdated) => Response::ok(),
                Ok(frogdb_core::store::BackdateExpiryResult::NoSuchKey) => {
                    Response::error("ERR no such key")
                }
                Ok(frogdb_core::store::BackdateExpiryResult::NoExpiry) => {
                    Response::error("ERR key has no expiry to backdate")
                }
                Err(err) => err,
            }
        })
    }

    /// DEBUG RE-ENCODE <key> — rebuild the key's value through its own encoding
    /// on the owning shard. The same single keyed round-trip
    /// [`Self::expire_backdate`] uses; the executor formats the reply.
    fn re_encode<'a>(
        &'a self,
        shard_id: usize,
        key: Bytes,
    ) -> BoxFuture<'a, Result<Option<frogdb_core::store::ReEncodeResult>, Response>> {
        Box::pin(async move {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let msg = frogdb_core::shard::DebugIntrospectionMsg::ReEncode { key, response_tx };
            self.scatter_gather()
                .query_one(shard_id, msg, response_rx)
                .await
        })
    }

    /// DEBUG OBJECT <key> — gather the key's internals from the owning shard.
    /// One keyed round-trip through the same timed send/timeout helper
    /// [`Self::expire_backdate`] uses; the executor formats the reply.
    fn object_info<'a>(
        &'a self,
        shard_id: usize,
        key: Bytes,
    ) -> BoxFuture<'a, Result<Option<frogdb_core::shard::ObjectInfo>, Response>> {
        Box::pin(async move {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let msg = frogdb_core::shard::DebugIntrospectionMsg::ObjectInfo { key, response_tx };
            self.scatter_gather()
                .query_one(shard_id, msg, response_rx)
                .await
        })
    }

    /// DEBUG KEYSIZES-HIST-ASSERT — merge keysize histograms across all shards.
    fn keysizes_snapshot<'a>(&'a self) -> BoxFuture<'a, KeysizeHistograms> {
        Box::pin(async move {
            let mut merged = KeysizeHistograms::new();
            let snapshots =
                self.scatter_gather()
                    .gather_all(|_shard, response_tx| {
                        frogdb_core::ObservabilityMsg::KeysizesSnapshot { response_tx }
                    })
                    .await;
            for snap in snapshots.into_iter().flatten() {
                merged.merge(&snap);
            }
            merged
        })
    }

    /// DEBUG ALLOCSIZE-SLOTS-ASSERT — total allocated memory for keys in `slot`.
    fn allocsize_in_slot<'a>(&'a self, slot: u16) -> BoxFuture<'a, usize> {
        Box::pin(async move {
            self.scatter_gather()
                .gather_all(
                    |_shard, response_tx| frogdb_core::ObservabilityMsg::AllocsizeInSlot {
                        slot,
                        response_tx,
                    },
                )
                .await
                .into_iter()
                .sum()
        })
    }

    /// DEBUG CLUSTER CHECK — run the invariant catalog against the live
    /// `ClusterState`. A plain read-lock borrow (see
    /// `ClusterState::check_invariants`), so unlike its scatter-gather
    /// siblings this never awaits anything; `None` in standalone mode, where
    /// there is no `ClusterState` to check.
    fn cluster_check(&self) -> Option<Vec<frogdb_core::Violation>> {
        self.cluster
            .cluster_state
            .as_ref()
            .map(|cs| cs.check_invariants())
    }

    /// DEBUG REPLICATION CHECK — run the replication invariant catalog against
    /// a complete [`ReplicationView`].
    ///
    /// Answers in every mode (see the trait doc): the `None` here is "this
    /// build wired no replication seams", which a running server never is —
    /// `init_replication` constructs the primary handler on every role,
    /// standalone included, precisely so a promotion has live seams.
    ///
    /// Like its cluster twin this awaits nothing: every group is a plain read
    /// of an atomic, a lock or a small collection, and no lock is held while
    /// the catalog runs.
    fn replication_check(&self) -> Option<Vec<frogdb_core::Violation>> {
        let handler = self.cluster.primary_replication_handler.as_ref()?;
        Some(frogdb_replication::invariants::check_all(
            &self.replication_view(handler),
        ))
    }

    fn shard_arenas(&self) -> Vec<(usize, u32)> {
        self.observability
            .shard_arenas
            .samples()
            .map(|s| (s.shard_id, s.arena))
            .collect()
    }
}

impl ConnectionHandler {
    /// The widest [`ReplicationView`] this node can produce: the primary
    /// handler's own capture plus the three groups it cannot reach.
    ///
    /// Split out of [`DebugProvider::replication_check`] so the assembly is
    /// testable without a live catalog run, and so each fill can say why it is
    /// here.
    fn replication_view(
        &self,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> frogdb_replication::view::ReplicationView {
        let mut view = handler.view();
        // The handler samples the identity with `try_read` because it is also
        // called from the promotion path, which holds the write lock. This
        // surface has no such caller, and an identity-less view would silently
        // skip every `INV-REPLID-*` claim — the ones an operator asked for.
        if view.state.is_none()
            && let Some(shared) = self.cluster.replication_state.as_ref()
        {
            view = view.with_state(shared.read().clone());
        }
        // The gate is already in `handler.view()`, but with no budget: the
        // replication crate does not depend on `frogdb-cluster` and so is
        // never told the ceiling. Re-filled here, where the constant is
        // visible, so `INV-GATE-1`'s over-budget half is actually evaluated.
        view = view.with_feed_gate(handler.feed_gate().view(Some(
            std::time::Duration::from_millis(frogdb_core::HANDOFF_BARRIER_MS),
        )));
        if let Some(fence) = self.cluster.replication_self_fence.as_ref() {
            view = view.with_fence(frogdb_replication::view::FenceView {
                self_fence_enabled: fence.self_fence_enabled(),
                armed: fence.is_armed(),
                freshness_window: fence.freshness_timeout(),
            });
        }
        view.with_role(self.role_view())
    }

    /// This node's role as the catalog reads it. The upstream address comes
    /// from the same `RoleController` that `ROLE` and INFO's `master_host`
    /// read, so a violation names the primary those surfaces name.
    fn role_view(&self) -> frogdb_replication::view::RoleView {
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            frogdb_replication::view::RoleView::Replica {
                upstream: self
                    .cluster
                    .role_controller
                    .as_ref()
                    .and_then(|c| c.primary_target()),
            }
        } else {
            frogdb_replication::view::RoleView::Primary
        }
    }
}
