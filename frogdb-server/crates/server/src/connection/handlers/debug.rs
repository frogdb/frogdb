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

use bytes::Bytes;
use frogdb_core::shard::{LockTableInfo, VllQueueInfo};
use frogdb_core::{BoxFuture, DebugProvider, KeysizeHistograms};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

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
            use tokio::sync::oneshot;

            let mut results = Vec::new();
            let timeout = std::time::Duration::from_secs(5);

            let shard_ids: Vec<usize> = match shard_filter {
                Some(id) => vec![id],
                None => (0..self.core.shard_senders.len()).collect(),
            };

            for shard_id in shard_ids {
                let (response_tx, response_rx) = oneshot::channel();

                let send_result = self.core.shard_senders[shard_id]
                    .send(frogdb_core::shard::ShardMessage::GetVllQueueInfo { response_tx })
                    .await;

                if send_result.is_err() {
                    tracing::warn!(shard_id, "Failed to send GetVllQueueInfo message");
                    continue;
                }

                match tokio::time::timeout(timeout, response_rx).await {
                    Ok(Ok(info)) => {
                        results.push(info);
                    }
                    Ok(Err(_)) => {
                        tracing::warn!(shard_id, "Channel closed while waiting for VLL info");
                    }
                    Err(_) => {
                        tracing::warn!(shard_id, "Timeout waiting for VLL info");
                    }
                }
            }

            results
        })
    }

    /// DEBUG LOCKTABLE — gather the VLL lock-table snapshot from every shard.
    fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>> {
        Box::pin(async move {
            use tokio::sync::oneshot;

            let mut results = Vec::new();
            let timeout = std::time::Duration::from_secs(5);

            for shard_id in 0..self.core.shard_senders.len() {
                let (response_tx, response_rx) = oneshot::channel();
                if self.core.shard_senders[shard_id]
                    .send(frogdb_core::shard::ShardMessage::GetLockTableInfo { response_tx })
                    .await
                    .is_err()
                {
                    tracing::warn!(shard_id, "Failed to send GetLockTableInfo message");
                    continue;
                }
                match tokio::time::timeout(timeout, response_rx).await {
                    Ok(Ok(info)) => results.push(info),
                    Ok(Err(_)) => {
                        tracing::warn!(shard_id, "Channel closed while waiting for lock-table info")
                    }
                    Err(_) => tracing::warn!(shard_id, "Timeout waiting for lock-table info"),
                }
            }
            results
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
                .send(frogdb_core::shard::ShardMessage::GetPubSubLimitsInfo { response_tx })
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
            for sender in self.core.shard_senders.iter() {
                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if sender
                    .send(frogdb_core::ShardMessage::SetActiveExpire {
                        enabled,
                        response_tx,
                    })
                    .await
                    .is_ok()
                {
                    let _ = response_rx.await;
                }
            }
        })
    }

    /// DEBUG KEYSIZES-HIST-ASSERT — merge keysize histograms across all shards.
    fn keysizes_snapshot<'a>(&'a self) -> BoxFuture<'a, KeysizeHistograms> {
        Box::pin(async move {
            let mut merged = KeysizeHistograms::new();
            for sender in self.core.shard_senders.iter() {
                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if sender
                    .send(frogdb_core::ShardMessage::KeysizesSnapshot { response_tx })
                    .await
                    .is_ok()
                    && let Ok(Some(snap)) = response_rx.await
                {
                    merged.merge(&snap);
                }
            }
            merged
        })
    }

    /// DEBUG ALLOCSIZE-SLOTS-ASSERT — total allocated memory for keys in `slot`.
    fn allocsize_in_slot<'a>(&'a self, slot: u16) -> BoxFuture<'a, usize> {
        Box::pin(async move {
            let mut total = 0usize;
            for sender in self.core.shard_senders.iter() {
                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if sender
                    .send(frogdb_core::ShardMessage::AllocsizeInSlot { slot, response_tx })
                    .await
                    .is_ok()
                    && let Ok(size) = response_rx.await
                {
                    total += size;
                }
            }
            total
        })
    }
}
