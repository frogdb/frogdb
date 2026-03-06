//! DEBUG command handlers.
//!
//! This module handles DEBUG subcommands:
//! - DEBUG SLEEP - Sleep without blocking the shard
//! - DEBUG TRACING STATUS - Show tracing status
//! - DEBUG TRACING RECENT - Show recent traces
//! - DEBUG VLL - Show VLL queue info
//! - DEBUG PUBSUB LIMITS - Show pub/sub subscription usage vs limits
//! - DEBUG BUNDLE GENERATE - Generate a diagnostic bundle
//! - DEBUG BUNDLE LIST - List available bundles

use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle DEBUG SLEEP command - sleep without blocking the shard.
    pub(crate) async fn handle_debug_sleep(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'debug|sleep' command");
        }

        // args[0] is "SLEEP", args[1] is the duration
        let duration_str = match std::str::from_utf8(&args[1]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid duration"),
        };

        let duration: f64 = match duration_str.parse() {
            Ok(d) => d,
            Err(_) => return Response::error("ERR invalid duration"),
        };

        if duration < 0.0 {
            return Response::error("ERR invalid duration");
        }

        // Sleep in the connection handler (not the shard worker)
        let duration_ms = (duration * 1000.0) as u64;
        tokio::time::sleep(Duration::from_millis(duration_ms)).await;

        Response::ok()
    }

    /// Handle DEBUG TRACING STATUS command.
    pub(crate) fn handle_debug_tracing_status(&self) -> Response {
        match &self.shared_tracer {
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

    /// Handle DEBUG TRACING RECENT [count] command.
    pub(crate) fn handle_debug_tracing_recent(&self, args: &[Bytes]) -> Response {
        // args[0] = "TRACING", args[1] = "RECENT", args[2] = optional count
        let count = args
            .get(2)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10);

        match &self.shared_tracer {
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

    /// Handle DEBUG VLL [shard_id] command.
    pub(crate) async fn handle_debug_vll(&self, args: &[Bytes]) -> Response {
        // args[0] = "VLL", args[1] = optional shard_id
        let shard_filter: Option<usize> = if args.len() > 1 {
            match std::str::from_utf8(&args[1]) {
                Ok(s) => match s.parse::<usize>() {
                    Ok(id) => {
                        if id >= self.shard_senders.len() {
                            return Response::error(format!(
                                "ERR invalid shard_id: {} (num_shards: {})",
                                id,
                                self.shard_senders.len()
                            ));
                        }
                        Some(id)
                    }
                    Err(_) => {
                        return Response::error("ERR invalid shard_id: must be a number");
                    }
                },
                Err(_) => {
                    return Response::error("ERR invalid shard_id: must be valid UTF-8");
                }
            }
        } else {
            None
        };

        let infos = self.gather_vll_queue_info(shard_filter).await;
        self.format_vll_response(infos)
    }

    /// Gather VLL queue info from shards.
    pub(crate) async fn gather_vll_queue_info(
        &self,
        shard_filter: Option<usize>,
    ) -> Vec<frogdb_core::shard::VllQueueInfo> {
        use tokio::sync::oneshot;

        let mut results = Vec::new();
        let timeout = std::time::Duration::from_secs(5);

        let shard_ids: Vec<usize> = match shard_filter {
            Some(id) => vec![id],
            None => (0..self.shard_senders.len()).collect(),
        };

        for shard_id in shard_ids {
            let (response_tx, response_rx) = oneshot::channel();

            let send_result = self.shard_senders[shard_id]
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
    }

    /// Format VLL queue info as a response.
    pub(crate) fn format_vll_response(
        &self,
        infos: Vec<frogdb_core::shard::VllQueueInfo>,
    ) -> Response {
        // Check if all queues are empty
        let all_empty = infos.iter().all(|i| {
            i.queue_depth == 0 && i.continuation_lock.is_none() && i.intent_table.is_empty()
        });

        if all_empty {
            return Response::Bulk(Some(Bytes::from("# VLL queues are empty")));
        }

        let mut lines = Vec::new();

        for info in infos {
            // Shard header
            let mut header = format!("shard:{} queue_depth:{}", info.shard_id, info.queue_depth);
            if let Some(txid) = info.executing_txid {
                header.push_str(&format!(" executing_txid:{}", txid));
            }
            lines.push(header);

            // Continuation lock
            if let Some(ref lock) = info.continuation_lock {
                lines.push(format!(
                    "continuation_lock: txid:{} conn_id:{} age_ms:{}",
                    lock.txid, lock.conn_id, lock.age_ms
                ));
            }

            // Pending operations
            if !info.pending_ops.is_empty() {
                lines.push("pending:".to_string());
                for op in &info.pending_ops {
                    lines.push(format!(
                        "  txid:{} operation:{} keys:{} state:{} age_ms:{}",
                        op.txid, op.operation, op.key_count, op.state, op.age_ms
                    ));
                }
            }

            // Intent table
            if !info.intent_table.is_empty() {
                lines.push("intents:".to_string());
                for intent in &info.intent_table {
                    let txids_str: Vec<String> =
                        intent.txids.iter().map(|t| t.to_string()).collect();
                    lines.push(format!(
                        "  key:{} txids:[{}] lock:{}",
                        intent.key,
                        txids_str.join(","),
                        intent.lock_state
                    ));
                }
            }

            // Empty line between shards
            lines.push(String::new());
        }

        // Remove trailing empty line
        if lines.last().map(|s| s.is_empty()).unwrap_or(false) {
            lines.pop();
        }

        Response::Bulk(Some(Bytes::from(lines.join("\n"))))
    }

    /// Handle DEBUG PUBSUB LIMITS command.
    ///
    /// Reports per-connection and per-shard subscription usage against configured maximums.
    pub(crate) async fn handle_debug_pubsub_limits(&self) -> Response {
        use frogdb_core::pubsub::{
            MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
            MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD, MAX_UNIQUE_CHANNELS_PER_SHARD,
            MAX_UNIQUE_PATTERNS_PER_SHARD,
        };
        use tokio::sync::oneshot;

        // Connection-level counts
        let conn_subscriptions = self.state.pubsub.subscriptions.len();
        let conn_patterns = self.state.pubsub.patterns.len();

        // Shard-level counts from shard 0 (broadcast pub/sub coordinator)
        let (response_tx, response_rx) = oneshot::channel();
        let send_result = self.shard_senders[0]
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
    }

    /// Handle DEBUG BUNDLE GENERATE [DURATION <seconds>] command.
    ///
    /// Generates a diagnostic bundle and returns the bundle ID.
    /// The bundle can be downloaded via HTTP: GET /debug/api/bundle/<id>
    pub(crate) async fn handle_debug_bundle_generate(&self, args: &[Bytes]) -> Response {
        // args[0] = "BUNDLE", args[1] = "GENERATE", args[2..] = optional DURATION <seconds>
        let mut duration_secs: u64 = 0;

        // Parse optional DURATION argument
        let mut i = 2;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"DURATION") {
                if i + 1 >= args.len() {
                    return Response::error("ERR DURATION requires a value in seconds");
                }
                match std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(d) => duration_secs = d,
                    None => return Response::error("ERR DURATION must be a positive integer"),
                }
                i += 2;
            } else {
                return Response::error(format!(
                    "ERR Unknown argument '{}' for DEBUG BUNDLE GENERATE",
                    String::from_utf8_lossy(&args[i])
                ));
            }
        }

        // Create bundle config and collector
        let config = frogdb_debug::BundleConfig::default();
        let collector = frogdb_debug::DiagnosticCollector::new(
            self.shard_senders.clone(),
            self.shared_tracer.clone(),
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
    }

    /// Handle DEBUG BUNDLE LIST command.
    ///
    /// Lists all available bundles with their ID, timestamp, and size.
    pub(crate) fn handle_debug_bundle_list(&self) -> Response {
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
}
