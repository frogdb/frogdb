use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::Response;
use tracing::Instrument;

use crate::store::Store;

use super::message::ShardMessage;
use super::types::PartialResult;
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

        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages
                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        ShardMessage::Execute { command, conn_id, txid: _, protocol_version, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                let _ = response_tx.send(err);
                                continue;
                            }
                            let shard_id = self.shard_id();
                            let response = if self.per_request_spans.load(std::sync::atomic::Ordering::Relaxed) {
                                self.execute_command(command.as_ref(), conn_id, protocol_version)
                                    .instrument(tracing::info_span!("shard_execute", shard_id))
                                    .await
                            } else {
                                self.execute_command(command.as_ref(), conn_id, protocol_version)
                                    .await
                            };
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScatterRequest { request_id: _, keys, operation, conn_id, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                // Return a PartialResult with the error for each key
                                let error_results: Vec<(Bytes, Response)> = keys.iter()
                                    .map(|k| (k.clone(), err.clone()))
                                    .collect();
                                let _ = response_tx.send(PartialResult { results: error_results });
                                continue;
                            }
                            let result = self.execute_scatter_part(&keys, &operation).await;
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::GetVersion { response_tx } => {
                            let _ = response_tx.send(self.shard_version);
                        }
                        ShardMessage::ExecTransaction { commands, watches, conn_id, protocol_version, response_tx } => {
                            let result = if self.per_request_spans.load(std::sync::atomic::Ordering::Relaxed) {
                                let shard_id = self.shard_id();
                                self.execute_transaction(commands, &watches, conn_id, protocol_version)
                                    .instrument(tracing::info_span!("shard_exec_txn", shard_id))
                                    .await
                            } else {
                                self.execute_transaction(commands, &watches, conn_id, protocol_version).await
                            };
                            let _ = response_tx.send(result);
                        }

                        // Pub/Sub message handlers
                        ShardMessage::Subscribe { channels, conn_id, sender, response_tx } => {
                            let counts = self.handle_subscribe(channels, conn_id, sender);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::Unsubscribe { channels, conn_id, response_tx } => {
                            let counts = self.handle_unsubscribe(channels, conn_id);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::PSubscribe { patterns, conn_id, sender, response_tx } => {
                            let counts = self.handle_psubscribe(patterns, conn_id, sender);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::PUnsubscribe { patterns, conn_id, response_tx } => {
                            let counts = self.handle_punsubscribe(patterns, conn_id);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::Publish { channel, message, response_tx } => {
                            let count = self.subscriptions.publish(&channel, &message);
                            let _ = response_tx.send(count);
                        }
                        ShardMessage::ShardedSubscribe { channels, conn_id, sender, response_tx } => {
                            let counts = self.handle_ssubscribe(channels, conn_id, sender);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::ShardedUnsubscribe { channels, conn_id, response_tx } => {
                            let counts = self.handle_sunsubscribe(channels, conn_id);
                            let _ = response_tx.send(counts);
                        }
                        ShardMessage::ShardedPublish { channel, message, response_tx } => {
                            let count = self.subscriptions.spublish(&channel, &message);
                            let _ = response_tx.send(count);
                        }
                        ShardMessage::PubSubIntrospection { request, response_tx } => {
                            let response = self.handle_introspection(request);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ConnectionClosed { conn_id } => {
                            self.subscriptions.remove_connection(conn_id);
                            self.subscriptions.reset_thresholds_if_needed();
                        }

                        // Scripting message handlers
                        ShardMessage::EvalScript { script_source, keys, argv, conn_id, protocol_version, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                let _ = response_tx.send(err);
                                continue;
                            }
                            let response = self.handle_eval_script(&script_source, &keys, &argv, conn_id, protocol_version);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::EvalScriptSha { script_sha, keys, argv, conn_id, protocol_version, response_tx } => {
                            // Check if this connection can execute during continuation lock
                            if let Err(err) = self.can_execute_during_lock(conn_id) {
                                let _ = response_tx.send(err);
                                continue;
                            }
                            let response = self.handle_evalsha(&script_sha, &keys, &argv, conn_id, protocol_version);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScriptLoad { script_source, response_tx } => {
                            let sha = self.handle_script_load(&script_source);
                            let _ = response_tx.send(sha);
                        }
                        ShardMessage::ScriptExists { shas, response_tx } => {
                            let results = self.handle_script_exists(&shas);
                            let _ = response_tx.send(results);
                        }
                        ShardMessage::ScriptFlush { response_tx } => {
                            self.handle_script_flush();
                            let _ = response_tx.send(());
                        }
                        ShardMessage::ScriptKill { response_tx } => {
                            let result = self.handle_script_kill();
                            let _ = response_tx.send(result);
                        }

                        // Function message handlers
                        ShardMessage::FunctionCall { function_name, keys, argv, conn_id, protocol_version, read_only, response_tx } => {
                            let response = self.handle_function_call(&function_name, &keys, &argv, conn_id, protocol_version, read_only);
                            let _ = response_tx.send(response);
                        }

                        // Blocking commands handlers
                        ShardMessage::BlockWait { conn_id, keys, op, response_tx, deadline } => {
                            self.handle_block_wait(conn_id, keys, op, response_tx, deadline);
                        }
                        ShardMessage::UnregisterWait { conn_id } => {
                            self.handle_unregister_wait(conn_id);
                        }

                        // Slowlog handlers
                        ShardMessage::SlowlogGet { count, response_tx } => {
                            let entries = self.observability.slowlog.get(count);
                            let _ = response_tx.send(entries);
                        }
                        ShardMessage::SlowlogLen { response_tx } => {
                            let _ = response_tx.send(self.observability.slowlog.len());
                        }
                        ShardMessage::SlowlogReset { response_tx } => {
                            self.observability.slowlog.reset();
                            let _ = response_tx.send(());
                        }
                        ShardMessage::SlowlogAdd { duration_us, command, client_addr, client_name, max_len } => {
                            // Keep shard slowlog max_len in sync with runtime config
                            self.observability.slowlog.set_max_len(max_len);
                            self.observability.slowlog.add_pre_truncated(duration_us, command, client_addr, client_name);
                        }

                        // Memory handlers
                        ShardMessage::MemoryUsage { key, samples: _, response_tx } => {
                            let usage = self.calculate_key_memory_usage(&key);
                            let _ = response_tx.send(usage);
                        }
                        ShardMessage::MemoryStats { response_tx } => {
                            let stats = self.collect_memory_stats();
                            let _ = response_tx.send(stats);
                        }
                        ShardMessage::WalLagStats { response_tx } => {
                            let stats = self.collect_wal_lag_stats();
                            let _ = response_tx.send(stats);
                        }
                        ShardMessage::ScanBigKeys { threshold_bytes, max_keys, response_tx } => {
                            let result = self.scan_big_keys(threshold_bytes, max_keys);
                            let _ = response_tx.send(result);
                        }

                        // Latency handlers
                        ShardMessage::LatencyLatest { response_tx } => {
                            let latest = self.observability.latency_monitor.latest();
                            let _ = response_tx.send(latest);
                        }
                        ShardMessage::LatencyHistory { event, response_tx } => {
                            let history = self.observability.latency_monitor.history(event);
                            let _ = response_tx.send(history);
                        }
                        ShardMessage::LatencyReset { events, response_tx } => {
                            self.observability.latency_monitor.reset(&events);
                            let _ = response_tx.send(());
                        }

                        ShardMessage::ResetStats { response_tx } => {
                            // Reset latency monitor (all events)
                            self.observability.latency_monitor.reset(&[]);
                            // Reset slowlog
                            self.observability.slowlog.reset();
                            // Reset peak memory
                            self.observability.peak_memory = 0;
                            let _ = response_tx.send(());
                        }

                        ShardMessage::HotShardStats { period_secs, response_tx } => {
                            let stats = self.calculate_hot_shard_stats(period_secs);
                            let _ = response_tx.send(stats);
                        }

                        ShardMessage::UpdateConfig { eviction_config, response_tx } => {
                            if let Some(config) = eviction_config {
                                self.eviction.config = config;
                                // Recalculate per-shard memory limit
                                self.eviction.memory_limit = if self.eviction.config.maxmemory > 0 {
                                    self.eviction.config.maxmemory / self.num_shards() as u64
                                } else {
                                    0
                                };
                                tracing::info!(
                                    shard_id = self.shard_id(),
                                    "Shard config updated"
                                );
                            }
                            let _ = response_tx.send(());
                        }

                        // VLL message handlers
                        ShardMessage::VllLockRequest { txid, keys, mode, operation, ready_tx, execute_rx } => {
                            self.handle_vll_lock_request(txid, keys, mode, operation, ready_tx, execute_rx).await;
                        }
                        ShardMessage::VllExecute { txid, response_tx } => {
                            self.handle_vll_execute(txid, response_tx).await;
                        }
                        ShardMessage::VllAbort { txid } => {
                            self.handle_vll_abort(txid);
                        }
                        ShardMessage::VllContinuationLock { txid, conn_id, ready_tx, release_rx } => {
                            self.handle_vll_continuation_lock(txid, conn_id, ready_tx, release_rx).await;
                        }

                        // Cluster / Raft message handlers
                        ShardMessage::RaftCommand { cmd, response_tx } => {
                            let result = if let Some(ref raft) = self.cluster.raft {
                                raft.client_write(cmd).await
                                    .map(|_| ())
                                    .map_err(|e| e.to_string())
                            } else {
                                Err("Raft not initialized".to_string())
                            };
                            let _ = response_tx.send(result);
                        }

                        ShardMessage::GetVllQueueInfo { response_tx } => {
                            let info = self.collect_vll_queue_info();
                            let _ = response_tx.send(info);
                        }

                        ShardMessage::GetPubSubLimitsInfo { response_tx } => {
                            let info = super::types::PubSubLimitsInfo {
                                total_subscriptions: self.subscriptions.total_subscription_count(),
                                unique_channels: self.subscriptions.unique_channel_count(),
                                unique_patterns: self.subscriptions.unique_pattern_count(),
                            };
                            let _ = response_tx.send(info);
                        }

                        ShardMessage::Shutdown => {
                            tracing::info!(shard_id = self.shard_id(), "Shard worker shutting down");
                            // Flush WAL before shutdown
                            if let Some(ref wal) = self.persistence.wal_writer
                                && let Err(e) = wal.flush_async().await {
                                    tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL on shutdown");
                                }
                            break;
                        }
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
                tracing::trace!(
                    shard_id = self.shard_id(),
                    key = %String::from_utf8_lossy(&key),
                    "Active expiry deleted key"
                );
            }
        }

        // Record expired keys metric and increment version
        if deleted_count > 0 {
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keys_expired_total",
                deleted_count,
                &[("shard", &shard_label)],
            );
            self.increment_version();
        }
    }

    /// Collect and emit shard metrics periodically.
    fn collect_shard_metrics(&mut self) {
        let shard_label = self.shard_id().to_string();
        let memory_used = self.store.memory_used() as u64;

        // Update peak memory if current exceeds it
        if memory_used > self.observability.peak_memory {
            self.observability.peak_memory = memory_used;
        }

        // Memory used by this shard
        self.observability.metrics_recorder.record_gauge(
            "frogdb_shard_memory_bytes",
            memory_used as f64,
            &[("shard", &shard_label)],
        );

        // Per-shard memory metrics
        self.observability.metrics_recorder.record_gauge(
            "frogdb_memory_used_bytes",
            memory_used as f64,
            &[("shard", &shard_label)],
        );

        // Peak memory for this shard
        self.observability.metrics_recorder.record_gauge(
            "frogdb_memory_peak_bytes",
            self.observability.peak_memory as f64,
            &[("shard", &shard_label)],
        );

        // Keyspace metrics: key count
        let key_count = self.store.len() as f64;

        self.observability.metrics_recorder.record_gauge(
            "frogdb_shard_keys",
            key_count,
            &[("shard", &shard_label)],
        );

        self.observability.metrics_recorder.record_gauge(
            "frogdb_keys_total",
            key_count,
            &[("shard", &shard_label)],
        );

        // Keys with expiry (using cleaner abstraction)
        self.observability.metrics_recorder.record_gauge(
            "frogdb_keys_with_expiry",
            self.store.keys_with_expiry_count() as f64,
            &[("shard", &shard_label)],
        );
    }
}
