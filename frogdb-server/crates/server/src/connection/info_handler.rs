//! INFO command handler.
//!
//! Gathers every INFO source exactly once — a single combined
//! [`frogdb_core::ObservabilityMsg::InfoSnapshot`] fleet scatter plus
//! connection-level snapshots (client registry, metrics recorder, replication
//! state, ACL rate limits, persistence coordinator) — and asks the
//! [`crate::info::InfoBuilder`] to render the requested sections. Every
//! section owns its data and its format; nothing is patched after the fact.

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::info::{
    BaselineSnapshot, ClientsSnapshot, InfoBuilder, InfoSources, LatencySnapshot,
    MemoryConfigSnapshot, PersistenceSnapshot, PrimarySnapshot, RateLimitSnapshot,
    ReplicationSnapshot, SectionSelector, gather_shard_snapshot,
};

/// INFO is dispatched through the [`frogdb_core::ConnectionCommand`] seam (see
/// [`crate::connection::info_conn_command`]): its executor reads only
/// [`frogdb_core::ConnCtx::info`] and delegates here. `render` gathers every
/// INFO source once and renders the requested sections — the same logic the
/// legacy `handle_info` ran, so the wire output is byte-for-byte unchanged.
impl frogdb_core::InfoProvider for ConnectionHandler {
    /// Handle INFO: gather sources once, render the requested sections.
    fn render<'a>(&'a self, args: &'a [Bytes]) -> frogdb_core::BoxFuture<'a, Response> {
        Box::pin(async move {
            let selector = SectionSelector::from_args(args);
            let sources = match self.gather_info_sources().await {
                Ok(sources) => sources,
                Err(error) => return error,
            };
            Response::bulk(Bytes::from(
                InfoBuilder::standard().render(&selector, &sources),
            ))
        })
    }
}

impl ConnectionHandler {
    /// Materialize every INFO source. The only shard messaging is the single
    /// [`gather_shard_snapshot`] fleet scatter.
    async fn gather_info_sources(&self) -> Result<InfoSources, Response> {
        let shards = gather_shard_snapshot(
            self.core.shard_senders.as_slice(),
            self.scatter_gather_timeout,
            self.state.id,
        )
        .await?;

        let registry = &self.admin.client_registry;
        let config = &self.admin.config_manager;

        let clients = ClientsSnapshot {
            connected: registry.client_count(),
            blocked: registry.blocked_client_count(),
            max_clients: config.max_clients(),
        };

        let total_error_replies = registry
            .error_stats
            .total_error_replies
            .load(std::sync::atomic::Ordering::Relaxed);

        // Combine global command stats with this connection's pending local
        // stats so the currently-executing INFO and any recent commands appear
        // immediately without waiting for the periodic sync threshold.
        let mut stats_map = registry.command_stats_snapshot();
        for (cmd, usec) in &self.state.local_stats.command_latencies {
            let entry = stats_map.entry(cmd.to_ascii_lowercase()).or_default();
            entry.calls += 1;
            entry.usec += usec;
        }
        let mut command_stats: Vec<_> = stats_map.into_iter().collect();
        command_stats.sort_by(|a, b| a.0.cmp(&b.0));

        let mut error_types: Vec<(String, u64)> = registry
            .error_stats
            .error_type_snapshot()
            .into_iter()
            .collect();
        error_types.sort_by(|a, b| a.0.cmp(&b.0));

        let rl_registry = self.core.acl_manager.rate_limit_registry();
        let rate_limit = RateLimitSnapshot {
            users: rl_registry.user_count(),
            commands_rejected: rl_registry.total_commands_rejected(),
            bytes_rejected: rl_registry.total_bytes_rejected(),
        };

        // The real replication id exchanged in PSYNC/FULLRESYNC lives in the
        // role's ReplicationState; standalone and pure cluster mode have none
        // and fall back to the node id. The same guard yields the failover
        // window (previous id + boundary) that INFO renders as
        // master_replid2/second_repl_offset.
        let (replication_id, secondary_window) = match &self.cluster.replication_state {
            Some(state) => {
                let guard = state.read();
                let id = guard.replication_id.clone();
                let replication_id = (!id.is_empty()).then_some(id);
                // A window exists only once new_replication_id() has frozen the
                // previous id together with an offset boundary. The -1 sentinel
                // (no failover yet) leaves it None so INFO reports the all-zero
                // replid2 / second_repl_offset:-1 pair.
                let secondary_window = match (&guard.secondary_id, guard.secondary_offset) {
                    (Some(prev), boundary) if boundary >= 0 => Some((prev.clone(), boundary)),
                    _ => None,
                };
                (replication_id, secondary_window)
            }
            None => (None, None),
        };
        let is_replica = self.is_replica.load(std::sync::atomic::Ordering::Acquire);
        // `replication_tracker` is wired at boot for any node that *can* track
        // downstream replicas and outlives a runtime Role Demotion (the
        // `RoleManager` only owns the read-only flag / primary target / inbound
        // stream, not this tracker) — so its mere presence does not mean this
        // node is currently a primary. Gate on the live role flag too, or a
        // demoted node keeps rendering the `master` branch (and never surfaces
        // `master_host`/`master_port`) even after `REPLICAOF host port`.
        let primary = self
            .cluster
            .replication_tracker
            .as_ref()
            .filter(|_| !is_replica)
            .map(|tracker| PrimarySnapshot {
                // Every registered replica in a phase Redis names, not only the
                // streaming ones: a replica being fed its checkpoint used to be
                // absent from both the lines and the count
                // (FM-REPLICATION-060).
                replicas: crate::info::rendered_replicas(tracker),
            });
        // One offset counter per node, whatever role is running: the tracker's
        // atomic is the node's replication identity offset, advanced by the
        // primary stream when this node stamps writes and by the replica
        // ingest loop when it applies them.
        let repl_offset = self
            .cluster
            .replication_tracker
            .as_ref()
            .map_or(0, |tracker| tracker.current_offset());
        // Reported whatever the current role is: a demoted node's lifetime
        // resync tally is exactly what an operator diagnosing the demotion
        // wants, and zeroing it on demotion would be a lie.
        let sync = self
            .cluster
            .replication_tracker
            .as_ref()
            .map_or_else(Default::default, |tracker| tracker.sync_counters());
        // Reported whatever the current role is, for the same reason `sync`
        // above is (hardening issue 29): a demoted node's lifetime transfer
        // tally is still real history worth reporting.
        let net_bytes = self
            .cluster
            .replication_tracker
            .as_ref()
            .map_or_else(Default::default, |tracker| tracker.net_bytes());
        // Reported in every role, from the ring the primary handler published at
        // construction: the capacity is a property of this node's config, and
        // the window is what PSYNC would actually grant a `+CONTINUE` over
        // (FM-REPLICATION-059).
        let backlog = self
            .cluster
            .replication_tracker
            .as_ref()
            .map_or_else(Default::default, |tracker| tracker.backlog_geometry());
        // Reported in every role for the same reason `sync` above is: a node
        // demoted since is still the node whose cuts lost the race, and the
        // tally is what an alert reads (FM-REPLICATION-066).
        let full_sync_hold_breaches = self
            .cluster
            .replication_tracker
            .as_ref()
            .map_or(0, |tracker| tracker.full_sync_hold_breaches());
        let replication = ReplicationSnapshot {
            is_replica,
            node_id: self.cluster.node_id,
            replication_id,
            primary,
            sync,
            net_bytes,
            full_sync_hold_breaches,
            backlog,
            repl_offset,
            master_host: shards.master_host.clone(),
            master_port: shards.master_port,
            master_link_up: shards.master_link_up,
            master_sync_error: shards.master_sync_error.clone(),
            secondary_window,
        };

        // One read of the coordinator's save history: the same value LASTSAVE
        // reports, plus the outcome/counters, so no two fields can describe
        // different moments. Passed through raw — [`crate::info::persistence_snapshot_fields`]
        // is the single place that turns it into `rdb_*` fields, shared with
        // the shard-local INFO renderer (issue 10 / FM-PERSISTENCE-022).
        let persistence = PersistenceSnapshot {
            durability_mode: config.durability_mode(),
            bgsave_in_progress: self.admin.snapshot_coordinator.in_progress(),
            snapshot_stats: self.admin.snapshot_coordinator.stats(),
            // Fixed at boot: recovery finished before any connection existed.
            load_keys_loaded: self.admin.recovery_stats.keys_loaded,
            load_keys_expired: self.admin.recovery_stats.keys_expired_skipped,
            load_keys_failed: self.admin.recovery_stats.keys_failed,
            load_functions_failed: self.admin.recovery_stats.functions_failed,
        };

        // Truthfully empty when no detector is wired (Hotkeys section
        // renders no fields rather than inventing shard-hotness data).
        let hot_shards = match self.observability.collectors.hot_shard_handle() {
            Some(detector) => Some(detector.collect_snapshot(None).await),
            None => None,
        };

        let baseline = crate::latency_test::get_global_baseline().map(|info| BaselineSnapshot {
            duration_secs: info.result.duration_secs,
            samples: info.result.samples,
            min_us: info.result.min_us,
            max_us: info.result.max_us,
            avg_us: info.result.avg_us,
            p99_us: info.result.p99_us,
            warning_threshold_us: info.warning_threshold_us,
        });

        Ok(InfoSources {
            cluster_state: self.cluster.cluster_state.clone(),
            clients,
            metrics: self.observability.metrics_recorder.clone(),
            total_error_replies,
            command_stats,
            error_types,
            latency: LatencySnapshot {
                histograms: self.observability.latency_histograms.clone(),
                percentiles: config.latency_tracking_percentiles(),
            },
            rate_limit,
            replication,
            persistence,
            memory_config: MemoryConfigSnapshot {
                maxmemory: config.maxmemory(),
                policy: config.maxmemory_policy().to_string(),
            },
            baseline,
            key_memory_enabled: config.key_memory_histograms_enabled(),
            shards,
            keyspace_stats: self.observability.keyspace_stats.clone(),
            hot_shards,
        })
    }
}
