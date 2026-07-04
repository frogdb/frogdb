//! INFO command handler.
//!
//! Gathers every INFO source exactly once — a single combined
//! [`frogdb_core::ShardMessage::InfoSnapshot`] fleet scatter plus
//! connection-level snapshots (client registry, metrics recorder, replication
//! state, ACL rate limits, persistence coordinator) — and asks the
//! [`crate::info::InfoBuilder`] to render the requested sections. Every
//! section owns its data and its format; nothing is patched after the fact.

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::info::{
    BaselineSnapshot, ClientsSnapshot, InfoBuilder, InfoSources, LatencySnapshot,
    MemoryConfigSnapshot, PersistenceSnapshot, PrimarySnapshot, RateLimitSnapshot, ReplicaLine,
    ReplicationSnapshot, SectionSelector, gather_shard_snapshot,
};

impl ConnectionHandler {
    /// Handle INFO: gather sources once, render the requested sections.
    pub(crate) async fn handle_info(&self, args: &[Bytes]) -> Response {
        let selector = SectionSelector::from_args(args);
        let sources = match self.gather_info_sources().await {
            Ok(sources) => sources,
            Err(error) => return error,
        };
        Response::bulk(Bytes::from(
            InfoBuilder::standard().render(&selector, &sources),
        ))
    }

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
        // and fall back to the node id.
        let replication_id = match &self.cluster.replication_state {
            Some(state) => {
                let id = state.read().await.replication_id.clone();
                (!id.is_empty()).then_some(id)
            }
            None => None,
        };
        let primary = self
            .cluster
            .replication_tracker
            .as_ref()
            .map(|tracker| PrimarySnapshot {
                replicas: tracker
                    .get_streaming_replicas()
                    .iter()
                    .map(|replica| ReplicaLine {
                        ip: replica.address.ip().to_string(),
                        port: replica.listening_port,
                        offset: replica.acked_offset,
                    })
                    .collect(),
                repl_offset: tracker.current_offset(),
            });
        let replication = ReplicationSnapshot {
            is_replica: self.is_replica.load(std::sync::atomic::Ordering::Relaxed),
            node_id: self.cluster.node_id,
            replication_id,
            primary,
            master_host: shards.master_host.clone(),
            master_port: shards.master_port,
        };

        // Same source LASTSAVE reports, so the two commands agree.
        let last_save_unix = self
            .admin
            .snapshot_coordinator
            .last_save_time()
            .map(|instant| {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                now.as_secs().saturating_sub(instant.elapsed().as_secs())
            });
        let persistence = PersistenceSnapshot {
            durability_mode: config.durability_mode(),
            bgsave_in_progress: self.admin.snapshot_coordinator.in_progress(),
            last_save_unix,
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
        })
    }
}
