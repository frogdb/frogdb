//! Shard worker creation and spawning loop.

use frogdb_core::persistence::{RocksStore, SnapshotCoordinator, WalConfig};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState, CommandRegistry,
    EvictionConfig, ExpiryIndex, HashMapStore, IndexLifecycleManager, MetricsRecorder,
    RecoveryOutcome, ReplicationTrackerImpl, ShardReceiver, ShardSender, ShardWorker,
    SharedBroadcaster,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::Config;
use crate::failure_detector::FailureDetector;
use crate::net::spawn;
use crate::runtime_config::ConfigManager;

/// Context for spawning shard workers.
pub(super) struct ShardSpawnContext {
    pub config: Config,
    pub num_shards: usize,
    pub shard_receivers: Vec<ShardReceiver>,
    pub new_conn_receivers: Vec<mpsc::Receiver<frogdb_core::shard::NewConnection>>,
    pub shard_senders: Arc<Vec<ShardSender>>,
    pub registry: Arc<CommandRegistry>,
    pub rocks_store: Option<Arc<RocksStore>>,
    pub recovered_stores: Vec<(HashMapStore, ExpiryIndex)>,
    pub wal_config: WalConfig,
    pub eviction_config: EvictionConfig,
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    pub metrics_recorder: Arc<dyn MetricsRecorder>,
    pub keyspace_stats: Arc<frogdb_core::KeyspaceStats>,
    pub slowlog_next_id: Arc<AtomicU64>,
    pub function_registry: frogdb_core::SharedFunctionRegistry,
    pub replication_broadcaster: SharedBroadcaster,
    pub replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    pub raft: Option<Arc<ClusterRaft>>,
    pub cluster_state: Option<Arc<ClusterState>>,
    pub node_id: Option<u64>,
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,
    pub failure_detector: Option<Arc<FailureDetector>>,
    pub replication_quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>>,
    pub is_replica_flag: Arc<std::sync::atomic::AtomicBool>,
    pub client_registry: Arc<ClientRegistry>,
    pub config_manager: Arc<ConfigManager>,
    pub shard_memory_used: Arc<Vec<AtomicU64>>,
    pub shard_monitor: tokio_metrics::TaskMonitor,
}

/// Spawn all shard workers and return their join handles.
///
/// Fails if the number of recovered per-shard stores does not match the
/// configured shard count. Such a mismatch means the data directory was written
/// with a different shard count than the server is now configured for; starting
/// anyway would silently drop or misroute recovered data, so recovery is aborted
/// loudly instead (see also the earlier guard in `RocksStore::open`).
pub(super) fn spawn_shard_workers(
    ctx: ShardSpawnContext,
) -> anyhow::Result<Vec<crate::net::JoinHandle<()>>> {
    if ctx.recovered_stores.len() != ctx.num_shards {
        anyhow::bail!(
            "recovered {} shard store(s) but the server is configured for {} shard(s); data \
             directory {} was written with a different shard count — refusing to start to avoid \
             silently dropping recovered data",
            ctx.recovered_stores.len(),
            ctx.num_shards,
            ctx.config.persistence.data_dir.display(),
        );
    }

    let mut shard_handles = Vec::with_capacity(ctx.num_shards);
    let mut recovered_iter = ctx.recovered_stores.into_iter();

    for (shard_id, (msg_rx, conn_rx)) in ctx
        .shard_receivers
        .into_iter()
        .zip(ctx.new_conn_receivers.into_iter())
        .enumerate()
    {
        // Length validated above to equal `num_shards`, and the spawn loop runs
        // exactly `num_shards` times, so this iterator always yields here.
        let (store, _expiry_index) = recovered_iter
            .next()
            .expect("recovered_stores length validated to equal num_shards");

        // Fake WAL mode (simulation tests, `turmoil` feature): enabled but
        // RocksDB-less. Recovery leaves `rocks_store = None`; select the
        // deterministic fake sink instead of the no-WAL eviction path.
        #[cfg(feature = "turmoil")]
        let fake_wal = ctx.config.persistence.enabled
            && ctx.config.persistence.mode.eq_ignore_ascii_case("fake");
        #[cfg(not(feature = "turmoil"))]
        let fake_wal = false;

        let mut worker = if fake_wal {
            #[cfg(feature = "turmoil")]
            {
                ShardWorker::with_fake_persistence(
                    shard_id,
                    ctx.num_shards,
                    store,
                    msg_rx,
                    conn_rx,
                    ctx.shard_senders.clone(),
                    ctx.registry.clone(),
                    ctx.eviction_config.clone(),
                    ctx.metrics_recorder.clone(),
                    ctx.slowlog_next_id.clone(),
                    ctx.replication_broadcaster.clone(),
                )
            }
            #[cfg(not(feature = "turmoil"))]
            {
                unreachable!("fake WAL mode requires the turmoil feature")
            }
        } else if let Some(ref rocks) = ctx.rocks_store {
            ShardWorker::with_persistence(
                shard_id,
                ctx.num_shards,
                store,
                msg_rx,
                conn_rx,
                ctx.shard_senders.clone(),
                ctx.registry.clone(),
                rocks.clone(),
                ctx.wal_config.clone(),
                ctx.snapshot_coordinator.clone(),
                ctx.eviction_config.clone(),
                ctx.metrics_recorder.clone(),
                ctx.slowlog_next_id.clone(),
                ctx.replication_broadcaster.clone(),
            )
        } else {
            ShardWorker::with_eviction(
                shard_id,
                ctx.num_shards,
                msg_rx,
                conn_rx,
                ctx.shard_senders.clone(),
                ctx.registry.clone(),
                ctx.eviction_config.clone(),
                ctx.metrics_recorder.clone(),
                ctx.slowlog_next_id.clone(),
                ctx.replication_broadcaster.clone(),
            )
        };

        // Set function registry on each shard
        worker.set_function_registry(ctx.function_registry.clone());

        // Share the process-wide keyspace hit/miss accumulator
        worker.set_keyspace_stats(ctx.keyspace_stats.clone());

        // Wire warm store for tiered storage
        if ctx.config.tiered_storage.enabled
            && let Some(ref rocks) = ctx.rocks_store
        {
            worker.store.set_warm_store(rocks.clone(), shard_id);
        }

        // Set cluster-related fields if cluster mode is enabled
        if let Some(ref raft_instance) = ctx.raft {
            worker.set_raft(raft_instance.clone());
        }
        if let Some(ref state) = ctx.cluster_state {
            worker.set_cluster_state(state.clone());
        }
        if let Some(id) = ctx.node_id {
            worker.set_node_id(id);
        }
        if let Some(ref factory) = ctx.network_factory {
            worker.set_network_factory(factory.clone());
        }
        if let Some(ref detector) = ctx.failure_detector {
            worker.set_quorum_checker(detector.clone());
        } else if let Some(ref rqc) = ctx.replication_quorum_checker {
            worker.set_quorum_checker(rqc.clone());
        }

        // Set blocking command limits from config
        worker.set_wait_queue_limits(
            ctx.config.blocking.max_waiters_per_key,
            ctx.config.blocking.max_blocked_connections,
        );

        // Share the server-wide is_replica flag with this shard worker
        worker.set_is_replica_flag(ctx.is_replica_flag.clone());

        // Share the expiry_paused flag so PAUSE ALL suppresses active expiry
        worker.set_expiry_paused_flag(ctx.client_registry.expiry_paused_flag());

        // Share replication tracker with shard workers for INFO replication
        if let Some(ref tracker) = ctx.replication_tracker {
            worker.set_replication_tracker(tracker.clone());
        }

        // Set master address on replica shard workers for INFO replication
        if ctx.config.replication.is_replica() {
            worker.set_master_address(
                ctx.config.replication.primary_host.clone(),
                ctx.config.replication.primary_port,
            );
        }

        // Share the per-request spans toggle with shard workers
        worker.set_per_request_spans(ctx.config_manager.per_request_spans_flag());

        // Share the WAL failure policy toggle with shard workers
        worker.set_wal_failure_policy_flag(ctx.config_manager.wal_failure_policy_flag());

        // Share the keyspace notification event flags with shard workers
        worker.set_notify_keyspace_events(ctx.config_manager.notify_keyspace_events_flags());

        // Share per-shard memory usage vec for fragmentation ratio
        worker.set_shard_memory_used(ctx.shard_memory_used.clone());

        // Set scripting config with shared lua-time-limit override
        {
            use frogdb_core::ScriptingConfig;
            worker.set_scripting_config(ScriptingConfig {
                lua_time_limit_override: Some(ctx.config_manager.lua_time_limit()),
                ..Default::default()
            });
        }

        // Always set data directory (needed for search indexes even without persistence)
        worker.set_data_dir(ctx.config.persistence.data_dir.clone());

        // Recover search indexes from RocksDB through the lifecycle seam.
        //
        // This recovery step deliberately lives outside the recovery
        // orchestrator (`crate::recovery`): `IndexLifecycleManager::recover`
        // opens per-shard tantivy + usearch handles that are not `Send`, so it
        // runs here at worker-spawn time and the manager is installed into the
        // worker it was built for (proposal 06 "Search-index recovery
        // placement"; proposal 15 gives the site a real home). A CF-level read
        // failure is fatal; a per-index failure is quarantined and surfaced.
        if ctx.config.persistence.enabled
            && let Some(rocks) = ctx.rocks_store.clone()
        {
            let data_dir = ctx.config.persistence.data_dir.clone();
            let result =
                IndexLifecycleManager::recover(rocks, data_dir, shard_id).map_err(|e| {
                    anyhow::anyhow!("search index recovery failed (shard {shard_id}): {e}")
                })?;

            let mut recovered = 0usize;
            for (name, outcome) in &result.outcomes {
                match outcome {
                    RecoveryOutcome::Recovered { .. } => recovered += 1,
                    RecoveryOutcome::Corrupt(e) => warn!(
                        shard_id,
                        index = %name,
                        error = %e,
                        "search index quarantined (metadata kept, index unavailable)"
                    ),
                    RecoveryOutcome::Undeserializable(e) => warn!(
                        shard_id,
                        index = %name,
                        error = %e,
                        "search index metadata undeserializable (quarantined)"
                    ),
                }
            }
            if recovered > 0 {
                info!(shard_id, count = recovered, "Search indexes recovered");
            }
            worker.install_search_manager(result.manager);
        }

        let monitor = ctx.shard_monitor.clone();
        let handle = spawn(monitor.instrument(async move {
            worker.run().await;
        }));

        shard_handles.push(handle);
    }

    Ok(shard_handles)
}
