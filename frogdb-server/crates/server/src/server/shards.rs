//! Shard worker creation and spawning loop.

use frogdb_core::persistence::{RocksStore, SnapshotCoordinator, WalConfig};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState, CommandRegistry,
    EvictionConfig, ExpiryIndex, HashMapStore, MetricsRecorder, ReplicationTrackerImpl,
    ShardReceiver, ShardSender, ShardWorker, SharedBroadcaster,
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
pub(super) fn spawn_shard_workers(ctx: ShardSpawnContext) -> Vec<crate::net::JoinHandle<()>> {
    let mut shard_handles = Vec::with_capacity(ctx.num_shards);
    let mut recovered_iter = ctx.recovered_stores.into_iter();

    for (shard_id, (msg_rx, conn_rx)) in ctx
        .shard_receivers
        .into_iter()
        .zip(ctx.new_conn_receivers.into_iter())
        .enumerate()
    {
        let (store, _expiry_index) = recovered_iter.next().unwrap_or_default();

        let mut worker = if let Some(ref rocks) = ctx.rocks_store {
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

        // Recover search indexes from RocksDB
        if ctx.config.persistence.enabled
            && let Some(ref rocks) = ctx.rocks_store
        {
            recover_search_indexes(&mut worker, rocks, shard_id, &ctx.config);
        }

        let monitor = ctx.shard_monitor.clone();
        let handle = spawn(monitor.instrument(async move {
            worker.run().await;
        }));

        shard_handles.push(handle);
    }

    shard_handles
}

/// Recover search indexes, aliases, dictionaries, and config from RocksDB.
fn recover_search_indexes(
    worker: &mut ShardWorker,
    rocks: &Arc<RocksStore>,
    shard_id: usize,
    config: &Config,
) {
    let mut indexes = std::collections::HashMap::new();
    let mut aliases = std::collections::HashMap::new();
    let mut dictionaries = std::collections::HashMap::new();
    let mut search_config = std::collections::HashMap::new();

    match rocks.iter_search_meta(shard_id) {
        Ok(iter) => {
            for (key_bytes, value_bytes) in iter {
                let index_name = String::from_utf8_lossy(&key_bytes).to_string();
                // Skip non-index metadata entries
                if index_name.starts_with("__") {
                    continue;
                }
                match serde_json::from_slice::<frogdb_search::SearchIndexDef>(&value_bytes) {
                    Ok(def) => {
                        let search_dir = config
                            .persistence
                            .data_dir
                            .join("search")
                            .join(&index_name)
                            .join(format!("shard_{}", shard_id));
                        match frogdb_search::ShardSearchIndex::open(def, &search_dir) {
                            Ok(idx) => {
                                info!(
                                    shard_id,
                                    index = %index_name,
                                    "Recovered search index"
                                );
                                indexes.insert(index_name, idx);
                            }
                            Err(e) => {
                                warn!(
                                    shard_id,
                                    index = %index_name,
                                    error = %e,
                                    "Failed to recover search index"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            shard_id,
                            index = %index_name,
                            error = %e,
                            "Failed to deserialize search index definition"
                        );
                    }
                }
            }
        }
        Err(e) => {
            warn!(shard_id, error = %e, "Failed to iterate search metadata");
        }
    }

    // Recover aliases, dictionaries, and config from search_meta CF
    if let Ok(Some(alias_json)) = rocks.get_search_meta(shard_id, b"__aliases__")
        && let Ok(a) =
            serde_json::from_slice::<std::collections::HashMap<String, String>>(&alias_json)
    {
        aliases = a;
    }
    if let Ok(Some(config_json)) = rocks.get_search_meta(shard_id, b"__config__")
        && let Ok(cfg) =
            serde_json::from_slice::<std::collections::HashMap<String, String>>(&config_json)
    {
        search_config = cfg;
    }
    // Recover dictionaries (keys prefixed with __dict__:)
    if let Ok(iter) = rocks.iter_search_meta(shard_id) {
        for (key_bytes, value_bytes) in iter {
            if let Ok(key_str) = std::str::from_utf8(&key_bytes)
                && let Some(dict_name) = key_str.strip_prefix("__dict__:")
                && let Ok(terms) = serde_json::from_slice::<Vec<String>>(&value_bytes)
            {
                dictionaries.insert(dict_name.to_string(), terms.into_iter().collect());
            }
        }
    }

    if !indexes.is_empty() {
        info!(shard_id, count = indexes.len(), "Search indexes recovered");
    }

    worker.restore_search_state(indexes, aliases, dictionaries, search_config);
}
