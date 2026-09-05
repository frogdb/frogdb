//! Shard worker creation and spawning loop.

use frogdb_core::persistence::{RecoveryStats, RocksStore, SnapshotCoordinator, WalConfig};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState, CommandRegistry,
    EvictionConfig, ExpiryIndex, HashMapStore, IndexLifecycleManager, MetricsRecorder,
    RecoveryOutcome, ReplicationTrackerImpl, ShardReceiver, ShardSender, ShardWorker,
    SharedBroadcaster,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::cluster::failure_detector::FailureDetector;
use crate::config::{Config, JsonConfigExt};
use crate::net::{ShardHandle, ShardPlacement};
use crate::runtime_config::ConfigManager;
use frogdb_telemetry::ShardArenaRegistry;

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
    /// This node's boot-time recovery outcome, shared read-only with every
    /// shard worker so `redis.call('INFO', 'persistence')` reports real
    /// `rdb_last_load_keys_*` counts instead of static zeros (issue 42).
    pub recovery_stats: Arc<RecoveryStats>,
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
    /// Server-wide role-transition controller (`RoleManager` handle) so
    /// `REPLICAOF` executed on a shard drives Role Promotion/Demotion.
    pub role_controller: Arc<dyn frogdb_core::RoleController>,
    pub client_registry: Arc<ClientRegistry>,
    pub config_manager: Arc<ConfigManager>,
    pub shard_memory_used: Arc<Vec<AtomicU64>>,
    /// Each shard's `TxnBuffering` budget (see `InitResult::txn_budgets`).
    pub txn_budgets: Arc<Vec<frogdb_memory::Budget>>,
    pub shard_monitor: tokio_metrics::TaskMonitor,
}

/// Everything `spawn_shard_workers` hands back: the supervisor's handles and
/// where connections for each shard belong.
pub(super) struct SpawnedShards {
    /// Join handles, each paired with its shard id so the supervisor can
    /// attribute a failure to the dead shard.
    pub handles: Vec<(usize, ShardHandle)>,
    /// Which runtime a connection assigned to each shard must run on — the
    /// connection→core half of PRD R3/R4, without which the thread-per-core
    /// shape is a 3.7× regression rather than a 2.3× win (spike-report §(b)).
    pub placement: ShardPlacement,
    /// Which jemalloc arena each shard bound, and the sampled figures for it.
    /// Empty when no shard has an arena (simulation, or a build without an
    /// arena-capable allocator).
    pub arenas: Arc<ShardArenaRegistry>,
}

/// Spawn all shard workers and return their join handles plus the connection
/// placement they imply.
///
/// Fails if the number of recovered per-shard stores does not match the
/// configured shard count. Such a mismatch means the data directory was written
/// with a different shard count than the server is now configured for; starting
/// anyway would silently drop or misroute recovered data, so recovery is aborted
/// loudly instead (see also the earlier guard in `RocksStore::open`).
pub(super) fn spawn_shard_workers(ctx: ShardSpawnContext) -> anyhow::Result<SpawnedShards> {
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

    // Shard placement goes through the executor seam (ADR-0006 §1): one
    // executor, chosen at compile time by the `turmoil` feature, launches every
    // shard. Today both implementations spawn a task on the ambient runtime;
    // the seam exists so the production shape can become thread-per-core
    // without the simulation following it onto threads it cannot schedule.
    let mut executor = crate::net::shard_executor_with_arenas(crate::net::shard_arena_source());
    info!(executor = executor.kind(), "Shard executor selected");

    // Each shard's broker reads its own arena through this. The slot it points
    // at is filled below, once every shard has launched and its arena is known
    // — see `crate::shard_arena_reading` for why the reading has to be
    // late-bound and why an unfilled slot reads as "no figure" rather than
    // zero.
    let arena_readings = crate::shard_arena_reading::ShardArenaReadings::new();

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

        // Adopt the transaction-buffer budget minted for this core.
        worker.set_txn_buffer_budget(ctx.txn_budgets[shard_id].clone());

        // Share this node's boot-time recovery outcome (issue 42).
        worker.set_recovery_stats(ctx.recovery_stats.clone());

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

        // Set JSON document limits (max depth / max size) from config so JSON
        // command handlers enforce the configured `[json]` limits.
        worker.set_json_limits(ctx.config.json.to_limits());

        // Share the server-wide is_replica flag with this shard worker
        worker.set_is_replica_flag(ctx.is_replica_flag.clone());

        // Share the server-wide role-transition controller so REPLICAOF on this
        // shard can drive Role Promotion/Demotion through the RoleManager.
        worker.set_role_controller(ctx.role_controller.clone());

        // Share the expiry_paused flag so PAUSE ALL suppresses active expiry
        worker.set_expiry_paused_flag(ctx.client_registry.expiry_paused_flag());

        // Share the node-global write-pause gate so a blocking command that
        // finds data already present parks instead of popping inside a
        // `CLIENT PAUSE WRITE` drain window (`specs/blocking.md`
        // TR-BLOCKING-026).
        worker.set_node_write_pause_gate(ctx.client_registry.node_write_pause_gate());

        // Share replication tracker with shard workers for INFO replication
        if let Some(ref tracker) = ctx.replication_tracker {
            worker.set_replication_tracker(tracker.clone());
        }

        // The `master_host`/`master_port` INFO fields are no longer a
        // per-shard copy set once at boot: `ShardIdentity` derives them live
        // from the role controller wired just above, whose `RoleManager` is
        // itself seeded with the `replicaof` boot target (see
        // `cluster_init::init_cluster`). One source, always current — a
        // runtime `REPLICAOF host port` can no longer leave this stale.

        // Share the per-request spans toggle with shard workers
        worker.set_per_request_spans(ctx.config_manager.per_request_spans_flag());

        // Share the hot-shard kill switch with shard workers, so CONFIG SET
        // hotshards-enabled stops the per-command op-rate accounting itself and
        // not merely the report the collector renders.
        worker.set_hotshards_enabled_flag(ctx.config_manager.hotshard_config().enabled_flag());

        // Share the WAL failure policy toggle with shard workers
        worker.set_wal_failure_policy_flag(ctx.config_manager.wal_failure_policy_flag());

        // Share the keyspace notification event flags with shard workers
        worker.set_notify_keyspace_events(ctx.config_manager.notify_keyspace_events_flags());

        // Share per-shard memory usage vec for fragmentation ratio
        worker.set_shard_memory_used(ctx.shard_memory_used.clone());

        // Give this shard's memory broker its own arena figure. Under
        // simulation — and on any build whose allocator has no arenas — the
        // registry published below simply has no entry for it, and the broker
        // keeps reporting no reading, which is what it did before this wiring.
        worker.set_arena_sampler(arena_readings.sampler_for(shard_id));

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
        // orchestrator (`frogdb_recovery`): `IndexLifecycleManager::recover`
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

        // Determinism (audit A51/R6): under the turmoil simulation the shard's
        // two periodic sweeps stop being `select!` timer branches — which race
        // queued commands for branch selection — and become ordinary queued
        // `DriveTick` messages produced by the pump below. The sweeps keep
        // their 100 ms cadence but now take a definite place in the shard's
        // single totally-ordered message queue.
        #[cfg(feature = "turmoil")]
        worker.set_driven_ticks(true);
        #[cfg(feature = "turmoil")]
        spawn_shard_tick_pump(shard_id, ctx.shard_senders.clone());

        let monitor = ctx.shard_monitor.clone();
        // A launch that fails aborts boot. The only failure is an allocator
        // that has arenas and refused this shard one: the shard's broker would
        // then take its `maxmemory` verdicts with nothing measuring the core
        // (`frogdb_net::RealShardExecutor::launch`).
        let handle = executor.launch(
            shard_id,
            Box::pin(monitor.instrument(async move {
                worker.run().await;
            })),
        )?;

        shard_handles.push((shard_id, handle));
    }

    // Asked once, now that every shard has been launched: the acceptor needs a
    // plain answer per shard, not a trait object it would have to consult on
    // every accepted socket.
    let launched = ShardPlacement::collect(&*executor, ctx.num_shards);
    let shards_own_threads = launched.is_pinned();

    // Same shape, same reason, for arenas: ask the executor once, here, and hand
    // the answer on as data. A shard missing from the registry has no arena and
    // is simply unattributable — never zero.
    let arenas = Arc::new(ShardArenaRegistry::new(
        (0..ctx.num_shards).filter_map(|shard_id| Some((shard_id, executor.arena_of(shard_id)?))),
    ));
    report_arena_binding(arenas.as_ref(), ctx.num_shards);

    // Every shard's broker has been holding a reading that points here; this is
    // the one publish that makes those readings live. After it, a broker reads
    // whatever its own arena reported on the sampler's last tick.
    arena_readings.publish(arenas.clone());

    // A shard that has a runtime of its own has a thread of its own. Declare it
    // once, here, where the executor that decided it is still in scope: a
    // synchronous cross-shard wait inside a script has to know which of the two
    // blocking strategies is legal (see `frogdb_core::shard::placement`). This
    // is a property of the *threads*, so it holds whether or not connections are
    // colocated onto them.
    if shards_own_threads {
        frogdb_core::shard::declare_shards_own_threads();
    }

    // Colocation is a deployment decision, not an executor one: it pays when the
    // process owns the machine's cores, and costs when it does not (see
    // `server.colocate-connections`). Dropping the runtimes here is all it takes
    // — the acceptor spawns onto the ambient runtime when it has none.
    let placement = if ctx.config.server.colocate_connections {
        launched
    } else {
        ShardPlacement::unpinned()
    };

    info!(
        executor = executor.kind(),
        shards_own_threads,
        connections_colocated = placement.is_pinned(),
        "Shard connection placement resolved"
    );

    Ok(SpawnedShards {
        handles: shard_handles,
        placement,
        arenas,
    })
}

/// Report what the arena layout actually came out as, once, at startup.
///
/// Two things are worth an operator's attention and neither is an error:
///
/// * A build that binds no arenas at all — the simulation seam, or an allocator
///   with no arenas — has no per-shard memory figures. That is a configuration,
///   so it is an `info` line rather than a warning. The mixed case does not
///   exist: a shard that *could* have had an arena and did not get one failed
///   its launch and boot never reached here
///   (`frogdb_net::RealShardExecutor::launch`).
/// * The intended layout is exactly `1 + num_shards` arenas: one automatic
///   arena for every non-shard thread plus one per shard, which is what
///   `narenas:1` in the allocator configuration buys (see
///   `crate::malloc_conf`). More than that means the setting did not take
///   effect and jemalloc created its own default pool, so the automatic-arena
///   figures are spread across arenas nobody is reading.
fn report_arena_binding(arenas: &ShardArenaRegistry, num_shards: usize) {
    let bound = arenas.len();
    let total = frogdb_telemetry::jemalloc::narenas();

    if bound == 0 {
        info!(
            num_shards,
            "No per-shard arenas bound; shard memory is not separately attributable"
        );
        return;
    }
    match arena_layout(num_shards, total) {
        ArenaLayout::Excess { total, expected } => warn!(
            arenas = total,
            expected,
            malloc_conf = crate::malloc_conf::requested(),
            applied = ?crate::malloc_conf::applied(),
            "More jemalloc arenas exist than shards plus one; the allocator's \
             arena-count setting did not take effect, so non-shard allocations \
             are spread over unattributed arenas"
        ),
        ArenaLayout::AsIntended | ArenaLayout::Unknown => info!(
            bound,
            num_shards,
            arenas = ?total,
            malloc_conf = crate::malloc_conf::requested(),
            "Per-shard arenas bound"
        ),
    }
}

/// What the live arena count says about the intended layout.
#[derive(Debug, PartialEq, Eq)]
enum ArenaLayout {
    /// No more arenas exist than the intended `1 + num_shards`.
    AsIntended,
    /// jemalloc has arenas nobody is reading: `narenas:1` did not take effect,
    /// so non-shard allocations are spread over its automatic pool
    /// (`4 × ncpu` arenas by default) instead of landing in one.
    Excess { total: u32, expected: u32 },
    /// No allocator to ask (`msvc`, or any build without jemalloc).
    Unknown,
}

/// Judge the live arena count against the intended `1 + num_shards`.
///
/// Split out from the logging so the arithmetic has a test: the whole point of
/// `narenas:1` is that this number is exact, and an off-by-one here would let
/// the pool silently come back.
fn arena_layout(num_shards: usize, total: Option<u32>) -> ArenaLayout {
    let expected = num_shards as u32 + 1;
    match total {
        None => ArenaLayout::Unknown,
        Some(total) if total > expected => ArenaLayout::Excess { total, expected },
        Some(_) => ArenaLayout::AsIntended,
    }
}

/// Deterministic replacement for a shard's periodic-sweep timer branches
/// (simulation builds only — determinism audit remediation R6).
///
/// A shard worker spawned with `set_driven_ticks(true)` suppresses the active
/// expiry and blocking-waiter-timeout arms of its `select!`; this task supplies
/// both sweeps at the same 100 ms cadence as queued
/// [`ShardMessage::DriveTick`] messages, which reach the same
/// `drive_expiry_tick` / `drive_waiter_timeout_tick` seams the shard harness
/// uses. Semantics and cadence are unchanged — what changes is that "sweep vs.
/// command" is no longer resolved by `select!` branch choice.
///
/// Under turmoil the interval runs on the host's paused virtual clock, so the
/// tick instants are a function of the simulation alone. The task exits when
/// the shard's receiver is gone (worker shut down), so it never outlives the
/// shard it drives.
#[cfg(feature = "turmoil")]
fn spawn_shard_tick_pump(shard_id: usize, senders: Arc<Vec<ShardSender>>) {
    use crate::net::spawn;
    use frogdb_core::TickKind;
    use std::time::Duration;

    spawn(async move {
        let sender = senders[shard_id].clone();
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        // The sweeps are idempotent catch-up work: a delayed tick must not
        // produce a burst of back-to-back sweeps (the default `Burst`
        // behaviour), which would be indistinguishable from real time passing.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            // Fixed order within a tick: expiry first, then the waiter sweep —
            // matching a real loop's worst-case interleaving, and fixed so the
            // pair is reproducible rather than scheduler-dependent.
            if sender
                .send(frogdb_core::ShardMessage::DriveTick(TickKind::Expiry))
                .await
                .is_err()
            {
                break;
            }
            if sender
                .send(frogdb_core::ShardMessage::DriveTick(
                    TickKind::WaiterTimeout,
                ))
                .await
                .is_err()
            {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{ArenaLayout, arena_layout};

    /// The wiring this issue delivers: which [`crate::net::ShardExecutor`] the
    /// server actually gets.
    ///
    /// This is not a restatement of `frogdb-net`'s own selection test — it is
    /// asserted *here*, from inside `frogdb-server`, because the feature has to
    /// travel: `frogdb-server/turmoil` must forward `frogdb-net/turmoil` or the
    /// simulation would silently run production shard placement. Same failure
    /// mode the `net` module's type-identity assertion guards for TCP types.
    #[test]
    fn selected_shard_executor_matches_the_build() {
        let executor = crate::net::shard_executor();

        #[cfg(feature = "turmoil")]
        assert_eq!(
            executor.kind(),
            "sim",
            "turmoil builds must place shards with the simulation executor"
        );

        #[cfg(not(feature = "turmoil"))]
        assert_eq!(
            executor.kind(),
            "real",
            "production builds must place shards with the real executor"
        );
    }

    /// Arena binding is deliberately not modelled under simulation: the sim
    /// host is one thread hosting every shard, so a reported arena would be a
    /// fiction. Permanent — it must still hold once the real executor binds
    /// real arenas (ADR-0006 §1/§3).
    #[cfg(feature = "turmoil")]
    #[test]
    fn simulation_reports_no_arena_for_any_shard() {
        let executor = crate::net::shard_executor();
        for shard_id in 0..16 {
            assert_eq!(executor.arena_of(shard_id), None);
        }
    }

    /// The intended arena layout is exactly `1 + num_shards`: one automatic
    /// arena for every non-shard thread, plus one per shard. That equality is
    /// the whole return on `narenas:1`, so it is pinned here rather than left
    /// to a log line nobody reads.
    ///
    /// Pinned on the arithmetic, not on a live count: a library test binary
    /// does not define the `malloc_conf` override (see `crate::malloc_conf`),
    /// so `narenas` in-test is jemalloc's own `4 × ncpu` default. The live
    /// count is checked where the override exists — the binary's
    /// `jemalloc_applies_the_requested_arena_count`, plus this verdict on the
    /// running server at startup.
    #[test]
    fn the_intended_arena_count_is_one_per_shard_plus_one() {
        assert_eq!(arena_layout(8, Some(9)), ArenaLayout::AsIntended);
        // Fewer than intended is not this function's complaint: an unbound
        // shard is already reported, loudly, as a partial bind.
        assert_eq!(arena_layout(8, Some(1)), ArenaLayout::AsIntended);
        assert_eq!(
            arena_layout(8, Some(10)),
            ArenaLayout::Excess {
                total: 10,
                expected: 9
            },
            "one arena past the shard count means the automatic pool came back"
        );
        // jemalloc's default on a 10-core machine, i.e. `narenas:1` ignored.
        assert_eq!(
            arena_layout(8, Some(40)),
            ArenaLayout::Excess {
                total: 40,
                expected: 9
            }
        );
        assert_eq!(arena_layout(8, None), ArenaLayout::Unknown);
    }
}
