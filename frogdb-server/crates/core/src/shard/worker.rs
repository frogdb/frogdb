use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64};

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_types::metrics::definitions::{FieldsExpired, KeysExpired};
use tokio::sync::mpsc;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::EvictionConfig;
use crate::functions::SharedFunctionRegistry;
use crate::keyspace_event::KeyspaceEventFlags;
use crate::persistence::{RocksStore, SnapshotCoordinator, WalConfig};
use crate::pubsub::ShardSubscriptions;
use crate::registry::CommandRegistry;
use crate::replication::SharedBroadcaster;
use crate::scripting::{ScriptExecutor, ScriptingConfig};
use crate::store::HashMapStore;
use crate::store::Store;

use super::active_expiry::ActiveExpiryCoordinator;
use super::builder::ShardWorkerBuilder;
use super::connection::NewConnection;
use super::keyspace_coordinator::KeyspaceNotificationCoordinator;
use super::message::{ShardReceiver, ShardSender, WatchEntry};
use super::partition::slot_for_key;
use super::search::lifecycle::IndexLifecycleManager;
use super::types::{
    ShardCluster, ShardEviction, ShardIdentity, ShardObservability, ShardPersistence,
    ShardScripting, ShardTracking, ShardVll,
};
use super::wait_queue::ShardWaitQueue;

/// Per-Internal-Shard WATCH version store, **slot-granular**.
///
/// Replaces the former single shard-wide `shard_version` counter. A watched key
/// is validated against its Hash Slot's stamp, so a write to a key in a
/// *different* slot on the same shard no longer over-aborts the watch (proposal
/// 18). Bounded without GC: at most one `u64` per slot ever written on this
/// shard (≤ 16384), and slots are permanent so entries never need reclaiming.
///
/// `global_epoch` is the honest coarse fallback for the write sources the
/// `shard` module cannot localize to keys within its reach: a whole-DB flush
/// (`FLUSHDB`/`FLUSHALL`, whose write record carries no keys) and any
/// active-expiry cycle that reaped hash *fields* from a surviving hash (a field
/// TTL that shrinks but does not remove the key, whose keys `ExpiryResult` does
/// not carry — only a `fields_expired` count). Because that field information is
/// not key-attributed, ANY cycle with `fields_expired > 0` bumps the epoch,
/// independently of whether the same cycle also removed whole keys (those are
/// slot-attributed separately). Folding the epoch into every key's effective
/// version makes those events invalidate *all* watches — a safe over-abort that
/// preserves the zero-false-negative invariant, exactly matching Redis's
/// `touchAllWatchedKeysOnFlush`.
#[derive(Debug, Default)]
pub struct SlotVersions {
    /// slot -> version; a slot absent from the map reads as 0 (never bumped).
    versions: std::collections::HashMap<u16, u64>,
    /// Shard-wide epoch folded into every key's effective version (see above).
    global_epoch: u64,
}

impl SlotVersions {
    /// The effective WATCH version for `slot`: its per-slot stamp plus the
    /// shard-wide epoch. Both components are monotonic, so the sum is monotonic
    /// from any snapshot — it changes iff the slot was bumped OR the epoch was.
    pub(crate) fn version_for(&self, slot: u16) -> u64 {
        self.versions
            .get(&slot)
            .copied()
            .unwrap_or(0)
            .wrapping_add(self.global_epoch)
    }

    /// Advance a single slot's stamp by one.
    fn bump_slot(&mut self, slot: u16) {
        let v = self.versions.entry(slot).or_insert(0);
        *v = v.wrapping_add(1);
    }

    /// Advance the shard-wide epoch (invalidates every outstanding watch).
    fn bump_global(&mut self) {
        self.global_epoch = self.global_epoch.wrapping_add(1);
    }

    /// Advance the slots of the given keys, each distinct slot at most once per
    /// call (so a write touching two keys in one slot bumps it once — mirroring
    /// the former one-bump-per-effect semantics). An empty key set advances the
    /// shard-wide epoch instead: a warranted bump that names no key (a whole-DB
    /// flush) must invalidate all watches, never nothing.
    fn bump_keys<'a>(&mut self, keys: impl IntoIterator<Item = &'a [u8]>) {
        let mut slots: Vec<u16> = keys.into_iter().map(slot_for_key).collect();
        if slots.is_empty() {
            self.bump_global();
            return;
        }
        slots.sort_unstable();
        slots.dedup();
        for slot in slots {
            self.bump_slot(slot);
        }
    }
}

/// A shard worker that owns a partition of the data.
pub struct ShardWorker {
    /// Immutable shard identity.
    pub(crate) identity: ShardIdentity,

    /// Local data store.
    pub store: HashMapStore,

    /// Receiver for shard messages.
    pub(crate) message_rx: ShardReceiver,

    /// Receiver for new connections.
    pub(crate) new_conn_rx: mpsc::Receiver<NewConnection>,

    /// Senders to all shards (for cross-shard operations).
    pub(crate) shard_senders: Arc<Vec<ShardSender>>,

    /// Command registry.
    pub(crate) registry: Arc<CommandRegistry>,

    /// Per-slot WATCH version store (slot-granular WATCH detection).
    pub(crate) slot_versions: SlotVersions,

    /// Persistence: RocksDB, WAL, snapshots.
    pub(crate) persistence: ShardPersistence,

    /// Observability: metrics, slowlog, latency, counters.
    pub(crate) observability: ShardObservability,

    /// Memory management: eviction config, pool, memory limit.
    pub(crate) eviction: ShardEviction,

    /// VLL: intent table, tx queue, continuation lock.
    pub(crate) vll: ShardVll,

    /// Cluster: raft, cluster state, node ID, network factory.
    pub(crate) cluster: ShardCluster,

    /// Pub/Sub subscriptions for this shard.
    pub(crate) subscriptions: ShardSubscriptions,

    /// Owns the emit→subscriber routing decision for keyspace notifications:
    /// broadcast subscribers register on the coordinator shard (shard 0), so an
    /// event emitted on the key-owner shard is routed there instead of into the
    /// emitting shard's own (subscriber-less) table.
    pub(crate) keyspace_notify: KeyspaceNotificationCoordinator,

    /// Client tracking: invalidation registry, tracking table, broadcast table.
    pub(crate) tracking: ShardTracking,

    /// Scripting: Lua script executor, function registry.
    pub(crate) scripting: ShardScripting,

    /// Wait queue for blocking commands.
    pub(crate) wait_queue: ShardWaitQueue,

    /// Replication broadcaster for streaming writes to replicas.
    pub(crate) replication_broadcaster: SharedBroadcaster,

    /// Whether per-request tracing spans are enabled.
    pub(crate) per_request_spans: Arc<AtomicBool>,

    /// Whether active key expiry is paused (true during CLIENT PAUSE ALL).
    pub(crate) expiry_paused: Arc<AtomicBool>,

    /// Shared keyspace notification event flags (from CONFIG notify-keyspace-events).
    /// Zero means disabled. Read atomically from the shard worker on every write.
    pub(crate) notify_keyspace_events: Arc<AtomicU32>,

    /// Whether active expiry is disabled via DEBUG SET-ACTIVE-EXPIRE 0.
    pub(crate) debug_active_expire_disabled: bool,

    /// Search: indexes, aliases, dictionaries, config.
    pub(crate) search: IndexLifecycleManager,

    /// Active-expiry decision + deletion engine (TTL key sweep + hash field
    /// sweep under a time budget). Side effects are applied shard-side from the
    /// returned `ExpiryResult`.
    pub(crate) expiry: ActiveExpiryCoordinator,

    /// JSON document limits (max depth / max size) from the server's `[json]`
    /// config, threaded into every [`CommandContext`](crate::command::CommandContext)
    /// this worker builds so JSON handlers enforce the configured limits.
    pub(crate) json_limits: crate::JsonLimits,
}

impl ShardWorker {
    /// Get the shard ID.
    pub fn shard_id(&self) -> usize {
        self.identity.shard_id()
    }

    /// Get the total number of shards.
    pub fn num_shards(&self) -> usize {
        self.identity.num_shards()
    }

    /// Get the data directory for this server.
    pub fn data_dir(&self) -> std::path::PathBuf {
        self.identity
            .data_dir()
            .cloned()
            .unwrap_or_else(|| std::path::PathBuf::from("data"))
    }

    /// Set the data directory.
    pub fn set_data_dir(&mut self, dir: std::path::PathBuf) {
        self.search.set_data_dir(dir.clone());
        self.identity.set_data_dir(dir);
    }

    /// Set whether this shard belongs to a replica server.
    pub fn set_is_replica(&mut self, is_replica: bool) {
        self.identity.set_is_replica(is_replica);
    }

    /// Get a shared handle to the is_replica flag.
    pub fn is_replica_flag(&self) -> Arc<AtomicBool> {
        self.identity.is_replica_flag().clone()
    }

    /// Replace this shard's is_replica flag with a shared one.
    ///
    /// This allows all shards, the acceptor, and connection handlers to share
    /// a single `Arc<AtomicBool>` so that `REPLICAOF NO ONE` can toggle replica
    /// status server-wide with a single atomic store.
    pub fn set_is_replica_flag(&mut self, flag: Arc<AtomicBool>) {
        self.identity.set_is_replica_flag(flag);
    }

    /// Install the server-wide role-transition controller so that `REPLICAOF`
    /// executed on this shard can drive Role Promotion/Demotion through the
    /// `RoleManager`.
    pub fn set_role_controller(&mut self, controller: Arc<dyn crate::command::RoleController>) {
        self.identity.set_role_controller(controller);
    }

    /// Replace this shard's expiry_paused flag with a shared one from the ClientRegistry.
    pub fn set_expiry_paused_flag(&mut self, flag: Arc<AtomicBool>) {
        self.expiry_paused = flag;
    }

    /// Replace this shard's WAL failure policy flag with a shared one from ConfigManager.
    pub fn set_wal_failure_policy_flag(&mut self, flag: Arc<AtomicU8>) {
        self.persistence.set_failure_policy(flag);
    }

    /// Set the shared per-shard memory usage vec.
    /// Used by SystemMetricsCollector to compute fragmentation ratio.
    pub fn set_shard_memory_used(&mut self, shared: Arc<Vec<AtomicU64>>) {
        self.observability.set_shard_memory_used(shared);
    }

    /// Share the process-wide keyspace hit/miss accumulator with this worker.
    ///
    /// The same `Arc` is held by the server so `INFO stats` reads it and
    /// `CONFIG RESETSTAT` advances its baseline.
    pub fn set_keyspace_stats(&mut self, stats: Arc<crate::KeyspaceStats>) {
        self.observability.set_keyspace_stats(stats);
    }

    /// Set the JSON document limits (max depth / max size) sourced from the
    /// server's `[json]` config. Threaded into every [`CommandContext`] this
    /// worker builds so JSON handlers enforce the configured limits.
    pub fn set_json_limits(&mut self, limits: crate::JsonLimits) {
        self.json_limits = limits;
    }

    /// Build a fully-populated [`CommandContext`](crate::command::CommandContext)
    /// for executing a command against this shard's local store.
    ///
    /// This is the single place that wires a command context from the shard
    /// worker. Cross-shard senders, cluster/replication handles, replica
    /// identity (`is_replica` / `master_host` / `master_port`), and the command
    /// registry are all sourced from `self` here — so every command-execution
    /// seam (normal dispatch, EVAL / EVALSHA / FCALL, and cross-shard script
    /// sub-commands) observes the *same* context and cannot drift out of sync
    /// (e.g. a Lua script reporting the wrong replica role via ROLE / INFO).
    pub(crate) fn command_context(
        &mut self,
        conn_id: u64,
        protocol_version: frogdb_protocol::ProtocolVersion,
    ) -> crate::command::CommandContext<'_> {
        // Prefer the dynamic self_node_id from ClusterState (updated by HARD
        // reset) over the static node_id captured at connection creation time.
        let node_id = self
            .cluster
            .cluster_state()
            .and_then(|cs| cs.self_node_id())
            .or(self.cluster.node_id());
        let is_replica = self.identity.is_replica();

        crate::command::CommandContext {
            store: &mut self.store,
            shard_senders: &self.shard_senders,
            shard_id: self.identity.shard_id(),
            num_shards: self.identity.num_shards(),
            conn_id,
            protocol_version,
            replication_tracker: self.cluster.replication_tracker(),
            cluster_state: self.cluster.cluster_state(),
            node_id,
            raft: self.cluster.raft(),
            network_factory: self.cluster.network_factory(),
            quorum_checker: self.cluster.quorum_checker(),
            command_registry: Some(&self.registry),
            is_replica,
            is_replica_flag: Some(self.identity.is_replica_flag().clone()),
            role_controller: self.identity.role_controller().cloned(),
            master_host: self.identity.master_host(),
            master_port: self.identity.master_port(),
            master_link_up: self.identity.master_link_up(),
            json_limits: self.json_limits,
            effects: Default::default(),
        }
    }

    /// Create a new shard worker without persistence.
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
        registry: Arc<CommandRegistry>,
    ) -> Self {
        ShardWorkerBuilder::new(shard_id, num_shards)
            .with_message_rx(message_rx)
            .with_new_conn_rx(new_conn_rx)
            .with_shard_senders(shard_senders)
            .with_registry(registry)
            .build()
    }

    /// Create a new shard worker without persistence but with eviction config.
    #[allow(clippy::too_many_arguments)]
    pub fn with_eviction(
        shard_id: usize,
        num_shards: usize,
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
        registry: Arc<CommandRegistry>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
    ) -> Self {
        ShardWorkerBuilder::new(shard_id, num_shards)
            .with_message_rx(message_rx)
            .with_new_conn_rx(new_conn_rx)
            .with_shard_senders(shard_senders)
            .with_registry(registry)
            .with_eviction(eviction_config)
            .with_metrics(metrics_recorder)
            .with_slowlog_id(slowlog_next_id)
            .with_replication(replication_broadcaster)
            .build()
    }

    /// Create a shard worker backed by the deterministic fake WAL sink.
    ///
    /// Mirrors [`Self::with_eviction`] but selects [`WalMode::Fake`], so the
    /// shard records WAL effects into the process-global
    /// [`FakeWalRegistry`](super::fake_wal_registry::FakeWalRegistry) without
    /// touching RocksDB. Test / `fake-wal` only.
    #[cfg(any(test, feature = "fake-wal"))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_fake_persistence(
        shard_id: usize,
        num_shards: usize,
        store: HashMapStore,
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
        registry: Arc<CommandRegistry>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
    ) -> Self {
        ShardWorkerBuilder::new(shard_id, num_shards)
            .with_store(store)
            .with_message_rx(message_rx)
            .with_new_conn_rx(new_conn_rx)
            .with_shard_senders(shard_senders)
            .with_registry(registry)
            .with_wal_mode(super::builder::WalMode::Fake)
            .with_eviction(eviction_config)
            .with_metrics(metrics_recorder)
            .with_slowlog_id(slowlog_next_id)
            .with_replication(replication_broadcaster)
            .build()
    }

    /// Create a new shard worker with persistence.
    #[allow(clippy::too_many_arguments)]
    pub fn with_persistence(
        shard_id: usize,
        num_shards: usize,
        store: HashMapStore,
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
        registry: Arc<CommandRegistry>,
        rocks_store: Arc<RocksStore>,
        wal_config: WalConfig,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
    ) -> Self {
        ShardWorkerBuilder::new(shard_id, num_shards)
            .with_store(store)
            .with_message_rx(message_rx)
            .with_new_conn_rx(new_conn_rx)
            .with_shard_senders(shard_senders)
            .with_registry(registry)
            .with_persistence(rocks_store, wal_config)
            .with_snapshot_coordinator(snapshot_coordinator)
            .with_eviction(eviction_config)
            .with_metrics(metrics_recorder)
            .with_slowlog_id(slowlog_next_id)
            .with_replication(replication_broadcaster)
            .build()
    }

    /// Replace the script executor with one using the given scripting config.
    pub fn set_scripting_config(&mut self, config: ScriptingConfig) {
        match ScriptExecutor::new(config) {
            Ok(executor) => self.scripting.set_executor(executor),
            Err(e) => {
                tracing::warn!(
                    shard_id = self.identity.shard_id(),
                    error = %e,
                    "Failed to reinitialize script executor with new config"
                );
            }
        }
    }

    /// Set the function registry for this shard.
    pub fn set_function_registry(&mut self, registry: SharedFunctionRegistry) {
        self.scripting.set_function_registry(registry);
    }

    /// Set the wait queue limits from blocking config.
    pub fn set_wait_queue_limits(
        &mut self,
        max_waiters_per_key: usize,
        max_blocked_connections: usize,
    ) {
        self.wait_queue = ShardWaitQueue::with_limits(max_waiters_per_key, max_blocked_connections);
    }

    /// Set the per-request spans flag (shared with connections and ConfigManager).
    pub fn set_per_request_spans(&mut self, flag: Arc<AtomicBool>) {
        self.per_request_spans = flag;
    }

    /// Set the shared keyspace notification event flags (from ConfigManager).
    pub fn set_notify_keyspace_events(&mut self, flag: Arc<AtomicU32>) {
        self.notify_keyspace_events = flag;
    }

    /// Install a search index lifecycle manager, replacing the worker's current
    /// one. Used during server startup recovery: the manager is built by
    /// [`IndexLifecycleManager::recover`] at spawn time (so its non-`Send` index
    /// handles never cross a thread boundary) and installed into the worker it
    /// was built for.
    pub fn install_search_manager(&mut self, manager: IndexLifecycleManager) {
        self.search = manager;
    }

    /// Get a mutable reference to the search indexes.
    pub fn search_indexes_mut(
        &mut self,
    ) -> &mut std::collections::HashMap<String, frogdb_search::ShardSearchIndex> {
        &mut self.search.indexes
    }

    /// Get a reference to the search indexes.
    pub fn search_indexes(
        &self,
    ) -> &std::collections::HashMap<String, frogdb_search::ShardSearchIndex> {
        &self.search.indexes
    }

    /// Set the replication broadcaster for this shard.
    pub fn set_replication_broadcaster(&mut self, broadcaster: SharedBroadcaster) {
        self.replication_broadcaster = broadcaster;
    }

    /// Set the Raft instance for cluster commands.
    pub fn set_raft(&mut self, raft: Arc<ClusterRaft>) {
        self.cluster.set_raft(raft);
    }

    /// Set the cluster state for cluster commands.
    pub fn set_cluster_state(&mut self, cluster_state: Arc<ClusterState>) {
        self.cluster.set_cluster_state(cluster_state);
    }

    /// Set this node's ID for cluster mode.
    pub fn set_node_id(&mut self, node_id: u64) {
        self.cluster.set_node_id(node_id);
    }

    /// Set the network factory for cluster node management.
    pub fn set_network_factory(&mut self, network_factory: Arc<ClusterNetworkFactory>) {
        self.cluster.set_network_factory(network_factory);
    }

    /// Set the quorum checker for local cluster health detection.
    pub fn set_quorum_checker(&mut self, quorum_checker: Arc<dyn QuorumChecker>) {
        self.cluster.set_quorum_checker(quorum_checker);
    }

    /// Set the replication tracker for INFO replication / WAIT support.
    pub fn set_replication_tracker(
        &mut self,
        tracker: Arc<crate::replication::ReplicationTrackerImpl>,
    ) {
        self.cluster.set_replication_tracker(tracker);
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        self.persistence.snapshot_coordinator()
    }

    /// Bump the WATCH version for the slots of the given keys (each distinct
    /// slot once). The load-bearing per-key bump: a write to key `b` no longer
    /// dirties a watch on key `a` unless they share a Hash Slot. An empty key
    /// set (a keyless-but-dirtying write, e.g. `FLUSHDB`) bumps the shard-wide
    /// epoch, invalidating every watch.
    pub(crate) fn bump_versions_for<'a>(&mut self, keys: impl IntoIterator<Item = &'a [u8]>) {
        self.slot_versions.bump_keys(keys);
    }

    /// Bump the WATCH version for a single key's slot.
    pub(crate) fn bump_version_for_key(&mut self, key: &[u8]) {
        self.slot_versions.bump_slot(slot_for_key(key));
    }

    /// Bump the shard-wide WATCH epoch, invalidating every outstanding watch on
    /// the shard. The safe over-abort fallback for a dirtying event whose keys
    /// the `shard` module cannot enumerate (a fields-only active-expiry cycle).
    pub(crate) fn bump_version_global(&mut self) {
        self.slot_versions.bump_global();
    }

    /// Get the WATCH version for a key — its Hash Slot's stamp (plus the
    /// shard-wide epoch). Now load-bearing: the key selects the slot, so
    /// `check_watches` discriminates keys by slot.
    pub fn get_key_version(&self, key: &[u8]) -> u64 {
        self.slot_versions.version_for(slot_for_key(key))
    }

    /// Check if watched keys have changed since they were watched.
    ///
    /// A watch is satisfied iff the key's version is unchanged AND it did not
    /// transition live -> expired/gone. The version compare catches every write
    /// and every expiry that bumped (active sweep, lazy read-path purge). The
    /// second clause catches the one death that does NOT bump for this watcher:
    /// a key watched while live that another watcher's no-bump WATCH-time purge
    /// (or its own already-elapsed TTL) removed — the gap-4 second-watcher case.
    /// `live_at_watch == false` means a stale/nonexistent watch (Redis
    /// `wk->expired`), which must NOT abort when the key stays gone. Uses the
    /// non-destructive `exists_unexpired` probe (constraint 1 — `check_watches`
    /// must not physically purge).
    pub(crate) fn check_watches(&self, watches: &[WatchEntry]) -> bool {
        watches.iter().all(
            |WatchEntry {
                 key,
                 version,
                 live_at_watch,
             }| {
                if self.get_key_version(key) != *version {
                    return false; // changed via a version-bumping path
                }
                if *live_at_watch && !self.store.exists_unexpired(key) {
                    return false; // watched live, now expired/gone with no bump (gap 4)
                }
                true
            },
        )
    }

    /// Lazily purge any watched keys whose TTL has elapsed, bumping the shard
    /// version once if a removal occurred (F3).
    ///
    /// A key that expired only lazily is still physically present until some
    /// access purges it, so the version-based [`Self::check_watches`] cannot
    /// see the expiry on its own. Calling this at the EXEC watch-validation
    /// seam makes the removal bump the shard version, so a watched key that
    /// transitioned live -> gone aborts the transaction — matching active
    /// expiry (`apply_expiry_effects`) and Redis/Valkey/Dragonfly. The store
    /// stays version-ignorant: the removal is decided by
    /// [`crate::store::Store::purge_if_expired`], the version bump lives here.
    /// One bump per call regardless of how many keys purge, mirroring active
    /// expiry's one-bump-per-cycle.
    pub(crate) fn purge_expired_watches(&mut self, watches: &[WatchEntry]) {
        for WatchEntry { key, .. } in watches {
            self.store.purge_if_expired(key);
        }
        // Apply the bump + drain for any watched key that expired during the
        // WATCH window — this must run before check_watches so the version
        // change is visible (F3). Subsumes the previous explicit increment.
        self.apply_lazy_purge_effects();
    }

    /// Drain the store's lazy-purge report and apply, for each physically
    /// removed key, the **same effect set active expiry applies for its own
    /// `deleted_keys`** (`apply_expiry_effects`, event_loop.rs): client-tracking
    /// invalidation, search-index deletion, the `expired` keyspace notification,
    /// the USDT key-expired probe, and an XREADGROUP-waiter drain — then a single
    /// shard-version bump for the batch. A key that died via a lazy read is thus
    /// indistinguishable from one the active sweep removed, matching
    /// Redis/Valkey, which fire the `expired` event from `expireIfNeeded`
    /// (lazy/on-access) and `activeExpireCycle` (sweep) alike.
    ///
    /// Also drains the sibling last-hash-field-death buffer (`take_lazily_emptied`)
    /// and fires the generic `del` effect set for those keys — see
    /// [`Self::drain_lazy_purge_effects`].
    ///
    /// Idempotency: every removal is pushed into the store's buffer exactly once
    /// (whole-key TTL via `check_and_delete_expired`'s actual-removal branch;
    /// last-hash-field death via `purge_expired_hash_fields`'s empty-and-delete
    /// branch — a second purge of the same key finds it already absent) and
    /// drained exactly once (`std::mem::take`). No key can be reported through two
    /// seams, because the first physical removal makes it absent for every later
    /// purge attempt. The active sweep shares `purge_expired_hash_fields` but
    /// discards the lazily-emptied buffer at its own seam (event_loop.rs), so a
    /// swept key never double-fires here. No guard is needed.
    pub(crate) fn apply_lazy_purge_effects(&mut self) {
        self.drain_lazy_purge_effects(true);
    }

    /// WATCH-time (`GetVersion`) variant: apply every physical-removal effect
    /// (tracking / search / `expired` notification / probe / XREADGROUP drain)
    /// but WITHHOLD the shard-version bump.
    ///
    /// A key purged here is genuinely gone, so the removal must still be
    /// externally visible — Redis fires the `expired` notification on lazy
    /// expiry regardless of which command triggered it, and a search index or a
    /// tracking consumer would otherwise silently miss the death. Only the
    /// version bump is withheld: the WATCH-time purge must stay no-bump (F3) so a
    /// WATCH on an already-expired key records a "nonexistent" watch and does not
    /// over-abort unrelated watchers on the shard. Splitting the drain here — fire
    /// the physical-removal effects, skip only the version bump — is what keeps
    /// the effect gap from silently persisting on the WATCH seam.
    pub(crate) fn apply_lazy_purge_effects_no_version_bump(&mut self) {
        self.drain_lazy_purge_effects(false);
    }

    /// Shared drain point (single-drain-point discipline): fire the per-key
    /// active-expiry effect set for each lazily-removed key, optionally bumping
    /// the shard version. Ordering mirrors `apply_expiry_effects`' `deleted_keys`
    /// branch (tracking → search → notify → probe, then the waiter drain), with
    /// the version bump applied once at the end for the whole batch.
    fn drain_lazy_purge_effects(&mut self, bump_version: bool) {
        // Fields reaped by this lazy read (whether or not they emptied a key).
        // Counted first and unconditionally — a lazy reap that shrinks but does
        // not empty a hash removes no key, so this is the only surface that sees
        // it. Mirrors the active sweep's per-field `frogdb_fields_expired_total`
        // increment (`ExpiryResult::fields_expired`, event_loop.rs).
        let expired_fields = self.store.take_lazily_expired_fields();
        if expired_fields > 0 {
            FieldsExpired::inc_by(
                self.observability.metrics(),
                expired_fields,
                &self.shard_id().to_string(),
            );
        }

        let purged = self.store.take_lazily_purged();
        // Keys removed because their last hash field expired on this lazy read.
        // Distinct seam, distinct event: Redis emits a generic `del` (not
        // `expired`) for a hash that empties via field TTL, matching active
        // expiry's `ExpiryResult::emptied_keys` branch (event_loop.rs).
        let emptied = self.store.take_lazily_emptied();
        // Hashes shrunk in place by this lazy read (≥1 field reaped, key still a
        // hash). They are not removed, so they carry no `del`/`expired` event and
        // do not flow through the removal branches below — but their search-index
        // doc now holds a stale reaped-field value, so re-index each survivor.
        // This is the READONLY-command analogue of a WRITE command's
        // `ReindexSpec` (a lazy reap on HGET/HGETALL/… has no such spec), and it
        // converges on the same `reindex_shrunk_hash_keys` owner the active sweep
        // uses (event_loop.rs).
        let shrunk = self.store.take_lazily_shrunk();
        self.reindex_shrunk_hash_keys(&shrunk);
        if purged.is_empty() && emptied.is_empty() {
            // A cycle that only shrank survivors still changed watched hashes:
            // bump their per-slot versions so a WATCH observes the mutation,
            // mirroring active expiry's field-expiry version bump.
            if bump_version && !shrunk.is_empty() {
                self.bump_versions_for(shrunk.iter().map(Bytes::as_ref));
            }
            return;
        }
        for key in &purged {
            // Invalidate tracked clients for the expired key (gated on there
            // being any — same guard active expiry uses).
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }
            // Remove the expired key from any search index it participated in.
            self.delete_from_search_indexes(key);
            // Emit the `expired` keyspace notification for the whole-key TTL
            // death — the exact event active expiry emits for `deleted_keys`.
            self.emit_keyspace_notification(key, "expired", KeyspaceEventFlags::EXPIRED);
            // Fire the USDT key-expired probe so the lazy removal is not
            // invisible to observers.
            crate::probes::fire_key_expired(
                std::str::from_utf8(key).unwrap_or("<binary>"),
                self.shard_id() as u64,
            );
            // Drain blocked XREADGROUP waiters for a removed stream key,
            // mirroring the DEL write path and the F1 active-expiry drain
            // (drain_stream_waiters_with_error → NOGROUP; plain XREAD waiters
            // stay blocked). No-op for non-stream keys.
            self.drain_stream_waiters_with_error(key);
        }
        // Last-hash-field-death keys: same effect set as active expiry's
        // `emptied_keys` branch — tracking + search invalidation, then a
        // generic `del` notification and the key-expired probe. A hash key is
        // never a stream, so the stream-waiter drain is a no-op, but keep it for
        // structural parity with the whole-key branch above.
        for key in &emptied {
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }
            self.delete_from_search_indexes(key);
            self.emit_keyspace_notification(key, "del", KeyspaceEventFlags::GENERIC);
            crate::probes::fire_key_expired(
                std::str::from_utf8(key).unwrap_or("<binary>"),
                self.shard_id() as u64,
            );
            self.drain_stream_waiters_with_error(key);
        }
        // Count each emptied key as one key expiration, on the same INFO stat
        // (`expired_keys`) AND Prometheus (`frogdb_keys_expired_total`) surfaces
        // the active sweep uses for its `emptied_keys` (event_loop.rs:
        // add_expired_keys + KeysExpired::inc_by via keys_expired()). The
        // whole-key `purged` keys already had the INFO stat bumped inside the
        // store (`check_and_delete_expired`), so only the emptied batch is
        // counted here. No double-count with the sweep: it discards the
        // lazily-emptied buffer before its own counting path, so a swept key
        // never reaches this drain.
        if !emptied.is_empty() {
            let n = emptied.len() as u64;
            self.store.add_expired_keys(n);
            KeysExpired::inc_by(
                self.observability.metrics(),
                n,
                &self.shard_id().to_string(),
            );
        }
        if bump_version {
            // Per-slot bump for each lazily-removed key (both seams) and each
            // shrunk survivor: a watched key that died lazily — whole-key TTL or
            // last-hash-field death — or whose hash shrank via field TTL is now
            // observed changed by check_watches (gap 3). Only the affected keys'
            // own slots are dirtied, so an unrelated watch on a different slot
            // survives.
            self.bump_versions_for(
                purged
                    .iter()
                    .chain(emptied.iter())
                    .chain(shrunk.iter())
                    .map(Bytes::as_ref),
            );
        }
    }

    /// Check if this connection can execute during a continuation lock.
    #[allow(clippy::result_large_err)]
    pub(crate) fn can_execute_during_lock(&self, conn_id: u64) -> Result<(), Response> {
        if let Some(owner) = self.vll.continuation_lock_owner()
            && owner != conn_id
        {
            return Err(Response::error("ERR shard busy with continuation lock"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod slot_versions_tests {
    use super::SlotVersions;

    #[test]
    fn absent_slot_reads_zero() {
        let sv = SlotVersions::default();
        assert_eq!(sv.version_for(7), 0, "a never-bumped slot reads 0");
        assert_eq!(sv.version_for(16383), 0);
    }

    #[test]
    fn bump_is_slot_local_and_monotonic() {
        let mut sv = SlotVersions::default();
        sv.bump_slot(7);
        assert_eq!(sv.version_for(7), 1, "bumped slot advances");
        assert_eq!(sv.version_for(8), 0, "a different slot is untouched");
        sv.bump_slot(7);
        assert_eq!(sv.version_for(7), 2, "same-slot bump advances again");
        assert_eq!(sv.version_for(8), 0);
    }

    #[test]
    fn bump_keys_dedups_slots_per_call() {
        let mut sv = SlotVersions::default();
        // Two keys colocated on one slot via a hash tag advance it once.
        sv.bump_keys([b"{t}a".as_slice(), b"{t}b".as_slice()]);
        let slot = super::slot_for_key(b"{t}a");
        assert_eq!(sv.version_for(slot), 1, "one bump for two same-slot keys");
    }

    #[test]
    fn bump_keys_distinct_slots_are_independent() {
        let mut sv = SlotVersions::default();
        sv.bump_keys([b"a".as_slice(), b"b".as_slice()]);
        assert_eq!(sv.version_for(super::slot_for_key(b"a")), 1);
        assert_eq!(sv.version_for(super::slot_for_key(b"b")), 1);
    }

    #[test]
    fn empty_key_set_bumps_global_epoch() {
        let mut sv = SlotVersions::default();
        let before_a = sv.version_for(super::slot_for_key(b"a"));
        // A warranted bump that names no key (e.g. FLUSHDB) invalidates all.
        sv.bump_keys(std::iter::empty::<&[u8]>());
        assert_eq!(
            sv.version_for(super::slot_for_key(b"a")),
            before_a + 1,
            "the global epoch folds into every slot's version"
        );
        assert_eq!(
            sv.version_for(super::slot_for_key(b"zzz")),
            1,
            "even an absent slot reflects the epoch bump (0 + epoch)"
        );
    }

    #[test]
    fn global_epoch_and_slot_bumps_compose() {
        let mut sv = SlotVersions::default();
        let slot = super::slot_for_key(b"a");
        sv.bump_slot(slot); // slot -> 1
        sv.bump_global(); // epoch -> 1
        assert_eq!(sv.version_for(slot), 2, "slot(1) + epoch(1)");
        // A different slot only carries the epoch.
        assert_eq!(sv.version_for(super::slot_for_key(b"b")), 1);
    }
}

#[cfg(test)]
mod command_context_tests {
    use super::*;
    use crate::registry::CommandRegistry;
    use crate::shard::builder::ShardWorkerBuilder;
    use crate::shard::connection::NewConnection;
    use crate::shard::message::{Envelope, ShardReceiver};
    use frogdb_protocol::ProtocolVersion;

    fn minimal_worker() -> ShardWorker {
        let (_mtx, mrx) = mpsc::channel::<Envelope>(1);
        let (_ntx, nrx) = mpsc::channel::<NewConnection>(1);
        ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(mrx))
            .with_new_conn_rx(nrx)
            .with_shard_senders(Arc::new(vec![]))
            .with_registry(Arc::new(CommandRegistry::new()))
            .build()
    }

    /// The builder must carry the shard's replica identity into every context —
    /// the fields EVAL/EVALSHA/FCALL previously dropped.
    #[test]
    fn command_context_carries_replica_identity() {
        use crate::shard::types::FixedRoleController;

        let mut worker = minimal_worker();
        worker.set_is_replica(true);
        let target: std::net::SocketAddr = "10.0.0.5:6390".parse().unwrap();
        worker.set_role_controller(Arc::new(FixedRoleController(Some(target), true)));

        let ctx = worker.command_context(42, ProtocolVersion::Resp2);
        assert!(ctx.is_replica, "built context must report replica role");
        assert_eq!(ctx.master_host.as_deref(), Some("10.0.0.5"));
        assert_eq!(ctx.master_port, Some(6390));
        assert!(
            ctx.master_link_up,
            "built context must report the role controller's link status"
        );
        assert_eq!(ctx.conn_id, 42);
        assert!(ctx.command_registry.is_some(), "registry must be wired");
        assert!(
            ctx.is_replica_flag.is_some(),
            "shared replica flag must be wired"
        );
    }

    /// On a primary the built context reports the primary role and no master.
    #[test]
    fn command_context_reports_primary_by_default() {
        let mut worker = minimal_worker();
        let ctx = worker.command_context(1, ProtocolVersion::Resp2);
        assert!(!ctx.is_replica);
        assert_eq!(ctx.master_host, None);
        assert_eq!(ctx.master_port, None);
        assert!(!ctx.master_link_up);
    }
}
