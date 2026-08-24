use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_types::metrics::definitions::{FieldsExpired, KeysExpired, ShardPanicsIsolated};
use tokio::sync::mpsc;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::EvictionConfig;
use crate::functions::SharedFunctionRegistry;
use crate::keyspace_event::KeyspaceEventFlags;
use crate::persistence::{RecoveryStats, RocksStore, SnapshotCoordinator, WalConfig};
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
/// `global_epoch` is the honest coarse fallback for the one write class the
/// `shard` module genuinely cannot localize to keys: a whole-DB flush
/// (`FLUSHDB`/`FLUSHALL`, whose write record carries no keys). Folding the epoch
/// into every key's effective version makes such a write invalidate *all*
/// watches, exactly matching Redis's `touchAllWatchedKeysOnFlush`.
///
/// It is deliberately *not* the fallback for field expiry. An active-expiry
/// cycle that reaps hash fields from surviving hashes enumerates those
/// survivors (`ExpiryResult::field_shrunk_keys`), so it bumps their slots
/// instead: a tenant whose field TTLs fire continuously would otherwise abort
/// every other connection's WATCH on the shard on every cycle, forever
/// (`specs/txn.md` FM-TXN-033 — an unbounded liveness violation, not a bounded
/// over-abort).
/// Which clause of the WATCH check refused an `EXEC`.
///
/// The label values are the observable contract (`specs/txn.md` FM-TXN-033), so
/// they live next to the check rather than being spelled at the metric call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchAbortReason {
    /// A watched key's Hash Slot version moved. That covers a write to the
    /// watched key itself, a write to any other key aliased onto the same slot
    /// (the declared slot-granularity deviation), an expiry that bumped, and a
    /// keyless dirtying write (`FLUSHDB`) that advanced the shard epoch.
    WatchedSlotWrite,
    /// A key that was live when watched is gone at `EXEC` with no version bump
    /// for this watcher — another watcher's no-bump `WATCH`-time purge, or its
    /// own already-elapsed TTL.
    Expiry,
}

impl WatchAbortReason {
    /// The `reason` label value reported on
    /// `frogdb_transactions_watch_aborted_total`.
    pub fn label(self) -> &'static str {
        match self {
            Self::WatchedSlotWrite => "watched-slot-write",
            Self::Expiry => "expiry",
        }
    }
}

/// A remote-readable handle on one watched slot's WATCH generation.
///
/// The generation itself lives on — and is only ever advanced by — the shard
/// that owns the slot. A fence hands a *reader* out: the two counters behind it
/// are the very cells the owner bumps, so `still_current` observes every write
/// the owning shard has finished, from any thread, with no round-trip.
///
/// That is what closes TR-TXN-028's window. A watch on a shard other than the
/// batch's target is verified there first (a cheap fail-fast), and the fence it
/// mints then rides on the target's `ExecTransaction`: the target re-reads the
/// foreign generation *inside its own atomic commit step*, before the first
/// queued command runs. Any write to the watched slot that the coordinator
/// could have missed between the two is therefore either visible to that read —
/// and aborts the EXEC — or happened after the commit step began, at which
/// point nothing on the target shard has observed the batch yet and the EXEC
/// legitimately orders before it.
#[derive(Debug, Clone)]
pub struct WatchFence {
    /// The owning shard's stamp cell for this slot.
    slot_stamp: Arc<AtomicU64>,
    /// The owning shard's epoch cell, folded in exactly as `version_for` does.
    epoch: Arc<AtomicU64>,
    /// The generation the watch was taken against.
    observed: u64,
}

impl WatchFence {
    /// Build a fence over caller-owned generation cells.
    ///
    /// Production fences come from the owning shard
    /// ([`ShardWorker::watch_fence_for_key`]); this constructor is what lets a
    /// test move a generation under a fence that has already been carried.
    pub fn over(slot_stamp: Arc<AtomicU64>, epoch: Arc<AtomicU64>, observed: u64) -> Self {
        Self {
            slot_stamp,
            epoch,
            observed,
        }
    }

    /// Whether the watched slot's generation still reads as it did at WATCH.
    ///
    /// The two loads are not one atomic read, and they do not need to be: both
    /// counters only ever advance, so a torn pair reads *some* value at or above
    /// the true current generation and can never coincide with an older
    /// snapshot. A fence can therefore refuse a hair early under a concurrent
    /// bump, but never miss one.
    pub fn still_current(&self) -> bool {
        self.slot_stamp
            .load(Ordering::Acquire)
            .wrapping_add(self.epoch.load(Ordering::Acquire))
            == self.observed
    }
}

/// Which side of the off-target watch protocol (TR-TXN-028) one
/// `ExecTransaction` round-trip plays.
#[derive(Debug, Clone)]
pub enum WatchFenceRole {
    /// Re-verify these carried fences before running anything, and abort the
    /// whole batch if any generation moved. `Verify(vec![])` — the default — is
    /// every EXEC that has no watch off its target shard.
    Verify(Vec<WatchFence>),
    /// Watch-only probe on a shard that is *not* the batch's target: answer a
    /// clean verdict with one fence per watched key, so the target's commit can
    /// re-verify them atomically.
    Mint,
}

impl Default for WatchFenceRole {
    fn default() -> Self {
        Self::Verify(Vec::new())
    }
}

#[derive(Debug, Default)]
pub struct SlotVersions {
    /// slot -> version cell; a slot absent from the map reads as 0 (never
    /// bumped). The cell is shared rather than owned so a [`WatchFence`] can
    /// hand the *reader* to another shard while this shard keeps bumping it.
    versions: std::collections::HashMap<u16, Arc<AtomicU64>>,
    /// Shard-wide epoch folded into every key's effective version (see above).
    global_epoch: Arc<AtomicU64>,
}

impl SlotVersions {
    /// The effective WATCH version for `slot`: its per-slot stamp plus the
    /// shard-wide epoch. Both components are monotonic, so the sum is monotonic
    /// from any snapshot — it changes iff the slot was bumped OR the epoch was.
    pub(crate) fn version_for(&self, slot: u16) -> u64 {
        self.versions
            .get(&slot)
            .map(|cell| cell.load(Ordering::Acquire))
            .unwrap_or(0)
            .wrapping_add(self.global_epoch.load(Ordering::Acquire))
    }

    /// A fence over `slot`'s generation, pinned to the `observed` value.
    ///
    /// Materializes the slot's cell if this shard has never bumped it: a fence
    /// over a never-written slot is exactly the interesting case (the dead
    /// watch on an absent key), and it has to see the *creation* that bumps it.
    pub(crate) fn fence_for(&mut self, slot: u16, observed: u64) -> WatchFence {
        let cell = Arc::clone(self.versions.entry(slot).or_default());
        WatchFence::over(cell, Arc::clone(&self.global_epoch), observed)
    }

    /// Advance a single slot's stamp by one.
    fn bump_slot(&mut self, slot: u16) {
        self.versions
            .entry(slot)
            .or_default()
            .fetch_add(1, Ordering::Release);
    }

    /// Advance the shard-wide epoch (invalidates every outstanding watch).
    fn bump_global(&mut self) {
        self.global_epoch.fetch_add(1, Ordering::Release);
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

    /// The replication offset of the last write **this shard** broadcast — the
    /// `Y_s` a full-sync payload reports as its per-shard coverage watermark.
    ///
    /// Written by the terminal `ReplicationBroadcast` write effect, which is
    /// the last thing a write does, so by the time this shard processes any
    /// later message every write it has executed is at or below this value —
    /// and nothing above it has executed here. That exactness is the whole
    /// point, and it is why
    /// [`ReplicationBroadcaster::current_offset`](frogdb_replication::ReplicationBroadcaster::current_offset)
    /// is *not* a substitute: that is the node-wide head, which another shard
    /// can have advanced past this shard's last broadcast. A watermark that is
    /// too high makes the replica skip a frame the payload does not contain —
    /// silent write loss.
    ///
    /// Kept as a maximum rather than a plain assignment: the broadcaster hands
    /// back the offset it assigned, and nothing in this type's contract
    /// promises those arrive in increasing order.
    pub(crate) last_broadcast_offset: u64,

    /// Deterministic pop commands synthesized while satisfying blocking waiters
    /// (issue 02), pending broadcast to replicas.
    ///
    /// When a blocking pop (BLPOP/BZPOPMIN/BLMOVE/…) is served by a later write,
    /// the store mutation happens at the `WaiterSatisfaction` effect but only
    /// the *waking* write is broadcast — a replica re-executing it keeps the
    /// element the primary's blocked client consumed. The satisfaction driver
    /// records the equivalent deterministic command(s) here; the terminal
    /// `ReplicationBroadcast` effect flushes them **after** the waking write's
    /// own broadcast, so replicas apply push-then-pop and converge. Populated
    /// and drained within a single `run_write_effects` call.
    pub(crate) pending_serve_propagations: Vec<crate::command::SynthesizedCommand>,

    /// Whether per-request tracing spans are enabled.
    pub(crate) per_request_spans: Arc<AtomicBool>,

    /// Whether active key expiry is paused (true during CLIENT PAUSE ALL).
    pub(crate) expiry_paused: Arc<AtomicBool>,

    /// The node-global `CLIENT PAUSE`, as the shard sees it.
    ///
    /// Read at the blocking-pop decision point: while a node pause is armed a
    /// blocking write command parks instead of taking an immediate pop, so the
    /// pop cannot cross the drain window the pause exists to create
    /// (`specs/blocking.md` TR-BLOCKING-026). The gate carries the pause's
    /// deadline rather than a latch, so it lapses on its own even on a shard
    /// that sees no traffic.
    pub(crate) node_write_pause: Arc<crate::client_registry::NodeWritePauseGate>,

    /// Whether this shard parked at least one blocking command that could have
    /// popped, because [`Self::node_write_pause`] was armed.
    ///
    /// Such a waiter has no wake coming: the data was already there, so no
    /// later write will drive the satisfaction pass. The 100 ms blocking sweep
    /// checks this flag and, once the pause has lapsed, runs the pass itself
    /// (`ShardWorker::resume_pops_deferred_by_pause`). Cleared by that pass.
    pub(crate) pops_deferred_by_pause: bool,

    /// Monotonic count of blocking waiters this shard has served store data
    /// to, bumped in the satisfaction driver's committed-delivery arm (and
    /// only there — a rejection or a restored pop is not a delivery).
    ///
    /// Read as a *delta* around a satisfaction pass by
    /// `resume_pops_deferred_by_pause`, which needs to know whether a pass
    /// actually consumed store data. The wait queue's own waiter count cannot
    /// answer that: a waiter also leaves the queue on a rejection
    /// (`WRONGTYPE`/`NOGROUP` drain) or an elapsed deadline, neither of which
    /// touches the keyspace and neither of which may drag a write's effects
    /// (a WATCH-invalidating version bump, a WAL record) along behind it.
    pub(crate) waiters_served_total: u64,

    /// Shared keyspace notification event flags (from CONFIG notify-keyspace-events).
    /// Zero means disabled. Read atomically from the shard worker on every write.
    pub(crate) notify_keyspace_events: Arc<AtomicU32>,

    /// Whether active expiry is disabled via DEBUG SET-ACTIVE-EXPIRE 0.
    pub(crate) debug_active_expire_disabled: bool,

    /// Shard-driver seam: when true the event loop's two periodic-sweep timer
    /// branches (active expiry, blocking-waiter timeout) are suppressed and the
    /// sweeps arrive as [`ShardMessage::DriveTick`](super::message::ShardMessage)
    /// messages instead. See [`ShardWorker::set_driven_ticks`].
    #[cfg(any(test, feature = "shard-driver"))]
    pub(crate) driven_ticks: bool,

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

    /// This shard's coverage watermark `Y_s`: the offset of the last write it
    /// broadcast. See [`Self::last_broadcast_offset`]'s field docs.
    pub fn last_broadcast_offset(&self) -> u64 {
        self.last_broadcast_offset
    }

    /// Record the offset the broadcaster assigned to a frame this shard just
    /// emitted. The single writer of [`Self::last_broadcast_offset`].
    pub(crate) fn record_broadcast_offset(&mut self, offset: u64) {
        self.last_broadcast_offset = std::cmp::max(self.last_broadcast_offset, offset);
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

    /// Shard-driver seam: switch this worker's two periodic sweeps from timer
    /// branches to queued [`DriveTick`](super::message::ShardMessage::DriveTick)
    /// messages.
    ///
    /// With `driven == true` the event loop stops polling its 100 ms
    /// active-expiry and blocking-waiter-timeout intervals; whoever set the flag
    /// owns delivering both sweeps as `DriveTick` messages at the cadence the
    /// timers would have used (the server does this under the `turmoil`
    /// feature). The point is determinism, not cadence: a queued sweep is
    /// totally ordered against the commands around it, whereas a timer branch
    /// races them inside `select!` (determinism audit A51 / remediation R6).
    ///
    /// The other two interval branches (metrics, search commit) are untouched —
    /// they keep the loop alive on channel close, which the shard supervisor's
    /// fail-stop classification depends on.
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    pub fn set_driven_ticks(&mut self, driven: bool) {
        self.driven_ticks = driven;
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

    /// Replace this shard's node-global write-pause gate with the shared one
    /// from the `ClientRegistry` (`specs/blocking.md` TR-BLOCKING-026).
    pub fn set_node_write_pause_gate(
        &mut self,
        gate: Arc<crate::client_registry::NodeWritePauseGate>,
    ) {
        self.node_write_pause = gate;
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

    /// Share this node's boot-time recovery outcome with the worker.
    ///
    /// Recovery runs once, node-wide, before any shard worker exists, so this
    /// is set once at spawn time (mirroring `set_keyspace_stats` above) rather
    /// than re-read live like the snapshot coordinator's stats below.
    pub fn set_recovery_stats(&mut self, stats: Arc<RecoveryStats>) {
        self.persistence.set_recovery_stats(stats);
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
    /// Assemble the [`ShardWriteSeam`](crate::write_seam::ShardWriteSeam) a
    /// script execution's `redis.call`s are admitted through
    /// (`specs/txn.md` FM-TXN-051).
    ///
    /// The issuer-scoped half (`admission`) arrives on the shard message; slot
    /// ownership and the self-fence come from *this worker's* live handles, so
    /// they are the node's current truth rather than a value captured when the
    /// command was dispatched — which is the whole point of moving the three
    /// checks off the connection's queue-time gauntlet.
    ///
    /// Separate from [`Self::command_context`] because it is not part of every
    /// execution: a command that reached the shard through the connection's
    /// gauntlet has already been admitted.
    pub(crate) fn write_seam(
        &self,
        admission: crate::write_seam::WriteAdmission,
    ) -> crate::write_seam::ShardWriteSeam {
        // Same derivation as `command_context`'s: the dynamic self_node_id from
        // ClusterState (updated by HARD reset) wins over the static one.
        let node_id = self
            .cluster
            .cluster_state()
            .and_then(|cs| cs.self_node_id())
            .or(self.cluster.node_id());
        crate::write_seam::ShardWriteSeam::new(
            Some(admission),
            self.cluster.cluster_state().cloned(),
            node_id,
            self.cluster.quorum_checker_owned(),
            self.cluster.replication_tracker().cloned(),
        )
    }

    /// Admit every command of a queued transaction through the shard write
    /// seam before any of them runs (`specs/txn.md` FM-TXN-051).
    ///
    /// The connection's queue-time gauntlet checked the same three things when
    /// each command was *queued*; between then and EXEC the slot can move, the
    /// user's ACL can be rewritten and the good-replica count can drop. Because
    /// EXEC's whole point is that the batch is indivisible, a refusal here fails
    /// the transaction as a whole rather than one slot of it — no command has
    /// run yet, so nothing partial survives.
    pub(crate) fn admit_transaction(
        &self,
        commands: &[frogdb_protocol::ParsedCommand],
        admission: &crate::write_seam::WriteAdmission,
    ) -> Result<(), String> {
        let seam = self.write_seam(admission.clone());
        for command in commands {
            let name = command.name_uppercase_string();
            let Some(entry) = self.registry.get(&name) else {
                // Unknown command: EXEC's own per-command path reports it.
                continue;
            };
            let args = &command.args;
            let keyed_flags = entry.keys_with_flags(args);
            seam.admit(&crate::write_seam::WriteRequest {
                name: &name,
                subcommand: crate::command::extract_subcommand(&name, args),
                is_write: entry.flags().contains(crate::command::CommandFlags::WRITE),
                keyed_flags: &keyed_flags,
                fallback_access: crate::command::key_access_type_for_flags(entry.flags()),
            })?;
        }
        Ok(())
    }

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
        // One read of the coordinator's save history, same as the
        // connection-level INFO builder's — so a script polling
        // `redis.call('INFO')` for save health sees the real state instead
        // of a static `ok` (issue 10 / FM-PERSISTENCE-022).
        let snapshot_coordinator = self.persistence.snapshot_coordinator();
        let snapshot_stats = snapshot_coordinator.stats();
        let bgsave_in_progress = snapshot_coordinator.in_progress();
        let recovery_stats = Arc::clone(self.persistence.recovery_stats());

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
            master_sync_error: self.identity.master_sync_error(),
            json_limits: self.json_limits,
            snapshot_stats,
            bgsave_in_progress,
            recovery_stats,
            eviction_policy: self.eviction.policy(),
            // Set by `run_script` alone: only a script invocation carries the
            // per-caller `WriteAdmission` the seam needs, and only a script
            // produces writes the connection's gauntlet never saw.
            write_seam: None,
            // Set by `execute_command_body` alone, and only for a write-flagged
            // blocking command: it is the one seam that knows both the handler
            // and the originating connection (`specs/blocking.md`
            // TR-BLOCKING-026).
            blocking_pop_paused: false,
            effects: Default::default(),
        }
    }

    /// Whether a blocking write command dispatched on `conn_id` must park
    /// rather than take an immediate pop (`specs/blocking.md`
    /// TR-BLOCKING-026).
    ///
    /// Three conjuncts, each load-bearing:
    ///
    /// - **write-flagged**: only a pop is a write. A read-only blocker
    ///   (`XREAD`) observes the keyspace without mutating it, so neither
    ///   `PAUSE WRITE` nor the drain window it opens has anything to protect
    ///   against — and gating it would newly park readers that `PAUSE ALL`
    ///   already handles at the connection.
    /// - **[`ExecutionStrategy::Blocking`]**: the canonical
    ///   blocking-capable predicate, the same one the connection's pause
    ///   bypass keys off. A command with nowhere to park must not be gated
    ///   here — it would have to be refused instead, which is the connection
    ///   gate's job.
    /// - **not replica apply**: the connection-side pause gate never sees the
    ///   replica apply path, so gating it here would newly hold writes the
    ///   primary already committed and diverge the two keyspaces.
    pub(crate) fn blocking_pop_paused(
        &self,
        handler: &dyn crate::command::Command,
        conn_id: u64,
    ) -> bool {
        conn_id != super::helpers::REPLICA_INTERNAL_CONN_ID
            && handler
                .flags()
                .contains(crate::command::CommandFlags::WRITE)
            && matches!(
                handler.execution_strategy(),
                crate::command::ExecutionStrategy::Blocking { .. }
            )
            && self.node_write_pause.active()
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

    /// Adopt the shared `hotshards-enabled` kill switch, so `CONFIG SET
    /// hotshards-enabled no` stops this shard's op-rate accounting from the next
    /// dispatched command (and re-enabling starts a fresh window).
    pub fn set_hotshards_enabled_flag(&mut self, flag: Arc<AtomicBool>) {
        self.observability
            .operation_counters_mut()
            .set_enabled_flag(flag);
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

    /// Get the WATCH version for a key — its Hash Slot's stamp (plus the
    /// shard-wide epoch). Now load-bearing: the key selects the slot, so
    /// `check_watches` discriminates keys by slot.
    pub fn get_key_version(&self, key: &[u8]) -> u64 {
        self.slot_versions.version_for(slot_for_key(key))
    }

    /// Mint a [`WatchFence`] over `key`'s slot, pinned to `observed`.
    ///
    /// Only this shard advances that generation, but the fence reads it from
    /// anywhere — which is what lets the batch's *target* shard re-check a watch
    /// this shard owns, inside the target's own commit step (TR-TXN-028).
    pub(crate) fn watch_fence_for_key(&mut self, key: &[u8], observed: u64) -> WatchFence {
        self.slot_versions.fence_for(slot_for_key(key), observed)
    }

    /// Mint one fence per watch, each pinned to that watch's *current* observed
    /// generation on this shard.
    ///
    /// Called only after [`Self::watch_abort_reason`] came back clean, so each
    /// key's live generation equals the version recorded at `WATCH` time.
    pub(crate) fn mint_watch_fences(&mut self, watches: &[WatchEntry]) -> Vec<WatchFence> {
        watches
            .iter()
            .map(|watch| self.watch_fence_for_key(&watch.key, watch.version))
            .collect()
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
    ///
    /// Test-only since the abort path started naming its reason: production
    /// code calls [`Self::watch_abort_reason`] directly, because it needs the
    /// reason for the metric label.
    #[cfg(test)]
    pub(crate) fn check_watches(&self, watches: &[WatchEntry]) -> bool {
        self.watch_abort_reason(watches).is_none()
    }

    /// [`Self::check_watches`], but naming *which* clause failed — `None` when
    /// every watch still holds.
    ///
    /// The two clauses are separately diagnosable on purpose: a WATCH loop that
    /// never commits is otherwise indistinguishable from one that is merely
    /// contended, and the difference (a slot being written vs. watched keys
    /// dying under the watcher) decides where to look. The caller at the EXEC
    /// seam turns this into `frogdb_transactions_watch_aborted_total{reason}`.
    pub(crate) fn watch_abort_reason(&self, watches: &[WatchEntry]) -> Option<WatchAbortReason> {
        for WatchEntry {
            key,
            version,
            live_at_watch,
        } in watches
        {
            if self.get_key_version(key) != *version {
                // Changed via a version-bumping path: a write to the key, a write
                // to any key aliased onto its slot, or a keyless dirtying write
                // that moved the shard epoch.
                return Some(WatchAbortReason::WatchedSlotWrite);
            }
            if *live_at_watch && !self.store.exists_unexpired(key) {
                // Watched live, now expired/gone with no bump for us (gap 4).
                return Some(WatchAbortReason::Expiry);
            }
        }
        None
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

    /// Restore command-scoped shard state after a panic was caught at one of the
    /// [`panic_guard`](super::panic_guard) boundaries, and record it.
    ///
    /// The unwind skipped whatever cleanup the panicking frame owed, so the two
    /// pieces of state that are set for the duration of one command and cleared
    /// after it are reset here (see the `panic_guard` module docs for why these
    /// and not others). Lock-table state is *not* touched here: the VLL entry of
    /// a dequeued op is released by its own guard, which knows which op it was.
    ///
    /// Returns the reply the caller should send in place of the answer the
    /// client never got.
    pub(crate) fn recover_from_panic(
        &mut self,
        site: super::panic_guard::PanicSite,
        command: &str,
        panic_message: &str,
    ) -> Response {
        let shard_id = self.shard_id();
        ShardPanicsIsolated::inc(
            self.observability.metrics(),
            &shard_id.to_string(),
            site.as_str(),
        );
        tracing::error!(
            shard_id,
            site = site.as_str(),
            command,
            panic = panic_message,
            "caught a panic at the shard boundary; the shard survived and the \
             client was answered with an error — this is always a bug"
        );

        // A panic between `set_suppress_touch(true)` and its reset would leave
        // OBJECT FREQ/IDLETIME frozen for every later command on this shard.
        self.store.set_suppress_touch(false);
        // Synthesized blocking-pop propagations belong to the write effects that
        // died; letting them ride along with the *next* write's broadcast would
        // ship a pop the primary never served.
        self.pending_serve_propagations.clear();

        Response::error(super::panic_guard::INTERNAL_ERROR)
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
        worker.set_role_controller(Arc::new(FixedRoleController::new(Some(target), true)));

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
