//! Primary node replication handling.
//!
//! This module handles the primary side of replication:
//! - Accepting replica connections
//! - Processing PSYNC requests
//! - Streaming WAL updates to replicas
//! - Handling REPLCONF ACKs

pub mod replay;
pub mod ring_buffer;
#[cfg(test)]
mod tests;

use bytes::Bytes;
use frogdb_persistence::RocksStore;
use frogdb_types::ReplicationTracker;
use parking_lot::RwLock;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;

use crate::BoxedStream;
use crate::ReplicationBroadcaster;
use crate::frame::{CONTROL_SHARD, ReplconfCodec, ReplicationFrame, serialize_command_to_resp};
use crate::identity::ReplicationIdentity;
use crate::offset_coordinator::OffsetCoordinator;
use crate::replica_session::SyncKind;
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;
use crate::wait_coordinator::WaitCoordinator;

pub use replay::{FullResyncReason, PartialSyncReplay, ReplayDecision, ReplayGrant};
pub use ring_buffer::{ReplicationRingBuffer, SplitBrainBufferConfig};

/// The split-brain divergence window this (demoted) Primary computed against the
/// last offset the cluster had acknowledged — the writes it committed past that
/// point and must surrender to the new Primary.
///
/// Constructed only when the node actually diverged: `end > start` AND `writes`
/// is non-empty (see [`PrimaryReplicationHandler::divergence_record`]). A
/// caught-up (or write-less) demotion yields `None`.
#[derive(Debug)]
pub struct DivergenceRecord {
    /// Lower bound: the minimum acked offset across streaming replicas
    /// (`min_acked().unwrap_or(0)` — `seq_diverge_start` in the split-brain log).
    pub start: u64,
    /// Upper bound: the live write position at demotion time
    /// (`seq_diverge_end` in the split-brain log).
    pub end: u64,
    /// The divergent writes `(offset, RESP)` with `offset > start`, offset-ordered
    /// (`writes.len()` is `ops_discarded`).
    pub writes: Vec<(u64, Bytes)>,
}

/// Configuration for proactive lag-threshold disconnection.
#[derive(Debug, Clone)]
pub struct LagThresholdConfig {
    /// Max replication lag in bytes before proactive disconnect. 0 = disabled.
    pub threshold_bytes: u64,
    /// Max replication lag in seconds (since last ACK) before proactive disconnect. 0 = disabled.
    pub threshold_secs: u64,
    /// Cooldown after a proactive disconnect before allowing another.
    pub cooldown: Duration,
}

/// How often the streaming task checks lag thresholds (every N frames).
pub(crate) const LAG_CHECK_INTERVAL: u64 = 100;

/// The **live** proactive lag-disconnect thresholds.
///
/// Shared by `Arc` between the primary handler and every streaming replica
/// session's lag policy, which re-reads both values on each evaluation. A store
/// here therefore retunes — or disables / arms — proactive lag disconnection on
/// sessions that are already streaming, with no reconnect and no restart, which
/// is what makes `CONFIG SET replication-lag-threshold-bytes` /
/// `replication-lag-threshold-secs` meaningful.
///
/// The cooldown is deliberately *not* here: it is derived from
/// `fullresync-cooldown-secs` and is captured per session.
#[derive(Debug)]
pub struct LagThresholds {
    threshold_bytes: AtomicU64,
    threshold_secs: AtomicU64,
}

impl LagThresholds {
    /// Seed the thresholds (0 = that threshold disabled).
    pub fn new(threshold_bytes: u64, threshold_secs: u64) -> Self {
        Self {
            threshold_bytes: AtomicU64::new(threshold_bytes),
            threshold_secs: AtomicU64::new(threshold_secs),
        }
    }

    /// Max replication lag in bytes before proactive disconnect (0 = disabled).
    pub fn threshold_bytes(&self) -> u64 {
        self.threshold_bytes.load(Ordering::Relaxed)
    }

    /// Retune the byte-lag threshold. Reachable from `ConfigManager` for
    /// `CONFIG SET replication-lag-threshold-bytes`.
    pub fn set_threshold_bytes(&self, bytes: u64) {
        self.threshold_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Max seconds since the last ACK before proactive disconnect (0 = disabled).
    pub fn threshold_secs(&self) -> u64 {
        self.threshold_secs.load(Ordering::Relaxed)
    }

    /// Retune the time-lag threshold. Reachable from `ConfigManager` for
    /// `CONFIG SET replication-lag-threshold-secs`.
    pub fn set_threshold_secs(&self, secs: u64) {
        self.threshold_secs.store(secs, Ordering::Relaxed);
    }
}

/// Primary replication handler.
///
/// Manages all replica connections and coordinates WAL streaming.
pub struct PrimaryReplicationHandler {
    /// Replication state (IDs and offsets)
    pub(crate) state: Arc<RwLock<ReplicationState>>,
    /// Path to the persisted replication state file, used by [`Self::save_state`].
    pub(crate) state_path: PathBuf,
    /// Replica tracker for ACKs and synchronous replication
    pub(crate) tracker: Arc<ReplicationTrackerImpl>,
    /// Channel for broadcasting WAL frames to all replicas
    pub(crate) wal_broadcast: broadcast::Sender<ReplicationFrame>,
    /// Optional RocksDB store for FULLRESYNC checkpoint streaming.
    pub(crate) rocks_store: Option<Arc<RocksStore>>,
    /// Directory for storing temporary checkpoint data.
    pub(crate) data_dir: PathBuf,
    /// Live proactive lag-disconnect thresholds, shared with every streaming
    /// session's lag policy (see [`LagThresholds`]).
    pub(crate) lag_thresholds: Arc<LagThresholds>,
    /// Cooldown after a proactive lag disconnect before allowing another.
    pub(crate) lag_cooldown: Duration,
    /// The replication backlog and its PSYNC grant decision. Owns the recent
    /// command buffer shared by partial-sync replay and split-brain
    /// reconciliation (see [`PartialSyncReplay`]).
    pub(crate) replay: PartialSyncReplay,
    /// Timeout for write_all to replicas (ms). 0 = disabled.
    pub(crate) write_timeout_ms: u64,
    /// Single owner of the replication-offset contract (live write position,
    /// per-replica acked offsets, and the persisted offset). All offset reads
    /// and the broadcast advance route through this seam instead of reaching
    /// into the tracker or `state.offset_at_save` directly.
    pub(crate) offsets: Arc<OffsetCoordinator>,
    /// Single owner of the WAIT quorum decision (offset snapshot, immediate
    /// check, GETACK solicitation policy, quorum-or-deadline wait). The
    /// connection handler asks this seam instead of assembling WAIT from
    /// tracker primitives.
    pub(crate) wait: WaitCoordinator,
    /// Set once [`Self::shutdown_downstream_sessions`] starts draining. A
    /// connection that was accepted just before the acceptors were aborted can
    /// still reach PSYNC afterwards; without this latch it would register a
    /// fresh session behind the drain and keep the storage engine open.
    pub(crate) draining: AtomicBool,
}

impl PrimaryReplicationHandler {
    /// Create a new primary replication handler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ReplicationIdentity,
        state_path: PathBuf,
        tracker: Arc<ReplicationTrackerImpl>,
        rocks_store: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
        lag_config: LagThresholdConfig,
        split_brain_config: SplitBrainBufferConfig,
        write_timeout_ms: u64,
    ) -> Self {
        let (wal_broadcast, _) = broadcast::channel(10000);
        let replay = PartialSyncReplay::new(&split_brain_config);
        let offsets = Arc::new(OffsetCoordinator::new(tracker.clone(), &identity));
        let wait = WaitCoordinator::new(offsets.clone(), tracker.clone());
        let lag_thresholds = Arc::new(LagThresholds::new(
            lag_config.threshold_bytes,
            lag_config.threshold_secs,
        ));
        // A node that recovers a nonzero offset is resuming a primary stint it
        // already had replicas for, so it claims history from there: a replica
        // reconnecting at exactly that offset gets an empty-tail `+CONTINUE`,
        // and every write from now on is stamped and buffered (`is_active`) so
        // no gap opens behind it. A genuinely fresh node stays unarmed until it
        // is promoted or a replica attaches, so standalone writes pay nothing.
        let recovered_offset = identity.current_offset();
        if recovered_offset > 0 {
            replay.arm_backlog_floor(recovered_offset);
        }
        Self {
            state: identity.state(),
            state_path,
            tracker,
            wal_broadcast,
            rocks_store,
            data_dir,
            lag_thresholds,
            lag_cooldown: lag_config.cooldown,
            replay,
            write_timeout_ms,
            offsets,
            wait,
            draining: AtomicBool::new(false),
        }
    }

    /// The WAIT quorum seam. The handler itself is the production
    /// [`crate::wait_coordinator::AckSolicitor`], so a caller typically runs
    /// `handler.wait_coordinator().wait_for_replicas(.., handler)`.
    pub fn wait_coordinator(&self) -> &WaitCoordinator {
        &self.wait
    }

    pub fn state(&self) -> ReplicationState {
        self.state.read().clone()
    }

    /// Begin a primary stint: mint a fresh replication id whose failover window
    /// is frozen at the **applied** offset, and open the backlog window there.
    ///
    /// This is the *replication* half of a promotion, kept next to the machinery
    /// it mutates. It is synchronous on purpose — the caller (`RoleManager`)
    /// holds a blocking mutex. It runs after the inbound replica stream has been
    /// signalled to stop, but frames already decoded may never have reached the
    /// keyspace, so the boundary comes from
    /// [`OffsetCoordinator::settle_at_applied`] — the received head is rewound
    /// down to the data this node actually holds. Freezing the received head
    /// instead would advertise history over a hole and hand a `+CONTINUE` to a
    /// replica whose data would then silently diverge. Returns the frozen
    /// boundary and the state snapshot that was persisted.
    ///
    /// Arming the backlog is not optional bookkeeping: without it the promoted
    /// node claims no history, so it would serve `+FULLRESYNC` to every replica
    /// that was already following the stream it just inherited. The buffer is
    /// reset first — anything left from a previous stint belongs to a history
    /// this node no longer heads.
    ///
    /// Fallible, and every failure leaves the node un-promoted: the staging area
    /// is disarmed *before* the identity is minted, and a failed persist rolls
    /// the identity back under the same lock that minted it. A promotion that
    /// could not be written down would be forgotten on the next boot while
    /// replicas kept following the minted id.
    ///
    /// The disarm stays first deliberately. Moving it past the persist would
    /// swap a harmless failure (the node stays a replica having dropped a
    /// staged checkpoint it would have to re-fetch) for the exact bug the
    /// disarm exists to fix: a persisted new identity with an armed checkpoint
    /// from the deposed primary still waiting to be installed on the next boot.
    pub fn begin_primary_stint(&self) -> std::io::Result<(u64, ReplicationState)> {
        // First, and before anything is minted: the node no longer follows the
        // history it inherited, so an inherited staged checkpoint must never be
        // re-installed under it (see [`crate::discard_staged_full_sync`]). If it
        // cannot be disarmed the promotion must not proceed.
        crate::discard_staged_full_sync(&self.data_dir)?;
        let boundary = self.offsets.settle_at_applied();
        // The mint, the persist and the rollback happen under ONE write lock:
        // the minted id must not be observable (`INFO replication`, a PSYNC
        // window check) on a node that is about to roll it back and stay a
        // replica. Holding a lock across file IO is deliberate here — it is one
        // small write, taken once per role change, and the alternative is a
        // window where the node advertises an identity it does not have.
        let snapshot = {
            let mut state = self.state.write();
            let previous = state.clone();
            state.new_replication_id(boundary);
            if boundary > state.offset_at_save {
                state.offset_at_save = boundary;
            }
            let snapshot = state.clone();
            if let Err(e) = self.save_snapshot(&snapshot) {
                *state = previous;
                tracing::error!(
                    error = %e,
                    boundary,
                    "Failed to persist the minted replication id; promotion aborted"
                );
                return Err(e);
            }
            snapshot
        };
        self.replay.reset_backlog();
        self.replay.arm_backlog_floor(boundary);
        tracing::info!(
            replication_id = %snapshot.replication_id,
            secondary_id = ?snapshot.secondary_id,
            boundary,
            "Primary stint started: minted replication id and armed backlog"
        );
        Ok((boundary, snapshot))
    }

    /// End a primary stint: close the backlog window and drop every downstream
    /// replica session.
    ///
    /// Redis's `replicationSetMaster` calls `disconnectSlaves` for the same
    /// reason: once this node follows someone else, the stream it was serving
    /// describes a history it no longer heads, and a full resync may rewind its
    /// offset below what it already handed out. Replicas must come back and
    /// PSYNC against the history this node ends up on. Returns how many sessions
    /// were signalled.
    ///
    /// Clients parked in WAIT are released here too, for the same reason and in
    /// the same breath: they are waiting for acknowledgments on the stream that
    /// is being torn down, so any count they could still reach would describe a
    /// history this node no longer heads.
    pub fn end_primary_stint(&self) -> usize {
        self.wait.fence_role_change();
        self.replay.reset_backlog();
        // Retire whatever applier the previous inbound stream left running, so
        // frames decoded under the old history cannot land on top of the
        // resync that is about to happen. Retiring (rather than aborting) lets
        // it finish the group it already claimed.
        self.offsets.retire_replica_applies();
        let disconnected = self.tracker.disconnect_all_replicas();
        tracing::info!(
            disconnected,
            "Primary stint ended: backlog cleared and downstream replicas disconnected"
        );
        disconnected
    }

    /// End every downstream replica session and wait for them to unregister.
    ///
    /// A PSYNC connection is served inline by the connection task that accepted
    /// it, and a streaming session only ends when its peer closes the socket or
    /// this node stops being a primary. Neither happens on shutdown: aborting
    /// the acceptors leaves established sessions streaming, so this node keeps
    /// writing frames after its shards have stopped and keeps its handle on the
    /// storage engine — which blocks a restart in the same process (the RocksDB
    /// LOCK is still held) and, in a cluster, delays the downstream replica's
    /// resync against whoever takes over.
    ///
    /// The drain latches first: aborting the acceptors stops *new* connections,
    /// but one accepted a moment earlier can still reach PSYNC while the drain
    /// runs, and a session registered behind the drain would outlive it. From
    /// here on [`Self::handle_psync`] refuses.
    ///
    /// Returns how many sessions were signalled. Waits up to `drain_timeout`
    /// for the registry to empty; a session that ignores the signal is logged
    /// rather than waited on forever.
    pub async fn shutdown_downstream_sessions(&self, drain_timeout: Duration) -> usize {
        self.draining.store(true, Ordering::Release);
        let signalled = self.tracker.disconnect_all_replicas();
        if signalled == 0 {
            return 0;
        }
        let deadline = tokio::time::Instant::now() + drain_timeout;
        while !self.tracker.get_all_replicas().is_empty() {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    remaining = self.tracker.get_all_replicas().len(),
                    "Replica sessions still registered after disconnect signal"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tracing::info!(
            signalled,
            "Downstream replica sessions torn down on shutdown"
        );
        signalled
    }

    pub fn tracker(&self) -> Arc<ReplicationTrackerImpl> {
        self.tracker.clone()
    }

    /// The live lag-threshold seam. Handing out the `Arc` (rather than only
    /// setters) lets `ConfigManager` hold the thresholds directly, the way it
    /// already holds `max_clients` and friends.
    pub fn lag_thresholds(&self) -> Arc<LagThresholds> {
        self.lag_thresholds.clone()
    }

    /// Retune the byte-lag disconnect threshold on every streaming session.
    /// Backs `CONFIG SET replication-lag-threshold-bytes`.
    pub fn set_lag_threshold_bytes(&self, bytes: u64) {
        self.lag_thresholds.set_threshold_bytes(bytes);
    }

    /// Retune the time-lag disconnect threshold on every streaming session.
    /// Backs `CONFIG SET replication-lag-threshold-secs`.
    pub fn set_lag_threshold_secs(&self, secs: u64) {
        self.lag_thresholds.set_threshold_secs(secs);
    }

    /// Persist the current replication identity + offset to the state file.
    ///
    /// The durable offset is reconciled from the live write position by the
    /// [`OffsetCoordinator`] (the broadcast path advances only the live offset,
    /// not `state.offset_at_save`). This couples offset durability to
    /// explicit save points (snapshot completion, graceful shutdown) rather than
    /// an fsync per write, mirroring Redis/Valkey, which persist repl-id +
    /// offset alongside the RDB instead of continuously.
    ///
    /// On restart the tracker is seeded from this file, so the reported
    /// `master_repl_offset` never silently rewinds to a stale boot value.
    pub fn save_state(&self) -> std::io::Result<()> {
        self.offsets.reconcile_for_persist().save(&self.state_path)
    }

    /// Persist an already-reconciled snapshot (the one
    /// [`Self::begin_primary_stint`] returned) to the same state file.
    pub fn save_snapshot(&self, snapshot: &ReplicationState) -> std::io::Result<()> {
        snapshot.save(&self.state_path)
    }

    /// Handle a new replica connection.
    ///
    /// Decides between partial and full sync, registers a [`ReplicaSession`],
    /// and drives it to completion. The session's exit handler unregisters
    /// itself and cleans up any per-sync resources regardless of which path
    /// the connection takes through `?`.
    pub async fn handle_psync(
        self: &Arc<Self>,
        stream: BoxedStream,
        addr: SocketAddr,
        replication_id: &str,
        offset: i64,
    ) -> io::Result<()> {
        // Refuse once the shutdown drain has started: a session opened now
        // would stream past the drain that was meant to end them all.
        if self.draining.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "primary is shutting down",
            ));
        }
        // One call resolves the whole PSYNC decision. [`PartialSyncReplay`] owns
        // both bounds of the continuable window — the upper bound + replid match
        // ([`ReplicationState::window_contains`]) and the lower bound (the
        // backlog eviction check the window primitive cannot make) — plus the
        // backlog tail to replay. The live stream head is read here from the
        // offset coordinator (the position advanced by `broadcast_command`); the
        // FULLRESYNC offset captured later in `handle_full` keys off the same
        // live value, so a granted partial and a granted full both correspond to
        // the data the replica will receive. `PSYNC ? -1` folds into the
        // `InitialSync` arm.
        let current_offset = self.offsets.current();
        // The guard lives in its own block: this future is spawned, so a
        // `parking_lot` guard merely *dropped* before the `.await` below would
        // still make the future `!Send`.
        let (current_repl_id, decision) = {
            let state = self.state.read();
            (
                state.replication_id.clone(),
                self.replay.handle_partial_sync_request(
                    &state,
                    replication_id,
                    offset.max(0) as u64,
                    current_offset,
                ),
            )
        };

        let session = self.tracker.register_replica(addr);
        let sync_kind = match decision {
            ReplayDecision::Continue(grant) => {
                tracing::info!(
                    replay_from = grant.replay_from,
                    resume = grant.resume_offset,
                    "PSYNC -> partial resync (+CONTINUE)"
                );
                SyncKind::Partial {
                    replay_from: grant.replay_from,
                }
            }
            ReplayDecision::FullResync(reason) => {
                // Surfaced for observability (Redis tracks sync_partial_err): an
                // operator can see *why* a replica fell back to a full resync.
                tracing::info!(?reason, "PSYNC -> full resync (+FULLRESYNC)");
                SyncKind::Full {
                    replication_id: current_repl_id,
                }
            }
        };
        session.run(stream, sync_kind, self.clone()).await
    }

    pub fn broadcast_frame(&self, frame: ReplicationFrame) {
        let _ = self.wal_broadcast.send(frame);
    }

    /// Untagged broadcast: control/global commands with no shard origin.
    ///
    /// Reachable through the `CONTROL_SHARD` frame tag. Kept as a crate-private
    /// helper (tests and any future control-only path) rather than a trait
    /// method, since production writes flow through the shard-tagged variant
    /// [`Self::broadcast_command_on_shard`]. This keeps the frame-emit trait
    /// surface to the tagged path only.
    #[cfg_attr(not(test), allow(dead_code))] // exercised by unit tests; kept per proposal 57
    pub(crate) fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64 {
        self.broadcast_tagged(CONTROL_SHARD, cmd_name, args)
    }

    /// Compute the split-brain divergence window from this handler's own offset
    /// coordinator and Replication Backlog. Pure read; no I/O, no telemetry, no
    /// logging.
    ///
    /// The window is `(start, end]` where `start` is the minimum offset acked
    /// across streaming replicas (`0` when there are none) and `end` is the live
    /// write position. `writes` is the backlog tail with `offset > start`,
    /// offset-ordered and non-destructive.
    ///
    /// Returns `None` when the node did not diverge — either it was caught up
    /// (`end <= start`) or nothing in the backlog sits past `start`. This is the
    /// one owner of the divergence predicate (`end > start && !writes.is_empty()`)
    /// and the `unwrap_or(0)` lower-bound floor; a `server`-side logger only
    /// formats and writes the record it returns.
    ///
    /// Both offset reads come from the one [`OffsetCoordinator`]; a concurrent
    /// `advance` between them only widens `end`, never truncates the write set
    /// below `start` (the extraction filter is `offset > start` against a
    /// non-destructive backlog), so no lock spanning the two reads is warranted.
    pub fn divergence_record(&self) -> Option<DivergenceRecord> {
        let start = self.offsets.min_acked().unwrap_or(0);
        let end = self.offsets.current();
        if end <= start {
            return None;
        }
        let writes = self.replay.extract_divergent_writes(start);
        if writes.is_empty() {
            return None;
        }
        Some(DivergenceRecord { start, end, writes })
    }

    /// Advance the offset, record into the backlog, and broadcast a single
    /// frame tagged with `shard_id`. Shared by [`Self::broadcast_command`] (the
    /// untagged path, `shard_id == CONTROL_SHARD`) and
    /// [`Self::broadcast_command_on_shard`] (data writes). The origin shard is
    /// stored in the backlog too, so a partial-resync replay tags the same shard
    /// the live stream did.
    fn broadcast_tagged(&self, shard_id: u16, cmd_name: &str, args: &[Bytes]) -> u64 {
        let resp_bytes = serialize_command_to_resp(cmd_name, args);
        let bytes_len = resp_bytes.len() as u64;
        // The single advance gate defines the byte unit; the primary no longer
        // hands a raw `.len()` a caller could mismeasure.
        let new_offset = self.offsets.advance(&resp_bytes);
        self.replay.record(new_offset, shard_id, resp_bytes.clone());
        let frame = ReplicationFrame::new_on_shard(new_offset, shard_id, resp_bytes);
        self.broadcast_frame(frame);
        tracing::trace!(
            cmd = cmd_name,
            bytes = bytes_len,
            offset = new_offset,
            shard = shard_id,
            "Broadcast command to replicas"
        );
        new_offset
    }

    pub async fn request_acks(&self) {
        let resp_bytes = ReplconfCodec::encode_getack();
        // GETACK is part of the command stream (Redis-compatible): it advances the
        // offset on both ends. The replica counts it via `frame_advance`, so the
        // primary must advance + stamp it too (and record it in the backlog like
        // any other command); stamping sequence 0 here would diverge the offsets.
        // Same advance gate as `broadcast_tagged`, so the unit is identical.
        let new_offset = self.offsets.advance(&resp_bytes);
        self.replay
            .record(new_offset, CONTROL_SHARD, resp_bytes.clone());
        self.broadcast_frame(ReplicationFrame::new(new_offset, resp_bytes));
    }

    pub fn replica_count(&self) -> usize {
        self.tracker.replica_count()
    }
    pub fn current_offset(&self) -> u64 {
        // The coordinator owns the live offset; it never returns a stale
        // persisted value.
        self.offsets.current()
    }

    pub fn replication_id(&self) -> String {
        self.state.read().replication_id.clone()
    }

    /// Get a shared reference to the replication state (IDs + offset).
    ///
    /// Used by INFO replication to report the live `master_replid`. Mirrors
    /// [`crate::replica::ReplicaReplicationHandler::shared_state`].
    pub fn shared_state(&self) -> Arc<RwLock<ReplicationState>> {
        self.state.clone()
    }

    /// The shared live-offset handle for the cluster bus's HealthProbe path.
    ///
    /// Vended by the [`OffsetCoordinator`] — the offset's single owner — rather
    /// than by the tracker, so the bus and every other reader observe the one
    /// atomic the `advance` gate writes.
    pub fn shared_offset(&self) -> Arc<AtomicU64> {
        self.offsets.shared_offset()
    }
}

impl ReplicationBroadcaster for PrimaryReplicationHandler {
    fn broadcast_command_on_shard(&self, shard_id: u16, cmd_name: &str, args: &[Bytes]) -> u64 {
        self.broadcast_tagged(shard_id, cmd_name, args)
    }

    fn is_active(&self) -> bool {
        // Zero connected replicas is NOT idle while the backlog still holds a
        // resume point: a replica that reconnects into that window is granted
        // `+CONTINUE`, so every write in the meantime must have an offset and a
        // backlog entry or the replica resumes past a hole and silently
        // diverges. Redis keeps its `repl_backlog` (and advances
        // `master_repl_offset`) after the last replica leaves for the same
        // reason. A primary that never had a replica has an empty backlog and
        // stays inactive, so standalone writes pay nothing.
        self.tracker.replica_count() > 0 || self.replay.has_resume_history()
    }
    fn current_offset(&self) -> u64 {
        self.offsets.current()
    }
}
