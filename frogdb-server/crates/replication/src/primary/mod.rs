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
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};

use crate::BoxedStream;
use crate::ReplicationBroadcaster;
use crate::frame::{CONTROL_SHARD, ReplconfCodec, ReplicationFrame, serialize_command_to_resp};
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
    /// Proactive lag-threshold disconnect configuration.
    pub(crate) lag_config: LagThresholdConfig,
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
}

impl PrimaryReplicationHandler {
    /// Create a new primary replication handler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state: ReplicationState,
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
        let state = Arc::new(RwLock::new(state));
        let offsets = Arc::new(OffsetCoordinator::new(tracker.clone(), state.clone()));
        let wait = WaitCoordinator::new(offsets.clone(), tracker.clone());
        Self {
            state,
            state_path,
            tracker,
            wal_broadcast,
            rocks_store,
            data_dir,
            lag_config,
            replay,
            write_timeout_ms,
            offsets,
            wait,
        }
    }

    /// The WAIT quorum seam. The handler itself is the production
    /// [`crate::wait_coordinator::AckSolicitor`], so a caller typically runs
    /// `handler.wait_coordinator().wait_for_replicas(.., handler)`.
    pub fn wait_coordinator(&self) -> &WaitCoordinator {
        &self.wait
    }

    pub async fn state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }
    pub fn tracker(&self) -> Arc<ReplicationTrackerImpl> {
        self.tracker.clone()
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
    pub async fn save_state(&self) -> std::io::Result<()> {
        let snapshot = self.offsets.reconcile_for_persist().await;
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
        let state = self.state.read().await;
        let current_repl_id = state.replication_id.clone();
        let decision = self.replay.handle_partial_sync_request(
            &state,
            replication_id,
            offset.max(0) as u64,
            current_offset,
        );
        drop(state);

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
    pub async fn current_offset(&self) -> u64 {
        // The coordinator owns the live offset; it never returns a stale
        // persisted value.
        self.offsets.current()
    }

    pub async fn replication_id(&self) -> String {
        self.state.read().await.replication_id.clone()
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
    pub fn current_offset_sync(&self) -> u64 {
        self.offsets.current()
    }
}

impl ReplicationBroadcaster for PrimaryReplicationHandler {
    fn broadcast_command_on_shard(&self, shard_id: u16, cmd_name: &str, args: &[Bytes]) -> u64 {
        self.broadcast_tagged(shard_id, cmd_name, args)
    }

    fn is_active(&self) -> bool {
        self.tracker.replica_count() > 0
    }
    fn current_offset(&self) -> u64 {
        self.offsets.current()
    }
}
