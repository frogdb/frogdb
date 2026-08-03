//! Per-replica session state machine.
//!
//! A `ReplicaSession` owns the entire lifecycle of one replica connection on
//! the primary side: from initial registration through optional FULLRESYNC to
//! live WAL streaming, and finally disconnect. The session drives its own
//! state transitions and runs cleanup in a single exit handler regardless of
//! which `?`-propagated error or task termination caused exit.
//!
//! # Phases
//!
//! ```text
//! Connecting ─► PreparingCheckpoint ─► StreamingCheckpoint ─► Streaming ─► Disconnecting
//!     │                                                          ▲
//!     └────────── partial sync (CONTINUE) ───────────────────────┘
//! ```
//!
//! The `Phase::Disconnecting` terminal is reached from any prior phase when
//! `run()` returns. The exit handler then unregisters the session, cleans up
//! any checkpoint directory, and logs the disconnect.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast;

use crate::BoxedStream;
use crate::frame::{ReplconfCodec, ReplicationFrame};
use crate::fullsync::{
    CheckpointChecksum, CheckpointFileHeader, CheckpointStreamCodec, FullSyncMetadata,
    calculate_bytes_checksum, calculate_file_checksum, stream_file_to_writer,
};
use crate::primary::{LAG_CHECK_INTERVAL, LagThresholds, PrimaryReplicationHandler};
use crate::tracker::ReplicationTrackerImpl;

/// Lifecycle phase of a replica session.
///
/// Each session moves monotonically forward through its phases. External
/// observers (INFO replication, ROLE, cluster bus) read the phase via
/// [`ReplicaSession::phase`] or via [`ReplicaInfo`] snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Registered, awaiting server-side decision (FULLRESYNC vs CONTINUE).
    Connecting,
    /// `spawn_blocking(rocks.create_checkpoint)` is in flight.
    PreparingCheckpoint,
    /// Sending checkpoint files to the replica.
    StreamingCheckpoint,
    /// Live WAL stream is flowing; partial syncs enter directly here.
    Streaming,
    /// Terminal — cleanup is running.
    Disconnecting,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Connecting => write!(f, "connecting"),
            Phase::PreparingCheckpoint => write!(f, "preparing-checkpoint"),
            Phase::StreamingCheckpoint => write!(f, "streaming-checkpoint"),
            Phase::Streaming => write!(f, "streaming"),
            Phase::Disconnecting => write!(f, "disconnecting"),
        }
    }
}

/// Capabilities advertised by the replica during REPLCONF capa negotiation.
#[derive(Debug, Clone, Default)]
pub struct ReplicaCapabilities {
    /// Supports EOF marker in RDB transfer.
    pub eof: bool,
    /// Supports PSYNC2 protocol.
    pub psync2: bool,
}

impl ReplicaCapabilities {
    pub fn parse_capa(capabilities: &[&str]) -> Self {
        let mut caps = Self::default();
        for cap in capabilities {
            match *cap {
                "eof" => caps.eof = true,
                "psync2" => caps.psync2 = true,
                _ => {}
            }
        }
        caps
    }
}

/// Snapshot view of a replica session for read consumers (INFO, ROLE, cluster bus).
///
/// Built on demand via [`ReplicaSession::snapshot`]. The snapshot is decoupled
/// from the session so callers don't need to hold any locks while reading.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub id: u64,
    pub address: SocketAddr,
    pub listening_port: u16,
    pub acked_offset: u64,
    pub last_ack_time: Instant,
    pub connected_at: Instant,
    pub phase: Phase,
    pub capabilities: ReplicaCapabilities,
    pub replica_version: Option<String>,
}

impl ReplicaInfo {
    /// Returns true if this replica is in the live-streaming phase.
    pub fn is_streaming(&self) -> bool {
        matches!(self.phase, Phase::Streaming)
    }
}

/// What sync flow to drive for this session.
#[derive(Debug)]
pub enum SyncKind {
    /// Partial resync (`+CONTINUE`): the replica's replid + offset are inside the
    /// continuable window AND the backlog still covers `(replay_from, current]`.
    /// The session replays that backlog tail before joining the live tail.
    /// `replay_from` is the replica's offset; the streamer re-extracts the tail
    /// after subscribing to the broadcast so no write made during the handshake
    /// slips through the gap (see [`ReplicaSession::start_streaming`]).
    Partial { replay_from: u64 },
    /// Send a full database snapshot. The snapshot's replication offset is
    /// captured from the live tracker at checkpoint-cut time inside
    /// [`ReplicaSession::handle_full`], not threaded in here, so it corresponds
    /// to the data actually contained in the checkpoint.
    Full { replication_id: String },
}

struct SessionInner {
    phase: Phase,
    last_ack_time: Instant,
    listening_port: u16,
    capabilities: ReplicaCapabilities,
    replica_version: Option<String>,
    /// Set once the checkpoint dir has been created and is owed cleanup.
    sync_checkpoint_path: Option<PathBuf>,
    /// Total bytes for the in-flight checkpoint stream (set when files are enumerated).
    sync_total_bytes: u64,
    /// Wall clock when the checkpoint stream started (for rate logging).
    sync_started_at: Option<Instant>,
}

/// A primary-side session that owns one replica's lifecycle from registration
/// to disconnect.
///
/// Constructed by [`crate::tracker::ReplicationTrackerImpl::register_replica`] and
/// driven to completion by [`ReplicaSession::run`]. A single exit handler in
/// `run()` runs cleanup (registry removal, checkpoint dir delete, disconnect log)
/// regardless of which path returned an error.
pub struct ReplicaSession {
    id: u64,
    address: SocketAddr,
    connected_at: Instant,

    // Hot atomic counters — written from the read/write tasks and queried by
    // INFO/ROLE consumers without needing to lock the inner state.
    acked_offset: AtomicU64,
    sync_bytes_transferred: AtomicU64,

    /// Primary-initiated teardown signal (see [`Self::request_disconnect`]).
    /// `notify_one`, not `notify_waiters`, so a request that races ahead of the
    /// streaming loop's `notified()` is stored as a permit rather than lost.
    disconnect: tokio::sync::Notify,

    inner: RwLock<SessionInner>,
}

impl ReplicaSession {
    /// Create a new session in the `Connecting` phase.
    pub fn new(id: u64, address: SocketAddr) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            id,
            address,
            connected_at: now,
            acked_offset: AtomicU64::new(0),
            sync_bytes_transferred: AtomicU64::new(0),
            disconnect: tokio::sync::Notify::new(),
            inner: RwLock::new(SessionInner {
                phase: Phase::Connecting,
                last_ack_time: now,
                listening_port: 0,
                capabilities: ReplicaCapabilities::default(),
                replica_version: None,
                sync_checkpoint_path: None,
                sync_total_bytes: 0,
                sync_started_at: None,
            }),
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn connected_at(&self) -> Instant {
        self.connected_at
    }
    pub fn acked_offset(&self) -> u64 {
        self.acked_offset.load(Ordering::Acquire)
    }
    pub fn last_ack_time(&self) -> Instant {
        self.inner.read().last_ack_time
    }
    pub fn phase(&self) -> Phase {
        self.inner.read().phase
    }
    pub fn is_streaming(&self) -> bool {
        matches!(self.phase(), Phase::Streaming)
    }
    pub fn listening_port(&self) -> u16 {
        self.inner.read().listening_port
    }
    pub fn capabilities(&self) -> ReplicaCapabilities {
        self.inner.read().capabilities.clone()
    }
    pub fn replica_version(&self) -> Option<String> {
        self.inner.read().replica_version.clone()
    }

    /// Ask this session's streaming loop to tear itself down.
    ///
    /// Redis's `replicationSetMaster` calls `disconnectSlaves` when a primary
    /// becomes a replica: the stream this node was serving describes a history
    /// it no longer heads, so the downstream replicas must resync against
    /// whoever does. The session ends as if the socket had closed — the replica
    /// reconnects and PSYNCs afresh.
    pub fn request_disconnect(&self) {
        self.disconnect.notify_one();
    }

    /// Resolves once [`Self::request_disconnect`] has been called.
    async fn disconnect_requested(&self) {
        self.disconnect.notified().await;
    }

    /// Build a snapshot for read-only consumers (INFO, ROLE, cluster bus).
    pub fn snapshot(&self) -> ReplicaInfo {
        let inner = self.inner.read();
        ReplicaInfo {
            id: self.id,
            address: self.address,
            listening_port: inner.listening_port,
            acked_offset: self.acked_offset.load(Ordering::Acquire),
            last_ack_time: inner.last_ack_time,
            connected_at: self.connected_at,
            phase: inner.phase,
            capabilities: inner.capabilities.clone(),
            replica_version: inner.replica_version.clone(),
        }
    }

    /// Record a REPLCONF ACK from the replica.
    ///
    /// Always refreshes `last_ack_time` (any ACK proves liveness, even on an
    /// idle primary). Returns `true` only if the offset advanced — callers
    /// use this to decide whether to notify WAIT waiters via the broadcast channel.
    pub fn record_ack(&self, sequence: u64) -> bool {
        // Refresh liveness regardless
        let now = Instant::now();
        self.inner.write().last_ack_time = now;
        // Conditional offset update
        let prev = self.acked_offset.load(Ordering::Acquire);
        if sequence > prev {
            self.acked_offset.store(sequence, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Seed the initial acked position when the session enters streaming.
    ///
    /// Unlike [`Self::record_ack`], this is a *primary bookkeeping* fact — where
    /// this replica resumed from (its PSYNC offset for a partial resync, or the
    /// checkpoint's `snapshot_offset` for a full resync) — not a replica
    /// acknowledgement read off the wire. It advances the same monotonic
    /// `acked_offset` atomic (so there is exactly one source of truth for the
    /// acked position) and shares `record_ack`'s "only advances forward, reports
    /// whether it did" mechanics; the two entry points remain distinct call sites
    /// only because the primary itself, not the replica, supplies this value.
    ///
    /// The lag clock (`last_ack_time`) is reset to the resume instant: the
    /// time-based lag threshold must measure from where streaming (re)started,
    /// not from registration — a long FULLRESYNC checkpoint stream would
    /// otherwise trip it immediately.
    ///
    /// Returns `true` iff the offset advanced (`offset > prev`), mirroring
    /// [`Self::record_ack`]'s return contract — callers use this to decide
    /// whether to notify WAIT waiters via the broadcast channel.
    pub fn seed_acked_position(&self, offset: u64) -> bool {
        self.inner.write().last_ack_time = Instant::now();
        let prev = self.acked_offset.load(Ordering::Acquire);
        if offset > prev {
            self.acked_offset.store(offset, Ordering::Release);
            true
        } else {
            false
        }
    }

    fn set_phase(&self, phase: Phase) {
        let mut inner = self.inner.write();
        let old = inner.phase;
        inner.phase = phase;
        drop(inner);
        if old != phase {
            tracing::debug!(
                replica_id = self.id,
                old_phase = %old,
                new_phase = %phase,
                "Replica phase change"
            );
        }
    }

    /// Test-only: force the session's phase without driving `run()`.
    ///
    /// Production code transitions phases inside `ReplicaSession::run`; this
    /// helper exists so unit/integration tests in other crates can stage a
    /// session in a particular phase without standing up the full I/O loop.
    #[doc(hidden)]
    pub fn force_phase_for_test(&self, phase: Phase) {
        self.set_phase(phase);
    }

    /// Test-only: pretend the last ACK landed `by` ago.
    ///
    /// The freshness gates ([`crate::ack_is_fresh`] and its callers) compare
    /// `last_ack_time.elapsed()` against a window. Without this hook a test for
    /// "a replica that has gone silent" would have to *sleep* the window, which
    /// makes the window a lower bound on suite runtime and makes the assertion
    /// racy on a loaded machine. Backdating the instant instead makes staleness
    /// exact and instantaneous.
    #[doc(hidden)]
    pub fn backdate_last_ack_for_test(&self, by: Duration) {
        let mut inner = self.inner.write();
        inner.last_ack_time = Instant::now() - by;
    }

    /// Drive the session to completion.
    ///
    /// This is the single owner of the session lifecycle. It dispatches to
    /// [`Self::handle_partial`] or [`Self::handle_full`] based on `sync_kind`,
    /// then enters [`Self::start_streaming`]. Regardless of where execution
    /// exits — `?` propagation, panic, or normal completion — the exit handler
    /// runs registry removal, checkpoint cleanup, and the disconnect log.
    pub async fn run(
        self: Arc<Self>,
        stream: BoxedStream,
        sync_kind: SyncKind,
        handler: Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        let result = self.clone().run_inner(stream, sync_kind, &handler).await;

        // Single exit handler — runs regardless of which `?` returned.
        self.set_phase(Phase::Disconnecting);

        // Best-effort checkpoint dir cleanup. Only set when a checkpoint was
        // actually created, so NotFound shouldn't occur in practice.
        let path = self.inner.read().sync_checkpoint_path.clone();
        if let Some(p) = path
            && let Err(e) = fs::remove_dir_all(&p).await
        {
            tracing::warn!(
                checkpoint_path = %p.display(),
                error = %e,
                "Failed to clean up checkpoint directory"
            );
        }

        // Drop the session from the registry, last: leaving the registry is
        // what tells a waiting
        // [`PrimaryReplicationHandler::shutdown_downstream_sessions`] that this
        // session is done with its per-sync resources, so the removal must
        // follow the cleanup above rather than precede it.
        handler.tracker.unregister_replica(self.id);

        tracing::info!(
            replica_id = self.id,
            addr = %self.address,
            "Replica disconnected"
        );

        result
    }

    async fn run_inner(
        self: Arc<Self>,
        stream: BoxedStream,
        sync_kind: SyncKind,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        match sync_kind {
            SyncKind::Partial { replay_from } => {
                self.handle_partial(stream, replay_from, handler).await
            }
            SyncKind::Full { replication_id } => {
                self.handle_full(stream, replication_id, handler).await
            }
        }
    }

    /// Drive a partial resync (`+CONTINUE`).
    ///
    /// Writes the `+CONTINUE` reply, then hands off to [`Self::start_streaming`]
    /// with `replay_from` so the backlog tail `(replay_from, current]` is
    /// streamed *before* the live tail. The replica side already reads frames off
    /// the same stream after `+CONTINUE` (`replica/connection.rs` →
    /// `stream_replication`), so the replayed frames arrive exactly like live
    /// ones — no replica-side protocol change.
    async fn handle_partial(
        self: Arc<Self>,
        mut stream: BoxedStream,
        replay_from: u64,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        let replication_id = handler.state.read().replication_id.clone();
        let response = format!("+CONTINUE {}\r\n", replication_id);
        stream.write_all(response.as_bytes()).await?;
        self.start_streaming(stream, handler, replay_from).await
    }

    async fn handle_full(
        self: Arc<Self>,
        mut stream: BoxedStream,
        replication_id: String,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        // Capture the live stream head from the tracker *before* cutting the
        // checkpoint, and use this single value for both the FULLRESYNC reply
        // and the checkpoint metadata so the granted offset and the snapshot
        // data correspond (the critical invariant: offset must match the data
        // the replica loads).
        //
        // Write ordering guarantees the safe direction. Each write's WAL entry is
        // enqueued in the command pipeline *before* `broadcast_command` advances
        // the tracker, and the pre-checkpoint hook below drains those queues into
        // RocksDB, so every write counted in `snapshot_offset` is captured when
        // the checkpoint is cut. Conversely, writes that land between this capture and
        // the cut only *add* data to the checkpoint, raising data past the
        // offset. The result is `offset <= data`: the checkpoint can never be
        // missing data the offset claims to include. Capturing after the cut
        // would invert this (offset > data) and silently lose writes — the same
        // shutdown-ordering principle as commit 17f01c9d. This mirrors Redis,
        // where the FULLRESYNC offset is the master_repl_offset captured at fork
        // time and the RDB corresponds to exactly that point.
        //
        // The writes in `(snapshot_offset, current_at_handoff]` — those broadcast
        // while the checkpoint is cut and streamed — are NOT in the checkpoint.
        // They are replayed from the backlog at the streaming handoff (F1 fix):
        // `start_streaming` subscribes to the broadcast first, then replays
        // `(snapshot_offset, current]` before the live tail, closing the window
        // that previously dropped those writes (the broadcast tail only carries
        // frames sent *after* the subscribe).
        let snapshot_offset = handler.offsets.current();

        let response = format!("+FULLRESYNC {} {}\r\n", replication_id, snapshot_offset);
        stream.write_all(response.as_bytes()).await?;

        if let Some(rocks) = handler.rocks_store.as_ref().cloned() {
            self.set_phase(Phase::PreparingCheckpoint);
            let checkpoint_path = handler.data_dir.join(format!("fullsync_{}", self.id));

            // The checkpoint is a snapshot of what RocksDB *holds*, and a write
            // is acknowledged as soon as it is staged in its shard's WAL
            // flush-engine (default durability commits to RocksDB on a later
            // size/timeout trigger). Cut without draining those engines, the
            // checkpoint silently omits the primary's most recent writes — and
            // for a full resync that is unrecoverable: with no replica attached
            // when they were made, they were never broadcast, so there is no
            // backlog tail to replay them from and the replica is missing them
            // forever. So: drain first, cut second. Same contract the snapshot
            // coordinator's pre-snapshot hook honours (issue 13).
            if let Some(drain) = handler.pre_checkpoint_hook()
                && let Err(e) = drain().await
            {
                // A shard that could not be drained leaves its acknowledged
                // writes out of the checkpoint, and for a full resync that hole
                // is permanent (nothing in the backlog replays them). Fail the
                // sync — the replica retries `PSYNC ? -1` on its reconnect
                // backoff — rather than shipping a dataset known to be missing
                // writes. Same reasoning as the checkpoint failure below, and
                // likewise nothing is staged, so there is nothing to clean up.
                tracing::error!(error = %e, "Pre-checkpoint drain failed for FULLRESYNC");
                return Err(io::Error::other(format!(
                    "pre-checkpoint drain failed for FULLRESYNC: {e}"
                )));
            }

            let path_clone = checkpoint_path.clone();
            let result = tokio::task::spawn_blocking(move || rocks.create_checkpoint(&path_clone))
                .await
                .map_err(io::Error::other)?;

            match result {
                Err(e) => {
                    // Checkpoint creation failed. There is nothing else this
                    // node can honestly put on the wire: the replica has already
                    // been granted `snapshot_offset`, and any payload that is
                    // not this primary's dataset would leave it streaming deltas
                    // onto a keyspace that never took the base snapshot (issue
                    // 67 — that is exactly what the old minimal-RDB fallback
                    // did). Failing the sync drops the connection, and the
                    // replica retries `PSYNC ? -1` on its reconnect backoff.
                    //
                    // sync_checkpoint_path is intentionally NOT set, so the exit
                    // handler won't try to clean a directory that doesn't exist.
                    tracing::error!(error = %e, "Failed to create checkpoint for FULLRESYNC");
                    return Err(io::Error::other(format!(
                        "failed to create checkpoint for FULLRESYNC: {e}"
                    )));
                }
                Ok(()) => {
                    // Mark for cleanup *only after* successful creation.
                    self.inner.write().sync_checkpoint_path = Some(checkpoint_path.clone());
                    self.set_phase(Phase::StreamingCheckpoint);
                    self.inner.write().sync_started_at = Some(Instant::now());
                    self.stream_checkpoint(
                        &mut stream,
                        &checkpoint_path,
                        &replication_id,
                        snapshot_offset,
                    )
                    .await?;
                }
            }
        } else {
            // No RocksDB to checkpoint (`persistence.enabled = false`), which
            // does not excuse this primary from shipping its dataset: Redis
            // serves a diskless full sync by serializing the keyspace straight
            // to the socket, and so does this branch (issue 67).
            self.stream_live_dataset(&mut stream, handler, &replication_id, snapshot_offset)
                .await?;
        }

        tracing::info!(
            replica_id = self.id,
            addr = %self.address,
            offset = snapshot_offset,
            "Completed FULLRESYNC"
        );

        // Replay any writes that landed during checkpoint creation/transfer
        // (the F1 handoff window) from the backlog before the live tail.
        self.start_streaming(stream, handler, snapshot_offset).await
    }

    /// Stream checkpoint files to the replica.
    ///
    /// The on-wire grammar is owned by [`CheckpointStreamCodec`]; this method
    /// drives it: prelude, then a per-file header + raw payload for each file,
    /// then the trailing metadata frame.
    async fn stream_checkpoint(
        &self,
        stream: &mut BoxedStream,
        checkpoint_path: &Path,
        replication_id: &str,
        replication_offset: u64,
    ) -> io::Result<()> {
        // Enumerate all files in the checkpoint directory.
        let mut files: Vec<(String, u64, PathBuf)> = Vec::new();
        let mut total_size = 0u64;
        let mut dir = fs::read_dir(checkpoint_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let metadata = fs::metadata(&path).await?;
                let file_name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let file_size = metadata.len();
                total_size += file_size;
                files.push((file_name, file_size, path));
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));

        self.inner.write().sync_total_bytes = total_size;
        self.sync_bytes_transferred.store(0, Ordering::Release);

        tracing::info!(
            replica_id = self.id,
            file_count = files.len(),
            total_size = total_size,
            "Streaming checkpoint to replica"
        );

        // Envelope prelude: marker + file count.
        CheckpointStreamCodec::write_prelude(stream, files.len()).await?;

        // Bodies: per-file header via the codec, then the raw payload bytes.
        // The combined checksum (owned by `CheckpointChecksum`) is fed one file
        // at a time, in the same order the codec frames them.
        let mut combined = CheckpointChecksum::new();
        for (file_name, file_size, file_path) in &files {
            CheckpointStreamCodec::write_file_header(
                stream,
                &CheckpointFileHeader {
                    name: file_name.clone(),
                    size: *file_size,
                },
            )
            .await?;
            let bytes_written =
                stream_file_to_writer(file_path, stream, Some(&self.sync_bytes_transferred))
                    .await?;
            let file_hash = calculate_file_checksum(file_path).await?;
            combined.update_file(file_name, &file_hash);
            tracing::debug!(
                file = %file_name,
                size = bytes_written,
                progress = format!("{:.1}%", self.progress_percent()),
                "Streamed checkpoint file"
            );
        }
        let checksum = combined.finalize();

        let metadata = FullSyncMetadata {
            rdb_size: total_size,
            checksum,
            replication_id: replication_id.to_string(),
            replication_offset,
        };
        CheckpointStreamCodec::write_metadata(stream, &metadata).await?;

        let elapsed = self
            .inner
            .read()
            .sync_started_at
            .map(|t| t.elapsed())
            .unwrap_or_default();
        let rate_mbps = if elapsed.as_secs_f64() > 0.0 {
            (total_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64()
        } else {
            0.0
        };
        tracing::info!(
            replica_id = self.id,
            files = files.len(),
            total_bytes = total_size,
            elapsed_ms = elapsed.as_millis() as u64,
            rate_mbps = format!("{:.2}", rate_mbps),
            "Checkpoint streaming complete"
        );

        Ok(())
    }

    /// Progress as a percentage (0-100). Returns 100 if no bytes are expected yet.
    fn progress_percent(&self) -> f64 {
        let total = self.inner.read().sync_total_bytes;
        if total == 0 {
            return 100.0;
        }
        let transferred = self.sync_bytes_transferred.load(Ordering::Relaxed);
        (transferred as f64 / total as f64) * 100.0
    }

    /// Stream the live keyspace to the replica: the full-sync payload of a
    /// primary that has no RocksDB to checkpoint (issue 67).
    ///
    /// Structurally identical to [`Self::stream_checkpoint`] — same envelope,
    /// same combined checksum, same trailing metadata — with per-shard dataset
    /// blobs in place of checkpoint files. Only the prelude marker differs, so
    /// the replica knows to install the bodies directly instead of staging them
    /// to a disk it is not using either.
    ///
    /// **No pre-checkpoint drain.** The drain exists to push acknowledged writes
    /// out of the shards' flush engines *into RocksDB* before the cut. Here the
    /// shards themselves are the source, so an acknowledged write is in the
    /// export by construction; there is nothing to wait for.
    ///
    /// **`replication_offset` is captured by the caller before this runs**, which
    /// keeps the `offset <= data` direction the checkpoint path relies on: writes
    /// landing during the export only add data, and `(offset, current]` is
    /// replayed from the backlog at the streaming handoff.
    async fn stream_live_dataset(
        &self,
        stream: &mut BoxedStream,
        handler: &Arc<PrimaryReplicationHandler>,
        replication_id: &str,
        replication_offset: u64,
    ) -> io::Result<()> {
        // No source wired means no way to read the keyspace, and a full resync
        // with no dataset is precisely the bug: fail the sync instead.
        let Some(source) = handler.live_snapshot_source() else {
            return Err(io::Error::other(
                "no live-snapshot source wired: a primary without persistence cannot serve \
                 a FULLRESYNC",
            ));
        };

        self.set_phase(Phase::PreparingCheckpoint);
        let blobs = source().await?;

        let total_size: u64 = blobs.iter().map(|b| b.len() as u64).sum();
        self.inner.write().sync_total_bytes = total_size;
        self.sync_bytes_transferred.store(0, Ordering::Release);
        self.set_phase(Phase::StreamingCheckpoint);
        self.inner.write().sync_started_at = Some(Instant::now());

        tracing::info!(
            replica_id = self.id,
            blob_count = blobs.len(),
            total_size,
            "Streaming live dataset to replica"
        );

        CheckpointStreamCodec::write_snapshot_prelude(stream, blobs.len()).await?;

        // The blob names are positional (`shard-<n>.dataset`) and feed the
        // combined checksum in wire order, exactly as filenames do for a
        // checkpoint — so a reordered or dropped blob fails verification.
        let mut combined = CheckpointChecksum::new();
        for (shard_id, blob) in blobs.iter().enumerate() {
            let name = format!("shard-{shard_id}.dataset");
            CheckpointStreamCodec::write_file_header(
                stream,
                &CheckpointFileHeader {
                    name: name.clone(),
                    size: blob.len() as u64,
                },
            )
            .await?;
            stream.write_all(blob).await?;
            self.sync_bytes_transferred
                .fetch_add(blob.len() as u64, Ordering::Relaxed);
            combined.update_file(&name, &calculate_bytes_checksum(blob));
        }

        CheckpointStreamCodec::write_metadata(
            stream,
            &FullSyncMetadata {
                rdb_size: total_size,
                checksum: combined.finalize(),
                replication_id: replication_id.to_string(),
                replication_offset,
            },
        )
        .await?;

        tracing::info!(
            replica_id = self.id,
            blobs = blobs.len(),
            total_bytes = total_size,
            elapsed_ms = self
                .inner
                .read()
                .sync_started_at
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or_default(),
            "Live dataset streaming complete"
        );
        Ok(())
    }

    /// Enter the live-streaming phase: replay the backlog handoff tail, then
    /// subscribe to WAL frames and forward them to the replica while a read task
    /// consumes REPLCONF ACKs.
    ///
    /// `replay_from` is the offset the replica already holds (its PSYNC offset
    /// for a partial resync, or the checkpoint's `snapshot_offset` for a full
    /// resync). The backlog tail `(replay_from, current]` is streamed before the
    /// live tail so no write is lost at the handoff:
    ///
    /// 1. Subscribe to `wal_broadcast` **first** — every frame broadcast from
    ///    here on is captured, so nothing can fall between the replayed tail and
    ///    the live tail.
    /// 2. Read the live head and replay `(replay_from, current]` from the
    ///    backlog. Subscribing before reading the head guarantees coverage: any
    ///    write whose broadcast preceded the subscribe is in the backlog; any
    ///    that followed is in the live receiver.
    /// 3. Forward the live tail, skipping frames at or below the replayed
    ///    `resume_offset` so the overlap between the two is sent exactly once.
    ///
    /// This single path fixes both the full-sync handoff gap (F1) and the
    /// partial-sync gap (F2). When the backlog is disabled or empty, the replay
    /// is a no-op and the behaviour reduces to forwarding the live tail.
    async fn start_streaming(
        self: Arc<Self>,
        mut stream: BoxedStream,
        handler: &Arc<PrimaryReplicationHandler>,
        replay_from: u64,
    ) -> io::Result<()> {
        self.set_phase(Phase::Streaming);

        // Subscribe BEFORE reading the head / extracting the backlog so the live
        // receiver and the replayed tail cannot leave a gap (step 1 above).
        let mut wal_rx = handler.wal_broadcast.subscribe();

        // Replay the backlog handoff tail `(replay_from, current]` ahead of the
        // live tail (steps 2). `resume_offset` tracks the last offset actually
        // streamed; the live tail dedups against it (step 3).
        let current = handler.offsets.current();
        let mut resume_offset = replay_from;
        // The window can close between the grant and here — the grant is written
        // before the checkpoint is cut, and the whole payload transfer sits in
        // between, which on a busy primary is long enough to evict the resume
        // point outright. A short tail would be streamed as if it were the whole
        // thing (the live tail then dedups against the last frame actually sent),
        // leaving the replica a permanent hole with a contiguous-looking offset,
        // so a truncated window abandons the resume instead: the link drops, the
        // replica reconnects, and its `PSYNC` is answered `+FULLRESYNC` because
        // the same floor check fails at grant time too (round-2 issue 52).
        let tail = handler
            .replay
            .extract_backlog(replay_from, current)
            .map_err(|truncated| {
                tracing::warn!(
                    replica_id = self.id,
                    replay_from,
                    error = %truncated,
                    "Backlog window closed before the resume could be streamed; \
                     dropping the link so the replica comes back for a full sync"
                );
                io::Error::new(io::ErrorKind::InvalidData, truncated)
            })?;
        for (offset, shard_id, payload) in tail {
            let encoded = ReplicationFrame::new_on_shard(offset, shard_id, payload).encode()?;
            stream.write_all(&encoded).await?;
            resume_offset = offset;
        }
        // Seed where the replica resumed, so WAIT waiters and the lag monitor
        // start from the resumed position. This is a primary bookkeeping fact
        // ("where this replica started"), not a replica ACK, so it uses the
        // coordinator's `seed_replica_position` verb rather than
        // `ingest_replica_ack` — but it advances the same monotonic atomic and,
        // if a reconnecting replica already resumed at/past a WAIT target, it
        // notifies WAIT waiters exactly like a genuine ACK would (a stale/
        // duplicate seed that doesn't advance the offset stays silent).
        handler
            .offsets
            .seed_replica_position(self.id, resume_offset);

        let (mut read_half, mut write_half) = tokio::io::split(stream);

        let read_offsets = handler.offsets.clone();
        let read_replica_id = self.id;
        let read_task = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);
            loop {
                match read_half.read_buf(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        while let Some((ack_offset, consumed)) = ReplconfCodec::parse_ack(&buf) {
                            read_offsets.ingest_replica_ack(read_replica_id, ack_offset);
                            buf.advance(consumed);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Error reading from replica");
                        break;
                    }
                }
            }
        });

        let lag_tracker = handler.tracker.clone();
        let lag_replica_id = self.id;
        let mut lag_policy = LagPolicy::new(handler.lag_thresholds.clone(), handler.lag_cooldown);
        let write_timeout = if handler.write_timeout_ms > 0 {
            Some(Duration::from_millis(handler.write_timeout_ms))
        } else {
            None
        };

        let write_task = tokio::spawn(async move {
            // Single live frame source: `wal_broadcast`. Subscribe, replay,
            // forward-or-break.
            loop {
                match wal_rx.recv().await {
                    Ok(frame) => {
                        // Dedup the handoff overlap: frames already sent via the
                        // backlog replay (sequence <= resume_offset) must not be
                        // re-sent, or the replica double-applies.
                        if frame.sequence <= resume_offset {
                            continue;
                        }
                        let encoded = match frame.encode() {
                            Ok(encoded) => encoded,
                            Err(e) => {
                                // Unreplicable, and no reconnect can change that
                                // — the same frame comes back from the backlog.
                                // Drop the link loudly rather than put a frame
                                // with a wrapped length prefix on the wire.
                                tracing::error!(
                                    replica_id = lag_replica_id,
                                    sequence = frame.sequence,
                                    error = %e,
                                    "Replication frame exceeds the link's frame ceiling; dropping the replica link"
                                );
                                break;
                            }
                        };
                        if let Forward::Break =
                            forward_frame(&mut write_half, &encoded, write_timeout, lag_replica_id)
                                .await
                        {
                            break;
                        }
                        if let Some(breach) =
                            lag_policy.should_disconnect(&lag_tracker, lag_replica_id)
                        {
                            tracing::warn!(
                                replica_id = lag_replica_id,
                                byte_exceeded = breach.byte_exceeded,
                                time_exceeded = breach.time_exceeded,
                                "Replica exceeded lag threshold, disconnecting for FULLRESYNC"
                            );
                            lag_tracker.record_lag_disconnect(lag_replica_id);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            replica_id = lag_replica_id,
                            lagged = n,
                            "Replica lagged in WAL stream, disconnecting for resync"
                        );
                        break;
                    }
                }
            }
        });

        // When either half ends — the write task after a lag / write-timeout
        // disconnect, or the read task on a replica-initiated close — abort the
        // sibling so BOTH halves of the split stream drop and the TCP socket
        // actually closes. Dropping the `JoinHandle`s alone does *not* abort the
        // tasks: a detached read task would otherwise keep its half alive after
        // a lag-disconnect, leaving the replica in a zombie half-open link
        // (unregistered here, but never sent a FIN, so it never resyncs).
        // The third exit is primary-initiated: a demotion asks every downstream
        // session to end so the replicas resync against the new primary.
        let mut read_task = read_task;
        let mut write_task = write_task;
        tokio::select! {
            _ = &mut read_task => { write_task.abort(); }
            _ = &mut write_task => { read_task.abort(); }
            _ = self.disconnect_requested() => {
                tracing::info!(
                    replica_id = self.id,
                    "Disconnecting replica session on role change"
                );
                read_task.abort();
                write_task.abort();
            }
        }
        Ok(())
    }
}

/// Outcome of a single frame write to a replica.
enum Forward {
    /// The frame was written; keep streaming.
    Continue,
    /// The session must end (write timeout or I/O error); the caller stops
    /// streaming.
    Break,
}

/// Write one encoded frame to the replica, honoring the optional write timeout.
///
/// This is the single home for "send a frame to a replica": with the dead
/// per-replica frame channel removed there is one live frame source, so the
/// write+timeout+error path is defined exactly once here instead of being
/// duplicated across two `select!` arms.
async fn forward_frame(
    write_half: &mut (impl AsyncWrite + Unpin),
    encoded: &[u8],
    timeout: Option<Duration>,
    replica_id: u64,
) -> Forward {
    match timeout {
        Some(dur) => match tokio::time::timeout(dur, write_half.write_all(encoded)).await {
            Ok(Ok(())) => Forward::Continue,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Error writing to replica");
                Forward::Break
            }
            Err(_) => {
                tracing::warn!(
                    replica_id,
                    timeout_ms = dur.as_millis() as u64,
                    "Write to replica timed out, disconnecting"
                );
                Forward::Break
            }
        },
        None => match write_half.write_all(encoded).await {
            Ok(()) => Forward::Continue,
            Err(e) => {
                tracing::warn!(error = %e, "Error writing to replica");
                Forward::Break
            }
        },
    }
}

/// Proactive lag-disconnect policy for one streaming session.
///
/// Owns the forwarded-frame counter and the threshold comparison so the
/// streaming loop consults "when do we proactively disconnect a lagging
/// replica" as a value rather than inlining it. Checks fire every
/// [`LAG_CHECK_INTERVAL`] frames (cadence unchanged by the extraction).
struct LagPolicy {
    /// Live byte/time thresholds, shared with the owning
    /// [`PrimaryReplicationHandler`]. Re-read on every evaluation, never copied
    /// into the session, so a `CONFIG SET` retunes this already-streaming
    /// session instead of only the next one.
    thresholds: Arc<LagThresholds>,
    /// Cooldown after a proactive disconnect before allowing another.
    cooldown: Duration,
    /// Frames forwarded so far (the check cadence counter).
    frames: u64,
}

impl LagPolicy {
    fn new(thresholds: Arc<LagThresholds>, cooldown: Duration) -> Self {
        Self {
            thresholds,
            cooldown,
            frames: 0,
        }
    }

    /// Count one forwarded frame and, every [`LAG_CHECK_INTERVAL`] frames,
    /// decide whether this replica has exceeded a threshold and is out of
    /// cooldown. Returns `Some(LagBreach)` naming which threshold(s) fired when
    /// a proactive disconnect is warranted, or `None` otherwise. On a `Some`
    /// return the caller records the disconnect (logging the breach detail) and
    /// breaks the streaming loop.
    ///
    /// Both thresholds are loaded from the shared [`LagThresholds`] here, at the
    /// point of use. A policy that is disabled at this instant neither counts
    /// the frame nor fires — so arming a threshold mid-session starts the check
    /// cadence from the next forwarded frame.
    fn should_disconnect(
        &mut self,
        tracker: &ReplicationTrackerImpl,
        id: u64,
    ) -> Option<LagBreach> {
        let threshold_bytes = self.thresholds.threshold_bytes();
        let threshold_secs = self.thresholds.threshold_secs();
        if threshold_bytes == 0 && threshold_secs == 0 {
            return None;
        }
        self.frames += 1;
        if !self.frames.is_multiple_of(LAG_CHECK_INTERVAL) {
            return None;
        }
        let byte_exceeded = threshold_bytes > 0
            && tracker
                .replica_lag(id)
                .is_some_and(|lag| lag >= threshold_bytes);
        let time_exceeded = threshold_secs > 0
            && tracker
                .replica_lag_secs(id)
                .is_some_and(|secs| secs >= threshold_secs as f64);
        if (byte_exceeded || time_exceeded) && !tracker.is_in_lag_cooldown(id, self.cooldown) {
            Some(LagBreach {
                byte_exceeded,
                time_exceeded,
            })
        } else {
            None
        }
    }
}

/// Which lag threshold(s) a proactive-disconnect decision tripped. Returned by
/// [`LagPolicy::should_disconnect`] so the disconnect warning can name the
/// specific threshold that fired (byte-lag vs. time-since-last-ACK) rather than
/// logging a bare boolean.
#[derive(Debug, Clone, Copy)]
struct LagBreach {
    /// The byte-lag threshold was met or exceeded.
    byte_exceeded: bool,
    /// The time-since-last-ACK threshold was met or exceeded.
    time_exceeded: bool,
}

#[cfg(test)]
mod tests {
    //! `ReplicaSession::run` lifecycle tests.
    //!
    //! These tests drive `run()` end-to-end against in-memory streams so we can
    //! verify that the single exit handler runs cleanup regardless of where in
    //! the lifecycle the connection drops. The `mid_fullsync_drop` case is the
    //! regression test for the leak that motivated this refactor: under the
    //! pre-refactor code, a `?`-propagated error from inside `handle_full_sync`
    //! left the replica registered as `Syncing` until process restart.
    use super::*;
    use crate::frame::serialize_command_to_resp;
    use crate::primary::BacklogConfig;
    use crate::primary::{LagThresholdConfig, PrimaryReplicationHandler};
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use bytes::Bytes;
    use frogdb_persistence::{RocksConfig, RocksStore};
    use frogdb_types::ReplicationTracker;
    use std::net::SocketAddr;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    fn addr() -> SocketAddr {
        "127.0.0.1:9001".parse().unwrap()
    }

    /// Wire a live-snapshot source returning `blobs`, standing in for the
    /// server crate's shard export. A persistence-disabled primary needs one to
    /// serve a full resync at all (issue 67).
    fn with_live_dataset(handler: &Arc<PrimaryReplicationHandler>, blobs: Vec<Vec<u8>>) {
        handler.set_live_snapshot_source(Arc::new(move || {
            let blobs = blobs.clone();
            Box::pin(async move { Ok(blobs) })
        }));
    }

    /// One dataset blob holding `entries`, in the framing the shard workers
    /// produce and the replica's installer consumes.
    fn dataset_blob(entries: &[(&str, &str)]) -> Vec<u8> {
        use frogdb_types::types::{KeyMetadata, Value};
        let mut blob = Vec::new();
        for (key, val) in entries {
            let value = Value::string(Bytes::from(val.to_string()));
            let metadata = KeyMetadata::new(value.memory_size());
            frogdb_persistence::append_entry(&mut blob, key.as_bytes(), &value, &metadata);
        }
        blob
    }

    /// Read a whole live-dataset envelope off the wire, returning the blob
    /// bodies and the trailing metadata frame.
    ///
    /// Byte-at-a-time on the raw stream rather than through a `BufReader`, so
    /// the caller can keep decoding replication frames from the same client
    /// afterwards without losing buffered bytes to a reader it dropped.
    async fn drain_live_dataset(
        client: &mut tokio::io::DuplexStream,
    ) -> (Vec<Vec<u8>>, FullSyncMetadata) {
        async fn dollar_len(client: &mut tokio::io::DuplexStream) -> usize {
            read_response_line(client)
                .await
                .trim()
                .trim_start_matches('$')
                .parse()
                .unwrap()
        }

        let marker = read_response_line(client).await;
        assert_eq!(
            marker.trim(),
            "$FROGDB_SNAPSHOT",
            "a persistence-disabled primary must announce a dataset envelope"
        );
        let count: usize = read_response_line(client).await.trim().parse().unwrap();

        let mut blobs = Vec::with_capacity(count);
        for _ in 0..count {
            // `$<name_len>\r\n<name>\r\n$<size>\r\n<size bytes>`.
            let name_len = dollar_len(client).await;
            let mut name = vec![0u8; name_len + 2];
            client.read_exact(&mut name).await.unwrap();
            let size = dollar_len(client).await;
            let mut blob = vec![0u8; size];
            client.read_exact(&mut blob).await.unwrap();
            blobs.push(blob);
        }

        let meta_len = dollar_len(client).await;
        let mut meta = vec![0u8; meta_len + 2];
        client.read_exact(&mut meta).await.unwrap();
        let metadata = FullSyncMetadata::from_bytes(&meta[..meta_len]).expect("metadata parses");
        (blobs, metadata)
    }

    fn make_handler(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
    ) -> Arc<PrimaryReplicationHandler> {
        let state_path = data_dir.join("replication_state.json");
        let identity =
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
        Arc::new(PrimaryReplicationHandler::new(
            identity,
            state_path,
            tracker,
            rocks,
            data_dir,
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            BacklogConfig {
                enabled: false,
                max_entries: 0,
                max_bytes: 0,
                ttl_secs: 0,
            },
            0,
        ))
    }

    /// Like [`make_handler`] but with the replication backlog enabled, so the
    /// partial-sync replay path has frames to serve.
    fn make_handler_with_backlog(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
    ) -> Arc<PrimaryReplicationHandler> {
        make_handler_with_backlog_entries(tracker, rocks, data_dir, 10_000)
    }

    /// Same, with the backlog's entry cap under the test's control — a cap of a
    /// few entries makes eviction happen on demand instead of after 10 000
    /// writes.
    fn make_handler_with_backlog_entries(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
        max_entries: usize,
    ) -> Arc<PrimaryReplicationHandler> {
        let state_path = data_dir.join("replication_state.json");
        let identity =
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
        Arc::new(PrimaryReplicationHandler::new(
            identity,
            state_path,
            tracker,
            rocks,
            data_dir,
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            BacklogConfig {
                enabled: true,
                max_entries,
                max_bytes: 64 * 1024 * 1024,
                ttl_secs: 0,
            },
            0,
        ))
    }

    /// Read the leading `+CONTINUE` line, then decode exactly `n` replication
    /// frames off the same stream.
    async fn read_continue_then_frames(
        client: &mut tokio::io::DuplexStream,
        n: usize,
    ) -> Vec<ReplicationFrame> {
        use crate::frame::ReplicationFrameCodec;
        use tokio_util::codec::Decoder;

        // Leading simple-string line.
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let read = client.read(&mut byte).await.unwrap();
            assert!(read > 0, "stream closed before +CONTINUE line");
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        let line = String::from_utf8(line).unwrap();
        assert!(
            line.starts_with("+CONTINUE"),
            "expected +CONTINUE, got {line:?}"
        );

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        while frames.len() < n {
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame);
                if frames.len() == n {
                    break;
                }
            }
            if frames.len() == n {
                break;
            }
            let read = client.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before {n} frames arrived");
        }
        frames
    }

    /// Decode exactly `n` replication frames off the stream.
    async fn decode_n_frames(
        client: &mut tokio::io::DuplexStream,
        n: usize,
    ) -> Vec<ReplicationFrame> {
        use crate::frame::ReplicationFrameCodec;
        use tokio_util::codec::Decoder;

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        while frames.len() < n {
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame);
                if frames.len() == n {
                    break;
                }
            }
            if frames.len() == n {
                break;
            }
            let read = client.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before {n} frames arrived");
        }
        frames
    }

    /// F1: writes broadcast during the full-sync handoff (after the snapshot
    /// offset is captured, before the live stream is joined) must NOT be lost.
    ///
    /// A tiny duplex buffer blocks the session inside `stream_live_dataset`,
    /// opening a deterministic window: the test reads the FULLRESYNC line
    /// (proving the snapshot offset is captured), then broadcasts commands while
    /// the session is blocked, then drains the dataset. When the session reaches
    /// `start_streaming` it must replay those commands from the backlog. Under
    /// the pre-fix code (subscribe-only, no replay) they would have been dropped
    /// and this test would hang waiting for frames that never arrive.
    // FM-REPLICATION-004
    #[tokio::test]
    async fn full_sync_replays_writes_made_during_handoff() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // No rocks store -> live-dataset full sync; backlog enabled so the
        // handoff window can be replayed.
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        with_live_dataset(&handler, vec![dataset_blob(&[("seed", "v")])]);
        let repl_id = handler.state.read().replication_id.clone();

        // Tiny buffer forces the session to block writing the dataset, giving us
        // a window to broadcast "during" the handoff.
        let (mut client, server) = tokio::io::duplex(32);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                    )
                    .await
            }
        });

        // 1. FULLRESYNC line — snapshot offset (0) is now captured.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        // 2. Broadcast while the session is blocked streaming the dataset. These
        //    advance the live offset and land in the backlog, after the snapshot
        //    offset and before start_streaming's replay extract.
        let mut expected = Vec::new();
        for i in 0..4 {
            let key = format!("during{i}");
            handler.broadcast_command("SET", &[Bytes::from(key.clone()), Bytes::from("v")]);
            expected.push(serialize_command_to_resp(
                "SET",
                &[Bytes::from(key), Bytes::from("v")],
            ));
        }

        // 3. Drain the dataset envelope, unblocking the session so it proceeds
        //    to the streaming handoff.
        let (blobs, _) = drain_live_dataset(&mut client).await;
        assert_eq!(blobs.len(), 1);

        // 4. The handoff replays exactly the 4 writes — none lost. A regression
        //    (subscribe-only handoff) drops them, so the frames never arrive;
        //    bound the wait so that fails fast instead of hanging.
        let frames = tokio::time::timeout(
            Duration::from_secs(5),
            decode_n_frames(&mut client, expected.len()),
        )
        .await
        .expect("handoff writes were not replayed within 5s (F1 regression)");
        let got: Vec<bytes::Bytes> = frames.iter().map(|f| f.payload.clone()).collect();
        assert_eq!(got, expected, "all writes during handoff must be replayed");

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-015
    /// F2: a partial resync replays the backlog tail `(replay_from, current]`
    /// before joining the live tail — it never silently drops the gap. Then a
    /// fresh write streams once (no duplicate of the replayed frames).
    #[tokio::test]
    async fn handle_partial_replays_backlog_then_live_tail() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());

        // Seed the backlog. `off1` is the offset the reconnecting replica holds;
        // it must be replayed `(off1, off3]` == {off2, off3}.
        let _off1_first = handler.broadcast_command("SET", &[Bytes::from("k0"), Bytes::from("v0")]);
        let off1 = handler.broadcast_command("SET", &[Bytes::from("k1"), Bytes::from("v1")]);
        let off2 = handler.broadcast_command("SET", &[Bytes::from("k2"), Bytes::from("v2")]);
        let off3 = handler.broadcast_command("SET", &[Bytes::from("k3"), Bytes::from("v3")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(server, SyncKind::Partial { replay_from: off1 }, handler)
                    .await
            }
        });

        // The replayed tail is exactly {off2, off3}, in offset order.
        let replayed = read_continue_then_frames(&mut client, 2).await;
        assert_eq!(replayed[0].sequence, off2);
        assert_eq!(replayed[1].sequence, off3);
        assert_eq!(
            replayed[0].payload,
            serialize_command_to_resp("SET", &[Bytes::from("k2"), Bytes::from("v2")])
        );
        assert_eq!(
            replayed[1].payload,
            serialize_command_to_resp("SET", &[Bytes::from("k3"), Bytes::from("v3")])
        );

        // A fresh write after the handoff arrives on the live tail exactly once.
        let off4 = handler.broadcast_command("SET", &[Bytes::from("k4"), Bytes::from("v4")]);
        let mut codec = {
            use crate::frame::ReplicationFrameCodec;
            ReplicationFrameCodec::new()
        };
        let mut buf = BytesMut::new();
        let live = loop {
            use tokio_util::codec::Decoder;
            if let Some(frame) = codec.decode(&mut buf).unwrap() {
                break frame;
            }
            let read = client.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before live frame");
        };
        assert_eq!(live.sequence, off4, "live tail must continue after replay");

        drop(client);
        let _ = task.await.unwrap();
        assert_eq!(tracker.replica_count(), 0);
    }

    /// Read to EOF and assert nothing more was streamed.
    ///
    /// An abandoned resume must leave the wire empty: not a short tail, not a
    /// frame, just the close.
    async fn assert_no_frames_then_eof(client: &mut tokio::io::DuplexStream) {
        let mut rest = Vec::new();
        loop {
            let mut buf = [0u8; 256];
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => rest.extend_from_slice(&buf[..n]),
            }
        }
        assert!(
            !rest.windows(4).any(|w| w == crate::frame::FRAME_MAGIC),
            "an abandoned resume must stream no frames, got {} trailing bytes",
            rest.len()
        );
    }

    /// The window can close between the grant and the replay, and when it does
    /// the resume is abandoned — never streamed short.
    ///
    /// The `+CONTINUE` is already on the wire when the eviction happens (a
    /// 1-byte duplex parks the session inside that write), which is exactly the
    /// production shape: the grant is decided against one window and streamed
    /// against a later one. Pre-fix, `extract_backlog` returned the surviving
    /// suffix, `resume_offset` was seeded from the last frame actually sent, the
    /// live tail deduped against it, and the replica was permanently missing the
    /// evicted range with an offset that looked contiguous (round-2 issue 52).
    // FM-REPLICATION-012
    #[tokio::test]
    async fn a_resume_evicted_after_the_grant_is_abandoned_not_truncated() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // Two entries of backlog: eviction is a few writes away, not 10 000.
        let handler =
            make_handler_with_backlog_entries(tracker.clone(), None, dir.path().to_path_buf(), 2);

        // The offset the reconnecting replica holds, granted while it is still
        // inside the window.
        let held = handler.broadcast_command("SET", &[Bytes::from("k0"), Bytes::from("v0")]);
        assert!(handler.replay.backlog_start().unwrap() <= held);

        let (mut client, server) = tokio::io::duplex(1);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(server, SyncKind::Partial { replay_from: held }, handler)
                    .await
            }
        });

        // The session is parked writing `+CONTINUE`; evict the resume point out
        // from under the grant it just made.
        for i in 1..=4 {
            handler.broadcast_command("SET", &[Bytes::from(format!("k{i}")), Bytes::from("v")]);
        }
        assert!(
            handler.replay.backlog_start().unwrap() > held,
            "the test must actually evict the resume point"
        );

        // Unblock the session by draining the grant line.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");

        // The replay refuses, so the session fails the link instead of streaming
        // the surviving suffix.
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the session must not hang on a closed window")
            .unwrap();
        let err = result.expect_err("a truncated replay must fail the link");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "got: {err}");

        assert_no_frames_then_eof(&mut client).await;
        assert_eq!(tracker.replica_count(), 0, "the session must be cleaned up");
    }

    /// The same window, on the path that opens it widest: a full sync, where the
    /// grant is written before the dataset is even cut and the whole payload
    /// transfer sits between the grant and the replay.
    // FM-REPLICATION-012
    #[tokio::test]
    async fn a_full_sync_whose_handoff_window_is_evicted_abandons_the_link() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler =
            make_handler_with_backlog_entries(tracker.clone(), None, dir.path().to_path_buf(), 2);
        with_live_dataset(&handler, vec![dataset_blob(&[("seed", "v")])]);
        let repl_id = handler.state.read().replication_id.clone();

        // Tiny buffer: the session parks inside the dataset write, which is the
        // production stand-in for a multi-gigabyte checkpoint transfer.
        let (mut client, server) = tokio::io::duplex(32);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        // Writes during the transfer overrun the backlog, so the snapshot offset
        // the replica was granted is no longer inside the window.
        for i in 0..4 {
            handler.broadcast_command(
                "SET",
                &[Bytes::from(format!("during{i}")), Bytes::from("v")],
            );
        }
        assert!(handler.replay.backlog_start().unwrap() > 0);

        let (blobs, _) = drain_live_dataset(&mut client).await;
        assert_eq!(blobs.len(), 1);

        // The replica gets a whole dataset and then a dropped link — it comes
        // back for a fresh full sync rather than resuming past the hole.
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the session must not hang on a closed window")
            .unwrap();
        let err = result.expect_err("a truncated handoff replay must fail the link");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "got: {err}");

        assert_no_frames_then_eof(&mut client).await;
        assert_eq!(tracker.replica_count(), 0, "the session must be cleaned up");
    }

    /// Streaming drop: a partial sync that completes and enters `Streaming`,
    /// then the replica disconnects. The exit handler must remove the session
    /// from the tracker.
    #[tokio::test]
    async fn run_cleans_up_on_streaming_drop_partial() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());

        let (mut client, server) = tokio::io::duplex(1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(server, SyncKind::Partial { replay_from: 0 }, handler)
                    .await
            }
        });

        // Read the +CONTINUE response so the session has reached Streaming.
        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0);
        assert!(buf[..n].starts_with(b"+CONTINUE"));

        // Drop client to trigger EOF on the read half.
        drop(client);

        let result = task.await.unwrap();
        assert!(result.is_ok());

        assert_eq!(tracker.replica_count(), 0);
        assert_eq!(session.phase(), Phase::Disconnecting);
    }

    /// Mid-handshake drop: the writer of `+CONTINUE` fails before the session
    /// can reach `Streaming`. This is the case the old code path did NOT clean
    /// up: a `?` from `write_all` returned without unregistering.
    #[tokio::test]
    async fn run_cleans_up_on_mid_handshake_drop() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());

        // Tiny buffer (1 byte) so write_all blocks; drop the reader so it errors.
        let (client, server) = tokio::io::duplex(1);
        drop(client);
        let session = tracker.register_replica(addr());

        let server: BoxedStream = Box::new(server);
        let result = session
            .clone()
            .run(
                server,
                SyncKind::Partial { replay_from: 0 },
                handler.clone(),
            )
            .await;

        assert!(result.is_err(), "expected write_all to error after drop");
        assert_eq!(tracker.replica_count(), 0);
        assert_eq!(session.phase(), Phase::Disconnecting);
    }

    /// Issue 67: a full sync served without a RocksStore carries the primary's
    /// **dataset**, not an empty envelope.
    ///
    /// This is the forcing test at the wire level. The old behaviour answered
    /// `PSYNC ? -1` with a data-less minimal RDB (`REDIS0011`… + EOF): the
    /// replica adopted the replid/offset, flipped to `Streaming`, and kept every
    /// key it already had. Here the envelope must be a `$FROGDB_SNAPSHOT` whose
    /// bodies decode to the primary's keys, and whose combined checksum and
    /// replid/offset match what was granted — everything a replica needs to
    /// replace its keyspace rather than keep it.
    ///
    /// Closing the client triggers normal cleanup; no checkpoint directory
    /// should exist (it was never created).
    // FM-REPLICATION-001
    #[tokio::test]
    async fn run_full_sync_without_rocks_streams_the_live_dataset() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        with_live_dataset(
            &handler,
            vec![
                dataset_blob(&[("a", "1"), ("b", "2")]),
                dataset_blob(&[("c", "3")]),
            ],
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id.clone(),
                        },
                        handler,
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        let (blobs, metadata) = drain_live_dataset(&mut client).await;

        // The dataset is really on the wire: one blob per shard, decoding to
        // the primary's keys. The old minimal-RDB payload had none of this.
        let keys: Vec<String> = blobs
            .iter()
            .flat_map(|b| frogdb_persistence::read_entries(b).expect("blob decodes"))
            .map(|e| String::from_utf8(e.key.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "b", "c"]);

        // Verified end to end the same way a checkpoint is, so a truncated or
        // reordered dataset cannot pass as the primary's keyspace.
        let mut combined = CheckpointChecksum::new();
        for (shard_id, blob) in blobs.iter().enumerate() {
            combined.update_file(
                &format!("shard-{shard_id}.dataset"),
                &calculate_bytes_checksum(blob),
            );
        }
        assert_eq!(combined.finalize(), metadata.checksum);
        assert_eq!(
            metadata.rdb_size,
            blobs.iter().map(Vec::len).sum::<usize>() as u64
        );
        assert_eq!(
            metadata.replication_id,
            handler.state.read().replication_id,
            "the dataset carries the identity the FULLRESYNC granted"
        );
        assert_eq!(
            metadata.replication_offset, 0,
            "and the offset captured before the export"
        );

        drop(client);
        let result = task.await.unwrap();
        assert!(result.is_ok());

        assert_eq!(tracker.replica_count(), 0);
        // The dataset path never sets sync_checkpoint_path, so no dir to clean.
        assert!(session.inner.read().sync_checkpoint_path.is_none());
    }

    /// A primary that cannot read its own keyspace fails the sync instead of
    /// sending an envelope with nothing in it — the shape issue 67 was about.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn full_sync_without_a_live_snapshot_source_fails_the_sync() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // No rocks store *and* no live-snapshot source: nothing to send.
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move { session.handle_full(server, repl_id, &handler).await }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        let err = task.await.unwrap().expect_err("the sync must fail");
        assert!(
            err.to_string().contains("no live-snapshot source"),
            "got: {err}"
        );
    }

    /// Mid-fullsync drop with a real checkpoint directory — the regression
    /// test for the leak that motivated this refactor.
    ///
    /// Drives a FULLRESYNC against a real RocksStore so a checkpoint is
    /// actually created (and `sync_checkpoint_path` is set), then drops the
    /// client mid-stream. The exit handler must:
    ///   1. unregister the session from the tracker
    ///   2. delete the on-disk checkpoint directory
    #[tokio::test]
    async fn run_cleans_up_checkpoint_dir_on_mid_fullsync_drop() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        // Insert at least one key so the checkpoint contains real data.
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (client, server) = tokio::io::duplex(64);
        let session = tracker.register_replica(addr());
        let session_id = session.id();
        let expected_checkpoint = dir.path().join(format!("fullsync_{}", session_id));

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                    )
                    .await
            }
        });

        // Drop the client so the checkpoint stream's writes start to fail
        // partway through. With a 64-byte duplex buffer, the writer blocks
        // long before the full checkpoint is sent.
        drop(client);

        let _ = task.await.unwrap();

        assert_eq!(tracker.replica_count(), 0);
        assert_eq!(session.phase(), Phase::Disconnecting);
        assert!(
            !expected_checkpoint.exists(),
            "checkpoint dir should have been removed by exit handler: {}",
            expected_checkpoint.display()
        );
    }

    /// The pre-checkpoint hook runs *before* the checkpoint is cut, and what it
    /// makes durable is inside the cut checkpoint.
    ///
    /// In production the hook drains every shard's WAL flush-engine into
    /// RocksDB; here it writes the key directly, which is the same step reduced
    /// to its essence — a write that reaches RocksDB only because the hook ran.
    /// Both halves matter: run the hook *after* `create_checkpoint` and the key
    /// is missing from the checkpoint even though the primary already
    /// acknowledged it, and for a full resync (nothing in the backlog to replay)
    /// the replica never sees it again.
    ///
    /// Drives `handle_full` directly rather than `run`, so no exit handler
    /// deletes the checkpoint directory before the assertions can read it.
    #[tokio::test]
    async fn fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};

        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        // Already committed before the resync starts.
        store.put(0, b"early", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();

        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        let runs = Arc::new(AtomicUsize::new(0));
        let ran_before_the_cut = Arc::new(AtomicBool::new(false));
        {
            let store = store.clone();
            let runs = runs.clone();
            let ran_before_the_cut = ran_before_the_cut.clone();
            let checkpoint_path = checkpoint_path.clone();
            handler.set_pre_checkpoint_hook(Arc::new(move || {
                let store = store.clone();
                let runs = runs.clone();
                let ran_before_the_cut = ran_before_the_cut.clone();
                let checkpoint_path = checkpoint_path.clone();
                Box::pin(async move {
                    if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                        ran_before_the_cut.store(!checkpoint_path.exists(), Ordering::SeqCst);
                    }
                    store.put(0, b"drained", b"v").unwrap();
                    Ok(())
                })
            }));
        }

        // A tiny duplex buffer stalls the checkpoint stream. Reading the
        // `+FULLRESYNC` line first pins the session past the reply, so dropping
        // the client afterwards fails a *checkpoint stream* write — i.e. returns
        // `handle_full` after the cut, not before it.
        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move { session.handle_full(server, repl_id, &handler).await }
        });
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);
        let _ = task.await.unwrap();

        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "the drain must run exactly once per full resync"
        );
        assert!(
            ran_before_the_cut.load(Ordering::SeqCst),
            "the drain must run before the checkpoint is cut, not after"
        );

        let checkpoint = RocksStore::open(&checkpoint_path, 1, &RocksConfig::default())
            .expect("cut checkpoint must be a readable database");
        assert_eq!(
            checkpoint.get(0, b"early").unwrap().as_deref(),
            Some(&b"v"[..]),
            "the checkpoint must carry writes committed before the resync"
        );
        assert_eq!(
            checkpoint.get(0, b"drained").unwrap().as_deref(),
            Some(&b"v"[..]),
            "the checkpoint must carry writes the drain committed"
        );
    }

    // FM-PERSISTENCE-020
    /// A drain that cannot complete fails the resync instead of shipping a
    /// dataset that is missing acknowledged writes.
    ///
    /// The checkpoint is the replica's entire base dataset: writes made before
    /// it attached were never broadcast, so nothing in the backlog can replay
    /// what an undrained shard left behind — the hole would be permanent and
    /// invisible. Failing drops the connection and the replica retries `PSYNC ?
    /// -1` on its backoff, which costs one reconnect. No checkpoint may be cut
    /// or staged for cleanup on that path.
    #[tokio::test]
    async fn fullresync_fails_when_the_pre_checkpoint_drain_fails() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"early", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        handler.set_pre_checkpoint_hook(Arc::new(move || {
            Box::pin(async {
                Err(io::Error::other(
                    "WAL drain: 1 of 2 shard(s) did not drain (shard [1])",
                ))
            })
        }));

        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move { session.handle_full(server, repl_id, &handler).await }
        });
        // The `+FULLRESYNC` line is written before the cut, so it still arrives;
        // what must not follow is a checkpoint. Dropping the client afterwards
        // keeps the assertions honest about *why* the sync failed: without the
        // drain check it would fail on a checkpoint-stream write instead, with a
        // cut checkpoint on disk.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);

        let err = task
            .await
            .unwrap()
            .expect_err("an incomplete drain must fail the full resync");
        assert!(
            err.to_string().contains("did not drain"),
            "the drain's cause must reach the failure: {err}"
        );
        assert!(
            !checkpoint_path.exists(),
            "no checkpoint may be cut once the drain has failed"
        );
        assert!(
            session.inner.read().sync_checkpoint_path.is_none(),
            "nothing was staged, so nothing may be marked for cleanup"
        );
    }

    /// Without a hook installed the full-sync path still cuts a checkpoint —
    /// a handler whose owner wired no drain (every unit test here, and any
    /// embedder that keeps its writes in RocksDB synchronously) must not stall
    /// or fail.
    #[tokio::test]
    async fn fullresync_without_a_pre_checkpoint_hook_still_cuts_the_checkpoint() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"early", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move { session.handle_full(server, repl_id, &handler).await }
        });
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);
        let _ = task.await.unwrap();

        let checkpoint = RocksStore::open(&checkpoint_path, 1, &RocksConfig::default())
            .expect("cut checkpoint must be a readable database");
        assert_eq!(
            checkpoint.get(0, b"early").unwrap().as_deref(),
            Some(&b"v"[..])
        );
    }

    #[test]
    fn force_phase_for_test_drives_phase() {
        let session = ReplicaSession::new(1, addr());
        assert_eq!(session.phase(), Phase::Connecting);
        session.force_phase_for_test(Phase::Streaming);
        assert_eq!(session.phase(), Phase::Streaming);
    }

    // FM-REPLICATION-039
    #[test]
    fn record_ack_is_monotonic_and_refreshes_liveness() {
        let session = ReplicaSession::new(1, addr());
        let t0 = session.last_ack_time();
        assert_eq!(session.acked_offset(), 0);

        // First ACK advances offset and refreshes time.
        std::thread::sleep(Duration::from_millis(2));
        assert!(session.record_ack(100));
        assert_eq!(session.acked_offset(), 100);
        assert!(session.last_ack_time() > t0);

        // Re-ACKing the same offset is treated as liveness only.
        let t1 = session.last_ack_time();
        std::thread::sleep(Duration::from_millis(2));
        assert!(!session.record_ack(100));
        assert_eq!(session.acked_offset(), 100);
        assert!(session.last_ack_time() > t1);

        // Stale ACK (lower offset) does not regress.
        assert!(!session.record_ack(50));
        assert_eq!(session.acked_offset(), 100);
    }

    // FM-REPLICATION-013
    /// The FULLRESYNC reply line and the streamed checkpoint metadata must both
    /// carry the primary's *live* offset (the tracker's write position), not the
    /// stale `state.replication_offset` (left at 0 here). This is the offset/data
    /// correspondence the staged `replication_metadata.json` relies on.
    #[tokio::test]
    async fn fullresync_offset_and_metadata_come_from_live_tracker() {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // Simulate writes having advanced the live stream head. The handler's
        // `state.replication_offset` stays 0, so a stale-field read would attach
        // offset 0 to the checkpoint.
        let live_offset = 4096u64;
        tracker.set_offset(live_offset);

        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (client, server) = tokio::io::duplex(1024 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                    )
                    .await
            }
        });

        let mut reader = BufReader::new(client);

        // 1. FULLRESYNC line carries the live tracker offset, not the stale 0.
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[0], "+FULLRESYNC");
        assert_eq!(parts[1], repl_id);
        assert_eq!(parts[2].parse::<u64>().unwrap(), live_offset);

        // 2. Checkpoint header + file count.
        let mut header = String::new();
        reader.read_line(&mut header).await.unwrap();
        assert_eq!(header.trim(), "$FROGDB_CHECKPOINT");
        let mut count_line = String::new();
        reader.read_line(&mut count_line).await.unwrap();
        let file_count: usize = count_line.trim().parse().unwrap();

        // 3. Drain each file body: "$<name_len>\r\n<name>\r\n$<size>\r\n<raw bytes>".
        for _ in 0..file_count {
            let mut name_len_line = String::new();
            reader.read_line(&mut name_len_line).await.unwrap();
            let mut name_line = String::new();
            reader.read_line(&mut name_line).await.unwrap();
            let mut size_line = String::new();
            reader.read_line(&mut size_line).await.unwrap();
            let size: usize = size_line.trim().trim_start_matches('$').parse().unwrap();
            let mut body = vec![0u8; size];
            reader.read_exact(&mut body).await.unwrap();
        }

        // 4. Metadata frame: "$<mlen>\r\n<metadata bytes>\r\n".
        let mut mlen_line = String::new();
        reader.read_line(&mut mlen_line).await.unwrap();
        let mlen: usize = mlen_line.trim().trim_start_matches('$').parse().unwrap();
        let mut meta_buf = vec![0u8; mlen];
        reader.read_exact(&mut meta_buf).await.unwrap();
        let metadata = crate::fullsync::FullSyncMetadata::from_bytes(&meta_buf).unwrap();
        assert_eq!(
            metadata.replication_offset, live_offset,
            "streamed checkpoint metadata must carry the live tracker offset"
        );
        assert_eq!(metadata.replication_id, repl_id);

        drop(reader);
        let _ = task.await.unwrap();
    }

    /// Read the leading simple-string response line from the stream.
    async fn read_response_line(client: &mut tokio::io::DuplexStream) -> String {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = client.read(&mut byte).await.unwrap();
            if n == 0 {
                break;
            }
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        String::from_utf8(line).unwrap()
    }

    // FM-REPLICATION-015
    /// The inverse of the old pinning test: with the backlog enabled and the
    /// requested offset still covered, a matching window now yields `+CONTINUE`
    /// — the gate is gone, partial resync is granted end to end.
    #[tokio::test]
    async fn partial_window_with_backlog_grants_continue() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        // Advance the live offset and populate the backlog with real commands.
        let resume_point = handler.broadcast_command("SET", &[Bytes::from("a"), Bytes::from("1")]);
        handler.broadcast_command("SET", &[Bytes::from("b"), Bytes::from("2")]);
        handler.broadcast_command("SET", &[Bytes::from("c"), Bytes::from("3")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(server, addr(), &repl_id, resume_point as i64)
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+CONTINUE"),
            "in-window PSYNC with backlog coverage must grant +CONTINUE, got: {line:?}"
        );
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(
            parts[1], repl_id,
            "+CONTINUE carries the live replication id"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-017
    /// Shutdown must end a streaming session even though its peer keeps the
    /// socket open: the session is served inline by the accepting connection
    /// task, so nothing else would ever release it — and it holds the storage
    /// engine open behind the shutdown.
    #[tokio::test]
    async fn shutdown_downstream_sessions_ends_a_streaming_session() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        let resume_point = handler.broadcast_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(server, addr(), &repl_id, resume_point as i64)
                    .await
            }
        });

        // Streaming: the session is registered and parked on the WAL stream.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");
        assert_eq!(tracker.get_all_replicas().len(), 1);

        // The peer never closes — only the shutdown signal can end this.
        let signalled = handler
            .shutdown_downstream_sessions(Duration::from_secs(5))
            .await;
        assert_eq!(signalled, 1, "the streaming session must be signalled");
        assert!(
            tracker.get_all_replicas().is_empty(),
            "shutdown must return only once every session has left the registry"
        );
        task.await.unwrap().unwrap();
    }

    // FM-REPLICATION-017
    /// A connection accepted a moment before the acceptors were aborted can
    /// still reach PSYNC while the drain runs. It must be refused: a session
    /// registered behind the drain would stream past the shutdown that was
    /// meant to end them all, holding the storage engine open.
    #[tokio::test]
    async fn psync_after_the_shutdown_drain_is_refused() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point = handler.broadcast_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        // No sessions yet, so the drain returns immediately — it still latches.
        assert_eq!(
            handler
                .shutdown_downstream_sessions(Duration::from_secs(5))
                .await,
            0
        );

        let (_client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let err = handler
            .handle_psync(server, addr(), &repl_id, resume_point as i64)
            .await
            .expect_err("PSYNC must not be served once the drain has started");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
        assert!(
            tracker.get_all_replicas().is_empty(),
            "a refused PSYNC must not register a session"
        );
    }

    // FM-REPLICATION-013
    /// With the backlog disabled there is nothing to replay, so even a matching
    /// window falls back to FULLRESYNC — and the offset is the live tracker's.
    #[tokio::test]
    async fn partial_falls_back_to_full_when_backlog_disabled() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let live_offset = 1000u64;
        tracker.set_offset(live_offset);

        // make_handler disables the backlog; no rocks → minimal-RDB FULLRESYNC.
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                // Offset 500 <= live 1000 with a matching replid is a valid
                // window, but the disabled backlog cannot replay the gap.
                handler.handle_psync(server, addr(), &repl_id, 500).await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+FULLRESYNC"),
            "disabled backlog must force FULLRESYNC, got: {line:?}"
        );
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[1], repl_id);
        assert_eq!(
            parts[2].parse::<u64>().unwrap(),
            live_offset,
            "FULLRESYNC offset must be the live tracker offset"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-014
    /// A replica whose resume point has been evicted from the backlog falls back
    /// to FULLRESYNC — the lower bound guards against a truncated replay.
    #[tokio::test]
    async fn partial_falls_back_to_full_when_offset_evicted() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // Tiny backlog (3 entries) so the early resume point is evicted.
        let handler = Arc::new(PrimaryReplicationHandler::new(
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker),
            dir.path().join("replication_state.json"),
            tracker.clone(),
            None,
            dir.path().to_path_buf(),
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            BacklogConfig {
                enabled: true,
                max_entries: 3,
                max_bytes: 64 * 1024 * 1024,
                ttl_secs: 0,
            },
            0,
        ));
        let repl_id = handler.state.read().replication_id.clone();

        // First command's offset is the replica's resume point; later writes
        // evict it from the 3-entry backlog.
        let evicted_point = handler.broadcast_command("SET", &[Bytes::from("a"), Bytes::from("1")]);
        for i in 0..5 {
            handler.broadcast_command("SET", &[Bytes::from(format!("k{i}")), Bytes::from("v")]);
        }
        assert!(
            handler.replay.oldest_offset().unwrap() > evicted_point,
            "resume point should have been evicted"
        );

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(server, addr(), &repl_id, evicted_point as i64)
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+FULLRESYNC"),
            "evicted resume point must force FULLRESYNC, got: {line:?}"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // ------------------------------------------------------------------
    // forward_frame: the single write+timeout+error path, no socket needed.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn forward_frame_clean_write_continues() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let outcome = forward_frame(&mut server, b"hello", None, 1).await;
        assert!(matches!(outcome, Forward::Continue));
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[tokio::test]
    async fn forward_frame_write_timeout_breaks() {
        // 1-byte duplex buffer and nobody reading: write_all can never
        // complete, so the timeout must fire. Keep `client` alive so the
        // failure is a timeout, not an I/O error.
        let (_client, mut server) = tokio::io::duplex(1);
        let payload = vec![0u8; 64];
        let outcome =
            forward_frame(&mut server, &payload, Some(Duration::from_millis(20)), 1).await;
        assert!(matches!(outcome, Forward::Break));
    }

    #[tokio::test]
    async fn forward_frame_io_error_breaks() {
        let (client, mut server) = tokio::io::duplex(64);
        drop(client);
        let outcome = forward_frame(&mut server, b"data", None, 1).await;
        assert!(matches!(outcome, Forward::Break));
    }

    // ------------------------------------------------------------------
    // LagPolicy: the proactive lag-disconnect decision, no live session.
    // ------------------------------------------------------------------

    /// Drive the policy through one full check interval and report whether it
    /// fired (`should_disconnect` only evaluates thresholds every
    /// `LAG_CHECK_INTERVAL` forwarded frames).
    fn drive_one_interval(
        policy: &mut LagPolicy,
        tracker: &ReplicationTrackerImpl,
        id: u64,
    ) -> bool {
        drive_one_interval_breach(policy, tracker, id).is_some()
    }

    /// Like [`drive_one_interval`] but returns the [`LagBreach`] detail from the
    /// evaluating call so tests can assert *which* threshold fired.
    fn drive_one_interval_breach(
        policy: &mut LagPolicy,
        tracker: &ReplicationTrackerImpl,
        id: u64,
    ) -> Option<LagBreach> {
        let mut breach = None;
        for _ in 0..LAG_CHECK_INTERVAL {
            if let Some(b) = policy.should_disconnect(tracker, id) {
                breach = Some(b);
            }
        }
        breach
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_disabled_never_fires() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(1_000_000); // enormous byte lag, but nothing armed
        let mut policy = LagPolicy::new(Arc::new(LagThresholds::new(0, 0)), Duration::ZERO);
        for _ in 0..(2 * LAG_CHECK_INTERVAL) {
            assert!(policy.should_disconnect(&tracker, session.id()).is_none());
        }
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_byte_threshold_triggers() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000); // acked = 0 → lag = 10_000 bytes
        let mut policy = LagPolicy::new(
            Arc::new(LagThresholds::new(1_000, 0)),
            Duration::from_secs(60),
        );
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("byte threshold should fire a breach");
        assert!(breach.byte_exceeded, "byte threshold must be flagged");
        assert!(
            !breach.time_exceeded,
            "time threshold is disabled, must not be flagged"
        );
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_byte_threshold_not_exceeded_does_not_fire() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(500); // lag 500 < threshold 1_000
        let mut policy = LagPolicy::new(
            Arc::new(LagThresholds::new(1_000, 0)),
            Duration::from_secs(60),
        );
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_time_threshold_triggers() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        // The smallest armable time threshold is 1s; age the last-ACK time
        // past it.
        std::thread::sleep(Duration::from_millis(1100));
        let mut policy =
            LagPolicy::new(Arc::new(LagThresholds::new(0, 1)), Duration::from_secs(60));
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("time threshold should fire a breach");
        assert!(breach.time_exceeded, "time threshold must be flagged");
        assert!(
            !breach.byte_exceeded,
            "byte threshold is disabled, must not be flagged"
        );
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_cooldown_suppresses_retrigger() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000);
        // A prior proactive disconnect for this replica's address...
        tracker.record_lag_disconnect(session.id());
        // ...suppresses a re-trigger inside the cooldown window...
        let mut in_cooldown = LagPolicy::new(
            Arc::new(LagThresholds::new(1_000, 0)),
            Duration::from_secs(60),
        );
        assert!(!drive_one_interval(
            &mut in_cooldown,
            &tracker,
            session.id()
        ));
        // ...but a zero cooldown (window already elapsed) fires again.
        let mut expired = LagPolicy::new(Arc::new(LagThresholds::new(1_000, 0)), Duration::ZERO);
        assert!(drive_one_interval(&mut expired, &tracker, session.id()));
    }

    // FM-REPLICATION-043
    /// Propagation truth for `replication-lag-threshold-bytes`: the policy of an
    /// *already-running* session reads the shared thresholds at evaluation time,
    /// so storing a new byte threshold changes the very next decision — no
    /// reconnect, no new `LagPolicy`.
    #[test]
    fn lag_policy_byte_threshold_retunes_live() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000); // acked = 0 → lag = 10_000 bytes

        // Booted disabled: the same policy object never fires, whatever the lag.
        let thresholds = Arc::new(LagThresholds::new(0, 0));
        let mut policy = LagPolicy::new(thresholds.clone(), Duration::ZERO);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));

        // CONFIG SET replication-lag-threshold-bytes 1000 — armed above the lag.
        thresholds.set_threshold_bytes(1_000);
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("a live-armed byte threshold must fire on the running policy");
        assert!(breach.byte_exceeded);
        assert!(!breach.time_exceeded);

        // Raising it back above the lag disarms the same running policy.
        thresholds.set_threshold_bytes(1_000_000);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));

        // ...and disabling it entirely also takes effect immediately.
        thresholds.set_threshold_bytes(1_000);
        assert!(drive_one_interval(&mut policy, &tracker, session.id()));
        thresholds.set_threshold_bytes(0);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));
    }

    // FM-REPLICATION-043
    /// Propagation truth for `replication-lag-threshold-secs`, via the handler's
    /// own setter — the exact call `ConfigManager` will make. The handler and the
    /// session's policy share one [`LagThresholds`], so the store is visible to
    /// the running policy's next evaluation.
    #[test]
    fn lag_policy_time_threshold_retunes_live_via_handler() {
        let tmp = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let session = tracker.register_replica(addr());
        // `make_handler` boots with both lag thresholds disabled.
        let handler = make_handler(tracker.clone(), None, tmp.path().to_path_buf());

        // A session's policy, built exactly as the streaming loop builds it.
        let mut policy = LagPolicy::new(handler.lag_thresholds(), handler.lag_cooldown);
        // Age the last-ACK time past one second.
        std::thread::sleep(Duration::from_millis(1100));
        assert!(
            !drive_one_interval(&mut policy, &tracker, session.id()),
            "disabled at boot: nothing fires"
        );

        // CONFIG SET replication-lag-threshold-secs 1
        handler.set_lag_threshold_secs(1);
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("a live-armed time threshold must fire on the running policy");
        assert!(breach.time_exceeded);
        assert!(!breach.byte_exceeded);

        // And back off again on the same policy object.
        handler.set_lag_threshold_secs(0);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));
    }
}
