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
use sha2::Digest;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};

use frogdb_types::ReplicationTracker;

use crate::BoxedStream;
use crate::frame::ReplicationFrame;
use crate::fullsync::{FullSyncMetadata, calculate_file_checksum, stream_file_to_writer};
use crate::primary::{
    LAG_CHECK_INTERVAL, PrimaryReplicationHandler, ReplicaConnectionHandle, parse_replconf_ack,
};

// ============================================================================
// RDB Format Constants (used by the minimal-RDB fallback)
// ============================================================================

const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EOF: u8 = 0xFF;

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
    /// Resume from `offset` — replica's repl id and offset are compatible.
    Partial { offset: u64 },
    /// Send a full database snapshot.
    Full {
        replication_id: String,
        current_offset: u64,
    },
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

        // Remove the handle inserted during start_streaming (no-op for sessions
        // that never reached the streaming phase, e.g. mid-handshake drops).
        handler.connections.write().await.remove(&self.id);

        // Drop the session from the registry.
        handler.tracker.unregister_replica(self.id);

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
            SyncKind::Partial { offset } => self.handle_partial(stream, offset, handler).await,
            SyncKind::Full {
                replication_id,
                current_offset,
            } => {
                self.handle_full(stream, replication_id, current_offset, handler)
                    .await
            }
        }
    }

    async fn handle_partial(
        self: Arc<Self>,
        mut stream: BoxedStream,
        offset: u64,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        let replication_id = handler.state.read().await.replication_id.clone();
        let response = format!("+CONTINUE {}\r\n", replication_id);
        stream.write_all(response.as_bytes()).await?;
        // Routes through tracker so WAIT waiters get notified for any newer ACK.
        handler.tracker.record_ack(self.id, offset);
        self.start_streaming(stream, handler).await
    }

    async fn handle_full(
        self: Arc<Self>,
        mut stream: BoxedStream,
        replication_id: String,
        current_offset: u64,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        let response = format!("+FULLRESYNC {} {}\r\n", replication_id, current_offset);
        stream.write_all(response.as_bytes()).await?;

        if let Some(rocks) = handler.rocks_store.as_ref().cloned() {
            self.set_phase(Phase::PreparingCheckpoint);
            let checkpoint_path = handler.data_dir.join(format!("fullsync_{}", self.id));

            let path_clone = checkpoint_path.clone();
            let result = tokio::task::spawn_blocking(move || rocks.create_checkpoint(&path_clone))
                .await
                .map_err(io::Error::other)?;

            match result {
                Err(e) => {
                    // Checkpoint creation failed — fall back to minimal RDB.
                    // sync_checkpoint_path is intentionally NOT set, so the exit
                    // handler won't try to clean a directory that doesn't exist.
                    tracing::error!(error = %e, "Failed to create checkpoint for FULLRESYNC");
                    self.send_minimal_rdb(&mut stream).await?;
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
                        current_offset,
                    )
                    .await?;
                }
            }
        } else {
            // No persistence — minimal RDB only.
            self.send_minimal_rdb(&mut stream).await?;
        }

        tracing::info!(
            replica_id = self.id,
            addr = %self.address,
            offset = current_offset,
            "Completed FULLRESYNC"
        );

        self.start_streaming(stream, handler).await
    }

    /// Stream checkpoint files to the replica.
    ///
    /// Protocol:
    /// 1. `$FROGDB_CHECKPOINT\r\n` header
    /// 2. File count `<n>\r\n`
    /// 3. For each file: filename bulk-string, size, raw bytes
    /// 4. Metadata frame (replication_id:offset:checksum)
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

        // Header.
        stream.write_all(b"$FROGDB_CHECKPOINT\r\n").await?;
        stream
            .write_all(format!("{}\r\n", files.len()).as_bytes())
            .await?;

        // Bodies.
        for (file_name, file_size, file_path) in &files {
            stream
                .write_all(format!("${}\r\n{}\r\n", file_name.len(), file_name).as_bytes())
                .await?;
            stream
                .write_all(format!("${}\r\n", file_size).as_bytes())
                .await?;
            let bytes_written =
                stream_file_to_writer(file_path, stream, Some(&self.sync_bytes_transferred))
                    .await?;
            tracing::debug!(
                file = %file_name,
                size = bytes_written,
                progress = format!("{:.1}%", self.progress_percent()),
                "Streamed checkpoint file"
            );
        }

        // Combined checksum: hash of (filename, file-hash) pairs.
        let mut combined_hash = sha2::Sha256::new();
        for (file_name, _, file_path) in &files {
            let file_hash = calculate_file_checksum(file_path).await?;
            combined_hash.update(file_name.as_bytes());
            combined_hash.update(file_hash);
        }
        let final_hash = Digest::finalize(combined_hash);
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&final_hash);

        let metadata = FullSyncMetadata {
            rdb_size: total_size,
            checksum,
            replication_id: replication_id.to_string(),
            replication_offset,
        };
        let metadata_bytes = metadata.to_bytes();
        stream
            .write_all(format!("${}\r\n", metadata_bytes.len()).as_bytes())
            .await?;
        stream.write_all(&metadata_bytes).await?;
        stream.write_all(b"\r\n").await?;

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

    /// Send a minimal valid RDB to the replica (used for empty databases or
    /// as a fallback when checkpoint creation fails).
    async fn send_minimal_rdb(&self, stream: &mut BoxedStream) -> io::Result<()> {
        let empty_rdb = create_minimal_rdb();
        let header = format!("${}\r\n", empty_rdb.len());
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(&empty_rdb).await?;
        Ok(())
    }

    /// Enter the live-streaming phase: subscribe to WAL frames and forward them
    /// to the replica while a read task consumes REPLCONF ACKs.
    async fn start_streaming(
        self: Arc<Self>,
        stream: BoxedStream,
        handler: &Arc<PrimaryReplicationHandler>,
    ) -> io::Result<()> {
        self.set_phase(Phase::Streaming);

        let (frame_tx, mut frame_rx) = mpsc::channel::<ReplicationFrame>(1000);
        let mut wal_rx = handler.wal_broadcast.subscribe();
        {
            let handle = ReplicaConnectionHandle {
                _replica_id: self.id,
                _address: self.address,
                _frame_tx: frame_tx,
                _connected_at: self.connected_at,
            };
            handler.connections.write().await.insert(self.id, handle);
        }

        let (mut read_half, mut write_half) = tokio::io::split(stream);

        let read_tracker = handler.tracker.clone();
        let read_replica_id = self.id;
        let read_task = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);
            loop {
                match read_half.read_buf(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        while let Some((ack_offset, consumed)) = parse_replconf_ack(&buf) {
                            read_tracker.record_ack(read_replica_id, ack_offset);
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

        let lag_threshold_bytes = handler.lag_config.threshold_bytes;
        let lag_threshold_secs = handler.lag_config.threshold_secs;
        let lag_cooldown = handler.lag_config.cooldown;
        let lag_tracker = handler.tracker.clone();
        let lag_replica_id = self.id;
        let lag_enabled = lag_threshold_bytes > 0 || lag_threshold_secs > 0;
        let write_timeout = if handler.write_timeout_ms > 0 {
            Some(Duration::from_millis(handler.write_timeout_ms))
        } else {
            None
        };

        let write_task = tokio::spawn(async move {
            let mut frame_count: u64 = 0;
            loop {
                tokio::select! {
                    frame = wal_rx.recv() => {
                        match frame {
                            Ok(frame) => {
                                let encoded = frame.encode();
                                let write_result = if let Some(timeout_dur) = write_timeout {
                                    match tokio::time::timeout(timeout_dur, write_half.write_all(&encoded)).await {
                                        Ok(r) => r,
                                        Err(_) => {
                                            tracing::warn!(
                                                replica_id = lag_replica_id,
                                                timeout_ms = timeout_dur.as_millis() as u64,
                                                "Write to replica timed out, disconnecting"
                                            );
                                            break;
                                        }
                                    }
                                } else {
                                    write_half.write_all(&encoded).await
                                };
                                if let Err(e) = write_result {
                                    tracing::warn!(error = %e, "Error writing to replica");
                                    break;
                                }
                                if lag_enabled {
                                    frame_count += 1;
                                    if frame_count.is_multiple_of(LAG_CHECK_INTERVAL) {
                                        let byte_exceeded = lag_threshold_bytes > 0
                                            && lag_tracker
                                                .replica_lag(lag_replica_id)
                                                .is_some_and(|lag| lag >= lag_threshold_bytes);
                                        let time_exceeded = lag_threshold_secs > 0
                                            && lag_tracker
                                                .replica_lag_secs(lag_replica_id)
                                                .is_some_and(|secs| secs >= lag_threshold_secs as f64);
                                        if (byte_exceeded || time_exceeded)
                                            && !lag_tracker.is_in_lag_cooldown(lag_replica_id, lag_cooldown)
                                        {
                                            tracing::warn!(
                                                replica_id = lag_replica_id,
                                                byte_exceeded,
                                                time_exceeded,
                                                "Replica exceeded lag threshold, disconnecting for FULLRESYNC"
                                            );
                                            lag_tracker.record_lag_disconnect(lag_replica_id);
                                            break;
                                        }
                                    }
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
                    frame = frame_rx.recv() => {
                        match frame {
                            Some(frame) => {
                                let encoded = frame.encode();
                                let write_result = if let Some(timeout_dur) = write_timeout {
                                    match tokio::time::timeout(timeout_dur, write_half.write_all(&encoded)).await {
                                        Ok(r) => r,
                                        Err(_) => {
                                            tracing::warn!(
                                                replica_id = lag_replica_id,
                                                timeout_ms = timeout_dur.as_millis() as u64,
                                                "Write to replica timed out (direct channel), disconnecting"
                                            );
                                            break;
                                        }
                                    }
                                } else {
                                    write_half.write_all(&encoded).await
                                };
                                if let Err(e) = write_result {
                                    tracing::warn!(error = %e, "Error writing to replica");
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        tokio::select! {
            _ = read_task => {}
            _ = write_task => {}
        }
        Ok(())
    }
}

/// Build a minimal valid RDB suitable for empty databases or fallbacks.
pub(crate) fn create_minimal_rdb() -> Vec<u8> {
    let mut rdb = Vec::new();
    // Magic + version
    rdb.extend_from_slice(b"REDIS");
    rdb.extend_from_slice(b"0011");
    // AUX redis-ver:7.2.0
    rdb.push(RDB_OPCODE_AUX);
    rdb.extend_from_slice(b"\x09redis-ver");
    rdb.extend_from_slice(b"\x057.2.0");
    // SELECTDB 0
    rdb.push(RDB_OPCODE_SELECTDB);
    rdb.push(0x00);
    // RESIZEDB 0,0
    rdb.push(RDB_OPCODE_RESIZEDB);
    rdb.push(0x00);
    rdb.push(0x00);
    // EOF + 8-byte CRC64 (zeros — not validated by FrogDB)
    rdb.push(RDB_OPCODE_EOF);
    rdb.extend_from_slice(&[0u8; 8]);
    rdb
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
    use crate::primary::SplitBrainBufferConfig;
    use crate::primary::{LagThresholdConfig, PrimaryReplicationHandler};
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use frogdb_persistence::{RocksConfig, RocksStore};
    use std::net::SocketAddr;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    fn addr() -> SocketAddr {
        "127.0.0.1:9001".parse().unwrap()
    }

    fn make_handler(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
    ) -> Arc<PrimaryReplicationHandler> {
        Arc::new(PrimaryReplicationHandler::new(
            ReplicationState::new(),
            tracker,
            rocks,
            data_dir,
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            SplitBrainBufferConfig {
                enabled: false,
                max_entries: 0,
                max_bytes: 0,
            },
            0,
        ))
    }

    /// Streaming drop: a partial sync that completes and enters `Streaming`,
    /// then the replica disconnects. The exit handler must remove the session
    /// from the tracker and clear the connection handle.
    #[tokio::test]
    async fn run_cleans_up_on_streaming_drop_partial() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());

        let (mut client, server) = tokio::io::duplex(1024);
        let session = tracker.register_replica(addr());
        let session_id = session.id();

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(server, SyncKind::Partial { offset: 0 }, handler)
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
        assert!(handler.connections.read().await.get(&session_id).is_none());
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
        let session_id = session.id();

        let server: BoxedStream = Box::new(server);
        let result = session
            .clone()
            .run(server, SyncKind::Partial { offset: 0 }, handler.clone())
            .await;

        assert!(result.is_err(), "expected write_all to error after drop");
        assert_eq!(tracker.replica_count(), 0);
        assert!(handler.connections.read().await.get(&session_id).is_none());
        assert_eq!(session.phase(), Phase::Disconnecting);
    }

    /// Full sync without a RocksStore: emits FULLRESYNC + minimal RDB, then
    /// enters streaming. Closing the client triggers normal cleanup; no
    /// checkpoint directory should exist (it was never created).
    #[tokio::test]
    async fn run_full_sync_minimal_rdb_path() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().await.replication_id.clone();

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
                            replication_id: repl_id,
                            current_offset: 0,
                        },
                        handler,
                    )
                    .await
            }
        });

        // Read until we've consumed FULLRESYNC + the minimal RDB.
        let mut all = Vec::new();
        let mut buf = [0u8; 4096];
        let mut saw_redis_magic = false;
        for _ in 0..10 {
            let n = client.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            all.extend_from_slice(&buf[..n]);
            if all.windows(5).any(|w| w == b"REDIS") {
                saw_redis_magic = true;
                break;
            }
        }
        assert!(saw_redis_magic, "should see REDIS magic in the RDB");
        assert!(all.starts_with(b"+FULLRESYNC"));

        drop(client);
        let result = task.await.unwrap();
        assert!(result.is_ok());

        assert_eq!(tracker.replica_count(), 0);
        // Minimal-RDB path never sets sync_checkpoint_path, so no dir to clean.
        assert!(session.inner.read().sync_checkpoint_path.is_none());
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
        let repl_id = handler.state.read().await.replication_id.clone();

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
                            current_offset: 0,
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
        assert!(handler.connections.read().await.get(&session_id).is_none());
        assert_eq!(session.phase(), Phase::Disconnecting);
        assert!(
            !expected_checkpoint.exists(),
            "checkpoint dir should have been removed by exit handler: {}",
            expected_checkpoint.display()
        );
    }

    #[test]
    fn force_phase_for_test_drives_phase() {
        let session = ReplicaSession::new(1, addr());
        assert_eq!(session.phase(), Phase::Connecting);
        session.force_phase_for_test(Phase::Streaming);
        assert_eq!(session.phase(), Phase::Streaming);
    }

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
}
