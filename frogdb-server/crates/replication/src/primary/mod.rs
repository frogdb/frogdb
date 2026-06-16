//! Primary node replication handling.
//!
//! This module handles the primary side of replication:
//! - Accepting replica connections
//! - Processing PSYNC requests
//! - Streaming WAL updates to replicas
//! - Handling REPLCONF ACKs

pub mod ring_buffer;
#[cfg(test)]
mod tests;

use bytes::Bytes;
use frogdb_persistence::RocksStore;
use frogdb_types::ReplicationTracker;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::BoxedStream;
use crate::ReplicationBroadcaster;
use crate::frame::{ReplicationFrame, serialize_command_to_resp};
use crate::offset_coordinator::OffsetCoordinator;
use crate::replica_session::SyncKind;
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;

pub use ring_buffer::{ReplicationRingBuffer, SplitBrainBufferConfig};

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
    /// Active replica connections
    pub(crate) connections: Arc<RwLock<HashMap<u64, ReplicaConnectionHandle>>>,
    /// Optional RocksDB store for FULLRESYNC checkpoint streaming.
    pub(crate) rocks_store: Option<Arc<RocksStore>>,
    /// Directory for storing temporary checkpoint data.
    pub(crate) data_dir: PathBuf,
    /// Proactive lag-threshold disconnect configuration.
    pub(crate) lag_config: LagThresholdConfig,
    /// Ring buffer for split-brain divergent-write detection.
    ring_buffer: Option<ReplicationRingBuffer>,
    /// Timeout for write_all to replicas (ms). 0 = disabled.
    pub(crate) write_timeout_ms: u64,
    /// Single owner of the replication-offset contract (live write position,
    /// per-replica acked offsets, and the persisted offset). All offset reads
    /// and the broadcast advance route through this seam instead of reaching
    /// into the tracker or `state.replication_offset` directly.
    pub(crate) offsets: Arc<OffsetCoordinator>,
}

/// Handle to a streaming replica connection.
///
/// Inserted into [`PrimaryReplicationHandler::connections`] by the session's
/// streaming-phase setup; removed by the session's exit handler.
#[allow(dead_code)]
pub(crate) struct ReplicaConnectionHandle {
    pub(crate) _replica_id: u64,
    pub(crate) _address: SocketAddr,
    pub(crate) _frame_tx: mpsc::Sender<ReplicationFrame>,
    pub(crate) _connected_at: Instant,
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
        let ring_buffer = if split_brain_config.enabled {
            Some(ReplicationRingBuffer::new(
                split_brain_config.max_entries,
                split_brain_config.max_bytes,
            ))
        } else {
            None
        };
        let state = Arc::new(RwLock::new(state));
        let offsets = Arc::new(OffsetCoordinator::new(tracker.clone(), state.clone()));
        Self {
            state,
            state_path,
            tracker,
            wal_broadcast,
            connections: Arc::new(RwLock::new(HashMap::new())),
            rocks_store,
            data_dir,
            lag_config,
            ring_buffer,
            write_timeout_ms,
            offsets,
        }
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
    /// not `state.replication_offset`). This couples offset durability to
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
        // Ask the coordinator whether the requested id/offset falls inside the
        // continuable window. The coordinator reads the live stream head itself
        // (the position advanced by `broadcast_command`) rather than this caller
        // having to know to fetch it from the tracker and thread it in — and the
        // FULLRESYNC offset, captured later in `handle_full`, keys off the same
        // live value so the granted offset corresponds to the data the replica
        // will receive.
        let offset_in_window = !(replication_id == "?" && offset == -1)
            && offset >= 0
            && self
                .offsets
                .can_serve_partial_sync(replication_id, offset as u64)
                .await;
        let current_repl_id = self.state.read().await.replication_id.clone();

        // A matching offset window is necessary but NOT sufficient to grant a
        // partial resync. Serving `+CONTINUE` also requires replaying the
        // backlog range `(requested_offset, current_offset]` to the replica, and
        // FrogDB has no replication backlog wired into the streaming path: the
        // live WAL stream is a `broadcast` tail carrying only *future* frames,
        // and the replica performs no offset-gap detection (see
        // `replica/streaming.rs`, which blindly advances its offset per frame).
        // Granting a partial sync without replaying the gap would therefore
        // silently drop those writes and diverge the replica — strictly worse
        // than a full resync. Until a backlog-replay path exists, force a full
        // resync even when the offset window matches. This makes the decision an
        // explicit structural limitation rather than an accident of reading a
        // stale offset that could never match.
        let can_partial = offset_in_window && self.partial_sync_replay_supported();

        let session = self.tracker.register_replica(addr);
        let sync_kind = if can_partial {
            SyncKind::Partial {
                offset: offset as u64,
            }
        } else {
            SyncKind::Full {
                replication_id: current_repl_id,
            }
        };
        session.run(stream, sync_kind, self.clone()).await
    }

    /// Whether the primary can serve a partial resync (`+CONTINUE`).
    ///
    /// Returns `false` unconditionally today: granting a partial resync requires
    /// replaying the backlog range between the replica's offset and the live
    /// stream head, and no such backlog is wired into the streaming path (see
    /// the detailed rationale in [`Self::handle_psync`]). This is the single,
    /// explicit gate for partial-sync support so the limitation is greppable and
    /// the offset-window check
    /// ([`crate::offset_coordinator::OffsetCoordinator::can_serve_partial_sync`])
    /// stays a correct, ready-to-use primitive for when replay is implemented.
    fn partial_sync_replay_supported(&self) -> bool {
        false
    }

    pub fn broadcast_frame(&self, frame: ReplicationFrame) {
        let _ = self.wal_broadcast.send(frame);
    }

    pub async fn request_acks(&self) {
        let resp_bytes = serialize_command_to_resp(
            "REPLCONF",
            &[Bytes::from_static(b"GETACK"), Bytes::from_static(b"*")],
        );
        // GETACK is part of the command stream (Redis-compatible): it advances the
        // offset on both ends. The replica counts it via `frame_advance`, so the
        // primary must advance + stamp it too (and record it in the backlog like
        // any other command); stamping sequence 0 here would diverge the offsets.
        let new_offset = self.offsets.advance_broadcast(resp_bytes.len() as u64);
        if let Some(ref rb) = self.ring_buffer {
            rb.push(new_offset, resp_bytes.clone());
        }
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
    pub fn current_offset_sync(&self) -> u64 {
        self.offsets.current()
    }
}

impl ReplicationBroadcaster for PrimaryReplicationHandler {
    fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64 {
        let resp_bytes = serialize_command_to_resp(cmd_name, args);
        let bytes_len = resp_bytes.len() as u64;
        let new_offset = self.offsets.advance_broadcast(bytes_len);
        if let Some(ref rb) = self.ring_buffer {
            rb.push(new_offset, resp_bytes.clone());
        }
        let frame = ReplicationFrame::new(new_offset, resp_bytes);
        self.broadcast_frame(frame);
        tracing::trace!(
            cmd = cmd_name,
            bytes = bytes_len,
            offset = new_offset,
            "Broadcast command to replicas"
        );
        new_offset
    }

    fn is_active(&self) -> bool {
        self.tracker.replica_count() > 0
    }
    fn current_offset(&self) -> u64 {
        self.offsets.current()
    }

    fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        match self.ring_buffer {
            Some(ref rb) => rb.extract_divergent_writes(last_replicated_offset),
            None => Vec::new(),
        }
    }
}

/// Parse a REPLCONF ACK response from a replica using proper RESP2 decoding.
///
/// Returns `Some((offset, consumed_bytes))` on success, or `None` if the buffer
/// does not contain a complete, valid REPLCONF ACK frame.
///
/// Expected wire format: `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n`
pub(crate) fn parse_replconf_ack(data: &[u8]) -> Option<(u64, usize)> {
    use redis_protocol::resp2::decode::decode;
    use redis_protocol::resp2::types::{OwnedFrame, Resp2Frame};

    let (frame, consumed) = decode(data).ok()??;
    if let OwnedFrame::Array(parts) = frame
        && parts.len() >= 3
    {
        let is_replconf = parts[0]
            .as_bytes()
            .is_some_and(|b: &[u8]| b.eq_ignore_ascii_case(b"REPLCONF"));
        let is_ack = parts[1]
            .as_bytes()
            .is_some_and(|b: &[u8]| b.eq_ignore_ascii_case(b"ACK"));
        if is_replconf && is_ack {
            let offset_str = std::str::from_utf8(parts[2].as_bytes()?).ok()?;
            let offset = offset_str.parse::<u64>().ok()?;
            return Some((offset, consumed));
        }
    }
    None
}
