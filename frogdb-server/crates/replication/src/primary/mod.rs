//! Primary node replication handling.
//!
//! This module handles the primary side of replication:
//! - Accepting replica connections
//! - Processing PSYNC requests
//! - Streaming WAL updates to replicas
//! - Handling REPLCONF ACKs

pub mod ring_buffer;
mod streaming;
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
use crate::state::ReplicationState;
use crate::tracker::{ReplicaState, ReplicationTrackerImpl};

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

/// How often to check lag thresholds (every N frames).
const LAG_CHECK_INTERVAL: u64 = 100;

/// Primary replication handler.
///
/// Manages all replica connections and coordinates WAL streaming.
pub struct PrimaryReplicationHandler {
    /// Replication state (IDs and offsets)
    pub(crate) state: Arc<RwLock<ReplicationState>>,
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
}

/// Handle to a replica connection.
#[allow(dead_code)]
pub(crate) struct ReplicaConnectionHandle {
    pub(crate) _replica_id: u64,
    pub(crate) _address: SocketAddr,
    pub(crate) _frame_tx: mpsc::Sender<ReplicationFrame>,
    pub(crate) _state: ReplicaState,
    pub(crate) _connected_at: Instant,
}

impl PrimaryReplicationHandler {
    /// Create a new primary replication handler.
    pub fn new(
        state: ReplicationState,
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
        Self {
            state: Arc::new(RwLock::new(state)),
            tracker,
            wal_broadcast,
            connections: Arc::new(RwLock::new(HashMap::new())),
            rocks_store,
            data_dir,
            lag_config,
            ring_buffer,
            write_timeout_ms,
        }
    }

    pub async fn state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }
    pub fn tracker(&self) -> Arc<ReplicationTrackerImpl> {
        self.tracker.clone()
    }

    /// Handle a new replica connection.
    pub async fn handle_psync(
        &self,
        stream: BoxedStream,
        addr: SocketAddr,
        replication_id: &str,
        offset: i64,
    ) -> io::Result<()> {
        let state = self.state.read().await;
        let can_partial = if replication_id == "?" && offset == -1 {
            false
        } else {
            offset >= 0 && state.can_partial_sync(replication_id, offset as u64)
        };
        drop(state);
        if can_partial {
            self.handle_partial_sync(stream, addr, offset as u64).await
        } else {
            self.handle_full_sync(stream, addr).await
        }
    }

    pub fn broadcast_frame(&self, frame: ReplicationFrame) {
        let _ = self.wal_broadcast.send(frame);
    }

    pub async fn request_acks(&self) {
        let payload = serialize_command_to_resp(
            "REPLCONF",
            &[Bytes::from_static(b"GETACK"), Bytes::from_static(b"*")],
        );
        self.broadcast_frame(ReplicationFrame::new(0, payload));
    }

    pub fn replica_count(&self) -> usize {
        self.tracker.replica_count()
    }
    pub async fn current_offset(&self) -> u64 {
        self.state.read().await.replication_offset
    }

    pub async fn increment_offset(&self, bytes: u64) -> u64 {
        let mut state = self.state.write().await;
        state.increment_offset(bytes);
        let new_offset = state.replication_offset;
        self.tracker.set_offset(new_offset);
        new_offset
    }

    pub async fn replication_id(&self) -> String {
        self.state.read().await.replication_id.clone()
    }
    pub fn current_offset_sync(&self) -> u64 {
        self.tracker.current_offset()
    }
}

impl ReplicationBroadcaster for PrimaryReplicationHandler {
    fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64 {
        let resp_bytes = serialize_command_to_resp(cmd_name, args);
        let bytes_len = resp_bytes.len() as u64;
        let new_offset = self.tracker.increment_offset(bytes_len);
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
        self.tracker.current_offset()
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
