//! Primary node replication handling.
//!
//! This module handles the primary side of replication:
//! - Accepting replica connections
//! - Processing PSYNC requests
//! - Streaming WAL updates to replicas
//! - Handling REPLCONF ACKs

use bytes::{Bytes, BytesMut};
use frogdb_persistence::RocksStore;
use frogdb_types::ReplicationTracker;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::ReplicationBroadcaster;
use crate::frame::{ReplicationFrame, serialize_command_to_resp};
use crate::state::ReplicationState;
use crate::tracker::{ReplicaState, ReplicationTrackerImpl};

/// Configuration for the split-brain replication ring buffer.
#[derive(Debug, Clone)]
pub struct SplitBrainBufferConfig {
    /// Whether split-brain logging is enabled.
    pub enabled: bool,
    /// Maximum number of recent commands to retain.
    pub max_entries: usize,
    /// Maximum memory in bytes for buffered commands.
    pub max_bytes: usize,
}

impl Default for SplitBrainBufferConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10_000,
            max_bytes: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// A single buffered command in the replication ring buffer.
struct BufferedCommand {
    offset: u64,
    resp_bytes: Bytes,
}

/// Bounded ring buffer that captures recent RESP-encoded commands with their
/// replication offsets. Used to recover divergent writes during split-brain detection.
pub struct ReplicationRingBuffer {
    entries: parking_lot::Mutex<VecDeque<BufferedCommand>>,
    max_entries: usize,
    current_bytes: AtomicUsize,
    max_bytes: usize,
}

impl ReplicationRingBuffer {
    /// Create a new ring buffer with the given capacity limits.
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: parking_lot::Mutex::new(VecDeque::with_capacity(
                max_entries.min(1024), // Don't pre-allocate more than 1K entries
            )),
            max_entries,
            current_bytes: AtomicUsize::new(0),
            max_bytes,
        }
    }

    /// Push a new command into the buffer, evicting oldest entries if capacity is exceeded.
    pub fn push(&self, offset: u64, resp_bytes: Bytes) {
        let entry_size = resp_bytes.len();
        let mut entries = self.entries.lock();

        // Evict oldest entries if we'd exceed limits
        while entries.len() >= self.max_entries
            || (self.current_bytes.load(Ordering::Relaxed) + entry_size > self.max_bytes
                && !entries.is_empty())
        {
            if let Some(evicted) = entries.pop_front() {
                self.current_bytes
                    .fetch_sub(evicted.resp_bytes.len(), Ordering::Relaxed);
            }
        }

        self.current_bytes.fetch_add(entry_size, Ordering::Relaxed);
        entries.push_back(BufferedCommand { offset, resp_bytes });
    }

    /// Extract commands with offset > `last_replicated_offset`.
    /// Returns entries in order. Non-destructive (clones data).
    pub fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|cmd| cmd.offset > last_replicated_offset)
            .map(|cmd| (cmd.offset, cmd.resp_bytes.clone()))
            .collect()
    }
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
    wal_broadcast: broadcast::Sender<ReplicationFrame>,

    /// Active replica connections
    connections: Arc<RwLock<HashMap<u64, ReplicaConnectionHandle>>>,

    /// Optional RocksDB store for FULLRESYNC checkpoint streaming.
    /// If None, only minimal RDB is sent (for in-memory mode).
    pub(crate) rocks_store: Option<Arc<RocksStore>>,

    /// Directory for storing temporary checkpoint data.
    pub(crate) data_dir: PathBuf,

    /// Proactive lag-threshold disconnect configuration.
    lag_config: LagThresholdConfig,

    /// Ring buffer for split-brain divergent-write detection.
    ring_buffer: Option<ReplicationRingBuffer>,

    /// Timeout for write_all to replicas (ms). 0 = disabled.
    write_timeout_ms: u64,
}

/// Handle to a replica connection.
#[allow(dead_code)]
struct ReplicaConnectionHandle {
    /// Replica ID
    _replica_id: u64,

    /// Replica address
    _address: SocketAddr,

    /// Channel to send frames to this replica
    _frame_tx: mpsc::Sender<ReplicationFrame>,

    /// Connection state
    _state: ReplicaState,

    /// Connected at timestamp
    _connected_at: Instant,
}

impl PrimaryReplicationHandler {
    /// Create a new primary replication handler.
    ///
    /// # Arguments
    /// * `state` - Initial replication state
    /// * `tracker` - Replication tracker for ACK handling
    /// * `rocks_store` - Optional RocksDB store for checkpoint streaming
    /// * `data_dir` - Directory for storing temporary checkpoint data
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

    /// Get a reference to the replication state.
    pub async fn state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }

    /// Get a reference to the replication tracker.
    pub fn tracker(&self) -> Arc<ReplicationTrackerImpl> {
        self.tracker.clone()
    }

    /// Handle a new replica connection.
    ///
    /// This is called when a connection sends PSYNC.
    pub async fn handle_psync(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        replication_id: &str,
        offset: i64,
    ) -> io::Result<()> {
        let state = self.state.read().await;

        // Check if we can do partial sync
        let can_partial = if replication_id == "?" && offset == -1 {
            false // Explicit full sync request
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

    /// Handle partial synchronization (CONTINUE).
    async fn handle_partial_sync(
        &self,
        mut stream: TcpStream,
        addr: SocketAddr,
        offset: u64,
    ) -> io::Result<()> {
        let state = self.state.read().await;

        // Send CONTINUE response
        let response = format!("+CONTINUE {}\r\n", state.replication_id);
        stream.write_all(response.as_bytes()).await?;

        drop(state);

        // Register replica
        let replica_id = self.tracker.register_replica(addr);
        self.tracker.set_state(replica_id, ReplicaState::Streaming);

        // Record initial offset
        self.tracker.record_ack(replica_id, offset);

        // Start streaming
        self.start_streaming(stream, addr, replica_id).await
    }

    /// Start streaming WAL updates to a replica.
    pub(crate) async fn start_streaming(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        replica_id: u64,
    ) -> io::Result<()> {
        // Create channel for this replica
        let (frame_tx, mut frame_rx) = mpsc::channel::<ReplicationFrame>(1000);

        // Subscribe to WAL broadcast
        let mut wal_rx = self.wal_broadcast.subscribe();

        // Register connection handle
        {
            let handle = ReplicaConnectionHandle {
                _replica_id: replica_id,
                _address: addr,
                _frame_tx: frame_tx,
                _state: ReplicaState::Streaming,
                _connected_at: Instant::now(),
            };
            self.connections.write().await.insert(replica_id, handle);
        }

        // Split stream for reading and writing
        let (mut read_half, mut write_half) = stream.into_split();

        // Spawn read task (for REPLCONF ACK)
        let tracker = self.tracker.clone();
        let read_task = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);

            loop {
                match read_half.read_buf(&mut buf).await {
                    Ok(0) => {
                        // Connection closed
                        break;
                    }
                    Ok(_) => {
                        // Parse REPLCONF ACK responses
                        // Format: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
                        // For simplicity, look for the offset directly
                        if let Some(ack_offset) = parse_replconf_ack(&buf) {
                            tracker.record_ack(replica_id, ack_offset);
                            buf.clear();
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Error reading from replica");
                        break;
                    }
                }
            }
        });

        // Capture lag config for write task
        let lag_threshold_bytes = self.lag_config.threshold_bytes;
        let lag_threshold_secs = self.lag_config.threshold_secs;
        let lag_cooldown = self.lag_config.cooldown;
        let lag_tracker = self.tracker.clone();
        let lag_enabled = lag_threshold_bytes > 0 || lag_threshold_secs > 0;
        let write_timeout = if self.write_timeout_ms > 0 {
            Some(Duration::from_millis(self.write_timeout_ms))
        } else {
            None
        };

        // Write task - forward frames to replica
        let write_task = tokio::spawn(async move {
            let mut frame_count: u64 = 0;
            loop {
                tokio::select! {
                    // Receive frame from broadcast channel
                    frame = wal_rx.recv() => {
                        match frame {
                            Ok(frame) => {
                                let encoded = frame.encode();
                                let write_result = if let Some(timeout_dur) = write_timeout {
                                    match tokio::time::timeout(timeout_dur, write_half.write_all(&encoded)).await {
                                        Ok(r) => r,
                                        Err(_) => {
                                            tracing::warn!(
                                                replica_id = replica_id,
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

                                // Proactive lag check (amortized to every N frames)
                                if lag_enabled {
                                    frame_count += 1;
                                    if frame_count.is_multiple_of(LAG_CHECK_INTERVAL) {
                                        let byte_exceeded = lag_threshold_bytes > 0
                                            && lag_tracker.replica_lag(replica_id)
                                                .is_some_and(|lag| lag >= lag_threshold_bytes);
                                        let time_exceeded = lag_threshold_secs > 0
                                            && lag_tracker.replica_lag_secs(replica_id)
                                                .is_some_and(|secs| secs >= lag_threshold_secs as f64);

                                        if (byte_exceeded || time_exceeded)
                                            && !lag_tracker.is_in_lag_cooldown(replica_id, lag_cooldown)
                                        {
                                            tracing::warn!(
                                                replica_id = replica_id,
                                                byte_exceeded,
                                                time_exceeded,
                                                "Replica exceeded lag threshold, disconnecting for FULLRESYNC"
                                            );
                                            lag_tracker.record_lag_disconnect(replica_id);
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(
                                    replica_id = replica_id,
                                    lagged = n,
                                    "Replica lagged in WAL stream, disconnecting for resync"
                                );
                                break; // Exit write loop → triggers cleanup → replica reconnects → full resync
                            }
                        }
                    }

                    // Receive frame from direct channel (for GETACK, etc.)
                    frame = frame_rx.recv() => {
                        match frame {
                            Some(frame) => {
                                let encoded = frame.encode();
                                let write_result = if let Some(timeout_dur) = write_timeout {
                                    match tokio::time::timeout(timeout_dur, write_half.write_all(&encoded)).await {
                                        Ok(r) => r,
                                        Err(_) => {
                                            tracing::warn!(
                                                replica_id = replica_id,
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
                            None => {
                                // Channel closed
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Wait for either task to complete (indicates disconnect)
        tokio::select! {
            _ = read_task => {}
            _ = write_task => {}
        }

        // Cleanup
        self.connections.write().await.remove(&replica_id);
        self.tracker.unregister_replica(replica_id);

        tracing::info!(
            replica_id = replica_id,
            addr = %addr,
            "Replica disconnected"
        );

        Ok(())
    }

    /// Broadcast a WAL frame to all replicas.
    pub fn broadcast_frame(&self, frame: ReplicationFrame) {
        let _ = self.wal_broadcast.send(frame);
    }

    /// Send REPLCONF GETACK to all streaming replicas.
    pub async fn request_acks(&self) {
        // GETACK is sent as an inline command in the replication stream
        // *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n
        let getack_frame = ReplicationFrame::new(
            0,
            Bytes::from_static(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"),
        );
        self.broadcast_frame(getack_frame);
    }

    /// Get number of connected streaming replicas.
    pub fn replica_count(&self) -> usize {
        self.tracker.replica_count()
    }

    /// Get current replication offset.
    pub async fn current_offset(&self) -> u64 {
        self.state.read().await.replication_offset
    }

    /// Increment replication offset after a write operation.
    pub async fn increment_offset(&self, bytes: u64) -> u64 {
        let mut state = self.state.write().await;
        state.increment_offset(bytes);
        let new_offset = state.replication_offset;

        // Also update tracker
        self.tracker.set_offset(new_offset);

        new_offset
    }

    /// Get replication ID.
    pub async fn replication_id(&self) -> String {
        self.state.read().await.replication_id.clone()
    }

    /// Get the current replication offset synchronously (non-async).
    ///
    /// This reads from the tracker which stores the offset atomically,
    /// allowing it to be called from synchronous contexts.
    pub fn current_offset_sync(&self) -> u64 {
        self.tracker.current_offset()
    }
}

impl ReplicationBroadcaster for PrimaryReplicationHandler {
    fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64 {
        // Serialize the command to RESP format
        let resp_bytes = serialize_command_to_resp(cmd_name, args);
        let bytes_len = resp_bytes.len() as u64;

        // Increment offset atomically via the tracker
        let new_offset = self.tracker.increment_offset(bytes_len);

        // Push to ring buffer for split-brain detection
        if let Some(ref rb) = self.ring_buffer {
            rb.push(new_offset, resp_bytes.clone());
        }

        // Create and broadcast the frame
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

/// Parse REPLCONF ACK response from replica.
///
/// Format: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
fn parse_replconf_ack(data: &[u8]) -> Option<u64> {
    // Simple parsing - look for "ACK" followed by the offset
    let data_str = std::str::from_utf8(data).ok()?;

    // Find ACK in the data
    if let Some(ack_pos) = data_str.to_ascii_uppercase().find("ACK") {
        // Skip past ACK and find the offset value
        let after_ack = &data_str[ack_pos + 3..];

        // Look for the offset value after the next $<len>\r\n
        if let Some(dollar_pos) = after_ack.find('$') {
            let after_dollar = &after_ack[dollar_pos + 1..];
            if let Some(crlf_pos) = after_dollar.find("\r\n") {
                let after_len = &after_dollar[crlf_pos + 2..];
                if let Some(end_crlf) = after_len.find("\r\n") {
                    let offset_str = &after_len[..end_crlf];
                    return offset_str.parse().ok();
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint_stream::create_minimal_rdb;

    #[test]
    fn test_create_minimal_rdb() {
        let rdb = create_minimal_rdb();

        // Check magic string
        assert_eq!(&rdb[0..5], b"REDIS");

        // Check version
        assert_eq!(&rdb[5..9], b"0011");

        // Check EOF marker exists
        assert!(rdb.contains(&0xFF));
    }

    #[test]
    fn test_parse_replconf_ack() {
        // Standard RESP format
        let data = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$5\r\n12345\r\n";
        assert_eq!(parse_replconf_ack(data), Some(12345));

        // Large offset
        let data = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$10\r\n1234567890\r\n";
        assert_eq!(parse_replconf_ack(data), Some(1234567890));

        // Invalid data
        let data = b"INVALID";
        assert_eq!(parse_replconf_ack(data), None);
    }

    // ========================================================================
    // Ring buffer tests
    // ========================================================================

    #[test]
    fn test_ring_buffer_push_and_extract() {
        let rb = ReplicationRingBuffer::new(100, 1024 * 1024);

        rb.push(10, Bytes::from("cmd1"));
        rb.push(20, Bytes::from("cmd2"));
        rb.push(30, Bytes::from("cmd3"));

        // Extract everything after offset 0
        let writes = rb.extract_divergent_writes(0);
        assert_eq!(writes.len(), 3);
        assert_eq!(writes[0], (10, Bytes::from("cmd1")));
        assert_eq!(writes[1], (20, Bytes::from("cmd2")));
        assert_eq!(writes[2], (30, Bytes::from("cmd3")));

        // Extract only after offset 20
        let writes = rb.extract_divergent_writes(20);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0], (30, Bytes::from("cmd3")));

        // Extract after all offsets
        let writes = rb.extract_divergent_writes(30);
        assert!(writes.is_empty());
    }

    #[test]
    fn test_ring_buffer_entry_limit_eviction() {
        let rb = ReplicationRingBuffer::new(3, 1024 * 1024);

        rb.push(10, Bytes::from("cmd1"));
        rb.push(20, Bytes::from("cmd2"));
        rb.push(30, Bytes::from("cmd3"));
        rb.push(40, Bytes::from("cmd4")); // Should evict cmd1

        let writes = rb.extract_divergent_writes(0);
        assert_eq!(writes.len(), 3);
        assert_eq!(writes[0].0, 20); // cmd1 evicted
        assert_eq!(writes[2].0, 40);
    }

    #[test]
    fn test_ring_buffer_byte_limit_eviction() {
        // Allow only 10 bytes total
        let rb = ReplicationRingBuffer::new(100, 10);

        rb.push(10, Bytes::from("abcde")); // 5 bytes
        rb.push(20, Bytes::from("fghij")); // 5 bytes, total 10
        rb.push(30, Bytes::from("klmno")); // 5 bytes, must evict first to fit

        let writes = rb.extract_divergent_writes(0);
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].0, 20);
        assert_eq!(writes[1].0, 30);
    }

    #[test]
    fn test_ring_buffer_empty() {
        let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
        let writes = rb.extract_divergent_writes(0);
        assert!(writes.is_empty());
    }

    #[test]
    fn test_ring_buffer_extract_is_nondestructive() {
        let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
        rb.push(10, Bytes::from("cmd1"));

        let w1 = rb.extract_divergent_writes(0);
        let w2 = rb.extract_divergent_writes(0);
        assert_eq!(w1.len(), 1);
        assert_eq!(w2.len(), 1);
    }
}
