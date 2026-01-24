//! Primary node replication handling.
//!
//! This module handles the primary side of replication:
//! - Accepting replica connections
//! - Processing PSYNC requests
//! - Streaming WAL updates to replicas
//! - Handling REPLCONF ACKs

use bytes::{Bytes, BytesMut};
use frogdb_core::{
    ReplicationBroadcaster, ReplicationFrame, ReplicationState, ReplicationTracker,
    ReplicationTrackerImpl, RocksStore, replication::tracker::ReplicaState, serialize_command_to_resp,
};
use std::path::PathBuf;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, RwLock};

/// Primary replication handler.
///
/// Manages all replica connections and coordinates WAL streaming.
pub struct PrimaryReplicationHandler {
    /// Replication state (IDs and offsets)
    state: Arc<RwLock<ReplicationState>>,

    /// Replica tracker for ACKs and synchronous replication
    tracker: Arc<ReplicationTrackerImpl>,

    /// Channel for broadcasting WAL frames to all replicas
    wal_broadcast: broadcast::Sender<ReplicationFrame>,

    /// Active replica connections
    connections: Arc<RwLock<HashMap<u64, ReplicaConnectionHandle>>>,

    /// Optional RocksDB store for FULLRESYNC checkpoint streaming.
    /// If None, only minimal RDB is sent (for in-memory mode).
    rocks_store: Option<Arc<RocksStore>>,

    /// Directory for storing temporary checkpoint data.
    data_dir: PathBuf,
}

/// Handle to a replica connection.
struct ReplicaConnectionHandle {
    /// Replica ID
    replica_id: u64,

    /// Replica address
    address: SocketAddr,

    /// Channel to send frames to this replica
    frame_tx: mpsc::Sender<ReplicationFrame>,

    /// Connection state
    state: ReplicaState,

    /// Connected at timestamp
    connected_at: Instant,
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
    ) -> Self {
        let (wal_broadcast, _) = broadcast::channel(10000);

        Self {
            state: Arc::new(RwLock::new(state)),
            tracker,
            wal_broadcast,
            connections: Arc::new(RwLock::new(HashMap::new())),
            rocks_store,
            data_dir,
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

    /// Handle full synchronization (FULLRESYNC).
    async fn handle_full_sync(&self, mut stream: TcpStream, addr: SocketAddr) -> io::Result<()> {
        let state = self.state.read().await;

        // Send FULLRESYNC response
        let response = format!(
            "+FULLRESYNC {} {}\r\n",
            state.replication_id, state.replication_offset
        );
        stream.write_all(response.as_bytes()).await?;

        let current_offset = state.replication_offset;
        drop(state);

        // Register replica in syncing state
        let replica_id = self.tracker.register_replica(addr);
        self.tracker.set_state(replica_id, ReplicaState::Syncing);

        // Stream database snapshot to replica
        if let Some(ref rocks) = self.rocks_store {
            // Create checkpoint for FULLRESYNC
            let checkpoint_path = self.data_dir.join(format!("fullsync_{}", replica_id));

            // Create checkpoint in blocking task (Checkpoint is !Send)
            let rocks_clone = rocks.clone();
            let path_clone = checkpoint_path.clone();
            let checkpoint_result = tokio::task::spawn_blocking(move || {
                rocks_clone.create_checkpoint(&path_clone)
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            if let Err(e) = checkpoint_result {
                tracing::error!(error = %e, "Failed to create checkpoint for FULLRESYNC");
                // Fall back to minimal RDB
                let empty_rdb = create_minimal_rdb();
                let rdb_header = format!("${}\r\n", empty_rdb.len());
                stream.write_all(rdb_header.as_bytes()).await?;
                stream.write_all(&empty_rdb).await?;
            } else {
                // TODO: Stream checkpoint files to replica
                // For now, send minimal RDB as placeholder
                // Future implementation will:
                // 1. Calculate total size of checkpoint files
                // 2. Send metadata (size, checksum)
                // 3. Stream each file using stream_file_to_writer
                // 4. Clean up checkpoint directory
                tracing::info!(
                    checkpoint_path = %checkpoint_path.display(),
                    "Checkpoint created for FULLRESYNC (streaming not yet implemented)"
                );

                let empty_rdb = create_minimal_rdb();
                let rdb_header = format!("${}\r\n", empty_rdb.len());
                stream.write_all(rdb_header.as_bytes()).await?;
                stream.write_all(&empty_rdb).await?;

                // Clean up checkpoint
                if let Err(e) = tokio::fs::remove_dir_all(&checkpoint_path).await {
                    tracing::warn!(
                        checkpoint_path = %checkpoint_path.display(),
                        error = %e,
                        "Failed to clean up checkpoint directory"
                    );
                }
            }
        } else {
            // No persistence - send minimal RDB
            let empty_rdb = create_minimal_rdb();
            let rdb_header = format!("${}\r\n", empty_rdb.len());
            stream.write_all(rdb_header.as_bytes()).await?;
            stream.write_all(&empty_rdb).await?;
        }

        tracing::info!(
            replica_id = replica_id,
            addr = %addr,
            offset = current_offset,
            "Completed FULLRESYNC"
        );

        // Update state to streaming
        self.tracker.set_state(replica_id, ReplicaState::Streaming);

        // Start streaming WAL updates
        self.start_streaming(stream, addr, replica_id).await
    }

    /// Start streaming WAL updates to a replica.
    async fn start_streaming(
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
                replica_id,
                address: addr,
                frame_tx,
                state: ReplicaState::Streaming,
                connected_at: Instant::now(),
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

        // Write task - forward frames to replica
        let write_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Receive frame from broadcast channel
                    frame = wal_rx.recv() => {
                        match frame {
                            Ok(frame) => {
                                let encoded = frame.encode();
                                if let Err(e) = write_half.write_all(&encoded).await {
                                    tracing::warn!(error = %e, "Error writing to replica");
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(lagged = n, "Replica lagged in WAL stream");
                                // TODO: Consider triggering full resync
                            }
                        }
                    }

                    // Receive frame from direct channel (for GETACK, etc.)
                    frame = frame_rx.recv() => {
                        match frame {
                            Some(frame) => {
                                let encoded = frame.encode();
                                if let Err(e) = write_half.write_all(&encoded).await {
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
}

/// Create a minimal valid RDB file.
///
/// This is used for empty databases during FULLRESYNC.
fn create_minimal_rdb() -> Vec<u8> {
    let mut rdb = Vec::new();

    // Magic string
    rdb.extend_from_slice(b"REDIS");

    // RDB version (e.g., 0011 for version 11)
    rdb.extend_from_slice(b"0011");

    // Auxiliary fields (optional, but good for compatibility)
    // FA = auxiliary field
    rdb.push(0xFA);
    // redis-ver
    rdb.extend_from_slice(b"\x09redis-ver");
    rdb.extend_from_slice(b"\x057.2.0");

    // FE = database selector
    rdb.push(0xFE);
    rdb.push(0x00); // DB 0

    // FB = resizedb (hash table sizes)
    rdb.push(0xFB);
    rdb.push(0x00); // DB hash table size
    rdb.push(0x00); // Expires hash table size

    // FF = EOF
    rdb.push(0xFF);

    // 8-byte checksum (CRC64)
    // For simplicity, we'll use zeros. A real implementation would calculate CRC64.
    rdb.extend_from_slice(&[0u8; 8]);

    rdb
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
}
