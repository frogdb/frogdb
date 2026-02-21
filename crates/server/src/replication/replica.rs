//! Replica node replication handling.
//!
//! This module handles the replica side of replication:
//! - Connecting to the primary
//! - PSYNC handshake
//! - Receiving WAL updates
//! - Sending ACKs

use bytes::BytesMut;
use frogdb_core::{ReplicationFrame, ReplicationFrameCodec, ReplicationState};
use serde_json;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tokio_util::codec::Decoder;

use crate::replication::fullsync::{receive_to_file, FullSyncMetadata};

/// Connection timeout for initial connection
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Handshake timeout
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// ACK interval - how often to send ACKs to primary
const ACK_INTERVAL: Duration = Duration::from_secs(1);

/// Replica replication handler.
///
/// Manages the connection to the primary and processes replication data.
pub struct ReplicaReplicationHandler {
    /// Primary address
    primary_addr: SocketAddr,

    /// Our listening port
    listening_port: u16,

    /// Replication state
    state: Arc<RwLock<ReplicationState>>,

    /// Channel to receive WAL frames for processing
    frame_tx: mpsc::Sender<ReplicationFrame>,

    /// Shutdown signal
    shutdown: tokio::sync::watch::Sender<bool>,

    /// Directory for storing checkpoint data
    data_dir: PathBuf,
}

/// Connection to the primary for replication.
pub struct ReplicaConnection {
    /// TCP stream to primary
    stream: TcpStream,

    /// Primary address
    _primary_addr: SocketAddr,

    /// Replication state
    state: Arc<RwLock<ReplicationState>>,

    /// Current connection state
    connection_state: ConnectionState,

    /// Directory for storing checkpoint data
    data_dir: PathBuf,
}

/// State of the replica connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// TCP connection established
    Connected,
    /// Sent AUTH (if configured)
    Authenticating,
    /// Exchanging REPLCONF
    Handshaking,
    /// Receiving FULLRESYNC data
    Syncing,
    /// Receiving incremental WAL updates
    Streaming,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "disconnected"),
            ConnectionState::Connected => write!(f, "connected"),
            ConnectionState::Authenticating => write!(f, "authenticating"),
            ConnectionState::Handshaking => write!(f, "handshaking"),
            ConnectionState::Syncing => write!(f, "syncing"),
            ConnectionState::Streaming => write!(f, "streaming"),
        }
    }
}

impl ReplicaReplicationHandler {
    /// Create a new replica replication handler.
    pub fn new(
        primary_addr: SocketAddr,
        listening_port: u16,
        state: ReplicationState,
        data_dir: PathBuf,
    ) -> (Self, mpsc::Receiver<ReplicationFrame>) {
        let (frame_tx, frame_rx) = mpsc::channel(10000);
        let (shutdown, _) = tokio::sync::watch::channel(false);

        let handler = Self {
            primary_addr,
            listening_port,
            state: Arc::new(RwLock::new(state)),
            frame_tx,
            shutdown,
            data_dir,
        };

        (handler, frame_rx)
    }

    /// Start replication - connects to primary and begins streaming.
    pub async fn start(&self) -> io::Result<()> {
        let mut backoff = Duration::from_millis(100);
        let max_backoff = Duration::from_secs(30);

        loop {
            match self.connect_and_sync().await {
                Ok(()) => {
                    // Normal disconnection or shutdown
                    tracing::info!("Replication connection closed normally");
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "Replication connection failed, retrying"
                    );

                    tokio::time::sleep(backoff).await;

                    // Exponential backoff with cap
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }

        Ok(())
    }

    /// Connect to primary and perform sync.
    async fn connect_and_sync(&self) -> io::Result<()> {
        // Connect to primary
        let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(self.primary_addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection timeout"))??;

        tracing::info!(
            primary = %self.primary_addr,
            "Connected to primary"
        );

        let mut conn = ReplicaConnection {
            stream,
            _primary_addr: self.primary_addr,
            state: self.state.clone(),
            connection_state: ConnectionState::Connected,
            data_dir: self.data_dir.clone(),
        };

        // Perform handshake
        conn.handshake(self.listening_port).await?;

        // Perform PSYNC
        let sync_type = conn.psync().await?;

        match sync_type {
            SyncType::FullSyncRdb { rdb_size } => {
                // Receive minimal RDB
                conn.receive_rdb(rdb_size).await?;
            }
            SyncType::FullSyncCheckpoint { file_count } => {
                // Receive FrogDB checkpoint
                conn.receive_checkpoint(file_count).await?;
            }
            SyncType::PartialSync => {
                // No RDB needed, continue to streaming
            }
        }

        // Start streaming
        conn.stream_replication(&self.frame_tx).await
    }

    /// Stop replication.
    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Get current replication state.
    pub async fn state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }
}

/// Type of sync initiated by PSYNC.
enum SyncType {
    /// Full sync with minimal RDB (size known upfront)
    FullSyncRdb { rdb_size: usize },
    /// Full sync with FrogDB checkpoint streaming
    FullSyncCheckpoint { file_count: usize },
    /// Partial sync - continue from current offset
    PartialSync,
}

impl ReplicaConnection {
    /// Perform the REPLCONF handshake.
    async fn handshake(&mut self, listening_port: u16) -> io::Result<()> {
        self.connection_state = ConnectionState::Handshaking;

        // Send REPLCONF listening-port
        let cmd = format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
            listening_port.to_string().len(),
            listening_port
        );
        self.stream.write_all(cmd.as_bytes()).await?;

        // Read OK response
        self.read_ok_response().await?;

        // Send REPLCONF capa eof psync2
        let cmd =
            "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        self.stream.write_all(cmd.as_bytes()).await?;

        // Read OK response
        self.read_ok_response().await?;

        tracing::debug!("REPLCONF handshake complete");
        Ok(())
    }

    /// Perform PSYNC to initiate synchronization.
    async fn psync(&mut self) -> io::Result<SyncType> {
        let state = self.state.read().await;

        // Build PSYNC command
        let (repl_id, offset) = if state.replication_offset == 0 {
            // First sync - request full sync
            ("?".to_string(), -1i64)
        } else {
            // Try partial sync
            (
                state.replication_id.clone(),
                state.replication_offset as i64,
            )
        };

        drop(state);

        let cmd = format!(
            "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            repl_id.len(),
            repl_id,
            offset.to_string().len(),
            offset
        );

        self.stream.write_all(cmd.as_bytes()).await?;

        // Read response
        let mut reader = BufReader::new(&mut self.stream);
        let mut line_buf = String::new();
        reader.read_line(&mut line_buf).await?;

        let line = line_buf.trim();

        if line.starts_with("+FULLRESYNC") {
            // Parse: +FULLRESYNC <replication_id> <offset>
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                let new_repl_id = parts[1].to_string();
                let new_offset: u64 = parts[2].parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid offset in FULLRESYNC")
                })?;

                // Update state
                let mut state = self.state.write().await;
                state.replication_id = new_repl_id.clone();
                state.replication_offset = new_offset;
                drop(state);

                tracing::info!(
                    replication_id = %new_repl_id,
                    offset = new_offset,
                    "FULLRESYNC initiated"
                );

                self.connection_state = ConnectionState::Syncing;

                // Read next line to determine sync type: $<length>\r\n or $FROGDB_CHECKPOINT\r\n
                line_buf.clear();
                reader.read_line(&mut line_buf).await?;
                let line = line_buf.trim();

                if !line.starts_with('$') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected RDB size prefix or checkpoint marker",
                    ));
                }

                let marker = &line[1..];
                if marker == "FROGDB_CHECKPOINT" {
                    // FrogDB checkpoint protocol
                    // Next line is file count
                    line_buf.clear();
                    reader.read_line(&mut line_buf).await?;
                    let file_count: usize = line_buf.trim().parse().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid file count in checkpoint",
                        )
                    })?;

                    tracing::info!(file_count = file_count, "FrogDB checkpoint FULLRESYNC");

                    Ok(SyncType::FullSyncCheckpoint { file_count })
                } else {
                    // Traditional minimal RDB protocol
                    let rdb_size: usize = marker.parse().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid RDB size")
                    })?;

                    Ok(SyncType::FullSyncRdb { rdb_size })
                }
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed FULLRESYNC response",
                ))
            }
        } else if line.starts_with("+CONTINUE") {
            // Partial sync accepted
            // May include new replication ID: +CONTINUE <replication_id>
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let new_repl_id = parts[1].to_string();
                let mut state = self.state.write().await;
                state.replication_id = new_repl_id.clone();
                tracing::info!(
                    replication_id = %new_repl_id,
                    "Partial sync with new replication ID"
                );
            }

            self.connection_state = ConnectionState::Streaming;
            tracing::info!("Partial sync (CONTINUE) initiated");

            Ok(SyncType::PartialSync)
        } else if line.starts_with("-") {
            // Error response
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("PSYNC error: {}", &line[1..]),
            ))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected PSYNC response: {}", line),
            ))
        }
    }

    /// Receive RDB data during FULLRESYNC.
    async fn receive_rdb(&mut self, rdb_size: usize) -> io::Result<()> {
        tracing::info!(size = rdb_size, "Receiving RDB data");

        // Read RDB data
        let mut rdb_data = vec![0u8; rdb_size];
        self.stream.read_exact(&mut rdb_data).await?;

        // TODO: Load RDB data into store
        // For now, we just validate the header

        if rdb_data.len() >= 9 && &rdb_data[0..5] == b"REDIS" {
            tracing::info!(size = rdb_data.len(), "RDB data received successfully");
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RDB header",
            ));
        }

        self.connection_state = ConnectionState::Streaming;
        Ok(())
    }

    /// Receive FrogDB checkpoint during FULLRESYNC.
    ///
    /// Protocol:
    /// 1. Already received $FROGDB_CHECKPOINT and file count
    /// 2. For each file:
    ///    a. Receive filename as bulk string
    ///    b. Receive file size as bulk string
    ///    c. Receive file contents
    /// 3. Receive metadata (replication_id:offset:checksum)
    async fn receive_checkpoint(&mut self, file_count: usize) -> io::Result<()> {
        tracing::info!(file_count = file_count, "Receiving FrogDB checkpoint");

        // Create checkpoint directory as a sibling to the data directory
        // This is because data_dir IS the RocksDB directory
        let parent_dir = self.data_dir.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "data_dir has no parent directory",
            )
        })?;
        let checkpoint_dir = parent_dir.join("checkpoint_incoming");
        fs::create_dir_all(&checkpoint_dir).await?;

        let mut total_bytes = 0u64;
        let mut reader = BufReader::new(&mut self.stream);

        // Receive each file
        for i in 0..file_count {
            // Read filename length: $<len>\r\n
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            let filename_len: usize =
                line.trim().trim_start_matches('$').parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid filename length")
                })?;

            // Read filename
            let mut filename_buf = vec![0u8; filename_len + 2]; // +2 for \r\n
            reader.read_exact(&mut filename_buf).await?;
            let filename = String::from_utf8_lossy(&filename_buf[..filename_len]).to_string();

            // Read file size: $<size>\r\n
            line.clear();
            reader.read_line(&mut line).await?;
            let file_size: u64 = line
                .trim()
                .trim_start_matches('$')
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid file size"))?;

            // Receive file contents
            let file_path = checkpoint_dir.join(&filename);
            let checksum = receive_to_file(
                reader.get_mut(),
                &file_path,
                file_size,
                None, // No progress tracking for now
            )
            .await?;

            total_bytes += file_size;

            tracing::debug!(
                file = i + 1,
                filename = %filename,
                size = file_size,
                checksum = %hex::encode(&checksum[..8]),
                "Received checkpoint file"
            );
        }

        // Read metadata: $<len>\r\n<metadata>\r\n
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let metadata_len: usize =
            line.trim().trim_start_matches('$').parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid metadata length")
            })?;

        let mut metadata_buf = vec![0u8; metadata_len + 2]; // +2 for \r\n
        reader.read_exact(&mut metadata_buf).await?;
        let metadata = FullSyncMetadata::from_bytes(&metadata_buf[..metadata_len])?;

        tracing::info!(
            total_bytes = total_bytes,
            files = file_count,
            replication_id = %metadata.replication_id,
            offset = metadata.replication_offset,
            "Checkpoint received successfully"
        );

        // Verify checksum
        // TODO: For now we trust the checksum, but we should verify against received files

        // Stage checkpoint for loading
        // Move from "checkpoint_incoming" to "checkpoint_ready" to signal it's complete
        // Both are sibling directories to the data_dir (which is the RocksDB directory)
        let checkpoint_ready_dir = parent_dir.join("checkpoint_ready");

        // Remove any existing staged checkpoint
        if checkpoint_ready_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&checkpoint_ready_dir).await {
                tracing::warn!(error = %e, "Failed to remove old staged checkpoint");
            }
        }

        // Move the incoming checkpoint to the ready location
        if let Err(e) = fs::rename(&checkpoint_dir, &checkpoint_ready_dir).await {
            tracing::error!(error = %e, "Failed to stage checkpoint for loading");
            // Clean up on failure
            let _ = fs::remove_dir_all(&checkpoint_dir).await;
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to stage checkpoint: {}", e),
            ));
        }

        // Write metadata to indicate replication state after loading
        let metadata_path = checkpoint_ready_dir.join("replication_metadata.json");
        let metadata_json = serde_json::json!({
            "replication_id": metadata.replication_id,
            "replication_offset": metadata.replication_offset,
            "checksum": hex::encode(&metadata.checksum),
        });
        if let Err(e) = fs::write(&metadata_path, metadata_json.to_string()).await {
            tracing::warn!(error = %e, "Failed to write replication metadata");
        }

        tracing::info!(
            checkpoint_dir = %checkpoint_ready_dir.display(),
            replication_id = %metadata.replication_id,
            offset = metadata.replication_offset,
            "Checkpoint staged for loading - server restart required to apply"
        );

        // Update replication state
        {
            let mut state = self.state.write().await;
            state.replication_id = metadata.replication_id.clone();
            state.replication_offset = metadata.replication_offset;
        }

        // Signal that a restart is required to load the checkpoint.
        //
        // Hot-reload would require pausing all shard workers, replacing the
        // RocksStore, reloading all data, and resuming - which is complex.
        // Instead, we return an error to trigger a graceful disconnect.
        // The server should be configured with a process manager (systemd,
        // Docker, k8s) that will restart it automatically. On restart,
        // RocksStore::load_staged_checkpoint() will load the checkpoint data.
        tracing::warn!(
            "Checkpoint received and staged. Server restart required to apply. \
             If running under a process manager, the server will restart automatically."
        );

        // Return error to break replication loop and trigger reconnection
        // On restart, the checkpoint will be loaded via RocksStore::load_staged_checkpoint()
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "checkpoint_received_restart_required",
        ));
    }

    /// Stream replication data from primary.
    async fn stream_replication(
        &mut self,
        frame_tx: &mpsc::Sender<ReplicationFrame>,
    ) -> io::Result<()> {
        tracing::info!("Starting replication stream");

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::with_capacity(64 * 1024);

        // ACK ticker
        let mut ack_interval = tokio::time::interval(ACK_INTERVAL);

        loop {
            tokio::select! {
                // Read from primary
                result = self.stream.read_buf(&mut buf) => {
                    match result {
                        Ok(0) => {
                            // Connection closed
                            tracing::info!("Primary connection closed");
                            return Ok(());
                        }
                        Ok(_) => {
                            // Try to decode frames
                            while let Some(frame) = codec.decode(&mut buf)? {
                                // Update our offset
                                let mut state = self.state.write().await;
                                state.increment_offset(frame.encoded_size() as u64);
                                let offset = state.replication_offset;
                                drop(state);

                                tracing::trace!(
                                    sequence = frame.sequence,
                                    offset = offset,
                                    "Received replication frame"
                                );

                                // Forward frame for processing
                                if frame_tx.send(frame).await.is_err() {
                                    tracing::warn!("Frame channel closed");
                                    return Ok(());
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }

                // Send periodic ACK
                _ = ack_interval.tick() => {
                    let state = self.state.read().await;
                    let offset = state.replication_offset;
                    drop(state);

                    self.send_ack(offset).await?;
                }
            }
        }
    }

    /// Send REPLCONF ACK to primary.
    async fn send_ack(&mut self, offset: u64) -> io::Result<()> {
        let offset_str = offset.to_string();
        let cmd = format!(
            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
            offset_str.len(),
            offset_str
        );

        self.stream.write_all(cmd.as_bytes()).await?;

        tracing::trace!(offset = offset, "Sent ACK to primary");
        Ok(())
    }

    /// Read a simple +OK response.
    async fn read_ok_response(&mut self) -> io::Result<()> {
        let mut reader = BufReader::new(&mut self.stream);
        let mut line = String::new();

        timeout(HANDSHAKE_TIMEOUT, reader.read_line(&mut line))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handshake timeout"))??;

        let line = line.trim();
        if line == "+OK" {
            Ok(())
        } else if line.starts_with("-") {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("error response: {}", &line[1..]),
            ))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {}", line),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_display() {
        assert_eq!(format!("{}", ConnectionState::Disconnected), "disconnected");
        assert_eq!(format!("{}", ConnectionState::Streaming), "streaming");
    }

    #[tokio::test]
    async fn test_replica_handler_creation() {
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let state = ReplicationState::new();
        let data_dir = PathBuf::from("/tmp/frogdb-test");

        let (handler, _rx) = ReplicaReplicationHandler::new(addr, 6380, state, data_dir);

        let current_state = handler.state().await;
        assert!(!current_state.replication_id.is_empty());
    }
}
