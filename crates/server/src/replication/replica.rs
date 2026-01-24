//! Replica node replication handling.
//!
//! This module handles the replica side of replication:
//! - Connecting to the primary
//! - PSYNC handshake
//! - Receiving WAL updates
//! - Sending ACKs

use bytes::BytesMut;
use frogdb_core::{ReplicationFrame, ReplicationFrameCodec, ReplicationState};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tokio_util::codec::Decoder;

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
}

/// Connection to the primary for replication.
pub struct ReplicaConnection {
    /// TCP stream to primary
    stream: TcpStream,

    /// Primary address
    primary_addr: SocketAddr,

    /// Replication state
    state: Arc<RwLock<ReplicationState>>,

    /// Current connection state
    connection_state: ConnectionState,
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
    ) -> (Self, mpsc::Receiver<ReplicationFrame>) {
        let (frame_tx, frame_rx) = mpsc::channel(10000);
        let (shutdown, _) = tokio::sync::watch::channel(false);

        let handler = Self {
            primary_addr,
            listening_port,
            state: Arc::new(RwLock::new(state)),
            frame_tx,
            shutdown,
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
            primary_addr: self.primary_addr,
            state: self.state.clone(),
            connection_state: ConnectionState::Connected,
        };

        // Perform handshake
        conn.handshake(self.listening_port).await?;

        // Perform PSYNC
        let sync_type = conn.psync().await?;

        match sync_type {
            SyncType::FullSync { rdb_size } => {
                // Receive RDB
                conn.receive_rdb(rdb_size).await?;
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
    FullSync { rdb_size: usize },
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
        let cmd = "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
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
            (state.replication_id.clone(), state.replication_offset as i64)
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

                // Read RDB size: $<length>\r\n
                line_buf.clear();
                reader.read_line(&mut line_buf).await?;
                let line = line_buf.trim();

                if !line.starts_with('$') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected RDB size prefix",
                    ));
                }

                let rdb_size: usize = line[1..].parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid RDB size")
                })?;

                Ok(SyncType::FullSync { rdb_size })
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
            tracing::info!(
                size = rdb_data.len(),
                "RDB data received successfully"
            );
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RDB header",
            ));
        }

        self.connection_state = ConnectionState::Streaming;
        Ok(())
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

        let (handler, _rx) = ReplicaReplicationHandler::new(addr, 6380, state);

        let current_state = handler.state().await;
        assert!(!current_state.replication_id.is_empty());
    }
}
