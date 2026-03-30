//! Replica connection state machine.

use crate::frame::serialize_command_to_resp;
use crate::fullsync::{FullSyncMetadata, receive_to_file};
use crate::state::ReplicationState;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Read a single RESP simple-string line from the stream without buffering.
///
/// This reads byte-by-byte to avoid buffering past the line boundary, which is
/// critical during the PSYNC handshake where the stream transitions from
/// line-oriented RESP responses to bulk data (RDB/checkpoint) or FRPL frames.
async fn read_resp_line<R: AsyncReadExt + Unpin>(reader: &mut R) -> io::Result<String> {
    let mut buf = Vec::with_capacity(128);
    loop {
        let mut byte = [0u8; 1];
        let n = reader.read(&mut byte).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed while reading RESP line",
            ));
        }
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    String::from_utf8(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "RESP line is not valid UTF-8"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connected,
    Authenticating,
    Handshaking,
    Syncing,
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

pub(crate) enum SyncType {
    FullSyncRdb { rdb_size: usize },
    FullSyncCheckpoint { file_count: usize },
    PartialSync,
}

pub struct ReplicaConnection {
    pub(crate) stream: TcpStream,
    pub(crate) _primary_addr: SocketAddr,
    pub(crate) state: Arc<RwLock<ReplicationState>>,
    pub(crate) connection_state: ConnectionState,
    pub(crate) data_dir: PathBuf,
    pub(crate) shared_offset: Option<Arc<AtomicU64>>,
}

impl ReplicaConnection {
    pub(crate) async fn handshake(&mut self, listening_port: u16) -> io::Result<()> {
        self.connection_state = ConnectionState::Handshaking;
        let cmd = serialize_command_to_resp(
            "REPLCONF",
            &[
                Bytes::from_static(b"listening-port"),
                Bytes::from(listening_port.to_string()),
            ],
        );
        self.stream.write_all(&cmd).await?;
        self.read_ok_response().await?;
        let cmd = serialize_command_to_resp(
            "REPLCONF",
            &[
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"eof"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"psync2"),
            ],
        );
        self.stream.write_all(&cmd).await?;
        self.read_ok_response().await?;
        tracing::debug!("REPLCONF handshake complete");
        Ok(())
    }

    pub(crate) async fn psync(&mut self) -> io::Result<SyncType> {
        let state = self.state.read().await;
        let (repl_id, offset) = if state.replication_offset == 0 {
            ("?".to_string(), -1i64)
        } else {
            (
                state.replication_id.clone(),
                state.replication_offset as i64,
            )
        };
        drop(state);
        let cmd = serialize_command_to_resp(
            "PSYNC",
            &[Bytes::from(repl_id), Bytes::from(offset.to_string())],
        );
        self.stream.write_all(&cmd).await?;
        let line_buf = read_resp_line(&mut self.stream).await?;
        let line = line_buf.trim();
        if line.starts_with("+FULLRESYNC") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                let new_repl_id = parts[1].to_string();
                let new_offset: u64 = parts[2].parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid offset in FULLRESYNC")
                })?;
                let mut state = self.state.write().await;
                state.replication_id = new_repl_id.clone();
                state.replication_offset = new_offset;
                drop(state);
                if let Some(ref shared) = self.shared_offset {
                    shared.store(new_offset, Ordering::Release);
                }
                tracing::info!(replication_id = %new_repl_id, offset = new_offset, "FULLRESYNC initiated");
                self.connection_state = ConnectionState::Syncing;
                let line_buf = read_resp_line(&mut self.stream).await?;
                let line = line_buf.trim();
                if !line.starts_with('$') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected RDB size prefix or checkpoint marker",
                    ));
                }
                let marker = &line[1..];
                if marker == "FROGDB_CHECKPOINT" {
                    let line_buf = read_resp_line(&mut self.stream).await?;
                    let file_count: usize = line_buf.trim().parse().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid file count in checkpoint",
                        )
                    })?;
                    tracing::info!(file_count = file_count, "FrogDB checkpoint FULLRESYNC");
                    Ok(SyncType::FullSyncCheckpoint { file_count })
                } else {
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
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let new_repl_id = parts[1].to_string();
                let mut state = self.state.write().await;
                state.replication_id = new_repl_id.clone();
                tracing::info!(replication_id = %new_repl_id, "Partial sync with new replication ID");
            }
            self.connection_state = ConnectionState::Streaming;
            tracing::info!("Partial sync (CONTINUE) initiated");
            Ok(SyncType::PartialSync)
        } else if let Some(rest) = line.strip_prefix('-') {
            Err(io::Error::other(format!("PSYNC error: {}", rest)))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected PSYNC response: {}", line),
            ))
        }
    }

    pub(crate) async fn receive_rdb(&mut self, rdb_size: usize) -> io::Result<()> {
        tracing::info!(size = rdb_size, "Receiving RDB data");
        let mut rdb_data = vec![0u8; rdb_size];
        self.stream.read_exact(&mut rdb_data).await?;
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

    pub(crate) async fn receive_checkpoint(&mut self, file_count: usize) -> io::Result<()> {
        tracing::info!(file_count = file_count, "Receiving FrogDB checkpoint");
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
        let mut file_checksums: Vec<(String, [u8; 32])> = Vec::with_capacity(file_count);
        for i in 0..file_count {
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            let filename_len: usize =
                line.trim().trim_start_matches('$').parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid filename length")
                })?;
            let mut filename_buf = vec![0u8; filename_len + 2];
            reader.read_exact(&mut filename_buf).await?;
            let filename = String::from_utf8_lossy(&filename_buf[..filename_len]).to_string();
            line.clear();
            reader.read_line(&mut line).await?;
            let file_size: u64 = line
                .trim()
                .trim_start_matches('$')
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid file size"))?;
            let file_path = checkpoint_dir.join(&filename);
            let checksum = receive_to_file(&mut reader, &file_path, file_size, None).await?;
            total_bytes += file_size;
            file_checksums.push((filename.clone(), checksum));
            tracing::debug!(file = i + 1, filename = %filename, size = file_size, checksum = %hex::encode(&checksum[..8]), "Received checkpoint file");
        }
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let metadata_len: usize =
            line.trim().trim_start_matches('$').parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid metadata length")
            })?;
        let mut metadata_buf = vec![0u8; metadata_len + 2];
        reader.read_exact(&mut metadata_buf).await?;
        let metadata = FullSyncMetadata::from_bytes(&metadata_buf[..metadata_len])?;
        tracing::info!(total_bytes = total_bytes, files = file_count, replication_id = %metadata.replication_id, offset = metadata.replication_offset, "Checkpoint received successfully");
        let mut combined_hash = Sha256::new();
        for (file_name, file_checksum) in &file_checksums {
            Digest::update(&mut combined_hash, file_name.as_bytes());
            Digest::update(&mut combined_hash, file_checksum);
        }
        let computed: [u8; 32] = Digest::finalize(combined_hash).into();
        if computed != metadata.checksum {
            tracing::error!(expected = %hex::encode(metadata.checksum), actual = %hex::encode(computed), "Checkpoint checksum mismatch");
            let _ = fs::remove_dir_all(&checkpoint_dir).await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checkpoint checksum mismatch: received data does not match primary's checksum",
            ));
        }
        tracing::info!(checksum = %hex::encode(computed), "Checkpoint checksum verified");
        let checkpoint_ready_dir = parent_dir.join("checkpoint_ready");
        if checkpoint_ready_dir.exists()
            && let Err(e) = fs::remove_dir_all(&checkpoint_ready_dir).await
        {
            tracing::warn!(error = %e, "Failed to remove old staged checkpoint");
        }
        if let Err(e) = fs::rename(&checkpoint_dir, &checkpoint_ready_dir).await {
            tracing::error!(error = %e, "Failed to stage checkpoint for loading");
            let _ = fs::remove_dir_all(&checkpoint_dir).await;
            return Err(io::Error::other(format!(
                "Failed to stage checkpoint: {}",
                e
            )));
        }
        let metadata_path = checkpoint_ready_dir.join("replication_metadata.json");
        let metadata_json = serde_json::json!({ "replication_id": metadata.replication_id, "replication_offset": metadata.replication_offset, "checksum": hex::encode(metadata.checksum) });
        if let Err(e) = fs::write(&metadata_path, metadata_json.to_string()).await {
            tracing::warn!(error = %e, "Failed to write replication metadata");
        }
        tracing::info!(checkpoint_dir = %checkpoint_ready_dir.display(), replication_id = %metadata.replication_id, offset = metadata.replication_offset, "Checkpoint staged for loading - server restart required to apply");
        {
            let mut state = self.state.write().await;
            state.replication_id = metadata.replication_id.clone();
            state.replication_offset = metadata.replication_offset;
        }
        if let Some(ref shared) = self.shared_offset {
            shared.store(metadata.replication_offset, Ordering::Release);
        }
        Ok(())
    }

    pub(crate) async fn read_ok_response(&mut self) -> io::Result<()> {
        let line = timeout(HANDSHAKE_TIMEOUT, read_resp_line(&mut self.stream))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handshake timeout"))??;
        let line = line.trim();
        if line == "+OK" {
            Ok(())
        } else if let Some(rest) = line.strip_prefix('-') {
            Err(io::Error::other(format!("error response: {}", rest)))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {}", line),
            ))
        }
    }
}
