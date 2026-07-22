//! Replica connection state machine.

use crate::frame::serialize_command_to_resp;
use crate::fullsync::{
    CHECKPOINT_MARKER, CheckpointChecksum, CheckpointStreamCodec, receive_to_file,
};
use crate::state::{ReplicationState, StagedReplicationMetadata};
use bytes::Bytes;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::offset::ReplicaOffset;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::BoxedStream;

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
    pub(crate) stream: BoxedStream,
    pub(crate) _primary_addr: SocketAddr,
    pub(crate) state: Arc<RwLock<ReplicationState>>,
    pub(crate) connection_state: ConnectionState,
    pub(crate) data_dir: PathBuf,
    /// Single owner of the replica-side offset lifecycle: the canonical
    /// `state.replication_offset` field and its cluster-bus mirror move together
    /// behind [`ReplicaOffset::advance`] / [`ReplicaOffset::reset_to`].
    pub(crate) offsets: ReplicaOffset,
    /// Shared with the owning [`super::ReplicaReplicationHandler`]; kept in
    /// lockstep with `connection_state` via [`Self::set_state`] so INFO can
    /// read the link status without a lock on this connection.
    pub(crate) link_up: Arc<AtomicBool>,
}

impl ReplicaConnection {
    /// Transition to `state`, publishing the derived up/down signal to the
    /// shared `link_up` atomic in the same step so the two can never drift:
    /// up iff the new state is [`ConnectionState::Streaming`].
    fn set_state(&mut self, state: ConnectionState) {
        self.connection_state = state;
        self.link_up
            .store(state == ConnectionState::Streaming, Ordering::Release);
    }

    pub(crate) async fn handshake(&mut self, listening_port: u16) -> io::Result<()> {
        self.set_state(ConnectionState::Handshaking);
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

        // Announce our binary version for rolling upgrade version tracking.
        let cmd = serialize_command_to_resp(
            "REPLCONF",
            &[
                Bytes::from_static(b"frogdb-version"),
                Bytes::from(env!("CARGO_PKG_VERSION")),
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
                self.offsets.reset_to(new_repl_id.clone(), new_offset).await;
                tracing::info!(replication_id = %new_repl_id, offset = new_offset, "FULLRESYNC initiated");
                self.set_state(ConnectionState::Syncing);
                let line_buf = read_resp_line(&mut self.stream).await?;
                let line = line_buf.trim();
                if !line.starts_with('$') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected RDB size prefix or checkpoint marker",
                    ));
                }
                let marker = &line[1..];
                if marker == CHECKPOINT_MARKER {
                    // Marker detection stays here (raw, byte-at-a-time reads) so
                    // the RDB-vs-checkpoint decision is not entangled with the
                    // envelope, but the count parse routes through the codec.
                    let line_buf = read_resp_line(&mut self.stream).await?;
                    let file_count = CheckpointStreamCodec::parse_file_count(&line_buf)?;
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
            self.set_state(ConnectionState::Streaming);
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
        self.set_state(ConnectionState::Streaming);
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
        // The combined checksum's coverage is owned by `CheckpointChecksum`: fold
        // each file in as it lands, in the same order the codec framed it.
        let mut combined = CheckpointChecksum::new();
        for i in 0..file_count {
            // Framing (per-file header) via the codec; the payload bytes still
            // flow through `receive_to_file`, which computes the per-file hash.
            let header = CheckpointStreamCodec::read_file_header(&mut reader).await?;
            let file_path = checkpoint_dir.join(&header.name);
            let checksum = receive_to_file(&mut reader, &file_path, header.size, None).await?;
            total_bytes += header.size;
            combined.update_file(&header.name, &checksum);
            tracing::debug!(file = i + 1, filename = %header.name, size = header.size, checksum = %hex::encode(&checksum[..8]), "Received checkpoint file");
        }
        let metadata = CheckpointStreamCodec::read_metadata(&mut reader).await?;
        tracing::info!(total_bytes = total_bytes, files = file_count, replication_id = %metadata.replication_id, offset = metadata.replication_offset, "Checkpoint received successfully");
        let computed = combined.finalize();
        if computed != metadata.checksum {
            tracing::error!(expected = %hex::encode(metadata.checksum), actual = %hex::encode(computed), "Checkpoint checksum mismatch");
            let _ = fs::remove_dir_all(&checkpoint_dir).await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checkpoint checksum mismatch: received data does not match primary's checksum",
            ));
        }
        tracing::info!(checksum = %hex::encode(computed), "Checkpoint checksum verified");
        // Stage through the typed contract shared with the boot-time installer
        // (`frogdb_persistence::rocks::staged`): the rename onto the staged dir
        // is the writer's commit point.
        let staged = frogdb_persistence::rocks::staged::StagedCheckpoint::in_parent(parent_dir);
        if staged.exists()
            && let Err(e) = fs::remove_dir_all(staged.dir()).await
        {
            tracing::warn!(error = %e, "Failed to remove old staged checkpoint");
        }
        if let Err(e) = fs::rename(&checkpoint_dir, staged.dir()).await {
            tracing::error!(error = %e, "Failed to stage checkpoint for loading");
            let _ = fs::remove_dir_all(&checkpoint_dir).await;
            return Err(io::Error::other(format!(
                "Failed to stage checkpoint: {}",
                e
            )));
        }
        let staged_meta = StagedReplicationMetadata {
            replication_id: metadata.replication_id.clone(),
            replication_offset: metadata.replication_offset,
            checksum: Some(hex::encode(metadata.checksum)),
        };
        match serde_json::to_string(&staged_meta) {
            Ok(json) => {
                if let Err(e) = fs::write(staged.replication_metadata_path(), json).await {
                    tracing::warn!(error = %e, "Failed to write replication metadata");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to serialize replication metadata");
            }
        }
        tracing::info!(checkpoint_dir = %staged.dir().display(), replication_id = %metadata.replication_id, offset = metadata.replication_offset, "Checkpoint staged for loading - server restart required to apply");
        self.offsets
            .reset_to(metadata.replication_id.clone(), metadata.replication_offset)
            .await;
        // The checkpoint itself needs a restart to load, but the connection
        // now moves straight into live WAL streaming (see `connect_and_sync`)
        // exactly like the RDB fullsync path below — so the link is up from
        // here, matching `receive_rdb`'s transition.
        self.set_state(ConnectionState::Streaming);
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
