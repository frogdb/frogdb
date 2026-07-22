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

/// Build the `(replication_id, offset)` pair for a reconnect `PSYNC` request
/// from the replica's **live applied** offset. A live offset of 0 means the
/// replica has never synced, so it asks for a full resync (`PSYNC ? -1`);
/// otherwise it resumes from its live head under its current replication id.
///
/// Kept as a free function so the offset-source decision is unit-testable
/// without a socket — the regression guard is that it is fed
/// [`ReplicaOffset::current`], not the lagging persisted `offset_at_save`.
fn psync_request_args(replication_id: &str, current_offset: u64) -> (String, i64) {
    if current_offset == 0 {
        ("?".to_string(), -1i64)
    } else {
        (replication_id.to_string(), current_offset as i64)
    }
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
    /// Single owner of the replica-side live offset: the applied-offset atomic
    /// (also the cluster-bus HealthProbe handle) advanced behind
    /// [`ReplicaOffset::frame_advance`] / [`ReplicaOffset::reset_to`].
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
        // The reconnect offset MUST come from the live applied head
        // (`ReplicaOffset::current`), never the persisted `offset_at_save` which
        // lags between save points — a resume from behind the applied head would
        // re-receive already-applied data or force a needless full resync.
        let current = self.offsets.current();
        let replication_id = self.state.read().await.replication_id.clone();
        let (repl_id, offset) = psync_request_args(&replication_id, current);
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
                self.state.write().await.replication_id = new_repl_id.clone();
                self.offsets.reset_to(new_offset);
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
        self.state.write().await.replication_id = metadata.replication_id.clone();
        self.offsets.reset_to(metadata.replication_offset);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::offset::ReplicaOffset;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn psync_request_args_asks_full_resync_when_never_synced() {
        let (id, offset) = psync_request_args("abc", 0);
        assert_eq!(id, "?");
        assert_eq!(offset, -1);
    }

    #[test]
    fn psync_request_args_resumes_from_the_live_offset() {
        let (id, offset) = psync_request_args("myid", 500);
        assert_eq!(id, "myid");
        assert_eq!(offset, 500);
    }

    /// Regression guard for the reconnect-offset hazard: the offset a reconnect
    /// `PSYNC` places in its request must equal the **live applied** head
    /// (`ReplicaOffset::current`), not the lagging persisted `offset_at_save`.
    /// Drives the real `psync()` over an in-memory duplex — no socket — and
    /// inspects the bytes it wrote.
    #[tokio::test]
    async fn psync_places_live_offset_not_offset_at_save_in_the_request() {
        let (client, server) = tokio::io::duplex(64 * 1024);

        let mut st = ReplicationState::new();
        let repl_id = st.replication_id.clone();
        st.offset_at_save = 100; // persisted save-point M lags
        let state = Arc::new(RwLock::new(st));
        // Live applied head N = 500 (diverged from the persisted 100).
        let offsets = ReplicaOffset::new(state.clone(), Arc::new(AtomicU64::new(500)));

        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state,
            connection_state: ConnectionState::Connected,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets,
            link_up: Arc::new(AtomicBool::new(false)),
        };

        let mut client = client;
        let task = tokio::spawn(async move { conn.psync().await });

        // Read the PSYNC request the replica wrote.
        let mut buf = vec![0u8; 512];
        let n = client.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();

        // Let psync() complete with a partial-sync continue.
        client
            .write_all(format!("+CONTINUE {}\r\n", repl_id).as_bytes())
            .await
            .unwrap();
        let sync = task.await.unwrap().unwrap();
        assert!(matches!(sync, SyncType::PartialSync));

        // The request resumes from the live head (500), never offset_at_save.
        assert!(
            req.contains("$3\r\n500\r\n"),
            "reconnect PSYNC must carry the live offset 500, got: {req:?}"
        );
    }
}
