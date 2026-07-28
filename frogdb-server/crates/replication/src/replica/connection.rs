//! Replica connection state machine.

use crate::frame::serialize_command_to_resp;
use crate::fullsync::{
    CHECKPOINT_MARKER, CheckpointStager, CheckpointStreamCodec, receive_checkpoint_files,
};
use crate::state::ReplicationState;
use bytes::Bytes;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::CheckpointInstaller;
use super::offset::ReplicaOffset;
use std::time::Duration;
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
    /// Cadence of the spontaneous replica→primary ACK tick, sourced from
    /// `replication.ack-interval-ms` (Redis `repl-ping-replica-period`) and
    /// copied in from the owning handler when the connection is built.
    pub(crate) ack_interval: Duration,
    /// Installs a received checkpoint into the live keyspace before streaming
    /// resumes; cloned in from the owning handler. `None` in tests and in any
    /// wiring that has no shards, which degrades to the staged-for-next-boot
    /// behaviour (warned about in [`Self::receive_checkpoint`]).
    pub(crate) checkpoint_installer: Option<CheckpointInstaller>,
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

    /// Receive a full-sync checkpoint, install it into the live keyspace, and
    /// resume streaming from the snapshot's offset.
    ///
    /// This is a thin driver over three seams: [`receive_checkpoint_files`] owns
    /// the transport loop (socket → scratch dir + combined checksum),
    /// [`CheckpointStager::commit`] owns verify → commit → metadata against the
    /// staged-checkpoint contract, and the injected [`CheckpointInstaller`] owns
    /// loading the staged snapshot into the live shards. What stays here is the
    /// ordering and the only step that belongs to the connection — adopting the
    /// staged offset into live replication state, then flipping to `Streaming`.
    ///
    /// **Install before adopt.** Redis flushes the replica's dataset and loads
    /// the master's snapshot before applying the stream; so does this. The
    /// offset is adopted only after the install succeeds, so the replica never
    /// advertises (or streams deltas onto) a keyspace that never took the base
    /// snapshot. An install failure rewinds the offset to 0, which makes the
    /// next reconnect send `PSYNC ? -1` and retry the whole full resync.
    ///
    /// The staged dir is deliberately left on disk after the install: if the
    /// process dies between install and the next durable write, the boot-time
    /// installer replays the same snapshot, which is idempotent.
    ///
    /// [`StagedOutcome`]: crate::fullsync::StagedOutcome
    pub(crate) async fn receive_checkpoint(&mut self, file_count: usize) -> io::Result<()> {
        tracing::info!(file_count = file_count, "Receiving FrogDB checkpoint");
        let parent_dir = self.data_dir.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "data_dir has no parent directory",
            )
        })?;
        let stager = CheckpointStager::new(parent_dir);
        let incoming = stager.incoming_dir();

        let mut reader = BufReader::new(&mut self.stream);
        let (metadata, computed) =
            receive_checkpoint_files(&mut reader, &incoming, file_count).await?;

        let outcome = stager.commit(incoming, computed, &metadata).await?;

        self.install_staged_checkpoint(stager.staged_dir()).await?;

        // Adopt the staged offset into live state — the one step that must stay
        // on the connection, because it mutates `ReplicationState` + the shared
        // replica offset atomic (the cluster-bus / INFO handle).
        self.state.write().await.replication_id = outcome.replication_id.clone();
        self.offsets.reset_to(outcome.replication_offset);
        // The live keyspace now matches the snapshot, so the connection moves
        // into live WAL streaming (see `connect_and_sync`) exactly like the RDB
        // fullsync path — the link is up from here, matching `receive_rdb`.
        self.set_state(ConnectionState::Streaming);
        Ok(())
    }

    /// Hand the staged snapshot to the injected installer, rewinding the offset
    /// on failure so the next reconnect asks for a fresh full resync instead of
    /// streaming deltas onto a keyspace that never adopted the base snapshot.
    async fn install_staged_checkpoint(&mut self, staged_dir: PathBuf) -> io::Result<()> {
        let Some(installer) = self.checkpoint_installer.clone() else {
            tracing::warn!(
                checkpoint_dir = %staged_dir.display(),
                "No checkpoint installer wired: the snapshot is staged for the next boot only, \
                 so this node keeps serving its previous keyspace until it restarts"
            );
            return Ok(());
        };
        if let Err(e) = installer(staged_dir).await {
            tracing::error!(error = %e, "Failed to install full-resync checkpoint into the live keyspace");
            self.offsets.reset_to(0);
            return Err(e);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fullsync::{
        CheckpointChecksum, CheckpointFileHeader, FullSyncMetadata, calculate_bytes_checksum,
    };
    use crate::replica::offset::ReplicaOffset;
    use std::future::Future;
    use std::pin::Pin;
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
            ack_interval: Duration::from_secs(1),
            checkpoint_installer: None,
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

    /// Encode a checkpoint envelope body (per-file frames + trailing metadata,
    /// *without* the marker/count prelude that `psync` already consumed) for a
    /// given offset, folding the combined checksum the sender way.
    async fn encode_checkpoint_body(
        files: &[(String, Vec<u8>)],
        replication_id: &str,
        offset: u64,
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        let mut combined = CheckpointChecksum::new();
        for (name, payload) in files {
            CheckpointStreamCodec::write_file_header(
                &mut buf,
                &CheckpointFileHeader {
                    name: name.clone(),
                    size: payload.len() as u64,
                },
            )
            .await
            .unwrap();
            buf.write_all(payload).await.unwrap();
            combined.update_file(name, &calculate_bytes_checksum(payload));
        }
        let metadata = FullSyncMetadata {
            rdb_size: files.iter().map(|(_, p)| p.len() as u64).sum(),
            checksum: combined.finalize(),
            replication_id: replication_id.to_string(),
            replication_offset: offset,
        };
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();
        buf
    }

    /// A checkpoint fixture: two files whose body is already encoded on the
    /// wire, plus the connection wired to read it. `data_dir` is `<tmp>/db` so
    /// its parent `<tmp>` is where staging lands.
    struct CheckpointFixture {
        conn: ReplicaConnection,
        file_count: usize,
        state: Arc<RwLock<ReplicationState>>,
        offsets: ReplicaOffset,
        link_up: Arc<AtomicBool>,
    }

    async fn checkpoint_fixture(
        tmp: &std::path::Path,
        offset: u64,
        installer: Option<CheckpointInstaller>,
    ) -> CheckpointFixture {
        let files = vec![
            ("CURRENT".to_string(), b"MANIFEST-000005\n".to_vec()),
            ("000042.sst".to_string(), (0u8..=200).collect()),
        ];
        let body = encode_checkpoint_body(&files, "primary-replid", offset).await;

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(state.clone(), Arc::new(AtomicU64::new(0)));
        let link_up = Arc::new(AtomicBool::new(false));

        let conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state: state.clone(),
            connection_state: ConnectionState::Syncing,
            data_dir: tmp.join("db"),
            offsets: offsets.clone(),
            link_up: link_up.clone(),
            ack_interval: Duration::from_secs(1),
            checkpoint_installer: installer,
        };

        // Feed the whole checkpoint body, then close so no read blocks.
        client.write_all(&body).await.unwrap();
        client.shutdown().await.unwrap();
        drop(client);

        CheckpointFixture {
            conn,
            file_count: files.len(),
            state,
            offsets,
            link_up,
        }
    }

    /// With no installer wired the driver degrades to the pre-issue-61
    /// behaviour: it still stages the checkpoint, adopts the offset +
    /// replication id, and raises `link_up` (Streaming). Drives the real
    /// `receive_checkpoint` over an in-memory duplex — no socket, no RocksStore.
    #[tokio::test]
    async fn receive_checkpoint_adopts_offset_and_streams() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f = checkpoint_fixture(tmp.path(), 4242, None).await;

        f.conn.receive_checkpoint(f.file_count).await.unwrap();

        // Offset adopted into the live head + visible through the shared atomic.
        assert_eq!(f.offsets.current(), 4242);
        // Replication id adopted into live state.
        assert_eq!(f.state.read().await.replication_id, "primary-replid");
        // Link up: Streaming, with the derived atomic set in lockstep.
        assert_eq!(f.conn.connection_state, ConnectionState::Streaming);
        assert!(f.link_up.load(Ordering::Acquire));

        // The checkpoint was staged (writer's commit point).
        let staged = frogdb_persistence::rocks::staged::StagedCheckpoint::in_parent(tmp.path());
        assert!(staged.exists());
        assert!(staged.dir().join("CURRENT").exists());
    }

    /// Issue 61: the wired installer is handed the committed staged dir, and it
    /// runs *before* the offset is adopted — a replica must never advertise an
    /// offset for a snapshot its keyspace has not taken.
    #[tokio::test]
    async fn receive_checkpoint_installs_staged_dir_before_adopting_offset() {
        let tmp = tempfile::tempdir().unwrap();
        // (staged dir handed to the installer, live offset at install time).
        let seen: Arc<std::sync::Mutex<Vec<(PathBuf, u64)>>> = Arc::default();

        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(state.clone(), Arc::new(AtomicU64::new(0)));
        let recorder = {
            let seen = seen.clone();
            let offsets = offsets.clone();
            Arc::new(move |dir: PathBuf| {
                let seen = seen.clone();
                let offsets = offsets.clone();
                Box::pin(async move {
                    seen.lock().unwrap().push((dir, offsets.current()));
                    Ok(())
                }) as Pin<Box<dyn Future<Output = io::Result<()>> + Send>>
            }) as CheckpointInstaller
        };

        let mut f = checkpoint_fixture(tmp.path(), 4242, Some(recorder)).await;
        f.conn.receive_checkpoint(f.file_count).await.unwrap();

        let seen = seen.lock().unwrap().clone();
        let staged = frogdb_persistence::rocks::staged::StagedCheckpoint::in_parent(tmp.path());
        assert_eq!(
            seen,
            vec![(staged.dir().to_path_buf(), 0)],
            "installer runs exactly once, on the committed staged dir, before the offset is adopted"
        );
        assert_eq!(
            f.offsets.current(),
            4242,
            "offset adopted after the install"
        );
        assert_eq!(f.conn.connection_state, ConnectionState::Streaming);
    }

    /// Issue 61: an install failure must not leave the replica streaming deltas
    /// onto a keyspace that never took the base snapshot. The sync fails and the
    /// offset rewinds to 0, so the next reconnect sends `PSYNC ? -1`.
    #[tokio::test]
    async fn receive_checkpoint_install_failure_rewinds_offset_for_full_resync() {
        let tmp = tempfile::tempdir().unwrap();
        let failing = Arc::new(|_dir: PathBuf| {
            Box::pin(async { Err(io::Error::other("shard install failed")) })
                as Pin<Box<dyn Future<Output = io::Result<()>> + Send>>
        }) as CheckpointInstaller;

        let mut f = checkpoint_fixture(tmp.path(), 4242, Some(failing)).await;
        let err = f.conn.receive_checkpoint(f.file_count).await.unwrap_err();

        assert_eq!(err.to_string(), "shard install failed");
        assert_eq!(f.offsets.current(), 0, "rewound so PSYNC asks ? -1");
        assert_ne!(f.conn.connection_state, ConnectionState::Streaming);
        assert!(!f.link_up.load(Ordering::Acquire));
        assert_eq!(
            psync_request_args(&f.state.read().await.replication_id, f.offsets.current()),
            ("?".to_string(), -1),
        );
    }
}
