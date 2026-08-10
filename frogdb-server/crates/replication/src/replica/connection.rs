//! Replica connection state machine.

use crate::frame::serialize_command_to_resp;
use crate::fullsync::{
    CHECKPOINT_MARKER, CheckpointChecksum, CheckpointStager, CheckpointStreamCodec,
    SNAPSHOT_MARKER, calculate_bytes_checksum, receive_checkpoint_files,
};
use crate::net_bytes::NetByteCounters;
use crate::state::ReplicationState;
use bytes::{Bytes, BytesMut};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::offset::ReplicaOffset;
use super::payload_reader::PayloadReader;
use super::{FullSyncPayload, InstallError, SnapshotInstaller};
use parking_lot::RwLock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

/// What the primary answered a `PSYNC` with, and therefore what has to be read
/// off the socket next.
///
/// There is deliberately no "plain RDB" arm: a FrogDB primary answers a full
/// resync with a checkpoint envelope (it has RocksDB) or a dataset envelope (it
/// does not), and both carry the primary's actual keyspace. The data-less
/// minimal RDB that used to be sent when persistence was disabled left the
/// replica serving its own stale keyspace while it believed it was in sync
/// (issue 67), so the payload that expressed it no longer exists on either side.
#[derive(Debug)]
pub(crate) enum SyncType {
    FullSyncCheckpoint { file_count: usize },
    FullSyncSnapshot { blob_count: usize },
    PartialSync,
}

pub struct ReplicaConnection {
    /// The link to the primary. Read it unbuffered (`read_resp_line`,
    /// `read_buf`) or through [`Self::payload_reader`] — **never** through a
    /// locally constructed `BufReader`, which would silently swallow the live
    /// frames that arrive alongside a full-sync trailer (hardening issue 01).
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
    pub(crate) snapshot_installer: Option<SnapshotInstaller>,
    /// Shared with the owning [`super::ReplicaReplicationHandler`]: the latch
    /// this connection sets when an install comes back
    /// [`InstallError::Incompatible`], which is what stops the reconnect loop
    /// and what INFO reports (issue 23).
    pub(crate) sync_refusal: Arc<RwLock<Option<String>>>,
    /// Live stream bytes a full-sync payload read pulled off the socket past
    /// the trailer, parked here by [`PayloadReader`] until
    /// [`Self::take_pending_stream_bytes`] seeds the streaming decoder with
    /// them. Empty on every other path.
    pub(crate) pending_stream_bytes: BytesMut,
    /// Lifetime replication-input tally, shared with the owning
    /// [`super::ReplicaReplicationHandler`] and (through it) the tracker, so
    /// `INFO` reports the same bytes this connection actually received
    /// (hardening issue 29). Recorded by [`Self::receive_snapshot`] /
    /// [`Self::receive_checkpoint`] for the full-sync payload lane and by
    /// `stream_replication`/`drain_frames` for the frame lane.
    pub(crate) net_bytes: Arc<NetByteCounters>,
}

impl ReplicaConnection {
    /// Transition to `state`, publishing the derived up/down signal to the
    /// shared `link_up` atomic in the same step so the two can never drift:
    /// up iff the new state is [`ConnectionState::Streaming`].
    fn set_state(&mut self, state: ConnectionState) {
        self.connection_state = state;
        self.link_up
            .store(state == ConnectionState::Streaming, Ordering::Release);
        // The one seam in the crate that knows this node is a *follower*, so it
        // is the one that fills the role field. It sees no downstream registry,
        // so `INV-ROLE-1` is skipped here — see `crate::view`'s note on the
        // honest cost of the optional fields.
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(
            &self
                .offsets
                .view()
                .with_role(crate::view::RoleView::Replica {
                    upstream: Some(self._primary_addr),
                }),
            "ReplicaConnection::set_state",
        );
    }

    /// The buffered view of the socket every full-sync payload path must read
    /// through: it parks whatever it reads past the payload in
    /// `pending_stream_bytes` instead of dropping it (hardening issue 01).
    ///
    /// This is the *only* place the socket may be wrapped in a buffering
    /// reader, which is what keeps a future third payload shape from
    /// reintroducing the frame loss: it gets the hand-back for free.
    fn payload_reader(&mut self) -> PayloadReader<'_> {
        PayloadReader::new(&mut self.stream, &mut self.pending_stream_bytes)
    }

    /// Take the live bytes a payload read buffered past its trailer, leaving
    /// the field empty so they are handed over exactly once.
    ///
    /// Called by [`Self::stream_replication`] to seed its decode buffer.
    ///
    /// [`Self::stream_replication`]: ReplicaConnection::stream_replication
    pub(crate) fn take_pending_stream_bytes(&mut self) -> BytesMut {
        std::mem::take(&mut self.pending_stream_bytes)
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
        let replication_id = self.state.read().replication_id.clone();
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
                // Neither half of the granted history — id nor offset — is
                // adopted here. A `+FULLRESYNC` line promises a dataset; until
                // that dataset is installed this node still holds the *previous*
                // primary's keyspace, and claiming the new id over it would
                // advertise a history it cannot serve and drop the failover
                // window (`secondary_id`/`secondary_offset`) that describes the
                // data it is still holding. Both receive paths adopt the id from
                // the payload's own trailer once the install succeeds, which is
                // where Redis adopts it too: `slaveTryPartialResynchronization`
                // only parks the granted id in the cached-master field, and
                // `readSyncBulkPayload` does `memcpy(server.replid, ...)` +
                // `clearReplicationId2()` after the RDB loads (round-2 issue 51).
                //
                // The offset still rewinds to 0, because the head this node
                // reached under the old history is about to be replaced: if the
                // payload never lands (socket dies, checksum mismatch, install
                // fails), a retained head would let the next reconnect ask for a
                // partial resync from a position the incoming dataset defines
                // and be granted `+CONTINUE`, silently forking. At 0 the
                // reconnect sends `PSYNC ? -1` and retries the full resync.
                if !self.offsets.reset_to(0) {
                    // A promotion froze the heads (or a newer stream took them)
                    // while this sync was in flight: this connection no longer
                    // owns the node's history and must not move heads the
                    // freshly minted window and backlog floor were built around.
                    return Err(io::Error::other(
                        "replication stream retired during FULLRESYNC",
                    ));
                }
                tracing::info!(replication_id = %new_repl_id, offset = new_offset, "FULLRESYNC initiated");
                self.set_state(ConnectionState::Syncing);
                let line_buf = read_resp_line(&mut self.stream).await?;
                let line = line_buf.trim();
                if !line.starts_with('$') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected a checkpoint or dataset marker",
                    ));
                }
                // Marker detection stays here (raw, byte-at-a-time reads) so the
                // payload-kind decision is not entangled with the envelope, but
                // the count parse routes through the codec.
                let marker = &line[1..];
                if marker == CHECKPOINT_MARKER || marker == SNAPSHOT_MARKER {
                    let count_line = read_resp_line(&mut self.stream).await?;
                    let count = CheckpointStreamCodec::parse_file_count(&count_line)?;
                    if marker == CHECKPOINT_MARKER {
                        tracing::info!(file_count = count, "FrogDB checkpoint FULLRESYNC");
                        Ok(SyncType::FullSyncCheckpoint { file_count: count })
                    } else {
                        tracing::info!(blob_count = count, "FrogDB live-dataset FULLRESYNC");
                        Ok(SyncType::FullSyncSnapshot { blob_count: count })
                    }
                } else {
                    // Anything else — including the data-less minimal RDB older
                    // primaries sent when persistence was disabled — carries no
                    // dataset this node can install, and accepting it would mean
                    // keeping a stale keyspace while claiming to be synced.
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported FULLRESYNC payload marker: {marker}"),
                    ))
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
                // The primary shifted its id (it was promoted) but the stream is
                // continuous: everything up to the current offset is identical
                // under the old id, so it becomes this node's failover window
                // rather than being discarded (Redis: `shiftReplicationId`).
                let resumed_at = self.offsets.current();
                let mut state = self.state.write();
                if state.replication_id != new_repl_id {
                    state.shift_replication_id(new_repl_id.clone(), resumed_at);
                }
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

    /// Receive a live-dataset full sync — the payload a primary running without
    /// persistence sends in place of a checkpoint — and install it.
    ///
    /// Same envelope as the checkpoint path and the same ordering rules; only
    /// the landing differs. Nothing is staged to disk, because there is no
    /// RocksDB on either side of this sync to stage *into*: the blobs go
    /// straight to the installer, which decodes them and pushes each key to its
    /// own shard.
    ///
    /// Read through [`Self::payload_reader`], so the live frames that arrive in
    /// the same segment as the trailer are handed to the streaming loop rather
    /// than dropped with the reader (hardening issue 01).
    ///
    /// The trailing metadata's combined checksum is verified over the blobs
    /// exactly as [`CheckpointStager::commit`] verifies it over the files, so a
    /// corrupted or truncated dataset fails the sync instead of being installed
    /// as if it were the primary's keyspace.
    ///
    /// **Durability note.** The offset this sync adopts is in memory until the
    /// next replication-state save. A crash in that window leaves the installed
    /// dataset with a state file that still names the *previous* offset; the
    /// replica then reconnects from there and, since its keyspace is at or ahead
    /// of that point, re-applies a tail it already holds. That is idempotent for
    /// the replicated write stream, and it is the same window the checkpoint
    /// path has between install and metadata consumption.
    pub(crate) async fn receive_snapshot(&mut self, blob_count: usize) -> io::Result<()> {
        tracing::info!(blob_count = blob_count, "Receiving FrogDB live dataset");
        // Scoped so the reader is dropped — handing its over-read live tail to
        // `pending_stream_bytes` — before anything else touches `self`.
        let (blobs, combined, metadata) = {
            let mut reader = self.payload_reader();
            let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(blob_count);
            let mut combined = CheckpointChecksum::new();
            for _ in 0..blob_count {
                let header = CheckpointStreamCodec::read_file_header(&mut *reader).await?;
                let mut blob = vec![
                    0u8;
                    usize::try_from(header.size).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "dataset blob size overflows usize",
                        )
                    })?
                ];
                reader.read_exact(&mut blob).await?;
                combined.update_file(&header.name, &calculate_bytes_checksum(&blob));
                blobs.push(blob);
            }
            let metadata = CheckpointStreamCodec::read_metadata(&mut *reader).await?;
            (blobs, combined, metadata)
        };
        if combined.finalize() != metadata.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "live-dataset checksum mismatch",
            ));
        }

        // The full-sync payload lane (hardening issue 29): `rdb_size` is the
        // real, checksum-verified size just confirmed above, not a value
        // derived from anything the payload might have been.
        self.net_bytes.record_input(metadata.rdb_size);

        self.install_payload(FullSyncPayload::LiveDataset(blobs))
            .await?;

        self.state
            .write()
            .adopt_replication_history(metadata.replication_id.clone());
        if !self.offsets.reset_to(metadata.replication_offset) {
            return Err(io::Error::other(
                "replication stream retired during dataset sync",
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
    /// staged-checkpoint contract, and the injected [`SnapshotInstaller`] owns
    /// loading the staged snapshot into the live shards. What stays here is the
    /// ordering and the only step that belongs to the connection — adopting the
    /// staged offset into live replication state, then flipping to `Streaming`.
    ///
    /// **Install before adopt.** Redis flushes the replica's dataset and loads
    /// the master's snapshot before applying the stream; so does this. The
    /// offset is adopted only after the install succeeds, so the replica never
    /// advertises (or streams deltas onto) a keyspace that never took the base
    /// snapshot. An install failure rewinds the offset to 0, which makes the
    /// next reconnect send `PSYNC ? -1` and retry the whole full resync — unless
    /// the installer refused the payload outright ([`InstallError::Incompatible`]),
    /// in which case there is no next reconnect: see [`Self::install_payload`].
    ///
    /// **Receive → stream continuity.** The transport loop runs on
    /// [`Self::payload_reader`], not a bare `BufReader`, so the live frames the
    /// trailer's read pulled in behind it survive into `stream_replication`
    /// (hardening issue 01) — a checkpoint is slow enough that the primary is
    /// almost always already streaming by the time the trailer lands.
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

        // Scoped like `receive_snapshot`: dropping the reader is what hands the
        // live tail it read past the trailer to `pending_stream_bytes`.
        let (metadata, computed) = {
            let mut reader = self.payload_reader();
            receive_checkpoint_files(&mut *reader, &incoming, file_count).await?
        };

        let outcome = stager.commit(incoming, computed, &metadata).await?;

        // The full-sync payload lane (hardening issue 29): `commit` verifies
        // the checksum before returning, so `rdb_size` is confirmed real —
        // see the matching comment in `receive_snapshot`.
        self.net_bytes.record_input(metadata.rdb_size);

        self.install_payload(FullSyncPayload::StagedCheckpoint(stager.staged_dir()))
            .await?;

        // Adopt the staged offset into live state — the one step that must stay
        // on the connection, because it mutates `ReplicationState` + the shared
        // replica offset atomic (the cluster-bus / INFO handle).
        self.state
            .write()
            .adopt_replication_history(outcome.replication_id.clone());
        if !self.offsets.reset_to(outcome.replication_offset) {
            // The stream was retired mid-sync, so adopting the checkpoint's
            // offset would overwrite a frozen boundary.
            return Err(io::Error::other(
                "replication stream retired during checkpoint sync",
            ));
        }
        // The live keyspace now matches the snapshot, so the connection moves
        // into live WAL streaming (see `connect_and_sync`) — the link is up from
        // here, exactly as in `receive_snapshot`.
        self.set_state(ConnectionState::Streaming);
        Ok(())
    }

    /// Hand the received dataset to the injected installer, rewinding the offset
    /// on failure so the next reconnect asks for a fresh full resync instead of
    /// streaming deltas onto a keyspace that never adopted the base snapshot.
    ///
    /// Two failures, two behaviours. A [`InstallError::Transient`] one is the
    /// case above: rewind, drop the link, reconnect, ask again. An
    /// [`InstallError::Incompatible`] one cannot be fixed by asking again — the
    /// primary would cut and ship another whole checkpoint for a payload this
    /// node refuses identically — so it is latched into the shared
    /// `sync_refusal`, which stops the reconnect loop and surfaces in INFO
    /// (issue 23).
    async fn install_payload(&mut self, payload: FullSyncPayload) -> io::Result<()> {
        let Some(installer) = self.snapshot_installer.clone() else {
            match &payload {
                FullSyncPayload::StagedCheckpoint(dir) => tracing::warn!(
                    checkpoint_dir = %dir.display(),
                    "No snapshot installer wired: the checkpoint is staged for the next boot only, \
                     so this node keeps serving its previous keyspace until it restarts"
                ),
                // A live dataset has no on-disk staging to fall back on, so
                // there is nothing to adopt now or later: fail the sync rather
                // than let the offset advance over a keyspace that never took
                // the snapshot.
                FullSyncPayload::LiveDataset(_) => {
                    return Err(io::Error::other(
                        "no snapshot installer wired: cannot install a live-dataset full resync",
                    ));
                }
            }
            return Ok(());
        };
        match installer(payload).await {
            Ok(()) => Ok(()),
            Err(err) => {
                // Latch the terminal case *before* returning: the reconnect loop
                // reads this to decide whether the failure is worth another full
                // checkpoint, and INFO reads it for as long as the node stays
                // stuck (issue 23).
                if let InstallError::Incompatible { detail } = &err {
                    tracing::error!(
                        detail = %detail,
                        "Refusing the full-resync dataset: this node can never install it"
                    );
                    *self.sync_refusal.write() = Some(detail.clone());
                } else {
                    tracing::error!(
                        error = %err,
                        "Failed to install the full-resync dataset into the live keyspace"
                    );
                }
                // Best effort: if the stream was retired meanwhile, the heads belong
                // to whoever retired it and the rewind is neither possible nor
                // needed.
                let _ = self.offsets.reset_to(0);
                Err(err.into())
            }
        }
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
    use crate::frame::ReplicationFrame;
    use crate::fullsync::{
        CheckpointChecksum, CheckpointFileHeader, FullSyncMetadata, calculate_bytes_checksum,
    };
    use crate::replica::offset::{AppliedOffset, ReplicaOffset};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    /// The invariant hook in [`ReplicaConnection::set_state`] fires on a dirty
    /// view. `set_state` is private, so its forcing test lives here rather than
    /// with the rest of the seam tests in `invariants::tests`. The dirt is a
    /// malformed replication id, which is what `INV-REPLID-3` claims about and
    /// the only claim a replica-side view carries the fields for.
    #[test]
    #[should_panic(expected = "ReplicaConnection::set_state")]
    fn the_set_state_seam_is_hooked() {
        let (_client, server) = tokio::io::duplex(64);
        let mut st = ReplicationState::new();
        st.replication_id = "nonsense".to_string();
        let state = Arc::new(RwLock::new(st));
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );

        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state,
            connection_state: ConnectionState::Connected,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets,
            link_up: Arc::new(AtomicBool::new(false)),
            ack_interval: Duration::from_secs(1),
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };

        conn.set_state(ConnectionState::Streaming);
    }

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
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(500)),
            AppliedOffset::detached(500),
        );

        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state,
            connection_state: ConnectionState::Connected,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets,
            link_up: Arc::new(AtomicBool::new(false)),
            ack_interval: Duration::from_secs(1),
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
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

    /// Drive the real `psync()` against a primary whose whole reply is scripted
    /// up front, and hand back its verdict together with the heads it acted on.
    ///
    /// The duplex is big enough to swallow the `PSYNC` request without a reader,
    /// so the exchange never blocks; the client half is dropped on return, which
    /// is only ever observed as EOF *after* the scripted bytes.
    async fn psync_against(
        script: &[u8],
        state: Arc<RwLock<ReplicationState>>,
        seed: u64,
    ) -> (io::Result<SyncType>, ReplicaOffset) {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        client.write_all(script).await.unwrap();

        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(seed)),
            AppliedOffset::detached(seed),
        );
        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state,
            connection_state: ConnectionState::Connected,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets: offsets.clone(),
            link_up: Arc::new(AtomicBool::new(false)),
            ack_interval: Duration::from_secs(1),
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };
        let verdict = conn.psync().await;
        (verdict, offsets)
    }

    /// The marker decides which receive path the caller drives, and the two
    /// payloads are framed differently end to end (staged RocksDB files vs.
    /// blobs pushed straight at the shards). Reading one as the other would
    /// install a dataset through machinery that cannot parse it — so the marker
    /// must route to its own kind, and the count must survive the routing.
    #[tokio::test]
    async fn each_full_resync_marker_routes_to_its_own_payload_kind() {
        for (marker, expect_checkpoint) in [(CHECKPOINT_MARKER, true), (SNAPSHOT_MARKER, false)] {
            let state = Arc::new(RwLock::new(ReplicationState::new()));
            let script = format!("+FULLRESYNC newid 900\r\n${marker}\r\n3\r\n");
            let (verdict, _offsets) = psync_against(script.as_bytes(), state, 0).await;
            match verdict.expect("the marker is understood") {
                SyncType::FullSyncCheckpoint { file_count } => {
                    assert!(expect_checkpoint, "{marker} must not read as a checkpoint");
                    assert_eq!(file_count, 3);
                }
                SyncType::FullSyncSnapshot { blob_count } => {
                    assert!(!expect_checkpoint, "{marker} must not read as a snapshot");
                    assert_eq!(blob_count, 3);
                }
                other => panic!("{marker} routed to {other:?}"),
            }
        }
    }

    /// A `+CONTINUE` that names an id shifts this node's history onto it (the
    /// deposed primary's id becomes the failover window at the offset the stream
    /// resumed from), and a bare `+CONTINUE` — what a primary that was never
    /// promoted sends — leaves the history exactly as it was.
    #[tokio::test]
    async fn a_continue_carrying_an_id_shifts_the_history_and_a_bare_one_does_not() {
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let old_id = state.read().replication_id.clone();
        let (verdict, _offsets) = psync_against(
            b"+CONTINUE cafebabecafebabecafebabecafebabecafebabe\r\n",
            state.clone(),
            700,
        )
        .await;
        assert!(matches!(verdict.unwrap(), SyncType::PartialSync));
        {
            let st = state.read();
            assert_eq!(
                st.replication_id,
                "cafebabecafebabecafebabecafebabecafebabe"
            );
            assert_eq!(
                st.secondary_id.as_deref(),
                Some(old_id.as_str()),
                "the id it was following becomes the failover window"
            );
            assert_eq!(
                st.secondary_offset, 700,
                "frozen at the head the stream resumed from"
            );
        }

        // Bare `+CONTINUE`: nothing to shift, and nothing may be invented.
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let unchanged = state.read().replication_id.clone();
        let (verdict, _offsets) = psync_against(b"+CONTINUE\r\n", state.clone(), 700).await;
        assert!(matches!(verdict.unwrap(), SyncType::PartialSync));
        let st = state.read();
        assert_eq!(st.replication_id, unchanged);
        assert_eq!(st.secondary_id, None);
        assert_eq!(st.secondary_offset, -1);
    }

    /// Issue 17 of `.scratch/replication-correctness/issues/`, muzzled: the
    /// `+CONTINUE` path writes the peer's bytes into this node's identity
    /// without asking whether they are a replication id at all. Persisted, the
    /// malformed id makes `ReplicationState::validate()` refuse the next boot —
    /// a node that synced fine comes back dead. The disk path already validates
    /// (`read_staged_replication_metadata`); the three wire paths do not.
    ///
    /// Un-ignore when issue 17 lands.
    #[tokio::test]
    #[ignore = "issue 17: the wire paths adopt a replication id without validating it"]
    async fn a_continue_carrying_a_malformed_id_is_refused() {
        for garbage in ["not-a-replid", "ABCDEF", &"f".repeat(41), &"f".repeat(39)] {
            let state = Arc::new(RwLock::new(ReplicationState::new()));
            let held = state.read().replication_id.clone();
            let script = format!("+CONTINUE {garbage}\r\n");
            let (verdict, _offsets) = psync_against(script.as_bytes(), state.clone(), 700).await;

            let st = state.read();
            assert_eq!(
                st.replication_id, held,
                "[{garbage:?}] a malformed grant must not become this node's identity"
            );
            assert_eq!(
                st.secondary_id, None,
                "[{garbage:?}] nor shift the history it never left"
            );
            assert!(
                verdict.is_err(),
                "[{garbage:?}] the link must drop so the reconnect asks again"
            );
        }
    }

    /// The ordinary reconnect: the primary echoes back the id this node is
    /// already on. Shifting on that would file the node's *own* current history
    /// as its failover window and clobber whatever real window it was holding —
    /// the id is only shifted when it actually changes.
    #[tokio::test]
    async fn a_continue_echoing_the_current_id_leaves_the_failover_window_alone() {
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let (same_id, prior_window) = {
            let mut st = state.write();
            // A window this node earned earlier and must keep.
            st.shift_replication_id("beefcafebeefcafebeefcafebeefcafebeefcafe".to_string(), 42);
            (st.replication_id.clone(), st.secondary_id.clone())
        };

        let script = format!("+CONTINUE {same_id}\r\n");
        let (verdict, _offsets) = psync_against(script.as_bytes(), state.clone(), 700).await;
        assert!(matches!(verdict.unwrap(), SyncType::PartialSync));

        let st = state.read();
        assert_eq!(st.replication_id, same_id);
        assert_eq!(
            st.secondary_id, prior_window,
            "an unchanged id must not push the current history into the window"
        );
        assert_eq!(st.secondary_offset, 42, "nor refreeze it at the live head");
    }

    // FM-REPLICATION-049
    /// The port the primary renders as `slaveN:port=` is whatever this
    /// handshake writes, so the value handed in must reach the wire verbatim —
    /// a replica that announces a port it does not serve on gives the primary
    /// an address nobody can dial, which is the `port=0` failure with a
    /// different origin.
    #[tokio::test]
    async fn the_handshake_announces_the_port_it_was_given() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);

        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );
        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state,
            connection_state: ConnectionState::Connected,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets,
            link_up: Arc::new(AtomicBool::new(false)),
            ack_interval: Duration::from_secs(1),
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };

        // One `+OK` per REPLCONF the handshake sends; scripted up front so it
        // never blocks on a reader.
        client.write_all(b"+OK\r\n+OK\r\n+OK\r\n").await.unwrap();

        conn.handshake(7001).await.expect("handshake completes");
        drop(conn); // close the server half so `read_to_end` returns

        let mut written = Vec::new();
        client.read_to_end(&mut written).await.unwrap();
        let written = String::from_utf8(written).expect("the handshake is RESP text");

        let expected = String::from_utf8(
            serialize_command_to_resp(
                "REPLCONF",
                &[
                    Bytes::from_static(b"listening-port"),
                    Bytes::from_static(b"7001"),
                ],
            )
            .to_vec(),
        )
        .unwrap();
        assert!(
            written.starts_with(&expected),
            "the handshake must announce the port it was given first, got: {written:?}"
        );
        assert!(
            written.contains("capa"),
            "the capability announcement still follows, got: {written:?}"
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
        /// The primary's half of the duplex, kept alive for the test's whole
        /// body. Its *write* side is shut down (so the connection's reads see
        /// EOF and terminate), but the half itself must not be dropped: the
        /// streaming path writes ACKs back, and a dropped peer would turn those
        /// into a broken-pipe error instead of the clean close under test.
        _client: tokio::io::DuplexStream,
    }

    async fn checkpoint_fixture(
        tmp: &std::path::Path,
        offset: u64,
        installer: Option<SnapshotInstaller>,
    ) -> CheckpointFixture {
        checkpoint_fixture_with_tail(tmp, offset, installer, &[]).await
    }

    /// As [`checkpoint_fixture`], with `tail` bytes appended to the payload in
    /// the *same* write — the live WAL frames a primary is already streaming by
    /// the time its checkpoint trailer lands.
    async fn checkpoint_fixture_with_tail(
        tmp: &std::path::Path,
        offset: u64,
        installer: Option<SnapshotInstaller>,
        tail: &[u8],
    ) -> CheckpointFixture {
        let files = vec![
            ("CURRENT".to_string(), b"MANIFEST-000005\n".to_vec()),
            ("000042.sst".to_string(), (0u8..=200).collect()),
        ];
        let mut body =
            encode_checkpoint_body(&files, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", offset)
                .await;
        body.extend_from_slice(tail);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );
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
            snapshot_installer: installer,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };

        // Feed the whole checkpoint body (plus any live tail) in one write, then
        // shut the write half so no read blocks. One write is what makes the
        // over-read deterministic: the trailer and the tail are a single chunk,
        // so the fill that completes the trailer necessarily takes the tail too.
        client.write_all(&body).await.unwrap();
        client.shutdown().await.unwrap();

        CheckpointFixture {
            conn,
            file_count: files.len(),
            state,
            offsets,
            link_up,
            _client: client,
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
        assert_eq!(
            f.state.read().replication_id,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
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
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );
        let recorder = {
            let seen = seen.clone();
            let offsets = offsets.clone();
            Arc::new(move |payload: FullSyncPayload| {
                let seen = seen.clone();
                let offsets = offsets.clone();
                Box::pin(async move {
                    let FullSyncPayload::StagedCheckpoint(dir) = payload else {
                        panic!("a checkpoint sync must hand the installer a staged dir")
                    };
                    seen.lock().unwrap().push((dir, offsets.current()));
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
            }) as SnapshotInstaller
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
        let failing = Arc::new(|_payload: FullSyncPayload| {
            Box::pin(async {
                Err(InstallError::Transient(io::Error::other(
                    "shard install failed",
                )))
            }) as Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
        }) as SnapshotInstaller;

        let mut f = checkpoint_fixture(tmp.path(), 4242, Some(failing)).await;
        let err = f.conn.receive_checkpoint(f.file_count).await.unwrap_err();

        assert_eq!(err.to_string(), "shard install failed");
        assert_eq!(f.offsets.current(), 0, "rewound so PSYNC asks ? -1");
        assert_ne!(f.conn.connection_state, ConnectionState::Streaming);
        assert!(!f.link_up.load(Ordering::Acquire));
        assert_eq!(
            psync_request_args(&f.state.read().replication_id, f.offsets.current()),
            ("?".to_string(), -1),
        );
    }

    // ---- live-dataset full sync (issue 67) --------------------------------

    /// What the recording installer saw: the blobs it was handed, paired with
    /// the live offset at the moment of the call — the install-before-adopt
    /// ordering is asserted on that pairing, so both halves travel together.
    type InstalledDatasets = Arc<std::sync::Mutex<Vec<(Vec<Vec<u8>>, u64)>>>;

    /// Encode a live-dataset envelope body — per-blob frames + trailing
    /// metadata, without the prelude `psync` already consumed.
    async fn encode_dataset_body(
        blobs: &[Vec<u8>],
        replication_id: &str,
        offset: u64,
        corrupt: bool,
    ) -> Vec<u8> {
        let named: Vec<(String, Vec<u8>)> = blobs
            .iter()
            .enumerate()
            .map(|(i, b)| (format!("shard-{i}.dataset"), b.clone()))
            .collect();
        let mut buf: Vec<u8> = Vec::new();
        let mut combined = CheckpointChecksum::new();
        for (name, payload) in &named {
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
            // Fold the *pristine* bytes even when the wire carries mutated ones,
            // so `corrupt` reproduces a payload that no longer matches its
            // advertised checksum.
            combined.update_file(name, &calculate_bytes_checksum(payload));
        }
        if corrupt {
            let last = buf.len() - 1;
            buf[last] ^= 0xFF;
        }
        let metadata = FullSyncMetadata {
            rdb_size: named.iter().map(|(_, p)| p.len() as u64).sum(),
            checksum: combined.finalize(),
            replication_id: replication_id.to_string(),
            replication_offset: offset,
        };
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();
        buf
    }

    /// A connection fed a live-dataset body, ready for `receive_snapshot`.
    async fn dataset_fixture(
        tmp: &std::path::Path,
        blobs: Vec<Vec<u8>>,
        offset: u64,
        corrupt: bool,
        installer: Option<SnapshotInstaller>,
    ) -> CheckpointFixture {
        dataset_fixture_with_tail(tmp, blobs, offset, corrupt, installer, &[]).await
    }

    /// As [`dataset_fixture`], with `tail` bytes appended to the payload in the
    /// same write — the live frames trailing a dataset envelope.
    async fn dataset_fixture_with_tail(
        tmp: &std::path::Path,
        blobs: Vec<Vec<u8>>,
        offset: u64,
        corrupt: bool,
        installer: Option<SnapshotInstaller>,
        tail: &[u8],
    ) -> CheckpointFixture {
        let mut body = encode_dataset_body(
            &blobs,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            offset,
            corrupt,
        )
        .await;
        body.extend_from_slice(tail);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );
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
            snapshot_installer: installer,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };

        client.write_all(&body).await.unwrap();
        client.shutdown().await.unwrap();

        CheckpointFixture {
            conn,
            file_count: blobs.len(),
            state,
            offsets,
            link_up,
            _client: client,
        }
    }

    /// Issue 67: the dataset a persistence-disabled primary sends reaches the
    /// installer *before* the offset is adopted, and nothing is staged to disk.
    ///
    /// The old behaviour had no dataset to hand over at all: the replica took
    /// the replid/offset off a minimal RDB and flipped to `Streaming` with its
    /// previous keyspace intact.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn receive_snapshot_installs_the_dataset_before_adopting_offset() {
        let tmp = tempfile::tempdir().unwrap();
        // (blobs handed to the installer, live offset at install time).
        let seen: InstalledDatasets = Arc::default();
        let blobs = vec![b"shard-zero-bytes".to_vec(), b"shard-one".to_vec()];

        let mut f = {
            let seen = seen.clone();
            // The fixture owns the offsets, so capture them through a cell the
            // recorder can read once the fixture exists.
            let observed: Arc<std::sync::Mutex<Option<ReplicaOffset>>> = Arc::default();
            let recorder_offsets = observed.clone();
            let recorder = Arc::new(move |payload: FullSyncPayload| {
                let seen = seen.clone();
                let offsets = recorder_offsets.lock().unwrap().clone();
                Box::pin(async move {
                    let FullSyncPayload::LiveDataset(blobs) = payload else {
                        panic!("a dataset sync must hand the installer blobs")
                    };
                    let at = offsets.expect("offsets wired").current();
                    seen.lock().unwrap().push((blobs, at));
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
            }) as SnapshotInstaller;
            let f = dataset_fixture(tmp.path(), blobs.clone(), 4242, false, Some(recorder)).await;
            *observed.lock().unwrap() = Some(f.offsets.clone());
            f
        };

        f.conn.receive_snapshot(f.file_count).await.unwrap();

        assert_eq!(
            seen.lock().unwrap().clone(),
            vec![(blobs, 0)],
            "the blobs reach the installer once, before the offset is adopted"
        );
        assert_eq!(
            f.offsets.current(),
            4242,
            "offset adopted after the install"
        );
        assert_eq!(
            f.state.read().replication_id,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        assert_eq!(f.conn.connection_state, ConnectionState::Streaming);
        assert!(f.link_up.load(Ordering::Acquire));

        // Nothing was staged: there is no RocksDB on this path.
        let staged = frogdb_persistence::rocks::staged::StagedCheckpoint::in_parent(tmp.path());
        assert!(!staged.exists(), "a live dataset must not stage to disk");
    }

    /// Issue 67: with no installer wired there is no disk staging to fall back
    /// on, so the sync fails rather than advancing the offset over a keyspace
    /// that never took the dataset.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn receive_snapshot_without_an_installer_fails_the_sync() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f = dataset_fixture(tmp.path(), vec![b"blob".to_vec()], 4242, false, None).await;

        let err = f.conn.receive_snapshot(f.file_count).await.unwrap_err();

        assert!(
            err.to_string().contains("no snapshot installer"),
            "got: {err}"
        );
        assert_eq!(f.offsets.current(), 0, "rewound so PSYNC asks ? -1");
        assert_ne!(f.conn.connection_state, ConnectionState::Streaming);
        assert!(!f.link_up.load(Ordering::Acquire));
    }

    /// A corrupted dataset must fail the sync, not be installed as if it were
    /// the primary's keyspace — the same coverage the checkpoint path gets.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn receive_snapshot_rejects_a_corrupted_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        let installed = Arc::new(AtomicBool::new(false));
        let installer = {
            let installed = installed.clone();
            Arc::new(move |_payload: FullSyncPayload| {
                installed.store(true, Ordering::Release);
                Box::pin(async { Ok(()) })
                    as Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
            }) as SnapshotInstaller
        };

        let mut f = dataset_fixture(
            tmp.path(),
            vec![b"payload-bytes".to_vec()],
            4242,
            true,
            Some(installer),
        )
        .await;

        let err = f.conn.receive_snapshot(f.file_count).await.unwrap_err();

        assert!(err.to_string().contains("checksum mismatch"), "got: {err}");
        assert!(
            !installed.load(Ordering::Acquire),
            "a dataset that fails verification never reaches the shards"
        );
        assert_eq!(f.offsets.current(), 0);
        assert_ne!(f.conn.connection_state, ConnectionState::Streaming);
    }

    /// Issue 67: the data-less minimal RDB an older primary sent when
    /// persistence was disabled is rejected outright.
    ///
    /// This is the bug's signature at the protocol boundary. Before the fix the
    /// `$<size>` arm parsed it, threw the bytes away, and returned success —
    /// after which the replica reported `master_link_status:up` while serving
    /// its own stale keyspace. There is now no payload shape that carries an
    /// offset without a dataset behind it.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn psync_rejects_a_payload_that_carries_no_dataset() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(500)),
            AppliedOffset::detached(500),
        );
        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state: state.clone(),
            connection_state: ConnectionState::Connected,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets: offsets.clone(),
            link_up: Arc::new(AtomicBool::new(false)),
            ack_interval: Duration::from_secs(1),
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };

        let task = tokio::spawn(async move {
            let r = conn.psync().await;
            (r, conn.connection_state)
        });

        // Drain the PSYNC request, then answer it the way a pre-fix primary did.
        let mut buf = vec![0u8; 512];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0, "the replica must have sent a PSYNC request");
        client
            .write_all(b"+FULLRESYNC 0123456789012345678901234567890123456789 4242\r\n")
            .await
            .unwrap();
        client.write_all(b"$88\r\n").await.unwrap();
        client.write_all(&[0u8; 88]).await.unwrap();

        let (result, connection_state) = task.await.unwrap();
        let err = result.expect_err("a data-less payload must fail the sync");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("unsupported FULLRESYNC payload marker"),
            "got: {err}"
        );
        assert_ne!(
            connection_state,
            ConnectionState::Streaming,
            "the link must not come up on a sync that transferred no dataset"
        );
        assert_eq!(
            offsets.current(),
            0,
            "the granted offset is not adopted, so the retry is a full resync"
        );
    }

    /// Round-2 issue 51: a `+FULLRESYNC` line is a *promise* of a dataset, not
    /// the dataset. Until the payload is installed this node still holds the
    /// previous primary's keyspace, so it must still claim the previous
    /// history — id and failover window both — and its next reconnect must ask
    /// for a position that history actually reached.
    ///
    /// Redis is the precedent: `slaveTryPartialResynchronization` parks the
    /// granted id in `server.master_replid` (a *cached-master* field, not the
    /// node's own history), and only `readSyncBulkPayload`, after the RDB is
    /// loaded, runs `memcpy(server.replid, server.master->replid, ...)` followed
    /// by `clearReplicationId2()`.
    ///
    /// Every row is a way the promise goes unkept, and the table spans both
    /// sides of the parse: rows that fail before the grant is understood keep
    /// the live head (nothing about this node changed), rows that fail after it
    /// have already rewound to 0 so the retry is a full resync.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone() {
        const GRANTED_ID: &str = "0123456789012345678901234567890123456789";
        const OLD_ID: &str = "abadcafeabadcafeabadcafeabadcafeabadcafe";
        const PRE_FAILOVER_ID: &str = "facefeedfacefeedfacefeedfacefeedfacefeed";

        /// One way the grant goes unkept: what the primary sends after the
        /// PSYNC request, how the failure reads, and the `(id, offset)` the
        /// *next* reconnect must therefore request.
        struct Unkept {
            case: &'static str,
            reply: Vec<u8>,
            fingerprint: &'static str,
            next_request: (String, i64),
        }

        let cases = vec![
            Unkept {
                case: "the socket dies on the grant",
                reply: format!("+FULLRESYNC {GRANTED_ID} 4242\r\n").into_bytes(),
                fingerprint: "connection closed",
                next_request: ("?".to_string(), -1),
            },
            Unkept {
                case: "the envelope names a payload this node cannot install",
                reply: format!("+FULLRESYNC {GRANTED_ID} 4242\r\n$88\r\n").into_bytes(),
                fingerprint: "unsupported FULLRESYNC payload marker",
                next_request: ("?".to_string(), -1),
            },
            Unkept {
                case: "the envelope line is not a payload at all",
                reply: format!("+FULLRESYNC {GRANTED_ID} 4242\r\n+OK\r\n").into_bytes(),
                fingerprint: "expected a checkpoint or dataset marker",
                next_request: ("?".to_string(), -1),
            },
            Unkept {
                case: "the grant carries no offset",
                reply: format!("+FULLRESYNC {GRANTED_ID}\r\n").into_bytes(),
                fingerprint: "malformed FULLRESYNC response",
                next_request: (OLD_ID.to_string(), 900),
            },
            Unkept {
                case: "the granted offset is not a number",
                reply: format!("+FULLRESYNC {GRANTED_ID} nine\r\n").into_bytes(),
                fingerprint: "invalid offset in FULLRESYNC",
                next_request: (OLD_ID.to_string(), 900),
            },
            Unkept {
                case: "the primary refuses the sync outright",
                reply: b"-ERR Can't SYNC while loading the dataset\r\n".to_vec(),
                fingerprint: "PSYNC error",
                next_request: (OLD_ID.to_string(), 900),
            },
        ];

        for Unkept {
            case,
            reply,
            fingerprint,
            next_request: expected_request,
        } in cases
        {
            let (mut client, server) = tokio::io::duplex(64 * 1024);
            let mut st = ReplicationState::new();
            st.replication_id = OLD_ID.to_string();
            // A failover window this node can still serve `+CONTINUE` against:
            // it describes the dataset it is holding right now, and a sync that
            // never lands does not invalidate it.
            st.secondary_id = Some(PRE_FAILOVER_ID.to_string());
            st.secondary_offset = 700;
            let state = Arc::new(RwLock::new(st));
            let offsets = ReplicaOffset::new(
                state.clone(),
                Arc::new(AtomicU64::new(900)),
                AppliedOffset::detached(900),
            );
            let mut conn = ReplicaConnection {
                stream: Box::new(server),
                _primary_addr: "127.0.0.1:6379".parse().unwrap(),
                state: state.clone(),
                connection_state: ConnectionState::Connected,
                data_dir: PathBuf::from("/tmp/frogdb-test"),
                offsets: offsets.clone(),
                link_up: Arc::new(AtomicBool::new(false)),
                ack_interval: Duration::from_secs(1),
                snapshot_installer: None,
                sync_refusal: Arc::new(RwLock::new(None)),
                pending_stream_bytes: BytesMut::new(),
                net_bytes: Arc::new(NetByteCounters::default()),
            };

            // Script the whole reply up front, then close the write half so
            // every row ends in EOF rather than a hang. The request itself fits
            // in the duplex buffer, so `psync` never blocks on a reader.
            client.write_all(&reply).await.unwrap();
            client.shutdown().await.unwrap();

            let err =
                conn.psync().await.err().unwrap_or_else(|| {
                    panic!("[{case}] a sync with no dataset behind it must fail")
                });
            assert!(err.to_string().contains(fingerprint), "[{case}] got: {err}");

            let state = state.read();
            assert_eq!(
                state.replication_id, OLD_ID,
                "[{case}] the node still holds the old keyspace, so it still claims the old history"
            );
            assert_eq!(
                state.secondary_id.as_deref(),
                Some(PRE_FAILOVER_ID),
                "[{case}] the failover window describes the dataset this node is still serving"
            );
            assert_eq!(state.secondary_offset, 700, "[{case}] window boundary");
            assert_ne!(
                offsets.current(),
                4242,
                "[{case}] the granted offset belongs to a dataset that never arrived"
            );
            assert_eq!(
                psync_request_args(&state.replication_id, offsets.current()),
                expected_request,
                "[{case}] the reconnect must ask from a position this node reached"
            );
            assert_ne!(
                conn.connection_state,
                ConnectionState::Streaming,
                "[{case}] the link must not come up"
            );
        }
    }

    /// The other half of round-2 issue 51, one layer down: the grant was well
    /// formed and the transfer started, then the socket died mid-file. The
    /// payload's trailer — the only thing that carries the primary's id — never
    /// arrived, so there is nothing to adopt and the node keeps the history that
    /// matches the keyspace it still has.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let files = vec![
            ("CURRENT".to_string(), b"MANIFEST-000005\n".to_vec()),
            ("000042.sst".to_string(), (0u8..=200).collect()),
        ];
        let body =
            encode_checkpoint_body(&files, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", 4242).await;
        // Cut mid-transfer: the first file's header is on the wire, the rest of
        // the payload and the whole trailer are not.
        let truncated = &body[..body.len() / 3];

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let mut st = ReplicationState::new();
        st.replication_id = "abadcafeabadcafeabadcafeabadcafeabadcafe".to_string();
        st.secondary_id = Some("facefeedfacefeedfacefeedfacefeedfacefeed".to_string());
        st.secondary_offset = 700;
        let state = Arc::new(RwLock::new(st));
        // 0 because `psync` already rewound the live head when it took the grant.
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );
        let link_up = Arc::new(AtomicBool::new(false));
        let mut conn = ReplicaConnection {
            stream: Box::new(server),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state: state.clone(),
            connection_state: ConnectionState::Syncing,
            data_dir: tmp.path().join("db"),
            offsets: offsets.clone(),
            link_up: link_up.clone(),
            ack_interval: Duration::from_secs(1),
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: Arc::new(NetByteCounters::default()),
        };
        client.write_all(truncated).await.unwrap();
        client.shutdown().await.unwrap();

        let err = conn
            .receive_checkpoint(files.len())
            .await
            .expect_err("a truncated checkpoint must fail the sync");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "got: {err}");

        let state = state.read();
        assert_eq!(
            state.replication_id,
            "abadcafeabadcafeabadcafeabadcafeabadcafe"
        );
        assert_eq!(
            state.secondary_id.as_deref(),
            Some("facefeedfacefeedfacefeedfacefeedfacefeed")
        );
        assert_eq!(state.secondary_offset, 700);
        assert_eq!(
            psync_request_args(&state.replication_id, offsets.current()),
            ("?".to_string(), -1),
            "the retry is a full resync, not a resume against a dataset that never landed"
        );
        assert_ne!(conn.connection_state, ConnectionState::Streaming);
        assert!(!link_up.load(Ordering::Acquire));
    }

    // ---- receive -> stream continuity (issue 01) --------------------------

    /// The live WAL frames a primary streams while its full-sync payload is
    /// still in flight, encoded as they appear on the wire, paired with the
    /// offset they advance the replica by (payload bytes, per `frame_advance`).
    fn live_frame_tail(payloads: &[&[u8]]) -> (Vec<u8>, u64) {
        let mut wire = Vec::new();
        let mut advance = 0u64;
        for (i, payload) in payloads.iter().enumerate() {
            let frame = ReplicationFrame::new(i as u64, Bytes::copy_from_slice(payload));
            wire.extend_from_slice(&frame.encode().unwrap());
            advance += payload.len() as u64;
        }
        (wire, advance)
    }

    /// Run the streaming loop to the primary's clean close and collect every
    /// frame it handed to the applier.
    async fn stream_to_close(conn: &mut ReplicaConnection) -> Vec<Bytes> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        conn.stream_replication(&tx).await.unwrap();
        drop(tx);
        let mut frames = Vec::new();
        while let Some(frame) = rx.recv().await {
            frames.push(frame.frame.payload);
        }
        frames
    }

    /// Issue 01: the frames that arrive in the same read as the checkpoint
    /// trailer are streamed, not swallowed by the payload reader.
    ///
    /// A checkpoint transfer is slow, so by the time its trailer lands the
    /// primary has almost always started streaming; the buffered read that
    /// completes the trailer takes those frames with it. Before the fix the
    /// reader was a function-local `BufReader` and dropping it discarded them —
    /// the socket had no copy left, so they were never decoded, never applied
    /// and never ACKed, and the replica's offset stayed permanently short with
    /// `master_link_status:up` (WAIT then never satisfiable).
    // FM-REPLICATION-005
    #[tokio::test]
    async fn receive_checkpoint_streams_the_frames_that_trailed_the_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let (tail, advance) = live_frame_tail(&[
            b"*1\r\n$4\r\nPING\r\n",
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        ]);
        let mut f = checkpoint_fixture_with_tail(tmp.path(), 4242, None, &tail).await;

        f.conn.receive_checkpoint(f.file_count).await.unwrap();
        let frames = stream_to_close(&mut f.conn).await;

        assert_eq!(
            frames,
            vec![
                Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
                Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
            ],
            "every byte after the trailer must reach the frame decoder"
        );
        assert_eq!(
            f.offsets.current(),
            4242 + advance,
            "the received head covers the trailing frames, so it can catch the primary"
        );
    }

    /// The live-dataset path has the same seam and the same bug: both payload
    /// shapes read through the same reader, so both hand their over-read tail
    /// to the stream.
    // FM-REPLICATION-005
    #[tokio::test]
    async fn receive_snapshot_streams_the_frames_that_trailed_the_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let (tail, advance) = live_frame_tail(&[b"*1\r\n$4\r\nPING\r\n"]);
        let installer = Arc::new(|_payload: FullSyncPayload| {
            Box::pin(async { Ok(()) })
                as Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
        }) as SnapshotInstaller;

        let mut f = dataset_fixture_with_tail(
            tmp.path(),
            vec![b"shard-zero".to_vec()],
            4242,
            false,
            Some(installer),
            &tail,
        )
        .await;

        f.conn.receive_snapshot(f.file_count).await.unwrap();
        let frames = stream_to_close(&mut f.conn).await;

        assert_eq!(frames, vec![Bytes::from_static(b"*1\r\n$4\r\nPING\r\n")]);
        assert_eq!(f.offsets.current(), 4242 + advance);
    }

    // ---- terminal install refusals (issue 23) -----------------------------

    /// An installer that classifies every payload the same way.
    fn classifying_installer(refuse: bool) -> SnapshotInstaller {
        Arc::new(move |_payload: FullSyncPayload| {
            let err = if refuse {
                InstallError::Incompatible {
                    detail: "shard-count mismatch: the primary's checkpoint was written with 2 \
                             shard(s), this node is configured for 1"
                        .to_string(),
                }
            } else {
                InstallError::Transient(io::Error::other("shard install failed"))
            };
            Box::pin(async move { Err(err) })
                as Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
        }) as SnapshotInstaller
    }

    // FM-REPLICATION-061
    /// An install this node can never accept latches the refusal the reconnect
    /// loop reads; an ordinary install failure does not, so the two stay
    /// distinguishable *after* the connection that learned it is gone. Before
    /// this, both were the same `io::Error` and the only record of the
    /// difference was a log line that scrolled past once per attempt.
    #[tokio::test]
    async fn an_incompatible_install_latches_the_refusal_and_a_transient_one_does_not() {
        let tmp = tempfile::tempdir().unwrap();

        let mut f = checkpoint_fixture(tmp.path(), 4242, Some(classifying_installer(true))).await;
        let refusal = f.conn.sync_refusal.clone();
        let err = f.conn.receive_checkpoint(f.file_count).await.unwrap_err();
        let latched = refusal.read().clone().expect("the refusal must be latched");
        assert!(
            latched.contains("with 2 shard(s)") && latched.contains("configured for 1"),
            "the latched reason must name both sides: {latched}"
        );
        assert!(
            err.to_string().contains("full resync refused"),
            "the wire error must say it was refused, not merely that it failed: {err}"
        );
        assert_eq!(
            f.offsets.current(),
            0,
            "still rewound: nothing was installed"
        );
        assert!(!f.link_up.load(Ordering::Acquire));

        let tmp = tempfile::tempdir().unwrap();
        let mut f = checkpoint_fixture(tmp.path(), 4242, Some(classifying_installer(false))).await;
        let refusal = f.conn.sync_refusal.clone();
        f.conn.receive_checkpoint(f.file_count).await.unwrap_err();
        assert!(
            refusal.read().is_none(),
            "an ordinary install failure must stay retryable"
        );
    }

    /// A primary that answers the handshake and ships a live dataset, over an
    /// in-memory duplex. Every dial is counted, so a test asserts what the
    /// reconnect loop *did* rather than how long it slept.
    ///
    /// It writes its whole script up front and never reads: the replica's three
    /// handshake commands plus `PSYNC` are a few hundred bytes against a 64 KiB
    /// duplex, so nothing blocks. The client halves are parked in the returned
    /// `Vec` — a dropped one turns the replica's ACK writes into a broken pipe
    /// instead of the clean close under test.
    #[allow(clippy::type_complexity)]
    fn scripted_primary_factory() -> (
        crate::replica::ConnectFactory,
        Arc<AtomicUsize>,
        Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>>,
    ) {
        let dials = Arc::new(AtomicUsize::new(0));
        let parked: Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>> = Arc::default();
        let counter = dials.clone();
        let keep = parked.clone();
        let factory: crate::replica::ConnectFactory = Arc::new(move |_addr| {
            counter.fetch_add(1, Ordering::SeqCst);
            let keep = keep.clone();
            Box::pin(async move {
                let (mut client, server) = tokio::io::duplex(64 * 1024);
                let body = encode_dataset_body(
                    &[b"shard-zero".to_vec()],
                    "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                    99,
                    false,
                )
                .await;
                let mut script: Vec<u8> = Vec::new();
                // Three `REPLCONF`s, then the granted full resync.
                script.extend_from_slice(b"+OK\r\n+OK\r\n+OK\r\n");
                script.extend_from_slice(
                    b"+FULLRESYNC deadbeefdeadbeefdeadbeefdeadbeefdeadbeef 99\r\n",
                );
                CheckpointStreamCodec::write_snapshot_prelude(&mut script, 1)
                    .await
                    .unwrap();
                script.extend_from_slice(&body);
                client.write_all(&script).await.unwrap();
                keep.lock().unwrap().push(client);
                Ok(Box::new(server) as crate::BoxedStream)
            })
        });
        (factory, dials, parked)
    }

    /// Build a handler whose primary is the scripted one above.
    #[allow(clippy::type_complexity)]
    fn handler_over_scripted_primary(
        tmp: &std::path::Path,
        installer: SnapshotInstaller,
    ) -> (
        Arc<crate::replica::ReplicaReplicationHandler>,
        Arc<AtomicUsize>,
        Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>>,
    ) {
        let (factory, dials, parked) = scripted_primary_factory();
        let (mut handler, _rx) = crate::replica::ReplicaReplicationHandler::new(
            "127.0.0.1:6379".parse().unwrap(),
            6380,
            crate::identity::ReplicationIdentity::detached(ReplicationState::new()),
            tmp.join("state.json"),
            tmp.join("db"),
        );
        handler.set_connect_factory(factory);
        handler.set_snapshot_installer(installer);
        (Arc::new(handler), dials, parked)
    }

    // FM-REPLICATION-061
    /// Issue 23: a full resync this node can never install is asked for
    /// **once**. Before the fix the loop treated it like any other error —
    /// reconnect, make the primary cut and ship another whole checkpoint, fail
    /// identically, forever — so a misconfigured pair burned the *primary's*
    /// disk and network for as long as it stayed misconfigured.
    #[tokio::test]
    async fn a_geometry_mismatch_is_refused_once_and_not_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let (handler, dials, _parked) =
            handler_over_scripted_primary(tmp.path(), classifying_installer(true));

        let result = tokio::time::timeout(Duration::from_secs(5), handler.start())
            .await
            .expect("the loop must give up rather than keep retrying");

        assert!(result.is_err(), "giving up is an error, not a clean stop");
        assert_eq!(
            dials.load(Ordering::SeqCst),
            1,
            "the primary must be asked for exactly one full resync"
        );
        let refusal = handler
            .sync_refusal()
            .expect("INFO must be able to say why");
        assert!(
            refusal.contains("shard-count mismatch"),
            "the operator surface must name the cause: {refusal}"
        );
        assert!(
            !handler.link_up(),
            "the link stays down until an operator intervenes"
        );
    }

    // FM-REPLICATION-061
    /// The terminal path must not swallow the ordinary case: an install that
    /// merely failed is retried exactly as before and latches nothing — down
    /// with no `master_sync_error` is "still trying", which is a different
    /// state from "given up" and must stay distinguishable.
    #[tokio::test]
    async fn a_transient_install_failure_is_still_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let (handler, dials, _parked) =
            handler_over_scripted_primary(tmp.path(), classifying_installer(false));

        let runner = {
            let handler = handler.clone();
            tokio::spawn(async move { handler.start().await })
        };
        // The first backoff is 100ms and doubles; two dials inside this window
        // is enough to show the loop is not latching, without asserting a rate.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let dialed = dials.load(Ordering::SeqCst);
        handler.stop();
        let _ = tokio::time::timeout(Duration::from_secs(5), runner).await;

        assert!(
            dialed >= 2,
            "a retryable install failure must keep reconnecting, got {dialed} dial(s)"
        );
        assert!(
            handler.sync_refusal().is_none(),
            "nothing was refused, so nothing must be latched"
        );
    }

    /// A sync that transferred no live tail must not leave phantom bytes in the
    /// decoder: the hand-back is exactly what the payload read over-read.
    // FM-REPLICATION-005
    #[tokio::test]
    async fn a_payload_with_no_trailing_frames_leaves_the_stream_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f = checkpoint_fixture(tmp.path(), 4242, None).await;

        f.conn.receive_checkpoint(f.file_count).await.unwrap();

        assert!(f.conn.pending_stream_bytes.is_empty());
        let frames = stream_to_close(&mut f.conn).await;
        assert!(frames.is_empty());
        assert_eq!(f.offsets.current(), 4242);
    }
}
