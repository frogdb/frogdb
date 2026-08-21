//! Replica node replication handling.

pub(crate) mod connection;
pub(crate) mod offset;
mod payload_reader;
pub mod psync;
mod streaming;
#[cfg(test)]
mod tests;

use parking_lot::RwLock;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::BoxedStream;
use crate::apply::StreamedFrame;
use crate::identity::ReplicationIdentity;
use crate::net_bytes::NetByteCounters;
use crate::state::ReplicationState;

use connection::SyncType;
pub use connection::{ConnectionState, ReplicaConnection};
use offset::ReplicaOffset;
pub use offset::{AppliedOffset, Claim, FrameDisposition, ReplicaApplyStint, frame_disposition};
pub use psync::{
    FullResyncPayload, PsyncArm, psync_request_args, select_full_resync_payload, select_psync_arm,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Factory for creating connections to the primary.
///
/// The server crate provides either a plain TCP or TLS-wrapped factory.
pub type ConnectFactory = Arc<
    dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = io::Result<BoxedStream>> + Send>>
        + Send
        + Sync,
>;

/// The dataset a full resync delivered, in whichever shape the primary had it.
///
/// Both shapes describe the same thing — the primary's whole keyspace at the
/// granted offset — and both are installed the same way; they differ only in
/// where the bytes are when the installer is handed them.
#[derive(Debug, Clone)]
pub enum FullSyncPayload {
    /// A committed staged checkpoint directory
    /// ([`CheckpointStager::staged_dir`]): the primary had RocksDB and cut a
    /// checkpoint from it.
    ///
    /// [`CheckpointStager::staged_dir`]: crate::fullsync::CheckpointStager::staged_dir
    StagedCheckpoint(PathBuf),
    /// One dataset blob per primary shard, serialized straight out of the
    /// primary's memory because it runs with `persistence.enabled = false` and
    /// has no checkpoint to cut (issue 67).
    ///
    /// The blobs are opaque to this crate; their framing belongs to
    /// `frogdb_persistence::serialization::dataset`, and the installer routes
    /// each decoded key to *its own* shard, so the two nodes' shard counts do
    /// not have to agree.
    LiveDataset(Vec<Vec<u8>>),
}

/// Installs a received full-resync dataset into the **live** keyspace.
///
/// Called with the [`FullSyncPayload`] after it has been received in full and
/// *before* the replica adopts the snapshot's offset and resumes streaming. The
/// replication crate owns no store, so the server crate injects the
/// implementation the same way it injects [`ConnectFactory`]; without it a
/// demoted node would keep serving its own forked keyspace until the next boot.
///
/// An `Err` is fatal to the sync attempt: the caller rewinds its offset to 0 so
/// the next reconnect asks for a fresh full resync rather than streaming deltas
/// onto a keyspace that never adopted the base snapshot. Whether the *link* then
/// retries is decided by which [`InstallError`] came back.
pub type SnapshotInstaller = Arc<
    dyn Fn(FullSyncPayload) -> Pin<Box<dyn Future<Output = Result<(), InstallError>> + Send>>
        + Send
        + Sync,
>;

/// Why a full-resync install failed, in the only distinction the reconnect loop
/// can act on: *would trying again help?*
///
/// The default is [`InstallError::Transient`] — `From<io::Error>` produces it,
/// so an installer that has not thought about the question keeps the retrying
/// behaviour it always had, and only a deliberate classification can stop the
/// link. That direction matters: mistaking a transient error for a terminal one
/// strands a replica that would have synced.
#[derive(Debug)]
pub enum InstallError {
    /// Something went wrong that a later attempt could get past: a disk error, a
    /// shard that was mid-shutdown, a corrupt transfer. The link reconnects and
    /// asks for another full resync, backing off between attempts.
    Transient(io::Error),
    /// This node can *never* install a payload from this primary, however many
    /// times it is shipped: the two disagree about the on-disk layout (shard
    /// count, warm tier). Retrying costs the primary a full checkpoint cut and a
    /// full transfer per attempt and cannot succeed, so the link stops and waits
    /// for an operator (see `ReplicaReplicationHandler::sync_refusal`).
    ///
    /// `detail` is written for the operator and reaches INFO verbatim: it must
    /// name *both* sides of the disagreement, because the whole failure is that
    /// two nodes were configured differently and neither one alone shows it.
    Incompatible { detail: String },
}

impl From<io::Error> for InstallError {
    fn from(err: io::Error) -> Self {
        Self::Transient(err)
    }
}

impl From<InstallError> for io::Error {
    fn from(err: InstallError) -> Self {
        match err {
            InstallError::Transient(err) => err,
            InstallError::Incompatible { detail } => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("full resync refused: {detail}"),
            ),
        }
    }
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(err) => write!(f, "{err}"),
            Self::Incompatible { detail } => write!(f, "full resync refused: {detail}"),
        }
    }
}

/// Default connection factory: plain TCP.
pub fn plain_tcp_connect_factory() -> ConnectFactory {
    Arc::new(|addr| {
        Box::pin(async move {
            let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection timeout"))??;
            Ok(Box::new(stream) as BoxedStream)
        })
    })
}

pub struct ReplicaReplicationHandler {
    primary_addr: SocketAddr,
    listening_port: u16,
    state: Arc<RwLock<ReplicationState>>,
    state_path: PathBuf,
    frame_tx: mpsc::Sender<StreamedFrame>,
    shutdown: tokio::sync::watch::Sender<bool>,
    data_dir: PathBuf,
    /// The live applied offset — the canonical home of "how far this replica has
    /// applied", owned here so it persists across reconnect attempts (each
    /// [`ReplicaConnection`] adopts a clone). Seeded from the persisted
    /// `offset_at_save` at construction; in cluster mode this same atomic becomes
    /// the cluster-bus HealthProbe handle (see [`Self::set_shared_offset`]).
    live: Arc<AtomicU64>,
    /// The node's **applied** offset — how far the received stream has actually
    /// been applied (see [`AppliedOffset`]). Handed to the frame consumer, which
    /// is the only thing that advances it, and read by the promotion path as the
    /// boundary this node can vouch for. Never swapped by
    /// [`Self::set_shared_offset`]: the cluster bus tracks the received head.
    applied: AppliedOffset,
    /// `Some` iff the live atomic is also wired to the cluster-bus HealthProbe;
    /// when set it is the SAME `Arc` as [`Self::live`]. `None` outside cluster
    /// mode. Preserves the vend-only-when-wired contract INFO/the failure
    /// detector depend on.
    shared_offset: Option<Arc<AtomicU64>>,
    connect_factory: ConnectFactory,
    /// Whether the connection to the primary is currently up: TCP-connected,
    /// past the PSYNC handshake, and streaming live WAL frames — the same
    /// condition [`ConnectionState::Streaming`] names, published here so
    /// readers outside the connect/reconnect loop (INFO replication) can
    /// observe it without reaching into a `ReplicaConnection` that only lives
    /// for the duration of one connection attempt. Starts `false` and is
    /// reset to `false` whenever [`Self::connect_and_sync`] returns for any
    /// reason (clean close, error, or a fresh attempt not yet past PSYNC).
    link_up: Arc<AtomicBool>,
    /// Cadence of the spontaneous replica→primary ACK tick (Redis
    /// `repl-ping-replica-period`). Seeded to a 1s default at construction and
    /// overridden from `replication.ack-interval-ms` via [`Self::set_ack_interval`];
    /// stamped into each [`ReplicaConnection`] built in [`Self::connect_and_sync`].
    ack_interval: Duration,
    /// Installs a received full-resync checkpoint into the live keyspace before
    /// streaming resumes (see [`SnapshotInstaller`]). `None` means nothing was
    /// wired: the checkpoint is staged for the next boot only, which is the
    /// pre-issue-61 behaviour and is warned about loudly at sync time.
    snapshot_installer: Option<SnapshotInstaller>,
    /// Set once, by the connection, when an install came back
    /// [`InstallError::Incompatible`]: the reason this node can never accept a
    /// full resync from this primary. Latched rather than cleared on reconnect
    /// because there is no reconnect — the loop stops on it — and because it is
    /// what INFO reports for as long as the node stays in that state.
    ///
    /// Shared with each [`ReplicaConnection`] this handler builds, for the same
    /// reason `link_up` is: the connection is the only thing that learns the
    /// fact, and the readers (the loop, INFO) outlive it.
    sync_refusal: Arc<RwLock<Option<String>>>,
    /// Lifetime replication-input tally (hardening issue 29). Defaults to a
    /// private, unpublished counter so a handler with nothing wired still
    /// works (tests, and any construction path that never calls
    /// [`Self::set_net_bytes_counters`]); overwritten with the tracker's
    /// shared handle at boot (`replication_init.rs`) so `INFO` sees the same
    /// bytes this handler's connections actually received. Cloned into each
    /// [`ReplicaConnection`] built in [`Self::connect_and_sync`].
    net_bytes: Arc<NetByteCounters>,
}

/// Default spontaneous-ACK cadence when config supplies nothing (1s, matching
/// `DEFAULT_ACK_INTERVAL_MS` in the config crate and Redis's default
/// `repl-ping-replica-period`).
const DEFAULT_ACK_INTERVAL: Duration = Duration::from_secs(1);

impl ReplicaReplicationHandler {
    /// Build the replica-side handler over the node's shared
    /// [`ReplicationIdentity`].
    ///
    /// The identity is the node's, not this handler's: taking it (rather than
    /// minting a fresh `ReplicationState` + offset atomic) is what lets a
    /// demoted primary keep the history and offset it already reached, and what
    /// makes INFO report one value regardless of which role is running.
    pub fn new(
        primary_addr: SocketAddr,
        listening_port: u16,
        identity: ReplicationIdentity,
        state_path: PathBuf,
        data_dir: PathBuf,
    ) -> (Self, mpsc::Receiver<StreamedFrame>) {
        let (frame_tx, frame_rx) = mpsc::channel(10000);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let state = identity.state();
        {
            let mut guard = state.write();
            guard.master_host = Some(primary_addr.ip().to_string());
            guard.master_port = Some(primary_addr.port());
        }
        // The live offset is the node's, already seeded from the persisted
        // save-point offset, so a clean restart resumes from where it left off
        // rather than rewinding to 0.
        let live = identity.live();
        let applied = identity.applied();
        let handler = Self {
            primary_addr,
            listening_port,
            state,
            state_path,
            frame_tx,
            shutdown,
            data_dir,
            live,
            applied,
            shared_offset: None,
            connect_factory: plain_tcp_connect_factory(),
            link_up: Arc::new(AtomicBool::new(false)),
            ack_interval: DEFAULT_ACK_INTERVAL,
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            net_bytes: Arc::new(NetByteCounters::default()),
        };
        (handler, frame_rx)
    }

    /// Override the spontaneous replica→primary ACK cadence from
    /// `replication.ack-interval-ms`. A zero value is ignored (config validation
    /// already rejects it), keeping the safe non-zero default rather than
    /// spinning a zero-duration `tokio::time::interval`.
    pub fn set_ack_interval(&mut self, ms: u64) {
        if ms > 0 {
            self.ack_interval = Duration::from_millis(ms);
        }
    }

    /// The spontaneous-ACK cadence this handler stamps into each connection.
    /// Exposed so the boot/demotion wiring can be asserted without a socket.
    pub fn ack_interval(&self) -> Duration {
        self.ack_interval
    }

    /// Whether the replica currently has a live, streaming connection to its
    /// primary. This is the source of truth behind INFO's
    /// `master_link_status`: `true` only once the PSYNC handshake has
    /// completed and WAL frames are flowing ([`ConnectionState::Streaming`]);
    /// `false` at every other point, including mid-handshake, mid-full-sync,
    /// and after the link drops while the reconnect loop backs off.
    pub fn link_up(&self) -> bool {
        self.link_up.load(Ordering::Acquire)
    }

    /// Why this replica gave up on its primary, if it did.
    ///
    /// `Some(reason)` means a full resync came back
    /// [`InstallError::Incompatible`] and the reconnect loop has stopped: the
    /// link will stay down until an operator fixes the configuration and
    /// re-issues `REPLICAOF` (or restarts the node). The string names both sides
    /// of the disagreement and is what INFO's `master_sync_error` reports, so an
    /// operator does not have to catch the one log line that scrolled past.
    ///
    /// `None` on every ordinary replica, including one that is merely
    /// disconnected and retrying — "down" and "given up" are different states
    /// and `master_link_status:down` alone cannot tell them apart.
    pub fn sync_refusal(&self) -> Option<String> {
        self.sync_refusal.read().clone()
    }

    /// Persist the replica's replication identity + offset to the state file.
    ///
    /// The replica advances its live offset (in [`ReplicaOffset`]) as it consumes
    /// the WAL stream; this snapshots that live applied value into the persisted
    /// `offset_at_save` (monotone-guarded) before writing — preserving the
    /// persist-what-you-applied semantic. Saving on graceful shutdown lets a
    /// clean restart resume from the right offset and attempt a partial resync
    /// instead of rewinding to the boot value.
    pub async fn save_state(&self) -> std::io::Result<()> {
        let offsets =
            ReplicaOffset::new(self.state.clone(), self.live.clone(), self.applied.clone());
        let snapshot = offsets.reconcile_for_persist().await;
        snapshot.save(&self.state_path)
    }

    /// The applied-offset counter to hand to this handler's frame consumer
    /// ([`crate::consume_frames`]). One counter per node: the consumer advances
    /// it, and the promotion path freezes its boundary on it.
    pub fn applied_offset(&self) -> AppliedOffset {
        self.applied.clone()
    }

    /// Set a custom connection factory (e.g. for TLS connections).
    pub fn set_connect_factory(&mut self, factory: ConnectFactory) {
        self.connect_factory = factory;
    }

    /// Wire the live-keyspace installer for received full-resync checkpoints.
    ///
    /// Must be called by every construction site that has shards to install into
    /// (boot-configured replica and runtime `REPLICAOF` demotion alike),
    /// otherwise a full resync only stages for the next boot.
    pub fn set_snapshot_installer(&mut self, installer: SnapshotInstaller) {
        self.snapshot_installer = Some(installer);
    }

    /// Wire the shared net-byte counters (hardening issue 29), replacing the
    /// private default with the tracker's handle
    /// ([`crate::tracker::ReplicationTrackerImpl::net_bytes_handle`]) so this
    /// handler's connections tally into the same counters `INFO` reads. Must
    /// be called by every construction site that wants real input bytes
    /// reported (boot-configured replica and runtime `REPLICAOF` demotion
    /// alike); otherwise the handler counts into a counter nothing reads.
    pub fn set_net_bytes_counters(&mut self, counters: Arc<NetByteCounters>) {
        self.net_bytes = counters;
    }

    /// Wire the cluster-bus HealthProbe atomic. The handler adopts `offset` as
    /// its live-offset home (carrying the current live value into it first) so
    /// there is a single atomic: the failure detector reads exactly what the
    /// replica advances, and the handle identity the caller passed is preserved.
    pub fn set_shared_offset(&mut self, offset: Arc<AtomicU64>) {
        offset.store(self.live.load(Ordering::Acquire), Ordering::Release);
        self.live = offset.clone();
        self.shared_offset = Some(offset);
    }

    /// The cluster-bus HealthProbe offset handle this replica publishes into, if
    /// wired. Mirrors the primary handler's `shared_offset()`; lets callers
    /// assert that a runtime-demoted replica advertises its offset to the failure
    /// detector the same way a boot-configured replica does.
    pub fn shared_offset(&self) -> Option<Arc<AtomicU64>> {
        self.shared_offset.clone()
    }

    /// The port every link this handler opens announces with `REPLCONF
    /// listening-port` — i.e. the address the primary will render as
    /// `slaveN:port=` and `ROLE` (FM-REPLICATION-049). Exposed so the wiring
    /// that supplies it can be asserted without opening a socket.
    pub fn listening_port(&self) -> u16 {
        self.listening_port
    }

    /// Run the connect/sync/reconnect loop until [`Self::stop`] is called.
    ///
    /// The loop reconnects after *any* link loss — a transport error or a
    /// clean close the primary initiated to force a resync (broadcast-lag /
    /// write-timeout disconnect) — because a configured replica must keep
    /// re-establishing its link, like a Redis replica. Termination is driven
    /// solely by the `shutdown` watch: the loop selects on it at every point
    /// it could block (an in-flight connect/handshake/stream, and the backoff
    /// sleep between attempts) so `stop()` breaks the loop directly — no
    /// `task.abort()` required, which matters for a boot-spawned handler whose
    /// reconnect loop would otherwise keep dialing a primary this node has
    /// since been promoted away from.
    pub async fn start(&self) -> io::Result<()> {
        let mut backoff = Duration::from_millis(100);
        let max_backoff = Duration::from_secs(30);
        let mut shutdown_rx = self.shutdown.subscribe();

        // Stop was requested before the loop even started.
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    tracing::info!("Replica replication stopped via shutdown watch");
                    return Ok(());
                }
                result = self.connect_and_sync() => {
                    match result {
                        Ok(()) => {
                            // The link closed without a transport error: almost
                            // always the primary dropped us on purpose to force a
                            // resync (a broadcast-lag / write-timeout disconnect,
                            // see `replica_session`'s write task) or the primary
                            // restarted. A configured replica must keep
                            // re-establishing the link — Redis replicas reconnect
                            // after *any* link loss until told otherwise. Every
                            // intentional teardown (promotion, re-demotion, server
                            // shutdown) fires the shutdown watch above, whose biased
                            // branch returns before we would reconnect, so treating a
                            // clean close as terminal here only stranded replicas
                            // that a primary had disconnected for lag. Reset the
                            // backoff since the connection had been healthy, then
                            // pause briefly to avoid a hot reconnect spin.
                            tracing::info!("Replication link closed by primary; reconnecting");
                            backoff = Duration::from_millis(100);
                            tokio::select! {
                                biased;
                                _ = shutdown_rx.changed() => {
                                    tracing::info!("Replica replication stopped via shutdown watch after link close");
                                    return Ok(());
                                }
                                _ = tokio::time::sleep(backoff) => {}
                            }
                        }
                        // The sync failed in a way no later attempt can get
                        // past: the two nodes disagree about the on-disk layout,
                        // and every retry costs the primary a full checkpoint
                        // cut and a full transfer for a payload this node will
                        // refuse identically (issue 23). Stop, and let INFO and
                        // the operator's next `REPLICAOF` take it from here — an
                        // unsatisfiable operation must not be retried on a
                        // timer.
                        Err(e) if self.sync_refusal().is_some() => {
                            let reason = self.sync_refusal().unwrap_or_default();
                            tracing::error!(
                                primary = %self.primary_addr,
                                reason = %reason,
                                error = %e,
                                "Full resync refused: this node cannot install this primary's \
                                 dataset and is giving up. The link stays down (INFO \
                                 master_sync_error) until the configuration is corrected and \
                                 REPLICAOF is re-issued, or the node is restarted."
                            );
                            return Err(e);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, backoff_ms = backoff.as_millis(), "Replication connection failed, retrying");
                            tokio::select! {
                                biased;
                                _ = shutdown_rx.changed() => {
                                    tracing::info!("Replica replication stopped via shutdown watch during backoff");
                                    return Ok(());
                                }
                                _ = tokio::time::sleep(backoff) => {}
                            }
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                    }
                }
            }
        }
    }

    async fn connect_and_sync(&self) -> io::Result<()> {
        let stream = (self.connect_factory)(self.primary_addr).await?;
        tracing::info!(primary = %self.primary_addr, "Connected to primary");
        // Adopt the handler-owned live atomic (already holds the applied offset,
        // seeded once at construction), so a reconnect never rewinds the live
        // head to the lagging persisted field.
        let offsets =
            ReplicaOffset::new(self.state.clone(), self.live.clone(), self.applied.clone());
        let mut conn = ReplicaConnection {
            stream,
            _primary_addr: self.primary_addr,
            state: self.state.clone(),
            connection_state: ConnectionState::Connected,
            data_dir: self.data_dir.clone(),
            offsets,
            link_up: self.link_up.clone(),
            ack_interval: self.ack_interval,
            snapshot_installer: self.snapshot_installer.clone(),
            sync_refusal: self.sync_refusal.clone(),
            pending_stream_bytes: bytes::BytesMut::new(),
            net_bytes: self.net_bytes.clone(),
        };
        // Whatever ends this attempt — clean close, a handshake/sync error, or
        // the caller dropping the stream — the link is no longer up. `conn`
        // only ever flips `link_up` to `true`; this is the one place it comes
        // back down, so a stale `true` can never survive past this function.
        let result = async {
            conn.handshake(self.listening_port).await?;
            let sync_type = conn.psync().await?;
            match sync_type {
                SyncType::FullSyncCheckpoint { file_count } => {
                    conn.receive_checkpoint(file_count).await?
                }
                SyncType::FullSyncSnapshot { blob_count } => {
                    conn.receive_snapshot(blob_count).await?
                }
                SyncType::PartialSync => {}
            }
            conn.stream_replication(&self.frame_tx).await
        }
        .await;
        self.link_up.store(false, Ordering::Release);
        result
    }

    /// Signal the reconnect loop in [`Self::start`] to stop.
    ///
    /// Uses `send_replace` rather than `send`: a plain `send` is a silent
    /// no-op whenever the watch channel currently has zero receivers (see
    /// `tokio::sync::watch::Sender::send`), which is exactly the state right
    /// after [`Self::new`] (the constructor's own receiver is dropped
    /// immediately) and before [`Self::start`] has run far enough to
    /// `subscribe()`. `send_replace` always stores the value, so a `stop()`
    /// that races ahead of `start()` is still observed once `start()` does
    /// subscribe, instead of being silently lost.
    ///
    /// **Signal only — it does not wait.** The reconnect loop notices at its
    /// next `select!` poll and the frame consumer is a separate task entirely.
    /// A caller that needs the applied offset to have stopped moving (the
    /// promotion path) must also stop the consumer: see
    /// `RoleManager::promote`, which aborts it before freezing its boundary.
    pub fn stop(&self) {
        self.shutdown.send_replace(true);
    }

    /// The primary this handler connects/reconnects to.
    pub fn primary_addr(&self) -> SocketAddr {
        self.primary_addr
    }

    pub async fn state(&self) -> ReplicationState {
        self.state.read().clone()
    }

    /// Get a shared reference to the replication state for use by the frame consumer.
    pub fn shared_state(&self) -> Arc<RwLock<ReplicationState>> {
        self.state.clone()
    }
}
