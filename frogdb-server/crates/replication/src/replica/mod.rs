//! Replica node replication handling.

pub(crate) mod connection;
pub(crate) mod offset;
mod streaming;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc};
use tokio::time::timeout;

use crate::BoxedStream;
use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;

use connection::SyncType;
pub use connection::{ConnectionState, ReplicaConnection};
use offset::ReplicaOffset;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Factory for creating connections to the primary.
///
/// The server crate provides either a plain TCP or TLS-wrapped factory.
pub type ConnectFactory = Arc<
    dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = io::Result<BoxedStream>> + Send>>
        + Send
        + Sync,
>;

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
    frame_tx: mpsc::Sender<ReplicationFrame>,
    shutdown: tokio::sync::watch::Sender<bool>,
    data_dir: PathBuf,
    /// The live applied offset — the canonical home of "how far this replica has
    /// applied", owned here so it persists across reconnect attempts (each
    /// [`ReplicaConnection`] adopts a clone). Seeded from the persisted
    /// `offset_at_save` at construction; in cluster mode this same atomic becomes
    /// the cluster-bus HealthProbe handle (see [`Self::set_shared_offset`]).
    live: Arc<AtomicU64>,
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
}

impl ReplicaReplicationHandler {
    pub fn new(
        primary_addr: SocketAddr,
        listening_port: u16,
        mut state: ReplicationState,
        state_path: PathBuf,
        data_dir: PathBuf,
    ) -> (Self, mpsc::Receiver<ReplicationFrame>) {
        let (frame_tx, frame_rx) = mpsc::channel(10000);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        state.master_host = Some(primary_addr.ip().to_string());
        state.master_port = Some(primary_addr.port());
        // Seed the live offset from the persisted save-point offset so a clean
        // restart resumes from where it left off rather than rewinding to 0.
        let live = Arc::new(AtomicU64::new(state.offset_at_save));
        let handler = Self {
            primary_addr,
            listening_port,
            state: Arc::new(RwLock::new(state)),
            state_path,
            frame_tx,
            shutdown,
            data_dir,
            live,
            shared_offset: None,
            connect_factory: plain_tcp_connect_factory(),
            link_up: Arc::new(AtomicBool::new(false)),
        };
        (handler, frame_rx)
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

    /// Persist the replica's replication identity + offset to the state file.
    ///
    /// The replica advances its live offset (in [`ReplicaOffset`]) as it consumes
    /// the WAL stream; this snapshots that live applied value into the persisted
    /// `offset_at_save` (monotone-guarded) before writing — preserving the
    /// persist-what-you-applied semantic. Saving on graceful shutdown lets a
    /// clean restart resume from the right offset and attempt a partial resync
    /// instead of rewinding to the boot value.
    pub async fn save_state(&self) -> std::io::Result<()> {
        let offsets = ReplicaOffset::new(self.state.clone(), self.live.clone());
        let snapshot = offsets.reconcile_for_persist().await;
        snapshot.save(&self.state_path)
    }

    /// Set a custom connection factory (e.g. for TLS connections).
    pub fn set_connect_factory(&mut self, factory: ConnectFactory) {
        self.connect_factory = factory;
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

    /// Run the connect/sync/reconnect loop until the primary connection
    /// closes normally or [`Self::stop`] is called.
    ///
    /// Selects on the `shutdown` watch at every point the loop could block
    /// (an in-flight connect/handshake/stream, and the backoff sleep between
    /// attempts) so `stop()` breaks the loop directly — no `task.abort()`
    /// required, which matters for a boot-spawned handler whose reconnect
    /// loop otherwise keeps dialing a primary this node has since been
    /// promoted away from.
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
                            tracing::info!("Replication connection closed normally");
                            return Ok(());
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
        let offsets = ReplicaOffset::new(self.state.clone(), self.live.clone());
        let mut conn = ReplicaConnection {
            stream,
            _primary_addr: self.primary_addr,
            state: self.state.clone(),
            connection_state: ConnectionState::Connected,
            data_dir: self.data_dir.clone(),
            offsets,
            link_up: self.link_up.clone(),
        };
        // Whatever ends this attempt — clean close, a handshake/sync error, or
        // the caller dropping the stream — the link is no longer up. `conn`
        // only ever flips `link_up` to `true`; this is the one place it comes
        // back down, so a stale `true` can never survive past this function.
        let result = async {
            conn.handshake(self.listening_port).await?;
            let sync_type = conn.psync().await?;
            match sync_type {
                SyncType::FullSyncRdb { rdb_size } => conn.receive_rdb(rdb_size).await?,
                SyncType::FullSyncCheckpoint { file_count } => {
                    conn.receive_checkpoint(file_count).await?
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
    pub fn stop(&self) {
        self.shutdown.send_replace(true);
    }

    /// The primary this handler connects/reconnects to.
    pub fn primary_addr(&self) -> SocketAddr {
        self.primary_addr
    }

    pub async fn state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }

    /// Get a shared reference to the replication state for use by the frame consumer.
    pub fn shared_state(&self) -> Arc<RwLock<ReplicationState>> {
        self.state.clone()
    }
}
