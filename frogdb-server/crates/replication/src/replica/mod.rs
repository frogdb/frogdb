//! Replica node replication handling.

pub(crate) mod connection;
mod streaming;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc};
use tokio::time::timeout;

use crate::BoxedStream;
use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;

use connection::SyncType;
pub use connection::{ConnectionState, ReplicaConnection};

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
    shared_offset: Option<Arc<AtomicU64>>,
    connect_factory: ConnectFactory,
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
        let handler = Self {
            primary_addr,
            listening_port,
            state: Arc::new(RwLock::new(state)),
            state_path,
            frame_tx,
            shutdown,
            data_dir,
            shared_offset: None,
            connect_factory: plain_tcp_connect_factory(),
        };
        (handler, frame_rx)
    }

    /// Persist the replica's replication identity + offset to the state file.
    ///
    /// The replica advances `state.replication_offset` as it consumes the WAL
    /// stream, so the in-memory state is the source of truth here. Saving on
    /// graceful shutdown lets a clean restart resume from the right offset and
    /// attempt a partial resync instead of rewinding to the boot value.
    pub async fn save_state(&self) -> std::io::Result<()> {
        let snapshot = self.state.read().await.clone();
        snapshot.save(&self.state_path)
    }

    /// Set a custom connection factory (e.g. for TLS connections).
    pub fn set_connect_factory(&mut self, factory: ConnectFactory) {
        self.connect_factory = factory;
    }

    pub fn set_shared_offset(&mut self, offset: Arc<AtomicU64>) {
        self.shared_offset = Some(offset);
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
        let mut conn = ReplicaConnection {
            stream,
            _primary_addr: self.primary_addr,
            state: self.state.clone(),
            connection_state: ConnectionState::Connected,
            data_dir: self.data_dir.clone(),
            shared_offset: self.shared_offset.clone(),
        };
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
