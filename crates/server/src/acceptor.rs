//! TCP connection acceptor.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use frogdb_core::{shard::NewConnection, CommandRegistry, ShardMessage};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::connection::ConnectionHandler;
use crate::server::next_conn_id;

/// Round-robin connection assigner.
struct RoundRobinAssigner {
    next: AtomicUsize,
    num_shards: usize,
}

impl RoundRobinAssigner {
    fn new(num_shards: usize) -> Self {
        Self {
            next: AtomicUsize::new(0),
            num_shards,
        }
    }

    fn assign(&self) -> usize {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        idx % self.num_shards
    }
}

/// TCP acceptor that distributes connections to shard workers.
pub struct Acceptor {
    /// TCP listener.
    listener: TcpListener,

    /// New connection senders (one per shard).
    new_conn_senders: Vec<mpsc::Sender<NewConnection>>,

    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Connection assigner.
    assigner: RoundRobinAssigner,
}

impl Acceptor {
    /// Create a new acceptor.
    pub fn new(
        listener: TcpListener,
        new_conn_senders: Vec<mpsc::Sender<NewConnection>>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
    ) -> Self {
        let num_shards = new_conn_senders.len();
        Self {
            listener,
            new_conn_senders,
            shard_senders,
            registry,
            assigner: RoundRobinAssigner::new(num_shards),
        }
    }

    /// Run the acceptor loop.
    pub async fn run(self) -> Result<()> {
        info!("Acceptor started");

        loop {
            match self.listener.accept().await {
                Ok((socket, addr)) => {
                    let conn_id = next_conn_id();
                    let shard_id = self.assigner.assign();

                    debug!(
                        conn_id,
                        shard_id,
                        addr = %addr,
                        "Accepted connection"
                    );

                    // For simplicity, we spawn the connection handler directly
                    // instead of sending to the shard's new_conn channel
                    let registry = self.registry.clone();
                    let shard_senders = self.shard_senders.clone();
                    let num_shards = self.shard_senders.len();

                    tokio::spawn(async move {
                        let handler = ConnectionHandler::new(
                            socket,
                            addr,
                            conn_id,
                            shard_id,
                            num_shards,
                            registry,
                            shard_senders,
                        );

                        if let Err(e) = handler.run().await {
                            debug!(conn_id, error = %e, "Connection ended with error");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}
