//! TCP connection acceptor.

use anyhow::Result;
use frogdb_core::sync::{Arc, AtomicUsize, Ordering};
use std::sync::atomic::AtomicI64;
use frogdb_core::{shard::NewConnection, CommandRegistry, MetricsRecorder, ShardMessage};
use frogdb_metrics::metric_names;
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
    /// Reserved for future use when connections are routed to shard workers.
    #[allow(dead_code)]
    new_conn_senders: Vec<mpsc::Sender<NewConnection>>,

    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Connection assigner.
    assigner: RoundRobinAssigner,

    /// Allow cross-slot operations.
    allow_cross_slot: bool,

    /// Scatter-gather timeout in milliseconds.
    scatter_gather_timeout_ms: u64,

    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,

    /// Current connection count (shared for decrement on drop).
    current_connections: Arc<AtomicI64>,
}

impl Acceptor {
    /// Create a new acceptor.
    pub fn new(
        listener: TcpListener,
        new_conn_senders: Vec<mpsc::Sender<NewConnection>>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        allow_cross_slot: bool,
        scatter_gather_timeout_ms: u64,
        metrics_recorder: Arc<dyn MetricsRecorder>,
    ) -> Self {
        let num_shards = new_conn_senders.len();
        Self {
            listener,
            new_conn_senders,
            shard_senders,
            registry,
            assigner: RoundRobinAssigner::new(num_shards),
            allow_cross_slot,
            scatter_gather_timeout_ms,
            metrics_recorder,
            current_connections: Arc::new(AtomicI64::new(0)),
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

                    // Record connection metrics
                    self.metrics_recorder
                        .increment_counter(metric_names::CONNECTIONS_TOTAL, 1, &[]);
                    let current = self.current_connections.fetch_add(1, Ordering::SeqCst) + 1;
                    self.metrics_recorder
                        .record_gauge(metric_names::CONNECTIONS_CURRENT, current as f64, &[]);

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
                    let allow_cross_slot = self.allow_cross_slot;
                    let scatter_gather_timeout_ms = self.scatter_gather_timeout_ms;
                    let metrics_recorder = self.metrics_recorder.clone();
                    let current_connections = self.current_connections.clone();

                    tokio::spawn(async move {
                        let handler = ConnectionHandler::new(
                            socket,
                            addr,
                            conn_id,
                            shard_id,
                            num_shards,
                            registry,
                            shard_senders,
                            allow_cross_slot,
                            scatter_gather_timeout_ms,
                            metrics_recorder.clone(),
                        );

                        if let Err(e) = handler.run().await {
                            debug!(conn_id, error = %e, "Connection ended with error");
                        }

                        // Decrement connection count when handler finishes
                        let current = current_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                        metrics_recorder
                            .record_gauge(metric_names::CONNECTIONS_CURRENT, current as f64, &[]);
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}
