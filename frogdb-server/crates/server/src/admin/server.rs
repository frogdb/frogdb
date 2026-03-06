//! Admin HTTP server implementation.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, routing::get};
use frogdb_core::{ClusterState, ReplicationTrackerImpl};
use tokio::net::TcpListener;
use tracing::{error, info};

use super::handlers;
use crate::config::AdminConfig;

/// Shared state for admin handlers.
#[derive(Clone)]
pub struct AdminState {
    /// Cluster state (if cluster mode is enabled).
    pub cluster_state: Option<Arc<ClusterState>>,
    /// Replication tracker (if replication is enabled).
    pub replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    /// This node's ID.
    pub node_id: Option<u64>,
    /// This node's client address.
    pub client_addr: String,
    /// This node's cluster bus address.
    pub cluster_bus_addr: Option<String>,
}

/// Admin HTTP server.
pub struct AdminServer {
    config: AdminConfig,
    listener: Option<TcpListener>,
    state: AdminState,
}

impl AdminServer {
    /// Create a new admin server.
    ///
    /// Call `with_listener()` to provide a pre-bound listener. If none is
    /// provided, `run()` will bind from the `AdminConfig`.
    pub fn new(config: AdminConfig, state: AdminState) -> Self {
        Self {
            config,
            listener: None,
            state,
        }
    }

    /// Provide a pre-bound `TcpListener` so the port is never released.
    pub fn with_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Run the admin server.
    pub async fn run(self) -> anyhow::Result<()> {
        if !self.config.enabled {
            info!("Admin API disabled");
            return Ok(());
        }

        let listener = match self.listener {
            Some(l) => l,
            None => TcpListener::bind(self.config.bind_addr().parse::<SocketAddr>()?).await?,
        };
        let addr = listener.local_addr()?;
        let app = Self::create_router(self.state);

        info!(addr = %addr, "Admin API server started");

        axum::serve(listener, app).await.map_err(|e| {
            error!(error = %e, "Admin API server error");
            anyhow::anyhow!("Admin API server error: {}", e)
        })
    }

    /// Create the router with all endpoints.
    fn create_router(state: AdminState) -> Router {
        Router::new()
            .route("/admin/health", get(handlers::health))
            .route("/admin/cluster", get(handlers::cluster_state))
            .route("/admin/role", get(handlers::role))
            .route("/admin/nodes", get(handlers::nodes))
            .with_state(Arc::new(state))
    }
}
