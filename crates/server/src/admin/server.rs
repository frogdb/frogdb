//! Admin HTTP server implementation.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::get, Router};
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
    state: AdminState,
}

impl AdminServer {
    /// Create a new admin server.
    pub fn new(config: AdminConfig, state: AdminState) -> Self {
        Self { config, state }
    }

    /// Create admin server with default state (no cluster, no replication).
    pub fn new_standalone(config: AdminConfig, client_addr: String) -> Self {
        Self {
            config,
            state: AdminState {
                cluster_state: None,
                replication_tracker: None,
                node_id: None,
                client_addr,
                cluster_bus_addr: None,
            },
        }
    }

    /// Run the admin server.
    pub async fn run(self) -> anyhow::Result<()> {
        if !self.config.enabled {
            info!("Admin API disabled");
            return Ok(());
        }

        let bind_addr = self.config.bind_addr();
        let addr: SocketAddr = bind_addr.parse()?;

        let app = Self::create_router(self.state);

        let listener = TcpListener::bind(addr).await?;
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
