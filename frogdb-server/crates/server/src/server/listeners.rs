//! Listener binding logic for server subsystems.

use anyhow::Result;
use tracing::info;

use crate::config::Config;
use crate::net::{TcpListener, tcp_listener_reusable};

use super::ServerListeners;

/// All bound listeners for the server, ready to be handed to `Server`.
pub struct BoundListeners {
    /// RESP protocol listener for client connections.
    pub resp: TcpListener,
    /// Optional admin RESP protocol listener.
    pub admin_resp: Option<TcpListener>,
    /// Optional metrics HTTP listener.
    pub metrics: Option<tokio::net::TcpListener>,
    /// Optional admin HTTP API listener.
    pub admin_http: Option<tokio::net::TcpListener>,
    /// Optional cluster bus (Raft RPC) listener.
    pub cluster_bus: Option<TcpListener>,
}

/// Bind all server listeners from config, using any pre-bound listeners from `pre_bound`.
pub async fn bind_listeners(config: &Config, pre_bound: ServerListeners) -> Result<BoundListeners> {
    // Bind TCP listener — use pre-bound if provided, otherwise bind from config.
    let resp = if let Some(l) = pre_bound.resp {
        info!(addr = %l.local_addr()?, "RESP using pre-bound listener");
        l
    } else {
        let bind_addr: std::net::SocketAddr = config.bind_addr().parse()?;
        let l = tcp_listener_reusable(bind_addr).await?;
        info!(addr = %bind_addr, "TCP listener bound");
        l
    };

    // Bind admin TCP listener if enabled
    let admin_resp = if let Some(l) = pre_bound.admin_resp {
        info!(addr = %l.local_addr()?, "Admin RESP using pre-bound listener");
        Some(l)
    } else if config.admin.enabled {
        let admin_bind_addr: std::net::SocketAddr = config.admin.bind_addr().parse()?;
        let admin_listener = tcp_listener_reusable(admin_bind_addr).await?;
        info!(
            addr = %config.admin.bind_addr(),
            "Admin TCP listener bound"
        );
        Some(admin_listener)
    } else {
        None
    };

    // Bind metrics HTTP listener if metrics are enabled
    let metrics = if let Some(l) = pre_bound.metrics {
        info!(addr = %l.local_addr()?, "Metrics using pre-bound listener");
        Some(l)
    } else if config.metrics.enabled {
        let metrics_bind_addr: std::net::SocketAddr = config.metrics.bind_addr().parse()?;
        let listener = tokio::net::TcpListener::bind(metrics_bind_addr).await?;
        info!(
            addr = %listener.local_addr()?,
            "Metrics listener bound"
        );
        Some(listener)
    } else {
        None
    };

    // Bind admin HTTP listener if admin API is enabled
    let admin_http = if let Some(l) = pre_bound.admin_http {
        info!(addr = %l.local_addr()?, "Admin HTTP using pre-bound listener");
        Some(l)
    } else if config.admin.enabled {
        let admin_http_bind_addr: std::net::SocketAddr = config.admin.http_bind_addr().parse()?;
        let listener = tokio::net::TcpListener::bind(admin_http_bind_addr).await?;
        info!(
            addr = %listener.local_addr()?,
            "Admin HTTP listener bound"
        );
        Some(listener)
    } else {
        None
    };

    // Bind cluster bus listener if cluster mode is enabled.
    // Uses crate::net::TcpListener (turmoil-compatible) so simulations can intercept it.
    // If a pre-bound listener was provided via ServerListeners, use it directly.
    let cluster_bus = if let Some(l) = pre_bound.cluster_bus {
        info!(addr = %l.local_addr()?, "Cluster bus using pre-bound listener");
        Some(l)
    } else if config.cluster.enabled {
        let cluster_bus_addr = config.cluster.cluster_bus_socket_addr();
        let listener = tcp_listener_reusable(cluster_bus_addr).await?;
        info!(
            addr = %listener.local_addr()?,
            "Cluster bus listener bound"
        );
        Some(listener)
    } else {
        None
    };

    Ok(BoundListeners {
        resp,
        admin_resp,
        metrics,
        admin_http,
        cluster_bus,
    })
}
