//! Unified HTTP server for observability, debug, and admin endpoints.
//!
//! Composes routes from:
//! - `frogdb_telemetry` — metrics, health, status handlers
//! - `frogdb_debug` — debug web UI
//! - `crate::admin::handlers` — admin REST API (cluster management)
//!
//! Protected routes (`/admin/*`, `/debug/*`) can require a bearer token
//! when `HttpConfig.token` is set.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::Json;
use axum::extract::{Request, State};
use axum::http::{StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use frogdb_debug::DebugState;
use frogdb_telemetry::{
    HealthChecker, PrometheusRecorder, StatusCollector, handle_health_live, handle_health_ready,
    handle_metrics, handle_status_json,
};
use http_body_util::Full;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::admin::handlers as admin_handlers;
use crate::admin::handlers::SharedAdminState;
use crate::config::HttpConfig;

/// Shared state for the unified HTTP server.
#[derive(Clone)]
pub struct HttpState {
    pub recorder: Arc<PrometheusRecorder>,
    pub health: HealthChecker,
    pub debug_state: Option<Arc<DebugState>>,
    pub status_collector: Option<Arc<StatusCollector>>,
    pub admin_state: Option<SharedAdminState>,
    pub token: Option<Arc<str>>,
}

/// Unified HTTP server for observability and admin endpoints.
pub struct ObservabilityServer {
    config: HttpConfig,
    listener: Option<TcpListener>,
    recorder: Arc<PrometheusRecorder>,
    health: HealthChecker,
    debug_state: Option<DebugState>,
    status_collector: Option<Arc<StatusCollector>>,
    admin_state: Option<SharedAdminState>,
    /// TLS manager for HTTPS support.
    #[cfg(not(feature = "turmoil"))]
    tls_manager: Option<Arc<crate::tls::TlsManager>>,
    /// TLS handshake timeout.
    #[cfg(not(feature = "turmoil"))]
    tls_handshake_timeout: std::time::Duration,
}

impl ObservabilityServer {
    /// Create a new observability server.
    ///
    /// Call `with_listener()` to provide a pre-bound listener. If none is
    /// provided, `run()` will bind from the `HttpConfig`.
    pub fn new(
        config: HttpConfig,
        recorder: Arc<PrometheusRecorder>,
        health: HealthChecker,
    ) -> Self {
        Self {
            config,
            listener: None,
            recorder,
            health,
            debug_state: None,
            status_collector: None,
            admin_state: None,
            #[cfg(not(feature = "turmoil"))]
            tls_manager: None,
            #[cfg(not(feature = "turmoil"))]
            tls_handshake_timeout: std::time::Duration::from_secs(10),
        }
    }

    /// Provide a pre-bound `TcpListener` so the port is never released.
    pub fn with_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Set the debug state for the debug web UI.
    pub fn with_debug_state(mut self, state: DebugState) -> Self {
        self.debug_state = Some(state);
        self
    }

    /// Set the status collector for the /status/json endpoint.
    pub fn with_status_collector(mut self, collector: Arc<StatusCollector>) -> Self {
        self.status_collector = Some(collector);
        self
    }

    /// Set the admin state for admin REST API endpoints.
    pub fn with_admin_state(mut self, state: SharedAdminState) -> Self {
        self.admin_state = Some(state);
        self
    }

    /// Set the TLS manager for HTTPS support.
    #[cfg(not(feature = "turmoil"))]
    pub fn with_tls(
        mut self,
        tls_manager: Arc<crate::tls::TlsManager>,
        handshake_timeout: std::time::Duration,
    ) -> Self {
        self.tls_manager = Some(tls_manager);
        self.tls_handshake_timeout = handshake_timeout;
        self
    }

    /// Start the HTTP server.
    ///
    /// This runs until the server is shut down.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = match self.listener {
            Some(l) => l,
            None => TcpListener::bind(self.config.bind_addr().parse::<SocketAddr>()?).await?,
        };
        let addr = listener.local_addr()?;

        let state = HttpState {
            recorder: self.recorder,
            health: self.health,
            debug_state: self.debug_state.map(Arc::new),
            status_collector: self.status_collector,
            admin_state: self.admin_state,
            token: self.config.token.map(|t| Arc::from(t.as_str())),
        };

        let app = create_router(state);

        #[cfg(not(feature = "turmoil"))]
        if let Some(tls_manager) = self.tls_manager {
            info!(addr = %addr, "HTTPS server listening");
            run_tls_accept_loop(listener, app, tls_manager, self.tls_handshake_timeout).await?;
            return Ok(());
        }

        info!(addr = %addr, "HTTP server listening");
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Spawn the observability server as a background task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!(error = %e, "HTTP server error");
            }
        })
    }
}

/// Run the HTTPS accept loop with TLS handshake on each connection.
#[cfg(not(feature = "turmoil"))]
async fn run_tls_accept_loop(
    listener: TcpListener,
    app: Router,
    tls_manager: Arc<crate::tls::TlsManager>,
    handshake_timeout: std::time::Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use hyper_util::rt::TokioIo;
    use tower::Service;

    loop {
        let (tcp_stream, _peer) = listener.accept().await?;
        let acceptor = tls_manager.acceptor();
        let app = app.clone();

        tokio::spawn(async move {
            let tls_result =
                tokio::time::timeout(handshake_timeout, acceptor.accept(tcp_stream)).await;

            let tls_stream = match tls_result {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "HTTPS TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!("HTTPS TLS handshake timed out");
                    return;
                }
            };

            let io = TokioIo::new(tls_stream);
            let hyper_service = hyper::service::service_fn(move |req| {
                let mut svc = app.clone().into_service();
                async move { svc.call(req).await }
            });

            if let Err(e) =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(io, hyper_service)
                    .await
            {
                tracing::debug!(error = %e, "HTTPS connection error");
            }
        });
    }
}

/// Build the axum router with all routes.
fn create_router(state: HttpState) -> Router {
    // Public routes (no auth required)
    let public = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health/live", get(health_live_handler))
        .route("/health/ready", get(health_ready_handler))
        .route("/healthz", get(health_live_handler))
        .route("/readyz", get(health_ready_handler))
        .route("/status/json", get(status_json_handler));

    // Protected routes (bearer token when configured)
    let protected = Router::new()
        .route("/debug", get(debug_handler))
        .route("/debug/", get(debug_handler))
        .route("/debug/{*path}", get(debug_handler))
        .route("/admin/health", get(admin_health_handler))
        .route("/admin/cluster", get(admin_cluster_handler))
        .route("/admin/role", get(admin_role_handler))
        .route("/admin/nodes", get(admin_nodes_handler))
        .route("/admin/upgrade-status", get(admin_upgrade_status_handler))
        .route("/admin/shutdown", post(admin_shutdown_handler))
        .route("/admin/transfer-leader", post(admin_transfer_leader_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            bearer_auth_middleware,
        ));

    public.merge(protected).with_state(state)
}

// ---- Bearer token middleware ----

async fn bearer_auth_middleware(
    State(state): State<HttpState>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(ref expected) = state.token {
        let expected_header = format!("Bearer {}", expected);
        match req.headers().get("authorization") {
            Some(val) if val.as_bytes() == expected_header.as_bytes() => next.run(req).await,
            _ => StatusCode::UNAUTHORIZED.into_response(),
        }
    } else {
        // No token configured — allow all
        next.run(req).await
    }
}

// ---- Handler wrappers ----
// These wrap the existing framework-agnostic handlers from frogdb_telemetry
// and frogdb_debug, which return Response<Full<Bytes>>.

async fn metrics_handler(State(s): State<HttpState>) -> Response<Full<Bytes>> {
    handle_metrics(s.recorder)
}

async fn health_live_handler(State(s): State<HttpState>) -> Response<Full<Bytes>> {
    handle_health_live(s.health)
}

async fn health_ready_handler(State(s): State<HttpState>) -> Response<Full<Bytes>> {
    handle_health_ready(s.health)
}

async fn status_json_handler(State(s): State<HttpState>) -> Response<Full<Bytes>> {
    handle_status_json(s.status_collector).await
}

async fn debug_handler(State(s): State<HttpState>, uri: Uri) -> Response<Full<Bytes>> {
    if let Some(ref state) = s.debug_state {
        frogdb_debug::web_ui::handle_debug_request(&uri, state, &s.recorder).await
    } else {
        hyper::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "text/plain")
            .body(Full::new(Bytes::from("Debug UI not enabled")))
            .unwrap()
    }
}

// ---- Admin handler wrappers ----
// These extract AdminState from HttpState and delegate to the existing handlers.

async fn admin_health_handler(State(s): State<HttpState>) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::health(State(admin)).await;
    Ok(result.into_response())
}

async fn admin_cluster_handler(State(s): State<HttpState>) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::cluster_state(State(admin)).await;
    match result {
        Ok(json) => Ok(json.into_response()),
        Err(status) => Err(status),
    }
}

async fn admin_role_handler(State(s): State<HttpState>) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::role(State(admin)).await;
    Ok(result.into_response())
}

async fn admin_nodes_handler(State(s): State<HttpState>) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::nodes(State(admin)).await;
    Ok(result.into_response())
}

async fn admin_upgrade_status_handler(State(s): State<HttpState>) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::upgrade_status(State(admin)).await;
    Ok(result.into_response())
}

async fn admin_shutdown_handler(
    State(s): State<HttpState>,
    body: Option<Json<admin_handlers::ShutdownRequest>>,
) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::shutdown(State(admin), body).await;
    match result {
        Ok(json) => Ok(json.into_response()),
        Err(status) => Err(status),
    }
}

async fn admin_transfer_leader_handler(
    State(s): State<HttpState>,
    Json(body): Json<admin_handlers::TransferLeaderRequest>,
) -> Result<Response, StatusCode> {
    let admin = s.admin_state.ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let result = admin_handlers::transfer_leader(State(admin), Json(body)).await;
    match result {
        Ok(json) => Ok(json.into_response()),
        Err(status) => Err(status),
    }
}
