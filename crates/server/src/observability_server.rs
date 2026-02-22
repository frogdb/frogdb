//! HTTP server for observability: metrics, health, status, and debug endpoints.
//!
//! Composes routes from `frogdb_telemetry` (metrics/health/status handlers)
//! and `frogdb_debug` (debug web UI).

use bytes::Bytes;
use frogdb_debug::DebugState;
use frogdb_telemetry::{
    HealthChecker, LatencyBandTracker, MetricsConfig, PrometheusRecorder, StatusCollector,
    handle_health_live, handle_health_ready, handle_metrics, handle_status_json,
    http_handlers::not_found,
};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

/// HTTP server for observability endpoints.
///
/// Composes telemetry handlers (/metrics, /health/*, /status/json)
/// with debug handlers (/debug/*).
pub struct ObservabilityServer {
    config: MetricsConfig,
    recorder: Arc<PrometheusRecorder>,
    health: HealthChecker,
    debug_state: Option<DebugState>,
    status_collector: Option<Arc<StatusCollector>>,
    band_tracker: Option<Arc<LatencyBandTracker>>,
}

impl ObservabilityServer {
    /// Create a new observability server.
    pub fn new(
        config: MetricsConfig,
        recorder: Arc<PrometheusRecorder>,
        health: HealthChecker,
    ) -> Self {
        Self {
            config,
            recorder,
            health,
            debug_state: None,
            status_collector: None,
            band_tracker: None,
        }
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

    /// Set the latency band tracker for Prometheus export.
    pub fn with_band_tracker(mut self, tracker: Arc<LatencyBandTracker>) -> Self {
        self.band_tracker = Some(tracker);
        self
    }

    /// Start the HTTP server.
    ///
    /// This runs in a loop accepting connections until shutdown.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = self.config.bind_addr().parse()?;
        let listener = TcpListener::bind(addr).await?;

        info!(addr = %addr, "Observability server listening");

        let recorder = self.recorder;
        let health = self.health;
        let debug_state = self.debug_state.map(Arc::new);
        let status_collector = self.status_collector;
        let band_tracker = self.band_tracker;

        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(error = %e, "Failed to accept observability connection");
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let recorder = Arc::clone(&recorder);
            let health = health.clone();
            let debug_state = debug_state.clone();
            let status_collector = status_collector.clone();
            let band_tracker = band_tracker.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let recorder = Arc::clone(&recorder);
                    let health = health.clone();
                    let debug_state = debug_state.clone();
                    let status_collector = status_collector.clone();
                    let band_tracker = band_tracker.clone();
                    async move {
                        handle_request(
                            req,
                            recorder,
                            health,
                            debug_state,
                            status_collector,
                            band_tracker,
                        )
                        .await
                    }
                });

                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                    debug!(peer = %peer_addr, error = %e, "Observability connection error");
                }
            });
        }
    }

    /// Spawn the observability server as a background task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!(error = %e, "Observability server error");
            }
        })
    }
}

/// Handle an HTTP request by routing to the appropriate handler.
async fn handle_request(
    req: Request<Incoming>,
    recorder: Arc<PrometheusRecorder>,
    health: HealthChecker,
    debug_state: Option<Arc<DebugState>>,
    status_collector: Option<Arc<StatusCollector>>,
    band_tracker: Option<Arc<LatencyBandTracker>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (method, path) = (req.method(), req.uri().path());

    debug!(method = %method, path = %path, "Observability server request");

    let response = match (method, path) {
        // Telemetry routes
        (&Method::GET, "/metrics") => handle_metrics(recorder, band_tracker),
        (&Method::GET, "/health/live") => handle_health_live(health),
        (&Method::GET, "/health/ready") => handle_health_ready(health),
        // Convenience aliases
        (&Method::GET, "/healthz") => handle_health_live(health),
        (&Method::GET, "/readyz") => handle_health_ready(health),
        // Status JSON endpoint
        (&Method::GET, "/status/json") => handle_status_json(status_collector).await,
        // Debug web UI (routed to frogdb_debug)
        (&Method::GET, p) if p.starts_with("/debug") => {
            if let Some(ref state) = debug_state {
                frogdb_debug::web_ui::handle_debug_request(req.uri(), state, &recorder).await
            } else {
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("Content-Type", "text/plain")
                    .body(Full::new(Bytes::from("Debug UI not enabled")))
                    .unwrap()
            }
        }
        _ => not_found(),
    };

    Ok(response)
}
