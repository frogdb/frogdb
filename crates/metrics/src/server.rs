//! HTTP server for metrics and health endpoints.

use crate::config::MetricsConfig;
use crate::debug::{self, DebugState};
use crate::health::HealthChecker;
use crate::prometheus_recorder::PrometheusRecorder;
use bytes::Bytes;
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

/// HTTP server for metrics and health endpoints.
pub struct MetricsServer {
    config: MetricsConfig,
    recorder: Arc<PrometheusRecorder>,
    health: HealthChecker,
    debug_state: Option<DebugState>,
}

impl MetricsServer {
    /// Create a new metrics server.
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
        }
    }

    /// Set the debug state for the debug web UI.
    pub fn with_debug_state(mut self, state: DebugState) -> Self {
        self.debug_state = Some(state);
        self
    }

    /// Start the HTTP server.
    ///
    /// This runs in a loop accepting connections until shutdown.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = self.config.bind_addr().parse()?;
        let listener = TcpListener::bind(addr).await?;

        info!(addr = %addr, "Metrics server listening");

        let recorder = self.recorder;
        let health = self.health;
        let debug_state = self.debug_state.map(Arc::new);

        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(error = %e, "Failed to accept metrics connection");
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let recorder = Arc::clone(&recorder);
            let health = health.clone();
            let debug_state = debug_state.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let recorder = Arc::clone(&recorder);
                    let health = health.clone();
                    let debug_state = debug_state.clone();
                    async move { handle_request(req, recorder, health, debug_state).await }
                });

                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                    debug!(peer = %peer_addr, error = %e, "Metrics connection error");
                }
            });
        }
    }

    /// Spawn the metrics server as a background task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!(error = %e, "Metrics server error");
            }
        })
    }
}

/// Handle an HTTP request.
async fn handle_request(
    req: Request<Incoming>,
    recorder: Arc<PrometheusRecorder>,
    health: HealthChecker,
    debug_state: Option<Arc<DebugState>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (method, path) = (req.method(), req.uri().path());

    debug!(method = %method, path = %path, "Metrics server request");

    let response = match (method, path) {
        (&Method::GET, "/metrics") => handle_metrics(recorder),
        (&Method::GET, "/health/live") => handle_health_live(health),
        (&Method::GET, "/health/ready") => handle_health_ready(health),
        // Convenience aliases
        (&Method::GET, "/healthz") => handle_health_live(health),
        (&Method::GET, "/readyz") => handle_health_ready(health),
        // Debug web UI
        (&Method::GET, p) if p.starts_with("/debug") => {
            if let Some(ref state) = debug_state {
                debug::handle_debug_request(p, state, &recorder).await
            } else {
                // Debug UI not enabled, return a helpful message
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

/// Handle GET /metrics - return Prometheus format metrics.
fn handle_metrics(recorder: Arc<PrometheusRecorder>) -> Response<Full<Bytes>> {
    let body = recorder.encode();

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Handle GET /health/live - liveness check.
fn handle_health_live(health: HealthChecker) -> Response<Full<Bytes>> {
    let status = health.check_live();
    let status_code = if status.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = serde_json::to_string(&status).unwrap();

    Response::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Handle GET /health/ready - readiness check.
fn handle_health_ready(health: HealthChecker) -> Response<Full<Bytes>> {
    let status = health.check_ready();
    let status_code = if status.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = serde_json::to_string(&status).unwrap();

    Response::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Handle unknown paths.
fn not_found() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/plain")
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::MetricsRecorder;

    #[test]
    fn test_handle_metrics() {
        let recorder = Arc::new(PrometheusRecorder::new());
        recorder.increment_counter("test_metric", 1, &[("label", "value")]);

        let response = handle_metrics(recorder);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/plain"));
    }

    #[test]
    fn test_handle_health_live_ok() {
        let health = HealthChecker::new();
        let response = handle_health_live(health);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_handle_health_live_shutdown() {
        let health = HealthChecker::new();
        health.shutdown();
        let response = handle_health_live(health);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_handle_health_ready_not_ready() {
        let health = HealthChecker::new();
        let response = handle_health_ready(health);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_handle_health_ready_ok() {
        let health = HealthChecker::new();
        health.set_ready();
        let response = handle_health_ready(health);
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_not_found() {
        let response = not_found();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
