//! HTTP handler functions for metrics, health, and status endpoints.

use crate::health::HealthChecker;
use crate::latency_bands::LatencyBandTracker;
use crate::metric_names;
use crate::prometheus_recorder::PrometheusRecorder;
use crate::status::StatusCollector;
use bytes::Bytes;
use frogdb_core::MetricsRecorder;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use std::sync::Arc;

/// Handle GET /metrics - return Prometheus format metrics.
pub fn handle_metrics(
    recorder: Arc<PrometheusRecorder>,
    band_tracker: Option<Arc<LatencyBandTracker>>,
) -> Response<Full<Bytes>> {
    // Update latency band gauges before encoding (if tracker is configured)
    if let Some(tracker) = band_tracker {
        // Export cumulative counts with "le" labels (like histogram buckets)
        for (label, count) in tracker.get_counts() {
            recorder.record_gauge(
                metric_names::LATENCY_BAND_REQUESTS,
                count as f64,
                &[("le", &label)],
            );
        }
    }

    let body = recorder.encode();

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Handle GET /health/live - liveness check.
pub fn handle_health_live(health: HealthChecker) -> Response<Full<Bytes>> {
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
pub fn handle_health_ready(health: HealthChecker) -> Response<Full<Bytes>> {
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

/// Handle GET /status/json - machine-readable server status.
pub async fn handle_status_json(
    status_collector: Option<Arc<StatusCollector>>,
) -> Response<Full<Bytes>> {
    match status_collector {
        Some(collector) => {
            let status = collector.collect().await;
            let body = collector.to_json(&status);

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error": "Status collector not configured"}"#,
            )))
            .unwrap(),
    }
}

/// Handle unknown paths.
pub fn not_found() -> Response<Full<Bytes>> {
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

        let response = handle_metrics(recorder, None);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("text/plain")
        );
    }

    #[test]
    fn test_handle_metrics_with_band_tracker() {
        let recorder = Arc::new(PrometheusRecorder::new());
        let tracker = Arc::new(LatencyBandTracker::new(vec![5, 10, 50]));

        // Record some latencies
        tracker.record(3); // <= 5ms
        tracker.record(7); // <= 10ms
        tracker.record(100); // overflow

        let response = handle_metrics(recorder.clone(), Some(tracker));
        assert_eq!(response.status(), StatusCode::OK);

        // Verify the encoded metrics contain band data
        let body = recorder.encode();
        assert!(body.contains("frogdb_latency_band_requests_total"));
        assert!(body.contains("le="));
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
