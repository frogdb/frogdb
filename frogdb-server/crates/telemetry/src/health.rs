//! Health check endpoints.

use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Health status response.
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl HealthStatus {
    /// Create a healthy status.
    pub fn ok() -> Self {
        Self {
            status: "ok".to_string(),
            message: None,
        }
    }

    /// Create an unhealthy status with a message.
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            status: "unhealthy".to_string(),
            message: Some(message.into()),
        }
    }

    /// Create a "not ready" status with a message.
    pub fn not_ready(message: impl Into<String>) -> Self {
        Self {
            status: "not_ready".to_string(),
            message: Some(message.into()),
        }
    }

    /// Check if the status is healthy.
    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }
}

/// Health checker that tracks server readiness.
#[derive(Debug, Clone)]
pub struct HealthChecker {
    inner: Arc<HealthCheckerInner>,
}

#[derive(Debug)]
struct HealthCheckerInner {
    /// Whether the server is ready to accept connections.
    ready: AtomicBool,
    /// Whether the server is alive (always true once created).
    alive: AtomicBool,
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthChecker {
    /// Create a new health checker.
    ///
    /// Initially the server is not ready.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HealthCheckerInner {
                ready: AtomicBool::new(false),
                alive: AtomicBool::new(true),
            }),
        }
    }

    /// Mark the server as ready.
    pub fn set_ready(&self) {
        self.inner.ready.store(true, Ordering::SeqCst);
    }

    /// Mark the server as not ready.
    pub fn set_not_ready(&self) {
        self.inner.ready.store(false, Ordering::SeqCst);
    }

    /// Check liveness.
    ///
    /// Returns healthy if the server process is running.
    /// This is used by Kubernetes liveness probes.
    pub fn check_live(&self) -> HealthStatus {
        if self.inner.alive.load(Ordering::SeqCst) {
            HealthStatus::ok()
        } else {
            HealthStatus::unhealthy("Server is shutting down")
        }
    }

    /// Check readiness.
    ///
    /// Returns healthy if the server is ready to accept connections.
    /// This is used by Kubernetes readiness probes.
    pub fn check_ready(&self) -> HealthStatus {
        if !self.inner.alive.load(Ordering::SeqCst) {
            return HealthStatus::not_ready("Server is shutting down");
        }

        if self.inner.ready.load(Ordering::SeqCst) {
            HealthStatus::ok()
        } else {
            HealthStatus::not_ready("Server is starting up")
        }
    }

    /// Mark the server as shutting down.
    pub fn shutdown(&self) {
        self.inner.alive.store(false, Ordering::SeqCst);
        self.inner.ready.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_ok() {
        let status = HealthStatus::ok();
        assert!(status.is_ok());
        assert_eq!(status.status, "ok");
        assert!(status.message.is_none());
    }

    #[test]
    fn test_health_status_unhealthy() {
        let status = HealthStatus::unhealthy("test error");
        assert!(!status.is_ok());
        assert_eq!(status.status, "unhealthy");
        assert_eq!(status.message.as_deref(), Some("test error"));
    }

    #[test]
    fn test_health_checker_initial_state() {
        let checker = HealthChecker::new();
        // Live check should pass
        assert!(checker.check_live().is_ok());
        // Ready check should fail (not ready yet)
        assert!(!checker.check_ready().is_ok());
    }

    #[test]
    fn test_health_checker_set_ready() {
        let checker = HealthChecker::new();
        checker.set_ready();
        assert!(checker.check_live().is_ok());
        assert!(checker.check_ready().is_ok());
    }

    #[test]
    fn test_health_checker_set_not_ready() {
        let checker = HealthChecker::new();
        checker.set_ready();
        checker.set_not_ready();
        assert!(checker.check_live().is_ok());
        assert!(!checker.check_ready().is_ok());
    }

    #[test]
    fn test_health_checker_shutdown() {
        let checker = HealthChecker::new();
        checker.set_ready();
        checker.shutdown();
        assert!(!checker.check_live().is_ok());
        assert!(!checker.check_ready().is_ok());
    }

    #[test]
    fn test_health_status_json_serialization() {
        let status = HealthStatus::ok();
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#"{"status":"ok"}"#);

        let status = HealthStatus::unhealthy("error");
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#"{"status":"unhealthy","message":"error"}"#);
    }
}
