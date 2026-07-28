//! The node's installed observability collectors.
//!
//! [`ServerObservability`] is the production implementation of
//! [`frogdb_core::ObservabilityConfig`]: it is built once during startup and
//! handed to every surface that needs a collector, so
//! `hot_shard_detector()` answers `Some` on a running server (and `None` only
//! where no collector was installed — a test fixture, or an embedding that
//! disabled the feature). Nothing fabricates a report from a missing collector;
//! callers render the absence instead.

use std::sync::Arc;

use frogdb_core::{HotShardDetector, MemoryDiagnosticsCollector, ObservabilityConfig};

/// The collectors installed on this node.
#[derive(Clone, Default)]
pub struct ServerObservability {
    hot_shards: Option<Arc<dyn HotShardDetector>>,
}

impl ServerObservability {
    /// Install a hot-shard detector.
    pub fn with_hot_shards(mut self, detector: Arc<dyn HotShardDetector>) -> Self {
        self.hot_shards = Some(detector);
        self
    }

    /// The installed hot-shard detector as a shared handle, for the surfaces
    /// (status collector, debug web UI) that need to hold it rather than borrow
    /// it for the duration of a call.
    pub fn hot_shard_handle(&self) -> Option<Arc<dyn HotShardDetector>> {
        self.hot_shards.clone()
    }
}

impl ObservabilityConfig for ServerObservability {
    fn memory_diagnostics(&self) -> Option<&dyn MemoryDiagnosticsCollector> {
        // MEMORY DOCTOR does not go through this seam: it is served by the
        // `MemoryDiagProvider` on the connection view, which builds a collector
        // per call from the connection's own shard senders. Reporting `None`
        // here is accurate — no collector is installed on this struct.
        None
    }

    fn hot_shard_detector(&self) -> Option<&dyn HotShardDetector> {
        self.hot_shards.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_debug::HotShardCollector;

    #[test]
    fn default_reports_no_collectors() {
        let obs = ServerObservability::default();
        assert!(obs.hot_shard_detector().is_none());
        assert!(obs.hot_shard_handle().is_none());
        assert!(obs.memory_diagnostics().is_none());
    }

    #[test]
    fn installed_hot_shard_detector_is_visible() {
        let collector = Arc::new(HotShardCollector::new(
            Arc::new(Vec::new()),
            &frogdb_debug::HotShardConfig::default(),
        ));
        let obs = ServerObservability::default().with_hot_shards(collector);
        assert!(obs.hot_shard_detector().is_some());
        assert!(obs.hot_shard_handle().is_some());
    }
}
