//! The typed metric registry — FrogDB's single source of truth for metrics.
//!
//! Every metric the server emits is declared once in [`definitions`] via the
//! `define_metrics!` macro. That one declaration produces:
//!
//! - a **typed handle** (a unit struct like `definitions::CommandsTotal`) whose
//!   generated `inc`/`inc_by`/`set`/`observe` methods are the only supported
//!   emission path — the label schema is part of the method signature, so a
//!   call site cannot emit the wrong label set;
//! - a **registry entry** ([`MetricDefinition`]) carrying the name, help text,
//!   type, and label schema, consumed by the Prometheus recorder (real HELP
//!   text, label arity fixed at creation), the Grafana dashboard generator,
//!   and the metrics verification tests.
//!
//! The [`MetricsRecorder`](crate::traits::metrics::MetricsRecorder) trait in
//! `traits::metrics` remains the *backend* seam (Prometheus, OTLP, composite,
//! no-op, test recorders). Handles sit on top of it: they fix *what* is
//! emitted; the trait object decides *where* it goes.
//!
//! # Emitting a metric
//! ```ignore
//! use frogdb_types::metrics::definitions::CommandsTotal;
//!
//! CommandsTotal::inc(&*recorder, "GET");
//! ```

pub mod definitions;
pub mod labels;

use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

pub use definitions::{ALL_METRICS, METRICS_COUNT};

/// Type of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// A monotonically increasing counter.
    Counter,
    /// A value that can go up and down.
    Gauge,
    /// A distribution of values (latency, sizes, etc.).
    Histogram,
}

impl MetricType {
    /// Get the string representation of this metric type.
    pub const fn as_str(&self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
        }
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Definition of a metric for runtime introspection.
///
/// One entry per metric in [`ALL_METRICS`], generated from the same
/// `define_metrics!` declaration that produces the typed handle. Consumed by
/// the Prometheus recorder (registration), the dashboard generator, and the
/// metrics verification tests.
#[derive(Debug, Clone, Copy)]
pub struct MetricDefinition {
    /// The Prometheus metric name (e.g., "frogdb_commands_total").
    pub name: &'static str,
    /// Help text describing the metric.
    pub help: &'static str,
    /// The type of metric.
    pub metric_type: MetricType,
    /// Label names for this metric, in emission order.
    pub labels: &'static [&'static str],
    /// The generated handle struct's name (e.g., "CommandsTotal"), used by
    /// the verification test to prove every metric is emitted through its
    /// typed handle.
    pub handle: &'static str,
}

impl MetricDefinition {
    /// Get the category prefix from the metric name.
    ///
    /// For "frogdb_commands_total", returns "commands".
    pub fn category(&self) -> &str {
        // Skip "frogdb_" prefix
        let name = self.name.strip_prefix("frogdb_").unwrap_or(self.name);
        // Take until the next underscore
        name.split('_').next().unwrap_or(name)
    }
}

/// Look up a metric's definition by its Prometheus name.
///
/// Backends use this at metric-creation time to attach the real help text and
/// fix the label arity from the registry instead of trusting the first caller.
pub fn definition_for(name: &str) -> Option<&'static MetricDefinition> {
    static BY_NAME: LazyLock<HashMap<&'static str, &'static MetricDefinition>> =
        LazyLock::new(|| ALL_METRICS.iter().map(|def| (def.name, def)).collect());
    BY_NAME.get(name).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_lookup_finds_registered_metric() {
        let def = definition_for(definitions::CommandsTotal::NAME).expect("registered");
        assert_eq!(def.name, "frogdb_commands_total");
        assert_eq!(def.metric_type, MetricType::Counter);
        assert_eq!(def.labels, &["command"]);
        assert_eq!(def.handle, "CommandsTotal");
        assert!(!def.help.is_empty());
    }

    #[test]
    fn definition_lookup_misses_unknown_metric() {
        assert!(definition_for("frogdb_not_a_metric").is_none());
    }

    #[test]
    fn handle_constants_match_registry_entry() {
        use definitions::LuaScriptsErrors;
        let def = definition_for(LuaScriptsErrors::NAME).expect("registered");
        assert_eq!(def.help, LuaScriptsErrors::HELP);
        assert_eq!(def.metric_type, LuaScriptsErrors::METRIC_TYPE);
        assert_eq!(def.labels, LuaScriptsErrors::LABELS);
    }
}
