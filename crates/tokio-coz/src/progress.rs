use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

/// Global progress point registry, initialized by the profiler.
static REGISTRY: OnceLock<ProgressPointRegistry> = OnceLock::new();

/// Registry of named progress points (throughput counters and latency pairs).
pub struct ProgressPointRegistry {
    /// Throughput counters: name → monotonic count.
    pub throughput: DashMap<String, AtomicU64>,
    /// Latency begin counters: name → monotonic count of `begin!()` calls.
    pub latency_begin: DashMap<String, AtomicU64>,
    /// Latency end counters: name → monotonic count of `end!()` calls.
    pub latency_end: DashMap<String, AtomicU64>,
}

impl Default for ProgressPointRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressPointRegistry {
    pub fn new() -> Self {
        Self {
            throughput: DashMap::new(),
            latency_begin: DashMap::new(),
            latency_end: DashMap::new(),
        }
    }

    /// Initialize the global registry. Called once by the profiler.
    pub fn init_global() {
        let _ = REGISTRY.set(ProgressPointRegistry::new());
    }

    /// Get the global registry (returns None if profiler not initialized).
    pub fn global() -> Option<&'static ProgressPointRegistry> {
        REGISTRY.get()
    }

    /// Increment a throughput progress point.
    pub fn record_throughput(&self, name: &str) {
        self.throughput
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a latency begin event.
    pub fn record_begin(&self, name: &str) {
        self.latency_begin
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a latency end event.
    pub fn record_end(&self, name: &str) {
        self.latency_end
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot all throughput counters (returns name → current count).
    pub fn snapshot_throughput(&self) -> Vec<(String, u64)> {
        self.throughput
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Snapshot all latency begin counters.
    pub fn snapshot_latency_begin(&self) -> Vec<(String, u64)> {
        self.latency_begin
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Snapshot all latency end counters.
    pub fn snapshot_latency_end(&self) -> Vec<(String, u64)> {
        self.latency_end
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect()
    }
}

/// Record a throughput progress point. Call this when a unit of work completes.
///
/// ```ignore
/// tokio_coz::progress!("requests_complete");
/// ```
#[macro_export]
macro_rules! progress {
    ($name:expr) => {
        if let Some(registry) = $crate::progress::ProgressPointRegistry::global() {
            registry.record_throughput($name);
        }
    };
}

/// Record the beginning of a latency measurement.
///
/// ```ignore
/// tokio_coz::begin!("request_latency");
/// // ... do work ...
/// tokio_coz::end!("request_latency");
/// ```
#[macro_export]
macro_rules! begin {
    ($name:expr) => {
        if let Some(registry) = $crate::progress::ProgressPointRegistry::global() {
            registry.record_begin($name);
        }
    };
}

/// Record the end of a latency measurement.
#[macro_export]
macro_rules! end {
    ($name:expr) => {
        if let Some(registry) = $crate::progress::ProgressPointRegistry::global() {
            registry.record_end($name);
        }
    };
}
