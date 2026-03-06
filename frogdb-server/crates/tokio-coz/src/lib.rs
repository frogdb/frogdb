//! `tokio-coz`: Causal profiler for Tokio async runtimes.
//!
//! Adapts the coz causal profiling methodology to async Rust by profiling tracing spans
//! instead of source lines, injecting delays at poll boundaries via tokio's runtime hooks.
//!
//! # Quick Start
//!
//! ```ignore
//! use tokio_coz::{CausalProfiler, ProfilerConfig};
//!
//! let profiler = CausalProfiler::new(ProfilerConfig::default());
//!
//! let runtime = tokio::runtime::Builder::new_multi_thread()
//!     .enable_all()
//!     .on_task_spawn(profiler.on_task_spawn())
//!     .on_before_task_poll(profiler.on_before_task_poll())
//!     .on_after_task_poll(profiler.on_after_task_poll())
//!     .on_task_terminate(profiler.on_task_terminate())
//!     .build()
//!     .unwrap();
//!
//! tracing_subscriber::registry()
//!     .with(profiler.tracing_layer())
//!     .init();
//!
//! runtime.block_on(async {
//!     profiler.start().await;
//!     // ... application code ...
//! });
//!
//! profiler.report();
//! ```
//!
//! # Requires
//!
//! Compile with `RUSTFLAGS="--cfg tokio_unstable"` to enable the runtime poll hooks.

pub mod config;
pub mod delay;
pub mod experiment;
pub mod hooks;
pub mod progress;
pub mod reporter;
pub mod results;
pub mod span_tracker;
pub mod state;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub use crate::config::{ProfilerConfig, SelectionStrategy};
use crate::delay::DelayController;
use crate::experiment::ExperimentEngine;
use crate::progress::ProgressPointRegistry;
use crate::results::ResultCollector;
use crate::span_tracker::SpanTracker;
use crate::state::SharedState;

/// The main causal profiler. Create one, wire it into your tokio runtime and tracing subscriber,
/// then call `start()` to begin profiling.
pub struct CausalProfiler {
    state: Arc<SharedState>,
    #[cfg_attr(not(tokio_unstable), allow(dead_code))]
    delay: Arc<DelayController>,
    config: ProfilerConfig,
    results: Arc<ResultCollector>,
    running: Arc<AtomicBool>,
}

impl CausalProfiler {
    /// Create a new profiler with the given configuration.
    pub fn new(config: ProfilerConfig) -> Self {
        let state = Arc::new(SharedState::new());
        let delay = Arc::new(DelayController::new(state.clone(), &config));
        let results = Arc::new(ResultCollector::new());

        ProgressPointRegistry::init_global();

        Self {
            state,
            delay,
            config,
            results,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a tracing `Layer` that tracks span entry/exit for each task.
    ///
    /// Wire this into your `tracing_subscriber::Registry`.
    pub fn tracing_layer(&self) -> SpanTracker {
        SpanTracker::new(self.state.clone())
    }

    /// Get the `on_task_spawn` hook closure for the tokio runtime builder.
    ///
    /// Requires `RUSTFLAGS="--cfg tokio_unstable"`.
    #[cfg(tokio_unstable)]
    pub fn on_task_spawn(&self) -> impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static {
        let state = self.state.clone();
        let delay = self.delay.clone();
        let (on_spawn, _, _, _) = hooks::make_hook_closures(state, delay);
        on_spawn
    }

    /// Get the `on_before_task_poll` hook closure for the tokio runtime builder.
    ///
    /// Requires `RUSTFLAGS="--cfg tokio_unstable"`.
    #[cfg(tokio_unstable)]
    pub fn on_before_task_poll(
        &self,
    ) -> impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static {
        let state = self.state.clone();
        let delay = self.delay.clone();
        let (_, on_before, _, _) = hooks::make_hook_closures(state, delay);
        on_before
    }

    /// Get the `on_after_task_poll` hook closure for the tokio runtime builder.
    ///
    /// Requires `RUSTFLAGS="--cfg tokio_unstable"`.
    #[cfg(tokio_unstable)]
    pub fn on_after_task_poll(
        &self,
    ) -> impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static {
        let state = self.state.clone();
        let delay = self.delay.clone();
        let (_, _, on_after, _) = hooks::make_hook_closures(state, delay);
        on_after
    }

    /// Get the `on_task_terminate` hook closure for the tokio runtime builder.
    ///
    /// Requires `RUSTFLAGS="--cfg tokio_unstable"`.
    #[cfg(tokio_unstable)]
    pub fn on_task_terminate(
        &self,
    ) -> impl Fn(&tokio::runtime::TaskMeta<'_>) + Send + Sync + 'static {
        let state = self.state.clone();
        let delay = self.delay.clone();
        let (_, _, _, on_terminate) = hooks::make_hook_closures(state, delay);
        on_terminate
    }

    /// Start the experiment engine as a background task.
    ///
    /// Must be called from within the tokio runtime context.
    pub async fn start(&self) {
        self.running.store(true, Ordering::Release);

        let engine = ExperimentEngine::new(
            self.state.clone(),
            self.config.clone(),
            self.results.clone(),
            self.running.clone(),
        );

        tokio::spawn(async move {
            engine.run().await;
        });
    }

    /// Stop the experiment engine.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        self.state.deactivate_experiment();
    }

    /// Generate and print the profiling report.
    ///
    /// Stops the experiment engine if still running, aggregates results,
    /// prints a terminal summary, and optionally writes a JSON file.
    pub fn report(&self) {
        self.stop();

        let profiles = self.results.aggregate();

        reporter::print_summary(&profiles);

        if let Some(ref path) = self.config.output_path {
            match reporter::write_json_report(&profiles, path) {
                Ok(()) => println!("[tokio-coz] Report written to {path}"),
                Err(e) => eprintln!("[tokio-coz] Failed to write report to {path}: {e}"),
            }
        }
    }

    /// Get access to the shared state (for testing/advanced use).
    pub fn shared_state(&self) -> &Arc<SharedState> {
        &self.state
    }

    /// Get access to the result collector (for testing/advanced use).
    pub fn results(&self) -> &Arc<ResultCollector> {
        &self.results
    }
}
