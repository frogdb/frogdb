//! Shard-worker supervision with a fail-stop policy.
//!
//! Shard workers are otherwise-unsupervised tokio tasks: a panic (or an early
//! return) leaves the shard silently dead — 1/N of the keyspace unreachable,
//! blocked waiters never wake, active expiry stops — a *zombie node*, which in
//! an HA deployment is worse than a crash (no failover trigger fires).
//!
//! This supervisor watches every shard-worker handle live. When a worker
//! terminates **while the node is live** (not shutting down) it is fatal: the
//! supervisor emits a CRITICAL log and invokes an injectable [`FailStopHandler`]
//! (production default: [`AbortFailStop`] = abort the process so an orchestrator
//! restarts a clean node). When the node is shutting down, worker completion is
//! expected and merely logged at debug — that guard is what keeps clean
//! shutdowns (and turmoil-sim teardowns) from registering as crashes.

use frogdb_core::sync::Arc;
use frogdb_telemetry::HealthChecker;
use futures::future::select_all;
use tracing::{debug, error};

use crate::net::{JoinHandle, spawn};

/// Fail-stop action taken when a shard worker dies unexpectedly.
///
/// Injectable so tests can assert the fail-stop fires without aborting the test
/// process. The production default is [`AbortFailStop`].
pub trait FailStopHandler: Send + Sync + 'static {
    /// A shard worker (`shard_id`) terminated unexpectedly while the node was
    /// live; `reason` is the panic payload (or an early-return description). The
    /// supervisor has already logged the event at CRITICAL; this performs the
    /// fail-stop action (production: abort the process).
    fn on_shard_failure(&self, shard_id: usize, reason: &str);
}

/// Production fail-stop: abort the process.
///
/// A dead shard worker means part of the keyspace is permanently unreachable
/// and blocked waiters will never wake — the node is a zombie. Aborting turns a
/// silent zombie into a clean crash an orchestrator can detect and restart.
///
/// Invariant this fail-stop relies on: the supervisor's shutdown guard
/// distinguishes a crash from clean teardown via the node's `alive` flag, which
/// `shutdown_subsystems` clears *before* telling workers to stop. The
/// failed-startup path (`run_until`'s early return on `start_subsystems()?`)
/// never flips `alive`, and today that is safe only because a shard worker's
/// `run()` loop never returns on channel close (its `select!` interval-tick
/// branches keep it alive; the `else => break` arm is unreachable). If `run()`
/// is ever changed to return on channel close, that error path MUST flip `alive`
/// first, or a benign teardown would be misread here as a crash and abort.
#[derive(Debug, Default, Clone, Copy)]
pub struct AbortFailStop;

impl FailStopHandler for AbortFailStop {
    fn on_shard_failure(&self, shard_id: usize, reason: &str) {
        // The supervisor already emitted the CRITICAL detail line via tracing,
        // but the file layer uses a buffered non-blocking writer whose
        // `WorkerGuard` will not flush before SIGABRT, and console output may be
        // disabled entirely. Write the diagnostic straight to stderr first so it
        // survives regardless of the tracing sink, then keep the structured log.
        eprintln!("FATAL: shard {shard_id} worker died ({reason}); aborting process (fail-stop)");
        error!(shard_id, reason, "shard-worker fail-stop: aborting process");
        // Hard-abort: no unwinding, no destructors, immediate SIGABRT.
        std::process::abort();
    }
}

/// Spawn the shard-worker supervisor.
///
/// `labeled_handles` pairs each shard id with its worker's join handle. The
/// returned handle completes once **every** shard worker has terminated, so the
/// shutdown path can await it in place of the individual worker handles.
///
/// `health_checker` is the node's existing shutdown signal (its `alive` flag is
/// cleared at the start of `shutdown_subsystems`): while the node is live a
/// worker completion is fatal; once shutdown has begun it is expected.
pub(super) fn spawn_shard_supervisor(
    labeled_handles: Vec<(usize, JoinHandle<()>)>,
    health_checker: HealthChecker,
    handler: Arc<dyn FailStopHandler>,
) -> JoinHandle<()> {
    spawn(supervise(labeled_handles, health_checker, handler))
}

/// Extract a human-readable panic payload from a [`tokio::task::JoinError`].
fn panic_payload(join_err: tokio::task::JoinError) -> String {
    match join_err.try_into_panic() {
        Ok(payload) => {
            if let Some(s) = payload.downcast_ref::<&'static str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "<non-string panic payload>".to_string()
            }
        }
        // Not a panic: the task was aborted/cancelled.
        Err(_) => "<task cancelled>".to_string(),
    }
}

/// Supervisor loop: wait for any shard worker to terminate, classify it against
/// the shutdown guard, and fail-stop on unexpected death.
async fn supervise(
    labeled_handles: Vec<(usize, JoinHandle<()>)>,
    health_checker: HealthChecker,
    handler: Arc<dyn FailStopHandler>,
) {
    let (mut shard_ids, mut handles): (Vec<usize>, Vec<JoinHandle<()>>) =
        labeled_handles.into_iter().unzip();

    while !handles.is_empty() {
        let (result, idx, remaining) = select_all(handles).await;
        handles = remaining;
        let shard_id = shard_ids.remove(idx);

        // Reuse the node's existing shutdown signal (see `AbortFailStop` for the
        // invariant this guard depends on).
        match result {
            _ if health_checker.is_shutting_down() => {
                // Expected: graceful shutdown sends each worker `Shutdown` only
                // after flipping the health flag, so any completion observed
                // here during shutdown is benign teardown, panic or not.
                debug!(shard_id, "shard worker exited during shutdown (expected)");
            }
            Ok(()) => {
                let reason = "worker returned early";
                error!(
                    shard_id,
                    "CRITICAL: shard worker returned unexpectedly while node is live; \
                     1/N of the keyspace is now unreachable — invoking fail-stop"
                );
                handler.on_shard_failure(shard_id, reason);
            }
            Err(join_err) => {
                let payload = panic_payload(join_err);
                error!(
                    shard_id,
                    panic = %payload,
                    "CRITICAL: shard worker panicked while node is live; 1/N of the \
                     keyspace is now unreachable — invoking fail-stop"
                );
                handler.on_shard_failure(shard_id, &payload);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    /// Test handler that records the `(shard_id, reason)` pairs it was invoked
    /// with (instead of aborting) and notifies a waiter after each invocation.
    #[derive(Default)]
    struct RecordingFailStop {
        calls: std::sync::Mutex<Vec<(usize, String)>>,
        count: AtomicUsize,
        notify: Notify,
    }

    impl RecordingFailStop {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
        fn calls(&self) -> Vec<(usize, String)> {
            self.calls.lock().unwrap().clone()
        }
        fn shard_ids(&self) -> Vec<usize> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(id, _)| *id)
                .collect()
        }
    }

    impl FailStopHandler for RecordingFailStop {
        fn on_shard_failure(&self, shard_id: usize, reason: &str) {
            self.calls
                .lock()
                .unwrap()
                .push((shard_id, reason.to_string()));
            self.count.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_one();
        }
    }

    /// A live node: a panicking worker must invoke the handler exactly once with
    /// the panicking shard's id, while healthy workers are left untouched.
    #[tokio::test]
    async fn panicking_worker_invokes_fail_stop_with_shard_id() {
        let health = HealthChecker::new();
        health.set_ready(); // node is live (alive == true)
        let handler = RecordingFailStop::new();

        // Shard 0 stays alive (parks forever); shard 1 panics.
        let alive: JoinHandle<()> = spawn(async {
            std::future::pending::<()>().await;
        });
        let panicker: JoinHandle<()> = spawn(async {
            panic!("shard 1 boom");
        });

        let sup = spawn_shard_supervisor(vec![(0, alive), (1, panicker)], health, handler.clone());

        // Await the handler firing (bounded so a regression fails fast).
        tokio::time::timeout(std::time::Duration::from_secs(5), handler.notify.notified())
            .await
            .expect("fail-stop handler was not invoked");

        assert_eq!(
            handler.shard_ids(),
            vec![1],
            "handler must fire once for shard 1"
        );
        assert_eq!(handler.count.load(Ordering::SeqCst), 1);
        // The panic payload is threaded through to the handler as the reason.
        assert_eq!(handler.calls()[0].1, "shard 1 boom");

        sup.abort();
    }

    /// Shutdown guard: a worker completing *after* the node is marked shutting
    /// down must NOT invoke the handler (clean teardown, not a crash).
    #[tokio::test]
    async fn shutdown_completion_does_not_invoke_fail_stop() {
        let health = HealthChecker::new();
        health.set_ready();
        health.shutdown(); // node is shutting down (alive == false)
        let handler = RecordingFailStop::new();

        // A worker that returns normally (as during graceful shutdown).
        let worker: JoinHandle<()> = spawn(async {});
        // A worker that panics during shutdown is still benign teardown.
        let panicker: JoinHandle<()> = spawn(async {
            panic!("late shutdown panic");
        });

        let sup = spawn_shard_supervisor(vec![(0, worker), (1, panicker)], health, handler.clone());

        // The supervisor drains both handles and returns without firing.
        tokio::time::timeout(std::time::Duration::from_secs(5), sup)
            .await
            .expect("supervisor did not drain within timeout")
            .expect("supervisor task panicked");

        assert!(
            handler.calls().is_empty(),
            "handler must not fire during shutdown, got {:?}",
            handler.calls()
        );
    }
}
