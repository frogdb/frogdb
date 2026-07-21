# Shard-worker fail-stop supervision — implementation report

Implements the accepted decision of
`.scratch/concurrency-testing/issues/05-vll-phase3-partial-commit-decision.md`:
**Option 4 = 4a supervision + 4c abort with shutdown guard** (4b fence subsumed while 4c is
unconditional; 2b txn-framing/abort-on-recovery deferred to the durability phase; no VLL
coordinator changes).

## Design choices

### Supervisor shape
New module `frogdb-server/crates/server/src/server/shard_supervisor.rs`. A single supervisor
tokio task **owns** the shard-worker join handles (paired with their shard id) and watches them
live via `futures::future::select_all` in a drain loop. When any worker terminates:

- **Node live** (not shutting down): fatal. CRITICAL `tracing::error!` with `shard_id` and — on
  panic — the extracted panic payload, then invoke the fail-stop handler.
- **Panic vs early return** both handled: `Err(JoinError)` (payload via `try_into_panic` + downcast
  to `&str`/`String`) and `Ok(())` (early return, equally fatal) each fail-stop.
- **Node shutting down**: expected. Log at `debug!` and do **not** invoke the handler.

`select_all` chosen over `JoinSet` so each worker keeps being spawned exactly as before —
`spawn(shard_monitor.instrument(worker.run()))`, preserving `tokio_metrics::TaskMonitor` — with
shard id paired externally (no task-id map, no new deps; `futures` already a workspace dep).

The supervisor handle completes only once every worker drains, so it replaces the per-worker
handle Vec in the shutdown path.

### Handler trait (injectable fail-stop)
`pub trait FailStopHandler { fn on_shard_failure(&self, shard_id: usize); }`, passed as
`Arc<dyn FailStopHandler>`. Production default `AbortFailStop` logs the abort decision then
`std::process::abort()` (no unwinding; SIGABRT → orchestrator restart). Supervisor emits the
CRITICAL detail line before the handler runs. Tests inject a recording handler.

### Shutdown-signal reuse (guard)
Reuses the existing `HealthChecker` signal — no parallel flag. `shutdown_subsystems` calls
`health_checker.shutdown()` (alive→false) as its first action, before sending `Shutdown` to any
worker; supervisor checks `!health_checker.check_live().is_ok()`. Ordering guarantees any
shutdown-time completion sees alive==false — no race, no false abort on clean/turmoil teardown.

## TDD evidence
Two unit tests in `shard_supervisor.rs`:
1. `panicking_worker_invokes_fail_stop_with_shard_id` — live node, parked worker + panicking
   worker; asserts handler fired exactly once with shard id 1.
2. `shutdown_completion_does_not_invoke_fail_stop` — node marked shutdown; normal-return + panic
   workers both drain; asserts handler never fired and supervisor task returns.

RED (supervise stubbed to drain without classifying): panicking test FAILED
("fail-stop handler was not invoked: Elapsed"), 1 passed 1 failed.
GREEN (real impl): both pass. No test calls process::abort in-process.

## Files changed
- `shard_supervisor.rs` (new) — trait, AbortFailStop, spawn_shard_supervisor, supervise loop,
  panic-payload extraction, 2 tests.
- `server/mod.rs` — declare module; spawn supervisor after spawn_shard_workers (wires
  health_checker + AbortFailStop); field `shard_handles: Vec<JoinHandle>` →
  `shard_supervisor_handle: Option<JoinHandle<()>>`.
- `server/shards.rs` — `spawn_shard_workers` returns `Vec<(usize, JoinHandle<()>)>`.
- `server/subsystems.rs` — `SubsystemHandles.shard_handles` → `shard_supervisor`; shutdown awaits
  the supervisor handle instead of looping per-worker handles.

## Test results
- `cargo test -p frogdb-server --lib shard_supervisor` — 2 passed (GREEN).
- `just test frogdb-server` (full) — 1726 passed, 0 failed (1 leaky, pre-existing/unrelated).
- turmoil `client_pause_write_vs_exec` — 1 passed (teardown does not trigger fail-stop).
- `just lint frogdb-server` — clean. `just fmt frogdb-server` — applied.

## Concerns / notes
- No config flag to disable fail-stop — per YAGNI + decision (4c unconditional, no degrade mode).
  Agree; revisit only if a graceful-degrade (4b fence) mode is added.
- 2b (durability phase) outstanding: WAL recovery still resurrects a pre-abort partial cross-shard
  commit until txn framing + abort-on-recovery lands. Deferred by the decision.
- Early-return treated as fatal: `ShardWorker::run` only returns on Shutdown today, so outside
  shutdown this only fires on a genuine bug — matches intent.

## Reviewer fixes (post-approval)

1. **Abort-time diagnostic survives file-only logging.** `AbortFailStop::on_shard_failure` now
   takes a `reason: &str` and `eprintln!`s the shard id + panic payload straight to stderr
   *before* the tracing `error!` and `std::process::abort()`. The file layer's buffered
   `tracing_appender` non-blocking writer (WorkerGuard) will not flush before SIGABRT, and console
   output can be `LogOutput::None` — the direct stderr write guarantees the diagnostic. The panic
   payload is threaded from the supervisor into the handler (trait signature gained `reason`), and
   the unit test now asserts the payload (`"shard 1 boom"`) reaches the handler.
2. **Implicit-invariant comment.** `AbortFailStop`'s doc now records the coupling: the shutdown
   guard rests on `alive` being cleared before workers are told to stop, and the failed-startup
   path (`run_until` early-return) never flips `alive` — safe only because `ShardWorker::run` never
   returns on channel close. If that ever changes, the error path must flip `alive` first.
3. **Helper.** Added `HealthChecker::is_shutting_down()` (telemetry/src/health.rs); the supervisor
   guard uses it instead of `!check_live().is_ok()`.

Re-verified: `cargo test -p frogdb-server --lib shard_supervisor` 2/2 pass; `just lint
frogdb-server` clean; `just fmt frogdb-server` + `frogdb-telemetry` applied.
