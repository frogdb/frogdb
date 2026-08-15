//! Panic isolation at the shard boundary.
//!
//! The shard worker is a single Tokio task owning every key on its shard. Until
//! this seam existed, a panic raised anywhere below the event loop — including
//! inside a third-party dependency, where no `unwrap()` grep can find it —
//! unwound that task, and
//! [`shard_supervisor`](../../../../server/src/server/shard_supervisor.rs)
//! turned it into `std::process::abort()`. One malformed query could therefore
//! take the whole node down; round-2 issue 63 (`FT.SEARCH … LIMIT 0 0` reaching
//! a `assert_ne!` inside tantivy) was exactly that, reachable before
//! authentication.
//!
//! The isolation here is the *structural backstop*, not a substitute for fixing
//! the arithmetic that panics. A caught panic is always a bug: the shard
//! survives, but a client received `-ERR internal error` instead of an answer,
//! and [`ShardPanicsIsolated`](frogdb_types::metrics::definitions::ShardPanicsIsolated)
//! is what makes that visible.
//!
//! # Recorded decisions
//!
//! **Poisoning.** A caught panic must not leave shard state owned by a command
//! that no longer exists. Three pieces of state are command-scoped and are
//! reset or released on the panic path:
//!
//! - `Store::suppress_touch`, set per `Execute` message and cleared after it.
//!   A panic in between used to leave it stuck on for every later command on
//!   the shard. [`ShardWorker::recover_from_panic`] clears it.
//! - `pending_serve_propagations`, populated and drained inside a single
//!   `run_write_effects`. A panic mid-effects would let a dead command's
//!   synthesized pops ride along with the *next* write's broadcast, so the
//!   buffer is dropped.
//! - The VLL lock entry of a dequeued op. `handle_vll_execute` releases it on
//!   the panic path exactly as on the success path; skipping it would leak the
//!   op's key locks *and* leave `executing_ops` incremented, which permanently
//!   blocks any parked continuation lock. This is the "a panic mid-transaction
//!   must not leave the lock owned by a dead command" case.
//!
//! A panic inside a queued MULTI command is caught *per command*, one frame
//! below the `ExecTransaction` guard. That command's slot in the `EXEC` array
//! becomes `-ERR internal error` and the remaining queued commands still run —
//! which is both Redis's own rule (a runtime error inside `EXEC` is reported in
//! that command's slot and does not abort the transaction) and the single
//! command contract applied unchanged, so there is one rule and not two.
//! Catching per command also keeps `execute_transaction`'s frame alive, so the
//! rollback snapshot captured *before* the panicking write is still in the undo
//! list if the batch's WAL write later fails. Residual hazard, inherent to
//! isolating a panic rather than preventing it: the dead command contributes no
//! `WriteCommandMeta`, so a mutation it half-applied is not written to the WAL
//! or broadcast — identical to the single-command path, and the reason a caught
//! panic is a bug to fix and not a supported mode of operation.
//!
//! The continuation lock itself is deliberately **not** touched — it is owned by
//! a connection across many messages, not by the panicking command, and its
//! release is driven by the owner's own lifecycle.
//!
//! **Escalation.** Repeated panics increment
//! [`ShardPanicsIsolated`](frogdb_types::metrics::definitions::ShardPanicsIsolated)
//! (labelled by shard and isolation site) and emit one `error!` each; nothing
//! auto-kills the shard or the process. Rationale: an auto-kill converts a
//! bounded per-query defect back into the exact availability loss this seam
//! exists to prevent, and a shard that answers `-ERR internal error` for one
//! command family while serving every other key is strictly better than a shard
//! that is gone. A repeat-panic loop is not silent — it is a monotonically
//! climbing counter operators can alert on. Fail-stop is retained for panics
//! *outside* the guarded boundary (the maintenance arms of the event loop, the
//! worker's own setup and teardown): those are not attributable to one client's
//! message and the supervisor still aborts on them.
//!
//! **`panic = "abort"`.** Verified absent: no `panic` key appears in any
//! `[profile.*]` section of any manifest in the workspace (the root
//! `Cargo.toml` defines `dev`, `release`, `docker` and `profiling` without
//! one), so unwinding is in effect in every profile and this isolation is live
//! in release builds — not just in tests. Adding `panic = "abort"` to any
//! profile would silently make every guard here inert.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;

/// Where a caught panic was isolated. Becomes the `site` metric label and the
/// `site` field on the `error!` log, so a repeat-panic alert says which
/// boundary is absorbing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PanicSite {
    /// A single command executed for a client (`CoreMsg::Execute`).
    Command,
    /// One shard's slice of a scatter/gather fan-out (`CoreMsg::ScatterRequest`).
    Scatter,
    /// One queued command inside `EXEC` — caught per command so the
    /// transaction's rollback snapshots survive.
    TransactionCommand,
    /// The `EXEC` batch outside the per-command guard (watch validation, the
    /// batched write-effects phase).
    Transaction,
    /// A VLL-queued scatter op, executed after its locks were granted.
    VllExecute,
    /// The outer net: any other shard message category.
    Message,
}

impl PanicSite {
    /// Stable label for metrics and logs.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            PanicSite::Command => "command",
            PanicSite::Scatter => "scatter",
            PanicSite::TransactionCommand => "transaction_command",
            PanicSite::Transaction => "transaction",
            PanicSite::VllExecute => "vll_execute",
            PanicSite::Message => "message",
        }
    }
}

/// The reply every isolated panic turns into. Deliberately opaque: a panic
/// message can carry key names, argument bytes or file paths, none of which
/// belong on the wire.
pub(crate) const INTERNAL_ERROR: &str = "ERR internal error";

/// Render a caught panic payload for the log.
///
/// Mirrors `shard_supervisor::panic_payload`, minus the `JoinError` unwrap —
/// `catch_unwind` hands back the payload directly.
pub(crate) fn payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Run `fut` to completion, converting an unwind into `Err(message)`.
///
/// [`AssertUnwindSafe`] is the honest annotation at this boundary: the shard's
/// state genuinely *can* be observed after a panic — that is the whole point —
/// so the caller takes responsibility for restoring it, which
/// [`ShardWorker::recover_from_panic`](super::worker::ShardWorker::recover_from_panic)
/// does. The future is fully dropped before this returns, so the caller's
/// `&mut self` borrow is free again.
pub(crate) async fn caught<F: Future>(fut: F) -> Result<F::Output, String> {
    AssertUnwindSafe(fut)
        .catch_unwind()
        .await
        .map_err(|payload| payload_message(payload.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn caught_returns_the_value_when_nothing_panics() {
        assert_eq!(caught(async { 7 }).await, Ok(7));
    }

    #[tokio::test]
    async fn caught_reports_a_panic_after_an_await_point() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = caught(async {
            tokio::task::yield_now().await;
            panic!("boom {}", 1);
        })
        .await;
        std::panic::set_hook(prev);

        assert_eq!(outcome, Err("boom 1".to_string()));
    }

    #[test]
    fn payload_message_handles_both_string_shapes_and_neither() {
        assert_eq!(payload_message(&"static"), "static");
        assert_eq!(payload_message(&"owned".to_string()), "owned");
        assert_eq!(payload_message(&42u8), "<non-string panic payload>");
    }

    #[test]
    fn every_site_has_a_distinct_label() {
        let sites = [
            PanicSite::Command,
            PanicSite::Scatter,
            PanicSite::TransactionCommand,
            PanicSite::Transaction,
            PanicSite::VllExecute,
            PanicSite::Message,
        ];
        let labels: std::collections::HashSet<&str> = sites.iter().map(|s| s.as_str()).collect();
        assert_eq!(labels.len(), sites.len());
    }
}

/// The forcing tests for the isolation itself: a command that really panics,
/// driven through a real [`ShardWorker`].
#[cfg(test)]
mod isolation_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    use bytes::Bytes;
    use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
    use tokio::sync::{mpsc, oneshot};

    use crate::ShardReadyResult;
    use crate::command::{
        Arity, Command, CommandContext, CommandFlags, ConnMutation, ExecutionStrategy, WaiterWake,
        WalStrategy,
    };
    use crate::command_spec::{
        AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec, ReindexSpec,
    };
    use crate::eviction::EvictionConfig;
    use crate::noop::MetricsRecorder;
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{CoreMsg, ScatterOp, ShardReceiver, ShardSender};
    use crate::shard::types::{PartialResult, TransactionResult};
    use crate::store::Store;
    use crate::types::Value;
    use crate::vll::LockMode;

    const PANICS_METRIC: &str = "frogdb_shard_panics_isolated_total";

    /// A command whose handler panics. Stands in for the whole class this seam
    /// exists for — an `assert!`/index/overflow anywhere below the shard event
    /// loop, including inside a dependency (round-2 issue 63 was tantivy's
    /// `assert_ne!(limit, 0)`).
    struct Boom;
    impl Command for Boom {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "BOOM",
                arity: Arity::AtLeast(0),
                flags: CommandFlags::READONLY,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                reindex: ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: ConnMutation::None,
                strategy: ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            panic!("boom from inside a command handler");
        }
    }

    /// A command that answers, so a test can prove the shard still serves after
    /// absorbing a panic and that a transaction kept executing past one.
    struct Fine;
    impl Command for Fine {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "FINE",
                arity: Arity::AtLeast(0),
                flags: CommandFlags::READONLY,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                reindex: ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: ConnMutation::None,
                strategy: ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    /// Counts increments so a test can read `frogdb_shard_panics_isolated_total`
    /// back.
    #[derive(Default)]
    struct RecordingRecorder {
        counters: Mutex<HashMap<String, u64>>,
    }

    impl MetricsRecorder for RecordingRecorder {
        fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
            *self
                .counters
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert(0) += value;
        }
        fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn counter_value(&self, name: &str) -> Option<u64> {
            self.counters.lock().unwrap().get(name).copied()
        }
    }

    /// The process-wide panic hook, as `std::panic::take_hook` hands it back.
    type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

    /// Silence the default panic hook for the duration of a forcing test — the
    /// backtrace it prints is noise, not a failure. Restored on drop so a later
    /// genuine panic still reports normally.
    struct QuietPanics(Option<PanicHook>);

    impl QuietPanics {
        fn install() -> Self {
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            Self(Some(prev))
        }
    }

    impl Drop for QuietPanics {
        fn drop(&mut self) {
            if let Some(prev) = self.0.take() {
                std::panic::set_hook(prev);
            }
        }
    }

    fn worker_with(registry: CommandRegistry, recorder: Arc<RecordingRecorder>) -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_, conn_rx) = mpsc::channel(16);
        // The sender lives on inside `shard_senders`, keeping the receiver open.
        let shard_senders = Arc::new(vec![ShardSender::new(msg_tx)]);
        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            shard_senders,
            Arc::new(registry),
            EvictionConfig::default(),
            recorder,
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    fn cmd(name: &'static str) -> Arc<ParsedCommand> {
        Arc::new(ParsedCommand::new(
            Bytes::from_static(name.as_bytes()),
            vec![],
        ))
    }

    async fn execute(worker: &mut ShardWorker, name: &'static str) -> Response {
        let (tx, rx) = oneshot::channel();
        worker
            .dispatch_core(CoreMsg::Execute {
                command: cmd(name),
                conn_id: 1,
                txid: None,
                protocol_version: ProtocolVersion::Resp2,
                track_reads: false,
                no_touch: true,
                response_tx: tx,
            })
            .await;
        rx.await.expect("shard replied")
    }

    fn error_text(response: &Response) -> String {
        match response {
            Response::Error(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    /// Round-2 issue 63 / campaign-2 issue 07: a panicking command handler must
    /// not take the shard with it. The client gets `-ERR internal error`, the
    /// panic is counted, and the very next command on the *same* worker is
    /// answered normally.
    ///
    /// Also pins the poisoning half: this `Execute` carries `no_touch: true`,
    /// which sets `suppress_touch` on the store immediately before the panicking
    /// handler runs. Without the reset on the recovery path that flag would stay
    /// on for every later command on this shard.
    #[tokio::test]
    async fn a_panicking_command_is_answered_with_an_error_and_the_shard_keeps_serving() {
        let quiet = QuietPanics::install();
        let recorder = Arc::new(RecordingRecorder::default());
        let mut registry = CommandRegistry::new();
        registry.register(Boom);
        registry.register(Fine);
        let mut worker = worker_with(registry, recorder.clone());

        let response = execute(&mut worker, "BOOM").await;
        // Restore the hook before asserting, so a *failing* assertion still
        // reports normally.
        drop(quiet);
        assert_eq!(error_text(&response), super::INTERNAL_ERROR);
        assert_eq!(
            recorder.counter_value(PANICS_METRIC),
            Some(1),
            "the caught panic must be counted — it is the only signal a repeat \
             loop leaves behind"
        );

        // Checked before the next command runs: a healthy command resets the
        // flag on its own way out, so asserting afterwards would pass either way.
        assert!(
            !worker.store.suppress_touch_enabled(),
            "suppress_touch must not survive the panicking command that set it"
        );

        // Liveness: the worker is not dead.
        assert_eq!(execute(&mut worker, "FINE").await, Response::ok());
        assert_eq!(
            recorder.counter_value(PANICS_METRIC),
            Some(1),
            "the healthy command must not add to the panic counter"
        );
    }

    /// Redis reports a runtime error inside `EXEC` in that command's own slot
    /// and keeps executing the rest; an isolated panic follows the same rule.
    /// The array comes back whole — `[OK, -ERR internal error, OK]` — so the
    /// command after the panicking one really ran.
    #[tokio::test]
    async fn a_panic_inside_exec_fails_only_that_command() {
        let quiet = QuietPanics::install();
        let recorder = Arc::new(RecordingRecorder::default());
        let mut registry = CommandRegistry::new();
        registry.register(Boom);
        registry.register(Fine);
        let mut worker = worker_with(registry, recorder.clone());

        let (tx, rx) = oneshot::channel();
        worker
            .dispatch_core(CoreMsg::ExecTransaction {
                commands: vec![
                    ParsedCommand::new(Bytes::from_static(b"FINE"), vec![]),
                    ParsedCommand::new(Bytes::from_static(b"BOOM"), vec![]),
                    ParsedCommand::new(Bytes::from_static(b"FINE"), vec![]),
                ],
                watches: vec![],
                conn_id: 1,
                protocol_version: ProtocolVersion::Resp2,
                admission: crate::write_seam::WriteAdmission::internal(),
                response_tx: tx,
            })
            .await;

        let reply = rx.await.expect("shard replied");
        drop(quiet);
        match reply {
            TransactionResult::Success(results) => {
                assert_eq!(results.len(), 3, "got {results:?}");
                assert_eq!(results[0], Response::ok());
                assert_eq!(error_text(&results[1]), super::INTERNAL_ERROR);
                assert_eq!(
                    results[2],
                    Response::ok(),
                    "the queued command after the panicking one must still run"
                );
            }
            other => panic!("expected Success with per-command results, got {other:?}"),
        }
        assert_eq!(recorder.counter_value(PANICS_METRIC), Some(1));
        assert_eq!(execute(&mut worker, "FINE").await, Response::ok());
    }

    // FM-VLL-005
    /// A VLL-queued op that panics mid-execution must still release its locks.
    ///
    /// The injection is a real latent panic, not a synthetic one:
    /// `scatter_write_handler` panics when the effect pipeline's command is
    /// absent from the registry, and this worker's registry has no `DEL`. If the
    /// unwind escaped `handle_vll_execute`, `release_after_execution` would be
    /// skipped — the key's intent would stay in the lock table and
    /// `executing_ops` would stay incremented, permanently blocking every later
    /// request on that key. The second op below is what proves it did not: it
    /// can only be granted if the first op's locks were released.
    #[tokio::test]
    async fn a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving() {
        let quiet = QuietPanics::install();
        let recorder = Arc::new(RecordingRecorder::default());
        let mut registry = CommandRegistry::new();
        registry.register(Fine);
        let mut worker = worker_with(registry, recorder.clone());

        let key = Bytes::from_static(b"k");
        worker.store.set(key.clone(), Value::string("v"));

        // Acquire, then execute — the DEL effect pipeline panics on the missing
        // handler.
        let (ready_tx, ready_rx) = oneshot::channel();
        worker
            .handle_vll_lock_request(
                7,
                vec![key.clone()],
                LockMode::Write,
                ScatterOp::Del,
                ready_tx,
            )
            .await;
        assert!(matches!(ready_rx.await, Ok(ShardReadyResult::Ready)));

        let (resp_tx, resp_rx) = oneshot::channel();
        worker.handle_vll_execute(7, resp_tx).await;
        let reply = resp_rx.await.expect("vll reply");
        drop(quiet);
        match reply {
            PartialResult::Keyed(entries) => {
                let (_, response) = &entries[0];
                assert_eq!(error_text(response), super::INTERNAL_ERROR);
            }
            other => panic!("expected a keyed error reply, got {other:?}"),
        }
        assert_eq!(recorder.counter_value(PANICS_METRIC), Some(1));

        // The lock table is clean...
        assert!(
            worker.collect_lock_table_info().intents.is_empty(),
            "the dead op's key intent must not survive it"
        );
        // ...and, decisively, a later op on the same key is still grantable.
        let (ready_tx, ready_rx) = oneshot::channel();
        worker
            .handle_vll_lock_request(
                8,
                vec![key.clone()],
                LockMode::Write,
                ScatterOp::Exists,
                ready_tx,
            )
            .await;
        assert!(
            matches!(ready_rx.await, Ok(ShardReadyResult::Ready)),
            "a later op on the same key must still be grantable"
        );
        let (resp_tx, resp_rx) = oneshot::channel();
        worker.handle_vll_execute(8, resp_tx).await;
        assert!(matches!(
            resp_rx.await.expect("vll reply"),
            PartialResult::Keyed(_)
        ));
        assert_eq!(
            recorder.counter_value(PANICS_METRIC),
            Some(1),
            "the healthy op must not add to the panic counter"
        );
    }
}
