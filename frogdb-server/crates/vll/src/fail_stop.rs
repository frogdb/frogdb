//! Fail-stop escalation for a panic caught in a VLL **write** path.
//!
//! Panic isolation at the shard boundary (`frogdb-core`'s `shard::panic_guard`)
//! keeps one client's `assert!`/index/overflow from taking the node down. That
//! is the right answer for a *read*: nothing was half-mutated, so the op's
//! locks release, the caller gets `-ERR internal error`, and the shard keeps
//! serving.
//!
//! It is not the right answer for a *write*. A panic partway through a mutation
//! leaves structures whose invariants may be broken and, for a cross-shard op,
//! siblings that applied writes this shard never finished. Continuing to serve
//! from that state is how a bounded defect becomes silent data corruption. So a
//! write-path panic escalates to a clean process exit
//! ([`WRITE_PATH_PANIC_EXIT_CODE`]); the node's existing startup recovery
//! replays the WALs, which is the restart-from-clean-state mechanism — there is
//! no second one.
//!
//! CockroachDB, Scylla, Redis and FoundationDB all treat a mid-write panic as
//! process-fatal; none hot-restarts a single shard.
//!
//! The action is behind [`FailStopSink`] so a test can assert that the
//! escalation fired without the test process going away with it. The default
//! installed on every [`VllShardState`](crate::VllShardState) is the production
//! one, [`ProcessExitFailStop`]: unwired code fails closed, and a test that
//! deliberately panics a write op is the one that has to say otherwise.

use std::fmt;

/// Exit code used when a VLL write-path panic escalates.
///
/// `sysexits.h`'s `EX_SOFTWARE` ("internal software error"), chosen because it
/// is distinct from every other way this process can die: 101 (a panic
/// unwinding out of `main`), 134 (`SIGABRT`, what the shard-worker supervisor's
/// fail-stop produces), and 0 (a requested shutdown). An orchestrator can
/// therefore tell "fail-stopped on a write-path panic" from "crashed" by exit
/// code alone.
pub const WRITE_PATH_PANIC_EXIT_CODE: i32 = 70;

/// The panic that triggered the escalation, as handed to a [`FailStopSink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePathPanic {
    /// The transaction whose granted op panicked while executing.
    pub txid: u64,
    /// The rendered panic payload, already logged by the host's panic guard.
    pub message: String,
}

/// How a write-path escalation is carried out.
///
/// Injectable so a forcing test can observe the escalation instead of exiting;
/// the production implementation is [`ProcessExitFailStop`].
pub trait FailStopSink: Send + Sync + fmt::Debug {
    /// A panic escaped a granted **write** op's execution. The host has already
    /// released the op's locks and logged the panic; this ends the process.
    fn fail_stop(&self, panic: &WritePathPanic);
}

/// Production fail-stop: exit the process with [`WRITE_PATH_PANIC_EXIT_CODE`].
///
/// Deliberately *not* a graceful drain. A graceful shutdown keeps serving
/// in-flight work while it winds down, and the state that work would be served
/// from is exactly the state this escalation exists to stop trusting. Exiting
/// runs `atexit` handlers and flushes the C stdio buffers without running Rust
/// destructors, so nothing further is written from the suspect structures;
/// whatever the WAL already made durable is what startup recovery replays.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessExitFailStop;

impl FailStopSink for ProcessExitFailStop {
    // Mutating this body away cannot be observed from inside the test process
    // that the real body would exit — the exclusion is registered in
    // `.cargo/mutants.toml`. The *decision* to call it is what the forcing
    // tests pin (`VllShardState::release_after_panic`).
    fn fail_stop(&self, panic: &WritePathPanic) {
        // Straight to stderr first: the tracing file layer is a buffered
        // non-blocking writer whose guard will not flush on the way out, and
        // console logging may be disabled entirely.
        eprintln!(
            "FATAL: panic in a VLL write path (txid {}): {} — exiting with \
             {WRITE_PATH_PANIC_EXIT_CODE} (fail-stop); startup recovery replays the WAL",
            panic.txid, panic.message,
        );
        std::process::exit(WRITE_PATH_PANIC_EXIT_CODE);
    }
}

/// What became of a panic that escaped a granted op's execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanicEscalation {
    /// Read path: the panic was isolated, the locks released, and the shard
    /// keeps serving.
    Isolated,
    /// Write path: the installed [`FailStopSink`] has been fired. In production
    /// this call does not return.
    FailStop,
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Mutex;

    use super::{FailStopSink, WritePathPanic};

    /// A sink that records what it was told instead of exiting, so a test can
    /// assert the escalation fired — and, for a read-path panic, that it did
    /// not.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingFailStop {
        calls: Mutex<Vec<WritePathPanic>>,
    }

    impl RecordingFailStop {
        pub(crate) fn calls(&self) -> Vec<WritePathPanic> {
            self.calls.lock().expect("recording sink poisoned").clone()
        }
    }

    impl FailStopSink for RecordingFailStop {
        fn fail_stop(&self, panic: &WritePathPanic) {
            self.calls
                .lock()
                .expect("recording sink poisoned")
                .push(panic.clone());
        }
    }
}
