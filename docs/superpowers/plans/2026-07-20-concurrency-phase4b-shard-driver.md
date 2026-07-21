# Concurrency Phase 4b — Shard-Driver Harness + 8 Targeted Scenarios

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 4b of the concurrency-invariant-testing design (`docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`, "Phasing" item 4b + "Targeted shard-driver scenarios"): an **integration-test** shard-driver harness (under `frogdb-server/crates/core/tests/shard_driver/`) that drives a real `ShardWorker` (and a real `VllCoordinator`) directly with controlled `ShardMessage` ordering, plus proptest-shrinkable permutations for the eight targeted races. Four production fixes land tests-first: F1 (TTL expiry never drains blocked XREADGROUP waiters), F2 (`EXECABRT`→`EXECABORT` spelling), F3 (lazy-expiry WATCH false negative), F4 (builder fake-WAL failure-injection seam). The design brief (`.superpowers/sdd/phase4b/brief.md`, Decisions D1–D8, constraints C1–C7, specs S1–S8) is settled and is the authority; the two research docs (`.superpowers/sdd/phase4b/research-harness-surface.md`, `.superpowers/sdd/phase4b/research-scenario-seams.md`) are ground truth for every signature — if code here contradicts them, the code is wrong.

**Architecture:** The harness lives at `frogdb-server/crates/core/tests/shard_driver/` as an **integration-test** tree (D1, as landed). It is *not* an in-crate `#[cfg(test)]` module: the harness needs the `frogdb-commands` crate to populate a real `CommandRegistry`, and `frogdb-commands` depends on `frogdb-core`, so adding it as a `frogdb-core` dev-dependency forms a dev-dep cycle that compiles `frogdb-core` twice (once as the crate-under-test, once as the copy `frogdb-commands` links). Unit-test code touching both copies trips `E0308` type mismatches. Integration tests do not: they link the single **normal** build of `frogdb-core` that `frogdb-commands` also links, and reach the otherwise-private seams through **feature-gated public wrappers** — the `drive*` methods on `ShardWorker` and `ShardReceiver::try_recv`, each `#[cfg(any(test, feature = "shard-driver"))] #[doc(hidden)] pub`, enabled here by the crate's own `[dev-dependencies]` self-dep (`frogdb-core = { path = ".", features = ["shard-driver", "fake-wal"] }`, mirroring `frogdb-telemetry`'s `features = ["testing"]` precedent). Two driving styles (D3): **Direct** — `driver.dispatch(shard, msg)` → `worker.drive(msg)` — used when the test is the only sender; **Pumped** — a real `VllCoordinator` runs as a spawned task talking through a harness-local `ChannelSink: ShardSink`, and the driver *chooses which shard's queue to service next* (`pump_one` → `worker.try_recv_queued()` + `worker.drive`), making cross-shard interleaving a deterministic function of the driver's service schedule under `#[tokio::test]` current-thread. Ticks (active expiry, waiter timeout, continuation-release) have no `ShardMessage` and are direct wrapper calls (`drive_expiry_tick`, `drive_waiter_timeout_tick`, `drive_continuation_release`) (D2). Probes go through the production DEBUG seam (`GetLockTableInfo` / `GetWaitQueueInfo` / `MemoryCheck` / `ExpiryIndexCheck`), not ad-hoc field peeking; `worker.store` (pub) is read for value-level asserts. The generator (C1–C7) models each logical sender as a small state machine advanced by a proptest-chosen schedule, so illegal message orders are **unrepresentable**, not filtered.

**Tech Stack:** Rust; `bytes::Bytes`; `tokio` (`#[tokio::test]`, `oneshot`, `mpsc`, current-thread); `proptest` (existing frogdb-core dev-dep); `frogdb-vll` (`VllCoordinator`, `ShardSink`, `LockMode`, `ShardReadyResult`); `frogdb-protocol` (`ParsedCommand`, `Response`, `ProtocolVersion`); `frogdb-persistence` (`FakeWalSink`, `FakeFailure`, `FakeWalLog`); `frogdb-server` turmoil sim harness for the two real-path verifications (F1, F3) and S7; `just` + `cargo nextest`.

## Global Constraints

- **Run `pwd` first.** You may be in a git worktree, not the main checkout. Only edit files under the directory `pwd` reports; use absolute paths.
- **Never weaken a checker or invariant to make a test pass.** A red test is signal: fix the code or fix a genuine harness/model gap — never loosen the assertion. New bugs the harness finds beyond F1/F3 require **real-path verification (D8)** — a turmoil or full-server repro against the real connection path — *before* any production fix lands.
- **TDD:** every production-fix task writes a failing test first, watches it fail with the predicted error, then writes the minimal implementation. Harness-only tasks compile-and-run the new tests green.
- **Crate names / build commands** (verified in `Justfile`):
  - `frogdb-core` — `just check frogdb-core`, `just test frogdb-core <pattern>` (expands `<pattern>` to `-E 'test(/<pattern>/)'`), `just lint frogdb-core`, `just fmt frogdb-core`.
  - `frogdb-server` turmoil tests need the `turmoil` feature: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/<pattern>/)'` (also `just concurrency-turmoil <PATTERN>`).
  - If `sccache` errors, re-run prefixed with `RUSTC_WRAPPER=""`.
- **Watchdog rules (CLAUDE.md).** Any build/test expected to take >2 min runs in the **foreground with a 600000ms Bash timeout**, output redirected to a `$TMPDIR` log (`cmd > "$TMPDIR/log" 2>&1`) then `tail` the log — never pipe a long run through `grep`/`tail` (filters buffer and hide progress), never use background `sleep`. **On a timeout, re-run the SAME command** (the `_dyld_start`/`syspolicyd` exec-validation-wedge signatures in CLAUDE.md apply). Commit before launching a long run.
- **lefthook sandbox-denial → retry the identical commit unsandboxed.** If a pre-commit hook fails with a sandbox/permission error (`.vscode` deny, XPC), re-run the exact same `git commit` command unsandboxed.
- **Commit per task** with the exact `git` command shown in the task's final step. **No `Co-Authored-By` lines** in any commit message. Conventional-commit style matching repo history (`test(core):`, `fix(core):`, `docs(spec):`).
- **Ledger dir:** `.superpowers/sdd/phase4b/`.
- **Wire-op discipline:** the VLL/scatter operation carried in messages is `ScatterOp` (`shard/message.rs:714`), NOT `core::command::ScatterGatherOp` (routing-only). Commands are built with `ParsedCommand::new(name: Bytes, args: Vec<Bytes>)` (`protocol/src/command.rs:22`) — no CommandSpec machinery. Do not conflate.
- **Command registry (discovered in Task 1):** `CommandRegistry::new()` is **EMPTY** — real data commands live in the downstream `frogdb-commands` crate (`frogdb_commands::register_all(&mut registry)` is the entry point; the server composes it in `server/src/server/register.rs:6`). A dev-dependency cycle is legal cargo, so **Task 2 adds `frogdb-commands.workspace = true` to `frogdb-server/crates/core/Cargo.toml` `[dev-dependencies]`**. Every harness/test in this plan that dispatches real commands (SET/GET/LPUSH/XADD/XGROUP/DEL/PEXPIRE/…) builds its registry as:
  ```rust
  let mut r = CommandRegistry::new();
  frogdb_commands::register_all(&mut r);
  let registry = Arc::new(r);
  ```
  Server-only commands (AUTH, scripting, search, connection-level MULTI/EXEC) are NOT in `register_all` and are not needed — shard-level `ExecTransaction` bypasses the connection layer. `register_all` is reached from the integration-test crate as a plain dev-dependency (the dev-dep cycle is legal cargo and resolves for integration tests).
- **Harness location:** `frogdb-server/crates/core/tests/shard_driver/` integration-test crate; seams are `#[doc(hidden)] pub` under `cfg(any(test, feature = "shard-driver"))`, enabled for tests by the self-dev-dep (`frogdb-core = { path = ".", features = ["shard-driver", "fake-wal"] }`). Harness code therefore imports from `frogdb_core::…` (not `crate::…`) and calls the `drive*` wrappers rather than the private `dispatch_message`/`run_active_expiry`/`check_waiter_timeouts`/`pump_continuation_release` methods.

---

## Task 1 — Visibility widenings, continuation-release pump helper, pump seam, and `enable_vll` dead-code deletion

**Goal:** Open the minimal set of seams the harness needs (D2), add the continuation-release pump helper and a `try_recv` on `ShardReceiver`, and delete the dead `enable_vll()` builder no-op.

**DONE — landed as `30af07f2` then `8e5362f8`.** The pivot to an integration-test harness changed the seam shape from the `pub(crate)` promotions written below to **feature-gated public wrappers**: rather than promoting `dispatch_message`/`run_active_expiry`/`check_waiter_timeouts`, commit `8e5362f8` added thin `#[cfg(any(test, feature = "shard-driver"))] #[doc(hidden)] pub` wrapper methods on `ShardWorker` — `drive(msg) -> bool` (async), `drive_expiry_tick()`, `drive_waiter_timeout_tick()`, `drive_continuation_release()` (async), `try_recv_queued() -> Option<Envelope>` — plus `ShardReceiver::try_recv` promoted to `pub` under the same cfg, the `shard-driver`/`fake-wal` features and self-dev-dep in `Cargo.toml`, and the compile-check moved to `tests/shard_driver.rs` (`smoke_real_registry_set_get_and_ticks`). The `enable_vll` deletion landed in `30af07f2`. The `pub(crate)` code below is retained as the historical record; the reachable seams are the `drive*` wrappers. All checkboxes complete.

**Files touched:**
- `frogdb-server/crates/core/src/shard/event_loop.rs` — three visibility promotions + one new pump helper + compile-check test module.
- `frogdb-server/crates/core/src/shard/message.rs` — `ShardReceiver::try_recv` (test-only).
- `frogdb-server/crates/core/src/shard/builder.rs` — delete `enable_vll` field, init, method, doc line.

**`enable_vll` call-site check (done during drafting; re-run to confirm before deleting):** `grep -rn "\.enable_vll()" frogdb-server` returns **zero** call sites — the only references are the builder field (`builder.rs:118`), its init (`:151`), the method (`:266-269`), and the doc-example line (`:92`). `try_build` never reads `self.enable_vll` (VLL state is unconditionally `ShardVll::default()`, builder.rs:445; `handle_vll_*` always live). The `pub enable_vll: bool` field on the *separate* `ShardConfig` struct (types.rs:703) is out of scope — do not touch it.

- [x] **Step 1: Promote the three seams to `pub(crate)`.** In `frogdb-server/crates/core/src/shard/event_loop.rs`:
  - Line 217: `async fn dispatch_message(&mut self, msg: ShardMessage) -> bool` → `pub(crate) async fn dispatch_message(&mut self, msg: ShardMessage) -> bool`.
  - Line 123: `fn run_active_expiry(&mut self)` → `pub(crate) fn run_active_expiry(&mut self)`.
  - Line 151: `fn apply_expiry_effects(&mut self, result: ExpiryResult)` → `pub(crate) fn apply_expiry_effects(&mut self, result: ExpiryResult)`.

- [x] **Step 2: Add the continuation-release pump helper.** In `event_loop.rs`, inside `impl ShardWorker` (after `dispatch_message`, before the closing `}` of the impl block at line 305), add the smallest `pub(crate)` seam mirroring the event-loop's continuation-release arm (event_loop.rs:88-91):

```rust
    /// Test/driver seam mirroring the event loop's continuation-release arm
    /// (`event_loop.rs:88-91`): await the stored release signal — fired when
    /// the coordinator's `ContinuationGuard` drops — then clear the lock.
    ///
    /// Only call when a continuation lock is held and its guard has been (or is
    /// about to be) dropped; with no lock held `await_continuation_release`
    /// resolves to `pending()` and this future never completes. The shard-driver
    /// harness pumps this per shard, in a permuted order, after inducing the
    /// guard drop (scenario 4).
    #[cfg(test)]
    pub(crate) async fn pump_continuation_release(&mut self) {
        self.vll.await_continuation_release().await;
        self.vll.clear_continuation_lock();
    }
```

- [x] **Step 3: Add the test-only non-blocking receive seam.** In `frogdb-server/crates/core/src/shard/message.rs`, inside `impl ShardReceiver` (after `is_empty`, before the closing `}` at line 101), add:

```rust
    /// Non-blocking receive of the next buffered envelope, if any.
    ///
    /// Test seam for the shard-driver harness's deterministic pump loop: under
    /// a current-thread runtime a message already delivered by a spawned
    /// coordinator task is buffered and returned without yielding, so pumping
    /// one queued message never lets another task interleave mid-pump.
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Option<Envelope> {
        self.inner.try_recv().ok()
    }
```

- [x] **Step 4: Delete the `enable_vll` dead code.** In `frogdb-server/crates/core/src/shard/builder.rs`:
  - Delete field `enable_vll: bool,` (line 118).
  - Delete init `enable_vll: false,` (line 151).
  - Delete the method (lines 265-269):

```rust
    /// Enable VLL (Virtual Lock Loom) for transaction coordination.
    pub fn enable_vll(mut self) -> Self {
        self.enable_vll = true;
        self
    }
```

  - In the doc-comment example (around line 92), delete the `///     .enable_vll()` line so the example still compiles conceptually.

- [x] **Step 5: Add the compile-check test.** At the bottom of `event_loop.rs`, the `#[cfg(test)] mod effect_tests` block already exists (line 307). Add a second small test module below it (after the closing `}` of `effect_tests`) that proves the promoted seams are callable from an in-crate test:

```rust
#[cfg(test)]
mod seam_reachability_tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use crate::registry::CommandRegistry;
    use crate::shard::builder::ShardWorkerBuilder;
    use crate::shard::connection::NewConnection;
    use crate::shard::message::{Envelope, ShardMessage, ShardReceiver, ShardSender};
    use crate::shard::worker::ShardWorker;

    fn worker() -> ShardWorker {
        let (_mtx, mrx) = mpsc::channel::<Envelope>(8);
        let (_ntx, nrx) = mpsc::channel::<NewConnection>(8);
        let (msg_tx, _msg_rx) = mpsc::channel::<Envelope>(8);
        ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(mrx))
            .with_new_conn_rx(nrx)
            .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
            .with_registry(Arc::new(CommandRegistry::new()))
            .build()
    }

    #[tokio::test]
    async fn promoted_seams_are_reachable_in_crate() {
        let mut w = worker();

        // `dispatch_message` (now pub(crate)) round-trips a SET then a GET.
        let (tx, rx) = oneshot::channel();
        let set = ShardMessage::Execute {
            command: Arc::new(ParsedCommand::new(
                Bytes::from_static(b"SET"),
                vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
            )),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        assert!(!w.dispatch_message(set).await, "SET must not signal shutdown");
        assert!(matches!(rx.await.unwrap(), Response::Simple(_)));

        let (tx, rx) = oneshot::channel();
        let get = ShardMessage::Execute {
            command: Arc::new(ParsedCommand::new(
                Bytes::from_static(b"GET"),
                vec![Bytes::from_static(b"k")],
            )),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        w.dispatch_message(get).await;
        assert_eq!(rx.await.unwrap(), Response::Bulk(Some(Bytes::from_static(b"v"))));

        // Tick seams (now pub(crate)) run without a timer.
        w.run_active_expiry();
        w.check_waiter_timeouts();
    }
}
```

- [x] **Step 6: Run tests.** `just test frogdb-core seam_reachability_tests` — expected PASS. Then `just check frogdb-core` — expected clean (no dead-code warning from the removed `enable_vll`).

- [x] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/shard/event_loop.rs frogdb-server/crates/core/src/shard/message.rs frogdb-server/crates/core/src/shard/builder.rs
git commit -m "test(core): open pub(crate) shard-driver seams; delete dead enable_vll builder no-op"
```

---

## Task 2 — F4: builder fake-WAL failure-injection seam (`with_fake_wal_failure`)

**Goal:** The builder's `WalMode::Fake` arm hardcodes a non-failing `FakeWalSink::new(shard_id)` (builder.rs:387). Add a `with_fake_wal_failure(FakeFailure)` setter, gated by the existing `cfg(any(test, feature = "fake-wal"))` convention, threaded into the fake-sink construction so S6 can inject `FakeFailure::AtWriteIndex(n)` / `Predicate`. Failing test first. **This task must precede S6 (Task 13).**

**DONE — landed as `de81ddb3`.** The setter, field, init, and `try_build` threading landed in `builder.rs` exactly as written below. The failing test landed in the integration harness at `frogdb-server/crates/core/tests/shard_driver.rs` as `fake_wal_failure_is_injected_at_index` (not the in-crate `mod fake_wal_tests` shown below), driving the real `SET` through the public `worker.drive(set)` seam and building the registry via `frogdb_commands::register_all`. All checkboxes complete.

**Files touched:**
- `frogdb-server/crates/core/src/shard/builder.rs`
- `frogdb-server/crates/core/Cargo.toml` — add `frogdb-commands.workspace = true` to `[dev-dependencies]` (see the Command-registry Global Constraint; dev-dep cycles are legal cargo; if `frogdb-commands` is missing from the root `[workspace.dependencies]` table, add it there the same way sibling crates are listed).

**Verified facts:** `FakeFailure` / `FakeWalSink` are reachable in-crate as `crate::persistence::FakeFailure` / `crate::persistence::FakeWalSink` (`frogdb-server/crates/core/src/persistence/mod.rs:13` `pub use frogdb_persistence::*;`; `frogdb-persistence/src/lib.rs:34` exports both). `FakeWalSink::with_failure(shard_id, failure)` exists (`fake.rs:128`). `FakeWalRegistry::install(shard_id, log)` registers the log (fake_wal_registry.rs:22).

- [x] **Step 1: Write the failing test.** In `frogdb-server/crates/core/src/shard/builder.rs`, in the existing `#[cfg(test)] mod fake_wal_tests` block (line 483), add:

```rust
    #[tokio::test]
    async fn fake_wal_failure_is_injected_at_index() {
        FakeWalRegistry::clear();
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_conn_tx, conn_rx) = mpsc::channel(16);
        let mut registry = CommandRegistry::new();
        frogdb_commands::register_all(&mut registry);
        let mut worker = ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(msg_rx))
            .with_new_conn_rx(conn_rx)
            .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
            .with_registry(Arc::new(registry))
            .with_metrics(Arc::new(NoopMetricsRecorder::new()))
            .with_wal_mode(WalMode::Fake)
            // Fail the FIRST write (index 0): a single SET must not persist.
            .with_fake_wal_failure(crate::persistence::FakeFailure::AtWriteIndex(0))
            .build();
        assert!(worker.persistence.has_wal());

        // Drive one SET through the real dispatch path; the injected WAL
        // failure means the write is not recorded in the fake log.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let set = crate::shard::message::ShardMessage::Execute {
            command: Arc::new(frogdb_protocol::ParsedCommand::new(
                bytes::Bytes::from_static(b"SET"),
                vec![
                    bytes::Bytes::from_static(b"k"),
                    bytes::Bytes::from_static(b"v"),
                ],
            )),
            conn_id: 1,
            txid: None,
            protocol_version: frogdb_protocol::ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        worker.dispatch_message(set).await;
        let _ = rx.await;

        let log = FakeWalRegistry::log(0).expect("fake sink log registered");
        assert!(
            log.effects().iter().all(|e| e.key.as_deref() != Some(&b"k"[..])),
            "the injected index-0 WAL failure must suppress the SET's recorded write"
        );
    }
```

- [x] **Step 2: Run to verify it fails.** `just test frogdb-core fake_wal_failure_is_injected` — expected FAIL: `no method named with_fake_wal_failure`.

- [x] **Step 3: Add the builder field.** In `builder.rs`, in the `ShardWorkerBuilder` struct (after `wal_failure_policy: Option<Arc<AtomicU8>>,`, line 121), add a test/fake-gated field:

```rust
    #[cfg(any(test, feature = "fake-wal"))]
    fake_wal_failure: crate::persistence::FakeFailure,
```

In `ShardWorkerBuilder::new` (the struct literal at lines 128-156), add the init (after `wal_failure_policy: None,`):

```rust
            #[cfg(any(test, feature = "fake-wal"))]
            fake_wal_failure: crate::persistence::FakeFailure::None,
```

- [x] **Step 4: Add the setter.** After `with_wal_failure_policy` (line 293), add:

```rust
    /// Inject a fake-WAL write failure (test / `fake-wal` only). Only takes
    /// effect together with [`WalMode::Fake`]. Enables the persist-failure /
    /// rollback (`EXECABORT`) branch to be exercised deterministically.
    #[cfg(any(test, feature = "fake-wal"))]
    pub fn with_fake_wal_failure(mut self, failure: crate::persistence::FakeFailure) -> Self {
        self.fake_wal_failure = failure;
        self
    }
```

- [x] **Step 5: Thread it into the fake-sink construction.** In `try_build`, replace the `WalMode::Fake` construction arm (builder.rs:385-390) so the sink is built with the injected failure. The `#[cfg(any(test, feature = "fake-wal"))]` block becomes:

```rust
                #[cfg(any(test, feature = "fake-wal"))]
                {
                    let sink = crate::persistence::FakeWalSink::with_failure(
                        shard_id,
                        self.fake_wal_failure.clone(),
                    );
                    crate::shard::fake_wal_registry::FakeWalRegistry::install(shard_id, sink.log());
                    Some(Box::new(sink) as Box<dyn WalSink>)
                }
```

(The `#[cfg(not(any(test, feature = "fake-wal")))] { None }` arm is unchanged. Note `self.fake_wal_failure` is moved-from in `try_build` which takes `self` by value — `.clone()` keeps `FakeFailure: Clone`, fake.rs:56.)

- [x] **Step 6: Run to verify green.** `just test frogdb-core fake_wal` — expected PASS (new test + pre-existing `fake_wal_mode_builds_a_recording_sink`).

- [x] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/shard/builder.rs
git commit -m "test(core): add ShardWorkerBuilder::with_fake_wal_failure fake-WAL injection seam"
```

---

## Task 3 — Driver core: `tests/shard_driver/` harness tree + `ShardDriver` (direct dispatch, ticks, probes)

**Goal:** Create the integration-test harness tree with the `ShardDriver` struct: N `ShardWorker`s built via `ShardWorkerBuilder` with a real length-N `Arc<Vec<ShardSender>>`, direct-dispatch helpers (`execute`, `exec_transaction`, `get_version`, `block_wait`), tick helpers, `pump_one`, and probe helpers via the DEBUG messages. Unit-test the driver itself.

**Files touched:**
- Create: `frogdb-server/crates/core/tests/shard_driver/harness.rs`
- Modify: `frogdb-server/crates/core/tests/shard_driver.rs` — declare the harness + scenario modules. This file is the single integration-test target for the whole harness (`cargo` compiles `tests/shard_driver.rs` as one crate; every file under `tests/shard_driver/` is one of its modules). The two smoke/F4 tests landed by Tasks 1–2 already live here and stay.

- [ ] **Step 1: Declare the modules.** In `frogdb-server/crates/core/tests/shard_driver.rs`, add the module declarations. Because the harness files live *flat* under `tests/shard_driver/`, every module is declared at the crate root here as a sibling (declaring `mod sink;` inside `harness.rs` would resolve to `tests/shard_driver/harness/sink.rs`, the wrong path):

```rust
mod harness;
mod sink;
mod generator;

// Scenario submodules (one per targeted scenario; S7 is turmoil-level, server crate).
mod scenario_s1;
mod scenario_s2;
mod scenario_s3;
mod scenario_s4;
mod scenario_s5;
mod scenario_s6;
mod scenario_s8;
```

- [ ] **Step 2: Write the driver core.** Create `frogdb-server/crates/core/tests/shard_driver/harness.rs`:

```rust
//! Shard-driver harness core (Phase 4b), integration-test tree.
//!
//! Drives a real [`ShardWorker`] (and, in pumped mode, a real
//! `VllCoordinator`) directly with controlled `ShardMessage` ordering. Lives
//! under `crates/core/tests/shard_driver/` rather than as an in-crate
//! `#[cfg(test)]` module: the harness populates a real `CommandRegistry` via the
//! `frogdb-commands` dev-dependency, whose dep on `frogdb-core` forms a dev-dep
//! cycle that compiles `frogdb-core` twice — unit-test code touching both copies
//! trips E0308. Integration tests link the single normal build and reach the
//! seams through the feature-gated public `drive*` wrappers (`shard-driver`
//! feature, enabled by the self-dev-dep) (brief D1).
//!
//! Two driving styles (brief D3):
//! - **Direct**: [`ShardDriver::dispatch`] → `worker.drive` — used when the test
//!   is the only sender.
//! - **Pumped**: a real coordinator task talks through [`crate::sink::ChannelSink`];
//!   the driver chooses which shard's queue to service next via
//!   [`ShardDriver::pump_one`] (`worker.try_recv_queued()` + `worker.drive`).
//!   Under a current-thread runtime this makes cross-shard interleaving a
//!   deterministic function of the service schedule.

#![allow(dead_code)] // harness surface is used piecemeal across scenario modules

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use frogdb_core::shard::types::{
    ExpiryIndexCheckInfo, LockTableInfo, MemoryCheckInfo, TransactionResult, WaitQueueInfo,
};
use frogdb_core::shard::{
    Envelope, NewConnection, ShardMessage, ShardReceiver, ShardSender, ShardWorkerBuilder,
};
use frogdb_core::types::BlockingOp;
use frogdb_core::{CommandRegistry, ShardWorker};

/// Owns N real shard workers plus the sender side of each shard's queue.
pub struct ShardDriver {
    workers: Vec<ShardWorker>,
    /// The length-N sender vector every worker shares (cross-shard routing).
    senders: Arc<Vec<ShardSender>>,
    /// Held open so shard queues never close under the workers.
    _conn_txs: Vec<mpsc::Sender<NewConnection>>,
}

impl ShardDriver {
    /// Build `n` shards with ids `0..n`, a shared registry, and a real
    /// length-`n` sender vector so cross-shard routing and keyspace-notify
    /// forwarding work.
    pub fn new(n: usize) -> Self {
        // Real data commands (Command-registry Global Constraint): the empty
        // registry has nothing to dispatch.
        let mut r = CommandRegistry::new();
        frogdb_commands::register_all(&mut r);
        let registry = Arc::new(r);

        // One (tx, rx) queue per shard; senders indexed by shard id.
        let mut msg_rxs: Vec<ShardReceiver> = Vec::with_capacity(n);
        let mut senders: Vec<ShardSender> = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<Envelope>(1024);
            senders.push(ShardSender::new(tx));
            msg_rxs.push(ShardReceiver::new(rx));
        }
        let senders = Arc::new(senders);

        let mut workers = Vec::with_capacity(n);
        let mut conn_txs = Vec::with_capacity(n);
        for (shard_id, msg_rx) in msg_rxs.into_iter().enumerate() {
            let (conn_tx, conn_rx) = mpsc::channel::<NewConnection>(16);
            conn_txs.push(conn_tx);
            let worker = ShardWorkerBuilder::new(shard_id, n)
                .with_message_rx(msg_rx)
                .with_new_conn_rx(conn_rx)
                .with_shard_senders(senders.clone())
                .with_registry(registry.clone())
                .build();
            workers.push(worker);
        }

        Self {
            workers,
            senders,
            _conn_txs: conn_txs,
        }
    }

    /// Cloned handle to a shard's sender (for wiring a coordinator sink).
    pub fn senders(&self) -> Arc<Vec<ShardSender>> {
        self.senders.clone()
    }

    /// Mutable access to a worker's store for value-level asserts.
    pub fn worker(&mut self, shard: usize) -> &mut ShardWorker {
        &mut self.workers[shard]
    }

    // --- Direct dispatch (test is the sole sender) ----------------------

    /// Dispatch a raw message directly to a shard, bypassing its queue.
    pub async fn dispatch(&mut self, shard: usize, msg: ShardMessage) -> bool {
        self.workers[shard].drive(msg).await
    }

    /// Run one command and await its reply.
    pub async fn execute(&mut self, shard: usize, name: &str, args: &[&str]) -> Response {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::Execute {
            command: Arc::new(cmd(name, args)),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        self.dispatch(shard, msg).await;
        rx.await.expect("execute response")
    }

    /// Run a command attributed to a specific connection.
    pub async fn execute_conn(
        &mut self,
        shard: usize,
        conn_id: u64,
        name: &str,
        args: &[&str],
    ) -> Response {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::Execute {
            command: Arc::new(cmd(name, args)),
            conn_id,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        self.dispatch(shard, msg).await;
        rx.await.expect("execute response")
    }

    /// Read a shard's WATCH version.
    pub async fn get_version(&mut self, shard: usize) -> u64 {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::GetVersion { response_tx: tx })
            .await;
        rx.await.expect("version")
    }

    /// Run a transaction with the given watches; returns the result.
    pub async fn exec_transaction(
        &mut self,
        shard: usize,
        conn_id: u64,
        commands: Vec<ParsedCommand>,
        watches: Vec<(Bytes, u64)>,
    ) -> TransactionResult {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::ExecTransaction {
            commands,
            watches,
            conn_id,
            protocol_version: ProtocolVersion::Resp3,
            response_tx: tx,
        };
        self.dispatch(shard, msg).await;
        rx.await.expect("transaction result")
    }

    /// Register a blocking waiter; the returned receiver resolves when the
    /// waiter is satisfied, drained, or timed out by the shard.
    pub async fn block_wait(
        &mut self,
        shard: usize,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        deadline: Option<Instant>,
    ) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::BlockWait {
            conn_id,
            keys,
            op,
            response_tx: tx,
            deadline,
            protocol_version: ProtocolVersion::Resp3,
        };
        self.dispatch(shard, msg).await;
        rx
    }

    /// Fire-and-forget waiter cleanup (connection gave up).
    pub async fn unregister_wait(&mut self, shard: usize, conn_id: u64) {
        self.dispatch(shard, ShardMessage::UnregisterWait { conn_id })
            .await;
    }

    // --- Ticks (timer-only work; no ShardMessage exists) ----------------

    pub fn tick_expiry(&mut self, shard: usize) {
        self.workers[shard].drive_expiry_tick();
    }

    pub fn tick_waiter_timeout(&mut self, shard: usize) {
        self.workers[shard].drive_waiter_timeout_tick();
    }

    pub async fn pump_continuation_release(&mut self, shard: usize) {
        self.workers[shard].drive_continuation_release().await;
    }

    // --- Pumped mode ----------------------------------------------------

    /// Service one buffered message on a shard's queue (recv + dispatch).
    /// Returns `true` if a message was serviced. Non-blocking: does nothing
    /// if the queue is empty.
    pub async fn pump_one(&mut self, shard: usize) -> bool {
        if let Some(env) = self.workers[shard].try_recv_queued() {
            self.workers[shard].drive(env.message).await;
            true
        } else {
            false
        }
    }

    /// Drain every buffered message on a shard until its queue is empty.
    pub async fn drain(&mut self, shard: usize) {
        while self.pump_one(shard).await {}
    }

    // --- Probes (production DEBUG seam) ----------------------------------

    pub async fn wait_queue_info(&mut self, shard: usize) -> WaitQueueInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::GetWaitQueueInfo { response_tx: tx })
            .await;
        rx.await.expect("wait queue info")
    }

    pub async fn lock_table_info(&mut self, shard: usize) -> LockTableInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::GetLockTableInfo { response_tx: tx })
            .await;
        rx.await.expect("lock table info")
    }

    pub async fn memory_check(&mut self, shard: usize) -> MemoryCheckInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::MemoryCheck { response_tx: tx })
            .await;
        rx.await.expect("memory check")
    }

    pub async fn expiry_index_check(&mut self, shard: usize) -> ExpiryIndexCheckInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::ExpiryIndexCheck { response_tx: tx })
            .await;
        rx.await.expect("expiry index check")
    }
}

/// Build a `ParsedCommand` from `&str`s.
pub fn cmd(name: &str, args: &[&str]) -> ParsedCommand {
    ParsedCommand::new(
        Bytes::from(name.to_string()),
        args.iter().map(|a| Bytes::from(a.to_string())).collect(),
    )
}

mod driver_tests {
    use super::*;

    #[tokio::test]
    async fn set_get_round_trip_and_empty_wait_queue() {
        let mut d = ShardDriver::new(1);
        assert!(matches!(d.execute(0, "SET", &["k", "v"]).await, Response::Simple(_)));
        assert_eq!(
            d.execute(0, "GET", &["k"]).await,
            Response::Bulk(Some(Bytes::from_static(b"v")))
        );
        let wq = d.wait_queue_info(0).await;
        assert_eq!(wq.total_waiters, 0, "no waiters after a plain SET/GET");
        assert_eq!(wq.shard_id, 0);
    }

    #[tokio::test]
    async fn probes_report_clean_idle_shard() {
        let mut d = ShardDriver::new(2);
        let lt = d.lock_table_info(0).await;
        assert!(lt.intents.is_empty());
        assert!(lt.continuation_lock.is_none());
        let mem = d.memory_check(1).await;
        assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
        assert!(d.expiry_index_check(0).await.anomalies.is_empty());
    }
}
```

**Note for the implementer:** the seven scenario submodules (`scenario_s1`..`scenario_s8`, minus `s7`) are declared in `tests/shard_driver.rs` (Step 1) but created in Tasks 6–13/15; `generator` and `sink` land in Tasks 5 and 4. Create all of them as one-line stub files in this task (Step 3 below) so the module tree compiles; each later task replaces its stub.

- [ ] **Step 3: Create the stub files** so the module tree compiles:

```bash
mkdir -p frogdb-server/crates/core/tests/shard_driver
printf '//! placeholder — filled in Task 4\n' > frogdb-server/crates/core/tests/shard_driver/sink.rs
printf '//! placeholder — filled in Task 5\n' > frogdb-server/crates/core/tests/shard_driver/generator.rs
for s in s1 s2 s3 s4 s5 s6 s8; do printf '//! placeholder — filled in its scenario task\n' > "frogdb-server/crates/core/tests/shard_driver/scenario_${s}.rs"; done
```

- [ ] **Step 4: Run tests.** `just test frogdb-core driver_tests` — expected PASS.

- [ ] **Step 5: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver.rs frogdb-server/crates/core/tests/shard_driver
git commit -m "test(core): shard-driver harness core (ShardDriver: dispatch, ticks, pump, probes)"
```

---

## Task 4 — `ChannelSink` + failure-injecting wrapper sink; pumped VLL round-trip test

**Goal:** Reimplement the server-private `ShardSenderSink` inside the harness as `ChannelSink: ShardSink` (`type Operation = ScatterOp; type Response = PartialResult;`, template `server/src/vll_adapter.rs:61-161`) — neither it nor vll's `TestSink` is reachable from core. Add a `FaultSink` wrapper that scripts per-shard lock/execute failure (service-withholding stays driver-side). Unit test: a real `VllCoordinator` scatter (MSET) round-trips over 2 pumped shards.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/sink.rs` (replace stub).

- [ ] **Step 1: Write the sink module.** Replace `frogdb-server/crates/core/tests/shard_driver/sink.rs`:

```rust
//! Harness-local [`ShardSink`] over a `Vec<ShardSender>` (template
//! `server/src/vll_adapter.rs:61-161`), plus a failure-injecting wrapper. The
//! server's `ShardSenderSink` and vll's `TestSink` are both unreachable from
//! the integration-test crate, so the harness supplies its own.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use frogdb_vll::{LockMode, ShardReadyResult, ShardSink, ShardSinkError};
use tokio::sync::oneshot;

use frogdb_core::shard::types::PartialResult;
use frogdb_core::shard::{ScatterOp, ShardMessage, ShardSender};

/// Plain sink: maps `ShardSink` calls onto `ShardMessage::Vll*` sends into the
/// per-shard queues. The driver services those queues via `pump_one`.
pub struct ChannelSink {
    senders: Arc<Vec<ShardSender>>,
}

impl ChannelSink {
    pub fn new(senders: Arc<Vec<ShardSender>>) -> Self {
        Self { senders }
    }
}

impl ShardSink for ChannelSink {
    type Operation = ScatterOp;
    type Response = PartialResult;

    async fn send_lock_request(
        &self,
        shard_id: usize,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: Self::Operation,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) -> Result<(), ShardSinkError> {
        self.senders[shard_id]
            .send(ShardMessage::VllLockRequest {
                txid,
                keys,
                mode,
                operation,
                ready_tx,
            })
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }

    async fn send_execute(
        &self,
        shard_id: usize,
        txid: u64,
        response_tx: oneshot::Sender<Self::Response>,
    ) -> Result<(), ShardSinkError> {
        self.senders[shard_id]
            .send(ShardMessage::VllExecute { txid, response_tx })
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }

    async fn send_abort(&self, shard_id: usize, txid: u64) {
        let _ = self.senders[shard_id]
            .send(ShardMessage::VllAbort { txid })
            .await;
    }

    async fn send_continuation_lock(
        &self,
        shard_id: usize,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) -> Result<(), ShardSinkError> {
        self.senders[shard_id]
            .send(ShardMessage::VllContinuationLock {
                txid,
                conn_id,
                ready_tx,
                release_rx,
            })
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }
}

/// Failure-injecting wrapper: fail the lock-request or execute dispatch for a
/// chosen set of shards (phase-2 / phase-3 failure). Service-withholding —
/// forcing a `LockTimeout` — is driver-side (the driver simply does not
/// `pump_one` that shard), so it is not modeled here.
pub struct FaultSink {
    inner: ChannelSink,
    /// Shards whose `send_lock_request` returns an error (phase-2 dispatch
    /// failure → `ScatterError::ShardUnavailable`).
    fail_lock: HashSet<usize>,
    /// Shards whose `send_execute` returns an error (phase-3 dispatch failure).
    fail_execute: HashSet<usize>,
}

impl FaultSink {
    pub fn new(
        senders: Arc<Vec<ShardSender>>,
        fail_lock: HashSet<usize>,
        fail_execute: HashSet<usize>,
    ) -> Self {
        Self {
            inner: ChannelSink::new(senders),
            fail_lock,
            fail_execute,
        }
    }
}

impl ShardSink for FaultSink {
    type Operation = ScatterOp;
    type Response = PartialResult;

    async fn send_lock_request(
        &self,
        shard_id: usize,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: Self::Operation,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) -> Result<(), ShardSinkError> {
        if self.fail_lock.contains(&shard_id) {
            return Err(ShardSinkError {
                shard_id,
                reason: "injected lock dispatch failure",
            });
        }
        self.inner
            .send_lock_request(shard_id, txid, keys, mode, operation, ready_tx)
            .await
    }

    async fn send_execute(
        &self,
        shard_id: usize,
        txid: u64,
        response_tx: oneshot::Sender<Self::Response>,
    ) -> Result<(), ShardSinkError> {
        if self.fail_execute.contains(&shard_id) {
            return Err(ShardSinkError {
                shard_id,
                reason: "injected execute dispatch failure",
            });
        }
        self.inner.send_execute(shard_id, txid, response_tx).await
    }

    async fn send_abort(&self, shard_id: usize, txid: u64) {
        self.inner.send_abort(shard_id, txid).await;
    }

    async fn send_continuation_lock(
        &self,
        shard_id: usize,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) -> Result<(), ShardSinkError> {
        self.inner
            .send_continuation_lock(shard_id, txid, conn_id, ready_tx, release_rx)
            .await
    }
}

mod sink_tests {
    use std::time::Duration;

    use bytes::Bytes;
    use frogdb_vll::{LockMode, NoopMetricsSink, ScatterParticipant, ScatterRequest, VllCoordinator};

    use super::*;
    use crate::harness::ShardDriver;
    use frogdb_core::shard::ScatterOp;

    #[tokio::test]
    async fn real_coordinator_scatter_mset_over_two_pumped_shards() {
        // Two shards, real VllCoordinator, ChannelSink. The coordinator runs as
        // a spawned task; the driver services each shard's queue deterministically.
        let mut driver = ShardDriver::new(2);
        let senders = driver.senders();

        let coordinator = Arc::new(VllCoordinator::new(
            ChannelSink::new(senders),
            NoopMetricsSink,
        ));

        // MSET {shard0-key: a} on shard 0, {shard1-key: b} on shard 1.
        let request = ScatterRequest {
            txid: 1,
            mode: LockMode::Write,
            participants: vec![
                ScatterParticipant {
                    shard_id: 0,
                    keys: vec![Bytes::from_static(b"k0")],
                    operation: ScatterOp::MSet {
                        pairs: vec![(Bytes::from_static(b"k0"), Bytes::from_static(b"a"))],
                    },
                },
                ScatterParticipant {
                    shard_id: 1,
                    keys: vec![Bytes::from_static(b"k1")],
                    operation: ScatterOp::MSet {
                        pairs: vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"b"))],
                    },
                },
            ],
            timeout: Duration::from_secs(5),
            command: "MSET",
        };

        let coord = coordinator.clone();
        let handle = tokio::spawn(async move { coord.scatter(request).await });

        // Phase 1: each shard gets a VllLockRequest → pump both so they reply Ready.
        // Loop until the coordinator finishes, servicing whatever is queued.
        loop {
            let a = driver.pump_one(0).await;
            let b = driver.pump_one(1).await;
            if handle.is_finished() && !a && !b {
                break;
            }
            tokio::task::yield_now().await;
        }

        let outcome = handle.await.unwrap().expect("scatter must succeed");
        assert_eq!(outcome.responses.len(), 2);

        // Both writes landed on their real shards.
        assert_eq!(
            driver.execute(0, "GET", &["k0"]).await,
            Response::Bulk(Some(Bytes::from_static(b"a")))
        );
        assert_eq!(
            driver.execute(1, "GET", &["k1"]).await,
            Response::Bulk(Some(Bytes::from_static(b"b")))
        );

        // Lock tables clean afterward.
        assert!(driver.lock_table_info(0).await.intents.is_empty());
        assert!(driver.lock_table_info(1).await.intents.is_empty());
    }
}
```

**Note (import):** the test uses `Response` — add `use frogdb_protocol::Response;` to the `sink_tests` module if not already brought in via `use super::*`; here `super::*` does not re-export `Response`, so add it explicitly.

- [ ] **Step 2: Run tests.** `just test frogdb-core real_coordinator_scatter_mset` — expected PASS. If it hangs, the pump loop is starving the coordinator: confirm `tokio::task::yield_now().await` is present so the spawned coordinator task advances between pumps (current-thread runtime).

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/sink.rs
git commit -m "test(core): harness ChannelSink + FaultSink; real VllCoordinator scatter round-trip"
```

---

## Task 5 — Sender state machines + proptest schedule generator (C1–C7 structural)

**Goal:** The heart of the harness. Model each logical sender as a small state machine advanced one step at a time by a proptest-chosen **schedule** (a choice sequence), so illegal message orders (violating C1–C7) are **unrepresentable**, not filtered. Ticks (C4) may sit between any two dispatches. Shrinking reduces both the schedule and the programs.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/generator.rs` (replace stub).

**Constraint mapping (documented in the module):**
- **C1 (per-sender FIFO):** a sender only ever advances to its *own next* step; the schedule cannot reorder within a sender.
- **C2 (request-response gating):** a gating step (`Execute`, `ExecTransaction`, `GetVersion`) `.await`s its reply inside `advance`, so a sender's step *n+1* cannot begin before step *n*'s response resolves.
- **C3 (UnregisterWait legality):** encoded as an optional trailing step on a blocking sender — only representable *after* its `BlockWait` step, and only when the program was constructed with `may_unregister = true` (Timeout/Unblocked histories).
- **C4 (ticks between dispatches only):** `Choice::Tick` is a distinct choice variant; a tick is a direct method call between whole dispatches, never mid-dispatch (single-task loop).
- **C5/C6/C7:** enforced by construction in pumped/VLL and WATCH scenarios (the real coordinator / response-gated `GetVersion`→`ExecTransaction` ordering), documented at the scenario call sites.

- [ ] **Step 1: Write the generator.** Replace `frogdb-server/crates/core/tests/shard_driver/generator.rs`:

```rust
//! Schedule generator enforcing the permutation-constraint model (brief
//! C1–C7) structurally: illegal message orders are unrepresentable.
//!
//! A test defines a fixed set of **sender programs** (each a `Vec<Step>`) plus a
//! proptest-chosen **schedule** — a bounded sequence of `Choice`s. Replaying
//! the schedule advances senders one step at a time (C1: a sender only advances
//! its own next step; C2: a gating step awaits its reply before completing) and
//! fires per-shard ticks between dispatches (C4). A `Choice::Advance(s)` for a
//! finished sender is a no-op, so shrinking (which removes/relabels choices)
//! can never synthesize an out-of-order history.

use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::ParsedCommand;
use proptest::prelude::*;

use crate::harness::ShardDriver;
use frogdb_core::types::BlockingOp;

/// A per-shard tick pseudo-event (C4). Ticks never preempt a dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tick {
    Expiry,
    WaiterTimeout,
    ContinuationRelease,
}

/// One scheduling choice: advance sender `s`, or fire a tick on `shard`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Advance(usize),
    Tick { shard: usize, tick: Tick },
}

/// One step in a logical sender's program.
#[derive(Debug, Clone)]
pub enum Step {
    /// Gated single command on `shard` (awaits its reply — C2).
    Execute {
        shard: usize,
        conn_id: u64,
        command: ParsedCommand,
    },
    /// Gated transaction on `shard` with resolved watches (C6: the versions are
    /// whatever the sender's earlier `GetVersion` read).
    ExecTransaction {
        shard: usize,
        conn_id: u64,
        commands: Vec<ParsedCommand>,
        watches: Vec<(Bytes, u64)>,
    },
    /// Register a blocking waiter (C2 exempt: no immediate reply). The receiver
    /// is dropped by the harness; scenarios that need the reply build the
    /// `BlockWait` directly via the driver.
    BlockWait {
        shard: usize,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        deadline: Option<Instant>,
    },
    /// Fire-and-forget waiter cleanup (C3): only legal after this conn's
    /// `BlockWait`, only in Timeout/Unblocked histories.
    UnregisterWait { shard: usize, conn_id: u64 },
}

/// A logical sender: an ordered program plus a cursor (C1).
#[derive(Debug, Clone)]
pub struct Sender {
    pub program: Vec<Step>,
    pub cursor: usize,
}

impl Sender {
    pub fn new(program: Vec<Step>) -> Self {
        Self { program, cursor: 0 }
    }
    pub fn finished(&self) -> bool {
        self.cursor >= self.program.len()
    }
}

/// Advance sender `idx` by exactly one step against the driver, awaiting any
/// gating reply (C2). No-op if the sender is finished.
pub async fn advance(driver: &mut ShardDriver, senders: &mut [Sender], idx: usize) {
    if idx >= senders.len() || senders[idx].finished() {
        return;
    }
    let step = senders[idx].program[senders[idx].cursor].clone();
    senders[idx].cursor += 1;
    match step {
        Step::Execute {
            shard,
            conn_id,
            command,
        } => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let msg = frogdb_core::shard::ShardMessage::Execute {
                command: std::sync::Arc::new(command),
                conn_id,
                txid: None,
                protocol_version: frogdb_protocol::ProtocolVersion::Resp3,
                track_reads: false,
                no_touch: false,
                response_tx: tx,
            };
            driver.dispatch(shard, msg).await;
            let _ = rx.await; // C2: gate on the reply
        }
        Step::ExecTransaction {
            shard,
            conn_id,
            commands,
            watches,
        } => {
            let _ = driver
                .exec_transaction(shard, conn_id, commands, watches)
                .await;
        }
        Step::BlockWait {
            shard,
            conn_id,
            keys,
            op,
            deadline,
        } => {
            // Receiver dropped here; the wait entry lives in the queue. Its
            // reply (satisfaction / drain / timeout) is observed via probes and
            // store reads, or captured directly when a scenario needs it.
            let _rx = driver.block_wait(shard, conn_id, keys, op, deadline).await;
        }
        Step::UnregisterWait { shard, conn_id } => {
            driver.unregister_wait(shard, conn_id).await;
        }
    }
}

/// Fire one tick against the driver.
pub async fn fire_tick(driver: &mut ShardDriver, shard: usize, tick: Tick) {
    match tick {
        Tick::Expiry => driver.tick_expiry(shard),
        Tick::WaiterTimeout => driver.tick_waiter_timeout(shard),
        Tick::ContinuationRelease => driver.pump_continuation_release(shard).await,
    }
}

/// Replay a whole schedule: advance senders and fire ticks in the chosen order,
/// then drain each sender to completion so no program is left half-run.
pub async fn replay(
    driver: &mut ShardDriver,
    senders: &mut Vec<Sender>,
    schedule: &[Choice],
    num_shards: usize,
) {
    for choice in schedule {
        match *choice {
            Choice::Advance(s) => advance(driver, senders, s).await,
            Choice::Tick { shard, tick } => {
                if shard < num_shards && tick != Tick::ContinuationRelease {
                    fire_tick(driver, shard, tick).await;
                }
            }
        }
    }
    // Finish every remaining step so the quiesce asserts see a full history.
    for s in 0..senders.len() {
        while !senders[s].finished() {
            advance(driver, senders, s).await;
        }
    }
}

/// proptest strategy for a schedule over `num_senders` senders and `num_shards`
/// shards, at most `max_len` choices. Advancing a finished sender is a no-op,
/// so the strategy is total (every generated schedule is legal) and shrinks by
/// dropping choices.
pub fn schedule_strategy(
    num_senders: usize,
    num_shards: usize,
    max_len: usize,
) -> impl Strategy<Value = Vec<Choice>> {
    let advance = (0..num_senders).prop_map(Choice::Advance);
    let tick = (
        0..num_shards,
        prop_oneof![Just(Tick::Expiry), Just(Tick::WaiterTimeout)],
    )
        .prop_map(|(shard, tick)| Choice::Tick { shard, tick });
    // Bias toward advances (senders drive most events); ticks interleave.
    let choice = prop_oneof![3 => advance, 1 => tick];
    proptest::collection::vec(choice, 0..max_len)
}

mod generator_tests {
    use super::*;

    #[test]
    fn advancing_a_finished_sender_is_a_noop() {
        let s = Sender::new(vec![]);
        assert!(s.finished());
    }

    proptest! {
        #[test]
        fn schedule_never_exceeds_bound(sched in schedule_strategy(2, 1, 8)) {
            prop_assert!(sched.len() <= 8);
            for c in &sched {
                match c {
                    Choice::Advance(s) => prop_assert!(*s < 2),
                    Choice::Tick { shard, .. } => prop_assert!(*shard < 1),
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run tests.** `just test frogdb-core generator_tests` — expected PASS.

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/generator.rs
git commit -m "test(core): schedule generator with structural C1-C7 sender state machines"
```

---

## Task 6 — S1: dual-timeout race (pin under permutation)

**Goal:** Pin the already-closed lost-element race (`drive_satisfaction` re-validates deadline + `is_closed()` before consuming, blocking.rs:243-261) under permutation. 1 shard. Conn A (`BlockWait` BLPOP with elapsed-or-future deadline), conn B (`Execute` LPUSH), `WaiterTimeoutTick` events, A's optional receiver-drop + `UnregisterWait` (C3). Invariants: **element conservation** (v delivered to A exactly once XOR present in `worker.store` at quiesce), wait queue empty at quiesce, a delivered response never sent to a closed receiver. This is a **pin**, not a bug hunt (brief D4).

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/scenario_s1.rs` (replace stub).

- [ ] **Step 1: Write the scenario.** Replace `scenario_s1.rs`:

```rust
//! S1 — dual-timeout race (pin). The lost-element race is closed in
//! `drive_satisfaction` (blocking.rs:243-261); 4b pins that closure under
//! permutation. Invariants: element conservation, empty wait queue at quiesce,
//! no delivery to a closed receiver.

use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::Response;
use proptest::prelude::*;

use crate::harness::ShardDriver;
use frogdb_core::types::BlockingOp;

/// Run one permutation. `deadline_elapsed` selects A's deadline variant;
/// `drop_receiver` models A giving up (receiver-drop before UnregisterWait);
/// `push_first` selects whether B's LPUSH is serviced before or after the
/// waiter-timeout tick.
async fn run_case(deadline_elapsed: bool, drop_receiver: bool, push_first: bool) {
    let mut d = ShardDriver::new(1);

    let deadline = if deadline_elapsed {
        Some(Instant::now() - Duration::from_millis(1))
    } else {
        Some(Instant::now() + Duration::from_secs(30))
    };

    // Conn A blocks on BLPOP k.
    let rx_a = d
        .block_wait(0, 10, vec![Bytes::from_static(b"k")], BlockingOp::BLPop, deadline)
        .await;

    // Optionally model A giving up: drop the receiver so the shard sees a
    // closed channel (is_closed()).
    let mut rx_a = Some(rx_a);
    if drop_receiver {
        rx_a = None;
    }

    if push_first {
        // Conn B pushes the element, then a coarse waiter-timeout tick.
        let _ = d.execute_conn(0, 20, "LPUSH", &["k", "v"]).await;
        d.tick_waiter_timeout(0);
    } else {
        // Waiter-timeout tick first (GC pass), then the push.
        d.tick_waiter_timeout(0);
        let _ = d.execute_conn(0, 20, "LPUSH", &["k", "v"]).await;
    }

    // A optionally sends UnregisterWait (C3: only in Timeout/Unblocked
    // histories — modeled by the elapsed-deadline or dropped-receiver cases).
    if deadline_elapsed || drop_receiver {
        d.unregister_wait(0, 10).await;
    }

    // Determine whether A received the element.
    let a_got_element = match rx_a.take() {
        Some(rx) => matches!(rx.await, Ok(Response::Array(_))),
        None => false, // receiver dropped: cannot have observed a delivery
    };

    // Element conservation: v delivered to A exactly once XOR still in the list.
    let llen = d.execute(0, "LLEN", &["k"]).await;
    let list_has_v = matches!(llen, Response::Integer(n) if n == 1);
    assert!(
        a_got_element ^ list_has_v,
        "element conservation violated: a_got={a_got_element} list_has_v={list_has_v} \
         (elapsed={deadline_elapsed} drop={drop_receiver} push_first={push_first})"
    );

    // Wait queue empty at quiesce.
    let wq = d.wait_queue_info(0).await;
    assert_eq!(
        wq.total_waiters, 0,
        "wait queue not drained at quiesce ({} waiters)",
        wq.total_waiters
    );
}

#[tokio::test]
async fn s1_all_deterministic_permutations() {
    for &elapsed in &[false, true] {
        for &drop_rx in &[false, true] {
            for &push_first in &[false, true] {
                run_case(elapsed, drop_rx, push_first).await;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    #[test]
    fn prop_s1_element_conserved(
        elapsed in any::<bool>(),
        drop_rx in any::<bool>(),
        push_first in any::<bool>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(run_case(elapsed, drop_rx, push_first));
    }
}
```

- [ ] **Step 2: Run tests.** `just test frogdb-core s1` — expected PASS (the race is already closed; this pins it). If `prop_s1_element_conserved` fails, that is a **genuine regression in the lost-element fix** — stop, follow `superpowers:systematic-debugging`, and do not weaken the conservation assertion.

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/scenario_s1.rs
git commit -m "test(core): S1 dual-timeout race pinned under permutation (element conservation)"
```

---

## Task 7 — S2 characterization: WATCH vs expiry vs unrelated write (no false negative; over-abort characterized)

**Goal:** S2 (brief D4/S2). 1 shard. Conn A: `GetVersion` → (gap) → `ExecTransaction{watches:[(k,v0)], commands:[SET k x]}`. In the gap, permuted: conn B unrelated-key write, conn B watched-key write, `ExpiryTick` (seeded expiring keys), lazy-expiry probe. Invariant: **zero false negatives** — if the watched key's state changed by a *write* or by *active expiry*, EXEC must return `WatchAborted`; over-aborts (unrelated same-shard write / active-expiry removal) are legal and **characterized** (counted, not asserted). The F3 lazy-expiry case is split into Tasks 8–9. This task covers the write and active-expiry arms only.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs` (replace stub).

- [ ] **Step 1: Write the scenario.** Replace `scenario_s2.rs`:

```rust
//! S2 — WATCH vs expiry vs unrelated write (characterization). WATCH is a
//! per-shard version (worker.rs:459-469): any same-shard write or active-expiry
//! removal bumps the version and aborts EXEC. Invariant: zero false negatives
//! (a genuine change to the watched key MUST abort). Over-aborts are legal and
//! characterized. F3 (lazy-expiry false negative) is covered in Tasks 8-9.

use std::time::Duration;

use bytes::Bytes;

use crate::harness::{cmd, ShardDriver};
use frogdb_core::shard::types::TransactionResult;

/// What happens in the WATCH→EXEC gap.
#[derive(Debug, Clone, Copy)]
enum Interleave {
    None,
    UnrelatedWrite,
    WatchedWrite,
    ActiveExpiryUnrelated,
    ActiveExpiryWatched,
}

/// Returns (aborted, watched_key_changed). `watched_key_changed` is the model
/// truth: did the watched key's value/existence change due to a write or
/// active expiry during the gap?
async fn run_case(interleave: Interleave) -> (bool, bool) {
    let mut d = ShardDriver::new(1);

    // Seed the watched key and an unrelated key.
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    let _ = d.execute(0, "SET", &["u", "u0"]).await;

    // Conn A reads the pre-gap version and watches k at it.
    let v0 = d.get_version(0).await;

    let mut watched_changed = false;
    match interleave {
        Interleave::None => {}
        Interleave::UnrelatedWrite => {
            let _ = d.execute_conn(0, 2, "SET", &["u", "u1"]).await;
        }
        Interleave::WatchedWrite => {
            let _ = d.execute_conn(0, 2, "SET", &["k", "v1"]).await;
            watched_changed = true;
        }
        Interleave::ActiveExpiryUnrelated => {
            // Seed an unrelated key with an already-elapsed TTL, then sweep.
            let _ = d.execute(0, "SET", &["e", "e0"]).await;
            let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;
            tokio::time::sleep(Duration::from_millis(3)).await;
            d.tick_expiry(0);
        }
        Interleave::ActiveExpiryWatched => {
            let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
            tokio::time::sleep(Duration::from_millis(3)).await;
            d.tick_expiry(0);
            watched_changed = true;
        }
    }

    // Conn A runs EXEC watching k at v0.
    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["k", "x"])],
            vec![(Bytes::from_static(b"k"), v0)],
        )
        .await;
    let aborted = matches!(result, TransactionResult::WatchAborted);
    (aborted, watched_changed)
}

#[tokio::test]
async fn s2_zero_false_negatives_and_over_abort_characterized() {
    let cases = [
        Interleave::None,
        Interleave::UnrelatedWrite,
        Interleave::WatchedWrite,
        Interleave::ActiveExpiryUnrelated,
        Interleave::ActiveExpiryWatched,
    ];
    let mut over_aborts = 0u32;
    for c in cases {
        let (aborted, changed) = run_case(c).await;
        // Zero false negatives: a real change must abort.
        if changed {
            assert!(aborted, "false negative: {c:?} changed the watched key but EXEC committed");
        }
        // Characterize over-aborts (legal, per the pinned per-shard-version
        // divergence) — count, do not assert.
        if aborted && !changed {
            over_aborts += 1;
        }
    }
    // Documented over-abort sources: unrelated same-shard write + unrelated
    // active-expiry removal both bump shard_version.
    eprintln!("S2 over-aborts (legal, characterized): {over_aborts}");
    assert!(over_aborts >= 1, "expected the per-shard-version over-abort to be observable");
}
```

- [ ] **Step 2: Run tests.** `just test frogdb-core s2_zero_false_negatives` — expected PASS.

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs
git commit -m "test(core): S2 WATCH vs write/active-expiry — zero false negatives, over-abort characterized"
```

---

## Task 8 — F3 real-path verification (D8): lazy-expiry WATCH false negative on the real server

**Goal:** Before fixing F3, verify the analysis two ways (brief D8, F3): (a) confirm current Redis/Valkey/Dragonfly behavior — an expired watched key touched only at EXEC time **aborts** the transaction; (b) reproduce the FrogDB defect on the **real connection path** (turmoil / full-server) — WATCH k → `PEXPIRE k 1` → wait past TTL with no sweep and no touch → EXEC commits (should abort). This task lands a turmoil-level test that **documents** current (wrong) behavior; the shard-driver failing test + fix land in Task 9.

**Files touched:**
- `frogdb-server/crates/server/tests/` — a new focused turmoil test (place beside the existing sim tests, e.g. a `regressions`/scenario module in `simulation.rs` or a small new `tests/watch_lazy_expiry.rs` gated `#[cfg(feature = "turmoil")]`; match the crate's existing test-file convention).

**Research to record in the commit message (behavior verification):**
- Redis 7/8 & Valkey: `WATCH k; PEXPIRE k 1; <sleep past TTL>; MULTI; <cmd>; EXEC` → EXEC returns **nil (aborted)** — an expired watched key is treated as a modification of the watch (the key transitioned live→gone). DragonflyDB matches. Cite the source you consult (redis `t_string.c`/`multi.c` expire-touches-watch semantics; valkey mirror) in the commit body.
- FrogDB seam: lazy expiry (`purge_if_expired` → hashmap.rs:1056-1058) never bumps `shard_version`; active expiry does (event_loop.rs:212). So a lazily-expiring watched key does not dirty the per-shard WATCH version → false negative.

- [ ] **Step 1: Write the real-path repro.** Add a turmoil test that drives a real sim server: one client `WATCH k`, `PEXPIRE k 1`, advances sim time past the TTL **without** issuing any command that touches `k` and with active expiry not sweeping it in the window, then `MULTI; SET k x; EXEC`. Assert the **currently observed** outcome and mark the correctness gap:

```rust
// In the chosen server-crate turmoil test file (gated #[cfg(feature = "turmoil")]).
// Uses the existing sim harness (see common/sim_harness.rs, common/sim_helpers.rs).
// SKELETON — the shape below is the contract; replace the body with real
// sim-harness calls (study a passing simulation.rs test for the client API).
#[test]
#[ignore = "F3 real-path repro: documents current false-negative; fixed + inverted in Task 9"]
fn watch_lazy_expiry_false_negative_realpath() {
    // NOTE: This documents the CURRENT (incorrect) behavior on the real
    // connection path, verifying F3 is not a shard-driver artifact. Once F3 is
    // fixed (Task 9), flip the assertion to require an ABORT and drop #[ignore].
    // Redis/Valkey/Dragonfly all ABORT here (expired watched key touches the watch).
    //
    // <build a single-shard sim server; single client:>
    //   SET k v
    //   WATCH k
    //   PEXPIRE k 1
    //   <advance sim time > 1ms; do NOT touch k; ensure no active sweep hits k>
    //   MULTI ; SET k x ; EXEC
    //
    // Assert the observed reply. If FrogDB currently COMMITS, assert commit and
    // leave a `// BUG(F3):` marker; the Task-9 fix inverts + un-ignores this.
    // Keep it minimal — one test, reuse the existing helpers, no new harness
    // infrastructure.
    unimplemented!("fill in with the existing sim harness per this task's notes");
}
```

**Implementer:** replace the `unimplemented!` skeleton with a concrete body using the existing sim harness helpers (study a passing test in `simulation.rs` for the exact client/setup API — `sim_helpers.rs` builds the standalone server; drive RESP commands through a sim client). Keep the test `#[ignore]` in this task (it captures the *current* wrong behavior). The point of this task is the **real-path confirmation + documented Redis/Valkey/Dragonfly behavior in the commit message**, gating the Task-9 fix.

- [ ] **Step 2: Run it.** `cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/watch_lazy_expiry_false_negative_realpath/)' > "$TMPDIR/f3_realpath.log" 2>&1` (foreground, 600000ms timeout; `tail "$TMPDIR/f3_realpath.log"`). Expected: the test asserts current behavior and passes as written (documenting the gap). Record in the commit body whether FrogDB currently commits (confirming the bug) and the Redis/Valkey/Dragonfly reference.

- [ ] **Step 3: Commit.**

```bash
git add frogdb-server/crates/server/tests
git commit -m "test(sim): real-path repro for WATCH lazy-expiry false negative (F3), Redis/Valkey/Dragonfly abort"
```

---

## Task 9 — F3 fix: lazy purge bumps shard version at the worker seam (S2 false-negative arm)

**Goal:** With the real-path repro (Task 8) confirming F3, land the shard-driver failing test (the lazy-expiry arm of S2) and fix it at the **worker seam** — lazy purge is invoked from command execution which *can* reach `increment_version`; the store stays version-ignorant (encapsulation, brief F3). Then invert + un-ignore the Task-8 turmoil test. If investigation (Task 8) had overturned the analysis, the test would pin the verified-correct behavior instead — it did not.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs` (add the F3 arm test).
- The worker seam where lazy purge runs (investigate: the read/write command path that calls `purge_if_expired`/`check_and_delete_expired`; bump `increment_version` when a lazy purge actually removed a key). **Do not** add `shard_version` awareness to `HashMapStore`.
- `frogdb-server/crates/server/tests/...` (invert + un-ignore the Task-8 test).

**Implementer investigation note:** locate where the shard worker invokes lazy expiry during command execution (grep `purge_if_expired`, `check_and_delete_expired`, `lazy`), confirm it returns whether a key was removed, and increment the shard version there when a removal occurred — the same version bump active expiry does (event_loop.rs:212), but at the worker/command seam so the store never learns about shard versions. Read the exact function around the call site before editing; the research points to `hashmap.rs:1056-1058` for the store method and `event_loop.rs:212` for the active-expiry precedent.

- [ ] **Step 1: Write the failing shard-driver test.** Append to `scenario_s2.rs`:

```rust
/// F3: a watched key whose TTL elapses with no active sweep and no touch until
/// EXEC. Lazy expiry removed it (observed by the EXEC's own key access), so the
/// watch MUST abort — Redis/Valkey/Dragonfly all abort here.
#[tokio::test]
async fn s2_f3_lazy_expiry_watched_key_aborts() {
    let mut d = ShardDriver::new(1);
    let _ = d.execute(0, "SET", &["k", "v0"]).await;
    let v0 = d.get_version(0).await;

    // Watched key gets a 1ms TTL; wait past it. Do NOT tick active expiry and
    // do NOT touch k — the key is only observed lazily-expired at EXEC time.
    let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["k", "x"])],
            vec![(Bytes::from_static(b"k"), v0)],
        )
        .await;

    assert!(
        matches!(result, TransactionResult::WatchAborted),
        "F3: an expired watched key touched only at EXEC must abort the transaction, got {result:?}"
    );
}
```

- [ ] **Step 2: Run to verify it fails.** `just test frogdb-core s2_f3_lazy_expiry` — expected FAIL (EXEC currently commits: lazy purge did not bump `shard_version`, so `check_watches` still sees `v0`).

- [ ] **Step 3: Fix at the worker seam.** Bump the shard version when a lazy purge removes a key during command execution (see investigation note). Keep `HashMapStore` version-ignorant.

- [ ] **Step 4: Run to verify green + no regressions.** `just test frogdb-core s2` — expected PASS (all S2 arms, including the new F3 arm). Then `just test frogdb-core driver_tests` and `just test frogdb-core command_context_tests` — expected PASS (no version-bump regressions).

- [ ] **Step 5: Invert + un-ignore the real-path test.** In the Task-8 turmoil test, remove `#[ignore]` and flip the assertion to require an **abort** (nil EXEC). Re-run: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/watch_lazy_expiry_false_negative_realpath/)' > "$TMPDIR/f3_fixed.log" 2>&1` — expected PASS.

- [ ] **Step 6: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs frogdb-server/crates/core/src/shard frogdb-server/crates/server/tests
git commit -m "fix(core): lazy-expiry purge bumps shard version so WATCH aborts on expired watched key (F3)"
```

---

## Task 10 — S3: VLL phase-2/3 failure, sparse participants [2,5,7]

**Goal:** 8 shards, pumped mode, real coordinator + `ChannelSink`/`FaultSink`. Inject phase-2 (lock dispatch) and phase-3 (execute dispatch) failures on chosen shards, plus driver service-withholding to force `LockTimeout`. Permute service schedules × failure points. Invariants (C5): abort messages address exactly the real shard ids per the C5 split; after drain, `GetLockTableInfo` on all of [2,5,7] shows zero intents + no continuation lock; non-participant shards untouched; no partial VLL-EXEC effects on aborted shards.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/scenario_s3.rs` (replace stub).

- [ ] **Step 1: Write the scenario.** Replace `scenario_s3.rs`:

```rust
//! S3 — VLL phase-2/3 failure with sparse participants [2,5,7]. Real
//! VllCoordinator over a FaultSink; the driver's freedom is the per-shard
//! service schedule + sink-injected failures (C5 holds by construction). After
//! any abort every participant's lock table is clean and no partial write
//! landed on an aborted shard.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_vll::{LockMode, NoopMetricsSink, ScatterParticipant, ScatterRequest, VllCoordinator};

use crate::harness::ShardDriver;
use crate::sink::FaultSink;
use frogdb_core::shard::ScatterOp;

const PARTICIPANTS: [usize; 3] = [2, 5, 7];
const NUM_SHARDS: usize = 8;

fn mset_request(txid: u64) -> ScatterRequest<ScatterOp> {
    ScatterRequest {
        txid,
        mode: LockMode::Write,
        participants: PARTICIPANTS
            .iter()
            .map(|&sid| {
                let k = Bytes::from(format!("k{sid}"));
                ScatterParticipant {
                    shard_id: sid,
                    keys: vec![k.clone()],
                    operation: ScatterOp::MSet {
                        pairs: vec![(k, Bytes::from(format!("val{sid}")))],
                    },
                }
            })
            .collect(),
        timeout: Duration::from_millis(200),
        command: "MSET",
    }
}

/// Drive one scatter with the given fault sink to completion, servicing all
/// participant queues, then assert clean lock tables + no partial writes.
async fn run_and_assert_clean(
    mut driver: ShardDriver,
    fail_lock: HashSet<usize>,
    fail_execute: HashSet<usize>,
    withhold: HashSet<usize>,
    expect_ok: bool,
) {
    let sink = FaultSink::new(driver.senders(), fail_lock, fail_execute);
    let coordinator = Arc::new(VllCoordinator::new(sink, NoopMetricsSink));

    let coord = coordinator.clone();
    let handle = tokio::spawn(async move { coord.scatter(mset_request(1)).await });

    // Service every participant queue except withheld ones, until the
    // coordinator finishes. Withholding forces a LockTimeout on that shard.
    loop {
        let mut serviced = false;
        for &sid in &PARTICIPANTS {
            if withhold.contains(&sid) {
                continue;
            }
            serviced |= driver.pump_one(sid).await;
        }
        if handle.is_finished() && !serviced {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Drain any trailing abort messages the coordinator queued on the shards.
    for &sid in &PARTICIPANTS {
        driver.drain(sid).await;
    }

    let outcome = handle.await.unwrap();
    assert_eq!(outcome.is_ok(), expect_ok, "unexpected scatter outcome: {outcome:?}");

    // Every participant's lock table is clean; no continuation lock.
    for &sid in &PARTICIPANTS {
        let lt = driver.lock_table_info(sid).await;
        assert!(
            lt.intents.is_empty(),
            "shard {sid} leaked lock intents after drain: {:?}",
            lt.intents
        );
        assert!(lt.continuation_lock.is_none(), "shard {sid} has a stray continuation lock");
    }

    // On any abort, no aborted shard retains a partial VLL-EXEC write.
    if !expect_ok {
        for &sid in &PARTICIPANTS {
            let got = driver.execute(sid, "GET", &[&format!("k{sid}")]).await;
            assert_eq!(
                got,
                Response::Bulk(None),
                "aborted scatter left a partial write on shard {sid}"
            );
        }
    }

    // Non-participant shards are untouched.
    for sid in 0..NUM_SHARDS {
        if PARTICIPANTS.contains(&sid) {
            continue;
        }
        let lt = driver.lock_table_info(sid).await;
        assert!(lt.intents.is_empty(), "non-participant shard {sid} touched");
    }
}

#[tokio::test]
async fn s3_phase2_lock_failure_aborts_all_participants() {
    // Fail the lock dispatch on shard 5 → ShardUnavailable, all real ids aborted.
    run_and_assert_clean(
        ShardDriver::new(NUM_SHARDS),
        HashSet::from([5]),
        HashSet::new(),
        HashSet::new(),
        false,
    )
    .await;
}

#[tokio::test]
async fn s3_phase3_execute_failure_aborts_remaining_participants() {
    // Fail the execute dispatch on shard 7 (last participant) → shards[idx..]
    // aborted; already-executed shards release their own locks.
    run_and_assert_clean(
        ShardDriver::new(NUM_SHARDS),
        HashSet::new(),
        HashSet::from([7]),
        HashSet::new(),
        false,
    )
    .await;
}

#[tokio::test]
async fn s3_withheld_service_forces_lock_timeout() {
    // Withhold service from shard 5: its ready_rx never resolves → LockTimeout;
    // all real ids aborted, tables clean.
    run_and_assert_clean(
        ShardDriver::new(NUM_SHARDS),
        HashSet::new(),
        HashSet::new(),
        HashSet::from([5]),
        false,
    )
    .await;
}

#[tokio::test]
async fn s3_clean_run_commits_on_all_participants() {
    let mut driver = ShardDriver::new(NUM_SHARDS);
    let sink = crate::sink::ChannelSink::new(driver.senders());
    let coordinator = Arc::new(VllCoordinator::new(sink, NoopMetricsSink));
    let coord = coordinator.clone();
    let handle = tokio::spawn(async move { coord.scatter(mset_request(1)).await });
    loop {
        let mut serviced = false;
        for &sid in &PARTICIPANTS {
            serviced |= driver.pump_one(sid).await;
        }
        if handle.is_finished() && !serviced {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(handle.await.unwrap().is_ok());
    for &sid in &PARTICIPANTS {
        assert_eq!(
            driver.execute(sid, "GET", &[&format!("k{sid}")]).await,
            Response::Bulk(Some(Bytes::from(format!("val{sid}"))))
        );
        assert!(driver.lock_table_info(sid).await.intents.is_empty());
    }
}
```

**Import note:** add `use crate::sink::ChannelSink;` if preferred over the fully-qualified path used in `s3_clean_run_commits_on_all_participants`.

- [ ] **Step 2: Run tests.** `just test frogdb-core s3` — expected PASS. If a fault case hangs, the coordinator is waiting on a queue the loop stopped servicing — confirm the drain-after-finish and `yield_now` are present.

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/scenario_s3.rs
git commit -m "test(core): S3 VLL phase-2/3 failure with sparse participants [2,5,7], clean lock tables"
```

---

## Task 11 — S4: continuation-lock holder panic releases the lock; shard resumes

**Goal:** ≥2 shards, pumped. Run `acquire_continuation_and_run` whose `run` closure panics after acquisition (task panic → unwind → guard `Drop` → `release_txs` fire; no `panic = "abort"` in the workspace). The driver then `pump_continuation_release` each shard (permuted order/timing vs other traffic). Invariants: every shard clears its continuation lock (`GetLockTableInfo.continuation_lock == None`); a subsequent `Execute` from another conn succeeds (no lingering `-ERR shard busy with continuation lock`); the panicked task's `JoinError` is observed.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/scenario_s4.rs` (replace stub).

- [ ] **Step 1: Write the scenario.** Replace `scenario_s4.rs`:

```rust
//! S4 — continuation-lock holder panic. The guard's Drop (coordinator.rs:136-142)
//! fires every release_tx on unwind; the shard observes it at a
//! ContinuationReleasePump step (event_loop.rs:88-91). After the pump every
//! shard's continuation lock is cleared and another conn can execute.

use std::sync::Arc;
use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_vll::{NoopMetricsSink, VllCoordinator};

use crate::harness::ShardDriver;
use crate::sink::ChannelSink;

const SHARDS: [usize; 2] = [0, 1];

#[tokio::test]
async fn s4_holder_panic_releases_locks_and_shard_resumes() {
    let mut driver = ShardDriver::new(2);
    let coordinator = Arc::new(VllCoordinator::new(
        ChannelSink::new(driver.senders()),
        NoopMetricsSink,
    ));

    // Spawn the continuation holder; its run() panics AFTER lock acquisition.
    let coord = coordinator.clone();
    let holder = tokio::spawn(async move {
        coord
            .acquire_continuation_and_run(
                1,       // txid
                99,      // conn_id (the lock owner)
                &SHARDS, // ascending
                Duration::from_secs(5),
                || async {
                    panic!("continuation holder panics after acquiring the lock");
                    #[allow(unreachable_code)]
                    Ok::<(), frogdb_vll::ContinuationError>(())
                },
            )
            .await
    });

    // Phase 1: service both shards so each acquires its continuation lock and
    // replies Ready, letting the holder proceed into run() and panic.
    // Pump until the lock is visible on both shards.
    for _ in 0..64 {
        driver.pump_one(0).await;
        driver.pump_one(1).await;
        tokio::task::yield_now().await;
        let l0 = driver.lock_table_info(0).await;
        let l1 = driver.lock_table_info(1).await;
        if l0.continuation_lock.is_some() && l1.continuation_lock.is_some() {
            break;
        }
    }
    assert!(driver.lock_table_info(0).await.continuation_lock.is_some());
    assert!(driver.lock_table_info(1).await.continuation_lock.is_some());

    // The holder task panicked → its JoinError is observed (not swallowed).
    let join = holder.await;
    assert!(join.is_err(), "the panicking holder task must surface a JoinError");

    // Guard Drop (on unwind) fired the release signals; pump each shard's
    // continuation-release seam in a permuted order.
    driver.pump_continuation_release(1).await;
    driver.pump_continuation_release(0).await;

    // Every shard cleared its continuation lock.
    for &sid in &SHARDS {
        assert!(
            driver.lock_table_info(sid).await.continuation_lock.is_none(),
            "shard {sid} still holds a continuation lock after release pump"
        );
    }

    // A different connection can now execute (no -ERR shard busy).
    for &sid in &SHARDS {
        let resp = driver.execute_conn(sid, 7, "SET", &["after", "ok"]).await;
        assert!(
            matches!(resp, Response::Simple(_)),
            "shard {sid} still refuses a post-release write: {resp:?}"
        );
    }
}
```

- [ ] **Step 2: Run tests.** `just test frogdb-core s4` — expected PASS. Note: a panicking spawned task prints a panic backtrace to stderr — that is expected; the test asserts the `JoinError`. If the acquisition loop times out, increase the bounded pump count (the coordinator needs both shards serviced to acquire before `run()` runs).

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/scenario_s4.rs
git commit -m "test(core): S4 continuation-lock holder panic releases locks, shard resumes"
```

---

## Task 12 — F1 real-path verification (D8) then fix; S5 blocked XREADGROUP + key death (TTL vs DEL)

**Goal:** F1 (confirmed bug): TTL expiry never drains blocked XREADGROUP waiters — `apply_expiry_effects` never touches `wait_queue`, while the DEL path drains via `drain_stream_waiters_with_error` → NOGROUP. This task: (1) real-path turmoil repro (D8: XREADGROUP BLOCK + short TTL, no subsequent write → hang-until-timeout vs expected NOGROUP); (2) failing shard-driver test (S5 TTL arm); (3) fix in `apply_expiry_effects`; (4) S5 full scenario green. XREAD (non-group) waiters stay blocked in both arms (blocking.rs:313-318).

**Files touched:**
- `frogdb-server/crates/server/tests/...` — F1 real-path turmoil test (per Task-8 conventions).
- `frogdb-server/crates/core/src/shard/event_loop.rs` — the F1 fix in `apply_expiry_effects`.
- `frogdb-server/crates/core/tests/shard_driver/scenario_s5.rs` (replace stub).

- [ ] **Step 1: Real-path repro first (D8).** Add a turmoil test: a client issues `XADD st 1-1 f v`, `XGROUP CREATE st g 0`, then a second client `XREADGROUP GROUP g c BLOCK 0 STREAMS st >` after acking the first (so it blocks), then `PEXPIRE st <short>`; advance sim time past the TTL with **no further write** to `st`. Observe: the blocked client currently **hangs until its own timeout** (or never, with BLOCK 0) instead of getting NOGROUP. Assert the current behavior with a `// BUG(F1):` marker; keep `#[ignore]` until the fix. Run:
  `cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/xreadgroup_ttl_no_nogroup_realpath/)' > "$TMPDIR/f1_realpath.log" 2>&1` (foreground, 600000ms). Record the confirmation in the commit body.

- [ ] **Step 2: Write the failing shard-driver test.** Replace `scenario_s5.rs`:

```rust
//! S5 — blocked XREADGROUP + key death (TTL vs DEL). DEL drains XREADGROUP
//! waiters to NOGROUP (blocking.rs:319-331); TTL/active-expiry currently does
//! NOT (F1 gap). After the fix both arms converge to identical NOGROUP
//! outcomes; a plain XREAD waiter stays blocked in both.

use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::harness::ShardDriver;
use frogdb_core::types::BlockingOp;

/// Register a blocked XREADGROUP waiter on `st` for group `g`, consumer `c`.
async fn block_xreadgroup(d: &mut ShardDriver, conn_id: u64) -> tokio::sync::oneshot::Receiver<Response> {
    d.block_wait(
        0,
        conn_id,
        vec![Bytes::from_static(b"st")],
        BlockingOp::XReadGroup {
            group: Bytes::from_static(b"g"),
            consumer: Bytes::from_static(b"c"),
            noack: false,
            count: None,
        },
        None,
    )
    .await
}

fn is_nogroup(resp: &Response) -> bool {
    matches!(resp, Response::Error(e) if e.starts_with(b"NOGROUP"))
}

async fn setup_stream_group(d: &mut ShardDriver) {
    let _ = d.execute(0, "XADD", &["st", "1-1", "f", "v"]).await;
    let _ = d.execute(0, "XGROUP", &["CREATE", "st", "g", "0"]).await;
}

#[tokio::test]
async fn s5_del_arm_drains_xreadgroup_to_nogroup() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let group_rx = block_xreadgroup(&mut d, 10).await;

    // A plain XREAD waiter alongside — must stay blocked.
    let xread_rx = d
        .block_wait(
            0,
            11,
            vec![Bytes::from_static(b"st")],
            BlockingOp::XRead { after_ids: vec![], count: None },
            None,
        )
        .await;

    // DEL the stream → NOGROUP for the group waiter.
    let _ = d.execute_conn(0, 20, "DEL", &["st"]).await;

    let group_resp = group_rx.await.expect("group waiter replied");
    assert!(is_nogroup(&group_resp), "DEL arm must drain group waiter to NOGROUP, got {group_resp:?}");

    // XREAD waiter still blocked (its receiver is not yet resolved).
    assert!(xread_rx.try_recv().is_err(), "plain XREAD waiter must stay blocked");

    // Only the XREAD waiter remains.
    let wq = d.wait_queue_info(0).await;
    assert_eq!(wq.total_waiters, 1, "XREADGROUP waiter drained; XREAD retained");
}

#[tokio::test]
async fn s5_ttl_arm_drains_xreadgroup_to_nogroup_after_f1_fix() {
    let mut d = ShardDriver::new(1);
    setup_stream_group(&mut d).await;
    let group_rx = block_xreadgroup(&mut d, 10).await;

    // TTL path: seed a short TTL, wait past it, then run the active-expiry tick.
    let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    d.tick_expiry(0);

    // After F1: the group waiter receives the SAME NOGROUP the DEL arm produces.
    let group_resp = group_rx.await.expect("group waiter replied after expiry");
    assert!(
        is_nogroup(&group_resp),
        "TTL arm must converge to NOGROUP after F1 fix, got {group_resp:?}"
    );

    let wq = d.wait_queue_info(0).await;
    assert_eq!(wq.total_waiters, 0, "no group waiter left after expiry drain");
    // Expiry index clean at quiesce.
    assert!(d.expiry_index_check(0).await.anomalies.is_empty());
}
```

- [ ] **Step 3: Run to verify the TTL arm fails.** `just test frogdb-core s5_ttl_arm` — expected FAIL mode: pre-fix the waiter is never drained — its `WaitEntry` (holding the reply sender) stays parked in the queue with no deadline, so `group_rx.await` pends forever and the test dies at the nextest per-test timeout. **The timeout IS the failing signal**; do not add a workaround or a deadline to dodge it. The DEL arm test (`s5_del_arm`) already passes (pins existing behavior).

- [ ] **Step 4: Fix F1 in `apply_expiry_effects`.** In `event_loop.rs`, in `apply_expiry_effects` (the shard side of the seam), drain XREADGROUP waiters for each deleted/emptied key — the same drain the write path uses (`drain_stream_waiters_with_error`, a `&mut self` method on `ShardWorker`, blocking.rs:319). Since `drain_stream_waiters_with_error` only pops XREADGROUP waiters (no-op if none) and does not touch the store, calling it per removed key is safe and matches DEL-path semantics. Add, inside the two existing `for key in &result.deleted_keys { ... }` and `for key in &result.emptied_keys { ... }` loops (after the existing effects, or in a dedicated pass after them to avoid borrow issues), a drain call. Because the two loops iterate `&result.deleted_keys` / `&result.emptied_keys` by reference while `drain_stream_waiters_with_error(&mut self, ...)` needs `&mut self`, collect the keys first to avoid overlapping borrows:

```rust
        // F1: TTL/active-expiry must drain blocked XREADGROUP waiters for any
        // removed stream key, mirroring the DEL write path
        // (drain_stream_waiters_with_error → NOGROUP; XREAD waiters stay
        // blocked). apply_expiry_effects previously never touched wait_queue.
        let removed_keys: Vec<Bytes> = result
            .deleted_keys
            .iter()
            .chain(result.emptied_keys.iter())
            .cloned()
            .collect();
        for key in &removed_keys {
            self.drain_stream_waiters_with_error(key);
        }
```

Read the current `apply_expiry_effects` body first to locate the actual early-return and per-key loop positions (research cites :151-213, but verify), then place this block so it executes **whenever `deleted_keys`/`emptied_keys` is non-empty** — after the existing per-key effect loops. Confirm `drain_stream_waiters_with_error` is reachable from `event_loop.rs` (same crate, `impl ShardWorker` in `blocking.rs` — it is a private method on the same type, callable from any `impl ShardWorker` block in the crate). If Rust complains about visibility of a private method across modules, promote it to `pub(crate)` (it is `fn drain_stream_waiters_with_error` at blocking.rs:319) — a one-word change with no external surface.

- [ ] **Step 5: Run to verify green.** `just test frogdb-core s5` — expected PASS (both arms + XREAD-stays-blocked). Then `just test frogdb-core effect_tests` — expected PASS (no expiry-effect regression).

- [ ] **Step 6: Invert + un-ignore the real-path test.** Flip the Task-1 (this task's Step 1) turmoil test to require NOGROUP and remove `#[ignore]`. Re-run:
  `cargo nextest run -p frogdb-server --features turmoil -E 'test(/xreadgroup_ttl_no_nogroup_realpath/)' > "$TMPDIR/f1_fixed.log" 2>&1` — expected PASS.

- [ ] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/shard/event_loop.rs frogdb-server/crates/core/src/shard/blocking.rs frogdb-server/crates/core/tests/shard_driver/scenario_s5.rs frogdb-server/crates/server/tests
git commit -m "fix(core): active-expiry drains blocked XREADGROUP waiters to NOGROUP (F1)"
```

---

## Task 13 — F2 + S6: persist-failure mid-transaction rollback (EXECABORT)

**Goal:** F2 (misspelled error code) folds into S6's assertions. S6 (needs F4, Task 2): 1 shard, `WalMode::Fake` + injected `FakeFailure::AtWriteIndex(n)`, rollback policy flag set. Drive `ExecTransaction` with ≥3 write commands across ≥2 keys; permute failure index × prior key states. Invariants: all command results replaced with the `EXECABORT` error (post-F2 spelling); store equals the pre-transaction snapshot exactly (reverse-order restore); `WalRollbacks` metric incremented (observed indirectly via store state + no post-failure appends); `FakeWalLog` shows no post-failure appends. Unique shard ids per D6.

**Files touched:**
- `frogdb-server/crates/core/src/shard/execution.rs` — F2 spelling fix (`EXECABRT`→`EXECABORT`).
- `frogdb-server/crates/core/tests/shard_driver/scenario_s6.rs` (replace stub).

**D6 note:** `FakeWalRegistry` is process-global keyed by shard_id. S6 builds each worker with a **unique** shard_id from a module `AtomicUsize` and `num_shards = 1` (keyspace routing stays `Local` for `num_shards <= 1`, keyspace_coordinator.rs:73, so a length-1 sender vec is safe regardless of the id). This guarantees no cross-case registry collision within the in-process proptest run.

- [ ] **Step 1: Write the failing test (asserts the CORRECT `EXECABORT` spelling → fails on the F2 typo).** Replace `scenario_s6.rs`:

```rust
//! S6 — persist-failure mid-transaction rollback (needs the F4 fake-WAL
//! injection seam). On WAL failure the transaction rolls back in reverse order
//! (rollback.rs:85-116) and every command result is replaced with the
//! EXECABORT error (post-F2 spelling). Store == pre-transaction snapshot.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use frogdb_core::persistence::{FakeFailure, WalFailurePolicy};
use frogdb_core::shard::types::TransactionResult;
use frogdb_core::shard::{
    Envelope, FakeWalRegistry, NewConnection, ShardMessage, ShardReceiver, ShardSender, ShardWorkerBuilder, WalMode,
};
use frogdb_core::{CommandRegistry, ShardWorker};

/// Unique shard ids so the process-global FakeWalRegistry never collides (D6).
static NEXT_SHARD_ID: AtomicUsize = AtomicUsize::new(1);

fn cmd(name: &str, args: &[&str]) -> ParsedCommand {
    ParsedCommand::new(
        Bytes::from(name.to_string()),
        args.iter().map(|a| Bytes::from(a.to_string())).collect(),
    )
}

/// Build a single-shard worker with a fake WAL failing at `fail_index`, in
/// rollback mode, holding the msg/conn senders alive.
fn build_rollback_worker(
    fail_index: usize,
) -> (ShardWorker, mpsc::Sender<Envelope>, mpsc::Sender<NewConnection>, usize) {
    let shard_id = NEXT_SHARD_ID.fetch_add(1, Ordering::SeqCst);
    let (msg_tx, msg_rx) = mpsc::channel::<Envelope>(16);
    let (conn_tx, conn_rx) = mpsc::channel::<NewConnection>(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let mut worker = ShardWorkerBuilder::new(shard_id, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx.clone())]))
        .with_registry(Arc::new(registry))
        .with_wal_mode(WalMode::Fake)
        .with_fake_wal_failure(FakeFailure::AtWriteIndex(fail_index))
        .build();
    // Enable rollback mode: shared AtomicU8 whose value is the Rollback
    // discriminant (WalFailurePolicy::Rollback.as_u8() == 1, config.rs:17).
    worker.set_wal_failure_policy_flag(Arc::new(AtomicU8::new(WalFailurePolicy::Rollback.as_u8())));
    (worker, msg_tx, conn_tx, shard_id)
}

async fn exec_tx(
    worker: &mut ShardWorker,
    commands: Vec<ParsedCommand>,
) -> TransactionResult {
    let (tx, rx) = oneshot::channel();
    let msg = ShardMessage::ExecTransaction {
        commands,
        watches: vec![],
        conn_id: 1,
        protocol_version: ProtocolVersion::Resp3,
        response_tx: tx,
    };
    worker.drive(msg).await;
    rx.await.expect("transaction result")
}

async fn get(worker: &mut ShardWorker, key: &str) -> Response {
    let (tx, rx) = oneshot::channel();
    let msg = ShardMessage::Execute {
        command: Arc::new(cmd("GET", &[key])),
        conn_id: 1,
        txid: None,
        protocol_version: ProtocolVersion::Resp3,
        track_reads: false,
        no_touch: false,
        response_tx: tx,
    };
    worker.drive(msg).await;
    rx.await.unwrap()
}

/// Fail at write index 2. The fake sink counts one index per WAL record
/// (write_set/write_merge/write_delete each pass the index check, fake.rs:147-155):
/// the seed SET consumes index 0; the failing transaction's three writes are
/// indices 1 (a, persists), 2 (b, FAILS), 3 (c). All results become EXECABORT;
/// the store is fully restored.
#[tokio::test]
async fn s6_rollback_all_results_execabort_and_store_restored() {
    FakeWalRegistry::clear();
    // Prior key states: a pre-existing, b and c absent.
    let (mut worker, _mtx, _ctx, shard_id) = build_rollback_worker(2);
    let _ = exec_tx(&mut worker, vec![cmd("SET", &["a", "orig-a"])]).await; // write index 0 (persists)

    // Failing transaction: 3 writes across a (overwrite), b (create), c (create).
    let result = exec_tx(
        &mut worker,
        vec![
            cmd("SET", &["a", "new-a"]), // write index 1 (persists)
            cmd("SET", &["b", "new-b"]), // write index 2 (FAILS → whole tx rolls back)
            cmd("SET", &["c", "new-c"]),
        ],
    )
    .await;

    // All results replaced with the EXECABORT error (post-F2 spelling).
    match result {
        TransactionResult::Success(results) => {
            assert_eq!(results.len(), 3);
            for r in &results {
                match r {
                    Response::Error(e) => assert!(
                        e.starts_with(b"EXECABORT"),
                        "expected EXECABORT spelling, got {:?}",
                        String::from_utf8_lossy(e)
                    ),
                    other => panic!("expected EXECABORT error, got {other:?}"),
                }
            }
        }
        other => panic!("expected Success(vec of EXECABORT), got {other:?}"),
    }

    // Store == pre-transaction snapshot: a unchanged, b and c absent.
    assert_eq!(get(&mut worker, "a").await, Response::Bulk(Some(Bytes::from_static(b"orig-a"))));
    assert_eq!(get(&mut worker, "b").await, Response::Bulk(None));
    assert_eq!(get(&mut worker, "c").await, Response::Bulk(None));

    // FakeWalLog shows no post-failure appends for b/c.
    let log = FakeWalRegistry::log(shard_id).expect("fake log registered");
    assert!(
        log.effects().iter().all(|e| e.key.as_deref() != Some(&b"b"[..]) && e.key.as_deref() != Some(&b"c"[..])),
        "no WAL append should exist for rolled-back keys"
    );
}
```

**Implementer notes:**
1. If the injected failure does not trip where expected, dump
   `FakeWalRegistry::log(shard_id).effects()` to recalibrate the write index —
   do **not** loosen the assertions.
2. Rollback mode is enabled by the verified expression `Arc::new(AtomicU8::new(WalFailurePolicy::Rollback.as_u8()))` passed to `set_wal_failure_policy_flag` (worker.rs:169 takes `Arc<AtomicU8>`; `should_rollback()` reads it, types.rs:390). `WalFailurePolicy::Rollback` (`as_u8() == 1`) lives at `frogdb-server/crates/persistence/src/wal/config.rs:6,17` and is reachable from the integration-test crate as `frogdb_core::persistence::WalFailurePolicy`. No new API to invent.

- [ ] **Step 2: Run to verify it fails on the F2 typo.** `just test frogdb-core s6_rollback_all_results` — expected FAIL: the assertion `e.starts_with(b"EXECABORT")` fails because the production string is the truncated `"EXECABRT transaction aborted due to WAL failure"` (execution.rs:533).

- [ ] **Step 3: Fix F2.** In `frogdb-server/crates/core/src/shard/execution.rs:533`, change `"EXECABRT transaction aborted due to WAL failure"` → `"EXECABORT transaction aborted due to WAL failure"`. Grep to confirm no test pins the truncated spelling: `grep -rn "EXECABRT" frogdb-server` should be empty after the change.

- [ ] **Step 4: Run to verify green.** `just test frogdb-core s6` — expected PASS.

- [ ] **Step 5: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/shard/execution.rs frogdb-server/crates/core/tests/shard_driver/scenario_s6.rs
git commit -m "fix(core): correct EXECABRT->EXECABORT WAL-rollback error code (F2); pin S6 rollback"
```

---

## Task 14 — S7: CLIENT PAUSE WRITE vs in-flight EXEC (turmoil-level, server crate)

**Goal:** S7 is **not expressible in the shard driver** (the pause gate is purely connection-side pre-dispatch, dispatch.rs:410-419 → lifecycle.rs:500-548; an EXEC in a shard queue is by construction past the gate — brief D4). Re-scoped to a targeted turmoil-level test in `crates/server/tests/`: conn A issues `CLIENT PAUSE WRITE` concurrently with conn B's write-`EXEC`. Invariant: any write-EXEC whose dispatch passes the pause gate after pause engages does not commit until unpause; reads proceed under WRITE mode; expiry suppressed while paused (`expiry_paused`, event_loop.rs:124-133). Small — one test file, reuses the existing sim harness.

**Files touched:**
- `frogdb-server/crates/server/tests/...` — one turmoil test (reuse the existing sim harness; match the crate's test-file convention).

- [ ] **Step 1: Write the S7 turmoil test.** Using the existing sim harness (study a passing `simulation.rs` test for the client API), drive: conn A `CLIENT PAUSE 200 WRITE`; conn B `MULTI; SET k v; EXEC` issued concurrently (turmoil schedule seed controls the interleave); conn C `GET k` (read, must proceed under WRITE-mode pause). Assertions:
  - The write-EXEC does not commit (its effect on `k` is not observable) until the pause elapses / `CLIENT UNPAUSE`.
  - The read from conn C returns while paused (WRITE mode does not block reads).
  - While paused, an expiring key is not swept (observe `expiry_paused` suppression: seed a short-TTL key, keep it paused past the TTL, assert it is still present until unpause). Keep this sub-assertion only if the sim harness exposes a deterministic way to observe it; otherwise assert the two write/read invariants and note the expiry suppression is covered by the existing `run_active_expiry` pause gate unit path.
  - Deterministic via turmoil schedule seeds (loop a handful of seeds).

Keep the test minimal and reuse helpers. Do **not** add new harness infrastructure.

- [ ] **Step 2: Run it.** `cargo nextest run -p frogdb-server --features turmoil -E 'test(/client_pause_write_vs_exec/)' > "$TMPDIR/s7.log" 2>&1` (foreground, 600000ms; `tail`). Expected PASS. On timeout, re-run the same command.

- [ ] **Step 3: Commit.**

```bash
git add frogdb-server/crates/server/tests
git commit -m "test(sim): S7 CLIENT PAUSE WRITE vs in-flight write-EXEC (turmoil-level, connection path)"
```

---

## Task 15 — S8: expiry sweep vs EXEC serialization (pin)

**Goal:** S8 (brief D4/S8). 1 shard. Seed keys with TTLs; permute sequences of [`ExecTransaction` (multi-write), `Execute`, `ExpiryTick`, `WaiterTimeoutTick`]. Invariants: EXEC effects atomic (a subsequent read + `GetVersion` never sees partial transaction state); version monotonic, bumped exactly once per committed EXEC (post_execution.rs:207) and once per non-empty sweep (event_loop.rs:212); keyspace notifications consistent with the chosen order; `MemoryCheck` + `ExpiryIndexCheck` clean at quiesce. Uses the shared schedule generator.

**Files touched:**
- `frogdb-server/crates/core/tests/shard_driver/scenario_s8.rs` (replace stub).

- [ ] **Step 1: Write the scenario.** Replace `scenario_s8.rs`:

```rust
//! S8 — expiry sweep interleaved with EXEC (pin). The shard event loop is a
//! single task; message handling and expiry ticks are separate select arms,
//! each awaited to completion (research scenario 8). Interleaving is
//! message-granularity only: EXEC effects are always atomic and version bumps
//! are exactly-once (per committed EXEC + per non-empty sweep).

use std::time::Duration;

use bytes::Bytes;
use frogdb_protocol::Response;
use proptest::prelude::*;

use crate::generator::{replay, schedule_strategy, Choice, Sender, Step, Tick};
use crate::harness::{cmd, ShardDriver};
use frogdb_core::shard::types::TransactionResult;

/// Deterministic pin: a multi-write EXEC is atomic even with an expiry tick and
/// an unrelated write permuted around it; the version bumps exactly once for
/// the committed EXEC.
#[tokio::test]
async fn s8_exec_atomic_and_version_bumped_once() {
    let mut d = ShardDriver::new(1);
    // Seed a soon-to-expire key and two transaction targets.
    let _ = d.execute(0, "SET", &["a", "0"]).await;
    let _ = d.execute(0, "SET", &["b", "0"]).await;
    let _ = d.execute(0, "SET", &["e", "x"]).await;
    let _ = d.execute(0, "PEXPIRE", &["e", "1"]).await;

    let v_before = d.get_version(0).await;

    // Permuted around the EXEC: an expiry tick (removes e) then EXEC (a,b).
    tokio::time::sleep(Duration::from_millis(3)).await;
    d.tick_expiry(0); // one non-empty sweep → +1

    let result = d
        .exec_transaction(
            0,
            1,
            vec![cmd("SET", &["a", "1"]), cmd("SET", &["b", "1"])],
            vec![],
        )
        .await; // committed EXEC → +1

    assert!(matches!(result, TransactionResult::Success(_)));

    // Version read FIRST — before any GETs — so read-path effects (including
    // the post-F3 lazy-purge bump, which Task 9 lands before this task) cannot
    // contaminate the count. Bumped exactly twice: one non-empty sweep + one
    // committed EXEC.
    let v_after = d.get_version(0).await;
    assert_eq!(v_after, v_before + 2, "expected exactly one sweep bump + one EXEC bump");

    // EXEC effects atomic: both keys reflect the committed values.
    assert_eq!(d.execute(0, "GET", &["a"]).await, Response::Bulk(Some(Bytes::from_static(b"1"))));
    assert_eq!(d.execute(0, "GET", &["b"]).await, Response::Bulk(Some(Bytes::from_static(b"1"))));
    // Expired key gone (already swept — this GET triggers no lazy purge).
    assert_eq!(d.execute(0, "GET", &["e"]).await, Response::Bulk(None));

    // Quiesce probes clean.
    let mem = d.memory_check(0).await;
    assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
    assert!(d.expiry_index_check(0).await.anomalies.is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// Permute [EXEC, Execute, ExpiryTick, WaiterTimeoutTick] via the shared
    /// scheduler. Whatever the order, at quiesce: the EXEC's effects are atomic
    /// (both a and b share the committed value, never one-updated), and the
    /// memory/expiry-index probes are clean.
    #[test]
    fn prop_s8_exec_atomic_under_permutation(schedule in schedule_strategy(2, 1, 10)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut d = ShardDriver::new(1);
            d.execute(0, "SET", &["a", "0"]).await;
            d.execute(0, "SET", &["b", "0"]).await;

            // Sender 0: a multi-write EXEC (a:=9, b:=9). Sender 1: an unrelated
            // single write on c.
            let mut senders = vec![
                Sender::new(vec![Step::ExecTransaction {
                    shard: 0,
                    conn_id: 1,
                    commands: vec![cmd("SET", &["a", "9"]), cmd("SET", &["b", "9"])],
                    watches: vec![],
                }]),
                Sender::new(vec![Step::Execute {
                    shard: 0,
                    conn_id: 2,
                    command: cmd("SET", &["c", "5"]),
                }]),
            ];
            // Keep only expiry/waiter ticks (ContinuationRelease not applicable).
            let sched: Vec<Choice> = schedule
                .into_iter()
                .filter(|c| !matches!(c, Choice::Tick { tick: Tick::ContinuationRelease, .. }))
                .collect();

            replay(&mut d, &mut senders, &sched, 1).await;

            // Atomicity: a and b share the committed transaction value.
            let a = d.execute(0, "GET", &["a"]).await;
            let b = d.execute(0, "GET", &["b"]).await;
            prop_assert_eq!(a, b, "EXEC not atomic: a and b diverged");

            let mem = d.memory_check(0).await;
            prop_assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
            prop_assert!(d.expiry_index_check(0).await.anomalies.is_empty());
            Ok(())
        }).unwrap();
    }
}
```

- [ ] **Step 2: Run tests.** `just test frogdb-core s8` — expected PASS.

- [ ] **Step 3: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/tests/shard_driver/scenario_s8.rs
git commit -m "test(core): S8 expiry sweep vs EXEC serialization pinned (atomicity, exactly-once version)"
```

---

## Task 16 — Spec amendment: harness location, S1/S7 re-scope, EXECABORT spelling note

**Goal:** Record the settled re-scopes in the design spec (brief D4). Exact old→new edits to `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`.

**Files touched:**
- `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`

- [ ] **Step 1: Harness location — integration-test tree with feature-gated seams.** In section "4. Shard-driver harness" (spec :113-122), set the header line:

  New: `### 4. Shard-driver harness (`frogdb-server/crates/core/tests/shard_driver/`, integration-test crate)`

  Append a sentence to that section's first bullet noting the reason: the harness needs `frogdb-commands` (added as a `frogdb-core` dev-dependency) to populate a real `CommandRegistry`, and that dev-dep cycle compiles `frogdb-core` twice — an in-crate `#[cfg(test)]` harness touching both copies trips E0308. The integration test links the single normal build and reaches the seams through **feature-gated public `drive*` wrappers** on `ShardWorker` (and `ShardReceiver::try_recv`), each `#[cfg(any(test, feature = "shard-driver"))] #[doc(hidden)] pub`, with the `shard-driver` feature enabled by the crate's self-dev-dep.

- [ ] **Step 2: S1 re-scope to a pin.** In "Targeted shard-driver scenarios" item 1 (spec :195-197), append: "The lost-element race is already closed (`drive_satisfaction` re-validates deadline + `is_closed()` before consuming); 4b **pins the closure under permutation** rather than hunting a known bug."

- [ ] **Step 3: S7 re-scope to turmoil-level.** In item 7 (spec :206-207), replace the body so it reads: the CLIENT PAUSE gate is purely connection-side pre-dispatch, so an EXEC already in a shard queue is by construction past the gate and the case is **not expressible in the shard driver**. Re-scoped to a targeted **turmoil-level** test in `crates/server/tests/` (pause engaged concurrently with write-EXEC issuance; invariant: no write-containing EXEC issued-after-pause commits before unpause; reads proceed under WRITE mode; `expiry_paused` suppresses sweeps).

- [ ] **Step 4: EXECABORT spelling note.** In item 6 (spec :204-205), note that the WAL-rollback reply code was corrected from the truncated `EXECABRT` to `EXECABORT` (F2) as part of 4b.

- [ ] **Step 5: Commit.**

```bash
git add docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md
git commit -m "docs(spec): 4b re-scopes — in-crate harness, S1 pin, S7 turmoil-level, EXECABORT fix"
```

---

## Task 17 — Justfile wiring decision + final full-suite verification

**Goal:** Decide whether to wire the shard-driver proptests into `just concurrency`, then run the full verification and state expected-green.

**Justfile decision (state in-plan and implement):** the shard-driver scenarios are plain `#[tokio::test]` / proptest unit tests in `frogdb-core` — they are **already covered by `just test frogdb-core`** (which `just test-all` runs via `just test`). The `concurrency:` recipe targets the shuttle + turmoil sweeps specifically. **Decision:** do **not** add the frogdb-core shard-driver tests to `concurrency:` (they are not shuttle/turmoil sweeps and would duplicate `just test`); the two new **turmoil** tests (F1/F3 real-path repros, S7) are picked up by the existing `simulation`/pattern coverage only if their names match — so add a single line to `concurrency:` that runs the new server-crate turmoil tests by a shared name pattern **iff** runtime stays sane. Verify their combined runtime is well under the per-PR budget before adding; if they exceed it, leave them to `just test-all`'s server-crate run and note that here.

**Files touched:**
- `Justfile` (only if the runtime check passes).

- [ ] **Step 1: Measure the new turmoil tests' runtime.** `cargo nextest run -p frogdb-server --features turmoil -E 'test(/watch_lazy_expiry|xreadgroup_ttl_no_nogroup|client_pause_write_vs_exec/)' > "$TMPDIR/newturmoil.log" 2>&1` (foreground, 600000ms). Record wall time. If comfortably under ~30s combined, proceed to Step 2; else skip Step 2 and note the deferral.

- [ ] **Step 2 (conditional): Wire into `just concurrency`.** If the runtime is sane, add after the existing sweep lines in the `concurrency:` recipe (Justfile :74-78):

```make
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/watch_lazy_expiry|xreadgroup_ttl_no_nogroup|client_pause_write_vs_exec/)'
```

- [ ] **Step 3: Final verification (foreground, 600000ms, log to `$TMPDIR`, re-run same command on timeout).**

```bash
just test frogdb-core > "$TMPDIR/final_core.log" 2>&1 ; tail -30 "$TMPDIR/final_core.log"
```

Expected: green (all `shard_driver` scenarios, F1–F4 fixes, seam + generator + sink tests).

```bash
just test frogdb-server > "$TMPDIR/final_server.log" 2>&1 ; tail -30 "$TMPDIR/final_server.log"
```

Expected: green (note: plain `just test frogdb-server` does not compile the `#[cfg(feature = "turmoil")]` tests; those run under the turmoil command below).

```bash
just concurrency > "$TMPDIR/final_conc.log" 2>&1 ; tail -40 "$TMPDIR/final_conc.log"
```

Expected: green (shuttle + turmoil sweeps + the two/three new turmoil tests if wired).

```bash
just lint && just fmt
```

Expected: clean (no clippy warnings; formatter idempotent).

- [ ] **Step 4: Commit (only if the Justfile changed).**

```bash
git add Justfile
git commit -m "test(sim): wire 4b real-path turmoil scenarios into just concurrency"
```

If the Justfile was not changed (runtime-deferral path), this task lands no commit — record the decision in the phase ledger note instead.

---

## Honest Scoping Notes

Per brief D7, 4b = harness + 8 (re-scoped) scenarios + F1–F4 + spec amendment. The following are **explicit non-goals**, deferred (recorded here verbatim from the brief, and in the 4a ledger / memory):

- live-id XACK/XCLAIM threading
- extended XPENDING/delivery counts
- per-op FIFO time-window matching
- bzpopmin/bzpopmax exact-FIFO extension
- UNWATCH-inside-MULTI divergence pin
- XREADGROUP-after-XDEL encoding
- group-reply binary-safety
- cross-shard WATCH validation via VLL-EXEC

- Rollback-mode WAL partial-append inconsistency: a mid-transaction WAL failure leaves
  earlier same-transaction records already appended while the store rolls back — a
  recovery-replay inconsistency candidate. Durability phase; S6 only asserts no
  post-failure appends.

Additional structural non-goals confirmed by 4b research (not bugs, not in scope):
- Same-slot-but-cross-internal-shard EXEC is structurally unreachable in standalone mode (same slot ⟹ same shard) — deferred single-shard-vs-VLL EXEC distinction beyond S3's sparse-participant scatter coverage.
- Persistence real path (RocksDB/FrogDB WAL) — fake sink only; durability phase later.
