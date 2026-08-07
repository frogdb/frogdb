# VLL (Very Lightweight Locking) — residual test gaps (15 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/12 — residual findings after promotion to issues 19–76
Score: 15 findings, priority range 11–19
Area: `frogdb-server/crates/vll/` (`coordinator.rs`, `shard.rs`, `lock_table.rs`, `queue.rs`, `traits.rs`, `types.rs`) and `frogdb-server/crates/server/src/vll_adapter.rs`

## Context

VLL is the atomicity mechanism for every cross-shard multi-key command and for cross-shard
Lua, so its failure modes are consistency and availability violations by construction. The
crate is 2.2k LOC over 6 modules with 4 inline `#[cfg(test)]` modules (25 tests total), **no
`tests/` dir**, no `dev-dependencies` beyond `tokio` (no proptest, no shuttle), and 90.6 %
lines; depth over 148 fn records is `untested` 14, `single-test` 68, `monoculture` 30,
`well-covered` 35, `covered` 1, and every untested/single-test *production* function sits on a
lock-release or continuation path. The proposal's verdict on the shape of that coverage: "The
line percentage is misleading: 90.6% is achieved almost entirely by 25 inline tests driving a
`TestSink` mock that replies instantly and never reorders, so what is covered is the *happy
sequence*, not the protocol." Its bottom line: "The bug that escapes today is exactly the one
that matters most: a lock left behind on an abort, timeout, or fault path, wedging a shard
permanently — no test in the repo would notice."

A reachability caveat applies to every scatter/continuation finding below and is the reason
none of them exceeds Likelihood 3: `allow_cross_slot_standalone` defaults to `false`
(`config/src/server.rs:41`, `#[param(skip)]` — startup-fixed), and
`shard_for_key(key) = crc16(tag) % 16384 % num_shards` co-locates same-slot keys, so cluster
mode never scatters — on the non-default config that turns the feature on, the traffic is
ordinary, not adversarial.

## Promoted elsewhere

- F2 → issue 50, `.scratch/testing-improvements-round2/issues/` (MULTI/EXEC executes while another connection holds a continuation lock — `CoreMsg::ExecTransaction` never calls `can_execute_during_lock`)
- F4 → issue 33, `.scratch/testing-improvements-round2/issues/` (`test_mset_full_atomicity_sharded` passes when every reply is an error — `MASTER.md` §4, tests that cannot fail)
- F5 → issue 64, `.scratch/testing-improvements-round2/issues/` (acquiring a continuation lock stalls the whole shard for 2 s and then always fails)

One further item from this proposal is owned elsewhere but is **not** a numbered finding, so it
is not counted in the arithmetic above: `core/tests/concurrency.rs:641
test_mset_cross_shard_partial_visibility` asserts that partial visibility *is* acceptable,
contradicting VLL's contract. Proposal 12 raises it only as a `## Deprioritised` bullet plus a
`## Cross-area notes` entry — explicitly "Not a VLL gap" because it runs against a mock
`TestCluster` and not VLL at all — and `MASTER.md` §4 claims it for issue 33,
`.scratch/testing-improvements-round2/issues/` (rename or delete). Nothing to do here.

## Residual findings

### F1 — the lock-table leak detector is never pointed at any VLL operation

- **Severity** 5 — a leaked intent or continuation lock wedges a shard permanently: every later command on those keys blocks or gets `BUSY` until restart. This is the canonical VLL failure and it is silent.
- **Likelihood** 3 — leaks arise on abort/timeout/fault paths, which the chaos tests already provoke; nothing checks for the residue afterwards.
- **Effort** 2 — the detector, the probe, and the runner all already exist; add scatter ops to a workload profile and call the existing probe after the existing chaos tests.
- **Priority** 19
- **Evidence**: `crates/testing/src/quiescence.rs::check_locktable_empty` flags `intent_key_count > 0 || continuation_lock_held`, consumed by `crates/server/tests/concurrency_workload.rs::quiescence_stage_runs_and_is_clean` via `common/quiescence_probe.rs`. But `crates/testing/src/workload.rs` profiles are `TxHeavy | BlockingHeavy | MultiWaiter | Mixed` and emit no MGET/MSET/DEL/UNLINK, so no scatter ever runs under the checker. Independently, none of the turmoil scatter chaos tests (`simulation.rs:1489`, `test_shard_unavailable_scatter_gather_atomicity`, `test_partial_failure_error_shard`, `test_partition_mid_mset`) issues `DEBUG LOCKTABLE` after the fault. `abort_pending` is `single-test`, `abort_shards` `monoculture` — both only against an instant mock.
- **Proposed test**: (a) add a `ScatterHeavy` profile emitting cross-shard MGET/MSET/DEL with `allow_cross_slot_standalone=true`, run it through the existing workload runner, and assert `check_locktable_empty` on **every** shard at quiescence; (b) append the same `DEBUG LOCKTABLE`-empty assertion to each existing turmoil scatter-fault test after `sim.run()`, asserting `intent_key_count == 0 && !continuation_lock_held` per shard.
- **Boundary**: 4 — the probe is `DEBUG LOCKTABLE` over RESP and the faults are injected at the server; no lower layer exposes the aggregate.
- **Note**: the proposal calls the `ScatterHeavy` profile "the single cheapest high-severity item in the audit"; it is filed as issue 07, `.scratch/testing-improvements-round2/issues/`. The quiescence checker and probe need no changes.

### F3 — the continuation-lock feature has zero coverage above the shard

- **Severity** 5 — this is the atomicity mechanism for cross-shard scripts and it freezes N shards while held. Untested code that freezes shards is the highest-consequence shape in the crate; F5 argues it is in fact broken today.
- **Likelihood** 3 — any cross-shard `EVAL` on the enabling config takes this path; there is no other path.
- **Effort** 3 — `TestServer` + the existing `allow_cross_slot_standalone` harness knob.
- **Priority** 18
- **Evidence**: `server/src/connection/scripting/eval.rs:119 execute_cross_shard_script` — **`untested`, tests=0, regions 0/0**. `server/src/vll_adapter.rs:137 send_continuation_lock` — **`untested`, tests=0**. `classify_script_shards` (eval.rs:65) is `well-covered` (162 tests) yet its `ScriptShards::CrossShard` arm (line 80) is never taken, which is exactly how a well-covered classifier can sit in front of a dead branch. `grep -l allow_cross_slot_standalone crates/server/tests` returns only transactions/client/metrics/persistence files — no EVAL test. `handle_vll_continuation_lock` (`core/src/shard/vll.rs:67`) is `single-test` (shard_driver S4), and S4 asserts only that *other* connections are excluded — never that the owner can complete.
- **Proposed test**: with `allow_cross_slot_standalone=true` and ≥2 shards, `EVAL` a script whose KEYS span two shards and which does `redis.call('SET', KEYS[1], ...)` and `redis.call('GET', KEYS[2])`. Assert: the script returns the correct value (proving the owner's remote sub-commands pass `can_execute_during_lock`), both writes are visible after, and `DEBUG LOCKTABLE` shows `continuation_lock_held=false` on every shard afterwards.
- **Boundary**: 4 — the path starts at EVAL parsing/classification in the connection layer; nothing below it can invoke `execute_cross_shard_script`.
- **Note**: the proposal's Deprioritised list folds one further assertion into this test rather than filing it separately — "Script sub-commands self-deadlocking against their own continuation lock — checked and safe: `dispatch_scripting.rs:17,42,104` all pass the script's own `conn_id` to `can_execute_during_lock`, which admits the owner (`worker.rs:806`). Worth an assertion, but it is already folded into F3's proposed test rather than filed separately." Keep that assertion in the same test.

### F9 — unknown-txid `VllExecute` returns an empty *success*, not an error

- **Severity** 5 — the fallback is success-shaped: MGET renders the missing keys as nils and MSET reports OK for writes that never happened. Any ordering regression upstream converts into a silent wrong answer / lost write rather than a loud failure.
- **Likelihood** 2 — the current coordinator never sends `VllExecute` after an abort for the same txid, so this needs a coordinator bug or a channel-ordering violation to reach; that is exactly the class of bug the rest of this audit says is untested.
- **Effort** 2 — one `shard_driver`/crate-level test.
- **Priority** 17
- **Evidence**: `core/src/shard/vll.rs::handle_vll_execute` — on a txid missing from the queue it does `let _ = response_tx.send(PartialResult::default()); return;` with no error and no metric. `crates/server/src/scatter/executor.rs:124-137` then merges the empty `into_keyed_results()` straight into the client reply. There is no `VllError` variant for "unknown txid" and no test sends `VllExecute` for a txid that was never enqueued.
- **Proposed test**: send `VllExecute` for an unknown txid and assert the shard's reply is distinguishable from a legitimate empty result (a dedicated error or a flagged `PartialResult`), and that the scatter executor turns it into an error reply rather than nils/OK. Add the abort-then-execute ordering case too.
- **Boundary**: 3 — the shard's message handling is the unit under test; the executor mapping can be a level-2 assertion on top.

### F7 — no deterministic interleaving / model coverage of the VLL protocol (shuttle assessment)

- **Severity** 5 — every escaped defect in this crate is a consistency or liveness violation; the failure modes (F5, F6, F9, F11) are all interleaving-dependent and all invisible to example tests.
- **Likelihood** 3 — same config gate as everything else.
- **Effort** 5 — new infrastructure: a message-schedule explorer plus the invariant oracles.
- **Priority** 16

**The explicit tool evaluation the dispatch asked for:**

- **loom is the wrong tool.** `grep` for `AtomicU*`/`UnsafeCell`/`Mutex` inside `crates/vll` returns nothing: `VllShardState` and `LockTable` are plain `&mut self` state machines owned by a single task, and all cross-task communication is `tokio::sync::{mpsc, oneshot}`. There is no memory-ordering surface for loom to explore, and loom cannot model tokio channels or timers anyway.
- **shuttle is the right tool, with a caveat.** The real nondeterminism is (a) the arrival order of `VllLockRequest` / `VllExecute` / `VllAbort` / `VllContinuationLock` across the per-shard mpsc queues, (b) the relative progress of two or more coordinator tasks, and (c) timer firing. shuttle explores exactly (a) and (b) by scheduling futures deterministically; (c) needs the timeouts to be modelled as a schedulable event rather than wall-clock, so the model must either use `tokio::time::pause`-style virtual time or treat "timeout fires" as an explicit injectable message. That is the one piece of new infrastructure.
- **Model boundary**: N in-process `VllShardState` instances each driven by a task reading a bounded queue, plus M `VllCoordinator` tasks over a sink that pushes into those queues. Real `vll` code, **no** `ShardWorker`, no store, no commands — the operations are opaque tokens. Keeping the store out is what makes the state space tractable; the store-level property ("no partial writes") is already covered by `scenario_s3` at level 3.
- **Invariants the model must assert**, on every schedule:
  1. **No leaked state** — at quiescence every `LockTable` is empty and no `continuation_lock_held` is set, for *every* terminal outcome (success, abort, lock failure, both timeouts, sink error).
  2. **Bounded acquisition / no deadlock** — no schedule exists in which two transactions are each blocked on the other with no timer pending; equivalently, with timeouts disabled, every transaction eventually terminates.
  3. **All-or-nothing per participant** — for each txid, either every participant received `VllExecute`, or none did after any participant was aborted; never both `VllExecute` and `VllAbort` for the same (txid, shard).
  4. **Exactly-once disposition** — each participant that received a lock request is eventually either executed or aborted, exactly once (the VLL analogue of round-1 issue 07's exactly-once waiter guard).
  5. **Continuation exclusion** — while a continuation lock is held by conn C on shard S, no other connection's operation executes on S; and the owner's own operations do.
  6. **No lost wakeup** — a transaction blocked on a key is granted within a bounded number of steps after the blocking holder releases.
- **Proposed test**: `crates/vll/tests/shuttle_protocol.rs` (new `tests/` dir, `shuttle` as a dev-dependency behind the same feature flag `crates/testing` already uses for issue 07), running the six invariants over 2–3 shards and 2–3 concurrent transactions with bounded exploration in CI and a longer nightly budget.
- **Boundary**: 2 — the crate's public API is already sink-abstracted (`ShardSink`, `MetricsSink`), which is precisely the seam a model needs; going to level 3 would drag the store and command registry into the state space for no added invariant.
- **OPTIONS**:
  - *A — shuttle at level 2 (recommended)*: cheapest state space, reuses the existing `TestSink` shape, catches F5/F6/F9/F11 classes. Does **not** cover the shard event loop, so F5's specific "the drain loop blocks its own drain" bug needs the level-3 test in F5 too.
  - *B — extend the existing `shard_driver` permuted-pump generator* (`core/tests/shard_driver/generator.rs`) to enumerate message orderings over real workers. Reuses existing infrastructure and covers the event loop, so it would catch F5; but the state space includes the whole store, so exploration is shallow and it cannot prove absence.
  - *C — turmoil at level 4*: already exists, already used for scatter chaos; good for end-to-end fault shapes but its scheduling is randomised, not exhaustive, so it cannot establish invariant 2.
  - **Recommendation: A as the primary tool, plus the targeted level-3 test in F5.** B is the fallback if adding a shuttle dependency to `vll` is unwanted; C stays as it is but gains the quiescence assertions from F1.
- **Cross-reference**: the "targeted level-3 test in F5" the recommendation depends on is owned by issue 64, `.scratch/testing-improvements-round2/issues/`; this issue owns the model, not that test. The virtual-time/injectable-timeout primitive the caveat calls for is issue 08, same directory — the proposal notes round-1 issue 07 already established shuttle in `crates/testing` for the MultiWaiter exactly-once guard, so only the timeout primitive is new, and "if another area also wants deterministic timeout exploration, build it once and share it."

### F6 — the lock table admits cross-shard wait-for cycles; deadlock freedom is only a comment

- **Severity** 4 — two cross-shard transactions can deadlock and are broken only by the 4 s timeout, during which the participating keys are locked and dependent traffic queues; both clients then get errors.
- **Likelihood** 3 — an MGET and an MSET over the same two keys is the ordinary contention pattern for this feature.
- **Effort** 3 — two concurrent coordinators over real shard workers; `shard_driver` supports this shape.
- **Priority** 15
- **Evidence**: `crates/vll/src/lock_table.rs:90-123 try_grant` applies two rules — (1) SCA: conflicting *lower-txid* intents block; (2) **holders: any conflicting *granted* intent from another txn blocks regardless of txid order**. Rule (2) deliberately breaks the total order that rule (1) establishes, so txn 7 granted on shard A can block txn 5 there while txn 5 granted on shard B blocks txn 7 — a genuine cycle across shards. The single-shard ingredient is already a named test (`lower_txid_writer_blocked_by_granted_higher_reader`, `lock_table.rs:284`) but is never composed across shards. Deadlock freedom appears only as prose: `coordinator.rs:8-11` ("send … in sorted order (deadlock prevention)") and `coordinator.rs:52-54` / the `acquire_continuation_and_run` doc ("`shards` must be sorted in ascending order to prevent deadlocks"). No test asserts it; `try_grant` is `well-covered` (11 tests) purely single-shard.
- **Proposed test**: two shard workers, keys `a`(shard 0) and `b`(shard 1). Txn 5 = MSET over `[a,b]`, txn 7 = MGET over `[a,b]`, driven concurrently with the pump ordered so that txn 7 is granted on shard 0 first and txn 5 on shard 1 first. Assert both transactions resolve within ≪ `DEFAULT_LOCK_ACQUISITION_TIMEOUT` (i.e. one is aborted promptly rather than both timing out), and that lock tables are empty afterwards. Best expressed as the "bounded acquisition" invariant of F7's model rather than as a one-off example.
- **Boundary**: 3 — real lock tables and real workers, deterministic pumping; a server test could not reliably force the interleaving.

### F8 — the ascending-shard-order deadlock-prevention convention is unenforced and untested

- **Severity** 4 — the *only* thing standing between the current design and cross-transaction deadlock on the continuation path is that all callers dispatch in ascending shard order; it is a comment, not a type, not an assertion.
- **Likelihood** 2 — today's two callers happen to comply, but by accident of a container type rather than by construction, so any refactor silently breaks it.
- **Effort** 1 — a pure unit test on the participant-building code plus a `debug_assert` in the coordinator.
- **Priority** 15
- **Evidence**: `coordinator.rs:52-54` documents the convention as a caller obligation ("callers should sort by shard id") and `acquire_continuation_and_run`'s doc says "`shards` must be sorted in ascending order to prevent deadlocks", but neither `scatter` nor `acquire_continuation` checks it. `server/src/scatter/executor.rs:87-99` builds participants by iterating `partition.shard_keys`, which is ordered only because `PartitionResult::shard_keys` is declared `BTreeMap<usize, Vec<Bytes>>` — change it to a `HashMap` for speed and the ordering guarantee evaporates with no test failing.
- **Proposed test**: (a) unit test asserting `ScatterGatherExecutor`'s participants come out strictly ascending by `shard_id` for a key set that hashes out of order, and that `classify_script_shards` returns an ascending, deduplicated `Vec<usize>`; (b) a `debug_assert!(shards.is_sorted())` equivalent in `scatter`/`acquire_continuation` so the existing test suite enforces it everywhere.
- **Boundary**: 1 — a pure ordering property of a pure function; no runtime needed.

### F10 — `await_continuation_release` cancel-safety is asserted nowhere

- **Severity** 4 — if a refactor replaces the in-place `&mut rx` poll with a `take()`, a lost select! race drops the release signal and the shard's continuation lock is held **forever**: permanent unavailability for every other connection on that shard, recoverable only by restart.
- **Likelihood** 2 — requires a refactor, but the property is subtle enough to be an easy mistake and there is no test guarding it.
- **Effort** 1 — a focused unit test on the shard state.
- **Priority** 15
- **Evidence**: `crates/vll/src/shard.rs:275 await_continuation_release` is nominally `well-covered` (3743 tests) but essentially all of those hits are the `None => std::future::pending()` branch taken by every idle shard on every event-loop turn — a textbook coverage illusion. `clear_continuation_lock` (`shard.rs:287`) is `single-test`. The cancel-safety claim in the doc comment ("if the surrounding select! fires another arm first, the release receiver is preserved") relies on `impl Future for &mut F` and is never exercised by a test that actually loses the race.
- **Proposed test**: acquire a continuation lock; race `await_continuation_release` against a ready branch in a `select!` so the other arm wins; then drop the sender and assert the release is still observed on the next poll and `has_continuation_lock()` becomes false.
- **Boundary**: 1 — pure state-machine property of `VllShardState`.

### F11 — `LockTable` has no property test for its own invariants

- **Severity** 4 — the table is the ground truth for mutual exclusion; a grant bug is a direct isolation violation.
- **Likelihood** 2 — the 10 example tests cover the obvious matrix; what they cannot cover is arbitrary interleavings of declare/grant/release across many txns and keys.
- **Effort** 1 — `proptest` on a pure data structure, no async.
- **Priority** 15
- **Evidence**: `crates/vll/Cargo.toml` has no `proptest` dev-dependency and the crate has no `tests/` dir. The 10 tests in `lock_table.rs:185-343` are all fixed 2–3 txn scenarios. `duplicate_keys_in_slice_are_idempotent` shows the authors already found one aliasing edge by hand — the kind of thing a property test finds exhaustively.
- **Proposed test**: proptest over random sequences of `(txid, keys, mode)` declare / try_grant / release, asserting after every step: no two conflicting granted intents coexist on a key; a granted intent's keys are all granted (all-or-nothing); release is idempotent and never releases another txn's intent; the table is empty once every txn has released; and `lock_state_string` reflects grants, not intents.
- **Boundary**: 1 — pure synchronous data structure.

### F12 — the gather phase never aborts on timeout, so a slow shard can commit after the client got an error

- **Severity** 4 — the client is told the operation failed while some or all shards go on to apply it: a phantom write, and durably so if it reaches the WAL.
- **Likelihood** 2 — needs a shard slow enough to exceed the gather timeout, i.e. the overload/stall case.
- **Effort** 2 — the `FaultSink` used by `scenario_s3` can already stall a participant.
- **Priority** 14
- **Evidence**: `crates/vll/src/coordinator.rs` phase 4 — both the `Ok(Err(_))` and `Err(_)` arms return `ScatterError::{ResultChannelClosed,ResultTimeout}` **without calling `abort_shards`**, on the documented assumption that "participants that already received `VllExecute` release their own locks when execution completes". That assumption is sound for locks but says nothing about the write becoming visible after the error reply, and it is false for liveness if the shard is wedged. Neither arm is covered: the 8 coordinator unit tests use an instantaneous `TestSink`, and `ScatterError`'s `Display` (`coordinator.rs:88`) is `untested`, confirming no test ever constructs these variants.
- **Proposed test**: stall one participant past the gather timeout; assert the client sees `ERR VLL execution timeout`, then let the shard finish and assert the documented outcome — either the write is visible (and the test pins that as the contract) or it is not. Then assert lock tables are empty on all shards. Today neither is stated anywhere.
- **Boundary**: 3 — `shard_driver` + `FaultSink`; this is `scenario_s3` with a stall instead of a drop.
- **OPTIONS**:
  - *3*: deterministic, cheap, but asserts visibility through the shard rather than the client.
  - *4 (turmoil, `shard_delays_ms` already exists in `vll_adapter.rs:89`)*: asserts the actual client-visible contradiction (error reply + present key). Slower, and the delay must be tuned past the 4 s floor.
  - **Recommendation: 3 for the mechanism, and fold the client-visible half into F1's quiescence assertions on the existing `test_asymmetric_per_shard_delays`.**

### F13 — per-shard sequential timeouts mean the effective deadline is N × timeout

- **Severity** 3 — a client can be held for `2 × N × timeout` (16 shards × 4 s × 2 phases ≈ 128 s) on a single MGET when shards are stalled; connection and client-side timeouts fire long before, and the operator's configured timeout bears no relation to observed latency.
- **Likelihood** 3 — any stalled or heavily contended shard set.
- **Effort** 1 — coordinator unit test with a sink that never replies.
- **Priority** 14
- **Evidence**: `coordinator.rs` phase 2 applies `tokio::time::timeout(request.timeout, ready_rx)` **inside** the per-shard `for` loop, as does phase 4, as does `acquire_continuation` (`coordinator.rs:392-407`). There is no overall deadline. No test has ever taken a timeout arm at all (see F12 evidence).
- **Proposed test**: `TestSink` that never sends any `Ready`; assert `scatter` over 8 participants returns `LockTimeout` in ≈`timeout`, not ≈`8 × timeout`. Same for `acquire_continuation`.
- **Boundary**: 1 — pure coordinator logic over the existing `TestSink`, using `tokio::time::pause()` so the test is instant.

### F15 — duplicate-txid enqueue silently overwrites the previous operation

- **Severity** 4 — the overwritten op's `ready_tx` is dropped (its coordinator sees `LockChannelClosed`) and, worse, its already-declared intents are orphaned in the lock table with no owner to release them — a permanent shard wedge.
- **Likelihood** 1 — `next_txid()` is a process-global `AtomicU64` (`server/src/server/util.rs:20,28`), so a collision requires a restart-with-reuse or a future per-connection txid scheme.
- **Effort** 1 — one unit test.
- **Priority** 13
- **Evidence**: `crates/vll/src/queue.rs::enqueue` does `self.pending.insert(op.txid, op)` and returns `Ok(())`, discarding any previous entry; `test_queue_ordering`/`test_queue_capacity` never enqueue the same txid twice. Note the txid space is *not* persisted across restart, so the "restart-with-reuse" case is real if a client's in-flight txid survives — worth stating explicitly in the test.
- **Proposed test**: enqueue txid 5 twice; assert the second is rejected (`VllError::Internal`/a new variant) rather than silently replacing, and that the first op's intents are still owned and releasable.
- **Boundary**: 1 — pure queue data structure.

### F16 — continuation-lock acquisition is never fault-injected

- **Severity** 4 — a shard that is unavailable or slow *during* continuation acquisition leaves the earlier shards locked until the guard drops; the partial-acquire release path (`coordinator.rs:381-388, 396-406`) is subtle (it relies on dropping `release_txs` to signal shards that already armed their receivers) and is only exercised by one mock unit test.
- **Likelihood** 2 — needs the enabling config plus a shard fault.
- **Effort** 3 — the chaos plumbing exists but is not wired to this sink.
- **Priority** 13
- **Evidence**: `server/src/vll_adapter.rs:40-47` — `ShardSenderSink::new` sets `chaos: None`, and `eval.rs:130` constructs the sink with `new`, so `is_shard_unavailable`/`get_shard_error`/`shard_delays_ms` never apply to continuation acquisition even under turmoil. The only coverage is `acquire_continuation_releases_partially_acquired_on_failure` (`coordinator.rs:707`, `single-test`, mock sink).
- **Proposed test**: extend `ShardSenderSink::with_chaos` to the EVAL path (or add a `shard_driver` equivalent) and assert that when shard 2 of `[0,1,2]` is unavailable, shards 0 and 1 report `continuation_lock_held=false` immediately afterwards and remain serviceable.
- **Boundary**: 3 — `shard_driver` gives deterministic control of which shard fails; level 4 only if the chaos config is extended instead.
- **Note**: the proposal's cross-area notes flag that unifying the two sinks is a decision for whoever owns the turmoil chaos config — "`vll_adapter.rs`'s chaos hooks apply only to the scatter sink (`with_chaos`), never to the continuation sink (`new`)". The `shard_driver` variant needs no such decision and is the safe default.

### F14 — `scatter_gather_timeout_ms` below 4000 is silently ignored

- **Severity** 3 — a live-mutable, operator-facing timeout that does not take effect is a configuration lie; the operator who lowers it to protect tail latency gets 4 s anyway.
- **Likelihood** 3 — `#[param(mutable)]`, so it is expected to be tuned at runtime; the default (5000) is above the floor, so the clamp only bites operators who deliberately lower it — exactly the ones who care.
- **Effort** 3 — `CONFIG SET` + a stalled shard through the server harness.
- **Priority** 12
- **Evidence**: `crates/server/src/scatter/executor.rs:115` — `timeout: self.timeout.max(DEFAULT_LOCK_ACQUISITION_TIMEOUT)` with `DEFAULT_LOCK_ACQUISITION_TIMEOUT = 4000 ms` (`vll/src/coordinator.rs:33`), versus `DEFAULT_SCATTER_GATHER_TIMEOUT_MS = 5000` and `#[param(mutable)] scatter_gather_timeout_ms` (`config/src/server.rs:44-46, 90`). No test sets the value below 4000.
- **Proposed test**: `CONFIG SET scatter-gather-timeout-ms 200`, stall a shard, assert the error arrives in ≪ 4 s (or, if the floor is intentional, assert `CONFIG SET` rejects values below it — a much better contract than silent clamping).
- **Boundary**: 4 — the config plumbing and `CONFIG SET` live at the server.

### F17 — no fairness/starvation coverage under sustained contention

- **Severity** 3 — a writer behind a continuous stream of readers is repeatedly denied by the holders rule and eventually returns a client error after the 4 s timeout; the workload looks healthy while one command class never completes.
- **Likelihood** 2 — needs sustained read contention on the same keys.
- **Effort** 2 — crate-level with many transactions.
- **Priority** 11
- **Evidence**: `lock_table.rs:90-123` — rule (2) blocks a waiting writer whenever *any* conflicting granted reader exists, and nothing ages or reserves for the waiter. `multiple_readers_share_then_writer_waits` (`lock_table.rs:259`) shows the writer waiting but with a finite reader set, so it never tests whether the writer can be starved indefinitely. No metric distinguishes "timed out due to starvation" from "timed out due to a stalled shard".
- **Proposed test**: drive a continuous stream of overlapping readers on key K and one writer; assert the writer is granted within a bounded number of reader arrivals. Naturally expressed as model invariant 6 in F7.
- **Boundary**: 2 — crate-level over `LockTable`/`VllShardState`; no server needed.

### F18 — client-visible VLL error strings are asserted nowhere

- **Severity** 2 — clients and retry loops key on `BUSY` vs `ERR`; a reshuffle silently changes the retry contract (`BUSY` is retriable, `ERR` is not).
- **Likelihood** 3 — every VLL failure path produces one of these.
- **Effort** 1 — table-driven unit test.
- **Priority** 11
- **Evidence**: `coordinator.rs:88 Display for ScatterError` and `coordinator.rs:154 Display for ContinuationError` are both `untested` (tests=0), and `executor.rs:140-192 scatter_error_to_response` — which maps `ShardBusy` to `"BUSY shard busy with continuation lock; retry"` and everything else to `"ERR ..."` — has no test asserting the mapping. `VllError`'s `Display` (`types.rs:41`) is likewise `untested`.
- **Proposed test**: table-driven mapping test over every `ScatterError`/`ContinuationError` variant asserting the exact response prefix, with `ShardBusy` explicitly pinned to `BUSY`.
- **Boundary**: 2 — pure mapping function; a server test would add nothing.

## Acceptance criteria

- [ ] F1: a `ScatterHeavy` workload profile emitting cross-shard MGET/MSET/DEL under `allow_cross_slot_standalone=true` runs through the existing workload runner and asserts `check_locktable_empty` on **every** shard at quiescence; and each existing turmoil scatter-fault test (`simulation.rs:1489`, `test_shard_unavailable_scatter_gather_atomicity`, `test_partial_failure_error_shard`, `test_partition_mid_mset`) asserts `intent_key_count == 0 && !continuation_lock_held` per shard after `sim.run()`.
- [ ] F3: a test with `allow_cross_slot_standalone=true` and ≥2 shards `EVAL`s a script whose KEYS span two shards and asserts the script returns the correct value, both writes are visible afterwards, and `DEBUG LOCKTABLE` reports `continuation_lock_held=false` on every shard — including an assertion that the script's own sub-commands are admitted by `can_execute_during_lock`.
- [ ] F9: a test asserts that `VllExecute` for a txid that was never enqueued yields a reply distinguishable from a legitimate empty result, and that the scatter executor renders it as an error rather than nils/OK; plus the abort-then-execute ordering case.
- [ ] F7: `crates/vll/tests/shuttle_protocol.rs` runs all six invariants — no leaked state at quiescence for every terminal outcome; bounded acquisition / no deadlock; all-or-nothing per participant; exactly-once disposition; continuation exclusion (including that the owner's own operations execute); no lost wakeup — over 2–3 shards and 2–3 concurrent transactions, bounded in CI and with a longer nightly budget.
- [ ] F6: a two-shard test drives txn 5 (MSET over `[a,b]`) and txn 7 (MGET over `[a,b]`) with the pump ordered so txn 7 is granted on shard 0 first and txn 5 on shard 1 first, and asserts both resolve in ≪ `DEFAULT_LOCK_ACQUISITION_TIMEOUT` with empty lock tables afterwards.
- [ ] F8: a test asserts `ScatterGatherExecutor`'s participants come out strictly ascending by `shard_id` for a key set that hashes out of order and that `classify_script_shards` returns an ascending, deduplicated `Vec<usize>`; and `scatter`/`acquire_continuation` carry a `debug_assert!(shards.is_sorted())` equivalent.
- [ ] F10: a test races `await_continuation_release` against a ready `select!` arm that wins, then drops the sender, and asserts the release is still observed on the next poll and `has_continuation_lock()` becomes false.
- [ ] F11: a proptest over random `(txid, keys, mode)` declare/try_grant/release sequences asserts after every step that no two conflicting granted intents coexist on a key, a granted intent's keys are all granted, release is idempotent and never releases another txn's intent, the table empties once every txn has released, and `lock_state_string` reflects grants rather than intents.
- [ ] F12: a test stalls one participant past the gather timeout, asserts the client sees `ERR VLL execution timeout`, pins the post-stall visibility outcome as the stated contract, and asserts lock tables are empty on all shards.
- [ ] F13: a test with a `TestSink` that never sends `Ready` asserts `scatter` over 8 participants returns `LockTimeout` in ≈`timeout`, not ≈`8 × timeout`, and the same for `acquire_continuation`.
- [ ] F15: a test asserts enqueuing txid 5 twice rejects the second enqueue rather than silently replacing, and that the first op's intents remain owned and releasable.
- [ ] F16: a test asserts that when shard 2 of `[0,1,2]` is unavailable during continuation acquisition, shards 0 and 1 report `continuation_lock_held=false` immediately afterwards and remain serviceable.
- [ ] F14: a test asserts `CONFIG SET scatter-gather-timeout-ms 200` followed by a stalled shard produces the error in ≪ 4 s — or, if the 4 s floor is intentional, that `CONFIG SET` rejects values below it rather than silently clamping.
- [ ] F17: a test drives a continuous stream of overlapping readers on key K plus one writer and asserts the writer is granted within a bounded number of reader arrivals.
- [ ] F18: a table-driven test asserts the exact response prefix for every `ScatterError`/`ContinuationError` variant through `scatter_error_to_response`, with `ShardBusy` pinned to `BUSY`.

## Depends on

- **Issue 07**, `.scratch/testing-improvements-round2/issues/` (I7 — `ScatterHeavy` workload profile in `testing/src/workload.rs`). F1 cannot run the existing leak detector against any VLL operation until a profile emits cross-shard MGET/MSET/DEL; the quiescence checker and probe themselves need no changes.
- **Issue 08**, `.scratch/testing-improvements-round2/issues/` (I8 — virtual-time / injectable-timeout primitive for shuttle). F7's model must schedule timer firing rather than observe wall-clock; without it, invariant 2 (bounded acquisition / no deadlock) cannot be established. shuttle itself already exists in `crates/testing` from round-1 issue 07 — only the timeout primitive is new.
- **Issue 15**, `.scratch/testing-improvements-round2/issues/` (I15 — cross-shard EVAL test helper). F3 needs a "run an `EVAL` whose keys span shards with `allow_cross_slot_standalone=true`" helper; the config knob already exists in `test-harness/src/server.rs`, only the scripting-side helper is missing.

## Re-triage 2026-08-06

**Verdict: partially-fixed** — 1/15 findings discharged (F10).

| F | verdict | evidence |
|---|---|---|
| F1 | still-valid | `shard-harness/tests/scenario_s3.rs` now asserts clean lock tables + no partial writes after every fault outcome, but the finding's leak-detector-over-a-`ScatterHeavy`-workload criterion (issue 07) is unmet. |
| F3 | still-valid | no cross-shard EVAL helper; `vll/` still has no `tests/` dir. |
| F6 | still-valid | no `ShardSink`/`MetricsSink` fake-sink contract suite. |
| F7 | still-valid | no shuttle model over the scatter protocol; needs issue 08. |
| F8 | still-valid | `ScatterError` arms still not exhaustively pinned to responses. |
| F9 | still-valid | `core/src/shard/vll.rs:40-55` still answers a missing txid with `PartialResult::default()` on the success channel; untested. |
| F10 | **fixed** | `continuation_release_survives_cancellation_then_fires` (`vll/src/shard.rs:985`) and `parked_continuation_deadline_survives_cancellation` (`:1014`, `// FM-VLL-003`); API renamed `await_continuation_release` → `next_continuation_event`. |
| F11 | still-valid | no crate-level `tests/` dir in `frogdb-vll`. |
| F12 | still-valid | `vll/src/coordinator.rs` phase-4 gather arms still `return Err(...)` without `abort_shards`. |
| F13 | still-valid | timeouts are still per-shard inside the phase-2 and phase-4 `for` loops (no whole-scatter budget). |
| F14 | still-valid | `scatter/executor.rs:116` still `self.timeout.max(DEFAULT_LOCK_ACQUISITION_TIMEOUT)`, untested. |
| F15 | still-valid | `vll/src/queue.rs:102-108` `enqueue` still overwrites a duplicate txid via `HashMap::insert`. |
| F16 | still-valid | `connection/scripting/eval.rs:130` builds a plain `ShardSenderSink::new` (no chaos seam). |
| F17 | still-valid | `MetricsSink` counter assertions still absent. |
| F18 | still-valid | `server/src/scatter/executor.rs` has no `mod tests`; `scatter_error_to_response` (`:141`) unpinned. |

Phase 1 locked `frogdb-txn` + `frogdb-vll` at 100% mutation score, but the lock did not close
these gaps: `vll-failure-modes.md` carries only **FM-VLL-001..004**, all about the continuation
lock, and states in its own words that the scatter phases (dispatch failure, phase-2/3 partial
unwind, gather timeouts) are "not yet rowed" — so F12/F13/F18 have no FM row to discharge them.
Only F10 has both a row (FM-VLL-003) and named forcing tests. Stale refs corrected: the
coordinator/queue/shard code cited under `server/src/` now lives in `frogdb-server/crates/vll/src/`
(`coordinator.rs`, `queue.rs`, `shard.rs`) and the executor at
`frogdb-server/crates/server/src/scatter/executor.rs`. No live production bug: every residual
item is a test gap or a documented design choice (`scenario_s3.rs:50-55` now records the
deliberate "already-executed participants are not aborted" contract).
