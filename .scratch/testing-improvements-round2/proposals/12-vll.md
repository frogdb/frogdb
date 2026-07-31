# VLL (Very Lightweight Locking) — testing gap audit (round 2)

## Scope

Paths audited:

- `frogdb-server/crates/vll/` — 2.2k LOC, 6 modules (`coordinator.rs` 830, `shard.rs` 541,
  `lock_table.rs` 343, `queue.rs` 260, `traits.rs`, `types.rs`), 4 inline `#[cfg(test)]`
  modules (25 tests total), **no `tests/` dir**, no `dev-dependencies` beyond `tokio`
  (no proptest, no shuttle), 90.6% lines.
- `frogdb-server/crates/server/src/vll_adapter.rs` (174 LOC) — `ShardSink` impl + turmoil chaos hooks.
- Read for context (other agents own them): `crates/server/src/scatter/executor.rs`,
  `crates/server/src/connection/scripting/eval.rs`, `crates/core/src/shard/{vll.rs,
  dispatch_core.rs, dispatch_vll.rs, event_loop.rs, worker.rs}`.

Depth classes for the `vll` crate (148 fn records, dedup'd by max `test_count`):
`untested` 14, `single-test` 68, `monoculture` 30, `well-covered` 35, `covered` 1.
The `single-test` mass is inflated by the 25 inline test fns themselves; the *production*
functions that are untested or single-test are the interesting ones and every one of them
sits on a lock-release or continuation path:

| fn | class |
|---|---|
| `shard.rs:252` continuation drain-loop closure | `untested` (tests=0, exec=0) |
| `shard.rs:293 has_continuation_lock` | `untested` |
| `queue.rs:129 is_empty` (the drain-loop predicate) | `untested` |
| `coordinator.rs:126 ContinuationGuard::release` | `untested` |
| `coordinator.rs:88 / :154` `Display for {Scatter,Continuation}Error` | `untested` |
| `coordinator.rs:413 abort_pending` | `single-test` |
| `shard.rs:287 clear_continuation_lock` | `single-test` |
| `coordinator.rs:359 acquire_continuation` | `monoculture` (4, all unit) |
| `coordinator.rs:425 abort_shards` | `monoculture` (3, all unit) |

And at the callers:

| fn | class |
|---|---|
| `server/src/connection/scripting/eval.rs:119 execute_cross_shard_script` | **`untested`, tests=0, regions 0** |
| `server/src/vll_adapter.rs:137 send_continuation_lock` | **`untested`, tests=0** |
| `core/src/shard/vll.rs:67 handle_vll_continuation_lock` | `single-test` (shard_driver S4 only) |

## Summary

VLL is the atomicity mechanism for every cross-shard multi-key command and for cross-shard
Lua, so its failure modes are consistency and availability violations by construction. The
line percentage is misleading: 90.6% is achieved almost entirely by 25 inline tests driving a
`TestSink` mock that replies instantly and never reorders, so what is covered is the
*happy sequence*, not the protocol. Three structural blind spots dominate. First, the entire
**continuation-lock feature is unexercised above the shard**: its server entry point
(`execute_cross_shard_script`) and its sink method (`vll_adapter::send_continuation_lock`)
both have `tests=0`, so a cross-shard `EVAL` has never once run end-to-end in CI — and static
reading says it currently cannot succeed whenever a scatter is in flight (F5). Second,
**deadlock freedom and lock-release-on-every-path are argued in doc comments, never asserted**:
`try_grant`'s second rule deliberately breaks the txid total order (F6), the caller-side
"sort shards ascending" convention is unenforced and holds only because a `BTreeMap` happens
to iterate in order (F8), and the crate's own leak detector (`check_locktable_empty`, already
built and already wired into the workload runner) is never pointed at a workload that emits a
single MGET/MSET (F1). Third, the one server-level atomicity test that does drive VLL,
`test_mset_full_atomicity_sharded`, cannot fail when every reply is an error (F4). The bug
that escapes today is exactly the one that matters most: a lock left behind on an abort,
timeout, or fault path, wedging a shard permanently — no test in the repo would notice.

## Existing test inventory

| surface | what it covers | strengths | blind spots |
|---|---|---|---|
| `vll/src/coordinator.rs` inline (8 tests) | 4-phase scatter over `TestSink`; abort-by-real-shard-id for sparse participants (`phase2_failure_aborts_real_shard_ids_for_sparse_participants`, `phase3_failure_aborts_remaining_holders_not_positions`); guard releases on drop / on partial-acquire failure | the off-by-one abort-indexing bugs are genuinely pinned; dispatch-order of responses asserted | sink is instantaneous and never reorders; no timeout path (`ResultTimeout` never taken); no concurrent txns at all; `Display` impls never asserted |
| `vll/src/lock_table.rs` inline (10 tests) | grant/conflict matrix, all-or-nothing grant, SCA ordering, duplicate-key idempotence, `lock_state_string` | `lower_txid_writer_blocked_by_granted_higher_reader` documents the order-breaking rule | single-shard only, so the *cross-shard* cycle that rule admits is never constructed; no property test of the table's invariants |
| `vll/src/shard.rs` inline (7 tests) | enqueue/grant/abort, waiter advance, `sca_lock_request_rejected_while_continuation_held`, `continuation_lock_blocks_second_acquire`, diagnostic snapshots | covers the ShardBusy rejection | **no test enqueues an SCA op and *then* requests a continuation lock** — the drain loop (`shard.rs:252`) has exec=0 |
| `vll/src/queue.rs` inline (6 tests) | ordering, capacity, dequeue, get_mut, continuation flag | | duplicate-txid enqueue overwrite untested; `is_empty` untested |
| `core/tests/shard_driver/scenario_s3.rs` | real `VllCoordinator` + real `ShardWorker`s over a `FaultSink`, sparse participants `[2,5,7]`, asserts empty lock tables and no partial writes after abort | **the best VLL test in the repo** — right boundary, asserts residue | one fixed fault shape, one txn at a time, no continuation locks, no timeouts |
| `core/tests/shard_driver/scenario_s4.rs` | continuation holder *panics* → locks released; other conns see `ERR shard busy`; execute after `pump_continuation_release` | covers the panic path the guard exists for | only proves *other* conns are excluded; never proves the **owner** can proceed, and never runs a real script |
| `server/tests/simulation.rs` (turmoil) | `test_mset_full_atomicity_sharded`, `test_shard_unavailable_scatter_gather_atomicity`, `test_partial_failure_error_shard`, `test_asymmetric_per_shard_delays`, `test_partition_mid_mset` | real server, real chaos delays, cross-shard keys | assertion-weak (F4); **none probes `DEBUG LOCKTABLE` after the fault**, so leaks are invisible |
| `server/tests/integration_transactions.rs` | `allow_cross_slot_standalone=true` MULTI/MSET behaviour (round-1 issues 19/49) | pins CROSSSLOT-vs-scatter semantics | MULTI never reaches VLL (always CROSSSLOT), so this does not cover the lock protocol |
| `core/tests/concurrency.rs` | `test_mset_cross_shard_partial_visibility`, `test_mset_same_shard_atomicity` | | uses a **mock `TestCluster`**, not VLL; the former *documents partial visibility as acceptable* — directly contradicts the VLL contract |
| `crates/testing/` quiescence | `check_locktable_empty` flags `intent_key_count>0 || continuation_lock_held`; wired through `common/quiescence_probe.rs` → `workload_runner` → `concurrency_workload::quiescence_stage_runs_and_is_clean` | the leak detector already exists and is already plumbed | `testing/src/workload.rs` profiles (`TxHeavy`/`BlockingHeavy`/`MultiWaiter`/`Mixed`) emit **no MGET/MSET/DEL/UNLINK** — the detector never sees a VLL op (F1) |
| `testing/jepsen` | round-1 issue 21 added `cross-slot-partition` / `cross-slot-kill` | | `frogdb/cross_slot.clj` is a **Raft-cluster hash-tag/CROSSSLOT** workload; `shard_for_key = slot % num_shards` means cluster mode never reaches the scatter path, so jepsen does not cover VLL |

**Round-1 residue.** Issue 07 (shuttle MultiWaiter exactly-once) established shuttle in
`crates/testing/` but scoped it to blocking-command waiters, not VLL. Issues 19/49/51/55
pinned cross-slot MULTI *routing* semantics and confirmed cross-shard MULTI is always
rejected with CROSSSLOT — i.e. they proved VLL does **not** back MULTI, which is exactly why
F2 below (MULTI executing *during* someone else's continuation lock) was never in their
blast radius. Issue 21's jepsen variants exercise cluster slot routing, not internal shards.
Nothing in round 1 touched the lock table, the coordinator phases, or continuation locks.

**Reachability caveat applied throughout.** `allow_cross_slot_standalone` defaults to `false`
(`config/src/server.rs:41`, `#[param(skip)]` — startup-fixed), and
`shard_for_key(key) = crc16(tag) % 16384 % num_shards` co-locates same-slot keys, so cluster
mode never scatters. Every scatter/continuation finding therefore caps at Likelihood 3: on the
non-default config that turns the feature on, the traffic is ordinary, not adversarial.

## Findings

### F1: the lock-table leak detector is never pointed at any VLL operation
- **Severity** 5 — a leaked intent or continuation lock wedges a shard permanently: every
  later command on those keys blocks or gets `BUSY` until restart. This is the canonical VLL
  failure and it is silent.
- **Likelihood** 3 — leaks arise on abort/timeout/fault paths, which the chaos tests already
  provoke; nothing checks for the residue afterwards.
- **Effort** 2 — the detector, the probe, and the runner all already exist; add scatter ops to
  a workload profile and call the existing probe after the existing chaos tests.
- **Priority** 19
- **Evidence**: `crates/testing/src/quiescence.rs::check_locktable_empty` flags
  `intent_key_count > 0 || continuation_lock_held`, consumed by
  `crates/server/tests/concurrency_workload.rs::quiescence_stage_runs_and_is_clean` via
  `common/quiescence_probe.rs`. But `crates/testing/src/workload.rs` profiles are
  `TxHeavy | BlockingHeavy | MultiWaiter | Mixed` and emit no MGET/MSET/DEL/UNLINK, so no
  scatter ever runs under the checker. Independently, none of the turmoil scatter chaos tests
  (`simulation.rs:1489`, `test_shard_unavailable_scatter_gather_atomicity`,
  `test_partial_failure_error_shard`, `test_partition_mid_mset`) issues `DEBUG LOCKTABLE`
  after the fault. `abort_pending` is `single-test`, `abort_shards` `monoculture` — both only
  against an instant mock.
- **Proposed test**: (a) add a `ScatterHeavy` profile emitting cross-shard MGET/MSET/DEL with
  `allow_cross_slot_standalone=true`, run it through the existing workload runner, and assert
  `check_locktable_empty` on **every** shard at quiescence; (b) append the same
  `DEBUG LOCKTABLE`-empty assertion to each existing turmoil scatter-fault test after
  `sim.run()`, asserting `intent_key_count == 0 && !continuation_lock_held` per shard.
- **Boundary**: 4 — the probe is `DEBUG LOCKTABLE` over RESP and the faults are injected at
  the server; no lower layer exposes the aggregate.

### F2: MULTI/EXEC executes while another connection holds a continuation lock
- **Severity** 5 — a cross-shard Lua script acquires continuation locks on N shards precisely
  to get exclusive access; a concurrent `EXEC` mutates those shards underneath it. The script's
  cross-shard read-modify-write is then non-atomic — a silent consistency violation, with no
  error surfaced to either client.
- **Likelihood** 3 — needs `allow_cross_slot_standalone=true` plus a concurrent MULTI, which is
  ordinary traffic on that config.
- **Effort** 3 — two connections against `TestServer`; the harness already exposes the config
  flag.
- **Priority** 18
- **Evidence**: `crates/core/src/shard/dispatch_core.rs` — `CoreMsg::Execute` (line 20) and
  `CoreMsg::ScatterRequest` (line 49) both call `self.can_execute_during_lock(conn_id)` and
  bail; `CoreMsg::ExecTransaction` (line 95) has `conn_id` in scope and **never calls it**,
  going straight to `execute_transaction`. The gate itself
  (`worker.rs:806 can_execute_during_lock`) is `well-covered` (3027 tests) — it is the missing
  call site, not the gate, that is untested. `dispatch_scripting.rs` applies the gate at lines
  17/42/104, confirming the intended policy is "everything is gated".
- **Proposed test**: conn A runs a cross-shard `EVAL` that sleeps/blocks while holding
  continuation locks on shards 0 and 1; conn B runs `MULTI; SET k_on_shard0 v; EXEC`. Assert B
  is rejected with the continuation-lock busy error (or is serialised after A), and that A's
  script observes a consistent snapshot. Mirror the existing `scenario_s4` assertion shape.
- **Boundary**: 4 — MULTI/EXEC queueing lives in the connection layer; the shard-level message
  is only reachable through it.
- **OPTIONS**:
  - *3 (`shard_driver`)*: send `CoreMsg::ExecTransaction` directly while a continuation lock is
    held and assert the busy error. Cheapest, deterministic, pins the exact missing gate — but
    asserts on an internal message rather than user-visible behaviour, and would not catch the
    connection layer routing around it.
  - *4 (server integration)*: as proposed. Proves the user-visible property, needs a script
    that reliably holds the lock for a window.
  - **Recommendation: both** — the level-3 test as the fast regression pin (it is ~20 lines
    given `scenario_s4`), the level-4 test as the behavioural guarantee. If only one, take 4.

### F3: the continuation-lock feature has zero coverage above the shard
- **Severity** 5 — this is the atomicity mechanism for cross-shard scripts and it freezes N
  shards while held. Untested code that freezes shards is the highest-consequence shape in the
  crate; F5 argues it is in fact broken today.
- **Likelihood** 3 — any cross-shard `EVAL` on the enabling config takes this path; there is no
  other path.
- **Effort** 3 — `TestServer` + the existing `allow_cross_slot_standalone` harness knob.
- **Priority** 18
- **Evidence**: `server/src/connection/scripting/eval.rs:119 execute_cross_shard_script` —
  **`untested`, tests=0, regions 0/0**. `server/src/vll_adapter.rs:137 send_continuation_lock`
  — **`untested`, tests=0**. `classify_script_shards` (eval.rs:65) is `well-covered` (162
  tests) yet its `ScriptShards::CrossShard` arm (line 80) is never taken, which is exactly how
  a well-covered classifier can sit in front of a dead branch. `grep -l allow_cross_slot_standalone
  crates/server/tests` returns only transactions/client/metrics/persistence files — no EVAL
  test. `handle_vll_continuation_lock` (`core/src/shard/vll.rs:67`) is `single-test`
  (shard_driver S4), and S4 asserts only that *other* connections are excluded — never that
  the owner can complete.
- **Proposed test**: with `allow_cross_slot_standalone=true` and ≥2 shards, `EVAL` a script
  whose KEYS span two shards and which does `redis.call('SET', KEYS[1], ...)` and
  `redis.call('GET', KEYS[2])`. Assert: the script returns the correct value (proving the owner's
  remote sub-commands pass `can_execute_during_lock`), both writes are visible after, and
  `DEBUG LOCKTABLE` shows `continuation_lock_held=false` on every shard afterwards.
- **Boundary**: 4 — the path starts at EVAL parsing/classification in the connection layer;
  nothing below it can invoke `execute_cross_shard_script`.

### F4: the flagship cross-shard atomicity test passes when every reply is an error
- **Severity** 4 — it is the only server-level test asserting VLL's core promise; a regression
  that turns every cross-shard MGET into `-ERR VLL lock acquisition failed` leaves it green.
- **Likelihood** 3 — F5/F6/F9 are all failure modes that produce exactly that reply shape.
- **Effort** 1 — tighten assertions in an existing test.
- **Priority** 17
- **Evidence**: `crates/server/tests/simulation.rs:1560-1595` — the reader computes
  `has_value = response_str.contains("$1\r\n1\r\n")` and `has_nil = contains("$-1\r\n")` and
  only records a failure when *both* are true. An error reply (`-ERR ...`) sets neither, so
  the loop records nothing and the final `assert!(!saw_partial)` passes. There is also no
  assertion that the 100 polls ever observed the *committed* state, and no post-run `MGET`
  asserting both keys are set.
- **Proposed test**: in the same test, assert every one of the 100 replies is a well-formed
  2-element array (fail on any `-ERR`/`BUSY`), assert at least one poll observed the fully
  committed `[1,1]`, and add a final `MGET key_a key_b` asserting `[1,1]`. Same for the
  sibling chaos tests.
- **Boundary**: 4 — modify in place; it is already a turmoil server test.

### F5: acquiring a continuation lock stalls the whole shard for 2 s and then always fails
- **Severity** 4 — while the drain loop spins, the shard's event loop processes **no**
  messages: every client on that shard hangs for `CONTINUATION_DRAIN_TIMEOUT`. The acquisition
  then returns `LockTimeout` regardless, so cross-shard scripts cannot succeed whenever any
  SCA op is queued.
- **Likelihood** 3 — needs the enabling config plus one in-flight scatter, which is normal
  traffic on that config; the queue is non-empty for the whole duration of any scatter.
- **Effort** 3 — `shard_driver` already builds real `ShardWorker`s and a real coordinator
  (scenario S3 does exactly this).
- **Priority** 15
- **Evidence**: `crates/vll/src/shard.rs:247-262` — the drain loop polls
  `self.tx_queue.as_ref().map(|q| !q.is_empty())` and `tokio::time::sleep(CONTINUATION_DRAIN_POLL)`
  until `CONTINUATION_DRAIN_TIMEOUT` (2000 ms), then sends
  `ShardReadyResult::Failed(VllError::LockTimeout)`. It is awaited from
  `core/src/shard/vll.rs::handle_vll_continuation_lock`, which is awaited from
  `dispatch_message(msg).await` **inline** in the `tokio::select!` message arm of
  `core/src/shard/event_loop.rs`. Queue entries are only removed by `VllExecute`/`VllAbort`,
  which are messages — so the loop is waiting for progress it is itself blocking. Coverage
  confirms nobody has ever run it: `shard.rs:252` (the loop closure) `untested`, exec=0, and
  `queue.rs:129 is_empty` `untested`. The 7 inline `shard.rs` tests never enqueue an SCA op
  before requesting a continuation lock, and `scenario_s4` starts from an empty queue.
- **Proposed test**: in `shard_driver`, enqueue a scatter lock request on shard 0 (leave it
  Ready, un-executed), then request a continuation lock on shard 0 from another connection.
  Assert the acquisition resolves in well under `CONTINUATION_DRAIN_TIMEOUT` and that an
  unrelated `PING`/`GET` on that shard is still answered while the acquisition is pending
  (i.e. the shard is not blocked). Both assertions fail today.
- **Boundary**: 3 — needs the real shard event loop (that is the whole bug); the socket adds
  nothing.

### F6: the lock table admits cross-shard wait-for cycles; deadlock freedom is only a comment
- **Severity** 4 — two cross-shard transactions can deadlock and are broken only by the 4 s
  timeout, during which the participating keys are locked and dependent traffic queues; both
  clients then get errors.
- **Likelihood** 3 — an MGET and an MSET over the same two keys is the ordinary contention
  pattern for this feature.
- **Effort** 3 — two concurrent coordinators over real shard workers; `shard_driver` supports
  this shape.
- **Priority** 15
- **Evidence**: `crates/vll/src/lock_table.rs:90-123 try_grant` applies two rules — (1) SCA:
  conflicting *lower-txid* intents block; (2) **holders: any conflicting *granted* intent from
  another txn blocks regardless of txid order**. Rule (2) deliberately breaks the total order
  that rule (1) establishes, so txn 7 granted on shard A can block txn 5 there while txn 5
  granted on shard B blocks txn 7 — a genuine cycle across shards. The single-shard ingredient
  is already a named test (`lower_txid_writer_blocked_by_granted_higher_reader`,
  `lock_table.rs:284`) but is never composed across shards. Deadlock freedom appears only as
  prose: `coordinator.rs:8-11` ("send … in sorted order (deadlock prevention)") and
  `coordinator.rs:52-54` / the `acquire_continuation_and_run` doc ("`shards` must be sorted in
  ascending order to prevent deadlocks"). No test asserts it; `try_grant` is `well-covered`
  (11 tests) purely single-shard.
- **Proposed test**: two shard workers, keys `a`(shard 0) and `b`(shard 1). Txn 5 = MSET over
  `[a,b]`, txn 7 = MGET over `[a,b]`, driven concurrently with the pump ordered so that
  txn 7 is granted on shard 0 first and txn 5 on shard 1 first. Assert both transactions
  resolve within ≪ `DEFAULT_LOCK_ACQUISITION_TIMEOUT` (i.e. one is aborted promptly rather
  than both timing out), and that lock tables are empty afterwards. Best expressed as the
  "bounded acquisition" invariant of F7's model rather than as a one-off example.
- **Boundary**: 3 — real lock tables and real workers, deterministic pumping; a server test
  could not reliably force the interleaving.

### F7: no deterministic interleaving / model coverage of the VLL protocol (shuttle assessment)
- **Severity** 5 — every escaped defect in this crate is a consistency or liveness violation;
  the failure modes (F5, F6, F9, F11) are all interleaving-dependent and all invisible to
  example tests.
- **Likelihood** 3 — same config gate as everything else.
- **Effort** 5 — new infrastructure: a message-schedule explorer plus the invariant oracles.
- **Priority** 16

**The explicit tool evaluation the dispatch asked for:**

- **loom is the wrong tool.** `grep` for `AtomicU*`/`UnsafeCell`/`Mutex` inside `crates/vll`
  returns nothing: `VllShardState` and `LockTable` are plain `&mut self` state machines owned
  by a single task, and all cross-task communication is `tokio::sync::{mpsc, oneshot}`. There
  is no memory-ordering surface for loom to explore, and loom cannot model tokio channels or
  timers anyway.
- **shuttle is the right tool, with a caveat.** The real nondeterminism is (a) the arrival
  order of `VllLockRequest` / `VllExecute` / `VllAbort` / `VllContinuationLock` across the
  per-shard mpsc queues, (b) the relative progress of two or more coordinator tasks, and (c)
  timer firing. shuttle explores exactly (a) and (b) by scheduling futures deterministically;
  (c) needs the timeouts to be modelled as a schedulable event rather than wall-clock, so the
  model must either use `tokio::time::pause`-style virtual time or treat "timeout fires" as an
  explicit injectable message. That is the one piece of new infrastructure.
- **Model boundary**: N in-process `VllShardState` instances each driven by a task reading a
  bounded queue, plus M `VllCoordinator` tasks over a sink that pushes into those queues. Real
  `vll` code, **no** `ShardWorker`, no store, no commands — the operations are opaque tokens.
  Keeping the store out is what makes the state space tractable; the store-level property
  ("no partial writes") is already covered by `scenario_s3` at level 3.
- **Invariants the model must assert**, on every schedule:
  1. **No leaked state** — at quiescence every `LockTable` is empty and no
     `continuation_lock_held` is set, for *every* terminal outcome (success, abort, lock
     failure, both timeouts, sink error).
  2. **Bounded acquisition / no deadlock** — no schedule exists in which two transactions are
     each blocked on the other with no timer pending; equivalently, with timeouts disabled,
     every transaction eventually terminates.
  3. **All-or-nothing per participant** — for each txid, either every participant received
     `VllExecute`, or none did after any participant was aborted; never both `VllExecute` and
     `VllAbort` for the same (txid, shard).
  4. **Exactly-once disposition** — each participant that received a lock request is
     eventually either executed or aborted, exactly once (the VLL analogue of round-1 issue
     07's exactly-once waiter guard).
  5. **Continuation exclusion** — while a continuation lock is held by conn C on shard S, no
     other connection's operation executes on S; and the owner's own operations do.
  6. **No lost wakeup** — a transaction blocked on a key is granted within a bounded number of
     steps after the blocking holder releases.
- **Proposed test**: `crates/vll/tests/shuttle_protocol.rs` (new `tests/` dir, `shuttle` as a
  dev-dependency behind the same feature flag `crates/testing` already uses for issue 07),
  running the six invariants over 2–3 shards and 2–3 concurrent transactions with bounded
  exploration in CI and a longer nightly budget.
- **Boundary**: 2 — the crate's public API is already sink-abstracted (`ShardSink`,
  `MetricsSink`), which is precisely the seam a model needs; going to level 3 would drag the
  store and command registry into the state space for no added invariant.
- **OPTIONS**:
  - *A — shuttle at level 2 (recommended)*: cheapest state space, reuses the existing
    `TestSink` shape, catches F5/F6/F9/F11 classes. Does **not** cover the shard event loop, so
    F5's specific "the drain loop blocks its own drain" bug needs the level-3 test in F5 too.
  - *B — extend the existing `shard_driver` permuted-pump generator* (`core/tests/shard_driver/generator.rs`)
    to enumerate message orderings over real workers. Reuses existing infrastructure and covers
    the event loop, so it would catch F5; but the state space includes the whole store, so
    exploration is shallow and it cannot prove absence.
  - *C — turmoil at level 4*: already exists, already used for scatter chaos; good for
    end-to-end fault shapes but its scheduling is randomised, not exhaustive, so it cannot
    establish invariant 2.
  - **Recommendation: A as the primary tool, plus the targeted level-3 test in F5.** B is the
    fallback if adding a shuttle dependency to `vll` is unwanted; C stays as it is but gains
    the quiescence assertions from F1.

### F8: the ascending-shard-order deadlock-prevention convention is unenforced and untested
- **Severity** 4 — the *only* thing standing between the current design and cross-transaction
  deadlock on the continuation path is that all callers dispatch in ascending shard order; it
  is a comment, not a type, not an assertion.
- **Likelihood** 2 — today's two callers happen to comply, but by accident of a container
  type rather than by construction, so any refactor silently breaks it.
- **Effort** 1 — a pure unit test on the participant-building code plus a `debug_assert` in
  the coordinator.
- **Priority** 15
- **Evidence**: `coordinator.rs:52-54` documents the convention as a caller obligation
  ("callers should sort by shard id") and `acquire_continuation_and_run`'s doc says "`shards`
  must be sorted in ascending order to prevent deadlocks", but neither `scatter` nor
  `acquire_continuation` checks it. `server/src/scatter/executor.rs:87-99` builds participants
  by iterating `partition.shard_keys`, which is ordered only because `PartitionResult::shard_keys`
  is declared `BTreeMap<usize, Vec<Bytes>>` — change it to a `HashMap` for speed and the
  ordering guarantee evaporates with no test failing.
- **Proposed test**: (a) unit test asserting `ScatterGatherExecutor`'s participants come out
  strictly ascending by `shard_id` for a key set that hashes out of order, and that
  `classify_script_shards` returns an ascending, deduplicated `Vec<usize>`; (b) a
  `debug_assert!(shards.is_sorted())` equivalent in `scatter`/`acquire_continuation` so the
  existing test suite enforces it everywhere.
- **Boundary**: 1 — a pure ordering property of a pure function; no runtime needed.

### F9: unknown-txid `VllExecute` returns an empty *success*, not an error
- **Severity** 5 — the fallback is success-shaped: MGET renders the missing keys as nils and
  MSET reports OK for writes that never happened. Any ordering regression upstream converts
  into a silent wrong answer / lost write rather than a loud failure.
- **Likelihood** 2 — the current coordinator never sends `VllExecute` after an abort for the
  same txid, so this needs a coordinator bug or a channel-ordering violation to reach; that is
  exactly the class of bug the rest of this audit says is untested.
- **Effort** 2 — one `shard_driver`/crate-level test.
- **Priority** 17
- **Evidence**: `core/src/shard/vll.rs::handle_vll_execute` — on a txid missing from the queue
  it does `let _ = response_tx.send(PartialResult::default()); return;` with no error and no
  metric. `crates/server/src/scatter/executor.rs:124-137` then merges the empty
  `into_keyed_results()` straight into the client reply. There is no `VllError` variant for
  "unknown txid" and no test sends `VllExecute` for a txid that was never enqueued.
- **Proposed test**: send `VllExecute` for an unknown txid and assert the shard's reply is
  distinguishable from a legitimate empty result (a dedicated error or a flagged
  `PartialResult`), and that the scatter executor turns it into an error reply rather than
  nils/OK. Add the abort-then-execute ordering case too.
- **Boundary**: 3 — the shard's message handling is the unit under test; the executor mapping
  can be a level-2 assertion on top.

### F10: `await_continuation_release` cancel-safety is asserted nowhere
- **Severity** 4 — if a refactor replaces the in-place `&mut rx` poll with a `take()`, a lost
  select! race drops the release signal and the shard's continuation lock is held **forever**:
  permanent unavailability for every other connection on that shard, recoverable only by
  restart.
- **Likelihood** 2 — requires a refactor, but the property is subtle enough to be an easy
  mistake and there is no test guarding it.
- **Effort** 1 — a focused unit test on the shard state.
- **Priority** 15
- **Evidence**: `crates/vll/src/shard.rs:275 await_continuation_release` is nominally
  `well-covered` (3743 tests) but essentially all of those hits are the
  `None => std::future::pending()` branch taken by every idle shard on every event-loop turn —
  a textbook coverage illusion. `clear_continuation_lock` (`shard.rs:287`) is `single-test`.
  The cancel-safety claim in the doc comment ("if the surrounding select! fires another arm
  first, the release receiver is preserved") relies on `impl Future for &mut F` and is never
  exercised by a test that actually loses the race.
- **Proposed test**: acquire a continuation lock; race `await_continuation_release` against a
  ready branch in a `select!` so the other arm wins; then drop the sender and assert the
  release is still observed on the next poll and `has_continuation_lock()` becomes false.
- **Boundary**: 1 — pure state-machine property of `VllShardState`.

### F11: `LockTable` has no property test for its own invariants
- **Severity** 4 — the table is the ground truth for mutual exclusion; a grant bug is a direct
  isolation violation.
- **Likelihood** 2 — the 10 example tests cover the obvious matrix; what they cannot cover is
  arbitrary interleavings of declare/grant/release across many txns and keys.
- **Effort** 1 — `proptest` on a pure data structure, no async.
- **Priority** 15
- **Evidence**: `crates/vll/Cargo.toml` has no `proptest` dev-dependency and the crate has no
  `tests/` dir. The 10 tests in `lock_table.rs:185-343` are all fixed 2–3 txn scenarios.
  `duplicate_keys_in_slice_are_idempotent` shows the authors already found one aliasing edge by
  hand — the kind of thing a property test finds exhaustively.
- **Proposed test**: proptest over random sequences of `(txid, keys, mode)` declare / try_grant
  / release, asserting after every step: no two conflicting granted intents coexist on a key;
  a granted intent's keys are all granted (all-or-nothing); release is idempotent and never
  releases another txn's intent; the table is empty once every txn has released; and
  `lock_state_string` reflects grants, not intents.
- **Boundary**: 1 — pure synchronous data structure.

### F12: the gather phase never aborts on timeout, so a slow shard can commit after the client got an error
- **Severity** 4 — the client is told the operation failed while some or all shards go on to
  apply it: a phantom write, and durably so if it reaches the WAL.
- **Likelihood** 2 — needs a shard slow enough to exceed the gather timeout, i.e. the
  overload/stall case.
- **Effort** 2 — the `FaultSink` used by `scenario_s3` can already stall a participant.
- **Priority** 14
- **Evidence**: `crates/vll/src/coordinator.rs` phase 4 — both the `Ok(Err(_))` and `Err(_)`
  arms return `ScatterError::{ResultChannelClosed,ResultTimeout}` **without calling
  `abort_shards`**, on the documented assumption that "participants that already received
  `VllExecute` release their own locks when execution completes". That assumption is sound for
  locks but says nothing about the write becoming visible after the error reply, and it is
  false for liveness if the shard is wedged. Neither arm is covered: the 8 coordinator unit
  tests use an instantaneous `TestSink`, and `ScatterError`'s `Display` (`coordinator.rs:88`)
  is `untested`, confirming no test ever constructs these variants.
- **Proposed test**: stall one participant past the gather timeout; assert the client sees
  `ERR VLL execution timeout`, then let the shard finish and assert the documented outcome —
  either the write is visible (and the test pins that as the contract) or it is not. Then
  assert lock tables are empty on all shards. Today neither is stated anywhere.
- **Boundary**: 3 — `shard_driver` + `FaultSink`; this is `scenario_s3` with a stall instead of
  a drop.
- **OPTIONS**:
  - *3*: deterministic, cheap, but asserts visibility through the shard rather than the client.
  - *4 (turmoil, `shard_delays_ms` already exists in `vll_adapter.rs:89`)*: asserts the actual
    client-visible contradiction (error reply + present key). Slower, and the delay must be
    tuned past the 4 s floor.
  - **Recommendation: 3 for the mechanism, and fold the client-visible half into F1's
    quiescence assertions on the existing `test_asymmetric_per_shard_delays`.**

### F13: per-shard sequential timeouts mean the effective deadline is N × timeout
- **Severity** 3 — a client can be held for `2 × N × timeout` (16 shards × 4 s × 2 phases ≈
  128 s) on a single MGET when shards are stalled; connection and client-side timeouts fire
  long before, and the operator's configured timeout bears no relation to observed latency.
- **Likelihood** 3 — any stalled or heavily contended shard set.
- **Effort** 1 — coordinator unit test with a sink that never replies.
- **Priority** 14
- **Evidence**: `coordinator.rs` phase 2 applies `tokio::time::timeout(request.timeout, ready_rx)`
  **inside** the per-shard `for` loop, as does phase 4, as does `acquire_continuation`
  (`coordinator.rs:392-407`). There is no overall deadline. No test has ever taken a timeout
  arm at all (see F12 evidence).
- **Proposed test**: `TestSink` that never sends any `Ready`; assert `scatter` over 8
  participants returns `LockTimeout` in ≈`timeout`, not ≈`8 × timeout`. Same for
  `acquire_continuation`.
- **Boundary**: 1 — pure coordinator logic over the existing `TestSink`, using
  `tokio::time::pause()` so the test is instant.

### F14: `scatter_gather_timeout_ms` below 4000 is silently ignored
- **Severity** 3 — a live-mutable, operator-facing timeout that does not take effect is a
  configuration lie; the operator who lowers it to protect tail latency gets 4 s anyway.
- **Likelihood** 3 — `#[param(mutable)]`, so it is expected to be tuned at runtime; the
  default (5000) is above the floor, so the clamp only bites operators who deliberately lower
  it — exactly the ones who care.
- **Effort** 3 — `CONFIG SET` + a stalled shard through the server harness.
- **Priority** 12
- **Evidence**: `crates/server/src/scatter/executor.rs:115` —
  `timeout: self.timeout.max(DEFAULT_LOCK_ACQUISITION_TIMEOUT)` with
  `DEFAULT_LOCK_ACQUISITION_TIMEOUT = 4000 ms` (`vll/src/coordinator.rs:33`), versus
  `DEFAULT_SCATTER_GATHER_TIMEOUT_MS = 5000` and `#[param(mutable)] scatter_gather_timeout_ms`
  (`config/src/server.rs:44-46, 90`). No test sets the value below 4000.
- **Proposed test**: `CONFIG SET scatter-gather-timeout-ms 200`, stall a shard, assert the
  error arrives in ≪ 4 s (or, if the floor is intentional, assert `CONFIG SET` rejects values
  below it — a much better contract than silent clamping).
- **Boundary**: 4 — the config plumbing and `CONFIG SET` live at the server.

### F15: duplicate-txid enqueue silently overwrites the previous operation
- **Severity** 4 — the overwritten op's `ready_tx` is dropped (its coordinator sees
  `LockChannelClosed`) and, worse, its already-declared intents are orphaned in the lock table
  with no owner to release them — a permanent shard wedge.
- **Likelihood** 1 — `next_txid()` is a process-global `AtomicU64` (`server/src/server/util.rs:20,28`),
  so a collision requires a restart-with-reuse or a future per-connection txid scheme.
- **Effort** 1 — one unit test.
- **Priority** 13
- **Evidence**: `crates/vll/src/queue.rs::enqueue` does `self.pending.insert(op.txid, op)` and
  returns `Ok(())`, discarding any previous entry; `test_queue_ordering`/`test_queue_capacity`
  never enqueue the same txid twice. Note the txid space is *not* persisted across restart, so
  the "restart-with-reuse" case is real if a client's in-flight txid survives — worth stating
  explicitly in the test.
- **Proposed test**: enqueue txid 5 twice; assert the second is rejected
  (`VllError::Internal`/a new variant) rather than silently replacing, and that the first op's
  intents are still owned and releasable.
- **Boundary**: 1 — pure queue data structure.

### F16: continuation-lock acquisition is never fault-injected
- **Severity** 4 — a shard that is unavailable or slow *during* continuation acquisition leaves
  the earlier shards locked until the guard drops; the partial-acquire release path
  (`coordinator.rs:381-388, 396-406`) is subtle (it relies on dropping `release_txs` to signal
  shards that already armed their receivers) and is only exercised by one mock unit test.
- **Likelihood** 2 — needs the enabling config plus a shard fault.
- **Effort** 3 — the chaos plumbing exists but is not wired to this sink.
- **Priority** 13
- **Evidence**: `server/src/vll_adapter.rs:40-47` — `ShardSenderSink::new` sets `chaos: None`,
  and `eval.rs:130` constructs the sink with `new`, so `is_shard_unavailable`/`get_shard_error`/
  `shard_delays_ms` never apply to continuation acquisition even under turmoil. The only
  coverage is `acquire_continuation_releases_partially_acquired_on_failure`
  (`coordinator.rs:707`, `single-test`, mock sink).
- **Proposed test**: extend `ShardSenderSink::with_chaos` to the EVAL path (or add a
  `shard_driver` equivalent) and assert that when shard 2 of `[0,1,2]` is unavailable, shards
  0 and 1 report `continuation_lock_held=false` immediately afterwards and remain serviceable.
- **Boundary**: 3 — `shard_driver` gives deterministic control of which shard fails;
  level 4 only if the chaos config is extended instead.

### F17: no fairness/starvation coverage under sustained contention
- **Severity** 3 — a writer behind a continuous stream of readers is repeatedly denied by the
  holders rule and eventually returns a client error after the 4 s timeout; the workload looks
  healthy while one command class never completes.
- **Likelihood** 2 — needs sustained read contention on the same keys.
- **Effort** 2 — crate-level with many transactions.
- **Priority** 11
- **Evidence**: `lock_table.rs:90-123` — rule (2) blocks a waiting writer whenever *any*
  conflicting granted reader exists, and nothing ages or reserves for the waiter.
  `multiple_readers_share_then_writer_waits` (`lock_table.rs:259`) shows the writer waiting but
  with a finite reader set, so it never tests whether the writer can be starved indefinitely.
  No metric distinguishes "timed out due to starvation" from "timed out due to a stalled shard".
- **Proposed test**: drive a continuous stream of overlapping readers on key K and one writer;
  assert the writer is granted within a bounded number of reader arrivals. Naturally expressed
  as model invariant 6 in F7.
- **Boundary**: 2 — crate-level over `LockTable`/`VllShardState`; no server needed.

### F18: client-visible VLL error strings are asserted nowhere
- **Severity** 2 — clients and retry loops key on `BUSY` vs `ERR`; a reshuffle silently changes
  the retry contract (`BUSY` is retriable, `ERR` is not).
- **Likelihood** 3 — every VLL failure path produces one of these.
- **Effort** 1 — table-driven unit test.
- **Priority** 11
- **Evidence**: `coordinator.rs:88 Display for ScatterError` and `coordinator.rs:154
  Display for ContinuationError` are both `untested` (tests=0), and
  `executor.rs:140-192 scatter_error_to_response` — which maps `ShardBusy` to
  `"BUSY shard busy with continuation lock; retry"` and everything else to `"ERR ..."` — has no
  test asserting the mapping. `VllError`'s `Display` (`types.rs:41`) is likewise `untested`.
- **Proposed test**: table-driven mapping test over every `ScatterError`/`ContinuationError`
  variant asserting the exact response prefix, with `ShardBusy` explicitly pinned to `BUSY`.
- **Boundary**: 2 — pure mapping function; a server test would add nothing.

## Deprioritised

- **Script sub-commands self-deadlocking against their own continuation lock** — checked and
  safe: `dispatch_scripting.rs:17,42,104` all pass the script's own `conn_id` to
  `can_execute_during_lock`, which admits the owner (`worker.rs:806`). Worth an assertion, but
  it is already folded into F3's proposed test rather than filed separately.
- **Blocking commands vs continuation locks** — `dispatch_blocking.rs` (`BlockWait`,
  `UnregisterWait`) has no gate, but those messages only register/deregister a waiter; the
  initial non-blocking attempt goes through the gated `CoreMsg::Execute`. Registering a waiter
  during a foreign continuation lock mutates no data. No finding.
- **Shard-count change / slot migration during a VLL transaction** — not reachable:
  `num_shards` is `#[param]` (not `mutable`, `config/src/server.rs:32`) so it is startup-fixed,
  and `shard_for_key = slot % num_shards` means cluster-mode slot migration never produces a
  cross-shard scatter (round-1 issue 19 confirms cross-shard MULTI is always CROSSSLOT). The
  `self.senders[shard_id]` direct indexes in `vll_adapter.rs:105,124,134,151` are therefore
  safe today; they would become panics if `num_shards` ever became live-mutable — worth a note
  in that PRD, not a test now.
- **`ContinuationGuard::release`** (`coordinator.rs:126`, `untested`) — an empty-bodied
  `fn release(self)` relying on `Drop`. Covered incidentally by any F3 test; not worth its own.
- **`sink()` accessor and `Debug` impls** — untested, cosmetic.
- **`QUEUE_DEPTH_WARN_THRESHOLD` (8000) / `DEFAULT_MAX_QUEUE_DEPTH` (10000) behaviour** —
  `QueueFull` is exercised by `test_queue_capacity`; the warn threshold only logs. Reaching
  10000 queued cross-shard transactions requires the shard to already be wedged, which F1/F5
  cover more directly.
- **`core/tests/concurrency.rs::test_mset_cross_shard_partial_visibility`** — asserts that
  partial visibility *is* acceptable, contradicting VLL's contract, but it runs against a mock
  `TestCluster` and not VLL at all. Not a VLL gap; flagged in cross-area notes as a misleading
  test that should be renamed or deleted.

## Cross-area notes

- **`crates/core` (shard worker) owns F2's fix**: `CoreMsg::ExecTransaction`
  (`dispatch_core.rs:95`) needs the `can_execute_during_lock(conn_id)` call the two sibling
  arms already have. Whoever owns `core` should be told this is a live consistency defect, not
  just a test gap.
- **`crates/core` also owns F5**: the drain loop blocks because `event_loop.rs` awaits
  `dispatch_message(msg)` inline. Fixing it means either moving continuation acquisition off
  the inline path or making the drain condition edge-triggered — an architecture decision for
  the core owner, not a test.
- **`crates/testing` shared infrastructure (F1)**: needs a `ScatterHeavy`/cross-shard workload
  profile in `testing/src/workload.rs`. The quiescence checker and probe already exist and need
  no changes — this is the single cheapest high-severity item in the audit and benefits any
  other agent auditing lock/latch leaks.
- **shuttle harness (F7)**: round-1 issue 07 already established shuttle in `crates/testing`
  for the MultiWaiter exactly-once guard. The VLL model needs the same dependency plus a
  virtual-time/injectable-timeout primitive, which does not exist yet. If another area also
  wants deterministic timeout exploration, build it once and share it.
- **`crates/server` scripting owns F3's harness need**: there is currently no test helper for
  "run an `EVAL` whose keys span shards with `allow_cross_slot_standalone=true`". The config
  knob exists in `test-harness/src/server.rs`; only the scripting-side helper is missing.
- **Turmoil chaos config**: `vll_adapter.rs`'s chaos hooks apply only to the scatter sink
  (`with_chaos`), never to the continuation sink (`new`). Whoever owns the turmoil chaos config
  should decide whether to unify them (F16).
- **`core/tests/concurrency.rs:641 test_mset_cross_shard_partial_visibility`** asserts the
  opposite of VLL's guarantee against a mock cluster. It should be renamed to make clear it
  models a non-VLL path, or deleted — as written it will be cited as evidence that partial
  visibility is acceptable.
