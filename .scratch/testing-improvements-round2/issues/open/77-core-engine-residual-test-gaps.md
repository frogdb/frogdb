# core engine (shard/store/eviction) — residual test gaps (13 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/01 — residual findings after promotion to issues 19–76
Score: 13 findings, priority range 9–21
Area: frogdb-core — `shard/**`, `store/**`, `eviction/**`, `command.rs`, `command_spec.rs`, `conn_command.rs`, `registry.rs`, `error.rs`, `noop.rs`, `lib.rs`

## Context

This is the storage engine of `frogdb-core`: the shard worker and its execution/rollback/expiry
pipeline, the `Store` trait and `HashMapStore` (including the warm/tiered tier), the eviction
pool and rankers, and the command registry/spec machinery — **30,308 LOC** including inline
`#[cfg(test)]`. Coverage over the in-scope file set is **lines 14,345 / 16,594 = 86.4 %** and
**regions 23,765 / 26,898 = 88.4 %**; the proposal deliberately anchors every finding on per-file
`line_counts` rather than depth classes, because async/generic duplicate function records and
never-evaluated `tracing!` argument blocks make the class counts lie (the `hot-but-shallow`
signal on `eviction/pool.rs` and `eviction/ranker.rs` is an `#[inline]` symbol artifact, not a
gap). The proposal's verdict on the *shape* of that coverage: "Coverage here is high and the
*happy paths* are genuinely well tested. The risk is concentrated in the **failure branches of
correct-looking code**" — the bug class that escapes is *an I/O or limit failure on a write path
that returns a success-shaped answer while the data is gone*.

## Promoted elsewhere

- F2 → issue 41, `.scratch/testing-improvements-round2/issues/` (a failed spill silently becomes a
  real delete and replicates the `DEL`) **and** issue 20,
  `.scratch/testing-improvements-round2/issues/` (theme T2 — failure of a derived structure
  reported as success).
- F3 → issue 20, `.scratch/testing-improvements-round2/issues/` (theme T2 — a warm-tier read
  failure makes a live key read as absent while it stays in `data`/`expiry_index`/DBSIZE).
- F4 → issue 19, `.scratch/testing-improvements-round2/issues/` (theme T1 — nothing proves a WRITE
  command's mutated key set is contained in its declared `WalStrategy` key set;
  registry-consistency).

## Residual findings

### F1 — single-command WAL-failure rollback is 0-covered while the transaction twin is covered

- **Severity** 5 — this branch decides whether a client that got an error had its write reverted. If `capture_write_snapshot`/`rollback_snapshot` misbehave here, the client sees `IOERR` while the mutation stands in memory and is absent from the WAL: a silent divergence that survives until restart.
- **Likelihood** 4 — `wal_failure_policy = rollback` plus any disk error (ENOSPC, EIO, fsync failure) on a plain `SET`. Single commands vastly outnumber `EXEC`.
- **Effort** 2 — the seam already exists; `scenario_s6.rs:32-59` builds exactly the right worker (`with_wal_mode(WalMode::Fake)` + `with_fake_wal_failure(FakeFailure::AtWriteIndex(n))` + `set_wal_failure_policy_flag(Rollback)`). One new test file reusing that builder with `drive` instead of `exec_tx`.
- **Priority** 21
- **Evidence**: `core/src/shard/execution.rs:436-491` — `line_counts` is 0 for 440, 453-464, 466, 469-479, 481-482, 484-485, 487-489, i.e. the entire `rollback_mode` arm: `capture_write_snapshot`, the `Durability::Confirm` persist, the `WalPhase::AlreadyPersisted` success arm, `rollback_snapshot(snapshot.unwrap())`, `WalRollbacks::inc`, and the `IOERR WAL persistence failed` response. The transaction twin at `execution.rs:627-650` (`EXECABORT transaction aborted due to WAL failure`) is covered by `scenario_s6.rs:132`; only line 635 (a tracing arg) is zero. `core/src/shard/rollback.rs`'s 8 unit tests all hand-simulate the mutation and none involves a WAL failure.
- **Proposed test**: seed `k=v1`; with `FakeFailure::AtWriteIndex(1)` and policy `rollback`, drive `SET k v2`; assert (a) the response is an `IOERR WAL persistence failed…` error, (b) `GET k` returns `v1`, (c) `PTTL k` is unchanged, (d) the `FakeWalLog` contains no append for the failed write, (e) `WalRollbacks` incremented by 1. Repeat for `DEL k` (rollback must resurrect the key), `RENAME a b`, `EXPIRE k 100` (rollback must clear the newly added expiry), and for policy `continue` (mutation must *stand* and no rollback recorded).
- **Boundary**: **3 — `shard_driver`.** The behaviour is entirely engine-internal (WAL seam + store + response) and needs failure injection the socket layer cannot express; the s6 builder already proves level 3 is sufficient.

### F5 — OOM rejection after eviction gives up is never exercised

- **Severity** 4 — the three OOM exits in `check_memory_for_write` are the last line of defence against unbounded memory growth. If the loop is wrong the shard either accepts writes past `maxmemory` (OOM-kill / crash-loop) or evicts in a tight loop. Both are unavailability, not corruption, hence 4 not 5.
- **Likelihood** 3 — reached whenever eviction cannot free enough in 10 attempts: a single value larger than `maxmemory`, `noeviction`-adjacent configs, or a volatile policy with no TTL'd keys left. The existing suite only ever tests the "eviction succeeds" side.
- **Effort** 2 — worker + `with_eviction(EvictionConfig::new(small_limit, policy))`; no persistence needed for the `AllkeysLru` case.
- **Priority** 16
- **Evidence**: `core/src/shard/eviction.rs:99-113` — `line_counts` 0 for the `if self.is_over_memory_limit()` post-loop block, the `EvictionOomTotal::inc` inside it, and the trailing `Ok(())`; `eviction.rs:121` (`EvictionPolicy::NoEviction => false` inside `evict_one`) and `eviction.rs:344` (`delete_for_eviction` returning `false`) are also 0. `redis-regression/tests/maxmemory_regression.rs:94` (`noeviction_rejects_writes_over_limit`) reaches only the *pre-loop* early return at `eviction.rs:56-64`.
- **Proposed test**: set `maxmemory` below the size of one value, policy `allkeys-lru`, store one huge key, then write a second: assert an OOM error response, assert the pre-existing key is still readable (the failed write must not have evicted it to death), and assert `EvictionOomTotal` incremented exactly once. Add the `volatile-lru`-with-no-TTL'd-keys variant, which must exit through the `evict_one() == false` arm.
- **Boundary**: **3 — `shard_driver`.** `maxmemory` accounting is per shard, and level 4 can only observe `used_memory` aggregates; the "which exit did we take" assertion needs the shard.

### F6 — active expiry of a spilled (warm) key is never tested live

- **Severity** 4 — when a warm key expires, the sweep's `store.delete` must reach `uninstall`, whose warm-side settlement (`hashmap.rs:465-468`, `if !entry.is_hot() { self.warm_tier.remove_warm(key) }`) deletes the warm CF record and decrements the warm-key count. If that link ever breaks, the warm CF grows without bound *and* the stale record can resurrect the key on the next warm-tier recovery — silent resurrection of deleted data.
- **Likelihood** 3 — normal operation for anyone running tiered storage with TTLs; every spilled TTL'd key takes this path.
- **Effort** 2 — level-2 test on the store, or level-3 with `drive_expiry_tick`.
- **Priority** 16
- **Evidence**: `core/src/store/hashmap.rs:437-470` (`uninstall`); the only warm+expiry test is `tiered_storage.rs:159 test_expired_warm_key_cleaned_on_unspill`, which goes through the **lazy read** path (`unspill_key`), not the active sweep. Round 1's issue 42 added `test_tiered_spilled_key_past_ttl_does_not_resurrect`, but that key expires *during the shutdown window* and is resolved by recovery, so the live active-sweep-of-a-warm-key path is still untested — that is the precise residue this finding claims.
- **Proposed test**: spill a TTL'd key; let the TTL elapse; run one expiry tick; assert `warm_tier.warm_keys()` decremented, the warm CF no longer contains the key, `len()`/DBSIZE decremented, an `expired` (not `evicted`) notification fired, and — after reopening the warm store — the key does not come back.
- **Boundary**: **3 — `shard_driver` with `drive_expiry_tick` and a warm store**, because the notification/`expired_keys` half only exists at the worker. The warm-CF half alone would be level 2.

### F7 — cross-shard COPY's destination write is 0 % covered

- **Severity** 4 — `scatter_copy_set` is the only path that reconstitutes a copied value on another shard, and its comment says it is *newly* responsible for WAL persistence, keyspace notification, replication broadcast, dirty counter and tracking invalidation for the destination key. Untested, any of those can be wrong: the copy exists in RAM but not on the replica or in the WAL, and disappears at restart.
- **Likelihood** 3 — requires `server.allow_cross_slot_standalone = true` (default `false`, `config/src/server.rs:108`) with the default 4 shards; that is a documented, supported configuration and any COPY whose two keys hash to different shards takes it.
- **Effort** 3 — server integration test; `TestServerConfig` already exposes the `allow_cross_slot` knob (`test-harness/src/server.rs:174`).
- **Priority** 15
- **Evidence**: `core/src/shard/execution.rs:1056-1124` — `line_counts` 0 for the whole `scatter_copy_set` body including both the success and the `ERR failed to deserialize value for COPY` arms. Confirmed at the caller too: `server/src/connection/routing.rs:229-298` (`execute_cross_shard_copy`) is 0-covered end to end, while the file overall is 98/188 — so server tests *are* in the profiled run, this path simply is never entered. `server/tests/integration_copy.rs`'s 13 tests all resolve on a single shard.
- **Proposed test**: with `allow_cross_slot` enabled and ≥2 shards, pick a key pair that provably hashes to different shards and COPY each value type across; assert value, `TYPE`, `PTTL`, `REPLACE` semantics, the `0` return when the destination exists without `REPLACE`, and the deserialize-failure error path; then restart and assert the copy survived (proves the WAL write), and with a replica attached assert the replica has it (proves the broadcast).
- **Boundary**: **4 — server integration over RESP.** Cross-shard COPY is a two-phase *routing* behaviour that begins in the connection layer (`routing.rs:130`); the `shard_driver` harness has no router, so level 3 could only test half of it.
- **Cross-area (from the proposal)**: "F7 (cross-shard COPY) spans this crate and the server's routing layer (`server/src/connection/routing.rs:229-298`, also 0-covered). Coordinate with the server net/connection agent so the test is written once." The other end of the same path is claimed by issue 33, `.scratch/testing-improvements-round2/issues/` (§4 tests that cannot fail — 03/F2, the 15 COPY integration tests that are accidentally same-shard). Write the test once.

### F8 — eviction never prefers already-expired keys, and can report a live key's death as `evicted`

- **Severity** 3 — `sample_with_ranker` builds candidates from `get_metadata` with no expiry filter, so under memory pressure the pool can pick a key whose TTL has already elapsed but which the sweep has not reached. It is then removed via `delete_for_eviction`, which emits `key-evicted` and increments `evicted_keys` rather than `expired_keys`. Worse for capacity: a large backlog of expired-but-unswept keys is *not* reclaimed preferentially, so live keys are evicted while dead bytes stay resident.
- **Likelihood** 4 — normal operation. Any workload with short TTLs plus a `maxmemory` limit hits this on every eviction burst; the 100 ms sweep is intentionally budget-bounded (`active_expiry.rs`, 25 ms / 1024 keys) so a backlog is the expected steady state, not an edge case.
- **Effort** 2 — worker with eviction config and a controllable clock/backdated TTLs (`DebugProvider::expire_backdate` already exists as a seam).
- **Priority** 15
- **Evidence**: `core/src/shard/eviction.rs:157-189` — `sample_with_ranker` passes `metadata.expires_at` to `EvictionCandidate::from_metadata` only so `TtlRanker` can *rank* by it; nothing rejects an already-elapsed deadline. `eviction/ranker.rs`'s `TtlRanker` returns `u64::MAX - ttl_micros`, so an expired key ranks worst under `volatile-ttl` but is scored no differently under LRU/LFU. No test in `maxmemory_regression.rs`/`maxmemory_tcl.rs` mixes expired keys into the pressure workload.
- **Proposed test**: fill a shard with N keys whose TTLs have already elapsed plus M live keys, all under `allkeys-lru`; write until eviction fires; assert the live keys survive and that the removals are counted as `expired_keys` (with `expired` notifications), not `evicted_keys`. If today's behaviour is deemed acceptable, the test should still pin it explicitly and the divergence from Redis's `performEvictions` documented.
- **Boundary**: **3 — `shard_driver`.** Needs deterministic control over which keys are expired and direct observation of which counter moved; level 4 can see the counters but cannot make the race deterministic.

### F9 — the `Store` trait ships ~40 silent no-op default bodies

- **Severity** 4 — `Store` has ~40 methods with **default** implementations that quietly degrade rather than fail: `exists_unexpired` → `contains` (ignores expiry), `purge_if_expired` → `false`, `audit_expiry_index` → empty (the audit reports "clean" for a store that never implemented it), `scan_filtered` ignores the type filter, `recompute_memory_used` → `memory_used`, `get_with_expiry_check` → `get`. Any decorator that forgets to forward a method inherits a *wrong answer*, not a compile error — and the codebase already has such decorators (`BatchSpyStore` in `active_expiry.rs:527` forwards 20+ methods by hand).
- **Likelihood** 2 — `HashMapStore` is the only production impl today, so this is currently latent; it becomes live the moment a second impl or a production decorator (metrics, tracing, quota) appears.
- **Effort** 2 — one conformance test suite parameterised over `Store` impls.
- **Priority** 14
- **Evidence**: `core/src/store/mod.rs` — 111/320 lines covered (34.7 %); the uncovered lines are almost entirely the default bodies (contiguous 3-line blocks from 555 to 819). Note this file's low percentage is *not itself* the finding: the default bodies are dead only because `HashMapStore` overrides them. The finding is that nothing enforces that an impl must.
- **Proposed test**: a `store_conformance!` macro asserting the expiry-sensitive contract (a logically-expired key is invisible to `exists_unexpired`/`get_with_expiry_check`/`scan_filtered`, and `audit_expiry_index` genuinely reports a hand-injected inconsistency), run against `HashMapStore` and against a trivial forwarding decorator built *without* overrides — the latter must fail unless the defaults are correct. Separately, consider deleting the defaults so a missing method is a compile error.
- **Boundary**: **2 — crate-level API.** The contract is the trait's; no shard or socket adds anything.

### F10 — active-expiry field-phase budget exhaustion is untested

- **Severity** 3 — if the field phase's budget/`break` logic is wrong, hash-field expiry either stalls (fields never reclaimed — a slow leak and wrong `HGETALL` answers) or blows the 25 ms budget every tick, stalling the whole single-writer shard and every client on it.
- **Likelihood** 3 — needs a large volume of hash-field TTLs, which is exactly the workload hash-field expiry exists for.
- **Effort** 2 — the key-phase equivalent is already tested; this reuses `BatchSpyStore`.
- **Priority** 13
- **Evidence**: `core/src/shard/active_expiry.rs:169-170, 184, 189-190` — `line_counts` 0 for the field-phase early return on `budget_exhausted` and the "whole batch already purged" `break`. The key-phase twins are covered by `avalanche_exhausts_budget_and_next_cycle_resumes`.
- **Proposed test**: seed one hash with more expired fields than `DEFAULT_BATCH_SIZE`, with a per-field delay that guarantees budget exhaustion mid-batch; assert `ExpiryResult.budget_exhausted`, assert `fields_expired` is a prefix (not the whole set), and assert the next cycle resumes and eventually drains everything, with the global version bumped exactly once per cycle that expired ≥1 field.
- **Boundary**: **1/2 — unit test on `ActiveExpiryCoordinator` with the existing spy store.** `run_cycle` is directly callable and the assertion is on the returned `ExpiryResult`; adding a worker would only hide the budget arithmetic.

### F11 — the real `run()` select loop is never the thing under test

- **Severity** 3 — every engine test drives `drive`/`drive_expiry_tick`/`drive_waiter_timeout_tick`/`drive_continuation_release` directly, so an ordering or shutdown bug *in `run()` itself* is invisible to the suite. The shutdown tail specifically — final search-index commit then final WAL flush — governs whether a clean `SHUTDOWN` loses the last writes.
- **Likelihood** 3 — every shutdown, every restart, every rolling upgrade takes the tail; the VLL arm takes it on every cross-shard continuation release.
- **Effort** 3 — needs a test that spawns `run()` and interacts through the real channels, plus a way to assert the flush happened.
- **Priority** 12
- **Evidence**: `core/src/shard/event_loop.rs` — the loop body is hot (line 34 = 494,974 executions) yet `line_counts` is **0** for line 37 (`handle_new_connection`), 99-100 (the VLL continuation-release arm), 103 (`else => break`), 92 (search-commit error), 114 and 123 (the shutdown-tail search-commit and WAL-flush error branches). Line 37's zero is explained by F16 below (the channel has no senders at all). Line 99-100's zero means the continuation-release arm is only ever reached through s4's `drive_continuation_release` seam, never through `run()`.
- **Proposed test**: spawn a real worker's `run()`; issue writes over the real `Envelope` channel; send the shutdown message; await the task; then assert the WAL contains every acknowledged write (via `FakeWalLog`) and that a dirty search index was committed. Add a case where the WAL flush fails on exit and assert it is logged and does not panic or hang.
- **Boundary**: **3 — `shard_driver`**, extended with a "run the real loop" mode alongside today's step-driven mode. The behaviour is the loop's own; level 4 would attribute a shutdown data-loss failure to the whole server.

### F12 — eviction is only ever asserted at level 4, through `used_memory` deltas

- **Severity** 2 — not a correctness bug in itself, but it means the eviction tests cannot assert *which* key was chosen, only that memory went down. LRU/LFU/TTL ranking regressions (a reversed comparator, a pool that never evicts the worst candidate) would pass ~38 existing tests.
- **Likelihood** 4 — ranking quality degrades silently on any refactor of `EvictionPool`/`EvictionRanker`; the pool's own unit tests check the pool contract in isolation but never that the worker feeds it correctly.
- **Effort** 3 — requires teaching `shard_driver/harness.rs` to build workers with an `EvictionConfig` (and optionally a warm store); today it passes none.
- **Priority** 11
- **Evidence**: `core/tests/shard_driver/harness.rs:60-95` — `ShardWorkerBuilder` is called with `store`/`message_rx`/`new_conn_rx`/`shard_senders`/`registry` only; no `with_eviction`, no `with_persistence`, no `with_replication`. `redis-regression/tests/maxmemory_regression.rs:138-350` and `maxmemory_tcl.rs:79-131` assert `used_memory <= limit` and key-class membership, never key identity.
- **Proposed test**: add an eviction option to the harness, then a level-3 test per ranker asserting *identity*: touch keys in a known order, force exactly one eviction, assert the specific expected key is the one that disappeared, for `allkeys-lru`, `allkeys-lfu` and `volatile-ttl`. This is a **move-down**: it should replace (not duplicate) the weakest of the `*_honors_memory_limit` level-4 tests, which are slow and can only make a fuzzy assertion.
- **Boundary**: **3 — `shard_driver`** (harness extension required). Recommend keeping *one* level-4 test per policy as a wiring/parity smoke test and moving the semantics down.
- **OPTIONS** (shared with F14 — reproduced verbatim from the proposal's `OPTIONS on F12/F14 (where eviction semantics should live)`):
  1. **Extend `shard_driver` and move ranking assertions to level 3.** *Trade-off*: deterministic key-identity assertions, fast; requires the harness change, and the tests then depend on per-shard memory accounting internals.
  2. **Keep everything at level 4 and add `DEBUG`-assisted determinism** (e.g. `OBJECT FREQ`, `DEBUG SLEEP`, backdated idle times). *Trade-off*: no harness work and survives refactors, but `used_memory`-threshold tests are inherently fuzzy and slow, and cross-shard aggregation makes "which key" assertions unreliable.
  3. **Both**: level-3 for ranking semantics, one level-4 smoke test per policy for config wiring and RESP-visible stats.

  **Recommendation: option 3.** The harness change is shared with five other findings, so its cost is amortised, and keeping one thin level-4 test per policy preserves the parity guarantee that `maxmemory_tcl.rs` exists to provide.

### F13 — active-expiry budget tests are wall-clock dependent

- **Severity** 2 — a flaky test in the expiry suite gets muted or retried, and the avalanche-bounding contract silently stops being enforced.
- **Likelihood** 4 — `avalanche_exhausts_budget_and_next_cycle_resumes` sleeps 2 ms per delete against a 15 ms budget; on a loaded CI box (or under the coverage instrumentation that produced this very report) that margin is 7 deletes wide.
- **Effort** 3 — needs an injectable clock in `ActiveExpiryCoordinator` (currently `Instant::now()` inline), which is a small but real production-code change.
- **Priority** 11
- **Evidence**: `core/src/shard/active_expiry.rs` — the `BatchSpyStore::delete` `thread::sleep(self.delete_delay)` at line 536 and the 15 ms budget in the avalanche test.
- **Proposed test**: replace the sleep with a mock clock the test advances explicitly; assert budget exhaustion at an exact deterministic key count. Keep one wall-clock smoke test.
- **Boundary**: **1 — pure unit test**, once the clock is injectable. This is a *move-down* of an existing test's dependency, not a new level.

### F14 — `EvictionPolicy::TieredLfu` is never executed anywhere in the suite

- **Severity** 3 — a shipped, documented `maxmemory-policy` value. If its arm were mis-wired (e.g. to the wrong ranker or to `evict_*` instead of `spill_*`), it would delete data where the operator asked for demotion.
- **Likelihood** 2 — `tiered-lfu` is the less-used of the two tiered policies, but it is selectable in config and documented in `configuration.mdx:119`.
- **Effort** 2 — the `tiered-lru` server test at `server/tests/integration_persistence.rs:2352` is directly parameterisable over the policy string.
- **Priority** 11
- **Evidence**: `core/src/shard/eviction.rs:130` — `line_counts` 0 for `EvictionPolicy::TieredLfu => self.spill_with_ranker(false, &LfuRanker).await`, while line 129 (`TieredLru`) is covered. A repo-wide search for `tiered-lfu` outside `policy.rs`/docs returns no test.
- **Proposed test**: parameterise `tiered_config()` over the policy and run the existing spill-survives-restart assertions for `tiered-lfu`; additionally assert at level 3 that the *LFU* ranker chose the key (a frequently-touched key must survive a rarely-touched one).
- **Boundary**: **3 for the ranking assertion** (needs key identity, see F12), **4 to reuse the existing restart test** for the wiring. Cheapest split: reuse level 4 for wiring, add the identity assertion at level 3 once F12's harness change lands.
- **OPTIONS**: shared with F12 — see the `OPTIONS on F12/F14` block reproduced under F12 above; recommendation is option 3.

### F16 — dead seams that inflate the "uncovered" surface and should be deleted, not tested

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`). `MASTER.md`
§5 cites no finding numbers, so it claims nothing on its own, and none of the three items below
appear in its list — but this finding is the same "delete, do not test" shape and should be
executed with that sweep rather than beside it.

- **Severity** 2 — dead code is a maintenance and review hazard (reviewers assume `handle_new_connection` is live), and it makes coverage numbers lie.
- **Likelihood** 3 — the cost is paid on every future change to these files.
- **Effort** 1 — deletion, plus running the suite.
- **Priority** 11
- **Evidence**:
  - `core/src/error.rs` — **0/69 lines**. `FrogDbError` (with `Display`, `Error::source`, `to_bytes`, `to_response`, 7 `From` impls and a `RespError` impl) is referenced only by `error.rs` itself and its `pub use` in `lib.rs`. Nothing constructs it.
  - `core/src/shard/event_loop.rs:37` + the whole `NewConnection` channel — `new_conn_rx` is plumbed through `builder.rs`, `server/src/acceptor.rs:110`, `server/src/server/init.rs:53-54` and `shards.rs:24`, but **no `send` of a `NewConnection` exists anywhere in the workspace** outside test setup. `handle_new_connection` can never run.
  - `core/src/shard/worker.rs:336-350, 386-411, 503-519` — the legacy `ShardWorker::new`, `with_eviction` and `with_fake_persistence` constructors, superseded by `ShardWorkerBuilder`; `with_fake_persistence` is 0-covered even though the fake-WAL tests exist (they use the builder directly).
- **Proposed test**: none. Delete `error.rs`, the `NewConnection` channel end to end, and the superseded constructors. If `FrogDbError` is a planned future unification of error handling, it belongs in a proposal, not in `src/`.
- **Boundary**: n/a — code removal.
- **Cross-area (from the proposal)**: "F16's `NewConnection` deletion touches `server/src/acceptor.rs`, `server/src/server/init.rs` and `server/src/server/shards.rs` — needs the server agent's sign-off."

### F15 — shard-level negative paths (unknown command, bad arity) are unreachable in tests

- **Severity** 2 — these are defence-in-depth guards behind the connection layer's own validation. If they regressed, a malformed internal message (replication apply, script call, cluster forward) would panic or mis-dispatch rather than return a clean error.
- **Likelihood** 2 — requires a path that reaches the shard without connection-layer validation: replica apply of a command the replica does not know (version skew during a rolling upgrade) is the realistic one.
- **Effort** 1 — two `drive` calls.
- **Priority** 9
- **Evidence**: `core/src/shard/execution.rs:132-150` — `line_counts` 0 for the `unknown command` and `wrong number of arguments` early returns; the connection layer rejects both before routing, so the shard-level guards are never entered.
- **Proposed test**: drive a `ParsedCommand` with an unregistered name and one with the wrong arity through the shard; assert the exact error strings and that no store mutation or WAL append occurred.
- **Boundary**: **3 — `shard_driver`**, one line each; this is the only level that can bypass connection-layer validation.

## Acceptance criteria

- [ ] F1: a `shard_driver` test asserts that a single-command `SET` failing at the WAL under policy `rollback` returns `IOERR WAL persistence failed…`, leaves `GET k` at its prior value and `PTTL k` unchanged, appends nothing to the `FakeWalLog`, and increments `WalRollbacks` by exactly 1 — with `DEL`, `RENAME`, `EXPIRE` and policy-`continue` variants.
- [ ] F5: a test asserts that a write which eviction cannot satisfy returns an OOM error, leaves the pre-existing key readable, and increments `EvictionOomTotal` exactly once; plus a `volatile-lru`-with-no-TTL'd-keys variant that exits through the `evict_one() == false` arm.
- [ ] F6: a test asserts that one expiry tick over an elapsed TTL'd *spilled* key decrements `warm_tier.warm_keys()` and `len()`/DBSIZE, removes the warm CF record, fires an `expired` (not `evicted`) notification, and that the key does not return after reopening the warm store.
- [ ] F7: a test with `allow_cross_slot` enabled and ≥2 shards asserts that COPY across shards preserves value bytes, `TYPE` and `PTTL`, honours `REPLACE`/no-`REPLACE` (`0` return), exercises the deserialize-failure arm, and that the copy survives a restart and reaches an attached replica.
- [ ] F8: a test asserts that under `allkeys-lru` with a mix of already-elapsed and live keys, the live keys survive and the removals are counted as `expired_keys` with `expired` notifications rather than `evicted_keys` — or explicitly pins today's behaviour with the divergence from Redis's `performEvictions` documented.
- [ ] F9: a `store_conformance!` suite asserts the expiry-sensitive `Store` contract (`exists_unexpired`, `get_with_expiry_check`, `scan_filtered`, `audit_expiry_index`) and is run against both `HashMapStore` and a forwarding decorator with no overrides, the latter failing unless the trait defaults are correct.
- [ ] F10: a unit test on `ActiveExpiryCoordinator` asserts `ExpiryResult.budget_exhausted` on the *field* phase, that `fields_expired` is a prefix of the expired set, that the next cycle resumes and drains, and that the global version bumps exactly once per cycle that expired ≥1 field.
- [ ] F11: a test spawns the real `run()` loop, drives writes over the real `Envelope` channel, sends shutdown, and asserts every acknowledged write is in the `FakeWalLog` and a dirty search index was committed — plus a case where the exit-path WAL flush fails and the loop neither panics nor hangs.
- [ ] F12: a level-3 test per ranker asserts the *identity* of the evicted key for `allkeys-lru`, `allkeys-lfu` and `volatile-ttl`, and the weakest `*_honors_memory_limit` level-4 tests are replaced rather than duplicated.
- [ ] F13: the avalanche budget test asserts exhaustion at an exact deterministic key count via an injectable clock, with at most one wall-clock smoke test remaining.
- [ ] F14: a test executes `EvictionPolicy::TieredLfu` (`eviction.rs:130`) and asserts the LFU ranker's choice — a frequently-touched key survives a rarely-touched one — plus the spill-survives-restart assertions parameterised over `tiered-lfu`.
- [ ] F16: `core/src/error.rs`, the `NewConnection` channel (core + `acceptor.rs`/`init.rs`/`shards.rs`) and the superseded `ShardWorker::new`/`with_eviction`/`with_fake_persistence` constructors are deleted and the suite passes — no test is added for them.
- [ ] F15: a `shard_driver` test asserts the exact `unknown command` and `wrong number of arguments` error strings from `execution.rs:132-150` and that neither produced a store mutation or a WAL append.

## Depends on

- Infrastructure I1 (`shard_driver` harness extension — `with_eviction(EvictionConfig)`, an optional
  warm/persistent store) — issue 01, `.scratch/testing-improvements-round2/issues/`. Needed by F5,
  F6, F8, F12 and F14; the proposal calls it "a small piece of **shared infrastructure** … it
  should be built once, first", and every builder option already exists on `ShardWorkerBuilder`
  (`builder.rs:207, 225, 286`) and is simply not forwarded by `harness.rs:60-95`. F11 additionally
  wants the same harness to grow a "run the real loop" mode.
- Infrastructure I16 (promote the fake-WAL failure fixture from `scenario_s6.rs:32-59` into
  `harness.rs`) — issue 16, `.scratch/testing-improvements-round2/issues/`. Needed by F1;
  `FakeFailure::Predicate(fn(write_index, key) -> bool)` already exists and is unused.
- Infrastructure I3 (injectable clock seam) — issue 03,
  `.scratch/testing-improvements-round2/issues/`. Needed by F13, which requires an injectable clock
  in `ActiveExpiryCoordinator` (today `Instant::now()` inline). `MASTER.md` §6 lists area 01 among
  I3's requesters; `INFRASTRUCTURE.md`'s "smallest useful slice" note scopes I3 to the expiry path,
  which is exactly what F13 needs.

## Re-triage 2026-08-06

**Verdict: partially-fixed** — 1/13 findings discharged (F1), 1 partially (F13).

| finding | verdict | evidence |
|---|---|---|
| F1 single-command WAL-failure rollback 0-covered | **fixed** | FM-PERSISTENCE-006 forced by `wal_failure_in_rollback_mode_replies_ioerr_and_restores_the_key` (`crates/core/src/shard/rollback.rs:573`) + `test_rollback_{existing_key,missing_key,del_restores_key,rename,preserves_expiry,clears_added_expiry}`; continue-mode twin is FM-PERSISTENCE-005 |
| F5 OOM rejection after eviction gives up | still-valid | branch unchanged, no forcing test |
| F6 active expiry of a spilled (warm) key | still-valid | |
| F7 cross-shard COPY destination write | still-valid | |
| F8 eviction never prefers already-expired keys | still-valid | |
| F9 `Store` trait ~40 silent no-op defaults | still-valid | |
| F10 active-expiry field-phase budget exhaustion | still-valid | |
| F11 real `run()` select loop never under test | still-valid | harness still drives `drive*` seams only |
| F12 eviction asserted only at level 4 | still-valid | builder still takes no `with_eviction` |
| F13 active-expiry budget tests wall-clock dependent | **partially-fixed** | production half landed |
| F14 `EvictionPolicy::TieredLfu` never executed | still-valid | |
| F16 dead seams (delete, don't test) | still-valid | tracked with issue 34 |
| F15 shard-level negative paths unreachable | still-valid | |

Stale references corrected: the boundary-3 harness moved `core/tests/shard_driver/harness.rs` →
`frogdb-server/crates/shard-harness/src/harness.rs`, with the scenario tests now in
`frogdb-server/crates/shard-harness/tests/` (`scenario_s1..s8`, `shard_driver.rs`); F1's cited
`scenario_s6.rs:32-59` builder is now `crates/shard-harness/tests/scenario_s6.rs:34`
(`build_rollback_worker`) and still covers only the `EXEC` twin — the single-command arm is
discharged by the `rollback.rs` unit tests above, not by s6. F13's production prerequisite is
**done**: the clock-seam sweep (2fb1051c, 0fe2dd0a) replaced the inline `Instant::now()` in
`ActiveExpiryCoordinator`'s `Budget` with `crate::clock::now()`
(`crates/core/src/shard/active_expiry.rs:96,102`), so an injectable clock now exists; the test half
is unchanged — `avalanche_exhausts_budget_and_next_cycle_resumes` (`:656`) still uses a 2 ms
per-delete `delete_delay` against a 15 ms wall-clock budget and asserts only
`deleted_keys.len() < total`. Note the seam's own doc (`:82`) records that a *paused* runtime never
advances the budget, so the deterministic rewrite must step the clock explicitly rather than merely
pausing it.
