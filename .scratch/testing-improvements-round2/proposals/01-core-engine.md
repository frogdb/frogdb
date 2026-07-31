# frogdb-core storage engine — testing gap audit (round 2)

## Scope

Paths audited (`frogdb-server/crates/core/src/`): `shard/**`, `store/**`, `eviction/**`,
`command.rs`, `command_spec.rs`, `conn_command.rs`, `registry.rs`, `error.rs`, `noop.rs`,
`lib.rs`. **30,308 LOC** (inline `#[cfg(test)]` included).

Out of scope and left to other agents: `persistence/`, `scripting/`, `client_registry/`,
`observability/`, `pubsub.rs`, `tracking.rs`, `keyspace_event.rs`, `keyspace_stats.rs`,
`hotkeys.rs`, `slowlog.rs`, `latency*.rs`, `metrics.rs`, `probes.rs`. `shard/blocking.rs` and
`shard/wait_queue.rs` are being audited concurrently by the blocking-commands agent; I inventoried
them but filed no findings there to avoid collision.

Coverage (computed from `target/llvm-cov/depth/depth.json`, filtered to the in-scope file set):

| metric | value |
|---|---|
| lines | 14,345 / 16,594 = **86.4 %** |
| regions | 23,765 / 26,898 = **88.4 %** |

Depth classes for in-scope functions: `untested` 6350, `single-test` 1062, `monoculture` 854,
`covered` 402, `well-covered` 2878, `hot-but-shallow` 10.

Two caveats on those class counts, both verified by hand before using them:

- Async fns and generic instantiations emit duplicate function records (e.g.
  `execute_transaction::{closure#0}` appears twice, once covered, once not), and `#[inline]`
  fns never hit their standalone symbol in a unit-test binary. That is exactly why
  `eviction/pool.rs` and `eviction/ranker.rs` show up as `hot-but-shallow` in the summary
  report despite having real, adequate unit tests. **The `hot-but-shallow` signal for eviction
  is an instrumentation artifact, not a gap.**
- Many zero-count lines are `tracing::warn!/error!` argument blocks that never evaluate with no
  subscriber installed. I filtered these out before treating a zero line as a gap.

Consequently every finding below is anchored on **per-file `line_counts`** (which the coverage
report states matches lcov exactly), not on the function classes.

## Summary

Coverage here is high and the *happy paths* are genuinely well tested. The risk is concentrated
in the **failure branches of correct-looking code**, and in three structural facts:

1. **The `shard_driver` harness builds workers with no persistence, no WAL, no eviction config and
   no replication broadcaster** (`tests/shard_driver/harness.rs`). Every WAL-failure, eviction and
   tiering behaviour is therefore either (a) an inline unit test that hand-simulates the mutation,
   or (b) a level-4 RESP test that pokes `maxmemory` and asserts a memory number went down. There
   is no level-3 test of the engine's degradation behaviour.
2. **Tiering failure silently becomes deletion.** `spill_for_eviction`'s `Err(_)` arm falls back to
   a real `delete_for_eviction`, and `unspill_key`'s RocksDB-read/deserialize failure arms return
   `None` (read as "key absent") while the key stays in `data`, in the expiry index and in DBSIZE.
   Both arms are 0-covered. A disk-full or corrupt warm CF turns tiered storage into a data
   shredder with no error surfaced to the client.
3. **The single-command WAL-rollback path is 0-covered while its transaction twin is covered.**
   Round 1's S6 scenario built the whole fake-WAL failure-injection seam and then used it only for
   `EXEC`.

The bug class that escapes today is: *an I/O or limit failure on a write path that returns a
success-shaped answer while the data is gone*. Encoding/listpack conversion thresholds are **not**
in this crate (they live in `types`) — see Cross-area notes.

## Existing test inventory

| surface | what it covers | notable strengths | notable blind spots |
|---|---|---|---|
| inline `#[cfg(test)]` in `shard/eviction.rs` (`eviction_effect_tests`) | `evicted` notification + DEL replication, durability across a real RocksDB restart (`recover_shard`), spill preserves WATCH version, spill emits no `evicted` event | uses a real RocksStore + WAL, asserts *observable* effects not internals | never drives `check_memory_for_write`; no spill-failure, no `TieredLfu`, no OOM path |
| inline `eviction/pool.rs`, `eviction/ranker.rs` | `check_pool_contract` over LRU/LFU/TTL, `pop_worst`, `remove`, `clear`, TTL rejects keys without TTL | genuinely good, ranker-parametric | pool never driven from a real store under real memory pressure |
| inline `shard/rollback.rs` (8 tests) | snapshot capture/restore for missing/existing/rename/DEL/expiry cases, `Arc` sharing, policy-flag toggling | precise, cheap | **every test hand-simulates the mutation**; none runs a real command, none involves a real WAL failure |
| inline `shard/active_expiry.rs` | `BatchSpyStore` decorator, `avalanche_scan_stays_bounded_within_a_cycle`, `avalanche_exhausts_budget_and_next_cycle_resumes` | pins the batching contract at the right level | key-phase budget only; **field-phase exhaustion 0-covered**; uses `thread::sleep(2ms)` vs a 15 ms budget |
| inline `shard/event_loop.rs` (`effect_tests`) | notifications for both deletion paths, `expired_keys` not double-counted | good coalescing assertions | drives `apply_expiry_effects` directly, never `run()` |
| `core/tests/shard_driver/` s1–s6, s8 (no s7) | s1 dual-timeout race, s2 WATCH vs expiry vs slot version, s3 VLL phase-2/3 failure, s4 continuation-lock holder panic, s5 XREADGROUP key-death (DEL / sweep / lazy read), s6 **fake-WAL mid-transaction rollback**, s8 expiry sweep vs EXEC | real worker, real dispatch, real WAL seam; s6 already owns `with_wal_mode(Fake)` + `FakeFailure::AtWriteIndex` + `set_wal_failure_policy_flag` | harness workers have **no** persistence/eviction/replication; no eviction, tiering or memory-pressure scenario exists |
| `core/tests/tiered_storage.rs` (18 tests) | spill/unspill cycle, warm overwrite/delete/contains/key_type/get_mut, expired-warm cleanup on unspill, warm keys excluded from eviction sample, `clear()` cleans warm CF, memory accounting, recovery + real reopen | thorough at level 2 on `HashMapStore` + real `RocksStore` | only `test_spill_errors` touches a failure; **no test of a warm read that fails after a successful spill**; nothing drives spill via the eviction policy |
| `core/tests/concurrency.rs` (55 tests) | conn-id, read-your-writes, ordering, per-type concurrent mutation, MSET cross-shard visibility, MULTI/EXEC isolation, WATCH races, shuttle snapshot tests, PCT tests, blocking-pop conservation model | strong; shuttle + PCT + a conservation model with a deliberately buggy protocol as a negative control | nothing about eviction, tiering or memory pressure under concurrency |
| `core/tests/proptest_{glob,json,serialization,types}.rs` | glob matching, JSON, serialization round-trips, type invariants | good pure-function property coverage | **no property test over the command registry's declared specs vs. actual behaviour** |
| `redis-regression/tests/maxmemory_regression.rs` (13) + `maxmemory_tcl.rs` (~25) | every non-tiered policy "honors memory limit", volatile-only vs allkeys key selection, `maxmemory 0`, `evicted_keys`, `OBJECT IDLETIME/FREQ`, client-eviction interactions | the only real eviction coverage; parity-shaped | level 4 over RESP asserting `used_memory` deltas; **cannot express "which key was evicted" deterministically**; never reaches OOM-after-max-attempts; never uses `tiered-*` |
| `server/tests/integration_persistence.rs:2342-2620` | `test_tiered_spill_survives_restart`, `test_tiered_spilled_key_past_ttl_does_not_resurrect` (round-1 issue 42) | proves a *genuine* spill happened via `INFO tiered` before restarting | `tiered-lru` only; no live (no-restart) warm-tier expiry; no spill-failure |
| `server/tests/integration_copy.rs` (13) | COPY basic/exists/REPLACE/TTL/per-type, DB rejected | — | **all key pairs land such that `execute_cross_shard_copy` is never entered — the whole cross-shard COPY path is 0-covered** |

## Findings

### F1: single-command WAL-failure rollback is 0-covered while the transaction twin is covered
- **Severity** 5 — this branch decides whether a client that got an error had its write reverted. If `capture_write_snapshot`/`rollback_snapshot` misbehave here, the client sees `IOERR` while the mutation stands in memory and is absent from the WAL: a silent divergence that survives until restart.
- **Likelihood** 4 — `wal_failure_policy = rollback` plus any disk error (ENOSPC, EIO, fsync failure) on a plain `SET`. Single commands vastly outnumber `EXEC`.
- **Effort** 2 — the seam already exists; `scenario_s6.rs:32-59` builds exactly the right worker (`with_wal_mode(WalMode::Fake)` + `with_fake_wal_failure(FakeFailure::AtWriteIndex(n))` + `set_wal_failure_policy_flag(Rollback)`). One new test file reusing that builder with `drive` instead of `exec_tx`.
- **Priority** 21
- **Evidence**: `core/src/shard/execution.rs:436-491` — `line_counts` is 0 for 440, 453-464, 466, 469-479, 481-482, 484-485, 487-489, i.e. the entire `rollback_mode` arm: `capture_write_snapshot`, the `Durability::Confirm` persist, the `WalPhase::AlreadyPersisted` success arm, `rollback_snapshot(snapshot.unwrap())`, `WalRollbacks::inc`, and the `IOERR WAL persistence failed` response. The transaction twin at `execution.rs:627-650` (`EXECABORT transaction aborted due to WAL failure`) is covered by `scenario_s6.rs:132`; only line 635 (a tracing arg) is zero. `core/src/shard/rollback.rs`'s 8 unit tests all hand-simulate the mutation and none involves a WAL failure.
- **Proposed test**: seed `k=v1`; with `FakeFailure::AtWriteIndex(1)` and policy `rollback`, drive `SET k v2`; assert (a) the response is an `IOERR WAL persistence failed…` error, (b) `GET k` returns `v1`, (c) `PTTL k` is unchanged, (d) the `FakeWalLog` contains no append for the failed write, (e) `WalRollbacks` incremented by 1. Repeat for `DEL k` (rollback must resurrect the key), `RENAME a b`, `EXPIRE k 100` (rollback must clear the newly added expiry), and for policy `continue` (mutation must *stand* and no rollback recorded).
- **Boundary**: **3 — `shard_driver`.** The behaviour is entirely engine-internal (WAL seam + store + response) and needs failure injection the socket layer cannot express; the s6 builder already proves level 3 is sufficient.

### F2: a failed spill silently becomes a real delete (tiered storage under disk failure)
- **Severity** 5 — `spill_for_eviction`'s `Err(_)` arm falls through to `delete_for_eviction`, which routes a *real* removal through `run_internal_removal_effects`: WAL delete, replicated DEL, `evicted` notification. A transient RocksDB write failure therefore destroys the value permanently, on the primary *and* on every replica, and the client's write succeeds.
- **Likelihood** 3 — tiered storage is by definition disk-heavy; ENOSPC / EIO / a RocksDB write stall on the warm CF is an ordinary ops event for exactly the deployments that enable it.
- **Effort** 2 — `core/tests/tiered_storage.rs:265` (`test_spill_errors`) already constructs the error cases at level 2; the missing piece is asserting the *policy* consequence, which needs a worker with `with_eviction(EvictionConfig::new(limit, TieredLru))` and a warm store — both builder options exist.
- **Priority** 19
- **Evidence**: `core/src/shard/eviction.rs:269-277` — `line_counts` 0 for the `Err(e) => { … self.delete_for_eviction(key).await }` arm. The doc comment above it (`eviction.rs:240-254`) explicitly says "Only its fallback-to-delete path is a real removal", i.e. the design is aware the fallback is destructive; nothing tests it. `core/src/store/hashmap.rs:771-772` (the `SpillError::Rocks` return) is likewise 0-covered.
- **Proposed test**: with `tiered-lru` and a warm store whose `try_put` fails, drive writes until eviction triggers, and assert the *decided* policy: either (a) the key is deleted and a `key-evicted` notification + replicated DEL are observed (pinning today's behaviour as intentional), or (b) — my recommendation for the product — the write is rejected with OOM and the key survives. Either way the test must assert the observable consequence, not that `spill_key` returned `Err`.
- **Boundary**: **3 — `shard_driver` (with a warm store)**, because the assertion is about the *replication + notification + WAL* effects of the fallback, which only the worker's effect pipeline produces. A level-2 store test cannot see them.
- **OPTIONS**: this finding contains a product decision, not only a test decision — see the OPTIONS block below.

### F3: a warm-tier read failure makes a live key read as absent while still counted
- **Severity** 5 — in `unspill_key`, "missing from RocksDB", "failed to read warm key" and "failed to deserialize warm key" all `return None`, which every caller interprets as *key does not exist*. The entry is **not** removed: it stays in `self.data`, in `expiry_index`, in `DBSIZE` and in the memory accounting. So `GET k` → nil, `EXISTS k` → 1, `DBSIZE` counts it, and it is never reclaimed. That is silent data loss *plus* a permanently inconsistent keyspace.
- **Likelihood** 3 — requires a warm CF read error or a corrupt/partially-written record; ordinary for a database that has crashed mid-write, and the deserialize arm also fires on any future serialization-format skew.
- **Effort** 2 — level-2 test on `HashMapStore` + a warm store, by corrupting the warm CF record after a successful spill (`tiered_storage.rs` already opens a real `RocksStore` and can write the CF directly).
- **Priority** 19
- **Evidence**: `core/src/store/hashmap.rs:813-827` — `line_counts` 0 for the `Ok(None)`, `Err(e)` and deserialize-`Err` arms of `unspill_key`. Contrast `hashmap.rs:800-806`, the *expired*-warm-key arm, which correctly calls `uninstall` + `note_expired_on_unspill` and **is** covered (`tiered_storage.rs:159`). The failure arms take no such repair action.
- **Proposed test**: spill a key; corrupt (or delete) its warm CF record out-of-band; then assert the chosen contract — `EXISTS`/`DBSIZE`/`GET`/`TYPE`/`SCAN` must agree with each other. Add the mirror case for a truncated (deserialize-failing) record. Assert an error counter or a hard error is surfaced, rather than a nil that is indistinguishable from a missing key.
- **Boundary**: **2 — crate-level API on `HashMapStore`.** The inconsistency is entirely inside the store; adding a worker or a socket would only obscure which structure disagrees. `core/tests/tiered_storage.rs` is the right file.

### F4: nothing proves a WRITE command's mutated key set is contained in its declared WAL key set
- **Severity** 5 — `capture_write_snapshot` derives the snapshot keys from `handler.wal_strategy().actions(args)` (`core/src/shard/rollback.rs:31-70`). If any WRITE handler mutates a key that its `WalStrategy` does not name, then on a WAL failure the rollback is *partial*: some keys revert, some do not, and the client is told the write failed. The same mismatch also means the un-named key is never written to the WAL at all, so it is lost on restart.
- **Likelihood** 3 — this is a latent whole-registry invariant: any new or edited command can break it, and nothing catches it. `registry.rs:184` `debug_assert!(command.spec().validate().is_ok())` checks only the *declarative* consistency rules in `command_spec.rs` (arity vs keys, WRITE vs event, movable-keys, reindex shape) — it cannot check "declared keys ⊇ actually-touched keys".
- **Effort** 3 — needs a store decorator that records every key touched by `set/delete/get_mut/set_expiry/…`, plus a per-command argument generator; `core/tests/shard_driver/generator.rs` and the `BatchSpyStore` pattern in `active_expiry.rs:527` are both existing precedent.
- **Priority** 18
- **Evidence**: `core/src/shard/rollback.rs:31-70` (the `WalAction::ClearShard` filter is documented as deliberately unrollbackable); `core/src/registry.rs:180-216`; `core/src/command_spec.rs:1026-1160` — every `validate()` test builds a hand-made spec, none iterates the real registry.
- **Proposed test**: a **property test** over `register_all`'s registry: for each WRITE command, generate a well-formed argument vector, execute it against a recording store decorator, and assert `touched_keys ⊆ wal_strategy().actions(args).keys()` (modulo the documented `ClearShard` exemption). Failing that invariant should name the offending command.
- **Boundary**: **3 — `shard_driver` harness**, because the check needs real dispatch through the real registry with a store it can observe. A pure unit test cannot enumerate handlers; a level-4 test cannot see touched keys.
- **OPTIONS**: see the OPTIONS block below (property test vs. compile-time/derive check).

### F5: OOM rejection after eviction gives up is never exercised
- **Severity** 4 — the three OOM exits in `check_memory_for_write` are the last line of defence against unbounded memory growth. If the loop is wrong the shard either accepts writes past `maxmemory` (OOM-kill / crash-loop) or evicts in a tight loop. Both are unavailability, not corruption, hence 4 not 5.
- **Likelihood** 3 — reached whenever eviction cannot free enough in 10 attempts: a single value larger than `maxmemory`, `noeviction`-adjacent configs, or a volatile policy with no TTL'd keys left. The existing suite only ever tests the "eviction succeeds" side.
- **Effort** 2 — worker + `with_eviction(EvictionConfig::new(small_limit, policy))`; no persistence needed for the `AllkeysLru` case.
- **Priority** 16
- **Evidence**: `core/src/shard/eviction.rs:99-113` — `line_counts` 0 for the `if self.is_over_memory_limit()` post-loop block, the `EvictionOomTotal::inc` inside it, and the trailing `Ok(())`; `eviction.rs:121` (`EvictionPolicy::NoEviction => false` inside `evict_one`) and `eviction.rs:344` (`delete_for_eviction` returning `false`) are also 0. `redis-regression/tests/maxmemory_regression.rs:94` (`noeviction_rejects_writes_over_limit`) reaches only the *pre-loop* early return at `eviction.rs:56-64`.
- **Proposed test**: set `maxmemory` below the size of one value, policy `allkeys-lru`, store one huge key, then write a second: assert an OOM error response, assert the pre-existing key is still readable (the failed write must not have evicted it to death), and assert `EvictionOomTotal` incremented exactly once. Add the `volatile-lru`-with-no-TTL'd-keys variant, which must exit through the `evict_one() == false` arm.
- **Boundary**: **3 — `shard_driver`.** `maxmemory` accounting is per shard, and level 4 can only observe `used_memory` aggregates; the "which exit did we take" assertion needs the shard.

### F6: active expiry of a spilled (warm) key is never tested live
- **Severity** 4 — when a warm key expires, the sweep's `store.delete` must reach `uninstall`, whose warm-side settlement (`hashmap.rs:465-468`, `if !entry.is_hot() { self.warm_tier.remove_warm(key) }`) deletes the warm CF record and decrements the warm-key count. If that link ever breaks, the warm CF grows without bound *and* the stale record can resurrect the key on the next warm-tier recovery — silent resurrection of deleted data.
- **Likelihood** 3 — normal operation for anyone running tiered storage with TTLs; every spilled TTL'd key takes this path.
- **Effort** 2 — level-2 test on the store, or level-3 with `drive_expiry_tick`.
- **Priority** 16
- **Evidence**: `core/src/store/hashmap.rs:437-470` (`uninstall`); the only warm+expiry test is `tiered_storage.rs:159 test_expired_warm_key_cleaned_on_unspill`, which goes through the **lazy read** path (`unspill_key`), not the active sweep. Round 1's issue 42 added `test_tiered_spilled_key_past_ttl_does_not_resurrect`, but that key expires *during the shutdown window* and is resolved by recovery, so the live active-sweep-of-a-warm-key path is still untested — that is the precise residue this finding claims.
- **Proposed test**: spill a TTL'd key; let the TTL elapse; run one expiry tick; assert `warm_tier.warm_keys()` decremented, the warm CF no longer contains the key, `len()`/DBSIZE decremented, an `expired` (not `evicted`) notification fired, and — after reopening the warm store — the key does not come back.
- **Boundary**: **3 — `shard_driver` with `drive_expiry_tick` and a warm store**, because the notification/`expired_keys` half only exists at the worker. The warm-CF half alone would be level 2.

### F7: cross-shard COPY's destination write is 0 % covered
- **Severity** 4 — `scatter_copy_set` is the only path that reconstitutes a copied value on another shard, and its comment says it is *newly* responsible for WAL persistence, keyspace notification, replication broadcast, dirty counter and tracking invalidation for the destination key. Untested, any of those can be wrong: the copy exists in RAM but not on the replica or in the WAL, and disappears at restart.
- **Likelihood** 3 — requires `server.allow_cross_slot_standalone = true` (default `false`, `config/src/server.rs:108`) with the default 4 shards; that is a documented, supported configuration and any COPY whose two keys hash to different shards takes it.
- **Effort** 3 — server integration test; `TestServerConfig` already exposes the `allow_cross_slot` knob (`test-harness/src/server.rs:174`).
- **Priority** 15
- **Evidence**: `core/src/shard/execution.rs:1056-1124` — `line_counts` 0 for the whole `scatter_copy_set` body including both the success and the `ERR failed to deserialize value for COPY` arms. Confirmed at the caller too: `server/src/connection/routing.rs:229-298` (`execute_cross_shard_copy`) is 0-covered end to end, while the file overall is 98/188 — so server tests *are* in the profiled run, this path simply is never entered. `server/tests/integration_copy.rs`'s 13 tests all resolve on a single shard.
- **Proposed test**: with `allow_cross_slot` enabled and ≥2 shards, pick a key pair that provably hashes to different shards and COPY each value type across; assert value, `TYPE`, `PTTL`, `REPLACE` semantics, the `0` return when the destination exists without `REPLACE`, and the deserialize-failure error path; then restart and assert the copy survived (proves the WAL write), and with a replica attached assert the replica has it (proves the broadcast).
- **Boundary**: **4 — server integration over RESP.** Cross-shard COPY is a two-phase *routing* behaviour that begins in the connection layer (`routing.rs:130`); the `shard_driver` harness has no router, so level 3 could only test half of it.

### F8: eviction never prefers already-expired keys, and can report a live key's death as `evicted`
- **Severity** 3 — `sample_with_ranker` builds candidates from `get_metadata` with no expiry filter, so under memory pressure the pool can pick a key whose TTL has already elapsed but which the sweep has not reached. It is then removed via `delete_for_eviction`, which emits `key-evicted` and increments `evicted_keys` rather than `expired_keys`. Worse for capacity: a large backlog of expired-but-unswept keys is *not* reclaimed preferentially, so live keys are evicted while dead bytes stay resident.
- **Likelihood** 4 — normal operation. Any workload with short TTLs plus a `maxmemory` limit hits this on every eviction burst; the 100 ms sweep is intentionally budget-bounded (`active_expiry.rs`, 25 ms / 1024 keys) so a backlog is the expected steady state, not an edge case.
- **Effort** 2 — worker with eviction config and a controllable clock/backdated TTLs (`DebugProvider::expire_backdate` already exists as a seam).
- **Priority** 15
- **Evidence**: `core/src/shard/eviction.rs:157-189` — `sample_with_ranker` passes `metadata.expires_at` to `EvictionCandidate::from_metadata` only so `TtlRanker` can *rank* by it; nothing rejects an already-elapsed deadline. `eviction/ranker.rs`'s `TtlRanker` returns `u64::MAX - ttl_micros`, so an expired key ranks worst under `volatile-ttl` but is scored no differently under LRU/LFU. No test in `maxmemory_regression.rs`/`maxmemory_tcl.rs` mixes expired keys into the pressure workload.
- **Proposed test**: fill a shard with N keys whose TTLs have already elapsed plus M live keys, all under `allkeys-lru`; write until eviction fires; assert the live keys survive and that the removals are counted as `expired_keys` (with `expired` notifications), not `evicted_keys`. If today's behaviour is deemed acceptable, the test should still pin it explicitly and the divergence from Redis's `performEvictions` documented.
- **Boundary**: **3 — `shard_driver`.** Needs deterministic control over which keys are expired and direct observation of which counter moved; level 4 can see the counters but cannot make the race deterministic.

### F9: the `Store` trait ships ~40 silent no-op default bodies
- **Severity** 4 — `Store` has ~40 methods with **default** implementations that quietly degrade rather than fail: `exists_unexpired` → `contains` (ignores expiry), `purge_if_expired` → `false`, `audit_expiry_index` → empty (the audit reports "clean" for a store that never implemented it), `scan_filtered` ignores the type filter, `recompute_memory_used` → `memory_used`, `get_with_expiry_check` → `get`. Any decorator that forgets to forward a method inherits a *wrong answer*, not a compile error — and the codebase already has such decorators (`BatchSpyStore` in `active_expiry.rs:527` forwards 20+ methods by hand).
- **Likelihood** 2 — `HashMapStore` is the only production impl today, so this is currently latent; it becomes live the moment a second impl or a production decorator (metrics, tracing, quota) appears.
- **Effort** 2 — one conformance test suite parameterised over `Store` impls.
- **Priority** 14
- **Evidence**: `core/src/store/mod.rs` — 111/320 lines covered (34.7 %); the uncovered lines are almost entirely the default bodies (contiguous 3-line blocks from 555 to 819). Note this file's low percentage is *not itself* the finding: the default bodies are dead only because `HashMapStore` overrides them. The finding is that nothing enforces that an impl must.
- **Proposed test**: a `store_conformance!` macro asserting the expiry-sensitive contract (a logically-expired key is invisible to `exists_unexpired`/`get_with_expiry_check`/`scan_filtered`, and `audit_expiry_index` genuinely reports a hand-injected inconsistency), run against `HashMapStore` and against a trivial forwarding decorator built *without* overrides — the latter must fail unless the defaults are correct. Separately, consider deleting the defaults so a missing method is a compile error.
- **Boundary**: **2 — crate-level API.** The contract is the trait's; no shard or socket adds anything.

### F10: active-expiry field-phase budget exhaustion is untested
- **Severity** 3 — if the field phase's budget/`break` logic is wrong, hash-field expiry either stalls (fields never reclaimed — a slow leak and wrong `HGETALL` answers) or blows the 25 ms budget every tick, stalling the whole single-writer shard and every client on it.
- **Likelihood** 3 — needs a large volume of hash-field TTLs, which is exactly the workload hash-field expiry exists for.
- **Effort** 2 — the key-phase equivalent is already tested; this reuses `BatchSpyStore`.
- **Priority** 13
- **Evidence**: `core/src/shard/active_expiry.rs:169-170, 184, 189-190` — `line_counts` 0 for the field-phase early return on `budget_exhausted` and the "whole batch already purged" `break`. The key-phase twins are covered by `avalanche_exhausts_budget_and_next_cycle_resumes`.
- **Proposed test**: seed one hash with more expired fields than `DEFAULT_BATCH_SIZE`, with a per-field delay that guarantees budget exhaustion mid-batch; assert `ExpiryResult.budget_exhausted`, assert `fields_expired` is a prefix (not the whole set), and assert the next cycle resumes and eventually drains everything, with the global version bumped exactly once per cycle that expired ≥1 field.
- **Boundary**: **1/2 — unit test on `ActiveExpiryCoordinator` with the existing spy store.** `run_cycle` is directly callable and the assertion is on the returned `ExpiryResult`; adding a worker would only hide the budget arithmetic.

### F11: the real `run()` select loop is never the thing under test
- **Severity** 3 — every engine test drives `drive`/`drive_expiry_tick`/`drive_waiter_timeout_tick`/`drive_continuation_release` directly, so an ordering or shutdown bug *in `run()` itself* is invisible to the suite. The shutdown tail specifically — final search-index commit then final WAL flush — governs whether a clean `SHUTDOWN` loses the last writes.
- **Likelihood** 3 — every shutdown, every restart, every rolling upgrade takes the tail; the VLL arm takes it on every cross-shard continuation release.
- **Effort** 3 — needs a test that spawns `run()` and interacts through the real channels, plus a way to assert the flush happened.
- **Priority** 12
- **Evidence**: `core/src/shard/event_loop.rs` — the loop body is hot (line 34 = 494,974 executions) yet `line_counts` is **0** for line 37 (`handle_new_connection`), 99-100 (the VLL continuation-release arm), 103 (`else => break`), 92 (search-commit error), 114 and 123 (the shutdown-tail search-commit and WAL-flush error branches). Line 37's zero is explained by F16 below (the channel has no senders at all). Line 99-100's zero means the continuation-release arm is only ever reached through s4's `drive_continuation_release` seam, never through `run()`.
- **Proposed test**: spawn a real worker's `run()`; issue writes over the real `Envelope` channel; send the shutdown message; await the task; then assert the WAL contains every acknowledged write (via `FakeWalLog`) and that a dirty search index was committed. Add a case where the WAL flush fails on exit and assert it is logged and does not panic or hang.
- **Boundary**: **3 — `shard_driver`**, extended with a "run the real loop" mode alongside today's step-driven mode. The behaviour is the loop's own; level 4 would attribute a shutdown data-loss failure to the whole server.

### F12: eviction is only ever asserted at level 4, through `used_memory` deltas
- **Severity** 2 — not a correctness bug in itself, but it means the eviction tests cannot assert *which* key was chosen, only that memory went down. LRU/LFU/TTL ranking regressions (a reversed comparator, a pool that never evicts the worst candidate) would pass ~38 existing tests.
- **Likelihood** 4 — ranking quality degrades silently on any refactor of `EvictionPool`/`EvictionRanker`; the pool's own unit tests check the pool contract in isolation but never that the worker feeds it correctly.
- **Effort** 3 — requires teaching `shard_driver/harness.rs` to build workers with an `EvictionConfig` (and optionally a warm store); today it passes none.
- **Priority** 11
- **Evidence**: `core/tests/shard_driver/harness.rs:60-95` — `ShardWorkerBuilder` is called with `store`/`message_rx`/`new_conn_rx`/`shard_senders`/`registry` only; no `with_eviction`, no `with_persistence`, no `with_replication`. `redis-regression/tests/maxmemory_regression.rs:138-350` and `maxmemory_tcl.rs:79-131` assert `used_memory <= limit` and key-class membership, never key identity.
- **Proposed test**: add an eviction option to the harness, then a level-3 test per ranker asserting *identity*: touch keys in a known order, force exactly one eviction, assert the specific expected key is the one that disappeared, for `allkeys-lru`, `allkeys-lfu` and `volatile-ttl`. This is a **move-down**: it should replace (not duplicate) the weakest of the `*_honors_memory_limit` level-4 tests, which are slow and can only make a fuzzy assertion.
- **Boundary**: **3 — `shard_driver`** (harness extension required). Recommend keeping *one* level-4 test per policy as a wiring/parity smoke test and moving the semantics down.

### F13: active-expiry budget tests are wall-clock dependent
- **Severity** 2 — a flaky test in the expiry suite gets muted or retried, and the avalanche-bounding contract silently stops being enforced.
- **Likelihood** 4 — `avalanche_exhausts_budget_and_next_cycle_resumes` sleeps 2 ms per delete against a 15 ms budget; on a loaded CI box (or under the coverage instrumentation that produced this very report) that margin is 7 deletes wide.
- **Effort** 3 — needs an injectable clock in `ActiveExpiryCoordinator` (currently `Instant::now()` inline), which is a small but real production-code change.
- **Priority** 11
- **Evidence**: `core/src/shard/active_expiry.rs` — the `BatchSpyStore::delete` `thread::sleep(self.delete_delay)` at line 536 and the 15 ms budget in the avalanche test.
- **Proposed test**: replace the sleep with a mock clock the test advances explicitly; assert budget exhaustion at an exact deterministic key count. Keep one wall-clock smoke test.
- **Boundary**: **1 — pure unit test**, once the clock is injectable. This is a *move-down* of an existing test's dependency, not a new level.

### F14: `EvictionPolicy::TieredLfu` is never executed anywhere in the suite
- **Severity** 3 — a shipped, documented `maxmemory-policy` value. If its arm were mis-wired (e.g. to the wrong ranker or to `evict_*` instead of `spill_*`), it would delete data where the operator asked for demotion.
- **Likelihood** 2 — `tiered-lfu` is the less-used of the two tiered policies, but it is selectable in config and documented in `configuration.mdx:119`.
- **Effort** 2 — the `tiered-lru` server test at `server/tests/integration_persistence.rs:2352` is directly parameterisable over the policy string.
- **Priority** 11
- **Evidence**: `core/src/shard/eviction.rs:130` — `line_counts` 0 for `EvictionPolicy::TieredLfu => self.spill_with_ranker(false, &LfuRanker).await`, while line 129 (`TieredLru`) is covered. A repo-wide search for `tiered-lfu` outside `policy.rs`/docs returns no test.
- **Proposed test**: parameterise `tiered_config()` over the policy and run the existing spill-survives-restart assertions for `tiered-lfu`; additionally assert at level 3 that the *LFU* ranker chose the key (a frequently-touched key must survive a rarely-touched one).
- **Boundary**: **3 for the ranking assertion** (needs key identity, see F12), **4 to reuse the existing restart test** for the wiring. Cheapest split: reuse level 4 for wiring, add the identity assertion at level 3 once F12's harness change lands.

### F15: shard-level negative paths (unknown command, bad arity) are unreachable in tests
- **Severity** 2 — these are defence-in-depth guards behind the connection layer's own validation. If they regressed, a malformed internal message (replication apply, script call, cluster forward) would panic or mis-dispatch rather than return a clean error.
- **Likelihood** 2 — requires a path that reaches the shard without connection-layer validation: replica apply of a command the replica does not know (version skew during a rolling upgrade) is the realistic one.
- **Effort** 1 — two `drive` calls.
- **Priority** 9
- **Evidence**: `core/src/shard/execution.rs:132-150` — `line_counts` 0 for the `unknown command` and `wrong number of arguments` early returns; the connection layer rejects both before routing, so the shard-level guards are never entered.
- **Proposed test**: drive a `ParsedCommand` with an unregistered name and one with the wrong arity through the shard; assert the exact error strings and that no store mutation or WAL append occurred.
- **Boundary**: **3 — `shard_driver`**, one line each; this is the only level that can bypass connection-layer validation.

### F16: dead seams that inflate the "uncovered" surface and should be deleted, not tested
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

## Deprioritised

- **`core/src/command_spec.rs:563-638`** (the `Display` impl for `SpecError`) and **`core/src/registry.rs:391-416`** (test-support stub types) are 0-covered but cosmetic; error-message text is asserted indirectly by the `validate()` unit tests.
- **`core/src/conn_command.rs:938-1052`** is 96 uncovered lines of `unimplemented!()` bodies in the test module's `StubDebug`, plus the `NoopStatusProvider`/`NoopScriptingProvider` fallbacks. Not a gap — deliberate slot-fillers.
- **`core/src/store/typed.rs` (15 scattered zero lines)** and **`core/src/shard/message.rs` (8)** are `Debug`/`From`/label impls. Cosmetic.
- **`core/src/store/warm_tier.rs`** — 7 unit tests, only the `Debug` impl (lines 47-56) uncovered. Genuinely well covered; no finding.
- **`eviction/pool.rs` / `eviction/ranker.rs` "hot-but-shallow" classification** — investigated and dismissed: an `#[inline]` symbol artifact (see Scope). Both files have adequate ranker-parametric unit tests (`check_pool_contract`).
- **Encoding/listpack→hashtable conversion thresholds** — explicitly named in my dispatch, but `rg` for `listpack|max_listpack|max_intset|encoding` under `core/src/store/` returns **nothing**. This logic lives in the `types` crate. Cross-area note below.
- **`shard/blocking.rs` (1161/1272) and `shard/wait_queue.rs` (458/549)** — both have substantial uncovered blocks (`wait_queue.rs:176-222, 439-450, 478-491, 563-570`), but a concurrent agent owns them. Handing off the line ranges rather than filing duplicate findings.
- **A shuttle/loom model of the shard event loop** — tempting given F11, but the shard is a single task with no shared mutable state across threads; the interleaving surface is message-granularity only, which `scenario_s8` already characterises. Effort 5 for near-zero marginal signal. Explicitly not recommended.
- **Mutation testing of the eviction rankers** — would catch F12's comparator-reversal class directly, but round-1 issue 66 already owns mutation-testing rollout; folding the eviction module into that effort is cheaper than a bespoke test.
- **`shard/persistence.rs:556-562`, `shard/post_execution.rs` scattered zeros, `command.rs:1370-1403/1461-1470/1739-1745`** — reviewed; predominantly `Debug`/label/`From` impls and tracing-argument lines. No finding worth the effort.

## Cross-area notes

- **Encoding-conversion thresholds are in the `types` crate**, not `core/src/store/`. The
  listpack→hashtable / intset→hashtable boundary tests belong to whichever agent owns `types`;
  they are level-1 pure unit tests and are exactly the "geohash anti-pattern" candidates the brief
  warns about if they are currently tested over RESP.
- **F1 and F2 need the `shard_driver` harness to grow two constructor options** —
  `with_eviction(EvictionConfig)` and an optional warm/persistent store. `harness.rs:60-95`
  currently passes neither, though `ShardWorkerBuilder` already supports both
  (`builder.rs:207, 225, 286`). This is a small piece of **shared infrastructure** that F2, F5,
  F6, F8, F12 and F14 all depend on; it should be built once, first.
- **`scenario_s6.rs:32-59` is the reusable fake-WAL failure fixture** (`WalMode::Fake` +
  `FakeFailure::AtWriteIndex` + `set_wal_failure_policy_flag`). It is currently private to s6 and
  should be promoted into `harness.rs` so F1 and the persistence agent can both use it.
  `FakeFailure::Predicate(fn(write_index, key) -> bool)` already exists and is unused — it is the
  right primitive for per-key WAL failure injection.
- **F7 (cross-shard COPY) spans this crate and the server's routing layer**
  (`server/src/connection/routing.rs:229-298`, also 0-covered). Coordinate with the server
  net/connection agent so the test is written once.
- **F16's `NewConnection` deletion touches `server/src/acceptor.rs`, `server/src/server/init.rs`
  and `server/src/server/shards.rs`** — needs the server agent's sign-off.
- **`registry.rs:184`'s `debug_assert!(spec.validate().is_ok())` is release-stripped.** Whoever
  owns build/CI should confirm the full suite runs in debug so the whole-registry spec validation
  actually executes; if any suite runs in release, that invariant is unchecked.

## OPTIONS

### OPTIONS on F2 (failed spill → delete)

The test cannot be written until the intended behaviour is decided; this is a product question
surfaced by the coverage gap.

1. **Pin today's behaviour** — a failed spill degrades to a real eviction (delete + replicate +
   `evicted` notification). *Trade-off*: cheapest, honest about the current contract, but it
   codifies silent data loss on a transient disk error, which is a poor default for a database.
2. **Fail the write with OOM and keep the key** — treat a spill failure like "could not free
   memory". *Trade-off*: no data loss, and consistent with `check_memory_for_write`'s existing OOM
   contract; costs availability under warm-tier failure (writes start erroring) and needs a
   production-code change before the test can be written.
3. **Retry-then-degrade** — retry the spill against the next candidate, and only delete after the
   pool is exhausted. *Trade-off*: best behaviour, most code, and hardest to test deterministically.

**Recommendation: option 2**, with the test asserting OOM + key survival. A database should not
delete data because a disk write failed. Option 1 is acceptable only as a temporary pin if the
change is deferred — in which case the test should carry an explicit comment naming the decision.

### OPTIONS on F4 (declared WAL keys ⊇ actually mutated keys)

1. **Property test over the real registry at level 3** (`shard_driver` + a recording store
   decorator). *Trade-off*: catches the real invariant including argument-dependent key sets;
   needs a per-command argument generator, and commands with `KeySpec::Dynamic` will need
   hand-written generators or an allowlist.
2. **Extend `CommandSpec::validate()` with a static rule** and rely on the existing
   `debug_assert` in `registry.rs:180`. *Trade-off*: free to run and fails at registration time,
   but it is a purely declarative check — it can only compare `KeySpec` against `WalStrategy`,
   never against what the handler actually does, so it would miss the exact bug class.
3. **Fold into the mutation-testing effort (round-1 issue 66)**. *Trade-off*: no new
   infrastructure, but mutation testing proves tests *detect* changes, not that an invariant holds
   across the registry — wrong tool for this.

**Recommendation: option 1**, scoped initially to commands with static `KeySpec`s (`First`,
`FirstTwo`, `All`, `None`), with `Dynamic` commands on an explicit, shrinking allowlist so the
gap is visible rather than silent. Add option 2 as a cheap complement, not a substitute.

### OPTIONS on F12/F14 (where eviction semantics should live)

1. **Extend `shard_driver` and move ranking assertions to level 3.** *Trade-off*: deterministic
   key-identity assertions, fast; requires the harness change, and the tests then depend on
   per-shard memory accounting internals.
2. **Keep everything at level 4 and add `DEBUG`-assisted determinism** (e.g. `OBJECT FREQ`,
   `DEBUG SLEEP`, backdated idle times). *Trade-off*: no harness work and survives refactors, but
   `used_memory`-threshold tests are inherently fuzzy and slow, and cross-shard aggregation makes
   "which key" assertions unreliable.
3. **Both**: level-3 for ranking semantics, one level-4 smoke test per policy for config wiring
   and RESP-visible stats.

**Recommendation: option 3.** The harness change is shared with five other findings, so its cost
is amortised, and keeping one thin level-4 test per policy preserves the parity guarantee that
`maxmemory_tcl.rs` exists to provide.
