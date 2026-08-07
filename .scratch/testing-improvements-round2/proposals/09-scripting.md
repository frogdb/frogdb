# Lua scripting and functions — testing gap audit (round 2)

## Scope

Paths audited:

| path | src LOC | notes |
|---|---:|---|
| `frogdb-server/crates/scripting/src/` | 3309 | `sandbox.rs` 1093, `loader.rs` 654, `persistence.rs` 440, `registry.rs` 413, `parser.rs` 260, `error.rs` 162, `function.rs` 130, `library.rs` 121, `lib.rs` 36. **No `tests/` dir.** |
| `frogdb-server/crates/core/src/scripting/` | 4079 | `executor.rs` 1216, `gate.rs` 1078, `lua_vm.rs` 822, `bindings.rs` 420, `cache.rs` 291, `error.rs` 137, `config.rs` 93, `mod.rs` 22 |
| adjacent seams read (not owned) | — | `core/src/shard/scripting.rs` 246, `core/src/shard/functions.rs` 118, `core/src/shard/dispatch_scripting.rs`, `core/src/shard/post_execution.rs::run_script_write_effects`, `server/src/connection/scripting/{eval,script,function}.rs`, `server/src/recovery/functions.rs` |

Coverage (re-measured 2026-08-07, `.scratch/testing-improvements/audit/coverage-depth-2026-08-07.md`;
the figures below are from the superseded 2026-07-28 run and are within ~1pp of current — `scripting`
is 1705/2038 lines / 83.7% in the fixed lcov):

- crate `scripting`: **1634/1970 lines (82.9%)**, 2739/3387 regions (80.9%) — lowest line
  coverage of any in-scope core crate; region coverage is 4pp below line coverage, i.e. the
  covered lines are being entered on one branch only.
- Three files in this area appear in the workspace's 60 worst files by *absolute uncovered
  lines*: `core/src/scripting/executor.rs` (287 uncovered), `core/src/scripting/gate.rs`
  (286), `scripting/src/loader.rs` (200).
- Depth-class caveat: `depth.json` only carries per-crate rollups plus truncated top-N
  class tables, and `lcov.info`'s `FNDA`/`DA` records in this export are all zero (even for
  functions that are demonstrably executed by inline tests), so **per-function depth classes
  are not recoverable for this area**. The one scripting entry that survives in the report's
  truncated tables is `frogdb_scripting::sandbox::msgpack_to_lua` (`single-test`, only
  `sandbox::tests::test_cmsgpack_in_both_modes`). Findings below cite source evidence rather
  than depth classes.

## Summary

The scripting subsystem is architecturally sound in its big decisions — a real
`mlua::Scope`-bound `&mut dyn Store` (no transmuted pointer), a single `classify` decision
point, and effect-based replication that frames multi-write scripts in MULTI/EXEC — and the
tests pin those decisions well. The risk lives entirely in the *edges* that no test touches:
hand-maintained tables that must agree with the command registry but do not, a global-protection
scheme whose hidden state is not actually hidden, a per-shard Lua VM that is long-lived and
accumulates state across invocations, and a resource-limit story (timeout, kill, memory) that
is implemented but never exercised end-to-end. The bugs that escape today are the quiet ones:
an `EVAL_RO` script writing JSON on a replica and diverging it silently; a 5-second write script
aborted mid-way with its partial effects committed *and replicated*; a `FUNCTION`-based
deployment losing every library on failover because libraries are never replicated and never
enter the RDB; and per-invocation Lua state that grows without bound on the normal path. Two of
the biggest gaps are actively *masked* by mislabelled parity exclusions (`scripting_tcl.rs`
excludes the whole SCRIPT KILL / timed-out-script family as "Redis-internal feature" although
FrogDB implements SCRIPT KILL).

## Existing test inventory

| surface | what it covers | notable strengths | notable blind spots |
|---|---|---|---|
| inline `#[cfg(test)]`, `crates/scripting/` (66 tests: sandbox 11, loader 17, parser 11, registry 10, persistence 9, function 4, library 3, error 1) | sandbox stdlib parity between Load/Execute VMs, forbidden globals, readonly `_G`/metatable tricks, `unpack` guard, shebang/flag parsing, `FDBF` dump/restore round-trip + corruption guards, registry CRUD + glob | the Load-vs-Execute parity idea is good and catches whole classes of drift; persistence has real truncation/checksum negatives | nothing probes `_G`'s *raw* keys; no memory-limit test; no test that a VM is clean between invocations; two persistence tests use fixed paths under `std::env::temp_dir()` (parallel-run collision risk) |
| inline `#[cfg(test)]`, `core/src/scripting/` (74 tests: executor 25, gate 22, cache 9, lua_vm 9, bindings 5, error 3, config 1) | shebang parsing, cache disposition, the `Scope` re-entrancy seam, the full round-1 slot-cohesion matrix, cross-shard dispatch on multi-thread vs current-thread runtime, kill/`Unkillable` state machine | `gate.rs`'s `classify` is unit-testable without a live invoker — the right seam, well used; `cross_shard_call_hard_errors_on_current_thread` is exactly the "scripting multi-thread runtime" area from the memory note | `bindings.rs` has 5 tests for ~420 lines of conversion + key-extraction tables; no `lua_to_response` RESP3 table; `request_kill` is tested as a state machine but never as a delivered kill |
| `server/tests/integration_scripting.rs` (18 tests) | `redis.call`/`pcall`, error tables, server-wide command denial, forbidden commands, write tracking, `redis.log`, EVALSHA after SCRIPT LOAD, per-shard cache | denial paths are genuinely negative-tested | no timeout, no kill, no RESP3, no `_RO` variants, no replication assertions, no resource limits |
| `server/tests/functions.rs` (20 tests) | FUNCTION LOAD/LIST/DELETE/FLUSH/STATS/DUMP/RESTORE/HELP, FCALL/FCALL_RO, persistence across restart (3 tests) | restart persistence is properly covered (`test_function_persistence_across_restart/_after_delete/_after_flush`) | `test_function_kill_not_busy` covers only the NOTBUSY branch; RESTORE tested only on the happy path; no replica/failover test |
| `redis-regression` (204 tests: `scripting_tcl` 112, `functions_tcl` 78, `cluster_scripting_tcl` 6, `functions_regression` 5, `cluster_scripting_regression` 3) | broad Redis 8.6 parity for EVAL basics, type conversion, cjson, error formats, FUNCTION lifecycle | large and honest; exclusion headers are documented per test | the exclusion headers themselves are stale/wrong in two places (see F16), and they remove precisely the timeout/KILL and propagation families that cover this area's severity-5 paths |
| `server/src/recovery/tests.rs:154` | a corrupt `functions.fdb` must not block startup | good negative test, already exists | nothing asserts what the surviving in-memory registry contains |

Round-1 overlap: issue 26 (`script-undeclared-key-policy`) is done and pinned the slot-cohesion
matrix for shebang EVAL. Its own "Caveat / follow-up" declares the residue this audit picks up
in **F13**, and F13 additionally covers FCALL, which round 1 did not mention.

## Findings

### F1: `is_write_command` is a hand-maintained list that disagrees with the command registry, so `_RO`/`no-writes` scripts can write

- **Severity** 5 — `EVAL_RO`/`EVALSHA_RO` are declared "replica-eligible, write-rejecting"
  (`server/src/connection/scripting_conn_command.rs:118`). A read-only script that calls
  `JSON.SET`/`PFADD`/`SETBIT`/`TS.ADD` is *not* rejected, and `ScriptInvoker::run_local`
  applies it with no second gate. On a replica that is a silent primary/replica divergence.
- **Likelihood** 3 — needs a `_RO` script (or a `no-writes` function) that touches a data type
  outside the hand-list. JSON/timeseries/bitmap workloads on read-only replicas are an ordinary
  deployment shape.
- **Effort** 1 — the catch-all is a pure unit test.
- **Priority** 20
- **Evidence**: `core/src/scripting/bindings.rs:44-77` enumerates ~70 write commands by name;
  missing at minimum `SETBIT`, `BITFIELD`, `BITOP`, `PFADD`, `SETRANGE`-adjacent `SORT … STORE`,
  `XSETID`, `GEORADIUS[BYMEMBER] … STORE`, `HEXPIRE`/`HPEXPIRE`/`HPERSIST`, and every
  `JSON.*`, `TS.*`, `BF.*`, `CF.*`, `CMS.*`, `TOPK.*`, `TDIGEST.*` write. It is consumed at
  `core/src/scripting/gate.rs:232-236` as the *only* read-only rejection. The same file's
  replication path uses the authoritative registry flag instead
  (`gate.rs:469-476`, `handler.flags().contains(CommandFlags::WRITE)`), so the two predicates
  are already known to be different sources of truth. Consequence chain: `is_write_command`
  false ⇒ no rejection ⇒ no `mark_write()` ⇒ `has_writes` stays false ⇒ the belt-and-braces
  `wrote_in_readonly` guard (`executor.rs:338`, `:541`) also cannot fire.
- **Proposed test**: a table-driven unit test over the full `CommandRegistry` asserting
  `is_write_command(name) == registry.get(name).flags().contains(CommandFlags::WRITE)` for every
  registered command, with an explicit allow-list for the deliberate extras
  (`PUBLISH`/`SPUBLISH`/`PFCOUNT`/`PFMERGE`, which the list marks write for replication-safety
  reasons). It must fail today. Plus one behavioural test: `EVAL_RO "redis.call('JSON.SET', …)"`
  returns `ERR Write commands are not allowed from read-only scripts`.
- **Boundary**: 2 (crate API) — the invariant is between two pure functions plus the registry;
  no shard, no socket. The divergence consequence deserves a second, separate test at level 5.
- **OPTIONS**:
  - *(a)* Unit test cross-checking the two predicates (effort 1). Fast, catches every future
    command addition, but asserts on an internal predicate rather than observable behaviour.
  - *(b)* `shard_driver` test: run `EVAL_RO` with a `JSON.SET` sub-command, assert rejection and
    assert the key is absent (effort 2). Observable, but only covers the commands you enumerate.
  - *(c)* Two-node replica test: `EVAL_RO` on the replica, assert the replica's dataset is
    unchanged and still matches the primary (effort 4). Proves the actual production
    consequence.
  - **Recommendation**: (a) as the regression net *and* (c) as the one behavioural proof;
    (b) adds little on top of the two.

### F2: the `_protected` Lua table grows by one entry per EVAL/FCALL, forever, on the default path

- **Severity** 4 — unbounded per-shard Lua heap growth on the normal scripting path; with the
  256 MB `lua_heap_limit_mb` cap in force, a long-lived shard eventually fails *all* scripts with
  an mlua memory error rather than one offending script.
- **Likelihood** 5 — every single EVAL/EVALSHA/FCALL, default config, no user action needed.
- **Effort** 2 — crate-level: build a `LuaVm`, run N trivial scripts, assert
  `lua.used_memory()` (or the `_protected` table length) does not grow linearly in N.
- **Priority** 20
- **Evidence**: `core/src/scripting/lua_vm.rs:452` — `execute_in_scope` calls
  `set_redis_functions` on *every* execution; `set_redis_functions` builds a fresh table
  (`lua_vm.rs:498 create_table()`) and ends at `lua_vm.rs:655` with
  `sandbox::register_protected_global(self.lua(), "redis", redis_table)`.
  `register_protected_global` (`scripting/src/sandbox.rs:451-453`) does
  `protected.set(table, true)` into `_protected`, which is a plain strong-keyed table
  (`sandbox.rs:359`, `local _protected = {}` — no `__mode`). `backing.set("redis", …)`
  replaces the previous binding, so each superseded `redis` table stays alive *only* through
  `_protected` and can never be collected. `cleanup_execution` (`lua_vm.rs:200-210`) clears
  only `KEYS`/`ARGV`.
- **Proposed test**: run 20 000 `EVAL "return 1" 0` against one `LuaVm` and assert
  `lua.used_memory()` after the run is within a small constant of the value after the first 100
  (e.g. < 2× ), and that the `_protected` table's entry count is stable. Assert on memory, not
  on the internal table, so the fix is free to be "hoist `set_redis_functions` out of the hot
  path" or "make `_protected` weak-keyed".
- **Boundary**: 2 (crate API) — the leak is entirely inside the VM; a server integration test
  would need 10⁵ round trips to see it.

### F3: the sandbox's "hidden" backing table and protected set are plain raw keys on `_G`, readable and writable from user Lua

- **Severity** 5 — this is the cross-tenant/sandbox-escape class. The execution VM is
  long-lived *per shard*, so a mutation made by one script is visible to every subsequent script
  from every other connection on that shard.
- **Likelihood** 2 — requires a script author who goes looking; no accidental path.
- **Effort** 1 — pure Lua-through-the-VM assertions.
- **Priority** 18
- **Evidence**: `scripting/src/sandbox.rs:416-419`:
  ```lua
  -- Store backing table and protected set as hidden raw keys in _G.
  -- User scripts cannot see these because __index only checks _real_G.
  _rawset(_G, "__frogdb_backing", _real_G)
  _rawset(_G, "__frogdb_protected", _protected)
  ```
  The comment is wrong: Lua consults `__index`/`__newindex` **only when the raw key is absent**.
  Both keys are present, so `_G.__frogdb_backing` and `_G.__frogdb_protected` are readable and
  assignable without ever entering the metatable at `sandbox.rs:390-409`. Three concrete escapes
  follow: (1) `_G.__frogdb_backing.tostring = f` injects a global seen by every later script;
  (2) `_G.__frogdb_backing.redis = nil` breaks `redis.call` for every later script on the shard;
  (3) `_G.__frogdb_protected[_G] = nil` un-protects `_G` itself, after which the wrapped
  `setmetatable` (`sandbox.rs:363-368`) permits `setmetatable(_G, {})` — and since
  `sandbox.rs:411-414` already cleared every raw key from `_G`, the sandbox degrades to an empty
  global table permanently. `register_protected_global` is only applied to `bit`, `cjson`,
  `cmsgpack` and `redis`; `string`/`table`/`math`/`os`/`coroutine` live unprotected in
  `_real_G` and are equally reachable through the backing table. The existing sandbox tests
  (`sandbox.rs`, 11 tests) probe `_G.foo = 1`, metatable tricks and `getmetatable(_G)` — none
  probes a raw key. `scripting_tcl.rs`'s `tcl_eval_return_g_is_empty` passes only because
  `lua_to_response` stops at index 1 of a table with no array part.
- **Proposed test**: a negative sandbox test asserting each of
  `type(_G.__frogdb_backing)`, `type(_G.__frogdb_protected)`, `rawget`, `rawset` evaluates to
  `nil`/errors; plus a *cross-invocation isolation* test on one long-lived VM: script A attempts
  to plant `_G.__frogdb_backing.x = 1` (or any global), script B asserts `x` is undefined and
  `redis.call` still works. Run the whole battery against both `SandboxMode::Load` and
  `SandboxMode::Execute`, matching the existing parity convention.
- **Boundary**: 2 (crate API) — `build_frogdb_lua_vm` + `lua.load(...).eval()` is the exact
  surface; a server round trip adds nothing.

### F4: a write script that exceeds `lua-time-limit` is aborted mid-way and its partial effects are committed *and replicated*

- **Severity** 5 — a partially-applied, partially-replicated script. Redis never does this: its
  busy threshold only starts replying `BUSY` to *other* clients, and a script that has written
  is explicitly `UNKILLABLE`. FrogDB's own kill path honours that rule (`lua_vm.rs:472-478`
  returns `Unkillable` when `has_writes`), but the timeout path does not.
- **Likelihood** 3 — default `lua_time_limit_ms` is 5000 (`core/src/scripting/config.rs:40`);
  a script looping over a large collection on a loaded shard reaches it without anything
  unusual happening.
- **Effort** 3 — needs a controllable clock or a lowered limit plus a real replica to assert
  propagation; the single-node half is a `shard_driver` test.
- **Priority** 18
- **Evidence**: the instruction hook (`scripting/src/sandbox.rs:209-228`) raises
  `mlua::Error::RuntimeError("BUSY script running for {} ms")` on elapsed >
  `timeout_ms + grace_ms` with **no `has_writes` consultation** — it has no access to the
  execution state at all, only the kill flag. The abort propagates out of `execute`, but
  `core/src/shard/scripting.rs:114-130` has already drained `ctx.effects.script_writes` and
  calls `run_script_write_effects` unconditionally — the comment at `:126-129` states this is
  deliberate ("Runs even when the script itself errored mid-way"), which is right for a
  *script-raised* error and wrong for a *server-imposed* abort. No test anywhere sets
  `lua_time_limit_ms` low and asserts what survives.
- **Proposed test**: with `lua-time-limit` set to ~50 ms, run
  `for i=1,1e9 do redis.call('SET', KEYS[1]..i, i) end`; assert (a) the client receives a `BUSY`
  error, (b) the number of keys actually present is the number reported by the replica after
  `WAIT`, i.e. primary and replica agree exactly on the partial set, and (c) the propagated
  batch is MULTI/EXEC-framed. Then the policy question the test forces: assert either
  "no writes survive" or "writes survive and replicate identically" — but *pin* one.
- **Boundary**: 4 (server integration) — needs `CONFIG SET lua-time-limit` and a replica; the
  primary-only half (partial effects present + framed) is worth doing first at level 3
  (`shard_driver`) because it is cheap and deterministic.
- **OPTIONS**:
  - *(a)* `shard_driver` with a low limit and the `fake-wal` seam: assert the WAL contains a
    MULTI/EXEC-framed prefix of the writes (effort 3). Deterministic, no replica.
  - *(b)* Two-node server integration: assert primary and replica converge after the abort
    (effort 4). Proves the real consequence, slower and clock-sensitive.
  - **Recommendation**: (a) first as the regression net; (b) once, as the consistency proof.

### F5: FUNCTION libraries are never replicated and never enter the RDB — a replica has no functions, and a failover loses them

- **Severity** 4 — not user-data loss, but every `FCALL` fails with "function not found" on a
  promoted replica until an operator re-loads by hand; applications built on FUNCTION break
  wholesale at failover. Also lost outright when persistence is disabled.
- **Likelihood** 4 — failover is an ordinary ops event, and building a fresh replica is the
  normal way to add capacity.
- **Effort** 3 — existing `start_primary`/`start_replica` harness.
- **Evidence**: `FUNCTION` is a `ConnectionLevel` command handled entirely in
  `server/src/connection/scripting/function.rs`; `handle_function_load`/`_delete`/`_flush`/
  `_restore` mutate `self.admin.function_registry` and then call `persist_functions()`, which
  writes `<data_dir>/functions.fdb` **only when `config_manager.persistence_enabled()`**. There
  is no propagation call on any of those paths — `rg` for `FunctionLoad|function_load` under
  `core/src/replication`, `core/src/persistence` and `crates/persistence` returns nothing, and
  `rg -l 'function_registry|FunctionLibrary' crates/persistence/` is empty, so the snapshot
  format carries no libraries either. Recovery reads the file back at
  `server/src/recovery/functions.rs:20-21`, which is a *local-restart* path only. The three
  restart tests in `server/tests/functions.rs:652-792` cover exactly that path and nothing else;
  `functions_tcl.rs` excludes replication as "needs:repl".
- **Priority** 17
- **Proposed test**: `FUNCTION LOAD` on the primary; wait for sync; assert `FUNCTION LIST` on the
  replica contains the library and `FCALL_RO` on the replica returns the expected value; then
  promote the replica and assert `FCALL` still works. Second test: with persistence disabled,
  `FUNCTION LOAD` then restart — assert the documented behaviour (whatever it is) rather than
  leaving it undefined.
- **Boundary**: 5 (multi-node) — replication and promotion are the behaviour; there is no lower
  level at which "the replica has the library" is expressible.

### F6: cross-shard scripted writes propagate immediately and unframed, so replicas *do* observe intermediate script state

- **Severity** 4 — the module contract is explicit that they must not
  (`core/src/shard/post_execution.rs:630-634`: "multiple writes propagate as an atomic
  MULTI/EXEC-framed batch … so replicas never observe intermediate script state"). For a
  cross-shard script that guarantee is false, and the *ordering* is inverted: remote writes are
  committed and propagated mid-script, local writes only after the script returns.
- **Likelihood** 4 — default multi-shard standalone; nothing prevents a script from touching
  keys on two shards outside cluster mode (slot cohesion is only enforced for shebang scripts in
  cluster mode — see F13).
- **Effort** 4 — needs a multi-shard server with a replica and a way to observe intermediate
  replica state, or WAL/replication-stream capture at the `shard_driver` level.
- **Priority** 16
- **Evidence**: `core/src/scripting/gate.rs:484-487` routes a non-local key to
  `run_remote_inner`; the target shard runs `execute_script_sub_command`
  (`core/src/shard/scripting.rs:181-232`) which calls
  `self.run_script_write_effects(vec![record], conn_id).await` at `:225` — one record, so
  `EffectScope::Command` (`post_execution.rs:645-649`), propagated *during* the script. The
  local writes accumulate in `ctx.effects.script_writes` and only run at
  `shard/scripting.rs:130`. A script that errors after a successful remote write leaves that
  write propagated with no framing and no companion. No test exercises a cross-shard script
  with a replica attached.
- **Proposed test**: multi-shard primary + replica; script writes `{a}` (shard X) then `{b}`
  (shard Y) then errors. Assert both shards' replicas converge with the primary, and pin the
  framing that is actually emitted (per-shard `EffectScope::Command` is a legitimate design
  answer for a sharded engine — but then the doc comment must say so and the test must say so).
- **Boundary**: 3 → 5. See OPTIONS.
- **OPTIONS**:
  - *(a)* `shard_driver` + `fake-wal`: drive a cross-shard script and assert the WAL/propagation
    record sequence per shard (effort 3). Deterministic, no networking; asserts the mechanism
    not the observable.
  - *(b)* Server integration, multi-shard, one replica, read the replica mid-script via a second
    connection (effort 4). Observable but racy — needs a blocking point inside the script.
  - *(c)* Turmoil/deterministic-sim ordering test (effort 5). Overkill for a documented-contract
    mismatch.
  - **Recommendation**: (a), and use the result to correct the `post_execution.rs` doc comment;
    escalate to (b) only if the answer is "this really should be atomic".

### F7: `FUNCTION RESTORE` is non-atomic — a mid-list failure destroys the previous libraries and leaves memory and disk disagreeing

- **Severity** 4 — `FUNCTION RESTORE … FLUSH` wipes the registry first and then loads
  library-by-library; a failure on library *k* leaves libraries 1..k-1 loaded, k..n missing, and
  everything that was there before permanently gone. Redis builds the replacement context and
  swaps atomically.
- **Likelihood** 3 — restoring a dump taken from a different build, or one containing a library
  whose name now conflicts, is exactly the disaster-recovery scenario RESTORE exists for.
- **Effort** 2 — the failure is reachable through the registry API with a hand-built payload.
- **Priority** 16
- **Evidence**: `server/src/connection/scripting/function.rs::handle_function_restore` —
  `registry.flush()` runs before the loop, and both the `load_library` parse failure and the
  `registry.load_library` conflict failure `return Response::error(...)` from inside the loop.
  `self.persist_functions()` is only reached on success, so after a failed restore the in-memory
  registry (partial) and `functions.fdb` (pre-restore) disagree until the next successful
  mutation — a restart silently "undoes" a partial restore. `server/tests/functions.rs:356`
  (`test_function_dump_restore`) covers only the happy path.
- **Proposed test**: load libraries A and B; `FUNCTION DUMP`; construct a payload whose second
  entry is invalid (or conflicts); `FUNCTION RESTORE <payload> FLUSH` → assert the command
  errors **and** `FUNCTION LIST` still returns exactly A and B. Same for `REPLACE` and the
  default `APPEND`.
- **Boundary**: 4 (server integration) — RESTORE is a connection-level command with no
  lower-level entry point; the harness already has `TestServer` + FUNCTION helpers.

### F8: SCRIPT KILL / FUNCTION KILL cannot reach a shard that is running a script, and the timeout error is swallowable by user `pcall`

- **Severity** 4 — a runaway script wedges one shard with no operator remedy short of a process
  restart, and the operator-facing symptom is a scatter-gather timeout rather than anything
  diagnostic.
- **Likelihood** 3 — an accidental infinite loop is the classic scripting incident; the
  `pcall`-swallowing variant needs a script that wraps work in `pcall`, which is idiomatic.
- **Effort** 3 — server integration with a background script and a second connection.
- **Priority** 15
- **Evidence**: two independent mechanisms, both untested.
  (1) *Delivery*: `SCRIPT KILL` scatters `ScriptingMsg::ScriptKill`
  (`server/src/connection/scripting/script.rs:106-120`, and `function.rs:496-507` for
  `FUNCTION KILL`) onto each shard's message channel; the shard worker's event loop is the same
  single task that is *inside* `run_script` (`core/src/shard/scripting.rs:88-152`) for the
  duration of the script, so `handle_script_kill` (`shard/scripting.rs:235-245`) cannot run and
  `kill_flag` is never set. The kill is structurally undeliverable exactly when it is needed.
  (2) *Escapability*: the hook raises an ordinary `mlua::Error::RuntimeError`
  (`scripting/src/sandbox.rs:213`, `:225`), so
  `while true do pcall(function() while true do end end) end` catches both the timeout and the
  kill error and loops forever. The `lua_vm.rs` tests cover `request_kill`/`Unkillable` as a
  state machine only (`lua_vm.rs:727-733`); `server/tests/functions.rs:625` covers only the
  NOTBUSY branch; and every upstream test for this is excluded (see F16).
- **Proposed test**: start `EVAL "while true do end" 0` on connection A; on connection B assert
  (a) other commands on the same shard get a `BUSY` reply rather than hanging, (b) `SCRIPT KILL`
  returns OK within a bounded time and connection A receives the killed error, (c) the same with
  the script body wrapped in `pcall` still terminates, and (d) a script that has already written
  returns `UNKILLABLE`. Every assertion needs a hard timeout so the test fails rather than hangs.
- **Boundary**: 4 (server integration) — the whole point is a *second connection* interrupting a
  busy shard; nothing below the connection layer can express it.

### F9: Lua error/status strings are passed to the RESP encoder unsanitised, so `{err=…}` can inject frames into the client stream

- **Severity** 4 — a CRLF in an error or status reply produces additional wire frames on that
  connection: the client's request/response alignment is broken and a forged reply can be
  attributed to the *next* command. Redis normalises newlines in error replies for this reason.
- **Likelihood** 2 — needs a script that returns attacker- or app-controlled text in an error
  table; multi-line error text from application code is a plausible accident.
- **Effort** 2 — crate-level or a single server round trip.
- **Priority** 14
- **Evidence**: `core/src/scripting/executor.rs::lua_to_response` returns
  `Response::Error(Bytes::from(e))` / `Response::Simple(Bytes::from(o))` straight from the Lua
  table's `err`/`ok` fields with no filtering; `redis.error_reply`/`status_reply`
  (`core/src/scripting/lua_vm.rs:566-604`) only special-case the empty string. The encoder
  (`protocol/src/response.rs:227-230`) wraps the bytes in
  `Resp2BytesFrame::SimpleString`/`Error` unchanged. Note the deliberate contrast: *Lua runtime*
  errors are first-line-stripped precisely because of this hazard
  (`lua_vm.rs:262-264`: "error messages are stripped to the first line because RESP simple errors
  must not contain newlines"), but the `{err=…}` *return-value* path bypasses that.
- **Proposed test**: `EVAL "return {err='ERR a\r\n+INJECTED'}" 0` followed by `PING` on the same
  connection — assert the second reply is `PONG` and that the first reply's error text contains
  no CR/LF. Same for `{ok=…}` and `redis.error_reply`. A second cheap case: a non-UTF-8 `err`
  value currently falls through the `t.get::<String>("err")` conversion and is silently
  reinterpreted as a generic table (→ nil) — pin whatever behaviour is chosen.
- **Boundary**: 4 (server integration) — the bug is *on the wire*; asserting on a `Response`
  value would miss it entirely. This is the rare case where the socket is the point.

### F10: Lua→RESP3 conversion is inferred rather than declared, so the same script returns different types to RESP2 and RESP3 clients

- **Severity** 3 — a wrong answer on a data path the user notices: a client that upgrades to
  RESP3 (as most modern clients now do by default) silently changes the type of every script
  result that is a float or a non-array table.
- **Likelihood** 4 — `HELLO 3` is the default in current client libraries.
- **Effort** 3 — needs a RESP3 connection, which the harness supports.
- **Priority** 14
- **Evidence**: `core/src/scripting/executor.rs::lua_to_response` — `Value::Number(n)` becomes
  `Response::Double(n)` under RESP3 but `Response::Integer(n as i64)` under RESP2, so
  `return 100.5` is `:100` to one client and a double to another (Redis truncates to `100` for
  both, and `scripting_tcl.rs::tcl_eval_lua_integer_to_redis_protocol` pins only the RESP2
  half). Under RESP3 a table with no `[1]` is walked with `t.pairs()` and emitted as a
  `Response::Map` in Lua's arbitrary hash order — Redis instead requires the script to *declare*
  a map with `{map={…}}`, and likewise `{double=}`, `{big_number=}`, `{set=}`. None of those
  field names is handled. `redis.setresp` is absent from the `redis` table entirely
  (the registered names are `call`, `pcall`, `log`, `LOG_*`, `REDIS_VERSION[_NUM]`, `sha1hex`,
  `error_reply`, `status_reply`, `replicate_commands`, `set_repl`, `REPL_*`), so a script
  containing the idiomatic `redis.setresp(3)` fails on the readonly-table `__index`. The
  `scripting_tcl.rs` header excludes all `resp3`-tagged tests.
- **Proposed test**: a conversion table driven over both protocol versions asserting each of:
  `return 3.7`, `return {1,2,3}`, `return {a=1}`, `return {ok='X'}`, `return {err='X'}`,
  `return {map={…}}`, `return {double=3.5}`, `return redis.call('CONFIG','GET',…)` — with the
  RESP2 and RESP3 expectations written side by side so a divergence is visible in the source.
  Include `redis.setresp(3)` returning either a working switch or a clean error, not a
  nonexistent-global error.
- **Boundary**: 4 (server integration) — protocol version is a connection property; a unit test
  on `lua_to_response` can cover the mapping but not the `HELLO 3` plumbing. Do both: the table
  at level 2 for breadth, three smoke cases at level 4 for the plumbing.

### F11: `redis.call` silently drops `nil` arguments, turning a buggy call into a different valid command

- **Severity** 3 — `redis.call('SET', KEYS[1], v, 'EX', ttl)` with `v = nil` executes
  `SET key EX ttl`, i.e. it sets the key to the literal string `"EX"` with no TTL, and returns
  OK. Redis raises "Lua redis lib command arguments must be strings or integers". A wrong value
  is written and replicated with no error anywhere.
- **Likelihood** 3 — `nil` from a missing `ARGV[n]` or a failed lookup is the single most common
  Lua scripting mistake.
- **Effort** 1 — pure unit test on `lua_args_to_command`, plus one behavioural case.
- **Priority** 14
- **Evidence**: `core/src/scripting/bindings.rs::lua_args_to_command` — the `Value::Nil` arm is
  `continue` with the comment "Skip nil values (common in Lua when passing optional args)".
  Every other non-string/number/boolean type errors. `bindings.rs` has 5 inline tests total and
  none covers this arm.
- **Proposed test**: assert `lua_args_to_command` on `["SET", "k", nil, "EX", "60"]` errors
  rather than yielding `["SET","k","EX","60"]`; behavioural companion:
  `EVAL "redis.call('SET', KEYS[1], ARGV[1])" 1 k` with no `ARGV[1]` returns an error and leaves
  `k` unset. If the skip is deliberate, pin it explicitly with a comment referencing the Redis
  divergence — but it must not be silent.
- **Boundary**: 1 (pure unit) — argument marshalling is a pure function; the behavioural
  companion belongs at level 3 (`shard_driver`) so it asserts the resulting keyspace.

### F12: the FUNCTION LOAD capture VM runs with **no** memory limit

- **Severity** 4 — a library whose top-level allocates without bound takes the *process* down
  (mlua has no cap to fail against), rather than failing the one `FUNCTION LOAD`. The 5-second
  load hook bounds time but not memory.
- **Likelihood** 2 — needs a hostile or pathological library; but `FUNCTION LOAD` is an
  ordinary, often unprivileged, client command.
- **Effort** 2 — crate-level: call `load_library` with a payload that allocates.
- **Priority** 14
- **Evidence**: `scripting/src/loader.rs:57` sets `memory_limit_bytes: 0` in the
  `SandboxOptions` for the Load VM, and `scripting/src/sandbox.rs:145-148` only calls
  `set_memory_limit` when the value is non-zero (`0` is documented as "unlimited" at
  `sandbox.rs:121`). The execution VM does get the cap (`core/src/scripting/lua_vm.rs:96`,
  `config.lua_heap_limit_mb * 1024 * 1024`, default 256 MB). None of `loader.rs`'s 17 tests
  supplies an allocating library, and `sandbox.rs`'s own test helper also passes
  `memory_limit_bytes: 0` (`sandbox.rs:941`), so the limit is never exercised in either VM.
- **Proposed test**: two tests — (a) `load_library` with a top-level
  `local t = {} while true do t[#t+1] = string.rep('x', 1e6) end` returns a bounded error rather
  than growing without limit (run under a low configured cap); (b) the execution VM's 256 MB cap
  actually fires: an `EVAL` that allocates past the limit returns an error and the *next* EVAL
  on the same shard still works (i.e. the VM is not left poisoned).
- **Boundary**: 2 (crate API) — the limit is a VM construction parameter. Test (b)'s "next EVAL
  still works" half is better at level 3 so it uses a real shard's long-lived VM.

### F13: FCALL never enforces slot cohesion, in any mode

- **Severity** 3 — a cluster `FCALL` may touch keys in slots this node does not own; the write
  lands on whatever shard the key hashes to locally with no `CROSSSLOT` rejection, so a later
  slot migration silently splits data the function treated as co-located. Redis enforces slot
  cohesion for FUNCTION exactly as for EVAL.
- **Likelihood** 3 — cluster mode plus FUNCTION plus a multi-key function; slot migration is the
  event that surfaces it.
- **Effort** 2 — the executor's inline cluster tests already do this for EVAL.
- **Priority** 13
- **Evidence**: `core/src/scripting/executor.rs::execute_function` passes
  `false, CrossSlotTracker::new(None)` into `execute_in_scope` (`executor.rs:526-531`) — the
  enforcement flag is hard-coded off and the tracker has no seed, so
  `gate.rs:239-241`'s `check_all` never runs for FCALL. This is *additional to*, not covered by,
  round-1 issue 26, whose declared residue is only "a plain (non-shebang) `EVAL` in cluster mode
  is not slot-enforced". The round-1 tests at `executor.rs` pin the EVAL matrix; there is no
  FCALL equivalent.
- **Proposed test**: mirror the round-1 EVAL matrix for FCALL: with `is_cluster_mode = true`, a
  function declaring `{tag}a` that calls `redis.call('get', '{other}b')` is rejected with "do not
  hash to the same slot"; the same with `allow-cross-slot-keys` in the shebang succeeds; a
  same-slot undeclared key succeeds. Also pin the non-shebang cluster EVAL residue in the same
  file so both halves of the divergence are recorded in one place.
- **Boundary**: 2 (crate API) — `executor.rs`'s existing cluster tests run real Lua through
  `eval`/`execute_function` with a `HashMapStore`; that is exactly the right level and already
  established.

### F14: library top-level code re-executes on every FCALL, with `redis.call` live — unlike at LOAD

- **Severity** 3 — a library whose top level has side effects performs them on every call;
  worse, `redis.call` is *bound* during that re-execution (it is a hard error during LOAD), so
  library top-level code can read and write the keyspace outside any registered function and
  outside the caller's declared keys. A library that loads cleanly can also fail at FCALL time.
- **Likelihood** 3 — any library with non-trivial top-level initialisation; the cost is paid on
  the hot path.
- **Effort** 2 — crate-level through `execute_function`.
- **Priority** 13
- **Evidence**: `core/src/scripting/executor.rs:526-538` — inside `execute_in_scope` (where
  `redis.call`/`pcall` are already bound to the live invoker), `execute_function` calls
  `setup_function_registration()`, then `vm.execute(lcc)` (the whole library body, shebang
  stripped), then `lock_runtime_environment()`, then the function invocation. During
  `FUNCTION LOAD` the same body runs on the capture VM where `redis.call` is a stub that errors
  (`scripting/src/loader.rs::setup_register_function_binding`). No test asserts what a
  side-effecting library top level does at FCALL time.
- **Proposed test**: a library whose top level does `redis.call('INCR', 'loadcount')`; assert
  `FUNCTION LOAD` errors (as it does today) and then, for a library whose top level increments a
  Lua-local counter, assert the counter's value after three FCALLs — pinning whether the intended
  contract is "once" or "per call". Second: assert that a top-level `redis.call` at FCALL time
  is rejected the same way it is at LOAD, so the two lifecycles agree.
- **Boundary**: 2 (crate API) — `execute_function` with a `HashMapStore` shows both the
  re-execution and the keyspace effect.

### F15: `FUNCTION STATS` never reports a running function — the field is dead

- **Severity** 2 — wrong observability on the one command an operator reaches for when a
  function is stuck; combined with F8 it means a wedged shard is invisible *and* unkillable.
- **Likelihood** 4 — any operator who runs `FUNCTION STATS` during an incident.
- **Effort** 2 — the registry API is directly testable; the end-to-end half needs a busy shard.
- **Priority** 12
- **Evidence**: `FunctionRegistry::set_running_function`
  (`scripting/src/registry.rs:174-176`) has **no callers anywhere in the workspace** — the only
  reference to the field outside `registry.rs` is the reader at
  `server/src/connection/scripting/function.rs:317`. `registry.rs`'s `test_stats` asserts
  `stats.running_function.is_none()` on a fresh registry, which is the only state that can ever
  occur. `server/tests/functions.rs:408` (`test_function_stats`) exercises the idle case only.
- **Proposed test**: while a long-running FCALL is in flight on connection A, assert
  `FUNCTION STATS` on connection B reports the running function's name, library and a non-zero
  duration. Pairs naturally with F8's test — same fixture, same busy script.
- **Boundary**: 4 (server integration) — the field is only meaningful concurrently with an
  in-flight call. Fold into F8's fixture rather than building a second one.

### F16: two parity-exclusion headers are wrong, and they exclude precisely the families that cover this area's worst gaps

- **Severity** 2 — no runtime consequence, but the headers are the record of what is
  *deliberately* not tested, and both are misleading in a way that has already prevented these
  gaps from being noticed.
- **Likelihood** 3 — the next person to audit this area reads the header and moves on.
- **Effort** 1 — reclassify and, where the behaviour exists, port the test.
- **Priority** 11
- **Evidence**: `redis-regression/tests/scripting_tcl.rs:36-44` excludes seven tests
  (`Timedout read-only scripts can be killed by SCRIPT KILL`, `… even when use pcall`,
  `Timedout scripts that modified data can't be killed by SCRIPT KILL`, …) under the heading
  "Script timeout / SCRIPT KILL (FrogDB has a different script-execution model)" and tags each
  `redis-specific — Redis-internal feature`. FrogDB *implements* SCRIPT KILL
  (`shard/scripting.rs:235`, `lua_vm.rs:472`, the `Unkillable` variant, the BUSY message in
  `sandbox.rs:222`) — these are not Redis-internal, they are FrogDB's own untested paths, and
  the `… can't be killed by SCRIPT KILL` case is the exact rule F4 shows the timeout path
  violates. Separately, `scripting_tcl.rs:66` excludes `cmsgpack` tests because "FrogDB may not
  ship cmsgpack", but it does (`sandbox.rs::setup_cmsgpack_library`, registered as a protected
  global; `msgpack_to_lua` is `single-test`). Round 1 already corrected a third wrong header in
  this same file (issue 26), so the file has form.
- **Proposed test**: not a test — a header correction plus porting the four SCRIPT KILL tests
  and the cmsgpack tests that FrogDB can actually satisfy, with the remainder retagged with the
  real reason.
- **Boundary**: 4 (the regression suite's own level) — these are parity tests; they belong where
  the rest of the ported suite lives.

## Deprioritised

- **`math.random` is unseeded and Lua 5.4-random per VM.** Redis replaces it with a
  deterministic PRNG because Redis replicates the *script*; FrogDB replicates *effects*
  (`ScriptWriteRecord` → `run_script_write_effects`), so non-determinism inside a script cannot
  diverge a replica. Same reasoning retires `TIME`/`RANDOMKEY`/`SRANDMEMBER`/`SPOP` determinism
  as a replication concern. Worth one cheap parity note, not a finding. (The *cross-invocation*
  concern — a script calling `math.randomseed` affecting later scripts on the shard — is
  already covered by F3's isolation test.)
- **Corrupt `functions.fdb` at startup.** Already tested (`server/src/recovery/tests.rs:154-171`,
  "corrupt functions.fdb is not fatal").
- **FUNCTION persistence across restart.** Already tested three ways
  (`server/tests/functions.rs:652/706/751`).
- **`save_to_file` does not fsync the containing directory** (`scripting/src/persistence.rs`,
  temp file + `sync_all` + rename). Real, but the durability-boundary work is round-1 issue 12's
  territory and the blast radius here is one file of function source; flag it to the persistence
  agent rather than duplicating the fsync harness.
- **Two `persistence.rs` tests use fixed filenames under `std::env::temp_dir()`** — a
  parallel-execution collision waiting to happen, but a hygiene fix (use `tempfile`), not a
  coverage gap.
- **`wrote_in_readonly` is unreachable** (`executor.rs:338`, `:541`): `classify` rejects and
  returns before `mark_write`, so `has_writes` can never be true when `read_only` is. It is a
  dead guard that *looks* like a backstop for F1 and is not one. Folded into F1's evidence
  rather than filed separately.
- **Deep recursion / stack exhaustion and huge return values.** `unpack` is capped at 8000
  (`sandbox.rs:341`) and cjson has its own nesting limits; a dedicated fuzz corpus for Lua
  inputs would be the right tool but is new infrastructure (effort 5) for a bounded-severity
  outcome. Better folded into the existing `testing/fuzz` corpus work (round-1 issue 40) than
  built here.
- **`extract_keys_from_command`'s hand-written key-position table** (`bindings.rs`, ~150 lines)
  duplicating the registry's `keys()`. Same class of defect as F1, but it only affects
  slot-cohesion accounting and shard routing (a wrong key position routes to the wrong shard,
  which `run_local`/`run_remote` still executes correctly against the owning store). Fixing it
  is the same cross-check test as F1 — recommend extending F1's test to cover key positions
  rather than filing a second finding.
- **`parse_flags_value` map-style silently ignores unknown keys** while array style errors
  (`scripting/src/loader.rs`). A typo'd `no-write` in map style yields a *writable* function
  that the author believes is read-only. Low likelihood (map style is the rarer form) and it is
  a one-line unit test — bundle it into whatever loader work lands, not a standalone finding.

## Cross-area notes

- **Command registry as single source of truth** — F1's cross-check test (and the
  `extract_keys_from_command` extension in Deprioritised) is really a *registry invariant* test:
  "no module may maintain a parallel copy of a command property". The `commands` and `core`
  agents may find the same pattern elsewhere; if so, this wants one shared invariant-test module
  over `register_all()` rather than three copies. Flagging to the coordinating agent as possible
  shared infrastructure.
- **Replication agent** — F5 (FUNCTION libraries absent from replication *and* from the RDB /
  fullsync payload) touches the fullsync path and is adjacent to round-1 issue 66
  ("minimal-RDB fullsync carries no dataset"), which is still open. These may share a root
  cause; worth cross-checking before either is scheduled.
- **Persistence agent** — the missing directory fsync in `scripting/src/persistence.rs::save_to_file`
  belongs to whatever fsync-boundary harness round-1 issue 12 produced.
- **Protocol agent** — F9's CRLF injection is only *reachable* through scripting but is
  *fixable* in `protocol/src/response.rs` (sanitise `WireResponse::Simple`/`Error` at encode
  time). If the protocol agent proposes encoder-level sanitisation, F9's test should assert at
  the protocol level and scripting keeps only a smoke test. Recommend the two agents' findings
  be resolved together.
- **Shared fixture needed: "a shard that is busy running a script."** F4, F8 and F15 all need
  it, and it does not exist in `frogdb-test-harness` today (nothing in the suite starts a
  long-running EVAL and then talks to the same shard on a second connection). One helper —
  spawn a bounded-but-slow script, wait until the shard is observably busy, hand back both
  connections, and guarantee teardown even if the script cannot be killed — unblocks three
  findings. Worth building once, deliberately, rather than three ad-hoc versions.
