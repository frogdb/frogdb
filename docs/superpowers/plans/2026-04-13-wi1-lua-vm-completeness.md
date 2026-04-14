# WI-1: Lua VM Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make EVAL/EVALSHA first-class Redis-compatible by fixing 58 ignored scripting tests across `scripting_tcl.rs` (52) and `cluster_scripting_tcl.rs` (6).

**Architecture:** The Lua VM (`mlua` v0.11, Lua 5.4) is already functional — EVAL/EVALSHA, redis.call/pcall, script caching, and global protection all work. The 58 failures fall into discrete feature gaps: missing libraries (cjson, cmsgpack), behavioral diffs (blocking commands, error format, type conversion), and missing EVAL shebang flag parsing. Each task adds one capability, un-ignores the tests it fixes, and verifies.

**Tech Stack:** Rust, mlua 0.11, Lua 5.4 (vendored). Tests use `TestServer`/`TestClient` harness.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `frogdb-server/crates/core/src/scripting/lua_vm.rs` | Modify | Register cjson/cmsgpack libs, fix global protection, type conversions |
| `frogdb-server/crates/core/src/scripting/executor.rs` | Modify | Fix error handling (pcall tables, status/error reply APIs), EVAL shebang parsing |
| `frogdb-server/crates/core/src/scripting/bindings.rs` | Modify | Fix blocking command behavior, type conversion edge cases |
| `frogdb-server/crates/core/src/scripting/cache.rs` | Modify | Add redis.sha1hex helper |
| `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs` | Modify | Remove `#[ignore]` as tests are fixed |
| `frogdb-server/crates/redis-regression/tests/cluster_scripting_tcl.rs` | Modify | Remove `#[ignore]` as tests are fixed |
| `Cargo.toml` (workspace) | Modify | Add `serde_json` dep if not present (for cjson) |

---

### Task 1: Add cjson library (~8 tests)

The most user-impactful gap. Any Lua script doing JSON work fails without this.

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/lua_vm.rs` (add `setup_cjson_library()`)
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs` (un-ignore cjson tests)

**Tests to fix:**
- `tcl_eval_json_smoke_test`
- `tcl_eval_json_numeric_decoding`
- `tcl_eval_json_string_decoding`
- `tcl_eval_json_empty_array_default_behavior`
- `tcl_eval_json_empty_array_with_array_mt`
- `tcl_eval_cjson_array_metatable_is_readonly`

**Implementation approach:**

- [ ] **Step 1: Un-ignore the 6 cjson tests, run to see exact failures**

Remove `#[ignore]` from the 6 tests listed above. Run each to capture the error message:

```bash
just test frogdb-redis-regression tcl_eval_json_smoke_test
```

- [ ] **Step 2: Implement cjson.encode and cjson.decode**

Add a `setup_cjson_library()` method to `LuaVm` in `lua_vm.rs`, called from `new()` after the existing library setup. Use `serde_json` for the actual JSON work.

Redis cjson behavior:
- `cjson.encode(value)` — Lua value → JSON string. Tables with consecutive integer keys 1..n become JSON arrays; tables with string keys become JSON objects. `cjson.null` represents JSON null.
- `cjson.decode(json_string)` — JSON string → Lua value. JSON null becomes `cjson.null`. JSON arrays become 1-indexed Lua tables. JSON numbers decode to Lua numbers.
- `cjson.decode_invalid_numbers(true)` — Allow nan/inf (Redis default).
- Empty arrays: By default, `cjson.decode("[]")` returns an empty table `{}`. When `cjson.decode_array_with_array_mt(json)` is used, the result has an array metatable that can be detected.

Register as global `cjson` table with `encode`, `decode`, `decode_invalid_numbers`, `null`, and optionally `decode_array_with_array_mt`.

- [ ] **Step 3: Run the 6 cjson tests**

```bash
just test frogdb-redis-regression tcl_eval_json
```

Expected: All 6 PASS.

- [ ] **Step 4: Run full regression to check for regressions**

```bash
just test frogdb-redis-regression
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(scripting): add cjson library to Lua VM

Register cjson.encode/cjson.decode/cjson.null in the Lua environment,
matching Redis's built-in cjson behavior. Uses serde_json for
JSON serialization/deserialization."
```

---

### Task 2: Add cmsgpack library (~2 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/lua_vm.rs` (add `setup_cmsgpack_library()`)
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_cmsgpack_can_pack_double`
- `tcl_eval_cmsgpack_can_pack_negative_int64`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see failures**
- [ ] **Step 2: Implement cmsgpack.pack and cmsgpack.unpack**

Use the `rmp-serde` crate for MessagePack. Redis's cmsgpack:
- `cmsgpack.pack(value)` → MessagePack binary string
- `cmsgpack.unpack(binary)` → Lua value

Register as global `cmsgpack` table.

- [ ] **Step 3: Run the 2 tests, verify pass**
- [ ] **Step 4: Run full regression, commit**

---

### Task 3: Fix blocking commands in scripts (~7 tests)

Currently FrogDB errors on blocking commands in scripts. Redis returns nil instead.

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/bindings.rs:9-28`
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_scripts_do_not_block_on_blpop`
- `tcl_eval_scripts_do_not_block_on_brpop`
- `tcl_eval_scripts_do_not_block_on_blmove`
- `tcl_eval_scripts_do_not_block_on_brpoplpush`
- `tcl_eval_scripts_do_not_block_on_bzpopmin`
- `tcl_eval_scripts_do_not_block_on_bzpopmax`
- `tcl_eval_scripts_do_not_block_on_wait`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failure**
- [ ] **Step 2: Change blocking command behavior**

In `bindings.rs`, the `is_forbidden_in_script()` function (lines 9-28) currently returns an error string for blocking commands. The tests expect these commands to execute **non-blocking** — i.e., BLPOP with timeout 0 should return nil immediately (no data available), not error.

Two approaches:
1. **Execute with timeout=0:** Allow the commands through but force timeout=0, so they return nil immediately if no data.
2. **Return nil directly:** Intercept and return `Response::Null` without executing.

Check what Redis does (approach 1 is more compatible) and implement accordingly. The key change is in `is_forbidden_in_script()` — either remove these from the forbidden list and handle in the execution path, or intercept and return nil.

- [ ] **Step 3: Run the 7 tests, verify pass**
- [ ] **Step 4: Run full regression, commit**

---

### Task 4: Fix pcall/error handling and redis.error_reply/status_reply (~6 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/executor.rs:279-338`
- Modify: `frogdb-server/crates/core/src/scripting/bindings.rs`
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_lua_pcall_with_error`
- `tcl_eval_lua_pcall_with_non_string_arg`
- `tcl_eval_pcall_returns_error_table_for_select_error`
- `tcl_eval_explicit_error_call_table_err`
- `tcl_eval_redis_error_reply_api`
- `tcl_eval_redis_status_reply_api`
- `tcl_eval_ro_pcall_write_command_returns_error_table`
- `tcl_eval_wrongtype_error_from_script`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failures**
- [ ] **Step 2: Fix pcall error table format**

Redis pcall returns `{err='ERR message'}` on error. Check that our format matches exactly — the error prefix handling, WRONGTYPE errors, and nested error tables.

- [ ] **Step 3: Implement redis.error_reply and redis.status_reply**

These are Redis helper functions:
- `redis.error_reply("message")` → returns `{err="message"}` table
- `redis.status_reply("message")` → returns `{ok="message"}` table

Register these in `setup_redis_bindings()` in `executor.rs`.

- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Run full regression, commit**

---

### Task 5: Fix global protection and Lua sandbox edge cases (~8 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/lua_vm.rs:296-317`
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_globals_protection_reading_undeclared_variable`
- `tcl_eval_return_g_is_empty`
- `tcl_eval_return_table_with_metatable_raise_error`
- `tcl_eval_try_trick_global_protection_modify_metatable_index`
- `tcl_eval_try_trick_global_protection_replace_redis`
- `tcl_eval_try_trick_global_protection_setmetatable_g`
- `tcl_eval_try_trick_readonly_bit_table`
- `tcl_eval_try_trick_readonly_cjson_table`
- `tcl_eval_try_trick_readonly_redis_table`
- `tcl_eval_print_not_available`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failures**
- [ ] **Step 2: Fix global protection behavior**

Key Redis behaviors:
- Reading an undeclared global should work (return nil in EVAL) — NOT error. The `__index` metamethod in EVAL mode returns nil for unknown globals. (Note: FUNCTION mode IS strict.)
- `_G` should appear empty when iterated but still allow access to registered globals.
- `getmetatable(_G)` should return the locked string, preventing metatable manipulation.
- `redis`, `cjson`, `bit` tables should be read-only (writes raise error).
- `print` should not be available (currently handled by forbidden list, verify).

Compare FrogDB's global protection Lua snippet (lua_vm.rs:296-317) with Redis's and fix differences.

- [ ] **Step 3: Run tests, verify pass**
- [ ] **Step 4: Run full regression, commit**

---

### Task 6: Fix type conversions and misc Lua semantics (~6 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/executor.rs:346-387`
- Modify: `frogdb-server/crates/core/src/scripting/bindings.rs`
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_lua_integer_to_redis_protocol`
- `tcl_eval_lua_number_to_redis_integer`
- `tcl_eval_keys_and_argv_populated`
- `tcl_eval_table_unpack_with_invalid_indexes`
- `tcl_eval_script_unpack_massive_arguments`
- `tcl_eval_correct_handling_of_reused_argv`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failures**
- [ ] **Step 2: Fix type conversions**

Key Redis behaviors:
- Lua number 3.0 → Redis integer `:3` (already handled, verify edge cases)
- Lua number 3.5 → Redis bulk string `$3\r\n3.5\r\n` (verify)
- KEYS and ARGV should be populated correctly for all argument patterns
- `table.unpack` with invalid indexes should return nil, not error
- Large argument counts (unpack of 1000+ args) should work

- [ ] **Step 3: Run tests, verify pass**
- [ ] **Step 4: Run full regression, commit**

---

### Task 7: Add helper functions (sha1hex, set_repl, replicate_commands) (~3 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/executor.rs` (setup_redis_bindings)
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_redis_sha1hex_implementation`
- `tcl_eval_sha1hex_wrong_number_of_args`
- `tcl_eval_redis_set_repl_invalid_values`
- `tcl_eval_redis_replicate_commands_returns_1`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failures**
- [ ] **Step 2: Implement redis.sha1hex**

`redis.sha1hex(string)` → returns SHA1 hex digest of the input string. Use the existing `sha1` crate (already a dependency in cache.rs).

Register in `setup_redis_bindings()`.

- [ ] **Step 3: Implement redis.set_repl and redis.replicate_commands stubs**

These are replication control functions. In Redis:
- `redis.replicate_commands()` — deprecated, always returns 1
- `redis.set_repl(mode)` — controls replication; modes: REPL_NONE=0, REPL_SLAVE=1, REPL_AOF=2, REPL_ALL=3

For FrogDB, stub these:
- `redis.replicate_commands()` → always return 1
- `redis.set_repl(mode)` → validate mode is 0-3, otherwise error. No-op in FrogDB.

- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Run full regression, commit**

---

### Task 8: EVAL shebang flag parsing + no-writes enforcement (~4 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/executor.rs`
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_no_writes_shebang_flag`
- `tcl_eval_shebang_support_for_lua_engine`
- `tcl_eval_verify_minimal_bitop_functionality`
- `tcl_eval_lua_bit_tohex_bug`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failures**
- [ ] **Step 2: Parse shebang flags in EVAL scripts**

Currently shebang parsing (`parser.rs`) is only used for FUNCTION LOAD. EVAL scripts with `#!lua flags=no-writes` should also be parsed. In `execute_script()` (executor.rs:104), before executing, check if the script starts with `#!lua` and parse flags.

Flags to support:
- `no-writes` — reject write commands (redis.call("SET",...) → error)
- `no-cluster` — (already partially handled in loader.rs)
- `allow-oom` — (no-op for FrogDB)
- `allow-stale` — (no-op for FrogDB)

- [ ] **Step 3: Fix bit library edge cases**

The bit library (`setup_bit_library()` in lua_vm.rs:603-654) may have edge cases with `bit.tohex()`. Check the failing tests and fix.

- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Run full regression, commit**

---

### Task 9: Remaining scripting edge cases (~8 tests)

**Files:**
- Modify: Various scripting files
- Modify: `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_redis_call_raises_error_on_wrong_arg_count`
- `tcl_eval_scripts_handle_commands_with_incorrect_arity`
- `tcl_eval_call_redis_with_many_args`
- `tcl_eval_cluster_reset_not_allowed_from_script`
- `tcl_script_flush_clears_cache`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run to see exact failures**
- [ ] **Step 2: Fix arity validation in redis.call**

When redis.call receives wrong number of args, the error message format must match Redis. Check `VmContextAccessor::execute_command()` (lua_vm.rs:94-98) for arity validation format.

- [ ] **Step 3: Fix SCRIPT FLUSH test**

SCRIPT FLUSH is already implemented (cache.rs). The test may fail due to response format or sync behavior. Run test, check error, fix.

- [ ] **Step 4: Add CLUSTER RESET to forbidden-in-script list**

If not already forbidden, add CLUSTER RESET to the forbidden commands list in bindings.rs.

- [ ] **Step 5: Run tests, verify pass**
- [ ] **Step 6: Run full regression, commit**

---

### Task 10: Cluster scripting — shebang flags + key validation (6 tests)

**Files:**
- Modify: `frogdb-server/crates/core/src/scripting/executor.rs`
- Modify: `frogdb-server/crates/redis-regression/tests/cluster_scripting_tcl.rs`

**Tests to fix:**
- `tcl_eval_scripts_with_shebangs_and_functions_default_to_no_cross_slots`
- `tcl_cross_slot_commands_are_allowed_by_default_for_eval_scripts_and_with_allow_cross_slot_keys_flag`
- `tcl_cross_slot_commands_are_allowed_by_default_if_they_disagree_with_pre_declared_keys`
- `tcl_cross_slot_commands_are_also_blocked_if_they_disagree_with_pre_declared_keys`
- `tcl_function_no_cluster_flag`
- `tcl_script_no_cluster_flag`

**Implementation approach:**

- [ ] **Step 1: Un-ignore tests, run in cluster mode to see failures**

These tests use `ClusterTestHarness`. Run with:
```bash
just test frogdb-redis-regression tcl_eval_scripts_with_shebangs
```

- [ ] **Step 2: Fix EVAL shebang flag interaction with key validation**

FrogDB's strict key validation rejects undeclared keys before considering cross-slot. Redis is more lenient — EVAL scripts without shebang default to allowing cross-slot. With `#!lua flags=allow-cross-slot-keys`, declared keys can differ from accessed keys.

The fix is in the key validation path in executor.rs — `validate_key_access()` should respect shebang flags.

- [ ] **Step 3: Enforce no-cluster flag at FCALL time**

The `no-cluster` flag is parsed but not enforced (per ignore reason). In cluster mode, FCALL/EVAL with `no-cluster` flag should return an error.

- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Run full regression, commit**

---

### Task 11: Final verification and cleanup

**Files:**
- Modify: `docs/superpowers/specs/2026-04-13-ignored-test-triage-design.md`

- [ ] **Step 1: Run ALL scripting tests (including previously passing)**

```bash
just test frogdb-redis-regression scripting
just test frogdb-redis-regression cluster_scripting
```

Verify 0 failures across all scripting tests.

- [ ] **Step 2: Run full regression suite**

```bash
just test frogdb-redis-regression
```

Verify no regressions.

- [ ] **Step 3: Run clippy and fmt**

```bash
just lint frogdb-core
just fmt frogdb-core
```

- [ ] **Step 4: Update spec**

Mark WI-1 as complete in `docs/superpowers/specs/2026-04-13-ignored-test-triage-design.md`.

- [ ] **Step 5: Commit**
