# The `_protected` Lua table gains one strong entry per EVAL/FCALL, forever, on the default path

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/09 F2 · MASTER.md §3 (availability / resource)
Score: severity 4 · likelihood 5 · effort 2 · priority 20
Area: frogdb-core / scripting Lua VM

## Context

Every EVAL/EVALSHA/FCALL rebuilds the `redis` table and registers it as a protected global. The
protection table is strong-keyed, so each superseded `redis` table stays reachable and can never
be collected. This is unbounded per-shard Lua heap growth on the *normal* scripting path with no
user action and no special config; with the 256 MB `lua_heap_limit_mb` cap in force, a
long-lived shard eventually fails *all* scripts with an mlua memory error rather than failing
the one offending script.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`core/src/scripting/lua_vm.rs:452` — `execute_in_scope` calls
`set_redis_functions` on *every* execution; `set_redis_functions` builds a fresh table
(`lua_vm.rs:498 create_table()`) and ends at `lua_vm.rs:655` with
`sandbox::register_protected_global(self.lua(), "redis", redis_table)`.
`register_protected_global` (`scripting/src/sandbox.rs:451-453`) does
`protected.set(table, true)` into `_protected`, which is a plain strong-keyed table
(`sandbox.rs:359`, `local _protected = {}` — no `__mode`). `backing.set("redis", …)`
replaces the previous binding, so each superseded `redis` table stays alive *only* through
`_protected` and can never be collected. `cleanup_execution` (`lua_vm.rs:200-210`) clears
only `KEYS`/`ARGV`.

## What to fix

1. Either hoist `set_redis_functions` out of the per-execution path so the `redis` table is
   built once per VM, or make `_protected` weak-keyed (`__mode`) so superseded tables are
   collectable.
2. Confirm `cleanup_execution` (`lua_vm.rs:200-210`) releases everything the execution
   introduced, not just `KEYS`/`ARGV`.
3. Assert on VM memory, not on the internal table, so either fix satisfies the test.

## Acceptance criteria

- [ ] A crate-level test runs 20 000 `EVAL "return 1" 0` against one `LuaVm` and asserts
      `lua.used_memory()` after the run is within a small constant of the value after the first
      100 (e.g. `< 2×`). Fails today.
- [ ] The `_protected` table's entry count is asserted stable across the same run.
- [ ] The assertion is written against VM memory so it holds under either fix.

## Test boundary

Level 2 (crate API) — the leak is entirely inside the VM; a server integration test would need
10⁵ round trips to see it. Not level 3: the shard adds nothing except cost, since the growth is
per-execution inside `LuaVm`.

## Depends on

nothing
