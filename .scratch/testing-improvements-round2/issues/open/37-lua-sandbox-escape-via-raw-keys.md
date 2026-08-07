# Lua sandbox escape — `__frogdb_backing` / `__frogdb_protected` are plain raw keys on `_G`

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/09 F3 · MASTER.md §3
Score: severity 5 · likelihood 2 · effort 1 · priority 18
Area: frogdb-scripting / Lua sandbox

## Context

The sandbox stores its backing global table and its protected set as raw keys on `_G` and relies
on a metatable to hide them — but Lua consults `__index`/`__newindex` **only when the raw key is
absent**, and both keys are present. User Lua can therefore read and write `_real_G` and
`_protected` directly. The execution VM is long-lived *per shard*, so a mutation made by one
script is visible to every subsequent script from every other connection on that shard, and
`setmetatable(_G, {})` sticks permanently once `_G` is un-protected.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `scripting/src/sandbox.rs:416-419`:
  ```lua
  -- Store backing table and protected set as hidden raw keys in _G.
  -- User scripts cannot see these because __index only checks _real_G.
  _rawset(_G, "__frogdb_backing", _real_G)
  _rawset(_G, "__frogdb_protected", _protected)
  ```
  The comment is wrong: both keys are present, so `_G.__frogdb_backing` and
  `_G.__frogdb_protected` are readable and assignable without ever entering the metatable at
  `sandbox.rs:390-409`.
- Three concrete escapes follow: (1) `_G.__frogdb_backing.tostring = f` injects a global seen by
  every later script; (2) `_G.__frogdb_backing.redis = nil` breaks `redis.call` for every later
  script on the shard; (3) `_G.__frogdb_protected[_G] = nil` un-protects `_G` itself, after which
  the wrapped `setmetatable` (`sandbox.rs:363-368`) permits `setmetatable(_G, {})` — and since
  `sandbox.rs:411-414` already cleared every raw key from `_G`, the sandbox degrades to an empty
  global table permanently.
- `register_protected_global` is only applied to `bit`, `cjson`, `cmsgpack` and `redis`;
  `string`/`table`/`math`/`os`/`coroutine` live unprotected in `_real_G` and are equally reachable
  through the backing table.
- **Why the existing tests pass anyway**: the 11 sandbox tests in `sandbox.rs` probe `_G.foo = 1`,
  metatable tricks and `getmetatable(_G)` — none probes a raw key. `scripting_tcl.rs`'s
  `tcl_eval_return_g_is_empty` passes only because `lua_to_response` stops at index 1 of a table
  with no array part.

## What to fix

1. Stop storing the backing table and protected set as raw keys on `_G` — hold them in an upvalue
   / registry slot the sandbox closure captures, unreachable from any Lua table the script can
   index.
2. Extend `register_protected_global` coverage to `string`, `table`, `math`, `os`, `coroutine`, or
   drop them from the backing table entirely.
3. Add the negative sandbox battery and the cross-invocation isolation test below.

## Acceptance criteria

- [ ] Negative sandbox test asserts each of `type(_G.__frogdb_backing)`,
      `type(_G.__frogdb_protected)`, `rawget` and `rawset` against those names evaluates to
      `nil`/errors. **Fails today.**
- [ ] Cross-invocation isolation test on one long-lived VM: script A attempts to plant
      `_G.__frogdb_backing.x = 1` (or any global); script B asserts `x` is undefined and
      `redis.call` still works.
- [ ] `_G.__frogdb_protected[_G] = nil` followed by `setmetatable(_G, {})` is rejected.
- [ ] The whole battery runs against both `SandboxMode::Load` and `SandboxMode::Execute`, matching
      the existing parity convention.

## Test boundary

**2** (crate API) — `build_frogdb_lua_vm` + `lua.load(...).eval()` is the exact surface. Not level
4: a server round trip adds a socket and RESP encoding without exercising anything the VM does not
already expose.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

Reproduces; only the line numbers moved. The two raw keys are still planted on `_G` at
`frogdb-server/crates/scripting/src/sandbox.rs:459-462` (was 416-419), under the same incorrect
comment; the `_G` metatable is at `sandbox.rs:433-452` (was 390-409), the wrapped `setmetatable` at
`sandbox.rs:406-411` (was 363-368), and the raw-key clear at `sandbox.rs:454-457` (was 411-414).
`_real_G` (`sandbox.rs:396`) and `_protected` (`sandbox.rs:402`) are plain unprotected tables, and
`register_protected_global` (`sandbox.rs:475-504`) still reaches them via `globals.raw_get`, so
`_G.__frogdb_backing.redis = nil` and `_G.__frogdb_backing.<name> = f` still bypass the metatable
entirely and persist for the life of the per-shard VM. Contrast `__frogdb_forbidden`, which *is*
cleaned up correctly (`_rawset(_G, "__frogdb_forbidden", nil)`, `sandbox.rs:379`) — the same
treatment is what the two remaining keys need. No test anywhere in the tree mentions
`__frogdb_backing`/`__frogdb_protected`; sandbox history since filing is only `2fb1051c` (clock
seam), `e68168f2` (lua-time-limit), `403309a8`, `17cb35fc` — none touches this. One nuance to
correct in the body: acceptance-criterion 3 as written probably does *not* reproduce — after
`_G.__frogdb_protected[_G] = nil`, `setmetatable(_G, {})` still raises "cannot change a protected
metatable" because the `_G` metatable carries `__metatable = "The metatable is locked"`
(`sandbox.rs:451`). Escapes (1) and (2) — global injection and `redis.call` removal across scripts
— are the live ones and are sufficient on their own. The bare-EVAL slot-validation fix from the
hardening campaign is unrelated to this finding.
