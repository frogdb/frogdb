# Proposal: Single Lua Sandbox Builder

Status: implemented
Date: 2026-07-04

## Problem

The "FrogDB Lua sandbox" — the stdlib set, the forbidden-globals scrubbing, the `bit`/`cjson`/
`cmsgpack` compatibility libraries, the readonly-`_G` protection scheme, the instruction/timeout
hook — is one concept with one correctness contract: *code that passes FUNCTION LOAD behaves the
same way at FCALL*. But it was **built twice, divergently**, with no single construction seam:

| Concern | loader.rs (capture VM, load time) | lua_vm.rs / executor.rs (real VM, exec time) |
|---|---|---|
| StdLib set | `COROUTINE\|TABLE\|STRING\|MATH` (loader.rs:47) | same set, redefined (lua_vm.rs:140) |
| `bit` library | 6 functions, minimal | 11 functions, LuaJIT-compatible (`BIT_LIBRARY_LUA`) |
| `cjson` / `cmsgpack` | absent | full implementations |
| Forbidden globals | **none removed** — `load`, `loadstring`, `print`, `collectgarbage` all live | 14 globals scrubbed; forbidden names read as `nil` |
| Safe `os` table | absent (`os.time` errors as "nonexistent global") | `os.clock`/`os.time` |
| `unpack` compat | absent | Lua 5.1-compat `unpack` with range guard |
| Global protection | shallow `setmetatable(_G, {__index=error, __newindex=error})` (loader.rs:99-114) | elaborate `_real_G` backing table, protected-set, wrapped `setmetatable`/`getmetatable`, `rawset`/`rawget` removed (lua_vm.rs:244-368) |
| `getmetatable` | removed entirely | wrapped (frozen proxy for protected tables) |
| `redis` table | plain writable global | readonly protected global |
| Timeout hook | inline `set_hook`, 5s constant (loader.rs:79-87) | inline `set_hook`, config timeout + grace + kill flag (lua_vm.rs:438-465) |
| Memory limit | none | `lua_heap_limit_mb` |

FUNCTION LOAD (server handler → `frogdb_core::load_library`, function.rs:123) runs the library's
top-level code once in the capture VM only to extract names/flags, then discards that VM; at FCALL
the shard **re-executes the same source** in the real VM (executor.rs `execute_function`). The same
top-level code therefore ran under two different sandboxes:

- A library whose top-level uses `bit.tohex` / `bit.rol` / `bit.bswap` (or `cjson`, or `os.time`)
  **failed at FUNCTION LOAD** while the exact same code executes fine on the real VM — the load
  gate rejected libraries the engine can run.
- Conversely, the loader **leaked `load`/`loadstring`/`print`** — a genuine sandbox hole: library
  top-level code could load arbitrary bytecode strings during FUNCTION LOAD, which the execution
  sandbox forbids.
- Global-protection semantics differed in kind: the loader's shallow metatable is trivially
  bypassable with `rawset(_G, ...)` (not removed there) and reports different behavior for
  forbidden-vs-undeclared globals.

**The deletion test.** Every sandbox change was two edits or drift. The historical record shows the
drift is not hypothetical: the `bit` library was upgraded to 11 functions on the execution side
only; `cjson`/`cmsgpack` were added on the execution side only; the protection scheme was deepened
on the execution side only. The loader was a snapshot of the sandbox as of its birth.

## Current state (before)

Two independent constructions:

- `loader.rs::execute_in_sandbox` — ~90 lines of inline VM assembly: stdlib set, 6-function `bit`
  snippet, inline hook, `getmetatable` removal, shallow `_G` metatable, then the capture
  `redis.register_function` wiring.
- `lua_vm.rs::LuaVm::new` + `apply_sandbox` + `register_protected_global` + `setup_bit_library` +
  `setup_cjson_library` + `setup_cmsgpack_library` (~700 lines) — the real sandbox, plus ~450 lines
  of JSON/MessagePack conversion helpers, re-installed **per execution** from
  `set_redis_functions`.

Crate dependency direction: `frogdb-scripting` is a dependency of `frogdb-core` (core/Cargo.toml:27),
so the shared seam can only live in `frogdb-scripting`.

## Design

One deep module, `frogdb-scripting/src/sandbox.rs`, owning the whole sandbox:

```rust
pub enum SandboxMode { Load, Execute }

pub struct SandboxOptions {
    pub mode: SandboxMode,
    pub memory_limit_bytes: usize, // 0 = unlimited
}

/// THE constructor. Owns: stdlib set, forbidden-global scrubbing + safe os,
/// global protection (_real_G scheme, wrapped set/getmetatable, unpack compat),
/// full 11-fn bit library, cjson, cmsgpack, memory limit, and (Load mode) the
/// FUNCTION LOAD timeout hook.
pub fn build_frogdb_lua_vm(options: &SandboxOptions) -> Result<Lua, SandboxError>;

/// The one way to add a readonly global table (redis, bit, cjson, ...).
pub fn register_protected_global(lua: &Lua, name: &str, table: Table) -> Result<(), SandboxError>;

/// The one instruction hook (every 10k instructions): kill flag + time budget.
pub fn install_timeout_hook(lua: &Lua, start: Instant, hook: TimeoutHook);
pub fn remove_timeout_hook(lua: &Lua);

pub const REDIS_VERSION: &str = "7.2.0";
pub const REDIS_VERSION_NUM: i64 = 0x0007_0200;
```

The **base sandbox is identical in both modes** — that is the point. `SandboxMode` selects only the
lifecycle pieces that genuinely differ:

- `Load`: a fixed 5s load-budget hook is installed at construction (the VM lives for one library
  evaluation). The caller wires the capture `redis` table — `register_function` collects
  name/flags, `call`/`pcall` error with "not available during library loading".
- `Execute`: no hook at construction; `LuaVm` installs the per-execution hook (config timeout +
  grace + kill flag) through the same `install_timeout_hook`. The caller wires the live `redis`
  table (gate-backed `call`/`pcall`; executor.rs swaps `register_function` between the real
  registrar and the post-init error stub, unchanged).

The two call sites become thin adapters:

- `loader.rs::execute_in_sandbox` → `build_frogdb_lua_vm(Load)` + one function that assembles the
  capture `redis` table and installs it via `register_protected_global`. All inline VM assembly is
  deleted.
- `lua_vm.rs::LuaVm::new` → `build_frogdb_lua_vm(Execute)`. `apply_sandbox`, the private
  `register_protected_global`, `setup_bit_library`, `setup_cjson_library`, `setup_cmsgpack_library`,
  `BIT_LIBRARY_LUA`, `CjsonNull`, and the JSON/MessagePack conversion helpers are deleted from core
  (moved, not duplicated). `setup_timeout_hook`/`remove_timeout_hook` delegate to the sandbox
  module.

### Why this is the right depth

- **Locality.** "What is the FrogDB Lua sandbox" is now answerable by reading one module. Adding a
  library (`struct`, say) or a forbidden global is a one-line edit that both VMs inherit; it cannot
  land on one side only, because there is no "one side" to land on.
- **Interface vs. implementation.** The seam's interface is small — one constructor, one
  protected-global installer, one hook installer — while hiding ~900 lines of sandbox mechanics.
  Callers state *intent* (`Load` vs `Execute`), not mechanism.
- **Deletion test.** The migration deleted: the loader's parallel bit snippet, its shallow
  protection metatable, its inline hook, its `getmetatable` removal, core's `apply_sandbox`,
  core's `register_protected_global`, the three `setup_*_library` methods, `BIT_LIBRARY_LUA`, and
  ~450 lines of conversion helpers from core. If the seam couldn't delete those, its shape would be
  wrong.
- **Dependency direction respected.** The module lives in `frogdb-scripting` (leaf) and is consumed
  by `frogdb-core`; no new edges, no cyclic wiring, and the server handler
  (`connection/handlers/scripting/function.rs`) needed no changes — `load_library`'s signature is
  untouched.

### Redis alignment

Redis 7 builds **one** Lua environment per engine (`luaRegisterRedisAPI` in `script_lua.c`
registers `bit`, `cjson`, `cmsgpack`, `struct`, the readonly-table protection via
`lua_enablereadonlytable`, and globals protection once), and runs both `FUNCTION LOAD` and FCALL
inside it — library top-level code sees the full API at load time, and `redis.register_function`
is merely *gated* to the load context (`functions_lua.c` errors with "redis.register_function can
only be called inside a script invoked by FUNCTION LOAD"). FrogDB keeps two VM instances (the
capture VM is a load-time validity gate discarded after registration extraction, which suits the
per-shard execution model), but after this change both instances are the *same environment* by
construction, matching Redis's single-environment semantics:

- full `bit`/`cjson`/`cmsgpack` visible to library top-level code at load time (Redis: yes);
- `redis` table readonly at load time (Redis: yes, `lua_enablereadonlytable`);
- `register_function` outside load context errors (executor.rs error stub ≈ Redis's load-ctx gate);
- error strings preserved: "Attempt to modify a readonly table", "Script attempted to access
  nonexistent global variable" (both match Redis).

## Behavior changes (intentional, all convergences toward the execution/Redis semantics)

1. Library top-level code may now use the full `bit` library, `cjson`, `cmsgpack`, `os.clock`/
   `os.time`, and `unpack` during FUNCTION LOAD (previously load-time errors).
2. The loader no longer exposes `load`/`loadstring`/`print`/`collectgarbage` (sandbox hole closed).
3. The `redis` table is readonly at load time (`redis.call = 1` in top-level code now fails the
   load; it always failed at FCALL).
4. `getmetatable` exists at load time as the wrapped (frozen-proxy) version instead of being
   removed; `mt = getmetatable(_G)` at load still errors (global write → readonly error), so the
   regression pin `tcl_function_getmetatable_on_script_load` still holds.
5. `bit`/`cjson`/`cmsgpack` are installed **once at VM construction** instead of re-created on
   every execution. Consequence: the `cjson.decode_array_with_array_mt` flag now persists for the
   VM's lifetime rather than resetting per script — matching Redis, where lua-cjson config state
   persists per interpreter.
6. The loader's undeclared-global error now distinguishes forbidden names (read as `nil`) from
   undeclared names (error), like the execution VM.

## Testing

- `frogdb-scripting sandbox::tests` — cross-mode parity matrix driven over `[Load, Execute]`:
  full bit library (`tohex`/`rol`/`bswap`), global write ("Attempt to modify a readonly table"),
  undeclared global read, forbidden globals read as nil, safe `os`, cjson/cmsgpack round-trips,
  builtin tables readonly, `setmetatable(_G)`/`getmetatable(_G)` tricks blocked, `unpack` compat,
  `register_protected_global` visibility + readonly.
- `frogdb-scripting loader::tests` — load-path pins: library whose **top level** uses
  `bit.tohex`/`rol`/`bswap` loads; top-level cjson/cmsgpack loads; global write/read rejected with
  the execution-VM error strings; `redis` table readonly at load; forbidden globals nil at load;
  `redis.call` still unavailable during load.
- `frogdb-core lua_vm::tests` — execute-path parity against the *actual* loader: the same source
  is driven through `frogdb_scripting::load_library` and `LuaVm::execute` and asserted to succeed/
  fail with the same message (`test_*_parity_with_loader`).
- Existing suites kept green: `frogdb-scripting`, `frogdb-core` scripting, `frogdb-server`
  functions/scripting integration, `frogdb-redis-regression` functions_tcl/scripting_tcl
  (FUNCTION LOAD → FCALL round-trips, global-protection tricks, redis version API via `bit`).

## Risks / open questions

- **Two VMs remain.** This proposal unifies *construction*, not *instances*. The capture VM is
  still discarded and the library re-evaluated per shard at FCALL. Folding load into the execution
  VM (true single-environment, as Redis) would also remove the double-evaluation of top-level code,
  but requires routing FUNCTION LOAD through a shard VM and is orthogonal to the seam introduced
  here — the seam makes that follow-up smaller, not larger.
- **`struct` library.** Redis also exposes a `struct` library; FrogDB has none on either side.
  When added, it is a one-site change in `build_frogdb_lua_vm`.
- **Loader memory limit.** The loader passes `memory_limit_bytes: 0` (unlimited), preserving prior
  behavior; the 5s load hook bounds runaway loads. Wiring the configured heap limit into the load
  path is a policy decision left open (Redis bounds it via the shared interpreter's limits).
