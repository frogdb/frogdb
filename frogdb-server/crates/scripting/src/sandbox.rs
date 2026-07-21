//! Single construction seam for the FrogDB Lua sandbox.
//!
//! FrogDB has two Lua environments: the FUNCTION LOAD capture VM (built by
//! [`crate::loader`], used once per `FUNCTION LOAD` to extract function
//! names/flags) and the per-shard execution VM (built by `frogdb-core`'s
//! `LuaVm`, used for EVAL/FCALL). Redis builds *one* environment
//! (`luaRegisterRedisAPI` in `script_lua.c`) and runs both library loading and
//! execution inside it, so the two can never diverge. FrogDB keeps two VMs
//! (the capture VM is discarded after load), but both MUST present the same
//! sandbox: same stdlib set, same forbidden globals, same `bit`/`cjson`/
//! `cmsgpack` libraries, same global-protection scheme, same instruction-hook
//! mechanism. This module is the only place that sandbox is defined.
//!
//! [`build_frogdb_lua_vm`] owns everything that is mode-independent. The only
//! things a caller adds afterwards are the `redis` table dialect — live
//! `redis.call`/`register_function` for execution, capture stubs for load —
//! installed via [`register_protected_global`], and (for the execution VM)
//! the per-execution timeout hook via [`install_timeout_hook`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use mlua::{HookTriggers, IntoLua, Lua, Result as LuaResult, StdLib, Table, Value, VmState};

/// Redis version reported to scripts as `redis.REDIS_VERSION`.
///
/// Single-sourced from [`frogdb_types::ADVERTISED_REDIS_VERSION`] so the
/// scripting API, INFO's `redis_version`, and the replication RDB AUX field
/// never drift apart.
pub const REDIS_VERSION: &str = frogdb_types::ADVERTISED_REDIS_VERSION;
/// Redis version reported to scripts as `redis.REDIS_VERSION_NUM`
/// (`(major << 16) | (minor << 8) | patch`).
pub const REDIS_VERSION_NUM: i64 = 0x0007_0200;

/// Instruction-count interval between timeout-hook checks.
const HOOK_INSTRUCTION_INTERVAL: u32 = 10_000;
/// Wall-clock budget for running library top-level code during FUNCTION LOAD.
const LOAD_TIMEOUT_MS: u64 = 5_000;

/// Registry key for the backing globals table (`_real_G`).
const REGISTRY_BACKING_TABLE: &str = "__frogdb_backing";
/// Registry key for the set of read-only protected tables.
const REGISTRY_PROTECTED_SET: &str = "__frogdb_protected";

/// Lua source for the LuaJIT-compatible `bit` library (Redis bundles LuaJIT's
/// `bit`; Lua 5.4 has native operators but no `bit` table).
const BIT_LIBRARY_LUA: &str = r#"
local bit = {}
function bit.tobit(x) x = x % 0x100000000; if x >= 0x80000000 then x = x - 0x100000000 end; return x end
function bit.band(a, ...) local r = a; for _, v in ipairs({...}) do r = r & v end; return bit.tobit(r) end
function bit.bor(a, ...) local r = a; for _, v in ipairs({...}) do r = r | v end; return bit.tobit(r) end
function bit.bxor(a, ...) local r = a; for _, v in ipairs({...}) do r = r ~ v end; return bit.tobit(r) end
function bit.bnot(a) return bit.tobit(~a) end
function bit.lshift(a, n) return bit.tobit(a << n) end
function bit.rshift(a, n) local ua = a % 0x100000000; return bit.tobit(ua >> n) end
function bit.arshift(a, n) return bit.tobit(a >> n) end
function bit.rol(a, n) n = n % 32; local ua = a % 0x100000000; return bit.tobit((ua << n) | (ua >> (32 - n))) end
function bit.ror(a, n) n = n % 32; local ua = a % 0x100000000; return bit.tobit((ua >> n) | (ua << (32 - n))) end
function bit.bswap(a) local ua = a % 0x100000000; return bit.tobit(((ua & 0xFF) << 24) | (((ua >> 8) & 0xFF) << 16) | (((ua >> 16) & 0xFF) << 8) | ((ua >> 24) & 0xFF)) end
function bit.tohex(x, n) x = x % 0x100000000; if n == nil then n = 8 end; local upper = false; if n < 0 then upper = true; n = -n end; if n == 0 then n = 8 end; if n > 8 then n = 8 end; local fmt = string.format("%%0%d%s", n, upper and "X" or "x"); local s = string.format(fmt, x); if #s > n then s = string.sub(s, #s - n + 1) end; return s end
return bit
"#;

/// Lua chunk that converts a plain table into a readonly proxy: contents are
/// moved to a hidden backing table, `__newindex` errors, `__metatable` is
/// locked. Takes the table as its single argument.
const PROTECT_TABLE_LUA: &str = r#"
local t = ...
local backing = {}
for k, v in pairs(t) do backing[k] = v end
local keys = {}
for k in pairs(t) do keys[#keys + 1] = k end
for _, k in ipairs(keys) do t[k] = nil end
setmetatable(t, {
    __newindex = function()
        error("Attempt to modify a readonly table")
    end,
    __index = backing,
    __metatable = "The metatable is locked",
})
"#;

/// Error produced while constructing or extending the sandbox.
#[derive(Debug)]
pub struct SandboxError(pub String);

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SandboxError {}

/// Which redis-table dialect the caller intends to wire onto this VM.
///
/// The base sandbox is IDENTICAL in both modes — that is the point of this
/// module. The mode only selects the lifecycle pieces that differ:
///
/// - `Load`: the VM exists to run library top-level code once during
///   FUNCTION LOAD, so a fixed load-budget timeout hook is installed at
///   construction. The caller wires a capture `redis.register_function`
///   (collects name/flags) and erroring `redis.call`/`redis.pcall` stubs.
/// - `Execute`: the VM is long-lived (per shard); timeout hooks are installed
///   per execution via [`install_timeout_hook`]. The caller wires live
///   `redis.call`/`redis.pcall` and a real `register_function`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxMode {
    /// FUNCTION LOAD capture pass.
    Load,
    /// Real script/function execution.
    Execute,
}

/// Options for [`build_frogdb_lua_vm`].
#[derive(Debug, Clone)]
pub struct SandboxOptions {
    /// Lifecycle mode (see [`SandboxMode`]).
    pub mode: SandboxMode,
    /// Lua heap limit in bytes. `0` = unlimited.
    pub memory_limit_bytes: usize,
}

/// Build the FrogDB Lua sandbox.
///
/// Owns, for both modes:
/// - the stdlib set (`coroutine`, `table`, `string`, `math` + mlua's base),
/// - removal of dangerous base-library globals (`load`, `require`, `print`,
///   `debug`, ...) with a safe `os` table (`clock`/`time` only),
/// - the global-protection scheme (readonly `_G` with a hidden backing table,
///   wrapped `setmetatable`/`getmetatable`, `rawset`/`rawget` removed,
///   Lua 5.1-compat `unpack`),
/// - the full LuaJIT-compatible `bit` library,
/// - `cjson` and `cmsgpack`,
/// - the memory limit,
/// - in `Load` mode, the FUNCTION LOAD instruction/timeout hook.
///
/// The returned VM has NO `redis` table: callers install their dialect via
/// [`register_protected_global`].
pub fn build_frogdb_lua_vm(options: &SandboxOptions) -> Result<Lua, SandboxError> {
    let libs = StdLib::COROUTINE | StdLib::TABLE | StdLib::STRING | StdLib::MATH;
    let lua = unsafe { Lua::unsafe_new_with(libs, mlua::LuaOptions::default()) };

    if options.memory_limit_bytes > 0 {
        lua.set_memory_limit(options.memory_limit_bytes)
            .map_err(|e| SandboxError(format!("Failed to set memory limit: {e}")))?;
    }

    apply_global_protection(&lua)?;
    setup_bit_library(&lua)?;
    setup_cjson_library(&lua)?;
    setup_cmsgpack_library(&lua)?;

    if options.mode == SandboxMode::Load {
        install_timeout_hook(
            &lua,
            Instant::now(),
            TimeoutHook {
                timeout_ms: LOAD_TIMEOUT_MS,
                grace_ms: 0,
                kill_flag: None,
                context: TimeoutContext::LibraryLoad,
            },
        );
    }

    Ok(lua)
}

/// What the timeout hook is guarding, which selects the BUSY error message.
#[derive(Debug, Clone, Copy)]
pub enum TimeoutContext {
    /// Library top-level code running during FUNCTION LOAD.
    LibraryLoad,
    /// A script or function body running on the execution VM.
    ScriptExecution,
}

/// Parameters for [`install_timeout_hook`].
#[derive(Debug, Clone)]
pub struct TimeoutHook {
    /// Time budget in milliseconds. `0` disables the hook entirely.
    pub timeout_ms: u64,
    /// Grace period added on top of `timeout_ms` before the hook errors.
    pub grace_ms: u64,
    /// Optional kill flag checked on every hook invocation (SCRIPT KILL).
    pub kill_flag: Option<Arc<AtomicBool>>,
    /// Selects the BUSY error message.
    pub context: TimeoutContext,
}

/// Install the sandbox instruction hook: every [`HOOK_INSTRUCTION_INTERVAL`]
/// instructions, check the kill flag (if any) and the elapsed-time budget.
///
/// No hook is installed when `timeout_ms == 0` (matching the historical
/// execution-VM behavior where an unlimited budget also disables kill checks).
pub fn install_timeout_hook(lua: &Lua, start: Instant, hook: TimeoutHook) {
    if hook.timeout_ms == 0 {
        return;
    }
    let TimeoutHook {
        timeout_ms,
        grace_ms,
        kill_flag,
        context,
    } = hook;
    let triggers = HookTriggers::new().every_nth_instruction(HOOK_INSTRUCTION_INTERVAL);
    let _ = lua.set_hook(triggers, move |_lua, _debug| {
        if let Some(flag) = &kill_flag
            && flag.load(Ordering::Relaxed)
        {
            return Err(mlua::Error::RuntimeError("Script killed".to_string()));
        }
        let elapsed = start.elapsed().as_millis() as u64;
        if elapsed > timeout_ms + grace_ms {
            let msg = match context {
                TimeoutContext::LibraryLoad => {
                    "BUSY timeout during FUNCTION LOAD (library loading took too long)".to_string()
                }
                TimeoutContext::ScriptExecution => {
                    format!("BUSY script running for {} ms", elapsed)
                }
            };
            return Err(mlua::Error::RuntimeError(msg));
        }
        Ok(VmState::Continue)
    });
}

/// Remove the sandbox instruction hook.
pub fn remove_timeout_hook(lua: &Lua) {
    lua.remove_hook();
}

/// Apply the sandbox restrictions and global-protection scheme.
///
/// This emulates Redis's `scriptingEnableGlobalsProtection()` /
/// `lua_enablereadonlytable`:
/// - Snapshot all current globals into a backing table (`_real_G`), then
///   clear `_G` so reads go through `__index` and writes trigger `__newindex`.
/// - `__index` errors on nonexistent globals (like Redis's
///   `luaProtectedTableError`); explicitly-forbidden globals return `nil`.
/// - `__newindex` always errors ("Attempt to modify a readonly table").
/// - `__metatable = "The metatable is locked"` prevents `setmetatable(_G)`.
/// - `setmetatable`/`getmetatable` are replaced with wrappers that enforce
///   readonly semantics for protected tables (`lua_enablereadonlytable` is a
///   C-level Redis feature not available in standard Lua 5.4).
/// - `rawset`/`rawget` are removed from user-visible globals.
/// - The backing table and protected-set are stored as hidden raw keys in
///   `_G` (inaccessible to user code since `__index` checks `_real_G`).
fn apply_global_protection(lua: &Lua) -> Result<(), SandboxError> {
    let globals = lua.globals();

    // Remove dangerous functions from global scope
    let forbidden = [
        "loadfile",
        "dofile",
        "load",           // Can load bytecode
        "loadstring",     // Lua 5.1 alias for load
        "setfenv",        // Lua 5.1 only, but be safe
        "getfenv",        // Lua 5.1 only, but be safe
        "module",         // Deprecated
        "require",        // No module loading
        "package",        // No package system
        "io",             // No I/O
        "os",             // No OS access (we'll add back safe functions)
        "debug",          // No debug library
        "collectgarbage", // No GC manipulation
        "print",          // Not available in Redis scripting
    ];

    for name in &forbidden {
        let _ = globals.raw_set(*name, Value::Nil);
    }

    // Create a safe os table with only clock and time
    let os_table = lua
        .create_table()
        .map_err(|e| SandboxError(format!("Failed to create os table: {e}")))?;

    let clock_fn = lua
        .create_function(|_, ()| -> LuaResult<f64> {
            Ok(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0))
        })
        .map_err(|e| SandboxError(format!("Failed to create clock fn: {e}")))?;

    let time_fn = lua
        .create_function(|_, ()| -> LuaResult<i64> {
            Ok(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0))
        })
        .map_err(|e| SandboxError(format!("Failed to create time fn: {e}")))?;

    os_table
        .set("clock", clock_fn)
        .map_err(|e| SandboxError(format!("Failed to set os.clock: {e}")))?;
    os_table
        .set("time", time_fn)
        .map_err(|e| SandboxError(format!("Failed to set os.time: {e}")))?;
    globals
        .set("os", os_table)
        .map_err(|e| SandboxError(format!("Failed to set os: {e}")))?;

    // Build a table of explicitly-forbidden global names so the __index
    // handler can distinguish them from truly-undeclared variables.
    // Forbidden globals return nil; undeclared globals error.
    {
        let forbidden_table = lua
            .create_table()
            .map_err(|e| SandboxError(format!("Failed to create forbidden table: {e}")))?;
        for name in &forbidden {
            forbidden_table
                .set(*name, true)
                .map_err(|e| SandboxError(format!("Failed to add forbidden name: {e}")))?;
        }
        globals
            .raw_set("__frogdb_forbidden", forbidden_table)
            .map_err(|e| SandboxError(format!("Failed to set forbidden table: {e}")))?;
    }

    lua.load(
        r#"
local _rawset = rawset
local _rawget = rawget
local _setmetatable = setmetatable
local _getmetatable = getmetatable

-- Retrieve the forbidden-names table set by Rust
local _forbidden = _rawget(_G, "__frogdb_forbidden") or {}
_rawset(_G, "__frogdb_forbidden", nil)

-- Lua 5.1 compat: global `unpack` with range guard
do
    local _table_unpack = table.unpack
    local MAX_UNPACK = 8000
    _G.unpack = function(t, i, j)
        i = i or 1
        j = j or #t
        if j - i + 1 > MAX_UNPACK then
            error("too many results to unpack")
        end
        return _table_unpack(t, i, j)
    end
end

-- Snapshot current globals into backing table
local _real_G = {}
for k, v in pairs(_G) do _real_G[k] = v end
_real_G.rawset = nil
_real_G.rawget = nil

-- Set of tables that are protected (readonly)
local _protected = {}
_protected[_G] = true

-- Replace setmetatable: error for protected tables
_real_G.setmetatable = function(t, mt)
    if _protected[t] then
        error("Attempt to modify a readonly table")
    end
    return _setmetatable(t, mt)
end

-- Replace getmetatable: return a frozen proxy for protected tables.
-- Redis returns the real metatable (also readonly via lua_enablereadonlytable).
-- We return a dummy readonly table so that attempts to modify it (e.g.
-- `local g = getmetatable(_G); g.__index = {}`) raise the right error.
_real_G.getmetatable = function(t)
    if _protected[t] then
        local proxy = {}
        _setmetatable(proxy, {
            __newindex = function()
                error("Attempt to modify a readonly table")
            end,
            __index = function() return nil end,
            __metatable = "The metatable is locked",
        })
        return proxy
    end
    return _getmetatable(t)
end

-- Apply the metatable to _G
_setmetatable(_G, {
    __newindex = function(_, key)
        error("Attempt to modify a readonly table")
    end,
    __index = function(_, key)
        local v = _real_G[key]
        if v ~= nil then
            return v
        end
        -- Explicitly-forbidden globals return nil (they were intentionally
        -- removed, not undeclared). This matches Redis behavior where
        -- type(require) returns "nil" rather than erroring.
        if _forbidden[key] then
            return nil
        end
        -- Error on truly undeclared globals (like Redis's luaProtectedTableError)
        error("Script attempted to access nonexistent global variable '" .. tostring(key) .. "'")
    end,
    __metatable = "The metatable is locked",
})

-- Clear all raw keys from _G so everything goes through the metatable
local _keys = {}
for k in pairs(_G) do _keys[#_keys + 1] = k end
for _, k in ipairs(_keys) do _rawset(_G, k, nil) end

-- Store backing table and protected set as hidden raw keys in _G.
-- User scripts cannot see these because __index only checks _real_G.
_rawset(_G, "__frogdb_backing", _real_G)
_rawset(_G, "__frogdb_protected", _protected)
"#,
    )
    .exec()
    .map_err(|e| SandboxError(format!("Failed to apply global protection: {e}")))?;

    Ok(())
}

/// Register a table as a global in the backing table and protect it from
/// writes. This must be used instead of `globals().raw_set()` for tables
/// that should be visible as globals AND readonly (e.g., `redis`, `bit`,
/// `cjson`). Only valid on a VM built by [`build_frogdb_lua_vm`].
pub fn register_protected_global(lua: &Lua, name: &str, table: Table) -> Result<(), SandboxError> {
    let globals = lua.globals();

    // Get the backing table and protected set from hidden raw keys
    let backing: Table = globals
        .raw_get(REGISTRY_BACKING_TABLE)
        .map_err(|e| SandboxError(format!("Failed to get backing table: {e}")))?;
    let protected: Table = globals
        .raw_get(REGISTRY_PROTECTED_SET)
        .map_err(|e| SandboxError(format!("Failed to get protected set: {e}")))?;

    // Protect the table itself with a readonly metatable.
    // We use the real setmetatable via Lua code. The sandbox's wrapped
    // setmetatable would also work since the table is not yet protected.
    lua.load(PROTECT_TABLE_LUA)
        .call::<()>(table.clone())
        .map_err(|e| SandboxError(format!("Failed to protect {name} table: {e}")))?;

    // Mark as protected
    protected
        .set(table.clone(), true)
        .map_err(|e| SandboxError(format!("Failed to mark {name} as protected: {e}")))?;

    // Add to backing table so it's accessible via _G.__index
    backing
        .set(name, table)
        .map_err(|e| SandboxError(format!("Failed to register {name} global: {e}")))?;

    Ok(())
}

/// Set up the `bit` compatibility library for Redis scripting compatibility.
fn setup_bit_library(lua: &Lua) -> Result<(), SandboxError> {
    let bit_table: Table = lua
        .load(BIT_LIBRARY_LUA)
        .call(())
        .map_err(|e| SandboxError(format!("Failed to create bit library: {e}")))?;
    register_protected_global(lua, "bit", bit_table)?;
    Ok(())
}

/// Set up the `cjson` library for Redis scripting compatibility.
///
/// Provides `cjson.encode`, `cjson.decode`, `cjson.null`,
/// `cjson.decode_array_with_array_mt`, and `cjson.decode_invalid_numbers`.
fn setup_cjson_library(lua: &Lua) -> Result<(), SandboxError> {
    let cjson = lua
        .create_table()
        .map_err(|e| SandboxError(format!("Failed to create cjson table: {e}")))?;

    // --- cjson.null sentinel ---
    let null_ud = lua
        .create_any_userdata(CjsonNull)
        .map_err(|e| SandboxError(format!("Failed to create cjson.null: {e}")))?;
    // Store in registry so closures can retrieve independent copies.
    let null_key = lua
        .create_registry_value(null_ud.clone())
        .map_err(|e| SandboxError(format!("Failed to register cjson.null: {e}")))?;

    cjson
        .set("null", null_ud)
        .map_err(|e| SandboxError(format!("Failed to set cjson.null: {e}")))?;

    // --- array metatable (shared, read-only) ---
    let array_mt = lua
        .create_table()
        .map_err(|e| SandboxError(format!("Failed to create array mt: {e}")))?;
    let is_array_fn = lua
        .create_function(|_, _: ()| Ok(true))
        .map_err(|e| SandboxError(format!("Failed to create __is_cjson_array: {e}")))?;
    array_mt
        .set("__is_cjson_array", is_array_fn)
        .map_err(|e| SandboxError(format!("Failed to set __is_cjson_array: {e}")))?;
    // Make the array metatable readonly using the same technique as
    // register_protected_global: move contents to a backing table, set
    // __newindex to error, __index to the backing table.
    lua.load(PROTECT_TABLE_LUA)
        .call::<()>(array_mt.clone())
        .map_err(|e| SandboxError(format!("Failed to protect array mt: {e}")))?;
    // Also add to protected set so setmetatable(array_mt, ...) errors
    {
        let globals = lua.globals();
        let protected: Table = globals
            .raw_get(REGISTRY_PROTECTED_SET)
            .map_err(|e| SandboxError(format!("Failed to get protected set for array mt: {e}")))?;
        protected
            .set(array_mt.clone(), true)
            .map_err(|e| SandboxError(format!("Failed to mark array mt as protected: {e}")))?;
    }

    let array_mt_key = lua
        .create_registry_value(array_mt)
        .map_err(|e| SandboxError(format!("Failed to register array mt: {e}")))?;

    // --- per-VM flag: use array metatable on decoded arrays ---
    let use_array_mt = Arc::new(AtomicBool::new(false));

    // --- cjson.encode ---
    let null_key_enc = lua
        .create_registry_value(
            lua.registry_value::<Value>(&null_key)
                .map_err(|e| SandboxError(format!("Failed to get cjson.null for encode: {e}")))?,
        )
        .map_err(|e| SandboxError(format!("Failed to register cjson.null for encode: {e}")))?;
    let array_mt_key_enc = lua
        .create_registry_value(
            lua.registry_value::<Value>(&array_mt_key)
                .map_err(|e| SandboxError(format!("Failed to get array mt for encode: {e}")))?,
        )
        .map_err(|e| SandboxError(format!("Failed to register array mt for encode: {e}")))?;

    let encode_fn = lua
        .create_function(move |lua, val: Value| -> LuaResult<String> {
            let null_val: Value = lua.registry_value(&null_key_enc)?;
            let amt_val: Value = lua.registry_value(&array_mt_key_enc)?;
            let json = lua_to_json(&val, &null_val, &amt_val)?;
            let s = serde_json::to_string(&json)
                .map_err(|e| mlua::Error::RuntimeError(format!("cjson.encode: {e}")))?;
            Ok(s)
        })
        .map_err(|e| SandboxError(format!("Failed to create cjson.encode: {e}")))?;

    cjson
        .set("encode", encode_fn)
        .map_err(|e| SandboxError(format!("Failed to set cjson.encode: {e}")))?;

    // --- cjson.decode ---
    let null_key_dec = lua
        .create_registry_value(
            lua.registry_value::<Value>(&null_key)
                .map_err(|e| SandboxError(format!("Failed to get cjson.null for decode: {e}")))?,
        )
        .map_err(|e| SandboxError(format!("Failed to register cjson.null for decode: {e}")))?;
    let array_mt_key_dec = lua
        .create_registry_value(
            lua.registry_value::<Value>(&array_mt_key)
                .map_err(|e| SandboxError(format!("Failed to get array mt for decode: {e}")))?,
        )
        .map_err(|e| SandboxError(format!("Failed to register array mt for decode: {e}")))?;
    let use_array_mt_dec = use_array_mt.clone();

    let decode_fn = lua
        .create_function(move |lua, s: mlua::String| -> LuaResult<Value> {
            let json_str = s.to_str()?;
            let json: serde_json::Value = serde_json::from_str(json_str.as_ref())
                .map_err(|e| mlua::Error::RuntimeError(format!("cjson.decode: {e}")))?;
            let null_val: Value = lua.registry_value(&null_key_dec)?;
            let amt: Option<Table> = if use_array_mt_dec.load(Ordering::Relaxed) {
                Some(lua.registry_value(&array_mt_key_dec)?)
            } else {
                None
            };
            json_to_lua(lua, &json, &null_val, amt.as_ref())
        })
        .map_err(|e| SandboxError(format!("Failed to create cjson.decode: {e}")))?;

    cjson
        .set("decode", decode_fn)
        .map_err(|e| SandboxError(format!("Failed to set cjson.decode: {e}")))?;

    // --- cjson.decode_array_with_array_mt(bool) ---
    let use_array_mt_setter = use_array_mt.clone();
    let damt_fn = lua
        .create_function(move |_, enable: bool| -> LuaResult<()> {
            use_array_mt_setter.store(enable, Ordering::Relaxed);
            Ok(())
        })
        .map_err(|e| {
            SandboxError(format!(
                "Failed to create cjson.decode_array_with_array_mt: {e}"
            ))
        })?;
    cjson
        .set("decode_array_with_array_mt", damt_fn)
        .map_err(|e| {
            SandboxError(format!(
                "Failed to set cjson.decode_array_with_array_mt: {e}"
            ))
        })?;

    // --- cjson.decode_invalid_numbers(bool) --- no-op stub
    let din_fn = lua
        .create_function(|_, _: bool| -> LuaResult<()> { Ok(()) })
        .map_err(|e| {
            SandboxError(format!(
                "Failed to create cjson.decode_invalid_numbers: {e}"
            ))
        })?;
    cjson
        .set("decode_invalid_numbers", din_fn)
        .map_err(|e| SandboxError(format!("Failed to set cjson.decode_invalid_numbers: {e}")))?;

    register_protected_global(lua, "cjson", cjson)?;
    Ok(())
}

/// Set up the `cmsgpack` library for Redis scripting compatibility.
///
/// Provides `cmsgpack.pack` and `cmsgpack.unpack` using the `rmpv` crate.
fn setup_cmsgpack_library(lua: &Lua) -> Result<(), SandboxError> {
    let cmsgpack = lua
        .create_table()
        .map_err(|e| SandboxError(format!("Failed to create cmsgpack table: {e}")))?;

    // --- cmsgpack.pack ---
    let pack_fn = lua
        .create_function(|lua, val: Value| -> LuaResult<mlua::String> {
            let msgpack_val = lua_to_msgpack(&val)?;
            let mut buf = Vec::new();
            rmpv::encode::write_value(&mut buf, &msgpack_val)
                .map_err(|e| mlua::Error::RuntimeError(format!("cmsgpack.pack: {e}")))?;
            lua.create_string(&buf)
        })
        .map_err(|e| SandboxError(format!("Failed to create cmsgpack.pack: {e}")))?;

    cmsgpack
        .set("pack", pack_fn)
        .map_err(|e| SandboxError(format!("Failed to set cmsgpack.pack: {e}")))?;

    // --- cmsgpack.unpack ---
    let unpack_fn = lua
        .create_function(|lua, data: mlua::String| -> LuaResult<Value> {
            let bytes = data.as_bytes();
            let mut cursor = std::io::Cursor::new(bytes);
            let msgpack_val = rmpv::decode::read_value(&mut cursor)
                .map_err(|e| mlua::Error::RuntimeError(format!("cmsgpack.unpack: {e}")))?;
            msgpack_to_lua(lua, &msgpack_val)
        })
        .map_err(|e| SandboxError(format!("Failed to create cmsgpack.unpack: {e}")))?;

    cmsgpack
        .set("unpack", unpack_fn)
        .map_err(|e| SandboxError(format!("Failed to set cmsgpack.unpack: {e}")))?;

    register_protected_global(lua, "cmsgpack", cmsgpack)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// cjson.null sentinel type
// ---------------------------------------------------------------------------

/// Sentinel userdata representing JSON null for the cjson library.
struct CjsonNull;

impl std::fmt::Display for CjsonNull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}

// ---------------------------------------------------------------------------
// JSON <-> Lua conversion helpers
// ---------------------------------------------------------------------------

/// Determine if a Lua table is an array (sequential integer keys 1..n).
fn table_is_array(t: &Table, array_mt: &Value) -> LuaResult<bool> {
    // If the table carries the cjson array metatable, treat it as an array.
    if let Some(mt) = t.metatable()
        && let Value::Table(amt) = array_mt
        && mt.equals(amt)?
    {
        return Ok(true);
    }

    let len = t.raw_len();
    if len > 0 {
        // Quick heuristic: if t[1] exists, treat as array.
        let first: Value = t.raw_get(1)?;
        if first != Value::Nil {
            return Ok(true);
        }
    }
    // len == 0 could be empty array or empty object. Default: object.
    // (array-metatable-tagged tables are handled above.)
    let mut has_string_keys = false;
    for pair in t.clone().pairs::<Value, Value>() {
        let (k, _) = pair?;
        if matches!(k, Value::String(_)) {
            has_string_keys = true;
            break;
        }
    }
    if has_string_keys {
        return Ok(false);
    }
    Ok(len > 0)
}

/// Convert a Lua value to a `serde_json::Value`.
fn lua_to_json(val: &Value, null_ud: &Value, array_mt: &Value) -> LuaResult<serde_json::Value> {
    match val {
        Value::Nil => Ok(serde_json::Value::Null),
        Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Integer(n) => Ok(serde_json::Value::Number(serde_json::Number::from(*n))),
        Value::Number(n) => {
            if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                Ok(serde_json::Value::Number(serde_json::Number::from(
                    *n as i64,
                )))
            } else {
                serde_json::Number::from_f64(*n)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        mlua::Error::RuntimeError("cjson.encode: cannot encode NaN/Inf".to_string())
                    })
            }
        }
        Value::String(s) => {
            let rs = s.to_str()?;
            Ok(serde_json::Value::String(rs.as_ref().to_owned()))
        }
        Value::Table(t) => {
            if table_is_array(t, array_mt)? {
                let len = t.raw_len();
                let mut arr = Vec::with_capacity(len);
                for i in 1..=len {
                    let v: Value = t.raw_get(i)?;
                    arr.push(lua_to_json(&v, null_ud, array_mt)?);
                }
                Ok(serde_json::Value::Array(arr))
            } else {
                let mut map = serde_json::Map::new();
                for pair in t.clone().pairs::<Value, Value>() {
                    let (k, v) = pair?;
                    let key = match &k {
                        Value::String(s) => s.to_str()?.as_ref().to_owned(),
                        Value::Integer(n) => n.to_string(),
                        Value::Number(n) => n.to_string(),
                        _ => {
                            return Err(mlua::Error::RuntimeError(
                                "cjson.encode: non-string table key".to_string(),
                            ));
                        }
                    };
                    map.insert(key, lua_to_json(&v, null_ud, array_mt)?);
                }
                Ok(serde_json::Value::Object(map))
            }
        }
        Value::UserData(_) => {
            if val.equals(null_ud)? {
                Ok(serde_json::Value::Null)
            } else {
                Err(mlua::Error::RuntimeError(
                    "cjson.encode: unsupported userdata".to_string(),
                ))
            }
        }
        _ => Err(mlua::Error::RuntimeError(
            "cjson.encode: unsupported Lua type".to_string(),
        )),
    }
}

/// Convert a `serde_json::Value` to a Lua value.
fn json_to_lua(
    lua: &Lua,
    json: &serde_json::Value,
    null_val: &Value,
    array_mt: Option<&Table>,
) -> LuaResult<Value> {
    match json {
        serde_json::Value::Null => Ok(null_val.clone()),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                // Redis cjson promotes whole-number floats (e.g. 0.0, -5e3) to
                // integers so that table.concat renders them without ".0".
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(Value::Integer(f as i64))
                } else {
                    Ok(Value::Number(f))
                }
            } else {
                Err(mlua::Error::RuntimeError(
                    "cjson.decode: unsupported number".to_string(),
                ))
            }
        }
        serde_json::Value::String(s) => {
            let ls = lua.create_string(s.as_bytes())?;
            ls.into_lua(lua)
        }
        serde_json::Value::Array(arr) => {
            let t = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                t.set(i + 1, json_to_lua(lua, v, null_val, array_mt)?)?;
            }
            if let Some(mt) = array_mt {
                let _ = t.set_metatable(Some(mt.clone()));
            }
            Ok(Value::Table(t))
        }
        serde_json::Value::Object(map) => {
            let t = lua.create_table()?;
            for (k, v) in map {
                let lk = lua.create_string(k.as_bytes())?;
                t.set(lk, json_to_lua(lua, v, null_val, array_mt)?)?;
            }
            Ok(Value::Table(t))
        }
    }
}

// ---------------------------------------------------------------------------
// MessagePack <-> Lua conversion helpers
// ---------------------------------------------------------------------------

/// Convert a Lua value to an `rmpv::Value`.
fn lua_to_msgpack(val: &Value) -> LuaResult<rmpv::Value> {
    match val {
        Value::Nil => Ok(rmpv::Value::Nil),
        Value::Boolean(b) => Ok(rmpv::Value::Boolean(*b)),
        Value::Integer(n) => {
            if *n >= 0 {
                Ok(rmpv::Value::Integer(rmpv::Integer::from(*n as u64)))
            } else {
                Ok(rmpv::Value::Integer(rmpv::Integer::from(*n)))
            }
        }
        Value::Number(n) => Ok(rmpv::Value::F64(*n)),
        Value::String(s) => {
            let rs = s.to_str()?;
            Ok(rmpv::Value::String(rmpv::Utf8String::from(
                rs.as_ref().to_owned(),
            )))
        }
        Value::Table(t) => {
            let len = t.raw_len();
            let first: Value = t.raw_get(1)?;
            if len > 0 && first != Value::Nil {
                let mut arr = Vec::with_capacity(len);
                for i in 1..=len {
                    let v: Value = t.raw_get(i)?;
                    arr.push(lua_to_msgpack(&v)?);
                }
                Ok(rmpv::Value::Array(arr))
            } else {
                let mut map = Vec::new();
                for pair in t.clone().pairs::<Value, Value>() {
                    let (k, v) = pair?;
                    map.push((lua_to_msgpack(&k)?, lua_to_msgpack(&v)?));
                }
                Ok(rmpv::Value::Map(map))
            }
        }
        _ => Err(mlua::Error::RuntimeError(
            "cmsgpack.pack: unsupported Lua type".to_string(),
        )),
    }
}

/// Convert an `rmpv::Value` to a Lua value.
fn msgpack_to_lua(lua: &Lua, val: &rmpv::Value) -> LuaResult<Value> {
    match val {
        rmpv::Value::Nil => Ok(Value::Nil),
        rmpv::Value::Boolean(b) => Ok(Value::Boolean(*b)),
        rmpv::Value::Integer(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(u) = n.as_u64() {
                Ok(Value::Number(u as f64))
            } else {
                Err(mlua::Error::RuntimeError(
                    "cmsgpack.unpack: unsupported integer".to_string(),
                ))
            }
        }
        rmpv::Value::F32(f) => Ok(Value::Number(*f as f64)),
        rmpv::Value::F64(f) => Ok(Value::Number(*f)),
        rmpv::Value::String(s) => {
            let rs = s.as_str().unwrap_or("");
            let ls = lua.create_string(rs.as_bytes())?;
            ls.into_lua(lua)
        }
        rmpv::Value::Binary(b) => {
            let ls = lua.create_string(b)?;
            ls.into_lua(lua)
        }
        rmpv::Value::Array(arr) => {
            let t = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                t.set(i + 1, msgpack_to_lua(lua, v)?)?;
            }
            Ok(Value::Table(t))
        }
        rmpv::Value::Map(map) => {
            let t = lua.create_table()?;
            for (k, v) in map {
                t.set(msgpack_to_lua(lua, k)?, msgpack_to_lua(lua, v)?)?;
            }
            Ok(Value::Table(t))
        }
        rmpv::Value::Ext(_, _) => Err(mlua::Error::RuntimeError(
            "cmsgpack.unpack: ext type not supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(mode: SandboxMode) -> Lua {
        build_frogdb_lua_vm(&SandboxOptions {
            mode,
            memory_limit_bytes: 0,
        })
        .unwrap()
    }

    fn exec_err(lua: &Lua, code: &str) -> String {
        lua.load(code).exec().unwrap_err().to_string()
    }

    /// The full 11-function LuaJIT-compatible bit library is present in BOTH
    /// modes (the load-time VM historically had only 6 functions).
    #[test]
    fn test_bit_library_identical_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let lua = build(mode);
            let hex: String = lua.load("return bit.tohex(255)").eval().unwrap();
            assert_eq!(hex, "000000ff", "mode {mode:?}");
            let rol: i64 = lua.load("return bit.rol(1, 1)").eval().unwrap();
            assert_eq!(rol, 2, "mode {mode:?}");
            let bswap: i64 = lua.load("return bit.bswap(0x12345678)").eval().unwrap();
            assert_eq!(bswap, 0x78563412, "mode {mode:?}");
        }
    }

    /// Setting an undeclared global raises the same readonly error in both
    /// modes.
    #[test]
    fn test_global_write_parity() {
        let load_err = exec_err(&build(SandboxMode::Load), "x = 1");
        let exec_err_ = exec_err(&build(SandboxMode::Execute), "x = 1");
        assert!(load_err.contains("Attempt to modify a readonly table"));
        assert!(exec_err_.contains("Attempt to modify a readonly table"));
    }

    /// Reading an undeclared global raises the same error in both modes.
    #[test]
    fn test_global_read_parity() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let err = exec_err(&build(mode), "return undeclared_thing");
            assert!(
                err.contains("Script attempted to access nonexistent global variable"),
                "mode {mode:?}: {err}"
            );
        }
    }

    /// Forbidden base-library globals are nil in both modes (the load-time VM
    /// historically leaked `load`, `print`, etc.).
    #[test]
    fn test_forbidden_globals_nil_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let lua = build(mode);
            let leaked: bool = lua
                .load("return load ~= nil or print ~= nil or require ~= nil or dofile ~= nil")
                .eval()
                .unwrap();
            assert!(!leaked, "mode {mode:?} leaked a forbidden global");
        }
    }

    /// The safe `os` table (clock/time only) is present in both modes.
    #[test]
    fn test_safe_os_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let lua = build(mode);
            let t: i64 = lua.load("return os.time()").eval().unwrap();
            assert!(t > 0, "mode {mode:?}");
            let leaked: bool = lua.load("return os.execute ~= nil").eval().unwrap();
            assert!(!leaked, "mode {mode:?} leaked os.execute");
        }
    }

    /// cjson round-trips in both modes (historically missing at load time).
    #[test]
    fn test_cjson_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let lua = build(mode);
            let s: String = lua
                .load("return cjson.encode(cjson.decode('[1,2,3]'))")
                .eval()
                .unwrap();
            assert_eq!(s, "[1,2,3]", "mode {mode:?}");
        }
    }

    /// cmsgpack round-trips in both modes.
    #[test]
    fn test_cmsgpack_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let lua = build(mode);
            let n: i64 = lua
                .load("return cmsgpack.unpack(cmsgpack.pack(42))")
                .eval()
                .unwrap();
            assert_eq!(n, 42, "mode {mode:?}");
        }
    }

    /// Built-in protected globals (bit/cjson/cmsgpack) are readonly in both
    /// modes.
    #[test]
    fn test_builtin_tables_readonly_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            for code in ["bit.band = 1", "cjson.encode = 1", "cmsgpack.pack = 1"] {
                let err = exec_err(&build(mode), code);
                assert!(
                    err.contains("Attempt to modify a readonly table"),
                    "mode {mode:?} code {code:?}: {err}"
                );
            }
        }
    }

    /// setmetatable(_G) / getmetatable(_G) tricks are blocked identically.
    #[test]
    fn test_metatable_tricks_blocked_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let err = exec_err(&build(mode), "setmetatable(_G, {})");
            assert!(
                err.contains("Attempt to modify a readonly table"),
                "mode {mode:?}: {err}"
            );
            let err = exec_err(&build(mode), "local g = getmetatable(_G); g.__index = {}");
            assert!(
                err.contains("Attempt to modify a readonly table"),
                "mode {mode:?}: {err}"
            );
        }
    }

    /// register_protected_global makes the table visible and readonly.
    #[test]
    fn test_register_protected_global() {
        let lua = build(SandboxMode::Execute);
        let t = lua.create_table().unwrap();
        t.set("answer", 42).unwrap();
        register_protected_global(&lua, "mytable", t).unwrap();
        let v: i64 = lua.load("return mytable.answer").eval().unwrap();
        assert_eq!(v, 42);
        let err = exec_err(&lua, "mytable.answer = 1");
        assert!(err.contains("Attempt to modify a readonly table"));
    }

    /// Lua 5.1-compat unpack is available in both modes.
    #[test]
    fn test_unpack_compat_in_both_modes() {
        for mode in [SandboxMode::Load, SandboxMode::Execute] {
            let lua = build(mode);
            let v: i64 = lua.load("return unpack({7})").eval().unwrap();
            assert_eq!(v, 7, "mode {mode:?}");
        }
    }
}
