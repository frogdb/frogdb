//! Lua VM with sandboxing and resource limits.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use mlua::{
    Function, HookTriggers, IntoLua, Lua, MultiValue, Result as LuaResult, StdLib, Table, Value,
    VmState,
};
use tracing::{debug, error};

use super::config::ScriptingConfig;
use super::error::ScriptError;
use crate::command::CommandContext;
use crate::registry::CommandRegistry;
use crate::shard::ShardSender;
use crate::store::Store;
use crate::sync::{MutexExt, RwLockExt};

/// Registry key for the backing globals table.
const REGISTRY_BACKING_TABLE: &str = "__frogdb_backing";
/// Registry key for the set of read-only protected tables.
const REGISTRY_PROTECTED_SET: &str = "__frogdb_protected";

/// Context for executing commands during script execution.
///
/// This struct holds raw pointers to the store and registry that are valid
/// for the duration of a single script execution. The pointers are set before
/// script execution begins and cleared immediately after.
pub struct CommandExecutionContext {
    /// Raw pointer to the store (valid for script duration).
    pub store_ptr: *mut dyn Store,
    /// Raw pointer to the command registry.
    pub registry_ptr: *const CommandRegistry,
    /// Shard message senders for cross-shard operations.
    pub shard_senders: Arc<Vec<ShardSender>>,
    /// This shard's ID.
    pub shard_id: usize,
    /// Total number of shards.
    pub num_shards: usize,
    /// Connection ID for this script execution.
    pub conn_id: u64,
    /// Protocol version for response encoding.
    pub protocol_version: ProtocolVersion,
}

// SAFETY: CommandExecutionContext is only accessed from a single shard thread
// during script execution. The raw pointers are valid for the duration of
// script execution, which is synchronous and single-threaded within a shard.
unsafe impl Send for CommandExecutionContext {}
unsafe impl Sync for CommandExecutionContext {}

/// Accessor handle for command execution context.
///
/// This is a clonable handle that can be moved into Lua closures to allow
/// them to execute Redis commands. It provides safe access to the underlying
/// command execution context.
#[derive(Clone)]
pub struct VmContextAccessor {
    /// Command execution context (set during script execution).
    cmd_ctx: Arc<RwLock<Option<CommandExecutionContext>>>,
    /// Execution state for tracking writes and other state.
    state: Arc<Mutex<ExecutionState>>,
}

impl VmContextAccessor {
    /// Execute a Redis command and return the response.
    ///
    /// # Safety
    /// This method must only be called during script execution when the
    /// command context pointers are valid.
    pub fn execute_command(&self, parts: &[Bytes]) -> Result<Response, String> {
        let ctx_guard = self
            .cmd_ctx
            .try_read_err()
            .map_err(|e| format!("ERR lock error: {}", e))?;
        let exec_ctx = ctx_guard
            .as_ref()
            .ok_or_else(|| "ERR command execution context not available".to_string())?;

        // SAFETY: These pointers are valid during script execution, which is
        // synchronous and single-threaded within a shard.
        let store = unsafe { &mut *exec_ctx.store_ptr };
        let registry = unsafe { &*exec_ctx.registry_ptr };

        // Get the command name
        if parts.is_empty() {
            return Err("ERR wrong number of arguments for redis command".to_string());
        }
        let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();

        // Look up the command handler
        let handler = registry
            .get(&cmd_name)
            .ok_or_else(|| format!("ERR unknown command '{}'", cmd_name))?;

        // Validate arity (args count excludes command name)
        let args = &parts[1..];
        if !handler.arity().check(args.len()) {
            return Err(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name().to_ascii_lowercase()
            ));
        }

        // Create command context
        let mut ctx = CommandContext::new(
            store,
            &exec_ctx.shard_senders,
            exec_ctx.shard_id,
            exec_ctx.num_shards,
            exec_ctx.conn_id,
            exec_ctx.protocol_version,
        );

        // Execute the command
        match handler.execute(&mut ctx, args) {
            Ok(response) => Ok(response),
            Err(err) => Err(err.to_string()),
        }
    }

    /// Mark the script as having performed a write operation.
    pub fn mark_write(&self) {
        self.state
            .lock_or_panic("VmContextAccessor::mark_write")
            .has_writes = true;
    }
}

/// Execution state for tracking script execution.
#[derive(Debug, Default)]
pub struct ExecutionState {
    /// Script start time.
    pub start_time: Option<Instant>,
    /// Timeout limit in milliseconds.
    pub timeout_ms: u64,
    /// Grace period in milliseconds.
    pub grace_ms: u64,
    /// Whether the script has been marked for soft stop.
    pub soft_stop: bool,
    /// Whether the script has performed any writes.
    pub has_writes: bool,
    /// Declared keys for strict validation.
    pub declared_keys: Vec<Bytes>,
}

impl ExecutionState {
    /// Check if the script has timed out.
    #[allow(dead_code)]
    pub fn is_timed_out(&self) -> bool {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_millis() as u64;
            elapsed > self.timeout_ms
        } else {
            false
        }
    }

    /// Check if the script has exceeded the grace period.
    #[allow(dead_code)]
    pub fn is_grace_exceeded(&self) -> bool {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_millis() as u64;
            elapsed > self.timeout_ms + self.grace_ms
        } else {
            false
        }
    }

    /// Reset for a new execution.
    pub fn reset(&mut self) {
        self.start_time = None;
        self.soft_stop = false;
        self.has_writes = false;
        self.declared_keys.clear();
    }
}

/// Per-shard Lua VM with sandboxing.
pub struct LuaVm {
    /// The Lua state.
    lua: Lua,
    /// Configuration.
    config: ScriptingConfig,
    /// Execution state (thread-safe).
    state: Arc<Mutex<ExecutionState>>,
    /// Kill flag (thread-safe for use in hooks).
    kill_flag: Arc<AtomicBool>,
    /// Command execution context (set during script execution).
    cmd_ctx: Arc<RwLock<Option<CommandExecutionContext>>>,
}

impl LuaVm {
    /// Create a new Lua VM with sandboxing.
    pub fn new(config: ScriptingConfig) -> Result<Self, ScriptError> {
        // Create Lua with minimal standard libraries
        let libs = StdLib::COROUTINE | StdLib::TABLE | StdLib::STRING | StdLib::MATH;

        let lua = unsafe { Lua::unsafe_new_with(libs, mlua::LuaOptions::default()) };

        // Set memory limit if configured
        if config.lua_heap_limit_mb > 0 {
            let limit_bytes = config.lua_heap_limit_mb * 1024 * 1024;
            lua.set_memory_limit(limit_bytes)
                .map_err(|e| ScriptError::Internal(format!("Failed to set memory limit: {}", e)))?;
        }

        let state = Arc::new(Mutex::new(ExecutionState::default()));
        let kill_flag = Arc::new(AtomicBool::new(false));
        let cmd_ctx = Arc::new(RwLock::new(None));

        debug!(
            heap_limit_mb = config.lua_heap_limit_mb,
            time_limit_ms = config.lua_time_limit_ms,
            "Lua VM initialized"
        );

        let vm = Self {
            lua,
            config,
            state,
            kill_flag,
            cmd_ctx,
        };

        // Apply sandbox restrictions
        vm.apply_sandbox().map_err(|e| {
            error!(error = %e, "Failed to initialize Lua VM");
            e
        })?;

        Ok(vm)
    }

    /// Apply sandbox restrictions to the Lua environment.
    fn apply_sandbox(&self) -> Result<(), ScriptError> {
        let globals = self.lua.globals();

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
        let os_table = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create os table: {}", e)))?;

        let clock_fn = self
            .lua
            .create_function(|_, ()| -> LuaResult<f64> {
                Ok(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0))
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create clock fn: {}", e)))?;

        let time_fn = self
            .lua
            .create_function(|_, ()| -> LuaResult<i64> {
                Ok(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0))
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create time fn: {}", e)))?;

        os_table
            .set("clock", clock_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set os.clock: {}", e)))?;
        os_table
            .set("time", time_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set os.time: {}", e)))?;
        globals
            .set("os", os_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set os: {}", e)))?;

        // Protect _G with a readonly metatable.
        //
        // This emulates Redis's scriptingEnableGlobalsProtection():
        // - Snapshot all current globals into a backing table (_real_G), then
        //   clear _G so reads go through __index and writes trigger __newindex.
        // - __index errors on nonexistent globals (like Redis's
        //   luaProtectedTableError).
        // - __newindex always errors ("Attempt to modify a readonly table").
        // - __metatable = "The metatable is locked" prevents setmetatable(_G)
        //   and makes getmetatable(_G) return the string.
        // - setmetatable/getmetatable are replaced with wrappers that enforce
        //   readonly semantics for protected tables (emulates Redis's
        //   lua_enablereadonlytable which is a C-level feature not available
        //   in standard Lua 5.4).
        // - rawset/rawget are removed from user-visible globals.
        // - The backing table and protected-set are stored as hidden raw keys
        //   in _G (inaccessible to user code since __index checks _real_G).
        // Build a table of explicitly-forbidden global names so the __index
        // handler can distinguish them from truly-undeclared variables.
        // Forbidden globals return nil; undeclared globals error.
        {
            let forbidden_table = self.lua.create_table().map_err(|e| {
                ScriptError::Internal(format!("Failed to create forbidden table: {}", e))
            })?;
            for name in &forbidden {
                forbidden_table.set(*name, true).map_err(|e| {
                    ScriptError::Internal(format!("Failed to add forbidden name: {}", e))
                })?;
            }
            self.lua
                .globals()
                .raw_set("__frogdb_forbidden", forbidden_table)
                .map_err(|e| {
                    ScriptError::Internal(format!("Failed to set forbidden table: {}", e))
                })?;
        }

        self.lua
            .load(
                r#"
local _rawset = rawset
local _rawget = rawget
local _setmetatable = setmetatable
local _getmetatable = getmetatable

-- Retrieve the forbidden-names table set by Rust
local _forbidden = _rawget(_G, "__frogdb_forbidden") or {}
_rawset(_G, "__frogdb_forbidden", nil)

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
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to apply global protection: {}", e))
            })?;

        Ok(())
    }

    /// Register a table as a global in the backing table and protect it from
    /// writes. This must be used instead of `globals().raw_set()` for tables
    /// that should be visible as globals AND readonly (e.g., `redis`, `bit`,
    /// `cjson`).
    fn register_protected_global(&self, name: &str, table: Table) -> Result<(), ScriptError> {
        let globals = self.lua.globals();

        // Get the backing table and protected set from hidden raw keys
        let backing: Table = globals
            .raw_get(REGISTRY_BACKING_TABLE)
            .map_err(|e| ScriptError::Internal(format!("Failed to get backing table: {}", e)))?;
        let protected: Table = globals
            .raw_get(REGISTRY_PROTECTED_SET)
            .map_err(|e| ScriptError::Internal(format!("Failed to get protected set: {}", e)))?;

        // Protect the table itself with a readonly metatable.
        // We use the real setmetatable via Lua code. The sandbox's wrapped
        // setmetatable would also work since the table is not yet protected.
        self.lua
            .load(
                r#"
local t, name = ...
-- Copy contents to a backing table, set readonly metatable
local backing = {}
for k, v in pairs(t) do backing[k] = v end
-- Clear original keys (table has no metatable yet, so direct set works)
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
"#,
            )
            .call::<()>((table.clone(), name))
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to protect {} table: {}", name, e))
            })?;

        // Mark as protected
        protected.set(table.clone(), true).map_err(|e| {
            ScriptError::Internal(format!("Failed to mark {} as protected: {}", name, e))
        })?;

        // Add to backing table so it's accessible via _G.__index
        backing.set(name, table).map_err(|e| {
            ScriptError::Internal(format!("Failed to register {} global: {}", name, e))
        })?;

        Ok(())
    }

    /// Set up hook for timeout checking.
    fn setup_timeout_hook(&self, start_time: Instant) {
        let timeout_ms = self.config.effective_lua_time_limit_ms();
        let grace_ms = self.config.lua_timeout_grace_ms;
        let kill_flag = self.kill_flag.clone();

        if timeout_ms > 0 {
            // Check every 10000 instructions for timeout
            let triggers = HookTriggers::new().every_nth_instruction(10000);

            let _ = self.lua.set_hook(triggers, move |_lua, _debug| {
                // Check kill flag
                if kill_flag.load(Ordering::Relaxed) {
                    return Err(mlua::Error::RuntimeError("Script killed".to_string()));
                }

                // Check timeout
                let elapsed = start_time.elapsed().as_millis() as u64;
                if elapsed > timeout_ms + grace_ms {
                    return Err(mlua::Error::RuntimeError(format!(
                        "BUSY script running for {} ms",
                        elapsed
                    )));
                }

                Ok(VmState::Continue)
            });
        }
    }

    /// Remove the timeout hook.
    fn remove_timeout_hook(&self) {
        self.lua.remove_hook();
    }

    /// Prepare the VM for a new script execution.
    pub fn prepare_execution(&self, keys: &[Bytes], argv: &[Bytes]) -> Result<(), ScriptError> {
        let start_time = Instant::now();

        // Reset execution state
        {
            let mut state = self.state.try_lock_err()?;
            state.reset();
            state.start_time = Some(start_time);
            state.timeout_ms = self.config.effective_lua_time_limit_ms();
            state.grace_ms = self.config.lua_timeout_grace_ms;
            state.declared_keys = keys.to_vec();
        }

        // Reset kill flag
        self.kill_flag.store(false, Ordering::Relaxed);

        // Set up timeout hook
        self.setup_timeout_hook(start_time);

        // Set KEYS and ARGV globals
        let globals = self.lua.globals();

        // Create KEYS table
        let keys_table = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create KEYS table: {}", e)))?;
        for (i, key) in keys.iter().enumerate() {
            let lua_str = self.lua.create_string(key.as_ref()).map_err(|e| {
                ScriptError::Internal(format!("Failed to create KEYS[{}] string: {}", i + 1, e))
            })?;
            keys_table.set(i + 1, lua_str).map_err(|e| {
                ScriptError::Internal(format!("Failed to set KEYS[{}]: {}", i + 1, e))
            })?;
        }
        globals
            .raw_set("KEYS", keys_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set KEYS: {}", e)))?;

        // Create ARGV table
        let argv_table = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create ARGV table: {}", e)))?;
        for (i, arg) in argv.iter().enumerate() {
            let lua_str = self.lua.create_string(arg.as_ref()).map_err(|e| {
                ScriptError::Internal(format!("Failed to create ARGV[{}] string: {}", i + 1, e))
            })?;
            argv_table.set(i + 1, lua_str).map_err(|e| {
                ScriptError::Internal(format!("Failed to set ARGV[{}]: {}", i + 1, e))
            })?;
        }
        globals
            .raw_set("ARGV", argv_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set ARGV: {}", e)))?;

        Ok(())
    }

    /// Clean up after script execution.
    pub fn cleanup_execution(&self) {
        self.remove_timeout_hook();

        // Clear KEYS and ARGV (use raw_set to bypass readonly metatable)
        let globals = self.lua.globals();
        let _ = globals.raw_set("KEYS", Value::Nil);
        let _ = globals.raw_set("ARGV", Value::Nil);

        // Reset state
        self.state.lock_or_panic("LuaVm::cleanup_execution").reset();
    }

    /// Execute a Lua script.
    pub fn execute(&self, source: &[u8]) -> Result<Value, ScriptError> {
        // Check if already killed
        if self.kill_flag.load(Ordering::Relaxed) {
            return Err(ScriptError::Killed);
        }

        // Compile the script
        let chunk = self.lua.load(source);

        // Execute
        // Note: error messages are stripped to the first line because RESP simple
        // errors must not contain newlines. Lua stack traces would otherwise
        // corrupt the protocol stream.
        let result = chunk.eval::<Value>().map_err(|e| match e {
            mlua::Error::MemoryError(_) => ScriptError::MemoryLimitExceeded,
            mlua::Error::SyntaxError { message, .. } => ScriptError::Compilation(message),
            mlua::Error::RuntimeError(msg) => {
                if msg.contains("BUSY") {
                    ScriptError::Timeout {
                        timeout_ms: self.config.effective_lua_time_limit_ms(),
                    }
                } else if msg.contains("killed") {
                    ScriptError::Killed
                } else {
                    let first_line = msg.lines().next().unwrap_or(&msg).to_string();
                    ScriptError::Runtime(first_line)
                }
            }
            _ => {
                let msg = e.to_string();
                let first_line = msg.lines().next().unwrap_or(&msg).to_string();
                ScriptError::Runtime(first_line)
            }
        })?;

        Ok(result)
    }

    /// Get the Lua state for setting up bindings.
    pub fn lua(&self) -> &Lua {
        &self.lua
    }

    /// Get the execution state.
    pub fn state(&self) -> &Arc<Mutex<ExecutionState>> {
        &self.state
    }

    /// Set the command execution context for the duration of script execution.
    pub fn set_command_context(&self, ctx: CommandExecutionContext) {
        *self.cmd_ctx.write_or_panic("LuaVm::set_command_context") = Some(ctx);
    }

    /// Clear the command execution context after script execution.
    pub fn clear_command_context(&self) {
        *self.cmd_ctx.write_or_panic("LuaVm::clear_command_context") = None;
    }

    /// Create a context accessor that can be used in Lua closures.
    pub fn create_context_accessor(&self) -> VmContextAccessor {
        VmContextAccessor {
            cmd_ctx: self.cmd_ctx.clone(),
            state: self.state.clone(),
        }
    }

    /// Mark the script as having performed writes.
    pub fn mark_write(&self) {
        self.state.lock_or_panic("LuaVm::mark_write").has_writes = true;
    }

    /// Check if the script has performed writes.
    pub fn has_writes(&self) -> bool {
        self.state.lock_or_panic("LuaVm::has_writes").has_writes
    }

    /// Request the script to be killed.
    pub fn request_kill(&self) -> Result<(), ScriptError> {
        let state = self.state.try_lock_err()?;
        if state.has_writes {
            Err(ScriptError::Unkillable)
        } else {
            self.kill_flag.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Check if the script is currently running.
    pub fn is_running(&self) -> bool {
        self.state
            .lock_or_panic("LuaVm::is_running")
            .start_time
            .is_some()
    }

    /// Set up the redis.call and redis.pcall functions.
    pub fn set_redis_functions(
        &self,
        call_fn: Function,
        pcall_fn: Function,
    ) -> Result<(), ScriptError> {
        let redis_table = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create redis table: {}", e)))?;

        redis_table
            .set("call", call_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.call: {}", e)))?;

        redis_table
            .set("pcall", pcall_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.pcall: {}", e)))?;

        // Add redis.log function
        let log_fn = self
            .lua
            .create_function(|_, (level, message): (i32, String)| {
                match level {
                    0 => tracing::debug!(target: "lua_script", "{}", message),
                    1 => tracing::trace!(target: "lua_script", "{}", message),
                    2 => tracing::info!(target: "lua_script", "{}", message),
                    3 => tracing::warn!(target: "lua_script", "{}", message),
                    _ => tracing::info!(target: "lua_script", "{}", message),
                }
                Ok(())
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create log fn: {}", e)))?;

        redis_table
            .set("log", log_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.log: {}", e)))?;

        // Add log level constants
        redis_table
            .set("LOG_DEBUG", 0i32)
            .map_err(|e| ScriptError::Internal(format!("Failed to set LOG_DEBUG: {}", e)))?;
        redis_table
            .set("LOG_VERBOSE", 1i32)
            .map_err(|e| ScriptError::Internal(format!("Failed to set LOG_VERBOSE: {}", e)))?;
        redis_table
            .set("LOG_NOTICE", 2i32)
            .map_err(|e| ScriptError::Internal(format!("Failed to set LOG_NOTICE: {}", e)))?;
        redis_table
            .set("LOG_WARNING", 3i32)
            .map_err(|e| ScriptError::Internal(format!("Failed to set LOG_WARNING: {}", e)))?;

        // Add Redis version constants (for compatibility with Redis scripting API)
        // Version 7.2.0 = (7 << 16) | (2 << 8) | 0 = 0x070200
        redis_table
            .set("REDIS_VERSION", "7.2.0")
            .map_err(|e| ScriptError::Internal(format!("Failed to set REDIS_VERSION: {}", e)))?;
        redis_table
            .set("REDIS_VERSION_NUM", 0x0007_0200i64)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to set REDIS_VERSION_NUM: {}", e))
            })?;

        // Register as a protected global (adds to backing table and sets
        // readonly metatable so `redis.call = ...` errors)
        self.register_protected_global("redis", redis_table)?;

        // Add `bit` compatibility library (Redis uses LuaJIT's bit library;
        // Lua 5.4 has native operators but no `bit` table)
        self.setup_bit_library()?;

        // Add cjson and cmsgpack libraries for Redis scripting compatibility
        self.setup_cjson_library()?;
        self.setup_cmsgpack_library()?;

        Ok(())
    }

    /// Set up the `bit` compatibility library for Redis scripting compatibility.
    fn setup_bit_library(&self) -> Result<(), ScriptError> {
        let bit_table = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit table: {e}")))?;

        // Use Lua's native 5.4 bitwise operators via small closures
        let band = self
            .lua
            .create_function(|_, (a, b): (i64, i64)| Ok(a & b))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.band: {e}")))?;
        let bor = self
            .lua
            .create_function(|_, (a, b): (i64, i64)| Ok(a | b))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.bor: {e}")))?;
        let bxor = self
            .lua
            .create_function(|_, (a, b): (i64, i64)| Ok(a ^ b))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.bxor: {e}")))?;
        let bnot = self
            .lua
            .create_function(|_, a: i64| Ok(!a))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.bnot: {e}")))?;
        let rshift = self
            .lua
            .create_function(|_, (a, n): (i64, u32)| Ok(((a as u64) >> n) as i64))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.rshift: {e}")))?;
        let lshift = self
            .lua
            .create_function(|_, (a, n): (i64, u32)| Ok(((a as u64) << n) as i64))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.lshift: {e}")))?;
        let tobit = self
            .lua
            .create_function(|_, a: i64| Ok(a as i32 as i64))
            .map_err(|e| ScriptError::Internal(format!("Failed to create bit.tobit: {e}")))?;

        bit_table.set("band", band).unwrap();
        bit_table.set("bor", bor).unwrap();
        bit_table.set("bxor", bxor).unwrap();
        bit_table.set("bnot", bnot).unwrap();
        bit_table.set("rshift", rshift).unwrap();
        bit_table.set("lshift", lshift).unwrap();
        bit_table.set("tobit", tobit).unwrap();

        // Register as a protected global (adds to backing table and sets
        // readonly metatable so `bit.lshift = ...` errors)
        self.register_protected_global("bit", bit_table)?;

        Ok(())
    }

    /// Set up the `cjson` library for Redis scripting compatibility.
    ///
    /// Provides `cjson.encode`, `cjson.decode`, `cjson.null`,
    /// `cjson.decode_array_with_array_mt`, and `cjson.decode_invalid_numbers`.
    fn setup_cjson_library(&self) -> Result<(), ScriptError> {
        let cjson = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create cjson table: {e}")))?;

        // --- cjson.null sentinel ---
        let null_ud = self
            .lua
            .create_any_userdata(CjsonNull)
            .map_err(|e| ScriptError::Internal(format!("Failed to create cjson.null: {e}")))?;
        // Store in registry so closures can retrieve independent copies.
        let null_key = self
            .lua
            .create_registry_value(null_ud.clone())
            .map_err(|e| ScriptError::Internal(format!("Failed to register cjson.null: {e}")))?;

        cjson
            .set("null", null_ud)
            .map_err(|e| ScriptError::Internal(format!("Failed to set cjson.null: {e}")))?;

        // --- array metatable (shared, read-only) ---
        let array_mt = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create array mt: {e}")))?;
        let is_array_fn = self.lua.create_function(|_, _: ()| Ok(true)).map_err(|e| {
            ScriptError::Internal(format!("Failed to create __is_cjson_array: {e}"))
        })?;
        array_mt.set("__is_cjson_array", is_array_fn).unwrap();
        // Mark the array metatable as protected so the sandbox's getmetatable
        // wrapper returns a frozen proxy (prevents scripts from modifying it).
        {
            let globals = self.lua.globals();
            let protected: Table = globals.raw_get(REGISTRY_PROTECTED_SET).map_err(|e| {
                ScriptError::Internal(format!("Failed to get protected set for array mt: {e}"))
            })?;
            protected.set(array_mt.clone(), true).map_err(|e| {
                ScriptError::Internal(format!("Failed to mark array mt as protected: {e}"))
            })?;
        }

        let array_mt_key = self
            .lua
            .create_registry_value(array_mt)
            .map_err(|e| ScriptError::Internal(format!("Failed to register array mt: {e}")))?;

        // --- per-VM flag: use array metatable on decoded arrays ---
        let use_array_mt = Arc::new(AtomicBool::new(false));

        // --- cjson.encode ---
        let null_key_enc = self
            .lua
            .create_registry_value(self.lua.registry_value::<Value>(&null_key).map_err(|e| {
                ScriptError::Internal(format!("Failed to get cjson.null for encode: {e}"))
            })?)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to register cjson.null for encode: {e}"))
            })?;
        let array_mt_key_enc = self
            .lua
            .create_registry_value(self.lua.registry_value::<Value>(&array_mt_key).map_err(
                |e| ScriptError::Internal(format!("Failed to get array mt for encode: {e}")),
            )?)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to register array mt for encode: {e}"))
            })?;

        let encode_fn = self
            .lua
            .create_function(move |lua, val: Value| -> LuaResult<String> {
                let null_val: Value = lua.registry_value(&null_key_enc)?;
                let amt_val: Value = lua.registry_value(&array_mt_key_enc)?;
                let json = lua_to_json(&val, &null_val, &amt_val)?;
                let s = serde_json::to_string(&json)
                    .map_err(|e| mlua::Error::RuntimeError(format!("cjson.encode: {e}")))?;
                Ok(s)
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create cjson.encode: {e}")))?;

        cjson
            .set("encode", encode_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set cjson.encode: {e}")))?;

        // --- cjson.decode ---
        let null_key_dec = self
            .lua
            .create_registry_value(self.lua.registry_value::<Value>(&null_key).map_err(|e| {
                ScriptError::Internal(format!("Failed to get cjson.null for decode: {e}"))
            })?)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to register cjson.null for decode: {e}"))
            })?;
        let array_mt_key_dec = self
            .lua
            .create_registry_value(self.lua.registry_value::<Value>(&array_mt_key).map_err(
                |e| ScriptError::Internal(format!("Failed to get array mt for decode: {e}")),
            )?)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to register array mt for decode: {e}"))
            })?;
        let use_array_mt_dec = use_array_mt.clone();

        let decode_fn = self
            .lua
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
            .map_err(|e| ScriptError::Internal(format!("Failed to create cjson.decode: {e}")))?;

        cjson
            .set("decode", decode_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set cjson.decode: {e}")))?;

        // --- cjson.decode_array_with_array_mt(bool) ---
        let use_array_mt_setter = use_array_mt.clone();
        let damt_fn = self
            .lua
            .create_function(move |_, enable: bool| -> LuaResult<()> {
                use_array_mt_setter.store(enable, Ordering::Relaxed);
                Ok(())
            })
            .map_err(|e| {
                ScriptError::Internal(format!(
                    "Failed to create cjson.decode_array_with_array_mt: {e}"
                ))
            })?;
        cjson
            .set("decode_array_with_array_mt", damt_fn)
            .map_err(|e| {
                ScriptError::Internal(format!(
                    "Failed to set cjson.decode_array_with_array_mt: {e}"
                ))
            })?;

        // --- cjson.decode_invalid_numbers(bool) --- no-op stub
        let din_fn = self
            .lua
            .create_function(|_, _: bool| -> LuaResult<()> { Ok(()) })
            .map_err(|e| {
                ScriptError::Internal(format!(
                    "Failed to create cjson.decode_invalid_numbers: {e}"
                ))
            })?;
        cjson.set("decode_invalid_numbers", din_fn).map_err(|e| {
            ScriptError::Internal(format!("Failed to set cjson.decode_invalid_numbers: {e}"))
        })?;

        self.register_protected_global("cjson", cjson)?;
        Ok(())
    }

    /// Set up the `cmsgpack` library for Redis scripting compatibility.
    ///
    /// Provides `cmsgpack.pack` and `cmsgpack.unpack` using the `rmpv` crate.
    fn setup_cmsgpack_library(&self) -> Result<(), ScriptError> {
        let cmsgpack = self
            .lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create cmsgpack table: {e}")))?;

        // --- cmsgpack.pack ---
        let pack_fn = self
            .lua
            .create_function(|_, val: Value| -> LuaResult<Vec<u8>> {
                let msgpack_val = lua_to_msgpack(&val)?;
                let mut buf = Vec::new();
                rmpv::encode::write_value(&mut buf, &msgpack_val)
                    .map_err(|e| mlua::Error::RuntimeError(format!("cmsgpack.pack: {e}")))?;
                Ok(buf)
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create cmsgpack.pack: {e}")))?;

        cmsgpack
            .set("pack", pack_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set cmsgpack.pack: {e}")))?;

        // --- cmsgpack.unpack ---
        let unpack_fn = self
            .lua
            .create_function(|lua, data: mlua::String| -> LuaResult<Value> {
                let bytes = data.as_bytes();
                let mut cursor = std::io::Cursor::new(bytes);
                let msgpack_val = rmpv::decode::read_value(&mut cursor)
                    .map_err(|e| mlua::Error::RuntimeError(format!("cmsgpack.unpack: {e}")))?;
                msgpack_to_lua(lua, &msgpack_val)
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create cmsgpack.unpack: {e}")))?;

        cmsgpack
            .set("unpack", unpack_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set cmsgpack.unpack: {e}")))?;

        self.register_protected_global("cmsgpack", cmsgpack)?;
        Ok(())
    }
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

    #[test]
    fn test_vm_creation() {
        let config = ScriptingConfig::default();
        let vm = LuaVm::new(config).unwrap();
        assert!(!vm.is_running());
    }

    #[test]
    fn test_simple_script() {
        let config = ScriptingConfig::default();
        let vm = LuaVm::new(config).unwrap();

        vm.prepare_execution(&[], &[]).unwrap();
        let result = vm.execute(b"return 42").unwrap();
        vm.cleanup_execution();

        match result {
            Value::Integer(n) => assert_eq!(n, 42),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_keys_and_argv() {
        let config = ScriptingConfig::default();
        let vm = LuaVm::new(config).unwrap();

        let keys = vec![Bytes::from_static(b"key1"), Bytes::from_static(b"key2")];
        let argv = vec![Bytes::from_static(b"arg1")];

        vm.prepare_execution(&keys, &argv).unwrap();
        let result = vm.execute(b"return #KEYS").unwrap();
        vm.cleanup_execution();

        match result {
            Value::Integer(n) => assert_eq!(n, 2),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_syntax_error() {
        let config = ScriptingConfig::default();
        let vm = LuaVm::new(config).unwrap();

        vm.prepare_execution(&[], &[]).unwrap();
        let result = vm.execute(b"this is not valid lua");
        vm.cleanup_execution();

        match result {
            Err(ScriptError::Compilation(_)) => {}
            _ => panic!("Expected compilation error"),
        }
    }

    #[test]
    fn test_kill_request() {
        let config = ScriptingConfig::default();
        let vm = LuaVm::new(config).unwrap();

        // Should succeed when no writes
        vm.request_kill().unwrap();

        // Mark write
        vm.mark_write();

        // Should fail now
        assert!(matches!(vm.request_kill(), Err(ScriptError::Unkillable)));
    }
}
