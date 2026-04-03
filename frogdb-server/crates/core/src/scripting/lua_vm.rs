//! Lua VM with sandboxing and resource limits.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use mlua::{Function, HookTriggers, Lua, Result as LuaResult, StdLib, Value, VmState};
use tracing::{debug, error};

use super::config::ScriptingConfig;
use super::error::ScriptError;
use crate::command::CommandContext;
use crate::registry::CommandRegistry;
use crate::shard::ShardSender;
use crate::store::Store;
use crate::sync::{MutexExt, RwLockExt};

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
            "setfenv",        // Lua 5.1 only, but be safe
            "getfenv",        // Lua 5.1 only, but be safe
            "module",         // Deprecated
            "require",        // No module loading
            "package",        // No package system
            "io",             // No I/O
            "os",             // No OS access (we'll add back safe functions)
            "debug",          // No debug library
            "collectgarbage", // No GC manipulation
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
        // Snapshot all current globals into a backing table, then clear _G so
        // reads go through __index and writes trigger __newindex (which errors).
        // rawset/rawget are kept available for this snippet but excluded from
        // the backing table so user scripts cannot access them.
        self.lua
            .load(
                r#"
local _rawset = rawset
local _real_G = {}
for k, v in pairs(_G) do _real_G[k] = v end
_real_G.rawset = nil
_real_G.rawget = nil
setmetatable(_G, {
    __newindex = function() error("Attempt to modify a readonly table") end,
    __index = _real_G,
    __metatable = "The metatable is locked",
})
local _keys = {}
for k in pairs(_G) do _keys[#_keys + 1] = k end
for _, k in ipairs(_keys) do _rawset(_G, k, nil) end
"#,
            )
            .exec()
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to apply global protection: {}", e))
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
        let globals = self.lua.globals();

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

        globals
            .raw_set("redis", redis_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis: {}", e)))?;

        // Add `bit` compatibility library (Redis uses LuaJIT's bit library;
        // Lua 5.4 has native operators but no `bit` table)
        self.setup_bit_library()?;

        Ok(())
    }

    /// Set up the `bit` compatibility library for Redis scripting compatibility.
    fn setup_bit_library(&self) -> Result<(), ScriptError> {
        // Create functions and table using the Lua API directly (bypasses global protection)
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

        // Use raw_set to bypass global protection
        self.lua
            .globals()
            .raw_set("bit", bit_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set bit global: {e}")))?;
        Ok(())
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
