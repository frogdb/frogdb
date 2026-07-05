//! Lua VM with sandboxing and resource limits.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::ProtocolVersion;
use frogdb_scripting::sandbox::{
    self, REDIS_VERSION, REDIS_VERSION_NUM, SandboxMode, SandboxOptions, TimeoutContext,
    TimeoutHook, build_frogdb_lua_vm,
};
use mlua::{Function, Lua, Result as LuaResult, Value};
use sha1::{Digest, Sha1};
use tracing::{debug, error};

use super::config::ScriptingConfig;
use super::error::ScriptError;
use super::gate::{CrossSlotTracker, ScriptCommandGate};
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
    /// Whether the shard running this script belongs to a replica server.
    ///
    /// Threaded through from the outer command context so that `redis.call`
    /// sub-commands (e.g. ROLE, INFO replication) report the correct role — a
    /// script on a replica must not observe itself as a primary.
    pub is_replica: bool,
    /// Shared replica-status flag (same `Arc<AtomicBool>` used server-wide).
    pub is_replica_flag: Option<Arc<AtomicBool>>,
    /// Primary host, when the shard belongs to a replica server.
    pub master_host: Option<String>,
    /// Primary port, when the shard belongs to a replica server.
    pub master_port: Option<u16>,
}

// SAFETY: CommandExecutionContext is only accessed from a single shard thread
// during script execution. The raw pointers are valid for the duration of
// script execution, which is synchronous and single-threaded within a shard.
unsafe impl Send for CommandExecutionContext {}
unsafe impl Sync for CommandExecutionContext {}

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
    ///
    /// The sandbox (stdlib set, forbidden globals, global protection,
    /// `bit`/`cjson`/`cmsgpack`, memory limit) is built by the shared
    /// [`build_frogdb_lua_vm`] constructor — the same one the FUNCTION LOAD
    /// capture VM uses — so load-time and execute-time semantics cannot
    /// diverge. Only the live `redis` table ([`Self::set_redis_functions`])
    /// and the per-execution timeout hook are added here.
    pub fn new(config: ScriptingConfig) -> Result<Self, ScriptError> {
        let lua = build_frogdb_lua_vm(&SandboxOptions {
            mode: SandboxMode::Execute,
            memory_limit_bytes: config.lua_heap_limit_mb * 1024 * 1024,
        })
        .map_err(|e| {
            error!(error = %e, "Failed to initialize Lua VM");
            ScriptError::Internal(e.to_string())
        })?;

        let state = Arc::new(Mutex::new(ExecutionState::default()));
        let kill_flag = Arc::new(AtomicBool::new(false));
        let cmd_ctx = Arc::new(RwLock::new(None));

        debug!(
            heap_limit_mb = config.lua_heap_limit_mb,
            time_limit_ms = config.lua_time_limit_ms,
            "Lua VM initialized"
        );

        Ok(Self {
            lua,
            config,
            state,
            kill_flag,
            cmd_ctx,
        })
    }

    /// Set up hook for timeout checking (shared sandbox hook + kill flag).
    fn setup_timeout_hook(&self, start_time: Instant) {
        sandbox::install_timeout_hook(
            &self.lua,
            start_time,
            TimeoutHook {
                timeout_ms: self.config.effective_lua_time_limit_ms(),
                grace_ms: self.config.lua_timeout_grace_ms,
                kill_flag: Some(self.kill_flag.clone()),
                context: TimeoutContext::ScriptExecution,
            },
        );
    }

    /// Remove the timeout hook.
    fn remove_timeout_hook(&self) {
        sandbox::remove_timeout_hook(&self.lua);
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
    ///
    /// Scripts are wrapped in xpcall so that table-based error objects (e.g.
    /// `error({err="ERR msg"})`) are properly handled.  When the error object
    /// is a table with an `err` field the message is extracted and propagated;
    /// otherwise `tostring()` is used (matching Redis behaviour).
    pub fn execute(&self, source: &[u8]) -> Result<Value, ScriptError> {
        // Check if already killed
        if self.kill_flag.load(Ordering::Relaxed) {
            return Err(ScriptError::Killed);
        }

        // First compile the script to catch syntax errors early.
        let chunk = self.lua.load(source);
        let func: Function = chunk.into_function().map_err(|e| match e {
            mlua::Error::SyntaxError { message, .. } => ScriptError::Compilation(message),
            _ => {
                let msg = e.to_string();
                ScriptError::Compilation(msg.lines().next().unwrap_or(&msg).to_string())
            }
        })?;

        // Wrap execution in xpcall with a message handler that extracts table
        // errors.  The handler checks if the error object is a table with an
        // `err` field and converts it to a tagged string so we can recover it
        // on the Rust side.
        let xpcall_wrapper: Function = self
            .lua
            .load(
                r#"
local fn = ...
local TAG = "__frogdb_table_err__:"
local handler = function(obj)
    if type(obj) == "table" and type(obj.err) == "string" then
        return TAG .. obj.err
    end
    return tostring(obj)
end
local ok, res = xpcall(fn, handler)
if ok then
    return res
else
    error(res, 0)
end
"#,
            )
            .into_function()
            .map_err(|e| ScriptError::Internal(format!("Failed to create xpcall wrapper: {e}")))?;

        // Execute the wrapper with the compiled function as argument.
        // Note: error messages are stripped to the first line because RESP simple
        // errors must not contain newlines. Lua stack traces would otherwise
        // corrupt the protocol stream.
        let result = xpcall_wrapper
            .call::<Value>(func)
            .map_err(|e| self.classify_lua_error(e))?;

        Ok(result)
    }

    /// Classify a Lua error into a ScriptError.
    ///
    /// Handles:
    /// - Table-based errors from our xpcall handler (prefixed with `__frogdb_table_err__:`)
    /// - Standard runtime errors (BUSY, killed, etc.)
    /// - Memory and syntax errors
    fn classify_lua_error(&self, e: mlua::Error) -> ScriptError {
        // Try to extract our xpcall handler's tagged error message.
        if let Some(msg) = Self::extract_tagged_table_error(&e) {
            let first_line = msg.lines().next().unwrap_or(&msg).to_string();
            return ScriptError::Runtime(first_line);
        }

        match e {
            mlua::Error::MemoryError(_) => ScriptError::MemoryLimitExceeded,
            mlua::Error::SyntaxError { message, .. } => ScriptError::Compilation(message),
            mlua::Error::RuntimeError(msg) => self.classify_runtime_msg(&msg),
            mlua::Error::CallbackError { cause, .. } => {
                self.classify_lua_error(cause.as_ref().clone())
            }
            _ => {
                let msg = e.to_string();
                let first_line = msg.lines().next().unwrap_or(&msg).to_string();
                ScriptError::Runtime(first_line)
            }
        }
    }

    /// Tag prefix used by the xpcall handler to mark table-based errors.
    const TABLE_ERR_TAG: &'static str = "__frogdb_table_err__:";

    /// Classify a RuntimeError message string.
    fn classify_runtime_msg(&self, msg: &str) -> ScriptError {
        // Check for our xpcall tag first
        if let Some(stripped) = msg.strip_prefix(Self::TABLE_ERR_TAG) {
            let first_line = stripped.lines().next().unwrap_or(stripped).to_string();
            return ScriptError::Runtime(first_line);
        }

        // Strip Lua source location prefixes that mlua/Lua add to error
        // messages (e.g. `[string "..."]:3: WRONGTYPE ...` or `runtime error: WRONGTYPE ...`).
        // This reveals the original error message for prefix matching.
        let cleaned = Self::strip_lua_error_prefix(msg);

        if cleaned.contains("BUSY") {
            ScriptError::Timeout {
                timeout_ms: self.config.effective_lua_time_limit_ms(),
            }
        } else if cleaned.contains("killed") {
            ScriptError::Killed
        } else {
            let first_line = cleaned.lines().next().unwrap_or(cleaned).to_string();
            ScriptError::Runtime(first_line)
        }
    }

    /// Strip Lua error location/runtime prefixes from an error message.
    ///
    /// Lua errors from `error()` or from mlua RuntimeError come with prefixes
    /// like `[string "..."]:N: ` or `runtime error: `.  We strip these to
    /// recover the original error message (e.g. "WRONGTYPE ...").
    fn strip_lua_error_prefix(msg: &str) -> &str {
        let s = msg.strip_prefix("runtime error: ").unwrap_or(msg);
        // Strip [string "..."]:N: prefix
        if s.starts_with("[string ")
            && let Some(pos) = s.find("]: ")
        {
            let after_bracket = &s[pos + 3..];
            // There may be a line number followed by ": "
            if let Some(colon_pos) = after_bracket.find(": ")
                && after_bracket[..colon_pos]
                    .chars()
                    .all(|c| c.is_ascii_digit())
            {
                return &after_bracket[colon_pos + 2..];
            }
            return after_bracket;
        }
        s
    }

    /// Walk the mlua error chain looking for our xpcall handler's tagged string.
    ///
    /// The xpcall handler converts table errors with `err` fields into strings
    /// prefixed with `__frogdb_table_err__:`.  We check RuntimeError messages
    /// for this prefix and also recurse through CallbackError chains.
    fn extract_tagged_table_error(err: &mlua::Error) -> Option<String> {
        match err {
            mlua::Error::CallbackError { cause, .. } => Self::extract_tagged_table_error(cause),
            mlua::Error::RuntimeError(msg) => {
                // The tag may appear directly or after a Lua location prefix
                let cleaned = Self::strip_lua_error_prefix(msg);
                cleaned
                    .strip_prefix(Self::TABLE_ERR_TAG)
                    .map(|rest| rest.to_string())
            }
            _ => None,
        }
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

    /// Create a [`ScriptCommandGate`] bound to this VM's command context and
    /// execution state. The gate owns the `redis.call` / `redis.pcall`
    /// classify + route + dispatch contract for one script execution.
    pub(crate) fn create_command_gate(
        &self,
        read_only: bool,
        enforce_cross_slot: bool,
        cross_slot: CrossSlotTracker,
    ) -> ScriptCommandGate {
        ScriptCommandGate::new(
            self.cmd_ctx.clone(),
            self.state.clone(),
            read_only,
            enforce_cross_slot,
            cross_slot,
        )
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
        redis_table
            .set("REDIS_VERSION", REDIS_VERSION)
            .map_err(|e| ScriptError::Internal(format!("Failed to set REDIS_VERSION: {}", e)))?;
        redis_table
            .set("REDIS_VERSION_NUM", REDIS_VERSION_NUM)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to set REDIS_VERSION_NUM: {}", e))
            })?;

        // --- redis.sha1hex(input) ---
        let sha1hex_fn = self
            .lua
            .create_function(|_, input: mlua::String| -> LuaResult<String> {
                let mut hasher = Sha1::new();
                hasher.update(input.as_bytes());
                let digest = hasher.finalize();
                Ok(hex::encode(digest))
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create sha1hex fn: {}", e)))?;
        redis_table
            .set("sha1hex", sha1hex_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.sha1hex: {}", e)))?;

        // --- redis.error_reply(msg) ---
        let error_reply_fn = self
            .lua
            .create_function(|lua, msg: mlua::String| -> LuaResult<Value> {
                let table = lua.create_table()?;
                // Redis defaults empty error strings to "ERR"
                let effective_msg = if msg.as_bytes().is_empty() {
                    lua.create_string("ERR")?
                } else {
                    msg
                };
                table.set("err", effective_msg)?;
                Ok(Value::Table(table))
            })
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to create error_reply fn: {}", e))
            })?;
        redis_table
            .set("error_reply", error_reply_fn)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to set redis.error_reply: {}", e))
            })?;

        // --- redis.status_reply(msg) ---
        let status_reply_fn = self
            .lua
            .create_function(|lua, msg: mlua::String| -> LuaResult<Value> {
                let table = lua.create_table()?;
                table.set("ok", msg)?;
                Ok(Value::Table(table))
            })
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to create status_reply fn: {}", e))
            })?;
        redis_table
            .set("status_reply", status_reply_fn)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to set redis.status_reply: {}", e))
            })?;

        // --- redis.replicate_commands() --- deprecated compat stub, always returns 1
        let replicate_commands_fn = self
            .lua
            .create_function(|_, ()| -> LuaResult<i64> { Ok(1) })
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to create replicate_commands fn: {}", e))
            })?;
        redis_table
            .set("replicate_commands", replicate_commands_fn)
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to set redis.replicate_commands: {}", e))
            })?;

        // --- redis.set_repl(mode) --- validates mode 0-3, no-op stub
        let set_repl_fn = self
            .lua
            .create_function(|_, mode: i64| -> LuaResult<()> {
                if !(0..=3).contains(&mode) {
                    return Err(mlua::Error::RuntimeError(
                        "ERR Invalid replication flags. Use REPL_AOF, REPL_SLAVE, REPL_ALL or REPL_NONE.".to_string(),
                    ));
                }
                Ok(())
            })
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to create set_repl fn: {}", e))
            })?;
        redis_table
            .set("set_repl", set_repl_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.set_repl: {}", e)))?;

        // Replication mode constants
        redis_table
            .set("REPL_NONE", 0i64)
            .map_err(|e| ScriptError::Internal(format!("Failed to set REPL_NONE: {}", e)))?;
        redis_table
            .set("REPL_SLAVE", 1i64)
            .map_err(|e| ScriptError::Internal(format!("Failed to set REPL_SLAVE: {}", e)))?;
        redis_table
            .set("REPL_AOF", 2i64)
            .map_err(|e| ScriptError::Internal(format!("Failed to set REPL_AOF: {}", e)))?;
        redis_table
            .set("REPL_ALL", 3i64)
            .map_err(|e| ScriptError::Internal(format!("Failed to set REPL_ALL: {}", e)))?;

        // Register as a protected global (adds to backing table and sets
        // readonly metatable so `redis.call = ...` errors). The `bit`,
        // `cjson`, and `cmsgpack` libraries are installed once at VM
        // construction by the shared sandbox constructor.
        sandbox::register_protected_global(self.lua(), "redis", redis_table)
            .map_err(|e| ScriptError::Internal(e.to_string()))?;

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

    // -----------------------------------------------------------------------
    // Load/execute sandbox parity (proposal 32): the execution VM and the
    // FUNCTION LOAD capture VM are built by the same constructor, so library
    // code must behave identically in both.
    // -----------------------------------------------------------------------

    fn exec_err(vm: &LuaVm, source: &[u8]) -> String {
        vm.prepare_execution(&[], &[]).unwrap();
        let err = vm.execute(source).unwrap_err().to_string();
        vm.cleanup_execution();
        err
    }

    /// The full bit library (tohex/rol/bswap) is available at VM construction
    /// (not only after set_redis_functions) and matches the load-time VM.
    #[test]
    fn test_bit_library_parity_with_loader() {
        // Execute side: full bit library present.
        let vm = LuaVm::new(ScriptingConfig::default()).unwrap();
        vm.prepare_execution(&[], &[]).unwrap();
        let result = vm
            .execute(b"return bit.tohex(bit.rol(bit.bswap(255), 4))")
            .unwrap();
        vm.cleanup_execution();
        assert!(matches!(result, Value::String(_)));

        // Load side: the same top-level expression loads.
        let lib = frogdb_scripting::load_library(
            "#!lua name=paritybit\n\
             local h = bit.tohex(bit.rol(bit.bswap(255), 4))\n\
             redis.register_function('f', function() return h end)\n",
        )
        .unwrap();
        assert!(lib.get_function("f").is_some());
    }

    /// Setting an undeclared global produces the same error in both modes.
    #[test]
    fn test_global_write_parity_with_loader() {
        const MSG: &str = "Attempt to modify a readonly table";

        let vm = LuaVm::new(ScriptingConfig::default()).unwrap();
        let exec_msg = exec_err(&vm, b"x = 1");
        assert!(exec_msg.contains(MSG), "execute: {exec_msg}");

        let load_err = frogdb_scripting::load_library("#!lua name=parityw\nx = 1\n").unwrap_err();
        assert!(load_err.to_string().contains(MSG), "load: {load_err}");
    }

    /// Reading an undeclared global produces the same error in both modes.
    #[test]
    fn test_global_read_parity_with_loader() {
        const MSG: &str = "Script attempted to access nonexistent global variable";

        let vm = LuaVm::new(ScriptingConfig::default()).unwrap();
        let exec_msg = exec_err(&vm, b"return undeclared_thing");
        assert!(exec_msg.contains(MSG), "execute: {exec_msg}");

        let load_err =
            frogdb_scripting::load_library("#!lua name=parityr\nlocal x = undeclared_thing\n")
                .unwrap_err();
        assert!(load_err.to_string().contains(MSG), "load: {load_err}");
    }

    /// cjson is available in both modes with identical output.
    #[test]
    fn test_cjson_parity_with_loader() {
        let vm = LuaVm::new(ScriptingConfig::default()).unwrap();
        vm.prepare_execution(&[], &[]).unwrap();
        let result = vm
            .execute(b"return cjson.encode(cjson.decode('[1,2,3]'))")
            .unwrap();
        vm.cleanup_execution();
        match result {
            Value::String(s) => assert_eq!(s.to_str().unwrap(), "[1,2,3]"),
            other => panic!("Expected string, got {other:?}"),
        }

        let lib = frogdb_scripting::load_library(
            "#!lua name=parityj\n\
             assert(cjson.encode(cjson.decode('[1,2,3]')) == '[1,2,3]')\n\
             redis.register_function('f', function() return 1 end)\n",
        )
        .unwrap();
        assert!(lib.get_function("f").is_some());
    }
}
