//! Lua VM with sandboxing and resource limits.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use mlua::{Function, HookTriggers, Lua, Result as LuaResult, StdLib, Value, VmState};

use super::config::ScriptingConfig;
use super::error::ScriptError;

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
    state: Mutex<ExecutionState>,
    /// Kill flag (thread-safe for use in hooks).
    kill_flag: Arc<AtomicBool>,
}

impl LuaVm {
    /// Create a new Lua VM with sandboxing.
    pub fn new(config: ScriptingConfig) -> Result<Self, ScriptError> {
        // Create Lua with minimal standard libraries
        let libs = StdLib::COROUTINE
            | StdLib::TABLE
            | StdLib::STRING
            | StdLib::MATH;

        let lua = unsafe {
            Lua::unsafe_new_with(libs, mlua::LuaOptions::default())
        };

        // Set memory limit if configured
        if config.lua_heap_limit_mb > 0 {
            let limit_bytes = config.lua_heap_limit_mb * 1024 * 1024;
            lua.set_memory_limit(limit_bytes)
                .map_err(|e| ScriptError::Internal(format!("Failed to set memory limit: {}", e)))?;
        }

        let state = Mutex::new(ExecutionState::default());
        let kill_flag = Arc::new(AtomicBool::new(false));

        let vm = Self { lua, config, state, kill_flag };

        // Apply sandbox restrictions
        vm.apply_sandbox()?;

        Ok(vm)
    }

    /// Apply sandbox restrictions to the Lua environment.
    fn apply_sandbox(&self) -> Result<(), ScriptError> {
        let globals = self.lua.globals();

        // Remove dangerous functions from global scope
        let forbidden = [
            "loadfile",
            "dofile",
            "load",     // Can load bytecode
            "rawset",   // Can bypass metatables
            "rawget",   // Can bypass metatables
            "setfenv",  // Lua 5.1 only, but be safe
            "getfenv",  // Lua 5.1 only, but be safe
            "module",   // Deprecated
            "require",  // No module loading
            "package",  // No package system
            "io",       // No I/O
            "os",       // No OS access (we'll add back safe functions)
            "debug",    // No debug library
            "collectgarbage", // No GC manipulation
        ];

        for name in &forbidden {
            let _ = globals.raw_set(*name, Value::Nil);
        }

        // Create a safe os table with only clock and time
        let os_table = self.lua.create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create os table: {}", e)))?;

        let clock_fn = self.lua.create_function(|_, ()| -> LuaResult<f64> {
            Ok(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0))
        }).map_err(|e| ScriptError::Internal(format!("Failed to create clock fn: {}", e)))?;

        let time_fn = self.lua.create_function(|_, ()| -> LuaResult<i64> {
            Ok(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0))
        }).map_err(|e| ScriptError::Internal(format!("Failed to create time fn: {}", e)))?;

        os_table.set("clock", clock_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set os.clock: {}", e)))?;
        os_table.set("time", time_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set os.time: {}", e)))?;
        globals.set("os", os_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set os: {}", e)))?;

        Ok(())
    }

    /// Set up hook for timeout checking.
    fn setup_timeout_hook(&self, start_time: Instant) {
        let timeout_ms = self.config.lua_time_limit_ms;
        let grace_ms = self.config.lua_timeout_grace_ms;
        let kill_flag = self.kill_flag.clone();

        if timeout_ms > 0 {
            // Check every 10000 instructions for timeout
            let triggers = HookTriggers::new().every_nth_instruction(10000);

            self.lua.set_hook(triggers, move |_lua, _debug| {
                // Check kill flag
                if kill_flag.load(Ordering::Relaxed) {
                    return Err(mlua::Error::RuntimeError("Script killed".to_string()));
                }

                // Check timeout
                let elapsed = start_time.elapsed().as_millis() as u64;
                if elapsed > timeout_ms + grace_ms {
                    return Err(mlua::Error::RuntimeError(
                        format!("BUSY script running for {} ms", elapsed)
                    ));
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
    pub fn prepare_execution(
        &self,
        keys: &[Bytes],
        argv: &[Bytes],
    ) -> Result<(), ScriptError> {
        let start_time = Instant::now();

        // Reset execution state
        {
            let mut state = self.state.lock().unwrap();
            state.reset();
            state.start_time = Some(start_time);
            state.timeout_ms = self.config.lua_time_limit_ms;
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
        let keys_table = self.lua.create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create KEYS table: {}", e)))?;
        for (i, key) in keys.iter().enumerate() {
            keys_table.set(i + 1, key.as_ref())
                .map_err(|e| ScriptError::Internal(format!("Failed to set KEYS[{}]: {}", i + 1, e)))?;
        }
        globals.set("KEYS", keys_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set KEYS: {}", e)))?;

        // Create ARGV table
        let argv_table = self.lua.create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create ARGV table: {}", e)))?;
        for (i, arg) in argv.iter().enumerate() {
            argv_table.set(i + 1, arg.as_ref())
                .map_err(|e| ScriptError::Internal(format!("Failed to set ARGV[{}]: {}", i + 1, e)))?;
        }
        globals.set("ARGV", argv_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set ARGV: {}", e)))?;

        Ok(())
    }

    /// Clean up after script execution.
    pub fn cleanup_execution(&self) {
        self.remove_timeout_hook();

        // Clear KEYS and ARGV
        let globals = self.lua.globals();
        let _ = globals.set("KEYS", Value::Nil);
        let _ = globals.set("ARGV", Value::Nil);

        // Reset state
        self.state.lock().unwrap().reset();
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
        let result = chunk.eval::<Value>()
            .map_err(|e| {
                match e {
                    mlua::Error::MemoryError(_) => ScriptError::MemoryLimitExceeded,
                    mlua::Error::SyntaxError { message, .. } => ScriptError::Compilation(message),
                    mlua::Error::RuntimeError(msg) => {
                        if msg.contains("BUSY") {
                            ScriptError::Timeout { timeout_ms: self.config.lua_time_limit_ms }
                        } else if msg.contains("killed") {
                            ScriptError::Killed
                        } else {
                            ScriptError::Runtime(msg)
                        }
                    }
                    _ => ScriptError::Runtime(e.to_string()),
                }
            })?;

        Ok(result)
    }

    /// Get the Lua state for setting up bindings.
    pub fn lua(&self) -> &Lua {
        &self.lua
    }

    /// Get the execution state.
    pub fn state(&self) -> &Mutex<ExecutionState> {
        &self.state
    }

    /// Mark the script as having performed writes.
    pub fn mark_write(&self) {
        self.state.lock().unwrap().has_writes = true;
    }

    /// Check if the script has performed writes.
    pub fn has_writes(&self) -> bool {
        self.state.lock().unwrap().has_writes
    }

    /// Request the script to be killed.
    pub fn request_kill(&self) -> Result<(), ScriptError> {
        let state = self.state.lock().unwrap();
        if state.has_writes {
            Err(ScriptError::Unkillable)
        } else {
            self.kill_flag.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Check if the script is currently running.
    pub fn is_running(&self) -> bool {
        self.state.lock().unwrap().start_time.is_some()
    }

    /// Set up the redis.call and redis.pcall functions.
    pub fn set_redis_functions(&self, call_fn: Function, pcall_fn: Function) -> Result<(), ScriptError> {
        let globals = self.lua.globals();

        let redis_table = self.lua.create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create redis table: {}", e)))?;

        redis_table.set("call", call_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.call: {}", e)))?;

        redis_table.set("pcall", pcall_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis.pcall: {}", e)))?;

        globals.set("redis", redis_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set redis: {}", e)))?;

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
