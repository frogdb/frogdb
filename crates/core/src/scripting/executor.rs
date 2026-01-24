//! Script executor that orchestrates Lua script execution.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::Response;
use mlua::{MultiValue, Value};
use tracing::{debug, info, warn};

use super::bindings::{
    extract_keys_from_command, is_forbidden_in_script, is_write_command,
    lua_args_to_command, response_to_lua, validate_key_access,
};
use super::cache::{hex_to_sha, sha_to_hex, ScriptCache, ScriptSha};
use super::config::ScriptingConfig;
use super::error::ScriptError;
use super::lua_vm::{CommandExecutionContext, LuaVm};

use crate::command::CommandContext;
use crate::registry::CommandRegistry;

/// Script executor for a single shard.
pub struct ScriptExecutor {
    /// Lua VM for this shard.
    vm: LuaVm,
    /// Script cache for this shard.
    cache: ScriptCache,
    /// Configuration (kept for future use).
    #[allow(dead_code)]
    config: ScriptingConfig,
    /// Whether a script is currently running.
    running: AtomicBool,
}

impl ScriptExecutor {
    /// Create a new script executor.
    pub fn new(config: ScriptingConfig) -> Result<Self, ScriptError> {
        let vm = LuaVm::new(config.clone())?;
        let cache = ScriptCache::new(
            config.lua_script_cache_max_size,
            config.lua_script_cache_max_bytes,
        );

        Ok(Self {
            vm,
            cache,
            config,
            running: AtomicBool::new(false),
        })
    }

    /// Execute a script by source.
    pub fn eval(
        &mut self,
        source: &[u8],
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
    ) -> Result<Response, ScriptError> {
        // Check if already running
        if self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::NestedScript);
        }

        // Cache the script
        let sha = self.cache.load(Bytes::copy_from_slice(source));

        // Execute
        self.execute_script(source, &sha, keys, argv, ctx, registry)
    }

    /// Execute a script by SHA.
    pub fn evalsha(
        &mut self,
        sha_hex: &[u8],
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
    ) -> Result<Response, ScriptError> {
        // Check if already running
        if self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::NestedScript);
        }

        // Parse SHA
        let sha = hex_to_sha(sha_hex).ok_or(ScriptError::NoScript)?;

        // Look up in cache
        let script = self.cache.get(&sha).ok_or(ScriptError::NoScript)?;
        let source = script.source.clone();

        // Execute
        self.execute_script(&source, &sha, keys, argv, ctx, registry)
    }

    /// Execute a script.
    fn execute_script(
        &self,
        source: &[u8],
        sha: &ScriptSha,
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
    ) -> Result<Response, ScriptError> {
        let script_sha = sha_to_hex(sha);
        let start_time = Instant::now();

        debug!(
            script_sha = %script_sha,
            keys_count = keys.len(),
            "Script execution started"
        );

        self.running.store(true, Ordering::Relaxed);

        // Prepare VM
        self.vm.prepare_execution(keys, argv)?;

        // Set up command execution context with raw pointers
        // SAFETY: These pointers are valid for the duration of script execution,
        // which is synchronous and single-threaded within a shard. We use raw
        // pointers to allow the context to be stored in RwLock and accessed
        // from Lua closures. The pointers are cleared immediately after script
        // execution completes, before the function returns.
        //
        // We use transmute to erase the lifetime from the fat pointer to dyn Store.
        // This is safe because we ensure the pointer is only used during script
        // execution and cleared afterwards.
        let store_ptr: *mut dyn crate::store::Store = unsafe {
            std::mem::transmute(ctx.store as *mut dyn crate::store::Store)
        };
        let cmd_exec_ctx = CommandExecutionContext {
            store_ptr,
            registry_ptr: registry as *const CommandRegistry,
            shard_senders: Arc::clone(ctx.shard_senders),
            shard_id: ctx.shard_id,
            num_shards: ctx.num_shards,
            conn_id: ctx.conn_id,
            protocol_version: ctx.protocol_version,
        };
        self.vm.set_command_context(cmd_exec_ctx);

        // Set up redis.call and redis.pcall
        self.setup_redis_bindings(keys)?;

        // Execute the script
        let result = self.vm.execute(source);

        // Cleanup - clear context BEFORE cleanup_execution
        self.vm.clear_command_context();
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);

        // Convert result
        let duration_ms = start_time.elapsed().as_millis() as u64;
        match result {
            Ok(value) => {
                debug!(
                    script_sha = %script_sha,
                    duration_ms,
                    "Script executed"
                );
                Ok(self.lua_to_response(value))
            }
            Err(ref e) => {
                match e {
                    ScriptError::Timeout { timeout_ms } => {
                        warn!(
                            script_sha = %script_sha,
                            timeout_ms = *timeout_ms,
                            "Script timed out"
                        );
                    }
                    ScriptError::Killed => {
                        info!(script_sha = %script_sha, "Script killed");
                    }
                    ScriptError::MemoryLimitExceeded => {
                        warn!(script_sha = %script_sha, "Script exceeded memory limit");
                    }
                    _ => {
                        warn!(
                            script_sha = %script_sha,
                            error = %e,
                            "Script execution failed"
                        );
                    }
                }
                Err(result.unwrap_err())
            }
        }
    }

    /// Set up redis.call and redis.pcall bindings.
    fn setup_redis_bindings(
        &self,
        declared_keys: &[Bytes],
    ) -> Result<(), ScriptError> {
        let lua = self.vm.lua();

        // Clone declared_keys so we can move it into the closures
        // Use Arc<Vec<Bytes>> so it can be shared between the two closures
        let keys_for_call = Arc::new(declared_keys.to_vec());
        let keys_for_pcall = keys_for_call.clone();

        // Get context accessor for command execution
        let accessor_for_call = self.vm.create_context_accessor();
        let accessor_for_pcall = self.vm.create_context_accessor();

        // Create redis.call function
        let call_fn = lua.create_function(move |lua_ctx, args: MultiValue| -> mlua::Result<Value> {
            // Convert args to command parts
            let parts = match lua_args_to_command(args) {
                Ok(p) => p,
                Err(e) => {
                    return Err(mlua::Error::RuntimeError(e.to_string()));
                }
            };

            if parts.is_empty() {
                return Err(mlua::Error::RuntimeError(
                    "ERR wrong number of arguments for redis command".to_string(),
                ));
            }

            // Get command name
            let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();

            // Check if forbidden
            if let Some(err) = is_forbidden_in_script(&cmd_name) {
                return Err(mlua::Error::RuntimeError(err.to_string()));
            }

            // Validate key access
            let keys = extract_keys_from_command(&cmd_name, &parts);
            for key in &keys {
                if let Err(e) = validate_key_access(key, &keys_for_call) {
                    return Err(mlua::Error::RuntimeError(e.to_string()));
                }
            }

            // Track writes
            if is_write_command(&cmd_name) {
                accessor_for_call.mark_write();
            }

            // Execute the command
            match accessor_for_call.execute_command(&parts) {
                Ok(response) => {
                    // redis.call raises error on Redis errors
                    if let Response::Error(ref e) = response {
                        return Err(mlua::Error::RuntimeError(
                            String::from_utf8_lossy(e).to_string()
                        ));
                    }
                    response_to_lua(lua_ctx, response)
                }
                Err(e) => Err(mlua::Error::RuntimeError(e)),
            }
        }).map_err(|e| ScriptError::Internal(format!("Failed to create call function: {}", e)))?;

        // Create redis.pcall function (same as call but catches errors)
        let pcall_fn = lua.create_function(move |lua_ctx, args: MultiValue| {
            // Convert args to command parts
            let parts = match lua_args_to_command(args) {
                Ok(p) => p,
                Err(e) => {
                    // pcall returns error as table
                    let table = lua_ctx.create_table()?;
                    table.set("err", e.to_string())?;
                    return Ok(Value::Table(table));
                }
            };

            if parts.is_empty() {
                let table = lua_ctx.create_table()?;
                table.set("err", "ERR wrong number of arguments for redis command")?;
                return Ok(Value::Table(table));
            }

            // Get command name
            let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();

            // Check if forbidden
            if let Some(err) = is_forbidden_in_script(&cmd_name) {
                let table = lua_ctx.create_table()?;
                table.set("err", err)?;
                return Ok(Value::Table(table));
            }

            // Validate key access
            let keys = extract_keys_from_command(&cmd_name, &parts);
            for key in &keys {
                if let Err(e) = validate_key_access(key, &keys_for_pcall) {
                    let table = lua_ctx.create_table()?;
                    table.set("err", e.to_string())?;
                    return Ok(Value::Table(table));
                }
            }

            // Track writes
            if is_write_command(&cmd_name) {
                accessor_for_pcall.mark_write();
            }

            // Execute the command
            match accessor_for_pcall.execute_command(&parts) {
                Ok(response) => {
                    // pcall returns errors as {err='...'} table instead of raising
                    response_to_lua(lua_ctx, response)
                }
                Err(e) => {
                    let table = lua_ctx.create_table()?;
                    table.set("err", e)?;
                    Ok(Value::Table(table))
                }
            }
        }).map_err(|e| ScriptError::Internal(format!("Failed to create pcall function: {}", e)))?;

        self.vm.set_redis_functions(call_fn, pcall_fn)?;

        Ok(())
    }

    /// Convert Lua value to RESP response.
    fn lua_to_response(&self, value: Value) -> Response {
        match value {
            Value::Nil => Response::Null,
            Value::Boolean(false) => Response::Null, // Lua convention
            Value::Boolean(true) => Response::Integer(1),
            Value::Integer(n) => Response::Integer(n),
            Value::Number(n) => {
                // Redis converts floats to integers if they're whole numbers
                if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
                    Response::Integer(n as i64)
                } else {
                    Response::bulk(Bytes::from(n.to_string()))
                }
            }
            Value::String(s) => Response::bulk(Bytes::copy_from_slice(s.as_bytes().as_ref())),
            Value::Table(t) => {
                // Check if it's an error table {err = "..."}
                if let Ok(err) = t.get::<String>("err") {
                    return Response::Error(Bytes::from(err));
                }
                // Check if it's an ok table {ok = "..."}
                if let Ok(ok) = t.get::<String>("ok") {
                    return Response::Simple(Bytes::from(ok));
                }

                // Otherwise treat as array
                let mut arr = Vec::new();
                let mut i: i64 = 1;
                loop {
                    match t.get::<Value>(i) {
                        Ok(v) if !matches!(v, Value::Nil) => {
                            arr.push(self.lua_to_response(v));
                            i += 1;
                        }
                        _ => break,
                    }
                }
                Response::Array(arr)
            }
            _ => Response::Null,
        }
    }

    /// Load a script into the cache and return its SHA.
    pub fn load_script(&mut self, source: Bytes) -> String {
        let sha = self.cache.load(source);
        let script_sha = sha_to_hex(&sha);
        debug!(script_sha = %script_sha, "Script cached");
        script_sha
    }

    /// Check if scripts exist in the cache.
    pub fn scripts_exist(&self, shas: &[&[u8]]) -> Vec<bool> {
        shas.iter()
            .map(|sha_hex| {
                hex_to_sha(sha_hex)
                    .map(|sha| self.cache.exists(&sha))
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Flush all scripts from the cache.
    pub fn flush_scripts(&mut self) {
        let scripts_removed = self.cache.len();
        self.cache.flush();
        info!(scripts_removed, "Script cache flushed");
    }

    /// Request to kill the currently running script.
    pub fn kill_script(&self) -> Result<(), ScriptError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::Internal("No script running".to_string()));
        }
        self.vm.request_kill()
    }

    /// Check if a script is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.cache.len(), self.cache.current_bytes())
    }

    /// Execute a function from a library.
    ///
    /// This loads the library code, then calls the named function with the given keys and args.
    pub fn execute_function(
        &mut self,
        function_name: &str,
        library_code: &str,
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
        read_only: bool,
    ) -> Result<Response, ScriptError> {
        // Check if already running
        if self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::NestedScript);
        }

        self.running.store(true, Ordering::Relaxed);

        // Prepare VM
        self.vm.prepare_execution(keys, argv)?;

        // Set up command execution context
        let store_ptr: *mut dyn crate::store::Store = unsafe {
            std::mem::transmute(ctx.store as *mut dyn crate::store::Store)
        };
        let cmd_exec_ctx = CommandExecutionContext {
            store_ptr,
            registry_ptr: registry as *const CommandRegistry,
            shard_senders: Arc::clone(ctx.shard_senders),
            shard_id: ctx.shard_id,
            num_shards: ctx.num_shards,
            conn_id: ctx.conn_id,
            protocol_version: ctx.protocol_version,
        };
        self.vm.set_command_context(cmd_exec_ctx);

        // Set up redis.call and redis.pcall
        self.setup_redis_bindings(keys)?;

        // Strip shebang from library code (Lua doesn't understand #!)
        let library_code_clean = if library_code.starts_with("#!") {
            library_code.find('\n').map(|pos| &library_code[pos + 1..]).unwrap_or("")
        } else {
            library_code
        };

        // Build function execution code
        // This loads the library first, then calls the function
        let execution_code = format!(
            r#"
-- Load the library (this registers functions in redis.functions)
{}

-- Get the registered function
local fn = __frogdb_functions and __frogdb_functions["{}"]
if not fn then
    error("Function not found after loading library: {}")
end

-- Call the function with KEYS and ARGV
return fn(KEYS, ARGV)
"#,
            library_code_clean, function_name, function_name
        );

        // Set up function registration before loading
        self.setup_function_registration()?;

        // Execute
        let result = self.vm.execute(execution_code.as_bytes());

        // Check for read-only violations
        if read_only && self.vm.has_writes() {
            self.vm.clear_command_context();
            self.vm.cleanup_execution();
            self.running.store(false, Ordering::Relaxed);
            return Err(ScriptError::Runtime(format!(
                "FCALL_RO called but function '{}' performed a write",
                function_name
            )));
        }

        // Cleanup
        self.vm.clear_command_context();
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);

        // Convert result
        match result {
            Ok(value) => Ok(self.lua_to_response(value)),
            Err(e) => Err(e),
        }
    }

    /// Set up the function registration mechanism for library loading.
    fn setup_function_registration(&self) -> Result<(), ScriptError> {
        let lua = self.vm.lua();
        let globals = lua.globals();

        // Create a storage table for registered functions
        let functions_table = lua.create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create functions table: {}", e)))?;

        globals.set("__frogdb_functions", functions_table)
            .map_err(|e| ScriptError::Internal(format!("Failed to set functions table: {}", e)))?;

        // Get the redis table and add register_function
        let redis_table: mlua::Table = globals.get("redis")
            .map_err(|e| ScriptError::Internal(format!("Failed to get redis table: {}", e)))?;

        // Create register_function
        let register_fn = lua.create_function(|lua_ctx, args: MultiValue| -> mlua::Result<()> {
            let args_vec: Vec<Value> = args.into_iter().collect();

            if args_vec.is_empty() {
                return Err(mlua::Error::RuntimeError(
                    "redis.register_function requires at least one argument".to_string(),
                ));
            }

            let globals = lua_ctx.globals();
            let functions_table: mlua::Table = globals.get("__frogdb_functions")
                .map_err(|_| mlua::Error::RuntimeError("Functions table not found".to_string()))?;

            match &args_vec[0] {
                // Table form: redis.register_function{function_name='name', callback=fn, ...}
                Value::Table(t) => {
                    let name: String = t.get("function_name")
                        .map_err(|_| mlua::Error::RuntimeError(
                            "Missing required field 'function_name'".to_string()
                        ))?;

                    let callback: mlua::Function = t.get("callback")
                        .map_err(|_| mlua::Error::RuntimeError(
                            "Missing required field 'callback'".to_string()
                        ))?;

                    functions_table.set(name, callback)?;
                }

                // Simple form: redis.register_function('name', callback)
                Value::String(name) => {
                    if args_vec.len() < 2 {
                        return Err(mlua::Error::RuntimeError(
                            "redis.register_function requires a callback function".to_string(),
                        ));
                    }

                    let callback = match &args_vec[1] {
                        Value::Function(f) => f.clone(),
                        _ => return Err(mlua::Error::RuntimeError(
                            "Second argument must be a function".to_string(),
                        )),
                    };

                    functions_table.set(name.to_str()?, callback)?;
                }

                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "First argument must be a string or table".to_string(),
                    ));
                }
            }

            Ok(())
        }).map_err(|e| ScriptError::Internal(format!("Failed to create register_function: {}", e)))?;

        redis_table.set("register_function", register_fn)
            .map_err(|e| ScriptError::Internal(format!("Failed to set register_function: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let config = ScriptingConfig::default();
        let executor = ScriptExecutor::new(config).unwrap();
        assert!(!executor.is_running());
    }

    #[test]
    fn test_load_script() {
        let config = ScriptingConfig::default();
        let mut executor = ScriptExecutor::new(config).unwrap();

        let sha1 = executor.load_script(Bytes::from_static(b"return 1"));
        let sha2 = executor.load_script(Bytes::from_static(b"return 1"));

        // Same script should produce same SHA
        assert_eq!(sha1, sha2);
        assert_eq!(sha1.len(), 40); // SHA1 hex is 40 chars
    }

    #[test]
    fn test_scripts_exist() {
        let config = ScriptingConfig::default();
        let mut executor = ScriptExecutor::new(config).unwrap();

        let sha = executor.load_script(Bytes::from_static(b"return 1"));

        let results = executor.scripts_exist(&[sha.as_bytes(), b"nonexistent1234567890123456789012345678"]);
        assert_eq!(results, vec![true, false]);
    }

    #[test]
    fn test_flush_scripts() {
        let config = ScriptingConfig::default();
        let mut executor = ScriptExecutor::new(config).unwrap();

        let sha = executor.load_script(Bytes::from_static(b"return 1"));
        assert!(executor.scripts_exist(&[sha.as_bytes()])[0]);

        executor.flush_scripts();
        assert!(!executor.scripts_exist(&[sha.as_bytes()])[0]);
    }

    #[test]
    fn test_lua_to_response_nil() {
        let config = ScriptingConfig::default();
        let executor = ScriptExecutor::new(config).unwrap();

        let response = executor.lua_to_response(Value::Nil);
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn test_lua_to_response_integer() {
        let config = ScriptingConfig::default();
        let executor = ScriptExecutor::new(config).unwrap();

        let response = executor.lua_to_response(Value::Integer(42));
        assert!(matches!(response, Response::Integer(42)));
    }

    #[test]
    fn test_lua_to_response_false_is_null() {
        let config = ScriptingConfig::default();
        let executor = ScriptExecutor::new(config).unwrap();

        // Lua convention: false -> nil in Redis
        let response = executor.lua_to_response(Value::Boolean(false));
        assert!(matches!(response, Response::Null));
    }

    #[test]
    fn test_lua_to_response_true_is_one() {
        let config = ScriptingConfig::default();
        let executor = ScriptExecutor::new(config).unwrap();

        // Lua convention: true -> 1 in Redis
        let response = executor.lua_to_response(Value::Boolean(true));
        assert!(matches!(response, Response::Integer(1)));
    }
}
