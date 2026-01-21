//! Script executor that orchestrates Lua script execution.

use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use frogdb_protocol::Response;
use mlua::{MultiValue, Value};

use super::bindings::{
    extract_keys_from_command, is_forbidden_in_script,
    lua_args_to_command, validate_key_access,
};
use super::cache::{hex_to_sha, sha_to_hex, ScriptCache, ScriptSha};
use super::config::ScriptingConfig;
use super::error::ScriptError;
use super::lua_vm::LuaVm;

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
        _sha: &ScriptSha,
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
    ) -> Result<Response, ScriptError> {
        self.running.store(true, Ordering::Relaxed);

        // Prepare VM
        self.vm.prepare_execution(keys, argv)?;

        // Set up redis.call and redis.pcall
        self.setup_redis_bindings(ctx, registry, keys)?;

        // Execute the script
        let result = self.vm.execute(source);

        // Cleanup
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);

        // Convert result
        match result {
            Ok(value) => Ok(self.lua_to_response(value)),
            Err(e) => Err(e),
        }
    }

    /// Set up redis.call and redis.pcall bindings.
    fn setup_redis_bindings(
        &self,
        _ctx: &mut CommandContext,
        _registry: &CommandRegistry,
        declared_keys: &[Bytes],
    ) -> Result<(), ScriptError> {
        let lua = self.vm.lua();

        // Clone declared_keys so we can move it into the closures
        // Use Arc<Vec<Bytes>> so it can be shared between the two closures
        let keys_for_call = std::sync::Arc::new(declared_keys.to_vec());
        let keys_for_pcall = keys_for_call.clone();

        // Create redis.call function
        let call_fn = lua.create_function(move |_lua_ctx, args: MultiValue| -> mlua::Result<Value> {
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

            // For now, return an error indicating commands need proper integration
            // This will be replaced with actual command execution when integrated
            Err(mlua::Error::RuntimeError(
                "ERR redis.call not yet integrated with command execution".to_string(),
            ))
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

            // For now, return an error table
            let table = lua_ctx.create_table()?;
            table.set("err", "ERR redis.pcall not yet integrated with command execution")?;
            Ok(Value::Table(table))
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
        sha_to_hex(&sha)
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
        self.cache.flush();
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
