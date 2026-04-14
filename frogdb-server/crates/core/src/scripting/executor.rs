//! Script executor — orchestrates Lua script execution with shebang support.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_scripting::FunctionFlags;
use mlua::{MultiValue, Value};
use tracing::{debug, info, warn};

use super::bindings::{
    extract_keys_from_command, is_forbidden_in_script, is_write_command, lua_args_to_command,
    response_to_lua,
};
use super::cache::{ScriptCache, ScriptSha, hex_to_sha, sha_to_hex};
use super::config::ScriptingConfig;
use super::error::ScriptError;
use super::lua_vm::{CommandExecutionContext, LuaVm};
use crate::command::CommandContext;
use crate::registry::CommandRegistry;
use crate::shard::slot_for_key;

#[derive(Debug, Clone, Default)]
struct EvalShebangFlags {
    flags: FunctionFlags,
    has_shebang: bool,
}

fn parse_eval_shebang(source: &[u8]) -> Result<Option<EvalShebangFlags>, ScriptError> {
    let s = std::str::from_utf8(source)
        .map_err(|_| ScriptError::Runtime("ERR Script is not valid UTF-8".into()))?;
    if !s.starts_with("#!") {
        return Ok(None);
    }
    let first_line = s.lines().next().unwrap_or("");
    let rest = &first_line[2..];
    let (eng, meta) = match rest.find(char::is_whitespace) {
        Some(i) => (&rest[..i], rest[i..].trim()),
        None => (rest.trim(), ""),
    };
    if !eng.eq_ignore_ascii_case("lua") {
        return Err(ScriptError::Runtime(format!(
            "ERR Unsupported engine '{eng}'"
        )));
    }
    let mut flags = FunctionFlags::empty();
    for part in meta.split_whitespace() {
        if let Some(("flags", v)) = part.split_once('=') {
            for f in v.split(',') {
                let f = f.trim();
                if f.is_empty() {
                    continue;
                }
                match f {
                    "no-writes" => flags |= FunctionFlags::NO_WRITES,
                    "allow-oom" => flags |= FunctionFlags::ALLOW_OOM,
                    "allow-stale" => flags |= FunctionFlags::ALLOW_STALE,
                    "no-cluster" => flags |= FunctionFlags::NO_CLUSTER,
                    "allow-cross-slot-keys" => flags |= FunctionFlags::ALLOW_CROSS_SLOT_KEYS,
                    _ => {
                        return Err(ScriptError::Runtime(format!(
                            "ERR Unsupported shebang flag: {f}"
                        )));
                    }
                }
            }
        } else {
            return Err(ScriptError::Runtime(format!(
                "ERR Unknown shebang option: {part}"
            )));
        }
    }
    Ok(Some(EvalShebangFlags {
        flags,
        has_shebang: true,
    }))
}

fn strip_shebang(src: &[u8]) -> &[u8] {
    src.iter()
        .position(|&b| b == b'\n')
        .map(|p| &src[p + 1..])
        .unwrap_or(b"")
}

#[derive(Debug, Clone, Default)]
struct ScriptKeyContext {
    has_shebang: bool,
    allow_cross_slot_keys: bool,
    is_cluster_mode: bool,
}
impl ScriptKeyContext {
    fn enforce_cross_slot(&self) -> bool {
        self.is_cluster_mode && self.has_shebang && !self.allow_cross_slot_keys
    }
}

pub struct ScriptExecutor {
    vm: LuaVm,
    cache: ScriptCache,
    #[allow(dead_code)]
    config: ScriptingConfig,
    running: AtomicBool,
}

impl ScriptExecutor {
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

    /// Execute a script by source (EVAL / EVAL_RO).
    #[allow(clippy::too_many_arguments)]
    pub fn eval(
        &mut self,
        source: &[u8],
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
        read_only: bool,
        is_cluster_mode: bool,
    ) -> Result<Response, ScriptError> {
        if self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::NestedScript);
        }
        let shebang = parse_eval_shebang(source)?;
        if let Some(ref sb) = shebang {
            if sb.flags.contains(FunctionFlags::NO_CLUSTER) && is_cluster_mode {
                return Err(ScriptError::Runtime(
                    "ERR Can not run script on cluster, 'no-cluster' flag is set".into(),
                ));
            }
        }
        let eff_ro = read_only
            || shebang
                .as_ref()
                .is_some_and(|sb| sb.flags.contains(FunctionFlags::NO_WRITES));
        let eff_src = if shebang.is_some() {
            strip_shebang(source)
        } else {
            source
        };
        let sha = self.cache.load(Bytes::copy_from_slice(source));
        let skc = ScriptKeyContext {
            has_shebang: shebang.as_ref().is_some_and(|sb| sb.has_shebang),
            allow_cross_slot_keys: shebang
                .as_ref()
                .is_some_and(|sb| sb.flags.contains(FunctionFlags::ALLOW_CROSS_SLOT_KEYS)),
            is_cluster_mode,
        };
        self.execute_script(eff_src, &sha, keys, argv, ctx, registry, eff_ro, &skc)
    }

    /// Execute a script by SHA (EVALSHA / EVALSHA_RO).
    #[allow(clippy::too_many_arguments)]
    pub fn evalsha(
        &mut self,
        sha_hex: &[u8],
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
        read_only: bool,
        is_cluster_mode: bool,
    ) -> Result<Response, ScriptError> {
        if self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::NestedScript);
        }
        let sha = hex_to_sha(sha_hex).ok_or(ScriptError::NoScript)?;
        let script = self.cache.get(&sha).ok_or(ScriptError::NoScript)?;
        let source = script.source.clone();
        let shebang = parse_eval_shebang(&source)?;
        if let Some(ref sb) = shebang {
            if sb.flags.contains(FunctionFlags::NO_CLUSTER) && is_cluster_mode {
                return Err(ScriptError::Runtime(
                    "ERR Can not run script on cluster, 'no-cluster' flag is set".into(),
                ));
            }
        }
        let eff_ro = read_only
            || shebang
                .as_ref()
                .is_some_and(|sb| sb.flags.contains(FunctionFlags::NO_WRITES));
        let eff_src = if shebang.is_some() {
            strip_shebang(&source)
        } else {
            &source
        };
        let skc = ScriptKeyContext {
            has_shebang: shebang.as_ref().is_some_and(|sb| sb.has_shebang),
            allow_cross_slot_keys: shebang
                .as_ref()
                .is_some_and(|sb| sb.flags.contains(FunctionFlags::ALLOW_CROSS_SLOT_KEYS)),
            is_cluster_mode,
        };
        self.execute_script(eff_src, &sha, keys, argv, ctx, registry, eff_ro, &skc)
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_script(
        &self,
        source: &[u8],
        sha: &ScriptSha,
        keys: &[Bytes],
        argv: &[Bytes],
        ctx: &mut CommandContext,
        registry: &CommandRegistry,
        read_only: bool,
        skc: &ScriptKeyContext,
    ) -> Result<Response, ScriptError> {
        let script_sha = sha_to_hex(sha);
        let start = Instant::now();
        debug!(script_sha = %script_sha, keys_count = keys.len(), "Script execution started");
        self.running.store(true, Ordering::Relaxed);
        self.vm.prepare_execution(keys, argv)?;
        let sp: *mut dyn crate::store::Store =
            unsafe { std::mem::transmute(ctx.store as *mut dyn crate::store::Store) };
        let cec = CommandExecutionContext {
            store_ptr: sp,
            registry_ptr: registry as *const CommandRegistry,
            shard_senders: Arc::clone(ctx.shard_senders),
            shard_id: ctx.shard_id,
            num_shards: ctx.num_shards,
            conn_id: ctx.conn_id,
            protocol_version: ctx.protocol_version,
        };
        self.vm.set_command_context(cec);
        self.setup_redis_bindings(keys, read_only, skc)?;
        let result = self.vm.execute(source);
        if read_only && self.vm.has_writes() {
            self.vm.clear_command_context();
            self.vm.cleanup_execution();
            self.running.store(false, Ordering::Relaxed);
            return Err(ScriptError::Runtime(
                "Write commands are not allowed from read-only scripts".into(),
            ));
        }
        self.vm.clear_command_context();
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);
        let dur = start.elapsed().as_millis() as u64;
        match result {
            Ok(v) => {
                debug!(script_sha = %script_sha, duration_ms = dur, "Script executed");
                Ok(self.lua_to_response(v))
            }
            Err(ref e) => {
                match e {
                    ScriptError::Timeout { timeout_ms } => {
                        warn!(script_sha = %script_sha, timeout_ms = *timeout_ms, "Script timed out")
                    }
                    ScriptError::Killed => info!(script_sha = %script_sha, "Script killed"),
                    ScriptError::MemoryLimitExceeded => {
                        warn!(script_sha = %script_sha, "Script exceeded memory limit")
                    }
                    _ => warn!(script_sha = %script_sha, error = %e, "Script execution failed"),
                }
                Err(result.unwrap_err())
            }
        }
    }

    fn setup_redis_bindings(
        &self,
        dk: &[Bytes],
        ro: bool,
        skc: &ScriptKeyContext,
    ) -> Result<(), ScriptError> {
        let lua = self.vm.lua();
        let ecx = skc.enforce_cross_slot();
        let is = if ecx && !dk.is_empty() {
            Some(slot_for_key(&dk[0]))
        } else {
            None
        };
        let stc: Arc<std::sync::Mutex<Option<u16>>> = Arc::new(std::sync::Mutex::new(is));
        let stp = stc.clone();
        let ac = self.vm.create_context_accessor();
        let ap = self.vm.create_context_accessor();
        let call_fn = lua.create_function(move |lc, args: MultiValue| -> mlua::Result<Value> {
            let parts = lua_args_to_command(args).map_err(|e| mlua::Error::RuntimeError(e.to_string()))?;
            if parts.is_empty() { return Err(mlua::Error::RuntimeError("ERR wrong number of arguments for redis command".into())); }
            let cn = String::from_utf8_lossy(&parts[0]).to_uppercase();
            if let Some(err) = is_forbidden_in_script(&cn) { return Err(mlua::Error::RuntimeError(err.to_string())); }
            if ro && is_write_command(&cn) { return Err(mlua::Error::RuntimeError("ERR Write commands are not allowed from read-only scripts".into())); }
            if ecx { for k in &extract_keys_from_command(&cn, &parts) { let ks = slot_for_key(k); let mut t = stc.lock().unwrap(); match *t { None => *t = Some(ks), Some(s) if ks != s => return Err(mlua::Error::RuntimeError("ERR Script attempted to access keys that do not hash to the same slot".into())), _ => {} } } }
            if is_write_command(&cn) { ac.mark_write(); }
            match ac.execute_command(&parts) { Ok(r) => { if let Response::Error(ref e) = r { return Err(mlua::Error::RuntimeError(String::from_utf8_lossy(e).to_string())); } response_to_lua(lc, r) } Err(e) => Err(mlua::Error::RuntimeError(e)) }
        }).map_err(|e| ScriptError::Internal(format!("Failed to create call function: {e}")))?;
        let pcall_fn = lua.create_function(move |lc, args: MultiValue| {
            let parts = match lua_args_to_command(args) { Ok(p) => p, Err(e) => { let t = lc.create_table()?; t.set("err", e.to_string())?; return Ok(Value::Table(t)); } };
            if parts.is_empty() { let t = lc.create_table()?; t.set("err", "ERR wrong number of arguments for redis command")?; return Ok(Value::Table(t)); }
            let cn = String::from_utf8_lossy(&parts[0]).to_uppercase();
            if let Some(err) = is_forbidden_in_script(&cn) { let t = lc.create_table()?; t.set("err", err)?; return Ok(Value::Table(t)); }
            if ro && is_write_command(&cn) { let t = lc.create_table()?; t.set("err", "ERR Write commands are not allowed from read-only scripts")?; return Ok(Value::Table(t)); }
            if ecx { for k in &extract_keys_from_command(&cn, &parts) { let ks = slot_for_key(k); let mut t = stp.lock().unwrap(); match *t { None => *t = Some(ks), Some(s) if ks != s => { let tbl = lc.create_table()?; tbl.set("err", "ERR Script attempted to access keys that do not hash to the same slot")?; return Ok(Value::Table(tbl)); } _ => {} } } }
            if is_write_command(&cn) { ap.mark_write(); }
            match ap.execute_command(&parts) { Ok(r) => response_to_lua(lc, r), Err(e) => { let t = lc.create_table()?; t.set("err", e)?; Ok(Value::Table(t)) } }
        }).map_err(|e| ScriptError::Internal(format!("Failed to create pcall function: {e}")))?;
        self.vm.set_redis_functions(call_fn, pcall_fn)?;
        Ok(())
    }

    fn lua_to_response(&self, value: Value) -> Response {
        match value {
            Value::Nil => Response::Null,
            Value::Boolean(false) => Response::Null,
            Value::Boolean(true) => Response::Integer(1),
            Value::Integer(n) => Response::Integer(n),
            Value::Number(n) => {
                if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
                    Response::Integer(n as i64)
                } else {
                    Response::bulk(Bytes::from(n.to_string()))
                }
            }
            Value::String(s) => Response::bulk(Bytes::copy_from_slice(s.as_bytes().as_ref())),
            Value::Table(t) => {
                if let Ok(e) = t.get::<String>("err") {
                    return Response::Error(Bytes::from(e));
                }
                if let Ok(o) = t.get::<String>("ok") {
                    return Response::Simple(Bytes::from(o));
                }
                let mut a = Vec::new();
                let mut i: i64 = 1;
                loop {
                    match t.get::<Value>(i) {
                        Ok(v) if !matches!(v, Value::Nil) => {
                            a.push(self.lua_to_response(v));
                            i += 1;
                        }
                        _ => break,
                    }
                }
                if a.is_empty() {
                    Response::Null
                } else {
                    Response::Array(a)
                }
            }
            _ => Response::Null,
        }
    }

    pub fn load_script(&mut self, source: Bytes) -> String {
        let sha = self.cache.load(source);
        let s = sha_to_hex(&sha);
        debug!(script_sha = %s, "Script cached");
        s
    }
    pub fn scripts_exist(&self, shas: &[&[u8]]) -> Vec<bool> {
        shas.iter()
            .map(|h| {
                hex_to_sha(h)
                    .map(|s| self.cache.exists(&s))
                    .unwrap_or(false)
            })
            .collect()
    }
    pub fn flush_scripts(&mut self) {
        let n = self.cache.len();
        self.cache.flush();
        info!(scripts_removed = n, "Script cache flushed");
    }
    pub fn kill_script(&self) -> Result<(), ScriptError> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::Internal("No script running".into()));
        }
        self.vm.request_kill()
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.cache.len(), self.cache.current_bytes())
    }

    /// Execute a function from a library (FCALL / FCALL_RO).
    #[allow(clippy::too_many_arguments)]
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
        if self.running.load(Ordering::Relaxed) {
            return Err(ScriptError::NestedScript);
        }
        self.running.store(true, Ordering::Relaxed);
        self.vm.prepare_execution(keys, argv)?;
        let sp: *mut dyn crate::store::Store =
            unsafe { std::mem::transmute(ctx.store as *mut dyn crate::store::Store) };
        let cec = CommandExecutionContext {
            store_ptr: sp,
            registry_ptr: registry as *const CommandRegistry,
            shard_senders: Arc::clone(ctx.shard_senders),
            shard_id: ctx.shard_id,
            num_shards: ctx.num_shards,
            conn_id: ctx.conn_id,
            protocol_version: ctx.protocol_version,
        };
        self.vm.set_command_context(cec);
        self.setup_redis_bindings(keys, read_only, &ScriptKeyContext::default())?;
        let lcc = if library_code.starts_with("#!") {
            library_code
                .find('\n')
                .map(|p| &library_code[p + 1..])
                .unwrap_or("")
        } else {
            library_code
        };
        let fnl = function_name.to_ascii_lowercase();
        self.setup_function_registration()?;
        self.vm.execute(lcc.as_bytes()).inspect_err(|_| {
            self.vm.clear_command_context();
            self.vm.cleanup_execution();
            self.running.store(false, Ordering::Relaxed);
        })?;
        self.lock_runtime_environment()?;
        let ic = format!(
            "local fn = __frogdb_functions and __frogdb_functions[\"{fnl}\"]\nif not fn then\n    error(\"Function not found after loading library: {function_name}\")\nend\nreturn fn(KEYS, ARGV)\n"
        );
        let result = self.vm.execute(ic.as_bytes());
        if read_only && self.vm.has_writes() {
            self.vm.clear_command_context();
            self.vm.cleanup_execution();
            self.running.store(false, Ordering::Relaxed);
            return Err(ScriptError::Runtime(
                "Write commands are not allowed from read-only scripts".into(),
            ));
        }
        self.vm.clear_command_context();
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);
        match result {
            Ok(v) => Ok(self.lua_to_response(v)),
            Err(e) => Err(e),
        }
    }

    fn setup_function_registration(&self) -> Result<(), ScriptError> {
        let lua = self.vm.lua();
        let g = lua.globals();
        let ft = lua
            .create_table()
            .map_err(|e| ScriptError::Internal(format!("Failed to create functions table: {e}")))?;
        g.raw_set("__frogdb_functions", ft)
            .map_err(|e| ScriptError::Internal(format!("Failed to set functions table: {e}")))?;
        let rt: mlua::Table = g
            .get("redis")
            .map_err(|e| ScriptError::Internal(format!("Failed to get redis table: {e}")))?;
        let rf = lua
            .create_function(|lc, args: MultiValue| -> mlua::Result<()> {
                let av: Vec<Value> = args.into_iter().collect();
                if av.is_empty() {
                    return Err(mlua::Error::RuntimeError(
                        "redis.register_function requires at least one argument".into(),
                    ));
                }
                let g = lc.globals();
                let ft: mlua::Table = g
                    .get("__frogdb_functions")
                    .map_err(|_| mlua::Error::RuntimeError("Functions table not found".into()))?;
                match &av[0] {
                    Value::Table(t) => {
                        let n: String = t.get("function_name").map_err(|_| {
                            mlua::Error::RuntimeError(
                                "Missing required field 'function_name'".into(),
                            )
                        })?;
                        let cb: mlua::Function = t.get("callback").map_err(|_| {
                            mlua::Error::RuntimeError("Missing required field 'callback'".into())
                        })?;
                        ft.set(n.to_ascii_lowercase(), cb)?;
                    }
                    Value::String(n) => {
                        if av.len() < 2 {
                            return Err(mlua::Error::RuntimeError(
                                "redis.register_function requires a callback function".into(),
                            ));
                        }
                        let cb = match &av[1] {
                            Value::Function(f) => f.clone(),
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "Second argument must be a function".into(),
                                ));
                            }
                        };
                        ft.set(n.to_str()?.to_ascii_lowercase(), cb)?;
                    }
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "First argument must be a string or table".into(),
                        ));
                    }
                }
                Ok(())
            })
            .map_err(|e| {
                ScriptError::Internal(format!("Failed to create register_function: {e}"))
            })?;
        rt.raw_set("register_function", rf)
            .map_err(|e| ScriptError::Internal(format!("Failed to set register_function: {e}")))?;
        Ok(())
    }

    fn lock_runtime_environment(&self) -> Result<(), ScriptError> {
        let lua = self.vm.lua();
        let g = lua.globals();
        let rt: mlua::Table = g
            .get("redis")
            .map_err(|e| ScriptError::Internal(format!("Failed to get redis table: {e}")))?;
        let ef = lua
            .create_function(|_, _: MultiValue| -> mlua::Result<()> {
                Err(mlua::Error::RuntimeError(
                    "can not run register_function after library initialization".into(),
                ))
            })
            .map_err(|e| ScriptError::Internal(format!("Failed to create error stub: {e}")))?;
        rt.raw_set("register_function", ef).map_err(|e| {
            ScriptError::Internal(format!("Failed to replace register_function: {e}"))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_executor_creation() {
        assert!(
            !ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .is_running()
        );
    }
    #[test]
    fn test_load_script() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let s1 = e.load_script(Bytes::from_static(b"return 1"));
        let s2 = e.load_script(Bytes::from_static(b"return 1"));
        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 40);
    }
    #[test]
    fn test_scripts_exist() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let s = e.load_script(Bytes::from_static(b"return 1"));
        assert_eq!(
            e.scripts_exist(&[s.as_bytes(), b"nonexistent1234567890123456789012345678"]),
            vec![true, false]
        );
    }
    #[test]
    fn test_flush_scripts() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let s = e.load_script(Bytes::from_static(b"return 1"));
        assert!(e.scripts_exist(&[s.as_bytes()])[0]);
        e.flush_scripts();
        assert!(!e.scripts_exist(&[s.as_bytes()])[0]);
    }
    #[test]
    fn test_lua_to_response_nil() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Nil),
            Response::Null
        ));
    }
    #[test]
    fn test_lua_to_response_integer() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Integer(42)),
            Response::Integer(42)
        ));
    }
    #[test]
    fn test_lua_to_response_false_is_null() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Boolean(false)),
            Response::Null
        ));
    }
    #[test]
    fn test_lua_to_response_true_is_one() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Boolean(true)),
            Response::Integer(1)
        ));
    }
    #[test]
    fn test_parse_shebang_none() {
        assert!(parse_eval_shebang(b"return 1").unwrap().is_none());
    }
    #[test]
    fn test_parse_shebang_basic() {
        let sb = parse_eval_shebang(b"#!lua\nreturn 1").unwrap().unwrap();
        assert!(sb.has_shebang && sb.flags.is_empty());
    }
    #[test]
    fn test_parse_shebang_flags() {
        let sb = parse_eval_shebang(b"#!lua flags=no-writes,no-cluster\nreturn 1")
            .unwrap()
            .unwrap();
        assert!(
            sb.flags.contains(FunctionFlags::NO_WRITES)
                && sb.flags.contains(FunctionFlags::NO_CLUSTER)
        );
    }
    #[test]
    fn test_parse_shebang_bad_engine() {
        assert!(parse_eval_shebang(b"#!python\nreturn 1").is_err());
    }
    #[test]
    fn test_parse_shebang_bad_flag() {
        assert!(parse_eval_shebang(b"#!lua flags=bad\nreturn 1").is_err());
    }
}
