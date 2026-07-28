//! Script executor — orchestrates Lua script execution with shebang support.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use frogdb_scripting::FunctionFlags;
use mlua::{MultiValue, Value};
use tracing::{debug, info, warn};

use super::cache::{ScriptCache, ScriptSha, hex_to_sha, sha_to_hex};
use super::config::ScriptingConfig;
use super::error::ScriptError;
use super::gate::{CrossSlotTracker, ScriptInvoker};
use super::lua_vm::LuaVm;
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

/// Whether an EVAL/EVALSHA invocation was served from the script cache.
///
/// This is derived from typed executor state (the cache lookup itself, or a
/// [`ScriptError::NoScript`]) — never by matching the formatted error
/// string, so a reworded message can't silently flip the
/// `frogdb_lua_scripts_cache_*` counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheDisposition {
    /// The script was already resident in the cache.
    Hit,
    /// The script had to be (re)loaded — always true for EVAL, and true for
    /// EVALSHA exactly when the SHA was not cached (`ScriptError::NoScript`).
    Miss,
}

/// Outcome of an EVAL/EVALSHA invocation: the script result plus the cache
/// disposition the caller needs to drive the `frogdb_lua_scripts_cache_*`
/// metrics, without re-deriving it from `result`.
#[derive(Debug)]
pub struct ScriptOutcome {
    pub disposition: CacheDisposition,
    pub result: Result<Response, ScriptError>,
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
    ///
    /// EVAL always (re)loads the script into the cache, so the disposition
    /// is unconditionally [`CacheDisposition::Miss`] regardless of whether
    /// execution itself succeeds.
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
    ) -> ScriptOutcome {
        let result = self.eval_result(
            source,
            keys,
            argv,
            ctx,
            registry,
            read_only,
            is_cluster_mode,
        );
        ScriptOutcome {
            disposition: CacheDisposition::Miss,
            result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn eval_result(
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
        if let Some(ref sb) = shebang
            && sb.flags.contains(FunctionFlags::NO_CLUSTER)
            && is_cluster_mode
        {
            return Err(ScriptError::Runtime(
                "ERR Can not run script on cluster, 'no-cluster' flag is set".into(),
            ));
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
    ///
    /// The disposition is [`CacheDisposition::Miss`] exactly when the SHA
    /// was not resident in the cache (surfaced as [`ScriptError::NoScript`]);
    /// any other outcome — successful execution or an execution-time error —
    /// is a [`CacheDisposition::Hit`], since the script *was* found.
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
    ) -> ScriptOutcome {
        let result = self.evalsha_result(
            sha_hex,
            keys,
            argv,
            ctx,
            registry,
            read_only,
            is_cluster_mode,
        );
        let disposition = match result {
            Err(ScriptError::NoScript) => CacheDisposition::Miss,
            _ => CacheDisposition::Hit,
        };
        ScriptOutcome {
            disposition,
            result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evalsha_result(
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
        if let Some(ref sb) = shebang
            && sb.flags.contains(FunctionFlags::NO_CLUSTER)
            && is_cluster_mode
        {
            return Err(ScriptError::Runtime(
                "ERR Can not run script on cluster, 'no-cluster' flag is set".into(),
            ));
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

        // Cross-slot enforcement policy: seed the shared accumulator with the
        // first declared key's slot when active, so sub-command keys must hash
        // to the same slot as the script's declared keys.
        let enforce_cross_slot = skc.enforce_cross_slot();
        let seed = if enforce_cross_slot && !keys.is_empty() {
            Some(slot_for_key(&keys[0]))
        } else {
            None
        };
        let pv = ctx.protocol_version;

        // The invoker owns a real `&mut` store borrow for the scope's duration;
        // `redis.call` re-enters through it safely — no aliased pointer.
        let invoker = ScriptInvoker::from_context(ctx, registry);
        let result = self.vm.execute_in_scope(
            &invoker,
            read_only,
            enforce_cross_slot,
            CrossSlotTracker::new(seed),
            |vm| vm.execute(source),
        );

        let wrote_in_readonly = read_only && self.vm.has_writes();
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);
        if wrote_in_readonly {
            return Err(ScriptError::Runtime(
                "Write commands are not allowed from read-only scripts".into(),
            ));
        }
        let dur = start.elapsed().as_millis() as u64;
        match result {
            Ok(v) => {
                debug!(script_sha = %script_sha, duration_ms = dur, "Script executed");
                Ok(self.lua_to_response(v, pv))
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

    fn lua_to_response(&self, value: Value, protocol_version: ProtocolVersion) -> Response {
        match value {
            Value::Nil => Response::Null,
            Value::Boolean(false) => {
                if protocol_version.is_resp3() {
                    Response::Boolean(false)
                } else {
                    Response::Null
                }
            }
            Value::Boolean(true) => {
                if protocol_version.is_resp3() {
                    Response::Boolean(true)
                } else {
                    Response::Integer(1)
                }
            }
            Value::Integer(n) => Response::Integer(n),
            Value::Number(n) => {
                if protocol_version.is_resp3() {
                    Response::Double(n)
                } else {
                    Response::Integer(n as i64)
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

                // In RESP3, detect map-like tables (non-sequential keys).
                if protocol_version.is_resp3() {
                    // First try sequential integer keys (array-like).
                    let mut a = Vec::new();
                    let mut i: i64 = 1;
                    loop {
                        match t.get::<Value>(i) {
                            Ok(v) if !matches!(v, Value::Nil) => {
                                a.push(self.lua_to_response(v, protocol_version));
                                i += 1;
                            }
                            _ => break,
                        }
                    }
                    if !a.is_empty() {
                        return Response::Array(a);
                    }

                    // No sequential keys found - try as a map.
                    let mut pairs = Vec::new();
                    for (k, v) in t.pairs::<Value, Value>().flatten() {
                        pairs.push((
                            self.lua_to_response(k, protocol_version),
                            self.lua_to_response(v, protocol_version),
                        ));
                    }

                    if pairs.is_empty() {
                        Response::Null
                    } else {
                        Response::Map(pairs)
                    }
                } else {
                    // RESP2: original behavior
                    let mut a = Vec::new();
                    let mut i: i64 = 1;
                    loop {
                        match t.get::<Value>(i) {
                            Ok(v) if !matches!(v, Value::Nil) => {
                                a.push(self.lua_to_response(v, protocol_version));
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

        let lcc = if library_code.starts_with("#!") {
            library_code
                .find('\n')
                .map(|p| &library_code[p + 1..])
                .unwrap_or("")
        } else {
            library_code
        };
        let fnl = function_name.to_ascii_lowercase();
        let ic = format!(
            "local fn = __frogdb_functions and __frogdb_functions[\"{fnl}\"]\nif not fn then\n    error(\"Function not found after loading library: {function_name}\")\nend\nreturn fn(KEYS, ARGV)\n"
        );
        let pv = ctx.protocol_version;

        // The invoker owns a real `&mut` store borrow for the scope's duration;
        // library load + function invocation both run through the safe seam.
        let invoker = ScriptInvoker::from_context(ctx, registry);
        let result = self.vm.execute_in_scope(
            &invoker,
            read_only,
            false,
            CrossSlotTracker::new(None),
            |vm| {
                // register_function is only callable during library load; the
                // runtime environment is locked before the function is invoked.
                self.setup_function_registration()?;
                vm.execute(lcc.as_bytes())?;
                self.lock_runtime_environment()?;
                vm.execute(ic.as_bytes())
            },
        );

        let wrote_in_readonly = read_only && self.vm.has_writes();
        self.vm.cleanup_execution();
        self.running.store(false, Ordering::Relaxed);
        if wrote_in_readonly {
            return Err(ScriptError::Runtime(
                "Write commands are not allowed from read-only scripts".into(),
            ));
        }
        match result {
            Ok(v) => Ok(self.lua_to_response(v, pv)),
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
    use crate::scripting::cache::compute_sha;
    use crate::store::Store;
    use std::sync::Arc;
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
                .lua_to_response(Value::Nil, ProtocolVersion::Resp2),
            Response::Null
        ));
    }
    #[test]
    fn test_lua_to_response_integer() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Integer(42), ProtocolVersion::Resp2),
            Response::Integer(42)
        ));
    }
    #[test]
    fn test_lua_to_response_false_is_null() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Boolean(false), ProtocolVersion::Resp2),
            Response::Null
        ));
    }
    #[test]
    fn test_lua_to_response_true_is_one() {
        assert!(matches!(
            ScriptExecutor::new(ScriptingConfig::default())
                .unwrap()
                .lua_to_response(Value::Boolean(true), ProtocolVersion::Resp2),
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

    // ---- Cache-disposition accounting -------------------------------------
    //
    // These pin `eval`/`evalsha`'s `CacheDisposition` to typed executor
    // state (a cache lookup, or `ScriptError::NoScript`) so a reworded error
    // message can never silently flip the `frogdb_lua_scripts_cache_*`
    // metrics the shard derives from it — the bug this refactor removes.

    /// Build a `CommandContext` over a throwaway store, for driving
    /// `eval`/`evalsha` directly without a running shard.
    fn test_command_context<'a>(
        store: &'a mut crate::store::HashMapStore,
        shard_senders: &'a Arc<Vec<crate::shard::ShardSender>>,
    ) -> CommandContext<'a> {
        CommandContext::new(store, shard_senders, 0, 1, 1, ProtocolVersion::Resp2)
    }

    #[test]
    fn evalsha_of_uncached_sha_is_noscript_miss() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let registry = crate::registry::CommandRegistry::new();
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let outcome = e.evalsha(
            b"0000000000000000000000000000000000000000",
            &[],
            &[],
            &mut ctx,
            &registry,
            false,
            false,
        );

        assert_eq!(outcome.disposition, CacheDisposition::Miss);
        assert!(
            matches!(outcome.result, Err(ScriptError::NoScript)),
            "expected NoScript, got {:?}",
            outcome.result
        );
    }

    #[test]
    fn eval_is_always_a_miss_even_on_success() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let registry = crate::registry::CommandRegistry::new();
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let outcome = e.eval(b"return 1", &[], &[], &mut ctx, &registry, false, false);

        assert_eq!(outcome.disposition, CacheDisposition::Miss);
        assert!(outcome.result.is_ok());
    }

    #[test]
    fn evalsha_after_eval_is_a_hit() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let registry = crate::registry::CommandRegistry::new();
        let source: &[u8] = b"return 1";

        {
            let mut ctx = test_command_context(&mut store, &shard_senders);
            let outcome = e.eval(source, &[], &[], &mut ctx, &registry, false, false);
            assert_eq!(outcome.disposition, CacheDisposition::Miss);
            assert!(outcome.result.is_ok());
        }

        let sha = sha_to_hex(&compute_sha(source));
        let mut ctx = test_command_context(&mut store, &shard_senders);
        let outcome = e.evalsha(sha.as_bytes(), &[], &[], &mut ctx, &registry, false, false);

        assert_eq!(outcome.disposition, CacheDisposition::Hit);
        assert!(outcome.result.is_ok());
    }

    #[test]
    fn evalsha_execution_error_after_cache_hit_is_still_a_hit() {
        // A script that is found in the cache but errors during execution is
        // still a `Hit` — only a missing SHA (`NoScript`) is a `Miss`.
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let registry = crate::registry::CommandRegistry::new();
        let source: &[u8] = b"error('boom')";

        {
            let mut ctx = test_command_context(&mut store, &shard_senders);
            let outcome = e.eval(source, &[], &[], &mut ctx, &registry, false, false);
            assert_eq!(outcome.disposition, CacheDisposition::Miss);
            assert!(outcome.result.is_err());
        }

        let sha = sha_to_hex(&compute_sha(source));
        let mut ctx = test_command_context(&mut store, &shard_senders);
        let outcome = e.evalsha(sha.as_bytes(), &[], &[], &mut ctx, &registry, false, false);

        assert_eq!(outcome.disposition, CacheDisposition::Hit);
        assert!(outcome.result.is_err());
    }

    #[test]
    fn script_error_metric_label_is_typed_not_string_matched() {
        // NoScript maps to the `noscript` label; everything else (including
        // messages that happen to *mention* "NOSCRIPT") maps to `execution`.
        assert_eq!(ScriptError::NoScript.metric_label().as_str(), "noscript");
        assert_eq!(
            ScriptError::Runtime("NOSCRIPT lookalike message".into())
                .metric_label()
                .as_str(),
            "execution"
        );
    }

    // ---- Re-entrancy through the safe seam (end-to-end) --------------------
    //
    // These drive a real Lua `redis.call` all the way through the new
    // `mlua`-scope seam: Lua closure -> ScriptCommandGate -> ScriptInvoker ->
    // handler -> live store -> back to Lua. No transmuted store pointer.

    /// Writes `n = "v1"` into the live store; a write command.
    struct SeamSet;
    impl crate::command::Command for SeamSet {
        fn spec(&self) -> &'static crate::command_spec::CommandSpec {
            use crate::command::{Arity, CommandFlags, WaiterWake, WalStrategy};
            use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
            static SPEC: CommandSpec = CommandSpec {
                name: "__SEAMSET",
                arity: Arity::Fixed(0),
                flags: CommandFlags::WRITE,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, crate::error::CommandError> {
            ctx.store
                .set(Bytes::from_static(b"n"), crate::types::Value::string("v1"));
            Ok(Response::Simple(Bytes::from_static(b"OK")))
        }
    }

    /// Reads `n` from the live store and returns it as a bulk reply.
    struct SeamGet;
    impl crate::command::Command for SeamGet {
        fn spec(&self) -> &'static crate::command_spec::CommandSpec {
            use crate::command::{Arity, CommandFlags, WaiterWake, WalStrategy};
            use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
            static SPEC: CommandSpec = CommandSpec {
                name: "__SEAMGET",
                arity: Arity::Fixed(0),
                flags: CommandFlags::READONLY,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, crate::error::CommandError> {
            match ctx.store.get(b"n") {
                Some(_) => Ok(Response::bulk(Bytes::from_static(b"v1"))),
                None => Ok(Response::Null),
            }
        }
    }

    #[test]
    fn eval_reentrant_redis_call_hits_live_store_through_scope() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let mut registry = crate::registry::CommandRegistry::new();
        registry.register(SeamSet);
        registry.register(SeamGet);
        let mut ctx = test_command_context(&mut store, &shard_senders);

        // A single script that writes then reads back through re-entrant
        // redis.call — both hops go through the scoped seam against the same
        // live store borrow.
        let outcome = e.eval(
            b"redis.call('__SEAMSET'); return redis.call('__SEAMGET')",
            &[],
            &[],
            &mut ctx,
            &registry,
            false,
            false,
        );

        match outcome.result {
            Ok(Response::Bulk(Some(ref v))) => assert_eq!(v.as_ref(), b"v1"),
            other => panic!("re-entrant redis.call must round-trip the store, got {other:?}"),
        }
    }

    #[test]
    fn eval_ro_rejects_write_via_reentrant_call() {
        let mut e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let registry = crate::registry::CommandRegistry::new();
        let mut ctx = test_command_context(&mut store, &shard_senders);

        // read_only = true: a re-entrant write command (SET is a recognized
        // write) must be rejected by the gate before it can reach the store.
        // The rejection travels back out through the scoped `redis.call` seam.
        let outcome = e.eval(
            b"return redis.call('SET', KEYS[1], 'v')",
            &[Bytes::from_static(b"k")],
            &[],
            &mut ctx,
            &registry,
            true,
            false,
        );
        assert!(
            outcome.result.is_err(),
            "read-only EVAL must reject a re-entrant write, got {:?}",
            outcome.result
        );
        // The rejected write must not have touched the live store.
        assert_eq!(
            store.len(),
            0,
            "rejected write must not touch the live store"
        );
    }

    // ---- Undeclared-key slot policy, end-to-end through real Lua -----------
    //
    // These drive real `#!lua` shebang scripts through `eval` with
    // `is_cluster_mode = true`, exercising the exact enforcement path a cluster
    // shard hits: eval -> execute_script (seed the tracker from the first
    // declared/accessed key) -> gate.check_all. They pin the true policy the
    // dead `validate_key_access` never enforced (see gate.rs module docs):
    // undeclared key access is allowed as long as it stays within the seeded
    // slot; a different slot is rejected with Redis's same-slot error.

    /// A read-probe command: reads its single key argument from the live store
    /// and returns it (or nil). Named so the gate's name-based key extraction
    /// falls to the default single-key-at-position-1 branch.
    struct ProbeReadKey;
    impl crate::command::Command for ProbeReadKey {
        fn spec(&self) -> &'static crate::command_spec::CommandSpec {
            use crate::command::{Arity, CommandFlags, WaiterWake, WalStrategy};
            use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
            static SPEC: CommandSpec = CommandSpec {
                name: "PROBEREADKEY",
                arity: Arity::Fixed(1),
                flags: CommandFlags::READONLY,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, crate::error::CommandError> {
            match ctx.store.get(&args[0]) {
                Some(_) => Ok(Response::bulk(Bytes::from_static(b"v"))),
                None => Ok(Response::Null),
            }
        }
    }

    /// Find a key whose slot differs from `reference`'s slot.
    fn key_with_different_slot(reference: &[u8]) -> Bytes {
        let ref_slot = slot_for_key(reference);
        for i in 0u32..1_000_000 {
            let cand = Bytes::from(format!("k{i}"));
            if slot_for_key(&cand) != ref_slot {
                return cand;
            }
        }
        panic!("no key with a slot different from the reference");
    }

    fn probe_executor_and_registry() -> (ScriptExecutor, CommandRegistry) {
        let e = ScriptExecutor::new(ScriptingConfig::default()).unwrap();
        let mut registry = CommandRegistry::new();
        registry.register(ProbeReadKey);
        (e, registry)
    }

    /// Cluster + shebang: `numkeys=1` declaring `{tag}a`, script reads the
    /// UNDECLARED same-slot key `{tag}b` -> succeeds. Undeclared access is fine
    /// within the seeded slot.
    #[test]
    fn eval_cluster_shebang_undeclared_same_slot_key_allowed() {
        let (mut e, registry) = probe_executor_and_registry();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let outcome = e.eval(
            b"#!lua\nreturn redis.call('PROBEREADKEY', '{tag}b')",
            &[Bytes::from_static(b"{tag}a")],
            &[],
            &mut ctx,
            &registry,
            false,
            true, // is_cluster_mode
        );
        assert!(
            outcome.result.is_ok(),
            "undeclared same-slot key access must succeed, got {:?}",
            outcome.result
        );
    }

    /// Cluster + shebang: `numkeys=1` declaring `{tag}a`, script reads an
    /// UNDECLARED key in a DIFFERENT slot -> rejected with the same-slot error.
    #[test]
    fn eval_cluster_shebang_undeclared_different_slot_key_rejected() {
        let (mut e, registry) = probe_executor_and_registry();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let other = key_with_different_slot(b"{tag}a");
        let src = format!(
            "#!lua\nreturn redis.call('PROBEREADKEY', '{}')",
            String::from_utf8_lossy(&other)
        );
        let outcome = e.eval(
            src.as_bytes(),
            &[Bytes::from_static(b"{tag}a")],
            &[],
            &mut ctx,
            &registry,
            false,
            true,
        );
        match outcome.result {
            Err(ScriptError::Runtime(msg)) => {
                assert!(
                    msg.contains("do not hash to the same slot"),
                    "expected same-slot error, got: {msg}"
                );
            }
            other => panic!("expected same-slot rejection, got: {other:?}"),
        }
    }

    /// Cluster + shebang, `numkeys=0`: the first ACCESSED key seeds the slot;
    /// a second access in a different slot -> rejected.
    #[test]
    fn eval_cluster_shebang_numkeys0_two_different_slot_keys_rejected() {
        let (mut e, registry) = probe_executor_and_registry();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let first = Bytes::from_static(b"{tag}a");
        let second = key_with_different_slot(&first);
        let src = format!(
            "#!lua\nredis.call('PROBEREADKEY', '{}')\nreturn redis.call('PROBEREADKEY', '{}')",
            String::from_utf8_lossy(&first),
            String::from_utf8_lossy(&second),
        );
        let outcome = e.eval(src.as_bytes(), &[], &[], &mut ctx, &registry, false, true);
        match outcome.result {
            Err(ScriptError::Runtime(msg)) => {
                assert!(
                    msg.contains("do not hash to the same slot"),
                    "expected same-slot error, got: {msg}"
                );
            }
            other => panic!("expected same-slot rejection, got: {other:?}"),
        }
    }

    /// Regression guard: enforcement is gated on `has_shebang`. A plain
    /// (non-shebang) cluster-mode EVAL is NOT slot-enforced — an undeclared
    /// different-slot access routes by its own key (local here) and succeeds.
    /// This pins why wiring a "strict undeclared-key" mode would silently
    /// change behaviour for plain EVAL scripts.
    #[test]
    fn eval_cluster_non_shebang_does_not_enforce_slot() {
        let (mut e, registry) = probe_executor_and_registry();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let other = key_with_different_slot(b"{tag}a");
        let src = format!(
            "return redis.call('PROBEREADKEY', '{}')",
            String::from_utf8_lossy(&other)
        );
        let outcome = e.eval(
            src.as_bytes(),
            &[Bytes::from_static(b"{tag}a")],
            &[],
            &mut ctx,
            &registry,
            false,
            true, // cluster mode, but no shebang -> no enforcement
        );
        assert!(
            outcome.result.is_ok(),
            "non-shebang cluster EVAL must not slot-enforce, got {:?}",
            outcome.result
        );
    }

    /// Standalone shebang script: enforcement is also gated on
    /// `is_cluster_mode`, so a cross-slot access outside cluster mode is
    /// allowed (routes by its own key). Pins the standalone half of the policy.
    #[test]
    fn eval_standalone_shebang_does_not_enforce_slot() {
        let (mut e, registry) = probe_executor_and_registry();
        let mut store = crate::store::HashMapStore::new();
        let shard_senders: Arc<Vec<crate::shard::ShardSender>> = Arc::new(vec![]);
        let mut ctx = test_command_context(&mut store, &shard_senders);

        let other = key_with_different_slot(b"{tag}a");
        let src = format!(
            "#!lua\nreturn redis.call('PROBEREADKEY', '{}')",
            String::from_utf8_lossy(&other)
        );
        let outcome = e.eval(
            src.as_bytes(),
            &[Bytes::from_static(b"{tag}a")],
            &[],
            &mut ctx,
            &registry,
            false,
            false, // standalone -> no enforcement
        );
        assert!(
            outcome.result.is_ok(),
            "standalone shebang must not slot-enforce, got {:?}",
            outcome.result
        );
    }
}
