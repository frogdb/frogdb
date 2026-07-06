use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

use frogdb_types::metrics::definitions::{
    LuaScriptsCacheHits, LuaScriptsCacheMisses, LuaScriptsDuration, LuaScriptsErrors,
    LuaScriptsTotal,
};
use frogdb_types::metrics::labels::{ScriptError as ScriptErrorLabel, ScriptKind};

use crate::command::CommandContext;
use crate::registry::CommandRegistry;
use crate::scripting::{CacheDisposition, ScriptExecutor, ScriptOutcome};

use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle EVAL / EVAL_RO - execute a Lua script.
    pub(crate) fn handle_eval_script(
        &mut self,
        script_source: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
        read_only: bool,
    ) -> Response {
        let is_cluster_mode = self.cluster.is_cluster_mode();
        self.run_script(
            ScriptKind::Eval,
            conn_id,
            protocol_version,
            |executor, ctx, registry| {
                executor.eval(
                    script_source,
                    keys,
                    argv,
                    ctx,
                    registry,
                    read_only,
                    is_cluster_mode,
                )
            },
        )
    }

    /// Handle EVALSHA / EVALSHA_RO - execute a cached Lua script by SHA.
    pub(crate) fn handle_evalsha(
        &mut self,
        script_sha: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
        read_only: bool,
    ) -> Response {
        let is_cluster_mode = self.cluster.is_cluster_mode();
        self.run_script(
            ScriptKind::Evalsha,
            conn_id,
            protocol_version,
            |executor, ctx, registry| {
                executor.evalsha(
                    script_sha,
                    keys,
                    argv,
                    ctx,
                    registry,
                    read_only,
                    is_cluster_mode,
                )
            },
        )
    }

    /// Shared EVAL/EVALSHA execution path.
    ///
    /// Builds the connection context, invokes the executor via `invoke`, and
    /// records the `frogdb_lua_scripts_*` metrics from the executor's typed
    /// [`ScriptOutcome`] — cache hit/miss comes from `outcome.disposition`
    /// and the error label from `ScriptError::metric_label()`, never from
    /// matching the formatted error string. This is the single place that
    /// owns cache-disposition + metric emission for both handlers above, so
    /// they only need to build the ctx and call into the executor.
    fn run_script(
        &mut self,
        kind: ScriptKind,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        invoke: impl FnOnce(&mut ScriptExecutor, &mut CommandContext, &CommandRegistry) -> ScriptOutcome,
    ) -> Response {
        let shard_label = self.identity.shard_id.to_string();

        if !self.scripting.has_executor() {
            LuaScriptsErrors::inc(
                self.observability.metrics(),
                &shard_label,
                ScriptErrorLabel::NotAvailable,
            );
            return Response::error("ERR scripting not available");
        }

        let start = Instant::now();
        // Clone the registry Arc and move the executor out so that `self` is free
        // for the `command_context` builder (which borrows `&mut self`).
        let registry = std::sync::Arc::clone(&self.registry);
        let mut executor = self
            .scripting
            .take_executor()
            .expect("executor presence checked above");
        let outcome = {
            let mut ctx = self.command_context(conn_id, protocol_version);
            invoke(&mut executor, &mut ctx, &registry)
        };
        self.scripting.set_executor(executor);
        let elapsed = start.elapsed().as_secs_f64();

        match outcome.disposition {
            CacheDisposition::Hit => {
                LuaScriptsCacheHits::inc(self.observability.metrics(), &shard_label)
            }
            CacheDisposition::Miss => {
                LuaScriptsCacheMisses::inc(self.observability.metrics(), &shard_label)
            }
        }
        LuaScriptsTotal::inc(self.observability.metrics(), &shard_label, kind);
        LuaScriptsDuration::observe(self.observability.metrics(), elapsed, &shard_label, kind);

        match outcome.result {
            Ok(response) => response,
            Err(e) => {
                LuaScriptsErrors::inc(self.observability.metrics(), &shard_label, e.metric_label());
                Response::error(e.to_string())
            }
        }
    }

    /// Handle SCRIPT LOAD - load a script into the cache.
    pub(crate) fn handle_script_load(&mut self, script_source: &Bytes) -> String {
        match self.scripting.executor_mut() {
            Some(executor) => executor.load_script(script_source.clone()),
            None => String::new(),
        }
    }

    /// Handle SCRIPT EXISTS - check if scripts are cached.
    pub(crate) fn handle_script_exists(&self, shas: &[Bytes]) -> Vec<bool> {
        match self.scripting.executor() {
            Some(executor) => {
                let sha_refs: Vec<&[u8]> = shas.iter().map(|s| s.as_ref()).collect();
                executor.scripts_exist(&sha_refs)
            }
            None => vec![false; shas.len()],
        }
    }

    /// Handle SCRIPT FLUSH - clear the script cache.
    pub(crate) fn handle_script_flush(&mut self) {
        if let Some(executor) = self.scripting.executor_mut() {
            executor.flush_scripts();
        }
    }

    /// Execute a sub-command dispatched from a Lua script running on another shard.
    pub(crate) fn execute_script_sub_command(
        &mut self,
        parts: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> Response {
        if parts.is_empty() {
            return Response::error("ERR wrong number of arguments for redis command");
        }
        let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();
        let handler = match self.registry.get(&cmd_name) {
            Some(h) => h,
            None => return Response::error(format!("ERR unknown command '{}'", cmd_name)),
        };
        let args = &parts[1..];
        if !handler.arity().check(args.len()) {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name().to_ascii_lowercase()
            ));
        }
        // Route through the shared builder so a cross-shard script sub-command
        // sees the same cluster + replica identity as any other command on this
        // shard (previously it used the bare `new` constructor).
        let mut ctx = self.command_context(conn_id, protocol_version);
        match handler.execute(&mut ctx, args) {
            Ok(response) => response,
            Err(err) => Response::error(err.to_string()),
        }
    }

    /// Handle SCRIPT KILL - kill the running script.
    pub(crate) fn handle_script_kill(&self) -> Result<(), String> {
        match self.scripting.executor() {
            Some(executor) => {
                if !executor.is_running() {
                    return Err("NOTBUSY No scripts in execution right now.".to_string());
                }
                executor.kill_script().map_err(|e| e.to_string())
            }
            None => Err("ERR scripting not available".to_string()),
        }
    }
}
