use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

use frogdb_types::metrics::definitions::{
    LuaScriptsCacheHits, LuaScriptsCacheMisses, LuaScriptsDuration, LuaScriptsErrors,
    LuaScriptsTotal,
};
use frogdb_types::metrics::labels::{ScriptError, ScriptKind};

use crate::command::CommandContext;
use crate::store::Store;

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
        let start = Instant::now();
        let shard_label = self.identity.shard_id.to_string();

        // EVAL always loads the script (cache miss)
        LuaScriptsCacheMisses::inc(&*self.observability.metrics_recorder, &shard_label);

        let executor = match &mut self.scripting.executor {
            Some(e) => e,
            None => {
                LuaScriptsErrors::inc(
                    &*self.observability.metrics_recorder,
                    &shard_label,
                    ScriptError::NotAvailable,
                );
                return Response::error("ERR scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.identity.shard_id,
            self.identity.num_shards,
            conn_id,
            protocol_version,
            None,
            self.cluster.cluster_state.as_ref(),
            self.cluster.node_id,
            self.cluster.raft.as_ref(),
            self.cluster.network_factory.as_ref(),
            self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        let is_cluster_mode = self.cluster.cluster_state.is_some();
        let result = executor.eval(
            script_source,
            keys,
            argv,
            &mut ctx,
            &self.registry,
            read_only,
            is_cluster_mode,
        );
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics
        LuaScriptsTotal::inc(
            &*self.observability.metrics_recorder,
            &shard_label,
            ScriptKind::Eval,
        );
        LuaScriptsDuration::observe(
            &*self.observability.metrics_recorder,
            elapsed,
            &shard_label,
            ScriptKind::Eval,
        );

        match result {
            Ok(response) => response,
            Err(e) => {
                LuaScriptsErrors::inc(
                    &*self.observability.metrics_recorder,
                    &shard_label,
                    ScriptError::Execution,
                );
                Response::error(e.to_string())
            }
        }
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
        let start = Instant::now();
        let shard_label = self.identity.shard_id.to_string();

        let executor = match &mut self.scripting.executor {
            Some(e) => e,
            None => {
                LuaScriptsErrors::inc(
                    &*self.observability.metrics_recorder,
                    &shard_label,
                    ScriptError::NotAvailable,
                );
                return Response::error("ERR scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.identity.shard_id,
            self.identity.num_shards,
            conn_id,
            protocol_version,
            None,
            self.cluster.cluster_state.as_ref(),
            self.cluster.node_id,
            self.cluster.raft.as_ref(),
            self.cluster.network_factory.as_ref(),
            self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        let is_cluster_mode = self.cluster.cluster_state.is_some();
        let result = executor.evalsha(
            script_sha,
            keys,
            argv,
            &mut ctx,
            &self.registry,
            read_only,
            is_cluster_mode,
        );
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics based on result
        match &result {
            Ok(_) => {
                // EVALSHA success = cache hit
                LuaScriptsCacheHits::inc(&*self.observability.metrics_recorder, &shard_label);
                LuaScriptsTotal::inc(
                    &*self.observability.metrics_recorder,
                    &shard_label,
                    ScriptKind::Evalsha,
                );
                LuaScriptsDuration::observe(
                    &*self.observability.metrics_recorder,
                    elapsed,
                    &shard_label,
                    ScriptKind::Evalsha,
                );
            }
            Err(e) => {
                // Check if it's a NOSCRIPT error (cache miss) or execution error
                let error_str = e.to_string();
                if error_str.contains("NOSCRIPT") {
                    LuaScriptsCacheMisses::inc(&*self.observability.metrics_recorder, &shard_label);
                    LuaScriptsErrors::inc(
                        &*self.observability.metrics_recorder,
                        &shard_label,
                        ScriptError::Noscript,
                    );
                } else {
                    // Execution error after cache hit
                    LuaScriptsCacheHits::inc(&*self.observability.metrics_recorder, &shard_label);
                    LuaScriptsErrors::inc(
                        &*self.observability.metrics_recorder,
                        &shard_label,
                        ScriptError::Execution,
                    );
                }
            }
        }

        match result {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// Handle SCRIPT LOAD - load a script into the cache.
    pub(crate) fn handle_script_load(&mut self, script_source: &Bytes) -> String {
        match &mut self.scripting.executor {
            Some(executor) => executor.load_script(script_source.clone()),
            None => String::new(),
        }
    }

    /// Handle SCRIPT EXISTS - check if scripts are cached.
    pub(crate) fn handle_script_exists(&self, shas: &[Bytes]) -> Vec<bool> {
        match &self.scripting.executor {
            Some(executor) => {
                let sha_refs: Vec<&[u8]> = shas.iter().map(|s| s.as_ref()).collect();
                executor.scripts_exist(&sha_refs)
            }
            None => vec![false; shas.len()],
        }
    }

    /// Handle SCRIPT FLUSH - clear the script cache.
    pub(crate) fn handle_script_flush(&mut self) {
        if let Some(ref mut executor) = self.scripting.executor {
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
        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::new(
            store,
            &self.shard_senders,
            self.identity.shard_id,
            self.identity.num_shards,
            conn_id,
            protocol_version,
        );
        match handler.execute(&mut ctx, args) {
            Ok(response) => response,
            Err(err) => Response::error(err.to_string()),
        }
    }

    /// Handle SCRIPT KILL - kill the running script.
    pub(crate) fn handle_script_kill(&self) -> Result<(), String> {
        match &self.scripting.executor {
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
