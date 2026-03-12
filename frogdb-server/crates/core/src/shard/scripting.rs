use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

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
        self.observability.metrics_recorder.increment_counter(
            "frogdb_lua_scripts_cache_misses_total",
            1,
            &[("shard", &shard_label)],
        );

        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_errors_total",
                    1,
                    &[("shard", &shard_label), ("error", "not_available")],
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
            None,
            self.cluster.cluster_state.as_ref(),
            self.cluster.node_id,
            self.cluster.raft.as_ref(),
            self.cluster.network_factory.as_ref(),
            self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        let result = executor.eval(
            script_source,
            keys,
            argv,
            &mut ctx,
            &self.registry,
            read_only,
        );
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics
        self.observability.metrics_recorder.increment_counter(
            "frogdb_lua_scripts_total",
            1,
            &[("shard", &shard_label), ("type", "eval")],
        );
        self.observability.metrics_recorder.record_histogram(
            "frogdb_lua_scripts_duration_seconds",
            elapsed,
            &[("shard", &shard_label), ("type", "eval")],
        );

        match result {
            Ok(response) => response,
            Err(e) => {
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_errors_total",
                    1,
                    &[("shard", &shard_label), ("error", "execution")],
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

        let executor = match &mut self.script_executor {
            Some(e) => e,
            None => {
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_errors_total",
                    1,
                    &[("shard", &shard_label), ("error", "not_available")],
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
            None,
            self.cluster.cluster_state.as_ref(),
            self.cluster.node_id,
            self.cluster.raft.as_ref(),
            self.cluster.network_factory.as_ref(),
            self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        let result = executor.evalsha(script_sha, keys, argv, &mut ctx, &self.registry, read_only);
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics based on result
        match &result {
            Ok(_) => {
                // EVALSHA success = cache hit
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_cache_hits_total",
                    1,
                    &[("shard", &shard_label)],
                );
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_lua_scripts_total",
                    1,
                    &[("shard", &shard_label), ("type", "evalsha")],
                );
                self.observability.metrics_recorder.record_histogram(
                    "frogdb_lua_scripts_duration_seconds",
                    elapsed,
                    &[("shard", &shard_label), ("type", "evalsha")],
                );
            }
            Err(e) => {
                // Check if it's a NOSCRIPT error (cache miss) or execution error
                let error_str = e.to_string();
                if error_str.contains("NOSCRIPT") {
                    self.observability.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_cache_misses_total",
                        1,
                        &[("shard", &shard_label)],
                    );
                    self.observability.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_errors_total",
                        1,
                        &[("shard", &shard_label), ("error", "noscript")],
                    );
                } else {
                    // Execution error after cache hit
                    self.observability.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_cache_hits_total",
                        1,
                        &[("shard", &shard_label)],
                    );
                    self.observability.metrics_recorder.increment_counter(
                        "frogdb_lua_scripts_errors_total",
                        1,
                        &[("shard", &shard_label), ("error", "execution")],
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
        match &mut self.script_executor {
            Some(executor) => executor.load_script(script_source.clone()),
            None => String::new(),
        }
    }

    /// Handle SCRIPT EXISTS - check if scripts are cached.
    pub(crate) fn handle_script_exists(&self, shas: &[Bytes]) -> Vec<bool> {
        match &self.script_executor {
            Some(executor) => {
                let sha_refs: Vec<&[u8]> = shas.iter().map(|s| s.as_ref()).collect();
                executor.scripts_exist(&sha_refs)
            }
            None => vec![false; shas.len()],
        }
    }

    /// Handle SCRIPT FLUSH - clear the script cache.
    pub(crate) fn handle_script_flush(&mut self) {
        if let Some(ref mut executor) = self.script_executor {
            executor.flush_scripts();
        }
    }

    /// Handle SCRIPT KILL - kill the running script.
    pub(crate) fn handle_script_kill(&self) -> Result<(), String> {
        match &self.script_executor {
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
