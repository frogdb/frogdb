//! FCALL/FCALL_RO and FUNCTION LOAD/DELETE/LIST/STATS/DUMP/RESTORE/KILL/HELP handlers.

use bytes::Bytes;
use frogdb_core::{RwLockExt, ScriptingMsg};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::function_store::FunctionStore;
use crate::slot_migration::SlotValidator;

impl ConnectionHandler {
    /// Handle FCALL and FCALL_RO commands.
    pub(crate) async fn handle_fcall(&self, args: &[Bytes], read_only: bool) -> Response {
        let cmd_name = if read_only { "fcall_ro" } else { "fcall" };

        if args.len() < 2 {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd_name
            ));
        }

        // Parse arguments: function numkeys [key ...] [arg ...]
        let function_name = args[0].clone();
        let numkeys_raw = std::str::from_utf8(&args[1]).unwrap_or("");
        let numkeys = match numkeys_raw.parse::<i64>() {
            Ok(n) if n < 0 => return Response::error("ERR Number of keys can't be negative"),
            Ok(n) => n as usize,
            Err(_) => return Response::error("ERR Bad number of keys provided"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine target shard: all keys must live on one shard (the cluster
        // CRC16-slot check already ran upstream). No keys -> shard 0.
        let target_shard = match SlotValidator::same_shard(&keys, self.num_shards) {
            Ok(shard) => shard.unwrap_or(0),
            Err(crossslot) => return crossslot,
        };

        // Send to shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ScriptingMsg::FunctionCall {
            function_name,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            read_only,
            response_tx,
        };

        if self.core.shard_senders[target_shard]
            .send(msg)
            .await
            .is_err()
        {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle FUNCTION command with subcommands.
    pub(crate) async fn handle_function(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "LOAD" => self.handle_function_load(&args[1..]),
            "LIST" => self.handle_function_list(&args[1..]),
            "DELETE" => self.handle_function_delete(&args[1..]),
            "FLUSH" => self.handle_function_flush(&args[1..]),
            "STATS" => self.handle_function_stats(),
            "DUMP" => self.handle_function_dump(),
            "RESTORE" => self.handle_function_restore(&args[1..]),
            "KILL" => self.handle_function_kill().await,
            "HELP" => self.handle_function_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try FUNCTION HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle FUNCTION LOAD [REPLACE] code.
    fn handle_function_load(&self, args: &[Bytes]) -> Response {
        self.mutate_functions("LOAD", args, |store| store.load(args))
    }

    /// Handle FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE].
    #[allow(clippy::vec_init_then_push)]
    fn handle_function_list(&self, args: &[Bytes]) -> Response {
        let mut pattern: Option<&str> = None;
        let mut with_code = false;

        let mut i = 0;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIBRARYNAME" => {
                    if pattern.is_some() {
                        return Response::error(format!(
                            "ERR Unknown argument {}",
                            String::from_utf8_lossy(&args[i])
                        ));
                    }
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR library name argument was not given");
                    }
                    pattern = std::str::from_utf8(&args[i]).ok();
                }
                b"WITHCODE" => {
                    if with_code {
                        return Response::error(format!(
                            "ERR Unknown argument {}",
                            String::from_utf8_lossy(&args[i])
                        ));
                    }
                    with_code = true;
                }
                _ => {
                    return Response::error(format!(
                        "ERR Unknown argument {}",
                        String::from_utf8_lossy(&args[i])
                    ));
                }
            }
            i += 1;
        }

        let registry = match self.admin.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
        let libraries = registry.list_libraries(pattern);

        let mut result = Vec::new();
        for lib in libraries {
            let mut lib_info = vec![
                // library_name
                Response::bulk(Bytes::from_static(b"library_name")),
                Response::bulk(Bytes::from(lib.name.clone())),
                // engine (normalized to uppercase for Redis compatibility)
                Response::bulk(Bytes::from_static(b"engine")),
                Response::bulk(Bytes::from(lib.engine.to_ascii_uppercase())),
                // functions
                Response::bulk(Bytes::from_static(b"functions")),
            ];
            let mut funcs = Vec::new();
            for func in lib.functions.values() {
                let mut func_info = Vec::new();

                func_info.push(Response::bulk(Bytes::from_static(b"name")));
                func_info.push(Response::bulk(Bytes::from(func.name.clone())));

                func_info.push(Response::bulk(Bytes::from_static(b"description")));
                let desc = func.description.clone().unwrap_or_default();
                func_info.push(Response::bulk(Bytes::from(desc)));

                func_info.push(Response::bulk(Bytes::from_static(b"flags")));
                let flags: Vec<Response> = func
                    .flags
                    .to_strings()
                    .into_iter()
                    .map(|f| Response::bulk(Bytes::from(f)))
                    .collect();
                func_info.push(Response::Array(flags));

                funcs.push(Response::Array(func_info));
            }
            lib_info.push(Response::Array(funcs));

            // code (if requested)
            if with_code {
                lib_info.push(Response::bulk(Bytes::from_static(b"library_code")));
                lib_info.push(Response::bulk(Bytes::from(lib.code.clone())));
            }

            result.push(Response::Array(lib_info));
        }

        Response::Array(result)
    }

    /// Handle FUNCTION DELETE library-name.
    fn handle_function_delete(&self, args: &[Bytes]) -> Response {
        self.mutate_functions("DELETE", args, |store| store.delete(args))
    }

    /// Handle FUNCTION FLUSH [ASYNC|SYNC].
    fn handle_function_flush(&self, args: &[Bytes]) -> Response {
        self.mutate_functions("FLUSH", args, |store| store.flush(args))
    }

    /// Handle FUNCTION STATS.
    #[allow(clippy::vec_init_then_push)]
    fn handle_function_stats(&self) -> Response {
        let registry = match self.admin.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
        let stats = registry.stats();

        let mut result = Vec::new();

        // running_script
        result.push(Response::bulk(Bytes::from_static(b"running_script")));
        if let Some(ref running) = stats.running_function {
            let script_info = vec![
                Response::bulk(Bytes::from_static(b"name")),
                Response::bulk(Bytes::from(running.name.clone())),
                Response::bulk(Bytes::from_static(b"command")),
                Response::bulk(Bytes::from_static(b"fcall")),
                Response::bulk(Bytes::from_static(b"duration_ms")),
                Response::Integer(running.duration_ms as i64),
            ];
            result.push(Response::Array(script_info));
        } else {
            result.push(Response::Null);
        }

        // engines
        result.push(Response::bulk(Bytes::from_static(b"engines")));
        let lua_info = vec![
            Response::bulk(Bytes::from_static(b"libraries_count")),
            Response::Integer(stats.library_count as i64),
            Response::bulk(Bytes::from_static(b"functions_count")),
            Response::Integer(stats.function_count as i64),
        ];
        let engines = vec![
            Response::bulk(Bytes::from_static(b"LUA")),
            Response::Array(lua_info),
        ];
        result.push(Response::Array(engines));

        Response::Array(result)
    }

    /// Handle FUNCTION DUMP.
    fn handle_function_dump(&self) -> Response {
        let registry = match self.admin.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
        let dump = frogdb_core::dump_libraries(&registry);
        Response::bulk(Bytes::from(dump))
    }

    /// Handle FUNCTION RESTORE payload [APPEND|REPLACE|FLUSH].
    fn handle_function_restore(&self, args: &[Bytes]) -> Response {
        self.mutate_functions("RESTORE", args, |store| store.restore(args))
    }

    /// Handle FUNCTION HELP.
    fn handle_function_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"FUNCTION <subcommand> [<arg> [value] ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"DELETE <library-name>")),
            Response::bulk(Bytes::from_static(
                b"    Delete a library and all its functions.",
            )),
            Response::bulk(Bytes::from_static(b"DUMP")),
            Response::bulk(Bytes::from_static(
                b"    Return a serialized payload of loaded libraries.",
            )),
            Response::bulk(Bytes::from_static(b"FLUSH [ASYNC|SYNC]")),
            Response::bulk(Bytes::from_static(b"    Delete all libraries.")),
            Response::bulk(Bytes::from_static(b"KILL")),
            Response::bulk(Bytes::from_static(
                b"    Kill a currently running read-only function.",
            )),
            Response::bulk(Bytes::from_static(b"LIST [LIBRARYNAME pattern] [WITHCODE]")),
            Response::bulk(Bytes::from_static(
                b"    List all libraries, optionally filtered by name pattern.",
            )),
            Response::bulk(Bytes::from_static(b"LOAD [REPLACE] <library-code>")),
            Response::bulk(Bytes::from_static(
                b"    Create a new library with the given code.",
            )),
            Response::bulk(Bytes::from_static(
                b"RESTORE <serialized-payload> [APPEND|REPLACE|FLUSH]",
            )),
            Response::bulk(Bytes::from_static(
                b"    Restore libraries from the serialized payload.",
            )),
            Response::bulk(Bytes::from_static(b"STATS")),
            Response::bulk(Bytes::from_static(
                b"    Return information about running scripts and engines.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }

    /// Mutating access to the process-wide registry, shared with the replica
    /// apply path so the two can not diverge (see [`FunctionStore`]).
    fn function_store(&self) -> FunctionStore {
        FunctionStore::new(
            self.admin.function_registry.clone(),
            self.admin.config_manager.clone(),
        )
    }

    /// Run a registry mutation and, if it succeeded, propagate the command
    /// verbatim to replicas.
    ///
    /// Propagation is *after* the local mutation and *only* on success, which is
    /// the same order every write in this server uses: a command that was
    /// refused here must not be replayed on a replica that might accept it.
    /// Redis propagates these four verbatim for the same reason — the library
    /// source is the state, so re-running the command is a faithful description
    /// of the change.
    fn mutate_functions(
        &self,
        subcommand: &'static str,
        args: &[Bytes],
        mutate: impl FnOnce(&FunctionStore) -> Response,
    ) -> Response {
        // A replica's registry is its primary's, applied off the stream. Letting
        // a client write it directly is exactly the divergence propagation was
        // added to close: the library would live on this node only, until the
        // primary's next FUNCTION command overwrote or failed to overwrite it.
        //
        // The generic gate in `guards.rs` cannot do this: it keys off the
        // registry's `CommandFlags::WRITE`, and FUNCTION is one container
        // command whose subcommands split between writes (these four) and reads
        // (LIST/STATS/DUMP/HELP). Redis flags them individually — `function|load`
        // carries `write`, `function|list` does not — and rejects the writers on
        // a read-only replica with exactly this error.
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            return Response::error("READONLY You can't write against a read only replica.");
        }

        // Mutation and propagation happen under one lock so their order can not
        // invert against a concurrent full resync's whole-registry snapshot (see
        // `function_store::propagation_order`).
        let _order = crate::function_store::propagation_order();
        let response = mutate(&self.function_store());
        if matches!(response, Response::Error(_) | Response::BlobError(_)) {
            return response;
        }
        self.propagate_function_command(subcommand, args);
        response
    }

    /// Broadcast `FUNCTION <subcommand> <args...>` on the control channel.
    ///
    /// Untagged ([`frogdb_replication::CONTROL_SHARD`]) because the function
    /// registry is process-wide, not per-shard: routing it to a shard would make
    /// a replica's libraries depend on which shard the frame happened to carry,
    /// and on the two nodes having the same shard count.
    fn propagate_function_command(&self, subcommand: &'static str, args: &[Bytes]) {
        let Some(handler) = self.cluster.primary_replication_handler.as_ref() else {
            return;
        };
        let mut frame_args = Vec::with_capacity(args.len() + 1);
        frame_args.push(Bytes::from_static(subcommand.as_bytes()));
        frame_args.extend_from_slice(args);
        handler.broadcast_control_command("FUNCTION", &frame_args);
    }

    /// Handle FUNCTION KILL - terminate a running read-only function.
    ///
    /// FUNCTION KILL uses the same mechanism as SCRIPT KILL since functions
    /// execute using the same Lua script executor. It will only kill functions
    /// that were called via FCALL_RO (read-only execution).
    async fn handle_function_kill(&self) -> Response {
        // Send ScriptKill to every shard (only one can be running a script at a
        // time per shard) and stop at the first decisive reply — a kill or a
        // hard error — skipping NOTBUSY / dropped / timed-out shards. The
        // per-shard await that once had no timeout is bounded structurally inside
        // `find_first`, so this can no longer hang on a wedged ShardWorker
        // (mirrors SCRIPT KILL). The three-way NOTBUSY / UNKILLABLE / OK
        // precedence is classified here at the call site, unchanged.
        match self
            .scatter_gather()
            .find_first(
                |_shard, response_tx| ScriptingMsg::ScriptKill { response_tx },
                |reply| !matches!(reply, Err(e) if e.contains("NOTBUSY")),
            )
            .await
        {
            Some(Ok(())) => Response::ok(),
            Some(Err(e)) if e.contains("UNKILLABLE") => {
                Response::error("UNKILLABLE The busy script was not running in read-only mode.")
            }
            _ => Response::error("NOTBUSY No scripts in execution right now."),
        }
    }
}
