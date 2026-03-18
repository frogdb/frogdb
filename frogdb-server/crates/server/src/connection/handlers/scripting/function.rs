//! FCALL/FCALL_RO and FUNCTION LOAD/DELETE/LIST/STATS/DUMP/RESTORE/KILL/HELP handlers.

use bytes::Bytes;
use frogdb_core::{RwLockExt, ShardMessage, shard_for_key};
use frogdb_protocol::Response;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::ConnectionHandler;

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

        // Determine target shard
        let target_shard = if keys.is_empty() {
            0 // No keys -> shard 0
        } else {
            let first_shard = shard_for_key(&keys[0], self.num_shards);
            // Check all keys hash to same shard
            for key in &keys[1..] {
                if shard_for_key(key, self.num_shards) != first_shard {
                    return Response::error(
                        "CROSSSLOT Keys in request don't hash to the same slot",
                    );
                }
            }
            first_shard
        };

        // Send to shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::FunctionCall {
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
            "LOAD" => self.handle_function_load(&args[1..]).await,
            "LIST" => self.handle_function_list(&args[1..]),
            "DELETE" => self.handle_function_delete(&args[1..]),
            "FLUSH" => self.handle_function_flush(&args[1..]),
            "STATS" => self.handle_function_stats(),
            "DUMP" => self.handle_function_dump(),
            "RESTORE" => self.handle_function_restore(&args[1..]).await,
            "KILL" => self.handle_function_kill().await,
            "HELP" => self.handle_function_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try FUNCTION HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle FUNCTION LOAD [REPLACE] code.
    async fn handle_function_load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|load' command");
        }

        // Check for REPLACE option
        let (replace, code) = if args.len() == 1 {
            (false, &args[0])
        } else if args.len() == 2 && args[0].to_ascii_uppercase() == b"REPLACE".as_slice() {
            (true, &args[1])
        } else {
            return Response::error("ERR Unknown option given");
        };

        let code_str = match std::str::from_utf8(code) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR library code must be valid UTF-8"),
        };

        // Load the library
        let library = match frogdb_core::load_library(code_str) {
            Ok(lib) => lib,
            Err(e) => return Response::error(e.to_string()),
        };

        let library_name = library.name.clone();

        // Register in the global registry
        {
            let mut registry = match self.admin.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry.load_library(library, replace) {
                Ok(_) => {}
                Err(frogdb_core::FunctionError::LibraryAlreadyExists { name }) => {
                    return Response::error(format!("ERR Library '{}' already exists", name));
                }
                Err(e) => return Response::error(e.to_string()),
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::bulk(Bytes::from(library_name))
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
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|delete' command");
        }

        let library_name = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR library name must be valid UTF-8"),
        };

        {
            let mut registry = match self.admin.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry.delete_library(library_name) {
                Ok(()) => {}
                Err(e) => return Response::error(e.to_string()),
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::ok()
    }

    /// Handle FUNCTION FLUSH [ASYNC|SYNC].
    fn handle_function_flush(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC|SYNC argument (we ignore it for now)
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            if mode.as_slice() != b"ASYNC" && mode.as_slice() != b"SYNC" {
                return Response::error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
            }
            if args.len() > 1 {
                return Response::error(
                    "ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
                );
            }
        }

        {
            let mut registry = match self.admin.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            registry.flush();
        }

        // Persist to disk (empty state)
        self.persist_functions();

        Response::ok()
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
    async fn handle_function_restore(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|restore' command");
        }

        let payload = &args[0];

        if args.len() > 2 {
            return Response::error(
                "ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
            );
        }

        let policy = if args.len() > 1 {
            match frogdb_core::RestorePolicy::from_str(&String::from_utf8_lossy(&args[1])) {
                Ok(p) => p,
                Err(e) => return Response::error(e.to_string()),
            }
        } else {
            frogdb_core::RestorePolicy::Append
        };

        // Parse the dump
        let libraries = match frogdb_core::restore_libraries(payload) {
            Ok(libs) => libs,
            Err(e) => return Response::error(e.to_string()),
        };

        // Apply based on policy
        {
            let mut registry = match self.admin.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };

            if policy == frogdb_core::RestorePolicy::Flush {
                registry.flush();
            }

            let replace = policy == frogdb_core::RestorePolicy::Replace;

            for (name, code) in libraries {
                let library = match frogdb_core::load_library(&code) {
                    Ok(lib) => lib,
                    Err(e) => {
                        return Response::error(format!(
                            "ERR Failed to load library '{}': {}",
                            name, e
                        ));
                    }
                };

                if let Err(e) = registry.load_library(library, replace) {
                    return Response::error(format!(
                        "ERR Failed to restore library '{}': {}",
                        name, e
                    ));
                }
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::ok()
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

    /// Persist functions to disk if persistence is enabled.
    fn persist_functions(&self) {
        if !self.admin.config_manager.persistence_enabled() {
            return;
        }

        let path = PathBuf::from(self.admin.config_manager.data_dir()).join("functions.fdb");
        let registry = match self.admin.function_registry.try_read_err() {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to acquire function registry lock for persistence");
                return;
            }
        };

        if let Err(e) = frogdb_core::save_to_file(&registry, &path) {
            warn!(error = %e, "Failed to persist functions to disk");
        }
    }

    /// Handle FUNCTION KILL - terminate a running read-only function.
    ///
    /// FUNCTION KILL uses the same mechanism as SCRIPT KILL since functions
    /// execute using the same Lua script executor. It will only kill functions
    /// that were called via FCALL_RO (read-only execution).
    async fn handle_function_kill(&self) -> Response {
        // Send ScriptKill to all shards (only one can be running a script at a time per shard)
        // We check all shards since we don't track which shard is running the function
        let mut responses = Vec::with_capacity(self.num_shards);

        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScriptKill { response_tx };

            if sender.send(msg).await.is_err() {
                continue;
            }

            if let Ok(result) = response_rx.await {
                responses.push(result);
            }
        }

        // Check if any shard was running a script
        for response in responses {
            match response {
                Ok(()) => return Response::ok(),
                Err(e) if e.contains("UNKILLABLE") => {
                    return Response::error(
                        "UNKILLABLE The busy script was not running in read-only mode.",
                    );
                }
                Err(_) => {} // NOTBUSY - continue checking other shards
            }
        }

        // No shard had a running script
        Response::error("NOTBUSY No scripts in execution right now.")
    }
}
