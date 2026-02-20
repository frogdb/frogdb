//! Scripting command handlers.
//!
//! This module handles scripting commands:
//! - EVAL - Execute a Lua script
//! - EVALSHA - Execute a cached Lua script by SHA
//! - SCRIPT LOAD/EXISTS/FLUSH/KILL/HELP
//! - FCALL/FCALL_RO - Call a registered function
//! - FUNCTION LOAD/DELETE/FLUSH/LIST/STATS/DUMP/RESTORE/KILL/HELP
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{shard_for_key, RwLockExt, ShardMessage, ShardReadyResult};
use frogdb_protocol::Response;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{next_txid, ConnectionHandler};

// ============================================================================
// EVAL / EVALSHA handlers
// ============================================================================

impl ConnectionHandler {
    /// Handle EVAL command.
    pub(crate) async fn handle_eval(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'eval' command");
        }

        // Parse arguments
        let script_source = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine which shards are involved
        if keys.is_empty() {
            // No keys -> single shard (shard 0)
            return self
                .execute_single_shard_script(script_source, keys, argv, 0)
                .await;
        }

        // Collect unique shards in sorted order
        let mut shards: Vec<usize> = keys
            .iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            // Single shard - use simple path
            self.execute_single_shard_script(script_source, keys, argv, shards[0])
                .await
        } else if self.allow_cross_slot {
            // Cross-shard script - use VLL continuation locks
            self.execute_cross_shard_script(script_source, keys, argv, shards)
                .await
        } else {
            // Cross-slot not allowed
            Response::error("CROSSSLOT Keys in request don't hash to the same slot")
        }
    }

    /// Execute a Lua script on a single shard.
    async fn execute_single_shard_script(
        &self,
        script_source: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Execute a Lua script across multiple shards using VLL continuation locks.
    ///
    /// This acquires continuation locks on all involved shards (in sorted order to
    /// prevent deadlocks), executes the script on the primary shard, then releases
    /// all locks.
    async fn execute_cross_shard_script(
        &self,
        script_source: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>, // Already sorted and deduplicated
    ) -> Response {
        use std::time::Duration;

        let txid = next_txid();
        let primary_shard = shards[0]; // Execute on first shard

        // Phase 1: Acquire continuation locks on all shards (in sorted order)
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<oneshot::Receiver<ShardReadyResult>> =
            Vec::with_capacity(shards.len());

        for &shard_id in &shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let msg = ShardMessage::VllContinuationLock {
                txid,
                conn_id: self.state.id,
                ready_tx,
                release_rx,
            };

            if self.shard_senders[shard_id].send(msg).await.is_err() {
                // Abort: release already-locked shards
                for tx in release_txs {
                    let _ = tx.send(());
                }
                return Response::error("ERR shard unavailable");
            }

            release_txs.push(release_tx);
            ready_rxs.push(ready_rx);
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {
                    // Shard is ready
                }
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    // Lock acquisition failed - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!("ERR lock acquisition failed: {}", e));
                }
                Ok(Err(_)) => {
                    // Channel closed - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error("ERR shard dropped lock request");
                }
                Err(_) => {
                    // Timeout - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!(
                        "ERR lock acquisition timeout on shard {}",
                        shards[i]
                    ));
                }
            }
        }

        // Phase 3: Execute script on primary shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        let response = if self.shard_senders[primary_shard].send(msg).await.is_ok() {
            match response_rx.await {
                Ok(resp) => resp,
                Err(_) => Response::error("ERR script execution failed"),
            }
        } else {
            Response::error("ERR shard unavailable")
        };

        // Phase 4: Release all locks
        for tx in release_txs {
            let _ = tx.send(());
        }

        response
    }

    /// Handle EVALSHA command.
    pub(crate) async fn handle_evalsha(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'evalsha' command");
        }

        // Parse arguments
        let script_sha = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine which shards are involved
        if keys.is_empty() {
            // No keys -> single shard (shard 0)
            return self
                .execute_single_shard_script_sha(script_sha, keys, argv, 0)
                .await;
        }

        // Collect unique shards in sorted order
        let mut shards: Vec<usize> = keys
            .iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            // Single shard - use simple path
            self.execute_single_shard_script_sha(script_sha, keys, argv, shards[0])
                .await
        } else if self.allow_cross_slot {
            // Cross-shard script - use VLL continuation locks
            self.execute_cross_shard_script_sha(script_sha, keys, argv, shards)
                .await
        } else {
            // Cross-slot not allowed
            Response::error("CROSSSLOT Keys in request don't hash to the same slot")
        }
    }

    /// Execute a cached Lua script (by SHA) on a single shard.
    async fn execute_single_shard_script_sha(
        &self,
        script_sha: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Execute a cached Lua script (by SHA) across multiple shards using VLL continuation locks.
    async fn execute_cross_shard_script_sha(
        &self,
        script_sha: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>, // Already sorted and deduplicated
    ) -> Response {
        use std::time::Duration;

        let txid = next_txid();
        let primary_shard = shards[0];

        // Phase 1: Acquire continuation locks on all shards
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<oneshot::Receiver<ShardReadyResult>> =
            Vec::with_capacity(shards.len());

        for &shard_id in &shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let msg = ShardMessage::VllContinuationLock {
                txid,
                conn_id: self.state.id,
                ready_tx,
                release_rx,
            };

            if self.shard_senders[shard_id].send(msg).await.is_err() {
                for tx in release_txs {
                    let _ = tx.send(());
                }
                return Response::error("ERR shard unavailable");
            }

            release_txs.push(release_tx);
            ready_rxs.push(ready_rx);
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!("ERR lock acquisition failed: {}", e));
                }
                Ok(Err(_)) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error("ERR shard dropped lock request");
                }
                Err(_) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!(
                        "ERR lock acquisition timeout on shard {}",
                        shards[i]
                    ));
                }
            }
        }

        // Phase 3: Execute script on primary shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        let response = if self.shard_senders[primary_shard].send(msg).await.is_ok() {
            match response_rx.await {
                Ok(resp) => resp,
                Err(_) => Response::error("ERR script execution failed"),
            }
        } else {
            Response::error("ERR shard unavailable")
        };

        // Phase 4: Release all locks
        for tx in release_txs {
            let _ = tx.send(());
        }

        response
    }
}

// ============================================================================
// SCRIPT command handlers
// ============================================================================

impl ConnectionHandler {
    /// Handle SCRIPT command.
    pub(crate) async fn handle_script(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "LOAD" => self.handle_script_load(&args[1..]).await,
            "EXISTS" => self.handle_script_exists(&args[1..]).await,
            "FLUSH" => self.handle_script_flush(&args[1..]).await,
            "KILL" => self.handle_script_kill().await,
            "HELP" => self.handle_script_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try SCRIPT HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle SCRIPT LOAD.
    async fn handle_script_load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|load' command");
        }

        let script_source = args[0].clone();

        // Send to shard 0 (scripts are loaded per-shard, but we return the SHA)
        // In a production system, we'd broadcast to all shards
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScriptLoad {
            script_source,
            response_tx,
        };

        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(sha) => Response::bulk(Bytes::from(sha)),
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle SCRIPT EXISTS.
    async fn handle_script_exists(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|exists' command");
        }

        let shas = args.to_vec();

        // Query shard 0 (in production, would need to check the target shard)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScriptExists { shas, response_tx };

        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(results) => {
                let arr: Vec<Response> = results
                    .into_iter()
                    .map(|exists| Response::Integer(if exists { 1 } else { 0 }))
                    .collect();
                Response::Array(arr)
            }
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle SCRIPT FLUSH.
    async fn handle_script_flush(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC|SYNC argument (we ignore it for now)
        let _async_mode = if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            match mode.as_slice() {
                b"ASYNC" => true,
                b"SYNC" => false,
                _ => {
                    return Response::error("ERR SCRIPT FLUSH only supports ASYNC and SYNC options")
                }
            }
        } else {
            false
        };

        // Broadcast to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::ScriptFlush { response_tx }).await;
            handles.push(response_rx);
        }

        // Wait for all to complete
        for rx in handles {
            let _ = rx.await;
        }

        Response::ok()
    }

    /// Handle SCRIPT KILL.
    async fn handle_script_kill(&self) -> Response {
        // Try to kill on all shards, return first error or success
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::ScriptKill { response_tx }).await;

            if let Ok(result) = response_rx.await {
                match result {
                    Ok(()) => return Response::ok(),
                    Err(e) if e.contains("NOTBUSY") => continue, // Try next shard
                    Err(e) => return Response::error(e),
                }
            }
        }

        Response::error("NOTBUSY No scripts in execution right now.")
    }

    /// Handle SCRIPT HELP.
    fn handle_script_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"EXISTS <sha1> [<sha1> ...]")),
            Response::bulk(Bytes::from_static(
                b"    Return information about the existence of the scripts in the script cache.",
            )),
            Response::bulk(Bytes::from_static(b"FLUSH [ASYNC|SYNC]")),
            Response::bulk(Bytes::from_static(
                b"    Flush the Lua scripts cache. Defaults to SYNC.",
            )),
            Response::bulk(Bytes::from_static(b"KILL")),
            Response::bulk(Bytes::from_static(
                b"    Kill the currently executing Lua script.",
            )),
            Response::bulk(Bytes::from_static(b"LOAD <script>")),
            Response::bulk(Bytes::from_static(
                b"    Load a script into the scripts cache without executing it.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }
}

// ============================================================================
// FCALL / FUNCTION command handlers
// ============================================================================

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
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
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

        if self.shard_senders[target_shard].send(msg).await.is_err() {
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
        let (replace, code) =
            if args.len() >= 2 && args[0].to_ascii_uppercase() == b"REPLACE".as_slice() {
                (true, &args[1])
            } else {
                (false, &args[0])
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
            let mut registry = match self.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry.load_library(library, replace) {
                Ok(_) => {}
                Err(e) => return Response::error(e.to_string()),
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::bulk(Bytes::from(library_name))
    }

    /// Handle FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE].
    fn handle_function_list(&self, args: &[Bytes]) -> Response {
        let mut pattern: Option<&str> = None;
        let mut with_code = false;

        let mut i = 0;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIBRARYNAME" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    pattern = std::str::from_utf8(&args[i]).ok();
                }
                b"WITHCODE" => {
                    with_code = true;
                }
                _ => {
                    return Response::error(format!(
                        "ERR unknown option '{}'",
                        String::from_utf8_lossy(&args[i])
                    ));
                }
            }
            i += 1;
        }

        let registry = match self.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
        let libraries = registry.list_libraries(pattern);

        let mut result = Vec::new();
        for lib in libraries {
            let mut lib_info = Vec::new();

            // library_name
            lib_info.push(Response::bulk(Bytes::from_static(b"library_name")));
            lib_info.push(Response::bulk(Bytes::from(lib.name.clone())));

            // engine
            lib_info.push(Response::bulk(Bytes::from_static(b"engine")));
            lib_info.push(Response::bulk(Bytes::from(lib.engine.clone())));

            // functions
            lib_info.push(Response::bulk(Bytes::from_static(b"functions")));
            let mut funcs = Vec::new();
            for func in lib.functions.values() {
                let mut func_info = Vec::new();

                func_info.push(Response::bulk(Bytes::from_static(b"name")));
                func_info.push(Response::bulk(Bytes::from(func.name.clone())));

                func_info.push(Response::bulk(Bytes::from_static(b"flags")));
                let flags: Vec<Response> = func
                    .flags
                    .to_strings()
                    .into_iter()
                    .map(|f| Response::bulk(Bytes::from(f)))
                    .collect();
                func_info.push(Response::Array(flags));

                if let Some(ref desc) = func.description {
                    func_info.push(Response::bulk(Bytes::from_static(b"description")));
                    func_info.push(Response::bulk(Bytes::from(desc.clone())));
                }

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
            let mut registry = match self.function_registry.try_write_err() {
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
                return Response::error("ERR FUNCTION FLUSH only supports ASYNC and SYNC options");
            }
        }

        {
            let mut registry = match self.function_registry.try_write_err() {
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
    fn handle_function_stats(&self) -> Response {
        let registry = match self.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
        let stats = registry.stats();

        let mut result = Vec::new();

        // running_script
        result.push(Response::bulk(Bytes::from_static(b"running_script")));
        if let Some(ref running) = stats.running_function {
            let mut script_info = Vec::new();
            script_info.push(Response::bulk(Bytes::from_static(b"name")));
            script_info.push(Response::bulk(Bytes::from(running.name.clone())));
            script_info.push(Response::bulk(Bytes::from_static(b"command")));
            script_info.push(Response::bulk(Bytes::from_static(b"fcall")));
            script_info.push(Response::bulk(Bytes::from_static(b"duration_ms")));
            script_info.push(Response::Integer(running.duration_ms as i64));
            result.push(Response::Array(script_info));
        } else {
            result.push(Response::Null);
        }

        // engines
        result.push(Response::bulk(Bytes::from_static(b"engines")));
        let mut engines = Vec::new();
        let mut lua_info = Vec::new();
        lua_info.push(Response::bulk(Bytes::from_static(b"libraries_count")));
        lua_info.push(Response::Integer(stats.library_count as i64));
        lua_info.push(Response::bulk(Bytes::from_static(b"functions_count")));
        lua_info.push(Response::Integer(stats.function_count as i64));
        engines.push(Response::bulk(Bytes::from_static(b"LUA")));
        engines.push(Response::Array(lua_info));
        result.push(Response::Array(engines));

        Response::Array(result)
    }

    /// Handle FUNCTION DUMP.
    fn handle_function_dump(&self) -> Response {
        let registry = match self.function_registry.try_read_err() {
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
            let mut registry = match self.function_registry.try_write_err() {
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
                        ))
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
        if !self.config_manager.persistence_enabled() {
            return;
        }

        let path = PathBuf::from(self.config_manager.data_dir()).join("functions.fdb");
        let registry = match self.function_registry.try_read_err() {
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

        for sender in self.shard_senders.iter() {
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
                    )
                }
                Err(_) => {} // NOTBUSY - continue checking other shards
            }
        }

        // No shard had a running script
        Response::error("NOTBUSY No scripts in execution right now.")
    }
}

// ============================================================================
// Utility functions (kept from original module)
// ============================================================================

/// Parse SCRIPT subcommand and return the subcommand name.
pub fn parse_script_subcommand(args: &[Bytes]) -> Result<(&str, &[Bytes]), Response> {
    if args.is_empty() {
        return Err(Response::error(
            "ERR wrong number of arguments for 'script' command",
        ));
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Err(Response::error("ERR invalid subcommand")),
    };

    Ok((subcommand, &args[1..]))
}

/// Generate SCRIPT command help text.
pub fn script_help() -> Response {
    let help = vec![
        "SCRIPT LOAD <script>",
        "    Load a script into the script cache.",
        "SCRIPT EXISTS <sha1> [<sha1> ...]",
        "    Check if scripts exist in the cache.",
        "SCRIPT FLUSH [ASYNC|SYNC]",
        "    Flush the script cache.",
        "SCRIPT KILL",
        "    Kill a running script.",
        "SCRIPT HELP",
        "    Show this help.",
    ];
    Response::Array(help.into_iter().map(Response::bulk).collect())
}

/// Generate FUNCTION command help text.
pub fn function_help() -> Response {
    let help = vec![
        "FUNCTION LOAD [REPLACE] <function-code>",
        "    Load a function library.",
        "FUNCTION DELETE <library-name>",
        "    Delete a function library.",
        "FUNCTION FLUSH [ASYNC|SYNC]",
        "    Delete all function libraries.",
        "FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]",
        "    List loaded function libraries.",
        "FUNCTION STATS",
        "    Show function statistics.",
        "FUNCTION DUMP",
        "    Dump all function libraries as serialized data.",
        "FUNCTION RESTORE <data> [FLUSH|APPEND|REPLACE]",
        "    Restore function libraries from serialized data.",
        "FUNCTION KILL",
        "    Kill a running function.",
        "FUNCTION HELP",
        "    Show this help.",
    ];
    Response::Array(help.into_iter().map(Response::bulk).collect())
}

/// Parse SCRIPT FLUSH arguments.
///
/// Returns true for SYNC mode, false for ASYNC mode.
pub fn parse_script_flush_mode(args: &[Bytes]) -> Result<bool, Response> {
    if args.is_empty() {
        return Ok(true); // Default to SYNC
    }

    let mode = String::from_utf8_lossy(&args[0]).to_uppercase();
    match mode.as_str() {
        "ASYNC" => Ok(false),
        "SYNC" => Ok(true),
        _ => Err(Response::error(
            "ERR SCRIPT FLUSH only supports ASYNC and SYNC",
        )),
    }
}

/// Parse FUNCTION LOAD arguments.
///
/// Returns (replace: bool, code_index: usize).
pub fn parse_function_load_args(args: &[Bytes]) -> Result<(bool, usize), Response> {
    if args.is_empty() {
        return Err(Response::error(
            "ERR wrong number of arguments for 'function load' command",
        ));
    }

    let mut replace = false;
    let mut code_idx = 0;

    if args.len() > 1 {
        let flag = String::from_utf8_lossy(&args[0]).to_uppercase();
        if flag == "REPLACE" {
            replace = true;
            code_idx = 1;
        }
    }

    if code_idx >= args.len() {
        return Err(Response::error("ERR FUNCTION LOAD requires function code"));
    }

    Ok((replace, code_idx))
}

/// Parsed FUNCTION LIST options.
#[derive(Debug, Default)]
pub struct FunctionListOptions<'a> {
    /// Library name pattern filter.
    pub pattern: Option<&'a str>,
    /// Whether to include function code.
    pub with_code: bool,
}

/// Parse FUNCTION LIST arguments.
pub fn parse_function_list_args(args: &[Bytes]) -> Result<FunctionListOptions<'_>, Response> {
    let mut opts = FunctionListOptions::default();
    let mut i = 0;

    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "LIBRARYNAME" => {
                if i + 1 >= args.len() {
                    return Err(Response::error("ERR LIBRARYNAME requires a pattern"));
                }
                opts.pattern = Some(
                    std::str::from_utf8(&args[i + 1])
                        .map_err(|_| Response::error("ERR invalid pattern"))?,
                );
                i += 2;
            }
            "WITHCODE" => {
                opts.with_code = true;
                i += 1;
            }
            _ => {
                return Err(Response::error(format!(
                    "ERR Unknown FUNCTION LIST option '{}'",
                    arg
                )));
            }
        }
    }

    Ok(opts)
}

/// Parse FUNCTION RESTORE policy.
pub fn parse_function_restore_policy(args: &[Bytes]) -> Result<&str, Response> {
    if args.len() <= 1 {
        return Ok("APPEND"); // Default policy
    }

    let policy = String::from_utf8_lossy(&args[1]).to_uppercase();
    match policy.as_str() {
        "FLUSH" | "APPEND" | "REPLACE" => Ok(match policy.as_str() {
            "FLUSH" => "FLUSH",
            "APPEND" => "APPEND",
            "REPLACE" => "REPLACE",
            _ => unreachable!(),
        }),
        _ => Err(Response::error(
            "ERR FUNCTION RESTORE policy must be FLUSH, APPEND, or REPLACE",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_script_flush_mode() {
        assert!(parse_script_flush_mode(&[]).unwrap());
        assert!(parse_script_flush_mode(&[Bytes::from("SYNC")]).unwrap());
        assert!(!parse_script_flush_mode(&[Bytes::from("ASYNC")]).unwrap());
        assert!(parse_script_flush_mode(&[Bytes::from("invalid")]).is_err());
    }

    #[test]
    fn test_parse_function_load_args() {
        // Without REPLACE
        let (replace, idx) = parse_function_load_args(&[Bytes::from("code")]).unwrap();
        assert!(!replace);
        assert_eq!(idx, 0);

        // With REPLACE
        let (replace, idx) =
            parse_function_load_args(&[Bytes::from("REPLACE"), Bytes::from("code")]).unwrap();
        assert!(replace);
        assert_eq!(idx, 1);
    }
}
