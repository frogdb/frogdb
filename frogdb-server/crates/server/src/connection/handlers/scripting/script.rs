//! SCRIPT LOAD, EXISTS, FLUSH, KILL handlers.

use bytes::Bytes;
use frogdb_core::ShardMessage;
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;

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

    /// Handle SCRIPT LOAD -- broadcast to all shards.
    async fn handle_script_load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|load' command");
        }
        let script_source = args[0].clone();
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (tx, rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::ScriptLoad {
                    script_source: script_source.clone(),
                    response_tx: tx,
                })
                .await;
            handles.push(rx);
        }
        match handles.into_iter().next().unwrap().await {
            Ok(sha) => Response::bulk(Bytes::from(sha)),
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle SCRIPT EXISTS -- query all shards (OR semantics).
    async fn handle_script_exists(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|exists' command");
        }
        let shas = args.to_vec();
        let num_shas = shas.len();
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (tx, rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::ScriptExists {
                    shas: shas.clone(),
                    response_tx: tx,
                })
                .await;
            handles.push(rx);
        }
        let mut combined = vec![false; num_shas];
        for rx in handles {
            if let Ok(results) = rx.await {
                for (i, exists) in results.into_iter().enumerate() {
                    if exists {
                        combined[i] = true;
                    }
                }
            }
        }
        Response::Array(
            combined
                .into_iter()
                .map(|e| Response::Integer(if e { 1 } else { 0 }))
                .collect(),
        )
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
                    return Response::error(
                        "ERR SCRIPT FLUSH only supports ASYNC and SYNC options",
                    );
                }
            }
        } else {
            false
        };

        // Broadcast to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
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
        for sender in self.core.shard_senders.iter() {
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
