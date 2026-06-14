//! SCRIPT LOAD, EXISTS, FLUSH, KILL handlers.

use bytes::Bytes;
use frogdb_core::ShardMessage;
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::scatter::{AllOk, BoolOr, ShardZeroReply};

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
    ///
    /// `FailFast` ensures every shard caches the script before we return its
    /// SHA: a dropped/slow shard now errors instead of returning a SHA that
    /// some shard never cached (fixes round-2 flag F2). Shard 0's SHA is
    /// returned (all shards derive the same SHA from the same source).
    async fn handle_script_load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|load' command");
        }
        let script_source = args[0].clone();
        self.scatter_gather()
            .run(
                Box::new(ShardZeroReply::<String>::unchecked()),
                |_shard, response_tx| ShardMessage::ScriptLoad {
                    script_source: script_source.clone(),
                    response_tx,
                },
            )
            .await
    }

    /// Handle SCRIPT EXISTS -- query all shards (OR semantics).
    async fn handle_script_exists(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|exists' command");
        }
        let shas = args.to_vec();
        let num_shas = shas.len();
        self.scatter_gather()
            .run(Box::new(BoolOr::new(num_shas)), |_shard, response_tx| {
                ShardMessage::ScriptExists {
                    shas: shas.clone(),
                    response_tx,
                }
            })
            .await
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

        self.scatter_gather()
            .run(Box::<AllOk<()>>::default(), |_shard, response_tx| {
                ShardMessage::ScriptFlush { response_tx }
            })
            .await
    }

    /// Handle SCRIPT KILL.
    ///
    /// Sequential first-success walk (not a fan-out): return on the first shard
    /// that kills a running script, skipping shards that report `NOTBUSY` or
    /// that drop / time out. The per-shard await is now bounded by the
    /// scatter-gather timeout (fixes round-2 flag F2's missing KILL timeout).
    async fn handle_script_kill(&self) -> Response {
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::ScriptKill { response_tx })
                .await
                .is_err()
            {
                continue;
            }

            match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
                Ok(Ok(Ok(()))) => return Response::ok(),
                Ok(Ok(Err(e))) if e.contains("NOTBUSY") => continue, // Try next shard
                Ok(Ok(Err(e))) => return Response::error(e),
                // Shard dropped the request or timed out: try the next shard.
                Ok(Err(_)) | Err(_) => continue,
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
