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

// Proves SCRIPT LOAD and PUBSUB introspection actually route through the timed
// broadcast seam (`ScatterGather`). The seam has its own timeout unit test, but
// nothing pinned that the *handlers* use it: reverting the handlers to a bare
// per-shard await (or an await-only-shard-0) still passed every other test.
// Here a real handler is wired to two mock shards where shard 1 stalls forever;
// SCRIPT LOAD and PUBSUB CHANNELS must surface `ERR timeout` rather than hang or
// silently ignore the stalled shard.
#[cfg(all(test, not(feature = "turmoil")))]
mod broadcast_timeout_routing_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use frogdb_core::{
        IntrospectionRequest, IntrospectionResponse, ShardMessage, ShardReceiver, ShardSender,
    };
    use frogdb_protocol::Response;
    use tokio::sync::mpsc;

    use crate::connection::ConnectionHandler;
    use crate::connection::deps::*;
    use crate::connection::pubsub_conn_command::PUBSUB_CONN_COMMAND;

    /// A stalled shard's parked receiver plus shard 0's responder task. Both must
    /// stay alive for the duration of the test: dropping the receiver would close
    /// the channel (turning the stall into an "unavailable" error), and dropping
    /// the task would stop shard 0 from answering.
    struct MockShards {
        _stalled: ShardReceiver,
        _responder: tokio::task::JoinHandle<()>,
    }

    /// Build a `ConnectionHandler` over two mock shards: shard 0 answers the
    /// scripting/introspection broadcasts, shard 1 receives its request but never
    /// replies (a stall). The gather must hit `timeout` and error.
    async fn handler_with_stalled_shard(timeout: Duration) -> (ConnectionHandler, MockShards) {
        // Loopback TCP pair for the handler's framed socket (never exercised).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = tokio::net::TcpStream::connect(addr);
        let (stream, _) = tokio::join!(async { listener.accept().await.unwrap() }, connect_fut);
        let tcp_stream: crate::net::ConnectionStream =
            crate::tls::MaybeTlsStream::Plain { inner: stream.0 };

        let (tx0, rx0) = mpsc::channel(16);
        let (tx1, rx1) = mpsc::channel(16);
        let shard_senders = Arc::new(vec![ShardSender::new(tx0), ShardSender::new(tx1)]);

        // Healthy shard 0: answer the two broadcast message kinds these handlers
        // emit. A real cluster would reply on every shard; here shard 1 does not,
        // exercising the stall path.
        let mut rx0 = ShardReceiver::new(rx0);
        let responder = tokio::spawn(async move {
            while let Some(env) = rx0.recv().await {
                match env.message {
                    ShardMessage::ScriptLoad { response_tx, .. } => {
                        let _ = response_tx.send("deadbeef".to_string());
                    }
                    ShardMessage::ScriptKill { response_tx } => {
                        // Healthy shard 0 has no running script.
                        let _ = response_tx
                            .send(Err("NOTBUSY No scripts in execution right now.".to_string()));
                    }
                    ShardMessage::PubSubIntrospection {
                        request,
                        response_tx,
                    } => {
                        let reply = match request {
                            IntrospectionRequest::NumPat => IntrospectionResponse::NumPat(0),
                            IntrospectionRequest::NumSub { channels }
                            | IntrospectionRequest::ShardNumSub { channels } => {
                                IntrospectionResponse::NumSub(
                                    channels.into_iter().map(|c| (c, 0)).collect(),
                                )
                            }
                            _ => IntrospectionResponse::Channels(vec![]),
                        };
                        let _ = response_tx.send(reply);
                    }
                    _ => {}
                }
            }
        });

        // Shard 1: keep the receiver alive but never poll it, so the request sits
        // buffered and its oneshot never resolves.
        let stalled = ShardReceiver::new(rx1);

        let mut registry = frogdb_core::CommandRegistry::new();
        crate::register_commands(&mut registry);
        let core = CoreDeps {
            registry: Arc::new(registry),
            shard_senders,
            acl_manager: frogdb_core::AclManager::new(Default::default()),
        };
        let client_registry = Arc::new(frogdb_core::ClientRegistry::new());
        let admin = AdminDeps {
            client_registry: client_registry.clone(),
            config_manager: Arc::new(crate::runtime_config::ConfigManager::new(
                &crate::config::Config::default(),
            )),
            snapshot_coordinator: Arc::new(frogdb_core::NoopSnapshotCoordinator::new()),
            function_registry: frogdb_core::SharedFunctionRegistry::default(),
            cursor_store: Arc::new(crate::cursor_store::AggregateCursorStore::new()),
        };
        let mut config = ConnectionConfig::default_for_testing(2);
        config.scatter_gather_timeout = timeout;
        let client_handle = client_registry.register(1, "127.0.0.1:9999".parse().unwrap(), None);

        let handler = ConnectionHandler::from_deps(
            tcp_stream,
            "127.0.0.1:9999".parse().unwrap(),
            1,
            0,
            client_handle,
            core,
            admin,
            ClusterDeps::default(),
            config,
            ObservabilityDeps::default(),
        );

        (
            handler,
            MockShards {
                _stalled: stalled,
                _responder: responder,
            },
        )
    }

    #[tokio::test]
    async fn script_load_errors_when_a_shard_stalls() {
        let (handler, _shards) = handler_with_stalled_shard(Duration::from_millis(150)).await;

        let resp = handler
            .handle_script(&[Bytes::from_static(b"LOAD"), Bytes::from_static(b"return 1")])
            .await;

        assert!(
            matches!(resp, Response::Error(ref e) if e.as_ref() == b"ERR timeout"),
            "SCRIPT LOAD must route through the timed broadcast seam and error on a \
             stalled shard rather than hang or return a SHA while a shard never cached \
             the script; got {resp:?}"
        );
    }

    #[tokio::test]
    async fn pubsub_channels_errors_when_a_shard_stalls() {
        let (mut handler, _shards) = handler_with_stalled_shard(Duration::from_millis(150)).await;

        let responses = handler
            .execute_pubsub(&PUBSUB_CONN_COMMAND, &[Bytes::from_static(b"CHANNELS")])
            .await;

        assert_eq!(responses.len(), 1, "PUBSUB CHANNELS returns one frame");
        assert!(
            matches!(responses[0], Response::Error(ref e) if e.as_ref() == b"ERR timeout"),
            "PUBSUB CHANNELS must route through the timed broadcast seam and error on a \
             stalled shard rather than hang; got {:?}",
            responses[0]
        );
    }

    // FUNCTION KILL is a hand-rolled gather-then-scan walk over every shard's
    // `ScriptKill` reply. It used to await each shard's oneshot with no timeout,
    // so a single wedged ShardWorker hung the whole ConnectionHandler forever —
    // while its twin SCRIPT KILL (retrofitted with `scatter_gather_timeout`)
    // returned and moved on. Shard 0 reports NOTBUSY, shard 1 stalls: the fixed
    // handler must bound the per-shard await, skip the stalled shard, and still
    // produce FUNCTION KILL's canonical NOTBUSY reply.
    #[tokio::test]
    async fn function_kill_returns_when_a_shard_stalls() {
        let (handler, _shards) = handler_with_stalled_shard(Duration::from_millis(150)).await;

        // Against current code this await never resolves; the outer guard bounds
        // the whole call so the regression fails as an expired guard here rather
        // than by tripping nextest's 15s cap.
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            handler.handle_function(&[Bytes::from_static(b"KILL")]),
        )
        .await
        .expect(
            "FUNCTION KILL hung on a stalled shard: each per-shard await must be \
             bounded by scatter_gather_timeout like SCRIPT KILL",
        );

        assert!(
            matches!(
                resp,
                Response::Error(ref e)
                    if e.as_ref() == b"NOTBUSY No scripts in execution right now."
            ),
            "FUNCTION KILL must skip the stalled shard and report NOTBUSY once no \
             shard is running a read-only function; got {resp:?}"
        );
    }
}
