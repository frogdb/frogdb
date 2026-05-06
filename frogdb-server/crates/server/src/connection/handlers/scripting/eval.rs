//! EVAL, EVALSHA, EVAL_RO, EVALSHA_RO handlers.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{ShardMessage, shard_for_key};
use frogdb_protocol::Response;
use frogdb_vll::{
    ContinuationError, ContinuationGuard, DEFAULT_LOCK_ACQUISITION_TIMEOUT, NoopMetricsSink,
    VllCoordinator,
};
use tokio::sync::oneshot;

use crate::connection::{ConnectionHandler, next_txid};
use crate::vll_adapter::ShardSenderSink;

impl ConnectionHandler {
    /// Handle EVAL / EVAL_RO command.
    pub(crate) async fn handle_eval(&self, args: &[Bytes], read_only: bool) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'eval' command");
        }

        let script_source = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        match self.classify_script_shards(&keys) {
            ScriptShards::Single(shard_id) => {
                self.execute_single_shard_script(
                    EvalKind::Source(script_source),
                    keys,
                    argv,
                    shard_id,
                    read_only,
                )
                .await
            }
            ScriptShards::CrossShard(shards) => {
                self.execute_cross_shard_script(
                    EvalKind::Source(script_source),
                    keys,
                    argv,
                    shards,
                    read_only,
                )
                .await
            }
            ScriptShards::CrossSlotForbidden => {
                Response::error("CROSSSLOT Keys in request don't hash to the same slot")
            }
        }
    }

    fn classify_script_shards(&self, keys: &[Bytes]) -> ScriptShards {
        if keys.is_empty() {
            return ScriptShards::Single(0);
        }
        let mut shards: Vec<usize> = keys
            .iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            ScriptShards::Single(shards[0])
        } else if self.allow_cross_slot {
            ScriptShards::CrossShard(shards)
        } else {
            ScriptShards::CrossSlotForbidden
        }
    }

    /// Send a script message to one shard and await the response.
    async fn execute_single_shard_script(
        &self,
        kind: EvalKind,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
        read_only: bool,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = kind.into_message(
            keys,
            argv,
            self.state.id,
            self.state.protocol_version,
            read_only,
            response_tx,
        );

        if self.core.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Acquire continuation locks across `shards` via the VLL coordinator,
    /// execute the script on the primary (first) shard, and release locks
    /// when the guard drops.
    async fn execute_cross_shard_script(
        &self,
        kind: EvalKind,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>,
        read_only: bool,
    ) -> Response {
        let txid = next_txid();
        let primary_shard = shards[0];

        let sink = ShardSenderSink::new(Arc::clone(&self.core.shard_senders));
        let coordinator = VllCoordinator::new(sink, NoopMetricsSink);

        let _guard: ContinuationGuard = match coordinator
            .acquire_continuation(
                txid,
                self.state.id,
                &shards,
                DEFAULT_LOCK_ACQUISITION_TIMEOUT,
            )
            .await
        {
            Ok(guard) => guard,
            Err(err) => return continuation_error_to_response(err),
        };

        let (response_tx, response_rx) = oneshot::channel();
        let msg = kind.into_message(
            keys,
            argv,
            self.state.id,
            self.state.protocol_version,
            read_only,
            response_tx,
        );

        if self.core.shard_senders[primary_shard]
            .send(msg)
            .await
            .is_err()
        {
            return Response::error("ERR shard unavailable");
        }
        match response_rx.await {
            Ok(resp) => resp,
            Err(_) => Response::error("ERR script execution failed"),
        }
        // _guard is dropped on return, releasing every continuation lock.
    }

    /// Handle EVALSHA / EVALSHA_RO command.
    pub(crate) async fn handle_evalsha(&self, args: &[Bytes], read_only: bool) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'evalsha' command");
        }

        let script_sha = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        match self.classify_script_shards(&keys) {
            ScriptShards::Single(shard_id) => {
                self.execute_single_shard_script(
                    EvalKind::Sha(script_sha),
                    keys,
                    argv,
                    shard_id,
                    read_only,
                )
                .await
            }
            ScriptShards::CrossShard(shards) => {
                self.execute_cross_shard_script(
                    EvalKind::Sha(script_sha),
                    keys,
                    argv,
                    shards,
                    read_only,
                )
                .await
            }
            ScriptShards::CrossSlotForbidden => {
                Response::error("CROSSSLOT Keys in request don't hash to the same slot")
            }
        }
    }
}

/// Classification of which shards a script touches.
enum ScriptShards {
    /// Single shard — no continuation locks needed.
    Single(usize),
    /// Multiple shards — caller must acquire continuation locks.
    CrossShard(Vec<usize>),
    /// Multiple shards but `cluster-allow-cross-slot` is disabled.
    CrossSlotForbidden,
}

/// Source of the script: literal source vs. cached SHA1.
enum EvalKind {
    Source(Bytes),
    Sha(Bytes),
}

impl EvalKind {
    fn into_message(
        self,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        conn_id: u64,
        protocol_version: frogdb_protocol::ProtocolVersion,
        read_only: bool,
        response_tx: oneshot::Sender<Response>,
    ) -> ShardMessage {
        match self {
            EvalKind::Source(script_source) => ShardMessage::EvalScript {
                script_source,
                keys,
                argv,
                conn_id,
                protocol_version,
                read_only,
                response_tx,
            },
            EvalKind::Sha(script_sha) => ShardMessage::EvalScriptSha {
                script_sha,
                keys,
                argv,
                conn_id,
                protocol_version,
                read_only,
                response_tx,
            },
        }
    }
}

fn continuation_error_to_response(err: ContinuationError) -> Response {
    match err {
        ContinuationError::ShardUnavailable(_) => Response::error("ERR shard unavailable"),
        ContinuationError::LockFailed { error, .. } => {
            Response::error(format!("ERR lock acquisition failed: {error}"))
        }
        ContinuationError::LockChannelClosed { .. } => {
            Response::error("ERR shard dropped lock request")
        }
        ContinuationError::LockTimeout { shard_id } => {
            Response::error(format!("ERR lock acquisition timeout on shard {shard_id}"))
        }
    }
}
