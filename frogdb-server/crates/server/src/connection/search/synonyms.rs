//! FT.SYNUPDATE, FT.SYNDUMP handlers.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use super::merge::OkOrFirstError;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.SYNUPDATE - parse args, broadcast to all shards.
    pub(crate) async fn handle_ft_synupdate(&self, args: &[Bytes]) -> Response {
        if args.len() < 3 {
            return Response::error("ERR wrong number of arguments for 'ft.synupdate' command");
        }

        let index_name = args[0].clone();
        let group_id = args[1].clone();

        // Skip optional SKIPINITIALSCAN, collect terms
        let mut term_start = 2;
        if term_start < args.len()
            && args[term_start].to_ascii_uppercase().as_slice() == b"SKIPINITIALSCAN"
        {
            term_start += 1;
        }

        if term_start >= args.len() {
            return Response::error("ERR at least one term is required");
        }

        let terms: Vec<Bytes> = args[term_start..].to_vec();

        // Broadcast to ALL shards; OK unless a shard reports an error.
        self.scatter_gather()
            .run(
                Box::new(OkOrFirstError::default()),
                |_shard, response_tx| ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FtSynupdate {
                        index_name: index_name.clone(),
                        group_id: group_id.clone(),
                        terms: terms.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle FT.SYNDUMP - query shard 0 only.
    pub(crate) async fn handle_ft_syndump(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.syndump' command");
        }

        let index_name = args[0].clone();
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtSyndump { index_name },
            conn_id: self.state.id,
            response_tx,
        };
        if self.core.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                if let Some((_, resp)) = partial.results.into_iter().next() {
                    resp
                } else {
                    Response::Array(vec![])
                }
            }
            Ok(Err(_)) => Response::error("ERR shard dropped request"),
            Err(_) => Response::error("ERR timeout"),
        }
    }
}
