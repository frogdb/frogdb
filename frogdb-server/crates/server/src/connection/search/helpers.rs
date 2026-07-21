//! Common scatter-gather helper methods.

use frogdb_core::{CoreMsg, PartialResult, ScatterOp};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::{ConnectionHandler, next_txid};
use crate::scatter::ShardZeroReply;

impl ConnectionHandler {
    /// Broadcast an operation to all shards and return shard 0's response.
    /// Returns the first embedded error from any shard, if present.
    pub(crate) async fn broadcast_and_check_shard0(&self, operation: ScatterOp) -> Response {
        self.scatter_gather()
            .run(
                Box::new(ShardZeroReply::<PartialResult>::checked()),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: operation.clone(),
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Broadcast an operation to all shards and return shard 0's direct response.
    pub(crate) async fn broadcast_and_return_shard0_response(
        &self,
        operation: ScatterOp,
    ) -> Response {
        self.scatter_gather()
            .run(
                Box::new(ShardZeroReply::<PartialResult>::unchecked()),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: operation.clone(),
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Send an operation to shard 0 only and return its response.
    pub(crate) async fn query_shard0(&self, operation: ScatterOp) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation,
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
