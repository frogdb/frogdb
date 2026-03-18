//! Common scatter-gather helper methods.

use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Broadcast an operation to all shards and return shard 0's response.
    /// Checks for errors from any shard.
    pub(crate) async fn broadcast_and_check_shard0(&self, operation: ScatterOp) -> Response {
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: operation.clone(),
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut shard0_response = Response::ok();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                    if shard_id == 0
                        && let Some((_, resp)) = partial.results.into_iter().next()
                    {
                        shard0_response = resp;
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "Request timeout");
                    return Response::error("ERR timeout");
                }
            }
        }
        shard0_response
    }

    /// Broadcast an operation to all shards and return shard 0's direct response.
    pub(crate) async fn broadcast_and_return_shard0_response(
        &self,
        operation: ScatterOp,
    ) -> Response {
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: operation.clone(),
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut shard0_response = Response::ok();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    if shard_id == 0
                        && let Some((_, resp)) = partial.results.into_iter().next()
                    {
                        shard0_response = resp;
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "Request timeout");
                    return Response::error("ERR timeout");
                }
            }
        }
        shard0_response
    }

    /// Send an operation to shard 0 only and return its response.
    pub(crate) async fn query_shard0(&self, operation: ScatterOp) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
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
