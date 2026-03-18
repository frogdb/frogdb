//! FT.ALTER, FT.DROPINDEX, FT.INFO, FT.LIST handlers.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.ALTER - parse new fields, broadcast to all shards.
    pub(crate) async fn handle_ft_alter(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.alter' command");
        }

        let index_name = args[0].clone();
        let raw_args: Vec<&[u8]> = args[1..].iter().map(|a| a.as_ref()).collect();
        let new_fields = match frogdb_search::parse_ft_alter_args(&raw_args) {
            Ok(f) => f,
            Err(e) => return Response::error(format!("ERR {}", e)),
        };

        let json = match serde_json::to_vec(&new_fields) {
            Ok(j) => j,
            Err(e) => return Response::error(format!("ERR serialization: {}", e)),
        };

        // Broadcast to ALL shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtAlter {
                    index_name: index_name.clone(),
                    new_fields_json: Bytes::from(json.clone()),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.ALTER request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.ALTER timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FT.DROPINDEX - broadcast to all shards.
    pub(crate) async fn handle_ft_dropindex(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.dropindex' command");
        }

        let index_name = args[0].clone();

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtDropIndex {
                    index_name: index_name.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.DROPINDEX request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.DROPINDEX timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FT.INFO - query shard 0 only.
    pub(crate) async fn handle_ft_info(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.info' command");
        }

        let index_name = args[0].clone();

        // Only query shard 0 (all shards have identical schemas)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtInfo { index_name },
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
                    Response::error("ERR empty response")
                }
            }
            Ok(Err(_)) => Response::error("ERR shard dropped request"),
            Err(_) => Response::error("ERR timeout"),
        }
    }

    /// Handle FT._LIST - query shard 0 only.
    pub(crate) async fn handle_ft_list(&self, _args: &[Bytes]) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtList,
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
