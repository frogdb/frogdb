//! FT.ALTER, FT.DROPINDEX, FT.INFO, FT.LIST handlers.

use bytes::Bytes;
use frogdb_core::{CoreMsg, ScatterOp};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use super::merge::OkOrFirstError;
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

        // Broadcast to ALL shards; OK unless a shard reports an error.
        let json = Bytes::from(json);
        self.scatter_gather()
            .run(
                Box::new(OkOrFirstError::default()),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FtAlter {
                        index_name: index_name.clone(),
                        new_fields_json: json.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle FT.DROPINDEX - broadcast to all shards.
    pub(crate) async fn handle_ft_dropindex(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.dropindex' command");
        }

        let index_name = args[0].clone();

        self.scatter_gather()
            .run(
                Box::new(OkOrFirstError::default()),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FtDropIndex {
                        index_name: index_name.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle FT.INFO - query shard 0 only.
    pub(crate) async fn handle_ft_info(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.info' command");
        }

        let index_name = args[0].clone();

        // Only query shard 0 (all shards have identical schemas)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ScatterRequest {
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
                if let Some((_, resp)) = partial.into_keyed_results().into_iter().next() {
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
        let msg = CoreMsg::ScatterRequest {
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
                if let Some((_, resp)) = partial.into_keyed_results().into_iter().next() {
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
