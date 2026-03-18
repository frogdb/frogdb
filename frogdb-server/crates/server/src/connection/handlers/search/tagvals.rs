//! FT.TAGVALS handler.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.TAGVALS - scatter to all shards, union results.
    pub(crate) async fn handle_ft_tagvals(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.tagvals' command");
        }
        let index_name = args[0].clone();
        let field_name = args[1].clone();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtTagvals {
                    index_name: index_name.clone(),
                    field_name: field_name.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Collect and union tag values from all shards
        let mut all_values = std::collections::HashSet::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in partial.results {
                        match resp {
                            Response::Error(_) => return resp,
                            Response::Array(items) => {
                                for item in items {
                                    if let Response::Bulk(Some(b)) = item
                                        && let Ok(s) = std::str::from_utf8(&b)
                                    {
                                        all_values.insert(s.to_string());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.TAGVALS request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.TAGVALS timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        let mut sorted: Vec<String> = all_values.into_iter().collect();
        sorted.sort();
        Response::Array(
            sorted
                .into_iter()
                .map(|v| Response::bulk(Bytes::from(v)))
                .collect(),
        )
    }
}
