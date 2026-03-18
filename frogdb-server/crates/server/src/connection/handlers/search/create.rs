//! FT.CREATE handler.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.CREATE - parse schema, broadcast to all shards.
    pub(crate) async fn handle_ft_create(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.create' command");
        }

        // First arg is the index name
        let index_name = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid index name"),
        };

        // Parse the schema
        let raw_args: Vec<&[u8]> = args[1..].iter().map(|a| a.as_ref()).collect();
        let def = match frogdb_search::parse_ft_create_args(index_name, &raw_args) {
            Ok(d) => d,
            Err(e) => return Response::error(format!("ERR {}", e)),
        };

        // Serialize to JSON for broadcast
        let json = match serde_json::to_vec(&def) {
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
                operation: ScatterOp::FtCreate {
                    index_def_json: Bytes::from(json.clone()),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Wait for all to complete
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    // Check for errors
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.CREATE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.CREATE timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }
}
