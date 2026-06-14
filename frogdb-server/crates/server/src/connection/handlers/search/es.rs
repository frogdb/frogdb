//! ES.ALL handler.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;

use super::merge::EsAllMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle ES.ALL — scatter to all shards, merge results by StreamId ordering.
    pub(crate) async fn handle_es_all(&self, args: &[Bytes]) -> Response {
        use frogdb_core::StreamId;

        // Parse optional COUNT and AFTER arguments
        let mut count: Option<usize> = None;
        let mut after_id: Option<StreamId> = None;
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => count = Some(n),
                        None => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    }
                    i += 1;
                }
                b"AFTER" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    match StreamId::parse(&args[i]) {
                        Ok(id) => after_id = Some(id),
                        Err(_) => return Response::error("ERR Invalid stream ID specified"),
                    }
                    i += 1;
                }
                _ => {
                    return Response::error("ERR syntax error");
                }
            }
        }

        // Scatter to all shards; merge by StreamId ordering with COUNT applied
        // after the merge (see `EsAllMerge`).
        self.scatter_gather()
            .run(Box::new(EsAllMerge::new(count)), |_shard, response_tx| {
                ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::EsAll { count, after_id },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
    }
}
