//! ES.ALL handler.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

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

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::EsAll { count, after_id },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Gather results from all shards
        let mut all_entries: Vec<(StreamId, Response)> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in partial.results {
                        // Each entry is [stream_id_string, [fields...]]
                        // Parse stream_id for sorting
                        if let Response::Array(ref parts) = resp
                            && let Some(Response::Bulk(Some(id_bytes))) = parts.first()
                            && let Ok(id) = StreamId::parse(id_bytes)
                        {
                            all_entries.push((id, resp));
                            continue;
                        }
                        // Fallback: use max ID to sort to end
                        all_entries.push((StreamId::max(), resp));
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped ES.ALL request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "ES.ALL timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort by StreamId for approximate global ordering
        all_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Apply COUNT limit after merge
        if let Some(limit) = count {
            all_entries.truncate(limit);
        }

        Response::Array(all_entries.into_iter().map(|(_, r)| r).collect())
    }
}
