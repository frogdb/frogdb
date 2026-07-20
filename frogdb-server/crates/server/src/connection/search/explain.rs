//! FT.EXPLAIN, FT.EXPLAINCLI handlers.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.EXPLAIN / FT.EXPLAINCLI - query shard 0 only.
    pub(crate) async fn handle_ft_explain(&self, args: &[Bytes], cli_mode: bool) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.explain' command");
        }

        let index_name = args[0].clone();
        let query_str = args[1].clone();

        // Only query shard 0 (schema is identical across shards)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtExplain {
                index_name,
                query_str,
            },
            conn_id: self.state.id,
            response_tx,
        };
        if self.core.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                if let Some((_, resp)) = partial.results.into_iter().next() {
                    if cli_mode {
                        // FT.EXPLAINCLI returns array of strings (one per line)
                        if let Response::Bulk(Some(ref b)) = resp {
                            let text = String::from_utf8_lossy(b);
                            let lines: Vec<Response> = text
                                .lines()
                                .filter(|l| !l.is_empty())
                                .map(|l| Response::bulk(Bytes::from(l.to_string())))
                                .collect();
                            return Response::Array(lines);
                        }
                    }
                    resp
                } else {
                    Response::error("ERR empty response")
                }
            }
            Ok(Err(_)) => Response::error("ERR shard dropped request"),
            Err(_) => Response::error("ERR timeout"),
        }
    }
}
