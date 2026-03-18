//! Timeseries scatter-gather command handlers (TS.QUERYINDEX, TS.MGET, TS.MRANGE/MREVRANGE).

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle TS.QUERYINDEX - scatter to all shards, collect matching keys.
    pub(crate) async fn handle_ts_queryindex(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ts.queryindex' command");
        }

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::TsQueryIndex {
                    args: args.to_vec(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut all_keys: Vec<Bytes> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (key, _) in partial.results {
                        all_keys.push(key);
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped TS.QUERYINDEX request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "TS.QUERYINDEX timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        all_keys.sort();
        Response::Array(all_keys.into_iter().map(Response::bulk).collect())
    }

    /// Handle TS.MGET - scatter to all shards, collect [key, labels, sample] tuples.
    pub(crate) async fn handle_ts_mget(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ts.mget' command");
        }

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::TsMget {
                    args: args.to_vec(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut all_results: Vec<(Bytes, Response)> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    all_results.extend(partial.results);
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped TS.MGET request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "TS.MGET timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort by key for consistency
        all_results.sort_by(|a, b| a.0.cmp(&b.0));
        Response::Array(all_results.into_iter().map(|(_, r)| r).collect())
    }

    /// Handle TS.MRANGE / TS.MREVRANGE - scatter to all shards, collect results.
    pub(crate) async fn handle_ts_mrange(&self, args: &[Bytes], reverse: bool) -> Response {
        if args.len() < 4 {
            let cmd = if reverse { "ts.mrevrange" } else { "ts.mrange" };
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            ));
        }

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::TsMrange {
                    args: args.to_vec(),
                    reverse,
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut all_results: Vec<(Bytes, Response)> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    all_results.extend(partial.results);
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped TS.MRANGE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "TS.MRANGE timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort by key for consistency
        all_results.sort_by(|a, b| a.0.cmp(&b.0));
        Response::Array(all_results.into_iter().map(|(_, r)| r).collect())
    }
}
