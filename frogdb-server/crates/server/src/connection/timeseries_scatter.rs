//! Timeseries scatter-gather command handlers (TS.QUERYINDEX, TS.MGET, TS.MRANGE/MREVRANGE).

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;

use crate::connection::{ConnectionHandler, next_txid};
use crate::scatter::{SortedByKey, SortedUnion};

impl ConnectionHandler {
    /// Handle TS.QUERYINDEX - scatter to all shards, collect matching keys.
    pub(crate) async fn handle_ts_queryindex(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ts.queryindex' command");
        }

        let args = args.to_vec();
        self.scatter_gather()
            .run(Box::new(SortedUnion::default()), |_shard, response_tx| {
                ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::TsQueryIndex { args: args.clone() },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
    }

    /// Handle TS.MGET - scatter to all shards, collect [key, labels, sample] tuples.
    pub(crate) async fn handle_ts_mget(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ts.mget' command");
        }

        let args = args.to_vec();
        self.scatter_gather()
            .run(Box::new(SortedByKey::default()), |_shard, response_tx| {
                ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::TsMget { args: args.clone() },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
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

        let args = args.to_vec();
        self.scatter_gather()
            .run(Box::new(SortedByKey::default()), |_shard, response_tx| {
                ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::TsMrange {
                        args: args.clone(),
                        reverse,
                    },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
    }
}
