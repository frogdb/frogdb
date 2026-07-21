//! FT.AGGREGATE handler.

use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use frogdb_search::FtAggregateRequest;

use super::merge::FtAggregateMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.AGGREGATE - scatter-gather aggregation across all shards.
    ///
    /// The pipeline is parsed exactly once, here, into typed steps carried by
    /// [`FtAggregateRequest`]; each shard runs the shard-local prefix and the
    /// merge runs the merge steps from the same vector.
    pub(crate) async fn handle_ft_aggregate(&self, args: &[bytes::Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.aggregate' command");
        }

        let index_name = args[0].clone();
        let request = match FtAggregateRequest::parse(&args[1..]) {
            Ok(r) => r,
            Err(e) => return Response::error(format!("ERR {e}")),
        };

        let effective_timeout = match request.timeout {
            Some(t) => t.min(self.scatter_gather_timeout),
            None => self.scatter_gather_timeout,
        };

        // Fan out to all shards; the typed partial-aggregate gather, pipeline
        // merge, and WITHCURSOR stashing all live in `FtAggregateMerge`.
        let merge = Box::new(FtAggregateMerge {
            steps: request.steps.clone(),
            withcursor: request.withcursor,
            cursor_count: request.cursor_count,
            cursor_maxidle_ms: request.cursor_maxidle_ms,
            index_name: index_name.clone(),
            cursor_store: self.admin.cursor_store.clone(),
            error: None,
            partials: Vec::new(),
        });
        let request = Box::new(request);
        self.scatter_gather_with_timeout(effective_timeout)
            .run(merge, |_shard, response_tx| ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtAggregate {
                    index_name: index_name.clone(),
                    request: request.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            })
            .await
    }
}
