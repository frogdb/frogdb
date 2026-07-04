//! FT.SEARCH handler.

use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use frogdb_search::FtSearchRequest;

use super::merge::FtSearchMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.SEARCH - fan out to all shards, merge results.
    ///
    /// The grammar is parsed exactly once, here, into an [`FtSearchRequest`];
    /// every shard receives the parsed request through the scatter op and the
    /// merge reads its knobs from the same struct — the coordinator and the
    /// shards cannot disagree about what an option means.
    pub(crate) async fn handle_ft_search(&self, args: &[bytes::Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.search' command");
        }

        let index_name = args[0].clone();
        let request = FtSearchRequest::parse(&args[1..]);

        let effective_timeout = match request.timeout {
            Some(t) => t.min(self.scatter_gather_timeout),
            None => self.scatter_gather_timeout,
        };

        // Fan out to all shards — each shard overfetches so we can apply the
        // global offset+limit after merging (see `FtSearchMerge`).
        let merge = Box::new(FtSearchMerge::from_request(&request));
        let request = Box::new(request);
        self.scatter_gather_with_timeout(effective_timeout)
            .run(merge, |_shard, response_tx| ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtSearch {
                    index_name: index_name.clone(),
                    request: request.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            })
            .await
    }
}
