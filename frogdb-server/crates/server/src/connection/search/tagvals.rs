//! FT.TAGVALS handler.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;

use super::merge::TagValsUnion;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.TAGVALS - scatter to all shards, union results.
    pub(crate) async fn handle_ft_tagvals(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.tagvals' command");
        }
        let index_name = args[0].clone();
        let field_name = args[1].clone();

        self.scatter_gather()
            .run(Box::new(TagValsUnion::default()), |_shard, response_tx| {
                ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FtTagvals {
                        index_name: index_name.clone(),
                        field_name: field_name.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
    }
}
