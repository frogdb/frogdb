//! FT.SPELLCHECK handler.

use bytes::Bytes;
use frogdb_core::{CoreMsg, ScatterOp};
use frogdb_protocol::Response;

use super::merge::SpellcheckMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.SPELLCHECK - scatter to all shards, merge suggestions.
    pub(crate) async fn handle_ft_spellcheck(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.spellcheck' command");
        }
        let index_name = args[0].clone();
        let query_args: Vec<Bytes> = args[1..].to_vec();

        self.scatter_gather()
            .run(
                Box::new(SpellcheckMerge::default()),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FtSpellcheck {
                        index_name: index_name.clone(),
                        query_args: query_args.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }
}
