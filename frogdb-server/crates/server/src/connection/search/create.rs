//! FT.CREATE handler.

use bytes::Bytes;
use frogdb_core::{CoreMsg, ScatterOp};
use frogdb_protocol::Response;

use super::merge::OkOrFirstError;
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

        // Broadcast to ALL shards; OK unless a shard reports an error.
        let json = Bytes::from(json);
        self.scatter_gather()
            .run(
                Box::new(OkOrFirstError::default()),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FtCreate {
                        index_def_json: json.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }
}
