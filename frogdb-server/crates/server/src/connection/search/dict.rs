//! FT.DICTADD, FT.DICTDEL, FT.DICTDUMP handlers.

use bytes::Bytes;
use frogdb_core::ScatterOp;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle FT.DICTADD - broadcast to all shards.
    pub(crate) async fn handle_ft_dictadd(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.dictadd' command");
        }
        let dict_name = args[0].clone();
        let terms: Vec<Bytes> = args[1..].to_vec();

        self.broadcast_and_return_shard0_response(ScatterOp::FtDictadd { dict_name, terms })
            .await
    }

    /// Handle FT.DICTDEL - broadcast to all shards.
    pub(crate) async fn handle_ft_dictdel(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.dictdel' command");
        }
        let dict_name = args[0].clone();
        let terms: Vec<Bytes> = args[1..].to_vec();

        self.broadcast_and_return_shard0_response(ScatterOp::FtDictdel { dict_name, terms })
            .await
    }

    /// Handle FT.DICTDUMP - query shard 0 only.
    pub(crate) async fn handle_ft_dictdump(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.dictdump' command");
        }
        let dict_name = args[0].clone();

        self.query_shard0(ScatterOp::FtDictdump { dict_name }).await
    }
}
