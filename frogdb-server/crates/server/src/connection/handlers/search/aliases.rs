//! FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE handlers.

use bytes::Bytes;
use frogdb_core::ScatterOp;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle FT.ALIASADD - broadcast to all shards.
    pub(crate) async fn handle_ft_aliasadd(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.aliasadd' command");
        }
        let alias_name = args[0].clone();
        let index_name = args[1].clone();

        self.broadcast_and_check_shard0(ScatterOp::FtAliasadd {
            alias_name,
            index_name,
        })
        .await
    }

    /// Handle FT.ALIASDEL - broadcast to all shards.
    pub(crate) async fn handle_ft_aliasdel(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.aliasdel' command");
        }
        let alias_name = args[0].clone();

        self.broadcast_and_check_shard0(ScatterOp::FtAliasdel { alias_name })
            .await
    }

    /// Handle FT.ALIASUPDATE - broadcast to all shards.
    pub(crate) async fn handle_ft_aliasupdate(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.aliasupdate' command");
        }
        let alias_name = args[0].clone();
        let index_name = args[1].clone();

        self.broadcast_and_check_shard0(ScatterOp::FtAliasupdate {
            alias_name,
            index_name,
        })
        .await
    }
}
