//! FT.CONFIG GET/SET handler.

use bytes::Bytes;
use frogdb_core::ScatterOp;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle FT.CONFIG GET/SET.
    pub(crate) async fn handle_ft_config(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.config' command");
        }

        let subcommand = std::str::from_utf8(&args[0])
            .unwrap_or("")
            .to_ascii_uppercase();

        match subcommand.as_str() {
            "GET" => {
                // Query shard 0
                self.query_shard0(ScatterOp::FtConfig {
                    args: args.to_vec(),
                })
                .await
            }
            "SET" => {
                // Broadcast SET to all shards
                self.broadcast_and_check_shard0(ScatterOp::FtConfig {
                    args: args.to_vec(),
                })
                .await
            }
            "HELP" => {
                // Return list of config options
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"MINPREFIX")),
                    Response::bulk(Bytes::from_static(b"MAXEXPANSIONS")),
                    Response::bulk(Bytes::from_static(b"TIMEOUT")),
                    Response::bulk(Bytes::from_static(b"DEFAULT_DIALECT")),
                ])
            }
            _ => Response::error("ERR Unknown subcommand for 'ft.config' command"),
        }
    }
}
