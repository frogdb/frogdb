//! FT.CURSOR READ/DEL handler.

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle FT.CURSOR READ/DEL - coordinator-only cursor management.
    pub(crate) async fn handle_ft_cursor(&self, args: &[Bytes]) -> Response {
        if args.len() < 3 {
            return Response::error("ERR wrong number of arguments for 'ft.cursor' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let index_name = std::str::from_utf8(&args[1]).unwrap_or("");
        let cursor_id_str = std::str::from_utf8(&args[2]).unwrap_or("");
        let cursor_id: u64 = match cursor_id_str.parse() {
            Ok(id) => id,
            Err(_) => return Response::error("ERR invalid cursor id"),
        };

        let store = &self.admin.cursor_store;

        match subcommand.as_slice() {
            b"READ" => {
                // Parse optional COUNT
                let count_override = if args.len() >= 5 && args[3].eq_ignore_ascii_case(b"COUNT") {
                    std::str::from_utf8(&args[4])
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                } else {
                    None
                };

                match store.read_cursor(cursor_id, count_override, index_name) {
                    Some((rows, new_cursor_id)) => {
                        let mut result_items = Vec::new();
                        result_items.push(Response::Integer(rows.len() as i64));
                        for row in &rows {
                            let mut field_array = Vec::new();
                            for (k, v) in row {
                                field_array.push(Response::bulk(Bytes::from(k.clone())));
                                field_array.push(Response::bulk(Bytes::from(v.clone())));
                            }
                            result_items.push(Response::Array(field_array));
                        }

                        Response::Array(vec![
                            Response::Array(result_items),
                            Response::Integer(new_cursor_id as i64),
                        ])
                    }
                    None => Response::error("ERR Cursor not found"),
                }
            }
            b"DEL" => {
                if store.delete_cursor(cursor_id) {
                    Response::ok()
                } else {
                    Response::error("ERR Cursor not found")
                }
            }
            _ => Response::error(format!(
                "ERR unknown FT.CURSOR subcommand '{}'",
                String::from_utf8_lossy(&subcommand)
            )),
        }
    }
}
