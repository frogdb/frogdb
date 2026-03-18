//! FT.PROFILE handler.

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle FT.PROFILE - wraps FT.SEARCH or FT.AGGREGATE with timing info.
    ///
    /// Protocol: `FT.PROFILE <index> SEARCH|AGGREGATE [LIMITED] QUERY <query> [args...]`
    /// Returns a 2-element array: [query_result, profile_info].
    pub(crate) async fn handle_ft_profile(&self, args: &[Bytes]) -> Response {
        if args.len() < 4 {
            return Response::error("ERR wrong number of arguments for 'ft.profile' command");
        }

        let index_name = &args[0];
        let mut i = 1;

        // Parse SEARCH or AGGREGATE
        let sub_cmd = args[i].to_ascii_uppercase();
        let is_search = match sub_cmd.as_slice() {
            b"SEARCH" => true,
            b"AGGREGATE" => false,
            _ => {
                return Response::error(
                    "ERR FT.PROFILE expects SEARCH or AGGREGATE as second argument",
                );
            }
        };
        i += 1;

        // Parse optional LIMITED (accepted but treated same as full profiling)
        if i < args.len() && args[i].eq_ignore_ascii_case(b"LIMITED") {
            i += 1;
        }

        // Expect QUERY keyword
        if i >= args.len() || !args[i].eq_ignore_ascii_case(b"QUERY") {
            return Response::error("ERR FT.PROFILE expects QUERY keyword");
        }
        i += 1;

        // Remaining args are the query + options (same as FT.SEARCH/FT.AGGREGATE args after index)
        if i >= args.len() {
            return Response::error("ERR FT.PROFILE missing query string");
        }

        // Build delegated args: [index_name, query_str, ...rest]
        let mut delegated_args = vec![index_name.clone()];
        delegated_args.extend_from_slice(&args[i..]);

        let start = std::time::Instant::now();
        let query_result = if is_search {
            self.handle_ft_search(&delegated_args).await
        } else {
            self.handle_ft_aggregate(&delegated_args).await
        };
        let elapsed_us = start.elapsed().as_micros() as f64;

        // Build profile info matching RediSearch format
        let profile_info = Response::Array(vec![
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"Total profile time")),
                Response::bulk(Bytes::from(format!("{:.2}", elapsed_us / 1000.0))),
            ]),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"Parsing time")),
                Response::bulk(Bytes::from_static(b"0")),
            ]),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"Pipeline creation time")),
                Response::bulk(Bytes::from_static(b"0")),
            ]),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"Warning")),
                Response::bulk(Bytes::from_static(b"None")),
            ]),
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"Iterators profile")),
                Response::Array(vec![]),
            ]),
        ]);

        Response::Array(vec![query_result, profile_info])
    }
}
