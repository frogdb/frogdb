//! FT.SEARCH handler.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;

use super::merge::FtSearchMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.SEARCH - fan out to all shards, merge results.
    pub(crate) async fn handle_ft_search(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.search' command");
        }

        let index_name = args[0].clone();
        let query_args: Vec<Bytes> = args[1..].to_vec();

        // Parse options from args for global merge
        let mut global_offset = 0usize;
        let mut global_limit = 10usize;
        let mut nocontent = false;
        let mut withscores = false;
        let mut sortby_active = false;
        let mut sortby_desc = false;
        // Numeric-sort detection now happens during the merge as sort values
        // arrive; it starts false here.
        let sortby_numeric = false;
        let mut timeout_override: Option<Duration> = None;
        let mut i = 1; // skip query string
        while i < query_args.len() {
            let arg_upper = query_args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"LIMIT" => {
                    if i + 2 < query_args.len() {
                        global_offset = std::str::from_utf8(&query_args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        global_limit = std::str::from_utf8(&query_args[i + 2])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(10);
                        i += 3;
                    } else {
                        i += 1;
                    }
                }
                b"NOCONTENT" => {
                    nocontent = true;
                    i += 1;
                }
                b"WITHSCORES" => {
                    withscores = true;
                    i += 1;
                }
                b"SORTBY" => {
                    if i + 1 < query_args.len() {
                        sortby_active = true;
                        if i + 2 < query_args.len() {
                            let dir = query_args[i + 2].to_ascii_uppercase();
                            if dir.as_slice() == b"DESC" {
                                sortby_desc = true;
                                i += 3;
                            } else if dir.as_slice() == b"ASC" {
                                i += 3;
                            } else {
                                i += 2;
                            }
                        } else {
                            i += 2;
                        }
                    } else {
                        i += 1;
                    }
                }
                b"TIMEOUT" => {
                    if i + 1 < query_args.len() {
                        if let Ok(ms) = std::str::from_utf8(&query_args[i + 1])
                            .unwrap_or("0")
                            .parse::<u64>()
                            && ms > 0
                        {
                            timeout_override = Some(Duration::from_millis(ms));
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let effective_timeout = match timeout_override {
            Some(t) => t.min(self.scatter_gather_timeout),
            None => self.scatter_gather_timeout,
        };

        // Detect KNN queries: lower distance = more similar, so sort ascending.
        let is_knn = std::str::from_utf8(&query_args[0])
            .ok()
            .map(|q| q.to_ascii_uppercase().contains("=>[KNN"))
            .unwrap_or(false);

        // Fan out to all shards — overfetch so we can apply the global
        // offset+limit after merging (see `FtSearchMerge`).
        let merge = Box::new(FtSearchMerge {
            sortby_active,
            sortby_desc,
            sortby_numeric,
            nocontent,
            withscores,
            is_knn,
            global_offset,
            global_limit,
            error: None,
            all_hits: Vec::new(),
            total: 0,
        });
        self.scatter_gather_with_timeout(effective_timeout)
            .run(merge, |_shard, response_tx| ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtSearch {
                    index_name: index_name.clone(),
                    query_args: query_args.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            })
            .await
    }
}
