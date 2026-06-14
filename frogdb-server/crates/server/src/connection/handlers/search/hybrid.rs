//! FT.HYBRID handler.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;

use super::merge::FtHybridMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.HYBRID - scatter-gather hybrid search across all shards.
    pub(crate) async fn handle_ft_hybrid(&self, args: &[Bytes]) -> Response {
        if args.len() < 6 {
            return Response::error("ERR wrong number of arguments for 'ft.hybrid' command");
        }

        let index_name = args[0].clone();
        let query_args: Vec<Bytes> = args[1..].to_vec();

        // Parse coordinator-level options from args
        let mut global_offset = 0usize;
        let mut global_limit = 10usize;
        let mut sortby_active = false;
        let mut sortby_desc = false;
        let sortby_numeric = false;
        let mut nosort = false;
        let mut timeout_override: Option<Duration> = None;

        let mut i = 1; // skip first arg (which is part of SEARCH/VSIM/COMBINE)
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
                b"NOSORT" => {
                    nosort = true;
                    i += 1;
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

        // Fan out to all shards; merge fused-score hits and apply the global
        // offset+limit after sorting (see `FtHybridMerge`).
        let merge = Box::new(FtHybridMerge {
            sortby_active,
            sortby_desc,
            sortby_numeric,
            nosort,
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
                operation: ScatterOp::FtHybrid {
                    index_name: index_name.clone(),
                    query_args: query_args.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            })
            .await
    }
}
