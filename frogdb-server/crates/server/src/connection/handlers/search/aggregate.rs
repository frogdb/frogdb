//! FT.AGGREGATE handler.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;

use super::merge::FtAggregateMerge;
use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.AGGREGATE - scatter-gather aggregation across all shards.
    pub(crate) async fn handle_ft_aggregate(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.aggregate' command");
        }

        let index_name = args[0].clone();

        // Scan for TIMEOUT, WITHCURSOR, DIALECT before forwarding to shards.
        // Strip these coordinator-only options from the args sent to shards.
        let mut timeout_override: Option<Duration> = None;
        let mut withcursor = false;
        let mut cursor_count: usize = 0; // 0 = return all
        let mut cursor_maxidle_ms: u64 = 300_000; // default 300s
        let mut shard_args: Vec<Bytes> = Vec::with_capacity(args.len());
        shard_args.push(args[1].clone()); // query string
        {
            let mut i = 2;
            while i < args.len() {
                let upper = args[i].to_ascii_uppercase();
                match upper.as_slice() {
                    b"TIMEOUT" => {
                        if i + 1 < args.len() {
                            if let Ok(ms) = std::str::from_utf8(&args[i + 1])
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
                    b"DIALECT" => {
                        // Accept and ignore
                        if i + 1 < args.len() {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    b"WITHCURSOR" => {
                        withcursor = true;
                        i += 1;
                        // Parse optional COUNT and MAXIDLE
                        while i < args.len() {
                            let sub = args[i].to_ascii_uppercase();
                            if sub.as_slice() == b"COUNT" && i + 1 < args.len() {
                                cursor_count = std::str::from_utf8(&args[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(0);
                                i += 2;
                            } else if sub.as_slice() == b"MAXIDLE" && i + 1 < args.len() {
                                cursor_maxidle_ms = std::str::from_utf8(&args[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(300_000);
                                i += 2;
                            } else {
                                break;
                            }
                        }
                    }
                    _ => {
                        shard_args.push(args[i].clone());
                        i += 1;
                    }
                }
            }
        }
        let query_args = shard_args;

        let effective_timeout = match timeout_override {
            Some(t) => t.min(self.scatter_gather_timeout),
            None => self.scatter_gather_timeout,
        };

        // Parse the pipeline steps for merging (need them at coordinator level too)
        let pipeline_strs: Vec<&str> = query_args[1..]
            .iter()
            .filter_map(|b| std::str::from_utf8(b).ok())
            .collect();
        let steps = match frogdb_search::aggregate::parse_aggregate_pipeline(&pipeline_strs) {
            Ok(s) => s,
            Err(e) => return Response::error(format!("ERR {e}")),
        };

        // Fan out to all shards; the partial-aggregate decode, pipeline merge,
        // and WITHCURSOR stashing all live in `FtAggregateMerge`.
        let merge = Box::new(FtAggregateMerge {
            steps,
            withcursor,
            cursor_count,
            cursor_maxidle_ms,
            index_name: index_name.clone(),
            cursor_store: self.admin.cursor_store.clone(),
            error: None,
            partials: Vec::new(),
        });
        self.scatter_gather_with_timeout(effective_timeout)
            .run(merge, |_shard, response_tx| ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtAggregate {
                    index_name: index_name.clone(),
                    query_args: query_args.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            })
            .await
    }
}
