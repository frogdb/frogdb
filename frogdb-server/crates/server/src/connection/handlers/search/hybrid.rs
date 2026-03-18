//! FT.HYBRID handler.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

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

        // Fan out to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtHybrid {
                    index_name: index_name.clone(),
                    query_args: query_args.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Collect results: (key, fused_score, response)
        let mut all_hits: Vec<(Bytes, f32, String, Response)> = Vec::new();
        let mut total: usize = 0;
        for (shard_id, rx) in handles {
            match tokio::time::timeout(effective_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (key, resp) in partial.results {
                        if let Response::Error(_) = &resp {
                            return resp;
                        }
                        if key.as_ref() == b"__ft_total__" {
                            if let Response::Integer(n) = &resp {
                                total += *n as usize;
                            }
                            continue;
                        }
                        if let Response::Array(ref items) = resp
                            && !items.is_empty()
                            && let Response::Bulk(Some(ref score_bytes)) = items[0]
                            && let Ok(s) = std::str::from_utf8(score_bytes)
                            && let Ok(score) = s.parse::<f32>()
                        {
                            all_hits.push((key, score, String::new(), resp));
                            continue;
                        }
                        all_hits.push((key, 0.0, String::new(), resp));
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.HYBRID request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.HYBRID timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort results
        if nosort {
            // No sorting
        } else if sortby_active {
            all_hits.sort_by(|a, b| {
                let cmp = if sortby_numeric {
                    let va: f64 = a.2.parse().unwrap_or(0.0);
                    let vb: f64 = b.2.parse().unwrap_or(0.0);
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a.2.cmp(&b.2)
                };
                if sortby_desc { cmp.reverse() } else { cmp }
            });
        } else {
            // Default: sort by fused score descending (higher = better)
            all_hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        }

        let hits: Vec<_> = all_hits
            .into_iter()
            .skip(global_offset)
            .take(global_limit)
            .collect();

        // Build RediSearch-format response: [total, key1, [fields...], key2, [fields...], ...]
        let mut response_items = Vec::new();
        response_items.push(Response::Integer(total as i64));

        for (key, _score, _sort_val, resp) in hits {
            response_items.push(Response::bulk(key));

            if let Response::Array(items) = resp {
                let idx = 1; // skip internal fused score

                if idx < items.len() {
                    response_items.push(items[idx].clone());
                }
            }
        }

        Response::Array(response_items)
    }
}
