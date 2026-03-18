//! FT.SEARCH handler.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

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
        let mut sortby_numeric = false;
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

        // Fan out to all shards — overfetch so we can apply global offset+limit
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtSearch {
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

        // Collect all results: (key, score, sort_value_str, response)
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
                            // Extract sort value if SORTBY is active (second element)
                            let sort_val = if sortby_active && items.len() > 1 {
                                if let Response::Bulk(Some(ref sv_bytes)) = items[1] {
                                    // Detect if numeric for sort ordering
                                    let sv =
                                        std::str::from_utf8(sv_bytes).unwrap_or("").to_string();
                                    if !sortby_numeric && sv.parse::<f64>().is_ok() {
                                        sortby_numeric = true;
                                    }
                                    sv
                                } else {
                                    String::new()
                                }
                            } else {
                                String::new()
                            };
                            all_hits.push((key, score, sort_val, resp));
                            continue;
                        }
                        all_hits.push((key, 0.0, String::new(), resp));
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.SEARCH request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.SEARCH timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Detect KNN queries: lower distance = more similar, so sort ascending
        let is_knn = std::str::from_utf8(&query_args[0])
            .ok()
            .map(|q| q.to_ascii_uppercase().contains("=>[KNN"))
            .unwrap_or(false);

        // Sort results
        if sortby_active {
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
        } else if is_knn {
            // KNN: sort by distance ascending (lower = more similar)
            all_hits.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            // BM25: sort by score descending (higher = more relevant)
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
                let mut idx = 1; // skip internal score

                // Skip sort value element if SORTBY was active
                if sortby_active && idx < items.len() {
                    idx += 1;
                }

                if withscores && idx < items.len() {
                    response_items.push(items[idx].clone());
                    idx += 1;
                }

                if !nocontent && idx < items.len() {
                    response_items.push(items[idx].clone());
                }
            }
        }

        Response::Array(response_items)
    }
}
