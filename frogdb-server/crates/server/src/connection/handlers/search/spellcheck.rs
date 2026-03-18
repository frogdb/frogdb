//! FT.SPELLCHECK handler.

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle FT.SPELLCHECK - scatter to all shards, merge suggestions.
    pub(crate) async fn handle_ft_spellcheck(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.spellcheck' command");
        }
        let index_name = args[0].clone();
        let query_args: Vec<Bytes> = args[1..].to_vec();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtSpellcheck {
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

        // Collect shard results. Each shard returns:
        // Array([ Array(["TERM", term, Array([Array([score, suggestion]), ...])]), ... ])
        // We merge by unioning suggestions per term and re-sorting by score.
        let mut term_map: std::collections::HashMap<
            String,
            std::collections::HashMap<String, f64>,
        > = std::collections::HashMap::new();

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in partial.results {
                        if let Response::Error(_) = &resp {
                            return resp;
                        }
                        if let Response::Array(term_entries) = resp {
                            for entry in term_entries {
                                if let Response::Array(parts) = entry {
                                    // parts: ["TERM", misspelled_term, Array([...])]
                                    if parts.len() >= 3 {
                                        let term = match &parts[1] {
                                            Response::Bulk(Some(b)) => {
                                                String::from_utf8_lossy(b).to_string()
                                            }
                                            _ => continue,
                                        };
                                        if let Response::Array(suggestions) = &parts[2] {
                                            let suggestions_map = term_map.entry(term).or_default();
                                            for sugg in suggestions {
                                                if let Response::Array(pair) = sugg
                                                    && pair.len() >= 2
                                                {
                                                    let score = match &pair[0] {
                                                        Response::Bulk(Some(b)) => {
                                                            std::str::from_utf8(b)
                                                                .ok()
                                                                .and_then(|s| s.parse().ok())
                                                                .unwrap_or(0.0)
                                                        }
                                                        _ => 0.0,
                                                    };
                                                    let word = match &pair[1] {
                                                        Response::Bulk(Some(b)) => {
                                                            String::from_utf8_lossy(b).to_string()
                                                        }
                                                        _ => continue,
                                                    };
                                                    let e =
                                                        suggestions_map.entry(word).or_insert(0.0);
                                                    if score > *e {
                                                        *e = score;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.SPELLCHECK request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.SPELLCHECK timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Build merged response
        let mut term_entries: Vec<Response> = term_map
            .into_iter()
            .map(|(term, suggestions)| {
                let mut sorted: Vec<(String, f64)> = suggestions.into_iter().collect();
                sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                let suggestion_items: Vec<Response> = sorted
                    .into_iter()
                    .map(|(word, score)| {
                        Response::Array(vec![
                            Response::bulk(Bytes::from(format!("{}", score))),
                            Response::bulk(Bytes::from(word)),
                        ])
                    })
                    .collect();
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"TERM")),
                    Response::bulk(Bytes::from(term)),
                    Response::Array(suggestion_items),
                ])
            })
            .collect();
        // Sort term entries for deterministic output
        term_entries.sort_by(|a, b| {
            let key_a = if let Response::Array(parts) = a {
                if let Some(Response::Bulk(Some(b))) = parts.get(1) {
                    String::from_utf8_lossy(b).to_string()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            let key_b = if let Response::Array(parts) = b {
                if let Some(Response::Bulk(Some(b))) = parts.get(1) {
                    String::from_utf8_lossy(b).to_string()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            key_a.cmp(&key_b)
        });

        Response::Array(term_entries)
    }
}
