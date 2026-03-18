//! FT.AGGREGATE handler.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

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

        // Fan out to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtAggregate {
                    index_name: index_name.clone(),
                    query_args: query_args.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push(response_rx);
        }

        // Collect partial aggregates from all shards
        let mut partials: Vec<frogdb_search::aggregate::PartialAggregate> = Vec::new();

        for rx in handles {
            match tokio::time::timeout(effective_timeout, rx).await {
                Ok(Ok(partial)) => {
                    // Check for errors
                    for (key, resp) in &partial.results {
                        if key.as_ref() == b"__ft_error__" {
                            return resp.clone();
                        }
                    }

                    // Deserialize partial results back into PartialAggregate
                    let mut groups = Vec::new();
                    for (_key, resp) in partial.results {
                        if let Response::Array(entry) = resp {
                            // Entry format: [field1, val1, field2, val2, ..., Array([states])]
                            let mut key_fields = Vec::new();
                            let mut state_items = Vec::new();

                            for item in &entry {
                                if let Response::Array(states) = item {
                                    state_items = states.clone();
                                }
                            }

                            // Parse key fields (all items except the last Array)
                            let mut idx = 0;
                            while idx + 1 < entry.len() {
                                if matches!(&entry[idx + 1], Response::Array(_)) {
                                    break;
                                }
                                if let (Response::Bulk(Some(k)), Response::Bulk(Some(v))) =
                                    (&entry[idx], &entry[idx + 1])
                                {
                                    let k = String::from_utf8_lossy(k).to_string();
                                    let v = String::from_utf8_lossy(v).to_string();
                                    key_fields.push((k, v));
                                }
                                idx += 2;
                            }

                            // Parse partial reducer states
                            let mut states = Vec::new();
                            let mut si = 0;
                            while si < state_items.len() {
                                if let Response::Bulk(Some(ref tag)) = state_items[si] {
                                    match tag.as_ref() {
                                        b"COUNT" => {
                                            si += 1;
                                            if let Response::Integer(c) = &state_items[si] {
                                                states.push(
                                                    frogdb_search::aggregate::PartialReducerState::Count(*c),
                                                );
                                            }
                                            si += 1;
                                        }
                                        b"SUM" => {
                                            si += 1;
                                            if let Response::Bulk(Some(ref v)) = state_items[si] {
                                                let val: f64 = String::from_utf8_lossy(v)
                                                    .parse()
                                                    .unwrap_or(0.0);
                                                states.push(
                                                    frogdb_search::aggregate::PartialReducerState::Sum(val),
                                                );
                                            }
                                            si += 1;
                                        }
                                        b"MIN" => {
                                            si += 1;
                                            if let Response::Bulk(Some(ref v)) = state_items[si] {
                                                let val: f64 = String::from_utf8_lossy(v)
                                                    .parse()
                                                    .unwrap_or(f64::INFINITY);
                                                states.push(
                                                    frogdb_search::aggregate::PartialReducerState::Min(val),
                                                );
                                            }
                                            si += 1;
                                        }
                                        b"MAX" => {
                                            si += 1;
                                            if let Response::Bulk(Some(ref v)) = state_items[si] {
                                                let val: f64 = String::from_utf8_lossy(v)
                                                    .parse()
                                                    .unwrap_or(f64::NEG_INFINITY);
                                                states.push(
                                                    frogdb_search::aggregate::PartialReducerState::Max(val),
                                                );
                                            }
                                            si += 1;
                                        }
                                        b"AVG" => {
                                            si += 1;
                                            let sum = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                String::from_utf8_lossy(v).parse().unwrap_or(0.0)
                                            } else {
                                                0.0
                                            };
                                            si += 1;
                                            let count =
                                                if let Response::Integer(c) = &state_items[si] {
                                                    *c
                                                } else {
                                                    0
                                                };
                                            si += 1;
                                            states.push(
                                                frogdb_search::aggregate::PartialReducerState::Avg(
                                                    sum, count,
                                                ),
                                            );
                                        }
                                        b"COUNT_DISTINCT" => {
                                            si += 1;
                                            let num = if let Response::Integer(n) = &state_items[si]
                                            {
                                                *n as usize
                                            } else {
                                                0
                                            };
                                            si += 1;
                                            let mut set = std::collections::HashSet::new();
                                            for _ in 0..num {
                                                if si < state_items.len() {
                                                    if let Response::Bulk(Some(ref v)) =
                                                        state_items[si]
                                                    {
                                                        set.insert(
                                                            String::from_utf8_lossy(v).to_string(),
                                                        );
                                                    }
                                                    si += 1;
                                                }
                                            }
                                            states.push(frogdb_search::aggregate::PartialReducerState::CountDistinct(set));
                                        }
                                        b"COUNT_DISTINCTISH" => {
                                            si += 1;
                                            let regs = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                v.to_vec()
                                            } else {
                                                vec![0u8; 256]
                                            };
                                            si += 1;
                                            states.push(frogdb_search::aggregate::PartialReducerState::CountDistinctish(regs));
                                        }
                                        b"TOLIST" => {
                                            si += 1;
                                            let num = if let Response::Integer(n) = &state_items[si]
                                            {
                                                *n as usize
                                            } else {
                                                0
                                            };
                                            si += 1;
                                            let mut list = Vec::with_capacity(num);
                                            for _ in 0..num {
                                                if si < state_items.len() {
                                                    if let Response::Bulk(Some(ref v)) =
                                                        state_items[si]
                                                    {
                                                        list.push(
                                                            String::from_utf8_lossy(v).to_string(),
                                                        );
                                                    }
                                                    si += 1;
                                                }
                                            }
                                            states.push(frogdb_search::aggregate::PartialReducerState::Tolist(list));
                                        }
                                        b"FIRST_VALUE" => {
                                            si += 1;
                                            let value = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                let s = String::from_utf8_lossy(v).to_string();
                                                if s.is_empty() { None } else { Some(s) }
                                            } else {
                                                None
                                            };
                                            si += 1;
                                            let sort_key = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                let s = String::from_utf8_lossy(v).to_string();
                                                if s.is_empty() { None } else { Some(s) }
                                            } else {
                                                None
                                            };
                                            si += 1;
                                            let sort_asc =
                                                if let Response::Integer(a) = &state_items[si] {
                                                    *a != 0
                                                } else {
                                                    true
                                                };
                                            si += 1;
                                            states.push(frogdb_search::aggregate::PartialReducerState::FirstValue { value, sort_key, sort_asc });
                                        }
                                        b"STDDEV" => {
                                            si += 1;
                                            let sum = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                String::from_utf8_lossy(v).parse().unwrap_or(0.0)
                                            } else {
                                                0.0
                                            };
                                            si += 1;
                                            let sum_sq = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                String::from_utf8_lossy(v).parse().unwrap_or(0.0)
                                            } else {
                                                0.0
                                            };
                                            si += 1;
                                            let count =
                                                if let Response::Integer(c) = &state_items[si] {
                                                    *c
                                                } else {
                                                    0
                                                };
                                            si += 1;
                                            states.push(frogdb_search::aggregate::PartialReducerState::Stddev { sum, sum_sq, count });
                                        }
                                        b"QUANTILE" => {
                                            si += 1;
                                            let quantile: f64 = if let Response::Bulk(Some(ref v)) =
                                                state_items[si]
                                            {
                                                String::from_utf8_lossy(v).parse().unwrap_or(0.5)
                                            } else {
                                                0.5
                                            };
                                            si += 1;
                                            let num = if let Response::Integer(n) = &state_items[si]
                                            {
                                                *n as usize
                                            } else {
                                                0
                                            };
                                            si += 1;
                                            let mut values = Vec::with_capacity(num);
                                            for _ in 0..num {
                                                if si < state_items.len()
                                                    && let Response::Bulk(Some(ref v)) =
                                                        state_items[si]
                                                    && let Ok(f) =
                                                        String::from_utf8_lossy(v).parse::<f64>()
                                                {
                                                    values.push(f);
                                                }
                                                if si < state_items.len() {
                                                    si += 1;
                                                }
                                            }
                                            states.push(frogdb_search::aggregate::PartialReducerState::Quantile { values, quantile });
                                        }
                                        b"RANDOM_SAMPLE" => {
                                            si += 1;
                                            let count =
                                                if let Response::Integer(c) = &state_items[si] {
                                                    *c as usize
                                                } else {
                                                    0
                                                };
                                            si += 1;
                                            let seen =
                                                if let Response::Integer(s) = &state_items[si] {
                                                    *s as usize
                                                } else {
                                                    0
                                                };
                                            si += 1;
                                            let num = if let Response::Integer(n) = &state_items[si]
                                            {
                                                *n as usize
                                            } else {
                                                0
                                            };
                                            si += 1;
                                            let mut reservoir = Vec::with_capacity(num);
                                            for _ in 0..num {
                                                if si < state_items.len() {
                                                    if let Response::Bulk(Some(ref v)) =
                                                        state_items[si]
                                                    {
                                                        reservoir.push(
                                                            String::from_utf8_lossy(v).to_string(),
                                                        );
                                                    }
                                                    si += 1;
                                                }
                                            }
                                            states.push(frogdb_search::aggregate::PartialReducerState::RandomSample { reservoir, count, seen });
                                        }
                                        _ => {
                                            si += 1;
                                        }
                                    }
                                } else {
                                    si += 1;
                                }
                            }

                            groups.push((key_fields, states));
                        }
                    }

                    partials.push(frogdb_search::aggregate::PartialAggregate { groups });
                }
                Ok(Err(_)) => {
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    return Response::error("ERR timeout");
                }
            }
        }

        // Merge all partials and apply SORTBY + LIMIT
        let rows = frogdb_search::aggregate::merge_partials(partials, &steps);

        if withcursor && cursor_count > 0 && rows.len() > cursor_count {
            // Return first batch and store remainder in cursor store
            let (first_batch, remaining) = rows.split_at(cursor_count);

            // Build the result array for the first batch
            let mut result_items = Vec::new();
            result_items.push(Response::Integer(first_batch.len() as i64));
            for row in first_batch {
                let mut field_array = Vec::new();
                for (k, v) in row {
                    field_array.push(Response::bulk(Bytes::from(k.clone())));
                    field_array.push(Response::bulk(Bytes::from(v.clone())));
                }
                result_items.push(Response::Array(field_array));
            }

            // Store remaining rows in cursor store
            let cursor_id = {
                let store = &self.admin.cursor_store;
                let idx_name = std::str::from_utf8(&index_name).unwrap_or("").to_string();
                store.create_cursor(
                    remaining.to_vec(),
                    cursor_count,
                    idx_name,
                    Duration::from_millis(cursor_maxidle_ms),
                )
            };

            Response::Array(vec![
                Response::Array(result_items),
                Response::Integer(cursor_id as i64),
            ])
        } else if withcursor {
            // All rows fit in one batch — return cursor_id 0 (done)
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
            Response::Array(vec![Response::Array(result_items), Response::Integer(0)])
        } else {
            // Standard non-cursor response
            let mut response_items = Vec::new();
            response_items.push(Response::Integer(rows.len() as i64));
            for row in &rows {
                let mut field_array = Vec::new();
                for (k, v) in row {
                    field_array.push(Response::bulk(Bytes::from(k.clone())));
                    field_array.push(Response::bulk(Bytes::from(v.clone())));
                }
                response_items.push(Response::Array(field_array));
            }
            Response::Array(response_items)
        }
    }
}
