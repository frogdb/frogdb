//! Scatter-gather command handlers.
//!
//! This module handles commands that need to be executed across
//! multiple shards and have their results merged:
//! - SCAN - Scan keys with cursor
//! - KEYS - Find keys matching pattern
//! - DBSIZE - Count total keys
//! - RANDOMKEY - Get a random key
//! - FLUSHDB/FLUSHALL - Flush databases

use bytes::Bytes;
use frogdb_core::{CoreMsg, KeyType, PartialResult, ScatterOp};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::{ConnectionHandler, next_txid};
use crate::scatter::{AllOk, ScatterGather, SortedUnion, SumIntegers};

impl ConnectionHandler {
    /// Bind a lock-free broadcast coordinator over this connection's shard
    /// senders, using the default scatter-gather timeout. Every broadcast
    /// command states only its per-shard message and its merge; the fan-out,
    /// the single shared deadline, and the send-failure / drop / timeout error
    /// mapping live in [`ScatterGather::run`].
    pub(crate) fn scatter_gather(&self) -> ScatterGather<'_> {
        ScatterGather::new(
            self.core.shard_senders.as_slice(),
            self.scatter_gather_timeout,
            self.state.id,
        )
    }

    /// Bind a broadcast coordinator with a per-command timeout (e.g. FT.SEARCH's
    /// `TIMEOUT` override, already clamped to the scatter-gather timeout).
    pub(crate) fn scatter_gather_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> ScatterGather<'_> {
        ScatterGather::new(self.core.shard_senders.as_slice(), timeout, self.state.id)
    }

    /// Handle SCAN command - scan keys across all shards with cursor.
    pub(crate) async fn handle_scan(&self, args: &[Bytes]) -> Response {
        use frogdb_commands::scan::cursor;

        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'scan' command");
        }

        // Parse cursor
        let cursor_str = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid cursor"),
        };
        let encoded_cursor: u64 = match cursor_str.parse() {
            Ok(c) => c,
            Err(_) => return Response::error("ERR invalid cursor"),
        };

        // Decode cursor to get shard_id and position
        let (shard_id, position) = cursor::decode(encoded_cursor);

        // Parse optional arguments
        let mut pattern: Option<Bytes> = None;
        let mut count: usize = 10;
        let mut key_type: Option<KeyType> = None;

        let mut i = 1;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    pattern = Some(args[i].clone());
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    count = match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(c) => c,
                        None => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    };
                }
                b"TYPE" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let type_str = args[i].to_ascii_lowercase();
                    key_type = match type_str.as_slice() {
                        b"string" => Some(KeyType::String),
                        b"list" => Some(KeyType::List),
                        b"set" => Some(KeyType::Set),
                        b"zset" => Some(KeyType::SortedSet),
                        b"hash" => Some(KeyType::Hash),
                        b"stream" => Some(KeyType::Stream),
                        _ => {
                            return Response::error(format!(
                                "ERR unknown type: {}",
                                String::from_utf8_lossy(&type_str)
                            ));
                        }
                    };
                }
                _ => return Response::error("ERR syntax error"),
            }
            i += 1;
        }

        // If shard_id is beyond our shards, we're done
        if shard_id as usize >= self.num_shards {
            return Response::Array(vec![
                Response::bulk(Bytes::from_static(b"0")),
                Response::Array(vec![]),
            ]);
        }

        // Iterate through shards, collecting keys
        let mut all_keys = Vec::new();
        let mut next_shard = shard_id as usize;
        let mut next_position = position;

        while all_keys.len() < count && next_shard < self.num_shards {
            // Send scan request to current shard
            let (response_tx, response_rx) = oneshot::channel();
            let remaining = count - all_keys.len();

            let msg = CoreMsg::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::Scan {
                    cursor: next_position,
                    count: remaining,
                    pattern: pattern.clone(),
                    key_type,
                },
                conn_id: self.state.id,
                response_tx,
            };

            // SCAN keeps its bespoke cursor walk but borrows the shared
            // per-shard send/timeout helper for each step.
            let partial = match self
                .scatter_gather()
                .query_one(next_shard, msg, response_rx)
                .await
            {
                Ok(partial) => partial,
                Err(resp) => return resp,
            };

            // Extract cursor and keys from response
            let mut shard_next_cursor = 0u64;
            for (key, response) in partial.results {
                if key.as_ref() == b"__cursor__" {
                    if let Response::Integer(c) = response {
                        shard_next_cursor = c as u64;
                    }
                } else {
                    all_keys.push(key);
                }
            }

            if shard_next_cursor == 0 {
                // Shard exhausted, move to next shard
                next_shard += 1;
                next_position = 0;
            } else {
                // More keys in this shard
                next_position = shard_next_cursor;
                break; // We have a valid cursor, stop
            }
        }

        // Encode next cursor
        let final_cursor = if next_shard >= self.num_shards {
            0 // Done
        } else {
            cursor::encode(next_shard as u16, next_position)
        };

        // Build response
        let key_responses: Vec<Response> = all_keys.into_iter().map(Response::bulk).collect();

        Response::Array(vec![
            Response::bulk(Bytes::from(final_cursor.to_string())),
            Response::Array(key_responses),
        ])
    }

    /// Handle KEYS command - scatter-gather across all shards.
    pub(crate) async fn handle_keys(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'keys' command");
        }

        let pattern = args[0].clone();

        self.scatter_gather()
            .run(Box::new(SortedUnion::default()), |_shard, response_tx| {
                CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::Keys {
                        pattern: pattern.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
    }

    /// Handle DBSIZE command - sum key counts from all shards.
    pub(crate) async fn handle_dbsize(&self) -> Response {
        self.scatter_gather()
            .run(
                Box::<SumIntegers<PartialResult>>::default(),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::DbSize,
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle RANDOMKEY command - return a random key using weighted shard selection.
    pub(crate) async fn handle_randomkey(&self) -> Response {
        use rand::RngExt;

        // Phase 1 (select): tally per-shard key counts via the shared per-shard
        // send/timeout helper, weighting the shard selection below.
        let mut shard_counts: Vec<(usize, i64)> = Vec::with_capacity(self.num_shards);
        let mut total_keys: i64 = 0;
        for shard_id in 0..self.num_shards {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = CoreMsg::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                conn_id: self.state.id,
                response_tx,
            };
            let partial = match self
                .scatter_gather()
                .query_one(shard_id, msg, response_rx)
                .await
            {
                Ok(partial) => partial,
                Err(resp) => return resp,
            };
            for (_, response) in partial.results {
                if let Response::Integer(count) = response {
                    shard_counts.push((shard_id, count));
                    total_keys += count;
                }
            }
        }

        // If database is empty, return nil
        if total_keys == 0 {
            return Response::null();
        }

        // Phase 2: Select shard probabilistically (weighted by key count)
        let selected_shard = {
            let mut rng = rand::rng();
            let selection = rng.random_range(0..total_keys);
            let mut cumulative: i64 = 0;
            let mut selected: usize = 0;

            for (shard_id, count) in &shard_counts {
                cumulative += count;
                if selection < cumulative {
                    selected = *shard_id;
                    break;
                }
            }
            selected
        };

        // Phase 3 (fetch): request a random key from the selected shard.
        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::RandomKey,
            conn_id: self.state.id,
            response_tx,
        };
        match self
            .scatter_gather()
            .query_one(selected_shard, msg, response_rx)
            .await
        {
            // Return the random key (or null if the shard is now empty).
            Ok(partial) => partial
                .results
                .into_iter()
                .next()
                .map(|(_, response)| response)
                .unwrap_or_else(Response::null),
            Err(resp) => resp,
        }
    }

    /// Handle FLUSHDB command - clear all shards.
    pub(crate) async fn handle_flushdb(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC/SYNC argument (we only support SYNC for now)
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            if mode.as_slice() != b"ASYNC" && mode.as_slice() != b"SYNC" {
                return Response::error("ERR syntax error");
            }
        }

        self.scatter_gather()
            .run(
                Box::<AllOk<PartialResult>>::default(),
                |_shard, response_tx| CoreMsg::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FlushDb,
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle FLUSHALL command - same as FLUSHDB (single database).
    pub(crate) async fn handle_flushall(&self, args: &[Bytes]) -> Response {
        self.handle_flushdb(args).await
    }
}
