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
use frogdb_core::{KeyType, ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
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

            let msg = ShardMessage::ScatterRequest {
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

            if self.shard_senders[next_shard].send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }

            match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
                Ok(Ok(partial)) => {
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
                Ok(Err(_)) => return Response::error("ERR shard dropped request"),
                Err(_) => return Response::error("ERR scan timeout"),
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

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::Keys {
                    pattern: pattern.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Gather all keys
        let mut all_keys = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (key, _) in partial.results {
                        all_keys.push(key);
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped KEYS request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "KEYS timeout");
                    return Response::error("ERR keys timeout");
                }
            }
        }

        // Sort keys for consistency
        all_keys.sort();

        Response::Array(all_keys.into_iter().map(Response::bulk).collect())
    }

    /// Handle DBSIZE command - sum key counts from all shards.
    pub(crate) async fn handle_dbsize(&self) -> Response {
        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Sum counts
        let mut total: i64 = 0;
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, response) in partial.results {
                        if let Response::Integer(count) = response {
                            total += count;
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped DBSIZE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "DBSIZE timeout");
                    return Response::error("ERR dbsize timeout");
                }
            }
        }

        Response::Integer(total)
    }

    /// Handle RANDOMKEY command - return a random key using weighted shard selection.
    pub(crate) async fn handle_randomkey(&self) -> Response {
        use rand::Rng;

        // Phase 1: Get key counts from all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Collect key counts per shard
        let mut shard_counts: Vec<(usize, i64)> = Vec::with_capacity(self.num_shards);
        let mut total_keys: i64 = 0;

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, response) in partial.results {
                        if let Response::Integer(count) = response {
                            shard_counts.push((shard_id, count));
                            total_keys += count;
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped DBSIZE request for RANDOMKEY");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "DBSIZE timeout for RANDOMKEY");
                    return Response::error("ERR timeout");
                }
            }
        }

        // If database is empty, return nil
        if total_keys == 0 {
            return Response::null();
        }

        // Phase 2: Select shard probabilistically (weighted by key count)
        let selected_shard = {
            let mut rng = rand::thread_rng();
            let selection = rng.gen_range(0..total_keys);
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

        // Phase 3: Request random key from selected shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::RandomKey,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[selected_shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                // Return the random key (or null if shard is now empty)
                if let Some((_, response)) = partial.results.into_iter().next() {
                    return response;
                }
                Response::null()
            }
            Ok(Err(_)) => {
                warn!(selected_shard, "Shard dropped RANDOMKEY request");
                Response::error("ERR shard dropped request")
            }
            Err(_) => {
                warn!(selected_shard, "RANDOMKEY timeout");
                Response::error("ERR timeout")
            }
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

        // Broadcast to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FlushDb,
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Wait for all to complete
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FLUSHDB request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FLUSHDB timeout");
                    return Response::error("ERR flushdb timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FLUSHALL command - same as FLUSHDB (single database).
    pub(crate) async fn handle_flushall(&self, args: &[Bytes]) -> Response {
        self.handle_flushdb(args).await
    }

    /// Handle INFO command - gather info from all shards.
    pub(crate) async fn handle_info(&self, args: &[Bytes]) -> Response {
        // Execute on local shard (INFO mostly returns static data or aggregate stats)
        let cmd = std::sync::Arc::new(frogdb_protocol::ParsedCommand {
            name: Bytes::from_static(b"INFO"),
            args: args.to_vec(),
        });
        let mut response = self.execute_on_shard(self.shard_id, cmd).await;

        // Gather per-shard stats and aggregate evicted/expired keys.
        let shard_stats = self.gather_memory_stats().await;
        let evicted: u64 = shard_stats.iter().map(|s| s.evicted_keys).sum();
        let expired: u64 = shard_stats.iter().map(|s| s.expired_keys).sum();

        // Patch the Clients section and stats with live data.
        if let Response::Bulk(Some(ref bytes)) = response {
            let s = String::from_utf8_lossy(bytes);
            let blocked = self.client_registry.blocked_client_count();
            let connected = self.client_registry.client_count();
            let patched = s
                .replace(
                    "blocked_clients:0\r\n",
                    &format!("blocked_clients:{blocked}\r\n"),
                )
                .replace(
                    "connected_clients:1\r\n",
                    &format!("connected_clients:{connected}\r\n"),
                )
                .replace("evicted_keys:0\r\n", &format!("evicted_keys:{evicted}\r\n"))
                .replace("expired_keys:0\r\n", &format!("expired_keys:{expired}\r\n"));
            response = Response::bulk(Bytes::from(patched));
        }
        response
    }
}

impl ConnectionHandler {
    /// Handle TS.QUERYINDEX - scatter to all shards, collect matching keys.
    pub(crate) async fn handle_ts_queryindex(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ts.queryindex' command");
        }

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::TsQueryIndex {
                    args: args.to_vec(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut all_keys: Vec<Bytes> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (key, _) in partial.results {
                        all_keys.push(key);
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped TS.QUERYINDEX request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "TS.QUERYINDEX timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        all_keys.sort();
        Response::Array(all_keys.into_iter().map(Response::bulk).collect())
    }

    /// Handle TS.MGET - scatter to all shards, collect [key, labels, sample] tuples.
    pub(crate) async fn handle_ts_mget(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ts.mget' command");
        }

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::TsMget {
                    args: args.to_vec(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut all_results: Vec<(Bytes, Response)> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    all_results.extend(partial.results);
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped TS.MGET request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "TS.MGET timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort by key for consistency
        all_results.sort_by(|a, b| a.0.cmp(&b.0));
        Response::Array(all_results.into_iter().map(|(_, r)| r).collect())
    }

    /// Handle TS.MRANGE / TS.MREVRANGE - scatter to all shards, collect results.
    pub(crate) async fn handle_ts_mrange(&self, args: &[Bytes], reverse: bool) -> Response {
        if args.len() < 4 {
            let cmd = if reverse { "ts.mrevrange" } else { "ts.mrange" };
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            ));
        }

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::TsMrange {
                    args: args.to_vec(),
                    reverse,
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut all_results: Vec<(Bytes, Response)> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    all_results.extend(partial.results);
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped TS.MRANGE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "TS.MRANGE timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort by key for consistency
        all_results.sort_by(|a, b| a.0.cmp(&b.0));
        Response::Array(all_results.into_iter().map(|(_, r)| r).collect())
    }

    // =========================================================================
    // FT.* search handlers
    // =========================================================================

    /// Handle FT.CREATE - parse schema, broadcast to all shards.
    pub(crate) async fn handle_ft_create(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.create' command");
        }

        // First arg is the index name
        let index_name = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid index name"),
        };

        // Parse the schema
        let raw_args: Vec<&[u8]> = args[1..].iter().map(|a| a.as_ref()).collect();
        let def = match frogdb_search::parse_ft_create_args(index_name, &raw_args) {
            Ok(d) => d,
            Err(e) => return Response::error(format!("ERR {}", e)),
        };

        // Serialize to JSON for broadcast
        let json = match serde_json::to_vec(&def) {
            Ok(j) => j,
            Err(e) => return Response::error(format!("ERR serialization: {}", e)),
        };

        // Broadcast to ALL shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtCreate {
                    index_def_json: Bytes::from(json.clone()),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Wait for all to complete
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    // Check for errors
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.CREATE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.CREATE timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FT.ALTER - parse new fields, broadcast to all shards.
    pub(crate) async fn handle_ft_alter(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.alter' command");
        }

        let index_name = args[0].clone();
        let raw_args: Vec<&[u8]> = args[1..].iter().map(|a| a.as_ref()).collect();
        let new_fields = match frogdb_search::parse_ft_alter_args(&raw_args) {
            Ok(f) => f,
            Err(e) => return Response::error(format!("ERR {}", e)),
        };

        let json = match serde_json::to_vec(&new_fields) {
            Ok(j) => j,
            Err(e) => return Response::error(format!("ERR serialization: {}", e)),
        };

        // Broadcast to ALL shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtAlter {
                    index_name: index_name.clone(),
                    new_fields_json: Bytes::from(json.clone()),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.ALTER request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.ALTER timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }

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
                _ => {
                    i += 1;
                }
            }
        }

        // Fan out to all shards — overfetch so we can apply global offset+limit
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
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
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
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
        } else {
            // Sort by score descending
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

    /// Handle FT.DROPINDEX - broadcast to all shards.
    pub(crate) async fn handle_ft_dropindex(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.dropindex' command");
        }

        let index_name = args[0].clone();

        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtDropIndex {
                    index_name: index_name.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.DROPINDEX request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.DROPINDEX timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FT.INFO - query shard 0 only.
    pub(crate) async fn handle_ft_info(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.info' command");
        }

        let index_name = args[0].clone();

        // Only query shard 0 (all shards have identical schemas)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtInfo { index_name },
            conn_id: self.state.id,
            response_tx,
        };
        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                if let Some((_, resp)) = partial.results.into_iter().next() {
                    resp
                } else {
                    Response::error("ERR empty response")
                }
            }
            Ok(Err(_)) => Response::error("ERR shard dropped request"),
            Err(_) => Response::error("ERR timeout"),
        }
    }

    /// Handle FT._LIST - query shard 0 only.
    pub(crate) async fn handle_ft_list(&self, _args: &[Bytes]) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtList,
            conn_id: self.state.id,
            response_tx,
        };
        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                if let Some((_, resp)) = partial.results.into_iter().next() {
                    resp
                } else {
                    Response::Array(vec![])
                }
            }
            Ok(Err(_)) => Response::error("ERR shard dropped request"),
            Err(_) => Response::error("ERR timeout"),
        }
    }
}

/// Handler for scatter-gather commands.
#[derive(Clone)]
pub struct ScatterHandler {
    /// Number of shards.
    num_shards: usize,
}

impl ScatterHandler {
    /// Create a new scatter handler.
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards }
    }

    /// Get the number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Merge SCAN results from multiple shards.
    ///
    /// The cursor encodes the shard ID and per-shard cursor, allowing
    /// iteration to resume from where it left off.
    pub fn merge_scan_results(
        &self,
        results: Vec<(usize, ScanResult)>,
        count: Option<usize>,
    ) -> Response {
        let mut keys = Vec::new();
        let mut next_cursor = 0u64;

        // Collect all keys and find the next cursor
        for (shard_id, result) in results {
            keys.extend(result.keys);

            if result.cursor != 0 {
                // Encode shard ID into cursor
                // Format: upper bits = shard_id, lower bits = shard_cursor
                next_cursor = encode_cursor(shard_id, result.cursor);
            }
        }

        // Apply count limit if specified
        if let Some(count) = count {
            keys.truncate(count);
        }

        Response::Array(vec![
            Response::bulk(Bytes::from(next_cursor.to_string())),
            Response::Array(keys.into_iter().map(Response::bulk).collect()),
        ])
    }

    /// Merge KEYS results from multiple shards.
    pub fn merge_keys_results(&self, results: Vec<Vec<Bytes>>) -> Response {
        let keys: Vec<Response> = results.into_iter().flatten().map(Response::bulk).collect();

        Response::Array(keys)
    }

    /// Merge DBSIZE results from multiple shards.
    pub fn merge_dbsize_results(&self, results: Vec<i64>) -> Response {
        let total: i64 = results.into_iter().sum();
        Response::Integer(total)
    }

    /// Select a random key from shard results.
    pub fn merge_randomkey_results(&self, results: Vec<Option<Bytes>>) -> Response {
        // Find first non-null result
        if let Some(key) = results.into_iter().flatten().next() {
            return Response::bulk(key);
        }
        Response::Null
    }

    /// Merge FLUSHDB/FLUSHALL results from multiple shards.
    ///
    /// Returns OK if all shards succeeded.
    pub fn merge_flush_results(&self, results: Vec<bool>) -> Response {
        if results.into_iter().all(|r| r) {
            Response::ok()
        } else {
            Response::error("ERR Some shards failed to flush")
        }
    }

    /// Decode a SCAN cursor to extract shard ID and per-shard cursor.
    pub fn decode_cursor(&self, cursor: u64) -> (usize, u64) {
        decode_cursor(cursor, self.num_shards)
    }

    /// Determine which shards need to be scanned based on cursor.
    pub fn shards_for_scan(&self, cursor: u64) -> Vec<usize> {
        if cursor == 0 {
            // Start from first shard
            (0..self.num_shards).collect()
        } else {
            let (start_shard, _) = self.decode_cursor(cursor);
            // Continue from the shard indicated by cursor
            (start_shard..self.num_shards).collect()
        }
    }
}

/// Result from a single shard's SCAN operation.
#[derive(Debug, Clone)]
pub struct ScanResult {
    /// Cursor for continuing scan on this shard (0 = complete).
    pub cursor: u64,
    /// Keys found in this batch.
    pub keys: Vec<Bytes>,
}

/// Encode a shard ID and shard-local cursor into a global cursor.
///
/// The encoding uses the lower 48 bits for the shard cursor and
/// the upper 16 bits for the shard ID.
fn encode_cursor(shard_id: usize, shard_cursor: u64) -> u64 {
    ((shard_id as u64) << 48) | (shard_cursor & 0x0000_FFFF_FFFF_FFFF)
}

/// Decode a global cursor into shard ID and shard-local cursor.
fn decode_cursor(cursor: u64, num_shards: usize) -> (usize, u64) {
    if cursor == 0 {
        return (0, 0);
    }

    let shard_id = ((cursor >> 48) as usize).min(num_shards - 1);
    let shard_cursor = cursor & 0x0000_FFFF_FFFF_FFFF;

    (shard_id, shard_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_encoding() {
        let shard_id = 5;
        let shard_cursor = 12345;

        let encoded = encode_cursor(shard_id, shard_cursor);
        let (decoded_shard, decoded_cursor) = decode_cursor(encoded, 16);

        assert_eq!(decoded_shard, shard_id);
        assert_eq!(decoded_cursor, shard_cursor);
    }

    #[test]
    fn test_zero_cursor() {
        let (shard, cursor) = decode_cursor(0, 16);
        assert_eq!(shard, 0);
        assert_eq!(cursor, 0);
    }

    #[test]
    fn test_shards_for_scan() {
        let handler = ScatterHandler::new(4);

        // Start scan - all shards
        let shards = handler.shards_for_scan(0);
        assert_eq!(shards, vec![0, 1, 2, 3]);

        // Continue from shard 2
        let cursor = encode_cursor(2, 100);
        let shards = handler.shards_for_scan(cursor);
        assert_eq!(shards, vec![2, 3]);
    }

    #[test]
    fn test_merge_dbsize() {
        let handler = ScatterHandler::new(4);
        let results = vec![100, 200, 150, 50];
        let response = handler.merge_dbsize_results(results);

        match response {
            Response::Integer(n) => assert_eq!(n, 500),
            _ => panic!("Expected integer response"),
        }
    }
}
