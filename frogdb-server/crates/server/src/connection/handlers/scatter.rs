//! Scatter-gather command handlers.
//!
//! This module handles commands that need to be executed across
//! multiple shards and have their results merged:
//! - SCAN - Scan keys with cursor
//! - KEYS - Find keys matching pattern
//! - DBSIZE - Count total keys
//! - RANDOMKEY - Get a random key
//! - FLUSHDB/FLUSHALL - Flush databases

use std::time::Duration;

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
            let mut patched = s
                .replace(
                    "blocked_clients:0\r\n",
                    &format!("blocked_clients:{blocked}\r\n"),
                )
                .replace(
                    "connected_clients:1\r\n",
                    &format!("connected_clients:{connected}\r\n"),
                )
                .replace("evicted_keys:0\r\n", &format!("evicted_keys:{evicted}\r\n"))
                .replace("expired_keys:0\r\n", &format!("expired_keys:{expired}\r\n"))
                .replace(
                    "maxclients:10000\r\n",
                    &format!("maxclients:{}\r\n", self.config_manager.max_clients()),
                );

            // Append rate limit stats section
            let rl_registry = self.acl_manager.rate_limit_registry();
            let rl_users = rl_registry.user_count();
            let rl_cmds_rejected = rl_registry.total_commands_rejected();
            let rl_bytes_rejected = rl_registry.total_bytes_rejected();
            if rl_users > 0 || rl_cmds_rejected > 0 || rl_bytes_rejected > 0 {
                patched.push_str("\r\n# Ratelimit\r\n");
                patched.push_str(&format!("ratelimit_users_configured:{rl_users}\r\n"));
                patched.push_str(&format!(
                    "ratelimit_total_commands_rejected:{rl_cmds_rejected}\r\n"
                ));
                patched.push_str(&format!(
                    "ratelimit_total_bytes_rejected:{rl_bytes_rejected}\r\n"
                ));
            }

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
        for sender in self.shard_senders.iter() {
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
            let cursor_id = if let Some(ref store) = self.cursor_store {
                let idx_name = std::str::from_utf8(&index_name).unwrap_or("").to_string();
                store.create_cursor(
                    remaining.to_vec(),
                    cursor_count,
                    idx_name,
                    Duration::from_millis(cursor_maxidle_ms),
                )
            } else {
                0
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
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
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

    /// Handle FT.CURSOR READ/DEL - coordinator-only cursor management.
    pub(crate) async fn handle_ft_cursor(&self, args: &[Bytes]) -> Response {
        if args.len() < 3 {
            return Response::error("ERR wrong number of arguments for 'ft.cursor' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let index_name = std::str::from_utf8(&args[1]).unwrap_or("");
        let cursor_id_str = std::str::from_utf8(&args[2]).unwrap_or("");
        let cursor_id: u64 = match cursor_id_str.parse() {
            Ok(id) => id,
            Err(_) => return Response::error("ERR invalid cursor id"),
        };

        let store = match self.cursor_store {
            Some(ref s) => s,
            None => return Response::error("ERR cursor store not available"),
        };

        match subcommand.as_slice() {
            b"READ" => {
                // Parse optional COUNT
                let count_override = if args.len() >= 5 && args[3].eq_ignore_ascii_case(b"COUNT") {
                    std::str::from_utf8(&args[4])
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                } else {
                    None
                };

                match store.read_cursor(cursor_id, count_override, index_name) {
                    Some((rows, new_cursor_id)) => {
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

                        Response::Array(vec![
                            Response::Array(result_items),
                            Response::Integer(new_cursor_id as i64),
                        ])
                    }
                    None => Response::error("ERR Cursor not found"),
                }
            }
            b"DEL" => {
                if store.delete_cursor(cursor_id) {
                    Response::ok()
                } else {
                    Response::error("ERR Cursor not found")
                }
            }
            _ => Response::error(format!(
                "ERR unknown FT.CURSOR subcommand '{}'",
                String::from_utf8_lossy(&subcommand)
            )),
        }
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

    /// Handle FT.SYNUPDATE - parse args, broadcast to all shards.
    pub(crate) async fn handle_ft_synupdate(&self, args: &[Bytes]) -> Response {
        if args.len() < 3 {
            return Response::error("ERR wrong number of arguments for 'ft.synupdate' command");
        }

        let index_name = args[0].clone();
        let group_id = args[1].clone();

        // Skip optional SKIPINITIALSCAN, collect terms
        let mut term_start = 2;
        if term_start < args.len()
            && args[term_start].to_ascii_uppercase().as_slice() == b"SKIPINITIALSCAN"
        {
            term_start += 1;
        }

        if term_start >= args.len() {
            return Response::error("ERR at least one term is required");
        }

        let terms: Vec<Bytes> = args[term_start..].to_vec();

        // Broadcast to ALL shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtSynupdate {
                    index_name: index_name.clone(),
                    group_id: group_id.clone(),
                    terms: terms.clone(),
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
                    warn!(shard_id, "Shard dropped FT.SYNUPDATE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.SYNUPDATE timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FT.ALIASADD - broadcast to all shards.
    pub(crate) async fn handle_ft_aliasadd(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.aliasadd' command");
        }
        let alias_name = args[0].clone();
        let index_name = args[1].clone();

        self.broadcast_and_check_shard0(ScatterOp::FtAliasadd {
            alias_name,
            index_name,
        })
        .await
    }

    /// Handle FT.ALIASDEL - broadcast to all shards.
    pub(crate) async fn handle_ft_aliasdel(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.aliasdel' command");
        }
        let alias_name = args[0].clone();

        self.broadcast_and_check_shard0(ScatterOp::FtAliasdel { alias_name })
            .await
    }

    /// Handle FT.ALIASUPDATE - broadcast to all shards.
    pub(crate) async fn handle_ft_aliasupdate(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.aliasupdate' command");
        }
        let alias_name = args[0].clone();
        let index_name = args[1].clone();

        self.broadcast_and_check_shard0(ScatterOp::FtAliasupdate {
            alias_name,
            index_name,
        })
        .await
    }

    /// Handle FT.TAGVALS - scatter to all shards, union results.
    pub(crate) async fn handle_ft_tagvals(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.tagvals' command");
        }
        let index_name = args[0].clone();
        let field_name = args[1].clone();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FtTagvals {
                    index_name: index_name.clone(),
                    field_name: field_name.clone(),
                },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Collect and union tag values from all shards
        let mut all_values = std::collections::HashSet::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in partial.results {
                        match resp {
                            Response::Error(_) => return resp,
                            Response::Array(items) => {
                                for item in items {
                                    if let Response::Bulk(Some(b)) = item
                                        && let Ok(s) = std::str::from_utf8(&b)
                                    {
                                        all_values.insert(s.to_string());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FT.TAGVALS request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FT.TAGVALS timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        let mut sorted: Vec<String> = all_values.into_iter().collect();
        sorted.sort();
        Response::Array(
            sorted
                .into_iter()
                .map(|v| Response::bulk(Bytes::from(v)))
                .collect(),
        )
    }

    /// Handle FT.DICTADD - broadcast to all shards.
    pub(crate) async fn handle_ft_dictadd(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.dictadd' command");
        }
        let dict_name = args[0].clone();
        let terms: Vec<Bytes> = args[1..].to_vec();

        self.broadcast_and_return_shard0_response(ScatterOp::FtDictadd { dict_name, terms })
            .await
    }

    /// Handle FT.DICTDEL - broadcast to all shards.
    pub(crate) async fn handle_ft_dictdel(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.dictdel' command");
        }
        let dict_name = args[0].clone();
        let terms: Vec<Bytes> = args[1..].to_vec();

        self.broadcast_and_return_shard0_response(ScatterOp::FtDictdel { dict_name, terms })
            .await
    }

    /// Handle FT.DICTDUMP - query shard 0 only.
    pub(crate) async fn handle_ft_dictdump(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.dictdump' command");
        }
        let dict_name = args[0].clone();

        self.query_shard0(ScatterOp::FtDictdump { dict_name }).await
    }

    /// Handle FT.CONFIG GET/SET.
    pub(crate) async fn handle_ft_config(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.config' command");
        }

        let subcommand = std::str::from_utf8(&args[0])
            .unwrap_or("")
            .to_ascii_uppercase();

        match subcommand.as_str() {
            "GET" => {
                // Query shard 0
                self.query_shard0(ScatterOp::FtConfig {
                    args: args.to_vec(),
                })
                .await
            }
            "SET" => {
                // Broadcast SET to all shards
                self.broadcast_and_check_shard0(ScatterOp::FtConfig {
                    args: args.to_vec(),
                })
                .await
            }
            "HELP" => {
                // Return list of config options
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"MINPREFIX")),
                    Response::bulk(Bytes::from_static(b"MAXEXPANSIONS")),
                    Response::bulk(Bytes::from_static(b"TIMEOUT")),
                    Response::bulk(Bytes::from_static(b"DEFAULT_DIALECT")),
                ])
            }
            _ => Response::error("ERR Unknown subcommand for 'ft.config' command"),
        }
    }

    /// Handle FT.SPELLCHECK - scatter to all shards, merge suggestions.
    pub(crate) async fn handle_ft_spellcheck(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.spellcheck' command");
        }
        let index_name = args[0].clone();
        let query_args: Vec<Bytes> = args[1..].to_vec();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
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

    /// Handle FT.EXPLAIN / FT.EXPLAINCLI - query shard 0 only.
    pub(crate) async fn handle_ft_explain(&self, args: &[Bytes], cli_mode: bool) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'ft.explain' command");
        }

        let index_name = args[0].clone();
        let query_str = args[1].clone();

        // Only query shard 0 (schema is identical across shards)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtExplain {
                index_name,
                query_str,
            },
            conn_id: self.state.id,
            response_tx,
        };
        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                if let Some((_, resp)) = partial.results.into_iter().next() {
                    if cli_mode {
                        // FT.EXPLAINCLI returns array of strings (one per line)
                        if let Response::Bulk(Some(ref b)) = resp {
                            let text = String::from_utf8_lossy(b);
                            let lines: Vec<Response> = text
                                .lines()
                                .filter(|l| !l.is_empty())
                                .map(|l| Response::bulk(Bytes::from(l.to_string())))
                                .collect();
                            return Response::Array(lines);
                        }
                    }
                    resp
                } else {
                    Response::error("ERR empty response")
                }
            }
            Ok(Err(_)) => Response::error("ERR shard dropped request"),
            Err(_) => Response::error("ERR timeout"),
        }
    }

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

    /// Handle ES.ALL — scatter to all shards, merge results by StreamId ordering.
    pub(crate) async fn handle_es_all(&self, args: &[Bytes]) -> Response {
        use frogdb_core::StreamId;

        // Parse optional COUNT and AFTER arguments
        let mut count: Option<usize> = None;
        let mut after_id: Option<StreamId> = None;
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => count = Some(n),
                        None => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    }
                    i += 1;
                }
                b"AFTER" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    match StreamId::parse(&args[i]) {
                        Ok(id) => after_id = Some(id),
                        Err(_) => return Response::error("ERR Invalid stream ID specified"),
                    }
                    i += 1;
                }
                _ => {
                    return Response::error("ERR syntax error");
                }
            }
        }

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::EsAll { count, after_id },
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Gather results from all shards
        let mut all_entries: Vec<(StreamId, Response)> = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in partial.results {
                        // Each entry is [stream_id_string, [fields...]]
                        // Parse stream_id for sorting
                        if let Response::Array(ref parts) = resp
                            && let Some(Response::Bulk(Some(id_bytes))) = parts.first()
                            && let Ok(id) = StreamId::parse(id_bytes)
                        {
                            all_entries.push((id, resp));
                            continue;
                        }
                        // Fallback: use max ID to sort to end
                        all_entries.push((StreamId::max(), resp));
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped ES.ALL request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "ES.ALL timeout");
                    return Response::error("ERR timeout");
                }
            }
        }

        // Sort by StreamId for approximate global ordering
        all_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Apply COUNT limit after merge
        if let Some(limit) = count {
            all_entries.truncate(limit);
        }

        Response::Array(all_entries.into_iter().map(|(_, r)| r).collect())
    }

    // =========================================================================
    // Helper methods for common scatter-gather patterns
    // =========================================================================

    /// Broadcast an operation to all shards and return shard 0's response.
    /// Checks for errors from any shard.
    async fn broadcast_and_check_shard0(&self, operation: ScatterOp) -> Response {
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: operation.clone(),
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut shard0_response = Response::ok();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, resp) in &partial.results {
                        if let Response::Error(_) = resp {
                            return resp.clone();
                        }
                    }
                    if shard_id == 0
                        && let Some((_, resp)) = partial.results.into_iter().next()
                    {
                        shard0_response = resp;
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "Request timeout");
                    return Response::error("ERR timeout");
                }
            }
        }
        shard0_response
    }

    /// Broadcast an operation to all shards and return shard 0's direct response.
    async fn broadcast_and_return_shard0_response(&self, operation: ScatterOp) -> Response {
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: operation.clone(),
                conn_id: self.state.id,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        let mut shard0_response = Response::ok();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    if shard_id == 0
                        && let Some((_, resp)) = partial.results.into_iter().next()
                    {
                        shard0_response = resp;
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "Request timeout");
                    return Response::error("ERR timeout");
                }
            }
        }
        shard0_response
    }

    /// Send an operation to shard 0 only and return its response.
    async fn query_shard0(&self, operation: ScatterOp) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation,
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

    /// Handle FT.SYNDUMP - query shard 0 only.
    pub(crate) async fn handle_ft_syndump(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'ft.syndump' command");
        }

        let index_name = args[0].clone();
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::FtSyndump { index_name },
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
