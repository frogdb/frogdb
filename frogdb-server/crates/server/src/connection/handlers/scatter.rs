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

            if self.core.shard_senders[next_shard].send(msg).await.is_err() {
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
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
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
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
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
        use rand::RngExt;

        // Phase 1: Get key counts from all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
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

        // Phase 3: Request random key from selected shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::RandomKey,
            conn_id: self.state.id,
            response_tx,
        };

        if self.core.shard_senders[selected_shard]
            .send(msg)
            .await
            .is_err()
        {
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
        for (shard_id, sender) in self.core.shard_senders.iter().enumerate() {
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
            let blocked = self.admin.client_registry.blocked_client_count();
            let connected = self.admin.client_registry.client_count();
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
                    &format!("maxclients:{}\r\n", self.admin.config_manager.max_clients()),
                );

            // Append rate limit stats section
            let rl_registry = self.core.acl_manager.rate_limit_registry();
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
