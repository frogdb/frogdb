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
use frogdb_protocol::Response;

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
        let keys: Vec<Response> = results
            .into_iter()
            .flatten()
            .map(Response::bulk)
            .collect();

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
        for result in results {
            if let Some(key) = result {
                return Response::bulk(key);
            }
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
