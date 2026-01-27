//! SCAN and KEYS commands.
//!
//! These commands iterate over the keyspace:
//! - SCAN: Cursor-based iteration with pattern matching and type filtering
//! - KEYS: Return all keys matching a pattern (scatter-gather)

use bytes::Bytes;
use frogdb_core::{
    ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy,
    KeyType, MergeStrategy,
};
use frogdb_protocol::Response;

/// Cursor encoding for cross-shard SCAN.
///
/// Format: shard_id (16 bits) | position (48 bits)
///
/// This allows iterating through all shards sequentially:
/// 1. Decode cursor to get shard_id and position
/// 2. Scan the target shard starting at position
/// 3. If shard exhausted, move to next shard
/// 4. Return cursor 0 when all shards exhausted
pub mod cursor {
    const POSITION_BITS: u64 = 48;
    const POSITION_MASK: u64 = (1u64 << POSITION_BITS) - 1;

    /// Encode a cursor from shard_id and position.
    pub fn encode(shard_id: u16, position: u64) -> u64 {
        ((shard_id as u64) << POSITION_BITS) | (position & POSITION_MASK)
    }

    /// Decode a cursor into (shard_id, position).
    pub fn decode(cursor: u64) -> (u16, u64) {
        let shard_id = (cursor >> POSITION_BITS) as u16;
        let position = cursor & POSITION_MASK;
        (shard_id, position)
    }
}

// ============================================================================
// SCAN - Cursor-based key iteration
// ============================================================================

pub struct ScanCommand;

impl Command for ScanCommand {
    fn name(&self) -> &'static str {
        "SCAN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SCAN cursor [MATCH pattern] [COUNT hint] [TYPE type]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::CursoredScan,
        }
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Parse cursor
        let mut parser = ArgParser::new(args);
        let cursor: u64 = parser.next_parsed().map_err(|_| CommandError::InvalidArgument {
            message: "invalid cursor".to_string(),
        })?;

        // Parse optional arguments [MATCH pattern] [COUNT count] [TYPE type]
        let mut pattern: Option<&[u8]> = None;
        let mut count: usize = 10; // Default count
        let mut key_type: Option<KeyType> = None;

        while parser.has_more() {
            if let Some(value) = parser.try_flag_value(b"MATCH")? {
                pattern = Some(value.as_ref());
            } else if let Some(value) = parser.try_flag_usize(b"COUNT")? {
                count = value;
            } else if let Some(value) = parser.try_flag_value(b"TYPE")? {
                let type_str = value.to_ascii_lowercase();
                key_type = Some(parse_key_type(&type_str)?);
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Execute scan on local shard
        // Note: In connection.rs, this is routed based on cursor to the correct shard
        let (next_cursor, keys) = ctx.store.scan_filtered(cursor, count, pattern, key_type);

        // Build response: [cursor, [keys...]]
        let key_responses: Vec<Response> = keys.into_iter().map(Response::bulk).collect();

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(next_cursor.to_string())),
            Response::Array(key_responses),
        ]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless - cursor-based routing
    }
}

// ============================================================================
// KEYS - Get all keys matching pattern
// ============================================================================

pub struct KeysCommand;

impl Command for KeysCommand {
    fn name(&self) -> &'static str {
        "KEYS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // KEYS pattern
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::CollectKeys,
        }
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let pattern = &args[0];

        // Get all keys from local store and filter by pattern
        let all_keys = ctx.store.all_keys();
        let matching_keys: Vec<Response> = all_keys
            .into_iter()
            .filter(|key| frogdb_core::glob_match(pattern, key))
            .map(Response::bulk)
            .collect();

        // Note: In scatter-gather mode, connection.rs will merge results from all shards
        Ok(Response::Array(matching_keys))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless - scatter-gather across all shards
    }
}

/// Parse a key type string into KeyType.
fn parse_key_type(type_str: &[u8]) -> Result<KeyType, CommandError> {
    match type_str {
        b"string" => Ok(KeyType::String),
        b"list" => Ok(KeyType::List),
        b"set" => Ok(KeyType::Set),
        b"zset" => Ok(KeyType::SortedSet),
        b"hash" => Ok(KeyType::Hash),
        b"stream" => Ok(KeyType::Stream),
        _ => Err(CommandError::InvalidArgument {
            message: format!("unknown type: {}", String::from_utf8_lossy(type_str)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::cursor;

    #[test]
    fn test_cursor_encoding() {
        // Cursor 0 = shard 0, position 0
        let (shard, pos) = cursor::decode(0);
        assert_eq!(shard, 0);
        assert_eq!(pos, 0);

        // Encode and decode round-trip
        let encoded = cursor::encode(5, 1000);
        let (shard, pos) = cursor::decode(encoded);
        assert_eq!(shard, 5);
        assert_eq!(pos, 1000);

        // Large position
        let encoded = cursor::encode(1, 0x0000_FFFF_FFFF_FFFF);
        let (shard, pos) = cursor::decode(encoded);
        assert_eq!(shard, 1);
        assert_eq!(pos, 0x0000_FFFF_FFFF_FFFF);
    }

    #[test]
    fn test_cursor_shard_transition() {
        // When position is 0 and shard > 0, we've moved to a new shard
        let encoded = cursor::encode(2, 0);
        let (shard, pos) = cursor::decode(encoded);
        assert_eq!(shard, 2);
        assert_eq!(pos, 0);
    }
}
