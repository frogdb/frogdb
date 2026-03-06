use bytes::Bytes;
use frogdb_protocol::Response;

/// Format XREAD response for a single stream.
pub(crate) fn format_xread_response(
    key: &Bytes,
    entries: &[crate::types::StreamEntry],
) -> Response {
    let entry_responses: Vec<Response> = entries
        .iter()
        .map(|entry| {
            let id = Response::bulk(Bytes::from(entry.id.to_string()));
            let fields: Vec<Response> = entry
                .fields
                .iter()
                .flat_map(|(k, v)| vec![Response::bulk(k.clone()), Response::bulk(v.clone())])
                .collect();
            Response::Array(vec![id, Response::Array(fields)])
        })
        .collect();

    Response::Array(vec![Response::Array(vec![
        Response::bulk(key.clone()),
        Response::Array(entry_responses),
    ])])
}

/// Extract hash tag from a key (Redis-compatible).
///
/// Rules:
/// - First `{` that has a matching `}` with at least one character between
/// - Nested braces: outer wins (first valid match)
/// - Empty braces `{}` are ignored (hash entire key)
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close_offset = key[open + 1..].iter().position(|&b| b == b'}')?;
    let tag = &key[open + 1..open + 1 + close_offset];
    if tag.is_empty() { None } else { Some(tag) }
}

/// Number of Redis cluster hash slots.
pub const REDIS_CLUSTER_SLOTS: usize = 16384;

/// Reserved connection ID for internally replicated commands.
///
/// Commands received via replication should use this connection ID to prevent
/// them from being re-broadcast back to replicas, which would cause infinite loops.
/// Connection IDs for real clients start at 1 (from NEXT_CONN_ID in server.rs).
pub const REPLICA_INTERNAL_CONN_ID: u64 = 0;

/// Determine which shard owns a key using Redis-compatible CRC16 hashing.
///
/// Uses the XMODEM variant of CRC16, same as Redis cluster.
/// The slot is calculated as: CRC16(key) % 16384 % num_shards
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % REDIS_CLUSTER_SLOTS;
    slot % num_shards
}

/// Calculate the Redis cluster slot for a key (0-16383).
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % REDIS_CLUSTER_SLOTS as u16
}
