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

/// Reserved connection ID for internally replicated commands.
///
/// Commands received via replication should use this connection ID to prevent
/// them from being re-broadcast back to replicas, which would cause infinite loops.
/// Connection IDs for real clients start at 1 (from NEXT_CONN_ID in server.rs).
pub const REPLICA_INTERNAL_CONN_ID: u64 = 0;
