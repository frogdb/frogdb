//! Pub/Sub command handlers.
//!
//! This module provides utilities for pub/sub commands. Note that pub/sub
//! commands are tightly coupled to connection state (subscriptions are stored
//! per-connection) and shard communication (subscriptions are registered with
//! shards). As such, the actual command execution remains in the connection
//! handler, but this module provides:
//!
//! - Response formatting helpers
//! - Help text generation
//! - Shared types for subscription state

use bytes::Bytes;
use frogdb_protocol::Response;

/// Build a SUBSCRIBE response.
pub fn subscribe_response(channel: &Bytes, subscription_count: usize) -> Response {
    Response::Array(vec![
        Response::bulk("subscribe"),
        Response::bulk(channel.clone()),
        Response::Integer(subscription_count as i64),
    ])
}

/// Build an UNSUBSCRIBE response.
pub fn unsubscribe_response(channel: Option<&Bytes>, subscription_count: usize) -> Response {
    Response::Array(vec![
        Response::bulk("unsubscribe"),
        match channel {
            Some(ch) => Response::bulk(ch.clone()),
            None => Response::Null,
        },
        Response::Integer(subscription_count as i64),
    ])
}

/// Build a PSUBSCRIBE response.
pub fn psubscribe_response(pattern: &Bytes, subscription_count: usize) -> Response {
    Response::Array(vec![
        Response::bulk("psubscribe"),
        Response::bulk(pattern.clone()),
        Response::Integer(subscription_count as i64),
    ])
}

/// Build a PUNSUBSCRIBE response.
pub fn punsubscribe_response(pattern: Option<&Bytes>, subscription_count: usize) -> Response {
    Response::Array(vec![
        Response::bulk("punsubscribe"),
        match pattern {
            Some(p) => Response::bulk(p.clone()),
            None => Response::Null,
        },
        Response::Integer(subscription_count as i64),
    ])
}

/// Build an SSUBSCRIBE response (sharded subscribe).
pub fn ssubscribe_response(channel: &Bytes, subscription_count: usize) -> Response {
    Response::Array(vec![
        Response::bulk("ssubscribe"),
        Response::bulk(channel.clone()),
        Response::Integer(subscription_count as i64),
    ])
}

/// Build an SUNSUBSCRIBE response (sharded unsubscribe).
pub fn sunsubscribe_response(channel: Option<&Bytes>, subscription_count: usize) -> Response {
    Response::Array(vec![
        Response::bulk("sunsubscribe"),
        match channel {
            Some(ch) => Response::bulk(ch.clone()),
            None => Response::Null,
        },
        Response::Integer(subscription_count as i64),
    ])
}

/// Generate PUBSUB command help text.
pub fn pubsub_help() -> Response {
    let help = vec![
        "PUBSUB CHANNELS [<pattern>]",
        "    Return the active channels matching the pattern.",
        "PUBSUB NUMSUB [<channel> ...]",
        "    Return the number of subscribers for the given channels.",
        "PUBSUB NUMPAT",
        "    Return the number of pattern subscriptions.",
        "PUBSUB SHARDCHANNELS [<pattern>]",
        "    Return the active shard channels matching the pattern.",
        "PUBSUB SHARDNUMSUB [<channel> ...]",
        "    Return the number of shard subscribers for the given channels.",
        "PUBSUB HELP",
        "    Show this help.",
    ];
    Response::Array(help.into_iter().map(Response::bulk).collect())
}

/// Build PUBSUB CHANNELS response.
pub fn pubsub_channels_response(channels: Vec<Bytes>) -> Response {
    Response::Array(channels.into_iter().map(Response::bulk).collect())
}

/// Build PUBSUB NUMSUB response.
pub fn pubsub_numsub_response(counts: Vec<(Bytes, usize)>) -> Response {
    Response::Array(
        counts
            .into_iter()
            .flat_map(|(ch, count)| vec![Response::bulk(ch), Response::Integer(count as i64)])
            .collect(),
    )
}

/// Build PUBSUB NUMPAT response.
pub fn pubsub_numpat_response(count: usize) -> Response {
    Response::Integer(count as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_response() {
        let resp = subscribe_response(&Bytes::from("test-channel"), 1);
        match resp {
            Response::Array(items) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_unsubscribe_no_channel() {
        let resp = unsubscribe_response(None, 0);
        match resp {
            Response::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[1], Response::Null));
            }
            _ => panic!("Expected array response"),
        }
    }
}
