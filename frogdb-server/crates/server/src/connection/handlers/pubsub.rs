//! Pub/Sub command handlers.
//!
//! This module handles pub/sub commands:
//! - SUBSCRIBE/UNSUBSCRIBE - Channel subscriptions
//! - PSUBSCRIBE/PUNSUBSCRIBE - Pattern subscriptions
//! - SSUBSCRIBE/SUNSUBSCRIBE - Sharded subscriptions
//! - PUBLISH/SPUBLISH - Message publishing
//! - PUBSUB - Introspection commands
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{
    GlobPattern, IntrospectionRequest, IntrospectionResponse,
    MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SUBSCRIPTIONS_PER_CONNECTION, ShardMessage, shard_for_key,
};
use frogdb_protocol::Response;
use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot;
use tracing::debug;

use crate::connection::ConnectionHandler;

// ============================================================================
// Pub/Sub command handlers
// ============================================================================

impl ConnectionHandler {
    /// Handle SUBSCRIBE command.
    pub(crate) async fn handle_subscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error(
                "ERR wrong number of arguments for 'subscribe' command",
            )];
        }

        // Check subscription limits
        let new_count = self.state.pubsub.subscriptions.len() + args.len();
        if new_count > MAX_SUBSCRIPTIONS_PER_CONNECTION {
            return vec![Response::error("ERR max subscriptions reached")];
        }

        // 80% warning threshold
        let threshold_80 = MAX_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5;
        if new_count >= threshold_80 && !self.state.pubsub.warned_sub_80 {
            self.state.pubsub.warned_sub_80 = true;
            tracing::warn!(
                conn_id = self.state.id,
                current = new_count,
                limit = MAX_SUBSCRIPTIONS_PER_CONNECTION,
                "Connection approaching channel subscription limit (80%)"
            );
        }

        let mut responses = Vec::with_capacity(args.len());

        // Fan out to all shards for broadcast subscriptions
        for channel in args {
            // Add to local tracking
            self.state.pubsub.subscriptions.insert(channel.clone());

            debug!(
                conn_id = self.state.id,
                channel = %String::from_utf8_lossy(channel),
                "Subscribed to channel"
            );

            // Broadcast pub/sub uses shard 0 as the coordinator so each
            // subscriber is registered exactly once and PUBLISH delivers
            // each message exactly once.
            let pubsub_tx = self.ensure_pubsub_channel();
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[0]
                .send(ShardMessage::Subscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    sender: pubsub_tx,
                    response_tx,
                })
                .await;

            // Build subscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"subscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle UNSUBSCRIBE command.
    pub(crate) async fn handle_unsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all channels
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state.pubsub.subscriptions.iter().cloned().collect()
        } else {
            args.to_vec()
        };

        // Handle case where no channels to unsubscribe from
        if channels.is_empty() {
            return vec![Response::Array(vec![
                Response::bulk(Bytes::from_static(b"unsubscribe")),
                Response::null(),
                Response::Integer(0),
            ])];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in channels {
            // Remove from local tracking
            self.state.pubsub.subscriptions.remove(&channel);

            // Broadcast pub/sub uses shard 0 as the coordinator.
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[0]
                .send(ShardMessage::Unsubscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    response_tx,
                })
                .await;

            // Build unsubscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"unsubscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        // Reset 80% warning if below threshold
        if self.state.pubsub.subscriptions.len() < MAX_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5 {
            self.state.pubsub.warned_sub_80 = false;
        }

        responses
    }

    /// Handle PSUBSCRIBE command.
    pub(crate) async fn handle_psubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error(
                "ERR wrong number of arguments for 'psubscribe' command",
            )];
        }

        // Check subscription limits
        let new_count = self.state.pubsub.patterns.len() + args.len();
        if new_count > MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION {
            return vec![Response::error("ERR max pattern subscriptions reached")];
        }

        // 80% warning threshold
        let threshold_80 = MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5;
        if new_count >= threshold_80 && !self.state.pubsub.warned_pattern_80 {
            self.state.pubsub.warned_pattern_80 = true;
            tracing::warn!(
                conn_id = self.state.id,
                current = new_count,
                limit = MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
                "Connection approaching pattern subscription limit (80%)"
            );
        }

        let mut responses = Vec::with_capacity(args.len());

        for pattern in args {
            // Add to local tracking
            self.state.pubsub.patterns.insert(pattern.clone());

            // Broadcast pub/sub uses shard 0 as the coordinator.
            let pubsub_tx = self.ensure_pubsub_channel();
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[0]
                .send(ShardMessage::PSubscribe {
                    patterns: vec![pattern.clone()],
                    conn_id: self.state.id,
                    sender: pubsub_tx,
                    response_tx,
                })
                .await;

            // Build subscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"psubscribe")),
                Response::bulk(pattern.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle PUNSUBSCRIBE command.
    pub(crate) async fn handle_punsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all patterns
        let patterns: Vec<Bytes> = if args.is_empty() {
            self.state.pubsub.patterns.iter().cloned().collect()
        } else {
            args.to_vec()
        };

        // Handle case where no patterns to unsubscribe from
        if patterns.is_empty() {
            return vec![Response::Array(vec![
                Response::bulk(Bytes::from_static(b"punsubscribe")),
                Response::null(),
                Response::Integer(0),
            ])];
        }

        let mut responses = Vec::with_capacity(patterns.len());

        for pattern in patterns {
            // Remove from local tracking
            self.state.pubsub.patterns.remove(&pattern);

            // Broadcast pub/sub uses shard 0 as the coordinator.
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[0]
                .send(ShardMessage::PUnsubscribe {
                    patterns: vec![pattern.clone()],
                    conn_id: self.state.id,
                    response_tx,
                })
                .await;

            // Build unsubscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"punsubscribe")),
                Response::bulk(pattern.clone()),
                Response::Integer(count as i64),
            ]));
        }

        // Reset 80% warning if below threshold
        if self.state.pubsub.patterns.len() < MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5 {
            self.state.pubsub.warned_pattern_80 = false;
        }

        responses
    }

    /// Handle PUBLISH command.
    ///
    /// In cluster mode, broadcasts to all other nodes via the forwarder and
    /// sums the subscriber counts.
    pub(crate) async fn handle_publish(&self, args: &[Bytes]) -> Response {
        if args.len() != 2 {
            return Response::error("ERR wrong number of arguments for 'publish' command");
        }

        let channel = &args[0];
        let message = &args[1];

        // Broadcast pub/sub uses shard 0 as the coordinator so the count
        // reflects actual unique subscribers rather than being multiplied by
        // the number of shards.
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self.core.shard_senders[0]
            .send(ShardMessage::Publish {
                channel: channel.clone(),
                message: message.clone(),
                response_tx,
            })
            .await;

        let local_count = response_rx.await.unwrap_or(0);

        // In cluster mode, also broadcast to all other nodes
        let remote_count = match &self.cluster.pubsub_forwarder {
            Some(forwarder) => forwarder.broadcast_publish(channel, message).await,
            None => 0,
        };

        Response::Integer((local_count + remote_count) as i64)
    }

    /// Handle SSUBSCRIBE command (sharded subscriptions).
    ///
    /// In cluster mode, returns a MOVED redirect if the channel's slot belongs
    /// to another node.
    pub(crate) async fn handle_ssubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error(
                "ERR wrong number of arguments for 'ssubscribe' command",
            )];
        }

        // Check subscription limits
        let new_count = self.state.pubsub.sharded_subscriptions.len() + args.len();
        if new_count > MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION {
            return vec![Response::error("ERR max sharded subscriptions reached")];
        }

        // 80% warning threshold
        let threshold_80 = MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5;
        if new_count >= threshold_80 && !self.state.pubsub.warned_sharded_80 {
            self.state.pubsub.warned_sharded_80 = true;
            tracing::warn!(
                conn_id = self.state.id,
                current = new_count,
                limit = MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
                "Connection approaching sharded subscription limit (80%)"
            );
        }

        let mut responses = Vec::with_capacity(args.len());

        for channel in args {
            // In cluster mode, check if the slot belongs to another node
            if let Some(forwarder) = &self.cluster.pubsub_forwarder
                && let Some((slot, addr)) = forwarder.get_slot_owner_addr(channel)
            {
                responses.push(Response::error(format!("MOVED {} {}", slot, addr)));
                continue;
            }

            // Add to local tracking
            self.state
                .pubsub
                .sharded_subscriptions
                .insert(channel.clone());

            // Route to the owning shard only
            let pubsub_tx = self.ensure_pubsub_channel();
            let shard_id = shard_for_key(channel, self.num_shards);
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[shard_id]
                .send(ShardMessage::ShardedSubscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    sender: pubsub_tx,
                    response_tx,
                })
                .await;

            // Build subscription confirmation response
            let count = self.state.pubsub.sharded_subscriptions.len();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"ssubscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle SUNSUBSCRIBE command (sharded subscriptions).
    pub(crate) async fn handle_sunsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all sharded channels
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state
                .pubsub
                .sharded_subscriptions
                .iter()
                .cloned()
                .collect()
        } else {
            args.to_vec()
        };

        // Handle case where no channels to unsubscribe from
        if channels.is_empty() {
            return vec![Response::Array(vec![
                Response::bulk(Bytes::from_static(b"sunsubscribe")),
                Response::null(),
                Response::Integer(0),
            ])];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in channels {
            // Remove from local tracking
            self.state.pubsub.sharded_subscriptions.remove(&channel);

            // Route to the owning shard only
            let shard_id = shard_for_key(&channel, self.num_shards);
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[shard_id]
                .send(ShardMessage::ShardedUnsubscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    response_tx,
                })
                .await;

            // Build unsubscription confirmation response
            let count = self.state.pubsub.sharded_subscriptions.len();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"sunsubscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        // Reset 80% warning if below threshold
        if self.state.pubsub.sharded_subscriptions.len()
            < MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5
        {
            self.state.pubsub.warned_sharded_80 = false;
        }

        responses
    }

    /// Handle SPUBLISH command (sharded publish).
    ///
    /// In cluster mode, forwards to the slot-owning node if the channel's slot
    /// is not local.
    pub(crate) async fn handle_spublish(&self, args: &[Bytes]) -> Response {
        if args.len() != 2 {
            return Response::error("ERR wrong number of arguments for 'spublish' command");
        }

        let channel = &args[0];
        let message = &args[1];

        // In cluster mode, forward to the slot owner if not local
        if let Some(forwarder) = &self.cluster.pubsub_forwarder
            && let Some(count) = forwarder.forward_spublish(channel, message).await
        {
            return Response::Integer(count as i64);
        }

        // Route to the owning shard locally
        let shard_id = shard_for_key(channel, self.num_shards);
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self.core.shard_senders[shard_id]
            .send(ShardMessage::ShardedPublish {
                channel: channel.clone(),
                message: message.clone(),
                response_tx,
            })
            .await;

        match response_rx.await {
            Ok(count) => Response::Integer(count as i64),
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle PUBSUB subcommands.
    pub(crate) async fn handle_pubsub_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'pubsub' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "CHANNELS" => self.handle_pubsub_channels(&args[1..]).await,
            "NUMSUB" => self.handle_pubsub_numsub(&args[1..]).await,
            "NUMPAT" => self.handle_pubsub_numpat().await,
            "SHARDCHANNELS" => self.handle_pubsub_shardchannels(&args[1..]).await,
            "SHARDNUMSUB" => self.handle_pubsub_shardnumsub(&args[1..]).await,
            "HELP" => pubsub_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle PUBSUB CHANNELS [pattern].
    async fn handle_pubsub_channels(&self, args: &[Bytes]) -> Response {
        let pattern = if args.is_empty() {
            None
        } else {
            Some(GlobPattern::new(args[0].clone()))
        };

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::Channels {
                        pattern: pattern.clone(),
                    },
                    response_tx,
                })
                .await;
            handles.push(response_rx);
        }

        // Gather and deduplicate
        let mut all_channels = HashSet::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::Channels(channels)) = rx.await {
                all_channels.extend(channels);
            }
        }

        let mut channels: Vec<_> = all_channels.into_iter().collect();
        channels.sort();
        Response::Array(channels.into_iter().map(Response::bulk).collect())
    }

    /// Handle PUBSUB NUMSUB [channel ...].
    async fn handle_pubsub_numsub(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::Array(vec![]);
        }

        let channels: Vec<Bytes> = args.to_vec();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::NumSub {
                        channels: channels.clone(),
                    },
                    response_tx,
                })
                .await;
            handles.push(response_rx);
        }

        // Gather and sum counts per channel
        let mut channel_counts: HashMap<Bytes, usize> = HashMap::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::NumSub(counts)) = rx.await {
                for (channel, count) in counts {
                    *channel_counts.entry(channel).or_insert(0) += count;
                }
            }
        }

        // Build response: [channel1, count1, channel2, count2, ...]
        let mut result = Vec::with_capacity(channels.len() * 2);
        for channel in channels {
            let count = channel_counts.get(&channel).copied().unwrap_or(0);
            result.push(Response::bulk(channel));
            result.push(Response::Integer(count as i64));
        }

        Response::Array(result)
    }

    /// Handle PUBSUB NUMPAT.
    async fn handle_pubsub_numpat(&self) -> Response {
        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::NumPat,
                    response_tx,
                })
                .await;
            handles.push(response_rx);
        }

        // Sum all pattern counts
        let mut total = 0usize;
        for rx in handles {
            if let Ok(IntrospectionResponse::NumPat(count)) = rx.await {
                total += count;
            }
        }

        Response::Integer(total as i64)
    }

    /// Handle PUBSUB SHARDCHANNELS [pattern].
    async fn handle_pubsub_shardchannels(&self, args: &[Bytes]) -> Response {
        let pattern = if args.is_empty() {
            None
        } else {
            Some(GlobPattern::new(args[0].clone()))
        };

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::ShardChannels {
                        pattern: pattern.clone(),
                    },
                    response_tx,
                })
                .await;
            handles.push(response_rx);
        }

        // Gather and deduplicate
        let mut all_channels = HashSet::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::Channels(channels)) = rx.await {
                all_channels.extend(channels);
            }
        }

        let mut channels: Vec<_> = all_channels.into_iter().collect();
        channels.sort();
        Response::Array(channels.into_iter().map(Response::bulk).collect())
    }

    /// Handle PUBSUB SHARDNUMSUB [channel ...].
    async fn handle_pubsub_shardnumsub(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::Array(vec![]);
        }

        let channels: Vec<Bytes> = args.to_vec();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::ShardNumSub {
                        channels: channels.clone(),
                    },
                    response_tx,
                })
                .await;
            handles.push(response_rx);
        }

        // Gather and sum counts per channel
        let mut channel_counts: HashMap<Bytes, usize> = HashMap::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::NumSub(counts)) = rx.await {
                for (channel, count) in counts {
                    *channel_counts.entry(channel).or_insert(0) += count;
                }
            }
        }

        // Build response: [channel1, count1, channel2, count2, ...]
        let mut result = Vec::with_capacity(channels.len() * 2);
        for channel in channels {
            let count = channel_counts.get(&channel).copied().unwrap_or(0);
            result.push(Response::bulk(channel));
            result.push(Response::Integer(count as i64));
        }

        Response::Array(result)
    }
}

// ============================================================================
// Response helper functions (kept from original module)
// ============================================================================

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
        "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CHANNELS [<pattern>]",
        "    Return channels that have at least one subscriber matching the pattern.",
        "NUMSUB [<channel> [<channel> ...]]",
        "    Return the number of subscribers for the specified channels.",
        "NUMPAT",
        "    Return the number of unique pattern subscriptions.",
        "SHARDCHANNELS [<pattern>]",
        "    Return shard channels that have at least one subscriber matching the pattern.",
        "SHARDNUMSUB [<channel> [<channel> ...]]",
        "    Return the number of subscribers for the specified shard channels.",
        "HELP",
        "    Return subcommand help summary.",
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
