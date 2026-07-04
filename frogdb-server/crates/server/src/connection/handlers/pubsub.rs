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

use std::collections::BTreeMap;

use bytes::Bytes;
use frogdb_core::{
    ConnId, GlobPattern, IntrospectionRequest, IntrospectionResponse, PubSubConfirmation,
    PubSubSender, ShardMessage, shard_for_key, slot_for_key,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::debug;

use crate::connection::ConnectionHandler;
use crate::connection::state::{SubKind, SubscribeOutcome};
use crate::scatter::{CountByKey, DedupSorted, SumIntegers};
use crate::slot_migration::RouteOutcome;

/// The broadcast pub/sub coordinator shard.
///
/// Broadcast (SUBSCRIBE/PSUBSCRIBE) registrations and PUBLISH delivery all go
/// through this single shard so each subscriber is registered exactly once and
/// each message is delivered exactly once, with a subscriber count that is not
/// multiplied by the number of shards. Forwarded keyspace notifications
/// (`ShardMessage::PublishKeyspace`) and the CLIENT TRACKING BCAST redirect
/// path rely on the same invariant.
pub(crate) const BROADCAST_SHARD: usize = 0;

// ============================================================================
// Subscription-kind table
// ============================================================================
//
// SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE (and their unsubscribe twins) differ only in
// *data*: which subscription set they touch, where the registration is routed,
// which ShardMessage variant carries it, and which confirmation variant
// answers it. That data lives in one table entry per kind; the control flow —
// admit limits on every subscribe, re-arm the 80% warning on every
// unsubscribe, batch one message per destination shard — lives once in
// `subscribe_kind` / `unsubscribe_kind`.

/// Where a subscription kind's shard registrations are routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubRouting {
    /// All registrations target [`BROADCAST_SHARD`] (broadcast pub/sub).
    Broadcast,
    /// Each channel is registered on the shard that owns its key hash
    /// (sharded pub/sub).
    ByChannelKey,
}

/// Everything that varies between the three subscription kinds.
struct SubKindSpec {
    /// Which per-connection subscription set this kind tracks.
    kind: SubKind,
    /// Where shard registrations are routed.
    routing: SubRouting,
    /// Command name used for cluster slot routing (sharded kind only).
    route_command: &'static str,
    /// Arity error for the subscribe command.
    arity_error: &'static str,
    /// Error returned when the per-connection limit is hit.
    limit_error: &'static str,
    /// Build the batched shard registration message.
    subscribe_msg:
        fn(Vec<Bytes>, ConnId, PubSubSender, oneshot::Sender<Vec<usize>>) -> ShardMessage,
    /// Build the batched shard deregistration message.
    unsubscribe_msg: fn(Vec<Bytes>, ConnId, oneshot::Sender<Vec<usize>>) -> ShardMessage,
    /// Build the subscribe confirmation.
    subscribed: fn(Bytes, usize) -> PubSubConfirmation,
    /// Build the unsubscribe confirmation (`None` channel = "nothing to
    /// unsubscribe from").
    unsubscribed: fn(Option<Bytes>, usize) -> PubSubConfirmation,
}

/// Broadcast channel subscriptions (SUBSCRIBE/UNSUBSCRIBE).
static CHANNEL_SPEC: SubKindSpec = SubKindSpec {
    kind: SubKind::Channel,
    routing: SubRouting::Broadcast,
    route_command: "SUBSCRIBE",
    arity_error: "ERR wrong number of arguments for 'subscribe' command",
    limit_error: "ERR max subscriptions reached",
    subscribe_msg: |channels, conn_id, sender, response_tx| ShardMessage::Subscribe {
        channels,
        conn_id,
        sender,
        response_tx,
    },
    unsubscribe_msg: |channels, conn_id, response_tx| ShardMessage::Unsubscribe {
        channels,
        conn_id,
        response_tx,
    },
    subscribed: |channel, count| PubSubConfirmation::Subscribe { channel, count },
    unsubscribed: |channel, count| PubSubConfirmation::Unsubscribe { channel, count },
};

/// Pattern subscriptions (PSUBSCRIBE/PUNSUBSCRIBE).
static PATTERN_SPEC: SubKindSpec = SubKindSpec {
    kind: SubKind::Pattern,
    routing: SubRouting::Broadcast,
    route_command: "PSUBSCRIBE",
    arity_error: "ERR wrong number of arguments for 'psubscribe' command",
    limit_error: "ERR max pattern subscriptions reached",
    subscribe_msg: |patterns, conn_id, sender, response_tx| ShardMessage::PSubscribe {
        patterns,
        conn_id,
        sender,
        response_tx,
    },
    unsubscribe_msg: |patterns, conn_id, response_tx| ShardMessage::PUnsubscribe {
        patterns,
        conn_id,
        response_tx,
    },
    subscribed: |pattern, count| PubSubConfirmation::PSubscribe { pattern, count },
    unsubscribed: |pattern, count| PubSubConfirmation::PUnsubscribe { pattern, count },
};

/// Sharded channel subscriptions (SSUBSCRIBE/SUNSUBSCRIBE).
static SHARDED_SPEC: SubKindSpec = SubKindSpec {
    kind: SubKind::Sharded,
    routing: SubRouting::ByChannelKey,
    route_command: "SSUBSCRIBE",
    arity_error: "ERR wrong number of arguments for 'ssubscribe' command",
    limit_error: "ERR max sharded subscriptions reached",
    subscribe_msg: |channels, conn_id, sender, response_tx| ShardMessage::ShardedSubscribe {
        channels,
        conn_id,
        sender,
        response_tx,
    },
    unsubscribe_msg: |channels, conn_id, response_tx| ShardMessage::ShardedUnsubscribe {
        channels,
        conn_id,
        response_tx,
    },
    subscribed: |channel, count| PubSubConfirmation::SSubscribe { channel, count },
    unsubscribed: |channel, count| PubSubConfirmation::SUnsubscribe { channel, count },
};

/// Group channels by destination shard so each shard receives exactly ONE
/// batched registration message. Broadcast kinds collapse onto
/// [`BROADCAST_SHARD`]; sharded kinds group by key-hash owner. `BTreeMap`
/// keeps the send order deterministic.
fn group_channels_by_shard(
    routing: SubRouting,
    channels: impl IntoIterator<Item = Bytes>,
    num_shards: usize,
) -> BTreeMap<usize, Vec<Bytes>> {
    let mut per_shard: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
    for channel in channels {
        let shard = match routing {
            SubRouting::Broadcast => BROADCAST_SHARD,
            SubRouting::ByChannelKey => shard_for_key(&channel, num_shards),
        };
        per_shard.entry(shard).or_default().push(channel);
    }
    per_shard
}

// ============================================================================
// Pub/Sub command handlers
// ============================================================================

impl ConnectionHandler {
    /// Handle SUBSCRIBE command.
    pub(crate) async fn handle_subscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        self.subscribe_kind(&CHANNEL_SPEC, args).await
    }

    /// Handle UNSUBSCRIBE command.
    pub(crate) async fn handle_unsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        self.unsubscribe_kind(&CHANNEL_SPEC, args).await
    }

    /// Handle PSUBSCRIBE command.
    pub(crate) async fn handle_psubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        self.subscribe_kind(&PATTERN_SPEC, args).await
    }

    /// Handle PUNSUBSCRIBE command.
    pub(crate) async fn handle_punsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        self.unsubscribe_kind(&PATTERN_SPEC, args).await
    }

    /// Subscribe to channels/patterns of one kind.
    ///
    /// Owns the invariants shared by all three subscribe commands:
    /// - the per-connection limit and 80% warning latch are consulted before
    ///   anything is inserted (`admit_subscriptions`);
    /// - confirmations report the *local* per-connection count, in argument
    ///   order (cluster redirects for the sharded kind interleave in place);
    /// - each destination shard receives exactly one batched registration
    ///   message, and we await its ack so the registration is visible on the
    ///   shard before the client sees the confirmation.
    async fn subscribe_kind(&mut self, spec: &SubKindSpec, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error(spec.arity_error)];
        }

        // Enforce the per-connection limit and 80% warning latch.
        if matches!(
            self.state.admit_subscriptions(spec.kind, args.len()),
            SubscribeOutcome::LimitReached
        ) {
            return vec![Response::error(spec.limit_error)];
        }

        // In cluster mode, sharded channels route through the same
        // slot-migration decision as the keyed command path (`route()` +
        // `RouteDecision::to_response`), so they honor ASKING /
        // importing-target / MOVED / CLUSTERDOWN identically. ASKING is a
        // one-shot flag, consumed once for the whole command (not per
        // channel). SSUBSCRIBE is not READONLY-flagged, so the READONLY
        // override never applies (`readonly_eligible` is always false).
        let cluster_router = match spec.routing {
            SubRouting::ByChannelKey => self
                .cluster
                .slot_migration
                .clone()
                .zip(self.cluster.node_id),
            SubRouting::Broadcast => None,
        };
        let asking = if cluster_router.is_some() {
            self.state.take_asking()
        } else {
            false
        };

        let mut responses = Vec::with_capacity(args.len());
        let mut accepted = Vec::with_capacity(args.len());

        for channel in args {
            if let Some((coordinator, node_id)) = &cluster_router {
                let slot = slot_for_key(channel);
                if let RouteOutcome::Reply(resp) = coordinator
                    .route(slot, spec.route_command, asking, *node_id)
                    .to_response(false)
                {
                    responses.push(resp);
                    continue;
                }
            }

            // Add to local tracking; the confirmation count is the
            // per-connection count, so it is known before the shard replies.
            let count = self.state.add_subscription(spec.kind, channel.clone());

            debug!(
                conn_id = self.state.id,
                channel = %String::from_utf8_lossy(channel),
                kind = ?spec.kind,
                "Subscribed"
            );

            accepted.push(channel.clone());
            responses.push(
                (spec.subscribed)(channel.clone(), count).to_response(self.state.protocol_version),
            );
        }

        // One batched registration message per destination shard. Await each
        // shard's ack (the per-shard subscriber counts, unused here — the
        // client-visible count is the per-connection one above) so that by the
        // time the confirmation reaches the client, a PUBLISH processed after
        // it is guaranteed to see the registration.
        if !accepted.is_empty() {
            let pubsub_tx = self.ensure_pubsub_channel();
            for (shard, channels) in
                group_channels_by_shard(spec.routing, accepted, self.num_shards)
            {
                let (response_tx, response_rx) = oneshot::channel();
                let _ = self.core.shard_senders[shard]
                    .send((spec.subscribe_msg)(
                        channels,
                        self.state.id,
                        pubsub_tx.clone(),
                        response_tx,
                    ))
                    .await;
                let _ = response_rx.await;
            }
        }

        responses
    }

    /// Unsubscribe from channels/patterns of one kind.
    ///
    /// Owns the invariants shared by all three unsubscribe commands:
    /// - no args means "all current subscriptions of this kind"; none at all
    ///   yields the single null-channel confirmation;
    /// - each destination shard receives exactly one batched deregistration
    ///   message, acked before the confirmations are returned;
    /// - the 80% warning latch is re-armed after every unsubscribe batch.
    async fn unsubscribe_kind(&mut self, spec: &SubKindSpec, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all channels/patterns of this kind.
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state.subscriptions(spec.kind)
        } else {
            args.to_vec()
        };

        // Nothing to unsubscribe from: null channel, count 0.
        if channels.is_empty() {
            return vec![(spec.unsubscribed)(None, 0).to_response(self.state.protocol_version)];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in &channels {
            // Remove from local tracking; the confirmation count is the
            // per-connection count.
            let count = self.state.remove_subscription(spec.kind, channel);
            responses.push(
                (spec.unsubscribed)(Some(channel.clone()), count)
                    .to_response(self.state.protocol_version),
            );
        }

        // One batched deregistration message per destination shard, acked so
        // the shard-side removal is visible before the client sees the
        // confirmations.
        for (shard, channels) in group_channels_by_shard(spec.routing, channels, self.num_shards) {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[shard]
                .send((spec.unsubscribe_msg)(channels, self.state.id, response_tx))
                .await;
            let _ = response_rx.await;
        }

        // Reset 80% warning if below threshold.
        self.state.rearm_subscription_warning(spec.kind);

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

        // Broadcast pub/sub uses the coordinator shard so the count reflects
        // actual unique subscribers rather than being multiplied by the number
        // of shards.
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self.core.shard_senders[BROADCAST_SHARD]
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
    /// In cluster mode, the channel's slot is routed through the same
    /// slot-migration decision as the keyed command path (`route()` +
    /// [`RouteDecision::to_response`](crate::slot_migration::RouteDecision::to_response)),
    /// so it honors ASKING / importing-target / MOVED / CLUSTERDOWN identically
    /// rather than deciding from raw slot ownership alone.
    pub(crate) async fn handle_ssubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        self.subscribe_kind(&SHARDED_SPEC, args).await
    }

    /// Handle SUNSUBSCRIBE command (sharded subscriptions).
    pub(crate) async fn handle_sunsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        self.unsubscribe_kind(&SHARDED_SPEC, args).await
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
    ///
    /// Fan-out + dedup/sort and the shared gather timeout live in
    /// [`ScatterGather`](crate::scatter::ScatterGather); `FailFast` means a
    /// dropped shard errors rather than silently under-reporting channels.
    async fn handle_pubsub_channels(&self, args: &[Bytes]) -> Response {
        let pattern = if args.is_empty() {
            None
        } else {
            Some(GlobPattern::new(args[0].clone()))
        };

        self.scatter_gather()
            .run(Box::new(DedupSorted::default()), |_shard, response_tx| {
                ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::Channels {
                        pattern: pattern.clone(),
                    },
                    response_tx,
                }
            })
            .await
    }

    /// Handle PUBSUB NUMSUB [channel ...].
    async fn handle_pubsub_numsub(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::Array(vec![]);
        }

        let channels: Vec<Bytes> = args.to_vec();
        self.scatter_gather()
            .run(
                Box::new(CountByKey::new(channels.clone())),
                |_shard, response_tx| ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::NumSub {
                        channels: channels.clone(),
                    },
                    response_tx,
                },
            )
            .await
    }

    /// Handle PUBSUB NUMPAT.
    async fn handle_pubsub_numpat(&self) -> Response {
        self.scatter_gather()
            .run(
                Box::<SumIntegers<IntrospectionResponse>>::default(),
                |_shard, response_tx| ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::NumPat,
                    response_tx,
                },
            )
            .await
    }

    /// Handle PUBSUB SHARDCHANNELS [pattern].
    async fn handle_pubsub_shardchannels(&self, args: &[Bytes]) -> Response {
        let pattern = if args.is_empty() {
            None
        } else {
            Some(GlobPattern::new(args[0].clone()))
        };

        self.scatter_gather()
            .run(Box::new(DedupSorted::default()), |_shard, response_tx| {
                ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::ShardChannels {
                        pattern: pattern.clone(),
                    },
                    response_tx,
                }
            })
            .await
    }

    /// Handle PUBSUB SHARDNUMSUB [channel ...].
    async fn handle_pubsub_shardnumsub(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::Array(vec![]);
        }

        let channels: Vec<Bytes> = args.to_vec();
        self.scatter_gather()
            .run(
                Box::new(CountByKey::new(channels.clone())),
                |_shard, response_tx| ShardMessage::PubSubIntrospection {
                    request: IntrospectionRequest::ShardNumSub {
                        channels: channels.clone(),
                    },
                    response_tx,
                },
            )
            .await
    }
}

// ============================================================================
// Response helper functions (kept from original module)
// ============================================================================
//
// Subscribe/unsubscribe confirmations are no longer hand-built here: they go
// through `frogdb_core::PubSubConfirmation`, the single owner of the
// RESP3-Push-vs-RESP2-Array confirmation shape (see the gate
// `lint-pubsub-confirmation-seam`).

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

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn broadcast_routing_collapses_to_one_message_on_the_coordinator() {
        // N channels -> exactly ONE batch, on the broadcast coordinator shard.
        let channels = vec![b("a"), b("b"), b("c"), b("d")];
        let grouped = group_channels_by_shard(SubRouting::Broadcast, channels.clone(), 8);

        assert_eq!(grouped.len(), 1, "broadcast must batch into one message");
        assert_eq!(grouped[&BROADCAST_SHARD], channels);
    }

    #[test]
    fn sharded_routing_groups_by_owner_and_covers_all_channels() {
        let num_shards = 4;
        let channels: Vec<Bytes> = (0..32).map(|i| b(&format!("chan-{i}"))).collect();
        let grouped =
            group_channels_by_shard(SubRouting::ByChannelKey, channels.clone(), num_shards);

        // At most one message per shard.
        assert!(grouped.len() <= num_shards);
        // Every channel appears exactly once, on the shard that owns it.
        let mut seen = 0;
        for (shard, batch) in &grouped {
            for channel in batch {
                assert_eq!(*shard, shard_for_key(channel, num_shards));
                seen += 1;
            }
        }
        assert_eq!(seen, channels.len());
    }

    #[test]
    fn sharded_routing_preserves_argument_order_within_a_shard() {
        let num_shards = 4;
        let channels: Vec<Bytes> = (0..32).map(|i| b(&format!("chan-{i}"))).collect();
        let grouped =
            group_channels_by_shard(SubRouting::ByChannelKey, channels.clone(), num_shards);

        for batch in grouped.values() {
            let positions: Vec<usize> = batch
                .iter()
                .map(|c| channels.iter().position(|x| x == c).unwrap())
                .collect();
            assert!(
                positions.windows(2).all(|w| w[0] < w[1]),
                "channels within a shard batch must keep argument order"
            );
        }
    }

    #[test]
    fn single_shard_topology_batches_everything_onto_shard_zero() {
        let channels = vec![b("x"), b("y")];
        let grouped = group_channels_by_shard(SubRouting::ByChannelKey, channels.clone(), 1);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[&0], channels);
    }

    #[test]
    fn spec_table_confirmation_constructors_match_their_kind() {
        // The table is data; pin the kind <-> confirmation pairing so a table
        // edit cannot silently cross-wire a confirmation label.
        for (spec, sub_label, unsub_label) in [
            (&CHANNEL_SPEC, "subscribe", "unsubscribe"),
            (&PATTERN_SPEC, "psubscribe", "punsubscribe"),
            (&SHARDED_SPEC, "ssubscribe", "sunsubscribe"),
        ] {
            let confirm =
                (spec.subscribed)(b("ch"), 1).to_response(frogdb_protocol::ProtocolVersion::Resp2);
            let Response::Array(items) = confirm else {
                panic!("RESP2 confirmation must be an Array");
            };
            assert_eq!(items[0], Response::bulk(b(sub_label)));

            let confirm =
                (spec.unsubscribed)(None, 0).to_response(frogdb_protocol::ProtocolVersion::Resp2);
            let Response::Array(items) = confirm else {
                panic!("RESP2 confirmation must be an Array");
            };
            assert_eq!(items[0], Response::bulk(b(unsub_label)));
        }
    }
}
