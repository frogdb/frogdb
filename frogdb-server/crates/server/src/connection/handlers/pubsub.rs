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
    GlobPattern, IntrospectionRequest, IntrospectionResponse, PubSubConfirmation, ShardMessage,
    shard_for_key, slot_for_key,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::debug;

use crate::connection::ConnectionHandler;
use crate::connection::state::{SubKind, SubscribeOutcome};
use crate::scatter::{CountByKey, DedupSorted, SumIntegers};
use crate::slot_migration::RouteOutcome;

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

        // Enforce the per-connection limit and 80% warning latch.
        if matches!(
            self.state.admit_subscriptions(SubKind::Channel, args.len()),
            SubscribeOutcome::LimitReached
        ) {
            return vec![Response::error("ERR max subscriptions reached")];
        }

        let mut responses = Vec::with_capacity(args.len());

        // Fan out to all shards for broadcast subscriptions
        for channel in args {
            // Add to local tracking
            let count = self
                .state
                .add_subscription(SubKind::Channel, channel.clone());

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

            // Build subscription confirmation through the protocol seam.
            responses.push(
                PubSubConfirmation::Subscribe {
                    channel: channel.clone(),
                    count,
                }
                .to_response(self.state.protocol_version),
            );
        }

        responses
    }

    /// Handle UNSUBSCRIBE command.
    pub(crate) async fn handle_unsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all channels
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state.subscriptions(SubKind::Channel)
        } else {
            args.to_vec()
        };

        // Handle case where no channels to unsubscribe from
        if channels.is_empty() {
            return vec![
                PubSubConfirmation::Unsubscribe {
                    channel: None,
                    count: 0,
                }
                .to_response(self.state.protocol_version),
            ];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in channels {
            // Remove from local tracking
            let count = self.state.remove_subscription(SubKind::Channel, &channel);

            // Broadcast pub/sub uses shard 0 as the coordinator.
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[0]
                .send(ShardMessage::Unsubscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    response_tx,
                })
                .await;

            // Build unsubscription confirmation through the protocol seam.
            responses.push(
                PubSubConfirmation::Unsubscribe {
                    channel: Some(channel.clone()),
                    count,
                }
                .to_response(self.state.protocol_version),
            );
        }

        // Reset 80% warning if below threshold
        self.state.rearm_subscription_warning(SubKind::Channel);

        responses
    }

    /// Handle PSUBSCRIBE command.
    pub(crate) async fn handle_psubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error(
                "ERR wrong number of arguments for 'psubscribe' command",
            )];
        }

        // Enforce the per-connection limit and 80% warning latch.
        if matches!(
            self.state.admit_subscriptions(SubKind::Pattern, args.len()),
            SubscribeOutcome::LimitReached
        ) {
            return vec![Response::error("ERR max pattern subscriptions reached")];
        }

        let mut responses = Vec::with_capacity(args.len());

        for pattern in args {
            // Add to local tracking
            let count = self
                .state
                .add_subscription(SubKind::Pattern, pattern.clone());

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

            // Build subscription confirmation through the protocol seam.
            responses.push(
                PubSubConfirmation::PSubscribe {
                    pattern: pattern.clone(),
                    count,
                }
                .to_response(self.state.protocol_version),
            );
        }

        responses
    }

    /// Handle PUNSUBSCRIBE command.
    pub(crate) async fn handle_punsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all patterns
        let patterns: Vec<Bytes> = if args.is_empty() {
            self.state.subscriptions(SubKind::Pattern)
        } else {
            args.to_vec()
        };

        // Handle case where no patterns to unsubscribe from
        if patterns.is_empty() {
            return vec![
                PubSubConfirmation::PUnsubscribe {
                    pattern: None,
                    count: 0,
                }
                .to_response(self.state.protocol_version),
            ];
        }

        let mut responses = Vec::with_capacity(patterns.len());

        for pattern in patterns {
            // Remove from local tracking
            let count = self.state.remove_subscription(SubKind::Pattern, &pattern);

            // Broadcast pub/sub uses shard 0 as the coordinator.
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.core.shard_senders[0]
                .send(ShardMessage::PUnsubscribe {
                    patterns: vec![pattern.clone()],
                    conn_id: self.state.id,
                    response_tx,
                })
                .await;

            // Build unsubscription confirmation through the protocol seam.
            responses.push(
                PubSubConfirmation::PUnsubscribe {
                    pattern: Some(pattern.clone()),
                    count,
                }
                .to_response(self.state.protocol_version),
            );
        }

        // Reset 80% warning if below threshold
        self.state.rearm_subscription_warning(SubKind::Pattern);

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
    /// In cluster mode, the channel's slot is routed through the same
    /// slot-migration decision as the keyed command path (`route()` +
    /// [`RouteDecision::to_response`](crate::slot_migration::RouteDecision::to_response)),
    /// so it honors ASKING / importing-target / MOVED / CLUSTERDOWN identically
    /// rather than deciding from raw slot ownership alone.
    pub(crate) async fn handle_ssubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error(
                "ERR wrong number of arguments for 'ssubscribe' command",
            )];
        }

        // Enforce the per-connection limit and 80% warning latch.
        if matches!(
            self.state.admit_subscriptions(SubKind::Sharded, args.len()),
            SubscribeOutcome::LimitReached
        ) {
            return vec![Response::error("ERR max sharded subscriptions reached")];
        }

        // In cluster mode, route each channel's slot through the migration-aware
        // decision. ASKING is a one-shot flag, consumed once for the whole
        // command (not per channel). SSUBSCRIBE is not READONLY-flagged, so the
        // READONLY override never applies (`readonly_eligible` is always false).
        let cluster_router = self
            .cluster
            .slot_migration
            .clone()
            .zip(self.cluster.node_id);
        let asking = if cluster_router.is_some() {
            self.state.take_asking()
        } else {
            false
        };

        let mut responses = Vec::with_capacity(args.len());

        for channel in args {
            if let Some((coordinator, node_id)) = &cluster_router {
                let slot = slot_for_key(channel);
                if let RouteOutcome::Reply(resp) = coordinator
                    .route(slot, "SSUBSCRIBE", asking, *node_id)
                    .to_response(false)
                {
                    responses.push(resp);
                    continue;
                }
            }

            // Add to local tracking
            let count = self
                .state
                .add_subscription(SubKind::Sharded, channel.clone());

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

            // Build subscription confirmation through the protocol seam.
            responses.push(
                PubSubConfirmation::SSubscribe {
                    channel: channel.clone(),
                    count,
                }
                .to_response(self.state.protocol_version),
            );
        }

        responses
    }

    /// Handle SUNSUBSCRIBE command (sharded subscriptions).
    pub(crate) async fn handle_sunsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all sharded channels
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state.subscriptions(SubKind::Sharded)
        } else {
            args.to_vec()
        };

        // Handle case where no channels to unsubscribe from
        if channels.is_empty() {
            return vec![
                PubSubConfirmation::SUnsubscribe {
                    channel: None,
                    count: 0,
                }
                .to_response(self.state.protocol_version),
            ];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in channels {
            // Remove from local tracking
            let count = self.state.remove_subscription(SubKind::Sharded, &channel);

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

            // Build unsubscription confirmation through the protocol seam.
            responses.push(
                PubSubConfirmation::SUnsubscribe {
                    channel: Some(channel.clone()),
                    count,
                }
                .to_response(self.state.protocol_version),
            );
        }

        // Reset 80% warning if below threshold
        self.state.rearm_subscription_warning(SubKind::Sharded);

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
