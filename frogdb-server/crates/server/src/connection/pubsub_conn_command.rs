//! The connection-command seam (server-side pub/sub executors).
//!
//! Pub/Sub is the most deeply connection-entangled command group behind the
//! [`ConnectionCommand`] seam: SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/
//! SSUBSCRIBE/SUNSUBSCRIBE emit **one confirmation per channel**
//! ([`ConnectionCommand::execute_multi`]'s `Vec<Response>`), and every command
//! interleaves per-connection subscription-set mutation, the lazily-created
//! pub/sub channel (`pubsub_tx`/`pubsub_rx`), per-shard batched registration,
//! cluster slot-migration routing, channel-access ACL enforcement, and
//! scatter-gather introspection (PUBSUB CHANNELS/NUMSUB/…).
//!
//! Rather than re-plumb all of that through narrow `ConnCtx` field borrows, the
//! logic runs server-side behind the [`PubSubProvider`] seam (the same shape as
//! [`frogdb_core::InfoProvider`]): [`PubSubIo`] bundles the *disjoint* handler
//! borrows the family needs — `&mut` the connection state and the pub/sub
//! channel fields, plus `&` the shard senders and cluster deps — and implements
//! [`PubSubProvider`]. The registrable [`PubSubConnCommand`] executor is a thin
//! dispatch over the command's [`PubSubKind`]; it carries the [`CommandSpec`] so
//! each pub/sub command is a single self-contained unit, exactly like the other
//! migrated connection commands.
//!
//! Dispatch builds a `ConnCtx` whose `pubsub` field holds `Some(&mut pubsub_io)`
//! and whose state/protocol/info placeholders mirror
//! [`ConnectionHandler::conn_ctx_authmut`] (the pub/sub executor reads its live
//! protocol through `PubSubIo`, never `ConnCtx::protocol_version`).

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnId, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, GlobPattern, IntrospectionRequest,
    IntrospectionResponse, KeySpec, LookupSpec, PubSubConfirmation, PubSubMessage, PubSubProvider,
    PubSubSender, ShardMessage, WaiterWake, WalStrategy, shard_for_key, slot_for_key,
};
use frogdb_protocol::Response;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::connection::ConnectionHandler;
use crate::connection::deps::{ClusterDeps, CoreDeps};
use crate::connection::handlers::pubsub::BROADCAST_SHARD;
use crate::connection::permission_guard::PermissionGuard;
use crate::connection::state::{ConnectionState, SubKind, SubscribeOutcome};
use crate::scatter::{CountByKey, DedupSorted, SumIntegers};
use crate::slot_migration::RouteOutcome;

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
// [`PubSubIo::subscribe_kind`] / [`PubSubIo::unsubscribe_kind`].

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

/// Generate PUBSUB command help text.
fn pubsub_help() -> Response {
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

// ============================================================================
// PubSubIo — the disjoint-borrow bundle behind the PubSubProvider seam
// ============================================================================

/// The connection-local pub/sub machinery, as a bundle of *disjoint* borrows of
/// the [`ConnectionHandler`] fields the pub/sub family needs. Held on
/// [`ConnCtx::pubsub`] for the pub/sub executor only; see [`PubSubProvider`].
///
/// The `&mut` borrows (state + the two channel fields) are of distinct handler
/// fields, so they coexist with each other and with the shared `core`/`cluster`
/// borrows — and with the disjoint shared sub-field borrows the surrounding
/// `ConnCtx` takes for its other subsystems (config, registries, …).
pub(crate) struct PubSubIo<'a> {
    /// Per-connection subscription sets, ASKING flag, negotiated protocol.
    state: &'a mut ConnectionState,
    /// Lazily-created pub/sub sender (cloned to shards on first subscribe).
    pubsub_tx: &'a mut Option<PubSubSender>,
    /// Lazily-created pub/sub receiver (paired with `pubsub_tx`).
    pubsub_rx: &'a mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
    /// Shard senders + ACL manager (channel-access enforcement).
    core: &'a CoreDeps,
    /// Cluster slot-migration routing + cross-node pub/sub forwarding.
    cluster: &'a ClusterDeps,
    /// Total shard count (sharded routing + introspection fan-out).
    num_shards: usize,
    /// Scatter-gather deadline for PUBSUB introspection.
    scatter_gather_timeout: Duration,
}

impl<'a> PubSubIo<'a> {
    /// Bundle the disjoint handler borrows the pub/sub family needs.
    pub(crate) fn new(
        state: &'a mut ConnectionState,
        pubsub_tx: &'a mut Option<PubSubSender>,
        pubsub_rx: &'a mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
        core: &'a CoreDeps,
        cluster: &'a ClusterDeps,
        num_shards: usize,
        scatter_gather_timeout: Duration,
    ) -> Self {
        Self {
            state,
            pubsub_tx,
            pubsub_rx,
            core,
            cluster,
            num_shards,
            scatter_gather_timeout,
        }
    }

    /// Ensure the pub/sub channel is initialized, returning a clone of the
    /// sender. Called lazily on the first subscribe to avoid allocating channels
    /// for the ~99% of connections that never use pub/sub.
    fn ensure_pubsub_channel(&mut self) -> PubSubSender {
        if let Some(tx) = self.pubsub_tx.as_ref() {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        *self.pubsub_tx = Some(tx.clone());
        *self.pubsub_rx = Some(rx);
        tx
    }

    /// Bind a lock-free broadcast coordinator over this connection's shard
    /// senders (mirrors [`ConnectionHandler::scatter_gather`]).
    fn scatter_gather(&self) -> crate::scatter::ScatterGather<'_> {
        crate::scatter::ScatterGather::new(
            self.core.shard_senders.as_slice(),
            self.scatter_gather_timeout,
            self.state.id,
        )
    }

    /// Validate that the current user has permission to access every channel:
    /// an unauthenticated connection (no-password instance / pre-AUTH exempt) is
    /// not enforced. Reconstructs the `PermissionGuard` the handler builds in
    /// `run_pre_checks`, over this bundle's disjoint state + ACL borrows.
    #[allow(clippy::result_large_err)]
    fn validate_channel_access(&self, channels: &[Bytes]) -> Result<(), Response> {
        let Some(user) = self.state.authenticated_user() else {
            return Ok(());
        };
        let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
        let guard = PermissionGuard::new(&self.core.acl_manager, user, client_info, self.state.id);
        guard.check_channels(channels)
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

    /// Handle PUBLISH.
    ///
    /// In cluster mode, broadcasts to all other nodes via the forwarder and
    /// sums the subscriber counts.
    async fn handle_publish(&self, args: &[Bytes]) -> Response {
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

        // In cluster mode, also broadcast to all other nodes.
        let remote_count = match &self.cluster.pubsub_forwarder {
            Some(forwarder) => forwarder.broadcast_publish(channel, message).await,
            None => 0,
        };

        Response::Integer((local_count + remote_count) as i64)
    }

    /// Handle SPUBLISH (sharded publish).
    ///
    /// In cluster mode, forwards to the slot-owning node if the channel's slot
    /// is not local.
    async fn handle_spublish(&self, args: &[Bytes]) -> Response {
        if args.len() != 2 {
            return Response::error("ERR wrong number of arguments for 'spublish' command");
        }

        let channel = &args[0];
        let message = &args[1];

        // In cluster mode, forward to the slot owner if not local.
        if let Some(forwarder) = &self.cluster.pubsub_forwarder
            && let Some(count) = forwarder.forward_spublish(channel, message).await
        {
            return Response::Integer(count as i64);
        }

        // Route to the owning shard locally.
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
    async fn handle_pubsub_command(&self, args: &[Bytes]) -> Response {
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

impl PubSubProvider for PubSubIo<'_> {
    fn subscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move {
            if let Err(err) = self.validate_channel_access(args) {
                return vec![err];
            }
            self.subscribe_kind(&CHANNEL_SPEC, args).await
        })
    }

    fn unsubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move { self.unsubscribe_kind(&CHANNEL_SPEC, args).await })
    }

    fn psubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move {
            if let Err(err) = self.validate_channel_access(args) {
                return vec![err];
            }
            self.subscribe_kind(&PATTERN_SPEC, args).await
        })
    }

    fn punsubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move { self.unsubscribe_kind(&PATTERN_SPEC, args).await })
    }

    fn ssubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move {
            if let Err(err) = self.validate_channel_access(args) {
                return vec![err];
            }
            self.subscribe_kind(&SHARDED_SPEC, args).await
        })
    }

    fn sunsubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move { self.unsubscribe_kind(&SHARDED_SPEC, args).await })
    }

    fn publish<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if !args.is_empty()
                && let Err(err) = self.validate_channel_access(&args[..1])
            {
                return err;
            }
            self.handle_publish(args).await
        })
    }

    fn spublish<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if !args.is_empty()
                && let Err(err) = self.validate_channel_access(&args[..1])
            {
                return err;
            }
            self.handle_spublish(args).await
        })
    }

    fn pubsub<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(async move { self.handle_pubsub_command(args).await })
    }
}

// ============================================================================
// The registrable pub/sub executors
// ============================================================================

/// Which pub/sub command a [`PubSubConnCommand`] dispatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PubSubKind {
    Subscribe,
    Unsubscribe,
    PSubscribe,
    PUnsubscribe,
    SSubscribe,
    SUnsubscribe,
    Publish,
    SPublish,
    PubSub,
}

/// Build a pub/sub [`CommandSpec`]. All pub/sub commands share the same
/// mechanical facts (connection-level, no WAL, no waiter wakes, no keyspace
/// event); they differ only in name/arity/flags/keys.
const fn pubsub_spec(
    name: &'static str,
    arity: Arity,
    flags: CommandFlags,
    keys: KeySpec,
) -> CommandSpec {
    CommandSpec {
        name,
        arity,
        flags,
        keys,
        access: AccessSpec::Uniform,
        wal: WalStrategy::NoOp,
        wakes: WaiterWake::None,
        event: EventSpec::NotApplicable,
        requires_same_slot: false,
        lookup: LookupSpec::None,
        strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub),
    }
}

/// Flags shared by every pub/sub command.
const PUBSUB_FLAGS: CommandFlags = CommandFlags::PUBSUB
    .union(CommandFlags::LOADING)
    .union(CommandFlags::STALE);

static SUBSCRIBE_SPEC: CommandSpec =
    pubsub_spec("SUBSCRIBE", Arity::AtLeast(1), PUBSUB_FLAGS, KeySpec::None);
static UNSUBSCRIBE_SPEC: CommandSpec = pubsub_spec(
    "UNSUBSCRIBE",
    Arity::AtLeast(0),
    PUBSUB_FLAGS,
    KeySpec::None,
);
static PSUBSCRIBE_SPEC: CommandSpec =
    pubsub_spec("PSUBSCRIBE", Arity::AtLeast(1), PUBSUB_FLAGS, KeySpec::None);
static PUNSUBSCRIBE_SPEC: CommandSpec = pubsub_spec(
    "PUNSUBSCRIBE",
    Arity::AtLeast(0),
    PUBSUB_FLAGS,
    KeySpec::None,
);
static SSUBSCRIBE_SPEC: CommandSpec =
    pubsub_spec("SSUBSCRIBE", Arity::AtLeast(1), PUBSUB_FLAGS, KeySpec::All);
static SUNSUBSCRIBE_SPEC: CommandSpec = pubsub_spec(
    "SUNSUBSCRIBE",
    Arity::AtLeast(0),
    PUBSUB_FLAGS,
    KeySpec::None,
);
static PUBLISH_SPEC: CommandSpec = pubsub_spec(
    "PUBLISH",
    Arity::Fixed(2),
    PUBSUB_FLAGS.union(CommandFlags::FAST),
    KeySpec::None,
);
static SPUBLISH_SPEC: CommandSpec = pubsub_spec(
    "SPUBLISH",
    Arity::Fixed(2),
    PUBSUB_FLAGS.union(CommandFlags::FAST),
    KeySpec::First,
);
static PUBSUB_SPEC: CommandSpec =
    pubsub_spec("PUBSUB", Arity::AtLeast(1), PUBSUB_FLAGS, KeySpec::None);

/// A registrable, `'static` pub/sub executor. One instance per command name,
/// differing only by [`CommandSpec`] and [`PubSubKind`]; the shared logic lives
/// on [`PubSubIo`] behind the [`PubSubProvider`] seam.
pub(crate) struct PubSubConnCommand {
    spec: &'static CommandSpec,
    kind: PubSubKind,
}

pub(crate) static SUBSCRIBE_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &SUBSCRIBE_SPEC,
    kind: PubSubKind::Subscribe,
};
pub(crate) static UNSUBSCRIBE_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &UNSUBSCRIBE_SPEC,
    kind: PubSubKind::Unsubscribe,
};
pub(crate) static PSUBSCRIBE_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &PSUBSCRIBE_SPEC,
    kind: PubSubKind::PSubscribe,
};
pub(crate) static PUNSUBSCRIBE_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &PUNSUBSCRIBE_SPEC,
    kind: PubSubKind::PUnsubscribe,
};
pub(crate) static SSUBSCRIBE_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &SSUBSCRIBE_SPEC,
    kind: PubSubKind::SSubscribe,
};
pub(crate) static SUNSUBSCRIBE_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &SUNSUBSCRIBE_SPEC,
    kind: PubSubKind::SUnsubscribe,
};
pub(crate) static PUBLISH_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &PUBLISH_SPEC,
    kind: PubSubKind::Publish,
};
pub(crate) static SPUBLISH_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &SPUBLISH_SPEC,
    kind: PubSubKind::SPublish,
};
pub(crate) static PUBSUB_CONN_COMMAND: PubSubConnCommand = PubSubConnCommand {
    spec: &PUBSUB_SPEC,
    kind: PubSubKind::PubSub,
};

impl ConnectionCommand for PubSubConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        self.spec
    }

    /// Single-response entry point. Pub/sub is always dispatched via
    /// [`execute_multi`](Self::execute_multi); this exists only to satisfy the
    /// trait and folds to the first frame (every single-response pub/sub command
    /// — PUBLISH/SPUBLISH/PUBSUB — emits exactly one).
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            self.execute_multi(ctx, args)
                .await
                .into_iter()
                .next()
                .unwrap_or_else(|| Response::error("ERR pub/sub command produced no response"))
        })
    }

    fn execute_multi<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move {
            let pubsub = ctx
                .pubsub
                .as_deref_mut()
                .expect("pub/sub executor requires ConnCtx::pubsub");
            match self.kind {
                PubSubKind::Subscribe => pubsub.subscribe(args).await,
                PubSubKind::Unsubscribe => pubsub.unsubscribe(args).await,
                PubSubKind::PSubscribe => pubsub.psubscribe(args).await,
                PubSubKind::PUnsubscribe => pubsub.punsubscribe(args).await,
                PubSubKind::SSubscribe => pubsub.ssubscribe(args).await,
                PubSubKind::SUnsubscribe => pubsub.sunsubscribe(args).await,
                PubSubKind::Publish => vec![pubsub.publish(args).await],
                PubSubKind::SPublish => vec![pubsub.spublish(args).await],
                PubSubKind::PubSub => vec![pubsub.pubsub(args).await],
            }
        })
    }
}

// ============================================================================
// Handler-side dispatch glue
// ============================================================================

impl ConnectionHandler {
    /// Build the pub/sub view over this handler's disjoint fields and run the
    /// command's `execute_multi` through it. Returns one reply per channel for
    /// the subscribe/unsubscribe family, or a single-element `Vec` for
    /// PUBLISH/SPUBLISH/PUBSUB.
    ///
    /// `command` is the `'static` executor already resolved from the registry,
    /// so it does not conflict with the `&mut self` borrows the `ConnCtx` takes.
    /// The `ConnCtx` mirrors [`conn_ctx_authmut`](Self::conn_ctx_authmut): the
    /// pub/sub executor reads only `ConnCtx::pubsub`, so `info`/`username`/
    /// `protocol_version`/`conn_state`/`tracking` are placeholders (the live
    /// protocol is read through [`PubSubIo`]).
    pub(crate) async fn execute_pubsub(
        &mut self,
        command: &'static dyn ConnectionCommand,
        args: &[Bytes],
    ) -> Vec<Response> {
        static NOOP_INFO: frogdb_core::NoopInfoProvider = frogdb_core::NoopInfoProvider;
        let mut pubsub_io = PubSubIo::new(
            &mut self.state,
            &mut self.pubsub_tx,
            &mut self.pubsub_rx,
            &self.core,
            &self.cluster,
            self.num_shards,
            self.scatter_gather_timeout,
        );
        let mut ctx = ConnCtx {
            config: self.admin.config_manager.as_ref(),
            client_registry: self.admin.client_registry.as_ref(),
            latency_histograms: self.observability.latency_histograms.as_ref(),
            keyspace_stats: self.observability.keyspace_stats.as_ref(),
            shard_senders: self.core.shard_senders.as_slice(),
            snapshot_coordinator: self.admin.snapshot_coordinator.as_ref(),
            hotkey_session: &self.observability.hotkey_session,
            hotkey_cluster: &self.cluster,
            protocol_version: frogdb_protocol::ProtocolVersion::default(),
            cursor_store: self.admin.cursor_store.as_ref(),
            metrics_recorder: self.observability.metrics_recorder.as_ref(),
            memory_diag: &self.memory_diag,
            num_shards: self.num_shards,
            max_clients: self.admin.config_manager.max_clients(),
            acl_manager: self.core.acl_manager.as_ref(),
            command_registry: self.core.registry.as_ref(),
            username: "",
            info: &NOOP_INFO,
            scripting: &frogdb_core::NoopScriptingProvider,
            conn_state: None,
            tracking: None,
            pubsub: Some(&mut pubsub_io),
            monitor: None,
        };
        command.execute_multi(&mut ctx, args).await
    }

    /// Execute a pub/sub command deferred from a transaction (EXEC time),
    /// preserving the pre-migration MULTI semantics exactly:
    /// - PUBLISH / SPUBLISH: a single response into the EXEC array;
    /// - SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE: one confirmation
    ///   per channel — in RESP3 they ride out-of-band after the EXEC array (the
    ///   last is the EXEC-slot value); in RESP2 the last is the EXEC-slot value
    ///   and no out-of-band frames are emitted;
    /// - PUBSUB / SSUBSCRIBE / SUNSUBSCRIBE: rejected inside MULTI.
    ///
    /// Returns `(exec_slot_response, push_confirmations)`.
    pub(crate) async fn exec_pubsub_in_transaction(
        &mut self,
        cmd_name: &str,
        command: &'static dyn ConnectionCommand,
        args: &[Bytes],
    ) -> (Response, Vec<Response>) {
        match cmd_name {
            "PUBLISH" | "SPUBLISH" => {
                let responses = self.execute_pubsub(command, args).await;
                (
                    responses.into_iter().next().unwrap_or_else(Response::ok),
                    vec![],
                )
            }
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" => {
                // Pub/sub subscription commands inside MULTI: the executor returns
                // Vec<Response> (one confirmation per channel), already shaped by
                // the PubSubConfirmation seam — Push in RESP3, Array in RESP2.
                let responses = self.execute_pubsub(command, args).await;
                if self.state.protocol_version.is_resp3() {
                    // In RESP3 the confirmations (already Push frames) ride
                    // out-of-band after the EXEC array; no reshaping needed.
                    let exec_result = responses.last().cloned().unwrap_or_else(Response::ok);
                    (exec_result, responses)
                } else {
                    (
                        responses.into_iter().last().unwrap_or_else(Response::ok),
                        vec![],
                    )
                }
            }
            _ => (
                Response::error("ERR command not supported inside MULTI"),
                vec![],
            ),
        }
    }
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

    /// Every pub/sub spec is a valid ConnectionLevel(PubSub) command.
    #[test]
    fn pubsub_specs_are_connection_level_and_valid() {
        for cmd in [
            &SUBSCRIBE_CONN_COMMAND,
            &UNSUBSCRIBE_CONN_COMMAND,
            &PSUBSCRIBE_CONN_COMMAND,
            &PUNSUBSCRIBE_CONN_COMMAND,
            &SSUBSCRIBE_CONN_COMMAND,
            &SUNSUBSCRIBE_CONN_COMMAND,
            &PUBLISH_CONN_COMMAND,
            &SPUBLISH_CONN_COMMAND,
            &PUBSUB_CONN_COMMAND,
        ] {
            assert!(
                cmd.spec().validate().is_ok(),
                "{}: invalid spec: {:?}",
                cmd.spec().name,
                cmd.spec().validate()
            );
            assert!(
                matches!(
                    cmd.spec().strategy,
                    ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
                ),
                "{}: expected ConnectionLevel(PubSub)",
                cmd.spec().name
            );
        }
    }
}
