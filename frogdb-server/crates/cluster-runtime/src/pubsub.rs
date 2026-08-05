//! Cluster pub/sub forwarder for cross-node message delivery.
//!
//! In cluster mode, pub/sub messages need to reach subscribers on all nodes:
//! - `PUBLISH` broadcasts to every node (broadcast pub/sub).
//! - `SPUBLISH` forwards to the slot-owning node (sharded pub/sub).
//!
//! `SSUBSCRIBE` redirects are not decided here: the handler routes the channel's
//! slot through the shared `coordinator.route()` + `RouteDecision::to_response`
//! seam (the same path keyed commands use), so the migration/ASKING/CLUSTERDOWN
//! logic lives in exactly one place.
//!
//! In standalone mode, the `Local` variant is a no-op — all delivery is local.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use frogdb_core::cluster::{
    BusRpc, ClusterError, ClusterNetworkFactory, ClusterRpcResponse, ClusterState, NodeId,
};
use frogdb_core::slot_for_key;
use tokio::task::JoinSet;
use tracing::{debug, warn};

/// How long a cross-node pub/sub RPC may take before it is abandoned.
const PUBSUB_RPC_TIMEOUT: Duration = Duration::from_secs(2);

/// Why a cross-node pub/sub RPC yielded no subscriber count.
///
/// Callers fold failures into a count of `0` for the client-visible total (a
/// dead node genuinely contributes zero subscribers), but the failure *mode*
/// is distinguishable — a protocol-shape mismatch is a peer bug and warns,
/// while transport failures and timeouts are expected partition noise and
/// only debug-log.
/// The offending response/error is logged at the point of failure (inside
/// [`send_pubsub_rpc`]), so the variants carry no payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PubSubRpcError {
    /// The peer answered with a response variant that does not match the
    /// request — a protocol bug, not a delivery failure.
    UnexpectedResponse,
    /// Transport-level failure (connect/send/receive).
    Rpc,
    /// The RPC did not complete within [`PUBSUB_RPC_TIMEOUT`].
    Timeout,
}

/// Send one pub/sub RPC and map its outcome.
///
/// This is the single owner of the timeout and the four-arm response mapping
/// that `broadcast_publish` and `forward_spublish` previously each spelled out
/// by hand. `extract` names the response shape the caller expects; any other
/// variant is a warn-logged [`PubSubRpcError::UnexpectedResponse`] rather than
/// a silent zero.
///
/// Generic over the RPC future (rather than a mocked network type) so the
/// mapping is unit-testable with plain `async` blocks — no network involved.
async fn send_pubsub_rpc<F>(
    target: NodeId,
    op: &'static str,
    rpc: F,
    extract: fn(&ClusterRpcResponse) -> Option<usize>,
) -> Result<usize, PubSubRpcError>
where
    F: Future<Output = Result<ClusterRpcResponse, ClusterError>>,
{
    match tokio::time::timeout(PUBSUB_RPC_TIMEOUT, rpc).await {
        Ok(Ok(response)) => match extract(&response) {
            Some(count) => Ok(count),
            None => {
                warn!(
                    target_id = target,
                    ?response,
                    op,
                    "Unexpected response shape for pub/sub RPC"
                );
                Err(PubSubRpcError::UnexpectedResponse)
            }
        },
        Ok(Err(e)) => {
            debug!(target_id = target, error = %e, op, "Pub/sub RPC failed");
            Err(PubSubRpcError::Rpc)
        }
        Err(_) => {
            debug!(target_id = target, op, "Pub/sub RPC timed out");
            Err(PubSubRpcError::Timeout)
        }
    }
}

/// Extract the subscriber count from a `PubSubBroadcastResult`.
fn extract_broadcast_count(response: &ClusterRpcResponse) -> Option<usize> {
    match response {
        ClusterRpcResponse::PubSubBroadcastResult { subscriber_count } => Some(*subscriber_count),
        _ => None,
    }
}

/// Extract the subscriber count from a `PubSubForwardResult`.
fn extract_forward_count(response: &ClusterRpcResponse) -> Option<usize> {
    match response {
        ClusterRpcResponse::PubSubForwardResult { subscriber_count } => Some(*subscriber_count),
        _ => None,
    }
}

/// Where a shard channel's slot says an `SPUBLISH` belongs.
///
/// The three non-`Remote` variants all end in local delivery, but only `Local`
/// is *correct* local delivery: the other two are fallbacks taken because the
/// slot map could not answer. Collapsing them (they were all a bare `None`) made
/// a message delivered on a node that does not own the slot look identical to
/// one delivered on the node that does — see hardening issue 36.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRoute {
    /// This node owns the slot. Deliver locally; nothing is wrong.
    Local,
    /// A peer owns the slot and this node can reach it. Forward.
    Remote {
        /// The slot's owner.
        owner: NodeId,
        /// The owner's cluster-bus address.
        addr: std::net::SocketAddr,
    },
    /// Nobody owns the slot — bootstrap, post-`FORGET`, or mid-reset. Delivery
    /// falls back to local so the message is not dropped, but subscribers on
    /// different nodes then see different subsets of the traffic.
    Unowned {
        /// The channel's slot.
        slot: u16,
    },
    /// A peer owns the slot but this node holds no address for it, so no RPC is
    /// possible. Same local fallback, different cause: a registry gap rather
    /// than an unassigned slot.
    OwnerUnaddressable {
        /// The slot's owner, per the replicated slot map.
        owner: NodeId,
        /// The channel's slot.
        slot: u16,
    },
}

impl ShardRoute {
    /// True when the caller must perform local delivery — correct ownership and
    /// both fallbacks.
    pub fn delivers_locally(&self) -> bool {
        !matches!(self, Self::Remote { .. })
    }
}

/// Outcome of an `SPUBLISH` handed to the forwarder.
///
/// `Forwarded` is the only variant carrying a subscriber count; every other
/// variant means the caller still has to deliver locally, and says why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpublishOutcome {
    /// Sent to the slot's owner. Carries the owner's subscriber count (`0` when
    /// the RPC failed, timed out, or answered with the wrong shape — logged in
    /// [`send_pubsub_rpc`]).
    Forwarded(usize),
    /// Not routed: the caller delivers locally. Carries the route that decided
    /// it, so a correct local delivery stays distinguishable from a fallback.
    Local(ShardRoute),
}

impl SpublishOutcome {
    /// The remote subscriber count, or `None` when the caller must deliver
    /// locally. This is the shape the connection layer consumes, so the
    /// client-visible reply is unchanged by the added distinction.
    pub fn remote_count(&self) -> Option<usize> {
        match self {
            Self::Forwarded(count) => Some(*count),
            Self::Local(_) => None,
        }
    }
}

/// Forwarder for cross-node pub/sub message delivery.
pub enum ClusterPubSubForwarder {
    /// Standalone mode — all delivery is local.
    Local,
    /// Cluster mode — forward messages to other nodes via the cluster bus.
    Cluster {
        cluster_state: Arc<ClusterState>,
        network_factory: Arc<ClusterNetworkFactory>,
        node_id: NodeId,
    },
}

impl ClusterPubSubForwarder {
    /// Broadcast a PUBLISH message to all other nodes in the cluster.
    ///
    /// Returns the total subscriber count from remote nodes (the caller adds
    /// the local count separately). Nodes that fail, time out, or answer with
    /// the wrong response shape contribute zero (logged in `send_pubsub_rpc`).
    pub async fn broadcast_publish(&self, channel: &[u8], message: &[u8]) -> usize {
        let Self::Cluster {
            cluster_state,
            network_factory,
            node_id,
        } = self
        else {
            return 0;
        };

        let all_nodes = network_factory.get_all_nodes();
        if all_nodes.len() <= 1 {
            return 0;
        }

        let mut join_set = JoinSet::new();

        for (&target_id, &addr) in &all_nodes {
            if target_id == *node_id {
                continue;
            }

            // Skip nodes marked as failed
            if let Some(info) = cluster_state.get_node(target_id)
                && info.flags.fail
            {
                continue;
            }

            let channel = channel.to_vec();
            let message = message.to_vec();

            let network = network_factory.connect(target_id, addr);
            join_set.spawn(async move {
                let request = BusRpc::PubSubBroadcast { channel, message }.into();
                send_pubsub_rpc(
                    target_id,
                    "PubSubBroadcast",
                    network.send_rpc(request),
                    extract_broadcast_count,
                )
                .await
                .unwrap_or(0)
            });
        }

        let mut total = 0;
        while let Some(result) = join_set.join_next().await {
            if let Ok(count) = result {
                total += count;
            }
        }
        total
    }

    /// Decide where a shard channel belongs, without sending anything.
    ///
    /// Pure over the slot map and the address registry, so every arm — including
    /// the two fallbacks that used to be invisible — is unit-testable with no
    /// network.
    pub fn route_shard_channel(&self, channel: &[u8]) -> ShardRoute {
        match self {
            // Standalone: there is one node and it owns everything.
            Self::Local => ShardRoute::Local,
            Self::Cluster {
                cluster_state,
                network_factory,
                node_id,
            } => route_shard_channel_in(cluster_state, network_factory, *node_id, channel),
        }
    }

    /// Forward an SPUBLISH message to the slot-owning node.
    ///
    /// [`SpublishOutcome::Forwarded`] carries the owner's subscriber count (`0`
    /// when the RPC failed, timed out, or answered with the wrong shape —
    /// logged in `send_pubsub_rpc`). Every other outcome leaves delivery to the
    /// caller and names the route that decided it: correct local ownership, or
    /// one of the two fallbacks, which are warn-logged here because a message
    /// delivered on a node that does not own its slot is a silent divergence
    /// between subscribers on different nodes.
    ///
    /// Redis answers `MOVED`/`CLUSTERDOWN` for a shard channel it does not
    /// serve; FrogDB's shard pub/sub does not slot-route subscribers at all, so
    /// refusing here would drop messages nobody else would deliver. The
    /// client-visible reply is therefore unchanged — the distinction is
    /// internal, and the "should shard pub/sub slot-route at all" question is
    /// tracked separately.
    pub async fn forward_spublish(&self, channel: &[u8], message: &[u8]) -> SpublishOutcome {
        let Self::Cluster {
            cluster_state,
            network_factory,
            node_id,
        } = self
        else {
            return SpublishOutcome::Local(ShardRoute::Local);
        };

        let route = route_shard_channel_in(cluster_state, network_factory, *node_id, channel);
        let (owner_id, addr) = match route {
            ShardRoute::Remote { owner, addr } => (owner, addr),
            ShardRoute::Local => return SpublishOutcome::Local(route),
            ShardRoute::Unowned { slot } => {
                warn!(
                    slot,
                    "SPUBLISH on a slot nobody owns: delivering locally, so subscribers on \
                     other nodes will not see this message"
                );
                return SpublishOutcome::Local(route);
            }
            ShardRoute::OwnerUnaddressable { owner, slot } => {
                warn!(
                    slot,
                    owner_id = owner,
                    "SPUBLISH owner has no registered address: delivering locally, so \
                     subscribers on the owner will not see this message"
                );
                return SpublishOutcome::Local(route);
            }
        };

        let network = network_factory.connect(owner_id, addr);
        let request = BusRpc::PubSubForward {
            channel: channel.to_vec(),
            message: message.to_vec(),
        }
        .into();

        SpublishOutcome::Forwarded(
            send_pubsub_rpc(
                owner_id,
                "PubSubForward",
                network.send_rpc(request),
                extract_forward_count,
            )
            .await
            .unwrap_or(0),
        )
    }
}

/// The routing decision itself, over the two collaborators that make it.
fn route_shard_channel_in(
    cluster_state: &ClusterState,
    network_factory: &ClusterNetworkFactory,
    node_id: NodeId,
    channel: &[u8],
) -> ShardRoute {
    let slot = slot_for_key(channel);
    let Some(owner) = cluster_state.get_slot_owner(slot) else {
        return ShardRoute::Unowned { slot };
    };
    if owner == node_id {
        return ShardRoute::Local;
    }
    match network_factory.get_node_addr(owner) {
        Some(addr) => ShardRoute::Remote { owner, addr },
        None => ShardRoute::OwnerUnaddressable { owner, slot },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::cluster::{ClusterCommand, NodeInfo, SlotRange};
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// A cluster state with `count` primaries (ids `1..=count`) and every slot
    /// owned by `slot_owner`, or no slots assigned at all when it is `None`.
    fn cluster_state(count: u64, slot_owner: Option<u64>) -> Arc<ClusterState> {
        let state = ClusterState::new();
        for id in 1..=count {
            let port = 6379 + id as u16;
            state
                .apply_local(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000)),
                })
                .expect("seeding a primary must succeed");
        }
        if let Some(owner) = slot_owner {
            state
                .apply_local(ClusterCommand::AssignSlots {
                    node_id: owner,
                    slots: vec![SlotRange::new(0, 16383)],
                })
                .expect("seeding slots must succeed");
        }
        Arc::new(state)
    }

    /// A network factory holding a client address for each of `ids`.
    fn network_with(ids: &[u64]) -> Arc<ClusterNetworkFactory> {
        let factory = ClusterNetworkFactory::new();
        for &id in ids {
            factory.register_node(id, test_addr(16379 + id as u16));
        }
        Arc::new(factory)
    }

    // FM-CLUSTER-069
    #[tokio::test]
    async fn test_local_forwarder_broadcast_is_noop() {
        let forwarder = ClusterPubSubForwarder::Local;
        let count = forwarder.broadcast_publish(b"chan", b"msg").await;
        assert_eq!(count, 0);
    }

    // FM-CLUSTER-069, FM-CLUSTER-070
    #[tokio::test]
    async fn test_local_forwarder_forward_returns_none() {
        let forwarder = ClusterPubSubForwarder::Local;
        let result = forwarder.forward_spublish(b"chan", b"msg").await;
        assert_eq!(result, SpublishOutcome::Local(ShardRoute::Local));
        assert_eq!(result.remote_count(), None);
    }

    /// A single-node cluster has nobody to fan out to: the broadcast must not
    /// dial anything, and in particular must not count its own subscribers a
    /// second time by RPCing itself.
    // FM-CLUSTER-069
    #[tokio::test]
    async fn cluster_broadcast_is_a_noop_below_two_nodes() {
        // No peers registered at all.
        let forwarder = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(1, None),
            network_factory: network_with(&[]),
            node_id: 1,
        };
        assert_eq!(forwarder.broadcast_publish(b"chan", b"msg").await, 0);

        // Exactly one registered node — this one. Still nothing to send.
        let forwarder = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(1, None),
            network_factory: network_with(&[1]),
            node_id: 1,
        };
        assert_eq!(forwarder.broadcast_publish(b"chan", b"msg").await, 0);
    }

    /// The slot is owned here: local delivery, and it is reached before any
    /// address lookup or RPC.
    // FM-CLUSTER-070
    #[tokio::test]
    async fn cluster_forward_returns_none_when_this_node_owns_the_slot() {
        let forwarder = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(2, Some(1)),
            network_factory: network_with(&[1, 2]),
            node_id: 1,
        };
        let outcome = forwarder.forward_spublish(b"chan", b"msg").await;
        assert_eq!(outcome, SpublishOutcome::Local(ShardRoute::Local));
        assert_eq!(outcome.remote_count(), None);
    }

    /// Nobody owns the slot (bootstrap, post-FORGET, mid-reset). The message is
    /// delivered locally rather than dropped, and the outcome says so.
    // FM-CLUSTER-070
    #[tokio::test]
    async fn cluster_forward_distinguishes_an_unowned_slot_from_local_ownership() {
        let unowned = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(2, None),
            network_factory: network_with(&[1, 2]),
            node_id: 1,
        };
        let owned = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(2, Some(1)),
            network_factory: network_with(&[1, 2]),
            node_id: 1,
        };

        let slot = slot_for_key(b"chan");
        let outcome = unowned.forward_spublish(b"chan", b"msg").await;
        assert_eq!(
            outcome,
            SpublishOutcome::Local(ShardRoute::Unowned { slot }),
            "an unowned slot must not report as local ownership"
        );
        assert_ne!(
            outcome,
            owned.forward_spublish(b"chan", b"msg").await,
            "the fallback and the correct delivery must be distinguishable"
        );
        // Client-visible behavior is unchanged: both still deliver locally.
        assert_eq!(outcome.remote_count(), None);
        assert!(unowned.route_shard_channel(b"chan").delivers_locally());
    }

    /// The owner is another node, but this node has no address for it, so no
    /// RPC is possible. Same local fallback, different reported cause.
    // FM-CLUSTER-070
    #[tokio::test]
    async fn cluster_forward_distinguishes_an_unaddressable_owner_from_local_ownership() {
        let unaddressable = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(2, Some(2)),
            // Node 2 owns every slot but is absent from the address registry.
            network_factory: network_with(&[1]),
            node_id: 1,
        };
        let owned = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(2, Some(1)),
            network_factory: network_with(&[1, 2]),
            node_id: 1,
        };

        let slot = slot_for_key(b"chan");
        let outcome = unaddressable.forward_spublish(b"chan", b"msg").await;
        assert_eq!(
            outcome,
            SpublishOutcome::Local(ShardRoute::OwnerUnaddressable { owner: 2, slot }),
            "an unaddressable owner must not report as local ownership"
        );
        assert_ne!(
            outcome,
            owned.forward_spublish(b"chan", b"msg").await,
            "the fallback and the correct delivery must be distinguishable"
        );
        // ... and distinguishable from the *other* fallback, too.
        assert_ne!(
            outcome,
            SpublishOutcome::Local(ShardRoute::Unowned { slot })
        );
        assert_eq!(outcome.remote_count(), None);
    }

    /// A reachable remote owner is the one route that does not deliver locally.
    // FM-CLUSTER-070
    #[test]
    fn cluster_route_names_a_reachable_remote_owner() {
        let forwarder = ClusterPubSubForwarder::Cluster {
            cluster_state: cluster_state(2, Some(2)),
            network_factory: network_with(&[1, 2]),
            node_id: 1,
        };
        let route = forwarder.route_shard_channel(b"chan");
        assert_eq!(
            route,
            ShardRoute::Remote {
                owner: 2,
                addr: test_addr(16381),
            }
        );
        assert!(!route.delivers_locally());
    }

    // `send_pubsub_rpc` is generic over the RPC future, so the timeout + shape
    // mapping is tested with plain async blocks — no network mock needed.

    // FM-CLUSTER-068
    #[tokio::test]
    async fn test_rpc_expected_shape_yields_count() {
        let result = send_pubsub_rpc(
            1,
            "PubSubBroadcast",
            async {
                Ok(ClusterRpcResponse::PubSubBroadcastResult {
                    subscriber_count: 7,
                })
            },
            extract_broadcast_count,
        )
        .await;
        assert_eq!(result, Ok(7));
    }

    // FM-CLUSTER-068
    #[tokio::test]
    async fn test_rpc_shape_mismatch_is_distinguishable_not_zero() {
        // A broadcast request answered with a *forward* result is a protocol
        // bug: it must surface as UnexpectedResponse, not fold into Ok(0).
        let result = send_pubsub_rpc(
            1,
            "PubSubBroadcast",
            async {
                Ok(ClusterRpcResponse::PubSubForwardResult {
                    subscriber_count: 7,
                })
            },
            extract_broadcast_count,
        )
        .await;
        assert_eq!(result, Err(PubSubRpcError::UnexpectedResponse));
    }

    // FM-CLUSTER-068
    #[tokio::test]
    async fn test_rpc_transport_error_maps_to_rpc_variant() {
        let result = send_pubsub_rpc(
            1,
            "PubSubForward",
            async { Err(ClusterError::NetworkError("boom".to_string())) },
            extract_forward_count,
        )
        .await;
        assert_eq!(result, Err(PubSubRpcError::Rpc));
    }

    // FM-CLUSTER-067
    #[tokio::test(start_paused = true)]
    async fn test_rpc_timeout_maps_to_timeout_variant() {
        // A never-completing RPC: with paused time, tokio auto-advances the
        // clock, so the 2s timeout fires without waiting in real time.
        let result = send_pubsub_rpc(
            1,
            "PubSubForward",
            std::future::pending::<Result<ClusterRpcResponse, ClusterError>>(),
            extract_forward_count,
        )
        .await;
        assert_eq!(result, Err(PubSubRpcError::Timeout));
    }

    // FM-CLUSTER-068
    #[tokio::test]
    async fn test_forward_extractor_matches_only_forward_results() {
        assert_eq!(
            extract_forward_count(&ClusterRpcResponse::PubSubForwardResult {
                subscriber_count: 3
            }),
            Some(3)
        );
        assert_eq!(
            extract_forward_count(&ClusterRpcResponse::PubSubBroadcastResult {
                subscriber_count: 3
            }),
            None
        );
        assert_eq!(
            extract_forward_count(&ClusterRpcResponse::Error("nope".to_string())),
            None
        );
    }
}
