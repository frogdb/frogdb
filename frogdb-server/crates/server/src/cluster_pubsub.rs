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

    /// Forward an SPUBLISH message to the slot-owning node.
    ///
    /// Returns `Some(count)` if the slot is owned by a remote node (`0` when
    /// the RPC failed, timed out, or answered with the wrong shape — logged in
    /// `send_pubsub_rpc`), or `None` if the slot is owned locally (caller
    /// should do local delivery).
    pub async fn forward_spublish(&self, channel: &[u8], message: &[u8]) -> Option<usize> {
        let Self::Cluster {
            cluster_state,
            network_factory,
            node_id,
        } = self
        else {
            return None;
        };

        let slot = slot_for_key(channel);
        let owner_id = cluster_state.get_slot_owner(slot)?;

        if owner_id == *node_id {
            return None; // Local — caller handles delivery
        }

        let addr = network_factory.get_node_addr(owner_id)?;
        let network = network_factory.connect(owner_id, addr);
        let request = BusRpc::PubSubForward {
            channel: channel.to_vec(),
            message: message.to_vec(),
        }
        .into();

        Some(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_forwarder_broadcast_is_noop() {
        let forwarder = ClusterPubSubForwarder::Local;
        let count = forwarder.broadcast_publish(b"chan", b"msg").await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_local_forwarder_forward_returns_none() {
        let forwarder = ClusterPubSubForwarder::Local;
        let result = forwarder.forward_spublish(b"chan", b"msg").await;
        assert!(result.is_none());
    }

    // `send_pubsub_rpc` is generic over the RPC future, so the timeout + shape
    // mapping is tested with plain async blocks — no network mock needed.

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
