//! Cluster pub/sub forwarder for cross-node message delivery.
//!
//! In cluster mode, pub/sub messages need to reach subscribers on all nodes:
//! - `PUBLISH` broadcasts to every node (broadcast pub/sub).
//! - `SPUBLISH` forwards to the slot-owning node (sharded pub/sub).
//! - `SSUBSCRIBE` returns a MOVED redirect if the slot belongs to another node.
//!
//! In standalone mode, the `Local` variant is a no-op — all delivery is local.

use std::net::SocketAddr;
use std::sync::Arc;

use frogdb_core::cluster::{
    ClusterNetworkFactory, ClusterRpcRequest, ClusterRpcResponse, ClusterState, NodeId,
};
use frogdb_core::slot_for_key;
use tokio::task::JoinSet;
use tracing::{debug, warn};

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
    /// the local count separately).
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
                let request = ClusterRpcRequest::PubSubBroadcast { channel, message };

                match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    network.send_rpc(request),
                )
                .await
                {
                    Ok(Ok(ClusterRpcResponse::PubSubBroadcastResult { subscriber_count })) => {
                        subscriber_count
                    }
                    Ok(Ok(other)) => {
                        warn!(target_id, ?other, "Unexpected response for PubSubBroadcast");
                        0
                    }
                    Ok(Err(e)) => {
                        debug!(target_id, error = %e, "PubSubBroadcast RPC failed");
                        0
                    }
                    Err(_) => {
                        debug!(target_id, "PubSubBroadcast RPC timed out");
                        0
                    }
                }
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
    /// Returns `Some(count)` if the message was forwarded to a remote node,
    /// or `None` if the slot is owned locally (caller should do local delivery).
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
        let request = ClusterRpcRequest::PubSubForward {
            channel: channel.to_vec(),
            message: message.to_vec(),
        };

        match tokio::time::timeout(std::time::Duration::from_secs(2), network.send_rpc(request))
            .await
        {
            Ok(Ok(ClusterRpcResponse::PubSubForwardResult { subscriber_count })) => {
                Some(subscriber_count)
            }
            Ok(Ok(other)) => {
                warn!(owner_id, ?other, "Unexpected response for PubSubForward");
                Some(0)
            }
            Ok(Err(e)) => {
                debug!(owner_id, error = %e, "PubSubForward RPC failed");
                Some(0)
            }
            Err(_) => {
                debug!(owner_id, "PubSubForward RPC timed out");
                Some(0)
            }
        }
    }

    /// Get the slot owner's client address for a MOVED redirect.
    ///
    /// Returns `Some((slot, client_addr))` if the slot belongs to a remote node,
    /// or `None` if it belongs to this node (or standalone mode).
    pub fn get_slot_owner_addr(&self, channel: &[u8]) -> Option<(u16, SocketAddr)> {
        let Self::Cluster {
            cluster_state,
            node_id,
            ..
        } = self
        else {
            return None;
        };

        let slot = slot_for_key(channel);
        let owner_id = cluster_state.get_slot_owner(slot)?;

        if owner_id == *node_id {
            return None;
        }

        let owner_info = cluster_state.get_node(owner_id)?;
        Some((slot, owner_info.addr))
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

    #[test]
    fn test_local_forwarder_slot_owner_returns_none() {
        let forwarder = ClusterPubSubForwarder::Local;
        assert!(forwarder.get_slot_owner_addr(b"chan").is_none());
    }
}
