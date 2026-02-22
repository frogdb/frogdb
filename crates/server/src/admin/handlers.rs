//! Admin API request handlers.

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};

use super::server::AdminState;

/// Health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub cluster_enabled: bool,
    pub node_id: Option<u64>,
}

/// Cluster state response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateResponse {
    pub enabled: bool,
    pub state: Option<String>,
    pub slots_assigned: u16,
    pub slots_ok: u16,
    pub known_nodes: usize,
    pub config_epoch: u64,
    pub my_id: Option<String>,
}

/// Role response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleResponse {
    pub role: String,
    pub replication_offset: u64,
    pub connected_replicas: usize,
}

/// Node info for the nodes endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfoResponse {
    pub id: u64,
    pub addr: String,
    pub cluster_bus_addr: String,
    pub role: String,
    pub primary_id: Option<u64>,
    pub slots: Vec<SlotRangeResponse>,
    pub flags: Vec<String>,
}

/// Slot range for node info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotRangeResponse {
    pub start: u16,
    pub end: u16,
}

/// Nodes list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesResponse {
    pub nodes: Vec<NodeInfoResponse>,
}

/// Shared admin state type.
pub type SharedAdminState = Arc<AdminState>;

/// Health check endpoint.
///
/// GET /admin/health
pub async fn health(State(state): State<SharedAdminState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        cluster_enabled: state.cluster_state.is_some(),
        node_id: state.node_id,
    })
}

/// Cluster state endpoint.
///
/// GET /admin/cluster
pub async fn cluster_state(
    State(state): State<SharedAdminState>,
) -> Result<Json<ClusterStateResponse>, StatusCode> {
    let response = if let Some(ref cluster_state) = state.cluster_state {
        let snapshot = cluster_state.snapshot();

        // Count assigned slots
        let slots_assigned = snapshot.slot_assignment.len() as u16;

        // Get node ID as string
        let my_id = state.node_id.map(|id| format!("{:040x}", id));

        ClusterStateResponse {
            enabled: true,
            state: Some("ok".to_string()), // TODO: Determine actual cluster state
            slots_assigned,
            slots_ok: slots_assigned, // TODO: Track unhealthy slots
            known_nodes: snapshot.nodes.len(),
            config_epoch: snapshot.config_epoch,
            my_id,
        }
    } else {
        ClusterStateResponse {
            enabled: false,
            state: None,
            slots_assigned: 0,
            slots_ok: 0,
            known_nodes: 0,
            config_epoch: 0,
            my_id: None,
        }
    };

    Ok(Json(response))
}

/// Role endpoint.
///
/// GET /admin/role
pub async fn role(State(state): State<SharedAdminState>) -> Json<RoleResponse> {
    // Check if we have a replication tracker to get real values
    let (replication_offset, connected_replicas) =
        if let Some(ref tracker) = state.replication_tracker {
            (
                tracker.current_offset(),
                tracker.get_streaming_replicas().len(),
            )
        } else {
            (0, 0)
        };

    // Determine role from cluster state or default to master
    let role = if let Some(ref cluster_state) = state.cluster_state {
        if let Some(node_id) = state.node_id {
            if let Some(node_info) = cluster_state.get_node(node_id) {
                if node_info.is_replica() {
                    "slave"
                } else {
                    "master"
                }
            } else {
                "master"
            }
        } else {
            "master"
        }
    } else {
        "master"
    };

    Json(RoleResponse {
        role: role.to_string(),
        replication_offset,
        connected_replicas,
    })
}

/// Nodes list endpoint.
///
/// GET /admin/nodes
pub async fn nodes(State(state): State<SharedAdminState>) -> Json<NodesResponse> {
    let nodes = if let Some(ref cluster_state) = state.cluster_state {
        let snapshot = cluster_state.snapshot();

        snapshot
            .nodes
            .values()
            .map(|node| {
                // Get slot ranges for this node
                let slots: Vec<SlotRangeResponse> = snapshot
                    .get_node_slots(node.id)
                    .iter()
                    .map(|range| SlotRangeResponse {
                        start: range.start,
                        end: range.end,
                    })
                    .collect();

                // Build flags list
                let mut flags = Vec::new();
                if node.is_primary() {
                    flags.push("master".to_string());
                }
                if node.is_replica() {
                    flags.push("slave".to_string());
                }
                // Check if this is us (by comparing IDs)
                if Some(node.id) == state.node_id {
                    flags.push("myself".to_string());
                }
                if node.flags.fail {
                    flags.push("fail".to_string());
                }
                if node.flags.pfail {
                    flags.push("fail?".to_string());
                }
                if node.flags.handshake {
                    flags.push("handshake".to_string());
                }
                if node.flags.noaddr {
                    flags.push("noaddr".to_string());
                }

                NodeInfoResponse {
                    id: node.id,
                    addr: node.addr.to_string(),
                    cluster_bus_addr: node.cluster_addr.to_string(),
                    role: if node.is_replica() {
                        "slave".to_string()
                    } else {
                        "master".to_string()
                    },
                    primary_id: node.primary_id,
                    slots,
                    flags,
                }
            })
            .collect()
    } else {
        // Standalone mode - just return self
        vec![NodeInfoResponse {
            id: state.node_id.unwrap_or(0),
            addr: state.client_addr.clone(),
            cluster_bus_addr: state.cluster_bus_addr.clone().unwrap_or_default(),
            role: "master".to_string(),
            primary_id: None,
            slots: vec![SlotRangeResponse {
                start: 0,
                end: 16383,
            }],
            flags: vec!["myself".to_string(), "master".to_string()],
        }]
    };

    Json(NodesResponse { nodes })
}
