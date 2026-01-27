//! Leader-only failure detection for cluster nodes.
//!
//! This module provides a failure detection mechanism that only runs on the Raft leader.
//! It uses TCP connect attempts to detect node failures, which is simpler than a full
//! PING/PONG protocol and leverages the existing Raft infrastructure.
//!
//! Design rationale:
//! - Leader-only detection (like CockroachDB/FoundationDB) instead of peer-to-peer gossip
//! - O(N) complexity instead of O(N²) for gossip
//! - Only the leader can write MarkNodeFailed commands via Raft anyway
//! - TCP connect timeout provides reliable failure detection

use frogdb_core::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use frogdb_core::cluster::{ClusterCommand, ClusterRaft, ClusterState, NodeId, NodeRole};
use frogdb_core::command::QuorumChecker;
use openraft::ServerState;

/// Configuration for failure detection.
#[derive(Debug, Clone)]
pub struct FailureDetectorConfig {
    /// How often to check node health (default: 1000ms).
    pub check_interval_ms: u64,
    /// Timeout for TCP connect (default: 500ms).
    pub connect_timeout_ms: u64,
    /// Number of consecutive failures before marking FAIL (default: 5).
    pub fail_threshold: u32,
    /// Enable automatic failover when primary fails (default: false).
    pub auto_failover: bool,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 1000,
            connect_timeout_ms: 500,
            fail_threshold: 5,
            auto_failover: false,
        }
    }
}

/// Per-node health tracking.
#[derive(Debug, Clone)]
struct NodeHealth {
    /// Last time the node was successfully reached. None if never reached.
    last_seen: Option<Instant>,
    /// Number of consecutive failures.
    failure_count: u32,
    /// Whether we've already marked this node as FAIL via Raft.
    is_marked_fail: bool,
}

impl Default for NodeHealth {
    fn default() -> Self {
        Self {
            last_seen: None, // None = never successfully reached
            failure_count: 0,
            is_marked_fail: false,
        }
    }
}

/// Leader-only failure detector.
///
/// Only performs health checks when this node is the Raft leader.
/// Uses TCP connect attempts to the cluster_addr of each node to detect failures.
pub struct FailureDetector {
    /// This node's ID.
    self_node_id: NodeId,
    /// Configuration for failure detection.
    config: FailureDetectorConfig,
    /// Per-node health tracking.
    health: RwLock<HashMap<NodeId, NodeHealth>>,
    /// Cluster state for reading node information.
    cluster_state: Arc<ClusterState>,
    /// Raft instance for writing failure/recovery commands.
    raft: Arc<ClusterRaft>,
}

impl FailureDetector {
    /// Create a new failure detector.
    pub fn new(
        self_node_id: NodeId,
        config: FailureDetectorConfig,
        cluster_state: Arc<ClusterState>,
        raft: Arc<ClusterRaft>,
    ) -> Self {
        Self {
            self_node_id,
            config,
            health: RwLock::new(HashMap::new()),
            cluster_state,
            raft,
        }
    }

    /// Check if this node is currently the Raft leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.state == ServerState::Leader
    }

    /// Record a successful connection to a node (local tracking).
    /// Only writes to Raft if this node is the leader.
    pub async fn record_success_local(&self, node_id: NodeId, is_leader: bool) {
        let was_failed = {
            let mut health = self.health.write().unwrap();
            let entry = health.entry(node_id).or_default();
            let was_failed = entry.is_marked_fail;
            entry.last_seen = Some(Instant::now());
            entry.failure_count = 0;
            entry.is_marked_fail = false;
            was_failed
        };

        // If node recovered and we're the leader, mark via Raft
        if was_failed && is_leader {
            self.mark_node_recovered(node_id).await;
        }
    }

    /// Record a failed connection attempt to a node (local tracking).
    /// Only writes to Raft if this node is the leader.
    pub async fn record_failure_local(&self, node_id: NodeId, is_leader: bool) {
        let should_mark_fail = {
            let mut health = self.health.write().unwrap();
            let entry = health.entry(node_id).or_default();
            entry.failure_count += 1;

            if entry.failure_count >= self.config.fail_threshold && !entry.is_marked_fail {
                entry.is_marked_fail = true;
                true
            } else {
                false
            }
        };

        if should_mark_fail && is_leader {
            self.mark_node_failed(node_id).await;
        }
    }

    /// Count the number of nodes that are currently reachable from this node's perspective.
    /// A node is reachable if:
    /// - It's this node (self), OR
    /// - It has been checked recently AND is not marked as failed
    pub fn count_reachable_nodes(&self) -> usize {
        let health = self.health.read().unwrap();
        let check_interval = Duration::from_millis(self.config.check_interval_ms);
        // Consider a node unreachable if not seen in N check intervals
        let stale_threshold = check_interval * (self.config.fail_threshold as u32 + 2);

        let all_nodes = self.cluster_state.get_all_nodes();

        // Count reachable nodes: self + nodes with recent successful checks
        let reachable_count = all_nodes
            .iter()
            .filter(|node| {
                if node.id == self.self_node_id {
                    return true; // Self is always reachable
                }

                // Check health entry
                if let Some(entry) = health.get(&node.id) {
                    // Node is reachable if:
                    // 1. Not marked as failed, AND
                    // 2. Has been seen recently (last_seen is Some and not stale)
                    if entry.is_marked_fail {
                        return false;
                    }
                    match entry.last_seen {
                        Some(seen) => seen.elapsed() < stale_threshold,
                        None => false, // Never successfully reached
                    }
                } else {
                    // No entry = never checked = might be unreachable
                    // Be conservative: don't count as reachable until we've verified
                    false
                }
            })
            .count();

        reachable_count
    }

    /// Check if this node can form a quorum with reachable nodes.
    pub fn has_quorum(&self) -> bool {
        let total_nodes = self.cluster_state.get_all_nodes().len();
        let reachable = self.count_reachable_nodes();
        let quorum = (total_nodes / 2) + 1;
        reachable >= quorum
    }

    /// Mark a node as failed via Raft consensus.
    async fn mark_node_failed(&self, node_id: NodeId) {
        let cmd = ClusterCommand::MarkNodeFailed { node_id };
        match self.raft.client_write(cmd).await {
            Ok(_) => {
                tracing::warn!(node_id, "Marked node as FAIL via Raft");

                // Trigger automatic failover if enabled
                if self.config.auto_failover {
                    self.trigger_auto_failover(node_id).await;
                }
            }
            Err(e) => {
                tracing::warn!(node_id, error = %e, "Failed to mark node as failed");
            }
        }
    }

    /// Mark a node as recovered via Raft consensus.
    async fn mark_node_recovered(&self, node_id: NodeId) {
        let cmd = ClusterCommand::MarkNodeRecovered { node_id };
        match self.raft.client_write(cmd).await {
            Ok(_) => {
                tracing::info!(node_id, "Marked node as recovered via Raft");
            }
            Err(e) => {
                tracing::warn!(node_id, error = %e, "Failed to mark node as recovered");
            }
        }
    }

    /// Trigger automatic failover for a failed primary.
    async fn trigger_auto_failover(&self, failed_node_id: NodeId) {
        let snapshot = self.cluster_state.snapshot();

        // Check if failed node is a primary
        let _failed_node = match snapshot.nodes.get(&failed_node_id) {
            Some(n) if n.is_primary() => n,
            _ => return, // Not a primary, no failover needed
        };

        // Find replicas of this primary
        let replicas: Vec<_> = snapshot.get_replicas(failed_node_id);

        if replicas.is_empty() {
            tracing::warn!(
                node_id = failed_node_id,
                "Primary failed but has no replicas for automatic failover"
            );
            return;
        }

        // Select first replica (could be enhanced to pick best replica by replication offset)
        let new_primary = replicas[0];
        tracing::info!(
            failed_primary = failed_node_id,
            new_primary = new_primary.id,
            "Triggering automatic failover"
        );

        // Promote replica: change role to Primary
        let cmd = ClusterCommand::SetRole {
            node_id: new_primary.id,
            role: NodeRole::Primary,
            primary_id: None,
        };
        if let Err(e) = self.raft.client_write(cmd).await {
            tracing::error!(error = %e, "Failed to promote replica during auto-failover");
            return;
        }

        // Transfer slot ownership from failed primary to new primary
        let slots = snapshot.get_node_slots(failed_node_id);
        for range in slots {
            let cmd = ClusterCommand::AssignSlots {
                node_id: new_primary.id,
                slots: vec![range],
            };
            if let Err(e) = self.raft.client_write(cmd).await {
                tracing::error!(error = %e, slot_range = ?range, "Failed to transfer slots during auto-failover");
            }
        }

        tracing::info!(
            new_primary = new_primary.id,
            "Automatic failover completed"
        );
    }

    /// Get the configuration.
    pub fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }
}

impl QuorumChecker for FailureDetector {
    fn has_quorum(&self) -> bool {
        FailureDetector::has_quorum(self)
    }

    fn count_reachable_nodes(&self) -> usize {
        FailureDetector::count_reachable_nodes(self)
    }
}

/// Check if a node is reachable via TCP connect to its cluster_addr.
async fn check_node_reachable(addr: SocketAddr, timeout: Duration) -> bool {
    use tokio::time::timeout as tokio_timeout;
    tokio_timeout(timeout, tokio::net::TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

/// Spawn the failure detection background task.
///
/// All nodes track node reachability locally, but only the leader writes
/// MarkNodeFailed/MarkNodeRecovered commands via Raft consensus.
/// Returns a JoinHandle that can be used to abort the task on shutdown.
pub fn spawn_failure_detector_task(detector: Arc<FailureDetector>) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_millis(detector.config.check_interval_ms);
    let timeout = Duration::from_millis(detector.config.connect_timeout_ms);
    let self_node_id = detector.self_node_id;

    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);

        loop {
            interval_timer.tick().await;

            // All nodes track reachability for local quorum detection,
            // but only the leader writes to Raft.
            let is_leader = detector.is_leader();

            // Get all nodes from cluster state
            let nodes = detector.cluster_state.get_all_nodes();

            for node in nodes {
                if node.id == self_node_id {
                    continue; // Don't check ourselves
                }

                // Continue checking nodes marked as failed - they may recover.
                // The record_success() method will mark them recovered via Raft.

                let detector = detector.clone();
                let addr = node.cluster_addr;
                let node_id = node.id;

                // Check each node concurrently
                tokio::spawn(async move {
                    if check_node_reachable(addr, timeout).await {
                        detector.record_success_local(node_id, is_leader).await;
                    } else {
                        tracing::debug!(node_id, %addr, "Node unreachable");
                        detector.record_failure_local(node_id, is_leader).await;
                    }
                });
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failure_detector_config_default() {
        let config = FailureDetectorConfig::default();
        assert_eq!(config.check_interval_ms, 1000);
        assert_eq!(config.connect_timeout_ms, 500);
        assert_eq!(config.fail_threshold, 5);
        assert!(!config.auto_failover);
    }

    #[test]
    fn test_node_health_default() {
        let health = NodeHealth::default();
        assert_eq!(health.failure_count, 0);
        assert!(!health.is_marked_fail);
    }
}
