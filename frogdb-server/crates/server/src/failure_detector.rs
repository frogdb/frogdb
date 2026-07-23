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

use frogdb_core::cluster::{
    ClusterCommand, ClusterNetworkFactory, ClusterRaft, ClusterState, NodeId, NodeInfo,
};
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
#[derive(Debug, Clone, Default)]
struct NodeHealth {
    /// Last time the node was successfully reached. None if never reached.
    last_seen: Option<Instant>,
    /// Number of consecutive failures.
    failure_count: u32,
    /// Whether we've already marked this node as FAIL via Raft.
    is_marked_fail: bool,
}

/// Outcome of recording a successful health check against a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SuccessOutcome {
    /// The node was previously latched as failed and has now recovered.
    /// The caller should propagate the recovery (e.g. via Raft).
    Recovered,
    /// The node was already healthy; nothing to propagate.
    NoChange,
}

/// Outcome of recording a failed health check against a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureOutcome {
    /// The consecutive-failure count just crossed the threshold and the node
    /// was latched as failed for the first time. The caller should propagate
    /// the failure (e.g. via Raft).
    ShouldMarkFail,
    /// The failure was recorded but the node is not (newly) latched as failed.
    NoChange,
}

/// Pure per-node health state machine.
///
/// Owns the `NodeId -> NodeHealth` table plus the thresholds that drive
/// failure latching and staleness. It contains **no** Raft, network, socket,
/// or global-clock dependencies: every method that needs the current time
/// takes it as a parameter so behavior is fully deterministic and unit
/// testable. `FailureDetector` wraps this table and maps its outcomes to Raft
/// writes and TCP probes.
#[derive(Debug)]
struct HealthTable {
    /// Per-node health tracking.
    health: HashMap<NodeId, NodeHealth>,
    /// Number of consecutive failures before latching a node as FAIL.
    fail_threshold: u32,
    /// Base cadence of health checks; scales the staleness window.
    check_interval: Duration,
}

impl HealthTable {
    /// Create an empty health table.
    fn new(fail_threshold: u32, check_interval: Duration) -> Self {
        Self {
            health: HashMap::new(),
            fail_threshold,
            check_interval,
        }
    }

    /// Record a successful health check for `node_id` at time `now`.
    ///
    /// Resets the failure count, clears any latched failure, and refreshes the
    /// last-seen timestamp. Returns [`SuccessOutcome::Recovered`] iff the node
    /// was previously latched as failed (so the caller can propagate recovery).
    fn record_success(&mut self, node_id: NodeId, now: Instant) -> SuccessOutcome {
        let entry = self.health.entry(node_id).or_default();
        let was_failed = entry.is_marked_fail;
        entry.last_seen = Some(now);
        entry.failure_count = 0;
        entry.is_marked_fail = false;
        if was_failed {
            SuccessOutcome::Recovered
        } else {
            SuccessOutcome::NoChange
        }
    }

    /// Record a failed health check for `node_id`.
    ///
    /// Increments the consecutive-failure count. Returns
    /// [`FailureOutcome::ShouldMarkFail`] exactly once — on the transition
    /// where the count first reaches `fail_threshold` and the node is not yet
    /// latched — so the caller propagates the failure a single time (the flag
    /// latches until a success resets it).
    fn record_failure(&mut self, node_id: NodeId) -> FailureOutcome {
        let entry = self.health.entry(node_id).or_default();
        entry.failure_count += 1;

        if entry.failure_count >= self.fail_threshold && !entry.is_marked_fail {
            entry.is_marked_fail = true;
            FailureOutcome::ShouldMarkFail
        } else {
            FailureOutcome::NoChange
        }
    }

    /// Count how many of `nodes` are reachable at time `now`.
    ///
    /// A node counts as reachable if it is `self_node_id` (always reachable),
    /// or it is not latched as failed AND was last seen within the staleness
    /// window `check_interval * (fail_threshold + 2)`. Nodes never seen, or not
    /// present in the table, are treated as unreachable (conservative).
    fn reachable_count(&self, nodes: &[NodeInfo], self_node_id: NodeId, now: Instant) -> usize {
        // Consider a node unreachable if not seen in N check intervals.
        let stale_threshold = self.check_interval * (self.fail_threshold + 2);

        nodes
            .iter()
            .filter(|node| {
                if node.id == self_node_id {
                    return true; // Self is always reachable
                }

                // Check health entry
                if let Some(entry) = self.health.get(&node.id) {
                    // Node is reachable if:
                    // 1. Not marked as failed, AND
                    // 2. Has been seen recently (last_seen is Some and not stale)
                    if entry.is_marked_fail {
                        return false;
                    }
                    match entry.last_seen {
                        Some(seen) => now.saturating_duration_since(seen) < stale_threshold,
                        None => false, // Never successfully reached
                    }
                } else {
                    // No entry = never checked = might be unreachable
                    // Be conservative: don't count as reachable until we've verified
                    false
                }
            })
            .count()
    }

    /// Whether `self_node_id` can form a majority quorum among `nodes` at
    /// `now` (reachable >= floor(total / 2) + 1).
    fn has_quorum(&self, nodes: &[NodeInfo], self_node_id: NodeId, now: Instant) -> bool {
        let total_nodes = nodes.len();
        let reachable = self.reachable_count(nodes, self_node_id, now);
        let quorum = (total_nodes / 2) + 1;
        reachable >= quorum
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
    /// Pure per-node health state machine (thresholds, latching, staleness).
    health: RwLock<HealthTable>,
    /// Cluster state for reading node information.
    cluster_state: Arc<ClusterState>,
    /// Raft instance for writing failure/recovery commands.
    raft: Arc<ClusterRaft>,
    /// Network factory for pooled connections to peers.
    network_factory: Arc<ClusterNetworkFactory>,
}

impl FailureDetector {
    /// Create a new failure detector.
    pub fn new(
        self_node_id: NodeId,
        config: FailureDetectorConfig,
        cluster_state: Arc<ClusterState>,
        raft: Arc<ClusterRaft>,
        network_factory: Arc<ClusterNetworkFactory>,
    ) -> Self {
        let health = HealthTable::new(
            config.fail_threshold,
            Duration::from_millis(config.check_interval_ms),
        );
        Self {
            self_node_id,
            config,
            health: RwLock::new(health),
            cluster_state,
            raft,
            network_factory,
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
        let outcome = self
            .health
            .write()
            .unwrap()
            .record_success(node_id, Instant::now());

        // If node recovered and we're the leader, mark via Raft
        if outcome == SuccessOutcome::Recovered && is_leader {
            self.mark_node_recovered(node_id).await;
        }
    }

    /// Record a failed connection attempt to a node (local tracking).
    /// Only writes to Raft if this node is the leader.
    pub async fn record_failure_local(&self, node_id: NodeId, is_leader: bool) {
        let outcome = self.health.write().unwrap().record_failure(node_id);

        if outcome == FailureOutcome::ShouldMarkFail && is_leader {
            self.mark_node_failed(node_id).await;
        }
    }

    /// Count the number of nodes that are currently reachable from this node's perspective.
    /// A node is reachable if:
    /// - It's this node (self), OR
    /// - It has been checked recently AND is not marked as failed
    pub fn count_reachable_nodes(&self) -> usize {
        let all_nodes = self.cluster_state.get_all_nodes();
        self.health
            .read()
            .unwrap()
            .reachable_count(&all_nodes, self.self_node_id, Instant::now())
    }

    /// Check if this node can form a quorum with reachable nodes.
    pub fn has_quorum(&self) -> bool {
        let all_nodes = self.cluster_state.get_all_nodes();
        self.health
            .read()
            .unwrap()
            .has_quorum(&all_nodes, self.self_node_id, Instant::now())
    }

    /// Mark a node as failed via Raft consensus.
    ///
    /// `MarkNodeFailed` bumps the config epoch inside the same state-machine
    /// transition, so no separate `IncrementEpoch` entry is needed (a separate
    /// entry could be lost on leader crash, leaving the FAIL flag visible at a
    /// stale epoch).
    async fn mark_node_failed(&self, node_id: NodeId) {
        let cmd = ClusterCommand::MarkNodeFailed { node_id };
        match self.raft.client_write(cmd).await {
            Ok(resp) => {
                if let frogdb_core::cluster::ClusterResponse::Error(msg) = &resp.data {
                    tracing::warn!(node_id, error = %msg, "MarkNodeFailed rejected by state machine");
                    return;
                }
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
    ///
    /// Queries each candidate replica's replication offset via a HealthProbe RPC,
    /// then applies a scoring formula to select the replica with the least data loss.
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

        // Query each replica's replication offset via HealthProbe RPC
        let mut replica_offsets: Vec<(NodeId, u64)> = Vec::new();
        for replica in &replicas {
            let net = self
                .network_factory
                .connect(replica.id, replica.cluster_addr);
            match tokio::time::timeout(
                Duration::from_millis(self.config.connect_timeout_ms),
                net.health_probe(),
            )
            .await
            {
                Ok(Ok((_nid, offset))) => replica_offsets.push((replica.id, offset)),
                _ => {
                    tracing::warn!(
                        replica_id = replica.id,
                        "Could not query replica offset for scoring"
                    );
                    replica_offsets.push((replica.id, 0)); // Unreachable gets worst offset
                }
            }
        }

        let max_offset = replica_offsets.iter().map(|(_, o)| *o).max().unwrap_or(0);

        // Score and select best replica (lowest score wins)
        let best = replicas
            .iter()
            .filter(|r| r.replica_priority != 0) // Exclude never-promote
            .min_by(|a, b| {
                let score_a = compute_replica_score(a, max_offset, &replica_offsets);
                let score_b = compute_replica_score(b, max_offset, &replica_offsets);
                score_a.cmp(&score_b).then_with(|| a.id.cmp(&b.id)) // Deterministic tiebreaker
            });

        let new_primary = match best {
            Some(node) => node,
            None => {
                tracing::warn!(
                    node_id = failed_node_id,
                    "Primary failed but all replicas have priority 0 (never-promote)"
                );
                return;
            }
        };

        let replica_offset = replica_offsets
            .iter()
            .find(|(id, _)| *id == new_primary.id)
            .map(|(_, o)| *o)
            .unwrap_or(0);

        tracing::info!(
            failed_primary = failed_node_id,
            new_primary = new_primary.id,
            replica_offset = replica_offset,
            max_offset = max_offset,
            score = compute_replica_score(new_primary, max_offset, &replica_offsets),
            "Triggering automatic failover with lag-based scoring"
        );

        // One replicated log entry: remove the failed primary, transfer its
        // slots to the successor, promote the successor, and bump the config
        // epoch atomically. Either the whole topology change commits or none
        // of it does — there is no window where the failed primary's slots are
        // ownerless.
        //
        // The write is retried a few times: is_marked_fail latches, so if this
        // single command were dropped (e.g. transient leadership churn) the
        // failover would not be re-attempted until the node recovered and
        // failed again.
        let cmd = ClusterCommand::Failover {
            old_primary_id: failed_node_id,
            new_primary_id: new_primary.id,
            force: true,
        };

        const MAX_ATTEMPTS: u32 = 3;
        for attempt in 1..=MAX_ATTEMPTS {
            match self.raft.client_write(cmd.clone()).await {
                Ok(resp) => {
                    if let frogdb_core::cluster::ClusterResponse::Error(msg) = &resp.data {
                        // The state machine rejected the transition (e.g. the
                        // successor vanished); retrying the same command cannot
                        // succeed, so surface and stop.
                        tracing::error!(
                            failed_primary = failed_node_id,
                            new_primary = new_primary.id,
                            error = %msg,
                            "Auto-failover rejected by cluster state machine"
                        );
                        return;
                    }
                    tracing::info!(
                        failed_primary = failed_node_id,
                        new_primary = new_primary.id,
                        "Automatic failover completed"
                    );
                    return;
                }
                Err(e) if attempt < MAX_ATTEMPTS => {
                    tracing::warn!(
                        error = %e,
                        attempt,
                        "Auto-failover Raft write failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        failed_primary = failed_node_id,
                        new_primary = new_primary.id,
                        "Auto-failover failed after {MAX_ATTEMPTS} attempts"
                    );
                }
            }
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }
}

/// Compute a failover score for a replica. Lower score = better candidate.
///
/// Formula from the spec (docs/spec/CLUSTER.md lines 1733-1755):
/// ```text
/// score = (priority == 0 ? u64::MAX : priority * 100_000)
///       + (max_offset - replica_offset) * 1_000
/// ```
///
/// Priority 0 means never promote (returns u64::MAX).
fn compute_replica_score(node: &NodeInfo, max_offset: u64, offsets: &[(NodeId, u64)]) -> u64 {
    let priority = node.replica_priority;
    if priority == 0 {
        return u64::MAX;
    }
    let replica_offset = offsets
        .iter()
        .find(|(id, _)| *id == node.id)
        .map(|(_, o)| *o)
        .unwrap_or(0);
    (priority as u64) * 100_000 + max_offset.saturating_sub(replica_offset) * 1_000
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
///
/// Dials through `crate::net::TcpStream` so the probe is routed over turmoil's
/// simulated network under the `turmoil` feature (tokio's real TCP driver is
/// disabled inside a simulation) and over tokio in production.
async fn check_node_reachable(addr: SocketAddr, timeout: Duration) -> bool {
    use tokio::time::timeout as tokio_timeout;
    tokio_timeout(timeout, crate::net::TcpStream::connect(addr))
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

    // ---- HealthTable: pure state-machine tests (no Raft, no network, no clock) ----

    /// N-1 consecutive failures must NOT latch; the Nth latches exactly once;
    /// further failures are NoChange; a success resets so it takes N again.
    #[test]
    fn test_health_table_threshold_latching() {
        let now = Instant::now();
        for threshold in [1u32, 2, 5] {
            let mut table = HealthTable::new(threshold, Duration::from_millis(1000));
            let node: NodeId = 42;

            // First threshold-1 failures do not latch.
            for i in 1..threshold {
                assert_eq!(
                    table.record_failure(node),
                    FailureOutcome::NoChange,
                    "failure #{i} (threshold {threshold}) must not latch"
                );
            }

            // The threshold-th failure latches exactly once.
            assert_eq!(
                table.record_failure(node),
                FailureOutcome::ShouldMarkFail,
                "failure #{threshold} (threshold {threshold}) must latch"
            );

            // Subsequent failures do not re-fire while latched.
            assert_eq!(table.record_failure(node), FailureOutcome::NoChange);
            assert_eq!(table.record_failure(node), FailureOutcome::NoChange);

            // A success on a latched node reports recovery and clears state.
            assert_eq!(table.record_success(node, now), SuccessOutcome::Recovered);

            // After recovery it takes a full `threshold` run again to re-latch.
            for i in 1..threshold {
                assert_eq!(
                    table.record_failure(node),
                    FailureOutcome::NoChange,
                    "post-recovery failure #{i} (threshold {threshold}) must not latch"
                );
            }
            assert_eq!(table.record_failure(node), FailureOutcome::ShouldMarkFail);
        }
    }

    /// A success on a node that was never latched reports NoChange.
    #[test]
    fn test_health_table_success_without_prior_failure_is_nochange() {
        let now = Instant::now();
        let mut table = HealthTable::new(3, Duration::from_millis(1000));
        assert_eq!(table.record_success(7, now), SuccessOutcome::NoChange);
        // A couple of sub-threshold failures then a success is still NoChange
        // (never latched), and it resets the counter.
        assert_eq!(table.record_failure(7), FailureOutcome::NoChange);
        assert_eq!(table.record_failure(7), FailureOutcome::NoChange);
        assert_eq!(table.record_success(7, now), SuccessOutcome::NoChange);
        // Counter reset: two fresh failures still do not latch (threshold 3).
        assert_eq!(table.record_failure(7), FailureOutcome::NoChange);
        assert_eq!(table.record_failure(7), FailureOutcome::NoChange);
        assert_eq!(table.record_failure(7), FailureOutcome::ShouldMarkFail);
    }

    /// Staleness window boundary: reachable strictly while
    /// `now - last_seen < check_interval * (fail_threshold + 2)`.
    #[test]
    fn test_health_table_staleness_boundary() {
        let t0 = Instant::now();
        let check_interval = Duration::from_millis(100);
        let fail_threshold = 3;
        // stale_threshold = 100ms * (3 + 2) = 500ms
        let stale = check_interval * (fail_threshold + 2);
        assert_eq!(stale, Duration::from_millis(500));

        let mut table = HealthTable::new(fail_threshold, check_interval);
        let self_id: NodeId = 1;
        let peer_id: NodeId = 2;
        let nodes = vec![make_node(self_id, 1), make_node(peer_id, 1)];

        table.record_success(peer_id, t0);

        // Just inside the window: peer still reachable (self always counts) => 2.
        assert_eq!(
            table.reachable_count(&nodes, self_id, t0 + stale - Duration::from_millis(1)),
            2,
            "peer must be reachable just inside the staleness window"
        );

        // Exactly at the window boundary: strict `<` means peer is stale => 1 (self only).
        assert_eq!(
            table.reachable_count(&nodes, self_id, t0 + stale),
            1,
            "peer must be stale exactly at the boundary"
        );

        // Past the window: still just self.
        assert_eq!(
            table.reachable_count(&nodes, self_id, t0 + stale + Duration::from_millis(1)),
            1
        );
    }

    /// A node that was never seen, or one latched as failed, is unreachable;
    /// self is always reachable regardless.
    #[test]
    fn test_health_table_reachable_never_seen_and_failed() {
        let now = Instant::now();
        let mut table = HealthTable::new(3, Duration::from_millis(100));
        let self_id: NodeId = 1;
        let never: NodeId = 2;
        let failed: NodeId = 3;
        let nodes = vec![
            make_node(self_id, 1),
            make_node(never, 1),
            make_node(failed, 1),
        ];

        // `never` has no entry; `failed` gets latched.
        for _ in 0..3 {
            table.record_failure(failed);
        }

        // Only self counts.
        assert_eq!(table.reachable_count(&nodes, self_id, now), 1);
    }

    /// Quorum arithmetic for odd and even cluster sizes.
    #[test]
    fn test_health_table_quorum_arithmetic() {
        let now = Instant::now();
        let check_interval = Duration::from_millis(1000);
        let self_id: NodeId = 1;

        // (total_nodes, reachable_peers_seen, expected_quorum)
        // quorum threshold = total/2 + 1; reachable = 1 (self) + reachable_peers.
        let cases = [
            (3usize, 0usize, false), // reachable 1, need 2
            (3, 1, true),            // reachable 2, need 2
            (3, 2, true),            // reachable 3, need 2
            (4, 1, false),           // reachable 2, need 3
            (4, 2, true),            // reachable 3, need 3
            (5, 1, false),           // reachable 2, need 3
            (5, 2, true),            // reachable 3, need 3
            (1, 0, true),            // solo node: reachable 1, need 1
            (2, 0, false),           // reachable 1, need 2
        ];

        for (total, reachable_peers, expected) in cases {
            let mut table = HealthTable::new(3, check_interval);
            let mut nodes = vec![make_node(self_id, 1)];
            for peer in 0..(total - 1) {
                let peer_id = (100 + peer) as NodeId;
                nodes.push(make_node(peer_id, 1));
                if peer < reachable_peers {
                    table.record_success(peer_id, now);
                }
            }

            assert_eq!(
                table.has_quorum(&nodes, self_id, now),
                expected,
                "total={total} reachable_peers={reachable_peers} (+self)"
            );
        }
    }

    fn make_node(id: NodeId, priority: u32) -> NodeInfo {
        let mut node = NodeInfo::new_replica(
            id,
            "127.0.0.1:6379".parse().unwrap(),
            "127.0.0.1:16379".parse().unwrap(),
            1,
        );
        node.replica_priority = priority;
        node
    }

    #[test]
    fn test_compute_replica_score_priority_zero_excluded() {
        let node = make_node(10, 0);
        let offsets = vec![(10, 1000u64)];
        assert_eq!(compute_replica_score(&node, 1000, &offsets), u64::MAX);
    }

    #[test]
    fn test_compute_replica_score_lower_priority_is_better() {
        let node_a = make_node(10, 50);
        let node_b = make_node(11, 100);
        let offsets = vec![(10, 1000u64), (11, 1000)];
        let score_a = compute_replica_score(&node_a, 1000, &offsets);
        let score_b = compute_replica_score(&node_b, 1000, &offsets);
        assert!(score_a < score_b, "Lower priority should give lower score");
    }

    #[test]
    fn test_compute_replica_score_higher_offset_is_better() {
        let node_a = make_node(10, 100);
        let node_b = make_node(11, 100);
        let offsets = vec![(10, 900u64), (11, 500)];
        let max_offset = 1000;
        let score_a = compute_replica_score(&node_a, max_offset, &offsets);
        let score_b = compute_replica_score(&node_b, max_offset, &offsets);
        assert!(
            score_a < score_b,
            "Higher offset (less lag) should give lower score"
        );
    }

    #[test]
    fn test_compute_replica_score_equal_scores_tiebreak_by_node_id() {
        let node_a = make_node(10, 100);
        let node_b = make_node(11, 100);
        let offsets = vec![(10, 1000u64), (11, 1000)];
        let score_a = compute_replica_score(&node_a, 1000, &offsets);
        let score_b = compute_replica_score(&node_b, 1000, &offsets);
        assert_eq!(score_a, score_b, "Same priority and offset = same score");
        // Tiebreaker is handled in the min_by comparator, not in compute_replica_score
    }

    #[test]
    fn test_compute_replica_score_formula() {
        let node = make_node(10, 100);
        let offsets = vec![(10, 800u64)];
        let max_offset = 1000;
        let score = compute_replica_score(&node, max_offset, &offsets);
        // priority * 100_000 + lag * 1_000
        // 100 * 100_000 + (1000 - 800) * 1_000 = 10_000_000 + 200_000 = 10_200_000
        assert_eq!(score, 10_200_000);
    }

    #[test]
    fn test_compute_replica_score_no_offset_found() {
        let node = make_node(10, 100);
        let offsets: Vec<(NodeId, u64)> = vec![]; // Node not in offsets list
        let max_offset = 1000;
        let score = compute_replica_score(&node, max_offset, &offsets);
        // offset defaults to 0, so lag = 1000
        // 100 * 100_000 + 1000 * 1_000 = 10_000_000 + 1_000_000 = 11_000_000
        assert_eq!(score, 11_000_000);
    }
}
