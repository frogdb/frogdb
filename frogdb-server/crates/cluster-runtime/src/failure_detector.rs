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
//!
//! Every node probes its peers and keeps a local [`HealthTable`]; the leader
//! *reconciles* that local view into the replicated topology once per check
//! interval. Reconciliation (rather than propagating the threshold-crossing
//! transition inline) is what makes the detector survive the case it exists
//! for: when the leader itself dies, the survivors accumulate their missed
//! probes while they are still followers, so the latch transition happens on a
//! node that cannot write to Raft. A transition-triggered write would be
//! dropped there and never retried — the latch is one-shot — leaving the dead
//! node unflagged forever. A level-triggered reconciliation converges no
//! matter which node was leader when the threshold was crossed.

use frogdb_core::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use frogdb_core::cluster::{
    ClusterCommand, ClusterNetworkFactory, ClusterRaft, ClusterState, NodeId, NodeInfo,
};
use frogdb_core::command::QuorumChecker;
use openraft::ServerState;

use crate::flags::ClusterRuntimeFlags;

/// Startup-fixed timing configuration for failure detection.
///
/// Runtime-mutable cluster decisions (`cluster-auto-failover`,
/// `cluster-replica-priority`) deliberately do **not** live here — they are read
/// from the shared [`ClusterRuntimeFlags`] at decision time so `CONFIG SET`
/// applies without a restart.
#[derive(Debug, Clone)]
pub struct FailureDetectorConfig {
    /// How often to check node health (default: 1000ms).
    pub check_interval_ms: u64,
    /// Timeout for TCP connect (default: 500ms).
    pub connect_timeout_ms: u64,
    /// Number of consecutive failures before marking FAIL (default: 5).
    pub fail_threshold: u32,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 1000,
            connect_timeout_ms: 500,
            fail_threshold: 5,
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
    /// Number of consecutive successes since the last failure. Only meaningful
    /// while `is_marked_fail` is set: it drives the symmetric un-latch.
    success_count: u32,
    /// Whether this node is latched as FAIL locally.
    is_marked_fail: bool,
}

/// This node's local opinion of a peer, as read by the leader when it
/// reconciles the replicated topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalVerdict {
    /// Latched as failed: consecutive failures reached the threshold and the
    /// peer has not yet strung together enough successes to clear the latch.
    Failed,
    /// Probed successfully within the staleness window and not latched.
    Healthy,
    /// Never probed, or last seen too long ago to claim either way. The leader
    /// leaves the replicated flags alone.
    Unknown,
}

/// What the leader decided to do about one peer in [`FailureDetector::reconcile_topology`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileAction {
    /// Local view says failed, replicated topology still says healthy.
    MarkFailed,
    /// Local view says healthy, replicated topology still says failed.
    MarkRecovered,
}

/// Holds a peer's slot in [`FailureDetector::inflight`] for the lifetime of one
/// spawned reconciliation.
///
/// A `Drop` impl rather than a trailing `remove`, because a panic anywhere in
/// the write path would otherwise leave the slot occupied for the life of the
/// process — and a peer whose slot is stuck is a peer whose FAIL flag is never
/// set or cleared again, silently.
struct InflightGuard {
    detector: Arc<FailureDetector>,
    node_id: NodeId,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        // This can run while unwinding, where a second panic aborts the
        // process; a poisoned lock still guards a consistent `HashSet`, so
        // recover from it rather than `unwrap()`.
        self.detector
            .inflight
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.node_id);
    }
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
    /// Resets the failure run and refreshes the last-seen timestamp. A latched
    /// node is *not* un-latched by a single success: clearing the latch takes
    /// `fail_threshold` consecutive successes, mirroring the number of
    /// consecutive failures it took to set it. Without that symmetry a peer
    /// that answers one TCP connect in five keeps clearing its FAIL flag, and
    /// every flap costs a `MarkNodeRecovered` / `MarkNodeFailed` pair — two
    /// Raft writes and a config-epoch bump each round.
    fn record_success(&mut self, node_id: NodeId, now: Instant) {
        let entry = self.health.entry(node_id).or_default();
        entry.last_seen = Some(now);
        entry.failure_count = 0;
        if entry.is_marked_fail {
            entry.success_count += 1;
            if entry.success_count >= self.fail_threshold {
                entry.is_marked_fail = false;
                entry.success_count = 0;
            }
        }
    }

    /// Record a failed health check for `node_id`.
    ///
    /// Increments the consecutive-failure count and latches the node as failed
    /// once the count reaches `fail_threshold`. Also resets any in-progress
    /// recovery run, so the successes that clear the latch must be consecutive.
    fn record_failure(&mut self, node_id: NodeId) {
        let entry = self.health.entry(node_id).or_default();
        entry.failure_count += 1;
        entry.success_count = 0;

        if entry.failure_count >= self.fail_threshold {
            entry.is_marked_fail = true;
        }
    }

    /// How long a node may go unprobed before its last success stops counting.
    fn stale_threshold(&self) -> Duration {
        self.check_interval * (self.fail_threshold + 2)
    }

    /// This node's local opinion of `node_id` at time `now`.
    ///
    /// Level-triggered: [`Self::record_failure`] / [`Self::record_success`]
    /// only move the state, they report nothing. Every decision is taken from
    /// the current state here, so a caller that was not the leader when the
    /// latch flipped can still act on it.
    fn verdict(&self, node_id: NodeId, now: Instant) -> LocalVerdict {
        let Some(entry) = self.health.get(&node_id) else {
            return LocalVerdict::Unknown;
        };
        if entry.is_marked_fail {
            return LocalVerdict::Failed;
        }
        match entry.last_seen {
            Some(seen) if now.saturating_duration_since(seen) < self.stale_threshold() => {
                LocalVerdict::Healthy
            }
            _ => LocalVerdict::Unknown,
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
        let stale_threshold = self.stale_threshold();

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

/// Cluster failure detector.
///
/// Every node probes its peers (TCP connect to `cluster_addr`) and keeps the
/// result in a local [`HealthTable`]; only the leader turns that local view
/// into `MarkNodeFailed` / `MarkNodeRecovered` Raft writes, via
/// [`Self::reconcile_topology`].
pub struct FailureDetector {
    /// This node's ID.
    self_node_id: NodeId,
    /// Startup-fixed timing configuration for failure detection.
    config: FailureDetectorConfig,
    /// Runtime-mutable cluster decision flags (auto-failover, replica priority).
    /// Read at decision time, never snapshotted.
    flags: Arc<ClusterRuntimeFlags>,
    /// Pure per-node health state machine (thresholds, latching, staleness).
    health: RwLock<HealthTable>,
    /// Cluster state for reading node information.
    cluster_state: Arc<ClusterState>,
    /// Raft instance for writing failure/recovery commands.
    raft: Arc<ClusterRaft>,
    /// Network factory for pooled connections to peers.
    network_factory: Arc<ClusterNetworkFactory>,
    /// Peers with a reconciliation write already in flight. Reconciliation is
    /// level-triggered and runs every tick, so without this a Raft write that
    /// is slow (or wedged behind an election) would be re-issued once per
    /// second forever.
    inflight: RwLock<HashSet<NodeId>>,
}

impl FailureDetector {
    /// Create a new failure detector.
    pub fn new(
        self_node_id: NodeId,
        config: FailureDetectorConfig,
        flags: Arc<ClusterRuntimeFlags>,
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
            flags,
            health: RwLock::new(health),
            cluster_state,
            raft,
            network_factory,
            inflight: RwLock::new(HashSet::new()),
        }
    }

    /// How long a single reconciliation Raft write may run before it is
    /// abandoned. Five check intervals (at least a second) is long enough to
    /// ride out an election, short enough that a wedged write does not pin the
    /// peer's in-flight slot — and with it the FAIL flag — indefinitely.
    fn raft_write_timeout(&self) -> Duration {
        Duration::from_millis(self.config.check_interval_ms.saturating_mul(5))
            .max(Duration::from_secs(1))
    }

    /// Check if this node is currently the Raft leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.state == ServerState::Leader
    }

    /// Record a successful connection to a node (local tracking only).
    ///
    /// Propagation to the replicated topology is the leader's job and happens
    /// in [`Self::reconcile_topology`], not here.
    pub fn record_success_local(&self, node_id: NodeId) {
        self.health
            .write()
            .unwrap()
            .record_success(node_id, Instant::now());
    }

    /// Record a failed connection attempt to a node (local tracking only).
    ///
    /// See [`Self::record_success_local`] for why this does not write to Raft.
    pub fn record_failure_local(&self, node_id: NodeId) {
        self.health.write().unwrap().record_failure(node_id);
    }

    /// Bring the replicated topology in line with this node's local health
    /// view. Leader-only: every write goes through Raft.
    ///
    /// Called once per check interval rather than on the latch transition, so
    /// a node that crossed the failure threshold while it was still a follower
    /// (the normal case when the *leader* is the node that died) still flags
    /// the peer once it wins the election. Writes are gated on the replicated
    /// flag already disagreeing, so a converged cluster performs no writes and
    /// the config epoch does not churn.
    ///
    /// Decides on the caller's task, writes off it: each divergent peer gets a
    /// spawned task, so a slow Raft write (or the auto-failover probes that
    /// follow it) cannot stall the probe cadence of every other peer. The
    /// [`Self::inflight`] set keeps the next tick from re-issuing a write that
    /// has not finished yet.
    pub fn reconcile_topology(self: &Arc<Self>) {
        let now = Instant::now();
        let verdicts: Vec<(NodeId, LocalVerdict, bool)> = {
            let health = self.health.read().unwrap();
            self.cluster_state
                .get_all_nodes()
                .into_iter()
                .filter(|node| node.id != self.self_node_id)
                .map(|node| (node.id, health.verdict(node.id, now), node.flags.fail))
                .collect()
        };

        for (node_id, verdict, marked_failed) in verdicts {
            match verdict {
                LocalVerdict::Failed if !marked_failed => {
                    self.spawn_reconcile(node_id, ReconcileAction::MarkFailed)
                }
                LocalVerdict::Healthy if marked_failed => {
                    self.spawn_reconcile(node_id, ReconcileAction::MarkRecovered)
                }
                _ => {}
            }
        }
    }

    /// Run one reconciliation write for `node_id` on its own task, unless one
    /// is already in flight for that peer.
    fn spawn_reconcile(self: &Arc<Self>, node_id: NodeId, action: ReconcileAction) {
        if !self.inflight.write().unwrap().insert(node_id) {
            tracing::debug!(
                node_id,
                ?action,
                "Reconciliation already in flight; skipping"
            );
            return;
        }

        let guard = InflightGuard {
            detector: Arc::clone(self),
            node_id,
        };
        tokio::spawn(async move {
            match action {
                ReconcileAction::MarkFailed => guard.detector.mark_node_failed(node_id).await,
                ReconcileAction::MarkRecovered => guard.detector.mark_node_recovered(node_id).await,
            }
        });
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
        let write = tokio::time::timeout(self.raft_write_timeout(), self.raft.client_write(cmd));
        match write.await {
            Err(_) => {
                tracing::warn!(
                    node_id,
                    timeout_ms = self.raft_write_timeout().as_millis(),
                    "MarkNodeFailed timed out; retrying on a later reconciliation"
                );
            }
            Ok(Ok(resp)) => {
                if let frogdb_core::cluster::ClusterResponse::Error(msg) = &resp.data {
                    tracing::warn!(node_id, error = %msg, "MarkNodeFailed rejected by state machine");
                    return;
                }
                tracing::warn!(node_id, "Marked node as FAIL via Raft");

                // Trigger automatic failover if enabled. Read live at the
                // decision point so `CONFIG SET cluster-auto-failover` arms or
                // disarms promotion for the very next node-FAIL latch.
                if self.flags.auto_failover() {
                    self.trigger_auto_failover(node_id).await;
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(node_id, error = %e, "Failed to mark node as failed");
            }
        }
    }

    /// Mark a node as recovered via Raft consensus.
    async fn mark_node_recovered(&self, node_id: NodeId) {
        let cmd = ClusterCommand::MarkNodeRecovered { node_id };
        let write = tokio::time::timeout(self.raft_write_timeout(), self.raft.client_write(cmd));
        match write.await {
            Err(_) => {
                tracing::warn!(
                    node_id,
                    timeout_ms = self.raft_write_timeout().as_millis(),
                    "MarkNodeRecovered timed out; retrying on a later reconciliation"
                );
            }
            Ok(Ok(_)) => {
                tracing::info!(node_id, "Marked node as recovered via Raft");
            }
            Ok(Err(e)) => {
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

        // Score and select best replica (lowest score wins). Priorities are
        // resolved through `effective_priority`, so this node's own candidacy
        // follows the live `cluster-replica-priority` atomic rather than the
        // value it published at boot.
        let new_primary = match select_failover_target(&replicas, &replica_offsets, &|n| {
            self.effective_priority(n)
        }) {
            Some(node) => node,
            None => {
                tracing::warn!(
                    node_id = failed_node_id,
                    "Primary failed but all replicas have priority 0 (never-promote)"
                );
                return;
            }
        };

        let replica_offset = offset_of(new_primary.id, &replica_offsets);

        tracing::info!(
            failed_primary = failed_node_id,
            new_primary = new_primary.id,
            replica_offset = replica_offset,
            max_offset = max_offset,
            score = compute_replica_score(
                self.effective_priority(new_primary),
                replica_offset,
                max_offset
            ),
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
            let write = tokio::time::timeout(
                self.raft_write_timeout(),
                self.raft.client_write(cmd.clone()),
            );
            match write.await {
                Err(_) => {
                    tracing::warn!(
                        failed_primary = failed_node_id,
                        new_primary = new_primary.id,
                        attempt,
                        timeout_ms = self.raft_write_timeout().as_millis(),
                        "Auto-failover Raft write timed out"
                    );
                }
                Ok(Ok(resp)) => {
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
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, attempt, "Auto-failover Raft write failed");
                }
            }
            if attempt < MAX_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        tracing::error!(
            failed_primary = failed_node_id,
            new_primary = new_primary.id,
            "Auto-failover failed after {MAX_ATTEMPTS} attempts"
        );
    }

    /// Get the startup timing configuration.
    pub fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }

    /// Shared runtime cluster flags this detector reads its decisions from.
    pub fn flags(&self) -> &Arc<ClusterRuntimeFlags> {
        &self.flags
    }

    /// Effective promotion priority for `node`, resolved at selection time.
    ///
    /// This node's entry follows the live `cluster-replica-priority` atomic so a
    /// runtime `CONFIG SET` steers the very next promotion decision made here.
    /// Peers are scored with the value they published through Raft — the only
    /// value this node can know for them.
    fn effective_priority(&self, node: &NodeInfo) -> u32 {
        if node.id == self.self_node_id {
            self.flags.replica_priority()
        } else {
            node.replica_priority
        }
    }
}

/// Replication offset recorded for `node_id`; unqueried nodes score as 0 (worst).
fn offset_of(node_id: NodeId, offsets: &[(NodeId, u64)]) -> u64 {
    offsets
        .iter()
        .find(|(id, _)| *id == node_id)
        .map(|(_, o)| *o)
        .unwrap_or(0)
}

/// Compute a failover score for a replica. Lower score = better candidate.
///
/// Formula from the spec (docs/spec/CLUSTER.md lines 1733-1755):
/// ```text
/// score = (priority == 0 ? u64::MAX : priority * 100_000)
///       + (max_offset - replica_offset) * 1_000
/// ```
///
/// Priority 0 means never promote (returns u64::MAX). The priority is passed in
/// rather than read off `NodeInfo` so the caller can substitute this node's live
/// `cluster-replica-priority` for its own stale published copy.
fn compute_replica_score(priority: u32, replica_offset: u64, max_offset: u64) -> u64 {
    if priority == 0 {
        return u64::MAX;
    }
    (priority as u64) * 100_000 + max_offset.saturating_sub(replica_offset) * 1_000
}

/// Pick the promotion target among `replicas`: lowest score wins, ties broken by
/// node id for determinism. Never-promote candidates (effective priority 0) are
/// excluded outright.
///
/// `priority_of` resolves each candidate's *effective* priority, so a caller can
/// override its own entry with a live config value. Pure: no Raft, no clock, no
/// sockets — the whole selection policy is unit-testable.
fn select_failover_target<'a>(
    replicas: &[&'a NodeInfo],
    offsets: &[(NodeId, u64)],
    priority_of: &dyn Fn(&NodeInfo) -> u32,
) -> Option<&'a NodeInfo> {
    let max_offset = offsets.iter().map(|(_, o)| *o).max().unwrap_or(0);
    replicas
        .iter()
        .copied()
        .filter(|r| priority_of(r) != 0)
        .min_by(|a, b| {
            let score_a =
                compute_replica_score(priority_of(a), offset_of(a.id, offsets), max_offset);
            let score_b =
                compute_replica_score(priority_of(b), offset_of(b.id, offsets), max_offset);
            score_a.cmp(&score_b).then_with(|| a.id.cmp(&b.id))
        })
}

impl QuorumChecker for FailureDetector {
    fn has_quorum(&self) -> bool {
        FailureDetector::has_quorum(self)
    }
}

/// Check if a node is reachable via TCP connect to its cluster_addr.
///
/// Dials through [`frogdb_net::TcpStream`] so the probe is routed over turmoil's
/// simulated network under the `turmoil` feature (tokio's real TCP driver is
/// disabled inside a simulation) and over tokio in production.
async fn check_node_reachable(addr: SocketAddr, timeout: Duration) -> bool {
    use tokio::time::timeout as tokio_timeout;
    tokio_timeout(timeout, frogdb_net::TcpStream::connect(addr))
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
        // A stalled tick (runtime pause, host suspend) must not fire a burst of
        // catch-up ticks: each one would probe every peer and count a failure
        // per missed tick, latching FAIL on peers that were never down.
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval_timer.tick().await;

            // All nodes track reachability for local quorum detection, but
            // only the leader writes to Raft. Reconcile before probing so the
            // verdicts acted on are the ones the previous round settled. The
            // writes it decides on run on their own tasks, so this loop keeps
            // its cadence regardless of Raft latency.
            if detector.is_leader() {
                detector.reconcile_topology();
            }

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
                        detector.record_success_local(node_id);
                    } else {
                        tracing::debug!(node_id, %addr, "Node unreachable");
                        detector.record_failure_local(node_id);
                    }
                });
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // FM-CLUSTER-054
    #[test]
    fn test_failure_detector_config_default() {
        let config = FailureDetectorConfig::default();
        assert_eq!(config.check_interval_ms, 1000);
        assert_eq!(config.connect_timeout_ms, 500);
        assert_eq!(config.fail_threshold, 5);
    }

    // FM-CLUSTER-052
    #[test]
    fn test_node_health_default() {
        let health = NodeHealth::default();
        assert_eq!(health.failure_count, 0);
        assert!(!health.is_marked_fail);
    }

    // ---- HealthTable: pure state-machine tests (no Raft, no network, no clock) ----

    /// N-1 consecutive failures must NOT latch; the Nth latches; clearing the
    /// latch takes the same N consecutive successes; then it takes N failures
    /// again to re-latch.
    // FM-CLUSTER-052
    #[test]
    fn test_health_table_threshold_latching_is_symmetric() {
        let now = Instant::now();
        for threshold in [1u32, 2, 5] {
            let mut table = HealthTable::new(threshold, Duration::from_millis(1000));
            let node: NodeId = 42;

            // First threshold-1 failures do not latch.
            for i in 1..threshold {
                table.record_failure(node);
                assert_ne!(
                    table.verdict(node, now),
                    LocalVerdict::Failed,
                    "failure #{i} (threshold {threshold}) must not latch"
                );
            }

            // The threshold-th failure latches, and it stays latched.
            table.record_failure(node);
            assert_eq!(
                table.verdict(node, now),
                LocalVerdict::Failed,
                "failure #{threshold} (threshold {threshold}) must latch"
            );
            table.record_failure(node);
            assert_eq!(table.verdict(node, now), LocalVerdict::Failed);

            // Symmetric hysteresis: threshold-1 successes must NOT clear it.
            for i in 1..threshold {
                table.record_success(node, now);
                assert_eq!(
                    table.verdict(node, now),
                    LocalVerdict::Failed,
                    "success #{i} (threshold {threshold}) must not clear the latch"
                );
            }
            table.record_success(node, now);
            assert_eq!(
                table.verdict(node, now),
                LocalVerdict::Healthy,
                "success #{threshold} (threshold {threshold}) must clear the latch"
            );

            // After recovery it takes a full `threshold` run again to re-latch.
            for i in 1..threshold {
                table.record_failure(node);
                assert_ne!(
                    table.verdict(node, now),
                    LocalVerdict::Failed,
                    "post-recovery failure #{i} (threshold {threshold}) must not latch"
                );
            }
            table.record_failure(node);
            assert_eq!(table.verdict(node, now), LocalVerdict::Failed);
        }
    }

    /// The recovery run must be *consecutive*: one failure in the middle of it
    /// resets the count, so a peer that answers one probe in two never clears
    /// its FAIL flag (and never churns the config epoch flipping it).
    // FM-CLUSTER-052
    #[test]
    fn test_health_table_latch_survives_flapping_recovery() {
        let now = Instant::now();
        let mut table = HealthTable::new(3, Duration::from_millis(1000));
        let node: NodeId = 7;

        for _ in 0..3 {
            table.record_failure(node);
        }
        assert_eq!(table.verdict(node, now), LocalVerdict::Failed);

        // Alternating success/failure never accumulates 3 in a row.
        for _ in 0..10 {
            table.record_success(node, now);
            table.record_failure(node);
            assert_eq!(
                table.verdict(node, now),
                LocalVerdict::Failed,
                "flapping peer must stay latched"
            );
        }

        // A sustained run of successes finally clears it.
        table.record_success(node, now);
        table.record_success(node, now);
        assert_eq!(table.verdict(node, now), LocalVerdict::Failed);
        table.record_success(node, now);
        assert_eq!(table.verdict(node, now), LocalVerdict::Healthy);
    }

    /// The level-triggered verdict outlives the one-shot latch transition.
    ///
    /// This is the property the leader's reconciliation depends on: a node
    /// that latched while this process was a follower (so nobody who could
    /// write to Raft saw the threshold crossing) must still report `Failed` on
    /// every later poll.
    // FM-CLUSTER-053
    #[test]
    fn test_health_table_verdict_is_level_triggered() {
        let t0 = Instant::now();
        let check_interval = Duration::from_millis(100);
        let mut table = HealthTable::new(2, check_interval);
        let node: NodeId = 42;

        // Never probed: no opinion, so the leader must not touch the flags.
        assert_eq!(table.verdict(node, t0), LocalVerdict::Unknown);

        table.record_success(node, t0);
        assert_eq!(table.verdict(node, t0), LocalVerdict::Healthy);

        // Sub-threshold failures do not change the verdict: the node was seen
        // recently and is not latched.
        table.record_failure(node);
        assert_eq!(table.verdict(node, t0), LocalVerdict::Healthy);

        // Latching flips the verdict, and it keeps reporting `Failed` on every
        // subsequent poll.
        table.record_failure(node);
        assert_eq!(table.verdict(node, t0), LocalVerdict::Failed);
        table.record_failure(node);
        assert_eq!(table.verdict(node, t0), LocalVerdict::Failed);

        // A latched node stays `Failed` however long it has been unreachable
        // (staleness must not downgrade it to `Unknown` and un-flag it).
        assert_eq!(
            table.verdict(node, t0 + Duration::from_secs(3600)),
            LocalVerdict::Failed
        );

        // Recovery reports `Healthy` again, after a full run of successes ...
        table.record_success(node, t0);
        table.record_success(node, t0);
        assert_eq!(table.verdict(node, t0), LocalVerdict::Healthy);

        // ... but only while the success is fresh: past the staleness window
        // this node has no basis for claiming the peer is up, so it reports
        // `Unknown` and leaves the replicated flags alone.
        let stale = check_interval * (2 + 2);
        assert_eq!(table.verdict(node, t0 + stale), LocalVerdict::Unknown);
    }

    /// A single success resets the failure run of a node that never latched —
    /// the hysteresis only applies in the un-latch direction.
    // FM-CLUSTER-052
    #[test]
    fn test_health_table_success_resets_failure_run() {
        let now = Instant::now();
        let mut table = HealthTable::new(3, Duration::from_millis(1000));
        table.record_success(7, now);
        assert_eq!(table.verdict(7, now), LocalVerdict::Healthy);

        // Two sub-threshold failures, then one success: still healthy.
        table.record_failure(7);
        table.record_failure(7);
        table.record_success(7, now);
        assert_eq!(table.verdict(7, now), LocalVerdict::Healthy);

        // Counter reset: two fresh failures still do not latch (threshold 3).
        table.record_failure(7);
        table.record_failure(7);
        assert_ne!(table.verdict(7, now), LocalVerdict::Failed);
        table.record_failure(7);
        assert_eq!(table.verdict(7, now), LocalVerdict::Failed);
    }

    /// Staleness window boundary: reachable strictly while
    /// `now - last_seen < check_interval * (fail_threshold + 2)`.
    // FM-CLUSTER-054
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
    // FM-CLUSTER-055
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
    // FM-CLUSTER-055
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

    /// Score a node by its published priority — the pre-seam behavior these
    /// formula tests pin.
    fn score_of(node: &NodeInfo, max_offset: u64, offsets: &[(NodeId, u64)]) -> u64 {
        compute_replica_score(
            node.replica_priority,
            offset_of(node.id, offsets),
            max_offset,
        )
    }

    // FM-CLUSTER-057
    #[test]
    fn test_compute_replica_score_priority_zero_excluded() {
        let node = make_node(10, 0);
        let offsets = vec![(10, 1000u64)];
        assert_eq!(score_of(&node, 1000, &offsets), u64::MAX);
    }

    // FM-CLUSTER-056
    #[test]
    fn test_compute_replica_score_lower_priority_is_better() {
        let node_a = make_node(10, 50);
        let node_b = make_node(11, 100);
        let offsets = vec![(10, 1000u64), (11, 1000)];
        let score_a = score_of(&node_a, 1000, &offsets);
        let score_b = score_of(&node_b, 1000, &offsets);
        assert!(score_a < score_b, "Lower priority should give lower score");
    }

    // FM-CLUSTER-056
    #[test]
    fn test_compute_replica_score_higher_offset_is_better() {
        let node_a = make_node(10, 100);
        let node_b = make_node(11, 100);
        let offsets = vec![(10, 900u64), (11, 500)];
        let max_offset = 1000;
        let score_a = score_of(&node_a, max_offset, &offsets);
        let score_b = score_of(&node_b, max_offset, &offsets);
        assert!(
            score_a < score_b,
            "Higher offset (less lag) should give lower score"
        );
    }

    // FM-CLUSTER-058
    #[test]
    fn test_compute_replica_score_equal_scores_tiebreak_by_node_id() {
        let node_a = make_node(10, 100);
        let node_b = make_node(11, 100);
        let offsets = vec![(10, 1000u64), (11, 1000)];
        let score_a = score_of(&node_a, 1000, &offsets);
        let score_b = score_of(&node_b, 1000, &offsets);
        assert_eq!(score_a, score_b, "Same priority and offset = same score");
        // Tiebreaker is handled in the min_by comparator, not in compute_replica_score
    }

    // FM-CLUSTER-056
    #[test]
    fn test_compute_replica_score_formula() {
        let node = make_node(10, 100);
        let offsets = vec![(10, 800u64)];
        let max_offset = 1000;
        let score = score_of(&node, max_offset, &offsets);
        // priority * 100_000 + lag * 1_000
        // 100 * 100_000 + (1000 - 800) * 1_000 = 10_000_000 + 200_000 = 10_200_000
        assert_eq!(score, 10_200_000);
    }

    // FM-CLUSTER-056
    #[test]
    fn test_compute_replica_score_no_offset_found() {
        let node = make_node(10, 100);
        let offsets: Vec<(NodeId, u64)> = vec![]; // Node not in offsets list
        let max_offset = 1000;
        let score = score_of(&node, max_offset, &offsets);
        // offset defaults to 0, so lag = 1000
        // 100 * 100_000 + 1000 * 1_000 = 10_000_000 + 1_000_000 = 11_000_000
        assert_eq!(score, 11_000_000);
    }

    // ---- Live-config seams -------------------------------------------------

    /// `cluster-replica-priority` is read at *selection* time through
    /// `priority_of`, so storing a new value re-decides the promotion target
    /// with no restart and no re-published `NodeInfo`.
    // FM-CLUSTER-058
    #[test]
    fn test_replica_priority_store_changes_failover_target() {
        let self_id: NodeId = 10;
        let peer = make_node(11, 100);
        let me = make_node(self_id, 100);
        let replicas = vec![&me, &peer];
        // Identical offsets, so priority alone decides; id 10 wins the tiebreak.
        let offsets = vec![(10, 1000u64), (11, 1000)];

        let flags = ClusterRuntimeFlags::new(true, true, 100);
        let priority_of = |n: &NodeInfo| {
            if n.id == self_id {
                flags.replica_priority()
            } else {
                n.replica_priority
            }
        };

        // Equal priorities: deterministic tiebreak picks the lower id (self).
        assert_eq!(
            select_failover_target(&replicas, &offsets, &priority_of).map(|n| n.id),
            Some(self_id)
        );

        // Demote self at runtime: the peer now wins without anything restarting.
        flags.set_replica_priority(200);
        assert_eq!(
            select_failover_target(&replicas, &offsets, &priority_of).map(|n| n.id),
            Some(11)
        );

        // Never-promote at runtime: self drops out of the candidate set entirely.
        flags.set_replica_priority(0);
        assert_eq!(
            select_failover_target(&replicas, &offsets, &priority_of).map(|n| n.id),
            Some(11)
        );

        // Promote self back: it wins outright.
        flags.set_replica_priority(1);
        assert_eq!(
            select_failover_target(&replicas, &offsets, &priority_of).map(|n| n.id),
            Some(self_id)
        );
    }

    /// Every candidate at effective priority 0 => no promotion target at all.
    // FM-CLUSTER-057
    #[test]
    fn test_select_failover_target_all_never_promote() {
        let a = make_node(10, 0);
        let b = make_node(11, 0);
        let replicas = vec![&a, &b];
        let offsets = vec![(10, 1u64), (11, 2)];
        assert!(select_failover_target(&replicas, &offsets, &|n| n.replica_priority).is_none());
    }
}
