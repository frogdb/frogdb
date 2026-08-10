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

use frogdb_core::clock;
use frogdb_core::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use frogdb_core::cluster::{
    ClusterCommand, ClusterNetworkFactory, ClusterRaft, ClusterResponse, ClusterState, NodeId,
    NodeInfo, RaftProposer, VoterChange, voter_change,
};
use frogdb_core::command::QuorumChecker;
use openraft::ServerState;

use crate::flags::ClusterRuntimeFlags;

/// The consensus surface the failure detector actually uses, as a seam.
///
/// Two things only: propose a topology command (inherited from
/// [`RaftProposer`], whose blanket impl for `Arc<ClusterRaft>` the detector
/// reuses verbatim) and read this node's Raft role. Naming them as a trait is
/// what makes the detector testable at all — an `openraft::Raft` needs storage,
/// a network factory and a won election before it can answer either question,
/// none of which a unit test can stand up.
pub trait DetectorRaft: RaftProposer + 'static {
    /// This node's current Raft server state, read at the moment of the call.
    fn server_state(&self) -> ServerState;

    /// Apply the Raft voter-set change a command this detector just committed
    /// owes (FM-CLUSTER-101).
    ///
    /// Part of the seam because the detector is the *third* commit site for a
    /// node removal — the other two are in the connection layer and in the
    /// leader-side `ForwardedWrite` receiver — and it is the only one that
    /// cannot reach a real `ClusterRaft` from here.
    fn spawn_voter_change(&self, change: VoterChange);
}

impl DetectorRaft for Arc<ClusterRaft> {
    // DOCUMENTED EQUIVALENT (mutation testing): `server_state -> Default::default()`
    // (`ServerState::Learner`) survives and cannot be killed from this crate.
    // The body is a single field read off a live `openraft::Raft`'s metrics
    // watch, and constructing one requires storage plus a real election, so no
    // test here can ever observe a state other than the default. Everything
    // this crate *decides* from the value — the `== ServerState::Leader`
    // comparison — lives in `FailureDetector::is_leader`, which is forced.
    fn server_state(&self) -> ServerState {
        self.metrics().borrow().state
    }

    fn spawn_voter_change(&self, change: VoterChange) {
        frogdb_core::cluster::spawn_voter_change((**self).clone(), change);
    }
}

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

/// Lower bound for `check_interval_ms`. A zero-length `tokio::time::interval`
/// panics on its very first tick (`spawn_failure_detector_task`), and
/// `raft_write_timeout`'s `saturating_mul(5)` assumes a nonzero cadence.
pub const MIN_CHECK_INTERVAL_MS: u64 = 1;
/// Upper bound for `check_interval_ms`. `HealthTable::stale_threshold`
/// multiplies this by `fail_threshold + 2`; capping both factors keeps that
/// multiplication well inside `Duration`'s representable range regardless of
/// how the other factor is configured.
pub const MAX_CHECK_INTERVAL_MS: u64 = 24 * 60 * 60 * 1000; // 24h

/// Lower bound for `connect_timeout_ms`. A zero timeout makes
/// `tokio::time::timeout` treat every probe as already elapsed, so every
/// peer — reachable or not — reads as unreachable on the very first tick,
/// producing mass false failures instead of real detection.
pub const MIN_CONNECT_TIMEOUT_MS: u64 = 1;
/// Upper bound for `connect_timeout_ms`, matching `MAX_CHECK_INTERVAL_MS`.
pub const MAX_CONNECT_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000; // 24h

/// Lower bound for `fail_threshold`. Zero would latch FAIL on the very first
/// failure and clear it on the very first success (`HealthTable::
/// record_failure`/`record_success`, both `>=` comparisons against the
/// threshold), discarding the hysteresis the field exists to provide.
pub const MIN_FAIL_THRESHOLD: u32 = 1;
/// Upper bound for `fail_threshold`. `HealthTable::stale_threshold` computes
/// `check_interval * (fail_threshold + 2)`, and `Duration`'s `Mul<u32>`
/// panics on overflow; this cap keeps that product in range even at
/// `MAX_CHECK_INTERVAL_MS`.
pub const MAX_FAIL_THRESHOLD: u32 = 100_000;

impl FailureDetectorConfig {
    /// Clamp every field into the range the rest of this module assumes it
    /// is in, pulling out-of-range values to the nearest bound rather than
    /// rejecting them. See FM-CLUSTER-102.
    ///
    /// This is a bare timing knob with no cross-field relationship to
    /// enforce (unlike, say, `heartbeat_interval_ms < election_timeout_ms`
    /// in `ClusterConfigSection::validate()`, which rejects at config-parse
    /// time because an ordering violation has no sane default to fall back
    /// to). A single out-of-range field here has an obvious, harmless
    /// nearest-bound reading, so silently sanitizing it at construction
    /// matches this codebase's precedent for that shape of value — e.g.
    /// `tls_watch.rs` floors `watch_debounce_ms` with
    /// `Duration::from_millis(x).max(MIN_POLL_INTERVAL)` rather than
    /// refusing to start.
    fn clamped(self) -> Self {
        Self {
            check_interval_ms: self
                .check_interval_ms
                .clamp(MIN_CHECK_INTERVAL_MS, MAX_CHECK_INTERVAL_MS),
            connect_timeout_ms: self
                .connect_timeout_ms
                .clamp(MIN_CONNECT_TIMEOUT_MS, MAX_CONNECT_TIMEOUT_MS),
            fail_threshold: self
                .fail_threshold
                .clamp(MIN_FAIL_THRESHOLD, MAX_FAIL_THRESHOLD),
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
struct InflightGuard<R> {
    detector: Arc<FailureDetector<R>>,
    node_id: NodeId,
}

impl<R> Drop for InflightGuard<R> {
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
///
/// Generic over the [`DetectorRaft`] seam; production uses the default
/// (`Arc<ClusterRaft>`), so no call site names the parameter.
pub struct FailureDetector<R = Arc<ClusterRaft>> {
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
    raft: R,
    /// Network factory for pooled connections to peers.
    network_factory: Arc<ClusterNetworkFactory>,
    /// Peers with a reconciliation write already in flight. Reconciliation is
    /// level-triggered and runs every tick, so without this a Raft write that
    /// is slow (or wedged behind an election) would be re-issued once per
    /// second forever.
    inflight: RwLock<HashSet<NodeId>>,
}

impl<R: DetectorRaft> FailureDetector<R> {
    /// Create a new failure detector.
    ///
    /// `config` is clamped to safe bounds before it is stored (FM-CLUSTER-102):
    /// every later read of `self.config` — including the accessor — sees the
    /// sanitized values, never the ones the caller passed in.
    pub fn new(
        self_node_id: NodeId,
        config: FailureDetectorConfig,
        flags: Arc<ClusterRuntimeFlags>,
        cluster_state: Arc<ClusterState>,
        raft: R,
        network_factory: Arc<ClusterNetworkFactory>,
    ) -> Self {
        let config = config.clamped();
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
        self.raft.server_state() == ServerState::Leader
    }

    /// Record a successful connection to a node (local tracking only).
    ///
    /// Propagation to the replicated topology is the leader's job and happens
    /// in [`Self::reconcile_topology`], not here.
    pub fn record_success_local(&self, node_id: NodeId) {
        self.health
            .write()
            .unwrap()
            .record_success(node_id, clock::now());
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
        let now = clock::now();
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
            .has_quorum(&all_nodes, self.self_node_id, clock::now())
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
                if let ClusterResponse::Error(msg) = &resp {
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
                    if let ClusterResponse::Error(msg) = &resp {
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
                    // A forced failover removes the old primary from the
                    // topology, so it owes the same voter-set shrink
                    // `CLUSTER FORGET` does — otherwise the node auto-failover
                    // exists to evict stays in every quorum computation
                    // (FM-CLUSTER-101). Derived from the command through the
                    // shared rule so this site cannot drift from the other two.
                    if let Some(change) = voter_change(&cmd) {
                        self.raft.spawn_voter_change(change);
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

impl<R: DetectorRaft> QuorumChecker for FailureDetector<R> {
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
pub fn spawn_failure_detector_task<R: DetectorRaft>(
    detector: Arc<FailureDetector<R>>,
) -> tokio::task::JoinHandle<()> {
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

    /// Zero is the classic misconfiguration for all three fields; the
    /// constructor must clamp every one of them to its documented minimum
    /// rather than storing the degenerate value, and latching must still
    /// work — at the clamped threshold of 1 — once it does.
    // FM-CLUSTER-102
    #[tokio::test]
    async fn degenerate_zero_config_is_clamped_to_safe_minimums() {
        let f = build(
            1,
            vec![primary_node(1), primary_node(2)],
            FailureDetectorConfig {
                check_interval_ms: 0,
                connect_timeout_ms: 0,
                fail_threshold: 0,
            },
            ClusterRuntimeFlags::default(),
            network_reporting(&[]),
        );

        assert_eq!(f.detector.config().check_interval_ms, MIN_CHECK_INTERVAL_MS);
        assert_eq!(
            f.detector.config().connect_timeout_ms,
            MIN_CONNECT_TIMEOUT_MS
        );
        assert_eq!(f.detector.config().fail_threshold, MIN_FAIL_THRESHOLD);

        // Latching still works at the clamped threshold of 1: one failure
        // latches, one success clears it.
        let verdict = || f.detector.health.read().unwrap().verdict(2, Instant::now());
        f.detector.record_failure_local(2);
        assert_eq!(verdict(), LocalVerdict::Failed);
        f.detector.record_success_local(2);
        assert_eq!(verdict(), LocalVerdict::Healthy);

        // has_quorum exercises HealthTable::stale_threshold(); must not panic.
        assert!(
            f.detector.has_quorum(),
            "self plus the now-healthy peer is 2 of 2"
        );
    }

    /// `u64::MAX`/`u32::MAX` are the classic misconfiguration for an upper
    /// bound: uncapped, `HealthTable::stale_threshold`'s `check_interval *
    /// (fail_threshold + 2)` would overflow `Duration`'s `Mul<u32>` and
    /// panic. The clamp caps both factors before the health table is ever
    /// built.
    // FM-CLUSTER-102
    #[tokio::test]
    async fn huge_config_values_are_clamped_and_do_not_overflow_the_staleness_multiply() {
        let f = build(
            1,
            vec![primary_node(1), primary_node(2)],
            FailureDetectorConfig {
                check_interval_ms: u64::MAX,
                connect_timeout_ms: u64::MAX,
                fail_threshold: u32::MAX,
            },
            ClusterRuntimeFlags::default(),
            network_reporting(&[]),
        );

        // Pinned as literals as well as by name: comparing the clamped config
        // against the constant moves both sides together, so an arithmetic
        // slip in the `24 * 60 * 60 * 1000` that defines the bound would go
        // unnoticed.
        assert_eq!(MAX_CHECK_INTERVAL_MS, 86_400_000, "24h in ms");
        assert_eq!(MAX_CONNECT_TIMEOUT_MS, 86_400_000, "24h in ms");

        assert_eq!(f.detector.config().check_interval_ms, MAX_CHECK_INTERVAL_MS);
        assert_eq!(
            f.detector.config().connect_timeout_ms,
            MAX_CONNECT_TIMEOUT_MS
        );
        assert_eq!(f.detector.config().fail_threshold, MAX_FAIL_THRESHOLD);

        // Would panic on the `Duration` multiply before the clamp existed.
        assert!(
            !f.detector.has_quorum(),
            "unprobed peers still do not count, clamp or not"
        );
    }

    /// End-to-end: a zero check interval reaching `spawn_failure_detector_task`
    /// unclamped would panic `tokio::time::interval` on its very first tick,
    /// killing the task immediately. The constructor's clamp means the task
    /// starts and keeps running instead.
    // FM-CLUSTER-102
    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn a_zero_check_interval_does_not_panic_the_detector_task() {
        let f = build(
            1,
            vec![primary_node(1)],
            FailureDetectorConfig {
                check_interval_ms: 0,
                connect_timeout_ms: 0,
                fail_threshold: 0,
            },
            ClusterRuntimeFlags::default(),
            network_reporting(&[]),
        );

        let handle = spawn_failure_detector_task(Arc::clone(&f.detector));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !handle.is_finished(),
            "the task must still be running, not panicked on the first tick"
        );
        handle.abort();
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

    // ---- The detector itself: consensus, reconciliation, auto-failover -----
    //
    // Everything below drives a real `FailureDetector` through the
    // `DetectorRaft` seam. The pure tests above cover the health table; these
    // cover what the detector *does* with it.

    use frogdb_core::cluster::network::ConnectFactory;
    use frogdb_core::cluster::{
        BoxedStream, BusRpc, ClusterBusStats, ClusterRpcRequest, ClusterRpcResponse,
        RaftClientWriteError, new_framed, parse_rpc_message, send_rpc_response,
    };
    use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    fn client_addr(id: NodeId) -> SocketAddr {
        format!("127.0.0.1:{}", 7000 + id).parse().unwrap()
    }

    fn bus_addr(id: NodeId) -> SocketAddr {
        format!("127.0.0.1:{}", 17000 + id).parse().unwrap()
    }

    fn primary_node(id: NodeId) -> NodeInfo {
        NodeInfo::new_primary(id, client_addr(id), bus_addr(id))
    }

    fn replica_node(id: NodeId, primary_id: NodeId) -> NodeInfo {
        NodeInfo::new_replica(id, client_addr(id), bus_addr(id), primary_id)
    }

    /// A scripted stand-in for `openraft::Raft`.
    ///
    /// Records every proposed command, answers with a settable outcome after a
    /// settable delay, and reports a settable server state. Cloning shares the
    /// recording, so a test can hold one handle while the detector holds another.
    #[derive(Clone, Default)]
    struct FakeRaft(Arc<FakeRaftInner>);

    struct FakeRaftInner {
        state: Mutex<ServerState>,
        outcome: Mutex<Result<ClusterResponse, RaftClientWriteError>>,
        delay: Mutex<Duration>,
        writes: Mutex<Vec<ClusterCommand>>,
        voter_changes: Mutex<Vec<VoterChange>>,
    }

    impl Default for FakeRaftInner {
        fn default() -> Self {
            Self {
                state: Mutex::new(ServerState::Follower),
                outcome: Mutex::new(Ok(ClusterResponse::Ok)),
                delay: Mutex::new(Duration::ZERO),
                writes: Mutex::new(Vec::new()),
                voter_changes: Mutex::new(Vec::new()),
            }
        }
    }

    impl FakeRaft {
        fn with_state(state: ServerState) -> Self {
            let raft = Self::default();
            raft.set_state(state);
            raft
        }

        fn set_state(&self, state: ServerState) {
            *self.0.state.lock().unwrap() = state;
        }

        fn set_outcome(&self, outcome: Result<ClusterResponse, RaftClientWriteError>) {
            *self.0.outcome.lock().unwrap() = outcome;
        }

        fn set_delay(&self, delay: Duration) {
            *self.0.delay.lock().unwrap() = delay;
        }

        fn write_count(&self) -> usize {
            self.0.writes.lock().unwrap().len()
        }

        /// The voter-set changes this detector asked Raft for.
        fn voter_changes(&self) -> Vec<VoterChange> {
            self.0.voter_changes.lock().unwrap().clone()
        }

        /// The proposals seen so far. `ClusterCommand` carries no `PartialEq`,
        /// so they are compared through `Debug`.
        fn writes(&self) -> Vec<String> {
            self.0
                .writes
                .lock()
                .unwrap()
                .iter()
                .map(|cmd| format!("{cmd:?}"))
                .collect()
        }
    }

    impl RaftProposer for FakeRaft {
        async fn client_write(
            &self,
            cmd: ClusterCommand,
        ) -> Result<ClusterResponse, RaftClientWriteError> {
            let delay = *self.0.delay.lock().unwrap();
            self.0.writes.lock().unwrap().push(cmd);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let outcome = self.0.outcome.lock().unwrap();
            outcome.clone()
        }
    }

    impl DetectorRaft for FakeRaft {
        fn server_state(&self) -> ServerState {
            *self.0.state.lock().unwrap()
        }

        fn spawn_voter_change(&self, change: VoterChange) {
            self.0.voter_changes.lock().unwrap().push(change);
        }
    }

    /// Render an expected proposal the same way [`FakeRaft::writes`] does.
    fn proposed(cmd: ClusterCommand) -> String {
        format!("{cmd:?}")
    }

    fn forward_to_leader_err() -> RaftClientWriteError {
        RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
            leader_id: None,
            leader_node: None,
        }))
    }

    struct Fixture {
        detector: Arc<FailureDetector<FakeRaft>>,
        raft: FakeRaft,
        state: Arc<ClusterState>,
    }

    fn build(
        self_id: NodeId,
        nodes: Vec<NodeInfo>,
        config: FailureDetectorConfig,
        flags: ClusterRuntimeFlags,
        network: Arc<ClusterNetworkFactory>,
    ) -> Fixture {
        let state = ClusterState::new();
        for node in nodes {
            state
                .apply_local(ClusterCommand::AddNode { node })
                .expect("seeding the topology must succeed");
        }
        let state = Arc::new(state);
        let raft = FakeRaft::with_state(ServerState::Leader);
        let detector = Arc::new(FailureDetector::new(
            self_id,
            config,
            Arc::new(flags),
            Arc::clone(&state),
            raft.clone(),
            network,
        ));
        Fixture {
            detector,
            raft,
            state,
        }
    }

    /// A leader detector with the default timings, auto-failover off, and a
    /// network on which no peer answers.
    fn leader_fixture(self_id: NodeId, nodes: Vec<NodeInfo>) -> Fixture {
        build(
            self_id,
            nodes,
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(false, true, 100),
            network_reporting(&[]),
        )
    }

    /// A network whose peers answer `HealthProbe` with a scripted replication
    /// offset; a node absent from the map refuses the connection, which is how
    /// an unreachable candidate is exercised without a real partition.
    fn network_reporting(offsets: &[(NodeId, u64)]) -> Arc<ClusterNetworkFactory> {
        let by_addr: BTreeMap<SocketAddr, (NodeId, u64)> = offsets
            .iter()
            .map(|&(id, offset)| (bus_addr(id), (id, offset)))
            .collect();
        let mut factory = ClusterNetworkFactory::new();
        factory.set_connect_factory(probe_factory(by_addr));
        Arc::new(factory)
    }

    fn probe_factory(by_addr: BTreeMap<SocketAddr, (NodeId, u64)>) -> ConnectFactory {
        Arc::new(move |addr: SocketAddr| {
            let probe = by_addr.get(&addr).copied();
            Box::pin(async move {
                let Some((node_id, offset)) = probe else {
                    return Err(std::io::Error::other("peer unreachable"));
                };
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(serve_health_probes(server, node_id, offset));
                Ok(Box::new(client) as BoxedStream)
            })
        })
    }

    /// The peer half of a bus connection: answer every health probe with `offset`.
    async fn serve_health_probes(server: tokio::io::DuplexStream, node_id: NodeId, offset: u64) {
        let mut framed = new_framed(Box::new(server));
        let stats = ClusterBusStats::new();
        while let Ok(request) = parse_rpc_message(&mut framed, &stats).await {
            let response = match request {
                ClusterRpcRequest::Bus(BusRpc::HealthProbe) => {
                    ClusterRpcResponse::HealthProbeResponse {
                        node_id,
                        replication_offset: offset,
                    }
                }
                other => ClusterRpcResponse::Error(format!("unexpected request: {other:?}")),
            };
            if send_rpc_response(&mut framed, response, &stats)
                .await
                .is_err()
            {
                break;
            }
        }
    }

    /// Give spawned reconciliation tasks time to run to completion.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// Poll `ready` until it holds, failing the test rather than hanging.
    async fn eventually(label: &str, mut ready: impl FnMut() -> bool) {
        for _ in 0..2_000 {
            if ready() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("timed out waiting for {label}");
    }

    /// Five check intervals, floored at a second: long enough to ride out an
    /// election, short enough that a wedged write frees the peer's slot.
    #[tokio::test]
    async fn the_raft_write_timeout_is_five_intervals_with_a_one_second_floor() {
        for (interval_ms, expected_ms) in
            [(1_000u64, 5_000u64), (400, 2_000), (10, 1_000), (0, 1_000)]
        {
            let f = build(
                1,
                vec![primary_node(1)],
                FailureDetectorConfig {
                    check_interval_ms: interval_ms,
                    ..Default::default()
                },
                ClusterRuntimeFlags::default(),
                network_reporting(&[]),
            );
            assert_eq!(
                f.detector.raft_write_timeout(),
                Duration::from_millis(expected_ms),
                "check interval {interval_ms}ms"
            );
        }
    }

    /// Only `Leader` reconciles. Every other Raft role — including `Learner`,
    /// which is what a default-constructed `ServerState` is — must not write.
    #[tokio::test]
    async fn only_the_leader_role_reconciles() {
        let f = leader_fixture(1, vec![primary_node(1)]);
        for state in [
            ServerState::Learner,
            ServerState::Follower,
            ServerState::Candidate,
            ServerState::Shutdown,
        ] {
            f.raft.set_state(state);
            assert!(!f.detector.is_leader(), "{state:?} is not the leader");
        }
        f.raft.set_state(ServerState::Leader);
        assert!(f.detector.is_leader());
    }

    /// The local probe recorders actually move the health table: a success
    /// makes a peer `Healthy`, a full run of failures latches it `Failed`, and
    /// a full run of successes clears it again.
    // FM-CLUSTER-052
    #[tokio::test]
    async fn local_probe_results_land_in_the_health_table() {
        let f = leader_fixture(1, vec![primary_node(1), primary_node(2)]);
        let threshold = f.detector.config().fail_threshold;
        let verdict = || f.detector.health.read().unwrap().verdict(2, Instant::now());

        assert_eq!(verdict(), LocalVerdict::Unknown, "never probed");

        f.detector.record_success_local(2);
        assert_eq!(verdict(), LocalVerdict::Healthy);

        for _ in 0..threshold {
            f.detector.record_failure_local(2);
        }
        assert_eq!(verdict(), LocalVerdict::Failed);

        for _ in 0..threshold {
            f.detector.record_success_local(2);
        }
        assert_eq!(verdict(), LocalVerdict::Healthy);
    }

    /// Reconciliation is a diff, not a broadcast: it writes exactly when the
    /// local verdict and the replicated flag disagree, and stays silent in
    /// every other combination — including both `Unknown` cases, where this
    /// node has no basis for an opinion.
    #[tokio::test]
    async fn reconciliation_writes_only_where_the_local_view_diverges() {
        enum Local {
            Failed,
            Healthy,
            Unknown,
        }
        let cases = [
            (
                Local::Failed,
                false,
                vec![proposed(ClusterCommand::MarkNodeFailed { node_id: 2 })],
            ),
            (Local::Failed, true, vec![]),
            (
                Local::Healthy,
                true,
                vec![proposed(ClusterCommand::MarkNodeRecovered { node_id: 2 })],
            ),
            (Local::Healthy, false, vec![]),
            (Local::Unknown, false, vec![]),
            (Local::Unknown, true, vec![]),
        ];

        for (local, marked_failed, expected) in cases {
            let f = leader_fixture(1, vec![primary_node(1), primary_node(2)]);
            match local {
                Local::Failed => {
                    for _ in 0..f.detector.config().fail_threshold {
                        f.detector.record_failure_local(2);
                    }
                }
                Local::Healthy => f.detector.record_success_local(2),
                Local::Unknown => {}
            }
            if marked_failed {
                f.state
                    .apply_local(ClusterCommand::MarkNodeFailed { node_id: 2 })
                    .expect("flagging the peer must succeed");
            }

            f.detector.reconcile_topology();
            settle().await;
            assert_eq!(f.raft.writes(), expected, "marked_failed={marked_failed}");
        }
    }

    /// Reconciliation judges peers, never this node. A node cannot probe
    /// itself, so acting on its own entry would flag the leader as failed on
    /// the strength of a probe that never ran.
    #[tokio::test]
    async fn reconciliation_never_judges_this_node() {
        let f = leader_fixture(1, vec![primary_node(1), primary_node(2)]);
        for _ in 0..f.detector.config().fail_threshold {
            f.detector.record_failure_local(1);
            f.detector.record_failure_local(2);
        }

        f.detector.reconcile_topology();
        settle().await;
        assert_eq!(
            f.raft.writes(),
            vec![proposed(ClusterCommand::MarkNodeFailed { node_id: 2 })],
            "only the peer is reconciled"
        );
    }

    /// The in-flight set is what keeps a level-triggered reconciliation from
    /// re-issuing a slow write once per tick — and the guard is what gives the
    /// slot back afterwards, so a write that needs retrying still gets one.
    #[tokio::test]
    async fn an_in_flight_reconciliation_blocks_the_next_tick_then_frees_the_slot() {
        let f = leader_fixture(1, vec![primary_node(1), primary_node(2)]);
        f.raft.set_delay(Duration::from_millis(300));
        for _ in 0..f.detector.config().fail_threshold {
            f.detector.record_failure_local(2);
        }

        f.detector.reconcile_topology();
        f.detector.reconcile_topology();
        settle().await;
        assert_eq!(
            f.raft.write_count(),
            1,
            "the second tick must not re-issue an in-flight write"
        );

        eventually("the in-flight slot to clear", || {
            f.detector.inflight.read().unwrap().is_empty()
        })
        .await;

        f.detector.reconcile_topology();
        eventually("the retry write", || f.raft.write_count() == 2).await;
    }

    /// Quorum is counted from locally probed peers, through both the inherent
    /// method and the `QuorumChecker` the write gate holds.
    // FM-CLUSTER-055
    #[tokio::test]
    async fn quorum_follows_the_locally_probed_peers() {
        let f = leader_fixture(1, vec![primary_node(1), primary_node(2), primary_node(3)]);
        let checker: Arc<dyn QuorumChecker> = f.detector.clone();

        assert!(
            !f.detector.has_quorum(),
            "unprobed peers do not count: 1 of 3 is not a majority"
        );
        assert!(!checker.has_quorum(), "the gate sees the same verdict");

        f.detector.record_success_local(2);
        assert!(f.detector.has_quorum(), "self plus one peer is 2 of 3");
        assert!(checker.has_quorum());
    }

    /// The offsets that drive promotion are the ones the probes returned. Here
    /// the freshest replica also holds the *higher* node id, so a detector that
    /// ignored the probe results would fall through to the id tiebreak and
    /// promote the laggard.
    // FM-CLUSTER-056
    #[tokio::test]
    async fn auto_failover_promotes_the_replica_with_the_freshest_offset() {
        let f = build(
            1,
            vec![
                primary_node(1),
                primary_node(2),
                replica_node(3, 2),
                replica_node(4, 2),
            ],
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(true, true, 100),
            network_reporting(&[(3, 100), (4, 900)]),
        );

        f.detector.trigger_auto_failover(2).await;

        assert_eq!(
            f.raft.writes(),
            vec![proposed(ClusterCommand::Failover {
                old_primary_id: 2,
                new_primary_id: 4,
                force: true,
            })],
            "the least-lagged replica wins despite losing the id tiebreak"
        );
    }

    /// Auto-failover evicts the failed primary from the topology, so it owes the
    /// Raft voter set the same shrink `CLUSTER FORGET` does. It is the one
    /// removal site the connection layer never sees, and leaving the evicted
    /// node a voter would keep it in every quorum computation — the failure the
    /// failover exists to route around would still be counted.
    // FM-CLUSTER-101
    #[tokio::test]
    async fn auto_failover_removes_the_failed_primary_from_the_voter_set() {
        let f = build(
            1,
            vec![primary_node(1), primary_node(2), replica_node(3, 2)],
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(true, true, 100),
            network_reporting(&[(3, 500)]),
        );

        f.detector.trigger_auto_failover(2).await;

        assert_eq!(
            f.raft.voter_changes(),
            vec![VoterChange::Remove { node_id: 2 }],
            "the evicted primary stops voting"
        );
    }

    /// A failover the state machine refused changed no topology, so it owes the
    /// voter set nothing. Shrinking anyway would evict a node that is still
    /// serving — the failure mode is the inverse of the one above and worse.
    // FM-CLUSTER-101
    #[tokio::test]
    async fn a_rejected_auto_failover_leaves_the_voter_set_alone() {
        let f = build(
            1,
            vec![primary_node(1), primary_node(2), replica_node(3, 2)],
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(true, true, 100),
            network_reporting(&[(3, 500)]),
        );
        f.raft.set_outcome(Ok(ClusterResponse::Error(
            frogdb_core::cluster::ClusterError::NodeNotFound(3),
        )));

        f.detector.trigger_auto_failover(2).await;

        assert!(
            f.raft.voter_changes().is_empty(),
            "a refused topology change owes no membership change"
        );
    }

    /// Both tests above drive the detector through a fake `DetectorRaft`, so
    /// the production impl's one-line delegation to Raft is otherwise never
    /// executed by anything. Dropping it would leave every one of those tests
    /// green while, in production, every auto-failover's voter removal became
    /// a silent no-op — the exact failure this row exists to prevent.
    // FM-CLUSTER-101
    #[cfg(not(feature = "turmoil"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_production_seam_reaches_raft_with_the_voter_change() {
        use frogdb_core::cluster::{ClusterStateMachine, ClusterStorage};
        use openraft::BasicNode;

        let dir = tempfile::tempdir().unwrap();
        let storage = ClusterStorage::open(dir.path()).unwrap();
        let raft = openraft::Raft::new(
            1,
            Arc::new(openraft::Config {
                election_timeout_min: 100,
                election_timeout_max: 200,
                heartbeat_interval: 50,
                ..Default::default()
            }),
            ClusterNetworkFactory::with_timeouts(50, 50),
            storage,
            ClusterStateMachine::new(),
        )
        .await
        .expect("a single-node Raft must start");
        raft.initialize(BTreeMap::from([(
            1u64,
            BasicNode {
                addr: "127.0.0.1:16379".to_string(),
            },
        )]))
        .await
        .expect("bootstrapping one voter must succeed");

        let raft = Arc::new(raft);
        DetectorRaft::spawn_voter_change(
            &raft,
            VoterChange::Add {
                node_id: 2,
                addr: "127.0.0.1:16380".parse().unwrap(),
            },
        );

        // The membership watch lags the commit that moves it, and `add_learner`
        // commits the entry before blocking on the (unreachable) peer's
        // catch-up, so node 2 appearing there is the delegation's only witness.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let membership = raft.metrics().borrow().membership_config.clone();
            if membership.membership().nodes().any(|(id, _)| *id == 2) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 2 never reached Raft: {membership:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        raft.shutdown().await.unwrap();
    }

    /// A replica's death is not a failover. Node 3 is a replica of 2 and node 4
    /// replicates 3, so a detector that skipped the primary check would find a
    /// candidate and promote it.
    #[tokio::test]
    async fn auto_failover_ignores_a_failed_replica() {
        let f = build(
            1,
            vec![
                primary_node(1),
                primary_node(2),
                replica_node(3, 2),
                replica_node(4, 3),
            ],
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(true, true, 100),
            network_reporting(&[(4, 500)]),
        );

        f.detector.trigger_auto_failover(3).await;

        assert!(
            f.raft.writes().is_empty(),
            "only a primary's failure triggers a promotion"
        );
    }

    /// The failover write is retried, and the backoff between attempts is real:
    /// three attempts means exactly two 500ms sleeps, with none after the last.
    #[tokio::test(start_paused = true)]
    async fn auto_failover_retries_the_failover_write_with_backoff() {
        let f = build(
            1,
            vec![primary_node(1), primary_node(2), replica_node(3, 2)],
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(true, true, 100),
            network_reporting(&[]),
        );
        f.raft.set_outcome(Err(forward_to_leader_err()));

        let started = tokio::time::Instant::now();
        f.detector.trigger_auto_failover(2).await;

        assert_eq!(f.raft.write_count(), 3, "three attempts");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(1_000),
            "two backoffs between three attempts, none trailing the last"
        );
    }

    /// The accessors hand back the instances the detector actually decides
    /// from, not fresh defaults — so an operator's `CONFIG SET` is visible
    /// through them.
    #[tokio::test]
    async fn the_detector_exposes_the_config_and_flags_it_decides_from() {
        let f = build(
            1,
            vec![primary_node(1)],
            FailureDetectorConfig {
                check_interval_ms: 37,
                connect_timeout_ms: 11,
                fail_threshold: 2,
            },
            ClusterRuntimeFlags::new(true, false, 42),
            network_reporting(&[]),
        );

        assert_eq!(f.detector.config().check_interval_ms, 37);
        assert_eq!(f.detector.config().connect_timeout_ms, 11);
        assert_eq!(f.detector.config().fail_threshold, 2);

        assert!(f.detector.flags().auto_failover());
        assert!(!f.detector.flags().self_fence_on_quorum_loss());
        assert_eq!(f.detector.flags().replica_priority(), 42);

        f.detector.flags().set_replica_priority(7);
        assert_eq!(
            f.detector.flags().replica_priority(),
            7,
            "the accessor exposes the shared instance, not a copy"
        );
    }

    /// This node scores itself from the live flag; peers are scored from the
    /// value they published through Raft, which is the only value this node can
    /// know for them.
    // FM-CLUSTER-058
    #[tokio::test]
    async fn effective_priority_is_live_for_this_node_and_published_for_peers() {
        let mut me = primary_node(1);
        me.replica_priority = 7; // the stale value this node published at boot
        let mut peer = primary_node(2);
        peer.replica_priority = 99;
        let f = build(
            1,
            vec![me.clone(), peer.clone()],
            FailureDetectorConfig::default(),
            ClusterRuntimeFlags::new(false, true, 42),
            network_reporting(&[]),
        );

        assert_eq!(f.detector.effective_priority(&me), 42, "live flag wins");
        assert_eq!(f.detector.effective_priority(&peer), 99, "published value");

        f.detector.flags().set_replica_priority(3);
        assert_eq!(f.detector.effective_priority(&me), 3);
        assert_eq!(f.detector.effective_priority(&peer), 99);
    }

    /// The reachability probe is a real TCP connect: a bound listener answers,
    /// a port nobody is listening on does not.
    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn a_probe_tells_a_live_listener_from_a_closed_port() {
        let listener = frogdb_net::tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
            .await
            .expect("binding an ephemeral port must succeed");
        let live = listener.local_addr().unwrap();
        assert!(check_node_reachable(live, Duration::from_secs(1)).await);

        let doomed = frogdb_net::tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
            .await
            .expect("binding an ephemeral port must succeed");
        let closed = doomed.local_addr().unwrap();
        drop(doomed);
        assert!(!check_node_reachable(closed, Duration::from_millis(500)).await);
    }

    /// The detector task probes peers and skips itself. This node's own bus
    /// address is closed here, so a loop that probed itself would record a
    /// failure for the one node that is by definition reachable — and would
    /// never learn that the peer is up, leaving quorum permanently lost.
    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn the_detector_task_probes_every_peer_except_itself() {
        let peer_listener = frogdb_net::tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
            .await
            .expect("binding an ephemeral port must succeed");
        let doomed = frogdb_net::tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
            .await
            .expect("binding an ephemeral port must succeed");
        let dead = doomed.local_addr().unwrap();
        drop(doomed);

        let mut me = primary_node(1);
        me.cluster_addr = dead;
        let mut peer = primary_node(2);
        peer.cluster_addr = peer_listener.local_addr().unwrap();

        let f = build(
            1,
            vec![me, peer],
            FailureDetectorConfig {
                check_interval_ms: 20,
                connect_timeout_ms: 200,
                fail_threshold: 5,
            },
            ClusterRuntimeFlags::default(),
            network_reporting(&[]),
        );
        f.raft.set_state(ServerState::Follower);

        let handle = spawn_failure_detector_task(Arc::clone(&f.detector));
        eventually("quorum to form from the probed peer", || {
            f.detector.has_quorum()
        })
        .await;
        handle.abort();
    }

    // ---- Generated config admission (issue 22, FM-CLUSTER-102) -------------
    //
    // `FailureDetectorConfig` is startup data: it arrives from `frogdb.conf`
    // (`ClusterConfigSection`) or a hand-built value in a test, and nothing
    // upstream of `FailureDetector::new` validates it. The three point
    // witnesses above (`degenerate_zero_config_is_clamped_to_safe_minimums`,
    // `huge_config_values_are_clamped_and_do_not_overflow_the_staleness_multiply`,
    // `a_zero_check_interval_does_not_panic_the_detector_task`) each pin one
    // hand-picked degenerate input. This module generates the space between
    // and around those points so a clamp regression that misses the exact
    // literals above still gets caught.
    //
    // No other constructor in this crate takes a millisecond knob straight
    // from a config struct — `handoff_barrier`'s `barrier_ms` is relayed data
    // from a Raft-replicated event, not a value admitted here, and every
    // other module's `new` takes booleans/priorities with no timing
    // arithmetic downstream. `FailureDetectorConfig` is the crate's only
    // instance of this shape today; C1/C2 are written generically enough
    // (drive any generated config through the real constructor, check what
    // comes out and what derives from it) that a second one added later
    // slots into the same two properties rather than needing its own.
    mod config_admission {
        use super::*;
        use proptest::prelude::*;

        /// Full-`u64`-range strategy for a millisecond knob, with the
        /// degenerate values that define FM-CLUSTER-102 — 0, 1, `MAX`,
        /// `MAX - 1` — weighted in rather than left to a 1-in-2^64 chance of
        /// being drawn from `any::<u64>()`.
        fn arb_ms_u64() -> impl Strategy<Value = u64> {
            prop_oneof![
                1 => Just(0u64),
                1 => Just(1u64),
                1 => Just(u64::MAX - 1),
                1 => Just(u64::MAX),
                6 => any::<u64>(),
            ]
        }

        /// Same shape as [`arb_ms_u64`], over `fail_threshold`'s `u32` range.
        fn arb_fail_threshold() -> impl Strategy<Value = u32> {
            prop_oneof![
                1 => Just(0u32),
                1 => Just(1u32),
                1 => Just(u32::MAX - 1),
                1 => Just(u32::MAX),
                6 => any::<u32>(),
            ]
        }

        /// Every field of [`FailureDetectorConfig`] drawn independently over
        /// its full representable range. Deliberately does **not** pre-clamp
        /// or otherwise bias toward sane values: the whole point is to hand
        /// the constructor exactly what an operator's config file, or a
        /// caller that built the struct by hand, could contain.
        fn arb_failure_detector_config() -> impl Strategy<Value = FailureDetectorConfig> {
            (arb_ms_u64(), arb_ms_u64(), arb_fail_threshold()).prop_map(
                |(check_interval_ms, connect_timeout_ms, fail_threshold)| FailureDetectorConfig {
                    check_interval_ms,
                    connect_timeout_ms,
                    fail_threshold,
                },
            )
        }

        /// Build a detector from a generated config against a minimal fixture
        /// (self plus one peer, default flags, a network on which nobody
        /// answers). What C1/C2 exercise is the constructor's admission of
        /// `cfg` and what derives from the config it stores, not the
        /// detector's probing/reconciliation behavior, so the rest of the
        /// fixture is intentionally the simplest one that type-checks.
        fn build_with(cfg: FailureDetectorConfig) -> Fixture {
            build(
                1,
                vec![primary_node(1), primary_node(2)],
                cfg,
                ClusterRuntimeFlags::default(),
                network_reporting(&[]),
            )
        }

        proptest! {
            /// **C1** — admission is total: constructing a detector from
            /// *any* generated [`FailureDetectorConfig`] never panics, and
            /// the config it reports back through the accessor always lies
            /// inside `[MIN_*, MAX_*]` for every field — never the raw value
            /// a caller passed in. This is the universal form of the three
            /// point witnesses; it fails on the first generated case once
            /// `FailureDetector::new`'s `config.clamped()` call is removed,
            /// because an unclamped `0` or `u64::MAX`/`u32::MAX` input is
            /// then reported back verbatim.
            // FM-CLUSTER-102
            #[test]
            fn c1_admission_is_total(cfg in arb_failure_detector_config()) {
                let f = build_with(cfg);
                let admitted = f.detector.config();

                prop_assert!(
                    (MIN_CHECK_INTERVAL_MS..=MAX_CHECK_INTERVAL_MS)
                        .contains(&admitted.check_interval_ms),
                    "check_interval_ms {} outside [{MIN_CHECK_INTERVAL_MS}, {MAX_CHECK_INTERVAL_MS}]",
                    admitted.check_interval_ms,
                );
                prop_assert!(
                    (MIN_CONNECT_TIMEOUT_MS..=MAX_CONNECT_TIMEOUT_MS)
                        .contains(&admitted.connect_timeout_ms),
                    "connect_timeout_ms {} outside [{MIN_CONNECT_TIMEOUT_MS}, {MAX_CONNECT_TIMEOUT_MS}]",
                    admitted.connect_timeout_ms,
                );
                prop_assert!(
                    (MIN_FAIL_THRESHOLD..=MAX_FAIL_THRESHOLD).contains(&admitted.fail_threshold),
                    "fail_threshold {} outside [{MIN_FAIL_THRESHOLD}, {MAX_FAIL_THRESHOLD}]",
                    admitted.fail_threshold,
                );

                // Exercises `HealthTable::stale_threshold`'s multiply end to
                // end through the admitted config; must not panic for any
                // input this property generated.
                let _ = f.detector.has_quorum();
            }

            /// **C2** — every duration this crate derives from an admitted
            /// config is finite (its arithmetic does not overflow `Duration`)
            /// and non-zero. Covers the three call sites that read
            /// `FailureDetectorConfig` fields directly: `raft_write_timeout`,
            /// the background task's interval/timeout construction
            /// (mirrored from `spawn_failure_detector_task`, which needs a
            /// live Tokio runtime to reach directly), and
            /// `HealthTable::stale_threshold`. A future derivation reusing
            /// any of these fields is caught by this same property rather
            /// than needing its own row.
            // FM-CLUSTER-102
            #[test]
            fn c2_every_derived_duration_is_finite_and_nonzero(cfg in arb_failure_detector_config()) {
                let f = build_with(cfg);
                let admitted = f.detector.config().clone();

                prop_assert!(f.detector.raft_write_timeout() > Duration::ZERO);

                // Mirrors spawn_failure_detector_task's interval/timeout
                // construction; a zero duration here is exactly what makes
                // `tokio::time::interval` panic on its first tick.
                let task_interval = Duration::from_millis(admitted.check_interval_ms);
                let task_timeout = Duration::from_millis(admitted.connect_timeout_ms);
                prop_assert!(task_interval > Duration::ZERO);
                prop_assert!(task_timeout > Duration::ZERO);

                // Mirrors HealthTable::stale_threshold's `check_interval *
                // (fail_threshold + 2)` multiply; panics on overflow if the
                // admitted values are not held inside their documented
                // bounds.
                let table = HealthTable::new(admitted.fail_threshold, task_interval);
                prop_assert!(table.stale_threshold() > Duration::ZERO);
            }
        }
    }
}
