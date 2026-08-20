//! Live cluster tuning flags.
//!
//! The `[cluster]` knobs that steer *decisions* (rather than Raft wiring) are
//! held here as shared atomics instead of being copied into each subsystem at
//! startup. Every consumer reads the atomic at the moment it makes the
//! decision, so a `CONFIG SET` takes effect on the next failover / fence /
//! promotion check without a restart.
//!
//! Ownership: the server's `runtime_config::ConfigManager` builds the single
//! instance from the startup [`frogdb_config::ClusterConfigSection`] and hands
//! clones to the failure detector (auto-failover, replica priority) and the
//! self-fence gate. That mirrors how `max_clients` / `listpack` /
//! `notify_keyspace_events` are shared today: one owner, handle passed to the
//! point of use, no post-construction wiring that can silently no-op.

use frogdb_core::command::QuorumChecker;
use frogdb_core::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

/// Shared, runtime-mutable cluster decision flags.
#[derive(Debug)]
pub struct ClusterRuntimeFlags {
    /// `cluster-auto-failover`: promote a replica when a primary is latched FAIL.
    auto_failover: AtomicBool,
    /// `cluster-self-fence-on-quorum-loss`: reject writes when quorum is lost.
    self_fence_on_quorum_loss: AtomicBool,
    /// `cluster-replica-priority`: this node's promotion priority (0 = never).
    replica_priority: AtomicU32,
    /// `cluster-promotion-max-lag-bytes`: the automatic-promotion staleness
    /// bound, in replication-offset bytes behind the freshest candidate
    /// (0 = no bound). See TR-CLUSTER-043.
    promotion_max_lag_bytes: AtomicU64,
}

impl ClusterRuntimeFlags {
    /// Build the flags from explicit values.
    pub fn new(
        auto_failover: bool,
        self_fence_on_quorum_loss: bool,
        replica_priority: u32,
        promotion_max_lag_bytes: u64,
    ) -> Self {
        Self {
            auto_failover: AtomicBool::new(auto_failover),
            self_fence_on_quorum_loss: AtomicBool::new(self_fence_on_quorum_loss),
            replica_priority: AtomicU32::new(replica_priority),
            promotion_max_lag_bytes: AtomicU64::new(promotion_max_lag_bytes),
        }
    }

    /// Build the shared flags from the startup `[cluster]` section.
    pub fn from_config(cluster: &frogdb_config::ClusterConfigSection) -> Arc<Self> {
        Arc::new(Self::new(
            cluster.auto_failover,
            cluster.self_fence_on_quorum_loss,
            cluster.replica_priority,
            cluster.promotion_max_lag_bytes,
        ))
    }

    /// Whether automatic failover is currently enabled.
    pub fn auto_failover(&self) -> bool {
        self.auto_failover.load(Ordering::Relaxed)
    }

    /// Enable/disable automatic failover. Takes effect on the next node-FAIL latch.
    pub fn set_auto_failover(&self, enabled: bool) {
        self.auto_failover.store(enabled, Ordering::Relaxed);
    }

    /// Whether writes are fenced when this node loses quorum.
    pub fn self_fence_on_quorum_loss(&self) -> bool {
        self.self_fence_on_quorum_loss.load(Ordering::Relaxed)
    }

    /// Enable/disable self-fencing. Takes effect on the next write pre-check.
    pub fn set_self_fence_on_quorum_loss(&self, enabled: bool) {
        self.self_fence_on_quorum_loss
            .store(enabled, Ordering::Relaxed);
    }

    /// This node's current replica-promotion priority (lower wins, 0 = never).
    pub fn replica_priority(&self) -> u32 {
        self.replica_priority.load(Ordering::Relaxed)
    }

    /// Set this node's replica-promotion priority.
    ///
    /// Read at failover-candidate scoring time for *this* node and at every
    /// self-registration proposal, so a runtime change steers any promotion
    /// decision this node makes immediately. Peers keep scoring this node with
    /// the last value published through Raft (`ClusterCommand::AddNode`) until
    /// the node re-registers — cross-node propagation of a runtime change is a
    /// separate concern from the local seam.
    pub fn set_replica_priority(&self, priority: u32) {
        self.replica_priority.store(priority, Ordering::Relaxed);
    }

    /// The automatic-promotion staleness bound, in offset bytes (0 = no bound).
    pub fn promotion_max_lag_bytes(&self) -> u64 {
        self.promotion_max_lag_bytes.load(Ordering::Relaxed)
    }

    /// Set the automatic-promotion staleness bound.
    ///
    /// Read inside `select_failover_target`, so a runtime change steers the very
    /// next automatic promotion this node decides. Node-local by design: the
    /// bound is this node's data-loss appetite, not a property of any candidate,
    /// so it is never published through Raft (TR-CLUSTER-043).
    pub fn set_promotion_max_lag_bytes(&self, bytes: u64) {
        self.promotion_max_lag_bytes.store(bytes, Ordering::Relaxed);
    }
}

impl Default for ClusterRuntimeFlags {
    /// Startup defaults, taken from the config section so the two can never drift.
    fn default() -> Self {
        let defaults = frogdb_config::ClusterConfigSection::default();
        Self::new(
            defaults.auto_failover,
            defaults.self_fence_on_quorum_loss,
            defaults.replica_priority,
            defaults.promotion_max_lag_bytes,
        )
    }
}

/// Quorum checker that applies `cluster-self-fence-on-quorum-loss` at read time.
///
/// The write pre-check (the server's `connection::guards`) fences a write when its
/// quorum checker reports no quorum. Gating *installation* of the checker on
/// the config flag made the knob startup-only; wrapping the real checker in
/// this gate instead moves the decision to the point of use, so toggling the
/// flag arms or disarms fencing immediately.
pub struct SelfFenceGate {
    inner: Arc<dyn QuorumChecker>,
    flags: Arc<ClusterRuntimeFlags>,
}

impl SelfFenceGate {
    /// Wrap `inner` so its quorum verdict only fences writes while
    /// `cluster-self-fence-on-quorum-loss` is enabled.
    pub fn new(inner: Arc<dyn QuorumChecker>, flags: Arc<ClusterRuntimeFlags>) -> Self {
        Self { inner, flags }
    }
}

impl QuorumChecker for SelfFenceGate {
    fn has_quorum(&self) -> bool {
        // Disabled => report quorum unconditionally, which is exactly "do not
        // fence": the caller's only use of this verdict is the write gate.
        !self.flags.self_fence_on_quorum_loss() || self.inner.has_quorum()
    }

    /// Delegated, never spelled here: this gate only decides *whether* the
    /// checker it wraps may fence, so answering with a string of its own would
    /// re-label whatever verdict came from inside (FM-CLUSTER-059).
    fn quorum_lost_error(&self) -> &'static str {
        self.inner.quorum_lost_error()
    }
}

impl frogdb_core::metrics::WriteFenceReporter for SelfFenceGate {
    /// Derived from the same call the write gate makes, so `/status` cannot
    /// disagree with the gate about whether writes are fenced.
    fn write_fence_reason(&self) -> Option<&'static str> {
        (!self.has_quorum()).then_some("cluster quorum lost")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::metrics::WriteFenceReporter;

    struct FixedChecker {
        has_quorum: bool,
    }

    impl QuorumChecker for FixedChecker {
        fn has_quorum(&self) -> bool {
            self.has_quorum
        }
    }

    /// `cluster-auto-failover` is read at decision time: a store flips the
    /// answer with no restart and no re-construction of the detector.
    // FM-CLUSTER-059
    #[test]
    fn auto_failover_flag_is_live() {
        let flags = ClusterRuntimeFlags::new(false, true, 100, 0);
        assert!(!flags.auto_failover());
        flags.set_auto_failover(true);
        assert!(flags.auto_failover());
        flags.set_auto_failover(false);
        assert!(!flags.auto_failover());
    }

    /// `cluster-self-fence-on-quorum-loss` gates the *verdict*, not the
    /// installation of the checker: with quorum lost, the gate fences while
    /// enabled and stops fencing the moment it is disabled.
    // FM-CLUSTER-059
    #[test]
    fn self_fence_gate_follows_live_flag() {
        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100, 0));
        let gate = SelfFenceGate::new(Arc::new(FixedChecker { has_quorum: false }), flags.clone());

        // Enabled + no quorum => fenced (has_quorum false).
        assert!(!gate.has_quorum());

        // Runtime disable => writes flow again, same gate instance.
        flags.set_self_fence_on_quorum_loss(false);
        assert!(gate.has_quorum());

        // Re-enable => fenced again.
        flags.set_self_fence_on_quorum_loss(true);
        assert!(!gate.has_quorum());
    }

    /// A healthy inner checker is reported as healthy regardless of the flag.
    // FM-CLUSTER-059
    #[test]
    fn self_fence_gate_passes_through_healthy_quorum() {
        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100, 0));
        let gate = SelfFenceGate::new(Arc::new(FixedChecker { has_quorum: true }), flags.clone());
        assert!(gate.has_quorum());
        flags.set_self_fence_on_quorum_loss(false);
        assert!(gate.has_quorum());
    }

    /// The reason `/status` reports is derived from the same call the write
    /// gate makes, so the two can never disagree: a reason is present exactly
    /// when the gate fences, and absent in all three non-fencing states.
    // FM-CLUSTER-059
    #[test]
    fn self_fence_gate_reports_the_reason_it_gates_on() {
        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100, 0));
        let fenced =
            SelfFenceGate::new(Arc::new(FixedChecker { has_quorum: false }), flags.clone());
        assert_eq!(fenced.write_fence_reason(), Some("cluster quorum lost"));
        assert!(!fenced.has_quorum(), "reason present => gate fences");

        // Disabling the flag clears the reason on the same instance.
        flags.set_self_fence_on_quorum_loss(false);
        assert_eq!(fenced.write_fence_reason(), None);
        assert!(fenced.has_quorum(), "no reason => gate does not fence");

        // Healthy quorum never reports a reason, flag either way.
        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100, 0));
        let healthy =
            SelfFenceGate::new(Arc::new(FixedChecker { has_quorum: true }), flags.clone());
        assert_eq!(healthy.write_fence_reason(), None);
        flags.set_self_fence_on_quorum_loss(false);
        assert_eq!(healthy.write_fence_reason(), None);
    }

    // FM-CLUSTER-059
    /// The gate decides *whether* the checker it wraps may fence, never *what*
    /// the refusal says. Two checkers share the one write-gate rung — the Raft
    /// detector answers `-CLUSTERDOWN`, the replica-loss fence answers
    /// `-SELFFENCE` — so a gate that spelled a string of its own would relabel
    /// whichever verdict came from inside and send the operator to the wrong
    /// subsystem (issue 30).
    #[test]
    fn self_fence_gate_delegates_the_quorum_lost_wording() {
        struct NamedChecker;
        impl QuorumChecker for NamedChecker {
            fn has_quorum(&self) -> bool {
                false
            }
            fn quorum_lost_error(&self) -> &'static str {
                "SELFFENCE inner wording"
            }
        }

        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100, 0));
        let gate = SelfFenceGate::new(Arc::new(NamedChecker), flags.clone());
        assert_eq!(gate.quorum_lost_error(), "SELFFENCE inner wording");

        // The wording does not depend on the gate's own verdict: disabling the
        // flag stops the fencing, not the attribution.
        flags.set_self_fence_on_quorum_loss(false);
        assert_eq!(gate.quorum_lost_error(), "SELFFENCE inner wording");

        // A checker that says nothing of its own still gets the cluster's
        // wording through the trait default — the gate adds nothing either way.
        let plain = SelfFenceGate::new(Arc::new(FixedChecker { has_quorum: false }), flags);
        assert_eq!(
            plain.quorum_lost_error(),
            frogdb_core::command::CLUSTER_DOWN_QUORUM_LOST
        );
        assert!(
            plain.quorum_lost_error().starts_with("CLUSTERDOWN"),
            "the Raft detector's refusal is the one that names the cluster"
        );
    }

    /// Each config field lands on its own flag. `auto_failover` and
    /// `self_fence_on_quorum_loss` are both `bool` with *different* defaults,
    /// so a transposition silently inverts both policies.
    // FM-CLUSTER-060
    #[test]
    fn runtime_flags_from_config_carry_each_field() {
        let mut cluster = frogdb_config::ClusterConfigSection {
            auto_failover: true,
            self_fence_on_quorum_loss: false,
            replica_priority: 7,
            promotion_max_lag_bytes: 4_096,
            ..Default::default()
        };
        let flags = ClusterRuntimeFlags::from_config(&cluster);
        assert!(flags.auto_failover());
        assert!(!flags.self_fence_on_quorum_loss());
        assert_eq!(flags.replica_priority(), 7);
        assert_eq!(flags.promotion_max_lag_bytes(), 4_096);

        // Both booleans inverted: a transposed mapping would look identical
        // under the first case alone.
        cluster.auto_failover = false;
        cluster.self_fence_on_quorum_loss = true;
        cluster.replica_priority = 0;
        cluster.promotion_max_lag_bytes = 0;
        let flags = ClusterRuntimeFlags::from_config(&cluster);
        assert!(!flags.auto_failover());
        assert!(flags.self_fence_on_quorum_loss());
        assert_eq!(flags.replica_priority(), 0, "0 = never promote");
        assert_eq!(flags.promotion_max_lag_bytes(), 0, "0 = no bound");
    }

    /// The runtime defaults are the config defaults, so the same deployment
    /// behaves identically whether or not a config file was supplied.
    // FM-CLUSTER-060
    #[test]
    fn runtime_flags_default_matches_the_config_default() {
        let flags = ClusterRuntimeFlags::default();
        let config = frogdb_config::ClusterConfigSection::default();
        assert_eq!(flags.auto_failover(), config.auto_failover);
        assert_eq!(
            flags.self_fence_on_quorum_loss(),
            config.self_fence_on_quorum_loss
        );
        assert_eq!(flags.replica_priority(), config.replica_priority);
        assert_eq!(
            flags.promotion_max_lag_bytes(),
            config.promotion_max_lag_bytes
        );

        // Pinned literally as well: an operator reads these from the docs, and
        // a change to either default is a behavior change, not a refactor.
        assert!(!flags.auto_failover(), "auto-failover is opt-in");
        assert!(flags.self_fence_on_quorum_loss(), "fencing is opt-out");
        assert_eq!(flags.replica_priority(), 100);
        assert_eq!(
            flags.promotion_max_lag_bytes(),
            0,
            "the promotion staleness bound ships off"
        );
    }
}
