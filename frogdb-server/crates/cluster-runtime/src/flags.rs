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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Shared, runtime-mutable cluster decision flags.
#[derive(Debug)]
pub struct ClusterRuntimeFlags {
    /// `cluster-auto-failover`: promote a replica when a primary is latched FAIL.
    auto_failover: AtomicBool,
    /// `cluster-self-fence-on-quorum-loss`: reject writes when quorum is lost.
    self_fence_on_quorum_loss: AtomicBool,
    /// `cluster-replica-priority`: this node's promotion priority (0 = never).
    replica_priority: AtomicU32,
}

impl ClusterRuntimeFlags {
    /// Build the flags from explicit values.
    pub fn new(
        auto_failover: bool,
        self_fence_on_quorum_loss: bool,
        replica_priority: u32,
    ) -> Self {
        Self {
            auto_failover: AtomicBool::new(auto_failover),
            self_fence_on_quorum_loss: AtomicBool::new(self_fence_on_quorum_loss),
            replica_priority: AtomicU32::new(replica_priority),
        }
    }

    /// Build the shared flags from the startup `[cluster]` section.
    pub fn from_config(cluster: &frogdb_config::ClusterConfigSection) -> Arc<Self> {
        Arc::new(Self::new(
            cluster.auto_failover,
            cluster.self_fence_on_quorum_loss,
            cluster.replica_priority,
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
}

impl Default for ClusterRuntimeFlags {
    /// Startup defaults, taken from the config section so the two can never drift.
    fn default() -> Self {
        let defaults = frogdb_config::ClusterConfigSection::default();
        Self::new(
            defaults.auto_failover,
            defaults.self_fence_on_quorum_loss,
            defaults.replica_priority,
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
    #[test]
    fn auto_failover_flag_is_live() {
        let flags = ClusterRuntimeFlags::new(false, true, 100);
        assert!(!flags.auto_failover());
        flags.set_auto_failover(true);
        assert!(flags.auto_failover());
        flags.set_auto_failover(false);
        assert!(!flags.auto_failover());
    }

    /// `cluster-self-fence-on-quorum-loss` gates the *verdict*, not the
    /// installation of the checker: with quorum lost, the gate fences while
    /// enabled and stops fencing the moment it is disabled.
    #[test]
    fn self_fence_gate_follows_live_flag() {
        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100));
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
    #[test]
    fn self_fence_gate_passes_through_healthy_quorum() {
        let flags = Arc::new(ClusterRuntimeFlags::new(false, true, 100));
        let gate = SelfFenceGate::new(Arc::new(FixedChecker { has_quorum: true }), flags.clone());
        assert!(gate.has_quorum());
        flags.set_self_fence_on_quorum_loss(false);
        assert!(gate.has_quorum());
    }
}
