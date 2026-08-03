//! Quorum checker for replication mode.
//!
//! Monitors replica ACK freshness via the ReplicationTrackerImpl. When no
//! replica has ACKed recently, `has_quorum()` returns false, causing the
//! server's write gate (`commands/guards.rs`) to reject writes with
//! CLUSTERDOWN — fencing the primary during partitions.

use frogdb_core::command::QuorumChecker;
use frogdb_core::metrics::WriteFenceReporter;
use frogdb_core::{ReplicationTrackerImpl, ack_is_fresh};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// The `/status` + log wording for an active replica-loss fence. One constant so
/// the operator surface and the transition warning cannot drift apart.
const FENCE_REASON: &str = "replica quorum lost";

/// Quorum checker for replication mode (primary + N replicas).
///
/// Arms itself on the first streaming replica. Once armed, requires at least
/// one streaming replica with a recent ACK to allow writes.
///
/// Both operator knobs are live: the fence toggle and the freshness window are
/// atomics read at decision time, not captured into the shape of the object. A
/// primary therefore always *has* a checker; `self-fence-on-replica-loss = no`
/// simply makes every decision permissive, so `CONFIG SET
/// replication-self-fence-on-replica-loss yes` starts fencing without a restart
/// (the checker is already installed on every shard worker and connection).
///
/// The checker is installed on *every* node at boot, not only on a
/// boot-configured primary: the tracker it reads is likewise process-wide and
/// simply empty while this node is a replica, so a node promoted at runtime
/// (`REPLICAOF NO ONE`) fences on the very same object the write gate has been
/// holding since boot. Nothing is rebuilt, and no publication has to reach the
/// per-connection write gate after the fact.
pub struct ReplicationQuorumChecker {
    tracker: Arc<ReplicationTrackerImpl>,
    /// Live `self-fence-on-replica-loss`. When false, [`has_quorum`] is
    /// unconditionally true — the pre-seam behaviour of having no checker at all.
    ///
    /// [`has_quorum`]: QuorumChecker::has_quorum
    self_fence_enabled: AtomicBool,
    /// Live `replica-freshness-timeout-ms`, in milliseconds.
    freshness_timeout_ms: AtomicU64,
    /// Once a replica reaches Streaming, this flips to true and stays true.
    armed: AtomicBool,
}

impl ReplicationQuorumChecker {
    pub fn new(
        tracker: Arc<ReplicationTrackerImpl>,
        self_fence_enabled: bool,
        freshness_timeout: Duration,
    ) -> Self {
        Self {
            tracker,
            self_fence_enabled: AtomicBool::new(self_fence_enabled),
            freshness_timeout_ms: AtomicU64::new(freshness_timeout.as_millis() as u64),
            armed: AtomicBool::new(false),
        }
    }

    /// Whether self-fencing is currently armed by configuration.
    pub fn self_fence_enabled(&self) -> bool {
        self.self_fence_enabled.load(Ordering::Relaxed)
    }

    /// Enable/disable self-fencing at the next fence decision. Reachable from
    /// `ConfigManager` for `CONFIG SET replication-self-fence-on-replica-loss`.
    ///
    /// Enabling is deliberately *immediate*: arming is latched independently of
    /// the toggle (see [`QuorumChecker::has_quorum`]), so a primary that has
    /// already served a replica and then lost it starts rejecting writes on the
    /// very next command rather than getting a fresh grace period — the same
    /// immediacy `min-replicas-to-write` has. That is a large behaviour change
    /// to make silently, so the false -> true transition logs a `warn!` naming
    /// the consequence when the fence engages on the spot.
    pub fn set_self_fence_enabled(&self, enabled: bool) {
        let was_enabled = self.self_fence_enabled.swap(enabled, Ordering::Relaxed);
        if enabled && !was_enabled && self.fence_engaged() {
            tracing::warn!(
                reason = FENCE_REASON,
                freshness_timeout_ms = self.freshness_timeout_ms.load(Ordering::Relaxed),
                "replication self-fencing enabled while replica quorum was already lost: writes \
                 are now rejected with CLUSTERDOWN until a replica streams and ACKs again"
            );
        }
    }

    /// The current ACK-freshness window.
    pub fn freshness_timeout(&self) -> Duration {
        Duration::from_millis(self.freshness_timeout_ms.load(Ordering::Relaxed))
    }

    /// Retune the ACK-freshness window. Reachable from `ConfigManager` for
    /// `CONFIG SET replication-replica-freshness-timeout-ms`; the next freshness
    /// check uses the new value.
    pub fn set_freshness_timeout_ms(&self, ms: u64) {
        self.freshness_timeout_ms.store(ms, Ordering::Relaxed);
    }

    /// Whether this checker has latched "a replica has streamed at least once".
    pub fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    /// Un-latch arming, so the next fence decision starts from the
    /// never-had-a-replica state.
    ///
    /// Called on Role Demotion: the replica sessions this node was tracking as a
    /// primary are gone, and a later re-promotion must not inherit their arming
    /// (which would fence the fresh primary before it has ever had a replica).
    pub fn reset_arming(&self) {
        self.armed.store(false, Ordering::Relaxed);
    }

    /// Count streaming replicas whose last ACK is within the freshness timeout.
    /// The window is loaded here, per check, not captured at construction.
    ///
    /// The comparison itself lives in [`ack_is_fresh`] so the self-fence and the
    /// `min-replicas-to-write` gate cannot drift apart on where the boundary
    /// falls, and so the boundary is assertable without racing a wall clock.
    /// Unlike `count_good_replicas`, a zero window here is *not* a disable
    /// sentinel: `replica-freshness-timeout-ms` rejects `0` at validation.
    fn count_fresh_streaming_replicas(&self) -> usize {
        let freshness_timeout = self.freshness_timeout();
        self.tracker
            .get_streaming_replicas()
            .iter()
            .filter(|r| ack_is_fresh(r.last_ack_time.elapsed(), freshness_timeout))
            .count()
    }

    /// Latch arming once any replica reaches Streaming, and report the latched
    /// state.
    ///
    /// Arming is tracked even while fencing is disabled, so enabling the toggle
    /// on a primary that has already served replicas fences immediately rather
    /// than granting a fresh grace period. Once latched this is a single relaxed
    /// load: the tracker is only consulted while still unarmed, and then through
    /// the existence-only [`ReplicationTrackerImpl::has_streaming_replica`],
    /// which allocates nothing (this path runs per write command, and previously
    /// built a `Vec` of replica snapshots every time).
    fn arm_if_streaming(&self) -> bool {
        if self.armed.load(Ordering::Relaxed) {
            return true;
        }
        if self.tracker.has_streaming_replica() {
            self.armed.store(true, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Whether a fence decision made *right now* would reject writes.
    fn fence_engaged(&self) -> bool {
        self.self_fence_enabled()
            && self.armed.load(Ordering::Relaxed)
            && self.count_fresh_streaming_replicas() == 0
    }
}

impl QuorumChecker for ReplicationQuorumChecker {
    fn has_quorum(&self) -> bool {
        // Cheapest gate first: one relaxed load. Nothing below it may cost a
        // tracker walk on the write path when fencing is disabled *and* the
        // checker is already armed.
        let fencing = self.self_fence_enabled();

        // Arming is latched regardless of the toggle (see `arm_if_streaming`),
        // so this runs either way — but it only touches the tracker while still
        // unarmed, and never allocates.
        let armed = self.arm_if_streaming();

        // Fencing disabled, or no replica has ever streamed: allow all writes.
        if !fencing || !armed {
            return true;
        }

        // Armed: require at least 1 fresh streaming replica
        self.count_fresh_streaming_replicas() >= 1
    }
}

impl WriteFenceReporter for ReplicationQuorumChecker {
    fn write_fence_reason(&self) -> Option<&'static str> {
        self.fence_engaged().then_some(FENCE_REASON)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::ReplicationTracker;
    use frogdb_replication::Phase;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn make_tracker() -> Arc<ReplicationTrackerImpl> {
        Arc::new(ReplicationTrackerImpl::new())
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    // FM-REPLICATION-041
    #[test]
    fn unarmed_allows_writes() {
        let tracker = make_tracker();
        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_secs(3));
        assert!(checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), None);
    }

    // FM-REPLICATION-041
    #[test]
    fn armed_with_fresh_replica_allows_writes() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_secs(3));
        assert!(checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), None);
    }

    // FM-REPLICATION-041
    #[test]
    fn armed_with_stale_replica_rejects_writes() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        // A realistic window with a backdated ACK, rather than a 1ns window plus
        // a sleep: the staleness is exact, the test is instant, and the window
        // under test is one an operator could actually configure.
        session.backdate_last_ack_for_test(Duration::from_secs(3600));
        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_millis(500));
        // The first call also arms the checker (a streaming replica exists), but
        // that replica's last ACK is already outside the window.
        assert!(!checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));
    }

    // FM-REPLICATION-041
    #[test]
    fn armed_with_no_replicas_rejects_writes() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker.clone(), true, Duration::from_secs(3));
        // Arm the checker
        assert!(checker.has_quorum());

        // Remove the replica
        tracker.unregister_replica(session.id());
        assert!(!checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));
    }

    // FM-REPLICATION-041
    #[test]
    fn arming_transition() {
        let tracker = make_tracker();
        let checker = ReplicationQuorumChecker::new(tracker.clone(), true, Duration::from_secs(3));

        // Not armed yet — quorum is true
        assert!(checker.has_quorum());
        assert!(!checker.is_armed());

        // Register a replica in Connecting state — still not armed
        let session = tracker.register_replica(addr(9001));
        assert!(checker.has_quorum());
        assert!(!checker.is_armed());

        // Move to Streaming — now it arms
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 0);
        assert!(checker.has_quorum());
        assert!(checker.is_armed());

        // Remove replica — armed stays true, quorum lost
        tracker.unregister_replica(session.id());
        assert!(!checker.has_quorum());
        assert!(checker.is_armed());

        // Role Demotion un-latches arming, so a later re-promotion does not
        // inherit a fence from replicas that are no longer this node's.
        checker.reset_arming();
        assert!(!checker.is_armed());
        assert!(checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), None);
    }

    // FM-REPLICATION-041
    /// Propagation truth for `replication-self-fence-on-replica-loss`: the
    /// toggle is read at the fence decision, so flipping it on a *live* checker
    /// changes the very next `has_quorum()` — no restart, no re-construction.
    #[test]
    fn self_fence_toggle_is_live() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        // Booted with fencing OFF: a lost replica must not fence writes. This is
        // also the shape a primary boots in today when the operator disables the
        // knob — a checker that is present but permissive.
        let checker = ReplicationQuorumChecker::new(tracker.clone(), false, Duration::from_secs(3));
        assert!(checker.has_quorum());
        tracker.unregister_replica(session.id());
        assert!(
            checker.has_quorum(),
            "fencing disabled: replica loss must not reject writes"
        );
        // Arming still happened while disabled, so enabling does not hand out a
        // fresh grace period.
        assert!(checker.is_armed());
        // Disabled means "not fencing", so nothing is reported as fenced even
        // though quorum is lost.
        assert_eq!(checker.write_fence_reason(), None);

        // CONFIG SET replication-self-fence-on-replica-loss yes.
        checker.set_self_fence_enabled(true);
        assert!(checker.self_fence_enabled());
        assert!(
            !checker.has_quorum(),
            "enabling the fence must reject writes on the same checker instance"
        );
        // ...and the fence is *attributable* rather than a silent CLUSTERDOWN.
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));

        // ...and back off again, same instance.
        checker.set_self_fence_enabled(false);
        assert!(checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), None);
    }

    // FM-REPLICATION-041
    /// Propagation truth for `replica-freshness-timeout-ms`: the window is
    /// loaded per freshness check, so shrinking it makes an already-registered
    /// replica stale (and widening it makes it fresh again) without a restart.
    #[test]
    fn freshness_timeout_is_live() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_secs(3600));
        assert!(checker.has_quorum());
        assert_eq!(checker.freshness_timeout(), Duration::from_secs(3600));

        // Let a measurable amount of ACK age accumulate, then CONFIG SET the
        // window below it.
        std::thread::sleep(Duration::from_millis(5));
        checker.set_freshness_timeout_ms(1);
        assert_eq!(checker.freshness_timeout(), Duration::from_millis(1));
        assert!(
            !checker.has_quorum(),
            "a shrunk freshness window must stale the replica on the next check"
        );
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));

        // Widening it again restores quorum on the same instance.
        checker.set_freshness_timeout_ms(3_600_000);
        assert!(checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), None);
    }

    /// A hand-rolled WARN collector. The crate carries no `tracing-subscriber`
    /// dev-dependency and adding one would edit the workspace lockfile, so the
    /// handful of `Subscriber` methods this needs are implemented directly.
    #[derive(Clone, Default)]
    struct WarnLog {
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl WarnLog {
        fn warnings(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    struct Fields(String);

    impl tracing::field::Visit for Fields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!(" {}={:?}", field.name(), value));
        }
    }

    impl tracing::Subscriber for WarnLog {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::WARN
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            self.events.lock().unwrap().push(fields.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// A checker that is armed, fencing-disabled, and has already lost its
    /// replica — the exact state in which turning the toggle on rejects writes
    /// on the spot.
    fn armed_and_quorum_lost() -> ReplicationQuorumChecker {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);
        let checker = ReplicationQuorumChecker::new(tracker.clone(), false, Duration::from_secs(3));
        // Fencing is off here, so `has_quorum()`'s return value is always true
        // regardless of arming — assert on `is_armed()` directly, which is the
        // only thing this call is actually meant to latch.
        checker.has_quorum();
        assert!(checker.is_armed(), "arms on the streaming replica");
        tracker.unregister_replica(session.id());
        checker
    }

    // FM-REPLICATION-041
    /// Turning the fence on while quorum is *already* lost starts rejecting
    /// writes immediately, so the transition is announced — otherwise the
    /// operator sees CLUSTERDOWN arrive out of nowhere and attributes it to the
    /// replicas rather than to their own `CONFIG SET`. The warning belongs to the
    /// off -> on edge: re-affirming a toggle that is already on changes nothing
    /// about the fence, and re-warning on every `CONFIG SET` would train the
    /// operator to ignore the line.
    #[test]
    fn enabling_the_fence_onto_a_lost_quorum_warns_once() {
        let checker = armed_and_quorum_lost();

        // Each call gets its own collector, so *which* call warned is asserted
        // rather than just how many warnings appeared in total.
        let edge = WarnLog::default();
        tracing::subscriber::with_default(edge.clone(), || {
            checker.set_self_fence_enabled(true);
        });
        let warnings = edge.warnings();
        assert_eq!(
            warnings.len(),
            1,
            "the off -> on edge is the call that announces the fence, got {warnings:?}"
        );

        // The same value again: no edge, no second warning.
        let repeat = WarnLog::default();
        tracing::subscriber::with_default(repeat.clone(), || {
            checker.set_self_fence_enabled(true);
        });
        assert!(
            repeat.warnings().is_empty(),
            "re-affirming a toggle that is already on changes nothing about the \
             fence, so it must not re-warn; got {:?}",
            repeat.warnings()
        );

        let warning = &warnings[0];
        assert!(
            warning.contains(FENCE_REASON),
            "the warning must name the fence reason, got {warning}"
        );
        assert!(
            warning.contains("CLUSTERDOWN"),
            "the warning must name what clients will see, got {warning}"
        );
        assert!(
            !checker.has_quorum(),
            "the warning describes a fence that is genuinely engaged"
        );
    }

    // FM-REPLICATION-041
    /// Enabling the fence on a *healthy* primary is a no-op for writes, so it
    /// must stay silent. A warning here would be a false alarm on the ordinary
    /// path — every operator who turns the knob on while their replicas are fine
    /// would be told their writes are now rejected, which they are not.
    #[test]
    fn enabling_the_fence_on_a_healthy_primary_is_silent() {
        let log = WarnLog::default();
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);
        let checker = ReplicationQuorumChecker::new(tracker, false, Duration::from_secs(3600));
        assert!(checker.has_quorum());

        tracing::subscriber::with_default(log.clone(), || {
            checker.set_self_fence_enabled(true);
        });

        assert!(
            log.warnings().is_empty(),
            "a fence that is not engaged must not be announced, got {:?}",
            log.warnings()
        );
        assert!(
            checker.has_quorum(),
            "the fresh replica still carries quorum"
        );
    }

    // FM-REPLICATION-041
    /// Turning the fence *off* never warns, whatever the quorum state: the
    /// transition can only make writes more permissive.
    #[test]
    fn disabling_the_fence_is_silent() {
        let log = WarnLog::default();
        let checker = armed_and_quorum_lost();
        checker.set_self_fence_enabled(true);
        assert!(!checker.has_quorum());

        tracing::subscriber::with_default(log.clone(), || {
            checker.set_self_fence_enabled(false);
            checker.set_self_fence_enabled(false);
        });

        assert!(
            log.warnings().is_empty(),
            "relaxing the fence is not an alarm, got {:?}",
            log.warnings()
        );
        assert!(checker.has_quorum());
    }

    // FM-REPLICATION-041
    /// An empty tracker is the shape a *replica* (or standalone) node carries
    /// from boot: the checker is installed but has nothing to fence on, so it is
    /// permissive until a promotion actually attracts replicas.
    #[test]
    fn empty_tracker_never_fences() {
        let checker = ReplicationQuorumChecker::new(make_tracker(), true, Duration::from_secs(3));
        for _ in 0..3 {
            assert!(checker.has_quorum());
        }
        assert!(!checker.is_armed());
        assert_eq!(checker.write_fence_reason(), None);
    }
}
