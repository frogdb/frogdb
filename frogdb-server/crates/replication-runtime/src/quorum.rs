//! Quorum checker for replication mode.
//!
//! Monitors replica ACK freshness via the ReplicationTrackerImpl. When no
//! replica has ACKed recently, `has_quorum()` returns false, causing the
//! server's write gate (`connection/guards.rs`) to reject writes with
//! `-SELFFENCE` — fencing the primary during partitions.
//!
//! Silence is the trigger, not absence: a replica that *departed cleanly* takes
//! the arming latch with it (FM-REPLICATION-062), so a decommissioned replica
//! leaves a writable primary while a lost one leaves a fenced one.

use frogdb_core::clock;
use frogdb_core::command::QuorumChecker;
use frogdb_core::metrics::WriteFenceReporter;
use frogdb_core::{ReplicationTrackerImpl, ack_is_fresh};
use frogdb_replication::ReplicaDeparture;
use frogdb_replication::view::FenceView;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// The `/status` + log wording for an active replica-loss fence. One constant so
/// the operator surface and the transition warning cannot drift apart.
const FENCE_REASON: &str = "replica quorum lost";

/// The wire refusal a fenced write is answered with.
///
/// Deliberately **not** `-CLUSTERDOWN`: this fence runs on every primary, most
/// of which are not cluster nodes, and an operator told their cluster is down
/// has been sent to diagnose a subsystem that may not even be running (issue
/// 30). The code names the mechanism and the string names the knob that turns
/// it off, because the refusal is otherwise indistinguishable from a bug.
const SELF_FENCE_REFUSAL: &str =
    "SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)";

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
                 are now rejected with SELFFENCE until a replica streams and ACKs again"
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

    /// This checker's contribution to the invariant projection.
    ///
    /// `INV-FENCE-1` and `INV-SESSION-3` are both claims about the arming latch
    /// held against the session registry, and neither is evaluable without it —
    /// `frogdb-replication` owns the catalog but not the checker, so before
    /// this seam existed the two entries were skipped everywhere. Read-only:
    /// three loads, no arming, no disarming, so capturing the projection can
    /// never be the thing that changes the decision it describes.
    pub fn view(&self) -> FenceView {
        FenceView {
            self_fence_enabled: self.self_fence_enabled(),
            armed: self.is_armed(),
            freshness_window: self.freshness_timeout(),
        }
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
            .filter(|r| ack_is_fresh(clock::elapsed(r.last_ack_time), freshness_timeout))
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

    /// Whether the fence may be dropped because the replica set left rather
    /// than went silent (FM-REPLICATION-062).
    ///
    /// Three conjuncts, each load-bearing:
    ///
    /// - nothing is registered as streaming, so a session whose link is dead
    ///   but whose teardown has not run — the partition this fence exists for —
    ///   still fences;
    /// - the last streaming departure was recorded, and
    /// - it was `Graceful`. An unknown departure (`None`) keeps the fence, so
    ///   the permissive answer is never the default.
    fn departed_cleanly(&self) -> bool {
        !self.tracker.has_streaming_replica()
            && self.tracker.last_streaming_departure() == Some(ReplicaDeparture::Graceful)
    }

    /// Drop the arming latch if the replica set left cleanly, and report
    /// whether writes may proceed.
    ///
    /// Un-latching rather than merely allowing the write matters: a primary
    /// whose replica was decommissioned is back to the never-had-a-replica
    /// state, so the *next* replica to stream re-arms it and a later silent
    /// loss fences again.
    fn disarm_if_departed_cleanly(&self) -> bool {
        if !self.departed_cleanly() {
            return false;
        }
        if self.armed.swap(false, Ordering::Relaxed) {
            tracing::info!(
                "Last streaming replica departed cleanly; replica-loss self-fence disarmed"
            );
        }
        true
    }

    /// Whether a fence decision made *right now* would reject writes.
    fn fence_engaged(&self) -> bool {
        self.self_fence_enabled()
            && self.armed.load(Ordering::Relaxed)
            && self.count_fresh_streaming_replicas() == 0
            // Read-only twin of the disarm step, so the reported state cannot
            // claim a fence the write gate would drop on its next call.
            && !self.departed_cleanly()
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
        if self.count_fresh_streaming_replicas() >= 1 {
            return true;
        }

        // Out of fresh replicas — but "lost my replica" and "my replica left"
        // are different states, and only the first one is what this fence
        // protects against (FM-REPLICATION-062).
        self.disarm_if_departed_cleanly()
    }

    fn quorum_lost_error(&self) -> &'static str {
        SELF_FENCE_REFUSAL
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
            warning.contains("SELFFENCE"),
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

    // FM-REPLICATION-041
    /// The refusal a fenced client receives names *this* mechanism and the knob
    /// that turns it off. It is deliberately not the cluster's `-CLUSTERDOWN`:
    /// that string sent operators of non-cluster primaries to debug a subsystem
    /// that was not even running (issue 30). Pinned literally, because the
    /// string is the entire diagnostic an operator gets.
    #[test]
    fn the_self_fence_names_itself_and_its_knob() {
        let checker = ReplicationQuorumChecker::new(make_tracker(), true, Duration::from_secs(3));

        assert_eq!(
            checker.quorum_lost_error(),
            "SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)"
        );
        // The three properties the wording carries, asserted as properties so a
        // reworded string still has to keep them.
        let error = checker.quorum_lost_error();
        assert!(
            error.starts_with("SELFFENCE "),
            "clients match on the leading error code, got {error}"
        );
        assert!(
            error.contains("self-fence-on-replica-loss"),
            "the refusal must name the knob that turns it off, got {error}"
        );
        assert!(
            !error.contains("CLUSTERDOWN"),
            "a replication fence must not claim the cluster is down, got {error}"
        );
        // The cluster's own wording is still available to the cluster checker,
        // and is a different string.
        assert_ne!(
            checker.quorum_lost_error(),
            frogdb_core::command::CLUSTER_DOWN_QUORUM_LOST
        );
    }

    /// Arm the checker off a streaming replica, then take that replica away with
    /// the given departure classification — the shape every disarm test starts
    /// from. `None` models a teardown that never recorded one.
    fn armed_then_departed(
        departure: Option<ReplicaDeparture>,
    ) -> (Arc<ReplicationTrackerImpl>, ReplicationQuorumChecker) {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker.clone(), true, Duration::from_secs(3));
        assert!(
            checker.has_quorum(),
            "a fresh streaming replica carries quorum"
        );
        assert!(checker.is_armed());

        tracker.unregister_replica(session.id());
        if let Some(departure) = departure {
            tracker.record_streaming_departure(departure);
        }
        (tracker, checker)
    }

    // FM-REPLICATION-062
    /// A replica that was *decommissioned* is not a replica that was lost. The
    /// fence exists to protect against a partition, and a clean departure is
    /// evidence there was none — so it takes the arming latch with it rather
    /// than merely being tolerated, and the primary stays writable.
    #[test]
    fn a_graceful_departure_disarms_the_fence() {
        let (_tracker, checker) = armed_then_departed(Some(ReplicaDeparture::Graceful));

        assert!(
            checker.has_quorum(),
            "a decommissioned replica must leave a writable primary"
        );
        assert!(
            !checker.is_armed(),
            "the latch is dropped, not just overridden: this primary is back to \
             the never-had-a-replica state"
        );
        assert_eq!(checker.write_fence_reason(), None);
    }

    // FM-REPLICATION-062
    /// The failure this fence exists for: the link died. An ungraceful
    /// departure leaves the latch armed and writes rejected, exactly as before
    /// the disarm seam existed.
    #[test]
    fn a_lost_departure_keeps_the_fence_armed() {
        let (_tracker, checker) = armed_then_departed(Some(ReplicaDeparture::Lost));

        assert!(!checker.has_quorum(), "a lost replica must fence writes");
        assert!(checker.is_armed());
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));
    }

    // FM-REPLICATION-062
    /// The unknown case is the fencing case. Any teardown path that forgets to
    /// classify itself — or a future one that has not been taught to — must fail
    /// closed, or the fence becomes a coin flip decided by which code path
    /// happened to run.
    #[test]
    fn an_unrecorded_departure_keeps_the_fence_armed() {
        let (tracker, checker) = armed_then_departed(None);

        assert_eq!(tracker.last_streaming_departure(), None);
        assert!(
            !checker.has_quorum(),
            "an unclassified departure must fence, not be assumed clean"
        );
        assert!(checker.is_armed());
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));
    }

    // FM-REPLICATION-062
    /// Silence, not absence, is what fences — and the disarm must not weaken
    /// that. A replica whose link is dead but whose session is still registered
    /// is the partition case: a *stale* graceful record from some earlier
    /// replica must not be read as "this one left cleanly".
    #[test]
    fn a_registered_but_silent_replica_still_fences() {
        let tracker = make_tracker();
        let first = tracker.register_replica(addr(9001));
        first.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(first.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker.clone(), true, Duration::from_secs(3));
        assert!(checker.has_quorum());

        // An earlier replica left cleanly...
        tracker.unregister_replica(first.id());
        tracker.record_streaming_departure(ReplicaDeparture::Graceful);
        assert!(checker.has_quorum());

        // ...and a new one streamed, then went silent without ever tearing down.
        let second = tracker.register_replica(addr(9002));
        second.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(second.id(), 200);
        second.backdate_last_ack_for_test(Duration::from_secs(3600));

        assert!(
            !checker.has_quorum(),
            "a registered-but-stale replica is the partition this fence exists \
             for; the stale graceful record must not excuse it"
        );
        assert!(checker.is_armed());
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));
    }

    // FM-REPLICATION-062
    /// Disarming is a return to the initial state, not a permanent opt-out: the
    /// next replica to stream re-arms the checker, so a later ungraceful loss
    /// fences again. A disarm that latched the other way would silently disable
    /// fencing for the lifetime of the process.
    #[test]
    fn a_disarmed_fence_re_arms_on_the_next_streaming_replica() {
        let (tracker, checker) = armed_then_departed(Some(ReplicaDeparture::Graceful));
        assert!(checker.has_quorum());
        assert!(!checker.is_armed());

        let next = tracker.register_replica(addr(9002));
        next.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(next.id(), 200);
        assert!(checker.has_quorum());
        assert!(
            checker.is_armed(),
            "the new streaming replica re-arms the fence"
        );

        // This one dies rather than leaving.
        tracker.unregister_replica(next.id());
        tracker.record_streaming_departure(ReplicaDeparture::Lost);
        assert!(!checker.has_quorum());
        assert_eq!(checker.write_fence_reason(), Some(FENCE_REASON));
    }
}
