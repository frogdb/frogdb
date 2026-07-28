//! The replication offset contract, owned in one place.
//!
//! The replication offset is a single logical quantity — "how many bytes of the
//! command stream have been produced/consumed" — but it has three backing
//! homes:
//!
//! - the **live** primary write position (owned here as [`OffsetCoordinator`]'s
//!   `live` atomic, advanced by the single [`OffsetCoordinator::advance`] gate),
//! - the **per-replica acked** positions (the tracker's session registry,
//!   aggregated for WAIT / safety), and
//! - the **persisted** position (`ReplicationState::replication_offset`,
//!   reconciled at save points).
//!
//! Before this module each caller had to know *which* home answers *which*
//! question — a rule that lived only in prose comments and leaked into call
//! sites (e.g. the PSYNC window check had to fetch the live offset itself and
//! thread it into [`ReplicationState::window_contains`]). [`OffsetCoordinator`]
//! pulls those homes behind one seam so callers ask questions in the vocabulary
//! of *replication*, never in the vocabulary of *which field*.

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

use frogdb_types::ReplicationTracker;

use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;

/// Single owner of the replication offset contract.
///
/// Invariant (the whole reason this type exists): the offset is measured in
/// **RESP command-stream bytes** — the bytes a replica would have to replay to
/// reach this position. Transport framing (the 20-byte [`ReplicationFrame`]
/// header) is NOT part of the offset. Primary and replica advance by the SAME
/// unit ([`ReplicationFrame::stream_advance`], reached via [`Self::advance`] on
/// the primary and [`Self::frame_advance`] on the replica), so an ACK is
/// directly comparable to the live offset.
///
/// The coordinator OWNS the live write position: the `live` atomic is its
/// canonical home, and it is the single vendor of the shared handle the cluster
/// bus reads for HealthProbe responses (see [`Self::shared_offset`]). The
/// tracker holds a *borrowed* clone of the same atomic purely for its
/// INFO/ROLE read + lag accessors; the coordinator, not the tracker, advances
/// it and hands it to the bus.
pub struct OffsetCoordinator {
    /// Live primary write position — the offset's canonical home. Advanced only
    /// through [`Self::advance`]; vended to the cluster bus by
    /// [`Self::shared_offset`]. The tracker borrows a clone of this Arc for its
    /// read/lag accessors, but this type owns the advance and the vend.
    live: Arc<AtomicU64>,
    /// Per-replica acked offsets + the WAIT ack-notification channel. The
    /// tracker's session registry; the coordinator borrows it for aggregation
    /// and ack ingestion only.
    tracker: Arc<ReplicationTrackerImpl>,
    /// Persisted identity + offset, reconciled at save points.
    state: Arc<RwLock<ReplicationState>>,
}

impl OffsetCoordinator {
    /// Build a coordinator over an existing tracker and persisted state.
    ///
    /// The coordinator adopts the tracker's offset atomic as its canonical
    /// `live` handle: from here on the coordinator owns the advance and vends
    /// the cluster-bus handle, while the tracker retains a borrowed clone only
    /// for its INFO/ROLE read + lag accessors.
    pub fn new(tracker: Arc<ReplicationTrackerImpl>, state: Arc<RwLock<ReplicationState>>) -> Self {
        let live = tracker.offset_handle();
        Self {
            live,
            tracker,
            state,
        }
    }

    /// The payload-bytes advance unit, defined EXACTLY ONCE. The offset counts
    /// RESP command-stream bytes; the [`ReplicationFrame`] header is transport,
    /// not stream. Both the primary advance gate ([`Self::advance`]) and the
    /// replica ingest path ([`Self::frame_advance`]) measure by this, so the two
    /// ends can never drift.
    #[inline]
    fn advance_unit(payload: &Bytes) -> u64 {
        payload.len() as u64
    }

    /// The ONE advance gate. The primary calls this with the RESP payload it is
    /// about to frame and broadcast; it advances the owned `live` atomic and
    /// returns the new offset, which the caller stamps into the frame's sequence
    /// field so every frame self-describes its end offset.
    ///
    /// The primary no longer hands a raw `.len()` to a `payload_len: u64`
    /// parameter a caller could get wrong — the unit is applied here, once.
    pub fn advance(&self, payload: &Bytes) -> u64 {
        let n = Self::advance_unit(payload);
        self.live.fetch_add(n, Ordering::Release) + n
    }

    /// Replica-side spelling of the advance *unit*. The replica advances its own
    /// live offset (it has no [`OffsetCoordinator`] — that type is
    /// primary-only), but must count by the SAME unit as [`Self::advance`]. A
    /// thin delegator to [`ReplicationFrame::stream_advance`], the neutral home
    /// of the unit, so the two ends cannot use different counts.
    #[inline]
    pub fn frame_advance(frame: &ReplicationFrame) -> u64 {
        frame.stream_advance()
    }

    /// The one true live offset. Replaces every `tracker.current_offset()` AND
    /// every `state.offset_at_save` read for "where is the stream now".
    pub fn current(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// The cluster bus's HealthProbe handle, vended by the OWNER of the atomic.
    /// The tracker no longer exposes a public `shared_offset()`; the shared
    /// handle is vended here so there is a single vendor.
    pub fn shared_offset(&self) -> Arc<AtomicU64> {
        self.live.clone()
    }

    /// Genuine durability ACK from a replica's `REPLCONF ACK`. Notifies WAIT
    /// waiters (delegates to the tracker's session registry + ack channel).
    pub fn ingest_replica_ack(&self, replica_id: u64, acked: u64) {
        self.tracker.record_ack(replica_id, acked);
    }

    /// Seed a resumed replica's *stream position* after backlog replay. Same
    /// monotonic session store as an ACK, but a distinct verb: this is
    /// primary-side bookkeeping ("where this replica resumed"), not a durability
    /// confirmation — [`Self::ingest_replica_ack`] is the only path driven by a
    /// value the replica itself sent. Despite that distinction in provenance,
    /// seeding notifies WAIT waiters exactly like a genuine ACK whenever it
    /// advances the acked offset (round-7 follow-up to proposal 57): a replica
    /// that reconnects via partial resync already at/past a blocked WAIT's
    /// target must wake it immediately rather than park for up to a
    /// spontaneous-ACK cadence (~1s). A stale/duplicate seed that does not
    /// advance the offset does not notify.
    pub fn seed_replica_position(&self, replica_id: u64, position: u64) {
        self.tracker.seed_acked_position(replica_id, position);
    }

    /// Minimum acked offset across streaming replicas (for WAIT / safety).
    pub fn min_acked(&self) -> Option<u64> {
        self.tracker.min_acked_offset()
    }

    /// Can a partial resync be *offset-wise* continued for this id/offset?
    /// Reads its OWN live offset — the caller no longer fetches and threads it.
    /// (Replay availability is gated separately; see proposal 14.)
    pub async fn can_serve_partial_sync(&self, requested_id: &str, requested_offset: u64) -> bool {
        let current = self.current();
        let state = self.state.read().await;
        state.window_contains(requested_id, requested_offset, current)
    }

    /// Reconcile the persisted offset up to the live offset (monotonic) and
    /// return a state snapshot to save. Absorbs `save_state`'s offset logic.
    ///
    /// The tracker only ever advances past the loaded offset, so the reconcile
    /// is monotonic; the guard keeps a save from ever moving the offset back.
    pub async fn reconcile_for_persist(&self) -> ReplicationState {
        let offset = self.current();
        let mut state = self.state.write().await;
        if offset > state.offset_at_save {
            state.offset_at_save = offset;
        }
        state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::serialize_command_to_resp;
    use crate::replica_session::Phase;
    use bytes::Bytes;

    fn coordinator() -> OffsetCoordinator {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        OffsetCoordinator::new(tracker, state)
    }

    /// A RESP-ish payload of exactly `n` bytes, for driving the advance gate.
    fn payload(n: usize) -> Bytes {
        Bytes::from(vec![b'x'; n])
    }

    #[test]
    fn frame_advance_counts_payload_not_header() {
        let payload = Bytes::from_static(b"*1\r\n$4\r\nPING\r\n");
        let frame = ReplicationFrame::new(0, payload.clone());
        // The advance is the payload length only — the frame header is
        // transport and must NOT be counted.
        assert_eq!(
            OffsetCoordinator::frame_advance(&frame),
            payload.len() as u64
        );
        assert_ne!(
            OffsetCoordinator::frame_advance(&frame),
            frame.encoded_size() as u64
        );
    }

    #[test]
    fn advance_is_cumulative_and_visible_via_current() {
        let coord = coordinator();
        assert_eq!(coord.current(), 0);
        assert_eq!(coord.advance(&payload(10)), 10);
        assert_eq!(coord.current(), 10);
        assert_eq!(coord.advance(&payload(5)), 15);
        assert_eq!(coord.current(), 15);
    }

    #[test]
    fn advance_and_frame_advance_agree_on_the_single_unit() {
        // The one gate: `advance(&payload)` and `frame_advance(&frame_of(payload))`
        // must return the same count, pinning the payload-bytes unit to a single
        // definition rather than two `.len()` sites that agree by convention.
        let coord = coordinator();
        let resp = serialize_command_to_resp("SET", &[Bytes::from("k"), Bytes::from("v")]);
        let advanced = coord.advance(&resp);
        let frame = ReplicationFrame::new(advanced, resp.clone());
        assert_eq!(advanced, OffsetCoordinator::frame_advance(&frame));
        assert_eq!(advanced, resp.len() as u64);
    }

    #[test]
    fn broadcast_and_ingest_count_the_same_unit() {
        // The primary advances by the RESP payload it serialized; the replica
        // advances by `frame_advance` of the frame it received. They must agree.
        let coord = coordinator();
        let resp = serialize_command_to_resp("SET", &[Bytes::from("k"), Bytes::from("v")]);
        let primary_offset = coord.advance(&resp);
        let frame = ReplicationFrame::new(primary_offset, resp);
        assert_eq!(OffsetCoordinator::frame_advance(&frame), primary_offset);
    }

    #[test]
    fn shared_offset_observes_advances_through_the_owner() {
        // The cluster bus reads the handle vended by the coordinator (not the
        // tracker). A read through that handle must observe every `advance`, so
        // the bus can never see a stale or separate atomic.
        let coord = coordinator();
        let bus_handle = coord.shared_offset();
        assert_eq!(bus_handle.load(Ordering::Acquire), 0);
        coord.advance(&payload(42));
        assert_eq!(bus_handle.load(Ordering::Acquire), 42);
        assert_eq!(bus_handle.load(Ordering::Acquire), coord.current());
    }

    #[test]
    fn ingest_replica_ack_advances_and_notifies_wait() {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let coord = OffsetCoordinator::new(tracker.clone(), state);
        let session = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
        let mut acks = tracker.subscribe_acks();

        coord.ingest_replica_ack(session.id(), 100);
        assert_eq!(session.acked_offset(), 100);
        // A genuine durability ACK notifies WAIT waiters.
        assert_eq!(acks.try_recv().unwrap(), (session.id(), 100));
    }

    #[test]
    fn seed_replica_position_notifies_wait_on_advance() {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let coord = OffsetCoordinator::new(tracker.clone(), state);
        let session = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
        let mut acks = tracker.subscribe_acks();

        // Seeding the resumed position advances the monotonic session store (so
        // WAIT counting + the lag monitor start from the resumed position) AND
        // notifies WAIT waiters exactly like a genuine ACK, since a reconnecting
        // replica may already be at/past a blocked WAIT's target.
        coord.seed_replica_position(session.id(), 100);
        assert_eq!(session.acked_offset(), 100);
        assert_eq!(tracker.count_acked(100), 1);
        assert_eq!(
            acks.try_recv().unwrap(),
            (session.id(), 100),
            "an advancing seed must notify WAIT waiters"
        );

        // A stale/duplicate seed does not advance the offset and does not notify.
        coord.seed_replica_position(session.id(), 50);
        assert_eq!(session.acked_offset(), 100);
        assert!(
            acks.try_recv().is_err(),
            "a stale/duplicate seed must not notify WAIT waiters"
        );
    }

    #[test]
    fn min_acked_aggregates_streaming_replicas() {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let coord = OffsetCoordinator::new(tracker.clone(), state);
        assert_eq!(coord.min_acked(), None);

        let s1 = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        let s2 = tracker.register_replica("127.0.0.1:6381".parse().unwrap());
        s1.force_phase_for_test(Phase::Streaming);
        s2.force_phase_for_test(Phase::Streaming);
        coord.ingest_replica_ack(s1.id(), 100);
        coord.ingest_replica_ack(s2.id(), 250);
        assert_eq!(coord.min_acked(), Some(100));
    }

    #[tokio::test]
    async fn can_serve_partial_sync_uses_its_own_live_offset() {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let coord = OffsetCoordinator::new(tracker.clone(), state.clone());
        let repl_id = state.read().await.replication_id.clone();

        // Advance the live offset through the coordinator; the window check must
        // read this value itself rather than receiving it as a parameter.
        coord.advance(&payload(1000));

        assert!(coord.can_serve_partial_sync(&repl_id, 500).await);
        assert!(coord.can_serve_partial_sync(&repl_id, 1000).await);
        // Past the live head — outside the window.
        assert!(!coord.can_serve_partial_sync(&repl_id, 1001).await);
        // Unknown replication id — never in the window.
        assert!(!coord.can_serve_partial_sync("unknown_id", 500).await);
    }

    #[tokio::test]
    async fn can_serve_partial_sync_honours_secondary_failover_window() {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let coord = OffsetCoordinator::new(tracker.clone(), state.clone());

        let old_id = {
            let mut s = state.write().await;
            let old = s.replication_id.clone();
            // Freeze the failover boundary from the live offset (1000).
            s.new_replication_id(1000);
            old
        };

        // The secondary window is the frozen failover boundary (1000), checked
        // independently of the live offset (still 0 here).
        assert!(coord.can_serve_partial_sync(&old_id, 500).await);
        assert!(coord.can_serve_partial_sync(&old_id, 1000).await);
        assert!(!coord.can_serve_partial_sync(&old_id, 1001).await);
    }

    #[tokio::test]
    async fn reconcile_for_persist_is_monotonic() {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let coord = OffsetCoordinator::new(tracker.clone(), state.clone());

        coord.advance(&payload(750));
        let snapshot = coord.reconcile_for_persist().await;
        assert_eq!(snapshot.offset_at_save, 750);
        // The persisted field was updated in place too.
        assert_eq!(state.read().await.offset_at_save, 750);

        // A reconcile never moves the offset backwards: if the persisted field
        // is already ahead (e.g. seeded from staged metadata), it is preserved.
        state.write().await.offset_at_save = 5000;
        let snapshot = coord.reconcile_for_persist().await;
        assert_eq!(snapshot.offset_at_save, 5000);
    }
}
