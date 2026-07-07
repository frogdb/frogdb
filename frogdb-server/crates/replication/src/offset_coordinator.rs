//! The replication offset contract, owned in one place.
//!
//! The replication offset is a single logical quantity — "how many bytes of the
//! command stream have been produced/consumed" — but it has three backing
//! homes:
//!
//! - the **live** primary write position (the tracker's atomic, advanced by
//!   `broadcast_command`),
//! - the **per-replica acked** positions (aggregated for WAIT / safety), and
//! - the **persisted** position (`ReplicationState::replication_offset`,
//!   reconciled at save points).
//!
//! Before this module each caller had to know *which* home answers *which*
//! question — a rule that lived only in prose comments and leaked into call
//! sites (e.g. the PSYNC window check had to fetch the live offset itself and
//! thread it into [`ReplicationState::window_contains`]). [`OffsetCoordinator`]
//! pulls those homes behind one seam so callers ask questions in the vocabulary
//! of *replication*, never in the vocabulary of *which field*.

use std::sync::Arc;
use tokio::sync::RwLock;

use frogdb_types::ReplicationTracker;

use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;

/// Single owner of the replication offset contract.
///
/// Invariant (the whole reason this type exists): the offset is measured in
/// **RESP command-stream bytes** — the bytes a replica would have to replay to
/// reach this position. Transport framing (the 18-byte [`ReplicationFrame`]
/// header) is NOT part of the offset. Primary and replica advance by the SAME
/// unit (see [`Self::frame_advance`]), so an ACK is directly comparable to the
/// live offset.
///
/// The coordinator deepens the existing seam rather than adding an adapter
/// layer: the [`ReplicationTrackerImpl`] already owns the live atomic and the
/// per-replica ack registry, so the coordinator borrows it for both the live
/// offset and ack aggregation, and additionally pulls the persisted
/// [`ReplicationState`] under the same roof.
pub struct OffsetCoordinator {
    /// Live write position + per-replica ack registry. The tracker owns the
    /// `Arc<AtomicU64>` shared with the cluster bus for HealthProbe responses.
    tracker: Arc<ReplicationTrackerImpl>,
    /// Persisted identity + offset, reconciled at save points.
    state: Arc<RwLock<ReplicationState>>,
}

impl OffsetCoordinator {
    /// Build a coordinator over an existing tracker and persisted state.
    pub fn new(tracker: Arc<ReplicationTrackerImpl>, state: Arc<RwLock<ReplicationState>>) -> Self {
        Self { tracker, state }
    }

    /// Canonical advance for one outbound/inbound frame. ONE definition of the
    /// unit, shared by primary broadcast and replica ingest, so the two ends
    /// can never drift. The header is transport, not stream.
    #[inline]
    pub fn frame_advance(frame: &ReplicationFrame) -> u64 {
        frame.payload.len() as u64
    }

    /// Primary side: record that `payload_len` RESP bytes were broadcast and
    /// return the new live offset. The frame's sequence field is stamped with
    /// this value by the caller, so every frame self-describes its end offset.
    pub fn advance_broadcast(&self, payload_len: u64) -> u64 {
        self.tracker.increment_offset(payload_len)
    }

    /// The one true live offset. Replaces every `tracker.current_offset()` AND
    /// every `state.replication_offset` read for "where is the stream now".
    pub fn current(&self) -> u64 {
        self.tracker.current_offset()
    }

    /// Record an ACK from a replica (delegates to the registry, notifies WAIT).
    pub fn record_replica_ack(&self, replica_id: u64, acked: u64) {
        self.tracker.record_ack(replica_id, acked);
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
        if offset > state.replication_offset {
            state.replication_offset = offset;
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
    fn advance_broadcast_is_cumulative_and_visible_via_current() {
        let coord = coordinator();
        assert_eq!(coord.current(), 0);
        assert_eq!(coord.advance_broadcast(10), 10);
        assert_eq!(coord.current(), 10);
        assert_eq!(coord.advance_broadcast(5), 15);
        assert_eq!(coord.current(), 15);
    }

    #[test]
    fn broadcast_and_ingest_count_the_same_unit() {
        // The primary advances by the RESP payload it serialized; the replica
        // advances by `frame_advance` of the frame it received. They must agree.
        let coord = coordinator();
        let resp = serialize_command_to_resp("SET", &[Bytes::from("k"), Bytes::from("v")]);
        let primary_offset = coord.advance_broadcast(resp.len() as u64);
        let frame = ReplicationFrame::new(primary_offset, resp);
        assert_eq!(OffsetCoordinator::frame_advance(&frame), primary_offset);
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
        coord.record_replica_ack(s1.id(), 100);
        coord.record_replica_ack(s2.id(), 250);
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
        coord.advance_broadcast(1000);

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
            s.replication_offset = 1000;
            let old = s.replication_id.clone();
            s.new_replication_id();
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

        coord.advance_broadcast(750);
        let snapshot = coord.reconcile_for_persist().await;
        assert_eq!(snapshot.replication_offset, 750);
        // The persisted field was updated in place too.
        assert_eq!(state.read().await.replication_offset, 750);

        // A reconcile never moves the offset backwards: if the persisted field
        // is already ahead (e.g. seeded from staged metadata), it is preserved.
        state.write().await.replication_offset = 5000;
        let snapshot = coord.reconcile_for_persist().await;
        assert_eq!(snapshot.replication_offset, 5000);
    }
}
