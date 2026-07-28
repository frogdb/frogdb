//! The replica-side live offset, owned in one place.

use crate::frame::ReplicationFrame;
use crate::offset_coordinator::OffsetCoordinator;
use crate::state::ReplicationState;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// The Replica-side live applied offset — the mirror of the Primary's
/// [`OffsetCoordinator`]'s `live` atomic: the single home of "how far this
/// Replica has applied", advanced per ingested frame, reconciled into
/// [`ReplicationState::offset_at_save`] at save points, and the single vendor of
/// the shared handle the cluster bus (HealthProbe) / INFO read.
///
/// The owner does **not** mint a second atomic: it takes over maintenance of the
/// `shared_offset` atomic the scattered call sites updated before, so the
/// cluster-bus handle identity is unchanged.
#[derive(Clone)]
pub struct ReplicaOffset {
    live: Arc<AtomicU64>,
    state: Arc<RwLock<ReplicationState>>,
}

impl ReplicaOffset {
    /// Adopt the Replica's shared `live` atomic — the one read by the cluster bus
    /// / INFO. The atomic is seeded from the persisted `offset_at_save` where it
    /// is minted (the handler at boot, a test fixture in unit tests), so this
    /// constructor never re-stores: a fresh connection attempt or a save point
    /// adopts the atomic that already holds the applied offset, and reconnecting
    /// can never rewind the live head to the lagging persisted field.
    pub fn new(state: Arc<RwLock<ReplicationState>>, live: Arc<AtomicU64>) -> Self {
        Self { live, state }
    }

    /// Advance by one ingested frame's payload unit — the SAME unit as the
    /// Primary's advance (via [`OffsetCoordinator::frame_advance`]), so a Replica
    /// ACK stays directly comparable to the Primary's live head. Returns the new
    /// live offset (what the streaming path ACKs).
    pub fn frame_advance(&self, frame: &ReplicationFrame) -> u64 {
        let n = OffsetCoordinator::frame_advance(frame);
        self.live.fetch_add(n, Ordering::Release) + n
    }

    /// The live applied position. Replaces `state.offset_at_save` reads on the
    /// Replica ingest / ACK / reconnect-PSYNC path.
    pub fn current(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// Adopt a fresh stream position on FULLRESYNC / staged-checkpoint install.
    /// Replaces the direct `state.replication_offset = new_offset` writes.
    pub fn reset_to(&self, offset: u64) {
        self.live.store(offset, Ordering::Release);
    }

    /// Reconcile [`offset_at_save`] up to the live head for persistence —
    /// monotone-guarded, symmetric to [`OffsetCoordinator::reconcile_for_persist`].
    /// On the Replica this preserves the persist-what-you-applied semantic: the
    /// net persisted value is the live applied offset at save time.
    ///
    /// [`offset_at_save`]: ReplicationState::offset_at_save
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
    use bytes::Bytes;

    fn state_with_save(offset_at_save: u64) -> Arc<RwLock<ReplicationState>> {
        let mut s = ReplicationState::new();
        s.offset_at_save = offset_at_save;
        Arc::new(RwLock::new(s))
    }

    /// Mint a live atomic seeded from `seed`, mirroring how the handler seeds
    /// the atomic from the persisted `offset_at_save` when it mints it.
    fn seeded(seed: u64) -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(seed))
    }

    fn frame_of(payload: &'static [u8]) -> ReplicationFrame {
        ReplicationFrame::new(0, Bytes::from_static(payload))
    }

    #[tokio::test]
    async fn new_adopts_the_seeded_live_atomic() {
        let offsets = ReplicaOffset::new(state_with_save(500), seeded(500));
        assert_eq!(offsets.current(), 500);
    }

    #[tokio::test]
    async fn frame_advance_advances_live_and_ack_equals_summed_payload() {
        // Replica ACK equals the live applied offset: the sum of payload units.
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0));
        assert_eq!(offsets.frame_advance(&frame_of(b"hello")), 5); // 5
        assert_eq!(offsets.frame_advance(&frame_of(b"world!")), 11); // +6
        assert_eq!(offsets.current(), 11);
    }

    #[tokio::test]
    async fn frame_advance_counts_payload_not_header() {
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0));
        let frame = frame_of(b"*1\r\n$4\r\nPING\r\n");
        let advanced = offsets.frame_advance(&frame);
        assert_eq!(advanced, frame.payload.len() as u64);
        assert_ne!(advanced, frame.encoded_size() as u64);
    }

    #[tokio::test]
    async fn current_reports_live_not_persisted_offset_at_save() {
        // The reconnect-PSYNC hazard: offset_at_save lags the live applied head
        // between save points. `current()` must report the live head (N), never
        // the persisted save-point value (M < N) — otherwise a reconnect would
        // ask the primary to resume from behind where the replica has applied.
        let st = state_with_save(100); // persisted save-point M = 100
        let offsets = ReplicaOffset::new(st.clone(), seeded(100));
        // Apply past the save point.
        offsets.frame_advance(&frame_of(b"aaaaaaaaaa")); // +10 -> live N = 110
        assert_eq!(offsets.current(), 110);
        // The persisted field still lags at the save point.
        assert_eq!(st.read().await.offset_at_save, 100);
    }

    #[tokio::test]
    async fn reconcile_for_persist_is_monotonic_and_persists_what_was_applied() {
        let st = state_with_save(0);
        let offsets = ReplicaOffset::new(st.clone(), seeded(0));

        offsets.frame_advance(&ReplicationFrame::new(0, Bytes::from(vec![b'x'; 750])));
        // Live advanced; offset_at_save still lags until reconcile.
        assert_eq!(offsets.current(), 750);
        assert_eq!(st.read().await.offset_at_save, 0);

        let snapshot = offsets.reconcile_for_persist().await;
        // Persist-what-you-applied: the persisted value is the live applied head.
        assert_eq!(snapshot.offset_at_save, 750);
        assert_eq!(st.read().await.offset_at_save, 750);

        // A reconcile never moves the offset backwards.
        st.write().await.offset_at_save = 5000;
        let snapshot = offsets.reconcile_for_persist().await;
        assert_eq!(snapshot.offset_at_save, 5000);
    }

    #[tokio::test]
    async fn reset_to_adopts_a_fresh_position_visible_through_the_shared_atomic() {
        // The cluster bus reads the adopted atomic directly; every mutation
        // through the owner must be visible there in lockstep.
        let shared = seeded(0);
        let offsets = ReplicaOffset::new(state_with_save(0), shared.clone());
        offsets.frame_advance(&frame_of(b"aaaaaaaaaa")); // 10
        assert_eq!(offsets.current(), 10);
        assert_eq!(shared.load(Ordering::Acquire), 10);

        // A fresh FULLRESYNC resets the live position (possibly backward) and it
        // is visible through the adopted cluster-bus atomic.
        offsets.reset_to(3);
        assert_eq!(offsets.current(), 3);
        assert_eq!(shared.load(Ordering::Acquire), 3);
    }
}
