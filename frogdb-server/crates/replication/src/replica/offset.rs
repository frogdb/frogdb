//! The replica-side offset lifecycle, owned in one place.

use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// Single owner of the *replica-side* offset lifecycle — symmetric in role to
/// the primary's [`crate::offset_coordinator::OffsetCoordinator`], though not in
/// mechanism: here the canonical offset home is
/// [`ReplicationState::replication_offset`] (a `u64` behind the shared
/// `RwLock`) and `mirror` is the cluster-bus HealthProbe atomic that must move
/// in lockstep with it. Every runtime ingest path routes its offset change
/// through this type so the mirror can never drift from the canonical field —
/// the failure detector scores replicas for failover off exactly that mirror,
/// so a stale mirror would mean a data-loss-suboptimal replica gets promoted.
pub struct ReplicaOffset {
    state: Arc<RwLock<ReplicationState>>,
    mirror: Option<Arc<AtomicU64>>,
}

impl ReplicaOffset {
    /// Adopt the state handle and the optional HealthProbe mirror. The mirror is
    /// `None` outside cluster mode (no HealthProbe atomic exists).
    pub fn new(state: Arc<RwLock<ReplicationState>>, mirror: Option<Arc<AtomicU64>>) -> Self {
        Self { state, mirror }
    }

    /// Streaming ingest: advance the canonical offset by the frame's stream unit
    /// and publish the new value to the mirror in the same step. Returns the new
    /// offset so the caller can stamp an ACK / trace.
    pub async fn advance(&self, frame: &ReplicationFrame) -> u64 {
        let offset = {
            let mut state = self.state.write().await;
            state.increment_offset(frame.stream_advance());
            state.replication_offset
        };
        if let Some(ref mirror) = self.mirror {
            mirror.store(offset, Ordering::Release);
        }
        offset
    }

    /// Full-sync / checkpoint: adopt an authoritative `(replication_id, offset)`
    /// sync point and publish `offset` to the mirror, so the mirror can never
    /// reflect a half-updated identity.
    pub async fn reset_to(&self, replication_id: String, offset: u64) {
        {
            let mut state = self.state.write().await;
            state.replication_id = replication_id;
            state.replication_offset = offset;
        }
        if let Some(ref mirror) = self.mirror {
            mirror.store(offset, Ordering::Release);
        }
    }

    /// The current stream offset (for the spontaneous ACK tick and PSYNC
    /// resume). Reads the canonical field, never the mirror.
    pub async fn current(&self) -> u64 {
        self.state.read().await.replication_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn state() -> Arc<RwLock<ReplicationState>> {
        Arc::new(RwLock::new(ReplicationState::new()))
    }

    fn frame_of(payload: &'static [u8]) -> ReplicationFrame {
        ReplicationFrame::new(0, Bytes::from_static(payload))
    }

    #[tokio::test]
    async fn advance_mirrors_in_lockstep_and_accumulates() {
        let mirror = Arc::new(AtomicU64::new(0));
        let offsets = ReplicaOffset::new(state(), Some(mirror.clone()));

        // A 5-byte payload advances the canonical field and the mirror together.
        let payload = b"hello"; // 5 bytes
        let after = offsets.advance(&frame_of(payload)).await;
        assert_eq!(after, 5);
        assert_eq!(offsets.current().await, 5);
        assert_eq!(mirror.load(Ordering::Acquire), 5);
        assert_eq!(mirror.load(Ordering::Acquire), offsets.current().await);

        // Cumulative + monotone.
        let after = offsets.advance(&frame_of(b"world!")).await; // 6 bytes
        assert_eq!(after, 11);
        assert_eq!(offsets.current().await, 11);
        assert_eq!(mirror.load(Ordering::Acquire), 11);
    }

    #[tokio::test]
    async fn advance_counts_payload_not_header() {
        let offsets = ReplicaOffset::new(state(), Some(Arc::new(AtomicU64::new(0))));
        let payload = b"*1\r\n$4\r\nPING\r\n";
        let frame = frame_of(payload);
        let advanced = offsets.advance(&frame).await;
        assert_eq!(advanced, payload.len() as u64);
        // The transport header must NOT be counted.
        assert_ne!(advanced, frame.encoded_size() as u64);
    }

    #[tokio::test]
    async fn reset_to_sets_id_offset_and_mirror_atomically() {
        let mirror = Arc::new(AtomicU64::new(0));
        let st = state();
        let offsets = ReplicaOffset::new(st.clone(), Some(mirror.clone()));

        let id = "0123456789abcdef0123456789abcdef01234567".to_string();
        offsets.reset_to(id.clone(), 4242).await;

        assert_eq!(st.read().await.replication_id, id);
        assert_eq!(st.read().await.replication_offset, 4242);
        assert_eq!(mirror.load(Ordering::Acquire), 4242);
    }

    #[tokio::test]
    async fn reset_to_can_move_backward_while_advance_does_not_underflow() {
        let offsets = ReplicaOffset::new(state(), Some(Arc::new(AtomicU64::new(0))));
        offsets.advance(&frame_of(b"aaaaaaaaaa")).await; // 10
        assert_eq!(offsets.current().await, 10);

        // A fresh full sync legitimately resets to a lower offset.
        let id = "0123456789abcdef0123456789abcdef01234567".to_string();
        offsets.reset_to(id, 3).await;
        assert_eq!(offsets.current().await, 3);
    }

    #[tokio::test]
    async fn none_mirror_is_a_noop_mirror() {
        // Non-cluster mode: no HealthProbe atomic. Canonical field still updates.
        let offsets = ReplicaOffset::new(state(), None);
        let after = offsets.advance(&frame_of(b"abc")).await;
        assert_eq!(after, 3);
        assert_eq!(offsets.current().await, 3);

        let id = "0123456789abcdef0123456789abcdef01234567".to_string();
        offsets.reset_to(id, 99).await;
        assert_eq!(offsets.current().await, 99);
    }

    #[tokio::test]
    async fn shared_offset_equals_current_after_every_advance() {
        // Regression guard for the failure detector: the mirror the HealthProbe
        // returns (and `trigger_auto_failover` scores on) must equal `current()`
        // after any sequence of advances.
        let mirror = Arc::new(AtomicU64::new(0));
        let offsets = ReplicaOffset::new(state(), Some(mirror.clone()));
        for payload in [&b"aa"[..], b"bbbb", b"c", b"dddddddd"] {
            offsets
                .advance(&ReplicationFrame::new(0, Bytes::copy_from_slice(payload)))
                .await;
            assert_eq!(mirror.load(Ordering::Acquire), offsets.current().await);
        }
    }
}
