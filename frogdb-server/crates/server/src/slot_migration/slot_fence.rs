//! The slot-ownership fencing token, and the verdict it produces at the
//! execute seam.
//!
//! # Why a token at all
//!
//! The finalization barrier (rework issue 02, phase 2a) stops *new* writes on
//! the source: `PrepareSlotHandoff` applies, the source arms a slot-scoped
//! `PauseMode::Write` barrier, and only then does the drain/confirm/complete
//! saga move ownership. That gate lives at `DispatchStage::PauseGate`, ahead of
//! `DispatchStage::ClusterSlotValidation` — so it sees a command *before* it is
//! routed, and a command that is already past it is invisible to it.
//!
//! The finalization-window measurement (2026-08-05) put a number on the
//! residual: the leader-source control has a ~0 state window — the leader
//! applies the entry it commits — and *still* showed ~150 µs p99 of writes
//! acknowledged after the handoff had committed. Those are commands that had
//! already passed the routing guard when the barrier armed. A gate at
//! admission, however tight, cannot see them; only a check that happens *again*
//! after the command ran can.
//!
//! # The token
//!
//! [`SlotFence`] is the slot-ownership generation this node validated against:
//! the owner it saw, plus the handoff attempt number (`SlotHandoff::seq`) that
//! was recorded for the slot at that instant. Both halves are already
//! replicated state — the token needs no new plumbing, no wire field, and no
//! shard-side change, which is what keeps `frogdb-core`'s shard layer
//! cluster-agnostic. Redis does the same job in `scriptVerifyClusterState`,
//! re-checking cluster state per `redis.call` rather than once per script.
//!
//! # Why the *prepare* is the load-bearing half
//!
//! The obvious token — "is the owner still me?" — cannot close the window on
//! its own, because the source only learns that ownership moved when it
//! *applies* the `CompleteSlotMigration` entry, which is by definition after
//! the cluster committed it. That lag is exactly the residual window being
//! fenced.
//!
//! `handoff_seq` closes it. The prepare applies on the source strictly before
//! the drain, the confirm, and the complete — a Raft round trip earlier than
//! the instant the measurement calls `t_leader`. A command that validated
//! before the prepare and is still running after it therefore sees a *changed*
//! token even though its own node still believes it owns the slot, and is
//! refused. That is the ordering the acceptance criterion (zero acknowledged
//! writes after commit) rests on.
//!
//! # No clock read
//!
//! The token deliberately ignores the handoff lease
//! ([`SlotMigration::live_handoff_at`](frogdb_cluster::types::SlotMigration::live_handoff_at)).
//! Lease expiry is a function of wall-clock time, so a lease-filtered token
//! would compare two different instants and could read as *unchanged* across a
//! prepare whose lease lapsed mid-command. The raw stored `seq` is pure
//! replicated data: equal tokens mean the replicated record did not move, full
//! stop. Over-refusing when a lease lapses is not possible either — a lapsed
//! lease still leaves the record (and its seq) in place until an abort,
//! complete, or cancel removes it, and each of those is a real change the
//! command deserves to be refused for.

use frogdb_cluster::types::ClusterSnapshot;
use frogdb_core::NodeId;
use frogdb_protocol::Response;

use super::redirect;

/// The slot-ownership generation a command was validated against.
///
/// Stamped by `ClusterSlotValidation` (and by the EXEC-time batch validator)
/// only when **this node is the slot's current owner** — i.e. only on the
/// source side of a handoff. The importing target is deliberately never
/// stamped: on the target, completing the handoff makes the node *more*
/// entitled to serve, and a fence there would answer `MOVED` pointing at
/// itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotFence {
    /// The slot the command is pinned to.
    pub(crate) slot: u16,
    /// The owner this node saw at validation time — always itself, by the
    /// construction rule above. Carried rather than assumed so the verdict is a
    /// pure function of its inputs.
    pub(crate) owner: NodeId,
    /// The handoff attempt recorded for `slot` at validation time; `0` when no
    /// handoff was prepared. `SlotHandoff::seq` is minted from a replicated
    /// monotone counter, so `0` can never be a real attempt id.
    pub(crate) handoff_seq: u64,
}

/// What slot validation decided, plus the fence the execute seam must re-check
/// if the command is allowed to run.
///
/// Distinguishing `Serve(None)` from `Serve(Some(..))` is the point: "run this
/// locally, nothing to fence" (importing target, READONLY replica read,
/// standalone) is a different answer from "run this locally, and check the
/// generation again before you acknowledge it".
#[derive(Debug)]
pub(crate) enum SlotVerdict {
    /// Refuse now: `-MOVED` / `-ASK` / `-CROSSSLOT` / `-CLUSTERDOWN`.
    Reply(Response),
    /// Run locally. `Some(fence)` when this node is the slot's owner and the
    /// execute seam owes a re-check.
    Serve(Option<SlotFence>),
}

/// The handoff attempt currently recorded for `slot`, or `0` for none.
fn handoff_seq_of(snapshot: &ClusterSnapshot, slot: u16) -> u64 {
    snapshot
        .migrations
        .get(&slot)
        .and_then(|migration| migration.handoff.as_ref())
        .map_or(0, |handoff| handoff.seq)
}

/// Stamp the fence for a command that slot validation just cleared to run
/// locally on `slot`.
///
/// `None` when this node is not the slot's owner — the importing-target and
/// READONLY-replica serve paths, neither of which is the source of a handoff.
pub(crate) fn stamp_fence(
    snapshot: &ClusterSnapshot,
    slot: u16,
    self_node_id: NodeId,
) -> Option<SlotFence> {
    if snapshot.get_slot_owner(slot) != Some(self_node_id) {
        return None;
    }
    Some(SlotFence {
        slot,
        owner: self_node_id,
        handoff_seq: handoff_seq_of(snapshot, slot),
    })
}

/// Re-check a stamped fence against the topology that exists *now*, at the
/// execute seam.
///
/// `None` means the generation is unchanged and the command's reply stands.
/// `Some(reply)` is what the client gets instead — the command's own answer is
/// discarded, acknowledged to nobody.
///
/// The three refusal classes, and why each is what it is:
///
/// * **Ownership moved to a node we can name** → `MOVED <slot> <addr>`. This is
///   settled: the slot belongs elsewhere, the retry belongs elsewhere, and this
///   is the same reply the client would have got had it arrived a moment later.
/// * **Ownership still ours, but the handoff generation changed** →
///   `TRYAGAIN`. Undecided: a prepared handoff can still abort, so naming the
///   target would be a redirect this node cannot back. See
///   [`redirect::tryagain_slot_handoff`].
/// * **The slot has no owner in our view, or its owner's node info is missing**
///   → `CLUSTERDOWN`. Same answer the routing guard gives for an unassigned
///   slot; there is nowhere to send the client.
pub(crate) fn fence_verdict(snapshot: &ClusterSnapshot, fence: SlotFence) -> Option<Response> {
    let owner_now = snapshot.get_slot_owner(fence.slot);
    if owner_now == Some(fence.owner) && handoff_seq_of(snapshot, fence.slot) == fence.handoff_seq {
        return None;
    }
    match owner_now {
        // Still ours; only the handoff generation moved. Undecided, so retry.
        Some(owner) if owner == fence.owner => Some(redirect::tryagain_slot_handoff(fence.slot)),
        Some(owner) => match snapshot.nodes.get(&owner).map(|node| node.addr) {
            Some(addr) => Some(redirect::moved(fence.slot, addr)),
            None => Some(redirect::clusterdown_slot(fence.slot)),
        },
        None => Some(redirect::clusterdown_slot(fence.slot)),
    }
}

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use frogdb_cluster::types::{NodeInfo, SlotHandoff, SlotMigration};

    const SLOT: u16 = 1234;
    const ME: NodeId = 1;
    const OTHER: NodeId = 2;

    fn node(id: NodeId, port: u16) -> NodeInfo {
        NodeInfo::new_primary(
            id,
            format!("127.0.0.1:{port}").parse().unwrap(),
            format!("127.0.0.1:{}", port + 10_000).parse().unwrap(),
        )
    }

    fn snapshot(owner: Option<NodeId>) -> ClusterSnapshot {
        let mut snapshot = ClusterSnapshot::new();
        snapshot.nodes.insert(ME, node(ME, 7001));
        snapshot.nodes.insert(OTHER, node(OTHER, 7002));
        if let Some(owner) = owner {
            snapshot.slot_assignment.insert(SLOT, owner);
        }
        snapshot
    }

    fn with_handoff(mut snapshot: ClusterSnapshot, seq: u64, drained: bool) -> ClusterSnapshot {
        let mut migration = SlotMigration::new(SLOT, ME, OTHER);
        migration.handoff = Some(SlotHandoff {
            seq,
            prepared_at_ms: 1_000,
            barrier_ms: 100,
            lease_ms: 10_000,
            drained,
        });
        snapshot.migrations.insert(SLOT, migration);
        snapshot
    }

    fn error_text(resp: &Response) -> String {
        match resp {
            Response::Error(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    #[test]
    fn only_the_owner_is_stamped() {
        assert_eq!(
            stamp_fence(&snapshot(Some(ME)), SLOT, ME),
            Some(SlotFence {
                slot: SLOT,
                owner: ME,
                handoff_seq: 0
            })
        );
        // Importing target serving under ASKING: not the owner, not fenced.
        assert_eq!(stamp_fence(&snapshot(Some(OTHER)), SLOT, ME), None);
        // Unassigned slot: nothing to fence against.
        assert_eq!(stamp_fence(&snapshot(None), SLOT, ME), None);
    }

    #[test]
    fn a_prepared_handoff_is_part_of_the_stamp() {
        let stamped = stamp_fence(&with_handoff(snapshot(Some(ME)), 9, false), SLOT, ME).unwrap();
        assert_eq!(stamped.handoff_seq, 9);
    }

    #[test]
    fn an_unchanged_generation_admits_the_command() {
        let snap = snapshot(Some(ME));
        let fence = stamp_fence(&snap, SLOT, ME).unwrap();
        assert!(fence_verdict(&snap, fence).is_none());

        // A migration that is merely *open* (bulk transfer, no handoff) is not a
        // generation change: that is where a migration spends nearly all its
        // life, and fencing it would refuse every write for the whole transfer.
        let mut open = snapshot(Some(ME));
        open.migrations
            .insert(SLOT, SlotMigration::new(SLOT, ME, OTHER));
        assert!(fence_verdict(&open, fence).is_none());
    }

    /// The load-bearing case: the source has applied `PrepareSlotHandoff` but
    /// not yet `CompleteSlotMigration`, so it still believes it owns the slot.
    /// This is exactly the residual window the measurement recorded, and the
    /// only signal available inside it is the handoff seq.
    #[test]
    fn a_handoff_prepared_after_validation_refuses_with_tryagain() {
        let fence = stamp_fence(&snapshot(Some(ME)), SLOT, ME).unwrap();
        let prepared = with_handoff(snapshot(Some(ME)), 1, false);
        let verdict = fence_verdict(&prepared, fence).expect("must refuse");
        assert_eq!(
            error_text(&verdict),
            format!("TRYAGAIN Slot {SLOT} finalization in progress")
        );
    }

    #[test]
    fn a_superseding_attempt_refuses_too() {
        let fence = stamp_fence(&with_handoff(snapshot(Some(ME)), 4, false), SLOT, ME).unwrap();
        let superseded = with_handoff(snapshot(Some(ME)), 5, false);
        assert!(fence_verdict(&superseded, fence).is_some());
    }

    /// An attempt that aborted restores the pre-prepare generation, and
    /// ownership never moved — so a command that validated before it is still
    /// entitled to its answer. Refusing here would turn every aborted
    /// finalization into a burst of spurious `TRYAGAIN`s.
    #[test]
    fn an_aborted_attempt_leaves_the_generation_where_it_was() {
        let fence = stamp_fence(&snapshot(Some(ME)), SLOT, ME).unwrap();
        let mut aborted = snapshot(Some(ME));
        aborted
            .migrations
            .insert(SLOT, SlotMigration::new(SLOT, ME, OTHER));
        assert!(fence_verdict(&aborted, fence).is_none());
    }

    #[test]
    fn ownership_that_moved_refuses_with_moved_at_the_new_owner() {
        let fence = stamp_fence(&snapshot(Some(ME)), SLOT, ME).unwrap();
        let moved_on = snapshot(Some(OTHER));
        let verdict = fence_verdict(&moved_on, fence).expect("must refuse");
        assert_eq!(error_text(&verdict), format!("MOVED {SLOT} 127.0.0.1:7002"));
    }

    #[test]
    fn an_owner_we_cannot_address_degrades_to_clusterdown() {
        let fence = stamp_fence(&snapshot(Some(ME)), SLOT, ME).unwrap();
        let mut moved_on = snapshot(Some(OTHER));
        moved_on.nodes.remove(&OTHER);
        let verdict = fence_verdict(&moved_on, fence).expect("must refuse");
        assert_eq!(
            error_text(&verdict),
            format!("CLUSTERDOWN Hash slot {SLOT} not served")
        );
    }

    #[test]
    fn a_slot_that_lost_its_owner_degrades_to_clusterdown() {
        let fence = stamp_fence(&snapshot(Some(ME)), SLOT, ME).unwrap();
        let verdict = fence_verdict(&snapshot(None), fence).expect("must refuse");
        assert_eq!(
            error_text(&verdict),
            format!("CLUSTERDOWN Hash slot {SLOT} not served")
        );
    }

    /// The fence is per slot: a handoff on a neighbouring slot must not refuse
    /// commands on this one. Under load a cluster finalizes slots continuously,
    /// and a coarser token would make every finalization a cluster-wide stall.
    #[test]
    fn a_handoff_on_another_slot_is_invisible() {
        let fence = stamp_fence(&snapshot(Some(ME)), SLOT, ME).unwrap();
        let mut elsewhere = snapshot(Some(ME));
        let mut migration = SlotMigration::new(SLOT + 1, ME, OTHER);
        migration.handoff = Some(SlotHandoff {
            seq: 77,
            prepared_at_ms: 1_000,
            barrier_ms: 100,
            lease_ms: 10_000,
            drained: true,
        });
        elsewhere.migrations.insert(SLOT + 1, migration);
        assert!(fence_verdict(&elsewhere, fence).is_none());
    }
}
