//! The decision half of a promotion.
//!
//! [`PrimaryReplicationHandler::begin_primary_stint`] is the I/O half — it
//! disarms the inherited staged checkpoint, freezes the offset heads, takes the
//! state write lock, persists, and re-arms the backlog. What it *decides* is
//! this module: given the state the node is on, a freshly minted replication id
//! and the boundary the heads settled at, what does the state become and where
//! does the backlog window re-open?
//!
//! Split out for the same reason
//! [`PartialSyncReplay::handle_partial_sync_request`] has the shape it has
//! (`primary/replay.rs`): a decision that reads plain data and returns a plain
//! description of the transition can be unit-tested over its whole input space
//! and driven by a model checker, while the method that owns the lock and the
//! file keeps owning only those.
//!
//! The mint itself is *not* in here. `generate_replication_id` reads entropy, so
//! hoisting it into the caller is what makes this function a pure function of
//! its inputs — the same reason the backlog TTL takes `now` as a parameter
//! rather than reading the clock.
//!
//! [`PrimaryReplicationHandler::begin_primary_stint`]: crate::PrimaryReplicationHandler::begin_primary_stint
//! [`PartialSyncReplay::handle_partial_sync_request`]: crate::primary::PartialSyncReplay::handle_partial_sync_request

use crate::state::ReplicationState;

/// What beginning a primary stint does: the state to publish and persist, the
/// state to restore if that persist fails, and where the resume window reopens.
///
/// Every field is data, not an instruction to re-derive something: the caller
/// applies `minted`, and on a failed persist assigns `rollback` back under the
/// same lock. Carrying the rollback here (rather than leaving the caller to
/// remember it held the previous value) is what makes the plan a complete
/// description of the transition — both of its outcomes — for a test or a model
/// that never takes the lock at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StintPlan {
    /// The state a successful promotion leaves behind: the minted id heading
    /// the history, the inherited id frozen into the failover window at the
    /// boundary, and a save offset that never moves backwards.
    pub minted: ReplicationState,
    /// The state to restore when the persist fails — bit for bit what the node
    /// was on before, so a promotion that could not be written down leaves no
    /// trace of itself.
    pub rollback: ReplicationState,
    /// Where [`crate::primary::PartialSyncReplay::arm_backlog_floor`] reopens
    /// the resume window once the persist succeeds. Always the boundary: the
    /// promoted node claims history from exactly the data it holds.
    pub backlog_floor: u64,
}

/// Decide the promotion: pure over `(previous, minted_id, boundary)`, performs
/// no I/O and takes no lock.
///
/// `boundary` is the **applied** offset the heads settled at
/// ([`crate::OffsetCoordinator::settle_at_applied`]), never the received head —
/// see FM-REPLICATION-019. It is passed in rather than read here because
/// settling mutates the coordinator, which is an effect.
///
/// The save offset is raised with a `max` rather than assigned: the persisted
/// offset may never move backwards, and a re-promotion after a full resync to a
/// *lower* offset is exactly the case that distinguishes the two.
pub fn plan_primary_stint(
    previous: &ReplicationState,
    minted_id: String,
    boundary: u64,
) -> StintPlan {
    let mut minted = previous.clone();
    minted.shift_replication_id(minted_id, boundary);
    minted.offset_at_save = minted.offset_at_save.max(boundary);
    StintPlan {
        minted,
        rollback: previous.clone(),
        backlog_floor: boundary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_at(offset_at_save: u64) -> ReplicationState {
        let mut state = ReplicationState::new();
        state.offset_at_save = offset_at_save;
        state
    }

    /// The shape of the transition: the minted id heads the history, the id it
    /// replaced becomes the failover window frozen at the boundary, and the
    /// window the backlog reopens is that same boundary.
    // FM-REPLICATION-019
    #[test]
    fn a_stint_heads_the_minted_id_and_freezes_the_inherited_one_at_the_boundary() {
        let previous = state_at(0);
        let inherited = previous.replication_id.clone();

        let plan = plan_primary_stint(&previous, "minted-id".to_string(), 100);

        assert_eq!(plan.minted.replication_id, "minted-id");
        assert_eq!(
            plan.minted.secondary_id.as_deref(),
            Some(inherited.as_str())
        );
        assert_eq!(
            plan.minted.secondary_offset, 100,
            "the inherited history ends at the data this node holds"
        );
        assert_eq!(
            plan.backlog_floor, 100,
            "the resume window reopens at the boundary, not at the old head"
        );
    }

    /// The save offset is a high-water mark on the way up…
    // FM-REPLICATION-019
    #[test]
    fn the_save_offset_rises_to_the_boundary() {
        let plan = plan_primary_stint(&state_at(40), "minted-id".to_string(), 100);
        assert_eq!(plan.minted.offset_at_save, 100);
    }

    /// …and does not follow a boundary that sits below it: a re-promotion after
    /// a full resync to a lower offset must not rewind what is on disk.
    // FM-REPLICATION-019
    #[test]
    fn the_save_offset_never_follows_a_boundary_backwards() {
        let plan = plan_primary_stint(&state_at(900), "minted-id".to_string(), 50);
        assert_eq!(
            plan.minted.offset_at_save, 900,
            "the persisted offset may never move back"
        );
        assert_eq!(
            plan.minted.secondary_offset, 50,
            "the failover window still freezes at the boundary itself"
        );
        assert_eq!(plan.backlog_floor, 50, "and so does the backlog floor");
    }

    /// The equal case is not a special case: `max` re-stores the value it
    /// already holds, and the rest of the plan is unchanged by it.
    // FM-REPLICATION-019
    #[test]
    fn a_boundary_equal_to_the_save_offset_changes_nothing_about_it() {
        let plan = plan_primary_stint(&state_at(500), "minted-id".to_string(), 500);
        assert_eq!(plan.minted.offset_at_save, 500);
        assert_eq!(plan.minted.secondary_offset, 500);
    }

    /// A promotion at offset 0 — a node that never received anything — still
    /// mints and still opens a window, at 0.
    // FM-REPLICATION-019
    #[test]
    fn a_stint_at_the_zero_boundary_still_mints_and_arms() {
        let previous = state_at(0);
        let inherited = previous.replication_id.clone();

        let plan = plan_primary_stint(&previous, "minted-id".to_string(), 0);

        assert_eq!(plan.minted.replication_id, "minted-id");
        assert_eq!(
            plan.minted.secondary_id.as_deref(),
            Some(inherited.as_str())
        );
        assert_eq!(plan.minted.secondary_offset, 0);
        assert_eq!(plan.backlog_floor, 0);
    }

    /// A second stint shifts again: the id minted by the first becomes the
    /// window, and the one it had replaced is dropped. Only one failover window
    /// is ever carried, which is the PSYNC2 shape.
    // FM-REPLICATION-019
    #[test]
    fn a_second_stint_shifts_the_first_stints_id_into_the_window() {
        let first = plan_primary_stint(&state_at(0), "first".to_string(), 100);
        let second = plan_primary_stint(&first.minted, "second".to_string(), 300);

        assert_eq!(second.minted.replication_id, "second");
        assert_eq!(second.minted.secondary_id.as_deref(), Some("first"));
        assert_eq!(second.minted.secondary_offset, 300);
    }

    /// The rollback half of the transition: what the caller restores when the
    /// persist fails is the state it was on, untouched by the mint.
    // FM-REPLICATION-020
    #[test]
    fn the_rollback_is_the_state_the_node_was_already_on() {
        let previous = state_at(700);
        let plan = plan_primary_stint(&previous, "minted-id".to_string(), 900);

        assert_eq!(plan.rollback, previous, "not a re-derivation, the original");
        assert_ne!(
            plan.rollback, plan.minted,
            "and it is emphatically not the minted state"
        );
    }

    /// Deciding twice from the same inputs decides the same thing: no clock, no
    /// entropy, no shared cell is read inside. This is the property the model
    /// checker in issue 08 rests on.
    // FM-REPLICATION-019
    #[test]
    fn the_decision_is_a_function_of_its_inputs_alone() {
        let previous = state_at(120);
        let first = plan_primary_stint(&previous, "minted-id".to_string(), 400);
        let second = plan_primary_stint(&previous, "minted-id".to_string(), 400);
        assert_eq!(first, second);
        assert_eq!(
            previous.replication_id,
            plan_primary_stint(&previous, "other".to_string(), 400)
                .rollback
                .replication_id,
            "and it leaves its input alone"
        );
    }
}
