//! The model's counterexample, re-run against real state with no model checker
//! in the loop.
//!
//! [`super`] found `a_failed_promotion_strands_the_node` by permuting actions;
//! this file is the same sequence written out by hand against a real
//! [`PrimaryReplicationHandler`], a real [`AppliedOffset`] and a real
//! [`ReplicaOffset`], so the defect keeps a failing witness even if the model
//! were deleted, and so a reader can see the exposure without running a
//! checker.
//!
//! See issue 16 (`.scratch/replication-correctness/issues/`).

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crate::identity::{ReplicationIdentity, SharedReplicationState};
use crate::primary::PrimaryReplicationHandler;
use crate::replica::Claim;
use crate::replica::offset::{AppliedOffset, ReplicaOffset};
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;
use crate::{BacklogConfig, LagThresholdConfig};

/// A handler whose state file cannot be written: the parent directory does not
/// exist, so `save_snapshot` fails while `discard_staged_full_sync` (which
/// takes the *data* dir, which does exist) succeeds — exactly the ordering
/// `begin_primary_stint` relies on.
///
/// The identity's handles are returned alongside it because the exposure is on
/// them: a promotion freezes the applied gate, and the connection and the
/// consumer are the two things that gate refuses.
type Fixture = (
    PrimaryReplicationHandler,
    AppliedOffset,
    SharedReplicationState,
    Arc<AtomicU64>,
);

fn handler_that_cannot_persist(dir: &std::path::Path) -> Fixture {
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let mut state = ReplicationState::new();
    state.replication_id = "a".repeat(40);
    let identity = ReplicationIdentity::adopting(state, &tracker);
    let applied = identity.applied();
    let shared_state = identity.state();
    let live = identity.live();
    let handler = PrimaryReplicationHandler::new(
        identity,
        dir.join("no-such-directory").join("replication_state.json"),
        tracker,
        None,
        dir.to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        BacklogConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024,
            ttl_secs: 0,
        },
        0,
        crate::feed_gate::ReplicaFeedGate::open(),
    );
    (handler, applied, shared_state, live)
}

/// KNOWN EXPOSURE (issue 16). A promotion whose persist fails restores the
/// replication state exactly — and leaves the node unable to replicate at all.
///
/// `begin_primary_stint` settles the heads (`AppliedOffset::freeze`) before the
/// persist it can fail on, and the rollback it performs is the
/// [`StintPlan`](crate::primary::StintPlan)'s: the replication state, and
/// nothing else. The gate stays frozen, so the frame consumer that is still
/// running stops applying at its next claim, and the connection cannot complete
/// a full resync either — `reset_to` is refused under the same frozen gate.
/// Nothing inside the node re-opens it; only a new replica stint
/// (`begin_replica_stint`, reached from `REPLICAOF` / the role reconciler)
/// does.
///
/// A *characterization* test: it asserts the exposure is still there, so a fix
/// fails it loudly rather than leaving a dead witness behind.
// FM-REPLICATION-020
#[test]
fn a_failed_promotion_leaves_the_node_unable_to_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let (handler, applied, shared_state, live) = handler_that_cannot_persist(dir.path());

    // A live replica stream: a consumer holding an apply licence, and a
    // connection that could adopt a full-resync position. Both are built under
    // the stream's stint, in that order, exactly as `RealReplicaStreamer::start`
    // builds them.
    let stint = applied.begin_replica_stint();
    let connection = ReplicaOffset::new(shared_state, live, applied.clone());
    let epoch = applied.epoch();
    assert!(
        matches!(stint.claim(epoch, 4), Claim::Granted),
        "precondition: the consumer is applying"
    );

    let before = handler.state();
    let err = handler
        .begin_primary_stint()
        .expect_err("the state file is unwritable, so the promotion must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);

    // The half `StintPlan` owns: bit for bit what the node was on.
    assert_eq!(
        handler.state(),
        before,
        "the replication state rollback is exact"
    );

    // The halves it does not own.
    assert!(
        matches!(stint.claim(epoch, 4), Claim::Retired),
        "issue 16 is open: the applied gate frozen by `settle_at_applied` is \
         never unfrozen by the rollback, so the consumer stops applying"
    );
    assert!(
        !connection.reset_to(0),
        "issue 16 is open: the same frozen gate refuses a full resync, so the \
         node cannot recover by resyncing either"
    );

    // And the way out is from outside the node: a new replica stint.
    let restart = applied.begin_replica_stint();
    assert!(
        matches!(restart.claim(applied.epoch(), 4), Claim::Granted),
        "a fresh replica stint is what re-opens the gate"
    );
}
