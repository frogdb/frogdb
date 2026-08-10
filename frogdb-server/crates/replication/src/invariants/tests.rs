//! Forcing tests for the replication catalog.
//!
//! Two kinds live here, and they answer different questions:
//!
//! - **Per-entry forcing tests** build a view that violates exactly one claim
//!   and assert the id it reports. They are what make the catalog live code to
//!   the mutation gate rather than a table nothing reads.
//! - **Per-seam hook tests** drive a real component into a state the catalog
//!   rejects and expect the panic, so deleting a `debug_assert_view_clean` call
//!   goes red. Where a seam cannot be driven dirty through any public path, the
//!   reason is written at the seam itself rather than papered over with a test
//!   that would still pass without the hook.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;

use super::*;
use crate::primary::ring_buffer::ReplicationRingBuffer;
use crate::replica::offset::{AppliedOffset, ReplicaOffset};
use crate::replica_session::ReplicaAnnouncement;
use crate::state::ReplicationState;
use crate::tracker::ReplicationTrackerImpl;
use crate::view::{
    ApplyGateView, BacklogView, ContinueGrant, FeedGateView, FenceView, PhaseChange,
    PromotionWitness, ReplicaView,
};

// ---- fixtures ----------------------------------------------------------

/// A well-formed replication id made of one repeated hex digit, so two of them
/// are visibly different in a failure message.
fn hex_id(digit: char) -> String {
    std::iter::repeat_n(digit, 40).collect()
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn clean_state() -> ReplicationState {
    let mut state = ReplicationState::new();
    state.replication_id = hex_id('a');
    state.secondary_id = Some(hex_id('b'));
    state.secondary_offset = 80;
    state.offset_at_save = 90;
    state
}

/// A view that satisfies every HARD entry, and satisfies each one *tightly*:
/// every forcing test below moves exactly one number or flag and gets exactly
/// one id back, which is only true if nothing here has slack.
fn clean_view() -> ReplicationView {
    ReplicationView::empty()
        .with_state(clean_state())
        .with_offsets(100, 100, 100)
        .with_apply_gate(ApplyGateView {
            frozen: false,
            stint: 0,
            epoch: 0,
            diverged: None,
        })
        .with_backlog(BacklogView {
            start_offset: Some(40),
            oldest_begin: Some(40),
            oldest_end: Some(50),
            newest_offset: Some(100),
            entries: 6,
            bytes: 60,
            max_entries: 10,
            max_bytes: 1000,
        })
        .with_replicas(
            vec![ReplicaView {
                id: 1,
                addr: addr(6380),
                announced_id: Some((IpAddr::V4(Ipv4Addr::LOCALHOST), 7000)),
                phase: Phase::Streaming,
                acked: 100,
                resume_floor: 90,
                last_ack_age: Duration::from_secs(1),
            }],
            None,
        )
        .with_feed_gate(FeedGateView {
            is_held: true,
            hold_remaining: Some(Duration::from_millis(50)),
            barrier_budget: Some(Duration::from_millis(100)),
        })
        .with_fence(FenceView {
            self_fence_enabled: true,
            armed: true,
            freshness_window: Duration::from_secs(1),
        })
        .with_role(RoleView::Primary)
        .with_promotion(PromotionWitness {
            previous_id: hex_id('b'),
            boundary: 80,
        })
        .with_grant(ContinueGrant {
            replay_from: 60,
            resume_offset: 100,
        })
        .with_phase_change(PhaseChange {
            replica_id: 1,
            from: Phase::StreamingCheckpoint,
            to: Phase::Streaming,
        })
}

#[track_caller]
fn assert_reports(view: &ReplicationView, expected: &[&str]) {
    let reported: Vec<&str> = check_hard(view).iter().map(|v| v.id).collect();
    assert_eq!(
        reported,
        expected,
        "\nreported:\n{}",
        render(&check_hard(view))
    );
}

// ---- catalog shape -----------------------------------------------------

#[test]
fn the_catalog_holds_the_seed_sixteen_with_unique_ids() {
    let ids: Vec<&str> = CATALOG.iter().map(|inv| inv.id).collect();
    assert_eq!(
        ids,
        vec![
            "INV-REPLID-1",
            "INV-REPLID-2",
            "INV-REPLID-3",
            "INV-OFFSET-1",
            "INV-OFFSET-2",
            "INV-OFFSET-3",
            "INV-OFFSET-4",
            "INV-BACKLOG-1",
            "INV-BACKLOG-2",
            "INV-BACKLOG-3",
            "INV-SESSION-1",
            "INV-SESSION-2",
            "INV-SESSION-3",
            "INV-GATE-1",
            "INV-FENCE-1",
            "INV-ROLE-1",
        ],
        "the seed catalog is fixed by PRD §3 W1; adding an entry means adding its forcing test"
    );
    let unique: std::collections::BTreeSet<&str> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "catalog ids must be unique");
    assert!(
        CATALOG.iter().all(|inv| !inv.claim.is_empty()),
        "every entry states its claim"
    );
    assert!(
        CATALOG.iter().all(|inv| !inv.requires.is_empty()),
        "every entry declares what it reads; an entry that needs nothing reads nothing"
    );
}

/// Two entries are excepted and both name their ruling. The list is asserted
/// whole so a third exception cannot be added quietly: an exception is a ruling
/// that a dirty state is legitimate, and each one is a claim this catalog stops
/// making.
#[test]
fn every_documented_exception_names_its_ruling() {
    let excepted: Vec<(&str, &str)> = CATALOG
        .iter()
        .filter_map(|inv| match inv.tier {
            Tier::Hard => None,
            Tier::DocumentedException(citation) => Some((inv.id, citation.as_str())),
        })
        .collect();
    assert_eq!(
        excepted,
        vec![
            ("INV-OFFSET-2", ".scratch/replication-correctness/issues/16"),
            (
                "INV-ROLE-1",
                ".scratch/testing-improvements/issues/done/48-chained-replication-contract.md"
            ),
        ],
        "every exception cites the ruling that makes it one"
    );
}

/// The whole point of the optional view: a seam that saw nothing reports
/// nothing, rather than reporting that everything is zero.
#[test]
fn an_empty_view_skips_every_entry() {
    let empty = ReplicationView::empty();
    assert!(CATALOG.iter().all(|inv| !inv.is_checkable(&empty)));
    assert_reports(&empty, &[]);
    assert!(check_all(&empty).is_empty());
}

/// A narrow capture must not be read as a claim about what it could not see:
/// the applied/landed pair alone leaves every live-relative entry unchecked.
#[test]
fn a_partial_view_skips_the_entries_it_cannot_evaluate() {
    let pair_only = ReplicationView::empty().with_applied_pair(100, 100);
    let checkable: Vec<&str> = CATALOG
        .iter()
        .filter(|inv| inv.is_checkable(&pair_only))
        .map(|inv| inv.id)
        .collect();
    assert_eq!(checkable, vec!["INV-OFFSET-1"]);

    // ... and the same numbers that would be a violation against a live head
    // are silent when no live head was captured.
    let inverted = ReplicationView::empty().with_applied_pair(100, 100);
    assert_reports(&inverted, &[]);
}

#[test]
fn the_clean_view_satisfies_every_entry_in_both_tiers() {
    let view = clean_view();
    assert!(
        CATALOG.iter().all(|inv| inv.is_checkable(&view)),
        "the fixture must exercise every entry, or a forcing test could pass by omission"
    );
    assert_reports(&view, &[]);
    assert!(check_all(&view).is_empty());
}

/// `check_hard` is the hook's filter and `check_all` is the report's, and the
/// only difference between them is the tier.
#[test]
fn the_hard_filter_is_what_separates_the_two_tiers() {
    let view = clean_view().with_role(RoleView::Replica {
        upstream: Some(addr(6379)),
    });
    assert_reports(&view, &[]);
    let all: Vec<&str> = check_all(&view).iter().map(|v| v.id).collect();
    assert_eq!(all, vec!["INV-ROLE-1"]);
}

#[test]
fn violations_accumulate_across_entries() {
    let mut view = clean_view();
    view.state.as_mut().unwrap().secondary_id = None;
    assert_reports(&view, &["INV-REPLID-1", "INV-REPLID-2"]);
}

// ---- forcing tests: one per HARD entry ---------------------------------

#[test]
fn inv_replid_1_forces_a_half_cleared_failover_window() {
    let mut view = clean_view().with_promotion(PromotionWitness {
        previous_id: hex_id('b'),
        boundary: -1_i64 as u64,
    });
    // An offset with no id: the other half of the same break.
    view.state.as_mut().unwrap().secondary_id = None;
    view.promotion = None;
    assert_reports(&view, &["INV-REPLID-1"]);
    assert!(check_hard(&view)[0].detail.contains("secondary_offset=80"));

    // An id with no offset.
    let mut view = clean_view();
    view.state.as_mut().unwrap().secondary_offset = -1;
    view.promotion = None;
    assert_reports(&view, &["INV-REPLID-1"]);
    assert!(check_hard(&view)[0].detail.contains("secondary_offset=-1"));
}

#[test]
fn inv_replid_2_forces_a_promotion_that_lost_its_history() {
    // Revert (b) `f6484219`: minted a new id without demoting the old one.
    let mut view = clean_view();
    view.state.as_mut().unwrap().secondary_id = Some(hex_id('c'));
    assert_reports(&view, &["INV-REPLID-2"]);
    assert!(check_hard(&view)[0].detail.contains(&hex_id('b')));

    // Froze the window at an offset the promotion did not stop at.
    let mut view = clean_view();
    view.promotion.as_mut().unwrap().boundary = 70;
    assert_reports(&view, &["INV-REPLID-2"]);
    assert!(check_hard(&view)[0].detail.contains("70"));
}

#[test]
fn inv_replid_3_forces_malformed_and_duplicated_ids() {
    let mut view = clean_view();
    view.state.as_mut().unwrap().replication_id = "not-a-replication-id".to_string();
    view.promotion = None;
    assert_reports(&view, &["INV-REPLID-3"]);

    let mut view = clean_view();
    view.state.as_mut().unwrap().secondary_id = Some("beef".to_string());
    view.promotion = None;
    assert_reports(&view, &["INV-REPLID-3"]);

    // The same history claimed twice: `window_contains` would accept
    // pre-failover offsets as current.
    let mut view = clean_view();
    view.state.as_mut().unwrap().secondary_id = Some(hex_id('a'));
    view.promotion.as_mut().unwrap().previous_id = hex_id('a');
    assert_reports(&view, &["INV-REPLID-3"]);
    assert!(check_hard(&view)[0].detail.contains(&hex_id('a')));
}

#[test]
fn inv_offset_1_forces_both_inversions_of_the_durability_chain() {
    let mut view = clean_view();
    view.offsets.as_mut().unwrap().landed = 101;
    assert_reports(&view, &["INV-OFFSET-1"]);
    assert!(check_hard(&view)[0].detail.contains("101"));

    let mut view = clean_view();
    view.offsets.as_mut().unwrap().applied = 101;
    assert_reports(&view, &["INV-OFFSET-1"]);

    // Without a live head the pair is still checked — the half that is
    // reachable from a seam that owns only the pair.
    let pair = ReplicationView::empty().with_applied_pair(5, 7);
    assert_reports(&pair, &["INV-OFFSET-1"]);
}

/// Reported, never asserted: the save point is monotone and a backwards full
/// resync leaves it above the head this node holds, which is shipped behaviour
/// until issue 16 rules on it — so the entry is a `DocumentedException` and the
/// hooks must stay quiet about it.
#[test]
fn inv_offset_2_reports_a_state_file_claiming_more_than_the_stream() {
    let mut view = clean_view();
    view.state.as_mut().unwrap().offset_at_save = 101;
    assert_reports(&view, &[]);
    let reported = check_all(&view);
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].id, "INV-OFFSET-2");
    assert!(reported[0].detail.contains("101"));
}

#[test]
fn inv_offset_3_forces_the_seeded_ack_that_wait_would_have_counted() {
    // Revert (c) `90fefaf7`: a replica credited before it acked anything.
    let mut view = clean_view();
    let replica = &mut view.replicas.as_mut().unwrap()[0];
    replica.phase = Phase::StreamingCheckpoint;
    view.phase_change.as_mut().unwrap().to = Phase::StreamingCheckpoint;
    assert_reports(&view, &["INV-OFFSET-3"]);
    assert!(check_hard(&view)[0].detail.contains("streaming-checkpoint"));

    // Credited past the stream itself.
    let mut view = clean_view();
    view.replicas.as_mut().unwrap()[0].acked = 101;
    assert_reports(&view, &["INV-OFFSET-3"]);

    let mut view = clean_view();
    view.replicas.as_mut().unwrap()[0].resume_floor = 101;
    assert_reports(&view, &["INV-OFFSET-3"]);

    // A departing replica keeps what it legitimately acked: `Disconnecting` is
    // past `Streaming`, not before it.
    let mut view = clean_view();
    view.replicas.as_mut().unwrap()[0].phase = Phase::Disconnecting;
    view.phase_change.as_mut().unwrap().to = Phase::Disconnecting;
    assert_reports(&view, &[]);
}

#[test]
fn inv_offset_4_forces_a_failover_window_past_the_stream() {
    let mut view = clean_view();
    view.state.as_mut().unwrap().secondary_offset = 101;
    view.promotion.as_mut().unwrap().boundary = 101;
    assert_reports(&view, &["INV-OFFSET-4"]);
    assert!(check_hard(&view)[0].detail.contains("101"));

    // A live head of 0 is a full resync in flight, not an empty stream: the
    // grant rewound the head and the window still describes the keyspace this
    // node is holding until the payload lands. Skipped, not violated.
    let mut rewound = clean_state();
    rewound.secondary_offset = 700;
    rewound.offset_at_save = 0;
    let view = ReplicationView::empty()
        .with_state(rewound)
        .with_offsets(0, 0, 0);
    assert_reports(&view, &[]);
    assert!(check_all(&view).is_empty());
}

#[test]
fn inv_backlog_1_forces_a_floor_off_its_data_and_a_hole_in_the_ring() {
    // FM-REPLICATION-016: the floor claims a range the entries do not cover.
    let mut view = clean_view();
    view.backlog.as_mut().unwrap().start_offset = Some(39);
    view.grant.as_mut().unwrap().replay_from = 39;
    assert_reports(&view, &["INV-BACKLOG-1"]);
    assert!(check_hard(&view)[0].detail.contains("39"));

    // A floor above the oldest entry's end skips data the ring still holds.
    let mut view = clean_view();
    view.backlog.as_mut().unwrap().start_offset = Some(51);
    assert_reports(&view, &["INV-BACKLOG-1"]);

    // A hole: the entries no longer tile their own range.
    let mut view = clean_view();
    view.backlog.as_mut().unwrap().bytes = 50;
    assert_reports(&view, &["INV-BACKLOG-1"]);
    assert!(check_hard(&view)[0].detail.contains("60"));

    // An empty or unarmed ring claims nothing, so it can be wrong about
    // nothing.
    let mut view = clean_view();
    let backlog = view.backlog.as_mut().unwrap();
    backlog.start_offset = None;
    backlog.oldest_begin = None;
    backlog.oldest_end = None;
    backlog.newest_offset = None;
    backlog.entries = 0;
    backlog.bytes = 0;
    view.grant = None;
    assert_reports(&view, &[]);
}

#[test]
fn inv_backlog_2_forces_a_grant_outside_what_can_be_replayed() {
    // FM-REPLICATION-014: granted below the floor.
    let mut view = clean_view();
    view.grant.as_mut().unwrap().replay_from = 39;
    assert_reports(&view, &["INV-BACKLOG-2"]);
    assert!(check_hard(&view)[0].detail.contains("39"));

    // Granted above the offset the replay carries it to.
    let mut view = clean_view();
    view.grant.as_mut().unwrap().replay_from = 101;
    assert_reports(&view, &["INV-BACKLOG-2"]);

    // Granted against a window that is not open at all.
    let mut view = clean_view();
    let backlog = view.backlog.as_mut().unwrap();
    backlog.start_offset = None;
    backlog.oldest_begin = None;
    backlog.oldest_end = None;
    backlog.newest_offset = None;
    backlog.entries = 0;
    backlog.bytes = 0;
    assert_reports(&view, &["INV-BACKLOG-2"]);
    assert!(check_hard(&view)[0].detail.contains("closed"));
}

#[test]
fn inv_backlog_3_forces_both_caps() {
    let mut view = clean_view();
    view.backlog.as_mut().unwrap().max_entries = 5;
    assert_reports(&view, &["INV-BACKLOG-3"]);
    assert!(check_hard(&view)[0].detail.contains("6 entries"));

    let mut view = clean_view();
    view.backlog.as_mut().unwrap().max_bytes = 59;
    assert_reports(&view, &["INV-BACKLOG-3"]);

    // A single command larger than the whole cap is retained, not dropped: the
    // alternative is a ring that can never serve anything.
    let mut view = clean_view();
    let backlog = view.backlog.as_mut().unwrap();
    backlog.entries = 1;
    backlog.oldest_begin = Some(40);
    backlog.oldest_end = Some(100);
    backlog.newest_offset = Some(100);
    backlog.max_bytes = 59;
    assert_reports(&view, &[]);
}

#[test]
fn inv_session_1_forces_a_backwards_and_a_resurrected_phase() {
    let mut view = clean_view();
    view.phase_change.as_mut().unwrap().from = Phase::Streaming;
    view.phase_change.as_mut().unwrap().to = Phase::Connecting;
    assert_reports(&view, &["INV-SESSION-1"]);
    assert!(check_hard(&view)[0].detail.contains("connecting"));

    // Disconnecting is terminal: cleanup has already run.
    let mut view = clean_view();
    view.phase_change.as_mut().unwrap().from = Phase::Disconnecting;
    view.phase_change.as_mut().unwrap().to = Phase::Streaming;
    assert_reports(&view, &["INV-SESSION-1", "INV-SESSION-1"]);

    // Re-publishing the same phase is not movement.
    let mut view = clean_view();
    view.phase_change.as_mut().unwrap().from = Phase::Streaming;
    assert_reports(&view, &[]);
}

#[test]
fn inv_session_2_forces_one_replica_streaming_on_two_sessions() {
    // Spec GAP-5: `WAIT` would count this replica twice.
    let mut view = clean_view();
    let replicas = view.replicas.as_mut().unwrap();
    let mut duplicate = replicas[0].clone();
    duplicate.id = 2;
    replicas.push(duplicate);
    assert_reports(&view, &["INV-SESSION-2"]);
    assert!(check_hard(&view)[0].detail.contains("[1, 2]"));

    // Two sessions that never announced are two *unknowns*, not one replica.
    let mut view = clean_view();
    let replicas = view.replicas.as_mut().unwrap();
    replicas[0].announced_id = None;
    let mut second = replicas[0].clone();
    second.id = 2;
    replicas.push(second);
    assert_reports(&view, &[]);

    // Neither is a duplicate that is not streaming.
    let mut view = clean_view();
    let replicas = view.replicas.as_mut().unwrap();
    let mut duplicate = replicas[0].clone();
    duplicate.id = 2;
    duplicate.phase = Phase::Connecting;
    duplicate.acked = 0;
    replicas.push(duplicate);
    assert_reports(&view, &[]);
}

#[test]
fn inv_session_3_forces_a_departure_no_session_could_have_produced() {
    // FM-REPLICATION-062: a Lost record standing over a checker that was never
    // armed means one of the two was written without a streaming generation.
    let mut view = clean_view();
    view.departure = Some(ReplicaDeparture::Lost);
    view.fence.as_mut().unwrap().armed = false;
    view.replicas.as_mut().unwrap()[0].phase = Phase::Connecting;
    view.replicas.as_mut().unwrap()[0].acked = 0;
    view.phase_change.as_mut().unwrap().to = Phase::Connecting;
    view.phase_change.as_mut().unwrap().from = Phase::Connecting;
    assert_reports(&view, &["INV-SESSION-3", "INV-FENCE-1"]);

    // A graceful departure is what disarms it, so that pairing is legal.
    let mut view = clean_view();
    view.departure = Some(ReplicaDeparture::Graceful);
    assert_reports(&view, &[]);
}

#[test]
fn inv_gate_1_forces_a_gate_lying_about_its_hold_and_one_past_the_budget() {
    // Revert (d) `8d55cc4f` / FM-CLUSTER-097: the feed runs during the barrier.
    let mut view = clean_view();
    view.feed_gate.as_mut().unwrap().is_held = false;
    assert_reports(&view, &["INV-GATE-1"]);
    assert!(check_hard(&view)[0].detail.contains("held=false"));

    let mut view = clean_view();
    view.feed_gate.as_mut().unwrap().hold_remaining = Some(Duration::from_millis(101));
    assert_reports(&view, &["INV-GATE-1"]);

    // A released gate with no hold is the ordinary state.
    let mut view = clean_view();
    let gate = view.feed_gate.as_mut().unwrap();
    gate.is_held = false;
    gate.hold_remaining = None;
    assert_reports(&view, &[]);

    // A caller that does not know the budget cannot be wrong about it.
    let mut view = clean_view();
    let gate = view.feed_gate.as_mut().unwrap();
    gate.hold_remaining = Some(Duration::from_secs(60));
    gate.barrier_budget = None;
    assert_reports(&view, &[]);
}

#[test]
fn inv_fence_1_forces_the_dead_detector_and_the_wrong_disarm() {
    // FM-REPLICATION-041: a replica is streaming and the fence can never fire.
    let mut view = clean_view();
    view.fence.as_mut().unwrap().armed = false;
    assert_reports(&view, &["INV-FENCE-1"]);
    assert!(check_hard(&view)[0].detail.contains("[1]"));

    // FM-REPLICATION-062: the one departure that must fence switched it off.
    let mut view = clean_view();
    view.fence.as_mut().unwrap().armed = false;
    view.departure = Some(ReplicaDeparture::Lost);
    view.replicas.as_mut().unwrap().clear();
    assert_reports(&view, &["INV-SESSION-3", "INV-FENCE-1"]);
}

/// The one `DocumentedException`, so the tier is not a theory: chained
/// replication is reachable, reported by `check_all`, and never asserted.
#[test]
fn inv_role_1_is_reported_but_never_asserted() {
    let view = clean_view().with_role(RoleView::Replica {
        upstream: Some(addr(6379)),
    });
    assert_reports(&view, &[]);
    let reported = check_all(&view);
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].id, "INV-ROLE-1");
    assert!(reported[0].detail.contains("downstream session 1"));

    // A primary serving replicas is the supported topology.
    assert!(check_all(&clean_view()).is_empty());
}

// ---- the hook ----------------------------------------------------------

#[test]
fn the_hook_lets_a_clean_view_through() {
    debug_assert_view_clean(&clean_view(), "a-seam");
    debug_assert_view_clean(&ReplicationView::empty(), "a-seam");
}

#[test]
#[should_panic(expected = "replication invariants violated after a-seam")]
fn the_hook_panics_on_a_dirty_view() {
    let mut view = clean_view();
    view.offsets.as_mut().unwrap().landed = 101;
    debug_assert_view_clean(&view, "a-seam");
}

#[test]
#[should_panic(expected = "INV-OFFSET-1")]
fn the_hook_names_the_broken_invariant() {
    let mut view = clean_view();
    view.offsets.as_mut().unwrap().landed = 101;
    debug_assert_view_clean(&view, "a-seam");
}

/// A documented exception is reachable by construction, so the hook must never
/// panic on one — otherwise the tier would be a slow-motion `Hard`.
#[test]
fn the_hook_ignores_a_documented_exception() {
    debug_assert_view_clean(
        &clean_view().with_role(RoleView::Replica {
            upstream: Some(addr(6379)),
        }),
        "a-seam",
    );
}

// ---- per-seam hook tests -----------------------------------------------
//
// Each drives the real component and expects the panic, so deleting the hook
// at that seam turns this red. Where the seam's own output cannot be made
// dirty, the state it is checked against is pre-corrupted instead: the claim
// being forced is "this seam checks", not "this seam breaks".

#[test]
#[should_panic(expected = "ReplicationState::new_replication_id")]
fn the_new_replication_id_seam_is_hooked() {
    let mut state = clean_state();
    // The mint demotes the current id into the window, so a malformed current
    // id surfaces as a malformed secondary one.
    state.replication_id = "nonsense".to_string();
    state.new_replication_id(10);
}

#[test]
#[should_panic(expected = "ReplicationState::shift_replication_id")]
fn the_shift_replication_id_seam_is_hooked() {
    let mut state = clean_state();
    let same = state.replication_id.clone();
    state.shift_replication_id(same, 10);
}

#[test]
#[should_panic(expected = "ReplicationState::clear_secondary_window")]
fn the_clear_secondary_window_seam_is_hooked() {
    let mut state = clean_state();
    state.replication_id = "nonsense".to_string();
    state.clear_secondary_window();
}

#[test]
#[should_panic(expected = "ReplicationState::adopt_replication_history")]
fn the_adopt_replication_history_seam_is_hooked() {
    let mut state = clean_state();
    state.adopt_replication_history("nonsense".to_string());
}

#[test]
#[should_panic(expected = "ReplicationState::apply_staged_metadata")]
fn the_apply_staged_metadata_seam_is_hooked() {
    let mut state = clean_state();
    state.apply_staged_metadata(&crate::state::StagedReplicationMetadata {
        replication_id: "nonsense".to_string(),
        replication_offset: 10,
        checksum: None,
    });
}

/// The landed head is seeded from the applied counter at construction, so
/// lowering the counter behind the pair's back is the only way to invert it —
/// which is exactly the shape `INV-OFFSET-1` exists to catch.
fn inverted_applied_offset() -> (AppliedOffset, Arc<AtomicU64>) {
    let counter = Arc::new(AtomicU64::new(100));
    let applied = AppliedOffset::over(counter.clone());
    counter.store(1, Ordering::Release);
    (applied, counter)
}

#[test]
#[should_panic(expected = "AppliedOffset::advance_by")]
fn the_advance_by_seam_is_hooked() {
    let (applied, _counter) = inverted_applied_offset();
    applied.advance_by(1);
}

#[test]
#[should_panic(expected = "AppliedOffset::freeze")]
fn the_freeze_seam_is_hooked() {
    let (applied, _counter) = inverted_applied_offset();
    let _ = applied.freeze();
}

#[test]
#[should_panic(expected = "AppliedOffset::retire_replica_applies")]
fn the_retire_replica_applies_seam_is_hooked() {
    let (applied, _counter) = inverted_applied_offset();
    applied.retire_replica_applies();
}

fn replica_offset_over_a_malformed_identity() -> ReplicaOffset {
    let mut state = clean_state();
    state.replication_id = "nonsense".to_string();
    ReplicaOffset::new(
        Arc::new(RwLock::new(state)),
        Arc::new(AtomicU64::new(100)),
        AppliedOffset::over(Arc::new(AtomicU64::new(100))),
    )
}

#[test]
#[should_panic(expected = "ReplicaOffset::frame_advance")]
fn the_frame_advance_seam_is_hooked() {
    let offsets = replica_offset_over_a_malformed_identity();
    offsets.frame_advance(&crate::frame::ReplicationFrame::new(
        1,
        Bytes::from_static(b"x"),
    ));
}

#[test]
#[should_panic(expected = "ReplicaOffset::reset_to")]
fn the_reset_to_seam_is_hooked() {
    let offsets = replica_offset_over_a_malformed_identity();
    let _ = offsets.reset_to(50);
}

#[test]
#[should_panic(expected = "ReplicationRingBuffer::push")]
fn the_ring_buffer_push_seam_is_hooked() {
    let ring = ReplicationRingBuffer::new(16, 4096);
    ring.push(10, 0, Bytes::from_static(b"0123456789"));
    // A jump the stream never made: the ring would claim to cover the gap.
    ring.push(200, 0, Bytes::from_static(b"0123456789"));
}

#[test]
#[should_panic(expected = "ReplicationRingBuffer::arm_start")]
fn the_ring_buffer_arm_start_seam_is_hooked() {
    let ring = ReplicationRingBuffer::new(16, 4096);
    ring.push(10, 0, Bytes::from_static(b"0123456789"));
    // A floor above the data the ring holds skips buffered commands.
    ring.arm_start(20);
}

// `ReplicationRingBuffer::reset` has no forcing test on purpose: it clears the
// entries, the byte counter and the floor in one step under the entries lock,
// so the state it leaves is the empty one — unarmed, no entries, zero bytes —
// which every backlog entry accepts by construction. There is no input that
// makes its output dirty. The hook stays because a future reset that clears
// only part of that state is exactly the defect it would catch; see the note
// at the seam.

fn announced_session(
    tracker: &ReplicationTrackerImpl,
    port: u16,
) -> Arc<crate::replica_session::ReplicaSession> {
    let session = tracker.register_announced_replica(
        addr(6380),
        ReplicaAnnouncement {
            listening_port: port,
            ..Default::default()
        },
    );
    session.force_phase_for_test(Phase::Streaming);
    session
}

#[test]
#[should_panic(expected = "ReplicationTrackerImpl::register_announced_replica")]
fn the_register_announced_replica_seam_is_hooked() {
    let tracker = ReplicationTrackerImpl::new();
    let _first = announced_session(&tracker, 7000);
    // The second session for the same announced identity is only a violation
    // once it streams, so register it and let the *third* registration observe
    // the pair.
    let second = tracker.register_announced_replica(
        addr(6381),
        ReplicaAnnouncement {
            listening_port: 7000,
            ..Default::default()
        },
    );
    second.force_phase_for_test(Phase::Streaming);
    let _third = tracker.register_replica(addr(6382));
}

#[test]
#[should_panic(expected = "ReplicationTrackerImpl::unregister_replica")]
fn the_unregister_replica_seam_is_hooked() {
    let tracker = ReplicationTrackerImpl::new();
    // The spare is registered *before* the duplicate pair starts streaming, so
    // the registration hook sees a clean registry and this test can only fail
    // at the seam it names.
    let spare = tracker.register_replica(addr(6382));
    let _first = announced_session(&tracker, 7000);
    let _second = announced_session(&tracker, 7000);
    tracker.unregister_replica(spare.id());
}

#[test]
#[should_panic(expected = "ReplicationTrackerImpl::record_streaming_departure")]
fn the_record_streaming_departure_seam_is_hooked() {
    let tracker = ReplicationTrackerImpl::new();
    let _first = announced_session(&tracker, 7000);
    let _second = announced_session(&tracker, 7000);
    tracker.record_streaming_departure(ReplicaDeparture::Graceful);
}

#[test]
#[should_panic(expected = "ReplicaSession::set_phase")]
fn the_set_phase_seam_is_hooked() {
    let tracker = ReplicationTrackerImpl::new();
    let session = tracker.register_replica(addr(6380));
    session.force_phase_for_test(Phase::Disconnecting);
    // Nothing leaves the terminal phase.
    session.force_phase_for_test(Phase::Streaming);
}

// `ReplicaFeedGate::publish` has no forcing test either: its view is the gate
// alone, `is_held` and the remaining hold are two reads of one published
// deadline, and the barrier budget is owned by `frogdb-cluster` and so is
// never known at the gate. Nothing a caller can publish makes the entry fire.
// The hook stays for the same reason as `reset`'s: it is the *pair* of
// accessors it guards, and a future gate that latches `is_held` separately
// would break here first.
