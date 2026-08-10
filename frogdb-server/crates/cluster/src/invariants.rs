//! The cluster invariant catalog: what a well-formed [`ClusterStateInner`] is.
//!
//! Every entry is a pure function of the replicated state — no clocks, no I/O,
//! no `self` — so the same catalog can be evaluated from a unit test, from the
//! self-check hook the state machine runs after every transition, from a
//! property-based permutation harness, and from a live node. A violation
//! report has one shape everywhere: a stable [`Violation::id`] plus a detail
//! string naming the offending ids.
//!
//! # Tiers
//!
//! There are exactly two, and no third:
//!
//! - [`Tier::Hard`] — the state is unreachable by any correct transition, so a
//!   violation is a defect by definition. These are what [`check_hard`]
//!   returns and what the state-machine hook panics on.
//! - [`Tier::DocumentedException`] — the state is reachable today, the
//!   behavior is deliberate, and the entry carries the [`Citation`] that says
//!   so. The citation is a field of the variant, so an exception without one
//!   does not compile; [`Citation`]'s constructors are `const fn`s that reject
//!   the empty string, and [`CATALOG`] is a `static`, so an exception citing
//!   `""` fails to compile too. The tier exists to force a known-dirty state
//!   into an explicit ruling rather than a silent shrug.
//!
//! The ten seed entries land as eleven, ten of them [`Tier::Hard`]. The
//! behaviors that would otherwise have needed an exception (`RemoveNode`
//! leaving dangling migrations and parent pointers, `CompleteSlotMigration`'s
//! unguarded owner insert) were fixed instead, under FM-CLUSTER-002 and
//! FM-CLUSTER-033. The one seed that splits is INV-REF-3, "a Replica's
//! `primary_id` names an existing Primary": the *existence* half is HARD, and
//! the *is a Primary* half is INV-REF-3B, the catalog's single
//! [`Tier::DocumentedException`], because the transitions admit replication
//! chains today. Its citation names the issue that closes the gap.
//!
//! # Cross-reference with the failure-mode spec
//!
//! An FM row states its guarantee for one transition; an entry here states a
//! well-formedness claim for every state. Where the two meet, the citation is
//! recorded on both sides and neither may drift:
//!
//! - each entry's `check_*` function names, in prose, the rows it generalizes
//!   — "deleting the code that row names makes this entry fire";
//! - each generalized row carries a `Catalog` field naming the entry ids, and
//!   `just lint-failure-modes` fails on an `INV-*` id the spec mentions and
//!   [`CATALOG`] does not define.
//!
//! Prose, not a `// FM-…` tag: a tag is the lint's claim that the *item below
//! it* forces that row, and these functions are catalog entries rather than
//! forcing tests. See `.scratch/hardening/specs/cluster-failure-modes.md`,
//! "The `Catalog` field".
//!
//! # What "clean" does not claim
//!
//! Two states the catalog deliberately accepts, because a correct transition
//! produces them:
//!
//! - A **detached replica** — `role == Replica`, `primary_id == None`. This is
//!   what `RemoveNode`/`FORGET` leaves behind (Redis' `freeClusterNode` nulls
//!   its replicas' `slaveof` and promotes nobody), because minting a
//!   replication identity is a role transition and those belong to `SetRole`
//!   and `Failover`. INV-REF-3 is about *dangling* parent pointers, not about
//!   every replica having a parent.
//! - An **unassigned migrating slot** — `BeginSlotMigration` accepts a slot
//!   with no recorded owner, because the slot map can be empty on a follower
//!   that was seeded locally. INV-MIG-1 constrains the owner when there is
//!   one.

use std::collections::BTreeMap;

use crate::state::ClusterStateInner;
use crate::types::{CLUSTER_SLOTS, ConfigEpoch, NodeId, NodeRole};

/// A single violated invariant, at one offending place in the state.
///
/// One check can produce several: a state with three dangling slot owners
/// reports three `INV-REF-1` violations, so the detail names which slots
/// rather than only how many.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The stable catalog id, e.g. `"INV-REF-1"`. Stable across refactors —
    /// specs, issues and checker output all quote it.
    pub id: &'static str,
    /// What is wrong, naming the concrete ids involved.
    pub detail: String,
}

impl Violation {
    fn new(id: &'static str, detail: String) -> Self {
        Self { id, detail }
    }
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.id, self.detail)
    }
}

/// The ruling that makes a [`Tier::DocumentedException`] legitimate.
///
/// Constructed only through [`Citation::failure_mode`] or [`Citation::issue`],
/// both of which reject an empty reference. Because [`CATALOG`] is a `static`,
/// those `const fn` assertions run at compile time: a citation-less — or
/// blank-citation — exception is a build error, not a review comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Citation(&'static str);

impl Citation {
    /// Cite the failure-mode row that rules the state deliberate, e.g.
    /// `"FM-CLUSTER-033"`.
    pub const fn failure_mode(row: &'static str) -> Self {
        assert!(
            !row.is_empty(),
            "a DOCUMENTED-EXCEPTION must cite a failure-mode row"
        );
        Self(row)
    }

    /// Cite the issue that rules the state deliberate, e.g. a path under
    /// `.scratch/cluster-correctness/issues/`.
    pub const fn issue(reference: &'static str) -> Self {
        assert!(
            !reference.is_empty(),
            "a DOCUMENTED-EXCEPTION must cite an issue"
        );
        Self(reference)
    }

    /// The cited reference.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// How seriously the catalog takes a violation of an entry. See the module
/// docs; there are two tiers and there is no third.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// A violation is a defect. Asserted by the state-machine hook.
    Hard,
    /// The state is reachable and deliberate; the [`Citation`] says where that
    /// was ruled. Reported by [`check_all`], never asserted.
    DocumentedException(Citation),
}

/// One catalog entry: an id, the claim in one line, its tier, and the pure
/// function that evaluates it.
pub struct Invariant {
    /// Stable id, quoted by specs, issues and checker output.
    pub id: &'static str,
    /// The claim, in the words the PRD's catalog table uses.
    pub claim: &'static str,
    /// [`Tier::Hard`] unless a ruling says otherwise.
    pub tier: Tier,
    /// Pure evaluation over the replicated state.
    check: fn(&ClusterStateInner) -> Vec<Violation>,
}

impl Invariant {
    /// True for [`Tier::Hard`] entries — the ones the hook asserts.
    pub fn is_hard(&self) -> bool {
        matches!(self.tier, Tier::Hard)
    }
}

impl std::fmt::Debug for Invariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Invariant")
            .field("id", &self.id)
            .field("claim", &self.claim)
            .field("tier", &self.tier)
            .finish_non_exhaustive()
    }
}

/// The catalog. Order is the report order, so a state that breaks several
/// entries reports them the same way every time.
pub static CATALOG: &[Invariant] = &[
    Invariant {
        id: "INV-REF-1",
        claim: "every slot_assignment owner exists in nodes",
        tier: Tier::Hard,
        check: check_ref_1,
    },
    Invariant {
        id: "INV-REF-2",
        claim: "every migration's source and target exist in nodes",
        tier: Tier::Hard,
        check: check_ref_2,
    },
    Invariant {
        id: "INV-REF-3",
        claim: "a Replica's primary_id, when set, names an existing node",
        tier: Tier::Hard,
        check: check_ref_3,
    },
    Invariant {
        id: "INV-REF-3B",
        claim: "a Replica's parent is a Primary (no replication chains)",
        // The PRD states INV-REF-3 as "names an existing Primary". The
        // existence half is HARD; this half is not, because `AddNode` and
        // `SetRole` both accept a `primary_id` that is itself a replica and
        // the runtime's failure-detector suite builds exactly that chain to
        // prove a replica's death is not a failover. Redis refuses the shape
        // (`CLUSTER REPLICATE`: "I can only replicate a master, not a
        // replica"), so this is a gap to close, not a design: the citation is
        // the issue that closes it, and the entry becomes HARD with it.
        tier: Tier::DocumentedException(Citation::issue(
            ".scratch/cluster-correctness/issues/open/\
             14-role-transitions-admit-malformed-parents.md",
        )),
        check: check_ref_3b,
    },
    Invariant {
        id: "INV-REF-4",
        claim: "a Primary has primary_id == None",
        tier: Tier::Hard,
        check: check_ref_4,
    },
    Invariant {
        id: "INV-EPOCH-1",
        claim: "config_epoch >= max(node config epochs)",
        tier: Tier::Hard,
        check: check_epoch_1,
    },
    Invariant {
        id: "INV-EPOCH-2",
        claim: "a nonzero node epoch is unique among Primaries",
        tier: Tier::Hard,
        check: check_epoch_2,
    },
    Invariant {
        id: "INV-HANDOFF-1",
        claim: "handoff_seq >= max(handoff.seq over migrations)",
        tier: Tier::Hard,
        check: check_handoff_1,
    },
    Invariant {
        id: "INV-HANDOFF-2",
        claim: "a handoff lives inside the migration for its own slot; drained implies prepared",
        tier: Tier::Hard,
        check: check_handoff_2,
    },
    Invariant {
        id: "INV-MIG-1",
        claim: "a migrating slot's current owner is the migration's source",
        tier: Tier::Hard,
        check: check_mig_1,
    },
    Invariant {
        id: "INV-SLOT-1",
        claim: "every slot key is below CLUSTER_SLOTS",
        tier: Tier::Hard,
        check: check_slot_1,
    },
];

/// Evaluate `catalog` against `state`, optionally restricted to the HARD tier.
///
/// The two public entry points differ only in that flag, so the filter and the
/// evaluation cannot drift apart between them.
fn check_catalog(
    catalog: &[Invariant],
    state: &ClusterStateInner,
    hard_only: bool,
) -> Vec<Violation> {
    catalog
        .iter()
        .filter(|inv| inv.is_hard() || !hard_only)
        .flat_map(|inv| (inv.check)(state))
        .collect()
}

/// Every violation, of every tier. The reporting view — a live self-check
/// wants to show a documented exception as well as a defect.
pub fn check_all(state: &ClusterStateInner) -> Vec<Violation> {
    check_catalog(CATALOG, state, false)
}

/// Only violations of HARD entries. The asserting view: anything here is a
/// defect.
pub fn check_hard(state: &ClusterStateInner) -> Vec<Violation> {
    check_catalog(CATALOG, state, true)
}

/// Render violations one per line for a panic message.
pub fn render(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(|v| format!("  - {v}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Panic if `state` violates a HARD invariant, naming the seam that produced
/// it.
///
/// Compiled into test and debug builds only: the checks are linear in the slot
/// table, and the release build pays nothing for them by construction. Every
/// mutation seam of the state machine calls this, which is what upgrades the
/// whole existing cluster suite into an invariant suite.
#[cfg(any(test, debug_assertions))]
#[track_caller]
pub(crate) fn debug_assert_clean(state: &ClusterStateInner, seam: &str) {
    let violations = check_hard(state);
    assert!(
        violations.is_empty(),
        "cluster state invariants violated after {seam}:\n{}",
        render(&violations)
    );
}

// ---- INV-REF-1 -------------------------------------------------------------

/// A slot owned by a node the topology does not have is a slot nothing can be
/// redirected to, and the coverage readers count it as healthy.
///
/// Generalizes the ownership half of FM-CLUSTER-002 (`FORGET` leaves the
/// departing node's slots *unassigned* rather than pointing at it),
/// FM-CLUSTER-003 (`AssignSlots` against an unknown node is `NodeNotFound`),
/// FM-CLUSTER-033 (whose `NOT observable` already names this entry as "the
/// ghost owner"), FM-CLUSTER-040/041 (a failover transfers every slot to a
/// successor it validated) and FM-CLUSTER-042 (the replayed failover's "end
/// state is coherent").
fn check_ref_1(state: &ClusterStateInner) -> Vec<Violation> {
    state
        .slot_assignment
        .iter()
        .filter(|(_, owner)| !state.nodes.contains_key(owner))
        .map(|(slot, owner)| {
            Violation::new(
                "INV-REF-1",
                format!("slot {slot} is owned by node {owner}, which is not a member"),
            )
        })
        .collect()
}

// ---- INV-REF-2 -------------------------------------------------------------

/// A migration naming a node that is gone can never complete, and blocks its
/// slot until someone cancels it.
///
/// Generalizes FM-CLUSTER-032 (a begin validates both endpoints before
/// recording anything), FM-CLUSTER-002 and FM-CLUSTER-036 (the two removal
/// paths prune every migration naming the node they remove, through the same
/// `prune_migrations_naming` helper).
fn check_ref_2(state: &ClusterStateInner) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (slot, migration) in &state.migrations {
        for (role, node_id) in [
            ("source", migration.source_node),
            ("target", migration.target_node),
        ] {
            if !state.nodes.contains_key(&node_id) {
                violations.push(Violation::new(
                    "INV-REF-2",
                    format!(
                        "migration of slot {slot} names {role} node {node_id}, \
                         which is not a member"
                    ),
                ));
            }
        }
    }
    violations
}

// ---- INV-REF-3 -------------------------------------------------------------

/// A replica whose parent pointer leads nowhere streams from a node the
/// topology does not link it to. A *detached* replica (`primary_id == None`) is
/// not that: see the module docs.
///
/// Generalizes FM-CLUSTER-005 (a `SetRole{Replica}` naming a non-member parent
/// is `NodeNotFound`), FM-CLUSTER-002 (`FORGET` detaches the departing node's
/// replicas instead of leaving the pointer) and FM-CLUSTER-040/041 (a failover
/// re-parents the old primary's siblings onto the successor).
fn check_ref_3(state: &ClusterStateInner) -> Vec<Violation> {
    replicas_with_parents(state)
        .filter(|(_, primary_id)| !state.nodes.contains_key(primary_id))
        .map(|(node_id, primary_id)| {
            Violation::new(
                "INV-REF-3",
                format!("replica {node_id} names primary {primary_id}, which is not a member"),
            )
        })
        .collect()
}

// ---- INV-REF-3B ------------------------------------------------------------

/// The other half of the PRD's INV-REF-3 claim — the parent is a *primary* —
/// held apart because it is the one seed the current transitions do not
/// uphold. See the entry's citation in [`CATALOG`].
///
/// It would complete FM-CLUSTER-005 — that row's title says "names a primary
/// that exists", but its `Observable` only requires the parent to *resolve*,
/// which is [`check_ref_3`]. The row's `Catalog` cell therefore names this
/// entry as the open half rather than as a universal check: it is reported,
/// never asserted, until issue 14 closes the gap.
fn check_ref_3b(state: &ClusterStateInner) -> Vec<Violation> {
    replicas_with_parents(state)
        .filter(|(_, primary_id)| {
            state
                .nodes
                .get(primary_id)
                .is_some_and(|parent| parent.role != NodeRole::Primary)
        })
        .map(|(node_id, primary_id)| {
            Violation::new(
                "INV-REF-3B",
                format!(
                    "replica {node_id} names {primary_id} as its primary, but {primary_id} is a \
                     replica"
                ),
            )
        })
        .collect()
}

/// Every replica that claims a parent, as `(replica id, parent id)`. Shared by
/// the two halves of the parent-pointer claim so they cannot disagree about
/// which nodes they are talking about.
fn replicas_with_parents(state: &ClusterStateInner) -> impl Iterator<Item = (NodeId, NodeId)> + '_ {
    state
        .nodes
        .values()
        .filter(|node| node.role == NodeRole::Replica)
        .filter_map(|node| node.primary_id.map(|primary_id| (node.id, primary_id)))
}

// ---- INV-REF-4 -------------------------------------------------------------

/// A primary carrying a parent pointer is a role/parent desync: the metadata
/// plane calls it a primary while the pointer says it should be following
/// someone.
///
/// Generalizes FM-CLUSTER-001 (a re-registration restores role *and*
/// `primary_id` from the recorded node, so the pair cannot come apart),
/// FM-CLUSTER-006 (both reset forms force this node to `Primary` with no
/// parent), FM-CLUSTER-040 ("the successor is a primary with no parent"),
/// FM-CLUSTER-041 (the demoted primary takes the role and the pointer in one
/// entry) and FM-CLUSTER-042 (the replay's coherent end state).
fn check_ref_4(state: &ClusterStateInner) -> Vec<Violation> {
    state
        .nodes
        .values()
        .filter(|node| node.role == NodeRole::Primary && node.primary_id.is_some())
        .map(|node| {
            Violation::new(
                "INV-REF-4",
                format!(
                    "primary {} carries primary_id {:?}",
                    node.id, node.primary_id
                ),
            )
        })
        .collect()
}

// ---- INV-EPOCH-1 -----------------------------------------------------------

/// The cluster-wide counter dominates every per-node epoch, so the next epoch
/// it mints outranks every claim already recorded (FM-CLUSTER-010).
///
/// Generalizes that row's dominance relation — which the row itself can only
/// state over one generated command sequence — plus FM-CLUSTER-011 (a
/// collision mint lands strictly above the counter), FM-CLUSTER-076
/// (`SET-CONFIG-EPOCH` ratchets the counter up to the assigned value and never
/// follows it down), FM-CLUSTER-040/041 (the successor is stamped with the
/// bumped counter) and FM-CLUSTER-042 (the replay bumps again, which is safe
/// precisely because of this relation).
fn check_epoch_1(state: &ClusterStateInner) -> Vec<Violation> {
    let Some((node_id, claimed)) = state
        .nodes
        .values()
        .map(|node| (node.id, node.config_epoch))
        .max_by_key(|&(_, epoch)| epoch)
    else {
        return Vec::new();
    };
    if state.config_epoch < claimed {
        return vec![Violation::new(
            "INV-EPOCH-1",
            format!(
                "cluster config_epoch {} is below node {node_id}'s claimed epoch {claimed}",
                state.config_epoch
            ),
        )];
    }
    Vec::new()
}

// ---- INV-EPOCH-2 -----------------------------------------------------------

/// Only a primary's epoch arbitrates slot ownership, so two primaries holding
/// the same nonzero epoch have no tie-break. Zero is "unassigned" and never
/// collides, matching Redis' `clusterHandleConfigEpochCollision`.
///
/// Generalizes FM-CLUSTER-010's "no two *primaries* share a nonzero epoch",
/// FM-CLUSTER-011 (the collision is resolved against the arriving node, and a
/// replica claiming a primary's epoch is not one) and FM-CLUSTER-012 (epoch
/// `0` is exempt here for the reason it is exempt there). FM-CLUSTER-040/041
/// stamp the successor, which is where a promotion could manufacture a tie.
fn check_epoch_2(state: &ClusterStateInner) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut claimed_by: BTreeMap<ConfigEpoch, NodeId> = BTreeMap::new();
    for node in state.nodes.values() {
        if node.role != NodeRole::Primary || node.config_epoch == 0 {
            continue;
        }
        if let Some(previous) = claimed_by.insert(node.config_epoch, node.id) {
            violations.push(Violation::new(
                "INV-EPOCH-2",
                format!(
                    "primaries {previous} and {} both claim config epoch {}",
                    node.id, node.config_epoch
                ),
            ));
        }
    }
    violations
}

// ---- INV-HANDOFF-1 ---------------------------------------------------------

/// The generation counter is spent, never re-derived: a `seq` above it means
/// the counter was rewound behind a live handoff and the next prepare will
/// reuse a generation a fence already accepted (FM-CLUSTER-100).
///
/// Generalizes FM-CLUSTER-100 across both restore vehicles — the hook runs at
/// `from_snapshot` and `restore_from_snapshot`, the exact seam that defect
/// lived on — and FM-CLUSTER-086, which is the row that makes `handoff_seq` a
/// replicated counter every follow-up message filters on.
fn check_handoff_1(state: &ClusterStateInner) -> Vec<Violation> {
    state
        .migrations
        .iter()
        .filter_map(|(slot, migration)| migration.handoff.as_ref().map(|h| (slot, h)))
        .filter(|(_, handoff)| handoff.seq > state.handoff_seq)
        .map(|(slot, handoff)| {
            Violation::new(
                "INV-HANDOFF-1",
                format!(
                    "slot {slot} carries handoff seq {} above the generation counter {}",
                    handoff.seq, state.handoff_seq
                ),
            )
        })
        .collect()
}

// ---- INV-HANDOFF-2 ---------------------------------------------------------

/// Barrier state is only ever reached through its own migration record: a
/// migration filed under a slot it does not name would arm the barrier for one
/// slot and fence another, and a `drained` handoff carrying the unminted
/// generation `0` was marked drained without ever having been prepared.
///
/// Generalizes FM-CLUSTER-088 (handoffs are stored per migration record keyed
/// by slot, so there is no shared cell for two slots to contend on) and
/// FM-CLUSTER-090 (only the record's own slot is fenced) on the first clause;
/// FM-CLUSTER-084 (`Complete` admits only a drained record) and
/// FM-CLUSTER-086 (a confirm takes effect only when its `seq` matches the
/// current attempt) on the second.
fn check_handoff_2(state: &ClusterStateInner) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (slot, migration) in &state.migrations {
        if *slot != migration.slot {
            violations.push(Violation::new(
                "INV-HANDOFF-2",
                format!(
                    "migration filed under slot {slot} records slot {}",
                    migration.slot
                ),
            ));
        }
        if let Some(handoff) = &migration.handoff
            && handoff.drained
            && handoff.seq == 0
        {
            violations.push(Violation::new(
                "INV-HANDOFF-2",
                format!("slot {slot} has a drained handoff with the unminted seq 0"),
            ));
        }
    }
    violations
}

// ---- INV-MIG-1 -------------------------------------------------------------

/// Ownership does not drift out from under a migration in flight: the record
/// authorizes moving the slot *from* its source, so a slot whose owner is
/// someone else would hand a keyspace over on the strength of a stale record.
/// An unassigned slot is not drift — see the module docs.
///
/// Generalizes FM-CLUSTER-032's owner check (which runs once, at begin time,
/// against a record that then outlives the check), FM-CLUSTER-033 (the swap
/// moves ownership and drops the record in the same transition) and
/// FM-CLUSTER-084 (ownership moves only under a prepared, drained,
/// still-armed handoff).
fn check_mig_1(state: &ClusterStateInner) -> Vec<Violation> {
    state
        .migrations
        .iter()
        .filter_map(|(slot, migration)| {
            let owner = *state.slot_assignment.get(slot)?;
            (owner != migration.source_node).then(|| {
                Violation::new(
                    "INV-MIG-1",
                    format!(
                        "slot {slot} is migrating from {} but is owned by {owner}",
                        migration.source_node
                    ),
                )
            })
        })
        .collect()
}

// ---- INV-SLOT-1 ------------------------------------------------------------

/// Slot keys are `0..CLUSTER_SLOTS`. A key above the range is unreachable by
/// hashing, so nothing routes to it and nothing ever clears it.
///
/// Generalizes no FM row, deliberately, and the spec says so: FM-CLUSTER-018
/// derives the range by hashing and FM-CLUSTER-075 enforces it at the
/// `SlotRange` parse boundary, but neither states it of the replicated slot
/// map, which is reachable from `AssignSlots` and `BeginSlotMigration` with a
/// `u16` that never passed either. This entry is the state-side backstop.
fn check_slot_1(state: &ClusterStateInner) -> Vec<Violation> {
    let mut violations = Vec::new();
    for slot in state.slot_assignment.keys() {
        if *slot >= CLUSTER_SLOTS {
            violations.push(Violation::new(
                "INV-SLOT-1",
                format!("slot_assignment holds slot {slot}, at or above {CLUSTER_SLOTS}"),
            ));
        }
    }
    for (slot, migration) in &state.migrations {
        if *slot >= CLUSTER_SLOTS {
            violations.push(Violation::new(
                "INV-SLOT-1",
                format!("migrations holds slot {slot}, at or above {CLUSTER_SLOTS}"),
            ));
        }
        if migration.slot >= CLUSTER_SLOTS {
            violations.push(Violation::new(
                "INV-SLOT-1",
                format!(
                    "migration under key {slot} records slot {}, at or above {CLUSTER_SLOTS}",
                    migration.slot
                ),
            ));
        }
    }
    violations
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::SocketAddr;

    use super::*;
    use crate::types::{NodeInfo, SlotHandoff, SlotMigration};

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn primary(id: NodeId) -> NodeInfo {
        NodeInfo::new_primary(id, addr(6379 + id as u16), addr(16379 + id as u16))
    }

    fn replica(id: NodeId, of: NodeId) -> NodeInfo {
        NodeInfo::new_replica(id, addr(6379 + id as u16), addr(16379 + id as u16), of)
    }

    fn nodes(list: Vec<NodeInfo>) -> BTreeMap<NodeId, NodeInfo> {
        list.into_iter().map(|n| (n.id, n)).collect()
    }

    fn handoff(seq: u64, drained: bool) -> SlotHandoff {
        SlotHandoff {
            seq,
            prepared_at_ms: 1_000_000,
            barrier_ms: 100,
            lease_ms: 5_000,
            drained,
        }
    }

    /// A state that satisfies every HARD entry, and does so *tightly*: it sits
    /// on the boundary of each check (a detached replica, an unassigned
    /// migrating slot, two primaries at the unassigned epoch `0`, a handoff at
    /// exactly the generation counter, slot `CLUSTER_SLOTS - 1`) so a check
    /// that widened by one case is caught here rather than passing vacuously.
    fn clean_state() -> ClusterStateInner {
        let mut node_map = nodes(vec![
            primary(1),
            primary(2),
            replica(3, 1),
            replica(4, 1),
            primary(5),
            primary(6),
        ]);
        // Primaries 2 and 6 sit at the *unassigned* epoch 0, which INV-EPOCH-2
        // skips: sharing it is not a collision.
        node_map.get_mut(&1).unwrap().config_epoch = 9;
        node_map.get_mut(&5).unwrap().config_epoch = 11;
        // A detached replica: what `RemoveNode` leaves behind, deliberately.
        node_map.get_mut(&4).unwrap().primary_id = None;

        ClusterStateInner {
            nodes: node_map,
            slot_assignment: BTreeMap::from([
                (0, 1),
                (10, 2),
                (CLUSTER_SLOTS - 1, 2),
                // Slot 20 is owned by the source of the migration below.
                (20, 1),
            ]),
            config_epoch: 12,
            migrations: BTreeMap::from([
                // Owner == source: the ordinary in-flight case.
                (
                    20,
                    SlotMigration {
                        slot: 20,
                        source_node: 1,
                        target_node: 2,
                        handoff: Some(handoff(4, true)),
                    },
                ),
                // Unassigned slot: legal, the slot map can be empty on a
                // follower seeded locally.
                (
                    21,
                    SlotMigration {
                        slot: 21,
                        source_node: 2,
                        target_node: 5,
                        handoff: None,
                    },
                ),
                // A handoff at exactly the generation counter (INV-HANDOFF-1
                // is `>`, not `>=`) that is *not* drained and carries seq 0 —
                // the discriminator for INV-HANDOFF-2's `drained &&` clause.
                (
                    22,
                    SlotMigration {
                        slot: 22,
                        source_node: 5,
                        target_node: 6,
                        handoff: Some(handoff(0, false)),
                    },
                ),
            ]),
            handoff_seq: 4,
            ..ClusterStateInner::default()
        }
    }

    /// The ids reported for `state`, deduplicated, so a test asserts *which*
    /// entries fired without depending on how many places each fired at.
    fn reported(state: &ClusterStateInner) -> BTreeSet<&'static str> {
        check_hard(state).into_iter().map(|v| v.id).collect()
    }

    fn assert_reports(state: &ClusterStateInner, expected: &[&'static str]) {
        let got = reported(state);
        let want: BTreeSet<&'static str> = expected.iter().copied().collect();
        assert_eq!(
            got,
            want,
            "unexpected violation set; full report:\n{}",
            render(&check_hard(state))
        );
    }

    #[test]
    fn the_clean_fixture_violates_nothing() {
        let state = clean_state();
        assert_eq!(
            check_hard(&state),
            Vec::new(),
            "the boundary fixture must be clean:\n{}",
            render(&check_hard(&state))
        );
        assert_eq!(check_all(&state), Vec::new());
    }

    #[test]
    fn an_empty_state_violates_nothing() {
        assert_eq!(check_hard(&ClusterStateInner::default()), Vec::new());
    }

    // ---- catalog shape -----------------------------------------------------

    #[test]
    fn the_catalog_holds_the_seed_entries_with_unique_ids() {
        let ids: Vec<&str> = CATALOG.iter().map(|inv| inv.id).collect();
        assert_eq!(
            ids,
            vec![
                "INV-REF-1",
                "INV-REF-2",
                "INV-REF-3",
                "INV-REF-3B",
                "INV-REF-4",
                "INV-EPOCH-1",
                "INV-EPOCH-2",
                "INV-HANDOFF-1",
                "INV-HANDOFF-2",
                "INV-MIG-1",
                "INV-SLOT-1",
            ]
        );
        let unique: BTreeSet<&str> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len(), "catalog ids must be unique");
        assert!(
            CATALOG.iter().all(|inv| !inv.claim.is_empty()),
            "every entry states its claim"
        );
    }

    /// Every entry is HARD except the one documented exception, and that one
    /// carries a citation. A new exception slipped in without a ruling fails
    /// here as well as in review.
    #[test]
    fn only_the_cited_chain_entry_is_excepted() {
        for inv in CATALOG {
            match inv.tier {
                Tier::Hard => assert_ne!(inv.id, "INV-REF-3B"),
                Tier::DocumentedException(citation) => {
                    assert_eq!(inv.id, "INV-REF-3B", "undeclared exception: {}", inv.id);
                    assert!(
                        citation
                            .as_str()
                            .contains("14-role-transitions-admit-malformed-parents"),
                        "the exception must cite the issue that retires it: {}",
                        citation.as_str()
                    );
                }
            }
        }
        assert_eq!(
            CATALOG.iter().filter(|inv| !inv.is_hard()).count(),
            1,
            "exactly one entry is excepted"
        );
    }

    /// A DOCUMENTED-EXCEPTION is reported by [`check_all`] and skipped by
    /// [`check_hard`] — the only behavioral difference between the tiers, and
    /// the reason a citation-less exception must not compile.
    #[test]
    fn the_hard_filter_is_what_separates_the_two_tiers() {
        fn always_dirty(_: &ClusterStateInner) -> Vec<Violation> {
            vec![Violation::new("INV-TEST", "always".to_string())]
        }
        let catalog = [
            Invariant {
                id: "INV-TEST-HARD",
                claim: "hard",
                tier: Tier::Hard,
                check: always_dirty,
            },
            Invariant {
                id: "INV-TEST-EXCEPT",
                claim: "excepted",
                tier: Tier::DocumentedException(Citation::failure_mode("FM-CLUSTER-000")),
                check: always_dirty,
            },
        ];
        let state = ClusterStateInner::default();
        assert_eq!(check_catalog(&catalog, &state, true).len(), 1);
        assert_eq!(check_catalog(&catalog, &state, false).len(), 2);
        assert!(catalog[0].is_hard());
        assert!(!catalog[1].is_hard());
    }

    #[test]
    fn a_citation_carries_its_reference() {
        assert_eq!(
            Citation::failure_mode("FM-CLUSTER-033").as_str(),
            "FM-CLUSTER-033"
        );
        assert_eq!(
            Citation::issue("issues/open/02.md").as_str(),
            "issues/open/02.md"
        );
        for inv in CATALOG {
            if let Tier::DocumentedException(citation) = inv.tier {
                assert!(
                    !citation.as_str().is_empty(),
                    "{} cites nothing — the const assertions should have caught this",
                    inv.id
                );
            }
        }
    }

    #[test]
    fn a_violation_renders_id_and_detail() {
        let violations = vec![
            Violation::new("INV-REF-1", "a".to_string()),
            Violation::new("INV-REF-2", "b".to_string()),
        ];
        assert_eq!(violations[0].to_string(), "INV-REF-1: a");
        assert_eq!(render(&violations), "  - INV-REF-1: a\n  - INV-REF-2: b");
        assert_eq!(render(&[]), "");
    }

    // ---- forcing tests: one per HARD entry ---------------------------------

    #[test]
    fn inv_ref_1_forces_a_slot_owned_by_a_stranger() {
        let mut state = clean_state();
        state.slot_assignment.insert(30, 404);
        assert_reports(&state, &["INV-REF-1"]);
        assert!(check_hard(&state)[0].detail.contains("404"));
    }

    #[test]
    fn inv_ref_2_forces_a_migration_naming_a_stranger_on_either_leg() {
        let mut state = clean_state();
        state.migrations.insert(
            31,
            SlotMigration {
                slot: 31,
                source_node: 404,
                target_node: 1,
                handoff: None,
            },
        );
        assert_reports(&state, &["INV-REF-2"]);
        assert!(check_hard(&state)[0].detail.contains("source node 404"));

        let mut state = clean_state();
        state.migrations.insert(
            31,
            SlotMigration {
                slot: 31,
                source_node: 1,
                target_node: 404,
                handoff: None,
            },
        );
        assert_reports(&state, &["INV-REF-2"]);
        assert!(check_hard(&state)[0].detail.contains("target node 404"));
    }

    #[test]
    fn inv_ref_3_forces_a_dangling_parent_pointer() {
        let mut state = clean_state();
        state.nodes.get_mut(&3).unwrap().primary_id = Some(404);
        assert_reports(&state, &["INV-REF-3"]);
        assert!(check_hard(&state)[0].detail.contains("not a member"));
    }

    /// The chain half is the documented exception: reported by `check_all`,
    /// invisible to `check_hard` and therefore to the state-machine hook. When
    /// issue 14 lands this becomes an `assert_reports` like its siblings.
    #[test]
    fn inv_ref_3b_reports_a_replica_parented_onto_a_replica_without_asserting() {
        let mut state = clean_state();
        // Node 3 is a replica; pointing node 4 at it makes a chain, which
        // cluster mode has no replication identity for.
        state.nodes.get_mut(&4).unwrap().primary_id = Some(3);
        assert_reports(&state, &[]);

        let reported = check_all(&state);
        assert_eq!(reported.len(), 1, "{reported:?}");
        assert_eq!(reported[0].id, "INV-REF-3B");
        assert!(reported[0].detail.contains("is a replica"));
    }

    /// The two halves of the old INV-REF-3 do not double-report: a parent that
    /// is absent is dangling, not a chain, and only the HARD half fires.
    #[test]
    fn the_two_halves_of_the_parent_claim_do_not_overlap() {
        let mut state = clean_state();
        state.nodes.get_mut(&3).unwrap().primary_id = Some(404);
        state.nodes.get_mut(&4).unwrap().primary_id = Some(3);
        let ids: Vec<&str> = check_all(&state).iter().map(|v| v.id).collect();
        assert_eq!(ids, vec!["INV-REF-3", "INV-REF-3B"]);
    }

    #[test]
    fn inv_ref_4_forces_a_primary_carrying_a_parent() {
        let mut state = clean_state();
        state.nodes.get_mut(&2).unwrap().primary_id = Some(1);
        assert_reports(&state, &["INV-REF-4"]);
        assert!(check_hard(&state)[0].detail.contains("primary 2"));
    }

    #[test]
    fn inv_epoch_1_forces_a_counter_below_a_node_claim() {
        let mut state = clean_state();
        state.nodes.get_mut(&2).unwrap().config_epoch = state.config_epoch + 1;
        assert_reports(&state, &["INV-EPOCH-1"]);
        assert!(check_hard(&state)[0].detail.contains("node 2"));

        // Equality is clean: the counter dominates, it does not exceed.
        let mut state = clean_state();
        state.nodes.get_mut(&2).unwrap().config_epoch = state.config_epoch;
        assert_reports(&state, &[]);
    }

    #[test]
    fn inv_epoch_2_forces_two_primaries_at_one_epoch() {
        let mut state = clean_state();
        state.nodes.get_mut(&2).unwrap().config_epoch = 9; // node 1 already holds 9
        assert_reports(&state, &["INV-EPOCH-2"]);
        assert!(check_hard(&state)[0].detail.contains("epoch 9"));

        // The same epoch on a replica is not a collision: only a primary's
        // epoch arbitrates slot ownership. Both replicas take node 1's epoch,
        // so neither the replica-vs-primary nor the replica-vs-replica pair is
        // reported.
        let mut state = clean_state();
        state.nodes.get_mut(&3).unwrap().config_epoch = 9;
        state.nodes.get_mut(&4).unwrap().config_epoch = 9;
        assert_reports(&state, &[]);
    }

    #[test]
    fn inv_handoff_1_forces_a_seq_above_the_generation_counter() {
        let mut state = clean_state();
        state.handoff_seq = 3; // slot 20 carries seq 4
        assert_reports(&state, &["INV-HANDOFF-1"]);
        assert!(check_hard(&state)[0].detail.contains("slot 20"));
    }

    #[test]
    fn inv_handoff_2_forces_a_migration_filed_under_a_foreign_slot() {
        let mut state = clean_state();
        state.migrations.insert(
            40,
            SlotMigration {
                slot: 41,
                source_node: 1,
                target_node: 2,
                handoff: None,
            },
        );
        assert_reports(&state, &["INV-HANDOFF-2"]);
        assert!(check_hard(&state)[0].detail.contains("records slot 41"));
    }

    #[test]
    fn inv_handoff_2_forces_a_drained_handoff_that_was_never_prepared() {
        let mut state = clean_state();
        // Slot 22's handoff carries the unminted seq 0; marking it drained is
        // the state a confirmation that skipped the prepare would leave.
        state
            .migrations
            .get_mut(&22)
            .unwrap()
            .handoff
            .as_mut()
            .unwrap()
            .drained = true;
        assert_reports(&state, &["INV-HANDOFF-2"]);
        assert!(check_hard(&state)[0].detail.contains("unminted seq 0"));
    }

    #[test]
    fn inv_mig_1_forces_ownership_drift_under_a_live_migration() {
        let mut state = clean_state();
        // Slot 20 migrates from node 1; hand it to node 2 behind the record.
        state.slot_assignment.insert(20, 2);
        assert_reports(&state, &["INV-MIG-1"]);
        assert!(check_hard(&state)[0].detail.contains("owned by 2"));

        // Dropping the assignment entirely is *not* drift.
        let mut state = clean_state();
        state.slot_assignment.remove(&20);
        assert_reports(&state, &[]);
    }

    #[test]
    fn inv_slot_1_forces_out_of_range_slot_keys() {
        let mut state = clean_state();
        state.slot_assignment.insert(CLUSTER_SLOTS, 1);
        state.slot_assignment.insert(u16::MAX, 1);
        assert_reports(&state, &["INV-SLOT-1"]);
        assert_eq!(
            check_hard(&state).len(),
            2,
            "both out-of-range keys are named"
        );

        // The migration side of the same range check, on both the map key and
        // the record's own slot field.
        let mut state = clean_state();
        state.migrations.insert(
            CLUSTER_SLOTS,
            SlotMigration {
                slot: CLUSTER_SLOTS,
                source_node: 1,
                target_node: 2,
                handoff: None,
            },
        );
        assert_reports(&state, &["INV-SLOT-1"]);
        assert_eq!(
            check_hard(&state).len(),
            2,
            "the key and the recorded slot are both out of range"
        );

        // The last legal slot stays clean.
        let mut state = clean_state();
        state.slot_assignment.insert(CLUSTER_SLOTS - 1, 1);
        assert_reports(&state, &[]);
    }

    /// Several broken entries report together rather than the first one
    /// masking the rest.
    #[test]
    fn violations_accumulate_across_entries() {
        let mut state = clean_state();
        state.slot_assignment.insert(30, 404);
        state.nodes.get_mut(&2).unwrap().primary_id = Some(1);
        state.handoff_seq = 0;
        assert_reports(&state, &["INV-REF-1", "INV-REF-4", "INV-HANDOFF-1"]);
    }

    /// The hook every mutation seam calls. A clean state passes it silently —
    /// without this the assertion could be inverted and nothing would notice.
    #[test]
    fn the_hook_lets_a_clean_state_through() {
        debug_assert_clean(&clean_state(), "a-seam");
        debug_assert_clean(&ClusterStateInner::default(), "a-seam");
    }

    /// ...and a dirty one panics with the seam name and the violation list, so
    /// a failure says where the state went wrong and which claim it broke.
    #[test]
    #[should_panic(expected = "cluster state invariants violated after a-seam")]
    fn the_hook_panics_on_a_dirty_state() {
        let mut state = clean_state();
        state.slot_assignment.insert(30, 404);
        debug_assert_clean(&state, "a-seam");
    }

    /// The panic body carries the offending invariant, not just the seam.
    #[test]
    #[should_panic(expected = "INV-REF-1")]
    fn the_hook_names_the_broken_invariant() {
        let mut state = clean_state();
        state.slot_assignment.insert(30, 404);
        debug_assert_clean(&state, "a-seam");
    }
}
