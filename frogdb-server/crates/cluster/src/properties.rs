//! Property-based permutation harness over the cluster state machine.
//!
//! The invariant catalog ([`crate::invariants`]) says what a well-formed state
//! is; this module generates the command sequences nobody hand-wrote and checks
//! the catalog against every state they reach. Where a failure-mode row is a
//! point witness ("this scenario behaves so"), a property here is universally
//! quantified over the generated space, which is the gap the 2026-08-08 cluster
//! audit identified as structural (PRD `.scratch/cluster-correctness/PRD.md`
//! §1 B1, §3 W2).
//!
//! # The generator
//!
//! [`arb_command_sequence`] is *stateful*: it folds the sequence through the
//! real transition function as it builds it, so each command is chosen against
//! the state its predecessors actually produced — live node ids, assigned
//! slots, open migrations and their handoff `seq`s are read from that state
//! rather than from a shadow model that could drift.
//!
//! It applies through [`ClusterState::apply_to`] rather than
//! [`ClusterState::apply_local`] deliberately: the latter runs the assertion
//! hook, and a panic raised inside a proptest strategy escapes the runner's
//! `catch_unwind`, aborting the run without shrinking. Generation stays
//! panic-free; the property does the asserting.
//!
//! Roughly [`IN_CONTEXT_BIAS`] of commands are aimed at that context and the
//! rest are deliberate garbage — unknown node ids, mismatched migration
//! parameters, stale handoff `seq`s. The garbage is not filler: a *rejected*
//! command must leave the state as clean as an accepted one, and the rejection
//! path is exactly where a validate-then-mutate bug hides.
//!
//! # Properties
//!
//! - **P1** ([`p1_every_apply_leaves_the_catalog_clean`]) — every state a
//!   sequence reaches satisfies every HARD entry of the catalog, whether the
//!   command was accepted or refused.
//!
//! P2 (snapshot/restore losslessness), P3 (replay determinism) and P4 (handoff
//! event conservation) land on this generator in issue 04.
//!
//! # Case budget
//!
//! [`DEFAULT_CASES`] in the normal suite, raised by the `PROPTEST_CASES`
//! environment variable — `just cluster-proptest` is the boosted pass, and the
//! nightly workflow calls that same recipe.

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

use crate::invariants;
use crate::state::{ClusterState, ClusterStateInner};
use crate::types::{
    CLUSTER_SLOTS, ClusterCommand, HANDOFF_BARRIER_MS, HANDOFF_LEASE_MS, NodeId, NodeInfo,
    NodeRole, SlotHandoff, SlotRange,
};

/// Cases run when nothing overrides the budget. Sized for the normal
/// `just test frogdb-cluster` loop (a case is [`SEQUENCE_LEN`] applies over a
/// small state, so this is well under a second); the nightly pass raises it
/// through `PROPTEST_CASES`.
const DEFAULT_CASES: u32 = 96;

/// Upper bound on commands per generated sequence; lengths are drawn uniformly
/// from `1..=SEQUENCE_LEN`, so the average sequence is half this.
///
/// Long enough to reach a prepared-and-drained handoff behind a couple of
/// failovers — the migration lifecycle is four commands deep *after* a cluster
/// with assigned slots exists, and at 24 the average sequence ran out before
/// `CompleteSlotMigration` was ever admissible. Shrinking pulls a counterexample
/// back down (the issue-16 repro reduced to four commands), so the length costs
/// coverage, not readability.
const SEQUENCE_LEN: usize = 48;

/// The environment variable that raises [`DEFAULT_CASES`].
const CASES_ENV: &str = "PROPTEST_CASES";

/// Fraction of generated commands aimed at the state the sequence has actually
/// reached. The remainder is garbage, on purpose — see the module docs.
const IN_CONTEXT_BIAS: f64 = 0.8;

/// The case budget for a `raw` reading of [`CASES_ENV`].
///
/// Unset, unparseable and zero all mean "use the default": a typo in a CI
/// invocation must not silently reduce the property to nothing.
fn cases_from(raw: Option<&str>) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|cases| *cases > 0)
        .unwrap_or(DEFAULT_CASES)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: cases_from(std::env::var(CASES_ENV).ok().as_deref()),
        ..ProptestConfig::default()
    }
}

// ---- the generator ---------------------------------------------------------

/// Node ids the generator introduces. Small and shared so commands collide with
/// each other instead of each one touching a private corner of the state.
const NODE_POOL: [NodeId; 5] = [1, 2, 3, 4, 5];

/// Ids [`NODE_POOL`] never contains, so a command naming one is guaranteed to
/// be talking about a non-member. `ResetCluster`'s new-id path adds
/// `NODE_POOL[i] + RESET_ID_OFFSET`, which stays clear of these too.
const STRANGERS: [NodeId; 3] = [404, 405, 406];

/// Added to a pool id to mint the successor id of a HARD `ResetCluster`.
const RESET_ID_OFFSET: NodeId = 100;

/// Slots the generator works with: a handful of low slots so migrations and
/// assignments contend, plus the last legal slot as a boundary probe.
const SLOT_POOL: [u16; 6] = [0, 1, 2, 3, 7, CLUSTER_SLOTS - 1];

/// How far the virtual clock advances between steps, in milliseconds.
///
/// Small on purpose. A prepare and the complete that finishes it are several
/// commands apart, so a pool that could jump [`HANDOFF_LEASE_MS`] on any step
/// makes the clock alone refuse essentially every completion — which is what
/// `the_generator_reaches_prepared_drained_and_completed_handoffs` caught. The
/// expiry paths do not need a big clock: a `0` in [`BARRIER_POOL`] /
/// [`LEASE_POOL`] arms a handoff that is *already* expired, which reaches the
/// same two rejection arms from the other side.
const CLOCK_STEPS: [u64; 6] = [0, 0, 1, 25, 25, HANDOFF_BARRIER_MS + 1];

/// Barrier windows offered to `PrepareSlotHandoff`. `0` is an already-elapsed
/// window, which a `CompleteSlotMigration` must refuse — held to a quarter of
/// the draws because the barrier and the lease each have to be live for a
/// completion to be admissible at all.
const BARRIER_POOL: [u64; 4] = [
    0,
    HANDOFF_BARRIER_MS,
    HANDOFF_BARRIER_MS * 4,
    HANDOFF_BARRIER_MS * 4,
];

/// Lease windows offered to `PrepareSlotHandoff`, on the same principle.
const LEASE_POOL: [u64; 4] = [
    0,
    HANDOFF_LEASE_MS,
    HANDOFF_LEASE_MS * 4,
    HANDOFF_LEASE_MS * 4,
];

/// Where the virtual clock starts. Nonzero so a `prepared_at_ms` is never
/// confused with "unset".
const BASE_CLOCK_MS: u64 = 1_000_000;

/// The command families the generator emits, with their relative weights.
///
/// Weights buy interaction, not uniformity: the commands that *build* a state
/// worth probing (join, assign, migrate, hand off) are drawn more often than
/// the ones that flatten it (`ResetCluster` wipes everything, so it is the
/// rarest).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    AddNode,
    RemoveNode,
    AssignSlots,
    RemoveSlots,
    SetRole,
    IncrementEpoch,
    SetConfigEpoch,
    Failover,
    MarkNodeFailed,
    MarkNodeRecovered,
    BeginSlotMigration,
    PrepareSlotHandoff,
    ConfirmSlotHandoffDrained,
    AbortSlotHandoff,
    CompleteSlotMigration,
    CancelSlotMigration,
    FinalizeUpgrade,
    ResetCluster,
}

/// Every [`ClusterCommand`] variant, weighted. The exhaustive `match` in
/// [`build`] and the coverage test both key off this table, so a nineteenth
/// command added to the enum without an entry here fails to build there and
/// fails the count assertion here.
const WEIGHTS: [(Variant, u32); 18] = [
    (Variant::AddNode, 12),
    (Variant::RemoveNode, 4),
    (Variant::AssignSlots, 10),
    (Variant::RemoveSlots, 6),
    (Variant::SetRole, 8),
    (Variant::IncrementEpoch, 3),
    (Variant::SetConfigEpoch, 2),
    (Variant::Failover, 6),
    (Variant::MarkNodeFailed, 3),
    (Variant::MarkNodeRecovered, 3),
    (Variant::BeginSlotMigration, 14),
    (Variant::PrepareSlotHandoff, 12),
    (Variant::ConfirmSlotHandoffDrained, 12),
    // `AbortSlotHandoff` and `CancelSlotMigration` are the two arms that
    // *always* succeed, handoff or not, so every draw of one tears down a
    // migration the four stages above spent several draws building. Held low
    // deliberately — at parity with the rest of the lifecycle they starved
    // `CompleteSlotMigration` of a drained handoff entirely.
    (Variant::AbortSlotHandoff, 2),
    (Variant::CompleteSlotMigration, 12),
    (Variant::CancelSlotMigration, 2),
    (Variant::FinalizeUpgrade, 2),
    (Variant::ResetCluster, 1),
];

/// Total weight, the exclusive upper bound of [`Step::variant`].
const TOTAL_WEIGHT: u32 = {
    let mut total = 0;
    let mut i = 0;
    while i < WEIGHTS.len() {
        total += WEIGHTS[i].1;
        i += 1;
    }
    total
};

/// The variant a weighted draw in `0..TOTAL_WEIGHT` selects.
fn variant_for(pick: u32) -> Variant {
    let mut acc = 0;
    for (variant, weight) in WEIGHTS {
        acc += weight;
        if pick < acc {
            return variant;
        }
    }
    WEIGHTS[WEIGHTS.len() - 1].0
}

/// One step's worth of raw entropy, before it is read against the state.
///
/// The strategy generates these rather than [`ClusterCommand`]s directly, which
/// is what makes the generator stateful *and* shrinkable: shrinking drops or
/// simplifies steps, and the fold in [`arb_command_sequence`] re-derives a
/// coherent sequence from whatever survives.
#[derive(Debug, Clone, Copy)]
struct Step {
    /// Weighted draw into [`WEIGHTS`].
    variant: u32,
    /// Aim this command at the current state (see [`IN_CONTEXT_BIAS`]).
    in_context: bool,
    /// Selector for the primary node argument.
    a: u8,
    /// Selector for the secondary node argument.
    b: u8,
    /// Selector into [`SLOT_POOL`] / the open migrations.
    slot: u8,
    /// Extra span for a slot range, and selector into the timing pools.
    span: u8,
    /// Selector into [`CLOCK_STEPS`].
    clock: u8,
    /// Free boolean: `force`, role choice, HARD-vs-SOFT reset, and so on.
    flag: bool,
}

fn arb_step() -> impl Strategy<Value = Step> {
    (
        0..TOTAL_WEIGHT,
        proptest::bool::weighted(IN_CONTEXT_BIAS),
        any::<u8>(),
        any::<u8>(),
        any::<u8>(),
        any::<u8>(),
        any::<u8>(),
        any::<bool>(),
    )
        .prop_map(
            |(variant, in_context, a, b, slot, span, clock, flag)| Step {
                variant,
                in_context,
                a,
                b,
                slot,
                span,
                clock,
                flag,
            },
        )
}

/// A sequence of up to `len` commands, each chosen against the state its
/// predecessors produced. See the module docs.
pub(crate) fn arb_command_sequence(len: usize) -> impl Strategy<Value = Vec<ClusterCommand>> {
    proptest::collection::vec(arb_step(), 1..=len).prop_map(|steps| {
        let mut state = ClusterStateInner::default();
        let mut clock_ms = BASE_CLOCK_MS;
        let mut commands = Vec::with_capacity(steps.len());
        for step in steps {
            clock_ms += CLOCK_STEPS[step.clock as usize % CLOCK_STEPS.len()];
            let command = build(&state, clock_ms, step);
            if known_defect(&state, &command).is_some() {
                continue;
            }
            let _ = ClusterState::apply_to(&mut state, command.clone());
            commands.push(command);
        }
        commands
    })
}

/// `items[i mod len]`, or `None` when there is nothing to choose from.
fn nth<T: Copy>(items: &[T], i: u8) -> Option<T> {
    if items.is_empty() {
        return None;
    }
    Some(items[i as usize % items.len()])
}

/// An id no cluster the generator builds ever contains.
fn stranger(i: u8) -> NodeId {
    STRANGERS[i as usize % STRANGERS.len()]
}

/// A node argument: a member when `in_context` (and there is one), a stranger
/// otherwise.
fn node_ref(state: &ClusterStateInner, i: u8, in_context: bool) -> NodeId {
    let members: Vec<NodeId> = state.nodes.keys().copied().collect();
    match in_context.then(|| nth(&members, i)).flatten() {
        Some(id) => id,
        None => stranger(i),
    }
}

/// The members holding `role`, in id order.
fn members_with_role(state: &ClusterStateInner, role: NodeRole) -> Vec<NodeId> {
    state
        .nodes
        .values()
        .filter(|node| node.role == role)
        .map(|node| node.id)
        .collect()
}

/// A slot argument: an assigned slot when `in_context` (and there is one), a
/// pool slot otherwise.
fn slot_ref(state: &ClusterStateInner, i: u8, in_context: bool) -> u16 {
    let assigned: Vec<u16> = state.slot_assignment.keys().copied().collect();
    match in_context.then(|| nth(&assigned, i)).flatten() {
        Some(slot) => slot,
        None => SLOT_POOL[i as usize % SLOT_POOL.len()],
    }
}

/// The `i`th open migration, as `(slot, source, target, handoff seq)`.
fn migration_ref(state: &ClusterStateInner, i: u8) -> Option<(u16, NodeId, NodeId, Option<u64>)> {
    let slots: Vec<u16> = state.migrations.keys().copied().collect();
    migration_ref_for(state, nth(&slots, i)?)
}

fn migration_ref_for(
    state: &ClusterStateInner,
    slot: u16,
) -> Option<(u16, NodeId, NodeId, Option<u64>)> {
    let migration = state.migrations.get(&slot)?;
    Some((
        slot,
        migration.source_node,
        migration.target_node,
        migration.handoff.as_ref().map(|handoff| handoff.seq),
    ))
}

/// The `i`th open migration whose handoff satisfies `wanted`, in the same shape
/// [`migration_ref`] returns.
fn migration_ref_where(
    state: &ClusterStateInner,
    i: u8,
    wanted: impl Fn(&SlotHandoff) -> bool,
) -> Option<(u16, NodeId, NodeId, Option<u64>)> {
    let slots: Vec<u16> = state
        .migrations
        .iter()
        .filter(|(_, migration)| migration.handoff.as_ref().is_some_and(&wanted))
        .map(|(slot, _)| *slot)
        .collect();
    migration_ref_for(state, nth(&slots, i)?)
}

/// The `i`th migration that has a handoff at all — the only shape
/// `ConfirmSlotHandoffDrained` and `AbortSlotHandoff` can acknowledge live.
fn prepared_migration_ref(
    state: &ClusterStateInner,
    i: u8,
) -> Option<(u16, NodeId, NodeId, Option<u64>)> {
    migration_ref_where(state, i, |_| true)
}

/// The `i`th migration whose handoff has been confirmed drained — the only
/// shape `CompleteSlotMigration` admits.
fn drained_migration_ref(
    state: &ClusterStateInner,
    i: u8,
) -> Option<(u16, NodeId, NodeId, Option<u64>)> {
    migration_ref_where(state, i, |handoff| handoff.drained)
}

fn addr_of(id: NodeId, offset: u16) -> std::net::SocketAddr {
    format!("127.0.0.1:{}", offset + id as u16)
        .parse()
        .expect("a loopback address for every generated node id")
}

fn node_info(id: NodeId, primary_id: Option<NodeId>) -> NodeInfo {
    let (client, bus) = (addr_of(id, 6379), addr_of(id, 16379));
    match primary_id {
        Some(parent) => NodeInfo::new_replica(id, client, bus, parent),
        None => NodeInfo::new_primary(id, client, bus),
    }
}

/// Turn one step's entropy into a command aimed at `state`.
///
/// Every arm reads the state for its in-context branch and reaches for a
/// stranger id, an unrelated slot, or a stale `seq` for its out-of-context one.
fn build(state: &ClusterStateInner, clock_ms: u64, step: Step) -> ClusterCommand {
    let Step {
        in_context,
        a,
        b,
        slot: slot_pick,
        span,
        flag,
        ..
    } = step;

    match variant_for(step.variant) {
        Variant::AddNode => {
            let id = NODE_POOL[a as usize % NODE_POOL.len()];
            let primaries = members_with_role(state, NodeRole::Primary);
            let primary_id = if in_context {
                // A replica of a real primary — but only when `flag` says so,
                // so most joins are the plain primary registration bootstrap
                // performs.
                flag.then(|| nth(&primaries, b)).flatten()
            } else {
                Some(stranger(b))
            };
            ClusterCommand::AddNode {
                node: node_info(id, primary_id),
            }
        }

        Variant::RemoveNode => ClusterCommand::RemoveNode {
            node_id: node_ref(state, a, in_context),
        },

        Variant::AssignSlots => {
            // In context: a run of slots nobody owns and nobody is migrating,
            // so the assignment can succeed. Skipping migrating slots is not
            // squeamishness about the issue-16 shape — an in-context draw that
            // the muzzle then drops costs the sequence a step, which is how the
            // generator starves itself of completed migrations. Out of context:
            // any pool slot, which will often already belong to someone else.
            let free = |slot: &u16| {
                !state.slot_assignment.contains_key(slot) && !state.migrations.contains_key(slot)
            };
            let start = if in_context {
                SLOT_POOL
                    .iter()
                    .copied()
                    .find(free)
                    .unwrap_or(SLOT_POOL[a as usize % SLOT_POOL.len()])
            } else {
                SLOT_POOL[slot_pick as usize % SLOT_POOL.len()]
            };
            let limit = start
                .saturating_add(u16::from(span % 4))
                .min(CLUSTER_SLOTS - 1);
            let end = match in_context {
                true => (start..=limit).take_while(free).last().unwrap_or(start),
                false => limit,
            };
            ClusterCommand::AssignSlots {
                node_id: node_ref(state, a, in_context),
                slots: vec![SlotRange::new(start, end)],
            }
        }

        Variant::RemoveSlots => {
            let slot = slot_ref(state, slot_pick, in_context);
            let node_id = match in_context {
                // The recorded owner, which is the only id this command accepts.
                true => state
                    .slot_assignment
                    .get(&slot)
                    .copied()
                    .unwrap_or_else(|| node_ref(state, a, true)),
                false => node_ref(state, a, false),
            };
            ClusterCommand::RemoveSlots {
                node_id,
                slots: vec![SlotRange::new(slot, slot)],
            }
        }

        Variant::SetRole => {
            let role = if flag {
                NodeRole::Primary
            } else {
                NodeRole::Replica
            };
            let primaries = members_with_role(state, NodeRole::Primary);
            let primary_id = match role {
                NodeRole::Primary => None,
                NodeRole::Replica if in_context => nth(&primaries, b),
                // Out of context: a parent that does not exist, or none at all
                // — both of which `SetRole` is supposed to refuse.
                NodeRole::Replica => (b % 2 == 0).then(|| stranger(b)),
            };
            ClusterCommand::SetRole {
                node_id: node_ref(state, a, in_context),
                role,
                primary_id,
            }
        }

        Variant::IncrementEpoch => ClusterCommand::IncrementEpoch,

        // Epochs stay small: `SetConfigEpoch` is the one command that writes an
        // arbitrary epoch, and a `u64::MAX` here would make the next
        // `IncrementEpoch` overflow — an arithmetic bug, not an invariant
        // violation, and not what this property is quantifying over.
        Variant::SetConfigEpoch => ClusterCommand::SetConfigEpoch {
            node_id: node_ref(state, a, in_context),
            epoch: u64::from(b % 8),
        },

        Variant::Failover => {
            let old_primary_id = node_ref(state, a, in_context);
            // Prefer a replica of the node being failed over: that is the shape
            // a real promotion has.
            let children: Vec<NodeId> = state
                .nodes
                .values()
                .filter(|node| node.primary_id == Some(old_primary_id))
                .map(|node| node.id)
                .collect();
            let new_primary_id = match in_context.then(|| nth(&children, b)).flatten() {
                Some(child) => child,
                None => node_ref(state, b, in_context),
            };
            ClusterCommand::Failover {
                old_primary_id,
                new_primary_id,
                force: flag,
            }
        }

        Variant::MarkNodeFailed => ClusterCommand::MarkNodeFailed {
            node_id: node_ref(state, a, in_context),
        },

        Variant::MarkNodeRecovered => ClusterCommand::MarkNodeRecovered {
            node_id: node_ref(state, a, in_context),
        },

        Variant::BeginSlotMigration => {
            // In context, prefer an assigned slot that is not *already*
            // migrating: `MigrationInProgress` is the one rejection that also
            // starves every later stage of the handoff lifecycle of a record to
            // work on.
            let idle: Vec<u16> = state
                .slot_assignment
                .keys()
                .copied()
                .filter(|slot| !state.migrations.contains_key(slot))
                .collect();
            let slot = match in_context.then(|| nth(&idle, slot_pick)).flatten() {
                Some(slot) => slot,
                None => slot_ref(state, slot_pick, in_context),
            };
            let source_node = match in_context {
                // The owner, which is the only source the arm accepts for an
                // assigned slot.
                true => state
                    .slot_assignment
                    .get(&slot)
                    .copied()
                    .unwrap_or_else(|| node_ref(state, a, true)),
                false => node_ref(state, a, false),
            };
            ClusterCommand::BeginSlotMigration {
                slot,
                source_node,
                target_node: node_ref(state, b, in_context),
            }
        }

        Variant::PrepareSlotHandoff => {
            let (slot, source_node, target_node) = match in_context
                .then(|| migration_ref(state, slot_pick))
                .flatten()
            {
                Some((slot, source, target, _)) => (slot, source, target),
                None => (
                    slot_ref(state, slot_pick, false),
                    node_ref(state, a, in_context),
                    node_ref(state, b, in_context),
                ),
            };
            ClusterCommand::PrepareSlotHandoff {
                slot,
                source_node,
                target_node,
                barrier_ms: BARRIER_POOL[span as usize % BARRIER_POOL.len()],
                lease_ms: LEASE_POOL[a as usize % LEASE_POOL.len()],
                proposed_at_ms: clock_ms,
            }
        }

        Variant::ConfirmSlotHandoffDrained => {
            let (slot, seq) = handoff_ref(state, step);
            ClusterCommand::ConfirmSlotHandoffDrained { slot, seq }
        }

        Variant::AbortSlotHandoff => {
            let (slot, seq) = handoff_ref(state, step);
            ClusterCommand::AbortSlotHandoff { slot, seq }
        }

        Variant::CompleteSlotMigration => {
            // In context, prefer a migration whose handoff is already drained —
            // the only shape the arm admits. Falling back to any open migration
            // (and, out of context, to a bare slot) keeps the rejection arms
            // reachable.
            let drained = drained_migration_ref(state, slot_pick);
            let (slot, source_node, target_node) = match in_context
                .then(|| drained.or_else(|| migration_ref(state, slot_pick)))
                .flatten()
            {
                Some((slot, source, target, _)) => (slot, source, target),
                None => (
                    slot_ref(state, slot_pick, false),
                    node_ref(state, a, in_context),
                    node_ref(state, b, in_context),
                ),
            };
            ClusterCommand::CompleteSlotMigration {
                slot,
                source_node,
                target_node,
                proposed_at_ms: clock_ms,
            }
        }

        Variant::CancelSlotMigration => ClusterCommand::CancelSlotMigration {
            slot: match in_context
                .then(|| migration_ref(state, slot_pick))
                .flatten()
            {
                Some((slot, ..)) => slot,
                None => slot_ref(state, slot_pick, false),
            },
        },

        // Every generated node reports this crate's own version, so the low
        // target is accepted and the unparseable one is refused.
        Variant::FinalizeUpgrade => ClusterCommand::FinalizeUpgrade {
            version: if in_context { "0.0.1" } else { "not.a.version" }.to_string(),
        },

        Variant::ResetCluster => {
            let node_id = node_ref(state, a, in_context);
            ClusterCommand::ResetCluster {
                node_id,
                new_node_id: flag.then_some(node_id + RESET_ID_OFFSET),
            }
        }
    }
}

/// A `(slot, seq)` pair for the two handoff acknowledgements: the live one when
/// in context, and a neighbouring `seq` — the classic stale ack — when not.
fn handoff_ref(state: &ClusterStateInner, step: Step) -> (u16, u64) {
    // In context, look past the migrations that have no handoff at all —
    // acknowledging one of those is the stale-ack case, not the live one.
    let live = step
        .in_context
        .then(|| prepared_migration_ref(state, step.slot))
        .flatten();
    match live.or_else(|| migration_ref(state, step.slot)) {
        Some((slot, _, _, Some(seq))) if step.in_context => (slot, seq),
        Some((slot, _, _, seq)) => (slot, seq.unwrap_or(0).wrapping_add(1)),
        None => (slot_ref(state, step.slot, false), u64::from(step.b % 4)),
    }
}

// ---- known defects ---------------------------------------------------------

/// Command shapes that drive today's state machine into a HARD violation, each
/// naming the filed issue that fixes it.
///
/// The generator drops these rather than emitting them, so P1 quantifies over
/// everything *except* the defects already under a ticket. Each entry is a
/// muzzle on the property and has to be removed the moment its issue lands —
/// which is what the `pinned_*` tests below force: they reproduce the shape
/// through the asserting path and expect the panic, so a fix turns them red and
/// points here.
///
/// Nothing else belongs in this function. A violation P1 finds that is *not*
/// one of these is a new defect and gets fixed or filed, not muzzled.
fn known_defect(state: &ClusterStateInner, command: &ClusterCommand) -> Option<&'static str> {
    match command {
        // Issue 14 — `AddNode` writes a brand-new node's `primary_id` verbatim,
        // so a replica of a non-member registers a dangling parent pointer
        // (INV-REF-3). Re-registration is unaffected: that path keeps the
        // recorded parent.
        ClusterCommand::AddNode { node } => {
            let dangling = node
                .primary_id
                .is_some_and(|parent| !state.nodes.contains_key(&parent));
            (dangling && !state.nodes.contains_key(&node.id))
                .then_some("issue 14: AddNode admits a dangling parent pointer")
        }

        // Issue 15 — a graceful `Failover` transfers the old primary's slots to
        // the successor but, unlike the force path, does not prune the
        // migrations sourced at it, so the slot ends up owned by someone other
        // than its migration's source (INV-MIG-1).
        ClusterCommand::Failover {
            old_primary_id,
            new_primary_id,
            force: false,
        } => state
            .migrations
            .iter()
            .any(|(slot, migration)| {
                migration.source_node == *old_primary_id
                    && state.slot_assignment.get(slot) == Some(old_primary_id)
                    && old_primary_id != new_primary_id
            })
            .then_some("issue 15: graceful failover strands a migration at the demoted primary"),

        // Issue 16 — `AssignSlots` never consults `migrations`, and
        // `BeginSlotMigration` accepts an unassigned slot (the follower-seed
        // allowance). The two compose: a migrating-but-unassigned slot can be
        // handed to a node that is not the migration's source (INV-MIG-1).
        ClusterCommand::AssignSlots { node_id, slots } => slots
            .iter()
            .flat_map(SlotRange::iter)
            .any(|slot| {
                state
                    .migrations
                    .get(&slot)
                    .is_some_and(|migration| migration.source_node != *node_id)
                    && state.slot_assignment.get(&slot).is_none()
            })
            .then_some("issue 16: AssignSlots hands a migrating slot to a third node"),

        _ => None,
    }
}

// ---- the properties --------------------------------------------------------

proptest! {
    #![proptest_config(config())]

    /// **P1** — the invariant catalog is clean after every transition of every
    /// generated sequence.
    ///
    /// The apply path already asserts this through
    /// [`invariants::debug_assert_clean`] in test and debug builds; the
    /// property asserts it again, explicitly, so it holds under any `cfg` and
    /// so a counterexample carries the sequence that produced it.
    #[test]
    fn p1_every_apply_leaves_the_catalog_clean(
        commands in arb_command_sequence(SEQUENCE_LEN),
    ) {
        let state = ClusterState::new();
        for (step, command) in commands.iter().enumerate() {
            let outcome = state.apply_local(command.clone());
            let violations = invariants::check_hard(&state.read_inner());
            prop_assert!(
                violations.is_empty(),
                "step {step} left the state dirty:\n{}\ncommand: {command:?}\noutcome: \
                 {outcome:?}\nfull sequence: {commands:#?}",
                invariants::render(&violations),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Draw `count` sequences from the generator with a fixed runner, so the
    /// meta-tests below measure the generator rather than the weather.
    fn sample(count: usize) -> Vec<Vec<ClusterCommand>> {
        let mut runner = TestRunner::deterministic();
        let strategy = arb_command_sequence(SEQUENCE_LEN);
        (0..count)
            .map(|_| {
                strategy
                    .new_tree(&mut runner)
                    .expect("the generator is total")
                    .current()
            })
            .collect()
    }

    /// The command's variant name, for coverage accounting.
    fn name_of(command: &ClusterCommand) -> &'static str {
        match command {
            ClusterCommand::AddNode { .. } => "AddNode",
            ClusterCommand::RemoveNode { .. } => "RemoveNode",
            ClusterCommand::AssignSlots { .. } => "AssignSlots",
            ClusterCommand::RemoveSlots { .. } => "RemoveSlots",
            ClusterCommand::SetRole { .. } => "SetRole",
            ClusterCommand::IncrementEpoch => "IncrementEpoch",
            ClusterCommand::SetConfigEpoch { .. } => "SetConfigEpoch",
            ClusterCommand::Failover { .. } => "Failover",
            ClusterCommand::MarkNodeFailed { .. } => "MarkNodeFailed",
            ClusterCommand::MarkNodeRecovered { .. } => "MarkNodeRecovered",
            ClusterCommand::BeginSlotMigration { .. } => "BeginSlotMigration",
            ClusterCommand::PrepareSlotHandoff { .. } => "PrepareSlotHandoff",
            ClusterCommand::ConfirmSlotHandoffDrained { .. } => "ConfirmSlotHandoffDrained",
            ClusterCommand::AbortSlotHandoff { .. } => "AbortSlotHandoff",
            ClusterCommand::CompleteSlotMigration { .. } => "CompleteSlotMigration",
            ClusterCommand::CancelSlotMigration { .. } => "CancelSlotMigration",
            ClusterCommand::FinalizeUpgrade { .. } => "FinalizeUpgrade",
            ClusterCommand::ResetCluster { .. } => "ResetCluster",
        }
    }

    // ---- case budget -------------------------------------------------------

    #[test]
    fn the_case_budget_defaults_and_is_raised_by_the_environment() {
        assert_eq!(cases_from(None), DEFAULT_CASES);
        assert_eq!(cases_from(Some("2500")), 2500);
        assert_eq!(cases_from(Some(" 2500 ")), 2500);
        // A typo must not silently disable the property.
        assert_eq!(cases_from(Some("")), DEFAULT_CASES);
        assert_eq!(cases_from(Some("lots")), DEFAULT_CASES);
        assert_eq!(cases_from(Some("0")), DEFAULT_CASES);
        assert_eq!(cases_from(Some("-1")), DEFAULT_CASES);
    }

    // ---- generator shape ---------------------------------------------------

    #[test]
    fn the_weight_table_covers_every_command_exactly_once() {
        let variants: BTreeSet<String> = WEIGHTS
            .iter()
            .map(|(variant, _)| format!("{variant:?}"))
            .collect();
        assert_eq!(variants.len(), WEIGHTS.len(), "duplicate weight entries");
        assert_eq!(
            WEIGHTS.len(),
            18,
            "every ClusterCommand variant is weighted"
        );
        assert!(
            WEIGHTS.iter().all(|(_, weight)| *weight > 0),
            "a zero weight is a variant that is never generated"
        );
        assert_eq!(
            TOTAL_WEIGHT,
            WEIGHTS.iter().map(|(_, weight)| weight).sum::<u32>()
        );
    }

    /// Every weighted draw lands on a variant, and every variant is reachable
    /// from some draw — a table whose boundaries were off by one would strand
    /// the first or last entry.
    #[test]
    fn every_weighted_draw_selects_a_variant() {
        let reachable: BTreeSet<String> = (0..TOTAL_WEIGHT)
            .map(|pick| format!("{:?}", variant_for(pick)))
            .collect();
        assert_eq!(reachable.len(), WEIGHTS.len());
        assert_eq!(variant_for(0), WEIGHTS[0].0);
        assert_eq!(variant_for(TOTAL_WEIGHT - 1), WEIGHTS[WEIGHTS.len() - 1].0);
        // Out of range saturates rather than panicking.
        assert_eq!(variant_for(TOTAL_WEIGHT), WEIGHTS[WEIGHTS.len() - 1].0);
    }

    /// All eighteen commands actually come out of the generator. Without this,
    /// a mis-weighted or unreachable arm would quietly shrink P1's coverage to
    /// whatever still worked.
    #[test]
    fn the_generator_emits_every_command_variant() {
        let seen: BTreeSet<&'static str> = sample(200).iter().flatten().map(name_of).collect();
        assert_eq!(
            seen.len(),
            WEIGHTS.len(),
            "commands never generated: {:?}",
            seen
        );
    }

    /// The 80/20 bias is real in both directions: a generator that only
    /// produced garbage would exercise nothing but the rejection arms, and one
    /// that only produced valid commands would never test them.
    ///
    /// The band is well below [`IN_CONTEXT_BIAS`] because in-context is an
    /// *aim*, not a guarantee: the slot pool saturates, a migration is already
    /// open on the slot the draw wanted, a failover names a node with no
    /// replicas. Those refusals are states worth reaching, so the assertion is
    /// a drift alarm, not a target.
    #[test]
    fn the_generator_mixes_accepted_and_rejected_commands() {
        let (mut accepted, mut rejected) = (0usize, 0usize);
        for commands in sample(200) {
            let state = ClusterState::new();
            for command in commands {
                match state.apply_local(command) {
                    Ok(_) => accepted += 1,
                    Err(_) => rejected += 1,
                }
            }
        }
        let total = accepted + rejected;
        assert!(total > 1_000, "too few commands to judge: {total}");
        let accepted_share = accepted as f64 / total as f64;
        assert!(
            (0.25..=0.9).contains(&accepted_share),
            "accepted share {accepted_share:.2} ({accepted} of {total}) — the generator has \
             drifted into all-garbage or all-valid"
        );
    }

    /// The generated sequences reach the states worth probing at all: a
    /// prepared handoff, a drained one, and a completed migration. A generator
    /// that never got past `AddNode` would still satisfy P1 vacuously.
    #[test]
    fn the_generator_reaches_prepared_drained_and_completed_handoffs() {
        let (mut prepared, mut drained, mut completed) = (0usize, 0usize, 0usize);
        for commands in sample(200) {
            let state = ClusterState::new();
            for command in commands {
                let Ok((_, events)) = state.apply_command(command) else {
                    continue;
                };
                for event in events {
                    match event {
                        crate::types::ClusterEvent::SlotHandoffPrepared { .. } => prepared += 1,
                        crate::types::ClusterEvent::SlotMigrationCompleted { .. } => completed += 1,
                        _ => {}
                    }
                }
                if state
                    .read_inner()
                    .migrations
                    .values()
                    .any(|migration| migration.handoff.as_ref().is_some_and(|h| h.drained))
                {
                    drained += 1;
                }
            }
        }
        assert!(prepared > 0, "no handoff was ever prepared");
        assert!(drained > 0, "no handoff was ever drained");
        assert!(completed > 0, "no migration ever completed");
    }

    // ---- pins on the muzzled defects ---------------------------------------

    /// Issue 14, through the asserting path: the shape [`known_defect`] filters
    /// out really does violate the catalog today. When the issue lands this
    /// test stops panicking and fails — delete the muzzle with it.
    #[test]
    #[should_panic(expected = "INV-REF-3")]
    fn pinned_issue_14_add_node_admits_a_dangling_parent() {
        let state = ClusterState::new();
        let orphan = node_info(1, Some(STRANGERS[0]));
        assert!(
            known_defect(
                &state.read_inner(),
                &ClusterCommand::AddNode {
                    node: orphan.clone()
                }
            )
            .is_some()
        );
        let _ = state.apply_local(ClusterCommand::AddNode { node: orphan });
    }

    /// Issue 15, likewise: a graceful failover over a migrating slot strands
    /// the migration at the demoted primary.
    #[test]
    #[should_panic(expected = "INV-MIG-1")]
    fn pinned_issue_15_graceful_failover_strands_a_migration() {
        let state = ClusterState::new();
        for id in [1, 2, 3] {
            state
                .apply_local(ClusterCommand::AddNode {
                    node: node_info(id, None),
                })
                .expect("seeding a primary must succeed");
        }
        state
            .apply_local(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(5, 5)],
            })
            .expect("seeding a slot must succeed");
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 3,
            })
            .expect("opening a migration must succeed");

        let failover = ClusterCommand::Failover {
            old_primary_id: 1,
            new_primary_id: 2,
            force: false,
        };
        assert!(known_defect(&state.read_inner(), &failover).is_some());
        let _ = state.apply_local(failover);
    }

    /// Issue 16, likewise, and this is the shrunk P1 counterexample itself: a
    /// migration opened over an unassigned slot (legal — the follower-seed
    /// allowance) followed by an assignment of that slot to a third node.
    #[test]
    #[should_panic(expected = "INV-MIG-1")]
    fn pinned_issue_16_assign_slots_hands_a_migrating_slot_to_a_third_node() {
        let state = ClusterState::new();
        for id in [5, 1] {
            state
                .apply_local(ClusterCommand::AddNode {
                    node: node_info(id, None),
                })
                .expect("seeding a primary must succeed");
        }
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 0,
                source_node: 1,
                target_node: 1,
            })
            .expect("a migration over an unassigned slot is accepted");

        let assign = ClusterCommand::AssignSlots {
            node_id: 5,
            slots: vec![SlotRange::new(0, 0)],
        };
        assert!(known_defect(&state.read_inner(), &assign).is_some());
        let _ = state.apply_local(assign);
    }

    /// The muzzle is narrow: no pinned shape is filtered when its precondition
    /// is absent, so P1 still quantifies over the neighbouring states.
    #[test]
    fn the_muzzle_only_covers_the_pinned_shapes() {
        let state = ClusterState::new();
        state
            .apply_local(ClusterCommand::AddNode {
                node: node_info(1, None),
            })
            .unwrap();
        let inner = state.read_inner();

        // A replica of a *member* is not the dangling case.
        assert!(
            known_defect(
                &inner,
                &ClusterCommand::AddNode {
                    node: node_info(2, Some(1))
                }
            )
            .is_none()
        );
        // A force failover prunes, so it is not muzzled even with a migration.
        assert!(
            known_defect(
                &inner,
                &ClusterCommand::Failover {
                    old_primary_id: 1,
                    new_primary_id: 2,
                    force: true,
                }
            )
            .is_none()
        );
        // A graceful failover with no migration in flight is not muzzled.
        assert!(
            known_defect(
                &inner,
                &ClusterCommand::Failover {
                    old_primary_id: 1,
                    new_primary_id: 2,
                    force: false,
                }
            )
            .is_none()
        );
        // An assignment over a slot with no migration is not muzzled.
        assert!(
            known_defect(
                &inner,
                &ClusterCommand::AssignSlots {
                    node_id: 1,
                    slots: vec![SlotRange::new(0, 9)],
                }
            )
            .is_none()
        );
        drop(inner);

        // Nor is an assignment to the migration's *own* source: that is the
        // follower catching up on the seed it already holds.
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 0,
                source_node: 1,
                target_node: 1,
            })
            .unwrap();
        let inner = state.read_inner();
        assert!(
            known_defect(
                &inner,
                &ClusterCommand::AssignSlots {
                    node_id: 1,
                    slots: vec![SlotRange::new(0, 0)],
                }
            )
            .is_none()
        );
        assert!(known_defect(&inner, &ClusterCommand::IncrementEpoch).is_none());
    }
}
