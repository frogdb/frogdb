//! Quint-connect conformance harness: replays traces from
//! `specs/quint/cluster_migration_failover.qnt` against the real `ClusterState`
//! apply path (`ClusterState::apply_local`), so the model and the code are checked
//! against each other rather than only against themselves.
//!
//! ## Driver target
//!
//! `ClusterState::apply_local` is the driver target: it is the crate's public,
//! test-visible, single validated mutation path (`apply_command` is `pub(crate)` and
//! not reachable from this external `tests/` crate). No production API was widened
//! for this harness.
//!
//! ## Projection coverage (projection-blindness guard)
//!
//! The model's state (`cluster_migration_failover.qnt`) declares:
//! `nodes, slots, migrations, handoff_seq, epoch, node_epoch`, plus a set of ghost /
//! derived variables that exist only to make the model's own guards and postconditions
//! expressible and have **no counterpart in `ClusterState`**.
//!
//! Covered below (every field a modeled TR postcondition touches, per the task brief):
//!   - `nodes`: `role`, `parent`, `member`, `fail`               -> `NodeProj`
//!   - `slots`: owner                                             -> `ClusterProjection::slots`
//!   - `migrations`: `source`, `target`, `handoff.seq`, `handoff.drained`
//!                                                                 -> `MigrationProj`, `HandoffProj`
//!   - `handoff_seq`, `epoch`, `node_epoch`                       -> top-level fields, read live
//!     from `ClusterState::snapshot()` (not a driver-side shadow counter — a shadow counter
//!     mirroring the model's own increments would defeat this exact guard by construction,
//!     since a code mutation to the real counter would have no shadow to disagree with)
//!
//! Excluded, and why (ghost/derived state with no `ClusterState` counterpart):
//!   - `migrations.*.cancelling` — no residue/repatriation bookkeeping exists in code
//!     (see divergences below); there is nothing to project it onto.
//!   - `armed_barriers`, `failover_fence` — the model's explicit barrier-arm ghost
//!     state (`armFailoverFence`); the production barrier planner
//!     (`handoff_barrier.rs::plan_handoff_action`) lives in `frogdb-cluster-runtime`,
//!     not `frogdb-cluster`, and is not reachable from this crate's test target. Per
//!     the task brief: "if it lives outside the crate, project only replicated state
//!     and note the exclusion" — noted here.
//!   - `provisional_target` — a ghost "who provisionally owns this slot" tracker with
//!     no code counterpart (code has no provisional-ownership concept distinct from
//!     `slot_assignment`).
//!   - `feed_bytes`, `disconnected_feed` — model the byte-cap replication feed hold
//!     (`feedBuffer`); `ClusterCommand` has no matching variant at all.
//!   - `repatriating` — the model's repatriation-phase membership set; code has no
//!     `CompleteRepatriation` command and no residue bookkeeping.
//!   - `prev_epoch`, `spent`, `defects{demotion,fence,barrier,admission,seqReuse}`,
//!     `coverage{...}` — pure bookkeeping/coverage-instrumentation internal to the
//!     model, never referenced by `ClusterState`.
//!
//! ## Known divergences (code lags the model's *ruled* target semantics)
//!
//! The model's header states plainly that it encodes the RULED/AMENDED semantics for
//! issues 15, 17, 19, 20 and 26 — not today's code. Every divergence below traces to
//! one of those five filed, `ready-for-agent` (Pending) issues:
//!
//!   - issue 15 (`.scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md`):
//!     graceful (`force: false`) `Failover` never prunes migrations sourced at the old
//!     primary, and there is no repatriation phase (`CancelSlotMigration` /
//!     `AbortSlotHandoff` apply immediately, unconditionally, with no residue tracking).
//!   - issue 17 (`.scratch/cluster-correctness/issues/open/17-*.md`, byte-cap feed hold
//!     amendment): `PrepareSlotHandoff` *refuses* (`HandoffNotReady`) a re-prepare over
//!     a live (unexpired-lease) handoff, rather than superseding it; and there is no
//!     `feedBuffer`-equivalent `ClusterCommand` at all.
//!   - issue 19 (`.scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md`):
//!     `ClusterCommand::Failover` has no per-object fence fields (no `obsEpoch`/
//!     `obsRole`) and no fence check — a stale proposal is never refused.
//!   - issue 20 (`.scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md`):
//!     `RemoveNode` has no "still owns slots" guard at all (always succeeds), and the
//!     forced (`force: true`) `Failover` branch still *removes* the old primary from
//!     the node table rather than demoting it (the ruling's demote-don't-remove
//!     amendment is implemented on the graceful branch only).
//!   - issue 26 (`.scratch/cluster-correctness/issues/open/26-planned-failover-gets-a-drain-and-offset-parity-barrier.md`):
//!     no drain/offset-parity barrier exists on the graceful failover path at all
//!     (`Failover{force: false}` proposes and applies immediately).
//!
//! Every named-run test below whose trace exercises one of these paths is
//! `#[ignore = "pending issue NN — ..."]`-annotated citing the issue file, with the
//! `#[ignore]` text stating the *observed* first failure verbatim (a panic inside
//! `apply_command`, a returned-`Ok` refusal that should have been `Err`, or a plain
//! projection mismatch) rather than an inferred one — run
//! `just quint-conformance-quarantine` to reproduce any of them directly. Two
//! quarantine-shaped tests, `abort_repatriates_test` and
//! `feed_hold_overflow_disconnects_test`, in fact pass and are *not* `#[ignore]`d: the
//! model state they would diverge on is excluded from `ClusterProjection` (see above),
//! so their pass is weak (real shared-field coverage, not evidence the cited issue is
//! fixed) — each has a comment saying so at its definition. Every other test that
//! stays in the default suite genuinely does not exercise a divergent path (confirmed
//! empirically, not merely by inspection).
//!
//! ## Ghost-field finding (issue 33): removed-node role/parent normalization
//!
//! `remove_node_after_demotion_succeeds_test` and
//! `remove_node_force_evicts_live_owner_test` were expected to match the model
//! exactly (no filed issue touches plain `removeNode` postconditions), and would
//! otherwise diverge on it. The model's `nodes` var is a *total* map over the fixed
//! `NODES` set — `removeNode` flips `member: false` but leaves `role`/`parent` at
//! their last live value, so a removed node's record survives as a "ghost".
//! `ClusterState`'s real `RemoveNode` arm (`commands.rs:215-236`) does
//! `inner.nodes.remove(&node_id)` — true deletion, matching `specs/cluster.md`'s own
//! TR-CLUSTER-003 postcondition ("Node membership" loses `node_id`) — so there is no
//! live data anywhere to recover a removed node's last role/parent from. This reads as
//! a model-representation artifact (the model's *own* target spec confirms real
//! deletion; nothing suggests the ghost fields encode an intentional requirement)
//! rather than a code defect, but per the divergence protocol it is reported, not
//! silently resolved: filed as
//! `.scratch/cluster-correctness/issues/open/33-quint-model-removeNode-leaves-ghost-role-parent-fields.md`
//! (most likely fix: the model nulls `role`/`parent`/`node_epoch` in the same
//! transition that flips `member: false`, matching real deletion's information loss).
//!
//! Until that model fix lands, `ScriptedProj::from_spec`'s
//! `normalize_removed_node_ghost_fields` (below) applies the same blanking on the spec
//! side, as a total, case-independent normalization — not a per-test workaround — so
//! both tests stay live in the default suite with their real slot-pruning/reparenting
//! assertions intact. This normalization is scoped to `ScriptedDriver`/`ScriptedProj`
//! only: the simulation tests below use the plain `ClusterDriver`/`ClusterProjection`
//! pair and remain exposed to this finding on any sampled trace that reaches
//! `removeNode`.
//!
//! ## Design note: `removeNode`'s `force` flag is not representable
//!
//! The model's `removeNode(n, force)` has two distinct branches (guarded refusal vs.
//! forced eviction). `ClusterCommand::RemoveNode` has no `force` field at all — the
//! driver maps both `removeNode(n, true)` and `removeNode(n, false)` to the same
//! command. This means the driver cannot structurally represent the model's
//! `force: false` guarded refusal — that divergence is caught only because code's
//! *unconditional* success mismatches the model's *guarded* refusal in
//! `removeNodeRefusesLiveOwnerTest` (issue 20). It does NOT mean `force: true` and
//! `force: false` are otherwise interchangeable against the model: both
//! `removeNodeAfterDemotionSucceedsTest` (`force: false`) and
//! `removeNodeForceEvictsLiveOwnerTest` (`force: true`) hit the ghost-field finding
//! above independent of which model branch they replay.
//!
//! ## Named-run replay uses `#[quint_run(init = ...)]`, not `#[quint_test]`
//!
//! `#[quint_test]` drives the `quint test` CLI subcommand, which has no `--mbt` flag
//! (confirmed via `quint test --help`) and therefore never emits the `mbt::actionTaken`
//! / `mbt::nondetPicks` trace variables that quint-connect's step decoder unconditionally
//! requires (`src/driver/step.rs`) — every `#[quint_test]` replay against this spec fails
//! immediately with `Missing 'mbt::actionTaken' variable in the trace`, regardless of
//! which named run is targeted. quint-connect's own bundled example that uses
//! `#[quint_test]` (`examples/two_phase_commit`) only works around this by adding
//! manual action-tracking state to its spec (`Extensions { actionTaken }`) and a
//! `Driver::config()` override redirecting `nondet` at that manual path instead of the
//! builtin `mbt::` vars — scaffolding this harness cannot add without editing
//! `specs/quint/cluster_migration_failover.qnt`, which is out of this crate's `tests/`
//! surface.
//!
//! `quint run` (used by `#[quint_run]`) has `--mbt` support and, separately, an
//! `--init <action>` flag naming the action used as the trace's sole initializer step.
//! A Quint named `run` (e.g. `happyMigrationTest = init.then(...).then(...)`) *is* an
//! action, so `quint run --init happyMigrationTest --max-steps 0 --mbt` replays the
//! exact same deterministic action chain `quint test --match ^happyMigrationTest$`
//! would have, and does emit `mbt::actionTaken` per intermediate step — empirically
//! verified against every named run below, including a `.fail()`-terminated one
//! (`gracefulFailoverRefusesWithoutBarrierTest`), before this file was written this way.
//! Every named-run test below is therefore `#[quint_run(init = "<name>", max_steps = 0,
//! max_samples = 1)]` rather than `#[quint_test(test = "<name>")]` — same replay
//! semantics, no spec changes, no production API changes.
//!
//! ## Why `.fail()`-terminated runs cannot complete via ordinary state comparison
//!
//! Five named runs below end with an action guarded `.fail()` (the model's own idiom
//! for "this must be refused from this state"). For that terminal step, `quint`'s
//! `--mbt` trace carries only `mbt::actionTaken`/`mbt::nondetPicks` — no state variable
//! at all (verified directly against `--out-itf` for `removeNodeRefusesLiveOwnerTest`)
//! — so `State::from_spec` sees a genuinely empty ITF record and `ClusterProjection`'s
//! required fields cannot deserialize from it. `ScriptedProj` (below) makes this
//! explicit: an empty record decodes as `ScriptedProj::NoSpecState`, which compares
//! equal to anything, and the refusal itself is asserted directly in each affected
//! run's terminal scripted closure (`assert!(result.is_err(), ...)` against
//! `ClusterState::apply_local`'s own return). Search this file for "Terminal `.fail()`
//! step" to find all five.

use frogdb_cluster::{
    ClusterCommand, ClusterState, HANDOFF_BARRIER_MS, HANDOFF_LEASE_MS, NodeInfo, NodeRole,
    SlotRange,
};
use quint_connect::{Driver, Result, State, quint_run, switch};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;

// ---------------------------------------------------------------------------
// Mirror types for the model's sum types / records.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum RoleTagQ {
    Primary,
    Replica,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "tag", content = "value")]
enum QOpt<T> {
    Some(T),
    None,
}

impl<T> QOpt<T> {
    fn from_option(o: Option<T>) -> Self {
        match o {
            Some(v) => QOpt::Some(v),
            Option::None => QOpt::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct HandoffProj {
    seq: i64,
    drained: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct MigrationProj {
    source: i64,
    target: i64,
    handoff: QOpt<HandoffProj>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct NodeProj {
    role: RoleTagQ,
    parent: QOpt<i64>,
    member: bool,
    fail: bool,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct ClusterProjection {
    nodes: BTreeMap<i64, NodeProj>,
    slots: BTreeMap<i64, QOpt<i64>>,
    migrations: BTreeMap<i64, QOpt<MigrationProj>>,
    handoff_seq: i64,
    epoch: i64,
    node_epoch: BTreeMap<i64, i64>,
}

// The model's fixed universe: NODES = Set(1,2,3,4), SLOTS = Set(1,2,3,4).
const NODES: [i64; 4] = [1, 2, 3, 4];
const SLOTS: [i64; 4] = [1, 2, 3, 4];

fn addr(n: u64) -> SocketAddr {
    format!("127.0.0.1:{}", 16379 + n).parse().unwrap()
}

fn cluster_addr(n: u64) -> SocketAddr {
    format!("127.0.0.1:{}", 26379 + n).parse().unwrap()
}

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

struct ClusterDriver {
    state: ClusterState,
    // Monotonic clock for `proposed_at_ms` args the model has no analogue for.
    // Ticks stay well inside HANDOFF_BARRIER_MS/HANDOFF_LEASE_MS so "should
    // succeed per the model" cases don't spuriously trip code's wall-clock
    // admission window (a real, separate mechanism the model doesn't have).
    clock_ms: u64,
}

impl ClusterDriver {
    fn new() -> Self {
        ClusterDriver {
            state: ClusterState::new(),
            clock_ms: 1_000_000,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock_ms += 1;
        self.clock_ms
    }

    // Mirrors the model's `init` action's topology exactly:
    //   1, 2 = Primary, parent None, slots {1,2} / {3,4}
    //   3 = Replica of 1, 4 = Replica of 2
    fn init(&mut self) {
        self.state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(1, addr(1), cluster_addr(1)),
            })
            .expect("seed node 1");
        self.state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_primary(2, addr(2), cluster_addr(2)),
            })
            .expect("seed node 2");
        self.state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(3, addr(3), cluster_addr(3), 1),
            })
            .expect("seed node 3");
        self.state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(4, addr(4), cluster_addr(4), 2),
            })
            .expect("seed node 4");
        self.state
            .apply_local(ClusterCommand::AssignSlots {
                node_id: 1,
                slots: vec![SlotRange::new(1, 2)],
            })
            .expect("seed slots 1-2");
        self.state
            .apply_local(ClusterCommand::AssignSlots {
                node_id: 2,
                slots: vec![SlotRange::new(3, 4)],
            })
            .expect("seed slots 3-4");
    }

    fn set_role(&mut self, n: i64, role: RoleTagQ, parent: QOpt<i64>) {
        let role = match role {
            RoleTagQ::Primary => NodeRole::Primary,
            RoleTagQ::Replica => NodeRole::Replica,
        };
        let primary_id = match parent {
            QOpt::Some(p) => Some(p as u64),
            QOpt::None => None,
        };
        let _ = self.state.apply_local(ClusterCommand::SetRole {
            node_id: n as u64,
            role,
            primary_id,
        });
    }

    // Diverges from the model (issue 20): code's RemoveNode has no "still owns
    // slots" guard, and no `force` field at all — both `force` values map to
    // the same unconditional command. See the module-level design note.
    fn remove_node(&mut self, n: i64, _force: bool) {
        let _ = self
            .state
            .apply_local(ClusterCommand::RemoveNode { node_id: n as u64 });
    }

    fn assign_slot(&mut self, s: i64, n: i64) {
        let _ = self.state.apply_local(ClusterCommand::AssignSlots {
            node_id: n as u64,
            slots: vec![SlotRange::new(s as u16, s as u16)],
        });
    }

    fn remove_slot(&mut self, s: i64) {
        if let Some(owner) = self.state.get_slot_owner(s as u16) {
            let _ = self.state.apply_local(ClusterCommand::RemoveSlots {
                node_id: owner,
                slots: vec![SlotRange::new(s as u16, s as u16)],
            });
        }
    }

    fn begin_migration(&mut self, s: i64, source: i64, target: i64) {
        let _ = self.state.apply_local(ClusterCommand::BeginSlotMigration {
            slot: s as u16,
            source_node: source as u64,
            target_node: target as u64,
        });
    }

    // Diverges from the model (issue 17): code refuses (`HandoffNotReady`) a
    // re-prepare while the current handoff's *lease* has not yet expired,
    // rather than unconditionally superseding it.
    fn prepare_handoff(&mut self, s: i64) {
        let Some(migration) = self.state.get_slot_migration(s as u16) else {
            return;
        };
        let now = self.tick();
        let _ = self.state.apply_local(ClusterCommand::PrepareSlotHandoff {
            slot: s as u16,
            source_node: migration.source_node,
            target_node: migration.target_node,
            barrier_ms: HANDOFF_BARRIER_MS,
            lease_ms: HANDOFF_LEASE_MS,
            proposed_at_ms: now,
        });
    }

    fn confirm_drained(&mut self, s: i64, seq: i64) {
        let _ = self
            .state
            .apply_local(ClusterCommand::ConfirmSlotHandoffDrained {
                slot: s as u16,
                seq: seq as u64,
            });
    }

    fn complete_migration(&mut self, s: i64, _seq: i64) {
        let Some(migration) = self.state.get_slot_migration(s as u16) else {
            return;
        };
        let now = self.tick();
        let _ = self
            .state
            .apply_local(ClusterCommand::CompleteSlotMigration {
                slot: s as u16,
                source_node: migration.source_node,
                target_node: migration.target_node,
                proposed_at_ms: now,
            });
    }

    fn abort_handoff(&mut self, s: i64, seq: i64) {
        let _ = self.state.apply_local(ClusterCommand::AbortSlotHandoff {
            slot: s as u16,
            seq: seq as u64,
        });
    }

    // Diverges from the model (issue 15): code removes the migration record
    // unconditionally and immediately; there is no repatriation phase, so
    // `cancelling`/`repatriating` have no code counterpart at all.
    fn cancel_migration(&mut self, s: i64) {
        let _ = self
            .state
            .apply_local(ClusterCommand::CancelSlotMigration { slot: s as u16 });
    }

    // No code counterpart: no `CompleteRepatriation` command exists (issue 15).
    fn complete_repatriation(&mut self, _s: i64) {}

    fn mark_node_failed(&mut self, n: i64) {
        let _ = self
            .state
            .apply_local(ClusterCommand::MarkNodeFailed { node_id: n as u64 });
    }

    fn mark_node_recovered(&mut self, n: i64) {
        let _ = self
            .state
            .apply_local(ClusterCommand::MarkNodeRecovered { node_id: n as u64 });
    }

    // No code counterpart: `armed_barriers`/`failover_fence` are pure ghost
    // state the model uses to express TR-CLUSTER-026's not-yet-implemented
    // drain/parity barrier (issue 26). The production barrier planner
    // (`handoff_barrier.rs::plan_handoff_action`) lives in
    // `frogdb-cluster-runtime`, unreachable from this crate's test target.
    fn arm_failover_fence(&mut self, _old: i64) {}

    // Diverges from the model (issues 19, 20, 26): `ClusterCommand::Failover`
    // has no fence fields (issue 19), the forced branch removes rather than
    // demotes the old primary (issue 20), and neither branch waits on a
    // drain/offset-parity barrier (issue 26 — graceful path only in the
    // model).
    fn failover(&mut self, old: i64, succ: i64, force: bool) {
        let _ = self.state.apply_local(ClusterCommand::Failover {
            old_primary_id: old as u64,
            new_primary_id: succ as u64,
            force,
        });
    }

    // No code counterpart: no `FeedBuffer`-shaped command exists at all
    // (issue 17's byte-cap hold amendment is unimplemented).
    fn feed_buffer(&mut self, _s: i64, _n: i64) {}
}

impl State<ClusterDriver> for ClusterProjection {
    fn from_driver(driver: &ClusterDriver) -> Result<Self> {
        let state = &driver.state;

        let mut nodes = BTreeMap::new();
        for &n in &NODES {
            let proj = match state.get_node(n as u64) {
                Some(info) => NodeProj {
                    role: match info.role {
                        NodeRole::Primary => RoleTagQ::Primary,
                        NodeRole::Replica => RoleTagQ::Replica,
                    },
                    parent: QOpt::from_option(info.primary_id.map(|p| p as i64)),
                    member: true,
                    fail: info.flags.fail,
                },
                // A node absent from the table only happens today via the
                // divergent force-remove/force-failover paths (issue 20). We
                // project it as non-member so equality against the model's
                // (demoted-but-member) expectation fails loudly rather than
                // panicking here.
                None => NodeProj {
                    role: RoleTagQ::Replica,
                    parent: QOpt::None,
                    member: false,
                    fail: false,
                },
            };
            nodes.insert(n, proj);
        }

        let mut slots = BTreeMap::new();
        for &s in &SLOTS {
            slots.insert(
                s,
                QOpt::from_option(state.get_slot_owner(s as u16).map(|o| o as i64)),
            );
        }

        let mut migrations = BTreeMap::new();
        for &s in &SLOTS {
            let proj = match state.get_slot_migration(s as u16) {
                Some(mig) => QOpt::Some(MigrationProj {
                    source: mig.source_node as i64,
                    target: mig.target_node as i64,
                    handoff: QOpt::from_option(mig.handoff.map(|h| HandoffProj {
                        seq: h.seq as i64,
                        drained: h.drained,
                    })),
                }),
                Option::None => QOpt::None,
            };
            migrations.insert(s, proj);
        }

        let mut node_epoch = BTreeMap::new();
        for &n in &NODES {
            let e = state
                .get_node(n as u64)
                .map(|i| i.config_epoch as i64)
                .unwrap_or(0i64);
            node_epoch.insert(n, e);
        }

        Ok(ClusterProjection {
            nodes,
            slots,
            migrations,
            handoff_seq: state.snapshot().handoff_seq as i64,
            epoch: state.config_epoch() as i64,
            node_epoch,
        })
    }
}

// ## Nondet-pick names (simulation driver only)
//
// The model's `step` action (the only place `nondet` appears in this spec) binds
// exactly 12 names into `mbt::nondetPicks`: `s, n1, n2, handoffSlot, succGuess,
// roleGuess, obsCorrectCoin, obsEpochGuess, obsRoleGuess, bytesGuess, seqGuess,
// forceGuess` (confirmed by inspecting `mbt::nondetPicks`'s keys in a real `--mbt`
// trace). `switch!` decodes a handler's declared parameters by looking each one up
// under its *exact* name in that record — so the identifiers below are the model's
// own nondet-binding names, not each action's own parameter names (which mostly
// differ, e.g. `beginMigration(s, beginSource, n2)`'s `beginSource` is a computed
// `val`, not a nondet binding, and is re-derived from `slots`/`n1` below to match).
// Several call-site arguments (`setRole`'s `parent`, `beginMigration`'s `source`,
// `failoverGraceful`'s `obsEpoch`/`obsRole`) are themselves *computed* `val`s over
// these 12 raw picks, not raw picks themselves — commented at each arm below.
impl Driver for ClusterDriver {
    type State = ClusterProjection;

    // `switch!` decodes each handler parameter by looking its Rust identifier up
    // verbatim in `mbt::nondetPicks` — so these names are pinned to the model's own
    // (non-snake-case) nondet-binding names, not renameable to satisfy clippy.
    #[allow(non_snake_case)]
    fn step(&mut self, step: &quint_connect::Step) -> Result {
        switch!(step {
            init => self.init(),
            // parent = if roleGuess == Replica { Some(n2) } else { None }
            setRole(n1: i64, roleGuess: RoleTagQ, n2: i64) => {
                let parent = match roleGuess {
                    RoleTagQ::Replica => QOpt::Some(n2),
                    RoleTagQ::Primary => QOpt::None,
                };
                self.set_role(n1, roleGuess, parent)
            }
            removeNode(n1: i64, forceGuess: bool) => self.remove_node(n1, forceGuess),
            assignSlot(s: i64, n1: i64) => self.assign_slot(s, n1),
            removeSlot(s: i64) => self.remove_slot(s),
            // source = match slots.get(s) { Some(o) => o, None => n1 } (beginSource)
            beginMigration(s: i64, n1: i64, n2: i64) => {
                let source = self.state.get_slot_owner(s as u16).map(|o| o as i64).unwrap_or(n1);
                self.begin_migration(s, source, n2)
            }
            prepareHandoff(handoffSlot: i64) => self.prepare_handoff(handoffSlot),
            confirmDrained(handoffSlot: i64, seqGuess: i64) => self.confirm_drained(handoffSlot, seqGuess),
            completeMigration(handoffSlot: i64, seqGuess: i64) => self.complete_migration(handoffSlot, seqGuess),
            abortHandoff(handoffSlot: i64, seqGuess: i64) => self.abort_handoff(handoffSlot, seqGuess),
            cancelMigration(handoffSlot: i64) => self.cancel_migration(handoffSlot),
            completeRepatriation(handoffSlot: i64) => self.complete_repatriation(handoffSlot),
            feedBuffer(handoffSlot: i64, bytesGuess: i64) => self.feed_buffer(handoffSlot, bytesGuess),
            markNodeFailed(n1: i64) => self.mark_node_failed(n1),
            markNodeRecovered(n1: i64) => self.mark_node_recovered(n1),
            armFailoverFence(n1: i64) => self.arm_failover_fence(n1),
            // obsEpoch/obsRole (all three failover variants) have no representation in
            // `ClusterCommand::Failover` at all (issue 19: no fence fields) — not decoded,
            // nothing to feed them to.
            failoverGraceful(n1: i64, succGuess: i64) => self.failover(n1, succGuess, false),
            failoverAuto(n1: i64, succGuess: i64) => self.failover(n1, succGuess, true),
            failoverForced(n1: i64, succGuess: i64) => self.failover(n1, succGuess, true),
            stutter => {},
        })
    }
}

fn new_driver() -> ClusterDriver {
    ClusterDriver::new()
}

// ---------------------------------------------------------------------------
// Named-run replay: scripted driver.
// ---------------------------------------------------------------------------
//
// Named `run`s in the spec invoke actions with LITERAL argument values
// (`beginMigration(1, 1, 2)`, `failoverGraceful(1, 3, 0, Primary)`, ...) — never
// through `nondet`. Quint's `--mbt` instrumentation only records values genuinely
// chosen via a `nondet` binding into `mbt::nondetPicks`; literal call arguments are
// never captured anywhere in the trace (empirically confirmed: every
// `mbt::nondetPicks` key is `None` at every step of every named-run trace, whether
// produced via `quint test` or via `quint run --init <name> --max-steps 0`).
// `switch!` decodes parameters *exclusively* via `mbt::nondetPicks` lookups (see
// quint-connect's `NondetPick::expand`, `step.nondet_picks.get(name)`), so it
// cannot recover a named run's literal arguments — structurally, not a naming
// mismatch to fix.
//
// `ScriptedDriver` sidesteps this: each named run is a small, fixed,
// already-known-in-advance action sequence (the literal bodies read directly from
// the spec's Tests section, reproduced verbatim in each test function below). Each
// test builds an ordered script of `(expected action name, closure)` pairs.
// `step()` asserts the trace's actual `mbt::actionTaken` matches the next scripted
// action name — so a spec edit that reorders/renames/adds a step in the run fails
// loudly here, not silently — then runs the closure against the same
// `ClusterDriver` helper methods the simulation driver above uses. The real
// conformance check is untouched: `ClusterProjection::from_driver` still reads
// live from `ClusterState`, and quint-connect still compares it against the real
// ITF trace's state at every step, exactly as for the simulation driver above.
// Only the argument *values* fed into each replayed action are hardcoded (from
// the spec text) rather than mechanically decoded from the trace.
struct ScriptedDriver {
    inner: ClusterDriver,
    script: std::collections::VecDeque<(&'static str, Box<dyn FnMut(&mut ClusterDriver)>)>,
}

impl ScriptedDriver {
    fn new(script: Vec<(&'static str, Box<dyn FnMut(&mut ClusterDriver)>)>) -> Self {
        ScriptedDriver {
            inner: ClusterDriver::new(),
            script: script.into(),
        }
    }
}

/// Shorthand for one scripted `(action name, closure)` pair.
fn step_(
    name: &'static str,
    f: impl FnMut(&mut ClusterDriver) + 'static,
) -> (&'static str, Box<dyn FnMut(&mut ClusterDriver)>) {
    (name, Box::new(f))
}

// ## `ScriptedProj`: the scripted driver's own state type
//
// Five named runs in the spec end with an action guarded `.fail()` (the Quint MBT
// idiom for "this must be refused from this state"): `gracefulFailoverRefusesWithoutBarrierTest`,
// `cancelRefusedWhileRepatriatingTest`, `prepareSupersedesLiveHandoffTest`,
// `failoverRefusesStaleFenceTest`, `removeNodeRefusesLiveOwnerTest`. For a `.fail()`-guarded
// step, quint's `--mbt` trace records only `mbt::actionTaken`/`mbt::nondetPicks` for that
// step — no state variables at all — and `Step::new` (quint-connect's `driver/step.rs`)
// strips exactly those two keys before handing the remainder to `State::from_spec`, so the
// terminal step of every one of these five runs hands `from_spec` a genuinely empty
// `itf::Value::Record` (confirmed against `quint run --init removeNodeRefusesLiveOwnerTest
// --max-steps 0 --mbt --out-itf`). `ClusterProjection`'s six required fields can't
// deserialize from that, so replaying any of the five hits
// `Failed to deserialize specification's state.` at the terminal step, independent of
// whether the run's own refusal actually holds in code — a harness gap, not a conformance
// result (three of the five happen to diverge at an *earlier* step under today's code, so
// this gap is latent for them until their earlier issue is fixed; see each test's comment).
//
// `ScriptedProj` makes the terminal stateless step explicit: `from_spec` recognizes an empty
// record and decodes it as `NoSpecState`, which compares equal to anything (see `PartialEq`
// below) rather than failing to deserialize. The refusal itself is then asserted directly in
// each affected run's terminal scripted closure (`assert!(result.is_err(), ...)` against
// `ClusterState::apply_local`'s own return value), independent of this state-comparison
// mechanism. Only `ScriptedDriver` uses this type — the simulation `ClusterDriver` above
// keeps plain `ClusterProjection`, since a sampled simulation step never targets a
// `.fail()`-guarded action specifically (`switch!`'s `stutter` arm no-ops on an unmatched
// action name rather than asserting refusal).
//
// `from_spec` is also where the ghost-field finding (issue 33) is normalized away: see
// `normalize_removed_node_ghost_fields` below.
#[derive(Debug, Deserialize)]
enum ScriptedProj {
    // Never produced by the derived `Deserialize` above — quint-connect's `State` trait
    // requires `DeserializeOwned` as a supertrait bound even though `from_spec` is
    // overridden below and never calls `Self::deserialize`. This derive exists solely to
    // satisfy that bound; its externally-tagged shape does not match any real trace.
    NoSpecState,
    Full(Box<ClusterProjection>),
}

impl PartialEq for ScriptedProj {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScriptedProj::NoSpecState, _) | (_, ScriptedProj::NoSpecState) => true,
            (ScriptedProj::Full(a), ScriptedProj::Full(b)) => a == b,
        }
    }
}

/// Ghost-field normalization for issue 33 (unfiled at Task-3 review time, filed as issue 33
/// during the fix round): the model's `nodes` map is total over the fixed `NODES` set, so
/// `removeNode` flips only `member: false` and leaves `role`/`parent` at their last live
/// value; `node_epoch`'s entry for the same node survives the same way. `ClusterState`'s
/// `RemoveNode` does a true `nodes.remove(&node_id)` (`commands.rs:215-236`, matching
/// `specs/cluster.md`'s TR-CLUSTER-003), so there is no live data to compare those stale
/// values against. This blanks them, for any node the spec reports `member: false`, to the
/// same defaults `ClusterProjection::from_driver` already produces for a node absent from
/// `ClusterState` (`RoleTagQ::Replica`, `parent: None`, `node_epoch: 0`) — a total,
/// case-independent normalization applied to every deserialize, not a per-test workaround.
/// A deleted record is unobservable by construction; this is the harness-side stopgap until
/// issue 33's model fix (null the same fields in the same `removeNode` transition) lands and
/// this function can be deleted.
fn normalize_removed_node_ghost_fields(value: itf::Value) -> itf::Value {
    use itf::Value;

    let Value::Record(mut rec) = value else {
        return value;
    };
    let Some(Value::Map(nodes)) = rec.get("nodes").cloned() else {
        return Value::Record(rec);
    };

    let mut ghost_ids = Vec::new();
    let mut normalized_nodes = itf::value::Map::new();
    for (id, node) in nodes.iter() {
        let is_member =
            matches!(node, Value::Record(r) if matches!(r.get("member"), Some(Value::Bool(true))));
        if is_member {
            normalized_nodes.insert(id.clone(), node.clone());
            continue;
        }
        ghost_ids.push(id.clone());
        if let Value::Record(node_rec) = node {
            let mut blanked = node_rec.clone();
            // Matches `RoleTagQ`/`QOpt`'s adjacently-tagged wire shape (`#[serde(tag =
            // "tag", content = "value")]`) for their unit variants, which carry no
            // "value" key.
            let mut role = itf::value::Record::new();
            role.insert("tag".to_string(), Value::String("Replica".to_string()));
            blanked.insert("role".to_string(), Value::Record(role));
            let mut parent = itf::value::Record::new();
            parent.insert("tag".to_string(), Value::String("None".to_string()));
            blanked.insert("parent".to_string(), Value::Record(parent));
            normalized_nodes.insert(id.clone(), Value::Record(blanked));
        } else {
            normalized_nodes.insert(id.clone(), node.clone());
        }
    }
    rec.insert("nodes".to_string(), Value::Map(normalized_nodes));

    if let Some(Value::Map(epochs)) = rec.get("node_epoch").cloned() {
        let mut normalized_epochs = itf::value::Map::new();
        for (id, epoch) in epochs.iter() {
            if ghost_ids.contains(id) {
                normalized_epochs.insert(id.clone(), Value::Number(0));
            } else {
                normalized_epochs.insert(id.clone(), epoch.clone());
            }
        }
        rec.insert("node_epoch".to_string(), Value::Map(normalized_epochs));
    }

    Value::Record(rec)
}

impl State<ScriptedDriver> for ScriptedProj {
    fn from_driver(driver: &ScriptedDriver) -> Result<Self> {
        Ok(ScriptedProj::Full(Box::new(
            ClusterProjection::from_driver(&driver.inner)?,
        )))
    }

    fn from_spec(value: itf::Value) -> Result<Self> {
        if let itf::Value::Record(rec) = &value {
            if rec.is_empty() {
                return Ok(ScriptedProj::NoSpecState);
            }
        }
        let normalized = normalize_removed_node_ghost_fields(value);
        let proj = ClusterProjection::deserialize(normalized)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize specification's state: {e}"))?;
        Ok(ScriptedProj::Full(Box::new(proj)))
    }
}

impl Driver for ScriptedDriver {
    type State = ScriptedProj;

    fn step(&mut self, step: &quint_connect::Step) -> Result {
        let action = step.action_taken.as_str();
        let (expected, mut apply) = self.script.pop_front().ok_or_else(|| {
            anyhow::anyhow!("script exhausted, but trace has another action `{action}`")
        })?;
        anyhow::ensure!(
            action == expected,
            "script/trace action mismatch: script expected `{expected}`, trace has `{action}` \
             (the named run in the .qnt spec no longer matches this test's hardcoded script)"
        );
        apply(&mut self.inner);
        Ok(())
    }
}

// I6: `step()` above already errors when the trace has more actions than the script; this
// catches the converse — a named run in the `.qnt` losing a trailing step would otherwise
// leave the corresponding scripted closure silently never applied, and the test would still
// pass. `!std::thread::panicking()` avoids masking a real panic (e.g. an `assert!` failure,
// or code's own invariant panic) with a confusing second one during unwind.
impl Drop for ScriptedDriver {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(
                self.script.is_empty(),
                "script not exhausted: {} scripted step(s) never appeared in the trace",
                self.script.len()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Named-run tests.
// ---------------------------------------------------------------------------

// (a) Full happy migration: begin -> prepare -> confirm drained -> complete.
// No failover/repatriation/fence involved; matches code exactly.
#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "happyMigrationTest",
    max_steps = 0,
    max_samples = 1
)]
fn happy_migration_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("confirmDrained", |d| d.confirm_drained(1, 1)),
        step_("completeMigration", |d| d.complete_migration(1, 1)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "gracefulFailoverBeforePrepareTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 15 — observed failure is a panic inside production code, not a projection mismatch: commands.rs:108 `cluster state invariants violated after apply_command: INV-MIG-1: slot 1 is migrating from 1 but is owned by 3` (graceful Failover never prunes migrations sourced at the old primary); see issue 15's witness section and .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md"]
fn graceful_failover_before_prepare_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("armFailoverFence", |d| d.arm_failover_fence(1)),
        step_("failoverGraceful", |d| d.failover(1, 3, false)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "gracefulFailoverRefusesWithoutBarrierTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 15 — observed failure is a panic inside production code, not a missing barrier: commands.rs:108 `cluster state invariants violated after apply_command: INV-MIG-1: slot 1 is migrating from 1 but is owned by 3` (graceful Failover never prunes migrations sourced at the old primary); previously miscited to issue 26 (F2/F4) — see issue 15's witness section and .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md"]
fn graceful_failover_refuses_without_barrier_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        // Terminal `.fail()` step (F3): asserted directly rather than scripted through
        // the `failover` helper, since `ScriptedProj::from_spec` sees an empty ITF
        // record here and cannot itself compare against a live projection.
        step_("failoverGraceful", |d| {
            let result = d.state.apply_local(ClusterCommand::Failover {
                old_primary_id: 1,
                new_primary_id: 3,
                force: false,
            });
            assert!(
                result.is_err(),
                "gracefulFailoverRefusesWithoutBarrierTest: expected Failover to be \
                 refused without an armed drain/parity barrier (issue 26, once the \
                 barrier exists); today this call instead panics inside apply_command \
                 via issue 15's INV-MIG-1 (see issue 15's witness section) — either way \
                 it must not return Ok"
            );
        }),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "gracefulFailoverArmedNotDrainedTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 15 — observed failure is a panic inside production code, not a projection mismatch: commands.rs:108 `cluster state invariants violated after apply_command: INV-MIG-1: slot 1 is migrating from 1 but is owned by 3` (graceful Failover never prunes migrations sourced at the old primary); see issue 15's witness section and .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md"]
fn graceful_failover_armed_not_drained_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("armFailoverFence", |d| d.arm_failover_fence(1)),
        step_("failoverGraceful", |d| d.failover(1, 3, false)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "gracefulFailoverDrainedNotCompleteTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 15 — observed failure is a panic inside production code, not a projection mismatch: commands.rs:108 `cluster state invariants violated after apply_command: INV-MIG-1: slot 1 is migrating from 1 but is owned by 3` (graceful Failover never prunes migrations sourced at the old primary); see issue 15's witness section and .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md"]
fn graceful_failover_drained_not_complete_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("confirmDrained", |d| d.confirm_drained(1, 1)),
        step_("armFailoverFence", |d| d.arm_failover_fence(1)),
        step_("failoverGraceful", |d| d.failover(1, 3, false)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "abortRepatriatesTest",
    max_steps = 0,
    max_samples = 1
)]
// PASSES today, but weakly (F2): `repatriating`/`provisional_target` are excluded from
// `ClusterProjection` (issue 15 — no repatriation phase exists in code at all, so there is
// nothing on the driver side to project), so this test cannot observe a divergence on those
// fields either way — only on the fields it shares with the happy-path projection (which
// `AbortSlotHandoff` does implement correctly: it clears the handoff and keeps the migration
// record). Left in the default suite because that shared-field coverage is real; not
// evidence that issue 15's repatriation phase is implemented.
fn abort_repatriates_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("confirmDrained", |d| d.confirm_drained(1, 1)),
        step_("abortHandoff", |d| d.abort_handoff(1, 1)),
        step_("completeRepatriation", |d| d.complete_repatriation(1)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "cancelBeforeRepatriationTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 15 — no repatriation phase exists in code: CancelSlotMigration removes the record unconditionally and immediately, with no `cancelling`/residue bookkeeping; see .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md"]
fn cancel_before_repatriation_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("confirmDrained", |d| d.confirm_drained(1, 1)),
        step_("cancelMigration", |d| d.cancel_migration(1)),
        step_("completeRepatriation", |d| d.complete_repatriation(1)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "cancelRefusedWhileRepatriatingTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 15 — no repatriation phase exists in code, so there is no repatriating-phase re-entry guard to refuse a second Cancel; see .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md"]
fn cancel_refused_while_repatriating_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("confirmDrained", |d| d.confirm_drained(1, 1)),
        step_("cancelMigration", |d| d.cancel_migration(1)),
        // Terminal `.fail()` step (F3): asserted directly, see the comment on
        // `graceful_failover_refuses_without_barrier_test` above.
        step_("cancelMigration", |d| {
            let result = d
                .state
                .apply_local(ClusterCommand::CancelSlotMigration { slot: 1 });
            assert!(
                result.is_err(),
                "cancelRefusedWhileRepatriatingTest: expected a second \
                 CancelSlotMigration to be refused while repatriation is still pending \
                 (issue 15's repatriating-phase re-entry guard, not yet implemented)"
            );
        }),
    ])
}

// The no-residue case: cancelling a migration that never reached a drained
// handoff removes the record immediately in both the model and the code —
// no repatriation phase applies, so this one does not diverge.
#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "cancelWithoutResidueAppliesImmediatelyTest",
    max_steps = 0,
    max_samples = 1
)]
fn cancel_without_residue_applies_immediately_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("cancelMigration", |d| d.cancel_migration(1)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "prepareSupersedesLiveHandoffTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 17 — code's PrepareSlotHandoff refuses (HandoffNotReady) a re-prepare while the current handoff's lease has not expired, rather than superseding it; see .scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md"]
fn prepare_supersedes_live_handoff_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        // Terminal `.fail()` step (F3): asserted directly, see the comment on
        // `graceful_failover_refuses_without_barrier_test` above. Ground truth per
        // `cluster_migration_failover.qnt`: the refused call is `confirmDrained(1, 1)`
        // with the now-stale seq 1 — not a third `prepareHandoff`.
        step_("confirmDrained", |d| {
            let result = d
                .state
                .apply_local(ClusterCommand::ConfirmSlotHandoffDrained { slot: 1, seq: 1 });
            assert!(
                result.is_err(),
                "prepareSupersedesLiveHandoffTest: expected ConfirmSlotHandoffDrained \
                 with the now-stale seq 1 to be refused once a second PrepareSlotHandoff \
                 has superseded it to seq 2 (issue 17, not yet implemented — code instead \
                 refuses the *second* PrepareSlotHandoff itself with HandoffNotReady)"
            );
        }),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "forcedFailoverDemotesTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 20 — code's force:true Failover branch removes the old primary from the node table rather than demoting it (demote-don't-remove is implemented on the graceful branch only); see .scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md"]
fn forced_failover_demotes_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("failoverForced", |d| d.failover(1, 3, true)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "failoverRefusesStaleFenceTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 19 — ClusterCommand::Failover has no per-object fence fields (no obsEpoch/obsRole) and no fence check, so a stale proposal is never refused; see .scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md"]
fn failover_refuses_stale_fence_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("failoverForced", |d| d.failover(1, 3, true)),
        // Terminal `.fail()` step (F3): asserted directly, see the comment on
        // `graceful_failover_refuses_without_barrier_test` above.
        step_("failoverForced", |d| {
            let result = d.state.apply_local(ClusterCommand::Failover {
                old_primary_id: 3,
                new_primary_id: 1,
                force: true,
            });
            assert!(
                result.is_err(),
                "failoverRefusesStaleFenceTest: expected a stale-epoch Failover proposal \
                 to be refused by a per-object fence (issue 19, not yet implemented — \
                 ClusterCommand::Failover has no obsEpoch/obsRole fields at all)"
            );
        }),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "feedHoldOverflowDisconnectsTest",
    max_steps = 0,
    max_samples = 1
)]
// PASSES today, but weakly (F2): `feed_bytes`/`disconnected_feed` are excluded from
// `ClusterProjection` (issue 17 — no `FeedBuffer`-shaped command exists in code at all, so
// `ClusterDriver::feed_buffer` is a no-op), so this test cannot observe a divergence on
// those fields either way. Left in the default suite for the same reason as
// `abort_repatriates_test` above (whatever shared-field coverage the surrounding
// begin/prepare steps exercise); not evidence the byte-cap hold is implemented.
fn feed_hold_overflow_disconnects_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("beginMigration", |d| d.begin_migration(1, 1, 2)),
        step_("prepareHandoff", |d| d.prepare_handoff(1)),
        step_("feedBuffer", |d| d.feed_buffer(1, 3)),
        step_("feedBuffer", |d| d.feed_buffer(1, 3)),
        step_("feedBuffer", |d| d.feed_buffer(1, 1)),
    ])
}

#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "removeNodeRefusesLiveOwnerTest",
    max_steps = 0,
    max_samples = 1
)]
#[ignore = "pending issue 20 — the terminal step now asserts directly (ScriptedProj/F3) that RemoveNode is refused; it is not, because code's RemoveNode has no still-owns-slots guard at all and always succeeds; see .scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md"]
fn remove_node_refuses_live_owner_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        // Terminal `.fail()` step (F3): asserted directly, see the comment on
        // `graceful_failover_refuses_without_barrier_test` above. Before this fix, this
        // was the one run whose observed symptom actually was
        // "Failed to deserialize specification's state." (F3's evidence anchor) rather
        // than a divergence — see the module-header ghost-field/`.fail()` discussion.
        step_("removeNode", |d| {
            let result = d
                .state
                .apply_local(ClusterCommand::RemoveNode { node_id: 1 });
            assert!(
                result.is_err(),
                "removeNodeRefusesLiveOwnerTest: expected RemoveNode to be refused for a \
                 member still owning a slot (issue 20, not yet implemented — code's \
                 RemoveNode has no still-owns-slots guard at all and always succeeds)"
            );
        }),
    ])
}

// Ghost-field finding, filed as issue 33: the model represents `removeNode` as
// tombstoning — `nodes` is a total map over the fixed NODES set, so a removed node's
// map entry survives with `member: false` but its `role`/`parent` fields untouched by
// the transition. The real code's `RemoveNode` arm (`commands.rs:215-236`) does
// `inner.nodes.remove(&node_id)` — a genuine deletion, matching TR-CLUSTER-003's own
// postcondition in `specs/cluster.md` ("Node membership" loses `node_id`) — so there
// is no live data anywhere in `ClusterState` to recover a removed node's last
// role/parent from; this is a model-representation artifact (`specs/cluster.md`
// confirms real deletion is the intended semantics), not a code defect. Un-ignored:
// `ScriptedProj::from_spec`'s ghost-field normalization (I4/F5, module header) blanks
// the same fields on the spec side for any node reporting `member: false`, restoring
// this test's real slot-pruning/reparenting coverage. Issue 33 tracks the model-side
// fix (null the same fields in `removeNode`'s own transition) that would let this
// normalization be deleted.
#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "removeNodeAfterDemotionSucceedsTest",
    max_steps = 0,
    max_samples = 1
)]
fn remove_node_after_demotion_succeeds_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("armFailoverFence", |d| d.arm_failover_fence(1)),
        step_("failoverGraceful", |d| d.failover(1, 3, false)),
        step_("removeNode", |d| d.remove_node(1, false)),
    ])
}

// Code's RemoveNode has no `force` field (see the module-level design note),
// so both `force` values map to the same unconditional command, which was
// expected to match the model's `force: true` branch exactly for this scenario
// (unconditional success, slots pruned, non-member) — it does for slots/
// membership, and now for role/parent too, via the same ghost-field
// normalization (issue 33) described on `remove_node_after_demotion_succeeds_test`
// above.
#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    init = "removeNodeForceEvictsLiveOwnerTest",
    max_steps = 0,
    max_samples = 1
)]
fn remove_node_force_evicts_live_owner_test() -> ScriptedDriver {
    ScriptedDriver::new(vec![
        step_("init", |d| d.init()),
        step_("removeNode", |d| d.remove_node(1, true)),
    ])
}

// ---------------------------------------------------------------------------
// Simulation tests.
// ---------------------------------------------------------------------------
//
// The model's `step` action samples uniformly across all 19 actions, including
// failoverGraceful/Auto/Forced, feedBuffer and completeRepatriation. Any
// sampled trace that reaches one of those is expected to diverge immediately
// by the same issues cited above (15, 17, 19, 20, 26) — the model's own header
// says as much: it encodes the ruled/amended target semantics for those five
// issues, none of which are implemented yet. Both simulation tests are
// therefore ignored pending the same issues as the named tests above, rather
// than narrowly scoped to a single row.
#[quint_run(
    spec = "../../../specs/quint/cluster_migration_failover.qnt",
    seed = "20260813"
)]
#[ignore = "pending issues 15/17/19/20/26 plus issue 33 (ghost-field finding) — any sampled trace touching failoverGraceful/Auto/Forced, feedBuffer, completeRepatriation, or removeNode diverges immediately (simulation traces use plain ClusterDriver/ClusterProjection, not ScriptedProj, so issue 33's harness-side normalization does not apply here); see the named-run citations above"]
fn seeded_simulation_test() -> ClusterDriver {
    new_driver()
}

#[quint_run(spec = "../../../specs/quint/cluster_migration_failover.qnt")]
#[ignore = "pending issues 15/17/19/20/26 plus issue 33 (ghost-field finding) — any sampled trace touching failoverGraceful/Auto/Forced, feedBuffer, completeRepatriation, or removeNode diverges immediately (simulation traces use plain ClusterDriver/ClusterProjection, not ScriptedProj, so issue 33's harness-side normalization does not apply here); see the named-run citations above"]
fn unpinned_simulation_test() -> ClusterDriver {
    new_driver()
}
