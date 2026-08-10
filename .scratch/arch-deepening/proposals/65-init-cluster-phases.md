# Proposal 65 — `init_cluster` splits into a boot *plan* and three phase runners

Round 38 · lane: cluster · effort **M** · `frogdb-server` (no mutation gate) wiring the LOCKED
cluster crates · **not spec-first** (no behavior change proposed; one prose spec cite must be
refreshed in the same commit)

Covers exploration-lane candidate **SV3** ("`init_cluster` god-fn — `cluster_init.rs:73-790`, ~20
params; `SplitBrainLogger`/`split_brain_header`/`RoleChangeConsumer` colocated unrelated").
Verified against `6e99f567` (lane base was `08c143d6`; the file has grown since and **every line
number in the lane brief has drifted** — corrections are inline below).

## Summary

`cluster_init.rs` is 1938 lines holding four things that share nothing but a file: one 733-line
`async fn init_cluster` (`:73-806`) with **18 parameters** and an 11-field result; a split-brain
audit *policy* (`SplitBrainLogger` `:813`, `split_brain_header` `:893`); a role-change *consumer*
(`RoleChangeConsumer` `:929`); and 883 lines of tests (`:1056-1938`, 45% of the file) that are
**entirely** about the last two. The boot function itself has **zero tests**.

That last sentence is the proposal. `init_cluster` makes six decisions — this node's id, the
initial member set, whether this node bootstraps, which restored peers to skip, how a port-0
listener resolves into an advertised address, and whether to auto-assign slots — and not one of
them is reachable without a live listener, an opened Raft store, a shard-sender fan-out, a client
registry, a metrics recorder and a TLS runtime. They are decided *inline, between I/O statements*,
so the only way to observe a wrong decision is to boot a cluster and watch what it converges to.

The change has two halves, and they are not the same kind of change:

* **Depth.** Extract `plan_cluster_boot(input) -> ClusterBootPlan`, a pure function over plain
  values, that makes all six decisions. This is the same shape the neighbouring code already uses
  — `plan_handoff_action` + `run_slot_handoff_barrier` in `frogdb-cluster-runtime`,
  `plan_migration_notice` in `frogdb-cluster` — a pure planner with a thin async runner. It buys a
  test surface the boot path does not currently have at any price, and it is the only part of this
  proposal that survives the deletion test.
* **Locality.** Move the split-brain policy and the role-change consumer (plus their 883 test
  lines, tags intact) into `cluster_init/split_brain.rs`, and give each boot phase its own
  function. This buys no leverage and I do not claim it does; it buys the ability to read one
  concern without scrolling past two others, and it makes the 18-parameter signature *visibly* the
  union of three smaller ones rather than an accident.

**No live bug was found in the split's blast radius.** One latent hazard (a phantom Raft voter
when an operator sets `cluster.node-id` *and* lists their own bus address in `initial-nodes`) and
two independently-landable hotfixes (a published config-doc claim that is false, and a dead
config-conversion path) are written up at the end, unfolded from the refactor.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 1938 | **the change.** `ClusterInitResult` `:43-65`; `init_cluster` `:73-806` (phase A `:126-175`, phase B `:177-757`, phase C `:759-791`); `SplitBrainLogger` `:813-881`; `split_brain_header` `:893-910`; `RoleChangeConsumer` `:929-1053`; tests `:1056-1938` |
| `frogdb-server/crates/server/src/server/cluster_init/boot_plan.rs` | new | the pure planner + its unit tests |
| `frogdb-server/crates/server/src/server/cluster_init/split_brain.rs` | new | `SplitBrainLogger`, `split_brain_header`, `RoleChangeConsumer` and the 883 test lines, moved verbatim |
| `frogdb-server/crates/server/src/server/mod.rs` | 1000+ | the **only** caller: `cluster_init::init_cluster(...)` `:348`, args `:349-372`, result consumed `:376-407`. **Diff is empty** under this proposal — the signature is preserved deliberately (see siblings 63/64) |
| `frogdb-server/crates/server/src/server/util.rs` | — | `hash_addr_to_node_id` `:33-37` — the live node-id derivation, called by the planner |
| `frogdb-server/crates/cluster/src/types.rs` | — | `even_slot_ranges` `:265` (called **twice** today, `:532` and `:616`); `ClusterConfig` `:541-558` (dead — hotfix 2) |
| `frogdb-server/crates/cluster/src/state.rs` | — | `attach_snapshot_store` `:646-674` — the restore whose completion the peer-seed skip guard depends on. Read-only here; the ordering it establishes is a constraint the split must preserve |
| `frogdb-server/crates/config/src/cluster.rs` | — | `node_id` field + doc `:23-27`, `effective_node_id` `:231-243` (dead), `effective_client_addr` `:246-258`, `cluster_bus_socket_addr` `:261-263` |
| `.scratch/hardening/specs/replication-failure-modes.md` | — | FM-REPLICATION-048 Invariant `:1022` — the **only** spec line naming `cluster_init.rs`. Prose-only refresh |
| `frogdb-server/crates/server/src/role_manager.rs` | — | doc `:25` names `server::cluster_init::RoleChangeConsumer` — a module-path cite that goes stale; one-line refresh |
| `website/docs-spec/specs/operations/clustering.md` | — | cites `cluster_init.rs` as bootstrap source of truth `:37`, `:73`, drift guard `:169`. **Stays true** — see "what does not change" |

Correction to the lane brief's citations, all re-derived at `6e99f567`: `init_cluster` is
`:73-806` (not `:73-790`), `SplitBrainLogger` `:813` (not `:795`), `split_brain_header` `:893`
(not `:875`), `RoleChangeConsumer` `:929` (not `:911`), and the parameter count is **18**, not
~20.

## Problem

### One function, three jobs, one of them unconditional

| lines | job | runs when |
|---|---|---|
| `:126-175` | build the **role stack** — mint the shared offset atomic, `RealReplicaStreamer`, `.with_control_applier`, `.with_net_bytes_counters`, `RoleManager::new`, `set_replication_self_fence`, `set_stint_target`, `RoleManagerHandle::new`, `role_controller` | **always**, including standalone |
| `:177-757` | cluster-**Raft** boot — id, state machine, snapshot restore, notification wiring, network factory + connect factories, member set, `Raft::new`, `initialize`, peer seeding, self address resolution, slot auto-assign, four spawns, the slot-migration coordinator | `config.cluster.enabled` |
| `:759-791` | **failure detector** — config, `FailureDetector::new`, `spawn_failure_detector_task` | `config.cluster.enabled` |

A standalone node — no cluster, no Raft, no detector — traverses this function for one reason:
lines `:126-175`. Its name says the opposite. The 18 parameters are the union of the three jobs'
inputs; `#[allow(clippy::too_many_arguments)]` at `:72` is the acknowledgement already in the
tree. The 11-field `ClusterInitResult` (`:43-65`) is the union of their outputs, and the union is
lossy: eight fields are `Option` purely to encode "phase B did not run", while
`role_controller`/`role_manager_handle`/`is_replica_flag` are unconditional because they come from
phase A. A reader cannot tell which `None` means "standalone" and which would mean "cluster mode,
but this thing failed" — the type does not distinguish them.

### The six decisions, none of them observable

Interleaved with that I/O are the decisions that determine what cluster this process joins:

1. **Node id** (`:181-185`) — `config.cluster.node_id` when non-zero, else
   `hash_addr_to_node_id(bus_addr)`.
2. **Initial member set** (`:360-372`, `:376-383`) — every parseable `initial_nodes` entry keyed by
   the *hash* of its address, then `entry(node_id).or_insert(self)`.
3. **Bootstrap election** (`:386`) — `initial_members.keys().next().copied() == Some(node_id)`,
   i.e. lowest id wins, restart-safe because the map is a `BTreeMap`.
4. **Peer seeding + the skip guard** (`:459-486`) — each peer gets a *guessed* client address
   (`bus_port.saturating_sub(10000)`) but only if `cluster.get_node(peer) .is_none()`. The comment
   there is exactly right about why the guard matters ("overwriting it with the guess would point
   this node's data path at a port nobody listens on"), and that correctness argument is enforced
   by no test in this file.
5. **Self address resolution** (`:492-517`) — `effective_client_addr` with the actual bound port
   substituted when `server.port == 0`; the bus address likewise from the actual listener.
6. **Slot auto-assign** (`:530-546`) — `should_bootstrap && !initial_members.is_empty() &&
   !cluster.all_slots_assigned()`.

Every one is a pure function of values already in hand. Every one is currently written as a
statement in the middle of 733 lines of `await`. The forcing evidence for the decisions today is
indirect: `simulation.rs:4862` and `sim_helpers.rs:18/:349/:390` document that the whole turmoil
cluster suite depends on decisions 1 and 4 (`sim_helpers.rs:18`: "`SERVER_PORT + 10000` so
`cluster_init`'s client-port derivation" resolves correctly) — a full simulated cluster is the
cheapest thing that can currently disagree with a wrong constant.

### Duplication, dead conditions, noise

Consequences of "decided inline", all of which the planner removes as a side effect:

* **`even_slot_ranges` is called twice** over the same input — `:532` (seed local state) and `:616`
  (replicate through Raft, inside a spawn). The two must agree; nothing makes them.
* **The self `NodeInfo` is built twice** in two shapes — as a *guessed* peer at `:459-486` if the
  node ever appears in its own `initial_nodes`, and correctly at `:518-525`.
* **`!initial_members.is_empty()`** (`:530`, `:606`) is dead: `:379-383` unconditionally inserts
  this node, and `should_bootstrap` already implies a non-empty map.
* **The bus-port branch** at `:503-513` reduces to `actual.port()` — both arms of
  `if actual.port() != configured.port() { actual.port() } else { configured.port() }` yield the
  same value.
* **Two 30 × 500 ms retry loops** with the same drop-the-redirect policy (`:556-601` self-register,
  `:606-658` slot replication), written out twice.

### 45% of the file is tests for two types that are not the boot

`:1056-1938` tests `RoleChangeConsumer` and `SplitBrainLogger` and nothing else — promotion retry,
demotion dedupe, ordering, header field mapping, the audit lifecycle, telemetry-follows-outcome.
Six FM tags live there: `FM-REPLICATION-024` at `:1129`, `:1169`, `:1206`, `:1241`, `:1564`, and
`FM-REPLICATION-048` at `:1763`. That test module is good — it is the reason the split-brain
policy is trustworthy. It is also the reason nobody notices the boot has none: the file *looks*
well tested.

## Proposed change

Keep `cluster_init.rs` as the module root (Rust 2018 permits `cluster_init.rs` alongside a
`cluster_init/` directory), and add two children.

### 1. `cluster_init/boot_plan.rs` — the planner (this is the depth)

```rust
/// Everything a cluster boot must decide, expressed as values.
pub(super) struct ClusterBootInput<'a> {
    pub configured_node_id: u64,          // config.cluster.node_id (0 = derive)
    pub self_bus_addr: SocketAddr,        // resolved: configured ip + actual bound port
    pub self_client_addr: SocketAddr,     // resolved: effective_client_addr + actual bound port
    pub initial_nodes: &'a [String],
    pub replica_priority: u16,
    pub known_node_ids: BTreeSet<u64>,    // from the restored ClusterState
    pub all_slots_assigned: bool,
}

pub(super) struct ClusterBootPlan {
    pub node_id: u64,
    pub initial_members: BTreeMap<u64, BasicNode>,   // -> raft.initialize
    pub peer_registrations: Vec<(u64, SocketAddr)>,  // -> network_factory.register_node
    pub peer_seeds: Vec<NodeInfo>,                   // already filtered by the skip guard
    pub this_node: NodeInfo,
    pub should_bootstrap: bool,
    pub slot_assignments: Option<Vec<(u64, SlotRange)>>, // Some => assign locally AND replicate
}

pub(super) fn plan_cluster_boot(input: ClusterBootInput<'_>) -> ClusterBootPlan;
```

The two address resolutions (`:492-517`) stay *outside* the planner and are passed in already
resolved — they are the only decisions that need `listener.local_addr()`, and keeping the planner
free of `Result` and of `Config` is worth the two lines at the call site.

`slot_assignments` being `Option` collapses the two `even_slot_ranges` calls and both dead
`!initial_members.is_empty()` guards into one decision computed once: `None` means "not the
bootstrap node, or slots already assigned"; `Some(v)` is consumed twice — once locally at the
`:530-546` site, once by the replication spawn at `:606-658` — with the identical vector rather
than a recomputation that is merely expected to match.

`peer_seeds` applies the skip guard (`known_node_ids`) inside the planner, so "a restored peer's
agreed address is never overwritten by the guess" becomes an assertion in a unit test instead of a
paragraph of comment.

### 2. `cluster_init/split_brain.rs` — the policy (this is locality)

`SplitBrainLogger`, `split_brain_header`, `RoleChangeConsumer` and the whole `#[cfg(test)] mod
tests` move **verbatim**, tags and test names untouched, visibility widened to `pub(super)`. Zero
logic edits: the point of doing it in the same change is that `git log --follow` on the boot is
never again 45% split-brain tests, and the point of doing it *verbatim* is that the diff is
reviewable as a move.

### 3. `cluster_init.rs` becomes the orchestrator

```rust
pub(super) async fn init_cluster(/* unchanged 18 params */) -> Result<ClusterInitResult> {
    let role = build_role_stack(/* 8 params */).await?;           // phase A, unconditional
    let cluster = if config.cluster.enabled {
        Some(bootstrap_cluster_raft(&role, /* ... */).await?)     // phase B
    } else { None };
    let detector = cluster.as_ref().map(spawn_failure_detection); // phase C
    Ok(ClusterInitResult { /* assembled */ })
}
```

`bootstrap_cluster_raft` is itself the thin runner over the plan: resolve addresses → build the
network factory and its connect factory → `plan_cluster_boot` → apply the plan (register, seed,
initialize) → spawn. The `enable_*` wiring, the four spawns and the coordinator construction stay
in phase B in their current order.

### Ordering constraints the split must preserve — boot order is behavior

These are the constraints I verified while tracing; they are the review checklist for the change,
and any one of them broken is a real bug the type system will not catch:

1. `attach_snapshot_store` (`:208`) **before** peer seeding (`:459-486`) — the skip guard reads the
   restored state. The planner takes `known_node_ids` as an *input*, which makes the dependency
   explicit at the call site instead of implicit in statement order.
2. All three `enable_*` calls (`:216` role change, `:237` migration complete, `:240` slot handoff)
   and `self_role_reconciler()` (`:421`) **before** the state machine moves into `Raft::new`
   (`:424`). After the move they are unreachable — this is why `:421` exists as a separate line
   today.
3. `reconcile_self_role()` (`:225`) may fire an event before the consumer is spawned (`:660`); the
   channel is unbounded, so the event waits. The split must not turn that channel into a bounded
   one or move the emit after the spawn (both would be behavior changes).
4. `network_factory.register_node` for every peer **before** `Raft::new` — openraft resolves peers
   through the factory on first append.
5. Phase A **before** the role-change consumer spawn — the consumer holds `role_controller`.
6. Phase C last: the detector reads the live flags and the assembled state.

`build_role_stack` / `bootstrap_cluster_raft` / `spawn_failure_detection` are called in that order
from one place, so the phase boundary is the constraint's documentation.

### What does not change

* **`init_cluster`'s signature and `ClusterInitResult`.** Deliberate: `server/mod.rs` gets an empty
  diff, which is what keeps this proposal orthogonal to siblings 63 and 64 (below). Slimming the
  18 parameters is a *different* change that belongs to whoever reshapes the caller.
* **The module path `crates/server/src/server/cluster_init.rs` still exists**, so
  `website/docs-spec/specs/operations/clustering.md:37/:73` and its S7 code-path drift guard
  (`:169`) stay true without a docs edit. (Note: no S7 checker is implemented in `scripts/` today —
  the guard is aspirational — but the cite is kept honest regardless.)
* **No spawn is added, removed, reordered or re-timed.** No retry constant, no interval, no
  channel bound.

### Deletion test, honestly

* `plan_cluster_boot` — **passes.** Delete it and the six decisions go back to being statements
  between `await`s; the caller is not shorter by much, but the decisions become unreachable from a
  unit test again, the two `even_slot_ranges` calls diverge again, and the skip guard goes back to
  being a comment. The interface (one input struct, one plan struct) is smaller than the behavior
  it hides, and its leverage grows with every future boot decision — a real one is already queued:
  hotfix 3 below is a decision the planner can simply refuse to make.
* `split_brain.rs` — **fails.** Delete the module boundary and nothing breaks; the code moves back
  into one file. This is a locality change and I am not dressing it as depth. It is worth doing
  *with* the split because the 883 test lines are the thing that makes the current file
  unreadable, and because moving them costs one commit and zero risk.
* `build_role_stack` / `bootstrap_cluster_raft` / `spawn_failure_detection` — **fails as
  interfaces, passes as an ordering statement.** Each has exactly one caller and will never have
  two; they are not seams. What they buy is that constraint 1-6 above becomes a three-line
  function body instead of a 733-line reading exercise. Named honestly in the commit message as
  such.

## Testability improvement

Today: **zero** tests reach any boot decision. The file's 883 test lines reach the consumer and the
logger only. The cheapest existing thing that disagrees with a wrong boot decision is a turmoil
cluster (`simulation.rs`) or the multi-node harness (`cluster_harness.rs`).

After: `plan_cluster_boot` is `fn(values) -> values` with no `async`, no `Result`, no I/O. The
tests that become writable — each a handful of lines, each currently impossible:

| what it forces | today's cheapest witness |
|---|---|
| configured `node_id` wins; `0` derives from the bus address | boot a node, read `CLUSTER MYID` |
| self appearing in its own `initial_nodes` yields **one** member, not two (hotfix 3) | nothing — undetected |
| lowest id bootstraps; the same input set elects the same node on every restart | 3-node turmoil run |
| a peer already in restored state is **not** re-seeded with the guessed port | restart-a-replica integration test |
| the guessed client port is `bus - 10000` and saturates rather than wraps for `bus < 10000` | nothing |
| `port == 0` resolves to the bound port in the advertised client address | port-0 harness boot |
| slot ranges are computed once and are identical for the local seed and the Raft replication | nothing — two calls, no cross-check |
| no slot plan when slots are already assigned, or when not bootstrapping | 3-node turmoil run |

`even_slot_ranges` itself is already forced in the cluster crate (`types.rs:1290-1312`,
`FM-CLUSTER-007`); what is unforced is *who calls it with what*, which is exactly what the plan
makes assertable.

The planner deliberately stays in `frogdb-server`, which has **no mutation gate**. I am not
claiming gate credit for it. The alternative — pushing it down into `frogdb-cluster` (0.80) per the
ADR-0004 pattern of "put the forcing test in the mutated crate" — would drag server-side config
resolution (`effective_client_addr`, the port-0 substitution, `ServerConfig`) into a LOCKED crate
to buy score on a decision that is server-shaped. Not worth it. If a later round wants the credit,
the planner is by then a pure function with tests attached and moving it is mechanical.

## Spec / LOCKED impact

* **`cluster-failure-modes.md` does not cite `cluster_init.rs` anywhere** (grepped; the lane brief's
  assumption that it does is wrong). FM-CLUSTER-091's Invariant names
  `server/src/slot_migration/mod.rs`, which this proposal does not touch.
* **One spec line names the file**: FM-REPLICATION-048 Invariant `:1022` — "`SplitBrainLogger::log`
  (`cluster_init.rs`) matches on `frogdb_replication::split_brain_log::write_log(...)`". Invariant
  prose is **never parsed** by `scripts/failure-modes.py`, so the lint stays green either way; the
  cite would simply become false. Refresh it to `cluster_init/split_brain.rs` in the same commit.
  That is a text edit inside a `Status: LOCKED` spec with **no** change to Trigger, Observable, NOT
  observable, Outcome variant, Forced by, or the row's semantics — flag it in review as a cite
  refresh, not a spec change. No other row moves.
* **All six FM tags move with their tests.** `scripts/failure-modes.py` binds backticked `Forced by`
  names to `// FM-` tags by **name**, scanning `SOURCE_ROOTS = [frogdb-server/crates]` — it is
  path-agnostic within that root. `FM-REPLICATION-024`'s five tags (`:1129/:1169/:1206/:1241/:1564`)
  and `FM-REPLICATION-048`'s one (`:1763`) land in `cluster_init/split_brain.rs` with the same test
  names (`demotion_fires_when_split_brain_log_disabled`,
  `demotion_identical_whether_or_not_log_enabled`, `split_brain_header_maps_record_and_event_fields`,
  `split_brain_header_unknown_new_primary`,
  `split_brain_lifecycle_captures_audit_and_initiates_discard`,
  `split_brain_telemetry_follows_the_log_write_outcome`). `just lint-failure-modes` passes with **no
  spec `Forced by` edits**. Renaming any of those tests during the move would break the lint — the
  move is verbatim for this reason.
* **No LOCKED crate is edited.** `frogdb-cluster`, `frogdb-cluster-runtime`,
  `frogdb-replication`, `frogdb-replication-runtime` are read-only here. `cargo mutants -p
  <locked crate>` runs that crate's own tests and is unaffected; the new planner tests live in
  `frogdb-server` (ungated) as noted.
* **Seam lints**: no clock reads, metrics emissions, redirect replies or durable-ack writes are
  added or moved out of their chokepoints — the split-brain telemetry arms (`:860`, `:869`,
  `:875-879`) move as a unit with the function that owns them. `just lint-gates` unaffected.
* One in-tree doc cite goes stale and is refreshed with the move:
  `role_manager.rs:25` names `server::cluster_init::RoleChangeConsumer`.

## Risks / scope boundaries vs siblings

**Proposal 59 (cluster-event-router) — real contact, ordered.** 59 preserves the
`enable_role_change_detection` / `enable_migration_complete_notification` /
`enable_slot_handoff_notification` signatures precisely so its `cluster_init.rs` diff is empty; its
sole caller cite for `enable_slot_handoff_notification` is `cluster_init.rs:240`. Under this
proposal those three call sites (`:216`, `:237`, `:240`) **move verbatim into
`bootstrap_cluster_raft` in `cluster_init.rs`**, unchanged in form, arguments and relative order,
and still before the `Raft::new` move (constraint 2). **Recommended order: 59 lands first** — 59
keeps its empty diff, and 65 then relocates already-final call sites once. Reversed, 59 becomes a
three-line rebase; either way there is no semantic conflict, only churn.

**Proposal 62 (handoff-finalizer-move) — no contact. Correcting a stale claim.** Proposal 59
asserts that 62 "will rewire the finalizer's construction in `cluster_init.rs`". Read at HEAD, 62
does not: it edits `server/src/slot_migration/mod.rs` plus new modules in `frogdb-cluster` /
`frogdb-cluster-runtime`, and explicitly leaves `spawn_event_dispatcher` and `spawn_handoff_barrier`
alone. **62's `cluster_init.rs` diff is empty.** The `SlotMigrationCoordinator::new` +
`spawn_event_dispatcher` + `spawn_handoff_barrier` block (`:735-746`) moves as-is under 65 and 62
never sees it. Either order; no constraint.

**Proposal 58 (auto-failover-propose-retry) — citation-only.** 58 cites `cluster_init.rs:556-601`
and `:606-658` (the two drop-the-redirect retry spawns) as census evidence and does not touch them.
After 65 those lines live in `cluster_init.rs`'s `bootstrap_cluster_raft` at new offsets. **Land 58
before 65**, or accept a line-number refresh in 58's census table.

**Proposals 63 (`server/mod.rs` + `init.rs`) and 64 (`subsystems.rs`) — the SV trio.** My Files set
is exactly: `cluster_init.rs`, two new files beneath it, one spec prose line, one doc comment in
`role_manager.rs`. **`server/mod.rs` and `subsystems.rs` are not in it** — preserving
`init_cluster`'s signature and result type is what makes that true, and it is a deliberate cost
(the 18 parameters survive this round). **Recommended trio order: 63 → 65 → 64** — 63 stabilises
the call site, 65 gives the callee its phase shape, and 64's lifecycle trait then has three named
phases to wrap instead of one 733-line function. Because all three diffs are disjoint by
construction, any order merges; the recommendation is about each step consuming the previous one's
boundary, not about conflicts.

**The risk that matters.** This is a boot-ordering refactor in a file with no boot tests. Nothing
in the split is caught by the compiler if constraints 1-6 are violated — `attach_snapshot_store`
after the seed loop compiles fine and silently overwrites agreed peer addresses with guesses. The
mitigations are: (a) the planner's `known_node_ids` input makes constraint 1 a *data* dependency
rather than a statement-order one; (b) the split is verbatim-move plus extraction, no rewrites; (c)
the turmoil cluster suite and `cluster_harness` multi-node tests are the acceptance gate — a
reordering that breaks bootstrap, peer address convergence or slot assignment fails them. The
change should not be reviewed as a formatting diff.

## Effort

**M.** Roughly: planner extraction + its unit tests (the real work, ~1 day), verbatim move of ~250
policy lines and ~883 test lines (mechanical), three phase functions (mechanical), plus one spec
prose line and one doc comment. No behavior change, no new dependency, no LOCKED crate edited, no
generated artifact regenerated. Reviewable in three commits: (1) move split-brain out verbatim,
(2) extract the planner + tests, (3) phase functions.

## Independently-landable hotfixes

Found while verifying; none is folded into the refactor.

**1 — the `node-id` documentation is false, and it is published.** `config/src/cluster.rs:23-27`
documents `node_id` as "This node's unique ID (0 = auto-generate from timestamp)". The live boot
does no such thing: `cluster_init.rs:181-185` falls back to
`hash_addr_to_node_id(cluster_bus_socket_addr())` (`util.rs:33-37`), a `DefaultHasher` over the
address — deterministic across restarts, which is exactly what bootstrap-by-lowest-id needs, and
the opposite of a timestamp. The timestamp implementation (`effective_node_id`, `:231-243`,
`(timestamp << 16) | random`) is real but unreachable (see hotfix 2). The doc string is generated
into `website/src/data/config-reference.json:145-148` and shipped. Fix: correct the doc comment to
state the address derivation and regenerate. One-line change plus `just docs-gen`.

**2 — a dead config-conversion path.** `ClusterConfigExt::to_core_config`
(`server/src/config/mod.rs:70-91`) is the **only** caller of `effective_node_id()` and itself has
**zero** callers; it builds `frogdb_core::ClusterConfig` (`cluster/src/types.rs:541-556` + `Default`
`:558`), which has no live consumers either. Deleting the three together removes the
timestamp-vs-hash contradiction at its root and shrinks a LOCKED crate's mutant surface. Verify
with `just check` that nothing behind a non-default feature reaches them.

**3 — latent: a phantom Raft voter when `node-id` and self-in-`initial-nodes` disagree.**
`cluster_init.rs:181-185` may take the id from config while `:360-372` derives every
`initial_nodes` entry's id by hashing its address, and `:376-383` then inserts the *configured* id
for this node's own address. An operator who sets `cluster.node-id = N` **and** lists their own bus
address in `initial-nodes` puts two ids for one process into `initial_members`, and the bootstrap
node feeds that map to `raft.initialize` — a voter that no process answers for, against a quorum
sized as if it did. `cluster/src/network.rs:933-945` (`handle_rpc_request`) dispatches
`AppendEntries`/`Vote`/`InstallSnapshot` into the local raft with **no target-node-id check**, so
the local node answers for both ids on the same socket; whether that masks the problem or produces
a split identity depends on openraft's bookkeeping, and I did not trace it to a conclusion.

**Reachability — why this is latent, not live.** Nothing that ships sets `node-id`: the Helm
generator's `ClusterConfig` (`ops/helm/helm-gen/src/main.rs:364-372`) has no `node_id` field and
never emits one (`:493-500`); the operator's `config_gen.rs` mentions neither `node_id` nor
`initial_nodes`; both shipped example configs ship `initial-nodes = []`
(`website/src/data/example-config.toml:148`, `ops/deploy/deb/frogdb.toml:147`). The test harness
*does* set an explicit id — `ClusterTestNode::generate_node_id_from_port`
(`test-harness/src/cluster_harness.rs:154-160`) — but computes it as `DefaultHasher` over
`127.0.0.1:<cluster_port>`, i.e. **exactly** `hash_addr_to_node_id` of the bus address, so
configured and derived ids coincide and the collision cannot occur. The precondition is therefore:
a hand-written config that sets `node-id` to something other than the hash of its own bus address
*and* lists its own bus address in `initial-nodes` — which
`website/docs-spec/specs/operations/clustering.md:132-133` invites by telling operators to set
`node-id` alongside `initial-nodes`.

**Fix shape** (naturally part of the planner, which is why it is worth filing now): make
`plan_cluster_boot` recognise this node's own bus address in `initial_nodes` and key it by the
*effective* node id rather than the hash, so one process contributes exactly one member — and
assert it in a unit test. Filing as a follow-up issue rather than folding it in, because a
membership-shape change under `raft.initialize` deserves its own review even when it is a
one-liner.
