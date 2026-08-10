# Proposal 59 — `ClusterEventRouter`: the metadata plane's outbound edge as a module

Round 38 · lane: replication+cluster · candidate **RC7** · effort **S/M** · LOCKED area
(cluster, mutation gate 0.80) · **step 2 is SPEC-FIRST**

## Summary

`ClusterStateMachine::apply` (`cluster/src/state.rs:914-1040`) is an openraft trait
**implementation** that inlines an 83-line event fan-out (`:942-1024`). That fan-out is the
metadata plane's *entire* outbound edge to the data path: it decides which applied
`ClusterEvent`s concern this node, translates each into a different channel-facing event type,
and delivers it onto one of three `Option<UnboundedSender<_>>` fields. It has no **interface**
— no name, no type, no test surface — so the four fields it depends on are hand-copied at
three constructor sites and the translation itself is reachable only by driving `apply` with
openraft `Entry` values.

The candidate as filed said "apply() inlines 83-line event fan-out, 3 Option senders +
self_node_id repeated at 3 ctor sites incl. `get_snapshot_builder` must-remember comment …
FM-CLUSTER-037/038 routing unchanged". Verified against the current tree (`9a62f79b`):

- **The structural claims all hold**, and the line citations in the lane file are stale by
  ~35-45 lines (they predate `1c70cf48`/`34de2424`, which added the invariant catalog). Every
  citation below is re-verified.
- **The claim is materially understated in one place.** The three channels are not symmetric
  copies of one rule — they implement *three different* delivery policies (self-filtered,
  broadcast, broadcast-with-consumer-side-filter), and one of the three, the slot-handoff pair
  (`:990-1019`, 30 of the 83 lines), is **forced by no test anywhere in the workspace**. It is
  the seam between FM-CLUSTER-087 (which forces the *emission*, in `commands.rs`) and
  FM-CLUSTER-090 (which forces the *consumption*, in `cluster-runtime`); each locked crate
  tests its own half and the adapter between them is tested by neither.
- **FM-CLUSTER-037/038 are the wrong rows.** -037 is the commit-to-apply window (routing is
  `route_with_snapshot`, an unrelated function) and -038 is the *consumer*
  (`cluster-runtime/src/migration_events.rs`). The rows this proposal actually touches are
  **FM-CLUSTER-043, -044, -045, -046** (role-event routing and reconciliation) and, for the
  new pins, **-034** and **-087**.
- **One defect was found**, in the identity the self-filter compares against. It is labelled
  **latent with a bounded reachability argument** below — not "live" — and the honest fix is
  spec-first, so it is not folded into the extraction.

## Files involved (verified at `9a62f79b`)

All paths under `frogdb-server/crates/` unless noted.

| path | lines | role in this proposal |
|---|---:|---|
| `cluster/src/state.rs` | 4402 | **the change.** `ClusterState.self_node_id: Arc<AtomicU64>` `:26-28`; `self_node_id()` `:210-213`, `set_self_node_id()` `:216-218`; `apply_local` `:338-342`; the four channel-facing event types `DemotionEvent` `:518-527`, `PromotionEvent` `:529-536`, `RoleChangeEvent` `:538-552`, `SlotMigrationCompleteEvent` `:554-560`, `SlotHandoffEvent` `:562-595`; **`ClusterStateMachine` fields `:598-611`**; ctor `new` `:615-624`; ctor `with_state` `:627-636`; `enable_role_change_detection` `:700-708`; `enable_migration_complete_notification` `:714-720`; `enable_slot_handoff_notification` `:729-735`; `self_role` `:744-747`; `reconcile_self_role` `:764-769`; `self_role_reconciler` `:776-786`; `emit_self_role_change` `:791-796`; `SelfRoleReconciler` `:824-829` + impl `:845-890`; **`apply` `:914-1040`, fan-out `:942-1024`**; ctor `get_snapshot_builder` `:1042-1054`; `install_snapshot` role diff `:1086`, `:1093-1096`; tests `:1166-4402` |
| `cluster/src/types.rs` | 1313 | `ClusterEvent` `:477-537` — the five node-agnostic variants the router consumes. **Not changed by this proposal**; shared read-only with proposal 60 |
| `cluster/src/commands.rs` | 2101 | `apply_command` `:94-…` — the producer. `release_events` `:18-32`; `ResetCluster` arm `:816-862`. **Not changed**; cited as the FM-CLUSTER-087 half that *is* tested (`abort_releases_the_barrier_and_keeps_the_migration` `:1578`, `cancel_releases_a_prepared_handoff` `:1610`, `force_failover_releases_the_handoffs_it_prunes` `:1643`, `reset_releases_prepared_handoffs` `:1675`) |
| `cluster/src/lib.rs` | 95 | `pub use state::{…}` `:71-74` — the crate's public event surface. Gains the router type; `ClusterEvent` stays crate-internal |
| `cluster/src/writer.rs` | 660 | `propose_reset` `:219-254`, `set_self_node_id(new_id)` `:250` — the HARD-reset re-key that the state machine's cached identity does not follow. **Not changed** by step 1 |
| `cluster-runtime/src/handoff_barrier.rs` | 465 | the *precedent*: `HandoffAction` `:71-91`, **`plan_handoff_action` `:96-120`** (pure planner), `run_slot_handoff_barrier` `:175-…`, and `cluster_state.self_node_id()` `:190` — the consumer that reads the **live** identity. Also the downstream half of the untested seam. **Not changed** |
| `cluster-runtime/src/migration_events.rs` | 251 | the other precedent (`plan_migration_notice` + delivery, FM-CLUSTER-038). **Not changed**; lane counter-example, deliberately untouched |
| `server/src/server/cluster_init.rs` | 1938 | the single production wiring site: `cluster.set_self_node_id(node_id)` `:200`, `with_state` `:202`, `enable_role_change_detection` `:216`, `reconcile_self_role` `:226`, `enable_migration_complete_notification` `:237`, `enable_slot_handoff_notification` `:240`, `self_role_reconciler` `:421`, reconcile ticker `:704-727`. **Conflict edge with proposals 62 and 60** |
| `server/src/commands/cluster/admin.rs` | — | `cluster_reset` `:390-429` — HARD mints `rand::random::<u64>()` as `new_node_id` (`:411-419`). Evidence for the reachability argument only |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1500+ | FM-CLUSTER-034 `:566`, -043 `:685`, -044 `:697`, -045 `:709`, -046 `:721`, -087 `:1237`, -090 `:1274`; the mis-cited -037 `:603`, -038 `:615` |
| `.cargo/mutants.toml` | 45 | gate config; no exclusion covers `state.rs` |

## Problem (verified evidence)

### 1. Three delivery policies, one undifferentiated block

The fan-out is a single `for event in events { match event { … } }` spanning `:942-1024`.
Decomposed by policy, verified arm by arm:

| arm | lines | policy | translation |
|---|---|---|---|
| `NodeDemoted` | `:947-959` | **self-filtered** — guard `if Some(demoted_node_id) == self.self_node_id` | → `RoleChangeEvent::Demoted(DemotionEvent{..})` on `role_change_tx` |
| `NodePromoted` | `:960-970` | **self-filtered**, same guard on `promoted_node_id` | → `RoleChangeEvent::Promoted(PromotionEvent{..})` on the *same* channel |
| `SlotMigrationCompleted` | `:971-984` | **broadcast** — no guard, comment `// Migration-complete fires on ALL nodes (no self-filter).` | → `SlotMigrationCompleteEvent` on `migration_complete_tx` |
| `SlotHandoffPrepared` | `:985-1006` | **broadcast, consumer-side filter** — comment `:985-989` explains the "am I the source?" test lives in `cluster-runtime` | → `SlotHandoffEvent::Prepared` on `slot_handoff_tx` |
| `SlotHandoffReleased` | `:1007-1019` | same | → `SlotHandoffEvent::Released` |
| catch-all | `:1020-1022` | role change naming another node: drop | — |

Those are three genuinely different rules, and which rule applies to which event is stated
only by adjacency and comment. Nothing names the policy, so nothing can assert it. A sixth
`ClusterEvent` variant added tomorrow gets whichever policy its author's nearest neighbour
happened to use, and the compiler's only help is match exhaustiveness — which the catch-all at
`:1021-1022` already partially defeats for the role-event family.

### 2. The slot-handoff arms are forced by nothing

`enable_slot_handoff_notification` (`state.rs:729`) has **exactly one caller in the workspace**:
`cluster_init.rs:240`. No test anywhere constructs a `ClusterStateMachine`, enables that
channel, and applies a `PrepareSlotHandoff`. Verified by exhaustive grep of the three
`enable_*` methods: 24 test call sites for `enable_role_change_detection`, one for
`enable_migration_complete_notification` (`state.rs:1722`, `test_migration_complete_event_fires`,
FM-CLUSTER-034), **zero** for `enable_slot_handoff_notification`.

The spec makes the gap legible rather than hiding it — both halves are specced, and the
sentence that bridges them is prose in an Invariant cell, which `scripts/failure-modes.py`
never parses:

- **FM-CLUSTER-087** (`:1237`) forces *emission*: its four `Forced by` tests all live in
  `commands.rs` (`:1578`, `:1610`, `:1643`, `:1675`) and assert the `Vec<ClusterEvent>`
  returned by `apply_command`. Its Invariant cell then says *"The events are emitted from
  `apply`, so they reach every node's dispatcher"* — an assertion about the 30 lines at
  `state.rs:990-1019` that no test in its `Forced by` list executes.
- **FM-CLUSTER-090** (`:1274`) forces *consumption*: its four tests live in
  `cluster-runtime/src/handoff_barrier.rs` and drive `plan_handoff_action` /
  `run_slot_handoff_barrier` from `SlotHandoffEvent` values the tests construct by hand
  (`:249-266`).

So the workspace proves "`apply_command` emits a `ClusterEvent::SlotHandoffPrepared`" and
proves "a `SlotHandoffEvent::Prepared` arms the barrier", and proves nothing about the
translation joining them. The two are separate types with separate field sets, translated by
hand at `:998-1005` and `:1013-1017`. Field-name shorthand makes a *swap* impossible today,
but the arm being deleted, the guard being inverted, or `seq` being dropped are all changes
that compile and that no test in either locked crate observes.

Because the whole fan-out is *inside* `apply`, `cargo mutants` sees it as part of one target
(`apply`'s body), killed trivially by any of the many tests that apply an entry. The 0.80 gate
therefore scores this region as covered. **The gate is not wrong; the shape of the code is
hiding a blind spot from it.**

### 3. Four fields, three constructors, one "must remember" comment

`ClusterStateMachine` (`:598-611`) has six fields, four of which exist solely to serve the
fan-out: `self_node_id`, `role_change_tx`, `migration_complete_tx`, `slot_handoff_tx`. All six
are written out longhand at three sites:

| site | lines | what it does with the four |
|---|---|---|
| `new()` | `:615-624` | all `None` |
| `with_state(state)` | `:627-636` | all `None` |
| `get_snapshot_builder()` | `:1042-1054` | all `None` **plus** a three-line comment (`:1049-1051`) explaining that the *fifth* field, `snapshot_store`, must **not** be nulled |

That comment is the tell. The builder's correctness rule is "null the outbound edges, keep the
durable one", and it is expressible only as a comment because the outbound edges are four
loose fields rather than one thing with a name. `get_snapshot_builder` returns `Self`, so the
list must be re-typed in full every time a field is added — the same must-remember shape ADR
0004 records as the cost of post-construction installers, and the shape proposal 61 addresses
on the replication side.

### 4. The identity the self-filter uses is a boot-time copy, and it is the wrong one

There are **two** homes for "which node am I":

- `ClusterState.self_node_id: Arc<AtomicU64>` (`:26-28`), whose doc-comment states the
  contract explicitly: *"Shared across all connections so that HARD reset (which generates a
  new node ID) is visible immediately."* Read via `ClusterState::self_node_id()` (`:210-213`).
- `ClusterStateMachine.self_node_id: Option<NodeId>` (`:600-601`), a plain `u64` copy taken
  once at `enable_role_change_detection` (`:705`) and never updated.

`SelfRoleReconciler` (`:824-829`) copies the *second* one again, at `:779`, into a handle that
`cluster_init.rs:421` captures for the life of the process.

`ClusterWriter::propose_reset` (`writer.rs:219-254`) updates the **first** on a HARD reset
(`:250`) and cannot update the second — the state machine has been moved into
`openraft::Raft::new` by then. `CLUSTER RESET HARD` mints `new_node_id` as
`rand::random::<u64>()` (`admin.rs:411-419`), and the `ResetCluster` arm re-keys this node's
record to it (`commands.rs:839-849`).

After a HARD reset, therefore, for the remaining life of the process:

1. `apply`'s role guards (`:951`, `:963`) compare against the **pre-reset** id, so a
   `NodeDemoted{new_id}` or `NodePromoted{new_id}` is silently dropped at `:1021-1022`.
2. `self_role()` (`:744-747`) looks up the pre-reset id, which the reset removed from
   `inner.nodes` — so it returns `None`. `install_snapshot`'s before/after diff (`:1086`,
   `:1093`) compares `None` to `None` and synthesizes nothing (FM-CLUSTER-045 disabled).
3. `SelfRoleReconciler::reconcile` (`:852-864`) hits the `self_role()` → `None` early return
   and reports `Agreed` forever, so the periodic re-drive at `cluster_init.rs:704-727` is
   silent (FM-CLUSTER-046 disabled).

The three mechanisms the spec relies on to keep the data-path role converged with the
replicated role are all keyed off the same stale copy, so they fail together and silently.
Note the asymmetry that makes this a genuine inconsistency rather than a design choice: the
*handoff* consumer already reads the live identity (`handoff_barrier.rs:190`
`cluster_state.self_node_id()`), because it was written later and reached for the shared cell.
Two consumers of the same question, two answers.

**Labelled latent, not live**, and the reason is stated so a reviewer can disagree with the
label rather than with a hidden assumption: reaching the *consequence* (a role change lost)
requires the re-keyed node to be re-admitted to a cluster and then demoted, without a restart
in between — and `CLUSTER RESET HARD` changes only the cluster-state identity, not openraft's
node id, which is fixed at `Raft::new` from config or `hash_addr_to_node_id`
(`cluster_init.rs:181-185`, `:425`). A node cannot currently rejoin Raft under its new random
id, so the full path does not close on today's tree. What *is* reachable with certainty is the
silent disarming of FM-CLUSTER-045 and -046 for that process — the safety nets stop reporting.
This is the same *class* as
[`.scratch/cluster-correctness/issues/open/20-…`](../../cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md)
(a node's data path keeps a role the cluster has moved on from) reached by a different route,
and is **not** that issue.

### 5. Why this is shallow (architecture vocabulary)

- **The producer is deep; the consumers are deep; the middle is not.** `apply_command`
  (`commands.rs:94`) is a real **module**: `release_events` (`:18-32`) makes "a prepared
  handoff never disappears without a release" structural, and events are pushed only on `Ok`
  paths so emit-on-failure is impossible (FM-CLUSTER-034's Invariant). `plan_handoff_action`
  (`handoff_barrier.rs:96-120`) and `plan_migration_notice` are pure planners the spec
  explicitly praises. Between two deep layers sits an 83-line block with no name.
- **The house pattern already exists and this seam does not follow it.** Both downstream
  consumers are *pure planner + thin delivery*, and both Invariant cells (FM-CLUSTER-090,
  FM-CLUSTER-038) cite the planner **by name** as the reason the behaviour is legible. The
  fan-out is the one link in the chain that is neither.
- **The seam that was never cut.** Between "`apply_command` produced node-agnostic events" and
  "three channels with three consumers" sits a rule nobody owns: *decide whose event this is,
  translate it into the consumer's vocabulary, deliver it in apply order, and treat a closed
  channel as shutdown rather than an error.* That last part is five separate `let _ =
  tx.send(…)` (`:953`, `:965`, `:978`, `:998`, `:1013`) — five independent decisions to
  discard a send failure, correct in all five cases and stated in none.
- **`ClusterEventRouter` would be deep, and passes the deletion test.** Delete it and: 83
  lines reappear inside an openraft trait method; four fields reappear at three constructor
  sites; the "null the edges, keep the store" rule goes back to being a comment; the three
  delivery policies go back to being adjacency; and the slot-handoff translation goes back to
  having no callable surface. The **leverage** is a two-line call standing in front of the
  self-filter, the per-variant channel selection, the broadcast-vs-filtered policy, the
  send-failure policy and the identity question. **Locality**: adding a sixth `ClusterEvent`
  variant becomes one edit in one file with an exhaustive match that has no catch-all, instead
  of an edit inside a 127-line trait method plus a review that has to notice three constructors.
- **A shallow version exists and must be rejected.** `fn route_events(&self, events:
  Vec<ClusterEvent>)` as a private method on `ClusterStateMachine` removes the nesting and
  nothing else: it still needs the state machine to exist, still cannot be called without
  openraft in scope, still leaves the four fields at three ctor sites, and still gives
  `cargo mutants` one target whose body-replacement is killed by the role tests while the
  handoff arms stay unobserved. The depth is in the **pure planner**, because the planner is
  what makes "which policy does this variant get" a value a test can compare.

## Proposed change

**One new module, `cluster/src/state/events.rs`**, with `state.rs` gaining a `mod events;`
line. It sits above the channels and below `apply`, mirroring `plan_handoff_action` /
`plan_migration_notice` one layer up the same path.

```rust
/// Where one applied [`ClusterEvent`] belongs, for a node with this identity.
///
/// Total and pure: no channels, no `self`, no allocation. The three delivery
/// policies the fan-out used to state by adjacency are variants here —
/// role changes are self-filtered, the other two are broadcast — so "which
/// policy does this variant get" is a value a test compares rather than a
/// property a reviewer infers from where the arm sits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventRoute {
    /// This node's own role changed. Self-filtered.
    RoleChange(RoleChangeEvent),
    /// A migration completed. Delivered on every node.
    MigrationComplete(SlotMigrationCompleteEvent),
    /// A handoff transitioned. Delivered on every node; the "am I the source?"
    /// filter is the consumer's (FM-CLUSTER-090).
    SlotHandoff(SlotHandoffEvent),
    /// A role change naming another node: nothing to deliver here.
    NotOurs,
}

pub(crate) fn plan_event_route(event: ClusterEvent, self_node_id: Option<NodeId>) -> EventRoute;

/// The metadata plane's outbound edge to the data path: the three channels the
/// runtime drains, plus the identity the role filter is written against.
pub(crate) struct ClusterEventRouter {
    self_node_id: Option<NodeId>,
    role_change_tx: Option<mpsc::UnboundedSender<RoleChangeEvent>>,
    migration_complete_tx: Option<mpsc::UnboundedSender<SlotMigrationCompleteEvent>>,
    slot_handoff_tx: Option<mpsc::UnboundedSender<SlotHandoffEvent>>,
}

impl ClusterEventRouter {
    /// A router wired to nothing. What `get_snapshot_builder` needs: a builder
    /// shares the live state but must never re-emit, and this says so by type
    /// instead of by four `None`s and a comment.
    pub(crate) fn silent() -> Self;

    /// Deliver, in apply order. A closed channel means the consumer is gone —
    /// this node is shutting down — and is not an error, which is now one
    /// statement rather than five `let _ =`.
    pub(crate) fn route(&self, events: Vec<ClusterEvent>);

    // the three `enable_*` methods move here, unchanged in behaviour
    pub(crate) fn enable_role_changes(&mut self, self_node_id: NodeId) -> mpsc::UnboundedReceiver<RoleChangeEvent>;
    pub(crate) fn enable_migration_complete(&mut self) -> mpsc::UnboundedReceiver<SlotMigrationCompleteEvent>;
    pub(crate) fn enable_slot_handoffs(&mut self) -> mpsc::UnboundedReceiver<SlotHandoffEvent>;
}
```

`ClusterStateMachine` loses four fields and gains one: `router: ClusterEventRouter`. Its three
`enable_*` methods (`:700`, `:714`, `:729`) stay as **public delegating one-liners** so
`cluster_init.rs:216/237/240` is unchanged — the crate's public surface does not move, which
keeps this proposal off proposal 62's wiring diff. `self_role`, `self_role_reconciler` and
`emit_self_role_change` read the identity through the router. `apply`'s fan-out collapses to:

```rust
let (response, events) = self.state.apply_command(cmd).unwrap_or_else(|e| { /* unchanged */ });
self.router.route(events);
results.push(response);
```

and `get_snapshot_builder` (`:1042-1054`) becomes `router: ClusterEventRouter::silent()`, with
the must-remember comment (`:1049-1051`) reduced to what it is actually about — why
`snapshot_store` is the one field that is *not* silenced.

**Step 1 is a pure move: `plan_event_route` reproduces the six arms exactly, including the
self-filter reading the same cached `Option<NodeId>` it reads today.** Problem §4 is
deliberately *not* fixed here.

### Step 2 (separate, SPEC-FIRST): the identity becomes the live one

Replace the router's `self_node_id: Option<NodeId>` with the `ClusterState` handle and read
`state.self_node_id()` per event, so the role filter follows a HARD reset exactly as
`handoff_barrier.rs:190` already does. `enable_role_changes(id)` additionally calls
`state.set_self_node_id(id)` so the two homes converge at the one call site and the 24 existing
role tests keep working unedited.

This is a behaviour change and goes **row → failing test → fix**:

- **FM-CLUSTER-043** — the Observable/Invariant cells define "this node" implicitly. Amend the
  Invariant to say the filter reads the **live** identity, and add an Observable clause: *a
  HARD reset re-points it, so a role change naming the node's new id is delivered and one
  naming its old id is not.*
- **FM-CLUSTER-046** — add the same clause to the reconciler: it reconciles against the live
  identity, so a re-keyed node is still reconciled rather than reporting `Agreed` forever.
- New forcing test in `frogdb-cluster`, e.g. `a_hard_reset_re_points_the_role_filter`, added to
  both rows' `Forced by`. Because `just lint-failure-modes` binds only backticked test names in
  `Forced by` cells, adding it there is what makes the lint enforce the new pin; the Invariant
  prose edits are a human-review item, exactly as proposal 53 records for FM-REPLICATION-001.

Step 2 is **not** a hotfix and must not be smuggled into step 1: it changes what a locked row
means, and §4's reachability argument says nothing is currently on fire.

### Acceptance criteria (LOCKED crate)

1. **Step 1 changes no behaviour, and the existing tests prove it unedited.** All 24
   `enable_role_change_detection` call sites in `state.rs` tests, `test_migration_complete_event_fires`
   (`:1719`), and the `install_snapshot` / reconciler families (`:3363-3567`) must pass with
   **zero edits**. If any of them appears in the step-1 diff, the move was not a move.
2. **Apply order is preserved and asserted.** `test_role_changes_preserve_apply_order`
   (`:3307`, FM-CLUSTER-044) is the pin; `route` iterates the `Vec` in order and the two role
   variants continue to share one channel.
3. **`enable_*` public signatures unchanged**, so `cluster_init.rs:216/237/240` is byte-identical
   in the step-1 diff. (See the conflict edge with proposal 62.)
4. **The new tests land with step 1, not after it** — they are the entire mutation-score case:

   | new test | pins | row |
   |---|---|---|
   | `a_prepared_handoff_routes_to_the_handoff_channel` | `ClusterEvent::SlotHandoffPrepared{slot,source,target,seq,barrier_ms}` → `SlotHandoffEvent::Prepared` with every field carried | -087 |
   | `a_released_handoff_routes_to_the_handoff_channel` | the `Released` translation, `seq` carried | -087 |
   | `a_handoff_event_is_never_self_filtered` | both variants route for a node that is not `source_node` — the broadcast policy, stated as a test | -090 |
   | `a_role_change_naming_another_node_routes_nowhere` | `EventRoute::NotOurs` for both role variants | -043 |
   | `a_migration_complete_is_never_self_filtered` | broadcast, complementing `test_migration_complete_event_fires` | -034 |
   | `a_silent_router_delivers_nothing` | `ClusterEventRouter::silent()` over all five variants — the `get_snapshot_builder` rule as a test rather than a comment | -017 |

   The first three are **new pins on behaviour that has never been forced**, which is the
   proposal's main testability claim. Add them to the `Forced by` cells of -087 and -090; both
   rows keep every existing entry.
5. **Forcing tests stay in the mutated crate.** All six are inline `#[cfg(test)]` tests in
   `frogdb-cluster`, alongside the module they force, so `cargo mutants -p frogdb-cluster`
   sees them. Nothing moves to `frogdb-server`.
6. **Spec citation hygiene.** FM-CLUSTER-043's Invariant cites `state.rs:688-728`, -044 cites
   `state.rs:333-347`, -045 cites `state.rs:787-797`, -046 cites `state.rs:517-527`, and -017
   cites `state.rs:808-837` — **all five are already stale** on the current tree (they predate
   the invariant-catalog commits) and all five must be re-cited in the step-1 commit. Meaning
   is unchanged; only citations move, which is the D2 precedent's exact allowance. `Forced by`
   cells change only by the additions in criterion 4.
7. **Mutation re-gate: full run, not diff.** `just mutants-diff frogdb-cluster` before pushing,
   then `just mutants frogdb-cluster` + `just mutants-gate frogdb-cluster 0.80`. A full run is
   mandatory here because the extraction **relocates mutation targets**: today the fan-out is
   part of `apply`'s single, trivially-killed target, and after the move `plan_event_route` and
   `route` are targets of their own. Expect the *measured* score to reflect coverage that was
   previously masked — which is the point, and which is why criterion 4's tests are not optional.

## Testability improvement

**Today the routing layer's only test surface is openraft.** Exercising it means building a
`ClusterStateMachine`, calling an `enable_*` method, constructing `openraft::Entry` values, and
awaiting `apply` — 20+ lines of scaffolding per assertion (`state.rs:1607-1640` is the
canonical shape). Four consequences, each verified:

1. **A 30-line untested region becomes six three-line assertions.** The slot-handoff
   translation (`:990-1019`) has no test in the workspace (§2). With `plan_event_route` it is
   `assert_eq!(plan_event_route(prepared_event, Some(1)), EventRoute::SlotHandoff(…))`. This is
   not a re-shaping of existing coverage — it is coverage that does not exist.
2. **The seam between two locked crates becomes assertable from one side.** FM-CLUSTER-087's
   Invariant currently asserts, in prose, something no test in its `Forced by` list executes.
   After this change `frogdb-cluster` can force the whole chain in one crate: `apply_command`
   emits, `plan_event_route` translates, and the resulting `SlotHandoffEvent` is the exact
   value `plan_handoff_action` already has tests for. Two proven halves plus a proven joint.
3. **The delivery *policy* becomes comparable.** "Role changes are self-filtered, handoffs are
   not" is currently asserted only negatively — by tests that would fail if a *role* event
   leaked (`test_demotion_detection_ignores_other_nodes`, `:1644`) — and not at all for the
   other two families. `EventRoute` makes the policy the return value, so a table-driven test
   over all five variants × {self, other, unset} is ~15 lines and covers a matrix no current
   test touches.
4. **The identity question becomes a parameter instead of a field.** `plan_event_route(event,
   self_node_id)` takes the identity as an argument, which is what makes step 2's spec-first
   test (`a_hard_reset_re_points_the_role_filter`) writable at all: today the only way to
   observe the stale copy is to build a state machine, mutate `ClusterState` behind its back,
   and apply an entry. This is also why step 1 has value even if step 2 is never approved —
   the extraction is what makes §4 *testable*, and an untestable defect is one nobody can
   confidently rule on.

## Risks / scope boundaries vs sibling proposals

### Conflict and order edges

| sibling | shared files | edge |
|---|---|---|
| **60 — migration-table (RC8)** | `cluster/src/types.rs` (`ClusterEvent` `:477-537`, read-only for me), `cluster/src/commands.rs` (untouched by me) | **Order: 59 first.** 60 is a large pure move of the handoff lifecycle into a `MigrationTable` type and the lane files it as "solo, land last". If 60 alters the `ClusterEvent` variant set, `plan_event_route` is then the *single* place that must follow — landing 59 first is what gives 60 that property instead of five loose match arms. **No production file is edited by both.** |
| **62 — handoff-finalizer (RC11)** | `server/src/server/cluster_init.rs`; the `SlotHandoffEvent` type (`state.rs:562-595`) | **Real textual edge, mitigated by design.** 62 moves `SlotMigrationCoordinator::complete/poll` into `cluster-runtime` and will rewire the finalizer's construction in `cluster_init.rs`. Criterion 3 keeps 59's `cluster_init.rs` diff **empty** — the `enable_*` signatures are deliberately preserved. If 59 later drops that constraint, 59 rebases onto 62, not the reverse. `SlotHandoffEvent`'s definition and field set are unchanged by both; 59 tests its *production*, 62 its *consumption*. |
| **61 — primary-snapshot-hooks (RC9+RC12)** | none | **No file overlap** (`frogdb-replication`). Same *shape* — post-construction `Option` setters collapsed into one wiring seam, mirroring ADR-0004's installer mitigation. Worth reviewing together for a consistent naming convention; land independently. |
| **58 — auto-failover-propose-retry (RC6)** | `server/src/server/cluster_init.rs` (58 cites `:556-658` as **evidence only**, no edit) | **No conflict.** 58 owns `cluster-runtime/src/failure_detector.rs` + `cluster/src/network.rs`; 59 owns `cluster/src/state.rs`. Both need the 0.80 gate — chain the PRs and run the full gate once at the end. |
| **57 — raft-network-send (RC5)** | `cluster/src/network.rs` (57's file, not mine) | No conflict; shared 0.80 gate only. |
| **53 / 54 / 55 / 56** | none | `frogdb-replication`; disjoint crate and gate (0.85). |

### Risk — step 1's whole claim is "nothing changed", and only unedited tests prove it

There is no new behavioural acceptance test for the move itself; it borrows the 24 role tests,
the migration test and the reconciler family. That is why criterion 1 is phrased as *zero
edits to existing tests*: a step-1 diff that touches an existing assertion has stopped being a
move. The six new tests in criterion 4 pin behaviour that was previously unpinned; they are
not evidence that the move was faithful.

### Risk — the extraction can lower the measured mutation score

Stated plainly rather than discovered at gate time. Moving the fan-out out of `apply` converts
masked coverage into measured coverage. If criterion 4's tests were skipped, the score would
drop even though nothing got worse. They are therefore acceptance criteria, not follow-ups,
and criterion 7 requires the full run rather than `mutants-diff` for exactly this reason.

### Risk — `EventRoute::NotOurs` re-introduces a catch-all

`plan_event_route` must match all five `ClusterEvent` variants **explicitly**, with the two
role-variant guards producing `NotOurs` in their `else` position, and must **not** use `_ =>`.
Today's `:1021-1022` catch-all is precisely the construct that would let a sixth variant be
added and silently dropped; reproducing it inside the new module would forfeit half the
proposal's value. Reviewable in one line of the diff.

### Non-risk — `ClusterEvent` stays crate-internal

`ClusterEvent` is not re-exported from `cluster/src/lib.rs` (`:77-82` exports `types::*`
without it), so `EventRoute` and `plan_event_route` are `pub(crate)` and constrain no
downstream crate. `ClusterEventRouter` need only be as public as `ClusterStateMachine`'s field
requires.

## Effort

**S/M** — two commits, one shared re-gate. Step 1 is the S; step 2 is the M, and is optional
and separately reviewable.

| step | scope | size |
|---|---|---|
| **1 — extraction (no behaviour change)** | New `cluster/src/state/events.rs`: `EventRoute`, `plan_event_route`, `ClusterEventRouter` + `silent()` + the three `enable_*` bodies. `ClusterStateMachine` loses four fields, gains `router`; three ctor sites collapse; `apply`'s `:942-1024` becomes one call; `self_role`/`self_role_reconciler`/`emit_self_role_change` read through the router. Six new tests (criterion 4). Five stale spec citations re-pointed; two `Forced by` cells extended. | **S** — ~180 new lines (≈half tests), ~95 deleted |
| **2 — live identity (SPEC-FIRST, optional)** | FM-CLUSTER-043 and -046 amended; `a_hard_reset_re_points_the_role_filter` written **failing**; router holds `ClusterState`; `enable_role_changes` also calls `set_self_node_id`. | **M** — small diff, spec-first process is the cost |
| **re-gate** | `mutants-diff frogdb-cluster`, then full `mutants` + `mutants-gate frogdb-cluster 0.80`. Chain with proposals 57/58/60 and gate once. | — |

**Independently landable ahead of everything:** the five stale spec citations in criterion 6
(FM-CLUSTER-017, -043, -044, -045, -046). Pure documentation, no code, no gate, correct
whether or not this proposal is approved.

**No hotfix is proposed.** §4 is the only defect found and it is latent by the argument given
there; its fix changes what a locked row means, which is step 2's job and not a hotfix's. If
triage prefers a zero-risk partial, the honest one is a **doc-comment** on
`ClusterStateMachine.self_node_id` (`state.rs:600-601`) recording that it is a boot-time copy
that a HARD reset does not re-point, and cross-referencing the live reader at
`handoff_barrier.rs:190` — which converts an invisible inconsistency into a visible one at
zero behavioural risk.
