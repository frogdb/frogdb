# Proposal 65 — `init_cluster` splits into a boot *plan* and three phase runners

Round 38 · lane: cluster · effort **M** · `frogdb-server` (no mutation gate) wiring the LOCKED
cluster crates · **not spec-first** (no behavior change proposed; one prose spec cite must be
refreshed in the same commit)

Covers exploration-lane candidate **SV3** ("`init_cluster` god-fn — `cluster_init.rs:73-790`, ~20
params; `SplitBrainLogger`/`split_brain_header`/`RoleChangeConsumer` colocated unrelated").
Verified against `6e99f567`; **every cite re-derived at HEAD `03efffeb`** after adversarial
review (the lane brief's line numbers had all drifted — corrections are inline below).

**Revision note (post-review).** This is the amended draft. The review upheld the spine and every
structural cite, and overturned three things: hotfix 3's *latent* ruling (it is live — the
published quickstart prescribes the broken config), hotfix 3's proposed fix (it removed a symptom,
not the defect), and two silent behavior changes hidden in the split's specification. All are
corrected below; §"Ordering constraints" grew from six entries to eight and one of the original
six had the wrong reason.

## Summary

`cluster_init.rs` is 1938 lines holding four things that share nothing but a file: one 733-line
`async fn init_cluster` (`:73-806`) with **18 parameters** (17 under `--features turmoil`:
`tls_runtime` is `#[cfg(not(feature = "turmoil"))]`, `:122-124`) and an 11-field result; a
split-brain audit *policy* (`SplitBrainLogger` `:813`, `split_brain_header` `:893`); a role-change
*consumer* (`RoleChangeConsumer` `:929`); and 883 lines of tests (`:1056-1938`, 45% of the file)
that are **entirely** about the last two. The boot function itself has **zero tests**.

That last sentence is the proposal. `init_cluster` makes six decisions — this node's id, the
initial member set, whether this node bootstraps, which restored member nodes to skip, how a port-0
listener resolves into an advertised address, and whether to auto-assign slots — and not one of
them is reachable without a live listener, an opened Raft store, a shard-sender fan-out, a client
registry, a metrics recorder and a TLS runtime. They are decided *inline, between I/O statements*,
so the only way to observe a wrong decision is to boot a cluster and watch what it converges to.

The change has two halves, and they are not the same kind of change:

* **Depth.** Extract `plan_cluster_boot(input) -> ClusterBootPlan`, a pure function over plain
  values, that makes all six decisions. This is the same shape the neighbouring code already uses
  — `plan_handoff_action` + `run_slot_handoff_barrier` and `plan_migration_notice`, both in
  `frogdb-cluster-runtime` — a pure planner with a thin async runner. It buys a test surface the
  boot path does not currently have at any price, and it is the only part of this proposal that
  survives the deletion test.
* **Locality.** Move the split-brain policy and the role-change consumer (plus their 883 test
  lines, tags intact) into `cluster_init/split_brain.rs`, and give each boot phase its own
  function. This buys no leverage and I do not claim it does; it buys the ability to read one
  concern without scrolling past two others, and it makes the 18-parameter signature *visibly* the
  union of three smaller ones rather than an accident.

**One live defect sits inside the split's blast radius** — hotfix 3, re-ruled: the published
operator quickstart prescribes a `[cluster]` config that makes all three nodes bootstrap three
different Raft clusters. It is filed at **defect priority**, is **not** folded into the refactor,
and its fix is a config-surface change plus a docs change, not a planner change. Two further
independently-landable hotfixes (a published config-doc claim that is false, and a dead
config-conversion path) are written up at the end.

## Files involved

| path | lines | role in this proposal |
|---|---|---|
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 1938 | **the change.** `ClusterInitResult` `:43-65`; `init_cluster` `:73-806` (phase A `:126-175`, phase B `:177-757`, phase C `:759-791`, result assembly `:793-805`); `SplitBrainLogger` `:813-881`; `split_brain_header` `:893-910`; `RoleChangeConsumer` `:929-1053`; tests `:1056-1938` |
| `frogdb-server/crates/server/src/server/cluster_init/boot_plan.rs` | new | the pure planner + its unit tests |
| `frogdb-server/crates/server/src/server/cluster_init/split_brain.rs` | new | `SplitBrainLogger`, `split_brain_header`, `RoleChangeConsumer` and the 883 test lines, moved verbatim |
| `frogdb-server/crates/server/src/server/mod.rs` | 1000+ | the **only** caller: `cluster_init::init_cluster(...)` `:348`, args `:349-370`, result consumed from `:373`. **Diff is empty** under this proposal — the signature is preserved deliberately (see siblings 63/64) |
| `frogdb-server/crates/server/src/server/util.rs` | — | `hash_addr_to_node_id` `:33-37` — the live node-id derivation, called by the planner |
| `frogdb-server/crates/cluster/src/types.rs` | — | `even_slot_ranges` `:265` (called **twice** today, `:532` and `:616`); `ClusterConfig` `:541-558` (dead — hotfix 2) |
| `frogdb-server/crates/cluster/src/network.rs` | — | `ClusterNetworkFactory` fields `:224-239`, `set_connect_factory` `:278`, `register_node` `:283`, `handle_rpc_request` `:933-945`. **Read-only** — cited by ordering constraint 4 and by hotfix 3 |
| `frogdb-server/crates/cluster/src/state.rs` | — | `attach_snapshot_store` `:646-674` — the restore whose completion the seed-skip guard depends on. Read-only here; the ordering it establishes is a constraint the split must preserve |
| `frogdb-server/crates/config/src/cluster.rs` | — | `node_id` field + doc `:23-27`, `validate` `:182-227`, `effective_node_id` `:231-243` (dead), `effective_client_addr` `:246-258`, `cluster_bus_socket_addr` `:261-263`. **Read-only under 65**; edited only by hotfixes 1 and 3 |
| `.scratch/hardening/specs/replication-failure-modes.md` | — | FM-REPLICATION-048 Invariant `:1022` — the **only** spec line naming `cluster_init.rs`. Prose-only refresh |
| `frogdb-server/crates/server/src/role_manager.rs` | — | doc `:25` names `server::cluster_init::RoleChangeConsumer` — a module-path cite that goes stale; one-line refresh |
| `website/docs-spec/specs/operations/clustering.md` | — | cites `cluster_init.rs` as bootstrap source of truth `:37`, `:73`, drift guard `:169`. **Stays true** — see "what does not change" |
| `website/src/content/docs/operations/clustering.md` | — | the **rendered** operator page. `:62-74` publishes the config hotfix 3 breaks; `:91` publishes hotfix 1's vagueness. Read-only under 65, edited by the hotfixes |

Correction to the lane brief's citations, all re-derived at HEAD: `init_cluster` is `:73-806` (not
`:73-790`), `SplitBrainLogger` `:813` (not `:795`), `split_brain_header` `:893` (not `:875`),
`RoleChangeConsumer` `:929` (not `:911`), and the parameter count is **18** (17 under turmoil), not
~20.

## Problem

### One function, three jobs, one of them unconditional

| lines | job | runs when |
|---|---|---|
| `:126-175` | build the **role stack** — mint the shared offset atomic, `RealReplicaStreamer`, `.with_control_applier`, `.with_net_bytes_counters`, `RoleManager::new`, `set_replication_self_fence`, `set_stint_target`, `RoleManagerHandle::new`, `role_controller` | **always**, including standalone |
| `:177-757` | cluster-**Raft** boot — id, state machine, snapshot restore, notification wiring, network factory + connect factories, member set, `Raft::new`, `initialize`, member seeding, self address resolution, slot auto-assign, four spawns, the slot-migration coordinator | `config.cluster.enabled` (`:177`) |
| `:759-791` | **failure detector** — config, `FailureDetector::new`, `spawn_failure_detector_task` | `if let (Some(raft), Some(state), Some(nid))` (`:760-761`) — a *destructure* of phase B's tuple, not a re-read of `config.cluster.enabled` |
| `:793-805` | **result assembly** — the 11-field `ClusterInitResult`, unioning all three phases' outputs | always |

A standalone node — no cluster, no Raft, no detector — traverses this function for one reason:
lines `:126-175`. Its name says the opposite. The 18 parameters are the union of the three jobs'
inputs; `#[allow(clippy::too_many_arguments)]` at `:72` is the acknowledgement already in the
tree. The 11-field `ClusterInitResult` (`:43-65`) is the union of their outputs, and the union is
lossy: eight fields are `Option` purely to encode "phase B did not run", while
`role_controller`/`role_manager_handle`/`is_replica_flag` are unconditional because they come from
phase A. A reader cannot tell which `None` means "standalone" and which would mean "cluster mode,
but this thing failed" — the type does not distinguish them. Phase C's gate makes that concrete:
it re-derives "cluster mode is on" by pattern-matching three `Option`s rather than reading the
flag, because by then the flag is no longer the thing it can act on.

### The six decisions, none of them observable

Interleaved with that I/O are the decisions that determine what cluster this process joins:

1. **Node id** (`:180-185`) — `config.cluster.node_id` when non-zero, else
   `hash_addr_to_node_id(cluster_bus_socket_addr())`. Note **which** bus address: the *configured*
   one, read at `:180`, not the resolved one computed later at `:501-517`.
2. **Initial member set** (`:360-372`, `:374-383`) — every parseable `initial_nodes` entry keyed by
   the *hash* of its address (`:362`), then `entry(node_id).or_insert(self)` (`:379-383`) keyed by
   the *configured* id over the *configured* bus address (`:375`).
3. **Bootstrap election** (`:386`) — `initial_members.keys().next().copied() == Some(node_id)`,
   i.e. lowest id wins, restart-safe because the map is a `BTreeMap`.
4. **Member seeding + the skip guard** (`:459-486`) — each member node gets a *guessed* client
   address (`bus_port.saturating_sub(10000)`, `:473`) but only if `cluster.get_node(id).is_none()`
   (`:469`). The comment there is exactly right about why the guard matters ("overwriting it with
   the guess would point this node's data path at a port nobody listens on"), and that correctness
   argument is enforced by no test in this file.
5. **Self address resolution** (`:492-517`) — `effective_client_addr` with the actual bound port
   substituted when `server.port == 0`; the bus address likewise from the actual listener. This
   **rebinds the name `cluster_bus_addr`** (`:501`), shadowing the configured binding from `:375`
   — the two are different values whenever the bus listener's port differs from the configured one,
   and only decisions *after* `:501` see the resolved one.
6. **Slot auto-assign** (`:530-546`) — `should_bootstrap && !initial_members.is_empty() &&
   !cluster.all_slots_assigned()`.

Every one is a pure function of values already in hand. Every one is currently written as a
statement in the middle of 733 lines of `await`. The forcing evidence for the decisions today is
indirect: the turmoil cluster suite depends on decisions 1 and 4
(`server/tests/common/sim_helpers.rs:18-20`: "`SERVER_PORT + 10000` so `cluster_init`'s
client-port derivation (`bus_port - 10000`) recovers exactly `SERVER_PORT`"; `:353-357` and `:449`
document that the sim deliberately runs `node_id = 0` so every node derives ids by hashing) — a
full simulated cluster is the cheapest thing that can currently disagree with a wrong constant.

### Duplication, dead conditions, noise

Consequences of "decided inline", all of which the planner removes as a side effect:

* **`even_slot_ranges` is called twice** over the same input — `:532` (seed local state) and `:616`
  (replicate through Raft, inside a spawn). The two must agree; nothing makes them.
* **The self `NodeInfo` is built three times** in two shapes — as a *guessed* member at `:475-479`
  if the node ever appears in its own `initial_nodes`, and correctly at `:518-519` and again at
  `:567-569` for the self-registration spawn. The last two also read
  `cluster_flags.replica_priority()` twice, at `:520` and `:569`, at two different instants; the
  planner's single input collapses that to one read, which is a (benign, but real) narrowing of
  behavior worth naming in the commit message.
* **`!initial_members.is_empty()`** is dead at **three** sites — `:437` (the `raft.initialize`
  guard), `:530` and `:606` — because `:379-383` unconditionally inserts this node and
  `should_bootstrap` (`:386`) is false on an empty map. `should_bootstrap` in the plan must absorb
  all three.
* **The bus-port branch** at `:508-512` reduces to `actual.port()` — both arms of
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
    pub configured_node_id: u64,               // config.cluster.node_id (0 = derive)
    /// The **configured** bus address (`cluster_bus_socket_addr()`, read at `:180`/`:375`).
    /// Feeds the node id, `register_node`, and the self `BasicNode` in `initial_members`.
    pub self_bus_addr_configured: SocketAddr,
    /// The **resolved** bus address (`:501-517`: configured IP + actual listener port).
    /// Feeds the advertised `NodeInfo` only.
    pub self_bus_addr_advertised: SocketAddr,
    /// Resolved client address (`:492-500`: `effective_client_addr` + actual bound port when
    /// `server.port == 0`). Advertised `NodeInfo` only.
    pub self_client_addr: SocketAddr,
    pub initial_nodes: &'a [String],
    pub replica_priority: u16,
    /// Node ids the restored `ClusterState` already knows (`cluster.get_node(..).is_some()`,
    /// `:469`). Must be read **after** `raft.initialize` returns — see constraint 7.
    pub restored_node_ids: BTreeSet<u64>,
    pub all_slots_assigned: bool,
}

pub(super) struct ClusterBootPlan {
    pub node_id: u64,
    pub initial_members: BTreeMap<u64, BasicNode>,   // -> raft.initialize
    pub node_registrations: Vec<(u64, SocketAddr)>,  // -> network_factory.register_node
    pub node_seeds: Vec<NodeInfo>,                   // already filtered by the skip guard
    pub this_node: NodeInfo,
    pub should_bootstrap: bool,
    pub slot_assignments: Option<Vec<(u64, SlotRange)>>, // Some => assign locally AND replicate
}

pub(super) fn plan_cluster_boot(input: ClusterBootInput<'_>) -> ClusterBootPlan;
```

**Two bus addresses, not one.** An earlier draft of this proposal carried a single `self_bus_addr`
described as "resolved: configured ip + actual bound port". That is a **silent behavior change**:
today the node id (`:180-185`), `network_factory.register_node` (`:376`) and the self `BasicNode`
in `initial_members` (`:379-383`) all use the *configured* address, and only the advertised
`NodeInfo` (`:518-519`) uses the resolved one — the name is bound twice and shadowed mid-function
(`:375` configured, `:501` resolved). Unifying them would change this node's raft id and its
membership entry on every port-0 bus boot, i.e. on every harness and turmoil run. Under a
proposal whose whole claim is "no behavior change", the input carries both and the planner is
explicit about which decision reads which. (Unifying them is defensible on its own merits — it is
arguably what an operator expects — but it is a *reviewed behavior change* belonging to whoever
files it, not a side effect of a refactor.)

The two address resolutions (`:492-517`) stay *outside* the planner and are passed in already
resolved — they are the only decisions that need `listener.local_addr()`, and keeping the planner
free of `Result` and of `Config` is worth the two lines at the call site.

`slot_assignments` being `Option` collapses the two `even_slot_ranges` calls and all three dead
`!initial_members.is_empty()` guards into one decision computed once: `None` means "not the
bootstrap node, or slots already assigned"; `Some(v)` is consumed twice — once locally at the
`:530-546` site, once by the replication spawn at `:606-658` — with the identical vector rather
than a recomputation that is merely expected to match.

`node_seeds` applies the skip guard (`restored_node_ids`) inside the planner, so "a restored
node's agreed address is never overwritten by the guess" becomes an assertion in a unit test
instead of a paragraph of comment.

### 2. `cluster_init/split_brain.rs` — the policy (this is locality)

`SplitBrainLogger`, `split_brain_header`, `RoleChangeConsumer` and the whole `#[cfg(test)] mod
tests` move **verbatim**, tags and test names untouched, visibility widened to `pub(super)`. Zero
logic edits: the point of doing it in the same change is that `git log --follow` on the boot is
never again 45% split-brain tests, and the point of doing it *verbatim* is that the diff is
reviewable as a move.

### 3. `cluster_init.rs` becomes the boot sequencer

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

("Sequencer", not *orchestrator*: `frogdb-server/CONTEXT.md:30-31` retires that word for the
superseded external-control-plane design and forbids it in new code and docs.)

`bootstrap_cluster_raft` is the thin runner over the plan, and its internal order is **today's
order**, not a rearrangement:

1. build the network factory; install the connect factory (`:304` turmoil / `:352` TLS) — see
   constraint 4;
2. parse `initial_nodes`, `register_node` each entry and self (`:363`, `:376`);
3. resolve the two addresses (`:492-517`) — hoisted above `Raft::new`, which is safe because both
   read only the listeners, and is what lets the plan be built in one call;
4. `Raft::new` (`:424`), then `raft.initialize` (`:448`) when bootstrapping;
5. **`plan_cluster_boot`** — after `initialize` returns, so `restored_node_ids` is the same
   snapshot today's `cluster.get_node(..)` at `:469` sees (constraint 7);
6. apply the plan: `node_seeds` (`:468-486`), `this_node` (`:518-525`), `slot_assignments`
   (`:530-546`);
7. the four spawns and the slot-migration coordinator, in their current order.

The `enable_*` wiring stays where it is (`:216`, `:237`, `:240`), before the state machine moves.

The earlier draft's one-line sketch ("plan → apply the plan (register, seed, initialize)") had
steps 2, 4 and 6 in the wrong order and would have computed the plan *before* `Raft::new`. That
version snapshots `restored_node_ids` earlier than the live read at `:469`, which today runs after
the Raft apply loop is up and can therefore see nodes Raft itself added. A narrower snapshot
re-seeds an already-known node with the guessed `bus - 10000` port — precisely the corruption
constraint 1 exists to prevent. If a future revision ever wants the plan computed earlier, the
skip guard must stay behind as an apply-time re-check; it cannot simply move.

### Ordering constraints the split must preserve — boot order is behavior

These are the constraints I verified while tracing; they are the review checklist for the change,
and any one of them broken is a real bug the type system will not catch:

1. `attach_snapshot_store` (`:208`) **before** member seeding (`:468-486`) — the skip guard reads
   the restored state. The planner takes `restored_node_ids` as an *input*, which makes the
   dependency explicit at the call site instead of implicit in statement order.
2. All three `enable_*` calls (`:216` role change, `:237` migration complete, `:240` slot handoff)
   and `self_role_reconciler()` (`:421`) **before** the state machine moves into `Raft::new`
   (`:424`). After the move they are unreachable — this is why `:421` exists as a separate line
   today.
3. `reconcile_self_role()` (`:225`) may fire an event before the consumer is spawned (`:660`); the
   channel is unbounded (`state.rs:700-708` returns an `mpsc::UnboundedReceiver`), so the event
   waits. The split must not turn that channel into a bounded one or move the emit after the spawn
   (both would be behavior changes).
4. **`set_connect_factory` before the factory is cloned and moved.** `ClusterNetworkFactory`
   (`network.rs:224-239`) holds `connect_factory: ConnectFactory` as a **plain, non-`Arc` field**,
   set by `set_connect_factory(&mut self)` (`network.rs:278`) at `cluster_init.rs:304` (turmoil)
   and `:352` (cluster-bus TLS). Both call sites must precede the clone at `:415` **and** the move
   into `Raft::new` at `:424`; otherwise the raft-owned copy and both `ClusterWriter`s built from
   the clone (`:562-566`, `:609-613`) dial plain TCP — cluster-bus TLS silently off in production,
   and every bus RPC black-holed under turmoil (which routes only through its own factory). The
   compiler catches none of it.

   *This replaces the earlier draft's constraint 4 ("`register_node` before `Raft::new`"), whose
   reason was wrong:* `node_addrs` is `Arc<RwLock<BTreeMap<..>>>` (`network.rs:226`) and
   `register_node(&self)` (`network.rs:283`) mutates through the `Arc`, which the clone at `:415`
   and the moved copy share. Registration order is genuinely **not** load-bearing; only the
   connect factory is.
5. Phase A **before** the role-change consumer spawn — the consumer holds `role_controller`.
6. Phase C last: the detector reads the live flags and the assembled state (its gate at
   `:760-761` destructures phase B's outputs, so it is structurally last already).
7. **`restored_node_ids` is read after `raft.initialize` returns.** Today's guard reads
   `cluster.get_node(*peer_id)` at `:469`, downstream of `Raft::new` (`:424`) and `initialize`
   (`:448`), so it observes anything the apply loop has already added. Computing the plan earlier
   narrows that set and re-seeds known nodes with guessed ports. Either the plan is computed at
   step 5 above, or the guard survives as an apply-time re-check.
8. **The three `apply_local` seedings race the live apply loop.** `:468-486`, `:518-525` and
   `:530-546` all write the same `ClusterState` the state machine holds
   (`ClusterStateMachine::with_state(cluster.clone())`, `:201`) — and by that point the state
   machine is inside a running, already-`initialize`d Raft. Local seeding and Raft-applied commands
   interleave in a window whose width is set by where the seeding sits relative to `Raft::new`
   (`:424`) and `initialize` (`:448`). The split must not move the seedings across either, in
   either direction; "it still compiles and the fields are the same" is not evidence here.

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
* **No address is unified.** See the two-bus-address note in §1.

### Deletion test, honestly

* `plan_cluster_boot` — **passes.** Delete it and the six decisions go back to being statements
  between `await`s; the caller is not shorter by much, but the decisions become unreachable from a
  unit test again, the two `even_slot_ranges` calls diverge again, and the skip guard goes back to
  being a comment. The interface (one input struct, one plan struct) is smaller than the behavior
  it hides, and its leverage grows with every future boot decision.
* `split_brain.rs` — **fails.** Delete the module boundary and nothing breaks; the code moves back
  into one file. This is a locality change and I am not dressing it as depth. It is worth doing
  *with* the split because the 883 test lines are the thing that makes the current file
  unreadable, and because moving them costs one commit and zero risk.
* `build_role_stack` / `bootstrap_cluster_raft` / `spawn_failure_detection` — **fails as
  interfaces, passes as an ordering statement.** Each has exactly one caller and will never have
  two; they are not seams. What they buy is that constraints 1-8 above become a three-line
  function body instead of a 733-line reading exercise. Named honestly in the commit message as
  such.

## Testability improvement

Today: **zero** tests reach any boot decision. The file's 883 test lines reach the consumer and the
logger only. The cheapest existing thing that disagrees with a wrong boot decision is a turmoil
cluster (`simulation.rs`) or the multi-node harness (`cluster_harness.rs`) — and neither exercises
a configured `node-id`, because the sim pins `node_id = 0` (`sim_helpers.rs:449`) and the harness
sets it to exactly the hash (`cluster_harness.rs:154-160`). Nothing in the tree boots the
configuration hotfix 3 describes.

After: `plan_cluster_boot` is `fn(values) -> values` with no `async`, no `Result`, no I/O. The
tests that become writable — each a handful of lines, each currently impossible:

| what it forces | today's cheapest witness |
|---|---|
| configured `node_id` wins; `0` derives from the **configured** bus address | boot a node, read `CLUSTER MYID` |
| the node id and the self `BasicNode` use the configured bus address while the advertised `NodeInfo` uses the resolved one (the shadowing at `:375`/`:501` made explicit) | nothing — undetected |
| lowest id bootstraps; the same input set elects the same node on every restart | 3-node turmoil run |
| a node already in restored state is **not** re-seeded with the guessed port | restart-a-replica integration test |
| the guessed client port is `bus - 10000` and saturates rather than wraps for `bus < 10000` | nothing |
| `port == 0` resolves to the bound port in the advertised client address | port-0 harness boot |
| slot ranges are computed once and are identical for the local seed and the Raft replication | nothing — two calls, no cross-check |
| no slot plan when slots are already assigned, or when not bootstrapping | 3-node turmoil run |
| self appearing in its own `initial_nodes` contributes **one** member, not two | nothing — undetected (a *symptom* of hotfix 3, not its fix; see below) |

The last row is deliberately demoted. An earlier draft advertised it as hotfix 3's fix; it is not
(see hotfix 3). It remains a useful assertion once hotfix 3 has removed the way to reach the
divergent case, because it pins that the planner never double-counts one process.

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
  names to `// FM-` tags by **leaf name**, scanning `SOURCE_ROOTS = [frogdb-server/crates]` — it is
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
  `frogdb-server` (ungated) as noted. (Hotfix 2 and hotfix 3 option (b) *do* touch
  `frogdb-cluster` and `frogdb-config` — they land as their own changes, with their own
  `just mutants-diff` pass.)
* **Seam lints**: no clock reads, metrics emissions, redirect replies or durable-ack writes are
  added or moved out of their chokepoints — the split-brain telemetry arms (`:860`, `:869`,
  `:875-879`) move as a unit with the function that owns them. `just lint-gates` unaffected.
* One in-tree doc cite goes stale and is refreshed with the move:
  `role_manager.rs:25` names `server::cluster_init::RoleChangeConsumer`.

## Risks / scope boundaries vs siblings

**Proposal 59 (cluster-event-router) — real contact, ordered.** 59 preserves the
`enable_role_change_detection` / `enable_migration_complete_notification` /
`enable_slot_handoff_notification` signatures precisely so its `cluster_init.rs` diff is empty
(59 `:464-466`); its sole caller cite for `enable_slot_handoff_notification` is
`cluster_init.rs:240`. Under this proposal those three call sites (`:216`, `:237`, `:240`) **move
verbatim into `bootstrap_cluster_raft` in `cluster_init.rs`**, unchanged in form, arguments and
relative order, and still before the `Raft::new` move (constraint 2). **Recommended order: 59
lands first** — 59 keeps its empty diff, and 65 then relocates already-final call sites once.
Reversed, 59 becomes a three-line rebase; either way there is no semantic conflict, only churn.

**Proposal 62 (handoff-finalizer-move) — no contact, and no dispute to record.** 62's
`cluster_init.rs` diff is empty: it edits `server/src/slot_migration/mod.rs` plus new modules in
`frogdb-cluster` / `frogdb-cluster-runtime`, and names `connection/cluster.rs:133-151` as the
finalizer's sole caller. The `SlotMigrationCoordinator::new` + `spawn_event_dispatcher` +
`spawn_handoff_barrier` block (`:735-746`) moves as-is under 65 and 62 never sees it. Either
order; no constraint. (An earlier draft of this proposal spent a paragraph "correcting" 59's claim
that 62 rewires `cluster_init.rs`. That paragraph was itself stale: 59 at HEAD already retracts it
verbatim at `59-cluster-event-router.md:477-480`. Nothing left to correct.)

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

**Hotfix 3 — sequencing.** Its fix touches `frogdb-config` and the rendered docs, not
`cluster_init.rs`'s structure, so it is independent of 65 in either direction. It should land
**first** regardless, because it is a live defect on the documented happy path and 65 is a
refactor.

**The risk that matters.** This is a boot-ordering refactor in a file with no boot tests. Nothing
in the split is caught by the compiler if constraints 1-8 are violated — `attach_snapshot_store`
after the seed loop compiles fine and silently overwrites agreed node addresses with guesses; a
`set_connect_factory` moved below the clone compiles fine and silently disables cluster-bus TLS.
The mitigations are: (a) the planner's `restored_node_ids` input makes constraint 1 a *data*
dependency rather than a statement-order one; (b) the split is verbatim-move plus extraction, no
rewrites; (c) the turmoil cluster suite and `cluster_harness` multi-node tests are the acceptance
gate — a reordering that breaks bootstrap, node-address convergence or slot assignment fails them,
and the TLS-cluster harness path (`cluster_harness.rs`, `tls_cluster: true`) is what catches
constraint 4. The change should not be reviewed as a formatting diff.

## Effort

**M.** Roughly: planner extraction + its unit tests (the real work, ~1 day), verbatim move of ~250
policy lines and ~883 test lines (mechanical), three phase functions (mechanical), plus one spec
prose line and one doc comment. No behavior change, no new dependency, no LOCKED crate edited, no
generated artifact regenerated. Reviewable in three commits: (1) move split-brain out verbatim,
(2) extract the planner + tests, (3) phase functions.

## Independently-landable hotfixes

Found while verifying; none is folded into the refactor.

**1 — the `node-id` documentation is false, and it is published.** `config/src/cluster.rs:23`
documents `node_id` as "This node's unique ID (0 = auto-generate from timestamp)". The live boot
does no such thing: `cluster_init.rs:181-185` falls back to
`hash_addr_to_node_id(cluster_bus_socket_addr())` (`util.rs:33-37`), a `DefaultHasher` over the
address — deterministic across restarts, which is exactly what bootstrap-by-lowest-id needs, and
the opposite of a timestamp. The timestamp implementation (`effective_node_id`, `:231-243`,
`(timestamp << 16) | random`) is real but unreachable (see hotfix 2). The doc string is generated
into `website/src/data/config-reference.json:148` (generator: `ops/docs-gen/src/main.rs:292`) and
shipped. **The rendered operator page carries the same vagueness**:
`website/src/content/docs/operations/clustering.md:91` publishes "`0` auto-generates one". Fix:
correct the doc comment to state the address derivation, make the docs table row specific, and
regenerate. One-line source change plus the prose line plus `just docs-gen`. Never edit the JSON.

**2 — a dead config-conversion path.** `ClusterConfigExt::to_core_config`
(`server/src/config/mod.rs:70-91`) is the **only** caller of `effective_node_id()` and itself has
**zero** callers; it builds `frogdb_core::ClusterConfig` (`cluster/src/types.rs:541-556` + `Default`
`:558`), which has no live consumers either. Deleting the three together removes the
timestamp-vs-hash contradiction at its root and shrinks a LOCKED crate's mutant surface. Verify
with `just check` that nothing behind a non-default feature reaches them.

---

### 3 — **LIVE DEFECT** (defect priority): a configured `node-id` splits the cluster into N single-node clusters, and the published quickstart prescribes it

*Re-ruled from "latent" after review. The earlier draft's reachability survey (helm-gen, the
operator's `config_gen`, both example configs, the harness) is individually correct on every point
— but it surveyed **generators and examples** and missed the **rendered operator documentation**,
and it stopped tracing at "phantom voter", which understates the consequence by a wide margin.*

#### The defect

Two ids for the same process are derived by two different rules that are never reconciled:

* **This process's own raft id** comes from configuration when set:
  `cluster_init.rs:181-185` — `if config.cluster.node_id != 0 { config.cluster.node_id } else {
  hash_addr_to_node_id(&cluster_addr) }`.
* **Every other node's id** is derived by hashing that node's bus address, with no configuration
  input whatsoever: `cluster_init.rs:362` — `hash_addr_to_node_id(&peer_cluster_addr)`, applied to
  every entry of `initial_nodes` (`:360-372`).

Therefore **any** node whose `node-id` is not exactly `hash_addr_to_node_id(own bus addr)` is
referred to by every other node in the cluster under an id it does not run as. Nothing validates
this: `config/src/cluster.rs:182-227` (`validate`) checks `cluster_bus_addr` non-empty and
parseable, the two timeouts, and that each `initial_nodes` entry parses — and nothing else. There
is no cross-check between `node_id` and `cluster_bus_addr`.

#### It is reachable from the published docs

`website/src/content/docs/operations/clustering.md:62-74` — the rendered "Bootstrapping a cluster"
quickstart, the first thing an operator follows — prescribes exactly this:

```toml
[cluster]
enabled = true
node-id = 1
cluster-bus-addr = "10.0.0.1:16379"
initial-nodes = [
  "10.0.0.1:16379",
  "10.0.0.2:16379",
  "10.0.0.3:16379",
]
```

`node-id = 1` (and 2, 3 on the other nodes) with each node's **own** bus address listed in
`initial-nodes`. The earlier draft cited only the docs-*spec*
(`website/docs-spec/specs/operations/clustering.md:132-133`) as "inviting" the pattern; the
rendered page does not invite it, it prescribes it.

Nothing in the tree exercises this configuration, which is why it has gone unnoticed:
`cluster_harness.rs:154-160` sets `node-id` to `DefaultHasher` over `127.0.0.1:<cluster_port>` —
byte-for-byte `hash_addr_to_node_id` of the bus address (`util.rs:33-37`), so configured and
derived coincide; and the turmoil sim sets `node_id = 0` outright (`sim_helpers.rs:449`, comment
at `:353-357`). The single production reader of the key is `cluster_init.rs:181-182`; the single
setter in the whole workspace is the harness (`test-harness/src/server.rs:567-568` ←
`cluster_harness.rs:180`).

#### Traced to conclusion: three nodes, three clusters, 3× 16384 slots

For the documented 3-node config, on node *i* with configured id *i* ∈ {1,2,3} and
h(a₁),h(a₂),h(a₃) the 64-bit hashes of the three bus addresses:

1. **Member map** (`:360-383`) — `initial_members` = `{h(a₁), h(a₂), h(a₃)}` from the loop, then
   `entry(i).or_insert(self)` adds the configured id. Four members for a three-process cluster,
   on **every** node, with a different fourth entry each time: `{1,h₁,h₂,h₃}`, `{2,h₁,h₂,h₃}`,
   `{3,h₁,h₂,h₃}`.
2. **All three nodes elect themselves bootstrap** (`:386`) — `should_bootstrap` is
   `initial_members.keys().next() == Some(node_id)`, and `BTreeMap` orders by `u64`. The
   configured ids 1, 2, 3 are the smallest `u64`s in each map with overwhelming probability
   (`DefaultHasher::finish()` output being ≥ 3 is a certainty in any practical sense). So on node
   1 the lowest key is 1, on node 2 it is 2, on node 3 it is 3 — **true on all three**.
3. **All three call `raft.initialize`** (`:448`). `already_initialized` (`:439-440`) is false on a
   fresh boot, and the `!initial_members.is_empty()` guard at `:437` is dead. Three nodes
   initialize three *different* 4-member configurations. Each believes itself the founder of a
   cluster whose other three members are ids nobody runs as.
4. **All three auto-assign all 16384 slots** (`:530-546`) — `should_bootstrap && !… && !
   cluster.all_slots_assigned()`, all true — spreading the full slot space over its **own**
   4-id set via `even_slot_ranges(&node_ids)` (`:531-532`), and then **all three replicate**
   conflicting `AssignSlots` through Raft (`:606-658`).
5. **The phantom ids answer only by accident.** `network_factory.register_node` maps both `i` and
   `h(aᵢ)` to the same socket (`:363` and `:376`), and the inbound bus path performs **no
   target-node-id check**: `cluster-runtime/src/bus.rs:86` delegates straight to
   `cluster/src/network.rs:933-945`, whose `AppendEntries` / `Vote` / `InstallSnapshot` arms
   dispatch into the *local* raft regardless of who the RPC was addressed to. So node 1's process
   will answer votes and appends addressed to `h(a₁)` **as node 1**, with node 1's term and log.

The outcome is not "a phantom voter in an otherwise healthy cluster". It is three competing
bootstraps, three conflicting full-slot-space assignments, and a bus that lets each process
impersonate an id it does not own. What the cluster converges to depends on openraft's term
bookkeeping and on RPC arrival order — which is the point: **the documented happy path has no
defined outcome.**

#### The earlier draft's one-liner does not fix this

That draft proposed making `plan_cluster_boot` recognise this node's own bus address in
`initial_nodes` and key it by the *effective* id. That removes the **local duplicate** — the map
becomes 3 members instead of 4 — and nothing else. With it applied, the documented config still
produces three divergent bootstraps: `{1, h₂, h₃}` on node 1, `{h₁, 2, h₃}` on node 2, `{h₁, h₂,
3}` on node 3. The promised planner test ("self in own `initial_nodes` yields one member, not
two") would have pinned a fix that leaves the cluster broken — which is worse than no fix, because
it retires the symptom that would have led someone here.

The root defect is the **two derivation rules**, not the duplicate entry.

#### Fix set, with a recommendation

The invariant that must hold is: *every node computes the same id for a given bus address.* Since
other nodes' ids are `hash(addr)` with no configuration input, this node's id must be
`hash(own bus addr)`. A configured `node-id` therefore has an empty valid domain: `0` (derive), or
a value that equals the hash (redundant). Everything else is unconditionally wrong.

**(b) Delete the `node-id` key — recommended.** You cannot misconfigure what does not exist, and
this is the only option that removes the second rule rather than restating the invariant somewhere
else. Concretely:

* Delete `ClusterConfigSection::node_id` (`config/src/cluster.rs:23-27`) and reduce
  `cluster_init.rs:181-185` to `hash_addr_to_node_id(&cluster_addr)`.
* **Composes with hotfixes 1 and 2**: hotfix 1's false doc string disappears with the field, and
  hotfix 2's `effective_node_id` (`:231-243`) loses its last reason to exist. All three land as
  one coherent change to the node-identity surface.
* **Costs the harness nothing**: `cluster_harness.rs:180` sets the key to exactly the value the
  derivation produces, so removing the setter is a no-op for every multi-node test; the turmoil
  sim already passes `0`.
* **Both shipped example configs regenerate**: `website/src/data/example-config.toml:145` and
  `frogdb-server/ops/deploy/deb/frogdb.toml:144` are generated (`Config::default()` → TOML, and
  `just deb-gen` respectively), so the key vanishes from them automatically.
* **Known cost, must be handled**: `ClusterConfigSection` does **not** carry
  `#[serde(deny_unknown_fields)]` (unlike `persistence.rs:12`, `security.rs:10`, `chaos.rs:14` and
  others), so a stale `node-id = 1` would be *silently ignored* rather than rejected. Silently
  ignoring it happens to produce the *correct* cluster — but silence is the wrong contract for a
  key the docs told operators to set. Add `deny_unknown_fields` to the section in the same change
  so a stale quickstart config fails loudly at boot with the offending key named. That is the
  behavior an operator following `clustering.md:62-74` deserves.
* Breaking a documented config key is acceptable here: FrogDB is unreleased, pre-production
  software (CLAUDE.md, Development Philosophy).

**(a) Reject the mismatch in `validate`** — `node_id != 0 && node_id != hash(bus_addr)` bails. Keeps
the key. Rejected as the primary fix because it *restates* the invariant in a second place: the
hash lives in `frogdb-server/crates/server/src/server/util.rs:33-37` and `frogdb-config` has no
dependency on it, so `validate` would either duplicate the `DefaultHasher` construction (a second
source of truth for a value the whole cluster's identity depends on) or force `hash_addr_to_node_id`
down into the config crate as a prerequisite. And the key it preserves still has an empty valid
domain. Worth doing as an **interim** if (b) is judged too broad for a hotfix — it turns the live
defect into a loud boot error with a one-file diff.

**(c) Configuration-driven id derivation** — give `initial-nodes` entries explicit ids (e.g.
`1@10.0.0.1:16379`) so other nodes' ids come from configuration too, and the two rules become one with
a configured input. This is the right long-term shape (it is roughly what Redis Cluster's
`nodes.conf` does), and it is what a future round should file if operator-chosen ids are wanted.
It is a config-format change plus a cluster-formation redesign — far too large for a hotfix, and
it should not block (b).

**Pair any of them with the docs fix.** `website/src/content/docs/operations/clustering.md:62-74`
must not stay as-is: under (b) the quickstart drops the `node-id` line entirely; under (a) it must
stop prescribing `node-id = 1` alongside self-listing bus addresses. Leaving the page unchanged
ships a copy-pasteable config that breaks cluster formation, whichever code fix lands.

**Regression coverage.** The forcing test belongs at the config boundary, not in the planner: a
`frogdb-config` unit test that the quickstart's `[cluster]` block is rejected (option a/b with
`deny_unknown_fields`) or normalises to the derived id, plus — because this is a cluster-formation
defect and `frogdb-cluster` is a LOCKED area — a failure-mode row and a 3-node harness test
asserting that a fresh 3-node boot yields **one** `initialize`d configuration and one slot
assignment, not three. That is spec-first work and belongs to the defect issue, not to 65.
