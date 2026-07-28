# Proposal 37 — Cleanup: delete the lying `Option` in the `RaftClusterOp → ClusterCommand` adapter, drop its unreachable "Unsupported cluster operation" branch, and give command construction one owner

Status: AMENDED — re-scoped from "bug fix" to "S-effort cleanliness refactor" after adversarial
review. The compile-time exhaustiveness guard the original draft advertised **already exists**, and
the runtime error branch it promised to remove is **unreachable dead code**, not a live failure
mode. The change is still a small net improvement (behaviour-preserving), but its justification is
tidiness, not correctness. See [Adversarial review](#adversarial-review).

## Summary

`convert_raft_cluster_op` (`frogdb-server/crates/server/src/connection/util.rs:167-215`) is a
**shallow, dishonest adapter**: it maps the protocol-layer `RaftClusterOp` onto the cluster-layer
`ClusterCommand` but returns `Option<ClusterCommand>`, using `None` to mean "the caller must build
this one itself." Its two `None` cases — `Failover` and `ResetCluster` — are **both fully
representable** as `ClusterCommand` (they are constructed by hand elsewhere: `Failover` inline in
`handle_raft_command`, `ResetCluster` in `handle_reset_command`), so the `Option` is not modelling
impossibility — it is modelling "handled out-of-band."

Two things the original draft got wrong (corrected here):

1. **The exhaustiveness guard already exists.** `convert_raft_cluster_op` matches all 11
   `RaftClusterOp` variants with **no `_` arm** (`util.rs:168-214`). Adding a variant is *already* a
   compile error in the converter today — you cannot "forget the converter and have it compile
   clean." The residual, honest compile-time benefit of this refactor is narrower: making the
   adapter total (return `ClusterCommand`, not `Option`) removes the `None` **escape hatch**, so a
   future variant can no longer be lazily satisfied with `NewVariant => None` (which compiles and
   then defers to a runtime error). You must produce a real `ClusterCommand`.

2. **The runtime `"ERR Unsupported cluster operation"` branch (`cluster.rs:62`) is unreachable dead
   code, not a live failure mode.** The only two `None`-returning arms are `Failover` and
   `ResetCluster`; `ResetCluster` early-returns at `cluster.rs:34-42` and `Failover` is caught at its
   explicit match arm (`cluster.rs:51`) *before* the `_ => convert_raft_cluster_op(&op)`
   fall-through. The fall-through therefore only ever sees the 9 `Some`-returning variants, so
   `None => error` can never fire — which is why it is untested. Deleting it removes a lie, not a
   bug.

So this is a **cleanup**: delete the lying `Option`, delete the provably-dead error branch, and
consolidate the `Failover` (and, cosmetically, `ResetCluster`) construction so the "which
`ClusterCommand`" knowledge has a single owner. Compiler-checked totality replaces the incidental
integration coverage that is the only thing pinning the mapping today.

**Recommendation: proceed as a small cleanliness refactor.** The core mechanism (a total
free-function adapter in `server`) is verified feasible and behaviour-preserving. Two mechanisms the
exploration candidate proposed are infeasible and are corrected below: you cannot move the command
enum into the `cluster` crate "constructed directly by the protocol layer" (that inverts the
dependency graph), and you cannot use a `From<RaftClusterOp>` impl (Rust's orphan rule forbids it in
`server`). The viable deepening is a total **free-function** adapter in `server`. The candidate's
third motivator — slot-range "lossiness" — is weak and de-scoped (see Evidence): the state machine
flattens ranges to a per-slot map on apply regardless.

## Problem

### Three parallel command shapes, bridged by a lying `Option`

| Shape | Crate | Variants | Route |
| --- | --- | --- | --- |
| `RaftClusterOp` | `protocol` (`response.rs:508`) | 11 | `Response::RaftNeeded` → `handle_raft_command` → `convert_raft_cluster_op` |
| `SlotMigrationKind` | `protocol` (`response.rs:76`) | 3 (Begin/Complete/Cancel) | `Response::SlotMigrationNeeded` → `handle_slot_migration` → `SlotMigrationCoordinator` |
| `ClusterCommand` | `cluster` (`types.rs:269`) | 14 | the replicated state-machine input everything funnels into |

`ClusterCommand` is the 14-variant superset. `RaftClusterOp` and `SlotMigrationKind` are two disjoint
protocol projections of it: the 11 non-migration ops, and the 3 migration-lifecycle ops. The
protocol crate cannot name `ClusterCommand` — its doc-comment says so outright:
*"a serializable representation of cluster commands that lives in the protocol crate (which cannot
depend on core)"* (`response.rs:504-506`). So the two enums exist because of the dependency graph,
and the `server` crate (which sees both) bridges them.

The bridge (`convert_raft_cluster_op`) is where the shallowness lives. It returns
`Option<ClusterCommand>`, and the two `None` arms are not "cannot represent" — they are "the caller
constructs this one":

- `RaftClusterOp::Failover { .. } => None` (`util.rs:211`), but `handle_raft_command` builds
  `ClusterCommand::Failover { .. }` inline in a match arm (`cluster.rs:50-59`).
- `RaftClusterOp::ResetCluster { .. } => None` (`util.rs:213`), but `handle_reset_command` builds
  `ClusterCommand::ResetCluster { .. }` (`cluster.rs:197-200`).

So the knowledge "which `ClusterCommand` does this `RaftClusterOp` become" is smeared across **three
locations** — `util.rs`, and two hand-written arms in `cluster.rs` — kept consistent by hand. This is
poor **locality**: the adapter's own job (produce a `ClusterCommand`) is done partly outside the
adapter.

### The `None` fall-through is unreachable dead code

`handle_raft_command` funnels the non-special cases through the converter and turns `None` into a
runtime error:

```rust
// cluster.rs:33-42 — ResetCluster is intercepted and early-returns BEFORE the match below.
if let RaftClusterOp::ResetCluster { node_id, new_node_id } = &op {
    return self.handle_reset_command(raft, *node_id, *new_node_id).await;
}

let cmd = match &op {
    RaftClusterOp::Failover { .. } => ClusterCommand::Failover { .. },   // cluster.rs:51-59
    _ => match convert_raft_cluster_op(&op) {
        Some(cmd) => cmd,
        None => return Response::error("ERR Unsupported cluster operation"), // cluster.rs:62
    },
};
```

Trace the two `None`-returning arms of the converter against this dispatch:

- `ResetCluster` never reaches the `match`: it is intercepted by the `if let` early-return at
  `cluster.rs:34-42`.
- `Failover` never reaches the fall-through: it is caught by its explicit arm at `cluster.rs:51`.

So `_ => convert_raft_cluster_op(&op)` only ever sees the **9 `Some`-returning variants**, and
`None => return Response::error(...)` (`cluster.rs:62`) **can never fire**. It is dead code — which is
exactly why it is untested (verified: zero test hits for `convert_raft_cluster_op` or the
`"Unsupported cluster operation"` string). This is a lie the type system is forced to tell: an
`Option` return type and a runtime error path for a case the control flow already makes impossible.

The compile-time guard the original draft claimed to *introduce* is **already present**: the
converter enumerates all 11 variants with no `_` arm (`util.rs:168-214`), so a new `RaftClusterOp`
variant is already a build error there. The only residual compile-time tightening from this refactor
is removing the `None` escape hatch — after the change, a new variant cannot be satisfied by mapping
it to `None` and silently inheriting the (now-deleted) runtime error path; it must yield a real
`ClusterCommand`. That is a genuine but modest improvement, not the "runtime failure → compile error"
transformation the draft oversold.

### `SlotMigrationKind` is a genuinely separate route, not a redundant converter shape

The candidate framed `SlotMigrationKind` as "a third parallel shape" alongside the converter. It is a
third *enum*, but it is **not** bridged by `convert_raft_cluster_op` and does not flow through
`handle_raft_command`. It routes to `SlotMigrationCoordinator` (`handle_slot_migration`,
`cluster.rs:151-169`), which builds `ClusterCommand::{Begin,Complete,Cancel}SlotMigration`
*internally* (`slot_migration/mod.rs:101,111,121`) and owns its own Raft commit + `ForwardToLeader`
handling. So it carries real added behaviour (coordinator orchestration), not just a redundant
mapping. Folding it into the same adapter is lower-value and is **de-scoped** here (see Alternatives).

### Duplicated propose-forward-redirect saga (overlaps Proposal 31)

Verified while tracing: the Raft-commit-and-forward orchestration in `handle_raft_command`
(`cluster.rs:96-142`) is duplicated byte-for-byte in `SlotMigrationCoordinator::commit`
(`slot_migration/mod.rs:129-160`) — its own doc-comment says *"with the same `ForwardToLeader`
handling as `handle_raft_command`."* That duplication is **Proposal 31's** territory (the
`ClusterWriter` propose-seam), not this one. 37 and 31 compose: 37 owns the *command-shape* seam, 31
owns the *write-path* seam. See Related.

## Evidence (verified file:line)

| Claim | Location | Status |
| --- | --- | --- |
| `convert_raft_cluster_op` returns `Option<ClusterCommand>` | `frogdb-server/crates/server/src/connection/util.rs:167` | CONFIRMED |
| `Failover` → `None` | `util.rs:211` | CONFIRMED |
| `ResetCluster` → `None` | `util.rs:213` | CONFIRMED |
| `Failover` built by hand in dispatch match arm | `frogdb-server/crates/server/src/connection/cluster.rs:50-59` | CONFIRMED (correction: inline match arm, **not** a pre-converter out-of-band block) |
| `ResetCluster` early-return for post-commit side effect | `cluster.rs:34-42` → `handle_reset_command` (`cluster.rs:176-225`); command built at `cluster.rs:197-200`; `set_self_node_id` at `cluster.rs:213-215` | CONFIRMED |
| `None` → `"ERR Unsupported cluster operation"` | `cluster.rs:62` | CONFIRMED |
| `RaftClusterOp` def, 11 variants, "cannot depend on core" | `frogdb-server/crates/protocol/src/response.rs:504-579` | CONFIRMED |
| `SlotMigrationKind` def, 3 variants | `frogdb-server/crates/protocol/src/response.rs:76-100` | CONFIRMED (correction: defined in `protocol`, not `connection/cluster.rs`) |
| `ClusterCommand` def, 14 variants | `frogdb-server/crates/cluster/src/types.rs:269-357` | CONFIRMED |
| `SlotMigrationKind` routes to coordinator, which builds the migration `ClusterCommand`s | `cluster.rs:151-169`, `slot_migration/mod.rs:101,111,121` | CONFIRMED |
| No test covers the converter or the error string | grep: 0 hits | CONFIRMED |
| **Dependency direction**: `protocol` and `cluster` are sibling leaf crates; neither depends on the other; `core` depends on both and re-exports (`pub use frogdb_cluster as cluster;`) | `frogdb-server/crates/protocol/Cargo.toml`, `frogdb-server/crates/cluster/Cargo.toml`, `frogdb-server/crates/core/src/lib.rs:10` | CONFIRMED |

### Corrected / weakened candidate claims

- **"A missed variant is a runtime `'Unsupported cluster operation'` instead of a compile error;
  add a `RaftClusterOp` variant, forget the converter, and it compiles clean then fails at
  runtime."** **False, and corrected.** `convert_raft_cluster_op` matches all 11 variants with no
  `_` arm (`util.rs:168-214`), so a new variant is *already* a compile error in the converter today.
  The headline compile-error-vs-runtime-string dichotomy is invalid. The honest residual benefit is
  only the removal of the `None` escape hatch (see Summary #1).
- **"The `None` fall-through fails silently and yields the runtime `'ERR Unsupported cluster
  operation'` string (`cluster.rs:62`), a live failure mode this removes."** **False, and
  corrected.** That branch is **unreachable dead code**. `ResetCluster` early-returns at
  `cluster.rs:34-42` and `Failover` is caught at its explicit arm (`cluster.rs:51`) before the
  `_ => convert_raft_cluster_op(&op)` fall-through, so the fall-through only sees the 9
  `Some`-returning variants and `None => error` can never fire (which is why it is untested). The
  refactor deletes a lie, it does not fix a bug. Motivation and severity are re-scoped to
  **cleanup** accordingly.
- **"AssignSlots lossily expands `Vec<u16>` into per-slot `SlotRange::single`, discarding range
  structure."** Partially true but **not load-bearing**, and de-scoped. The expansion happens at
  `util.rs:181` (and `RemoveSlots` identically at `util.rs:185` — the candidate omitted it). But the
  range structure was already gone *upstream*: `RaftClusterOp::AssignSlots` models slots as
  `Vec<u16>` (`response.rs:528`), and the wire `CLUSTER ADDSLOTS` command lists individual slots. More
  decisively, `apply_command` immediately flattens any `Vec<SlotRange>` into a **per-slot** map —
  `inner.slot_assignment: HashMap<u16, NodeId>`, filled via `range.iter()` (`frogdb-server/crates/cluster/src/commands.rs`,
  `AssignSlots` arm ~L62-84). So no semantic structure is lost; the only cost of N single-ranges vs
  coalesced ranges is a mildly larger serialized Raft-log entry. This is a minor efficiency nit, not a
  motivator, and is left as an optional follow-up.

## Proposed design (Rust interface sketch)

### 1. A total adapter — free function, not `From`

```rust
// frogdb-server/crates/server/src/connection/util.rs
// Total: every RaftClusterOp variant maps to a ClusterCommand. Exhaustive match,
// NO `_` arm — a new RaftClusterOp variant is a compile error here until mapped.
pub(crate) fn raft_op_to_command(op: &RaftClusterOp) -> ClusterCommand;
```

Why a free function and **not** `impl From<&RaftClusterOp> for ClusterCommand`: the orphan rule
forbids it. `RaftClusterOp ∈ protocol` and `ClusterCommand ∈ cluster` are **both foreign** to the
`server` crate, and neither owning crate can host the impl either — `protocol` and `cluster` are
sibling leaf crates with no dependency between them (verified Cargo.toml). The adapter's only legal
home is a crate that sees both, i.e. `server`, and there it must be a free function.

Why not "move the enum into `cluster` and construct it from the protocol layer" (candidate's primary
idea): that inverts the dependency graph. `protocol` is a lightweight RESP-parsing leaf; making it
depend on `cluster` would pull `openraft` + `rocksdb` into the wire-protocol crate. The
`RaftClusterOp` doc-comment codifies the current, correct direction. Both enums stay; only the
*adapter* deepens.

### 2. Fold `Failover` in; kill the caller's `None` branch

`raft_op_to_command` gains the `Failover` arm (moved verbatim from `cluster.rs:51-59`) and, to stay
total, a `ResetCluster` arm (`ClusterCommand::ResetCluster { node_id, new_node_id }`).
`handle_raft_command` loses its `match &op { Failover => …, _ => convert(…) }` wrapper and its
now-dead `None => "ERR Unsupported cluster operation"` branch, becoming:

```rust
// cluster.rs — ResetCluster still early-returns for its post-commit side effect and
// keeps its own ClusterCommand::ResetCluster literal in handle_reset_command (it never
// reaches this adapter call — see §migration step 3). The adapter's ResetCluster arm
// exists only so the match is total, not because this path routes through it.
if let RaftClusterOp::ResetCluster { node_id, new_node_id } = &op {
    return self.handle_reset_command(raft, *node_id, *new_node_id).await;
}
let cmd = raft_op_to_command(&op);   // total; no Option, no dead "Unsupported" string
```

`handle_reset_command` keeps its distinct post-commit orchestration (`set_self_node_id`, unregister
peers) **and its own `ClusterCommand::ResetCluster` construction** (`cluster.rs:197-200`). Note it
receives already-destructured params — `node_id: u64, new_node_id: Option<u64>` (`cluster.rs:179-180`),
not a `RaftClusterOp` — so routing it through `raft_op_to_command` would mean reconstructing a
`RaftClusterOp::ResetCluster` just to convert it straight back, an awkward round-trip for no gain.
Since `ResetCluster` keeps its out-of-band branch anyway, "single owner" for *its* construction is
cosmetic and **not pursued**: the two-line `ClusterCommand::ResetCluster { node_id, new_node_id }`
literal stays where it is. The genuine consolidation win is `Failover`, whose construction moves out
of the dispatch match and into the adapter. (If Proposal 31's `ClusterWriter` lands,
`handle_reset_command`'s duplicated propose-check collapses too.)

### 3. (Optional, deferred) coalesce slot runs

If the Raft-log-entry size is ever measured to matter, add
`fn coalesce_slots(slots: &[u16]) -> Vec<SlotRange>` in the adapter and use it for the `AssignSlots`
/ `RemoveSlots` arms. Pure, trivially unit-testable, no cross-crate type needed. Not required for the
core win.

## Migration plan (ordered)

1. Add `raft_op_to_command(&RaftClusterOp) -> ClusterCommand` next to (or replacing)
   `convert_raft_cluster_op`, with an exhaustive `match` and no `_` arm. Include the `Failover` and
   `ResetCluster` arms.
2. Update `handle_raft_command` (`cluster.rs`): drop the `Failover` match arm and the
   `Some/None → "Unsupported"` block; call `raft_op_to_command(&op)` after the `ResetCluster`
   early-return.
3. Leave `handle_reset_command` unchanged: it receives destructured `node_id`/`new_node_id`, not a
   `RaftClusterOp`, so its two-line `ClusterCommand::ResetCluster { .. }` literal (`cluster.rs:197-200`)
   stays local — routing it through `raft_op_to_command` would require reconstructing a
   `RaftClusterOp` just to convert it back. Its out-of-band branch persists for the post-commit side
   effect regardless.
4. Delete `convert_raft_cluster_op` (now unused) — a `cargo check` dead-code warning confirms no
   other caller.
5. Add the unit tests below (they did not exist before).
6. `just check server` / `just lint server` locally; full suite on the testbox.

Each step is compiler-guided; the exhaustive `match` errors until every variant is handled.

## Test plan

The mapping is currently untested. New synchronous, socket-free unit tests in `util.rs`'s test
module — no Raft, no Tokio:

- `raft_op_to_command_maps_every_variant`: one assertion per `RaftClusterOp` variant → expected
  `ClusterCommand` discriminant, **including** `Failover` and `ResetCluster` (previously the
  untested `None` cases). This is the regression guard the current design cannot express.
- `failover_maps_old_and_new_primary`: `RaftClusterOp::Failover { replica_id, primary_id, force }`
  → `ClusterCommand::Failover { old_primary_id: primary_id, new_primary_id: replica_id, force }`
  (pin the field cross-wiring that today lives in `cluster.rs:55-58`).
- `set_role_replica_vs_primary`: the `is_replica` bool → `NodeRole` mapping.
- Compile-time guard (documented, not a test): the exhaustive `match` already errors on an unmapped
  variant *today* (the converter has no `_` arm). The refactor's marginal addition is that, with no
  `Option` return, a new variant can no longer be discharged with `=> None`; it must produce a real
  `ClusterCommand`. The (dead) runtime `"Unsupported cluster operation"` path is deleted regardless.
- If step 3 (coalesce) is taken: `coalesce_slots([1,2,3,7,8]) == [1-3, 7-8]` and the singleton case.

Existing integration tests (`CLUSTER ADDSLOTS`, `CLUSTER FAILOVER`, `CLUSTER RESET`) must stay green,
confirming the total adapter is behaviour-preserving.

## Risks & alternatives

- **Orphan rule (design-governing).** `From<RaftClusterOp> for ClusterCommand` is impossible in
  `server`; the adapter must remain a free function. Do not "fix" this by adding a `protocol →
  cluster` or `cluster → protocol` dependency — both are wrong directions and one pulls
  consensus/storage into the RESP parser.
- **`ResetCluster` is not a pure adapter case.** Its post-commit side effect (`set_self_node_id`,
  peer unregister) genuinely belongs in `cluster.rs`, so the early-return stays. The proposal only
  claims its *command construction* joins the single owner — not that the branch disappears. Keeping
  this honest avoids over-selling "one seam."
- **Overlap with Proposal 31.** Both touch `handle_raft_command`. Land order does not matter, but if
  both land, sequence 37 (command shape) then 31 (write path) so 31 wraps the already-total
  `raft_op_to_command(&op)` call in `writer.propose(...)`. They are orthogonal seams on the same
  function.
- **Alternative — unify `SlotMigrationKind` too.** Rejected for this pass: it routes to the
  `SlotMigrationCoordinator`, which adds real orchestration, so collapsing it into
  `raft_op_to_command` would either drag coordinator logic into a pure adapter or lose the distinct
  route. Leave it; revisit only if the coordinator's Raft path merges with `handle_raft_command`
  (a Proposal-31 outcome).
- **Alternative — a single protocol enum (`RaftClusterOp` absorbs migration + reset).** Rejected:
  the migration ops have a different runtime destination; merging the *enums* would not merge the
  *routes* and would force `handle_raft_command` to re-split them anyway.
- **ADR-0001 (`frogdb-server/docs/adr/0001-raft-cluster-metadata.md`) untouched.** This is purely a
  refactor of how the **Raft Metadata Plane** write is *shaped* on the way in; the data path never
  goes through Raft, and this changes nothing about that. **Config Epoch** semantics are unchanged
  (`Failover`/`IncrementEpoch` map identically).

## Effort

**S (cleanup).** One new total function, deletion of one `Option`-returning function and one *dead*
caller branch, consolidation of the `Failover` construction (`ResetCluster` keeps its local literal),
and ~4 new synchronous unit tests. Behaviour-preserving — no live failure mode is being fixed, so
this competes for time against other cleanups on tidiness/testability grounds, not urgency. Blast
radius is two files in `server` (`connection/util.rs`, `connection/cluster.rs`); no cross-crate
signature changes, no async, no protocol/cluster Cargo edits. The compiler drives the migration. The
optional slot-coalescing (step 3) is a further S if ever pursued.

## Related

- **Proposal 31** (`31-cluster-propose-seam.md`) — the `ClusterWriter` propose-forward-redirect seam;
  both touch `handle_raft_command`. 37 makes the *command* total; 31 makes the *write* a single
  module. Complementary, compose cleanly.
- **Proposal 10** (`10-cluster-apply-owns-events.md`) — the mirror problem on the *apply* side: event
  derivation duplicated across a thin seam. Same shape (shallow interface forces the caller to
  re-derive), other end of the Raft state machine.

## Adversarial review

**Verdict: AMEND** — design sound and buildable, but the original justification was substantially
wrong. All issues verified against source and resolved in place; re-scoped from "bug fix" to
"S-effort cleanliness refactor."

- **[critical] "Missed variant → runtime string instead of compile error."** *Upheld against the
  draft; premise was false.* Verified: `convert_raft_cluster_op` (`util.rs:168-214`) enumerates all
  11 `RaftClusterOp` variants with **no `_` arm**, so adding a variant is already a compile error in
  the converter — you cannot "forget the converter and compile clean." **Resolved:** title and
  summary reframed; Summary correction #1 states the guard already exists and narrows the honest
  benefit to removing the `None` escape hatch; the false dichotomy is retracted in "Corrected /
  weakened candidate claims."

- **[major] `None` fall-through "fails silently" / is a live runtime failure mode.** *Upheld; the
  branch is unreachable dead code.* Verified: `ResetCluster` early-returns at `cluster.rs:34-42` and
  `Failover` is caught at its explicit arm (`cluster.rs:51`) before `_ => convert_raft_cluster_op`,
  so the fall-through only sees the 9 `Some`-returning variants and `cluster.rs:62` can never fire.
  **Resolved:** the Problem heading changed to "The `None` fall-through is unreachable dead code" and
  rewritten to trace both `None` arms to their interceptors; motivation/severity re-scoped to cleanup
  throughout (Summary, Effort, "Corrected claims").

- **[minor] Migration step 3 round-trip.** *Upheld.* Verified: `handle_reset_command` receives
  destructured `node_id: u64, new_node_id: Option<u64>` (`cluster.rs:179-180`), not a
  `RaftClusterOp`; routing it through `raft_op_to_command` would mean reconstructing a
  `RaftClusterOp::ResetCluster` to convert straight back. **Resolved:** step 3 rewritten to leave
  `handle_reset_command` unchanged (its literal stays local); design §2 and its code comment updated
  to note the adapter's `ResetCluster` arm exists only for totality, not because this path routes
  through it; Effort updated ("`ResetCluster` keeps its local literal").

- **[minor] Wrong path prefixes in prose.** *Upheld.* Verified crate layout: files live under
  `frogdb-server/crates/{server,protocol,cluster,core}/…`. **Resolved:** all crate-qualified path
  citations in prose, the Evidence table, and code comments corrected; short forms (`util.rs:NN`,
  `cluster.rs:NN`) retained as in-context shorthand and remain line-accurate.

Reviewer's closing note is accepted: the core mechanism (total free-function adapter in `server`) is
feasible and behaviour-preserving, and the orphan-rule / no-crate-move corrections are verified
correct; the proposal survives as a small net-positive cleanup, not a bug fix.
