# Proposal 36 — Extract the even-slot bootstrap partition into one pure `cluster` function

## Summary (2-3 sentences)

Bootstrap slot assignment computes the same "split 0-16383 evenly across N nodes, remainder to the
last" partition **twice** — once for the local seed (`cluster_init.rs:337-358`, `apply_local`) and
again in the Raft-replication spawn (`cluster_init.rs:434-493`, `client_write`) — kept in sync only
by a hand-written comment ("Match the inclusive ranges used by the Raft replication path below").
Any edit to one arithmetic block silently diverges the bootstrap node's `ClusterState` from what the
followers receive through the Raft Metadata Plane, producing overlapping or ownerless slots. This
proposal extracts a pure `even_slot_ranges(node_ids: &[NodeId]) -> Vec<(NodeId, SlotRange)>` into the
`frogdb-cluster` crate, consumed by both paths, and pins it with a partition-correctness unit test
that needs no Raft, async, or server boot.

## Problem (concrete verified evidence)

Bootstrap must give the bootstrap node and every follower the **same** slot ownership. But the two
converging paths derive it independently:

- The local seed writes directly into the bootstrap node's `ClusterState` via `apply_local`
  (`cluster_init.rs:355`), because that is the only `ClusterState` instance the bootstrap node holds
  at this point.
- Followers hold **separate** `ClusterState` instances (comment at `cluster_init.rs:429-430`), so a
  spawned task re-proposes the identical `AssignSlots` commands through `raft.client_write`
  (`cluster_init.rs:452`) for Raft log replication.

Convergence depends entirely on the two blocks computing byte-identical ranges. Today that is
enforced by a comment, not by the compiler.

**The formula is duplicated verbatim.** Local seed (`cluster_init.rs:337-358`):

```rust
let node_ids: Vec<u64> = initial_members.keys().copied().collect();
let num_nodes = node_ids.len();
let slots_per_node = 16384 / num_nodes;
for (i, &nid) in node_ids.iter().enumerate() {
    // Match the inclusive ranges used by the Raft replication path
    // below so the bootstrap node and followers converge on the same
    // slot ownership.
    let start = (i * slots_per_node) as u16;
    let end = if i == num_nodes - 1 {
        16383u16 // Last node gets remainder
    } else {
        ((i + 1) * slots_per_node - 1) as u16
    };
    let cmd = frogdb_core::cluster::ClusterCommand::AssignSlots {
        node_id: nid,
        slots: vec![frogdb_core::cluster::SlotRange::new(start, end)],
    };
    if let Err(e) = cluster.apply_local(cmd) { /* warn */ }
}
```

Raft replication spawn (`cluster_init.rs:434-493`) recomputes exactly the same `num_nodes`,
`slots_per_node`, `start`, and `end` (`:436-445`) to build the identical `AssignSlots` command
(`:446-449`).

**The comment is the only coupling.** The verbatim phrase "Match the inclusive ranges used by the
Raft replication path below" (`cluster_init.rs:342-344`) is a load-bearing invariant expressed in
English. Change `16383u16` to a `CLUSTER_SLOTS - 1` constant, fix an off-by-one, or switch to a
ceil-division split in one block and forget the other, and the bootstrap node's local `ClusterState`
disagrees with the followers' replicated state — the ownerless / double-owned slot bug class. The
`AssignSlots` apply arm (`commands.rs:62-84`) rejects a slot already owned by a *different* node with
`SlotAlreadyAssigned`, so a divergence surfaces as a silently-dropped assignment (logged
`warn!`/`Failed to replicate`, never surfaced to a client), leaving slots unowned — `CLUSTERDOWN`
territory.

**Iteration order is safe today, and the extraction preserves that.** Both blocks derive `node_ids`
from `initial_members.keys().copied().collect()` (`:337`, `:434`), and `initial_members` is a
`BTreeMap<u64, openraft::BasicNode>` (`cluster_init.rs:198`), so both produce the same node_id-sorted
`Vec` — position `i` maps to the same node in both paths. This ordering assumption is currently
**implicit and unverified**; folding both call sites onto one function that takes an ordered
`&[NodeId]` makes "same order in, same partition out" explicit and impossible to break in only one
place.

**The two blocks have already drifted in their guards.** The local seed is gated on
`should_bootstrap && !initial_members.is_empty() && !cluster.all_slots_assigned()`
(`cluster_init.rs:336`); the Raft replication block drops the `!all_slots_assigned()` term
(`cluster_init.rs:431`). The surrounding conditions are not identical, which is exactly how the
*arithmetic* could drift next.

**No test pins the partition.** `frogdb-server/crates/server/tests/integration_cluster.rs` exercises `CLUSTER
SLOTS` / `CLUSTER NODES` / MOVED / ASK behaviour but never asserts the bootstrap boundaries (e.g. 3
nodes → `0-5460`, `5461-10921`, `10922-16383`). The `cluster` crate's own tests
(`frogdb-server/crates/cluster/src/state.rs`) drive `AssignSlots` with hand-picked ranges but never the even-split
derivation. So the duplicated arithmetic is covered by nothing that would catch a one-sided edit.

## Evidence (verified file:line)

| Claim | Location | Verified |
| --- | --- | --- |
| Local seed even-split loop, `apply_local` | `frogdb-server/crates/server/src/server/cluster_init.rs:337-358` | Yes |
| "Match the inclusive ranges…" coupling comment | `frogdb-server/crates/server/src/server/cluster_init.rs:342-344` | Yes |
| Raft replication spawn recomputes identical arithmetic, `client_write` | `frogdb-server/crates/server/src/server/cluster_init.rs:434-493` (formula `:436-445`, `client_write` `:452`) | Yes |
| `initial_members: BTreeMap<u64, BasicNode>` (deterministic order) | `frogdb-server/crates/server/src/server/cluster_init.rs:198` | Yes |
| Guards already differ (`!all_slots_assigned()` only on local path) | `:336` vs `:431` | Yes |
| `SlotRange { start, end }` + `new`/`single`/`contains`/`len`/`iter` | `frogdb-server/crates/cluster/src/types.rs:189-228` | Yes |
| `CLUSTER_SLOTS: u16 = 16384` (the `16383u16` / `16384` literals reference this) | defined `frogdb-server/crates/cluster/src/types.rs:10`; re-exported via `frogdb-server/crates/core/src/lib.rs:10` (`pub use frogdb_cluster as cluster`) | Yes |
| `type NodeId = u64` | `frogdb-server/crates/cluster/src/types.rs:14` | Yes |
| `AssignSlots` rejects cross-node overlap with `SlotAlreadyAssigned` | `frogdb-server/crates/cluster/src/commands.rs:62-84` | Yes |
| `frogdb_core::cluster` is a re-export of `frogdb_cluster` | `frogdb-server/crates/core/src/lib.rs:9-10` | Yes |
| Only two occurrences of `slots_per_node` arithmetic workspace-wide | grep `slots_per_node` → `cluster_init.rs:339,437` only | Yes |
| No test pins bootstrap partition boundaries | `frogdb-server/crates/server/tests/integration_cluster.rs` (no boundary asserts) | Yes |
| ADR: data path never goes through Raft; metadata only | `frogdb-server/docs/adr/0001-raft-cluster-metadata.md:7-8` | Yes |

**Candidate line-number correction (recorded in evidence_discrepancies):** the candidate cited the
local block at `334-364` and the Raft block at `431-495`. The actual arithmetic loops are
`337-358` (local) and `434-493` (Raft); `334-336` is the guarding comment/condition and `359-364`
is the trailing `info!`. The candidate also gave the path as `server/src/server/cluster_init.rs`;
the repo path is `frogdb-server/crates/server/src/server/cluster_init.rs`. The core premise —
duplicated even-slot arithmetic across two async paths synced by a comment — is confirmed.

## Why it is shallow/fragmented (architecture vocabulary)

**A domain algorithm with no home Module.** "Partition 0..CLUSTER_SLOTS evenly across an ordered set
of nodes" is a pure, total function of `&[NodeId]` — it belongs *inside* the `cluster` crate next to
`SlotRange` and `CLUSTER_SLOTS`, the types it produces. Instead it is inlined in a server-crate
bootstrap function, twice. There is no **seam** a caller can call; there is only a shape to copy. The
knowledge has no owner the compiler can hold to consistency.

**Locality is inverted.** The two derivations of one fact live 96 lines apart in one file and are
kept equal by a prose instruction. A reader must diff two loops by eye to trust convergence. Pulling
the derivation behind one **interface** collapses both copies to a single call site each and makes
the partition a value both paths *share*, not a procedure both paths *re-run*.

**The interface is too thin.** `apply_local` / `client_write` accept one `AssignSlots` command at a
time, so the caller is forced to own the loop that generates them. A pure
`even_slot_ranges(&[NodeId]) -> Vec<(NodeId, SlotRange)>` raises the abstraction: the caller asks for
*the partition* and forwards it, rather than reconstructing it. This is the deep-module move — a
small interface (`node_ids -> assignments`) over the arithmetic that is currently smeared across the
caller.

**Deletion test.** Delete one arithmetic block and the other still compiles and runs; nothing links
them. Add a fourth node-count edge case (e.g. guard `num_nodes > CLUSTER_SLOTS`, which today
underflows `((i+1)*slots_per_node - 1)` at `usize` when `slots_per_node == 0`) and you must remember
to add it in both places. A single owner makes the edge case fixable — and testable — once.

**How Redis/Valkey/DragonflyDB frame it.** Redis Cluster does *not* auto-partition on bootstrap at
all: `redis-cli --cluster create` computes the even split **client-side** in a single helper
(`ClusterManager::assign_slots` / `clusterManagerAssignSlotsToNodes` in `redis-cli.c`) and issues
`CLUSTER ADDSLOTS`; the server never owns the arithmetic, and it exists exactly once. Valkey inherits
that same single-owner helper. DragonflyDB takes slot ranges from its external orchestrator rather
than deriving them. FrogDB's embedded-Raft model (ADR-0001) means the *node* bootstraps itself, so
the arithmetic legitimately lives server-side — but the lesson from all three is the same: the even
split is **one** function, computed **once**. FrogDB currently computes it twice; this proposal
restores the single-owner shape those systems already have.

## Proposed design (Rust interface sketch)

One new pure function in the `cluster` crate (no new dependency; `SlotRange`, `NodeId`,
`CLUSTER_SLOTS` already live there):

```rust
// frogdb-server/crates/cluster/src/types.rs (or a new frogdb-server/crates/cluster/src/slots.rs), re-exported via lib.rs

/// Partition all `CLUSTER_SLOTS` hash slots as evenly as possible across `node_ids`,
/// in the given order, assigning any remainder to the last node.
///
/// Returns one `(NodeId, SlotRange)` per node, covering `0..CLUSTER_SLOTS` exactly once
/// with disjoint, contiguous ranges. Order of `node_ids` is significant and preserved:
/// callers must pass a deterministically ordered slice (bootstrap uses BTreeMap order).
///
/// Returns an empty `Vec` for empty input. `node_ids.len()` must be `<= CLUSTER_SLOTS`
/// (a cluster cannot have more primaries than slots); callers already guarantee a small
/// odd member count, so this is a `debug_assert`, not a runtime error.
pub fn even_slot_ranges(node_ids: &[NodeId]) -> Vec<(NodeId, SlotRange)>;
```

Signature notes (no implementation here):

- Returns `Vec<(NodeId, SlotRange)>`, not `Vec<ClusterCommand>` — it stays a *data* function with no
  dependency on the command enum, so it is trivially unit-testable and reusable (e.g. a future
  `CLUSTER RESET` / rebalance path). Each call site maps the pairs into its own
  `AssignSlots { node_id, slots: vec![range] }`.
- Takes `&[NodeId]` (a slice), forcing the caller to have already materialized and ordered the ids —
  which both call sites do via `initial_members.keys().copied().collect()`. Order-in equals
  partition-out, made explicit.
- Total and pure: same input always yields the same output, no I/O, no async, no `ClusterState`.

Both call sites in `cluster_init.rs` collapse to:

```rust
let node_ids: Vec<NodeId> = initial_members.keys().copied().collect();
for (nid, range) in frogdb_core::cluster::even_slot_ranges(&node_ids) {
    let cmd = ClusterCommand::AssignSlots { node_id: nid, slots: vec![range] };
    // local:  if let Err(e) = cluster.apply_local(cmd) { warn!(...) }
    // raft:   raft_clone.client_write(cmd.clone()).await  (retry loop unchanged)
}
```

The comment "Match the inclusive ranges used by the Raft replication path below" is deleted — the
match is now structural.

## Migration plan (ordered steps)

1. **Add the pure function first, test-first.** Write `even_slot_ranges` in the `cluster` crate with
   the partition-correctness unit tests (below) before touching either call site. This is a pure
   addition; nothing depends on it yet.
2. **Rewrite the local seed** (`cluster_init.rs:337-358`) to call `even_slot_ranges` and map each
   pair to `apply_local`. Preserve the `!cluster.all_slots_assigned()` guard at `:336`.
3. **Rewrite the Raft replication spawn** (`cluster_init.rs:434-493`) to call `even_slot_ranges` and
   map each pair into the existing 30-attempt `client_write` / `forward_write` retry loop. The retry
   and forward logic is unchanged — only range derivation moves.
4. **Delete the coupling comment** (`:342-344`) and the now-duplicated `num_nodes` / `slots_per_node`
   / `start` / `end` locals in both blocks.
5. **`just check frogdb-cluster` then `just check frogdb-server`**; run `just test frogdb-cluster`
   for the new unit tests. Full `just test` (cluster integration) on a Blacksmith testbox per the
   remote-execution policy.

No public protocol, RESP, or on-disk format changes; no ADR touched (data path stays off Raft; this
is metadata-plane bootstrap only).

## Test plan

Pure unit tests in the `cluster` crate (no Raft, no async, no server boot) — the core win:

- **Partition correctness (the headline test).** For representative `num_nodes` (1, 2, 3, 5, 16, and
  a non-divisor like 7): every slot in `0..CLUSTER_SLOTS` is covered **exactly once** (build a
  `[bool; 16384]` / count map from the returned ranges and assert all set, none twice), ranges are
  **disjoint and contiguous**, and the **remainder lands on the last node** (last range ends at
  `CLUSTER_SLOTS - 1`).
- **Order preservation.** `even_slot_ranges(&[10, 20, 30])` and a reordered `&[30, 10, 20]` assign
  the same *ranges* in slice order (node at index 0 always gets `0..`), pinning the "position i →
  node i" contract both call sites rely on.
- **Exact boundaries for the common case.** 3 nodes → `[(_, 0..=5460), (_, 5461..=10921), (_,
  10922..=16383)]`; 1 node → `0..=16383`. These are the values a regression in either block would
  change.
- **Degenerate input.** Empty slice → empty `Vec`. (`num_nodes > CLUSTER_SLOTS` guarded by
  `debug_assert`; document that callers never hit it.)
- **Cross-path equivalence (regression guard for the original bug).** Assert that mapping
  `even_slot_ranges(&ids)` to `AssignSlots` yields the identical command sequence a "local" and a
  "raft" caller would send — trivially true once both share the function, but the test documents the
  invariant the comment used to assert.

Existing `integration_cluster.rs` cluster tests remain the end-to-end coverage that bootstrap still
assigns all 16384 slots and MOVED/ASK still work; they should stay green unchanged.

## Risks & alternatives

- **Behavioural parity is exact by construction.** The extracted function reproduces the current
  arithmetic verbatim (floor division, remainder to last, inclusive ends), so no observable bootstrap
  behaviour changes. The boundary unit test locks the current values in before the refactor, so any
  accidental arithmetic change during extraction is caught synchronously.
- **Crate-direction check.** The function lives in `frogdb-cluster` alongside `SlotRange` /
  `CLUSTER_SLOTS` / `NodeId` and depends on nothing new; `core` re-exports `frogdb_cluster as
  cluster`, and `server` already imports these types via `frogdb_core::cluster`. No new edge in the
  dependency graph, and no reach into `core` internals (`persistence`/`replication`/`cluster` layering
  respected).
- **Alternative — helper on `ClusterState`.** Could hang the derivation off `ClusterState` (which
  already has range-coalescing in `get_node_slots`, `types.rs:514`). Rejected: the Raft replication
  path runs in a spawned task that does *not* hold the target `ClusterState`, and coupling a pure
  partition to a stateful type needlessly narrows reuse. A free function is the deeper, more
  reusable seam.
- **Alternative — leave it, tighten the comment.** Rejected: a comment cannot be enforced by the
  compiler or a test; the whole point is to remove the hand-sync.
- **Guard asymmetry is deliberately left in place.** This proposal unifies only the *arithmetic*; it
  does **not** reconcile the surrounding guards (local keeps `!all_slots_assigned()` at `:336`, the
  Raft spawn omits it at `:431`). That asymmetry is cited above as evidence of how the code drifts, so
  a reader might expect the fix to touch it too — but it is out of scope and safe to leave: re-proposing
  an already-satisfied assignment is idempotent (the `AssignSlots` arm's same-owner check accepts a
  re-assignment to the current owner rather than erroring), so the missing `!all_slots_assigned()` term
  on the Raft path only ever re-sends commands that no-op. Folding the guards together would change the
  bootstrap control flow and belongs in a separate change; the arithmetic seam here is the minimal,
  compiler-enforceable win.
- **Latent edge case surfaced, not introduced.** Today `num_nodes > CLUSTER_SLOTS` underflows
  `((i+1)*slots_per_node - 1)` at `usize` (`slots_per_node == 0`). The extraction gives that edge a
  single place to be guarded (`debug_assert`), documented as unreachable for real clusters (odd
  member count, ≥3, far below 16384). Not fixing runtime behaviour here, just noting the win.

## Effort

**S.** One pure function (~15 lines) plus its unit tests in the `cluster` crate, and two mechanical
call-site rewrites in `cluster_init.rs` that *delete* more than they add. No signature changes to any
existing type, no async or protocol changes, blast radius two files (one new/edited in `cluster`, one
edited in `server`), compiler-guided.

## Related

None.

## Adversarial review

**Verdict: CONFIRMED.** The reviewer attempted to refute the premise and it survived. Both arithmetic
blocks (`cluster_init.rs:337-358` local `apply_local`, `434-493` Raft `client_write` spawn) were
verified byte-identical even-split code synced only by the `:342-344` comment; `grep` confirmed
`slots_per_node` arithmetic exists in exactly those two blocks. `NodeId`/`SlotRange`/`CLUSTER_SLOTS`
all live in the `cluster` crate and are re-exported via `core`, so the pure
`even_slot_ranges(&[NodeId]) -> Vec<(NodeId, SlotRange)>` adds no crate edge and reaches no internals
(crate direction correct, borrow-checker/async trivial). No wire/RESP/on-disk/ADR impact
(metadata-plane bootstrap only). `initial_members` is a `BTreeMap` consumed identically by both sites,
so order-preservation is real. The deletion test passes and the `S` effort estimate is accurate. No
blocking issues; only path/line-precision nits and a scope-clarification suggestion.

Issues raised (all minor) and resolution:

1. **CLUSTER_SLOTS evidence row cited the re-export site, not the definition** — Fixed. The evidence
   row now cites the definition `frogdb-server/crates/cluster/src/types.rs:10` and separately notes the
   `frogdb-server/crates/core/src/lib.rs:10` re-export.
2. **Design/evidence sections used shortened `crates/...` paths** — Fixed. All `crates/server`,
   `crates/cluster`, `crates/core` references now carry the correct `frogdb-server/crates/...` prefix.
   The evidence_discrepancies note (which had already disclosed the shortening) is retained.
3. **Guard asymmetry left unreconciled by the "divergence" fix** — Addressed. Added a Risks bullet
   stating the guard difference (`!all_slots_assigned()` on the local path only) is intentionally out
   of scope, and why it is safe: re-proposing an already-satisfied assignment is idempotent because the
   `AssignSlots` arm rejects only a slot owned by a *different* node (verified `commands.rs:68-73`,
   `existing_owner != node_id`), so the missing guard term only re-sends no-op commands. Reconciling
   the guards would change bootstrap control flow and belongs in a separate change.
