# Proposal 34 — One `cluster_wire` renderer beside `ClusterSnapshot`; delete the disagreeing dead `to_cluster_nodes_line`

## Summary

CLUSTER NODES, CLUSTER SLOTS, and CLUSTER SHARDS each **independently** re-derive the same three
facts from a `ClusterSnapshot` — how to render a node id as hex, how to assemble a node's flag/role
label, and how to group a primary with its replicas — inside `frogdb-server/crates/server/src/commands/cluster/mod.rs`.
Meanwhile the `cluster` crate ships a *dead* `NodeInfo::to_cluster_nodes_line` that renders the same
CLUSTER NODES line a **fourth** way and disagrees with production on two observable columns: it emits
node ids as `{:016x}` (16 hex chars) where production emits `{:040x}` (40, Redis's true id width), and
it omits the `pong-recv` column entirely, producing a structurally malformed line. It is reachable
only from its own unit test. The one fact that *is* already deduplicated —
slot-range compaction — is centralized in `ClusterSnapshot::get_node_slots` (round-8 P02) and every
renderer correctly calls it; the rest was left copy-pasted.

This proposal introduces a single `cluster_wire` module in the `cluster` crate, beside
`ClusterSnapshot`, that owns id-width, flag/role assembly, migration-marker formatting, and
primary+replica grouping — emitting the CLUSTER NODES **text** directly and a neutral `ShardView`
data structure that the server maps to RESP (the RESP `Response` type lives in the protocol/server
layer, which the `cluster` crate must not depend on). The dead `to_cluster_nodes_line` is deleted.
Test win: golden-string / golden-struct assertions over a `ClusterSnapshot` fixture, with no live
cluster harness.

## Evidence (verified file:line)

All paths verified against the worktree at `/Users/nathan/workspace/workspace-3`.

### The dead renderer disagrees with production on two columns

`NodeInfo::to_cluster_nodes_line` — `frogdb-server/crates/cluster/src/types.rs:132-156`:

```rust
pub fn to_cluster_nodes_line(&self, myself: bool, slots: &[SlotRange]) -> String {
    let flags = if myself { format!("myself,{}", self.role) } else { self.role.to_string() };
    let primary_id = self.primary_id
        .map(|id| format!("{:016x}", id))            // 16 hex
        .unwrap_or_else(|| "-".to_string());
    let slots_str: Vec<String> = slots.iter().map(|r| r.to_string()).collect();
    format!(
        "{:016x} {}@{} {} {} 0 {} connected {}",     // <id> ...@<cport> <flags> <master> 0 <epoch> ...
        self.id, self.addr, self.cluster_addr.port(), flags, primary_id, self.config_epoch,
        slots_str.join(" ")
    )
}
```

- **Sole caller is its own test.** `grep to_cluster_nodes_line` across the tree returns exactly two
  hits besides the definition: `types.rs:625` (the test fn `test_node_info_to_cluster_nodes_line`)
  and `types.rs:632` (the call inside it). Zero production callers. **Fails the deletion test.**
- **Id width disagreement is real.** `NodeId = u64` (`types.rs:14`). This method renders ids as
  `{:016x}`; production `cluster_nodes` renders `{:040x}` (`mod.rs:340, 375, 395`), as do MYID
  (`mod.rs:407`), SLOTS (`mod.rs:447, 457, 477`) and SHARDS (`mod.rs:519, 550, 593`). Redis node ids
  are 40 hex chars (160-bit); the whole rest of the codebase pads the u64 to 40. The dead method
  would emit a 16-char id — a latent Redis-parity landmine if it were ever promoted.
- **Missing column.** The Redis CLUSTER NODES line is
  `<id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots…>`
  (documented in the header comment at `mod.rs:301`). The dead method's format string has a **single**
  `0` between `<master>` and `<config-epoch>` — it emits `ping-sent` but drops `pong-recv`, shifting
  every following column. Production emits `0 0` (both, `mod.rs:375`). The dead line is malformed.
- **Incomplete flag assembly.** It only ever renders `myself,<role>` (`role` Display →
  `"master"`/`"slave"`, verified `types.rs:45-50`); it cannot express `fail`, `fail?`, `handshake`,
  or `noaddr`, all of which production assembles (`mod.rs:319-330`).

### Three production renderers re-derive the same facts

`frogdb-server/crates/server/src/commands/cluster/mod.rs`:

- **`cluster_nodes` (L300-401)** hand-rolls: a flag vector (L308-335), `{:040x}` for id and master-id
  (L340, L375), migration-marker strings `[slot->-{:040x}]` / `[slot-<-{:040x}]` (L358-371), and the
  full line via `format!` (L374-385). Slot ranges come from `snapshot.get_node_slots(node.id)` (L344).
- **`cluster_slots` (L411-483)** re-derives primary grouping by building its **own**
  `HashMap<u64, Vec<(u16,u16)>>` keyed by `primary_id` (L418-432) — then discards the accumulated
  `_slots` and re-calls `snapshot.get_node_slots(node_id)` anyway (L438), so the hand-rolled grouping
  loop is dead work. It re-renders `{:040x}` (L447, L457) and re-nests replicas by scanning
  `snapshot.nodes.values()` for `primary_id == Some(node_id)` (L452-460).
- **`cluster_shards` (L486-611)** independently groups: `filter(is_primary)` + `sort_by_key(id)`
  (L492-494), `get_node_slots` (L499), `{:040x}` (L519, L550), the **same** replica-nesting scan
  (L541-542), and a health string derived from `flags.fail`/`flags.pfail` twice (L531-537 primary,
  L562-568 replica).

The replica-nesting predicate `replica.primary_id == Some(primary.id)` is copy-pasted in both SLOTS
(L453) and SHARDS (L542); the `{:040x}` id rendering appears at **12** sites in this one file.

### The one fact already deduplicated — and its second consumer

`ClusterSnapshot::get_node_slots` (`frogdb-server/crates/cluster/src/types.rs:514-540`) owns slot-range compaction;
`ClusterState::get_node_slots` (`frogdb-server/crates/cluster/src/state.rs:119-121`) is a thin delegator to it. A round-8
follow-up already routed the debug UI through it: `frogdb-server/crates/server/src/debug_providers.rs:123-131` calls
`snapshot.get_node_slots(node.id)` with a comment noting it is "the same routine" rather than a
re-implementation. So compaction is the *precedent* — the rendering concerns (hex width, flags,
markers, grouping) are what remain fragmented.

### No hidden downstream coupling to the wire text

- `frogctl/src/commands/cluster.rs` drives cluster admin over structured RPC (its subcommands are
  clap enums; no `CLUSTER NODES`/`SLOTS`/`SHARDS` text parsing — grep confirms none).
- The integration tests that exercise these commands assert only shape, not exact columns:
  `test_cluster_nodes_command` (`integration_cluster.rs:152-166`) calls `parse_cluster_nodes` and
  asserts non-empty; `test_cluster_slots_command` (L182-194) and `test_cluster_shards_command`
  (L196-208) assert `!is_error`. None pins id width or column order, so a consolidation that fixes the
  dead method's divergence cannot silently break a golden test — but it also means **nothing currently
  guards the wire format**, which the test plan below closes.

### ADR / glossary alignment

- **ADR-0001** (`frogdb-server/docs/adr/0001-raft-cluster-metadata.md`): the Raft Metadata Plane owns
  topology/roles/Config Epoch; the data path never goes through Raft. This proposal is pure rendering
  of an already-consensus'd `ClusterSnapshot` — it touches neither Raft nor the data path.
- **Glossary** (`frogdb-server/CONTEXT.md`): uses **Hash Slot** (0–16383, CRC16), **Config Epoch**,
  **Primary/Replica**, **Raft Metadata Plane**. The renderer maps FrogDB's Primary/Replica to Redis's
  wire tokens `master`/`slave` in exactly one place (today `NodeRole`'s Display does this at
  `types.rs:48-49`; the consolidated renderer keeps that mapping as the single translation seam).

## Proposed design (Rust interface sketch)

A new module `frogdb-server/crates/cluster/src/wire.rs` (re-exported from `frogdb-server/crates/cluster/src/lib.rs`), sited beside
`ClusterSnapshot`. It depends only on `cluster` types (`ClusterSnapshot`, `NodeInfo`, `NodeFlags`,
`NodeRole`, `SlotRange`, `NodeId`) — **no** dependency on `frogdb-protocol` or `server`, preserving
crate direction. It emits the NODES **text** and neutral view structs; the server maps view structs
to `Response`.

```rust
// frogdb-server/crates/cluster/src/wire.rs

/// Canonical Redis node-id width. NodeId is u64; Redis ids are 40 hex chars.
pub fn format_node_id(id: NodeId) -> String;              // "{:040x}" — the single owner

/// Assemble the CLUSTER NODES flag token, e.g. "myself,master,fail".
pub fn node_flags_label(node: &NodeInfo, myself: bool) -> String;

/// Redis-wire health token for CLUSTER SHARDS ("online" | "loading" | "fail").
pub fn node_health(flags: &NodeFlags) -> &'static str;

/// Full CLUSTER NODES payload (text). Owns id width, flags, master-id,
/// migration markers, slot compaction (delegates to get_node_slots), link-state.
pub fn render_cluster_nodes(snapshot: &ClusterSnapshot, myself: NodeId) -> String;

/// Neutral, protocol-agnostic view of one shard (primary + its replicas + slots),
/// grouped and sorted once. The server maps this to RESP for SLOTS and SHARDS,
/// overlaying per-node data it alone owns (replication offset).
pub struct ShardView<'a> {
    pub slots: Vec<SlotRange>,
    pub primary: NodeView<'a>,
    pub replicas: Vec<NodeView<'a>>,
}

pub struct NodeView<'a> {
    pub id: NodeId,
    pub node: &'a NodeInfo,          // ip/port/cport/flags/role read by the caller
    pub health: &'static str,        // node_health(&node.flags)
}

/// Group the snapshot into shards (primaries sorted by id, replicas nested).
/// Single source for the primary/replica grouping SLOTS and SHARDS both need.
pub fn shard_views(snapshot: &ClusterSnapshot) -> Vec<ShardView<'_>>;
```

Server side, the three command fns collapse to thin adapters:

```rust
// frogdb-server/crates/server/src/commands/cluster/mod.rs (sketch — signatures/shape only)
fn cluster_nodes(ctx) -> Result<Response, CommandError> {
    let text = frogdb_cluster::wire::render_cluster_nodes(&snapshot, my_id);
    Ok(Response::bulk(Bytes::from(text)))
}

fn cluster_slots(ctx) -> Result<Response, CommandError> {
    // map shard_views(&snapshot) -> nested Response::Array (start,end,[ip,port,id],replicas…)
}

fn cluster_shards(ctx) -> Result<Response, CommandError> {
    // map shard_views(&snapshot) -> Response::Array, overlaying replication-offset per node
    //   (offset stays server-side: it reads ctx.raft / ctx.replication_tracker)
}
```

Replication offset (`local_replication_offset`, `mod.rs:27-41`) stays in the server — it reads Raft
metrics / the replication tracker, which the `cluster` crate does not and must not see. The
`ShardView` carries only topology; the server overlays `replication-offset` when mapping to RESP.

Standalone-mode fallbacks (`mod.rs:391-399, 468-482, 584-610`) are constructed by building a
single-primary `ClusterSnapshot` (or a tiny standalone helper in `wire.rs`) and running the same
renderer, so the standalone and clustered paths stop diverging too.

## Migration plan (ordered steps)

1. **Add `wire.rs` with `format_node_id` + `node_flags_label` + `node_health`**, unit-tested against
   fixtures. Re-export from `lib.rs`. No call-site changes yet.
2. **Add `render_cluster_nodes`** reproducing today's `cluster_nodes` output byte-for-byte (40-hex
   ids, `0 0` ping/pong, migration markers, `get_node_slots`). Add a golden test asserting the exact
   line for a fixed fixture (this is the format guard that does not exist today).
3. **Rewire `cluster_nodes`** (`mod.rs:300-401`) to call `render_cluster_nodes`; delete the hand-rolled
   body and the standalone fallback duplication.
4. **Add `shard_views` + `ShardView`/`NodeView`**; unit-test grouping/sorting/replica-nesting on a
   fixture (primaries sorted by id, replicas nested under their primary).
5. **Rewire `cluster_slots` and `cluster_shards`** to map `shard_views`, overlaying replication offset
   server-side. Delete the ad-hoc `HashMap` grouping (`mod.rs:418-432`) and the duplicated replica
   scans. **`shard_views` enumerates every primary in the snapshot, including primaries that own zero
   slots; today's `cluster_slots` only ever emits primaries that own at least one slot (it discovers
   primaries by iterating `snapshot.slot_assignment`, so a zero-slot primary never enters its `node_slots`
   map and is never emitted).** The SLOTS mapper must therefore explicitly filter out any `ShardView`
   whose `slots` is empty before converting to `Response::Array` entries, to match current behavior.
   (`cluster_shards` already includes zero-slot primaries in its own primary listing today, so the
   SHARDS mapper needs no such filter.)
6. **Delete `NodeInfo::to_cluster_nodes_line`** (`types.rs:132-156`) and its test
   (`types.rs:624-635`). Its assertions (`contains("myself,master")`, `contains("0-5460")`) are
   subsumed by the `render_cluster_nodes` golden test.
7. `just check cluster`, `just check server`, then `just test cluster` and the cluster integration
   suite (`just test frogdb-server cluster`).

Steps are compiler-guided: each rewire is a local swap, and the delete in step 6 fails to compile only
its own test, which is removed in the same step.

## Test plan

- **`wire.rs` unit tests (synchronous, no harness):**
  - `format_node_id(1) == "000…0001"` (40 chars) — pins the width that the dead method got wrong.
  - `node_flags_label` for each combination: `myself` primary → `"myself,master"`; replica with
    `fail` → `"slave,fail"`; handshake/noaddr/pfail (`fail?`) tokens present; empty → `"noflags"`
    (matches `mod.rs:331-335`).
  - `node_health`: online / loading (pfail) / fail.
  - `render_cluster_nodes` golden string over a `ClusterSnapshot` fixture with a primary, a replica,
    and one migration in each direction — asserts the exact line incl. `@cport`, `0 0` columns,
    `[slot->-id]` / `[slot-<-id]` markers, and compacted slot ranges.
  - `shard_views`: two primaries + replicas → correct grouping, primaries sorted by id, replicas
    nested, slot ranges compacted.
- **Server mapping tests:** small tests over `cluster_slots` / `cluster_shards` mapping a fixture
  `ShardView` to RESP, asserting structure and that per-node `replication-offset` is overlaid for the
  local node only (mirrors `mod.rs:512-516, 543-547`).
- **Existing integration tests** (`test_cluster_nodes_command`, `_slots_`, `_shards_`) stay green
  unchanged — behavior is byte-preserved except the now-deleted dead method.
- **Regression guard:** the round-trip `parse_cluster_nodes` in the integration suite continues to
  parse `render_cluster_nodes` output.

## Risks & alternatives

- **Byte-for-byte parity is the whole risk — with one known, deliberate exception: CLUSTER SLOTS shard
  ordering.** The migration must preserve current CLUSTER NODES/SLOTS/SHARDS output exactly (the format
  guard golden test lands in step 2, *before* the rewire in step 3), *except* that today's `cluster_slots`
  groups primaries via a `HashMap<u64, Vec<(u16,u16)>>` (`mod.rs:418-432`) and then iterates that map
  (`mod.rs:435`) to build the response — HashMap iteration order is not guaranteed stable across runs, so
  the current shard emission order in CLUSTER SLOTS is already nondeterministic. `shard_views` sorts
  primaries by id (deterministic), so the proposed SLOTS output changes shard *order* relative to today.
  This is not a parity break to guard against; it is latent nondeterminism being fixed, and should be
  called out as an intentional, low-risk behavior change rather than something the golden test needs to
  reproduce byte-for-byte. Redis clients are expected to treat CLUSTER SLOTS/SHARDS as an unordered set of
  shards, so this has no known client-compatibility impact. The only intended behavioral change beyond
  this is deleting the divergent dead method, which no production path reaches.
- **Crate-direction boundary.** RESP `Response` lives in the protocol/server layer; `wire.rs` must not
  import it. Mitigated by having `wire.rs` emit text (NODES) + neutral `ShardView` structs (SLOTS/
  SHARDS), with RESP shaping staying in `server`. This is the deliberate seam — the `cluster` crate
  owns *what the topology renders to*, the server owns *how it becomes a protocol reply* and injects
  server-only data (replication offset).
- **Lifetime on `ShardView<'a>`/`NodeView<'a>`.** Borrowing `&NodeInfo` from the snapshot avoids
  clones; if the borrow proves awkward at the call site (the snapshot is a short-lived local), fall
  back to owning the few scalar fields the mapper reads. Minor.
- **Alternative — leave SLOTS/SHARDS alone, only fix NODES + delete the dead method.** Smaller (S), but
  leaves the `{:040x}` rendering and replica-nesting duplicated across three fns and misses the
  grouping-dedup leverage. The `ShardView` step is where most of the locality win is; recommend doing
  it, but it can be split into a follow-up if scope must shrink.
- **Alternative — promote `to_cluster_nodes_line` instead of deleting.** Rejected: it is malformed
  (missing `pong-recv`) and 16-hex, so promoting it means fixing it to match production anyway — at
  which point it is easier to write the consolidated `render_cluster_nodes` and delete the old one.
- **Wider id-hex scattering (out of scope, noted).** `{:040x}` node-id rendering also appears in
  `admin/handlers.rs:109`, `commands/info.rs:400,421`, `info/mod.rs:329` (master_replid). Those are a
  separate identity-rendering concern; `format_node_id` gives them a future home but this proposal does
  not rewire them.

## Effort

**M.** One new module in the `cluster` crate (~4 small functions + 2 view structs) plus rewiring three
command fns in one server file and deleting one dead method. No async, no cross-crate signature churn
beyond a new public `wire` module, and the compiler drives every swap. Not S because `ShardView`
grouping + the RESP mapping for SLOTS/SHARDS need care to preserve byte parity; not L because it is two
files, no data-path or Raft involvement, and behavior is preserved.

## Related

- **Round-8 P02** (`02-node-state-snapshot.md`) — established `ClusterSnapshot::get_node_slots` as the
  single slot-range compaction routine and routed `debug_providers.rs` through it (verified
  `debug_providers.rs:123-131`). This proposal extends that same "one owner beside `ClusterSnapshot`"
  pattern from *compaction* to *rendering*.
- **Proposal 10** (`10-cluster-apply-owns-events.md`) — the same fragmentation shape in the `cluster`
  crate: a fact (variant→event there, render-format here) duplicated across a seam and kept consistent
  by hand until the copies drift.
- **Proposal 12** (`12-snapshot-coordinator-surface-trim.md`) — precedent for deleting a
  deletion-test-failing member (`to_cluster_nodes_line`) whose only caller is its own test.

## Adversarial review

**Verdict: CONFIRMED.** All three flagged issues were minor and have been resolved in place; the core
premise and crate-direction call are sound.

1. **"Byte-for-byte parity" imprecise for CLUSTER SLOTS.** Today's `cluster_slots` groups primaries via
   a `HashMap<u64, Vec<(u16,u16)>>` (`frogdb-server/crates/server/src/commands/cluster/mod.rs:418`) and
   iterates it (`mod.rs:435`) to emit shards — HashMap iteration order is not guaranteed, so current
   SLOTS shard ordering is already nondeterministic. `shard_views` sorts primaries by id, so the
   proposed output changes ordering relative to today. *Resolution:* the Risks section now states this
   explicitly — it is latent nondeterminism being fixed by the consolidation, not a parity regression,
   and the golden test is not expected to pin shard order for SLOTS.
2. **`shard_views` over-enumerates relative to today's SLOTS.** `shard_views` walks every primary in the
   snapshot, including primaries with zero slots; today's `cluster_slots` only discovers primaries by
   iterating `snapshot.slot_assignment`, so a zero-slot primary is never emitted. *Resolution:* Migration
   step 5 now explicitly calls out that the SLOTS mapper must filter out any `ShardView` with an empty
   `slots` vec before converting to `Response::Array`, to preserve current behavior (SHARDS needs no such
   filter — it already includes zero-slot primaries today, verified at `mod.rs:492-494`).
3. **Path shorthand.** The proposal wrote `cluster/src/...` and `server/src/...`; the real paths are
   `frogdb-server/crates/cluster/src/...` and `frogdb-server/crates/server/src/...`. *Resolution:* all
   such references throughout the document (Summary, Evidence, and the interface sketch) are now
   normalized to full repo-relative paths; line numbers were already exact and are unchanged.

**Reviewer notes (summary):** the premise is valid and every load-bearing claim was independently
verified — the dead `to_cluster_nodes_line`, the 16-vs-40-hex id-width mismatch, the dropped
`pong-recv` column, the three hand-rolled renderers in `mod.rs`, the 12 `{:040x}` call sites, and the
`get_node_slots` dedup precedent all check out against the tree. The `cluster` crate owning
migrations/flags/roles/rendering with no dependency on the protocol layer is the right direction. No
conflict with ADR-0001 or the `frogdb-server/CONTEXT.md` glossary was found.
