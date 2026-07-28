# Proposal 35 — Split the fused `ClusterRpcRequest` so `handle_rpc_request` cannot carry dead app-RPC arms

## Summary

`ClusterRpcRequest` (`cluster/src/network.rs:92-108`) is one flat enum holding both the openraft
consensus RPCs (`AppendEntries`, `Vote`, `InstallSnapshot`) **and** the application RPCs
(`ForwardedWrite`, `PubSubBroadcast`, `PubSubForward`, `HealthProbe`). Dispatch is split across two
owners in two crates: the cluster bus (`server/src/cluster_bus.rs:155-168`) explicitly peels
`PubSubBroadcast` / `PubSubForward` / `HealthProbe` and services them locally, then a catch-all
`raft_request => handle_rpc_request(...)` forwards everything else. `handle_rpc_request`
(`network.rs:655-703`) then re-lists `PubSubBroadcast | PubSubForward` and `HealthProbe` as arms
that return an error string — `"PubSub RPCs must be handled by the cluster bus, not the Raft
handler"` — arms that are **unreachable in production**, kept alive only by the flat enum's
exhaustiveness requirement. The compiler *does* force each variant to be handled somewhere (see the
no-wildcard note below), so the seam is not purely stringly-enforced; but the classification of
*which* handler owns *which* variant is expressed only by arm order, a `// All Raft RPCs` comment,
and the runtime error strings — none of them checked by types.

This proposal wraps the wire enum into a two-arm envelope (`Raft(RaftRpc)` / `Bus(BusRpc)`) so the
subset each handler owns is a distinct type. `handle_rpc_request` takes only `RaftRpc`, making its
exhaustive match cover exactly the variants it can service — the three dead arms and the runtime
error string are deleted, and mis-routing an app RPC into the Raft handler becomes impossible to
express. The one non-obvious constraint (verified below) governs the design: `parse_rpc_message`
deserializes exactly **one** type from a frame, so the split cannot be two independent top-level
enums — the wire needs a single envelope with a discriminant, which the nested design provides.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fused design |
| --- | --- | --- |
| `frogdb-server/crates/cluster/src/network.rs` | 852 | `ClusterRpcRequest` enum (L92-108, 7 variants); `ClusterRpcResponse` enum (L111-132, symmetric); `handle_rpc_request` (L655-704) with the 3 dead arms (L695-702); construct sites: `health_probe` (L349), `forward_write` (L363), `RaftNetwork` impl (L506/532/561) |
| `frogdb-server/crates/server/src/cluster_bus.rs` | 255 | `handle_connection` dispatch (L155-168): peels `PubSubBroadcast`/`PubSubForward`/`HealthProbe`, catch-all `raft_request => handle_rpc_request` (L166-167); disconnect classified by string (L139-142) |
| `frogdb-server/crates/server/src/cluster_pubsub.rs` | — | Constructs `PubSubBroadcast` (L160) and `PubSubForward` (L206), sends via `network.send_rpc` (L164, L215) |
| `frogdb-server/crates/server/src/failure_detector.rs` | — | `net.health_probe()` (L359) — the only `HealthProbe` producer |
| `frogdb-server/crates/server/src/connection/cluster.rs` | — | `net.forward_write(cmd)` (L106) — a `ForwardedWrite` producer |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | — | `net.forward_write` (L404, L471) — `ForwardedWrite` producers |
| `frogdb-server/crates/server/src/slot_migration/mod.rs` | — | `net.forward_write` (L144) — `ForwardedWrite` producer |
| `testing/fuzz/fuzz_targets/cluster_bus_postcard.rs` | 8 | `postcard::from_bytes::<ClusterRpcRequest>` — decodes the top-level wire type |
| `testing/fuzz/fuzz_targets/rpc_parse.rs` | 8 | `serde_json::from_slice::<ClusterRpcRequest>` — decodes the top-level wire type |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | 16 | Embedded Raft owns metadata; the data path never goes through Raft. Untouched by this proposal. |
| `frogdb-server/CONTEXT.md` | — | Glossary: **Raft Metadata Plane** (L27), **Config Epoch** (L33), **Scatter-Gather** (L107) |

## Problem (concrete verified evidence)

### One enum, two handlers, three dead arms

`ClusterRpcRequest` (`network.rs:92-108`) is flat:

```rust
pub enum ClusterRpcRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),   // consensus (openraft)
    Vote(VoteRequest<NodeId>),                          // consensus (openraft)
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),// consensus (openraft)
    ForwardedWrite(ClusterCommand),                     // app: write proposal to leader
    PubSubBroadcast { channel: Vec<u8>, message: Vec<u8> }, // app: pub/sub fan-out
    PubSubForward   { channel: Vec<u8>, message: Vec<u8> }, // app: sharded pub/sub
    HealthProbe,                                        // app: failover scoring
}
```

Every frame off the wire is parsed to this one type (`parse_rpc_message`, `network.rs:720-731`),
then dispatched in `cluster_bus.rs:155-168`:

```rust
let response = match request {
    ClusterRpcRequest::PubSubBroadcast { channel, message } => handle_pubsub_broadcast(..).await,
    ClusterRpcRequest::PubSubForward   { channel, message } => handle_pubsub_forward(..).await,
    ClusterRpcRequest::HealthProbe => ClusterRpcResponse::HealthProbeResponse { .. },
    // All Raft RPCs (AppendEntries, Vote, InstallSnapshot, ForwardedWrite)
    raft_request => handle_rpc_request(&ctx.raft, raft_request).await,
};
```

The bus is the **single ingress**: it classifies each request into "handle locally" (the three
app RPCs it peels) versus "hand to the Raft handler" (the catch-all). But because the catch-all
passes a value of the *whole* enum type, `handle_rpc_request` (`network.rs:655-703`) must still
match all seven variants — so it re-lists the three the bus already consumed, as arms that can only
be reached on a contract violation:

```rust
ClusterRpcRequest::PubSubBroadcast { .. } | ClusterRpcRequest::PubSubForward { .. } => {
    ClusterRpcResponse::Error(
        "PubSub RPCs must be handled by the cluster bus, not the Raft handler".to_string(),
    )
}
ClusterRpcRequest::HealthProbe => {
    ClusterRpcResponse::Error("HealthProbe must be handled by cluster bus".to_string())
}
```

**These arms are dead in production.** The only caller of `handle_rpc_request` is
`cluster_bus.rs:167`, and it is reached only *after* the three app variants have been peeled by the
preceding arms. The sole workspace call site is confirmed by grep: `handle_rpc_request` is
referenced only in the re-export lists (`cluster/src/lib.rs:54`, `core/src/lib.rs:60`) and at
`cluster_bus.rs:167`. So the "must be handled by the cluster bus" strings can never be produced by
any real dispatch — they exist purely to satisfy the flat enum's exhaustiveness.

### The handler-ownership classification is stringly-enforced (though the *presence* of a handler is not)

Be precise about what the type system does and does not guarantee today. Because
`handle_rpc_request` has **no wildcard arm**, the flat enum's exhaustiveness *does* force every
variant to be handled somewhere — adding an eighth variant and forgetting it produces a compile
error, not a silent miss. What is *not* type-checked is the finer decision of **which handler owns
which variant**; that classification is encoded three ways, none checked by the compiler:

1. The **order of arms** in `cluster_bus.rs:155-168` (peel-then-catch-all).
2. The `// All Raft RPCs (AppendEntries, Vote, InstallSnapshot, ForwardedWrite)` comment
   (`cluster_bus.rs:166`).
3. The runtime error strings in `handle_rpc_request`'s dead arms (`network.rs:696-701`).

Add an eighth variant — say a future `SlotStatsProbe` serviced by the bus — and nothing forces you
to peel it in `cluster_bus.rs`. Forget to, and it silently falls into the catch-all, reaches
`handle_rpc_request`, hits its wildcard-free match... which today has **no** wildcard, so it would
actually be a *compile* error in `handle_rpc_request` — but the fix a developer reaches for under
compile pressure is "add another dead arm returning an error string," deepening the exact smell
this proposal removes. The type system is being used to force a match, but on the wrong (too wide)
type, so the forced match produces dead code instead of preventing the mistake.

### ForwardedWrite is an *app* RPC that lives with the consensus handler — the seam is "needs the Raft handle," not "is a Raft protocol message"

A subtle but load-bearing fact for the design: the catch-all comment calls `ForwardedWrite` a "Raft
RPC," but it is not an openraft protocol message. It is an application-level write proposal —
`forward_write` (`network.rs:362`) wraps a `ClusterCommand`, and `handle_rpc_request`
(`network.rs:672-694`) services it via `raft.client_write(cmd)`, extracting `AddNode` info and
spawning `spawn_add_raft_voter`. It groups with the consensus RPCs **because it needs the
`ClusterRaft` handle**, not because it is consensus traffic. The bus-local RPCs
(`PubSubBroadcast`/`PubSubForward`/`HealthProbe`) group together because they need the *bus
context* — `shard_senders`, `num_shards`, `node_id`, `replication_offset`
(`cluster_bus.rs:32-47`) — and never touch Raft. So the real seam is **"requires the Raft handle"
vs "requires the bus/shard context,"** which is exactly the split `handle_rpc_request` vs the bus
match already draw by hand. Naming the two subsets after their handler (not after "Raft protocol")
keeps `ForwardedWrite` correctly on the Raft-handle side.

### Wire constraint: one frame deserializes to exactly one type

`parse_rpc_message` (`network.rs:720-731`) does `postcard::from_bytes(&frame)` into a single
`ClusterRpcRequest`; `send_rpc` (`network.rs:376-391`) serializes a single `ClusterRpcRequest`.
Both fuzz targets decode the top-level type by name (`cluster_bus_postcard.rs:6`,
`rpc_parse.rs:6`). This means the split **cannot** be "two independent top-level enums serialized
directly" — the receiver would not know which of two types a frame is. The wire needs one envelope
with a discriminant. postcard encodes enum discriminants as a leading varint in declaration order,
so a nested `Raft(RaftRpc)` / `Bus(BusRpc)` envelope adds one outer tag byte and re-numbers the
inner variants — a wire-format change. FrogDB is pre-production and both ends share the same type
(no cross-version frames in flight), so this is acceptable; it is called out here because it is the
one thing that makes "just split the enum" wrong.

### Adjacent: the response enum has the same fusion (symmetric, lower-severity)

`ClusterRpcResponse` (`network.rs:111-132`) mirrors the request: consensus responses
(`AppendEntries`/`Vote`/`InstallSnapshot`) fused with app responses
(`ForwardedWrite`/`PubSubBroadcastResult`/`PubSubForwardResult`/`HealthProbeResponse`) plus a
catch-all `Error(String)`. This is less acute — each caller (`health_probe` L350,
`forward_write` L364, the three `RaftNetwork` impls L510/536/565) already matches its one expected
variant with a `_ => Err(unexpected)`, so there are no *dead* arms, only a wide type each caller
under-uses. A symmetric response split is an optional follow-on, not required for the request fix.

## Why it is shallow/fragmented (architecture vocabulary)

**The Interface (`handle_rpc_request`'s parameter type) is wider than the Module's real domain.** A
deep module presents a narrow interface over the states it actually handles. `handle_rpc_request`'s
domain is "RPCs serviced through the `ClusterRaft` handle," but its parameter is `ClusterRpcRequest`
— a type that also names three RPCs it explicitly *cannot* handle. The mismatch between the
interface type and the module's domain is paid for in dead arms: surface area that exists only to
say "not me." By Ousterhout's deletion test, the two PubSub/HealthProbe arms fail — delete them and
no production behavior changes (they are unreachable) — yet they cannot be deleted while the
parameter type is the wide enum. That is the signature of an interface fitted to the wrong type.

**The Seam is duplicated and stringly-enforced.** The Raft-vs-bus boundary is one decision, but it
is expressed in three unchecked places (arm order, a comment, runtime error strings) across two
crates. Good **Locality** would put the classification in exactly one place the compiler can hold
to completeness. Today the classification lives in `cluster_bus.rs`'s match *and* is re-asserted
defensively in `handle_rpc_request`; the two must agree by hand. Narrowing the handler's parameter
to `RaftRpc` collapses this to a single seam — the envelope match in the bus — and lets the
compiler prove the handler covers its subset with no leftover arms.

**Low Leverage: a match forced on the wrong type.** Rust's exhaustiveness is high-leverage — it
turns "did you handle every case?" into a compile error, and (no wildcard) it already does its job:
every variant is handled somewhere. The narrower complaint is that the leverage is aimed at the
flat enum, so it forces `handle_rpc_request` to *mention* three variants it should never receive.
The exhaustiveness still fires, but it fires on a too-wide type, so the "fix" it demands is a dead
arm rather than correct routing. Pointing the same exhaustiveness at a `RaftRpc` subset that
contains *only* handleable variants keeps the guarantee while removing the dead surface:
"exhaustiveness by construction" — the handler's match is complete precisely because the type
cannot express the app RPCs, and the reflexive "add another dead error arm" fix becomes
unrepresentable. The safety improvement is real but modest; the concrete win is the deletion of the
dead arms plus relocating the forced match to the handler that can actually satisfy it.

**Adapter placement.** The bus is the correct **Adapter** between the wire and the two handlers; it
is where classification belongs. The dead arms are a second, redundant adapter (wire-variant →
error string) grafted onto the Raft handler. Removing them leaves one adapter (the bus envelope
match) doing the whole job.

## Proposed design (Rust interface sketch — types/signatures only)

Wrap the wire enum into a two-arm envelope; give each handler its own subset type. Keep the name
`ClusterRpcRequest` for the envelope to minimize churn in re-exports and the fuzz targets.

```rust
// cluster/src/network.rs

/// RPCs serviced through the `ClusterRaft` handle: the openraft consensus trio
/// plus the application write-forward (which reaches the leader via client_write).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpc {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    ForwardedWrite(ClusterCommand),
}

/// RPCs serviced locally from the cluster-bus context (shard senders, node id,
/// replication offset) — never touch Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusRpc {
    PubSubBroadcast { channel: Vec<u8>, message: Vec<u8> },
    PubSubForward   { channel: Vec<u8>, message: Vec<u8> },
    HealthProbe,
}

/// Wire envelope. One discriminant selects the handler; the payload is that
/// handler's exhaustive subset. Serialized/parsed as today (single type per frame).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterRpcRequest {
    Raft(RaftRpc),
    Bus(BusRpc),
}

/// Now takes only the subset it can service. The match becomes exhaustive with
/// no dead arms and no runtime "wrong handler" strings.
pub async fn handle_rpc_request(raft: &ClusterRaft, request: RaftRpc) -> ClusterRpcResponse;
```

Construction adapts at the (few) producer sites: `RaftNetwork::append_entries/vote/install_snapshot`
build `ClusterRpcRequest::Raft(RaftRpc::AppendEntries(..))` etc.; `forward_write` builds
`Raft(RaftRpc::ForwardedWrite(cmd))`; `health_probe` builds `Bus(BusRpc::HealthProbe)`; the pub/sub
sites build `Bus(BusRpc::PubSubBroadcast { .. })`. Ergonomic `From<RaftRpc>`/`From<BusRpc> for
ClusterRpcRequest` keep those call sites terse.

The bus dispatch reduces to one envelope match — the classification now lives in exactly one place:

```rust
// server/src/cluster_bus.rs
let response = match request {
    ClusterRpcRequest::Bus(b)  => handle_bus_rpc(ctx, b).await,   // pub/sub + health probe
    ClusterRpcRequest::Raft(r) => handle_rpc_request(&ctx.raft, r).await,
};
```

where `handle_bus_rpc(ctx, BusRpc) -> ClusterRpcResponse` owns the three local arms
(`PubSubBroadcast`/`PubSubForward`/`HealthProbe`) exhaustively — the same bodies that live inline
in `cluster_bus.rs:156-165` today, just moved behind a subset type so *that* match is also
exhaustive-by-construction.

**Note on `ClusterResponse` naming:** these are the *transport* RPC types (`cluster` crate), not the
state-machine `ClusterResponse { Ok, Value, Error }` from `types.rs`. This proposal touches only the
transport layer; the Raft state machine and `apply_command` (see proposal 10) are untouched.

**Optional symmetric follow-on (defer):** mirror the split on `ClusterRpcResponse`
(`RaftRpcResponse` / `BusRpcResponse` envelope). Lower value — no dead arms today — so keep it a
separate, later step to hold this proposal's blast radius to the request path.

## Migration plan (ordered steps)

1. **Introduce the subset types** `RaftRpc` and `BusRpc` alongside the existing flat enum; add
   `From<RaftRpc>`/`From<BusRpc> for ClusterRpcRequest`. No behavior change yet.
2. **Reshape `ClusterRpcRequest` into the `Raft`/`Bus` envelope.** This is the wire-format change;
   the compiler flags every construct and match site.
3. **Narrow `handle_rpc_request`'s parameter to `RaftRpc`** and delete the two dead arms
   (`network.rs:695-702`) and their error strings. The match now covers exactly
   `AppendEntries`/`Vote`/`InstallSnapshot`/`ForwardedWrite`.
4. **Extract `handle_bus_rpc(ctx, BusRpc)`** in `cluster_bus.rs` from the three inline arms; rewrite
   `handle_connection`'s dispatch as the two-arm envelope match.
5. **Update the producers** (`RaftNetwork` impls, `forward_write`, `health_probe`,
   `cluster_pubsub.rs:160/206`) to build enveloped values via the `From` shims.
6. **Update the fuzz targets** — no code change needed (they decode `ClusterRpcRequest` by name; the
   envelope reshape is transparent), but re-run them against the new layout to confirm no panics.
7. **`just fmt` + `just check cluster` + `just check server`**, then the cluster/turmoil suites on a
   testbox (`just tb-run`). The turmoil build compiles out `handle_connection`
   (`cluster_bus.rs:87` is `#[cfg(not(turmoil))]`), so verify both feature configurations build.

Each step after (1) is compiler-driven: the reshape makes every affected site a type error until
updated, so there is no silent-miss window.

## Test plan

- **Exhaustiveness-by-construction is the headline win — it is a *compile-time* guarantee, not a
  test.** After step 3, `handle_rpc_request` physically cannot name a PubSub/HealthProbe variant;
  attempting to route one there is unrepresentable. The "dead arm" class of bug is removed by
  types, so no runtime test is needed to guard it.
- **Serialization round-trip:** extend `test_all_rpc_variants_roundtrip` (`network.rs:780-850`) to
  build each variant through the envelope (`Raft(RaftRpc::AppendEntries(..))`,
  `Bus(BusRpc::PubSubBroadcast { .. })`, etc.) and assert postcard + serde_json round-trip. This
  pins the new wire layout.
- **Bus dispatch routing:** a unit test on `handle_bus_rpc` asserting each `BusRpc` variant produces
  the right `ClusterRpcResponse` (e.g. `HealthProbe -> HealthProbeResponse { node_id, offset }`),
  socket-free.
- **Regression on the two fuzz targets:** run `cluster_bus_postcard` and `rpc_parse` briefly to
  confirm the reshaped enum decodes arbitrary bytes without panic.
- **Integration:** existing cluster pub/sub, failover (health probe), and write-forwarding tests
  are the end-to-end guard that routing still works across the real bus; run the cluster suite on a
  testbox. No new integration test is required — the change is a type-level refactor with identical
  runtime behavior.

## Risks & alternatives

- **Wire-format change (the main risk).** Nesting re-numbers postcard discriminants. Acceptable
  because FrogDB is pre-production and both ends share the type (no in-flight cross-version frames),
  but it means a cluster running mixed old/new binaries during a rolling restart would fail to
  decode peer frames. Land it as a single atomic change; do not straddle a release boundary. This
  is consistent with the project's "breaking changes fine" stance.
- **Alternative A — narrow the parameter only, keep the flat wire enum.** Instead of an envelope,
  keep `ClusterRpcRequest` flat on the wire and add `TryFrom<ClusterRpcRequest> for RaftRpc`; the
  bus peels the three bus variants (handling them) and converts the rest. This avoids the
  wire-format change entirely (zero on-wire impact) and still narrows `handle_rpc_request` to
  `RaftRpc`. Cost: the `TryFrom` conversion becomes the single place that re-lists the bus variants
  as "not convertible" — moving the dead-arm knowledge into one boundary instead of erasing it.
  That is strictly better than today (one owner, not two, and it is a *conversion* boundary where a
  rejection is legitimate rather than an unreachable handler arm), and it is lower-risk. **Recommend
  A as the ship-first option if the wire change is undesirable now**, with the envelope (primary
  design) as the fuller fix. Both achieve the core goal: `handle_rpc_request` can no longer carry
  app-RPC arms.
- **Alternative B — three-way envelope** (`Consensus(ConsensusRpc)` / `WriteForward(ClusterCommand)`
  / `Bus(BusRpc)`), separating `ForwardedWrite` from the openraft trio. Cleaner taxonomy (consensus
  vs write-proposal vs local), but `handle_rpc_request` services both consensus and write-forward
  through the same `ClusterRaft` handle, so the two-way split already matches the handler boundary.
  B adds a variant without removing a handler. Defer.
- **Response-enum split deferred.** Keeping the request and response splits separate holds blast
  radius; the response enum has no dead arms, so the value is lower.
- **Turmoil build.** `handle_connection` is `#[cfg(not(turmoil))]`; confirm the enveloped types
  compile under both feature sets (the subset types themselves are unconditional).

## Effort

**S–M.** The type introduction and handler narrowing are mechanical and compiler-guided: add two
subset enums + envelope, delete two dead arms, extract `handle_bus_rpc`, update ~7 construct sites
via `From` shims (plus ~6 in-crate test construct sites at `network.rs:770/784/795/819/825/831`),
and extend two serialization tests. Confined to `cluster/src/network.rs` and
`server/src/cluster_bus.rs` (plus trivial construct-site edits in `cluster_pubsub.rs`,
`failure_detector.rs`, `cluster/src/network.rs` producers). The wire-format reshape and the
dual-feature (turmoil) build check are what lift it above pure-S. Alternative A (narrow-only, no
wire change) is a clean **S**.

**Net LOC honesty.** This is *not* a pure deletion. Ousterhout's deletion test justifies removing
the ~8 lines of dead arms, but the envelope design *adds* two enums, an envelope layer, two `From`
impls, an extracted `handle_bus_rpc`, and reshaped construct/test sites — net LOC almost certainly
*increases*. The payoff is structural (a single typed seam, exhaustiveness-by-construction), not a
line count reduction, and the safety delta over today is modest (§"Low Leverage"). The cost/benefit
is therefore a genuine judgment call, defensible under the project's "breaking changes fine,
cleanups encouraged" stance rather than an unambiguous win. **Given that modest benefit, prefer
landing Alternative A first** (`TryFrom<ClusterRpcRequest> for RaftRpc`, zero wire impact, clean
**S**): it achieves the core goal — `handle_rpc_request` can no longer carry app-RPC arms — without
the wire reshape, the two new enums, or the test-site churn. Reserve the full envelope for when the
wire reshape buys something A does not (e.g. a later three-way taxonomy, Alternative B).

## Related

- **Backlog: "cluster-bus stringly disconnect classification"** — *adjacent, fold in.* The same file
  and the same anti-pattern: `handle_connection` classifies peer disconnects by substring-matching
  the error message (`cluster_bus.rs:139-142`, `error_msg.contains("connection closed") ||
  error_msg.contains("connection reset") || error_msg.contains("broken pipe")`), the reader-side
  twin of the dead arms' `Error(String)` producer-side. Both encode a control decision (which
  handler / clean-vs-dirty disconnect) as string identity rather than a type. A companion cleanup
  would give `ClusterError` a typed `Disconnected`/`ConnectionReset` variant (or match on
  `io::ErrorKind`) so the bus loop branches on a variant, not a substring. It shares this proposal's
  seam (`cluster/src/network.rs` error surface ↔ `cluster_bus.rs` dispatch), and both are best done
  together while the RPC transport types are already open. Recommend tracking as a sub-task of this
  proposal.
- **Proposal 10 (cluster-apply-owns-events)** — sibling in the `cluster` crate but disjoint surface:
  10 concerns the Raft *state machine* (`apply`/`apply_command`, `ClusterResponse`); this concerns
  the *transport* (`ClusterRpcRequest`, `handle_rpc_request`). No overlap; can land independently.
- **Proposal 28 (psync-handoff-typed)** — same "stringly-typed sentinel `Response` → typed dispatch
  outcome" theme in a different subsystem; cite as prior art for the pattern.
- **ADR-0001 (raft-cluster-metadata)** — untouched. This is a transport-layer type refactor; the
  data-path-never-through-Raft and Raft-owns-metadata decisions are unchanged.

## Adversarial review

**Verdict: CONFIRMED** — sound as written; the two issues raised are precision/emphasis and
cost-benefit framing, not design defects. Every cited file:line was independently opened and matches
the proposal (flat 7-variant `ClusterRpcRequest` at `network.rs:92-108`; bus peel-then-catch-all at
`cluster_bus.rs:155-168`; the two dead arms with verbatim error strings and no wildcard at
`network.rs:695-702`; sole call site at `cluster_bus.rs:167`, grep-confirmed unreachable; the
single-type-per-frame postcard constraint at `network.rs:720-731`). Design is feasible under the
borrow checker (all types in the `cluster` crate — no orphan/dep-direction issue), no hot-path cost,
no conflict with rounds 1-9 or ADR-0001, and `ForwardedWrite` is correctly kept on the Raft-handle
side.

- **Issue 1 (minor) — "seam enforced only by a string + comment, not types" overstates the
  problem.** *Resolved.* The reviewer is right that because `handle_rpc_request` has no wildcard,
  the flat enum's exhaustiveness already forces every variant to be handled *somewhere* — so the
  seam is not purely stringly-enforced, and the "safety feature into a dead-code generator" framing
  overstates it. Reworded the summary ("The seam is not purely stringly-enforced…"), retitled and
  re-scoped the problem subsection ("The handler-ownership *classification* is stringly-enforced,
  though the presence of a handler is not"), and toned the "Low Leverage" section to state the win
  precisely: the genuine gains are (a) deleting the two unreachable arms and (b) relocating the
  forced-exhaustiveness to the handler that can satisfy it, making the reflexive "add another dead
  error arm" fix unrepresentable — a real but modest safety improvement.

- **Issue 2 (minor) — "S–M mechanical cleanup" understates the cost; net LOC increases.**
  *Resolved.* The deletion test only justifies removing the ~8 dead lines; the envelope adds two
  enums, an envelope layer, two `From` impls, an extracted `handle_bus_rpc`, ~7 producer + ~6
  in-crate test construct sites, and a wire-format change — net LOC almost certainly rises. Added a
  "Net LOC honesty" paragraph to the Effort section stating this plainly, framing the payoff as
  structural rather than line-count, calling the cost/benefit a genuine judgment call under the
  project's "cleanups encouraged" stance, and strengthening the existing nudge to **land
  Alternative A (TryFrom, zero wire impact, clean S) first**, reserving the full envelope for when
  the wire reshape buys something A does not.
