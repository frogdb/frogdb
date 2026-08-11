# Proposal 84 — `BlockingOp` / `Direction`: one concept, two declarations, and a 52-line adapter that is the identity function

Round 38 · lane: protocol / net / core · candidate **PN6** · effort **S** · **no locked crate
edited**, **no `FM-` tag inside any edited region**, **no seam lint constrains the change**

**Rev 2** — revised after adversarial review (verdict **AMEND**: direction, home, R2 rejection,
serialization proof, spec clearance and both hotfixes stand; **one headline claim was false**). See
§Revision ledger for applied / refuted / withdrawn.

**Verified at HEAD `ee5efee9018889c437c85cc53d4bf1f5d3722821`** (worktree `arch-round-38-99`,
branch `main`). Every file:line below was re-derived at this SHA — and **every cite was re-derived
a second time during rev 2**, which found a −4 offset across the `core/src/shard/blocking.rs`
XREAD cluster and ten other stale line numbers. Nothing is inherited from the lane brief.
Concurrent authors hold `frogdb-server/crates/protocol/src/response.rs` (proposal 80),
`frogdb-server/crates/core/src/shard/wait_queue.rs` (proposal 81) — **which rev 1 wrongly declared
read-only for this proposal; 84 edits four of its test lines** — and
`frogdb-server/crates/server/src/connection/util.rs` (proposal 86). All three edges are declared in
§Risks.

## Corrections to the lane brief (and to proposal 81's forward reference)

| Claim | Verified at HEAD |
|---|---|
| Brief: "`types` depends on `protocol` already … **Sol: StreamId into/re-export from protocol**, `after_ids: Vec<StreamId>`" | The dependency direction is right; the solution is **rejected**. `StreamId`'s inherent impl (`types/src/types/stream.rs:20-156`) reads the clock seam at `:126` (`crate::clock::system_now()`) and returns three companion types (`StreamRangeBound` `:74`, `StreamIdSpec` `:96`, `StreamIdParseError`). Moving it drags the wall-clock seam and three types into a leaf wire crate. §Proposed change (R2) rejects it with the cost, and unifies on the **tuple** representation instead. |
| **Proposal 81 §"vs future proposal 84"**: "84 will fold `frogdb-protocol`'s copies **into `frogdb-types`' copies**" | **Backwards, and impossible.** `frogdb-protocol/Cargo.toml` declares **no `frogdb-*` dependency at all** — it is the graph leaf. `frogdb-types/Cargo.toml:14` declares `frogdb-protocol.workspace = true`. Folding toward `types` requires `protocol → types`, which cycles. The fold goes the other way: **`types`' copies collapse into `protocol`'s.** 81's *conclusion* ("no ordering constraint") survives the correction unchanged. |
| Orchestrator's framing: "`frogdb-types` is the likely home (only shared dep)" | **Falsified by the same two Cargo.toml lines.** `frogdb-types` is not shared by `frogdb-protocol`; `frogdb-protocol` is shared by everyone including `frogdb-types`. Home = **`frogdb-protocol`**. |
| Brief: effort **M** ("crate graph") | **S**, but the original justification was **wrong and is corrected below.** The M rating assumed the ~164 `BlockingOp` mentions would churn. They do not: `frogdb-types` keeps the *names* it exports (`types/src/lib.rs:65`) by re-exporting protocol's, and `frogdb-core/src/lib.rs:7` (`pub use frogdb_types::*;`) forwards them unchanged. What is **not** true is "zero use-site edits" — the `after_ids` representation change forces **six one-line construction edits plus one orphaned import**, all mechanical, all in test modules or import lists. §The `after_ids` churn ledger, and §Effort. |

Two findings the brief did not name: **`frogdb_types::Direction::parse` has zero callers**
(§Problem 3 — a dead duplicate parser), and the 52-line adapter has **zero direct tests**
(§Testability).

One brief-adjacent claim is **retracted here rather than repeated**: this duplication is *not* a
correctness hazard. Both `match`es in the adapter are exhaustive over the protocol enum, so a
variant added to either copy is a **compile error**, not a silent divergence (§Problem 4). The case
for the change is cost and **locality**, and this document says so instead of inflating it.

## The `after_ids` churn ledger (rev 2 — the headline claim of rev 1 was false)

Rev 1 claimed "zero use-site churn" and listed `core/src/shard/wait_queue.rs` as
"read-only, must NOT be edited". **Both were wrong.** Changing `XRead.after_ids` from
`Vec<StreamId>` to `Vec<(u64,u64)>` changes the type at every site that *constructs* the variant,
not only at the site that consumes it. `rg -n after_ids` at HEAD returns the complete set:

| Site | Shape today | Edit? |
|---|---|---|
| `core/src/shard/blocking.rs:1098` — `let after_id = &after_ids[key_idx];` | **consumption** | **yes** — the one conversion, §(d) |
| `types/src/types/mod.rs:642` — `after_ids: vec![StreamId::new(0, 0)]` | construction (test, relocating) | **yes** — `vec![(0, 0)]` |
| `core/src/shard/wait_queue.rs:821` (in `test_has_waiters_for_kind_mixed` `:809`) | construction (test) | **yes** — `vec![(0, 0)]` |
| `core/src/shard/wait_queue.rs:844` (in `test_pop_of_kind_skips_wrong_kind` `:833`) | construction (test) | **yes** — same |
| `core/src/shard/wait_queue.rs:875` (in `test_pop_of_kind_returns_none_when_only_wrong_kind` `:866`) | construction (test) | **yes** — same |
| `core/src/shard/wait_queue.rs:905` (in `test_pop_of_kind_preserves_fifo_within_kind` `:892`) | construction (test) | **yes** — same |
| `core/src/shard/blocking.rs:1579` (in the test opening at `:1578`) | construction (test) | **yes** — same |
| `server/src/connection/util.rs:100-103` | construction (the adapter) | **deleted wholesale** |
| `protocol/src/response.rs:535` / `types/src/types/mod.rs:379` | the two declarations | the change itself |
| `commands/src/stream/read.rs:116` — `after_ids: resolved_ids` | construction, **already `Vec<(u64,u64)>`** (`:69`) | **no** |
| `shard-harness/tests/scenario_s5.rs:61` — `after_ids: vec![]` | construction, **element type inferred** | **no** |
| `website/src/content/docs/architecture/blocking.md:49` | prose | **yes** — doc, see §Files involved |

**Corrected ledger: one consumption conversion + six construction sites** (rev 1 counted one), plus
one **orphaned import**: `server/src/connection/util.rs:8` imports `StreamId` from `frogdb_core`
and its **only** use is `:102`, inside the adapter being deleted. Left in place it is an
`unused_imports` warning, which `Justfile:320` (`cargo clippy … -- -D warnings`) turns into a
build failure. It must come out of the `use` list in the same commit.

**The effort rating does not move.** All seven extra edits are one-liners, five of them the same
literal substitution, and **none is inside a tagged region**: `wait_queue.rs` has **zero** `FM-`
tags anywhere in the file, and `core/src/shard/blocking.rs`'s sole tag is `FM-CLUSTER-038` at
`:2065` — 486 lines below the `:1579` edit. What the correction *does* move is the sibling-risk
picture against proposal 81, which owns `wait_queue.rs`; see §Risks vs 81.

Retracted phrasing, replaced everywhere below: not "zero use-site churn", but **"zero churn at the
164 `BlockingOp` and 61 `Direction` mentions, except six `after_ids` construction sites and one
import line — all one-line, all in test modules or import lists."**

## Summary

Two enums describe one concept — "which blocking command is parked on this key, and which end of
the list does it touch" — and they are declared twice, 1200 lines and one crate apart:

- **`Direction`** — `protocol/src/response.rs:473-493` and `types/src/types/mod.rs:322-340`. Same
  two variants, same five derives. The `parse` bodies differ in **implementation** only
  (allocation-free `eq_ignore_ascii_case` vs an allocating `to_ascii_uppercase()`), and the
  allocating one is **dead**.
- **`BlockingOp`** — `protocol/src/response.rs:495-550` and `types/src/types/mod.rs:342-394`. Nine
  variants, variant-for-variant and field-for-field identical, with exactly **one** representation
  delta: `XRead.after_ids` is `Vec<(u64, u64)>` in protocol and `Vec<StreamId>` in types.
- **The adapter** — `server/src/connection/util.rs:74-118` (`convert_blocking_op`, 45 lines) and
  `:120-126` (`convert_direction`, 7 lines). One caller
  (`server/src/connection/blocking.rs:40`), zero tests. Because the two enums agree on every
  variant name and every field name, forty-five of those fifty-two lines are the identity function
  written out by hand; the seven that are not are `StreamId::new(ms, seq)` applied to a `Vec`.

The one delta is not a design decision, it is a **crate-graph artefact**: `frogdb-protocol` is the
dependency-graph leaf and cannot name `StreamId`, so `frogdb-commands` destructures a `StreamId`
into a tuple at `commands/src/stream/read.rs:83` and `frogdb-server` reconstructs it at
`util.rs:100-104`, so that `frogdb-core` can index it at `core/src/shard/blocking.rs:1098`. Three
crates participate in a round trip that starts and ends with the same value.

The proposal: **`frogdb-protocol` becomes the sole home.** Move the two enums out of the 1770-line
`response.rs` into a small `protocol/src/blocking.rs` **module** (they are payload types, not part
of the RESP value tree — see §Risks vs 80), move `BlockingOp::timeout_reply` there from
`frogdb-types` (it already returns `frogdb_protocol::Response`, so it is moving *toward* its own
return type), replace 105 lines of `frogdb-types` with a one-line `pub use`, delete the 52-line
adapter, and convert the tuple to a `StreamId` at the single site that needs one.
**≈156 lines of duplication removed** (105 in `frogdb-types` + 52 in `frogdb-server`); the
**whole diff nets ≈ −123 lines** once the ~157-line new module is counted against the 78 lines
lifted out of `response.rs` and the 44-line test that relocates. **No behaviour change**, and no
churn at the 164 use sites beyond the seven one-liners in §The `after_ids` churn ledger.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/protocol/src/response.rs` | 1770 | **Primary.** `Direction` `:473-493`, `BlockingOp` `:495-550` — **78 lines moved out** to a new sibling module. `Response::BlockingNeeded` `:709-716` and `InternalAction::BlockingNeeded` `:41-49` keep the `op: BlockingOp` field **verbatim** (name resolution follows the `pub use`). Existing tests at `:1290`, `:1368`, `:1404`, `:1602` construct `BlockingOp` and are **unedited**. Owned concurrently by **proposal 80** — see §Risks. 5 commits of recent churn. |
| `frogdb-server/crates/protocol/src/blocking.rs` | **new, ~157** | **Primary.** `Direction` + `BlockingOp` verbatim (78 lines lifted from `response.rs:473-550`), plus `impl BlockingOp { timeout_reply }` relocated from `frogdb-types` (31 lines, `types/mod.rs:396-426`), plus the relocated `#[cfg(test)]` assertion (44 lines, `types/mod.rs:623-666`), plus ~4 lines of module doc and imports. Rev 1 said "~110"; that figure omitted the relocated test. |
| `frogdb-server/crates/protocol/src/lib.rs` | 27 | **Primary.** `mod blocking;` added; the two names move from the `pub use response::{…}` list (`:20-23`) to a `pub use blocking::{BlockingOp, Direction};`. **Crate-root public API is byte-identical** — both names are already root re-exports today. |
| `frogdb-server/crates/types/src/types/mod.rs` | 1800 | **Primary.** `:322-340` (`Direction` + `parse`), `:342-394` (`BlockingOp`, doc comment at `:342`), `:396-426` (`impl timeout_reply`) — **105 lines deleted**, replaced by `pub use frogdb_protocol::{BlockingOp, Direction};`. Test `test_blocking_op_timeout_reply_nil_shape` `:623-666` relocates with the method (its `:642` `after_ids: vec![StreamId::new(0, 0)]` becomes `vec![(0, 0)]`). **Zero `FM-` tags in this file.** |
| `frogdb-server/crates/types/src/lib.rs` | 76 | **Read-only evidence — not edited.** `pub use types::{BlockingOp, …, Direction, …};` — **both names are on `:65`**. A re-export of a re-export resolves identically, which is why the 164 downstream mentions do not move. |
| `frogdb-server/crates/types/Cargo.toml` | — | **Read-only evidence.** `frogdb-protocol.workspace = true` `:14` — the edge that already exists and that makes the fold legal. |
| `frogdb-server/crates/protocol/Cargo.toml` | — | **Read-only evidence.** **No `frogdb-*` dependency of any kind.** This single fact fixes the direction of the fold and kills the "home = `frogdb-types`" hypothesis. |
| `frogdb-server/crates/core/src/lib.rs` | — | **Read-only evidence — not edited.** `pub use frogdb_types::*;` `:7`. Makes `frogdb_core::BlockingOp`, `frogdb_core::Direction` and `frogdb_core::types::BlockingOp` keep resolving. |
| `frogdb-server/crates/server/src/connection/util.rs` | 503 | **Primary.** `convert_blocking_op` `:74-118`, `convert_direction` `:120-126` — **52 lines deleted** — **plus `:8`**, where `StreamId` leaves the `use frogdb_core::{…}` list: its only use is `:102`, inside the deleted adapter, so leaving it orphans an import into an `unused_imports` warning that `Justfile:320`'s `-D warnings` fails on. Zero `FM-` tags. The file's *other* adapter, `raft_op_to_command` (doc `:165-182`, fn `:183`), **stays** and is the instructive contrast (§Problem 4). |
| `frogdb-server/crates/server/src/connection/blocking.rs` | 396 | **Primary, 3 lines.** `use crate::connection::util::convert_blocking_op;` `:19` deleted; `proto_op: frogdb_protocol::BlockingOp` `:38` becomes `op: BlockingOp` (already imported at `:10`); `let op = convert_blocking_op(proto_op);` `:40` deleted. `FM-BLOCKING-005` tags at `:342`, `:360`, `:371`, `:384` are inside the test module — **not in any edited region**. |
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | **Primary, 3 lines.** Import `:13` gains `StreamId`; `let after_id = &after_ids[key_idx];` `:1098` becomes a `StreamId::new(…)` local; **the test at `:1578` has `after_ids: vec![crate::types::StreamId::new(0, 0)]` at `:1579` → `vec![(0, 0)]`** (missed in rev 1). `FM-CLUSTER-038` at `:2065` is in the test module, 486 lines below the lowest edit — untouched. Owned concurrently by **81** (PN2 dummy channels at `:1595-1603`, 16 lines below `:1579`, same test module, disjoint) and future **88** — see §Risks. |
| `frogdb-server/crates/commands/src/stream/read.rs` | 408 | **Read-only evidence.** `resolved_ids: Vec<(u64, u64)>` `:69`, the destructure `:83`, the construction `:112-119`. Unchanged by this proposal — the tuple representation it already writes becomes the *only* representation. |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | **Read-only evidence.** `use frogdb_protocol::{BlockingOp, Direction, Response};` `:12`; the only three `Direction::parse` call sites in the tree (`:230`, `:231`, `:365`), all on protocol's copy. |
| `frogdb-server/crates/core/src/shard/wait_queue.rs` | 931 | **Primary, 4 test lines** — rev 1 wrongly called this "read-only, must NOT be edited". `after_ids: vec![crate::types::StreamId::new(0, 0)]` at `:821`, `:844`, `:875`, `:905` → `vec![(0, 0)]`, in four tests (`:809`, `:833`, `:866`, `:892`) inside the `#[cfg(test)]` module that opens at `:672`. Its *production* region (`WaitEntry.op` `:19`, `entry_matches_kind` `:490-501`, `blocking_op_name` `:658-670`) is genuinely read-only and compiles unchanged. **Zero `FM-` tags anywhere in the file.** **Owned by proposal 81** — see §Risks vs 81 for the corrected ordering. |
| `frogdb-server/crates/server/src/connection/blocking/coordinator.rs` | — | **Read-only, must NOT be edited.** `use frogdb_core::{BlockingOp, …}` `:10`, `op.timeout_reply()` `:40-41`, and `use frogdb_core::Direction;` `:216` — **inside the test tagged `FM-BLOCKING-002, FM-BLOCKING-003` at `:213`**. The re-export approach is what keeps this file at zero edits; see §Spec impact. |
| `frogdb-server/crates/types/src/types/stream.rs` | 2500 | **Read-only evidence.** `StreamId` `:13-18`, its impl `:20-156` — the clock read at `:126`, the companion returns at `:74` (`StreamRangeBound`) and `:97` (`parse_for_add → StreamIdSpec`), and the **`impl std::fmt::Display for StreamId` at `:158`** are the evidence for rejecting (R2). |
| `website/src/content/docs/architecture/blocking.md` | — | **Primary, 2 lines — found during revision, not in rev 1.** `:36-37` names `frogdb-server/crates/types/src/types/mod.rs` as `BlockingOp`'s home (becomes `protocol/src/blocking.rs`), and the code block's `:49` reads `XRead { after_ids: Vec<StreamId>, count: Option<usize> }` (becomes `Vec<(u64, u64)>`). Doc-only; `:65`'s `pub op: BlockingOp` in the `WaitEntry` block stays correct. |
| `.scratch/hardening/specs/blocking-failure-modes.md` | — | **Read-only.** **No `Status: LOCKED` line** (verified: `grep -n 'Status:' .scratch/hardening/specs/*.md` returns five files; this is not one of them). Scope prose `:8-14`. |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | **Read-only (LOCKED).** `FM-TXN-044` `:554-565` — the "blocking commands never block inside MULTI" row. §Spec impact proves it is untouched. |
| `.scratch/hardening/specs/cluster-failure-modes.md` | — | **Read-only (LOCKED).** `FM-CLUSTER-038` `:615-625`; its `Forced by` row (`:624`) names `slot_migrated_without_a_known_target_replies_clusterdown`, which lives in `core/src/shard/blocking.rs:2067`. §Spec impact. |

## Problem

### 1. The duplication census

Both declarations were read line-by-line at HEAD. This table is the load-bearing evidence.

| Item | `frogdb-protocol` | `frogdb-types` | Delta |
|---|---|---|---|
| `Direction` enum | `response.rs:473-480` (doc + `#[derive(Debug, Clone, Copy, PartialEq, Eq)]` + `Left`/`Right`) | `types/mod.rs:322-329` | **None.** Same doc sentence, same five derives, same two variants, same order. |
| `Direction::parse` | `response.rs:482-493` — `eq_ignore_ascii_case(b"LEFT")` / `(b"RIGHT")`, **no allocation** | `types/mod.rs:331-340` — `arg.to_ascii_uppercase().as_slice()` matched against `b"LEFT"`/`b"RIGHT"`, **allocates a `Vec<u8>` per call** | Same result for every input (both are exact ASCII-case-insensitive equality; non-ASCII bytes fail both). Different cost. **The allocating one has zero callers** — §Problem 3. |
| `BlockingOp` enum | `response.rs:495-550`, `#[derive(Debug, Clone, PartialEq)]` | `types/mod.rs:342-394`, `#[derive(Debug, Clone)]` | Nine variants — `BLPop`, `BRPop`, `BLMove{dest,src_dir,dest_dir}`, `BLMPop{direction,count}`, `BZPopMin`, `BZPopMax`, `BZMPop{min,count}`, `XRead{after_ids,count}`, `XReadGroup{group,consumer,noack,count}` — **identical names, identical field names, identical order** in both. Two deltas: protocol adds `PartialEq`; `after_ids` is `Vec<(u64,u64)>` vs `Vec<StreamId>`. |
| `timeout_reply` | — | `types/mod.rs:396-426` | The **only** method on either copy. Returns `frogdb_protocol::Response` (`:410`). |
| identity adapter | — | — | `server/util.rs:74-118` + `:120-126` = **52 lines**, **1 caller** (`connection/blocking.rs:40`), **0 tests** (`rg convert_blocking_op` returns the definition, one internal use, and the single caller — nothing else in the tree). |

The census also establishes what is **not** duplicated, because the brief's "and any sibling
duplicated blocking types" invited a wider hunt. Every other type on the blocking path is declared
exactly once: `UnblockMode` (`core/src/client_registry/mod.rs:307`), `WaiterKind`
(`core/src/command.rs:336`), `WaitOutcome` (`server/src/connection/blocking/coordinator.rs:21`),
`SlotMigrationKind` (`protocol/src/response.rs:77`). **The duplication is exactly two enums.** The
one other `StreamId` in the tree (`crates/testing/src/models/stream.rs:10`) is a deliberate
model-checking oracle and is out of scope.

### 2. The tuple round trip crosses three crates

`XRead.after_ids` is the whole reason two declarations exist, and the value it carries makes a
complete circuit:

| Step | Site | Shape |
|---|---|---|
| resolve `$` to the stream's last ID | `commands/src/stream/read.rs:72-79` | `StreamId` |
| **destructure** | `commands/src/stream/read.rs:83` — `resolved_ids.push((after_id.ms, after_id.seq))` | `(u64, u64)` |
| park it on the wait queue | `protocol/src/response.rs:535` → `core/src/shard/wait_queue.rs:19` | `Vec<(u64,u64)>` → **converted to** `Vec<StreamId>` en route |
| **reconstruct**, eagerly, for the whole vector | `server/src/connection/util.rs:100-104` — `.map(\|(ms, seq)\| StreamId::new(ms, seq))` | `Vec<StreamId>` |
| use exactly one element | `core/src/shard/blocking.rs:1098` — `let after_id = &after_ids[key_idx];` — then `:1103`, `s.read_after(after_id, *count)` | `&StreamId` |

`commands` already holds a `StreamId` and `core` wants one back; the only participant that cannot
name the type is `protocol`, which sits between them. The **adapter** exists to serve a constraint
neither of its neighbours has.

The round trip also costs a `Vec` allocation per parked blocking `XREAD` that nothing needs:
`util.rs:100-104` builds an N-element `Vec<StreamId>` at registration, and
`core/src/shard/blocking.rs:1098` reads one element of it per satisfaction attempt.

### 3. `frogdb_types::Direction::parse` is dead

`rg 'Direction::parse'` across the tree returns **three** sites, all in `frogdb-commands`
(`commands/src/blocking.rs:230`, `:231`, `:365`), and that file imports
`frogdb_protocol::{BlockingOp, Direction, Response}` (`:12`). The `frogdb-types` copy at
`types/mod.rs:331-340` is reachable by nothing. Ten lines of dead, allocating parser sitting
directly beneath a live enum, which is precisely how a second copy of a type earns its cost: it
looks load-bearing because the type above it is.

**The systemic lesson — why the compiler never said so.** `Direction::parse` is `pub`, on a `pub`
enum, re-exported from the crate root (`types/lib.rs:65`). `dead_code` never fires on an item
reachable from a crate's public API, because for a *published* library that item is someone's
entry point. But `frogdb-types` is `publish = false` (`types/Cargo.toml:6`) — a workspace-internal
crate whose only consumers are the other members of this workspace, all of which are visible to
`cargo`. Its "public API" is a fiction, and the lint is silenced by that fiction. **In a
`publish = false` workspace lib crate, `pub` is not an API claim — it is a lint suppression.** That
is the reusable finding here, and it applies to **every crate under `frogdb-server/crates/`**
(`grep -L 'publish = false' frogdb-server/crates/*/Cargo.toml` returns **nothing** — all of them
are `publish = false`): dead public items in them are invisible to `cargo clippy -D warnings` and only
`rg` finds them. It is why a ten-line duplicate parser survived long enough to be discovered by a
dedupe audit rather than by the build.

### 4. What the duplication does **not** cost, stated honestly

It is tempting to sell this as a divergence hazard. It is not one, and the proposal is stronger
for saying so:

- `convert_blocking_op` (`util.rs:76`) matches `frogdb_protocol::BlockingOp` **exhaustively, with
  no `_` arm**. A variant added to protocol and not to types is a compile error here. A field added
  to a types variant and not to protocol is a compile error at the construction site in the same
  function.
- `convert_direction` (`util.rs:122`) is exhaustive over two variants.

So the duplication is **compiler-guarded against silent drift**. What it actually costs is: 105
lines of `frogdb-types`, 52 lines of `frogdb-server`, ten dead lines, one `Vec` allocation per
parked `XREAD`, a stale doc comment (below), and — the real item — the fact that "which blocking
op" is a concept with **two definitions and no single place to add a tenth variant**. Adding
`BZMPOP`-style command number ten today means editing two enums in two crates plus an adapter arm
in a third.

The stale doc comment is the tell. `protocol/src/response.rs:497-498` says (`:495` is the summary
line "Blocking operation type for responses.", `:496` is a bare `///` separator):

```rust
/// This is a simplified version for use in Response::BlockingNeeded.
/// The connection handler converts this to the full BlockingOp in frogdb_core.
```

There is nothing simplified about it and nothing full about the other one: nine variants each,
same fields, same names. The comment describes an intent the code does not implement — see hotfix
**H2**.

**The instructive contrast is in the same file.** `util.rs`'s *other* adapter,
`raft_op_to_command` (doc `:165-182`, fn `:183`), carries an eighteen-line doc comment justifying
itself: `RaftClusterOp` lives in `protocol` and `ClusterCommand` lives in the **locked**
`frogdb-cluster`, the two have genuinely different shapes (`Vec<u16>` slots vs `Vec<SlotRange>`,
`SocketAddr` pairs vs `NodeInfo`), and both are foreign to `frogdb-server` so the orphan rule
forbids a `From` impl. That adapter is real: it **transforms** — and it is **tested**, by
`raft_op_to_command_maps_every_variant` (`util.rs:374`), which asserts all eleven variants map,
under a doc comment (`:370-372`) explaining that the test is "the regression guard the previous
design could not express".

`convert_blocking_op` transforms nothing and is tested by nothing. The two are **not** adjacent —
rev 1 said "eleven lines above", which was wrong. `convert_direction` ends at `:126`;
`raft_op_to_command`'s doc opens at `:165` and its `fn` at `:183` — a gap of 39 to 57 lines, with
`estimate_resp2_frame_size` (`:128-146`) and `estimate_command_size` (`:148-163`) sitting between
them. The contrast is one of kind, not of proximity, and stating it as proximity overstated it.
**A tested transformer and an untested identity function in one file, one earning its keep and one
not, is the clearest statement of the problem this proposal can make. `raft_op_to_command`
stays.**

### 5. The doc comment on the `frogdb-types` copy points the wrong way too

`types/mod.rs:342` calls its copy "Blocking operation type for **wait queue entries**" while
`response.rs:495` calls its copy "Blocking operation type for **responses**". Both are true of both
— the same nine variants travel from a command handler, through `Response::BlockingNeeded`, into a
`WaitEntry`, and back out as a reply. Two doc comments each describing one leg of one journey is
what a split concept reads like.

## Proposed change

### The direction is forced, not chosen

`frogdb-protocol/Cargo.toml` declares **no `frogdb-*` dependency**. Every other crate that names
`BlockingOp` — `frogdb-types` (`Cargo.toml:14`), `frogdb-core` (`:32-33`), `frogdb-commands`
(`:48-49`), `frogdb-server` (`:115`), `frogdb-shard-harness` (`:22`) — depends on it. **Protocol
is the only crate all of them can see**, and it is the crate whose `Response::BlockingNeeded`
(`response.rs:709-716`) and `InternalAction::BlockingNeeded` (`response.rs:41-49`) structurally
require the type. There is one legal home.

### (a) A new `protocol/src/blocking.rs` module, not a bigger `response.rs`

The two enums move **verbatim** out of `response.rs:473-550` into a new sibling module. Three
reasons, in order of weight:

1. **Locality.** `response.rs` is 1770 lines describing the RESP value tree. A parser for the
   literal bytes `LEFT`/`RIGHT` is not part of that tree — it is a command-argument type that
   happens to ride inside one variant of it. The two enums sit today under a section banner that
   reads `// Full Response Type (union of wire + internal)` (`:469-471`), which they are not part
   of.
2. **It converts a conflict with proposal 80 into a deletion.** 80 restructures `response.rs`'s
   variant declarations (`:40-73`, `:116-175`, `:647-743`) and its `into_wire`/`from_wire`
   recursion (`:770-881`). Lifting `:473-550` out wholesale leaves 80 a contiguous 78-line removal
   in a region it does not otherwise touch, rather than two proposals editing interleaved
   declarations. §Risks.
3. **Depth.** The new module is ~157 lines — 78 enum lines, 31 for `timeout_reply`, 44 for its
   relocated test, ~4 of header — and owns one decision — *what are the blocking ops and
   which end of a list does each touch* — for every crate in the tree. Shallow by line count, deep
   by reach.

`protocol/src/lib.rs` gains `mod blocking;` and moves the two names from the
`pub use response::{…}` list (`:20-23`) into `pub use blocking::{BlockingOp, Direction};`. **The
crate-root public API does not change**: both names are root re-exports today, so every
`frogdb_protocol::BlockingOp` in the tree resolves identically.

### (b) `timeout_reply` moves with the type — it has to, and it should

An inherent `impl` must live in the type's defining crate, so `types/mod.rs:396-426` cannot stay
once the type is foreign to `frogdb-types`. That is a constraint, but it is also the right
outcome: the method returns `frogdb_protocol::Response` and its whole job is to pick between
`Response::NullArray` and `Response::Null` (`:420`, `:423`). It has been living one crate away from
both its return type and the RESP2 nil-shape rule its doc comment (`:397-407`) explains. **The
alternative — an extension trait in `frogdb-types` — was considered and rejected**: it would
preserve the split it exists to remove and force every caller to import a trait.

Its two callers are unaffected: `core/src/shard/blocking.rs:198` and
`server/src/connection/blocking/coordinator.rs:40-41` call it as a method on a value they already
hold.

### (c) `frogdb-types` keeps the *names*, drops the *declarations*

`types/src/types/mod.rs:322-426` (105 lines) becomes one line:

```rust
pub use frogdb_protocol::{BlockingOp, Direction};
```

This is the entire reason the change is **S** rather than **M**, so it is worth being explicit
about the resolution chain:

| Path used in the tree | Resolves via | Still valid? |
|---|---|---|
| `frogdb_protocol::BlockingOp` | `protocol/lib.rs` root re-export | yes — unchanged |
| `frogdb_types::BlockingOp` | `types/lib.rs:65` → `types::types::BlockingOp` → the new `pub use` | **yes** |
| `frogdb_core::BlockingOp` | `core/lib.rs:7` `pub use frogdb_types::*;` | **yes** |
| `frogdb_core::types::BlockingOp` — shard-harness `src/generator.rs:23`, `src/harness.rs:44`, and (omitted from rev 1's table, verified here) `tests/scenario_s1.rs:13`, `tests/scenario_s2.rs:26`, `tests/scenario_s5.rs:16`, `tests/shard_driver.rs:189` | same glob, `types` module | **yes** — this is the complete set: `rg -n BlockingOp frogdb-server/crates/shard-harness/` returns these six `use` lines and nothing else importing the type |
| `crate::types::BlockingOp` (core `wait_queue.rs:10`, `blocking.rs:13`, `diagnostics.rs:530`, `execution.rs:1222`, `post_execution.rs:1784`/`:1855`, `message.rs:507`) | same | **yes** |

All **164** `BlockingOp` mentions across 21 files and all **61** `Direction::{Left,Right}` mentions
compile untouched. **Verified there is no trait impl to relocate**: `rg 'for BlockingOp|for
Direction'` returns nothing — only the three inherent `impl` blocks, two of which are deleted and
one of which moves.

The `PartialEq` that `frogdb-core`'s users gain is purely additive.

### (d) Delete the adapter; convert once, where it is used

- `server/src/connection/util.rs:74-126` — **52 lines deleted** — **and `:8`**, where `StreamId`
  comes out of `use frogdb_core::{CommandFlags, KeyAccessFlag, KeyAccessType, StreamId, …}`. Its
  only use in the file is `:102`, inside the deleted body; leaving it is an `unused_imports`
  warning and `Justfile:320` runs clippy with `-D warnings`. **Same commit, not a follow-up.**
- `server/src/connection/blocking.rs`: `:19` import deleted; `:38` parameter becomes `op:
  BlockingOp` (the name is already in scope from `:10`); `:40` deleted.
- `core/src/shard/blocking.rs:1098` — the one **consumption** conversion. Line numbers here are the
  HEAD values an implementer will paste at; rev 1 cited this cluster four lines high throughout:

```rust
   1096:            BlockingOp::XRead { after_ids, count } => {
   1097:                let key_idx = entry.keys.iter().position(|k| k == key).unwrap_or(0);
-  1098:                let after_id = &after_ids[key_idx];
+  1098:                let (ms, seq) = after_ids[key_idx];
+                      let after_id = StreamId::new(ms, seq);
   1099:
   1100:                let entries: Vec<StreamEntry> = match store.get(key) {
   1101:                    Some(value) => value
   1102:                        .as_stream()
-  1103:                        .map(|s| s.read_after(after_id, *count))
+  1103:                        .map(|s| s.read_after(&after_id, *count))
```

  with `StreamId` added to the `use crate::types::{BlockingOp, Direction, StreamEntry, Value};` at
  `:13`. `StreamId` is a two-`u64` `Copy` POD, so this is free — and it **removes** the eager
  `Vec<StreamId>` allocation that `util.rs:100-104` performed per parked `XREAD`. (`:1097`, the
  `position(…).unwrap_or(0)` immediately above, is the latent duplicate-key bug recorded under
  §Other risks. It is **not** touched.)

- **Six construction sites** become `vec![(0, 0)]`, all one line each, all in test modules:
  `types/mod.rs:642` (relocating with the test), `wait_queue.rs:821`, `:844`, `:875`, `:905`, and
  `core/src/shard/blocking.rs:1579`. Rev 1 listed only the first. See §The `after_ids` churn ledger
  for the derivation and for the two sites that correctly need **no** edit
  (`commands/src/stream/read.rs:116`, already tuples; `scenario_s5.rs:61`, `vec![]`).
- `website/src/content/docs/architecture/blocking.md:37` (home crate) and `:49` (`Vec<StreamId>` →
  `Vec<(u64, u64)>`) — doc-only, same commit.

### Alternatives considered and rejected

**(R1) Make `frogdb-types` the home.** Requires `frogdb-protocol → frogdb-types`, and
`frogdb-types → frogdb-protocol` already exists (`types/Cargo.toml:14`; four `frogdb_protocol` use
sites: `error.rs:8`, `redirect.rs:11`, `types/string_value.rs:340`, `types/mod.rs:410`). A cycle.
**Impossible.** This is the brief's and proposal 81's stated plan, and it does not compile.

**(R2) Move `StreamId` into `frogdb-protocol` so `after_ids` can be `Vec<StreamId>`.** The brief's
solution. Rejected on cost:

- `StreamId::generate` reads the **clock seam** — `crate::clock::system_now()`,
  `types/src/types/stream.rs:126`. `scripts/clock-seam.py` treats `frogdb_types::clock` as the seam
  (`:16`) and allowlists `frogdb-server/crates/types/src/clock.rs` (`:76`). A wall-clock read in
  the leaf wire crate needs a new allowlist entry and puts time into a crate whose job is bytes.
  The clock there is not incidental: it was deliberately virtualized by `8b62120f fix(types):
  virtualize XADD stream-ID wall clock (issue 17)`.
- `StreamId`'s impl block (`:20-156`) returns `StreamRangeBound` (`:74`), `StreamIdSpec`
  (`parse_for_add`, `:97`) and `StreamIdParseError`, so the move is four types, not one.
- **`StreamId` carries an `impl std::fmt::Display`** (`stream.rs:158-162`, rendering `"{ms}-{seq}"`)
  — and `StreamIdParseError` carries another (`:259`). Moving the type moves a **human-facing
  rendering** into the crate that owns the *wire* rendering, where `format.rs` is pinned as the
  sole float renderer by `lint-format-float` precisely because "how a value is written out" is a
  chokepoint concern in this crate. R2 does not merely drag a clock into `frogdb-protocol`; it
  drags a second, unpinned `Display` surface in beside a lint-guarded one. This is a stronger
  objection than rev 1 made, and it was found by review, not by the original author.
- The alternative — splitting the impl, clock-free half in `protocol` and `generate`/
  `generate_with_ms` left behind — puts one type's methods in two crates to save one line in a
  third.

**One line of conversion at the single use site is cheaper than all of that**, and it is the
representation `frogdb-commands` already writes at `read.rs:83`.

**(R3) Keep both and generate the adapter with a macro.** Machinery to preserve the problem.

### Deletion test

- **The unified `BlockingOp`/`Direction` in `frogdb-protocol`** — delete them and
  `Response::BlockingNeeded` and `InternalAction::BlockingNeeded` have no payload type; nine
  variants must be re-declared inside the protocol crate immediately. **Earns its keep.**
- **The `frogdb-types` copy** — delete it and **nothing reappears** except one `StreamId::new`
  call at `core/src/shard/blocking.rs:1098` and six one-line test-literal substitutions. Everything
  else that referenced it (164 mentions) keeps compiling through the `pub use`. **Does not earn its
  keep — this is the change.**
- **`convert_blocking_op` / `convert_direction`** — delete them and nothing reappears; there is
  one caller and it stops needing a call. **Pure deletion.**
- **`frogdb_types::Direction::parse`** — delete it and nothing reappears; it has no callers.
  **Pure deletion.**
- **`raft_op_to_command` (same file, `util.rs:183`)** — delete it and a real shape transformation
  (`Vec<u16>` → `Vec<SlotRange>`, `(u64, SocketAddr, SocketAddr)` → `NodeInfo`) reappears at its
  call sites, across an orphan-rule boundary into a **locked** crate — and its eleven-variant
  regression test (`:374`) loses its subject. **Earns its keep; untouched.**

## Testability improvement

The honest sizing first: **this change does not unlock a test that could not be written today.**
What it does is delete untested code and move a test to the crate it is about.

1. **It deletes a 52-line untested function.** `rg convert_blocking_op` over the whole tree returns
   its definition (`util.rs:75`), its internal `convert_direction` uses (`:85`, `:86`, `:90`), and
   its single caller (`connection/blocking.rs:40`). No test names it. Its correctness — that all
   nine variants and all thirteen fields map through — is today asserted by nothing but the
   exhaustive `match`. After the change there is no mapping to assert. **The control case is in
   the same file:** `raft_op_to_command` **does** have a variant-exhaustive test
   (`raft_op_to_command_maps_every_variant`, `util.rs:374`, all eleven `RaftClusterOp` variants),
   under a doc comment calling it "the regression guard the previous design could not express"
   (`:370-372`). Same file, same shape of risk — the adapter that does real work got a test; the
   identity adapter never did. That asymmetry is the argument for deleting one and keeping the
   other, and it is sharper than rev 1's "zero tests" framing on its own.

2. **It moves `timeout_reply`'s test to the crate that owns its assertions.**
   `test_blocking_op_timeout_reply_nil_shape` (`types/mod.rs:623-666`) asserts
   `Response::NullArray` for eight ops and `Response::Null` for `BLMove`. Both constants belong to
   `frogdb-protocol`; the test currently lives in `frogdb-types` and imports them across a crate
   edge (`:625`). Relocated, it sits beside the method, beside the enum, and beside the RESP2 nil
   encoders it is really pinning. This is the RESP2-vs-RESP3 distinction called out in the method's
   own doc (`types/mod.rs:401-407`), and it is separately pinned end-to-end by
   `redis-regression/tests/blocking_nil_shape_regression.rs` (nine wire-level cases, `:49`-`:121`)
   — **which this proposal does not touch**, so the byte-level contract keeps its independent net
   throughout.

3. **It removes a proposition, which is better than testing one.** "The protocol enum and the core
   enum agree" is currently a property of the system. Nobody wrote a test for it (and a good one
   would be awkward — a round-trip property test over an enum with no `Arbitrary` impl). After the
   fold it is not a proposition at all. The **seam** where drift could enter is gone rather than
   guarded.

4. **`Direction::parse` gets one owner.** Today two implementations of one parse rule exist and
   only one is exercised (via `commands/src/blocking.rs:230`/`:231`/`:365`, covered by the
   BLMOVE/BLMPOP regression suites). After the fold the tested implementation is the only
   implementation. A three-case unit test (`LEFT`, `left`, garbage) is worth adding next to it in
   the new module — cheap, and there is currently no direct test of either copy.

**Regression net for the change itself.** Because the change is representation-only, the existing
suites are the net: `frogdb-core`'s `shard/blocking.rs` test module (~30 tests, `:1268-2090`),
`wait_queue.rs`'s 11 tests (module `:672-931`), `coordinator.rs`'s
`timeout_reply_picks_nil_shape_per_op` (`:215`), and `blocking_nil_shape_regression.rs`.

**All of them must pass unedited; five of them change one literal each and nothing else.** Rev 1
said "all compile unedited", which was false — `wait_queue.rs`'s `test_has_waiters_for_kind_mixed`
(`:809`), `test_pop_of_kind_skips_wrong_kind` (`:833`),
`test_pop_of_kind_returns_none_when_only_wrong_kind` (`:866`),
`test_pop_of_kind_preserves_fifo_within_kind` (`:892`), and `blocking.rs`'s test at `:1578` each
carry an `after_ids: vec![StreamId::new(0, 0)]` literal that becomes `vec![(0, 0)]`. **No
assertion, no name, no arrangement, and no expected value in any of the five changes** — the
literal is a don't-care seed for a stream waiter whose kind, not whose ID, is under test. The
stopping rule stands with its bound corrected: **if any test needs an edit beyond those five
literals and the one `let` at `blocking.rs:1098`, the change has stopped being
representation-only and should stop.**

## Serialization safety — the encoding is proven unchanged

The brief requires proof that these types do not serialize into the replication feed, the WAL, or a
wire response. Verified **five** ways (rev 1's prose said "four" over a five-item list; the review
counted five legs and confirmed all of them):

1. **No serialization derives, no `Display`.** `rg 'for BlockingOp|for Direction'` returns
   **nothing** — there is no `impl Display`, no `impl Serialize`, no `impl Deserialize`, no trait
   impl of any kind on either copy in either crate. The derives are `Debug, Clone[, Copy,
   PartialEq, Eq]` and nothing else. **No structural encoding of either type exists anywhere.**

2. **Every observable rendering is a hand-written `match` on variant names, and the variant names
   are identical in both copies.** The complete set:

   | Renderer | Site | Output | Changed? |
   |---|---|---|---|
   | `blocking_op_name` | `core/src/shard/wait_queue.rs:658-670` | the nine strings `"BLPOP"` … `"XREADGROUP"`, consumed at `:168` (trace) and `:603` (`WaiterDump.op`, i.e. `DEBUG WAITQUEUE`) | **No** — same nine arms, same nine literals |
   | `direction_arg` | `core/src/shard/blocking.rs:1189-1194` (doc `:1187-1188`) | `Bytes::from_static(b"LEFT")` / `b"RIGHT"` | **No** |
   | replication propagation | `core/src/shard/blocking.rs:830-838` | `SynthesizedCommand { name: "LMOVE", args: [src, dest, direction_arg(src_dir), direction_arg(dest_dir)] }` — the served-`BLMOVE` command replicas replay | **No** — built from `direction_arg`, above |
   | `lpop`/`rpop`/`lpush`/`rpush` event names | `core/src/shard/blocking.rs:803-808`, `:863-864` | keyspace-notification event strings | **No** — `match` on `Direction` |
   | `timeout_reply` | relocating, body verbatim | `Response::NullArray` / `Response::Null` | **No** |

   This is the load-bearing point: **the replica feed does receive a rendering derived from
   `Direction`** (the two `direction_arg(…)` args at `blocking.rs:835-836`, inside the
   `propagate: Some(SynthesizedCommand { … })` at `:830-838`), and it is produced by a two-arm
   `match` whose arms are byte-identical before and after. Nothing about the fold reaches it.

3. **`BlockingOp` never reaches the wire encoder.** `Response::is_internal` (`response.rs:754-762`)
   lists `BlockingNeeded` as internal, and `into_wire` (`:820-822`) returns it as
   `Err(InternalAction::BlockingNeeded{…})`. The only site that can observe that error,
   `narrow_to_wire` (`server/src/connection/frame_io.rs:41-53`), emits the **fixed string**
   `"ERR internal action reached response encoder"` (`:50`) — no payload interpolation. (Its
   `tracing::error!(?action, …)` at `:45` does `Debug`-print the payload, but that payload is the
   **protocol** `BlockingOp`, already in tuple form today, so even that log line is byte-identical
   after the change.)

4. **No `Debug`-rendering of the `frogdb-types` copy exists outside tests.** Grepping the shard and
   connection modules for `{op` / `?op` / `op:?` returns only `entry.op` *matches* and
   `blocking_op_name(&entry.op)` — no formatting. So the one representation change that does exist
   — `Debug` of `XRead` printing `after_ids: [(0, 0)]` instead of `[StreamId { ms: 0, seq: 0 }]` —
   **appears in no log, no reply, no dump, and no persisted byte.** It is visible only in test
   panic messages.

5. **No WAL or snapshot path.** Neither type appears in `frogdb-persistence`, `frogdb-recovery`,
   `frogdb-replication`, `frogdb-replication-runtime`, `frogdb-cluster` or
   `frogdb-cluster-runtime`: `rg 'BlockingOp|BlockingNeeded'` over all six returns **zero hits**.

**Verdict: representation-only. Byte-identical semantics on every wire, feed, log and dump.**

## Spec / LOCKED impact — clear, with the reasoning shown

- **Locked crates.** The four locked pairs are `frogdb-txn`+`frogdb-vll`,
  `frogdb-persistence`+`frogdb-recovery`, `frogdb-replication`+`frogdb-replication-runtime`,
  `frogdb-cluster`+`frogdb-cluster-runtime` (ADRs `adr/0002`–`0004`). This proposal edits
  `frogdb-protocol`, `frogdb-types`, `frogdb-core` and `frogdb-server` — **none locked, none
  mutation-gated**. As proved in §Serialization safety (5), no locked crate so much as mentions
  either type, so `just mutants-diff` is not owed. (Running it on `frogdb-core` before push costs
  nothing and is cheap insurance for the `blocking.rs`/`wait_queue.rs` edits — five of which are
  test-side and therefore invisible to a mutation score anyway.)

- **The txn question the brief raised — "BLPOP inside MULTI?"** Real, tagged, and untouched.
  **`FM-TXN-044`** (`txn-failure-modes.md:554-565`, LOCKED) rules that blocking and socket-handoff
  commands never block inside `MULTI`. Its stated invariant is that "the blocking interceptors sit
  on the direct dispatch path, which `EXEC` does not use", and the mechanism for the `BLPOP` family
  is `core/src/shard/execution.rs:626`:

  ```rust
  let response = if matches!(&response, Response::BlockingNeeded { .. }) {
      Response::Null
  } else { response };
  ```

  That line **matches on the `Response` variant, not on `BlockingOp`**, and this proposal does not
  touch `Response::BlockingNeeded`, `execution.rs:626`, or the row's three forcing tests
  (`test_wait_inside_multi_returns_count_immediately`,
  `test_wait_inside_multi_nonzero_timeout_does_not_block`, `test_psync_inside_multi_replies_ok`,
  all in `frogdb-server`). The same is true of the script path
  (`core/src/scripting/bindings.rs:184`), which matches `InternalAction::BlockingNeeded { .. }`
  with `..`. **Clear.**

- **`FM-` tags in the edited file set.** The set is now **seven code files plus one doc page** (rev
  1 said six; `wait_queue.rs` was wrongly listed read-only). Grepping `FM-` across all of them
  returns hits in exactly two, both **outside every edited region**:
  - `server/src/connection/blocking.rs` — `FM-BLOCKING-005` at `:342`, `:360`, `:371`, `:384`, all
    doc comments on tests inside `#[cfg(test)]`. The edits are at `:19`, `:38`, `:40`.
  - `core/src/shard/blocking.rs` — `FM-CLUSTER-038` at `:2065`, on
    `slot_migrated_without_a_known_target_replies_clusterdown` (`:2067`), a test named in the
    **LOCKED** cluster spec's `Forced by` row (`cluster-failure-modes.md:624`). The edits are at
    `:13`, `:1098` and `:1579` — the lowest is 486 lines above the tag. The tagged test is not
    renamed, not moved, and its body (`make_entry(BlockingOp::BLPop, …)`, `:2072`) compiles
    unchanged through the re-export.

  `core/src/shard/wait_queue.rs` — the file newly promoted to Primary — contains **zero** `FM-`
  tags anywhere (proposal 81 independently states the same at its `§Files involved`), so its four
  test-literal edits are ungoverned by any spec row. `protocol/src/response.rs`,
  `protocol/src/lib.rs`, `types/src/types/mod.rs` and `server/src/connection/util.rs` likewise
  contain **zero**.

- **`coordinator.rs` is why the re-export matters, not just why it is convenient.** Its test at
  `:213` carries `// FM-BLOCKING-002, FM-BLOCKING-003` and its body opens `use frogdb_core::
  Direction;` (`:216`). Any fold that changed the *name* `frogdb_core::Direction` would edit the
  interior of a spec-forcing test. The `pub use` chain means **`coordinator.rs` is not edited at
  all**. This is the design constraint that picked the approach, not a happy accident.

- **`just lint-failure-modes`** globs every `.scratch/hardening/specs/*-failure-modes.md`
  (`scripts/failure-modes.py:243`) and checks both directions: every `Forced by` test exists and
  carries a tag, every tag names a row. No test is renamed, moved between crates, or retagged by
  this change, and no spec row is edited. **Gate unaffected.**

- **`blocking-failure-modes.md` is not locked.** `grep -n 'Status:' .scratch/hardening/specs/*.md`
  returns five files — cluster, vll, replication, txn, persistence. The blocking spec has no
  `Status:` line and therefore no `LOCKED` header. Its scope (`:8-14`) is the **connection-side**
  wait path and explicitly hands the shard-side wait queue to a spec that does not yet exist.
  Nothing here is spec-first work.

- **Seam lints.** Checked against `agents/seam-lints.md` and `Justfile:329`
  (`lint-gates` = fourteen gates; `lint` adds `lint-turmoil-features`, `lint-turmoil`,
  `lint-failure-modes`):
  - **`lint-clock-seam`** — the one gate this change could plausibly trip, and only under the
    **rejected** alternative (R2). As proposed, no clock read moves: `StreamId::generate` stays in
    `frogdb-types` beside `crate::clock` (`types/lib.rs:9`). Grepping the edited files for
    `Instant::now` / `SystemTime::now` returns nothing. **Unaffected.**
  - **`lint-format-float`** — pins `protocol/src/format.rs` as the sole float renderer
    (`Justfile:1253`). `frogdb-types` keeps its `pub(super) use frogdb_protocol::format_float`
    (`types/string_value.rs:340`) untouched; the `frogdb-types → frogdb-protocol` edge this gate
    depends on is **preserved, not weakened** — the fold uses the same edge.
  - **`lint-metrics-chokepoint`, `lint-info-seam`, `lint-redirect-seam`,
    `lint-pubsub-confirmation-seam`, `lint-failover-atomicity`, `lint-durable-ack`,
    `lint-nested-config`, `lint-error-sanitize`, `lint-no-typed-unwrap`,
    `lint-keyspace-notify-routing`, `lint-script-gate`, `lint-continuation-lock`** — none has a
    surface in an enum declaration or in a deleted identity `match`. No metric is emitted, no INFO
    field written, no redirect rendered, no error constructed, no `as_*_mut().unwrap()` added.
    **Unaffected.**
  - **`lint-turmoil-features`** — no `#[cfg(feature = "turmoil")]` is added or removed; neither
    enum is behind a `cfg`. **Unaffected.**
  - **Command-family features** — `BlockingOp::XRead`/`XReadGroup` are *type variants*, not
    command registrations, and they live in `frogdb-protocol`, which has no feature gates. The
    `stream` family gate in `frogdb-commands` is untouched. No `full`/`cmd-full` build is needed.

- **Vocabulary** (`frogdb-server/CONTEXT.md`). Prose here uses **ShardWorker** (`:100`),
  **ConnectionHandler** (`:103`) and **Internal Shard**, and avoids the *Avoid* list — no
  "session"/"connection fiber" for a connection, no "peer"/"instance", no bare "shard" where
  cluster partitioning could be meant, no master/slave. No wire-visible string in the edited set
  contains a flagged term.

## Risks / scope boundaries vs siblings

### vs proposal 80 (PN1, `Response`/`WireResponse` fold) — same file, disjoint concern, ordering preferred

80 makes the RESP value tree one generic type (`Resp<I>`), editing `response.rs:40-73`
(`InternalAction`), `:116-175` (`WireResponse`), `:647-743` (`Response`), and `:770-881`
(`into_wire`/`from_wire`). **84 edits `response.rs:473-550` and nothing else in that file** — a
region 80 does not restructure. 80's digest explicitly rules that PN1 (the response *tree*) and
PN6 (its *payload* types) must not be conflated, and this proposal honours that:

- **84 does not restructure `Response`.** `BlockingOp` does appear inside `Response::BlockingNeeded`
  (`:709-716`) and `InternalAction::BlockingNeeded` (`:41-49`). Both variants keep their exact
  field list; only the *path* the name resolves through changes, and the crate root re-export makes
  even that invisible. **The edge is declared and not crossed.**
- **No interaction with 80's size argument.** 80 shrinks `Response` from 128 to 40 bytes by boxing
  `InternalAction`. `BlockingOp`'s own size is unchanged by 84 — `Vec<(u64,u64)>` and
  `Vec<StreamId>` are both a three-word `Vec` of 16-byte elements, and the variant that survives is
  the one `Response` already holds. 80's measurement stands either way.
- **Preferred order: 80 first, then 84.** Not a constraint — the regions are ~100 lines apart and
  `git` will merge them — but 84's extraction is a clean 78-line cut whose line numbers shift under
  80's edits, so doing it second means re-deriving two numbers rather than re-deriving 80's four
  regions. If 84 lands first, 80 finds `response.rs` **78 lines shorter** and two fewer types in
  its `pub use` list, which is strictly easier for 80.

### vs proposal 81 (PN2 + PN3, core shard dead seams) — **two** shared files, and a corrected ordering

**This section is rewritten. Rev 1 claimed 84 and 81 were disjoint at the file level, on the
strength of the false "84 does not edit `wait_queue.rs`". 84 edits `wait_queue.rs`.** The two
proposals now share both of 81's files.

| File | 81's regions | 84's regions | Overlap |
|---|---|---|---|
| `core/src/shard/wait_queue.rs` | **production region only**: `ShardWaitQueue` `:51-81`; six unlink copies `:222-238`, `:260-289`, `:313-337`, `:370-394`, `:456-485`, `:545-570`; three dead methods `:243-292`, `:399-405`, `:503-520`; duplicated pop `:522-573`; `entry_matches_kind` `:490-501`. Plus ~60 lines of **new** tests it adds. | **test module only**: `:821`, `:844`, `:875`, `:905`, one literal each | **None textual.** 81's every deletion is at `:573` or above-in-file; the `#[cfg(test)]` module opens at `:672`; 84's lowest edit is `:821`. |
| `core/src/shard/blocking.rs` | PN2 `:1595`, `:1598`, `:1603` (dummy-channel ceremony, test module); PN3 read-only (call sites `:306`, `:494`, `:513` keep their signatures) | `:13` (import), `:1098` (one `let`), `:1579` (one literal) | **None.** `:1579` sits **16 lines above** 81's `:1595`, inside the same test module — close, but not within any three-line diff context of it. |

**No merge conflict in either file. But the ordering is no longer free.**

**Preferred order: 81 first, then 84.** 81's three dead-method deletions alone remove **75 lines**
(`:243-292` = 50, `:399-405` = 7, `:503-520` = 18) from the region *above* 84's four edits, and its
unlink/pop unification removes more; PN3 is stated at "~90 lines net deleted" in one file. Every
one of those lines is above `:672`, so **all four of 84's `wait_queue.rs` line numbers shift** if
81 lands first — and 84's `blocking.rs:1579` shifts nothing, since 81's `blocking.rs` edits are
below it. Going **81 → 84** costs 84 one re-derivation of four numbers in one file (a `rg -n
after_ids` away). Going **84 → 81** costs 81 nothing in `wait_queue.rs`'s production region either,
so the asymmetry is small — but it points one way, and rev 1's flat "**No ordering constraint**"
is **withdrawn** and replaced by this.

This is a *presentation*, not a ruling: 81 and 84 are separately landable in either order, and the
merge-order call belongs to the orchestrator.

**81's own forward reference still resolves correctly.** 81 reasoned this edge out under "vs future
proposal 84" and reached the right conclusion — its predicates and `blocking_op_name` compile
unchanged because the variant names are identical — via the wrong premise (that the fold goes
*into* `frogdb-types`; §Corrections). That conclusion survives: 81 "does not touch, move, or
re-shape either enum", and 84 does not touch any pop, unlink, or predicate. What did **not** survive
is rev 1's inference that "84 edits none of `wait_queue.rs`'s `BlockingOp` mentions" — it edits four
of them, in tests 81 does not modify.

### vs future proposal 88 (PN12, blocking-serve wake effects) — hard boundary, declared

88 owns served-wake semantics: routing `ListSatisfaction::satisfy`'s writes through
`WRITE_EFFECT_ORDER`, the `bump_version_for_key` at `blocking.rs:348`, the inline notify at `:369`,
and `pending_serve_propagations` at `:360`. **84 changes no behaviour on that path.** Specifically:

- 84 does **not** touch `Satisfaction::Done`, `Restore`, `propagate`, or the `SynthesizedCommand`
  at `:830-838` (its two `direction_arg(…)` args at `:835-836`). §Serialization safety proves the
  `LMOVE src dst LEFT RIGHT` replicas replay is byte-identical.
- 84's only edit inside a satisfaction arm is `:1098`, in `BlockingOp::XRead` — the one arm that
  **mutates nothing**. Its `Satisfaction::Done` block at `:1113-1123` reads `cascade: None`,
  `events: Vec::new()` (`:1116`), `restore: Restore::None` (`:1119`), `propagate: None` (`:1123`),
  each under a comment saying why. It is outside 88's write-effect scope by construction.
- What 88 gains from 84 landing first: one enum to reason about instead of two, so "the written-key
  set carried by `Done`" is stated once. Not a dependency; a convenience.

**No ordering constraint. 84 is type dedupe only — no behaviour change, per the brief's ruling.**

### vs proposal 86 (PN8 + PN9, RESP3 egress codec) — **correcting rev 1's attribution**

**Rev 1 credited PN8 (`estimate_*` → `WireResponse::encoded_len`) to proposal 67. That is the wrong
owner.** Verified two ways: `grep -n 'util.rs\|estimate_' 67-server-small-dedups.md` returns
**nothing** — 67 contains zero references to `connection/util.rs` — and proposal 86 (committed,
`86-resp3-egress-codec.md`) declares PN8 as its own candidate in its header line `:3` and lists
`connection/util.rs` in its own §Files involved (`:75`). 86 independently flags the same
mis-attribution in its §Corrections (`:23`). **The owner is 86; the ordering conclusion is
unchanged.**

The sibling edge, restated against the right proposal: **86 and 84 are adjacent in `util.rs` and do
not touch.** 84 deletes `:74-126` (`convert_blocking_op` + `convert_direction`) and edits the
import at `:8`; 86 deletes `:128-146` (`estimate_resp2_frame_size`) and, in its separable ingress
half, `:148-163` (`estimate_command_size`).

**One sub-claim from the review does not verify and is refuted here.** The review characterised the
adjacency as "`:126` vs `:128` — not overlapping, **not even sharing 3-line diff context**". The
first half holds; the second does not. `:127` is a **single blank line** — the only thing between
84's last deleted line and 86's first — so the two hunks' three-line contexts *do* overlap and
`git diff` will render them as one region. The **merge** is nonetheless clean: the changed ranges
are non-overlapping and separated by an unchanged line, and both sides are pure deletions.
**Either order** — the review's conclusion is right; its stated reason is one line optimistic. If
both land, `util.rs` drops ~90 lines and keeps `raft_op_to_command`, which is the outcome
§Problem 4 argues for.

### vs proposal 66 (ShardWorker builder) and 67 (server small dedups)

**Neither shares a file with 84.** 66's `builder.rs` is not in 84's file set, and 67 — contrary to
rev 1 — has no `connection/util.rs` interest at all (see the correction above). Nothing to
coordinate in either direction.

### Other risks

- **Full-workspace rebuild.** `frogdb-protocol` is the dependency-graph leaf; adding a module to it
  invalidates every downstream crate. This is a wall-clock cost on the reviewer's machine
  (local-mode build), not a design risk, and it is the *only* reason to hesitate about calling this
  **S**. Mentioned so the implementer plans one full `just test` rather than iterating per-crate.
- **`PartialEq` becomes available on `frogdb_core::BlockingOp`.** Purely additive; no existing code
  can break. It does make `assert_eq!` on a `BlockingOp` newly compile, which future tests may
  reach for — fine.
- **`Direction::parse`'s surviving implementation is the allocation-free one.** Verified identical
  in result for all inputs (both are exact ASCII-case-insensitive equality against `LEFT`/`RIGHT`;
  every non-matching input, ASCII or not, yields `None` from both). The three live call sites
  (`commands/src/blocking.rs:230`, `:231`, `:365`) already use it.
- **A latent, unclaimed observation for 81/88's authors, recorded because it was found here.**
  `core/src/shard/blocking.rs:1097` (rev 1 cited `:1101`) resolves the per-key index with
  `entry.keys.iter().position(|k| k == key).unwrap_or(0)`, then indexes `after_ids` with it at
  `:1098`. For `XREAD … STREAMS s s 5 9` — the same stream named twice with different IDs —
  `position` returns `0` both times, so **the second stream's ID is silently ignored**: no panic,
  no error, just a read from the wrong offset. Review confirmed the defect is real and confirmed
  that **this refactor neither fixes nor worsens it** — `:1097` is untouched and the value it
  indexes is the same value in a different representation. It is the same duplicate-key family as
  81's §Problem 4 `BLPOP k k` leak, it is **not** caused by the duplication this proposal removes,
  and it is not a one-line fix. **Recorded, not claimed** — and deliberately not folded into H1/H2,
  because a behaviour fix does not belong in a representation-only commit.
- **Security.** Nothing in the edited set touches authentication, ACL, input sanitisation, or an
  untrusted parse boundary; `Direction::parse` is a two-literal comparison on an already
  length-checked argument. **No security finding to file.** (Per the standing policy, any such
  finding would be recorded here and parked, not fixed in a refactor commit.)

## Effort

| Part | Effort | Notes |
|---|---|---|
| **PN6** — new `protocol/src/blocking.rs`; `timeout_reply` + its test relocate; `frogdb-types` 105 lines → 1; adapter 52 lines deleted; 10 mechanical lines across `frogdb-server`/`frogdb-core`; 2 doc lines | **S** | **8 files** (7 code + 1 doc page). No spec edit, no gate change, no wire change. The only non-mechanical judgement in the whole change is the `after_ids` representation, ruled in §(R2). |

**Line accounting, restated honestly** (rev 1 collapsed two different numbers into one "≈156 net
deleted", which was wrong):

| Number | What it is | Load-bearing? |
|---|---|---|
| **≈156** | **Duplication removed**: 105 lines of `frogdb-types` (`types/mod.rs:322-426`) + 52 lines of `frogdb-server` (`util.rs:74-126`). | **Yes — this is the argument.** These lines exist only because one concept has two declarations, and after the change nothing occupies their place. |
| **≈ −123** | **Whole-diff net**, all files: −78 (`response.rs`) +157 (new `blocking.rs`) +1 (`lib.rs`) −148 (`types/mod.rs`: 105 declarations + 44 relocating test, +1 `pub use`) −53 (`util.rs`) −2 (`connection/blocking.rs`) +1 (`shard/blocking.rs`). | Reported for planning, not as the case. Most of the difference between the two numbers is **relocation**, not deletion. |
| **≈157** | The new `protocol/src/blocking.rs` — 78 enum + 31 `timeout_reply` + 44 test + ~4 header. Rev 1 said "~110" by forgetting the test. | Sizing only. |

**Churn at use sites, restated:** zero at the 164 `BlockingOp` and 61 `Direction` mentions —
*except* six one-line `after_ids` construction sites and one import line, all mechanical, all in
test modules or `use` lists (§The `after_ids` churn ledger). That is still the reason this is **S**
and not the brief's **M**; the correction changes the sentence, not the rating.

**Landable independently** of 80, 81, 86, 88, 66 and 67. Two soft preferences, neither a
constraint, both presented for the orchestrator to rule on: **80-then-84** (saves 84 re-deriving
two `response.rs` numbers) and **81-then-84** (saves 84 re-deriving four `wait_queue.rs` numbers).
Both point the same way — 84 goes last among its `response.rs`/`wait_queue.rs` neighbours — and
neither costs anything if ignored.

## Independently-landable hotfixes

No **LIVE** defect was found in this file set, and this section says so rather than inflating a
tidy-up into a bug. §Problem 4 explains why: the duplication is compiler-guarded, so it cannot
drift silently. Two latent one-line items, both claimed:

**H1 — delete dead `frogdb_types::Direction::parse` (LATENT, claimed).**
`types/src/types/mod.rs:331-340`. Zero callers tree-wide (`rg 'Direction::parse'` → three sites,
all on `frogdb_protocol::Direction`). Ten lines, pure deletion, no behaviour change. **Landable on
its own today**, before and independently of the fold — and if the fold never lands, it is still
right. **Review-confirmed dead three ways** (no callers, `frogdb-commands` imports protocol's copy
at `blocking.rs:12`, and the lint that would have said so is structurally silenced — §Problem 3).

**H2 — the doc comment on `protocol::BlockingOp` describes a design that does not exist (LATENT,
claimed).** `protocol/src/response.rs:497-498` — rev 1 cited `:496-498`, but `:496` is a bare `///`
separator and `:495` is the summary line; **only two lines change**:

```rust
-/// This is a simplified version for use in Response::BlockingNeeded.
-/// The connection handler converts this to the full BlockingOp in frogdb_core.
+/// Carried by `Response::BlockingNeeded` and `InternalAction::BlockingNeeded`, and parked
+/// verbatim on the shard's wait queue as `WaitEntry.op`.
```

Neither copy is "simplified" and neither is "full" — nine variants each, same fields, same names
(§Problem 1). The comment is what sent the lane brief, proposal 81, and the orchestrator's own
framing toward a `frogdb-types` home that does not compile, so it has already cost three readings.
**Two lines, documentation-only, zero risk**; land it with H1 whether or not the fold proceeds.
Once the fold lands the comment's replacement moves with the type into `protocol/src/blocking.rs`.

**Both hotfixes stand as filed** (review verdict: CONFIRMED; H2's line cite corrected above). The
hotfix list is unchanged at **two**: H1 and H2. The XREAD duplicate-key defect (§Other risks) is
**not** promoted to a hotfix — it is a behaviour bug in a proposal whose whole claim is that it
changes no behaviour.

## Revision ledger (rev 2)

Review verdict: **AMEND**. Direction (`types` → `protocol`), home choice, R2 rejection, the
serialization proof, the spec/LOCKED clearance, and both hotfixes all **stand**. One headline claim
was false.

### Applied — corrections that verified and are now in the document

| # | Correction | Where it landed | Evidence re-derived at HEAD |
|---|---|---|---|
| 1 | **"Zero use-site churn" was false.** The `after_ids` type change forces six construction-site edits, not one. | New §The `after_ids` churn ledger; §Summary; §Corrections row; §(d); §Effort | `rg -n after_ids` → `types/mod.rs:642`, `wait_queue.rs:821/844/875/905`, `blocking.rs:1579` |
| 2 | **`wait_queue.rs` is not read-only.** Reclassified "Read-only, must NOT be edited" → **"Primary, 4 test lines"**. | §Files involved; §Risks vs 81; §Spec impact | The four literals above, in tests `:809`, `:833`, `:866`, `:892`, inside the `#[cfg(test)]` module opening at `:672` |
| 3 | **`core/shard/blocking.rs` is 3 lines, not 2** (`:13`, `:1098`, `:1579`). | §Files involved; §(d) | `blocking.rs:1578-1579` |
| 4 | **Orphaned import.** `util.rs:8`'s `StreamId` has its only use at `:102`, inside the deleted adapter → `unused_imports` under `-D warnings`. | §Files involved; §(d); §The churn ledger | `grep -n StreamId util.rs` → `:8`, `:102` only; `Justfile:320` |
| 5 | **The −4 cite cluster in §(d)**, which an implementer would have pasted at the wrong lines: `:1102`→**`:1098`** (6 occurrences tree-wide in the doc), `:1107`→**`:1103`**, `:1101`→**`:1097`**, `:1114-1122`→**`:1113-1123`**. | §(d) diff block, §Problem 2, §Deletion test, §Spec impact, §vs 88, §Other risks | `sed -n '1090,1130p' blocking.rs` |
| 6a | §Serialization safety's lead-in said "verified **four** ways" over a **five**-item list; corrected to five. | §Serialization safety | the list itself |
| 6 | Ten further cite fixes: `blocking.rs:836-837`→**`:835-836`**; `direction_arg` **`:1189-1194`**; `types/mod.rs:341`→**`:342`**; `protocol/lib.rs:19-22`→**`:20-23`** (both occurrences); H2 quote **`:497-498`**; `types/lib.rs` both names on **`:65`**; `is_internal` **`:754-762`**; `narrow_to_wire` **`:41-53`**; `stream.rs:96`→**`:97`** (`parse_for_add`); `cluster-failure-modes` `Forced by` **`:624`**. | throughout | each read directly at HEAD |
| 7 | **`raft_op_to_command` geometry.** "Eleven lines above" was wrong: doc `:165`, fn `:183`, a 39–57 line gap with `estimate_resp2_frame_size` (`:128-146`) and `estimate_command_size` (`:148-163`) between. | §Problem 4 | `awk 'NR>=120 && NR<=190' util.rs` |
| 8 | **Net-lines restated** — 156 = duplication removed (the load-bearing figure); whole diff ≈ **−123**; new module ≈ **157**, not ~110. | §Summary; §Effort (three-row table); §Files involved; §(a) | arithmetic over the seven per-file deltas, each cited |
| 9 | **N3 strengthener** — `raft_op_to_command` **has** a test (`raft_op_to_command_maps_every_variant`, `util.rs:374`, 11 variants). Contrast sharpened to *tested transformer vs untested identity adapter*. | §Problem 4; §Testability 1 | `util.rs:370-374` |
| 10 | **N4 strengthener** — `StreamId` has `impl Display` (`stream.rs:158`), and `StreamIdParseError` another (`:259`). R2 would drag a rendering impl into the wire crate beside a lint-pinned one. | §(R2) | `grep -n 'impl std::fmt::Display for' stream.rs` |
| 11 | **N5 systemic lesson** — `pub` items in a `publish = false` workspace lib crate are invisible to `dead_code`. That is *why* `Direction::parse` survived, and it applies to every crate under `frogdb-server/crates/`. | §Problem 3 | `grep -L 'publish = false' frogdb-server/crates/*/Cargo.toml` → nothing |
| 12 | **Four omitted resolver sites added**: `shard-harness/tests/scenario_s1.rs:13`, `scenario_s2.rs:26`, `scenario_s5.rs:16`, `shard_driver.rs:189`. | §(c) resolution table | `rg -n BlockingOp frogdb-server/crates/shard-harness/` |
| 13 | **PN8 mis-attribution.** Rev 1 credited `encoded_len`/`estimate_*` to proposal **67**; the owner is proposal **86**. New §vs 86 with the sibling edge; §vs 66/67 reduced to "no shared file". | §Risks | `grep -n 'util.rs\|estimate_' 67-server-small-dedups.md` → nothing; `86-resp3-egress-codec.md:3`, `:23`, `:75` |
| 14 | **Ordering vs 81 replaced.** "No ordering constraint" → **81-then-84 is cheaper** (81's 75 lines of dead-method deletion above `:672` shift all four of 84's `wait_queue.rs` numbers). No textual conflict either way. | §Risks vs 81 | `81-core-dead-seams.md:84`; `wait_queue.rs` `#[cfg(test)]` at `:672` |
| 15 | **New adjacency noted**: 84's `blocking.rs:1579` sits 16 lines above 81's `:1595` dummy-channel edit, same test module, disjoint. | §Files involved; §Risks vs 81 | `81-core-dead-seams.md:76` |
| 16 | **Found during revision, not in the review**: `website/src/content/docs/architecture/blocking.md` `:37` (names `types/src/types/mod.rs` as the home) and `:49` (`after_ids: Vec<StreamId>`) both go stale. Added as a 2-line doc edit in the same commit. | §Files involved; §(d) | `rg -n after_ids website/` |

### Refuted — asserted in review, does not verify

| Claim | Finding |
|---|---|
| The `util.rs` edge with 86 is "not overlapping, **not even sharing 3-line diff context**". | **Half right.** Not overlapping: confirmed (84 ends `:126`, 86 starts `:128`). But `:127` is a single blank line, so the two hunks' three-line contexts **do** overlap and `git diff` renders them as one region. The *merge* is still clean (non-overlapping ranges, one unchanged line between, both sides pure deletions) and the **"either order" conclusion stands** — only the stated reason is one line optimistic. Recorded in §vs 86 rather than silently adopted. |

### Withdrawn — rev 1 claims retracted outright

| Withdrawn claim | Replaced by |
|---|---|
| "**Zero use-site churn**" / "**Zero use-site edits** in `frogdb-core`, `frogdb-shard-harness`, or any test" (§Corrections, §Summary, §Effort) | "Zero churn at the 164 `BlockingOp` and 61 `Direction` mentions, **except six `after_ids` construction sites and one import line** — all one-line, all in test modules or `use` lists." |
| "`wait_queue.rs` — **Read-only, must NOT be edited**" (§Files involved) | "**Primary, 4 test lines.**" Its *production* region remains genuinely read-only. |
| "84 and 81 are **disjoint**; **no ordering constraint**" (§Risks vs 81) | Two shared files, no textual conflict, **81-then-84 preferred** on line-number stability. Presented, not ruled. |
| "All existing tests **compile unedited**" (§Testability, regression net) | Five tests change one don't-care literal each; no assertion, name, arrangement, or expected value moves. Stopping rule re-bounded accordingly. |
| "Net **≈156 lines deleted, ~0 added**" (§Effort) | Three distinct numbers, separated: 156 duplication removed / ≈ −123 whole-diff net / ≈157 new module. |
| "PN8 belongs to proposal **67**" (§Risks) | PN8 belongs to proposal **86**. |
| "`raft_op_to_command` sits **eleven lines** below the adapter" (§Problem 4) | 39–57 lines, with two other functions between; the contrast is of kind, not proximity. |

### Unchanged — review-verified, not weakened

Leaf-dependency direction (`frogdb-protocol` has zero `frogdb-*` deps; `types/Cargo.toml:14`
depends on it) · home = `frogdb-protocol` · the re-export chain resolving all 164 sites with no
`E0252` and no aliasing · `Direction::parse` dead three ways · **R2 rejected** (now on three
grounds: clock seam, four types, `Display`) · the **five-leg serialization proof** (zero trait
impls tree-wide; replica-feed bytes provably unchanged via `direction_arg`'s static `LEFT`/`RIGHT`
and the `SynthesizedCommand` `LMOVE`; `Debug`-of-`XRead` confined to test panic messages; zero hits
across all six locked crates) · `FM-TXN-044` clear · `coordinator.rs` at **zero edits** ·
`blocking-failure-modes.md` **not LOCKED** · the clock-seam cites `:16`/`:76` · the XREAD
duplicate-key latent bug **REAL** and correctly dispositioned as "recorded, not claimed" · **H1**
and **H2** both confirmed.
