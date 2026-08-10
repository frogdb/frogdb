# Proposal 80 — `Response` / `WireResponse` / `InternalAction`: sixteen variants, declared twice, plus a narrowing that is not total

Round 38 · lane: protocol / net / core · candidate **PN1** · effort **M** (fold) + **S** each
(five hotfixes, independently landable) · **no locked crate edited**, **no FM tag exists in any
file this proposal edits**, **one seam lint constrains the change** (`lint-error-sanitize`)

**Revision 2 — adversarial review verdict AMEND.** Every review finding was re-derived against the
tree before being applied; two were refuted with evidence and one first-draft claim is withdrawn.
See the [Review ledger](#review-ledger) at the end for the item-by-item disposition.

**Verified at HEAD `49a21b18`** (worktree `arch-round-38-99`, branch `main`). Revision 1 was
derived at `54baa2bb`; `git diff --name-only 54baa2bb..HEAD | grep -v '^\.scratch/'` is **empty**,
so every revision-1 line number was carried forward against an unchanged source tree, and every
line number cited below (new or carried) was re-checked at `49a21b18`.

## Corrections to the lane brief

Every brief claim was re-derived. Six are wrong, and one of the six changes the shape of this
proposal.

| Brief claim | Verified at HEAD |
|---|---|
| "6386 `Response::` call sites" | **6458** occurrences of `Response::` (`git grep -o 'Response::' -- '*.rs' \| wc -l`), of which **6315** are bare `Response::` and **143** are `WireResponse::`. By *lines*: 6327 / 6192 / 135. Cite the metric — the four numbers differ and the brief did not say which it meant. |
| "`into_wire`/`from_wire` ~250 lines of pure recursion" | **107 lines.** `into_wire` is `response.rs:770-837` (68), `from_wire` is `response.rs:843-881` (39). |
| "~34 non-protocol internal-variant uses" | **53 lines across 10 files**, but only **30 are code**. Decomposition (`git grep -nE '(BlockingNeeded\|RaftNeeded\|MigrateNeeded\|SlotMigrationNeeded)' -- '*.rs' ':!…/response.rs'`): **19 comment lines** (`admin.rs` 14, `execution.rs` 1, `cluster/mod.rs` 1, `migrate_cmd.rs` 1, `connection/cluster.rs` 2), **4 already-`InternalAction::` match arms** (`bindings.rs:184,190,196,202`), and **30 real code sites** = **25 producers** (`admin.rs` 14, `blocking.rs` 8, `stream/read.rs` 2, `version.rs` 1) + **4 `handle_internal_action` arms** (`dispatch.rs:287,290,297,298`) + **1 `matches!`** (`execution.rs:626`). The churn figure that matters for the fold is **25**, not 53 and not 27 (revision 1's number — withdrawn). Inside the protocol crate: 42 more, all in `response.rs`. |
| **PN1 rated "Latent"** | **Wrong — there is a LIVE, reachable, wholly untested defect** in exactly the seam PN1 names. `MULTI; CLUSTER ADDSLOTS 0; EXEC` on a Raft node **whose `admin.enabled` is `false` (the shipped default) or on the admin port** returns `-ERR internal action reached response encoder`, discards *every other command's* result in the transaction, and silently no-ops the cluster operation. The reachability condition is load-bearing and revision 1 omitted it; full proof chain, gate analysis and default-config note in §Problem 4. |
| CT7's "~43 `is_resp3` branches" | **86** occurrences on **73** lines. (Recorded for the CT lane; this proposal does not touch them.) |
| "proposal 62 Item B — `codec.rs:254` oversized-bulk rescan" | **Not present in the authored proposal 62.** That item is unclaimed by any document on disk. Flagged, not adopted. |

Two further findings the brief did not name: the `RaftNeeded` payload is **boxed in one enum and
unboxed in its twin** (§Problem 7), and there is a **dead `.expect` panic surface** on the public
API (§Problem 5, hotfix H2).

## Summary

`frogdb-protocol` declares the RESP value tree **twice**: `WireResponse` (16 variants,
`response.rs:116-175`) and `Response` (the same 16 variants, byte-identical, plus 4 internal
control-flow variants, `response.rs:647-743`). A third type, `InternalAction`
(`response.rs:40-73`), declares those 4 internal variants a second time. A 107-line hand-written
recursion (`into_wire`, `from_wire`) exists solely to walk one tree into the other.

The duplication is not merely verbose. It buys a stated safety property —
*"an internal action is structurally unrepresentable past this point"* (`frame_io.rs:39-40`) — and
**it does not deliver it**, because the property it enforces is *"no internal action at the root"*
while the type it enforces it on is *recursive*. A `Response::Array` containing a
`Response::RaftNeeded` fails the narrowing at the root, and the whole reply is thrown away. That
path is live today (§Problem 4).

The proposal: make the RESP tree **one** recursive type generic over its internal-action payload —

```rust
pub enum Resp<I> { Simple(Bytes), … , Internal(I) }   // 16 wire variants + 1
pub type WireResponse = Resp<Infallible>;              // the 17th arm is uninhabited
pub type Response     = Resp<Box<InternalAction>>;
```

— which deletes 16 duplicate variant declarations, deletes `from_wire` entirely, replaces the
"trust the two-enum argument" safety story with a **compiler-discharged uninhabited arm**
(`Resp::Internal(never) => match never {}` — no `unreachable!()`, no panic), shrinks the type used
6315 times from **128 bytes to 40** (measured, §Problem 3), and — because Rust resolves enum
variants through type aliases — **changes none of the 6458 `Response::`/`WireResponse::` call
sites**. Verified by compiling a standalone model of the formulation (§Proposed change).

Five hotfixes are separable and land alone: **H1** the LIVE EXEC defect (spec-first — the behaviour
it changes is EXEC reply semantics, squarely the LOCKED **txn** *area*, so it needs an FM row and a
forcing test first, even though every candidate fix site is in an unlocked crate); **H2** delete a
dead `.expect` panic; **H3** delete three zero-caller public methods; **H4** correct a doc comment
that asserts the invariant §Problem 4 disproves; **H5** delete `Response::MigrateNeeded`, a variant
with zero production producers.

## Files involved

Line counts at `49a21b18`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/protocol/src/response.rs` | 1770 (941 code + 829 tests, 45 `#[test]`) | **Primary.** Owns all three types, both recursions, both encoders, `sanitize_error_message`. |
| `frogdb-server/crates/protocol/src/lib.rs` | 27 | Re-export list `:20-23` (`InternalAction, Response, WireResponse, WireResult, sanitize_error_message`) — the `WireResult` token sits on **`:22`** and dies with H3. |
| `frogdb-server/crates/server/src/connection/frame_io.rs` | 283 | The encoder boundary. This proposal owns **`narrow_to_wire` `:31-53`** (doc `:31-40`, `fn` `:41`, degrade arm `:44-51`) **only** (see §Boundary vs 86). |
| `frogdb-server/crates/server/src/connection.rs` | 922 | 4 `narrow_to_wire` sites (`:409`, `:552`, `:719`, `:731`); the false comment at **`:548`** (H4 — revision 1 said `:547`, which is the preceding "Feed responses into the write buffer" line). |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 1177 | `handle_internal_action` `:284-301` (4 internal arms at `:287`, `:290`, `:297`, `:298`); its **sole** call site `:779`. The `:297` arm is deleted by H5. |
| `frogdb-server/crates/server/src/connection/transaction.rs` | — | **Hotfix H1 candidate site.** `ConnectionHandler::handle_exec` `:37-44` owns the `Vec<Response>` that `frogdb_txn::handle_exec` returns (`:43`). Unlocked crate; see H1 option 3. |
| `frogdb-server/crates/core/src/scripting/bindings.rs` | 420 | The only non-`frogdb-server` consumer of the split: `response_to_lua` `:82-87`, `wire_response_to_lua` `:93-176`, `internal_action_to_lua` `:182-209`. |
| `frogdb-server/crates/server/src/commands/cluster/admin.rs` | 662 | **14 producers** (10 `RaftNeeded` at `:82,:118,:177,:225,:301,:338,:375,:422,:468,:643`; 4 `SlotMigrationNeeded` at `:561,:602,:633,:654`) plus 14 comment lines naming the variants. Mechanical churn only. (Revision 1's "28 producer lines" conflated the two — withdrawn.) |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | 8 `BlockingNeeded` producers. Mechanical churn only. |
| `frogdb-server/crates/commands/src/stream/read.rs` | — | 2 `BlockingNeeded` producers (`:112`, `:352`). Mechanical churn only. |
| `frogdb-server/crates/server/src/commands/version.rs` | — | 1 `RaftNeeded` producer (`:123`, `FROGDB.FINALIZE`). Mechanical churn only. |
| `frogdb-server/crates/server/src/commands/migrate_cmd.rs` | — | **Hotfix H5 evidence.** `execute` `:41-54` deliberately returns `Err(CommandError::Internal)` (`:51-53`) rather than a `MigrateNeeded` signal. Not edited. |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | **Hotfix H1, option 2 only** (`:626`). Not edited by the fold, and not edited at all under H1 option 3. |

Read-only evidence, not edited: `frogdb-server/crates/txn/src/exec.rs:347` (**LOCKED crate — must
not be touched**), `frogdb-server/crates/server/src/connection/transaction.rs:133-145`
(`deferral_of`), `frogdb-server/crates/server/src/commands/cluster/mod.rs:88-108` (`:105` =
`ExecutionStrategy::Standard`), `frogdb-server/crates/core/src/command_spec.rs:520-559` +
`:584-599` (`AdminSurface` / `SPLIT_ADMIN_SURFACES`),
`frogdb-server/crates/server/src/connection/guards.rs:347-360` (the NOADMIN gate),
`frogdb-server/crates/config/src/admin.rs:43-50` (`AdminConfig::default().enabled == false`),
`frogdb-server/crates/server/src/connection/pubsub_conn_command.rs:961-962` (the
no-queue-time-reject policy), `frogdb-server/crates/server/src/connection/util.rs:130-163`,
`frogdb-server/crates/protocol/src/reply.rs` (168 lines, `MapReply`),
`scripts/error-sanitize.py`, `scripts/failure-modes.py:64-77` (`NEXTEST_CRATES`), `Justfile:1249`,
`Justfile:272-279` (`mutants-diff` / `mutants-gate`).

## Problem

### 1. Sixteen variants, declared twice, byte-identical

`WireResponse` (`response.rs:116-175`) and `Response` (`response.rs:647-743`) declare the same 16
variants — `Simple(Bytes)`, `Error(Bytes)`, `Integer(i64)`, `Bulk(Option<Bytes>)`, `Array(Vec<…>)`,
`Null`, `NullArray`, `Double`, `Boolean`, `BlobError`, `VerbatimString{format,data}`, `Map`, `Set`,
`Push`, `Attribute{attrs,data}`, `BigNumber` — with identical payload types, identical ordering,
and near-identical doc comments (`Response`'s block at `:639-645` even re-explains the split it
is one half of). Six constructors are duplicated too: `ok`/`error`/`null`/`bulk`/`pong`/`queued`
at `WireResponse` `:226-254` and again at `Response` `:887-914`, with bodies differing only in the
type name.

Adding a RESP variant today means editing **six** places: two enum declarations, two constructor
sets (if applicable), the `into_wire` arm, the `from_wire` arm — before touching either encoder.
Nothing in the type system notices if you edit five.

`InternalAction` (`response.rs:40-73`) then declares the 4 control-flow variants a **third** time
against `Response`'s copies at `:700-743`.

### 2. A 107-line recursion whose only job is to copy a tree onto itself

`into_wire` (`response.rs:770-837`) is 68 lines of `Response::X(a) => Ok(WireResponse::X(a))`, with
five arms doing `Result`-collecting recursion into children. `from_wire` (`:843-881`) is the same
39 lines in reverse. Both are pure structure: they carry **zero** protocol decisions. Every decision
lives in `to_resp2_frame` (`:274-334`) and `to_resp3_frame` (`:341-432`), which the recursion feeds.

`from_wire` has **zero non-test callers** in the tree.

`is_internal` (`:754-763`) is a fourth encoding of the same classification — a `matches!` over the
identical 4 variants that `into_wire`'s tail arms already discriminate. Zero non-test callers.

### 3. `Response` is 3.2× larger than the value it carries

Measured in-tree with a temporary `size_of` test compiled against the real payload types
(`just test frogdb-protocol`, then reverted — no repo file was left modified):

| Type | `size_of` |
|---|---|
| `WireResponse` | **40** |
| `Response` | **128** |
| `InternalAction` | 120 |
| `WireResult = Result<WireResponse, InternalAction>` | 120 |
| `BlockingOp` | 88 |
| `RaftClusterOp` | 72 |
| `SlotMigrationKind` | 24 |
| *folded* `Response` (16 wire variants + `Internal(Box<InternalAction>)`) | **40** |

`Response` is the return type of every command in the tree (6315 bare `Response::` occurrences) and
the element type of every `Vec<Response>` batch. It pays 88 bytes per value for four variants that
are produced at **25 sites** (see the corrected count in §Corrections) and that cannot appear on the
wire at all. The `Vec<Response>` that EXEC builds (`exec.rs:347`) pays it per element.

The oversize is already visible in the code as an apology: `#[allow(clippy::result_large_err)]` at
`response.rs:769`, suppressing clippy's complaint about `WireResult`'s 120-byte error arm.

**Independent corroboration of the folded layout.** The review re-derived the 40-byte figure from a
standalone model (`rustc --edition 2024 -O`, `bytes::Bytes` modelled as `NonNull<u8> + usize +
NonNull<()> + NonNull<()>`, the internal payloads as same-sized stand-ins) and reached the same
numbers: `WireResponse = 40`, `Resp<Box<InternalAction>> = 40`, `Result<WireResponse,
Box<InternalAction>> = 40`. Three properties fall out of that model and are worth stating because
each is a way the fold could have failed and did not:

- **The 40 depends on `Bytes` carrying a pointer niche.** `Option<Bytes>` measures 32 in the model
  (the niche absorbs the `None` tag) and the enum's own discriminant rides in `VerbatimString`'s
  `[u8; 3]` padding. A naive no-niche model (raw `*mut` instead of `NonNull`) gives `Option<Bytes>
  = 40` and `WireResponse = 48`. The real `bytes::Bytes` does have the niche, so **40 is the number
  to plan against — but it is a layout consequence, not a guarantee**, and it should be re-measured
  in-tree at implementation time rather than treated as a contract.
- **Same-named inherent methods on the two instantiations compile.** `impl Resp<Infallible> { fn
  tag(&self) }` and `impl Resp<Box<InternalAction>> { fn tag(&self) }` coexist without conflict
  (disjoint instantiations, no overlapping-impl error). This matters because today's
  `WireResponse` and `Response` both carry `ok`/`error`/`null`/`bulk`/`pong`/`queued` — the fold can
  keep both constructor sets verbatim if it wants to, and the 6 duplicated bodies are a *choice* to
  collapse, not a forced merge.
- **Variant *paths* resolve through an alias; variant *imports* do not.** `Response::Integer(42)`
  and `Response::VerbatimString { format, .. } => …` compile; `use Response::Integer;` fails with
  **E0432** (*"`Response` is a type alias, not a module"*). This is the one call-site-breaking
  pattern the fold cannot absorb — and the tree contains **zero** of it: `git grep -nE '^\s*use
  .*(Response|WireResponse)::'` returns no hits. Stated as examined-and-absent rather than
  unexamined. Relatedly, **`frogdb-protocol` has no serde dependency at all** (`Cargo.toml`:
  `bytes`, `bytes-utils`, `redis-protocol`, `ryu`, `tokio-util`, `thiserror`, `tracing`; dev-only
  `tokio`, `proptest`), and `response.rs` contains no `Serialize`/`Deserialize`, so the other
  classic alias hazard — a derived serde impl naming the concrete type — does not exist here
  either.

### 4. The narrowing is not total, and the gap is LIVE

`frame_io.rs:33-40` states the design's central claim:

> Internal control-flow actions are resolved upstream (see `handle_internal_action`) before a
> response is handed to the encoder, so this narrowing is total in practice. […] an internal
> action is structurally unrepresentable past this point.

The second half is true (the encoders take `WireResponse`). The first half is false. Proof chain,
every link verified at HEAD:

0. **The admin gate must be clear first — this is the condition revision 1 omitted.** `CLUSTER` is
   *not* wholly admin: it carries `CommandFlags::STALE` only, and its admin surface is split per
   subcommand in `SPLIT_ADMIN_SURFACES` (`core/src/command_spec.rs:584-599`), whose public list is
   exactly `INFO, NODES, MYID, SLOTS, SHARDS, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, HELP`.
   `ADDSLOTS` is not on it, and `AdminSurface::requires_admin` fails closed (`:545-548`: an
   unlisted subcommand — or none at all — is admin). The gate that acts on that verdict is
   `run_pre_checks` (`guards.rs:347-360`) and it is conditional:
   ```rust
   if self.admin_enabled && !self.is_admin && … .requires_admin(…) { return Some(NOADMIN…) }
   ```
   `DispatchStage::PreChecks` is **index 1** of `PRE_DISPATCH_ORDER` (`dispatch.rs:123-141`),
   `DispatchStage::TransactionQueue` is **index 5**, so the gate runs *before* queuing; and a
   pre-check refusal poisons the transaction (`dispatch.rs:477-484` calls
   `view.state.abort_transaction(msg)`). **On a connection where `admin_enabled && !is_admin`
   holds, the repro yields `-NOADMIN` at queue time and `-EXECABORT` at EXEC — not the defect.**

   The defect therefore requires `is_admin == true` (admin port) **or** `admin_enabled == false`.
   **`admin_enabled == false` is the shipped default**: `AdminConfig::default()` sets
   `enabled: false` (`config/src/admin.rs:43-50`) and the field is `#[serde(default)]` on a `bool`
   (`:17-19`), so a config that does not mention `admin.enabled` leaves the whole NOADMIN gate
   inert and `CLUSTER ADDSLOTS` reachable from an ordinary client port. A separately configured
   ACL (`guards.rs:362-371`, the next gate down in `run_pre_checks`) can still refuse it, but
   nothing does so by default.
1. `CLUSTER` declares `strategy: ExecutionStrategy::Standard` and `keys: KeySpec::None`
   (`cluster/mod.rs:88-108`, `:105` for the strategy).
2. `deferral_of` (`transaction.rs:133-145`) defers only `ConnectionLevel(_)` and `ServerWide(op)`;
   `_ => None`. **`CLUSTER` inside `MULTI` is therefore queued and shipped to the shard batch**,
   not intercepted at the connection layer. `KeySpec::None` also means the queue-time cross-slot
   validation has nothing to object to.
3. `CLUSTER ADDSLOTS` on a node with Raft returns `Response::RaftNeeded { op:
   RaftClusterOp::AssignSlots { … }, … }` — `cluster_addslots` is **`cluster/admin.rs:128-186`**,
   the `RaftNeeded` construction is at **`:177`** (`AssignSlots` at `:178`), gated on
   `ctx.raft.is_some()` at `:135-137`. *(Revision 1 cited `admin.rs:82`; that line is
   `cluster_meet`'s `RaftClusterOp::AddNode`. Corrected — and note `CLUSTER MEET` is an equally
   valid repro under the same gate condition.)*
4. The shard batch loop (`core/src/shard/execution.rs:618-632`) filters exactly **one** internal
   variant:
   ```rust
   let response = if matches!(&response, Response::BlockingNeeded { .. }) {
       Response::Null
   } else { response };
   results.push(response);
   ```
   `RaftNeeded` and `SlotMigrationNeeded` fall through into `results` untouched. (`MigrateNeeded`
   would too, but it has **zero production producers** — see H5 — so that half of the fall-through
   is vacuous. Revision 1 listed it as a live case; withdrawn.)
5. EXEC wraps the batch: `let mut result = vec![Response::Array(final_results)];`
   (`txn/src/exec.rs:347`). The internal action is now **nested one level deep**.
6. EXEC dispatches in `DispatchStage::TransactionControl`, which
   `StageOutcome::ShortCircuit`s at `dispatch.rs:523-528` — **before** the `Execute` stage. The
   **only** call to `handle_internal_action` in the entire tree is `dispatch.rs:779`, inside
   `Execute`. EXEC's reply never passes through it.
7. `connection.rs:552` feeds the reply through `Self::narrow_to_wire(response)`.
8. `into_wire`'s `Array` arm recurses, hits `Response::RaftNeeded`, returns `Err`. `narrow_to_wire`
   (`frame_io.rs:44-51`) logs and **replaces the entire array** with
   `WireResponse::error("ERR internal action reached response encoder")`.

Observable result: the client gets one error line instead of the transaction's result array — every
other command's result in that EXEC is silently destroyed — **and the cluster operation never
happens**, because the `RaftNeeded` signal that would have driven it was consumed by the error path.

Same reachability, under the same step-0 condition, for every `Standard`-strategy internal-action
producer: `CLUSTER MEET/FORGET/ADDSLOTS/DELSLOTS/FAILOVER/REPLICATE/RESET/SET-CONFIG-EPOCH/SETSLOT`
(`cluster/admin.rs` `:82,:118,:177,:225,:301,:338,:375,:422,:468,:561,:602,:633,:643,:654` — 14
producers) and `FROGDB.FINALIZE` (`version.rs:123`). Every one of them is behind the same gate:
the `CLUSTER` subcommands via `SPLIT_ADMIN_SURFACES`, `FROGDB.FINALIZE` via whole-command
`CommandFlags::ADMIN` (`version.rs:88`) ⇒ `AdminSurface::Whole`. So the condition is uniform: the
gate must be inert (`admin_enabled == false`, the default) or the connection must be the admin
port. **`MIGRATE` is not affected** — it is `ServerWide`, hence deferred at step 2 — and
`Response::MigrateNeeded` has no production producer at all (H5).

**Blast-radius / security classification.** The review proposed narrowing this to "the admin
surface (frogctl / operator path)" and filing it as a parked security item. **That narrowing is
refuted**: `admin.enabled` defaults to `false` (`config/src/admin.rs:46`), which makes the NOADMIN
gate a no-op on a default deployment and puts `CLUSTER ADDSLOTS` in reach of any ordinary client
connection. The defect is therefore an ordinary correctness defect on the default configuration,
not an admin-only one, and there is no privilege boundary being crossed — a client that can reach
the command at all is a client the deployment already allows to run it. **No security fix is
proposed here** (standing policy: security issues are filed, never implemented in an architecture
proposal); the classification is recorded so the ruling does not have to re-derive it. What *is*
worth recording for whoever owns the admin-port story separately: on a deployment that *does* set
`admin.enabled = true`, the gate closes the plain port and the residual exposure is exactly the
admin port — that is the review's original framing and it is correct for that configuration only.

**Nothing tests this.** `git grep 'internal action reached'` returns exactly two hits, both the
producing code (`frame_io.rs:47`, `:50`). No test anywhere asserts the degraded string, and no test
in the tree runs a `CLUSTER` subcommand inside `MULTI`.

Note what the two-enum design bought here: it converted a panic into a silent, wrong reply. That is
better than a panic and worse than a compile error — and it is precisely the outcome you get when a
non-recursive invariant is enforced on a recursive type.

### 5. A dead panic on the public API, four dead methods, and one dead variant

`response.rs:933-940`:

```rust
impl From<Response> for BytesFrame {
    fn from(response: Response) -> Self {
        response.into_wire()
            .expect("cannot convert internal action to BytesFrame")
            .to_resp2_frame()
    }
}
```

This is a public, infallible-looking conversion that panics on a whole class of input. Its only
callers in the tree are its own two tests (`:955`, `:962`). It is the pre-`narrow_to_wire` design,
left behind.

Also zero non-test callers: `Response::try_to_resp2_frame` (`:920`), `Response::try_to_resp3_frame`
(`:928`), `Response::is_internal` (`:754`), `Response::from_wire` (`:843`). `WireResult` (`:748`)
appears only as `into_wire`'s return type plus the `lib.rs:22` re-export.

Six public API items on the crate's most-used type, none of them reachable from production code.

**And one dead *variant*.** `Response::MigrateNeeded` / `InternalAction::MigrateNeeded` have **zero
production producers**. `git grep -n MigrateNeeded` over the whole tree returns 12 hits: the two
declarations (`response.rs:62`, `:731`), the `is_internal` `matches!` (`:759`), the `into_wire` arm
(`:832`), three `response.rs` *test* constructions (`:1331`, `:1378`, `:1421`), the
`internal_action_to_lua` arm (`bindings.rs:196`), the sole consumer arm
(`dispatch.rs:297 → handle_migrate_command`), and one comment (`migrate_cmd.rs:50`). Nothing
constructs it outside `response.rs`'s own tests, and `migrate_cmd.rs`'s shard executor deliberately
returns `Err(CommandError::Internal { … "server-wide command reached shard executor" })` (`:51-53`)
with the comment "*fail loudly rather than leak an internal `MigrateNeeded` signal*". This is
hotfix **H5** — a pure deletion, and it shrinks the surface the fold must carry from 4 internal
variants to 3.

### 6. The doc comments assert the invariant that §4 disproves

Three places state the false property. `frame_io.rs:33-40` (quoted above). `connection.rs:548`:
*"Internal actions were already resolved by the dispatch layer"* — false for the
`TransactionControl` stage, which is the very stage that produces the nested case.
`frame_io.rs:61-62`: *"it can never encode a control-flow signal and cannot panic on one"* — true
of the encoder, but stated in a way that reads as a claim about the pipeline.

A future reader auditing this seam is told, three times, that the case in §4 cannot happen.

### 7. The same payload is boxed in one enum and not in its twin

`InternalAction::RaftNeeded { op: Box<RaftClusterOp>, … }` (`response.rs:40-73`) versus
`Response::RaftNeeded { op: RaftClusterOp }` (`response.rs:700-743`). `into_wire` re-boxes on the
way out (`:828`). One indirection decision, made twice, differently, for the same value — a direct
consequence of the payload being declared in two places. Under the fold there is one declaration
and the question is asked once.

## Proposed change

Make the RESP tree one recursive type, generic over what an internal action *is*. Interface
unchanged; implementation unified.

```rust
// frogdb-protocol/src/response.rs
#[derive(Debug, Clone, PartialEq)]
pub enum Resp<I> {
    Simple(Bytes), Error(Bytes), Integer(i64), Bulk(Option<Bytes>),
    Array(Vec<Resp<I>>), Null, NullArray, Double(f64), Boolean(bool),
    BlobError(Bytes), VerbatimString { format: [u8; 3], data: Bytes },
    Map(Vec<(Resp<I>, Resp<I>)>), Set(Vec<Resp<I>>), Push(Vec<Resp<I>>),
    Attribute { attrs: Vec<(Resp<I>, Resp<I>)>, data: Box<Resp<I>> },
    BigNumber(Bytes),
    /// The 17th arm: uninhabited for `WireResponse`, `Box<InternalAction>` for `Response`.
    Internal(I),
}

pub type WireResponse = Resp<std::convert::Infallible>;
pub type Response     = Resp<Box<InternalAction>>;
```

`InternalAction` keeps its 4 variants and becomes their **only** declaration; `Response`'s copies
at `:700-743` are deleted.

Three consequences, each load-bearing:

**Call sites do not change.** Rust resolves enum variants through type aliases
(`type_alias_enum_variants`, stable since 1.37; this tree pins 1.92.0). `Response::Integer(42)`,
`WireResponse::Simple(s)`, and struct-variant *patterns* like
`Response::VerbatimString { format, data } => …` all continue to compile verbatim. **Verified by
compiling a standalone model of exactly this formulation** (`rustc --edition 2024 -O`, expression
position + struct-variant pattern position + both aliases): clean compile, correct output. All 6458
`Response::`/`WireResponse::` occurrences are untouched.

**The safety story becomes a compiler proof.** Today's argument is *"`WireResponse` has no internal
variants, therefore the encoder is total"* — a claim about two hand-maintained declarations staying
in sync. After the fold, `to_resp2_frame`/`to_resp3_frame` gain one arm:

```rust
Resp::Internal(never) => match never {},
```

`Infallible` is uninhabited, so `match never {}` type-checks with no arms. This is **not**
`unreachable!()` — there is no panic, no runtime branch, and no `_ =>` catch-all. Adding a 17th wire
variant later fails the encoder's exhaustiveness check; the uninhabited arm keeps working.

**`from_wire` deletes.** A `WireResponse` *is* a `Response` up to the payload type; the widening is
a single generic map, not 39 hand-written arms. `into_wire` keeps only its recursive shape (the
`Ok(Same::X(a))` arms collapse into the generic walk); `is_internal` becomes
`matches!(self, Resp::Internal(_))` if kept at all — H3 deletes it.

**Size.** The folded `Response` measures **40 bytes** (in-tree measurement, §Problem 3) against
today's 128, because the internal payload rides a `Box`. `Result<Resp<…>, Box<InternalAction>>`
measures 40 too, which is expected to let `#[allow(clippy::result_large_err)]` (`response.rs:769`)
be deleted — stated as an expectation to re-check under clippy at implementation time, not asserted.

**Churn inventory (corrected).** Revision 1 said "27 producer sites" and "two 4-arm matches move";
both were wrong. The verified list:

| Site | Change under the fold |
|---|---|
| **25 producers** — `admin.rs` 14, `blocking.rs` 8, `stream/read.rs` 2, `version.rs` 1 | Wrap: `Response::Internal(Box::new(InternalAction::RaftNeeded{…}))`, or keep them one-line behind preserved `Response::raft_needed(…)` / `blocking_needed(…)` / `slot_migration_needed(…)` constructors. **Additionally**, `InternalAction::RaftNeeded` declares `op: Box<RaftClusterOp>` while `Response::RaftNeeded` declares `op: RaftClusterOp` (§Problem 7); folding onto the boxed declaration means each of the **11 `RaftNeeded` producers** (10 in `admin.rs`, 1 in `version.rs`) also gains a `Box::new(…)` around its `RaftClusterOp`. Folding onto the *unboxed* declaration instead avoids that but grows `InternalAction` — a real choice, to be made when the fold is written, not now. |
| `handle_internal_action` (`dispatch.rs:284-301`) | **The only match that actually moves.** Its 4 arms are `Response::X` today (`:287`, `:290`, `:297`, `:298`) and become `InternalAction::X` behind one `Resp::Internal(action) => match *action { … }`. H5 deletes the `:297` arm first, leaving 3. |
| `internal_action_to_lua` (`bindings.rs:182-209`) | **No match rewrite — revision 1 was wrong.** It already matches `InternalAction::BlockingNeeded/RaftNeeded/MigrateNeeded/SlotMigrationNeeded` (`:184`, `:190`, `:196`, `:202`). Untouched by the fold (H5 removes one arm). |
| `response_to_lua` (`bindings.rs:82-87`) | One-token change: the `Err(action)` arm at `:85` now receives `Box<InternalAction>` and needs a deref (`*action`) — or `internal_action_to_lua` takes the box. Not a rewrite. |
| `wire_response_to_lua` (`bindings.rs:93-176`) | **New churn revision 1 missed.** It is an exhaustive match over all 16 `WireResponse` variants (15 arms, `Bulk(None)`/`Null`/`NullArray` sharing one) with **no `_` catch-all**, so under the fold it must gain the uninhabited arm `Resp::Internal(never) => match never {}` or it stops compiling. One line; worth naming because it is the second site (after the two encoders) where the compiler-discharged arm has to be written. |
| `execution.rs:626` `matches!(&response, Response::BlockingNeeded { .. })` | Becomes `matches!(&response, Response::Internal(a) if matches!(**a, InternalAction::BlockingNeeded { .. }))` or equivalent. One line. **This is the same line H1 option 2 would edit** — see H1 for why option 2 is not the leading candidate. |

#### Depth and locality

The module's **interface** is 16 RESP variants plus a control-flow escape hatch, and it does not
change. Its **implementation** is currently 3 type declarations + 107 lines of structural recursion
+ 4 zero-caller methods; it becomes 1 declaration + 2 aliases + the recursion's one surviving half.
That is depth added: the same interface over strictly less machinery, with a stronger guarantee
behind it.

Locality improves at the seam that matters. Today, "can this value reach the encoder?" is answered
by reading two enum declarations 500 lines apart and trusting they agree. After the fold it is
answered by one type parameter, checked by the compiler at the encoder itself.

#### Deletion test, applied honestly

*What breaks if this module is deleted?* — `Response` is the return type of every command; nothing
is deletable. The right question for a fold is **which of the module's parts carry decisions**.

Deleting `WireResponse`'s 16 variant declarations: nothing is lost — they are a copy.
Deleting `from_wire`: nothing is lost — zero callers, and the widening is mechanical.
Deleting `Response`'s 4 internal variant declarations: nothing is lost — `InternalAction` has them.
Deleting `into_wire`'s 11 identity arms: nothing is lost — they carry no decision.
Deleting `into_wire` **entirely**: the *seam* is lost. The recursive check "is there an internal
action anywhere in this tree" is a real decision, and it must survive. It survives as the generic
walk.

**The honest limitation.** This formulation does **not** make §Problem 4 unrepresentable.
`Resp::Array(vec![Resp::Internal(a)])` still type-checks, because `Internal` is still an arm of the
recursive enum. The fold makes the *failure* cheaper to reason about and impossible to reach past
the encoder, but the nesting itself remains legal. **H1 is therefore a separate fix, not a
consequence of the fold, and this proposal does not claim otherwise.**

#### Alternatives weighed

**(B) Fold the 4 internal variants into a single `Response::Internal(InternalAction)`, keep both
enums.** Rejected: it deletes the `InternalAction`/`Response` triplication (§Problem 1's third
copy) but leaves the 16-variant duplication and the whole 107-line recursion in place. It is a
proper subset of (A) for the same call-site churn.

**(C) Move `Internal` out of the RESP tree entirely — make nesting structurally impossible.**
This is the formulation that would kill §Problem 4 by construction. Rejected *for now*, with the
cost measured rather than asserted: it requires either rewriting the 6315 bare `Response::` sites,
or widening the error channel — `Err(CommandError::` appears **552** times, `CommandError::` 893,
and `Command::execute`'s signature is `core/src/command.rs:720`. Routing a control-flow *signal*
through an error type is a second type-level lie traded for the first. Recorded as the stated
follow-up once (A) has collapsed the declaration count that (C) would otherwise have to rewrite
twice.

## Testability improvement

Today the encoder-boundary invariant is untestable *as an invariant*: you can only test that
`narrow_to_wire` degrades gracefully, which is what §Problem 4 shows is happening in production
without anyone noticing. Three concrete gains:

1. **The uninhabited arm is a compile-time test that runs on every build.** No test can be written
   for "the encoder never sees an internal action" today — the guarantee rests on two declarations
   agreeing. After the fold, breaking it does not compile.
2. **The 16-variant round-trip tests collapse to one property.** `response.rs` has 45 tests, many of
   which exist to pin `into_wire`/`from_wire` arms per variant. Under one type they become a single
   generic property over the tree shape, and the per-variant tests can concentrate on encoding
   (`to_resp2_frame`/`to_resp3_frame`) where the actual decisions live.
3. **H1's forcing test is writable today** — but only against a correctly configured host. The test
   must run `MULTI; CLUSTER ADDSLOTS 0; EXEC` on a Raft-mode node **with the NOADMIN gate inert or
   bypassed** (`admin.enabled = false`, the default — or a connection on the admin port). Without
   that pin the test is red for the wrong reason (`-NOADMIN` then `-EXECABORT`, from
   `guards.rs:351-359` + `dispatch.rs:477-484`) and goes green the moment anything at all is
   returned, with or without the fix. The assertion must be on the **array reply and the slot
   assignment**, never on the absence of an error string. Write it **before** the fold, so the
   fold's regression surface is pinned by a red-then-green test rather than by the refactor's own
   assertions.

Mutation-testing note: `frogdb-protocol` is not a gated crate, but the fold removes ~107 lines of
identity-mapping code that a mutation run cannot distinguish from correct code anyway — it raises
the *signal* of any future run on the crate rather than the score.

## Spec / LOCKED impact

**Crates edited by the fold:** `frogdb-protocol`, `frogdb-server`, `frogdb-commands`,
`frogdb-core`. **None is a LOCKED area.** `frogdb-txn`, `frogdb-vll`, `frogdb-persistence`,
`frogdb-recovery`, `frogdb-replication[-runtime]`, `frogdb-cluster[-runtime]` are **not edited** —
in particular `frogdb-txn/src/exec.rs:347` is cited as evidence and must stay untouched.

**FM tags:** `git grep 'FM-'` over the entire `frogdb-server/crates/protocol/` tree returns **zero**
hits. `dispatch.rs` carries real tags only at `:978` and `:1006`, neither in the edited region.
Do **not** add an id-only `// FM-…` comment inside `frogdb-protocol` — the crate is absent from
`NEXTEST_CRATES` in `scripts/failure-modes.py:63-76`, so a tag there would be unenforceable and
would fail `just lint-failure-modes`.

**H1 is the exception and is spec-first.** Every candidate fix site is in an unlocked crate
(`core/src/shard/execution.rs:626` for option 2, `server/src/connection/transaction.rs:37-44` for
option 3), but the behaviour it changes is **EXEC reply semantics**, squarely the LOCKED **txn**
*area*, whose spec already carries the adjacent row **FM-TXN-045**
(`.scratch/hardening/specs/txn-failure-modes.md:566-576`, forced by
`test_exec_nested_null_array_encodes_as_nested_null_resp2`, tagged at
`frogdb-server/crates/server/tests/integration_transactions.rs:1110`, `fn` at `:1122`). H1
therefore follows the locked-area protocol: **new `FM-TXN-NNN` row → failing forcing test → fix**.

**On mutation coverage — revision 1's stated protocol was unsatisfiable and is withdrawn.**
Revision 1 required "the forcing test must live in a crate whose own `cargo mutants -p` run covers
the mutated code". No such crate exists for this row. The four mutation gates are **txn 0.90**
(`frogdb-txn` + `frogdb-vll`), **persistence 0.85**, **replication 0.85**, **cluster 0.80**;
neither `frogdb-core` nor `frogdb-server` carries a gate at all, and `cargo mutants -p frogdb-txn`
mutates only `frogdb-txn`'s own lines — it would never touch `execution.rs:626` or
`transaction.rs:37-44`, whichever site H1 lands on. There is no configuration of the existing
tooling under which a mutation gate scores this fix.

The truth to state plainly: **no mutation gate covers this row; the FM row plus its integration
forcing test is the entire enforcement.** That is sufficient for `just lint-failure-modes`, which
requires only that the row name a test resolvable in `NEXTEST_CRATES` — and the precedent is
immediately adjacent: **FM-TXN-045 is itself forced from `frogdb-server`**
(`integration_transactions.rs`), and `frogdb-server` is in `NEXTEST_CRATES`
(`scripts/failure-modes.py:64-77`, which also lists `frogdb-core`). So a `FM-TXN-NNN` row forced by
a `frogdb-server` integration test is lint-clean and consistent with how the neighbouring row is
already enforced. `just mutants-diff <crate>` remains a push-discipline obligation only if the
change ends up touching a gated crate — under both H1 options it does not.

### Seam lints — one hard constraint, three checks passed

**`lint-error-sanitize` constrains this change and must be read before editing `response.rs`.**
`scripts/error-sanitize.py` hard-codes the path `frogdb-server/crates/protocol/src/response.rs` and
requires `sanitize_error_message` to be the **immediate first token** inside every
`Resp2BytesFrame::Error(` and `Resp3BytesFrame::SimpleError { data:` construction. Compliant sites
today: `:277`, `:292`, `:348`; the `BlobError` exemption at `:378` is deliberate. Consequences:
(a) do not rename or hoist `sanitize_error_message`; (b) **do not relocate the encoders out of
`response.rs`** — the gate would then pass **vacuously** against an empty file. If a future split is
wanted, `RESPONSE` in the script must move in the same commit. This proposal keeps both encoders in
`response.rs` and touches neither.

Passed / not applicable:
- **`lint-format-float`** — `Justfile:1249` pins exactly one `fn format_float`, at
  `frogdb-protocol/src/format.rs`. Not touched.
- **`lint-pubsub-confirmation-seam`** — `frame_io.rs` must keep using the `RESP2_NULL_ARRAY` const
  (`:21`), never a `b"*-1"` literal. `narrow_to_resp2_outbound` is **not edited by this proposal**
  (it is proposal 86's, §Boundaries), so the constraint is inherited unchanged.
- **`lint-script-gate`** — `core/src/scripting/bindings.rs` is inside the scanned tree; the edits
  there are a one-token deref at `:85` and one uninhabited arm added to `wire_response_to_lua`
  (see the corrected churn inventory) and introduce no `block_in_place`.
- **Golden encoding tests** — `encoding_golden.rs` lives in `frogdb-cluster` and contains **zero**
  references to `Response`/`WireResponse`/`InternalAction`. Not affected. No golden wire bytes are
  pinned against these types.

**Wire-visible behaviour change: none from the fold, and none from H2/H3/H4/H5.** The 16 variants,
their payloads, and both `to_resp*_frame` bodies are preserved byte-for-byte; H5 deletes a variant
no production code can produce, so no reply changes. **H1 is the only wire change in this document**
— an EXEC that today returns one error line would return an array, whose corresponding slot holds
either a per-slot error (option 2) or the resolved `+OK` (option 3). Which of those it is, is the
open ruling; that it changes the wire is why H1 is spec-first.

## Risks / scope boundaries

### vs proposal 86 — resp3-egress-codec (PN8 `encoded_len` + PN9 RESP3 outbound codec) — two shared files, partitioned by function

**86 is still unauthored at `49a21b18`** (no `86-*.md` on disk; it is being written concurrently in
another lane and was deliberately not read for this revision). Every claim in this subsection is
therefore **per the lane brief and defined from 80's side**, not verified against 86's text, and 86
should adopt or contest it. Both proposals touch `response.rs` and `frame_io.rs`. The partition is
by *function*, and it is clean because the two proposals sit on opposite sides of the `WireResponse`
type:

| Owned by **80** | Owned by **86** |
|---|---|
| `enum Response` (`:647-743`), `enum InternalAction` (`:40-73`), `WireResult` (`:748`) | `impl WireResponse`'s encoders — `to_resp2_frame` (`:274-334`), `to_resp3_frame` (`:341-432`), any new `encoded_len` |
| `impl Response` — `is_internal`, `into_wire`, `from_wire`, `try_to_resp*_frame` (`:754-931`) | `narrow_to_resp2_outbound` (`frame_io.rs:19-28`) |
| `impl From<Response> for BytesFrame` (`:933-940`) | `send_wire_response` / `feed_wire_response` (`frame_io.rs:63-150`) |
| `narrow_to_wire` (`frame_io.rs:31-53`) **only** | `estimate_resp2_frame_size` / `estimate_command_size` (`util.rs:130-163`) |

**Landing order is free**, because 80 does not rename `WireResponse`, does not change its 16-variant
set, and does not change either encoder's body — only its *definition site* moves from a standalone
enum to a type alias, which is source-compatible for everything 86 does.

**Recorded for 86, not claimed here:** `frame_io.rs:146` calls `self.resp3_buf.clear()` immediately
after the write, directly contradicting the comment at `:136` (*"Don't clear resp3_buf here —
accumulate across multiple feeds"*), and `:143-144` therefore reports a cumulative-looking
`encoded_len` that is in fact per-frame. That is 86's territory; flagged so it is not lost.

### vs proposal 81 — core-dead-seams (PN2 `NewConnection`, PN3 `ShardWaitQueue`) — disjoint, confirmed against the authored text

**81 is now authored** (`81-core-dead-seams.md`, committed `f73bdd8f`) and its file set was re-read
for this revision rather than guessed. Revision 1 guessed `core/src/shard/{connection.rs,
builder.rs, event_loop.rs, wait_queue.rs, worker.rs, mod.rs, blocking.rs}` + `server/src/acceptor.rs`
+ three `server/tests/common/` files. The authored §Files-involved is wider on the server side
(`server/src/server/{init.rs, util.rs, mod.rs, shards.rs, subsystems.rs}`, `shard-harness`, ten
further `frogdb-core` dummy-channel files, five `frogdb-shard-harness` test files, plus
`core/src/shard/{persistence.rs, diagnostics.rs}` and a website doc page) and **narrower** on one
point 81 flags explicitly: **81 does not touch `server/tests/common/`** — revision 1's guess was
wrong there and is corrected.

**None of that changes the disjointness verdict, and 81's author asserts the same conclusion
independently.** 81 §"vs proposal 80" states: *"80's edits are confined to `protocol/src/response.rs`
plus `core/src/scripting/bindings.rs` and (in its H1) `core/src/shard/execution.rs:626` — **zero
file overlap** with 81. Either order."* Re-checked from this side: 81's only `frogdb-protocol`
contact is read-only (`wait_queue.rs:4` imports `ProtocolVersion` and `Response`, stores them in
`WaitEntry` at `:21`/`:32`, and never constructs, matches, or converts either); 80's *fold* contact
inside `frogdb-core` is `core/src/scripting/bindings.rs` alone, which 81 does not list; and 80's
only `core/src/shard/` contact is `execution.rs:626`, which appears in 81's set **only** as
`execution.rs:1359` (a dummy-channel line, 733 lines away). H1 option 3 removes even that contact.
**Zero file overlap in either direction. Either order.**

### vs CT7 — RESP3-shape-once / RESP2 downgrader — enabling, not conflicting

CT7 (73 lines / 86 occurrences of `is_resp3`, future CT-lane proposal) wants command handlers to
emit the RESP3 shape once and have a downgrader own RESP2. 80 preserves all 16 wire variants and
does not touch `to_resp2_frame`, so CT7's downgrader can be built on `WireResponse` exactly as
planned. 80 makes CT7 **easier**: a single generic tree is one place to hang a downgrade walk
instead of two. The existing prototype of CT7's idea — protocol shape owned in one place, rendered
per protocol version — is `frogdb-protocol/src/reply.rs`'s `MapReply` (168 lines); CT7 should cite
it. **No edge requiring coordination.**

### vs proposal 78 — test-harness RESP client — additive, same file, one edge

78 adds `Response::from_resp2_frame` to `response.rs`. Under the fold this is a constructor over
the same 16 variants and is unaffected by the alias change; it does not touch `into_wire`,
`from_wire`, or the internal variants. Land in either order; if 78 lands first, its new function
needs no edit from 80.

**One edge, recorded from 80's side only — 78 is not edited here; the orchestrator owns the 78-side
sync.** 78's H3 leaves the destination type of the deduplicated `frame_to_response` an open ruling
and describes candidate **(ii)** (`WireResponse::from_resp2_frame` + a `WireResponse → Response`
lift) as *"requires a conversion that does not exist"*. That conversion **does** exist today:
`Response::from_wire(WireResponse) -> Response` at `response.rs:843` is exactly the lift, and it is
`pub`. Two consequences for the ruling:

- 78's stated cost for candidate (ii) is overstated **as of today** — the lift is already written.
- **80's H3 deletes `from_wire`** (zero non-test callers). So if the orchestrator rules 78 → (ii),
  either 78 lands before H3, or H3 lands and (ii) re-derives the lift — which is trivial under the
  fold, where the widening `Resp<Infallible> → Resp<Box<InternalAction>>` is a single generic map
  over the tree rather than 39 hand-written arms. Either sequencing is cheap; it just must be
  chosen rather than discovered at merge time. **80 does not depend on the ruling** and does not
  ask for one.

### vs proposals 50 / 67 / 68 — read-only or non-overlapping regions

- **68** cites `frame_io.rs:41` and `response.rs:422` **read-only** and owns `dispatch.rs:187-218`.
  80 owns `dispatch.rs:284-301` and `:779`. Disjoint.
- **67** edits `connection.rs` lines 4-6, 21, 71. 80 edits `connection.rs:548` (comment, H4) and the
  four `narrow_to_wire` call sites — none of which change under the fold (`Self::narrow_to_wire`
  keeps its signature). Disjoint.
- **50** owns `dispatch.rs:477-482`. Disjoint from 80's `:284-301` / `:779`. Note that `:477-484` is
  the transaction-poisoning block cited in §Problem 4 step 0 — 80 cites it **read-only** and does
  not edit it under any hotfix.

### vs proposals 63 / 64 (server composition) and 76 (observability extractors)

Verified zero overlap: 63/64 operate on `Server`/subsystem bundles, 76 on
`observability_server.rs` + `frogdb-telemetry`. Neither references `Response`, `WireResponse`, or
`InternalAction` in an edited region.

### vs the unclaimed PN6 — `BlockingOp` / `Direction` duplication

`BlockingOp` (`response.rs:500-550`) and `Direction` (`:475-493`) are duplicated against
`frogdb-types/src/types/mod.rs`. **80 does not touch either type** — they are `InternalAction`
payloads, and the fold moves the *declaration* of `InternalAction`'s variants nowhere. PN6 remains
independently landable in either order. Flagged so the two are not conflated: PN1 is about the
*response tree*, PN6 about *payload types the tree carries*.

### Other risks

- **`derive(PartialEq)` on a generic enum** requires `I: PartialEq`. `Infallible` and
  `Box<InternalAction>` both satisfy it, but the derive adds an `I: PartialEq` bound to the impl
  rather than making it unconditional. Any code that is generic over `Resp<I>` (none today) would
  need the bound written out. Low, but check at implementation time.
- **Diff size.** The fold is a large mechanical diff over `response.rs` plus 25 producer sites
  (11 of which also gain a `Box::new` if the boxed `RaftNeeded` declaration wins). Split the
  landing: (1) H2+H3+H5 dead-API/dead-variant deletions, (2) the type fold, (3) the
  `handle_internal_action` match move plus `bindings.rs`'s two one-line edits. Each is
  independently reviewable and each keeps the suite green.
- **`rust-analyzer` / error-message quality.** Type errors will name `Resp<Box<InternalAction>>`
  rather than `Response` in some diagnostics. Cosmetic; noted because it is the most common
  complaint about alias-based folds.

## Effort

**M** for the fold — unchanged from revision 1, on a corrected and slightly *smaller* inventory.
One 1770-line file restructured (of which 829 lines are tests that shrink), **25** mechanical
producer sites (not 27), **one** match move (not two — `internal_action_to_lua` already matches
`InternalAction`), two one-line `bindings.rs` edits (`response_to_lua`'s deref, the uninhabited arm
in `wire_response_to_lua`), one `execution.rs:626` `matches!` rewrite, one deletion of `from_wire`.
The one item pushing *up*: if the fold keeps `InternalAction`'s boxed `op: Box<RaftClusterOp>`, 11
`RaftNeeded` producers gain a `Box::new(…)`. Net: the two corrections roughly cancel, and the sizing
stays **M**. Bounded by the fact that **call sites do not change** — the compile-verified property
is what turns this from L to M. Land in the three steps listed above.

**S** each for the five hotfixes, all independently landable ahead of the fold — with the exception
that **H1 is S only for the code change**; its spec-first obligation (FM row + forcing test) is
what actually gates it.

## Independently-landable hotfixes

### H1 — LIVE: internal actions inside `MULTI/EXEC` destroy the transaction reply *(spec-first; fix site is an OPEN RULING, not decided here)*

**Evidence:** §Problem 4, full chain, including the step-0 reachability condition.

**Reproduction condition — must be pinned in the forcing test.** `MULTI; CLUSTER ADDSLOTS 0; EXEC`
(or `CLUSTER MEET`, same shape) on a Raft-mode node, on a connection where the NOADMIN gate is
inert: **`admin.enabled == false`** (the shipped default, `config/src/admin.rs:46`) **or** the
connection is the admin port (`is_admin == true`). On any other connection the reply is `-NOADMIN`
then `-EXECABORT` and the test proves nothing. Revision 1 omitted this and its test spec would have
been red for the wrong reason.

**Fix site: revision 1 pre-committed to `core/src/shard/execution.rs:626`. That pre-commitment is
withdrawn** — proposal policy is to state options and let the orchestrator rule, and the Redis
evidence below rules *against* the site revision 1 chose. Three options, with what is now known
about each:

**Option 1 — reject `CLUSTER`/`FROGDB.FINALIZE` at queue time inside `MULTI`.**
**Affirmatively incompatible, and against this tree's own policy.** Redis has **no `NO_MULTI` flag**
on these subcommands: upstream `redis/redis@unstable`
`src/commands/cluster-addslots.json` and `src/commands/cluster-setslot.json` both declare
`"command_flags": ["NO_ASYNC_LOADING", "ADMIN", "STALE"]` — fetched and read for this revision.
Redis queues `CLUSTER` inside `MULTI` and executes it at EXEC, returning `+OK` in the array.
Separately, this tree already carries an explicit standing instruction against exactly this move:
`pubsub_conn_command.rs:961-962` — *"Do not reintroduce queue-time `NO_MULTI`/`EXECABORT` rejection
for any of these commands — verified they don't carry that flag upstream."* Option 1 is recorded
for completeness and should not be the ruling.

**Option 2 — per-slot error at `core/src/shard/execution.rs:626`** (extend the `BlockingNeeded`
special-case to the remaining internal variants, converting each to a `Response::Error` so the EXEC
array survives). Smallest diff; strictly better than today (the other commands' results stop being
destroyed). **Still Redis-incompatible**, and it leaves the worse half of the defect in place: the
cluster operation still silently never happens. It converts a wrong reply plus a silent no-op into
a correct-shaped reply plus a *reported* no-op.

**Option 3 — make it work (leading candidate).** Redis executes `CLUSTER` at EXEC, so the compatible
answer is to resolve the internal action instead of erroring on it. The obstacle revision 1 assumed
— that resolving a `RaftNeeded` needs an async Raft round-trip, which the shard batch loop cannot
do — is real, but it does not apply at the seam that actually owns the reply. **The review located
an unlocked seam this proposal missed, and it is verified:**

- `ConnectionHandler::handle_exec` — `server/src/connection/transaction.rs:37-44` — owns the
  `Vec<Response>` that `frogdb_txn::handle_exec(self, summary).await` returns (`:43`). It is in
  `frogdb-server`, **not** in LOCKED `frogdb-txn`.
- `ConnectionHandler::handle_internal_action` — `dispatch.rs:284-301` — is an `async fn` on the
  same `&mut self`, and every branch it dispatches (`handle_blocking_wait`, `handle_raft_command`,
  `handle_slot_migration`) already performs its async round-trip in the connection layer.
- So a post-pass in `handle_exec` — map each element of the returned `Vec<Response>`, and each
  element of any `Response::Array` inside it, through `handle_internal_action` — resolves the
  action **exactly where the async machinery already lives**. It touches **no LOCKED crate**, needs
  **no `execution.rs` change**, and needs no new plumbing.

Costs option 3 must answer at implementation time, stated so the ruling is informed rather than
sold: (a) the shard batch has already committed by the time the post-pass runs, so a Raft failure
surfaces as a per-slot error in an otherwise-committed transaction — that is Redis's behaviour too
(`CLUSTER` is not transactional in Redis either), but it must be written into the FM row's
*NOT observable* line rather than discovered later; (b) the recursion must be depth-1 into
`Response::Array` only, matching the exact shape `exec.rs:347` produces, not a general deep walk;
(c) `BlockingNeeded` must keep its existing `execution.rs:626` → `Response::Null` conversion (queued
blocking commands run non-blocking at EXEC, matching Redis) — the post-pass must not resurrect it.

**Ruling owed.** 80 does not decide between options 2 and 3; it records that **option 1 is refuted**
and that **option 3 is the Redis-compatible one with a verified, unlocked seam**. The heading no
longer pre-commits a fix site.

**Protocol.** Behaviour change in EXEC reply semantics ⇒ LOCKED **txn** *area* ⇒ spec-first:
new `FM-TXN-NNN` row in `.scratch/hardening/specs/txn-failure-modes.md` (adjacent to `FM-TXN-045`
at `:566-576`) → failing forcing test with the step-0 condition pinned → fix. **Do not site the fix
in `frogdb-txn/src/exec.rs` (LOCKED).** Neither candidate site is in a gated crate, so **no mutation
gate scores this fix** — see §Spec / LOCKED impact for why the revision-1 mutation protocol was
unsatisfiable and what enforcement actually applies.

### H2 — Delete `impl From<Response> for BytesFrame` and its `.expect` panic *(claimed)*

`response.rs:933-940`. Zero non-test callers; only its own tests at `:955` and `:962` (delete those
too). Removes a public panic surface on the crate's most-used type. Pure deletion.

### H3 — Delete four zero-caller public methods and one type alias *(claimed)*

`Response::try_to_resp2_frame` (`:920`), `Response::try_to_resp3_frame` (`:928`),
`Response::is_internal` (`:754`), `Response::from_wire` (`:843`); and `pub type WireResult` (`:748`)
once `into_wire`'s signature is written out, with the **`lib.rs:22`** re-export. Pure deletion;
shrinks the surface the fold has to preserve. Land **before** the fold — it is ~120 fewer lines to
restructure. **One coordination note:** `from_wire` is the `WireResponse → Response` lift that
proposal 78's H3 candidate (ii) believes does not exist — see §vs proposal 78 before deleting it.

### H4 — Correct three doc comments that assert a false invariant *(claimed)*

**`connection.rs:548`** (*"Internal actions were already resolved by the dispatch layer"* — false
for `DispatchStage::TransactionControl`; revision 1 cited `:547`, which is the preceding "Feed
responses into the write buffer without flushing" line — corrected),
`frame_io.rs:33-40` (*"this narrowing is total in practice"*), `frame_io.rs:61-62`. Replace with
what is actually true: the narrowing is total for the `Execute` stage only; `TransactionControl`
bypasses `handle_internal_action` entirely; the encoder — not the pipeline — is what cannot
represent an internal action. Comment-only; should land **with or after H1** so it describes the
fixed behaviour.

### H5 — Delete `MigrateNeeded`, a variant with zero production producers *(claimed — new in revision 2)*

**Evidence:** §Problem 5. `git grep -n MigrateNeeded` over the whole tree returns 12 hits and
**not one of them constructs the variant outside `response.rs`'s own tests**. `MIGRATE` is
`ExecutionStrategy::ServerWide(ServerWideOp::Migrate)` (`migrate_cmd.rs:36`), so it is deferred at
the connection layer and never reaches a shard executor; the shard executor it would reach
deliberately returns `Err(CommandError::Internal { … })` instead (`migrate_cmd.rs:41-54`, the
comment at `:50` says so explicitly: *"fail loudly rather than leak an internal `MigrateNeeded`
signal"*).

**Pure deletion, five sites:**

| Site | Action |
|---|---|
| `response.rs:731` (`Response::MigrateNeeded`) | delete variant |
| `response.rs:62` (`InternalAction::MigrateNeeded`) | delete variant |
| `response.rs:759` (`is_internal` `matches!` arm), `:832` (`into_wire` arm) | delete arms (both die entirely under H3/the fold) |
| `dispatch.rs:297` (`handle_internal_action` arm) | delete arm — and with it the last caller of `handle_migrate_command`, which then also deletes |
| `bindings.rs:196-201` (`internal_action_to_lua` arm) | delete arm |
| `response.rs:1331`, `:1378`, `:1421` | delete the three tests that exist only to exercise the dead variant |

`handle_migrate_command` is `connection/persistence_handler.rs:31` and `dispatch.rs:297` is its
**only** caller (checked: `git grep -n handle_migrate_command` returns exactly those two lines), so
the handler deletes with the arm. It is the one part of H5 that is not a mechanical arm removal, and
whoever lands it should re-run that grep rather than trust this line. Note the near-miss: the live
`MIGRATE` path is `ServerWideOp::Migrate => self.handle_migrate(args)` at **`dispatch.rs:244`** — a
*different* method (`handle_migrate`, not `handle_migrate_command`) reached through
`dispatch_server_wide`. H5 must not touch it.

**Interaction with the rest of this proposal:** H5 makes §Problem 4's "`MigrateNeeded` falls
through" case **vacuous** (already corrected in the text above), reduces the fold's internal-variant
count from 4 to 3, and removes one arm from the single match that has to move. Land it with or
before H2/H3.

### Recorded, not claimed

- `frame_io.rs:146` `resp3_buf.clear()` contradicts the `:136` comment and makes `:143` mis-report
  encoded size → **proposal 86** (per lane brief; 86 not yet authored).
- `InternalAction::RaftNeeded { op: Box<RaftClusterOp> }` vs `Response::RaftNeeded { op: RaftClusterOp }`
  (§Problem 7) → resolved by the fold; no separate hotfix. Which of the two declarations survives is
  an implementation-time choice with an 11-site `Box::new` cost attached — see the churn inventory.
- Lane-brief item "62 Item B — `codec.rs:254` oversized-bulk rescan" is **not in the authored
  proposal 62** and is claimed by no document on disk → needs an owner.
- **Security classification of §Problem 4** — filed as a note, **parked, not implemented** (standing
  policy). The review's proposed narrowing to "admin surface only" is refuted for the default
  configuration; see §Problem 4's blast-radius paragraph. No security fix is proposed.

## Review ledger

Adversarial review verdict: **AMEND**. Every finding was re-derived against the tree at `49a21b18`
before disposition. Two were refuted with evidence; three revision-1 claims are withdrawn.

### Applied

| Item | Disposition | Verification |
|---|---|---|
| **B1** (admin gate blocks the headline repro) | **APPLIED** — new §Problem 4 step 0, forcing-test condition pinned in §Testability item 3 and in H1's reproduction paragraph. | Confirmed. `SPLIT_ADMIN_SURFACES` `command_spec.rs:584-599` lists exactly the 9 public CLUSTER subcommands the review named; fail-closed is `AdminSurface::requires_admin` `:545-548` (review cited `:527-530` — that is the doc comment describing the behaviour, the code is at `:545-548`; both are right about the property). NOADMIN gate `guards.rs:347-360`. `PRE_DISPATCH_ORDER` `dispatch.rs:123-141`: `PreChecks` index 1, `TransactionQueue` index 5. Poison `dispatch.rs:477-484` (`abort_transaction`). |
| **B2** (`admin.rs:82` is `cluster_meet`, not ADDSLOTS) | **APPLIED** — cite corrected; repro kept as `CLUSTER ADDSLOTS` with `CLUSTER MEET` named as an equally valid alternative. | Confirmed. `:82` = `RaftClusterOp::AddNode` inside `cluster_meet` (`:16`). `cluster_addslots` = `:128-186`, `RaftNeeded` at `:177`, `AssignSlots` at `:178`. ADDSLOTS kept because it is the subcommand whose silent no-op is most visibly wrong (a slot that never gets assigned). |
| **B3** (option 1 Redis-incompatible; option 3 seam at `transaction.rs:37-43`) | **APPLIED in full** — H1 heading no longer names a fix site; option 1 marked refuted with the upstream JSON; option 3 presented as leading candidate, re-sited at `transaction.rs:37-44`; ruling explicitly left owed. | Confirmed on all three legs. Upstream `cluster-addslots.json` and `cluster-setslot.json` both `["NO_ASYNC_LOADING","ADMIN","STALE"]` — **no `NO_MULTI`** (fetched from `raw.githubusercontent.com/redis/redis/unstable`). Tree policy `pubsub_conn_command.rs:961-962`. Seam verified by reading it: `handle_exec` `:37-44` in `frogdb-server` returns the `Vec<Response>` from `frogdb_txn::handle_exec` at `:43`; `handle_internal_action` `dispatch.rs:284-301` is `async fn(&mut self)`; sole existing call `:779`. |
| **B4** (mutation-gate protocol unsatisfiable) | **APPLIED** — revision 1's protocol withdrawn; §Spec / LOCKED impact now states plainly that no mutation gate covers this row and the FM row + integration test is the whole enforcement. | Confirmed. `Justfile:272-279` has only the generic `mutants-diff`/`mutants-gate` recipes; the four gates (txn 0.90 / persistence 0.85 / replication 0.85 / cluster 0.80) name no `frogdb-core` or `frogdb-server`. Precedent confirmed: FM-TXN-045's forcing test is `frogdb-server`'s `integration_transactions.rs` (tag `:1110`, `fn` `:1122`), and `frogdb-server` **and** `frogdb-core` are both in `NEXTEST_CRATES` (`scripts/failure-modes.py:64-77`). |
| **N1** (`wire_response_to_lua` needs the uninhabited arm) | **APPLIED** — added to churn inventory and to the `lint-script-gate` note. | Confirmed with one precision correction: it is exhaustive over all **16** variants in **15** arms (`Bulk(None)`/`Null`/`NullArray` share one), `bindings.rs:93-176`, no `_` catch-all. |
| **N2** (`internal_action_to_lua` already matches `InternalAction`) | **APPLIED** — churn inventory corrected; "two 4-arm matches move" withdrawn. | Confirmed. `bindings.rs:184,190,196,202` are already `InternalAction::*`. `response_to_lua`'s `Err(action)` arm is `:85`. Only `handle_internal_action` moves. |
| **N3** (boxed `RaftNeeded` ⇒ `Box::new` at producers) | **APPLIED**, with a count correction — **11** `RaftNeeded` producers, not 10. | Confirmed. `admin.rs` `:82,:118,:177,:225,:301,:338,:375,:422,:468,:643` = 10, plus `version.rs:123` (`FROGDB.FINALIZE`) = 11. The review's "10" missed `version.rs`. Effort statement updated. |
| **N4** (new hotfix H5, `MigrateNeeded` dead) | **APPLIED** — H5 written; §Problem 5 extended; §Problem 4's fall-through text corrected to note the vacuity. | Confirmed by repo-wide grep: 12 hits, zero production producers. Two cite corrections to the review: `migrate_cmd.rs` `execute` is `:41-54` (the deliberate `Err` at `:51-53`), not `:44-52`; and `handle_migrate_command` (`persistence_handler.rs:31`) also deletes, while the *live* `MIGRATE` path is the differently-named `handle_migrate` at `dispatch.rs:244`. |
| **N5** (off-by-ones) | **APPLIED** — `connection.rs:547 → :548` (3 places: Files table, §Problem 6, H4); `lib.rs:23 → :22` (3 places). | Confirmed by grep. `:547` is the *preceding* "Feed responses into the write buffer" comment line; `:549` is the `narrow_to_wire` continuation. |
| **N6** (81 authored; re-verify disjointness) | **APPLIED** — §vs 81 rewritten against the authored text, with 81's own assertion quoted; 86 subsection relabelled "per lane brief, 86 not yet authored". | Confirmed. 81 committed `f73bdd8f`. Revision 1's guess at 81's file set was wrong in both directions (81 is wider on the server side, and does **not** touch `server/tests/common/` — 81 says so explicitly). Verdict unchanged: zero file overlap. 86: no `86-*.md` on disk at `49a21b18`; deliberately not read. |
| **N7** (78's candidate (ii) — the lift does exist) | **APPLIED** — noted in §vs proposal 78 and cross-linked from H3. **Proposal 78 was not edited.** | Confirmed. `Response::from_wire` is `response.rs:843`, `pub`, zero non-test callers; 78's H3 candidate (ii) text is `78-test-harness-resp-client.md:847-849`. |
| **N8** (site count) | **APPLIED**, with one refutation inside it — see below. | 53 total lines confirmed; **30 real code sites = 25 producers + 4 dispatch arms + 1 `matches!`** confirmed exactly. |
| **N9** (§Problem 3 corroboration) | **APPLIED** — new subsection with all four properties, each independently re-derived. | All four reproduced by compiling standalone models in `$TMPDIR` (`rustc --edition 2024 -O`, outside the repo tree): sizes **40/40/40** with a niched `Bytes` (`Option<Bytes> = 32`); **48** with a no-niche `Bytes` (`Option<Bytes> = 40`) — footnoted as a layout consequence, not a contract; same-named inherent methods on `Resp<Infallible>` / `Resp<Box<InternalAction>>` compile; `use Response::Integer;` fails **E0432** *"`Response` is a type alias, not a module"*. Repo has **zero** `use …Response::…` variant imports, and `frogdb-protocol` has **no serde dependency at all** and no `Serialize`/`Deserialize` in `response.rs`. |
| **N10 / N11** (seam lints, 80/86 partition) | **NO CHANGE NEEDED**, as the review found — both re-checked and left standing. | `scripts/error-sanitize.py` still hard-codes `frogdb-server/crates/protocol/src/response.rs`; both encoders (`to_resp2_frame:274`, `to_resp3_frame:341`) and `sanitize_error_message` (`:209`) stay in that file. |

### Refuted

| Item | Refutation | Evidence |
|---|---|---|
| **B1's blast-radius narrowing** — "admin surface: frogctl/operator path", classified as a parked security note | **REFUTED for the default configuration.** `AdminConfig::default()` sets `enabled: false` (`config/src/admin.rs:43-50`) and the field is `#[serde(default)]` on a `bool` (`:17-19`), so a deployment that does not opt in to the admin API leaves `admin_enabled == false` — the NOADMIN gate at `guards.rs:351` never fires, and `CLUSTER ADDSLOTS` inside `MULTI` is reachable from an ordinary client port. The defect is an ordinary correctness defect on the shipped default, not an admin-only one, and no privilege boundary is crossed. | The *condition* the review derived (`is_admin == true` **or** `admin_enabled == false`) is correct and is applied in full — only the narrowing drawn from it is refuted. The review's framing is recorded as correct for the `admin.enabled = true` configuration specifically. Classification filed and parked either way; no security fix proposed. |
| **N8's "8 of the 53 were comment lines"** | **REFUTED — 19 are comment lines**, not 8. Decomposition: 19 comments (`admin.rs` 14, `execution.rs` 1, `cluster/mod.rs` 1, `migrate_cmd.rs` 1, `connection/cluster.rs` 2) + 4 `InternalAction::` arms + 30 code sites = 53. The review's own headline (30 real code sites) is correct; its subtraction (53 − 8 − 4 = 41 ≠ 30) was not. | `git grep -nE '(BlockingNeeded\|RaftNeeded\|MigrateNeeded\|SlotMigrationNeeded)' -- '*.rs' ':!…/response.rs'`, then filtering `^\s*(//\|///\|//!)`. The corrected decomposition is what §Corrections now states. |

### Withdrawn first-draft claims

| Revision-1 claim | Status |
|---|---|
| "27 producer sites" (§Problem 3, §Proposed change, §Effort, §Risks) | **WITHDRAWN → 25.** Origin: conflated producers with the 4 `handle_internal_action` arms plus assorted comment lines. |
| "`cluster/admin.rs` — 28 producer lines (10 `RaftNeeded`, 4 `SlotMigrationNeeded`)" (Files table) | **WITHDRAWN → 14 producers + 14 comment lines.** The 28 was the raw grep line count; the parenthetical (10 + 4) was already the correct producer count and contradicted the 28 in the same cell. |
| "two 4-arm matches move from `Response::X` to `InternalAction::X`" (§Proposed change) | **WITHDRAWN → one.** `internal_action_to_lua` already matches `InternalAction`. |
| "the fix site is `core/src/shard/execution.rs:626`" (H1 heading + body, stated as settled) | **WITHDRAWN.** Now one of three options, and not the leading one; the heading no longer names a site. Proposal policy: state options, do not decide. |
| "the forcing test must live in a crate whose own `cargo mutants -p` run covers the mutated code" (§Spec / LOCKED impact, H1) | **WITHDRAWN as unsatisfiable.** No such crate exists for this row. |
| "`RaftNeeded`, `MigrateNeeded` and `SlotMigrationNeeded` fall through into `results` untouched" (§Problem 4 step 4) | **PARTIALLY WITHDRAWN.** True of `RaftNeeded` and `SlotMigrationNeeded`; vacuous for `MigrateNeeded`, which has no production producer (H5). |
| "`connection.rs:547`" (3 sites), "`lib.rs:23`" (3 sites) | **WITHDRAWN → `:548`, `:22`.** |
| "81's file set is … plus three `server/tests/common/` files" (§vs 81) | **WITHDRAWN.** 81 is now authored and states it does not touch `server/tests/common/`. Disjointness verdict unchanged. |

### Hotfix status after revision 2

| # | Subject | Review ruling | Status |
|---|---|---|---|
| **H1** | Internal actions inside `MULTI/EXEC` destroy the transaction reply | **AMEND** (B1–B4) | Amended. Repro condition pinned; ADDSLOTS cite fixed; fix site de-committed; option 1 refuted with upstream evidence; option 3 presented as leading candidate at the verified unlocked seam `transaction.rs:37-44`; mutation protocol corrected. **Ruling still owed to the orchestrator — 80 does not decide.** |
| **H2** | Delete `impl From<Response> for BytesFrame` + its `.expect` panic | **CONFIRMED** | Unchanged. `response.rs:933-940`, tests `:955`/`:962`. |
| **H3** | Delete four zero-caller methods + `WireResult` | **CONFIRMED** (with N7 caveat) | Unchanged except the `lib.rs:22` correction and a new coordination note pointing at proposal 78's candidate (ii), which needs `from_wire`. |
| **H4** | Correct three false doc comments | **CONFIRMED** (with `:548` correction) | Corrected. |
| **H5** | Delete `MigrateNeeded` (zero production producers) | **NEW** (N4) | Added. Zero-producer claim independently verified by repo-wide grep; two review cites corrected in the process. |
