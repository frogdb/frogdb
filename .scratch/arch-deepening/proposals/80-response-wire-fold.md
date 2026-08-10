# Proposal 80 — `Response` / `WireResponse` / `InternalAction`: sixteen variants, declared twice, plus a narrowing that is not total

Round 38 · lane: protocol / net / core · candidate **PN1** · effort **M** (fold) + **S** each
(four hotfixes, independently landable) · **no locked crate edited**, **no FM tag exists in any
file this proposal edits**, **one seam lint constrains the change** (`lint-error-sanitize`)

**Verified at HEAD `54baa2bb6a3d0586808fb2686c49026089793171`** (worktree
`arch-round-38-99`, branch `main`). The brief named `2e81506b` as HEAD; four commits landed while
this was being authored, and **all four touch only `.scratch/**.md`** —
`git diff --name-only 2e81506b..HEAD | grep -v '^\.scratch/'` is empty — so every line number
below was derived against an unchanged source tree and re-checked at `54baa2bb`.

## Corrections to the lane brief

Every brief claim was re-derived. Six are wrong, and one of the six changes the shape of this
proposal.

| Brief claim | Verified at HEAD |
|---|---|
| "6386 `Response::` call sites" | **6458** occurrences of `Response::` (`git grep -o 'Response::' -- '*.rs' \| wc -l`), of which **6315** are bare `Response::` and **143** are `WireResponse::`. By *lines*: 6327 / 6192 / 135. Cite the metric — the four numbers differ and the brief did not say which it meant. |
| "`into_wire`/`from_wire` ~250 lines of pure recursion" | **107 lines.** `into_wire` is `response.rs:770-837` (68), `from_wire` is `response.rs:843-881` (39). |
| "~34 non-protocol internal-variant uses" | **53 lines across 10 files** (`blocking.rs` 8, `stream/read.rs` 2, `bindings.rs` 4, `execution.rs` 2, `cluster/admin.rs` 28, `cluster/mod.rs` 1, `migrate_cmd.rs` 1, `version.rs` 1, `connection/cluster.rs` 2, `dispatch.rs` 4). Inside the protocol crate: 42 more, all in `response.rs`. |
| **PN1 rated "Latent"** | **Wrong — there is a LIVE, reachable, wholly untested defect** in exactly the seam PN1 names. `MULTI; CLUSTER ADDSLOTS 0; EXEC` on a Raft node returns `-ERR internal action reached response encoder`, discards *every other command's* result in the transaction, and silently no-ops the cluster operation. Full proof chain in §Problem 4. |
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

Four hotfixes are separable and land alone: **H1** the LIVE EXEC defect (spec-first, LOCKED **txn**
area — the fix site is in unlocked `frogdb-core`, but the behaviour it changes is EXEC reply
semantics, so it needs an FM row and a forcing test first); **H2** delete a dead `.expect` panic;
**H3** delete three zero-caller public methods; **H4** correct a doc comment that asserts the
invariant §Problem 4 disproves.

## Files involved

Line counts and commit counts at `54baa2bb`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/protocol/src/response.rs` | 1770 (941 code + 829 tests, 45 `#[test]`) | **Primary.** Owns all three types, both recursions, both encoders, `sanitize_error_message`. |
| `frogdb-server/crates/protocol/src/lib.rs` | 27 | Re-export list `:20-23` (`InternalAction, Response, WireResponse, WireResult, sanitize_error_message`) — `WireResult` re-export dies with H3. |
| `frogdb-server/crates/server/src/connection/frame_io.rs` | 283 | The encoder boundary. This proposal owns **`narrow_to_wire` `:31-53` only** (see §Boundary vs 86). |
| `frogdb-server/crates/server/src/connection.rs` | 922 | 4 `narrow_to_wire` sites (`:409`, `:552`, `:719`, `:731`); the false comment at `:547` (H4). |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 1177 | `handle_internal_action` `:284-301`; its **sole** call site `:779`. |
| `frogdb-server/crates/core/src/scripting/bindings.rs` | 420 | The only non-`frogdb-server` consumer of the split: `response_to_lua` `:82-87`, `wire_response_to_lua` `:93-176`, `internal_action_to_lua` `:182-209`. |
| `frogdb-server/crates/server/src/commands/cluster/admin.rs` | 662 | 28 producer lines (10 `RaftNeeded`, 4 `SlotMigrationNeeded`). Mechanical churn only. |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | 8 `BlockingNeeded` producers. Mechanical churn only. |
| `frogdb-server/crates/commands/src/stream/read.rs` | — | 2 `BlockingNeeded` producers (`:112`, `:352`). Mechanical churn only. |
| `frogdb-server/crates/server/src/commands/version.rs` | — | 1 `RaftNeeded` producer (`:123`). Mechanical churn only. |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | **Hotfix H1 only** (`:626`). Not edited by the fold. |

Read-only evidence, not edited: `frogdb-server/crates/txn/src/exec.rs:347` (**LOCKED crate — must
not be touched**), `frogdb-server/crates/server/src/connection/transaction.rs:133-145`,
`frogdb-server/crates/server/src/commands/cluster/mod.rs:88-108`,
`frogdb-server/crates/server/src/connection/util.rs:130-163`,
`frogdb-server/crates/protocol/src/reply.rs` (168 lines, `MapReply`),
`scripts/error-sanitize.py`, `Justfile:1249`.

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
are produced at **27 sites** and that cannot appear on the wire at all. The `Vec<Response>` that
EXEC builds (`exec.rs:347`) pays it per element.

The oversize is already visible in the code as an apology: `#[allow(clippy::result_large_err)]` at
`response.rs:769`, suppressing clippy's complaint about `WireResult`'s 120-byte error arm.

### 4. The narrowing is not total, and the gap is LIVE

`frame_io.rs:33-40` states the design's central claim:

> Internal control-flow actions are resolved upstream (see `handle_internal_action`) before a
> response is handed to the encoder, so this narrowing is total in practice. […] an internal
> action is structurally unrepresentable past this point.

The second half is true (the encoders take `WireResponse`). The first half is false. Proof chain,
every link verified at HEAD:

1. `CLUSTER` declares `strategy: ExecutionStrategy::Standard` and `keys: KeySpec::None`
   (`cluster/mod.rs:88-108`, `:105` for the strategy).
2. `deferral_of` (`transaction.rs:133-145`) defers only `ConnectionLevel(_)` and `ServerWide(op)`;
   `_ => None`. **`CLUSTER` inside `MULTI` is therefore queued and shipped to the shard batch**,
   not intercepted at the connection layer.
3. `CLUSTER ADDSLOTS` on a node with Raft returns `Response::RaftNeeded { … }`
   (`cluster/admin.rs:82`, gated on `ctx.raft.is_some()`).
4. The shard batch loop (`core/src/shard/execution.rs:618-632`) filters exactly **one** internal
   variant:
   ```rust
   let response = if matches!(&response, Response::BlockingNeeded { .. }) {
       Response::Null
   } else { response };
   results.push(response);
   ```
   `RaftNeeded`, `MigrateNeeded` and `SlotMigrationNeeded` fall through into `results` untouched.
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

Same reachability for every `Standard`-strategy internal-action producer: `CLUSTER
MEET/FORGET/ADDSLOTS/DELSLOTS/FAILOVER/REPLICATE/RESET/SET-CONFIG-EPOCH/SETSLOT`
(`cluster/admin.rs` `:82,:118,:177,:225,:301,:338,:375,:422,:468,:561,:602,:633,:643,:654`) and
`FROGDB.FINALIZE` (`version.rs:123`). **`MIGRATE` is not affected** — it is `ServerWide`, hence
deferred at step 2.

**Nothing tests this.** `git grep 'internal action reached'` returns exactly two hits, both the
producing code (`frame_io.rs:47`, `:50`). No test anywhere asserts the degraded string, and no test
in the tree runs a `CLUSTER` subcommand inside `MULTI`.

Note what the two-enum design bought here: it converted a panic into a silent, wrong reply. That is
better than a panic and worse than a compile error — and it is precisely the outcome you get when a
non-recursive invariant is enforced on a recursive type.

### 5. A dead panic on the public API, and three dead methods

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
appears only as `into_wire`'s return type plus the `lib.rs:23` re-export.

Six public API items on the crate's most-used type, none of them reachable from production code.

### 6. The doc comments assert the invariant that §4 disproves

Three places state the false property. `frame_io.rs:33-40` (quoted above). `connection.rs:547`:
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

Churn: the 27 producer sites gain a wrapper (`Response::Internal(Box::new(InternalAction::RaftNeeded{…}))`,
or a preserved `Response::raft_needed(…)` constructor to keep them one-line), and two 4-arm matches
move from `Response::X` to `InternalAction::X` — `handle_internal_action` (`dispatch.rs:284-301`)
and `internal_action_to_lua` (`bindings.rs:182-209`). `response_to_lua` (`bindings.rs:82-87`) keeps
its `into_wire()` shape unchanged.

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
3. **H1's forcing test is writable today** (`MULTI; CLUSTER ADDSLOTS 0; EXEC` against a Raft-mode
   node, asserting the array reply and the slot assignment) and should be written **before** the
   fold, so the fold's regression surface is pinned by a red-then-green test rather than by the
   refactor's own assertions.

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

**H1 is the exception and is spec-first.** Its fix site (`core/src/shard/execution.rs:626`) is in
unlocked `frogdb-core`, but the behaviour it changes is **EXEC reply semantics**, squarely the
LOCKED **txn** area (gate 0.90), whose spec already carries the adjacent row **FM-TXN-045**
(`.scratch/hardening/specs/txn-failure-modes.md:566-576`, forced by
`integration_transactions.rs:1122`). H1 therefore follows the locked-area protocol: **new
`FM-TXN-NNN` row → failing forcing test → fix**, and the forcing test must live in a crate whose own
`cargo mutants -p` run covers the mutated code.

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
- **`lint-script-gate`** — `core/src/scripting/bindings.rs` is inside the scanned tree; the edit
  there is a 4-arm match rewrite and introduces no `block_in_place`.
- **Golden encoding tests** — `encoding_golden.rs` lives in `frogdb-cluster` and contains **zero**
  references to `Response`/`WireResponse`/`InternalAction`. Not affected. No golden wire bytes are
  pinned against these types.

**Wire-visible behaviour change: none from the fold.** The 16 variants, their payloads, and both
`to_resp*_frame` bodies are preserved byte-for-byte. **H1 does change wire behaviour** (an EXEC that
today returns one error line would return an array with a per-slot error) — that is the point of the
spec-first requirement, and it is the only wire change in this document.

## Risks / scope boundaries

### vs proposal 86 — resp3-egress-codec (PN8 `encoded_len` + PN9 RESP3 outbound codec) — two shared files, partitioned by function

86 is **not yet authored**, so the boundary is defined from this side and 86 should adopt it.
Both proposals touch `response.rs` and `frame_io.rs`. The partition is by *function*, and it is
clean because the two proposals sit on opposite sides of the `WireResponse` type:

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

### vs proposal 81 — core-dead-seams (PN2 `NewConnection`, PN3 `ShardWaitQueue`) — disjoint

81's file set is `core/src/shard/{connection.rs, builder.rs, event_loop.rs, wait_queue.rs,
worker.rs, mod.rs, blocking.rs}` plus `server/src/acceptor.rs` and three `server/tests/common/`
files. 80's only `core/src/shard/` contact is **`execution.rs:626`, and only in hotfix H1**. Zero
file overlap. 80's `frogdb-core` edit for the fold is `core/src/scripting/bindings.rs`, which 81
does not touch.

### vs CT7 — RESP3-shape-once / RESP2 downgrader — enabling, not conflicting

CT7 (73 lines / 86 occurrences of `is_resp3`, future CT-lane proposal) wants command handlers to
emit the RESP3 shape once and have a downgrader own RESP2. 80 preserves all 16 wire variants and
does not touch `to_resp2_frame`, so CT7's downgrader can be built on `WireResponse` exactly as
planned. 80 makes CT7 **easier**: a single generic tree is one place to hang a downgrade walk
instead of two. The existing prototype of CT7's idea — protocol shape owned in one place, rendered
per protocol version — is `frogdb-protocol/src/reply.rs`'s `MapReply` (168 lines); CT7 should cite
it. **No edge requiring coordination.**

### vs proposal 78 — test-harness RESP client — additive, same file

78 adds `Response::from_resp2_frame` to `response.rs`. Under the fold this is a constructor over
the same 16 variants and is unaffected by the alias change; it does not touch `into_wire`,
`from_wire`, or the internal variants. Land in either order; if 78 lands first, its new function
needs no edit from 80.

### vs proposals 50 / 67 / 68 — read-only or non-overlapping regions

- **68** cites `frame_io.rs:41` and `response.rs:422` **read-only** and owns `dispatch.rs:187-218`.
  80 owns `dispatch.rs:284-301` and `:779`. Disjoint.
- **67** edits `connection.rs` lines 4-6, 21, 71. 80 edits `connection.rs:547` (comment, H4) and the
  four `narrow_to_wire` call sites — none of which change under the fold (`Self::narrow_to_wire`
  keeps its signature). Disjoint.
- **50** owns `dispatch.rs:477-482`. Disjoint from 80's `:284-301` / `:779`.

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
- **Diff size.** The fold is a large mechanical diff over `response.rs` plus 27 producer sites. Split
  the landing: (1) H2+H3 dead-API deletions, (2) the type fold, (3) `bindings.rs` and
  `handle_internal_action` match rewrites. Each is independently reviewable and each keeps the
  suite green.
- **`rust-analyzer` / error-message quality.** Type errors will name `Resp<Box<InternalAction>>`
  rather than `Response` in some diagnostics. Cosmetic; noted because it is the most common
  complaint about alias-based folds.

## Effort

**M** for the fold. One 1770-line file restructured (of which 829 lines are tests that shrink), 27
mechanical producer sites, 2 match rewrites, 1 deletion of `from_wire`. Bounded by the fact that
**call sites do not change** — the compile-verified property is what turns this from L to M. Land in
the three steps listed above.

**S** each for the four hotfixes, all independently landable ahead of the fold.

## Independently-landable hotfixes

### H1 — LIVE: internal actions inside `MULTI/EXEC` destroy the transaction reply *(spec-first, do not fold into the refactor)*

**Evidence:** §Problem 4, full chain. **Fix site:** `core/src/shard/execution.rs:626` — extend the
`BlockingNeeded` special-case to the other three internal variants, converting each to a per-slot
`Response::Error` so the EXEC array survives and the client learns which command failed.

**Protocol:** the fix site is in unlocked `frogdb-core`, but the behaviour is EXEC reply semantics
in the LOCKED **txn** area. Required order: new `FM-TXN-NNN` row in
`.scratch/hardening/specs/txn-failure-modes.md` (adjacent to `FM-TXN-045` at `:566-576`) → failing
forcing test → fix. **Do not site the fix in `frogdb-txn/src/exec.rs` (LOCKED).** Run
`just mutants-diff frogdb-txn` before pushing if any `frogdb-txn` file is touched.

**Open design question for the ruling** (state it, do not decide it here): should
`CLUSTER`/`FROGDB.FINALIZE` be *rejected at queue time* inside `MULTI` (like Redis rejects some
commands) rather than erroring per-slot at EXEC time? Rejecting at queue time is the stronger
contract and would make the FM row a queue-time row; per-slot erroring is the smaller change. Redis
allows `CLUSTER` in `MULTI` and executes it, which argues for making it actually work — a third
option, larger than both.

### H2 — Delete `impl From<Response> for BytesFrame` and its `.expect` panic *(claimed)*

`response.rs:933-940`. Zero non-test callers; only its own tests at `:955` and `:962` (delete those
too). Removes a public panic surface on the crate's most-used type. Pure deletion.

### H3 — Delete four zero-caller public methods and one type alias *(claimed)*

`Response::try_to_resp2_frame` (`:920`), `Response::try_to_resp3_frame` (`:928`),
`Response::is_internal` (`:754`), `Response::from_wire` (`:843`); and `pub type WireResult` (`:748`)
once `into_wire`'s signature is written out, with the `lib.rs:23` re-export. Pure deletion; shrinks
the surface the fold has to preserve. Land **before** the fold — it is ~120 fewer lines to restructure.

### H4 — Correct three doc comments that assert a false invariant *(claimed)*

`connection.rs:547` (*"Internal actions were already resolved by the dispatch layer"* — false for
`DispatchStage::TransactionControl`), `frame_io.rs:33-40` (*"this narrowing is total in practice"*),
`frame_io.rs:61-62`. Replace with what is actually true: the narrowing is total for the `Execute`
stage only; `TransactionControl` bypasses `handle_internal_action` entirely; the encoder — not the
pipeline — is what cannot represent an internal action. Comment-only; should land **with or after
H1** so it describes the fixed behaviour.

### Recorded, not claimed

- `frame_io.rs:146` `resp3_buf.clear()` contradicts the `:136` comment and makes `:143` mis-report
  encoded size → **proposal 86**.
- `InternalAction::RaftNeeded { op: Box<RaftClusterOp> }` vs `Response::RaftNeeded { op: RaftClusterOp }`
  (§Problem 7) → resolved by the fold; no separate hotfix.
- Lane-brief item "62 Item B — `codec.rs:254` oversized-bulk rescan" is **not in the authored
  proposal 62** and is claimed by no document on disk → needs an owner.
