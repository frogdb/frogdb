# Proposal 68 — EXEC residual seam → `ExecFraming` datum

Round 38 · lane: seam-vocabulary · effort **M** · `frogdb-core` + `frogdb-server` · **not
spec-first for the refactor; the carved-out hotfix H1 IS spec-first and now sequences FIRST**

Covers exploration-lane candidate **SV8**.

**Revision 2 — verified at HEAD `43720822`.** (Revision 1 was written at `3dd9f1df`; the
adversarial review was conducted against that revision, and the reviewer **built the server and
captured wire frames**. Their bug characterisation supersedes revision 1's and is adopted
wholesale below.) Every citation in this document was re-derived at `43720822`; siblings landed
in between and **many line numbers moved**. Corrections are listed in
[Citation drift since revision 1](#citation-drift-since-revision-1) so a reviewer holding the old
text can diff.

The headline change in this revision: **the live wire bug is four defects, not one; the worst of
them loses data in RESP2; and the enum revision 1 proposed would have codified all four into a
public `frogdb-core` type.** The corrected shape is *protocol-independent* and strictly smaller.

## Summary

EXEC's deferred-command path is the last place in connection dispatch that still decides *shape*
from a **command-name string**. Round 8 (`ab340681`, proposal 03) replaced `cmd_name == "…"`
dispatch across the whole connection surface with a declared capability datum, `ConnMutation`, and
closed with an explicit deferral: EXEC keeps its own match, because the framing question EXEC asks
— *how many wire frames does this command produce, and which of them is the EXEC-array slot value?*
— had no datum to answer it. That deferral is on the ledger twice:
`.scratch/arch-deepening/issues/open/04-retire-legacy-handlers.md:19-21` ("since EXEC is
deliberately not a narrow-ConnCtx leaf") and
`.scratch/testing-improvements/issues/57-subscribe-in-multi-parity-residue.md` (pruned from disk;
`git show 693f6384:` — the Redis-8.6.4-verified policy record, which also records at its `:103-105`
that the fallback arm was "kept as a defensive fallback"). This proposal is the sanctioned revisit.

- **The name-keyed match hides four wire defects, one of which loses data.** Captured from a
  running server, RESP3, `MULTI` / `SUBSCRIBE a b` / `EXEC`: duplicate frame, **count inversion**,
  a `>` push nested inside a `*` array, and — RESP2, same command sequence — **N−1 confirmations
  silently dropped**. See [P4](#p4--the-four-wire-defects-live-captured). All six FM-TXN-043
  forcing tests are single-channel, so every one of the four is invisible to the suite.
- **The fix and the design simplification are the same edit.** Redis emits N confirmations,
  each exactly once, in argument order; only its array *header* under-counts. Reproducing that is
  `(responses.remove(0), responses)` — for **both** protocols, deleting the protocol branch. This
  is hotfix **H1**, spec-first, and it now **sequences before** the refactor, because the enum must
  be defined against the fixed shape, not the broken one.
- **One field, six declarations, two matches deleted.** Add
  `CommandSpec::exec_framing: ExecFraming` — `{Single, OnePerChannel, DeniedInMulti}`, with **no
  protocol parameter anywhere**. Of **400** `CommandSpec` literals in the tree (grep pattern
  `= CommandSpec {`: 296 `frogdb-commands` / 72 `frogdb-server` / 32 `frogdb-core`; the naive
  `CommandSpec {` gives 830 and is wrong), exactly **six** declare a non-default value.
- **The duplicated mutation match collapses.** `transaction.rs:78-106` is a hand-copy of
  `dispatch.rs:201-217` that has already **drifted** (`execute` vs `execute_multi` — ruled
  **latent**). Under the proposal EXEC calls the *same* `dispatch_connection_command`, then applies
  a pure framing function. One match, one home.
- **The name-keyed match dies, and its `_` arm is proven dead** — by a corrected proof
  ([P3](#p3--the-_-arm-is-unreachable-and-is-a-trap)); revision 1's proof leaned on two links that
  do not hold (registry lookup ≠ match-arm domain; `validate()` is `debug_assert`-only).

Nothing here edits a **locked crate**. `frogdb-txn` carries only the command *name* across its seam
(`host.rs:137-141`); the datum lives on `CommandSpec` in `frogdb-core` and is consumed in
`frogdb-server`. `frogdb-vll` is untouched. But FM-TXN-043 is a **LOCKED** row
(`txn-failure-modes.md:542-552`) whose *NOT observable* field the current code **violates as
written**, so H1 is a defect fix against an existing invariant — see
[Spec / LOCKED impact](#spec--locked-impact).

## Files involved

| Path | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/core/src/command_spec.rs` | 1778 | **New**: `ExecFraming` enum + `CommandSpec::exec_framing` field (struct `:469-507`; `strategy` `:494`, `mutation` `:501`). `validate()` (`:807`) gains one cross-check, reusing the existing `SpecError::ConnMutationStrategyMismatch` machinery (variant `:679`, message `:738`, the pub/sub cross-check `:847-852`) |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | 1128 | **Primary deletion.** `exec_pubsub_in_transaction` (`:965-1005`) removed in full, incl. the dead `_` arm (`:1000-1003`). The nine specs (`:767-804`) gain `exec_framing` on six of them; `pubsub_spec()` (`:739-760`) gains one parameter. **H1 edits `:983-994` only.** Read-only: `PubSubKind` (`:722-734`), `execute_multi` (`:874-896`), `execute_pubsub` (`:915-940`), the `to_response(self.state.protocol_version)` call sites (`:376`, `:424`, `:435`) that make the protocol parameter unnecessary |
| `frogdb-server/crates/server/src/connection/transaction.rs` | 243 | **Primary edit.** `execute_connection_level_in_transaction` (`:56-116`): the duplicated `spec().mutation` match (`:78-106`) is replaced by a call to `dispatch_connection_command` + `frame_for_exec`. The PSYNC tail (`:109-115`) is preserved verbatim. Read-only: `handle_exec` (`:37-44`), `impl TxnHost` (`:120-243`) |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 1177 | `dispatch_connection_command` (`:187-218`; the match itself `:201-217`) becomes the single home of the mutation match; its visibility widens within the module. Read-only evidence: `execute_reset` (`:317-326`, reads `spec().mutation` at `:319`) and the `PreAuthIntercept` arm (`:442-458`, reads it at `:451`) each do so for a *single statically-known command* — **not census hits**. The name↔`ConnMutation::PubSub` test table (`:1108-1116`) is the tripwire for P3 and is unchanged. The TransactionControl→queue ordering (`:524` then `:537`) is the evidence for census ruling C4 |
| `frogdb-server/crates/server/src/connection/transaction_conn_command.rs` | 667 | **Read-only, out of scope.** `dispatch_transaction_command` (`:406-439`) is name-keyed on `EXEC`/`MULTI`/`DISCARD`/`WATCH`/`UNWATCH`; its own doc (`:403-405`) states the caller intercepts it *before* the queuing check. Census hit, ruled **out of scope** — see C4 |
| `frogdb-server/crates/txn/src/exec.rs` | 412 | **Read-only, LOCKED.** The deferred partition (`:255-266`, incl. `cmd.name_uppercase()` at `:256` — the load-bearing link in the P3 proof), the re-zip (`:313-333`, `run_connection_level` at `:318-320`), and `result.extend(deferred_pushes)` (`:347-348`) — the second half of the P4 trace |
| `frogdb-server/crates/txn/src/host.rs` | 145 | **Read-only, LOCKED.** `run_connection_level(&mut self, name: &str, …) -> (Response, Vec<Response>)` (`:137-141`). The seam's *signature is unchanged*; only its server-side body changes |
| `frogdb-server/crates/core/src/registry.rs` | 506 | **Read-only.** `register_connection` (`:205`) inserts under `spec().name.to_ascii_uppercase()` (`:219`); `get_entry` (`:229`) uppercases the lookup. No alias table. Its `validate()` assertions (`:206-212`) are `debug_assert!` — **release builds skip them**, which is why the P3 proof no longer rests on them |
| `frogdb-server/crates/core/src/pubsub.rs` | 1523 | **Read-only.** `PubSubConfirmation` (`:303-326`), `items()` (`:328-359`), `to_response` (`:360-368`): RESP3 → `Response::Push`, RESP2 → `Response::Array`. **This is where protocol is resolved — before any `Vec<Response>` exists**, which is why `frame_for_exec` takes no protocol |
| `frogdb-server/crates/core/src/conn_command.rs` | 1172 | **Read-only.** `execute_multi`'s default one-element wrap (`:962-968`) — half of the P1 latency argument |
| `frogdb-server/crates/server/src/connection.rs` | 922 | **Read-only.** The reply write loop (`:543-561`) applies no push/array filtering — first hop of the P4 socket trace. The genuinely out-of-band pub/sub delivery arm lives in `run` (`:700-785`) and is a *different* path that EXEC pushes never take |
| `frogdb-server/crates/server/src/server/register.rs` | 922 | **Read-only.** The nine pub/sub registrations (`:129-139`) |
| `frogdb-server/crates/server/tests/integration_pubsub.rs` | — | The six FM-TXN-043 forcing tests (`:2051`, `:2083`, `:2117`, `:2160`, `:2222`, `:2324`) — **every one single-channel**. H1's two new frame-exact tests (RESP3 + RESP2, ≥2 channels) belong here; the framing *unit* tests belong in `frogdb-core` |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | **LOCKED.** FM-TXN-043 (`:542-552`) + its deviations row (`:650`) + the Scope line (`:10-15`). H1 edits the row (clarify `Observable`, extend `Forced by`, extend `:650`); the refactor adds one `Invariant` clause; H3 fixes the Scope line |

## Problem

### The census — every site that special-cases EXEC/MULTI framing

Derived by grepping `MULTI`, `EXEC`, `in_transaction`, `queued`, `Deferral`, `spec().mutation` and
`cmd_name ==` across `frogdb-server/crates`. Twelve regions match; four are in scope.

| # | Site | What it special-cases | Ruling |
|---|---|---|---|
| C1 | `transaction.rs:78-106` | Deferred connection command → dispatch shape, by `spec().mutation` | **IN SCOPE.** The duplicate of C2 |
| C2 | `dispatch.rs:201-217` | Main-path connection command → dispatch shape, by `spec().mutation` | **IN SCOPE.** Becomes the single home |
| C3 | `pubsub_conn_command.rs:965-1005` | Pub/sub-in-MULTI → EXEC framing, **by command name** | **IN SCOPE.** Deleted; replaced by the datum |
| C4 | `transaction_conn_command.rs:406-439` | `EXEC`/`MULTI`/`DISCARD`/`WATCH`/`UNWATCH` → transaction *control* | **OUT OF SCOPE — a different obligation, now argued from code.** These five verbs are intercepted at the `TransactionControl` stage (`dispatch.rs:524`) **before** the queuing check (`dispatch.rs:537`), and the function's own doc says so (`transaction_conn_command.rs:403-405`: "The caller intercepts this *before* the transaction-queuing check, so these commands are never queued inside a MULTI"). `ExecFraming` describes what happens to a command that **was** queued and is being replayed at EXEC. A command that is never queued has no EXEC slot and no frame apportionment — the datum is not merely silent about C4, it is *inapplicable*. Folding C4 in would need a separate "pre-queue interception" datum, which is a real but distinct proposal. Owned by nobody this round |
| C5 | `guards.rs:513` (`try_queue_in_transaction`), `:544` (`queue_command`) | *Queue eligibility* — whether a command is queued at all | **OUT OF SCOPE, already datum-driven.** Decided from `spec()`, exactly as FM-TXN-043's Invariant requires. This is the half of the invariant that is already true |
| C6 | `guards.rs:964` (`fold_queued_batch`) | Batch key/slot folding | **OUT OF SCOPE.** Routing, not framing |
| C7 | `guards.rs:180-197` (`is_allowed_in_pubsub_mode`) / `:200-215` (`is_auth_exempt`) | Pub/sub-mode and auth-exempt command lists | **ADJACENT.** Both are *mostly* spec-driven already (they consult `entry.execution_strategy()` at `:190-196` / `:209-214`); the residual name literals are three-element `matches!` escapes (`"PING" | "QUIT"` at `:186`, `"QUIT" | "PING" | "HELLO"` at `:205`). Same *class* of smell, different datum (allow-lists, not frame counts), and they gate the non-transactional path. Explicitly not claimed |
| C8 | `txn/exec.rs:313-333`, `:347-348` | Deferred re-zip + `result.extend(deferred_pushes)` | **READ-ONLY, LOCKED.** The consumer of the `(Response, Vec<Response>)` contract. Unchanged — and the second half of the P4 trace |
| C9 | `txn/host.rs:137-141` | `run_connection_level(name: &str, …)` | **READ-ONLY, LOCKED.** The *name* is what crosses the seam. Signature deliberately unchanged: narrowing it to a `&CommandSpec` would drag a `frogdb-core` type across an ADR-0002 boundary for no gain |
| C10 | `core/src/scripting/bindings.rs:10` (`is_forbidden_in_script`) | `MULTI`/`EXEC`/`DISCARD`/`WATCH` name table | **OUT OF SCOPE.** Script sandbox policy, different axis |
| C11 | `slot_migration/routing.rs:276-278`, `:348` | Literal `"EXEC"` as a *synthetic* command name for batch routing | **OUT OF SCOPE.** A placeholder name for a whole-batch route, not a real registry lookup |
| C12 | `replication/src/lib.rs:137-141`, `apply.rs:550`, `:571` | `MULTI`/`EXEC` **wire framing on the replication stream** | **OUT OF SCOPE, LOCKED crate.** Different protocol layer; unrelated to client EXEC framing |

Three near-misses worth naming so a reviewer does not re-find them. `dispatch.rs:319`
(`execute_reset`) and `dispatch.rs:451` (`PreAuthIntercept`) both read `spec().mutation`, but each
does so for one statically-known command to build its `ConnCtx` — they are *consumers* of the datum,
not framing dispatches. And `connection.rs:700-785` is the pub/sub **delivery** arm of the run loop:
genuinely out-of-band frames, fed from `pubsub_rx`, on a path EXEC's `deferred_pushes` never take
(those go through the ordinary reply write loop at `connection.rs:543-561`). Naming it matters
because "out-of-band" in this proposal means *"after the EXEC array, on the reply path"*, not *"from
the delivery arm"*.

### P1 — the duplicated match, already drifted

`dispatch.rs:201-217` and `transaction.rs:78-106` are the same three-way match over `ConnMutation`,
written twice, with the same comment written twice ("selects its dispatch shape from its declared
`mutation` capability … never from its string name"). They have already diverged in the arm that
matters least *today*:

```rust
// dispatch.rs:213-216 — main path
mutation @ (ConnMutation::None | ConnMutation::Auth | ConnMutation::Client) => {
    let mut ctx = self.conn_ctx_for(mutation);
    command.execute_multi(&mut ctx, args).await          // <-- Vec<Response>
}

// transaction.rs:98-105 — EXEC path
mutation @ (frogdb_core::ConnMutation::None
| frogdb_core::ConnMutation::Auth
| frogdb_core::ConnMutation::Client) => (
    command.execute(&mut self.conn_ctx_for(mutation), args).await,   // <-- Response
    vec![],
),
```

**Ruling: LATENT, not live** (re-confirmed at `43720822`). `execute_multi`'s default wraps `execute`
in a one-element `Vec` (`core/src/conn_command.rs:962-968`), and the *only* override in the entire
tree is `PubSubConnCommand::execute_multi` (`pubsub_conn_command.rs:874` — verified by
`grep -rn 'fn execute_multi'`, exactly two hits: the default and that override). Pub/sub never
reaches this arm (`ConnMutation::PubSub` is matched first), so no frames are dropped today. The trap
is the second override: the first non-pub/sub connection command to emit multiple frames will work
on the main path and silently lose all but its first frame inside MULTI. There is no test that would
catch it, because there is no such command to test.

### P2 — the name-keyed EXEC framing

`exec_pubsub_in_transaction` (`pubsub_conn_command.rs:965-1005`) is a four-arm `match cmd_name`:

| Arm | Lines | Commands | Behaviour |
|---|---|---|---|
| `"PUBLISH" \| "SPUBLISH" \| "PUBSUB"` | `:972-978` | 3 | Take the **first** response as the EXEC slot; no trailing frames |
| `"SUBSCRIBE" \| "UNSUBSCRIBE" \| "PSUBSCRIBE" \| "PUNSUBSCRIBE" \| "SUNSUBSCRIBE"` | `:979-995` | 5 | Branches on protocol; both branches are wrong — see P4 |
| `"SSUBSCRIBE"` | `:996-999` | 1 | Refuse: `ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client` |
| `_` | `:1000-1003` | 0 | `ERR command not supported inside MULTI` |

The `PubSubKind` enum at `:722-734` already *is* the per-command datum — it is threaded through
every one of the nine specs by `pubsub_spec()` at `:739-760` and consumed by `execute_multi` at
`:884-894`. It is private to the module, so the EXEC path in a *different* module could not reach it
and re-derived the same partition from strings. That is the locality failure in one sentence: **the
fact was declared once, in a place the second reader could not see, so the second reader re-derived
it from names — and got it wrong four different ways.**

### P3 — the `_` arm is unreachable, and is a trap

Revision 1's proof had two bad links, both correctly caught by review:

- it cited `registry.rs:229` (`get_entry` uppercases) as governing the *match arms*. It does not:
  `get_entry` governs **lookup**. The match arms are matched against the `cmd_name` **string the
  caller passes**, which is a different value with a different provenance.
- it cited `CommandSpec::validate()` as *enforcing* `PubSub ⇔ ConnectionLevel(PubSub)`. It is
  invoked only from `debug_assert!` (`registry.rs:206-212`), so **release builds skip it entirely**.
  It is a development tripwire, not a guarantee.

The corrected proof, four links, each re-derived at `43720822`:

1. **Sole caller.** `exec_pubsub_in_transaction` has exactly one caller: `transaction.rs:85-88`,
   inside the `ConnMutation::PubSub` arm.
2. **The string is uppercase, guaranteed in `frogdb-txn`.** The `cmd_name` that reaches
   `execute_connection_level_in_transaction` is the `DeferredKind::ConnectionLevel { name }` payload
   built at `txn/exec.rs:256-259`: `let name = cmd.name_uppercase();` then
   `String::from_utf8_lossy(&name).to_string()`. This is the real guarantee — and it sits in a
   **LOCKED** crate this proposal reads and does not touch, which makes it a *stable* guarantee, not
   an incidental one.
3. **The command reached is registered under that exact uppercase name.** `transaction.rs:69-73`
   resolves `registry.get_entry(cmd_name).and_then(as_connection)`; entries are inserted under
   `spec().name.to_ascii_uppercase()` (`registry.rs:219`) with **no alias table**. So a non-`None`
   resolution means `cmd_name` equals some registered spec's uppercased name.
4. **`pubsub_spec()` is the sole tree-wide producer of `ConnMutation::PubSub`, over nine uppercase
   literals.** `grep -rn 'ConnMutation::PubSub'` across `frogdb-server/crates` returns exactly one
   spec-construction site: `pubsub_conn_command.rs:757`, inside the `const fn pubsub_spec()` body
   (`:739-760`). Every other hit is a *consumer* (`transaction.rs:85`, `dispatch.rs:206`,
   `conn_command.rs:198`), the `validate()` cross-check (`command_spec.rs:847`), the
   name↔mutation test table (`dispatch.rs:1108-1116`), or two test-local field mutations
   (`command_spec.rs:1210`, `:1222`). `pubsub_spec()` has exactly nine call sites (`:768`, `:769`,
   `:776`, `:777`, `:784`, `:785`, `:791`, `:797`, `:803`), each passing an uppercase string
   literal: `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE`,
   `PUBLISH`, `SPUBLISH`, `PUBSUB` — precisely the nine names with explicit arms, and precisely the
   nine registered at `server/register.rs:129-139`.

Links 2 + 3 + 4 close the domain: the only strings that can reach the match are those nine, all of
which have explicit arms. **The `_` arm is dead.** A bypass hunt for any other way to construct a
`ConnMutation::PubSub` spec (a `..spread`, a builder, a non-literal `mutation:` assignment) came back
clean at this HEAD.

Issue 57 records at `:103-105` that the arm was "kept as a defensive fallback" — but the defence is
inverted: it does not catch a bug, it *manufactures* one. Add a tenth pub/sub command tomorrow, and
it queues fine (C5 is spec-driven), executes fine on the main path, and answers
`-ERR command not supported inside MULTI` inside a transaction. The compiler cannot help, because
`&str` is not an enum. Hence **H2** below.

### P4 — the four wire defects (**LIVE, captured**)

The reviewer built the server and captured raw frames. RESP3 client,
`MULTI` / `SUBSCRIBE a b` / `EXEC`:

```
*1
>3 subscribe b 2      <- the EXEC array's single declared element
>3 subscribe a 1
>3 subscribe b 2
```

RESP2 client, same sequence:

```
*1
*3 subscribe b 2
```

Four distinct defects, against the direct (non-MULTI) path, which emits N confirmations in argument
order with monotonically increasing counts (`pubsub_conn_command.rs:376`, one
`to_response(self.state.protocol_version)` per channel inside the subscribe loop):

| # | Defect | Severity | Cause |
|---|---|---|---|
| **(a)** | **Duplicate.** `subscribe b 2` appears twice — once as the array element, once trailing | Wire-visible, client-confusing | `pubsub_conn_command.rs:987-988`: `let exec_result = responses.last().cloned(); (exec_result, responses)` — the last element is *both* the slot value and still in the pushed vec |
| **(b)** | **Order inversion.** Counts arrive `2, 1, 2` — non-monotonic. A client tracking its subscription count from the confirmations sees it go 2 → 1 → 2 | Wire-visible; breaks the one invariant the count field exists to carry | Same line: `.last()` promotes the *final* confirmation ahead of the earlier ones |
| **(c)** | **Illegal RESP3 nesting.** The array's declared element is a `>` push frame. A conformant RESP3 client treats `>` as out-of-band and will skip it while waiting for the array element — then skip the next two as well, then block | Protocol-conformance | `txn/exec.rs:347` wraps `final_results` (which now contains a `Response::Push`) in `Response::Array`; nothing between there and the socket rewrites it (`connection.rs:543-561` no filtering; `frame_io.rs:41` `narrow_to_wire`; `protocol/src/response.rs:422` keeps `Push` as `Push`) |
| **(d)** | **RESP2 confirmation loss.** `subscribe a 1` is **dropped entirely** — never written to any socket, unrecoverable by the client | **Worst: silent data loss.** For an N-channel SUBSCRIBE, N−1 confirmations vanish | `pubsub_conn_command.rs:990-993`: `(responses.into_iter().last().unwrap_or_else(Response::ok), vec![])` — takes the last and discards the vec |

**All six FM-TXN-043 forcing tests are single-channel and therefore structurally blind to all
four.** `test_subscribe_confirmation_in_multi_exec_resp2` (`:2051`) subscribes to `ch` and asserts
`items.len() == 1`; `test_subscribe_confirmation_in_multi_exec_resp3` (`:2083`) subscribes to `ch`,
asserts `matches!(exec, Resp3Frame::Array { .. })`, then reads *exactly one* further raw frame. With
one channel, `.first()`, `.last()`, dup-vs-no-dup, and drop-vs-keep are all the same bytes. The
remaining four (`:2117`, `:2160`, `:2222`, `:2324`) are likewise single-argument. Issue 57's own
"Tests added" list (`git show 693f6384:…:112-128`) confirms the multi-channel case was never
considered.

Two paths verified **correct** and out of H1's blast radius: `SSUBSCRIBE` (refused, `:996-999`) and
`PUBLISH`/`SPUBLISH`/`PUBSUB` (`:972-978` — these genuinely produce exactly one frame, per
`execute_multi`'s `:891-893` arms, so `.next()` is right). `UNSUBSCRIBE`/`PUNSUBSCRIBE`/
`SUNSUBSCRIBE` ride the same broken branch as `SUBSCRIBE` and are fixed by the same edit.

**Parity ruling (settled, not deferred).** Revision 1 declined to rule and called this "a parity
question this proposal does not presume to answer". That deferral is withdrawn. Redis's
`execCommand` writes the array header for `mstate.count` and then lets each queued command append
its own replies directly to the output buffer; `subscribeCommand` appends one confirmation per
channel via `addReplyPubsubSubscribed`. So Redis emits **N confirmations, each exactly once, in
argument order**, and its array header **under-counts** (declares 1 for a 2-channel SUBSCRIBE).
The under-count is an upstream quirk; the N-once-each-in-order emission is the contract. Issue 57's
record independently verified this policy family against Redis 8.6.4 source
(`git show 693f6384:….md:70-71` and the `pubsub.c` readings at `:79-89`), and nothing in that record
contradicts the ruling.

Therefore the correct implementation is, **for both protocols**:

```rust
let responses = self.execute_pubsub(command, args).await;
let mut responses = responses;
if responses.is_empty() { return (Response::ok(), vec![]); }
let slot = responses.remove(0);
(slot, responses)          // first is the array element; the rest follow, in order
```

This kills (a), (b) and (d) at once and **deletes the `is_resp3()` branch** — the parity fix and the
design simplification are literally the same edit, confined to `pubsub_conn_command.rs:983-994`.
Defect (c) **survives on purpose**: it is the visible residue of Redis's own under-counting array
header, so imitating Redis means keeping it. It gets a row in the deviations table (`:650`) rather
than a fix — see [Spec / LOCKED impact](#spec--locked-impact).

### The ledger record being revisited

- `.scratch/arch-deepening/proposals/03-conn-command-unification.md` — the round-8 proposal that
  introduced `ConnMutation`, with migration step 1 "Add the declarative field, default-preserving."
- Commit **`ab340681`** — "refactor(server): spec-declared `ConnMutation` replaces `cmd_name`
  dispatch matches", 92 files, +807/−211. Its message claims "the MULTI-deferred path … derive their
  builders the same way" — true of the *builder*, not of the framing, which is precisely the residue.
- `.scratch/arch-deepening/issues/open/04-retire-legacy-handlers.md:19-21` — the deliberate-deferral
  statement.
- `.scratch/testing-improvements/issues/57-subscribe-in-multi-parity-residue.md` (pruned from disk;
  `git show 693f6384:`) — the Redis-8.6.4 policy verification (`:70-94`), the change log (`:96-110`),
  the "kept as a defensive fallback" note (`:103-105`), and the single-channel test list (`:112-128`).

The seam the deferral was waiting on now exists: `CommandSpec` carries per-command dispatch facts
(`mutation`, `strategy`, `lookup`, `reindex`, `wakes`, …) and `validate()` already cross-checks them.
The framing fact is the same kind of fact.

## Proposed change

> **Sequencing precondition.** This section describes the tree **after H1 has landed**. Defining
> `ExecFraming` against today's behaviour would freeze four wire defects into a public `frogdb-core`
> enum — the single worst outcome available here, and the reason the hotfix order changed in this
> revision. See [Hotfix ordering](#independently-landable-hotfixes).

### The datum

In `frogdb-core` (`command_spec.rs`), next to `ConnMutation`:

```rust
/// How this command's wire frames are apportioned when it runs deferred inside
/// `MULTI`/`EXEC`: which frame becomes the EXEC-array slot value, and which
/// frames (if any) follow the EXEC reply on the wire.
///
/// **Protocol-independent by construction.** Every `Response` this datum
/// apportions has already been resolved to its protocol shape by
/// `PubSubConfirmation::to_response` (`core/src/pubsub.rs:360-368`) before the
/// `Vec<Response>` exists. Framing counts frames; it never reshapes them.
///
/// Ignored outside EXEC — the main dispatch path flattens every frame onto the
/// wire in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecFraming {
    /// Exactly one frame; it is the EXEC slot value. Every command except the
    /// six below.
    #[default]
    Single,
    /// One frame per argument (channel/pattern). The **first** is the EXEC slot
    /// value; the remainder follow the EXEC reply, in argument order. Matches
    /// Redis, whose array header under-counts these (see FM-TXN-043).
    OnePerChannel,
    /// Refused inside MULTI. Redis's DENY_BLOCKING refusal, rendered from the
    /// command's own name — no per-command error string.
    DeniedInMulti,
}
```

`CommandSpec` gains `pub exec_framing: ExecFraming`. `validate()` gains one cross-check:
`exec_framing != Single` requires `strategy: ConnectionLevel(_)` — a shard command's frames never
reach this path, so a non-default there is a spec bug. It reuses the existing
`SpecError::ConnMutationStrategyMismatch` family (`:679`/`:738`/`:847-852`). **Caveat, stated
plainly:** `validate()` is only ever called under `debug_assert!` (`registry.rs:206-212`), so this
cross-check is a development tripwire, not a release guarantee — the same status every other
`CommandSpec` invariant has today.

### The framing function

A free function in `frogdb-core`, taking plain data and returning plain data — **no protocol
parameter**:

```rust
pub fn frame_for_exec(
    framing: ExecFraming,
    name: &str,
    responses: Vec<Response>,
) -> (Response, Vec<Response>)
```

- `Single` → `(responses.into_iter().next().unwrap_or_else(Response::ok), vec![])`
- `OnePerChannel` → first as slot, the remainder trailing, in order
- `DeniedInMulti` → `(Response::error(format!("ERR {name} isn't allowed for a DENY BLOCKING client")), vec![])`

`DeniedInMulti` **renders** SSUBSCRIBE's error rather than storing it: upstream Redis's message is
the generic `"%s isn't allowed for a DENY BLOCKING client"`, so the name is the only variable and no
error text needs to live in a spec literal.

**An uncomfortable consequence, stated up front.** After H1, `Single` and `OnePerChannel` differ
*only* in what they do with frames 2..N: `Single` drops them, `OnePerChannel` forwards them. And no
`Single` command produces a frame 2 today — `execute_multi`'s default emits exactly one
(`conn_command.rs:962-968`) and the sole override's `Single` arms emit exactly one
(`pubsub_conn_command.rs:891-893`). So at the moment H1 lands, the only *observable* distinction the
enum carries is `DeniedInMulti` versus everything else: one bit, for one command. The
`Single`/`OnePerChannel` split is forward-safety for exactly the case P1 describes, not a present
behaviour difference. A reviewer may reasonably prefer the two-variant `{Framed, DeniedInMulti}`
shape; that datum is honestly named "is this command refused inside MULTI", which is close kin to the
queue-eligibility family (C5) and invites a merge this proposal does not attempt. Three variants are
proposed because the drop-vs-forward policy is the thing P1's latent bug turns on, and encoding it
is the only way to make the answer reviewable at the spec instead of implicit in a match arm. This is
the proposal's thinnest point and the [deletion test](#deletion-test-honestly) weighs it accordingly.

### The call sites

`transaction.rs:78-106` collapses to a call into the *existing* main-path dispatch plus the framing
function:

```rust
if let Some(command) = migrated {
    let responses = self.dispatch_connection_command(command, args).await;
    return frogdb_core::frame_for_exec(
        command.spec().exec_framing,
        command.spec().name,
        responses,
    );
}
// PSYNC tail (109-115) unchanged.
```

`dispatch_connection_command` (`dispatch.rs:187-218`) keeps its three-arm mutation match verbatim and
becomes the single home of it. `exec_pubsub_in_transaction` is deleted whole, along with its four-arm
name match. `pubsub_spec()` (`:739-760`) gains an `ExecFraming` parameter; six of its nine call sites
(`:768`/`:769`/`:776`/`:777`/`:785` → `OnePerChannel`, `:784` → `DeniedInMulti`) pass a non-default;
`PUBLISH` (`:791`), `SPUBLISH` (`:797`) and `PUBSUB` (`:803`) take the `Single` default.

Two consequences fall out for free, without a behavioural decision:

- **P1 disappears by construction.** EXEC now goes through `execute_multi`, the same as the main
  path, because there is only one path.
- **P3 disappears by construction.** A tenth pub/sub command must declare `exec_framing`; omission
  gives it `Single`, which is a *defensible* default (one frame, that frame is the slot value) rather
  than a spurious refusal. There is no `_` arm to fall into.

### The alternative, weighed honestly

The cheaper shape is to make `PubSubKind` (`pubsub_conn_command.rs:722-734`) `pub(crate)` and have
`transaction.rs` match on it. That deletes the string match with ~zero churn and no `CommandSpec`
edit — no 400-literal sweep, no `frogdb-core` change.

It is rejected, but the margin is **thin, and thinner after H1** than revision 1 claimed:

- It keeps EXEC framing knowledge in the pub/sub module, so the *next* connection command family
  that needs multi-frame EXEC framing re-derives it again. That is the exact failure P2 documents,
  deferred one round.
- It cannot host the framing function in `frogdb-core`, so the unit tests stay in `frogdb-server` —
  which matters, because `cargo mutants -p <crate>` runs only that package's own tests.
- It leaves P1 (the duplicated mutation match) standing, since `transaction.rs` would still need its
  own dispatch to get at the `Vec<Response>`.
- **Against all three:** post-H1 the enum's live discriminating content is one bit for one command
  (above), so "the next family re-derives it" is a smaller loss than it sounds, and `PubSubKind`
  already *is* a per-command enum — a tenth pub/sub command must extend it and gets a compiler error
  rather than a silent `_`. That is P3's fix at a fraction of the cost.

**Recommendation: if 90 does not land this round, take the `PubSubKind` alternative.** It captures
P2's deletion and P3's fix and forfeits only P1 and the test-locality win — which is not worth 400
literal edits.

### What does not change

- **The `TxnHost` seam.** `run_connection_level(name: &str, …) -> (Response, Vec<Response>)` keeps
  its signature and its `&str`. `frogdb-txn` compiles untouched; ADR-0002's boundary is not crossed.
  `frogdb-vll` is not read or written.
- **The wire, once H1 has landed.** `Single` reproduces the PUBLISH/SPUBLISH/PUBSUB arm exactly;
  `OnePerChannel` reproduces H1's fixed first-then-rest shape exactly; `DeniedInMulti` reproduces
  SSUBSCRIBE's byte-identical error. The refactor is then a genuine no-op, provable against the two
  frame-exact tests H1 introduces.
- **`PubSubConfirmation`** (`core/src/pubsub.rs:303-368`) and the shared RESP2/RESP3 encoder.
  FM-TXN-043's Invariant clause about a shared encoder stays true and untouched — and it is exactly
  what lets `frame_for_exec` drop the protocol parameter.
- **Queue eligibility** (`guards.rs:513`, `:544`), which was already spec-driven.
- **`PubSubKind`** stays private and keeps its `execute_multi` job. It answers "what does this command
  *do*"; `ExecFraming` answers "how are its frames apportioned at EXEC". Merging them would couple
  two unrelated axes.

### Deletion test, honestly

*Imagine `ExecFraming` gone. What reappears?*

- **Passes.** The four-arm `match cmd_name` reappears in `pubsub_conn_command.rs`, and with it the
  dead `_` arm and the silent-wrong-answer trap for command #10. The duplicated mutation match
  reappears in `transaction.rs`, with its `execute`/`execute_multi` drift. Both are complexity that
  lives *across* modules and is reconstructed from strings — the thing the datum abolishes.
- **Passes.** The framing rules become untestable without a live `ConnectionHandler`; today's only
  coverage is six `frogdb-server` integration tests, every one of which is structurally blind to the
  four defects P4 captured.
- **Fails, and harder than revision 1 admitted.** Post-H1 `frame_for_exec` is **under 10 lines** —
  `Single` is `next()`, `OnePerChannel` is `remove(0)` plus the tail, `DeniedInMulti` is a `format!`
  — with exactly **one** caller. A ten-line one-caller function is a pass-through; it does not earn
  an abstraction. The keep must be earned by the **enum**, and the enum earns it through **locality,
  not leverage**: it moves a per-command fact from a match arm in a module the reader cannot see into
  the spec literal the reader is already looking at, and makes omission a defensible default instead
  of a refusal. It does *not* earn it through discriminating power, which post-H1 is one bit for one
  command (see [The framing function](#the-framing-function)).
- **Fails.** For 394 of 400 specs the field is pure ceremony. That is the standard cost of a
  declarative table, and the same trade `ConnMutation` made in round 8 (where 5 of 5 arms mattered but
  ~380 specs wrote `ConnMutation::None`). It is a real cost, not a zero.

**Net:** two passes, two fails, and the margin over the `PubSubKind` fallback is near-parity if the
400-literal sweep must be paid. The proposal is worth taking **behind 90** and is honestly not worth
taking ahead of it.

## Testability improvement

| Today | After |
|---|---|
| Framing rules are only reachable through a live `ConnectionHandler` + socket; the sole coverage is six `frogdb-server` integration tests (`integration_pubsub.rs:2051`, `:2083`, `:2117`, `:2160`, `:2222`, `:2324`), **all single-channel** | `frame_for_exec` is a pure `(framing, name, Vec<Response>) -> (Response, Vec<Response>)`; every arm × multi-element input is a table test in `frogdb-core`, microseconds, no I/O |
| Those six tests are `frogdb-server` tests, so they contribute **nothing** to `cargo mutants -p frogdb-txn` or `-p frogdb-core` | The framing unit tests live in the crate that owns the code, so mutants of the framing arms are killable in-package |
| No test counts frames after the EXEC array, and none uses ≥2 channels → **all four P4 defects invisible** | H1 adds two frame-exact, multi-channel tests (RESP3 + RESP2) that pin count, order and no-duplication. `.first()`-vs-`.last()` and keep-vs-drop become killable mutants |
| A tenth pub/sub command's MULTI behaviour is untested and silently wrong (`_` arm) | Omission yields `Single`; a `validate()` cross-check (debug builds) catches a non-default on a non-`ConnectionLevel` spec |
| The `execute`/`execute_multi` drift (P1) is invisible — no command exercises it | One dispatch path; the drift is structurally impossible |

## Spec / LOCKED impact

- **No locked-crate source is edited.** `frogdb-txn` (`exec.rs`, `host.rs`) and `frogdb-vll` are
  read-only here — `frogdb-vll` is not even referenced. `frogdb-core`, `frogdb-server`,
  `frogdb-commands` are outside the four locked pairs (ADRs 0002–0004).
- **H1 is a defect fix against the row as it already reads — the row is NOT weakened.** Revision 1
  claimed "the row does not cover it". That was wrong, and the review is correct: **the current code
  violates two of FM-TXN-043's `NOT observable` clauses verbatim** (`txn-failure-modes.md:548`):
  - *"a confirmation shape that differs from the direct path"* — the direct path emits N
    confirmations in argument order with counts 1..N (`pubsub_conn_command.rs:376`); the MULTI path
    emits one (RESP2, count N) or three in the order N,1,…,N (RESP3). Both differ.
  - *"the RESP3 push being folded into the array"* — `responses.last().cloned()` (`:987`) places a
    `Response::Push` at `final_results[i]`, which `txn/exec.rs:347` wraps in `Response::Array`.
    That is the clause, literally.

  The row is **silent** only on multi-channel count and ordering. So H1's `Observable` edit is a
  **clarification**, not a relaxation:
  - *Observable* gains: for an N-argument subscribe/unsubscribe inside MULTI, N confirmations are
    emitted, **each exactly once, in argument order, with monotonically increasing counts**; the EXEC
    array header declares 1 and the first confirmation is its declared element, with the remaining
    N−1 following it.
  - *NOT observable* is **kept verbatim** and extended, not softened: add *"a confirmation emitted
    twice; a confirmation dropped; confirmations out of argument order."*
  - *Forced by* (`:551`) gains two names — this field **is** lint-visible to
    `just lint-failure-modes`, so the tests must exist in the same commit:
    `test_subscribe_multi_channel_in_multi_exec_frames_resp3` and
    `test_subscribe_multi_channel_in_multi_exec_frames_resp2`, both ≥2 channels, both frame-exact
    (assert the full trailing frame sequence, not just the first one).
  - The **deviations table row at `:650`** gains a clause: FrogDB reproduces Redis's under-counting
    EXEC array header for the subscribe family (declared length 1, N frames follow) — defect (c) is
    a *deliberately imitated upstream quirk*, recorded rather than fixed, because fixing it would
    diverge from Redis on a wire shape real clients already tolerate.
- **The refactor owes one Invariant clause.** The Invariant (`:549`) reads: *"Queue eligibility is
  decided by the command's spec, not by an ad-hoc list; the subscribe confirmation encoder is shared
  with the non-transactional path."* Half is already true (C5) and half stays true
  (`PubSubConfirmation`), but neither clause covers *framing*. Add a clause naming `exec_framing`.
  **This is prose only** — `failure-modes.py` parses only backticked `Forced-by` names, so the
  refactor adds/removes/renames no forcing test and `just lint-failure-modes` cannot detect the
  staleness. It must be done by hand, same commit.
- **The spec's Scope line (`txn-failure-modes.md:10-15`) is wrong today.** It names
  `connection/{dispatch,guards,transaction,transaction_conn_command,state}.rs` and **omits
  `pubsub_conn_command.rs`**, even though FM-TXN-043's behaviour is forced entirely from that file.
  Fix as hotfix **H3** — it is independently landable and is the cheapest of the four.
- **No mutation re-gate is owed.** The txn gate (0.90) measures `frogdb-txn` + `frogdb-vll`, neither
  of which changes. H1's forcing tests live in `frogdb-server` (they need a socket), so they earn no
  gate credit either — the gate-relevant win is the `frogdb-core` framing unit tests, and
  `frogdb-core` is ungated. State this rather than imply a gate improvement.

## Risks / scope boundaries vs sibling proposals

**Proposal 90 (CT2) — `CommandSpec` DEFAULT const. Still the load-bearing edge; still not on disk.**
Adding a field to `CommandSpec` today requires editing all **400** literals — pinned with the exact
grep pattern `= CommandSpec {` (296 `frogdb-commands`, 72 `frogdb-server`, 32 `frogdb-core`). The
naive `grep 'CommandSpec {'` returns **830** and double-counts type positions, `impl` blocks and doc
text; anyone re-deriving this number must use the `= ` prefix. Sibling 70 independently verified the
same precondition at its `:443-448`: `CommandSpec` has no `Default` and no `DEFAULT` const at HEAD,
so every literal writes all 13 fields. If 90's `..CommandSpec::DEFAULT` sweep lands first, this
proposal's spec cost is **six** edits and one field declaration. **Recommendation: 90 → 68,
strictly.** If 68 lands first, 90's sweep must re-touch every literal 68 wrote and the diffs collide
across 400 sites. If 90 is dropped from the round, take the `PubSubKind` alternative. 90 does not
exist on disk at `43720822`, so its exact shape remains **unverifiable from this side**.

**Proposal 70 (ACL registry consult) — now on disk; the open question from revision 1 resolves NO.**
70 exists at `.scratch/arch-deepening/proposals/70-acl-registry-consult.md` and states explicitly
(`:66`, `:362`) that **`CommandSpec` `:469-507` is deliberately NOT edited** — it adds a defaulted
trait method on `Command`/`ConnectionCommand` instead. So 70 does **not** inherit 68's 90-ordering
constraint, and the two need not land on the same side of 90. Two real contact points, both benign:
- `core/src/registry.rs` — 70 **edits** it (`CommandImpl` `:29-35` gains `subcommands()`,
  `CommandRegistry` `:163-167`, a new `impl CommandVocabulary`). 68 touches it **read-only**, as
  evidence for the P3 proof. If 70 lands first, 68 must re-derive `register_connection` `:205`, the
  insert at `:219`, `get_entry` `:229` and the `debug_assert!` block `:206-212`. Costs 68 nothing but
  a citation refresh.
- `connection/pubsub_conn_command.rs` — 70 edits the **PUBSUB container declaration** at `:539-549`;
  68 edits `:739-804` (specs) and deletes `:965-1005`. Disjoint line regions in a shared file; a
  conflict would be textual, not semantic.
70 also edits `command_spec.rs` additively (`SPLIT_ADMIN_SURFACES` `:584-629`, `admin_surface`
`:637-651`) while 68 adds `ExecFraming` next to `ConnMutation` and one field inside the struct —
different regions of the same file.

**Proposal 50 (transaction state consolidation) — pre-negotiated, and 68 assents.** 50 wrote, at its
`:32`, `:34`, `:38` and `:402`, that "Sibling 68 owns `exec_pubsub_in_transaction` (L965)" and
proposed a file-ownership rule needing 68's assent. **68 assents to 50's rule as written.** 50's row
`:402` already cites the corrected ranges (`transaction.rs` **L78-106**, `dispatch.rs` **L201-217**)
— those match this revision exactly, so the negotiated boundary is sound. One correction to 50's row
for the record: it places the `ExecFraming` datum "on `CommandSpec` in `frogdb-commands`". That is
the wrong crate — `CommandSpec` is defined in `frogdb-server/crates/core/src/command_spec.rs`
(`frogdb-core`); `frogdb-commands` only *writes literals* of it. 68's core placement stands and
supersedes 50's mislocation.

| File | 50 owns | 68 owns |
|---|---|---|
| `connection/transaction.rs` | `:40` (`take_transaction()`) | `:78-106` (the mutation match) |
| `connection/dispatch.rs` | `:477`, `:482` | `:187-218` |
| `connection/pubsub_conn_command.rs` | `:343` (`take_asking()`) | `:739-804`, `:965-1005` |

Three shared files, zero shared lines. Either order works.

**Proposal 67 (server small dedups) — disjoint *by file*, but the disjointness is
mode-dependent.** 67's touched files are `connection/builder.rs`, `connection.rs`,
`connection/deps.rs`, `acceptor.rs` (read-only), `server/subsystems.rs` (read-only),
`commands/search.rs`, `commands/timeseries.rs`, `commands/migrate_cmd.rs`, `commands/server.rs`,
`core/src/command.rs`, `commands/stub.rs` (read-only), `connection/search/{helpers,index_mgmt,
explain,synonyms}.rs`, and two read-only `core/src/shard/*` files. **No overlap with 68's file set**,
and the two `frogdb-core` additions land in different modules (`command.rs` vs `command_spec.rs`).

**But that holds only behind 90.** Without 90's `DEFAULT` sweep, 68 must add a field to every
`CommandSpec` literal in the tree — including **50 literals inside four of 67's own files**
(verified with `grep -c '= CommandSpec {'` at `43720822`): `commands/search.rs` **26**,
`commands/timeseries.rs` **17**, `commands/server.rs` **6**, `commands/migrate_cmd.rs` **1**. In that
mode the two proposals share four files and the conflicts are mechanical but real. Stated explicitly
so the round scheduler can see it: **68-behind-90 is disjoint from 67; 68-without-90 is not.**

**Proposal 69 (config combinators) — disjoint.** Scoped to `frogdb-config`/config plumbing; shares no
file with 68.

**Behavioural risk of the refactor itself.** With H1 landed first, the refactor's only hazard is that
`Single` and `OnePerChannel` must reproduce H1's `next()`/`remove(0)` selection exactly, including
the `unwrap_or_else(Response::ok)` empty-vec fallbacks. Mitigation: H1 ships its two frame-exact
multi-channel tests, so the refactor has a real oracle for the first time — revision 1's plan
("land the refactor with the duplicate intact") had no such oracle and would have been checked only
against blind single-channel tests. Reviewers should reject any version of this refactor that
changes wire behaviour in passing: with the corrected ordering there is nothing left for it to fix.

## Effort

**M** — the lane estimate holds, but it is bimodal on the 90 ordering.

| Part | Size |
|---|---|
| H1 (spec row edit + 2 frame-exact tests + the `:983-994` fix) — **prerequisite, not part of M** | S |
| `ExecFraming` enum + field + `validate()` cross-check + unit tests (`frogdb-core`) | S |
| Spec literal churn | **~0 if 90 lands first; L (400 literals) if not** |
| Delete `exec_pubsub_in_transaction`, thread `ExecFraming` through `pubsub_spec()` ×9 | S |
| Rewrite `execute_connection_level_in_transaction` to call `dispatch_connection_command` | S |
| FM-TXN-043 Invariant prose refresh | S |

Behind 90: solidly **M**. Ahead of 90 or without it: **L**, and the `PubSubKind` alternative (**S**)
is the better trade.

## Independently-landable hotfixes

**Ordering: H3 → H2 → H1 → 90 → 68.** This is a change from revision 1, which put the refactor's
prerequisites in the wrong order and would have frozen four wire defects into a public enum. H3 and
H2 are free and unblock nothing; H1 must precede 68 because 68's enum is *defined against* the fixed
wire shape.

1. **H3 — the txn spec Scope line omits `pubsub_conn_command.rs`** (`txn-failure-modes.md:10-15`),
   despite FM-TXN-043 being forced entirely from behaviour in that file. Documentation-only, one
   line, lint-invisible (`failure-modes.py` parses only backticked `Forced-by` names). **Land first —
   it costs nothing and every later step cites the corrected scope.**
2. **H2 — delete the dead `_` arm** (`pubsub_conn_command.rs:1000-1003`). Unreachable by the
   corrected four-link proof in P3, which the review independently re-verified (bypass hunt clean;
   arm dead; H2 safe). Zero behaviour change today; removes a future silent-wrong-answer trap. Prefer
   the belt-and-braces form: replace it with
   `unreachable!("pub/sub command {cmd_name} has no EXEC framing arm")` so command #10 fails loudly in
   debug/test instead of answering wrongly in production. **No spec edit** — the arm appears in no
   `Observable` field. Trivially landable now; superseded (not blocked) by the refactor.
3. **H1 — the four wire defects. LIVE, captured, spec-first, and a PREREQUISITE for 68.** Confined
   entirely to `pubsub_conn_command.rs:983-994`. Sequence, in one commit:
   1. Amend FM-TXN-043 (`:542-552`): clarify `Observable` (N frames, once each, argument order,
      monotonic counts, array header declares 1); **keep `NOT observable` verbatim** and extend it
      (duplicated / dropped / out-of-order confirmations); add both new test names to `Forced by`.
      Add the array-header-under-count clause to the deviations row (`:650`).
   2. Write the two failing frame-exact tests in `integration_pubsub.rs`, **≥2 channels each**, one
      RESP3 and one RESP2, asserting the *complete* trailing frame sequence.
   3. Fix: `(responses.remove(0), responses)` for both protocols — which **deletes** the
      `is_resp3()` branch (`:984`, `:989`) entirely.

   Defects (a), (b) and (d) die; (c) is retained on purpose as the imitated upstream quirk and is
   recorded at `:650`. The parity question revision 1 deferred is settled in
   [P4](#p4--the-four-wire-defects-live-captured) — Redis emits N frames once each in argument order
   and under-counts its array header.
4. **H4 (optional, S) — pin the `execute`/`execute_multi` divergence with a test rather than waiting
   for the refactor.** A test-only `ConnectionCommand` that overrides `execute_multi` with two frames,
   registered in a test registry and run inside MULTI, fails today (P1) and passes after the
   refactor. It is the only way to make P1 observable before it becomes impossible. Lives in
   `frogdb-server`, so it earns no mutation-gate credit — file it as a regression test, not a gate
   contribution.

## Citation drift since revision 1

Revision 1 was verified at `3dd9f1df`; everything below was re-derived at `43720822`. Corrections
marked ✎ are line-number drift; marked ✗ are substantive errors in revision 1.

| Revision 1 | Correct at `43720822` | |
|---|---|---|
| `transaction.rs:78-108` (used throughout) | `:78-106` — `};` closes at 106, `}` at 107 | ✎ Sibling 50 already used the correct range at its `:402` |
| `dispatch.rs:188-218` / `:201-218` | fn `:187-218`; match `:201-217` | ✎ |
| `dispatch.rs:205-208` (the P1 snippet) | `:213-216` | ✎ |
| `dispatch.rs:317-326` / `:319`; `:445-456` / `:450` | `:317-326` / `:319`; `:442-458` / `:451` | ✎ |
| `dispatch.rs:1098-1102` (test table) | `:1108-1116` (nine rows) | ✎ |
| `pubsub_conn_command.rs:988-989` (the bug) | `:987-988` RESP3; `:990-993` RESP2 — **both** branches are defective | ✗ revision 1 named only the RESP3 half |
| `pubsub_conn_command.rs:764-800` (nine specs) | `:767-804` | ✎ |
| `pubsub_conn_command.rs:724-733` (`PubSubKind`) | `:722-734` | ✎ |
| `pubsub_conn_command.rs:874-890` (`execute_multi`) | `:874-896` | ✎ |
| `core/src/pubsub.rs:303-365` (`to_response`) | enum `:303-326`, `items()` `:328-359`, `to_response` `:360-368` | ✎ |
| `txn/exec.rs:309-320` ("the deferred partition") | partition `:255-266`; re-zip `:313-333`; `run_connection_level` `:318-320`; append `:347-348` | ✗ `:309-320` is the re-zip preamble, not the partition |
| `connection.rs:440-565` (write loop) | `process_one_command` `:326-564`; the loop `:543-561` | ✎ |
| `server/register.rs:120-139` | `:129-139` | ✎ |
| `guards.rs:180-196` / `:198-215` | `:180-197` / `:200-215` | ✎ |
| `transaction_conn_command.rs:406-435` | `:406-439` | ✎ |
| Six forcing tests at `2047, 2079, 2110, 2155, 2217, 2318` | `2051, 2083, 2117, 2160, 2222, 2324` | ✎ |
| `registry.rs:205`, `:229` as the dead-arm proof | Lookup ≠ match-arm domain; the real guarantee is `txn/exec.rs:256` (`name_uppercase()`) + `pubsub_spec()` `:739`/`:757` as sole `ConnMutation::PubSub` producer | ✗ proof rebuilt in P3 |
| `CommandSpec::validate()` "enforces" the cross-check (`command_spec.rs:849`) | `:847-852`, invoked only under `debug_assert!` (`registry.rs:206-212`) — **release builds skip it** | ✗ tripwire, not guarantee |
| "the row does not cover it" (FM-TXN-043) | The row's `NOT observable` (`:548`) covers it **twice**; the code violates it as written | ✗ H1 is a defect fix, not a spec extension |
| Parity "not presumed to answer" | Settled: N frames, once each, argument order; array header under-counts | ✗ ruling made in P4 |
| `frame_for_exec(…, protocol: ProtocolVersion)` | No protocol parameter — `pubsub.rs:360-368` resolves shape before the `Vec` exists | ✗ per review B2 |
| Hotfix order H1…H4 with H1 after the refactor | **H3 → H2 → H1 → 90 → 68** | ✗ per review B2 |
