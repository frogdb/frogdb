# Proposal 68 — EXEC residual seam → `ExecFraming` datum

Round 38 · lane: seam-vocabulary · effort **M** · `frogdb-core` + `frogdb-server` · **not
spec-first for the refactor; one carved-out hotfix IS spec-first**

Covers exploration-lane candidate **SV8**.

**Verified at HEAD `3dd9f1df`.** The lane brief was written against `08c143d6`. Unusually for
this round, **every lane citation re-derives exactly**: `transaction.rs:78-106` (the match
spans 78–106, the `return match` statement 78–108), `dispatch.rs:201-217` (201–218 with the
closing paren), `pubsub_conn_command.rs:965` (`exec_pubsub_in_transaction`, exact). Nothing
below corrects the brief. What the brief did *not* say is the more important finding: the
name-keyed match it asks us to delete has a **live wire-level bug** in one arm, and the
governing failure-mode row is a **LOCKED** row that says that bug is "NOT observable".

## Summary

EXEC's deferred-command path is the last place in connection dispatch that still decides
*shape* from a **command-name string**. Round 8 (`ab340681`, proposal 03) replaced
`cmd_name == "…"` dispatch across the whole connection surface with a declared capability
datum, `ConnMutation`, and closed with an explicit deferral: EXEC keeps its own match,
because the framing question EXEC asks — *how many wire frames does this command produce, and
which of them is the EXEC-array slot value?* — had no datum to answer it. That deferral is on
the ledger twice: `.scratch/arch-deepening/issues/open/04-retire-legacy-handlers.md:19-21`
("since EXEC is deliberately not a narrow-`ConnCtx` leaf") and
`.scratch/testing-improvements/issues/57-subscribe-in-multi-parity-residue.md` (the
Redis-8.6.4-verified policy record, which also records that the fallback arm was "kept as a
defensive fallback"). This proposal is the sanctioned revisit: the datum now has an obvious
home (`CommandSpec`), and three separate defects have accumulated behind the missing one.

- **One field, six declarations, two matches deleted.** Add
  `CommandSpec::exec_framing: ExecFraming` — `{Single, OnePerChannel, DeniedInMulti}`.
  Of **400** `CommandSpec` literals in the tree (296 `frogdb-commands` / 72 `frogdb-server` /
  32 `frogdb-core`), exactly **six** declare a non-default value: five subscribe-family
  commands (`OnePerChannel`) and `SSUBSCRIBE` (`DeniedInMulti`). Everything else is `Single`,
  which is what the code already does.
- **The duplicated mutation match collapses.** `transaction.rs:78-108` is a hand-copy of
  `dispatch.rs:201-218` that has already **drifted** (`execute` vs `execute_multi` — see
  Problem, ruled **latent**). Under the proposal EXEC calls the *same*
  `dispatch_connection_command`, then applies a pure framing function. One match, one home.
- **The name-keyed match dies, and its `_` arm is proven dead.** The registry keys by
  `spec.name` with **no aliases** (`registry.rs:205`, `:229`), `exec_pubsub_in_transaction`
  has exactly one caller gated on `ConnMutation::PubSub`, and `CommandSpec::validate()`
  enforces `PubSub ⇔ ConnectionLevel(PubSub)` (`command_spec.rs:849`). All nine such commands
  have explicit arms. The `_` arm at `:1000-1003` cannot be reached — and it is worse than
  dead: it is the arm a *tenth* pub/sub command would silently land in, answering
  `-ERR command not supported inside MULTI` to a command that should work.
- **Headline finding: RESP3 subscribe-in-MULTI emits the last confirmation TWICE, live, and
  no test can see it.** `pubsub_conn_command.rs:988-989` returns
  `(responses.last().cloned(), responses)`; `txn/exec.rs:348` (`result.extend(deferred_pushes)`)
  puts the whole `responses` vec on the wire *after* the EXEC array that already contains its
  last element. Traced end-to-end: no filtering exists anywhere between there and the socket.
  FM-TXN-043's own **NOT observable** field says "the RESP3 push being folded into the array"
  — this is the mirror image of that sentence and the row does not cover it. **Carved out as
  hotfix H1: spec-first, independently landable, NOT part of this refactor.**

Nothing here edits a **locked crate**. `frogdb-txn` carries only the command *name* across
its seam (`host.rs:137-141`); the datum lives on `CommandSpec` in `frogdb-core` and is
consumed in `frogdb-server`. But FM-TXN-043 is a **LOCKED** row (`txn-failure-modes.md:542`)
whose Invariant prose describes this code, so the refactor owes a same-commit prose refresh —
see Spec impact.

## Files involved

| Path | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/core/src/command_spec.rs` | 1778 | **New**: `ExecFraming` enum + `CommandSpec::exec_framing` field (struct at 469–507; `mutation` at 501, `strategy` at 494). `validate()` gains one cross-check (existing `ConnMutationStrategyMismatch` machinery at 679 / 738 / 849) |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | 1128 | **Primary deletion.** `exec_pubsub_in_transaction` (965–1006) removed in full, incl. the dead `_` arm (1000–1003). The nine specs (764–800) gain `exec_framing` on six of them. `pubsub_spec()` const fn (739) gains one parameter. Read-only: `PubSubKind` (724–733), `execute_multi` (874–890), `execute_pubsub` (915–940) |
| `frogdb-server/crates/server/src/connection/transaction.rs` | 243 | **Primary edit.** `execute_connection_level_in_transaction` (56–116): the duplicated `spec().mutation` match (78–108) is replaced by a call to `dispatch_connection_command` + `frame_for_exec`. The PSYNC tail (109–115) is preserved verbatim. Read-only: `handle_exec` (37–44), `impl TxnHost` (119–243) |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 1177 | `dispatch_connection_command` (188–218) becomes the single home of the mutation match; its visibility widens within the module. Read-only evidence: `execute_reset` (317–326) and the PreAuthIntercept arm (445–456) read `spec().mutation` for a *single known command*, not to frame a dispatch — **not census hits**. Test table (1098–1102) unchanged |
| `frogdb-server/crates/server/src/connection/transaction_conn_command.rs` | 667 | **Read-only, out of scope.** `dispatch_transaction_command` (406–435) is name-keyed on `EXEC`/`MULTI`/`DISCARD`/`WATCH`/`UNWATCH`. Census hit, ruled **out of scope** — see census note C4 |
| `frogdb-server/crates/txn/src/exec.rs` | 412 | **Read-only, LOCKED.** The deferred partition (309–320) and `result.extend(deferred_pushes)` (348) — the second half of the H1 duplicate-frame trace |
| `frogdb-server/crates/txn/src/host.rs` | 145 | **Read-only, LOCKED.** `run_connection_level(&mut self, name: &str, …) -> (Response, Vec<Response>)` (137–141). The seam's *signature is unchanged*; only its server-side body changes |
| `frogdb-server/crates/core/src/registry.rs` | 506 | **Read-only.** `register_connection` (205) keys by `spec().name`; `get_entry` (229) uppercases. No alias table → the name-keyed match is total, which is the proof the `_` arm is dead |
| `frogdb-server/crates/core/src/pubsub.rs` | 1523 | **Read-only.** `PubSubConfirmation::to_response` (≈303–365): RESP3 → `Response::Push`, RESP2 → `Response::Array`. The shared encoder FM-TXN-043's Invariant names; unchanged by this proposal |
| `frogdb-server/crates/server/tests/integration_pubsub.rs` | — | The six FM-TXN-043 forcing tests (2047, 2079, 2110, 2155, 2217, 2318). One new frame-exact RESP3 assertion belongs here (H1); the framing *unit* tests belong in `frogdb-core` |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | **LOCKED.** FM-TXN-043 (542–553) + its deviations row (650) + the Scope line (10–15). Prose-only edits for the refactor; a row edit for H1 |

## Problem

### The census — every site that special-cases EXEC/MULTI framing

Derived by grepping `MULTI`, `EXEC`, `in_transaction`, `queued`, `Deferral`, `spec().mutation`
and `cmd_name ==` across `frogdb-server/crates`. Twelve regions match; four are in scope.

| # | Site | What it special-cases | Ruling |
|---|---|---|---|
| C1 | `transaction.rs:78-108` | Deferred connection command → dispatch shape, by `spec().mutation` | **IN SCOPE.** The duplicate of C2 |
| C2 | `dispatch.rs:201-218` | Main-path connection command → dispatch shape, by `spec().mutation` | **IN SCOPE.** Becomes the single home |
| C3 | `pubsub_conn_command.rs:965-1006` | Pub/sub-in-MULTI → EXEC framing, **by command name** | **IN SCOPE.** Deleted; replaced by the datum |
| C4 | `transaction_conn_command.rs:406-435` | `EXEC`/`MULTI`/`DISCARD`/`WATCH`/`UNWATCH` → transaction *control* | **OUT OF SCOPE.** Not framing — this routes the control verbs themselves. `ExecFraming` says nothing about it, and folding it in would need a second, unrelated datum. Owned by nobody this round |
| C5 | `guards.rs:513-537` (`try_queue_in_transaction`), `:544-630` (`queue_command`) | *Queue eligibility* — whether a command is queued at all | **OUT OF SCOPE, already datum-driven.** Decided from `spec()`, exactly as FM-TXN-043's Invariant requires. This is the half of the invariant that is already true |
| C6 | `guards.rs:964` (`fold_queued_batch`) | Batch key/slot folding | **OUT OF SCOPE.** Routing, not framing |
| C7 | `guards.rs:180-196` / `:198-215` | Pub/sub-mode and auth-exempt command lists | **ADJACENT.** Name-keyed, and genuinely the same *class* of smell — but they gate the non-transactional path and belong to a different datum (allow-lists, not frame counts). Explicitly not claimed |
| C8 | `txn/exec.rs:309-348` | Deferred partition + `result.extend(deferred_pushes)` | **READ-ONLY, LOCKED.** The consumer of the `(Response, Vec<Response>)` contract. Unchanged — and the second half of the H1 trace |
| C9 | `txn/host.rs:137-141` | `run_connection_level(name: &str, …)` | **READ-ONLY, LOCKED.** The *name* is what crosses the seam. Signature deliberately unchanged: narrowing it to a `&CommandSpec` would drag a `frogdb-core` type across an ADR-0002 boundary for no gain |
| C10 | `core/src/scripting/bindings.rs:10-25` | `is_forbidden_in_script` name table (`MULTI`/`EXEC`/`DISCARD`/`WATCH`) | **OUT OF SCOPE.** Script sandbox policy, different axis |
| C11 | `slot_migration/routing.rs:276-278`, `:348` | Literal `"EXEC"` as a *synthetic* command name for batch routing | **OUT OF SCOPE.** A placeholder name for a whole-batch route, not a real registry lookup |
| C12 | `replication/src/lib.rs:137-141`, `apply.rs:550`, `:571` | `MULTI`/`EXEC` **wire framing on the replication stream** | **OUT OF SCOPE, LOCKED crate.** Different protocol layer; unrelated to client EXEC framing |

Two near-misses worth naming so a reviewer does not re-find them: `dispatch.rs:319`
(`execute_reset`) and `dispatch.rs:450` (PreAuthIntercept) both read `spec().mutation`, but
each does so for one statically-known command to build its `ConnCtx` — they are *consumers* of
the datum, not framing dispatches, and they are unaffected.

### P1 — the duplicated match, already drifted

`dispatch.rs:201-218` and `transaction.rs:78-108` are the same three-way match over
`ConnMutation`, written twice, with the same comment written twice ("selects its dispatch
shape from its declared `mutation` capability … never from its string name"). They have
already diverged in the arm that matters least *today*:

```rust
// dispatch.rs:205-208 — main path
mutation @ (ConnMutation::None | ConnMutation::Auth | ConnMutation::Client) => {
    let mut ctx = self.conn_ctx_for(mutation);
    command.execute_multi(&mut ctx, args).await          // <-- Vec<Response>
}

// transaction.rs:98-105 — EXEC path
mutation @ (ConnMutation::None | ConnMutation::Auth | ConnMutation::Client) => (
    command.execute(&mut self.conn_ctx_for(mutation), args).await,   // <-- Response
    vec![],
),
```

**Ruling: LATENT, not live.** `execute_multi`'s default wraps `execute` in a one-element
`Vec` (`core/src/conn_command.rs:962-968`), and the *only* override in the tree is
`PubSubConnCommand::execute_multi` (`pubsub_conn_command.rs:874`). Pub/sub never reaches this
arm (`ConnMutation::PubSub` is matched first), so no frames are dropped today. The trap is
the second override: the first non-pub/sub connection command to emit multiple frames will
work on the main path and silently lose all but its first frame inside MULTI. There is no
test that would catch it, because there is no such command to test.

### P2 — the name-keyed EXEC framing

`exec_pubsub_in_transaction` (`pubsub_conn_command.rs:965-1006`) is a four-arm `match
cmd_name`. Its three real arms encode exactly three framing behaviours, and all three are
mechanical functions of "how many frames does this command produce":

| Arm | Commands | Behaviour |
|---|---|---|
| `"PUBLISH" \| "SPUBLISH" \| "PUBSUB"` | 3 | Take the **first** response as the EXEC slot; no pushes |
| `"SUBSCRIBE" \| "UNSUBSCRIBE" \| "PSUBSCRIBE" \| "PUNSUBSCRIBE" \| "SUNSUBSCRIBE"` | 5 | One confirmation **per channel**; the **last** is the EXEC slot; in RESP3 the rest ride out-of-band |
| `"SSUBSCRIBE"` | 1 | Refuse: `ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client` |
| `_` | 0 | `ERR command not supported inside MULTI` |

The `PubSubKind` enum at `:724-733` already *is* the per-command datum — it is threaded
through every one of the nine specs by `pubsub_spec()` at `:739` and consumed by
`execute_multi` at `:874`. It is private to the module, so the EXEC path in a *different*
module could not reach it and re-derived the same partition from strings. That is the
locality failure in one sentence: **the fact was declared once, in a place the second reader
could not see, so the second reader re-derived it from names.**

### P3 — the `_` arm is unreachable, and is a trap

Proof, four links:

1. `register_connection` (`registry.rs:205`) keys the map by `spec().name`; `get_entry`
   (`:229`) uppercases the lookup. There is **no alias table** — a command is reachable under
   exactly the name its spec declares.
2. `exec_pubsub_in_transaction` has exactly **one** caller: `transaction.rs:86`, inside the
   `ConnMutation::PubSub` arm.
3. `CommandSpec::validate()` rejects `mutation: PubSub` without
   `strategy: ConnectionLevel(PubSub)` and vice versa (`command_spec.rs:849`,
   `SpecError::ConnMutationStrategyMismatch`).
4. All nine commands carrying `mutation: PubSub` (`pubsub_conn_command.rs:764-800`, registered
   at `server/register.rs:120-139`) have an explicit name arm.

So the `_` arm is dead code. Issue 57 records it was "kept as a defensive fallback" — but the
defence is inverted: it does not catch a bug, it *manufactures* one. Add a tenth pub/sub
command tomorrow, and it queues fine (C5 is spec-driven), executes fine on the main path, and
answers `-ERR command not supported inside MULTI` inside a transaction. The compiler cannot
help, because `&str` is not an enum.

### P4 — the RESP3 duplicate confirmation (**LIVE**)

```rust
// pubsub_conn_command.rs:985-991
let responses = self.execute_pubsub(command, args).await;
if self.state.protocol_version.is_resp3() {
    let exec_result = responses.last().cloned().unwrap_or_else(Response::ok);
    (exec_result, responses)          // last element is BOTH the slot value AND a push
}
```

Trace to the socket, every hop verified:

1. `txn/exec.rs:318-320` — `let (response, pushes) = host.run_connection_level(…)`; `response`
   goes into `final_results` at the command's queued index, `pushes` into `deferred_pushes`.
2. `txn/exec.rs:346-348` — `let mut result = vec![Response::Array(final_results)];` then
   `result.extend(deferred_pushes);`.
3. `connection.rs:440-565` — the response write loop applies **no** push/array filtering and
   no dedup.
4. `frame_io.rs:41-53` (`narrow_to_wire`) and `protocol/src/response.rs:422` — a nested
   `Push` stays a `Push` in RESP3; nothing collapses it.

So a RESP3 client running `MULTI; SUBSCRIBE a b; EXEC` sees the confirmation for channel `b`
**twice**: once as the last element of the EXEC array, once as a trailing out-of-band Push.

**Ruling: LIVE, and unforced.** `test_subscribe_confirmation_in_multi_exec_resp3`
(`integration_pubsub.rs:2079-2107`) asserts `matches!(exec, Resp3Frame::Array { .. })` and
then reads *exactly one* further raw frame. It cannot count trailing frames, so it passes.

Whether the duplicate is *wrong* is a parity question this proposal does not presume to
answer — real Redis writes subscribe confirmations straight to the output buffer during the
EXEC loop, so its array element and its push frames are the same bytes, which is arguably
what FrogDB is imitating. But FM-TXN-043's **NOT observable** field explicitly forbids "the
RESP3 push being folded into the array", and emitting it in *both* places is not a shape that
row describes either way. That makes it a **spec-first ruling**, which is exactly why it is
**carved out** (H1) rather than folded into a refactor whose whole claim is behaviour
preservation.

### The ledger record being revisited

The chain, for the reviewer who asked for it:

- `.scratch/arch-deepening/proposals/03-conn-command-unification.md` — the round-8 proposal
  that introduced `ConnMutation`, with migration step 1 "Add the declarative field,
  default-preserving." (The same shape this proposal reuses.)
- Commit **`ab340681`** — "refactor(server): spec-declared `ConnMutation` replaces `cmd_name`
  dispatch matches", 92 files, +807/−211. Its message claims "the MULTI-deferred path … derive
  their builders the same way" — true of the *builder*, not of the framing, which is precisely
  the residue.
- `.scratch/arch-deepening/issues/open/04-retire-legacy-handlers.md:19-21` — "since EXEC is
  deliberately not a narrow-`ConnCtx` leaf". The deliberate-deferral statement.
- `.scratch/testing-improvements/issues/57-subscribe-in-multi-parity-residue.md` (pruned from
  disk; retrieved via `git show 693f6384:`) — the Redis-8.6.4 policy verification and the
  "kept as a defensive fallback" note about the `_` arm.

The seam the deferral was waiting on now exists: `CommandSpec` carries per-command dispatch
facts (`mutation`, `strategy`, `lookup`, `reindex`, `wakes`, …) and `validate()` already
cross-checks them. The framing fact is the same kind of fact.

## Proposed change

### The datum

In `frogdb-core` (`command_spec.rs`), next to `ConnMutation`:

```rust
/// How this command's wire frames are apportioned when it runs deferred inside
/// `MULTI`/`EXEC`: which frame becomes the EXEC-array slot value, and which
/// frames (if any) ride out-of-band after the EXEC reply.
///
/// Ignored outside EXEC — the main dispatch path flattens every frame onto the
/// wire in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecFraming {
    /// One frame; it is the EXEC slot value. Every command except the six below.
    #[default]
    Single,
    /// One frame per channel. The last is the EXEC slot value; in RESP3 the
    /// confirmations also travel out-of-band as Push frames.
    OnePerChannel,
    /// Refused inside MULTI. Redis's DENY_BLOCKING refusal, rendered from the
    /// command's own name — no per-command error string.
    DeniedInMulti,
}
```

`CommandSpec` gains `pub exec_framing: ExecFraming`. `validate()` gains one cross-check:
`exec_framing != Single` requires `strategy: ConnectionLevel(_)` — a shard command's frames
never reach this path, so a non-default there is a spec bug and should not compile past
validation.

### The framing function

A free function in `frogdb-core`, taking plain data and returning plain data:

```rust
pub fn frame_for_exec(
    framing: ExecFraming,
    name: &str,
    responses: Vec<Response>,
    protocol: ProtocolVersion,
) -> (Response, Vec<Response>)
```

- `Single` → `(responses.into_iter().next().unwrap_or_else(Response::ok), vec![])`
- `OnePerChannel` → last as slot; RESP3 also returns the push vec, RESP2 returns `vec![]`
- `DeniedInMulti` → `(Response::error(format!("ERR {name} isn't allowed for a DENY BLOCKING client")), vec![])`

Note that `DeniedInMulti` **renders** SSUBSCRIBE's error rather than storing it: upstream
Redis's message is the generic `"%s isn't allowed for a DENY BLOCKING client"`, so the name is
the only variable and no error text needs to live in a spec literal.

### The call sites

`transaction.rs:78-108` collapses to a call into the *existing* main-path dispatch plus the
framing function:

```rust
if let Some(command) = migrated {
    let responses = self.dispatch_connection_command(command, args).await;
    return frogdb_core::frame_for_exec(
        command.spec().exec_framing,
        command.spec().name,
        responses,
        self.state.protocol_version,
    );
}
// PSYNC tail (109-115) unchanged.
```

`dispatch_connection_command` (`dispatch.rs:188-218`) keeps its three-arm mutation match
verbatim and becomes the single home of it. `exec_pubsub_in_transaction` is deleted whole,
along with its four-arm name match. `pubsub_spec()` gains an `ExecFraming` parameter; six of
the nine call sites pass a non-default.

Two consequences fall out for free, without a behavioural decision:

- **P1 disappears by construction.** EXEC now goes through `execute_multi`, the same as the
  main path, because there is only one path.
- **P3 disappears by construction.** A tenth pub/sub command must declare `exec_framing`;
  omission gives it `Single`, which is a *defensible* default (one frame, that frame is the
  slot value) rather than a spurious refusal. There is no `_` arm to fall into.

### The alternative, weighed honestly

The cheaper shape is to make `PubSubKind` (`pubsub_conn_command.rs:724`) `pub(crate)` and have
`transaction.rs` match on it. That deletes the string match with ~zero churn and no
`CommandSpec` edit — no 400-literal sweep, no `frogdb-core` change.

It is rejected, but not by much:

- It keeps EXEC framing knowledge in the pub/sub module, so the *next* connection command
  family that needs multi-frame EXEC framing re-derives it again. That is the exact failure
  P2 documents, deferred one round.
- It cannot host the framing function in `frogdb-core`, so the unit tests stay in
  `frogdb-server` — which matters, because `cargo mutants -p <crate>` runs only that
  package's own tests.
- It leaves P1 (the duplicated mutation match) standing, since `transaction.rs` would still
  need its own dispatch to get at the `Vec<Response>`.

If the reviewer decides proposal 90 will not land this round and the 400-literal sweep is
unacceptable, the `PubSubKind` variant is the correct fallback and should be taken — it
captures P2's deletion and P3's fix, and forfeits only P1 and the test-locality win.

### What does not change

- **The `TxnHost` seam.** `run_connection_level(name: &str, …) -> (Response, Vec<Response>)`
  keeps its signature and its `&str`. `frogdb-txn` compiles untouched; ADR-0002's boundary is
  not crossed.
- **The wire, in every case except the one carved out.** `Single` reproduces the
  PUBLISH/SPUBLISH/PUBSUB arm exactly; `OnePerChannel` reproduces the subscribe arm exactly
  (**including the duplicate** — H1 is a separate commit); `DeniedInMulti` reproduces
  SSUBSCRIBE's byte-identical error.
- **`PubSubConfirmation`** (`core/src/pubsub.rs:303-365`) and the shared RESP2/RESP3 encoder.
  FM-TXN-043's Invariant clause about a shared encoder stays true and untouched.
- **Queue eligibility** (`guards.rs:513-630`), which was already spec-driven.
- **`PubSubKind`** stays private and keeps its `execute_multi` job. It answers "what does this
  command *do*"; `ExecFraming` answers "how many frames does it produce". Merging them would
  couple two unrelated axes.

### Deletion test, honestly

*Imagine `ExecFraming` gone. What reappears?*

- **Passes.** The four-arm `match cmd_name` reappears in `pubsub_conn_command.rs`, and with it
  the dead `_` arm and the silent-wrong-answer trap for command #10. The duplicated mutation
  match reappears in `transaction.rs`, with its `execute`/`execute_multi` drift. Both are
  complexity that lives *across* modules and is reconstructed from strings — the thing the
  datum abolishes.
- **Passes.** The framing rules become untestable without a live `ConnectionHandler`; today's
  only coverage is six `frogdb-server` integration tests, one of which (P4) demonstrably
  cannot see what it asserts.
- **Fails, partially.** `frame_for_exec` itself is thin — three arms, ~12 lines. If it had
  *one* caller it would be a pass-through and the abstraction would not earn its keep. It has
  one caller today. The keep is earned by the **enum**, not the function: the enum is what the
  400 specs are checked against and what makes omission a defensible default rather than a
  refusal. A reviewer who weights this differently should take the `PubSubKind` alternative.
- **Fails.** For 394 of 400 specs the field is pure ceremony. That is the standard cost of a
  declarative table, and the same trade `ConnMutation` made in round 8 (where 5 of 5 arms
  mattered but ~380 specs wrote `ConnMutation::None`). It is a real cost, not a zero.

## Testability improvement

| Today | After |
|---|---|
| Framing rules are only reachable through a live `ConnectionHandler` + socket; the sole coverage is six `frogdb-server` integration tests (`integration_pubsub.rs:2047-2318`) | `frame_for_exec` is a pure `(framing, name, Vec<Response>, protocol) -> (Response, Vec<Response>)`; every arm × both protocols is a table test in `frogdb-core`, microseconds, no I/O |
| Those six tests are `frogdb-server` tests, so they contribute **nothing** to `cargo mutants -p frogdb-txn` or `-p frogdb-core` | The framing unit tests live in the crate that owns the code, so mutants of the framing arms are killable in-package |
| The RESP3 test reads exactly one frame after the array → **cannot observe** a duplicate or a missing push (P4) | A frame-exact assertion is possible and cheap; H1 adds it |
| A tenth pub/sub command's MULTI behaviour is untested and silently wrong (`_` arm) | Omission yields `Single`; a `validate()` cross-check catches a non-default on a non-`ConnectionLevel` spec at spec-validation time |
| The `execute`/`execute_multi` drift (P1) is invisible — no command exercises it | One dispatch path; the drift is structurally impossible |

## Spec / LOCKED impact

- **No locked-crate source is edited.** `frogdb-txn` (`exec.rs`, `host.rs`) and `frogdb-vll`
  are read-only here. `frogdb-core`, `frogdb-server`, `frogdb-commands` are outside the four
  locked pairs (ADRs 0002–0004).
- **FM-TXN-043 prose is stale after this refactor, and the edit is same-commit.** The
  Invariant reads: *"Queue eligibility is decided by the command's spec, not by an ad-hoc
  list; the subscribe confirmation encoder is shared with the non-transactional path."* Half
  is already true (C5) and half stays true (`PubSubConfirmation`), but neither clause covers
  *framing*, which is what this proposal moves onto the spec. Add a clause naming
  `exec_framing`. **This is prose only** — `failure-modes.py` parses only backticked
  `Forced-by` test names, so no forcing test is added, removed, or renamed by the refactor and
  `just lint-failure-modes` cannot detect the staleness. It must be done by hand.
- **The spec's Scope line (`txn-failure-modes.md:10-15`) is already wrong today.** It names
  `connection/{dispatch,guards,transaction,transaction_conn_command,state}.rs` and **omits
  `pubsub_conn_command.rs`**, even though FM-TXN-043's behaviour is forced entirely from that
  file. Fix in the same commit (or as hotfix H3 — it is independently landable).
- **No mutation re-gate is owed by the refactor.** The txn gate (0.90) measures
  `frogdb-txn` + `frogdb-vll`, neither of which changes. Adding `frogdb-core` unit tests can
  only help `frogdb-core`, which is ungated.
- **H1 is spec-first and is not part of this refactor.** It changes what a RESP3 client sees,
  against a LOCKED row whose *NOT observable* field speaks to the adjacent case. Sequence:
  amend FM-TXN-043's Observable/NOT-observable → write the failing frame-exact test → fix the
  code. Its forcing test name goes in the `Forced by` list, which *is* lint-visible.

## Risks / scope boundaries vs sibling proposals

**Proposal 90 (CT2) — `CommandSpec` DEFAULT const. This is the load-bearing edge, and it is
leverage, not just conflict.** Adding a field to `CommandSpec` today requires editing all
**400** literals (296 `frogdb-commands`, 72 `frogdb-server`, 32 `frogdb-core`) — the
`ab340681` precedent for exactly this move was 92 files / +807/−211. If 90's
`..CommandSpec::DEFAULT` struct-update sweep lands first, this proposal's spec cost is **six**
edits and one field declaration. **Recommendation: 90 → 68, strictly.** If 68 lands first,
90's sweep must re-touch every literal 68 wrote and the diffs collide across 400 sites. If 90
is dropped from the round, take the `PubSubKind` alternative above rather than paying 400
edits for one field. 90 does not exist on disk at this HEAD, so its exact shape is
**unverifiable from this side** — the edge is stated from the candidate description and
should be re-checked once 90 is written.

**Proposal 50 (transaction state consolidation) — pre-negotiated, and 68 assents.** 50 wrote,
at its `:38` and `:402`, that "Sibling 68 owns `exec_pubsub_in_transaction` (L965)" and
proposed a file-ownership rule it flagged as needing 68's assent (68 did not exist on disk
when 50 was written). **68 assents to 50's rule as written**, and states the resulting line
split, all three verified disjoint at `3dd9f1df`:

| File | 50 owns | 68 owns |
|---|---|---|
| `connection/transaction.rs` | `:40` (`take_transaction()`) | `:78-108` (the mutation match) |
| `connection/dispatch.rs` | `:477`, `:482` | `:188-218` |
| `connection/pubsub_conn_command.rs` | `:343` | `:965-1006` |

Three shared files, zero shared lines. Either order works; a merge conflict would be textual,
not semantic.

**Proposal 67 (server small dedups) — disjoint, verified against the file on disk.** 67's ten
touched files are `connection/builder.rs`, `connection.rs`, `connection/deps.rs`,
`commands/search.rs`, `commands/timeseries.rs`, `commands/migrate_cmd.rs`, `commands/server.rs`,
`core/src/command.rs`, `connection/search/{helpers,index_mgmt,explain,synonyms}.rs`. **No
overlap with 68's file set.** One near-edge: 67 adds a `frogdb-core` item (its refusal function
+ macro) and 68 adds a `frogdb-core` item (`ExecFraming` + `frame_for_exec`) — different
modules (`command.rs` vs `command_spec.rs`), no conflict. 67 also touches
`frogdb-commands`/`frogdb-server` `CommandSpec`-adjacent code but **not the literals**, so it
does not collide with 90's sweep either.

**Proposals 69 (config combinators) and 70 (ACL registry) — not on disk at this HEAD;
boundary stated from the candidate descriptions and flagged unverifiable.** 69 is expected in
`frogdb-config`/`frogdb-server` config plumbing and 70 in the ACL/registry surface. The one
place 70 could reach 68 is `core/src/registry.rs` — 68 touches it **read-only** (evidence for
the dead-arm proof), so even a full rewrite there costs 68 nothing but a re-verified citation.
If 70 adds a `CommandSpec` field of its own, it inherits the same 90-ordering constraint 68
does, and the two should land on the same side of 90.

**Behavioural risk of the refactor itself.** The one real hazard is that `Single` and
`OnePerChannel` must reproduce today's `.next()`/`.last()` selection exactly, including the
`unwrap_or_else(Response::ok)` fallbacks, and that `OnePerChannel` must reproduce the RESP3
duplicate **on purpose** until H1 lands. Mitigation: land the refactor with the duplicate
intact and the six FM-TXN-043 tests green, then land H1 separately with its own spec row and
its own frame-exact test. Reviewers should reject any version of this refactor that "fixes"
P4 silently in passing — a behaviour change hidden inside a claimed-neutral refactor against a
LOCKED row is the worst available outcome.

## Effort

**M** — the lane estimate holds, but it is bimodal on the 90 ordering.

| Part | Size |
|---|---|
| `ExecFraming` enum + field + `validate()` cross-check + unit tests (`frogdb-core`) | S |
| Spec literal churn | **~0 if 90 lands first; L (400 literals) if not** |
| Delete `exec_pubsub_in_transaction`, thread `ExecFraming` through `pubsub_spec()` ×9 | S |
| Rewrite `execute_connection_level_in_transaction` to call `dispatch_connection_command` | S |
| FM-TXN-043 Invariant prose + Scope-line refresh | S |

Behind 90: solidly **M**. Ahead of 90 or without it: **L**, and the `PubSubKind` alternative
(**S**) becomes the better trade.

## Independently-landable hotfixes

Carved **out** of the refactor. Each stands alone at today's HEAD and does not depend on
`ExecFraming`.

1. **H1 — RESP3 subscribe-in-MULTI emits the last confirmation twice. LIVE, unforced,
   spec-first.** `pubsub_conn_command.rs:988-989` returns
   `(responses.last().cloned(), responses)`; `txn/exec.rs:348` appends the whole vec after the
   array that already holds its last element. Traced to the socket with no filtering at any
   hop. `test_subscribe_confirmation_in_multi_exec_resp3` (`integration_pubsub.rs:2079-2107`)
   reads exactly one frame after the array and cannot see it. **This needs a parity ruling
   before a fix** — real Redis writes confirmations into the output buffer during the EXEC
   loop, so "the array element and the push are the same bytes" may be the intended shape, in
   which case the bug is that FrogDB writes them *twice* rather than *once shared*. Either
   way: amend FM-TXN-043's Observable / NOT-observable fields first, then add a frame-exact
   RESP3 test, then change code. **Do not fold into the refactor.**
2. **H2 — delete the dead `_` arm** (`pubsub_conn_command.rs:1000-1003`). Unreachable by the
   four-link proof in P3. Zero behaviour change today; removes a future silent-wrong-answer
   trap. If a reviewer wants belt-and-braces, replace it with an
   `unreachable!("pub/sub command {cmd_name} has no EXEC framing arm")` so command #10 fails
   loudly in tests instead of answering wrongly in production. **No spec edit** — the arm
   appears in no `Observable` field. Trivially landable now; superseded (not blocked) by the
   refactor.
3. **H3 — the txn spec Scope line omits `pubsub_conn_command.rs`**
   (`txn-failure-modes.md:10-15`), despite FM-TXN-043 being forced entirely from behaviour in
   that file. Documentation-only, one line, lint-invisible (`failure-modes.py` parses only
   backticked `Forced-by` names). Land any time.
4. **H4 (optional, S) — pin the `execute`/`execute_multi` divergence with a test rather than
   waiting for the refactor.** A test-only `ConnectionCommand` that overrides `execute_multi`
   with two frames, registered in a test registry and run inside MULTI, fails today (P1) and
   passes after the refactor. It is the only way to make P1 observable before it becomes
   impossible. Lives in `frogdb-server`, so it earns no mutation-gate credit — file it as a
   regression test, not a gate contribution.
