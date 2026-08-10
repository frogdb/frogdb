# Proposal 47 — one EVAL/EVALSHA orchestration; one owner for shard-transport reply strings

*Round 38. Candidates TX4 + TX5 of the txn+vll+scripting lane. One proposal because both land in
`eval.rs` and the second is only cheap once the first has collapsed the two copies: TX5's mapping
has three call sites today and one after TX4.*

## Summary

`handle_eval` (`eval.rs:19-63`) and `handle_evalsha` (`eval.rs:172-216`) are 45-line bodies that
differ in **five lines**, two of which are a binding rename — there is no orchestration difference
between them beyond a two-variant tag that already exists (`EvalKind`, `eval.rs:230-233`). TX4
collapses them to one `handle_eval_family(ScriptSource, args, read_only)`. TX5 fixes what the
duplication was hiding: the two copies of the *same* shard-transport failure map to two different
client-visible strings — the single-shard path answers `ERR shard dropped request` (`eval.rs:111`,
matching `frogdb-txn`'s `exec.rs:382` and 12 other sites) while the cross-shard path answers
`ERR script execution failed` (`eval.rs:159`), a string that exists **nowhere else in the
repository** and is asserted by nothing. Same shard death, same command, different answer depending
on whether the caller's keys happened to land on one shard. TX5 gives both a single owner — a
plain-data `ShardChannelFailure` in `frogdb-types`, modelled directly on the `redirect.rs`
precedent — and leaves the connection-side adapter in the server crate, per
[ADR-0002](../../../adr/0002-txn-orchestration-behind-txnhost-seam.md).

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/server/src/connection/scripting/eval.rs` | 281 | **Owner of both candidates.** `handle_eval` (L19-63), `classify_script_shards` (L65-84), `execute_single_shard_script` (L87-113, transport map at L105-112), `execute_cross_shard_script` (L119-169, transport map at L150-160), `handle_evalsha` (L172-216), `ScriptShards` (L219-227), `EvalKind` (L230-233) + `impl`/`into_message` (L235-266). **Not owned: `continuation_error_to_response` (L268-281) — sibling 46.** |
| `frogdb-server/crates/types/src/redirect.rs` | 159 | The **precedent**, not an edit. `pub const CROSSSLOT_MSG` (L17) + builder fns; its header comment (L1-9) states exactly the argument TX5 makes. Reachable everywhere via `frogdb-core`'s blanket `pub use frogdb_types::*` (`core/src/lib.rs:7`). |
| `frogdb-server/crates/types/src/shard_reply.rs` | *new, ~40* | TX5's new module: `ShardChannelFailure` + the two message consts. Unlocked crate. |
| `frogdb-server/crates/types/src/lib.rs` | — | One added line: `pub mod shard_reply;`, alphabetically between `pub mod redis_version;` (L20) and `pub mod skiplist;` (L21). |
| `frogdb-server/crates/txn/src/exec.rs` | 412 | `run_shard_transaction` (L357-385); the two literals at **L378** (`"ERR shard unavailable"`) and **L382** (`"ERR shard dropped request"`) become calls. **LOCKED crate** — the only locked edit in this proposal, two lines. |
| `frogdb-server/crates/txn/src/host.rs` | 145 | `ShardTxnReply` (L40-48) and its doc comment (L35-39: *"the algorithm — not the host — owns the mapping onto wire replies"*) — the design intent TX5 completes. Read-only under the recommended variant. |
| `frogdb-server/crates/txn/tests/exec_outcomes.rs` | — | `error_when_the_shard_channel_is_closed_or_the_request_is_dropped` (L397-403, `// FM-TXN-032`) — pins both strings. Unchanged under the recommended variant. |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | **FM-TXN-032** (L410-419): Observable (L415) pins both strings; Invariant (L416) names `ShardTxnReply::Unavailable` / `::Dropped` verbatim. Scope (L10-15) covers `frogdb-txn` + `connection/{dispatch,guards,transaction,transaction_conn_command,state}.rs` + `slot_migration/` — **`connection/scripting/` is not in it.** |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | Scope (L13-17) names `eval.rs`'s `continuation_error_to_response` as the VLL acquire-error mapping's home. **Sibling 46's row, not this one's** — TX5 must not move or absorb that function. |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | `handle_fcall` (L14-73) carries the same transport pair at **L66/L71**. **Ceded to sibling 48** — see boundaries. |
| `frogdb-server/crates/server/src/connection/scripting/script.rs` | — | `handle_script` (L12) — the **existing** SCRIPT command handler. Source of the name collision noted under TX4. |
| `frogdb-server/crates/server/src/vll_adapter.rs` | — | `MetricsRecorderSink` (L162-173) — the target of the separately-landing H7/TX3 hotfix that replaces `NoopMetricsSink` at `eval.rs:131`. Not edited here; see Risks. |

## Problem (verified)

### 1. The two handlers differ in nothing that matters

Aligning `eval.rs:19-63` against `eval.rs:172-216` (45 lines each), the complete set of differences:

| `handle_eval` | `handle_evalsha` | Kind |
| --- | --- | --- |
| L19 `fn handle_eval` | L172 `fn handle_evalsha` | name |
| L22 `…for 'eval' command` | L175 `…for 'evalsha' command` | string (dead — see below) |
| L25 `let script_source = args[0].clone();` | L178 `let script_sha = args[0].clone();` | **binding rename only** |
| L43 `EvalKind::Source(script_source)` | L196 `EvalKind::Sha(script_sha)` | tag |
| L53 `EvalKind::Source(script_source)` | L206 `EvalKind::Sha(script_sha)` | tag |

**40 of 45 lines are byte-identical**, and there is *no* difference beyond source-vs-sha resolution:
the numkeys parse (L26-31 / L179-184), the `2 + numkeys` bound check (L33-35 / L186-188), the
keys/argv split (L37-38 / L190-191), the three-arm classification match, the `read_only` threading,
and the `CrossSlotForbidden → redirect::crossslot()` arm are all the same text.

`EvalKind` (L230-266) is already the abstraction — it is threaded through both private helpers and
`into_message` handles the `ScriptingMsg::EvalScript` vs `EvalScriptSha` split in one place. The
duplication is purely the *orchestration around it*, which nobody hoisted.

**The `args.len() < 2` guard in both bodies is dead code, not a live compat bug** (verified; an
earlier draft of this proposal claimed otherwise and was wrong). `DispatchStage::CommandLookup`
(`dispatch.rs:551-558`) runs `command_lookup_check` (`guards.rs:490-503`) *before*
`DispatchStage::ConnectionCommand` (`dispatch.rs:60`), which is where the scripting family is
dispatched. That check renders the **invoked** name lowercase — `entry.name().to_ascii_lowercase()`
(`guards.rs:501`) — and `EVAL_RO_SPEC.arity = Arity::AtLeast(2)`
(`scripting_conn_command.rs:121`; `EVAL_RO_CONN_COMMAND` is registered at `server/register.rs:60`,
and `Registry::get_entry` uppercases the lookup key, `registry.rs:229-231`). `Arity` counts
arguments *excluding* the command name (`command.rs:878-887`: "Exactly N arguments (e.g., GET =
`Fixed(1)`)"), so `EVAL_RO <script>` is rejected upstream with
`ERR wrong number of arguments for 'eval_ro' command` — **FrogDB already matches Redis.** The
`args.len() < 2` arms at `eval.rs:21-23` / `eval.rs:174-176` are unreachable, and so is
`handle_fcall`'s `cmd_name` (`function.rs:15`, used only at `function.rs:20`; `FCALL_SPEC` /
`FCALL_RO_SPEC` are `AtLeast(2)` at `scripting_conn_command.rs:292` / `:334`). The MULTI path is
covered too: `queue_command` (`guards.rs:562-570`) rejects wrong arity at queue time, so the handler
never sees a short arg list there either.

This makes the guards an **optional dead-guard removal**, not a fix: TX4 may keep one copy of the
check as a belt-and-braces assert or drop it. Either way there is **no client-visible change**, so
no CHANGELOG entry and no regression-suite confirmation is required.

**Adjacent real bug, deliberately out of scope.** `guards.rs:566` formats `entry.name()` **without**
`.to_ascii_lowercase()`, so a wrong-arity command *inside `MULTI`* replies with the uppercase spec
name (`ERR wrong number of arguments for 'EVAL_RO' command`, `'GET'`, `'SMISMEMBER'`, …) where Redis
replies with the lowercase fullname — the two sibling sites, `guards.rs:501` and `routing.rs:52`,
both lowercase. It is unpinned: FM-TXN-006's Observable elides the name
(`txn-failure-modes.md:103`) and its forcing test asserts a prefix only
(`integration_transactions.rs:591-593`, `starts_with(b"ERR wrong number of arguments")`). Queued as
**hotfix HF-J**, in a file 47 does not own — recorded here, not scoped in.

**Depth reading.** The module presents two ~45-line entry points whose interface leverage over each
other is zero: a reader must diff them to learn they are the same algorithm, and a change to the
argument grammar has to be made twice or it silently diverges. The deletion test applies to the
*copy*: deleting `handle_evalsha`'s body costs nothing, because its whole content reappears from one
enum arm.

### 2. Same failure, two strings — decided by key layout

Both script paths do the identical two-step: send a `ScriptingMsg` down a shard channel, then await
a reply oneshot. Both steps can fail, and both failures are mapped inline, twice, with one string
out of family:

```rust
// eval.rs:105-112 — SINGLE-shard path
if self.core.shard_senders[shard_id].send(msg).await.is_err() {
    return Response::error("ERR shard unavailable");
}
match response_rx.await {
    Ok(response) => response,
    Err(_) => Response::error("ERR shard dropped request"),   // ← the family string
}

// eval.rs:150-160 — CROSS-shard path, inside the continuation-lock closure
if self.core.shard_senders[primary_shard].send(msg).await.is_err() {
    return Response::error("ERR shard unavailable");          // ← same
}
match response_rx.await {
    Ok(resp) => resp,
    Err(_) => Response::error("ERR script execution failed"), // ← DIFFERENT, for no reason
}
```

The dropped-reply string, repo-wide (non-test source, verified by grep):

| String | Sites | Where |
| --- | --- | --- |
| `ERR shard dropped request` | **14** | `txn/src/exec.rs:382`, `routing.rs:348`, `scripting/eval.rs:111`, `scripting/function.rs:71`, `search/explain.rs:59`, `search/helpers.rs:72`, `search/synonyms.rs:84`, `search/index_mgmt.rs:105,135`, `pubsub_conn_command.rs:526`, `transaction_conn_command.rs:315`, `info/mod.rs:843`, `scatter/broadcast.rs:226,265` |
| `ERR source/destination shard dropped request` | 2 | `routing.rs:255,297` (deliberately scoped variants) |
| **`ERR script execution failed`** | **1** | **`scripting/eval.rs:159`** |

`ERR shard unavailable` is spelled inline **19** times across the same files. Neither string has a
single owner; `eval.rs:159` is what a 15-way copy-paste eventually produces.

**The divergent arm is reachable, not theoretical.** `dispatch_scripting.rs:20-56` sends the reply
with `let _ = response_tx.send(response);` *after* awaiting `handle_eval_script` /
`handle_evalsha`. An unwind inside the handler (the failure class FM-VLL-005 exists to survive) or a
shard-worker shutdown between accept and reply drops the sender, and the client gets whichever of
the two strings its key layout selected. The cross-shard arm additionally needs
`allow_cross_slot_standalone = true` (`config/src/server.rs:38-41`; default `false`,
`server.rs:108-110`) or cluster mode — an opt-in path, which explains why nobody noticed, not why
it is acceptable.

**Nothing pins the odd string.** `grep "script execution failed"` across the whole tree returns
`eval.rs:159` and one unrelated doc comment (`types/src/metrics/labels.rs:80`). Zero tests, zero
spec rows, zero website pages, zero regression cases.

### 3. The type that should own the mapping already exists — and says so

`frogdb-txn` models exactly these two failures as a type, with a doc comment that states the
intended ownership (`host.rs:35-39`):

> The outcome of one EXEC shard round-trip, as the host observed it. Both channel failures are
> named separately from the shard's own `TransactionResult` so the algorithm — not the host — owns
> the mapping onto wire replies.

```rust
pub enum ShardTxnReply { Replied(TransactionResult), Unavailable, Dropped }   // host.rs:41-48
```

The server produces the variants (`connection/transaction.rs:222,226-227` — the adapter), and
`run_shard_transaction` maps them (`exec.rs:376-383` — the mapping). That split is right and is what
FM-TXN-032 pins. The defect is that the *mapping half* still spells its strings as inline literals
inside `frogdb-txn`, so no crate outside the transaction path can reuse them — leaving the scripting,
routing, search, pub/sub, INFO, and scatter paths to respell them, 33 times, one of them wrong.

### 4. Spec status: **not** spec-first (verified, both directions)

- `ERR script execution failed` — no FM row anywhere. Changing it edits no spec.
- `ERR shard dropped request` / `ERR shard unavailable` — pinned by **FM-TXN-032**
  (`txn-failure-modes.md:415-416`), Observable *and* Invariant, forced by
  `error_when_the_shard_channel_is_closed_or_the_request_is_dropped` (`exec_outcomes.rs:397-403`).
  That row's scope (`txn-failure-modes.md:10-15`) is the **connection-side transaction path**;
  `connection/scripting/` is explicitly not listed. So the row does not govern `eval.rs` today — but
  it does fix the target string: the unification must adopt `ERR shard dropped request`, not invent
  a third spelling.
- The vll spec's scope (`vll-failure-modes.md:13-17`) names `eval.rs`'s
  `continuation_error_to_response` as the home of the VLL *acquire*-error mapping. TX5 leaves that
  function exactly where it is, so that sentence stays true.

**Conclusion: no spec row is edited.** `just lint-failure-modes` should be a no-op — run it anyway
(it is part of `just lint`).

## Proposed change

### TX4 — one orchestration, parameterised by a fieldless source tag

```rust
/// Which cached form the script arrives in. Fieldless: the tag is chosen by the
/// entry point, *before* the payload has been parsed out of `args`.
#[derive(Clone, Copy)]
enum ScriptSource { Body, Sha }

impl ScriptSource {
    fn into_message(self, script: Bytes, keys, argv, conn_id, protocol_version,
                    read_only, response_tx) -> ScriptingMsg { … }   // was EvalKind::into_message
}

impl ConnectionHandler {
    pub(crate) async fn handle_eval(&self, args: &[Bytes], read_only: bool) -> Response {
        self.handle_eval_family(ScriptSource::Body, args, read_only).await
    }
    pub(crate) async fn handle_evalsha(&self, args: &[Bytes], read_only: bool) -> Response {
        self.handle_eval_family(ScriptSource::Sha, args, read_only).await
    }

    /// The whole EVAL-family orchestration, once: numkeys, key/argv split,
    /// shard classification, single-vs-cross-shard dispatch.
    async fn handle_eval_family(&self, source: ScriptSource, args: &[Bytes],
                                read_only: bool) -> Response { /* the 45 lines, once */ }
}
```

`EvalKind` is deleted and its `into_message` moves onto `ScriptSource`; the script `Bytes` becomes a
sibling argument to the two private helpers (`execute_single_shard_script` /
`execute_cross_shard_script` take `(source: ScriptSource, script: Bytes)` instead of
`kind: EvalKind`). `ScriptShards` and `classify_script_shards` are untouched — but their caller count
drops from 2 to 1, which is what makes sibling 48's job smaller. The dead `args.len() < 2` guards
collapse to one copy or none (Problem §1); either is a no-op on the wire.

**Name note:** the lane suggested `handle_script(ScriptSource::…)`. That name is **taken** —
`scripting/script.rs:12` is the SCRIPT LOAD/EXISTS/FLUSH handler and is another `impl
ConnectionHandler` method, so the collision is a compile error, not a style question. Use
`handle_eval_family`.

### TX5 — one owner for the transport strings; the adapter stays server-side

New module `frogdb-server/crates/types/src/shard_reply.rs`, modelled line-for-line on
`frogdb-types/src/redirect.rs`:

```rust
//! The single owner of the "shard channel failed" wire replies.

pub const SHARD_UNAVAILABLE_MSG: &str = "ERR shard unavailable";
pub const SHARD_DROPPED_MSG:     &str = "ERR shard dropped request";

/// How a shard round-trip failed, as the caller observed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardChannelFailure {
    /// The send failed: the channel is closed, the request was never delivered.
    Unavailable,
    /// The shard took the request and dropped the reply without answering.
    Dropped,
}

impl ShardChannelFailure {
    pub fn to_response(self) -> Response { … }   // the two strings, once
}
```

**Placement is free.** `frogdb-core/src/lib.rs:7` is `pub use frogdb_types::*`, so this is reachable
as `frogdb_core::shard_reply` from both `frogdb-txn` (which depends on `frogdb-core`) and
`frogdb-server` — **no `Cargo.toml` edits, no new dependency edges.** This is precisely how
`redirect` already reaches both crates (`server/src/slot_migration/redirect.rs` is a nine-line
re-export of `frogdb_core::redirect`).

**The adapter stays server-side, per ADR-0002.** ADR-0002 puts "everything touching connection
dispatch … server-side" and prescribes "plain-data signatures" across the seam. `ShardChannelFailure
→ Response` is pure plain data (no host, no trait, no `&self`), so it may live below the server; the
thing that *produces* the variant is connection dispatch and stays put:

```rust
// frogdb-server/crates/server/src/connection/shard_call.rs — server-side adapter
pub(crate) async fn await_shard_reply<M, R>(
    sender: &mpsc::Sender<M>, msg: M, rx: oneshot::Receiver<R>,
) -> Result<R, ShardChannelFailure>
```

Call sites converted **in this proposal** (5):

| Site | Before | After |
| --- | --- | --- |
| `eval.rs:105-112` | 8 lines, two literals | `await_shard_reply(…).unwrap_or_else(ShardChannelFailure::to_response)` |
| `eval.rs:150-160` | 10 lines, `"…script execution failed"` | same helper — **the divergence disappears structurally** |
| `txn/src/exec.rs:378` | `Response::error("ERR shard unavailable")` | `ShardChannelFailure::Unavailable.to_response()` |
| `txn/src/exec.rs:382` | `Response::error("ERR shard dropped request")` | `ShardChannelFailure::Dropped.to_response()` |
| `info/mod.rs:1110`, `broadcast.rs:1103` (test predicates) | `e.as_ref() == b"ERR shard dropped request"` | compare against `SHARD_DROPPED_MSG` |

`ShardTxnReply` keeps its variant names — FM-TXN-032's Invariant names them verbatim, and delegating
the two literals changes nothing observable, so **no spec edit and no test edit**.

**Rejected (deferred) alternative:** collapse the enum to `ShardTxnReply::{Replied,
Failed(ShardChannelFailure)}`. Structurally tidier, but it renames the two variants FM-TXN-032's
Invariant cites by name — making it spec-first (edit `txn-failure-modes.md:416`, update
`exec_outcomes.rs:401-402`, then the code) for zero behavioural gain. Not worth it in this round.

**Not converted here (~14 remaining sites):** `routing.rs`, `search/*`, `pubsub_conn_command.rs`,
`transaction_conn_command.rs`, `info/mod.rs`, `scatter/broadcast.rs`, `scatter/executor.rs`,
`guards.rs`, and `function.rs` (sibling 48). Deliberate: this proposal proves the seam on the two
paths the lane audited. The burn-down belongs in a follow-up issue, ideally as a
`lint-shard-reply-seam` gate following `agents/seam-lints.md` §"Adding a new rule" — invariant
("every shard-transport failure reply is built through `ShardChannelFailure`"), a compile-free grep
predicate, and the count-pinned per-file allowlist ratchet that `lint-redirect-seam` and
`lint-clock-seam` already use. Filed as a follow-up, **not proposed for this round** — a gate should
land after its chokepoint has one real consumer, not with it.

## Testability improvement

**There are no tests to break.** Verified: `grep "mod tests"` across
`server/src/connection/scripting/` returns **nothing** — `eval.rs`, `function.rs`, and `script.rs`
have zero unit tests, and neither `handle_eval` nor `handle_evalsha` has a direct test anywhere.

- **TX4 halves the untested surface and makes future coverage transitive.** Today a test of the
  EVAL orchestration proves nothing about EVALSHA: they are separate 45-line copies that can drift
  silently. After the dedup, one test of `handle_eval_family` covers both families, and a third
  source form is one enum arm plus one parameterised case rather than a third copy.
- **TX5 moves the transport mapping into a crate where it is a sub-second unit test.** Today the
  scripting path's strings are asserted **nowhere**; the only assertions on either string are
  `exec_outcomes.rs:399-403` (the EXEC path, via `MockTxnHost`) and two `matches!` predicates in
  `info/mod.rs:1110` / `broadcast.rs:1103`. After: `frogdb-types` pins both strings directly, in the
  same shape `redirect.rs` already pins `CROSSSLOT_MSG`, and every converted caller inherits the
  pin. Forcing the scripting arms otherwise requires killing a shard worker mid-EVAL through a live
  integration harness.
- **The divergence class becomes unrepresentable, not merely tested.** After TX5 there is no place
  in `eval.rs` where a transport-failure string can be spelled, so a future copy-paste cannot
  reintroduce a third spelling — the same argument the `redirect.rs` seam makes for MOVED/ASK.

## Risks / scope boundaries vs siblings

### Hotfix drift — the implementation must be written against post-hotfix `eval.rs`

Two hotfixes land on `main` **before** this proposal, both inside `eval.rs`:

- **H7/TX3** (VLL continuation-acquisition observability) replaces `NoopMetricsSink` at
  `eval.rs:131` with `MetricsRecorderSink` (`vll_adapter.rs:162-173`), possibly also hoisting a
  `finish(outcome)` around the `eval.rs:165-168` outcome match. Both are inside
  `execute_cross_shard_script`, whose body this proposal does **not** rewrite — only its
  `kind: EvalKind` parameter changes and its `Err(_)` arm (L159) moves to the shared helper. Expect
  a 1-3 line textual conflict at `eval.rs:130-131` and possibly `165-168`.
- **HF-H** (sibling 46's independently-landable hotfix) adds the missing `ShardBusy` arm to
  `continuation_error_to_response`, at `eval.rs:271-273` (the `LockFailed` arm). That range is
  **disjoint from every region 47 edits** — same file, no overlapping lines — so it is a line-number
  shift, not a conflict.

**Rebase onto `main` after both and re-read the functions before editing; do not write the diff
against the `NoopMetricsSink` text in this document.** Every `eval.rs` line number cited above is
pre-hotfix and will have shifted.

### File ownership

| Proposal | Owns in `eval.rs` / elsewhere | Overlap with 47 |
| --- | --- | --- |
| **47** (this) | `eval.rs` **L19-63, L87-113, L119-169 (transport arms only), L172-216, L230-266**; new `types/src/shard_reply.rs` + its `lib.rs` module line; new `server/src/connection/shard_call.rs`; `txn/src/exec.rs:376-383` | — |
| **45** vll-key-ownership-diagnostics | `vll/src/shard.rs`, `vll/src/lock_table.rs`, `core/src/shard/{vll,diagnostics}.rs` | **None.** Disjoint crates and files. Any order. |
| **46** vll-acquire-error-unify | `eval.rs` **L268-281** (`continuation_error_to_response`), `vll/src/types.rs`, `scatter/executor.rs:149-157` | **Same file, disjoint line ranges.** Textual-only; git merges cleanly if 47 does not reflow L268-281 and 46 does not touch L19-266. **Semantic line, stated as a rule below.** |
| **48** fcall-cross-shard | `scripting/function.rs` (whole `handle_fcall`, L14-73), the keyless-shard policy, script classification/dispatch; will add a caller to `classify_script_shards` (`eval.rs:65-84`) | **Ceded, not conflicting.** See below. |

**Land order for the whole lane: HF-H + H7/TX3 → 46 → 47 → 48.** 46's own boundary section demands
`46 → 47` (both change reply strings in one file, and 46's spec diff is the constrained one); 47 →
48 is argued below.

**The 46/47 line: acquire errors vs transport errors.**

- **46 owns the VLL *acquire* outcome** — `ContinuationError::{ShardUnavailable, LockFailed,
  LockChannelClosed, LockTimeout}`: the lock was refused, timed out, or its request channel died.
  Its home is named in the vll spec's scope (`vll-failure-modes.md:13-17`) and 46 plans to move it
  to a `to_response` in `frogdb-vll`.
- **47 owns the *transport* failure of the work request itself** — the `ScriptingMsg` send and the
  reply oneshot, i.e. `eval.rs:105-112` and `eval.rs:150-160`.

They intersect in **string space** but never in **code space**: both can produce
`ERR shard unavailable` (46 via `ContinuationError::ShardUnavailable` at `eval.rs:270`, 47 via a
failed `shard_senders[..].send`), and that is correct — a failed lock-request send and a failed
work-request send *are* the same condition. If 46 moves its mapping into `frogdb-vll`, it should
consume `shard_reply::SHARD_UNAVAILABLE_MSG` rather than respell the literal. **Note for 46:**
`frogdb-vll/Cargo.toml` depends only on `frogdb-protocol` among the frogdb crates (verified), so
that reuse needs a `frogdb-types` dependency added — legal (no cycle) but 46's call, not a blocker
for 47. If 46 declines, the literal stays double-sourced between two crates and the follow-up seam
lint should record it as a named-gap entry.

**The 47/48 line: `function.rs` is 48's, but `ScriptSource`'s shape is 47's.**

48 rewrites `handle_fcall` end-to-end to route through `classify_script_shards` + a single
keyless-shard policy. 47 therefore does **not** convert `function.rs:66/71` to
`ShardChannelFailure`, even though they are the same pair — that conversion rides with 48 and costs
it two lines. **Land 47 first:** it drops `classify_script_shards`'s caller count from 2 to 1, so 48
generalises one call site instead of two, and inherits `handle_eval_family` as the shape FCALL
converges onto.

**The contract 48 must adopt (48's current text does not compile against 47):**

- `ScriptSource` is **fieldless** — `enum ScriptSource { Body, Sha }`, and 48 adds a fieldless
  `Function` variant, *not* `Function(Bytes)`.
- The payload `Bytes` is a **sibling argument**, taken from `args[0]` by the caller. This fits FCALL
  unchanged: `function.rs:25` is already `let function_name = args[0].clone();`, exactly as
  `eval.rs:25` / `eval.rs:178` take the body/sha.
- **48 §"Step 1" (48:205-213) is wrong as written** — it shows `Body(Bytes) / Sha(Bytes) /
  Function(Bytes)`, and **48 §"Step 2" (48:226-228)** calls
  `execute_single_shard_script(ScriptSource::Function(function_name), keys, argv, …)`. Against 47's
  shape those become `execute_{single,cross}_shard_script(ScriptSource::Function, function_name,
  keys, argv, …)`. 48 must be updated to 47's signature before implementation.
- **48 also still names the pre-collision handler.** Its ownership table (48:334) says
  `handle_script(ScriptSource)`; the method is `handle_eval_family` (the `handle_script` name is
  taken by `scripting/script.rs:12`). 48 inherits the rename.

### Locked-area exposure

- **All of TX4 is in `frogdb-server` (unlocked).** No locked crate, no gate.
- **TX5 touches `frogdb-txn` (LOCKED, gate 0.90) in exactly two lines** — `exec.rs:378` and
  `exec.rs:382`, each an inline `Response::error(literal)` becoming a `to_response()` call. The new
  module lands in `frogdb-types`, which carries no gate.
- **No spec row is edited** (see Problem §4). FM-TXN-032's Observable, NOT-observable, and Invariant
  all stay literally true; `ShardTxnReply`'s variant names are unchanged;
  `error_when_the_shard_channel_is_closed_or_the_request_is_dropped` is unchanged.
- **Re-gate:** `just mutants-diff frogdb-txn` before pushing (push discipline). For the full re-gate,
  `just mutants frogdb-txn` + `just mutants-gate frogdb-txn 0.90`. The score should hold or improve:
  `exec.rs`'s mutable surface *shrinks* (two literal constructions become two calls), and the
  surviving mutation target moves into `frogdb-types` where a direct string-pin test kills it.
- **`frogdb-vll` is untouched** by this proposal — no vll re-gate, even though the locked *area*
  nominally spans both crates. (46 is the one that touches `frogdb-vll`.)
- **`just lint-failure-modes`** — expected no-op; run it (it is in `just lint`).

### Other risks

- **One client-visible string changes.** Cross-shard EVAL/EVALSHA's dropped-reply answer becomes
  `ERR shard dropped request` instead of `ERR script execution failed`. Verified zero assertions
  repo-wide (source, unit tests, integration tests, `.scratch/hardening/specs/`, website). It is a
  **strict improvement in diagnosability** — the new string names the actual condition and is what
  14 other sites and FM-TXN-032 already use, where `ERR script execution failed` misattributes a
  transport death to the script. It changes no retryability contract (neither string carries a
  retryable prefix such as `BUSY`; contrast sibling 46's `ShardBusy` arm, which does). It is still a
  behaviour change and belongs in the CHANGELOG.
- **`lint-continuation-lock` arm counts are unaffected.** The refactor keeps both
  `ScriptingMsg::EvalScript` and `EvalScriptSha` constructions (`into_message` merely moves onto
  `ScriptSource`) and edits no shard-dispatch arm, so the per-enum pinned counts in
  `scripts/continuation-lock-gate.py` and the two GATE entries hold. **Explicitly out of scope:**
  collapsing the two `ScriptingMsg` variants into one `EvalScript { source_kind }` — that *would*
  move `ScriptingMsg`'s pinned count and rewrite two GATE classifications, and it is a `frogdb-core`
  change with its own review.
- **`await_shard_reply`'s generic signature.** The ~14 unconverted sites are not all the same shape
  (`routing.rs` awaits `Result<Result<_,_>,_>`; `pubsub_conn_command.rs:515-527` ignores the send
  result entirely; `broadcast.rs` fans out). The helper is designed for the send-then-recv shape and
  should stay that shape; sites that do not fit call `ShardChannelFailure::…to_response()` directly.
  Do not contort the helper to cover all 14 — that is how a chokepoint becomes a framework.
- **`ScriptSource` must stay fieldless — because the tag is chosen before the payload exists.**
  `handle_eval_family` is what parses `args`, so it is what extracts `args[0]`; its two callers
  (`handle_eval` / `handle_evalsha`) have only the raw slice and must be able to name the variant
  without a payload. A payload-carrying variant would force the parse back up into both entry
  points — reinstating the duplication TX4 removes. (The *borrow*-flavoured version of this
  argument — "a payload-carrying tag would move a non-`Copy` value into the cross-shard closure" —
  is **false** and must not be used: `acquire_continuation_and_run` takes `F: FnOnce() -> Fut`
  (`vll/src/coordinator.rs:330-341`), and today's payload-carrying `EvalKind` is already moved into
  that closure at `eval.rs:139-141` and compiles.)

## Effort estimate

- **TX4: S.** One file, ~60 lines net removed, no new dependencies, no tests to update (there are
  none), no locked crate. Mechanical once `ScriptSource` replaces `EvalKind`.
- **TX5: S–M.** New ~40-line `frogdb-types` module + one `lib.rs` line + one server-side adapter
  helper + 3 `eval.rs` sites + 2 `frogdb-txn` lines + 2 test predicates + one `frogdb-types`
  string-pin test. The `frogdb-txn` mutation re-gate is the long pole, not the diff.
- **Combined: S–M**, landable as one PR. TX5 is meaningfully cheaper after TX4 (three transport
  sites in `eval.rs` become effectively one shared helper call rather than three edits).

### Independently-landable hotfix

**The string divergence — one line, recommended, land first.**
`eval.rs:159`: `"ERR script execution failed"` → `"ERR shard dropped request"`. Plus one integration
test forcing a dropped reply on the cross-shard EVAL path and asserting the two paths agree. This
fixes the live client-visible inconsistency with **no** new module, **no** adapter, and **no**
locked-crate edit — so it needs no mutation re-gate and can go to `main` immediately. 47 then
rebases over it trivially (the line it changes is the line 47 deletes) and TX5 becomes purely
structural: preventing the *next* divergence rather than fixing this one.

*(The former hotfix (b), "the EVAL_RO arity command name", is withdrawn: it was not a bug. See
Problem §1 — the arity rejection never reaches `eval.rs`. The unrelated `guards.rs:566` uppercase
defect found while verifying it is tracked separately as HF-J.)*
