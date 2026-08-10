# Proposal 46 — One `VllAcquireError`, one reply mapping

*Round 38, txn+vll+scripting lane. **Rebased on HF-H** (the `ShardBusy` hotfix, landing
separately on main first — see [Baseline](#baseline-hf-h-has-already-landed)). Every line
number below is against pre-HF-H `main`; HF-H's own diff is four lines in `eval.rs` plus
one spec field, so the drift is small but real — re-read `eval.rs:268–281` and
`vll-failure-modes.md:56` before starting.*

## Summary

`frogdb-vll` exposes two error types for the same thing. `ScatterError`
(coordinator.rs:74–87) and `ContinuationError` (coordinator.rs:148–153) describe the
failure of a VLL *lock acquisition*; `ContinuationError`'s four variants are
field-for-field identical to `ScatterError`'s first four. Each has its own `Display`
(coordinator.rs:89–110, 155–173) and — the part that matters — its own translator to the
client's reply, in a different crate: `scatter_error_to_response`
(`server/src/scatter/executor.rs:141–193`) and `continuation_error_to_response`
(`server/src/connection/scripting/eval.rs:268–281`).

Two adapters exist for a seam that has only one meaning, and they have drifted. Of the
seven conditions both can see, **one produced the same reply and six did not**. The
consequential one — `VllError::ShardBusy` answered `-BUSY` on the scatter path and `-ERR`
on the continuation path — is a live bug, and it is being fixed on its own as HF-H. This
proposal assumes that fix has landed and addresses what HF-H cannot: the *structure* that
let one arm of a seven-row table go missing for the life of the code, and the five
remaining disagreements HF-H leaves in place.

It collapses both types into one `VllAcquireError` whose reply mapping lives once, in
`frogdb-vll`, behind an inner `reply_message()` the mutation gate can actually see. That
is spec-first work: `FM-VLL-001`–`004` pin the strings, so the row edits and the failing
tests come before the fix.

## Baseline: HF-H has already landed

HF-H adds the missing `ShardBusy` arm to `continuation_error_to_response`, mirroring
executor.rs:155–159:

```rust
        ContinuationError::LockFailed { error, .. } => {
            if matches!(error, frogdb_vll::VllError::ShardBusy) {
                Response::error("BUSY shard busy with continuation lock; retry")
            } else {
                Response::error(format!("ERR lock acquisition failed: {error}"))
            }
        }
```

It is spec-first in its own right — it flips `FM-VLL-002`'s pinned `Observable`
(vll-failure-modes.md:56) from `-ERR lock acquisition failed: Shard busy with continuation
lock` to `-BUSY shard busy with continuation lock; retry`, so that row edit and a failing
forcing test land first. Its forcing test should be written to **move into `frogdb-vll`
unchanged** (assert on reply bytes for a given `VllError`, never on a `ConnectionHandler`),
because a test that stays in `frogdb-server` contributes nothing to the 0.90 score. This
proposal moves it.

**What 46 inherits from HF-H:**

- `FM-VLL-002`'s `Observable` (:56) is already correct — **46 does not touch it**.
- `eval.rs:271–273` is already a two-branch arm, not a one-liner.
- The live bug is gone. 46's remaining value is structural, and is stated as such.

**What HF-H does *not* fix, and 46 does:** the other five drifted conditions (below), the
duplicate type, the shard-id leak on the wire, the silent continuation path, and the fact
that the reply table sits outside every mutation gate in the tree. HF-H is a patch to one
arm of a table that nothing forces to be complete; 46 makes completeness structural.

## Files involved

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/vll/src/coordinator.rs` | 886 | Both error types (`ScatterError` 74–87, `ContinuationError` 148–153), both `Display`s (89–110, 155–173), and the two producers (`scatter` 208–310, `acquire_continuation` 361–413). **Owned by this proposal.** |
| ↳ same file, test module | — | **Six in-crate pattern matches break with the `ScatterError::Acquire(..)` reshape**: `ScatterError::LockFailed` at 618, 650; `ScatterError::ShardUnavailable` at 689, 737; `ContinuationError::LockFailed` at 779, 870. All are `assert!(matches!(…))` / `assert!(matches!(err, …))` in the locked crate's own tests, so they are gate-relevant and must be re-spelled, not deleted. |
| `frogdb-server/crates/vll/src/lib.rs` | 28 | Re-export surface (line 19) — `ContinuationError` disappears, `VllAcquireError` appears. **Also line 9**, a doc-comment discrepancy (D2 below) |
| `frogdb-server/crates/vll/Cargo.toml` | 15 | Declares `frogdb-protocol` (line 12), **used nowhere in the crate today** (zero `frogdb_protocol` hits under `crates/vll/`) — the dependency edge the new mapping needs already exists. **Not edited.** |
| `frogdb-server/crates/server/src/scatter/executor.rs` | 194 | `scatter_error_to_response` (141–193), 6 arms / 7 replies. Keeps its `warn!`s, loses its string table |
| `frogdb-server/crates/server/src/connection/scripting/eval.rs` | 281 | `continuation_error_to_response` (268–281, **post-HF-H: 5 arms**); sole call site at 167, import at line 9. **Only these three regions are owned here** — see sibling boundaries |
| `frogdb-server/crates/shard-harness/tests/scenario_s4.rs` | — | `Ok::<(), frogdb_vll::ContinuationError>(())` (line 37) — a type annotation, renamed |
| `frogdb-server/crates/shard-harness/src/sink.rs` | — | Doc comment at line 107 names `ScatterError::ShardUnavailable` as the failure it produces; becomes `VllAcquireError::ShardUnavailable`. One line, easy to miss |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | Preamble scope sentence (11–12, **D1**), preamble mapping-location sentence (14–17), generic-string note (line 29, inside the 25–30 paragraph); `FM-VLL-001`–`004` `Observable` / `Outcome variant` fields (47, 59, 68, 71, 93). **:44 and :56 are not touched** (HF-H owns :56; :44 is already correct) |
| `website/src/content/docs/architecture/vll.md` | — | Pins the generic reply at 167–170 and 237–239; 167–170 additionally claims the internal `VllError` values are "logged, not sent", which the suffix change makes wrong (**D4**) |
| `website/src/content/docs/architecture/glossary.md` | — | Line 144 pins the `-BUSY` string — already correct, already describing the fixed behavior. **Not edited** |

Not edited: `frogdb-txn` (neither error type is referenced from it, verified by grep — the
`txn` area gate command covers both crates but only `frogdb-vll` changes).

## Problem

### The two enums are the same enum

Verbatim, coordinator.rs:74–87 and 148–153:

```rust
pub enum ScatterError {
    ShardUnavailable(ShardSinkError),
    LockFailed { shard_id: usize, error: VllError },
    LockChannelClosed { shard_id: usize },
    LockTimeout { shard_id: usize },
    ResultChannelClosed { shard_id: usize },   // execution half
    ResultTimeout { shard_id: usize },         // execution half
}

pub enum ContinuationError {
    ShardUnavailable(ShardSinkError),
    LockFailed { shard_id: usize, error: VllError },
    LockChannelClosed { shard_id: usize },
    LockTimeout { shard_id: usize },
}
```

`ContinuationError` is `ScatterError` minus the two result-phase variants — not a different
failure model, just a different *phase count*. Both are produced by the same three-way
`match` on `tokio::time::timeout(_, ready_rx)`: compare coordinator.rs:247–264 with
coordinator.rs:394–409.

The two producers have **parallel structure, not identical text** — worth stating precisely,
because the difference is the part a mechanical unification must preserve:

| | scatter (247–264) | continuation (394–409) |
|---|---|---|
| `Failed(error)` arm | `abort_shards(&shard_ids, txid)`, `record_outcome(cmd, "error", …)`, `Err(ScatterError::LockFailed)` | `drop(release_txs)`, `Err(ContinuationError::LockFailed)` |
| `Err(_)` (channel closed) | `abort_shards` + `record_outcome("error")` | `drop(release_txs)` |
| timeout | `abort_shards` + `record_outcome(`**`"timeout"`**`)` | `drop(release_txs)` |

Two differences the unification deliberately leaves untouched: scatter's cleanup is
`abort_shards`, continuation's is `drop(release_txs)` (coordinator.rs:398, 402, 406 — the
release-signal drop documented at 381–385); and scatter splits its metrics label
`"error"`/`"timeout"` where continuation records nothing at all. Only the *error
constructor* is being unified. The cleanup and the metrics stay where they are — the
metrics gap is lane item 3's, routed below.

Under the **deletion test**: delete `ContinuationError` and no complexity reappears
anywhere — the continuation path needs exactly the four states scatter already names. It is
a duplicate, not a distinction.

### Both mappers, in full

`scatter_error_to_response` (executor.rs:141–193) — 6 variants, 7 replies, every arm logs
`conn_id` + `txid` + `shard_id`:

| Arm | Lines | Reply |
|---|---|---|
| `ShardUnavailable(e)` | 143–152 | `ERR shard unavailable` |
| `LockFailed` where `error == ShardBusy` | 153–157 | `BUSY shard busy with continuation lock; retry` |
| `LockFailed` otherwise | 153, 157–159 | `ERR VLL lock acquisition failed` |
| `LockChannelClosed` | 161–167 | `ERR VLL lock acquisition failed` |
| `LockTimeout` | 168–174 | `ERR VLL lock acquisition failed` |
| `ResultChannelClosed` | 175–181 | `ERR shard dropped VLL result` |
| `ResultTimeout` | 182–191 | `ERR VLL execution timeout` |

`continuation_error_to_response` (eval.rs:268–281), **shown post-HF-H** — 4 variants, 5
replies, **no logging**:

| Arm | Reply |
|---|---|
| `ShardUnavailable(_)` | `ERR shard unavailable` |
| `LockFailed` where `error == ShardBusy` | `BUSY shard busy with continuation lock; retry` *(HF-H)* |
| `LockFailed` otherwise | `ERR lock acquisition failed: {error}` (`VllError`'s `Display`, types.rs:40–50) |
| `LockChannelClosed { .. }` | `ERR shard dropped lock request` |
| `LockTimeout { shard_id }` | `ERR lock acquisition timeout on shard {shard_id}` |

### The asymmetry, condition by condition (post-HF-H)

| Condition at the shard / coordinator | Variant | Scatter reply | Continuation reply | Agree? |
|---|---|---|---|---|
| Sink send failed (channel closed) | `ShardUnavailable` | `ERR shard unavailable` | `ERR shard unavailable` | yes |
| Shard replied `Failed(ShardBusy)` | `LockFailed` | `BUSY shard busy with continuation lock; retry` | same *(HF-H)* | yes |
| Shard replied `Failed(LockTimeout)` | `LockFailed` | `ERR VLL lock acquisition failed` | `ERR lock acquisition failed: VLL lock acquisition timeout` | no |
| Shard replied `Failed(QueueFull)` | `LockFailed` | `ERR VLL lock acquisition failed` | `ERR lock acquisition failed: VLL queue full` | no |
| Shard replied `Failed(Aborted)` | `LockFailed` | `ERR VLL lock acquisition failed` | `ERR lock acquisition failed: VLL operation aborted` | no |
| Shard replied `Failed(Internal(m))` | `LockFailed` | `ERR VLL lock acquisition failed` | `ERR lock acquisition failed: VLL internal error: {m}` | no — **and puts an arbitrary internal string on the wire** |
| `ready_tx` dropped before a reply | `LockChannelClosed` | `ERR VLL lock acquisition failed` | `ERR shard dropped lock request` | no |
| Coordinator-side wait for `Ready` expired | `LockTimeout` | `ERR VLL lock acquisition failed` | `ERR lock acquisition timeout on shard {shard_id}` | no — **and leaks an internal shard id to the client** |
| Result channel dropped | `ResultChannelClosed` | `ERR shard dropped VLL result` | n/a | — |
| Gather wait expired | `ResultTimeout` | `ERR VLL execution timeout` | n/a | — |

Two of eight agree; HF-H bought one of them. Five differ, and each difference is an accident
of which file the author was editing — there is no place in the tree where any difference is
stated, justified, or tested.

Three secondary observations fall out of the table:

- **The continuation path is silent.** Every scatter failure emits a `warn!` with `conn_id`,
  `txid`, and `shard_id`; a cross-shard `EVAL` that fails to acquire produces no server-side
  record whatsoever. (Adjacent to the lane's metrics item — routed, not claimed, below.)
- **The continuation path leaks topology.** `ERR lock acquisition timeout on shard
  {shard_id}` puts an internal shard index on the wire (eval.rs:278). Scatter deliberately
  logs the shard id and does not send it.
- **The continuation path leaks internal error text.** `{error}` on the `LockFailed` arm
  renders `VllError::Internal(msg)` through types.rs:47 as `VLL internal error: {msg}` —
  an arbitrary, unbounded, developer-authored string, straight onto the wire. This one
  matters for the unified table below, because a naive `: {e}` suffix *preserves* it.

That asymmetry argues the scatter side is the better baseline for everything except the
missing reason detail.

### Why HF-H is not the end of it

`VllError::ShardBusy` means "this shard is exclusively held by someone else's continuation
lock, right now" — the most transient condition VLL produces (`FM-VLL-003`: a parked
continuation is granted at the drain point or fails at its 2 s deadline; `FM-VLL-002`/`004`:
the holder releases on the guard's drop, coordinator.rs:138–144). One arm of one table
disagreed with that for the life of the code, and nothing anywhere noticed.

A caveat on the argument that clients key retry off the code: **`-BUSY` already carries
three distinct meanings in FrogDB** (glossary.md:142–144 — a Lua script still running, a
`FUNCTION LOAD` over its time budget, and this one), so a client cannot dispatch on `-BUSY`
alone with full precision. The claim is therefore *directional*, not absolute: `-BUSY` is
the retryable family and `-ERR` is not, and putting a transient condition in the wrong
family is still wrong. It is exactly why the *code* must be a function of the error and not
of the entry path.

### Nothing tests either mapper

Campaign-2 issue 10
(`.scratch/hardening-2/issues/open/10-scatter-error-replies-have-no-tests.md`) measured it:
`ERR VLL lock acquisition failed` and `ERR shard dropped VLL result` occur **zero times
outside `executor.rs`**. This proposal's own grep reproduces that and extends it — `ERR lock
acquisition failed:`, `ERR shard dropped lock request`, and `ERR lock acquisition timeout on
shard` likewise occur nowhere but `eval.rs:268–281`. Round-2 issue 88 (line 219) records
both `Display` impls and `VllError`'s as `untested` (tests = 0).

The reason is structural, and it is a **seam** problem: `scatter_error_to_response` is a
private method on `ScatterGatherExecutor`, so reaching it needs `shard_senders`, a
`MetricsRecorder`, a `conn_id`, and (under `turmoil`) a `ChaosConfig`;
`continuation_error_to_response` is a private free function in a server module whose only
caller sits inside a closure passed to the coordinator by a live `ConnectionHandler`. The
reply table is a pure function of one enum, and it is reachable only through the two
heaviest objects in the crate.

Issue 10 already prescribes the remedy's *shape*: "lift the `match` into a free `fn
scatter_error_reply(err: &ScatterError, …) -> Response` that the method delegates to. Prefer
that over making the method public." — and, in its last paragraph, asks for the same
treatment for `continuation_error_to_response` in the same change. This proposal is that
change, with the free function relocated into the gated crate.

### The gate cannot see it — and a naive move does not fix that

`frogdb-vll` carries a 0.90 mutation gate (CLAUDE.md; ADR-0002 records 100% at the Phase 1
lock). `cargo mutants -p <crate>` runs only that package's own tests and generates mutants
only in that package. Both string tables live in `frogdb-server`, which has no gate. So the
mutant that rewrites executor.rs:156 from `"BUSY shard busy with continuation lock; retry"`
to `""` is never generated.

**The naive fix does not change that.** cargo-mutants' only mutant for a function returning
an unknown type is whole-body replacement with `Default::default()`. `Response` derives
`Debug, Clone, PartialEq` and nothing else (`protocol/src/response.rs:646`), so
`Default::default()` does not compile: the mutant is **Unviable**, and
`scripts/mutants-gate.py:46` computes `denom = caught + missed` — Unviable is excluded from
the denominator entirely. A bare `fn to_response(&self) -> Response` moved into
`frogdb-vll` is therefore **exactly as gate-invisible inside the crate as it is outside it**.

Same for the `Display` collapse: `.cargo/mutants.toml`'s `exclude_re` lists `impl Display`,
`impl std::fmt::Display`, and `impl Debug` — merging the two `Display` impls has **zero**
gate effect by construction.

This is the amendment that inverts the original draft's own recommendation, and it drives
the design below.

## Proposed change

### The type

One acquisition error, in `frogdb-vll`, with the string table behind a **mutable return
type**:

```rust
/// Failure of the acquisition half of any VLL choreography — scatter or
/// continuation. Both entry paths reach the same four states, so they answer
/// the client with the same reply, chosen here and nowhere else.
#[derive(Debug)]
pub enum VllAcquireError {
    ShardUnavailable(ShardSinkError),
    LockFailed { shard_id: usize, error: VllError },
    LockChannelClosed { shard_id: usize },
    LockTimeout { shard_id: usize },
}

impl VllAcquireError {
    /// The complete client-visible reply text, code included. This is the
    /// table; `to_response` only wraps it. Returning `String` is deliberate:
    /// it is the shape cargo-mutants can mutate (see "Mutation reachability").
    fn reply_message(&self) -> String { … }

    /// The client-visible reply. `ShardBusy` is the only retryable condition
    /// VLL acquisition produces, and is the only one answered with `-BUSY`.
    pub fn to_response(&self) -> Response {
        Response::error(self.reply_message())
    }

    /// The participant the failure is attributed to — for the host's log line.
    pub fn shard_id(&self) -> usize { … }
}

/// Scatter adds the execute/gather half on top of acquisition.
#[derive(Debug)]
pub enum ScatterError {
    Acquire(VllAcquireError),
    ResultChannelClosed { shard_id: usize },
    ResultTimeout { shard_id: usize },
}

impl ScatterError {
    fn reply_message(&self) -> String { … }        // delegates for `Acquire`
    pub fn to_response(&self) -> Response { Response::error(self.reply_message()) }
}
```

`ContinuationError` is deleted; `acquire_continuation_and_run` returns `Result<T,
VllAcquireError>`. Both `Display` impls collapse to one plus a short wrapper. Call sites
shrink to:

```rust
// executor.rs
Err(err) => { self.log_scatter_error(&err, txid); return err.to_response(); }

// eval.rs:165-168
match outcome { Ok(resp) => resp, Err(err) => err.to_response() }
```

and `continuation_error_to_response` (eval.rs:268–281) is deleted outright. Logging stays
host-side: `conn_id`, `txid`, and `self.timeout` are host facts and have no business in the
locking crate.

### Where the mapping lives — the decision is the *shape*, not the crate

The crate question is settled and needs no sign-off: `frogdb-vll/Cargo.toml:12` already
declares `frogdb-protocol` and the crate uses it **nowhere** (zero `frogdb_protocol`
references anywhere under `crates/vll/`), so `to_response() -> frogdb_protocol::Response`
requires no manifest change and introduces no new dependency edge — it puts an existing,
currently-idle edge to work. ADR-0002 governs the `TxnHost` seam in `frogdb-txn` and does
not bind here: `frogdb-txn` references neither error type (the `ShardScatterError` hits in
`telemetry/src/node_state.rs` and `server/src/info/mod.rs` are an unrelated INFO-scatter
type). What *does* carry over is the ADR's underlying reason for the extraction: put the
behavior where the gate that guards it can see it.

**The real decision is the return type**, and it is the one the original draft got
backwards. Three shapes:

| Shape | Single decision point? | Gate-visible? |
|---|---|---|
| `to_response(&self) -> Response` only | yes | **no** — the only mutant is `Default::default()`, Unviable, excluded from the denominator (`mutants-gate.py:46`) |
| `wire_reply(&self) -> (&'static str, Option<String>)` + a renderer in each host | **no** — two renderers, free to disagree again | yes |
| **`reply_message(&self) -> String` (private) + `to_response` wrapper** | yes | yes |

The third gets both, and is what this proposal recommends. cargo-mutants replaces `String`
returns with its String defaults (`String::new()` and `"xyzzy".into()`); the table test
kills both. `to_response` stays the caller-facing interface, so no host ever renders a
reply itself and the drift this proposal closes cannot reopen. If a host later genuinely
needs the code separately (a metrics label, say), promote the split to `wire_reply(&self)
-> (&'static str, Option<String>)` **inside `frogdb-vll`** and keep `to_response` as its
sole renderer — never a renderer per host.

### The new pinned string set

Rule: the retryable condition keeps its distinct code; everything else keeps the `ERR VLL
lock acquisition failed` prefix the docs already name, gains the reason suffix the
continuation path already provides, never carries a shard id, and **never carries an
operator-authored string**.

| Variant | Unified reply |
|---|---|
| `ShardUnavailable` | `ERR shard unavailable` *(both paths already; unchanged)* |
| `LockFailed { error: ShardBusy }` | `BUSY shard busy with continuation lock; retry` *(unchanged, both paths, post-HF-H)* |
| `LockFailed { error: QueueFull \| LockTimeout \| Aborted }` | `ERR VLL lock acquisition failed: {e}` |
| `LockFailed { error: Internal(_) }` | `ERR VLL lock acquisition failed` — **no suffix**, see below |
| `LockChannelClosed` | `ERR VLL lock acquisition failed: shard dropped the lock request` |
| `LockTimeout` | `ERR VLL lock acquisition failed: coordinator timeout` |
| `ResultChannelClosed` *(scatter only)* | `ERR shard dropped VLL result` *(unchanged)* |
| `ResultTimeout` *(scatter only)* | `ERR VLL execution timeout` *(unchanged)* |

**`Internal` is excluded from the suffix, deliberately.** `VllError::Internal(String)`
(types.rs:37) renders through types.rs:47 as `VLL internal error: {msg}` — an arbitrary
string chosen by whoever wrote the call site. A blanket `: {e}` suffix would take today's
continuation-path leak (`ERR lock acquisition failed: VLL internal error: {m}`) and extend
it to the scatter path, which does not leak it today. The reason detail exists to name a
*condition*, and `Internal` is precisely the variant that names no condition. It is logged
host-side, with `error = %error`, where it already goes (executor.rs:154).

The suffix set is therefore **closed**: four fixed strings (`VLL queue full`, `VLL lock
acquisition timeout`, `VLL operation aborted`, plus the two channel/timeout literals), no
interpolation of anything a caller controls, no shard index. That is a property the guard
test can assert directly.

**Deliberately not folded in:** whether `QueueFull` should also be retryable (`-BUSY`). It
is arguably as transient as `ShardBusy`, but no FM row covers it, the website documents it
as `ERR` (vll.md:237–239), and changing it is a separate client-contract decision. Left as
a stated open question so the unification does not smuggle a second behavior change.

## Discrepancies (spec/doc statements that are wrong today)

Four, all found while verifying this proposal. D1 and D2 are corrections this proposal
lands as part of Step 1; D3 and D4 are consequences of the change.

- **D1 — `vll-failure-modes.md:11–12`** describes the continuation lock as "the
  shard-exclusive lock **a cross-shard Lua script / MULTI** takes". No MULTI path takes one.
  `acquire_continuation_and_run` has exactly two callers: `eval.rs:134` and
  `shard-harness/tests/scenario_s4.rs:29` (one production, one test). The only production
  sender of `VllMsg::VllContinuationLock` is `server/src/vll_adapter.rs:137`
  (`send_continuation_lock`, constructing the message at :145), and its only caller is the
  coordinator. No `EXEC`/`MULTI` path exists. **Fix:** name Lua scripts; note MULTI as
  not-yet.
- **D2 — `vll/src/lib.rs:9`** carries the same error in the crate's own module docs:
  "Continuation locks for **MULTI/EXEC and Lua scripts**". Same fix.
- **D3 — issue 10 cites `vll-failure-modes.md:15-18`** as where the mapping location is
  named. That citation is stale after this change (the mapping moves into `frogdb-vll`);
  issue 10 is closed by this proposal, so the citation dies with it — but if issue 10 is
  closed *before* 46 lands, the closure note must say the location moved.
- **D4 — `vll.md:167–170`** states "the internal `VllError::QueueFull` / `LockTimeout`
  values are **logged, not sent** as distinct wire codes". After the suffix change they are
  sent — as *text*, still not as codes. The sentence needs the distinction made explicit
  rather than deleted, because the "no dedicated VLL timeout error code" half stays true.

D1/D2 matter beyond tidiness: they are why the blast radius of `FM-VLL-002`/`003`'s
client-visible strings is smaller than the spec implies. Those strings are reachable only
via `EVAL`/`EVALSHA` today.

## Spec-first sequence

### Step 1 — edit the rows and the prose. No code yet.

1. **`vll-failure-modes.md:11–12`** (D1) — "a cross-shard Lua script / MULTI takes" → "a
   cross-shard Lua script takes (no MULTI/EXEC path acquires one today)".
2. **`vll/src/lib.rs:9`** (D2) — "Continuation locks for MULTI/EXEC and Lua scripts" →
   "Continuation locks for cross-shard Lua scripts". Doc-only; lands with the spec edit, not
   with the code.
3. **`vll-failure-modes.md:14–17`**, mapping location — "lives in `…/scatter/executor.rs`
   (`scatter_error_to_response`) and `…/scripting/eval.rs`
   (`continuation_error_to_response`)" → "lives in `VllAcquireError::reply_message`
   (`frogdb-vll`), rendered by `to_response`; the hosts only log".
4. **`vll-failure-modes.md:29`** (inside the 25–30 "not yet rowed" note), generic string —
   `-ERR VLL lock acquisition failed` → same prefix, `: {reason}` suffix, with the
   `Internal` exception named.
5. **FM-VLL-001 (47)**, Outcome variant — `→ ScatterError::LockFailed` → `→
   VllAcquireError::LockFailed`. (`:44` Observable is already correct; `:46` Invariant
   belongs to sibling 51 — do not touch.)
6. **FM-VLL-002 (59)**, Outcome variant — `→ ContinuationError::LockFailed` → `→
   VllAcquireError::LockFailed`. (`:56` Observable was fixed by HF-H.)
7. **FM-VLL-002 (57)**, NOT observable — add, *if HF-H did not already*: *the same
   shard-busy condition answered with a fatal code on one entry path and a retryable one on
   another — the reply is a function of the error, not of the caller.* Check before adding;
   this sentence is the natural home for HF-H's own row edit.
8. **FM-VLL-003 (68)**, Observable — `-ERR lock acquisition failed: VLL lock acquisition
   timeout` → `-ERR VLL lock acquisition failed: VLL lock acquisition timeout`.
9. **FM-VLL-003 (71)**, Outcome variant — `→ ContinuationError::LockFailed` → `→
   VllAcquireError::LockFailed`.
10. **FM-VLL-004 (93)**, Outcome variant — `→ ScatterError::LockFailed` → `→
    VllAcquireError::LockFailed`. (`:90` Observable refers to FM-VLL-001; unchanged.)

### Step 2 — write the failing tests

In `frogdb-vll`, tagged and appended to the four rows' `Forced by` lists:

- **A table test over `reply_message`/`to_response`:** 4 variants × the 5 `VllError` values
  on `LockFailed`, asserting exact reply bytes. Tagged `// FM-VLL-001`, `// FM-VLL-002`,
  `// FM-VLL-003`, `// FM-VLL-004` on the cases that force each row's `Observable`. HF-H's
  forcing test moves in here unchanged.
- **A guard test with three assertions**, all over the same enumeration of every
  `VllAcquireError` × `VllError` pair:
  1. no reply contains a shard index (currently violated by eval.rs:278);
  2. no reply contains an operator-authored string — assert `LockFailed { error:
     Internal("aaa") }` and `LockFailed { error: Internal("bbb") }` produce **byte-identical
     replies**, which is the leak-proof formulation and does not depend on guessing the
     payload;
  3. `ShardBusy` — and only `ShardBusy` — yields a reply beginning `BUSY`.
- **An end-to-end case** driving a client into the busy arm through **both** entry paths and
  asserting `BUSY shard busy with continuation lock; retry` off the wire — the thing issue
  10 says no test does. Post-HF-H both halves pass; the test's job is to keep them passing.

### Step 3 — unify

Introduce `VllAcquireError` with `reply_message` + `to_response` + `shard_id`, fold
`ContinuationError` into it, reshape `ScatterError` around `Acquire(..)`, move both string
tables into `reply_message`, delete `continuation_error_to_response`, re-spell the six
in-crate `matches!` assertions (coordinator.rs:618, 650, 689, 737, 779, 870), fix
`shard-harness/src/sink.rs:107` and `scenario_s4.rs:37`, reduce the two host arms to log +
delegate.

### Step 4 — docs

`vll.md:167–170` (suffix added; D4's logged-vs-sent sentence corrected) and `vll.md:237–239`
(suffix). `glossary.md:144` needs no change.

### Step 5 — re-gate

`just mutants-diff frogdb-vll`, then `just mutants frogdb-vll` + `just mutants-gate
frogdb-vll 0.90`. `just lint-failure-modes` must pass in both directions.

## Testability improvement

**What is untestable today.** The reply table — the only part of VLL a client actually keys
behavior off — is a pure function of one enum that cannot be called without constructing
either a `ScatterGatherExecutor` (shard senders, metrics recorder, conn id, chaos config) or
a live `ConnectionHandler` plus a coordinator plus real shards. Consequently: zero tests,
per issue 10 and issue 88.

**What becomes testable.** `VllAcquireError::to_response` is a `&self -> Response` on a
public type in a crate whose whole suite runs in under a second. Every row's `Observable`
sentence becomes an assertion:

- **The retry contract as an invariant, not a coincidence.** `ShardBusy` — and only
  `ShardBusy` — yields `-BUSY`. Add a `VllError` variant and the wildcard-free match makes it
  a compile error until someone decides its retryability, the same forcing function
  `MockTxnHost` gives `TransactionOutcome` under ADR-0002. This is what HF-H cannot buy:
  HF-H fixes one arm, this makes a missing arm impossible.
- **Entry-path independence.** A single test can assert `scatter_err.to_response() ==
  continuation_err.to_response()` for the same underlying `VllAcquireError`. Today that
  property is unstatable — there are two types, so there is nothing to compare.
- **No topology and no operator strings on the wire** (guard test, above).
- **`FM-VLL-001`–`004`'s client sentences get real witnesses.**

**Closing issue 10.** Issue 10's `Fix` section asks for a table test, an end-to-end `-BUSY`
case, `Forced by` citations, and the same treatment for `continuation_error_to_response`.
Steps 2–3 deliver all four. To be precise about *what* closes it: the **tests** close issue
10, not the crate move — a table test in `frogdb-server` would satisfy the issue's letter.
The crate move is what keeps it closed, by putting the table where the 0.90 gate runs.

**Mutation reachability — a real but bounded win.** Stated honestly, because the original
draft overstated it:

- Today: `cargo mutants -p frogdb-vll` generates **no** mutant of either string table (both
  are in an ungated crate).
- After a naive `to_response(&self) -> Response` move: still effectively none — one Unviable
  mutant, excluded from the denominator. **No change.**
- After the `reply_message(&self) -> String` shape: cargo-mutants generates its String
  replacements for `VllAcquireError::reply_message` and `ScatterError::reply_message`, and
  `shard_id(&self) -> usize` picks up the `0`/`1` replacements. The table test kills all of
  them.

That is a handful of viable mutants, not per-arm coverage — cargo-mutants replaces function
bodies, not individual match arms, so the gate never proves each arm's string
independently. **The table test is the pin; the gate is what stops the pin being quietly
removed.** Claiming more would repeat the mistake this proposal is trying to fix.

## Risks / scope boundaries vs siblings

**Client-visible behavior change (intended, and smaller than pre-HF-H).** The `-BUSY` fix is
HF-H's. What 46 changes on the wire: the `ERR VLL lock acquisition failed` family gains a
reason suffix on both paths; the continuation path stops emitting a shard index and stops
emitting `Internal`'s message; three continuation-only strings (`ERR lock acquisition
failed: …`, `ERR shard dropped lock request`, `ERR lock acquisition timeout on shard …`)
cease to exist. FrogDB is pre-production and CLAUDE.md permits breaking changes that are
improvements.

**String churn is test-free.** Verified by grep: `ERR VLL lock acquisition failed`, `ERR
shard dropped VLL result`, `ERR lock acquisition failed:`, `ERR shard dropped lock request`,
and `ERR lock acquisition timeout on shard` appear in **no test, no tcl regression file, and
no fixture** — only in the two mappers themselves, the spec, the website, and scratch notes.
Adding the `: {reason}` suffix therefore breaks no assertion; a client matching the existing
prefix is also unaffected. Only exact-equality matchers on the bare generic string would
notice, and none exist in-tree.

**A third spelling exists and is explicitly out of scope.** `ERR shard busy with continuation
lock` — no `BUSY` code, no `; retry` — is produced by `ShardWorker::can_execute_during_lock`
(`core/src/shard/worker.rs:859`) with a fallback at `core/src/shard/dispatch_core.rs:202`,
and asserted in `search/merge.rs` (887/893, 950/955, 976/981) and `scatter/broadcast.rs`
(1057/1067). That is a **different seam and a different condition**: the shard-local
admission gate refusing a *foreign connection's already-dispatched command*, not a lock
*acquisition*. It is pinned by the C3 seam lint (`scripts/continuation-lock-gate.py`,
chokepoint `can_execute_during_lock`) and by those tests. Do not sweep it into this change.
Whether it should also carry `-BUSY` is a real question and is **already tracked** —
campaign-2 issue 05
(`.scratch/hardening-2/issues/open/05-functioncall-bypasses-the-vll-continuation-gate.md:47`)
names this exact string as the reply an `FCALL` against a held shard must get. Route the
question there; touching it here would put a seam-linted chokepoint and six live assertions
into a diff that is otherwise test-free.

**Blast radius is smaller than the spec implies** — see D1/D2. `FM-VLL-002`/`003`'s
client-visible strings are reachable only via `EVAL`/`EVALSHA`.

**Mutation re-gate required.** `frogdb-vll` is LOCKED at 0.90 and this change adds mutable
code to it. `just mutants-diff frogdb-vll` as push discipline; full run `just mutants
frogdb-vll` + `just mutants-gate frogdb-vll 0.90`. Expect the score to *move* — new viable
mutants land in the gated crate and the table test exists to kill them. `frogdb-txn` is
untouched (it shares the `txn` area in `just core-test` but has no code in this diff).

**Sibling boundaries — one shared code file, one shared spec file, three orderings.**

- **HF-H (the `ShardBusy` hotfix, landing on main first).** Owns `eval.rs:271–273` and
  `vll-failure-modes.md:56`. 46 assumes both; re-read them before writing 46's diff. If HF-H
  slips, 46 absorbs it — the `-BUSY` arm is in 46's unified table either way — but then 46's
  spec diff regains the `:56` edit and the "live bug" framing above.
- **Proposal 51 (`txn-slot-vll-state-small`, TX11+TX12).** Owns `vll-failure-modes.md:46` —
  FM-VLL-001's `Invariant`, which names `ensure_initialized`. That is **one line from 46's
  `:47` edit, inside the same row**, and adjacent to `:44`. No code overlap (51 owns
  `vll/src/shard.rs` and `txn/src/state.rs`; 46 owns `coordinator.rs`). **Sequence, do not
  parallelize** — a same-row markdown conflict is trivial to resolve but not worth resolving
  by hand under time pressure. Recommended order: **51 → 46 → 45**. 51 is S and mechanical
  and is already on 45's critical path, so putting it first satisfies both constraints with
  one ordering; 46 then rebases one row-field over a one-line Invariant edit.
- **Proposal 45 (`vll-key-ownership-diagnostics`, lane items 1 + 13).** Owns
  `vll/src/shard.rs`, `vll/src/lock_table.rs`, `core/src/shard/vll.rs`,
  `core/src/shard/diagnostics.rs`, and in the spec `L20–23`, `:69`, `:105–106`. No code
  overlap with 46. **Spec adjacency is real**: 45's scope-carve-out edit at L20–23 sits
  immediately below 46's preamble edits at L11–12 and L14–17, and both proposals touch
  FM-VLL-003 (45 the NOT-observable at `:69`, 46 the Observable at `:68` and Outcome variant
  at `:71`). **Keep the existing ruling: 46 before 45.** 46's spec diff is the smaller one
  and 45 rebases onto it cleanly.
- **Proposal 47 (`eval-orchestration-dedup`, lane item 4)** owns `eval.rs` `handle_eval`
  (19–63), `handle_evalsha` (172–216), `ScriptShards`, and `EvalKind`. **Same file as 46,
  disjoint regions**: 46 owns *only* the import at line 9, the `Err(err) =>` arm at 165–168,
  and the function at 268–281. Neither may touch the other's region. If 47 also takes lane
  item 5 (the single-shard `ERR shard unavailable` / `ERR shard dropped request` pair at
  eval.rs:106/111 versus the cross-shard `ERR shard unavailable` / `ERR script execution
  failed` pair at eval.rs:155/159), that lives inside 47's region — but both proposals then
  change reply strings in one file, so land **46 → 47** and 47 must not re-open the
  acquisition mapping.
- **Proposal 52 (`vll-unknown-txid-refusal`, lane item 14)** owns
  `core/src/shard/vll.rs::handle_vll_execute` and appends a new `FM-VLL-006`. No code
  overlap; the spec edit is append-only, so it merges in either order.
- **Lane item 3 (continuation acquisition unobservable)** is the one genuine code collision:
  it edits `coordinator.rs::acquire_continuation` (a `finish(outcome)` metrics hoist) and
  `eval.rs:131` (`NoopMetricsSink` → a real sink) — both files 46 edits, and
  `acquire_continuation` is the function whose error type 46 changes. 46 does **not** claim
  it, and deliberately leaves the metrics asymmetry documented above untouched, but
  sequencing 46 first makes item 3's diff mechanical. 46's problem statement surfaces the
  same gap from the logging side (the continuation path emits no `warn!` at all); whoever
  takes item 3 should take the missing `warn!` with it.

## Effort

**M.** The code is S: one enum, one `reply_message` + two thin wrappers, one wrapper enum,
two call sites reduced to log-plus-delegate, one deleted free function, six in-crate
`matches!` re-spellings, two doc-comment/type-annotation fixes in `shard-harness`. What makes
it M is the locked-crate tax — six spec row fields and three preamble edits, a table test
plus a three-assertion guard test plus an end-to-end two-entry-path test written *before* the
fix, `Forced by` lists extended, `just lint-failure-modes` re-run, and the `frogdb-vll` 0.90
gate re-run. Plus two website regions. No manifest changes, no dependency changes, no caller
churn outside the two mappers.

**Independently-landable hotfix: none remaining.** It was HF-H, and it is landing separately.
Everything left here is structural and must land as one change: the type, the table, the
tests, and the spec rows move together or the spec lies about where the mapping is.
