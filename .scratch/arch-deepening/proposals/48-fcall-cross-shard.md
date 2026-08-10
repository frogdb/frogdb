# Proposal 48 — FCALL routes through the script-shard classifier; one keyless-target policy

*Round 38. Candidate TX6 of the txn+vll+scripting lane. **Live capability gap**: a cross-shard
`FCALL` is refused where the byte-equivalent `EVAL` is served.*

## Summary (2-3 sentences)

`classify_script_shards` (`eval.rs:65-84`) is the deep module that owns "which shards does this
script's declared key set touch, and is that allowed here" — three outcomes, one of which
(`CrossShard`) carries the continuation-lock obligation. `handle_fcall`
(`function.rs:14-73`) is a **second dispatch path for the same concept**: it calls
`SlotValidator::same_shard` directly, never reads `self.allow_cross_slot`, never takes a
continuation lock, and hardcodes the keyless target to shard 0 — so with
`allow_cross_slot_standalone = true` a two-shard `EVAL` runs under VLL coordination while the
identical `FCALL` gets `-CROSSSLOT`. The fix is to delete the duplicated adapter: route `FCALL`
through `classify_script_shards` + the two existing executors, and give the keyless-target
decision one named policy function instead of two hardcoded `0`s.

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | **Owner of this proposal's client-side change.** `handle_fcall` (L14–73): shard determination L42–47, `ScriptingMsg::FunctionCall` construction L50–59, send L61–67, receive L69–72. Nothing else in the file (the `FUNCTION` subcommands) is touched. |
| `frogdb-server/crates/server/src/connection/scripting/eval.rs` | 281 | **The module being reused, owned by sibling 47.** `classify_script_shards` (L65–84), `execute_single_shard_script` (L87–113), `execute_cross_shard_script` (L119–169), `ScriptShards` (L219–227), `EvalKind` (L230–266), `continuation_error_to_response` (L268–281). 48 adds one `EvalKind`/`ScriptSource` variant; it must not otherwise edit this file. |
| `frogdb-server/crates/server/src/slot_migration/validator.rs` | 168 | `SlotValidator::same_shard` (L31–43) — `Ok(None)` on empty, `Err(redirect::crossslot())` on mismatch. Unchanged; both paths already get a byte-identical CROSSSLOT reply (`frogdb-server/crates/types/src/redirect.rs:17`). |
| `frogdb-server/crates/server/src/connection/routing.rs` | 351 | The **other** keyless policy: keyless commands → `self.shard_id` (L76–78); the `same_shard` `Ok(None)` fallback → `self.shard_id` (L106–110, documented unreachable); `allow_cross_slot` gate (L124). |
| `frogdb-server/crates/txn/src/exec.rs` | 412 | **LOCKED (txn, gate 0.90) — read-only reference.** The canonical keyless-target policy: `Ok(TransactionTarget::None) => host.shard_id()` (L278), with its rationale at L268–277. No edit proposed here. |
| `frogdb-server/crates/core/src/shard/dispatch_scripting.rs` | 116 | The shard-side twin of the asymmetry: `EvalScript` gates at L17, `EvalScriptSha` at L42, `FunctionCall` (L77–97) does not. **Not this proposal's edit** — see hardening-2 issue 05 below. |
| `scripts/continuation-lock-gate.py` | 458 | Seam lint. `GATE` set L96–103 (names `ScriptingMsg::EvalScript`, `EvalScriptSha`, `ScriptSubCommand`); `GATE_GAP` L136–150 pins `ScriptingMsg::FunctionCall` (L144–150) as a *known warn-not-fail bypass*. |
| `.scratch/hardening-2/issues/open/05-functioncall-bypasses-the-vll-continuation-gate.md` | 63 | The already-filed, `ready-for-agent` issue for the shard-side half. 48 is its client-side complement; the two are independent (see Risks). |
| `frogdb-server/crates/core/src/shard/functions.rs` | 118 | `handle_function_call` (L11–117): registry lookup, `no-cluster` enforcement (L55), `FCALL_RO` write rejection, OOM check, `execute_function`, `run_script_write_effects` (L111). Unchanged by 48. |
| `frogdb-server/crates/core/src/scripting/executor.rs` | 1215 | `execute_script` seeds the runtime tracker from the shebang (L318–333); `execute_function` passes `enforce_cross_slot = false` + `CrossSlotTracker::new(None)` (L525–529). **Adjacent verified divergence — out of scope, see §5.** |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | LOCKED. FM-TXN-021 (L278–288), FM-TXN-029 (L374–384), FM-TXN-030 (L386–396), deviations table L649. **Analysed below: none pins FCALL; no spec-first requirement.** |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED. Scope paragraph L10–17 names `eval.rs`'s `continuation_error_to_response` as the client-reply mapper. One wording edit (see §Proposed change step 5). |
| `frogdb-server/crates/server/tests/functions.rs` | 866 | Every FCALL test starts a **one-shard** server (`num_shards: Some(1)` at L11, and again at L687/L719/L764/L821). The gap is invisible to the suite by construction. |
| `website/src/content/docs/architecture/vll.md` | 277 | L51: `| Lua scripts | EVAL, EVALSHA | Continuation lock before execution |` — the row that must gain FCALL. |
| `website/src/content/docs/architecture/consistency.md` | — | L49–56: the documented contract that `allow_cross_slot_standalone = true` means *cross-shard operations are served via VLL coordination*, with no carve-out for FCALL. |

## Problem (verified)

### 1. Two dispatch paths for one concept

Both handlers answer the same question about the same argument layout
(`… numkeys key… arg…`, extracted by the same `numkeys_keys` helper at
`scripting_conn_command.rs:58-70`). They answer it differently:

| Decision | `handle_eval` / `handle_evalsha` (`eval.rs:40-62`, `:193-215`) | `handle_fcall` (`function.rs:42-72`) |
| --- | --- | --- |
| Same-shard check | `SlotValidator::same_shard` **inside** `classify_script_shards` (`eval.rs:70`) | `SlotValidator::same_shard` inline at the call site (`function.rs:44`) |
| Keyless (`Ok(None)`) | `ScriptShards::Single(0)` (`eval.rs:71`) | `shard.unwrap_or(0)` (`function.rs:45`) |
| Keys span shards, `allow_cross_slot == true` | `ScriptShards::CrossShard(shards)`, shards deduped + sorted (`eval.rs:73-80`) | **not consulted** — falls into the `Err` arm |
| Keys span shards, `allow_cross_slot == false` | `redirect::crossslot()` (`eval.rs:61`, `:82`) | `redirect::crossslot()` (via the `Err` payload, `function.rs:46`) |
| Continuation lock | `execute_cross_shard_script` → `VllCoordinator::acquire_continuation_and_run` (`eval.rs:119-169`) | **none** |
| Dropped-oneshot reply | `"ERR shard dropped request"` single-shard (`eval.rs:111`) / `"ERR script execution failed"` cross-shard (`eval.rs:159`) | `"ERR shard dropped request"` (`function.rs:71`) |
| Lock-failure replies | `continuation_error_to_response` (`eval.rs:268-281`) | unreachable |

The deletion test settles the shape: delete `classify_script_shards` and the same three-way
decision reappears at three call sites (`handle_eval`, `handle_evalsha`, `handle_fcall`). It is
earning its keep — `handle_fcall` is simply not calling it.

### 2. The capability gap is live and user-visible

`self.allow_cross_slot` is `config.server.allow_cross_slot_standalone`
(`server/subsystems.rs:559` → `acceptor.rs:113/175` → `connection.rs:296`). `eval.rs:73` is its
**only** reader in the scripting module; `routing.rs:124` is the only other reader in the
connection layer. `function.rs` never mentions it.

So on a standalone multi-shard server with the flag on:

```
EVAL  "return redis.call('GET', KEYS[1])"  2  alpha beta   →  served (VLL continuation locks)
FCALL myfn                                 2  alpha beta   →  -CROSSSLOT Keys in request don't hash to the same slot
```

with `alpha` and `beta` on different shards. The website documents the flag as governing
cross-shard *operations* generically (`consistency.md:49-56`, a three-row table whose only
carve-out is cluster mode), and the regression suite header records the same understanding —
`cluster_scripting_regression.rs:6`: *"Cross-slot validation: Implemented (via `allow_cross_slot`
server config)"*. FCALL is the exception nobody wrote down.

**Redis-parity note (repo-local evidence only).** Upstream Redis rejects cross-slot keys for
*both* `EVAL` and `FCALL` in cluster mode, and FrogDB does too — `allow_cross_slot_standalone`
is explicitly standalone-only (`consistency.md:56`, *"Cluster mode (any setting) → `-CROSSSLOT`"*).
So this is **not** a Redis-compatibility deviation; it is an internal inconsistency in a FrogDB
extension. Redis's own script-level cross-slot control is the `allow-cross-slot-keys` shebang
flag (`FunctionFlags::ALLOW_CROSS_SLOT_KEYS`, `frogdb-scripting/src/function.rs:18`; parsed at
`executor.rs:56`; policy documented at `core/src/scripting/gate.rs:24-45`), which is a *runtime
undeclared-key* rule and orthogonal to the shard-routing decision this proposal is about. The
repo's cluster-mode FCALL regression tests
(`cluster_scripting_regression.rs`, `cluster_scripting_tcl.rs:165`) exercise only the
`no-cluster` flag and zero-key/one-key calls — nothing there pins the cross-shard refusal.

### 3. FCALL takes no continuation lock — on either side

Client side: `handle_fcall` sends `ScriptingMsg::FunctionCall` to one shard with no `VllCoordinator`
involvement (`function.rs:50-67`). That is *consistent* today only because FCALL is never
cross-shard; the moment step 2 of the fix lands, the lock is mandatory.

Shard side, independently: `dispatch_scripting.rs:77-97` runs `handle_function_call` with no
`can_execute_during_lock(conn_id)` check, while `EvalScript` (L17) and `EvalScriptSha` (L42) in
the same `match` do. This is **already known and pinned**: `scripts/continuation-lock-gate.py`
lists `ScriptingMsg::FunctionCall` in `GATE_GAP` (L144–150) — a warn-not-fail entry linked to
`.scratch/hardening-2/issues/open/05-functioncall-bypasses-the-vll-continuation-gate.md`
(status `ready-for-agent`, score 6). The two halves are the same asymmetry seen from opposite
ends: FCALL cannot *be* a lock owner (48) and does not *respect* one (issue 05).

### 4. The keyless target has two policies, and scripting owns the odd one

Enumerating every place a `same_shard`-style "no keys" outcome picks a shard:

| # | Site | Policy | Reachable? |
| --- | --- | --- | --- |
| 1 | `eval.rs:71` — `Ok(None) => ScriptShards::Single(0)` | **hardcoded 0** | yes (`EVAL … 0`) |
| 2 | `function.rs:45` — `shard.unwrap_or(0)` | **hardcoded 0** | yes (`FCALL fn 0`) |
| 3 | `routing.rs:106-110` — `shard.unwrap_or(self.shard_id)` | `self.shard_id` | no — the file's own comment (L102–105) proves the empty and single-key cases returned above |
| — | `routing.rs:76-78` — keyless commands | `self.shard_id` | yes, the general-command policy |
| — | `txn/exec.rs:278` — `TransactionTarget::None` | `host.shard_id()` | yes, the EXEC policy, rationale at L268–277 |

**Verification discrepancy vs the lane note:** the lane says "keyless→shard-0 hardcoded 3 places".
Only **two** sites hardcode `0` (rows 1–2, both in scripting). The third `same_shard` `Ok(None)`
fallback (row 3) already uses `self.shard_id` and is documented-unreachable. The real finding is
sharper than the lane's: the server has **one** keyless policy — "this connection's shard",
used by `routing.rs:76`, `routing.rs:108` and `txn/exec.rs:278` — and scripting is the sole
dissenter, in two places, with no comment explaining why beyond `function.rs:43`'s bare
*"No keys -> shard 0."*

### 5. Adjacent verified divergence (called out, deliberately **not** fixed here)

`execute_function` passes `enforce_cross_slot = false` and `CrossSlotTracker::new(None)`
(`executor.rs:525-529`), where `execute_script` derives both from the shebang
(`executor.rs:318-333`). Consequence: in cluster mode, an `EVAL` shebang script without
`allow-cross-slot-keys` is rejected for touching a second slot at runtime, and an `FCALL` of a
function with the same flags is not — and `FunctionFlags::ALLOW_CROSS_SLOT_KEYS` has **no reader
at all** on the FCALL path. This is a genuine Redis-parity divergence in the *runtime undeclared-key*
rule, distinct from the *declared-key routing* rule this proposal fixes, and it lives in
`frogdb-core`'s executor rather than the connection layer. **File it as a separate issue**;
bundling it doubles 48's blast radius and its crate.

## Proposed change

### Is this spec-first? No — with the analysis stated precisely

The lane flagged FM-TXN-021 and FM-TXN-030. Both were re-read in full; **neither pins FCALL's
refusal**, and no row in `txn-failure-modes.md` or `vll-failure-modes.md` mentions FCALL,
`FunctionCall`, or functions at all (`rg -i 'fcall|function' ` over both specs returns only
`txn-failure-modes.md:29`, a sentence about *Rust* function names in test IDs).

- **FM-TXN-021** (`:278-288`) — *"allow-cross-slot-standalone does not relax transactions"*.
  Trigger is a cross-shard `MULTI`; the pinned invariant is *"The transaction fold never reads the
  config"*, with the NOT-observable being the flag *leaking into* `fold_transaction_keys`/`resolve`.
  Forced by `test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone`,
  `test_multi_cross_shard_crossslot_with_flag_disabled`,
  `test_multi_single_shard_commits_with_allow_cross_slot_standalone`. This proposal adds a **new**
  reader of the flag in the connection layer (`handle_fcall`) and touches nothing in
  `frogdb-txn` — the row's invariant is *strengthened by contrast*, not violated. Its own
  deviations-table entry (`:649`) even states the intended split: *"Single-key ops may cross shards
  via VLL; transactions deliberately may not."* An FCALL is an op, not a transaction.
- **FM-TXN-030** (`:386-396`) — *"Scatter and script batches are slot-validated like any other"*.
  Trigger is a *transaction containing* an `EVAL` with declared `KEYS` on a node that lost the slot;
  the invariant is about **key extraction for the fold** covering script-declared keys so the
  keyless fast path is not taken. FCALL's keys already feed that fold through the same registry
  path (`FcallConnCommand` `scripting_conn_command.rs:313-330`, whose `dynamic_keys` calls the
  shared `numkeys_keys` at `:58-70`),
  which `validate_cluster_slots` also reads (`guards.rs:715-732`). Nothing in this change alters
  key extraction. Forced by `test_multi_exec_scatter_gather_batch_is_slot_validated`,
  `test_multi_exec_eval_with_declared_keys_is_slot_validated`,
  `test_multi_exec_readonly_does_not_rescue_a_scatter_write` — all unaffected.
- **FM-TXN-029** (`:374-384`) is the relevant *precedent* for step 3, not a constraint: it pins
  that a genuinely keyless batch *serves local*. That is the policy scripting diverges from.
- **`vll-failure-modes.md`**: FM-VLL-001..005 describe shard-side observables
  (`ShardReadyResult`, BUSY/timeout replies). Making FCALL a continuation-lock *requester* adds a
  new caller of an unchanged mechanism; no observable in any row changes. One **documentation**
  edit is warranted: the Scope paragraph (`:10-17`) says the client-reply mapping *"lives in …
  `eval.rs` (`continuation_error_to_response`)"* — still true, but FCALL replies will now come
  through it too, so the sentence should say so. `just lint-failure-modes` checks row↔test
  agreement, not prose, so this edit is for accuracy rather than to unblock the lint.

**Conclusion: no `Forced by` row changes, no failing-test-before-fix ordering is imposed by the
specs.** The change is still test-first by ordinary TDD (step 0), and it still needs the locked-area
push discipline noted under Risks.

### Step 0 — the red test (the capability gap, stated as a test)

In `frogdb-server/crates/server/tests/functions.rs`, a new module with a **multi-shard** server
(`num_shards: Some(4), allow_cross_slot_standalone: true`) — the first FCALL test in the file that
is not single-shard:

```rust
// Two keys chosen with `shard_for_key` so they land on different shards
// (same technique as integration_scripting.rs:577).
async fn fcall_serves_a_cross_shard_call_that_eval_already_serves() {
    // 1. EVAL over the same two keys succeeds today — the control.
    // 2. FCALL of a function doing the same work over the same two keys
    //    currently returns CROSSSLOT.  RED.
    // 3. After the change both return the same value.
}
```

Plus its negative twin with `allow_cross_slot_standalone: false`, asserting **both** EVAL and FCALL
return `frogdb_types::redirect::CROSSSLOT_MSG` — that pins the flag as the single switch rather than
letting FCALL become unconditionally permissive.

### Step 1 — one script source type (coordinate with sibling 47)

`EvalKind` (`eval.rs:230-266`) gains a third variant:

```rust
enum ScriptSource {          // 47 renames EvalKind -> ScriptSource
    Body(Bytes),             // EVAL      -> ScriptingMsg::EvalScript
    Sha(Bytes),              // EVALSHA   -> ScriptingMsg::EvalScriptSha
    Function(Bytes),         // FCALL     -> ScriptingMsg::FunctionCall   (48)
}
```

`into_message` gains the matching arm. `ScriptingMsg::FunctionCall`'s field set
(`function_name, keys, argv, conn_id, protocol_version, read_only, response_tx`) is already
field-for-field identical to `EvalScript`'s modulo the first name, so the arm is three lines.

### Step 2 — `handle_fcall` classifies instead of validating

`function.rs:42-72` collapses to the same shape `handle_eval` has:

```rust
match self.classify_script_shards(&keys) {
    ScriptShards::Single(shard_id) =>
        self.execute_single_shard_script(ScriptSource::Function(function_name), keys, argv, shard_id, read_only).await,
    ScriptShards::CrossShard(shards) =>
        self.execute_cross_shard_script(ScriptSource::Function(function_name), keys, argv, shards, read_only).await,
    ScriptShards::CrossSlotForbidden => redirect::crossslot(),
}
```

Net effect on `function.rs`: ~30 lines of hand-rolled dispatch (L42–72) become 8, and
`use crate::slot_migration::SlotValidator` plus `use tokio::sync::oneshot` drop out of the file's
imports if nothing else there uses them. `classify_script_shards`, `execute_single_shard_script`
and `execute_cross_shard_script` need `pub(super)`/module visibility rather than private `fn`s —
they are already in the same `scripting` module (`scripting/mod.rs`), so this is a visibility
tweak inside one module, not a new seam.

### Step 3 — one keyless-target policy function

Replace both hardcoded `0`s with one named policy on `ConnectionHandler`:

```rust
/// Where a script that declares no keys runs.
///
/// Deliberately a fixed shard rather than `self.shard_id` (the policy every
/// *keyed* path uses — `routing.rs:76`, `txn/exec.rs:278`): a script cached
/// implicitly by `EVAL` lands in the executing shard's own script cache, so a
/// connection-dependent home would let `EVAL` on one connection and `EVALSHA`
/// on another disagree about whether the SHA exists.
fn keyless_script_shard(&self) -> usize { 0 }
```

**This deliberately preserves current behaviour**, and the reason is a verified hazard, not
timidity. `SCRIPT LOAD` broadcasts to every shard (`script.rs:39-52`, `ShardZeroReply` over a
fan-out — its doc comment calls out exactly this: *"a dropped/slow shard now errors instead of
returning a SHA that some shard never cached"*), but the **implicit** cache write on the `EVAL`
path is per-shard (`executor.rs:205`, `self.cache.load(...)` inside `eval_result`, executed only
on the target shard). Today keyless `EVAL` and keyless `EVALSHA` both land on shard 0, so they
agree. Switching to `self.shard_id` would make `EVAL script 0` on a connection assigned shard 1
followed by `EVALSHA sha 0` on a connection assigned shard 2 return `NOSCRIPT` — a new,
connection-assignment-dependent flake.

So step 3 buys **locality, not a behaviour change**: one function, one comment, two call sites,
and the next reader stops having to guess whether the two `0`s mean the same thing. Unifying
scripting onto `self.shard_id` requires making the implicit `EVAL` cache write fan out the way
`SCRIPT LOAD` does — **file that as a follow-up issue**, do not bundle it. (FCALL itself has no
such constraint: the function registry is process-wide, not per-shard — see `function.rs:366-371`.)

### Step 4 — docs

`website/src/content/docs/architecture/vll.md:51` — `| Lua scripts | EVAL, EVALSHA | Continuation
lock before execution |` gains FCALL/FCALL_RO. `consistency.md:49-56` needs no change; the fix is
what makes its existing table true.

### Step 5 — spec prose

`vll-failure-modes.md:10-17`, Scope: name FCALL alongside the cross-shard Lua script / MULTI as a
continuation-lock requester whose reply mapping runs through `eval.rs`'s
`continuation_error_to_response`. Run `just lint-failure-modes` after (it is part of `just lint`).

## Testability improvement

**The gap is currently untestable by construction.** Every FCALL test in
`frogdb-server/crates/server/tests/functions.rs` runs a one-shard server (L11, L687, L719, L764,
L821); a one-shard server cannot produce a cross-shard key set, so no assertion in the suite can
distinguish "FCALL refuses cross-shard" from "FCALL supports cross-shard". Step 0 introduces the
first multi-shard FCALL fixture, and that fixture is what makes the whole class of FCALL routing
questions askable.

**One classifier means one test surface for three commands.** `classify_script_shards` is the
interface both callers and tests cross. Today a test of the classifier proves something about
EVAL/EVALSHA and nothing about FCALL; after the change, the classifier's unit tests are load-bearing
for all three, and an FCALL-specific test only has to prove *"FCALL reaches the classifier"* —
one assertion instead of a re-derivation of the whole three-way lattice. That is the leverage
argument for the merge, stated as tests.

**It removes a `GATE_GAP` from the "cannot be tested from here" bucket.** hardening-2 issue 05's
prescribed forcing test is *"a cross-shard script takes the continuation lock; a second connection
issues FCALL against a held shard and must be rejected"*. That test needs a multi-shard fixture
that can hold a continuation lock and issue an FCALL — precisely what step 0 builds. 48 does not
close issue 05, but it hands 05 its harness.

**It makes the keyless policy assertable.** `keyless_script_shard` is a named function, so the
"keyless EVAL and keyless EVALSHA share a home, so the implicit script cache is coherent"
invariant becomes a one-line test (`EVAL … 0` on connection A, `EVALSHA … 0` on connection B,
expect a hit) instead of an emergent property of two unrelated literals.

## Risks / scope boundaries vs siblings

### Which crate each change lands in

| Change | Crate | Locked? |
| --- | --- | --- |
| Steps 1–3 (`function.rs`, `eval.rs` variant, keyless policy fn) | `frogdb-server` (server) | **No** |
| Step 0 tests | `frogdb-server` integration tests | No |
| Steps 4–5 (website, spec prose) | — | Spec is LOCKED but the edit is prose in the Scope paragraph, not a row |
| Shard-side gate (issue 05) | `frogdb-core` | No (core is not one of the four locked areas) |

**`frogdb-txn` and `frogdb-vll` are not edited.** `txn/exec.rs:278` is cited as the reference
policy and read only. `frogdb-vll` gains a new *caller* (`execute_cross_shard_script` already
calls `VllCoordinator::acquire_continuation_and_run`; FCALL reuses that call site verbatim) but no
source change. **Therefore: no `just mutants-gate frogdb-txn 0.90` re-gate and no
`frogdb-vll` re-gate is required by this proposal.** If the implementation ends up touching
either crate — it should not — the push discipline is `just mutants-diff <crate>` before pushing,
then the full `just mutants <crate>` + `just mutants-gate <crate> <gate>` for the area's number.

### File ownership across round-38 scripting siblings

| Proposal | Owns (must not be edited by the others) | Overlap with 48 |
| --- | --- | --- |
| **48** (this) | `connection/scripting/function.rs` L14–73; the new `keyless_script_shard` policy fn; `server/tests/functions.rs` multi-shard module; `vll-failure-modes.md` Scope paragraph; `website/.../vll.md:51` | — |
| **47** eval-orchestration-dedup | `connection/scripting/eval.rs` orchestration: `handle_eval`/`handle_evalsha` bodies, the `EvalKind`→`ScriptSource` rename, `handle_script(ScriptSource)` | **Direct dependency, not a conflict.** 48 needs exactly one new enum variant + one `into_message` arm + a visibility bump on three `fn`s in a file 47 is rewriting. **Land 47 first**, then 48 adds `Function(Bytes)`. If 47 slips, 48 can land against today's `EvalKind` with a two-line variant add and 47 rebases — but not both at once. 48 must not touch `classify_script_shards`' body. |
| **49** function-registry-surface | `frogdb-scripting` registry internals (`FunctionRegistry`, `FunctionLibrary` field privacy, `matches_pattern`), and `function.rs`'s `handle_function_list`/`handle_function_stats` renderers | **Same file, disjoint regions.** 49 owns `function.rs` L76–313 (the `FUNCTION` subcommands); 48 owns L14–73 (`handle_fcall`). No shared lines. Either order; expect a trivial import-block conflict at L1–10 if both remove imports. |
| **H7 hotfix** (separately landing) | wires a real metrics sink into `eval.rs`'s continuation path, replacing `NoopMetricsSink` (`eval.rs:131`) | **Expect drift.** H7 lands in `execute_cross_shard_script` (`eval.rs:119-169`) — the exact function 48 starts calling from a third command. By implementation time `eval.rs` will read slightly differently: `VllCoordinator::new(sink, …)` will take a real sink. 48 needs no change for that (it calls the function, not its internals), but do **not** copy `eval.rs:130-131` into any new code, and re-read the function before wiring FCALL into it. A pleasant consequence: once 48 lands, H7's metrics cover FCALL for free. |

### Other risks

- **FCALL becomes a continuation-lock owner while the shard-side gate is still open.** After 48, a
  cross-shard FCALL holds continuation locks that a *concurrent* FCALL from another connection will
  happily ignore (`dispatch_scripting.rs:77-97`, the `GATE_GAP`). That is not a regression — the
  same hole exists for cross-shard EVAL today, and `CoreMsg::ExecTransaction` has the twin — but 48
  increases the number of ways to reach it. **Land order preference: issue 05 first if it is in
  flight**; otherwise cross-reference the two in both PRs. Note the lint mechanics from issue 05's
  2026-08-07 comment: the fix must add the guard *at the dispatch arm* and move
  `ScriptingMsg::FunctionCall` from `GATE_GAP` to `GATE` in `scripts/continuation-lock-gate.py` in
  the same commit, or the gate hard-fails.
- **New failure modes become reachable for FCALL.** A cross-shard FCALL can now return
  `-ERR lock acquisition failed: …`, `-ERR lock acquisition timeout on shard N`,
  `-ERR shard dropped lock request` and `-ERR script execution failed` (`eval.rs:159`, `:268-281`)
  where it previously returned only `-CROSSSLOT`. These are the *same* strings EVAL already
  returns, so no new vocabulary enters the wire — but any client-side FCALL error handling that
  assumed a closed set widens. Pre-production; document in the PR. (Sibling lane candidates 2 and 5
  are separately proposing to unify some of these strings — 48 inherits whatever they settle on,
  which is another reason to route rather than to copy.)
- **Cluster mode is unaffected.** `allow_cross_slot` is `allow_cross_slot_standalone`
  (`subsystems.rs:559`) and cluster-mode key sets are already CROSSSLOT-checked upstream at
  `guards.rs:728-732` before dispatch ever runs. A cluster-mode FCALL still gets `-CROSSSLOT` from
  the same place it does today.
- **`ScriptingMsg::FunctionCall` under a continuation lock reaches `run_script_write_effects`**
  (`core/shard/functions.rs:111`) — the same canonical write-effect pipeline EVAL uses, per that
  line's own comment. No new write path is created.
- **Scope creep magnet.** §5 (runtime `CrossSlotTracker` for functions) and the implicit-EVAL-cache
  fan-out (step 3) are both one-file changes that will look tempting mid-implementation. Both are
  in a *different crate* from 48's diff (`frogdb-core`). File them; do not bundle.

## Effort estimate

**S–M.** Steps 1–3 are roughly −30/+15 lines in `function.rs`, +6 in `eval.rs`, +5 for the policy
fn. The work is in step 0: the first multi-shard FCALL fixture in the suite, plus its
flag-disabled twin, plus a keyless-cache coherence test. Docs and the spec Scope sentence are
minutes. No mutation re-gate. The dependency on sibling 47 is the schedule risk, not the diff.

### Independently-landable hotfix

**H48 — the keyless-target policy function (step 3 alone).** Strictly behaviour-preserving
(both call sites keep returning `0`), touches only `frogdb-server`, has no dependency on sibling 47,
and conflicts with nothing: replace `eval.rs:71`'s `Single(0)` and `function.rs:45`'s
`unwrap_or(0)` with `self.keyless_script_shard()`, carrying the comment that records *why* it is a
fixed shard (the per-shard implicit `EVAL` cache) — which is currently written down nowhere and is
the single fact a future reader is most likely to get wrong. Ships with the keyless-cache
coherence test from the Testability section.

The capability gap itself (steps 1–2) has **no smaller safe hotfix**: honouring `allow_cross_slot`
in `handle_fcall` without also acquiring continuation locks would hand cross-shard FCALL the
routing but not the isolation — strictly worse than today's refusal. The gap closes as one change
or not at all.
