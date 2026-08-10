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
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | **Owner of this proposal's client-side change.** `handle_fcall` (L14–73). **Only L42–72 changes**: shard determination L42–47, `ScriptingMsg::FunctionCall` construction L50–59, send L61–67, receive L69–72. **L14–41 (the arg preamble) is untouched — see §"Step 2".** Nothing else in the file (the `FUNCTION` subcommands) is touched. |
| `frogdb-server/crates/server/src/connection/scripting/eval.rs` | 281 | **The module being reused, owned by sibling 47.** `classify_script_shards` (L65–84), `execute_single_shard_script` (L87–113), `execute_cross_shard_script` (L119–169), `ScriptShards` (L219–227), `EvalKind` (L230–266), `continuation_error_to_response` (L268–281). 48 adds one `ScriptSource` variant + one `into_message` arm, and bumps four items to `pub(super)` (`classify_script_shards`, `execute_single_shard_script`, `execute_cross_shard_script`, `ScriptShards`, plus `ScriptSource` itself — see §"Step 2"). It must not otherwise edit this file. |
| `frogdb-server/crates/server/src/slot_migration/validator.rs` | 168 | `SlotValidator::same_shard` (L31–44) — `Ok(None)` on empty, `Err(redirect::crossslot())` on mismatch. Unchanged; both paths already get a byte-identical CROSSSLOT reply (`frogdb-server/crates/types/src/redirect.rs:17`). |
| `frogdb-server/crates/server/src/connection/routing.rs` | 351 | The **other** keyless policy: keyless commands → `self.shard_id` (L76–78); the `same_shard` `Ok(None)` fallback → `self.shard_id` (L106–110, documented unreachable); `allow_cross_slot` gate (L124). |
| `frogdb-server/crates/txn/src/exec.rs` | 412 | **LOCKED (txn, gate 0.90) — read-only reference.** The canonical keyless-target policy: `Ok(TransactionTarget::None) => host.shard_id()` (L278), with its rationale at L268–277. No edit proposed here. |
| `frogdb-server/crates/core/src/shard/dispatch_scripting.rs` | 116 | The shard-side twin of the asymmetry: `EvalScript` gates at L17, `EvalScriptSha` at L42, `FunctionCall` (L77–97) does not. **Not this proposal's edit** — see hardening-2 issue 05 below. |
| `scripts/continuation-lock-gate.py` | 458 | Seam lint. `GATE` set L96–102 (names `ScriptingMsg::EvalScript`, `EvalScriptSha`, `ScriptSubCommand`); `GATE_GAP` L136–150 pins `ScriptingMsg::FunctionCall` (L144–150) as a *known warn-not-fail bypass*. |
| `.scratch/hardening-2/issues/open/05-functioncall-bypasses-the-vll-continuation-gate.md` | 62 | The already-filed, `ready-for-agent` issue for the shard-side half. 48 is its client-side complement; the two are independent (see Risks). |
| `frogdb-server/crates/core/src/shard/functions.rs` | 118 | `handle_function_call` (L11–117): registry lookup, `no-cluster` enforcement (L55), `FCALL_RO` write rejection, OOM check, `execute_function`, `run_script_write_effects` (L111). Unchanged by 48. |
| `frogdb-server/crates/core/src/scripting/executor.rs` | 1215 | `execute_script` seeds the runtime tracker from the shebang (L318–333); `execute_function` passes `enforce_cross_slot = false` + `CrossSlotTracker::new(None)` (L525–529). **Adjacent verified divergence — out of scope, see §5.** |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | LOCKED. FM-TXN-021 (L278–288), FM-TXN-029 (L374–384), FM-TXN-030 (L386–396), deviations table L649. **Analysed below: none pins FCALL; no spec-first requirement.** |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1573 | **LOCKED (cluster, gate 0.80) — and it does name FCALL.** FM-CLUSTER-030 (L503–513) covers the whole bare-script family *by name*, `FCALL`/`FCALL_RO` included (L505). Analysed below: **checked, not violated, not edited.** The phase-2b deferral note (L1365–1372) records the FCALL continuation-lock bypass as a deliberately unrowed, known-leaky seam — quoted under Risks. FM-CLUSTER-094 (L1323–1333, a script in flight across a handoff) was also read: cluster-mode only, `SCRIPT`-flag/pause-gate mechanics, untouched. |
| `.scratch/hardening/specs/replication-failure-modes.md` | 1571 | **LOCKED (replication, gate 0.85) — also names FCALL.** FM-REPLICATION-054 (L1259–1275) is the authority for "the function registry is process-wide, one per node, not shard-tagged" (Invariant L1268: the frame is `CONTROL_SHARD` *because* the registry is per-process). FM-REPLICATION-055 (L1277) and FM-REPLICATION-056 (L1298–1306) name `FCALL`/`FCALL_RO` in Observables. All three concern *library propagation*, not call routing; **untouched, no edit, no row change**. |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED. **L12** is the load-bearing sentence — it enumerates the continuation lock's takers as "a cross-shard Lua script / MULTI", which excludes FCALL. L14–17 names the client-reply mapper's home. One prose edit, **coordinated with sibling 46, which owns both spans** (see §Proposed change step 5). |
| `frogdb-server/crates/server/tests/functions.rs` | 866 | Every FCALL test starts a **one-shard** server (`num_shards: Some(1)` at L11, and again at L687/L719/L764/L821). The gap is invisible to the suite by construction. |
| `frogdb-server/crates/server/tests/cluster_slots.rs` | 1872 | **LOCKED-row forcing tests, read-only.** `test_bare_script_family_on_non_owner_returns_moved` (tag L1758, fn L1769–1827) drives `FCALL`/`FCALL_RO` with `numkeys 1` at a non-owner and asserts MOVED; `test_keyless_script_is_never_redirected` (tag L1830, fn L1838–1871) pins the `numkeys 0` negative (via `EVAL` and `SCRIPT LOAD` — it does **not** exercise FCALL). Both carry `// FM-CLUSTER-030`. **Unaffected by 48** — single-key/keyless only, and the verdict is produced upstream of `ConnectionCommand`. Their tags stay and they must pass unchanged. |
| `frogdb-server/crates/server/tests/integration_replication_functions.rs` | 216 | Every call is `FCALL`/`FCALL_RO … 0` (L50, L64, L145, L173) — keyless throughout. Forces FM-REPLICATION-054/055/056. **Unaffected:** `keyless_script_shard()` keeps returning `0`. |
| `frogdb-server/crates/scripting/src/function.rs` | — | `FunctionFlags` (L8–20) declares `ALLOW_CROSS_SLOT_KEYS` (L18), but `from_strings` (L24–35) — the only parser the `FUNCTION LOAD` path reaches, via `parser.rs:148` — **rejects** `allow-cross-slot-keys` with `Unknown flag`. See §5. |
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
| Keys span shards, `allow_cross_slot == false` | classified `CrossSlotForbidden` (`eval.rs:82`), replied `redirect::crossslot()` at both call sites (`eval.rs:61`, `:214`) | `redirect::crossslot()` (via the `Err` payload, `function.rs:46`) |
| Continuation lock | `execute_cross_shard_script` → `VllCoordinator::acquire_continuation_and_run` (`eval.rs:119-169`) | **none** |
| Dropped-oneshot reply | `"ERR shard dropped request"` single-shard (`eval.rs:111`) / `"ERR script execution failed"` cross-shard (`eval.rs:159`) | `"ERR shard dropped request"` (`function.rs:71`) |
| Lock-failure replies | `continuation_error_to_response` (`eval.rs:268-281`) | unreachable |

The deletion test settles the shape: delete `classify_script_shards` and the same three-way
decision reappears at three call sites (`handle_eval`, `handle_evalsha`, `handle_fcall`). It is
earning its keep — `handle_fcall` is simply not calling it.

**What is *not* duplicated: the arg preamble.** `function.rs:14-41` and `eval.rs:19-38` parse the
same grammar but produce **three different client-visible strings**, all of them FCALL's own and all
of them deliberate:

| Condition | `handle_eval` | `handle_fcall` |
| --- | --- | --- |
| Wrong arity (dead — see 47 §1) | `ERR wrong number of arguments for 'eval'/'evalsha' command` | `ERR wrong number of arguments for 'fcall'/'fcall_ro' command` (`function.rs:15`, `:18-21`) |
| `numkeys` negative | `parse::<usize>` fails → `ERR value is not an integer or out of range` (`eval.rs:30`) | `parse::<i64>`, `n < 0` → **`ERR Number of keys can't be negative`** (`function.rs:28`) |
| `numkeys` unparseable | `ERR value is not an integer or out of range` (`eval.rs:30`) | **`ERR Bad number of keys provided`** (`function.rs:30`) |
| `numkeys` > available args | `ERR Number of keys can't be greater than number of args` | same string (`function.rs:35`) |

The split mirrors Redis, whose `fcallCommandGeneric` spells its own numkeys errors (`Bad number of
keys provided` / `Number of keys can't be negative`) rather than reusing `evalGenericCommand`'s
integer-parse message — *recalled upstream detail, not verified in this repo; treat the repo facts
above as the binding ones.* Either way the strings are live on the wire today.

**48 shares only the classification and the dispatch that follow the parse.** `function.rs:14-41`
stays byte-for-byte as it is; FCALL does **not** call 47's `handle_eval_family` (which does its own
EVAL-flavoured parse) — it calls `classify_script_shards` + the two executors directly, with
`function_name`/`keys`/`argv` already in hand. Merging the preambles would be a wire-visible
regression, not a dedup, and is explicitly out of scope.

### 2. The capability gap is live and user-visible

`self.allow_cross_slot` is `config.server.allow_cross_slot_standalone`
(`frogdb-server/crates/server/src/server/subsystems.rs:559` →
`frogdb-server/crates/server/src/acceptor.rs:113/175` →
`frogdb-server/crates/server/src/connection.rs:296`). `eval.rs:73` is its
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
flag (`FunctionFlags::ALLOW_CROSS_SLOT_KEYS`,
`frogdb-server/crates/scripting/src/function.rs:18`; parsed on the EVAL shebang path at
`frogdb-server/crates/core/src/scripting/executor.rs:56`; policy documented at
`core/src/scripting/gate.rs:24-45`), which is a *runtime undeclared-key* rule and orthogonal to the
shard-routing decision this proposal is about. On the **function** path that flag is not merely
unread — it is **unsettable**: see §5. The repo's cluster-mode FCALL regression tests
(`frogdb-server/crates/redis-regression/tests/cluster_scripting_regression.rs`,
`frogdb-server/crates/redis-regression/tests/cluster_scripting_tcl.rs:165`) exercise only the
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
function with the same flags is not.

The divergence is **worse than "no reader"** — the flag is also **unwritable on the function path**,
so a library author cannot even ask for the behaviour:

- `FunctionFlags::ALLOW_CROSS_SLOT_KEYS` exists (`frogdb-server/crates/scripting/src/function.rs:18`)
  and round-trips through `to_strings`.
- `FunctionFlags::from_strings` (`:24-35`) accepts exactly four names — `no-writes`, `allow-oom`,
  `allow-stale`, `no-cluster` — and returns `Err(Unknown flag: …)` for everything else, including
  `allow-cross-slot-keys`.
- `from_strings` is the **only** parser the `FUNCTION LOAD` path reaches
  (`frogdb-server/crates/scripting/src/parser.rs:148`), so
  `redis.register_function{ …, flags = { 'allow-cross-slot-keys' } }` fails the load outright.
- The single site that *can* set the bit is the shebang parser at
  `frogdb-server/crates/core/src/scripting/executor.rs:56` — the **EVAL** path only.

So the fix, when it is written, is two-sided: make the flag **settable** (add the arm to
`from_strings`) *and* **read** (derive `enforce_cross_slot` / `CrossSlotTracker` from the function's
flags in `execute_function`, as `execute_script` already does). This is a genuine Redis-parity
divergence in the *runtime undeclared-key* rule, distinct from the *declared-key routing* rule this
proposal fixes, and it lives in `frogdb-scripting` + `frogdb-core` rather than the connection layer.
**File it as a separate issue**; bundling it doubles 48's blast radius and spans two more crates.

## Proposed change

### Is this spec-first? No — with the analysis stated precisely

**Retraction.** An earlier draft of this section claimed "no spec mentions FCALL". That claim was
scoped to `txn-failure-modes.md` and `vll-failure-modes.md` and is **false as a global statement**.
`rg -ni fcall .scratch/hardening/specs/` returns **eight** hits across two further LOCKED specs:
`cluster-failure-modes.md:505` and `:1367`, and `replication-failure-modes.md:1264`, `:1265`,
`:1271`, `:1283`, `:1303`, `:1304`. All six specs are now enumerated below. The **conclusion is
unchanged** — no row's Trigger/Observable/NOT-observable/Invariant changes, no `Forced by` cell
moves, and the only spec edit is one prose sentence in `vll-failure-modes.md` — but it now rests on
a complete sweep rather than a partial one.

The lane flagged FM-TXN-021 and FM-TXN-030. Both were re-read in full; **neither pins FCALL's
refusal**, and no row in `txn-failure-modes.md` or `vll-failure-modes.md` mentions FCALL,
`FunctionCall`, or functions at all (`rg -i 'fcall|function'` over those two specs returns only
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
- **FM-CLUSTER-030** (`cluster-failure-modes.md:503-513`, LOCKED, cluster gate 0.80) — *"A bare
  script is redirected, never executed on a non-owner"*. **This row names `FCALL` and `FCALL_RO`
  explicitly** (Trigger, `:505`), so it must be checked rather than assumed away. It is **not
  violated**:
  - Its Invariant (`:509`) rests on three things 48 does not touch — `is_cluster_exempt` refusing to
    exempt `ConnectionLevelOp::Scripting` (`guards.rs:696-713`), the `MUST_PRECEDE` pair
    `(ClusterSlotValidation, ConnectionCommand)` in `PRE_DISPATCH_ORDER`, and `validate_cluster_slots
    → coordinator.route` as the single verdict source. 48 edits **none** of them; the MOVED verdict
    is produced *upstream* of `ConnectionCommand`, which is the stage that dispatches the scripting
    family and therefore the only stage 48 lives in.
  - Its keyless clause (Observable, `:506`: *"A script that declares `numkeys 0` … still runs on any
    node"*) is a **node-routing** statement, not a shard-selection one. `keyless_script_shard()`
    returns `0` exactly as `function.rs:45` does today (step 3 is behaviour-preserving by
    construction), and internal shard choice is invisible to the redirect decision either way.
  - Its four `Forced by` tests — `test_bare_eval_on_non_owner_returns_moved`,
    `test_bare_script_family_on_non_owner_returns_moved` (`cluster_slots.rs:1769`, the one that
    drives FCALL/FCALL_RO), `test_keyless_script_is_never_redirected` (`:1838`), and
    `load_bearing_ordering_invariants` — **keep their `// FM-CLUSTER-030` tags and must pass
    unchanged**. Confirm that in the PR; do not re-tag or re-scope them.
  - The blast radius stays standalone-only regardless: `allow_cross_slot` *is*
    `allow_cross_slot_standalone`, and cluster-mode key sets are CROSSSLOT-checked at
    `guards.rs:728-732` before dispatch (see Risks).
  - FM-CLUSTER-094 (`:1323-1333`, a script in flight across a handoff) was read for the same reason
    and is likewise untouched: cluster-mode, `SCRIPT`-flag/pause-gate mechanics, no shard-selection
    content.
- **`replication-failure-modes.md`** (LOCKED, replication gate 0.85) — FM-REPLICATION-054
  (`:1259-1275`), -055 (`:1277`) and -056 (`:1298-1306`) all name `FCALL`/`FCALL_RO`, and all three
  are about **library propagation**, not call routing: what crosses the link is `FUNCTION
  LOAD/DELETE/FLUSH/RESTORE`, never an `FCALL` (`:1271`: *"`FCALL` effects are not propagated as
  function calls"*). 48 changes no `FUNCTION` subcommand, no propagation, and no registry access.
  **Untouched, no edit.** FM-REPLICATION-054's Invariant (`:1268`) is also the *authoritative*
  citation for step 3's parenthetical: the frame is tagged `CONTROL_SHARD` (`u16::MAX`) rather than
  a real shard id **because the registry is per-process** — a stronger source than
  `function.rs:366-371`'s doc comment, which merely restates it locally.
- **`vll-failure-modes.md`**: FM-VLL-001..005 describe shard-side observables
  (`ShardReadyResult`, BUSY/timeout replies). Making FCALL a continuation-lock *requester* adds a
  new caller of an unchanged mechanism; no observable in any row changes. One **documentation**
  edit is warranted, in the Scope paragraph — see step 5, which also carries the coordination note
  with sibling 46. `just lint-failure-modes` checks row↔test agreement, not prose, so this edit is
  for accuracy rather than to unblock the lint.
- **`blocking-failure-modes.md`, `persistence-failure-modes.md`**: swept, zero `fcall` /
  `FunctionCall` hits. No interaction.

**Conclusion (all six LOCKED specs swept): no row is edited, no `Forced by` cell changes, and no
failing-test-before-fix ordering is imposed by the specs.** The single spec diff is one prose
sentence in `vll-failure-modes.md`'s Scope paragraph (step 5). The change is still test-first by
ordinary TDD (step 0), and it still needs the locked-area push discipline noted under Risks.

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

**Written against 47's revised shape; an earlier draft of this step did not compile against it.**
47 replaces `EvalKind` (`eval.rs:230-266`) with a **fieldless** `ScriptSource`, and 48 adds a
fieldless third variant:

```rust
/// Which cached form the script arrives in. Fieldless: the tag is chosen by the
/// entry point, *before* the payload has been parsed out of `args`.
#[derive(Clone, Copy)]
enum ScriptSource {   // 47 owns Body/Sha; 48 adds Function
    Body,             // EVAL      -> ScriptingMsg::EvalScript      { script_source }
    Sha,              // EVALSHA   -> ScriptingMsg::EvalScriptSha   { script_sha }
    Function,         // FCALL     -> ScriptingMsg::FunctionCall    { function_name }   (48)
}
```

The payload is a **sibling argument** (`script: Bytes`), taken from `args[0]` by the caller.
47's rationale — the tag must be nameable before the payload has been parsed, or the parse moves
back up into every entry point — applies to FCALL unchanged: `function.rs:25` is already
`let function_name = args[0].clone();`, structurally identical to `eval.rs:24`
(`let script_source = args[0].clone();`) and `eval.rs:177`.

`ScriptSource::into_message` gains a `Function` arm mapping the `script` argument onto
`ScriptingMsg::FunctionCall { function_name: script, .. }`. `FunctionCall`'s field set
(`function_name, keys, argv, conn_id, protocol_version, read_only, response_tx`) is already
field-for-field identical to `EvalScript`'s modulo the first name, so the arm is three lines.

### Step 2 — `handle_fcall` classifies instead of validating

**`function.rs:14-41` is untouched** — FCALL keeps its own arity/numkeys preamble and its three
distinct client-visible strings (Problem §1). Only `function.rs:42-72` changes, collapsing to the
same shape `handle_eval` has:

```rust
// L14-41 unchanged: cmd_name, arity guard, function_name, numkeys (i64, FCALL's own
// error strings), the `2 + numkeys` bound check, the keys/argv split.
match self.classify_script_shards(&keys) {
    ScriptShards::Single(shard_id) =>
        self.execute_single_shard_script(
            ScriptSource::Function, function_name, keys, argv, shard_id, read_only).await,
    ScriptShards::CrossShard(shards) =>
        self.execute_cross_shard_script(
            ScriptSource::Function, function_name, keys, argv, shards, read_only).await,
    ScriptShards::CrossSlotForbidden => redirect::crossslot(),
}
```

**Imports.** `use crate::slot_migration::SlotValidator` and `use tokio::sync::oneshot` drop out of
`function.rs` if nothing else in the file uses them (verified: `SlotValidator` appears only at
`function.rs:44`; `oneshot` only at `:50`). One import must be **added**:
`use crate::slot_migration::redirect;` — `function.rs` does not import it today, because it has
never called `redirect::crossslot()` itself (it forwards the `Err` payload `same_shard` built).

**Visibility.** Five items in `eval.rs` move from private to `pub(super)`:
`classify_script_shards`, `execute_single_shard_script`, `execute_cross_shard_script`, the
`ScriptShards` enum (matched by `handle_fcall`) and `ScriptSource` with its `Function` variant
(constructed by `handle_fcall`). `pub(super)` is exactly right here, and the reason is **not** that
the two files are the same module — they are not. `scripting/mod.rs` declares `mod eval; mod
function; mod script;`, so `eval` and `function` are **sibling child modules** under `scripting`.
`pub(super)` on an item in `eval` makes it visible in `scripting` *and all of its descendants*,
which includes `function` — the minimal visibility that works, with no new public surface and no
new seam. (`pub(crate)` would be broader than needed; plain `pub` on a private module's item would
be misleading.)

Net effect on `function.rs`: ~30 lines of hand-rolled dispatch (L42–72) become 8.

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

**Call-site count depends on order — and on whether step 2 has landed.** Two sites hardcode `0`
today (`eval.rs:71`, `function.rs:45`), but **step 2 deletes `function.rs:45` outright**: once
`handle_fcall` routes through `classify_script_shards`, the keyless decision lives in exactly one
place, inside the classifier. So:

- **H48 landing standalone (before step 2):** two call sites. `keyless_script_shard` is a
  `ConnectionHandler` method; if it is defined in `eval.rs` it needs **`pub(super)`** for
  `function.rs:45` to call it (same sibling-module reasoning as step 2). Defining it in
  `connection/mod.rs` beside the other handler state is the alternative and needs no bump —
  pick one and say so in the PR.
- **After step 2:** one call site (`eval.rs:71`), and the visibility bump is no longer needed.
  Leaving it as `pub(super)` is harmless; dropping it back to private is tidier.

So step 3 buys **locality, not a behaviour change**: one function, one comment, and the next reader
stops having to guess whether the two `0`s mean the same thing. Unifying
scripting onto `self.shard_id` requires making the implicit `EVAL` cache write fan out the way
`SCRIPT LOAD` does — **file that as a follow-up issue**, do not bundle it. (FCALL itself has no
such constraint: the function registry is process-wide, not per-shard. The authority is
**FM-REPLICATION-054**'s Invariant, `replication-failure-modes.md:1260-1273`, which states it as a
pinned property — *"one `SharedFunctionRegistry` per node"*, and the propagation frame is tagged
`CONTROL_SHARD` rather than a real shard id precisely **because** the registry is per-process;
`function.rs:366-371`'s doc comment is the same fact restated locally.)

### Step 4 — docs

`website/src/content/docs/architecture/vll.md:51` — `| Lua scripts | EVAL, EVALSHA | Continuation
lock before execution |` gains FCALL/FCALL_RO. `consistency.md:49-56` needs no change; the fix is
what makes its existing table true.

### Step 5 — spec prose (`vll-failure-modes.md` Scope paragraph)

**The load-bearing sentence is L12, not L14-17.** An earlier draft targeted only the mapping-location
sentence; that is the weaker of the two. L10-13 reads:

> Scope: the per-shard state machine and the cross-shard coordinator in
> `frogdb-server/crates/vll/src/{shard,coordinator}.rs` — specifically the **continuation lock**
> (**the shard-exclusive lock a cross-shard Lua script / MULTI takes**) and its interaction with the
> SCA scatter queue.

That parenthetical (**L12**) is an *enumeration of who takes the lock*, and after 48 it is wrong by
omission: FCALL becomes a taker. This is the sentence to edit.

L14-17 — *"the mapping to the client's reply … lives in … `scripting/eval.rs`
(`continuation_error_to_response`)"* — stays true after 48 (FCALL's continuation errors run through
that same function), so 48's edit there is only *"…and now for FCALL as well"*, and it is
**contingent**: see the coordination note.

**COORDINATION — sibling 46 owns both spans; land after it and edit its post-state.**
`46-vll-acquire-error-unify.md` claims *"Preamble scope sentence (11–12, D1), preamble
mapping-location sentence (14–17)"* (46's files table) and its spec-first step 1 rewrites both:

1. 46 §D1 / step 1.1 rewrites **L11-12** to *"a cross-shard Lua script takes (no MULTI/EXEC path
   acquires one today)"* — 46 verified that no `MULTI`/`EXEC` path acquires a continuation lock
   (`acquire_continuation_and_run` has exactly two callers: `eval.rs:134` and a test).
2. 46 step 1.3 rewrites **L14-17** to *"lives in `VllAcquireError::reply_message` (`frogdb-vll`),
   rendered by `to_response`; the hosts only log"* — i.e. **the mapping leaves `eval.rs`**.

Consequences for 48, given the lane order (HF-H + H7 → 46 → 47 → 48):

- **L12 is 48's only spec edit in the normal case.** After 46, the sentence reads "a cross-shard Lua
  script takes"; 48 amends the enumeration to include cross-shard `FCALL`/`FCALL_RO` — a *widening*
  of a sentence 46 just narrowed, on the same line. **Not a conflict but a strict sequencing
  dependency**: 48 must be written against 46's text, never against today's.
- **L14-17 needs no 48 edit once 46 has landed** — post-46 the sentence names
  `VllAcquireError::reply_message` in `frogdb-vll`, which is caller-agnostic and already true for
  FCALL. 48 edits L14-17 **only if 46 slips**, in which case the sentence still names
  `eval.rs`'s `continuation_error_to_response` and should say it now serves FCALL too.
- **No collision with 51 or 45.** 51 owns `vll-failure-modes.md:46` **only** (FM-VLL-001's
  Invariant, `ensure_initialized`); 45 owns L3, L20-23, `:69`, `:105-106`, `:108`. Both are
  disjoint from L12 and L14-17 — 45's L20-23 is the nearest, immediately below, and is a different
  paragraph.

Run `just lint-failure-modes` after (it is part of `just lint`); prose-only, so it should be a
no-op.

## Testability improvement

**The gap is currently untestable by construction.** Every FCALL test in
`frogdb-server/crates/server/tests/functions.rs` runs a one-shard server (L11, L687, L719, L764,
L821); a one-shard server cannot produce a cross-shard key set, so no assertion in the suite can
distinguish "FCALL refuses cross-shard" from "FCALL supports cross-shard". Step 0 introduces the
first multi-shard FCALL fixture, and that fixture is what makes the whole class of FCALL routing
questions askable.

**The full FCALL test inventory, and why nothing else moves.** `rg -l 'FCALL'` over the integration
and regression suites finds three more files, none of which can see this change:

| File | What it drives | Effect of 48 |
| --- | --- | --- |
| `frogdb-server/crates/server/tests/cluster_slots.rs` | `test_bare_script_family_on_non_owner_returns_moved` (L1769, `// FM-CLUSTER-030`) — FCALL/FCALL_RO with `numkeys 1` at a non-owner; `test_keyless_script_is_never_redirected` (L1838, same tag) — the `numkeys 0` negative, via EVAL and SCRIPT LOAD | **None.** Single-key and keyless only; the verdict comes from `ClusterSlotValidation`, upstream of the stage 48 edits. **These are LOCKED-row forcing tests: their tags stay and they must pass unchanged.** |
| `frogdb-server/crates/server/tests/integration_replication_functions.rs` | `FCALL`/`FCALL_RO … 0` throughout (L50, L64, L145, L173); forces FM-REPLICATION-054/055/056 | **None.** Keyless, and `keyless_script_shard()` keeps returning `0`. |
| `frogdb-server/crates/redis-regression/tests/cluster_scripting_regression.rs`, `…/cluster_scripting_tcl.rs:165` | the `no-cluster` flag, zero-key and one-key calls | **None.** Nothing pins the cross-shard refusal (Problem §2). |

So the behaviour change 48 makes is, today, asserted by **zero** tests in either direction — which
is the finding, not a comfort: step 0 exists to make it one.

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
| Steps 4–5 (website, spec prose) | — | `vll-failure-modes.md` is LOCKED but the edit is one prose sentence in the Scope paragraph (L12), not a row |
| Shard-side gate (issue 05) | `frogdb-core` | No (core is not one of the four locked areas) |

**Two further LOCKED specs were checked and are not edited**: `cluster-failure-modes.md`
(FM-CLUSTER-030 names FCALL/FCALL_RO by name — checked, not violated, its four forcing tests
unchanged) and `replication-failure-modes.md` (FM-REPLICATION-054/055/056 name FCALL — library
propagation only, untouched). Neither area's *crate* is edited, so **no `frogdb-cluster` (0.80) or
`frogdb-replication` (0.85) re-gate is required** either. See §"Is this spec-first?".

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
| **48** (this) | `connection/scripting/function.rs` **L42–72 only** (L14–41 preamble explicitly not touched); the new `keyless_script_shard` policy fn; `eval.rs`'s `ScriptSource::Function` variant + `into_message` arm + five `pub(super)` bumps; `server/tests/functions.rs` multi-shard module; `vll-failure-modes.md` **L12** (and L14–17 only if 46 slips); `website/.../vll.md:51` | — |
| **47** eval-orchestration-dedup | `connection/scripting/eval.rs` orchestration: `handle_eval`/`handle_evalsha` bodies, the `EvalKind`→`ScriptSource` rename (**fieldless**), `handle_eval_family(ScriptSource, args, read_only)` | **Direct dependency, not a conflict.** 48 needs one new *fieldless* enum variant + one `into_message` arm + a visibility bump on five items in a file 47 is rewriting. **Land 47 first**, then 48 adds `Function`. If 47 slips, 48 can land against today's `EvalKind` — but then the variant is `Function(Bytes)` and 47 rebases it to fieldless; not both at once. 48 must not touch `classify_script_shards`' body. **Name inherited from 47:** the shared orchestration is `handle_eval_family`, **not** `handle_script` — that name is taken by `scripting/script.rs:12` (SCRIPT LOAD/EXISTS/FLUSH), so the lane's original `handle_script(ScriptSource)` is a compile error. FCALL does not call `handle_eval_family` at all (it parses its own args — Problem §1); it calls `classify_script_shards` + the two executors. *(Minor: 47's text cites `eval.rs:25`/`:178` for the `args[0].clone()` lines; they are `eval.rs:24` and `:177`. Immaterial to the contract.)* |
| **49** function-registry-surface | `frogdb-scripting` registry internals (`FunctionRegistry`, `FunctionLibrary` field privacy, `matches_pattern`), and `function.rs`'s `handle_function_list`/`handle_function_stats` renderers | **Same file, disjoint regions.** 49 owns `function.rs` L76–313 (the `FUNCTION` subcommands); 48 owns L14–73 (`handle_fcall`). No shared lines. Either order; expect a trivial import-block conflict at L1–10 if both remove imports. |
| **46** vll-acquire-error-unify | `vll/src/{coordinator,types}.rs`, `scatter/executor.rs:141-193`, `eval.rs:268-281` (`continuation_error_to_response`), and in the spec **`vll-failure-modes.md` L11–12, L14–17, L29, plus FM-VLL-001..004 `Observable`/`Outcome variant`** | **Spec-span dependency, no code overlap.** 46 owns both preamble sentences 48 wants to amend and rewrites them (D1: drops the bogus MULTI claim; step 1.3: moves the reply mapping out of `eval.rs` into `VllAcquireError::reply_message`). **Land 46 first** — it is already ahead of 48 in the lane order — and write 48's L12 edit against 46's new wording. Post-46, 48's L14–17 edit becomes unnecessary. See step 5. |
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
- **The cluster spec already records this seam as known-leaky — and that is the argument for the
  land order.** `cluster-failure-modes.md:1365-1372` (the phase-2b deferral note closing the
  FM-CLUSTER-09x block) says a cross-shard script holding a VLL continuation across a slot handoff
  **has no row on purpose**: *"the continuation-lock gate has two tracked bypasses of its own
  (`MULTI`/`EXEC`, `FCALL`), so a row written now would pin the barrier's behaviour on top of a seam
  that is itself known-leaky, and it belongs with those issues rather than ahead of them."* 48 makes
  FCALL a continuation-lock *holder* while it is still one of the two named bypasses — exactly the
  configuration that note declines to specify. This does not block 48 (the note pins nothing; it
  defers), but it **strengthens "land issue 05 first"** from a preference to the thing that lets the
  deferred row finally be written: close the FCALL bypass, and one of the note's two blockers is
  gone. Cross-reference the note in the issue-05 PR.
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
- **Scope creep magnet.** §5 (the unsettable-and-unread `allow-cross-slot-keys` flag) and the
  implicit-EVAL-cache fan-out (step 3) will both look like small changes mid-implementation. Both
  are in *different crates* from 48's diff — §5 spans `frogdb-scripting` (`from_strings`) **and**
  `frogdb-core` (`execute_function`), the cache fan-out is `frogdb-core`. File them; do not bundle.

## Effort estimate

**S–M.** Steps 1–3 are roughly −30/+10 lines in `function.rs` (L42–72 only; the L14–41 preamble
stays), +4 lines and five `pub(super)` bumps in `eval.rs`, +5 for the policy fn. The work is in
step 0: the first multi-shard FCALL fixture in the suite, plus its flag-disabled twin, plus a
keyless-cache coherence test. Docs and the one spec sentence are minutes. No mutation re-gate in
any of the four locked areas. The dependencies on siblings **46 (spec span) and 47 (`ScriptSource`
shape)** are the schedule risk, not the diff.

### Independently-landable hotfix

**H48 — the keyless-target policy function (step 3 alone).** Strictly behaviour-preserving
(both call sites keep returning `0`), touches only `frogdb-server`, has no dependency on sibling 47,
and conflicts with nothing: replace `eval.rs:71`'s `Single(0)` and `function.rs:45`'s
`unwrap_or(0)` with `self.keyless_script_shard()`, carrying the comment that records *why* it is a
fixed shard (the per-shard implicit `EVAL` cache) — which is currently written down nowhere and is
the single fact a future reader is most likely to get wrong. Ships with the keyless-cache
coherence test from the Testability section. **Visibility caveat**: in this standalone form
`function.rs` calls the new method, so it needs `pub(super)` if it is defined in `eval.rs` (step 3
records the alternative). Step 2 later deletes that call site.

The capability gap itself (steps 1–2) has **no smaller safe hotfix**: honouring `allow_cross_slot`
in `handle_fcall` without also acquiring continuation locks would hand cross-shard FCALL the
routing but not the isolation — strictly worse than today's refusal. The gap closes as one change
or not at all.
