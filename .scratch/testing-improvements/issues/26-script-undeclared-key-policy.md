# Undeclared-key script validation is dead code; real (CROSSSLOT-based) policy is untested and misdocumented

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: scripting

## Context

`validate_key_access` and `ScriptError::UndeclaredKey` (`frogdb-server/crates/core/src/scripting/bindings.rs:82-92`, `frogdb-server/crates/core/src/scripting/error.rs:22-23`) are `#[allow(dead_code)]` — confirmed no production caller (only exercised by an isolated unit test, `bindings.rs:404-409`). The actual script key-safety enforcement in cluster mode is CROSSSLOT-based: `ScriptCommandGate::classify` consults `cross_slot.check_all` (`frogdb-server/crates/core/src/scripting/gate.rs:213-215`), seeded from the first declared key's slot (`executor.rs:316-324`). This means a cluster/shebang script that accesses an *undeclared key in the same slot* as its declared keys is currently **allowed**, and one that accesses a key in a *different slot* gets `CROSSSLOT` — not "errors immediately on any undeclared key access" as claimed by the exclusion-header comment in `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs:19-25` ("FrogDB errors immediately on undeclared keys ... intentional behavioral diff (strict key validation)").

No test pins either the true-allow case (same-slot undeclared access succeeds) or the numkeys=0 cluster case (no declared keys, slot seed is `None`, the first *accessed* key sets the slot — `executor.rs:320-324`). The dead `validate_key_access` function is a landmine: a future developer could "fix" the misdocumented exclusion header by wiring it in, which would break the existing (and currently expected-to-succeed) standalone `numkeys=0` scripting case at `integration_scripting.rs:342` (`test_eval_undeclared_key_access_standalone`).

## What to build

- Document the true policy explicitly (code comment + this task's reference) and test it:
  - Cluster/shebang script with `numkeys=1` declaring key `{tag}a` that internally does `redis.call('get', '{tag}b')` (undeclared, same slot) → succeeds.
  - Same script pattern but `{tag}b` in a different slot → `CROSSSLOT`.
  - A `numkeys=0` shebang script that touches two different-slot keys → `CROSSSLOT` (slot seeded by first accessed key).
- Remove the dead `validate_key_access`/`ScriptError::UndeclaredKey` code, or wire it in behind an explicit, tested opt-in flag if there's a real use case — leaving it as inert dead code invites exactly the accidental-activation regression described above. Default to deletion unless a concrete use case surfaces.
- Fix the misdocumented exclusion header in `scripting_tcl.rs:19-25` to describe the real policy (CROSSSLOT-based, not "errors immediately on undeclared keys").

## Acceptance criteria

- [x] Test: cluster script, `numkeys=1`, undeclared same-slot key access succeeds.
- [x] Test: cluster script, `numkeys=1`, undeclared different-slot key access returns `CROSSSLOT`.
- [x] Test: cluster script, `numkeys=0`, two different-slot keys accessed → `CROSSSLOT`.
- [x] `validate_key_access`/`ScriptError::UndeclaredKey` are either deleted, or wired to an actual code path with tests covering the wired behavior — no `#[allow(dead_code)]` survives untouched.
- [x] `scripting_tcl.rs:19-25` exclusion header text is corrected to describe the actual CROSSSLOT-based policy.
- [x] Existing `test_eval_undeclared_key_access_standalone` (`integration_scripting.rs:342`) still passes (standalone `numkeys=0` undeclared-key access continues to succeed).

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/core/src/scripting/bindings.rs:82-92` (dead `validate_key_access`, unit test at :404-409)
- `frogdb-server/crates/core/src/scripting/error.rs:22-23` (`ScriptError::UndeclaredKey`)
- `frogdb-server/crates/core/src/scripting/gate.rs:213-215` (`ScriptCommandGate::classify`, real CROSSSLOT-based enforcement)
- `frogdb-server/crates/core/src/scripting/executor.rs:316-324,320-324` (slot seeding from first declared/accessed key)
- `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs:19-25` (misdocumented exclusion header)
- `frogdb-server/crates/server/tests/integration_scripting.rs:342` (`test_eval_undeclared_key_access_standalone`, must keep passing)

## Resolution

**Observed behavior (probed, then pinned).** FrogDB has exactly one script key-safety
policy: slot cohesion, enforced by `CrossSlotTracker` in `gate.rs`. There is *no* strict
"key must be declared in `KEYS[]`" enforcement anywhere in the live path — `validate_key_access`
was dead. Enforcement activates only when
`ScriptKeyContext::enforce_cross_slot() = is_cluster_mode && has_shebang && !allow_cross_slot_keys`
(`executor.rs:115`). The tracker is seeded from the first declared key's slot (or, for
`numkeys=0`, the first *accessed* key's slot); any later key in a different slot is rejected
with `"...Script attempted to access keys that do not hash to the same slot"`. Undeclared keys
are fine as long as they stay in the seeded slot. This matches Redis 8.6, verified against
`src/script.c::scriptVerifyClusterState` (Redis message: "Script attempted to access keys that
do not hash to the same slot"; `SCRIPT_ALLOW_CROSS_SLOT` flag = FrogDB's `allow-cross-slot-keys`
shebang flag). Note the audit's "gets CROSSSLOT" was imprecise: the surfaced error contains
"do not hash to the same slot", not the literal `CROSSSLOT` token.

**Policy pinned (tests).**
- `gate.rs` unit tests (name-based `classify`, deterministic): undeclared same-slot allowed
  when seeded; undeclared different-slot rejected when seeded; `numkeys=0` first-accessed key
  seeds the slot then a different slot rejects; different-slot allowed when enforcement off.
- `executor.rs` end-to-end tests (real Lua `#!lua` shebang scripts through `eval`,
  `is_cluster_mode=true`): `numkeys=1` undeclared same-slot access succeeds; `numkeys=1`
  undeclared different-slot access rejected; `numkeys=0` two different-slot keys rejected; plus
  two regression guards pinning that enforcement is gated on both `has_shebang` and
  `is_cluster_mode` (non-shebang cluster EVAL and standalone shebang do NOT slot-enforce).
- Existing `test_eval_undeclared_key_access_standalone` still passes.

**Wire-or-delete decision: DELETE.** Removed `validate_key_access` (`bindings.rs`) and
`ScriptError::UndeclaredKey` (`error.rs`) plus their isolated unit tests. Strict undeclared-key
rejection is not the real policy, has no Redis analogue, and (as the issue warned) wiring it in
would break the standalone `numkeys=0` case. No `#[allow(dead_code)]` remains on this path.

**Header fix.** The `scripting_tcl.rs` exclusion header no longer claims a fictitious
"strict key validation" divergence. The five excluded tests
(SORT-by-constant / SPOP / EXPIRE / INCRBYFLOAT) are actually `attach_to_replication_stream`
propagation tests (`needs:repl`, effect-based-replication model diff; SORT is `cluster:skip`
upstream) — not key-declaration tests. The header now records this and points at the gate's
policy docs. Documented the true policy in `gate.rs` module docs and the `CrossSlotTracker`
comment.

**Caveat / follow-up (out of scope).** FrogDB's cluster slot-check is gated on `has_shebang`,
so a plain (non-shebang) `EVAL` in cluster mode is *not* slot-enforced, whereas Redis enforces
slot cohesion for all cluster scripts (EVAL and FUNCTION). This is a pre-existing divergence and
was intentionally left unchanged here (the task scoped to shebang scripts + dead code + header);
worth a separate issue if EVAL-in-cluster parity is desired.
