# Undeclared-key script validation is dead code; real (CROSSSLOT-based) policy is untested and misdocumented

Status: needs-triage
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

- [ ] Test: cluster script, `numkeys=1`, undeclared same-slot key access succeeds.
- [ ] Test: cluster script, `numkeys=1`, undeclared different-slot key access returns `CROSSSLOT`.
- [ ] Test: cluster script, `numkeys=0`, two different-slot keys accessed → `CROSSSLOT`.
- [ ] `validate_key_access`/`ScriptError::UndeclaredKey` are either deleted, or wired to an actual code path with tests covering the wired behavior — no `#[allow(dead_code)]` survives untouched.
- [ ] `scripting_tcl.rs:19-25` exclusion header text is corrected to describe the actual CROSSSLOT-based policy.
- [ ] Existing `test_eval_undeclared_key_access_standalone` (`integration_scripting.rs:342`) still passes (standalone `numkeys=0` undeclared-key access continues to succeed).

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/core/src/scripting/bindings.rs:82-92` (dead `validate_key_access`, unit test at :404-409)
- `frogdb-server/crates/core/src/scripting/error.rs:22-23` (`ScriptError::UndeclaredKey`)
- `frogdb-server/crates/core/src/scripting/gate.rs:213-215` (`ScriptCommandGate::classify`, real CROSSSLOT-based enforcement)
- `frogdb-server/crates/core/src/scripting/executor.rs:316-324,320-324` (slot seeding from first declared/accessed key)
- `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs:19-25` (misdocumented exclusion header)
- `frogdb-server/crates/server/tests/integration_scripting.rs:342` (`test_eval_undeclared_key_access_standalone`, must keep passing)
