# Five independent `format_float` implementations — `INCRBYFLOAT` already replies one rendering and stores another

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T8
Score: aggregate of 1 finding across 5 implementation sites
Area: frogdb-commands · frogdb-types · frogdb-protocol · frogdb-core

## Context

The workspace contains five independent float-rendering functions. One of them is correct (ryu,
shortest round-trip, matching Redis's `d2string`/`fpconv_dtoa`); the rest are not, and they are
reachable from the same commands. This is already a live divergence, not a hypothetical one:
`INCRBYFLOAT` renders its reply with one implementation and stores the value with another, so
`SET k 0; INCRBYFLOAT k 0.1` replies `0.1` and `GET k` returns `0.10000000000000001`. Redis agrees
on both. The stored (ugly) string is what persists to RDB/AOF and replicates, and the error
compounds across repeated increments.

This is **one piece of work, not five site fixes**: collapse to a single implementation and add a
test asserting no second definition exists, so the sixth copy cannot be added silently. The
assertion-weak double tests that let this ship are a separate sweep — see issue 33.

## Evidence

*(all from 08/F1)*

- `crates/commands/src/utils.rs:31-62` — ryu-based, minimal round-trip representation. **This is
  the correct one.**
- `crates/types/src/types/string_value.rs:338-354` — `format!("{:.17}", f)` then
  `trim_end_matches('0')`. Used by `increment_float` at `string_value.rs:211`, i.e. on the *store*
  path.
- `crates/protocol/src/response.rs:876`
- `crates/core/src/shard/timeseries_execution.rs:352`
- `crates/commands/src/timeseries.rs:1370`

The live divergence: `crates/commands/src/string.rs:767-771` replies with
`commands::utils::format_float(new_val)` (or `Response::Double` under RESP3) while the value stored
by `sv.increment_float(delta)` was rendered by the `string_value.rs` variant. Existing regression
tests miss it because they use only exactly-representable values —
`crates/redis-regression/tests/incr_tcl.rs:169-240` uses `1`, `0.25`, `1.5`, `17179869184`, and
`crates/server/tests/property_tests.rs:210` asserts "within epsilon".

Known `{:.17}` failure modes named by the audit: `-0.0` (Redis preserves the sign; both FrogDB
variants return `"0"`), `1e-320` (subnormal — collapses to `"0"`), `1e300` (expands to 300 digits
where Redis emits `1e+300`).

## What to fix

1. Pick `crates/commands/src/utils.rs:31-62` as the single implementation and give it a home every
   crate in the list can depend on.
2. Route the other four sites through it; delete their bodies.
3. Add the invariant test: exactly one `format_float` definition exists in the workspace.
4. Add the rendering table and the reply-equals-store assertion below.

## Acceptance criteria

- [ ] A unit test over a table of `f64`s including `0.1`, `3.14`, `1e-7`, `-0.0`, `1e17`, `1e-320`,
      `1e300` asserts `StringValue::increment_float` stores exactly
      `commands::utils::format_float(new_val)`. Fails today.
- [ ] A `shard_driver` test: `SET k 0` → `INCRBYFLOAT k 0.1` → the reply bytes equal the `GET k`
      bytes. Fails today.
- [ ] A test asserts there is exactly one `format_float` definition in the workspace (a source
      scan, or a compile-time re-export check) and fails if a second is added.
- [ ] `-0.0` renders with its sign, `1e-320` does not render as `"0"`, and `1e300` does not render
      as 300 digits.
- [ ] The four non-canonical sites (`types/src/types/string_value.rs:338`,
      `protocol/src/response.rs:876`, `core/src/shard/timeseries_execution.rs:352`,
      `commands/src/timeseries.rs:1370`) no longer contain their own rendering logic.

## Test boundary

**Level 1** for the rendering table and the single-definition assertion — pure rendering, no engine
needed, and the anti-pattern would be proving a formatting bug through a socket. **Level 3** for
the reply-equals-store assertion, which needs real command dispatch and a real store to compare the
wire reply against the persisted bytes, but nothing from the connection layer.

## Depends on

Nothing. The fix lands in `types`/`commands`; area 08 surfaced it because the protocol crate owns
one of the five copies. Issue 33, `.scratch/testing-improvements-round2/issues/`, owns the
`< 1e-10` assertions that let this ship — the two should land together, but neither blocks the
other.

## Resolution

Done as one piece of work with issue 55 (the live consequence). The collapse, the single-definition
guard (`just lint-format-float`, wired into `just lint`), the rendering table and the
reply-equals-store assertion are all described there — see
`.scratch/testing-improvements-round2/issues/done/55-incrbyfloat-stores-a-different-rendering-than-it-replies.md`.

Two of this issue's acceptance criteria were **not** met as written, deliberately:

- **`-0.0` renders as `"0"`, not `"-0"`.** The criterion is right about Redis's `d2string`
  (`ZSCORE`) and wrong about `ld2string(LD_STR_HUMAN)` (`INCRBYFLOAT`), which has an explicit
  "convert -0 to 0" step. A single renderer cannot do both, and having two is the defect this
  issue exists to remove. `"0"` matches what the pre-existing canonical implementation returned,
  so nothing regressed; the `ZSCORE`-of-`-0.0` divergence from Redis is pre-existing and is now
  recorded in issue 55's Resolution rather than silently carried.
- **`1e300` renders as `1e+300`** (criterion met) **and `1e-320` renders as `1e-320`, not as a
  decimal expansion** (criterion met — it does not collapse to `"0"`). Redis's `INCRBYFLOAT` would
  spell both without an exponent; that is a separate, larger change and a separate decision, noted
  in issue 55.
