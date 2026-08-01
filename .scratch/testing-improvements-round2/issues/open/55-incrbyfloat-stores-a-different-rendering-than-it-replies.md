# `INCRBYFLOAT` stores a different float rendering than it replies

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/08 F1 · MASTER.md §3
Score: severity 3 · likelihood 4 · effort 1 · priority 16
Area: frogdb-types / string_value + frogdb-commands / string

## Context

The reply path renders the new value with the ryu-based `commands::utils::format_float` (shortest
round-trip), while the store path inside `StringValue::increment_float` uses a different,
`{:.17}`-then-trim implementation. `SET k 0; INCRBYFLOAT k 0.1` therefore replies `0.1` and stores
`0.10000000000000001`; a subsequent `GET` returns a different string than the command that wrote
it, and the ugly string is what persists to RDB/AOF and replicates. It compounds across repeated
increments. Redis renders both with `d2string`/`fpconv_dtoa` and they agree. No special setup is
required — any `INCRBYFLOAT` whose result is not exactly representable, on an existing key.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- The reply uses the ryu-based `commands::utils::format_float`
  (`crates/commands/src/utils.rs:31-62`, minimal round-trip representation) —
  `crates/commands/src/string.rs:767-771`:
  ```rust
  let new_val = sv.increment_float(delta)?;
  if is_resp3 { Ok(Response::Double(new_val)) }
  else { Ok(Response::bulk(Bytes::from(format_float(new_val)))) }   // commands::utils
  ```
- The *store* inside `increment_float` uses a different function —
  `crates/types/src/types/string_value.rs:211` writing `format_float` from
  `crates/types/src/types/string_value.rs:338-354`:
  ```rust
  let s = format!("{:.17}", f);
  let s = s.trim_end_matches('0');
  ```
- So `SET k 0; INCRBYFLOAT k 0.1` replies `"0.1"` and stores `"0.10000000000000001"`.
- Three further variants exist: `crates/protocol/src/response.rs:876`,
  `crates/core/src/shard/timeseries_execution.rs:352`, `crates/commands/src/timeseries.rs:1370`.
- **Why the existing tests pass anyway**: the regression tests only use exactly-representable
  values — `crates/redis-regression/tests/incr_tcl.rs:169-240` uses `1`, `0.25`, `1.5`,
  `17179869184` — and `crates/server/tests/property_tests.rs:210` asserts "within epsilon", which
  cannot see a rendering difference.

## What to fix

1. Make `StringValue::increment_float` store exactly what the reply renders — delete
   `string_value.rs:338-354` and call the ryu implementation.
2. Collapse all five `format_float` implementations to one (theme T8): `commands/src/utils.rs:31`
   is the correct one; `types/src/types/string_value.rs:338`, `protocol/src/response.rs:876`,
   `core/src/shard/timeseries_execution.rs:352` and `commands/src/timeseries.rs:1370` go.
3. Check the RESP3 `Response::Double` path renders identically to the RESP2 bulk path.

## Acceptance criteria

- [ ] Unit test: for a table of `f64`s including `0.1`, `3.14`, `1e-7`, `-0.0`, `1e17`, `1e-320`,
      assert `StringValue::increment_float` stores exactly
      `commands::utils::format_float(new_val)` (invariant P7). **Fails today.**
- [ ] `shard_driver` test: `SET k 0` → `INCRBYFLOAT k 0.1` → assert the reply bytes equal the
      `GET k` bytes. **Fails today.**
- [ ] A follow-up test asserts there is exactly one `format_float` definition in the workspace.
- [ ] No remaining double-comparison test in this path uses an epsilon where a byte comparison is
      what matters (see proposal 08/F10).

## Test boundary

**1** for the rendering table — pure rendering, no engine needed. **3** for the reply-vs-store
equality, which needs real command dispatch and a real store but no socket; a level-4 test would
add RESP encoding without adding signal.

## Depends on

Theme T8 (five independent `format_float` implementations) — issue 26,
`.scratch/testing-improvements-round2/issues/`. This defect is the live consequence of that theme
and should land with the collapse, not before it.
