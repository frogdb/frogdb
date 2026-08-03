# `INCRBYFLOAT` stores a different float rendering than it replies

Status: done
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

## Resolution

**Confirmed live, fixed together with issue 26** (the collapse is the fix; doing one without the
other would have left the second renderer available to drift again).

### Root cause

Two renderers on one path. The reply came from the ryu-based
`commands::utils::format_float`; the store came from a `{:.17}`-then-trim copy in
`types/src/types/string_value.rs`. `INCRBYFLOAT`/`HINCRBYFLOAT` are the only commands that *store
the string they render*, so the two implementations were compared against each other by every
such write — and the stored spelling, not the reply, is what the WAL persists and what crosses the
replication link.

One detail the issue does not record, and which matters for reproducing it: **the bug was
order-dependent.** `INCRBYFLOAT` on a *missing* key stored `format_float(delta)` through the
canonical renderer (`commands/src/string.rs:779`) and was always correct. Only the existing-key
branch went through `StringValue::increment_float`. That is why the level-3 tests seed with
`SET k 0` first. `HINCRBYFLOAT` diverged unconditionally.

### Fix

One definition, in the crate every renderer on the path already depends on:

- **New** `crates/protocol/src/format.rs` — `frogdb_protocol::format_float`, the canonical
  renderer, byte-for-byte the previous `commands::utils` body plus explicit NaN handling and a
  comment recording why `-0.0` normalizes to `"0"` (see divergences).
- `crates/commands/src/utils.rs` → `pub use frogdb_protocol::format_float;` (ryu dependency
  dropped from `frogdb-commands`).
- `crates/types/src/types/string_value.rs` → `pub(super) use frogdb_protocol::format_float;`.
  This is the store path, and the actual bug fix.
- `crates/protocol/src/response.rs`, `crates/core/src/shard/timeseries_execution.rs`,
  `crates/commands/src/timeseries.rs` → bodies deleted, all route to the canonical one.
- **New** `just lint-format-float`, wired into `just lint`: fails if `fn format_float` is defined
  anywhere but `protocol/src/format.rs`, or more than once there. This is the "no sixth copy"
  guard; a source scan rather than a Rust test because the copies were spread over four crates
  with no common test target.

### Tests

Level 1 — `crates/protocol/src/format.rs`: `non_finite_values_render_as_redis_spells_them`,
`both_zeroes_render_as_a_bare_zero`, `integer_valued_floats_lose_the_decimal_point`,
`inexact_values_render_as_the_shortest_string_that_round_trips`,
`extreme_magnitudes_use_c_style_exponents`, `every_rendering_parses_back_to_the_value_it_came_from`.

Level 1 — `crates/types/src/types/mod.rs`, over the issue's rendering table:
`increment_float_stores_exactly_what_the_reply_renders`,
`hash_incr_by_float_stores_exactly_what_the_reply_renders`,
`repeated_increments_keep_the_stored_rendering_canonical`.

Level 3 — **new** `crates/shard-harness/tests/rendering_incrbyfloat.rs`:
`incrbyfloat_reply_bytes_equal_the_stored_bytes`,
`hincrbyfloat_reply_bytes_equal_the_stored_bytes`,
`repeated_incrbyfloat_never_drifts_from_the_reply`,
`the_resp3_double_and_the_resp2_bulk_describe_the_same_number`.

Level 4 — `crates/server/tests/property_tests.rs::test_incrbyfloat_precision` gained a byte
comparison of the reply against a following `GET` (criterion 4, see below).

**RED proof**: with the old `{:.17}`-and-trim body temporarily restored in `string_value.rs`, the
three `frogdb-types` tests fail with `left: b"0.10000000000000001"`, `right: b"0.1"`, and the
level-3 tests fail on the same pair.

### Divergences from the acceptance criteria — read these

1. **Criterion 3 (RESP3 `Double` renders identically to the RESP2 bulk) is asserted as a
   round-trip, not as byte equality.** RESP3's `,<double>\r\n` is encoded by the external
   `redis-protocol` crate straight from the `f64` (Rust `Display`), not by FrogDB's
   `format_float`. The spellings therefore differ legitimately at extreme magnitudes — `1e300` is
   `1e+300` on RESP2 and 301 literal digits on RESP3. Both parse back to the same `f64`, which is
   the property a client can rely on. Pinning byte equality would mean overriding a
   spec-conformant encoder from inside FrogDB; that is a product decision, not a bug fix, so it
   was not taken. The test states this in a comment.
2. **Criterion 4 (no remaining epsilon where a byte comparison is what matters) was satisfied by
   *adding* the byte comparison, not by removing the epsilon.** In
   `test_incrbyfloat_precision` the epsilon guards `+delta` then `-delta` not landing exactly back
   on the initial value, which is genuine f64 behaviour and has no byte-exact answer. The
   reply-equals-store property does, so it is now asserted alongside it on both increments.
3. The canonical renderer normalizes `-0.0` to `"0"`. Issue 26's criterion says Redis preserves
   the sign; that is true of Redis's `d2string` (`ZSCORE`) but **not** of `ld2string(LD_STR_HUMAN)`
   (`INCRBYFLOAT`), which contains an explicit "convert -0 to 0" step. One shared renderer cannot
   satisfy both, and splitting it would reintroduce exactly the two-implementation shape this
   issue is about. `"0"` is what the previous canonical implementation already returned, so this
   is not a behaviour change — but `ZSCORE` of a `-0.0` score replies `0` where Redis replies
   `-0`. **Pre-existing, unchanged, and now recorded.**
4. Also unchanged and pre-existing: Redis's `ld2string(LD_STR_HUMAN)` never emits scientific
   notation, so Redis's `INCRBYFLOAT k 1e-7` stores `0.0000001` where FrogDB stores `1e-7`. This
   already applied to the reply path before the collapse; the collapse made the store agree with
   the reply, which is what the issue asked for. Making both match Redis is a separate change (it
   needs a `%.17Lf`-equivalent at f64 precision) and a separate decision.
