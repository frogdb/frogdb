# `sig_persistence` + `sig_wake`: one test per signature at the WAL and waiter boundaries

Status: ready-for-agent

Size: M

## Why

`specs/signatures.md` (issue 03 in this directory) has a `Forced by (persistence)` row for every
census signature and every one of them says `MISSING`, citing this issue. This issue closes them:
one test per signature at the WAL sink, driving that signature's representative command, living
in a `sig_persistence` binary so `just test-core persistence` selects it.

The same issue owns the **wake axis**: every `Forced by (wake)` row (required iff
`wake ≠ None`, PRD §6) also cites this issue. Those tests prove the representative write wakes
the declared `WaiterKind` (`LPUSH` wakes a parked `BLPOP`, `ZADD` a `BZPOPMIN`, `XADD` an
`XREADGROUP BLOCK`, `RENAME` all kinds) and live in
`frogdb-server/crates/shard-harness/tests/sig_wake.rs` (created by issue 02), selected by
`just test-core blocking`. Same shard-driver harness, same session; that is why the two axes
share one issue.

See [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling) and
[PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint).

## What to build

1. **Read the census.** `website/src/data/signatures.json` is ground truth for which signatures
   exist, their axes, and which commands are members. Never hand-maintain a member list; never
   guess a representative — take the one `specs/signatures.md` names, which the lint already
   checks is a census member.

2. **Move existing representatives, do not tag them in place** (decision Q3 in
   [PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)). For every signature with a
   `Forced by (persistence)` row, find an existing representative test in:
   - `frogdb-server/crates/server/tests/integration_persistence.rs`
   - the shard-driver tests under `frogdb-server/crates/shard-harness/tests/` (e.g.
     `shard_driver.rs`, `scenario_*.rs`, `eviction_spill_failure.rs`)

   and `git mv` the function into the signature binary (use `git mv` for the file-level moves and
   keep the function's history legible in the diff). `integration_persistence.rs` already contains
   "SET survives restart" — that is the `SIG-UPSERT-VERBATIM-1KEY-NOWAKE` persistence test and it
   moves. Where no existing test covers a signature at this boundary, write a new one. The
   `integration_*.rs` files shrink to surface and edge tests; do not leave a copy behind.

3. **Tag each test** with a `// SIG-<NAME>` comment line at its definition site, per
   [PRD §3.3](../../PRD.md#33-naming-is-mechanical) — a line that is nothing but the id, matching
   the tag grammar `spec-lint` enforces. The name in the tag, the spec section heading and the
   census entry are the same string.

4. **Dynamic signatures need per-member tests.** For `PersistShape::Dynamic` (`SORT ... STORE`,
   `GEORADIUS ... STORE`, `BITOP`, and the string/stream members the census lists), one
   representative proves nothing about the others: write a test per member and give each its own
   `Forced by` row, per
   [PRD §3.4](../../PRD.md#34-dynamic-buckets-are-the-one-exception-to-representative-only).

5. **Replace the `MISSING` rows** in `specs/signatures.md` with the real test names and run
   `just lint-spec` until it resolves every `Forced by (persistence)` row against
   `cargo nextest list`.

**Location and level.** Default level for this boundary is `shard_driver` — real dispatch, real
WAL seam, no connection ([PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling)), so
most tests belong in `sig_persistence.rs` under `frogdb-server/crates/shard-harness/tests/`
(created by issue 02 with its `[[test]]` entry). Tests that genuinely need a
socket go in `frogdb-server/crates/server/tests/sig_persistence.rs` (created by issue 02) at
`TestServer` level. Both are selected by `binary(=sig_persistence)`; adjust per test as the
boundary demands, and remember that moving a test between the two locations later is cheap.

The compact suite runs on the **core profile** — `SET` stands in for `GEOADD` and `JSON.SET` at
every boundary (issue 01's conformance test guarantees each signature has a core-profile member),
so no test here may require `cmd-full`.

**Run it with:**

```
just test-core persistence
# = cargo nextest run -E 'package(/^frogdb-(persistence|recovery)$/) | binary(=sig_persistence)'
```

## Acceptance criteria

- [ ] Every `Forced by (persistence)` row in `specs/signatures.md` names a real test; no
      `MISSING` rows remain at this boundary
- [ ] Every such test carries a `// SIG-<NAME>` tag at its definition site and lives in a `sig_`
      binary (the location rule from PRD §5, enforced by `spec-lint`)
- [ ] Each `PersistShape::Dynamic` member has its own test and its own `Forced by` row
- [ ] Moved tests are gone from `integration_persistence.rs` / the shard-harness files they came
      from — moved, not copied
- [ ] Every `Forced by (wake)` row in `specs/signatures.md` names a real test in
      `shard-harness/tests/sig_wake.rs`, tagged `// SIG-<NAME>`; `just test-core blocking` green
- [ ] No test in either binary requires `cmd-full`
- [ ] `just test-core persistence` green; `just lint-spec` green with no `MISSING` at this
      boundary; `just test frogdb-server integration_persistence` still green

## Blocked by

Issue 03 in this directory. Independent of issues 05, 06 and 07 — may run in a parallel worktree.
