# `sig_replication`: one test per signature at the replica-stream boundary

Status: ready-for-agent

Size: M

## Why

`specs/signatures.md` (issue 03 in this directory) has a `Forced by (replication)` row for every
census signature and every one of them says `MISSING`, citing this issue. This issue closes them:
one test per signature at the replica stream, driving that signature's representative command,
living in the `sig_replication` binary so `just test-core replication` selects it.

See [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling) and
[PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint).

## What to build

1. **Read the census.** `website/src/data/signatures.json` is ground truth for which signatures
   exist, their axes, and which commands are members. Never hand-maintain a member list; take the
   representative `specs/signatures.md` names.

2. **Move existing representatives, do not tag them in place** (decision Q3 in
   [PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)). For every signature with a
   `Forced by (replication)` row, find an existing representative test in
   `frogdb-server/crates/server/tests/integration_replication.rs` and `git mv` the function into
   `frogdb-server/crates/server/tests/sig_replication.rs` (created by issue 02). Where no
   existing test covers a signature at this boundary, write a new one. `integration_replication.rs`
   shrinks to surface and edge tests; do not leave a copy behind.

3. **Tag each test** with a `// SIG-<NAME>` comment line at its definition site, per
   [PRD §3.3](../../PRD.md#33-naming-is-mechanical) — a line that is nothing but the id. The name
   in the tag, the spec section heading and the census entry are the same string.

4. **Dynamic signatures need per-member tests**, per
   [PRD §3.4](../../PRD.md#34-dynamic-buckets-are-the-one-exception-to-representative-only): one
   test per member with its own `Forced by` row, because a runtime decision, not the spec,
   determines what crosses the boundary.

5. **Replace the `MISSING` rows** in `specs/signatures.md` with the real test names and run
   `just lint-spec` until it resolves every `Forced by (replication)` row against
   `cargo nextest list`.

**What this boundary observes** ([PRD §2](../../PRD.md#2-what-exists-today-findings-2026-09-02)):
RESP-encoded command bytes (statement-shipping), a rewritten form, or nothing. So the axis under
test is `PropagateShape`: `None` (nothing reaches the replica), `Verbatim` (the bytes arrive
as sent), `Rewritten` (`SPOP` → `SREM`/`DEL`, which must deposit a `ReplicationOverride` —
`SPOP` in `frogdb-server/crates/commands/src/set.rs` is the only runtime rewriter today), and
`Control` (function-library mutators shipped on the control shard).

**Location and level.** `TestServer` level, in
`frogdb-server/crates/server/tests/sig_replication.rs`
([PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling)); adjust per test as the
boundary demands. Note `.config/nextest.toml` gives `test(integration_replication::)` a longer
slow-timeout — if the moved tests need the same headroom, add an override arm for
`binary(=sig_replication)` in the same file.

The compact suite runs on the **core profile** — `SET` stands in for `GEOADD` and `JSON.SET` at
every boundary — so no test here may require `cmd-full`.

**Run it with:**

```
just test-core replication
# = cargo nextest run -E 'package(/^frogdb-(replication|replication-runtime)$/) | binary(=sig_replication)'
```

## Acceptance criteria

- [ ] Every `Forced by (replication)` row in `specs/signatures.md` names a real test; no
      `MISSING` rows remain at this boundary
- [ ] Every such test carries a `// SIG-<NAME>` tag at its definition site and lives in the
      `sig_replication` binary
- [ ] `PropagateShape::None` signatures are forced by a test asserting *nothing* reaches the
      replica; `Rewritten` by a test asserting the rewritten form, not the original bytes
- [ ] Each Dynamic-signature member has its own test and its own `Forced by` row
- [ ] Moved tests are gone from `integration_replication.rs` — moved, not copied
- [ ] No test in the binary requires `cmd-full`
- [ ] `just test-core replication` green; `just lint-spec` green with no `MISSING` at this
      boundary; `just test frogdb-server integration_replication` still green

## Blocked by

Issue 03 in this directory. Independent of issues 04, 06 and 07 — may run in a parallel worktree.
