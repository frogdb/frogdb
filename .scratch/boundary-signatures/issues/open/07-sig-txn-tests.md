# `sig_txn`: one test per signature at the transaction-queueing boundary

Status: ready-for-agent

Size: M

## Why

`specs/signatures.md` (issue 03 in this directory) has a `Forced by (txn)` row for every census
signature and every one of them says `MISSING`, citing this issue. This issue closes them: one
test per signature at transaction queueing, driving that signature's representative command,
living in the `sig_txn` binary so `just test-core txn` selects it.

See [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling) and
[PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint).

## What to build

1. **Read the census.** `website/src/data/signatures.json` is ground truth for which signatures
   exist, their axes, and which commands are members. Never hand-maintain a member list; take the
   representative `specs/signatures.md` names.

2. **Move existing representatives, do not tag them in place** (decision Q3 in
   [PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)). For every signature with a
   `Forced by (txn)` row, find an existing representative test in
   `frogdb-server/crates/server/tests/integration_transactions.rs` and `git mv` the function into
   `frogdb-server/crates/server/tests/sig_txn.rs` (created by issue 02). Where no existing test
   covers a signature at this boundary, write a new one. `integration_transactions.rs` shrinks to
   surface and edge tests (`WATCH` semantics, error-in-queue behavior, `DISCARD`, ...); do not
   leave a copy behind.

3. **Tag each test** with a `// SIG-<NAME>` comment line at its definition site, per
   [PRD §3.3](../../PRD.md#33-naming-is-mechanical) — a line that is nothing but the id.

4. **Dynamic signatures need per-member tests**, per
   [PRD §3.4](../../PRD.md#34-dynamic-buckets-are-the-one-exception-to-representative-only) —
   `RouteShape::DynamicKeys` members in particular, since the key list a deferral decision is
   taken over is a runtime decision.

5. **Replace the `MISSING` rows** in `specs/signatures.md` with the real test names and run
   `just lint-spec` until it resolves every `Forced by (txn)` row against `cargo nextest list`.

**What this boundary observes.** Deferral is a pure function of `ExecutionStrategy`
(`TxnHost::deferral_of`), so `RouteShape` already determines it, and VLL sees only the key list
and lock mode ([PRD §3.1](../../PRD.md#31-axes-and-derivation)). The test per signature is
therefore "queue the representative inside `MULTI`, `EXEC`, and assert the queue/deferral/apply
behavior its `RouteShape` implies" — including the shapes that cannot be queued at all
(connection-level and server-wide ops) and the ones that fold across shards.

**Location and level.** `TestServer` level, in `frogdb-server/crates/server/tests/sig_txn.rs`
([PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling)); adjust per test as the
boundary demands.

The compact suite runs on the **core profile** — `SET` stands in for `GEOADD` and `JSON.SET` at
every boundary — so no test here may require `cmd-full`.

**Run it with:**

```
just test-core txn
# = cargo nextest run -E 'package(/^frogdb-(txn|vll)$/) | binary(=sig_txn)'
```

## Acceptance criteria

- [ ] Every `Forced by (txn)` row in `specs/signatures.md` names a real test; no `MISSING` rows
      remain at this boundary
- [ ] Every such test carries a `// SIG-<NAME>` tag at its definition site and lives in the
      `sig_txn` binary
- [ ] Shapes that cannot be queued (connection-level, server-wide) are forced by tests asserting
      the refusal/passthrough behavior, not skipped
- [ ] Each Dynamic-signature member has its own test and its own `Forced by` row
- [ ] Moved tests are gone from `integration_transactions.rs` — moved, not copied
- [ ] No test in the binary requires `cmd-full`
- [ ] `just test-core txn` green; `just lint-spec` green with no `MISSING` at this boundary;
      `just test frogdb-server integration_transactions` still green

## Blocked by

Issue 03 in this directory. Independent of issues 04, 05 and 06 — may run in a parallel worktree.
