# `sig_cluster`: one test per signature at the cluster-routing boundary

Status: ready-for-agent

Size: L

## Why

`specs/signatures.md` (issue 03 in this directory) has a `Forced by (cluster)` row for every
census signature and every one of them says `MISSING`, citing this issue. This issue closes them:
one test per signature at cluster routing, driving that signature's representative command,
living in the `sig_cluster` binary so `just test-core cluster` selects it.

This is the largest of the four area issues: `RouteShape` keeps the op in the shape (decision Q6
in [PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)), so it has the most
variants, and the existing tests are spread over eight files.

See [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling) and
[PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint).

## What to build

1. **Read the census.** `website/src/data/signatures.json` is ground truth for which signatures
   exist, their axes, and which commands are members. Never hand-maintain a member list; take the
   representative `specs/signatures.md` names.

2. **Move existing representatives, do not tag them in place** (decision Q3). For every signature
   with a `Forced by (cluster)` row, find an existing representative test in the eight cluster
   integration binaries under `frogdb-server/crates/server/tests/`:
   `cluster_topology.rs`, `cluster_slots.rs`, `cluster_migration.rs`, `cluster_failover.rs`,
   `cluster_misc.rs`, `cluster_pause_barrier.rs`, `cluster_handoff_barrier.rs`,
   `cluster_finalization_window.rs` — and `git mv` the function into
   `frogdb-server/crates/server/tests/sig_cluster.rs` (created by issue 02). Where no existing
   test covers a signature at this boundary, write a new one. The `cluster_*.rs` files shrink to
   surface and edge tests; do not leave a copy behind.
   (`cluster_finalization_window.rs` is an all-`#[ignore]` measurement harness — take nothing
   from it unless a case genuinely forces a routing verdict.)

3. **Tag each test** with a `// SIG-<NAME>` comment line at its definition site, per
   [PRD §3.3](../../PRD.md#33-naming-is-mechanical) — a line that is nothing but the id.

4. **Dynamic signatures need per-member tests**, per
   [PRD §3.4](../../PRD.md#34-dynamic-buckets-are-the-one-exception-to-representative-only).
   `RouteShape::DynamicKeys` (⇔ `MOVABLEKEYS`: `EVAL`, `SORT`, `ZUNIONSTORE`, `MIGRATE`, `XREAD`,
   JSON multi-path, ...) is the big one here — every census member gets its own test and its own
   `Forced by` row, because the key list, and therefore the slot verdict, is a runtime decision.

5. **Replace the `MISSING` rows** in `specs/signatures.md` with the real test names and run
   `just lint-spec` until it resolves every `Forced by (cluster)` row against
   `cargo nextest list`.

**Topology.** Use **single-node cluster mode** wherever the routing verdict
(`MOVED` / `ASK` / `CROSSSLOT` / `TRYAGAIN`) is computable from a fixed topology
([PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling)). Multi-node scenarios —
failover, live migration, handoff barriers — stay in the full suite in the `cluster_*.rs` files.
The point of this binary is the routing verdict per shape, not cluster liveness.

**Location and level.** `TestServer` level, in `frogdb-server/crates/server/tests/sig_cluster.rs`;
adjust per test as the boundary demands. `.config/nextest.toml` gives
`package(frogdb-server) & binary(/^cluster_/)` a 30s slow-timeout, the `cluster` test group and
`retries = 2` — `sig_cluster` does not match that filter, so decide deliberately whether to add
an override arm for it. Single-node cluster tests should not need the group cap; say so in the
file's module comment if you leave it out.

Transaction deferral and VLL are **not** separate axes: `TxnHost::deferral_of` is a pure function
of `ExecutionStrategy` and VLL sees only keys and lock mode, so `RouteShape` already determines
both ([PRD §3.1](../../PRD.md#31-axes-and-derivation)). Issue 07 in this directory covers the txn
boundary's own rows; do not duplicate them here.

The compact suite runs on the **core profile** — no test here may require `cmd-full`.

**Run it with:**

```
just test-core cluster
# = cargo nextest run -E 'package(/^frogdb-(cluster|cluster-runtime)$/) | binary(=sig_cluster)'
```

## Acceptance criteria

- [ ] Every `Forced by (cluster)` row in `specs/signatures.md` names a real test; no `MISSING`
      rows remain at this boundary
- [ ] Every such test carries a `// SIG-<NAME>` tag at its definition site and lives in the
      `sig_cluster` binary
- [ ] Each `RouteShape::DynamicKeys` member has its own test and its own `Forced by` row
- [ ] Signature tests use single-node cluster mode wherever the verdict is computable from a
      fixed topology; multi-node scenarios stayed in the `cluster_*.rs` files
- [ ] Moved tests are gone from the `cluster_*.rs` files they came from — moved, not copied
- [ ] A deliberate decision recorded (in the module comment) about the `.config/nextest.toml`
      timeout/test-group/retry treatment for `sig_cluster`
- [ ] No test in the binary requires `cmd-full`
- [ ] `just test-core cluster` green; `just lint-spec` green with no `MISSING` at this boundary;
      `just test frogdb-server cluster_` still green

## Blocked by

Issue 03 in this directory. Independent of issues 04, 05 and 07 — may run in a parallel worktree.
