# `BoundarySignature` projection + `signatures.json` census + core-profile conformance test

Status: ready-for-agent

Size: M

## Why

Every application boundary (WAL sink, replica stream, cluster routing / txn queueing, blocking
waiters) reads a different projection of `CommandSpec`, but nothing names the *shape* a command
presents at those boundaries. Without a name there is no way to say "this shape is tested once"
and no way for a lint to notice a new command introducing a shape nobody tests. This issue builds
the model and its ground-truth census; everything else in this directory hangs off it.

See [PRD §3](../../PRD.md#3-the-model-boundarysignature) and [PRD §4](../../PRD.md#4-census).

## What to build

**1. `frogdb-server/crates/core/src/signature.rs`** — a new module holding
`BoundarySignature { persist, propagate, route, wake }` and its four axis enums, plus
`impl CommandSpec { pub const fn signature(&self) -> BoundarySignature }`.

- The axis variants and their derivation from `WalStrategy` / `CommandFlags` / `ExecutionStrategy`
  × `KeySpec` × `requires_same_slot` / `WaiterWake` are given by the tables in
  [PRD §3.1](../../PRD.md#31-axes-and-derivation). Use the variant names in those tables; where
  the PRD leaves an internal detail open, follow the PRD §3.1 table and pick the obvious spelling.
- `RouteShape` **reuses** the existing op enums (`ScatterGatherOp`, `ConnectionLevelOp`,
  `ServerWideOp`) rather than re-declaring them, and the op stays in the shape (PRD §3.1, decision
  Q6 in [PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)).
- This is a **read-only projection**. It must change no engine behavior: the dispatcher, router,
  WAL path and replication path keep reading the spec fields they read today. No existing call
  site is rewritten to consume `signature()`.
- Deliberately excluded from the model: `EventSpec`, `ReindexSpec`, `LookupSpec`, `AdminSurface`,
  ACL categories, `docs.group` — see [PRD §3.2](../../PRD.md#32-deliberately-excluded).
- Signature **name** rendering (axes joined, upper-cased, `SIG-` prefixed —
  `SIG-UPSERT-VERBATIM-1KEY-NOWAKE`) is mechanical per
  [PRD §3.3](../../PRD.md#33-naming-is-mechanical). Implement it once, on `BoundarySignature`, so
  the census generator, the spec and the test tags cannot drift.

**2. Census output from `docs-gen`.** `frogdb-server/ops/docs-gen` (`src/main.rs`) already links
`frogdb-server` with `cmd-full` and emits `website/src/data/commands.json`, driven by
`just docs-gen` with a `just docs-gen-check` drift check (CI job "Docs Generation Check"). Add a
second output alongside it, `website/src/data/signatures.json`, in the shape given in
[PRD §4](../../PRD.md#4-census): a `_generated` header, `count`, and a `signatures` array of
`{ name, axes: { persist, propagate, route, wake }, members: [<command names>] }`. Order
deterministically (signatures by name, members by name) so `--check` is stable. `--check` must
cover the new file exactly the way it covers `commands.json`.

Expected order of magnitude is 35–45 distinct signatures over 391 commands; the real number comes
from running it. Do not hand-tune the output toward that number.

**3. Conformance test: every full-registry signature has a core-profile member.** CI's
`cmd-full` job only type-checks — no test ever runs under `cmd-full` — so this cannot be a test
over the full registry (see [PRD §4](../../PRD.md#4-census)). Instead add a normal test in
`frogdb-server` (core-profile build) next to `every_write_command_declares_wal` in
`frogdb-server/crates/server/src/server/register.rs`: compute the census of *its own* registry and
assert that the set of names in the checked-in `website/src/data/signatures.json` (the full
census) is a subset of it. The failure message should name the missing signature(s) and say that
the compact suite would have no representative for them.

The consequence this test buys is the whole point of the design: the compact suite never needs
`cmd-full`, because `SET` stands in for `GEOADD` and `JSON.SET` at every boundary.

**4. Docs.** Note the new generated file wherever `commands.json` is described (the `docs-gen`
crate docs and any website data README). The file is generated, never hand-edited.

## Acceptance criteria

- [ ] `frogdb-server/crates/core/src/signature.rs` exists with `BoundarySignature`, its four axis
      enums, `CommandSpec::signature()`, and mechanical name rendering per PRD §3.1/§3.3
- [ ] No engine behavior changed — `signature()` has no call sites outside the census generator,
      the conformance test, and tests
- [ ] `just docs-gen` writes `website/src/data/signatures.json` with `_generated`, `count` and
      `signatures[]` (`name`, `axes`, `members`), deterministically ordered
- [ ] `just docs-gen-check` fails on drift in `signatures.json`, not just `commands.json`
- [ ] The generated `signatures.json` is committed (issue 03 in this directory writes the spec
      from it, and issues 04–07 read it as the census)
- [ ] A core-profile test in `frogdb-server/crates/server/src/server/register.rs` asserts the
      checked-in full census's signature names are a subset of the core-profile registry's census
- [ ] `just check`, `just lint` and `just test frogdb-server signature` are green

## Blocked by

None.
