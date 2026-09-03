# Crate split: extract `frogdb-command-spec` as a leaf crate

Status: needs-triage

Size: L

**Deferred; not scheduled; filed so the decision is recorded.** Ruled out of the boundary-
signatures build at the user's request in the 2026-09-02 design session (decision Q0' in
[PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)) and recorded as deferred work
in [PRD §10](../../PRD.md#10-non-goals-and-deferred-work-filed-as-issues). Nothing in issues
01–09 in this directory depends on it.

## Why

The command-classification metadata — `CommandSpec`, `WalStrategy`, `ExecutionStrategy`,
`CommandFlags`, `KeySpec`, and now `signature.rs` — lives inside `frogdb-core`. Every datatype
crate that only wants to *declare* what its commands do therefore depends on the whole engine
crate. Two costs follow:

- **Build/iteration cost.** Touching anything in `frogdb-core` rebuilds the world, which is the
  cost this whole initiative is trying to reduce.
- **Coarse change-based selection.** `scripts/affected.py`'s map (issue 08 in this directory,
  [PRD §7](../../PRD.md#7-change-based-selection-just-test-affected)) has to glob module paths
  inside `frogdb-core` — `crates/core/src/{command.rs,command_spec.rs,signature.rs,
  shard/post_execution.rs,...}` — precisely because there is no crate boundary to key on. With
  the spec extracted, the map keys on the crate graph instead, which is both sharper and
  self-maintaining.

## What to build

Extract `CommandSpec`, `WalStrategy`, `ExecutionStrategy`, `CommandFlags`, `KeySpec` and
`signature.rs` into a new leaf crate `frogdb-command-spec` with no dependency on `frogdb-core`,
and repoint the datatype crates at it. Then sharpen `scripts/affected.py`'s map from module-glob
to crate-graph granularity.

Open questions for triage before this is scheduled:

- Which of the spec's neighbours (`ReindexSpec`, `EventSpec`, `LookupSpec`, `AdminSurface`, ACL
  categories, `docs.group`) come along and which stay in `frogdb-core` — the boundary-facing
  five are named above, but `CommandSpec` is one struct and its fields travel together.
- Whether `docs-gen` and the census move with it (they consume the spec, not the engine).
- Whether the split is worth doing before or after the `KeyspaceEffect` extraction (issue 11 in
  this directory), which reshapes some of the same types.
- Measured benefit: run `just loop-cost <area>` before and after on a spike branch rather than
  arguing from first principles.

## Acceptance criteria

- [ ] Triaged: scheduled with an owner, or closed `wontfix` with the reason recorded
- [ ] If scheduled: `frogdb-command-spec` exists as a leaf crate carrying `CommandSpec`,
      `WalStrategy`, `ExecutionStrategy`, `CommandFlags`, `KeySpec` and `signature.rs`, with the
      datatype crates no longer depending on `frogdb-core` merely to declare command metadata
- [ ] If scheduled: `scripts/affected.py`'s map keys on the crate graph for the extracted types,
      and its fixtures are updated
- [ ] If scheduled: `just loop-cost` rows recorded before and after so the benefit is measured,
      not assumed

## Blocked by

Issue 01 in this directory (it creates `signature.rs`, one of the types to be extracted).
Deferred beyond that — not scheduled.
