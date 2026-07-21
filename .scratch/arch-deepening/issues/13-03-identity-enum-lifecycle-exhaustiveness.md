# 13-03 — Phase B: identity enums for compile-time CONFIG-param lifecycle exhaustiveness

Status: ready-for-human

## What to build

Proposal 13 collapsed the *metadata* half of a config parameter (name/section/field/mutable/noop)
into a single derived source (`#[derive(ConfigParams)]`). The *lifecycle* half (parse → validate
→ apply → render → propagation, in `frogdb_config::ConfigParam` / `DynParam`, populated by
`build_typed_params` in `frogdb-server/crates/server/src/runtime_config.rs`) is still a separate
list of boxed trait objects keyed by string name. The two are tied together only by a **runtime
test** (`test_param_registry_consistency`, strengthened in Phase 2 to pin the
mutable/immutable/noop partition), not by the compiler.

Phase B (the "identity enum" direction sketched in the arch-deepening notes): give each CONFIG
parameter a compile-time identity — a flat enum variant — so that the mapping from identity to
{metadata row, lifecycle handler} is an exhaustive `match`. A missing or duplicated handler then
becomes a `non-exhaustive patterns` / unreachable-arm compile error rather than a red unit test,
matching the `ServerWideOp` design referenced in proposal 13's "Deletion test" section.

This is the deeper structural change proposal 13 explicitly deferred (its Step 2 stopped at
deriving the metadata; the lifecycle stayed string-keyed). Scope it against:
- the flat command-identity enum precedent (see MEMORY note
  `feedback_spec_enums_pure_identity.md`: identity enums live in core, impl flags stay in the
  server dispatch match)
- the ADR-0001 crate-lightness constraint (identity can live in the light config crate; the
  handlers stay in the heavy server crate, selected by an exhaustive match over the identity)
- the existing `Propagation` enum, which already models part of the lifecycle on the param

## Acceptance criteria

- [x] Each CONFIG param has a compile-time identity (enum variant), not just a `&'static str`
- [x] Metadata ⇄ lifecycle correspondence is enforced by an exhaustive `match` (missing handler =
      compile error), so `test_param_registry_consistency`'s partition checks become redundant or
      guard only the derivation
- [x] No change to the client-visible CONFIG GET/SET surface (golden snapshot green)
- [x] Operator serde surface untouched; config crate stays light

## Blocked by

Best sequenced after 13-02 (root-`Config` per-struct coverage), since both touch the assembly
seam; not strictly dependent. Independent of 13-01.

## Source

Proposal 13 Implementation section, residual gap (d) — "phase B identity enums for compile-time
lifecycle exhaustiveness not done". Filed 2026-07-21.

## Implementation Notes (2026-07-21)

### Enum shape chosen + why

Two flat, payload-free identity enums live in the **light config crate**
(`frogdb-server/crates/config/src/param_id.rs`), partitioned by mutability — the one fact that
decides which server lifecycle serves a param:

- `MutableParamId` — the 45 runtime-mutable params (31 real `ConfigParam` + 14 Redis-compat no-ops).
- `ImmutableParamId` — the 16 restart-required params.

Mutability is now *which enum a param is in*, not a copyable bool. Each enum is declared via a small
`param_id_enum!` `macro_rules!` table that co-locates, from a single `Variant => "wire-name"` list:
the enum, its `pub const ALL: &[Self]` roster, and `pub const fn name(self) -> &'static str`. This is
the "cleaner mechanism" the issue invited over hand-writing three parallel things — you cannot add a
variant without giving it a name, and `ALL`/`name()` are generated from the same list, so they cannot
drift. Identity is payload-free, mirroring `core`'s `ServerWideOp`/`ScatterGatherOp` (no
parse/apply/section/field on the enum).

**Why the enum can't itself be derive-generated:** the params span many section structs *plus* 25
virtual params that have no struct field at all, so a per-struct `#[derive(ConfigParams)]` (which sees
one struct at a time) can never emit one unified enum. Instead the enum is hand-written and *pinned to
the derived registry by tests*: `param_id::tests` asserts `MutableParamId::ALL` names == the registry's
`mutable: true` partition, `ImmutableParamId::ALL` names == the `mutable: false` partition, that the two
rosters are unique + disjoint and cover every registry row, and the 45/16 counts. The registry is
already derived (proposal 13), so the enums are pinned to that single source of truth.

### How the exhaustive matches look

`build_typed_params`/`build_param_registry` are now thin: `Id::ALL.iter().map(|&id|
Self::build_<one>(id)).collect()`. The bodies moved into per-id constructors that are **exhaustive
matches** over the identity:

- `build_typed_param(id: MutableParamId) -> Param` — `match id { Maxmemory => Box::new(ConfigParam
  {...}), … Save => Box::new(NoopParam {...}), … }`. A new `MutableParamId` variant with no arm is a
  `non-exhaustive patterns` compile error; a duplicated arm is an unreachable-pattern error.
- `readonly_param_meta(id: ImmutableParamId) -> ParamMeta` — same shape over the 16 immutable ids.

Each arm sets `name: id.name()` (not a re-typed literal), so the `DynParam`/`ParamMeta` wire name is
pinned to the enum identity, which is in turn pinned to the registry by the config-crate test. The
string-keyed lookup maps (`typed_param(name)` etc.) stay string-keyed internally, but their
*population* is now exhaustive over the identity roster. (One gotcha: `use MutableParamId::*` shadows
the imported `WalFailurePolicy` **type** with the same-named identity variant, so the two references in
that one arm are fully-qualified `frogdb_core::persistence::WalFailurePolicy::…`.)

### What happened to the partition test

`test_param_registry_consistency` (`runtime_config.rs`) was **repurposed**, not deleted. Its old
name-set membership + mutable/immutable partition assertions are now redundant: the exhaustive matches
guarantee every id is served, and the config-crate `param_id` tests guarantee the id rosters equal the
registry partitions (and are disjoint). What is *not* compiler-enforced is the **noop ⟺ NoopParam**
correspondence (a noop id's arm could accidentally build a real `ConfigParam`, or vice versa), so the
test now guards only that, keyed off the metadata `noop` flag and `DynParam::is_noop()`. A comment at
the top points at the compile-time guarantee for the rest.

### Edit checklist — adding a new CONFIG param (relevant to 13-01 skip audit)

Adding a param now touches **three edits, and the compiler/tests catch a missed one**:

1. **Metadata**: annotate the serde field with `#[param(...)]` (derived), or add a `VIRTUAL_PARAMS`
   row (config crate) — same as before proposal 13.
2. **Identity**: add a variant to `MutableParamId` *or* `ImmutableParamId` in
   `config/src/param_id.rs` (pick the enum = the mutability). Miss this and the config-crate
   `*_ids_match_registry_partition` test goes red (registry row with no identity), and the
   count-stability test trips.
3. **Lifecycle**: add the `match` arm — a `ConfigParam`/`NoopParam` in `build_typed_param` (mutable)
   or a `ParamMeta` in `readonly_param_meta` (immutable). Miss this and it's a **`non-exhaustive
   patterns` compile error** (the whole point). Use `name: id.name()`.

Also update the `id_counts_are_stable` numbers (45/16) and, for a mutable param, the golden snapshot
if it changes the registry's client-visible shape.

### Gate results (all green)

- `cargo nextest run -p frogdb-config` — 106 passed (incl. 4 new `param_id` tests + golden snapshot).
- `just test frogdb-server config` — 111 passed (incl. repurposed `test_param_registry_consistency`).
- `just check` (workspace) — clean.
- `just lint frogdb-config` / `just lint frogdb-server` — clean.
- `cargo fmt -p frogdb-config -p frogdb-server -- --check` — clean.
- `just docs-gen-check` — all generated artifacts up to date.

### Files touched

- `frogdb-server/crates/config/src/param_id.rs` — **new**: the two identity enums, `param_id_enum!`
  macro, and the pinning tests.
- `frogdb-server/crates/config/src/lib.rs` — `pub mod param_id;` + re-export
  `MutableParamId`/`ImmutableParamId`.
- `frogdb-server/crates/server/src/runtime_config.rs` — exhaustive-match constructors
  (`build_typed_param`/`readonly_param_meta`), thin `build_typed_params`/`build_param_registry`,
  new imports, repurposed `test_param_registry_consistency`.
