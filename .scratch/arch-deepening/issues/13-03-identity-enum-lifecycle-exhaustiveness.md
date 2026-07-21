# 13-03 — Phase B: identity enums for compile-time CONFIG-param lifecycle exhaustiveness

Status: ready-for-agent

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

- [ ] Each CONFIG param has a compile-time identity (enum variant), not just a `&'static str`
- [ ] Metadata ⇄ lifecycle correspondence is enforced by an exhaustive `match` (missing handler =
      compile error), so `test_param_registry_consistency`'s partition checks become redundant or
      guard only the derivation
- [ ] No change to the client-visible CONFIG GET/SET surface (golden snapshot green)
- [ ] Operator serde surface untouched; config crate stays light

## Blocked by

Best sequenced after 13-02 (root-`Config` per-struct coverage), since both touch the assembly
seam; not strictly dependent. Independent of 13-01.

## Source

Proposal 13 Implementation section, residual gap (d) — "phase B identity enums for compile-time
lifecycle exhaustiveness not done". Filed 2026-07-21.
