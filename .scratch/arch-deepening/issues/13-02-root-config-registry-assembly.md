# 13-02 — Derive the registry assembly from the root `Config` struct for per-STRUCT coverage

Status: ready-for-agent

## What to build

Phase 2 of proposal 13 enforces per-**field** coverage: within any section struct that carries
`#[derive(ConfigParams)]`, every field must declare `#[param(...)]` or `#[param(skip)]` or the
derive fails to compile. But per-**struct** coverage is still unenforced. A brand-new config
section — a new field added to `frogdb_config::Config` whose type is a new struct that nobody
adds to `config_param_registry`'s hand-written assembly in
`frogdb-server/crates/config/src/params.rs` — is a **silent hole**: its fields could all be
`#[param(mutable)]` and yet never appear in the registry, because the assembly function names
each contributing section explicitly (`MemoryConfig::PARAMS`, `ServerConfig::PARAMS`, …,
spliced with `VIRTUAL_PARAMS` slices to preserve historical order).

Close the hole by making the assembly derive its list of contributing sections from the root
`Config` struct itself, rather than a hand-maintained call list. Sketch (to be refined):

- A `#[derive(ConfigSections)]` (or an extension of `ConfigParams`) on `Config` that, for each
  section field whose type implements the "has `PARAMS`" contract, contributes that type's
  `PARAMS`. A field that is a section but is deliberately not a CONFIG-param source declares that
  explicitly (e.g. `#[sections(skip)]`), so *adding a section without deciding* is a compile error.
- Reconcile with **ordering**: the current registry order is historical and interleaves virtual
  rows between section rows (see `config_param_registry`). A root-derived assembly must either
  preserve that exact order (golden snapshot is the contract) or the golden snapshot is
  deliberately re-captured to a section-grouped order — decide and document.
- A trait/marker so `pick()`-style splicing still works, or move virtual-row interleaving into
  an explicit ordering table keyed by name.

## Acceptance criteria

- [ ] Adding a new section struct to `Config` without classifying it is a compile error
      (per-struct coverage), analogous to the per-field guarantee
- [ ] `config_param_registry()` output is unchanged (golden snapshot green) OR the snapshot is
      re-captured with a documented rationale
- [ ] `docs-gen` still builds and its generated output is unchanged (or diffed intentionally)
- [ ] ADR-0001 respected: the config crate stays light (no server/RocksDB deps); operator serde
      surface untouched

## Blocked by

None — builds on the merged Phase 2. Independent of 13-01.

## Source

Proposal 13 Implementation section, residual gap (a). Filed 2026-07-21.
