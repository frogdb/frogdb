# 13-02 — Derive the registry assembly from the root `Config` struct for per-STRUCT coverage

Status: ready-for-human

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

- [x] Adding a new section struct to `Config` without classifying it is a compile error
      (per-struct coverage), analogous to the per-field guarantee
- [x] `config_param_registry()` output is unchanged (golden snapshot green) OR the snapshot is
      re-captured with a documented rationale
- [x] `docs-gen` still builds and its generated output is unchanged (or diffed intentionally)
- [x] ADR-0001 respected: the config crate stays light (no server/RocksDB deps); operator serde
      surface untouched

## Blocked by

None — builds on the merged Phase 2. Independent of 13-01.

## Source

Proposal 13 Implementation section, residual gap (a). Filed 2026-07-21.

## Implementation Notes (2026-07-21)

### Design chosen

A second derive, **`#[derive(ConfigSections)]`**, applied to the root `frogdb_config::Config`.
It is the per-struct analogue of `#[derive(ConfigParams)]`: every root field must carry either
`#[section]` (its type is a `ConfigParams` section that contributes `PARAMS`) or `#[section(skip)]`
(an internal / non-parameter field, e.g. `config_source_path`). **A field lacking both is a hard
compile error** — this is the primary acceptance criterion.

The derive emits `impl Config { pub const SECTION_PARAMS: &'static [&'static [ConfigParamInfo]]; }`
referencing each `#[section]` field's `<FieldType>::PARAMS`. That single mechanism does double duty:

1. **Compile-time completeness link.** Referencing `<FieldType>::PARAMS` in the emitted `const` means
   a `#[section]` field whose type has no `PARAMS` (i.e. is not a `#[derive(ConfigParams)]` section)
   fails to compile with `no associated item named PARAMS`. Classifying a field as a section forces it
   through the per-field coverage guarantee. (Both failure modes were verified manually — see gates.)
2. **Test hook.** `params.rs::tests::test_registry_covers_derived_sections` asserts the hand-spliced,
   order-preserving `config_param_registry()` covers **exactly** the derived section set
   (`Config::SECTION_PARAMS`, minus virtual rows). A section classified `#[section]` but never wired
   into the assembly leaves rows in `SECTION_PARAMS` yet absent from the registry → red test.

Per the issue's guidance, the derive **does not** regenerate the assembly (row order is historical and
load-bearing, pinned by the golden snapshot). It enforces coverage while the hand-spliced,
order-preserving assembly in `config_param_registry()` is left untouched. The golden snapshot was
**not** re-captured — it stays byte-identical and green.

No trybuild harness exists in this repo (confirmed: no `trybuild` dep anywhere), so — matching the
house pattern for `ConfigParams` (`DeriveSmoke`) — the compile-error behavior is documented in doc
comments on the derive and on `Config`, and exercised by positive tests (`SectionsSmoke`,
`test_derive_sections_smoke`) plus a section-count guard (`test_section_params_counts_every_section`).

### Files touched

- `frogdb-server/crates/config-derive/src/lib.rs` — new `#[proc_macro_derive(ConfigSections,
  attributes(section))]` + `expand_sections` / `parse_section_opts`; module + derive docs.
- `frogdb-server/crates/config/src/lib.rs` — `Config` now derives `ConfigSections`; every field
  annotated `#[section]` / `#[section(skip)]`; struct doc updated. Serde/schemars surface unchanged.
- `frogdb-server/crates/config/src/params.rs` — three new tests:
  `test_registry_covers_derived_sections`, `test_section_params_counts_every_section`,
  `test_derive_sections_smoke` (+ `SectionsSmoke` fixture).

Not touched (per scope): `runtime_config.rs` param lists, section struct fields/serde attrs, docs-gen.

### Gate results (all green)

- `cargo nextest run -p frogdb-config` — 102 passed, 0 failed (incl. 3 new tests; golden snapshot
  `test_registry_matches_golden_snapshot` still green, not re-captured).
- `just check` (workspace) — clean.
- `just lint frogdb-config` / `frogdb-config-derive` / `frogdb-server` — clean (`-D warnings`).
- `cargo fmt -p frogdb-config -p frogdb-config-derive --check` — clean.
- `just docs-gen-check` — all generated artifacts up to date; `git status website/` clean.
- Negative-path verification: temporarily adding an unclassified field →
  `field ... is missing a #[section] or #[section(skip)] attribute`; adding `#[section]` on a
  `PARAMS`-less type → `no associated item named PARAMS found for type ...`. Both revert clean.

ADR-0001 respected: change is confined to the light config crate + its derive; no server/RocksDB
deps added, and no section struct or serde attribute was renamed/restructured.
