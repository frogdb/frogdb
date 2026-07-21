# Proposal 13 — Make the typed config-param registry the single source; derive `ConfigParamInfo`

## Summary (2-3 sentences)

Every runtime config parameter's identity is written down **three** times: as a flat
`ConfigParamInfo` metadata row in the config crate (`params.rs`), as a typed `ConfigParam`/`NoopParam`
lifecycle literal in the server crate (`build_typed_params`), and — for immutable ones — as a
`ParamMeta` getter (`build_param_registry`). Whether a parameter is *mutable* is decided twice and
independently (the `mutable` flag in `params.rs` **and** which server list contains it), and the one
test that guards these registries (`test_param_registry_consistency`) checks only the *name set*, not
that the mutability partition agrees — so a `mutable: true` param served only by the immutable
registry passes every test yet returns `-ERR ... is not mutable` to clients at runtime. This proposal
makes the per-parameter definitions the single source of truth for name/section/field/mutability and
*derives* `ConfigParamInfo`, starting with the minimal step of a consistency test that actually pins
the mutability/no-op partition.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/config/src/params.rs` | 510 | `config_param_registry()` (fn at L31) — flat `ConfigParamInfo` table, **61 entries** (L34–L464): name/section/field/`mutable`/`noop`. The independently-maintained metadata list. |
| `frogdb-server/crates/config/src/param.rs` | 273 | The typed `ConfigParam<T,C>` / `DynParam<C>` lifecycle abstraction (parse→validate→apply, render, propagation). Carries **only** `name` + `propagation` — no section/field/mutability metadata. |
| `frogdb-server/crates/server/src/runtime_config.rs` | 2712 | `build_param_registry` (L685–L808, **16** immutable `ParamMeta`); `build_typed_params` (L816–L1611, **31** `ConfigParam` + **14** `NoopParam` = **45** mutable); the CONFIG GET/SET/REWRITE machinery; `test_param_registry_consistency` (L2171–L2206). |
| `frogdb-server/ops/docs-gen/src/main.rs` | 324 | `build_param_lookup` (L125–L134) consumes `config_param_registry()` by `(section, field)` to enrich the generated config reference. Links the **config** crate, not the server crate. |
| `frogdb-operator/src/config_gen.rs` | 162 | Imports `frogdb_config` for the serde `Config`/section structs **only** (L12); never touches `config_param_registry`/`ConfigParamInfo`/`ConfigParam`. The ADR-0001 consumer. |
| `docs/adr/0001-operator-imports-server-config-crate.md` | 7 | The constraint: operator serializes through the server's own serde types; a rename must stay a compile error, not a runtime drift. |

## Problem (concrete verified evidence)

**The same parameter identity is declared in three places, none compiler-linked.** Take
`maxmemory`. It appears as (a) a `ConfigParamInfo` row in `params.rs:34-40`
(`name: "maxmemory", section: Some("memory"), field: Some("maxmemory"), mutable: true, noop: false`);
(b) a `ConfigParam::<u64, ConfigManager>` literal in `build_typed_params`
(`runtime_config.rs:819`) that owns its parse/validate/apply; and its immutable siblings like
`bind` appear as a `ParamMeta` in `build_param_registry` (`runtime_config.rs:691`). The `name`
string and the mutable/immutable partition are copied across these; nothing at compile time ties a
row to its lifecycle.

**Mutability is decided twice, out of band.** `ConfigManager::set` (`runtime_config.rs:1672`) resolves
a CONFIG SET in two independent steps against two independent sources:

```rust
// 1) existence + no-op + MUTABILITY gate — from the params.rs metadata table
let info = frogdb_config::config_param_registry()
    .iter().find(|p| p.name == normalized).ok_or_else(...)?;   // L1677
...
if !info.mutable {                                              // L1691
    return Err(ConfigError::ImmutableParameter(name.to_string()));
}
...
// 2) the actual apply — from the server's typed registry
let param = self.typed_param(&normalized)                      // L1703
    .ok_or_else(|| ConfigError::ImmutableParameter(name.to_string()))?;
param.set(self, value)?;
```

So a parameter is "mutable" only if BOTH `params.rs` says `mutable: true` AND a `ConfigParam` exists
in `build_typed_params`. These are two separate facts a human keeps in agreement by hand.

**The existing guard does not cover the drift.** `test_param_registry_consistency`
(`runtime_config.rs:2171`) asserts exactly three things: (1) no server name is registered by both
lists, (2) every metadata name is served by *some* server list, (3) every server name exists in the
metadata. It **never asserts that `mutable == true` ⟺ the name lives in the typed registry**, nor
that `noop == true` ⟺ the entry is a `NoopParam`. Concretely, if a new param is added as
`mutable: true` in `params.rs` but its getter is placed in `build_param_registry` (the immutable
list) — an easy mistake, since both lists take a `name` — the test passes (the name is served
exactly once and appears in both metadata and server). At runtime it clears the mutability gate at
`L1691` and then fails the typed lookup at `L1703`, returning `ImmutableParameter`: the metadata
advertises the param as settable (it shows in CONFIG HELP's "Mutable parameters" line via
`help_text`, L1782), but CONFIG SET refuses it. A metadata lie the test cannot see.

**Verified counts and current drift status (systematic comparison).** Parsing the three lists and
cross-checking the `mutable`/`noop` flag against which registry actually serves each name:

| List | Count |
| --- | --- |
| `config_param_registry()` entries (`params.rs`) | **61** (the "62nd" `ConfigParamInfo {` a naive grep sees is the `pub struct` definition at L11) |
| — of which `mutable: true` | **45** |
| — of which `mutable: false` | **16** |
| — of which `noop: true` | **14** |
| `build_param_registry` (`ParamMeta`, all immutable) | **16** |
| `build_typed_params` `ConfigParam` (mutable) | **31** |
| `build_typed_params` `NoopParam` (mutable no-op) | **14** |
| server union total | **61** (matches metadata) |

**Today there is NO live drift:** `mutable:true (45) == typed (31+14)`, `mutable:false (16) ==
legacy (16)`, `noop:true (14) == NoopParam (14)`, name sets identical. The invariant holds — but it
holds by hand, and the test that is supposed to protect it checks a *weaker* property (name-set
membership) than the property that actually matters (the mutability/no-op *partition*). The system is
one careless edit away from a silent contradiction, with a green test suite.

## Why it is shallow/fragmented (architecture vocabulary)

**One concept, three modules, no shared seam.** "A config parameter" is a single concept — its name,
where it lands in the TOML, whether it can change at runtime, and how it parses/applies. `param.rs`
already documents the *ambition* of a deep module: "The config-parameter lifecycle as a single deep
module … adding or changing a parameter is a local edit rather than a sweep across five files"
(`param.rs:1-7`). But `ConfigParam` only captures the *lifecycle* half (parse/validate/apply/render/
propagation); the *metadata* half (section, field, mutability, no-op) lives in a **separate** flat
table in a **different** position of a **different** file, and the immutable getters live in a
**third**. The deep module was built and then bypassed for half the facts.

**Mutability has no single Interface.** The fact "is this parameter runtime-mutable?" is not owned by
any one type. It is *implied* redundantly by (a) `ConfigParamInfo.mutable`, and (b) membership in
`build_typed_params` vs `build_param_registry`. There is no seam that makes the two the same
statement; a `ConfigParam` in the typed list does not *know* it is the authority on its own
mutability, and the metadata row does not *point* at its lifecycle.

**Deletion test.** Delete the `mutable` field from a `ConfigParamInfo` row (or flip it): nothing fails
to compile. Move a param's getter from `build_typed_params` to `build_param_registry` without
touching `params.rs`: nothing fails to compile, and the name-only consistency test still passes.
Behaviour changes silently at runtime. A fact whose two encodings can independently be edited with no
compiler and no test objecting is not one fact behind an interface — it is two copies with a manual
sync contract. Contrast the sibling proposal-01 `ServerWideOp` design, where a missing handler is a
`non-exhaustive patterns` compile error.

**Poor locality.** To add one mutable parameter today you edit `params.rs` (metadata row),
`build_typed_params` (lifecycle literal), and — if it is exposed in docs — you rely on `docs-gen`
reading the `(section, field)` you typed into `params.rs` matching the serde field the lifecycle
writes. Three edits, three files, two crates, coordinated by convention. The knowledge
"`maxmemory` is a mutable `memory.maxmemory` u64" is smeared, not localized.

## Proposed change (plain English)

Make each parameter's **own definition** the single source of truth for its metadata, and *derive*
`ConfigParamInfo` from it. Because the typed registry lives in the `server` crate and is generic over
a server type (`ConfigManager`) while `config_param_registry()` lives in the light `config` crate and
is consumed standalone by `docs-gen`, the full collapse straddles a crate boundary and must be staged.

**Step 1 — minimal enforcement (recommended first; pure test, no crate-boundary cost).** Strengthen
`test_param_registry_consistency` (`runtime_config.rs:2171`) so it pins the *partition*, not just the
name set. For every `ConfigParamInfo`:

- `info.mutable == true`  ⟺  `info.name` is served by `build_typed_params`;
- `info.mutable == false` ⟺  `info.name` is served by `build_param_registry`;
- `info.noop == true`     ⟺  the serving typed entry is a `NoopParam`.

This turns the drift described above (which passes today's test) into a red test the moment it is
introduced. It codifies the invariant that currently only holds by luck. It is entirely inside the
server crate's test module and cannot affect the operator or docs-gen. This is the "make the seam
load-bearing" step, cheap and immediately valuable.

**Step 2 — derive `ConfigParamInfo` (spans `config` + `server` crates, plus `docs-gen`).** Eliminate
`params.rs` as a hand-maintained table:

1. Attach the missing metadata to the per-parameter definitions. Add `section: Option<&'static str>`
   and `field: Option<&'static str>` (and a `noop` marker, already implicit in `NoopParam`) to the
   parameter abstraction in `param.rs`, and add a lightweight immutable-parameter descriptor
   (today's `ParamMeta` gains the same `section`/`field`). Mutability is then *not a field at all* —
   it is which kind of definition the parameter is (typed lifecycle = mutable; readonly descriptor =
   immutable). The fact stops being copyable because there is only one place to state it.
2. Have the server assemble the `Vec<ConfigParamInfo>` from `build_typed_params` + `build_param_registry`
   at startup, replacing the hand-written table. `config_param_registry()`'s *shape* (the
   `&[ConfigParamInfo]` the consumers iterate) is preserved.
3. Repoint `docs-gen`. Today it links the `config` crate and calls the free function
   `config_param_registry()`. Since the derived list is built from server-side registries, either
   (a) `docs-gen` consumes a server-emitted artifact, or (b) a `build.rs`/codegen step serializes the
   derived list into the config crate. Pick (a) or (b) per the crate-lightness constraint below.

After Step 2, deleting a mutable parameter's `ConfigParam` literal removes it from the derived
metadata automatically (no orphan metadata row can survive), and a param cannot be simultaneously
"metadata-mutable" and "immutable-served" because the two are the same declaration.

## Before / After

### Before — mutability stated twice, guarded by a name-only test

```rust
// params.rs — metadata table (one of 61 hand-written rows)
ConfigParamInfo { name: "maxmemory", section: Some("memory"),
                  field: Some("maxmemory"), mutable: true, noop: false },

// runtime_config.rs — lifecycle literal, in the *mutable* list by convention
Box::new(ConfigParam::<u64, ConfigManager> { name: "maxmemory", /* parse/apply… */ }),

// runtime_config.rs:2171 — the guard checks only the NAME SET
for config_param in config_params {
    assert!(server_names.contains(&config_param.name), "… missing …");   // name membership
}
// (no assertion that mutable==true  <=>  served by build_typed_params)
```

A new `mutable: true` row whose getter is (wrongly) placed in `build_param_registry` compiles, passes
this test, and returns `ImmutableParameter` at runtime.

### After — Step 1 (the partition is pinned) — illustrative test addition

```rust
let typed: Vec<&str> = ConfigManager::build_typed_params().iter().map(|p| p.name()).collect();
let legacy: Vec<&str> = ConfigManager::build_param_registry().iter().map(|p| p.name).collect();
for info in frogdb_config::config_param_registry() {
    if info.mutable {
        assert!(typed.contains(&info.name),
            "'{}' is mutable in metadata but not served by the typed registry", info.name);
        assert!(!legacy.contains(&info.name),
            "'{}' is mutable but also in the immutable registry", info.name);
    } else {
        assert!(legacy.contains(&info.name),
            "'{}' is immutable in metadata but not served by the readonly registry", info.name);
    }
}
```

### After — Step 2 (metadata derived, one declaration) — illustrative

```rust
// The parameter definition is the ONLY place its identity is stated.
ConfigParam::<u64, ConfigManager> {
    name: "maxmemory", section: Some("memory"), field: Some("maxmemory"),
    /* parse/validate/apply/render/propagation … */
}
// config_param_registry() is *built* from the typed + readonly registries; there is no
// separate `mutable`/`noop`/section/field row to drift. Mutability == "is a ConfigParam".
```

### Adding a NEW mutable parameter, file/edit count

| Step | Before | After (Step 2) |
| --- | --- | --- |
| Declare lifecycle | edit `build_typed_params` | edit `build_typed_params` (now also carries section/field) |
| Declare metadata row | **edit `params.rs`** (independent; wrong `mutable`/list mismatch is silent) | **none** — derived |
| Keep the two in sync | manual; name-only test | impossible to desync — one declaration |
| Drift failure mode | silent `ImmutableParameter` at runtime, green tests | cannot arise (single source) |

## Testability improvement

**Step 1 is itself the testability win:** it converts an invisible runtime contradiction into a unit
test that fails at build/test time, with no server boot, no socket, no multi-shard fixture — it reads
three in-process `Vec`s. Today the only way to discover a mutability/registry mismatch is to issue a
CONFIG SET against the mis-placed parameter on a running server and observe the wrong error.

**Step 2 removes the class of bug** rather than testing for it: with `ConfigParamInfo` derived, there
is no second copy of `mutable`/`noop`/`section`/`field` to diverge, so the enrichment `docs-gen`
performs (`main.rs:125`) and the CONFIG REWRITE placement (`config_updates`, `runtime_config.rs:602`)
read facts that are the same object the lifecycle owns. The existing `test_param_registry_consistency`
either becomes trivially true (single source) or is repurposed to guard the derivation itself.

The heterogeneous-type strength of `ConfigParam` (`param.rs:8-13`, the `Box<dyn DynParam>` erasure)
is untouched — this proposal adds metadata fields alongside the lifecycle, it does not disturb the
parse/validate/apply monomorphization.

## Risks / open questions

- **ADR-0001 (operator imports `frogdb-config`) — MUST NOT break the serde schema.** Verified: the
  operator (`frogdb-operator/src/config_gen.rs:12`, `tests/integration.rs`) imports only the serde
  `Config`/section structs (`MemoryConfig`, `PersistenceConfig`, `ServerConfig`, …) and
  `toml::from_str::<frogdb_config::Config>`. It does **not** reference `config_param_registry`,
  `ConfigParamInfo`, `ConfigParam`, or `DynParam`. The registry redesign is orthogonal to the serde
  schema and therefore safe for the operator **provided** the section/field structs and their
  `#[serde(rename)]` names are left exactly as-is. This proposal touches the *parameter registry*,
  not the *config section structs*; keep them separate and the operator's compile-time import surface
  is unchanged. Do not, as part of this work, rename or restructure any `Config` section type.
- **Crate-lightness (also ADR-0001).** The config crate is "deliberately light — no RocksDB/mlua/
  tantivy deps". The typed registry lives in the heavy `server` crate. So Step 2 cannot simply move
  the registry into config, and `docs-gen` (which links config, not server) cannot call a
  server-built function. Resolve via a server-emitted artifact or a codegen/`build.rs` that keeps
  `config_param_registry()` a config-crate static derived from the definitions. This is the crux that
  makes Step 2 an M–L, and the reason Step 1 is recommended standalone first.
- **`section`/`field` on `ConfigParam` crosses the same boundary.** `ConfigParam<T, C>` is generic
  over the server context `C`; its *type* is defined in config but its *instances* live in server.
  Adding `section`/`field` fields is a config-crate change (safe), but populating them happens in
  server. No operator impact.
- **`docs-gen` (`main.rs:125`) and CONFIG REWRITE (`config_updates`, L602) both key off
  `(section, field)`.** Any derivation must preserve the exact `(section, field)` pairs currently in
  `params.rs` (e.g. `min-replicas-max-lag` → `replication.min-replicas-timeout-ms`, `loglevel` →
  `logging.level`, `maxclients` → `server.max-clients`), several of which deliberately differ from the
  Redis-facing name. A mechanical audit against the current 61 rows is required before deleting the
  table.
- **Step 1 alone leaves three lists.** It does not reduce edit sites; it only makes the drift a red
  test. That is an honest, low-risk down payment — full locality requires Step 2.

## Effort estimate

**Step 1: S.** A single strengthened test in the existing `#[cfg(test)]` module of
`runtime_config.rs`; no production code changes; no crate-boundary work; verified today's registries
already satisfy the stronger invariant, so it goes green immediately and starts guarding.

**Step 2: M–L.** Spans `config` (add `section`/`field` to the parameter abstraction, keep the
`ConfigParamInfo` shape stable for consumers), `server` (assemble the derived metadata from the two
registries; delete the 61-row hand table), and `docs-gen` (repoint from the free function to the
derived source via artifact or codegen). Not S because it crosses the config↔server boundary under
the ADR's crate-lightness constraint and must reproduce all 61 `(section, field, mutable, noop)`
tuples exactly. Kept out of pure-L because the lifecycle machinery (`ConfigParam`/`DynParam`, CONFIG
GET/SET/REWRITE) is untouched and the operator's serde surface is deliberately not in scope.

## Implementation (2026-07-21)

This supersedes the Step-2 sketch above. Rather than "assemble the `ConfigParamInfo` list from the
server's two typed registries and delete the config-crate table" (which would have forced the
metadata to be *built in the heavy server crate* and re-exported to `docs-gen`, straining ADR-0001),
the metadata was made derivable **inside the light config crate** via a proc-macro on the serde
section structs. The `ConfigParamInfo` shape and the free function `config_param_registry()` are
unchanged, so `docs-gen`, the operator, and `runtime_config.rs` consumers are untouched.

### What was actually built

**A `#[derive(ConfigParams)]` proc macro** (`frogdb-server/crates/config-derive`). Attached to a
serde config-section struct, it emits `impl <Struct> { pub const PARAMS: &'static
[ConfigParamInfo]; }`. Attribute grammar:

- Struct: `#[params(section = "<toml>")]` (required) names the TOML section.
- Field (exactly one required per field — an unannotated field is a **compile error**, which is the
  whole point):
  - `#[param]` — registered, immutable (CONFIG GET only).
  - `#[param(mutable)]` — registered, runtime-mutable (CONFIG GET/SET).
  - `#[param(noop)]` — Redis-compat no-op; emits `section: None, field: None`.
  - `#[param(name = "...")]` — CONFIG name diverges from the serde field name.
  - `#[param(skip)]` — internal field, no CONFIG parameter (cannot combine with other options).
- Emitted `field:` honors serde `rename_all = "kebab-case"` **and** a field-level `#[serde(rename)]`.

**Every struct-backed section migrated** (27 section structs carry the derive; `MemoryConfig` was
the Phase-1 pilot). This includes sections with **zero** registered rows (`snapshot`, `acl`,
`admin`, `blocking`, `compat`, `debug-bundle`, `tracing`, `http`, `json`, `latency`,
`latency-bands`, `monitor`, `status`, `hotshards`, `tiered-storage`, `vll`, `cluster`, `chaos`),
which get the derive with all fields `#[param(skip)]` — the per-field coverage guarantee is the
point. Field tally: **36** registered rows + **123** `#[param(skip)]` fields. Divergent names were
reproduced exactly (`loglevel`→`logging.level`, `min-replicas-max-lag`→
`replication.min-replicas-timeout-ms`, `maxclients`→`server.max-clients`, `dir`→
`persistence.data-dir`, `persistence-enabled`→`persistence.enabled`, `slowlog-*` prefix,
`metrics-enabled`/`metrics-port`, the `tls-*` renames like `tls-cert-file`→`cert-file`,
`tls-auth-clients`→`require-client-cert`, `tls-protocols`→`protocols`).

**Nested sub-struct decision:** `RotationConfig` (reached through `LoggingConfig.rotation:
Option<RotationConfig>`) has no individually TOML-visible CONFIG params, so the parent field is
`#[param(skip)]` and the sub-struct is deliberately **not** derived. Enum field types
(`SortedSetIndexConfig`, `LogOutput`, `TlsProtocol`, `ClientCertMode`, …) are not structs and are
skipped at their owning field. No struct was unable to take the derive.

**Virtual params are now the single home** of the 25 `field: None` rows. `HAND_ROWS` was deleted;
`VIRTUAL_PARAMS` (unchanged, 25 rows) is the only copy. `config_param_registry()` reproduces the
**exact historical order** — which interleaves individual section rows with virtual rows — by
splicing: `extend_from_slice` for whole-section contiguous runs (`memory`, `replication`,
`slowlog`, `metrics`), a `pick(SectionPARAMS, "name")` helper for rows scattered through the order
(`logging`, `server`, `persistence`, `security`, and all of `tls` — whose field order differs from
the historical order), and three slices of `VIRTUAL_PARAMS` (`[0..2]`, `[2..8]`, `[8..25]`). The
assembly is ugly on purpose; the golden snapshot is the contract.

**Golden parity:** `tests::test_registry_matches_golden_snapshot` (61 rows, exact order) was **not**
re-captured or reordered; the derived assembly matches it byte-for-byte.

**Partition test (Step 1, delivered):** `test_param_registry_consistency`
(`runtime_config.rs`) now pins the mutability/no-op partition, not just the name set:
`info.mutable ⟺ served by build_typed_params` (and never `build_param_registry`);
`!info.mutable ⟺ served by build_param_registry` (and never the typed list);
`info.noop ⟺ the serving typed entry is a NoopParam`. No-op-ness is observed via a new
defaulted `DynParam::is_noop()` (returns `false`; `NoopParam` overrides to `true`) — a test-usable
accessor that needs no downcast through the `dyn` boundary.

### Residual gaps (honest)

- **(a) Per-STRUCT coverage still unenforced.** The derive guarantees every *field* of a section is
  classified, but the assembly still names each contributing section by hand. A brand-new section
  struct added to `Config` and never wired into `config_param_registry` is a silent hole. Mitigation
  idea: a derive on the root `Config` that assembles from its section field types, so adding a
  section without classifying it is a compile error. **Future work — filed as issue 13-02.**
- **(b) The `noop` derive option is unused.** All 14 no-op params are virtual (`VIRTUAL_PARAMS`), so
  no struct field uses `#[param(noop)]`. The option is implemented and unit-tested (via
  `DeriveSmoke`) but has no production caller today.
- **(c) The 123 `#[param(skip)]` fields need a promote-or-justify audit.** `skip` currently conflates
  "genuinely internal" with "arguably should be a CONFIG param but unwired". **Filed as issue 13-01.**
- **(d) Phase B (identity enums) not done.** The lifecycle half (`build_typed_params`) is still a
  string-keyed list tied to the metadata only by the (now stronger) runtime partition test, not by
  the compiler. Compile-time exhaustiveness via a flat identity enum is deferred. **Filed as issue
  13-03.**
