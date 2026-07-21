# Spec: reference/configuration.mdx
Status: update
Audiences: A4 (test-deployment operators), A5 (contributors)

Goal: The reader can find every FrogDB configuration parameter — its TOML key,
type, default, whether it is runtime-mutable via `CONFIG SET`, and what it does —
grouped by config section. They understand the TOML file format, the four config
surfaces and their precedence (cross-linked, not re-explained), and can copy a
minimal per-section example. Every parameter table is generated from the Rust
`Config` struct, so the page never drifts from the binary.

Not in scope:
- The four config surfaces / precedence / env-var mechanics in depth — that is
  the Operations → Configuration page's job (§5). This Reference page is the
  exhaustive parameter table; it links to Operations for the conceptual model,
  and does not duplicate it.
- A full copy-paste config file — that is `reference/example-config.mdx`
  (see `example-config.md`), which replaces the cut `reference-config.mdx`.
- Per-command `CONFIG SET` semantics beyond the mutable/immutable flag.
- Any hand-typed default value, type, or parameter name. If it is not in the
  generated table, it does not belong on this page.

Sources of truth (author must read):
- `frogdb-server/crates/config/src/` — the `Config` struct and its section
  structs; doc comments become descriptions, `#[derive]`d defaults become the
  `default` column.
- `frogdb-server/ops/docs-gen/src/main.rs` — the generator. It serializes
  `Config::default()`, reads the JSON Schema from `schemars`, and joins with
  `config_param_registry()` for `CONFIG GET/SET` name + mutability. Output:
  `website/src/data/config-reference.json`.
- `website/src/components/ConfigTable.astro` — renders one section's table
  (columns: Option, Type, Default, Runtime, Description; appends enum options
  and a `CONFIG:` sub-line when the param name differs from the TOML key).
- `website/src/components/ConfigDefault.astro` — inlines a single default value
  by `section.field` path; use it wherever prose would otherwise hardcode a
  default (e.g. "the default port is <ConfigDefault path='server.port' />").
- `website/src/data/config-reference.json` — the generated data. Its `sections`
  keys are the authoritative section list.

Existing content:
- Replace: current `reference/configuration.mdx` (keep its structure and the
  `<ConfigTable>` mechanism; rewrite the hand-written prose per style rules).
- Mine and cut: `reference/reference-config.mdx` is being removed. Its only
  reusable asset is the "generate one with `frogdb-server --generate-config`"
  pointer — repoint that at the new `reference/example-config.mdx` page.

Structure (H2/H3 outline):
- Intro (2–3 sentences): FrogDB is configured with a TOML file; every parameter
  below is extracted from the source so types and defaults match the binary.
  Link to Operations → Configuration for the surfaces/precedence model. Replace
  the current "always up to date" marketing phrasing with a plain statement.
- Legend: the Runtime column meaning (✓ mutable via `CONFIG SET`, ✗ immutable /
  restart required, — not exposed to `CONFIG`). Keep concise. Note the `CONFIG:`
  sub-line convention.
- H2 "Configuration file format": one TOML snippet showing `[section] key = value`
  and the `--config` flag. One sentence pointing to env-var and CLI surfaces on
  the Operations page (do not re-teach `FROGDB_` prefix rules here — link).
- One H2 per config section, each containing a one-line purpose, an empty
  `[section]` TOML fence, then `<ConfigTable section="..." />`. Optional short
  per-section TOML example only where it materially helps (server, persistence,
  replication, cluster, metrics). Sections MUST match the generated
  `sections` keys exactly (see Drift guards). Include `[http]` and `[tls]`,
  which the current page is missing.
- H3 enum-explanation tables that add value beyond the generated `Options: …`
  list — keep "Durability modes" (async/periodic/sync trade-offs) and "Eviction
  policies" (including `tiered-lru`/`tiered-lfu` and their `tiered-storage`
  dependency) since these carry operational rationale the enum list does not.
  Verify both tables' rows against the actual enum variants in the config
  source before keeping any row.
- H2 "See also": links to Operations → Configuration, `reference/example-config`,
  `reference/frogdb-server`, `reference/frogctl`. Drop the removed
  `reference-config` link.

Generated data: `config-reference.json` (work item: existing `docs-gen`, §6).
Consumed via `<ConfigTable>` / `<ConfigDefault>`. No new work item required for
the mechanism; see Drift guards for one gap to close.

Drift guards:
- All parameter data is generated; `just docs-gen-check` (CI) fails the build if
  `config-reference.json` is stale relative to the Rust source.
- Section-coverage gap: the hand-written H2 list must stay in sync with the
  generated `sections` keys. `ConfigTable` already throws at build time on an
  unknown section name, so a renamed/removed section fails the docs build. The
  reverse (a new section in JSON with no H2 on the page) is NOT caught today —
  recommend a tiny build-time check (or a test) asserting every
  `config-reference.json` section key is referenced by exactly one `<ConfigTable>`
  in this page. VERIFY-BEFORE-WRITING: confirm whether such a check exists; if
  not, flag it as a small S8-adjacent follow-up. Current concrete instance of
  this gap: `http` and `tls` exist in the JSON but are undocumented on the page.
- No hardcoded defaults/types/param names in prose (§6 policy) — use
  `<ConfigDefault>` for any inline default.
- The two hand-maintained enum tables (durability, eviction) are the only
  hand-typed data allowed; a reviewer must diff their rows against the config
  enum source on each change.
