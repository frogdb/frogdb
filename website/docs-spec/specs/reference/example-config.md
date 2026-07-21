# Spec: reference/example-config.mdx
Status: generated (new page; replaces the cut `reference/reference-config.mdx`)
Audiences: A4 (test-deployment operators), A1 (evaluators)

Goal: The reader gets a single, complete, annotated TOML config file they can
copy to disk and edit — every section present, defaults shown, optional keys
commented. The file on the page is byte-identical to what the reader's own
binary emits with `frogdb-server --generate-config`, so there is no drift
between "the example" and "what the tool prints".

Not in scope:
- Per-parameter explanation (type, mutability, description) — that is
  `reference/configuration.mdx` (see `configuration.md`); this page links to it
  and does not repeat the tables.
- Teaching the config surfaces / precedence — link to Operations → Configuration.
- Hand-editing the TOML block. The TOML on this page is generated output; no
  human types or trims it (the current `reference-config.mdx` hand-trims
  sections and carries an Aside admitting the omission — that whole pattern is
  removed).

Sources of truth (author must read):
- `frogdb-server/crates/server/src/main.rs` — confirms the flag. VERIFIED: the
  `--generate-config` flag EXISTS (`Cli.generate_config`, main.rs:98-100). When
  set, main prints `Config::default_toml()` to stdout and exits 0
  (main.rs:114-119). The exact invocation for the pipeline is
  `frogdb-server --generate-config`.
- `frogdb-server/crates/server/src/config/loader.rs` — where the output text
  actually comes from. `default_toml()` (trait, loader.rs:63; impl loader.rs:232)
  delegates to `default_toml_impl()` (loader.rs:379).
- `Justfile` — `docs-build` recipe (line 466) currently runs
  `docs-gen compat-gen` then `cd website && bun run build`; this page's
  generation step is added here.

Existing content:
- Replace: `reference/reference-config.mdx`. Reuse its "generate it yourself"
  framing and its `See Also` cards; discard its hand-pasted, hand-trimmed TOML
  block and its "some sections omitted" Aside entirely.

**VERIFY-BEFORE-WRITING — build work required before this page can be a true
single source:**
`default_toml_impl()` (loader.rs:379) returns a **hardcoded `r#"..."#` string
literal**, NOT a serialization of `Config::default()`. Consequence: two
independent representations of the defaults exist —
  1. `config-reference.json` (generated from `Config::default()` via `schemars`
     in `docs-gen`), which drives the Configuration reference page, and
  2. the hand-written `default_toml_impl()` string, which drives this page.
These can silently disagree (a new field added to `Config` with a `#[serde]`
default appears in the reference table but not in the generated TOML). So
wiring `--generate-config` into the docs build removes drift between *the page*
and *the binary's output*, but does NOT remove drift between *the binary's
output* and *the actual `Config` defaults*. The spec author must decide with the
maintainer which of these is built first:
  - Option A (recommended, closes the real gap): change `default_toml_impl()` to
    serialize `Config::default()` to TOML (e.g. via `toml::to_string`), with a
    curated header comment and commented-out optional keys, so both surfaces
    derive from one source. Then this page is genuinely generated end-to-end.
  - Option B (interim): keep the hardcoded string but add a `docs-gen --check`-
    style test asserting `default_toml_impl()` round-trips to a `Config` equal
    to `Config::default()` and covers every section in `config-reference.json`.
    This catches drift without restructuring the emitter.
Record which option is chosen; the "Generated data" and "Drift guards" sections
below assume the pipeline is wired regardless.

Structure (H2/H3 outline):
- Intro (2 sentences): this is a complete config file with every section at its
  default; generate an identical one locally with `frogdb-server --generate-config`.
- H2 "Generate it yourself": the `frogdb-server --generate-config > frogdb.toml`
  command and a one-line note that the block below is that exact output captured
  at docs-build time.
- H2 "Full configuration": a single fenced ```toml block whose contents are the
  generated file (injected at build; see Generated data). No manual edits, no
  per-section omissions.
- H2 "See also": links to `reference/configuration` (per-parameter detail),
  Operations → Configuration (surfaces/precedence), Operations → Deployment.

Generated data / pipeline (work item S4, §6):
- Add a `docs-build`-time step that runs the server binary with
  `--generate-config`, captures stdout to a file under `website/src/data/`
  (e.g. `example-config.toml`), and the page includes it (Astro/Starlight can
  import the raw text into a code fence, mirroring how `config-reference.json`
  is imported). Prefer emitting to `website/src/data/` so all generated docs
  inputs live together.
- Concrete Justfile wiring: extend the `docs-build` (and `docs-dev`) recipe to
  produce the TOML before `bun run build`, e.g. a `docs-config` recipe:
  `cargo run -p frogdb-server -- --generate-config > website/src/data/example-config.toml`
  added as a prerequisite alongside `docs-gen` / `compat-gen`. VERIFY the crate/
  bin name used by `cargo run` (`-p frogdb-server`) against the workspace before
  writing.
- CI (ties to S8): `deploy-docs.yml` must run `just docs-build` (which now
  regenerates this TOML) rather than a raw `bun run build`, so the deployed page
  reflects the current binary.

Drift guards:
- The TOML block is captured from the binary at every docs build — it cannot
  lag the flag's output.
- Add a `--check` mode for the capture step (regenerate to a temp file, diff
  against the committed `website/src/data/example-config.toml`; fail if they
  differ), wired into a CI job that also triggers on `website/**` and server
  crate changes (S8). This is the analogue of `docs-gen-check`.
- The deeper guard (defaults-vs-TOML agreement) depends on the Option A/B
  decision above; without it, this page is only as correct as
  `default_toml_impl()`. State that dependency explicitly in the page's
  generated-notice comment.
