# Spec: reference/frogdb-server.mdx
Status: rewrite (of the existing hand-maintained `reference/frogdb-server.mdx`)
Audiences: A4 (test-deployment operators), A1 (evaluators), A5 (contributors)

Goal: The reader can look up every `frogdb-server` command-line flag — its long
and short form, value name, default, and meaning — and see how flags relate to
the config file (CLI overrides config). The options table is generated from (or
check-enforced against) the clap definition, so it cannot silently drift from
the binary.

Not in scope:
- Config-file parameters — those live in `reference/configuration.mdx`. The CLI
  page documents only flags; where a flag overrides a config key, it links.
- Env-var mechanics in depth — link to Operations → Configuration; keep at most
  a two-line pointer, not the full `FROGDB_` mapping.
- Operational how-tos (TLS setup, deployment) — link to the relevant Operations
  pages from the examples.

Sources of truth (author must read):
- `frogdb-server/crates/server/src/main.rs` — the `Cli` struct
  (`#[derive(Parser)]`, lines 18-109) is the authoritative flag list. VERIFIED
  current flags: `--config/-c`, `--bind/-b`, `--port/-p`, `--shards/-s`,
  `--log-level/-l`, `--log-format`, `--admin-bind`, `--admin-port`,
  `--http-bind`, `--http-port`, `--http-token`, `--tls-enabled`,
  `--tls-cert-file`, `--tls-key-file`, `--tls-ca-file`, `--tls-port`,
  `--tls-require-client-cert`, `--tls-replication`, `--tls-cluster`,
  `--generate-config`, `--intrinsic-latency`, `--startup-latency-check`.
  Note deltas from the current page: the current page lists `--shards` type as
  "string" (correct — it accepts `auto`), and the current page's flag set
  matches the source as of this writing. Regenerate rather than trust this list.

Existing content:
- Replace: `reference/frogdb-server.mdx`. Keep its structure (synopsis, Options
  table, Examples, Environment Variables pointer, See Also). Replace the
  hand-typed Options table with generated/checked content and re-verify every
  example command's flags exist.

Structure (H2/H3 outline):
- Intro + synopsis: one line ("the main FrogDB server process") and a
  `frogdb-server [OPTIONS]` fence.
- H2 "Options": the generated table. Columns: Option (long form), Short, Type,
  Default, Description. Source = clap (see Generated data). Group ordering
  follows the `Cli` struct declaration order (matches `--help`).
- H2 "Examples": a handful of real invocations (defaults, `--config`,
  `-b 0.0.0.0 -s auto`, `--generate-config`, TLS, `--http-token`,
  `--intrinsic-latency`). Every flag used must exist in the generated table;
  the docs build / example test should fail otherwise.
- H2 "Environment variables": 3-line pointer — all config fields accept
  `FROGDB_`-prefixed env vars with `__` nesting — then link to Operations →
  Configuration. Do not reproduce the full mapping.
- H2 "See also": `reference/configuration`, `reference/example-config`,
  `reference/frogctl`.

Generated data / sync mechanism (work item S5, §6):
Recommended approach — capture `--help` to a committed fixture and check it,
generate the table from a small structured dump:
1. Add a hidden/undocumented dump path or reuse clap introspection. Two viable
   mechanisms, pick one and state it:
   - (a) Parse `frogdb-server --help` text into a JSON fixture. Simple, but
     brittle to clap's text layout.
   - (b, recommended) Emit structured JSON directly from the clap `Command`
     model (`Cli::command()` from `clap::CommandFactory`), walking
     `get_arguments()` for id, long, short, value name, default, and help. This
     is stable across clap's text-format changes and gives clean table data.
   Implement (b) as a subcommand of the existing `docs-gen` binary (e.g.
   `docs-gen --cli` or a sibling `ops/cli-gen`) so all doc generation shares one
   entry point and `--check` convention. Output: `website/src/data/cli-server.json`.
2. A new Astro component (e.g. `CliTable.astro`, mirroring `ConfigTable.astro`)
   renders the table from that JSON.
3. `--check` mode (mirroring `docs-gen`'s, main.rs:103) regenerates in memory
   and diffs against the committed JSON; CI fails on drift.
VERIFY-BEFORE-WRITING: none of (cli-gen, `cli-server.json`, `CliTable.astro`)
exist yet — this is net-new build work under S5. Confirm `clap`'s
`CommandFactory`/`derive` feature is available in the server crate (clap is
already a dependency; check the `derive`/`cargo` features) before speccing the
introspection route; fall back to the `--help`-capture route only if not.

Drift guards:
- Options table is generated from the clap model; `--check` CI job (analogous to
  `docs-gen-check`) fails when `cli-server.json` is stale relative to `main.rs`.
- The check job must also trigger on `website/**` and on server-crate changes
  (S8), so editing either side is caught.
- Examples are prose but every flag in them is validated against the generated
  table — add this to the same check (grep each ```bash frogdb-server` flag
  against the JSON's long/short set) or a small test.
- No hand-typed defaults or flag names remain in the Options table.
