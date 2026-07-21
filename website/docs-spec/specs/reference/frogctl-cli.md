# Spec: reference/frogctl.mdx
Status: rewrite (of the existing hand-maintained `reference/frogctl.mdx`)
Audiences: A4 (test-deployment operators), A5 (contributors), A1 (evaluators)

Goal: The reader can see every `frogctl` global option and every top-level
command (with a one-line purpose), plus the synopsis and representative
examples. Both the global-options table and the command list are generated from
(or check-enforced against) the clap definition, so the command inventory cannot
drift from the binary.

Not in scope:
- Exhaustive per-subcommand flag reference for all nested subcommands. The page
  documents global options + the top-level command list; deep per-subcommand
  flags are better served by `frogctl <cmd> --help`. (If the S5 mechanism can
  cheaply emit nested subcommands, the spec author may include a second level;
  default is top-level only to keep the page maintainable.)
- Redis command semantics reached via `frogctl exec` — link to the Command
  matrix.

Sources of truth (author must read):
- `frogctl/src/cli.rs` — the `Cli` / `GlobalOpts` / `Commands` clap definitions
  are authoritative. VERIFIED against source:
  - Global options (`GlobalOpts`, cli.rs:22-71): `--host/-H` (default
    `127.0.0.1`), `--port/-p` (default `6379`), `--auth/-a`, `--user/-u`,
    `--tls`, `--tls-cert`, `--tls-key`, `--tls-ca`, `--admin-url`,
    `--metrics-url`, `--output/-o` (default `table`; enum `table|json|raw`),
    `--no-color`.
  - Top-level commands (`Commands`, cli.rs:80-143): **17** commands — `health`,
    `stat`, `config`, `cluster`, `replication`, `debug`, `backup`, `data`,
    `exec`, `acl`, `client`, `scan`, `watch`, `subscribe`, `search`,
    `benchmark`, `upgrade`. The current page's "17 commands" claim is CORRECT and
    the list matches; still regenerate rather than trust it.
- `frogctl/src/main.rs` and `frogctl/src/commands/` — subcommand arg structs, if
  the nested level is included.

Existing content:
- Replace: `reference/frogctl.mdx`. Keep structure (synopsis, Global Options,
  Commands, Examples, See Also). Replace both hand-typed tables with
  generated/checked content. Fix the `See Also` link to the Command matrix at
  `compatibility/command-matrix` (its canonical slug in the new IA).

Structure (H2/H3 outline):
- Intro + synopsis: one line and a `frogctl [GLOBAL OPTIONS] <COMMAND>` fence.
- H2 "Global options": generated table (Option, Short, Type, Default,
  Description), from clap `GlobalOpts`.
- H2 "Commands": generated two-column table (Command, Description) from the
  `Commands` enum's variant doc comments. One row per top-level command.
- H2 "Examples": representative invocations (`health`, `stat`, `exec GET k`,
  `scan --match`, `watch`, `cluster info`, `backup snapshot`, `-o json health`,
  `--tls --tls-ca ca.crt health`, `benchmark`). Every command/flag used must
  exist in the generated tables.
- H2 "See also": `reference/frogdb-server`, Command matrix, `reference/configuration`.

Generated data / sync mechanism (work item S5, §6):
Same mechanism as `frogdb-server-cli.md` — one shared CLI-dump tool covering
both binaries:
1. Extend the recommended `docs-gen` CLI-introspection path (clap
   `CommandFactory` on `frogctl::cli::Cli`) to also emit `website/src/data/
   cli-frogctl.json` containing global options and the subcommand list (name +
   about text; optionally nested args).
2. Render with the shared `CliTable.astro` component (global-options table) plus
   a simple command-list table.
3. `--check` mode diffs regenerated JSON against the committed file; CI fails on
   drift.
VERIFY-BEFORE-WRITING: `frogctl`'s `Cli` must be reachable from the generator
(expose `frogctl::cli::Cli` publicly, or run `frogctl` with a hidden dump
subcommand). Confirm the frogctl crate name and that its `cli` module is `pub`
before choosing the in-process introspection route; otherwise use the
`frogctl --help` / `frogctl <cmd> --help` capture route. Neither the generator
extension nor `cli-frogctl.json` exists yet — net-new under S5.

Drift guards:
- Command list and global options are generated from clap; `--check` CI job
  fails when `cli-frogctl.json` is stale relative to `cli.rs`. This directly
  prevents the "17 commands" figure from silently going wrong when a subcommand
  is added or removed.
- The check job triggers on `website/**` and frogctl-crate changes (S8).
- Example commands validated against the generated command/flag inventory.
- No hand-typed command names or option defaults remain in the tables.
