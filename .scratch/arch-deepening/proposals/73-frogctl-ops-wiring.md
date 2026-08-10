# Proposal 73 — frogctl `ops/`: wire the orphaned operation modules, delete the duplicated scan loop

Round 38 · lane: frogctl / operator / telemetry · effort **M** · candidate FR1 · ordered **after**
proposal 72 (FR2)

**Revised at `4c36827d` per adversarial review @`43720822`.** The original was verified at
`159cb7a2`; `git diff --stat 43720822..HEAD -- ':!*.md'` is **empty** and so is
`159cb7a2..HEAD -- ':!*.md'` — every intervening commit touches only
`.scratch/arch-deepening/proposals/*.md`. All source citations below were therefore re-derived
by reading at `4c36827d`, and all *sibling-proposal* citations were re-read too, because those
files **did** move (see N7 below).

The review's verdict was **AMEND**: the **WIRE** ruling survives, but the argument that produced
it was wrong in its load-bearing parts. This revision **rebuilds** the wire-vs-delete case on the
three arguments that actually carry it, **concedes** the three that do not, corrects **six**
blocking errors, and adds **two defects neither the brief, the original proposal, nor the review's
own census had named** (D7, D8 — D7 was raised by the review as O2; D8 as O3).

### Review-response ledger

| item | disposition |
|---|---|
| **P3** canonical-home pillar | **CONCEDED, weight zero.** `git show c47f443e -- frogctl/src/commands/data.rs`: the four removed `data` alias arms were **themselves `bail!("… not yet implemented")`** (`Bigkeys`, `Memkeys`, `Export`, `Import` — clap variants + stub arms, no engine call anywhere). The ruling was namespace ownership over *stubs*. Deleting `ops/` leaves `CONTEXT.md:45-53` untouched and true |
| **P4** `sha2`/`hex` pillar | **CONCEDED and INVERTED.** Dep removal is a **DELETE benefit**. Verified ops-exclusive: `sha2`/`hex` (`ops/backup.rs` only), `toml` (`ops/config.rs` only), `tempfile` (`ops/{backup,config}.rs` `#[cfg(test)]` only). DELETE removes **four** dependency edges |
| **P1/P2** false glossary sentence + defined term | **DOWNGRADED to low.** Retracting a sentence is not losing a capability |
| **scan-dedup exclusivity** | **CONCEDED.** DELETE collapses the duplicate too, and more cheaply (−309 lines vs −95/+15). Not a wire argument |
| **B1** D2 does not exist | **ACCEPTED.** Both loops verified behaviourally identical. D2 **retracted**; the swap is behaviour-preserving. Test reframed |
| **B2** count-pin arithmetic; `debug.rs` row | **ACCEPTED.** 19 after wiring, 16 after 72. `debug.rs` has 8 bails: 5 wired, 3 stay |
| **B3** phantom census wrong set | **ACCEPTED.** `memory doctor` `:3332` is implemented; `latency graph` `:2952` and `memkeys` `:3536` were omitted |
| **B4** serde rejector | **ACCEPTED.** Rejector is `ServerConfig` (`server.rs:18`), not root `Config`; `num_shards` is the **third** key |
| **B5** rebuild §wire-vs-delete | **DONE** — see §The deletion test |
| **B6** "the tested one is the orphan" | **ACCEPTED, retracted.** Zero `tokio::test` in `ops/`; the 2 `ops/scan.rs` tests cover `summarize_keyspace`/`find_bigkeys`, neither duplicated. `scan_keyspace`/`enrich_keys` are untested in **both** copies |
| **N1–N13** | All accepted; N6 and N7 with recorded refinements (below) |
| **O1** lint family fit | **ACCEPTED.** Now a standalone recipe reached by `just lint` (`Justfile:319`), **not** a `lint-gates` member — it has no `Y` chokepoint (`agents/seam-lints.md:3`) |
| **O2** digest gap | **ACCEPTED — promoted to defect D7.** Confirmed at source |
| **O3** unbounded key accumulation | **ACCEPTED — promoted to defect D8** |
| **O4** forced enrichment cost | **ACCEPTED — recorded** in §Proposed change step 3 and §Risks |
| **H1 / H2 / H3** | H1 kept (with 75-collision sequencing note); H2 **split** (minimal bail now, fan-out to 75); H3 **agreed → file as issue** |

**Refinements recorded against the review (not refutations of its rulings):**

* **N6.** The review says "5 error spans (says four), 2 covering tests not 1". Both halves confirmed,
  but the coverage claim is generous in the wrong direction: `verify_export` has **five**
  `errors.push` spans (`ops/backup.rs:355`, `:365`, `:379-383`, `:386-391`, `:405-409`) and **two**
  tests (`:444-449` missing manifest, `:451-476` empty archive) — and **neither test reaches any of
  the five**. `test_verify_export_missing_manifest` exercises the pre-loop `?` at `:343-344`;
  `test_verify_export_empty` walks an archive with zero data files. Error-path coverage is **zero**,
  not "one".
* **N7.** The review's sibling line numbers (`67:106/:643`, `70:369/:540`, `69:265`) do not resolve
  at `4c36827d` — those `.md` files were revised by the intervening commits (HEAD itself is
  *"revise proposal 70"*). Re-read at HEAD, the frogctl mentions are `49:594`, `67:106`, `67:658`,
  `69:449`, `69:642`, `70:441`, `70:822`. The review's substance stands (the original's numbers were
  stale and it missed `49`); the numbers used below are HEAD's, per the re-verification rule.
* **New, found during this revision (not in the review):** replacing `ScanEntry` with
  `ops::scan::KeyInfo` **would rename a `--output json` field** (`memory` → `memory_bytes`). The
  original's claim that the JSON shape is unchanged was false. Handled in step 2.

## Summary

`frogctl` has a two-layer shape that nobody wrote down but that the code plainly intends:

- **`frogctl/src/ops/`** — *operation modules*. Pure engines. Each takes a
  `redis::aio::MultiplexedConnection` (or nothing), runs one multi-round-trip operation, and
  returns a `Serialize` summary struct. No `println!`, no clap, no `ConnectionContext`.
- **`frogctl/src/commands/`** — *adapters*. Each maps clap args → an operation → a `Renderable`
  → the single output seam `print_output` (`output.rs:9-16`).

The adapter layer for `ops/` was never written. `lib.rs:5` declares `pub mod ops;`, `ops/mod.rs`
declares four submodules, and **nothing in the workspace names any of them** — grepping
`crate::ops` / `frogctl::ops` / `ops::` across every `.rs` outside `frogctl/src/ops/` returns zero
hits (the only `ops::` matches repo-wide are `std::ops::`). 1,530 lines of engine sit behind a
`pub mod` in a library crate, which is exactly why neither rustc's `dead_code` pass nor clippy has
ever complained: `pub` items of a `[lib]` target are reachable-by-definition from outside the
crate, so the compiler cannot see that only the `[[bin]]` consumes it and the `[[bin]]` never
does.

Downstream of that gap, **28 subcommand arms across seven command modules bail with
`"not yet implemented"`** — while all 28 are published on the documentation website, because
`docs-gen` links against the `frogctl` **library** and walks `frogctl::cli::Cli::command()`
(`frogdb-server/ops/docs-gen/src/main.rs:329-336`) to generate
`website/src/data/frogctl-cli.json`. The clap surface is the doc surface; the dispatch is not.

**Nine of those 28 have a complete, argument-compatible implementation already sitting in `ops/`,
and the match is nine-for-nine, not approximate.** `BackupCommand::Export { output,
match_pattern, count, key_type }` (`commands/backup.rs:20-36`) against
`export_dataset(conn, output_dir, pattern, key_type, batch, on_progress)` (`ops/backup.rs:69-76`);
`Verify { dir }` against `verify_export(&Path)`; `LatencySubcommand::Graph { event }` against
`latency_history(conn, event, …)` + `render_ascii_graph(event, points, width)`;
`MemorySubcommand::Bigkeys { key_type, top, samples }` against `ScanOpts { key_type, limit, … }` +
`find_bigkeys(keys, top)`. That isomorphism is not a coincidence you can read two ways: `ops/` was
written *as* the implementation of these exact subcommands, and only the ~30-line-per-command
adapter was skipped.

**The wire-vs-delete question turns on what has already shipped, not on what the glossary says.**
The nine subcommands are not internal API — they are **published, released, operator-facing
surface**: nine entries with full flag documentation in `website/src/data/frogctl-cli.json`,
rendered on the reference page by `<CliCommandsTable source="frogctl-cli" />`
(`website/src/content/docs/reference/frogctl.mdx:26`), inside a binary built and shipped in every
release (`.github/workflows/release.yml:119,123`). An honest DELETE cannot stop at `ops/`: leaving
the clap variants in place would keep publishing nine commands that provably cannot exist, so
DELETE must also remove the variants from `commands/{backup,data,debug}.rs` — a **user-visible CLI
and website shrink**, colliding with the same enums proposals 74 and 75 edit. That is a *larger and
more contentious* change than writing the adapters. See §The deletion test for the full
adjudication, including three pillars of the original argument that collapse.

**One thing does delete.** `commands/scan.rs:142-235` is a line-for-line reimplementation of
`ops::scan::scan_keyspace` + `enrich_keys` (`ops/scan.rs:40-167`) — same SCAN/MATCH/COUNT/TYPE
loop, same enrichment pipeline, same `fields_per_key` index arithmetic, down to the same
`let _ = idx;` warning-suppression line (`commands/scan.rs:200` ≡ `ops/scan.rs:152`). Two copies of
the keyspace scan engine exist; **neither is tested** (B6). This is a real finding but **not a wire
argument** — deleting `ops/` collapses the duplicate too, and more cheaply. It is recorded here as
what it is: the cheapest correct resolution *given* the wire ruling, and the thing that finally
makes `CONTEXT.md:60-61` true.

The change: write the missing adapters for the nine backed subcommands, route `scan` through the
one scan engine, fix the four latent defects the wiring exposes plus the two archive-integrity
defects found in review, and add a **count pin** on the remaining `not yet implemented` arms so the
number can only go down.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogctl/src/ops/mod.rs` | 4 | **read-only evidence.** Declares `backup`/`config`/`latency`/`scan`. Gains only a module doc-comment |
| `frogctl/src/lib.rs` | 7 | **read-only evidence.** `pub mod ops;` at `:5` — the reason no `dead_code` warning ever fired |
| `frogctl/src/ops/backup.rs` | 477 | **wired (near-unchanged).** `export_dataset` `:69-226`, `import_dataset` `:229-296`, `verify_export` `:339-418`, `parse_data_file` `:299-336`. Defects D3–D5, D7, D8 fixed on the way in. Types `ExportSummary` `:21-27`, `ImportSummary` `:29-35`, `VerifySummary` `:37-45`, `ExportManifest` `:47-56` (`version` `:50`), `DataFileEntry` `:58-63`, `ExportProgress` `:7-12`, `ImportProgress` `:14-18`; `MANIFEST_VERSION` `:66` |
| `frogctl/src/ops/scan.rs` | 309 | **wired.** `scan_keyspace` `:40-101`, `enrich_keys` `:104-167` (**private** — reachable only through `scan_keyspace`), `summarize_keyspace` `:179-206`, `find_bigkeys` `:209-237`, `format_key_info` `:240-247`. `ScanOpts` `:7-15`, `KeyInfo` `:17-27`, `ScanSummary` `:29-34`, `KeyspaceTypeSummary` `:170-176`. Becomes the *only* scan engine |
| `frogctl/src/ops/latency.rs` | 261 | **wired.** `latency_doctor` `:27`, `latency_history` `:37`, `latency_histogram` `:68`, `parse_histogram_response` `:90`, `render_ascii_graph` `:135`, `render_histogram_table` `:188`. The last two are rendering living in an operation module — see §Risks (boundary with 75) |
| `frogctl/src/ops/config.rs` | 479 | **NOT TOUCHED — owned by proposal 72.** Cited only for the ordering argument (§Risks). `generate_default_config` `:26-118` emits snake_case keys (`num_shards` `:38`, `max_clients` `:40`, `tcp_keepalive` `:42`, `max_memory` `:50`, `maxmemory_policy` `:53`, `data_dir` `:60`) |
| `frogctl/src/commands/backup.rs` | 190 | **the change.** `run` `:130-144` — the three `bail!`s at `:135`/`:138`/`:141` become adapters. Arg surface `:20-62` matches the engine 1:1. `Renderable for PersistenceStatus` `:76-128` is the pattern the three new impls follow |
| `frogctl/src/commands/scan.rs` | 243 | **the change (deletion).** `run` `:134-243`; the duplicated engine `:142-235` (~95 lines) deleted and replaced by one `scan_keyspace` call. `ScanEntry` `:40-49` **stays as a serde shim** (see step 2 — deleting it renames a JSON field); `Renderable for ScanResult` `:57-132` stays verbatim |
| `frogctl/src/commands/data.rs` | 119 | **the change.** `run` `:33-43` — `bail!` at `:36` (`data keyspace`) becomes an adapter over `scan_keyspace` + `summarize_keyspace`. `:39` (`data pipe`) **stays a bail** — no engine backs it. `Keyspace { samples }` `:9-13` (default 10 000) |
| `frogctl/src/commands/debug.rs` | 770 | **the change.** `run` `:391-440` holds **8** bails. **5 become adapters:** `:405`/`:408`/`:411` (latency doctor/graph/histogram), `:418`/`:421` (memory bigkeys/memkeys). **3 stay:** `:394` (zip → proposal 74), `:425` (hotshards), `:434` (vll). Note `MemorySubcommand::Doctor` is **already implemented** (`:416` → `run_memory_doctor` `:763`). Subcommand shapes `LatencySubcommand` `:120-137`, `MemorySubcommand` `:139-164` |
| `frogctl/src/commands/config.rs` | 139 | **NOT TOUCHED — owned by proposal 72.** `run` `:99-112`, `bail!`s at `:102`/`:105`/`:108` |
| `frogctl/src/output.rs` | 173 | **read-only evidence — the seam.** `trait Renderable` `:3-7`, `print_output` `:9-16`. Every new adapter exits here. `render_value` `:19-29` is the competing path (proposal 75's problem, not this one) |
| `frogctl/src/cli.rs` | 143 | **read-only evidence.** `GlobalOpts.output` `:64-66`, `.no_color` `:68-70`; `enum Commands` `:80-143`. **The doc surface** — `docs-gen` walks exactly this |
| `frogctl/src/connection.rs` | 193 | **read-only evidence.** `ConnectionContext::resp` `:32` is how an adapter hands a connection to an engine; `resp_to` `:52` is the fan-out primitive (H2); `global()` `:180` supplies output mode |
| `frogctl/src/util.rs` | 180 | **read-only evidence.** `extract_string`/`extract_int`/`extract_int_opt` `:4-30`, `format_bytes` `:45-59` — already shared by both `ops/` and `commands/`, so the layering is already half-built |
| `frogctl/src/commands/health.rs` | — | **read-only evidence.** `--all` honoured at `:131`; `run_fanout` `:284-306` is the Fan-out primitive H2/75 reuse |
| `frogctl/CONTEXT.md` | 78 | **the change (doc).** The engine relationship `:60-61` — today a false statement, made true by this change. **Fan-out** `:26-28`; **Export Archive** `:38-39`; canonical command homes `:45-53`. Its *Avoid* list is honored below |
| `frogctl/Cargo.toml` | 48 | **read-only evidence + one edit.** `cli-tests` feature `:18-20`, `[[test]] required-features` `:22-25`. Deps: `indicatif` `:37`, `comfy-table` `:38`, `dialoguer` `:39`, `zip` `:40` have **zero** uses anywhere in the crate; `sha2` `:41` / `hex` `:42` exist **only** for `ops/backup.rs`; `toml` `:43` **only** for `ops/config.rs`; `tempfile` `:44` sits in `[dependencies]` but is used only from `#[cfg(test)]` (`ops/backup.rs:446,455`; `ops/config.rs:372,396,414,432,451`) |
| `frogctl/tests/main.rs` | 11 | **the change (tests).** The `mod` list gains `integration_backup` |
| `frogctl/tests/integration_scan.rs` | 96 | **the change (tests).** Five tests that assert only `exit_code == 0` (`:33,45,63,78,95`) — no output inspected. Strengthened to pin engine behaviour |
| `frogctl/tests/integration_data.rs` | 29 | **the change (tests).** Only `data slot` is covered (`:15,28` — both bare exit-code asserts); `data keyspace` gains coverage |
| `frogctl/tests/common/setup.rs` | 69 | **read-only evidence.** `ctx_for_server(&TestServer)` — the harness hook every new integration test uses. Imports `frogctl::cli` + `frogctl::connection` only (`:1-2`); it is the `integration_*.rs` files that reach `frogctl::commands::*` |
| `.config/nextest.toml` | — | **the change.** `default-filter = 'not package(frogctl)'` at `:5` with its rationale comment at `:2-4` — frogctl's **lib unit tests are excluded from `just test` too**, not only its integration tests (§Problem C2) |
| `Justfile` | — | **read-only evidence + one edit.** `just test frogctl` is hard-refused at `:80-83`; `frogctl-test` `:297-298` (`--features cli-tests --ignore-default-filter`); `coverage-lcov` `:101-104` (the only place these tests execute today); `docs-gen` `:812-813`, `docs-gen-check` `:816-817`; `lint` `:319` (where the new count pin joins); `lint-gates` `:329` (which it deliberately does **not** join — see step 5) |
| `frogdb-server/ops/docs-gen/src/main.rs` | — | **read-only evidence.** `generate_cli_reference(frogctl::cli::Cli::command(), …)` `:329`, written to `frogctl-cli.json` `:331-336`. The mechanism that publishes phantom commands (dep edge: `docs-gen/Cargo.toml:14`) |
| `website/src/data/frogctl-cli.json` | 9,018 | **read-only evidence (generated).** The nine phantoms: latency `doctor` `:2860`, `graph` `:2952`, `histogram` `:3049`, `bigkeys` `:3424`, `memkeys` `:3536`, `export` `:4350`, `import` `:4460`, `verify` `:4578`, `keyspace` `:4769`. (`generate` `:431` is proposal 72's; memory `doctor` `:3332` is **implemented**.) Regenerated by `just docs-gen`; **not edited by hand** |
| `website/src/content/docs/reference/frogctl.mdx` | — | **read-only evidence + hotfix H1.** `<CliCommandsTable source="frogctl-cli" />` `:26`; `:50` documents `frogctl backup snapshot` — a subcommand that has never existed |
| `frogdb-server/crates/config/src/server.rs` | — | **read-only evidence.** `#[serde(deny_unknown_fields, rename_all = "kebab-case")]` `:18` on `struct ServerConfig` `:19` — **the actual rejector** of `ops/config.rs`'s output |
| `frogdb-server/crates/config/src/lib.rs` | — | **read-only evidence.** The same attribute `:83` on root `struct Config` `:84` — governs **section names only**, which `ops/config.rs` spells correctly |

Nothing here is in a **locked** area — see §Spec / LOCKED.

## Problem

### The orphan, precisely

```
frogctl/src/lib.rs:5          pub mod ops;
frogctl/src/ops/mod.rs:1-4    pub mod backup; pub mod config; pub mod latency; pub mod scan;
```

Repo-wide, outside `frogctl/src/ops/` itself: **zero** references to `crate::ops`,
`frogctl::ops`, or any `ops::backup` / `ops::scan` / `ops::latency` / `ops::config` path. The only
`ops::` hits in the workspace are `std::ops::Deref`, `std::ops::Bound`, and friends. `main.rs`
dispatches 17 top-level commands (`main.rs:21-37`, inside the `match` at `:20-38`) and touches
`commands::` only.

1,530 lines: `backup.rs` 477 + `scan.rs` 309 + `latency.rs` 261 + `config.rs` 479 + `mod.rs` 4.
The lane brief's total is correct. Its characterisation of them as "1530 tested lines" is not —
see C1.

### The 28 dead arms, and which nine are backed

`anyhow::bail!("frogctl <cmd>: not yet implemented")` appears **28 times**:

| module | count | lines |
|---|---:|---|
| `commands/cluster.rs` | 10 | `:362,365,369,372,375,378,381,384,387,390` |
| `commands/debug.rs` | 8 | `:394,405,408,411,418,421,425,434` |
| `commands/backup.rs` | 3 | `:135,138,141` |
| `commands/config.rs` | 3 | `:102,105,108` |
| `commands/data.rs` | 2 | `:36,39` |
| `commands/replication.rs` | 1 | `:111` |
| `commands/upgrade.rs` | 1 | `:127` |

Nine have a complete engine already in `ops/`, and the argument shapes are isomorphic nine for
nine:

| subcommand | bails at | engine | arg match |
|---|---|---|---|
| `backup export` | `commands/backup.rs:135` | `ops/backup.rs:69` `export_dataset` | 1:1 (`output`→`output_dir`, `match`→`pattern`, `type`→`key_type`, `count`→`batch`); one decision — `match_pattern: Option<String>` vs `pattern: &str` (below) |
| `backup import` | `:138` | `ops/backup.rs:229` `import_dataset` | 1:1 (`input`, `replace`, `pipeline`→`pipeline_depth`, `ttl`→`preserve_ttl`) |
| `backup verify` | `:141` | `ops/backup.rs:339` `verify_export` | 1:1 (`dir`); **needs no connection** |
| `data keyspace` | `commands/data.rs:36` | `scan_keyspace` + `summarize_keyspace` | `samples` → `ScanOpts.limit` (decision, below) |
| `debug memory bigkeys` | `commands/debug.rs:418` | `scan_keyspace` + `find_bigkeys` | `type`→`key_type`, `top` (u64→usize), `samples`→`limit` |
| `debug memory memkeys` | `:421` | `scan_keyspace` + `summarize_keyspace` | no args |
| `debug latency doctor` | `:405` | `ops/latency.rs:27` `latency_doctor` | no args |
| `debug latency graph` | `:408` | `latency_history` + `render_ascii_graph` | `event` 1:1; graph `width` is an adapter choice |
| `debug latency histogram` | `:411` | `latency_histogram` + `render_histogram_table` | `commands: Vec<String>` 1:1 |

The remaining 19 have no implementation anywhere: 10 `cluster`, 3 `config` (proposal 72), `data
pipe`, `debug zip` (proposal 74), `debug hotshards`, `debug vll`, `replication lag`,
`upgrade node`. They are out of scope here and stay bailing.

The wiring is **not** purely mechanical at three points, which is why this is M and not S:

- **`samples` → `limit`.** `MemorySubcommand::Bigkeys { samples }` (`commands/debug.rs:157-159`,
  "SCAN sample count (0 = full scan)") has no counterpart in `ScanOpts` (`ops/scan.rs:7-15`), which
  offers `limit: Option<usize>`. `samples == 0` → `None`; otherwise `Some(n)`. `DataCommand::Keyspace
  { samples }` (`commands/data.rs:10-12`, default 10 000) maps the same way but with a non-zero
  default, so "sample" means *ceiling* in both, and the summary must say so rather than implying a
  full-keyspace count.
- **`top: u64` → `usize`.** `find_bigkeys(keys, top: usize)` against the CLI's `top: u64`
  (`commands/debug.rs:154-155`). One cast, in the adapter, not the engine.
- **`match_pattern: Option<String>` → `pattern: &str`.** `BackupCommand::Export.match_pattern`
  (`commands/backup.rs:26-27`) is optional; `export_dataset` takes a required `&str`. `None` must
  map to `"*"` — the same default `scan`'s clap surface already spells out
  (`commands/scan.rs:12`, `default_value = "*"`). Recording it as a decision so it is not
  silently re-litigated as `""`.

### Everything published, nothing reachable

`docs-gen` depends on the `frogctl` **library** (`frogdb-server/ops/docs-gen/Cargo.toml:14`) and
reflects over the clap tree at `main.rs:329`. So `website/src/data/frogctl-cli.json` carries full
flag documentation for latency `doctor` (`:2860`), `graph` (`:2952`), `histogram` (`:3049`),
`bigkeys` (`:3424`), `memkeys` (`:3536`), `export` (`:4350`), `import` (`:4460`), `verify`
(`:4578`) and `keyspace` (`:4769`) — every one of which fails at runtime. The published reference
page renders that JSON through `<CliCommandsTable source="frogctl-cli" />`
(`website/src/content/docs/reference/frogctl.mdx:26`).

This is the **LIVE** part of FR1: an operator reads the shipped documentation, runs
`frogctl backup export -o /backups`, and gets `Error: frogctl backup export: not yet
implemented` — from a binary that is built and shipped in every release
(`.github/workflows/release.yml:119,123`).

### Corrections to the lane brief

**C1 — "1530 tested lines" overstates coverage twice over.** The 1,530 lines carry **16**
`#[test]` functions total (backup 3, scan 2, latency 4, config 7 — counted at HEAD; proposal 72's
"6 unit tests" for `ops/config.rs` is one short), and every one of them tests only a *pure*
helper: `parse_data_file` round-trip, `verify_export` on an empty archive, `render_ascii_graph`,
`render_histogram_table`, `summarize_keyspace`, `find_bigkeys`. There are **zero** `#[tokio::test]`
functions anywhere in `ops/`. The connection-taking functions — `export_dataset`,
`import_dataset`, `scan_keyspace`, `enrich_keys`, `latency_*` — have **no** tests, because a unit
test has no server. That is where all the protocol risk lives.

**C2 — those tests do not run in `just test`, and the brief did not say so.**
`.config/nextest.toml:5` sets `default-filter = 'not package(frogctl)'`, which drops frogctl's
**lib unit tests** as well as its integration tests (the integration tests are separately gated
behind `required-features = ["cli-tests"]`, `frogctl/Cargo.toml:22-25`); `Justfile:80-83` goes
further and hard-refuses `just test frogctl` with exit 2. No workflow under `.github/workflows/`
invokes `just frogctl-test` (`Justfile:297-298`) — grepped. The **only** place these 16 tests
execute is `just coverage-lcov` (`Justfile:104`, `--features frogctl/cli-tests
--ignore-default-filter`), driven by the nightly coverage job (`coverage-nightly.yml:79`), which
measures and does not gate. So `ops/` is orphaned from the binary *and* from CI.

**C3 — the brief lists the bailing arms as "commands/config.rs:102-108, backup.rs:135-141,
data.rs:36, debug.rs:405-421"; that undercounts by more than half.** The true count is 28 across
seven modules (table above), including ten in `cluster.rs` the brief never mentions. The brief's
`debug.rs:405-421` range also silently omits `:394` (zip), `:425` (hotshards) and `:434` (vll) —
and sweeps in `:416`, which is not a bail at all but the implemented `MemorySubcommand::Doctor`
arm.

### Defects

**D1 — `commands/scan.rs` is a second copy of the scan engine.** `commands/scan.rs:142-235`
against `ops/scan.rs:51-93` + `:109-164`: identical `SCAN cursor MATCH pattern COUNT n [TYPE t]`
construction, identical `redis::pipe()` enrichment ordering (TYPE, then TTL, then MEMORY USAGE),
identical `fields_per_key = with_type as usize + with_ttl as usize + with_memory as usize`
arithmetic, identical `let _ = idx;` suppression (`commands/scan.rs:200` ≡ `ops/scan.rs:152`),
identical `truncate(limit)` tail. `ScanEntry` (`commands/scan.rs:40-49`) and `KeyInfo`
(`ops/scan.rs:17-27`) are the same four fields with one renamed (`memory` vs `memory_bytes`).
**Neither copy is tested** (C1/B6): `scan_keyspace` and `enrich_keys` have zero unit tests, and
the five `integration_scan.rs` tests exercise the *caller's* copy while asserting only
`exit_code == 0`. Any future SCAN fix — cursor-guarantee handling, `MEMORY USAGE` on a missing key,
the enrich/limit interaction — lands in one copy and not the other.

**D2 — RETRACTED.** The original claimed the two copies apply `limit` at different points and
would therefore produce different answers. **They do not.** Verified line by line:

* Engine: `enrich_keys` (`ops/scan.rs:104-167`) builds its pipeline over the **entire** batch
  (`:110` `for key in keys`) and returns all of it *before* `scan_keyspace` starts pushing; the
  `break` at `:69-71` only stops **accumulation**, after the round-trips are already issued.
* Caller: `commands/scan.rs:159-170` builds the same pipeline over the same whole batch, and its
  `break` at `:213-215` likewise only stops accumulation.
* Both then `truncate(limit)` (`ops/scan.rs:95` ≡ `commands/scan.rs:237`) and both re-check the
  limit in the loop condition (`ops/scan.rs:90` ≡ `commands/scan.rs:232`).

Same round-trip count, same key set, same enrichment cost, for every input. The swap is
**behaviour-preserving**. The only observable difference is the engine's extra `on_progress(…)`
callback (`ops/scan.rs:87`), which the adapter satisfies with a no-op closure. The test is still
worth writing (§Testability) — not to pin a change, but because the limit/enrichment interaction
is asserted **nowhere** today, in either copy.

**D3 — `import_dataset` converts a connection failure into a silent per-key error count.**
`ops/backup.rs:268-271`: `pipe.query_async(conn).await.unwrap_or_else(|_| vec![redis::Value::Nil;
chunk.len()])`. A dropped connection mid-restore yields `chunk.len()` `Nil`s, each counted as an
error at `:278`, and the loop continues against a dead connection for the rest of the archive. The
user sees "restored: 0, errors: 40000" and no cause. Latent (unreachable), and must be fixed as
part of wiring — an I/O failure has to abort with the underlying error.

**D4 — `ImportSummary.keys_skipped` is computed and then not reported.** `ops/backup.rs:277`
increments `skipped` for a `ServerError` when `replace == false` (the normal "key already exists"
path), `:291-295` returns it in the summary — but the terminal progress event
`ImportProgress::Done { restored, errors }` (`:14-18`, emitted at `:289`) drops it. An adapter
that renders progress rather than the returned summary would silently lose the most interesting
number of a non-`--replace` import. Latent; fixed by having the adapter render the returned
`ImportSummary`, not the progress event.

**D5 — `export_dataset`'s `source` field records a whole INFO line, not a version.**
`ops/backup.rs:200-204` takes the first line starting with `frogdb_version:` or `redis_version:`
and stores the entire `"frogdb_version:1.2.3"` string into `ExportManifest.source`. Cosmetic,
latent, one line; worth fixing while the file is open since `manifest.json` is a persisted
artifact format.

**D6 — two declared, published, silently-ignored flags in `debug.rs`.** Both are **LIVE** and both
fail *silently* (no error, wrong behaviour):

- `DebugCommand::Latency { .. history: bool .. }` (`commands/debug.rs:44-46`, "Periodic snapshot
  mode (every 15s)") is destructured as `{ subcommand, samples, interval, dist, .. }` at
  `:396-402`; `history` lands in the `..` and `run_latency` (`:444-449`) never receives it.
  `frogctl debug latency --history` runs an ordinary one-shot latency measurement.
- `DebugCommand::Slowlog { .. all: Option<Vec<String>> .. }` (`:88-90`, "Collect from multiple
  nodes") is destructured as `{ count, analyze, reset, .. }` at `:427-431`. `frogctl debug slowlog
  --all a:6379 b:6379` queries only the default node. This **contradicts `CONTEXT.md:26-28`**,
  which defines **Fan-out** as "the `--all <addrs>` pattern where a command queries a list of
  nodes and merges results (health, hotshards, slowlog)". `health` does honour it
  (`commands/health.rs:131`, fan-out at `:284-306`); `slowlog` does not.

Both live in the dispatch `match` this proposal edits, so they are called out here — but their
*family* (declared-and-unread options) is proposal 75's. See §Risks for the handshake and §Effort
for the hotfixes.

**D7 — the Export Archive's integrity check does not cover the data it exists to protect.**
(Raised in review as O2; confirmed at source.) `export_dataset` hashes **only the key name** into
each batch's SHA-256:

```
ops/backup.rs:158-159   hasher.update((key_bytes.len() as u32).to_le_bytes());
                        hasher.update(key_bytes);
```

The `DUMP` payload (`:156`) and the `PTTL` (`:154`) are written to the file and **never hashed**.
`verify_export` reproduces the same digest exactly (`:370-375`), so it agrees with the manifest by
construction. Consequence: **flip any byte inside a `DUMP` payload in `batch_000000.dat` and
`frogctl backup verify` reports `valid: true`** — the archive checker misses precisely the
corruption class it exists to detect. (Truncation and key-count drift *are* caught, via
`parse_data_file` `:299-336` and the count check `:385-391`.) This is latent today and becomes a
**correctness claim about a persisted artifact** the moment `backup verify` is reachable, so it
must be resolved as part of wiring: either extend the digest to cover `pttl` + `dump_len` +
`dump_bytes` — the file already carries `MANIFEST_VERSION` (`:66`, field `version` `:50`) for
exactly this kind of format change — or state in the archive's documented corruption table that
payload corruption is undetectable by design. The first is four lines in two places; the second is
not defensible for a backup tool.

**D8 — `export_dataset` accumulates every key name in memory, unbounded, client-side.**
(Review O3.) Phase 1 (`ops/backup.rs:83-105`) drains the whole SCAN cursor into `all_keys: Vec<String>`
before a single `DUMP` is issued; there is no cap, no streaming, and no `limit` parameter on the
function. A 50M-key export materialises 50M `String`s in the CLI process before phase 2 starts.
Latent while unreachable; it becomes a real operator-facing failure mode the moment `backup export`
works. Recorded, not fixed here: the streaming rewrite (scan a batch → dump that batch → next) is a
restructure of the three-phase shape, out of scope for an adapter proposal. The adapter's clap
`about` string must state the memory cost, and a follow-up issue should carry the rewrite.

### Why this is depth, not just plumbing

`export_dataset` is a three-phase operation behind a six-parameter interface returning four
fields:

1. **SCAN sweep** (`:85-105`) — cursor loop with MATCH/COUNT/TYPE, progress callback per batch.
2. **`DUMP` + `PTTL` pipelined into a length-prefixed wire format with per-batch SHA-256**
   (`:113-179`) — two commands per key pipelined per 1000-key chunk (`BATCH_SIZE` `:65`), a
   `key_len(u32) | key | pttl(i64) | dump_len(u32) | dump_data` record layout (`:150-156`),
   deleted-between-SCAN-and-DUMP keys skipped (`:137-139`), one numbered `batch_NNNNNN.dat` per
   chunk.
3. **Manifest rollup** (`:181-216`) — a checksum over the per-file checksums, timestamp, source
   version, key count, written as `manifest.json`.

`verify_export` is 80 lines of archive validation behind `fn(&Path) -> Result<VerifySummary>` and
needs **no connection at all**, which makes it the cheapest end-to-end test in the crate. That is
good **depth**: a small interface over substantial machinery. The missing piece is ~30 lines of
**adapter** per subcommand — the cheapest layer in the crate.

## The deletion test, applied honestly

The review dismantled three of the original's four pillars. They are re-adjudicated here at their
corrected weight, and the ruling is rebuilt on the arguments that survive.

### Pillars that collapse

**P3 — "the canonical-home decision would have to be retracted." WEIGHT ZERO.**
`git show c47f443e -- frogctl/src/commands/data.rs` shows what commit `c47f443e` actually removed:
four clap variants (`Bigkeys`, `Memkeys`, `Export`, `Import`) whose dispatch arms were
`anyhow::bail!("frog data bigkeys: not yet implemented")` and friends. It was a ruling about
**namespace ownership over stubs**, not about engines — no `ops::` call existed on either side of
it. `CONTEXT.md:45-53` says the `data` namespace keeps only its own concepts and that
bigkeys/memkeys/export/import belong to `debug memory`/`backup`. Deleting `ops/` does not make that
sentence false; nothing would be aliased anywhere. `.scratch/naming-cleanup/issues/03` does not
re-open.

**P4 — "`sha2` and `hex` would go with it." INVERTED: this is a DELETE *benefit*.**
Removing a dependency edge is a win, not a cost, and the original had it exactly backwards. Verified
ops-exclusivity by grep at HEAD: `sha2` + `hex` appear only in `ops/backup.rs`; `toml` only in
`ops/config.rs`; `tempfile` only in `#[cfg(test)]` blocks of those two files. DELETE would remove
**four** dependency edges from `Cargo.toml` (`:41`, `:42`, `:43`, `:44`) — and `tempfile` currently
sits in `[dependencies]`, i.e. in the shipped binary's graph. Scored honestly, this is a point
**for** DELETE.

**P1/P2 — the false relationship sentence (`CONTEXT.md:60-61`) and the **Export Archive** glossary
term (`:38-39`). LOW.** P1 is a sentence that is false today; DELETE retracts it, WIRE makes it
true. P2 defines a term for an artifact nothing can currently produce; DELETE retracts the term.
Retraction is a doc edit, not a capability loss. Both count, neither decides.

**The scan-dedup finding is not wire-exclusive. CONCEDED.** DELETE removes `ops/scan.rs` entirely
(−309 lines) and the duplicate is gone; WIRE removes the caller's copy (−95/+15). D1 is a real
defect and its resolution is genuinely cheaper *under* DELETE. It is evidence that the codebase
has two engines, not evidence about which to keep.

### Pillars that carry the ruling

**(a) The promise has already shipped, and un-shipping it is the bigger change.** Nine subcommands
are published with full flag documentation in a generated artifact
(`frogctl-cli.json:2860,2952,3049,3424,3536,4350,4460,4578,4769`), rendered on the public reference
page (`frogctl.mdx:26`), inside a binary shipped in every release
(`release.yml:119,123`). A DELETE that stops at `ops/` leaves those nine advertised and broken —
it does not even fix the LIVE bug. An **honest** DELETE must also remove the clap variants:
`BackupCommand::{Export, Import, Verify}` (`commands/backup.rs:20-62`), `DataCommand::Keyspace`
(`commands/data.rs:8-13`), `MemorySubcommand::{Bigkeys, Memkeys}` (`commands/debug.rs:147-163`),
`LatencySubcommand::{Doctor, Graph, Histogram}` (`:120-137`). That is a **user-visible CLI
surface removal plus a website shrink**, landing in exactly the enums proposals 74 (`debug zip`)
and 75 (rendering + role enum, `health.rs`/`debug.rs`) are editing this round. It is larger,
touches more contested files, and needs a compatibility story that "wire the adapter" does not.
The cheap-looking branch is the expensive one.

**(b) Nine-for-nine argument-shape isomorphism.** Verified variant by variant (§The 28 dead arms):
`Export`→`export_dataset`, `Import`→`import_dataset`, `Verify`→`verify_export`,
`Keyspace`→`scan_keyspace`+`summarize_keyspace`, `Bigkeys`→`scan_keyspace`+`find_bigkeys`,
`Memkeys`→`scan_keyspace`+`summarize_keyspace`, `Doctor`→`latency_doctor`,
`Graph`→`latency_history`+`render_ascii_graph`, `Histogram`→`latency_histogram`+
`render_histogram_table`. Every parameter maps, in order, with **three** named conversions
(`samples`→`limit`, `top` u64→usize, `Option<String>`→`&str` with a `"*"` default) and no
unmatched engine parameter or unmatched CLI flag in either direction. Code does not acquire that
property by accident. `ops/` is not "some abandoned utility library" — it is the missing half of
these nine commands, and the seam between the halves is already drawn where the crate's own
layering wants it.

**(c) Genuine depth behind a small interface.** `export_dataset`: three phases (SCAN sweep
`:85-105` → pipelined `DUMP`+`PTTL` into a length-prefixed wire format with per-batch SHA-256
`:113-179` → manifest rollup `:181-216`) behind six parameters. `verify_export`: 80 lines of
archive validation behind `fn(&Path) -> Result<VerifySummary>` with **no connection** — the
cheapest end-to-end test in the crate. Deleting that is deleting the expensive part and keeping
the cheap part unwritten. Wiring it costs ~30 lines per command.

### Ruling

**WIRE.** Not because deleting `ops/` costs a glossary retraction (it does, cheaply), and not
because it collapses the scan duplicate (DELETE collapses it too, for fewer lines). Because the
nine subcommands are **already shipped promises** whose honest withdrawal is a bigger, more
contentious change than their fulfilment (a); because the engines are demonstrably the missing
implementation *of those exact commands* rather than adjacent code (b); and because what is
missing is the crate's cheapest layer while what exists is its most substantial (c). Cost of WIRE:
~230 lines of new adapter, ~95 deleted, five defect fixes. Cost of honest DELETE: −1,051 engine
lines *plus* a CLI/website surface removal across three files two siblings are editing. The
leverage runs one way.

Per-module, for the record:

* **`ops/config.rs`** — not this proposal's call; proposal 72 owns it and replaces its body.
* **`ops/scan.rs::scan_keyspace` + `enrich_keys`** — deletes clean in isolation (the caller carries
  a verbatim copy; `enrich_keys` is **private**, reachable only via `scan_keyspace`, so the pair
  moves together). Kept because it is the copy already shaped as an engine and already named by the
  glossary — **not** because it is better tested (neither copy is tested at all).
* **`ops/scan.rs::summarize_keyspace` / `find_bigkeys` / `format_key_info`** — no caller-side twin;
  deleting them means deleting `data keyspace` and `debug memory bigkeys/memkeys` from the CLI.
  Covered by argument (a). These two are also the only `ops/scan.rs` functions with unit tests
  (`:253-308`).
* **`ops/backup.rs`** — no twin; carries the Export Archive format, D7's integrity question, and
  `verify_export`, the crate's cheapest e2e test surface. Covered by (a) and (c).
* **`ops/latency.rs`** — no twin; `LATENCY DOCTOR`/`HISTORY`/`HISTOGRAM` have no other client-side
  reader in the repo. Covered by (a).

## Proposed change

### 1. Name the two layers, and pin the rule

Add a module doc-comment to `ops/mod.rs` and a paragraph to `frogctl/CONTEXT.md` stating the
contract that the code already half-follows:

> An **operation module** (`ops/`) takes a connection and arguments, performs one operation, and
> returns a `Serialize` summary. It never prints, never reads `GlobalOpts`, and never depends on
> `clap`. A **command adapter** (`commands/`) owns the clap types, calls exactly one operation,
> implements `Renderable` over its summary, and exits through `print_output`.

This is the **seam**: `print_output(&dyn Renderable, OutputMode, no_color)` (`output.rs:9-16`) is
already the sole exit for every wired command, and every new adapter uses it. Nothing new is
invented — the rule is written down and then obeyed.

### 2. Route `scan` through the one engine (the deletion)

`commands/scan.rs:142-235` — the duplicated SCAN loop and enrichment pipeline — is deleted.
`run` becomes: build `ScanOpts` from `ScanArgs`, call `scan_keyspace` with a no-op progress
closure, wrap the result in the existing `ScanResult`/`Renderable` impl (`:57-132`, unchanged),
call `print_output`. Per D2 this is **behaviour-preserving**: same commands, same round-trip
count, same key set.

**`ScanEntry` (`:40-49`) stays, as a serde shim.** The original proposal said to replace it with
`ops::scan::KeyInfo` and claimed the `--output json` shape was unchanged; that was **wrong**.
`ScanEntry` serializes its memory field as `memory`, `KeyInfo` as `memory_bytes`
(`ops/scan.rs:26`), so the substitution would silently rename a field in machine-parsed output.
Keep `ScanEntry` with a `From<KeyInfo>` impl (four lines) so the JSON contract is provably
untouched; the alternative — `#[serde(rename)]` on `KeyInfo` — would push a CLI-shaped concern
into an engine type and is rejected. Net: ~95 lines deleted, ~15 added, one engine remains, and
`CONTEXT.md:60-61` becomes true for the first time.

### 3. Write the nine adapters

Each is the same shape. `backup export`, in full:

- get `ctx.resp()`, build the four arguments from the clap variant (`match_pattern` → `"*"` when
  `None`),
- call `export_dataset`, passing a progress closure that writes to **stderr** — never stdout,
  because stdout carries the `--output json` document and must stay machine-parseable,
- wrap `ExportSummary` in a local `Renderable` and `print_output` it,
- return `0`.

`backup verify` skips the connection entirely (`verify_export` takes only a `&Path`) and returns
a **non-zero exit code** when `VerifySummary.valid == false` — an archive checker that always
exits 0 is useless in a cron job, and the `run` signature (`-> Result<i32>`) already supports it;
`commands/cluster.rs:356` (`Ok(report.exit_code())`) is the in-crate precedent.

`debug latency graph` and `debug latency histogram` call the engine and then delegate their
`render_table` bodies to `render_ascii_graph` / `render_histogram_table` **in place** in
`ops/latency.rs` (`:135`, `:188`). Those two functions are rendering that lives in an operation
module — a **locality** violation against the rule in step 1 — but relocating them collides head-on
with proposal 75's rendering-ownership work, so this proposal calls the debt out and leaves the
move to 75. `render_json` on those adapters serializes the engine's typed data
(`LatencyHistoryPoint` `ops/latency.rs:8`, `CommandHistogramEntry` `:15`), not the ASCII art, so
`--output json` is correct regardless of where the table renderer ends up living.

`data keyspace`, `debug memory bigkeys`, `debug memory memkeys` are three adapters over the *same*
engine call with different post-processing (`summarize_keyspace`, `find_bigkeys`,
`summarize_keyspace`) — which is precisely the "shared engine powers three commands" claim in
`CONTEXT.md:60-61`.

**The three scan-backed adapters must force `with_type` and `with_memory` on, regardless of user
flags.** `find_bigkeys` filters on `memory_bytes.is_some()` (`ops/scan.rs:210`) and groups on
`key_type` (`:221`); `summarize_keyspace` does the same (`:184-185`). Without both enrichments they
return empty or all-`unknown` results. So every one of these three commands issues a `TYPE` **and**
a `MEMORY USAGE` per sampled key — pipelined per SCAN batch, but still two extra server commands
per key. At `data keyspace`'s default `--samples 10000` that is 20,000 pipelined commands plus the
SCAN sweep. This cost is currently stated nowhere; the adapters' clap `about` strings must state
it, and `--samples` must be honoured as a real ceiling (see §Risks).

### 4. Fix the defects the wiring exposes

D3 (import swallows I/O failure → propagate the error), D4 (report `keys_skipped` from the
returned summary, not the progress event), D5 (parse the version out of the INFO line), **D7**
(extend the batch digest to cover `pttl` + `dump_len` + `dump_bytes` on both the write side
`ops/backup.rs:158-159` and the verify side `:370-375`, bumping `MANIFEST_VERSION` `:66` — or,
if rejected, document payload corruption as undetectable in the archive's corruption table).
**D8** is recorded and deferred to a follow-up issue (streaming export); the adapter documents the
memory cost. D2 needs no fix — it does not exist.

### 5. Count-pin the remaining bails

Add `just lint-frogctl-bails`: a grep asserting that the number of `not yet implemented` bails per
command module equals a pinned table. After this proposal the pin reads **cluster 10, debug 3,
config 3, data 1, replication 1, upgrade 1 — total 19** (28 − 9), dropping to **16** once proposal
72 zeroes the `config` row. A new stub cannot be added and an implemented one cannot be quietly
re-stubbed without editing the pin. This is the systematic answer to "how did 28 dead arms
accumulate while the docs advertised all of them": nothing counted them.

**Placement: a standalone recipe reached by `just lint` (`Justfile:319`), *not* a member of
`lint-gates` (`:329`), and not an entry in the seam-lint family.** A seam lint states "every X must
go through Y" for a specific chokepoint `Y` (`agents/seam-lints.md:3`); a stub census has no `Y` —
it is a count pin borrowed from `lint-continuation-lock`'s *technique*, not an instance of the
family's *invariant*. (Note also that `lint-continuation-lock` is not a grep but a brace-matching
Python script, `agents/seam-lints.md:113-118`; this recipe genuinely is a grep.) Consequences,
stated so the orchestrator can overrule with open eyes:

* It runs in CI's `lint` job and in `just check`/`just lint`, but **not** in lefthook `pre-commit`,
  which runs only `lint-gates`. A new stub is caught at CI, not at commit.
* **CI needs no new wiring** — `just lint` is already a CI job; the `seam-gates` job is
  `lint-gates` only, and is untouched.
* If the orchestrator prefers family membership instead, the budget is: `Justfile:329` (add to the
  `lint-gates` dependency list) **plus** three registry edits in `agents/seam-lints.md` — the
  count at `:4` ("Fifteen"), the compile-free count at `:9` ("fourteen"), and a new row in the
  table `:20-36` — and a paragraph justifying a member with no chokepoint. That is a real doc-debt
  cost for a rule that does not fit the definition, which is why it is not the default here.

### 6. Put frogctl back in the graded suite

Wiring engines whose tests only run in a non-gating nightly coverage job is not wiring them. Three
edits, not one:

* `.config/nextest.toml:5` — drop (or narrow) `default-filter = 'not package(frogctl)'` so the lib
  unit tests run, **and** update the rationale comment at `:2-4`, which currently explains the
  exclusion as campaign policy.
* `Justfile:80-83` — the `just test frogctl` special case that exits 2 with *"frogctl is excluded
  from the default suite"* becomes false and must go.
* Alternatively add `just frogctl-test` to the CI `test` job.

The `cli-tests` feature gate on the *integration* tests (each spins a `TestServer`) can stay if
runtime is the concern — but the 16 pure unit tests cost milliseconds and there is no reason for
them to be invisible.

## Testability improvement

The interface is the test surface, and today there is no interface — so there is no test surface.

**What cannot be tested today.** `export_dataset`, `import_dataset`, `scan_keyspace`,
`enrich_keys`, `latency_doctor`, `latency_history`, `latency_histogram` all take a live
`MultiplexedConnection`. A `#[cfg(test)]` unit test in the lib cannot supply one — the crate's
server harness (`frogdb_test_harness::server::TestServer`) is a `[dev-dependencies]` entry
(`Cargo.toml:47`) reachable only from `tests/`, and `tests/` reaches the engines' *siblings*
through `frogctl::commands::*` (each `integration_*.rs`; `tests/common/setup.rs:1-3` itself imports
only `frogctl::cli` and `frogctl::connection`). With no `commands` entry point, there is no path
from a running server to these functions. That is why all 16 tests cover pure helpers, and why
`ops/` contains **zero** `#[tokio::test]` (C1): pure helpers are the only thing reachable.

**What the wiring unlocks.** Each adapter creates the missing entry point, and the existing test
idiom applies unchanged:

- **A backup round-trip property.** `TestServer::start_standalone()` → populate keys of several
  types with TTLs → `backup::run(&BackupCommand::Export { … })` → `backup::run(&Verify { dir })`
  (asserting `valid == true` and `keys == n`) → `FLUSHALL` → `backup::run(&Import { … })` →
  assert every key, value and TTL came back. This exercises `SCAN`+`DUMP`+`PTTL`+`RESTORE`
  compatibility end-to-end against FrogDB's own server, which is a **Redis-compatibility test
  nothing in the repo performs today** — `DUMP`/`RESTORE` payload round-tripping through a
  third-party client is precisely the kind of thing the project's compatibility goal cares about.
- **Corruption behaviour, including D7's regression test.** Truncate a `batch_*.dat`, delete a data
  file, corrupt `manifest.json`, **and flip one byte inside a `DUMP` payload** — the last of these
  is the test that fails today and passes after D7 is fixed. `verify_export` has **five**
  `errors.push` spans (`ops/backup.rs:355`, `:365`, `:379-383`, `:386-391`, `:405-409`) and **zero**
  coverage of any of them: its two tests reach the pre-loop manifest-read failure (`:444-449`, via
  `:343-344`) and the empty-archive happy path (`:451-476`). With `backup verify` wired these become
  table-driven adapter tests over real archives produced by real exports.
- **One scan engine, one set of assertions.** After the deletion, the five `integration_scan.rs`
  tests exercise `ops::scan::scan_keyspace` — the same code path `data keyspace` and
  `debug memory bigkeys/memkeys` use. Today they exercise the copy, and the limit/enrichment
  interaction is asserted in **neither** copy (D1/D2). The new test asserts the key set **and** the
  enrichment round-trip count at `--limit 3` over 10 keys — pinning behaviour that is currently
  unpinned, and pinning the swap as the no-op D2 says it is.
- **Assertions that mean something.** All five current scan tests, both data tests and both config
  tests assert only `exit_code == 0` (`integration_scan.rs:33,45,63,78,95`,
  `integration_data.rs:15,28`, `integration_config.rs:15,28`) — a test that passes if the command
  prints nothing. Every new adapter returns a typed summary before rendering, so the tests assert
  on `ScanSummary.total_scanned`, `VerifySummary.errors`, `ImportSummary.keys_restored`.

**And they have to actually run** — hence step 6. Restoring frogctl to the graded suite converts
16 nightly-coverage-only tests plus everything above into gating coverage.

**Prevention, not just coverage.** The count pin (step 5) is the answer to the class of bug that
produced this proposal: a documented, published subcommand whose dispatch arm is a stub. The pin
makes the stub count a reviewed number rather than an emergent one.

## Risks / scope boundaries vs siblings

### Ordering: why 72 (FR2) must land first — verified, with the mechanism corrected

The lane brief asserts "fix #2 before #1" because wiring `ops/` first would promote a latent
schema drift to a live one. **Confirmed — but the original proposal named the wrong rejector.**

The root type `Config` does carry `#[serde(deny_unknown_fields, rename_all = "kebab-case")]`
(`frogdb-server/crates/config/src/lib.rs:83`, on `struct Config` `:84`) — but at that level the
attribute governs **section names** (`server`, `memory`, `persistence`, …), and
`generate_default_config` spells all of those correctly. The actual rejection happens one level
down, at the section structs: `ServerConfig` carries the same attribute
(`frogdb-server/crates/config/src/server.rs:18`, on `struct ServerConfig` `:19`), and
`ops/config.rs` emits **snake_case** field names into it — `num_shards` (`:38`), `max_clients`
(`:40`), `tcp_keepalive` (`:42`), and likewise `max_memory` (`:50`), `maxmemory_policy` (`:53`),
`data_dir` (`:60`) in their own sections.

The first two keys `[server]` emits, `bind` (`:34`) and `port` (`:36`), are **kebab-identical** and
pass. `num_shards` is the **third** key, and it is the first to be rejected. The outcome is
unchanged — a generated file is not partially wrong, it fails to parse outright — but the original's
"rejects the first key of the first section" was mechanically false, and 72's own re-derivation is
the authority here. **The 72-first conclusion stands.**

The failure modes differ sharply by order:

- **72 first (correct).** `config generate` starts life emitting the server's own serde types,
  exactly as ADR-0001 already binds the operator (`CONTEXT-MAP.md`, "Operator → Server (config
  schema, compile-time)"). Nothing user-visible ever regresses.
- **73 first (wrong).** `config generate` becomes reachable and immediately ships a file that
  `frogdb-server` refuses to boot on — worse than the current `bail!`, which at least fails
  honestly and instantly, and it leaves a `frogdb.toml` with the wrong vocabulary on someone's disk.

This proposal therefore **does not touch `ops/config.rs` or the three `commands/config.rs` arms at
all**, in either order. Even so, 72 should land first so the "wire every backed subcommand" story
is complete and reviewable in one direction.

### Boundary vs proposal 72 — **now on disk**

`.scratch/arch-deepening/proposals/72-frogctl-config-schema.md` **exists at `4c36827d`** (the
original's "does not exist at this SHA" caveat is retired). The partitions agree; one factual
divergence is noted for the orchestrator.

- **72 owns:** `frogctl/src/ops/config.rs` (479 lines; 72 takes it to ~150), `frogctl/src/commands/config.rs`
  (139 lines — including the `Generate`/`Validate`/`Diff` arms at `:102`/`:105`/`:108`), the
  `frogdb-config` dependency edge plus the ADR-0001 extension, and the `toml` dependency question
  (`Cargo.toml:43`).
- **73 (this proposal) owns:** `frogctl/src/ops/{backup,scan,latency}.rs`,
  `frogctl/src/commands/{backup,scan,data}.rs`, the five listed arms of
  `frogctl/src/commands/debug.rs`, `frogctl/tests/{main,integration_scan,integration_data}.rs`
  plus a new `integration_backup.rs`, `.config/nextest.toml:2-5`, `Justfile:80-83`, and the new
  `lint-frogctl-bails` recipe (`Justfile:319`).
- **Shared, edit-order-sensitive:** `frogctl/src/ops/mod.rs` (module doc-comment),
  `frogctl/CONTEXT.md` (72 touches `:13`; 73 touches `:60-61`; 75 touches `:19-20`, `:26-28`,
  `:41-43`, `:59`), `frogctl/Cargo.toml` (72 adds `frogdb-config`; 73 may move `tempfile` to
  `[dev-dependencies]` — which 72 must confirm, since `ops/config.rs`'s surviving tests use it),
  and the count-pin table (72 zeroes the `config` row 73 pins: 19 → 16).
- **Factual divergence to reconcile:** 72's file table says `ops/config.rs` has "6 unit tests
  :347-479". At `4c36827d` it has **7** (`grep -c '#\[test\]'`). C1's total of 16 uses the correct
  count.
- **If 72 chooses not to wire its command arms** and only repairs the engine, the adapter pattern
  in step 3 applies to them verbatim as a trailing three-adapter step — but it must be attributed
  to 72, or the schema fix and the wiring land in the wrong order.

### Boundary vs proposal 74 (FR3, debug bundle)

74 owns `debug zip` (`commands/debug.rs:394`) and the server-side bundle machinery. **This
proposal leaves `:394` bailing.** The overlap is one `match` arm in one file: 74 replaces the arm
body at `:393-395`; 73 replaces the arm bodies at `:405`/`:408`/`:411`/`:418`/`:421`. No shared
lines. 74 also owns the unused `zip` dependency — at `Cargo.toml:**40**`, not `:41` (`:41` is
`sha2`); 74 has accepted this correction in its own review round, and the original of this
proposal carried the identical off-by-one, now fixed here in every citation.

### Boundary vs proposal 75 (FR4 rendering + FR5 role enum)

Three explicit handshakes:

1. **`render_value` vs `Renderable`.** 75 makes `Renderable`/`print_output` the sole exit. Every
   adapter this proposal adds already exits through `print_output` — so 73 *adds no new work* for
   75 and removes some (the scan deletion drops a hand-rolled path).
2. **`render_ascii_graph` / `render_histogram_table` live in `ops/latency.rs`** (`:135`, `:188`) —
   rendering inside an operation module. 73 calls them from `render_table` **in place**. If 75 lands
   a rendering-ownership rule, relocating those two functions into the adapter is a mechanical move
   of two pure `String`-returning functions with no call-site change beyond the import.
3. **D6's two silently-ignored flags** (`debug latency --history`, `debug slowlog --all`) are in
   75's family (declared-and-unread options, alongside `no_color`, `tls_cert`, `tls_key`,
   `tls_ca`). **Ownership: 75**, which has since ruled to *implement* the slowlog fan-out over
   `health.rs:284-306` rather than bail. 73's H2 is the interim minimum only — see §Effort.

75 also owns the unused `comfy-table` (`Cargo.toml:38`) and `dialoguer` (`:39`) deps, and
`indicatif` (`:37`) — note that `indicatif` is the *intended* sink for `ops/`'s `on_progress`
callbacks (`ExportProgress`, `ImportProgress`, `ops/backup.rs:7-18`; `on_progress: impl Fn(usize)`,
`ops/scan.rs:43`). This proposal deliberately does **not** adopt `indicatif`: it writes plain
progress lines to stderr, keeping the dependency question wholly in 75's hands. If 75 keeps
`indicatif`, swapping the stderr closure for a progress bar is self-contained inside each adapter.

### Boundary vs the rest of the round (re-checked at HEAD)

All 48 proposal files on disk were re-read at `4c36827d` — including 71, 76, 77 and 78, which the
original recorded as absent or unchecked.

- **Negative findings only** (frogctl named as *not* involved): `49:594`, `67:106`, `67:658`,
  `69:449`, `69:642`, `70:441`, `70:822`. Zero overlap with this file set. (The original cited
  `67:87`, `67:501`, `69:265`, `70:340`, `70:511` — all stale after the intervening revision
  commits, and it missed `49` entirely.)
- **71** (`71-search-query-plan.md`) is on disk and contains **zero** `frogctl` mentions.
- **77** (FR7/FR8, operator child resources) shares this lane and explicitly clears 73 at
  `77:568` ("Different crate entirely; verified no operator file in its set"); it reuses 72's
  finding at `77:299`/`77:550`.
- **78** (test-harness RESP client) cites this proposal at `78:516` for its single read-only
  mention of `frogdb_test_harness::server::TestServer`; no overlap. 78 also builds on 72's schema
  finding (`78:187-192`, `78:515`).
- The open issues in `.scratch/arch-deepening/issues/open/` contain no `frogctl` reference.

### Behavioural risk of the change itself

- **`frogctl scan` output must not change.** The `Renderable for ScanResult` impl
  (`commands/scan.rs:57-132`) is preserved verbatim and `ScanEntry` is kept as a serde shim, so the
  table and the `--output json` field names are both unchanged. Per D2 the engine swap issues the
  same round trips and returns the same keys. The pinning test asserts key set **and** enrichment
  call count at `--limit 3` over 10 keys.
- **The website regenerates.** `just docs-gen` must run; `just docs-gen-check` (`Justfile:817`)
  will fail the build otherwise. Wiring changes no clap types, so `frogctl-cli.json` should be
  byte-identical — itself a useful assertion that the change is dispatch-only. (H2's minimal
  `--all` bail also changes no clap type; only the arm body.)
- **New failure surface for real operators.** Nine commands go from "fails loudly, always" to
  "does something against the live keyspace", and none of it is throttled:
  - `backup export` issues an unbounded SCAN sweep, accumulates every key name client-side (**D8**),
    then `DUMP`+`PTTL` for every key;
  - `data keyspace`, `debug memory bigkeys`, `debug memory memkeys` each force `TYPE` **and**
    `MEMORY USAGE` per sampled key (step 3), i.e. two extra server commands per key on top of the
    SCAN — 20,000 pipelined commands at `data keyspace`'s default `--samples 10000`.
  That is acceptable for operator tooling that is explicitly invoked, but the adapters must state
  the cost in their clap `about` strings, and `--samples` (`commands/data.rs:10-12`) must be
  honoured as a real ceiling rather than silently ignored.
- **`ExportManifest` becomes a compatibility surface.** `MANIFEST_VERSION` (`ops/backup.rs:66`,
  field `version` `:50`) and the length-prefixed data-file layout (`:150-156`) stop being private
  the moment `export` works. D7's digest fix is therefore a **format change that must land in the
  same release as the first working `export`**, bumping the version field — after that, changing
  what the digest covers is a compatibility break rather than a bug fix. The round-trip test pins
  the format.

### Vocabulary

`frogctl/CONTEXT.md`'s *Avoid* list is honoured throughout: "Observability API" (use **Metrics
API**), bare "topology" (use **Cluster Topology** / **Replication Topology**), "Diagnostic bundle"
(use **Debug Bundle** — proposal 74's term). **Export Archive** is used as defined at `:38-39`.
**Fan-out** is used as defined at `:26-28`, which is what makes D6's `slowlog --all` finding a
documented-contract violation rather than a missing feature.

## Spec / LOCKED

**No locked-area exposure, and no mutation-gate implications.**

- The four locked areas are `frogdb-txn`+`frogdb-vll`, `frogdb-persistence`+`frogdb-recovery`,
  `frogdb-replication`+`frogdb-replication-runtime`, `frogdb-cluster`+`frogdb-cluster-runtime`
  (ADRs `adr/0002`–`0004`). **`frogctl` is none of them**, and every file in the §Files table is
  under `frogctl/` except read-only citations (`docs-gen`, `frogdb-config`, `website/`) and two
  build-config edits (`.config/nextest.toml`, `Justfile`).
- **No `FM-` tag exists anywhere in `frogctl/`** — grepped, zero hits. No touched file is named as
  a *Forced by* test in any `.scratch/hardening/specs/*-failure-modes.md`.
- `frogctl` is named twice across the six spec files, both as prose, neither as a contract on this
  code: `cluster-failure-modes.md:56` explicitly places it **out of scope** ("Also out of scope:
  the operator and `frogctl`"), and `replication-failure-modes.md:523` names it only as a
  *consumer* of the replication-identity fields in `INFO replication` — a server-side guarantee
  this proposal does not touch. (`frogctl/src/info_parser.rs` is not in this proposal's file set;
  it is proposal 75's, via FR5.)
- `just lint-failure-modes` is unaffected: it walks spec rows against tagged tests, and this
  change adds neither.
- **Mutation gates: none apply.** `just mutants-diff <crate>` push discipline covers the four
  locked crate pairs; `frogctl` has no gate and is not a `cargo mutants` target in any recipe.
  Nothing here changes that, and no gate percentage moves.
- The one new lint (`lint-frogctl-bails`) is a compile-free grep joining `just lint`
  (`Justfile:319`) and **not** `lint-gates` (`:329`) — see step 5. It states an invariant about
  *stub counts*, not about a locked-area seam, so it needs no spec row, and the `seam-gates` CI job
  is unchanged.
- `just check frogctl` is green at `4c36827d` (verified), so the baseline this proposal edits from
  compiles clean.

## Effort

**M.**

| step | size | note |
|---|---|---|
| 1. Layer rule in `ops/mod.rs` + `CONTEXT.md` | XS | doc only |
| 2. `scan` through the engine, delete the duplicate | S | ~95 lines removed, ~15 added (incl. the `From<KeyInfo>` shim); behaviour- and JSON-preserving |
| 3. Nine adapters (3 backup, 3 scan-backed, 3 latency) | M | ~25-30 lines each; three real argument decisions (`samples`→`limit`, `top` u64→usize, `Option<String>`→`"*"`); forced `with_type`/`with_memory` documented |
| 4. Fix D3, D4, D5, D7 (+ D8 issue) | S | four localized fixes in `ops/backup.rs`; D7 touches write **and** verify digests plus `MANIFEST_VERSION`. D2 needs no fix — retracted |
| 5. `lint-frogctl-bails` count pin + `just lint` wiring | S | grep recipe; pins 19 (→16 after 72) |
| 6. frogctl back into the graded suite | S | `.config/nextest.toml:5` **plus** its `:2-4` rationale comment **plus** the `just test frogctl` refusal at `Justfile:80-83`; expect the 16 unit tests to surface (was mis-budgeted XS) |
| 7. Tests: backup round-trip, verify-corruption table (incl. the D7 payload-flip regression), scan limit/enrichment, keyspace summary | M | the real work; needs `TestServer` for each |
| 8. `just docs-gen` (expect no diff) + `frogctl.mdx:50` example fix | XS | |

Not L: no new abstraction is introduced, no crate boundary moves, no dependency is added, and the
engines themselves are near-unchanged. Not S: nine adapters plus a real test suite plus three
argument decisions plus an archive-format fix is more than an afternoon, and step 7 is where most
of it lives.

### Independently-landable hotfixes

**H1 — `website/.../reference/frogctl.mdx:50` documents a subcommand that does not exist.**
**CONFIRMED LIVE. XS. One word.**
*Trace:* the published reference page's Examples block reads ```frogctl backup snapshot``` →
`BackupCommand` (`commands/backup.rs:12-63`) declares `Trigger`, `Status`, `Export`, `Import`,
`Verify` — there is no `Snapshot` → clap exits 2 with `unrecognized subcommand 'snapshot'`. The
correct spelling is `frogctl backup trigger`, which the operations guide already uses
(`website/src/content/docs/operations/backup-restore.md:34`). *Owner:* free — hand-written `.mdx`,
not generated.
**Sequencing note (textual collision):** proposal 75's hotfix H6 edits `frogctl.mdx:56` — the TLS
example — inside the **same fenced ```bash block** (the block runs `:30`–`:64`). The two edits are
six lines apart in one code fence, so whichever lands second rebases by hand. They are independent
in content; only the diff context collides. Land H1 first (one word, no dependency) and 75's H6
absorbs the shift.

**H2 — `frogctl debug slowlog --all` silently ignores its addresses. SPLIT.**
*Trace:* `DebugCommand::Slowlog { … all: Option<Vec<String>> … }` is declared at
`commands/debug.rs:88-90` with help "Collect from multiple nodes" → published to
`website/src/data/frogctl-cli.json` via `docs-gen` (`main.rs:329`) → the dispatch destructures
`{ count, analyze, reset, .. }` at `:427-431`, dropping `all` → `run_slowlog(*count, *analyze,
*reset, ctx)` queries `ctx`'s single default node. No warning, no error; the operator gets one
node's slow log and believes it is the cluster's. This **contradicts `CONTEXT.md:26-28`**, which
names slowlog as a Fan-out command, and diverges from `health`, which does honour `--all`
(`commands/health.rs:131`, `run_fanout` `:284-306`).
*Ruling:* **73 lands the minimal honest fix now** — a `bail!` when `all.is_some()`, in 73's own
match arm at `commands/debug.rs:427-431`, which 73 is already editing. Two lines, no clap change,
no doc regeneration. **The real Fan-out implementation is owned by proposal 75**, whose author has
since ruled to implement it over `health.rs:284-306` rather than bail — 75 concurs that fan-out is
the end state, and 73's bail is strictly an interim that 75 replaces. If the orchestrator's
consistency sweep prefers 75 to land first, 73 simply drops its bail; the arm-body edits do not
otherwise interact. *Size:* XS here, S in 75.

**H3 — `frogctl debug latency --history` silently ignores the flag. AGREE → file as an issue.**
Same class, same file (`commands/debug.rs:44-46` declared → `:396-402` dropped in `..` →
`run_latency` `:444-449` never sees it). It is **silently wrong today**, so it deserves a tracked
record — but it is not hotfixable the way H2 is: unlike `--all`, the flag has no correct
implementation to fall back to ("periodic snapshot mode (every 15s)" is an unwritten feature), so
the only honest standalone fix is to **remove the flag**, which is a CLI-surface change that
regenerates `frogctl-cli.json` and belongs to **75** (declared-and-unread options). *Action:* file
an issue against 75's family recording the trace and the two options (implement the 15s snapshot
loop, or remove the flag); do not fold it into 73.

### Deliberately not hotfixed

- **`tempfile` in `[dependencies]`** (`Cargo.toml:44`) while used only from `#[cfg(test)]`
  (`ops/backup.rs:446,455`; `ops/config.rs:372,396,414,432,451`). Real build hygiene — it pulls
  `tempfile` into the shipped binary's dependency graph — but **not user-observable**, so not a
  hotfix. Fold it into step 3, coordinating with 72 (whose surviving `ops/config.rs` tests also use
  it).
- **Hiding the still-unimplemented subcommands from `--help`/docs** (`#[command(hide = true)]`),
  which would stop the website advertising the remaining 19 stubs. Defensible, and arguably the
  correct interim honesty — but it edits `cli.rs` and the command enums that proposals 74 and 75
  both touch, and it is the *delete* branch's opening move applied to commands this proposal
  argues should eventually be wired. Flagged for the orchestrator; not proposed here.
