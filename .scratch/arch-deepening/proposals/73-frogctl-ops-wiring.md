# Proposal 73 — frogctl `ops/`: wire the orphaned operation modules, delete the duplicated scan loop

Round 38 · lane: frogctl / operator / telemetry · effort **M** · candidate FR1 · ordered **after**
proposal 72 (FR2)

Verified against the current tree at `159cb7a26b459bccb11b8f04130444f660d0a9f6` (worktree
`arch-round-38-99`, branch `main`, clean). Every path, line number and count below was re-derived
by reading at that SHA. **Three lane-brief claims are corrected** and **five defects the brief did
not name were found** — see §Problem.

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

**Nine of those 28 have a complete, matching implementation already sitting in `ops/`.** The
argument shapes line up 1:1 — `BackupCommand::Export { output, match_pattern, count, key_type }`
(`commands/backup.rs:21-37`) against
`export_dataset(conn, output_dir, pattern, key_type, batch, on_progress)`
(`ops/backup.rs:69-76`) — which is decisive evidence that `ops/` was written *as* the
implementation of these subcommands and only the ~30-line-per-command adapter was skipped.

**The deletion test does not come out clean, and the reason is not sentiment about lost work — it
is that the domain glossary already asserts the wiring exists.** `frogctl/CONTEXT.md:60-61` states
as a *relationship*: "Shared engines back multiple commands: the scan engine powers `scan`,
`data keyspace`, and `debug memory bigkeys/memkeys`; the backup engine powers
`backup export/import`." `CONTEXT.md:38-39` defines **Export Archive** as a first-class domain
term ("the `backup export` artifact: a directory with `manifest.json` plus per-key data files").
And `CONTEXT.md:47-52` records a *deliberate naming decision* — the four `data` alias commands
were removed so that `debug memory` and `backup` are the canonical homes for bigkeys/memkeys and
export/import (commit `c47f443e`, `.scratch/naming-cleanup/issues/03`). Deleting `ops/` therefore
deletes a documented capability and forces three separate retractions (glossary term, engine
relationship, canonical-home decision) plus a website surface shrink. That is capability
deletion, not complexity deletion — and this proposal takes the **wire** branch.

**One thing does delete, and it is the sharpest finding here.** `commands/scan.rs:142-235` is a
line-for-line reimplementation of `ops::scan::scan_keyspace` + `enrich_keys`
(`ops/scan.rs:40-167`) — same SCAN/MATCH/COUNT/TYPE loop, same enrichment pipeline, same
`fields_per_key` index arithmetic, down to the same `let _ = idx;` warning-suppression line
(`commands/scan.rs:200` ≡ `ops/scan.rs:152`). Two copies of the keyspace scan engine exist, the
live one is the copy, and the tested one is the orphan. Wiring `scan` through `ops/scan.rs`
deletes ~95 lines at the caller and makes the CONTEXT.md sentence true.

The change: write the missing adapters for the nine backed subcommands, route `scan` through the
one scan engine, and add a **count pin** (`lint-gates` family idiom) on the remaining
`not yet implemented` arms so the number can only go down.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogctl/src/ops/mod.rs` | 4 | **read-only evidence.** Declares `backup`/`config`/`latency`/`scan`. Unchanged by this proposal |
| `frogctl/src/lib.rs` | 7 | **read-only evidence.** `pub mod ops;` at `:5` — the reason no `dead_code` warning ever fired |
| `frogctl/src/ops/backup.rs` | 477 | **wired (near-unchanged).** `export_dataset` `:69-226`, `import_dataset` `:229-296`, `verify_export` `:339-418`, `parse_data_file` `:299-336`. Three latent defects fixed on the way in (§Problem D3–D5). Types `ExportSummary` `:21-27`, `ImportSummary` `:29-35`, `VerifySummary` `:37-45`, `ExportManifest` `:47-63`, `ExportProgress` `:7-12`, `ImportProgress` `:14-18` |
| `frogctl/src/ops/scan.rs` | 309 | **wired.** `scan_keyspace` `:40-101`, `enrich_keys` `:104-167`, `summarize_keyspace` `:179-206`, `find_bigkeys` `:209-237`, `format_key_info` `:240-247`. `ScanOpts` `:7-15`, `KeyInfo` `:17-27`, `ScanSummary` `:29-34`, `KeyspaceTypeSummary` `:169-176`. Becomes the *only* scan engine |
| `frogctl/src/ops/latency.rs` | 261 | **wired.** `latency_doctor` `:27-34`, `latency_history` `:37-65`, `latency_histogram` `:68-84`, `parse_histogram_response` `:90-108`, `render_ascii_graph` `:135-185`, `render_histogram_table` `:188-203`. The last two are rendering living in an operation module — see §Risks (boundary with 75) |
| `frogctl/src/ops/config.rs` | 479 | **NOT TOUCHED — owned by proposal 72.** Cited here only for the ordering argument (§Risks). `generate_default_config` `:25-…` emits snake_case keys (`num_shards` `:37`, `max_clients` `:39`, `max_memory` `:49`, `maxmemory_policy` `:52`, `data_dir` `:59`) against a server schema that is `deny_unknown_fields, rename_all = "kebab-case"` |
| `frogctl/src/commands/backup.rs` | 190 | **the change.** `run` `:130-144` — the three `bail!`s at `:135`/`:138`/`:141` become adapters. Arg surface `:20-62` already matches the engine 1:1. `Renderable for PersistenceStatus` `:76-128` is the pattern the three new impls follow |
| `frogctl/src/commands/scan.rs` | 243 | **the change (deletion).** `run` `:134-243`; the duplicated engine `:142-235` (~95 lines) deleted and replaced by one `scan_keyspace` call. `ScanEntry` `:40-49` folds into `ops::scan::KeyInfo`; `Renderable for ScanResult` `:57-132` **stays** (it is the adapter's job) |
| `frogctl/src/commands/data.rs` | 119 | **the change.** `run` `:33-43` — `bail!` at `:36` (`data keyspace`) becomes an adapter over `scan_keyspace` + `summarize_keyspace`. `:39` (`data pipe`) **stays a bail** — no engine backs it |
| `frogctl/src/commands/debug.rs` | 770 | **the change.** `run` `:391-440`. Six `bail!`s become adapters: `:405`/`:408`/`:411` (latency doctor/graph/histogram), `:418`/`:421` (memory bigkeys/memkeys). Five `bail!`s **stay**: `:394` (zip → proposal 74), `:425` (hotshards), `:434` (vll). Subcommand shapes `LatencySubcommand` `:120-137`, `MemorySubcommand` `:139-164` |
| `frogctl/src/commands/config.rs` | 139 | **NOT TOUCHED — owned by proposal 72.** `run` `:99-112`, `bail!`s at `:102`/`:105`/`:108` |
| `frogctl/src/output.rs` | 173 | **read-only evidence — the seam.** `trait Renderable` `:3-7`, `print_output` `:9-16`. Every new adapter exits here. `render_value` `:19-29` is the competing path (proposal 75's problem, not this one) |
| `frogctl/src/cli.rs` | 143 | **read-only evidence.** `GlobalOpts.output` `:64-66`, `.no_color` `:68-70`; `enum Commands` `:80-143`. **The doc surface** — `docs-gen` walks exactly this |
| `frogctl/src/connection.rs` | 193 | **read-only evidence.** `ConnectionContext::resp` `:32-49` is how an adapter hands a connection to an engine. `global()` `:180-182` supplies output mode |
| `frogctl/src/util.rs` | 180 | **read-only evidence.** `extract_string`/`extract_int`/`extract_int_opt` `:4-30`, `format_bytes` `:45-59` — already shared by both `ops/` and `commands/`, so the layering is already half-built |
| `frogctl/CONTEXT.md` | 78 | **the change (doc).** `Export Archive` `:38-39`; canonical command homes `:47-52`; **the engine relationship `:60-61`** — today a false statement, made true by this change. Its *Avoid* list is honored below |
| `frogctl/Cargo.toml` | 47 | **read-only evidence + one edit.** `cli-tests` feature `:19-20`, `[[test]] required-features` `:22-25`. Deps `:34-44`: `sha2`/`hex` exist **only** for `ops/backup.rs`; `indicatif` `:38`, `comfy-table` `:39`, `dialoguer` `:40`, `zip` `:41` have **zero** uses anywhere in the crate; `tempfile` `:44` sits in `[dependencies]` but is used only from `#[cfg(test)]` (`ops/backup.rs:446,455`, `ops/config.rs:372+`) |
| `frogctl/tests/main.rs` | 11 | **the change (tests).** The `mod` list gains `integration_backup` |
| `frogctl/tests/integration_scan.rs` | 96 | **the change (tests).** Five tests that assert only `exit_code == 0` — no output is inspected. Strengthened to pin engine behavior |
| `frogctl/tests/integration_data.rs` | 29 | **the change (tests).** Only `data slot` is covered; `data keyspace` gains coverage |
| `frogctl/tests/common/setup.rs` | 69 | **read-only evidence.** `ctx_for_server(&TestServer)` — the harness hook every new integration test uses |
| `.config/nextest.toml` | — | **the change.** `default-filter = 'not package(frogctl)'` at `:5` — frogctl's **lib unit tests are excluded from `just test` too**, not only its integration tests (§Problem D1) |
| `Justfile` | — | **read-only evidence.** `frogctl-test` `:297-298` (`--features cli-tests --ignore-default-filter`); `coverage-lcov` `:101-104` (the only place these tests execute today); `docs-gen` `:812-813`, `docs-gen-check` `:816-817`; `lint-gates` `:329` (where the new count pin joins) |
| `frogdb-server/ops/docs-gen/src/main.rs` | — | **read-only evidence.** `generate_cli_reference(frogctl::cli::Cli::command(), …)` `:329`, written to `frogctl-cli.json` `:333`. The mechanism that publishes phantom commands |
| `website/src/data/frogctl-cli.json` | 9,019 | **read-only evidence (generated).** `generate` `:431`, latency `doctor` `:2860`, `histogram` `:3049`, memory `doctor` `:3332`, `bigkeys` `:3424`, `export` `:4350`, `import` `:4460`, `verify` `:4578`, `keyspace` `:4769`. Regenerated by `just docs-gen`; **not edited by hand** |
| `website/src/content/docs/reference/frogctl.mdx` | — | **read-only evidence + hotfix H1.** `:50` documents `frogctl backup snapshot` — a subcommand that has never existed |
| `frogdb-server/crates/config/src/lib.rs` | — | **read-only evidence.** `#[serde(deny_unknown_fields, rename_all = "kebab-case")]` at `:83` on `struct Config` `:84`. The schema `ops/config.rs` contradicts — the whole basis for 72-before-73 |

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
dispatches 17 top-level commands (`main.rs:22-38`) and touches `commands::` only.

1,530 lines: `backup.rs` 477 + `scan.rs` 309 + `latency.rs` 261 + `config.rs` 479 + `mod.rs` 4.
The lane brief's total is correct. Its characterisation of them as "1530 tested lines" is not —
see D1.

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

Nine have a complete engine already in `ops/`:

| subcommand | bails at | engine | arg match |
|---|---|---|---|
| `backup export` | `commands/backup.rs:135` | `ops/backup.rs:69` `export_dataset` | 1:1 (`output`→`output_dir`, `match`→`pattern`, `type`→`key_type`, `count`→`batch`) |
| `backup import` | `:138` | `ops/backup.rs:229` `import_dataset` | 1:1 (`input`, `replace`, `pipeline`→`pipeline_depth`, `ttl`→`preserve_ttl`) |
| `backup verify` | `:141` | `ops/backup.rs:339` `verify_export` | 1:1 (`dir`); **needs no connection** |
| `data keyspace` | `commands/data.rs:36` | `scan_keyspace` + `summarize_keyspace` | `samples` → `ScanOpts.limit` (needs a decision, see below) |
| `debug memory bigkeys` | `commands/debug.rs:418` | `scan_keyspace` + `find_bigkeys` | `type`→`key_type`, `top` (u64→usize), `samples`→`limit` |
| `debug memory memkeys` | `:421` | `scan_keyspace` + `summarize_keyspace` | no args |
| `debug latency doctor` | `:405` | `ops/latency.rs:27` `latency_doctor` | no args |
| `debug latency graph` | `:408` | `latency_history` + `render_ascii_graph` | `event` 1:1; graph `width` is an adapter choice |
| `debug latency histogram` | `:411` | `latency_histogram` + `render_histogram_table` | `commands: Vec<String>` 1:1 |

The remaining 19 have no implementation anywhere: 10 `cluster`, 3 `config` (proposal 72), `data
pipe`, `debug zip` (proposal 74), `debug hotshards`, `debug vll`, `replication lag`,
`upgrade node`. They are out of scope here and stay bailing.

The wiring is **not** purely mechanical at two points, which is why this is M and not S:

- `MemorySubcommand::Bigkeys { samples }` (`commands/debug.rs:157-159`, "SCAN sample count
  (0 = full scan)") has no counterpart in `ScanOpts` (`ops/scan.rs:7-15`), which offers
  `limit: Option<usize>`. `samples == 0` → `None`; otherwise `Some(n)`. `DataCommand::Keyspace
  { samples }` (`commands/data.rs:10-12`, default 10 000) maps the same way but with a non-zero
  default, so "sample" means *ceiling* in both, and the summary must say so rather than implying
  a full-keyspace count.
- `find_bigkeys(keys, top: usize)` takes `usize`; the CLI gives `top: u64`
  (`commands/debug.rs:154-155`). One cast, but it belongs in the adapter, not the engine.

### Everything published, nothing reachable

`docs-gen` depends on the `frogctl` **library** (`frogdb-server/ops/docs-gen/Cargo.toml:14`) and
reflects over the clap tree at `main.rs:329`. So `website/src/data/frogctl-cli.json` carries full
flag documentation for `export` (`:4350`), `import` (`:4460`), `verify` (`:4578`), `keyspace`
(`:4769`), `bigkeys` (`:3424`), latency `doctor` (`:2860`) and `histogram` (`:3049`), `generate`
(`:431`) — every one of which fails at runtime. The published reference page renders that JSON
through `<CliCommandsTable source="frogctl-cli" />`
(`website/src/content/docs/reference/frogctl.mdx:25`).

This is the **LIVE** part of FR1: an operator reads the shipped documentation, runs
`frogctl backup export -o /backups`, and gets `Error: frogctl backup export: not yet
implemented` — from a binary that is built and shipped in every release
(`.github/workflows/release.yml:119,123`).

### Corrections to the lane brief

**C1 — "1530 tested lines" overstates coverage twice over.** The 1,530 lines carry **16**
`#[test]` functions total (backup 3, scan 2, latency 4, config 7), and every one of them tests
only a *pure* helper: `parse_data_file` round-trip, `verify_export` on an empty archive,
`render_ascii_graph`, `render_histogram_table`, `summarize_keyspace`, `find_bigkeys`. The four
connection-taking functions — `export_dataset`, `import_dataset`, `scan_keyspace`, `enrich_keys`,
`latency_*` — have **zero** tests, because a unit test has no server. That is where all the
protocol risk lives.

**C2 — those tests do not run in `just test`, and the brief did not say so.**
`.config/nextest.toml:5` sets `default-filter = 'not package(frogctl)'`, which drops frogctl's
**lib unit tests** as well as its integration tests (the integration tests are separately gated
behind `required-features = ["cli-tests"]`, `frogctl/Cargo.toml:22-25`). No workflow under
`.github/workflows/` invokes `just frogctl-test` (`Justfile:297-298`) — grepped. The **only**
place these 16 tests execute is `just coverage-lcov` (`Justfile:104`, `--features
frogctl/cli-tests --ignore-default-filter`), driven by the nightly coverage job
(`coverage-nightly.yml:79`), which measures and does not gate. So `ops/` is orphaned from the
binary *and* from CI.

**C3 — the brief lists the bailing arms as "commands/config.rs:102-108, backup.rs:135-141,
data.rs:36, debug.rs:405-421"; that undercounts by more than half.** The true count is 28 across
seven modules (table above), including ten in `cluster.rs` the brief never mentions. The brief's
`debug.rs:405-421` range also silently omits `:394` (zip), `:425` (hotshards) and `:434` (vll).

### Defects the brief did not name

**D1 — `commands/scan.rs` is a second copy of the scan engine.** `commands/scan.rs:142-235`
against `ops/scan.rs:51-93` + `:109-164`: identical `SCAN cursor MATCH pattern COUNT n [TYPE t]`
construction, identical `redis::pipe()` enrichment ordering (TYPE, then TTL, then MEMORY USAGE),
identical `fields_per_key = with_type as usize + with_ttl as usize + with_memory as usize`
arithmetic, identical `let _ = idx;` suppression (`commands/scan.rs:200` ≡ `ops/scan.rs:152`),
identical `truncate(limit)` tail. `ScanEntry` (`commands/scan.rs:40-49`) and `KeyInfo`
(`ops/scan.rs:17-27`) are the same four fields with one renamed (`memory` vs `memory_bytes`). Two
engines, one live and untested-in-CI, one tested-in-nightly-only and unreachable. Any future SCAN
fix — cursor-guarantee handling, `MEMORY USAGE` on a missing key, the `enrich`-limit interaction —
lands in one and not the other.

**D2 — `data keyspace` and `debug memory memkeys` would produce different answers than `scan`
does today**, because the live scan applies `limit` *after* enrichment while the engine applies it
inside the enrichment loop with an early `break` (`ops/scan.rs:69-71`). Latent today; the wiring
must pick one. The engine's behaviour is the correct one (it stops issuing enrichment round-trips
once the limit is hit).

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
  --all a:6379 b:6379` queries only the default node. This **contradicts `CONTEXT.md:34-36`**,
  which defines **Fan-out** as "the `--all <addrs>` pattern where a command queries a list of
  nodes and merges results (health, hotshards, slowlog)". `health` does honour it
  (`commands/health.rs:131`); `slowlog` does not.

Both live in the dispatch `match` this proposal edits, so they are called out here — but their
*family* (declared-and-unread options) is proposal 75's. See §Risks for the handshake and §Effort
for the hotfixes.

### Why this is depth, not just plumbing

`export_dataset` is a 158-line, three-phase operation (SCAN sweep → `DUMP`+`PTTL` pipelining into
a length-prefixed wire format with per-batch SHA-256 → manifest with rollup checksum) behind a
six-parameter interface returning four fields. `verify_export` is 80 lines of archive validation
behind `fn(&Path) -> Result<VerifySummary>` and needs no connection at all. That is good
**depth**: a small interface over substantial machinery. The missing piece is ~30 lines of
**adapter** per subcommand. The reason to wire rather than delete is that the engines are already
deep and already shaped for the seam; what is absent is the cheapest layer in the crate.

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
`run` becomes: build `ScanOpts` from `ScanArgs`, call `scan_keyspace`, wrap the returned
`ScanSummary` in the existing `ScanResult`/`Renderable` impl (`:57-132`, unchanged), call
`print_output`. `ScanEntry` (`:40-49`) is replaced by `ops::scan::KeyInfo`; the one field rename
(`memory` → `memory_bytes`) is absorbed inside `render_table`, so **the rendered output and the
`--output json` shape are unchanged**.

Net: ~95 lines deleted, one engine remains, and `CONTEXT.md:60-61` becomes true for the first
time.

### 3. Write the nine adapters

Each is the same shape. `backup export`, in full:

- get `ctx.resp()`, build the four arguments from the clap variant,
- call `export_dataset`, passing a progress closure that writes to **stderr** — never stdout,
  because stdout carries the `--output json` document and must stay machine-parseable,
- wrap `ExportSummary` in a local `Renderable` and `print_output` it,
- return `0`.

`backup verify` skips the connection entirely (`verify_export` takes only a `&Path`) and returns
a **non-zero exit code** when `VerifySummary.valid == false` — an archive checker that always
exits 0 is useless in a cron job, and the `run` signature (`-> Result<i32>`) already supports it;
`commands/cluster.rs:356` (`report.exit_code()`) is the in-crate precedent.

`debug latency graph` and `debug latency histogram` call the engine and then delegate their
`render_table` bodies to `render_ascii_graph` / `render_histogram_table` **in place** in
`ops/latency.rs`. Those two functions are rendering that lives in an operation module — a
**locality** violation against the rule in step 1 — but relocating them collides head-on with
proposal 75's rendering-ownership work, so this proposal calls the debt out and leaves the move
to 75. `render_json` on those adapters serializes the engine's typed data
(`LatencyHistoryPoint`, `CommandHistogramEntry`), not the ASCII art, so `--output json` is
correct regardless of where the table renderer ends up living.

`data keyspace`, `debug memory bigkeys`, `debug memory memkeys` are three adapters over the *same*
engine call with different post-processing (`summarize_keyspace`, `find_bigkeys`,
`summarize_keyspace`) — which is precisely the "shared engine powers three commands" claim in
`CONTEXT.md:60-61`.

### 4. Fix the four latent defects the wiring exposes

D3 (import swallows I/O failure → propagate the error), D4 (report `keys_skipped` from the
returned summary), D5 (parse the version out of the INFO line), D2 (adopt the engine's
limit-inside-enrichment semantics, and pin it with a test).

### 5. Count-pin the remaining bails

Add `just lint-frogctl-bails` to the `lint-gates` family (`Justfile:329`) — a grep that asserts
the number of `not yet implemented` bails per command module equals a pinned table, in the idiom
of `lint-continuation-lock`'s per-enum arm counts (`agents/seam-lints.md`). After this proposal
the pin reads: cluster 10, debug 5, config 3 (or 0 after proposal 72), data 1, replication 1,
upgrade 1 — total 21 (18 after 72). A new stub cannot be added and an implemented one cannot be
quietly re-stubbed without editing the pin. This is the systematic answer to "how did 28 dead arms
accumulate while the docs advertised all of them": nothing counted them.

### 6. Put frogctl back in the graded suite

Remove `not package(frogctl)` from `.config/nextest.toml:5` for the lib unit tests, or add
`just frogctl-test` to the CI `test` job. Wiring engines whose tests only run in a non-gating
nightly coverage job is not wiring them. The `cli-tests` feature gate on the *integration* tests
(each spins a `TestServer`) can stay if runtime is the concern — but the 16 pure unit tests cost
milliseconds and there is no reason for them to be invisible.

### The deletion test, applied honestly

**`ops/config.rs` — not this proposal's call** (proposal 72 owns it, and 72 replaces its body
outright).

**`ops/scan.rs::scan_keyspace` + `enrich_keys` — deletes clean *in isolation*, and that is exactly
the trap.** Removing them costs nothing today because the caller already carries a verbatim copy.
But the right resolution of a duplicate is to keep one copy, and the copy to keep is the one
already shaped as an engine, already covered by unit tests, and already named by the domain
glossary. So: the *caller's* copy is deleted, not the engine's. Net line change is negative.

**`ops/scan.rs::summarize_keyspace` / `find_bigkeys` / `format_key_info` — do not delete clean.**
No caller-side twin exists. Deleting them means deleting `data keyspace` and `debug memory
bigkeys/memkeys` from `cli.rs`, which means shrinking the published reference, which means
retracting `CONTEXT.md:49` ("bigkeys / memkeys → `debug memory` (canonical)") and re-opening
`.scratch/naming-cleanup/issues/03`, whose whole content was choosing those homes.

**`ops/backup.rs` — does not delete clean.** Same, plus: `Export Archive` is a defined glossary
term (`CONTEXT.md:38-39`) describing a persisted on-disk artifact format; `sha2` and `hex`
(`Cargo.toml:42-43`) exist for nothing else and would go with it; and `backup verify` is the only
frogctl subcommand that needs no server at all, which makes it the cheapest thing in the crate to
test end-to-end.

**`ops/latency.rs` — does not delete clean.** No twin, and `LATENCY DOCTOR`/`HISTORY`/`HISTOGRAM`
are server capabilities with no other client-side reader in the repo.

**Verdict: wire.** Every public item in `ops/{backup,scan,latency}.rs` has an existing,
argument-compatible CLI home; the only thing with two homes is the scan loop, and this proposal
removes the wrong one. The **leverage** is lopsided: ~230 lines of new adapter and ~95 deleted
turn 1,051 lines of unreachable engine into nine working subcommands and collapse a duplicated
engine — and every one of those subcommands is already documented, already argument-shaped, and
already promised by the glossary.

## Testability improvement

The interface is the test surface, and today there is no interface — so there is no test surface.

**What cannot be tested today.** `export_dataset`, `import_dataset`, `scan_keyspace`,
`enrich_keys`, `latency_doctor`, `latency_history`, `latency_histogram` all take a live
`MultiplexedConnection`. A `#[cfg(test)]` unit test in the lib cannot supply one — the crate's
server harness (`frogdb_test_harness::server::TestServer`) is a `[dev-dependencies]` entry used
only from `tests/`, and `tests/` reaches the crate through `frogctl::commands::*`
(`tests/common/setup.rs:1-2` + every `integration_*.rs`). With no `commands` entry point, there is
no path from a running server to these functions. That is why 16 tests cover only pure helpers
(C1): it is the only thing reachable.

**What the wiring unlocks.** Each adapter creates the missing entry point, and the existing test
idiom applies unchanged:

- **A backup round-trip property.** `TestServer::start_standalone()` → populate keys of several
  types with TTLs → `backup::run(&BackupCommand::Export { … })` → `backup::run(&Verify { dir })`
  (asserting `valid == true` and `keys == n`) → `FLUSHALL` → `backup::run(&Import { … })` →
  assert every key, value and TTL came back. This exercises `SCAN`+`DUMP`+`PTTL`+`RESTORE`
  compatibility end-to-end against FrogDB's own server, which is a **Redis-compatibility test
  nothing in the repo performs today** — `DUMP`/`RESTORE` payload round-tripping through a
  third-party client is precisely the kind of thing the project's compatibility goal cares about.
- **Corruption behaviour.** Truncate a `batch_*.dat` file, flip a byte in `manifest.json`, delete
  a data file — `verify_export`'s four error paths (`ops/backup.rs:355`, `:365`, `:379-383`,
  `:385-391`, `:404-409`) are currently covered by exactly one test (an *empty* archive,
  `:451-476`). With `backup verify` wired, these become table-driven adapter tests with real
  archives produced by real exports.
- **One scan engine, one set of assertions.** After the deletion, the five
  `integration_scan.rs` tests exercise `ops::scan::scan_keyspace` — the same code path
  `data keyspace` and `debug memory bigkeys/memkeys` use. Today they exercise the copy, and the
  engine's behaviour under `--limit` with enrichment (D2) is asserted nowhere.
- **Assertions that mean something.** All five current scan tests, both data tests and both config
  tests assert only `exit_code == 0` (`integration_scan.rs:34,47,66,81,97`,
  `integration_data.rs:15,27`, `integration_config.rs:15,28`) — a test that passes if the command
  prints nothing. Every new adapter returns a typed summary before rendering, so the tests assert
  on `ScanSummary.total_scanned`, `VerifySummary.errors`, `ImportSummary.keys_restored`.

**And they have to actually run** — hence step 6. Restoring frogctl to the graded suite converts
16 nightly-coverage-only tests plus everything above into gating coverage.

**Prevention, not just coverage.** The count pin (step 5) is the seam-lint answer to the class of
bug that produced this proposal: a documented, published subcommand whose dispatch arm is a stub.
The pin makes the stub count a reviewed number rather than an emergent one.

## Risks / scope boundaries vs siblings

### Ordering: why 72 (FR2) must land first — verified

The lane brief asserts "fix #2 before #1" because wiring `ops/` first would promote a latent
schema drift to a live one. **Confirmed, and the mechanism is exact.**

`frogdb-config`'s root type carries `#[serde(deny_unknown_fields, rename_all = "kebab-case")]`
(`frogdb-server/crates/config/src/lib.rs:83-84`). `ops::config::generate_default_config`
(`ops/config.rs:25+`) emits **snake_case** keys — `num_shards` (`:37`), `max_clients` (`:39`),
`tcp_keepalive` (`:41`), `max_memory` (`:49`), `maxmemory_policy` (`:52`), `data_dir` (`:59`) —
so `deny_unknown_fields` rejects the **first key of the first section**. A generated file is not
partially wrong; it fails to parse outright.

The failure modes therefore differ sharply by order:

- **72 first (correct).** `config generate` starts life emitting
  `toml::to_string_pretty(&Config::default())`, i.e. the server's own serde types, exactly as
  ADR-0001 already binds the operator (`CONTEXT-MAP.md`, "Operator → Server (config schema,
  compile-time)"). Nothing user-visible ever regresses.
- **73 first (wrong).** `config generate` becomes reachable and immediately ships a file that
  `frogdb-server` refuses to boot on. That is a *worse* outcome than the current `bail!`, which
  at least fails honestly and instantly. It also creates a support artifact — a `frogdb.toml` on
  someone's disk with the wrong vocabulary — that outlives the fix.

This proposal therefore **does not touch `ops/config.rs` or the three `commands/config.rs`
arms at all**, in either order. Even so, 72 should land first so that the "wire every backed
subcommand" story is complete and reviewable in one direction.

### Boundary vs proposal 72 — **stated as a caveat: 72 is not on disk at this SHA**

`.scratch/arch-deepening/proposals/72-frogctl-config-schema.md` **does not exist** at
`159cb7a2`; it is being authored concurrently. The boundary is therefore declared here for the
orchestrator to reconcile:

- **72 owns:** `frogctl/src/ops/config.rs` (all 479 lines), `frogctl/src/commands/config.rs`
  (all 139 lines — including the `Generate`/`Validate`/`Diff` arms at `:102`/`:105`/`:108` and
  the `run` dispatch at `:99-112`), the `toml` dependency (`Cargo.toml:43`), and any new
  `frogdb-config` dependency edge plus the ADR-0001 extension.
- **73 (this proposal) owns:** `frogctl/src/ops/{backup,scan,latency}.rs`,
  `frogctl/src/commands/{backup,scan,data}.rs`, the nine listed arms of
  `frogctl/src/commands/debug.rs`, `frogctl/tests/{main,integration_scan,integration_data}.rs`
  plus a new `integration_backup.rs`, `.config/nextest.toml:5`, and the new `lint-frogctl-bails`
  recipe.
- **Shared, edit-order-sensitive:** `frogctl/src/ops/mod.rs` (module doc-comment),
  `frogctl/CONTEXT.md` (72 may touch the config vocabulary; 73 touches `:60-61`),
  `frogctl/Cargo.toml` (72 may add `frogdb-config`; 73 may move `tempfile` to
  `[dev-dependencies]`), and the count-pin table (72 zeroes the `config` row 73 pins).
- **If 72 chooses not to wire its command arms** and only repairs the engine, the adapter pattern
  in §Proposed change step 3 applies to them verbatim as a trailing three-adapter step — but it
  must be attributed to 72, not folded in here, or the schema fix and the wiring land in the wrong
  order.

### Boundary vs proposal 74 (FR3, debug bundle)

74 owns `debug zip` (`commands/debug.rs:394`) and the server-side bundle machinery. **This
proposal leaves `:394` bailing.** The overlap is one `match` arm in one file: 74 replaces the arm
body at `:393-395`; 73 replaces the arm bodies at `:405`/`:408`/`:411`/`:418`/`:421`. No shared
lines. 74 also owns the unused `zip` dependency (`Cargo.toml:41`).

### Boundary vs proposal 75 (FR4 rendering + FR5 role enum)

Three explicit handshakes:

1. **`render_value` vs `Renderable`.** 75 makes `Renderable`/`print_output` the sole exit. Every
   adapter this proposal adds already exits through `print_output` — so 73 *adds no new work* for
   75 and removes some (the scan deletion drops a hand-rolled path).
2. **`render_ascii_graph` / `render_histogram_table` live in `ops/latency.rs`** (`:135-185`,
   `:188-203`) — rendering inside an operation module. 73 calls them from `render_table` **in
   place**. If 75 lands a rendering-ownership rule, relocating those two functions into the
   adapter is a mechanical move of two pure `String`-returning functions with no call-site change
   beyond the import.
3. **D6's two silently-ignored flags** (`debug latency --history`, `debug slowlog --all`) are in
   75's family (declared-and-unread options, alongside `no_color`, `tls_cert`, `tls_key`,
   `tls_ca`). They are reported here because they were found here and because they sit in the
   `match` 73 edits. **Ownership: 75.** If either is hotfixed independently (see §Effort) the
   destructuring pattern at `commands/debug.rs:396-402` / `:427-431` changes, which is a
   one-line rebase against 73's arm-body edits.

75 also owns the unused `comfy-table` (`:39`) and `dialoguer` (`:40`) deps, and `indicatif`
(`:38`) — note that `indicatif` is the *intended* sink for `ops/`'s `on_progress` callbacks
(`ExportProgress`, `ImportProgress`, `ops/backup.rs:7-18`; `on_progress: impl Fn(usize)`,
`ops/scan.rs:43`). This proposal deliberately does **not** adopt `indicatif`: it writes plain
progress lines to stderr, keeping the dependency question wholly in 75's hands. If 75 decides to
keep `indicatif`, swapping the stderr closure for a progress bar is a self-contained change inside
each adapter.

### Boundary vs proposals 63–72 at HEAD

Checked all proposal files on disk. `frogctl` appears in four (`49`, `67`, `69`, `70`) and in
every case as a *negative* finding — "no reference from `frogctl`" (`67:87`, `67:501`), "`frogctl`
… not consumers of the param table" (`69:265`), "`frogctl` is unaffected … has no subcommand
table (grepped)" (`70:511`, `70:340`). Zero overlap. The 15 open issues in
`.scratch/arch-deepening/issues/open/` contain no `frogctl` reference at all. Proposal 71 (not on
disk) is the last server/search candidate (SV11/SV12) and does not reach this crate.

### Behavioural risk of the change itself

- **`frogctl scan` output must not change.** The `Renderable for ScanResult` impl
  (`commands/scan.rs:57-132`) is preserved verbatim; only the data source changes. The one real
  semantic delta is D2 (limit applied inside vs after enrichment), which changes *how many
  round-trips are issued*, not which keys are returned. Pin it with a test asserting both the key
  set and the enrichment-call count at `--limit 3` over 10 keys.
- **The website regenerates.** `just docs-gen` must run; `just docs-gen-check` (`Justfile:817`)
  will fail the build otherwise. Wiring changes no clap types, so the JSON should be byte-identical
  — which is itself a useful assertion that the change is dispatch-only.
- **New failure surface for real operators.** Nine commands go from "fails loudly, always" to
  "does something against the live keyspace". `backup export` issues an unbounded `SCAN` sweep
  followed by a `DUMP` of every key; `debug memory memkeys` issues `MEMORY USAGE` per key. Neither
  is throttled. That is acceptable for operator tooling that is explicitly invoked — but the
  adapters must document the cost in their clap `about` strings, and `data keyspace`'s
  `--samples` default of 10 000 (`commands/data.rs:10-12`) must be honoured as a real ceiling
  rather than silently ignored.
- **`ExportManifest` is now a compatibility surface.** `version: u32 = 1` (`ops/backup.rs:66`)
  and the length-prefixed data-file layout (`:150`) stop being private the moment `export` works.
  The round-trip test pins them; a future format change needs the version bump the field already
  provides.

### Vocabulary

`frogctl/CONTEXT.md`'s *Avoid* list is honoured throughout: "Observability API" (use **Metrics
API**), bare "topology" (use **Cluster Topology** / **Replication Topology**), "Diagnostic bundle"
(use **Debug Bundle** — proposal 74's term). **Export Archive** is used as defined at `:38-39`.
**Fan-out** is used as defined at `:34-36`, which is what makes D6's `slowlog --all` finding a
documented-contract violation rather than a missing feature.

## Spec / LOCKED

**No locked-area exposure, and no mutation-gate implications.**

- The four locked areas are `frogdb-txn`+`frogdb-vll`, `frogdb-persistence`+`frogdb-recovery`,
  `frogdb-replication`+`frogdb-replication-runtime`, `frogdb-cluster`+`frogdb-cluster-runtime`
  (ADRs `adr/0002`–`0004`). **`frogctl` is none of them**, and every file in the §Files table is
  under `frogctl/` except four read-only citations (`docs-gen`, `frogdb-config`, `Justfile`,
  `website/`) and one config edit (`.config/nextest.toml`).
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
- The one new lint (`lint-frogctl-bails`) is a compile-free grep and joins `lint-gates`
  (`Justfile:329`), which runs unconditionally in lefthook `pre-commit` and in CI's `seam-gates`
  job. It states an invariant about *stub counts*, not about a locked-area seam, so it needs no
  spec row.

## Effort

**M.**

| step | size | note |
|---|---|---|
| 1. Layer rule in `ops/mod.rs` + `CONTEXT.md` | XS | doc only |
| 2. `scan` through the engine, delete the duplicate | S | ~95 lines removed, ~15 added; output must not change |
| 3. Nine adapters (3 backup, 3 scan-backed, 3 latency) | M | ~25-30 lines each; two need real argument decisions (`samples`→`limit`, `top` u64→usize) |
| 4. Fix D2–D5 | S | four localized fixes in `ops/backup.rs` and one semantic choice in `ops/scan.rs` |
| 5. `lint-frogctl-bails` count pin + `lint-gates` wiring | S | grep recipe, mirrors `lint-continuation-lock` |
| 6. frogctl back into the graded suite | XS | one line in `.config/nextest.toml`; expect the 16 unit tests to surface |
| 7. Tests: backup round-trip, verify-corruption table, scan limit/enrichment, keyspace summary | M | the real work; needs `TestServer` for each |
| 8. `just docs-gen` (expect no diff) + `frogctl.mdx` example fix | XS | |

Not L: no new abstraction is introduced, no crate boundary moves, no dependency is added, and the
engines themselves are near-unchanged. Not S: nine adapters plus a real test suite plus a semantic
decision on `--samples` is more than an afternoon, and step 7 is where most of it lives.

### Independently-landable hotfixes

Both are **confirmed LIVE** with a full trace, and neither depends on any part of this proposal.

**H1 — `website/.../reference/frogctl.mdx:50` documents a subcommand that does not exist.**
*Trace:* the published reference page's Examples block reads ```frogctl backup snapshot``` →
`BackupCommand` (`commands/backup.rs:12-63`) declares `Trigger`, `Status`, `Export`, `Import`,
`Verify` — there is no `Snapshot` → clap exits 2 with `unrecognized subcommand 'snapshot'`. The
correct spelling is `frogctl backup trigger`, which is what the operations guide already uses
(`website/src/content/docs/operations/backup-restore.md:34`). *Fix:* one word. *Size:* XS. *Owner:*
free — hand-written `.mdx`, not generated, and not in any sibling's file set.

**H2 — `frogctl debug slowlog --all` silently ignores its addresses.**
*Trace:* `DebugCommand::Slowlog { … all: Option<Vec<String>> … }` is declared at
`commands/debug.rs:88-90` with help "Collect from multiple nodes" → published to
`website/src/data/frogctl-cli.json` via `docs-gen` (`main.rs:329`) → the dispatch destructures
`{ count, analyze, reset, .. }` at `:427-431`, dropping `all` → `run_slowlog(*count, *analyze,
*reset, ctx)` (`:620`) queries `ctx`'s single default node. No warning, no error; the operator
gets one node's slow log and believes it is the cluster's. This **contradicts `CONTEXT.md:34-36`**,
which names slowlog as a Fan-out command, and diverges from `health`, which does honour `--all`
(`commands/health.rs:131`). *Fix:* either implement the fan-out over `ConnectionContext::resp_to`
(`connection.rs:52-61`) — the same primitive `health` uses — or, minimally and honestly, `bail!`
on `--all` until it exists. *Size:* S either way. *Owner:* proposal 75's family (declared-and-unread
options); if landed standalone it rebases against 73's edits to the same `match` in one line.

**H3 — `frogctl debug latency --history` silently ignores the flag.** Same class, same file
(`commands/debug.rs:44-46` declared → `:396-402` dropped in `..` → `run_latency` `:444-449` never
sees it). *Not proposed as a hotfix on its own*, because unlike H2 the flag has no correct
implementation to fall back to — "periodic snapshot mode (every 15s)" is an unwritten feature, so
the only honest standalone fix is to remove the flag, which is a CLI-surface change and therefore
75's call.

### Deliberately not hotfixed

- **`tempfile` in `[dependencies]`** (`Cargo.toml:44`) while used only from `#[cfg(test)]`
  (`ops/backup.rs:446,455`; `ops/config.rs:372,396,414,432,451`). Real build hygiene — it pulls
  `tempfile` into the shipped binary's dependency graph — but it is **not user-observable**, so it
  is not a hotfix. Fold it into step 3 (and coordinate with 72, which owns the `ops/config.rs`
  test that also uses it).
- **Hiding the still-unimplemented subcommands from `--help`/docs** (`#[command(hide = true)]`),
  which would stop the website advertising the remaining 19 stubs. Defensible, and arguably the
  correct interim honesty — but it edits `cli.rs` and the command enums that proposals 74 and 75
  both touch, and it is the *delete* branch's opening move applied to commands this proposal
  argues should eventually be wired. Flagged for the orchestrator; not proposed here.
