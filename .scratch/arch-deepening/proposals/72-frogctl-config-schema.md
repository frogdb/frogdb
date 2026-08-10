# 72 — `frogctl config generate`/`validate` through `frogdb-config` (extends `adr/0001`)

**Candidate:** FR2 · **Effort:** M · **Crates:** `frogctl` (unlocked), `frogdb-config` (unlocked),
`frogdb-server` (unlocked, delete-only) · **Order:** before proposal 73 (FR1)

All paths, line numbers and counts below were re-derived against the working tree at
**HEAD `159cb7a2`** ("arch-deepening: revise proposal 65"). Where the lane brief and the tree
disagree, the tree wins and the correction is stated inline.

## Summary

`frogctl` carries a **second, hand-written copy of the `frogdb.toml` schema**: 92 lines of
`push_str` string literals that emit a config file (`frogctl/src/ops/config.rs:26-118`), and a
9-entry `BTreeMap<&str, Vec<&str>>` of "known sections and their known keys" that judges one
(`:144-179`). The real schema lives in `frogdb-config`, is **kebab-case** and mostly
`deny_unknown_fields`. The copy is snake_case and names keys that do not exist.

The result is not "slightly stale". Of the **20 keys** `frogctl config generate` emits, **11 are
unknown to the server**, 1 carries an invalid value, and 8 are correct — and because
`ServerConfig` is `#[serde(deny_unknown_fields)]` (`config/src/server.rs:18`), the very first
section aborts the parse. Every file the generator produces is rejected by `frogdb-server
--config`. Symmetrically, `frogctl config validate` **certifies** those files as valid and reports
the *server's own* key spellings as `unknown field` warnings.

The fix is not to write frogctl's schema more carefully. It is to **delete frogctl's schema**. The
generator this proposal wants already exists and is already correct: `default_toml_impl()`
(`frogdb-server/crates/server/src/config/loader.rs:396-400`) is `toml::to_string(&Config::default())`
plus a header, guarded by three regression tests, and it is what `frogdb-server --generate-config`,
the website's `example-config.toml` and (via `to_string_pretty`) the `.deb`'s `frogdb.toml` are all
built from. It is simply unreachable from `frogctl`, because it sits in the heavy server crate and
`frogctl` does not depend on `frogdb-config` at all — the **only** workspace tool that emits or
reads `frogdb.toml` and does not.

The validator half has the same shape. `frogdb-config` already owns the exact datum `frogctl config
validate` wants to print — `ValidationReport { errors, warnings, infos }`
(`config/src/validators/mod.rs:63-70`) — but the only public door to it, `Config::validate()`, is
**lossy**: it logs the warnings to `tracing` and returns `anyhow::Result<()>`
(`config/src/lib.rs:369-372`). A CLI with no `tracing` subscriber sees none of them. So frogctl
reimplemented warnings too.

**Deletion, not translation, is the test.** Any surviving hand-written key list in `frogctl` — even
a correct one — is a second source of truth and re-drifts on the next config rename. After this
proposal `frogctl/src/ops/config.rs` goes **479 → ~150 lines**, and the ~330 deleted lines are
replaced by two function calls.

**Two LIVE defects found in the reachable part of the same files, both independently landable, both
the same disease** (frogctl restating a `frogdb-config` fact from memory):

* `frogctl`'s Admin API default is hardcoded `6380` (`connection.rs:143`); `/admin/*` is served by
  the observability server on `http.port`, default **9090** (`config/src/http.rs:46`). Every
  Admin-API command without `--admin-url` targets a port nothing listens on.
* `frogctl config show --section <s>` builds the glob `"{s}.*"` (`commands/config.rs:116`) and **no
  CONFIG parameter name contains a dot** — every section filter returns zero rows.

A third (`config show --diff <file>` parsed and silently discarded) is LIVE but cosmetic in
consequence and is folded into the proposal.

## Files involved

Whole-file line counts, verified at HEAD `159cb7a2`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogctl/src/ops/config.rs` | 479 | **Primary.** `generate_default_config` :26-118 (92 lines of literals) and `validate_config` :121-281 (`known_sections` :144-179, enum arrays :181-190, semantic checks :231-273) are **deleted**. `diff_configs` :284-320 + `flatten_toml*` :323-345 **stay** (schema-free). 6 unit tests :347-479 — 5 deleted, 1 kept. |
| `frogctl/src/commands/config.rs` | 139 | **Primary.** The three `bail!("not yet implemented")` arms at :102, :105, :108; `run_show` :114-139 (the broken `--section` glob :115-118); `ConfigResult`/`Renderable` :45-97 is the render adapter to copy. |
| `frogctl/Cargo.toml` | 48 | Gains `frogdb-config.workspace = true`. The ADR extension, in one line. |
| `frogdb-server/crates/config/src/lib.rs` | 479 | `Config` :82-228 (27 `#[section]` fields; 26 serialize — `chaos` is `cfg(turmoil)` + `serde(skip)` :222-227). `Config::validate` :306-373 splits: pure part → new `validation_report()`, host probes (`validate_path_parent` :271-302, called :357-368) stay in `validate()`. |
| `frogdb-server/crates/config/src/document.rs` | *new, ~70* | Destination for `DEFAULT_TOML_HEADER` + `default_toml_impl` + their 3 tests, moved verbatim. |
| `frogdb-server/crates/config/src/validators/mod.rs` | 224 | `ValidationResult` :16-25, `ValidationReport` :63-70, `run_all_validators` :127-158 — **verified pure** (zero `std::fs`/`Path::new`/`exists()` hits across all 6 validator files). Read-only here; it is the datum the CLI renders. |
| `frogdb-server/crates/server/src/config/loader.rs` | 480 | `ConfigLoader::default_toml()` decl :63, impl :232-234, `DEFAULT_TOML_HEADER` :379-394, `default_toml_impl` :396-400, tests :406-479. All five move or die. |
| `frogdb-server/crates/server/src/main.rs` | 152 | `Config::default_toml()` at :23 → `frogdb_config::default_toml()`. One of the two callers that let the trait method be deleted. |
| `frogdb-server/ops/docs-gen/src/main.rs` | 771 | `generate_example_config` :454-458 calls `Config::default_toml()` :457. The other caller. (`extract_fields` :644-694 is the annotated-comment follow-up, not this round.) |
| `frogctl/src/connection.rs` | 193 | `admin_url()` :139-144 — hotfix H1 only. |
| `frogctl/src/cli.rs` | 143 | `--admin-url` help text :56 — hotfix H1 only. |
| `frogctl/CONTEXT.md` | 79 | Admin API port claim :13 — hotfix H1 only. |
| `frogctl/tests/integration_config.rs` | 29 | 2 tests; `test_config_show_section` :18-29 is the one that passes while returning nothing. |

**Not in this file set** (deliberate, see *Scope boundaries*): `frogctl/src/ops/{backup,scan,latency}.rs`,
`frogctl/src/commands/{backup,data,debug,scan}.rs`, `frogctl/src/output.rs`,
`frogctl/src/info_parser.rs`, `frogdb-config/src/{param,param_id,params}.rs`.

## Problem

### P1 — Two schemas, and the copy is wrong in 12 of 20 keys

`generate_default_config` (`ops/config.rs:26-118`) writes snake_case keys into 7 top-level tables
(9 TOML tables with `--cluster`, one of which is nested). The server's `Config` is
`#[serde(deny_unknown_fields, rename_all = "kebab-case")]` (`config/src/lib.rs:83`) and so are most
of its sections. Key-by-key, against the current schema:

| Emitted (`ops/config.rs`) | Server reality | Verdict |
|---|---|---|
| `[server] bind` :34, `port` :36 | `bind`, `port` | ok |
| `[server] num_shards` :38 | `num-shards` (`server.rs:33` + kebab) | **rejected** |
| `[server] max_clients` :40 | `max-clients` (`server.rs:59`) | **rejected** |
| `[server] tcp_keepalive` :42 | *no such field* | **rejected** |
| `[server] timeout` :44 | *no such field* | **rejected** |
| `[memory] max_memory` :50 | `maxmemory` (`memory.rs:23`) | **rejected** |
| `[memory] maxmemory_policy` :53 | `maxmemory-policy` (`memory.rs:30`) | **rejected** |
| `[persistence] enabled` :58 | `enabled` | ok |
| `[persistence] data_dir` :60 | `data-dir` (`persistence.rs:30`) | **rejected** |
| `[persistence] durability_mode` :62 | `durability-mode` (`persistence.rs:69`) | **rejected** |
| `[persistence] snapshot_interval` :64 | *not in `[persistence]`* — it is `[snapshot] snapshot-interval-secs` (`persistence.rs:350`) | **rejected** |
| `[logging] level` :69 | `level` | ok |
| `[logging] format = "text"` :71 | parses, but `Config::validate` accepts only `pretty`/`json` (`lib.rs:317`) | **fails validate** |
| `[admin] enabled` :77/79, `port = 6380` :82 | keys exist; **default port is 6382** (`admin.rs:32`) | parses, wrong value |
| `[metrics] enabled` :87, `port` :89 | ok | ok |
| `[slowlog] slower_than` :94 | `log-slower-than` (`slowlog.rs:26`) | **rejected** |
| `[slowlog] max_len` :96 | `max-len` (`slowlog.rs:31`) | **rejected** |
| `[cluster] enabled` :102 | ok | ok |
| `[cluster.raft]` :108-113 | *no such table* — `ClusterConfigSection` is flat (`cluster.rs:21-112`) and carries **no** `deny_unknown_fields` (`cluster.rs:16`), so the table is **silently ignored** | silent no-op |

**11 rejected, 1 invalid value, 1 wrong-but-parseable value, 1 silently ignored table, 8 correct.**
Because `deny_unknown_fields` aborts at the first offender, a user never gets past
`[server] num_shards`.

The scale gap is the second half of the story, and the brief's "9 vs 27 sections" is off in both
directions: the honest numbers are **7 top-level sections emitted (8 with `--cluster`) vs 26 the
server serializes**, and **20 keys emitted vs 147**. Counted from the two generated artifacts that
*are* faithful: `website/src/data/example-config.toml` (26 `^[` headers, 147 `^[a-z]` key lines,
213 lines) and `frogdb-server/ops/deploy/deb/frogdb.toml` (221 lines). Nineteen whole sections —
`snapshot`, `recovery`, `http`, `tracing`, `security`, `acl`, `blocking`, `replication`, `json`,
`status`, `hotshards`, `latency`, `latency-bands`, `debug-bundle`, `compat`, `tiered-storage`,
`monitor`, `tls`, and `cluster` in standalone mode — never appear. That part is harmless
(`#[serde(default)]` on every field of `Config`), but it means the "annotated default config" a user
is handed documents 14% of the surface.

### P2 — `validate` inverts the server's contract, and a passing test says so

`validate_config`'s vocabulary (`ops/config.rs:144-179`) is 26 keys across 9 section names. Under
the server's kebab spelling, **8 of the 26 exist**. The rest name fields the server would reject.

Worse than the vocabulary is the *policy*. `ops/config.rs:202-204, 208, 215, 226` classify every
unrecognised key or section as a **warning**, and `:276` sets `valid: errors.is_empty()` — warnings
do not affect the verdict. The server's rule is the exact opposite: `deny_unknown_fields` makes an
unknown key a **hard parse failure**. This inversion is pinned by a currently-green test:

```rust
// frogctl/src/ops/config.rs:431-447
fn test_validate_unknown_field_warning() {
    …
    assert!(result.valid); // unknown fields are warnings, not errors
    assert!(result.warnings.iter().any(|w| w.contains("bogus_field")));
}
```

`test_validate_valid_config` :371-392 goes further and asserts `result.valid` for a body containing
`num_shards = 4`, `data_dir`, and `durability_mode` — three keys the server rejects. **The test
suite certifies the drift.** These assertions must be *deleted*, not ported: there is no correct
version of "unknown fields are warnings" while `deny_unknown_fields` stands.

The one genuinely useful thing `validate_config` does that a bare `from_str` does not is report
*multiple* problems and distinguish warnings from errors. That capability is the reason for P4, not
a reason to keep the hand-written table.

### P3 — The correct generator exists, is tested, and `frogctl` cannot reach it

```rust
// frogdb-server/crates/server/src/config/loader.rs:396-400
fn default_toml_impl() -> String {
    let body = toml::to_string(&Config::default())
        .expect("Config::default() must serialize to TOML — all fields are TOML-representable");
    format!("{DEFAULT_TOML_HEADER}\n{body}")
}
```

Three tests guard it (`loader.rs:413-427` round-trip to `Config::default()`, `:434-458` every
section present with a `>= 24` floor, `:464-479` the five sections that regressed once). Its
callers are `frogdb-server --generate-config` (`main.rs:23`) and docs-gen's
`generate_example_config` (`docs-gen/src/main.rs:457`).

Every workspace tool that touches `frogdb.toml` already imports `frogdb-config` — `frogdb-operator`
(`frogdb-operator/Cargo.toml:18`, the ADR), `helm-gen` (`:14`), `deb-gen` (`:14`, which does its own
`toml::to_string_pretty(config)` at `deb-gen/src/main.rs:230`), `docs-gen` (`:14`) — **except
`frogctl`** (`frogctl/Cargo.toml:33-47`: no `frogdb-*` dependency at all outside dev-dependencies).
It is the one tool that had to invent the schema, and it is the one tool whose copy is wrong. That
correlation is the whole argument.

There is no dependency obstacle: `docs-gen` already links `frogctl` *and* `frogdb-config`
(`docs-gen/Cargo.toml:15-16`), so `frogctl → frogdb-config` closes no cycle.

### P4 — The validation report exists in the schema crate and is thrown away at the door

`run_all_validators(&Config) -> ValidationReport` (`validators/mod.rs:127-158`) runs 12 cross-field
validators and returns `{ errors, warnings, infos }`. It is **pure** — verified: no `std::fs`,
`Path::new` or `exists()` anywhere in `validators/{mod,logging,memory,network,persistence,timeouts}.rs`.

`Config::validate()` then does this (`lib.rs:369-372`):

```rust
let report = validators::run_all_validators(self);
report.log_non_errors();     // warnings + infos → tracing, and nowhere else
report.into_result()?;       // errors → one joined anyhow::Error string
```

A CLI process installs no `tracing` subscriber, so `report.log_non_errors()` writes to a black hole.
`into_result()` flattens N errors into one `anyhow` string. Neither survives as data. So the
richest, most correct validation in the workspace is invisible to the tool whose entire job is
showing it to an operator — and frogctl grew its own `ValidationResult { valid, errors, warnings,
fields_parsed }` (`ops/config.rs:8-14`) to fill the hole, using a key table instead of the real
validators.

Two further asymmetries `validate()` currently hides:

* `Config::validate` also performs **host-environment probes** — `validate_path_parent`
  (`lib.rs:271-302`) *creates and deletes a probe file* in the parent directory, called for
  `persistence.data_dir`, `snapshot.snapshot_dir`, `acl.aclfile` and `logging.file_path`
  (`:357-368`). `persistence.enabled` defaults to **true** (`lib.rs:398`), so this fires on a
  default config. A `frogctl config validate ./k8s-frogdb.toml` run on a laptop against a file
  naming `/var/lib/frogdb/data` fails on the *host*, not the *file*. That is the wrong answer to
  the question asked.
* `deny_unknown_fields` is **not** universal: `AdminConfig` (`admin.rs:14`) and
  `ClusterConfigSection` (`cluster.rs:16`) carry `rename_all` without it. So a validator built on
  `from_str` alone cannot report unknown keys in `[admin]`/`[cluster]`. That is a gap in the schema
  crate — see *Risks*.

### P5 — None of this is visible to the default dev loop

`.config/nextest.toml:5` sets `default-filter = 'not package(frogctl)'`, and `just test frogctl` is
refused outright (`Justfile:80-83`, "use: just frogctl-test"). So the six `ops::config` unit tests —
including the two that certify the wrong schema — **never run** in `just test`; only under `just
frogctl-test` (`Justfile:297-298`).

The favourable half of the same fact: `just check` and `just lint` are `cargo check/clippy
--all-targets` over the whole workspace (`Justfile:55`, `:320`), **including** `frogctl`. So the
compile-time coupling this proposal introduces *does* fire in the normal dev loop even though
frogctl's tests do not. That is precisely the ADR-0001 mechanism, and it is what makes the extension
worth making rather than merely tidy.

### P6 — LIVE: `config show --section` returns nothing, for every section

End-to-end trace:

1. `frogctl config show --section memory`
2. `cli.rs` → `Commands::Config(ConfigCommand::Show { section: Some("memory"), diff: None })`
3. `main.rs:23` → `commands::config::run` → `:110` → `run_show(Some("memory"), ctx)`
4. `commands/config.rs:115-118`: `pattern = format!("{s}.*")` → `"memory.*"`
5. `redis::cmd("CONFIG").arg("GET").arg("memory.*")` (`:121-126`)
6. Server: `config_get` (`connection/conn_command.rs:275-295`) → `ConfigManager::get`
   (`runtime_config.rs:3359-3368`) → `glob_match(pattern, info.name)` over
   `frogdb_config::config_param_registry()`
7. **Zero parameter names contain a `.`** — verified: `grep -c 'name: "[^"]*\.' config/src/params.rs`
   → `0`. Names are flat kebab (`maxmemory`, `maxmemory-policy`, `lfu-log-factor`, …
   `params.rs:552+`). The glob's literal dot matches nothing.
8. Empty vec → `ConfigResult { entries: [] }` → `"No configuration parameters found.\n"`
   (`commands/config.rs:58-60`), exit 0.

`tests/integration_config.rs:18-29` exercises exactly this with `section: Some("maxmemory")` and
asserts only `exit_code == 0` — it passes on empty output. The information needed to do it right is
already in the registry: `ConfigParamInfo` carries `section` and `field` (`params.rs:31-46`,
`docs-gen/src/main.rs:378-388` already uses them as a lookup key).

### P7 — LIVE: `config show --diff <file>` is parsed and silently discarded

`ConfigCommand::Show { section, .. }` (`commands/config.rs:110`) destructures away `diff`.
`run_show` never sees it and there is no other reader (`grep` for `diff` in `commands/config.rs`:
declaration :41 and the discarding arm :110 only). The flag is documented — `"Compare running config
against a file"`, published to the website's CLI reference at
`website/src/data/frogctl-cli.json:741`. A user passing it gets a plain `config show`, exit 0, no
warning.

The same publication makes the three bailing arms a documented-but-broken surface:
`frogctl-cli.json:432` ("Emit an annotated default TOML configuration to stdout"), `:530`, `:627`.

## Proposed change

Four moves. The first two are in the schema crate and stand alone; the last two are the CLI's.

### 1. A config **document module** in `frogdb-config`

New `frogdb-server/crates/config/src/document.rs`, exported from `lib.rs`:

```
default_toml() -> String                 // Config::default(), header + TOML body
to_toml(&Config) -> Result<String>       // any Config value, same rendering
```

`DEFAULT_TOML_HEADER` (`loader.rs:379-394`), `default_toml_impl` (`:396-400`) and all three tests
(`:406-479`) **move verbatim**. The header's self-reference ("see `default_toml_impl()` in
`frogdb-server/crates/server/src/config/loader.rs`") updates to the new home; that string is
byte-compared by the generated-file check, so `just deb-gen`/`just docs-gen` re-run in the same
commit.

This is a **depth** move, not a relocation for tidiness. `default_toml() -> String` is a
zero-argument interface over 26 sections and 147 keys; `generate_default_config(cluster: bool)` is
92 lines of implementation wearing an interface's clothes. Same signature shape, three orders of
magnitude difference in what is hidden behind it.

**Deletion test.** After the move, `ConfigLoader::default_toml()` (decl `loader.rs:63`, impl
`:232-234`) has exactly two callers — `main.rs:23` and `docs-gen/src/main.rs:457` — and both can
name `frogdb_config::default_toml()` directly. So the trait method is **deleted**, not left as a
shim: the `ConfigLoader` trait goes 5 methods → 4 and stops being the only route to a fact that has
nothing to do with loading. If a reviewer wants to keep the shim, that is the signal the move was
not worth making.

**Leverage.** One move pays four consumers (`--generate-config`, docs-gen, frogctl, and any future
tool), and it moves the fact *down* — into the light crate every config consumer already imports —
rather than sideways.

### 2. Split `Config::validate()` into a report and a host probe

```
Config::validation_report(&self) -> ValidationReport   // pure: section validates + run_all_validators
Config::validate(&self) -> Result<()>                  // validation_report() + host probes + log + into_result
```

`validate()`'s observable behaviour is **unchanged** — same errors, same order, same
`log_non_errors()`, same `into_result()`. The split is purely additive: the pure part becomes
nameable. `validate_bind_address` (pure, `lib.rs:231-268`) belongs in the report;
`validate_path_parent` (touches the filesystem, `:271-302`) stays in `validate()`.

This names a distinction the code already half-makes: **portable facts** (is this file a valid
config?) versus **local-host facts** (can *this* machine run it?). A CLI validating a config
destined for another machine wants the first and must opt in to the second. `frogctl config
validate` therefore defaults to the report and takes `--check-paths` for the probes.

### 3. `frogctl` depends on `frogdb-config` — the ADR extension

`frogctl/Cargo.toml` gains `frogdb-config.workspace = true`. Then:

* **generate** = `frogdb_config::default_toml()`. `--cluster` becomes a mutation of a **`Config`
  value**, not of a string: `let mut c = Config::default(); c.cluster.enabled = true;
  c.admin.enabled = true; to_toml(&c)`. Both preset flags exist and both default `false`
  (`cluster.rs:21`, `admin.rs:19` — confirmed against `example-config.toml:72,144`). The
  `[cluster.raft]` block disappears because there is no such thing.
* **validate** = `toml::from_str::<Config>(text)` → on error, one entry naming the offending key
  (serde's message already names it — `config/src/persistence.rs:585-588` asserts exactly that
  property for a different key); on success, `validation_report()` → `errors`/`warnings`/`infos`,
  plus host probes under `--check-paths`.
* **`--section`** = filter `config_param_registry()` by `ConfigParamInfo::section` and issue
  `CONFIG GET` for those names, instead of the `"{s}.*"` glob. The section vocabulary stops being
  guessed.
* **`--diff`** = compare the `CONFIG GET` result against the file's parsed `Config`, keyed by
  `(section, field)` from the registry — the same lookup `docs-gen/src/main.rs:378-388` builds.

`ValidationResult` and `DiffEntry` (`ops/config.rs:8-23`) **stay**, demoted to what they should
always have been: the CLI's **render adapter** — plain serializable rows that
`Renderable`/`print_output` turn into the requested **Output Mode**. They carry no schema knowledge;
they are populated from `ValidationReport`. That is the boundary: the schema crate owns *what is
true*, frogctl owns *how it is shown*.

### 4. Deletions

| Deleted | Lines |
|---|---|
| `generate_default_config` body (`ops/config.rs:26-118`) | 92 |
| `known_sections` table (`:144-179`) | 36 |
| `valid_durability_modes` / `valid_eviction_policies` / `valid_log_levels` (`:181-190`) | 10 |
| Hand-written semantic checks (`:192-273`) | 82 |
| 5 of 6 unit tests (`:351-447`) | ~97 |

`ops/config.rs`: **479 → ~150**. What survives is `diff_configs` + `flatten_toml*` (`:284-345`),
which encode no schema — they flatten arbitrary TOML — plus the render types and
`test_diff_configs` (`:450-478`), which is schema-free and stays green.

**Module-level deletion test:** could `ops/config.rs` go entirely? No, and that is the useful
answer: the flatten/diff half is a real, schema-independent file utility with a working command
behind it. The generate/validate half must go, because *any* hand-written key list in frogctl —
including a freshly-corrected one — is a second source of truth that re-drifts the next time a
config field is renamed. That is exactly the failure `adr/0001` was written to prevent, one crate
over.

### `adr/0001` — the clause extended, and why it is an extension

The whole decision, quoted (`adr/0001-operator-imports-server-config-crate.md:3-7`):

> We decided the operator imports `frogdb-config` (kept deliberately light — no RocksDB/mlua/tantivy
> deps) and serializes through the server's own serde types rather than maintaining a parallel
> schema. Any server-side config rename or addition becomes a compile error in the operator instead
> of a runtime deployment failure.

**No contradiction.** Two invariants, both preserved:

* *"kept deliberately light"* — `frogdb-config`'s dependency list is unchanged
  (`config/Cargo.toml:14-22`: serde, schemars, serde_json, anyhow, toml, tracing, rand,
  config-derive). Adding `document.rs` and `validation_report()` adds no dependency. `frogctl` gains
  schemars/tracing/rand/config-derive transitively and nothing heavy.
* *"rather than maintaining a parallel schema"* — this proposal deletes a parallel schema. It is the
  ADR's own remedy applied to the crate the ADR did not name.

**One genuine widening, which should be written down.** The ADR speaks only of *serializing* and of
*the operator*. `frogctl config validate` **parses**, and a user-supplied file is data, not code — no
compile error can catch a stale file. The compile-time guarantee therefore transfers to the
*generate* direction unchanged, while *validate* gets the runtime analogue: the same serde types are
the only thing that decides. Suggested amendment, appended to `adr/0001` (keeping its terse
one-paragraph form):

> The same rule binds every workspace tool that emits **or parses** `frogdb.toml`, `frogctl`
> included: generation goes through `Config`'s serde types (a rename is a compile error), and
> validation goes through the same types' `Deserialize` plus `Config::validation_report()` (a
> renamed key is reported against the real schema, never a copy of it). No tool keeps its own list
> of section or key names.

Renaming the ADR file is not required; its title stays accurate about origin.

**Citation hazard, worth one line in the amendment.** "ADR-0001" appears in 11 server source and
spec locations (`role_manager.rs:19,35`; `connection/blocking.rs:214`; `commands/cluster/mod.rs:75`;
`commands/replication.rs:106`; `server/cluster_init.rs:950`; `tests/cluster_failover.rs:1686,2140`;
`tests/cluster_misc.rs:1033`; `tests/simulation.rs:5438`; `specs/replication-failure-modes.md:843`)
meaning the **context-scoped** `frogdb-server/docs/adr/0001-raft-cluster-metadata.md`, not this one.
Both numberings are legitimate under `CONTEXT-MAP.md:3-5`. Cite the workspace one as `adr/0001`.

### Vocabulary

Per `frogctl/CONTEXT.md`: the rendering choice is the **Output Mode** (`:19-20`), the HTTP endpoint
`frogctl` reaches with `--admin-url` is the **Admin API** (`:12-13`), the RESP link is the **Data
Plane** (`:9-10`). `frogctl config show` reads the running configuration over the Data Plane
(`CONFIG GET`); `generate`/`validate` touch no plane at all — they are the CLI's only genuinely
offline commands, which is why they can be tested without a server. Nothing here needs the
Avoid-listed bare "topology" or "Observability API"; the cli context lists no other Avoid terms.

## Testability improvement

**The interface is the test surface.** Once generation is `default_toml() -> String` and validation
is `from_str::<Config>` + `validation_report()`, the entire cross-tool contract is expressible in
one line:

```rust
toml::from_str::<Config>(&default_toml()).expect("must parse");
```

That test already exists (`loader.rs:413-427`) and **moves into `frogdb-config` with the code**,
which matters because of P5: `frogdb-config` is in the default nextest run and `frogctl` is not. The
contract test lands where it will actually be executed.

New tests, all in `frogdb-config`:

1. **Both presets round-trip.** `to_toml(&preset)` → `from_str::<Config>` → `validation_report()`
   has no errors, for standalone *and* cluster. This is the assertion whose absence is P1: today
   nothing anywhere checks that frogctl's output is loadable.
2. **`validation_report()` is portable.** A `Config` with `persistence.data-dir =
   /nonexistent/parent/x` yields a clean report, while `validate()` on the same value errors. Pins
   the P4 split so a later refactor cannot quietly push a filesystem probe back into the pure path
   — the property that lets frogctl validate a config for a machine it is not running on.
3. **Section coverage is derived.** Every `[section]` header in `default_toml()` is a `Config` field
   — a generalisation of `default_toml_contains_every_config_section` (`loader.rs:434-458`) that
   drops its hand-maintained `>= 24` floor in favour of the serialized field set (26 today).

What `frogctl` keeps is render tests over `ValidationResult`/`DiffEntry` → table/json/raw. Those may
stay outside the default suite without hiding anything, because they no longer assert schema facts —
which is the point of the split.

**Coverage-depth note** (`agents/…`, `just coverage-depth`): `ops/config.rs` today has 6 tests over
~300 lines of schema literals — high line coverage, **zero** contract coverage, and two of the tests
assert the inverse of the real contract. It is a clean specimen of why per-function *test diversity*
is the metric that matters. Worth citing in the next depth audit.

**Deleted-not-migrated, explicitly:** `test_validate_unknown_field_warning` (`:431-447`) and
`test_validate_valid_config` (`:371-392`). Their assertions are false under `deny_unknown_fields`.

## Risks / scope boundaries

### Ordering: 72 before 73 (FR1) — verified independently

The lane brief asserts this; I re-derived it rather than inheriting it.

* **The drift is latent today.** All three arms bail: `commands/config.rs:102` (`generate`), `:105`
  (`validate`), `:108` (`diff`). And `ops::config`'s three public functions have **zero non-test
  callers** — `grep -rn 'generate_default_config\|validate_config\|diff_configs' --include='*.rs'`
  over the whole tree returns only their definitions and their own `#[cfg(test)]` block. `grep -rn
  'ops::'` across `frogctl/` returns **0**. No user can obtain a bad file today.
* **73 makes it live.** FR1's stated remedy is "thin adapters → `print_output`". Applied to
  `commands/config.rs`, `frogctl config generate > frogdb.toml && frogdb-server --config
  frogdb.toml` starts failing at `[server] num_shards` — a user-facing break on a path the published
  CLI reference advertises (`frogctl-cli.json:432`).
* **The reverse order costs nothing.** 72 leaves the arms bailing; it changes what `ops::config`
  *is*, not whether it is reached. 73 then wires a correct implementation.
* **Caveat, stated honestly.** If 73 rules DELETE for `frogctl config generate/validate` rather than
  wire, 72's frogctl half is moot — but its `frogdb-config` half (move `default_toml`, split
  `validation_report`, delete the `ConfigLoader::default_toml` method) stands entirely on its own
  and still pays `--generate-config` and docs-gen. Land the crate-side work regardless of 73's
  verdict.

### Sibling proposals

* **73 (FR1, ops/ wiring — authored concurrently).** Boundary by file, not by intent: **72 owns**
  `frogctl/src/ops/config.rs`, `frogctl/src/commands/config.rs`, `frogctl/Cargo.toml`. **73 owns**
  `ops/{backup,scan,latency}.rs` and the arms in `commands/{backup,data,debug,scan}.rs`. All four
  arms of `commands/config.rs::run` — including `Diff`, which is `ops::config`'s — are 72's, so the
  `match` at `:100-111` has exactly one editor. `frogctl/Cargo.toml` is the one shared file: 72 adds
  `frogdb-config`, 73 (if it wires `indicatif`/`comfy-table`/`dialoguer`, currently unused) may
  touch adjacent lines. Trivial merge; 72 first.
* **74/75 (FR3 bundles, FR4 rendering, FR5 role enum).** 74/75 land later in the same crate; pinning
  72's file set keeps their boundary derivable. FR4 owns `frogctl/src/output.rs` and
  `commands/upgrade.rs` (the 87 raw `println!`s); 72 adds two `Renderable` impls **inside
  `commands/config.rs`** and does **not** touch `output.rs` or `print_output`'s signature
  (`output.rs:9-16`). FR5 owns `info_parser.rs`. Disjoint.
* **69 (config-param combinators).** Same crate, different files: 69's set is
  `runtime_config.rs`, `config/src/param.rs`, `param_id.rs`, `params.rs` (marked *untouched* in its
  own table), `config-derive/src/lib.rs`. 72's is `config/src/lib.rs` (adding `validation_report`)
  plus new `document.rs`. The only shared symbol is `config_param_registry()`, which 69 leaves alone
  and 72 only **reads** (for `--section`/`--diff`). Either order; no conflict.
* **65 (init-cluster phases), Hotfix 3.** Proposes adding `deny_unknown_fields` to
  `ClusterConfigSection` so a stale `node-id` fails loudly. That directly changes what `frogctl
  config validate` reports for a `[cluster]` block — today `[cluster.raft]` is silently ignored
  (verified: `cluster.rs:16` has `rename_all` without `deny_unknown_fields`). If 65's hotfix lands
  first, validate gets stricter for free. **Do not compensate in frogctl.** `[admin]`
  (`admin.rs:14`) and `[cluster]` are the two sections without `deny_unknown_fields`; teaching
  frogctl to police them by hand would recreate exactly the second source of truth this proposal
  deletes. It is a schema-crate gap and belongs to the schema crate — file it as a follow-up if 65's
  hotfix does not land.

### Behaviour changes, named

* **Generated config loses per-key comments.** `default_toml()` emits a 14-line header, not the
  per-key comments frogctl's literals carry, and the clap help says "annotated"
  (`commands/config.rs:12`). Real cost, accepted. **Do not re-add hand-written comments** — that is
  the drift, reintroduced. The right follow-up generates them from the schemars descriptions, which
  `docs-gen`'s `extract_fields` (`docs-gen/src/main.rs:644-694`) already extracts; doing it in
  `document.rs` upgrades `--generate-config` and frogctl together. Out of scope for this round; the
  clap help text is amended to "default TOML configuration" until then.
* **Validate becomes stricter and the exit code becomes meaningful.** Files that "passed" now fail —
  correctly, since the server rejects them.
* **`frogctl config validate` checks the file, not the merge.** The server loads via figment:
  defaults + file + `FROGDB_*` env + CLI overrides (`loader.rs:82-105`). Validating with
  `from_str::<Config>` checks the file in isolation — a strictly *stronger* check on the file, but
  it cannot see env overrides. Document that in the command's help. (Related but not ours:
  `loader.rs:91`'s `.nested()`, the known bug the `lint-nested-config` gate allowlists — this
  proposal adds no figment call anywhere and the gate scans `frogdb-server` only.)
* **Dependency weight.** `frogctl` gains schemars, tracing, rand, `frogdb-config-derive`
  transitively. `frogdb-config` has no `frogdb-*` dependency except its derive, and its `turmoil`
  feature is opt-in — do not enable it from frogctl.

### Spec / LOCKED duties — none, and here is the verification

* **`frogctl` is outside all four locked areas** (txn, persistence, replication, cluster;
  `adr/0002`-`0004`). Verified: `grep -rn "FM-" frogctl/` → **0 hits**.
  `.scratch/hardening/specs/cluster-failure-modes.md:56` puts it out of scope in as many words
  ("Also out of scope: the operator and `frogctl`"). No spec row names any file in this proposal's
  frogctl set.
* **`frogdb-config` is not a locked crate, but it *is* spec-relevant** — and this is the one place
  the lane brief would have let an author be careless.
  `specs/replication-failure-modes.md:1229-1233` records that `frogdb-config` was added to
  `NEXTEST_CRATES` because "config `validate()` tests are the forcing tests for every 'rejected at
  boot' clause". Three FM tags live in the crate: `persistence.rs:580` (FM-PERSISTENCE-051),
  `replication.rs:489` (FM-REPLICATION-045), `replication.rs:516` (FM-REPLICATION-047). All three
  force **section-level** `validate()` / `Deserialize` behaviour in files this proposal **does not
  touch**, and `Config::validate()`'s observable behaviour is unchanged by design (§2). So
  `just lint-failure-modes` sees no tag added, removed or moved, and no `Forced by` cell goes stale.
  **If a future revision changes what `validate()` rejects, that becomes spec-first work.** This one
  does not.
* **Mutation gates: none apply.** The four gated crate pairs are txn/vll (0.90),
  persistence/recovery (0.85), replication/replication-runtime (0.85), cluster/cluster-runtime
  (0.80). Neither `frogctl` nor `frogdb-config` nor `frogdb-server` is among them, so
  `just mutants-diff` is not push discipline for this change.
* **Seam lints: unaffected.** `lint-gates` (`Justfile:329`) — no clock read, metrics emission,
  redirect reply or durable-ack write is added; `lint-nested-config` scans `frogdb-server` only and
  no `.nested(` is added.

### The residual risk

Deleting a 300-line hand-written schema in favour of a derived one is low-risk *because* nothing
calls it. The real risk sits in §2: `Config::validate()` is on the server's boot path and is the
forcing surface for spec rows. The mitigation is a strict rule for the implementer — **the split
must be pure motion**: every statement in `validate()` either moves into `validation_report()` or
stays, none is reordered relative to the others, and `validate()`'s body becomes
`report.merge(host_probes())` + the existing log/into_result tail. A behavioural diff there would
show up as a boot-rejection change, which is exactly what the failure-mode specs are about.

## Effort

**M.** Roughly:

* **S** — move `document.rs` + 3 tests, delete `ConfigLoader::default_toml`, repoint 2 callers,
  re-run `just docs-gen` / `just deb-gen` (header string changes → generated files change).
* **S/M** — split `validation_report()` out of `validate()`; pure motion, but on a spec-relevant
  surface, so it wants a careful diff and the three new `frogdb-config` tests.
* **M** — frogctl: add the dependency, rewrite three arms, fix `--section` via the registry,
  implement `--diff`, delete ~330 lines, re-render `frogctl-cli.json` (help text changes).

Sequencing: the two `frogdb-config` steps land first and independently — they are useful even if the
frogctl half is later rescoped or deleted by 73.

### Hotfix H1 — LIVE, independently landable: `frogctl`'s Admin API port is wrong

**Confirmed live**, end-to-end:

1. `frogctl health --detailed` (or `frogctl upgrade status`) with no `--admin-url`.
2. `ConnectionContext::admin_url()` (`connection.rs:139-144`) returns
   `format!("http://{}:6380", host)` — a hardcoded literal.
3. `admin_get("/admin/health")` (`commands/health.rs:365`; also `commands/upgrade.rs:136,277`) GETs
   `http://127.0.0.1:6380/admin/health`.
4. `/admin/*` routes are mounted on the **observability server**
   (`frogdb-server/crates/server/src/observability_server.rs:236-243`), which binds
   `HttpConfig::bind_addr()` (`config/src/http.rs:65-67`) — default port **9090**
   (`http.rs:46: DEFAULT_HTTP_PORT = 9090`, confirmed by `example-config.toml:59-62`).
5. `AdminConfig.port` — the only thing 638x-shaped in the schema — defaults to **6382**
   (`admin.rs:32`) and is a **RESP** listener (`admin.rs:21`, "Port for the admin RESP protocol
   listener"), not HTTP.
6. So `6380` is neither endpoint. On a stock server every Admin-API command without an explicit
   `--admin-url` fails with a connection error.

**Fix** (3 sites, no dependency needed for the immediate correction): `connection.rs:143` →
`9090`; `cli.rs:56` help text `http://127.0.0.1:6380` → `:9090`; `frogctl/CONTEXT.md:13` "default
port 6380" → 9090, and note there that the Admin API and the **Metrics API** (`:15-16`, already
correct at 9090 via `connection.rs:148-152`) are the *same* HTTP server, which the glossary
currently implies are two. The durable form — `frogdb_config::http::DEFAULT_HTTP_PORT` instead of a
literal — lands with this proposal's dependency and is the reason the hotfix and the proposal are
the same disease. Regression test: assert `admin_url()` equals `metrics_url()`'s host:port under
default `GlobalOpts`, which is the invariant the two fallbacks share.

### Not hotfix-eligible, though LIVE

* **P6 (`--section` always empty).** The correct fix requires `config_param_registry()` — i.e. the
  dependency this proposal adds. A dependency-free "fix" (`format!("{s}*")`) is also wrong:
  `[memory]`'s parameters are `maxmemory`, `lfu-log-factor`, … which no section-name prefix
  matches. It ships inside the proposal. The existing test
  (`tests/integration_config.rs:18-29`) must gain a non-empty assertion at the same time.
* **P7 (`--diff` silently ignored).** A dependency-free hotfix exists — make `Show { diff: Some(_) }`
  `bail!` like its three siblings, 3 lines at `commands/config.rs:110` — and is worth landing if 72
  slips, since silently ignoring a documented flag is worse than refusing it. If 72 lands promptly,
  fold it in; the real implementation needs the registry either way.
