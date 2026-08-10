# 72 — `frogctl config generate`/`validate` through `frogdb-config` (extends `adr/0001`)

**Candidate:** FR2 · **Effort:** M · **Crates:** `frogctl` (unlocked), `frogdb-config` (unlocked),
`frogdb-server` (unlocked, delete-only) · **Order:** before proposal 73 (FR1)

**Revision 2** — re-verified at **HEAD `4c36827d`** after adversarial review at
`43720822` (verdict AMEND). Every citation was re-read at `4c36827d`; the source tree is
**byte-identical** to the `159cb7a2` draft base and to the review SHA — both
`git diff --stat 43720822..4c36827d -- ':!*.md'` and
`git diff --stat 159cb7a2..4c36827d -- ':!*.md'` are **empty** (the intervening commits are
`.scratch/arch-deepening/proposals/*.md` only). So nothing below is *drift*: every corrected
number was wrong in revision 1.

This revision **rewrites §P4's host-probe inventory** (revision 1 named one of three host inputs
and claimed a purity property that does not exist — B1), **retracts the `report.merge(host_probes())`
form** in favour of a strictly additive extraction (B2), **replaces the 72-before-73 ordering
argument** with the real coupling (revision 1's hazard cannot occur — B3), and **fixes four count
errors** (B4). Hotfix **H1** is confirmed live and gains a fourth site the review did not name
(`CONTEXT-MAP.md:28`). Three review line-drift claims are refuted with evidence in
*§Review response ledger*.

Where the lane brief and the tree disagree, the tree wins and the correction is stated inline.

## Summary

`frogctl` carries a **second, hand-written copy of the `frogdb.toml` schema**: 92 lines of
`push_str` string literals that emit a config file (`frogctl/src/ops/config.rs:26-118`), and a
9-entry `BTreeMap<&str, Vec<&str>>` of "known sections and their known keys" that judges one
(`:144-179`). The real schema lives in `frogdb-config`, is **kebab-case** and mostly
`deny_unknown_fields`. The copy is snake_case and names keys that do not exist.

The result is not "slightly stale". Of the **20 keys** `frogctl config generate` emits in its
default (standalone) mode, **11 are unknown to the server**, 2 carry wrong values, and 7 are
correct — and because `ServerConfig` is `#[serde(deny_unknown_fields)]` (`config/src/server.rs:18`),
the very first section aborts the parse. Every file the generator produces is rejected by `frogdb-server
--config`. Symmetrically, `frogctl config validate` **certifies** those files as valid and reports
the *server's own* key spellings as `unknown field` warnings.

The fix is not to write frogctl's schema more carefully. It is to **delete frogctl's schema**. The
generator this proposal wants already exists and is already correct: `default_toml_impl()`
(`frogdb-server/crates/server/src/config/loader.rs:396-400`) is `toml::to_string(&Config::default())`
plus a header, guarded by three regression tests, and it is what `frogdb-server --generate-config`
and the website's `example-config.toml` are built from. (The `.deb`'s `frogdb.toml` is *not* —
revision 1 said it was; `deb-gen` renders a mutated `Config` by hand under its own header, which is
the fourth copy of this job rather than a consumer of it. See §*Proposed change* 1.) It is simply
unreachable from `frogctl`, because it sits in the heavy server crate and
`frogctl` does not depend on `frogdb-config` at all — the **only** workspace tool that emits or
reads `frogdb.toml` and does not.

The validator half has the same shape. `frogdb-config` already owns the exact datum `frogctl config
validate` wants to print — `ValidationReport { errors, warnings, infos }`
(`config/src/validators/mod.rs:63-70`) — but the only public door to it, `Config::validate()`, is
**lossy**: it logs the warnings to `tracing` and returns `anyhow::Result<()>`
(`config/src/lib.rs:369-372`). A CLI with no `tracing` subscriber sees none of them. So frogctl
reimplemented warnings too. That door is also **not portable**: `validate()` mixes three
host-environment inputs into the same call (§P4), so extracting the report is a real split, not a
rename.

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

Whole-file line counts, verified at HEAD `4c36827d`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogctl/src/ops/config.rs` | 479 | **Primary.** `generate_default_config` :26-118 (92 lines of literals) and `validate_config` :121-281 (`known_sections` :144-179, enum arrays :181-190, semantic checks :231-273) are **deleted**. `diff_configs` :284-320 + `flatten_toml*` :323-345 **stay** (schema-free). **7** unit tests :347-479 (`:352`, `:363`, `:371`, `:395`, `:413`, `:431`, `:450`) — 6 deleted, 1 kept. |
| `frogctl/src/commands/config.rs` | 139 | **Primary.** The three `bail!("not yet implemented")` arms at :102, :105, :108; `run_show` :114-139 (the broken `--section` glob :115-118); `ConfigResult`/`Renderable` :45-97 is the render adapter to copy. |
| `frogctl/Cargo.toml` | 48 | Gains `frogdb-config.workspace = true`. The ADR extension, in one line. |
| `frogdb-server/crates/config/src/lib.rs` | 479 | `Config` :82-228 (27 `#[section]` fields; 26 serialize — `chaos` is `cfg(turmoil)` + `serde(skip)` :222-227). **`Config::validate` :306-373 is not edited** (B2): a new, additive `validation_report()` is added beside it. `validate_path_parent` :271-302 (called :358, :361, :364, :367) and `validate_bind_address` :231-268 are read-only. |
| `frogdb-server/crates/config/src/tls.rs` | 593 | **New in revision 2.** `TlsConfig::validate` :268-386 splits into a pure half :269-328 and a **7-probe filesystem tail** :330-383 (§P4). This is the only *motion* edit in the schema crate, and it is order-preserving (every pure check already precedes every probe). `TlsConfig` :76-77 and nested `AdditionalCert` :65-66 are the second and third sections missing `deny_unknown_fields`. |
| `frogdb-server/crates/config/src/document.rs` | *new, ~70* | Destination for `DEFAULT_TOML_HEADER` + `default_toml_impl` + their 3 tests, moved verbatim, plus `to_toml(&Config, header)`. |
| `frogdb-server/crates/config/src/validators/mod.rs` | 224 | `ValidationResult` :16-25, `ValidationReport` :63-70, `run_all_validators` :127-158. **No filesystem access** anywhere in the 6 validator files (`grep 'std::fs\|Path::new\|exists()'` → 0 hits) — but *not* host-independent: see the `memory.rs` row. Gains a portable/host-dependent partition. |
| `frogdb-server/crates/config/src/validators/memory.rs` | 94 | **New in revision 2.** `ShardCountVsCpusValidator` :35-60 reads `std::thread::available_parallelism()` :44; registered in the `run_all_validators` vec at `mod.rs:141`. The one host-dependent validator. |
| `frogdb-server/crates/server/src/config/loader.rs` | 480 | `ConfigLoader::default_toml()` decl :63, impl :232-234, `DEFAULT_TOML_HEADER` :379-394, `default_toml_impl` :396-400, tests :413-479. All five move or die. Trait methods :32/:47/:49/:56/:63 → 5 becomes 4. |
| `frogdb-server/crates/server/src/main.rs` | 152 | `Config::default_toml()` at **:24** → `frogdb_config::default_toml()`. One of the two callers that let the trait method be deleted. (Revision 1 said `:23` three times; `:23` is `if cli.generate_config {`. Unrelated: `frogctl/src/main.rs:23` in §P6 *is* :23 — a genuine collision of two different files' line 23.) |
| `frogdb-server/ops/docs-gen/src/main.rs` | 771 | `generate_example_config` **:456-458** calls `Config::default_toml()` :457. The other caller. **`use frogdb_server::config::ConfigLoader;` :18 is imported for that one call and nothing else** (`grep ConfigLoader` → :18 only), so it is deleted in the same edit or `-D warnings` fails. (`extract_fields` :644-694 is the annotated-comment follow-up, not this round.) |
| `frogdb-server/ops/deb/deb-gen/src/main.rs` | 274 | **New in revision 2, read-only + optional adopter.** `production_config()` :212-230 mutates a `Config::default()` (FHS paths, JSON logging) and `generate_frogdb_toml` :229-232 does `toml::to_string_pretty(config)` under its **own** `GENERATED_HEADER_TOML` :18-24. So the `.deb`'s `frogdb.toml` is **not** built from `default_toml_impl` (revision 1 implied it was), and it is the **fourth** hand-rolled `to_toml(&Config)`. |
| `frogctl/src/connection.rs` | 193 | `admin_url()` :139-144, `metrics_url()` **:146-152** — hotfix H1 only. |
| `frogctl/src/cli.rs` | 143 | `--admin-url` help text :56 — hotfix H1 only. |
| `frogctl/CONTEXT.md` | 78 | Admin API port claim :13 — hotfix H1 only. |
| `CONTEXT-MAP.md` | — | **New in revision 2.** `:28` repeats the same false claim ("the **Admin API** (HTTP, port 6380)") — hotfix H1's fourth site. |
| `frogctl/tests/integration_config.rs` | 29 | 2 tests; `test_config_show_section` :18-29 is the one that passes while returning nothing. |

**Not in this file set** (deliberate, see *Scope boundaries*): `frogctl/src/ops/{backup,scan,latency}.rs`,
`frogctl/src/commands/{backup,data,debug,scan}.rs`, `frogctl/src/output.rs`,
`frogctl/src/info_parser.rs`, `frogdb-config/src/{param,param_id,params}.rs`.

## Problem

### P1 — Two schemas, and the copy is wrong in 13 of 20 keys

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

**Two denominators, stated separately** — revision 1 mixed them and its tally did not close.

* **Standalone (no `--cluster`), 20 keys:** 11 rejected · 2 wrong values (`logging.format`,
  `admin.port`) · **7 correct**. 11 + 2 + 7 = 20.
* **`--cluster`, 24 keys:** the same 20, plus `[cluster] enabled` (correct, → 8 correct) and the
  three `[cluster.raft]` keys (`heartbeat_interval`, `election_timeout_min`,
  `election_timeout_max`, `ops/config.rs:110,112,113`) which land inside a **silently ignored
  table**. 11 rejected + 2 wrong values + 8 correct + 3 silently ignored = 24. (`announce_ip` /
  `announce_port` at `:104-105` are emitted *commented out*, so they are not keys.)

Because `deny_unknown_fields` aborts at the first offender, a user never gets past
`[server] num_shards` in either mode.

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
callers are `frogdb-server --generate-config` (`frogdb-server/crates/server/src/main.rs:24`) and
docs-gen's `generate_example_config` (`docs-gen/src/main.rs:456-458`, call at `:457`).

Every workspace tool that touches `frogdb.toml` already imports `frogdb-config` — `frogdb-operator`
(`frogdb-operator/Cargo.toml:18`, the ADR), `helm-gen` (`:14`), `deb-gen` (`:14`, which does its own
`toml::to_string_pretty(config)` at `deb-gen/src/main.rs:230`, on a mutated `Config` and under its
own header — see §1), `docs-gen` (`:15`) — **except `frogctl`** (`frogctl/Cargo.toml:28-44` is the
whole `[dependencies]` block: no `frogdb-*` entry; the only `frogdb-*` name in the file is
`frogdb-test-harness` under `[dev-dependencies]` `:46-48`).
It is the one tool that had to invent the schema, and it is the one tool whose copy is wrong. That
correlation is the whole argument.

There is no dependency obstacle: `docs-gen` already links `frogctl` *and* `frogdb-config`
(`docs-gen/Cargo.toml:14-15` — revision 1 said `:15-16`, which is `frogdb-config` + `frogdb-core`),
so `frogctl → frogdb-config` closes no cycle.

### P4 — The validation report exists in the schema crate and is thrown away at the door

`run_all_validators(&Config) -> ValidationReport` (`validators/mod.rs:127-158`) runs 12 cross-field
validators and returns `{ errors, warnings, infos }`. It performs **no filesystem access** —
verified: no `std::fs`, `Path::new` or `exists()` anywhere in
`validators/{mod,logging,memory,network,persistence,timeouts}.rs`. Revision 1 called this "pure";
it is not (see input 3 below), and the narrower claim is the one that survives.

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

#### The host-environment inventory — **three** inputs, not one

Revision 1 named only the first and then asserted a "pure `validation_report`" property that does
not exist. Complete inventory of everything `Config::validate()` reads from the machine it runs on:

| # | Input | Where | Reached when | On failure |
|---|---|---|---|---|
| 1 | `validate_path_parent` — `parent.exists()` `lib.rs:274`, `parent.is_dir()` `:281`, then **creates and deletes** `.frogdb_write_test_<pid>` `:289-292` | `lib.rs:271-302`, called at `:358` `persistence.data_dir`, `:361` `snapshot.snapshot_dir`, `:364` `acl.aclfile`, `:367` `logging.file_path` | `:358` gated on `persistence.enabled`, which defaults **true** (`persistence.rs:15` → `default_persistence_enabled()` `:135-137` → `true`); the other three are gated on non-default values | hard `bail!` |
| 2 | `TlsConfig::validate` — **seven `Path::exists()` probes**: `cert_file` `tls.rs:331`, `key_file` `:337`, `ca_file` `:341`, `client_cert_file` `:351`, `client_key_file` `:356`, and per additional identity `additional_certs[i].cert_file` `:371` / `.key_file` `:377` | called unconditionally from `lib.rs:335` | early-returns `Ok` when `!self.enabled` (`tls.rs:269-271`) — **but the motivating scenario is a TLS-enabled k8s config**, which is exactly when it does not return early | all seven hard `bail!` |
| 3 | `ShardCountVsCpusValidator` — `std::thread::available_parallelism()` (`validators/memory.rs:44`), registered in `run_all_validators`'s vec at `validators/mod.rs:141` | every `run_all_validators` call | always | **warning** whose text embeds the *validating* host's CPU count |

Each is the same wrong answer to the same question, in three registers:

* **(1)** `frogctl config validate ./k8s-frogdb.toml` on a laptop, against a file naming
  `/var/lib/frogdb/data`, fails on the *host*, not the *file*.
* **(2)** is worse, because it defeats the `--check-paths` design revision 1 proposed: the probes
  are inside `TlsConfig::validate()`, they all hard-bail, and the `!enabled` early return is no
  shelter for the one config a CLI is most likely to be handed off-host. A TLS config for another
  machine cannot be validated at all today.
* **(3)** is the subtlest and the reason revision 1's *test 2* was unwritable: a 10-core laptop
  validating a production file with `num-shards = 64` emits
  *"more than 2x the available CPUs (10)"* — a warning about the **laptop**, printed as though it
  were a fact about the file. That is precisely the class this section condemns, produced by the
  very report the CLI is meant to render.

So the split is **portable vs host-dependent**, not *pure vs filesystem*. §2 of *Proposed change*
partitions all three.

#### `deny_unknown_fields` is not universal — **three** sections, not two

`AdminConfig` (`admin.rs:14`), `ClusterConfigSection` (`cluster.rs:16`) **and `TlsConfig`**
(`tls.rs:76`) carry `rename_all` without `deny_unknown_fields`; so does the nested `AdditionalCert`
(`tls.rs:65`). Exhaustive: `grep -rn 'serde(rename_all = "kebab-case")' config/src/` returns exactly
these four plus one hit inside `params.rs`'s test module (`:1495`). A validator built on `from_str`
alone therefore cannot report unknown keys in `[admin]`, `[cluster]` **or `[tls]`** — a wider gap
than revision 1 stated, and (see *Risks*) a stronger reason not to compensate in frogctl.

### P5 — None of this is visible to the default dev loop

**Two layers, not one.** `.config/nextest.toml:5` sets `default-filter = 'not package(frogctl)'`,
and `just test frogctl` is refused outright (`Justfile:80-83`, "use: just frogctl-test"). So the
**seven** `ops::config` unit tests — including the two that certify the wrong schema — **never
run** in `just test`; only under `just frogctl-test` (`Justfile:297-298`). Underneath that,
`frogctl/Cargo.toml` sets `autotests = false` (`:8`) and gates the integration target behind
`[[test]] name = "integration" … required-features = ["cli-tests"]` (`:22-25`), so
`tests/integration_config.rs` is invisible to `just check` / `just lint` as well, not merely to
`just test`. Proposal 73 §6 removes the nextest exclusion; if it lands first, the drift-certifying
tests this proposal deletes would start failing the graded suite — which is a *good* outcome and a
real coupling (see *Ordering*), not a conflict.

The favourable half of the same fact: `just check` and `just lint` are `cargo check/clippy
--all-targets` over the whole workspace (`Justfile:55`, `:320`), **including** `frogctl`. So the
compile-time coupling this proposal introduces *does* fire in the normal dev loop even though
frogctl's tests do not. That is precisely the ADR-0001 mechanism, and it is what makes the extension
worth making rather than merely tidy.

### P6 — LIVE: `config show --section` returns nothing, for every section

End-to-end trace:

1. `frogctl config show --section memory`
2. `cli.rs` → `Commands::Config(ConfigCommand::Show { section: Some("memory"), diff: None })`
3. `frogctl/src/main.rs:23` → `commands::config::run` → `:110` → `run_show(Some("memory"), ctx)`
   (this `:23` is correct; the `:23`s revision 1 gave for the *server*'s `main.rs` were not — see
   the file table)
4. `commands/config.rs:115-118`: `pattern = format!("{s}.*")` → `"memory.*"`
5. `redis::cmd("CONFIG").arg("GET").arg("memory.*")` (`:121-126`)
6. Server: `config_get` (`connection/conn_command.rs:275-295`) → `ConfigManager::get`
   (`runtime_config.rs:3359-3368`) → `glob_match(pattern, info.name)` over
   `frogdb_config::config_param_registry()`. *(`ConfigManager::get` `:3359` is inside proposal
   69's declared edit set — `69`'s file table names `get :3359` explicitly. 72 only **reads** this
   line as evidence and edits nothing here, so there is no conflict; if 69 lands first the number
   drifts and the prose stays true.)*
7. **Zero parameter names contain a `.`** — verified crate-wide:
   `grep -c 'name: "[^"]*\.' config/src/params.rs` → `0`, counting both the assembled registry and
   its golden snapshot. Names are flat kebab (`maxmemory`, `maxmemory-policy`, `lfu-log-factor`, …).
   The glob's literal dot matches nothing. *(Revision 1 cited `params.rs:552+` as the registry; `:552`
   is the head of `GOLDEN_SNAPSHOT`, the `#[cfg(test)]` verbatim copy the assembled registry is
   asserted equal to — test data, not the live table. The claim is unaffected because the grep count
   is zero over the whole file, but the citation was wrong.)*
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
default_toml() -> String                        // Config::default(), DEFAULT_TOML_HEADER + body
to_toml(&Config, header: &str) -> Result<String> // any Config value, caller's header
```

`DEFAULT_TOML_HEADER` (`loader.rs:379-394`), `default_toml_impl` (`:396-400`) and all three tests
(`:413-479`) **move verbatim**. The header's self-reference (`loader.rs:385-386`: "see
`default_toml_impl()` in `frogdb-server/crates/server/src/config/loader.rs`") updates to the new
home.

**Which generated files that touches — corrected.** Only `just docs-gen`.
`website/src/data/example-config.toml` is `format!("{}\n", Config::default_toml())`
(`docs-gen/src/main.rs:456-458`) and does carry the header, so it changes. The `.deb`'s
`frogdb-server/ops/deploy/deb/frogdb.toml` **does not**: `deb-gen` never calls `default_toml_impl`
— it builds a **mutated** `Config` (`production_config()` `deb-gen/src/main.rs:212-230`: FHS
`data_dir`/`snapshot_dir`/`cluster.data_dir`, `logging.format = "json"`, `LogOutput::None`,
`file_path`, `RotationConfig`) and renders it under its own `GENERATED_HEADER_TOML` (`:18-24`) via
`toml::to_string_pretty` (`:229-232`). Revision 1 said the `.deb` file was built from
`default_toml_impl` "via `to_string_pretty`" and that `just deb-gen` must re-run; both were wrong.
**No `deb-gen` re-run is required for a header change.**

**Why `to_toml` takes the header explicitly.** That correction is also free leverage: `deb-gen` is
the **fourth** natural consumer of `to_toml(&Config)`, and it is already doing the job by hand on a
*mutated* value — the exact shape frogctl's `--cluster` needs. But `DEFAULT_TOML_HEADER` states
"This file is generated by serializing `Config::default()` to TOML" (`loader.rs:381`), which is
**false** of any mutated value — the `--cluster` preset and the `.deb`'s production config alike.
So `to_toml` is parameterised on the header rather than reusing `DEFAULT_TOML_HEADER`; a single
zero-argument `to_toml(&Config)` would have shipped a lie in two artifacts. `default_toml()` keeps
the existing header and stays byte-identical. Adopting `to_toml` in `deb-gen` is a two-line
follow-up, not required by this proposal.

This is a **depth** move, not a relocation for tidiness. `default_toml() -> String` is a
zero-argument interface over 26 sections and 147 keys; `generate_default_config(cluster: bool)` is
92 lines of implementation wearing an interface's clothes. Same signature shape, three orders of
magnitude difference in what is hidden behind it.

**Deletion test.** After the move, `ConfigLoader::default_toml()` (decl `loader.rs:63`, impl
`:232-234`) has exactly two callers — `frogdb-server/crates/server/src/main.rs:24` and
`docs-gen/src/main.rs:457` — and both can name `frogdb_config::default_toml()` directly. So the
trait method is **deleted**, not left as a shim: the `ConfigLoader` trait goes 5 methods → 4
(`:32`, `:47`, `:49`, `:56`, `:63`) and stops being the only route to a fact that has nothing to do
with loading. If a reviewer wants to keep the shim, that is the signal the move was not worth
making.

The deletion is **load-bearing at the second caller**, which strengthens it: `docs-gen` imports the
trait solely to reach this one method (`use frogdb_server::config::ConfigLoader;`
`docs-gen/src/main.rs:18` — the only `ConfigLoader` occurrence in the file's 771 lines). Deleting
the method without deleting the import fails `just lint` (`cargo clippy --all-targets -- -D
warnings`, `Justfile:320`) on `unused_imports`. The compiler enforces the deletion test.

**Leverage.** One move pays five consumers (`--generate-config`, docs-gen, frogctl, `deb-gen` if it
adopts `to_toml`, and any future tool), and it moves the fact *down* — into the light crate every
config consumer already imports — rather than sideways.

### 2. Add a portable report **beside** `Config::validate()` — additive only

Revision 1 said two mutually exclusive things: "`validate()`'s observable behaviour is unchanged"
here, and "`validate()`'s body becomes `report.merge(host_probes())`" in *§The residual risk*. The
second is false. `validate()` today (`lib.rs:306-373`) is **eager-bail**: 27 statements at `:307-368`
each ending `?` or `bail!`, so the caller sees the **first** failure — and `run_all_validators` runs **last**
(`:369`), after every section check has already had its chance to abort. A merged form would both
reorder which error surfaces first and join N errors into one `into_result()` string. That is
observable on the server boot path, which is the forcing surface for three FM rows. The merge
sentence is **retracted**; only this section is normative.

```
Config::validate(&self) -> Result<()>                  // UNCHANGED — body byte-identical
Config::validation_report(&self) -> ValidationReport   // NEW, additive, portable-only
```

**Rule for the implementer: `Config::validate()`'s body is not edited at all.** Not a statement
moved, not a `bail!` turned into a `?`. `git diff` over `lib.rs:306-373` must be empty. The only
motion edit anywhere in the schema crate is inside `TlsConfig` (below), and it is order-preserving.

`validation_report()` is new code that re-expresses the same checks in **collecting** form and then
runs the **portable** validators:

* the pure scalar checks (`server.port == 0`, log level, log format — `lib.rs:307-324`) and
  `validate_bind_address` (`:231-268`, pure) as collected errors rather than bails;
* the per-section `validate()` methods, with `tls` taking its pure half only (below);
* `run_all_validators(self)` **minus** host-dependent validators.

**The `TlsConfig` split** — the one motion edit. `TlsConfig::validate` (`tls.rs:268-386`) already
has the two halves laid out in order: every pure check is at `:274-328`, every `exists()` probe at
`:330-383`. So `validate_portable()` takes `:269-328` (including the `!enabled` early return) and
`validate()` becomes `self.validate_portable()?;` followed by the untouched probe tail. Same order,
same first error, same strings — a rebase-safe, reviewable diff.

**Host-dependent validators.** `ConfigValidator` gains `fn host_dependent(&self) -> bool { false }`,
overridden `true` by `ShardCountVsCpusValidator` (`validators/memory.rs:35-60`).
`run_all_validators` keeps its exact present behaviour (all 12, so `validate()` is untouched); a
sibling `run_portable_validators` skips the host-dependent ones. Today that is one validator; the
value is that the *category* is now nameable, so the next `available_parallelism`/`hostname`/
`num_cpus` read cannot silently enter the portable report.

**The cost, named.** The pure check list is now enumerated twice — once bailing, once collecting.
That is a genuine second source of truth of the kind this proposal exists to delete, and it is
accepted **only** because collapsing it means reordering `validate()`, which is spec-first work
(see *Spec / LOCKED duties*). Two mitigations, both required:

1. An **equivalence test** in `frogdb-config`: over a corpus (`Config::default()` plus the
   section-invalid fixtures the crate's own tests already build), assert
   `validate().is_err() == validation_report().has_errors()` for every config that touches no host
   input (persistence disabled, TLS disabled, empty `acl.aclfile`, no `logging.file_path`,
   `snapshot_interval_secs == 0`). A check added to one and not the other breaks it.
2. A comment at both sites naming the other, and the follow-up: *when a future spec-first change is
   already reordering `validate()`, collapse it to `validation_report()` + probes.*

This names a distinction the code already half-makes: **portable facts** (is this file a valid
config?) versus **local-host facts** (can *this* machine run it?). A CLI validating a config
destined for another machine wants the first and must opt in to the second. The frogctl flag is
therefore `--check-host`, not revision 1's `--check-paths`: it covers all three inputs of the §P4
inventory (path probes, TLS file existence, CPU-count warnings), and "paths" would have promised
coverage of only one of them.

### 3. `frogctl` depends on `frogdb-config` — the ADR extension

`frogctl/Cargo.toml` gains `frogdb-config.workspace = true`. Then:

* **generate** = `frogdb_config::default_toml()`. `--cluster` becomes a mutation of a **`Config`
  value**, not of a string: `let mut c = Config::default(); c.cluster.enabled = true;
  c.admin.enabled = true; to_toml(&c, CLUSTER_PRESET_HEADER)`. Both preset flags exist and both
  default `false` (`cluster.rs:21`, `admin.rs:19` — confirmed against `example-config.toml:72,144`).
  The header is the preset's own, not `DEFAULT_TOML_HEADER`, whose "serializing `Config::default()`"
  sentence would be false of a mutated value (§1). The `[cluster.raft]` block disappears because
  there is no such thing.
* **validate** = `toml::from_str::<Config>(text)` → on error, one entry naming the offending key
  (serde's message already names it — `config/src/persistence.rs:583-588` asserts exactly that
  property for a different key); on success, `validation_report()` → `errors`/`warnings`/`infos`,
  plus all three host inputs under **`--check-host`** (renamed from revision 1's `--check-paths`:
  two of the three inputs are not paths).
* **`--section`** = filter `config_param_registry()` by `ConfigParamInfo::section` and issue
  `CONFIG GET` for those names, instead of the `"{s}.*"` glob. The section vocabulary stops being
  guessed. **The existing test must change its argument, not just gain an assertion**:
  `tests/integration_config.rs:24` passes `section: Some("maxmemory")`, which is a *parameter*
  name (`params.rs` `name: "maxmemory"`), not a section — under the registry filter it still
  returns zero rows and the test would still pass vacuously. It becomes `Some("memory")` (the
  `section` value `maxmemory` carries) **and** asserts a non-empty result containing `maxmemory`.
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
| 6 of 7 unit tests (`:351-447`) | ~97 |

`ops/config.rs`: **479 → ~150**. What survives is `diff_configs` + `flatten_toml*` (`:284-345`),
which encode no schema — they flatten arbitrary TOML — plus the render types and the **one** kept
test of seven, `test_diff_configs` (`:450-478`), which is schema-free and stays green. (Revision 1
said "6 unit tests … 5 deleted"; the file has **seven** `#[test]`s — `:352`, `:363`, `:371`, `:395`,
`:413`, `:431`, `:450`. The deleted range `:351-447` was right; the arithmetic over it was not.)

**Module-level deletion test:** could `ops/config.rs` go entirely? No, and that is the useful
answer: the flatten/diff half is a real, schema-independent file utility with a working command
behind it. The generate/validate half must go, because *any* hand-written key list in frogctl —
including a freshly-corrected one — is a second source of truth that re-drifts the next time a
config field is renamed. That is exactly the failure `adr/0001` was written to prevent, one crate
over.

### `adr/0001` — the clause extended, and why it is an extension

The whole decision, quoted (`adr/0001-operator-imports-server-config-crate.md:4-7`; the paragraph
opens at `:3` with the problem statement):

> We decided the operator imports `frogdb-config` (kept deliberately light — no RocksDB/mlua/tantivy
> deps) and serializes through the server's own serde types rather than maintaining a parallel
> schema. Any server-side config rename or addition becomes a compile error in the operator instead
> of a runtime deployment failure.

**No contradiction.** Two invariants, both preserved:

* *"kept deliberately light"* — `frogdb-config`'s dependency list is unchanged
  (`config/Cargo.toml:13-20`: config-derive, serde, schemars, serde_json, anyhow, toml, tracing,
  rand). Adding `document.rs` and `validation_report()` adds no dependency. `frogctl` gains
  schemars/tracing/rand/config-derive transitively and nothing heavy.
* *"rather than maintaining a parallel schema"* — this proposal deletes a parallel schema. It is the
  ADR's own remedy applied to the crate the ADR did not name.

**One genuine widening, which should be written down.** The ADR speaks only of *serializing* and of
*the operator*. `frogctl config validate` **parses**, and a user-supplied file is data, not code — no
compile error can catch a stale file. The compile-time guarantee therefore transfers to the
*generate* direction unchanged, while *validate* gets the runtime analogue: the same serde types are
the only thing that decides. Suggested amendment, appended to `adr/0001` (keeping its terse
one-paragraph form), **carrying a dated amendment marker** so the file records that its scope grew
rather than silently reading as the original 2024 decision:

> **Amended 2026-08-10** (proposal 72, `frogctl`): the same rule binds every workspace tool that
> emits **or parses** `frogdb.toml`, `frogctl` included: generation goes through `Config`'s serde
> types (a rename is a compile error), and validation goes through the same types' `Deserialize`
> plus `Config::validation_report()` (a renamed key is reported against the real schema, never a
> copy of it). No tool keeps its own list of section or key names.

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

One caveat on that vocabulary, which hotfix H1 resolves: **Admin API** (`:12-13`) and **Metrics
API** (`:15-16`) are written as two endpoints on two ports, and they are one HTTP server on
`http.port`. H1 merges the two entries; until it lands, read both terms as route prefixes, not as
listeners.

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

1. **Both presets round-trip.** `to_toml(&preset, header)` → `from_str::<Config>` →
   `validation_report()` has no errors, for standalone *and* cluster. This is the assertion whose
   absence is P1: today nothing anywhere checks that frogctl's output is loadable.
2. **`validation_report()` is portable — all three inputs.** Revision 1 wrote this test against a
   property that does not exist ("the report is pure"); input 3 of the §P4 inventory makes the
   report host-*dependent* today. Restated as three cases against the actual inventory, each
   asserting the report is clean **and** `validate()`/`--check-host` still sees the problem:
   * `persistence.data-dir = /nonexistent/parent/x` → clean report; `validate()` errors (input 1).
   * `tls.enabled = true` with `cert-file`/`key-file` naming absent paths → clean report;
     `validate()` errors (input 2). Without the `TlsConfig` split this case cannot pass, which is
     what makes the split load-bearing rather than tidy.
   * `server.num-shards = 8 × available_parallelism()` → **zero warnings** in the portable report;
     `run_all_validators` still warns (input 3). Computing the value from the running host is what
     makes the test deterministic on a 4-core CI box and a 64-core laptop alike, and it is the
     assertion that stops a later refactor from re-admitting a host-derived string.
3. **Section coverage is derived.** Every `[section]` header in `default_toml()` is a `Config` field
   — a generalisation of `default_toml_contains_every_config_section` (`loader.rs:434-458`) that
   drops its hand-maintained `>= 24` floor in favour of the serialized field set (26 today).
4. **Report/validate equivalence** — the duplication guard required by §2. See that section for the
   corpus and the exact predicate.

What `frogctl` keeps is render tests over `ValidationResult`/`DiffEntry` → table/json/raw. Those may
stay outside the default suite without hiding anything, because they no longer assert schema facts —
which is the point of the split.

**Coverage-depth note** (`agents/…`, `just coverage-depth`): `ops/config.rs` today has 7 tests over
~300 lines of schema literals — high line coverage, **zero** contract coverage, and two of the tests
assert the inverse of the real contract. It is a clean specimen of why per-function *test diversity*
is the metric that matters. Worth citing in the next depth audit.

**Deleted-not-migrated, explicitly:** `test_validate_valid_config` (`:371-392`) and
`test_validate_unknown_field_warning` (`:431-447`).

Revision 1 said "their assertions are false" flatly; that over-claims by one assertion, and the
exception is worth stating because it is the successor test:

* `test_validate_valid_config` — wholly false. It asserts `result.valid` for a body containing
  `num_shards = 4`, `data_dir`, `durability_mode`, all three of which the server rejects. Nothing
  survives.
* `test_validate_unknown_field_warning` — the **first** assertion (`assert!(result.valid)`, i.e.
  "unknown fields are warnings, not errors", `:445`) is false under `deny_unknown_fields` and dies
  with the function. The **second** (`warnings.iter().any(|w| w.contains("bogus_field"))`, `:446`)
  states something true and worth keeping — *the offending key is named in the output* — and has an
  exact precedent in the schema crate: `force_fresh_data_dir_is_not_settable_from_the_config_file`
  (`config/src/persistence.rs:583-588`) asserts `err.to_string().contains("force-fresh-data-dir")`
  for the same reason. Its successor is the same fixture with the same key, asserted over
  **`errors`** rather than `warnings`, in `frogdb-config` rather than `frogctl` — which is also the
  test that pins the *validate* half of the ADR amendment.

## Risks / scope boundaries

### Ordering: 72 before 73 (FR1) — verified independently

The lane brief asserts this; I re-derived it rather than inheriting it. **Revision 2 retracts
revision 1's reason and keeps its conclusion.**

* **The drift is latent today.** All three arms bail: `commands/config.rs:102` (`generate`), `:105`
  (`validate`), `:108` (`diff`). And `ops::config`'s three public functions have **zero non-test
  callers** — `grep -rn 'generate_default_config\|validate_config\|diff_configs' --include='*.rs'`
  over the whole tree returns only their definitions and their own `#[cfg(test)]` block. `grep -rn
  'ops::'` across `frogctl/` returns **0**. No user can obtain a bad file today.
* **Retracted: "73 makes it live."** Revision 1 argued that 73 would wire `commands/config.rs`'s
  arms and thereby ship a bad `frogdb.toml`. **73 does no such thing, and says so twice.**
  73's file table marks `frogctl/src/commands/config.rs` "**NOT TOUCHED — owned by proposal 72**"
  (`73:82`), and its ordering section states it "**does not touch `ops/config.rs` or the three
  `commands/config.rs` arms at all, in either order**" (`73:455-457`). The hazard cannot occur in
  either order, so it cannot be the reason for one.
* **The real coupling is three shared artifacts, none of them a hazard — all three merely say
  "72 first is cheaper".**
  1. **73's bail count pin.** 73 §5 adds `just lint-frogctl-bails` to the `lint-gates` family
     (`Justfile:329`) with a pinned per-module table that already reads **"config 3 (or 0 after
     proposal 72)"** (`73:334-342`). That parenthesis is the coupling: if 72 lands first the pin is
     written once, as `config 0`; if 73 lands first the pin is written, landed, and then edited by
     72 — a gate-file conflict in a lint that runs on every commit.
  2. **`.config/nextest.toml:5`.** 73 §6 removes `not package(frogctl)` (`73:344-350`). That is the
     exclusion this proposal's §P5 is built on and the reason 72 deletes rather than ports the two
     drift-certifying tests. Landing 72 first means the exclusion is lifted onto a file whose
     schema tests are already correct; landing 73 first briefly grades `test_validate_valid_config`
     and `test_validate_unknown_field_warning` — which pass, and certify the drift, in the graded
     suite.
  3. **`frogctl/CONTEXT.md`.** Both edit it, at **disjoint lines**: 72 touches the *Admin API*
     glossary entry `:13` (hotfix H1), 73 touches the ops/commands engine relationship `:60-61`
     plus `:38-39` and `:47-52` (`73:87`). Textual merge, not a semantic one, in either order.
  4. **`frogctl/Cargo.toml`.** 72 adds `frogdb-config.workspace = true`; 73's single Cargo edit is
     moving **`tempfile`** (`:44`) from `[dependencies]` to `[dev-dependencies]` (`73:476`, `73:636`).
     Adjacent lines, trivial merge.
* **The reverse order costs nothing.** 72 leaves the arms bailing; it changes what `ops::config`
  *is*, not whether it is reached. 73 then leaves them alone.
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
  `match` at `:100-111` has exactly one editor. Four shared artifacts, all enumerated under
  *Ordering* above: `frogctl/Cargo.toml` (72 adds `frogdb-config`, 73 moves `tempfile`),
  `frogctl/CONTEXT.md` (disjoint lines), `.config/nextest.toml:5`, and 73's `lint-frogctl-bails`
  pin. **Correction to revision 1:** the unused `indicatif` (`Cargo.toml:37`), `comfy-table`
  (`:38`) and `dialoguer` (`:39`) are **not** 73's — 73 explicitly assigns them to proposal **75**
  and declines `indicatif` in favour of plain stderr progress lines precisely to keep the
  dependency question out of its own diff (`73:508-514`). Trivial merge; 72 first.
* **74/75 (FR3 bundles, FR4 rendering, FR5 role enum).** 74/75 land later in the same crate; pinning
  72's file set keeps their boundary derivable. FR4 owns `frogctl/src/output.rs` and
  `commands/upgrade.rs` (the 87 raw `println!`s); 72 adds two `Renderable` impls **inside
  `commands/config.rs`** and does **not** touch `output.rs` or `print_output`'s signature
  (`output.rs:9-16`). FR5 owns `info_parser.rs`. Disjoint.
* **69 (config-param combinators).** Same crate, different files: 69's set is
  `runtime_config.rs`, `config/src/param.rs`, `param_id.rs`, `params.rs` (marked *untouched* in its
  own table), `config-derive/src/lib.rs`. 72's is `config/src/lib.rs` (adding `validation_report`),
  `config/src/tls.rs` (the pure/probe split), `validators/{mod,memory}.rs` (the portable partition)
  plus new `document.rs`. The only shared symbol is `config_param_registry()`, which 69 leaves alone
  and 72 only **reads** (for `--section`/`--diff`). **One read-only cite does land inside 69's edit
  range**, and revision 1 did not say so: §P6 step 6 names `ConfigManager::get`
  (`runtime_config.rs:3359-3368`), which 69's own file table claims as `get :3359`. 72 edits nothing
  there — no conflict — but if 69 lands first the number drifts and the citation, not the argument,
  needs re-deriving. Either order.
* **65 (init-cluster phases), Hotfix 3.** Proposes adding `deny_unknown_fields` to
  `ClusterConfigSection` so a stale `node-id` fails loudly. That directly changes what `frogctl
  config validate` reports for a `[cluster]` block — today `[cluster.raft]` is silently ignored
  (verified: `cluster.rs:16` has `rename_all` without `deny_unknown_fields`). If 65's hotfix lands
  first, validate gets stricter for free — that holds under either of 65's two options. **Do not
  compensate in frogctl.** The gap is **three** sections wide, not two: `[admin]` (`admin.rs:14`),
  `[cluster]` (`cluster.rs:16`) and `[tls]` (`tls.rs:76`), plus the nested `AdditionalCert`
  (`tls.rs:65`) — exhaustively enumerated in §P4. That **widens** the gap and therefore
  **strengthens** the ruling: teaching frogctl to police three sections by hand would recreate
  exactly the second source of truth this proposal deletes, and it would do so over the section
  whose own `validate()` this proposal is already splitting. It is a schema-crate gap and belongs to
  the schema crate — file it as a follow-up (covering all three) if 65's hotfix does not land.

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
  touch**, and `Config::validate()` is **not edited at all** (§2 — revision 2 replaced "unchanged by
  design" with "unchanged by construction"; the FM tag at `persistence.rs:580` sits in one of those
  untouched section `validate()`s). So
  `just lint-failure-modes` sees no tag added, removed or moved, and no `Forced by` cell goes stale.
  **If a future revision changes what `validate()` rejects, that becomes spec-first work.** This one
  does not.
* **The `TlsConfig` split is spec-clear too** — re-verified in revision 2, since it is the one
  motion edit the schema crate takes. `grep -rn "FM-" config/src/` returns exactly the three tags
  above; **none is in `tls.rs`**, and no spec row cites `config/src/tls.rs` or
  `TlsConfig::validate`. The split preserves order, first error and message text, so even the
  untagged `tls` boot-rejection behaviour is unchanged.
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
forcing surface for spec rows.

Revision 1 mitigated this with "the split must be pure motion … `validate()`'s body becomes
`report.merge(host_probes())`". **That sentence is deleted**: it is not pure motion, it contradicts
the same revision's "observable behaviour unchanged", and it is the one form of this change that
*would* be a boot-rejection change (§2 shows why — first-error reordering plus N-errors-joined).

The mitigation is instead the rule stated in §2, which is stricter and mechanically checkable:
**`Config::validate()` is not edited.** `git diff` over `lib.rs:306-373` is empty; the only motion
is inside `TlsConfig::validate` (`tls.rs:268-386`), where the pure/probe boundary already falls on a
statement boundary so the split preserves order, first error and message text. The residual cost is
the duplicated check list, accepted and pinned by the equivalence test in §2 — a *maintenance* risk
inside `frogdb-config`, not a boot-behaviour risk on the server path. Collapsing the duplication is
deferred to whenever a spec-first change is already reordering `validate()`.

## Effort

**M.** Roughly:

* **S** — move `document.rs` + 3 tests, delete `ConfigLoader::default_toml` **and docs-gen's now-unused
  `ConfigLoader` import (`:18`)**, repoint 2 callers, re-run **`just docs-gen` only** (the header
  string changes `example-config.toml`; the `.deb`'s `frogdb.toml` carries its own header and is
  unaffected — see §1).
* **S/M** — add `validation_report()` **beside** an unedited `validate()`, split
  `TlsConfig::validate`, add the portable/host-dependent validator partition; on a spec-relevant
  surface, so it wants a careful diff plus the four new `frogdb-config` tests.
* **M** — frogctl: add the dependency, rewrite three arms, fix `--section` via the registry,
  implement `--diff`, delete ~330 lines, re-render `frogctl-cli.json` (help text changes).

Sequencing: the two `frogdb-config` steps land first and independently — they are useful even if the
frogctl half is later rescoped or deleted by 73.

### Hotfix H1 — LIVE, independently landable: `frogctl`'s Admin API port is wrong

**Confirmed live**, end-to-end — all six steps re-verified at `4c36827d` in revision 2, and the
review independently confirmed each. **Ship this separately and immediately**; it does not wait on
the proposal.

1. `frogctl health --detailed` (or `frogctl upgrade status`) with no `--admin-url`.
2. `ConnectionContext::admin_url()` (`connection.rs:139-144`) returns
   `format!("http://{}:6380", host)` — a hardcoded literal.
3. `admin_get("/admin/health")` (`commands/health.rs:365`; also `commands/upgrade.rs:136,277`) GETs
   `http://127.0.0.1:6380/admin/health`.
4. `/admin/*` routes are mounted on the **observability server**
   (`frogdb-server/crates/server/src/observability_server.rs:236-245` — seven routes on the
   `protected` router; revision 1's `:236-243` cut `transfer-leader` in half), which binds
   `HttpConfig::bind_addr()` (`config/src/http.rs:65-67`) — default port **9090**
   (`http.rs:46: DEFAULT_HTTP_PORT = 9090`, confirmed by `example-config.toml:59-62`).
5. `AdminConfig.port` defaults to **6382** (`admin.rs:32`) and is a **RESP** listener
   (`admin.rs:21`, "Port for the admin RESP protocol listener"), not HTTP.
6. So `6380` is neither endpoint. On a stock server every Admin-API command without an explicit
   `--admin-url` fails with a connection error.

**`6380` is not a typo for `6382` — it is a *different real port*, which makes the bug worse.**
Revision 1 called `AdminConfig.port` "the only thing 638x-shaped in the schema"; that is false.
`DEFAULT_TLS_PORT = 6380` (`config/src/tls.rs:218`, `tls_port` `:117-119`) is the **RESP TLS**
listener. The workspace already says so in as many words:
`website/docs-spec/specs/operations/clustering.md:101-103` — "There is NO admin listener on port
6380 (6380 is the default `tls-port` — a real RESP TLS port documented in Security — not an admin
port)". So on a TLS-enabled server, `frogctl health --detailed` sends an HTTP GET at a **TLS RESP
socket**, not at a closed port.

**Fix — 4 sites** (no dependency needed for the immediate correction; revision 1 listed 3):

1. `frogctl/src/connection.rs:143` — `6380` → `9090`.
2. `frogctl/src/cli.rs:56` — help text `http://127.0.0.1:6380` → `:9090`. This is published:
   `website/src/data/frogctl-cli.json:69` carries the same string, so `just docs-gen` re-runs in the
   same commit.
3. `frogctl/CONTEXT.md:13` — "default port 6380" → 9090. **This one is a glossary *merge*, not a
   number swap.** The file defines **Admin API** `:12-13` and **Metrics API** `:15-16` as two
   endpoints; once both read 9090 they are visibly one HTTP server (`http.port`) with two route
   prefixes, and leaving two entries invites the next author to re-derive two ports. Merge them into
   one entry naming both prefixes, and keep `--admin-url`/`--metrics-url` as two *flags* onto one
   address.
4. `CONTEXT-MAP.md:28` — **not named by revision 1 or the review**: "`frogctl` talks to a node on
   three planes — the RESP **data plane** (port 6379), the **Admin API** (HTTP, port 6380), and the
   **Metrics API** (HTTP, port 9090)". Same false fact, one level up, and it is the document the
   per-context glossaries derive from. Fixing `frogctl/CONTEXT.md` without it leaves the two in
   contradiction.

The remaining repo hits for `6380` are legitimate: `ops/config.rs:82,467,477` (the literals this
proposal deletes, plus diff test data), `commands/cluster.rs` CLUSTER NODES fixtures, and the
`website/` security docs where 6380 correctly means `tls-port`.

**Follow-up flag, out of the hotfix.** `--admin-url` and `--metrics-url` are two flags for one
address; a later change can collapse them (or make one an alias) without touching the hotfix.

**Durable form** — `frogdb_config::http::DEFAULT_HTTP_PORT` instead of a literal — lands with this
proposal's dependency, and is the reason the hotfix and the proposal are the same disease.

**Regression test — constructible today, verified.** `GlobalOpts` derives `clap::Parser`
(`cli.rs:22-23`) and `ConnectionContext::new` (`connection.rs:13-19`) is lazy — it stores
`GlobalOpts`, sets `resp: None` and builds a `reqwest::Client`; no socket is opened — so
`ConnectionContext::new(GlobalOpts::parse_from(["frogctl"])).admin_url()` runs with no server. The
assertion is `admin_url()`'s host:port `== metrics_url()`'s host:port under default `GlobalOpts` —
the invariant the two fallbacks share, and the one that would have caught this. Caveat, stated:
this lives in `frogctl`'s lib tests, which `just test` skips (§P5), so it runs under `just
frogctl-test` and under 73 §6 if that lands. Acceptable — a test that is correct and
occasionally-run beats a hardcoded literal with none — but not a reason to skip the durable fix.

### Not hotfix-eligible, though LIVE

* **P6 (`--section` always empty).** Upheld. The correct fix requires `config_param_registry()` —
  i.e. the dependency this proposal adds. A dependency-free "fix" (`format!("{s}*")`) is also wrong:
  `[memory]`'s parameters are `maxmemory`, `lfu-log-factor`, … which no section-name prefix
  matches. It ships inside the proposal. The existing test (`tests/integration_config.rs:18-29`)
  must **change its argument** (`"maxmemory"` → `"memory"`, `:24`) *and* gain a non-empty assertion
  at the same time — the argument change alone is not optional, since `maxmemory` is a parameter
  name and the fixed code would return zero rows for it just as the broken code does (§3).
* **P7 (`--diff` silently ignored).** Upheld. A dependency-free hotfix exists — make
  `Show { diff: Some(_) }` `bail!` like its three siblings, 3 lines at `commands/config.rs:110` —
  and is worth landing **if 72 slips**, since silently ignoring a documented flag is worse than
  refusing it. If 72 lands promptly, fold it in; the real implementation needs the registry either
  way.

## Review response ledger

Adversarial review at `43720822`, verdict AMEND. All blocking items accepted; three line-drift
claims refuted with evidence. Recorded here because the ledger is the only durable record of what
was checked.

**Accepted (blocking).** B1 → §P4 rewritten as a 3-input inventory (`validate_path_parent`,
`TlsConfig::validate`'s 7 probes, `ShardCountVsCpusValidator`), purity claim narrowed to "no
filesystem access", testability test 2 restated against the real property, `--check-paths` renamed
`--check-host`. B2 → the `report.merge(host_probes())` sentence deleted; `Config::validate()` is now
*not edited at all*, with the duplication cost named and pinned. B3 → the "73 makes it live" hazard
retracted (73 does not touch these files, `73:82`, `73:455-457`) and replaced with four real shared
artifacts; conclusion (72 first) unchanged. B4 → three denominators/counts fixed (20-key standalone
split 11/2/7 and 24-key `--cluster` split 11/2/8/3; 7 unit tests not 6; three sections lack
`deny_unknown_fields`, not two).

**Accepted (non-blocking).** deb-gen is not built from `default_toml_impl` (so no `deb-gen` re-run,
and it is a fourth `to_toml` consumer); `to_toml` parameterised on the header; server `main.rs:24`
not `:23` (×3), with the `frogctl/src/main.rs:23` collision called out; `persistence.enabled`
default re-cited to `persistence.rs:15`→`:135`; `params.rs:552` re-cited as `GOLDEN_SNAPSHOT` test
data; `--section` test argument fix; docs-gen's `ConfigLoader` import `:18` deleted (and it
strengthens the deletion test into a compiler-enforced one); ADR gains a dated `Amended` marker;
proposal 69's `ConfigManager::get :3359` overlap recorded; `metrics_url` `:146-152`;
`frogctl/CONTEXT.md` 78 lines; `frogctl/Cargo.toml` deps `:28-44`; `config/Cargo.toml` deps
`:13-20`; loader tests `:413-479`.

**Refuted, with evidence (three line-drift claims; revision 1's numbers or new ones stand).**

| Review said | Verified at `4c36827d` | Kept |
|---|---|---|
| `ValidationReport` is `validators/mod.rs:62-69` | `:61` doc comment, `:62` `#[derive(Debug, Default)]`, `:63` `pub struct ValidationReport {`, `:70` closing brace | **`:63-70`** (revision 1 was right) |
| `generate_example_config` is `docs-gen/src/main.rs:455-457` | `:453-455` doc comment, `:456` `fn generate_example_config() -> String {`, `:457` the call, `:458` closing brace | **`:456-458`** (both revision 1's `:454-458` and the review were off) |
| `/admin/*` is `observability_server.rs:235-244` | `:235` is `.route("/debug/{*path}", …)`; the admin routes run `:236` (`/admin/health`) through `:245` (the closing paren of the multi-line `/admin/transfer-leader` route) | **`:236-245`** (revision 1's `:236-243` truncated the last route) |

**Held from the review, re-verified and left intact.** The §P1 key-by-key drift census (11 rejected
rows unchanged); `logging.format` invalid per `lib.rs:317`; `admin.port` 6382 triple-sourced;
`[cluster.raft]` silently ignored; `num_shards` as first offender; the scale figures (26 sections /
147 keys / 213 lines `example-config.toml`, 221 lines deb `frogdb.toml`); the §P6 six-step trace;
§P7; the deletion test's two callers; the "delete, not migrate" ruling on the inverse-assertion
tests (softened by exactly one assertion, which has a successor); §P5 and its second gating layer;
the `frogctl → frogdb-config` dependency edge closing no cycle and `toml` already being present;
the whole spec/LOCKED section; and 65-H3's "stricter for free under either option" — now stated as
**three** sections.
