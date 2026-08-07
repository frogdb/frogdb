# An implicitly-discovered `./frogdb.toml` is merged with `.nested()` and silently ignored

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/05 F1 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 2 · priority 21
Area: frogdb-config / loader

## Context

The two config-loading branches use different figment semantics for the same file. When the path is
passed explicitly the file is merged normally; when it is *discovered* in the working directory it
is merged with `.nested()`, which reinterprets top-level tables as figment **profiles** rather than
sections. The operator's entire config file then has no effect: the node boots on defaults
(persistence/AOF/maxmemory/bind all default) with no error and no warning. Worse,
`config_source_path` is still set from that file, so a later `CONFIG REWRITE` writes into a file
whose contents were never read. Running `frogdb-server` with no `-c` in a directory containing
`frogdb.toml` is the documented default discovery path — the `else` branch exists precisely for it
— and durability settings silently not applied is a data-loss path.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `config/loader.rs:87` `figment.merge(Toml::file(path))` vs `config/loader.rs:91`
  `figment.merge(Toml::file(default_path).nested())` — the two branches use different figment
  semantics for the same file.
- `config/loader.rs:193–202` then sets `config_source_path` from that same discovered file, so
  CONFIG REWRITE will *write into* a file whose contents were never read.
- **Why nothing catches it**: `Config::load` is `single-test`, **14/176 regions** — neither branch
  is asserted.

## What to fix

1. Drop `.nested()` from the discovery branch so both branches share identical merge semantics —
   ideally by routing both through one call site that takes the resolved path.
2. Refactor discovery to take a base directory instead of relying on the process working
   directory, so the test does not need `set_current_dir` (which races across parallel tests).
3. Confirm `config_source_path` is only set for a file that was actually read with the same
   semantics `CONFIG REWRITE` will write back.

## Acceptance criteria

- [ ] New crate-level test writes `frogdb.toml` containing
      `[server]\nport = 7777\n[persistence]\naof_enabled = true`, loads it (a) explicitly and
      (b) via discovery, and asserts **both** yield `port == 7777` and `aof_enabled == true`.
      The discovery case **fails today**.
- [ ] The same test asserts `config_source_path` is `Some(canonicalized path)` in both cases.
- [ ] The test does not call `set_current_dir` (discovery takes a base dir), so it is safe under
      parallel execution.
- [ ] A `CONFIG REWRITE` round-trip over the discovered file preserves the values that were loaded.

## Test boundary

**2** — crate-level API test on `Config::load`. The behaviour is entirely in figment merge
semantics; a server boot would add nothing but seconds, and would not make the divergence between
the two branches any more visible.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

Confirmed live on today's tree. `frogdb-server/crates/server/src/config/loader.rs:87` still merges an
explicit path with plain `Toml::file(path)` while `:91` merges the discovered `./frogdb.toml` with
`Toml::file(default_path).nested()` — figment's `nested()` reinterprets `[server]`/`[persistence]` as
*profiles*, and `figment.extract()` at `:128` only reads the Default profile, so a discovered file
contributes nothing. `config_source_path` is still set unconditionally from that unread file at
`loader.rs:199-204` (issue cited `:193–202`), so `CONFIG REWRITE` still writes into it. File:line
refs are unchanged apart from the `config_source_path` block; the file did not move during the
crate extractions (`git log -- .../config/loader.rs` shows no touch since `0169fbae`). No test
exercises the discovery branch: `Config::load` has exactly one caller in tests,
`test_load_explicit_config_file_not_found` (`crates/server/src/config/mod.rs:402-435`), which only
covers the missing-explicit-path bail. No FM row covers config loading (config is not one of the
six locked areas).
