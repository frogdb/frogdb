# 34: the quint conformance harness resolves `quint` the way the Justfile does

Status: ready-for-agent
Type: AFK
Origin: whole-branch fix round, 2026-09-05 (D7) — 24 `quint_conformance` tests failed in
an agent shell until `mise which quint` was prepended to PATH by hand
Area: Justfile + frogdb-replication test harness
Phase: 6 — polish

## Parent

[PRD.md](../../PRD.md), decision D7.

## Why

`Justfile:40` already resolves the binary without relying on shell activation:

```
quint := `command -v quint 2>/dev/null || mise which quint 2>/dev/null || echo quint`
```

and the `quint-check` / `quint-run` recipes use `{{quint}}`. The Rust harness does not: at
`frogdb-server/crates/replication/tests/quint_conformance.rs:350`

```rust
let output = Command::new("quint").args(args).output();
```

so `just test frogdb-replication` fails 24 tests in any shell where mise is installed but
not activated — every subagent Bash shell, and any CI step that does not activate mise. The
harness's panic message is good; the point is that it should not need to fire when the
Justfile already knows where `quint` is.

## What to build

1. The `test` recipe (and every other recipe that runs `cargo nextest run` against
   `frogdb-replication` or `--all`: `test`, `test-concurrency`-family lines that include the
   replication crate, `test-changed` at `Justfile:346`) exports `QUINT={{quint}}` in the
   environment of the `cargo nextest run` invocation. Do it the way `{{dyld-env}}` /
   `{{rocksdb-env}}` are already threaded — a `quint-env` variable
   (`quint-env := "QUINT=" + quint`) placed beside them is the smallest change.
2. `run_quint` in `quint_conformance.rs` reads `std::env::var("QUINT")` and falls back to
   `"quint"` when unset or empty. The `NotFound` panic message stays and additionally prints
   which path it tried.
3. The frogdb-cluster `quint_conformance` binary (`Justfile:775`, `:785`) gets the same
   treatment if it also spawns `quint` by bare name — check with grep; if it already goes
   through an env var or the recipe, leave it.

## Acceptance criteria

- [ ] In a shell with mise installed but **not** activated (`env -u PATH …` is not needed:
      a plain `just test frogdb-replication quint_conformance` from an agent Bash shell is
      the reproduction), the 24 `quint_conformance` tests pass.
- [ ] `QUINT=/nonexistent just test frogdb-replication quint_conformance` fails with the
      existing "not on PATH" panic naming `/nonexistent`.
- [ ] `just quint-check` and `just quint-run` unchanged and green.
- [ ] `just fmt-check` green.

## Files likely touched

- `Justfile`
- `frogdb-server/crates/replication/tests/quint_conformance.rs`
- (only if item 3 applies) `frogdb-server/crates/cluster/tests/quint_conformance.rs`

## Out of scope

Installing quint; changing `.mise.toml`; the models themselves.

## Blocked by

None.

## Decisions

D7
